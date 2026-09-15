//! Configuration (RFD 011): one schema read from TOML or JSON into one
//! structure and validated once, so no setting is accepted in one syntax
//! and refused in the other.

use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::contract::PAGE_CACHE_DISK_BLOCK_BYTES;
use crate::error::Result;
use crate::time;

/// The first non-whitespace character of a JSON document; a file that
/// starts with it is JSON and every other file is TOML.
const JSON_OPENING: char = '{';

/// The gRPC listener when the file names none. Loopback: RFD 012 has no
/// mode in which the port is safe to expose directly, so reaching it from
/// another host is a decision the deployer writes down. 7433 is the port
/// the prototype ran on.
const DEFAULT_LISTEN: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7433);

/// The listener for `/metrics` and `/ready` (RFD 012) when the file names
/// none. Loopback for the same reason as `DEFAULT_LISTEN`; 9433 sits beside
/// the gRPC port so the pair is recognizable together.
const DEFAULT_METRICS_LISTEN: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9433);

/// How often a table's ref is re-resolved when the file says nothing. An
/// unchanged poll costs one read (RFD 005), so ten seconds is cheap, and a
/// committed snapshot is serving within one poll of its commit.
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(10);

/// The value-page cache budget when the file says nothing: 256 mebibytes,
/// a few thousand pages at the layout's 64-kibibyte bound (RFD 002), enough
/// for a warm working set on a small replica while leaving memory to the
/// resident index. A replica that serves more raises it in the file.
const DEFAULT_PAGE_CACHE_BYTES: usize = 256 * 1024 * 1024;

/// `strict` when a table says nothing: on. A table that serves at a cost is
/// refused until its owner turns strict off with one line (RFD 007).
const DEFAULT_STRICT: bool = true;

/// Residency when a table says nothing: `full`, RFD 017's fastest tier, one
/// GET per hit and none per miss. The other tiers are chosen by the owner
/// with the doctor's numbers in front of them, never silently.
const DEFAULT_RESIDENCY: Residency = Residency::Full;

/// Hedging when the file says nothing: on. A minority of stalled GETs is
/// what hedging removes, and the ceiling bounds its cost when there is no
/// tail (RFD 013).
const DEFAULT_HEDGE_ENABLED: bool = true;

/// The latency percentile the hedge delay is taken at when the file says
/// nothing: the 95th, above the typical read and below the tail it exists
/// to cut. A starting point the first real measurement may move (RFD 013).
const DEFAULT_HEDGE_PERCENTILE: f64 = 0.95;

/// Lookup GETs in the per-table latency window when the file says nothing:
/// 256, enough to estimate a 95th percentile and short enough to follow a
/// stall episode (RFD 013).
const DEFAULT_HEDGE_WINDOW: usize = 256;

/// The share of requests that may hedge when the file says nothing: five
/// percent, so the GET overhead stays small where reads are billed per GET
/// (RFD 013).
const DEFAULT_HEDGE_MAX_RATE: f64 = 0.05;

/// What `S3Config`'s `Debug` prints in place of the secret access key.
const SECRET_PLACEHOLDER: &str = "[redacted]";

/// Why a configuration could not be read or acted on.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// The configuration file could not be read.
    #[error("cannot read configuration file: {0}")]
    Read(#[from] std::io::Error),
    /// The text did not parse as TOML.
    #[error("configuration is not valid toml: {0}")]
    Toml(#[from] toml::de::Error),
    /// The text did not parse as JSON.
    #[error("configuration is not valid json: {0}")]
    Json(#[from] serde_json::Error),
    /// The configuration parsed but breaks a rule.
    #[error("invalid configuration: {0}")]
    Invalid(#[from] ConfigViolation),
}

/// A rule a parsed configuration breaks. The set is closed; a rule not
/// listed here is not checked. The two location rules are checked when a
/// location is resolved rather than when the file is parsed, because
/// locations arrive from table metadata; they are configuration faults all
/// the same, since the remedy is a rewrite rule or a registered store.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum ConfigViolation {
    /// A table has an empty name.
    #[error("a table has an empty name")]
    EmptyTableName,
    /// Two tables share a name.
    #[error("table name {0} is configured more than once")]
    DuplicateTableName(String),
    /// One of the two static S3 keys is set without the other.
    #[error("static credentials need both access_key_id and secret_access_key")]
    IncompleteStaticKeys,
    /// A resident-bytes budget on a table whose residency has nothing to budget.
    #[error("resident_budget_bytes is set on a table whose residency does not take a budget")]
    BudgetWithoutResidency,
    /// Disk residency with no local directory to write chunk files to.
    #[error("disk residency needs a local directory under storage.local")]
    DiskResidencyWithoutLocalDir,
    /// A page cache disk level smaller than its one block, which would hold
    /// nothing (RFD 017); zero turns the level off instead.
    #[error(
        "page_cache_disk_bytes {bytes} is below one block of {minimum} bytes; zero means no disk level"
    )]
    PageCacheDiskTooSmall {
        /// The configured size.
        bytes: u64,
        /// The smallest level that holds a page: one block.
        minimum: u64,
    },
    /// The hedge percentile is outside `(0, 1]`; the delay is that percentile
    /// of the latency window (RFD 013).
    #[error("hedge percentile {percentile} is not in (0, 1]")]
    HedgePercentileOutOfRange {
        /// The configured percentile.
        percentile: f64,
    },
    /// The hedge ceiling is outside `[0, 1]`; it is the share of requests
    /// that may hedge (RFD 013).
    #[error("hedge max_rate {max_rate} is not in [0, 1]")]
    HedgeMaxRateOutOfRange {
        /// The configured ceiling.
        max_rate: f64,
    },
    /// The hedge window holds no samples, so it has no percentile.
    #[error("hedge window must hold at least one sample")]
    HedgeWindowEmpty,
    /// A location, after rewriting, whose `scheme://authority/` prefix names
    /// no store the process has registered and none it can build (RFD 003:
    /// it builds `s3://`, `s3a://`, and `file://`).
    #[error(
        "no object store for location {location}; add a rewrite rule or register a store for its prefix"
    )]
    NoStoreForLocation {
        /// The location as the process tried to resolve it.
        location: String,
    },
    /// A location, after rewriting, that is neither a URI nor an absolute
    /// path, so it names no store at all.
    #[error("location {location} is neither a uri nor an absolute path")]
    LocationNotAbsolute {
        /// The location as the process tried to resolve it.
        location: String,
    },
}

/// The whole configuration: the server, storage, and the tables it serves.
/// Every section defaults when absent; a key the schema does not know is an
/// error in every section.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Listeners, polling, the page cache, and logging.
    #[serde(default)]
    pub server: ServerConfig,
    /// Object storage, hedging, and the optional local directory.
    #[serde(default)]
    pub storage: StorageConfig,
    /// The tables to serve; names are unique and non-empty.
    #[serde(default)]
    pub tables: Vec<TableConfig>,
}

impl Config {
    /// Parses TOML or JSON, decided by content: a first non-whitespace `{`
    /// is JSON. The result is validated once, so the two syntaxes cannot
    /// disagree on what is accepted.
    pub fn parse(text: &str) -> Result<Self> {
        let config: Self = if text.trim_start().starts_with(JSON_OPENING) {
            serde_json::from_str(text).map_err(ConfigError::Json)?
        } else {
            toml::from_str(text).map_err(ConfigError::Toml)?
        };
        validate(&config).map_err(ConfigError::Invalid)?;
        Ok(config)
    }

    /// Reads the file, then `parse`s it; a file that cannot be read is
    /// `ConfigError::Read`.
    pub fn from_path(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path).map_err(ConfigError::Read)?;
        Self::parse(&text)
    }
}

/// The first rule the configuration breaks, or `Ok` when it breaks none.
fn validate(config: &Config) -> std::result::Result<(), ConfigViolation> {
    validate_hedge(&config.storage.hedge)?;
    if let Some(s3) = &config.storage.s3
        && s3.access_key_id.is_some() != s3.secret_access_key.is_some()
    {
        return Err(ConfigViolation::IncompleteStaticKeys);
    }
    if let Some(local) = &config.storage.local
        && local.page_cache_disk_bytes > 0
        && local.page_cache_disk_bytes < PAGE_CACHE_DISK_BLOCK_BYTES
    {
        return Err(ConfigViolation::PageCacheDiskTooSmall {
            bytes: local.page_cache_disk_bytes,
            minimum: PAGE_CACHE_DISK_BLOCK_BYTES,
        });
    }
    let mut names = HashSet::new();
    for table in &config.tables {
        if table.name.is_empty() {
            return Err(ConfigViolation::EmptyTableName);
        }
        if !names.insert(table.name.as_str()) {
            return Err(ConfigViolation::DuplicateTableName(table.name.clone()));
        }
        match table.residency {
            Residency::Full => {}
            Residency::Disk | Residency::Bloom => {
                if table.resident_budget_bytes.is_some() {
                    return Err(ConfigViolation::BudgetWithoutResidency);
                }
            }
        }
        if table.residency == Residency::Disk && config.storage.local.is_none() {
            return Err(ConfigViolation::DiskResidencyWithoutLocalDir);
        }
    }
    Ok(())
}

/// The hedge settings are checked whether or not hedging is enabled, so a
/// mistyped value is caught before the day it is switched on.
fn validate_hedge(hedge: &HedgeConfig) -> std::result::Result<(), ConfigViolation> {
    let percentile_in_range = hedge.percentile > 0.0 && hedge.percentile <= 1.0;
    if !percentile_in_range {
        return Err(ConfigViolation::HedgePercentileOutOfRange {
            percentile: hedge.percentile,
        });
    }
    if !(0.0..=1.0).contains(&hedge.max_rate) {
        return Err(ConfigViolation::HedgeMaxRateOutOfRange {
            max_rate: hedge.max_rate,
        });
    }
    if hedge.window == 0 {
        return Err(ConfigViolation::HedgeWindowEmpty);
    }
    Ok(())
}

/// Listeners, polling, the page cache, and logging. `metrics_listen` is
/// always bound: it serves `/metrics` and `/ready`, and RFD 012 makes both
/// part of the surface.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ServerConfig {
    /// The gRPC listener.
    pub listen: SocketAddr,
    /// The `/metrics` and `/ready` listener.
    pub metrics_listen: SocketAddr,
    /// How often each table's ref is re-resolved (RFD 005); written as a
    /// friendly duration such as `"10s"`.
    #[serde(with = "time::friendly_duration")]
    pub poll_interval: Duration,
    /// The value-page cache budget in bytes.
    pub page_cache_bytes: usize,
    /// Log field renames and additions.
    pub log: LogConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: DEFAULT_LISTEN,
            metrics_listen: DEFAULT_METRICS_LISTEN,
            poll_interval: DEFAULT_POLL_INTERVAL,
            page_cache_bytes: DEFAULT_PAGE_CACHE_BYTES,
            log: LogConfig::default(),
        }
    }
}

/// Logs are JSON only (RFD 012). `fields` renames the standard field names
/// and `static_fields` are added to every event; both are empty by default.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct LogConfig {
    /// Standard field name to the name it is emitted under.
    pub fields: BTreeMap<String, String>,
    /// Field name to a constant value added to every event.
    pub static_fields: BTreeMap<String, String>,
}

/// Hedging of lookup GETs (RFD 013). The delay is `percentile` of the last
/// `window` lookup GETs of the table, and no hedge is issued while hedges
/// would exceed `max_rate` of requests. Load reads are never hedged.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct HedgeConfig {
    /// Whether lookup GETs are hedged at all.
    pub enabled: bool,
    /// The latency percentile the delay is taken at, in `(0, 1]`.
    pub percentile: f64,
    /// Lookup GETs in the per-table latency window; at least one.
    pub window: usize,
    /// The share of requests that may hedge, in `[0, 1]`.
    pub max_rate: f64,
}

impl Default for HedgeConfig {
    fn default() -> Self {
        Self {
            enabled: DEFAULT_HEDGE_ENABLED,
            percentile: DEFAULT_HEDGE_PERCENTILE,
            window: DEFAULT_HEDGE_WINDOW,
            max_rate: DEFAULT_HEDGE_MAX_RATE,
        }
    }
}

/// Explicit S3 client settings, for S3-compatible stores and tests. Absent,
/// the storage layer uses the SDK's default chain (RFD 003). Static keys
/// come as a pair or not at all. Never serializable: a rendering of this
/// struct other than `Debug` would carry the secret in plain text.
#[derive(Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct S3Config {
    /// Endpoint URL, for stores other than AWS.
    pub endpoint: Option<String>,
    /// Region name.
    pub region: Option<String>,
    /// Static access key id; set together with `secret_access_key`.
    pub access_key_id: Option<String>,
    /// Static secret key; never printed.
    pub secret_access_key: Option<String>,
    /// Permit plain HTTP endpoints.
    pub allow_http: bool,
    /// Use path-style addressing, `host/bucket/key`.
    pub path_style: bool,
}

impl fmt::Debug for S3Config {
    /// The secret is rendered as a fixed placeholder so no log or panic
    /// message carries it.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Config")
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("access_key_id", &self.access_key_id)
            .field(
                "secret_access_key",
                &self.secret_access_key.as_ref().map(|_| SECRET_PLACEHOLDER),
            )
            .field("allow_http", &self.allow_http)
            .field("path_style", &self.path_style)
            .finish()
    }
}

/// A location prefix replaced before any read (RFD 003), for tables whose
/// metadata records paths from another host or bucket.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RewriteRule {
    /// The prefix as it appears in the Iceberg metadata.
    pub from: String,
    /// The prefix to read from instead.
    pub to: String,
}

/// The optional local directory (RFD 017, RFD 012): chunk files for disk
/// residency and the page cache's disk level. Everything in it is rebuilt
/// from object storage.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalConfig {
    /// The directory.
    pub dir: PathBuf,
    /// The bytes chunk files may occupy across every table; unbounded when absent.
    pub index_budget_bytes: Option<u64>,
    /// The size bound of the page cache's disk level: zero means no disk
    /// level, anything else at least `contract::PAGE_CACHE_DISK_BLOCK_BYTES`,
    /// the level's one block.
    pub page_cache_disk_bytes: u64,
}

/// Object storage, hedging, and the local directory.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct StorageConfig {
    /// Explicit S3 settings; absent means the SDK's default chain.
    pub s3: Option<S3Config>,
    /// Location prefix rewrites, applied in order.
    pub rewrite: Vec<RewriteRule>,
    /// Hedging of lookup GETs.
    pub hedge: HedgeConfig,
    /// The local directory; required by disk residency.
    pub local: Option<LocalConfig>,
}

/// One served table.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableConfig {
    /// The name clients use in requests; unique and non-empty.
    pub name: String,
    /// Where the table's current metadata comes from.
    pub source: SourceConfig,
    /// The Iceberg ref (branch or tag) that is served; `ref` in the file.
    pub r#ref: String,
    /// The key column; its Iceberg type is one of `KeyType`.
    pub key_column: String,
    /// The value column; Iceberg binary, fixed, or string.
    pub value_column: String,
    /// Refuse a generation the doctor prices at a cost (RFD 007).
    #[serde(default = "default_strict")]
    pub strict: bool,
    /// Where the key chunks live (RFD 017).
    #[serde(default = "default_residency")]
    pub residency: Residency,
    /// The resident-bytes budget a `full` load is held to (RFD 017); only
    /// that tier takes one.
    pub resident_budget_bytes: Option<u64>,
}

fn default_strict() -> bool {
    DEFAULT_STRICT
}

fn default_residency() -> Residency {
    DEFAULT_RESIDENCY
}

/// Where a table keeps its key chunks (RFD 017), ranked by lookup speed.
/// Chosen by the table owner and never by the loader. Display and serde give
/// the lowercase name that appears in the file, the doctor's output, and
/// logs.
// The serde names and Display are the same strings;
// `test_serialize_residency_matches_display` holds them together.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Residency {
    /// Key chunks in memory: one GET per hit, none per miss.
    Full,
    /// Key chunks in a local file, fences in memory: one GET and one local
    /// read per hit, one local read per miss.
    Disk,
    /// Bloom filters in memory, no local disk: two GETs per hit, a GET per
    /// miss at the filter's false-positive rate.
    Bloom,
}

impl fmt::Display for Residency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Full => "full",
            Self::Disk => "disk",
            Self::Bloom => "bloom",
        };
        f.write_str(name)
    }
}

/// Where a table's current metadata is read from (RFD 005). Tagged by a
/// `type` key in the file: `static` or `rest`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum SourceConfig {
    /// A metadata file location, or a table root whose `metadata/` carries
    /// a version hint.
    Static {
        /// The metadata file or table root.
        location: String,
    },
    /// An Iceberg REST catalog.
    Rest {
        /// The catalog's base URI.
        uri: String,
        /// The warehouse, when the catalog needs one.
        warehouse: Option<String>,
        /// The namespace, dot-separated.
        namespace: String,
        /// The table name within the namespace.
        table: String,
    },
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use super::*;
    use crate::error::Error;

    /// The gRPC listener the full fixture names.
    const FIXTURE_LISTEN: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 7433);
    /// The metrics listener the full fixture names.
    const FIXTURE_METRICS_LISTEN: SocketAddr =
        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 9433);
    /// The poll interval the full fixture names, `"30s"`.
    const FIXTURE_POLL_INTERVAL: Duration = Duration::from_secs(30);
    /// The page cache budget the full fixture names: 128 mebibytes.
    const FIXTURE_PAGE_CACHE_BYTES: usize = 128 * 1024 * 1024;
    /// The hedge percentile the full fixture names.
    const FIXTURE_HEDGE_PERCENTILE: f64 = 0.9;
    /// The hedge window the full fixture names.
    const FIXTURE_HEDGE_WINDOW: usize = 128;
    /// The hedge ceiling the full fixture names.
    const FIXTURE_HEDGE_MAX_RATE: f64 = 0.1;
    /// The chunk-file budget the full fixture names: 4 gibibytes.
    const FIXTURE_INDEX_BUDGET_BYTES: u64 = 4 * 1024 * 1024 * 1024;
    /// The disk page cache bound the full fixture names: 8 gibibytes.
    const FIXTURE_PAGE_CACHE_DISK_BYTES: u64 = 8 * 1024 * 1024 * 1024;
    /// The resident budget the full fixture puts on its `full` table: 1 gibibyte.
    const FIXTURE_RESIDENT_BUDGET_BYTES: u64 = 1024 * 1024 * 1024;
    /// A hedge percentile above the range.
    const PERCENTILE_TOO_HIGH: f64 = 1.5;
    /// A hedge ceiling above the range.
    const MAX_RATE_TOO_HIGH: f64 = 1.5;
    /// A hedge ceiling below the range.
    const MAX_RATE_NEGATIVE: f64 = -0.5;
    /// The top of both hedge ranges, which is inside each.
    const HEDGE_RANGE_TOP: f64 = 1.0;
    /// The bottom of the ceiling's range, which is inside it.
    const MAX_RATE_BOTTOM: f64 = 0.0;

    /// One complete configuration: every optional section present and two
    /// tables, one per source kind. `json_full` is the same document. Every
    /// number is a `FIXTURE_*` constant, so the value has one home.
    fn toml_full() -> String {
        format!(
            r#"
[server]
listen = "{FIXTURE_LISTEN}"
metrics_listen = "{FIXTURE_METRICS_LISTEN}"
poll_interval = "30s"
page_cache_bytes = {FIXTURE_PAGE_CACHE_BYTES}

[server.log.fields]
timestamp = "ts"
level = "severity"

[server.log.static_fields]
service = "icetray"

[storage.s3]
endpoint = "http://object-store.example:9000"
region = "us-example-1"
access_key_id = "AKIAEXAMPLE"
secret_access_key = "secret-example"
allow_http = true
path_style = true

[[storage.rewrite]]
from = "s3://old-warehouse/"
to = "s3://example-warehouse/"

[storage.hedge]
enabled = true
percentile = {FIXTURE_HEDGE_PERCENTILE:?}
window = {FIXTURE_HEDGE_WINDOW}
max_rate = {FIXTURE_HEDGE_MAX_RATE:?}

[storage.local]
dir = "/var/lib/icetray"
index_budget_bytes = {FIXTURE_INDEX_BUDGET_BYTES}
page_cache_disk_bytes = {FIXTURE_PAGE_CACHE_DISK_BYTES}

[[tables]]
name = "orders"
ref = "main"
key_column = "id"
value_column = "payload"
strict = false
residency = "full"
resident_budget_bytes = {FIXTURE_RESIDENT_BUDGET_BYTES}

[tables.source]
type = "static"
location = "s3://example-warehouse/db/orders/metadata/v3.metadata.json"

[[tables]]
name = "profiles"
ref = "serving"
key_column = "user_id"
value_column = "vector"
residency = "disk"

[tables.source]
type = "rest"
uri = "http://catalog.example:8181"
warehouse = "example"
namespace = "db"
table = "profiles"
"#
        )
    }

    /// The document `toml_full` renders, as JSON.
    fn json_full() -> String {
        serde_json::json!({
            "server": {
                "listen": FIXTURE_LISTEN.to_string(),
                "metrics_listen": FIXTURE_METRICS_LISTEN.to_string(),
                "poll_interval": "30s",
                "page_cache_bytes": FIXTURE_PAGE_CACHE_BYTES,
                "log": {
                    "fields": {"timestamp": "ts", "level": "severity"},
                    "static_fields": {"service": "icetray"}
                }
            },
            "storage": {
                "s3": {
                    "endpoint": "http://object-store.example:9000",
                    "region": "us-example-1",
                    "access_key_id": "AKIAEXAMPLE",
                    "secret_access_key": "secret-example",
                    "allow_http": true,
                    "path_style": true
                },
                "rewrite": [{"from": "s3://old-warehouse/", "to": "s3://example-warehouse/"}],
                "hedge": {
                    "enabled": true,
                    "percentile": FIXTURE_HEDGE_PERCENTILE,
                    "window": FIXTURE_HEDGE_WINDOW,
                    "max_rate": FIXTURE_HEDGE_MAX_RATE
                },
                "local": {
                    "dir": "/var/lib/icetray",
                    "index_budget_bytes": FIXTURE_INDEX_BUDGET_BYTES,
                    "page_cache_disk_bytes": FIXTURE_PAGE_CACHE_DISK_BYTES
                }
            },
            "tables": [
                {
                    "name": "orders",
                    "source": {
                        "type": "static",
                        "location": "s3://example-warehouse/db/orders/metadata/v3.metadata.json"
                    },
                    "ref": "main",
                    "key_column": "id",
                    "value_column": "payload",
                    "strict": false,
                    "residency": "full",
                    "resident_budget_bytes": FIXTURE_RESIDENT_BUDGET_BYTES
                },
                {
                    "name": "profiles",
                    "source": {
                        "type": "rest",
                        "uri": "http://catalog.example:8181",
                        "warehouse": "example",
                        "namespace": "db",
                        "table": "profiles"
                    },
                    "ref": "serving",
                    "key_column": "user_id",
                    "value_column": "vector",
                    "residency": "disk"
                }
            ]
        })
        .to_string()
    }

    /// The smallest valid configuration: one table, everything else defaulted.
    /// The source is an inline table so that a test can append table keys
    /// after it and have them land in the table, not in the source.
    const TOML_MINIMAL: &str = r#"
[[tables]]
name = "kv"
source = { type = "static", location = "s3://example-warehouse/db/kv" }
ref = "main"
key_column = "key"
value_column = "value"
"#;

    /// The value `toml_full` and `json_full` both denote.
    fn full_config() -> Config {
        Config {
            server: ServerConfig {
                listen: FIXTURE_LISTEN,
                metrics_listen: FIXTURE_METRICS_LISTEN,
                poll_interval: FIXTURE_POLL_INTERVAL,
                page_cache_bytes: FIXTURE_PAGE_CACHE_BYTES,
                log: LogConfig {
                    fields: BTreeMap::from([
                        ("timestamp".to_string(), "ts".to_string()),
                        ("level".to_string(), "severity".to_string()),
                    ]),
                    static_fields: BTreeMap::from([("service".to_string(), "icetray".to_string())]),
                },
            },
            storage: StorageConfig {
                s3: Some(S3Config {
                    endpoint: Some("http://object-store.example:9000".to_string()),
                    region: Some("us-example-1".to_string()),
                    access_key_id: Some("AKIAEXAMPLE".to_string()),
                    secret_access_key: Some("secret-example".to_string()),
                    allow_http: true,
                    path_style: true,
                }),
                rewrite: vec![RewriteRule {
                    from: "s3://old-warehouse/".to_string(),
                    to: "s3://example-warehouse/".to_string(),
                }],
                hedge: HedgeConfig {
                    enabled: true,
                    percentile: FIXTURE_HEDGE_PERCENTILE,
                    window: FIXTURE_HEDGE_WINDOW,
                    max_rate: FIXTURE_HEDGE_MAX_RATE,
                },
                local: Some(LocalConfig {
                    dir: PathBuf::from("/var/lib/icetray"),
                    index_budget_bytes: Some(FIXTURE_INDEX_BUDGET_BYTES),
                    page_cache_disk_bytes: FIXTURE_PAGE_CACHE_DISK_BYTES,
                }),
            },
            tables: vec![
                TableConfig {
                    name: "orders".to_string(),
                    source: SourceConfig::Static {
                        location: "s3://example-warehouse/db/orders/metadata/v3.metadata.json"
                            .to_string(),
                    },
                    r#ref: "main".to_string(),
                    key_column: "id".to_string(),
                    value_column: "payload".to_string(),
                    strict: false,
                    residency: Residency::Full,
                    resident_budget_bytes: Some(FIXTURE_RESIDENT_BUDGET_BYTES),
                },
                TableConfig {
                    name: "profiles".to_string(),
                    source: SourceConfig::Rest {
                        uri: "http://catalog.example:8181".to_string(),
                        warehouse: Some("example".to_string()),
                        namespace: "db".to_string(),
                        table: "profiles".to_string(),
                    },
                    r#ref: "serving".to_string(),
                    key_column: "user_id".to_string(),
                    value_column: "vector".to_string(),
                    strict: DEFAULT_STRICT,
                    residency: Residency::Disk,
                    resident_budget_bytes: None,
                },
            ],
        }
    }

    /// The violation `text` parses to; anything else fails the test.
    fn violation(text: &str) -> ConfigViolation {
        let result = Config::parse(text);
        let Err(Error::Config(ConfigError::Invalid(violation))) = &result else {
            panic!("expected a configuration violation, got {result:?}");
        };
        violation.clone()
    }

    #[test]
    fn test_parse_toml_and_json_are_equivalent() {
        let from_toml = Config::parse(&toml_full()).unwrap();
        let from_json = Config::parse(&json_full()).unwrap();
        assert_eq!(from_toml, from_json);
        assert_eq!(from_toml, full_config());
    }

    #[test]
    fn test_parse_applies_defaults_when_absent() {
        let config = Config::parse(TOML_MINIMAL).unwrap();
        assert_eq!(config.server, ServerConfig::default());
        assert_eq!(config.server.listen, DEFAULT_LISTEN);
        assert_eq!(config.server.metrics_listen, DEFAULT_METRICS_LISTEN);
        assert_eq!(config.server.poll_interval, DEFAULT_POLL_INTERVAL);
        assert_eq!(config.server.page_cache_bytes, DEFAULT_PAGE_CACHE_BYTES);
        assert!(config.server.log.fields.is_empty());
        assert!(config.server.log.static_fields.is_empty());
        assert_eq!(config.storage, StorageConfig::default());
        assert_eq!(config.storage.s3, None);
        assert!(config.storage.rewrite.is_empty());
        assert_eq!(config.storage.hedge, HedgeConfig::default());
        assert!(config.storage.hedge.enabled);
        assert_eq!(config.storage.hedge.window, DEFAULT_HEDGE_WINDOW);
        assert_eq!(config.storage.local, None);
        assert_eq!(config.tables.len(), 1);
        assert!(config.tables[0].strict);
        assert_eq!(config.tables[0].residency, Residency::Full);
        assert_eq!(config.tables[0].resident_budget_bytes, None);
    }

    #[test]
    fn test_parse_s3_defaults_allow_http_and_path_style_off() {
        let config = Config::parse(&format!(
            "{TOML_MINIMAL}\n[storage.s3]\nregion = \"us-example-1\"\n"
        ))
        .unwrap();
        let s3 = config.storage.s3.unwrap();
        assert!(!s3.allow_http);
        assert!(!s3.path_style);
        assert_eq!(s3.access_key_id, None);
        assert_eq!(s3.secret_access_key, None);
    }

    #[test]
    fn test_parse_rejects_unknown_field() {
        let toml = Config::parse(&format!(
            "{TOML_MINIMAL}\n[server]\nlisten_addr = \"127.0.0.1:1\"\n"
        ));
        assert!(matches!(toml, Err(Error::Config(ConfigError::Toml(_)))));
        let nested = Config::parse(&format!(
            "{TOML_MINIMAL}\n[storage.hedge]\npercentil = 0.5\n"
        ));
        assert!(matches!(nested, Err(Error::Config(ConfigError::Toml(_)))));
        let json = Config::parse(r#"{"server": {"listen_addr": "127.0.0.1:1"}}"#);
        assert!(matches!(json, Err(Error::Config(ConfigError::Json(_)))));
        // The source is the one internally tagged enum in the schema.
        let source = Config::parse(
            r#"
[[tables]]
name = "kv"
ref = "main"
key_column = "key"
value_column = "value"

[tables.source]
type = "static"
location = "s3://example-warehouse/db/kv"
locaton = "typo"
"#,
        );
        assert!(matches!(source, Err(Error::Config(ConfigError::Toml(_)))));
    }

    #[test]
    fn test_parse_dispatches_on_first_non_whitespace_brace() {
        let json = Config::parse("  \n\t {\"tables\": []}").unwrap();
        assert!(json.tables.is_empty());
        let toml = Config::parse("  \n\t tables = []\n").unwrap();
        assert!(toml.tables.is_empty());
        // A brace later in the text does not make TOML into JSON.
        let inline = Config::parse("storage = { rewrite = [] }\n").unwrap();
        assert!(inline.storage.rewrite.is_empty());
        let malformed_json = Config::parse(" { not json");
        assert!(matches!(
            malformed_json,
            Err(Error::Config(ConfigError::Json(_)))
        ));
        let malformed_toml = Config::parse("not = toml = either");
        assert!(matches!(
            malformed_toml,
            Err(Error::Config(ConfigError::Toml(_)))
        ));
    }

    #[test]
    fn test_parse_refuses_empty_table_name() {
        let text = TOML_MINIMAL.replace("name = \"kv\"", "name = \"\"");
        assert_eq!(violation(&text), ConfigViolation::EmptyTableName);
    }

    #[test]
    fn test_parse_refuses_duplicate_table_name() {
        let text = format!("{TOML_MINIMAL}{TOML_MINIMAL}");
        assert_eq!(
            violation(&text),
            ConfigViolation::DuplicateTableName("kv".to_string())
        );
    }

    #[test]
    fn test_parse_refuses_incomplete_static_keys() {
        let id_only = format!("{TOML_MINIMAL}\n[storage.s3]\naccess_key_id = \"AKIAEXAMPLE\"\n");
        assert_eq!(violation(&id_only), ConfigViolation::IncompleteStaticKeys);
        let secret_only =
            format!("{TOML_MINIMAL}\n[storage.s3]\nsecret_access_key = \"secret-example\"\n");
        assert_eq!(
            violation(&secret_only),
            ConfigViolation::IncompleteStaticKeys
        );
    }

    #[test]
    fn test_parse_refuses_budget_without_residency() {
        let bloom = format!(
            "{}\nresidency = \"bloom\"\nresident_budget_bytes = 1\n",
            TOML_MINIMAL.trim_end()
        );
        assert_eq!(violation(&bloom), ConfigViolation::BudgetWithoutResidency);
        // Disk has its local directory, so the budget is the only fault.
        let disk = format!(
            "{}\nresidency = \"disk\"\nresident_budget_bytes = 1\n\n[storage.local]\ndir = \"/var/lib/icetray\"\npage_cache_disk_bytes = {PAGE_CACHE_DISK_BLOCK_BYTES}\n",
            TOML_MINIMAL.trim_end()
        );
        assert_eq!(violation(&disk), ConfigViolation::BudgetWithoutResidency);
    }

    #[test]
    fn test_parse_refuses_page_cache_disk_below_one_block() {
        let with_disk_level = |bytes: u64| {
            format!(
                "{}\n\n[storage.local]\ndir = \"/var/lib/icetray\"\npage_cache_disk_bytes = {bytes}\n",
                TOML_MINIMAL.trim_end()
            )
        };
        let just_below = PAGE_CACHE_DISK_BLOCK_BYTES - 1;
        assert_eq!(
            violation(&with_disk_level(just_below)),
            ConfigViolation::PageCacheDiskTooSmall {
                bytes: just_below,
                minimum: PAGE_CACHE_DISK_BLOCK_BYTES,
            }
        );
        assert_eq!(
            violation(&with_disk_level(1)),
            ConfigViolation::PageCacheDiskTooSmall {
                bytes: 1,
                minimum: PAGE_CACHE_DISK_BLOCK_BYTES,
            }
        );
        let one_block = Config::parse(&with_disk_level(PAGE_CACHE_DISK_BLOCK_BYTES)).unwrap();
        assert_eq!(
            one_block.storage.local.unwrap().page_cache_disk_bytes,
            PAGE_CACHE_DISK_BLOCK_BYTES
        );
        let no_level = Config::parse(&with_disk_level(0)).unwrap();
        assert_eq!(no_level.storage.local.unwrap().page_cache_disk_bytes, 0);
    }

    #[test]
    fn test_parse_refuses_disk_residency_without_local_dir() {
        let text = format!("{}\nresidency = \"disk\"\n", TOML_MINIMAL.trim_end());
        assert_eq!(
            violation(&text),
            ConfigViolation::DiskResidencyWithoutLocalDir
        );
    }

    #[test]
    fn test_parse_refuses_hedge_percentile_out_of_range() {
        let text = format!("{TOML_MINIMAL}\n[storage.hedge]\npercentile = {PERCENTILE_TOO_HIGH}\n");
        assert_eq!(
            violation(&text),
            ConfigViolation::HedgePercentileOutOfRange {
                percentile: PERCENTILE_TOO_HIGH
            }
        );
        let zero = format!("{TOML_MINIMAL}\n[storage.hedge]\npercentile = 0.0\n");
        assert_eq!(
            violation(&zero),
            ConfigViolation::HedgePercentileOutOfRange { percentile: 0.0 }
        );
    }

    #[test]
    fn test_parse_refuses_hedge_max_rate_out_of_range() {
        let negative = format!("{TOML_MINIMAL}\n[storage.hedge]\nmax_rate = {MAX_RATE_NEGATIVE}\n");
        assert_eq!(
            violation(&negative),
            ConfigViolation::HedgeMaxRateOutOfRange {
                max_rate: MAX_RATE_NEGATIVE
            }
        );
        let too_high = format!("{TOML_MINIMAL}\n[storage.hedge]\nmax_rate = {MAX_RATE_TOO_HIGH}\n");
        assert_eq!(
            violation(&too_high),
            ConfigViolation::HedgeMaxRateOutOfRange {
                max_rate: MAX_RATE_TOO_HIGH
            }
        );
    }

    #[test]
    fn test_parse_refuses_hedge_window_empty() {
        let text = format!("{TOML_MINIMAL}\n[storage.hedge]\nwindow = 0\n");
        assert_eq!(violation(&text), ConfigViolation::HedgeWindowEmpty);
    }

    #[test]
    fn test_parse_accepts_hedge_range_ends() {
        let top = format!(
            "{TOML_MINIMAL}\n[storage.hedge]\npercentile = {HEDGE_RANGE_TOP:?}\nmax_rate = {HEDGE_RANGE_TOP:?}\nwindow = 1\n"
        );
        assert_eq!(
            Config::parse(&top).unwrap().storage.hedge,
            HedgeConfig {
                enabled: DEFAULT_HEDGE_ENABLED,
                percentile: HEDGE_RANGE_TOP,
                window: 1,
                max_rate: HEDGE_RANGE_TOP,
            }
        );
        let bottom = format!("{TOML_MINIMAL}\n[storage.hedge]\nmax_rate = {MAX_RATE_BOTTOM:?}\n");
        assert_eq!(
            Config::parse(&bottom).unwrap().storage.hedge,
            HedgeConfig {
                max_rate: MAX_RATE_BOTTOM,
                ..HedgeConfig::default()
            }
        );
    }

    #[test]
    fn test_from_path_reads_then_parses() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(toml_full().as_bytes()).unwrap();
        assert_eq!(Config::from_path(file.path()).unwrap(), full_config());
    }

    #[test]
    fn test_from_path_missing_file_is_read_error() {
        let dir = tempfile::tempdir().unwrap();
        let result = Config::from_path(&dir.path().join("missing.toml"));
        assert!(matches!(result, Err(Error::Config(ConfigError::Read(_)))));
    }

    #[test]
    fn test_fmt_s3_config_debug_hides_secret() {
        let s3 = full_config().storage.s3.unwrap();
        let secret = s3.secret_access_key.clone().unwrap();
        let rendered = format!("{s3:?}");
        assert!(!rendered.contains(&secret));
        assert!(rendered.contains(SECRET_PLACEHOLDER));
        let without = S3Config::default();
        assert!(!format!("{without:?}").contains(SECRET_PLACEHOLDER));
    }

    #[test]
    fn test_fmt_residency_lowercase_names() {
        let cases = [
            (Residency::Full, "full"),
            (Residency::Disk, "disk"),
            (Residency::Bloom, "bloom"),
        ];
        for (residency, name) in cases {
            assert_eq!(residency.to_string(), name);
        }
    }

    #[test]
    fn test_serialize_residency_matches_display() {
        for residency in [Residency::Full, Residency::Disk, Residency::Bloom] {
            let json = serde_json::to_string(&residency).unwrap();
            assert_eq!(json, format!("\"{residency}\""));
            let back: Residency = serde_json::from_str(&json).unwrap();
            assert_eq!(back, residency);
        }
    }
}
