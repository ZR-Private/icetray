//! Configuration (RFD 011): one schema read from TOML or JSON into one
//! structure and validated once, so no setting is accepted in one syntax
//! and refused in the other.

use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::error::Category;

use crate::approx;
use crate::cache::min_memory_bytes;
use crate::contract::PAGE_CACHE_DISK_BLOCK_BYTES;
use crate::error::Result;
use crate::time;

/// The first non-whitespace character of a JSON document; a file that
/// starts with it is JSON and every other file is TOML.
const JSON_OPENING: char = '{';

/// The gRPC listener when the file names none. Loopback: RFD 012 has no
/// mode in which the port is safe to expose directly, so reaching it from
/// another host is a decision the deployer writes down. 7433 carries no
/// meaning beyond being the documented default; `DEFAULT_METRICS_LISTEN`
/// shares its last three digits so the pair reads as one.
const DEFAULT_LISTEN: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7433);

/// The listener for `/metrics` and `/ready` (RFD 012) when the file names
/// none. Loopback for the same reason as `DEFAULT_LISTEN`; 9433 sits beside
/// the gRPC port so the pair is recognizable together.
const DEFAULT_METRICS_LISTEN: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9433);

/// How often a table's ref is re-resolved when the file says nothing. An
/// unchanged poll costs one read (RFD 005), so ten seconds is cheap, and a
/// committed snapshot is serving within one poll of its commit.
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(10);

/// The shortest poll interval a configuration may name: one second. Each
/// table polls on the interval, and a poll of a catalog table is a catalog
/// call and a poll of a table root reads its version hint, so a shorter one
/// is a busy loop against the catalog that no freshness need justifies.
const MIN_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// The longest poll interval a configuration may name: one day. A table
/// written once a day is still followed within a day of its commit, and the
/// gauges sampled every interval are at most a day old. A day is also far
/// inside what the runtime's `Instant` arithmetic holds, where the first
/// tick is scheduled as now plus the interval and an interval as long as
/// `Duration::MAX` overflows it.
const MAX_POLL_INTERVAL: Duration = Duration::from_hours(24);

/// The value-page cache budget when the file says nothing: 256 mebibytes,
/// a few thousand pages at the layout's 64-kibibyte bound (RFD 002), enough
/// for a warm working set on a small replica while leaving memory to the
/// resident index. A replica that serves more raises it in the file.
const DEFAULT_PAGE_CACHE_BYTES: usize = 256 * 1024 * 1024;

/// The page cache shards `Config::parse` checks `page_cache_bytes` against:
/// one, the fewest a cache has on any machine, since the worker count that
/// sets the real number is not known until the cache opens on a runtime
/// (`cache::check_capacity`).
const PARSE_CACHE_SHARDS: usize = 1;

/// `strict` when a table says nothing: on. A table that is inefficient is
/// refused until its owner turns strict off with one line (RFD 007).
const DEFAULT_STRICT: bool = true;

/// Residency when a table says nothing: `full`, RFD 017's fastest tier, one
/// GET per hit and none per miss. The other tiers are chosen by the owner
/// with the doctor's numbers in front of them, never silently. The one home
/// for the default: the command line's `--residency` falls back to it too.
pub const DEFAULT_RESIDENCY: Residency = Residency::Full;

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

/// How long a `Get` may run when the file says nothing: five seconds. A
/// live lookup answers well under 200 ms, so a lookup still running at five
/// seconds is waiting on a wedged store, and failing it frees the caller
/// to fall back. A client's own shorter deadline is the gRPC layer's to
/// enforce, not this one's.
const DEFAULT_GET_DEADLINE: Duration = Duration::from_secs(5);

/// How long a `MultiGet` may run when the file says nothing: fifteen
/// seconds, flat whatever the batch size. A batch runs a bounded number of
/// lookups at a time, so a full batch of cold hits takes several GET round
/// trips; three `Get` deadlines covers that and still fails a wedged store
/// long before a caller's own patience runs out.
const DEFAULT_MULTI_GET_DEADLINE: Duration = Duration::from_secs(15);

/// Requests (`Get` and `MultiGet` together) in flight at once when the file
/// says nothing: 1024. A request that finds every slot taken is refused at
/// once rather than queued, so a wedged store cannot pile up requests until
/// the process runs out of memory. A single-pod guess, to revisit with
/// measurement (RFD 013).
const DEFAULT_MAX_IN_FLIGHT: usize = 1024;

/// Keys one `MultiGet` may carry when the file says nothing: 1000. A larger
/// batch is refused before any lookup, so the work one request can ask for,
/// and the answer it holds in memory, is bounded. A single-pod guess, to
/// revisit with measurement (RFD 013).
const DEFAULT_MAX_MULTI_GET_KEYS: usize = 1000;

/// How long a build from no serving generation may run when the file says
/// nothing: thirty minutes. Such a build, a first load or a retry after
/// one failed, reads every file's footer and key chunks, so it is allowed
/// several times the largest first load measured (RFD 013). It is bounded
/// all the same: the build holds the table's load lock, which
/// `Opened::retain` waits on for every table in turn.
const DEFAULT_LOAD_DEADLINE: Duration = Duration::from_mins(30);

/// How long a refresh over a serving generation may run when the file says
/// nothing: five minutes. A refresh reuses every file the new snapshot
/// still names and reads only the rest, so it is short, and it is bounded
/// tighter than a first load for the same reason: it holds the table's
/// load lock.
const DEFAULT_REFRESH_DEADLINE: Duration = Duration::from_mins(5);

/// How long a shutdown waits for open connections when the file says
/// nothing: twenty seconds. A request runs at most `DEFAULT_MULTI_GET_DEADLINE`,
/// so twenty seconds lets the last of them finish and leaves time to flush
/// the page cache before an orchestrator's usual thirty-second kill.
const DEFAULT_DRAIN_DEADLINE: Duration = Duration::from_secs(20);

/// Retries of one failed object-store request when the file says nothing:
/// two. A transient error recovers within a retry or two; more retries
/// against a wedged store only hold the request open longer.
const DEFAULT_MAX_RETRIES: usize = 2;

/// How long an object-store connection may take to open when the file says
/// nothing: one second. A connection to the store opens in milliseconds, so
/// a second is already a failure, and it leaves the retries time to try
/// another address.
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(1);

/// How long one catalog call may run when the file says nothing: thirty
/// seconds. A catalog call is on the background path, a poll or a load that
/// nobody waits on, so it is patient, but a catalog that never answers must
/// not hold a poll forever.
const DEFAULT_CATALOG_TIMEOUT: Duration = Duration::from_secs(30);

/// What `S3Config`'s `Debug` prints in place of the secret access key.
const SECRET_PLACEHOLDER: &str = "[redacted]";

/// Why a configuration could not be read or acted on. A document that is
/// not TOML or JSON at all is `Toml` or `Json`; one that is, but whose
/// content does not fit the schema, is `Schema`; one that fits but breaks
/// a rule is `Invalid`.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// The configuration file at `path` could not be read, or its bytes are
    /// not UTF-8.
    #[error("cannot read configuration file {}: {source}", path.display())]
    Read {
        /// The file.
        path: PathBuf,
        /// The operating system's refusal, or the decoding failure.
        source: std::io::Error,
    },
    /// The text is not TOML.
    #[error("configuration is not valid toml: {0}")]
    Toml(toml::de::Error),
    /// The text is not JSON.
    #[error("configuration is not valid json: {0}")]
    Json(serde_json::Error),
    /// The text is TOML or JSON, but the setting at `at` does not fit the
    /// schema: a required key missing, a key the schema does not know, a
    /// value of the wrong type or form. A missing key's `at` is the table
    /// that lacks it; the root is not named.
    #[error("invalid configuration: {}", schema_detail(at, fault))]
    Schema {
        /// Where in the document the fault is, as serde walked it:
        /// `tables[0]`, `server.poll_interval`.
        at: serde_path_to_error::Path,
        /// What does not fit; boxed, since a TOML error is large and every
        /// result that can fail on configuration carries the variant.
        fault: Box<SchemaFault>,
    },
    /// The configuration parsed but breaks a rule.
    #[error("invalid configuration: {0}")]
    Invalid(#[from] ConfigViolation),
}

/// What in a document does not fit the schema, in the syntax's own words.
#[derive(Debug, thiserror::Error)]
pub enum SchemaFault {
    /// From a TOML document; its message alone, without the source excerpt
    /// the TOML error prints, which frames every fault as a parse error.
    #[error("{}", .0.message())]
    Toml(toml::de::Error),
    /// From a JSON document, with the line and column it carries.
    #[error("{0}")]
    Json(serde_json::Error),
}

/// A schema fault prefixed by where it is, unless it is at the root: what
/// `ConfigError::Schema` displays after its framing, for a caller that
/// frames it with the file the text came from.
pub fn schema_detail(at: &serde_path_to_error::Path, fault: &SchemaFault) -> String {
    let place = place(at);
    if place.is_empty() {
        fault.to_string()
    } else {
        format!("{place}: {fault}")
    }
}

/// `at` as the file spells a setting's place: keys joined by `.`, a list
/// entry as `[index]`. A step serde could not name, such as a map key that
/// is not a string, is left out, so the place reads as far as it is known.
fn place(at: &serde_path_to_error::Path) -> String {
    let mut place = String::new();
    for segment in at {
        match segment {
            serde_path_to_error::Segment::Seq { index } => {
                place.push('[');
                place.push_str(&index.to_string());
                place.push(']');
            }
            serde_path_to_error::Segment::Map { key: name }
            | serde_path_to_error::Segment::Enum { variant: name } => {
                if !place.is_empty() {
                    place.push('.');
                }
                place.push_str(name);
            }
            serde_path_to_error::Segment::Unknown => {}
        }
    }
    place
}

/// A rule a parsed configuration breaks. The set is closed; a rule not
/// listed here is not checked. The two location rules are checked when a
/// location is resolved rather than when the file is parsed, because
/// locations arrive from table metadata; they are configuration faults all
/// the same, since the remedy is a rewrite rule or a registered store.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum ConfigViolation {
    /// No table is configured: a server would serve nothing and report
    /// ready, and a one-shot would have nothing to open. The file is at
    /// fault, so `serve` refuses to start, a reload is refused, and a
    /// one-shot fails, as for any other violation.
    #[error("no table is configured; list at least one under [[tables]]")]
    NoTables,
    /// A table has an empty name.
    #[error("tables[{index}].name is empty")]
    EmptyTableName {
        /// The table's position in `tables`.
        index: usize,
    },
    /// A table has the name of a table before it.
    #[error("tables[{index}].name {name} is configured more than once")]
    DuplicateTableName {
        /// The later table's position in `tables`.
        index: usize,
        /// The name both carry.
        name: String,
    },
    /// One of the two static S3 keys is set without the other.
    #[error(
        "storage.s3.access_key_id and storage.s3.secret_access_key are set together or not at all"
    )]
    IncompleteStaticKeys,
    /// A resident-bytes budget on a table whose residency has nothing to budget.
    #[error("table {table}: resident_budget_bytes is set, but {residency} residency takes none")]
    BudgetWithoutResidency {
        /// The table's name.
        table: String,
        /// Its residency.
        residency: Residency,
    },
    /// Disk residency with no local directory to write chunk files to.
    /// Raised by validation and again by a load, which knows the table by
    /// name alone.
    #[error("table {table}: disk residency needs a local directory under storage.local")]
    DiskResidencyWithoutLocalDir {
        /// The table's name.
        table: String,
    },
    /// The memory budget of the page cache cannot hold one value page in
    /// every shard of its memory level, so the cache would keep a page per
    /// shard however small the budget, and the budget would bound nothing
    /// (`cache::min_memory_bytes`). `Config::parse` checks the budget
    /// against one shard, the least any cache has; opening the cache
    /// checks it against the shards it has on the runtime it opens on, one
    /// per worker thread rounded up to a power of two.
    #[error(
        "server.page_cache_bytes {bytes} is below one value page per cache shard, {minimum} bytes for {shards} shards"
    )]
    PageCacheTooSmall {
        /// The configured budget.
        bytes: u64,
        /// The smallest budget that holds a value page in every shard: one
        /// contract value page with its entry overhead per shard.
        minimum: u64,
        /// The shards the minimum is for.
        shards: usize,
    },
    /// A page cache disk level smaller than its one block, which would hold
    /// nothing (RFD 017); zero turns the level off instead.
    #[error(
        "storage.local.page_cache_disk_bytes {bytes} is below one block of {minimum} bytes; zero means no disk level"
    )]
    PageCacheDiskTooSmall {
        /// The configured size.
        bytes: u64,
        /// The smallest level that holds a page: one block.
        minimum: u64,
    },
    /// The hedge percentile is outside `(0, 1]`; the delay is that percentile
    /// of the latency window (RFD 013).
    #[error("storage.hedge.percentile {percentile} is not in (0, 1]")]
    HedgePercentileOutOfRange {
        /// The configured percentile.
        percentile: f64,
    },
    /// The hedge ceiling is outside `[0, 1]`; it is the share of requests
    /// that may hedge (RFD 013).
    #[error("storage.hedge.max_rate {max_rate} is not in [0, 1]")]
    HedgeMaxRateOutOfRange {
        /// The configured ceiling.
        max_rate: f64,
    },
    /// The hedge window holds no samples, so it has no percentile.
    #[error("storage.hedge.window 0 holds no sample; it must be at least 1")]
    HedgeWindowEmpty,
    /// A poll interval below `MIN_POLL_INTERVAL`: each table's poll would
    /// call the catalog or read its version hint in a busy loop, and at
    /// zero the runtime's interval refuses the period.
    #[error(
        "server.poll_interval {} is below the minimum of {}",
        time::friendly(*interval),
        time::friendly(*minimum)
    )]
    PollIntervalTooShort {
        /// The configured interval.
        interval: Duration,
        /// `MIN_POLL_INTERVAL`.
        minimum: Duration,
    },
    /// A poll interval above `MAX_POLL_INTERVAL`, past which a table is not
    /// followed and the first tick's `Instant` can overflow.
    #[error(
        "server.poll_interval {} is above the maximum of {}",
        time::friendly(*interval),
        time::friendly(*maximum)
    )]
    PollIntervalTooLong {
        /// The configured interval.
        interval: Duration,
        /// `MAX_POLL_INTERVAL`.
        maximum: Duration,
    },
    /// A `Get` deadline of zero, which would fail every lookup unstarted.
    #[error("server.get_deadline 0s is not above zero")]
    GetDeadlineZero,
    /// A `MultiGet` deadline of zero, which would fail every batch unstarted.
    #[error("server.multi_get_deadline 0s is not above zero")]
    MultiGetDeadlineZero,
    /// No request may be in flight, so every request would be refused.
    #[error("server.max_in_flight 0 is not above zero")]
    MaxInFlightZero,
    /// No key may be in a `MultiGet`, so every batch would be refused.
    #[error("server.max_multi_get_keys 0 is not above zero")]
    MaxMultiGetKeysZero,
    /// A load deadline of zero, which would fail every first load
    /// unstarted.
    #[error("server.load_deadline 0s is not above zero")]
    LoadDeadlineZero,
    /// A refresh deadline of zero, which would fail every refresh
    /// unstarted.
    #[error("server.refresh_deadline 0s is not above zero")]
    RefreshDeadlineZero,
    /// A drain deadline of zero, which would cut every open request off at
    /// shutdown.
    #[error("server.drain_deadline 0s is not above zero")]
    DrainDeadlineZero,
    /// An object-store connect timeout of zero, which would fail every
    /// connection before it opened.
    #[error("storage.connect_timeout 0s is not above zero")]
    ConnectTimeoutZero,
    /// A catalog timeout of zero, which would fail every catalog call
    /// unstarted.
    #[error("storage.catalog_timeout 0s is not above zero")]
    CatalogTimeoutZero,
    /// Two standard log fields emitted under one name, so every line would
    /// lose one of them (RFD 012).
    #[error(
        "server.log.fields: {first} and {second} are both emitted as {name}; each needs its own name"
    )]
    LogFieldCollision {
        /// The emitted name both fields map to.
        name: String,
        /// The field earlier in `StandardField::ALL`.
        first: StandardField,
        /// The field later in `StandardField::ALL`.
        second: StandardField,
    },
    /// The rewrite rule at `index` of `storage.rewrite` is one
    /// `RewriteRule::fault` refuses.
    #[error("storage.rewrite[{index}]: {fault}")]
    Rewrite {
        /// The rule's position in the file's list.
        index: usize,
        /// What is wrong with it.
        fault: RewriteFault,
    },
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
    /// Text that names no residency tier; raised by `Residency`'s `FromStr`.
    /// The command line reads the names it offers back through it, and
    /// clap refuses any other text before it gets here.
    #[error("unknown residency {text}; expected one of {}", Residency::names())]
    UnknownResidency {
        /// The text as given.
        text: String,
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
    /// The tables to serve: at least one, with names unique and non-empty.
    #[serde(default)]
    pub tables: Vec<TableConfig>,
}

impl Config {
    /// Parses TOML or JSON, decided by content: a first non-whitespace `{`
    /// is JSON. The result is validated once, so the two syntaxes cannot
    /// disagree on what is accepted. The error is a `ConfigError` alone,
    /// so a caller that read the text from a file can name the file beside
    /// it.
    pub fn parse(text: &str) -> std::result::Result<Self, ConfigError> {
        let config = Self::from_text(text)?;
        validate(&config)?;
        Ok(config)
    }

    /// The text as either syntax reads it, not yet validated. The syntax
    /// is checked apart from the schema, so a document that parses is never
    /// reported as malformed TOML or JSON: TOML is parsed whole before it is
    /// deserialized, and a JSON error is sorted by `serde_json`'s category.
    fn from_text(text: &str) -> std::result::Result<Self, ConfigError> {
        if text.trim_start().starts_with(JSON_OPENING) {
            let mut document = serde_json::Deserializer::from_str(text);
            let config = serde_path_to_error::deserialize(&mut document).map_err(|error| {
                let at = error.path().clone();
                let inner = error.into_inner();
                match inner.classify() {
                    Category::Data => ConfigError::Schema {
                        at,
                        fault: Box::new(SchemaFault::Json(inner)),
                    },
                    Category::Io | Category::Syntax | Category::Eof => ConfigError::Json(inner),
                }
            })?;
            document.end().map_err(ConfigError::Json)?;
            Ok(config)
        } else {
            let document = toml::Deserializer::parse(text).map_err(ConfigError::Toml)?;
            Ok(
                serde_path_to_error::deserialize(document).map_err(|error| {
                    ConfigError::Schema {
                        at: error.path().clone(),
                        fault: Box::new(SchemaFault::Toml(error.into_inner())),
                    }
                })?,
            )
        }
    }

    /// The configuration, checked against every rule `parse` checks, so a
    /// configuration built in code is held to the same rules as one read
    /// from a file; the first rule it breaks is `ConfigError::Invalid`.
    pub fn validated(self) -> Result<Self> {
        validate(&self).map_err(ConfigError::Invalid)?;
        Ok(self)
    }

    /// The settings outside `tables` whose value in `next` differs from
    /// this configuration's, by their names in the file, in file order:
    /// what a reload refuses, since the server builds them once at start
    /// (RFD 018). Empty when only the tables differ. Every field is named
    /// in a destructuring with no rest pattern, so a setting added later
    /// does not compile until it is compared.
    pub fn restart_settings(&self, next: &Self) -> Vec<&'static str> {
        let ServerConfig {
            listen,
            metrics_listen,
            poll_interval,
            page_cache_bytes,
            log,
            get_deadline,
            multi_get_deadline,
            max_in_flight,
            max_multi_get_keys,
            load_deadline,
            refresh_deadline,
            drain_deadline,
        } = &self.server;
        let StorageConfig {
            s3,
            rewrite,
            hedge,
            local,
            max_retries,
            connect_timeout,
            catalog_timeout,
        } = &self.storage;
        let server = &next.server;
        let storage = &next.storage;
        let compared = [
            ("server.listen", *listen == server.listen),
            (
                "server.metrics_listen",
                *metrics_listen == server.metrics_listen,
            ),
            (
                "server.poll_interval",
                *poll_interval == server.poll_interval,
            ),
            (
                "server.page_cache_bytes",
                *page_cache_bytes == server.page_cache_bytes,
            ),
            ("server.log", *log == server.log),
            ("server.get_deadline", *get_deadline == server.get_deadline),
            (
                "server.multi_get_deadline",
                *multi_get_deadline == server.multi_get_deadline,
            ),
            (
                "server.max_in_flight",
                *max_in_flight == server.max_in_flight,
            ),
            (
                "server.max_multi_get_keys",
                *max_multi_get_keys == server.max_multi_get_keys,
            ),
            (
                "server.load_deadline",
                *load_deadline == server.load_deadline,
            ),
            (
                "server.refresh_deadline",
                *refresh_deadline == server.refresh_deadline,
            ),
            (
                "server.drain_deadline",
                *drain_deadline == server.drain_deadline,
            ),
            ("storage.s3", *s3 == storage.s3),
            ("storage.rewrite", *rewrite == storage.rewrite),
            ("storage.hedge", *hedge == storage.hedge),
            ("storage.local", *local == storage.local),
            ("storage.max_retries", *max_retries == storage.max_retries),
            (
                "storage.connect_timeout",
                *connect_timeout == storage.connect_timeout,
            ),
            (
                "storage.catalog_timeout",
                *catalog_timeout == storage.catalog_timeout,
            ),
        ];
        compared
            .into_iter()
            .filter(|(_, same)| !same)
            .map(|(name, _)| name)
            .collect()
    }
}

/// The first rule the configuration breaks, or `Ok` when it breaks none.
fn validate(config: &Config) -> std::result::Result<(), ConfigViolation> {
    validate_bounds(&config.server, &config.storage)?;
    validate_hedge(&config.storage.hedge)?;
    validate_log(&config.server.log)?;
    if let Some((index, fault)) = config
        .storage
        .rewrite
        .iter()
        .enumerate()
        .find_map(|(index, rule)| rule.fault().map(|fault| (index, fault)))
    {
        return Err(ConfigViolation::Rewrite { index, fault });
    }
    if let Some(s3) = &config.storage.s3
        && s3.access_key_id.is_some() != s3.secret_access_key.is_some()
    {
        return Err(ConfigViolation::IncompleteStaticKeys);
    }
    let minimum = min_memory_bytes(PARSE_CACHE_SHARDS);
    if approx::count(config.server.page_cache_bytes) < minimum {
        return Err(ConfigViolation::PageCacheTooSmall {
            bytes: approx::count(config.server.page_cache_bytes),
            minimum,
            shards: PARSE_CACHE_SHARDS,
        });
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
    if config.tables.is_empty() {
        return Err(ConfigViolation::NoTables);
    }
    let mut names = HashSet::new();
    for (index, table) in config.tables.iter().enumerate() {
        if table.name.is_empty() {
            return Err(ConfigViolation::EmptyTableName { index });
        }
        if !names.insert(table.name.as_str()) {
            return Err(ConfigViolation::DuplicateTableName {
                index,
                name: table.name.clone(),
            });
        }
        match table.residency {
            Residency::Full => {}
            Residency::Disk | Residency::Bloom => {
                if table.resident_budget_bytes.is_some() {
                    return Err(ConfigViolation::BudgetWithoutResidency {
                        table: table.name.clone(),
                        residency: table.residency,
                    });
                }
            }
        }
        if table.residency == Residency::Disk && config.storage.local.is_none() {
            return Err(ConfigViolation::DiskResidencyWithoutLocalDir {
                table: table.name.clone(),
            });
        }
    }
    Ok(())
}

/// The poll interval is within `MIN_POLL_INTERVAL..=MAX_POLL_INTERVAL`,
/// and every other deadline, timeout, and limit that a zero would turn into
/// "refuse everything" is above zero. `max_retries` is not among them: zero
/// retries is a request tried once.
fn validate_bounds(
    server: &ServerConfig,
    storage: &StorageConfig,
) -> std::result::Result<(), ConfigViolation> {
    let interval = server.poll_interval;
    if interval < MIN_POLL_INTERVAL {
        return Err(ConfigViolation::PollIntervalTooShort {
            interval,
            minimum: MIN_POLL_INTERVAL,
        });
    }
    if interval > MAX_POLL_INTERVAL {
        return Err(ConfigViolation::PollIntervalTooLong {
            interval,
            maximum: MAX_POLL_INTERVAL,
        });
    }
    let durations = [
        (server.get_deadline, ConfigViolation::GetDeadlineZero),
        (
            server.multi_get_deadline,
            ConfigViolation::MultiGetDeadlineZero,
        ),
        (server.load_deadline, ConfigViolation::LoadDeadlineZero),
        (
            server.refresh_deadline,
            ConfigViolation::RefreshDeadlineZero,
        ),
        (server.drain_deadline, ConfigViolation::DrainDeadlineZero),
        (storage.connect_timeout, ConfigViolation::ConnectTimeoutZero),
        (storage.catalog_timeout, ConfigViolation::CatalogTimeoutZero),
    ];
    let limits = [
        (server.max_in_flight, ConfigViolation::MaxInFlightZero),
        (
            server.max_multi_get_keys,
            ConfigViolation::MaxMultiGetKeysZero,
        ),
    ];
    if let Some((_, violation)) = durations
        .into_iter()
        .find(|(duration, _)| duration.is_zero())
    {
        return Err(violation);
    }
    match limits.into_iter().find(|(limit, _)| *limit == 0) {
        Some((_, violation)) => Err(violation),
        None => Ok(()),
    }
}

/// Each standard field is emitted under a name of its own; the first pair
/// in `StandardField::ALL` order that shares one is the violation.
fn validate_log(log: &LogConfig) -> std::result::Result<(), ConfigViolation> {
    for (at, &second) in StandardField::ALL.iter().enumerate() {
        let name = log.emitted_name(second);
        if let Some(&first) = StandardField::ALL[..at]
            .iter()
            .find(|&&earlier| log.emitted_name(earlier) == name)
        {
            return Err(ConfigViolation::LogFieldCollision {
                name: name.to_string(),
                first,
                second,
            });
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

/// Listeners, polling, the page cache, logging, and the bounds of the live
/// and background paths (RFD 013). `metrics_listen` is always bound: it
/// serves `/metrics` and `/ready`, and RFD 012 makes both part of the
/// surface. Every duration is written as a friendly duration such as
/// `"5s"`. `Config::validated` holds `poll_interval` between one second
/// and one day, and every other duration, `max_in_flight`, and
/// `max_multi_get_keys` above zero.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ServerConfig {
    /// The gRPC listener.
    pub listen: SocketAddr,
    /// The `/metrics` and `/ready` listener.
    pub metrics_listen: SocketAddr,
    /// How often each table's ref is re-resolved (RFD 005); written as a
    /// friendly duration such as `"10s"`, from `MIN_POLL_INTERVAL` (one
    /// second) to `MAX_POLL_INTERVAL` (one day).
    #[serde(with = "time::friendly_duration")]
    pub poll_interval: Duration,
    /// The value-page cache budget in bytes; at least one contract value
    /// page with its entry overhead per shard of the cache's memory level
    /// (`cache::min_memory_bytes`), or the budget would bound nothing. The
    /// shards are one per worker thread rounded up to a power of two, so
    /// parsing checks one shard's minimum and opening the cache checks the
    /// rest.
    pub page_cache_bytes: usize,
    /// Log field renames and additions.
    pub log: LogConfig,
    /// How long a `Get` may run before it is `DEADLINE_EXCEEDED`, and how
    /// long each page fetch of a lookup may run. A client's own deadline is
    /// enforced by the gRPC layer and reported as the client's.
    #[serde(with = "time::friendly_duration")]
    pub get_deadline: Duration,
    /// How long a `MultiGet` may run before it is `DEADLINE_EXCEEDED`,
    /// whatever its size. A client's own deadline is enforced by the gRPC
    /// layer and reported as the client's.
    #[serde(with = "time::friendly_duration")]
    pub multi_get_deadline: Duration,
    /// Requests, `Get` and `MultiGet` together, in flight at once; one more
    /// is refused at once as `RESOURCE_EXHAUSTED`, never queued.
    pub max_in_flight: usize,
    /// Keys one `MultiGet` may carry; more is `INVALID_ARGUMENT` before any
    /// lookup.
    pub max_multi_get_keys: usize,
    /// How long a build from no serving generation, a table's first load
    /// or a retry after one failed, may run before it fails and the table
    /// is `Degraded`; the next poll tries again.
    #[serde(with = "time::friendly_duration")]
    pub load_deadline: Duration,
    /// How long one refresh over a serving generation may run before it
    /// fails and the table is `Degraded`, the generation still serving; the
    /// next poll tries again.
    #[serde(with = "time::friendly_duration")]
    pub refresh_deadline: Duration,
    /// How long a shutdown waits for open requests before it stops waiting,
    /// closes the page cache, and ends the run.
    #[serde(with = "time::friendly_duration")]
    pub drain_deadline: Duration,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: DEFAULT_LISTEN,
            metrics_listen: DEFAULT_METRICS_LISTEN,
            poll_interval: DEFAULT_POLL_INTERVAL,
            page_cache_bytes: DEFAULT_PAGE_CACHE_BYTES,
            log: LogConfig::default(),
            get_deadline: DEFAULT_GET_DEADLINE,
            multi_get_deadline: DEFAULT_MULTI_GET_DEADLINE,
            max_in_flight: DEFAULT_MAX_IN_FLIGHT,
            max_multi_get_keys: DEFAULT_MAX_MULTI_GET_KEYS,
            load_deadline: DEFAULT_LOAD_DEADLINE,
            refresh_deadline: DEFAULT_REFRESH_DEADLINE,
            drain_deadline: DEFAULT_DRAIN_DEADLINE,
        }
    }
}

/// Logs are JSON only (RFD 012). `fields` renames the standard fields and
/// `static_fields` are added to every event; both are empty by default. A
/// key of `fields` that names no standard field fails the parse, and
/// `Config::validated` refuses two standard fields emitted under one name
/// (`ConfigViolation::LogFieldCollision`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct LogConfig {
    /// Standard field to the name it is emitted under.
    pub fields: BTreeMap<StandardField, String>,
    /// Field name to a constant value added to every event.
    pub static_fields: BTreeMap<String, String>,
}

impl LogConfig {
    /// The name `field` is emitted under: the name `fields` maps it to, or
    /// its own.
    pub fn emitted_name(&self, field: StandardField) -> &str {
        self.fields
            .get(&field)
            .map_or_else(|| field.name(), String::as_str)
    }
}

/// The standard fields of every log event (RFD 012), each renamable through
/// `LogConfig.fields`. Display and serde give the lowercase name the file
/// uses, which is also the name a field is emitted under by default.
// The serde names and Display are the same strings;
// `test_serialize_standard_field_matches_display` holds them together.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StandardField {
    /// When the event happened, RFC 3339 in UTC.
    Time,
    /// The event's level, lowercase.
    Level,
    /// The event's message.
    Message,
    /// The module the event came from.
    Target,
}

impl StandardField {
    /// Every standard field, for the collision check and the tests that run
    /// a case per field.
    pub(crate) const ALL: [Self; 4] = [Self::Time, Self::Level, Self::Message, Self::Target];

    /// The field's lowercase name.
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Time => "time",
            Self::Level => "level",
            Self::Message => "message",
            Self::Target => "target",
        }
    }
}

impl fmt::Display for StandardField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
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

impl RewriteRule {
    /// What makes the rule unusable, or `None`: the one check of a rule,
    /// which a file's rules and the command line's both go through. An
    /// empty `from` is a prefix of every location, so the rule would
    /// rewrite all of them; an empty `to` turns every location it matches
    /// into a relative path, which names no store.
    pub fn fault(&self) -> Option<RewriteFault> {
        if self.from.is_empty() {
            Some(RewriteFault::EmptyFrom)
        } else if self.to.is_empty() {
            Some(RewriteFault::EmptyTo)
        } else {
            None
        }
    }
}

/// Why a rewrite rule is unusable (`RewriteRule::fault`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RewriteFault {
    /// The `from` prefix is empty.
    #[error("from is empty")]
    EmptyFrom,
    /// The `to` prefix is empty.
    #[error("to is empty")]
    EmptyTo,
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

/// Object storage, hedging, the local directory, and the bounds on every
/// request to the object store and the catalog (RFD 013). The two
/// durations are written as friendly durations such as `"1s"` and are
/// above zero; `max_retries` may be zero.
#[derive(Clone, Debug, PartialEq, Deserialize)]
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
    /// Retries of one failed S3 request, on the live and background paths
    /// alike; zero tries each request once.
    pub max_retries: usize,
    /// How long an S3 connection may take to open.
    #[serde(with = "time::friendly_duration")]
    pub connect_timeout: Duration,
    /// How long one catalog call may run before the poll or load that made
    /// it fails.
    #[serde(with = "time::friendly_duration")]
    pub catalog_timeout: Duration,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            s3: None,
            rewrite: Vec::new(),
            hedge: HedgeConfig::default(),
            local: None,
            max_retries: DEFAULT_MAX_RETRIES,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            catalog_timeout: DEFAULT_CATALOG_TIMEOUT,
        }
    }
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
    /// Refuse a generation the doctor finds inefficient (RFD 007).
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

impl Residency {
    /// Every tier, for the parser, the refusal message, and the tests that
    /// run a case per tier.
    pub(crate) const ALL: [Self; 3] = [Self::Full, Self::Disk, Self::Bloom];

    /// Every tier's name, comma-separated, for the message that refuses a
    /// name; rendered from `ALL` so the list cannot lag the enum.
    fn names() -> String {
        Self::ALL
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// The tier's lowercase name, as the file spells it.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Disk => "disk",
            Self::Bloom => "bloom",
        }
    }
}

impl fmt::Display for Residency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl std::str::FromStr for Residency {
    type Err = ConfigViolation;

    /// Exactly the lowercase names `Display` prints; the one boundary parser
    /// for the setting when it arrives from the command line rather than
    /// the file, where serde reads the same names.
    fn from_str(text: &str) -> std::result::Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|residency| residency.to_string() == text)
            .ok_or_else(|| ConfigViolation::UnknownResidency {
                text: text.to_string(),
            })
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
    use serde_path_to_error::Segment;

    use super::*;
    use crate::contract::VALUE_PAGE_BYTES;
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
    /// The `Get` deadline the full fixture names, `"2s"`.
    const FIXTURE_GET_DEADLINE: Duration = Duration::from_secs(2);
    /// The `MultiGet` deadline the full fixture names, `"8s"`.
    const FIXTURE_MULTI_GET_DEADLINE: Duration = Duration::from_secs(8);
    /// The in-flight request limit the full fixture names.
    const FIXTURE_MAX_IN_FLIGHT: usize = 512;
    /// The `MultiGet` key limit the full fixture names.
    const FIXTURE_MAX_MULTI_GET_KEYS: usize = 500;
    /// The load deadline the full fixture names, `"45m"`.
    const FIXTURE_LOAD_DEADLINE: Duration = Duration::from_mins(45);
    /// The refresh deadline the full fixture names, `"10m"`.
    const FIXTURE_REFRESH_DEADLINE: Duration = Duration::from_mins(10);
    /// The drain deadline the full fixture names, `"25s"`.
    const FIXTURE_DRAIN_DEADLINE: Duration = Duration::from_secs(25);
    /// The S3 retry count the full fixture names.
    const FIXTURE_MAX_RETRIES: usize = 3;
    /// The S3 connect timeout the full fixture names, `"500ms"`.
    const FIXTURE_CONNECT_TIMEOUT: Duration = Duration::from_millis(500);
    /// The catalog timeout the full fixture names, `"1m"`.
    const FIXTURE_CATALOG_TIMEOUT: Duration = Duration::from_secs(60);
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
get_deadline = "2s"
multi_get_deadline = "8s"
max_in_flight = {FIXTURE_MAX_IN_FLIGHT}
max_multi_get_keys = {FIXTURE_MAX_MULTI_GET_KEYS}
load_deadline = "45m"
refresh_deadline = "10m"
drain_deadline = "25s"

[server.log.fields]
time = "ts"
level = "severity"

[server.log.static_fields]
service = "icetray"

[storage]
max_retries = {FIXTURE_MAX_RETRIES}
connect_timeout = "500ms"
catalog_timeout = "1m"

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
                "get_deadline": "2s",
                "multi_get_deadline": "8s",
                "max_in_flight": FIXTURE_MAX_IN_FLIGHT,
                "max_multi_get_keys": FIXTURE_MAX_MULTI_GET_KEYS,
                "load_deadline": "45m",
                "refresh_deadline": "10m",
                "drain_deadline": "25s",
                "log": {
                    "fields": {"time": "ts", "level": "severity"},
                    "static_fields": {"service": "icetray"}
                }
            },
            "storage": {
                "max_retries": FIXTURE_MAX_RETRIES,
                "connect_timeout": "500ms",
                "catalog_timeout": "1m",
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
                        (StandardField::Time, "ts".to_string()),
                        (StandardField::Level, "severity".to_string()),
                    ]),
                    static_fields: BTreeMap::from([("service".to_string(), "icetray".to_string())]),
                },
                get_deadline: FIXTURE_GET_DEADLINE,
                multi_get_deadline: FIXTURE_MULTI_GET_DEADLINE,
                max_in_flight: FIXTURE_MAX_IN_FLIGHT,
                max_multi_get_keys: FIXTURE_MAX_MULTI_GET_KEYS,
                load_deadline: FIXTURE_LOAD_DEADLINE,
                refresh_deadline: FIXTURE_REFRESH_DEADLINE,
                drain_deadline: FIXTURE_DRAIN_DEADLINE,
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
                max_retries: FIXTURE_MAX_RETRIES,
                connect_timeout: FIXTURE_CONNECT_TIMEOUT,
                catalog_timeout: FIXTURE_CATALOG_TIMEOUT,
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
        let Err(ConfigError::Invalid(violation)) = &result else {
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
        assert_eq!(config.server.get_deadline, DEFAULT_GET_DEADLINE);
        assert_eq!(config.server.multi_get_deadline, DEFAULT_MULTI_GET_DEADLINE);
        assert_eq!(config.server.max_in_flight, DEFAULT_MAX_IN_FLIGHT);
        assert_eq!(config.server.max_multi_get_keys, DEFAULT_MAX_MULTI_GET_KEYS);
        assert_eq!(config.server.load_deadline, DEFAULT_LOAD_DEADLINE);
        assert_eq!(config.server.refresh_deadline, DEFAULT_REFRESH_DEADLINE);
        assert_eq!(config.server.drain_deadline, DEFAULT_DRAIN_DEADLINE);
        assert_eq!(config.storage, StorageConfig::default());
        assert_eq!(config.storage.max_retries, DEFAULT_MAX_RETRIES);
        assert_eq!(config.storage.connect_timeout, DEFAULT_CONNECT_TIMEOUT);
        assert_eq!(config.storage.catalog_timeout, DEFAULT_CATALOG_TIMEOUT);
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

    /// One step of a schema fault's place, comparable.
    #[derive(Debug, PartialEq, Eq)]
    enum At {
        /// A key of a table or object.
        Key(String),
        /// An entry of a list.
        Index(usize),
        /// A variant of an externally tagged enum.
        Variant(String),
        /// A step serde could not name.
        Unknown,
    }

    /// Where `text`'s schema fault is, as the steps serde walked to it.
    /// Anything but a schema fault fails the test.
    fn schema_fault_at(text: &str) -> Vec<At> {
        match Config::parse(text) {
            Err(ConfigError::Schema { at, .. }) => at
                .iter()
                .map(|segment| match segment {
                    Segment::Map { key } => At::Key(key.clone()),
                    Segment::Seq { index } => At::Index(*index),
                    Segment::Enum { variant } => At::Variant(variant.clone()),
                    Segment::Unknown => At::Unknown,
                })
                .collect(),
            other => panic!("expected a schema fault, got {other:?}"),
        }
    }

    /// The step to `key`.
    fn key(key: &str) -> At {
        At::Key(key.to_string())
    }

    #[test]
    fn test_parse_rejects_unknown_field_as_a_schema_fault() {
        let toml = format!("{TOML_MINIMAL}\n[server]\nlisten_addr = \"127.0.0.1:1\"\n");
        assert_eq!(schema_fault_at(&toml), [key("server"), key("listen_addr")]);
        let nested = format!("{TOML_MINIMAL}\n[storage.hedge]\npercentil = 0.5\n");
        assert_eq!(
            schema_fault_at(&nested),
            [key("storage"), key("hedge"), key("percentil")]
        );
        let json = r#"{"server": {"listen_addr": "127.0.0.1:1"}}"#;
        assert_eq!(schema_fault_at(json), [key("server"), key("listen_addr")]);
        // The source is the one internally tagged enum in the schema, which
        // serde buffers, so the fault is placed at the source itself.
        let source = r#"
[[tables]]
name = "kv"
ref = "main"
key_column = "key"
value_column = "value"

[tables.source]
type = "static"
location = "s3://example-warehouse/db/kv"
locaton = "typo"
"#;
        assert_eq!(
            schema_fault_at(source),
            [key("tables"), At::Index(0), key("source")]
        );
    }

    #[test]
    fn test_parse_reports_a_missing_key_at_its_table_in_either_syntax() {
        let toml = TOML_MINIMAL.replace("ref = \"main\"\n", "");
        let at_first_table = [key("tables"), At::Index(0)];
        assert_eq!(schema_fault_at(&toml), at_first_table);
        let json = r#"{"tables": [{"name": "kv", "source": {"type": "static", "location": "s3://example-warehouse/db/kv"}, "key_column": "key", "value_column": "value"}]}"#;
        assert_eq!(schema_fault_at(json), at_first_table);
    }

    #[test]
    fn test_parse_reports_a_value_of_the_wrong_form_at_its_key() {
        let toml = format!("{TOML_MINIMAL}\n[server]\npoll_interval = \"10\"\n");
        assert_eq!(
            schema_fault_at(&toml),
            [key("server"), key("poll_interval")]
        );
        let json = r#"{"server": {"max_in_flight": "many"}}"#;
        assert_eq!(schema_fault_at(json), [key("server"), key("max_in_flight")]);
    }

    #[test]
    fn test_place_spells_a_setting_as_the_file_does() {
        let text = TOML_MINIMAL.replace("ref = \"main\"\n", "ref = \"main\"\nstrict = 1\n");
        let Err(ConfigError::Schema { at, .. }) = Config::parse(&text) else {
            panic!("strict = 1 is the wrong type");
        };
        assert_eq!(place(&at), "tables[0].strict");
        let log = format!("{TOML_MINIMAL}\n[server.log.fields]\ntimestamp = \"ts\"\n");
        let Err(ConfigError::Schema { at, .. }) = Config::parse(&log) else {
            panic!("timestamp names no standard field");
        };
        assert_eq!(place(&at), "server.log.fields");
    }

    #[test]
    fn test_parse_reports_malformed_text_as_its_syntax_s_error() {
        let toml = Config::parse("[[tables]\n");
        assert!(matches!(toml, Err(ConfigError::Toml(_))), "{toml:?}");
        for json in [
            "{\"tables\": [",
            "{\"tables\": []} trailing",
            "{\"tables\" []}",
        ] {
            let result = Config::parse(json);
            assert!(
                matches!(result, Err(ConfigError::Json(_))),
                "{json}: {result:?}"
            );
        }
    }

    #[test]
    fn test_parse_rejects_a_log_field_that_names_no_standard_field() {
        let toml = format!("{TOML_MINIMAL}\n[server.log.fields]\ntimestamp = \"ts\"\n");
        let at_fields = [key("server"), key("log"), key("fields"), At::Unknown];
        assert_eq!(schema_fault_at(&toml), at_fields);
        let json = r#"{"server": {"log": {"fields": {"timestamp": "ts"}}}}"#;
        assert_eq!(schema_fault_at(json), at_fields);
    }

    #[test]
    fn test_parse_refuses_two_log_fields_under_one_name() {
        let onto_a_default = format!("{TOML_MINIMAL}\n[server.log.fields]\nmessage = \"level\"\n");
        assert_eq!(
            violation(&onto_a_default),
            ConfigViolation::LogFieldCollision {
                name: "level".to_string(),
                first: StandardField::Level,
                second: StandardField::Message,
            }
        );
        let two_onto_one =
            format!("{TOML_MINIMAL}\n[server.log.fields]\ntime = \"x\"\ntarget = \"x\"\n");
        assert_eq!(
            violation(&two_onto_one),
            ConfigViolation::LogFieldCollision {
                name: "x".to_string(),
                first: StandardField::Time,
                second: StandardField::Target,
            }
        );
        let swapped = format!(
            "{TOML_MINIMAL}\n[server.log.fields]\nlevel = \"message\"\nmessage = \"level\"\n"
        );
        assert!(Config::parse(&swapped).is_ok());
    }

    #[test]
    fn test_emitted_name_is_the_mapped_name_or_the_field_s_own() {
        let log = full_config().server.log;
        assert_eq!(log.emitted_name(StandardField::Time), "ts");
        assert_eq!(log.emitted_name(StandardField::Level), "severity");
        assert_eq!(log.emitted_name(StandardField::Message), "message");
        assert_eq!(log.emitted_name(StandardField::Target), "target");
    }

    #[test]
    fn test_from_text_dispatches_on_first_non_whitespace_brace() {
        let json = Config::from_text("  \n\t {\"tables\": []}").unwrap();
        assert!(json.tables.is_empty());
        let toml = Config::from_text("  \n\t tables = []\n").unwrap();
        assert!(toml.tables.is_empty());
        // A brace later in the text does not make TOML into JSON.
        let inline = Config::from_text("storage = { rewrite = [] }\n").unwrap();
        assert!(inline.storage.rewrite.is_empty());
        let malformed_json = Config::from_text(" { not json");
        assert!(matches!(malformed_json, Err(ConfigError::Json(_))));
        let malformed_toml = Config::from_text("not = toml = either");
        assert!(matches!(malformed_toml, Err(ConfigError::Toml(_))));
    }

    #[test]
    fn test_parse_refuses_a_configuration_with_no_table() {
        for text in ["", "tables = []\n", "{}", "{\"tables\": []}", "[server]\n"] {
            assert_eq!(violation(text), ConfigViolation::NoTables, "{text:?}");
        }
    }

    #[test]
    fn test_parse_refuses_empty_table_name() {
        let text = TOML_MINIMAL.replace("name = \"kv\"", "name = \"\"");
        assert_eq!(
            violation(&text),
            ConfigViolation::EmptyTableName { index: 0 }
        );
    }

    /// Two configurations that differ in their tables alone need no
    /// restart; each other setting that differs is named as the file
    /// names it.
    #[test]
    fn test_restart_settings_names_what_differs_outside_the_tables() {
        let running = Config::parse(TOML_MINIMAL).unwrap();
        let mut next = running.clone();
        next.tables.clear();
        assert!(running.restart_settings(&next).is_empty());
        next.server.poll_interval += Duration::from_secs(1);
        next.storage.max_retries += 1;
        assert_eq!(
            running.restart_settings(&next),
            ["server.poll_interval", "storage.max_retries"]
        );
    }

    #[test]
    fn test_parse_refuses_duplicate_table_name() {
        let text = format!("{TOML_MINIMAL}{TOML_MINIMAL}");
        assert_eq!(
            violation(&text),
            ConfigViolation::DuplicateTableName {
                index: 1,
                name: "kv".to_string(),
            }
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
        assert_eq!(
            violation(&bloom),
            ConfigViolation::BudgetWithoutResidency {
                table: "kv".to_string(),
                residency: Residency::Bloom,
            }
        );
        // Disk has its local directory, so the budget is the only fault.
        let disk = format!(
            "{}\nresidency = \"disk\"\nresident_budget_bytes = 1\n\n[storage.local]\ndir = \"/var/lib/icetray\"\npage_cache_disk_bytes = {PAGE_CACHE_DISK_BLOCK_BYTES}\n",
            TOML_MINIMAL.trim_end()
        );
        assert_eq!(
            violation(&disk),
            ConfigViolation::BudgetWithoutResidency {
                table: "kv".to_string(),
                residency: Residency::Disk,
            }
        );
    }

    #[test]
    fn test_parse_refuses_page_cache_below_a_value_page_per_shard() {
        let with_budget = |bytes: u64| {
            format!(
                "[server]\npage_cache_bytes = {bytes}\n{}",
                TOML_MINIMAL.trim_start()
            )
        };
        let minimum = min_memory_bytes(PARSE_CACHE_SHARDS);
        for bytes in [0, VALUE_PAGE_BYTES, minimum - 1] {
            assert_eq!(
                violation(&with_budget(bytes)),
                ConfigViolation::PageCacheTooSmall {
                    bytes,
                    minimum,
                    shards: PARSE_CACHE_SHARDS
                },
                "{bytes} bytes"
            );
        }
        let at_minimum = Config::parse(&with_budget(minimum)).unwrap();
        assert_eq!(approx::count(at_minimum.server.page_cache_bytes), minimum);
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
            ConfigViolation::DiskResidencyWithoutLocalDir {
                table: "kv".to_string(),
            }
        );
    }

    /// The smallest step a friendly duration can take, so a bound plus or
    /// minus it is the first interval past the bound.
    const SMALLEST_STEP: Duration = Duration::from_nanos(1);

    /// The violation `validated` finds in the minimal configuration with
    /// `poll_interval` set, or `None` when it accepts it.
    fn poll_interval_violation(interval: Duration) -> Option<ConfigViolation> {
        let mut config = Config::parse(TOML_MINIMAL).unwrap();
        config.server.poll_interval = interval;
        match config.validated() {
            Ok(_) => None,
            Err(Error::Config(ConfigError::Invalid(violation))) => Some(violation),
            Err(other) => panic!("expected a configuration violation, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_refuses_poll_interval_zero() {
        let text = format!("{TOML_MINIMAL}\n[server]\npoll_interval = \"0s\"\n");
        assert_eq!(
            violation(&text),
            ConfigViolation::PollIntervalTooShort {
                interval: Duration::ZERO,
                minimum: MIN_POLL_INTERVAL,
            }
        );
    }

    #[test]
    fn test_validated_holds_poll_interval_between_its_bounds() {
        let below = MIN_POLL_INTERVAL.checked_sub(SMALLEST_STEP).unwrap();
        assert_eq!(
            poll_interval_violation(below),
            Some(ConfigViolation::PollIntervalTooShort {
                interval: below,
                minimum: MIN_POLL_INTERVAL,
            })
        );
        assert_eq!(poll_interval_violation(MIN_POLL_INTERVAL), None);
        assert_eq!(poll_interval_violation(MAX_POLL_INTERVAL), None);
        let above = MAX_POLL_INTERVAL + SMALLEST_STEP;
        assert_eq!(
            poll_interval_violation(above),
            Some(ConfigViolation::PollIntervalTooLong {
                interval: above,
                maximum: MAX_POLL_INTERVAL,
            })
        );
        assert_eq!(
            poll_interval_violation(Duration::MAX),
            Some(ConfigViolation::PollIntervalTooLong {
                interval: Duration::MAX,
                maximum: MAX_POLL_INTERVAL,
            })
        );
    }

    #[test]
    fn test_parse_refuses_a_zero_bound() {
        let cases = [
            ("server", "get_deadline", ConfigViolation::GetDeadlineZero),
            (
                "server",
                "multi_get_deadline",
                ConfigViolation::MultiGetDeadlineZero,
            ),
            ("server", "load_deadline", ConfigViolation::LoadDeadlineZero),
            (
                "server",
                "refresh_deadline",
                ConfigViolation::RefreshDeadlineZero,
            ),
            (
                "server",
                "drain_deadline",
                ConfigViolation::DrainDeadlineZero,
            ),
            (
                "storage",
                "connect_timeout",
                ConfigViolation::ConnectTimeoutZero,
            ),
            (
                "storage",
                "catalog_timeout",
                ConfigViolation::CatalogTimeoutZero,
            ),
        ];
        for (section, key, expected) in cases {
            let text = format!("{TOML_MINIMAL}\n[{section}]\n{key} = \"0s\"\n");
            assert_eq!(violation(&text), expected, "{section}.{key}");
        }
        let limits = [
            ("max_in_flight", ConfigViolation::MaxInFlightZero),
            ("max_multi_get_keys", ConfigViolation::MaxMultiGetKeysZero),
        ];
        for (key, expected) in limits {
            let text = format!("{TOML_MINIMAL}\n[server]\n{key} = 0\n");
            assert_eq!(violation(&text), expected, "server.{key}");
        }
    }

    #[test]
    fn test_parse_accepts_zero_retries() {
        let text = format!("{TOML_MINIMAL}\n[storage]\nmax_retries = 0\n");
        assert_eq!(Config::parse(&text).unwrap().storage.max_retries, 0);
    }

    #[test]
    fn test_parse_refuses_a_rewrite_rule_with_an_empty_prefix() {
        let rule = |from: &str, to: &str| {
            format!("\n[[storage.rewrite]]\nfrom = \"{from}\"\nto = \"{to}\"\n")
        };
        let usable = rule("s3://old/", "s3://new/");
        let cases = [
            (rule("", "s3://new/"), RewriteFault::EmptyFrom),
            (rule("s3://old/", ""), RewriteFault::EmptyTo),
            (rule("", ""), RewriteFault::EmptyFrom),
        ];
        for (unusable, fault) in cases {
            let text = format!("{TOML_MINIMAL}{usable}{unusable}");
            assert_eq!(
                violation(&text),
                ConfigViolation::Rewrite { index: 1, fault },
                "{unusable}"
            );
        }
        assert!(Config::parse(&format!("{TOML_MINIMAL}{usable}")).is_ok());
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
        for residency in Residency::ALL {
            let json = serde_json::to_string(&residency).unwrap();
            assert_eq!(json, format!("\"{residency}\""));
            let back: Residency = serde_json::from_str(&json).unwrap();
            assert_eq!(back, residency);
        }
    }

    #[test]
    fn test_serialize_standard_field_matches_display() {
        for field in StandardField::ALL {
            let json = serde_json::to_string(&field).unwrap();
            assert_eq!(json, format!("\"{field}\""));
            let back: StandardField = serde_json::from_str(&json).unwrap();
            assert_eq!(back, field);
        }
    }

    #[test]
    fn test_from_str_residency_round_trips_display() {
        for residency in Residency::ALL {
            let parsed: Residency = residency.to_string().parse().unwrap();
            assert_eq!(parsed, residency);
        }
    }

    #[test]
    fn test_from_str_residency_refuses_another_word() {
        for text in ["memory", "Full", "FULL", "", " full"] {
            assert_eq!(
                text.parse::<Residency>(),
                Err(ConfigViolation::UnknownResidency {
                    text: text.to_string(),
                }),
                "{text:?}"
            );
        }
    }
}
