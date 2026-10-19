//! The defaults `docs/configuration.md` states are the defaults the code
//! builds. The reference is the one document that states a setting's
//! default (CONTRIBUTING.md, User documentation), so this test reads its
//! settings tables and compares each stated value with the configuration
//! the parser produces for a file that sets nothing but the required keys.
//!
//! The code side is read through exhaustive destructuring of each
//! configuration struct, so a key added to one fails to compile here until
//! the test names it, and every named key must then have a row. A row that
//! states no value (a required key, an unset one, a nested table) is listed
//! in `EXEMPT` with the words the document uses and the fact about the code
//! those words stand for.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use icetray_core::config::LogConfig;
use icetray_core::time::friendly_duration;
use icetray_core::{
    Config, HedgeConfig, LocalConfig, RewriteRule, S3Config, ServerConfig, SourceConfig,
    StorageConfig, TableConfig,
};
use serde::de::IntoDeserializer;
use serde::de::value::{Error as ValueError, StrDeserializer};

/// The configuration reference, read at compile time so a change to it
/// rebuilds this test.
const REFERENCE: &str = include_str!("../../../docs/configuration.md");

/// The fence that opens and closes an example, inside which a `#` is a
/// comment and not a heading.
const FENCE: &str = "```";

/// The columns of a settings table in the reference.
const SETTINGS_HEADER: [&str; 4] = ["Key", "Type", "Default", "Meaning"];

/// The heading of the section whose table lists each source kind's keys,
/// not their defaults.
const SOURCE_HEADING: &str = "[tables.source]";

/// The prefix of a source kind's keys: `tables.source.<kind>.<key>`.
const SOURCE_PREFIX: &str = "tables.source";

/// The words that say a source key must be present.
const REQUIRED_WORD: &str = "required";

/// The words that say a source key may be left out.
const OPTIONAL_WORD: &str = "optional";

/// Every heading with a settings table, and the dotted path its keys sit
/// under. A settings table under any other heading fails the test, so a new
/// section cannot go unchecked.
const SECTIONS: [(&str, &str); 10] = [
    ("[server]", "server"),
    ("[server.log]", "server.log"),
    ("[storage]", "storage"),
    ("[storage.s3]", "storage.s3"),
    ("[[storage.rewrite]]", "storage.rewrite"),
    ("[storage.hedge]", "storage.hedge"),
    ("[storage.local]", "storage.local"),
    ("[[tables]]", "tables"),
    ("glue", "tables.source.glue"),
    ("s3tables", "tables.source.s3tables"),
];

/// A table entry's required keys, up to its source.
const TABLE_HEAD: &str = r#"[[tables]]
name = "example"
ref = "main"
key_column = "key"
value_columns = ["value"]
"#;

/// The keys of `[storage.local]` and `[[storage.rewrite]]` that have no
/// default, set so the parser builds both, and an ordered table, so the
/// default `max_scan_rows` is one the parser applies.
const OTHER_REQUIRED_KEYS: &str = r#"
[storage.local]
dir = "/var/lib/icetray"
page_cache_disk_bytes = 0

[[storage.rewrite]]
from = "s3://one/"
to = "s3://two/"

[[tables]]
name = "ordered"
ref = "main"
key_column = "key"
value_columns = ["value"]
order_column = "ts"

[tables.source]
type = "static"
location = "/data/ordered"
"#;

/// A `[tables.source]` body of each kind, with only its required keys, so
/// the parser builds every variant of `SourceConfig`.
const SOURCES: [&str; 4] = [
    "type = \"static\"\nlocation = \"/data/example\"\n",
    "type = \"rest\"\nuri = \"http://catalog.example:8181\"\nnamespace = \"db\"\ntable = \"kv\"\n",
    "type = \"glue\"\ndatabase = \"db\"\ntable = \"kv\"\n",
    "type = \"s3tables\"\ntable_bucket_arn = \"arn:aws:s3tables:us-east-1:111122223333:bucket/warehouse\"\nnamespace = \"db\"\ntable = \"kv\"\n",
];

/// A value a setting can default to, in the form both sides are compared in.
#[derive(Clone, Debug, PartialEq)]
enum Setting {
    Duration(Duration),
    /// A byte size or a count.
    Count(u64),
    Number(f64),
    Flag(bool),
    Address(SocketAddr),
    Text(String),
    /// No value: an absent option or an empty collection.
    Unset,
}

/// What a row that states no value stands for in the code.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Exempt {
    /// The key has no default: the file must set it.
    Required,
    /// The code's default is no value: an absent option or an empty
    /// collection.
    Unset,
    /// The row names a nested table whose keys are rows of its own section.
    Nested,
}

/// A row exempt from the comparison.
struct Exemption {
    /// The row's dotted path.
    path: &'static str,
    /// The words its default cell must hold.
    cell: &'static str,
    /// What the words stand for in the code.
    kind: Exempt,
    /// Why the row has no value to compare.
    reason: &'static str,
}

/// The reason of a key the file must set.
const NO_DEFAULT: &str = "the key has no default; the file must set it";

/// The reason of an absent option.
const ABSENT_OPTION: &str = "an option, absent unless the file sets it";

/// The reason of a setting resolved from something else when it is absent.
const RESOLVED_ELSEWHERE: &str = "an option the SDK or the service resolves when it is absent";

/// A key the file must set, which its row says as `required`.
const fn required(path: &'static str) -> Exemption {
    Exemption {
        path,
        cell: "required",
        kind: Exempt::Required,
        reason: NO_DEFAULT,
    }
}

/// A key whose default is no value, which its row says as `cell`.
const fn unset(path: &'static str, cell: &'static str, reason: &'static str) -> Exemption {
    Exemption {
        path,
        cell,
        kind: Exempt::Unset,
        reason,
    }
}

/// A nested table, which its row says as `cell`.
const fn nested(path: &'static str, cell: &'static str, reason: &'static str) -> Exemption {
    Exemption {
        path,
        cell,
        kind: Exempt::Nested,
        reason,
    }
}

/// The rows whose default cell states no value. The words are pinned: a
/// row that changes them must be read again, and one that comes to state a
/// value is compared like any other.
const EXEMPT: &[Exemption] = &[
    unset("server.log.fields", "empty", "a map the file fills in"),
    unset(
        "server.log.static_fields",
        "empty",
        "a map the file fills in",
    ),
    unset("storage.s3", "absent", ABSENT_OPTION),
    unset("storage.rewrite", "empty", "a list the file fills in"),
    nested(
        "storage.hedge",
        "hedging on",
        "a table, its keys are rows of [storage.hedge]",
    ),
    unset("storage.local", "absent", ABSENT_OPTION),
    unset("storage.s3.endpoint", "AWS", RESOLVED_ELSEWHERE),
    unset("storage.s3.region", "the SDK's", RESOLVED_ELSEWHERE),
    unset("storage.s3.access_key_id", "none", ABSENT_OPTION),
    unset("storage.s3.secret_access_key", "none", ABSENT_OPTION),
    required("storage.rewrite.from"),
    required("storage.rewrite.to"),
    required("storage.local.dir"),
    required("storage.local.page_cache_disk_bytes"),
    unset(
        "storage.local.index_budget_bytes",
        "unbounded",
        "an option, no bound when absent",
    ),
    required("tables.name"),
    required("tables.ref"),
    required("tables.key_column"),
    required("tables.value_columns"),
    unset(
        "tables.order_column",
        "none",
        "an option, absent on a key-value table",
    ),
    required("tables.source"),
    unset(
        "tables.lookups",
        "the best level `residency` reaches",
        "an option that TableConfig::lookups resolves from the residency",
    ),
    unset(
        "tables.resident_budget_bytes",
        "unbounded",
        "an option, no bound when absent",
    ),
    required("tables.source.static.location"),
    required("tables.source.rest.uri"),
    unset("tables.source.rest.warehouse", "optional", ABSENT_OPTION),
    required("tables.source.rest.namespace"),
    required("tables.source.rest.table"),
    required("tables.source.glue.database"),
    required("tables.source.glue.table"),
    unset(
        "tables.source.glue.catalog_id",
        "the caller's account",
        RESOLVED_ELSEWHERE,
    ),
    unset("tables.source.glue.region", "as above", RESOLVED_ELSEWHERE),
    unset("tables.source.glue.endpoint", "AWS", RESOLVED_ELSEWHERE),
    required("tables.source.s3tables.table_bucket_arn"),
    required("tables.source.s3tables.namespace"),
    required("tables.source.s3tables.table"),
    unset(
        "tables.source.s3tables.region",
        "as above",
        RESOLVED_ELSEWHERE,
    ),
    unset("tables.source.s3tables.endpoint", "AWS", RESOLVED_ELSEWHERE),
];

/// One row of a settings table in the reference.
#[derive(Debug)]
struct Row {
    /// The dotted path of the section the row sits in.
    section: String,
    /// The section and the key.
    path: String,
    /// The Type cell: `duration`, `bytes`, `count`, `address`, `boolean`,
    /// `number`, `string`, or words for a key with no scalar default.
    type_cell: String,
    /// The Default cell, as written.
    default: String,
}

/// Why the reference and the code disagree.
#[derive(Debug, PartialEq)]
enum Failure {
    /// A settings table sits under a heading `SECTIONS` does not list.
    UnlistedSection { heading: String },
    /// A listed section has no settings table.
    MissingSection { section: &'static str },
    /// Two rows name one path.
    DuplicateRow { path: String },
    /// The row states a value its Type cell cannot read.
    UnreadableValue { path: String, text: String },
    /// The row states a value the code does not default to.
    Mismatch {
        path: String,
        documented: Setting,
        code: Setting,
    },
    /// The row states a value for a key the code has no default for.
    NoCodeDefault { path: String },
    /// The code has a default for a key the reference has no row for.
    NoRow { path: String },
    /// The row states no value and is not in `EXEMPT`.
    NotExempt { path: String, cell: String },
    /// An `EXEMPT` entry names no row.
    StaleExemption { path: String },
    /// An `EXEMPT` entry the row or the code no longer bears out.
    ExemptionWrong {
        path: String,
        kind: Exempt,
        reason: &'static str,
        cell: String,
    },
}

/// The cells of a Markdown table row, trimmed.
fn cells(line: &str) -> Vec<&str> {
    line.trim()
        .trim_matches('|')
        .split('|')
        .map(str::trim)
        .collect()
}

/// `text` without its backticks.
fn unticked(text: &str) -> String {
    text.replace('`', "")
}

/// The value a Default cell states: the first code span, when the cell
/// opens with one. A cell of words states none.
fn stated_value(cell: &str) -> Option<&str> {
    let (value, _) = cell.strip_prefix('`')?.split_once('`')?;
    Some(value)
}

/// `text` without its surrounding double quotes, or `None` when it has none.
fn unquoted(text: &str) -> Option<&str> {
    text.strip_prefix('"')?.strip_suffix('"')
}

/// A friendly duration, read by the configuration's own parser.
fn friendly(text: &str) -> Option<Duration> {
    let deserializer: StrDeserializer<'_, ValueError> = text.into_deserializer();
    friendly_duration::deserialize(deserializer).ok()
}

/// The value of a Default cell's code span, read as its Type cell says.
/// Integers may carry `_` or `,` separators.
fn documented(type_cell: &str, text: &str) -> Option<Setting> {
    match type_cell {
        "duration" => friendly(unquoted(text)?).map(Setting::Duration),
        "bytes" | "count" => text
            .replace(['_', ','], "")
            .parse()
            .ok()
            .map(Setting::Count),
        "address" => unquoted(text)?.parse().ok().map(Setting::Address),
        "boolean" => text.parse().ok().map(Setting::Flag),
        "number" => text.parse().ok().map(Setting::Number),
        "string" => unquoted(text).map(|text| Setting::Text(text.to_string())),
        _ => None,
    }
}

/// The keys of one source kind's Keys cell, each with `required` or
/// `optional`. The cell lists keys in groups split by `;`, and a group names
/// its keys in code spans before the word that gives their status.
fn source_keys(cell: &str) -> Vec<(String, &'static str)> {
    let mut keys = Vec::new();
    for group in cell.split(';') {
        let status = [REQUIRED_WORD, OPTIONAL_WORD]
            .into_iter()
            .filter_map(|word| group.find(word).map(|at| (at, word)))
            .min();
        let Some((at, word)) = status else { continue };
        keys.extend(
            group[..at]
                .split('`')
                .skip(1)
                .step_by(2)
                .map(|key| (key.to_string(), word)),
        );
    }
    keys
}

/// What a Markdown table is, by its header row.
enum Table {
    /// A settings table, its keys under this path.
    Settings(&'static str),
    /// The table of source kinds and their keys.
    Sources,
    /// Any other table.
    Other,
}

/// The rows of every settings table in `text`, with a failure for a
/// settings table under a heading `SECTIONS` does not list.
fn documented_rows(text: &str, failures: &mut Vec<Failure>) -> Vec<Row> {
    let mut rows = Vec::new();
    let mut heading = String::new();
    let mut in_fence = false;
    let mut table: Option<Table> = None;
    for line in text.lines() {
        if line.trim_start().starts_with(FENCE) {
            in_fence = !in_fence;
            table = None;
            continue;
        }
        if in_fence {
            continue;
        }
        if line.starts_with('#') {
            heading = unticked(line.trim_start_matches('#').trim());
            table = None;
            continue;
        }
        if !line.starts_with('|') {
            table = None;
            continue;
        }
        let row = cells(line);
        match &table {
            None if row == SETTINGS_HEADER => {
                let section = SECTIONS.iter().find(|(name, _)| *name == heading);
                table = Some(section.map_or_else(
                    || {
                        failures.push(Failure::UnlistedSection {
                            heading: heading.clone(),
                        });
                        Table::Other
                    },
                    |(_, prefix)| Table::Settings(prefix),
                ));
            }
            None if heading == SOURCE_HEADING && row.get(1) == Some(&"Keys") => {
                table = Some(Table::Sources);
            }
            None => table = Some(Table::Other),
            Some(Table::Settings(prefix)) if row.len() > 2 && !row[0].starts_with('-') => {
                rows.push(Row {
                    section: (*prefix).to_string(),
                    path: format!("{prefix}.{}", unticked(row[0])),
                    type_cell: row[1].to_string(),
                    default: row[2].to_string(),
                });
            }
            Some(Table::Sources) if row.len() > 1 && !row[0].starts_with('-') => {
                let kind = unticked(row[0]).replace('"', "");
                let section = format!("{SOURCE_PREFIX}.{kind}");
                // A kind with a settings table of its own is read from it.
                if SECTIONS.iter().all(|(_, prefix)| *prefix != section) {
                    rows.extend(source_keys(row[1]).into_iter().map(|(key, status)| Row {
                        section: section.clone(),
                        path: format!("{section}.{key}"),
                        type_cell: String::new(),
                        default: status.to_string(),
                    }));
                }
            }
            Some(Table::Settings(_) | Table::Sources | Table::Other) => {}
        }
    }
    rows
}

/// A default that is no value when `is_absent`, and a marker no row can
/// state when it is not.
fn absent(is_absent: bool) -> Setting {
    if is_absent {
        Setting::Unset
    } else {
        Setting::Text("a default value".to_string())
    }
}

/// A size or a count as a `Setting`.
fn count<T: TryInto<u64>>(value: T) -> Setting {
    Setting::Count(value.try_into().ok().expect("a default count fits u64"))
}

/// A configuration with one table, `source` being the body of its
/// `[tables.source]`.
fn with_source(source: &str) -> String {
    format!("{TABLE_HEAD}\n[tables.source]\n{source}")
}

/// The defaults of every key, by dotted path, with `put` as the way in.
struct Defaults(BTreeMap<String, Setting>);

impl Defaults {
    fn put(&mut self, path: &str, setting: Setting) {
        self.0.insert(path.to_string(), setting);
    }
}

/// The `[server]` and `[server.log]` defaults.
fn server_defaults(out: &mut Defaults, server: ServerConfig) {
    let ServerConfig {
        listen,
        metrics_listen,
        poll_interval,
        page_cache_bytes,
        log,
        get_deadline,
        multi_get_deadline,
        max_in_flight,
        max_multi_get_values,
        batch_fan_out,
        max_multi_scan_values,
        scan_deadline,
        multi_scan_deadline,
        max_scan_bytes,
        scan_page_cache_bytes,
        load_deadline,
        refresh_deadline,
        drain_deadline,
    } = server;
    out.put("server.listen", Setting::Address(listen));
    out.put("server.metrics_listen", Setting::Address(metrics_listen));
    out.put("server.poll_interval", Setting::Duration(poll_interval));
    out.put("server.page_cache_bytes", count(page_cache_bytes));
    out.put("server.get_deadline", Setting::Duration(get_deadline));
    out.put(
        "server.multi_get_deadline",
        Setting::Duration(multi_get_deadline),
    );
    out.put("server.max_in_flight", count(max_in_flight));
    out.put("server.max_multi_get_values", count(max_multi_get_values));
    out.put("server.batch_fan_out", count(batch_fan_out));
    out.put("server.max_multi_scan_values", count(max_multi_scan_values));
    out.put("server.max_scan_bytes", count(max_scan_bytes));
    out.put("server.scan_deadline", Setting::Duration(scan_deadline));
    out.put(
        "server.multi_scan_deadline",
        Setting::Duration(multi_scan_deadline),
    );
    out.put("server.scan_page_cache_bytes", count(scan_page_cache_bytes));
    out.put("server.load_deadline", Setting::Duration(load_deadline));
    out.put(
        "server.refresh_deadline",
        Setting::Duration(refresh_deadline),
    );
    out.put("server.drain_deadline", Setting::Duration(drain_deadline));
    let LogConfig {
        fields,
        static_fields,
    } = log;
    out.put("server.log.fields", absent(fields.is_empty()));
    out.put("server.log.static_fields", absent(static_fields.is_empty()));
}

/// The `[storage]`, `[storage.s3]`, and `[storage.hedge]` defaults, and
/// those of `[[storage.rewrite]]` and `[storage.local]` as the parser
/// builds them for a file that sets both.
fn storage_defaults(out: &mut Defaults, storage: StorageConfig, with_required: StorageConfig) {
    let StorageConfig {
        s3,
        rewrite,
        hedge,
        local,
        max_retries,
        connect_timeout,
        catalog_timeout,
    } = storage;
    out.put("storage.max_retries", count(max_retries));
    out.put(
        "storage.connect_timeout",
        Setting::Duration(connect_timeout),
    );
    out.put(
        "storage.catalog_timeout",
        Setting::Duration(catalog_timeout),
    );
    out.put("storage.s3", absent(s3.is_none()));
    out.put("storage.rewrite", absent(rewrite.is_empty()));
    out.put("storage.local", absent(local.is_none()));
    let HedgeConfig {
        enabled,
        percentile,
        window,
        max_rate,
    } = hedge;
    out.put("storage.hedge.enabled", Setting::Flag(enabled));
    out.put("storage.hedge.percentile", Setting::Number(percentile));
    out.put("storage.hedge.window", count(window));
    out.put("storage.hedge.max_rate", Setting::Number(max_rate));
    let S3Config {
        endpoint,
        region,
        access_key_id,
        secret_access_key,
        allow_http,
        path_style,
    } = S3Config::default();
    out.put("storage.s3.endpoint", absent(endpoint.is_none()));
    out.put("storage.s3.region", absent(region.is_none()));
    out.put("storage.s3.access_key_id", absent(access_key_id.is_none()));
    out.put(
        "storage.s3.secret_access_key",
        absent(secret_access_key.is_none()),
    );
    out.put("storage.s3.allow_http", Setting::Flag(allow_http));
    out.put("storage.s3.path_style", Setting::Flag(path_style));

    let RewriteRule { from: _, to: _ } = with_required
        .rewrite
        .first()
        .expect("the fixture sets a rule");
    let LocalConfig {
        dir: _,
        index_budget_bytes,
        page_cache_disk_bytes: _,
    } = with_required.local.expect("the fixture sets storage.local");
    out.put(
        "storage.local.index_budget_bytes",
        absent(index_budget_bytes.is_none()),
    );
}

/// The `[[tables]]` defaults: the entry of a file that sets only the
/// required keys, and the `max_scan_rows` the parser applies to an ordered
/// table that leaves it out.
fn table_defaults(out: &mut Defaults, table: TableConfig, ordered: &TableConfig) {
    let TableConfig {
        name: _,
        source: _,
        r#ref: _,
        key_column: _,
        value_columns: _,
        order_column,
        max_scan_rows,
        strict,
        lookups,
        residency,
        resident_budget_bytes,
    } = table;
    out.put("tables.order_column", absent(order_column.is_none()));
    out.put("tables.strict", Setting::Flag(strict));
    out.put("tables.lookups", absent(lookups.is_none()));
    out.put("tables.residency", Setting::Text(residency.to_string()));
    out.put(
        "tables.resident_budget_bytes",
        absent(resident_budget_bytes.is_none()),
    );
    assert!(
        max_scan_rows.is_none(),
        "a table that leaves max_scan_rows out has none"
    );
    let rows = ordered
        .scan_rows()
        .expect("an ordered table has a scan limit");
    out.put("tables.max_scan_rows", count(rows));
}

/// The keys of every kind of `[tables.source]` that are optional. A key
/// that is not is required, and has no default to record.
fn source_defaults(out: &mut Defaults, source: SourceConfig) {
    match source {
        SourceConfig::Static { location: _ } => {}
        SourceConfig::Rest {
            uri: _,
            warehouse,
            namespace: _,
            table: _,
        } => {
            out.put("tables.source.rest.warehouse", absent(warehouse.is_none()));
        }
        SourceConfig::Glue {
            database: _,
            table: _,
            catalog_id,
            region,
            endpoint,
        } => {
            out.put(
                "tables.source.glue.catalog_id",
                absent(catalog_id.is_none()),
            );
            out.put("tables.source.glue.region", absent(region.is_none()));
            out.put("tables.source.glue.endpoint", absent(endpoint.is_none()));
        }
        SourceConfig::S3Tables {
            table_bucket_arn: _,
            namespace: _,
            table: _,
            region,
            endpoint,
        } => {
            out.put("tables.source.s3tables.region", absent(region.is_none()));
            out.put(
                "tables.source.s3tables.endpoint",
                absent(endpoint.is_none()),
            );
        }
    }
}

/// Every default the code builds, by dotted path. A key with no default is
/// left out.
fn code_defaults() -> BTreeMap<String, Setting> {
    let mut out = Defaults(BTreeMap::new());
    let Config {
        server,
        storage,
        mut tables,
    } = Config::parse(&with_source(SOURCES[0])).expect("a minimal configuration is valid");
    let required = Config::parse(OTHER_REQUIRED_KEYS).expect("the fixture is valid");
    server_defaults(&mut out, server);
    storage_defaults(&mut out, storage, required.storage);
    let ordered = required.tables.first().expect("the fixture lists a table");
    table_defaults(&mut out, tables.remove(0), ordered);
    for source in SOURCES {
        let Config { mut tables, .. } =
            Config::parse(&with_source(source)).expect("each source fixture is valid");
        source_defaults(&mut out, tables.remove(0).source);
    }
    out.0
}

/// Whether `exemption` still holds of `row` and the code.
fn exemption_holds(
    Exemption {
        path, cell, kind, ..
    }: &Exemption,
    row: &Row,
    code: &BTreeMap<String, Setting>,
    rows: &BTreeMap<String, Row>,
) -> bool {
    row.default == *cell
        && match kind {
            Exempt::Required => !code.contains_key(*path),
            Exempt::Unset => code.get(*path) == Some(&Setting::Unset),
            Exempt::Nested => rows.values().any(|other| other.section == *path),
        }
}

/// The failure of `row`, if its default does not match the code.
fn check_row(
    row: &Row,
    code: &BTreeMap<String, Setting>,
    rows: &BTreeMap<String, Row>,
) -> Option<Failure> {
    let path = row.path.clone();
    let exemption = EXEMPT.iter().find(|exempt| exempt.path == row.path);
    match (stated_value(&row.default), exemption) {
        (None, Some(exemption)) if exemption_holds(exemption, row, code, rows) => None,
        (_, Some(&Exemption { kind, reason, .. })) => Some(Failure::ExemptionWrong {
            path,
            kind,
            reason,
            cell: row.default.clone(),
        }),
        (None, None) => Some(Failure::NotExempt {
            path,
            cell: row.default.clone(),
        }),
        (Some(text), None) => {
            let Some(actual) = code.get(&row.path) else {
                return Some(Failure::NoCodeDefault { path });
            };
            match documented(&row.type_cell, text) {
                None => Some(Failure::UnreadableValue {
                    path,
                    text: text.to_string(),
                }),
                Some(stated) if stated == *actual => None,
                Some(stated) => Some(Failure::Mismatch {
                    path,
                    documented: stated,
                    code: actual.clone(),
                }),
            }
        }
    }
}

/// Every way `reference` and the code disagree about a default.
fn failures(reference: &str) -> Vec<Failure> {
    let mut found = Vec::new();
    let mut rows = BTreeMap::new();
    for row in documented_rows(reference, &mut found) {
        if let Some(first) = rows.insert(row.path.clone(), row) {
            found.push(Failure::DuplicateRow { path: first.path });
        }
    }
    for (_, section) in SECTIONS {
        if rows.values().all(|row| row.section != section) {
            found.push(Failure::MissingSection { section });
        }
    }
    let code = code_defaults();
    found.extend(
        code.keys()
            .filter(|path| !rows.contains_key(*path))
            .map(|path| Failure::NoRow { path: path.clone() }),
    );
    found.extend(rows.values().filter_map(|row| check_row(row, &code, &rows)));
    found.extend(
        EXEMPT
            .iter()
            .filter(|exempt| !rows.contains_key(exempt.path))
            .map(|exempt| Failure::StaleExemption {
                path: exempt.path.to_string(),
            }),
    );
    found
}

#[test]
fn test_failures_finds_none_in_the_reference() {
    let found = failures(REFERENCE);
    assert!(
        found.is_empty(),
        "the reference and the code disagree:\n{found:#?}"
    );
}

/// A row as the reference would write it under `section`.
fn row(section: &str, key: &str, type_cell: &str, default: &str) -> Row {
    Row {
        section: section.to_string(),
        path: format!("{section}.{key}"),
        type_cell: type_cell.to_string(),
        default: default.to_string(),
    }
}

#[test]
fn test_check_row_reports_a_changed_default() {
    let changed = Duration::from_secs(11);
    let stated = row("server", "poll_interval", "duration", "`\"11s\"`");
    assert_eq!(
        check_row(&stated, &code_defaults(), &BTreeMap::new()),
        Some(Failure::Mismatch {
            path: "server.poll_interval".to_string(),
            documented: Setting::Duration(changed),
            code: Setting::Duration(ServerConfig::default().poll_interval),
        })
    );
}

#[test]
fn test_check_row_reports_a_value_for_a_key_with_no_default() {
    let stated = row("storage", "extra", "count", "`3`");
    assert_eq!(
        check_row(&stated, &code_defaults(), &BTreeMap::new()),
        Some(Failure::NoCodeDefault {
            path: "storage.extra".to_string()
        })
    );
}

#[test]
fn test_check_row_reports_words_that_are_not_exempt() {
    let words = row("server", "listen", "address", "the loopback address");
    assert_eq!(
        check_row(&words, &code_defaults(), &BTreeMap::new()),
        Some(Failure::NotExempt {
            path: "server.listen".to_string(),
            cell: "the loopback address".to_string(),
        })
    );
}

#[test]
fn test_check_row_reports_an_exempt_row_whose_words_changed() {
    let changed = row("tables", "order_column", "string", "`\"ts\"`");
    assert!(matches!(
        check_row(&changed, &code_defaults(), &BTreeMap::new()),
        Some(Failure::ExemptionWrong {
            kind: Exempt::Unset,
            ..
        })
    ));
    let reworded = row("tables", "order_column", "string", "unset");
    assert!(matches!(
        check_row(&reworded, &code_defaults(), &BTreeMap::new()),
        Some(Failure::ExemptionWrong {
            kind: Exempt::Unset,
            ..
        })
    ));
}

#[test]
fn test_failures_reports_a_default_the_reference_leaves_out() {
    let found = failures("");
    assert!(found.contains(&Failure::NoRow {
        path: "server.listen".to_string()
    }));
    assert!(found.contains(&Failure::MissingSection { section: "server" }));
    assert!(found.contains(&Failure::StaleExemption {
        path: "tables.ref".to_string()
    }));
}

#[test]
fn test_failures_reports_a_row_listed_twice() {
    let twice = format!("{REFERENCE}\n{REFERENCE}");
    assert!(failures(&twice).contains(&Failure::DuplicateRow {
        path: "server.listen".to_string()
    }));
}

#[test]
fn test_documented_rows_reports_a_settings_table_under_an_unlisted_heading() {
    let text = "### `[elsewhere]`\n\n| Key | Type | Default | Meaning |\n|---|---|---|---|\n\
                | `a` | count | `1` | Made up. |\n";
    let mut found = Vec::new();
    assert!(documented_rows(text, &mut found).is_empty());
    assert_eq!(
        found,
        [Failure::UnlistedSection {
            heading: "[elsewhere]".to_string()
        }]
    );
}

#[test]
fn test_documented_reads_each_value_format() {
    let ninety = Some(Setting::Duration(Duration::from_secs(90)));
    assert_eq!(documented("duration", "\"1m30s\""), ninety);
    assert_eq!(documented("duration", "\"1m 30s\""), ninety);
    assert_eq!(documented("duration", "\"90s\""), ninety);
    assert_eq!(documented("duration", "90"), None);
    assert_eq!(
        documented("bytes", "268435456"),
        documented("bytes", "268_435_456")
    );
    assert_eq!(documented("count", "65,600"), Some(Setting::Count(65_600)));
    assert_eq!(documented("boolean", "true"), Some(Setting::Flag(true)));
    assert_eq!(documented("number", "0.95"), Some(Setting::Number(0.95)));
    assert_eq!(
        documented("string", "\"full\""),
        Some(Setting::Text("full".to_string()))
    );
    assert_eq!(
        documented("address", "\"[::]:7433\""),
        Some(Setting::Address("[::]:7433".parse().unwrap()))
    );
    assert_eq!(documented("table", "1"), None);
}

#[test]
fn test_source_keys_reads_required_and_optional_keys() {
    let cell = "`uri`, required: the catalog's base URI; `warehouse`, optional; \
                `namespace`, required, its levels (`\"db.schema\"`); `a` and `b`, required";
    assert_eq!(
        source_keys(cell),
        [
            ("uri".to_string(), REQUIRED_WORD),
            ("warehouse".to_string(), OPTIONAL_WORD),
            ("namespace".to_string(), REQUIRED_WORD),
            ("a".to_string(), REQUIRED_WORD),
            ("b".to_string(), REQUIRED_WORD),
        ]
    );
}
