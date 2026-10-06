//! The JSON lines the one-shot subcommands print: one object per line, the
//! `kind` first and the `table` second, so a reader routes a line before it
//! parses the rest. A kind names what the line describes, not the command
//! that printed it. The `table` line of `doctor` and of `info` flattens
//! core's `TableSummary`, so a new measurement in core reaches both without
//! a change here.

use std::fmt;
use std::io::Write as _;

use base64::Engine as _;
use icetray_core::generation::{Lookup, Value};
use icetray_core::{ErrorKind, FileReport, Finding, Key, RefusalReason, SnapshotId, TableSummary};
use serde::Serialize;

use crate::error::CliError;
use crate::key_text;

/// The kind of a line, its first key: the thing the line describes.
/// Serialized as the lowercase name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    /// One table's summary, from `info` or `doctor`.
    Table,
    /// One finding of a `doctor` run.
    Finding,
    /// One data file of a `doctor --files` run.
    File,
    /// A table the doctor refused.
    Refusal,
    /// One lookup of a `get` run, as `icetray_lookups_total` counts it.
    Lookup,
}

/// A lookup's outcome as the `lookup` line carries it; the same three values
/// as the wire (RFD 010). Serialized as the lowercase name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Presence {
    /// The key is in the snapshot with a non-null value.
    Present,
    /// The key is in the snapshot and its value is null.
    Null,
    /// The key is not in the snapshot.
    Absent,
}

impl Presence {
    /// The outcome of `value`.
    pub fn of(value: &Value) -> Self {
        match value {
            Value::Present(_) => Self::Present,
            Value::Null => Self::Null,
            Value::Absent => Self::Absent,
        }
    }
}

/// A `table` line, which `info` and `doctor` both print: `kind`, then the
/// summary, whose first field is `table`.
#[derive(Debug, Serialize)]
pub struct SummaryLine<'a> {
    kind: Kind,
    #[serde(flatten)]
    summary: &'a TableSummary,
}

impl<'a> SummaryLine<'a> {
    /// The line for `summary`.
    pub fn new(summary: &'a TableSummary) -> Self {
        Self {
            kind: Kind::Table,
            summary,
        }
    }
}

/// One finding of a `doctor` run, with the finding's own fields flattened.
#[derive(Debug, Serialize)]
pub struct FindingLine<'a> {
    kind: Kind,
    table: &'a str,
    #[serde(flatten)]
    finding: &'a Finding,
}

impl<'a> FindingLine<'a> {
    /// The line for `finding` on `table`.
    pub fn new(table: &'a str, finding: &'a Finding) -> Self {
        Self {
            kind: Kind::Finding,
            table,
            finding,
        }
    }
}

/// One data file of a `doctor --files` run, with the report's fields flattened.
#[derive(Debug, Serialize)]
pub struct FileLine<'a> {
    kind: Kind,
    table: &'a str,
    #[serde(flatten)]
    file: &'a FileReport,
}

impl<'a> FileLine<'a> {
    /// The line for `file` on `table`.
    pub fn new(table: &'a str, file: &'a FileReport) -> Self {
        Self {
            kind: Kind::File,
            table,
            file,
        }
    }
}

/// What a `refusal` line's `reason` carries: the name of the rule the
/// table breaks, or `load_failed` when an error rather than a rule stopped
/// its load. Display and serde give the `snake_case` code.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RefusalCode {
    /// A refusal reason of RFD 007 or RFD 020, under its own name.
    Rule(RefusalReason),
    /// `load_failed`: the load failed with an error that names no rule.
    LoadFailed,
}

impl RefusalCode {
    /// The code a failure of `kind` refuses a table under, and `kind`
    /// itself when that code is `LoadFailed`, since the code alone does not
    /// say what failed.
    pub fn of(kind: ErrorKind) -> (Self, Option<ErrorKind>) {
        match kind {
            ErrorKind::Refused(reason) => (Self::Rule(reason), None),
            failed @ (ErrorKind::Iceberg
            | ErrorKind::Parquet
            | ErrorKind::ObjectStore
            | ErrorKind::ObjectStorePath
            | ErrorKind::Json
            | ErrorKind::Config
            | ErrorKind::Location
            | ErrorKind::Cache
            | ErrorKind::KeyType
            | ErrorKind::Table
            | ErrorKind::File
            | ErrorKind::NotReady
            | ErrorKind::Local
            | ErrorKind::CatalogTimeout
            | ErrorKind::Catalog
            | ErrorKind::LoadDeadline
            | ErrorKind::RefreshDeadline
            | ErrorKind::VerifyCanceled
            | ErrorKind::LookupCanceled) => (Self::LoadFailed, Some(failed)),
        }
    }
}

impl fmt::Display for RefusalCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rule(reason) => reason.fmt(f),
            Self::LoadFailed => f.write_str("load_failed"),
        }
    }
}

impl Serialize for RefusalCode {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

/// A table the doctor refused: the refusal code, the error's kind when the
/// code is `load_failed`, and the error's message for a person. Whichever
/// of `reason` and `error_kind` is the more specific is the `reason` label
/// `refresh_errors_total` carries for the same failure.
#[derive(Debug, Serialize)]
pub struct RefusalLine<'a> {
    kind: Kind,
    table: &'a str,
    reason: RefusalCode,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_kind: Option<ErrorKind>,
    message: String,
}

impl<'a> RefusalLine<'a> {
    /// The line for `error`, which refused `table`.
    pub fn new(table: &'a str, error: &icetray_core::Error) -> Self {
        let (reason, error_kind) = RefusalCode::of(error.kind());
        Self {
            kind: Kind::Refusal,
            table,
            reason,
            error_kind,
            message: error.to_string(),
        }
    }
}

/// One lookup of a `get` run, a `lookup` line: the key as the text `get`
/// takes for it (`key_text::text`), so a `key` passed back to `get` looks
/// up the same key, the outcome, the value as standard base64 with padding
/// and, when its bytes are UTF-8, as text, the snapshot answered from, the
/// GETs the lookup issued, and the time it took. The value fields are
/// absent for a null or absent key.
#[derive(Debug, Serialize)]
pub struct LookupLine<'a> {
    kind: Kind,
    table: &'a str,
    key: String,
    presence: Presence,
    #[serde(skip_serializing_if = "Option::is_none")]
    value_base64: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    value_utf8: Option<String>,
    value_bytes: usize,
    snapshot_id: SnapshotId,
    gets: u64,
    elapsed_micros: u64,
}

impl<'a> LookupLine<'a> {
    /// The line for `lookup` of `key` on `table`, which issued `gets` GETs
    /// and took `elapsed_micros`.
    pub fn new(table: &'a str, key: &Key, lookup: &Lookup, gets: u64, elapsed_micros: u64) -> Self {
        let bytes = match &lookup.value {
            Value::Present(bytes) => Some(bytes.as_ref()),
            Value::Null | Value::Absent => None,
        };
        Self {
            kind: Kind::Lookup,
            table,
            key: key_text::text(key),
            presence: Presence::of(&lookup.value),
            value_base64: bytes
                .map(|bytes| base64::engine::general_purpose::STANDARD.encode(bytes)),
            // intentional: a value that is not UTF-8 has no text form, and
            // the base64 field already carries its bytes.
            value_utf8: bytes.and_then(|bytes| std::str::from_utf8(bytes).ok().map(str::to_string)),
            value_bytes: bytes.map_or(0, <[u8]>::len),
            snapshot_id: lookup.snapshot_id,
            gets,
            elapsed_micros,
        }
    }
}

/// Writes one JSON object and a newline to stdout. A reader that closed the
/// pipe is `CliError::BrokenPipe`, which ends the run with exit 0 and
/// nothing more printed; any other write failure is `CliError::Io`.
pub fn emit<T: Serialize>(line: &T) -> Result<(), CliError> {
    let json = serde_json::to_string(line)?;
    let mut stdout = std::io::stdout().lock();
    match writeln!(stdout, "{json}") {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::BrokenPipe => Err(CliError::BrokenPipe),
        Err(error) => Err(CliError::Io(error)),
    }
}

/// The keys of a JSON object in the order the text carries them, duplicates
/// included: what a test reads when the order or the uniqueness of keys is
/// the claim, since `serde_json::Value` keeps neither.
#[cfg(test)]
pub(crate) struct KeyOrder(pub(crate) Vec<String>);

#[cfg(test)]
impl<'de> serde::Deserialize<'de> for KeyOrder {
    fn deserialize<D: serde::de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::{IgnoredAny, MapAccess, Visitor};

        struct KeysVisitor;

        impl<'de> Visitor<'de> for KeysVisitor {
            type Value = KeyOrder;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a JSON object")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<KeyOrder, A::Error> {
                let mut keys = Vec::new();
                while let Some(key) = map.next_key::<String>()? {
                    map.next_value::<IgnoredAny>()?;
                    keys.push(key);
                }
                Ok(KeyOrder(keys))
            }
        }

        deserializer.deserialize_map(KeysVisitor)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use std::collections::BTreeMap;

    use icetray_core::{FindingCode, KeyType, Level, Residency, ResidentBytes, TableFault};
    use serde_json::Value as Json;

    use super::*;

    /// The table every sample line names.
    const TABLE: &str = "kv";

    /// The keys every line leads with: `kind`, then `table`.
    const LEADING_KEYS: usize = 2;

    /// The snapshot every sample line names.
    const SNAPSHOT: i64 = 42;

    /// A value whose bytes are not UTF-8.
    const BINARY_VALUE: [u8; 3] = [0xff, 0xfe, 0x00];

    /// A value whose bytes are UTF-8.
    const TEXT_VALUE: &str = "hello";

    /// Standard base64 with padding of `TEXT_VALUE`.
    const TEXT_VALUE_BASE64: &str = "aGVsbG8=";

    /// The GETs the sample `lookup` line carries: one cold hit.
    const GETS: u64 = 1;

    /// The microseconds the sample `lookup` line carries.
    const ELAPSED_MICROS: u64 = 250;

    /// The first `LEADING_KEYS` keys of `line`'s JSON.
    fn leading_keys<T: Serialize>(line: &T) -> Vec<String> {
        let json = serde_json::to_string(line).unwrap();
        let KeyOrder(keys) = serde_json::from_str(&json).unwrap();
        keys.into_iter().take(LEADING_KEYS).collect()
    }

    fn summary() -> TableSummary {
        TableSummary {
            table: TABLE.to_string(),
            snapshot_id: SnapshotId(SNAPSHOT),
            level: Level::Optimal,
            lookups: Level::Optimal,
            residency: Residency::Full,
            file_count: 1,
            buckets: Some(1),
            buckets_with_files: 1,
            max_files_per_bucket: 1,
            records: 1,
            index_bytes: 1,
            predicted: ResidentBytes {
                full: 1,
                disk_memory: 1,
                disk_bytes: 1,
                bloom: None,
                bloom_bits_per_key: None,
                key_chunk_bytes: 1,
                filter_bytes: None,
            },
            level_by_residency: BTreeMap::from([
                (Residency::Full, Level::Optimal),
                (Residency::Disk, Level::Optimal),
                (Residency::Bloom, Level::Balanced),
            ]),
            capping: 0,
            advice: 0,
        }
    }

    fn finding() -> Finding {
        Finding::advice(FindingCode::LargeFile, None, "sample".to_string(), None)
    }

    fn file_report() -> FileReport {
        FileReport {
            path: "mem://f0.parquet".to_string(),
            bucket: 0,
            key_type: KeyType::Long,
            rows: 1,
            row_groups: 1,
            key_pages: 1,
            value_pages: 1,
            max_key_page_rows: 1,
            max_key_bytes: 1,
            avg_value_page_bytes: 1,
            avg_value_bytes: 1,
            pages_aligned: true,
            dictionary_columns: Vec::new(),
            bloom_bytes: None,
            bloom_bits_per_key: None,
            key_chunk_bytes: 1,
            distinct_page_mins_ratio: 1.0,
            bounds_present: true,
            resident_bytes: 1,
        }
    }

    fn lookup(value: Value) -> Lookup {
        Lookup {
            value,
            snapshot_id: SnapshotId(SNAPSHOT),
        }
    }

    #[test]
    fn test_serialize_every_line_puts_kind_then_table_first() {
        let summary = summary();
        let finding = finding();
        let file = file_report();
        let error = icetray_core::Error::Table {
            table: TABLE.to_string(),
            fault: TableFault::MetadataLocationUnknown,
        };
        let present = lookup(Value::Present(Bytes::from_static(TEXT_VALUE.as_bytes())));
        let key = Key::Long(1);
        assert_eq!(leading_keys(&SummaryLine::new(&summary)), ["kind", "table"]);
        assert_eq!(
            leading_keys(&FindingLine::new(TABLE, &finding)),
            ["kind", "table"]
        );
        assert_eq!(
            leading_keys(&FileLine::new(TABLE, &file)),
            ["kind", "table"]
        );
        assert_eq!(
            leading_keys(&RefusalLine::new(TABLE, &error)),
            ["kind", "table"]
        );
        assert_eq!(
            leading_keys(&LookupLine::new(
                TABLE,
                &key,
                &present,
                GETS,
                ELAPSED_MICROS
            )),
            ["kind", "table"]
        );
    }

    #[test]
    fn test_serialize_summary_line_is_a_table_line_carrying_the_summary() {
        let summary = summary();
        let line = serde_json::to_value(SummaryLine::new(&summary)).unwrap();
        assert_eq!(line["kind"], Json::String("table".to_string()));
        assert_eq!(line["table"], Json::String(TABLE.to_string()));
        assert_eq!(line["snapshot_id"], Json::String(SNAPSHOT.to_string()));
        assert_eq!(line["level"], Json::String("optimal".to_string()));
        assert_eq!(line["lookups"], Json::String("optimal".to_string()));
        assert_eq!(
            line["level_by_residency"]["bloom"],
            Json::String("balanced".to_string())
        );
        assert_eq!(line["capping"], Json::from(0));
    }

    #[test]
    fn test_serialize_finding_and_file_lines_flatten_their_report() {
        let finding = finding();
        let line = serde_json::to_value(FindingLine::new(TABLE, &finding)).unwrap();
        assert_eq!(line["kind"], Json::String("finding".to_string()));
        assert_eq!(
            line["code"],
            Json::String(FindingCode::LargeFile.to_string())
        );
        assert_eq!(line["caps"], Json::Null, "advice caps nothing");
        let file = file_report();
        let line = serde_json::to_value(FileLine::new(TABLE, &file)).unwrap();
        assert_eq!(line["kind"], Json::String("file".to_string()));
        assert_eq!(line["path"], Json::String(file.path.clone()));
    }

    #[test]
    fn test_serialize_refusal_line_rule_is_its_reason_with_no_error_kind() {
        let refused = icetray_core::Error::Refused {
            table: TABLE.to_string(),
            reason: RefusalReason::DeleteFiles,
        };
        let below_level = icetray_core::Error::Refused {
            table: TABLE.to_string(),
            reason: RefusalReason::BelowLevel {
                declared: Level::Optimal,
                achieved: Level::Permissive,
                findings: vec![FindingCode::NoBucketPartition],
            },
        };
        for (error, code) in [(refused, "delete_files"), (below_level, "below_level")] {
            let line = serde_json::to_value(RefusalLine::new(TABLE, &error)).unwrap();
            assert_eq!(line["kind"], Json::String("refusal".to_string()));
            assert_eq!(line["reason"], Json::String(code.to_string()));
            assert!(line.get("error_kind").is_none(), "{line}");
            assert!(line["message"].is_string());
        }
    }

    #[test]
    fn test_serialize_refusal_line_error_is_load_failed_with_its_kind() {
        let table = icetray_core::Error::Table {
            table: TABLE.to_string(),
            fault: TableFault::MetadataLocationUnknown,
        };
        let deadline = icetray_core::Error::LoadDeadline {
            table: TABLE.to_string(),
            after: std::time::Duration::ZERO,
        };
        for (error, kind) in [(table, "table"), (deadline, "load_deadline")] {
            let line = serde_json::to_value(RefusalLine::new(TABLE, &error)).unwrap();
            assert_eq!(line["reason"], Json::String("load_failed".to_string()));
            assert_eq!(line["error_kind"], Json::String(kind.to_string()));
            assert!(line["message"].is_string());
        }
    }

    #[test]
    fn test_serialize_lookup_line_present_value_as_base64_and_utf8() {
        let key = Key::Long(1);
        let present = lookup(Value::Present(Bytes::from_static(TEXT_VALUE.as_bytes())));
        let line =
            serde_json::to_value(LookupLine::new(TABLE, &key, &present, GETS, ELAPSED_MICROS))
                .unwrap();
        assert_eq!(line["kind"], Json::String("lookup".to_string()));
        assert_eq!(line["key"], Json::String("1".to_string()));
        assert_eq!(line["presence"], Json::String("present".to_string()));
        assert_eq!(
            line["value_base64"],
            Json::String(TEXT_VALUE_BASE64.to_string())
        );
        assert_eq!(line["value_utf8"], Json::String(TEXT_VALUE.to_string()));
        assert_eq!(line["value_bytes"], Json::from(TEXT_VALUE.len()));
        assert_eq!(line["snapshot_id"], Json::String(SNAPSHOT.to_string()));
        assert_eq!(line["gets"], Json::from(GETS));
        assert_eq!(line["elapsed_micros"], Json::from(ELAPSED_MICROS));
    }

    /// A string key with a space, both quote characters, a backslash, and
    /// non-ASCII text.
    const AWKWARD_STRING: &str = "a \"quoted\" 'key' with a \\ in caf\u{e9} \u{1f9ca}";

    #[test]
    fn test_serialize_lookup_line_key_parses_back_to_the_key_for_every_type() {
        let keys = [
            Key::Long(-1),
            Key::Str(AWKWARD_STRING.to_string()),
            Key::Bytes(BINARY_VALUE.to_vec()),
            Key::Uuid(*b"0123456789abcdef"),
        ];
        let absent = lookup(Value::Absent);
        for key in keys {
            let line =
                serde_json::to_string(&LookupLine::new(TABLE, &key, &absent, 0, ELAPSED_MICROS))
                    .unwrap();
            let line: Json = serde_json::from_str(&line).unwrap();
            let Json::String(printed) = &line["key"] else {
                panic!("{key}: the key is a JSON string in {line}");
            };
            assert_eq!(
                key_text::parse(printed, key.key_type()),
                Ok(key.clone()),
                "{line}"
            );
        }
    }

    #[test]
    fn test_serialize_lookup_line_string_key_is_the_string_itself() {
        let key = Key::Str(AWKWARD_STRING.to_string());
        let line = serde_json::to_value(LookupLine::new(
            TABLE,
            &key,
            &lookup(Value::Absent),
            0,
            ELAPSED_MICROS,
        ))
        .unwrap();
        assert_eq!(line["key"], Json::String(AWKWARD_STRING.to_string()));
    }

    #[test]
    fn test_serialize_lookup_line_non_utf8_value_has_no_utf8_field() {
        let key = Key::Long(1);
        let present = lookup(Value::Present(Bytes::from_static(&BINARY_VALUE)));
        let line =
            serde_json::to_value(LookupLine::new(TABLE, &key, &present, GETS, ELAPSED_MICROS))
                .unwrap();
        assert!(line["value_base64"].is_string());
        assert!(line.get("value_utf8").is_none());
        assert_eq!(line["value_bytes"], Json::from(BINARY_VALUE.len()));
    }

    #[test]
    fn test_serialize_lookup_line_null_and_absent_carry_no_value() {
        let key = Key::Long(1);
        for (value, name) in [(Value::Null, "null"), (Value::Absent, "absent")] {
            let line = serde_json::to_value(LookupLine::new(
                TABLE,
                &key,
                &lookup(value),
                0,
                ELAPSED_MICROS,
            ))
            .unwrap();
            assert_eq!(line["presence"], Json::String(name.to_string()));
            assert!(line.get("value_base64").is_none());
            assert!(line.get("value_utf8").is_none());
            assert_eq!(line["value_bytes"], Json::from(0));
        }
    }

    #[test]
    fn test_of_maps_every_value() {
        assert_eq!(
            Presence::of(&Value::Present(Bytes::new())),
            Presence::Present
        );
        assert_eq!(Presence::of(&Value::Null), Presence::Null);
        assert_eq!(Presence::of(&Value::Absent), Presence::Absent);
    }
}
