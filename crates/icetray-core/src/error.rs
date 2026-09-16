//! The one error type of the crate, and the names that travel with it into
//! logs and metric labels: refusal reasons, snapshot ids, lookup results.

use std::fmt;
use std::path::PathBuf;

use crate::cache::CacheError;
use crate::catalog::TableFault;
use crate::check::FindingCode;
use crate::config::ConfigError;
use crate::index::FileFault;
use crate::key::{Key, KeyType};
use crate::local::LocalFault;

/// Every failure a caller of this crate can observe. A caller decides what
/// to do by matching the variant; the message is for a person reading a log.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A failure inside the Iceberg library: metadata, a manifest, or a catalog.
    #[error("iceberg: {0}")]
    Iceberg(#[from] iceberg::Error),
    /// A failure inside the Parquet library: a footer, a page index, or a decode.
    #[error("parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    /// An object storage request failed.
    #[error("object store: {0}")]
    ObjectStore(#[from] object_store::Error),
    /// A location is not a valid object-store path.
    #[error("object store path: {0}")]
    ObjectStorePath(#[from] object_store::path::Error),
    /// JSON that did not parse or could not be written.
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    /// Configuration that cannot be read or acted on.
    #[error("config: {0}")]
    Config(#[from] ConfigError),
    /// The page cache could not produce a page.
    #[error("page cache: {0}")]
    Cache(#[from] CacheError),
    /// The table cannot be served correctly.
    #[error("table {table} refused: {}", refusal_detail(.reason))]
    Refused {
        /// The table's configured name.
        table: String,
        /// Why it cannot be served (RFD 007).
        reason: RefusalReason,
    },
    /// The table serves at a cost and `strict` is on.
    #[error("table {table} serves at a cost and strict is on: {}", finding_list(.findings))]
    Inefficient {
        /// The table's configured name.
        table: String,
        /// The cost findings the doctor raised.
        findings: Vec<FindingCode>,
    },
    /// A lookup key whose type is not the table's key type.
    #[error("table {table} keys are {expected}, got {got}")]
    KeyType {
        /// The table's configured name.
        table: String,
        /// The key type the snapshot's schema declares.
        expected: KeyType,
        /// The key type the caller sent.
        got: KeyType,
    },
    /// The table's metadata makes it unusable before any data file is read;
    /// the fault set is closed.
    #[error("table {table}: {fault}")]
    Table {
        /// The table's configured name, or, for a fault raised while a
        /// `TableSource` is resolved and no table has been named yet, the
        /// source itself: the static location or the catalog identifier.
        table: String,
        /// What is wrong with its metadata.
        fault: TableFault,
    },
    /// A data file is malformed for serving; the fault set is closed.
    #[error("file {path}: {fault}")]
    File {
        /// The data file's location.
        path: String,
        /// What is wrong with it.
        fault: FileFault,
    },
    /// The table has no serving generation yet.
    #[error("table {table} has no serving generation")]
    NotReady {
        /// The table's configured name.
        table: String,
    },
    /// The local directory failed (RFD 017); the fault set is closed.
    #[error("local {}: {fault}", path.display())]
    Local {
        /// The file or directory the operation was on.
        path: PathBuf,
        /// What went wrong with it.
        fault: LocalFault,
    },
}

impl Error {
    /// `Error::Refused` for `table`.
    pub(crate) fn refused(table: &str, reason: RefusalReason) -> Self {
        Self::Refused {
            table: table.to_string(),
            reason,
        }
    }

    /// `Error::Table` for `table`.
    pub(crate) fn table_fault(table: &str, fault: TableFault) -> Self {
        Self::Table {
            table: table.to_string(),
            fault,
        }
    }
}

/// A refusal for a log line: the name, then the fields a reader needs to
/// act on: the file and row group, the types found and expected, the length
/// over the cap, the bytes needed and the budget (RFD 007, RFD 017).
fn refusal_detail(reason: &RefusalReason) -> String {
    match reason {
        RefusalReason::Unsorted { path, row_group }
        | RefusalReason::NullKeys { path, row_group }
        | RefusalReason::NoBloomFilter { path, row_group } => {
            format!("{reason} (file {path}, row group {row_group})")
        }
        RefusalReason::KeyTypeMismatch {
            path,
            expected,
            got,
        } => format!("{reason} (file {path}, expected {expected}, got {got})"),
        RefusalReason::ValueTypeUnsupported { got } => format!("{reason} (got {got})"),
        RefusalReason::KeyTooLong { path, bytes } => {
            format!("{reason} (file {path}, {bytes} bytes)")
        }
        RefusalReason::DuplicateKeys { path, key } => {
            format!("{reason} (file {path}, key {key})")
        }
        RefusalReason::OverBudget { needed, budget } => {
            format!("{reason} (needed {needed} bytes, budget {budget} bytes)")
        }
        RefusalReason::KeyTypeUnsupported
        | RefusalReason::DeleteFiles
        | RefusalReason::NoLocalDir => reason.to_string(),
    }
}

/// The finding codes of an `Inefficient` error as one comma-separated list.
fn finding_list(codes: &[FindingCode]) -> String {
    codes
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// The result type of every fallible operation in this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Why a table cannot be served correctly (RFD 007). Display gives the
/// `snake_case` name that appears in logs, the doctor's output, and the
/// `reason` metric label; the variants that carry fields render their name
/// alone, and `Error::Refused`'s message adds the fields.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum RefusalReason {
    /// A data file's key column is not sorted: a key of a row group is below
    /// the one before it, or a row group's first key is below the previous
    /// row group's last.
    Unsorted {
        /// The data file's location.
        path: String,
        /// The row group where the order breaks.
        row_group: usize,
    },
    /// A data file has a null key.
    NullKeys {
        /// The data file's location.
        path: String,
        /// The row group holding the null.
        row_group: usize,
    },
    /// One key appears in more than one row: twice within `path`, or in
    /// `path` and in an earlier file of the same bucket.
    DuplicateKeys {
        /// The data file the key appears in again.
        path: String,
        /// The repeated key.
        key: Key,
    },
    /// The key column's physical type is not the one its Iceberg type maps to.
    KeyTypeMismatch {
        /// The data file's location.
        path: String,
        /// The key type the snapshot's schema declares.
        expected: KeyType,
        /// The physical type the file stores the column as.
        got: parquet::basic::Type,
    },
    /// The key column's Iceberg type is not long, string, binary, fixed, or uuid.
    KeyTypeUnsupported,
    /// The value column's Iceberg type in the snapshot's schema is none of
    /// binary, fixed, or string, so its bytes have no defined encoding on the
    /// wire (RFD 002, RFD 010). Raised by `TableLayout::derive`; a data file
    /// whose physical type contradicts an admitted schema type is
    /// `FileFault::ValuePhysicalType`, a malformed file rather than a refusal.
    ValueTypeUnsupported {
        /// The Iceberg type's `Display`, held as text because
        /// `iceberg::spec::Type` does not implement `Hash`.
        got: String,
    },
    /// The snapshot carries delete files.
    DeleteFiles,
    /// A key is longer than `contract::MAX_KEY_BYTES`.
    KeyTooLong {
        /// The data file's location.
        path: String,
        /// The length of the file's longest key.
        bytes: usize,
    },
    /// A full-residency load would exceed its resident-bytes budget (RFD 017).
    OverBudget {
        /// The resident bytes the load would need.
        needed: u64,
        /// The budget it is held to.
        budget: u64,
    },
    /// Bloom residency on a file whose row group has no key bloom filter (RFD 017).
    NoBloomFilter {
        /// The data file's location.
        path: String,
        /// The row group without a filter.
        row_group: usize,
    },
    /// Disk residency with no local directory configured (RFD 017).
    NoLocalDir,
}

impl fmt::Display for RefusalReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Unsorted { .. } => "unsorted",
            Self::NullKeys { .. } => "null_keys",
            Self::DuplicateKeys { .. } => "duplicate_keys",
            Self::KeyTypeMismatch { .. } => "key_type_mismatch",
            Self::KeyTypeUnsupported => "key_type_unsupported",
            Self::ValueTypeUnsupported { .. } => "value_type_unsupported",
            Self::DeleteFiles => "delete_files",
            Self::KeyTooLong { .. } => "key_too_long",
            Self::OverBudget { .. } => "over_budget",
            Self::NoBloomFilter { .. } => "no_bloom_filter",
            Self::NoLocalDir => "no_local_dir",
        };
        f.write_str(name)
    }
}

/// A snapshot id. It is a newtype so that it is never compared with or
/// added to a bare integer, and it has no order: Iceberg ids are random, and
/// snapshots order by sequence number. It is exported as a metric label
/// rather than a gauge value, where an `f64` would lose its low digits
/// (RFD 015). Serde is transparent: the id is the integer on the wire and in
/// JSON.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct SnapshotId(pub i64);

impl fmt::Display for SnapshotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The three outcomes of a lookup as the `result` metric label sees them.
/// Display gives the `snake_case` label value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum LookupResult {
    /// The key was present and its value page was fetched.
    ColdHit,
    /// The key was present and its value page was already cached.
    WarmHit,
    /// The key was absent.
    Miss,
}

impl fmt::Display for LookupResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::ColdHit => "cold_hit",
            Self::WarmHit => "warm_hit",
            Self::Miss => "miss",
        };
        f.write_str(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An id wider than 32 bits, so the round trip exercises the whole `i64`.
    const SAMPLE_SNAPSHOT_ID: i64 = 5_123_456_789_012;

    /// A synthetic data file location for the reasons that name one.
    const FILE_PATH: &str = "mem://w/db/kv/data/f0.parquet";

    #[test]
    fn test_fmt_refusal_reason_snake_case_names() {
        let cases = [
            (
                RefusalReason::Unsorted {
                    path: FILE_PATH.to_string(),
                    row_group: 1,
                },
                "unsorted",
            ),
            (
                RefusalReason::NullKeys {
                    path: FILE_PATH.to_string(),
                    row_group: 0,
                },
                "null_keys",
            ),
            (
                RefusalReason::DuplicateKeys {
                    path: FILE_PATH.to_string(),
                    key: Key::Long(1),
                },
                "duplicate_keys",
            ),
            (
                RefusalReason::KeyTypeMismatch {
                    path: FILE_PATH.to_string(),
                    expected: KeyType::Long,
                    got: parquet::basic::Type::BYTE_ARRAY,
                },
                "key_type_mismatch",
            ),
            (RefusalReason::KeyTypeUnsupported, "key_type_unsupported"),
            (
                RefusalReason::ValueTypeUnsupported {
                    got: iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long)
                        .to_string(),
                },
                "value_type_unsupported",
            ),
            (RefusalReason::DeleteFiles, "delete_files"),
            (
                RefusalReason::KeyTooLong {
                    path: FILE_PATH.to_string(),
                    bytes: 1,
                },
                "key_too_long",
            ),
            (
                RefusalReason::OverBudget {
                    needed: 1,
                    budget: 0,
                },
                "over_budget",
            ),
            (
                RefusalReason::NoBloomFilter {
                    path: FILE_PATH.to_string(),
                    row_group: 0,
                },
                "no_bloom_filter",
            ),
            (RefusalReason::NoLocalDir, "no_local_dir"),
        ];
        for (reason, name) in cases {
            assert_eq!(reason.to_string(), name);
        }
    }

    #[test]
    fn test_fmt_lookup_result_snake_case_names() {
        let cases = [
            (LookupResult::ColdHit, "cold_hit"),
            (LookupResult::WarmHit, "warm_hit"),
            (LookupResult::Miss, "miss"),
        ];
        for (result, name) in cases {
            assert_eq!(result.to_string(), name);
        }
    }

    #[test]
    fn test_fmt_snapshot_id_is_the_integer() {
        assert_eq!(
            SnapshotId(SAMPLE_SNAPSHOT_ID).to_string(),
            SAMPLE_SNAPSHOT_ID.to_string()
        );
    }

    #[test]
    fn test_serialize_snapshot_id_transparent() {
        let json = serde_json::to_string(&SnapshotId(SAMPLE_SNAPSHOT_ID)).unwrap();
        assert_eq!(json, SAMPLE_SNAPSHOT_ID.to_string());
    }

    #[test]
    fn test_deserialize_snapshot_id_transparent() {
        let id: SnapshotId = serde_json::from_str(&SAMPLE_SNAPSHOT_ID.to_string()).unwrap();
        assert_eq!(id, SnapshotId(SAMPLE_SNAPSHOT_ID));
    }
}
