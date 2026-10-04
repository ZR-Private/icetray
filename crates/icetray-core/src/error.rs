//! The one error type of the crate, and the refusal reasons that travel with
//! it into logs and metric labels.

use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

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
    #[error(transparent)]
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
    /// The table is inefficient and `strict` is on.
    #[error("table {table} is inefficient and strict is on: {}", finding_list(.findings))]
    Inefficient {
        /// The table's configured name.
        table: String,
        /// The inefficient findings the doctor raised.
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
    /// A catalog call did not answer within `storage.catalog_timeout`, so
    /// the poll or load that made it failed (RFD 013).
    #[error("catalog did not answer for table {table} within {after:?}")]
    CatalogTimeout {
        /// The table's catalog identifier: the source, since no table has
        /// been named while the source resolves.
        table: String,
        /// The timeout that passed.
        after: Duration,
    },
    /// A build from no serving generation, a first load or a retry after
    /// one failed, did not finish within `server.load_deadline`, so it was
    /// dropped and failed (RFD 013).
    #[error("table {table} first load did not finish within {after:?} (load_deadline)")]
    LoadDeadline {
        /// The table's configured name.
        table: String,
        /// The deadline that passed.
        after: Duration,
    },
    /// A refresh over a serving generation did not finish within
    /// `server.refresh_deadline`, so it was dropped and failed (RFD 013).
    #[error("table {table} refresh did not finish within {after:?} (refresh_deadline)")]
    RefreshDeadline {
        /// The table's configured name.
        table: String,
        /// The deadline that passed.
        after: Duration,
    },
    /// The blocking task that verified keys was canceled: the runtime shut
    /// down under it. The task checked one data file's keys at load, an
    /// overlap group's keys for a key two files share, or one file's keys
    /// against its bucket in the doctor. A panic in that task is resumed on
    /// the caller instead, never turned into an error.
    #[error("key verification of file {path} was canceled: {source}")]
    VerifyCanceled {
        /// The data file's location; for an overlap group, its first file
        /// in file order.
        path: String,
        /// The canceled task's outcome.
        source: tokio::task::JoinError,
    },
    /// A task of a `multi_get` was canceled before it answered its keys:
    /// the runtime it was spawned on shut down under it. A panic in that
    /// task is resumed on the caller instead, never turned into an error.
    #[error("a lookup task of table {table} was canceled: {source}")]
    LookupCanceled {
        /// The table's configured name.
        table: String,
        /// The canceled task's outcome.
        source: tokio::task::JoinError,
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

    /// `Error::VerifyCanceled` for the file at `path`.
    pub(crate) fn verify_canceled(path: &str, source: tokio::task::JoinError) -> Self {
        Self::VerifyCanceled {
            path: path.to_string(),
            source,
        }
    }

    /// `Error::Table` for `table`.
    pub(crate) fn table_fault(table: &str, fault: TableFault) -> Self {
        Self::Table {
            table: table.to_string(),
            fault,
        }
    }

    /// Which variant this is, with the reason for a refusal: the `reason`
    /// label of `refresh_errors_total`.
    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::Iceberg(_) => ErrorKind::Iceberg,
            Self::Parquet(_) => ErrorKind::Parquet,
            Self::ObjectStore(_) => ErrorKind::ObjectStore,
            Self::ObjectStorePath(_) => ErrorKind::ObjectStorePath,
            Self::Json(_) => ErrorKind::Json,
            Self::Config(_) => ErrorKind::Config,
            Self::Cache(_) => ErrorKind::Cache,
            Self::Refused { reason, .. } => ErrorKind::Refused(reason.clone()),
            Self::Inefficient { .. } => ErrorKind::Inefficient,
            Self::KeyType { .. } => ErrorKind::KeyType,
            Self::Table { .. } => ErrorKind::Table,
            Self::File { .. } => ErrorKind::File,
            Self::NotReady { .. } => ErrorKind::NotReady,
            Self::Local { .. } => ErrorKind::Local,
            Self::CatalogTimeout { .. } => ErrorKind::CatalogTimeout,
            Self::LoadDeadline { .. } => ErrorKind::LoadDeadline,
            Self::RefreshDeadline { .. } => ErrorKind::RefreshDeadline,
            Self::VerifyCanceled { .. } => ErrorKind::VerifyCanceled,
            Self::LookupCanceled { .. } => ErrorKind::LookupCanceled,
        }
    }
}

/// Which variant an `Error` is, for the `reason` label of
/// `refresh_errors_total`; a refusal carries its `RefusalReason`, so the
/// label names the refusal. Display gives the label value: the refusal's own
/// name for `Refused`, `inefficient` for the strict gate, and the variant's
/// `snake_case` name for the rest; serde gives the same string.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ErrorKind {
    /// `Error::Iceberg`.
    Iceberg,
    /// `Error::Parquet`.
    Parquet,
    /// `Error::ObjectStore`.
    ObjectStore,
    /// `Error::ObjectStorePath`.
    ObjectStorePath,
    /// `Error::Json`.
    Json,
    /// `Error::Config`.
    Config,
    /// `Error::Cache`.
    Cache,
    /// `Error::Refused`, with its reason.
    Refused(RefusalReason),
    /// `Error::Inefficient`.
    Inefficient,
    /// `Error::KeyType`.
    KeyType,
    /// `Error::Table`.
    Table,
    /// `Error::File`.
    File,
    /// `Error::NotReady`.
    NotReady,
    /// `Error::Local`.
    Local,
    /// `Error::CatalogTimeout`.
    CatalogTimeout,
    /// `Error::LoadDeadline`.
    LoadDeadline,
    /// `Error::RefreshDeadline`.
    RefreshDeadline,
    /// `Error::VerifyCanceled`.
    VerifyCanceled,
    /// `Error::LookupCanceled`.
    LookupCanceled,
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Refused(reason) => return reason.fmt(f),
            Self::Iceberg => "iceberg",
            Self::Parquet => "parquet",
            Self::ObjectStore => "object_store",
            Self::ObjectStorePath => "object_store_path",
            Self::Json => "json",
            Self::Config => "config",
            Self::Cache => "cache",
            Self::Inefficient => "inefficient",
            Self::KeyType => "key_type",
            Self::Table => "table",
            Self::File => "file",
            Self::NotReady => "not_ready",
            Self::Local => "local",
            Self::CatalogTimeout => "catalog_timeout",
            Self::LoadDeadline => "load_deadline",
            Self::RefreshDeadline => "refresh_deadline",
            Self::VerifyCanceled => "verify_canceled",
            Self::LookupCanceled => "lookup_canceled",
        };
        f.write_str(name)
    }
}

impl serde::Serialize for ErrorKind {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.collect_str(self)
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
        RefusalReason::KeyOutsideBucket {
            path,
            key,
            recorded,
            computed,
        } => format!(
            "{reason} (file {path}, key {key} hashes to bucket {computed}, the manifest records bucket {recorded})"
        ),
        RefusalReason::OverBudget { needed, budget } => {
            format!("{reason} (needed {needed} bytes, budget {budget} bytes)")
        }
        RefusalReason::KeyTypeUnsupported | RefusalReason::DeleteFiles => reason.to_string(),
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
    /// A key of a bucketed data file hashes to another bucket than the one
    /// its manifest entry records (RFD 007). Routing sends a key to no
    /// bucketed file outside the bucket it hashes to, so such a key would
    /// answer `Absent` though present. The load hashes each file's fences
    /// and its first and last key (`Generation::load`); the doctor hashes
    /// every key (`Generation::verify_buckets`).
    KeyOutsideBucket {
        /// The data file's location.
        path: String,
        /// The first key found outside the bucket.
        key: Key,
        /// The bucket the manifest records for the file.
        recorded: u32,
        /// The bucket the key hashes to under the file's own bucketing.
        computed: u32,
    },
    /// The key column's physical type is not the one its Iceberg type maps
    /// to, or is a `FIXED_LEN_BYTE_ARRAY` of a width that cannot hold it: a
    /// uuid's other than `UUID_BYTES`, a binary key's of 0, which holds one
    /// distinct value.
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
    /// A load would exceed its budget (RFD 017): a full-residency table's
    /// resident-bytes budget, or the local directory's chunk-file budget
    /// shared by every disk-residency table.
    OverBudget {
        /// The bytes the load would need: the table's resident bytes for
        /// full residency; for disk, the bytes `index/` would hold, other
        /// loads' reservations included.
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
}

impl fmt::Display for RefusalReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Unsorted { .. } => "unsorted",
            Self::NullKeys { .. } => "null_keys",
            Self::DuplicateKeys { .. } => "duplicate_keys",
            Self::KeyOutsideBucket { .. } => "key_outside_bucket",
            Self::KeyTypeMismatch { .. } => "key_type_mismatch",
            Self::KeyTypeUnsupported => "key_type_unsupported",
            Self::ValueTypeUnsupported { .. } => "value_type_unsupported",
            Self::DeleteFiles => "delete_files",
            Self::KeyTooLong { .. } => "key_too_long",
            Self::OverBudget { .. } => "over_budget",
            Self::NoBloomFilter { .. } => "no_bloom_filter",
        };
        f.write_str(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ConfigViolation;
    use crate::generation::testing::TABLE;

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
                RefusalReason::KeyOutsideBucket {
                    path: FILE_PATH.to_string(),
                    key: Key::Long(1),
                    recorded: 1,
                    computed: 0,
                },
                "key_outside_bucket",
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
        ];
        for (reason, name) in cases {
            assert_eq!(reason.to_string(), name);
        }
    }

    /// One error of every variant that wraps a library's or the local
    /// directory's failure, each beside its `reason` label.
    fn wrapped_cases() -> Vec<(Error, &'static str)> {
        vec![
            (
                Error::Iceberg(iceberg::Error::new(
                    iceberg::ErrorKind::Unexpected,
                    "sample",
                )),
                "iceberg",
            ),
            (
                Error::Parquet(parquet::errors::ParquetError::General("sample".to_string())),
                "parquet",
            ),
            (
                Error::ObjectStore(object_store::Error::Generic {
                    store: "sample",
                    source: Box::new(std::io::Error::other("sample")),
                }),
                "object_store",
            ),
            (
                Error::ObjectStorePath(object_store::path::Error::EmptySegment {
                    path: "a//b".to_string(),
                }),
                "object_store_path",
            ),
            (
                Error::Json(serde_json::from_str::<u8>("sample").unwrap_err()),
                "json",
            ),
            (
                Error::Config(ConfigError::Invalid(ConfigViolation::EmptyTableName {
                    index: 0,
                })),
                "config",
            ),
            (
                Error::Cache(CacheError::Fetch(std::sync::Arc::new(Error::NotReady {
                    table: TABLE.to_string(),
                }))),
                "cache",
            ),
            (
                Error::Local {
                    path: PathBuf::from("/var/lib/icetray"),
                    fault: LocalFault::Io(std::io::Error::other("sample")),
                },
                "local",
            ),
        ]
    }

    /// One error of every variant the crate raises itself, each beside its
    /// `reason` label.
    fn raised_cases() -> Vec<(Error, &'static str)> {
        vec![
            (
                Error::Refused {
                    table: TABLE.to_string(),
                    reason: RefusalReason::DeleteFiles,
                },
                "delete_files",
            ),
            (
                Error::Inefficient {
                    table: TABLE.to_string(),
                    findings: vec![FindingCode::DictionaryEncodedValue],
                },
                "inefficient",
            ),
            (
                Error::KeyType {
                    table: TABLE.to_string(),
                    expected: KeyType::Long,
                    got: KeyType::String,
                },
                "key_type",
            ),
            (
                Error::Table {
                    table: TABLE.to_string(),
                    fault: TableFault::MetadataLocationUnknown,
                },
                "table",
            ),
            (
                Error::File {
                    path: FILE_PATH.to_string(),
                    fault: FileFault::NoRows,
                },
                "file",
            ),
            (
                Error::NotReady {
                    table: TABLE.to_string(),
                },
                "not_ready",
            ),
            (
                Error::CatalogTimeout {
                    table: TABLE.to_string(),
                    after: Duration::ZERO,
                },
                "catalog_timeout",
            ),
            (
                Error::LoadDeadline {
                    table: TABLE.to_string(),
                    after: Duration::ZERO,
                },
                "load_deadline",
            ),
            (
                Error::RefreshDeadline {
                    table: TABLE.to_string(),
                    after: Duration::ZERO,
                },
                "refresh_deadline",
            ),
            (
                Error::VerifyCanceled {
                    path: FILE_PATH.to_string(),
                    source: canceled_task(),
                },
                "verify_canceled",
            ),
        ]
    }

    /// The outcome of a task aborted before it ran: the `JoinError` a
    /// canceled task leaves.
    fn canceled_task() -> tokio::task::JoinError {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(async {
                let task = tokio::spawn(std::future::pending::<()>());
                task.abort();
                task.await.unwrap_err()
            })
    }

    #[test]
    fn test_kind_display_label_values() {
        for (error, name) in wrapped_cases().into_iter().chain(raised_cases()) {
            assert_eq!(error.kind().to_string(), name);
        }
        let duplicate = Error::Refused {
            table: TABLE.to_string(),
            reason: RefusalReason::DuplicateKeys {
                path: FILE_PATH.to_string(),
                key: Key::Long(1),
            },
        };
        assert_eq!(duplicate.kind().to_string(), "duplicate_keys");
        assert_eq!(
            duplicate.kind(),
            ErrorKind::Refused(RefusalReason::DuplicateKeys {
                path: FILE_PATH.to_string(),
                key: Key::Long(1),
            })
        );
    }
}
