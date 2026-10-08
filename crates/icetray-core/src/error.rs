//! The one error type of the crate, the refusal reasons that travel with it
//! into logs and metric labels, and `Recurs`, the one way to ask any failure
//! whether it is fixed for the snapshot or can succeed on another try (RFD
//! 005).

use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use crate::aws_catalog::AwsCatalogError;
use crate::cache::CacheError;
use crate::catalog::TableFault;
use crate::check::FindingCode;
use crate::config::ConfigError;
use crate::index::FileFault;
use crate::key::{Key, KeyType};
use crate::level::Level;
use crate::local::LocalFault;
use crate::storage::LocationError;

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
    /// A location the read needed names no store this process reads (RFD
    /// 003): one a table's metadata or catalog named, found as the table
    /// loads. A static table's own location is checked before, as
    /// `ConfigViolation::TableLocation`.
    #[error("{}", location_detail(table.as_deref(), source))]
    Location {
        /// The table whose load read the location: `None` as storage
        /// raises it, since storage reads for no table in particular, and
        /// set by the table's load (`Error::for_table`).
        table: Option<String>,
        /// The location as rewritten, and why no store serves it.
        source: LocationError,
    },
    /// The page cache could not produce a page.
    #[error("page cache: {0}")]
    Cache(#[from] CacheError),
    /// The table cannot be served correctly, or under `strict` not at the
    /// level it declares.
    #[error("table {table} refused: {}", refusal_detail(.reason))]
    Refused {
        /// The table's configured name.
        table: String,
        /// Why it is not served (RFD 007, RFD 020).
        reason: RefusalReason,
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
    /// A file a load reads is malformed for serving: a data file, or a
    /// delete file (a position delete file or a Puffin file) the snapshot
    /// lists; the fault set is closed.
    #[error("file {path}: {fault}")]
    File {
        /// The file's location: the data file, the position delete file, or
        /// the Puffin file.
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
    /// An AWS catalog refused or failed the call that asks for a table's
    /// metadata location (RFD 019), so the poll or load that made it
    /// failed.
    #[error("catalog call for table {table} failed: {source}")]
    Catalog {
        /// The table as its catalog names it: the source, since no table
        /// has been named while the source resolves.
        table: String,
        /// The SDK's error.
        source: AwsCatalogError,
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
    /// A blocking task of a load or of the doctor was canceled: the runtime
    /// shut down under it. The task verified keys (one data file's at load,
    /// an overlap group's for a key two files share, or one file's against
    /// its bucket in the doctor) or worked a load's deletes (decoded a
    /// position delete file or a Puffin file's vectors, or applied the
    /// decoded rows to the data files). A panic in that task is resumed on
    /// the caller instead, never turned into an error.
    #[error(
        "the blocking task verifying keys or working deletes of file {path} was canceled: {source}"
    )]
    VerifyCanceled {
        /// The file the task worked on: the data file; for an overlap
        /// group, its first file in file order; the position delete file or
        /// the Puffin file it decoded; for the applying of deletes, the
        /// load's first data file.
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

    /// This error raised while `table` loaded: an `Error::Location` that
    /// names no table names it; every other error is as it was.
    pub(crate) fn for_table(self, table: &str) -> Self {
        match self {
            Self::Location {
                table: None,
                source,
            } => Self::Location {
                table: Some(table.to_string()),
                source,
            },
            other @ (Self::Iceberg(_)
            | Self::Parquet(_)
            | Self::ObjectStore(_)
            | Self::ObjectStorePath(_)
            | Self::Json(_)
            | Self::Config(_)
            | Self::Location { .. }
            | Self::Cache(_)
            | Self::Refused { .. }
            | Self::KeyType { .. }
            | Self::Table { .. }
            | Self::File { .. }
            | Self::NotReady { .. }
            | Self::Local { .. }
            | Self::CatalogTimeout { .. }
            | Self::Catalog { .. }
            | Self::LoadDeadline { .. }
            | Self::RefreshDeadline { .. }
            | Self::VerifyCanceled { .. }
            | Self::LookupCanceled { .. }) => other,
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
            Self::Location { .. } => ErrorKind::Location,
            Self::Cache(_) => ErrorKind::Cache,
            Self::Refused { reason, .. } => ErrorKind::Refused(reason.clone()),
            Self::KeyType { .. } => ErrorKind::KeyType,
            Self::Table { .. } => ErrorKind::Table,
            Self::File { .. } => ErrorKind::File,
            Self::NotReady { .. } => ErrorKind::NotReady,
            Self::Local { .. } => ErrorKind::Local,
            Self::CatalogTimeout { .. } => ErrorKind::CatalogTimeout,
            Self::Catalog { .. } => ErrorKind::Catalog,
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
/// name for `Refused`, and the variant's `snake_case` name for the rest;
/// serde gives the same string.
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
    /// `Error::Location`.
    Location,
    /// `Error::Cache`.
    Cache,
    /// `Error::Refused`, with its reason.
    Refused(RefusalReason),
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
    /// `Error::Catalog`.
    Catalog,
    /// `Error::LoadDeadline`.
    LoadDeadline,
    /// `Error::RefreshDeadline`.
    RefreshDeadline,
    /// `Error::VerifyCanceled`.
    VerifyCanceled,
    /// `Error::LookupCanceled`.
    LookupCanceled,
}

/// A failure that says whether it is fixed for the snapshot it was met on
/// (RFD 005): the one question a refresh asks of a failure before it
/// decides to remember it. Every error type whose variants differ in the
/// answer implements it beside its variants, as one exhaustive match, so a
/// new variant does not compile until it is classified; a variant that
/// holds such a type asks the one it holds.
pub(crate) trait Recurs {
    /// Whether the same work over the same snapshot, under the same table
    /// entry, fails this way again.
    fn recurrence(&self) -> Recurrence;
}

/// Whether a failure can end otherwise when the same work is tried again
/// (RFD 005).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Recurrence {
    /// A pure function of the snapshot's metadata and data files and the
    /// table's configuration entry: a metadata file and the files it names
    /// are immutable, so building the snapshot again under the same entry
    /// fails the same way, and costs its reads and a log line each time.
    Deterministic,
    /// May succeed on another try: the failure depends on I/O, on another
    /// table, on the clock, on the process, or on anything else a retry
    /// could find changed, or it cannot tell whether it does. A failure
    /// classed here wrongly costs a poll's reads; one classed
    /// `Deterministic` wrongly leaves the table behind until its source
    /// moves.
    Transient,
}

impl Recurs for Error {
    fn recurrence(&self) -> Recurrence {
        match self {
            Self::Refused { reason, .. } => reason.recurrence(),
            Self::Table { fault, .. } => fault.recurrence(),
            Self::File { fault, .. } => fault.recurrence(),
            Self::Local { fault, .. } => fault.recurrence(),
            Self::Cache(error) => error.recurrence(),
            // A library's error does not say whether it met malformed
            // bytes or a read that failed under it.
            Self::Iceberg(_)
            | Self::Parquet(_)
            | Self::Json(_)
            // Whether a location names a store, and the path it names
            // there, follow the rewrite rules and stores of the process's
            // storage settings, not the table's entry.
            | Self::ObjectStorePath(_)
            | Self::Location { .. }
            // The configuration document and the process's settings, which
            // a reload or a restart reads again.
            | Self::Config(_)
            // A lookup's own key, never a build's.
            | Self::KeyType { .. }
            | Self::ObjectStore(_)
            | Self::NotReady { .. }
            | Self::CatalogTimeout { .. }
            | Self::Catalog { .. }
            | Self::LoadDeadline { .. }
            | Self::RefreshDeadline { .. }
            | Self::VerifyCanceled { .. }
            | Self::LookupCanceled { .. } => Recurrence::Transient,
        }
    }
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
            Self::Location => "location",
            Self::Cache => "cache",
            Self::KeyType => "key_type",
            Self::Table => "table",
            Self::File => "file",
            Self::NotReady => "not_ready",
            Self::Local => "local",
            Self::CatalogTimeout => "catalog_timeout",
            Self::Catalog => "catalog",
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

/// `Error::Location`'s message: the location and its fault, after the
/// table that read it when one is named.
fn location_detail(table: Option<&str>, source: &LocationError) -> String {
    match table {
        Some(table) => format!("table {table}: {source}"),
        None => source.to_string(),
    }
}

/// A refusal for a log line: the name, then the fields a reader needs to
/// act on: the file and row group, the types found and expected, the length
/// over the cap, the bytes needed and the budget, the levels declared and
/// achieved and the findings that cap it (RFD 007, RFD 017, RFD 020).
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
        RefusalReason::EqualityDeletes { path } => format!("{reason} (file {path})"),
        RefusalReason::DeleteKindUnsupported { path, kind } => {
            format!("{reason} (file {path}, {kind})")
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
        RefusalReason::OverBudget { of, needed, budget } => {
            format!("{reason} (needed {needed} bytes, {of} {budget} bytes)")
        }
        RefusalReason::BelowLevel {
            declared,
            achieved,
            findings,
        } => format!(
            "{reason} (lookups {declared}, the layout achieves {achieved}: {})",
            finding_list(findings)
        ),
        RefusalReason::KeyTypeUnsupported => reason.to_string(),
    }
}

/// The finding codes of a `BelowLevel` refusal as one comma-separated list.
fn finding_list(codes: &[FindingCode]) -> String {
    codes
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// The result type of every fallible operation in this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Why a table cannot be served correctly (RFD 007), or under `strict` not
/// at the level it declares (RFD 020). Display gives the `snake_case` name
/// that appears in logs, the doctor's output, and the `reason` metric
/// label; the variants that carry fields render their name alone, and
/// `Error::Refused`'s message adds the fields.
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
    /// and its first and last key (`Planned::build`); the doctor hashes
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
    /// An equality delete applies to a data file the snapshot holds: its
    /// data sequence number is above that of the snapshot's oldest data
    /// file, so it may delete rows of it (RFD 021). Equality deletes are
    /// not served; one at or below every data file's applies to nothing
    /// and is ignored.
    EqualityDeletes {
        /// The delete file's location.
        path: String,
    },
    /// A delete file icetray cannot read (RFD 021).
    DeleteKindUnsupported {
        /// The delete file's location.
        path: String,
        /// What it is.
        kind: UnsupportedDelete,
    },
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
        /// Which of the two budgets it is held to.
        of: Budget,
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
    /// The snapshot's layout puts a `strict` table below the level it
    /// declares (RFD 020). Unlike the other reasons the snapshot could be
    /// served correctly; its lookups would cost more than the table
    /// accepts.
    BelowLevel {
        /// The table's `lookups`.
        declared: Level,
        /// The level the layout achieves in the table's residency.
        achieved: Level,
        /// The distinct codes of the capping findings whose cap does not
        /// meet `declared`, in the doctor's order.
        findings: Vec<FindingCode>,
    },
}

/// Every rule but one reads only the snapshot's metadata and files and the
/// table's configuration (its columns, residency, resident budget, and
/// declared level) against fixed contract limits.
impl Recurs for RefusalReason {
    fn recurrence(&self) -> Recurrence {
        match self {
            Self::Unsorted { .. }
            | Self::NullKeys { .. }
            | Self::DuplicateKeys { .. }
            | Self::KeyOutsideBucket { .. }
            | Self::KeyTypeMismatch { .. }
            | Self::KeyTypeUnsupported
            | Self::ValueTypeUnsupported { .. }
            | Self::EqualityDeletes { .. }
            | Self::DeleteKindUnsupported { .. }
            | Self::KeyTooLong { .. }
            | Self::NoBloomFilter { .. }
            | Self::BelowLevel { .. }
            | Self::OverBudget {
                of: Budget::Resident,
                ..
            } => Recurrence::Deterministic,
            // The local directory's index budget is shared: whether a load
            // fits depends on the chunk files and reservations of every
            // other table, which change without this table's source moving.
            Self::OverBudget {
                of: Budget::Index, ..
            } => Recurrence::Transient,
        }
    }
}

/// A delete file icetray cannot read, as `RefusalReason::DeleteKindUnsupported`
/// names it (RFD 021). Display gives what it is, for a log line.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum UnsupportedDelete {
    /// A position delete file written as Avro.
    AvroPositions,
    /// A position delete file written as ORC.
    OrcPositions,
    /// A Puffin blob a manifest entry names as a delete that is not a
    /// deletion vector.
    PuffinBlob {
        /// The blob type the footer records.
        blob_type: String,
    },
    /// A Puffin file whose footer payload is compressed, which icetray
    /// does not decompress.
    CompressedFooter,
    /// A Puffin file whose footer sets flags the spec reserves, which no
    /// writer icetray knows of sets, so icetray cannot tell how to read it.
    ReservedFooterFlags {
        /// The reserved flags set, the four flag bytes read little-endian.
        flags: u32,
    },
}

impl fmt::Display for UnsupportedDelete {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AvroPositions => f.write_str("position deletes in avro"),
            Self::OrcPositions => f.write_str("position deletes in orc"),
            Self::PuffinBlob { blob_type } => write!(f, "a puffin blob of type {blob_type}"),
            Self::CompressedFooter => f.write_str("a compressed puffin footer"),
            Self::ReservedFooterFlags { flags } => {
                write!(f, "a puffin footer with reserved flags {flags:#010x} set")
            }
        }
    }
}

/// The budget an `OverBudget` refusal is held to (RFD 017). Display gives
/// the setting that sets it, for a log line.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Budget {
    /// A full-residency table's `resident_budget_bytes`, the table's own.
    Resident,
    /// The local directory's `index_budget_bytes`, shared by every
    /// disk-residency table.
    Index,
}

impl fmt::Display for Budget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Resident => "resident_budget_bytes",
            Self::Index => "index_budget_bytes",
        };
        f.write_str(name)
    }
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
            Self::EqualityDeletes { .. } => "equality_deletes",
            Self::DeleteKindUnsupported { .. } => "delete_kind_unsupported",
            Self::KeyTooLong { .. } => "key_too_long",
            Self::OverBudget { .. } => "over_budget",
            Self::NoBloomFilter { .. } => "no_bloom_filter",
            Self::BelowLevel { .. } => "below_level",
        };
        f.write_str(name)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::config::ConfigViolation;
    use crate::deletes::{BlobFraming, DeleteFault};
    use crate::generation::testing::TABLE;
    use crate::index::{ChunkPage, DecodedPart, HeaderField, PageHeaderFault};
    use crate::snapshot::SnapshotId;
    use crate::storage::LocationFault;

    /// A synthetic data file location for the reasons that name one.
    const FILE_PATH: &str = "mem://w/db/kv/data/f0.parquet";
    /// A synthetic table root for the version hint faults.
    const TABLE_ROOT: &str = "mem://w/db/kv";
    /// A synthetic chunk file path for the local directory faults.
    const CHUNK_PATH: &str = "/var/lib/icetray/index/f0";

    /// One reason of every variant, each beside its name; an over-budget
    /// one is held to the table's own budget.
    fn reason_cases() -> Vec<(RefusalReason, &'static str)> {
        vec![
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
            (
                RefusalReason::KeyTooLong {
                    path: FILE_PATH.to_string(),
                    bytes: 1,
                },
                "key_too_long",
            ),
            (
                RefusalReason::OverBudget {
                    of: Budget::Resident,
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
            (
                RefusalReason::BelowLevel {
                    declared: Level::Optimal,
                    achieved: Level::Balanced,
                    findings: vec![FindingCode::DictionaryEncodedValue],
                },
                "below_level",
            ),
        ]
        .into_iter()
        .chain(delete_reason_cases())
        .collect()
    }

    /// One delete refusal of every reason and kind, beside its label.
    fn delete_reason_cases() -> Vec<(RefusalReason, &'static str)> {
        vec![
            (
                RefusalReason::EqualityDeletes {
                    path: FILE_PATH.to_string(),
                },
                "equality_deletes",
            ),
            (
                RefusalReason::DeleteKindUnsupported {
                    path: FILE_PATH.to_string(),
                    kind: UnsupportedDelete::AvroPositions,
                },
                "delete_kind_unsupported",
            ),
            (
                RefusalReason::DeleteKindUnsupported {
                    path: FILE_PATH.to_string(),
                    kind: UnsupportedDelete::OrcPositions,
                },
                "delete_kind_unsupported",
            ),
            (
                RefusalReason::DeleteKindUnsupported {
                    path: FILE_PATH.to_string(),
                    kind: UnsupportedDelete::PuffinBlob {
                        blob_type: "apache-datasketches-theta-v1".to_string(),
                    },
                },
                "delete_kind_unsupported",
            ),
            (
                RefusalReason::DeleteKindUnsupported {
                    path: FILE_PATH.to_string(),
                    kind: UnsupportedDelete::CompressedFooter,
                },
                "delete_kind_unsupported",
            ),
            (
                RefusalReason::DeleteKindUnsupported {
                    path: FILE_PATH.to_string(),
                    kind: UnsupportedDelete::ReservedFooterFlags { flags: 2 },
                },
                "delete_kind_unsupported",
            ),
        ]
    }

    #[test]
    fn test_fmt_refusal_reason_snake_case_names() {
        for (reason, name) in reason_cases() {
            assert_eq!(reason.to_string(), name);
        }
    }

    /// A refusal for `reason` of the fixture table.
    fn refused(reason: RefusalReason) -> Error {
        Error::refused(TABLE, reason)
    }

    /// A table fault of the fixture table.
    fn table(fault: TableFault) -> Error {
        Error::table_fault(TABLE, fault)
    }

    /// A file fault of the synthetic data file.
    fn file(fault: FileFault) -> Error {
        Error::File {
            path: FILE_PATH.to_string(),
            fault,
        }
    }

    /// A local directory fault on a synthetic chunk file.
    fn local(fault: LocalFault) -> Error {
        Error::Local {
            path: PathBuf::from(CHUNK_PATH),
            fault,
        }
    }

    /// A refusal of every reason under the table's own budget and one under
    /// the shared one, beside their classification.
    fn refusal_recurrence_cases() -> Vec<(Error, Recurrence)> {
        use Recurrence::{Deterministic, Transient};
        let mut cases: Vec<(Error, Recurrence)> = reason_cases()
            .into_iter()
            .map(|(reason, _)| (refused(reason), Deterministic))
            .collect();
        cases.push((
            refused(RefusalReason::OverBudget {
                of: Budget::Index,
                needed: 1,
                budget: 0,
            }),
            Transient,
        ));
        cases
    }

    /// One table fault of every variant beside its classification.
    fn table_fault_recurrence_cases() -> Vec<(Error, Recurrence)> {
        use Recurrence::{Deterministic, Transient};
        vec![
            (
                table(TableFault::RefNotFound {
                    ref_name: "main".to_string(),
                }),
                Deterministic,
            ),
            (
                table(TableFault::SchemaMissing { schema_id: 1 }),
                Deterministic,
            ),
            (table(TableFault::SpecMissing { spec_id: 1 }), Deterministic),
            (
                table(TableFault::ColumnMissing {
                    column: "k".to_string(),
                }),
                Deterministic,
            ),
            (table(TableFault::ZeroBuckets { spec_id: 0 }), Deterministic),
            (
                table(TableFault::BucketValueMissing {
                    path: FILE_PATH.to_string(),
                    spec_id: 0,
                }),
                Deterministic,
            ),
            (
                table(TableFault::BucketValueOutOfRange {
                    path: FILE_PATH.to_string(),
                    spec_id: 0,
                    bucket: 2,
                    count: 2,
                }),
                Deterministic,
            ),
            (
                table(TableFault::SequenceNumberMissing {
                    path: FILE_PATH.to_string(),
                    manifest: FILE_PATH.to_string(),
                }),
                Deterministic,
            ),
            (
                table(TableFault::VectorUnplaced {
                    path: FILE_PATH.to_string(),
                }),
                Deterministic,
            ),
            (
                table(TableFault::MetadataGzipMalformed {
                    location: FILE_PATH.to_string(),
                }),
                Deterministic,
            ),
            (
                table(TableFault::SnapshotTimestampOutOfRange {
                    snapshot_id: SnapshotId(1),
                }),
                Deterministic,
            ),
            (
                table(TableFault::VersionHintEmpty {
                    root: TABLE_ROOT.to_string(),
                }),
                Transient,
            ),
            (
                table(TableFault::VersionHintMalformed {
                    root: TABLE_ROOT.to_string(),
                    hint: "1 2".to_string(),
                }),
                Transient,
            ),
            (table(TableFault::MetadataLocationUnknown), Transient),
            (
                table(TableFault::NotIceberg { table_type: None }),
                Transient,
            ),
        ]
    }

    /// One file fault of every variant, a decoder panic over every part,
    /// beside its classification.
    fn file_fault_recurrence_cases() -> Vec<(Error, Recurrence)> {
        use Recurrence::{Deterministic, Transient};
        let row_group = 0;
        vec![
            (
                file(FileFault::MissingColumn("k".to_string())),
                Deterministic,
            ),
            (file(FileFault::MissingPageIndex), Deterministic),
            (file(FileFault::NoRows), Deterministic),
            (file(FileFault::InconsistentRowCounts), Deterministic),
            (
                file(FileFault::InconsistentOffsetIndex { row_group }),
                Deterministic,
            ),
            (
                file(FileFault::UnplaceableBloomFilter { row_group }),
                Deterministic,
            ),
            (
                file(FileFault::ValuePhysicalType {
                    got: parquet::basic::Type::INT64,
                }),
                Deterministic,
            ),
            (file(FileFault::ZeroWidthValue), Deterministic),
            (
                file(FileFault::CorruptBloomFilter { row_group }),
                Deterministic,
            ),
            (
                file(FileFault::PageRowsMismatch { row_group, page: 0 }),
                Transient,
            ),
            (
                file(FileFault::MalformedPageHeader {
                    row_group,
                    page: ChunkPage::Data(0),
                    fault: PageHeaderFault::Truncated,
                }),
                Transient,
            ),
            (
                file(FileFault::PageHeaderOverBound {
                    row_group,
                    page: ChunkPage::Data(0),
                    field: HeaderField::UncompressedPageSize,
                    declared: 2,
                    allowed: 1,
                }),
                Transient,
            ),
            (file(FileFault::KeyNotUtf8 { row_group }), Transient),
            (
                file(FileFault::PageOutOfRange { row: 1, row_group }),
                Transient,
            ),
            (
                file(FileFault::DecoderPanicked {
                    part: DecodedPart::Footer,
                    message: "sample".to_string(),
                }),
                Deterministic,
            ),
            (
                file(FileFault::DecoderPanicked {
                    part: DecodedPart::BloomFilter { row_group },
                    message: "sample".to_string(),
                }),
                Deterministic,
            ),
            (
                file(FileFault::DecoderPanicked {
                    part: DecodedPart::Pages {
                        row_group,
                        page: None,
                    },
                    message: "sample".to_string(),
                }),
                Transient,
            ),
            (
                file(FileFault::DecoderPanicked {
                    part: DecodedPart::PositionDeletes,
                    message: "sample".to_string(),
                }),
                Deterministic,
            ),
        ]
        .into_iter()
        .chain(delete_fault_recurrence_cases())
        .collect()
    }

    /// One delete fault of every variant beside its classification.
    fn delete_fault_recurrence_cases() -> Vec<(Error, Recurrence)> {
        use Recurrence::{Deterministic, Transient};
        let delete = |fault| file(FileFault::Delete(fault));
        let data_file = || FILE_PATH.to_string();
        vec![
            (delete(DeleteFault::FooterFraming), Transient),
            (
                delete(DeleteFault::BlobFraming(BlobFraming::Length)),
                Transient,
            ),
            (
                delete(DeleteFault::BlobFraming(BlobFraming::Magic)),
                Transient,
            ),
            (
                delete(DeleteFault::BlobFraming(BlobFraming::Checksum)),
                Transient,
            ),
            (
                delete(DeleteFault::VectorUnlisted {
                    offset: 4,
                    length: 1,
                }),
                Deterministic,
            ),
            (
                delete(DeleteFault::BlobOutOfRange {
                    offset: 0,
                    length: 1,
                    blobs_end: 1,
                }),
                Deterministic,
            ),
            (
                delete(DeleteFault::PuffinSizeMismatch {
                    recorded: 1,
                    other: 2,
                }),
                Deterministic,
            ),
            (
                delete(DeleteFault::BlobCompressed {
                    codec: iceberg::puffin::CompressionCodec::zstd_default(),
                }),
                Deterministic,
            ),
            (
                delete(DeleteFault::ReferencedFileMismatch {
                    referenced: data_file(),
                    found: None,
                }),
                Deterministic,
            ),
            (
                delete(DeleteFault::CardinalityProperty {
                    recorded: 1,
                    found: None,
                }),
                Deterministic,
            ),
            (
                delete(DeleteFault::RecordsDecoded {
                    recorded: 1,
                    decoded: 2,
                }),
                Deterministic,
            ),
            (delete(DeleteFault::VectorMalformed), Deterministic),
            (
                delete(DeleteFault::PositionPastEnd {
                    data_file: data_file(),
                    position: 1,
                    records: 1,
                }),
                Deterministic,
            ),
            (
                delete(DeleteFault::NegativePosition {
                    data_file: data_file(),
                    position: -1,
                }),
                Deterministic,
            ),
            (
                delete(DeleteFault::TwoVectors {
                    data_file: data_file(),
                    other: data_file(),
                }),
                Deterministic,
            ),
            (delete(DeleteFault::PositionSchema), Deterministic),
            (delete(DeleteFault::PositionNull), Deterministic),
            (
                delete(DeleteFault::RecordCountMismatch {
                    manifest: 1,
                    footer: 2,
                }),
                Deterministic,
            ),
        ]
    }

    /// One local directory fault of every variant beside its
    /// classification.
    fn local_fault_recurrence_cases() -> Vec<(Error, Recurrence)> {
        use Recurrence::{Deterministic, Transient};
        vec![
            (local(LocalFault::HeaderTooLarge { len: 1 }), Deterministic),
            (
                local(LocalFault::ChunkOverlap { start: 0, end: 1 }),
                Deterministic,
            ),
            (
                local(LocalFault::ChunkLengthMismatch {
                    start: 0,
                    end: 2,
                    len: 1,
                }),
                Transient,
            ),
            (
                local(LocalFault::RangeNotResident { start: 0, end: 1 }),
                Transient,
            ),
            (
                local(LocalFault::Io(std::io::Error::other("sample"))),
                Transient,
            ),
            (local(LocalFault::Join(canceled_task())), Transient),
        ]
    }

    /// One cache failure of every variant, a fetch failure of either class
    /// among them, beside its classification.
    fn cache_recurrence_cases() -> Vec<(Error, Recurrence)> {
        use Recurrence::{Deterministic, Transient};
        vec![
            (
                Error::Cache(CacheError::Fetch(Arc::new(Error::NotReady {
                    table: TABLE.to_string(),
                }))),
                Transient,
            ),
            (
                Error::Cache(CacheError::Fetch(Arc::new(refused(
                    RefusalReason::KeyTypeUnsupported,
                )))),
                Deterministic,
            ),
            (
                Error::Cache(CacheError::FetchDeadline {
                    path: FILE_PATH.to_string(),
                    offset: 0,
                    after: Duration::ZERO,
                }),
                Transient,
            ),
            (
                Error::Cache(CacheError::Backend(foyer::Error::io_error(
                    std::io::Error::other("sample"),
                ))),
                Transient,
            ),
            (
                Error::Cache(CacheError::DescriptorLimit {
                    limit: 1,
                    needed: 2,
                    source: std::io::Error::other("sample"),
                }),
                Transient,
            ),
        ]
    }

    /// One failure of every variant of every type that answers `Recurs`,
    /// each beside its classification: a refusal of every reason and of
    /// both budgets, every table, file, local, and cache failure, and every
    /// other variant of `Error`, all transient.
    fn recurrence_cases() -> Vec<(Error, Recurrence)> {
        let mut cases = refusal_recurrence_cases();
        cases.extend(table_fault_recurrence_cases());
        cases.extend(file_fault_recurrence_cases());
        cases.extend(local_fault_recurrence_cases());
        cases.extend(cache_recurrence_cases());
        let classified_above = |error: &Error| match error {
            Error::Refused { .. }
            | Error::Table { .. }
            | Error::File { .. }
            | Error::Local { .. }
            | Error::Cache(_) => true,
            Error::Iceberg(_)
            | Error::Parquet(_)
            | Error::ObjectStore(_)
            | Error::ObjectStorePath(_)
            | Error::Json(_)
            | Error::Config(_)
            | Error::Location { .. }
            | Error::KeyType { .. }
            | Error::NotReady { .. }
            | Error::CatalogTimeout { .. }
            | Error::Catalog { .. }
            | Error::LoadDeadline { .. }
            | Error::RefreshDeadline { .. }
            | Error::VerifyCanceled { .. }
            | Error::LookupCanceled { .. } => false,
        };
        cases.extend(
            wrapped_cases()
                .into_iter()
                .chain(raised_cases())
                .map(|(error, _)| error)
                .filter(|error| !classified_above(error))
                .map(|error| (error, Recurrence::Transient)),
        );
        cases
    }

    #[test]
    fn test_recurrence_classifies_every_variant() {
        for (error, expected) in recurrence_cases() {
            assert_eq!(error.recurrence(), expected, "{error:?}");
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
                Error::Location {
                    table: Some(TABLE.to_string()),
                    source: LocationError {
                        location: FILE_PATH.to_string(),
                        fault: LocationFault::NoStore,
                    },
                },
                "location",
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
                    reason: RefusalReason::KeyTypeUnsupported,
                },
                "key_type_unsupported",
            ),
            (
                Error::Refused {
                    table: TABLE.to_string(),
                    reason: RefusalReason::BelowLevel {
                        declared: Level::Optimal,
                        achieved: Level::Permissive,
                        findings: vec![FindingCode::OverlappingFiles],
                    },
                },
                "below_level",
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
                Error::Catalog {
                    table: TABLE.to_string(),
                    source: AwsCatalogError::Glue(Box::new(
                        aws_sdk_glue::error::SdkError::timeout_error("sample"),
                    )),
                },
                "catalog",
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
            (
                Error::LookupCanceled {
                    table: TABLE.to_string(),
                    source: canceled_task(),
                },
                "lookup_canceled",
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
