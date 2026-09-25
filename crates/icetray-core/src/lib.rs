//! Everything from bytes to lookups for serving get-by-key over Apache
//! Iceberg tables: keys, storage, catalog, manifests, the resident index,
//! the page cache, generations, the doctor, and configuration. The binary
//! and the testkit build on this crate; it depends on neither.

pub mod approx;
pub mod bucket;
pub mod cache;
pub mod catalog;
pub mod check;
pub mod config;
pub mod contract;
pub mod error;
pub mod generation;
pub mod index;
pub mod key;
pub mod local;
pub mod manifest;
pub mod metric;
pub mod open;
pub mod parquet_io;
pub mod snapshot;
pub mod storage;
pub mod table;
pub mod time;

pub use bucket::bucket;
pub use cache::{CacheError, PageCache, PageOrigin};
pub use catalog::{Resolved, TableFault, TableSource};
pub use check::{
    FileReport, Finding, FindingCode, Penalty, Serviceability, TableReport, TableSummary, Tier,
    WriterProperty, diagnose,
};
pub use config::{
    Config, ConfigError, ConfigViolation, DEFAULT_RESIDENCY, HedgeConfig, LocalConfig, LogConfig,
    Residency, RewriteRule, S3Config, ServerConfig, SourceConfig, StandardField, StorageConfig,
    TableConfig,
};
pub use error::{Error, ErrorKind, RefusalReason, Result};
pub use generation::{Generation, LoadedFile, Lookup, MultiLookup, Value};
pub use index::{
    ChunkPage, DecodedPart, FileFault, FileIndex, HeaderField, Inspected, PageHeaderFault, PageRef,
    ResidentBytes, RowRef, WireType,
};
pub use key::{Key, KeyType};
pub use local::{DataFileIdentity, DiskChunks, DiskTier, LocalDir, LocalFault};
pub use manifest::{Bucketing, DataFileEntry, TableLayout, list_data_files};
pub use metric::LookupResult;
pub use open::{Opened, open};
pub use parquet_io::{FileHandle, Segments, SegmentsFault};
pub use snapshot::{SequenceNumber, SnapshotId};
pub use storage::{HedgeWindow, IoStats, Located, Storage};
pub use table::{Table, TableState};
