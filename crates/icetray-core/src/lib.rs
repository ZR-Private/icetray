//! Everything from bytes to lookups for serving get-by-key over Apache
//! Iceberg tables: keys, storage, catalog, manifests, the resident index,
//! the page cache, generations, the doctor, and configuration. The binary
//! and the testkit build on this crate; it depends on neither.

pub mod approx;
mod aws_catalog;
mod blocking;
mod bucket;
mod cache;
pub mod catalog;
pub mod check;
pub mod config;
pub mod contract;
pub mod deletes;
pub mod descriptors;
mod error;
pub mod generation;
mod index;
pub mod key;
mod level;
mod local;
mod manifest;
pub mod metric;
pub mod open;
mod parquet_io;
mod shard;
mod snapshot;
mod storage;
mod table;
pub mod time;

pub use aws_catalog::{AwsCatalogError, AwsSource};
pub use bucket::bucket;
pub use cache::{CacheError, PageCache};
pub use catalog::{TableFault, TableSource};
pub use check::{
    FileReport, Finding, FindingCode, LookupCost, Penalty, TableReport, TableSummary,
    WriterProperty, diagnose, judge,
};
pub use config::{
    Config, ConfigError, ConfigViolation, DEFAULT_RESIDENCY, HedgeConfig, LocalConfig, LogConfig,
    Residency, RewriteFault, RewriteRule, S3Config, ServerConfig, SourceConfig, SourceSetting,
    StandardField, StorageConfig, TableConfig,
};
pub use deletes::{BlobFraming, DeleteFault, DeleteSummary};
pub use error::{Budget, Error, ErrorKind, RefusalReason, Result, UnsupportedDelete};
pub use generation::{Generation, LoadedFile, Lookup, MultiLookup, Value};
pub use index::{
    ChunkPage, DecodedPart, FileFault, FileIndex, HeaderField, PageHeaderFault, PageRef,
    ResidentBytes, WireType,
};
pub use key::{Key, KeyBytes, KeyType};
pub use level::{FALL_PER_MILLE, Level, PAGE_CACHE_SAMPLE_INTERVAL, PER_MILLE, RECOVER_PER_MILLE};
pub use local::{DataFileIdentity, DiskChunks, DiskLevel, LocalDir, LocalFault};
pub use manifest::{Bucketing, DataFileEntry, TableLayout};
pub use open::{Applied, Opened, Refused, Tables, open};
pub use snapshot::{SequenceNumber, SnapshotId};
pub use storage::{IoStats, LocationError, LocationFault, Storage};
pub use table::{CallsInFlight, FindingLog, Table, TableHandles, TableState};
