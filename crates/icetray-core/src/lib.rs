//! Everything from bytes to lookups for serving get-by-key over Apache
//! Iceberg tables: keys, storage, catalog, manifests, the resident index,
//! the page cache, generations, the doctor, and configuration. The binary
//! and the testkit build on this crate; it depends on neither.

pub mod approx;
mod blocking;
mod bucket;
mod cache;
pub mod catalog;
pub mod check;
pub mod config;
pub mod contract;
mod error;
pub mod generation;
mod index;
pub mod key;
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

pub use bucket::bucket;
pub use cache::{CacheError, PageCache};
pub use catalog::{TableFault, TableSource};
pub use check::{
    FileReport, Finding, FindingCode, Penalty, Serviceability, TableReport, TableSummary, Tier,
    WriterProperty, admit, diagnose,
};
pub use config::{
    Config, ConfigError, ConfigViolation, DEFAULT_RESIDENCY, HedgeConfig, LocalConfig, LogConfig,
    Residency, RewriteFault, RewriteRule, S3Config, ServerConfig, SourceConfig, StandardField,
    StorageConfig, TableConfig,
};
pub use error::{Error, ErrorKind, RefusalReason, Result};
pub use generation::{Generation, LoadedFile, Lookup, MultiLookup, Value};
pub use index::{
    ChunkPage, DecodedPart, FileFault, FileIndex, HeaderField, PageHeaderFault, PageRef,
    ResidentBytes, WireType,
};
pub use key::{Key, KeyBytes, KeyType};
pub use local::{DataFileIdentity, DiskChunks, DiskLevel, LocalDir, LocalFault};
pub use manifest::{Bucketing, DataFileEntry, TableLayout};
pub use open::{Applied, Opened, Refused, Tables, open};
pub use snapshot::{SequenceNumber, SnapshotId};
pub use storage::{IoStats, Storage};
pub use table::{CallsInFlight, Table, TableHandles, TableState};
