//! Everything from bytes to lookups for serving get-by-key over Apache
//! Iceberg tables: keys, storage, catalog, manifests, the resident index,
//! the page cache, generations, the doctor, and configuration. The binary
//! and the testkit build on this crate; it depends on neither.

pub mod approx;
pub mod bucket;
pub mod cache;
pub mod check;
pub mod config;
pub mod contract;
pub mod error;
pub mod index;
pub mod key;
pub mod time;

pub use bucket::bucket;
pub use cache::CacheError;
pub use check::FindingCode;
pub use config::{
    Config, ConfigError, ConfigViolation, HedgeConfig, LocalConfig, LogConfig, Residency,
    RewriteRule, S3Config, ServerConfig, SourceConfig, StorageConfig, TableConfig,
};
pub use error::{Error, LookupResult, RefusalReason, Result, SnapshotId};
pub use index::FileFault;
pub use key::{Key, KeyType};
