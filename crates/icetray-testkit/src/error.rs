//! The one error type of the testkit: every library failure a fixture or
//! an assertion can meet, one variant per source, plus the two faults this
//! crate raises itself, a fixture specification the writer refuses and a
//! helper called in a state it cannot measure. A test matches the variant;
//! the message is for a person reading the failure.

use std::env::VarError;

use icetray_core::KeyType;
use icetray_core::config::Residency;

use crate::golden::WAREHOUSE_VAR;

/// The environment variable that names the REST catalog the container-gated
/// test writes through (`dev/test-int.sh` exports it).
pub const REST_URI_VAR: &str = "ICETRAY_REST_URI";

/// Every failure the testkit can observe.
#[derive(Debug, thiserror::Error)]
pub enum TestkitError {
    /// A failure inside `icetray-core`: a refusal, a fault, or a library
    /// error it wrapped. The refusal tests match on it.
    #[error("core: {0}")]
    Core(#[from] icetray_core::Error),
    /// A failure inside the Iceberg library: the writer, a commit, or a
    /// catalog.
    #[error("iceberg: {0}")]
    Iceberg(#[from] iceberg::Error),
    /// A failure inside the Parquet writer.
    #[error("parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    /// A failure building an arrow array or batch.
    #[error("arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    /// A filesystem or socket failure.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// Metadata that could not be written as JSON.
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    /// A failure building or using an `object_store` client.
    #[error("object store: {0}")]
    ObjectStore(#[from] object_store::Error),
    /// The in-process S3 server could not open its root directory.
    /// `s3s_fs::Error` implements neither `Display` nor `std::error::Error`,
    /// so it is held whole and rendered through `Debug`.
    #[error("s3 server: {0:?}")]
    S3Server(s3s_fs::Error),
    /// A fixture specification the writer refuses.
    #[error("fixture specification: {0}")]
    Spec(#[from] SpecFault),
    /// A helper called in a state it cannot measure.
    #[error("precondition: {0}")]
    Precondition(#[from] Precondition),
}

impl From<s3s_fs::Error> for TestkitError {
    fn from(error: s3s_fs::Error) -> Self {
        Self::S3Server(error)
    }
}

/// What makes a fixture specification unwritable; closed.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SpecFault {
    /// `key_pad` lengthens string and binary keys; a long or uuid key has a
    /// fixed width and cannot take one.
    #[error("key_pad applies to string and binary keys, not {key_type}")]
    PadOnFixedWidthKey {
        /// The fixed-width key type the specification named.
        key_type: KeyType,
    },
    /// The fixture has committed a snapshot by hand, so its catalog record
    /// is behind its root and a commit through the catalog would fork the
    /// table; only `commit_by_hand` may advance it from then on.
    #[error("the table has committed by hand and its catalog record is behind")]
    CatalogBehind,
}

/// A helper asked to measure what it cannot, from the state it was given;
/// closed. A gated test forced without its environment fails on one of
/// these rather than passing without doing its work (RFD 014).
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Precondition {
    /// The cost oracle names value pages from resident key pages, which
    /// only a Full-tier generation holds.
    #[error("the cost oracle needs a full-tier generation, got {residency}")]
    OracleNotFull {
        /// The tier the generation was loaded in.
        residency: Residency,
    },
    /// A cold-cost assertion was given a cache that has already served.
    #[error("the page cache is not cold: {hits} hits and {fetches} fetches")]
    CacheNotCold {
        /// Lookups the cache had already served without a fetch.
        hits: u64,
        /// Fetches the cache had already run.
        fetches: u64,
    },
    /// The golden warehouse is not named in the environment.
    #[error("{WAREHOUSE_VAR} is not set")]
    GoldenUnset,
    /// The REST catalog is not named in the environment.
    #[error("{REST_URI_VAR} is not set")]
    RestUriUnset,
}

/// The result type of every fallible operation in this crate.
pub type Result<T> = std::result::Result<T, TestkitError>;

/// The environment variable `name` a gated test needs, or `missing` when it
/// is absent or not text: the one place a gate's variable is read, so a
/// test forced without its environment fails on the typed precondition.
pub fn required_var(name: &str, missing: Precondition) -> Result<String> {
    match std::env::var(name) {
        Ok(value) => Ok(value),
        Err(VarError::NotPresent | VarError::NotUnicode(_)) => Err(missing.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A variable no environment sets, so the unset branch is always taken.
    const NEVER_SET: &str = "ICETRAY_TESTKIT_NEVER_SET";

    #[test]
    fn test_required_var_unset_is_the_typed_precondition() {
        assert!(matches!(
            required_var(NEVER_SET, Precondition::GoldenUnset),
            Err(TestkitError::Precondition(Precondition::GoldenUnset))
        ));
        assert!(matches!(
            required_var(NEVER_SET, Precondition::RestUriUnset),
            Err(TestkitError::Precondition(Precondition::RestUriUnset))
        ));
    }
}
