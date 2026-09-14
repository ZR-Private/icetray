//! Configuration (RFD 011): one schema read from TOML or JSON into one
//! structure and validated once, so no setting is accepted in one syntax
//! and refused in the other.

/// Why a configuration could not be read or acted on.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// The configuration file could not be read.
    #[error("cannot read configuration file: {0}")]
    Read(#[from] std::io::Error),
    /// The text did not parse as TOML.
    #[error("configuration is not valid toml: {0}")]
    Toml(#[from] toml::de::Error),
    /// The text did not parse as JSON.
    #[error("configuration is not valid json: {0}")]
    Json(#[from] serde_json::Error),
    /// The configuration parsed but breaks a rule.
    #[error("invalid configuration: {0}")]
    Invalid(#[from] ConfigViolation),
}

/// A rule a parsed configuration breaks. The set is closed; a rule not
/// listed here is not checked.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ConfigViolation {
    /// A table has an empty name.
    #[error("a table has an empty name")]
    EmptyTableName,
    /// Two tables share a name.
    #[error("table name {0} is configured more than once")]
    DuplicateTableName(String),
    /// One of the two static S3 keys is set without the other.
    #[error("static credentials need both access_key_id and secret_access_key")]
    IncompleteStaticKeys,
    /// A resident-bytes budget on a table whose residency has nothing to budget.
    #[error("resident_budget_bytes is set on a table whose residency does not take a budget")]
    BudgetWithoutResidency,
    /// Disk residency with no local directory to write chunk files to.
    #[error("disk residency needs a local directory under storage.local")]
    DiskResidencyWithoutLocalDir,
}
