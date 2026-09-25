//! The golden tables `dev/golden-tables.py` writes (RFD 014): where they
//! are, what each holds, and the key and value expressions of the recipe as
//! Rust, so a G-tier test checks an answer without a stored oracle. The
//! tables live outside the tree, named by `ICETRAY_GOLDEN_WAREHOUSE`; a run
//! without it fails on `Precondition::GoldenUnset` rather than passing.

use std::fmt::{self, Write as _};

use icetray_core::config::{Config, ServerConfig, SourceConfig, StorageConfig, TableConfig};
use icetray_core::key::HEX_DIGITS_PER_BYTE;
use icetray_core::{Key, KeyType, Residency};
use sha2::digest::OutputSizeUser;
use sha2::digest::typenum::Unsigned as _;
use sha2::{Digest as _, Sha256};

use crate::error::{Precondition, Result, required_var};
use crate::keys::{key_at, key_at_padded};
use crate::layout::{KEY_COLUMN, VALUE_COLUMN};

/// The environment variable naming the warehouse the recipe wrote into: an
/// `s3a://` or `file://` location whose `db/` holds the tables.
pub const WAREHOUSE_VAR: &str = "ICETRAY_GOLDEN_WAREHOUSE";

/// The namespace the recipe creates every table in (`CATALOG.db`).
const NAMESPACE: &str = "db";

/// The branch every golden table is read on.
const BRANCH: &str = iceberg::spec::MAIN_BRANCH;

/// Rows of `long_2m` and `str_2m` (the recipe's `fill(..., 2_000_000, ...)`).
const ROWS_2M: u64 = 2_000_000;

/// Rows of `default_layout_1m`.
const ROWS_1M: u64 = 1_000_000;

/// Rows of `bin16_500k` and `unpartitioned_500k`.
const ROWS_500K: u64 = 500_000;

/// Rows of `long_wide_250k` and `overlap_250k`.
const ROWS_250K: u64 = 250_000;

/// Rows of `long_v3_50k`.
const ROWS_50K: u64 = 50_000;

/// Buckets of the contract tables, `long_v3_50k`, and `default_layout_1m`
/// (`create(..., 4)`).
const FOUR_BUCKETS: u32 = 4;

/// Buckets of the wide and overlap tables (`create(..., 2, extra=WIDE)`).
const TWO_BUCKETS: u32 = 2;

/// `create(..., 0)`: no bucket partition.
const UNPARTITIONED: u32 = 0;

/// The recipe's `repeat('p', 70)` before a string key: the `key_pad` of
/// `keys::key_at_padded`, whose `user:` infix and sixteen zero-padded digits
/// are the rest of the recipe's `STRING_KEY`.
const STRING_PREFIX_LEN: usize = 70;

/// The recipe's `PERMUTED_KEY = pmod(id * 2654435761, 1000000007)`: a
/// bijection for ids below the modulus.
const PERMUTATION_MULTIPLIER: u64 = 2_654_435_761;
const PERMUTATION_MODULUS: u64 = 1_000_000_007;

/// The recipe's `repeat(sha2(...), 3)`: a contract value is the key's hex
/// digest three times.
pub const CONTRACT_REPEATS: usize = 3;

/// The recipe's `sequence(1, 256)`: a wide value concatenates this many
/// distinct digests.
pub const WIDE_REPEATS: usize = 256;

/// The ASCII bytes of one hex SHA-256 digest: the digest's width in hex
/// digits.
pub const HEX_DIGEST_BYTES: usize =
    <Sha256 as OutputSizeUser>::OutputSize::USIZE * HEX_DIGITS_PER_BYTE;

/// The tables the recipe writes. Display gives the table name it uses.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum GoldenTable {
    /// `long_2m`: the contract layout, long keys.
    Long2m,
    /// `str_2m`: the contract layout, string keys.
    Str2m,
    /// `bin16_500k`: the contract layout, sixteen-byte binary keys.
    Bin16_500k,
    /// `long_wide_250k`: the contract layout with byte-bounded pages and
    /// sixteen-kibibyte values.
    LongWide250k,
    /// `long_v3_50k`: the contract layout, long keys, in table format
    /// version 3 (the recipe's `FORMAT_V3`).
    LongV3_50k,
    /// `default_layout_1m`: the writer's defaults, priced by the doctor.
    DefaultLayout1m,
    /// `unpartitioned_500k`: no bucket partition.
    Unpartitioned500k,
    /// `overlap_250k`: permuted keys written with skew splitting on, so a
    /// bucket may hold overlapping sorted files.
    Overlap250k,
}

impl GoldenTable {
    /// The Iceberg type of the table's key column.
    pub fn key_type(self) -> KeyType {
        match self {
            Self::Long2m
            | Self::LongWide250k
            | Self::LongV3_50k
            | Self::DefaultLayout1m
            | Self::Unpartitioned500k
            | Self::Overlap250k => KeyType::Long,
            Self::Str2m => KeyType::String,
            Self::Bin16_500k => KeyType::Binary,
        }
    }

    /// The bucket count the table was created with; zero is unpartitioned.
    pub fn buckets(self) -> u32 {
        match self {
            Self::Long2m
            | Self::Str2m
            | Self::Bin16_500k
            | Self::LongV3_50k
            | Self::DefaultLayout1m => FOUR_BUCKETS,
            Self::LongWide250k | Self::Overlap250k => TWO_BUCKETS,
            Self::Unpartitioned500k => UNPARTITIONED,
        }
    }

    /// The rows the recipe filled the table with.
    pub fn rows(self) -> u64 {
        match self {
            Self::Long2m | Self::Str2m => ROWS_2M,
            Self::DefaultLayout1m => ROWS_1M,
            Self::Bin16_500k | Self::Unpartitioned500k => ROWS_500K,
            Self::LongWide250k | Self::Overlap250k => ROWS_250K,
            Self::LongV3_50k => ROWS_50K,
        }
    }
}

impl fmt::Display for GoldenTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Long2m => "long_2m",
            Self::Str2m => "str_2m",
            Self::Bin16_500k => "bin16_500k",
            Self::LongWide250k => "long_wide_250k",
            Self::LongV3_50k => "long_v3_50k",
            Self::DefaultLayout1m => "default_layout_1m",
            Self::Unpartitioned500k => "unpartitioned_500k",
            Self::Overlap250k => "overlap_250k",
        };
        f.write_str(name)
    }
}

/// The warehouse location from the environment; `Precondition::GoldenUnset`
/// when the variable is absent or not text.
pub fn warehouse() -> Result<String> {
    required_var(WAREHOUSE_VAR, Precondition::GoldenUnset)
}

/// The root of `table` under `warehouse`: `{warehouse}/db/{name}`, as the
/// Hadoop catalog lays it out; a `Static` source follows it through its
/// version hint.
pub fn table_config(warehouse: &str, table: GoldenTable, strict: bool) -> TableConfig {
    TableConfig {
        name: table.to_string(),
        source: SourceConfig::Static {
            location: format!("{warehouse}/{NAMESPACE}/{table}"),
        },
        r#ref: BRANCH.to_string(),
        key_column: KEY_COLUMN.to_string(),
        value_column: VALUE_COLUMN.to_string(),
        strict,
        residency: Residency::Full,
        resident_budget_bytes: None,
    }
}

/// A configuration serving `table` alone with default server and storage
/// settings; an `s3a://` warehouse needs the SDK's credential chain, so the
/// G tests open it through `icetray_core::open`.
pub fn config(warehouse: &str, table: GoldenTable, strict: bool) -> Config {
    Config {
        server: ServerConfig::default(),
        storage: StorageConfig::default(),
        tables: vec![table_config(warehouse, table, strict)],
    }
}

/// The recipe's `LONG_KEY` for `id`: the fixture derivation's long key,
/// whose `KEY_STEP` the recipe shares.
pub fn long_key(id: u64) -> Key {
    key_at(id, KeyType::Long)
}

/// The recipe's `STRING_KEY` for `id`: the fixture derivation's string key
/// under `STRING_PREFIX_LEN` bytes of padding.
pub fn string_key(id: u64) -> Key {
    key_at_padded(id, KeyType::String, STRING_PREFIX_LEN)
}

/// The recipe's `PERMUTED_KEY` for `id`, in `u64` arithmetic.
pub fn permuted_key(id: u64) -> Key {
    let value = (id * PERMUTATION_MULTIPLIER) % PERMUTATION_MODULUS;
    Key::Long(i64::try_from(value).expect("a value below the modulus fits i64"))
}

/// A contract table's value for the key whose Spark string form is
/// `key_text` (a long's decimal digits, a string itself): the lowercase hex
/// SHA-256 of that text, `CONTRACT_REPEATS` times, as ASCII bytes.
pub fn value(key_text: &str) -> Vec<u8> {
    hex_digest(key_text).repeat(CONTRACT_REPEATS).into_bytes()
}

/// A wide table's value for `key_text`: the hex SHA-256 of `{key_text}:{i}`
/// for `i` in `1..=WIDE_REPEATS`, concatenated, as ASCII bytes.
pub fn wide_value(key_text: &str) -> Vec<u8> {
    (1..=WIDE_REPEATS)
        .map(|i| hex_digest(&format!("{key_text}:{i}")))
        .collect::<String>()
        .into_bytes()
}

/// The lowercase hex SHA-256 of `text`, as Spark's `sha2(_, 256)` renders it.
fn hex_digest(text: &str) -> String {
    Sha256::digest(text.as_bytes()).iter().fold(
        String::with_capacity(HEX_DIGEST_BYTES),
        |mut hex, byte| {
            write!(hex, "{byte:0HEX_DIGITS_PER_BYTE$x}")
                .expect("formatting into a String cannot fail");
            hex
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys::{STRING_KEY_FIXED_BYTES, STRING_PAD};

    /// Every table, beside the name the recipe writes it under.
    const NAMES: [(GoldenTable, &str); 8] = [
        (GoldenTable::Long2m, "long_2m"),
        (GoldenTable::Str2m, "str_2m"),
        (GoldenTable::Bin16_500k, "bin16_500k"),
        (GoldenTable::LongWide250k, "long_wide_250k"),
        (GoldenTable::LongV3_50k, "long_v3_50k"),
        (GoldenTable::DefaultLayout1m, "default_layout_1m"),
        (GoldenTable::Unpartitioned500k, "unpartitioned_500k"),
        (GoldenTable::Overlap250k, "overlap_250k"),
    ];

    /// Ids the permutation is checked over: small next to the modulus, so
    /// the recipe's bijection claim holds.
    const PERMUTATION_SAMPLE: u64 = 10_000;

    /// A sample id and its recipe key texts.
    const SAMPLE_ID: u64 = 12_345;

    /// The SHA-256 of `"0"`, as `sha2('0', 256)` gives it.
    const SHA256_OF_ZERO: &str = "5feceb66ffc86f38d952786c6d696c79c2dbc239dd4e91b46729d73a27fb57e9";

    #[test]
    fn test_fmt_golden_table_matches_the_recipe() {
        for (table, name) in NAMES {
            assert_eq!(table.to_string(), name);
        }
    }

    #[test]
    fn test_table_config_names_the_hadoop_root() {
        let config = table_config("file:///w", GoldenTable::Str2m, false);
        assert_eq!(
            config.source,
            SourceConfig::Static {
                location: "file:///w/db/str_2m".to_string()
            }
        );
        assert_eq!(config.name, "str_2m");
        assert!(!config.strict);
        assert_eq!(config.residency, Residency::Full);
    }

    #[test]
    fn test_permuted_key_is_a_bijection_over_a_small_range() {
        let mut seen = std::collections::HashSet::new();
        for id in 0..PERMUTATION_SAMPLE {
            let Key::Long(value) = permuted_key(id) else {
                panic!("permuted keys are long");
            };
            assert!(value >= 0);
            assert!(seen.insert(value), "id {id} repeats {value}");
        }
    }

    #[test]
    fn test_string_key_has_the_recipe_shape() {
        let key = string_key(SAMPLE_ID);
        assert_eq!(
            key,
            key_at_padded(SAMPLE_ID, KeyType::String, STRING_PREFIX_LEN)
        );
        let Key::Str(text) = key else {
            panic!("string keys are strings");
        };
        assert_eq!(text.len(), STRING_PREFIX_LEN + STRING_KEY_FIXED_BYTES);
        assert!(text.starts_with(&STRING_PAD.to_string().repeat(STRING_PREFIX_LEN)));
        assert_eq!(long_key(SAMPLE_ID), key_at(SAMPLE_ID, KeyType::Long));
    }

    #[test]
    fn test_value_has_contract_repeats_of_the_hex_digest() {
        let value = value("0");
        assert_eq!(value.len(), CONTRACT_REPEATS * HEX_DIGEST_BYTES);
        assert_eq!(value, SHA256_OF_ZERO.repeat(CONTRACT_REPEATS).into_bytes());
    }

    #[test]
    fn test_wide_value_concatenates_distinct_digests() {
        let value = wide_value("0");
        assert_eq!(value.len(), WIDE_REPEATS * HEX_DIGEST_BYTES);
        let first = &value[..HEX_DIGEST_BYTES];
        let second = &value[HEX_DIGEST_BYTES..2 * HEX_DIGEST_BYTES];
        assert_ne!(first, second);
        assert_eq!(first, hex_digest("0:1").as_bytes());
    }
}
