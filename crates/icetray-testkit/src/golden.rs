//! The golden tables `dev/golden-tables.py` writes (RFD 014): where they
//! are, what each holds, and the key and value expressions of the recipe as
//! Rust, so a G-tier test checks an answer without a stored oracle. The
//! tables live outside the tree, named by `ICETRAY_GOLDEN_WAREHOUSE`; a run
//! without it fails on `Precondition::GoldenUnset` rather than passing.

use std::fmt::{self, Write as _};
use std::ops::Range;

use icetray_core::config::{Config, ServerConfig, SourceConfig, StorageConfig, TableConfig};
use icetray_core::key::HEX_DIGITS_PER_BYTE;
use icetray_core::{Key, KeyType, OrderValue, Residency};
use sha2::digest::OutputSizeUser;
use sha2::digest::typenum::Unsigned as _;
use sha2::{Digest as _, Sha256};

use crate::error::{Precondition, Result, required_var};
use crate::keys::{key_at, key_at_padded};
use crate::layout::{KEY_COLUMN, VALUE_COLUMN};
use crate::ordered::{ORDER_COLUMN, OrderKind};

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

/// Rows of the merge-on-read tables before their changes.
const ROWS_100K: u64 = 100_000;

/// Rows of `long_v3_50k`.
const ROWS_50K: u64 = 50_000;

/// Buckets of the contract tables, `long_v3_50k`, the merge-on-read tables,
/// and `default_layout_1m` (`create(..., 4)`).
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

/// The recipe's `MOR_DELETED_IDS`: the ids a merge-on-read table's DELETE
/// removes, half-open.
pub const MOR_DELETED_IDS: Range<u64> = 10_000..20_000;

/// The recipe's `MOR_UPDATED_IDS`: the ids a merge-on-read table's UPDATE
/// rewrites at `MOR_UPDATE_VERSION`, half-open.
pub const MOR_UPDATED_IDS: Range<u64> = 50_000..60_000;

/// The recipe's `MOR_FIRST_VERSION`: the version a merge-on-read table's
/// INSERT writes every id at.
pub const MOR_FIRST_VERSION: u64 = 1;

/// The recipe's `MOR_UPDATE_VERSION`: the version its UPDATE writes.
pub const MOR_UPDATE_VERSION: u64 = 2;

/// The recipe's `ORDERED_KEYS`: the keys of an ordered table.
pub const ORDERED_KEYS: u64 = 1_000;

/// The recipe's `ORDERED_ROWS_PER_KEY`: the rows each key of an ordered
/// table starts with.
pub const ORDERED_ROWS_PER_KEY: u64 = 100;

/// The recipe's `ORDER_START`: the raw order value of a key's first row.
pub const ORDER_START: i64 = 1_000;

/// The recipe's `ORDER_STEP`: the raw distance between a key's consecutive
/// rows, leaving room for the rows a MERGE inserts between two.
pub const ORDER_STEP: i64 = 10;

/// The recipe's `APPEND_ROWS_PER_KEY`: the rows `ordered_appended_100k`
/// adds to every key, after its last.
pub const APPEND_ROWS_PER_KEY: u64 = 10;

/// The recipe's `MERGE_KEY_IDS`: the ids whose keys an ordered table's
/// MERGE changes, half-open.
pub const MERGE_KEY_IDS: Range<u64> = 300..400;

/// The recipe's `MERGE_UPDATE_EVERY`: of each merged key's rows, every
/// row at an index divisible by this is matched and updated.
pub const MERGE_UPDATE_EVERY: u64 = 2;

/// The recipe's `MERGE_INSERT_EVERY`: after each row of a merged key at
/// an index divisible by this, the MERGE inserts a row.
pub const MERGE_INSERT_EVERY: u64 = 10;

/// The recipe's `MERGE_INSERT_OFFSET`: the raw order value an inserted row
/// lies above the row before it.
pub const MERGE_INSERT_OFFSET: i64 = 5;

/// The recipe's `ORDER_PREFIX_LEN`: the bytes of a string order value's
/// prefix, the key's text left-padded with `k`.
const ORDER_PREFIX_LEN: usize = 64;

/// The recipe's `ORDER_STRING_DIGITS`: the digits a string order value's raw
/// part is zero-padded to.
const ORDER_STRING_DIGITS: usize = 16;

/// The padding of a string order value's prefix, as `lpad(.., 64, 'k')`.
const ORDER_PREFIX_PAD: char = 'k';

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
    /// `default_layout_1m`: the writer's defaults, whose large key pages
    /// the doctor finds cap the level at balanced.
    DefaultLayout1m,
    /// `unpartitioned_500k`: no bucket partition.
    Unpartitioned500k,
    /// `overlap_250k`: permuted keys written with skew splitting on, so a
    /// bucket may hold overlapping sorted files.
    Overlap250k,
    /// `long_mor_v2_100k`: the contract layout in format version 2, then a
    /// merge-on-read DELETE of `MOR_DELETED_IDS` and UPDATE of
    /// `MOR_UPDATED_IDS`, recorded as Parquet position deletes.
    LongMorV2_100k,
    /// `long_mor_v3_100k`: the same in format version 3, recorded as
    /// deletion vectors.
    LongMorV3_100k,
    /// `long_mor_v2_compacted_100k`: `long_mor_v2_100k`'s commands, then
    /// `rewrite_data_files`, which leaves its position deletes listed and
    /// dangling.
    LongMorV2Compacted100k,
    /// `long_mor_v3_compacted_100k`: `long_mor_v3_100k`'s commands, then
    /// `rewrite_data_files` and `rewrite_position_delete_files`.
    LongMorV3Compacted100k,
    /// `ordered_compacted_100k`: an ordered table of long keys and a long
    /// order column, rewritten by a sorting compaction.
    OrderedCompacted100k,
    /// `ordered_appended_100k`: the same rows, then `APPEND_ROWS_PER_KEY`
    /// more for every key at later order values, in a second file per bucket.
    OrderedAppended100k,
    /// `ordered_overlap_100k`: permuted keys written with skew splitting
    /// on, in two buckets.
    OrderedOverlap100k,
    /// `ordered_merge_v2_100k`: a merge-on-read MERGE on the key and the
    /// order value, recorded as Parquet position deletes.
    OrderedMergeV2_100k,
    /// `ordered_merge_v3_100k`: the same MERGE, recorded as deletion vectors.
    OrderedMergeV3_100k,
    /// `ordered_merge_v3_compacted_100k`: the version 3 MERGE, then a sorting
    /// compaction and `rewrite_position_delete_files`.
    OrderedMergeV3Compacted100k,
    /// `ordered_string_100k`: a string order column whose values share a
    /// 64-byte prefix per key, compacted by a sorting compaction.
    OrderedString100k,
}

impl GoldenTable {
    /// Every table the recipe's `ordered` group writes.
    pub const ORDERED: [Self; 7] = [
        Self::OrderedCompacted100k,
        Self::OrderedAppended100k,
        Self::OrderedOverlap100k,
        Self::OrderedMergeV2_100k,
        Self::OrderedMergeV3_100k,
        Self::OrderedMergeV3Compacted100k,
        Self::OrderedString100k,
    ];

    /// The order column's type, or `None` for a key-value table.
    pub fn order_kind(self) -> Option<OrderKind> {
        match self {
            Self::OrderedCompacted100k
            | Self::OrderedAppended100k
            | Self::OrderedOverlap100k
            | Self::OrderedMergeV2_100k
            | Self::OrderedMergeV3_100k
            | Self::OrderedMergeV3Compacted100k => Some(OrderKind::Long),
            Self::OrderedString100k => Some(OrderKind::Str),
            Self::Long2m
            | Self::Str2m
            | Self::Bin16_500k
            | Self::LongWide250k
            | Self::LongV3_50k
            | Self::DefaultLayout1m
            | Self::Unpartitioned500k
            | Self::Overlap250k
            | Self::LongMorV2_100k
            | Self::LongMorV3_100k
            | Self::LongMorV2Compacted100k
            | Self::LongMorV3Compacted100k => None,
        }
    }

    /// The Iceberg type of the table's key column.
    pub fn key_type(self) -> KeyType {
        match self {
            Self::Long2m
            | Self::LongWide250k
            | Self::LongV3_50k
            | Self::DefaultLayout1m
            | Self::Unpartitioned500k
            | Self::Overlap250k
            | Self::LongMorV2_100k
            | Self::LongMorV3_100k
            | Self::LongMorV2Compacted100k
            | Self::LongMorV3Compacted100k
            | Self::OrderedCompacted100k
            | Self::OrderedAppended100k
            | Self::OrderedOverlap100k
            | Self::OrderedMergeV2_100k
            | Self::OrderedMergeV3_100k
            | Self::OrderedMergeV3Compacted100k
            | Self::OrderedString100k => KeyType::Long,
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
            | Self::DefaultLayout1m
            | Self::LongMorV2_100k
            | Self::LongMorV3_100k
            | Self::LongMorV2Compacted100k
            | Self::LongMorV3Compacted100k
            | Self::OrderedCompacted100k
            | Self::OrderedAppended100k
            | Self::OrderedMergeV2_100k
            | Self::OrderedMergeV3_100k
            | Self::OrderedMergeV3Compacted100k
            | Self::OrderedString100k => FOUR_BUCKETS,
            Self::LongWide250k | Self::Overlap250k | Self::OrderedOverlap100k => TWO_BUCKETS,
            Self::Unpartitioned500k => UNPARTITIONED,
        }
    }

    /// The rows the recipe filled the table with, before any change.
    pub fn rows(self) -> u64 {
        match self {
            Self::Long2m | Self::Str2m => ROWS_2M,
            Self::DefaultLayout1m => ROWS_1M,
            Self::Bin16_500k | Self::Unpartitioned500k => ROWS_500K,
            Self::LongWide250k | Self::Overlap250k => ROWS_250K,
            Self::LongV3_50k => ROWS_50K,
            Self::LongMorV2_100k
            | Self::LongMorV3_100k
            | Self::LongMorV2Compacted100k
            | Self::LongMorV3Compacted100k
            | Self::OrderedCompacted100k
            | Self::OrderedAppended100k
            | Self::OrderedOverlap100k
            | Self::OrderedMergeV2_100k
            | Self::OrderedMergeV3_100k
            | Self::OrderedMergeV3Compacted100k
            | Self::OrderedString100k => ROWS_100K,
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
            Self::LongMorV2_100k => "long_mor_v2_100k",
            Self::LongMorV3_100k => "long_mor_v3_100k",
            Self::LongMorV2Compacted100k => "long_mor_v2_compacted_100k",
            Self::LongMorV3Compacted100k => "long_mor_v3_compacted_100k",
            Self::OrderedCompacted100k => "ordered_compacted_100k",
            Self::OrderedAppended100k => "ordered_appended_100k",
            Self::OrderedOverlap100k => "ordered_overlap_100k",
            Self::OrderedMergeV2_100k => "ordered_merge_v2_100k",
            Self::OrderedMergeV3_100k => "ordered_merge_v3_100k",
            Self::OrderedMergeV3Compacted100k => "ordered_merge_v3_compacted_100k",
            Self::OrderedString100k => "ordered_string_100k",
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
/// Hadoop catalog lays it out; a `Static` source, and so `--location`,
/// follows it through its version hint.
pub fn location(warehouse: &str, table: GoldenTable) -> String {
    format!("{warehouse}/{NAMESPACE}/{table}")
}

/// `table` under `warehouse` as a `Static` source at its `location`.
pub fn table_config(warehouse: &str, table: GoldenTable, strict: bool) -> TableConfig {
    TableConfig {
        name: table.to_string(),
        source: SourceConfig::Static {
            location: location(warehouse, table),
        },
        r#ref: BRANCH.to_string(),
        key_column: KEY_COLUMN.to_string(),
        value_columns: vec![icetray_core::ValueColumnConfig::named(VALUE_COLUMN)],
        order_column: table.order_kind().map(|_| ORDER_COLUMN.to_string()),
        max_scan_rows: None,
        strict,
        lookups: None,
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

/// A merge-on-read table's value for the key whose Spark string form is
/// `key_text` at `version`: `value` of `v{version}:{key_text}`, the recipe's
/// `value_expr(..., version=version)`.
pub fn versioned_value(key_text: &str, version: u64) -> Vec<u8> {
    value(&format!("v{version}:{key_text}"))
}

/// What a lookup of `long_key(id)` answers in a merge-on-read table once
/// the recipe's commands ran: absent for an id in `MOR_DELETED_IDS`, the
/// `MOR_UPDATE_VERSION` value for one in `MOR_UPDATED_IDS`, and the
/// `MOR_FIRST_VERSION` value for every other id the table was filled with.
pub fn merge_on_read_value(id: u64) -> Option<Vec<u8>> {
    if MOR_DELETED_IDS.contains(&id) {
        return None;
    }
    let version = if MOR_UPDATED_IDS.contains(&id) {
        MOR_UPDATE_VERSION
    } else {
        MOR_FIRST_VERSION
    };
    Some(versioned_value(&long_key_text(id), version))
}

/// The decimal text Spark casts `long_key(id)` to.
fn long_key_text(id: u64) -> String {
    long_key_text_of(&long_key(id))
}

/// One row of a golden ordered table's key, as the recipe's commands leave
/// it: its raw order value and the version that wrote it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrderedRow {
    /// The raw order value: the recipe's `ORDER_START + j * ORDER_STEP`, at
    /// the row's index `j`, and `MERGE_INSERT_OFFSET` above that for a row
    /// the MERGE inserted.
    pub order: i64,
    /// The version whose value the row holds: `MOR_FIRST_VERSION`, or
    /// `MOR_UPDATE_VERSION` for a row the MERGE wrote.
    pub version: u64,
}

/// The raw order value of the recipe's row index `index` of a key.
fn order_at(index: u64) -> i64 {
    ORDER_START + i64::try_from(index).expect("row indexes are small") * ORDER_STEP
}

/// Whether `table`'s recipe ends in a MERGE over `MERGE_KEY_IDS`.
fn is_merged(table: GoldenTable) -> bool {
    match table {
        GoldenTable::OrderedMergeV2_100k
        | GoldenTable::OrderedMergeV3_100k
        | GoldenTable::OrderedMergeV3Compacted100k => true,
        GoldenTable::Long2m
        | GoldenTable::Str2m
        | GoldenTable::Bin16_500k
        | GoldenTable::LongWide250k
        | GoldenTable::LongV3_50k
        | GoldenTable::DefaultLayout1m
        | GoldenTable::Unpartitioned500k
        | GoldenTable::Overlap250k
        | GoldenTable::LongMorV2_100k
        | GoldenTable::LongMorV3_100k
        | GoldenTable::LongMorV2Compacted100k
        | GoldenTable::LongMorV3Compacted100k
        | GoldenTable::OrderedCompacted100k
        | GoldenTable::OrderedAppended100k
        | GoldenTable::OrderedOverlap100k
        | GoldenTable::OrderedString100k => false,
    }
}

/// The key of key index `id` in an ordered golden table: the recipe's
/// `ordered_exprs` key, `LONG_KEY` of the index or, in the overlap table,
/// `PERMUTED_KEY` of it.
pub fn ordered_key(table: GoldenTable, id: u64) -> Key {
    match table {
        GoldenTable::OrderedOverlap100k => permuted_key(id),
        GoldenTable::OrderedCompacted100k
        | GoldenTable::OrderedAppended100k
        | GoldenTable::OrderedMergeV2_100k
        | GoldenTable::OrderedMergeV3_100k
        | GoldenTable::OrderedMergeV3Compacted100k
        | GoldenTable::OrderedString100k => long_key(id),
        GoldenTable::Long2m
        | GoldenTable::Str2m
        | GoldenTable::Bin16_500k
        | GoldenTable::LongWide250k
        | GoldenTable::LongV3_50k
        | GoldenTable::DefaultLayout1m
        | GoldenTable::Unpartitioned500k
        | GoldenTable::Overlap250k
        | GoldenTable::LongMorV2_100k
        | GoldenTable::LongMorV3_100k
        | GoldenTable::LongMorV2Compacted100k
        | GoldenTable::LongMorV3Compacted100k => panic!("{table} is not an ordered table"),
    }
}

/// The live rows of the key at key index `id` in an ordered golden table
/// once the recipe's commands ran, ascending by order value: the
/// `ORDERED_ROWS_PER_KEY` rows of the first fill, `APPEND_ROWS_PER_KEY` more
/// after them in the appended table, and in a merged table, for a key in
/// `MERGE_KEY_IDS`, the matched rows at `MOR_UPDATE_VERSION` and the
/// inserted ones between.
pub fn ordered_rows(table: GoldenTable, id: u64) -> Vec<OrderedRow> {
    let appended = if table == GoldenTable::OrderedAppended100k {
        APPEND_ROWS_PER_KEY
    } else {
        0
    };
    let merged = is_merged(table) && MERGE_KEY_IDS.contains(&id);
    let mut rows = Vec::new();
    for index in 0..ORDERED_ROWS_PER_KEY + appended {
        let updated = merged && index % MERGE_UPDATE_EVERY == 0;
        rows.push(OrderedRow {
            order: order_at(index),
            version: if updated {
                MOR_UPDATE_VERSION
            } else {
                MOR_FIRST_VERSION
            },
        });
        if merged && index % MERGE_INSERT_EVERY == 0 {
            rows.push(OrderedRow {
                order: order_at(index) + MERGE_INSERT_OFFSET,
                version: MOR_UPDATE_VERSION,
            });
        }
    }
    rows
}

/// The text of the order value with raw value `order` for the key at key
/// index `id`: its decimal digits in a long order column, and in
/// `ordered_string_100k` the recipe's `ORDER_PREFIX_LEN` bytes of the key's
/// text left-padded with `k`, then the raw value zero-padded to
/// `ORDER_STRING_DIGITS`.
pub fn ordered_order_text(table: GoldenTable, id: u64, order: i64) -> String {
    if table != GoldenTable::OrderedString100k {
        return order.to_string();
    }
    let key = long_key_text_of(&ordered_key(table, id));
    let padding = ORDER_PREFIX_PAD
        .to_string()
        .repeat(ORDER_PREFIX_LEN.saturating_sub(key.len()));
    format!("{padding}{key}{order:0ORDER_STRING_DIGITS$}")
}

/// The order value a Scan states or returns for raw order value `order` of
/// the key at key index `id`: a long, or the string `ordered_order_text`.
pub fn ordered_order_value(table: GoldenTable, id: u64, order: i64) -> OrderValue {
    assert!(
        table.order_kind().is_some(),
        "{table} is not an ordered table"
    );
    if table == GoldenTable::OrderedString100k {
        OrderValue::Str(ordered_order_text(table, id, order))
    } else {
        OrderValue::Long(order)
    }
}

/// An ordered golden table's value for the row at raw order value `order`
/// of the key at key index `id`, written at `version`: `value` of
/// `v{version}:{key}:{order text}`, the recipe's `value_expr(...,
/// order_expr=...)`.
pub fn ordered_value(table: GoldenTable, id: u64, order: i64, version: u64) -> Vec<u8> {
    let key = long_key_text_of(&ordered_key(table, id));
    versioned_value(
        &format!("{key}:{}", ordered_order_text(table, id, order)),
        version,
    )
}

/// The decimal text Spark casts a long `key` to.
fn long_key_text_of(key: &Key) -> String {
    let Key::Long(value) = key else {
        panic!("an ordered golden table has long keys, got {key}");
    };
    value.to_string()
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
    const NAMES: [(GoldenTable, &str); 19] = [
        (GoldenTable::Long2m, "long_2m"),
        (GoldenTable::Str2m, "str_2m"),
        (GoldenTable::Bin16_500k, "bin16_500k"),
        (GoldenTable::LongWide250k, "long_wide_250k"),
        (GoldenTable::LongV3_50k, "long_v3_50k"),
        (GoldenTable::DefaultLayout1m, "default_layout_1m"),
        (GoldenTable::Unpartitioned500k, "unpartitioned_500k"),
        (GoldenTable::Overlap250k, "overlap_250k"),
        (GoldenTable::LongMorV2_100k, "long_mor_v2_100k"),
        (GoldenTable::LongMorV3_100k, "long_mor_v3_100k"),
        (
            GoldenTable::LongMorV2Compacted100k,
            "long_mor_v2_compacted_100k",
        ),
        (
            GoldenTable::LongMorV3Compacted100k,
            "long_mor_v3_compacted_100k",
        ),
        (GoldenTable::OrderedCompacted100k, "ordered_compacted_100k"),
        (GoldenTable::OrderedAppended100k, "ordered_appended_100k"),
        (GoldenTable::OrderedOverlap100k, "ordered_overlap_100k"),
        (GoldenTable::OrderedMergeV2_100k, "ordered_merge_v2_100k"),
        (GoldenTable::OrderedMergeV3_100k, "ordered_merge_v3_100k"),
        (
            GoldenTable::OrderedMergeV3Compacted100k,
            "ordered_merge_v3_compacted_100k",
        ),
        (GoldenTable::OrderedString100k, "ordered_string_100k"),
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
    fn test_merge_on_read_value_follows_the_recipe_commands() {
        assert_eq!(merge_on_read_value(MOR_DELETED_IDS.start), None);
        assert_eq!(merge_on_read_value(MOR_DELETED_IDS.end - 1), None);
        let updated = MOR_UPDATED_IDS.start;
        let updated_text = long_key_text(updated);
        assert_eq!(
            merge_on_read_value(updated),
            Some(value(&format!("v{MOR_UPDATE_VERSION}:{updated_text}")))
        );
        let untouched = MOR_DELETED_IDS.end;
        assert_eq!(
            merge_on_read_value(untouched),
            Some(value(&format!(
                "v{MOR_FIRST_VERSION}:{}",
                long_key_text(untouched)
            )))
        );
        assert_ne!(
            versioned_value(&updated_text, MOR_FIRST_VERSION),
            versioned_value(&updated_text, MOR_UPDATE_VERSION),
        );
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

    /// A merged key of `MERGE_KEY_IDS`, and an id outside them.
    const MERGED_ID: u64 = 350;
    const UNMERGED_ID: u64 = 5;

    #[test]
    fn test_ordered_rows_are_ascending_and_follow_the_recipe_commands() {
        let rows_per_key = usize::try_from(ORDERED_ROWS_PER_KEY).unwrap();
        for table in GoldenTable::ORDERED {
            for id in [UNMERGED_ID, MERGED_ID] {
                let rows = ordered_rows(table, id);
                assert!(
                    rows.windows(2).all(|pair| pair[0].order < pair[1].order),
                    "{table} {id}"
                );
                let first = rows.iter().filter(|row| row.version == MOR_FIRST_VERSION);
                let updated = rows.iter().filter(|row| row.version == MOR_UPDATE_VERSION);
                let merged = is_merged(table) && id == MERGED_ID;
                let appended = usize::try_from(APPEND_ROWS_PER_KEY).unwrap();
                if merged {
                    let matched = rows_per_key / usize::try_from(MERGE_UPDATE_EVERY).unwrap();
                    let inserted = rows_per_key / usize::try_from(MERGE_INSERT_EVERY).unwrap();
                    assert_eq!(updated.count(), matched + inserted, "{table}");
                    assert_eq!(first.count(), rows_per_key - matched, "{table}");
                    assert_eq!(rows.len(), rows_per_key + inserted, "{table}");
                } else {
                    assert_eq!(updated.count(), 0, "{table} {id}");
                    let expected = if table == GoldenTable::OrderedAppended100k {
                        rows_per_key + appended
                    } else {
                        rows_per_key
                    };
                    assert_eq!(rows.len(), expected, "{table} {id}");
                }
            }
        }
    }

    #[test]
    fn test_ordered_rows_insert_between_two_rows_of_the_first_fill() {
        let rows = ordered_rows(GoldenTable::OrderedMergeV2_100k, MERGED_ID);
        let inserted = order_at(0) + MERGE_INSERT_OFFSET;
        let at = rows
            .iter()
            .position(|row| row.order == inserted)
            .expect("the MERGE inserts a row after the first");
        assert_eq!(rows[at - 1].order, order_at(0));
        assert_eq!(rows[at + 1].order, order_at(1));
        assert_eq!(rows[at].version, MOR_UPDATE_VERSION);
    }

    #[test]
    fn test_ordered_order_text_of_the_string_table_shares_a_long_prefix_per_key() {
        let table = GoldenTable::OrderedString100k;
        let first = ordered_order_text(table, SAMPLE_ID, order_at(0));
        let last = ordered_order_text(table, SAMPLE_ID, order_at(ORDERED_ROWS_PER_KEY - 1));
        assert_eq!(first.len(), ORDER_PREFIX_LEN + ORDER_STRING_DIGITS);
        assert_eq!(first[..ORDER_PREFIX_LEN], last[..ORDER_PREFIX_LEN]);
        assert!(first < last);
        let other = ordered_order_text(table, SAMPLE_ID + 1, order_at(0));
        assert_ne!(first[..ORDER_PREFIX_LEN], other[..ORDER_PREFIX_LEN]);
        assert_eq!(
            ordered_order_text(GoldenTable::OrderedCompacted100k, SAMPLE_ID, order_at(0)),
            order_at(0).to_string()
        );
    }

    #[test]
    fn test_ordered_value_is_a_function_of_key_order_and_version() {
        let table = GoldenTable::OrderedCompacted100k;
        let order = order_at(3);
        let value = ordered_value(table, SAMPLE_ID, order, MOR_FIRST_VERSION);
        let text = format!("v{MOR_FIRST_VERSION}:{}:{order}", long_key_text(SAMPLE_ID));
        assert_eq!(value, super::value(&text));
        assert_ne!(
            value,
            ordered_value(table, SAMPLE_ID, order, MOR_UPDATE_VERSION)
        );
        assert_ne!(
            value,
            ordered_value(table, SAMPLE_ID, order_at(4), MOR_FIRST_VERSION)
        );
    }

    #[test]
    fn test_table_config_names_the_order_column_of_an_ordered_table_only() {
        for table in GoldenTable::ORDERED {
            let config = table_config("file:///w", table, false);
            assert_eq!(config.order_column.as_deref(), Some(ORDER_COLUMN));
        }
        let config = table_config("file:///w", GoldenTable::Long2m, false);
        assert_eq!(config.order_column, None);
    }
}
