//! Ordered fixtures (RFD 025): tables of many rows per key, written sorted
//! by key and then order value, and the naive-sort model every Scan is
//! checked against. A row's value is a function of its key, its order value,
//! and the version that wrote it (RFD 014), so a Scan's answer is checkable
//! without a stored oracle, and a live row is told apart from a deleted copy
//! of its pair. The model reads only the fixture's own bookkeeping and sorts
//! it the obvious way; it shares nothing with the server's merge.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, Date32Array, FixedSizeBinaryArray, Int32Array, Int64Array, StringArray,
    TimestampMicrosecondArray, TimestampNanosecondArray,
};
use arrow::buffer::Buffer;
use iceberg::spec::PrimitiveType;
use icetray_core::{DEFAULT_MAX_SCAN_ROWS, Direction, Key, OrderValue, ScanBounds};

use crate::error::{Result, SpecFault};
use crate::keys::{FIRST_VERSION, ValueKind, value_of_version};

/// The field id of the order column in an ordered fixture that also holds
/// `SECOND_VALUE_COLUMN`. The catalog numbers a new table's columns in
/// schema order and the order column is the last, so without the second
/// value column it takes the next id, `SECOND_VALUE_FIELD_ID`:
/// `Fixture::order_field_id` names the id either way.
pub const ORDER_FIELD_ID: i32 = 4;

/// The order column of every ordered fixture: a timestamp, or whatever
/// `OrderKind` the specification names.
pub const ORDER_COLUMN: &str = "ts";

/// Digits a string order value is zero-padded to: those of the largest
/// long, so bytewise order is numeric order for every non-negative value.
const ORDER_STRING_DIGITS: usize = i64::MAX.ilog10() as usize + 1;

/// The bytes of a long as an order value's bytes: a binary order value is
/// this wide, and a fixed one is at least this wide or holds the low bytes
/// alone.
const ORDER_LONG_BYTES: usize = size_of::<u64>();

/// The zone a timestamp-with-zone column's values are in: UTC, as the
/// library's schema conversion gives it.
const UTC: &str = "+00:00";

/// The types the order column takes (RFD 025, Order values).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderKind {
    /// Iceberg `long`.
    Long,
    /// Iceberg `int`: raw values must fit 32 bits.
    Int,
    /// Iceberg `date`, in days: raw values must fit 32 bits.
    Date,
    /// Iceberg `timestamp`, in microseconds.
    Timestamp,
    /// Iceberg `timestamptz`, in microseconds.
    TimestampTz,
    /// Iceberg `timestamp_ns`, in nanoseconds; a format version 3 type.
    TimestampNs,
    /// Iceberg `timestamptz_ns`, in nanoseconds; a format version 3 type.
    TimestampTzNs,
    /// Iceberg `string`: the raw value as zero-padded decimal, so bytewise
    /// order is numeric order. Raw values must not be negative.
    Str,
    /// Iceberg `string`: the raw value as decimal zero-padded to this many
    /// digits, or to `Str`'s when that is more, so bytewise order is
    /// numeric order and every value starts with the zeros the width adds
    /// past its digits: a long prefix all values share, inside which a
    /// truncated column index cuts. Raw values must not be negative.
    PaddedStr(u32),
    /// Iceberg `binary`: the raw value as eight big-endian bytes. Raw
    /// values must not be negative.
    Bin,
    /// Iceberg `fixed(n)`: the raw value as big-endian bytes, padded with
    /// zeros to `n`. Raw values must not be negative or past `n` bytes.
    Fixed(u32),
}

impl OrderKind {
    /// The Iceberg type of the order column.
    pub fn primitive(self) -> PrimitiveType {
        match self {
            Self::Long => PrimitiveType::Long,
            Self::Int => PrimitiveType::Int,
            Self::Date => PrimitiveType::Date,
            Self::Timestamp => PrimitiveType::Timestamp,
            Self::TimestampTz => PrimitiveType::Timestamptz,
            Self::TimestampNs => PrimitiveType::TimestampNs,
            Self::TimestampTzNs => PrimitiveType::TimestamptzNs,
            Self::Str | Self::PaddedStr(_) => PrimitiveType::String,
            Self::Bin => PrimitiveType::Binary,
            Self::Fixed(width) => PrimitiveType::Fixed(u64::from(width)),
        }
    }

    /// The order value `raw` is in this kind, or `None` when the kind
    /// cannot hold it: past 32 bits for `Int` and `Date`, negative or too
    /// wide for the byte kinds.
    pub fn encode(self, raw: i64) -> Option<OrderValue> {
        match self {
            Self::Long
            | Self::Timestamp
            | Self::TimestampTz
            | Self::TimestampNs
            | Self::TimestampTzNs => Some(OrderValue::Long(raw)),
            Self::Int | Self::Date => i32::try_from(raw)
                .ok()
                .map(|narrow| OrderValue::Long(i64::from(narrow))),
            Self::Str => string_of(raw, ORDER_STRING_DIGITS).map(OrderValue::Str),
            Self::PaddedStr(width) => string_of(raw, padded_digits(width)).map(OrderValue::Str),
            Self::Bin => bytes_of(raw, ORDER_LONG_BYTES).map(OrderValue::Bytes),
            Self::Fixed(width) => bytes_of(raw, width_of(width)).map(OrderValue::Bytes),
        }
    }

    /// `raws` as the order column's arrow array, which the schema's own
    /// conversion accepts as it is; a raw value this kind cannot hold is
    /// `SpecFault::OrderUnencodable`.
    pub(crate) fn array(self, raws: &[i64]) -> Result<ArrayRef> {
        let unencodable = |raw: i64| SpecFault::OrderUnencodable { kind: self, raw };
        let narrow = || -> Result<Vec<i32>> {
            raws.iter()
                .map(|&raw| i32::try_from(raw).map_err(|_| unencodable(raw).into()))
                .collect()
        };
        let strings = |digits: usize| -> Result<Vec<String>> {
            raws.iter()
                .map(|&raw| string_of(raw, digits).ok_or_else(|| unencodable(raw).into()))
                .collect()
        };
        let bytes = |width: usize| -> Result<Vec<Vec<u8>>> {
            raws.iter()
                .map(|&raw| bytes_of(raw, width).ok_or_else(|| unencodable(raw).into()))
                .collect()
        };
        Ok(match self {
            Self::Long => Arc::new(Int64Array::from(raws.to_vec())),
            Self::Int => Arc::new(Int32Array::from(narrow()?)),
            Self::Date => Arc::new(Date32Array::from(narrow()?)),
            Self::Timestamp => Arc::new(TimestampMicrosecondArray::from(raws.to_vec())),
            Self::TimestampTz => {
                Arc::new(TimestampMicrosecondArray::from(raws.to_vec()).with_timezone(UTC))
            }
            Self::TimestampNs => Arc::new(TimestampNanosecondArray::from(raws.to_vec())),
            Self::TimestampTzNs => {
                Arc::new(TimestampNanosecondArray::from(raws.to_vec()).with_timezone(UTC))
            }
            Self::Str => Arc::new(StringArray::from(strings(ORDER_STRING_DIGITS)?)),
            Self::PaddedStr(width) => Arc::new(StringArray::from(strings(padded_digits(width))?)),
            Self::Bin => Arc::new(BinaryArray::from_iter_values(bytes(ORDER_LONG_BYTES)?)),
            Self::Fixed(width) => Arc::new(FixedSizeBinaryArray::new(
                i32::try_from(width).map_err(|_| SpecFault::OrderUnencodable {
                    kind: self,
                    raw: i64::from(width),
                })?,
                Buffer::from(bytes(width_of(width))?.concat()),
                None,
            )),
        })
    }
}

/// `width` as a byte count.
fn width_of(width: u32) -> usize {
    usize::try_from(width).expect("a fixed width fits usize")
}

/// `raw` as decimal zero-padded to `digits`, or `None` when negative.
fn string_of(raw: i64, digits: usize) -> Option<String> {
    u64::try_from(raw)
        .ok()
        .map(|value| format!("{value:0digits$}"))
}

/// The digits a `PaddedStr` of `width` pads to: `width`, or
/// `ORDER_STRING_DIGITS` when that is more, so every non-negative long
/// fits and bytewise order stays numeric order.
fn padded_digits(width: u32) -> usize {
    width_of(width).max(ORDER_STRING_DIGITS)
}

/// `raw` as `width` big-endian bytes, or `None` when negative or when it
/// does not fit.
fn bytes_of(raw: i64, width: usize) -> Option<Vec<u8>> {
    let value = u64::try_from(raw).ok()?.to_be_bytes();
    let Some(padding) = width.checked_sub(value.len()) else {
        let (head, tail) = value.split_at(value.len() - width);
        return head.iter().all(|&byte| byte == 0).then(|| tail.to_vec());
    };
    Some(std::iter::repeat_n(0, padding).chain(value).collect())
}

/// The order value of `raw` under `kind`, as a Scan's bound or a row's
/// order value is stated.
///
/// Panics on a raw value the kind cannot hold (`OrderKind::encode`).
pub fn order_value(kind: OrderKind, raw: i64) -> OrderValue {
    kind.encode(raw)
        .unwrap_or_else(|| panic!("{kind:?} cannot hold {raw}"))
}

/// The `len`-byte value of `kind` that version `version` of the row at
/// (`key`, `order`) stores: a function of all three (RFD 014), so two rows
/// of one key differ, and an update's value differs from every other
/// version's. Built as `value_of_version` of a key that holds the pair, so
/// every `ValueKind` stays its own shape.
pub fn row_value(key: &Key, order: i64, len: usize, kind: ValueKind, version: u64) -> Vec<u8> {
    let mut pair = key.as_bytes().to_vec();
    pair.extend_from_slice(&order.to_be_bytes());
    value_of_version(&Key::Bytes(pair), len, kind, version)
}

/// The rows an ordered fixture writes per id: id `i` gets
/// `rows_per_key.of(i)` rows whose order values are `start + step * j` for
/// `j` below that count, written in `kind`'s encoding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ordered {
    /// The order column's type.
    pub kind: OrderKind,
    /// How many rows each id has.
    pub rows_per_key: RowsPerKey,
    /// The first row's raw order value.
    pub start: i64,
    /// The raw distance between a key's consecutive rows; a negative step
    /// writes a key's rows descending, which a reader refuses as unsorted.
    pub step: i64,
}

/// How many rows each id of an ordered fixture has.
#[derive(Clone, Copy, Debug)]
pub enum RowsPerKey {
    /// The same count for every id.
    Each(u64),
    /// A count that varies by id.
    By(fn(u64) -> u64),
}

impl RowsPerKey {
    /// The rows id `id` has.
    pub fn of(self, id: u64) -> u64 {
        match self {
            Self::Each(rows) => rows,
            Self::By(rows) => rows(id),
        }
    }
}

impl PartialEq for RowsPerKey {
    /// Equal when both are the same count, or the same function by the
    /// address the two specifications were given: the same function value,
    /// as a copied specification holds.
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Each(one), Self::Each(other)) => one == other,
            (Self::By(one), Self::By(other)) => std::ptr::fn_addr_eq(*one, *other),
            (Self::Each(_), Self::By(_)) | (Self::By(_), Self::Each(_)) => false,
        }
    }
}

impl Eq for RowsPerKey {}

impl Ordered {
    /// The raw order values of `id`'s rows, in the order they are written;
    /// one past the largest long is `SpecFault::OrderOverflow`.
    pub fn orders_of(&self, id: u64) -> Result<Vec<i64>> {
        (0..self.rows_per_key.of(id))
            .map(|index| {
                i64::try_from(index)
                    .ok()
                    .and_then(|index| self.step.checked_mul(index))
                    .and_then(|offset| self.start.checked_add(offset))
                    .ok_or_else(|| SpecFault::OrderOverflow { id }.into())
            })
            .collect()
    }
}

/// What `Fixture::expected_scan` answers: each row's raw order value and one
/// value per requested column, `None` for a null.
pub type ScanRows = Vec<(i64, Vec<Option<Vec<u8>>>)>;

/// A row to write: the id its key derives from and, in an ordered fixture,
/// its raw order value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct RowId {
    /// The id the row's key derives from.
    pub(crate) id: u64,
    /// The row's raw order value; `None` in a key-value fixture.
    pub(crate) order: Option<i64>,
}

impl RowId {
    /// The one row of `id` in a key-value fixture.
    pub(crate) fn keyed(id: u64) -> Self {
        Self { id, order: None }
    }

    /// The row of `id` at raw order value `order`.
    pub(crate) fn at(id: u64, order: i64) -> Self {
        Self {
            id,
            order: Some(order),
        }
    }
}

/// The live rows of an ordered fixture: each (id, order) pair and the
/// version that wrote it. Empty for a key-value fixture.
#[derive(Clone, Debug, Default)]
pub(crate) struct LiveRows(BTreeMap<(u64, i64), u64>);

impl LiveRows {
    /// Records the row at (`id`, `order`) as live at `version`.
    pub(crate) fn insert(&mut self, id: u64, order: i64, version: u64) {
        self.0.insert((id, order), version);
    }

    /// Forgets the row at (`id`, `order`).
    pub(crate) fn remove(&mut self, id: u64, order: i64) {
        self.0.remove(&(id, order));
    }

    /// The version of the live row at (`id`, `order`), `FIRST_VERSION` for
    /// a pair that has none.
    pub(crate) fn version(&self, id: u64, order: i64) -> u64 {
        self.0.get(&(id, order)).copied().unwrap_or(FIRST_VERSION)
    }

    /// Whether `id` has a live row.
    pub(crate) fn holds(&self, id: u64) -> bool {
        self.of_id(id).next().is_some()
    }

    /// `id`'s live rows as (order, version), ascending by order.
    pub(crate) fn of_id(&self, id: u64) -> impl Iterator<Item = (i64, u64)> + '_ {
        self.0
            .range((id, i64::MIN)..=(id, i64::MAX))
            .map(|(&(_, order), &version)| (order, version))
    }

    /// Every live row as (id, order), ascending.
    pub(crate) fn pairs(&self) -> impl Iterator<Item = (u64, i64)> + '_ {
        self.0.keys().copied()
    }
}

/// What a Scan asks for, in raw order values (`order_value` turns a bound
/// into the kind's encoding).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TestScan {
    /// The direction the rows are returned in.
    pub direction: Direction,
    /// The most rows to return; zero reads up to `max_scan_rows`.
    pub limit: u32,
    /// Only rows whose order value is above this.
    pub after: Option<i64>,
    /// Only rows whose order value is below this.
    pub before: Option<i64>,
    /// The table's `max_scan_rows`, what a limit of zero reads.
    pub max_scan_rows: u32,
}

impl Default for TestScan {
    /// Every row, newest first, up to the default `max_scan_rows`.
    fn default() -> Self {
        Self {
            direction: Direction::NewestFirst,
            limit: 0,
            after: None,
            before: None,
            max_scan_rows: DEFAULT_MAX_SCAN_ROWS,
        }
    }
}

impl TestScan {
    /// The request as the core's bounds, each bound's raw order value
    /// encoded by `order`.
    pub fn bounds_by(&self, order: impl Fn(i64) -> OrderValue) -> ScanBounds {
        ScanBounds {
            direction: self.direction,
            limit: self.limit,
            after: self.after.map(&order),
            before: self.before.map(&order),
        }
    }

    /// The request as the core's bounds over order values of `kind`.
    pub fn bounds(&self, kind: OrderKind) -> ScanBounds {
        self.bounds_by(|raw| order_value(kind, raw))
    }
}

/// What a Scan answers over a key's live `rows`, as (order, payload): the
/// rows strictly between `scan.after` and `scan.before`, sorted by order
/// value, in the direction asked, cut to the limit, and whether live rows
/// remain inside the bounds past the last one returned.
pub fn scan_model<T>(
    rows: impl IntoIterator<Item = (i64, T)>,
    scan: &TestScan,
) -> (Vec<(i64, T)>, bool) {
    let mut inside: Vec<(i64, T)> = rows
        .into_iter()
        .filter(|(order, _)| {
            scan.after.is_none_or(|after| *order > after)
                && scan.before.is_none_or(|before| *order < before)
        })
        .collect();
    inside.sort_by_key(|(order, _)| *order);
    if scan.direction == Direction::NewestFirst {
        inside.reverse();
    }
    let limit = match scan.limit {
        0 => scan.max_scan_rows,
        limit => limit,
    };
    let limit = usize::try_from(limit).expect("a scan limit fits usize");
    let more = inside.len() > limit;
    inside.truncate(limit);
    (inside, more)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::ops::Range;

    use iceberg::Catalog;
    use iceberg::spec::{FormatVersion, SortDirection};
    use icetray_core::KeyType;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::metadata::ParquetMetaData;
    use tempfile::TempDir;

    use super::*;
    use crate::Nulls;
    use crate::error::{OracleFault, TestkitError};
    use crate::fixture::{
        DeleteForm, Fixture, FixtureSpec, KEY_FIELD_ID, NAMESPACE, SAMPLE, SECOND_VALUE_COLUMN,
        TWO_BUCKETS, local_catalog, write_fixture,
    };
    use crate::layout::{KEY_COLUMN, Layout, Statistics, VALUE_COLUMN};
    use crate::merge_oracle::{MergeOracle, RowCopy};

    /// Order values of the hand-built key: five rows.
    const ORDERS: [i64; 5] = [10, 20, 30, 40, 50];

    /// The third of `ORDERS`.
    const THIRD: i64 = 30;

    /// The second of `ORDERS`.
    const SECOND: i64 = 20;

    /// A limit of one row.
    const ONE: u32 = 1;

    /// A raw order value above every other the byte kinds are asked to hold
    /// in one test, below the width a fixed(4) holds.
    const LARGE_RAW: i64 = 0x0102_0304;

    /// A fixed width narrower than a long.
    const NARROW_WIDTH: u32 = 4;

    /// A fixed width wider than a long.
    const WIDE_WIDTH: u32 = 16;

    /// A padded string width far past a long's digits.
    const WIDE_DIGITS: u32 = 80;

    /// A raw value past 32 bits.
    const PAST_32_BITS: i64 = 1 << 33;

    /// The rows of the hand-built key, each carrying its own order as the
    /// payload.
    fn rows() -> Vec<(i64, i64)> {
        ORDERS.iter().map(|&order| (order, order)).collect()
    }

    /// The orders of the rows a model answer holds.
    fn orders_of(answer: &(Vec<(i64, i64)>, bool)) -> Vec<i64> {
        answer.0.iter().map(|(order, _)| *order).collect()
    }

    #[test]
    fn test_scan_model_applies_direction_bounds_and_limit() {
        let scan = TestScan {
            direction: Direction::NewestFirst,
            limit: ONE,
            before: Some(THIRD),
            ..TestScan::default()
        };
        let answer = scan_model(rows(), &scan);
        assert_eq!(orders_of(&answer), [SECOND]);
        assert!(answer.1, "the row at 10 remains inside the bounds");
    }

    #[test]
    fn test_scan_model_bounds_are_exclusive_on_both_sides() {
        let scan = TestScan {
            direction: Direction::OldestFirst,
            after: Some(SECOND),
            before: Some(ORDERS[3]),
            ..TestScan::default()
        };
        let answer = scan_model(rows(), &scan);
        assert_eq!(orders_of(&answer), [THIRD]);
        assert!(!answer.1);
    }

    #[test]
    fn test_scan_model_reads_both_directions_in_order() {
        let newest = scan_model(rows(), &TestScan::default());
        let oldest = scan_model(
            rows(),
            &TestScan {
                direction: Direction::OldestFirst,
                ..TestScan::default()
            },
        );
        let mut reversed = orders_of(&newest);
        reversed.reverse();
        assert_eq!(orders_of(&oldest), ORDERS);
        assert_eq!(reversed, ORDERS);
        assert!(!newest.1 && !oldest.1);
    }

    #[test]
    fn test_scan_model_sets_more_only_when_a_live_row_remains() {
        let all = ORDERS.len();
        let exact = TestScan {
            limit: u32::try_from(all).unwrap(),
            ..TestScan::default()
        };
        let one_short = TestScan {
            limit: u32::try_from(all - 1).unwrap(),
            ..TestScan::default()
        };
        assert!(!scan_model(rows(), &exact).1);
        let short = scan_model(rows(), &one_short);
        assert_eq!(short.0.len(), all - 1);
        assert!(short.1);
    }

    #[test]
    fn test_scan_model_limit_zero_reads_up_to_max_scan_rows() {
        let capped = TestScan {
            max_scan_rows: ONE,
            ..TestScan::default()
        };
        let answer = scan_model(rows(), &capped);
        assert_eq!(answer.0.len(), 1);
        assert!(answer.1);
        let explicit = TestScan {
            limit: u32::try_from(ORDERS.len()).unwrap(),
            max_scan_rows: ONE,
            ..TestScan::default()
        };
        assert_eq!(scan_model(rows(), &explicit).0.len(), ORDERS.len());
    }

    #[test]
    fn test_scan_model_of_no_rows_is_empty_and_has_no_more() {
        let answer = scan_model(Vec::<(i64, i64)>::new(), &TestScan::default());
        assert!(answer.0.is_empty());
        assert!(!answer.1);
    }

    #[test]
    fn test_encode_keeps_every_kind_in_numeric_order() {
        let kinds = [
            OrderKind::Long,
            OrderKind::Int,
            OrderKind::Date,
            OrderKind::Timestamp,
            OrderKind::TimestampTz,
            OrderKind::TimestampNs,
            OrderKind::TimestampTzNs,
            OrderKind::Str,
            OrderKind::PaddedStr(WIDE_DIGITS),
            OrderKind::PaddedStr(NARROW_WIDTH),
            OrderKind::Bin,
            OrderKind::Fixed(NARROW_WIDTH),
            OrderKind::Fixed(WIDE_WIDTH),
        ];
        let raws = [0, 1, 9, 10, 255, 256, 65_536, LARGE_RAW];
        for kind in kinds {
            let encoded: Vec<OrderValue> = raws.iter().map(|&raw| order_value(kind, raw)).collect();
            assert!(
                encoded.windows(2).all(|pair| pair[0] < pair[1]),
                "{kind:?}: {encoded:?}"
            );
        }
    }

    /// A padded string is as wide as its width asks, or as `Str`'s when
    /// that is wider, and the zeros past a value's digits are a prefix
    /// every value of the width shares.
    #[test]
    fn test_encode_pads_a_string_to_its_width() {
        let wide = |raw| {
            let encoded = order_value(OrderKind::PaddedStr(WIDE_DIGITS), raw);
            let OrderValue::Str(text) = encoded else {
                panic!("a string kind encodes {encoded:?}");
            };
            text
        };
        let (low, high) = (wide(0), wide(LARGE_RAW));
        assert_eq!(low.len(), width_of(WIDE_DIGITS));
        assert_eq!(high.len(), width_of(WIDE_DIGITS));
        let shared = width_of(WIDE_DIGITS) - ORDER_STRING_DIGITS;
        assert_eq!(low[..shared], high[..shared]);
        assert_eq!(
            order_value(OrderKind::PaddedStr(NARROW_WIDTH), LARGE_RAW),
            order_value(OrderKind::Str, LARGE_RAW)
        );
    }

    #[test]
    fn test_encode_refuses_what_a_kind_cannot_hold() {
        assert_eq!(OrderKind::Int.encode(PAST_32_BITS), None);
        assert_eq!(OrderKind::Date.encode(PAST_32_BITS), None);
        assert_eq!(
            OrderKind::Long.encode(PAST_32_BITS),
            Some(OrderValue::Long(PAST_32_BITS))
        );
        for kind in [
            OrderKind::Str,
            OrderKind::PaddedStr(WIDE_DIGITS),
            OrderKind::Bin,
            OrderKind::Fixed(WIDE_WIDTH),
        ] {
            assert_eq!(kind.encode(-1), None, "{kind:?}");
        }
        assert_eq!(OrderKind::Fixed(NARROW_WIDTH).encode(PAST_32_BITS), None);
        assert!(matches!(
            OrderKind::Int.array(&[PAST_32_BITS]),
            Err(crate::error::TestkitError::Spec(
                SpecFault::OrderUnencodable { raw, .. }
            )) if raw == PAST_32_BITS
        ));
    }

    #[test]
    fn test_orders_of_counts_the_rows_and_refuses_an_overflow() {
        let spec = ordered(OrderKind::Long, THREE_ROWS);
        let expected: Vec<i64> = (0..THREE).map(order_of).collect();
        assert_eq!(spec.orders_of(0).unwrap(), expected);
        let overflowing = Ordered {
            start: i64::MAX - 1,
            ..spec
        };
        assert!(matches!(
            overflowing.orders_of(OVERFLOWING_ID),
            Err(crate::error::TestkitError::Spec(SpecFault::OrderOverflow {
                id: OVERFLOWING_ID
            }))
        ));
    }

    #[test]
    fn test_row_value_is_a_function_of_key_order_and_version() {
        let len = 64;
        let key = Key::Long(3);
        let value =
            |key: &Key, order, version| row_value(key, order, len, ValueKind::Random, version);
        assert_eq!(value(&key, 1, 0), value(&key, 1, 0));
        assert_eq!(value(&key, 1, 0).len(), len);
        assert_ne!(value(&key, 1, 0), value(&key, 2, 0), "order");
        assert_ne!(value(&key, 1, 0), value(&key, 1, 1), "version");
        assert_ne!(value(&key, 1, 0), value(&Key::Long(6), 1, 0), "key");
        for kind in [ValueKind::Text, ValueKind::Protobuf] {
            assert_ne!(
                row_value(&key, 1, len, kind, 0),
                row_value(&key, 2, len, kind, 0),
                "{kind:?}"
            );
        }
    }

    /// Rows per key of the fixtures most tests write.
    const THREE: usize = 3;

    /// The first raw order value and the distance between a key's rows.
    const ORDER_START: i64 = 1_000;
    const ORDER_STEP: i64 = 10;

    /// Ids of the small ordered fixtures: forty keys over two buckets.
    const IDS: Range<u64> = 0..40;

    /// An id of `IDS`, and one the tests delete and update rows of.
    const AN_ID: u64 = 7;

    /// The raw order value of the `index`-th row of a key.
    fn order_of(index: usize) -> i64 {
        ORDER_START + ORDER_STEP * i64::try_from(index).unwrap()
    }

    /// `THREE` rows for every id.
    const THREE_ROWS: RowsPerKey = RowsPerKey::Each(THREE as u64);

    /// Rows per key of the row group test: more than a row group holds.
    const SIX: usize = 6;

    /// Rows per row group of the row group test.
    const ROW_GROUP_ROWS: usize = 4;

    /// `SIX` rows for every id.
    const SIX_ROWS: RowsPerKey = RowsPerKey::Each(SIX as u64);

    /// An id whose order values overflow.
    const OVERFLOWING_ID: u64 = 5;

    /// Zero to four rows per key, so some keys have none.
    fn up_to_four(id: u64) -> u64 {
        id % 5
    }

    /// `rows_per_key` rows of kind `kind` for every id.
    fn ordered(kind: OrderKind, rows_per_key: RowsPerKey) -> Ordered {
        Ordered {
            kind,
            rows_per_key,
            start: ORDER_START,
            step: ORDER_STEP,
        }
    }

    /// A small ordered spec: `IDS` over two buckets.
    fn spec(name: &str, kind: OrderKind, rows_per_key: RowsPerKey) -> FixtureSpec {
        FixtureSpec {
            ids: IDS,
            buckets: TWO_BUCKETS,
            ..FixtureSpec::ordered(name, KeyType::Long, ordered(kind, rows_per_key))
        }
    }

    /// `spec` written to a fresh catalog; the directory keeps its files.
    async fn written(spec: FixtureSpec) -> Result<(TempDir, Arc<dyn Catalog>, Fixture)> {
        let (dir, catalog) = local_catalog().await?;
        let fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
        Ok((dir, catalog, fixture))
    }

    /// The footer of the Parquet file at `path`.
    async fn footer(fixture: &Fixture, path: &str) -> Result<Arc<ParquetMetaData>> {
        let bytes = fixture.file_io().new_input(path)?.read().await?;
        Ok(Arc::clone(
            ParquetRecordBatchReaderBuilder::try_new(bytes)?.metadata(),
        ))
    }

    /// The live rows of `key` the oracle reads from the files, as raw order
    /// values (the fixtures here order by long) and the value column.
    fn oracle_rows(oracle: &MergeOracle, key: &Key) -> Vec<(i64, Vec<Option<Vec<u8>>>)> {
        oracle
            .copies(key)
            .iter()
            .filter(|copy| !copy.deleted)
            .map(|copy| match copy.order {
                Some(OrderValue::Long(order)) => (order, vec![copy.value.clone()]),
                Some(OrderValue::Str(_) | OrderValue::Bytes(_)) | None => {
                    panic!("a long order value: {copy:?}")
                }
            })
            .collect()
    }

    #[tokio::test]
    async fn test_write_fixture_writes_each_key_s_rows_sorted_by_order() -> Result<()> {
        let (_dir, _catalog, fixture) =
            written(spec("events", OrderKind::Long, THREE_ROWS)).await?;
        let oracle = MergeOracle::read(&fixture).await?;
        let key = fixture.key(SAMPLE[0]);
        let copies = oracle.copies(&key);
        assert_eq!(copies.len(), THREE);
        assert!(copies.windows(2).all(|pair| pair[0].order < pair[1].order));
        let orders: Vec<Option<OrderValue>> =
            copies.iter().map(|copy| copy.order.clone()).collect();
        let expected: Vec<Option<OrderValue>> = (0..THREE)
            .map(|index| Some(OrderValue::Long(order_of(index))))
            .collect();
        assert_eq!(orders, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_write_fixture_writes_every_file_sorted_by_key_then_order() -> Result<()> {
        let (_dir, _catalog, fixture) =
            written(spec("sorted", OrderKind::Long, RowsPerKey::By(up_to_four))).await?;
        let oracle = MergeOracle::read(&fixture).await?;
        let mut by_file: BTreeMap<String, Vec<(u64, Key, Option<OrderValue>)>> = BTreeMap::new();
        for key in oracle.keys() {
            for copy in oracle.copies(key) {
                by_file.entry(copy.path.clone()).or_default().push((
                    copy.position,
                    key.clone(),
                    copy.order.clone(),
                ));
            }
        }
        assert_eq!(by_file.len(), fixture.file_count());
        assert!(fixture.file_count() >= TWO_BUCKETS as usize);
        for (path, mut rows) in by_file {
            rows.sort_by_key(|(position, _, _)| *position);
            let pairs: Vec<_> = rows.iter().map(|(_, key, order)| (key, order)).collect();
            assert!(
                pairs.windows(2).all(|pair| pair[0] < pair[1]),
                "{path} strictly increases by key and then order"
            );
        }
        let held: Vec<u64> = fixture.ids().collect();
        let expected: Vec<u64> = IDS.filter(|&id| up_to_four(id) > 0).collect();
        assert_eq!(held, expected, "an id with no rows is not held");
        Ok(())
    }

    #[tokio::test]
    async fn test_write_fixture_round_trips_every_order_kind() -> Result<()> {
        let kinds = [
            OrderKind::Long,
            OrderKind::Int,
            OrderKind::Date,
            OrderKind::Timestamp,
            OrderKind::TimestampTz,
            OrderKind::TimestampNs,
            OrderKind::TimestampTzNs,
            OrderKind::Str,
            OrderKind::Bin,
            OrderKind::Fixed(NARROW_WIDTH),
            OrderKind::Fixed(WIDE_WIDTH),
        ];
        for (at, kind) in kinds.into_iter().enumerate() {
            let spec = FixtureSpec {
                format_version: FormatVersion::V3,
                ..spec(&format!("kind_{at}"), kind, THREE_ROWS)
            };
            let (_dir, _catalog, fixture) = written(spec).await?;
            let schema = fixture.metadata().current_schema();
            let field = schema
                .field_by_name(ORDER_COLUMN)
                .expect("the order column");
            assert_eq!(Some(field.id), fixture.order_field_id());
            assert_eq!(
                *field.field_type,
                iceberg::spec::Type::Primitive(kind.primitive())
            );
            let oracle = MergeOracle::read(&fixture).await?;
            for id in IDS {
                let read: Vec<Option<OrderValue>> = oracle
                    .copies(&fixture.key(id))
                    .iter()
                    .map(|copy| copy.order.clone())
                    .collect();
                let expected: Vec<Option<OrderValue>> = (0..THREE)
                    .map(|index| Some(order_value(kind, order_of(index))))
                    .collect();
                assert_eq!(read, expected, "{kind:?} id {id}");
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_write_fixture_refuses_an_order_value_the_kind_cannot_hold() -> Result<()> {
        let (_dir, catalog) = local_catalog().await?;
        let spec = FixtureSpec {
            ordered: Some(Ordered {
                start: -1,
                ..ordered(OrderKind::Str, THREE_ROWS)
            }),
            ..spec("negative", OrderKind::Str, THREE_ROWS)
        };
        let refused = write_fixture(catalog.as_ref(), NAMESPACE, spec).await;
        assert!(
            matches!(
                refused,
                Err(TestkitError::Spec(SpecFault::OrderUnencodable {
                    raw: -1,
                    ..
                }))
            ),
            "{refused:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_write_fixture_declares_the_order_column_and_its_sort() -> Result<()> {
        let spec = FixtureSpec {
            second_value: true,
            ..spec("declared", OrderKind::Long, THREE_ROWS)
        };
        let (_dir, _catalog, fixture) = written(spec).await?;
        let metadata = fixture.metadata();
        let names: Vec<&str> = metadata
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|field| field.name.as_str())
            .collect();
        assert_eq!(
            names,
            [KEY_COLUMN, VALUE_COLUMN, SECOND_VALUE_COLUMN, ORDER_COLUMN]
        );
        assert_eq!(fixture.order_field_id(), Some(ORDER_FIELD_ID));
        let sorted: Vec<(i32, SortDirection)> = metadata
            .default_sort_order()
            .fields
            .iter()
            .map(|field| (field.source_id, field.direction))
            .collect();
        assert_eq!(
            sorted,
            [
                (KEY_FIELD_ID, SortDirection::Ascending),
                (ORDER_FIELD_ID, SortDirection::Ascending)
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_write_fixture_leaves_a_key_value_table_as_it_was() -> Result<()> {
        let spec = FixtureSpec {
            ids: IDS,
            buckets: TWO_BUCKETS,
            second_value: true,
            ..FixtureSpec::small("key_value", KeyType::Long)
        };
        assert_eq!(spec.ordered, None);
        let (_dir, _catalog, fixture) = written(spec).await?;
        let metadata = fixture.metadata();
        let names: Vec<&str> = metadata
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|field| field.name.as_str())
            .collect();
        assert_eq!(names, [KEY_COLUMN, VALUE_COLUMN, SECOND_VALUE_COLUMN]);
        assert_eq!(metadata.default_sort_order().fields.len(), 1);
        let oracle = MergeOracle::read(&fixture).await?;
        for key in oracle.keys() {
            assert!(oracle.copies(key).iter().all(|copy| copy.order.is_none()));
        }
        let file = fixture.files()[0].file_path().to_string();
        let sorting = footer(&fixture, &file)
            .await?
            .row_group(0)
            .sorting_columns()
            .cloned();
        assert_eq!(sorting.map(|columns| columns.len()), Some(1));
        Ok(())
    }

    #[tokio::test]
    async fn test_write_fixture_splits_a_run_across_row_groups() -> Result<()> {
        let spec = FixtureSpec {
            layout: Layout {
                row_group_rows: Some(ROW_GROUP_ROWS),
                ..Layout::contract()
            },
            ..spec("groups", OrderKind::Long, SIX_ROWS)
        };
        let (_dir, _catalog, fixture) = written(spec).await?;
        let oracle = MergeOracle::read(&fixture).await?;
        let mut straddling = 0;
        for key in oracle.keys() {
            let groups: BTreeSet<u64> = oracle
                .copies(key)
                .iter()
                .map(|copy| copy.position / u64::try_from(ROW_GROUP_ROWS).unwrap())
                .collect();
            assert_eq!(oracle.copies(key).len(), SIX);
            straddling += usize::from(groups.len() > 1);
        }
        assert!(straddling > 0, "some run spans two row groups");
        for file in fixture.files() {
            let groups = footer(&fixture, file.file_path()).await?.num_row_groups();
            assert!(groups > 1, "{} has {groups} row groups", file.file_path());
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_write_fixture_applies_the_order_column_layout() -> Result<()> {
        let layout = Layout {
            order_dictionary: true,
            order_statistics: Statistics::None,
            ..Layout::contract()
        };
        let (_dir, _catalog, fixture) = written(FixtureSpec {
            layout,
            ..spec("layout", OrderKind::Long, THREE_ROWS)
        })
        .await?;
        let file = fixture.files()[0].file_path().to_string();
        let footer = footer(&fixture, &file).await?;
        let group = footer.row_group(0);
        let leaf = |name: &str| {
            group
                .columns()
                .iter()
                .position(|column| column.column_path().string() == name)
                .expect("a leaf column")
        };
        let (key, order) = (
            group.column(leaf(KEY_COLUMN)),
            group.column(leaf(ORDER_COLUMN)),
        );
        assert!(
            order.dictionary_page_offset().is_some(),
            "dictionary-encoded"
        );
        assert!(key.dictionary_page_offset().is_none());
        assert!(
            order.column_index_offset().is_none(),
            "no order column index"
        );
        assert!(key.column_index_offset().is_some());
        assert!(order.offset_index_offset().is_some());
        let sorted_by: Vec<String> = group
            .sorting_columns()
            .expect("the file declares its sort")
            .iter()
            .map(|sorting| {
                let at = usize::try_from(sorting.column_idx).unwrap();
                group.column(at).column_path().string()
            })
            .collect();
        assert_eq!(sorted_by, [KEY_COLUMN, ORDER_COLUMN]);
        Ok(())
    }

    #[tokio::test]
    async fn test_read_admits_a_key_repeated_and_refuses_a_live_pair_repeated() -> Result<()> {
        let (_dir, catalog, mut fixture) =
            written(spec("pairs", OrderKind::Long, THREE_ROWS)).await?;
        let key = fixture.key(AN_ID);
        let new_order = order_of(THREE);
        fixture
            .append_ordered(catalog.as_ref(), &[(AN_ID, new_order)], "later")
            .await?;
        let oracle = MergeOracle::read(&fixture).await?;
        assert_eq!(oracle.copies(&key).len(), THREE + 1);
        let again = fixture
            .append_ordered(catalog.as_ref(), &[(AN_ID, new_order)], "again")
            .await;
        assert!(
            again.is_ok(),
            "the writer commits what the oracle then refuses"
        );
        assert!(matches!(
            MergeOracle::read(&fixture).await,
            Err(TestkitError::Oracle(OracleFault::LiveDuplicate { key: found, order: Some(OrderValue::Long(order)), .. }))
                if found == key && order == new_order
        ));
        Ok(())
    }

    #[tokio::test]
    async fn test_read_admits_a_pair_whose_older_copy_is_deleted() -> Result<()> {
        let (_dir, _catalog, mut fixture) =
            written(spec("updated", OrderKind::Long, THREE_ROWS)).await?;
        let key = fixture.key(AN_ID);
        fixture
            .update_rows(&[(AN_ID, order_of(1))], DeleteForm::Vectors, "update")
            .await?;
        let oracle = MergeOracle::read(&fixture).await?;
        let copies = oracle.copies(&key);
        assert_eq!(
            copies.len(),
            THREE + 1,
            "the deleted copy and its replacement"
        );
        let at_pair: Vec<&RowCopy> = copies
            .iter()
            .filter(|copy| copy.order == Some(OrderValue::Long(order_of(1))))
            .collect();
        assert_eq!(at_pair.len(), 2);
        assert_eq!(at_pair.iter().filter(|copy| copy.deleted).count(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_expected_scan_agrees_with_the_files_after_every_kind_of_change() -> Result<()> {
        let forms = [
            DeleteForm::Vectors,
            DeleteForm::ScopedPositions,
            DeleteForm::Positions,
        ];
        for (at, form) in forms.into_iter().enumerate() {
            let spec = FixtureSpec {
                second_value: true,
                nulls: Nulls::EveryNth(NULL_EVERY),
                ..spec(
                    &format!("changes_{at}"),
                    OrderKind::Long,
                    RowsPerKey::By(up_to_four),
                )
            };
            let (_dir, catalog, mut fixture) = written(spec).await?;
            let [appended, deleted_from, updated, other_updated, emptied] = CHANGED_IDS;
            let next_order = order_of(MORE_ROWS);
            fixture
                .append_ordered(
                    catalog.as_ref(),
                    &[(appended, next_order), (deleted_from, next_order)],
                    "appended",
                )
                .await?;
            fixture
                .delete_rows(
                    &[(appended, order_of(0)), (deleted_from, order_of(1))],
                    form,
                    "gone",
                )
                .await?;
            fixture
                .update_rows(
                    &[(updated, order_of(0)), (other_updated, order_of(2))],
                    form,
                    "rewritten",
                )
                .await?;
            fixture.delete(&[emptied], form, "whole").await?;
            assert!(!fixture.ids().any(|id| id == emptied));
            let oracle = MergeOracle::read(&fixture).await?;
            assert!(oracle.deleted_rows() > 0, "{form:?} deleted rows");
            check_against_the_files(&fixture).await?;
            fixture.compact("compacted").await?;
            check_against_the_files(&fixture).await?;
            let oracle = MergeOracle::read(&fixture).await?;
            assert_eq!(
                oracle.deleted_rows(),
                0,
                "a compaction drops what was deleted"
            );
        }
        Ok(())
    }

    /// Every key's model answer, under a spread of bounds, directions, and
    /// limits, is the answer `scan_model` gives the rows the files hold, and
    /// the bookkeeping's rows are those rows.
    async fn check_against_the_files(fixture: &Fixture) -> Result<()> {
        let oracle = MergeOracle::read(fixture).await?;
        let mut checked_rows = 0;
        for id in IDS {
            let key = fixture.key(id);
            let read = oracle_rows(&oracle, &key);
            for (direction, limit, after, before) in scans(&read) {
                let scan = TestScan {
                    direction,
                    limit,
                    after,
                    before,
                    ..TestScan::default()
                };
                let (rows, more) = fixture.expected_scan(&key, &[VALUE_COLUMN], &scan);
                let from_files = scan_model(read.clone(), &scan);
                assert_eq!((rows.clone(), more), from_files, "id {id} {scan:?}");
                checked_rows += rows.len();
            }
            let (both, _) = fixture.expected_scan(
                &key,
                &[VALUE_COLUMN, SECOND_VALUE_COLUMN],
                &TestScan::default(),
            );
            for (_, values) in both {
                assert_eq!(
                    values[1],
                    values[0]
                        .clone()
                        .map(|value| value.into_iter().rev().collect()),
                    "the second column is the first reversed"
                );
            }
        }
        assert!(checked_rows > 0, "the scans compared some rows");
        Ok(())
    }

    /// The scans a key's `rows` are checked under: both directions, with and
    /// without a limit, with bounds at and between its order values.
    fn scans(
        rows: &[(i64, Vec<Option<Vec<u8>>>)],
    ) -> Vec<(Direction, u32, Option<i64>, Option<i64>)> {
        let orders: Vec<i64> = rows.iter().map(|(order, _)| *order).collect();
        let first = orders.first().copied().unwrap_or(ORDER_START);
        let last = orders.last().copied().unwrap_or(ORDER_START);
        let mut scans = Vec::new();
        for direction in [Direction::NewestFirst, Direction::OldestFirst] {
            for limit in [0, ONE, ONE + 1] {
                for (after, before) in [
                    (None, None),
                    (Some(first), None),
                    (None, Some(last)),
                    (Some(first - 1), Some(last + 1)),
                    (Some(first), Some(last)),
                ] {
                    scans.push((direction, limit, after, before));
                }
            }
        }
        scans
    }

    /// Ids of `up_to_four` with at least three rows, which the change test
    /// appends to, deletes from, updates, and empties: one each, then a
    /// second to update.
    const CHANGED_IDS: [u64; 5] = [8, 9, 13, 14, 18];

    /// Every this-many-th id has a null value.
    const NULL_EVERY: u64 = 3;

    /// The row count of the longest key `up_to_four` gives, which the
    /// appended rows follow.
    const MORE_ROWS: usize = 4;

    #[tokio::test]
    async fn test_delete_rows_refuses_a_pair_that_is_not_live() -> Result<()> {
        let (_dir, _catalog, mut fixture) =
            written(spec("not_live", OrderKind::Long, THREE_ROWS)).await?;
        let missing = order_of(THREE);
        let refused = fixture
            .delete_rows(&[(AN_ID, missing)], DeleteForm::Vectors, "gone")
            .await;
        assert!(
            matches!(
                refused,
                Err(TestkitError::Spec(SpecFault::RowNotLive { id: AN_ID, order })) if order == missing
            ),
            "{refused:?}"
        );
        let refused = fixture
            .delete(&[IDS.end], DeleteForm::Vectors, "gone")
            .await;
        assert!(
            matches!(refused, Err(TestkitError::Spec(SpecFault::IdNotLive { id })) if id == IDS.end),
            "{refused:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_ordered_calls_refuse_a_key_value_fixture() -> Result<()> {
        let spec = FixtureSpec {
            ids: IDS,
            buckets: TWO_BUCKETS,
            ..FixtureSpec::small("plain", KeyType::Long)
        };
        let (_dir, catalog, mut fixture) = written(spec).await?;
        let rows = [(AN_ID, ORDER_START)];
        let calls = [
            fixture.append_ordered(catalog.as_ref(), &rows, "x").await,
            fixture.delete_rows(&rows, DeleteForm::Vectors, "x").await,
            fixture.update_rows(&rows, DeleteForm::Vectors, "x").await,
        ];
        for call in calls {
            assert!(
                matches!(call, Err(TestkitError::Spec(SpecFault::NotOrdered))),
                "{call:?}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_move_ref_returns_an_ordered_fixture_to_the_rows_it_listed() -> Result<()> {
        let (_dir, _catalog, mut fixture) =
            written(spec("moved", OrderKind::Long, THREE_ROWS)).await?;
        let key = fixture.key(AN_ID);
        let before = fixture.snapshot_id().unwrap();
        let all = fixture.expected_scan(&key, &[VALUE_COLUMN], &TestScan::default());
        fixture
            .update_rows(&[(AN_ID, order_of(0))], DeleteForm::Vectors, "update")
            .await?;
        fixture
            .delete_rows(&[(AN_ID, order_of(1))], DeleteForm::Vectors, "delete")
            .await?;
        let changed = fixture.expected_scan(&key, &[VALUE_COLUMN], &TestScan::default());
        assert_eq!(changed.0.len(), THREE - 1);
        assert_ne!(changed, all);
        fixture.move_ref(before).await?;
        assert_eq!(
            fixture.expected_scan(&key, &[VALUE_COLUMN], &TestScan::default()),
            all
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_expected_scan_of_a_key_the_fixture_does_not_hold_has_no_rows() -> Result<()> {
        let (_dir, _catalog, fixture) =
            written(spec("absent", OrderKind::Long, THREE_ROWS)).await?;
        for key in [fixture.absent_near(AN_ID), fixture.key(IDS.end)] {
            assert_eq!(
                fixture.expected_scan(&key, &[VALUE_COLUMN], &TestScan::default()),
                (Vec::new(), false)
            );
        }
        let held =
            fixture.expected_scan(&fixture.key(AN_ID), &[VALUE_COLUMN], &TestScan::default());
        assert_eq!(held.0.len(), THREE);
        Ok(())
    }
}
