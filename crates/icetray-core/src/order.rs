//! The order column of an ordered table (RFD 025): the Iceberg types it
//! takes, the Parquet types a file may store it in, the values a Scan
//! compares, and what a Scan asks for. A table entry that sets
//! `order_column` is an ordered table; every other is a key-value table.

use std::fmt;

use iceberg::spec::PrimitiveType;
use parquet::basic::{ConvertedType, LogicalType, TimeUnit, Type as PhysicalType};
use parquet::schema::types::ColumnDescriptor;

use crate::key::write_hex;
use crate::time::EpochUnit;

/// The Iceberg type of an ordered table's order column (RFD 025, Order
/// values). Integer and time types compare as signed 64-bit integers in
/// Iceberg's unit (days, microseconds, nanoseconds); string, binary, and
/// fixed compare as their bytes. Display gives the Iceberg type name, as
/// iceberg's `PrimitiveType` displays it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum OrderType {
    /// Iceberg `int`.
    Int,
    /// Iceberg `long`.
    Long,
    /// Iceberg `date`, in days.
    Date,
    /// Iceberg `timestamp`, in microseconds.
    Timestamp,
    /// Iceberg `timestamptz`, in microseconds.
    TimestampTz,
    /// Iceberg `timestamp_ns`, in nanoseconds.
    TimestampNs,
    /// Iceberg `timestamptz_ns`, in nanoseconds.
    TimestampTzNs,
    /// Iceberg `string`.
    String,
    /// Iceberg `binary`.
    Binary,
    /// Iceberg `fixed` of this width.
    Fixed(u32),
}

/// Which `OrderValue` variant an order type's values travel as. Display
/// gives its wire name, as the protobuf `OrderValue`'s oneof names it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum OrderVariant {
    /// `OrderValue::Long`: integer and time types.
    Long,
    /// `OrderValue::Str`: string.
    Str,
    /// `OrderValue::Bytes`: binary and fixed.
    Bin,
}

impl fmt::Display for OrderVariant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Long => "long",
            Self::Str => "str",
            Self::Bin => "bin",
        })
    }
}

/// Whether a file's order column holds the values its Iceberg type means
/// (`OrderType::admits`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Admit {
    /// The file stores the column in the Parquet type and annotation
    /// Iceberg writes for the type.
    Exact,
    /// A `long` column the file stores as a plain `INT32`: written before
    /// Iceberg promoted the column from `int`, its values read as longs.
    PromotedFromInt,
    /// Any other Parquet type: INT96, a timestamp of another unit, a
    /// `date` under `timestamp`, a byte array under an integer type.
    Mismatch,
}

impl OrderType {
    /// The order type an Iceberg primitive maps to: `int`, `long`, `date`,
    /// the four timestamp types, `string`, `binary`, and a `fixed` whose
    /// width a Parquet `FIXED_LEN_BYTE_ARRAY` can hold, whose type length
    /// is an `i32`. Every other primitive is `None`, which the caller that
    /// owns the table name refuses as `RefusalReason::OrderTypeUnsupported`.
    pub fn from_iceberg(t: &PrimitiveType) -> Option<Self> {
        match t {
            PrimitiveType::Int => Some(Self::Int),
            PrimitiveType::Long => Some(Self::Long),
            PrimitiveType::Date => Some(Self::Date),
            PrimitiveType::Timestamp => Some(Self::Timestamp),
            PrimitiveType::Timestamptz => Some(Self::TimestampTz),
            PrimitiveType::TimestampNs => Some(Self::TimestampNs),
            PrimitiveType::TimestamptzNs => Some(Self::TimestampTzNs),
            PrimitiveType::String => Some(Self::String),
            PrimitiveType::Binary => Some(Self::Binary),
            PrimitiveType::Fixed(width) => i32::try_from(*width)
                .ok()
                .and_then(|width| u32::try_from(width).ok())
                .map(Self::Fixed),
            PrimitiveType::Boolean
            | PrimitiveType::Float
            | PrimitiveType::Double
            | PrimitiveType::Decimal { .. }
            | PrimitiveType::Time
            | PrimitiveType::Uuid => None,
        }
    }

    /// Whether `column`, a file's order column, is stored as the Parquet
    /// physical type and logical annotation Iceberg writes for this type:
    /// `int` as a plain signed `INT32`, `long` as a plain signed `INT64`
    /// or, written before a promotion from `int`, `INT32`
    /// (`Admit::PromotedFromInt`), `date` as `INT32` annotated `DATE`, the
    /// timestamps as `INT64` annotated as timestamps of their unit,
    /// microseconds or nanoseconds, `string` and `binary` as a
    /// `BYTE_ARRAY`, and `fixed` as a `FIXED_LEN_BYTE_ARRAY` of its width,
    /// at least one byte, on which parquet's fixed-width decoder insists.
    /// An annotation is the logical type, or for a file that records only
    /// the legacy converted type, that. A timestamp's UTC flag is not
    /// checked: it changes neither the stored value nor its order. A unit
    /// is, since a value in another unit compares wrong against every
    /// other file's: a timestamp without an annotation, in milliseconds, or
    /// as INT96, is `Admit::Mismatch`, as is `date` under `timestamp`.
    pub(crate) fn admits(self, column: &ColumnDescriptor) -> Admit {
        let physical = column.physical_type();
        let annotation = Annotation::of(column);
        let exact = |admitted: bool| {
            if admitted {
                Admit::Exact
            } else {
                Admit::Mismatch
            }
        };
        let plain = annotation == Annotation::Plain;
        match self {
            Self::Long if physical == PhysicalType::INT32 && plain => Admit::PromotedFromInt,
            Self::Long => exact(physical == PhysicalType::INT64 && plain),
            Self::Int => exact(physical == PhysicalType::INT32 && plain),
            Self::Date => exact(physical == PhysicalType::INT32 && annotation == Annotation::Date),
            Self::Timestamp | Self::TimestampTz => exact(
                physical == PhysicalType::INT64
                    && annotation == Annotation::Timestamp(TimeUnit::MICROS),
            ),
            Self::TimestampNs | Self::TimestampTzNs => exact(
                physical == PhysicalType::INT64
                    && annotation == Annotation::Timestamp(TimeUnit::NANOS),
            ),
            Self::String | Self::Binary => exact(physical == PhysicalType::BYTE_ARRAY),
            Self::Fixed(width) => exact(
                physical == PhysicalType::FIXED_LEN_BYTE_ARRAY
                    && width > 0
                    && u32::try_from(column.type_length()).is_ok_and(|stored| stored == width),
            ),
        }
    }

    /// The `OrderValue` variant this type's values travel as.
    pub fn variant(&self) -> OrderVariant {
        match self {
            Self::Int
            | Self::Long
            | Self::Date
            | Self::Timestamp
            | Self::TimestampTz
            | Self::TimestampNs
            | Self::TimestampTzNs => OrderVariant::Long,
            Self::String => OrderVariant::Str,
            Self::Binary | Self::Fixed(_) => OrderVariant::Bin,
        }
    }

    /// The unit a date or timestamp counts from the epoch in, whose ISO
    /// 8601 text `time::epoch_count` reads; `None` for an integer, string,
    /// binary, or fixed type, which takes no ISO text.
    pub fn epoch_unit(self) -> Option<EpochUnit> {
        match self {
            Self::Date => Some(EpochUnit::Days),
            Self::Timestamp | Self::TimestampTz => Some(EpochUnit::Microseconds),
            Self::TimestampNs | Self::TimestampTzNs => Some(EpochUnit::Nanoseconds),
            Self::Int | Self::Long | Self::String | Self::Binary | Self::Fixed(_) => None,
        }
    }

    /// The width of every value of a `fixed` order column; `None` for
    /// every other type.
    pub(crate) fn width(self) -> Option<u64> {
        match self {
            Self::Fixed(width) => Some(u64::from(width)),
            Self::Int
            | Self::Long
            | Self::Date
            | Self::Timestamp
            | Self::TimestampTz
            | Self::TimestampNs
            | Self::TimestampTzNs
            | Self::String
            | Self::Binary => None,
        }
    }
}

/// What an integer column's annotation says its values are, as far as an
/// order type cares (`OrderType::admits`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Annotation {
    /// None, or a signed integer of the physical type's width.
    Plain,
    /// Days since the epoch.
    Date,
    /// A timestamp in this unit.
    Timestamp(TimeUnit),
    /// Anything else: an unsigned or narrower integer, a time of day, a
    /// decimal, or an annotation of a byte array.
    Other,
}

impl Annotation {
    /// The annotation of `column`: its logical type, or its converted type
    /// when it records none, as a file written before logical types does.
    fn of(column: &ColumnDescriptor) -> Self {
        let width = match column.physical_type() {
            PhysicalType::INT32 => Some(INT32_BITS),
            PhysicalType::INT64 => Some(INT64_BITS),
            PhysicalType::BOOLEAN
            | PhysicalType::INT96
            | PhysicalType::FLOAT
            | PhysicalType::DOUBLE
            | PhysicalType::BYTE_ARRAY
            | PhysicalType::FIXED_LEN_BYTE_ARRAY => None,
        };
        match column.logical_type_ref() {
            Some(LogicalType::Integer {
                bit_width,
                is_signed: true,
            }) if Some(*bit_width) == width => Self::Plain,
            Some(LogicalType::Date) => Self::Date,
            Some(LogicalType::Timestamp { unit, .. }) => Self::Timestamp(*unit),
            Some(_) => Self::Other,
            None => match column.converted_type() {
                ConvertedType::NONE => Self::Plain,
                ConvertedType::INT_32 if width == Some(INT32_BITS) => Self::Plain,
                ConvertedType::INT_64 if width == Some(INT64_BITS) => Self::Plain,
                ConvertedType::DATE => Self::Date,
                ConvertedType::TIMESTAMP_MILLIS => Self::Timestamp(TimeUnit::MILLIS),
                ConvertedType::TIMESTAMP_MICROS => Self::Timestamp(TimeUnit::MICROS),
                ConvertedType::UTF8
                | ConvertedType::MAP
                | ConvertedType::MAP_KEY_VALUE
                | ConvertedType::LIST
                | ConvertedType::ENUM
                | ConvertedType::DECIMAL
                | ConvertedType::TIME_MILLIS
                | ConvertedType::TIME_MICROS
                | ConvertedType::UINT_8
                | ConvertedType::UINT_16
                | ConvertedType::UINT_32
                | ConvertedType::UINT_64
                | ConvertedType::INT_8
                | ConvertedType::INT_16
                | ConvertedType::INT_32
                | ConvertedType::INT_64
                | ConvertedType::JSON
                | ConvertedType::BSON
                | ConvertedType::INTERVAL => Self::Other,
            },
        }
    }
}

/// The bits of a Parquet `INT32` and `INT64`, as an integer annotation
/// states its width.
const INT32_BITS: i8 = 32;
const INT64_BITS: i8 = 64;

/// The Parquet type `column` is stored as, for a refusal a person reads:
/// the physical type and its annotation, logical or converted.
pub(crate) fn stored_type(column: &ColumnDescriptor) -> String {
    let physical = column.physical_type();
    match (column.logical_type_ref(), column.converted_type()) {
        (Some(logical), _) => format!("{physical} {logical:?}"),
        (None, ConvertedType::NONE) => physical.to_string(),
        (None, converted) => format!("{physical} {converted}"),
    }
}

impl fmt::Display for OrderType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Int => f.write_str("int"),
            Self::Long => f.write_str("long"),
            Self::Date => f.write_str("date"),
            Self::Timestamp => f.write_str("timestamp"),
            Self::TimestampTz => f.write_str("timestamptz"),
            Self::TimestampNs => f.write_str("timestamp_ns"),
            Self::TimestampTzNs => f.write_str("timestamptz_ns"),
            Self::String => f.write_str("string"),
            Self::Binary => f.write_str("binary"),
            Self::Fixed(width) => write!(f, "fixed({width})"),
        }
    }
}

/// An order value as a Scan compares it (RFD 025): `Long` numerically as
/// a signed 64-bit integer, `Str` as its UTF-8 bytes, `Bytes` bytewise
/// unsigned, which is the derived order of each variant. The derived order
/// across variants carries no meaning: one table's values are all of its
/// order type's variant.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum OrderValue {
    /// An `int`, `long`, `date`, or timestamp, in Iceberg's unit.
    Long(i64),
    /// A `string`.
    Str(String),
    /// A `binary` or `fixed`.
    Bytes(Vec<u8>),
}

impl OrderValue {
    /// The variant this value travels as.
    pub fn variant(&self) -> OrderVariant {
        match self {
            Self::Long(_) => OrderVariant::Long,
            Self::Str(_) => OrderVariant::Str,
            Self::Bytes(_) => OrderVariant::Bin,
        }
    }

    /// The least order value above this one, in the order a Scan compares
    /// them: one more for a long, and for a string or bytes the value with
    /// one zero byte appended, which sorts after it and before every other
    /// value above it, whatever its length, so of a `fixed` type it is one
    /// byte longer than the type's width. `None` for `i64::MAX`, which no
    /// long lies above.
    #[must_use]
    pub fn successor(&self) -> Option<Self> {
        match self {
            Self::Long(value) => value.checked_add(1).map(Self::Long),
            Self::Str(text) => {
                let mut next = String::with_capacity(text.len() + 1);
                next.push_str(text);
                next.push('\0');
                Some(Self::Str(next))
            }
            Self::Bytes(bytes) => {
                let mut next = Vec::with_capacity(bytes.len() + 1);
                next.extend_from_slice(bytes);
                next.push(0);
                Some(Self::Bytes(next))
            }
        }
    }
}

impl fmt::Display for OrderValue {
    /// A long as the integer, a string quoted with `Debug` escaping, bytes
    /// as `0x`-prefixed lowercase hex, as `Key` displays its variants.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Long(value) => write!(f, "{value}"),
            Self::Str(text) => write!(f, "{text:?}"),
            Self::Bytes(bytes) => {
                f.write_str("0x")?;
                write_hex(f, bytes)
            }
        }
    }
}

impl serde::Serialize for OrderType {
    /// The Iceberg type name `Display` gives, as the doctor's lines carry
    /// it.
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl serde::Serialize for OrderValue {
    /// A long as a JSON number, a string as a JSON string with nothing
    /// added, bytes as a JSON string of `0x` and lowercase hex: the form a
    /// reader who knows the order type reads back as the same value.
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Long(value) => serializer.serialize_i64(*value),
            Self::Str(text) => serializer.serialize_str(text),
            Self::Bytes(_) => serializer.collect_str(self),
        }
    }
}

/// What the load records of one ordered file's order column (RFD 025,
/// Load), from every order value it decodes: the exact least and greatest
/// order value over all the file's rows, deleted rows included, the
/// distinct keys of each row group, in footer order, a key whose run
/// crosses a row-group boundary counted in each row group it is in, and
/// the most rows one key's run holds, across row groups included. A row
/// group without rows has no distinct key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrderFacts {
    /// The least order value of the file.
    pub min: OrderValue,
    /// The greatest order value of the file.
    pub max: OrderValue,
    /// Per row group, the distinct keys it holds.
    pub distinct_keys: Vec<u64>,
    /// The rows of the file's longest run of one key.
    pub longest_run: u64,
}

/// An ordered table's order column as a snapshot's schema gives it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrderColumnLayout {
    /// The column's configured name.
    pub name: String,
    /// The column's field id in the snapshot's schema.
    pub field_id: i32,
    /// The column's Iceberg type.
    pub order_type: OrderType,
}

/// Which end of a key's run a Scan reads from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Direction {
    /// Highest order value first.
    NewestFirst,
    /// Lowest order value first.
    OldestFirst,
}

/// What one Scan of a key asks for. `limit` is from 1 to the table's
/// `max_scan_rows`, and 0 is refused; the bounds are half-open, `start`
/// inclusive and `before` exclusive.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanBounds {
    /// The direction the rows are returned in.
    pub direction: Direction,
    /// The most rows to return.
    pub limit: u32,
    /// Only rows whose order value is at or above this.
    pub start: Option<OrderValue>,
    /// Only rows whose order value is below this.
    pub before: Option<OrderValue>,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parquet::schema::parser::parse_message_type;
    use parquet::schema::types::SchemaDescriptor;

    use super::*;

    /// The width of the fixed types the tests name.
    const WIDTH: u32 = 16;

    #[test]
    fn test_from_iceberg_admits_every_order_type_and_refuses_others() {
        let admitted = [
            (PrimitiveType::Int, OrderType::Int),
            (PrimitiveType::Long, OrderType::Long),
            (PrimitiveType::Date, OrderType::Date),
            (PrimitiveType::Timestamp, OrderType::Timestamp),
            (PrimitiveType::Timestamptz, OrderType::TimestampTz),
            (PrimitiveType::TimestampNs, OrderType::TimestampNs),
            (PrimitiveType::TimestamptzNs, OrderType::TimestampTzNs),
            (PrimitiveType::String, OrderType::String),
            (PrimitiveType::Binary, OrderType::Binary),
            (
                PrimitiveType::Fixed(u64::from(WIDTH)),
                OrderType::Fixed(WIDTH),
            ),
        ];
        for (iceberg, order) in admitted {
            assert_eq!(OrderType::from_iceberg(&iceberg), Some(order), "{iceberg}");
            assert_eq!(order.to_string(), iceberg.to_string());
        }
        let refused = [
            PrimitiveType::Decimal {
                precision: 10,
                scale: 2,
            },
            PrimitiveType::Uuid,
            PrimitiveType::Float,
            PrimitiveType::Double,
            PrimitiveType::Boolean,
            PrimitiveType::Time,
            PrimitiveType::Fixed(u64::from(u32::MAX)),
        ];
        for iceberg in refused {
            assert_eq!(OrderType::from_iceberg(&iceberg), None, "{iceberg}");
        }
    }

    #[test]
    fn test_successor_is_the_least_value_above() {
        assert_eq!(OrderValue::Long(-1).successor(), Some(OrderValue::Long(0)));
        assert_eq!(OrderValue::Long(i64::MAX).successor(), None);
        let text = OrderValue::Str("ab".to_string());
        let next = text.successor().unwrap();
        assert_eq!(next, OrderValue::Str("ab\0".to_string()));
        for above in ["ab\0\0", "ab\u{1}", "abc", "b", "ac"] {
            let above = OrderValue::Str(above.to_string());
            assert!(text < next && next <= above, "{above}");
        }
        // Of a fixed width: one byte longer, below every other value of the
        // width above the one it follows.
        let fixed = OrderValue::Bytes(vec![1, 0xff]);
        let next = fixed.successor().unwrap();
        assert_eq!(next, OrderValue::Bytes(vec![1, 0xff, 0]));
        for above in [vec![2, 0], vec![1, 0xff, 0], vec![0xff, 0xff]] {
            let above = OrderValue::Bytes(above);
            assert!(fixed < next && next <= above, "{above}");
        }
    }

    #[test]
    fn test_order_values_compare_signed_and_bytewise() {
        assert!(OrderValue::Long(-1) < OrderValue::Long(0));
        assert!(OrderValue::Long(i64::MIN) < OrderValue::Long(i64::MAX));
        // Bytewise unsigned: 0x80 is above 0x7f, though as a signed byte
        // it is below.
        assert!(OrderValue::Bytes(vec![0x7f]) < OrderValue::Bytes(vec![0x80]));
        assert!(OrderValue::Bytes(vec![1]) < OrderValue::Bytes(vec![1, 0]));
        // UTF-8 bytes: a non-ASCII character is above every ASCII one.
        assert!(OrderValue::Str("z".to_string()) < OrderValue::Str("é".to_string()));
        assert!(OrderValue::Str("a".to_string()) < OrderValue::Str("ab".to_string()));
    }

    #[test]
    fn test_serialize_renders_a_long_as_a_number_a_string_as_text_and_bytes_as_hex() {
        let long = i64::MIN;
        assert_eq!(
            serde_json::to_value(OrderValue::Long(long)).unwrap(),
            serde_json::json!(long)
        );
        let text = "a \"quoted\" caf\u{e9}";
        assert_eq!(
            serde_json::to_value(OrderValue::Str(text.to_string())).unwrap(),
            serde_json::json!(text)
        );
        assert_eq!(
            serde_json::to_value(OrderValue::Bytes(vec![0x00, 0xab, 0xff])).unwrap(),
            serde_json::json!("0x00abff")
        );
        assert_eq!(
            serde_json::to_value(OrderType::Fixed(WIDTH)).unwrap(),
            serde_json::json!(OrderType::Fixed(WIDTH).to_string())
        );
    }

    #[test]
    fn test_variant_is_long_for_integer_and_time_types() {
        for order in [
            OrderType::Int,
            OrderType::Long,
            OrderType::Date,
            OrderType::Timestamp,
            OrderType::TimestampTz,
            OrderType::TimestampNs,
            OrderType::TimestampTzNs,
        ] {
            assert_eq!(order.variant(), OrderVariant::Long, "{order}");
        }
        assert_eq!(OrderType::String.variant(), OrderVariant::Str);
        assert_eq!(OrderType::Binary.variant(), OrderVariant::Bin);
        assert_eq!(OrderType::Fixed(WIDTH).variant(), OrderVariant::Bin);
    }

    #[test]
    fn test_variant_of_a_value_is_its_type_s_and_displays_as_the_oneof_names_it() {
        let values = [
            (OrderValue::Long(0), OrderType::Long, "long"),
            (OrderValue::Str(String::new()), OrderType::String, "str"),
            (OrderValue::Bytes(Vec::new()), OrderType::Binary, "bin"),
        ];
        for (value, order, name) in values {
            assert_eq!(value.variant(), order.variant(), "{value}");
            assert_eq!(value.variant().to_string(), name);
        }
    }

    /// How `order` admits the one column of the Parquet schema `column`,
    /// written as a message field such as `required int64 ts`.
    fn admits(order: OrderType, column: &str) -> Admit {
        let message = format!("message m {{ {column}; }}");
        let schema = SchemaDescriptor::new(Arc::new(parse_message_type(&message).unwrap()));
        order.admits(&schema.column(0))
    }

    #[test]
    fn test_admits_the_parquet_type_iceberg_writes_for_each_order_type() {
        let exact = [
            (OrderType::Int, "required int32 ts"),
            (OrderType::Int, "required int32 ts (INTEGER(32, true))"),
            (OrderType::Long, "required int64 ts"),
            (OrderType::Long, "required int64 ts (INTEGER(64, true))"),
            (OrderType::Date, "required int32 ts (DATE)"),
            (
                OrderType::Timestamp,
                "required int64 ts (TIMESTAMP(MICROS, false))",
            ),
            (
                OrderType::TimestampTz,
                "required int64 ts (TIMESTAMP(MICROS, true))",
            ),
            (OrderType::Timestamp, "required int64 ts (TIMESTAMP_MICROS)"),
            (
                OrderType::TimestampNs,
                "required int64 ts (TIMESTAMP(NANOS, false))",
            ),
            (
                OrderType::TimestampTzNs,
                "required int64 ts (TIMESTAMP(NANOS, true))",
            ),
            (OrderType::String, "required binary ts (STRING)"),
            (OrderType::Binary, "required binary ts"),
            (
                OrderType::Fixed(WIDTH),
                "required fixed_len_byte_array(16) ts",
            ),
        ];
        for (order, column) in exact {
            assert_eq!(admits(order, column), Admit::Exact, "{order} over {column}");
        }
        assert_eq!(
            admits(OrderType::Long, "required int32 ts"),
            Admit::PromotedFromInt
        );
        assert_eq!(
            admits(OrderType::Long, "required int32 ts (INTEGER(32, true))"),
            Admit::PromotedFromInt
        );
    }

    #[test]
    fn test_admits_refuses_int96_another_unit_and_another_physical_type() {
        let mismatched = [
            (OrderType::Timestamp, "required int96 ts"),
            (
                OrderType::Timestamp,
                "required int64 ts (TIMESTAMP(MILLIS, false))",
            ),
            (
                OrderType::TimestampTz,
                "required int64 ts (TIMESTAMP_MILLIS)",
            ),
            (
                OrderType::Timestamp,
                "required int64 ts (TIMESTAMP(NANOS, false))",
            ),
            (
                OrderType::TimestampNs,
                "required int64 ts (TIMESTAMP(MICROS, false))",
            ),
            // Unannotated, the unit is unknown.
            (OrderType::Timestamp, "required int64 ts"),
            // A promotion that changes the unit.
            (OrderType::Timestamp, "required int32 ts (DATE)"),
            (OrderType::Long, "required int32 ts (DATE)"),
            (
                OrderType::Long,
                "required int64 ts (TIMESTAMP(MICROS, true))",
            ),
            (OrderType::Int, "required int32 ts (DATE)"),
            (OrderType::Int, "required int64 ts"),
            (OrderType::Int, "required int32 ts (INTEGER(32, false))"),
            (OrderType::Date, "required int32 ts"),
            (OrderType::Long, "required binary ts"),
            (OrderType::String, "required int64 ts"),
            (OrderType::Binary, "required fixed_len_byte_array(16) ts"),
            (
                OrderType::Fixed(WIDTH),
                "required fixed_len_byte_array(8) ts",
            ),
            (OrderType::Fixed(WIDTH), "required binary ts"),
            (OrderType::Long, "required double ts"),
        ];
        for (order, column) in mismatched {
            assert_eq!(
                admits(order, column),
                Admit::Mismatch,
                "{order} over {column}"
            );
        }
    }
}
