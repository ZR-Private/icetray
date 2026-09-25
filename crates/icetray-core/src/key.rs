//! Lookup keys (RFD 008): the key types icetray serves and the order it
//! compares them in. A composite key is the caller's encoding into one of
//! them; icetray moves the bytes unchanged.

use std::borrow::Cow;
use std::fmt;

use iceberg::spec::{Datum, PrimitiveLiteral, PrimitiveType, Type};

/// The width of an Iceberg `uuid`, and of the `FIXED_LEN_BYTE_ARRAY` Parquet
/// stores it in.
pub const UUID_BYTES: usize = 16;

/// Where each hyphen falls in the canonical text form of a uuid: the byte
/// offsets that end its five groups of 8, 4, 4, 4, and 12 hex digits.
const UUID_GROUP_ENDS: [usize; 5] = [4, 6, 8, 10, UUID_BYTES];

/// Hex digits per byte, one per nibble, in every hex rendering of bytes: a
/// key's text form here, a chunk file's name in `local`, and the key text
/// the binary parses back. The one home for the width, so a parser and a
/// printer cannot disagree on it.
pub const HEX_DIGITS_PER_BYTE: usize = 2;

/// The Iceberg type of a table's key column (RFD 008), one of four; Iceberg
/// `fixed` is served as `Binary`. Display and serde give the lowercase
/// Iceberg type name that appears in the doctor's output and in logs.
// The serde names and Display are the same strings;
// `test_serialize_key_type_matches_display` holds them together.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KeyType {
    /// Iceberg `long`: a 64-bit signed integer, compared numerically.
    Long,
    /// Iceberg `string`: UTF-8 text, compared as its bytes.
    String,
    /// Iceberg `binary` or `fixed`: opaque bytes, compared bytewise.
    Binary,
    /// Iceberg `uuid`: sixteen bytes, compared in the order the Iceberg
    /// specification defines for the type, which is not assumed bytewise.
    Uuid,
}

impl KeyType {
    /// Every key type, in declaration order, for a caller that covers each.
    /// Complete: a successor match over every variant, which a new variant
    /// fails to compile, is compared to this list in tests.
    pub const ALL: [Self; 4] = [Self::Long, Self::String, Self::Binary, Self::Uuid];

    /// The key type an Iceberg column type maps to: long to `Long`, string
    /// to `String`, binary and fixed of any length to `Binary`, uuid to
    /// `Uuid`. Every other type, primitive or nested, is `None`; the caller
    /// that owns the table name turns that into
    /// `RefusalReason::KeyTypeUnsupported` (RFD 008).
    pub fn from_iceberg(ty: &Type) -> Option<Self> {
        match ty {
            Type::Primitive(primitive) => match primitive {
                PrimitiveType::Long => Some(Self::Long),
                PrimitiveType::String => Some(Self::String),
                PrimitiveType::Binary | PrimitiveType::Fixed(_) => Some(Self::Binary),
                PrimitiveType::Uuid => Some(Self::Uuid),
                PrimitiveType::Boolean
                | PrimitiveType::Int
                | PrimitiveType::Float
                | PrimitiveType::Double
                | PrimitiveType::Decimal { .. }
                | PrimitiveType::Date
                | PrimitiveType::Time
                | PrimitiveType::Timestamp
                | PrimitiveType::Timestamptz
                | PrimitiveType::TimestampNs
                | PrimitiveType::TimestamptzNs => None,
            },
            Type::Struct(_) | Type::List(_) | Type::Map(_) => None,
        }
    }

    /// Whether every key of the type has one width: a `long` is eight bytes
    /// and a `uuid` sixteen, while `string` and `binary` keys vary. The
    /// testkit's fixture writer consults it to refuse padding a fixed-width
    /// key. The match names every variant without a wildcard, so a new key
    /// type does not compile until it states its width here.
    pub fn is_fixed_width(self) -> bool {
        match self {
            Self::Long | Self::Uuid => true,
            Self::String | Self::Binary => false,
        }
    }
}

impl fmt::Display for KeyType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Long => "long",
            Self::String => "string",
            Self::Binary => "binary",
            Self::Uuid => "uuid",
        };
        f.write_str(name)
    }
}

/// A lookup key. `Ord` is the order a table's files are sorted and searched
/// in, per type: `Long` numerically, `Str` as its UTF-8 bytes, `Bytes`
/// bytewise unsigned, and `Uuid` bytewise unsigned over its sixteen bytes.
/// The uuid order is derived because both references agree on it:
/// iceberg-rust compares a uuid `Datum` as the big-endian `u128` of its
/// bytes, and Parquet gives a `FIXED_LEN_BYTE_ARRAY` with the `UUID` logical
/// type the `UNSIGNED` sort order and writes its statistics under it
/// (`test_cmp_uuid_matches_iceberg_datum_and_parquet_order`,
/// `test_cmp_uuid_matches_parquet_uuid_column_statistics`). That order is
/// not the one Iceberg Java's `Comparators` gives the type, which is
/// `java.util.UUID`'s and compares each 64-bit half as a signed long; files
/// sorted by that comparator read as unsorted here (RFD 007). The derived
/// order across variants carries no meaning: keys of different types never
/// share a table.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Key {
    /// An Iceberg `long`.
    Long(i64),
    /// An Iceberg `string`.
    Str(String),
    /// An Iceberg `binary` or `fixed`.
    Bytes(Vec<u8>),
    /// An Iceberg `uuid`, as the sixteen bytes Parquet stores.
    Uuid([u8; UUID_BYTES]),
}

impl Key {
    /// The key's type.
    pub fn key_type(&self) -> KeyType {
        match self {
            Self::Long(_) => KeyType::Long,
            Self::Str(_) => KeyType::String,
            Self::Bytes(_) => KeyType::Binary,
            Self::Uuid(_) => KeyType::Uuid,
        }
    }

    /// The key an Iceberg datum (a manifest bound, for instance) carries.
    /// The literal decides: a long, a string, the bytes of a binary or fixed
    /// literal, or a uuid literal as its sixteen big-endian bytes. Any other
    /// literal is `None`. The datum's type was already admitted by
    /// `KeyType::from_iceberg` when the table's layout was derived.
    pub(crate) fn from_datum(datum: &Datum) -> Option<Self> {
        match datum.literal() {
            PrimitiveLiteral::Long(value) => Some(Self::Long(*value)),
            PrimitiveLiteral::String(text) => Some(Self::Str(text.clone())),
            PrimitiveLiteral::Binary(bytes) => Some(Self::Bytes(bytes.clone())),
            PrimitiveLiteral::UInt128(value) => Some(Self::Uuid(value.to_be_bytes())),
            PrimitiveLiteral::Boolean(_)
            | PrimitiveLiteral::Int(_)
            | PrimitiveLiteral::Float(_)
            | PrimitiveLiteral::Double(_)
            | PrimitiveLiteral::Int128(_)
            | PrimitiveLiteral::AboveMax
            | PrimitiveLiteral::BelowMin => None,
        }
    }

    /// The bytes the bucket transform hashes (RFD 008, spec appendix B): a
    /// long as its eight little-endian bytes, a string as its UTF-8, binary
    /// and uuid as their bytes. For every type but `Long` these are also the
    /// bytes the served order compares.
    pub fn as_bytes(&self) -> Cow<'_, [u8]> {
        match self {
            Self::Long(value) => Cow::Owned(value.to_le_bytes().to_vec()),
            Self::Str(text) => Cow::Borrowed(text.as_bytes()),
            Self::Bytes(bytes) => Cow::Borrowed(bytes),
            Self::Uuid(bytes) => Cow::Borrowed(bytes),
        }
    }
}

/// Lowercase hex, `HEX_DIGITS_PER_BYTE` per byte, no prefix.
fn write_hex(f: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    bytes
        .iter()
        .try_for_each(|byte| write!(f, "{byte:0HEX_DIGITS_PER_BYTE$x}"))
}

/// Uuid vectors and random keys the tests of this module and of `bucket`
/// share. Nothing here is compiled outside tests.
#[cfg(test)]
pub(crate) mod testing {
    use rand::RngExt;
    use rand::rngs::StdRng;

    use super::UUID_BYTES;

    /// The width of one of a uuid's two 64-bit halves, the unit
    /// `java.util.UUID` compares as a signed long and the half a fixture
    /// numbers its uuids in.
    pub(crate) const UUID_HALF_BYTES: usize = UUID_BYTES / 2;
    /// The uuid of the Iceberg specification's appendix B hash vector, as
    /// bytes, which iceberg-rust's own bucket test states.
    pub(crate) const SPEC_UUID: [u8; UUID_BYTES] = [
        0xf7, 0x9c, 0x3e, 0x09, 0x67, 0x7c, 0x4b, 0xbd, 0xa4, 0x79, 0x3f, 0x34, 0x9c, 0xb7, 0x85,
        0xe7,
    ];

    /// `len` bytes drawn from `rng`.
    pub(crate) fn random_bytes(rng: &mut StdRng, len: usize) -> Vec<u8> {
        (0..len).map(|_| rng.random()).collect()
    }
}

impl fmt::Display for Key {
    /// A long as the integer, a string quoted with `Debug` escaping, bytes as
    /// `0x`-prefixed lowercase hex, a uuid in its canonical hyphenated form.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Long(value) => write!(f, "{value}"),
            Self::Str(text) => write!(f, "{text:?}"),
            Self::Bytes(bytes) => {
                f.write_str("0x")?;
                write_hex(f, bytes)
            }
            Self::Uuid(bytes) => {
                let mut start = 0;
                for (index, &end) in UUID_GROUP_ENDS.iter().enumerate() {
                    if index > 0 {
                        f.write_str("-")?;
                    }
                    write_hex(f, &bytes[start..end])?;
                    start = end;
                }
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;
    use std::collections::HashSet;
    use std::sync::Arc;

    use iceberg::spec::StructType;
    use parquet::basic::{
        ColumnOrder, ConvertedType, LogicalType, Repetition, SortOrder, Type as PhysicalType,
    };
    use parquet::data_type::{
        ByteArray, ByteArrayType, DataType, FixedLenByteArray, FixedLenByteArrayType, Int64Type,
    };
    use parquet::file::properties::WriterProperties;
    use parquet::file::statistics::Statistics;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::types::Type as ParquetType;
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};
    use uuid::Uuid;

    use super::testing::{SPEC_UUID, UUID_HALF_BYTES, random_bytes};
    use super::*;

    /// The seed of the long ordering sample; fixed so a failure reproduces.
    const LONG_SEED: u64 = 1;
    /// The seed of the string ordering sample.
    const STRING_SEED: u64 = 2;
    /// The seed of the binary ordering sample.
    const BYTES_SEED: u64 = 3;
    /// The seed of the fixed ordering sample.
    const FIXED_SEED: u64 = 4;
    /// The seed of the uuid sample.
    const UUID_SEED: u64 = 8;
    /// Keys each of the long, string, binary, and fixed ordering oracles
    /// draws.
    const ORDER_KEYS: usize = 512;
    /// Keys per row group in an oracle file, so a file's written bounds are
    /// checked at several extremes rather than one pair.
    const ROW_GROUP_KEYS: usize = 64;
    /// The longest random string drawn, in characters.
    const MAX_STR_CHARS: usize = 24;
    /// The longest random byte key drawn.
    const MAX_BYTES_LEN: usize = 40;
    /// The width of the fixed keys drawn.
    const FIXED_LEN: usize = 12;
    /// The code point ranges of one-, two-, three-, and four-byte UTF-8,
    /// starting at space and clear of the surrogates. The four-byte range is
    /// where UTF-16 order diverges from byte order, so a sample without it
    /// would not tell the two apart.
    const UTF8_WIDTH_RANGES: [std::ops::Range<u32>; 4] =
        [0x20..0x80, 0x80..0x800, 0x800..0xD800, 0x1_0000..0x11_0000];
    /// Random uuids drawn; each is expanded to every `SIGN_BIT_SETTINGS`.
    const UUID_DRAWS: usize = 128;
    /// The settings of the two halves' sign bits each drawn uuid is expanded
    /// to, so the sample covers every quadrant where signed-half order and
    /// bytewise order can disagree.
    const SIGN_BIT_SETTINGS: [(bool, bool); 4] =
        [(false, false), (true, false), (false, true), (true, true)];
    /// The sign bit of a big-endian 64-bit half, in its first byte.
    const HIGH_BIT: u8 = 0x80;
    /// `SPEC_UUID` as its canonical text; the display test pins the
    /// rendering to it.
    const SPEC_UUID_TEXT: &str = "f79c3e09-677c-4bbd-a479-3f349cb785e7";
    /// A long inside the range from `LOW` to `HIGH`.
    const INSIDE: i64 = 5;
    /// A negative long, the lower end of that range.
    const LOW: i64 = -10;
    /// A positive long, the upper end of that range.
    const HIGH: i64 = 10;

    /// Random uuids with every combination of the two halves' sign bits.
    fn uuid_sample() -> Vec<[u8; UUID_BYTES]> {
        let mut rng = StdRng::seed_from_u64(UUID_SEED);
        let mut sample = Vec::with_capacity(UUID_DRAWS * SIGN_BIT_SETTINGS.len());
        for _ in 0..UUID_DRAWS {
            let drawn: [u8; UUID_BYTES] = rng.random();
            for (first_high, second_high) in SIGN_BIT_SETTINGS {
                let mut uuid = drawn;
                set_sign_bit(&mut uuid[0], first_high);
                set_sign_bit(&mut uuid[UUID_HALF_BYTES], second_high);
                sample.push(uuid);
            }
        }
        sample
    }

    fn set_sign_bit(byte: &mut u8, high: bool) {
        if high {
            *byte |= HIGH_BIT;
        } else {
            *byte &= !HIGH_BIT;
        }
    }

    /// `java.util.UUID.compareTo`: each 64-bit half as a signed long, most
    /// significant half first.
    fn signed_halves_cmp(a: &[u8; UUID_BYTES], b: &[u8; UUID_BYTES]) -> Ordering {
        let halves = |uuid: &[u8; UUID_BYTES]| {
            let (first, second) = uuid.split_at(UUID_HALF_BYTES);
            (
                i64::from_be_bytes(first.try_into().unwrap()),
                i64::from_be_bytes(second.try_into().unwrap()),
            )
        };
        halves(a).cmp(&halves(b))
    }

    /// Random text of one to `MAX_STR_CHARS` characters, each drawn from one
    /// of the four UTF-8 widths.
    fn random_text(rng: &mut StdRng) -> String {
        let len = rng.random_range(1..=MAX_STR_CHARS);
        (0..len)
            .map(|_| {
                let width = rng.random_range(0..UTF8_WIDTH_RANGES.len());
                char::from_u32(rng.random_range(UTF8_WIDTH_RANGES[width].clone())).unwrap()
            })
            .collect()
    }

    fn long_sample() -> Vec<i64> {
        let mut rng = StdRng::seed_from_u64(LONG_SEED);
        (0..ORDER_KEYS).map(|_| rng.random()).collect()
    }

    fn str_sample() -> Vec<String> {
        let mut rng = StdRng::seed_from_u64(STRING_SEED);
        (0..ORDER_KEYS).map(|_| random_text(&mut rng)).collect()
    }

    fn bytes_sample() -> Vec<Vec<u8>> {
        let mut rng = StdRng::seed_from_u64(BYTES_SEED);
        (0..ORDER_KEYS)
            .map(|_| {
                let len = rng.random_range(0..=MAX_BYTES_LEN);
                random_bytes(&mut rng, len)
            })
            .collect()
    }

    fn fixed_sample() -> Vec<Vec<u8>> {
        let mut rng = StdRng::seed_from_u64(FIXED_SEED);
        (0..ORDER_KEYS)
            .map(|_| random_bytes(&mut rng, FIXED_LEN))
            .collect()
    }

    /// A required key column of the physical type, with an optional logical
    /// type and, for `FIXED_LEN_BYTE_ARRAY`, its width.
    fn key_field(
        physical: PhysicalType,
        logical: Option<LogicalType>,
        fixed_len: Option<usize>,
    ) -> ParquetType {
        let mut builder = ParquetType::primitive_type_builder("key", physical)
            .with_repetition(Repetition::REQUIRED)
            .with_logical_type(logical);
        if let Some(len) = fixed_len {
            builder = builder.with_length(i32::try_from(len).unwrap());
        }
        builder.build().unwrap()
    }

    /// Writer properties that keep statistics whole, so a written bound is
    /// the key it came from and not a cut prefix.
    fn exact_statistics() -> WriterProperties {
        WriterProperties::builder()
            .set_statistics_truncate_length(None)
            .build()
    }

    /// Writes `values` as the one column of an in-memory Parquet file,
    /// `ROW_GROUP_KEYS` per row group, and returns each row group's column
    /// chunk statistics in order.
    fn write_column<T: DataType>(
        field: ParquetType,
        properties: WriterProperties,
        values: &[T::T],
    ) -> Vec<Statistics> {
        let schema = ParquetType::group_type_builder("schema")
            .with_fields(vec![Arc::new(field)])
            .build()
            .unwrap();
        let mut sink = Vec::new();
        let mut writer =
            SerializedFileWriter::new(&mut sink, Arc::new(schema), Arc::new(properties)).unwrap();
        for chunk in values.chunks(ROW_GROUP_KEYS) {
            let mut row_group = writer.next_row_group().unwrap();
            let mut column = row_group.next_column().unwrap().unwrap();
            column.typed::<T>().write_batch(chunk, None, None).unwrap();
            column.close().unwrap();
            row_group.close().unwrap();
        }
        let metadata = writer.close().unwrap();
        metadata
            .row_groups()
            .iter()
            .map(|group| group.column(0).statistics().unwrap().clone())
            .collect()
    }

    /// Asserts that each row group's written bounds are exact and equal the
    /// least and greatest of the keys written into it under `Key`'s order.
    /// Returns the keys whose row groups were checked.
    fn assert_statistics_are_key_bounds(keys: &[Key], statistics: &[Statistics]) -> usize {
        let chunks = keys.chunks(ROW_GROUP_KEYS);
        assert_eq!(statistics.len(), chunks.len());
        let mut checked = 0;
        for (chunk, stats) in chunks.zip(statistics) {
            let min = chunk.iter().min().unwrap();
            let max = chunk.iter().max().unwrap();
            assert_ne!(min, max);
            assert!(stats.min_is_exact() && stats.max_is_exact());
            let min_bytes = stats.min_bytes_opt().unwrap();
            let max_bytes = stats.max_bytes_opt().unwrap();
            assert_eq!(min_bytes, &*min.as_bytes());
            assert_eq!(max_bytes, &*max.as_bytes());
            checked += chunk.len();
        }
        checked
    }

    /// Compares every pair of `keys` under `Key`'s order and under Parquet's
    /// `ByteArray` order, the comparator its writer ranks `BYTE_ARRAY`
    /// statistics with. Returns the pairs compared.
    fn assert_cmp_matches_byte_array(keys: &[Key]) -> usize {
        let mut compared = 0;
        for a in keys {
            for b in keys {
                let parquet = ByteArray::from(a.as_bytes().to_vec())
                    .partial_cmp(&ByteArray::from(b.as_bytes().to_vec()));
                assert_eq!(parquet, Some(a.cmp(b)), "parquet byte order of {a} vs {b}");
                compared += 1;
            }
        }
        compared
    }

    /// The key type after `key_type` in declaration order, `None` after the
    /// last. The match names every variant, so a variant added to the enum
    /// fails to compile here until it is placed in the chain, and the chain
    /// is what `ALL` is checked against.
    fn after(key_type: KeyType) -> Option<KeyType> {
        match key_type {
            KeyType::Long => Some(KeyType::String),
            KeyType::String => Some(KeyType::Binary),
            KeyType::Binary => Some(KeyType::Uuid),
            KeyType::Uuid => None,
        }
    }

    #[test]
    fn test_all_lists_every_key_type_once_in_declaration_order() {
        let mut chain = vec![KeyType::ALL[0]];
        while let Some(next) = after(*chain.last().unwrap()) {
            chain.push(next);
        }
        assert_eq!(chain, KeyType::ALL);
    }

    #[test]
    fn test_fmt_key_type_lowercase_iceberg_names() {
        let cases = [
            (KeyType::Long, "long"),
            (KeyType::String, "string"),
            (KeyType::Binary, "binary"),
            (KeyType::Uuid, "uuid"),
        ];
        assert_eq!(cases.map(|(key_type, _)| key_type), KeyType::ALL);
        for (key_type, name) in cases {
            assert_eq!(key_type.to_string(), name);
        }
    }

    #[test]
    fn test_serialize_key_type_matches_display() {
        for key_type in KeyType::ALL {
            let json = serde_json::to_string(&key_type).unwrap();
            assert_eq!(json, format!("\"{key_type}\""));
            let back: KeyType = serde_json::from_str(&json).unwrap();
            assert_eq!(back, key_type);
        }
    }

    #[test]
    fn test_from_iceberg_admits_the_four_key_types_and_fixed() {
        let admitted = [
            (PrimitiveType::Long, KeyType::Long),
            (PrimitiveType::String, KeyType::String),
            (PrimitiveType::Binary, KeyType::Binary),
            (PrimitiveType::Fixed(1), KeyType::Binary),
            (
                PrimitiveType::Fixed(crate::approx::count(UUID_BYTES)),
                KeyType::Binary,
            ),
            (PrimitiveType::Uuid, KeyType::Uuid),
        ];
        for (primitive, expected) in admitted {
            assert_eq!(
                KeyType::from_iceberg(&Type::Primitive(primitive)),
                Some(expected)
            );
        }
    }

    #[test]
    fn test_is_fixed_width_for_long_and_uuid_only() {
        let cases = [
            (KeyType::Long, true),
            (KeyType::String, false),
            (KeyType::Binary, false),
            (KeyType::Uuid, true),
        ];
        for (key_type, fixed) in cases {
            assert_eq!(key_type.is_fixed_width(), fixed, "{key_type}");
        }
    }

    #[test]
    fn test_from_iceberg_refuses_other_types() {
        let refused = [
            Type::Primitive(PrimitiveType::Int),
            Type::Primitive(PrimitiveType::Boolean),
            Type::Primitive(PrimitiveType::Timestamp),
            Type::Primitive(PrimitiveType::Decimal {
                precision: 1,
                scale: 0,
            }),
            Type::Struct(StructType::new(Vec::new())),
        ];
        for ty in &refused {
            assert_eq!(KeyType::from_iceberg(ty), None);
        }
    }

    #[test]
    fn test_key_type_matches_variant() {
        assert_eq!(Key::Long(0).key_type(), KeyType::Long);
        assert_eq!(Key::Str(String::new()).key_type(), KeyType::String);
        assert_eq!(Key::Bytes(Vec::new()).key_type(), KeyType::Binary);
        assert_eq!(Key::Uuid(SPEC_UUID).key_type(), KeyType::Uuid);
    }

    #[test]
    fn test_cmp_long_numeric_including_negatives() {
        assert!(Key::Long(i64::MIN) < Key::Long(LOW));
        assert!(Key::Long(LOW) < Key::Long(-1));
        assert!(Key::Long(-1) < Key::Long(0));
        assert!(Key::Long(0) < Key::Long(1));
        assert!(Key::Long(HIGH) < Key::Long(i64::MAX));
    }

    #[test]
    fn test_cmp_str_utf8_bytes_including_non_ascii() {
        assert!(Key::Str("a".into()) < Key::Str("b".into()));
        assert!(Key::Str(String::new()) < Key::Str("a".into()));
        assert!(Key::Str("ab".into()) < Key::Str("abc".into()));
        // "é" is 0xC3 0xA9 in UTF-8 and sorts after every ASCII byte.
        assert!(Key::Str("é".into()) > Key::Str("z".into()));
        // Code-point order and UTF-8 byte order coincide.
        assert!(Key::Str("é".into()) < Key::Str("ﬁ".into()));
    }

    #[test]
    fn test_cmp_bytes_unsigned_bytewise() {
        assert!(Key::Bytes(vec![HIGH_BIT]) > Key::Bytes(vec![HIGH_BIT - 1]));
        assert!(Key::Bytes(Vec::new()) < Key::Bytes(vec![0]));
        assert!(Key::Bytes(vec![1]) < Key::Bytes(vec![1, 0]));
        assert!(Key::Bytes(vec![u8::MAX]) > Key::Bytes(vec![0, u8::MAX]));
    }

    #[test]
    fn test_cmp_uuid_matches_iceberg_datum_and_parquet_order() {
        let sample = uuid_sample();
        let mut compared = 0;
        let mut signed_disagreements = 0;
        for a in &sample {
            for b in &sample {
                let ours = Key::Uuid(*a).cmp(&Key::Uuid(*b));
                let iceberg = Datum::uuid(Uuid::from_bytes(*a))
                    .partial_cmp(&Datum::uuid(Uuid::from_bytes(*b)));
                assert_eq!(iceberg, Some(ours), "iceberg datum order of {a:?} vs {b:?}");
                let parquet = FixedLenByteArray::from(a.to_vec())
                    .partial_cmp(&FixedLenByteArray::from(b.to_vec()));
                assert_eq!(parquet, Some(ours), "parquet fixed order of {a:?} vs {b:?}");
                if signed_halves_cmp(a, b) != ours {
                    signed_disagreements += 1;
                }
                compared += 1;
            }
        }
        assert_eq!(compared, sample.len() * sample.len());
        // The sample reaches the region where java.util.UUID's signed order
        // diverges; otherwise agreement with bytewise would prove nothing.
        assert!(signed_disagreements > 0);
        assert_eq!(
            ColumnOrder::sort_order_for_type(
                Some(&LogicalType::Uuid),
                ConvertedType::NONE,
                parquet::basic::Type::FIXED_LEN_BYTE_ARRAY,
            ),
            SortOrder::UNSIGNED
        );
    }

    #[test]
    fn test_cmp_long_matches_parquet_int64_column_statistics() {
        let sample = long_sample();
        assert!(sample.iter().any(|value| *value < 0) && sample.iter().any(|value| *value >= 0));
        let keys: Vec<Key> = sample.iter().map(|value| Key::Long(*value)).collect();
        let statistics = write_column::<Int64Type>(
            key_field(PhysicalType::INT64, None, None),
            exact_statistics(),
            &sample,
        );
        assert_eq!(
            assert_statistics_are_key_bounds(&keys, &statistics),
            ORDER_KEYS
        );
        assert_eq!(
            ColumnOrder::sort_order_for_type(None, ConvertedType::NONE, PhysicalType::INT64),
            SortOrder::SIGNED
        );
    }

    #[test]
    fn test_cmp_str_matches_parquet_string_column_statistics() {
        let sample = str_sample();
        let widths: HashSet<usize> = sample
            .iter()
            .flat_map(|text| text.chars().map(char::len_utf8))
            .collect();
        assert_eq!(widths.len(), UTF8_WIDTH_RANGES.len());
        let keys: Vec<Key> = sample.iter().map(|text| Key::Str(text.clone())).collect();
        let values: Vec<ByteArray> = sample
            .iter()
            .map(|text| ByteArray::from(text.as_str()))
            .collect();
        let statistics = write_column::<ByteArrayType>(
            key_field(PhysicalType::BYTE_ARRAY, Some(LogicalType::String), None),
            exact_statistics(),
            &values,
        );
        assert_eq!(
            assert_statistics_are_key_bounds(&keys, &statistics),
            ORDER_KEYS
        );
        assert_eq!(
            assert_cmp_matches_byte_array(&keys),
            ORDER_KEYS * ORDER_KEYS
        );
        assert_eq!(
            ColumnOrder::sort_order_for_type(
                Some(&LogicalType::String),
                ConvertedType::NONE,
                PhysicalType::BYTE_ARRAY,
            ),
            SortOrder::UNSIGNED
        );
    }

    #[test]
    fn test_cmp_bytes_matches_parquet_binary_column_statistics() {
        let sample = bytes_sample();
        let keys: Vec<Key> = sample
            .iter()
            .map(|bytes| Key::Bytes(bytes.clone()))
            .collect();
        let values: Vec<ByteArray> = sample
            .iter()
            .map(|bytes| ByteArray::from(bytes.clone()))
            .collect();
        let statistics = write_column::<ByteArrayType>(
            key_field(PhysicalType::BYTE_ARRAY, None, None),
            exact_statistics(),
            &values,
        );
        assert_eq!(
            assert_statistics_are_key_bounds(&keys, &statistics),
            ORDER_KEYS
        );
        assert_eq!(
            assert_cmp_matches_byte_array(&keys),
            ORDER_KEYS * ORDER_KEYS
        );
        assert_eq!(
            ColumnOrder::sort_order_for_type(None, ConvertedType::NONE, PhysicalType::BYTE_ARRAY),
            SortOrder::UNSIGNED
        );
    }

    #[test]
    fn test_cmp_fixed_matches_parquet_fixed_column_statistics() {
        let sample = fixed_sample();
        let keys: Vec<Key> = sample
            .iter()
            .map(|bytes| Key::Bytes(bytes.clone()))
            .collect();
        let values: Vec<FixedLenByteArray> = sample
            .iter()
            .map(|bytes| FixedLenByteArray::from(bytes.clone()))
            .collect();
        let statistics = write_column::<FixedLenByteArrayType>(
            key_field(PhysicalType::FIXED_LEN_BYTE_ARRAY, None, Some(FIXED_LEN)),
            exact_statistics(),
            &values,
        );
        assert_eq!(
            assert_statistics_are_key_bounds(&keys, &statistics),
            ORDER_KEYS
        );
        assert_eq!(
            ColumnOrder::sort_order_for_type(
                None,
                ConvertedType::NONE,
                PhysicalType::FIXED_LEN_BYTE_ARRAY,
            ),
            SortOrder::UNSIGNED
        );
    }

    #[test]
    fn test_cmp_uuid_matches_parquet_uuid_column_statistics() {
        let sample = uuid_sample();
        let keys: Vec<Key> = sample.iter().map(|uuid| Key::Uuid(*uuid)).collect();
        let values: Vec<FixedLenByteArray> = sample
            .iter()
            .map(|uuid| FixedLenByteArray::from(uuid.to_vec()))
            .collect();
        let statistics = write_column::<FixedLenByteArrayType>(
            key_field(
                PhysicalType::FIXED_LEN_BYTE_ARRAY,
                Some(LogicalType::Uuid),
                Some(UUID_BYTES),
            ),
            exact_statistics(),
            &values,
        );
        assert_eq!(
            assert_statistics_are_key_bounds(&keys, &statistics),
            sample.len()
        );
    }

    #[test]
    fn test_as_bytes_per_type() {
        assert_eq!(&*Key::Long(1).as_bytes(), &[1, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(&*Key::Long(-1).as_bytes(), &[u8::MAX; size_of::<i64>()]);
        assert_eq!(&*Key::Str("hé".into()).as_bytes(), "hé".as_bytes());
        assert_eq!(&*Key::Bytes(vec![0, HIGH_BIT]).as_bytes(), &[0, HIGH_BIT]);
        assert_eq!(&*Key::Uuid(SPEC_UUID).as_bytes(), &SPEC_UUID);
    }

    #[test]
    fn test_from_datum_per_type() {
        assert_eq!(
            Key::from_datum(&Datum::long(INSIDE)),
            Some(Key::Long(INSIDE))
        );
        assert_eq!(
            Key::from_datum(&Datum::string("k")),
            Some(Key::Str("k".into()))
        );
        assert_eq!(
            Key::from_datum(&Datum::binary(vec![0, 1])),
            Some(Key::Bytes(vec![0, 1]))
        );
        assert_eq!(
            Key::from_datum(&Datum::fixed(SPEC_UUID)),
            Some(Key::Bytes(SPEC_UUID.to_vec()))
        );
        assert_eq!(
            Key::from_datum(&Datum::uuid(Uuid::from_bytes(SPEC_UUID))),
            Some(Key::Uuid(SPEC_UUID))
        );
        assert_eq!(Key::from_datum(&Datum::int(1)), None);
        assert_eq!(Key::from_datum(&Datum::bool(true)), None);
    }

    #[test]
    fn test_fmt_key_per_type() {
        assert_eq!(Key::Long(LOW).to_string(), "-10");
        assert_eq!(Key::Str("a\"b".into()).to_string(), "\"a\\\"b\"");
        assert_eq!(Key::Bytes(vec![1, u8::MAX]).to_string(), "0x01ff");
        assert_eq!(Key::Uuid(SPEC_UUID).to_string(), SPEC_UUID_TEXT);
    }

    #[test]
    fn test_fmt_uuid_matches_hyphenated_reference() {
        for uuid in uuid_sample() {
            assert_eq!(
                Key::Uuid(uuid).to_string(),
                Uuid::from_bytes(uuid).hyphenated().to_string()
            );
        }
    }
}
