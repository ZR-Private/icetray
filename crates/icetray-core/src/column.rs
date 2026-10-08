//! Column resolution (RFD 023): how each configured column is found in a
//! data file. A column is matched by its name among the file's top-level
//! columns and checked against the field id the snapshot's schema gives
//! that name when the file records field ids; a value column the file
//! provably lacks answers the column's default from that schema, and every
//! other mismatch refuses the snapshot as `RefusalReason::ColumnMismatch`.
//! The check compares the names and field ids a footer holds and reads
//! nothing, so a load runs it against every file of its snapshot, a file
//! reused from the previous generation included: a snapshot that drops a
//! column and adds another under the same name changes what an unchanged
//! file means.

use std::borrow::Cow;
use std::collections::BTreeSet;
use std::fmt;
use std::fmt::Write as _;

use bytes::Bytes;
use iceberg::spec::{DEFAULT_SCHEMA_ID, Literal, NestedField, PrimitiveLiteral, TableMetadata};
use parquet::basic::Type as PhysicalType;
use parquet::schema::types::{ColumnDescriptor, SchemaDescriptor, Type as ParquetType};

use crate::error::{Error, RefusalReason, Result};
use crate::manifest::TableLayout;

/// What makes a configured column unservable in a snapshot (RFD 023), as
/// `RefusalReason::ColumnMismatch` carries it. Display gives the case for
/// a log line.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ColumnMismatch {
    /// The file holds a column of the configured name under another field
    /// id than the snapshot's schema gives the name: the names were
    /// swapped, or the name was dropped and added again, so the column
    /// found by name is not the one the schema means.
    OtherFieldId {
        /// The field id the snapshot's schema gives the name.
        expected: i32,
        /// The field id the file's column of that name carries.
        found: i32,
    },
    /// The file holds no column of the configured name but holds the field
    /// id the schema gives it under another name: the column was renamed.
    Renamed {
        /// The field id the snapshot's schema gives the configured name.
        field_id: i32,
        /// The name the file holds that field id under.
        found: String,
    },
    /// The file records no field ids and holds no column of the configured
    /// name, so nothing in it says whether the column was added after it
    /// or renamed.
    AbsentWithoutFieldIds,
    /// The file records field ids and holds neither the key column's name
    /// nor its field id. A value column answers its default there; a key
    /// column has none.
    KeyAbsent,
    /// The configured name is not a top-level field of the snapshot's
    /// schema, such as a nested field's dotted name; names are matched
    /// among top-level fields alone.
    NotTopLevel,
    /// The snapshot's schema states an `initial-default` for the value
    /// column that the iceberg crate does not read as the column's type.
    UnreadableDefault,
    /// A `fixed` key or value column that the file stores as anything but
    /// a `FIXED_LEN_BYTE_ARRAY` of the schema's width.
    FixedWidth {
        /// The width the snapshot's schema gives the column.
        expected: u64,
        /// The width the file stores it at; `None` for a `BYTE_ARRAY`.
        stored: Option<i32>,
    },
    /// The file's column of the configured name is a group of nested
    /// columns, where the schema gives the column a primitive type.
    Group,
}

impl fmt::Display for ColumnMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OtherFieldId { expected, found } => write!(
                f,
                "the file holds the name under field id {found}, the schema under {expected}"
            ),
            Self::Renamed { field_id, found } => {
                write!(
                    f,
                    "the file holds field id {field_id} under the name {found}"
                )
            }
            Self::AbsentWithoutFieldIds => {
                f.write_str("the file records no field ids and holds no column of the name")
            }
            Self::KeyAbsent => {
                f.write_str("the file holds neither the key column's name nor its field id")
            }
            Self::NotTopLevel => f.write_str("the name is not a top-level field of the schema"),
            Self::UnreadableDefault => {
                f.write_str("the schema's initial-default does not read as the column's type")
            }
            Self::FixedWidth {
                expected,
                stored: Some(width),
            } => write!(
                f,
                "the file stores the fixed[{expected}] column at a width of {width}"
            ),
            Self::FixedWidth {
                expected,
                stored: None,
            } => write!(
                f,
                "the file stores the fixed[{expected}] column as a BYTE_ARRAY"
            ),
            Self::Group => f.write_str("the file holds the name as a group of nested columns"),
        }
    }
}

/// Where one configured column is in one data file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Found {
    /// The column's leaf in the file's schema.
    Leaf(usize),
    /// The file records field ids and holds neither the column's name nor
    /// its field id: it was written before the column was added.
    Absent,
}

/// The configured column `name`, given field id `field_id` by the
/// snapshot's schema, in the file whose schema is `schema`, by RFD 023's
/// table: a column of that name is the column when its own field id is
/// `field_id` or it carries none in a file that records no field ids, and
/// a mismatch when it carries another, whether or not the file records
/// field ids; with no column of that name, `field_id` under another name
/// is a rename, neither in a file that records field ids is
/// `Found::Absent`, and a file that records none cannot tell. A file
/// records field ids when every top-level column carries one; one where
/// only some do records none. A column of that name is the one named
/// `name`, else the one named `avro_compatible(name)`, the name Iceberg's
/// Java writers give it in the file: a file may hold both, `a-b` beside a
/// sibling named `a_x2Db`, and the escaped form is the other column.
pub(crate) fn find(
    schema: &SchemaDescriptor,
    name: &str,
    field_id: i32,
) -> std::result::Result<Found, ColumnMismatch> {
    let fields = schema.root_schema().get_fields();
    let ids: Vec<Option<i32>> = fields.iter().map(|field| field_id_of(field)).collect();
    let records_ids = ids.iter().all(Option::is_some);
    let compatible = avro_compatible(name);
    let named = fields
        .iter()
        .position(|field| field.name() == name)
        .or_else(|| fields.iter().position(|field| field.name() == compatible));
    match named {
        Some(at) => match ids[at] {
            Some(found) if found != field_id => Err(ColumnMismatch::OtherFieldId {
                expected: field_id,
                found,
            }),
            Some(_) | None => leaf_of(schema, at)
                .map(Found::Leaf)
                .ok_or(ColumnMismatch::Group),
        },
        None if records_ids => match ids.iter().position(|&id| id == Some(field_id)) {
            Some(at) => Err(ColumnMismatch::Renamed {
                field_id,
                found: fields[at].name().to_string(),
            }),
            None => Ok(Found::Absent),
        },
        None => Err(ColumnMismatch::AbsentWithoutFieldIds),
    }
}

/// The prefix `avro_compatible` gives a character a name may not hold
/// where it is, before the character's UTF-16 code unit in uppercase hex.
const AVRO_ESCAPE: &str = "_x";

/// The prefix `avro_compatible` gives a digit that starts a name.
const AVRO_DIGIT_ESCAPE: char = '_';

/// `name` as Iceberg's Java writers name its column in a Parquet file:
/// `TypeToMessageType` names every field
/// `AvroSchemaUtil.makeCompatibleName(name)` (apache/iceberg,
/// core/src/main/java/org/apache/iceberg/avro/AvroSchemaUtil.java), which
/// keeps a name that starts with a letter or `_` and goes on in letters,
/// digits, and `_`, and otherwise replaces, per UTF-16 code unit, a
/// leading digit `d` with `_d` and every other unit outside those classes
/// with `_x` and the unit's uppercase hex: `user-id` is `user_x2Did`, `1st`
/// is `_1st`. A surrogate is never a letter or digit, as in Java. For
/// ASCII the classes are Java's exactly; beyond it they are Rust's
/// `char::is_alphabetic` and `char::is_numeric`, which differ from Java's
/// `Character.isLetter` and `isDigit` on a few characters.
fn avro_compatible(name: &str) -> Cow<'_, str> {
    let units: Vec<u16> = name.encode_utf16().collect();
    let kept = |at: usize, unit: u16| {
        char::from_u32(u32::from(unit)).is_some_and(|character| {
            character == '_' || character.is_alphabetic() || (at > 0 && character.is_numeric())
        })
    };
    if units.iter().enumerate().all(|(at, &unit)| kept(at, unit)) {
        return Cow::Borrowed(name);
    }
    let mut compatible = String::with_capacity(name.len());
    for (at, &unit) in units.iter().enumerate() {
        match char::from_u32(u32::from(unit)) {
            Some(character) if kept(at, unit) => compatible.push(character),
            Some(digit) if digit.is_numeric() => {
                compatible.push(AVRO_DIGIT_ESCAPE);
                compatible.push(digit);
            }
            Some(_) | None => write!(compatible, "{AVRO_ESCAPE}{unit:X}")
                .expect("writing to a String does not fail"),
        }
    }
    Cow::Owned(compatible)
}

/// The field id a top-level column of a Parquet schema carries, if any.
fn field_id_of(field: &ParquetType) -> Option<i32> {
    let info = field.get_basic_info();
    info.has_id().then(|| info.id())
}

/// The leaf of top-level column `at` of `schema`, `None` when that column is
/// a group: a primitive top-level column is exactly one leaf.
fn leaf_of(schema: &SchemaDescriptor, at: usize) -> Option<usize> {
    if !schema.root_schema().get_fields()[at].is_primitive() {
        return None;
    }
    (0..schema.num_columns()).find(|&leaf| schema.get_column_root_idx(leaf) == at)
}

/// The key and value columns of one data file under a layout.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FileColumns {
    /// The key column's leaf.
    pub(crate) key_leaf: usize,
    /// The value column's leaf; `None` for a file written before the
    /// column was added, which answers the layout's `value_default`.
    pub(crate) value_leaf: Option<usize>,
}

/// The key and value columns of the data file at `path`, whose schema is
/// `schema`, under `layout`, the snapshot's (`find`), or
/// `RefusalReason::ColumnMismatch` under `table` naming the column and the
/// file: a key column the file lacks (`ColumnMismatch::KeyAbsent`, since a
/// key has no default), any mismatch `find` gives, and a `fixed` key or
/// value column stored at another width (`check_width`).
pub(crate) fn resolve(
    table: &str,
    path: &str,
    schema: &SchemaDescriptor,
    layout: &TableLayout,
) -> Result<FileColumns> {
    let refuse = |column: &str, mismatch: ColumnMismatch| {
        Error::refused(
            table,
            RefusalReason::ColumnMismatch {
                column: column.to_string(),
                path: Some(path.to_string()),
                mismatch,
            },
        )
    };
    let key_leaf = match find(schema, &layout.key_column, layout.key_field_id) {
        Ok(Found::Leaf(leaf)) => {
            check_width(&schema.column(leaf), layout.key_width)
                .map_err(|mismatch| refuse(&layout.key_column, mismatch))?;
            leaf
        }
        Ok(Found::Absent) => return Err(refuse(&layout.key_column, ColumnMismatch::KeyAbsent)),
        Err(mismatch) => return Err(refuse(&layout.key_column, mismatch)),
    };
    let value_leaf = match find(schema, &layout.value_column, layout.value_field_id) {
        Ok(Found::Leaf(leaf)) => {
            check_width(&schema.column(leaf), layout.value_width)
                .map_err(|mismatch| refuse(&layout.value_column, mismatch))?;
            Some(leaf)
        }
        Ok(Found::Absent) => None,
        Err(mismatch) => return Err(refuse(&layout.value_column, mismatch)),
    };
    Ok(FileColumns {
        key_leaf,
        value_leaf,
    })
}

/// A `fixed` column of width `width` must be stored as a
/// `FIXED_LEN_BYTE_ARRAY` of that width, or a value would be served, or a
/// key compared, at another length than its type holds;
/// `ColumnMismatch::FixedWidth` otherwise. `None` (every other type)
/// admits any physical type here: a value column that is no byte array is
/// the file fault `FileFault::ValuePhysicalType` at inspection, and a key
/// column of the wrong physical type `RefusalReason::KeyTypeMismatch`.
fn check_width(
    descr: &ColumnDescriptor,
    width: Option<u64>,
) -> std::result::Result<(), ColumnMismatch> {
    let Some(expected) = width else {
        return Ok(());
    };
    match descr.physical_type() {
        PhysicalType::FIXED_LEN_BYTE_ARRAY
            if u64::try_from(descr.type_length()).is_ok_and(|stored| stored == expected) =>
        {
            Ok(())
        }
        PhysicalType::FIXED_LEN_BYTE_ARRAY => Err(ColumnMismatch::FixedWidth {
            expected,
            stored: Some(descr.type_length()),
        }),
        PhysicalType::BYTE_ARRAY => Err(ColumnMismatch::FixedWidth {
            expected,
            stored: None,
        }),
        PhysicalType::BOOLEAN
        | PhysicalType::INT32
        | PhysicalType::INT64
        | PhysicalType::INT96
        | PhysicalType::FLOAT
        | PhysicalType::DOUBLE => Ok(()),
    }
}

/// What a file written before the value column `field` was added answers
/// for it (RFD 023): the column's `initial-default` as the iceberg crate
/// parses it, as bytes (a binary or fixed default's bytes, a string's
/// UTF-8), or `None`, a null, when the schema gives none.
/// `ColumnMismatch::UnreadableDefault` when `unread`, a default the
/// metadata states that the crate did not read (`UnreadDefaults`), when
/// the crate read it as a literal that is not bytes or text, which a
/// binary, fixed, or string column's default never parses to, or when a
/// `fixed` column of `width` states a default of another length.
pub(crate) fn value_default(
    field: &NestedField,
    width: Option<u64>,
    unread: bool,
) -> std::result::Result<Option<Bytes>, ColumnMismatch> {
    if unread {
        return Err(ColumnMismatch::UnreadableDefault);
    }
    let bytes = match &field.initial_default {
        None => return Ok(None),
        Some(Literal::Primitive(PrimitiveLiteral::Binary(bytes))) => Bytes::copy_from_slice(bytes),
        Some(Literal::Primitive(PrimitiveLiteral::String(text))) => {
            Bytes::copy_from_slice(text.as_bytes())
        }
        Some(
            Literal::Primitive(
                PrimitiveLiteral::Boolean(_)
                | PrimitiveLiteral::Int(_)
                | PrimitiveLiteral::Long(_)
                | PrimitiveLiteral::Float(_)
                | PrimitiveLiteral::Double(_)
                | PrimitiveLiteral::Int128(_)
                | PrimitiveLiteral::UInt128(_)
                | PrimitiveLiteral::AboveMax
                | PrimitiveLiteral::BelowMin,
            )
            | Literal::Struct(_)
            | Literal::List(_)
            | Literal::Map(_),
        ) => return Err(ColumnMismatch::UnreadableDefault),
    };
    if width.is_some_and(|width| u64::try_from(bytes.len()).ok() != Some(width)) {
        return Err(ColumnMismatch::UnreadableDefault);
    }
    Ok(Some(bytes))
}

/// The top-level fields, by schema id and field id, whose metadata states
/// an `initial-default` that the parsed metadata does not hold. The iceberg
/// crate parses a default with `Literal::try_from_json(..).ok()`, so a
/// default it cannot read as the field's type becomes no default, which
/// would answer null for data the writer meant otherwise; only the raw JSON
/// tells the two apart. A default stated as JSON `null` is no default.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct UnreadDefaults(BTreeSet<(i32, i32)>);

impl UnreadDefaults {
    /// The unread defaults of `metadata`, which was parsed from `raw`: a
    /// second parse of the raw JSON that keeps each schema's top-level
    /// fields' ids and stated defaults alone. The schemas read are the
    /// ones the crate reads: the `schemas` list when the file holds it and
    /// a `current-schema-id`, else a v1 metadata file's single `schema`,
    /// whose `schema-id`, when it records none, is `DEFAULT_SCHEMA_ID`.
    pub(crate) fn of(raw: &[u8], metadata: &TableMetadata) -> Result<Self> {
        let stated: RawMetadata = serde_json::from_slice(raw)?;
        let schemas = match (stated.schemas, stated.current_schema_id, stated.schema) {
            (Some(schemas), Some(_), _) => schemas,
            (_, _, schema) => schema.into_iter().collect(),
        };
        let unread = schemas
            .iter()
            .flat_map(|schema| {
                schema
                    .fields
                    .iter()
                    .filter(|field| field.initial_default.is_some())
                    .map(|field| (schema.schema_id, field.id))
            })
            .filter(|&(schema_id, field_id)| {
                metadata
                    .schema_by_id(schema_id)
                    .and_then(|schema| schema.as_struct().field_by_id(field_id))
                    .is_none_or(|field| field.initial_default.is_none())
            })
            .collect();
        Ok(Self(unread))
    }

    /// Whether field `field_id` of schema `schema_id` states a default the
    /// parsed metadata does not hold.
    pub(crate) fn contains(&self, schema_id: i32, field_id: i32) -> bool {
        self.0.contains(&(schema_id, field_id))
    }
}

#[cfg(test)]
impl UnreadDefaults {
    /// The unread defaults of one field alone, for a test that needs a
    /// metadata whose default did not read without writing its JSON.
    pub(crate) fn naming(schema_id: i32, field_id: i32) -> Self {
        Self(BTreeSet::from([(schema_id, field_id)]))
    }
}

/// The part of a metadata file `UnreadDefaults::of` reads.
#[derive(serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
struct RawMetadata {
    #[serde(default)]
    schemas: Option<Vec<RawSchema>>,
    #[serde(default)]
    current_schema_id: Option<i32>,
    #[serde(default)]
    schema: Option<RawSchema>,
}

/// One schema's id and top-level fields.
#[derive(serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
struct RawSchema {
    #[serde(default = "default_schema_id")]
    schema_id: i32,
    #[serde(default)]
    fields: Vec<RawField>,
}

/// One top-level field's id and its stated default, `None` when absent or
/// JSON `null`.
#[derive(serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
struct RawField {
    id: i32,
    #[serde(default)]
    initial_default: Option<serde_json::Value>,
}

/// The id of a schema that records none, as the iceberg crate reads it.
fn default_schema_id() -> i32 {
    DEFAULT_SCHEMA_ID
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use iceberg::spec::{PrimitiveType, Type};
    use parquet::schema::parser::parse_message_type;

    use super::*;
    use crate::key::KeyType;
    use crate::manifest::testing::{self as manifest_testing, KEY_FIELD_ID, VALUE_FIELD_ID};

    /// The configured names and their field ids in the snapshot's schema.
    const KEY: &str = "key";
    const VALUE: &str = "value";
    /// A field id the snapshot's schema gives no column the tests name; the
    /// schemas below write it as `= 7`.
    const OTHER_FIELD_ID: i32 = 7;
    /// The field id of the third column of the schemas that hold one,
    /// written `= 3`.
    const THIRD_FIELD_ID: i32 = 3;
    /// The width of the `fixed` value column of the width tests.
    const FIXED_WIDTH: u64 = 8;
    /// A data file location for the refusals that name one.
    const PATH: &str = "mem://w/db/kv/data/f0.parquet";
    const TABLE: &str = "kv";

    fn schema_of(message: &str) -> SchemaDescriptor {
        SchemaDescriptor::new(Arc::new(parse_message_type(message).unwrap()))
    }

    fn found(
        message: &str,
        name: &str,
        field_id: i32,
    ) -> std::result::Result<Found, ColumnMismatch> {
        find(&schema_of(message), name, field_id)
    }

    #[test]
    fn test_find_name_with_the_schemas_field_id_is_served() {
        let message = "message m { required int64 key = 1; optional binary value = 2; }";
        assert_eq!(found(message, VALUE, VALUE_FIELD_ID), Ok(Found::Leaf(1)));
        assert_eq!(found(message, KEY, KEY_FIELD_ID), Ok(Found::Leaf(0)));
    }

    #[test]
    fn test_find_name_with_another_field_id_is_refused() {
        let message = "message m { required int64 key = 1; optional binary value = 3; }";
        assert_eq!(
            found(message, VALUE, VALUE_FIELD_ID),
            Err(ColumnMismatch::OtherFieldId {
                expected: VALUE_FIELD_ID,
                found: THIRD_FIELD_ID,
            })
        );
    }

    #[test]
    fn test_find_field_id_under_another_name_is_a_rename() {
        let message = "message m { required int64 key = 1; optional binary payload = 2; }";
        assert_eq!(
            found(message, VALUE, VALUE_FIELD_ID),
            Err(ColumnMismatch::Renamed {
                field_id: VALUE_FIELD_ID,
                found: "payload".to_string(),
            })
        );
    }

    #[test]
    fn test_find_neither_in_a_file_with_field_ids_is_absent() {
        let message = "message m { required int64 key = 1; optional binary value = 2; }";
        assert_eq!(found(message, "added", OTHER_FIELD_ID), Ok(Found::Absent));
    }

    /// A file that records no field ids.
    const WITHOUT_FIELD_IDS: &str = "message m { required int64 key; optional binary value; }";

    #[test]
    fn test_find_without_field_ids_serves_the_name_unchecked() {
        assert_eq!(
            found(WITHOUT_FIELD_IDS, VALUE, OTHER_FIELD_ID),
            Ok(Found::Leaf(1))
        );
    }

    #[test]
    fn test_find_without_field_ids_refuses_an_absent_name() {
        assert_eq!(
            found(WITHOUT_FIELD_IDS, "added", OTHER_FIELD_ID),
            Err(ColumnMismatch::AbsentWithoutFieldIds)
        );
    }

    #[test]
    fn test_find_field_ids_on_only_some_columns_records_none() {
        let message = "message m { required int64 key = 1; optional binary value; }";
        assert_eq!(
            found(message, "added", OTHER_FIELD_ID),
            Err(ColumnMismatch::AbsentWithoutFieldIds)
        );
        assert_eq!(found(message, VALUE, OTHER_FIELD_ID), Ok(Found::Leaf(1)));
    }

    #[test]
    fn test_find_a_partial_id_file_refuses_a_matched_column_whose_own_id_differs() {
        let message = "message m { required int64 key = 1; optional binary value; }";
        assert_eq!(found(message, KEY, KEY_FIELD_ID), Ok(Found::Leaf(0)));
        assert_eq!(
            found(message, KEY, OTHER_FIELD_ID),
            Err(ColumnMismatch::OtherFieldId {
                expected: OTHER_FIELD_ID,
                found: KEY_FIELD_ID,
            })
        );
        let message = "message m { required int64 key; optional binary value = 3; }";
        assert_eq!(
            found(message, VALUE, VALUE_FIELD_ID),
            Err(ColumnMismatch::OtherFieldId {
                expected: VALUE_FIELD_ID,
                found: THIRD_FIELD_ID,
            })
        );
        assert_eq!(found(message, KEY, KEY_FIELD_ID), Ok(Found::Leaf(0)));
    }

    #[test]
    fn test_find_matches_top_level_names_and_ids_alone() {
        let message = "message m { required int64 key = 1; optional group outer = 3 { optional binary value = 2; } }";
        // The nested `value` carries the field id, but only top-level
        // columns are matched: the file records ids and holds neither.
        assert_eq!(found(message, VALUE, VALUE_FIELD_ID), Ok(Found::Absent));
        assert_eq!(
            found(message, "outer", THIRD_FIELD_ID),
            Err(ColumnMismatch::Group)
        );
    }

    #[test]
    fn test_avro_compatible_escapes_as_iceberg_java_writers_name_columns() {
        for (name, compatible) in [
            ("user_id", "user_id"),
            ("_1st", "_1st"),
            ("user-id", "user_x2Did"),
            ("1st", "_1st"),
            ("a.b c", "a_x2Eb_x20c"),
            ("día", "día"),
            ("😀", "_xD83D_xDE00"),
        ] {
            assert_eq!(avro_compatible(name), compatible, "{name}");
        }
    }

    #[test]
    fn test_find_matches_the_name_an_iceberg_java_writer_gives_the_column() {
        let message = "message m { required int64 _1key = 1; optional binary user_x2Did = 2; }";
        assert_eq!(
            found(message, "user-id", VALUE_FIELD_ID),
            Ok(Found::Leaf(1))
        );
        assert_eq!(found(message, "1key", KEY_FIELD_ID), Ok(Found::Leaf(0)));
        assert_eq!(
            found(message, "user-id", OTHER_FIELD_ID),
            Err(ColumnMismatch::OtherFieldId {
                expected: OTHER_FIELD_ID,
                found: VALUE_FIELD_ID,
            })
        );
    }

    #[test]
    fn test_find_prefers_the_exact_name_over_an_avro_escaped_sibling() {
        /// The field ids of `a-b` and of its sibling `a_x2Db`, written `= 5`
        /// and `= 6`; the file lists the sibling first.
        const EXACT_FIELD_ID: i32 = 5;
        const ESCAPED_FIELD_ID: i32 = 6;
        let with_ids = "message m { required int64 key = 1; optional binary a_x2Db = 6; optional binary a-b = 5; }";
        assert_eq!(found(with_ids, "a-b", EXACT_FIELD_ID), Ok(Found::Leaf(2)));
        assert_eq!(
            found(with_ids, "a_x2Db", ESCAPED_FIELD_ID),
            Ok(Found::Leaf(1))
        );
        let without_ids =
            "message m { required int64 key; optional binary a_x2Db; optional binary a-b; }";
        assert_eq!(
            found(without_ids, "a-b", EXACT_FIELD_ID),
            Ok(Found::Leaf(2))
        );
        assert_eq!(
            found(without_ids, "a_x2Db", ESCAPED_FIELD_ID),
            Ok(Found::Leaf(1))
        );
    }

    #[test]
    fn test_find_leaf_counts_the_leaves_of_earlier_groups() {
        let message = "message m { optional group outer = 3 { optional binary a = 4; optional binary b = 5; } required int64 key = 1; optional binary value = 2; }";
        assert_eq!(found(message, KEY, KEY_FIELD_ID), Ok(Found::Leaf(2)));
        assert_eq!(found(message, VALUE, VALUE_FIELD_ID), Ok(Found::Leaf(3)));
    }

    /// A layout over the key and value columns, the value `fixed` of
    /// `width` when given.
    fn layout(width: Option<u64>) -> TableLayout {
        TableLayout {
            value_width: width,
            ..manifest_testing::layout(KeyType::Long, None)
        }
    }

    fn mismatch_of(result: Result<FileColumns>) -> (String, Option<String>, ColumnMismatch) {
        match result {
            Err(Error::Refused {
                reason:
                    RefusalReason::ColumnMismatch {
                        column,
                        path,
                        mismatch,
                    },
                ..
            }) => (column, path, mismatch),
            other => panic!("expected a column mismatch, got {other:?}"),
        }
    }

    #[test]
    fn test_resolve_a_file_without_its_key_column_is_refused() {
        let schema = schema_of("message m { optional binary value = 2; }");
        assert_eq!(
            mismatch_of(resolve(TABLE, PATH, &schema, &layout(None))),
            (
                KEY.to_string(),
                Some(PATH.to_string()),
                ColumnMismatch::KeyAbsent
            )
        );
        let schema = schema_of("message m { optional binary value; }");
        assert_eq!(
            mismatch_of(resolve(TABLE, PATH, &schema, &layout(None))).2,
            ColumnMismatch::AbsentWithoutFieldIds
        );
    }

    #[test]
    fn test_resolve_a_value_column_the_file_lacks_answers_the_default() {
        let schema = schema_of("message m { required int64 key = 1; optional binary other = 7; }");
        assert_eq!(
            resolve(TABLE, PATH, &schema, &layout(None)).unwrap(),
            FileColumns {
                key_leaf: 0,
                value_leaf: None,
            }
        );
    }

    #[test]
    fn test_resolve_a_fixed_value_column_at_another_width_is_refused() {
        let at_width = schema_of(
            "message m { required int64 key = 1; optional fixed_len_byte_array(8) value = 2; }",
        );
        assert_eq!(
            resolve(TABLE, PATH, &at_width, &layout(Some(FIXED_WIDTH)))
                .unwrap()
                .value_leaf,
            Some(1)
        );
        let narrower = schema_of(
            "message m { required int64 key = 1; optional fixed_len_byte_array(4) value = 2; }",
        );
        assert_eq!(
            mismatch_of(resolve(TABLE, PATH, &narrower, &layout(Some(FIXED_WIDTH)))).2,
            ColumnMismatch::FixedWidth {
                expected: FIXED_WIDTH,
                stored: Some(4),
            }
        );
        let variable =
            schema_of("message m { required int64 key = 1; optional binary value = 2; }");
        assert_eq!(
            mismatch_of(resolve(TABLE, PATH, &variable, &layout(Some(FIXED_WIDTH)))).2,
            ColumnMismatch::FixedWidth {
                expected: FIXED_WIDTH,
                stored: None,
            }
        );
        // A binary column admits a fixed-width byte array.
        assert_eq!(
            resolve(TABLE, PATH, &narrower, &layout(None))
                .unwrap()
                .value_leaf,
            Some(1)
        );
    }

    #[test]
    fn test_resolve_a_fixed_key_column_at_another_width_is_refused() {
        let layout = TableLayout {
            key_width: Some(FIXED_WIDTH),
            ..manifest_testing::layout(KeyType::Binary, None)
        };
        let at_width = schema_of(
            "message m { required fixed_len_byte_array(8) key = 1; optional binary value = 2; }",
        );
        assert_eq!(
            resolve(TABLE, PATH, &at_width, &layout).unwrap().key_leaf,
            0
        );
        let narrower = schema_of(
            "message m { required fixed_len_byte_array(4) key = 1; optional binary value = 2; }",
        );
        assert_eq!(
            mismatch_of(resolve(TABLE, PATH, &narrower, &layout)),
            (
                KEY.to_string(),
                Some(PATH.to_string()),
                ColumnMismatch::FixedWidth {
                    expected: FIXED_WIDTH,
                    stored: Some(4),
                }
            )
        );
    }

    fn field_with_default(ty: PrimitiveType, default: Option<Literal>) -> NestedField {
        let field = NestedField::optional(VALUE_FIELD_ID, VALUE, Type::Primitive(ty));
        match default {
            Some(literal) => field.with_initial_default(literal),
            None => field,
        }
    }

    #[test]
    fn test_value_default_is_the_initial_default_as_bytes_or_null() {
        let bytes = vec![1, 2, 3];
        let fixed = vec![7_u8; crate::approx::length(FIXED_WIDTH)];
        assert_eq!(
            value_default(
                &field_with_default(PrimitiveType::Binary, None),
                None,
                false
            ),
            Ok(None)
        );
        assert_eq!(
            value_default(
                &field_with_default(PrimitiveType::Binary, Some(Literal::binary(bytes.clone()))),
                None,
                false
            ),
            Ok(Some(Bytes::from(bytes)))
        );
        assert_eq!(
            value_default(
                &field_with_default(
                    PrimitiveType::Fixed(FIXED_WIDTH),
                    Some(Literal::fixed(fixed.clone()))
                ),
                Some(FIXED_WIDTH),
                false
            ),
            Ok(Some(Bytes::from(fixed)))
        );
        assert_eq!(
            value_default(
                &field_with_default(PrimitiveType::String, Some(Literal::string("none"))),
                None,
                false
            ),
            Ok(Some(Bytes::from_static(b"none")))
        );
    }

    #[test]
    fn test_value_default_refuses_an_unread_default_and_one_not_of_the_type() {
        assert_eq!(
            value_default(&field_with_default(PrimitiveType::Binary, None), None, true),
            Err(ColumnMismatch::UnreadableDefault)
        );
        assert_eq!(
            value_default(
                &field_with_default(PrimitiveType::Binary, Some(Literal::long(1))),
                None,
                false
            ),
            Err(ColumnMismatch::UnreadableDefault)
        );
        let narrower = vec![7_u8; crate::approx::length(FIXED_WIDTH) - 1];
        assert_eq!(
            value_default(
                &field_with_default(
                    PrimitiveType::Fixed(FIXED_WIDTH),
                    Some(Literal::fixed(narrower))
                ),
                Some(FIXED_WIDTH),
                false
            ),
            Err(ColumnMismatch::UnreadableDefault)
        );
    }

    /// `metadata` serialized, with the value field of every schema given
    /// `default` as its stated `initial-default`.
    fn with_stated_default(metadata: &TableMetadata, default: &serde_json::Value) -> Vec<u8> {
        let mut json = serde_json::to_value(metadata).unwrap();
        for schema in json["schemas"].as_array_mut().unwrap() {
            state_default(schema, default);
        }
        serde_json::to_vec(&json).unwrap()
    }

    /// `schema` with its value field given `default` as its stated
    /// `initial-default`.
    fn state_default(schema: &mut serde_json::Value, default: &serde_json::Value) {
        for field in schema["fields"].as_array_mut().unwrap() {
            if field["id"] == VALUE_FIELD_ID {
                field["initial-default"] = default.clone();
            }
        }
    }

    /// A v1 metadata file may hold its one schema as `schema` alone, or a
    /// `schemas` list beside a `schema` the list supersedes; the schemas
    /// read are the ones the crate reads.
    #[test]
    fn test_unread_defaults_reads_the_schemas_the_crate_reads_in_v1_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let metadata = manifest_testing::metadata_of_version(
            &manifest_testing::file_root(dir.path()),
            manifest_testing::long_schema(),
            manifest_testing::unpartitioned(),
            iceberg::spec::FormatVersion::V1,
        );
        let schema_id = metadata.current_schema_id();
        let unread = serde_json::json!("not hex");
        let json = serde_json::to_value(&metadata).unwrap();
        let schema = json["schemas"][0].clone();

        let mut single = json.clone();
        let object = single.as_object_mut().unwrap();
        object.remove("schemas");
        object.remove("current-schema-id");
        object.insert("schema".to_string(), schema.clone());
        state_default(&mut single["schema"], &unread);
        let raw = serde_json::to_vec(&single).unwrap();
        let parsed: TableMetadata = serde_json::from_slice(&raw).unwrap();
        assert_eq!(
            UnreadDefaults::of(&raw, &parsed).unwrap(),
            UnreadDefaults::naming(schema_id, VALUE_FIELD_ID)
        );

        let mut stale = json;
        stale
            .as_object_mut()
            .unwrap()
            .insert("schema".to_string(), schema);
        state_default(&mut stale["schema"], &unread);
        let raw = serde_json::to_vec(&stale).unwrap();
        let parsed: TableMetadata = serde_json::from_slice(&raw).unwrap();
        assert_eq!(
            UnreadDefaults::of(&raw, &parsed).unwrap(),
            UnreadDefaults::default(),
            "the crate reads the list, so the stale schema states nothing"
        );
    }

    #[test]
    fn test_unread_defaults_names_a_stated_default_the_crate_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let metadata = manifest_testing::metadata_of_version(
            &manifest_testing::file_root(dir.path()),
            manifest_testing::long_schema(),
            manifest_testing::unpartitioned(),
            iceberg::spec::FormatVersion::V3,
        );
        let schema_id = metadata.current_schema_id();
        for (stated, unread) in [
            // Not hex: the crate drops it.
            (serde_json::json!("not hex"), true),
            (serde_json::json!(7), true),
            // Hex for the bytes 0x01 0xab, which the crate reads.
            (serde_json::json!("01ab"), false),
            // A null default is no default.
            (serde_json::Value::Null, false),
        ] {
            let raw = with_stated_default(&metadata, &stated);
            let parsed: TableMetadata = serde_json::from_slice(&raw).unwrap();
            let defaults = UnreadDefaults::of(&raw, &parsed).unwrap();
            assert_eq!(
                defaults.contains(schema_id, VALUE_FIELD_ID),
                unread,
                "{stated}"
            );
            assert!(!defaults.contains(schema_id, KEY_FIELD_ID));
        }
        let plain = serde_json::to_vec(&metadata).unwrap();
        assert_eq!(
            UnreadDefaults::of(&plain, &metadata).unwrap(),
            UnreadDefaults::default()
        );
    }
}
