//! The contract's wire behavior: every message survives an encode/decode
//! round trip with every field set, the bytes key variant, a column value,
//! a request naming columns and fields, a present answer, and a value
//! column with its format each have one fixed encoding a client in another
//! language can be checked against, each enum value maps to the integer
//! `icetray.proto` declares, and the service carries its full name.

use std::fmt::Debug;

use icetray_proto::v1::icetray_server::SERVICE_NAME;
use icetray_proto::v1::{
    ColumnRead, ColumnValue, GetRequest, GetResponse, Hit, Key, KeyType, MultiGetRequest,
    MultiGetResponse, NullValue, Presence, TableInfoRequest, TableInfoResponse, ValueColumn,
    ValueFormat, ValueType, column_value, key,
};
use prost::bytes::Bytes;
use prost::{Message, UnknownEnumValue};

/// The name the fixture table is served under.
const TABLE: &str = "orders_by_id";

/// A negative long key: it takes the full ten varint bytes, so every byte
/// of the encoding is exercised.
const LONG_KEY: i64 = -17;

/// A string key with non-ASCII characters, so its bytes are not a plain
/// ASCII run.
const STR_KEY: &str = "clé-ünïcode";

/// A bytes key that is neither valid UTF-8 nor a varint, so a wrong variant
/// could not decode it by accident.
const BIN_KEY: &[u8] = &[0xde, 0xad, 0xbe, 0xef];

/// The tag byte that opens `Key.bin` on the wire: field number 3, wire
/// type 2 (length-delimited), encoded as `(3 << 3) | 2`.
const KEY_BIN_TAG: u8 = 0x1a;

/// The length prefix that follows the tag for `BIN_KEY`: its byte count,
/// which fits in one varint byte.
const BIN_KEY_LEN: u8 = 4;

/// A value's bytes; not valid UTF-8, so the value is proven opaque.
const VALUE: &[u8] = &[0x00, 0xff, 0x10];

/// The value columns the fixture requests name, in request order.
const COLUMNS: [&str; 2] = ["payload", "embedding"];

/// The field numbers the fixture request keeps of its second column, the
/// last over 127 so its varint takes two bytes.
const FIELDS: [u32; 3] = [1, 4, 300];

/// `FIELDS` packed as varints, the payload of `ColumnRead.fields`: 300 is
/// `0b10_0101100`, its low seven bits with the continuation bit, then 2.
const FIELDS_PACKED: [u8; 4] = [0x01, 0x04, 0xac, 0x02];

/// The width of the fixture table's `fixed` value column; under 128, so
/// its varint is the one byte of its value.
const FIXED_WIDTH: u32 = 16;

/// The tag byte that opens a length-delimited field 1 (wire type 2),
/// encoded as `(1 << 3) | 2`: `ColumnValue.value` and `ValueColumn.name`.
const FIELD_1_LEN_TAG: u8 = 0x0a;

/// The tag byte that opens a length-delimited field 2, `(2 << 3) | 2`:
/// `ColumnValue.null_value` and `GetResponse.values`.
const FIELD_2_LEN_TAG: u8 = 0x12;

/// The tag byte that opens a length-delimited field 3, `(3 << 3) | 2`:
/// each name of `GetRequest.columns`.
const FIELD_3_LEN_TAG: u8 = 0x1a;

/// The tag byte that opens a length-delimited field 8, `(8 << 3) | 2`:
/// each of `TableInfoResponse.value_columns`.
const FIELD_8_LEN_TAG: u8 = 0x42;

/// The tag byte that opens a varint field 1 (wire type 0), `1 << 3`:
/// `GetResponse.presence`.
const FIELD_1_VARINT_TAG: u8 = 0x08;

/// The tag byte that opens a varint field 2, `2 << 3`:
/// `ValueColumn.value_type`.
const FIELD_2_VARINT_TAG: u8 = 0x10;

/// The tag byte that opens a varint field 3, `3 << 3`:
/// `GetResponse.snapshot_id` and `ValueColumn.width`.
const FIELD_3_VARINT_TAG: u8 = 0x18;

/// The tag byte that opens a varint field 4, `4 << 3`:
/// `ValueColumn.format`.
const FIELD_4_VARINT_TAG: u8 = 0x20;

/// A snapshot id under 128, whose varint is the one byte of its value.
const SMALL_SNAPSHOT_ID: u8 = 7;

/// The length prefix of an empty message or of zero bytes.
const EMPTY_LEN: u8 = 0;

/// The largest snapshot id the field carries; a lost high bit would show.
const SNAPSHOT_ID: i64 = i64::MAX;

/// A bucket count for a bucket-partitioned fixture table.
const BUCKETS: u32 = 16;

/// A data file count for the fixture table.
const DATA_FILES: u64 = 3;

/// A row count for the fixture table.
const RECORDS: u64 = 1_000_000;

/// A resident index size for the fixture table.
const INDEX_BYTES: u64 = 65_536;

/// Every `Presence` value beside the integer `icetray.proto` gives it: what
/// a client in another language puts on the wire.
const PRESENCE_NUMBERS: &[(Presence, i32)] = &[
    (Presence::Unspecified, 0),
    (Presence::Present, 1),
    (Presence::Absent, 2),
];

/// Every `ValueType` value beside the integer `icetray.proto` gives it.
const VALUE_TYPE_NUMBERS: &[(ValueType, i32)] = &[
    (ValueType::Unspecified, 0),
    (ValueType::Binary, 1),
    (ValueType::Fixed, 2),
    (ValueType::String, 3),
];

/// Every `ValueFormat` value beside the integer `icetray.proto` gives it.
const VALUE_FORMAT_NUMBERS: &[(ValueFormat, i32)] = &[
    (ValueFormat::Unspecified, 0),
    (ValueFormat::Opaque, 1),
    (ValueFormat::Protobuf, 2),
];

/// Every `KeyType` value beside the integer `icetray.proto` gives it.
const KEY_TYPE_NUMBERS: &[(KeyType, i32)] = &[
    (KeyType::Unspecified, 0),
    (KeyType::Long, 1),
    (KeyType::String, 2),
    (KeyType::Binary, 3),
    (KeyType::Uuid, 4),
];

/// The full name of the service as it appears in a gRPC request path.
const SERVICE_FULL_NAME: &str = "icetray.v1.Icetray";

/// Encodes a message and decodes the bytes back: what a peer would read.
fn round_trip<M: Message + Default>(message: &M) -> M {
    M::decode(message.encode_to_vec().as_slice())
        .expect("bytes this message type encoded decode under the same type")
}

/// Asserts that a peer reads back exactly the message that was sent.
fn assert_round_trips<M: Message + Default + PartialEq + Debug>(message: &M) {
    assert_eq!(round_trip(message), *message);
}

/// The key a lookup sends to a table whose key type is `KEY_TYPE_LONG`.
fn long_key() -> Key {
    Key {
        key: Some(key::Key::Long(LONG_KEY)),
    }
}

/// The key a lookup sends to a table whose key type is `KEY_TYPE_STRING`.
fn str_key() -> Key {
    Key {
        key: Some(key::Key::Str(STR_KEY.to_owned())),
    }
}

/// The key a lookup sends to a table whose key type is `KEY_TYPE_BINARY` or
/// `KEY_TYPE_UUID`.
fn bin_key() -> Key {
    Key {
        key: Some(key::Key::Bin(BIN_KEY.to_vec())),
    }
}

/// The exact bytes of `bin_key()` on the wire: the tag, the length, the
/// key's bytes.
fn key_bin_wire() -> Vec<u8> {
    let mut wire = vec![KEY_BIN_TAG, BIN_KEY_LEN];
    wire.extend_from_slice(BIN_KEY);
    wire
}

/// The request's column reads: the first column whole, the second trimmed
/// to `FIELDS`.
fn columns() -> Vec<ColumnRead> {
    vec![
        ColumnRead {
            name: COLUMNS[0].to_owned(),
            fields: Vec::new(),
        },
        ColumnRead {
            name: COLUMNS[1].to_owned(),
            fields: FIELDS.to_vec(),
        },
    ]
}

/// A present value of `bytes`.
fn bytes_value(bytes: &'static [u8]) -> ColumnValue {
    ColumnValue {
        kind: Some(column_value::Kind::Value(Bytes::from_static(bytes))),
    }
}

/// A null value.
fn null_value() -> ColumnValue {
    ColumnValue {
        kind: Some(column_value::Kind::NullValue(NullValue {})),
    }
}

/// The values a present answer to `COLUMNS` carries: the value's bytes,
/// then a null.
fn values() -> Vec<ColumnValue> {
    vec![bytes_value(VALUE), null_value()]
}

/// One integer past the highest value in a mapping table: an integer no
/// variant has.
fn unknown_number<E: Copy>(numbers: &[(E, i32)]) -> i32 {
    numbers
        .iter()
        .map(|&(_, number)| number)
        .max()
        .expect("a mapping table names at least one value")
        + 1
}

#[test]
fn test_round_trip_key_long() {
    assert_round_trips(&long_key());
}

#[test]
fn test_round_trip_key_str() {
    assert_round_trips(&str_key());
}

#[test]
fn test_round_trip_key_bin() {
    assert_round_trips(&bin_key());
}

#[test]
fn test_round_trip_get_request_all_fields() {
    assert_round_trips(&GetRequest {
        table: TABLE.to_owned(),
        key: Some(bin_key()),
        columns: columns(),
    });
}

#[test]
fn test_round_trip_get_response_every_presence() {
    for &(presence, _) in PRESENCE_NUMBERS {
        let response = GetResponse {
            presence: i32::from(presence),
            values: values(),
            snapshot_id: SNAPSHOT_ID,
        };
        assert_round_trips(&response);
        assert_eq!(round_trip(&response).presence(), presence);
    }
}

#[test]
fn test_round_trip_multi_get_request_every_key_variant() {
    assert_round_trips(&MultiGetRequest {
        table: TABLE.to_owned(),
        keys: vec![long_key(), str_key(), bin_key()],
        columns: columns(),
    });
}

#[test]
fn test_round_trip_hit_every_presence() {
    for &(presence, _) in PRESENCE_NUMBERS {
        let hit = Hit {
            presence: i32::from(presence),
            values: values(),
        };
        assert_round_trips(&hit);
        assert_eq!(round_trip(&hit).presence(), presence);
    }
}

#[test]
fn test_round_trip_multi_get_response_every_presence() {
    let results = PRESENCE_NUMBERS
        .iter()
        .map(|&(presence, _)| Hit {
            presence: i32::from(presence),
            values: values(),
        })
        .collect();
    let response = MultiGetResponse {
        results,
        snapshot_id: SNAPSHOT_ID,
    };
    assert_round_trips(&response);
    assert_eq!(round_trip(&response).results.len(), PRESENCE_NUMBERS.len());
}

#[test]
fn test_round_trip_table_info_request_all_fields() {
    assert_round_trips(&TableInfoRequest {
        table: TABLE.to_owned(),
    });
}

#[test]
fn test_round_trip_table_info_response_every_key_type() {
    for &(key_type, _) in KEY_TYPE_NUMBERS {
        let response = TableInfoResponse {
            table: TABLE.to_owned(),
            snapshot_id: SNAPSHOT_ID,
            buckets: BUCKETS,
            data_files: DATA_FILES,
            records: RECORDS,
            key_type: i32::from(key_type),
            index_bytes: INDEX_BYTES,
            value_columns: VALUE_TYPE_NUMBERS
                .iter()
                .map(|&(value_type, _)| ValueColumn {
                    name: value_type.as_str_name().to_owned(),
                    value_type: i32::from(value_type),
                    width: if value_type == ValueType::Fixed {
                        FIXED_WIDTH
                    } else {
                        0
                    },
                    format: i32::from(if value_type == ValueType::Binary {
                        ValueFormat::Protobuf
                    } else {
                        ValueFormat::Opaque
                    }),
                })
                .collect(),
        };
        assert_round_trips(&response);
        assert_eq!(round_trip(&response).key_type(), key_type);
    }
}

#[test]
fn test_encode_key_bin_exact_bytes() {
    assert_eq!(usize::from(BIN_KEY_LEN), BIN_KEY.len());
    assert_eq!(bin_key().encode_to_vec(), key_bin_wire());
}

#[test]
fn test_decode_key_bin_exact_bytes() {
    assert_eq!(Key::decode(key_bin_wire().as_slice()).unwrap(), bin_key());
}

#[test]
fn test_presence_as_i32_matches_proto() {
    for &(presence, number) in PRESENCE_NUMBERS {
        assert_eq!(presence as i32, number);
        assert_eq!(Presence::try_from(number), Ok(presence));
    }
}

#[test]
fn test_presence_try_from_unknown_is_err() {
    let unknown = unknown_number(PRESENCE_NUMBERS);
    assert_eq!(Presence::try_from(unknown), Err(UnknownEnumValue(unknown)));
}

#[test]
fn test_key_type_as_i32_matches_proto() {
    for &(key_type, number) in KEY_TYPE_NUMBERS {
        assert_eq!(key_type as i32, number);
        assert_eq!(KeyType::try_from(number), Ok(key_type));
    }
}

#[test]
fn test_key_type_try_from_unknown_is_err() {
    let unknown = unknown_number(KEY_TYPE_NUMBERS);
    assert_eq!(KeyType::try_from(unknown), Err(UnknownEnumValue(unknown)));
}

#[test]
fn test_value_type_as_i32_matches_proto() {
    for &(value_type, number) in VALUE_TYPE_NUMBERS {
        assert_eq!(value_type as i32, number);
        assert_eq!(ValueType::try_from(number), Ok(value_type));
    }
}

#[test]
fn test_value_type_try_from_unknown_is_err() {
    let unknown = unknown_number(VALUE_TYPE_NUMBERS);
    assert_eq!(ValueType::try_from(unknown), Err(UnknownEnumValue(unknown)));
}

#[test]
fn test_value_format_as_i32_matches_proto() {
    for &(format, number) in VALUE_FORMAT_NUMBERS {
        assert_eq!(format as i32, number);
        assert_eq!(ValueFormat::try_from(number), Ok(format));
    }
}

#[test]
fn test_value_format_try_from_unknown_is_err() {
    let unknown = unknown_number(VALUE_FORMAT_NUMBERS);
    assert_eq!(
        ValueFormat::try_from(unknown),
        Err(UnknownEnumValue(unknown))
    );
}

/// A null and a value of zero length are distinct on the wire and read back
/// as what was sent: a null is the empty `null_value` message, a
/// zero-length value is `value` holding nothing, and a value with neither
/// variant set
/// is the empty message, which the server never sends.
#[test]
fn test_encode_column_value_null_and_zero_length_bytes_differ() {
    let null = null_value();
    let empty = bytes_value(&[]);
    assert_eq!(null.encode_to_vec(), [FIELD_2_LEN_TAG, EMPTY_LEN]);
    assert_eq!(empty.encode_to_vec(), [FIELD_1_LEN_TAG, EMPTY_LEN]);
    assert_round_trips(&null);
    assert_round_trips(&empty);
    assert_ne!(round_trip(&null), round_trip(&empty));
    assert!(ColumnValue { kind: None }.encode_to_vec().is_empty());
}

/// `bytes` length-delimited under `tag`: the tag, the one-byte length,
/// the bytes.
fn delimited(tag: u8, bytes: &[u8]) -> Vec<u8> {
    let mut wire = vec![
        tag,
        u8::try_from(bytes.len()).expect("a fixture field is under 128 bytes, a one-byte length"),
    ];
    wire.extend_from_slice(bytes);
    wire
}

/// A request naming columns carries each column read, in request order, as
/// its own field 3: the name as field 1 and, when fields are named, the
/// numbers packed as field 2; a whole read carries its name alone.
#[test]
fn test_encode_get_request_columns_exact_bytes() {
    let request = GetRequest {
        columns: columns(),
        ..GetRequest::default()
    };
    let whole = delimited(FIELD_1_LEN_TAG, COLUMNS[0].as_bytes());
    let trimmed = [
        delimited(FIELD_1_LEN_TAG, COLUMNS[1].as_bytes()),
        delimited(FIELD_2_LEN_TAG, &FIELDS_PACKED),
    ]
    .concat();
    let wire = [
        delimited(FIELD_3_LEN_TAG, &whole),
        delimited(FIELD_3_LEN_TAG, &trimmed),
    ]
    .concat();
    assert_eq!(request.encode_to_vec(), wire);
    assert_eq!(GetRequest::decode(wire.as_slice()).unwrap(), request);
}

/// A present answer carries its presence, then each value as its own
/// field 2 in request order, bytes and null alike, then the snapshot id.
#[test]
fn test_encode_get_response_present_with_bytes_and_null_exact_bytes() {
    let response = GetResponse {
        presence: i32::from(Presence::Present),
        values: values(),
        snapshot_id: i64::from(SMALL_SNAPSHOT_ID),
    };
    let present = u8::try_from(i32::from(Presence::Present)).unwrap();
    let wire = [
        vec![FIELD_1_VARINT_TAG, present],
        delimited(FIELD_2_LEN_TAG, &delimited(FIELD_1_LEN_TAG, VALUE)),
        delimited(FIELD_2_LEN_TAG, &[FIELD_2_LEN_TAG, EMPTY_LEN]),
        vec![FIELD_3_VARINT_TAG, SMALL_SNAPSHOT_ID],
    ]
    .concat();
    assert_eq!(response.encode_to_vec(), wire);
    assert_eq!(GetResponse::decode(wire.as_slice()).unwrap(), response);
}

/// Value columns in `TableInfoResponse`, each as field 8: a `fixed` one
/// carries its name, its type, its width, and its format; a `binary`
/// protobuf one its name, its type, and its format, the zero width left
/// out.
#[test]
fn test_encode_table_info_value_column_exact_bytes() {
    let response = TableInfoResponse {
        value_columns: vec![
            ValueColumn {
                name: COLUMNS[1].to_owned(),
                value_type: i32::from(ValueType::Fixed),
                width: FIXED_WIDTH,
                format: i32::from(ValueFormat::Opaque),
            },
            ValueColumn {
                name: COLUMNS[0].to_owned(),
                value_type: i32::from(ValueType::Binary),
                width: 0,
                format: i32::from(ValueFormat::Protobuf),
            },
        ],
        ..TableInfoResponse::default()
    };
    let number = |value: i32| u8::try_from(value).unwrap();
    let fixed = [
        delimited(FIELD_1_LEN_TAG, COLUMNS[1].as_bytes()),
        vec![FIELD_2_VARINT_TAG, number(i32::from(ValueType::Fixed))],
        vec![FIELD_3_VARINT_TAG, u8::try_from(FIXED_WIDTH).unwrap()],
        vec![FIELD_4_VARINT_TAG, number(i32::from(ValueFormat::Opaque))],
    ]
    .concat();
    let protobuf = [
        delimited(FIELD_1_LEN_TAG, COLUMNS[0].as_bytes()),
        vec![FIELD_2_VARINT_TAG, number(i32::from(ValueType::Binary))],
        vec![FIELD_4_VARINT_TAG, number(i32::from(ValueFormat::Protobuf))],
    ]
    .concat();
    let wire = [
        delimited(FIELD_8_LEN_TAG, &fixed),
        delimited(FIELD_8_LEN_TAG, &protobuf),
    ]
    .concat();
    assert_eq!(response.encode_to_vec(), wire);
    assert_eq!(
        TableInfoResponse::decode(wire.as_slice()).unwrap(),
        response
    );
}

#[test]
fn test_service_name_is_full_name() {
    assert_eq!(SERVICE_NAME, SERVICE_FULL_NAME);
}
