//! The contract's wire behavior: every message survives an encode/decode
//! round trip with every field set, the bytes key variant has one fixed
//! encoding a client in another language can be checked against, each enum
//! value maps to the integer `icetray.proto` declares, and the service
//! carries its full name.

use std::fmt::Debug;

use icetray_proto::v1::icetray_server::SERVICE_NAME;
use icetray_proto::v1::{
    GetRequest, GetResponse, Hit, Key, KeyType, MultiGetRequest, MultiGetResponse, Presence,
    TableInfoRequest, TableInfoResponse, key,
};
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
    (Presence::Null, 2),
    (Presence::Absent, 3),
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
    });
}

#[test]
fn test_round_trip_get_response_every_presence() {
    for &(presence, _) in PRESENCE_NUMBERS {
        let response = GetResponse {
            presence: i32::from(presence),
            value: VALUE.to_vec(),
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
    });
}

#[test]
fn test_round_trip_hit_every_presence() {
    for &(presence, _) in PRESENCE_NUMBERS {
        let hit = Hit {
            presence: i32::from(presence),
            value: VALUE.to_vec(),
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
            value: VALUE.to_vec(),
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
fn test_service_name_is_full_name() {
    assert_eq!(SERVICE_NAME, SERVICE_FULL_NAME);
}
