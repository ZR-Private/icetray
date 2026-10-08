//! Protobuf values for a fixture column declared protobuf (RFD 024): a
//! message written by hand from a key's stream, one top-level field of
//! every wire type, a singular field written twice, a packed field split in
//! two records, and a group holding a group, each field's bytes kept apart
//! so a test knows what a projection of any field numbers returns without a
//! protobuf parser and without the walker it tests.

use icetray_core::varint::{CONTINUES, PAYLOAD, PAYLOAD_BITS};
use icetray_core::{Key, Malformed, WIRE_TYPE_BITS, WIRE_TYPE_MASK, Wire};

use crate::fixture::Fixture;
use crate::keys::Stream;

/// A varint field, written first and again last, so a singular field
/// occurs twice and the last write wins.
pub const ID_FIELD: u32 = 1;
/// A fixed64 field.
pub const CHECKSUM_FIELD: u32 = 2;
/// A length-delimited field holding text.
pub const NAME_FIELD: u32 = 3;
/// A packed repeated varint field, split in two records apart from each
/// other, as an encoder may write one.
pub const SCORES_FIELD: u32 = 4;
/// A fixed32 field.
pub const FLAGS_FIELD: u32 = 5;
/// A group holding a varint and a group of the same number nested in it.
pub const GROUP_FIELD: u32 = 6;
/// A repeated length-delimited field that pads the value to its length.
pub const FILLER_FIELD: u32 = 7;
/// A number no fixture value holds.
pub const ABSENT_FIELD: u32 = 99;

/// Every field number a fixture value holds.
pub const FIELDS: [u32; 7] = [
    ID_FIELD,
    CHECKSUM_FIELD,
    NAME_FIELD,
    SCORES_FIELD,
    FLAGS_FIELD,
    GROUP_FIELD,
    FILLER_FIELD,
];

/// The elements of each record of `SCORES_FIELD`.
const SCORES_PER_RECORD: usize = 3;

/// The letters of `NAME_FIELD`'s text.
const NAME_BYTES: usize = 12;

/// The most bytes one `FILLER_FIELD` occurrence carries.
const FILLER_CHUNK_BYTES: usize = 24;

/// The alphabet a name's letters are drawn from.
const LETTERS: &[u8; 26] = b"abcdefghijklmnopqrstuvwxyz";

/// The top-level fields of the value version `version` of `key` stores,
/// in the order the value holds them, each its number and its bytes from
/// its tag to its end; `FILLER_FIELD` pads the whole to at least `len`
/// bytes.
pub fn fields(key: &Key, len: usize, version: u64) -> Vec<(u32, Vec<u8>)> {
    let mut stream = Stream::from_key_at(key, version);
    let id = stream.next();
    let scores = |stream: &mut Stream| -> Vec<u8> {
        let payload: Vec<u8> = (0..SCORES_PER_RECORD)
            .flat_map(|_| varint(stream.next() >> PAYLOAD_BITS))
            .collect();
        delimited(SCORES_FIELD, &payload)
    };
    let mut fields = vec![
        (ID_FIELD, [tag(ID_FIELD, Wire::Varint), varint(id)].concat()),
        (SCORES_FIELD, scores(&mut stream)),
    ];
    let letters = u64::try_from(LETTERS.len()).expect("an alphabet's length fits a u64");
    let name: Vec<u8> = (0..NAME_BYTES)
        .map(|_| {
            let at =
                usize::try_from(stream.next() % letters).expect("a letter's index fits a usize");
            LETTERS[at]
        })
        .collect();
    fields.push((NAME_FIELD, delimited(NAME_FIELD, &name)));
    fields.push((
        CHECKSUM_FIELD,
        [
            tag(CHECKSUM_FIELD, Wire::Fixed64),
            stream.next().to_le_bytes().to_vec(),
        ]
        .concat(),
    ));
    fields.push((
        GROUP_FIELD,
        [
            tag(GROUP_FIELD, Wire::StartGroup),
            tag(ID_FIELD, Wire::Varint),
            varint(stream.next()),
            tag(GROUP_FIELD, Wire::StartGroup),
            tag(GROUP_FIELD, Wire::EndGroup),
            tag(GROUP_FIELD, Wire::EndGroup),
        ]
        .concat(),
    ));
    let flags =
        u32::try_from(stream.next() >> u32::BITS).expect("the high half of a u64 fits a u32");
    fields.push((
        FLAGS_FIELD,
        [
            tag(FLAGS_FIELD, Wire::Fixed32),
            flags.to_le_bytes().to_vec(),
        ]
        .concat(),
    ));
    let tail = [
        (SCORES_FIELD, scores(&mut stream)),
        (
            ID_FIELD,
            [tag(ID_FIELD, Wire::Varint), varint(stream.next())].concat(),
        ),
    ];
    let tail_bytes: usize = tail.iter().map(|(_, bytes)| bytes.len()).sum();
    let mut held: usize = fields.iter().map(|(_, bytes)| bytes.len()).sum::<usize>() + tail_bytes;
    while held < len {
        let chunk: Vec<u8> = (0..FILLER_CHUNK_BYTES.min(len - held))
            .map(|_| stream.next().to_le_bytes()[0])
            .collect();
        let filler = delimited(FILLER_FIELD, &chunk);
        held += filler.len();
        fields.push((FILLER_FIELD, filler));
    }
    fields.extend(tail);
    fields
}

/// The value version `version` of `key` stores: its `fields` together.
pub fn value(key: &Key, len: usize, version: u64) -> Vec<u8> {
    fields(key, len, version)
        .into_iter()
        .flat_map(|(_, bytes)| bytes)
        .collect()
}

/// `value` trimmed to the top-level fields `numbers` names, every
/// occurrence in the value's order: what a projection to `numbers` returns.
pub fn projected(key: &Key, len: usize, version: u64, numbers: &[u32]) -> Vec<u8> {
    fields(key, len, version)
        .into_iter()
        .filter(|(number, _)| numbers.contains(number))
        .flat_map(|(_, bytes)| bytes)
        .collect()
}

/// The fault a walk of `value` meets at its first byte when that byte is
/// a tag of a wire type protobuf does not define, which makes the value
/// malformed whatever follows; `None` otherwise.
pub fn first_byte_fault(value: &[u8]) -> Option<Malformed> {
    let wire_type = value.first()? & WIRE_TYPE_MASK;
    Wire::of(wire_type)
        .is_none()
        .then_some(Malformed::WireType { at: 0, wire_type })
}

/// The first key of `fixture`, in its order, whose value is malformed at
/// its first byte (`first_byte_fault`), with that fault: a fixture of
/// random values holds some.
pub fn malformed_key(fixture: &Fixture) -> Option<(Key, Malformed)> {
    fixture.keys().find_map(|key| {
        let fault = first_byte_fault(&fixture.value(&key)?)?;
        Some((key, fault))
    })
}

/// A tag of field `number` and wire type `wire`.
fn tag(number: u32, wire: Wire) -> Vec<u8> {
    varint((u64::from(number) << WIRE_TYPE_BITS) | u64::from(wire as u8))
}

/// `bytes` as field `number`, length-delimited.
fn delimited(number: u32, bytes: &[u8]) -> Vec<u8> {
    let length = u64::try_from(bytes.len()).expect("a field's length fits a u64");
    [
        tag(number, Wire::LengthDelimited),
        varint(length),
        bytes.to_vec(),
    ]
    .concat()
}

/// `value` as a varint.
fn varint(mut value: u64) -> Vec<u8> {
    let mut bytes = Vec::new();
    loop {
        let low = u8::try_from(value & u64::from(PAYLOAD)).expect("seven bits fit a byte");
        value >>= PAYLOAD_BITS;
        if value == 0 {
            bytes.push(low);
            return bytes;
        }
        bytes.push(low | CONTINUES);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The length the tests ask a value for.
    const LEN: usize = 100;

    #[test]
    fn test_value_is_its_fields_and_reaches_its_length() {
        let key = Key::Long(42);
        let whole = value(&key, LEN, 0);
        assert!(whole.len() >= LEN);
        let numbers: Vec<u32> = fields(&key, LEN, 0).into_iter().map(|(n, _)| n).collect();
        assert_eq!(numbers.iter().filter(|&&n| n == ID_FIELD).count(), 2);
        assert_eq!(numbers.iter().filter(|&&n| n == SCORES_FIELD).count(), 2);
        assert_ne!(value(&key, LEN, 1), whole, "a later version differs");
    }
}
