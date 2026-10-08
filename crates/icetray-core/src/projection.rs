//! Field projection (RFD 024): a protobuf value trimmed to the top-level
//! fields a request names, by field number alone, with no schema. The walk
//! reads every top-level field's tag and skips its payload by its wire
//! type; a group is walked through to its end tag with a stack of the
//! groups open inside it, so the walk never recurses and is linear in the
//! value's bytes whatever its nesting. Nothing inside a length-delimited
//! field is examined. A value the walk cannot read to its end is
//! `Malformed`, whichever fields are named. The framing is read as prost's
//! decoder reads it: a tag is a varint whose value fits 32 bits, and a
//! varint is at most ten bytes, its tenth at most 1, so a value is
//! malformed exactly where that decoder refuses its framing, except that no
//! depth of group nesting is refused.

use std::collections::HashSet;
use std::fmt;
use std::ops::Range;

use bytes::Bytes;

use crate::manifest::ColumnRequestFault;

/// The largest protobuf field number, `2^29 - 1`: a field number is the
/// tag's value above its three wire-type bits, and the protobuf
/// specification caps it here so a tag fits an unsigned 32-bit varint.
pub const MAX_FIELD_NUMBER: u32 = (1 << 29) - 1;

/// The most bytes a varint may take: 64 bits at seven bits a byte.
const MAX_VARINT_BYTES: usize = 10;

/// The most a varint's tenth byte may hold: nine bytes carry 63 bits, so
/// the tenth carries the last bit and no continuation, as prost's decoder
/// requires.
const LAST_VARINT_BYTE_MAX: u8 = 1;

/// The bits of a tag below its field number that hold the wire type.
const WIRE_TYPE_BITS: u32 = 3;

/// The mask of a tag's wire-type bits.
const WIRE_TYPE_MASK: u32 = (1 << WIRE_TYPE_BITS) - 1;

/// The bit of a varint byte that says another byte follows.
const CONTINUATION: u8 = 0x80;

/// The bits of a varint byte that carry its value.
const VARINT_PAYLOAD: u8 = 0x7f;

/// The value bits each varint byte carries.
const VARINT_PAYLOAD_BITS: u32 = 7;

/// The bytes of a `fixed64`, `sfixed64`, or `double` field's payload.
const FIXED64_BYTES: usize = 8;

/// The bytes of a `fixed32`, `sfixed32`, or `float` field's payload.
const FIXED32_BYTES: usize = 4;

/// A protobuf wire type a tag may carry; 6 and 7 are not defined and make
/// a value malformed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Wire {
    /// 0: a varint.
    Varint,
    /// 1: eight bytes.
    Fixed64,
    /// 2: a varint length, then that many bytes.
    LengthDelimited,
    /// 3: the start of a group, ended by an end tag of the same number.
    StartGroup,
    /// 4: the end of a group.
    EndGroup,
    /// 5: four bytes.
    Fixed32,
}

impl Wire {
    /// The wire type of a tag's low bits; `None` for 6 and 7.
    fn of(bits: u32) -> Option<Self> {
        match bits {
            0 => Some(Self::Varint),
            1 => Some(Self::Fixed64),
            2 => Some(Self::LengthDelimited),
            3 => Some(Self::StartGroup),
            4 => Some(Self::EndGroup),
            5 => Some(Self::Fixed32),
            _ => None,
        }
    }
}

/// Why a value is not protobuf as its top-level walk reads it (RFD 024).
/// Every offset is in bytes from the value's start, at the tag of the
/// field at fault unless it says otherwise. Display gives the fault for a
/// log line.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Malformed {
    /// A tag carries wire type 6 or 7, which protobuf does not define.
    WireType {
        /// The tag's offset.
        at: usize,
        /// The wire type it carries.
        wire_type: u8,
    },
    /// A tag carries field number 0, or one above `MAX_FIELD_NUMBER`,
    /// which is a tag whose value does not fit 32 bits.
    FieldNumber {
        /// The tag's offset.
        at: usize,
        /// The field number it carries.
        number: u64,
    },
    /// A varint, a tag, a field's value, or a length, runs over
    /// `MAX_VARINT_BYTES` bytes or past 64 bits.
    VarintOverflow {
        /// The varint's offset.
        at: usize,
    },
    /// A field runs past the value's end: its tag, its varint, its fixed
    /// bytes, or the bytes its length gives.
    PastEnd {
        /// The field's offset.
        at: usize,
    },
    /// A group's end tag is missing: the value ends inside it.
    GroupUnclosed {
        /// The offset of the start tag of the top-level group left open.
        at: usize,
        /// That group's field number.
        number: u32,
    },
    /// An end tag closes a group of another number than the one open.
    GroupMismatch {
        /// The end tag's offset.
        at: usize,
        /// The number of the group open.
        open: u32,
        /// The number the end tag carries.
        closed: u32,
    },
    /// An end tag where no group is open.
    UnmatchedEndGroup {
        /// The end tag's offset.
        at: usize,
        /// The number the end tag carries.
        number: u32,
    },
}

impl fmt::Display for Malformed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WireType { at, wire_type } => {
                write!(f, "byte {at}: tag of undefined wire type {wire_type}")
            }
            Self::FieldNumber { at, number } => {
                write!(
                    f,
                    "byte {at}: field number {number} outside 1 to {MAX_FIELD_NUMBER}"
                )
            }
            Self::VarintOverflow { at } => {
                write!(
                    f,
                    "byte {at}: varint longer than {MAX_VARINT_BYTES} bytes or 64 bits"
                )
            }
            Self::PastEnd { at } => write!(f, "byte {at}: field runs past the value's end"),
            Self::GroupUnclosed { at, number } => {
                write!(f, "byte {at}: group {number} has no end tag")
            }
            Self::GroupMismatch { at, open, closed } => {
                write!(
                    f,
                    "byte {at}: end tag of group {closed} inside group {open}"
                )
            }
            Self::UnmatchedEndGroup { at, number } => {
                write!(f, "byte {at}: end tag of group {number} with no group open")
            }
        }
    }
}

/// The field numbers one column read names (RFD 024): at least one, each
/// in 1 to `MAX_FIELD_NUMBER`, none twice, held ascending for the walk's
/// membership test.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FieldSet(Box<[u32]>);

impl FieldSet {
    /// The set `fields` names; `None` for an empty list, which reads the
    /// whole value. The first number in `fields`' order that is outside the
    /// range is `ColumnRequestFault::FieldOutOfRange`, or, given a second
    /// time, `ColumnRequestFault::RepeatedField`.
    pub(crate) fn new(fields: &[u32]) -> Result<Option<Self>, ColumnRequestFault> {
        if fields.is_empty() {
            return Ok(None);
        }
        let mut seen = HashSet::with_capacity(fields.len());
        for &field in fields {
            if !(1..=MAX_FIELD_NUMBER).contains(&field) {
                return Err(ColumnRequestFault::FieldOutOfRange { field });
            }
            if !seen.insert(field) {
                return Err(ColumnRequestFault::RepeatedField { field });
            }
        }
        let mut sorted = fields.to_vec();
        sorted.sort_unstable();
        Ok(Some(Self(sorted.into_boxed_slice())))
    }

    /// Whether the set names `number`.
    fn contains(&self, number: u32) -> bool {
        self.0.binary_search(&number).is_ok()
    }
}

/// `value` trimmed to its top-level fields whose numbers `fields` names,
/// every occurrence, in the order the value holds them, each copied whole
/// from its tag to its end, a group from its start tag to its end tag. A
/// value holding none of them is empty. When the fields kept are one
/// contiguous run of the value, the answer is a slice of `value` and
/// copies nothing. The whole value is walked, so a malformed value is
/// `Malformed` whichever fields it holds.
pub(crate) fn project(value: &Bytes, fields: &FieldSet) -> Result<Bytes, Malformed> {
    let mut kept = Kept::default();
    walk(value, |number, range| {
        if fields.contains(number) {
            kept.push(value, range);
        }
    })?;
    Ok(kept.finish(value))
}

/// `Ok` when `value` walks to its end as protobuf, `Malformed` otherwise:
/// the check a projection of any fields would make, for the doctor.
pub(crate) fn check(value: &[u8]) -> Result<(), Malformed> {
    walk(value, |_, _| {})
}

/// What a projection keeps: the run of the value being extended, and the
/// bytes of the runs before it once there is more than one.
#[derive(Default)]
struct Kept {
    run: Option<Range<usize>>,
    copied: Option<Vec<u8>>,
}

impl Kept {
    /// Keeps `range` of `value`, which starts at or after the end of every
    /// range kept before.
    fn push(&mut self, value: &[u8], range: Range<usize>) {
        match &mut self.run {
            Some(run) if run.end == range.start => run.end = range.end,
            Some(run) => {
                self.copied
                    .get_or_insert_with(Vec::new)
                    .extend_from_slice(&value[run.clone()]);
                *run = range;
            }
            None => self.run = Some(range),
        }
    }

    /// The bytes kept: empty for none, a slice of `value` for one run, the
    /// runs' bytes together otherwise.
    fn finish(self, value: &Bytes) -> Bytes {
        match (self.copied, self.run) {
            (_, None) => Bytes::new(),
            (None, Some(run)) => value.slice(run),
            (Some(mut copied), Some(run)) => {
                copied.extend_from_slice(&value[run]);
                Bytes::from(copied)
            }
        }
    }
}

/// Walks `value`'s top-level fields in order, handing `field` each one's
/// number and its byte range, from its tag to its end; a group's range runs
/// through its end tag. The groups open inside a top-level group are a
/// stack of their numbers, so nesting costs four bytes per open group and
/// no recursion; the top-level field's number and offset are kept beside
/// it.
fn walk(value: &[u8], mut field: impl FnMut(u32, Range<usize>)) -> Result<(), Malformed> {
    let mut at = 0;
    let mut open: Vec<u32> = Vec::new();
    let mut top = (0, 0);
    while at < value.len() {
        let start = at;
        let (number, wire) = tag(value, &mut at)?;
        if open.is_empty() {
            top = (number, start);
        }
        match wire {
            Wire::Varint => {
                varint(value, &mut at, start)?;
            }
            Wire::Fixed64 => skip(value, &mut at, FIXED64_BYTES, start)?,
            Wire::LengthDelimited => {
                let length = varint(value, &mut at, start)?;
                let length =
                    usize::try_from(length).map_err(|_| Malformed::PastEnd { at: start })?;
                skip(value, &mut at, length, start)?;
            }
            Wire::Fixed32 => skip(value, &mut at, FIXED32_BYTES, start)?,
            Wire::StartGroup => open.push(number),
            Wire::EndGroup => match open.pop() {
                None => return Err(Malformed::UnmatchedEndGroup { at: start, number }),
                Some(group) if group != number => {
                    return Err(Malformed::GroupMismatch {
                        at: start,
                        open: group,
                        closed: number,
                    });
                }
                Some(_) => {}
            },
        }
        if open.is_empty() {
            field(top.0, top.1..at);
        }
    }
    if open.is_empty() {
        Ok(())
    } else {
        Err(Malformed::GroupUnclosed {
            at: top.1,
            number: top.0,
        })
    }
}

/// The tag at `*at`, its field number and wire type, with `*at` moved past
/// it, checked in prost's decoder's order: a varint whose value fits 32
/// bits, which bounds the field number by `MAX_FIELD_NUMBER`, then a
/// defined wire type, then a field number other than 0.
fn tag(value: &[u8], at: &mut usize) -> Result<(u32, Wire), Malformed> {
    let start = *at;
    let bits = read_varint(value, at)
        .map_err(|fault| fault.at(start, Malformed::VarintOverflow { at: start }))?;
    let out_of_range = Malformed::FieldNumber {
        at: start,
        number: bits >> WIRE_TYPE_BITS,
    };
    let bits = u32::try_from(bits).map_err(|_| out_of_range)?;
    let wire_bits = bits & WIRE_TYPE_MASK;
    let wire = Wire::of(wire_bits).ok_or(Malformed::WireType {
        at: start,
        wire_type: u8::try_from(wire_bits).expect("three bits fit a byte"),
    })?;
    match bits >> WIRE_TYPE_BITS {
        0 => Err(out_of_range),
        number => Ok((number, wire)),
    }
}

/// The varint at `*at` of the field whose tag is at `field`, with `*at`
/// moved past it.
fn varint(value: &[u8], at: &mut usize, field: usize) -> Result<u64, Malformed> {
    let start = *at;
    read_varint(value, at).map_err(|fault| fault.at(field, Malformed::VarintOverflow { at: start }))
}

/// Moves `*at` past `bytes` bytes of the field whose tag is at `field`;
/// `Malformed::PastEnd` when the value holds fewer.
fn skip(value: &[u8], at: &mut usize, bytes: usize, field: usize) -> Result<(), Malformed> {
    match at.checked_add(bytes) {
        Some(end) if end <= value.len() => {
            *at = end;
            Ok(())
        }
        Some(_) | None => Err(Malformed::PastEnd { at: field }),
    }
}

/// Why a varint did not read.
enum VarintFault {
    /// The value ended inside it.
    Truncated,
    /// It ran over `MAX_VARINT_BYTES` bytes or past 64 bits.
    Overflow,
}

impl VarintFault {
    /// The fault as `Malformed`: a truncated varint is the field at `field`
    /// running past the end, an overflowing one `overflow`.
    fn at(self, field: usize, overflow: Malformed) -> Malformed {
        match self {
            Self::Truncated => Malformed::PastEnd { at: field },
            Self::Overflow => overflow,
        }
    }
}

/// The varint at `*at`, little-endian groups of seven bits, with `*at`
/// moved past it: at most `MAX_VARINT_BYTES` bytes, the last of ten at most
/// `LAST_VARINT_BYTE_MAX`, so its value fits 64 bits with none dropped.
fn read_varint(value: &[u8], at: &mut usize) -> Result<u64, VarintFault> {
    let mut result: u64 = 0;
    for index in 0..MAX_VARINT_BYTES {
        let Some(&byte) = value.get(*at + index) else {
            return Err(VarintFault::Truncated);
        };
        let shift = VARINT_PAYLOAD_BITS * u32::try_from(index).expect("a varint index fits a u32");
        result |= u64::from(byte & VARINT_PAYLOAD) << shift;
        if byte & CONTINUATION == 0 {
            if index + 1 == MAX_VARINT_BYTES && byte > LAST_VARINT_BYTE_MAX {
                return Err(VarintFault::Overflow);
            }
            *at += index + 1;
            return Ok(result);
        }
    }
    Err(VarintFault::Overflow)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use prost::Message as _;
    use rand::rngs::StdRng;
    use rand::{RngExt as _, SeedableRng as _};

    use super::*;

    /// The set of `fields`, which the test gives valid and non-empty.
    fn set(fields: &[u32]) -> FieldSet {
        FieldSet::new(fields).unwrap().unwrap()
    }

    /// `value` projected to `fields`.
    fn projected(value: &[u8], fields: &[u32]) -> Result<Vec<u8>, Malformed> {
        project(&Bytes::copy_from_slice(value), &set(fields)).map(|bytes| bytes.to_vec())
    }

    /// A tag of `number` and `wire`.
    fn tag_of(number: u32, wire: u8) -> Vec<u8> {
        varint_of((u64::from(number) << WIRE_TYPE_BITS) | u64::from(wire))
    }

    /// `value` as a varint.
    fn varint_of(mut value: u64) -> Vec<u8> {
        let mut bytes = Vec::new();
        loop {
            let low = u8::try_from(value & u64::from(VARINT_PAYLOAD)).unwrap();
            value >>= VARINT_PAYLOAD_BITS;
            if value == 0 {
                bytes.push(low);
                return bytes;
            }
            bytes.push(low | CONTINUATION);
        }
    }

    /// Wire type numbers, as a tag carries them.
    const VARINT: u8 = 0;
    const FIXED64: u8 = 1;
    const LEN: u8 = 2;
    const START_GROUP: u8 = 3;
    const END_GROUP: u8 = 4;
    const FIXED32: u8 = 5;

    /// One field of every wire type, by number: 1 varint, 2 fixed64, 3
    /// length-delimited, 4 a group holding a varint, 5 fixed32.
    fn every_wire_type() -> Vec<Vec<u8>> {
        let varint = [tag_of(1, VARINT), varint_of(300)].concat();
        let fixed64 = [tag_of(2, FIXED64), vec![0xAB; FIXED64_BYTES]].concat();
        let delimited = [tag_of(3, LEN), varint_of(3), b"abc".to_vec()].concat();
        let group = [
            tag_of(4, START_GROUP),
            tag_of(1, VARINT),
            varint_of(7),
            tag_of(4, END_GROUP),
        ]
        .concat();
        let fixed32 = [tag_of(5, FIXED32), vec![0xCD; FIXED32_BYTES]].concat();
        vec![varint, fixed64, delimited, group, fixed32]
    }

    #[test]
    fn test_project_keeps_each_wire_type_whole() {
        let fields = every_wire_type();
        let value = fields.concat();
        for (index, field) in fields.iter().enumerate() {
            let number = u32::try_from(index).unwrap() + 1;
            assert_eq!(
                projected(&value, &[number]).unwrap(),
                *field,
                "field {number}"
            );
        }
        assert_eq!(projected(&value, &[1, 2, 3, 4, 5]).unwrap(), value);
        assert_eq!(
            projected(&value, &[1, 3, 5]).unwrap(),
            [fields[0].clone(), fields[2].clone(), fields[4].clone()].concat()
        );
    }

    #[test]
    fn test_project_keeps_every_occurrence_in_the_value_s_order() {
        let first = [tag_of(2, VARINT), varint_of(1)].concat();
        let between = [tag_of(1, VARINT), varint_of(9)].concat();
        let second = [tag_of(2, VARINT), varint_of(2)].concat();
        let value = [first.clone(), between, second.clone()].concat();
        assert_eq!(projected(&value, &[2]).unwrap(), [first, second].concat());
    }

    #[test]
    fn test_project_of_none_of_the_fields_is_empty_and_of_an_empty_value_too() {
        let value = every_wire_type().concat();
        assert_eq!(projected(&value, &[9, MAX_FIELD_NUMBER]).unwrap(), b"");
        assert_eq!(projected(b"", &[1]).unwrap(), b"");
    }

    #[test]
    fn test_project_slices_a_contiguous_run_and_copies_otherwise() {
        let value = Bytes::from(every_wire_type().concat());
        let run = project(&value, &set(&[2, 3])).unwrap();
        let start = every_wire_type()[0].len();
        assert_eq!(
            run.as_ptr(),
            value[start..].as_ptr(),
            "a slice of the value"
        );
        let whole = project(&value, &set(&[1, 2, 3, 4, 5])).unwrap();
        assert_eq!(whole.as_ptr(), value.as_ptr(), "every field is one run");
        assert_eq!(whole.len(), value.len());
        let apart = project(&value, &set(&[1, 5])).unwrap();
        assert!(
            !value.as_ptr_range().contains(&apart.as_ptr()),
            "two runs are copied"
        );
    }

    #[test]
    fn test_project_does_not_look_inside_a_length_delimited_field() {
        let inner = [tag_of(5, END_GROUP), tag_of(1, 7), vec![0xFF]].concat();
        let value = [
            tag_of(3, LEN),
            varint_of(u64::try_from(inner.len()).unwrap()),
            inner,
        ]
        .concat();
        assert_eq!(projected(&value, &[3]).unwrap(), value);
        assert_eq!(check(&value), Ok(()));
    }

    #[test]
    fn test_project_keeps_a_group_with_its_nested_groups_through_its_end_tag() {
        let group = [
            tag_of(6, START_GROUP),
            tag_of(7, START_GROUP),
            tag_of(6, START_GROUP),
            tag_of(1, FIXED32),
            vec![0; FIXED32_BYTES],
            tag_of(6, END_GROUP),
            tag_of(7, END_GROUP),
            tag_of(2, LEN),
            varint_of(0),
            tag_of(6, END_GROUP),
        ]
        .concat();
        let after = [tag_of(1, VARINT), varint_of(5)].concat();
        let value = [group.clone(), after.clone()].concat();
        assert_eq!(projected(&value, &[6]).unwrap(), group);
        assert_eq!(
            projected(&value, &[1]).unwrap(),
            after,
            "a nested 1 is not top-level"
        );
        assert_eq!(
            projected(&value, &[7]).unwrap(),
            b"",
            "a nested 7 is not top-level"
        );
    }

    /// Groups nested in the deep-nesting test: three orders past a protobuf
    /// runtime's recursion limit (prost's is 100), so a walk that recursed
    /// would overflow its stack.
    const DEEP_NESTING: usize = 100_000;

    #[test]
    fn test_project_walks_deep_nesting_without_recursing() {
        let value = [
            tag_of(1, START_GROUP).repeat(DEEP_NESTING),
            tag_of(1, END_GROUP).repeat(DEEP_NESTING),
        ]
        .concat();
        assert_eq!(projected(&value, &[1]).unwrap(), value);
    }

    #[test]
    fn test_project_refuses_wire_types_six_and_seven() {
        for wire_type in [6, 7] {
            let value = [tag_of(1, VARINT), varint_of(1), tag_of(2, wire_type)].concat();
            assert_eq!(
                projected(&value, &[1]),
                Err(Malformed::WireType { at: 2, wire_type }),
                "wire type {wire_type}"
            );
        }
    }

    #[test]
    fn test_project_refuses_field_number_zero_and_above_the_range() {
        let zero = tag_of(0, VARINT);
        assert_eq!(
            projected(&[zero, varint_of(1)].concat(), &[1]),
            Err(Malformed::FieldNumber { at: 0, number: 0 })
        );
        let above = MAX_FIELD_NUMBER + 1;
        let tag = tag_of(above, VARINT);
        assert!(
            u32::try_from((u64::from(above)) << WIRE_TYPE_BITS).is_err(),
            "the tag does not fit 32 bits"
        );
        assert_eq!(
            projected(&[tag, varint_of(1)].concat(), &[1]),
            Err(Malformed::FieldNumber {
                at: 0,
                number: u64::from(above)
            })
        );
        let highest = [tag_of(MAX_FIELD_NUMBER, VARINT), varint_of(1)].concat();
        assert_eq!(projected(&highest, &[MAX_FIELD_NUMBER]).unwrap(), highest);
    }

    #[test]
    fn test_project_reads_tags_and_varints_as_prost_s_decoder_does() {
        let padded_tag = [0x88, 0x80, 0x80, 0x80, 0x80, 0x00, 0x01];
        assert_eq!(
            projected(&padded_tag, &[1]).unwrap(),
            padded_tag,
            "a tag padded past five bytes is field 1"
        );
        assert_eq!(Inner::decode(&padded_tag[..]).unwrap().number, 1);
        let mut ten = tag_of(1, VARINT);
        ten.extend([0xFF; MAX_VARINT_BYTES - 1]);
        ten.push(LAST_VARINT_BYTE_MAX);
        assert_eq!(projected(&ten, &[1]).unwrap(), ten, "ten bytes is a varint");
        let past_64_bits = [
            0x08, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F,
        ];
        let mut eleven = tag_of(1, VARINT);
        eleven.extend([0xFF; MAX_VARINT_BYTES]);
        eleven.push(0x01);
        let mut long_length = tag_of(3, LEN);
        long_length.extend([0x80; MAX_VARINT_BYTES]);
        long_length.push(0x00);
        let mut long_tag = vec![0x88; MAX_VARINT_BYTES];
        long_tag.push(0x00);
        let tag_past_32_bits = varint_of(1 << u32::BITS);
        let cases = [
            (&past_64_bits[..], Malformed::VarintOverflow { at: 1 }),
            (&eleven[..], Malformed::VarintOverflow { at: 1 }),
            (&long_length[..], Malformed::VarintOverflow { at: 1 }),
            (&long_tag[..], Malformed::VarintOverflow { at: 0 }),
            (
                &tag_past_32_bits[..],
                Malformed::FieldNumber {
                    at: 0,
                    number: 1 << (u32::BITS - WIRE_TYPE_BITS),
                },
            ),
        ];
        for (value, expected) in cases {
            assert_eq!(projected(value, &[1]), Err(expected), "{value:02X?}");
            assert!(
                Unknown::decode(value).is_err(),
                "prost refuses {value:02X?}"
            );
        }
    }

    #[test]
    fn test_project_refuses_every_field_that_runs_past_the_end() {
        let prefix = [tag_of(1, VARINT), varint_of(1)].concat();
        let at = prefix.len();
        let cases = [
            ("a tag cut short", vec![0x80]),
            (
                "a varint cut short",
                [tag_of(2, VARINT), vec![0x80]].concat(),
            ),
            ("a varint missing", tag_of(2, VARINT)),
            (
                "fixed64 short",
                [tag_of(2, FIXED64), vec![0; FIXED64_BYTES - 1]].concat(),
            ),
            (
                "fixed32 short",
                [tag_of(2, FIXED32), vec![0; FIXED32_BYTES - 1]].concat(),
            ),
            (
                "a length past the end",
                [tag_of(2, LEN), varint_of(4), b"abc".to_vec()].concat(),
            ),
            ("a length cut short", [tag_of(2, LEN), vec![0x80]].concat()),
            (
                "a length of u64::MAX",
                [tag_of(2, LEN), varint_of(u64::MAX)].concat(),
            ),
        ];
        for (case, tail) in cases {
            let value = [prefix.clone(), tail].concat();
            assert_eq!(
                projected(&value, &[1]),
                Err(Malformed::PastEnd { at }),
                "{case}"
            );
            assert_eq!(check(&value), Err(Malformed::PastEnd { at }), "{case}");
        }
    }

    #[test]
    fn test_project_refuses_unclosed_mismatched_and_unmatched_groups() {
        let unclosed = [
            tag_of(1, START_GROUP),
            tag_of(2, START_GROUP),
            tag_of(2, END_GROUP),
            tag_of(3, START_GROUP),
        ]
        .concat();
        assert_eq!(
            projected(&unclosed, &[1]),
            Err(Malformed::GroupUnclosed { at: 0, number: 1 }),
            "the top-level group left open"
        );
        let mismatched = [tag_of(1, START_GROUP), tag_of(2, END_GROUP)].concat();
        assert_eq!(
            projected(&mismatched, &[1]),
            Err(Malformed::GroupMismatch {
                at: 1,
                open: 1,
                closed: 2
            })
        );
        let unmatched = [tag_of(1, VARINT), varint_of(0), tag_of(1, END_GROUP)].concat();
        assert_eq!(
            projected(&unmatched, &[1]),
            Err(Malformed::UnmatchedEndGroup { at: 2, number: 1 })
        );
        let inside = [tag_of(1, START_GROUP), tag_of(2, 6), tag_of(1, END_GROUP)].concat();
        assert_eq!(
            projected(&inside, &[2]),
            Err(Malformed::WireType {
                at: 1,
                wire_type: 6
            }),
            "a field inside a group is walked"
        );
    }

    #[test]
    fn test_project_refuses_a_malformed_value_whichever_fields_are_named() {
        let value = [tag_of(1, VARINT), varint_of(1), tag_of(2, 7)].concat();
        for fields in [&[1][..], &[2], &[3]] {
            assert!(projected(&value, fields).is_err(), "{fields:?}");
        }
        assert!(check(&value).is_err());
    }

    #[test]
    fn test_new_field_set_is_none_when_empty_and_refuses_range_and_repeats() {
        assert_eq!(FieldSet::new(&[]), Ok(None));
        assert_eq!(
            FieldSet::new(&[3, 1, 2]).unwrap().unwrap(),
            FieldSet(vec![1, 2, 3].into_boxed_slice())
        );
        assert_eq!(
            FieldSet::new(&[1, 0]),
            Err(ColumnRequestFault::FieldOutOfRange { field: 0 })
        );
        assert_eq!(
            FieldSet::new(&[MAX_FIELD_NUMBER + 1]),
            Err(ColumnRequestFault::FieldOutOfRange {
                field: MAX_FIELD_NUMBER + 1
            })
        );
        assert_eq!(
            FieldSet::new(&[4, 9, 2, 9, 4]),
            Err(ColumnRequestFault::RepeatedField { field: 9 }),
            "the first number given again, in the list's order"
        );
    }

    /// A nested message of the typed fixtures.
    #[derive(Clone, PartialEq, prost::Message)]
    struct Inner {
        #[prost(int32, tag = "1")]
        number: i32,
        #[prost(string, tag = "2")]
        text: String,
        #[prost(uint64, repeated, tag = "3")]
        values: Vec<u64>,
    }

    /// A proto2 group's body, holding a group of its own.
    #[derive(Clone, PartialEq, prost::Message)]
    struct Grouped {
        #[prost(sint32, optional, tag = "1")]
        a: Option<i32>,
        #[prost(bytes = "vec", optional, tag = "2")]
        b: Option<Vec<u8>>,
        #[prost(message, optional, tag = "3")]
        inner: Option<Inner>,
        #[prost(group, optional, tag = "4")]
        deeper: Option<Deeper>,
    }

    /// The body of a group nested in `Grouped`.
    #[derive(Clone, PartialEq, prost::Message)]
    struct Deeper {
        #[prost(int32, optional, tag = "1")]
        a: Option<i32>,
    }

    /// A message of no fields, which prost parses by skipping every field
    /// of a value: whether it parses is whether prost's decoder accepts the
    /// value's framing.
    #[derive(Clone, PartialEq, prost::Message)]
    struct Unknown {}

    /// The members of the typed fixture's oneof: a scalar and a message.
    #[derive(Clone, PartialEq, prost::Oneof)]
    enum Choice {
        #[prost(int64, tag = "7")]
        Scalar(i64),
        #[prost(message, tag = "8")]
        Message(Inner),
    }

    /// The typed fixture: scalars of every wire type, repeated non-packed
    /// and packed fields, a map, a oneof, a nested message, and a group.
    #[derive(Clone, PartialEq, prost::Message)]
    struct Typed {
        #[prost(int64, tag = "1")]
        id: i64,
        #[prost(string, tag = "2")]
        name: String,
        #[prost(uint32, repeated, packed = "false", tag = "3")]
        unpacked: Vec<u32>,
        #[prost(int64, repeated, tag = "4")]
        packed: Vec<i64>,
        #[prost(btree_map = "string, int32", tag = "5")]
        map: BTreeMap<String, i32>,
        #[prost(message, optional, tag = "6")]
        nested: Option<Inner>,
        #[prost(oneof = "Choice", tags = "7, 8")]
        choice: Option<Choice>,
        #[prost(group, optional, tag = "9")]
        group: Option<Grouped>,
        #[prost(fixed32, tag = "10")]
        fixed32: u32,
        #[prost(sfixed64, tag = "11")]
        fixed64: i64,
        #[prost(bytes = "vec", repeated, tag = "12")]
        blobs: Vec<Vec<u8>>,
        #[prost(bool, tag = "13")]
        flag: bool,
    }

    /// Every field number of `Typed`.
    const TYPED_FIELDS: [u32; 13] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13];

    /// The members of `Typed`'s oneof.
    const ONEOF_MEMBERS: [u32; 2] = [7, 8];

    /// The field of `Typed` that is packed.
    const PACKED_FIELD: u32 = 4;

    /// A field number `Typed` does not have: the oracle's values carry it as
    /// unknown fields, and its field sets name it at times.
    const UNKNOWN_FIELD: u32 = 99;

    /// The number of a group nested in the oracle values' unknown group.
    const UNKNOWN_NESTED_GROUP: u32 = 98;

    /// One field set in this many names `UNKNOWN_FIELD`.
    const UNKNOWN_FIELD_ODDS: u32 = 4;

    /// The seed of the projection oracle's instances.
    const PROJECTION_SEED: u64 = 0x0024;

    /// The seed of the check's instances.
    const CHECK_SEED: u64 = 0x0016;

    /// The seed of the framing oracle's mutations.
    const FRAMING_SEED: u64 = 0x0010;

    /// Seeded instances per oracle test.
    const INSTANCES: u64 = 300;

    /// Mutated values per instance in the framing oracle.
    const MUTATIONS_PER_INSTANCE: usize = 20;

    /// Field sets drawn per instance.
    const SETS_PER_INSTANCE: usize = 12;

    /// The longest repeated field or string a random instance holds.
    const MOST_ELEMENTS: usize = 5;

    fn random_text(rng: &mut StdRng) -> String {
        let len = rng.random_range(0..=MOST_ELEMENTS);
        (0..len)
            .map(|_| char::from(rng.random_range(b'a'..=b'z')))
            .collect()
    }

    fn random_inner(rng: &mut StdRng) -> Inner {
        Inner {
            number: rng.random(),
            text: random_text(rng),
            values: (0..rng.random_range(0..=MOST_ELEMENTS))
                .map(|_| rng.random())
                .collect(),
        }
    }

    /// A random `Typed`, each field set or left at its default by a coin.
    fn random_typed(rng: &mut StdRng) -> Typed {
        let mut typed = Typed::default();
        if rng.random() {
            typed.id = rng.random();
        }
        if rng.random() {
            typed.name = random_text(rng);
        }
        typed.unpacked = (0..rng.random_range(0..=MOST_ELEMENTS))
            .map(|_| rng.random())
            .collect();
        typed.packed = (0..rng.random_range(0..=MOST_ELEMENTS))
            .map(|_| rng.random())
            .collect();
        for _ in 0..rng.random_range(0..=MOST_ELEMENTS) {
            typed.map.insert(random_text(rng), rng.random());
        }
        if rng.random() {
            typed.nested = Some(random_inner(rng));
        }
        typed.choice = match rng.random_range(0..3) {
            0 => None,
            1 => Some(Choice::Scalar(rng.random())),
            _ => Some(Choice::Message(random_inner(rng))),
        };
        if rng.random() {
            typed.group = Some(Grouped {
                a: rng.random::<bool>().then(|| rng.random()),
                b: rng.random::<bool>().then(|| random_text(rng).into_bytes()),
                inner: rng.random::<bool>().then(|| random_inner(rng)),
                deeper: rng.random::<bool>().then(|| Deeper {
                    a: rng.random::<bool>().then(|| rng.random()),
                }),
            });
        }
        if rng.random() {
            typed.fixed32 = rng.random();
        }
        if rng.random() {
            typed.fixed64 = rng.random();
        }
        typed.blobs = (0..rng.random_range(0..=MOST_ELEMENTS))
            .map(|_| random_text(rng).into_bytes())
            .collect();
        typed.flag = rng.random();
        typed
    }

    /// One packed record of `PACKED_FIELD` holding `values`, written by
    /// hand: a second record of a packed field, which an encoder may emit
    /// and every parser appends.
    fn packed_record(values: &[i64]) -> Vec<u8> {
        let payload: Vec<u8> = values
            .iter()
            .flat_map(|value| varint_of(value.cast_unsigned()))
            .collect();
        [
            tag_of(PACKED_FIELD, LEN),
            varint_of(u64::try_from(payload.len()).unwrap()),
            payload,
        ]
        .concat()
    }

    /// `typed` with every field `fields` does not name cleared, the oneof
    /// kept when `fields` names the member it holds.
    fn masked(typed: &Typed, fields: &[u32]) -> Typed {
        let named = |number: u32| fields.contains(&number);
        let mut out = typed.clone();
        if !named(1) {
            out.id = 0;
        }
        if !named(2) {
            out.name.clear();
        }
        if !named(3) {
            out.unpacked.clear();
        }
        if !named(PACKED_FIELD) {
            out.packed.clear();
        }
        if !named(5) {
            out.map.clear();
        }
        if !named(6) {
            out.nested = None;
        }
        out.choice = match &typed.choice {
            Some(Choice::Scalar(_)) if !named(ONEOF_MEMBERS[0]) => None,
            Some(Choice::Message(_)) if !named(ONEOF_MEMBERS[1]) => None,
            other => other.clone(),
        };
        if !named(9) {
            out.group = None;
        }
        if !named(10) {
            out.fixed32 = 0;
        }
        if !named(11) {
            out.fixed64 = 0;
        }
        if !named(12) {
            out.blobs.clear();
        }
        if !named(13) {
            out.flag = false;
        }
        out
    }

    /// A random non-empty set of `Typed`'s fields that names both oneof
    /// members or neither, a number no field has among them at times.
    fn random_fields(rng: &mut StdRng) -> Vec<u32> {
        loop {
            let mut fields: Vec<u32> = TYPED_FIELDS
                .iter()
                .copied()
                .filter(|number| !ONEOF_MEMBERS.contains(number) && rng.random())
                .collect();
            if rng.random() {
                fields.extend(ONEOF_MEMBERS);
            }
            if rng.random_range(0..UNKNOWN_FIELD_ODDS) == 0 {
                fields.push(UNKNOWN_FIELD);
            }
            if !fields.is_empty() {
                return fields;
            }
        }
    }

    /// Fields of `UNKNOWN_FIELD`, which `Typed` does not have: a varint,
    /// and a group holding a group of `UNKNOWN_NESTED_GROUP` and a
    /// length-delimited field.
    fn unknown_fields(rng: &mut StdRng) -> Vec<u8> {
        [
            tag_of(UNKNOWN_FIELD, VARINT),
            varint_of(rng.random()),
            tag_of(UNKNOWN_FIELD, START_GROUP),
            tag_of(UNKNOWN_NESTED_GROUP, START_GROUP),
            tag_of(1, FIXED32),
            rng.random::<u32>().to_le_bytes().to_vec(),
            tag_of(UNKNOWN_NESTED_GROUP, END_GROUP),
            tag_of(2, LEN),
            varint_of(1),
            vec![rng.random()],
            tag_of(UNKNOWN_FIELD, END_GROUP),
        ]
        .concat()
    }

    /// The value the oracle tests project, and the unknown fields it holds:
    /// two encodings concatenated, as a writer that merges messages
    /// produces, with a hand-written packed record of `PACKED_FIELD` and
    /// fields `Typed` does not have between them, so singular fields occur
    /// twice, messages and groups merge, and the packed field is split.
    fn merged_value(rng: &mut StdRng) -> (Vec<u8>, Vec<u8>) {
        let extra: Vec<i64> = (0..rng.random_range(1..=MOST_ELEMENTS))
            .map(|_| rng.random())
            .collect();
        let unknown = unknown_fields(rng);
        let value = [
            random_typed(rng).encode_to_vec(),
            packed_record(&extra),
            unknown.clone(),
            random_typed(rng).encode_to_vec(),
        ]
        .concat();
        (value, unknown)
    }

    #[test]
    fn test_project_named_fields_parse_as_the_whole_value_s_over_typed_messages() {
        let mut rng = StdRng::seed_from_u64(PROJECTION_SEED);
        for instance in 0..INSTANCES {
            let (value, unknown) = merged_value(&mut rng);
            let whole = Typed::decode(value.as_slice()).unwrap();
            assert_eq!(
                projected(&value, &[UNKNOWN_FIELD]).unwrap(),
                unknown,
                "unknown fields are kept whole, nested group included"
            );
            for _ in 0..SETS_PER_INSTANCE {
                let fields = random_fields(&mut rng);
                let trimmed = projected(&value, &fields).unwrap();
                assert_eq!(
                    Typed::decode(trimmed.as_slice()).unwrap(),
                    masked(&whole, &fields),
                    "instance {instance}, fields {fields:?}"
                );
            }
            let every = [&TYPED_FIELDS[..], &[UNKNOWN_FIELD]].concat();
            assert_eq!(projected(&value, &every).unwrap(), value);
        }
    }

    #[test]
    fn test_project_split_packed_field_keeps_both_records() {
        let first = packed_record(&[1, -2]);
        let second = packed_record(&[3]);
        let between = Typed {
            id: 5,
            ..Typed::default()
        }
        .encode_to_vec();
        let value = [first.clone(), between, second.clone()].concat();
        let trimmed = projected(&value, &[PACKED_FIELD]).unwrap();
        assert_eq!(trimmed, [first, second].concat());
        assert_eq!(
            Typed::decode(trimmed.as_slice()).unwrap().packed,
            [1, -2, 3]
        );
    }

    #[test]
    fn test_project_oneof_is_exact_only_when_every_member_is_named() {
        let scalar = Typed {
            choice: Some(Choice::Scalar(11)),
            ..Typed::default()
        };
        let message = Typed {
            choice: Some(Choice::Message(Inner {
                number: 3,
                ..Inner::default()
            })),
            ..Typed::default()
        };
        let value = [scalar.encode_to_vec(), message.encode_to_vec()].concat();
        let whole = Typed::decode(value.as_slice()).unwrap();
        assert_eq!(whole.choice, message.choice, "the later member wins");
        let every = projected(&value, &ONEOF_MEMBERS).unwrap();
        assert_eq!(
            Typed::decode(every.as_slice()).unwrap().choice,
            whole.choice
        );
        let one = projected(&value, &[ONEOF_MEMBERS[0]]).unwrap();
        assert_eq!(
            Typed::decode(one.as_slice()).unwrap().choice,
            scalar.choice,
            "a member the whole value later cleared comes back set"
        );
    }

    #[test]
    fn test_check_accepts_every_typed_instance() {
        let mut rng = StdRng::seed_from_u64(CHECK_SEED);
        for _ in 0..INSTANCES {
            assert_eq!(check(&merged_value(&mut rng).0), Ok(()));
        }
    }

    /// `value` with one random change: cut short, a byte replaced, or a
    /// byte inserted.
    fn mutated(rng: &mut StdRng, value: &[u8]) -> Vec<u8> {
        let mut out = value.to_vec();
        let at = rng.random_range(0..=out.len());
        match rng.random_range(0..3) {
            0 => out.truncate(at),
            1 if at < out.len() => out[at] = rng.random(),
            _ => out.insert(at, rng.random()),
        }
        out
    }

    #[test]
    fn test_check_refuses_exactly_the_framing_prost_s_decoder_refuses() {
        let mut rng = StdRng::seed_from_u64(FRAMING_SEED);
        let mut refused = 0;
        for _ in 0..INSTANCES {
            let (value, _) = merged_value(&mut rng);
            for _ in 0..MUTATIONS_PER_INSTANCE {
                let value = mutated(&mut rng, &value);
                let prost = Unknown::decode(value.as_slice()).is_ok();
                assert_eq!(check(&value).is_ok(), prost, "{value:02X?}");
                refused += usize::from(!prost);
            }
        }
        assert!(refused > 0, "some mutations break the framing");
    }
}
