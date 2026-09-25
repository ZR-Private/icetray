//! Key and value derivation shared by every fixture (RFD 014): the `i`-th
//! key of each type, an absent key beside it, a key past every fixture key,
//! and a value that is a function of its key, so any answer is checkable
//! without a stored oracle.

use icetray_core::key::UUID_BYTES;
use icetray_core::{Key, KeyType};

/// The spacing between fixture keys, shared with the golden recipe
/// (`dev/golden-tables.py`): `key + 1` is absent and inside the range.
pub const KEY_STEP: u64 = 3;

/// Bytes in a fixture value where a specification does not say: a
/// hundred, so a contract page of a thousand values is about a hundred
/// kibibytes, under the doctor's value-page advice, and the small fixture
/// draws no finding.
pub const VALUE_BYTES: usize = 100;

/// The text between a string key's padding and its digits.
const STRING_PREFIX: &str = "user:";

/// Digits a string key's value is zero-padded to, so string order is
/// numeric order.
const STRING_DIGITS: usize = 16;

/// The bytes of a string key beyond its padding: the prefix and the digits.
/// A test that wants keys of a given length takes its `key_pad` from this.
pub const STRING_KEY_FIXED_BYTES: usize = STRING_PREFIX.len() + STRING_DIGITS;

/// The character `key_pad` repeats before a string key.
pub(crate) const STRING_PAD: char = 'p';

/// The suffix that turns a string key into an absent one greater than it.
const ABSENT_SUFFIX: char = 'x';

/// The tag byte that starts every binary key, `pad + 1` times, before the
/// big-endian value; bytewise order is then numeric order, as an encoded
/// composite key's would be.
const BINARY_TAG: u8 = 0x4b;

/// The byte appended to a binary key to make an absent one greater than it.
const ABSENT_BYTE: u8 = 0;

/// Where a uuid key's big-endian value starts: its last eight bytes, so the
/// bytewise unsigned order RFD 008 fixes for uuid is numeric order.
const UUID_VALUE_OFFSET: usize = UUID_BYTES - size_of::<u64>();

/// The string past every fixture string key: `z` is above the padding
/// character and the prefix's first letter.
const FAR_STRING: &str = "zzzz";

/// The length of the binary far key; its first byte is above `BINARY_TAG`.
const FAR_BYTES_LEN: usize = 4;

/// The byte every far binary and uuid key is made of.
const FAR_BYTE: u8 = 0xff;

/// The FNV-1a offset basis, the seed of a string, binary, or uuid key's value stream.
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;

/// The FNV-1a prime.
const FNV_PRIME: u64 = 0x0100_0000_01b3;

/// The multiplier that spreads a key's seed before the xorshift stream
/// starts: the golden ratio's fractional bits, so neighboring keys start
/// far apart.
const SEED_MULTIPLIER: u64 = 0x9e37_79b9_7f4a_7c15;

/// The first shift of Marsaglia's xorshift64 triple (13, 7, 17), whose
/// stream has full period, so no fixture's values repeat.
const XORSHIFT_FIRST_LEFT: u32 = 13;

/// The second shift of the triple, to the right.
const XORSHIFT_RIGHT: u32 = 7;

/// The third shift of the triple, to the left.
const XORSHIFT_SECOND_LEFT: u32 = 17;

/// The `i`-th fixture key of `key_type`, with no padding.
pub fn key_at(i: u64, key_type: KeyType) -> Key {
    key_at_padded(i, key_type, 0)
}

/// The `i`-th fixture key of `key_type` with `pad` extra prefix bytes on a
/// string or binary key: `Long(3i)`; `pad` copies of `p`, `user:`, and the
/// sixteen-digit value; `pad + 1` tag bytes and the big-endian value; a
/// uuid whose last eight bytes are the big-endian value. Strictly
/// increasing in `i` for every type under `Key`'s order. A long or uuid key
/// takes no padding; `write_fixture` refuses a specification that asks.
pub fn key_at_padded(i: u64, key_type: KeyType, pad: usize) -> Key {
    let value = i * KEY_STEP;
    match key_type {
        KeyType::Long => {
            Key::Long(i64::try_from(value).expect("fixture ids stay far below i64::MAX / KEY_STEP"))
        }
        KeyType::String => {
            let padding = String::from(STRING_PAD).repeat(pad);
            let width = STRING_DIGITS;
            Key::Str(format!("{padding}{STRING_PREFIX}{value:0width$}"))
        }
        KeyType::Binary => {
            let mut bytes = vec![BINARY_TAG; pad + 1];
            bytes.extend_from_slice(&value.to_be_bytes());
            Key::Bytes(bytes)
        }
        KeyType::Uuid => {
            let mut bytes = [0u8; UUID_BYTES];
            bytes[UUID_VALUE_OFFSET..].copy_from_slice(&value.to_be_bytes());
            Key::Uuid(bytes)
        }
    }
}

/// A key just above `key` that no fixture holds: the long plus one, the
/// string with a character appended, the bytes with a byte appended, the
/// uuid with its big-endian value plus one.
pub fn absent_key_from(key: &Key) -> Key {
    match key {
        Key::Long(value) => Key::Long(value + 1),
        Key::Str(text) => Key::Str(format!("{text}{ABSENT_SUFFIX}")),
        Key::Bytes(bytes) => {
            let mut absent = bytes.clone();
            absent.push(ABSENT_BYTE);
            Key::Bytes(absent)
        }
        Key::Uuid(bytes) => {
            let value = uuid_value(bytes)
                .checked_add(1)
                .expect("a fixture uuid key's value is far below u64::MAX");
            let mut absent = *bytes;
            absent[UUID_VALUE_OFFSET..].copy_from_slice(&value.to_be_bytes());
            Key::Uuid(absent)
        }
    }
}

/// `absent_key_from` the `i`-th key of `key_type`.
pub fn absent_key_near(i: u64, key_type: KeyType) -> Key {
    absent_key_from(&key_at(i, key_type))
}

/// A key of `key_type` past every fixture key, so a lookup for it routes to
/// no file.
pub fn far_key(key_type: KeyType) -> Key {
    match key_type {
        KeyType::Long => Key::Long(i64::MAX),
        KeyType::String => Key::Str(FAR_STRING.to_string()),
        KeyType::Binary => Key::Bytes(vec![FAR_BYTE; FAR_BYTES_LEN]),
        KeyType::Uuid => Key::Uuid([FAR_BYTE; UUID_BYTES]),
    }
}

/// The fixture id `key` was derived from under `pad`, or `None` for a key
/// no `key_at_padded(_, _, pad)` produces: a value that is not a multiple
/// of `KEY_STEP`, a string without the padding and prefix, bytes of another
/// length, a uuid with a non-zero head.
pub(crate) fn id_of(key: &Key, pad: usize) -> Option<u64> {
    let value = match key {
        Key::Long(value) => u64::try_from(*value).ok()?,
        Key::Str(text) => {
            let (padding, rest) = text.split_at_checked(pad)?;
            if padding.chars().any(|c| c != STRING_PAD) {
                return None;
            }
            let digits = rest.strip_prefix(STRING_PREFIX)?;
            if digits.len() != STRING_DIGITS {
                return None;
            }
            digits.parse().ok()?
        }
        Key::Bytes(bytes) => {
            let (tags, value) = bytes.split_at_checked(pad + 1)?;
            if tags.iter().any(|&byte| byte != BINARY_TAG) {
                return None;
            }
            u64::from_be_bytes(<[u8; size_of::<u64>()]>::try_from(value).ok()?)
        }
        Key::Uuid(bytes) => {
            if bytes[..UUID_VALUE_OFFSET].iter().any(|&byte| byte != 0) {
                return None;
            }
            uuid_value(bytes)
        }
    };
    (value % KEY_STEP == 0).then_some(value / KEY_STEP)
}

/// The big-endian value in a uuid key's last eight bytes.
fn uuid_value(bytes: &[u8; UUID_BYTES]) -> u64 {
    u64::from_be_bytes(
        <[u8; size_of::<u64>()]>::try_from(&bytes[UUID_VALUE_OFFSET..])
            .expect("the tail of a sixteen-byte uuid is eight bytes"),
    )
}

/// The `VALUE_BYTES`-byte value stored for `key`.
pub fn value_for(key: &Key) -> Vec<u8> {
    value_for_len(key, VALUE_BYTES)
}

/// The `len`-byte value stored for `key` in a table with values of that
/// length: a xorshift64 stream seeded from the key (a long's bits, or
/// FNV-1a over the key's bytes), deterministic, distinct for neighboring
/// keys, and incompressible, so a fixture of `n` rows occupies about
/// `n * len` bytes on disk after zstd.
pub fn value_for_len(key: &Key, len: usize) -> Vec<u8> {
    let seed = match key {
        Key::Long(value) => u64::from_ne_bytes(value.to_ne_bytes()),
        Key::Str(_) | Key::Bytes(_) | Key::Uuid(_) => fnv1a(&key.as_bytes()),
    };
    let mut state = seed.wrapping_mul(SEED_MULTIPLIER) | 1;
    (0..len)
        .map(|_| {
            state ^= state << XORSHIFT_FIRST_LEFT;
            state ^= state >> XORSHIFT_RIGHT;
            state ^= state << XORSHIFT_SECOND_LEFT;
            state.to_le_bytes()[0]
        })
        .collect()
}

/// FNV-1a over `bytes`.
fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(FNV_OFFSET_BASIS, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Ids spanning several orders of magnitude, so the order test crosses
    /// digit and byte boundaries.
    const SAMPLE_IDS: [u64; 12] = [
        0, 1, 2, 9, 10, 99, 100, 999, 1_000, 65_535, 65_536, 1_000_000,
    ];

    /// The padding the padded cases use.
    const PAD: usize = 3;

    /// The length the sized value test asks for.
    const OTHER_LEN: usize = 17;

    /// The padding each type admits: none for the fixed-width types.
    fn pads(key_type: KeyType) -> Vec<usize> {
        if key_type.is_fixed_width() {
            vec![0]
        } else {
            vec![0, PAD]
        }
    }

    #[test]
    fn test_key_at_padded_is_strictly_increasing_per_type() {
        for key_type in KeyType::ALL {
            for pad in pads(key_type) {
                let keys: Vec<Key> = SAMPLE_IDS
                    .iter()
                    .map(|&id| key_at_padded(id, key_type, pad))
                    .collect();
                assert!(
                    keys.windows(2).all(|pair| pair[0] < pair[1]),
                    "{key_type} pad {pad}: {keys:?}"
                );
                assert!(keys.iter().all(|key| key.key_type() == key_type));
            }
        }
    }

    #[test]
    fn test_key_at_is_key_at_padded_with_no_pad() {
        for key_type in KeyType::ALL {
            for id in SAMPLE_IDS {
                assert_eq!(key_at(id, key_type), key_at_padded(id, key_type, 0));
            }
        }
    }

    #[test]
    fn test_absent_key_from_is_above_its_key_and_not_a_fixture_key() {
        for key_type in KeyType::ALL {
            for pad in pads(key_type) {
                for id in SAMPLE_IDS {
                    let key = key_at_padded(id, key_type, pad);
                    let absent = absent_key_from(&key);
                    assert!(absent > key, "{key} < {absent}");
                    assert!(absent < key_at_padded(id + 1, key_type, pad));
                    assert_eq!(id_of(&absent, pad), None, "{absent}");
                    assert_eq!(absent.key_type(), key_type);
                }
            }
        }
    }

    #[test]
    fn test_absent_key_near_is_absent_key_from_key_at() {
        for key_type in KeyType::ALL {
            assert_eq!(
                absent_key_near(SAMPLE_IDS[3], key_type),
                absent_key_from(&key_at(SAMPLE_IDS[3], key_type))
            );
        }
    }

    #[test]
    fn test_far_key_is_above_every_sample_key() {
        for key_type in KeyType::ALL {
            let far = far_key(key_type);
            assert_eq!(far.key_type(), key_type);
            for pad in pads(key_type) {
                for id in SAMPLE_IDS {
                    let key = key_at_padded(id, key_type, pad);
                    assert!(far > key, "{far} <= {key}");
                    assert!(far > absent_key_from(&key));
                }
            }
        }
    }

    #[test]
    fn test_id_of_inverts_key_at_padded() {
        for key_type in KeyType::ALL {
            for pad in pads(key_type) {
                for id in SAMPLE_IDS {
                    assert_eq!(id_of(&key_at_padded(id, key_type, pad), pad), Some(id));
                }
            }
        }
        assert_eq!(id_of(&key_at_padded(1, KeyType::String, PAD), 0), None);
        assert_eq!(id_of(&Key::Long(-KEY_STEP.cast_signed()), 0), None);
        assert_eq!(id_of(&far_key(KeyType::Uuid), 0), None);
    }

    #[test]
    fn test_value_for_is_deterministic_and_distinct_for_neighbors() {
        for key_type in KeyType::ALL {
            for id in SAMPLE_IDS {
                let key = key_at(id, key_type);
                let value = value_for(&key);
                assert_eq!(value.len(), VALUE_BYTES);
                assert_eq!(value, value_for(&key));
                assert_ne!(value, value_for(&key_at(id + 1, key_type)));
                assert_ne!(value, value_for(&absent_key_from(&key)));
            }
        }
    }

    #[test]
    fn test_value_for_len_has_the_length_and_shares_the_prefix() {
        let key = key_at(SAMPLE_IDS[5], KeyType::Long);
        let short = value_for_len(&key, OTHER_LEN);
        assert_eq!(short.len(), OTHER_LEN);
        assert_eq!(&value_for(&key)[..OTHER_LEN], &short[..]);
    }
}
