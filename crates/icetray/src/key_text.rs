//! A key from command-line text in the form for its key type (`parse`),
//! and a key as that text (`text`), which `parse` reads back as the same
//! key. The key type comes from the table's metadata, read before any data
//! file is (`Table::resolve_key_type`); the text alone does not say whether
//! `1` is a long or a string.

use icetray_core::key::HEX_DIGITS_PER_BYTE;
use icetray_core::{Key, KeyType};

use crate::error::KeyTextFault;

/// The prefix `Key`'s `Display` puts before a binary key's hex; optional on
/// input, in either case.
const HEX_PREFIXES: [&str; 2] = ["0x", "0X"];

/// The base of a hex digit.
const HEX_RADIX: u32 = 16;

/// Parses `text` as a key of `key_type`. A long is `i64::from_str`. A string
/// is the text as given: the shell already removed its quoting, and quotes
/// inside the text are part of the key. A binary key is hex,
/// lowercase or uppercase, with or without a `0x` prefix, an even number of
/// digits. A uuid is any form `uuid::Uuid::parse_str` reads, hyphenated or
/// simple, into its sixteen bytes.
pub fn parse(text: &str, key_type: KeyType) -> Result<Key, KeyTextFault> {
    match key_type {
        KeyType::Long => text
            .parse()
            .map(Key::Long)
            .map_err(|_| KeyTextFault::NotALong {
                text: text.to_string(),
            }),
        KeyType::String => Ok(Key::Str(text.to_string())),
        KeyType::Binary => parse_hex(text)
            .map(Key::Bytes)
            .map_err(|fault| match fault {
                HexFault::OddLength => KeyTextFault::OddHexLength {
                    text: text.to_string(),
                },
                HexFault::NotHex => KeyTextFault::NotHex {
                    text: text.to_string(),
                },
            }),
        KeyType::Uuid => uuid::Uuid::parse_str(text)
            .map(|uuid| Key::Uuid(uuid.into_bytes()))
            .map_err(|_| KeyTextFault::NotAUuid {
                text: text.to_string(),
            }),
    }
}

/// `key` in the form `parse` reads back as `key` under its own key type: a
/// long as its digits, a string as its text with nothing added, a binary
/// key as `0x` and lowercase hex, a uuid hyphenated. That is `Key`'s
/// `Display` for every type but a string, whose `Display` adds `Debug`
/// quotes and escapes to set it apart from a number in prose; a lookup line
/// carries the key as a JSON string, which needs neither.
pub fn text(key: &Key) -> String {
    match key {
        Key::Str(text) => text.clone(),
        Key::Long(_) | Key::Bytes(_) | Key::Uuid(_) => key.to_string(),
    }
}

/// Why text is not bytes in hex (`parse_hex`); the caller names the text
/// and what it was for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum HexFault {
    /// An odd number of digits, so a byte is half given.
    #[error("an odd number of hex digits")]
    OddLength,
    /// A character that is not a hex digit.
    #[error("not hex")]
    NotHex,
}

/// The bytes `text` spells in hex, lowercase or uppercase, with or without
/// a `0x` prefix: the form of a binary key and of a binary or fixed order
/// value. The length is checked before the digits, so `abc` is an odd
/// length and `zz` is not hex. Every byte of a pair must be a hex digit
/// before the pair is converted: `u8::from_str_radix` accepts a leading
/// `+`, which is not hex.
pub(crate) fn parse_hex(text: &str) -> Result<Vec<u8>, HexFault> {
    let hex = HEX_PREFIXES
        .iter()
        .find_map(|prefix| text.strip_prefix(prefix))
        .unwrap_or(text);
    if !hex.len().is_multiple_of(HEX_DIGITS_PER_BYTE) {
        return Err(HexFault::OddLength);
    }
    hex.as_bytes()
        .chunks(HEX_DIGITS_PER_BYTE)
        .map(|pair| {
            if !pair.iter().all(u8::is_ascii_hexdigit) {
                return Err(HexFault::NotHex);
            }
            let digits = std::str::from_utf8(pair).map_err(|_| HexFault::NotHex)?;
            u8::from_str_radix(digits, HEX_RADIX).map_err(|_| HexFault::NotHex)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use icetray_core::key::UUID_BYTES;

    use super::*;

    /// A long with every digit position and a sign.
    const SAMPLE_LONG: i64 = -1_234_567_890_123;

    /// Bytes whose hex has both letter and digit forms and a zero byte.
    const SAMPLE_BYTES: [u8; 4] = [0x00, 0x4b, 0xab, 0xff];

    /// A uuid whose bytes are all distinct.
    const SAMPLE_UUID: [u8; UUID_BYTES] = [
        0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x10, 0x32, 0x54, 0x76, 0x98, 0xba, 0xdc,
        0xfe,
    ];

    /// A string key with a space, both quote characters, a backslash, and a
    /// tab, which `Debug` or a JSON string escapes, and non-ASCII text,
    /// which the key's text carries unchanged.
    const AWKWARD_STRING: &str = "a \"quoted\" 'key' with a \\ and a\ttab: caf\u{e9} \u{1f9ca}";

    /// One key of every type and the edges of each: the text `text` gives
    /// for each must parse back to it.
    fn round_trip_keys() -> Vec<Key> {
        vec![
            Key::Long(SAMPLE_LONG),
            Key::Long(0),
            Key::Long(i64::MIN),
            Key::Long(i64::MAX),
            Key::Str(AWKWARD_STRING.to_string()),
            Key::Str("user:0000000000000042".to_string()),
            Key::Str("\"already quoted\"".to_string()),
            Key::Str("-5".to_string()),
            Key::Str(String::new()),
            Key::Bytes(SAMPLE_BYTES.to_vec()),
            Key::Bytes(Vec::new()),
            Key::Uuid(SAMPLE_UUID),
            Key::Uuid([0; UUID_BYTES]),
        ]
    }

    #[test]
    fn test_text_parses_back_to_the_key_for_every_type() {
        let keys = round_trip_keys();
        for key_type in KeyType::ALL {
            assert!(
                keys.iter().any(|key| key.key_type() == key_type),
                "no sample of {key_type}"
            );
        }
        for key in keys {
            assert_eq!(parse(&text(&key), key.key_type()), Ok(key.clone()), "{key}");
        }
    }

    #[test]
    fn test_text_of_a_string_is_the_string_itself() {
        let key = Key::Str(AWKWARD_STRING.to_string());
        assert_eq!(text(&key), AWKWARD_STRING);
        assert_ne!(text(&key), key.to_string());
    }

    #[test]
    fn test_text_of_a_long_binary_and_uuid_key_is_its_documented_form() {
        assert_eq!(text(&Key::Long(SAMPLE_LONG)), "-1234567890123");
        assert_eq!(text(&Key::Bytes(SAMPLE_BYTES.to_vec())), "0x004babff");
        assert_eq!(
            text(&Key::Uuid(SAMPLE_UUID)),
            "01234567-89ab-cdef-1032-547698badcfe"
        );
    }

    #[test]
    fn test_parse_long_accepts_its_digits() {
        assert_eq!(parse("0", KeyType::Long), Ok(Key::Long(0)));
        assert_eq!(parse("-5", KeyType::Long), Ok(Key::Long(-5)));
        assert_eq!(
            parse(&i64::MAX.to_string(), KeyType::Long),
            Ok(Key::Long(i64::MAX))
        );
    }

    #[test]
    fn test_parse_long_refuses_text_that_is_not_a_long() {
        for text in ["abc", "1.5", "", "9223372036854775808"] {
            assert_eq!(
                parse(text, KeyType::Long),
                Err(KeyTextFault::NotALong {
                    text: text.to_string(),
                }),
                "{text:?}"
            );
        }
    }

    #[test]
    fn test_parse_string_is_the_text_as_given() {
        let text = "user:0000000000000042";
        assert_eq!(parse(text, KeyType::String), Ok(Key::Str(text.to_string())));
        // Display quotes a string; the CLI does not strip the quotes.
        let displayed = Key::Str(text.to_string()).to_string();
        assert_eq!(parse(&displayed, KeyType::String), Ok(Key::Str(displayed)));
        assert_eq!(parse("", KeyType::String), Ok(Key::Str(String::new())));
    }

    #[test]
    fn test_parse_binary_accepts_uppercase_and_no_prefix() {
        let key = Key::Bytes(SAMPLE_BYTES.to_vec());
        let printed = text(&key);
        assert!(printed.starts_with("0x"));
        let bare = printed.trim_start_matches("0x");
        assert_eq!(parse(bare, KeyType::Binary), Ok(key.clone()));
        assert_eq!(
            parse(&bare.to_uppercase(), KeyType::Binary),
            Ok(key.clone())
        );
        assert_eq!(
            parse(&format!("0X{}", bare.to_uppercase()), KeyType::Binary),
            Ok(key)
        );
        assert_eq!(parse("0x", KeyType::Binary), Ok(Key::Bytes(Vec::new())));
    }

    #[test]
    fn test_parse_binary_refuses_an_odd_length() {
        for text in ["0xabc", "abc", "0x0"] {
            assert_eq!(
                parse(text, KeyType::Binary),
                Err(KeyTextFault::OddHexLength {
                    text: text.to_string(),
                }),
                "{text:?}"
            );
        }
    }

    #[test]
    fn test_parse_binary_refuses_non_hex() {
        for text in ["0xzz", "abcg", "0x-1", "0x+1", "+f", "éé"] {
            assert_eq!(
                parse(text, KeyType::Binary),
                Err(KeyTextFault::NotHex {
                    text: text.to_string(),
                }),
                "{text:?}"
            );
        }
    }

    #[test]
    fn test_parse_uuid_accepts_the_simple_form() {
        let key = Key::Uuid(SAMPLE_UUID);
        let hyphenated = text(&key);
        assert_eq!(parse(&hyphenated, KeyType::Uuid), Ok(key.clone()));
        let simple: String = hyphenated.chars().filter(|c| *c != '-').collect();
        assert_eq!(parse(&simple, KeyType::Uuid), Ok(key.clone()));
        assert_eq!(parse(&simple.to_uppercase(), KeyType::Uuid), Ok(key));
    }

    #[test]
    fn test_parse_uuid_refuses_text_that_is_not_a_uuid() {
        for text in [
            "abc",
            "",
            "0x0123456789abcdef1032547698badcfe",
            "12345678-1234",
        ] {
            assert_eq!(
                parse(text, KeyType::Uuid),
                Err(KeyTextFault::NotAUuid {
                    text: text.to_string(),
                }),
                "{text:?}"
            );
        }
    }
}
