//! A key from command-line text in the form for its key type (`parse`).
//! The key type comes from the served generation; the text alone does not
//! say whether `1` is a long or a string.

use icetray_core::key::HEX_DIGITS_PER_BYTE;
use icetray_core::{Key, KeyType};

use crate::error::KeyTextFault;

/// The prefix `Key`'s `Display` puts before a binary key's hex; optional on
/// input, in either case.
const HEX_PREFIXES: [&str; 2] = ["0x", "0X"];

/// The base of a hex digit.
const HEX_RADIX: u32 = 16;

/// Parses `text` as a key of `key_type`. A long is `i64::from_str`. A string
/// is the text as given: the shell already removed its quoting, and `Key`'s
/// `Display` quotes a string with `Debug` escaping, so a string key printed
/// by `Display` is passed back without those quotes. A binary key is hex,
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
        KeyType::Binary => parse_hex(text).map(Key::Bytes),
        KeyType::Uuid => uuid::Uuid::parse_str(text)
            .map(|uuid| Key::Uuid(uuid.into_bytes()))
            .map_err(|_| KeyTextFault::NotAUuid {
                text: text.to_string(),
            }),
    }
}

/// The bytes `text` spells in hex. The length is checked before the digits,
/// so `abc` is an odd length and `zz` is not hex. Every byte of a pair must
/// be a hex digit before the pair is converted: `u8::from_str_radix` accepts
/// a leading `+`, which is not hex.
fn parse_hex(text: &str) -> Result<Vec<u8>, KeyTextFault> {
    let hex = HEX_PREFIXES
        .iter()
        .find_map(|prefix| text.strip_prefix(prefix))
        .unwrap_or(text);
    if !hex.len().is_multiple_of(HEX_DIGITS_PER_BYTE) {
        return Err(KeyTextFault::OddHexLength {
            text: text.to_string(),
        });
    }
    let not_hex = || KeyTextFault::NotHex {
        text: text.to_string(),
    };
    hex.as_bytes()
        .chunks(HEX_DIGITS_PER_BYTE)
        .map(|pair| {
            if !pair.iter().all(u8::is_ascii_hexdigit) {
                return Err(not_hex());
            }
            let digits = std::str::from_utf8(pair).map_err(|_| not_hex())?;
            u8::from_str_radix(digits, HEX_RADIX).map_err(|_| not_hex())
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

    #[test]
    fn test_parse_long_round_trips_display() {
        let key = Key::Long(SAMPLE_LONG);
        assert_eq!(parse(&key.to_string(), KeyType::Long), Ok(key));
        assert_eq!(parse("0", KeyType::Long), Ok(Key::Long(0)));
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
    fn test_parse_binary_round_trips_display() {
        let key = Key::Bytes(SAMPLE_BYTES.to_vec());
        let displayed = key.to_string();
        assert!(displayed.starts_with("0x"));
        assert_eq!(parse(&displayed, KeyType::Binary), Ok(key));
    }

    #[test]
    fn test_parse_binary_accepts_uppercase_and_no_prefix() {
        let key = Key::Bytes(SAMPLE_BYTES.to_vec());
        let displayed = key.to_string();
        let bare = displayed.trim_start_matches("0x");
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
    fn test_parse_uuid_round_trips_display_and_simple_form() {
        let key = Key::Uuid(SAMPLE_UUID);
        let hyphenated = key.to_string();
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
