//! An order value from command-line text in the form for its table's order
//! type (`parse`), and an order value as that text on a line (`OrderText`),
//! which `parse` reads back as the same value, so a `scan` line's `next`
//! passed back as `--start` or `--before` pages on. The order type comes
//! from the table's metadata, read before any data file is
//! (`Table::resolve_layout`); the text alone does not say whether `10` is a
//! long or a string.
//!
//! A date or timestamp bound is a long of the type's unit or ISO 8601 text
//! for the same value; a line carries the long, so a `next` pages on
//! whichever form the first bound was given in.
//!
//! A string travels as its text with two escapes, `\\` for a backslash and
//! `\0` for a zero byte: no command-line argument can hold a zero byte, and
//! the `next` of a string read oldest first always ends in one, the zero
//! byte `OrderValue::successor` appends.

use icetray_core::time;
use icetray_core::{BoundSide, OrderType, OrderValue, OrderVariant};
use serde::Serialize;

use crate::error::OrderTextFault;
use crate::key_text::{HexFault, parse_hex};

/// What starts an escape in a string's text.
const ESCAPE: char = '\\';

/// What follows `ESCAPE` for a zero byte.
const ESCAPED_ZERO: char = '0';

/// Parses `text`, given for `bound`, as an order value of `order_type`. A
/// value of an integer or time type is `i64::from_str`, in the type's
/// Iceberg unit: days for `date`, microseconds or nanoseconds for a
/// timestamp, as the wire and the doctor's lines carry it; digits alone
/// are always that long, so `20260101` on a `date` is a day count. Text
/// that is no integer is, for a date or timestamp type, ISO 8601 in the
/// one form `time::epoch_count` reads, `YYYY-MM-DD` or
/// `YYYY-MM-DDTHH:MM:SS[.fraction]` with `Z` or an offset, converted to
/// the same unit; no other type takes it. A string is the
/// text as given with `\\` read as a backslash and `\0` as a zero byte;
/// any other backslash is a fault, so a typed path cannot pass for an
/// escape. A binary or fixed value is hex as a binary key is, lowercase or
/// uppercase, with or without `0x`, an even number of digits. No width is
/// checked: a bound need not be a value the column can hold.
pub fn parse(
    text: &str,
    order_type: OrderType,
    bound: BoundSide,
) -> Result<OrderValue, OrderTextFault> {
    match order_type.variant() {
        OrderVariant::Long => {
            let not_a_long = || OrderTextFault::NotALong {
                bound,
                text: text.to_string(),
            };
            if let Ok(count) = text.parse() {
                return Ok(OrderValue::Long(count));
            }
            let Some(unit) = order_type.epoch_unit().filter(|_| !is_integer(text)) else {
                return Err(not_a_long());
            };
            time::epoch_count(text, unit)
                .map(OrderValue::Long)
                .map_err(|fault| OrderTextFault::NotATime {
                    bound,
                    order_type,
                    unit,
                    text: text.to_string(),
                    fault,
                })
        }
        OrderVariant::Str => {
            unescape(text)
                .map(OrderValue::Str)
                .ok_or_else(|| OrderTextFault::BadEscape {
                    bound,
                    text: text.to_string(),
                })
        }
        OrderVariant::Bin => parse_hex(text)
            .map(OrderValue::Bytes)
            .map_err(|fault| match fault {
                HexFault::OddLength => OrderTextFault::OddHexLength {
                    bound,
                    text: text.to_string(),
                },
                HexFault::NotHex => OrderTextFault::NotHex {
                    bound,
                    text: text.to_string(),
                },
            }),
    }
}

/// Whether `text` is an integer as `i64::from_str` reads one, an optional
/// sign and at least one ASCII digit, whatever its size: text that fails
/// to parse as a long for its size alone is no ISO text either.
fn is_integer(text: &str) -> bool {
    let digits = text.strip_prefix(['+', '-']).unwrap_or(text);
    !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
}

/// `text` with its escapes read, or `None` for a backslash that starts
/// neither, a trailing one included.
fn unescape(text: &str) -> Option<String> {
    let mut value = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        if c != ESCAPE {
            value.push(c);
            continue;
        }
        match chars.next()? {
            ESCAPE => value.push(ESCAPE),
            ESCAPED_ZERO => value.push('\0'),
            _ => return None,
        }
    }
    Some(value)
}

/// `value` with a backslash and a zero byte escaped, as `unescape` reads it
/// back.
fn escape(value: &str) -> String {
    let mut text = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            ESCAPE => {
                text.push(ESCAPE);
                text.push(ESCAPE);
            }
            '\0' => {
                text.push(ESCAPE);
                text.push(ESCAPED_ZERO);
            }
            other => text.push(other),
        }
    }
    text
}

/// An order value as a line carries it for `parse` to read back: a long as
/// a JSON number, its digits the text; a string as a JSON string of its
/// escaped text; bytes as a JSON string of `0x` and lowercase hex, as
/// `OrderValue` displays them.
#[derive(Clone, Copy, Debug)]
pub struct OrderText<'a>(pub &'a OrderValue);

impl Serialize for OrderText<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0 {
            OrderValue::Long(value) => serializer.serialize_i64(*value),
            OrderValue::Str(value) => serializer.serialize_str(&escape(value)),
            OrderValue::Bytes(_) => serializer.collect_str(self.0),
        }
    }
}

#[cfg(test)]
mod tests {
    use icetray_core::time::EpochTextFault;
    use serde_json::Value as Json;

    use super::*;

    /// One order type of each variant.
    const LONG: OrderType = OrderType::Long;
    const STRING: OrderType = OrderType::String;
    const BINARY: OrderType = OrderType::Binary;

    /// A string with both escapes, a quote, and non-ASCII text.
    const AWKWARD_STRING: &str = "a \\ b\0c \"d\" caf\u{e9}";

    /// The text a JSON line's `OrderText` gives for `value`: a string's
    /// contents, or a number's digits.
    fn printed(value: &OrderValue) -> String {
        match serde_json::to_value(OrderText(value)).unwrap() {
            Json::String(text) => text,
            Json::Number(number) => number.to_string(),
            other @ (Json::Null | Json::Bool(_) | Json::Array(_) | Json::Object(_)) => {
                panic!("an order value's text is a string or a number, got {other}")
            }
        }
    }

    #[test]
    fn test_order_text_parses_back_to_the_value_for_every_variant() {
        let cases = [
            (OrderValue::Long(i64::MIN), LONG),
            (OrderValue::Long(i64::MAX), LONG),
            (OrderValue::Long(0), OrderType::Date),
            (OrderValue::Str(AWKWARD_STRING.to_string()), STRING),
            (OrderValue::Str("ab\0".to_string()), STRING),
            (OrderValue::Str(String::new()), STRING),
            (OrderValue::Bytes(vec![0x00, 0xab, 0xff]), BINARY),
            (OrderValue::Bytes(vec![1, 2, 0]), OrderType::Fixed(2)),
            (OrderValue::Bytes(Vec::new()), BINARY),
        ];
        for (value, order_type) in cases {
            let text = printed(&value);
            assert!(!text.contains('\0'), "{text:?} holds no zero byte");
            assert_eq!(
                parse(&text, order_type, BoundSide::Start),
                Ok(value.clone()),
                "{text:?}"
            );
        }
    }

    #[test]
    fn test_order_text_is_a_number_for_a_long_and_a_string_otherwise() {
        let long = serde_json::to_value(OrderText(&OrderValue::Long(-7))).unwrap();
        assert_eq!(long, Json::from(-7));
        let bytes = serde_json::to_value(OrderText(&OrderValue::Bytes(vec![0, 0xab]))).unwrap();
        assert_eq!(bytes, Json::String("0x00ab".to_string()));
        let text = serde_json::to_value(OrderText(&OrderValue::Str("a\\b\0".to_string()))).unwrap();
        assert_eq!(text, Json::String("a\\\\b\\0".to_string()));
    }

    #[test]
    fn test_parse_string_reads_text_with_no_escape_as_given() {
        for text in ["", "user:42", "\"quoted\"", "-5", "caf\u{e9}"] {
            assert_eq!(
                parse(text, STRING, BoundSide::Before),
                Ok(OrderValue::Str(text.to_string()))
            );
        }
    }

    #[test]
    fn test_parse_string_refuses_a_backslash_that_is_no_escape() {
        for text in ["C:\\temp", "trailing\\", "\\n", "\\x00"] {
            assert_eq!(
                parse(text, STRING, BoundSide::Before),
                Err(OrderTextFault::BadEscape {
                    bound: BoundSide::Before,
                    text: text.to_string(),
                }),
                "{text:?}"
            );
        }
    }

    #[test]
    fn test_parse_long_refuses_text_that_is_not_a_long_and_names_the_bound() {
        for text in [
            "abc",
            "1.5",
            "",
            "2026-10-20T00:00:00Z",
            "9223372036854775808",
        ] {
            assert_eq!(
                parse(text, LONG, BoundSide::Start),
                Err(OrderTextFault::NotALong {
                    bound: BoundSide::Start,
                    text: text.to_string(),
                }),
                "{text:?}"
            );
        }
    }

    /// One instant in each form a time bound takes, per time type: the
    /// ISO text and the long of the type's unit it names, on both sides of
    /// the epoch and on a leap day.
    const SAME_INSTANT: [(OrderType, &str, &str); 11] = [
        (OrderType::Date, "2026-01-01", "20454"),
        (OrderType::Date, "1969-12-31", "-1"),
        (OrderType::Date, "2024-02-29", "19782"),
        (
            OrderType::Timestamp,
            "2026-01-01T00:00:00.000001Z",
            "1767225600000001",
        ),
        (OrderType::Timestamp, "1969-12-31T23:59:59.999999Z", "-1"),
        (
            OrderType::TimestampTz,
            "2026-01-01T01:00:00+01:00",
            "1767225600000000",
        ),
        (
            OrderType::TimestampTz,
            "2024-02-29T00:00:00Z",
            "1709164800000000",
        ),
        (
            OrderType::TimestampNs,
            "2026-01-01T00:00:00.000000001Z",
            "1767225600000000001",
        ),
        (
            OrderType::TimestampNs,
            "1969-12-31T23:59:59.999999999Z",
            "-1",
        ),
        (
            OrderType::TimestampTzNs,
            "2025-12-31T19:00:00-05:00",
            "1767225600000000000",
        ),
        (OrderType::TimestampTzNs, "1969-12-31T23:00:00-01:00", "0"),
    ];

    /// The `NotATime` fault `parse` gives for `text` as `order_type`.
    fn not_a_time(order_type: OrderType, text: &str, fault: EpochTextFault) -> OrderTextFault {
        OrderTextFault::NotATime {
            bound: BoundSide::Start,
            order_type,
            unit: order_type.epoch_unit().expect("a time type"),
            text: text.to_string(),
            fault,
        }
    }

    #[test]
    fn test_parse_time_takes_iso_text_and_the_long_it_names_as_one_bound() {
        for (order_type, iso, long) in SAME_INSTANT {
            let from_iso = parse(iso, order_type, BoundSide::Start);
            assert!(from_iso.is_ok(), "{iso} as {order_type}: {from_iso:?}");
            assert_eq!(
                from_iso,
                parse(long, order_type, BoundSide::Start),
                "{iso} as {order_type}"
            );
        }
    }

    #[test]
    fn test_parse_time_refuses_digits_below_a_microsecond_type_s_unit() {
        let text = "2026-01-01T00:00:00.0000001Z";
        for order_type in [OrderType::Timestamp, OrderType::TimestampTz] {
            assert_eq!(
                parse(text, order_type, BoundSide::Start),
                Err(not_a_time(order_type, text, EpochTextFault::BelowTheUnit))
            );
        }
        assert_eq!(
            parse(text, OrderType::TimestampNs, BoundSide::Start),
            parse(
                "1767225600000000100",
                OrderType::TimestampNs,
                BoundSide::Start
            )
        );
    }

    #[test]
    fn test_parse_time_refuses_text_out_of_its_type_s_range_or_form() {
        let cases = [
            (OrderType::Date, "10000-01-01", EpochTextFault::NotIso),
            (OrderType::Date, "-000001-01-01", EpochTextFault::NotIso),
            (OrderType::Date, "2026-02-30", EpochTextFault::NoSuchTime),
            (OrderType::Date, "2023-02-29", EpochTextFault::NoSuchTime),
            (
                OrderType::Date,
                "2026-01-01T00:00:00Z",
                EpochTextFault::NotIso,
            ),
            (
                OrderType::Timestamp,
                "2026-01-01T00:00:00",
                EpochTextFault::NotIso,
            ),
            (
                OrderType::Timestamp,
                "2026-01-01 00:00:00Z",
                EpochTextFault::NotIso,
            ),
            (
                OrderType::TimestampTz,
                "2026-01-01T00:00:00Z[America/New_York]",
                EpochTextFault::NotIso,
            ),
            (
                OrderType::TimestampTz,
                "2023-02-29T00:00:00Z",
                EpochTextFault::NoSuchTime,
            ),
            (
                OrderType::Timestamp,
                "2016-12-31T23:59:60Z",
                EpochTextFault::LeapSecond,
            ),
            (OrderType::TimestampTz, "2026-01-01", EpochTextFault::NotIso),
            (
                OrderType::TimestampTzNs,
                "2263-01-01T00:00:00Z",
                EpochTextFault::OutOfRange,
            ),
        ];
        for (order_type, text, fault) in cases {
            assert_eq!(
                parse(text, order_type, BoundSide::Start),
                Err(not_a_time(order_type, text, fault)),
                "{text} as {order_type}"
            );
        }
    }

    #[test]
    fn test_parse_date_reads_digits_alone_as_days_whatever_their_date() {
        assert_eq!(
            parse("2026", OrderType::Date, BoundSide::Start),
            parse("1975-07-20", OrderType::Date, BoundSide::Start),
            "2026 is day 2026, not a year"
        );
        for days in [
            "20260101",
            "2147483647",
            "-2147483648",
            "9223372036854775807",
        ] {
            assert_eq!(
                parse(days, OrderType::Date, BoundSide::Start),
                Ok(OrderValue::Long(days.parse().unwrap())),
                "{days}: a long, past the dates ISO text names"
            );
        }
    }

    #[test]
    fn test_parse_time_refuses_a_long_too_big_for_a_long_as_no_long() {
        for text in [
            "9223372036854775808",
            "-9223372036854775809",
            "+99999999999999999999",
        ] {
            for order_type in [
                OrderType::Date,
                OrderType::Timestamp,
                OrderType::TimestampNs,
            ] {
                assert_eq!(
                    parse(text, order_type, BoundSide::Start),
                    Err(OrderTextFault::NotALong {
                        bound: BoundSide::Start,
                        text: text.to_string(),
                    }),
                    "{text} as {order_type}"
                );
            }
        }
    }

    #[test]
    fn test_parse_takes_iso_text_for_no_type_but_a_date_or_timestamp() {
        let iso = ["2026-01-01", "2026-01-01T00:00:00Z"];
        for order_type in [OrderType::Int, OrderType::Long] {
            for text in iso {
                assert_eq!(
                    parse(text, order_type, BoundSide::Start),
                    Err(OrderTextFault::NotALong {
                        bound: BoundSide::Start,
                        text: text.to_string(),
                    })
                );
            }
        }
        for text in iso {
            assert_eq!(
                parse(text, STRING, BoundSide::Start),
                Ok(OrderValue::Str(text.to_string()))
            );
            assert!(matches!(
                parse(text, BINARY, BoundSide::Start),
                Err(OrderTextFault::NotHex { .. } | OrderTextFault::OddHexLength { .. })
            ));
        }
    }

    #[test]
    fn test_parse_binary_takes_hex_as_a_binary_key_does() {
        let value = OrderValue::Bytes(vec![0xab, 0x01]);
        for text in ["0xab01", "AB01", "0XAB01", "ab01"] {
            assert_eq!(parse(text, BINARY, BoundSide::Start), Ok(value.clone()));
        }
        assert_eq!(
            parse("0xabc", BINARY, BoundSide::Start),
            Err(OrderTextFault::OddHexLength {
                bound: BoundSide::Start,
                text: "0xabc".to_string(),
            })
        );
        assert_eq!(
            parse("zz", OrderType::Fixed(1), BoundSide::Before),
            Err(OrderTextFault::NotHex {
                bound: BoundSide::Before,
                text: "zz".to_string(),
            })
        );
    }
}
