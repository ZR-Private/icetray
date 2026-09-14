//! Time conversions. Elapsed time is a `Duration` everywhere else in the
//! crate; this is the one module where a duration becomes text or a number
//! and back, so no call site carries its own unit arithmetic.

/// Serde `with` module for a `std::time::Duration` field written in jiff's
/// friendly form: `"10s"`, `"250ms"`, `"1m 30s"`. Only that grammar is
/// accepted: a negative duration (`"-1s"`, `"1s ago"`) and text without a
/// unit designator (`"10"`) are errors. Serialization writes the compact
/// friendly form, so a value written by this module reads back equal.
pub mod friendly_duration {
    use std::time::Duration;

    use jiff::fmt::friendly::{Designator, SpanParser, SpanPrinter};
    use serde::de::Error as _;
    use serde::{Deserialize, Deserializer, Serializer};

    static PARSER: SpanParser = SpanParser::new();
    static PRINTER: SpanPrinter = SpanPrinter::new().designator(Designator::Compact);

    /// Parses the friendly form; anything else is a deserialization error.
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
        let text = String::deserialize(deserializer)?;
        PARSER
            .parse_unsigned_duration(&text)
            .map_err(D::Error::custom)
    }

    /// Writes the compact friendly form, units balanced up to hours.
    pub fn serialize<S: Serializer>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&PRINTER.unsigned_duration_to_string(duration))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde::{Deserialize, Serialize};

    /// A field carrying the `with` module, so the tests go through serde
    /// exactly as a configuration field does.
    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct Holder {
        #[serde(with = "super::friendly_duration")]
        elapsed: Duration,
    }

    /// The value `"10s"` denotes.
    const TEN_SECONDS: Duration = Duration::from_secs(10);
    /// The value `"1m"` denotes.
    const ONE_MINUTE: Duration = Duration::from_secs(60);
    /// The value `"250ms"` denotes.
    const QUARTER_SECOND: Duration = Duration::from_millis(250);
    /// A value that spans two units, `"1m 30s"`, for the round trip.
    const NINETY_SECONDS: Duration = Duration::from_secs(90);

    fn parse(text: &str) -> Result<Holder, serde_json::Error> {
        serde_json::from_str(&format!("{{\"elapsed\": \"{text}\"}}"))
    }

    #[test]
    fn test_deserialize_parses_seconds() {
        assert_eq!(parse("10s").unwrap().elapsed, TEN_SECONDS);
    }

    #[test]
    fn test_deserialize_parses_minutes() {
        assert_eq!(parse("1m").unwrap().elapsed, ONE_MINUTE);
    }

    #[test]
    fn test_deserialize_parses_milliseconds() {
        assert_eq!(parse("250ms").unwrap().elapsed, QUARTER_SECOND);
    }

    #[test]
    fn test_deserialize_rejects_negative() {
        assert!(parse("-1s").is_err());
        assert!(parse("1s ago").is_err());
    }

    #[test]
    fn test_deserialize_rejects_bare_number() {
        assert!(parse("10").is_err());
    }

    #[test]
    fn test_serialize_round_trips_compact_form() {
        let holder = Holder {
            elapsed: NINETY_SECONDS,
        };
        let json = serde_json::to_string(&holder).unwrap();
        assert_eq!(json, "{\"elapsed\":\"1m 30s\"}");
        let back: Holder = serde_json::from_str(&json).unwrap();
        assert_eq!(back, holder);
    }
}
