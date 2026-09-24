//! Time conversions. Elapsed time is a `Duration` and a wall-clock instant
//! a `jiff::Timestamp` everywhere else in the crate; this is the one module
//! where either becomes text or a number and back, so no call site carries
//! its own unit arithmetic.

use std::time::{Duration, SystemTime};

/// A duration as a float of seconds, the unit a Prometheus histogram takes
/// and the unit its bucket bounds are declared in. The one place a
/// `Duration` becomes a bare number for a metric.
pub fn seconds(duration: Duration) -> f64 {
    duration.as_secs_f64()
}

/// An elapsed time as whole microseconds, for a one-shot subcommand's
/// output. Saturates at `u64::MAX`, which is over five hundred thousand
/// years; no one-shot reaches it.
pub fn micros(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX)
}

/// The wall clock now as a timestamp, for every caller that keeps time in
/// `jiff`; a caller that needs "now" never names the clock itself. The one
/// other read of the clock is `system_now`, for the SDK boundary.
pub fn now() -> jiff::Timestamp {
    jiff::Timestamp::now()
}

/// The wall clock now as a `SystemTime`, for the one boundary that speaks
/// it: the AWS SDK hands a credential's expiry back as a `SystemTime`, and
/// the credential cache compares and offsets it with `SystemTime`'s own
/// methods rather than converting a value that can fall outside `jiff`'s
/// range. Every other caller uses `now`.
pub fn system_now() -> SystemTime {
    SystemTime::now()
}

/// Seconds from `since` to now: the `generation_age_seconds` gauge, from a
/// served snapshot's own timestamp. A `since` in the future gives a negative
/// age, kept as measured rather than clamped, so a writer's clock running
/// ahead shows as such.
pub fn age_seconds(since: jiff::Timestamp) -> f64 {
    now().duration_since(since).as_secs_f64()
}

/// A wall-clock timestamp as log text: RFC 3339 in UTC with a `Z` offset,
/// through jiff's own printer, with the fraction of a second only when it
/// is non-zero. The one place a timestamp becomes text.
pub fn log_timestamp(at: jiff::Timestamp) -> String {
    at.to_string()
}

/// A count of milliseconds since the Unix epoch, as Iceberg records a
/// snapshot's timestamp, as a `jiff::Timestamp`. The one place an epoch
/// millisecond becomes a timestamp. `Err` for a value outside the range jiff
/// represents, which spans years -9999 to 9999.
pub(crate) fn timestamp_from_millis(millis: i64) -> Result<jiff::Timestamp, jiff::Error> {
    jiff::Timestamp::from_millisecond(millis)
}

/// The epoch-millisecond count `step` after `base`, an epoch-millisecond
/// count as Iceberg records a snapshot's timestamp and the metadata's last
/// update: how a fixture writer, the testkit's or core's tests', stamps a
/// commit later than the metadata's last update, which Iceberg requires.
/// No serving path writes a commit; this is public because the testkit is
/// another crate. `step` counts in whole milliseconds, the unit Iceberg
/// stamps in, and its remainder is dropped. Saturates at `i64::MAX`,
/// outside the range `timestamp_from_millis` admits, so an overflow reads
/// back as `SnapshotTimestampOutOfRange` and never as another time.
pub fn millis_after(base: i64, step: Duration) -> i64 {
    base.saturating_add(i64::try_from(step.as_millis()).unwrap_or(i64::MAX))
}

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

    /// The parser of the friendly grammar, in its default configuration,
    /// which accepts every unit designator and no bare number.
    static PARSER: SpanParser = SpanParser::new();
    /// The printer of the compact form (`1m 30s`, not `1 minute 30
    /// seconds`), so a written value is short and parses back equal.
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
    /// A duration with a fractional second, so the float carries it.
    const ONE_AND_A_HALF_SECONDS: Duration = Duration::from_millis(1500);
    /// The float `ONE_AND_A_HALF_SECONDS` becomes; exactly representable.
    const ONE_AND_A_HALF: f64 = 1.5;

    fn parse(text: &str) -> Result<Holder, serde_json::Error> {
        serde_json::from_str(&format!("{{\"elapsed\": \"{text}\"}}"))
    }

    #[test]
    fn test_seconds_carries_the_fraction() {
        assert_eq!(
            super::seconds(ONE_AND_A_HALF_SECONDS).to_bits(),
            ONE_AND_A_HALF.to_bits()
        );
    }

    #[test]
    fn test_seconds_zero_is_zero() {
        assert_eq!(super::seconds(Duration::ZERO).to_bits(), 0.0f64.to_bits());
    }

    /// The microseconds `ONE_AND_A_HALF_SECONDS` holds.
    const ONE_AND_A_HALF_SECONDS_MICROS: u64 = 1_500_000;

    #[test]
    fn test_micros_is_the_whole_microseconds() {
        assert_eq!(
            super::micros(ONE_AND_A_HALF_SECONDS),
            ONE_AND_A_HALF_SECONDS_MICROS
        );
        assert_eq!(super::micros(Duration::ZERO), 0);
    }

    /// One nanosecond short of a microsecond; truncates to 0.
    const JUST_UNDER_A_MICRO: Duration = Duration::from_nanos(999);
    /// One nanosecond short of two microseconds; truncates to 1.
    const JUST_UNDER_TWO_MICROS: Duration = Duration::from_nanos(1_999);

    #[test]
    fn test_micros_truncates_below_a_microsecond() {
        assert_eq!(super::micros(JUST_UNDER_A_MICRO), 0);
        assert_eq!(super::micros(JUST_UNDER_TWO_MICROS), 1);
    }

    #[test]
    fn test_micros_saturates_at_u64_max() {
        assert_eq!(super::micros(Duration::MAX), u64::MAX);
    }

    /// An age far enough in the past that clock skew between two reads of
    /// `now` cannot bring it below `AGE_LOWER_BOUND`.
    const AGE_OFFSET: Duration = Duration::from_secs(3_600);
    /// Half of `AGE_OFFSET`; the measured age is at least this.
    const AGE_LOWER_BOUND: f64 = 1_800.0;
    /// Twice `AGE_OFFSET`; the measured age is at most this.
    const AGE_UPPER_BOUND: f64 = 7_200.0;

    #[test]
    fn test_age_seconds_of_the_past_is_positive() {
        let since = super::now()
            .checked_sub(AGE_OFFSET)
            .expect("an hour ago is representable");
        let age = super::age_seconds(since);
        assert!(age > AGE_LOWER_BOUND && age < AGE_UPPER_BOUND, "{age}");
    }

    #[test]
    fn test_age_seconds_of_the_future_is_negative() {
        let since = super::now()
            .checked_add(AGE_OFFSET)
            .expect("an hour ahead is representable");
        let age = super::age_seconds(since);
        assert!(age < -AGE_LOWER_BOUND && age > -AGE_UPPER_BOUND, "{age}");
    }

    #[test]
    fn test_log_timestamp_is_rfc3339_in_utc() {
        assert_eq!(
            super::log_timestamp(jiff::Timestamp::UNIX_EPOCH),
            "1970-01-01T00:00:00Z"
        );
        let with_millis = super::timestamp_from_millis(EPOCH_MILLIS).unwrap();
        assert_eq!(
            super::log_timestamp(with_millis),
            "2023-11-14T22:13:20.123Z"
        );
    }

    #[test]
    fn test_log_timestamp_round_trips_through_jiff() {
        let at = super::timestamp_from_millis(EPOCH_MILLIS).unwrap();
        let back: jiff::Timestamp = super::log_timestamp(at).parse().unwrap();
        assert_eq!(back, at);
    }

    /// Milliseconds since the epoch with a fraction of a second, so the
    /// round trip carries the milliseconds.
    const EPOCH_MILLIS: i64 = 1_700_000_000_123;

    #[test]
    fn test_timestamp_from_millis_zero_is_the_epoch() {
        assert_eq!(
            super::timestamp_from_millis(0).unwrap(),
            jiff::Timestamp::UNIX_EPOCH
        );
    }

    #[test]
    fn test_timestamp_from_millis_round_trips() {
        assert_eq!(
            super::timestamp_from_millis(EPOCH_MILLIS)
                .unwrap()
                .as_millisecond(),
            EPOCH_MILLIS
        );
    }

    #[test]
    fn test_timestamp_from_millis_rejects_out_of_range() {
        assert!(super::timestamp_from_millis(i64::MAX).is_err());
        assert!(super::timestamp_from_millis(i64::MIN).is_err());
    }

    /// The step a fixture commit is stamped after the metadata's last update,
    /// and the same step in the epoch milliseconds it adds.
    const COMMIT_STEP: Duration = Duration::from_millis(3);
    const COMMIT_STEP_MILLIS: i64 = 3;
    /// Below a millisecond: dropped, as Iceberg's timestamps cannot hold it.
    const SUB_MILLISECOND: Duration = Duration::from_micros(999);

    #[test]
    fn test_millis_after_adds_the_step() {
        assert_eq!(
            super::millis_after(EPOCH_MILLIS, COMMIT_STEP),
            EPOCH_MILLIS + COMMIT_STEP_MILLIS
        );
        assert_eq!(
            super::millis_after(EPOCH_MILLIS, COMMIT_STEP + SUB_MILLISECOND),
            EPOCH_MILLIS + COMMIT_STEP_MILLIS
        );
    }

    #[test]
    fn test_millis_after_saturates_outside_i64() {
        assert_eq!(super::millis_after(i64::MAX, COMMIT_STEP), i64::MAX);
        assert_eq!(super::millis_after(EPOCH_MILLIS, Duration::MAX), i64::MAX);
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
