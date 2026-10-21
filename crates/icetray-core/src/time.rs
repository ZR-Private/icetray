//! Time conversions. Elapsed time is a `Duration` and a wall-clock instant
//! a `jiff::Timestamp` everywhere else in the crate; this is the one module
//! where either becomes text or a number and back, so no call site carries
//! its own unit arithmetic.

use std::time::{Duration, SystemTime};

/// A duration as a float of seconds, the unit a Prometheus histogram takes
/// and the unit its bucket bounds are declared in. The one place a
/// `Duration` becomes a bare number for a metric.
pub(crate) fn seconds(duration: Duration) -> f64 {
    duration.as_secs_f64()
}

/// An elapsed time as whole microseconds, for a one-shot subcommand's
/// output. Saturates at `u64::MAX`, which is over five hundred thousand
/// years; no one-shot reaches it.
pub fn micros(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX)
}

/// Whole microseconds as an elapsed time: the inverse of `micros`, for a
/// caller that stores a duration as that count and reads it back.
pub fn from_micros(micros: u64) -> Duration {
    Duration::from_micros(micros)
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
pub(crate) fn system_now() -> SystemTime {
    SystemTime::now()
}

/// Seconds from `since` to now: the `generation_age_seconds` gauge, from a
/// served snapshot's own timestamp. A `since` in the future gives a negative
/// age, kept as measured rather than clamped, so a writer's clock running
/// ahead shows as such.
pub fn age_seconds(since: jiff::Timestamp) -> f64 {
    now().duration_since(since).as_secs_f64()
}

/// A wall-clock timestamp as seconds since the Unix epoch, with the
/// fraction: the `generation_swapped_at_seconds` gauge, the unit Prometheus
/// reads a time in. The one place a timestamp becomes epoch seconds.
pub(crate) fn unix_seconds(at: jiff::Timestamp) -> f64 {
    at.as_duration().as_secs_f64()
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

/// The unit an Iceberg date or timestamp counts from the Unix epoch in,
/// as its order values carry it: days for `date`, microseconds for
/// `timestamp` and `timestamptz`, nanoseconds for the two `_ns` types.
/// Display gives the unit's plural name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum EpochUnit {
    /// Days, for `date`.
    Days,
    /// Microseconds, for `timestamp` and `timestamptz`.
    Microseconds,
    /// Nanoseconds, for `timestamp_ns` and `timestamptz_ns`.
    Nanoseconds,
}

impl std::fmt::Display for EpochUnit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Days => "days",
            Self::Microseconds => "microseconds",
            Self::Nanoseconds => "nanoseconds",
        })
    }
}

/// Why text is not an ISO 8601 time that `epoch_count` turns into a count
/// of its unit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum EpochTextFault {
    /// The text is not the form the unit takes, or names a time outside
    /// the years jiff holds, -9999 to 9999: for days a calendar date
    /// `YYYY-MM-DD`, for the others an RFC 3339 instant with `Z` or an
    /// offset.
    #[error("the text is not ISO 8601 in the unit's form, within the years -9999 to 9999")]
    NotIso,
    /// The instant has digits below a microsecond that are not zero, which
    /// a count of microseconds cannot hold.
    #[error("the instant has digits below a microsecond")]
    BelowTheUnit,
    /// The instant's count of nanoseconds does not fit a signed 64-bit
    /// integer, which spans 1677-09-21 to 2262-04-11.
    #[error("the instant is outside the range a long of nanoseconds holds")]
    OutOfRange,
}

/// The count of `unit` since the Unix epoch that ISO 8601 `text` names,
/// as an Iceberg `date` or timestamp stores it: the one place such text
/// becomes an epoch count. Days take only a calendar date, `2026-01-01`,
/// exactly as jiff prints one, so a datetime's time is never dropped. The
/// other units take an RFC 3339 instant, which carries `Z` or an offset,
/// so no local time zone is ever consulted, a `timestamp`'s included,
/// though that type records none. An instant is never rounded: digits
/// below a microsecond that are not zero are `BelowTheUnit` for
/// microseconds, and an instant whose nanoseconds a long does not hold is
/// `OutOfRange`.
pub fn epoch_count(text: &str, unit: EpochUnit) -> Result<i64, EpochTextFault> {
    match unit {
        EpochUnit::Days => {
            let date: jiff::civil::Date = text.parse().map_err(|_| EpochTextFault::NotIso)?;
            if date.to_string() != text {
                return Err(EpochTextFault::NotIso);
            }
            let epoch = jiff::tz::Offset::UTC
                .to_datetime(jiff::Timestamp::UNIX_EPOCH)
                .date();
            let days = date
                .since(epoch)
                .expect("a span in days holds the distance between any two dates jiff holds")
                .get_days();
            Ok(i64::from(days))
        }
        EpochUnit::Microseconds => {
            let at: jiff::Timestamp = text.parse().map_err(|_| EpochTextFault::NotIso)?;
            let micros = at.as_microsecond();
            let whole = jiff::Timestamp::from_microsecond(micros)
                .expect("an instant truncated toward the epoch stays in jiff's range");
            if whole == at {
                Ok(micros)
            } else {
                Err(EpochTextFault::BelowTheUnit)
            }
        }
        EpochUnit::Nanoseconds => {
            let at: jiff::Timestamp = text.parse().map_err(|_| EpochTextFault::NotIso)?;
            i64::try_from(at.as_nanosecond()).map_err(|_| EpochTextFault::OutOfRange)
        }
    }
}

/// A duration as configuration text, in the friendly form
/// `friendly_duration` reads: `"0s"`, `"250ms"`, `"1m 30s"`. What a
/// message prints a configured duration as, so it reads as the file wrote
/// it; text that spelled the same duration in other units reads back in
/// the largest units that hold it.
pub fn friendly(duration: Duration) -> String {
    jiff::fmt::friendly::SpanPrinter::new().unsigned_duration_to_string(&duration)
}

/// Serde `with` module for a `std::time::Duration` field written in jiff's
/// friendly form: `"10s"`, `"250ms"`, `"1m 30s"`. Only that grammar is
/// accepted: a negative duration (`"-1s"`, `"1s ago"`) and text without a
/// unit designator (`"10"`) are errors.
pub mod friendly_duration {
    use std::time::Duration;

    use jiff::fmt::friendly::SpanParser;
    use serde::de::Error as _;
    use serde::{Deserialize, Deserializer};

    /// The parser of the friendly grammar, in its default configuration,
    /// which accepts every unit designator and no bare number.
    static PARSER: SpanParser = SpanParser::new();

    /// Parses the friendly form; anything else is a deserialization error.
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
        let text = String::deserialize(deserializer)?;
        PARSER
            .parse_unsigned_duration(&text)
            .map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde::Deserialize;

    /// A field carrying the `with` module, so the tests go through serde
    /// exactly as a configuration field does.
    #[derive(Debug, PartialEq, Eq, Deserialize)]
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
    /// The value `"1m 30s"` denotes, which spans two units.
    const NINETY_SECONDS: Duration = Duration::from_secs(90);
    /// A duration with a fractional second, so the float carries it.
    const ONE_AND_A_HALF_SECONDS: Duration = Duration::from_millis(1500);
    /// The float `ONE_AND_A_HALF_SECONDS` becomes; exactly representable.
    const ONE_AND_A_HALF: f64 = 1.5;

    fn parse(text: &str) -> Result<Holder, serde_json::Error> {
        serde_json::from_str(&format!("{{\"elapsed\": \"{text}\"}}"))
    }

    #[test]
    fn test_friendly_round_trips_through_the_parser() {
        for duration in [
            Duration::ZERO,
            TEN_SECONDS,
            ONE_MINUTE,
            QUARTER_SECOND,
            NINETY_SECONDS,
        ] {
            let text = super::friendly(duration);
            assert_eq!(parse(&text).unwrap().elapsed, duration, "{text}");
        }
        assert_eq!(super::friendly(Duration::ZERO), "0s");
        assert_eq!(super::friendly(NINETY_SECONDS), "1m 30s");
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

    #[test]
    fn test_from_micros_reads_back_what_micros_gives() {
        assert_eq!(
            super::from_micros(ONE_AND_A_HALF_SECONDS_MICROS),
            ONE_AND_A_HALF_SECONDS
        );
        assert_eq!(
            super::from_micros(super::micros(ONE_AND_A_HALF_SECONDS)),
            ONE_AND_A_HALF_SECONDS
        );
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

    /// `EPOCH_MILLIS` in seconds, the fraction kept.
    const EPOCH_SECONDS: f64 = 1_700_000_000.123;

    #[test]
    fn test_unix_seconds_counts_from_the_epoch_with_the_fraction() {
        assert_eq!(
            super::unix_seconds(jiff::Timestamp::UNIX_EPOCH).to_bits(),
            0.0f64.to_bits()
        );
        let at = super::timestamp_from_millis(EPOCH_MILLIS).unwrap();
        assert_eq!(super::unix_seconds(at).to_bits(), EPOCH_SECONDS.to_bits());
    }

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
    fn test_epoch_count_days_counts_calendar_days_from_the_epoch() {
        let days = |text| super::epoch_count(text, super::EpochUnit::Days);
        assert_eq!(days("1970-01-01"), Ok(0));
        assert_eq!(days("1970-01-02"), Ok(1));
        assert_eq!(days("1969-12-31"), Ok(-1));
        assert_eq!(days("1972-03-01"), Ok(790), "1972 is a leap year");
    }

    #[test]
    fn test_epoch_count_days_refuses_anything_but_a_calendar_date_jiff_holds() {
        for text in [
            "2026-02-30",
            "10000-01-01",
            "+010000-01-01",
            "2026-01-01T00:00:00",
            "2026-01-01T00:00:00+00:00",
            "20260101",
            "2026-1-1",
            "",
        ] {
            assert_eq!(
                super::epoch_count(text, super::EpochUnit::Days),
                Err(super::EpochTextFault::NotIso),
                "{text:?}"
            );
        }
    }

    #[test]
    fn test_epoch_count_microseconds_reads_an_instant_with_its_offset() {
        let micros = |text| super::epoch_count(text, super::EpochUnit::Microseconds);
        assert_eq!(micros("1970-01-01T00:00:00Z"), Ok(0));
        assert_eq!(micros("1970-01-01T00:00:00.000001Z"), Ok(1));
        assert_eq!(micros("1970-01-01T00:00:00.000001000Z"), Ok(1));
        assert_eq!(micros("1969-12-31T23:59:59.999999Z"), Ok(-1));
        assert_eq!(micros("1970-01-01T01:00:00+01:00"), Ok(0));
        assert_eq!(
            micros("2023-11-14T22:13:20.123Z"),
            Ok(EPOCH_MILLIS * MICROS_PER_MILLI)
        );
    }

    #[test]
    fn test_epoch_count_microseconds_refuses_digits_below_a_microsecond() {
        for text in [
            "1970-01-01T00:00:00.0000001Z",
            "1970-01-01T00:00:00.000001001Z",
            "1969-12-31T23:59:59.9999999Z",
        ] {
            assert_eq!(
                super::epoch_count(text, super::EpochUnit::Microseconds),
                Err(super::EpochTextFault::BelowTheUnit),
                "{text:?}"
            );
        }
    }

    #[test]
    fn test_epoch_count_instants_refuse_text_with_no_offset() {
        for unit in [
            super::EpochUnit::Microseconds,
            super::EpochUnit::Nanoseconds,
        ] {
            for text in ["2026-01-01T00:00:00", "2026-01-01", "1700000000", "now"] {
                assert_eq!(
                    super::epoch_count(text, unit),
                    Err(super::EpochTextFault::NotIso),
                    "{text:?} as {unit}"
                );
            }
        }
    }

    #[test]
    fn test_epoch_count_nanoseconds_spans_exactly_a_long() {
        let nanos = |text| super::epoch_count(text, super::EpochUnit::Nanoseconds);
        assert_eq!(nanos("1970-01-01T00:00:00.000000001Z"), Ok(1));
        assert_eq!(nanos("2262-04-11T23:47:16.854775807Z"), Ok(i64::MAX));
        assert_eq!(nanos("1677-09-21T00:12:43.145224192Z"), Ok(i64::MIN));
        for text in ["2262-04-11T23:47:16.854775808Z", "1677-09-21T00:00:00Z"] {
            assert_eq!(
                nanos(text),
                Err(super::EpochTextFault::OutOfRange),
                "{text:?}"
            );
        }
    }

    /// Microseconds in a millisecond, so `EPOCH_MILLIS` reads as an
    /// instant's microseconds.
    const MICROS_PER_MILLI: i64 = 1_000;

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
    fn test_deserialize_parses_two_units() {
        assert_eq!(parse("1m 30s").unwrap().elapsed, NINETY_SECONDS);
    }
}
