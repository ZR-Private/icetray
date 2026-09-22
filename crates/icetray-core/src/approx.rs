//! Integer-to-float conversions for reporting, and the widening of counts
//! to the `u64` the byte and row totals take. Ratios and gauge values are
//! approximate by nature, and this module is the one place that conversion
//! happens, so a call site says what the float is for. Identifiers never
//! become floats: a 63-bit id loses its low digits in an `f64`, so ids go in
//! metric labels and log fields instead.

#![allow(clippy::cast_precision_loss)]

/// `numerator / denominator` as a float, or zero when the denominator is
/// zero. For bytes-per-key and similar reporting ratios.
pub fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

/// A count or byte total as a metric gauge value.
pub fn gauge(value: u64) -> f64 {
    value as f64
}

/// A length or byte total held in memory as a gauge value.
pub fn gauge_len(value: usize) -> f64 {
    gauge(count(value))
}

/// A count or length held in memory as the `u64` the byte and row totals
/// take. The one place a `usize` widens: it fits on every supported target,
/// whose `usize` is at most 64 bits, and `expect` marks that.
pub fn count(value: usize) -> u64 {
    u64::try_from(value).expect("a usize fits u64 on the supported targets")
}

/// A footer number as a measurement: a chunk size, a value count, a filter
/// length, a row count, or a column width the doctor and the resident-bytes
/// prediction sum, in the `i32` or `i64` the footer holds it in. A negative
/// one, which no writer produces and which `FileIndex::build` refuses
/// wherever a decision rests on it, measures as zero, so an estimate over
/// any footer is a number and never a fault.
pub(crate) fn measured(value: impl Into<i64>) -> u64 {
    u64::try_from(value.into()).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ratio_zero_denominator_is_zero() {
        assert_eq!(ratio(5, 0).to_bits(), 0.0f64.to_bits());
    }

    #[test]
    fn test_ratio_divides() {
        assert_eq!(ratio(3, 4).to_bits(), 0.75f64.to_bits());
    }

    #[test]
    fn test_gauge_is_the_value() {
        assert_eq!(gauge(7).to_bits(), 7.0f64.to_bits());
    }

    #[test]
    fn test_gauge_len_is_the_value() {
        assert_eq!(gauge_len(7).to_bits(), 7.0f64.to_bits());
    }

    #[test]
    fn test_count_is_the_value() {
        assert_eq!(count(7), 7);
        assert_eq!(count(usize::MAX), u64::try_from(usize::MAX).unwrap());
    }

    #[test]
    fn test_measured_negative_is_zero() {
        assert_eq!(measured(7), 7);
        assert_eq!(measured(0), 0);
        assert_eq!(measured(-1), 0);
        assert_eq!(measured(i64::MIN), 0);
    }
}
