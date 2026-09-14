//! Integer-to-float conversions for reporting. Ratios and gauge values are
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
}
