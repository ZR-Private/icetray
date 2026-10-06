//! Latency samples as the benchmark and check programs report them: the
//! nearest-rank percentiles beside the maximum, written to JSON in
//! microseconds.

use std::time::Duration;

use icetray_core::{PER_MILLE, approx};

/// The percentiles reported, in thousandths.
pub const P50: usize = 500;
/// The 90th percentile, in thousandths.
pub const P90: usize = 900;
/// The 99th percentile, in thousandths.
pub const P99: usize = 990;

/// One microsecond, the unit latencies are written to JSON in.
const MICROSECOND: Duration = Duration::from_micros(1);

/// A run's latencies, held ascending.
#[derive(Debug, Default)]
pub struct Latencies {
    sorted: Vec<Duration>,
}

impl Latencies {
    /// `samples`, sorted.
    pub fn of(mut samples: Vec<Duration>) -> Self {
        samples.sort_unstable();
        Self { sorted: samples }
    }

    /// How many samples there are.
    pub fn len(&self) -> usize {
        self.sorted.len()
    }

    /// Whether there are none.
    pub fn is_empty(&self) -> bool {
        self.sorted.is_empty()
    }

    /// The nearest-rank percentile at `per_mille` thousandths; zero for no
    /// samples.
    pub fn percentile(&self, per_mille: usize) -> Duration {
        let rank = (self.sorted.len() * per_mille).div_ceil(approx::length(PER_MILLE));
        self.sorted
            .get(rank.saturating_sub(1))
            .copied()
            .unwrap_or_default()
    }

    /// The slowest sample; zero for no samples.
    pub fn max(&self) -> Duration {
        self.sorted.last().copied().unwrap_or_default()
    }
}

/// `duration` in microseconds, fractions kept, as the programs' JSON holds
/// every time.
pub fn micros(duration: Duration) -> f64 {
    duration.div_duration_f64(MICROSECOND)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_percentile_is_the_nearest_rank() {
        let latencies = Latencies::of((1..=10).rev().map(Duration::from_millis).collect());
        assert_eq!(latencies.percentile(P50), Duration::from_millis(5));
        assert_eq!(latencies.percentile(P90), Duration::from_millis(9));
        assert_eq!(latencies.percentile(P99), Duration::from_millis(10));
        assert_eq!(latencies.max(), Duration::from_millis(10));
    }

    #[test]
    fn test_no_samples_read_as_zero() {
        let latencies = Latencies::of(Vec::new());
        assert!(latencies.is_empty());
        assert_eq!(latencies.percentile(P50), Duration::ZERO);
        assert_eq!(latencies.max(), Duration::ZERO);
    }

    #[test]
    fn test_micros_keeps_fractions() {
        assert!((micros(Duration::from_nanos(1_500)) - 1.5).abs() < f64::EPSILON);
    }
}
