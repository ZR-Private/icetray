//! The binary's share of the metrics (blueprint section 7): the Prometheus
//! recorder with the latency buckets, the `requests_total` counter with its
//! `call` label, and the sampler for `generation_age_seconds` and
//! `process_rss_bytes`. Core's modules emit every other series.

use std::fmt;
use std::sync::{Mutex, OnceLock, PoisonError};

use icetray_core::{Opened, approx, metric, time};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};

use crate::error::ServeError;

/// Histogram bucket bounds in seconds for `lookup_seconds` and
/// `object_get_seconds`, from 100 microseconds to 2 seconds: sized from
/// measured object-store first-byte latency, whose typical read sits in the
/// tens of milliseconds and whose tail runs to seconds (RFD 013), with a
/// 1-2.5-5 progression so each decade has three bounds.
pub const LATENCY_BUCKETS: [f64; 14] = [
    0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0,
];

/// The handle of the one recorder a process installs.
static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// Serializes `install`, so two callers racing to install do not both build
/// a recorder and have the second refused by the `metrics` crate.
static INSTALLING: Mutex<()> = Mutex::new(());

/// Installs the Prometheus recorder once per process and returns its handle;
/// every later call returns the same handle, since a `metrics` recorder is
/// global and the daemon's tests start several servers in one process.
/// Histograms use `LATENCY_BUCKETS`. No idle timeout: the exporter's only
/// series removal is an idle timeout, which would also drop live gauges that
/// core sets only on change, so a `generation_info` series of a replaced
/// snapshot reads 0 until the process restarts.
pub fn install() -> Result<PrometheusHandle, ServeError> {
    let _installing = INSTALLING.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(handle) = HANDLE.get() {
        return Ok(handle.clone());
    }
    let handle = PrometheusBuilder::new()
        .set_buckets_for_metric(
            Matcher::Full(metric::LOOKUP_SECONDS.to_string()),
            &LATENCY_BUCKETS,
        )?
        .set_buckets_for_metric(
            Matcher::Full(metric::OBJECT_GET_SECONDS.to_string()),
            &LATENCY_BUCKETS,
        )?
        .install_recorder()?;
    Ok(HANDLE.get_or_init(|| handle).clone())
}

/// The RPC a request was: the `call` label of `requests_total`. Display
/// gives the label value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Call {
    /// `Icetray.Get`.
    Get,
    /// `Icetray.MultiGet`.
    MultiGet,
    /// `Icetray.TableInfo`.
    TableInfo,
}

impl fmt::Display for Call {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Get => "get",
            Self::MultiGet => "multi_get",
            Self::TableInfo => "table_info",
        };
        f.write_str(name)
    }
}

/// Counts one request of `call` under `requests_total`.
pub fn count(call: Call) {
    metrics::counter!(metric::REQUESTS_TOTAL, metric::LABEL_CALL => call.to_string()).increment(1);
}

/// Samples the gauges the binary owns: `generation_age_seconds` for every
/// table serving a generation, from the snapshot's own timestamp against
/// now, and `process_rss_bytes` when the platform reports a resident set
/// size; then runs the recorder's upkeep, which drains histogram samples.
pub fn sample(opened: &Opened, handle: &PrometheusHandle) {
    for table in &opened.tables {
        if let Some(generation) = table.current() {
            metrics::gauge!(
                metric::GENERATION_AGE_SECONDS,
                metric::LABEL_TABLE => table.name().to_string()
            )
            .set(time::age_seconds(generation.snapshot_timestamp));
        }
    }
    if let Some(stats) = memory_stats::memory_stats() {
        metrics::gauge!(metric::PROCESS_RSS_BYTES).set(approx::gauge_len(stats.physical_mem));
    }
    handle.run_upkeep();
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The first and last bucket bounds the contract fixes.
    const FIRST_BUCKET: f64 = 1e-4;
    const LAST_BUCKET: f64 = 2.0;

    #[test]
    fn test_fmt_call_label_values() {
        let cases = [
            (Call::Get, "get"),
            (Call::MultiGet, "multi_get"),
            (Call::TableInfo, "table_info"),
        ];
        for (call, name) in cases {
            assert_eq!(call.to_string(), name);
        }
    }

    #[test]
    fn test_latency_buckets_strictly_increase_from_first_to_last() {
        assert!(
            LATENCY_BUCKETS
                .windows(2)
                .all(|pair| pair[0].total_cmp(&pair[1]).is_lt())
        );
        assert_eq!(LATENCY_BUCKETS[0].to_bits(), FIRST_BUCKET.to_bits());
        assert_eq!(
            LATENCY_BUCKETS[LATENCY_BUCKETS.len() - 1].to_bits(),
            LAST_BUCKET.to_bits()
        );
    }

    #[test]
    fn test_install_returns_one_handle_per_process() {
        let first = install().unwrap();
        let second = install().unwrap();
        // Two handles onto one recorder both see a count made through the
        // global recorder; the whole expositions are not compared, since
        // another test in this process may count between two renders.
        count(Call::TableInfo);
        let series = format!(
            "{}{{{}=\"{}\"}}",
            metric::REQUESTS_TOTAL,
            metric::LABEL_CALL,
            Call::TableInfo
        );
        for handle in [&first, &second] {
            let exposition = handle.render();
            let value: u64 = exposition
                .lines()
                .find_map(|line| line.strip_prefix(&series))
                .and_then(|rest| rest.trim().parse().ok())
                .unwrap_or_else(|| panic!("{series} in {exposition}"));
            assert!(value >= 1, "{value}");
        }
    }
}
