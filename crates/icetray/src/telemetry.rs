//! The binary's share of the metrics (blueprint section 7): the Prometheus
//! recorder with the latency buckets, the `requests_total` counter with its
//! `call` and `code` labels, the in-flight gauges a scrape reads from the
//! service's slots, and the sampler for `generation_age_seconds`,
//! `process_rss_bytes`, and the allocator gauges. Core's modules emit every
//! other series.

use std::fmt;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use icetray_core::{Opened, approx, metric, time};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use tikv_jemalloc_ctl::{epoch, stats};
use tonic::{Code, Status};

use crate::error::ServeError;
use crate::service::Slots;

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
/// Histograms use `LATENCY_BUCKETS`. No idle timeout: it would drop live
/// gauges that core sets only on change.
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

/// A gRPC status code as the `code` label of `requests_total`. Display
/// gives the code's name in the gRPC specification, in lower snake case.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CodeLabel(pub Code);

impl fmt::Display for CodeLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self.0 {
            Code::Ok => "ok",
            Code::Cancelled => "cancelled",
            Code::Unknown => "unknown",
            Code::InvalidArgument => "invalid_argument",
            Code::DeadlineExceeded => "deadline_exceeded",
            Code::NotFound => "not_found",
            Code::AlreadyExists => "already_exists",
            Code::PermissionDenied => "permission_denied",
            Code::ResourceExhausted => "resource_exhausted",
            Code::FailedPrecondition => "failed_precondition",
            Code::Aborted => "aborted",
            Code::OutOfRange => "out_of_range",
            Code::Unimplemented => "unimplemented",
            Code::Internal => "internal",
            Code::Unavailable => "unavailable",
            Code::DataLoss => "data_loss",
            Code::Unauthenticated => "unauthenticated",
        };
        f.write_str(name)
    }
}

/// One request of a call being answered, counted under `requests_total`
/// when it drops, with the code of the outcome `answer` was given. A call
/// dropped before it answers counts as `CANCELLED`: tonic drops it at the
/// client's `grpc-timeout` and answers `CANCELLED` itself, and a client
/// that cancels is answered nothing.
#[derive(Debug)]
pub struct Answering {
    call: Call,
    code: Code,
}

impl Answering {
    /// A request of `call`, not yet answered.
    pub fn new(call: Call) -> Self {
        Self {
            call,
            code: Code::Cancelled,
        }
    }

    /// Passes `outcome` through as the call's answer, counted under its
    /// code: `OK` for a response, the status's own code otherwise.
    pub fn answer<T>(mut self, outcome: Result<T, Status>) -> Result<T, Status> {
        self.code = outcome.as_ref().map_or_else(Status::code, |_| Code::Ok);
        outcome
    }
}

impl Drop for Answering {
    fn drop(&mut self) {
        count(self.call, self.code);
    }
}

/// Counts one request of `call` that answered `code` under `requests_total`.
fn count(call: Call, code: Code) {
    metrics::counter!(
        metric::REQUESTS_TOTAL,
        metric::LABEL_CALL => call.to_string(),
        metric::LABEL_CODE => CodeLabel(code).to_string()
    )
    .increment(1);
}

/// What a scrape of `/metrics` reads: the recorder, the tables and page
/// cache whose counts it publishes first, and the service's in-flight
/// slots. Clone: every metrics connection holds one.
#[derive(Clone)]
pub struct Scrape {
    handle: PrometheusHandle,
    opened: Arc<Opened>,
    slots: Slots,
}

impl Scrape {
    /// A scrape of `handle`'s recorder over `opened` and `slots`.
    pub fn new(handle: PrometheusHandle, opened: Arc<Opened>, slots: Slots) -> Self {
        Self {
            handle,
            opened,
            slots,
        }
    }

    /// The exposition, with every table's lookup counts
    /// (`Table::publish_lookup_counts`) and the page cache's hit counts
    /// (`PageCache::publish_hit_counts`) published first, so the scrape
    /// reads every lookup answered and every page hit before it, and the
    /// in-flight gauges read from the slots as they stand.
    pub fn render(&self) -> String {
        for table in self.opened.tables().iter() {
            table.publish_lookup_counts();
        }
        self.opened.cache.publish_hit_counts();
        metrics::gauge!(metric::REQUESTS_IN_FLIGHT).set(approx::gauge_len(self.slots.admitted()));
        metrics::gauge!(metric::MAX_IN_FLIGHT).set(approx::gauge_len(self.slots.limit()));
        self.handle.render()
    }
}

/// Samples the gauges the binary owns: `generation_age_seconds` for every
/// table serving a generation, from the snapshot's own timestamp against
/// now, `process_rss_bytes` when the platform reports a resident set size,
/// and the allocator gauges; then runs the recorder's upkeep, which drains
/// histogram samples.
pub fn sample(opened: &Opened, handle: &PrometheusHandle) {
    for table in opened.tables().iter() {
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
    sample_allocator();
    handle.run_upkeep();
}

/// Sets the allocator gauges from jemalloc's statistics. jemalloc refreshes
/// them only when its epoch advances, so the epoch is advanced first. A
/// statistic jemalloc cannot read leaves its gauge as it was: the gauges
/// explain `process_rss_bytes`, and serving does not depend on them.
fn sample_allocator() {
    if let Err(error) = epoch::advance() {
        tracing::debug!(%error, "jemalloc statistics are unavailable");
        return;
    }
    let gauges = [
        (metric::ALLOCATOR_ALLOCATED_BYTES, stats::allocated::read()),
        (metric::ALLOCATOR_ACTIVE_BYTES, stats::active::read()),
        (metric::ALLOCATOR_METADATA_BYTES, stats::metadata::read()),
        (metric::ALLOCATOR_RESIDENT_BYTES, stats::resident::read()),
        (metric::ALLOCATOR_RETAINED_BYTES, stats::retained::read()),
    ];
    for (name, bytes) in gauges {
        match bytes {
            Ok(bytes) => metrics::gauge!(name).set(approx::gauge_len(bytes)),
            Err(error) => tracing::debug!(%error, gauge = name, "jemalloc statistic unavailable"),
        }
    }
}

#[cfg(test)]
mod tests {
    use icetray_testkit::verify::series_value;

    use super::*;

    /// The lowest bucket bound the contract fixes: 100 microseconds.
    const FIRST_BUCKET: f64 = 1e-4;

    /// The highest bucket bound the contract fixes: 2 seconds.
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
    fn test_fmt_code_label_names_every_grpc_code() {
        let cases = [
            (Code::Ok, "ok"),
            (Code::Cancelled, "cancelled"),
            (Code::Unknown, "unknown"),
            (Code::InvalidArgument, "invalid_argument"),
            (Code::DeadlineExceeded, "deadline_exceeded"),
            (Code::NotFound, "not_found"),
            (Code::AlreadyExists, "already_exists"),
            (Code::PermissionDenied, "permission_denied"),
            (Code::ResourceExhausted, "resource_exhausted"),
            (Code::FailedPrecondition, "failed_precondition"),
            (Code::Aborted, "aborted"),
            (Code::OutOfRange, "out_of_range"),
            (Code::Unimplemented, "unimplemented"),
            (Code::Internal, "internal"),
            (Code::Unavailable, "unavailable"),
            (Code::DataLoss, "data_loss"),
            (Code::Unauthenticated, "unauthenticated"),
        ];
        for (code, name) in cases {
            assert_eq!(CodeLabel(code).to_string(), name, "{code:?}");
        }
    }

    /// The `requests_total` count of `call` answering `code` in the
    /// recorder now, zero before the series exists.
    fn requests(handle: &PrometheusHandle, call: Call, code: Code) -> f64 {
        series_value(
            &handle.render(),
            metric::REQUESTS_TOTAL,
            &[
                (metric::LABEL_CALL, &call.to_string()),
                (metric::LABEL_CODE, &CodeLabel(code).to_string()),
            ],
        )
        .unwrap_or(0.0)
    }

    /// A request counts once, when it drops, under the code it answered:
    /// a response's `OK`, a status's own code, and `CANCELLED` for one
    /// dropped unanswered. `MultiGet` is the call no other test here
    /// counts, so the differences are exact.
    #[test]
    fn test_answering_counts_the_code_answered_once_on_drop() {
        let handle = install().unwrap();
        let cases: [(Option<Result<(), Status>>, Code); 3] = [
            (Some(Ok(())), Code::Ok),
            (Some(Err(Status::unavailable("sample"))), Code::Unavailable),
            (None, Code::Cancelled),
        ];
        for (outcome, code) in cases {
            let before = requests(&handle, Call::MultiGet, code);
            let answering = Answering::new(Call::MultiGet);
            let pending = requests(&handle, Call::MultiGet, code);
            assert_eq!(pending.to_bits(), before.to_bits(), "{code:?} before drop");
            match outcome {
                Some(outcome) => {
                    let passed = answering
                        .answer(outcome)
                        .map_or_else(|status| status.code(), |()| Code::Ok);
                    assert_eq!(passed, code, "the outcome passes through");
                }
                None => drop(answering),
            }
            let after = requests(&handle, Call::MultiGet, code);
            assert_eq!((after - before).to_bits(), 1.0_f64.to_bits(), "{code:?}");
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

    /// Each allocator gauge is set, and they nest as jemalloc defines them:
    /// live bytes within active pages within what it holds resident.
    #[test]
    fn test_sample_allocator_sets_every_gauge_in_order() {
        let handle = install().unwrap();
        sample_allocator();
        let exposition = handle.render();
        let read = |name: &str| {
            series_value(&exposition, name, &[]).unwrap_or_else(|| panic!("{name} in {exposition}"))
        };
        let allocated = read(metric::ALLOCATOR_ALLOCATED_BYTES);
        let active = read(metric::ALLOCATOR_ACTIVE_BYTES);
        let resident = read(metric::ALLOCATOR_RESIDENT_BYTES);
        read(metric::ALLOCATOR_METADATA_BYTES);
        read(metric::ALLOCATOR_RETAINED_BYTES);
        assert!(
            allocated <= active,
            "allocated {allocated} > active {active}"
        );
        assert!(active <= resident, "active {active} > resident {resident}");
    }

    #[test]
    fn test_install_returns_one_handle_per_process() {
        let first = install().unwrap();
        let second = install().unwrap();
        // Two handles onto one recorder both see a count made through the
        // global recorder; the whole expositions are not compared, since
        // another test in this process may count between two renders.
        count(Call::TableInfo, Code::Ok);
        let call = Call::TableInfo.to_string();
        let code = CodeLabel(Code::Ok).to_string();
        for handle in [&first, &second] {
            let exposition = handle.render();
            let value = series_value(
                &exposition,
                metric::REQUESTS_TOTAL,
                &[(metric::LABEL_CALL, &call), (metric::LABEL_CODE, &code)],
            )
            .unwrap_or_else(|| panic!("{call} in {exposition}"));
            assert!(value >= 1.0, "{value}");
        }
    }
}
