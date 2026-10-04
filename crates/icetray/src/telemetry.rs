//! The binary's share of the metrics (blueprint section 7): the Prometheus
//! recorder with the latency buckets, the `requests_total` counter with its
//! `call` and `code` labels, and the scrape, which writes every gauge a
//! live table, the page cache, the service, the reload, and the process
//! hold before it renders, so the recorder drops the gauges of a table no
//! longer served. Core's modules emit every other series.

use std::fmt;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use icetray_core::{Opened, Tables, approx, metric, time};
use metrics::Counter;
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use metrics_util::MetricKindMask;
use tikv_jemalloc_ctl::{epoch, stats};
use tonic::{Code, Status};

use crate::error::ServeError;
use crate::reload::Pending;
use crate::service::Slots;

/// Histogram bucket bounds in seconds for `lookup_seconds` and
/// `object_get_seconds`, from 100 microseconds to 2 seconds: sized from
/// measured object-store first-byte latency, whose typical read sits in the
/// tens of milliseconds and whose tail runs to seconds (RFD 013), with a
/// 1-2.5-5 progression so each decade has three bounds.
pub const LATENCY_BUCKETS: [f64; 14] = [
    0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0,
];

/// How long a gauge no one writes stays in the exposition: none, so a
/// render drops every gauge left unwritten since the render before it.
/// Every render writes each live gauge first (`render_after`), so what it
/// drops is a gauge nothing holds any more, such as a removed table's,
/// which would otherwise read its last value until the process ends.
/// Counters and histograms never expire.
const GAUGE_IDLE_TIMEOUT: Duration = Duration::ZERO;

/// The handle of the one recorder a process installs.
static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// Serializes every render with the gauge writes before it, so one render
/// cannot find another's writes already seen and drop them as idle.
static RENDERING: Mutex<()> = Mutex::new(());

/// Serializes `install`, so two callers racing to install do not both build
/// a recorder and have the second refused by the `metrics` crate.
static INSTALLING: Mutex<()> = Mutex::new(());

/// Installs the Prometheus recorder once per process and returns its handle;
/// every later call returns the same handle, since a `metrics` recorder is
/// global and the daemon's tests start several servers in one process.
/// Histograms use `LATENCY_BUCKETS`; gauges expire after
/// `GAUGE_IDLE_TIMEOUT`; every metric is described (`metric::describe`).
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
        .idle_timeout(MetricKindMask::GAUGE, Some(GAUGE_IDLE_TIMEOUT))
        .install_recorder()?;
    metric::describe();
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

impl Call {
    /// Every call, for the series registered at start.
    pub const ALL: [Self; 3] = [Self::Get, Self::MultiGet, Self::TableInfo];
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
    requests_total(call, code).increment(1);
}

/// The `requests_total` series of `call` answering `code`.
fn requests_total(call: Call, code: Code) -> Counter {
    metrics::counter!(
        metric::REQUESTS_TOTAL,
        metric::LABEL_CALL => call.to_string(),
        metric::LABEL_CODE => CodeLabel(code).to_string()
    )
}

/// Registers `requests_total` of every call answering `OK`, so each reads
/// 0 from the start rather than appearing at its first call.
pub fn register_requests() {
    for call in Call::ALL {
        requests_total(call, Code::Ok).increment(0);
    }
}

/// What a scrape of `/metrics` reads: the recorder, the tables and page
/// cache whose counts and gauges it publishes first, the service's
/// in-flight slots, and the reload's pending flag when the run follows its
/// configuration. Clone: every metrics connection holds one.
#[derive(Clone)]
pub struct Scrape {
    handle: PrometheusHandle,
    opened: Arc<Opened>,
    slots: Slots,
    pending: Option<Pending>,
}

impl Scrape {
    /// A scrape of `handle`'s recorder over `opened`, `slots`, and
    /// `pending`.
    pub fn new(
        handle: PrometheusHandle,
        opened: Arc<Opened>,
        slots: Slots,
        pending: Option<Pending>,
    ) -> Self {
        Self {
            handle,
            opened,
            slots,
            pending,
        }
    }

    /// The exposition, after `publish`.
    pub fn render(&self) -> String {
        render_after(&self.handle, || self.publish())
    }

    /// Writes every series a scrape reads as it stands: each table's lookup
    /// counts (`Table::publish_lookup_counts`) and gauges
    /// (`Table::publish_gauges`), the page cache's hit counts and bytes, the
    /// in-flight gauges from the slots, `config_pending`, and the sampled
    /// gauges (`sample`). The counts are published so a scrape reads every
    /// lookup answered and every page hit before it; the gauges are written
    /// so the recorder keeps them (`GAUGE_IDLE_TIMEOUT`).
    fn publish(&self) {
        let tables = self.opened.tables();
        for table in tables.iter() {
            table.publish_lookup_counts();
            table.publish_gauges();
        }
        self.opened.cache.publish_hit_counts();
        self.opened.cache.publish_usage();
        metrics::gauge!(metric::REQUESTS_IN_FLIGHT).set(approx::gauge_len(self.slots.admitted()));
        metrics::gauge!(metric::MAX_IN_FLIGHT).set(approx::gauge_len(self.slots.limit()));
        if let Some(pending) = &self.pending {
            pending.publish();
        }
        sample(&tables);
    }
}

/// `handle`'s exposition, rendered right after `publish` writes the gauges
/// it keeps, with no other render between them. Every render in the
/// process goes through here.
fn render_after(handle: &PrometheusHandle, publish: impl FnOnce()) -> String {
    let _rendering = RENDERING.lock().unwrap_or_else(PoisonError::into_inner);
    publish();
    handle.render()
}

/// Samples the gauges the binary owns: `generation_age_seconds` for every
/// one of `tables` serving a generation, from the snapshot's own timestamp
/// against now, `process_rss_bytes` when the platform reports a resident
/// set size, and the allocator gauges. A gauge left unsampled drops out of
/// the exposition.
fn sample(tables: &Tables) {
    for table in tables.iter() {
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
}

/// Sets the allocator gauges from jemalloc's statistics. jemalloc refreshes
/// them only when its epoch advances, so the epoch is advanced first. A
/// statistic jemalloc cannot read is logged at debug and left unset: the
/// gauges explain `process_rss_bytes`, and serving does not depend on them.
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
            &render_after(handle, || {}),
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

    /// After `register_requests`, every call's `OK` series is exposed,
    /// before any request of it.
    #[test]
    fn test_register_requests_exposes_every_call_answering_ok() {
        let handle = install().unwrap();
        register_requests();
        let exposition = render_after(&handle, || {});
        let ok = CodeLabel(Code::Ok).to_string();
        for call in Call::ALL {
            let call = call.to_string();
            assert!(
                series_value(
                    &exposition,
                    metric::REQUESTS_TOTAL,
                    &[(metric::LABEL_CALL, &call), (metric::LABEL_CODE, &ok)],
                )
                .is_some(),
                "{call} in {exposition}"
            );
        }
    }

    /// A gauge written before one render and not since is gone from the
    /// next, while one written before each stays: what drops a removed
    /// table's gauges. The gauges are named for this test alone.
    #[test]
    fn test_render_after_drops_a_gauge_no_render_wrote() {
        const KEPT: &str = "icetray_test_kept";
        const DROPPED: &str = "icetray_test_dropped";
        let handle = install().unwrap();
        let first = render_after(&handle, || {
            metrics::gauge!(KEPT).set(1.0);
            metrics::gauge!(DROPPED).set(1.0);
        });
        for name in [KEPT, DROPPED] {
            assert!(
                series_value(&first, name, &[]).is_some(),
                "{name} in {first}"
            );
        }
        let second = render_after(&handle, || metrics::gauge!(KEPT).set(1.0));
        assert!(series_value(&second, KEPT, &[]).is_some(), "{second}");
        assert_eq!(series_value(&second, DROPPED, &[]), None, "{second}");
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
        let exposition = render_after(&handle, sample_allocator);
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
            let exposition = render_after(handle, || {});
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
