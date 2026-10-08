//! The table lifecycle (RFD 005, RFD 007, RFD 020): a `Table` follows one
//! ref, builds each new snapshot as a `Generation` beside the one it
//! serves, runs the doctor on the new generation and refuses one below the
//! table's level under `strict`, then swaps it in atomically. Loads and
//! refreshes of one table are serialized; a failed one leaves the last
//! generation serving and the table `Degraded`, or `Held` when the snapshot
//! was refused for its level. A lookup takes one generation and answers
//! from it alone, so a swap during a batch cannot mix snapshots. A `disk`
//! table's level also follows how much of its chunk files the kernel's
//! page cache holds, a condition of the server that `strict` never acts
//! on.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use arc_swap::{ArcSwap, ArcSwapOption};
use futures::{StreamExt as _, TryStreamExt as _, stream};
use iceberg::spec::Snapshot;
use metrics::{Counter, Histogram};
use tokio::sync::{MutexGuard, Semaphore};
use tokio::task::JoinSet;

use crate::approx;
use crate::blocking::on_blocking_pool;
use crate::cache::PageCache;
use crate::catalog::{Resolved, TableSource};
use crate::check::{self, Finding, FindingCode, LookupCost};
use crate::config::{Residency, TableConfig};
use crate::deletes::DeleteSummary;
use crate::error::{Error, ErrorKind, Recurrence, Recurs as _, RefusalReason, Result};
use crate::generation::{Generation, Lookup, MultiLookup, Value};
use crate::key::{Key, KeyType};
use crate::level::{Level, PageCacheCondition};
use crate::local::{LocalDir, ResidentPages, Unobserved};
use crate::manifest::TableLayout;
use crate::metric::{self, LookupResult};
use crate::shard::{self, Sharded};
use crate::snapshot::{SequenceNumber, SnapshotId};
use crate::storage::{HedgeWindow, Storage};
use crate::time;

/// The lookups of one `multi_get` in flight at once: 32, across every task
/// the batch runs on, each lookup holding one of the batch's permits while
/// it runs. A lookup waits on one GET at a time, and at most one hedge
/// beside it, so a batch holds at most twice this many GETs open on the
/// store however many keys it carries, rather than one per key. A full
/// batch of cold hits takes about thirty GET round trips, inside the
/// `MultiGet` deadline at the latency a live lookup expects, well under
/// 200 ms each (RFD 013).
const MULTI_GET_FAN_OUT: usize = 32;

/// The fewest keys one task of a `multi_get` is given: 8. A warm hit's
/// decode is on the order of ten microseconds, so a task of eight keys
/// holds about a hundred microseconds of work, far more than spawning it
/// costs, and a batch of a hundred keys still spreads over a dozen
/// workers.
const MULTI_GET_TASK_KEYS_MIN: usize = 8;

/// Where a table is in its lifecycle. Display gives the `state` metric
/// label: the variant's name alone, `loading`, `serving`, `degraded`, or
/// `held`.
#[derive(Clone, Debug)]
pub enum TableState {
    /// Constructed and not yet loaded; nothing has been read.
    Loading,
    /// A generation answers lookups and the last load or refresh succeeded.
    Serving {
        /// The snapshot it serves.
        snapshot_id: SnapshotId,
        /// When the state began: the swap, or the poll that ended a
        /// `Degraded` or `Held` state by finding the table in step with its
        /// ref.
        since: Instant,
    },
    /// The last load or refresh failed. The generation loaded before it, if
    /// any, keeps answering (RFD 005). The state ends at the next swap, or
    /// at the next poll that finds the table in step with its ref.
    Degraded {
        /// The snapshot still serving, or `None` after a failed first load.
        serving: Option<SnapshotId>,
        /// Why the load or refresh failed, as the failure's kind: a refusal
        /// with its reason, a table or file fault, or the
        /// library or local-directory failure's variant. The failure itself
        /// went to the caller of `load` or `refresh`, which logs it; `Error`
        /// is not `Clone`, and the kind is what a reader of the state acts on.
        reason: ErrorKind,
        /// When the failure happened.
        since: Instant,
    },
    /// The newest snapshot was refused under `strict` for its level
    /// (`RefusalReason::BelowLevel`, RFD 020), and the last snapshot that
    /// met the level keeps serving. Not a fault: a writer's layout put the
    /// table there, and a poll that finds the source moved builds again. A
    /// transient failure of a later poll makes the table `Degraded` until a
    /// poll finds the source unchanged, which makes it `Held` again; the
    /// state ends at the next swap, or at a poll that finds the ref back on
    /// the serving snapshot.
    Held {
        /// The snapshot still serving.
        serving: SnapshotId,
        /// The snapshot refused.
        refused: SnapshotId,
        /// The level the refused snapshot's layout achieves.
        level: Level,
        /// When the refusal was made.
        since: Instant,
    },
}

impl TableState {
    /// The state's name, for the `state` label.
    fn name(&self) -> StateName {
        match self {
            Self::Loading => StateName::Loading,
            Self::Serving { .. } => StateName::Serving,
            Self::Degraded { .. } => StateName::Degraded,
            Self::Held { .. } => StateName::Held,
        }
    }
}

impl fmt::Display for TableState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.name().fmt(f)
    }
}

/// A `TableState`'s name without its fields: the `state` label's values,
/// so the `table_state` gauge is set to zero for the states a table is not
/// in. Display gives the label value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StateName {
    /// `TableState::Loading`.
    Loading,
    /// `TableState::Serving`.
    Serving,
    /// `TableState::Degraded`.
    Degraded,
    /// `TableState::Held`.
    Held,
}

impl StateName {
    /// Every state, for the gauge's series.
    const ALL: [Self; 4] = [Self::Loading, Self::Serving, Self::Degraded, Self::Held];
}

impl fmt::Display for StateName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Loading => "loading",
            Self::Serving => "serving",
            Self::Degraded => "degraded",
            Self::Held => "held",
        };
        f.write_str(name)
    }
}

/// What a table is built over beside its configuration and source: the
/// storage its reads go through, the page cache, the local directory (RFD
/// 017) when one is configured, and the count of `get` and `multi_get`
/// calls in flight. `open_with` builds one and hands every table it opens
/// a clone, so a process's tables share one of each. Clones share what
/// they hold.
#[derive(Clone)]
pub struct TableHandles {
    /// The storage the table's reads go through.
    pub storage: Arc<Storage>,
    /// The page cache the table's lookups read pages through.
    pub cache: PageCache,
    /// The local directory, when `storage.local` is set.
    pub local: Option<Arc<LocalDir>>,
    /// The calls in flight on every table holding this count, which a
    /// `multi_get` reads to decide whether to split.
    pub calls: CallsInFlight,
    /// What the tables do with the doctor's findings on each generation
    /// they admit.
    pub findings: FindingLog,
}

/// What a table does with the doctor's findings on each generation it
/// admits (RFD 016).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FindingLog {
    /// Each finding is a log event, at warn when it caps the table below
    /// its declared level and at info otherwise: the daemon's choice, whose
    /// findings reach no other output.
    Events,
    /// Each finding is a debug event: for a caller that prints the
    /// findings as data, which a log line would only repeat.
    Debug,
}

/// The `get` and `multi_get` calls in flight on every table built over a
/// clone of it, sharded as the lookup counts are, so a call does not write
/// a line every other worker writes too: a call adds one to its thread's
/// shard as it starts and takes it back from that same shard as it ends,
/// so the shards sum to the calls in flight. Clones share the count.
#[derive(Clone)]
pub struct CallsInFlight(Arc<Sharded<AtomicUsize>>);

impl CallsInFlight {
    /// No calls, over one shard per worker thread of the calling runtime
    /// (`shard::count`).
    pub fn new() -> Self {
        Self(Arc::new(Sharded::new()))
    }

    /// Counts one call in flight until the returned guard drops.
    pub(crate) fn enter(&self) -> InFlight<'_> {
        let shard = self.0.mine();
        shard.fetch_add(1, Ordering::Relaxed);
        InFlight(shard)
    }

    /// The calls in flight, summed over the shards without synchronization:
    /// a load signal, which may lag a call starting or ending as it is read.
    pub(crate) fn sum(&self) -> usize {
        self.0
            .iter()
            .map(|shard| shard.load(Ordering::Relaxed))
            .sum()
    }
}

impl Default for CallsInFlight {
    fn default() -> Self {
        Self::new()
    }
}

/// One served table: its configuration and source, the generation it
/// serves, and its lifecycle state. Owns the table's `HedgeWindow`, built
/// from the storage's hedge settings and handed to every
/// `Generation::plan_load`, so one table's delay is set from its own reads
/// and never another's (RFD 013). Shared across tasks behind an `Arc`.
pub struct Table {
    /// The table's entry. A reload that changes only its `lookups` or
    /// `strict` stores another in place (`retune`) while `serial` is held,
    /// so a load or refresh reads one entry throughout.
    config: ArcSwap<TableConfig>,
    /// `config`'s name, which no retune changes.
    name: String,
    source: TableSource,
    storage: Arc<Storage>,
    local: Option<Arc<LocalDir>>,
    hedge: Arc<HedgeWindow>,
    /// The serving generation. A `get` borrows it through one `load` guard
    /// for the lookup's whole duration, so the lookup does not see a swap,
    /// and a swap meanwhile keeps the borrowed generation alive until the
    /// guard drops. The guard takes one of its thread's eight fast slots
    /// (arc-swap's debts) and writes no shared reference count; a lookup
    /// that reads nothing completes without suspending and frees its slot
    /// at once, while one waiting on a read holds it through the wait. A
    /// load on a thread whose slots are all held falls back to a counted
    /// reference, what `load_full` costs.
    current: ArcSwapOption<Generation>,
    /// Never held across an `await`.
    state: Mutex<TableState>,
    /// The metadata location a poll compares the source against: the one
    /// the table last admitted or found its serving snapshot at, so a
    /// metadata change that did not move the followed ref does not make
    /// every later poll read the metadata again, or the one where a poll
    /// over a serving generation met a failure fixed for the snapshot
    /// (`Recurrence::Deterministic`): a refusal, or a table or file fault of
    /// the metadata or a data file. That snapshot is not built again until
    /// the source names another location. A transient failure does not
    /// advance it: the next poll reads the metadata again and retries.
    /// `None` until the first successful build. A reload that changes the
    /// table's entry forgets every refusal: one that changes only `lookups`
    /// or `strict` resets this to the serving generation's location
    /// (`retune`), and one that changes another field builds a new `Table`,
    /// which remembers nothing, so a refusal that depended on the old entry
    /// is tried again under the new one (RFD 005, RFD 018).
    known: Mutex<Option<Known>>,
    /// Serializes `load`, `refresh`, and `retune`; `Opened::retain` takes
    /// every table's in turn and `Opened::apply` every retuned table's, each
    /// while holding `Opened`'s membership lock, and nothing else ever
    /// takes two.
    serial: tokio::sync::Mutex<()>,
    /// How long a `load` or `refresh` with no generation serving, which
    /// builds from nothing, may hold `serial` doing its work, from
    /// `server.load_deadline`.
    load_deadline: Duration,
    /// How long a `load` or `refresh` over a serving generation may hold
    /// `serial` doing its work, from `server.refresh_deadline`.
    refresh_deadline: Duration,
    lookups: Arc<Lookups>,
    /// `generation_swaps_total` under the table's name, registered when the
    /// table is built, so the series reads 0 from then on rather than
    /// appearing at the first swap.
    swaps: Counter,
    /// Where the doctor's findings on an admitted generation go.
    findings: FindingLog,
    /// What the table's state and generation gauges read, and whether it
    /// shows them. A table a reload loads beside the one serving under its
    /// name shows none until the reload swaps it in, so the serving table's
    /// series describe the serving table (RFD 018). Held while a gauge is
    /// written, so a withdrawal and a publish never interleave; never held
    /// across an `await`.
    gauges: Mutex<Gauges>,
}

/// The metadata location a table's polls compare their source against,
/// and whether the ref there was refused.
#[derive(Clone, Debug)]
struct Known {
    /// The metadata file's location.
    location: String,
    /// The failure fixed for the snapshot the ref names at `location`, or
    /// for the ref itself when it names none there; `None` when the table
    /// serves that snapshot.
    refused: Option<Refusal>,
}

/// A failure fixed for the snapshot that a table remembers until its source
/// names another metadata location.
#[derive(Clone, Debug)]
struct Refusal {
    /// The snapshot refused; `None` when the metadata lacks the ref
    /// (`TableFault::RefNotFound`), so no snapshot was named.
    snapshot_id: Option<SnapshotId>,
    /// Why, as the failure's kind.
    reason: ErrorKind,
    /// When the refusal was made.
    since: Instant,
}

/// A failed load or poll as `fail` records it: the error, boxed so a step's
/// result stays small, and the snapshot the failure was met building,
/// which a `below_level` refusal holds.
struct Failed {
    error: Box<Error>,
    /// The snapshot `build_and_admit` was building; `None` for a failure
    /// before the ref named one.
    snapshot_id: Option<SnapshotId>,
}

impl From<Error> for Failed {
    fn from(error: Error) -> Self {
        Self {
            error: Box::new(error),
            snapshot_id: None,
        }
    }
}

/// What a step of a load or poll returns: its value, or the failure.
type Attempt<T> = std::result::Result<T, Failed>;

/// A table's gauges: whether they are shown, what its serving generation
/// set them to, and the level series, `None` until the first swap.
struct Gauges {
    shown: bool,
    generation: Option<GenerationGauges>,
    level: Option<LevelGauges>,
    /// Whether the last page-cache sample failed to observe a chunk file,
    /// so the failure, and the recovery from it, are each logged once.
    page_cache_unobserved: bool,
}

/// What the level gauges show (RFD 020): the level the newest snapshot the
/// table measured achieves, the one serving or the one held, with the
/// page cache's condition of the serving generation, against the level the
/// table declares. The layout's level is set at a swap and at a
/// `below_level` refusal; a refusal for any other reason measured nothing
/// new and leaves it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct LevelGauges {
    /// The newest measured snapshot's level for its layout.
    measured: Level,
    /// The table's `lookups`.
    declared: Level,
    /// The page cache's condition of the serving generation's chunk files,
    /// `None` until a `disk` table's first sample. A swap carries it to
    /// the new generation, which keeps most of the old one's chunk files,
    /// until a sample of the new one replaces it.
    page_cache: Option<PageCacheCondition>,
}

impl LevelGauges {
    /// The level the table achieves: its layout's, capped by the page
    /// cache's condition. The one place a server condition joins the
    /// layout's level; `strict` reads the layout's alone.
    fn achieved(self) -> Level {
        self.measured.max(
            self.page_cache
                .map_or(Level::Optimal, PageCacheCondition::cap),
        )
    }

    /// Whether the table is below its declared level.
    fn below_level(self) -> bool {
        !self.achieved().meets(self.declared)
    }
}

/// What a swap sets the generation gauges to, kept so a table shown after
/// its swap publishes them then.
struct GenerationGauges {
    index_bytes: usize,
    findings: BTreeMap<FindingCode, u64>,
    sequence_number: SequenceNumber,
    swapped_at: f64,
    lookup_cost: LookupCost,
    deletes: DeleteSummary,
}

/// What a lookup needs beside its generation: the table's page cache
/// handle, its lookup metrics, and the calls in flight it counts among.
/// Behind an `Arc`, so the tasks a `multi_get` spawns hold it for as long
/// as they run.
struct Lookups {
    /// The process's page cache, through a handle whose fetches end at the
    /// table's `get_deadline`.
    cache: PageCache,
    metrics: LookupMetrics,
    /// The calls in flight on every table sharing the table's handles, its
    /// own among them.
    in_flight: CallsInFlight,
}

/// One call counted in a `CallsInFlight` while held, on the shard it was
/// counted on; dropped as the call returns or is dropped.
pub(crate) struct InFlight<'a>(&'a AtomicUsize);

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Lookups {
    /// Counts one call in flight until the returned guard drops.
    fn enter(&self) -> InFlight<'_> {
        self.in_flight.enter()
    }

    /// The runs a batch of `keys` keys is split into: up to one per worker
    /// thread (`shard::workers`), each of at least `MULTI_GET_TASK_KEYS_MIN`
    /// keys, while fewer calls than worker threads are in flight on the
    /// tables sharing this one's count, the batch's own included; one run
    /// otherwise. A split batch waits on its slowest run, and on busy
    /// workers its runs queue behind other calls' work, so splitting pays
    /// while some workers would sit idle and costs once they are all busy.
    /// Calls to every table of the process run on the same workers, so
    /// their count is the signal, not this table's alone.
    fn runs(&self, keys: usize) -> usize {
        let workers = shard::workers();
        if self.in_flight.sum() < workers {
            workers.min(keys.div_ceil(MULTI_GET_TASK_KEYS_MIN))
        } else {
            1
        }
    }

    /// One lookup through `generation`, timed and counted under its result;
    /// the one path `get` and `multi_get` share.
    async fn one(&self, generation: &Generation, key: &Key) -> Result<Value> {
        let started = Instant::now();
        let (value, result) = generation.lookup(key, &self.cache).await?;
        self.metrics.record(result, started.elapsed());
        Ok(value)
    }

    /// `keys` looked up through `generation` and answered in their order.
    /// Each lookup runs holding a permit of `fan_out`, the batch's bound,
    /// so the batch's tasks together run at most `MULTI_GET_FAN_OUT` at
    /// once; this task waits on at most that many permits itself. The
    /// first failure ends the run.
    async fn run(
        &self,
        generation: &Generation,
        keys: &[Key],
        fan_out: &Semaphore,
    ) -> Result<Vec<Value>> {
        // The lookups are built before the stream, unpolled, so no closure
        // taking a key by reference lives in the returned future; one would
        // make it unprovably `Send`, and the gRPC service needs it `Send`.
        let lookups: Vec<_> = keys
            .iter()
            .map(|key| async move {
                let _permit = fan_out
                    .acquire()
                    .await
                    .expect("a batch's semaphore is never closed");
                self.one(generation, key).await
            })
            .collect();
        stream::iter(lookups)
            .buffered(MULTI_GET_FAN_OUT)
            .try_collect()
            .await
    }
}

/// A table's lookup metric handles, one pair per `LookupResult` under the
/// table's name, registered once when the table is built, with the
/// recorder current then, so a lookup clones no name, formats no label, and looks up no
/// series. A lookup's count goes to its thread's shard of `counts` and
/// reaches `lookups_total` at `publish`, which a scrape runs first and a
/// dropped table runs last, so a scrape reads every lookup counted before
/// it. The latency histogram is recorded per lookup, as it happens.
struct LookupMetrics {
    cold_hit: ResultMetrics,
    warm_hit: ResultMetrics,
    miss: ResultMetrics,
    /// One shard per worker thread when the workers are the threads that
    /// count.
    counts: Sharded<Counts>,
}

/// `lookups_total` and `lookup_seconds` of one table and result.
struct ResultMetrics {
    total: Counter,
    seconds: Histogram,
}

/// One shard of a table's lookup counts, not yet published, per result.
#[derive(Default)]
struct Counts {
    cold_hit: AtomicU64,
    warm_hit: AtomicU64,
    miss: AtomicU64,
}

impl Counts {
    fn of(&self, result: LookupResult) -> &AtomicU64 {
        match result {
            LookupResult::ColdHit => &self.cold_hit,
            LookupResult::WarmHit => &self.warm_hit,
            LookupResult::Miss => &self.miss,
        }
    }
}

impl LookupMetrics {
    fn new(table: &str) -> Self {
        let of = |result: LookupResult| ResultMetrics {
            total: metrics::counter!(
                metric::LOOKUPS_TOTAL,
                metric::LABEL_TABLE => table.to_string(),
                metric::LABEL_RESULT => result.to_string()
            ),
            seconds: metrics::histogram!(
                metric::LOOKUP_SECONDS,
                metric::LABEL_TABLE => table.to_string(),
                metric::LABEL_RESULT => result.to_string()
            ),
        };
        Self {
            cold_hit: of(LookupResult::ColdHit),
            warm_hit: of(LookupResult::WarmHit),
            miss: of(LookupResult::Miss),
            counts: Sharded::new(),
        }
    }

    fn of(&self, result: LookupResult) -> &ResultMetrics {
        match result {
            LookupResult::ColdHit => &self.cold_hit,
            LookupResult::WarmHit => &self.warm_hit,
            LookupResult::Miss => &self.miss,
        }
    }

    /// One lookup of `result` that took `elapsed`.
    fn record(&self, result: LookupResult, elapsed: Duration) {
        self.counts
            .mine()
            .of(result)
            .fetch_add(1, Ordering::Relaxed);
        self.of(result).seconds.record(time::seconds(elapsed));
    }

    /// Moves the counts every shard holds into `lookups_total`. Each shard's
    /// count is taken with a swap to zero, so a lookup counted meanwhile is
    /// published by this call or the next, and never twice.
    fn publish(&self) {
        for result in LookupResult::ALL {
            let counted: u64 = self
                .counts
                .iter()
                .map(|shard| shard.of(result).swap(0, Ordering::Relaxed))
                .sum();
            if counted > 0 {
                self.of(result).total.increment(counted);
            }
        }
    }
}

impl Drop for LookupMetrics {
    /// Publishes the lookups counted since the last `publish`, so a table
    /// dropped between scrapes loses none of its counts.
    fn drop(&mut self) {
        self.publish();
    }
}

impl Table {
    /// Constructs in `Loading`; nothing is read until `load`. Every `load`
    /// and `refresh` with no generation serving runs within
    /// `load_deadline`, since it reads every file, and every one over a
    /// serving generation within `refresh_deadline`. Every page fetch a
    /// lookup runs through `cache` ends at `get_deadline` from its start,
    /// failing as `CacheError::FetchDeadline` for every lookup waiting on
    /// it (RFD 013); load reads do not go through the cache and are not
    /// bounded by it. Every `get` and `multi_get` counts among `handles`'
    /// calls in flight while it runs. The lookup metrics and
    /// `generation_swaps_total` are registered here, at zero, with the
    /// recorder current then: a process installs its recorder first. The
    /// table shows its gauges from here on.
    pub fn new(
        config: TableConfig,
        source: TableSource,
        handles: TableHandles,
        load_deadline: Duration,
        refresh_deadline: Duration,
        get_deadline: Duration,
    ) -> Self {
        let table = Self::hidden(
            config,
            source,
            handles,
            load_deadline,
            refresh_deadline,
            get_deadline,
        );
        table.show();
        table
    }

    /// `new`, with the table's state and generation gauges not shown until
    /// `show`: what a reload builds beside a table serving under the same
    /// name, whose series it must leave alone until the swap (RFD 018).
    pub(crate) fn hidden(
        config: TableConfig,
        source: TableSource,
        handles: TableHandles,
        load_deadline: Duration,
        refresh_deadline: Duration,
        get_deadline: Duration,
    ) -> Self {
        let TableHandles {
            storage,
            cache,
            local,
            calls,
            findings,
        } = handles;
        let hedge = Arc::new(HedgeWindow::new(storage.hedge_config()));
        let lookups = Arc::new(Lookups {
            cache: cache.with_fetch_deadline(get_deadline),
            metrics: LookupMetrics::new(&config.name),
            in_flight: calls,
        });
        let swaps = metrics::counter!(
            metric::GENERATION_SWAPS_TOTAL,
            metric::LABEL_TABLE => config.name.clone()
        );
        Self {
            name: config.name.clone(),
            config: ArcSwap::from_pointee(config),
            source,
            storage,
            local,
            hedge,
            current: ArcSwapOption::empty(),
            state: Mutex::new(TableState::Loading),
            known: Mutex::new(None),
            serial: tokio::sync::Mutex::new(()),
            load_deadline,
            refresh_deadline,
            lookups,
            swaps,
            findings,
            gauges: Mutex::new(Gauges {
                shown: false,
                generation: None,
                level: None,
                page_cache_unobserved: false,
            }),
        }
    }

    /// Shows the table's gauges from now on and publishes them: its state,
    /// and its serving generation's when it has one.
    pub(crate) fn show(&self) {
        let mut gauges = self.gauges.lock().unwrap_or_else(PoisonError::into_inner);
        gauges.shown = true;
        self.write_gauges(&gauges);
    }

    /// Stops showing the table's gauges, for a table a reload removed or
    /// replaced (RFD 018): from here on neither a state change, a swap,
    /// nor `publish_gauges` writes one. The binary's recorder drops a gauge
    /// that no scrape has written since the last, so a removed table's
    /// gauges leave the exposition at the next scrape instead of reading
    /// stale values; a replacement shown after this writes its own over
    /// them.
    pub(crate) fn withdraw(&self) {
        let mut gauges = self.gauges.lock().unwrap_or_else(PoisonError::into_inner);
        gauges.shown = false;
    }

    /// Writes the table's state gauges, its serving generation's gauges,
    /// and the level gauges (`icetray_table_below_level`,
    /// `icetray_table_level`, `icetray_table_declared_level`, and
    /// `icetray_table_chunk_resident_fraction` while judged) again as they
    /// stand, while the table shows them, as `show` does; a scrape calls
    /// this for every table served, since the binary's recorder drops a
    /// gauge no scrape wrote.
    pub fn publish_gauges(&self) {
        let gauges = self.gauges.lock().unwrap_or_else(PoisonError::into_inner);
        if gauges.shown {
            self.write_gauges(&gauges);
        }
    }

    /// Writes the state gauges and, when there is a serving generation,
    /// its gauges and the level gauges; `gauges` is the held lock.
    fn write_gauges(&self, gauges: &Gauges) {
        self.write_state(gauges);
        if let Some(generation) = &gauges.generation {
            self.write_generation(generation);
        }
        if let Some(level) = gauges.level {
            self.write_level(level);
        }
    }

    /// Takes `sample` of `sampled`'s chunk files as the page cache's
    /// condition, unless another generation has been swapped in since the
    /// sample began: it describes the generation it read, and the next
    /// sample judges the one serving. A sample that observed nothing
    /// (`Err`, or no page counted) makes the condition unjudged, which caps
    /// nothing and withdraws the resident fraction. The level gauges are
    /// written while the table shows them. A failure to observe a chunk
    /// file is logged at warn when the sample before observed, and the
    /// first sample that observes again after one is too; a sample that
    /// counted no page is unjudged without a log.
    fn observe_page_cache(
        &self,
        sampled: &Arc<Generation>,
        sample: &std::result::Result<ResidentPages, Unobserved>,
    ) {
        let mut gauges = self.gauges.lock().unwrap_or_else(PoisonError::into_inner);
        let serving = self.current.load();
        if !serving
            .as_ref()
            .is_some_and(|serving| Arc::ptr_eq(serving, sampled))
        {
            return;
        }
        let Some(level) = gauges.level.as_mut() else {
            return;
        };
        // intentional: an error is logged below when the sample before
        // observed, and not on every sample after.
        let now = level
            .page_cache
            .unwrap_or(PageCacheCondition::Unjudged)
            .next(sample.as_ref().ok().copied());
        level.page_cache = Some(now);
        let level = *level;
        if gauges.shown {
            self.write_level(level);
        }
        let was_unobserved = gauges.page_cache_unobserved;
        gauges.page_cache_unobserved = sample.is_err();
        drop(gauges);
        match (was_unobserved, sample) {
            (false, Err(error)) => tracing::warn!(
                table = self.name(),
                %error,
                "chunk-file residency cannot be observed, so the page cache caps no level"
            ),
            (true, Ok(_)) => tracing::warn!(
                table = self.name(),
                "chunk-file residency is observed again"
            ),
            (false, Ok(_)) | (true, Err(_)) => {}
        }
    }

    /// The table's entry as it stands; a later retune does not change the
    /// one returned.
    pub fn config(&self) -> Arc<TableConfig> {
        self.config.load_full()
    }

    /// First load: resolves the source, builds the snapshot the ref names,
    /// runs the doctor on it, and swaps it in, within the load deadline;
    /// past it the load is dropped as `Error::LoadDeadline`. `Err` leaves
    /// the table `Degraded` with no generation; a first-load failure is
    /// fatal to the process (RFD 005) and is the caller's to report, so it
    /// is not counted under `refresh_errors_total`. On a table that already
    /// serves, a full re-resolve and reload, reusing every file the new
    /// snapshot still names by the same path and size, within the refresh
    /// deadline; a failure leaves the table as a poll's would (`fail`),
    /// `Held` for a `below_level` refusal.
    pub async fn load(&self) -> Result<()> {
        let _serialized = self.serial.lock().await;
        self.within_deadline(self.reload())
            .await
            .map_err(|failed| self.fail(failed))
    }

    /// One poll, serialized per table. `Ok(true)` when a new generation was
    /// swapped in. With no current generation, a full load (a table left
    /// `Degraded` with no generation recovers on the next poll); otherwise
    /// one small read when nothing moved (RFD 005), and a build with reuse
    /// when the ref advanced. `Err` leaves the current generation serving,
    /// the table `Degraded`, or `Held` for a `below_level` refusal, and
    /// counts one `refresh_errors_total` with the error's kind as its
    /// reason; `Ok(false)` after such a failure ends the state, since the
    /// table is in step with its ref again. A failure over a serving
    /// generation that is fixed for the snapshot is the exception
    /// (`Recurrence::Deterministic`): the table remembers it, and while the
    /// source names the same metadata location each poll costs only the
    /// small read, returns `Ok(false)`, counts nothing, and leaves the table
    /// in the state that failure set (`fail`). A transient failure is tried
    /// again on the next poll.
    /// The poll runs within the refresh deadline, or with no generation
    /// serving within the load deadline; past it the poll is dropped as
    /// `Error::RefreshDeadline` or `Error::LoadDeadline`, a failure like any
    /// other.
    pub async fn refresh(&self) -> Result<bool> {
        let _serialized = self.serial.lock().await;
        let outcome = self
            .within_deadline(async {
                match self.current() {
                    None => self.reload().await.map(|()| true),
                    Some(current) => self.poll(&current).await,
                }
            })
            .await;
        match outcome {
            Ok(swapped) => {
                if swapped {
                    self.swaps.increment(1);
                }
                Ok(swapped)
            }
            Err(failed) => {
                metrics::counter!(
                    metric::REFRESH_ERRORS_TOTAL,
                    metric::LABEL_TABLE => self.name.clone(),
                    metric::LABEL_REASON => failed.error.kind().to_string()
                )
                .increment(1);
                Err(self.fail(failed))
            }
        }
    }

    /// The table's configured name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The lifecycle state now.
    pub fn state(&self) -> TableState {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The serving generation, or `None` before the first successful load.
    pub fn current(&self) -> Option<Arc<Generation>> {
        self.current.load_full()
    }

    /// `current`, or `Error::NotReady`: the one refusal for a table with no
    /// generation to answer from, which `get`, `key_type`, the gRPC
    /// service, and the one-shot subcommands all raise.
    pub fn serving(&self) -> Result<Arc<Generation>> {
        self.current().ok_or_else(|| self.not_ready())
    }

    /// The serving generation's key type, or `Error::NotReady`: the type a
    /// caller reads a key from the wire under before `get` or `multi_get`. Read through a `load` guard dropped before it
    /// returns, so it writes no reference count on the shared generation. A
    /// swap onto a generation of another key type between this and the
    /// lookup is refused by the lookup's own check, as `Error::KeyType`.
    pub fn key_type(&self) -> Result<KeyType> {
        self.current
            .load()
            .as_deref()
            .map(|generation| generation.layout.key_type)
            .ok_or_else(|| self.not_ready())
    }

    /// The key type of the snapshot the ref names now, from that snapshot's
    /// own schema, with no data file read: a table root's version hint and
    /// metadata file, a metadata file alone, or one catalog call. What a
    /// caller parses command-line keys under before it pays for a `load`.
    /// The schema and partition-spec faults a load would refuse are refused
    /// here first, as the load refuses them. A load afterward resolves the
    /// source again, and a key of a type its generation no longer has is
    /// refused at lookup as `Error::KeyType`. Serialized with `load` and
    /// `refresh`, and bounded as a load from nothing is.
    pub async fn resolve_key_type(&self) -> Result<KeyType> {
        let _serialized = self.serial.lock().await;
        self.within_deadline(async {
            let resolved = self.source.resolve(&self.storage).await?;
            let snapshot = self.ref_snapshot(&resolved)?;
            let config = self.config.load();
            let layout = TableLayout::derive(
                self.name(),
                &resolved.metadata,
                &resolved.unread_defaults,
                &snapshot,
                &config.key_column,
                &config.value_column,
            )?;
            Ok(layout.key_type)
        })
        .await
        .map_err(|failed| *failed.error)
    }

    /// One lookup against the serving generation. `Error::NotReady` with no
    /// generation; `Error::KeyType` for a key of another type than the
    /// table's, before any read.
    pub async fn get(&self, key: &Key) -> Result<Lookup> {
        let _in_flight = self.lookups.enter();
        // A guard held across the lookup's awaits stays correct wherever
        // the task resumes. While held it occupies one of the loading
        // thread's few fast slots (arc-swap's debts), and a load on a
        // thread whose slots are all taken takes arc-swap's slower path,
        // about what `load_full` costs.
        let current = self.current.load();
        let generation = current.as_deref().ok_or_else(|| self.not_ready())?;
        self.check_key_type(generation, key)?;
        let value = self.lookups.one(generation, key).await?;
        Ok(Lookup {
            value,
            snapshot_id: generation.snapshot_id,
        })
    }

    /// Moves the lookups counted since the last call into `lookups_total`.
    /// A lookup is counted on its thread's shard, not on the shared series,
    /// so a scrape calls this first to read every lookup counted before it;
    /// the binary's `/metrics` does, and a dropped table does it last.
    pub fn publish_lookup_counts(&self) {
        self.lookups.metrics.publish();
    }

    /// The calls in flight this table counts among, for the tests of which
    /// tables share one.
    #[cfg(test)]
    pub(crate) fn calls_in_flight(&self) -> &CallsInFlight {
        &self.lookups.in_flight
    }

    /// Several lookups against one generation, answered in key order. Every
    /// key's type is checked before any read, so a mismatch anywhere fails
    /// the whole call and issues nothing; any failure fails the whole call
    /// (RFD 010). While fewer `get` and `multi_get` calls than worker
    /// threads are in flight on the tables sharing this one's
    /// `TableHandles::calls`, this one included, the keys are split, in
    /// order, into up to one run per worker thread (`shard::workers`) of at
    /// least `MULTI_GET_TASK_KEYS_MIN` keys, each spawned as a task so a
    /// batch's decodes run on several cores (`Lookups::runs`). A batch that
    /// makes one run, as a batch does once that many calls are in flight,
    /// is looked up here, unspawned. `open_with` gives every table of a
    /// process one count, so calls to any of them count toward holding a
    /// batch back from splitting. The runs share one bound of `MULTI_GET_FAN_OUT` lookups
    /// in flight. Dropping the returned future, as a deadline does,
    /// aborts the runs still going. A run's panic is resumed here; a run
    /// canceled under a runtime shutdown is `Error::LookupCanceled`.
    pub async fn multi_get(&self, keys: &[Key]) -> Result<MultiLookup> {
        let _in_flight = self.lookups.enter();
        let generation = self.serving()?;
        for key in keys {
            self.check_key_type(&generation, key)?;
        }
        let fan_out = Arc::new(Semaphore::new(MULTI_GET_FAN_OUT));
        let runs = self.lookups.runs(keys.len());
        let values = if runs <= 1 {
            self.lookups.run(&generation, keys, &fan_out).await?
        } else {
            self.run_spawned(&generation, keys, runs, &fan_out).await?
        };
        Ok(MultiLookup {
            values,
            snapshot_id: generation.snapshot_id,
        })
    }

    /// `keys` split in order into `runs` runs of nearly equal length, each
    /// run spawned on its own task over owned copies of its keys, and the
    /// answers put back in key order. The tasks live in a `JoinSet`, so
    /// returning early on a failure, or being dropped, aborts the rest.
    async fn run_spawned(
        &self,
        generation: &Arc<Generation>,
        keys: &[Key],
        runs: usize,
        fan_out: &Arc<Semaphore>,
    ) -> Result<Vec<Value>> {
        let per_run = keys.len().div_ceil(runs);
        let mut tasks = JoinSet::new();
        for (index, part) in keys.chunks(per_run).enumerate() {
            let lookups = Arc::clone(&self.lookups);
            let generation = Arc::clone(generation);
            let fan_out = Arc::clone(fan_out);
            let part = part.to_vec();
            tasks.spawn(async move { (index, lookups.run(&generation, &part, &fan_out).await) });
        }
        let mut answered: Vec<Vec<Value>> = vec![Vec::new(); tasks.len()];
        while let Some(joined) = tasks.join_next().await {
            let (index, values) = joined.map_err(|error| match error.try_into_panic() {
                Ok(payload) => std::panic::resume_unwind(payload),
                Err(canceled) => Error::LookupCanceled {
                    table: self.name.clone(),
                    source: canceled,
                },
            })?;
            answered[index] = values?;
        }
        Ok(answered.into_iter().flatten().collect())
    }

    /// Holds the table's load and refresh lock, so no load is in flight
    /// while the holder reads the current generation's identities or
    /// retunes the table; for `Opened::retain`, which takes every table's
    /// in turn, and `Opened::apply`, which takes every retuned table's.
    pub(crate) async fn serialized(&self) -> MutexGuard<'_, ()> {
        self.serial.lock().await
    }

    /// Whether `config`, an entry that differs from the table's in
    /// `lookups` and `strict` alone (`TableConfig::same_but_levels`), can be
    /// applied in place: a `lookups` the residency cannot reach is refused
    /// as a load refuses it (`TableConfig::check_reachable`), and the
    /// serving generation is judged against the entry (`Generation::judge`),
    /// so a strict entry whose level the serving snapshot does not meet is
    /// refused as `RefusalReason::BelowLevel`, as a load under it would
    /// refuse that snapshot. With no generation serving there is nothing to
    /// judge. Only a judgement made while holding `serial` (`serialized`)
    /// through `retune` decides: without it a build may swap another
    /// generation in after the judgement.
    pub(crate) fn judge_retune(&self, config: &TableConfig) -> Result<()> {
        config.check_reachable()?;
        match self.current() {
            Some(current) => current.judge(config.lookups(), config.strict),
            None => Ok(()),
        }
    }

    /// Applies `config`, which `judge_retune` admitted under the same held
    /// `serial`, in place (RFD 020): the serving generation keeps serving
    /// and nothing is read. The level gauges declare the new `lookups` at
    /// once, against the newest measured snapshot's level, which a retune
    /// does not change. Every remembered refusal is forgotten, and logged
    /// as forgotten: the known location becomes the serving generation's,
    /// so the next poll builds the snapshot the ref names under the new
    /// entry (RFD 005). A `Held` state ends in `Serving` when the new entry
    /// would admit the held snapshot's level, which the next poll then
    /// swaps in, and stays `Held` until that poll refuses it again
    /// otherwise. A `Degraded` state stays until the next poll ends it,
    /// since the last load or refresh failed whatever the entry.
    pub(crate) fn retune(&self, config: TableConfig, _serialized: &MutexGuard<'_, ()>) {
        let (declared, strict) = (config.lookups(), config.strict);
        self.config.store(Arc::new(config));
        {
            let mut gauges = self.gauges.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(level) = gauges.level.as_mut() {
                level.declared = declared;
                let level = *level;
                if gauges.shown {
                    self.write_level(level);
                }
            }
        }
        let Some(current) = self.current() else {
            return;
        };
        let previous = self
            .known
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .replace(Known {
                location: current.metadata_location.clone(),
                refused: None,
            });
        if let Some(Known {
            location,
            refused: Some(past),
        }) = previous
        {
            tracing::info!(
                table = self.name(),
                snapshot_id = past.snapshot_id.map(tracing::field::display),
                reason = %past.reason,
                location,
                "retune forgot a refused snapshot"
            );
        }
        if let TableState::Held { level, .. } = self.state()
            && (!strict || level.meets(declared))
        {
            self.serve_state(current.snapshot_id);
        }
    }

    /// `work` within the deadline its build takes: with no generation
    /// serving, a build from nothing, the load deadline, or
    /// `Error::LoadDeadline` once it passes; over a serving generation,
    /// which reuses every unchanged file, the refresh deadline, or
    /// `Error::RefreshDeadline`. Called with `serial` held, so no other
    /// build swaps a generation in between the choice and the work. The
    /// work is dropped at the deadline, which cancels its reads and
    /// releases what it reserved, so the caller returns, and releases
    /// `serial`, once the deadline passes, and the next poll or
    /// `Opened::retain` can take it. A location error from the work is
    /// given the table's name (`Error::for_table`).
    async fn within_deadline<T>(&self, work: impl Future<Output = Attempt<T>>) -> Attempt<T> {
        let table = &self.name;
        let outcome = if self.current().is_none() {
            let after = self.load_deadline;
            tokio::time::timeout(after, work)
                .await
                .map_err(|_elapsed| Error::LoadDeadline {
                    table: table.clone(),
                    after,
                })?
        } else {
            let after = self.refresh_deadline;
            tokio::time::timeout(after, work)
                .await
                .map_err(|_elapsed| Error::RefreshDeadline {
                    table: table.clone(),
                    after,
                })?
        };
        outcome.map_err(|Failed { error, snapshot_id }| Failed {
            error: Box::new(error.for_table(table)),
            snapshot_id,
        })
    }

    /// Resolves the source and builds the snapshot the ref names over the
    /// current generation, whether or not one serves.
    async fn reload(&self) -> Attempt<()> {
        let resolved = self.source.resolve(&self.storage).await?;
        let snapshot = self.ref_snapshot(&resolved)?;
        let previous = self.current();
        self.build_and_admit(&resolved, &snapshot, previous.as_ref())
            .await
    }

    /// One poll over `current`: the source is asked whether its metadata
    /// moved from the known location, and the table advances to what the
    /// ref names there (`advance`). A failure of that step fixed for the
    /// snapshot is remembered against the location, with the snapshot it
    /// was met building, so an unchanged source then builds nothing.
    async fn poll(&self, current: &Arc<Generation>) -> Attempt<bool> {
        let known = self
            .known
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
            .expect("a serving generation was admitted, which recorded its metadata location");
        let Some(resolved) = self
            .source
            .resolve_if_changed(&self.storage, Some(&known.location))
            .await?
        else {
            match &known.refused {
                Some(refusal) => self.held_back(refusal),
                None => self.in_step(current),
            }
            return Ok(false);
        };
        self.advance(&resolved, current)
            .await
            .inspect_err(|failed| {
                if failed.error.recurrence() == Recurrence::Deterministic {
                    let reason = failed.error.kind();
                    let refusal = Refusal {
                        snapshot_id: failed.snapshot_id,
                        since: self.refused_since(failed.snapshot_id, &reason),
                        reason,
                    };
                    self.remember(&resolved.location, Some(refusal));
                }
            })
    }

    /// Moves the table onto the snapshot the ref names in `resolved`: only
    /// a snapshot with another id than `current`'s is built. `Ok(true)`
    /// when one was swapped in.
    async fn advance(&self, resolved: &Resolved, current: &Arc<Generation>) -> Attempt<bool> {
        let snapshot = self.ref_snapshot(resolved)?;
        if SnapshotId(snapshot.snapshot_id()) == current.snapshot_id {
            self.remember(&resolved.location, None);
            self.in_step(current);
            return Ok(false);
        }
        self.build_and_admit(resolved, &snapshot, Some(current))
            .await?;
        Ok(true)
    }

    /// The snapshot the configured ref names in `resolved`, under the
    /// table's name.
    fn ref_snapshot(&self, resolved: &Resolved) -> Result<Arc<Snapshot>> {
        resolved.snapshot_for_ref(&self.name, &self.config.load().r#ref)
    }

    /// Plans `snapshot` over `previous` at the footer stage, judges its
    /// level there, builds it, gates it, and swaps it in, all under one
    /// entry. A failure carries `snapshot`'s id.
    async fn build_and_admit(
        &self,
        resolved: &Resolved,
        snapshot: &Snapshot,
        previous: Option<&Arc<Generation>>,
    ) -> Attempt<()> {
        let config = self.config();
        let admitted: Result<()> = async {
            // The footer stage and the chunk stage are boxed: inline, the
            // larger of them puts a refresh's future over the size
            // clippy::large_futures allows a future that is moved.
            let mut planned = Box::pin(Generation::plan_load(
                &config,
                resolved,
                snapshot,
                &self.storage,
                Arc::clone(&self.hedge),
                previous.map(Arc::as_ref),
            ))
            .await?;
            planned.judge_level(self.local.as_ref()).await?;
            let generation = Box::pin(planned.build(self.local.as_ref())).await?;
            let findings = self.gate(&generation, &config)?;
            self.swap(generation, previous, &findings, config.lookups());
            self.remember(&resolved.location, None);
            Ok(())
        }
        .await;
        admitted.map_err(|error| Failed {
            error: Box::new(error),
            snapshot_id: Some(SnapshotId(snapshot.snapshot_id())),
        })
    }

    /// The poll found the table in step with its ref: `current` is the
    /// snapshot the ref names, so it is the newest snapshot and the level
    /// gauges measure it, in place of a snapshot a `below_level` refusal
    /// held, whatever the state (a retune can end `Held` with the held
    /// snapshot's level still measured). A `Degraded` or `Held` state an
    /// earlier poll left ends, since the last refresh succeeded.
    fn in_step(&self, current: &Generation) {
        self.measured(current.level);
        if matches!(
            self.state(),
            TableState::Degraded { .. } | TableState::Held { .. }
        ) {
            self.serve_state(current.snapshot_id);
        }
    }

    /// Records that `snapshot_id` serves from now; the one writer of
    /// `TableState::Serving`.
    fn serve_state(&self, snapshot_id: SnapshotId) {
        self.set_state(TableState::Serving {
            snapshot_id,
            since: Instant::now(),
        });
    }

    /// The doctor's verdict on a built generation before it is swapped in
    /// (RFD 016), under the entry `config` it was built for. Every finding
    /// is logged as the table's `FindingLog` says.
    /// The level was judged at the footer stage (`Planned::judge_level`);
    /// the generation's measured level is judged again under `strict` as a
    /// backstop, by the same `check::judge`, so a generation below the
    /// table's `lookups` is never swapped in whatever the footer stage
    /// missed. `Ok` carries the admitted generation's findings per code,
    /// for the `findings` gauge; a refused generation's reach the logs and
    /// the error and never the gauge, which describes the serving
    /// generation.
    fn gate(
        &self,
        generation: &Generation,
        config: &TableConfig,
    ) -> Result<BTreeMap<FindingCode, u64>> {
        let declared = config.lookups();
        let report = check::diagnose(generation, declared);
        let mut counts: BTreeMap<FindingCode, u64> = BTreeMap::new();
        for finding in &report.findings {
            *counts.entry(finding.code()).or_default() += 1;
            log_finding(self.name(), finding, self.findings, declared);
        }
        generation.judge(declared, config.strict)?;
        Ok(counts)
    }

    /// Makes `generation` the serving one. The generation gauges it sets,
    /// `index_bytes`, `findings` from `findings`,
    /// `generation_sequence_number`, `generation_swapped_at_seconds` from
    /// the clock, the lookup-cost gauges from its `lookup_cost`, and the
    /// delete gauges from its `delete_summary`, are kept and written while
    /// the table shows them
    /// (`write_generation`), and the level gauges take the generation's
    /// level against `declared`, keeping the page cache's condition.
    fn swap(
        &self,
        generation: Generation,
        previous: Option<&Arc<Generation>>,
        findings: &BTreeMap<FindingCode, u64>,
        declared: Level,
    ) {
        let reused = previous.map_or(0, |previous| {
            generation
                .files
                .iter()
                .filter(|file| previous.files.iter().any(|old| Arc::ptr_eq(file, old)))
                .count()
        });
        tracing::info!(
            table = self.name(),
            snapshot_id = %generation.snapshot_id,
            files = generation.files.len(),
            reused,
            records = generation.records,
            residency = %generation.residency,
            index_bytes = generation.index_bytes,
            "swapped in a generation"
        );
        let published = GenerationGauges {
            index_bytes: generation.index_bytes,
            findings: findings.clone(),
            sequence_number: generation.sequence_number,
            swapped_at: time::unix_seconds(time::now()),
            lookup_cost: generation.lookup_cost,
            deletes: generation.delete_summary(),
        };
        let snapshot_id = generation.snapshot_id;
        let replaced = {
            let mut gauges = self.gauges.lock().unwrap_or_else(PoisonError::into_inner);
            let level = LevelGauges {
                measured: generation.level,
                declared,
                page_cache: gauges.level.and_then(|level| level.page_cache),
            };
            if gauges.shown {
                self.write_generation(&published);
                self.write_level(level);
            }
            gauges.generation = Some(published);
            gauges.level = Some(level);
            // Stored under the lock, so a page-cache sample of the
            // generation this replaces is seen to be stale
            // (`observe_page_cache`).
            self.current.swap(Some(Arc::new(generation)))
        };
        drop(replaced);
        self.serve_state(snapshot_id);
    }

    /// Records the failure as the table's state and hands its error back
    /// to the caller, which logs it. The current generation is untouched. A
    /// `below_level` refusal measured a snapshot, so the level gauges take
    /// its level once the table has swapped one in, and it makes the table
    /// `Held` on the snapshot it was met building whenever a generation
    /// serves, from a poll or a `load` alike (`failed_state`); every other
    /// failure, and a first load below its level, makes it `Degraded`.
    fn fail(&self, Failed { error, snapshot_id }: Failed) -> Error {
        let reason = error.kind();
        if let ErrorKind::Refused(RefusalReason::BelowLevel { achieved, .. }) = &reason {
            self.measured(*achieved);
        }
        self.set_state(self.failed_state(&Refusal {
            snapshot_id,
            since: self.refused_since(snapshot_id, &reason),
            reason,
        }));
        *error
    }

    /// When the refusal of `snapshot_id` for `reason` counts from: a `Held`
    /// table refused below its level again for the snapshot it holds keeps
    /// the `since` of the first refusal, so the state's age is how long that
    /// snapshot has been held; any other refusal counts from now.
    fn refused_since(&self, snapshot_id: Option<SnapshotId>, reason: &ErrorKind) -> Instant {
        if let TableState::Held { refused, since, .. } = self.state()
            && let ErrorKind::Refused(RefusalReason::BelowLevel { .. }) = reason
            && snapshot_id == Some(refused)
        {
            since
        } else {
            Instant::now()
        }
    }

    /// The poll found the source still naming the location where `refusal`
    /// was met, so the table stays in the state that refusal set
    /// (`failed_state`). The state is set again when it is another: a
    /// transient failure of a poll since the refusal replaced it.
    fn held_back(&self, refusal: &Refusal) {
        let state = self.failed_state(refusal);
        let already = match (&state, self.state()) {
            (
                TableState::Held { refused, .. },
                TableState::Held {
                    refused: current, ..
                },
            ) => *refused == current,
            (
                TableState::Degraded { reason, .. },
                TableState::Degraded {
                    reason: current, ..
                },
            ) => *reason == current,
            (
                TableState::Loading
                | TableState::Serving { .. }
                | TableState::Degraded { .. }
                | TableState::Held { .. },
                _,
            ) => false,
        };
        if !already {
            self.set_state(state);
        }
    }

    /// The state `refusal` leaves the table in: `Held` for a `below_level`
    /// refusal of a named snapshot while a generation serves (RFD 020),
    /// `Degraded` for anything else.
    fn failed_state(&self, refusal: &Refusal) -> TableState {
        let serving = self.current().map(|generation| generation.snapshot_id);
        match (&refusal.reason, serving, refusal.snapshot_id) {
            (
                ErrorKind::Refused(RefusalReason::BelowLevel { achieved, .. }),
                Some(serving),
                Some(refused),
            ) => TableState::Held {
                serving,
                refused,
                level: *achieved,
                since: refusal.since,
            },
            _ => TableState::Degraded {
                serving,
                reason: refusal.reason.clone(),
                since: refusal.since,
            },
        }
    }

    /// Takes `measured` as the newest measured snapshot's level for the
    /// level gauges, once a swap made them appear, and writes them while
    /// the table shows its gauges and the level changed.
    fn measured(&self, measured: Level) {
        let mut gauges = self.gauges.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(level) = gauges.level.as_mut() else {
            return;
        };
        if level.measured == measured {
            return;
        }
        level.measured = measured;
        let level = *level;
        if gauges.shown {
            self.write_level(level);
        }
    }

    /// Records the metadata location the table is now in step with, or
    /// where `refused` was met. Replacing a remembered refusal with another
    /// location logs, once, that the source moved past it, with the
    /// refused snapshot when the ref named one.
    fn remember(&self, location: &str, refused: Option<Refusal>) {
        let next = Known {
            location: location.to_string(),
            refused,
        };
        let previous = self
            .known
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .replace(next);
        if let Some(Known {
            location: was,
            refused: Some(past),
        }) = previous
            && was != location
        {
            tracing::info!(
                table = self.name(),
                snapshot_id = past.snapshot_id.map(tracing::field::display),
                reason = %past.reason,
                location,
                "source moved past a refused snapshot"
            );
        }
    }

    /// Replaces the state and publishes it.
    fn set_state(&self, state: TableState) {
        *self.state.lock().unwrap_or_else(PoisonError::into_inner) = state;
        self.publish_state();
    }

    /// Sets the `table_state` gauge to 1 for the current state and 0 for
    /// the others, while the table shows its gauges; every
    /// state the table is ever in, the initial `Loading` included, passes
    /// through here or through `show`.
    fn publish_state(&self) {
        let gauges = self.gauges.lock().unwrap_or_else(PoisonError::into_inner);
        if gauges.shown {
            self.write_state(&gauges);
        }
    }

    /// Writes the `table_state` gauge; `_shown` is the held lock that
    /// orders the write against `withdraw`.
    fn write_state(&self, _shown: &Gauges) {
        let current = self.state().name();
        for name in StateName::ALL {
            metrics::gauge!(
                metric::TABLE_STATE,
                metric::LABEL_TABLE => self.name.clone(),
                metric::LABEL_STATE => name.to_string()
            )
            .set(approx::gauge(u64::from(name == current)));
        }
    }

    /// Writes the generation gauges from `generation`; called with the
    /// gauges' lock held. The `findings` gauge is set for every code, zero
    /// for the absent ones, so a code that cleared on the swap reads zero.
    fn write_generation(&self, generation: &GenerationGauges) {
        metrics::gauge!(
            metric::INDEX_BYTES,
            metric::LABEL_TABLE => self.name.clone()
        )
        .set(approx::gauge_len(generation.index_bytes));
        for code in FindingCode::ALL {
            metrics::gauge!(
                metric::FINDINGS,
                metric::LABEL_TABLE => self.name.clone(),
                metric::LABEL_CODE => code.to_string()
            )
            .set(approx::gauge(
                generation.findings.get(&code).copied().unwrap_or(0),
            ));
        }
        metrics::gauge!(
            metric::GENERATION_SEQUENCE_NUMBER,
            metric::LABEL_TABLE => self.name.clone()
        )
        .set(approx::signed_gauge(generation.sequence_number.0));
        metrics::gauge!(
            metric::GENERATION_SWAPPED_AT_SECONDS,
            metric::LABEL_TABLE => self.name.clone()
        )
        .set(generation.swapped_at);
        let cost = generation.lookup_cost;
        metrics::gauge!(
            metric::TABLE_GETS_PER_HIT,
            metric::LABEL_TABLE => self.name.clone()
        )
        .set(approx::gauge(cost.gets_per_hit));
        metrics::gauge!(
            metric::TABLE_GETS_PER_MISS,
            metric::LABEL_TABLE => self.name.clone()
        )
        .set(approx::gauge(cost.gets_per_miss));
        metrics::gauge!(
            metric::TABLE_FILES_PROBED_PER_LOOKUP,
            metric::LABEL_TABLE => self.name.clone()
        )
        .set(approx::gauge_len(cost.files_probed));
        let deletes = generation.deletes;
        metrics::gauge!(
            metric::TABLE_DELETED_ROWS,
            metric::LABEL_TABLE => self.name.clone()
        )
        .set(approx::gauge(deletes.rows));
        metrics::gauge!(
            metric::TABLE_DELETED_FRACTION,
            metric::LABEL_TABLE => self.name.clone()
        )
        .set(deletes.fraction);
        metrics::gauge!(
            metric::TABLE_DELETE_FILES,
            metric::LABEL_TABLE => self.name.clone()
        )
        .set(approx::gauge(deletes.files));
        metrics::gauge!(
            metric::TABLE_DELETE_STATE_BYTES,
            metric::LABEL_TABLE => self.name.clone()
        )
        .set(approx::gauge_len(deletes.state_bytes));
    }

    /// Writes the level gauges from `level`, and the resident fraction of
    /// the chunk files while the page cache's condition is judged, which
    /// the binary's recorder then drops at the next scrape; called with the
    /// gauges' lock held.
    fn write_level(&self, level: LevelGauges) {
        metrics::gauge!(
            metric::TABLE_BELOW_LEVEL,
            metric::LABEL_TABLE => self.name.clone()
        )
        .set(approx::gauge(u64::from(level.below_level())));
        metrics::gauge!(
            metric::TABLE_LEVEL,
            metric::LABEL_TABLE => self.name.clone()
        )
        .set(level.achieved().gauge());
        metrics::gauge!(
            metric::TABLE_DECLARED_LEVEL,
            metric::LABEL_TABLE => self.name.clone()
        )
        .set(level.declared.gauge());
        if let Some(fraction) = level.page_cache.and_then(PageCacheCondition::fraction) {
            metrics::gauge!(
                metric::TABLE_CHUNK_RESIDENT_FRACTION,
                metric::LABEL_TABLE => self.name.clone()
            )
            .set(fraction);
        }
    }

    /// `Error::NotReady` for this table.
    fn not_ready(&self) -> Error {
        Error::NotReady {
            table: self.name.clone(),
        }
    }

    /// `Error::KeyType` for a key of another type than the generation's.
    fn check_key_type(&self, generation: &Generation, key: &Key) -> Result<()> {
        let expected = generation.layout.key_type;
        let got = key.key_type();
        if got == expected {
            Ok(())
        } else {
            Err(Error::KeyType {
                table: self.name.clone(),
                expected,
                got,
            })
        }
    }
}

/// Samples which pages of the serving generation's chunk files the
/// kernel's page cache holds, for every one of `tables` (RFD 020), on one
/// blocking task, sampling each chunk file once however many of `tables`
/// read it, as two entries over one Iceberg table do. Each table takes its
/// sample as the page cache's condition (`Table::observe_page_cache`):
/// the table falls below optimal while the resident fraction is short and
/// recovers once it is not, with hysteresis (`PageCacheCondition::next`).
/// A table with no generation serving, or not in the Disk tier, samples
/// nothing. The condition caps the level the gauges show and nothing else:
/// whether a snapshot is refused or held, and how a retune is judged, read
/// the layout's level alone, since the page cache depends on the server
/// and its other tables, not on the snapshot.
pub(crate) async fn sample_page_cache(tables: &[&Table]) {
    let serving: Vec<(&Table, Arc<Generation>)> = tables
        .iter()
        .filter_map(|table| {
            let generation = table.current()?;
            (generation.residency == Residency::Disk).then_some((*table, generation))
        })
        .collect();
    if serving.is_empty() {
        return;
    }
    let generations: Vec<Arc<Generation>> = serving
        .iter()
        .map(|(_, generation)| Arc::clone(generation))
        .collect();
    let Ok(samples) = on_blocking_pool(move || {
        let mut sampled = HashMap::new();
        generations
            .iter()
            .map(|generation| generation.chunk_residency_in(&mut sampled))
            .collect::<Vec<_>>()
    })
    .await
    else {
        // intentional: the runtime is shutting down, and every condition
        // stays as the last sample left it.
        return;
    };
    for ((table, generation), sample) in serving.iter().zip(&samples) {
        table.observe_page_cache(generation, sample);
    }
}

impl fmt::Debug for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Table")
            .field("name", &self.name)
            .field("state", &self.state())
            .finish_non_exhaustive()
    }
}

/// One finding as a log event, with the table, code, cap, and file as
/// fields, the code and cap in the names the doctor's JSON lines carry
/// (`caps` absent for advice), and the finding's message as the event's:
/// under `FindingLog::Events` at warn when its cap is below `declared`, the
/// table's level, and at info otherwise; under `FindingLog::Debug` at
/// debug.
fn log_finding(table: &str, finding: &Finding, log: FindingLog, declared: Level) {
    let message = finding.message();
    let code = finding.code();
    let caps = finding.caps().map(Level::name);
    let file = finding.file();
    let below = finding.caps().is_some_and(|cap| !cap.meets(declared));
    match (log, below) {
        (FindingLog::Events, true) => {
            tracing::warn!(table, code = %code, caps, file, "{message}");
        }
        (FindingLog::Events, false) => {
            tracing::info!(table, code = %code, caps, file, "{message}");
        }
        (FindingLog::Debug, true | false) => {
            tracing::debug!(table, code = %code, caps, file, "{message}");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::ops::Range;
    use std::sync::Arc;

    use bytes::Bytes;
    use iceberg::io::FileIO;
    use metrics::{Key as MetricKey, SharedString, Unit};
    use metrics_util::CompositeKey;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};

    use object_store::local::LocalFileSystem;
    use object_store::throttle::{ThrottleConfig, ThrottledStore};

    use super::*;
    use crate::catalog::TableFault;
    use crate::config::{Residency, ServerConfig};
    use crate::error::{Budget, ErrorKind, UnsupportedDelete};
    use crate::generation::testing::{
        Bloom, Bounds, Dictionary, E2E_KEYS, FILE_KEYS, FIRST_SEQUENCE, FIRST_SNAPSHOT,
        FIRST_VERSION, FileSpec, KEY_STEP, POLL_GETS, RESOLVE_GETS, SECOND_SEQUENCE,
        SECOND_SNAPSHOT, SECOND_VERSION, TABLE, THIRD_SEQUENCE, THIRD_SNAPSHOT, THIRD_VERSION,
        TINY_BUDGET, TWO, Table as Fixture, WriterLayout, bucketing, cache, keys_from,
        keys_in_bucket, local_dir, long_keys, parquet, refusal_of, two_bucket_table, value_for,
        values_of,
    };
    use crate::index::testing::{with_dictionary_one_entry_short, with_first_page_claiming};
    use crate::index::{DecodedPart, FileFault};
    use crate::key::KeyType;
    use crate::level::{FALL_PER_MILLE, PER_MILLE};
    #[cfg(target_os = "linux")]
    use crate::local::testing::evict;
    use crate::local::testing::{chunks_offset, overwrite};
    use crate::manifest::testing::{self as manifest_testing, Partition};
    use crate::metric::LookupResult;
    use crate::storage::testing::{DEADLINE_GUARD, Gauged, STALL};
    use crate::storage::{FILE_PREFIX, LocationError, LocationFault};

    /// The positions in bucket 0's key list that the duplicate file
    /// repeats.
    const DUP_KEYS: Range<usize> = 1..3;
    /// The name of a second, strict table in the metrics tests, beside the
    /// fixture's `TABLE`.
    const STRICT_TABLE: &str = "strict";
    /// The refresh deadline of the tests that stall a refresh: short
    /// enough to keep the test quick.
    const TEST_REFRESH_DEADLINE: Duration = Duration::from_secs(1);
    /// The load deadline of the tests that stall a load: long enough for
    /// the fixtures' first load, and other than `TEST_REFRESH_DEADLINE`,
    /// so a test tells which deadline ended the work.
    const TEST_LOAD_DEADLINE: Duration = Duration::from_secs(2);

    /// A one-bucket fixture with one file of `FILE_KEYS` keys laid out as
    /// `layout` says, committed as `FIRST_SNAPSHOT`.
    async fn one_file_fixture(layout: WriterLayout) -> Fixture {
        let mut fixture = Fixture::bucketed(1);
        let keys = long_keys(&keys_from(0, FILE_KEYS));
        fixture
            .add_file(FileSpec {
                name: "f",
                keys: &keys,
                values: values_of(&keys_from(0, FILE_KEYS)),
                partition: Partition::Bucket(0),
                bounds: Bounds::Exact,
                layout,
            })
            .await;
        fixture
            .commit(FIRST_SNAPSHOT, None, FIRST_SEQUENCE, FIRST_VERSION)
            .await;
        fixture
    }

    /// A table following `fixture`'s main branch under `config`, with no
    /// local directory and the default load, refresh, and get deadlines,
    /// in `Loading`.
    async fn table_over(fixture: &Fixture, config: TableConfig) -> Table {
        let defaults = ServerConfig::default();
        table_within(
            fixture,
            config,
            defaults.load_deadline,
            defaults.refresh_deadline,
        )
        .await
    }

    /// `table_over` with `load_deadline` on every build from no generation
    /// and `refresh_deadline` on every build over one.
    async fn table_within(
        fixture: &Fixture,
        config: TableConfig,
        load_deadline: Duration,
        refresh_deadline: Duration,
    ) -> Table {
        let handles = handles_over(fixture).await;
        table_sharing(fixture, config, handles, load_deadline, refresh_deadline)
    }

    /// Handles over `fixture`'s storage, a fresh cache, no local directory,
    /// and a count of calls in flight of their own.
    async fn handles_over(fixture: &Fixture) -> TableHandles {
        TableHandles {
            storage: Arc::clone(&fixture.storage),
            cache: cache().await,
            local: None,
            calls: CallsInFlight::new(),
            findings: FindingLog::Events,
        }
    }

    /// A table following `fixture`'s main branch under `config`, built over
    /// `handles`, which other tables may share, in `Loading`.
    fn table_sharing(
        fixture: &Fixture,
        config: TableConfig,
        handles: TableHandles,
        load_deadline: Duration,
        refresh_deadline: Duration,
    ) -> Table {
        Table::new(
            config,
            TableSource::Static {
                location: fixture.root(),
            },
            handles,
            load_deadline,
            refresh_deadline,
            ServerConfig::default().get_deadline,
        )
    }

    /// Replaces the store `fixture`'s storage reads `file://` locations
    /// through with one whose every GET waits `STALL`, as a wedged store
    /// does. Handles a loaded generation opened keep the store they were
    /// opened over.
    fn stall_reads(fixture: &Fixture) {
        let stalled = ThrottledStore::new(
            LocalFileSystem::new(),
            ThrottleConfig {
                wait_get_per_call: STALL,
                ..ThrottleConfig::default()
            },
        );
        fixture.storage.register(FILE_PREFIX, Arc::new(stalled));
    }

    /// Runs `work`, which a deadline should end, under `DEADLINE_GUARD`,
    /// and expects the error of the deadline `kind` names, for `TABLE`,
    /// after `deadline`.
    async fn ends_at_deadline<T: fmt::Debug>(
        work: impl Future<Output = Result<T>>,
        kind: ErrorKind,
        deadline: Duration,
    ) {
        let outcome = tokio::time::timeout(DEADLINE_GUARD, work)
            .await
            .expect("the work ends at its deadline, before the guard");
        let (table, after) = match outcome {
            Err(Error::LoadDeadline { table, after }) if kind == ErrorKind::LoadDeadline => {
                (table, after)
            }
            Err(Error::RefreshDeadline { table, after }) if kind == ErrorKind::RefreshDeadline => {
                (table, after)
            }
            other => panic!("expected {kind:?}, got {other:?}"),
        };
        assert_eq!(table, TABLE);
        assert_eq!(after, deadline);
    }

    /// Appends to `fixture` one file `name` in bucket 0 whose keys lie above
    /// every key of `all`, laid out as `layout` says, committed as
    /// `SECOND_SNAPSHOT`; returns the appended keys.
    async fn append_second(
        fixture: &mut Fixture,
        all: &[i64],
        name: &str,
        layout: WriterLayout,
    ) -> Vec<i64> {
        let appended = keys_in_bucket(
            &keys_from(all.last().unwrap() + KEY_STEP, E2E_KEYS),
            bucketing(TWO),
            0,
        );
        let keys = long_keys(&appended);
        fixture
            .add_file(FileSpec {
                name,
                keys: &keys,
                values: values_of(&appended),
                partition: Partition::Bucket(0),
                bounds: Bounds::Exact,
                layout,
            })
            .await;
        fixture
            .commit(
                SECOND_SNAPSHOT,
                Some(FIRST_SNAPSHOT),
                SECOND_SEQUENCE,
                SECOND_VERSION,
            )
            .await;
        appended
    }

    /// Adds to `fixture` a file `dup` in bucket 0 repeating two keys of
    /// `all` that bucket 0's file already holds, committed as `snapshot`
    /// over `parent`; returns the reason a load of it is refused for.
    async fn commit_duplicate(
        fixture: &mut Fixture,
        all: &[i64],
        snapshot: i64,
        parent: i64,
        sequence: i64,
        version: u64,
    ) -> RefusalReason {
        let in_bucket_zero = keys_in_bucket(all, bucketing(TWO), 0);
        fixture
            .add_long_file("dup", &in_bucket_zero[DUP_KEYS], 0)
            .await;
        fixture
            .commit(snapshot, Some(parent), sequence, version)
            .await;
        RefusalReason::DuplicateKeys {
            path: fixture.path_of("dup"),
            key: Key::Long(in_bucket_zero[DUP_KEYS.start]),
        }
    }

    fn present(key: i64) -> Value {
        Value::Present(Bytes::from(value_for(key)))
    }

    /// The snapshot id a `Serving` state carries; any other state fails.
    fn serving_id(table: &Table) -> SnapshotId {
        let TableState::Serving { snapshot_id, .. } = table.state() else {
            panic!("expected Serving, got {:?}", table.state());
        };
        snapshot_id
    }

    /// The snapshots a `Held` state names, serving then refused, and the
    /// refused snapshot's level; any other state fails.
    fn held(table: &Table) -> (SnapshotId, SnapshotId, Level) {
        let TableState::Held {
            serving,
            refused,
            level,
            ..
        } = table.state()
        else {
            panic!("expected Held, got {:?}", table.state());
        };
        (serving, refused, level)
    }

    /// The `Held` state of a table serving `FIRST_SNAPSHOT` whose refresh
    /// to `SECOND_SNAPSHOT` was refused at balanced.
    const HELD_AT_BALANCED: (SnapshotId, SnapshotId, Level) = (
        SnapshotId(FIRST_SNAPSHOT),
        SnapshotId(SECOND_SNAPSHOT),
        Level::Balanced,
    );

    /// The fields of a `Degraded` state; any other state fails.
    fn degraded(table: &Table) -> (Option<SnapshotId>, ErrorKind) {
        let TableState::Degraded {
            serving, reason, ..
        } = table.state()
        else {
            panic!("expected Degraded, got {:?}", table.state());
        };
        (serving, reason)
    }

    /// A location no store serves, met as the table loads, is refused
    /// naming the table, which storage alone cannot.
    #[tokio::test]
    async fn test_load_names_the_table_of_a_location_no_store_serves() {
        let (fixture, _) = two_bucket_table().await;
        let defaults = ServerConfig::default();
        let table = Table::new(
            fixture.config(Residency::Full, None),
            TableSource::Static {
                location: "gs://bucket-a/db/kv".to_string(),
            },
            handles_over(&fixture).await,
            defaults.load_deadline,
            defaults.refresh_deadline,
            defaults.get_deadline,
        );
        let error = table.load().await.unwrap_err();
        assert!(
            matches!(
                &error,
                Error::Location {
                    table: Some(name),
                    source: LocationError {
                        fault: LocationFault::NoStore,
                        ..
                    },
                } if name == TABLE
            ),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn test_get_before_load_is_not_ready() {
        let (fixture, all) = two_bucket_table().await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        assert!(matches!(table.state(), TableState::Loading));
        assert!(table.current().is_none());
        let error = table.get(&Key::Long(all[0])).await.unwrap_err();
        assert!(matches!(error, Error::NotReady { table: ref name } if name == TABLE));
        assert_eq!(fixture.gets(), 0);
    }

    #[tokio::test]
    async fn test_key_type_is_not_ready_before_load_then_the_generations() {
        let (fixture, _) = two_bucket_table().await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        assert!(matches!(
            table.key_type(),
            Err(Error::NotReady { table: ref name }) if name == TABLE
        ));
        table.load().await.unwrap();
        assert_eq!(table.key_type().unwrap(), KeyType::Long);
        assert_eq!(
            table.key_type().unwrap(),
            table.current().unwrap().layout.key_type
        );
    }

    #[tokio::test]
    async fn test_resolve_key_type_reads_the_metadata_alone_and_loads_nothing() {
        let (fixture, _) = two_bucket_table().await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        assert_eq!(table.resolve_key_type().await.unwrap(), KeyType::Long);
        assert_eq!(fixture.gets(), RESOLVE_GETS, "no data file is read");
        assert!(table.current().is_none());
        assert!(matches!(table.state(), TableState::Loading));
    }

    #[tokio::test]
    async fn test_load_serves_the_committed_snapshot() {
        let (fixture, all) = two_bucket_table().await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        table.load().await.unwrap();
        let snapshot_id = serving_id(&table);
        assert_eq!(snapshot_id, SnapshotId(FIRST_SNAPSHOT));
        assert_eq!(table.current().unwrap().snapshot_id, snapshot_id);
        assert_eq!(
            table.get(&Key::Long(all[0])).await.unwrap(),
            Lookup {
                value: present(all[0]),
                snapshot_id,
            }
        );
        assert_eq!(
            table.get(&Key::Long(all[0] + 1)).await.unwrap(),
            Lookup {
                value: Value::Absent,
                snapshot_id,
            }
        );
    }

    /// The daemon spawns every load, poll, and lookup onto a multi-thread
    /// runtime, and the gRPC service runs batches there too, so their
    /// futures are `Send`; `tokio::spawn` refuses one that is not at compile
    /// time.
    #[tokio::test]
    async fn test_load_refresh_get_and_multi_get_are_spawnable() {
        let (fixture, all) = two_bucket_table().await;
        let table = Arc::new(table_over(&fixture, fixture.config(Residency::Full, None)).await);
        let loaded = tokio::spawn({
            let table = Arc::clone(&table);
            async move { table.load().await }
        });
        loaded.await.unwrap().unwrap();
        let polled = tokio::spawn({
            let table = Arc::clone(&table);
            async move { table.refresh().await }
        });
        assert!(!polled.await.unwrap().unwrap(), "nothing moved");
        let batch = tokio::spawn({
            let table = Arc::clone(&table);
            let keys = vec![Key::Long(all[0])];
            async move { table.multi_get(&keys).await }
        });
        assert_eq!(batch.await.unwrap().unwrap().values, [present(all[0])]);
        let key = Key::Long(all[0]);
        let looked_up = tokio::spawn(async move { table.get(&key).await });
        assert_eq!(looked_up.await.unwrap().unwrap().value, present(all[0]));
    }

    #[tokio::test]
    async fn test_get_refuses_a_key_of_another_type_before_any_read() {
        let (fixture, _) = two_bucket_table().await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        table.load().await.unwrap();
        let before = fixture.gets();
        let error = table.get(&Key::Str("k".to_string())).await.unwrap_err();
        assert!(matches!(
            error,
            Error::KeyType {
                expected: KeyType::Long,
                got: KeyType::String,
                ..
            }
        ));
        assert_eq!(fixture.gets(), before);
    }

    #[tokio::test]
    async fn test_multi_get_answers_in_key_order_from_one_snapshot() {
        let (fixture, all) = two_bucket_table().await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        table.load().await.unwrap();
        let keys = [Key::Long(all[0]), Key::Long(all[0] + 1), Key::Long(all[1])];
        assert_eq!(
            table.multi_get(&keys).await.unwrap(),
            MultiLookup {
                values: vec![present(all[0]), Value::Absent, present(all[1])],
                snapshot_id: SnapshotId(FIRST_SNAPSHOT),
            }
        );
    }

    #[tokio::test]
    async fn test_multi_get_runs_at_most_the_fan_out_bound_at_once_in_key_order() {
        fills_the_fan_out_bound_in_key_order().await;
    }

    /// On several workers the batch is split over spawned runs, and the
    /// runs together hold the one bound.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_multi_get_on_several_workers_runs_at_most_the_fan_out_bound_in_key_order() {
        assert!(
            shard::workers() > 1,
            "the batch is split only over several workers"
        );
        fills_the_fan_out_bound_in_key_order().await;
    }

    /// A loaded table of `key_count` keys, one row per page, so every key
    /// is a cold hit on a page of its own and costs a GET no other key
    /// shares; its keys; and the gauge its storage reads through.
    async fn one_row_per_page(key_count: usize) -> (Table, Fixture, Vec<i64>, Arc<Gauged>) {
        let (fixture, all, gauged) = one_row_per_page_fixture(key_count).await;
        let table = table_over(&fixture, lenient(&fixture)).await;
        table.load().await.unwrap();
        (table, fixture, all, gauged)
    }

    /// `fixture`'s table in the Full tier with `strict` off: as the
    /// one-row-per-page tables are served, and wherever a test checks a
    /// refusal or a decode that the level gate would otherwise meet first.
    /// A duplicate key, for one, is only possible across overlapping files,
    /// which strict refuses as `below_level` before the merge that finds
    /// it.
    fn lenient(fixture: &Fixture) -> TableConfig {
        TableConfig {
            strict: false,
            ..fixture.config(Residency::Full, None)
        }
    }

    /// The fixture `one_row_per_page` serves, its keys, and the gauge its
    /// storage reads through.
    async fn one_row_per_page_fixture(key_count: usize) -> (Fixture, Vec<i64>, Arc<Gauged>) {
        let all = keys_from(0, i64::try_from(key_count).unwrap());
        let keys = long_keys(&all);
        let mut fixture = Fixture::bucketed(1);
        fixture
            .add_file(FileSpec {
                name: "f",
                keys: &keys,
                values: values_of(&all),
                partition: Partition::Bucket(0),
                bounds: Bounds::Exact,
                layout: WriterLayout {
                    page_rows: 1,
                    ..WriterLayout::contract()
                },
            })
            .await;
        fixture
            .commit(FIRST_SNAPSHOT, None, FIRST_SEQUENCE, FIRST_VERSION)
            .await;
        let gauged = Arc::new(Gauged::new(Arc::new(LocalFileSystem::new())));
        fixture
            .storage
            .register(FILE_PREFIX, Arc::<Gauged>::clone(&gauged));
        (fixture, all, gauged)
    }

    /// A batch of cold hits, each on a page of its own, reaches exactly
    /// `MULTI_GET_FAN_OUT` GETs in flight at once and answers in key order.
    async fn fills_the_fan_out_bound_in_key_order() {
        let batches_of_the_bound = 3;
        let (table, _fixture, all, gauged) =
            one_row_per_page(MULTI_GET_FAN_OUT * batches_of_the_bound).await;
        let keys = long_keys(&all);
        // A batch that never has the bound's lookups in flight at once
        // never passes the gate: its page fetches end at the get deadline
        // and the call fails.
        gauged.fill_to(MULTI_GET_FAN_OUT);
        let answered = table.multi_get(&keys).await.unwrap();
        let expected: Vec<Value> = all.iter().map(|key| present(*key)).collect();
        assert_eq!(answered.values, expected);
        assert_eq!(
            gauged.high_water(),
            MULTI_GET_FAN_OUT,
            "the batch fills the bound and never passes it"
        );
    }

    /// Workers of the runtime the split tests run on: enough that a batch
    /// splits into several runs, and that some calls in flight still leave
    /// it room to.
    const SPLIT_WORKERS: usize = 4;

    /// The name of the second table over a fixture, beside `TABLE`.
    const OTHER_TABLE: &str = "other";

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_multi_get_splits_while_fewer_calls_than_workers_are_in_flight() {
        assert_eq!(shard::workers(), SPLIT_WORKERS);
        let idle = 0;
        assert_eq!(runs_spawned_beside(idle).await, SPLIT_WORKERS);
        let others_leaving_room = SPLIT_WORKERS - 2;
        assert_eq!(
            runs_spawned_beside(others_leaving_room).await,
            SPLIT_WORKERS,
            "with the batch, one fewer call in flight than workers"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_multi_get_runs_inline_once_as_many_calls_as_workers_are_in_flight() {
        assert_eq!(shard::workers(), SPLIT_WORKERS);
        let others_filling_the_workers = SPLIT_WORKERS - 1;
        assert_eq!(runs_spawned_beside(others_filling_the_workers).await, 0);
        let others_past_the_workers = SPLIT_WORKERS;
        assert_eq!(runs_spawned_beside(others_past_the_workers).await, 0);
    }

    /// Calls held in flight on one table hold a batch on another back from
    /// splitting when the two share their handles, as `open_with`'s tables
    /// do: every table's calls run on the same workers.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_multi_get_counts_the_calls_in_flight_on_every_table_sharing_its_handles() {
        assert_eq!(shard::workers(), SPLIT_WORKERS);
        let others_leaving_room = SPLIT_WORKERS - 2;
        assert_eq!(
            runs_spawned_beside_another_table(others_leaving_room).await,
            SPLIT_WORKERS,
            "with the batch, one fewer call in flight than workers"
        );
        let others_filling_the_workers = SPLIT_WORKERS - 1;
        assert_eq!(
            runs_spawned_beside_another_table(others_filling_the_workers).await,
            0
        );
    }

    /// The runs a batch of `MULTI_GET_FAN_OUT` cold hits on one table
    /// spawns while `others` cold-hit `get`s are in flight on another table
    /// over the same handles and file, each on a page of its own. The store
    /// holds every GET of both, so the `get`s stay in flight while the
    /// batch starts; each spawned run holds the batch's generation's `Arc`,
    /// so the holders beyond the batch table's own, the batch's, and this
    /// read's are the runs.
    async fn runs_spawned_beside_another_table(others: usize) -> usize {
        let holders_of_an_inline_batch = 3;
        let (fixture, all, gauged) = one_row_per_page_fixture(MULTI_GET_FAN_OUT + others).await;
        let handles = handles_over(&fixture).await;
        let defaults = ServerConfig::default();
        let batch_table = table_sharing(
            &fixture,
            lenient(&fixture),
            handles.clone(),
            defaults.load_deadline,
            defaults.refresh_deadline,
        );
        let other = Arc::new(table_sharing(
            &fixture,
            TableConfig {
                name: OTHER_TABLE.to_string(),
                ..lenient(&fixture)
            },
            handles,
            defaults.load_deadline,
            defaults.refresh_deadline,
        ));
        batch_table.load().await.unwrap();
        other.load().await.unwrap();
        let (batch, beside) = all.split_at(MULTI_GET_FAN_OUT);
        let keys = long_keys(batch);
        // One GET per key and no hedge, since neither table has recorded a
        // lookup GET to take a delay from, so one more GET than both ask
        // is never reached and the store holds every one until the test
        // drops them.
        gauged.fill_to(MULTI_GET_FAN_OUT + others + 1);
        let mut held = JoinSet::new();
        for key in long_keys(beside) {
            let other = Arc::clone(&other);
            held.spawn(async move { other.get(&key).await });
        }
        // Before the fetches end at the get deadline, which would end the
        // calls.
        let reach = |gets: usize| {
            let gauged = Arc::clone(&gauged);
            tokio::time::timeout(ServerConfig::default().get_deadline, async move {
                while gauged.high_water() < gets {
                    tokio::task::yield_now().await;
                }
            })
        };
        reach(others)
            .await
            .expect("every get on the other table reaches the store");
        let every_get_held = async {
            reach(others + MULTI_GET_FAN_OUT)
                .await
                .expect("every lookup of the batch reaches the store");
            Arc::strong_count(&batch_table.current().unwrap()) - holders_of_an_inline_batch
        };
        tokio::select! {
            answered = batch_table.multi_get(&keys) => panic!("the store holds every GET: {answered:?}"),
            spawned = every_get_held => spawned,
        }
    }

    /// The runs a batch of `MULTI_GET_FAN_OUT` cold hits spawns while
    /// `others` calls are counted in flight on its table, which the test
    /// holds directly rather than running them. Read while the store holds
    /// every GET of the batch: each spawned run holds the generation's
    /// `Arc`, so the holders beyond the table's own, the batch's, and this
    /// read's are the runs.
    async fn runs_spawned_beside(others: usize) -> usize {
        let holders_of_an_inline_batch = 3;
        let (table, _fixture, all, gauged) = one_row_per_page(MULTI_GET_FAN_OUT).await;
        let keys = long_keys(&all);
        let _others: Vec<InFlight<'_>> = (0..others).map(|_| table.lookups.enter()).collect();
        // The batch issues one GET per key and no hedge, since its table
        // has recorded no lookup GET to take a delay from, so one more GET
        // than it has keys is never reached and the store holds every one
        // until the batch is dropped.
        gauged.fill_to(MULTI_GET_FAN_OUT + 1);
        let every_get_held = async {
            // Before the fetches end at the get deadline, which would end
            // the runs.
            tokio::time::timeout(ServerConfig::default().get_deadline, async {
                while gauged.high_water() < MULTI_GET_FAN_OUT {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("every lookup of the batch reaches the store");
            Arc::strong_count(&table.current().unwrap()) - holders_of_an_inline_batch
        };
        tokio::select! {
            answered = table.multi_get(&keys) => panic!("the store holds every GET: {answered:?}"),
            spawned = every_get_held => spawned,
        }
    }

    #[tokio::test]
    async fn test_multi_get_refuses_a_mistyped_key_anywhere_and_issues_no_get() {
        let (fixture, all) = two_bucket_table().await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        table.load().await.unwrap();
        let before = fixture.gets();
        let keys = [Key::Long(all[2]), Key::Str("k".to_string())];
        let error = table.multi_get(&keys).await.unwrap_err();
        assert!(matches!(
            error,
            Error::KeyType {
                expected: KeyType::Long,
                got: KeyType::String,
                ..
            }
        ));
        assert_eq!(fixture.gets(), before);
    }

    #[tokio::test]
    async fn test_load_refused_leaves_the_table_degraded_with_no_generation() {
        let (fixture, all) = two_bucket_table().await;
        let table = table_over(&fixture, fixture.config(Residency::Full, Some(TINY_BUDGET))).await;
        let error = table.load().await.unwrap_err();
        let Error::Refused {
            reason: reason @ RefusalReason::OverBudget { .. },
            ..
        } = error
        else {
            panic!("expected an over-budget refusal, got {error:?}");
        };
        let (serving, kept) = degraded(&table);
        assert_eq!(serving, None);
        assert_eq!(kept, ErrorKind::Refused(reason));
        assert!(table.current().is_none());
        let error = table.get(&Key::Long(all[0])).await.unwrap_err();
        assert!(matches!(error, Error::NotReady { .. }));
    }

    #[tokio::test]
    async fn test_load_strict_refuses_a_snapshot_below_its_level_and_only_that() {
        let balanced = WriterLayout {
            dictionary: Dictionary::Value,
            ..WriterLayout::contract()
        };
        let fixture = one_file_fixture(balanced).await;
        let strict = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        assert!(strict.config().strict);
        let reason = refusal_of(strict.load().await);
        assert_eq!(reason, dictionary_below_optimal());
        assert!(strict.current().is_none());
        let (serving, kept) = degraded(&strict);
        assert_eq!(serving, None);
        assert_eq!(kept, ErrorKind::Refused(reason));

        let accepting = table_over(
            &fixture,
            TableConfig {
                lookups: Some(Level::Balanced),
                ..fixture.config(Residency::Full, None)
            },
        )
        .await;
        accepting.load().await.unwrap();
        assert_eq!(serving_id(&accepting), SnapshotId(FIRST_SNAPSHOT));

        let lenient = table_over(&fixture, lenient(&fixture)).await;
        lenient.load().await.unwrap();
        assert_eq!(serving_id(&lenient), SnapshotId(FIRST_SNAPSHOT));

        let advice_only = WriterLayout {
            bloom: Bloom::Off,
            ..WriterLayout::contract()
        };
        let fixture = one_file_fixture(advice_only).await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        table.load().await.unwrap();
        assert_eq!(serving_id(&table), SnapshotId(FIRST_SNAPSHOT));
    }

    #[tokio::test]
    async fn test_load_fails_at_the_load_deadline_and_releases_the_lock() {
        let (fixture, _) = two_bucket_table().await;
        stall_reads(&fixture);
        let table = table_within(
            &fixture,
            fixture.config(Residency::Full, None),
            TEST_LOAD_DEADLINE,
            TEST_REFRESH_DEADLINE,
        )
        .await;
        ends_at_deadline(table.load(), ErrorKind::LoadDeadline, TEST_LOAD_DEADLINE).await;
        assert_eq!(degraded(&table), (None, ErrorKind::LoadDeadline));
        // The load dropped its lock with its work: the next poll takes the
        // lock and, with no generation serving, builds from nothing under
        // the load deadline too.
        ends_at_deadline(table.refresh(), ErrorKind::LoadDeadline, TEST_LOAD_DEADLINE).await;
        assert_eq!(degraded(&table), (None, ErrorKind::LoadDeadline));
    }

    #[tokio::test]
    async fn test_refresh_fails_at_the_refresh_deadline_and_keeps_serving() {
        let (fixture, all) = two_bucket_table().await;
        let table = table_within(
            &fixture,
            fixture.config(Residency::Full, None),
            TEST_LOAD_DEADLINE,
            TEST_REFRESH_DEADLINE,
        )
        .await;
        table.load().await.unwrap();
        stall_reads(&fixture);
        ends_at_deadline(
            table.refresh(),
            ErrorKind::RefreshDeadline,
            TEST_REFRESH_DEADLINE,
        )
        .await;
        let serving = Some(SnapshotId(FIRST_SNAPSHOT));
        assert_eq!(degraded(&table), (serving, ErrorKind::RefreshDeadline));
        assert_eq!(
            table.get(&Key::Long(all[0])).await.unwrap().value,
            present(all[0]),
            "the generation loaded before the stall keeps answering"
        );
        ends_at_deadline(
            table.refresh(),
            ErrorKind::RefreshDeadline,
            TEST_REFRESH_DEADLINE,
        )
        .await;
        assert_eq!(degraded(&table), (serving, ErrorKind::RefreshDeadline));
    }

    #[tokio::test]
    async fn test_refresh_unchanged_is_false_at_one_get() {
        let (fixture, _) = two_bucket_table().await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        table.load().await.unwrap();
        let before = fixture.gets();
        assert!(!table.refresh().await.unwrap());
        assert_eq!(fixture.gets(), before + POLL_GETS);
        assert_eq!(serving_id(&table), SnapshotId(FIRST_SNAPSHOT));
        assert_eq!(
            table.current().unwrap().snapshot_id,
            SnapshotId(FIRST_SNAPSHOT)
        );
    }

    #[tokio::test]
    async fn test_refresh_swaps_in_a_commit_reusing_unchanged_files() {
        let (mut fixture, all) = two_bucket_table().await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        table.load().await.unwrap();
        let first = table.current().unwrap();
        let appended = append_second(&mut fixture, &all, "f2", WriterLayout::contract()).await;
        assert!(table.refresh().await.unwrap());
        let second = table.current().unwrap();
        assert_eq!(second.snapshot_id, SnapshotId(SECOND_SNAPSHOT));
        assert_eq!(second.files.len(), first.files.len() + 1);
        for old in &first.files {
            assert!(
                second.files.iter().any(|file| Arc::ptr_eq(file, old)),
                "{} is reused by pointer",
                old.entry.path
            );
        }
        assert_eq!(serving_id(&table), SnapshotId(SECOND_SNAPSHOT));
        assert_eq!(
            table.get(&Key::Long(appended[0])).await.unwrap(),
            Lookup {
                value: present(appended[0]),
                snapshot_id: SnapshotId(SECOND_SNAPSHOT),
            }
        );
    }

    #[tokio::test]
    async fn test_refresh_refused_keeps_the_previous_generation_then_recovers() {
        let (mut fixture, all) = two_bucket_table().await;
        let table = table_over(&fixture, lenient(&fixture)).await;
        table.load().await.unwrap();
        let reason = commit_duplicate(
            &mut fixture,
            &all,
            SECOND_SNAPSHOT,
            FIRST_SNAPSHOT,
            SECOND_SEQUENCE,
            SECOND_VERSION,
        )
        .await;
        let error = table.refresh().await.unwrap_err();
        assert!(matches!(error, Error::Refused { reason: ref got, .. } if *got == reason));
        assert_eq!(
            table.current().unwrap().snapshot_id,
            SnapshotId(FIRST_SNAPSHOT)
        );
        assert_eq!(
            table.get(&Key::Long(all[0])).await.unwrap(),
            Lookup {
                value: present(all[0]),
                snapshot_id: SnapshotId(FIRST_SNAPSHOT),
            }
        );
        let (serving, kept) = degraded(&table);
        assert_eq!(serving, Some(SnapshotId(FIRST_SNAPSHOT)));
        assert_eq!(kept, ErrorKind::Refused(reason.clone()));

        fixture.remove_file("dup");
        fixture
            .commit(
                THIRD_SNAPSHOT,
                Some(SECOND_SNAPSHOT),
                THIRD_SEQUENCE,
                THIRD_VERSION,
            )
            .await;
        assert!(table.refresh().await.unwrap());
        assert_eq!(serving_id(&table), SnapshotId(THIRD_SNAPSHOT));
        assert_eq!(
            table.current().unwrap().snapshot_id,
            SnapshotId(THIRD_SNAPSHOT)
        );
    }

    /// A refresh onto a new data file that cannot be read fails transiently,
    /// and the next poll builds the snapshot again rather than remembering
    /// the failure; once the file reads, the snapshot is swapped in.
    #[tokio::test]
    async fn test_refresh_tries_a_transient_failure_again_on_the_next_poll() {
        let (mut fixture, all) = two_bucket_table().await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        table.load().await.unwrap();
        append_second(&mut fixture, &all, "f2", WriterLayout::contract()).await;
        let location = fixture.path_of("f2");
        let on_disk = location.strip_prefix(FILE_PREFIX).unwrap();
        let written = std::fs::read(on_disk).unwrap();
        std::fs::remove_file(on_disk).unwrap();

        let unreadable = |error: &Error| {
            matches!(
                error,
                Error::ObjectStore(object_store::Error::NotFound { .. })
            )
        };
        let error = table.refresh().await.unwrap_err();
        assert!(unreadable(&error), "{error:?}");
        assert_eq!(error.recurrence(), Recurrence::Transient);
        let before = fixture.gets();
        let error = table.refresh().await.unwrap_err();
        assert!(unreadable(&error), "{error:?}");
        assert!(
            fixture.gets() > before + POLL_GETS,
            "the poll read the snapshot's metadata and manifests again"
        );
        assert_eq!(
            degraded(&table),
            (Some(SnapshotId(FIRST_SNAPSHOT)), ErrorKind::ObjectStore)
        );

        std::fs::write(on_disk, written).unwrap();
        assert!(table.refresh().await.unwrap());
        assert_eq!(serving_id(&table), SnapshotId(SECOND_SNAPSHOT));
    }

    /// A key chunk changed on disk under a Disk-tier table is a file fault
    /// of the local directory's copy, not of the data file: the overlap
    /// check meets it reading the chunk back, the next poll builds the
    /// snapshot again rather than remembering the fault, and once the chunk
    /// file holds the data file's bytes again the snapshot is swapped in.
    #[tokio::test]
    async fn test_refresh_tries_a_file_fault_of_a_local_chunk_file_again_on_the_next_poll() {
        let mut fixture = Fixture::bucketed(1);
        let first = keys_from(0, FILE_KEYS);
        fixture.add_long_file("f0", &first, 0).await;
        fixture
            .commit(FIRST_SNAPSHOT, None, FIRST_SEQUENCE, FIRST_VERSION)
            .await;
        let scratch = tempfile::tempdir().unwrap();
        let local = local_dir(scratch.path(), fixture.storage.stats(), None);
        let handles = TableHandles {
            local: Some(Arc::clone(&local)),
            ..handles_over(&fixture).await
        };
        // Overlapping files cap the level at permissive, which strict refuses.
        let config = TableConfig {
            residency: Residency::Disk,
            ..lenient(&fixture)
        };
        let defaults = ServerConfig::default();
        let table = table_sharing(
            &fixture,
            config,
            handles,
            defaults.load_deadline,
            defaults.refresh_deadline,
        );
        table.load().await.unwrap();
        let identity = table
            .current()
            .unwrap()
            .identities()
            .into_iter()
            .next()
            .unwrap();
        let chunk_file = local.chunk_path(&identity);
        let at = chunks_offset(&chunk_file);
        let held = std::fs::read(&chunk_file).unwrap()[approx::length(at)..].to_vec();
        let more_than_the_file_holds = i32::try_from(FILE_KEYS + 1).unwrap();
        overwrite(
            &chunk_file,
            at,
            &with_first_page_claiming(&held, more_than_the_file_holds),
        );
        let interleaved: Vec<i64> = first.iter().map(|key| key + 1).collect();
        fixture.add_long_file("f1", &interleaved, 0).await;
        fixture
            .commit(
                SECOND_SNAPSHOT,
                Some(FIRST_SNAPSHOT),
                SECOND_SEQUENCE,
                SECOND_VERSION,
            )
            .await;

        let changed = fixture.path_of("f0");
        let of_the_copy = |error: &Error| {
            matches!(
                error,
                Error::File {
                    path,
                    fault: FileFault::PageRowsMismatch { .. },
                } if *path == changed
            )
        };
        let error = table.refresh().await.unwrap_err();
        assert!(of_the_copy(&error), "{error:?}");
        assert_eq!(error.recurrence(), Recurrence::Transient);
        let before = fixture.gets();
        let error = table.refresh().await.unwrap_err();
        assert!(of_the_copy(&error), "{error:?}");
        assert!(
            fixture.gets() > before + POLL_GETS,
            "the poll read the snapshot's metadata and manifests again"
        );
        assert_eq!(
            degraded(&table),
            (Some(SnapshotId(FIRST_SNAPSHOT)), ErrorKind::File)
        );

        overwrite(&chunk_file, at, &held);
        assert!(table.refresh().await.unwrap());
        assert_eq!(serving_id(&table), SnapshotId(SECOND_SNAPSHOT));
    }

    /// String keys for `ids`, each written `repeats` times in a row.
    fn string_keys(ids: Range<usize>, repeats: usize) -> Vec<Key> {
        ids.flat_map(|id| std::iter::repeat_n(Key::Str(format!("key:{id:06}")), repeats))
            .collect()
    }

    /// An unpartitioned contract-layout file of `keys` with `dictionary`
    /// encoding, the value at each position `value_for` of it.
    fn string_file<'a>(name: &'a str, keys: &'a [Key], dictionary: Dictionary) -> FileSpec<'a> {
        FileSpec {
            name,
            keys,
            values: (0..keys.len())
                .map(|at| Some(value_for(i64::try_from(at).unwrap())))
                .collect(),
            partition: Partition::None,
            bounds: Bounds::Exact,
            layout: WriterLayout {
                dictionary,
                ..WriterLayout::contract()
            },
        }
    }

    /// A refresh onto a file parquet panics decoding, a string key
    /// dictionary counting one entry more than its body holds, fails with
    /// `FileFault::DecoderPanicked` instead of panicking the task that runs
    /// it, and the previous generation keeps serving.
    #[tokio::test]
    async fn test_refresh_onto_a_file_parquet_panics_decoding_keeps_serving() {
        let file_keys = usize::try_from(FILE_KEYS).unwrap();
        let mut fixture = Fixture::new(KeyType::String, manifest_testing::unpartitioned());
        let served = string_keys(0..file_keys, 1);
        fixture
            .add_file(string_file("f", &served, Dictionary::Off))
            .await;
        fixture
            .commit(FIRST_SNAPSHOT, None, FIRST_SEQUENCE, FIRST_VERSION)
            .await;
        // Two unbucketed files cap the table at permissive, which strict
        // refuses as below_level before any key page is decoded.
        let table = table_over(&fixture, lenient(&fixture)).await;
        table.load().await.unwrap();

        let repeats = 2;
        let repeated = string_keys(file_keys..2 * file_keys, repeats);
        let short = string_file("short", &repeated, Dictionary::Key);
        let written = parquet(KeyType::String, short.keys, &short.values, &short.layout);
        fixture.add_file(short).await;
        let key_leaf = 0;
        let (patched, _) = with_dictionary_one_entry_short(&written, key_leaf);
        let path = fixture.path_of("short");
        manifest_testing::write_file(&FileIO::new_with_fs(), &path, patched).await;
        fixture
            .commit(
                SECOND_SNAPSHOT,
                Some(FIRST_SNAPSHOT),
                SECOND_SEQUENCE,
                SECOND_VERSION,
            )
            .await;

        let error = table.refresh().await.unwrap_err();
        assert!(
            matches!(
                error,
                Error::File {
                    path: ref at,
                    fault: FileFault::DecoderPanicked {
                        part: DecodedPart::Pages { row_group: 0, .. },
                        ..
                    },
                } if *at == path
            ),
            "{error:?}"
        );
        assert_eq!(
            table.current().unwrap().snapshot_id,
            SnapshotId(FIRST_SNAPSHOT)
        );
        let (serving, kept) = degraded(&table);
        assert_eq!(serving, Some(SnapshotId(FIRST_SNAPSHOT)));
        assert_eq!(kept, ErrorKind::File);
        let first = served[0].clone();
        assert_eq!(
            table.get(&first).await.unwrap(),
            Lookup {
                value: present(0),
                snapshot_id: SnapshotId(FIRST_SNAPSHOT),
            }
        );
    }

    /// The refusal a strict table declared `optimal` meets on a snapshot
    /// whose value column is dictionary-encoded.
    fn dictionary_below_optimal() -> RefusalReason {
        RefusalReason::BelowLevel {
            declared: Level::Optimal,
            achieved: Level::Balanced,
            findings: vec![FindingCode::DictionaryEncodedValue],
        }
    }

    /// Appends to `fixture` a file `dict` in bucket 0 whose value column is
    /// dictionary-encoded and whose keys interleave with bucket 0's file's,
    /// committed as `SECOND_SNAPSHOT`.
    async fn append_dictionary_overlap(fixture: &mut Fixture) {
        add_dictionary_overlap(fixture).await;
        fixture
            .commit(
                SECOND_SNAPSHOT,
                Some(FIRST_SNAPSHOT),
                SECOND_SEQUENCE,
                SECOND_VERSION,
            )
            .await;
    }

    /// Records in `fixture` the file `append_dictionary_overlap` commits,
    /// for the next snapshot.
    async fn add_dictionary_overlap(fixture: &mut Fixture) {
        let interleaved = keys_in_bucket(&keys_from(1, E2E_KEYS), bucketing(TWO), 0);
        let keys = long_keys(&interleaved);
        fixture
            .add_file(FileSpec {
                name: "dict",
                keys: &keys,
                values: values_of(&interleaved),
                partition: Partition::Bucket(0),
                bounds: Bounds::Exact,
                layout: WriterLayout {
                    dictionary: Dictionary::Value,
                    ..WriterLayout::contract()
                },
            })
            .await;
    }

    /// A footer cap that already puts a strict table below its level does
    /// not leave overlap unread: the refusal is at the level the snapshot
    /// achieves and names every code below the declared level.
    #[tokio::test]
    async fn test_refresh_below_level_names_overlap_beside_a_footer_cap() {
        let (mut fixture, _) = two_bucket_table().await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        table.load().await.unwrap();
        append_dictionary_overlap(&mut fixture).await;
        assert_eq!(
            refusal_of(table.refresh().await),
            RefusalReason::BelowLevel {
                declared: Level::Optimal,
                achieved: Level::Permissive,
                findings: vec![
                    FindingCode::DictionaryEncodedValue,
                    FindingCode::OverlappingFiles
                ],
            }
        );
        assert_eq!(
            held(&table),
            (
                SnapshotId(FIRST_SNAPSHOT),
                SnapshotId(SECOND_SNAPSHOT),
                Level::Permissive
            )
        );
    }

    /// A strict Disk table below its level under a local directory too
    /// small for its chunk files is refused for its level, not its budget:
    /// the level is judged at the footer stage, before the budget is
    /// reserved, and the refusal writes nothing.
    #[tokio::test]
    async fn test_level_gate_runs_before_the_index_budget_is_reserved() {
        let fixture = one_file_fixture(WriterLayout {
            dictionary: Dictionary::Value,
            ..WriterLayout::contract()
        })
        .await;
        let scratch = tempfile::tempdir().unwrap();
        let local = local_dir(scratch.path(), fixture.storage.stats(), Some(TINY_BUDGET));
        let handles = TableHandles {
            local: Some(Arc::clone(&local)),
            ..handles_over(&fixture).await
        };
        let defaults = ServerConfig::default();
        let strict = table_sharing(
            &fixture,
            fixture.config(Residency::Disk, None),
            handles,
            defaults.load_deadline,
            defaults.refresh_deadline,
        );
        assert!(strict.config().strict);
        assert_eq!(refusal_of(strict.load().await), dictionary_below_optimal());
        assert_eq!(
            local.index_bytes_in_use().unwrap(),
            0,
            "nothing was written"
        );

        let lenient = table_sharing(
            &fixture,
            TableConfig {
                residency: Residency::Disk,
                ..lenient(&fixture)
            },
            TableHandles {
                local: Some(Arc::clone(&local)),
                ..handles_over(&fixture).await
            },
            defaults.load_deadline,
            defaults.refresh_deadline,
        );
        assert!(
            matches!(
                refusal_of(lenient.load().await),
                RefusalReason::OverBudget {
                    of: Budget::Index,
                    ..
                }
            ),
            "the budget refuses it once the level does not"
        );
    }

    #[tokio::test]
    async fn test_refresh_below_level_under_strict_holds_the_last_snapshot_at_level() {
        let (mut fixture, all) = two_bucket_table().await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        table.load().await.unwrap();
        let balanced = WriterLayout {
            dictionary: Dictionary::Value,
            ..WriterLayout::contract()
        };
        append_second(&mut fixture, &all, "dict", balanced).await;
        let reason = refusal_of(table.refresh().await);
        assert_eq!(reason, dictionary_below_optimal());
        assert_eq!(
            table.current().unwrap().snapshot_id,
            SnapshotId(FIRST_SNAPSHOT)
        );
        assert_eq!(
            table.get(&Key::Long(all[0])).await.unwrap().value,
            present(all[0])
        );
        assert_eq!(held(&table), HELD_AT_BALANCED);
    }

    /// A transient failure of a poll while held makes the table `Degraded`,
    /// and the next poll that finds the source unchanged makes it `Held`
    /// for the remembered refusal again, building nothing.
    #[tokio::test]
    async fn test_refresh_transient_fault_while_held_reads_degraded_until_an_unchanged_poll() {
        let (mut fixture, all) = two_bucket_table().await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        table.load().await.unwrap();
        let balanced = WriterLayout {
            dictionary: Dictionary::Value,
            ..WriterLayout::contract()
        };
        append_second(&mut fixture, &all, "dict", balanced).await;
        table.refresh().await.unwrap_err();
        assert_eq!(held(&table), HELD_AT_BALANCED);

        fixture.remove_hint();
        let error = table.refresh().await.unwrap_err();
        assert!(matches!(error, Error::ObjectStore(_)), "{error:?}");
        let (serving, reason) = degraded(&table);
        assert_eq!(serving, Some(SnapshotId(FIRST_SNAPSHOT)));
        assert_eq!(reason, ErrorKind::ObjectStore);

        fixture.restore_hint(SECOND_VERSION).await;
        let before = fixture.gets();
        assert!(!table.refresh().await.unwrap());
        assert_eq!(fixture.gets(), before + POLL_GETS, "the version hint alone");
        assert_eq!(held(&table), HELD_AT_BALANCED);
    }

    /// A first load below its level has no snapshot to hold: the table is
    /// `Degraded` with nothing serving, which `serve` exits on.
    #[tokio::test]
    async fn test_load_below_level_with_nothing_serving_is_degraded() {
        let fixture = one_file_fixture(WriterLayout {
            dictionary: Dictionary::Value,
            ..WriterLayout::contract()
        })
        .await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        let reason = refusal_of(table.load().await);
        assert_eq!(degraded(&table), (None, ErrorKind::Refused(reason)));
    }

    /// A `load` over a serving generation that meets a snapshot below its
    /// level holds it as a poll does, naming the snapshot the load refused.
    #[tokio::test]
    async fn test_load_below_level_over_a_serving_generation_is_held() {
        let (mut fixture, all) = two_bucket_table().await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        table.load().await.unwrap();
        let balanced = WriterLayout {
            dictionary: Dictionary::Value,
            ..WriterLayout::contract()
        };
        append_second(&mut fixture, &all, "dict", balanced).await;
        assert_eq!(refusal_of(table.load().await), dictionary_below_optimal());
        assert_eq!(held(&table), HELD_AT_BALANCED);
    }

    /// When a `Held` state began; any other state fails.
    fn held_since(table: &Table) -> Instant {
        let TableState::Held { since, .. } = table.state() else {
            panic!("expected Held, got {:?}", table.state());
        };
        since
    }

    /// A held table refused below its level again for the snapshot it
    /// holds, by a `load` here, stays held from the first refusal.
    #[tokio::test]
    async fn test_load_below_level_again_for_the_held_snapshot_keeps_its_since() {
        let (mut fixture, all) = two_bucket_table().await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        table.load().await.unwrap();
        let balanced = WriterLayout {
            dictionary: Dictionary::Value,
            ..WriterLayout::contract()
        };
        append_second(&mut fixture, &all, "dict", balanced).await;
        table.refresh().await.unwrap_err();
        let first = held_since(&table);

        assert_eq!(refusal_of(table.load().await), dictionary_below_optimal());
        assert_eq!(held(&table), HELD_AT_BALANCED);
        assert_eq!(held_since(&table), first);
    }

    /// A refresh onto a file whose keys hash to bucket 0 but whose manifest
    /// records bucket 1 is refused as `KeyOutsideBucket`, and the previous
    /// generation keeps serving.
    #[tokio::test]
    async fn test_refresh_onto_a_file_outside_its_bucket_keeps_serving() {
        let (mut fixture, all) = two_bucket_table().await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        table.load().await.unwrap();
        let (hashed, recorded) = (0, 1);
        let appended = keys_in_bucket(
            &keys_from(all.last().unwrap() + KEY_STEP, E2E_KEYS),
            bucketing(TWO),
            hashed,
        );
        fixture
            .add_long_file("misplaced", &appended, recorded)
            .await;
        fixture
            .commit(
                SECOND_SNAPSHOT,
                Some(FIRST_SNAPSHOT),
                SECOND_SEQUENCE,
                SECOND_VERSION,
            )
            .await;
        let error = table.refresh().await.unwrap_err();
        let expected = RefusalReason::KeyOutsideBucket {
            path: fixture.path_of("misplaced"),
            key: Key::Long(appended[0]),
            recorded,
            computed: hashed,
        };
        assert!(
            matches!(error, Error::Refused { ref reason, .. } if *reason == expected),
            "{error:?}"
        );
        assert_eq!(
            table.current().unwrap().snapshot_id,
            SnapshotId(FIRST_SNAPSHOT)
        );
        let (serving, kept) = degraded(&table);
        assert_eq!(serving, Some(SnapshotId(FIRST_SNAPSHOT)));
        assert_eq!(kept, ErrorKind::Refused(expected));
        assert_eq!(
            table.get(&Key::Long(all[0])).await.unwrap().value,
            present(all[0])
        );
    }

    #[tokio::test]
    async fn test_refresh_concurrent_polls_swap_once() {
        let (mut fixture, all) = two_bucket_table().await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        table.load().await.unwrap();
        append_second(&mut fixture, &all, "f2", WriterLayout::contract()).await;
        let (a, b) = tokio::join!(table.refresh(), table.refresh());
        let outcomes = [a.unwrap(), b.unwrap()];
        assert_eq!(outcomes.iter().filter(|swapped| **swapped).count(), 1);
        assert_eq!(
            table.current().unwrap().snapshot_id,
            SnapshotId(SECOND_SNAPSHOT)
        );
    }

    /// The series a debugging recorder captured, looked up by name and
    /// exact label set.
    struct Series(Vec<(CompositeKey, Option<Unit>, Option<SharedString>, DebugValue)>);

    impl Series {
        fn find(&self, name: &str, labels: &[(&str, String)]) -> &DebugValue {
            let found: Vec<&DebugValue> = self
                .0
                .iter()
                .filter(|(key, ..)| labeled(key.key(), name, labels))
                .map(|(.., value)| value)
                .collect();
            assert_eq!(found.len(), 1, "{name} {labels:?}");
            found[0]
        }

        fn counter(&self, name: &str, labels: &[(&str, String)]) -> u64 {
            match self.find(name, labels) {
                DebugValue::Counter(value) => *value,
                DebugValue::Gauge(_) | DebugValue::Histogram(_) => panic!("{name} is a counter"),
            }
        }

        fn gauge(&self, name: &str, labels: &[(&str, String)]) -> f64 {
            match self.find(name, labels) {
                DebugValue::Gauge(value) => value.into_inner(),
                DebugValue::Counter(_) | DebugValue::Histogram(_) => panic!("{name} is a gauge"),
            }
        }

        /// The series named `name`, of any labels.
        fn named(&self, name: &str) -> usize {
            self.0
                .iter()
                .filter(|(key, ..)| key.key().name() == name)
                .count()
        }

        /// The series, of any name, that carry `label`.
        fn labeled_with(&self, label: &str) -> usize {
            self.0
                .iter()
                .filter(|(key, ..)| key.key().labels().any(|have| have.key() == label))
                .count()
        }

        fn samples(&self, name: &str, labels: &[(&str, String)]) -> usize {
            match self.find(name, labels) {
                DebugValue::Histogram(values) => values.len(),
                DebugValue::Counter(_) | DebugValue::Gauge(_) => panic!("{name} is a histogram"),
            }
        }
    }

    /// Whether `key` is `name` with exactly `labels`.
    fn labeled(key: &MetricKey, name: &str, labels: &[(&str, String)]) -> bool {
        key.name() == name
            && key.labels().count() == labels.len()
            && labels.iter().all(|(label, value)| {
                key.labels()
                    .any(|have| have.key() == *label && have.value() == value)
            })
    }

    fn table_label() -> (&'static str, String) {
        (metric::LABEL_TABLE, TABLE.to_string())
    }

    /// The label no generation metric carries: a series per snapshot would
    /// grow with every swap.
    const LABEL_SNAPSHOT_ID: &str = "snapshot_id";

    #[test]
    fn test_metrics_follow_lookups_swaps_and_failures() {
        let started = time::unix_seconds(time::now());
        let (reason, series) = recorded(async {
            let (mut fixture, all) = two_bucket_table().await;
            let table = table_over(&fixture, lenient(&fixture)).await;
            table.load().await.unwrap();
            table.get(&Key::Long(all[0])).await.unwrap();
            table.get(&Key::Long(all[0])).await.unwrap();
            table.get(&Key::Long(all[0] + 1)).await.unwrap();
            append_second(&mut fixture, &all, "f2", WriterLayout::contract()).await;
            assert!(table.refresh().await.unwrap());
            let reason = commit_duplicate(
                &mut fixture,
                &all,
                THIRD_SNAPSHOT,
                SECOND_SNAPSHOT,
                THIRD_SEQUENCE,
                THIRD_VERSION,
            )
            .await;
            assert!(table.refresh().await.is_err());
            reason
        });

        for result in [
            LookupResult::ColdHit,
            LookupResult::WarmHit,
            LookupResult::Miss,
        ] {
            let labels = [table_label(), (metric::LABEL_RESULT, result.to_string())];
            assert_eq!(
                series.counter(metric::LOOKUPS_TOTAL, &labels),
                1,
                "{result}"
            );
            assert_eq!(
                series.samples(metric::LOOKUP_SECONDS, &labels),
                1,
                "{result}"
            );
        }
        assert_eq!(
            series.counter(metric::GENERATION_SWAPS_TOTAL, &[table_label()]),
            1
        );
        assert_eq!(
            series
                .gauge(metric::GENERATION_SEQUENCE_NUMBER, &[table_label()])
                .to_bits(),
            approx::signed_gauge(SECOND_SEQUENCE).to_bits(),
            "the swapped-in snapshot's, not the refused one's"
        );
        let swapped_at = series.gauge(metric::GENERATION_SWAPPED_AT_SECONDS, &[table_label()]);
        assert!(
            (started..=time::unix_seconds(time::now())).contains(&swapped_at),
            "{swapped_at} is not between the test's start and now"
        );
        assert!(series.labeled_with(metric::LABEL_TABLE) > 0);
        assert_eq!(series.labeled_with(LABEL_SNAPSHOT_ID), 0);
        assert_eq!(
            series.counter(
                metric::REFRESH_ERRORS_TOTAL,
                &[table_label(), (metric::LABEL_REASON, reason.to_string())]
            ),
            1
        );
        let state = |name: StateName| state_gauge(&series, TABLE, name);
        assert_eq!(
            state(StateName::Degraded).to_bits(),
            approx::gauge(1).to_bits()
        );
        assert_eq!(
            state(StateName::Serving).to_bits(),
            approx::gauge(0).to_bits()
        );
        assert_eq!(
            state(StateName::Loading).to_bits(),
            approx::gauge(0).to_bits()
        );
    }

    /// Lookups spread over a multi-thread runtime's workers reach
    /// `lookups_total` at `publish_lookup_counts`, each once: a snapshot of
    /// the debugging recorder takes the counter's value and resets it, so
    /// each snapshot reads what was published since the one before.
    #[test]
    fn test_publish_lookup_counts_moves_every_task_count_once() {
        let workers = 4;
        let lookups_per_task = 8;
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(workers)
            .enable_all()
            .build()
            .unwrap();
        let labels = [
            table_label(),
            (metric::LABEL_RESULT, LookupResult::Miss.to_string()),
        ];
        let published =
            || Series(snapshotter.snapshot().into_vec()).counter(metric::LOOKUPS_TOTAL, &labels);
        metrics::with_local_recorder(&recorder, || {
            runtime.block_on(async {
                let (fixture, all) = two_bucket_table().await;
                let table =
                    Arc::new(table_over(&fixture, fixture.config(Residency::Full, None)).await);
                table.load().await.unwrap();
                let absent = Key::Long(all[0] + 1);
                let tasks: Vec<_> = (0..workers)
                    .map(|_| {
                        let table = Arc::clone(&table);
                        let absent = absent.clone();
                        tokio::spawn(async move {
                            for _ in 0..lookups_per_task {
                                assert_eq!(table.get(&absent).await.unwrap().value, Value::Absent);
                            }
                        })
                    })
                    .collect();
                for task in tasks {
                    task.await.unwrap();
                }
                assert_eq!(published(), 0, "counted on the shards until published");
                table.publish_lookup_counts();
                assert_eq!(published(), approx::count(workers * lookups_per_task));
                table.publish_lookup_counts();
                assert_eq!(published(), 0, "nothing is published twice");
                table.get(&absent).await.unwrap();
                drop(table);
                assert_eq!(published(), 1, "a dropped table publishes what it counted");
            });
        });
    }

    /// A debugging recorder's snapshot of what `body` emitted, run on a
    /// current-thread runtime, since the local recorder is thread-local.
    fn recorded<T>(body: impl Future<Output = T>) -> (T, Series) {
        recorded_with(|_| body)
    }

    /// `recorded`, with `body` given a snapshotter of the same recorder, to
    /// read the series while it runs. A snapshot zeroes every series it
    /// reads, so a later one holds only what was written after it.
    fn recorded_with<T, F: Future<Output = T>>(body: impl FnOnce(Snapshotter) -> F) -> (T, Series) {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let out = metrics::with_local_recorder(&recorder, || {
            runtime.block_on(body(recorder.snapshotter()))
        });
        (out, Series(snapshotter.snapshot().into_vec()))
    }

    /// The `table_state` gauge of `name` for the table `table`.
    fn state_gauge(series: &Series, table: &str, name: StateName) -> f64 {
        series.gauge(
            metric::TABLE_STATE,
            &[
                (metric::LABEL_TABLE, table.to_string()),
                (metric::LABEL_STATE, name.to_string()),
            ],
        )
    }

    /// The level gauges of `table`, as bits: below the declared level, the
    /// level achieved, and the level declared.
    fn level_gauges(series: &Series) -> [u64; 3] {
        let labels = [table_label()];
        [
            metric::TABLE_BELOW_LEVEL,
            metric::TABLE_LEVEL,
            metric::TABLE_DECLARED_LEVEL,
        ]
        .map(|name| series.gauge(name, &labels).to_bits())
    }

    /// What `level_gauges` reads for `below`, 1 or 0, at `achieved` against
    /// `declared`.
    fn level_bits(below: u64, achieved: Level, declared: Level) -> [u64; 3] {
        [
            approx::gauge(below).to_bits(),
            achieved.gauge().to_bits(),
            declared.gauge().to_bits(),
        ]
    }

    #[test]
    fn test_metrics_level_series_appear_at_first_swap() {
        let ((), before) = recorded(async {
            let (fixture, _) = two_bucket_table().await;
            let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
            fixture.remove_hint();
            table.load().await.unwrap_err();
        });
        for name in [
            metric::TABLE_BELOW_LEVEL,
            metric::TABLE_LEVEL,
            metric::TABLE_DECLARED_LEVEL,
        ] {
            assert_eq!(before.named(name), 0, "{name} before any swap");
        }
        let ((), after) = recorded(async {
            let (fixture, _) = two_bucket_table().await;
            let config = TableConfig {
                lookups: Some(Level::Balanced),
                ..fixture.config(Residency::Full, None)
            };
            table_over(&fixture, config).await.load().await.unwrap();
        });
        assert_eq!(
            level_gauges(&after),
            level_bits(0, Level::Optimal, Level::Balanced)
        );
    }

    /// The lookup-cost gauges of `table`, as bits: GETs per hit, GETs per
    /// miss, and files probed per lookup.
    fn cost_gauges(series: &Series) -> [u64; 3] {
        let labels = [table_label()];
        [
            metric::TABLE_GETS_PER_HIT,
            metric::TABLE_GETS_PER_MISS,
            metric::TABLE_FILES_PROBED_PER_LOOKUP,
        ]
        .map(|name| series.gauge(name, &labels).to_bits())
    }

    /// What `cost_gauges` reads for `cost`.
    fn cost_bits(cost: LookupCost) -> [u64; 3] {
        [
            approx::gauge(cost.gets_per_hit).to_bits(),
            approx::gauge(cost.gets_per_miss).to_bits(),
            approx::gauge_len(cost.files_probed).to_bits(),
        ]
    }

    /// The cost of a cold lookup on `two_bucket_table` in `full`.
    const CONTRACT_COST: LookupCost = LookupCost {
        gets_per_hit: 1,
        gets_per_miss: 0,
        files_probed: 1,
    };

    /// The cost on `two_bucket_table` after `append_dictionary_overlap`, in
    /// `full`: the value dictionary page beside the value page, and the
    /// two files of bucket 0 probed.
    const DICTIONARY_OVERLAP_COST: LookupCost = LookupCost {
        gets_per_hit: 2,
        gets_per_miss: 0,
        files_probed: 2,
    };

    /// The lookup-cost gauges appear with the first swap and follow the
    /// serving generation: a swap to a layout with overlapping files and a
    /// dictionary-encoded value column raises them.
    #[test]
    fn test_metrics_lookup_cost_appears_at_first_swap_and_follows_the_swaps() {
        let ((), before) = recorded(async {
            let (fixture, _) = two_bucket_table().await;
            let table = table_over(&fixture, lenient(&fixture)).await;
            fixture.remove_hint();
            table.load().await.unwrap_err();
        });
        for name in [
            metric::TABLE_GETS_PER_HIT,
            metric::TABLE_GETS_PER_MISS,
            metric::TABLE_FILES_PROBED_PER_LOOKUP,
        ] {
            assert_eq!(before.named(name), 0, "{name} before any swap");
        }
        let (first, after) = recorded_with(|snapshotter| async move {
            let (mut fixture, _) = two_bucket_table().await;
            let table = table_over(&fixture, lenient(&fixture)).await;
            table.load().await.unwrap();
            assert_eq!(table.current().unwrap().lookup_cost, CONTRACT_COST);
            let first = Series(snapshotter.snapshot().into_vec());
            append_dictionary_overlap(&mut fixture).await;
            assert!(table.refresh().await.unwrap());
            assert_eq!(
                table.current().unwrap().lookup_cost,
                DICTIONARY_OVERLAP_COST
            );
            first
        });
        assert_eq!(cost_gauges(&first), cost_bits(CONTRACT_COST));
        assert_eq!(cost_gauges(&after), cost_bits(DICTIONARY_OVERLAP_COST));
    }

    /// A strict table held on a snapshot that costs more keeps the serving
    /// generation's cost: the gauges say what lookups pay now.
    #[test]
    fn test_metrics_lookup_cost_is_the_serving_generations_while_held() {
        let ((), series) = recorded(async {
            let (mut fixture, _) = two_bucket_table().await;
            let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
            table.load().await.unwrap();
            append_dictionary_overlap(&mut fixture).await;
            table.refresh().await.unwrap_err();
            assert_eq!(
                held(&table),
                (
                    SnapshotId(FIRST_SNAPSHOT),
                    SnapshotId(SECOND_SNAPSHOT),
                    Level::Permissive
                )
            );
            table.publish_gauges();
        });
        assert_eq!(cost_gauges(&series), cost_bits(CONTRACT_COST));
        assert_eq!(
            level_gauges(&series),
            level_bits(1, Level::Permissive, Level::Optimal),
            "the level gauges judge the held snapshot"
        );
    }

    /// The rows of `two_bucket_table`'s bucket 0 file that `commit_deletes`
    /// deletes.
    const F0_DELETED: [i64; 2] = [0, 1];
    /// The rows of its bucket 1 file that `commit_deletes` deletes.
    const F1_DELETED: [i64; 1] = [0];
    /// The position delete files of `commit_deletes` that apply.
    const APPLYING_DELETE_FILES: u64 = 2;

    /// Commits to `two_bucket_table`'s `fixture`, as `SECOND_SNAPSHOT`, a
    /// position delete file per data file deleting `F0_DELETED` and
    /// `F1_DELETED`, and a dangling one naming a file the table never held.
    async fn commit_deletes(fixture: &mut Fixture) {
        fixture
            .add_position_delete("d0", "f0", &F0_DELETED, 0)
            .await;
        fixture
            .add_position_delete("d1", "f1", &F1_DELETED, 1)
            .await;
        fixture
            .add_position_delete("dangling", "gone", &F1_DELETED, 1)
            .await;
        fixture
            .commit(
                SECOND_SNAPSHOT,
                Some(FIRST_SNAPSHOT),
                SECOND_SEQUENCE,
                SECOND_VERSION,
            )
            .await;
    }

    /// The delete gauges of `table`, as bits: deleted rows, deleted
    /// fraction, delete files, and delete state bytes.
    fn delete_gauges(series: &Series) -> [u64; 4] {
        let labels = [table_label()];
        DELETE_GAUGES.map(|name| series.gauge(name, &labels).to_bits())
    }

    /// The gauges a swap writes from `Generation::delete_summary`.
    const DELETE_GAUGES: [&str; 4] = [
        metric::TABLE_DELETED_ROWS,
        metric::TABLE_DELETED_FRACTION,
        metric::TABLE_DELETE_FILES,
        metric::TABLE_DELETE_STATE_BYTES,
    ];

    /// What `delete_gauges` reads for `summary`.
    fn delete_bits(summary: DeleteSummary) -> [u64; 4] {
        [
            approx::gauge(summary.rows).to_bits(),
            summary.fraction.to_bits(),
            approx::gauge(summary.files).to_bits(),
            approx::gauge_len(summary.state_bytes).to_bits(),
        ]
    }

    /// The delete summary of `generation` is what `commit_deletes` applies:
    /// its rows, of the snapshot's rows, from the two files that name a
    /// data file the snapshot holds and not the dangling one, holding the
    /// bytes `index_bytes` counts past the file indexes.
    fn assert_commit_deletes_applied(generation: &Generation) -> DeleteSummary {
        let summary = generation.delete_summary();
        let deleted = approx::count(F0_DELETED.len() + F1_DELETED.len());
        assert_eq!(summary.rows, deleted);
        assert_eq!(summary.files, APPLYING_DELETE_FILES);
        assert_eq!(
            summary.fraction.to_bits(),
            approx::ratio(deleted, generation.records).to_bits()
        );
        let file_bytes: usize = generation
            .files
            .iter()
            .map(|file| file.index.resident_bytes())
            .sum();
        assert!(summary.state_bytes > 0);
        assert_eq!(generation.index_bytes, file_bytes + summary.state_bytes);
        summary
    }

    /// The delete gauges appear with the first swap, at zero on a snapshot
    /// with no deletes, and follow the swaps: a snapshot with deletes
    /// writes what its applying deletes come to, a dangling delete counted
    /// in none.
    #[test]
    fn test_metrics_delete_gauges_appear_at_first_swap_and_count_applying_deletes() {
        let ((), before) = recorded(async {
            let (fixture, _) = two_bucket_table().await;
            let table = table_over(&fixture, lenient(&fixture)).await;
            fixture.remove_hint();
            table.load().await.unwrap_err();
        });
        for name in DELETE_GAUGES {
            assert_eq!(before.named(name), 0, "{name} before any swap");
        }
        let ((first, summary), after) = recorded_with(|snapshotter| async move {
            let (mut fixture, _) = two_bucket_table().await;
            let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
            table.load().await.unwrap();
            let generation = table.current().unwrap();
            assert_eq!(generation.delete_summary(), DeleteSummary::default());
            assert_eq!(generation.oldest_delete_committed, None);
            let first = Series(snapshotter.snapshot().into_vec());
            commit_deletes(&mut fixture).await;
            assert!(table.refresh().await.unwrap());
            let summary = assert_commit_deletes_applied(&table.current().unwrap());
            (first, summary)
        });
        assert_eq!(delete_gauges(&first), delete_bits(DeleteSummary::default()));
        assert_eq!(delete_gauges(&after), delete_bits(summary));
    }

    /// A strict table held on a snapshot below its level keeps the serving
    /// generation's delete gauges, though the held snapshot deletes more.
    #[test]
    fn test_metrics_delete_gauges_are_the_serving_generations_while_held() {
        let (summary, series) = recorded(async {
            let (mut fixture, _) = two_bucket_table().await;
            commit_deletes(&mut fixture).await;
            let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
            table.load().await.unwrap();
            let summary = assert_commit_deletes_applied(&table.current().unwrap());
            add_dictionary_overlap(&mut fixture).await;
            fixture.add_position_delete("d2", "f1", &[1], 1).await;
            fixture
                .commit(
                    THIRD_SNAPSHOT,
                    Some(SECOND_SNAPSHOT),
                    THIRD_SEQUENCE,
                    THIRD_VERSION,
                )
                .await;
            table.refresh().await.unwrap_err();
            assert_eq!(
                held(&table),
                (
                    SnapshotId(SECOND_SNAPSHOT),
                    SnapshotId(THIRD_SNAPSHOT),
                    Level::Permissive
                )
            );
            table.publish_gauges();
            summary
        });
        assert_eq!(delete_gauges(&series), delete_bits(summary));
    }

    /// The oldest applying delete is dated by the snapshot that committed
    /// it, found by its data sequence number; once that snapshot has
    /// expired, by the earliest later snapshot, never by an older one that
    /// predates the delete.
    #[tokio::test]
    async fn test_load_dates_the_oldest_delete_by_its_snapshot_or_the_next_listed() {
        let (mut fixture, _) = two_bucket_table().await;
        commit_deletes(&mut fixture).await;
        fixture
            .commit(
                THIRD_SNAPSHOT,
                Some(SECOND_SNAPSHOT),
                THIRD_SEQUENCE,
                THIRD_VERSION,
            )
            .await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        table.load().await.unwrap();
        let generation = table.current().unwrap();
        assert_eq!(generation.snapshot_id, SnapshotId(THIRD_SNAPSHOT));
        assert_eq!(
            generation.oldest_delete_committed,
            Some(fixture.committed(SECOND_SNAPSHOT))
        );

        let expired_version = THIRD_VERSION + 1;
        fixture.expire(SECOND_SNAPSHOT, expired_version).await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        table.load().await.unwrap();
        let generation = table.current().unwrap();
        assert_commit_deletes_applied(&generation);
        assert_ne!(
            fixture.committed(FIRST_SNAPSHOT),
            fixture.committed(THIRD_SNAPSHOT),
            "the older snapshot would date the delete otherwise"
        );
        assert_eq!(
            generation.oldest_delete_committed,
            Some(fixture.committed(THIRD_SNAPSHOT))
        );
    }

    /// A sample of `PER_MILLE` pages, `resident` of them in the page
    /// cache, so a resident count reads as thousandths.
    fn sample_at(resident: u64) -> ResidentPages {
        ResidentPages {
            pages: PER_MILLE,
            resident,
        }
    }

    /// A sample just short of the page cache: one page below the fall.
    fn short_sample() -> ResidentPages {
        sample_at(FALL_PER_MILLE - 1)
    }

    /// `short_sample`'s fraction as the gauge reads it.
    fn short_fraction() -> u64 {
        approx::ratio(FALL_PER_MILLE - 1, PER_MILLE).to_bits()
    }

    /// `config` in the Disk tier over `fixture`, its chunk files in a fresh
    /// local directory that the returned guard keeps, in `Loading`.
    async fn disk_table(fixture: &Fixture, config: TableConfig) -> (Table, tempfile::TempDir) {
        let scratch = tempfile::tempdir().unwrap();
        let local = local_dir(scratch.path(), fixture.storage.stats(), None);
        let handles = TableHandles {
            local: Some(local),
            ..handles_over(fixture).await
        };
        let defaults = ServerConfig::default();
        let table = table_sharing(
            fixture,
            TableConfig {
                residency: Residency::Disk,
                ..config
            },
            handles,
            defaults.load_deadline,
            defaults.refresh_deadline,
        );
        (table, scratch)
    }

    /// Takes `sample` as the page cache's condition of `table`'s serving
    /// generation.
    fn observe(table: &Table, sample: &std::result::Result<ResidentPages, Unobserved>) {
        table.observe_page_cache(&table.current().unwrap(), sample);
    }

    /// The resident fraction gauge of `TABLE`, as bits.
    fn fraction_gauge(series: &Series) -> u64 {
        series
            .gauge(metric::TABLE_CHUNK_RESIDENT_FRACTION, &[table_label()])
            .to_bits()
    }

    /// A disk table whose chunk files fall short of the page cache is below
    /// optimal for it, its layout's level notwithstanding, and recovers once
    /// they are back; the resident fraction is the sample's.
    #[test]
    fn test_metrics_a_disk_table_short_of_the_page_cache_is_below_optimal() {
        let (short, recovered) = recorded_with(|snapshotter| async move {
            let (fixture, _) = two_bucket_table().await;
            let (table, _scratch) = disk_table(&fixture, lenient(&fixture)).await;
            table.load().await.unwrap();
            observe(&table, &Ok(short_sample()));
            let short = Series(snapshotter.snapshot().into_vec());
            observe(&table, &Ok(sample_at(PER_MILLE)));
            assert_eq!(table.current().unwrap().level, Level::Optimal);
            short
        });
        assert_eq!(
            level_gauges(&short),
            level_bits(1, Level::Balanced, Level::Optimal)
        );
        assert_eq!(fraction_gauge(&short), short_fraction());
        assert_eq!(
            level_gauges(&recovered),
            level_bits(0, Level::Optimal, Level::Optimal)
        );
        assert_eq!(fraction_gauge(&recovered), 1.0_f64.to_bits());
    }

    /// A table that declares balanced is at its level with its chunk files
    /// short of the page cache: device reads of key chunks are a balanced
    /// cost.
    #[test]
    fn test_metrics_a_balanced_disk_table_short_of_the_page_cache_stays_at_its_level() {
        let ((), series) = recorded(async {
            let (fixture, _) = two_bucket_table().await;
            let config = TableConfig {
                lookups: Some(Level::Balanced),
                ..lenient(&fixture)
            };
            let (table, _scratch) = disk_table(&fixture, config).await;
            table.load().await.unwrap();
            observe(&table, &Ok(short_sample()));
        });
        assert_eq!(
            level_gauges(&series),
            level_bits(0, Level::Balanced, Level::Balanced)
        );
    }

    /// `strict` never acts on the page cache: a strict table short of it
    /// stays serving, a refresh swaps in the next snapshot, and the swap
    /// and a retune both keep the condition until the next sample.
    #[test]
    fn test_refresh_of_a_strict_disk_table_short_of_the_page_cache_swaps_and_keeps_it() {
        let (swapped, retuned) = recorded_with(|snapshotter| async move {
            let (mut fixture, all) = two_bucket_table().await;
            let strict = fixture.config(Residency::Disk, None);
            assert!(strict.strict);
            let (table, _scratch) = disk_table(&fixture, strict.clone()).await;
            table.load().await.unwrap();
            observe(&table, &Ok(short_sample()));
            assert_eq!(serving_id(&table), SnapshotId(FIRST_SNAPSHOT));
            append_second(&mut fixture, &all, "f2", WriterLayout::contract()).await;
            assert!(table.refresh().await.unwrap());
            assert_eq!(serving_id(&table), SnapshotId(SECOND_SNAPSHOT));
            let swapped = Series(snapshotter.snapshot().into_vec());
            let serialized = table.serialized().await;
            table.retune(
                TableConfig {
                    lookups: Some(Level::Balanced),
                    ..strict
                },
                &serialized,
            );
            swapped
        });
        assert_eq!(
            level_gauges(&swapped),
            level_bits(1, Level::Balanced, Level::Optimal)
        );
        assert_eq!(fraction_gauge(&swapped), short_fraction());
        assert_eq!(
            level_gauges(&retuned),
            level_bits(0, Level::Balanced, Level::Balanced)
        );
        assert_eq!(fraction_gauge(&retuned), short_fraction());
    }

    /// A sample of a generation swapped out since it began describes chunk
    /// files the table no longer serves from, and is dropped.
    #[test]
    fn test_observe_page_cache_drops_a_sample_of_a_generation_swapped_out() {
        let ((), series) = recorded(async {
            let (mut fixture, all) = two_bucket_table().await;
            let (table, _scratch) = disk_table(&fixture, lenient(&fixture)).await;
            table.load().await.unwrap();
            let stale = table.current().unwrap();
            append_second(&mut fixture, &all, "f2", WriterLayout::contract()).await;
            assert!(table.refresh().await.unwrap());
            table.observe_page_cache(&stale, &Ok(short_sample()));
            table.publish_gauges();
        });
        assert_eq!(
            level_gauges(&series),
            level_bits(0, Level::Optimal, Level::Optimal)
        );
        assert_eq!(series.named(metric::TABLE_CHUNK_RESIDENT_FRACTION), 0);
    }

    /// A sample that cannot observe a chunk file makes the condition
    /// unjudged: the cap is lifted, and the resident fraction is no longer
    /// written. A debugging snapshot sets every gauge it reads to zero, so
    /// the fraction reads zero after one only when nothing wrote it since.
    #[test]
    fn test_metrics_an_unobserved_sample_lifts_the_cap_and_stops_the_fraction() {
        let (short, unjudged) = recorded_with(|snapshotter| async move {
            let (fixture, _) = two_bucket_table().await;
            let (table, _scratch) = disk_table(&fixture, lenient(&fixture)).await;
            table.load().await.unwrap();
            observe(&table, &Ok(short_sample()));
            let short = Series(snapshotter.snapshot().into_vec());
            let unobserved = Err(Unobserved {
                path: std::path::PathBuf::from("chunk"),
                why: std::io::Error::from(std::io::ErrorKind::PermissionDenied).into(),
            });
            observe(&table, &unobserved);
            table.publish_gauges();
            short
        });
        assert_eq!(
            level_gauges(&short),
            level_bits(1, Level::Balanced, Level::Optimal)
        );
        assert_eq!(
            level_gauges(&unjudged),
            level_bits(0, Level::Optimal, Level::Optimal)
        );
        assert_eq!(fraction_gauge(&unjudged), 0.0_f64.to_bits());
    }

    /// A disk table just loaded has just written its chunk files, so the
    /// page cache holds them whole and the table is at its layout's level;
    /// a full table has no chunk file and is never judged.
    #[test]
    fn test_sample_page_cache_reads_a_just_loaded_disk_table_whole_and_skips_a_full_one() {
        let ((), series) = recorded(async {
            let (fixture, _) = two_bucket_table().await;
            let (table, _scratch) = disk_table(&fixture, lenient(&fixture)).await;
            table.load().await.unwrap();
            sample_page_cache(&[&table]).await;
            let full = table_over(
                &fixture,
                TableConfig {
                    name: STRICT_TABLE.to_string(),
                    ..lenient(&fixture)
                },
            )
            .await;
            full.load().await.unwrap();
            sample_page_cache(&[&full]).await;
        });
        assert_eq!(
            level_gauges(&series),
            level_bits(0, Level::Optimal, Level::Optimal)
        );
        assert_eq!(fraction_gauge(&series), 1.0_f64.to_bits());
        assert_eq!(series.named(metric::TABLE_CHUNK_RESIDENT_FRACTION), 1);
    }

    /// Evicting every chunk file of a disk table puts it below optimal at
    /// the next sample, and reading them back recovers it.
    #[cfg(target_os = "linux")]
    #[test]
    fn test_sample_page_cache_follows_chunk_files_evicted_and_read_back() {
        let (evicted, read_back) = recorded_with(|snapshotter| async move {
            let (fixture, _) = two_bucket_table().await;
            let (table, scratch) = disk_table(&fixture, lenient(&fixture)).await;
            table.load().await.unwrap();
            let local = local_dir(scratch.path(), fixture.storage.stats(), None);
            let paths: Vec<_> = table
                .current()
                .unwrap()
                .identities()
                .iter()
                .map(|identity| local.chunk_path(identity))
                .collect();
            for path in &paths {
                evict(path);
            }
            sample_page_cache(&[&table]).await;
            let evicted = Series(snapshotter.snapshot().into_vec());
            for path in &paths {
                std::fs::read(path).unwrap();
            }
            sample_page_cache(&[&table]).await;
            evicted
        });
        assert_eq!(
            level_gauges(&evicted),
            level_bits(1, Level::Balanced, Level::Optimal)
        );
        assert_eq!(fraction_gauge(&evicted), 0.0_f64.to_bits());
        assert_eq!(
            level_gauges(&read_back),
            level_bits(0, Level::Optimal, Level::Optimal)
        );
        assert_eq!(fraction_gauge(&read_back), 1.0_f64.to_bits());
    }

    /// A retune of a lenient table's `lookups` declares the new level at
    /// once, before any poll: a table serving a balanced snapshot reads
    /// below its declared `optimal`, and retuned to `balanced` reads at its
    /// level.
    #[test]
    fn test_metrics_retune_declares_the_new_level_at_once() {
        let (before, after) = recorded_with(|snapshotter| async move {
            let fixture = one_file_fixture(WriterLayout {
                dictionary: Dictionary::Value,
                ..WriterLayout::contract()
            })
            .await;
            let table = table_over(&fixture, lenient(&fixture)).await;
            table.load().await.unwrap();
            let before = Series(snapshotter.snapshot().into_vec());
            let serialized = table.serialized().await;
            table.retune(
                TableConfig {
                    lookups: Some(Level::Balanced),
                    ..lenient(&fixture)
                },
                &serialized,
            );
            before
        });
        assert_eq!(
            level_gauges(&before),
            level_bits(1, Level::Balanced, Level::Optimal)
        );
        assert_eq!(
            level_gauges(&after),
            level_bits(0, Level::Balanced, Level::Balanced)
        );
    }

    /// A held table retuned to `strict` off serves on with the held
    /// snapshot's level measured, below its level; once its ref goes back
    /// to the serving snapshot, the poll finds it in step and the level
    /// gauges measure the serving snapshot again, though no state ended.
    #[test]
    fn test_refresh_in_step_after_a_retune_out_of_held_measures_the_serving_snapshot() {
        let (during, after) = recorded_with(|snapshotter| async move {
            let (mut fixture, all) = two_bucket_table().await;
            let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
            table.load().await.unwrap();
            let balanced = WriterLayout {
                dictionary: Dictionary::Value,
                ..WriterLayout::contract()
            };
            append_second(&mut fixture, &all, "dict", balanced).await;
            table.refresh().await.unwrap_err();
            assert_eq!(held(&table), HELD_AT_BALANCED);
            {
                let serialized = table.serialized().await;
                table.retune(lenient(&fixture), &serialized);
            }
            assert_eq!(serving_id(&table), SnapshotId(FIRST_SNAPSHOT));
            let during = Series(snapshotter.snapshot().into_vec());

            fixture.restore_hint(FIRST_VERSION).await;
            assert!(!table.refresh().await.unwrap());
            assert_eq!(serving_id(&table), SnapshotId(FIRST_SNAPSHOT));
            table.publish_gauges();
            during
        });
        assert_eq!(
            level_gauges(&during),
            level_bits(1, Level::Balanced, Level::Optimal)
        );
        assert_eq!(
            level_gauges(&after),
            level_bits(0, Level::Optimal, Level::Optimal)
        );
    }

    #[test]
    fn test_metrics_below_level_judges_the_held_snapshot() {
        let ((), series) = recorded(async {
            let (mut fixture, all) = two_bucket_table().await;
            let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
            table.load().await.unwrap();
            let balanced = WriterLayout {
                dictionary: Dictionary::Value,
                ..WriterLayout::contract()
            };
            append_second(&mut fixture, &all, "dict", balanced).await;
            table.refresh().await.unwrap_err();
            assert_eq!(held(&table), HELD_AT_BALANCED);
        });
        assert_eq!(
            level_gauges(&series),
            level_bits(1, Level::Balanced, Level::Optimal),
            "the held snapshot is below the level, though the serving one meets it"
        );
        assert_eq!(
            state_gauge(&series, TABLE, StateName::Held).to_bits(),
            approx::gauge(1).to_bits()
        );
        assert_eq!(
            state_gauge(&series, TABLE, StateName::Degraded).to_bits(),
            approx::gauge(0).to_bits()
        );
    }

    #[test]
    fn test_metrics_refusal_outside_the_levels_leaves_the_level_series() {
        let ((), series) = recorded(async {
            let (mut fixture, all) = two_bucket_table().await;
            let table = table_over(&fixture, lenient(&fixture)).await;
            table.load().await.unwrap();
            commit_duplicate(
                &mut fixture,
                &all,
                SECOND_SNAPSHOT,
                FIRST_SNAPSHOT,
                SECOND_SEQUENCE,
                SECOND_VERSION,
            )
            .await;
            let reason = refusal_of(table.refresh().await);
            assert!(matches!(reason, RefusalReason::DuplicateKeys { .. }));
        });
        assert_eq!(
            level_gauges(&series),
            level_bits(0, Level::Optimal, Level::Optimal),
            "the refused snapshot overlaps, but only the serving one was measured"
        );
    }

    /// A held table whose ref goes back to the serving snapshot is in step
    /// with it without a swap: it serves, and the level gauges measure the
    /// serving snapshot again.
    #[test]
    fn test_refresh_ref_back_on_the_serving_snapshot_ends_held_at_its_level() {
        let ((), series) = recorded(async {
            let (mut fixture, all) = two_bucket_table().await;
            let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
            table.load().await.unwrap();
            let balanced = WriterLayout {
                dictionary: Dictionary::Value,
                ..WriterLayout::contract()
            };
            append_second(&mut fixture, &all, "dict", balanced).await;
            table.refresh().await.unwrap_err();
            assert_eq!(held(&table), HELD_AT_BALANCED);

            fixture.restore_hint(FIRST_VERSION).await;
            assert!(!table.refresh().await.unwrap());
            assert_eq!(serving_id(&table), SnapshotId(FIRST_SNAPSHOT));
        });
        assert_eq!(
            level_gauges(&series),
            level_bits(0, Level::Optimal, Level::Optimal)
        );
        assert_eq!(
            state_gauge(&series, TABLE, StateName::Serving).to_bits(),
            approx::gauge(1).to_bits()
        );
        assert_eq!(
            state_gauge(&series, TABLE, StateName::Held).to_bits(),
            approx::gauge(0).to_bits()
        );
    }

    #[test]
    fn test_refresh_unchanged_after_a_failed_poll_returns_to_serving() {
        let ((), series) = recorded(async {
            let (fixture, all) = two_bucket_table().await;
            let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
            table.load().await.unwrap();
            fixture.remove_hint();
            let error = table.refresh().await.unwrap_err();
            assert!(matches!(error, Error::ObjectStore(_)), "{error:?}");
            let (serving, kept) = degraded(&table);
            assert_eq!(serving, Some(SnapshotId(FIRST_SNAPSHOT)));
            assert_eq!(kept, ErrorKind::ObjectStore);
            assert_eq!(
                table.get(&Key::Long(all[0])).await.unwrap().value,
                present(all[0])
            );

            fixture.restore_hint(FIRST_VERSION).await;
            let before = fixture.gets();
            assert!(!table.refresh().await.unwrap());
            assert_eq!(fixture.gets(), before + POLL_GETS);
            assert_eq!(serving_id(&table), SnapshotId(FIRST_SNAPSHOT));
        });
        assert_eq!(
            state_gauge(&series, TABLE, StateName::Serving).to_bits(),
            approx::gauge(1).to_bits()
        );
        assert_eq!(
            state_gauge(&series, TABLE, StateName::Degraded).to_bits(),
            approx::gauge(0).to_bits()
        );
        assert_eq!(
            series.counter(
                metric::REFRESH_ERRORS_TOTAL,
                &[
                    table_label(),
                    (metric::LABEL_REASON, ErrorKind::ObjectStore.to_string())
                ]
            ),
            1
        );
    }

    /// A snapshot refused for a delete file icetray cannot read (RFD 021)
    /// is remembered against its metadata location: while the version hint
    /// names that location each poll reads the hint alone, counts no error,
    /// and leaves the table degraded for the refusal, a transient failure
    /// in between included; once the hint names another location, its
    /// snapshot is built.
    #[test]
    fn test_refresh_does_not_build_a_refused_snapshot_again_until_its_location_moves() {
        let deletes = "deletes";
        let serving = Some(SnapshotId(FIRST_SNAPSHOT));
        let (refused, series) = recorded(async {
            let (mut fixture, all) = two_bucket_table().await;
            let refused = ErrorKind::Refused(RefusalReason::DeleteKindUnsupported {
                path: fixture.path_of(deletes),
                kind: UnsupportedDelete::AvroPositions,
            });
            let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
            table.load().await.unwrap();
            fixture.add_avro_position_delete(deletes, 0);
            fixture
                .commit(
                    SECOND_SNAPSHOT,
                    Some(FIRST_SNAPSHOT),
                    SECOND_SEQUENCE,
                    SECOND_VERSION,
                )
                .await;

            let before = fixture.gets();
            let error = table.refresh().await.unwrap_err();
            assert_eq!(error.kind(), refused, "{error:?}");
            assert!(
                fixture.gets() > before + POLL_GETS,
                "the first poll reads the snapshot's metadata and manifests"
            );
            let polls_while_unchanged = 3;
            for _ in 0..polls_while_unchanged {
                let before = fixture.gets();
                assert!(!table.refresh().await.unwrap());
                assert_eq!(fixture.gets(), before + POLL_GETS, "the version hint alone");
                assert_eq!(degraded(&table), (serving, refused.clone()));
            }

            fixture.remove_hint();
            let error = table.refresh().await.unwrap_err();
            assert!(matches!(error, Error::ObjectStore(_)), "{error:?}");
            assert_eq!(degraded(&table), (serving, ErrorKind::ObjectStore));
            fixture.restore_hint(SECOND_VERSION).await;
            let before = fixture.gets();
            assert!(!table.refresh().await.unwrap());
            assert_eq!(fixture.gets(), before + POLL_GETS);
            assert_eq!(degraded(&table), (serving, refused.clone()));
            assert_eq!(
                table.get(&Key::Long(all[0])).await.unwrap(),
                Lookup {
                    value: present(all[0]),
                    snapshot_id: SnapshotId(FIRST_SNAPSHOT),
                }
            );

            fixture.remove_file(deletes);
            fixture
                .commit(
                    THIRD_SNAPSHOT,
                    Some(SECOND_SNAPSHOT),
                    THIRD_SEQUENCE,
                    THIRD_VERSION,
                )
                .await;
            assert!(table.refresh().await.unwrap());
            assert_eq!(serving_id(&table), SnapshotId(THIRD_SNAPSHOT));
            let before = fixture.gets();
            assert!(!table.refresh().await.unwrap());
            assert_eq!(fixture.gets(), before + POLL_GETS);
            refused
        });
        let errors = |reason: &ErrorKind| {
            series.counter(
                metric::REFRESH_ERRORS_TOTAL,
                &[table_label(), (metric::LABEL_REASON, reason.to_string())],
            )
        };
        assert_eq!(errors(&refused), 1, "counted once, when made");
        assert_eq!(errors(&ErrorKind::ObjectStore), 1);
        assert_eq!(
            state_gauge(&series, TABLE, StateName::Serving).to_bits(),
            approx::gauge(1).to_bits()
        );
    }

    /// A snapshot a strict table refuses as below its level is remembered
    /// against its metadata location like any refusal: while the version
    /// hint names that location each poll reads the hint alone, counts no
    /// error, and leaves the table held for the refusal.
    #[test]
    fn test_refresh_does_not_judge_a_snapshot_below_its_level_again_until_its_location_moves() {
        let refused = ErrorKind::Refused(dictionary_below_optimal());
        let ((), series) = recorded(async {
            let (mut fixture, all) = two_bucket_table().await;
            let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
            assert!(table.config().strict);
            table.load().await.unwrap();
            let balanced = WriterLayout {
                dictionary: Dictionary::Value,
                ..WriterLayout::contract()
            };
            append_second(&mut fixture, &all, "dict", balanced).await;

            let before = fixture.gets();
            let error = table.refresh().await.unwrap_err();
            assert_eq!(error.kind(), refused, "{error:?}");
            assert!(
                fixture.gets() > before + POLL_GETS,
                "the first poll reads the snapshot's metadata and footers"
            );
            let polls_while_unchanged = 3;
            for _ in 0..polls_while_unchanged {
                let before = fixture.gets();
                assert!(!table.refresh().await.unwrap());
                assert_eq!(fixture.gets(), before + POLL_GETS, "the version hint alone");
                assert_eq!(held(&table), HELD_AT_BALANCED);
            }
        });
        assert_eq!(
            series.counter(
                metric::REFRESH_ERRORS_TOTAL,
                &[table_label(), (metric::LABEL_REASON, refused.to_string())],
            ),
            1,
            "counted once, when made"
        );
    }

    /// A ref the metadata does not have is a table fault fixed for that
    /// metadata file, and is remembered against its location like a
    /// refusal: while the version hint names it each poll reads the hint
    /// alone, counts no error, and leaves the table degraded for the
    /// fault; once the hint names metadata holding the ref again, its
    /// snapshot is built.
    #[test]
    fn test_refresh_does_not_read_metadata_lacking_the_ref_again_until_its_location_moves() {
        let faulted = ErrorKind::Table;
        let serving = Some(SnapshotId(FIRST_SNAPSHOT));
        let ((), series) = recorded(async {
            let (mut fixture, _) = two_bucket_table().await;
            let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
            table.load().await.unwrap();
            fixture.drop_main(SECOND_VERSION).await;

            let before = fixture.gets();
            let error = table.refresh().await.unwrap_err();
            assert!(
                matches!(
                    error,
                    Error::Table {
                        fault: TableFault::RefNotFound { .. },
                        ..
                    }
                ),
                "{error:?}"
            );
            assert!(
                fixture.gets() > before + POLL_GETS,
                "the first poll reads the metadata file"
            );
            let polls_while_unchanged = 3;
            for _ in 0..polls_while_unchanged {
                let before = fixture.gets();
                assert!(!table.refresh().await.unwrap());
                assert_eq!(fixture.gets(), before + POLL_GETS, "the version hint alone");
                assert_eq!(degraded(&table), (serving, faulted.clone()));
            }

            fixture
                .commit(
                    SECOND_SNAPSHOT,
                    Some(FIRST_SNAPSHOT),
                    SECOND_SEQUENCE,
                    THIRD_VERSION,
                )
                .await;
            assert!(table.refresh().await.unwrap());
            assert_eq!(serving_id(&table), SnapshotId(SECOND_SNAPSHOT));
        });
        let errors = series.counter(
            metric::REFRESH_ERRORS_TOTAL,
            &[table_label(), (metric::LABEL_REASON, faulted.to_string())],
        );
        assert_eq!(errors, 1, "counted once, when met");
    }

    /// The `findings` gauge of `code` for the table `table`.
    fn findings_gauge(series: &Series, table: &str, code: FindingCode) -> f64 {
        series.gauge(
            metric::FINDINGS,
            &[
                (metric::LABEL_TABLE, table.to_string()),
                (metric::LABEL_CODE, code.to_string()),
            ],
        )
    }

    #[test]
    fn test_metrics_findings_and_index_bytes_describe_the_serving_generation() {
        let balanced = WriterLayout {
            dictionary: Dictionary::Value,
            ..WriterLayout::contract()
        };
        let ((lenient_bytes, strict_bytes), series) = recorded(async {
            let fixture = one_file_fixture(balanced).await;
            let lenient = table_over(&fixture, lenient(&fixture)).await;
            lenient.load().await.unwrap();
            let lenient_bytes = lenient.current().unwrap().index_bytes;

            let (mut fixture, all) = two_bucket_table().await;
            let strict = table_over(
                &fixture,
                TableConfig {
                    name: STRICT_TABLE.to_string(),
                    ..fixture.config(Residency::Full, None)
                },
            )
            .await;
            strict.load().await.unwrap();
            let strict_bytes = strict.current().unwrap().index_bytes;
            append_second(&mut fixture, &all, "dict", balanced).await;
            assert!(matches!(
                strict.refresh().await.unwrap_err(),
                Error::Refused {
                    reason: RefusalReason::BelowLevel { .. },
                    ..
                }
            ));
            (lenient_bytes, strict_bytes)
        });
        assert!(lenient_bytes > 0);
        assert_eq!(
            findings_gauge(&series, TABLE, FindingCode::DictionaryEncodedValue).to_bits(),
            approx::gauge(1).to_bits()
        );
        assert_eq!(
            findings_gauge(&series, TABLE, FindingCode::NoBloomFilter).to_bits(),
            approx::gauge(0).to_bits()
        );
        assert_eq!(
            series
                .gauge(metric::INDEX_BYTES, &[table_label()])
                .to_bits(),
            approx::gauge_len(lenient_bytes).to_bits()
        );
        assert_eq!(
            findings_gauge(&series, STRICT_TABLE, FindingCode::DictionaryEncodedValue).to_bits(),
            approx::gauge(0).to_bits(),
            "a refused generation's findings never reach the gauge"
        );
        assert_eq!(
            series
                .gauge(
                    metric::INDEX_BYTES,
                    &[(metric::LABEL_TABLE, STRICT_TABLE.to_string())]
                )
                .to_bits(),
            approx::gauge_len(strict_bytes).to_bits()
        );
    }

    /// A lookup that reads nothing, a warm hit or a Full-tier miss,
    /// completes on its first poll, so the generation guard it holds is
    /// never parked on its thread's fast slots; a cold hit, which waits on
    /// its page's read, holds one across the wait.
    #[tokio::test]
    async fn test_a_lookup_that_reads_nothing_completes_on_its_first_poll() {
        use futures::FutureExt as _;

        let (fixture, all) = two_bucket_table().await;
        // A plain local read can finish on the blocking pool before the poll
        // that started it looks, and let a cold hit be ready at once; reads
        // go through a gate the test closes, so the cold hit waits.
        let gate = Arc::new(Gauged::new(Arc::new(LocalFileSystem::new())));
        fixture
            .storage
            .register(FILE_PREFIX, Arc::clone(&gate) as _);
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        table.load().await.unwrap();
        let key = Key::Long(all[0]);
        gate.fill_to(2);
        assert!(
            table.get(&key).now_or_never().is_none(),
            "a cold hit waits on its page's read"
        );
        gate.fill_to(0);
        table.get(&key).await.unwrap();
        let warm = table.get(&key).now_or_never();
        assert_eq!(
            warm.expect("a warm hit is ready").unwrap().value,
            present(all[0])
        );
        let absent = Key::Long(all.iter().max().unwrap() + 1);
        let miss = table.get(&absent).now_or_never();
        assert_eq!(miss.expect("a miss is ready").unwrap().value, Value::Absent);
    }

    /// A table built hidden over `fixture`, as a reload builds one.
    async fn hidden_over(fixture: &Fixture) -> Table {
        let defaults = ServerConfig::default();
        Table::hidden(
            fixture.config(Residency::Full, None),
            TableSource::Static {
                location: fixture.root(),
            },
            handles_over(fixture).await,
            defaults.load_deadline,
            defaults.refresh_deadline,
            defaults.get_deadline,
        )
    }

    /// A table's counters are registered at zero when it is built, before
    /// any lookup or swap, so each series exists from the start.
    #[test]
    fn test_new_registers_the_table_s_counters_at_zero() {
        let ((), series) = recorded(async {
            let (fixture, _) = two_bucket_table().await;
            table_over(&fixture, fixture.config(Residency::Full, None)).await;
        });
        for result in LookupResult::ALL {
            let labels = [table_label(), (metric::LABEL_RESULT, result.to_string())];
            assert_eq!(
                series.counter(metric::LOOKUPS_TOTAL, &labels),
                0,
                "{result}"
            );
        }
        assert_eq!(
            series.counter(metric::GENERATION_SWAPS_TOTAL, &[table_label()]),
            0
        );
    }

    /// A hidden table writes no gauge while it loads, nor when a scrape
    /// publishes, so the table serving under its name keeps its series;
    /// `show` then publishes its state, its generation's gauges, and the
    /// level gauges as the swap set them.
    #[test]
    fn test_a_hidden_table_writes_its_gauges_once_shown() {
        let (index_bytes, loading) = recorded(async {
            let (fixture, _) = two_bucket_table().await;
            let table = hidden_over(&fixture).await;
            table.load().await.unwrap();
            table.publish_gauges();
            table.current().unwrap().index_bytes
        });
        let gauges = [
            metric::TABLE_STATE,
            metric::INDEX_BYTES,
            metric::FINDINGS,
            metric::GENERATION_SEQUENCE_NUMBER,
            metric::GENERATION_SWAPPED_AT_SECONDS,
            metric::TABLE_BELOW_LEVEL,
            metric::TABLE_LEVEL,
            metric::TABLE_DECLARED_LEVEL,
            metric::TABLE_GETS_PER_HIT,
            metric::TABLE_GETS_PER_MISS,
            metric::TABLE_FILES_PROBED_PER_LOOKUP,
            metric::TABLE_DELETED_ROWS,
            metric::TABLE_DELETED_FRACTION,
            metric::TABLE_DELETE_FILES,
            metric::TABLE_DELETE_STATE_BYTES,
        ];
        for name in gauges {
            assert_eq!(loading.named(name), 0, "{name}");
        }

        let ((), shown) = recorded(async {
            let (fixture, _) = two_bucket_table().await;
            let table = hidden_over(&fixture).await;
            table.load().await.unwrap();
            table.show();
        });
        assert_eq!(
            state_gauge(&shown, TABLE, StateName::Serving).to_bits(),
            approx::gauge(1).to_bits()
        );
        assert_eq!(
            shown.gauge(metric::INDEX_BYTES, &[table_label()]).to_bits(),
            approx::gauge_len(index_bytes).to_bits()
        );
        assert_eq!(
            level_gauges(&shown),
            level_bits(0, Level::Optimal, Level::Optimal)
        );
        assert_eq!(cost_gauges(&shown), cost_bits(CONTRACT_COST));
        assert_eq!(delete_gauges(&shown), delete_bits(DeleteSummary::default()));
    }

    /// A withdrawn table writes no gauge: a refresh that fails after the
    /// withdrawal, which would read `Degraded`, and a scrape's publish
    /// leave its series as the withdrawal found them, for the binary's
    /// recorder to drop.
    #[test]
    fn test_a_withdrawn_table_writes_no_gauge() {
        let ((), series) = recorded(async {
            let (fixture, _) = two_bucket_table().await;
            let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
            table.load().await.unwrap();
            table.withdraw();
            fixture.remove_hint();
            table.refresh().await.unwrap_err();
            table.publish_gauges();
        });
        for name in StateName::ALL {
            let shown = u64::from(name == StateName::Serving);
            assert_eq!(
                state_gauge(&series, TABLE, name).to_bits(),
                approx::gauge(shown).to_bits(),
                "{name}"
            );
        }
    }

    #[test]
    fn test_fmt_table_state_label_values() {
        let cases = [
            (TableState::Loading, "loading"),
            (
                TableState::Serving {
                    snapshot_id: SnapshotId(FIRST_SNAPSHOT),
                    since: Instant::now(),
                },
                "serving",
            ),
            (
                TableState::Degraded {
                    serving: None,
                    reason: ErrorKind::ObjectStore,
                    since: Instant::now(),
                },
                "degraded",
            ),
            (
                TableState::Held {
                    serving: SnapshotId(FIRST_SNAPSHOT),
                    refused: SnapshotId(SECOND_SNAPSHOT),
                    level: Level::Balanced,
                    since: Instant::now(),
                },
                "held",
            ),
        ];
        for (state, name) in cases {
            assert_eq!(state.to_string(), name);
        }
    }

    #[test]
    fn test_table_and_state_are_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Table>();
        assert_send_sync::<TableState>();
    }
}
