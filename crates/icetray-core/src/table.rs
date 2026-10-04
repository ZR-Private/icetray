//! The table lifecycle (RFD 005, RFD 007): a `Table` follows one ref,
//! builds each new snapshot as a `Generation` beside the one it serves,
//! runs the doctor on the new generation and refuses an inefficient one under
//! `strict`, then swaps it in atomically. Loads and refreshes of one table
//! are serialized; a failed one leaves the last generation serving and the
//! table `Degraded`. A lookup takes one generation and answers from it
//! alone, so a swap during a batch cannot mix snapshots.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use arc_swap::ArcSwapOption;
use futures::{StreamExt as _, TryStreamExt as _, stream};
use iceberg::spec::Snapshot;
use metrics::{Counter, Histogram};
use tokio::sync::{MutexGuard, Semaphore};
use tokio::task::JoinSet;

use crate::approx;
use crate::cache::PageCache;
use crate::catalog::{Resolved, TableSource};
use crate::check::{self, Finding, FindingCode, Tier};
use crate::config::TableConfig;
use crate::error::{Error, ErrorKind, Result};
use crate::generation::{Generation, Lookup, MultiLookup, Value};
use crate::key::{Key, KeyType};
use crate::local::LocalDir;
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
/// label: the variant's name alone, `loading`, `serving`, or `degraded`.
#[derive(Clone, Debug)]
pub enum TableState {
    /// Constructed and not yet loaded; nothing has been read.
    Loading,
    /// A generation answers lookups and the last load or refresh succeeded.
    Serving {
        /// The snapshot it serves.
        snapshot_id: SnapshotId,
        /// When the state began: the swap, or the poll that ended a
        /// `Degraded` state by finding the table in step with its ref.
        since: Instant,
    },
    /// The last load or refresh failed. The generation loaded before it, if
    /// any, keeps answering (RFD 005). The state ends at the next swap, or
    /// at the next poll that finds the table in step with its ref.
    Degraded {
        /// The snapshot still serving, or `None` after a failed first load.
        serving: Option<SnapshotId>,
        /// Why the load or refresh failed, as the failure's kind: a refusal
        /// with its reason, the strict gate, a table or file fault, or the
        /// library or local-directory failure's variant. The failure itself
        /// went to the caller of `load` or `refresh`, which logs it; `Error`
        /// is not `Clone`, and the kind is what a reader of the state acts on.
        reason: ErrorKind,
        /// When the failure happened.
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
        }
    }
}

impl fmt::Display for TableState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.name().fmt(f)
    }
}

/// A `TableState`'s name without its fields: the `state` label's values,
/// so the `table_state` gauge is set to zero for the two states a table is
/// not in. Display gives the label value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StateName {
    /// `TableState::Loading`.
    Loading,
    /// `TableState::Serving`.
    Serving,
    /// `TableState::Degraded`.
    Degraded,
}

impl StateName {
    /// Every state, for the gauge's three series.
    const ALL: [Self; 3] = [Self::Loading, Self::Serving, Self::Degraded];
}

impl fmt::Display for StateName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Loading => "loading",
            Self::Serving => "serving",
            Self::Degraded => "degraded",
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
/// from the storage's hedge settings and handed to every `Generation::load`,
/// so one table's delay is set from its own reads and never another's (RFD
/// 013). Shared across tasks behind an `Arc`.
pub struct Table {
    config: TableConfig,
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
    /// The metadata location the table last resolved and admitted, or found
    /// unchanged in snapshot; what `refresh` compares the source against,
    /// so a metadata change that did not move the followed ref does not
    /// make every later poll read the metadata again. A failed build does
    /// not advance it: the next poll reads the metadata again and retries,
    /// so a transient failure recovers and a refusal is counted per poll.
    /// `None` until the first successful resolve.
    known: Mutex<Option<String>>,
    /// Serializes `load` and `refresh`; `Opened::retain` takes every
    /// table's in turn, and nothing else ever takes two.
    serial: tokio::sync::Mutex<()>,
    /// How long a `load` or `refresh` with no generation serving, which
    /// builds from nothing, may hold `serial` doing its work, from
    /// `server.load_deadline`.
    load_deadline: Duration,
    /// How long a `load` or `refresh` over a serving generation may hold
    /// `serial` doing its work, from `server.refresh_deadline`.
    refresh_deadline: Duration,
    lookups: Arc<Lookups>,
    /// What the table's state and generation gauges read, and whether it
    /// shows them. A table a reload loads beside the one serving under its
    /// name shows none until the reload swaps it in, so the serving table's
    /// series describe the serving table (RFD 018). Held while a gauge is
    /// written, so a withdrawal and a publish never interleave; never held
    /// across an `await`.
    gauges: Mutex<Gauges>,
}

/// A table's gauges: whether they are shown, and what its serving
/// generation set them to.
struct Gauges {
    shown: bool,
    generation: Option<GenerationGauges>,
}

/// What a swap sets the generation gauges to, kept so a table shown after
/// its swap publishes them then.
struct GenerationGauges {
    index_bytes: usize,
    findings: BTreeMap<FindingCode, u64>,
    sequence_number: SequenceNumber,
    swapped_at: f64,
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
    /// calls in flight while it runs. The lookup metrics are registered
    /// here, with the recorder current then: a process installs its
    /// recorder first. The table shows its gauges from here on.
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
        } = handles;
        let hedge = Arc::new(HedgeWindow::new(storage.hedge_config()));
        let lookups = Arc::new(Lookups {
            cache: cache.with_fetch_deadline(get_deadline),
            metrics: LookupMetrics::new(&config.name),
            in_flight: calls,
        });
        Self {
            config,
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
            gauges: Mutex::new(Gauges {
                shown: false,
                generation: None,
            }),
        }
    }

    /// Shows the table's gauges from now on and publishes them: its state,
    /// and its serving generation's when it has one.
    pub(crate) fn show(&self) {
        let mut gauges = self.gauges.lock().unwrap_or_else(PoisonError::into_inner);
        gauges.shown = true;
        self.write_state(&gauges);
        if let Some(generation) = &gauges.generation {
            self.write_generation(generation);
        }
    }

    /// Stops showing the table's gauges, for a table a reload removed or
    /// replaced (RFD 018). The recorder cannot remove a series, so the
    /// state series all read 0, saying the table is in none of the states,
    /// and `index_bytes` reads 0, since the index is dropped; the table's
    /// other series keep their last values. A replacement shown after this
    /// writes its own values over them.
    pub(crate) fn withdraw(&self) {
        let mut gauges = self.gauges.lock().unwrap_or_else(PoisonError::into_inner);
        gauges.shown = false;
        for name in StateName::ALL {
            metrics::gauge!(
                metric::TABLE_STATE,
                metric::LABEL_TABLE => self.config.name.clone(),
                metric::LABEL_STATE => name.to_string()
            )
            .set(0.0);
        }
        metrics::gauge!(
            metric::INDEX_BYTES,
            metric::LABEL_TABLE => self.config.name.clone()
        )
        .set(0.0);
    }

    /// The table's configuration.
    pub fn config(&self) -> &TableConfig {
        &self.config
    }

    /// First load: resolves the source, builds the snapshot the ref names,
    /// runs the doctor on it, and swaps it in, within the load deadline;
    /// past it the load is dropped as `Error::LoadDeadline`. `Err` leaves
    /// the table `Degraded` with no generation; a first-load failure is
    /// fatal to the process (RFD 005) and is the caller's to report, so it
    /// is not counted under `refresh_errors_total`. On a table that already
    /// serves, a full re-resolve and reload, reusing every file the new
    /// snapshot still names by the same path and size, within the refresh
    /// deadline.
    pub async fn load(&self) -> Result<()> {
        let _serialized = self.serial.lock().await;
        self.within_deadline(self.reload())
            .await
            .map_err(|error| self.degrade(error))
    }

    /// One poll, serialized per table. `Ok(true)` when a new generation was
    /// swapped in. With no current generation, a full load (a table left
    /// `Degraded` with no generation recovers on the next poll); otherwise
    /// one small read when nothing moved (RFD 005), and a build with reuse
    /// when the ref advanced. `Err` leaves the current generation serving,
    /// the table `Degraded`, and counts one `refresh_errors_total` with the
    /// error's kind as its reason; `Ok(false)` after such a failure ends
    /// the `Degraded` state, since the table is in step with its ref again.
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
                    metrics::counter!(
                        metric::GENERATION_SWAPS_TOTAL,
                        metric::LABEL_TABLE => self.config.name.clone()
                    )
                    .increment(1);
                }
                Ok(swapped)
            }
            Err(error) => {
                metrics::counter!(
                    metric::REFRESH_ERRORS_TOTAL,
                    metric::LABEL_TABLE => self.config.name.clone(),
                    metric::LABEL_REASON => error.kind().to_string()
                )
                .increment(1);
                Err(self.degrade(error))
            }
        }
    }

    /// The table's configured name.
    pub fn name(&self) -> &str {
        &self.config.name
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
            let layout = TableLayout::derive(
                self.name(),
                &resolved.metadata,
                &snapshot,
                &self.config.key_column,
                &self.config.value_column,
            )?;
            Ok(layout.key_type)
        })
        .await
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
                    table: self.config.name.clone(),
                    source: canceled,
                },
            })?;
            answered[index] = values?;
        }
        Ok(answered.into_iter().flatten().collect())
    }

    /// Holds the table's load and refresh lock, so no load is in flight
    /// while the holder reads the current generation's identities; for
    /// `Opened::retain`, which takes every table's in turn.
    pub(crate) async fn serialized(&self) -> MutexGuard<'_, ()> {
        self.serial.lock().await
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
    /// `Opened::retain` can take it.
    async fn within_deadline<T>(&self, work: impl Future<Output = Result<T>>) -> Result<T> {
        let table = self.config.name.clone();
        if self.current().is_none() {
            let after = self.load_deadline;
            tokio::time::timeout(after, work)
                .await
                .map_err(|_elapsed| Error::LoadDeadline { table, after })?
        } else {
            let after = self.refresh_deadline;
            tokio::time::timeout(after, work)
                .await
                .map_err(|_elapsed| Error::RefreshDeadline { table, after })?
        }
    }

    /// Resolves the source and builds the snapshot the ref names over the
    /// current generation, whether or not one serves.
    async fn reload(&self) -> Result<()> {
        let resolved = self.source.resolve(&self.storage).await?;
        let snapshot = self.ref_snapshot(&resolved)?;
        let previous = self.current();
        self.build_and_admit(&resolved, &snapshot, previous.as_ref())
            .await
    }

    /// One poll over `current`: the source is asked whether its metadata
    /// moved from the known location, and only a snapshot with another id
    /// is built.
    async fn poll(&self, current: &Arc<Generation>) -> Result<bool> {
        let known = self
            .known
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
            .expect("a serving generation was admitted, which recorded its metadata location");
        let Some(resolved) = self
            .source
            .resolve_if_changed(&self.storage, Some(&known))
            .await?
        else {
            self.in_step(current);
            return Ok(false);
        };
        let snapshot = self.ref_snapshot(&resolved)?;
        if SnapshotId(snapshot.snapshot_id()) == current.snapshot_id {
            self.remember(&resolved.location);
            self.in_step(current);
            return Ok(false);
        }
        self.build_and_admit(&resolved, &snapshot, Some(current))
            .await?;
        Ok(true)
    }

    /// The snapshot the configured ref names in `resolved`, under the
    /// table's name.
    fn ref_snapshot(&self, resolved: &Resolved) -> Result<Arc<Snapshot>> {
        resolved.snapshot_for_ref(&self.config.name, &self.config.r#ref)
    }

    /// Builds `snapshot` over `previous`, gates it, and swaps it in.
    async fn build_and_admit(
        &self,
        resolved: &Resolved,
        snapshot: &Snapshot,
        previous: Option<&Arc<Generation>>,
    ) -> Result<()> {
        let generation = Generation::load(
            &self.config,
            resolved,
            snapshot,
            &self.storage,
            Arc::clone(&self.hedge),
            self.local.as_ref(),
            previous.map(Arc::as_ref),
        )
        .await?;
        let findings = self.gate(&generation)?;
        self.swap(generation, previous, &findings);
        self.remember(&resolved.location);
        Ok(())
    }

    /// The poll found the table in step with its ref, so a `Degraded` state
    /// an earlier failed poll left ends: the last refresh succeeded and
    /// `current` is the snapshot the ref names.
    fn in_step(&self, current: &Generation) {
        if matches!(self.state(), TableState::Degraded { .. }) {
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

    /// The strict gate (RFD 007, RFD 016): the doctor's verdict on a built
    /// generation before it is swapped in. Every finding is logged,
    /// inefficient at warn and advice at info. Under `strict`, an
    /// inefficient generation is dropped, never swapped, with the
    /// `Error::Inefficient` that `check::admit` gives. `Ok` carries the admitted generation's findings per code, for the
    /// `findings` gauge; a refused generation's reach the logs and the
    /// error and never the gauge, which describes the serving generation.
    fn gate(&self, generation: &Generation) -> Result<BTreeMap<FindingCode, u64>> {
        let report = check::diagnose(generation);
        let mut counts: BTreeMap<FindingCode, u64> = BTreeMap::new();
        for finding in &report.findings {
            *counts.entry(finding.code()).or_default() += 1;
            log_finding(self.name(), finding);
        }
        check::admit(&report, self.config.strict)?;
        Ok(counts)
    }

    /// Makes `generation` the serving one. The generation gauges it sets,
    /// `index_bytes`, `findings` from `findings`,
    /// `generation_sequence_number`, and `generation_swapped_at_seconds`
    /// from the clock, are kept and written while the table shows them
    /// (`write_generation`).
    fn swap(
        &self,
        generation: Generation,
        previous: Option<&Arc<Generation>>,
        findings: &BTreeMap<FindingCode, u64>,
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
        };
        {
            let mut gauges = self.gauges.lock().unwrap_or_else(PoisonError::into_inner);
            if gauges.shown {
                self.write_generation(&published);
            }
            gauges.generation = Some(published);
        }
        let snapshot_id = generation.snapshot_id;
        self.current.store(Some(Arc::new(generation)));
        self.serve_state(snapshot_id);
    }

    /// Records the failure's kind as the table's state and hands the
    /// failure back to the caller, which logs it. The current generation is
    /// untouched.
    fn degrade(&self, error: Error) -> Error {
        self.set_state(TableState::Degraded {
            serving: self.current().map(|generation| generation.snapshot_id),
            reason: error.kind(),
            since: Instant::now(),
        });
        error
    }

    /// Records the metadata location the table is now in step with.
    fn remember(&self, location: &str) {
        *self.known.lock().unwrap_or_else(PoisonError::into_inner) = Some(location.to_string());
    }

    /// Replaces the state and publishes it.
    fn set_state(&self, state: TableState) {
        *self.state.lock().unwrap_or_else(PoisonError::into_inner) = state;
        self.publish_state();
    }

    /// Sets the `table_state` gauge to 1 for the current state and 0 for
    /// each of the other two, while the table shows its gauges; every
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
                metric::LABEL_TABLE => self.config.name.clone(),
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
            metric::LABEL_TABLE => self.config.name.clone()
        )
        .set(approx::gauge_len(generation.index_bytes));
        for code in FindingCode::ALL {
            metrics::gauge!(
                metric::FINDINGS,
                metric::LABEL_TABLE => self.config.name.clone(),
                metric::LABEL_CODE => code.to_string()
            )
            .set(approx::gauge(
                generation.findings.get(&code).copied().unwrap_or(0),
            ));
        }
        metrics::gauge!(
            metric::GENERATION_SEQUENCE_NUMBER,
            metric::LABEL_TABLE => self.config.name.clone()
        )
        .set(approx::signed_gauge(generation.sequence_number.0));
        metrics::gauge!(
            metric::GENERATION_SWAPPED_AT_SECONDS,
            metric::LABEL_TABLE => self.config.name.clone()
        )
        .set(generation.swapped_at);
    }

    /// `Error::NotReady` for this table.
    fn not_ready(&self) -> Error {
        Error::NotReady {
            table: self.config.name.clone(),
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
                table: self.config.name.clone(),
                expected,
                got,
            })
        }
    }
}

impl fmt::Debug for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Table")
            .field("name", &self.config.name)
            .field("state", &self.state())
            .finish_non_exhaustive()
    }
}

/// One finding as a log event: inefficient at warn, advice at info, with the
/// table, code, tier, and file as fields and the finding's message as the
/// event's.
fn log_finding(table: &str, finding: &Finding) {
    let message = finding.message();
    let code = finding.code();
    let tier = finding.tier();
    let file = finding.file();
    match tier {
        Tier::Inefficient => tracing::warn!(table, code = %code, tier = ?tier, file, "{message}"),
        Tier::Advice => tracing::info!(table, code = %code, tier = ?tier, file, "{message}"),
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
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    use object_store::local::LocalFileSystem;
    use object_store::throttle::{ThrottleConfig, ThrottledStore};

    use super::*;
    use crate::config::{Residency, ServerConfig};
    use crate::error::{ErrorKind, RefusalReason};
    use crate::generation::testing::{
        Bloom, Bounds, Dictionary, E2E_KEYS, FILE_KEYS, FIRST_SEQUENCE, FIRST_SNAPSHOT,
        FIRST_VERSION, FileSpec, KEY_STEP, SECOND_SEQUENCE, SECOND_SNAPSHOT, SECOND_VERSION, TABLE,
        THIRD_SEQUENCE, THIRD_SNAPSHOT, THIRD_VERSION, TINY_BUDGET, TWO, Table as Fixture,
        WriterLayout, bucketing, cache, keys_from, keys_in_bucket, long_keys, parquet,
        two_bucket_table, value_for, values_of,
    };
    use crate::index::testing::with_dictionary_one_entry_short;
    use crate::index::{DecodedPart, FileFault};
    use crate::key::KeyType;
    use crate::manifest::testing::{self as manifest_testing, Partition};
    use crate::metric::LookupResult;
    use crate::storage::FILE_PREFIX;
    use crate::storage::testing::{DEADLINE_GUARD, Gauged, STALL};

    /// The GETs an unchanged poll costs: the version hint (RFD 005).
    const POLL_GETS: u64 = 1;
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

    /// The GETs resolving a table root costs: its version hint and the
    /// metadata file the hint names.
    const RESOLVE_GETS: u64 = 2;

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

    /// `fixture`'s table with `strict` off, as the one-row-per-page tables
    /// are served.
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
    async fn test_load_strict_refuses_an_inefficient_finding_and_only_that() {
        let inefficient = WriterLayout {
            dictionary: Dictionary::Value,
            ..WriterLayout::contract()
        };
        let fixture = one_file_fixture(inefficient).await;
        let strict = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        assert!(strict.config.strict);
        let error = strict.load().await.unwrap_err();
        let Error::Inefficient { findings, .. } = error else {
            panic!("expected Inefficient, got {error:?}");
        };
        assert_eq!(findings, [FindingCode::DictionaryEncodedValue]);
        assert!(strict.current().is_none());
        let (serving, kept) = degraded(&strict);
        assert_eq!(serving, None);
        assert_eq!(kept, ErrorKind::Inefficient);

        let lenient = table_over(
            &fixture,
            TableConfig {
                strict: false,
                ..fixture.config(Residency::Full, None)
            },
        )
        .await;
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
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
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
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
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

    #[tokio::test]
    async fn test_refresh_strict_refuses_an_inefficient_finding_and_keeps_serving() {
        let (mut fixture, all) = two_bucket_table().await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        table.load().await.unwrap();
        let inefficient = WriterLayout {
            dictionary: Dictionary::Value,
            ..WriterLayout::contract()
        };
        append_second(&mut fixture, &all, "dict", inefficient).await;
        let error = table.refresh().await.unwrap_err();
        let Error::Inefficient { findings, .. } = error else {
            panic!("expected Inefficient, got {error:?}");
        };
        assert_eq!(findings, [FindingCode::DictionaryEncodedValue]);
        assert_eq!(
            table.current().unwrap().snapshot_id,
            SnapshotId(FIRST_SNAPSHOT)
        );
        assert_eq!(
            table.get(&Key::Long(all[0])).await.unwrap().value,
            present(all[0])
        );
        let (serving, kept) = degraded(&table);
        assert_eq!(serving, Some(SnapshotId(FIRST_SNAPSHOT)));
        assert_eq!(kept, ErrorKind::Inefficient);
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
            let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
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
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let out = metrics::with_local_recorder(&recorder, || runtime.block_on(body));
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
        let inefficient = WriterLayout {
            dictionary: Dictionary::Value,
            ..WriterLayout::contract()
        };
        let ((lenient_bytes, strict_bytes), series) = recorded(async {
            let fixture = one_file_fixture(inefficient).await;
            let lenient = table_over(
                &fixture,
                TableConfig {
                    strict: false,
                    ..fixture.config(Residency::Full, None)
                },
            )
            .await;
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
            append_second(&mut fixture, &all, "dict", inefficient).await;
            assert!(matches!(
                strict.refresh().await.unwrap_err(),
                Error::Inefficient { .. }
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

    /// A hidden table writes no gauge while it loads, so the table serving
    /// under its name keeps its series; `show` then publishes its state and
    /// its generation's gauges as the swap set them.
    #[test]
    fn test_a_hidden_table_writes_its_gauges_once_shown() {
        let (index_bytes, loading) = recorded(async {
            let (fixture, _) = two_bucket_table().await;
            let table = hidden_over(&fixture).await;
            table.load().await.unwrap();
            table.current().unwrap().index_bytes
        });
        let gauges = [
            metric::TABLE_STATE,
            metric::INDEX_BYTES,
            metric::FINDINGS,
            metric::GENERATION_SEQUENCE_NUMBER,
            metric::GENERATION_SWAPPED_AT_SECONDS,
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
    }

    /// A withdrawn table reads in no state and holds no index bytes, and a
    /// refresh after the withdrawal writes neither back.
    #[test]
    fn test_a_withdrawn_table_reads_in_no_state_and_writes_no_gauge() {
        let ((), series) = recorded(async {
            let (fixture, _) = two_bucket_table().await;
            let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
            table.load().await.unwrap();
            table.withdraw();
            fixture.remove_hint();
            table.refresh().await.unwrap_err();
        });
        for name in StateName::ALL {
            assert_eq!(
                state_gauge(&series, TABLE, name).to_bits(),
                approx::gauge(0).to_bits(),
                "{name}"
            );
        }
        assert_eq!(
            series
                .gauge(metric::INDEX_BYTES, &[table_label()])
                .to_bits(),
            approx::gauge(0).to_bits()
        );
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
