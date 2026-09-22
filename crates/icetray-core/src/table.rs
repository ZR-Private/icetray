//! The table lifecycle (RFD 005, RFD 007): a `Table` follows one ref,
//! builds each new snapshot as a `Generation` beside the one it serves,
//! runs the doctor on the new generation and refuses a cost-tier one under
//! `strict`, then swaps it in atomically. Loads and refreshes of one table
//! are serialized; a failed one leaves the last generation serving and the
//! table `Degraded`. A lookup takes one generation and answers from it
//! alone, so a swap during a batch cannot mix snapshots.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;

use arc_swap::ArcSwapOption;
use futures::future::try_join_all;
use iceberg::spec::Snapshot;
use tokio::sync::MutexGuard;

use crate::approx;
use crate::cache::PageCache;
use crate::catalog::{Resolved, TableSource};
use crate::check::{self, Finding, FindingCode, Serviceability, Tier};
use crate::config::TableConfig;
use crate::error::{Error, ErrorKind, Result, SnapshotId};
use crate::generation::{Generation, Lookup, MultiLookup, Value};
use crate::key::Key;
use crate::local::LocalDir;
use crate::metric;
use crate::storage::{HedgeWindow, Storage};
use crate::time;

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

/// One served table: its configuration and source, the generation it
/// serves, and its lifecycle state. Owns the table's `HedgeWindow`, built
/// from the storage's hedge settings and handed to every `Generation::load`,
/// so one table's delay is set from its own reads and never another's (RFD
/// 013). Shared across tasks behind an `Arc`.
pub struct Table {
    config: TableConfig,
    source: TableSource,
    storage: Arc<Storage>,
    cache: PageCache,
    local: Option<Arc<LocalDir>>,
    hedge: Arc<HedgeWindow>,
    /// The serving generation; a lookup takes one `load_full` and uses that
    /// generation for its whole duration.
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
}

impl Table {
    /// Constructs in `Loading`; nothing is read until `load`.
    pub fn new(
        config: TableConfig,
        source: TableSource,
        storage: Arc<Storage>,
        cache: PageCache,
        local: Option<Arc<LocalDir>>,
    ) -> Self {
        let hedge = Arc::new(HedgeWindow::new(storage.hedge_config()));
        let table = Self {
            config,
            source,
            storage,
            cache,
            local,
            hedge,
            current: ArcSwapOption::empty(),
            state: Mutex::new(TableState::Loading),
            known: Mutex::new(None),
            serial: tokio::sync::Mutex::new(()),
        };
        table.set_state(TableState::Loading);
        table
    }

    /// First load: resolves the source, builds the snapshot the ref names,
    /// runs the doctor on it, and swaps it in. `Err` leaves the table
    /// `Degraded` with no generation; a first-load failure is fatal to the
    /// process (RFD 005) and is the caller's to report, so it is not counted
    /// under `refresh_errors_total`. On a table that already serves, a full
    /// re-resolve and reload, reusing every file the new snapshot still
    /// names by the same path and size.
    pub async fn load(&self) -> Result<()> {
        let _serialized = self.serial.lock().await;
        self.reload().await.map_err(|error| self.degrade(error))
    }

    /// One poll, serialized per table. `Ok(true)` when a new generation was
    /// swapped in. With no current generation, a full load (a table left
    /// `Degraded` with no generation recovers on the next poll); otherwise
    /// one small read when nothing moved (RFD 005), and a build with reuse
    /// when the ref advanced. `Err` leaves the current generation serving,
    /// the table `Degraded`, and counts one `refresh_errors_total` with the
    /// error's kind as its reason; `Ok(false)` after such a failure ends
    /// the `Degraded` state, since the table is in step with its ref again.
    pub async fn refresh(&self) -> Result<bool> {
        let _serialized = self.serial.lock().await;
        let outcome = match self.current() {
            None => self.reload().await.map(|()| true),
            Some(current) => self.poll(&current).await,
        };
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

    /// The table's configuration.
    pub fn config(&self) -> &TableConfig {
        &self.config
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

    /// One lookup against the serving generation. `Error::NotReady` with no
    /// generation; `Error::KeyType` for a key of another type than the
    /// table's, before any read.
    pub async fn get(&self, key: &Key) -> Result<Lookup> {
        let generation = self.serving()?;
        self.check_key_type(&generation, key)?;
        let value = self.lookup(&generation, key).await?;
        Ok(Lookup {
            value,
            snapshot_id: generation.snapshot_id,
        })
    }

    /// Several lookups against one generation, answered in key order. Every
    /// key's type is checked before any read, so a mismatch anywhere fails
    /// the whole call and issues nothing; the lookups then run concurrently,
    /// and any failure fails the whole call (RFD 010).
    pub async fn multi_get(&self, keys: &[Key]) -> Result<MultiLookup> {
        let generation = self.serving()?;
        for key in keys {
            self.check_key_type(&generation, key)?;
        }
        let values = try_join_all(keys.iter().map(|key| self.lookup(&generation, key))).await?;
        Ok(MultiLookup {
            values,
            snapshot_id: generation.snapshot_id,
        })
    }

    /// Holds the table's load and refresh lock, so no load is in flight
    /// while the holder reads the current generation's identities; for
    /// `Opened::retain`, which takes every table's in turn.
    pub(crate) async fn serialized(&self) -> MutexGuard<'_, ()> {
        self.serial.lock().await
    }

    /// Resolves the source and builds the snapshot the ref names over the
    /// current generation, whether or not one serves.
    async fn reload(&self) -> Result<()> {
        let resolved = self.source.resolve(&self.storage).await?;
        let snapshot = resolved.snapshot_for_ref(&self.config.name, &self.config.r#ref)?;
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
            .resolve_if_changed(&self.storage, &known)
            .await?
        else {
            self.in_step(current);
            return Ok(false);
        };
        let snapshot = resolved.snapshot_for_ref(&self.config.name, &self.config.r#ref)?;
        if SnapshotId(snapshot.snapshot_id()) == current.snapshot_id {
            self.remember(&resolved.location);
            self.in_step(current);
            return Ok(false);
        }
        self.build_and_admit(&resolved, &snapshot, Some(current))
            .await?;
        Ok(true)
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
            self.local.as_deref(),
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
            self.set_state(TableState::Serving {
                snapshot_id: current.snapshot_id,
                since: Instant::now(),
            });
        }
    }

    /// The strict gate (RFD 007, RFD 016): the doctor's verdict on a built
    /// generation before it is swapped in. Every finding is logged, cost at
    /// warn and advice at info. Under `strict`, a cost-tier generation is
    /// dropped, never swapped, as `Error::Inefficient` with the distinct
    /// cost codes in the report's order; advice never affects admission.
    /// `Ok` carries the admitted generation's findings per code, for the
    /// `findings` gauge; a refused generation's reach the logs and the
    /// error and never the gauge, which describes the serving generation.
    fn gate(&self, generation: &Generation) -> Result<BTreeMap<FindingCode, u64>> {
        let report = check::diagnose(generation);
        let mut counts: BTreeMap<FindingCode, u64> = BTreeMap::new();
        for finding in &report.findings {
            *counts.entry(finding.code).or_default() += 1;
            log_finding(self.name(), finding);
        }
        match report.summary.serviceability {
            Serviceability::Inefficient if self.config.strict => Err(Error::Inefficient {
                table: self.config.name.clone(),
                findings: cost_codes(&report.findings),
            }),
            Serviceability::Inefficient | Serviceability::Efficient => Ok(counts),
        }
    }

    /// Makes `generation` the serving one. `generation_info` is set to 0 for
    /// the snapshot it replaces and 1 for its own (no series is removed; a
    /// replaced snapshot's reads 0 until the process restarts); the
    /// `findings` gauge is set from
    /// `findings` for every code, zero for the absent ones, so a code that
    /// cleared on the swap reads zero.
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
            .set(approx::gauge(findings.get(&code).copied().unwrap_or(0)));
        }
        if let Some(previous) = previous {
            self.generation_info(previous.snapshot_id, false);
        }
        self.generation_info(generation.snapshot_id, true);
        let snapshot_id = generation.snapshot_id;
        self.current.store(Some(Arc::new(generation)));
        self.set_state(TableState::Serving {
            snapshot_id,
            since: Instant::now(),
        });
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

    /// Replaces the state and sets the `table_state` gauge to 1 for it and
    /// 0 for each of the other two.
    fn set_state(&self, state: TableState) {
        let current = state.name();
        *self.state.lock().unwrap_or_else(PoisonError::into_inner) = state;
        for name in StateName::ALL {
            metrics::gauge!(
                metric::TABLE_STATE,
                metric::LABEL_TABLE => self.config.name.clone(),
                metric::LABEL_STATE => name.to_string()
            )
            .set(approx::gauge(u64::from(name == current)));
        }
    }

    /// Sets `generation_info` for one snapshot: 1 while it serves, 0 once
    /// replaced.
    fn generation_info(&self, snapshot_id: SnapshotId, serving: bool) {
        metrics::gauge!(
            metric::GENERATION_INFO,
            metric::LABEL_TABLE => self.config.name.clone(),
            metric::LABEL_SNAPSHOT_ID => snapshot_id.to_string()
        )
        .set(approx::gauge(u64::from(serving)));
    }

    /// The serving generation, or `Error::NotReady`.
    fn serving(&self) -> Result<Arc<Generation>> {
        self.current().ok_or_else(|| Error::NotReady {
            table: self.config.name.clone(),
        })
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

    /// One lookup through `generation`, timed and counted under its result;
    /// the one path `get` and `multi_get` share.
    async fn lookup(&self, generation: &Generation, key: &Key) -> Result<Value> {
        let started = Instant::now();
        let (value, result) = generation.lookup(key, &self.cache).await?;
        let elapsed = started.elapsed();
        metrics::counter!(
            metric::LOOKUPS_TOTAL,
            metric::LABEL_TABLE => self.config.name.clone(),
            metric::LABEL_RESULT => result.to_string()
        )
        .increment(1);
        metrics::histogram!(
            metric::LOOKUP_SECONDS,
            metric::LABEL_TABLE => self.config.name.clone(),
            metric::LABEL_RESULT => result.to_string()
        )
        .record(time::seconds(elapsed));
        Ok(value)
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

/// One finding as a log event: cost at warn, advice at info, with the
/// table, code, tier, and file as fields and the finding's message as the
/// event's.
fn log_finding(table: &str, finding: &Finding) {
    let message = finding.message.as_str();
    match finding.tier {
        Tier::Cost => tracing::warn!(
            table,
            code = %finding.code,
            tier = ?finding.tier,
            file = finding.file.as_deref(),
            "{message}"
        ),
        Tier::Advice => tracing::info!(
            table,
            code = %finding.code,
            tier = ?finding.tier,
            file = finding.file.as_deref(),
            "{message}"
        ),
    }
}

/// The distinct codes of the cost findings, in the order the report gives
/// them: what `Error::Inefficient` carries.
fn cost_codes(findings: &[Finding]) -> Vec<FindingCode> {
    let mut codes = Vec::new();
    for finding in findings.iter().filter(|finding| finding.tier == Tier::Cost) {
        if !codes.contains(&finding.code) {
            codes.push(finding.code);
        }
    }
    codes
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::ops::Range;
    use std::sync::Arc;

    use bytes::Bytes;
    use metrics::{Key as MetricKey, SharedString, Unit};
    use metrics_util::CompositeKey;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    use super::*;
    use crate::config::Residency;
    use crate::error::{ErrorKind, LookupResult, RefusalReason};
    use crate::generation::testing::{
        Bloom, Bounds, Dictionary, E2E_KEYS, FIRST_SEQUENCE, FIRST_SNAPSHOT, FIRST_VERSION,
        FileSpec, KEY_STEP, SECOND_SEQUENCE, SECOND_SNAPSHOT, SECOND_VERSION, TABLE,
        THIRD_SEQUENCE, THIRD_SNAPSHOT, THIRD_VERSION, TWO, Table as Fixture, WriterLayout,
        bucketing, keys_from, keys_in_bucket, long_keys, two_bucket_table, value_for, values_of,
    };
    use crate::key::KeyType;
    use crate::manifest::testing::Partition;

    /// The memory budget of a test page cache: every page a test touches fits.
    const CACHE_BYTES: usize = 64 * 1024 * 1024;
    /// Keys in a one-file fixture: enough that its bloom filter, sized to
    /// them, draws no finding of its own.
    const FILE_KEYS: i64 = 32;
    /// The GETs an unchanged poll costs: the version hint (RFD 005).
    const POLL_GETS: u64 = 1;
    /// The positions in bucket 0's key list that the duplicate file
    /// repeats.
    const DUP_KEYS: Range<usize> = 1..3;
    /// The name of a second, strict table in the metrics tests, beside the
    /// fixture's `TABLE`.
    const STRICT_TABLE: &str = "strict";

    async fn cache() -> PageCache {
        PageCache::new(CACHE_BYTES, None).await.unwrap()
    }

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
    /// local directory, in `Loading`.
    async fn table_over(fixture: &Fixture, config: TableConfig) -> Table {
        Table::new(
            config,
            TableSource::Static {
                location: fixture.root(),
            },
            Arc::clone(&fixture.storage),
            cache().await,
            None,
        )
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
    /// runtime, so their futures are `Send`; `tokio::spawn` refuses one that
    /// is not at compile time.
    #[tokio::test]
    async fn test_load_refresh_and_get_are_spawnable() {
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
        let table = table_over(&fixture, fixture.config(Residency::Disk, None)).await;
        let error = table.load().await.unwrap_err();
        assert!(matches!(
            error,
            Error::Refused {
                reason: RefusalReason::NoLocalDir,
                ..
            }
        ));
        let (serving, kept) = degraded(&table);
        assert_eq!(serving, None);
        assert_eq!(kept, ErrorKind::Refused(RefusalReason::NoLocalDir));
        assert!(table.current().is_none());
        let error = table.get(&Key::Long(all[0])).await.unwrap_err();
        assert!(matches!(error, Error::NotReady { .. }));
    }

    #[tokio::test]
    async fn test_load_strict_refuses_a_cost_finding_and_only_that() {
        let cost = WriterLayout {
            dictionary: Dictionary::Value,
            ..WriterLayout::contract()
        };
        let fixture = one_file_fixture(cost).await;
        let strict = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        assert!(strict.config().strict);
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

    #[tokio::test]
    async fn test_refresh_strict_refuses_a_cost_finding_and_keeps_serving() {
        let (mut fixture, all) = two_bucket_table().await;
        let table = table_over(&fixture, fixture.config(Residency::Full, None)).await;
        table.load().await.unwrap();
        let cost = WriterLayout {
            dictionary: Dictionary::Value,
            ..WriterLayout::contract()
        };
        append_second(&mut fixture, &all, "dict", cost).await;
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
                .filter(|(key, ..)| labelled(key.key(), name, labels))
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

        fn samples(&self, name: &str, labels: &[(&str, String)]) -> usize {
            match self.find(name, labels) {
                DebugValue::Histogram(values) => values.len(),
                DebugValue::Counter(_) | DebugValue::Gauge(_) => panic!("{name} is a histogram"),
            }
        }
    }

    /// Whether `key` is `name` with exactly `labels`.
    fn labelled(key: &MetricKey, name: &str, labels: &[(&str, String)]) -> bool {
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

    #[test]
    fn test_metrics_follow_lookups_swaps_and_failures() {
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
        let info = |snapshot: i64| {
            series.gauge(
                metric::GENERATION_INFO,
                &[
                    table_label(),
                    (metric::LABEL_SNAPSHOT_ID, SnapshotId(snapshot).to_string()),
                ],
            )
        };
        assert_eq!(info(SECOND_SNAPSHOT).to_bits(), approx::gauge(1).to_bits());
        assert_eq!(info(FIRST_SNAPSHOT).to_bits(), approx::gauge(0).to_bits());
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
        let cost = WriterLayout {
            dictionary: Dictionary::Value,
            ..WriterLayout::contract()
        };
        let ((lenient_bytes, strict_bytes), series) = recorded(async {
            let fixture = one_file_fixture(cost).await;
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
            append_second(&mut fixture, &all, "dict", cost).await;
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
