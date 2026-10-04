//! Opening every configured table (RFD 012): one storage, one page cache,
//! the optional local directory, one count of calls in flight, and every
//! table in `Loading` over them, built the same way for the daemon and the
//! one-shot subcommands, so a subcommand reads a table exactly as `serve`
//! does. Loading is the caller's step. A running daemon replaces its set of
//! tables through `Opened::apply` (RFD 018).

use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use arc_swap::{ArcSwap, Guard};
use futures::future::join_all;

use crate::cache::{self, PageCache};
use crate::catalog::TableSource;
use crate::config::{Config, ConfigError, TableConfig};
use crate::error::{Error, Result};
use crate::local::{DataFileIdentity, LocalDir};
use crate::storage::Storage;
use crate::table::{CallsInFlight, FindingLog, Table, TableHandles};

/// Everything a process opened: the storage every read goes through, the
/// one page cache every table shares, the local directory when configured,
/// and the tables in configuration order.
pub struct Opened {
    /// The storage every table reads through.
    pub storage: Arc<Storage>,
    /// The page cache every table shares.
    pub cache: PageCache,
    /// The local directory (RFD 017), when `storage.local` is set.
    pub local: Option<Arc<LocalDir>>,
    /// The tables served, each in `Loading` until the caller loads it.
    /// Read without a lock; `apply` replaces the set whole.
    tables: ArcSwap<Tables>,
    /// The tables an `apply` has built and not yet swapped in or dropped.
    /// Their loads write chunk files no serving generation references, so
    /// `retain` counts them live. Never held across an `await`.
    pending: Mutex<Vec<Arc<Table>>>,
    /// Held by `retain` from computing its live set to its deletes, and by
    /// `apply` to add to `pending` and to swap, so no table joins the set
    /// between the two; retains run one at a time. A refused or dropped
    /// `apply` takes its tables out of `pending` without it: a table
    /// leaving only makes a retain in progress keep more than it needs.
    membership: tokio::sync::Mutex<()>,
    /// Held by `apply` throughout: one reload at a time, each diffed
    /// against the set the last one left.
    applying: tokio::sync::Mutex<()>,
    /// What every table is built over, for the tables `apply` adds.
    handles: TableHandles,
    /// The server's deadlines every table is built under.
    deadlines: Deadlines,
    /// Whether `tables` is every configured table: `only` narrowed nothing,
    /// or named every one. `retain`'s live set is complete only then.
    complete: bool,
}

/// The tables an `Opened` serves: a slice in configuration order, through
/// `Deref`, and by name through `get`. Immutable; `Opened::apply` swaps in
/// another.
pub struct Tables {
    in_order: Vec<Arc<Table>>,
    /// Each table's position in `in_order`, by its configured name.
    by_name: HashMap<String, usize>,
}

impl Tables {
    /// `in_order`, indexed by name. The names are distinct: configuration
    /// validation refuses a repeated one.
    fn new(in_order: Vec<Arc<Table>>) -> Self {
        let by_name = in_order
            .iter()
            .enumerate()
            .map(|(at, table)| (table.name().to_string(), at))
            .collect();
        Self { in_order, by_name }
    }

    /// The table served under `name`, if any.
    pub fn get(&self, name: &str) -> Option<&Arc<Table>> {
        self.by_name.get(name).map(|&at| &self.in_order[at])
    }
}

impl Deref for Tables {
    type Target = [Arc<Table>];

    fn deref(&self) -> &Self::Target {
        &self.in_order
    }
}

/// The server's deadlines a table is built under (`Table::new`).
#[derive(Clone, Copy)]
struct Deadlines {
    load: Duration,
    refresh: Duration,
    get: Duration,
}

/// What `Opened::apply` changed: the tables it loaded and swapped in, the
/// tables it swapped out, and how many it kept as they were. A table whose
/// configuration changed is in both lists, under one name.
pub struct Applied {
    /// The tables swapped in, each serving its first generation.
    pub started: Vec<Arc<Table>>,
    /// The tables swapped out, whose gauges are withdrawn.
    pub stopped: Vec<Arc<Table>>,
    /// The tables kept, whose configuration is unchanged.
    pub kept: usize,
}

/// Why `Opened::apply` changed nothing: every table it could not open or
/// load, by name, in configuration order.
#[derive(Debug)]
pub struct Refused {
    /// Each table that failed, with its failure.
    pub failures: Vec<(String, Error)>,
}

/// Tables `apply` added to `Opened::pending`, taken out again when this
/// drops, whether the tables were swapped in or dropped with the reload.
struct Registered<'a> {
    opened: &'a Opened,
    tables: Vec<Arc<Table>>,
}

impl Drop for Registered<'_> {
    fn drop(&mut self) {
        self.opened
            .pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|pending| !self.tables.iter().any(|table| Arc::ptr_eq(table, pending)));
    }
}

impl Opened {
    /// The tables served: the set as it stands, which a later `apply`
    /// does not change.
    pub fn tables(&self) -> Arc<Tables> {
        self.tables.load_full()
    }

    /// The tables served, borrowed through one `ArcSwap` load guard: what
    /// a call holds for its whole duration, and a swap meanwhile keeps the
    /// borrowed set alive until the guard drops, as `Table::get` holds its
    /// generation. The guard takes a fast slot as that one does, and falls
    /// back to a counted reference when its thread's slots are all held.
    pub fn serving_tables(&self) -> Guard<Arc<Tables>> {
        self.tables.load()
    }

    /// Replaces the tables served with `configs`, all or nothing (RFD
    /// 018). Tables are matched by name: one whose configuration is equal
    /// in every field is kept as it is, generation and refresh untouched;
    /// every other is built and loaded beside the set serving, the
    /// serving table of its name included, and neither is touched until
    /// every one has loaded. Then the set is swapped at once: the tables
    /// no longer configured are withdrawn (`Table::withdraw`) and dropped
    /// from the set, the new ones shown, and a call that looks a table up
    /// after the swap finds the new set; a call in flight finishes on the
    /// generation it holds. When any table fails to open or load, the
    /// tables built are dropped and the set serving is untouched, and
    /// `Refused` names every failure: every load runs to its end or its
    /// deadline, so a refusal holds what the loads reserved until the
    /// slowest ends. The caller spawns and stops the refreshes, and runs
    /// `retain` after either outcome, to free what the dropped tables held
    /// or what the refused ones wrote. `configs` is taken as validated: the names are
    /// distinct. An `Opened` that `only` narrowed stays narrowed, and its
    /// `retain` still frees nothing.
    pub async fn apply(&self, configs: &[TableConfig]) -> std::result::Result<Applied, Refused> {
        let _applying = self.applying.lock().await;
        let serving = self.tables();
        let mut next = Vec::with_capacity(configs.len());
        let mut started = Vec::new();
        let mut failures = Vec::new();
        for config in configs {
            let kept = serving
                .iter()
                .find(|table| table.name() == config.name && table.config() == config);
            if let Some(kept) = kept {
                next.push(Arc::clone(kept));
                continue;
            }
            match self.build(config).await {
                Ok(table) => {
                    let table = Arc::new(table);
                    started.push(Arc::clone(&table));
                    next.push(table);
                }
                Err(error) => failures.push((config.name.clone(), error)),
            }
        }
        if !failures.is_empty() {
            return Err(Refused { failures });
        }
        let registered = self.register(&started).await;
        let loads = join_all(started.iter().map(|table| table.load())).await;
        let failures: Vec<(String, Error)> = started
            .iter()
            .zip(loads)
            .filter_map(|(table, loaded)| {
                loaded.err().map(|error| (table.name().to_string(), error))
            })
            .collect();
        if !failures.is_empty() {
            return Err(Refused { failures });
        }
        let stopped: Vec<Arc<Table>> = serving
            .iter()
            .filter(|table| !next.iter().any(|kept| Arc::ptr_eq(kept, table)))
            .cloned()
            .collect();
        let kept = next.len() - started.len();
        {
            let _members = self.membership.lock().await;
            for table in &stopped {
                table.withdraw();
            }
            self.tables.store(Arc::new(Tables::new(next)));
            drop(registered);
            for table in &started {
                table.show();
            }
        }
        Ok(Applied {
            started,
            stopped,
            kept,
        })
    }

    /// A table for `config` over this process's handles and deadlines, not
    /// showing its gauges (`Table::hidden`).
    async fn build(&self, config: &TableConfig) -> Result<Table> {
        build(config, &self.handles, self.deadlines).await
    }

    /// Adds `tables` to `pending` while no `retain` runs, until the
    /// returned registration drops.
    async fn register(&self, tables: &[Arc<Table>]) -> Registered<'_> {
        let _members = self.membership.lock().await;
        self.pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend(tables.iter().cloned());
        Registered {
            opened: self,
            tables: tables.to_vec(),
        }
    }

    /// Deletes the chunk files no table's serving generation references and
    /// returns the bytes freed; `Ok(0)` with no local directory. The live
    /// set is the union of `Generation::identities` (RFD 017) over every
    /// table served and every table an `apply` is loading, computed and
    /// applied while every one's load lock is held, in `tables` order and
    /// then the pending ones, so no load is in flight anywhere meanwhile,
    /// and while `membership` is held, so no table joins the set. An
    /// `Opened` that `only` narrowed frees nothing: the tables it left out
    /// share the directory, and their chunk files are live to them. This
    /// is the one place chunk files are deleted: the serve loop calls it
    /// after every table's first `load`, after any `refresh` that swapped,
    /// and after an `apply` that did, and a one-shot subcommand may call it
    /// before exit.
    ///
    /// Every lock is taken because a load in flight has written chunk files
    /// that no serving generation references yet, and a live set computed
    /// beside it would delete them from under the generation about to swap
    /// in. The locks are taken one at a time, and nothing that holds one
    /// waits for `membership`, so no lock is waited for in a cycle. The cost
    /// is that a slow table holds retention up, and with it the next load
    /// or refresh of every table whose lock is already held. A load or
    /// refresh gives its lock up once its deadline has passed since it took
    /// it: the load deadline for a build with no generation serving
    /// (`server.load_deadline`, thirty minutes by default), the refresh
    /// deadline otherwise (`server.refresh_deadline`, five minutes). The
    /// lock is fair, so a request queued behind it comes next: each lock is
    /// waited for at most one deadline, and one slow table holds the others
    /// up at most that long, plus the deletes; while a table's first load
    /// runs, that is its load deadline. A table
    /// whose lock is not yet taken can start a refresh while an earlier one
    /// is waited for, so when several tables are slow, a table early in
    /// `tables` can be held up one deadline for each slow table after it.
    pub async fn retain(&self) -> Result<u64> {
        let Some(local) = &self.local else {
            return Ok(0);
        };
        if !self.complete {
            return Ok(0);
        }
        let _members = self.membership.lock().await;
        let pending = self
            .pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let tables: Vec<Arc<Table>> = self.tables().iter().cloned().chain(pending).collect();
        let mut held = Vec::with_capacity(tables.len());
        for table in &tables {
            held.push(table.serialized().await);
        }
        let live: HashSet<DataFileIdentity> = tables
            .iter()
            .filter_map(|table| table.current())
            .flat_map(|generation| generation.identities())
            .collect();
        let freed = local.retain(&live)?;
        drop(held);
        Ok(freed)
    }
}

/// Builds storage over the SDK's default chain, which it loads before its
/// first S3 read and never for a process that reads none, one cache, one
/// count of calls in flight, and every configured table in `Loading` over
/// them, each logging the doctor's findings as `findings` says; `only`
/// narrows by name, and a name that
/// matches no table yields `Ok` with empty `tables`, whose message and exit
/// code are the caller's. Nothing is read from any table: `open` issues no
/// GET.
pub async fn open(config: &Config, only: Option<&str>, findings: FindingLog) -> Result<Opened> {
    let storage = Arc::new(Storage::with_sdk_defaults(config.storage.clone()));
    open_with(config, only, storage, findings).await
}

/// `open` over a storage the caller built, for the testkit and embedders:
/// a test constructs one with `Storage::new` and never touches the SDK
/// chain, and registers its own stores on it. The binary's subcommands
/// use `open`; the daemon's in-process tests start it over this. Before
/// anything is opened, `page_cache_bytes` is checked against the minimum
/// of the shards the cache has on the calling runtime, which parsing could
/// not know: less is `Error::Config` with
/// `ConfigViolation::PageCacheTooSmall` (`cache::check_capacity`).
pub async fn open_with(
    config: &Config,
    only: Option<&str>,
    storage: Arc<Storage>,
    findings: FindingLog,
) -> Result<Opened> {
    cache::check_capacity(config.server.page_cache_bytes)
        .map_err(|violation| Error::Config(ConfigError::Invalid(violation)))?;
    let local = match &config.storage.local {
        Some(local) => Some(Arc::new(LocalDir::open(local, storage.stats())?)),
        None => None,
    };
    let cache = PageCache::new(
        config.server.page_cache_bytes,
        local.as_ref().and_then(|local| local.page_cache_level()),
    )
    .await?;
    let handles = TableHandles {
        storage: Arc::clone(&storage),
        cache: cache.clone(),
        local: local.clone(),
        calls: CallsInFlight::new(),
        findings,
    };
    let deadlines = Deadlines {
        load: config.server.load_deadline,
        refresh: config.server.refresh_deadline,
        get: config.server.get_deadline,
    };
    let mut tables = Vec::with_capacity(config.tables.len());
    for table in config
        .tables
        .iter()
        .filter(|table| only.is_none_or(|name| table.name == name))
    {
        let table = build(table, &handles, deadlines).await?;
        table.show();
        tables.push(Arc::new(table));
    }
    let complete = tables.len() == config.tables.len();
    Ok(Opened {
        storage,
        cache,
        local,
        tables: ArcSwap::from_pointee(Tables::new(tables)),
        pending: Mutex::new(Vec::new()),
        membership: tokio::sync::Mutex::new(()),
        applying: tokio::sync::Mutex::new(()),
        handles,
        deadlines,
        complete,
    })
}

/// A table for `config` over `handles`, under `deadlines`, in `Loading` and
/// not showing its gauges (`Table::hidden`).
async fn build(
    config: &TableConfig,
    handles: &TableHandles,
    deadlines: Deadlines,
) -> Result<Table> {
    let source = TableSource::from_config(&config.source).await?;
    Ok(Table::hidden(
        config.clone(),
        source,
        handles.clone(),
        deadlines.load,
        deadlines.refresh,
        deadlines.get,
    ))
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use futures::FutureExt as _;
    use object_store::local::LocalFileSystem;
    use object_store::throttle::{ThrottleConfig, ThrottledStore};

    use super::*;
    use crate::approx;
    use crate::config::{
        ConfigViolation, LocalConfig, Residency, ServerConfig, SourceConfig, StorageConfig,
        TableConfig,
    };
    use crate::generation::testing::{
        FIRST_SNAPSHOT, SECOND_SEQUENCE, SECOND_SNAPSHOT, SECOND_VERSION, TWO, Table as Fixture,
        two_bucket_table,
    };
    use crate::storage::FILE_PREFIX;
    use crate::storage::testing::STALL;
    use crate::table::TableState;

    /// The names of the two configured tables.
    const FIRST_TABLE: &str = "a";
    const SECOND_TABLE: &str = "b";
    /// A name no table is configured under.
    const NO_TABLE: &str = "nobody";

    /// Two contract files in two buckets, committed as `FIRST_SNAPSHOT`.
    async fn two_file_fixture() -> Fixture {
        two_bucket_table().await.0
    }

    /// A table named `name` following `fixture` in `residency`.
    fn named(name: &str, fixture: &Fixture, residency: Residency) -> TableConfig {
        TableConfig {
            name: name.to_string(),
            ..fixture.config(residency, None)
        }
    }

    /// A configuration with no local directory over `tables`.
    fn config(tables: Vec<TableConfig>) -> Config {
        Config {
            server: ServerConfig::default(),
            storage: StorageConfig::default(),
            tables,
        }
    }

    /// A configuration with a local directory at `dir` and no page cache
    /// disk level, over `tables`.
    fn config_with_local(dir: &Path, tables: Vec<TableConfig>) -> Config {
        Config {
            storage: StorageConfig {
                local: Some(LocalConfig {
                    dir: dir.to_path_buf(),
                    index_budget_bytes: None,
                    page_cache_disk_bytes: 0,
                }),
                ..StorageConfig::default()
            },
            ..config(tables)
        }
    }

    fn storage_for(config: &Config) -> Arc<Storage> {
        Arc::new(Storage::new(config.storage.clone()))
    }

    fn names(opened: &Opened) -> Vec<String> {
        names_of(&opened.tables())
    }

    /// The table `opened` serves under `name`, if any.
    fn table_named(opened: &Opened, name: &str) -> Option<Arc<Table>> {
        opened.tables().get(name).cloned()
    }

    #[tokio::test]
    async fn test_open_with_builds_every_table_loading_with_no_get() {
        let first = two_file_fixture().await;
        let second = two_file_fixture().await;
        let config = config(vec![
            named(FIRST_TABLE, &first, Residency::Full),
            named(SECOND_TABLE, &second, Residency::Full),
        ]);
        let storage = storage_for(&config);
        let opened = open_with(&config, None, Arc::clone(&storage), FindingLog::Events)
            .await
            .unwrap();
        assert_eq!(names(&opened), [FIRST_TABLE, SECOND_TABLE]);
        for table in opened.tables().iter() {
            assert!(matches!(table.state(), TableState::Loading));
            assert!(table.current().is_none());
        }
        assert!(Arc::ptr_eq(&opened.storage, &storage));
        assert!(opened.local.is_none());
        assert_eq!(storage.stats().gets(), 0);
    }

    /// A budget `Config::parse` admits, one shard's minimum, is refused
    /// on a runtime of two workers, whose cache has two shards, before any
    /// table is opened.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_open_with_refuses_a_page_cache_below_the_runtime_shards_minimum() {
        let workers = 2;
        let fixture = two_file_fixture().await;
        let one_shard = cache::min_memory_bytes(1);
        let two_shards = cache::min_memory_bytes(workers);
        let mut config = config(vec![named(FIRST_TABLE, &fixture, Residency::Full)]);
        config.server.page_cache_bytes = approx::length(one_shard);
        let error = open_with(&config, None, storage_for(&config), FindingLog::Events)
            .await
            .err()
            .expect("a budget below two shards' minimum");
        assert!(
            matches!(
                error,
                Error::Config(ConfigError::Invalid(ConfigViolation::PageCacheTooSmall {
                    bytes,
                    minimum,
                    shards,
                })) if bytes == one_shard && minimum == two_shards && shards == workers
            ),
            "{error}"
        );
        config.server.page_cache_bytes = approx::length(two_shards);
        let opened = open_with(&config, None, storage_for(&config), FindingLog::Events)
            .await
            .unwrap();
        assert_eq!(names(&opened), [FIRST_TABLE]);
    }

    /// A call in flight on one table is counted where every other table
    /// of the process reads it: `open_with` gives them one count.
    #[tokio::test]
    async fn test_open_with_gives_every_table_one_count_of_calls_in_flight() {
        let first = two_file_fixture().await;
        let second = two_file_fixture().await;
        let config = config(vec![
            named(FIRST_TABLE, &first, Residency::Full),
            named(SECOND_TABLE, &second, Residency::Full),
        ]);
        let opened = open_with(&config, None, storage_for(&config), FindingLog::Events)
            .await
            .unwrap();
        let tables = opened.tables();
        let [first, second] = &tables[..] else {
            panic!("two tables open");
        };
        assert_eq!(second.calls_in_flight().sum(), 0);
        let held = first.calls_in_flight().enter();
        assert_eq!(second.calls_in_flight().sum(), 1);
        drop(held);
        assert_eq!(second.calls_in_flight().sum(), 0);
    }

    #[tokio::test]
    async fn test_open_with_only_narrows_by_name() {
        let first = two_file_fixture().await;
        let second = two_file_fixture().await;
        let config = config(vec![
            named(FIRST_TABLE, &first, Residency::Full),
            named(SECOND_TABLE, &second, Residency::Full),
        ]);
        let one = open_with(
            &config,
            Some(SECOND_TABLE),
            storage_for(&config),
            FindingLog::Events,
        )
        .await
        .unwrap();
        assert_eq!(names(&one), [SECOND_TABLE]);
        let none = open_with(
            &config,
            Some(NO_TABLE),
            storage_for(&config),
            FindingLog::Events,
        )
        .await
        .unwrap();
        assert!(none.tables().is_empty());
    }

    #[tokio::test]
    async fn test_open_with_local_directory_loads_disk_tables_into_chunk_files() {
        let first = two_file_fixture().await;
        let second = two_file_fixture().await;
        let dir = tempfile::tempdir().unwrap();
        let config = config_with_local(
            dir.path(),
            vec![
                named(FIRST_TABLE, &first, Residency::Disk),
                named(SECOND_TABLE, &second, Residency::Disk),
            ],
        );
        let opened = open_with(&config, None, storage_for(&config), FindingLog::Events)
            .await
            .unwrap();
        let local = opened.local.as_ref().unwrap();
        for table in opened.tables().iter() {
            table.load().await.unwrap();
            assert!(matches!(table.state(), TableState::Serving { .. }));
            let generation = table.current().unwrap();
            assert_eq!(generation.residency, Residency::Disk);
            let identities = generation.identities();
            assert_eq!(identities.len(), crate::approx::length(TWO));
            for identity in &identities {
                assert!(
                    local.chunks_for(identity).unwrap().is_some(),
                    "{} has a chunk file",
                    identity.path
                );
            }
        }
    }

    #[tokio::test]
    async fn test_retain_keeps_what_any_table_still_serves() {
        let mut fixture = two_file_fixture().await;
        let dir = tempfile::tempdir().unwrap();
        let config = config_with_local(
            dir.path(),
            vec![
                named(FIRST_TABLE, &fixture, Residency::Disk),
                named(SECOND_TABLE, &fixture, Residency::Disk),
            ],
        );
        let opened = open_with(&config, None, storage_for(&config), FindingLog::Events)
            .await
            .unwrap();
        for table in opened.tables().iter() {
            table.load().await.unwrap();
        }
        let local = opened.local.as_ref().unwrap();
        assert_eq!(opened.retain().await.unwrap(), 0);

        let before = opened.tables()[0].current().unwrap().identities();
        fixture.remove_file("f1");
        fixture
            .commit(
                SECOND_SNAPSHOT,
                Some(FIRST_SNAPSHOT),
                SECOND_SEQUENCE,
                SECOND_VERSION,
            )
            .await;
        assert!(opened.tables()[0].refresh().await.unwrap());
        let after = opened.tables()[0].current().unwrap().identities();
        let dropped: Vec<&DataFileIdentity> = before.difference(&after).collect();
        assert_eq!(dropped.len(), 1);
        assert!(local.chunks_for(dropped[0]).unwrap().is_some());
        assert_eq!(
            opened.retain().await.unwrap(),
            0,
            "the second table still serves the dropped file"
        );

        assert!(opened.tables()[1].refresh().await.unwrap());
        assert!(opened.retain().await.unwrap() > 0);
        assert!(local.chunks_for(dropped[0]).unwrap().is_none());
        assert_eq!(opened.retain().await.unwrap(), 0, "nothing left to free");
    }

    #[tokio::test]
    async fn test_retain_on_a_narrowed_open_frees_nothing() {
        let first = two_file_fixture().await;
        let second = two_file_fixture().await;
        let dir = tempfile::tempdir().unwrap();
        let config = config_with_local(
            dir.path(),
            vec![
                named(FIRST_TABLE, &first, Residency::Disk),
                named(SECOND_TABLE, &second, Residency::Disk),
            ],
        );
        let every = open_with(&config, None, storage_for(&config), FindingLog::Events)
            .await
            .unwrap();
        for table in every.tables().iter() {
            table.load().await.unwrap();
        }
        assert!(every.complete);
        let second_files = every.tables()[1].current().unwrap().identities();
        drop(every);

        let narrowed = open_with(
            &config,
            Some(FIRST_TABLE),
            storage_for(&config),
            FindingLog::Events,
        )
        .await
        .unwrap();
        assert!(!narrowed.complete);
        assert_eq!(names(&narrowed), [FIRST_TABLE]);
        narrowed.tables()[0].load().await.unwrap();
        assert_eq!(narrowed.retain().await.unwrap(), 0);
        let local = narrowed.local.as_ref().unwrap();
        for identity in &second_files {
            assert!(
                local.chunks_for(identity).unwrap().is_some(),
                "{} is live to the table left out",
                identity.path
            );
        }
    }

    /// The daemon runs retention on a spawned task after every load and
    /// swap, so `retain`'s future is `Send`; `tokio::spawn` refuses one
    /// that is not at compile time.
    #[tokio::test]
    async fn test_retain_is_spawnable() {
        let fixture = two_file_fixture().await;
        let dir = tempfile::tempdir().unwrap();
        let config = config_with_local(
            dir.path(),
            vec![named(FIRST_TABLE, &fixture, Residency::Disk)],
        );
        let opened = Arc::new(
            open_with(&config, None, storage_for(&config), FindingLog::Events)
                .await
                .unwrap(),
        );
        opened.tables()[0].load().await.unwrap();
        let retained = tokio::spawn(async move { opened.retain().await });
        assert_eq!(retained.await.unwrap().unwrap(), 0);
    }

    /// The name the reload tests add a table under.
    const THIRD_TABLE: &str = "c";

    /// `names` of `tables`.
    fn names_of(tables: &[Arc<Table>]) -> Vec<String> {
        tables
            .iter()
            .map(|table| table.name().to_string())
            .collect()
    }

    /// Opens `config` and loads every table.
    async fn loaded(config: &Config) -> Opened {
        let opened = open_with(config, None, storage_for(config), FindingLog::Events)
            .await
            .unwrap();
        for table in opened.tables().iter() {
            table.load().await.unwrap();
        }
        opened
    }

    /// A table whose source names a root with no table in it, so its
    /// first load fails.
    fn missing(name: &str, fixture: &Fixture) -> TableConfig {
        TableConfig {
            source: SourceConfig::Static {
                location: format!("{}/missing", fixture.root()),
            },
            ..named(name, fixture, Residency::Full)
        }
    }

    /// A table whose configuration is unchanged is kept as it serves; one
    /// no longer configured is dropped and withdrawn; one newly configured
    /// is loaded and swapped in serving, in configuration order.
    #[tokio::test]
    async fn test_apply_keeps_unchanged_tables_adds_new_ones_and_drops_the_rest() {
        let fixture = two_file_fixture().await;
        let first = named(FIRST_TABLE, &fixture, Residency::Full);
        let config = config(vec![
            first.clone(),
            named(SECOND_TABLE, &fixture, Residency::Full),
        ]);
        let opened = loaded(&config).await;
        let before = opened.tables();
        let serving = before[0].current().unwrap();

        let applied = opened
            .apply(&[named(THIRD_TABLE, &fixture, Residency::Full), first])
            .await
            .unwrap();
        assert_eq!(applied.kept, 1);
        assert_eq!(names_of(&applied.started), [THIRD_TABLE]);
        assert_eq!(names_of(&applied.stopped), [SECOND_TABLE]);
        assert_eq!(names(&opened), [THIRD_TABLE, FIRST_TABLE]);
        let kept = table_named(&opened, FIRST_TABLE).unwrap();
        assert!(Arc::ptr_eq(&kept, &before[0]));
        assert!(Arc::ptr_eq(&kept.current().unwrap(), &serving));
        assert!(matches!(
            table_named(&opened, THIRD_TABLE).unwrap().state(),
            TableState::Serving { .. }
        ));
        assert!(table_named(&opened, SECOND_TABLE).is_none());
        assert!(opened.pending.lock().unwrap().is_empty());
    }

    /// A table whose configuration changed is built and loaded as a new
    /// table under its name, and the one it replaces is dropped.
    #[tokio::test]
    async fn test_apply_replaces_a_changed_table_under_its_name() {
        let fixture = two_file_fixture().await;
        let first = named(FIRST_TABLE, &fixture, Residency::Full);
        let opened = loaded(&config(vec![first.clone()])).await;
        let before = table_named(&opened, FIRST_TABLE).unwrap();
        let changed = TableConfig {
            strict: !first.strict,
            ..first
        };

        let applied = opened.apply(std::slice::from_ref(&changed)).await.unwrap();
        assert_eq!(applied.kept, 0);
        assert_eq!(names_of(&applied.started), [FIRST_TABLE]);
        assert!(Arc::ptr_eq(&applied.stopped[0], &before));
        let after = table_named(&opened, FIRST_TABLE).unwrap();
        assert!(!Arc::ptr_eq(&after, &before));
        assert_eq!(after.config(), &changed);
        assert!(after.current().is_some());
    }

    /// A table that fails its first load refuses the whole reload: the set
    /// serving is the one before it, a table the reload would have added
    /// is dropped, and nothing is left pending.
    #[tokio::test]
    async fn test_apply_refuses_every_change_when_one_table_fails_to_load() {
        let fixture = two_file_fixture().await;
        let first = named(FIRST_TABLE, &fixture, Residency::Full);
        let opened = loaded(&config(vec![first.clone()])).await;
        let before = opened.tables();

        let refused = opened
            .apply(&[
                named(SECOND_TABLE, &fixture, Residency::Full),
                missing(THIRD_TABLE, &fixture),
            ])
            .await
            .err()
            .expect("the missing table fails its load");
        let failed: Vec<&str> = refused
            .failures
            .iter()
            .map(|(name, _)| name.as_str())
            .collect();
        assert_eq!(failed, [THIRD_TABLE]);
        assert!(Arc::ptr_eq(&opened.tables(), &before));
        assert!(table_named(&opened, SECOND_TABLE).is_none());
        assert!(opened.pending.lock().unwrap().is_empty());
        assert!(matches!(
            table_named(&opened, FIRST_TABLE).unwrap().state(),
            TableState::Serving { .. }
        ));
    }

    /// A reload dropped while its new table loads leaves nothing behind:
    /// nothing pending, and the set serving as it was.
    #[tokio::test]
    async fn test_apply_dropped_while_loading_leaves_the_set_as_it_was() {
        let fixture = two_file_fixture().await;
        let first = named(FIRST_TABLE, &fixture, Residency::Full);
        let opened = loaded(&config(vec![first.clone()])).await;
        let before = opened.tables();
        let configs = [first, named(SECOND_TABLE, &fixture, Residency::Full)];
        // Every read from here on stalls, so the new table's load is
        // waiting when the reload is dropped; a plain local read can finish
        // within the first poll and let the whole reload complete.
        opened.storage.register(
            FILE_PREFIX,
            Arc::new(ThrottledStore::new(
                LocalFileSystem::new(),
                ThrottleConfig {
                    wait_get_per_call: STALL,
                    ..ThrottleConfig::default()
                },
            )),
        );

        let mut applying = Box::pin(opened.apply(&configs));
        assert!(
            futures::poll!(applying.as_mut()).is_pending(),
            "the new table's load waits on its reads"
        );
        assert_eq!(
            opened.pending.lock().unwrap().len(),
            1,
            "the reload registered its table"
        );
        drop(applying);
        assert!(opened.pending.lock().unwrap().is_empty());
        assert!(Arc::ptr_eq(&opened.tables(), &before));
        assert!(table_named(&opened, SECOND_TABLE).is_none());
    }

    /// `retain` waits for the load lock of a table still loading for a
    /// reload before it computes its live set, and the chunk files that
    /// table wrote are live until the reload lets it go, and freed after.
    #[tokio::test]
    async fn test_retain_waits_for_and_keeps_what_a_pending_table_loads() {
        let first = two_file_fixture().await;
        let second = two_file_fixture().await;
        let dir = tempfile::tempdir().unwrap();
        let config = config_with_local(
            dir.path(),
            vec![named(FIRST_TABLE, &first, Residency::Disk)],
        );
        let opened = loaded(&config).await;
        let local = opened.local.as_ref().unwrap();
        let pending = Arc::new(
            opened
                .build(&named(SECOND_TABLE, &second, Residency::Disk))
                .await
                .unwrap(),
        );
        let registered = opened.register(std::slice::from_ref(&pending)).await;
        let loading = pending.serialized().await;
        assert!(
            opened.retain().now_or_never().is_none(),
            "retain waits for the pending table's load"
        );
        drop(loading);
        pending.load().await.unwrap();
        let written = pending.current().unwrap().identities();

        assert_eq!(opened.retain().await.unwrap(), 0);
        for identity in &written {
            assert!(local.chunks_for(identity).unwrap().is_some());
        }
        drop(registered);
        assert!(opened.retain().await.unwrap() > 0);
        for identity in &written {
            assert!(local.chunks_for(identity).unwrap().is_none());
        }
    }

    /// A reload that drops a disk table lets `retain` free its chunk files.
    #[tokio::test]
    async fn test_retain_after_apply_frees_a_dropped_table_s_chunk_files() {
        let first = two_file_fixture().await;
        let second = two_file_fixture().await;
        let dir = tempfile::tempdir().unwrap();
        let kept = named(FIRST_TABLE, &first, Residency::Disk);
        let config = config_with_local(
            dir.path(),
            vec![kept.clone(), named(SECOND_TABLE, &second, Residency::Disk)],
        );
        let opened = loaded(&config).await;
        let dropped = table_named(&opened, SECOND_TABLE)
            .unwrap()
            .current()
            .unwrap()
            .identities();

        opened.apply(&[kept]).await.unwrap();
        assert!(opened.retain().await.unwrap() > 0);
        let local = opened.local.as_ref().unwrap();
        for identity in &dropped {
            assert!(local.chunks_for(identity).unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn test_retain_without_local_directory_is_zero() {
        let fixture = two_file_fixture().await;
        let config = config(vec![named(FIRST_TABLE, &fixture, Residency::Full)]);
        let opened = open_with(&config, None, storage_for(&config), FindingLog::Events)
            .await
            .unwrap();
        opened.tables()[0].load().await.unwrap();
        assert_eq!(opened.retain().await.unwrap(), 0);
    }
}
