//! Opening every configured table (RFD 012): one storage, one page cache,
//! the optional local directory, one count of calls in flight, and every
//! table in `Loading` over them, built the
//! same way for the daemon and the one-shot subcommands, so a subcommand
//! reads a table exactly as `serve` does. Loading is the caller's step.

use std::collections::HashSet;
use std::sync::Arc;

use crate::cache::{self, PageCache};
use crate::catalog::TableSource;
use crate::config::{Config, ConfigError};
use crate::error::{Error, Result};
use crate::local::{DataFileIdentity, LocalDir};
use crate::storage::Storage;
use crate::table::{CallsInFlight, Table, TableHandles};

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
    /// The tables, in configuration order, each in `Loading`.
    pub tables: Vec<Arc<Table>>,
    /// Whether `tables` is every configured table: `only` narrowed nothing,
    /// or named every one. `retain`'s live set is complete only then.
    complete: bool,
}

impl Opened {
    /// Deletes the chunk files no table's serving generation references and
    /// returns the bytes freed; `Ok(0)` with no local directory. The live
    /// set is the union of every table's `Generation::identities` (RFD
    /// 017), computed and applied while every table's load lock is held,
    /// in `tables` order, so no load is in flight anywhere meanwhile. An
    /// `Opened` that `only` narrowed frees nothing: the tables it left out
    /// share the directory, and their chunk files are live to them. This
    /// is the one place chunk files are deleted: the serve loop calls it
    /// after every table's first `load` and after any `refresh` that
    /// swapped, and a one-shot subcommand may call it before exit.
    ///
    /// Every lock is taken because a load in flight has written chunk files
    /// that no serving generation references yet, and a live set computed
    /// beside it would delete them from under the generation about to swap
    /// in. The locks are taken one at a time in `tables` order, so two
    /// calls at once cannot each hold a lock the other waits for. The cost
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
        let mut held = Vec::with_capacity(self.tables.len());
        for table in &self.tables {
            held.push(table.serialized().await);
        }
        let live: HashSet<DataFileIdentity> = self
            .tables
            .iter()
            .filter_map(|table| table.current())
            .flat_map(|generation| generation.identities())
            .collect();
        let freed = local.retain(&live)?;
        drop(held);
        Ok(freed)
    }
}

/// Builds storage from the SDK's default chain, one cache, one count of
/// calls in flight, and every configured table in `Loading` over them; `only` narrows by name, and a name that
/// matches no table yields `Ok` with empty `tables`, whose message and exit
/// code are the caller's. Nothing is read from any table: `open` issues no
/// GET.
pub async fn open(config: &Config, only: Option<&str>) -> Result<Opened> {
    let storage = Arc::new(Storage::with_sdk_defaults(config.storage.clone()).await);
    open_with(config, only, storage).await
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
    };
    let mut tables = Vec::with_capacity(config.tables.len());
    for table in config
        .tables
        .iter()
        .filter(|table| only.is_none_or(|name| table.name == name))
    {
        let source = TableSource::from_config(&table.source).await?;
        tables.push(Arc::new(Table::new(
            table.clone(),
            source,
            handles.clone(),
            config.server.load_deadline,
            config.server.refresh_deadline,
            config.server.get_deadline,
        )));
    }
    let complete = tables.len() == config.tables.len();
    Ok(Opened {
        storage,
        cache,
        local,
        tables,
        complete,
    })
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::approx;
    use crate::config::{
        ConfigViolation, LocalConfig, Residency, ServerConfig, StorageConfig, TableConfig,
    };
    use crate::generation::testing::{
        FIRST_SNAPSHOT, SECOND_SEQUENCE, SECOND_SNAPSHOT, SECOND_VERSION, TWO, Table as Fixture,
        two_bucket_table,
    };
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

    fn names(opened: &Opened) -> Vec<&str> {
        opened.tables.iter().map(|table| table.name()).collect()
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
        let opened = open_with(&config, None, Arc::clone(&storage))
            .await
            .unwrap();
        assert_eq!(names(&opened), [FIRST_TABLE, SECOND_TABLE]);
        for table in &opened.tables {
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
        let error = open_with(&config, None, storage_for(&config))
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
        let opened = open_with(&config, None, storage_for(&config))
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
        let opened = open_with(&config, None, storage_for(&config))
            .await
            .unwrap();
        let [first, second] = opened.tables.as_slice() else {
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
        let one = open_with(&config, Some(SECOND_TABLE), storage_for(&config))
            .await
            .unwrap();
        assert_eq!(names(&one), [SECOND_TABLE]);
        let none = open_with(&config, Some(NO_TABLE), storage_for(&config))
            .await
            .unwrap();
        assert!(none.tables.is_empty());
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
        let opened = open_with(&config, None, storage_for(&config))
            .await
            .unwrap();
        let local = opened.local.as_ref().unwrap();
        for table in &opened.tables {
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
        let opened = open_with(&config, None, storage_for(&config))
            .await
            .unwrap();
        for table in &opened.tables {
            table.load().await.unwrap();
        }
        let local = opened.local.as_ref().unwrap();
        assert_eq!(opened.retain().await.unwrap(), 0);

        let before = opened.tables[0].current().unwrap().identities();
        fixture.remove_file("f1");
        fixture
            .commit(
                SECOND_SNAPSHOT,
                Some(FIRST_SNAPSHOT),
                SECOND_SEQUENCE,
                SECOND_VERSION,
            )
            .await;
        assert!(opened.tables[0].refresh().await.unwrap());
        let after = opened.tables[0].current().unwrap().identities();
        let dropped: Vec<&DataFileIdentity> = before.difference(&after).collect();
        assert_eq!(dropped.len(), 1);
        assert!(local.chunks_for(dropped[0]).unwrap().is_some());
        assert_eq!(
            opened.retain().await.unwrap(),
            0,
            "the second table still serves the dropped file"
        );

        assert!(opened.tables[1].refresh().await.unwrap());
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
        let every = open_with(&config, None, storage_for(&config))
            .await
            .unwrap();
        for table in &every.tables {
            table.load().await.unwrap();
        }
        assert!(every.complete);
        let second_files = every.tables[1].current().unwrap().identities();
        drop(every);

        let narrowed = open_with(&config, Some(FIRST_TABLE), storage_for(&config))
            .await
            .unwrap();
        assert!(!narrowed.complete);
        assert_eq!(names(&narrowed), [FIRST_TABLE]);
        narrowed.tables[0].load().await.unwrap();
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
            open_with(&config, None, storage_for(&config))
                .await
                .unwrap(),
        );
        opened.tables[0].load().await.unwrap();
        let retained = tokio::spawn(async move { opened.retain().await });
        assert_eq!(retained.await.unwrap().unwrap(), 0);
    }

    #[tokio::test]
    async fn test_retain_without_local_directory_is_zero() {
        let fixture = two_file_fixture().await;
        let config = config(vec![named(FIRST_TABLE, &fixture, Residency::Full)]);
        let opened = open_with(&config, None, storage_for(&config))
            .await
            .unwrap();
        opened.tables[0].load().await.unwrap();
        assert_eq!(opened.retain().await.unwrap(), 0);
    }
}
