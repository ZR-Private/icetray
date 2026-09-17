//! Assertions shared by the acceptance tests (RFD 014): one table served
//! over its own storage and cache, the cost oracle on `Generation::plan`,
//! and the hit and miss checks that compare a lookup's answer with the
//! fixture's derivation and its GETs with the oracle's count.

use std::collections::HashSet;
use std::sync::Arc;

use iceberg::Catalog;
use icetray_core::config::{HedgeConfig, StorageConfig, TableConfig};
use icetray_core::{
    Generation, Key, LocalDir, PageCache, Residency, Storage, Table, TableSource, Value,
};

use crate::error::{Precondition, Result};
use crate::fixture::Fixture;
use crate::keys::far_key;

/// The page cache budget of a served table: large enough that nothing a
/// test touches is evicted, so a repeat batch is served from memory and a
/// cold count is exactly the distinct pages a batch fetches.
pub const TEST_PAGE_CACHE_BYTES: usize = 64 * 1024 * 1024;

/// One table over its own storage, so its counters are its own, and its own
/// cache. `build` hands it back unloaded; `serve_over` and `serve` loaded.
pub struct Served {
    /// The storage every read of the table goes through; its `stats` are
    /// the GET oracle.
    pub storage: Arc<Storage>,
    /// The table's page cache; its counters say whether it is cold.
    pub cache: PageCache,
    /// The table.
    pub table: Table,
}

impl Served {
    /// Runs the table's first load and hands the table back serving; a
    /// refused load comes back as `TestkitError::Core`, which the refusal
    /// tests match on.
    pub async fn load(self) -> Result<Self> {
        self.table.load().await?;
        Ok(self)
    }

    /// The serving generation of a loaded table; a failed refresh keeps the
    /// previous one.
    pub fn current(&self) -> Arc<Generation> {
        self.table
            .current()
            .expect("a Served table was loaded and a failed refresh keeps its generation")
    }
}

/// `config` from `source` over `storage`, not yet loaded: a `cache_bytes`
/// page cache on `local`'s page tier when there is one, and the table in
/// `Loading`. A test that asserts on a refused table's state or needs a
/// cache of another size builds here and calls `Served::load` itself. The
/// disk-tier tests build the storage first and open their `LocalDir` over
/// its `stats`, so the same counters see the local reads.
pub async fn build(
    storage: Arc<Storage>,
    config: TableConfig,
    source: TableSource,
    local: Option<Arc<LocalDir>>,
    cache_bytes: usize,
) -> Result<Served> {
    let cache = PageCache::new(
        cache_bytes,
        local.as_ref().and_then(|local| local.page_tier()),
    )
    .await?;
    let table = Table::new(config, source, Arc::clone(&storage), cache.clone(), local);
    Ok(Served {
        storage,
        cache,
        table,
    })
}

/// `build` with a `TEST_PAGE_CACHE_BYTES` cache, loaded.
pub async fn serve_over(
    storage: Arc<Storage>,
    config: TableConfig,
    source: TableSource,
    local: Option<Arc<LocalDir>>,
) -> Result<Served> {
    build(storage, config, source, local, TEST_PAGE_CACHE_BYTES)
        .await?
        .load()
        .await
}

/// `serve_over` a fresh `Storage::new(storage)` with no local directory.
pub async fn serve(
    config: TableConfig,
    source: TableSource,
    storage: StorageConfig,
) -> Result<Served> {
    serve_over(Arc::new(Storage::new(storage)), config, source, None).await
}

/// `fixture` served in `residency` through `catalog` over hedge-free storage
/// (`storage_config_for_tests`), the opening of every in-process test that
/// follows a catalog.
pub async fn serve_catalog(
    fixture: &Fixture,
    catalog: &Arc<dyn Catalog>,
    residency: Residency,
    strict: bool,
) -> Result<Served> {
    serve_catalog_with(
        fixture,
        catalog,
        residency,
        strict,
        storage_config_for_tests(),
    )
    .await
}

/// `serve_catalog` over `storage`, for a fixture whose files live behind
/// the in-process S3 server.
pub async fn serve_catalog_with(
    fixture: &Fixture,
    catalog: &Arc<dyn Catalog>,
    residency: Residency,
    strict: bool,
    storage: StorageConfig,
) -> Result<Served> {
    serve(
        fixture.config(residency, strict),
        fixture.catalog_source(Arc::clone(catalog)),
        storage,
    )
    .await
}

/// The default storage configuration with hedging disabled, so a batch
/// larger than the hedge window can never add a hedge GET to a count the
/// oracle predicts. Every in-process cost test serves under it or under
/// `S3Fixture::storage_config`, which is hedge-disabled too.
pub fn storage_config_for_tests() -> StorageConfig {
    StorageConfig {
        hedge: HedgeConfig {
            enabled: false,
            ..HedgeConfig::default()
        },
        ..StorageConfig::default()
    }
}

/// The GETs a cold batch of hits on `keys` costs in the Full tier: the
/// distinct `(file, value page offset)` pairs `generation.plan` names. A key
/// the plan names no value page for costs nothing in the Full tier, where a
/// resident key page shows it absent; in any other tier the oracle cannot
/// name value pages without I/O, which is `Precondition::OracleNotFull`.
pub fn expected_cold_gets(generation: &Generation, keys: &[Key]) -> Result<u64> {
    let mut pages = HashSet::new();
    for (file, page) in generation.plan(keys).into_iter().flatten() {
        match page.value_page_offset {
            Some(offset) => {
                pages.insert((file.entry.path.clone(), offset));
            }
            None if generation.residency == Residency::Full => {}
            None => {
                return Err(Precondition::OracleNotFull {
                    residency: generation.residency,
                }
                .into());
            }
        }
    }
    Ok(count(pages.len()))
}

/// The distinct key pages `generation.plan` names for `keys`: the key-page
/// GETs a Bloom-tier cold batch adds to the value pages.
pub fn expected_key_pages(generation: &Generation, keys: &[Key]) -> u64 {
    let pages: HashSet<(String, usize, usize)> = generation
        .plan(keys)
        .into_iter()
        .flatten()
        .map(|(file, page)| (file.entry.path.clone(), page.row_group, page.key_page))
        .collect();
    count(pages.len())
}

/// Whether `generation.plan` would decode a key page for `key`: for a
/// Bloom-tier generation, whether a miss pays one key-page GET (a filter
/// positive) or nothing; for a Disk-tier one, whether it pays a local read.
pub fn predicted_probe(generation: &Generation, key: &Key) -> bool {
    generation
        .plan(std::slice::from_ref(key))
        .first()
        .is_some_and(Option::is_some)
}

/// Looks up the fixture key at every id, checking each answer against the
/// fixture's derivation (`Present` bytes compared whole, or `Null`) and its
/// snapshot against the serving generation's; returns the GETs the batch
/// cost.
pub async fn verify_hits(served: &Served, fixture: &Fixture, ids: &[u64]) -> Result<u64> {
    let stats = served.storage.stats();
    let before = stats.gets();
    let generation = served.current();
    for &id in ids {
        let key = fixture.key(id);
        let lookup = served.table.get(&key).await?;
        assert_eq!(lookup.value, fixture.expected(&key), "value for key {key}");
        assert_eq!(
            lookup.snapshot_id, generation.snapshot_id,
            "snapshot for key {key}"
        );
    }
    Ok(stats.gets() - before)
}

/// Cold hits cost exactly the value pages the oracle names, warm hits none:
/// requires a cold cache (else `Precondition::CacheNotCold`), takes the
/// oracle's count, which must be above zero, runs `verify_hits` twice, and
/// hands the table back warm.
pub async fn verify_hit_cost(served: Served, fixture: &Fixture, ids: &[u64]) -> Result<Served> {
    let (hits, fetches) = (served.cache.hits(), served.cache.fetches());
    if hits > 0 || fetches > 0 {
        return Err(Precondition::CacheNotCold { hits, fetches }.into());
    }
    let expected = expected_cold_gets(&served.current(), &fixture.keys_at(ids))?;
    assert!(expected > 0, "the sample names no value page");
    let cold = verify_hits(&served, fixture, ids).await?;
    assert_eq!(
        cold, expected,
        "cold hits cost one GET per distinct value page"
    );
    let warm = verify_hits(&served, fixture, ids).await?;
    assert_eq!(warm, 0, "repeat hits are served from the page cache");
    Ok(served)
}

/// The absent key near every id and the far key are `Absent` and the GET
/// counter does not move: the Full and Disk tiers' miss cost. The Bloom
/// tests price misses through `predicted_probe` instead.
pub async fn verify_misses(served: &Served, fixture: &Fixture, ids: &[u64]) -> Result<()> {
    let stats = served.storage.stats();
    let before = stats.gets();
    for &id in ids {
        let key = fixture.absent_near(id);
        assert_eq!(
            served.table.get(&key).await?.value,
            Value::Absent,
            "key {key}"
        );
    }
    let far = far_key(fixture.spec().key_type);
    assert_eq!(
        served.table.get(&far).await?.value,
        Value::Absent,
        "key {far}"
    );
    assert_eq!(stats.gets(), before, "misses touch no storage");
    Ok(())
}

/// A length or count as the GET totals, row counts, and byte sizes take it:
/// the one `usize` to `u64` conversion in the crate.
pub fn count(n: usize) -> u64 {
    u64::try_from(n).expect("a count fits u64 on the supported systems")
}
