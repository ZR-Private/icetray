//! Assertions shared by the acceptance tests (RFD 014): one table served
//! over its own storage and cache, the cost oracle on `Generation::plan`,
//! the hit and miss checks that compare a lookup's answer with the
//! fixture's derivation and its GETs with the oracle's count, and the
//! reader of one series out of a metrics scrape.

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;

use iceberg::Catalog;
use icetray_core::approx::count;
use icetray_core::config::{HedgeConfig, LocalConfig, ServerConfig, StorageConfig, TableConfig};
use icetray_core::{
    CallsInFlight, FindingLog, Generation, Key, LocalDir, PageCache, Residency, Row, Storage,
    Table, TableHandles, TableSource,
};

use crate::error::{Precondition, Result};
use crate::fixture::Fixture;
use crate::keys::far_key;
use crate::layout::VALUE_COLUMN;

/// The page cache budget of a served table: large enough that nothing a
/// test touches is evicted, so a repeat batch is served from memory and a
/// cold count is exactly the distinct pages a batch fetches.
pub const TEST_PAGE_CACHE_BYTES: usize = 64 * 1024 * 1024;

/// Load GETs past the per-file ones and the metadata file
/// (`METADATA_FILE_GETS`): the manifest list and the one manifest a
/// fixture's first snapshot has.
pub const LOAD_METADATA_GETS: u64 = 2;

/// The GET every source spends on the metadata file at a location it has
/// not read before `LOAD_METADATA_GETS`: a static source parses it, and a
/// catalog source, whose client hands over its own parse, reads it for
/// the column defaults that parse drops (RFD 023).
pub const METADATA_FILE_GETS: u64 = 1;

/// Load GETs per single-row-group file in every tier: the footer and the
/// key chunk. The Bloom tier reads one filter per row group besides.
pub const LOAD_GETS_PER_FILE: u64 = 2;

/// An unchanged poll of a static root reads its version hint and nothing
/// else.
pub const HINT_POLL_GETS: u64 = 1;

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
/// page cache on `local`'s disk level when there is one, a count of calls
/// in flight of its own, and the table in `Loading`. A test that asserts on a refused table's state or needs a
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
        local.as_ref().and_then(|local| local.page_cache_level()),
    )
    .await?;
    let handles = TableHandles {
        storage: Arc::clone(&storage),
        cache: cache.clone(),
        local,
        calls: CallsInFlight::new(),
        findings: FindingLog::Events,
    };
    let server = ServerConfig::default();
    let table = Table::new(
        config,
        source,
        handles,
        server.load_deadline,
        server.refresh_deadline,
        server.get_deadline,
    );
    Ok(Served {
        storage,
        cache,
        table,
    })
}

/// `build` of `config` from `source` over a fresh hedge-free storage
/// (`storage_config_for_tests`), so every GET counted is one a load or a
/// lookup planned, with a `cache_bytes` page cache and, when
/// `config.residency` is Disk, a local directory at `dir` over the
/// storage's counters, with no index budget and no disk page cache; not
/// loaded. The opening of a test that serves a fixture in each residency.
pub async fn build_in(
    config: TableConfig,
    source: TableSource,
    dir: &Path,
    cache_bytes: usize,
) -> Result<Served> {
    let storage = Arc::new(Storage::new(storage_config_for_tests()));
    let local = match config.residency {
        Residency::Disk => Some(Arc::new(LocalDir::open(
            &LocalConfig {
                dir: dir.to_path_buf(),
                index_budget_bytes: None,
                page_cache_disk_bytes: 0,
            },
            storage.stats(),
        )?)),
        Residency::Full | Residency::Bloom => None,
    };
    build(storage, config, source, local, cache_bytes).await
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

/// The GETs a cold batch of hits on `keys` naming the value columns
/// `columns` costs in the Full tier (RFD 022): the distinct `(file, column,
/// value page offset)` triples `generation.plan` names, one GET per value
/// page of each column named; a dictionary-encoded column's dictionary page
/// is not counted. A key the plan names no row for costs nothing in the Full
/// tier, where a resident key page shows it absent, and neither does a
/// column a file answers by its default (RFD 023); in any other tier the
/// oracle cannot name value pages without I/O, which is
/// `Precondition::OracleNotFull`.
///
/// Panics on a column the generation does not serve.
pub fn expected_cold_gets(generation: &Generation, keys: &[Key], columns: &[&str]) -> Result<u64> {
    if generation.residency != Residency::Full {
        return Err(Precondition::OracleNotFull {
            residency: generation.residency,
        }
        .into());
    }
    let positions = column_positions(generation, columns);
    let mut pages = HashSet::new();
    for (file, page) in generation.plan(keys).into_iter().flatten() {
        for &column in &positions {
            if let Some(offset) = file.index.value_page_offset(column, &page) {
                pages.insert((file.entry.path.clone(), column, offset));
            }
        }
    }
    Ok(count(pages.len()))
}

/// The value a lookup naming one column answers for a present key, when it
/// is not null: `None` for an absent key, a null value, or a row that does
/// not carry exactly one value.
pub fn one_value(row: &Row) -> Option<&Bytes> {
    match row {
        Row::Present(values) => match values.as_slice() {
            [value] => value.as_ref(),
            _ => None,
        },
        Row::Absent => None,
    }
}

/// The positions in `generation`'s layout of the value columns `columns`
/// names.
///
/// Panics on a column the generation does not serve.
pub fn column_positions(generation: &Generation, columns: &[&str]) -> Vec<usize> {
    columns
        .iter()
        .map(|&column| {
            generation
                .layout
                .value_column(column)
                .unwrap_or_else(|| panic!("the generation serves no column {column}"))
        })
        .collect()
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

/// The pages a sequence of lookups has fetched, so `expected_lookup_gets`
/// counts each once.
#[derive(Default)]
pub struct SeenPages {
    value_pages: HashSet<(String, u64)>,
    key_pages: HashSet<(String, usize, usize)>,
}

/// The GETs a lookup of `key` costs in `generation`'s tier after the
/// lookups `seen` records, for a key with a live copy when `live` (RFD
/// 021), naming value column `column`: its value page the Full tier's
/// generation `full` plans for the live copy, unless fetched before or its
/// file answers the column's default and reads none (RFD 023); and in the
/// Bloom tier every key page a probe decodes, walking the candidates in
/// probe order up to the file the plan names, or through every candidate
/// for a key with no live copy, deleted copies among them. The Full and
/// Disk tiers decode key pages from memory and from local disk.
pub fn expected_lookup_gets(
    full: &Generation,
    generation: &Generation,
    column: &str,
    key: &Key,
    live: bool,
    seen: &mut SeenPages,
) -> u64 {
    let mut gets = 0;
    let mut live_path = None;
    if live {
        let (file, page) = full
            .plan(std::slice::from_ref(key))
            .into_iter()
            .next()
            .flatten()
            .expect("the plan names a live key's file");
        let [column] = column_positions(full, &[column])[..] else {
            unreachable!("one name is one position");
        };
        if file.index.value_leaf(column).is_some() {
            let offset = file
                .index
                .value_page_offset(column, &page)
                .expect("the Full tier names the value page of a file that holds the column");
            gets += u64::from(seen.value_pages.insert((file.entry.path.clone(), offset)));
        }
        live_path = Some(file.entry.path.clone());
    }
    if generation.residency == Residency::Bloom {
        for candidate in generation.candidates(key) {
            if let Some(page) = candidate.index.page_of(key) {
                gets += u64::from(seen.key_pages.insert((
                    candidate.entry.path.clone(),
                    page.row_group,
                    page.key_page,
                )));
            }
            if live_path.as_ref() == Some(&candidate.entry.path) {
                break;
            }
        }
    }
    gets
}

/// Looks up the fixture key at every id naming `VALUE_COLUMN`, checking
/// each answer against the fixture's derivation (`Present` bytes compared
/// whole, or a null) and its snapshot against the serving generation's;
/// returns the GETs the batch cost.
pub async fn verify_hits(served: &Served, fixture: &Fixture, ids: &[u64]) -> Result<u64> {
    let stats = served.storage.stats();
    let before = stats.gets();
    let generation = served.current();
    for &id in ids {
        let key = fixture.key(id);
        let lookup = served.table.get(&key, &[VALUE_COLUMN]).await?;
        assert_eq!(lookup.row, fixture.expected(&key), "value for key {key}");
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
    let expected = expected_cold_gets(&served.current(), &fixture.keys_at(ids), &[VALUE_COLUMN])?;
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
            served.table.get(&key, &[VALUE_COLUMN]).await?.row,
            Row::Absent,
            "key {key}"
        );
    }
    let far = far_key(fixture.spec().key_type);
    assert_eq!(
        served.table.get(&far, &[VALUE_COLUMN]).await?.row,
        Row::Absent,
        "key {far}"
    );
    assert_eq!(stats.gets(), before, "misses touch no storage");
    Ok(())
}

/// The value of the first series `name` whose labels include every pair
/// in `labels`, read from a Prometheus text exposition (`name{label="value",
/// ..} value`), or `None` when no such series is exposed: a counter appears
/// only after its first count, so a reading before it is zero to the
/// caller. The value is the float the format defines, so a gauge's fraction
/// or a counter's integer reads the same way. The whole name must match, so
/// `requests_total` does not read a `requests_total_bucket` line.
///
/// Panics on a matching line whose value is not a float, which a
/// conforming exposition does not hold, so a malformed value fails the test
/// rather than reading as absent.
pub fn series_value(exposition: &str, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
    matching_series(exposition, name, labels).next()
}

/// The sum of every series `name` whose labels include every pair in
/// `labels`, as `series_value` matches them: a counter's total across the
/// labels left out, such as `lookups_total` of one table over every
/// `result`. Zero when none is exposed. Panics as `series_value` does.
pub fn series_sum(exposition: &str, name: &str, labels: &[(&str, &str)]) -> f64 {
    // A fold from positive zero: `Sum` of no floats is negative zero.
    matching_series(exposition, name, labels).fold(0.0, |sum, value| sum + value)
}

/// The values of the series `series_value` and `series_sum` match, in
/// exposition order.
fn matching_series<'a>(
    exposition: &'a str,
    name: &'a str,
    labels: &'a [(&str, &str)],
) -> impl Iterator<Item = f64> + 'a {
    exposition.lines().filter_map(move |line| {
        let rest = line.strip_prefix(name)?;
        let (label_text, value) = match rest.strip_prefix('{') {
            Some(labeled) => labeled.split_once('}')?,
            None => ("", rest.strip_prefix(' ')?),
        };
        let present: Vec<(&str, &str)> = label_text
            .split(',')
            .filter_map(|pair| {
                let (label, quoted) = pair.split_once('=')?;
                Some((label, quoted.trim_matches('"')))
            })
            .collect();
        if !labels.iter().all(|pair| present.contains(pair)) {
            return None;
        }
        let parsed = value
            .split_whitespace()
            .next()
            .and_then(|sample| sample.parse().ok());
        Some(parsed.unwrap_or_else(|| panic!("the sample value of `{line}` is not a float")))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The values the sample exposition carries, one per series.
    const GETS: f64 = 3.0;
    const MULTI_GETS: f64 = 1.0;
    const SERVING: f64 = 1.0;
    const RSS: f64 = 12_345.0;
    const AGE: f64 = 2.5;

    /// A scrape as the exporter renders it: a comment line, labeled
    /// counters, a two-label gauge, and an unlabeled gauge.
    fn exposition() -> String {
        format!(
            "# TYPE requests_total counter
requests_total{{call=\"get\"}} {GETS}
requests_total{{call=\"multi_get\"}} {MULTI_GETS}
table_state{{table=\"a\",state=\"serving\"}} {SERVING}
process_rss_bytes {RSS}
generation_age_seconds{{table=\"a\"}} {AGE}
"
        )
    }

    #[test]
    fn test_series_value_reads_by_name_and_labels_in_any_order() {
        let scrape = exposition();
        assert_eq!(
            series_value(&scrape, "requests_total", &[("call", "get")]),
            Some(GETS)
        );
        assert_eq!(
            series_value(&scrape, "requests_total", &[("call", "multi_get")]),
            Some(MULTI_GETS)
        );
        assert_eq!(
            series_value(&scrape, "requests_total", &[]),
            Some(GETS),
            "no label constraint reads the first series of the name"
        );
        assert_eq!(
            series_value(
                &scrape,
                "table_state",
                &[("state", "serving"), ("table", "a")]
            ),
            Some(SERVING)
        );
        assert_eq!(series_value(&scrape, "process_rss_bytes", &[]), Some(RSS));
    }

    #[test]
    fn test_series_sum_adds_every_series_that_matches() {
        let scrape = exposition();
        assert_eq!(
            series_sum(&scrape, "requests_total", &[]).to_bits(),
            (GETS + MULTI_GETS).to_bits()
        );
        assert_eq!(
            series_sum(&scrape, "requests_total", &[("call", "get")]).to_bits(),
            GETS.to_bits()
        );
        assert_eq!(
            series_sum(&scrape, "requests_total", &[("call", "table_info")]).to_bits(),
            0.0_f64.to_bits(),
            "nothing exposed sums to zero"
        );
    }

    #[test]
    fn test_series_value_reads_a_fractional_value_as_present() {
        let scrape = exposition();
        assert_eq!(
            series_value(&scrape, "generation_age_seconds", &[("table", "a")]),
            Some(AGE)
        );
    }

    #[test]
    #[should_panic(expected = "is not a float")]
    fn test_series_value_panics_on_a_value_that_is_not_a_float() {
        series_value(
            "requests_total{call=\"get\"} three\n",
            "requests_total",
            &[],
        );
    }

    #[test]
    fn test_series_value_is_none_for_an_absent_series_or_a_name_prefix() {
        let scrape = exposition();
        assert_eq!(
            series_value(&scrape, "requests_total", &[("call", "table_info")]),
            None
        );
        assert_eq!(
            series_value(&scrape, "table_state", &[("table", "b")]),
            None
        );
        assert_eq!(series_value(&scrape, "requests", &[]), None);
        assert_eq!(series_value(&scrape, "process_rss", &[]), None);
    }
}
