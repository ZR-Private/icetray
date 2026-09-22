//! The read path under deterministic simulation (the S tier of RFD 014):
//! the real S3 client and server over a simulated network, latency injected
//! per link, virtual time as the oracle. A cold hit is one round trip, a
//! load a fixed number of dependent ones, the same across runs of one seed;
//! hedging trims an injected tail and stays under its ceiling without one;
//! load reads never hedge (RFD 013).

use std::ops::Range;
use std::path::Path;
use std::sync::{Arc, mpsc};
use std::time::Duration;

use iceberg::memory::MemoryCatalog;
use icetray_core::approx;
use icetray_core::config::{HedgeConfig, Residency, RewriteRule, StorageConfig, TableConfig};
use icetray_core::{Key, KeyType, Storage, TableSource, Value};
use icetray_testkit::fixture::{APPENDED_4K_6K, IDS_4K, NAMESPACE, NEW_ID};
use icetray_testkit::sim::{SlowEvery, serve_s3_with, sim_s3_store};
use icetray_testkit::verify::{
    self, LOAD_GETS_PER_FILE, LOAD_METADATA_GETS, METADATA_FILE_GETS, Served,
};
use icetray_testkit::{Fixture, FixtureSpec, Result, memory_catalog_local, write_fixture};
use tempfile::TempDir;

/// The simulated hosts, the bucket the S3 host serves from its root, and the
/// warehouse under it the fixture is written into.
const S3_HOST: &str = "s3";
const CLIENT_HOST: &str = "icetray";
const BUCKET: &str = "bucket";
const WAREHOUSE: &str = "warehouse";

/// Virtual time a simulation may run before it is declared stuck.
const SIM_DURATION: Duration = Duration::from_secs(600);

/// The seed two cold runs share, whose virtual timings must then agree;
/// any value serves.
const SEED_FIRST: u64 = 7;

/// A seed other than `SEED_FIRST`: the GET counts must not depend on it.
const SEED_SECOND: u64 = 11;

/// One-way link latency when it is fixed; a round trip is twice it.
const LINK_LATENCY: Duration = Duration::from_millis(5);
const ROUND_TRIP: Duration = LINK_LATENCY.saturating_mul(2);

/// The bounds of the per-message latency when it varies, so round trips
/// vary and a percentile sits strictly above the typical round trip.
const MIN_LINK_LATENCY: Duration = Duration::from_millis(2);
const MAX_LINK_LATENCY: Duration = Duration::from_millis(8);

/// A cold hit is one request and its response over a pooled connection:
/// at least one round trip and below this many.
const HIT_MAX_ROUND_TRIPS: u32 = 3;

/// A load is at least this many dependent round trips: the metadata file,
/// the manifest list, the manifest, then footers and chunks.
const LOAD_MIN_ROUND_TRIPS: u32 = 4;

/// An id in the middle of an `IDS_4K` fixture the cold lookup asks for.
const SAMPLE_ID: u64 = 1_414;

/// Ids of the tail fixtures: enough distinct pages for hundreds of cold
/// lookups.
const TAIL_IDS: Range<u64> = 0..200_000;

/// The injected tail: every tenth request stalls for this long.
const STALL_EVERY: u32 = 10;
const STALL: Duration = Duration::from_millis(300);

/// The hedge window: the warm-up fills it, and nothing hedges before.
const HEDGE_WINDOW: usize = 20;

/// Lookups measured after the warm-up in the tail test, and their spacing
/// in ids, which spreads them over the table.
const MEASURED_LOOKUPS: u64 = 40;
const LOOKUP_STRIDE: u64 = 3_000;

/// The tail test's hedge settings: the ninetieth percentile, half the
/// requests allowed to hedge.
const TAIL_PERCENTILE: f64 = 0.9;
const TAIL_MAX_RATE: f64 = 0.5;

/// The ceiling test: the median as the delay, so half the round trips
/// exceed it, under a tenth as the ceiling, over this many measured lookups
/// at this spacing.
const CEILING: f64 = 0.1;
const CEILING_PERCENTILE: f64 = 0.5;
const CEILING_LOOKUPS: u64 = 200;
const CEILING_STRIDE: u64 = 900;

/// The never-hedge test: a delay of the largest sample after one, every
/// request allowed to hedge, every second request stalled.
const EAGER_PERCENTILE: f64 = 1.0;
const EAGER_WINDOW: usize = 1;
const EAGER_MAX_RATE: f64 = 1.0;
const STALL_EVERY_SECOND: u32 = 2;

/// The fixture as written for real: the temporary root the S3 host serves,
/// the fixture under `<root>/bucket/warehouse`, and the memory catalog and
/// tokio runtime it was written through, kept so a test can commit to it
/// again before the simulation starts.
struct Written {
    dir: TempDir,
    fixture: Fixture,
    catalog: Arc<MemoryCatalog>,
    runtime: tokio::runtime::Runtime,
}

/// Writes `spec` for real on a tokio runtime of the test's own, under the
/// bucket the simulated host will serve.
fn write(spec: FixtureSpec) -> Result<Written> {
    let dir = tempfile::tempdir()?;
    let warehouse = dir.path().join(BUCKET).join(WAREHOUSE);
    let runtime = tokio::runtime::Runtime::new()?;
    let (catalog, fixture) = runtime.block_on(async {
        let catalog = memory_catalog_local(&warehouse).await?;
        let fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
        Ok::<_, icetray_testkit::TestkitError>((catalog, fixture))
    })?;
    Ok(Written {
        dir,
        fixture,
        catalog,
        runtime,
    })
}

/// A storage for the simulated client: the fixture's `file://` bucket
/// rewritten to `s3://bucket`, served by the simulated host, hedging as
/// `hedge` says.
fn client_storage(root: &Path, hedge: HedgeConfig) -> Result<Arc<Storage>> {
    let storage = Arc::new(Storage::new(StorageConfig {
        rewrite: vec![RewriteRule {
            from: format!("file://{}/{BUCKET}", root.display()),
            to: format!("s3://{BUCKET}"),
        }],
        hedge,
        ..StorageConfig::default()
    }));
    storage.register(
        format!("s3://{BUCKET}/"),
        Arc::new(sim_s3_store(S3_HOST, BUCKET)?),
    );
    Ok(storage)
}

/// `hedge` disabled.
fn no_hedging() -> HedgeConfig {
    HedgeConfig {
        enabled: false,
        ..HedgeConfig::default()
    }
}

/// Serves `config` from `source` over `storage` inside the simulation.
async fn serve_in_sim(
    storage: Arc<Storage>,
    config: TableConfig,
    source: TableSource,
) -> Result<Served> {
    verify::serve_over(storage, config, source, None).await
}

/// A simulation with the S3 host serving `root`, stalled as `slow` says.
fn simulation<'a>(
    seed: u64,
    root: &Path,
    slow: Option<SlowEvery>,
    varying_latency: bool,
) -> turmoil::Sim<'a> {
    let mut builder = turmoil::Builder::new();
    builder.simulation_duration(SIM_DURATION).rng_seed(seed);
    if varying_latency {
        builder
            .min_message_latency(MIN_LINK_LATENCY)
            .max_message_latency(MAX_LINK_LATENCY);
    }
    let mut sim = builder.build();
    let server_root = root.to_path_buf();
    sim.host(S3_HOST, move || serve_s3_with(server_root.clone(), slow));
    sim
}

fn run(mut sim: turmoil::Sim<'_>) {
    if let Err(error) = sim.run() {
        panic!("simulation failed: {error}");
    }
}

/// One cold run: the load, a hit, and a miss, timed in virtual time.
struct Outcome {
    load: Duration,
    load_gets: u64,
    files: u64,
    hit: Duration,
    hit_gets: u64,
    miss: Duration,
}

fn run_cold(seed: u64, written: &Written) -> Outcome {
    let mut sim = simulation(seed, written.dir.path(), None, false);
    let (tx, rx) = mpsc::channel();
    let root = written.dir.path().to_path_buf();
    let config = written.fixture.config(Residency::Full, false);
    let location = written.fixture.metadata_location().to_string();
    let key = written.fixture.key(SAMPLE_ID);
    let expected = written.fixture.expected(&key);
    let absent = written.fixture.absent_near(SAMPLE_ID);
    sim.client(CLIENT_HOST, async move {
        let storage = client_storage(&root, no_hedging())?;
        let stats = storage.stats();
        let started = turmoil::elapsed();
        let served = serve_in_sim(storage, config, TableSource::Static { location }).await?;
        let load = turmoil::elapsed().saturating_sub(started);
        let load_gets = stats.gets();
        let files = u64::try_from(served.current().files.len())?;

        let before = stats.gets();
        let started = turmoil::elapsed();
        let lookup = served.table.get(&key).await?;
        let hit = turmoil::elapsed().saturating_sub(started);
        let hit_gets = stats.gets() - before;
        assert_eq!(lookup.value, expected);

        let started = turmoil::elapsed();
        let miss_value = served.table.get(&absent).await?.value;
        let miss = turmoil::elapsed().saturating_sub(started);
        assert_eq!(miss_value, Value::Absent);

        tx.send(Outcome {
            load,
            load_gets,
            files,
            hit,
            hit_gets,
            miss,
        })
        .expect("the test holds the receiver");
        Ok(())
    });
    sim.set_link_latency(CLIENT_HOST, S3_HOST, LINK_LATENCY);
    run(sim);
    rx.recv()
        .expect("the client sends its outcome before the simulation ends")
}

#[test]
fn test_cold_hit_is_one_round_trip_and_load_is_fixed_dependent_round_trips() -> Result<()> {
    let written = write(FixtureSpec {
        ids: IDS_4K,
        ..FixtureSpec::small("kv_sim", KeyType::Long)
    })?;
    let first = run_cold(SEED_FIRST, &written);
    let second = run_cold(SEED_FIRST, &written);
    let other_seed = run_cold(SEED_SECOND, &written);
    eprintln!(
        "virtual time: load {:?} ({} GETs), hit {:?} ({} GET), miss {:?}",
        first.load, first.load_gets, first.hit, first.hit_gets, first.miss
    );
    assert_eq!(first.hit_gets, 1);
    assert_eq!(first.miss, Duration::ZERO, "a miss touches no network");
    assert!(
        first.hit >= ROUND_TRIP && first.hit < ROUND_TRIP.saturating_mul(HIT_MAX_ROUND_TRIPS),
        "a cold hit is one request and response: {:?}",
        first.hit
    );
    assert_eq!(
        first.load_gets,
        METADATA_FILE_GETS + LOAD_METADATA_GETS + LOAD_GETS_PER_FILE * first.files,
        "metadata file, manifest list, manifest, then a footer and a chunk per file"
    );
    assert!(
        first.load >= ROUND_TRIP.saturating_mul(LOAD_MIN_ROUND_TRIPS),
        "a load is several dependent round trips: {:?}",
        first.load
    );
    assert_eq!(first.hit, second.hit, "same seed, same virtual time");
    assert_eq!(first.load, second.load);
    assert_eq!(other_seed.load_gets, first.load_gets);
    assert_eq!(other_seed.hit_gets, first.hit_gets);
    Ok(())
}

/// Tail statistics over the measured cold lookups of one run.
struct Tail {
    max: Duration,
    mean: Duration,
    hedges: u64,
    gets: u64,
    expected_gets: u64,
    lookups: u64,
}

/// Cold lookups at `stride` ids apart under `hedge`: `HEDGE_WINDOW` of
/// warm-up, then `measured` timed ones. `gets` and `hedges` count every
/// lookup; `max` and `mean` the measured ones.
fn run_tail(
    seed: u64,
    written: &Written,
    hedge: HedgeConfig,
    slow: Option<SlowEvery>,
    measured: u64,
    stride: u64,
) -> Tail {
    let mut sim = simulation(seed, written.dir.path(), slow, true);
    let (tx, rx) = mpsc::channel();
    let root = written.dir.path().to_path_buf();
    let config = written.fixture.config(Residency::Full, false);
    let location = written.fixture.metadata_location().to_string();
    let warm_up = u64::try_from(HEDGE_WINDOW).expect("the window fits u64");
    let keys: Vec<Key> = (0..warm_up + measured)
        .map(|i| written.fixture.key(i * stride))
        .collect();
    sim.client(CLIENT_HOST, async move {
        let storage = client_storage(&root, hedge)?;
        let stats = storage.stats();
        let served = serve_in_sim(storage, config, TableSource::Static { location }).await?;
        let generation = served.current();
        let expected_gets = verify::expected_cold_gets(&generation, &keys)?;
        let (gets_before, hedges_before) = (stats.gets(), stats.hedges());
        let mut max = Duration::ZERO;
        let mut total = Duration::ZERO;
        for (at, key) in keys.iter().enumerate() {
            let started = turmoil::elapsed();
            let lookup = served.table.get(key).await?;
            let elapsed = turmoil::elapsed().saturating_sub(started);
            assert!(matches!(lookup.value, Value::Present(_)), "key {key}");
            if at >= HEDGE_WINDOW {
                max = max.max(elapsed);
                total += elapsed;
            }
        }
        tx.send(Tail {
            max,
            mean: total / u32::try_from(measured)?,
            hedges: stats.hedges() - hedges_before,
            gets: stats.gets() - gets_before,
            expected_gets,
            lookups: measured,
        })
        .expect("the test holds the receiver");
        Ok(())
    });
    run(sim);
    rx.recv()
        .expect("the client sends its outcome before the simulation ends")
}

#[test]
fn test_hedging_trims_an_injected_tail() -> Result<()> {
    let written = write(FixtureSpec {
        ids: TAIL_IDS,
        ..FixtureSpec::small("kv_tail", KeyType::Long)
    })?;
    let stall = Some(SlowEvery {
        every: STALL_EVERY,
        delay: STALL,
    });
    let hedging = HedgeConfig {
        enabled: true,
        percentile: TAIL_PERCENTILE,
        window: HEDGE_WINDOW,
        max_rate: TAIL_MAX_RATE,
    };
    let plain = run_tail(
        SEED_FIRST,
        &written,
        no_hedging(),
        stall,
        MEASURED_LOOKUPS,
        LOOKUP_STRIDE,
    );
    let hedged = run_tail(
        SEED_FIRST,
        &written,
        hedging,
        stall,
        MEASURED_LOOKUPS,
        LOOKUP_STRIDE,
    );
    eprintln!(
        "plain: max {:?} mean {:?} gets {}; hedged: max {:?} mean {:?} gets {} hedges {}",
        plain.max, plain.mean, plain.gets, hedged.max, hedged.mean, hedged.gets, hedged.hedges
    );
    assert_eq!(plain.hedges, 0);
    assert_eq!(plain.gets, plain.expected_gets, "one GET per cold page");
    assert!(
        plain.max >= STALL,
        "the stall shows without hedging: {:?}",
        plain.max
    );
    assert!(hedged.hedges > 0, "stalled requests were hedged");
    assert_eq!(
        hedged.gets,
        hedged.expected_gets + hedged.hedges,
        "each hedge is one extra GET"
    );
    assert!(
        hedged.max < STALL,
        "a stalled request is cut short: {:?}",
        hedged.max
    );
    assert!(hedged.mean < plain.mean);
    Ok(())
}

#[test]
fn test_hedge_rate_stays_under_the_ceiling_without_a_tail() -> Result<()> {
    let written = write(FixtureSpec {
        ids: TAIL_IDS,
        ..FixtureSpec::small("kv_ceiling", KeyType::Long)
    })?;
    let hedging = HedgeConfig {
        enabled: true,
        percentile: CEILING_PERCENTILE,
        window: HEDGE_WINDOW,
        max_rate: CEILING,
    };
    let tail = run_tail(
        SEED_FIRST,
        &written,
        hedging,
        None,
        CEILING_LOOKUPS,
        CEILING_STRIDE,
    );
    eprintln!(
        "ceiling: {} hedges over {} lookups, max {:?} mean {:?}",
        tail.hedges, tail.lookups, tail.max, tail.mean
    );
    assert!(tail.hedges > 0, "half the round trips exceed the median");
    assert!(
        approx::ratio(tail.hedges, tail.lookups) <= CEILING,
        "{} hedges over {} lookups",
        tail.hedges,
        tail.lookups
    );
    Ok(())
}

#[test]
fn test_load_reads_never_hedge() -> Result<()> {
    let written = write(FixtureSpec {
        ids: IDS_4K,
        ..FixtureSpec::small("kv_loadhedge", KeyType::Long)
    })?;
    let Written {
        dir,
        mut fixture,
        catalog,
        runtime,
    } = written;
    // The hint names v1 while the catalog holds v2: the appended files load
    // only once the hint moves, inside the simulation.
    let v1_location = fixture.metadata_location().to_string();
    runtime.block_on(async {
        fixture.write_hint().await?;
        fixture
            .append(catalog.as_ref(), APPENDED_4K_6K, "build2")
            .await
    })?;
    assert_ne!(fixture.metadata_location(), v1_location);

    let stall = Some(SlowEvery {
        every: STALL_EVERY_SECOND,
        delay: STALL,
    });
    let mut sim = simulation(SEED_FIRST, dir.path(), stall, false);
    let (tx, rx) = mpsc::channel();
    let root = dir.path().to_path_buf();
    let source = fixture.root_source();
    let config = TableConfig {
        source: source.clone(),
        ..fixture.config(Residency::Full, false)
    };
    let old_key = fixture.key(SAMPLE_ID);
    let new_key = fixture.key(NEW_ID);
    sim.client(CLIENT_HOST, async move {
        let storage = client_storage(
            &root,
            HedgeConfig {
                enabled: true,
                percentile: EAGER_PERCENTILE,
                window: EAGER_WINDOW,
                max_rate: EAGER_MAX_RATE,
            },
        )?;
        let stats = storage.stats();
        let served =
            serve_in_sim(storage, config, TableSource::from_config(&source).await?).await?;
        assert_eq!(served.current().metadata_location, v1_location);
        assert_eq!(stats.hedges(), 0, "a load never hedges");
        assert_eq!(
            served.table.get(&old_key).await?.value,
            fixture.expected(&old_key)
        );
        assert_eq!(served.table.get(&new_key).await?.value, Value::Absent);

        fixture.write_hint().await?;
        let gets_before = stats.gets();
        assert!(served.table.refresh().await?);
        assert!(
            stats.gets() > gets_before,
            "the new files' footers and chunks were read"
        );
        let hedges_after_refresh = stats.hedges();
        assert_eq!(hedges_after_refresh, 0, "and none of them hedged");
        // The lookup GETs after the refresh may hedge under these eager
        // settings; only the load's count is the invariant.
        assert_eq!(
            served.table.get(&new_key).await?.value,
            fixture.expected(&new_key)
        );
        tx.send(hedges_after_refresh)
            .expect("the test holds the receiver");
        Ok(())
    });
    sim.set_link_latency(CLIENT_HOST, S3_HOST, LINK_LATENCY);
    run(sim);
    assert_eq!(
        rx.recv()
            .expect("the client sends its outcome before the simulation ends"),
        0
    );
    Ok(())
}
