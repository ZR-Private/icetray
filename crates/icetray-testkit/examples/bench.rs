//! The benchmark program, a program and not a test: the in-process lookup
//! and load paths measured over contract-shaped tables on the local
//! filesystem, so every number is icetray's own CPU and allocation cost with
//! no object-store latency in it. It writes a long-key and a string-key
//! table of `ROWS` rows each through the fixture writer (once, then reused
//! from `--data-dir`), serves them through `Table` over `Storage`'s
//! `file://` store in every residency tier, and reports latency
//! percentiles, throughput, and allocations per operation, plus the wall
//! time, CPU time, allocations, and peak resident set of a load and a
//! refresh. Results print as a table and are written as JSON under
//! `--out-dir`. Run with
//! `cargo run -p icetray-testkit --example bench --release -- --help`; a
//! profile of one scenario narrows `--scenario`, `--tier`, `--table`, and
//! `--mode`, and lengthens `--duration`.

use std::alloc::{GlobalAlloc, Layout};
use std::error::Error;
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use iceberg::spec::MAIN_BRANCH;
use icetray_core::config::{
    LocalConfig, Residency, ServerConfig, SourceConfig, StorageConfig, TableConfig,
};
use icetray_core::contract::TARGET_FILE_BYTES_MAX;
use icetray_core::{
    CallsInFlight, Key, KeyType, LocalDir, PageCache, Storage, Table, TableHandles, TableSource,
    Value,
};
use icetray_core::{approx, catalog, time};
use icetray_testkit::fixture::{NAMESPACE, memory_catalog_local};
use icetray_testkit::keys::{STRING_KEY_FIXED_BYTES, VALUE_BYTES};
use icetray_testkit::layout::{KEY_COLUMN, VALUE_COLUMN};
use icetray_testkit::{FixtureSpec, absent_key_from, key_at, write_fixture};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use serde_json::{Value as Json, json};
use tempfile::TempDir;
use tikv_jemallocator::Jemalloc;

/// Whatever failed; the program reports it and exits.
type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

// ---------------------------------------------------------------------------
// Allocation counting.

/// The allocator every allocation reaches in the end: jemalloc, as the
/// binary runs, so what is counted is what the server would allocate.
static JEMALLOC: Jemalloc = Jemalloc;

/// jemalloc behind a counter of allocation calls and requested bytes.
#[global_allocator]
static GLOBAL: Counting = Counting;

/// Shards of the allocation counters. One shared counter would put every
/// allocation of every worker thread on one cache line and slow the
/// multi-thread runs by the contention; each thread adds to the shard its
/// marker's address hashes to, and a read sums the shards.
const COUNTER_SHARDS: usize = 64;

/// The multiplier of the shard hash: the golden ratio's fractional bits, a
/// Fibonacci hash, which spreads nearby thread-local addresses apart.
const SHARD_HASH: usize = 0x9e37_79b9_7f4a_7c15;

/// The shift that keeps the hash's top `log2(COUNTER_SHARDS)` bits.
const SHARD_SHIFT: u32 = usize::BITS - COUNTER_SHARDS.trailing_zeros();

/// One shard of the counters, alone on its cache line; 128 bytes is the
/// line of the Apple M-series cores and twice that of x86, so it separates
/// shards on both.
#[repr(align(128))]
struct Shard {
    allocations: AtomicU64,
    bytes: AtomicU64,
}

impl Shard {
    const fn new() -> Self {
        Self {
            allocations: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
        }
    }
}

static SHARDS: [Shard; COUNTER_SHARDS] = [const { Shard::new() }; COUNTER_SHARDS];

thread_local! {
    /// A byte per thread whose address picks the thread's shard. Constant
    /// and without a destructor, so reading it from inside the allocator
    /// does not allocate, and it stays readable while the thread exits.
    static MARKER: u8 = const { 0 };
}

/// Counts one allocation call of `size` bytes on the calling thread's shard.
fn count_allocation(size: usize) {
    let address = MARKER.with(|marker| std::ptr::from_ref(marker).addr());
    let shard = &SHARDS[address.wrapping_mul(SHARD_HASH) >> SHARD_SHIFT];
    shard.allocations.fetch_add(1, Ordering::Relaxed);
    shard
        .bytes
        .fetch_add(approx::count(size), Ordering::Relaxed);
}

/// jemalloc, counting `alloc`, `alloc_zeroed`, and `realloc` as one
/// allocation each, at the bytes requested (a `realloc` at its new size).
struct Counting;

// SAFETY: every method forwards to jemalloc with the caller's arguments
// unchanged, so jemalloc's guarantees are the caller's; the counting beside
// it touches only atomics and a constant thread-local, and never allocates.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count_allocation(layout.size());
        // SAFETY: the caller upholds `alloc`'s contract, forwarded as is.
        unsafe { JEMALLOC.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count_allocation(layout.size());
        // SAFETY: the caller upholds `alloc_zeroed`'s contract.
        unsafe { JEMALLOC.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        count_allocation(new_size);
        // SAFETY: the caller upholds `realloc`'s contract.
        unsafe { JEMALLOC.realloc(ptr, layout, new_size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the caller upholds `dealloc`'s contract.
        unsafe { JEMALLOC.dealloc(ptr, layout) }
    }
}

/// The process's allocation calls and requested bytes so far.
#[derive(Clone, Copy, Debug, Default)]
struct Allocations {
    calls: u64,
    bytes: u64,
}

impl Allocations {
    fn now() -> Self {
        SHARDS.iter().fold(Self::default(), |sum, shard| Self {
            calls: sum.calls + shard.allocations.load(Ordering::Relaxed),
            bytes: sum.bytes + shard.bytes.load(Ordering::Relaxed),
        })
    }

    fn since(self, earlier: Self) -> Self {
        Self {
            calls: self.calls - earlier.calls,
            bytes: self.bytes - earlier.bytes,
        }
    }
}

// ---------------------------------------------------------------------------
// The command line.

/// What to measure.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Scenario {
    /// A hit whose value page the page cache holds.
    WarmHit,
    /// A hit whose value page the page cache does not hold.
    ColdHit,
    /// A key inside the table's range that no file holds.
    Miss,
    /// A `MultiGet` of 100 hits, warm.
    #[value(name = "multi-get-100")]
    MultiGet100,
    /// A `MultiGet` of 1,000 hits, warm.
    #[value(name = "multi-get-1000")]
    MultiGet1000,
    /// A table's first load.
    Load,
    /// A refresh onto an appended snapshot, and an unchanged poll.
    Refresh,
    /// Both tables served in one process: `--tasks` tasks of warm hits on
    /// the long table beside one task looping a `MultiGet` of 1,000 warm
    /// hits on the string table. Serves both tables whatever `--table`
    /// selects.
    TwoTable,
}

impl Scenario {
    const ALL: [Self; 8] = [
        Self::WarmHit,
        Self::ColdHit,
        Self::Miss,
        Self::MultiGet100,
        Self::MultiGet1000,
        Self::Load,
        Self::Refresh,
        Self::TwoTable,
    ];

    const LOOKUPS: [Self; 5] = [
        Self::WarmHit,
        Self::ColdHit,
        Self::Miss,
        Self::MultiGet100,
        Self::MultiGet1000,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::WarmHit => "warm-hit",
            Self::ColdHit => "cold-hit",
            Self::Miss => "miss",
            Self::MultiGet100 => "multi-get-100",
            Self::MultiGet1000 => "multi-get-1000",
            Self::Load => "load",
            Self::Refresh => "refresh",
            Self::TwoTable => "two-table",
        }
    }
}

/// A residency tier (RFD 017).
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Tier {
    Full,
    Disk,
    Bloom,
}

impl Tier {
    const ALL: [Self; 3] = [Self::Full, Self::Disk, Self::Bloom];

    fn residency(self) -> Residency {
        match self {
            Self::Full => Residency::Full,
            Self::Disk => Residency::Disk,
            Self::Bloom => Residency::Bloom,
        }
    }
}

/// One of the two benchmark tables.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum TableName {
    Long,
    String,
}

/// How many tasks drive the lookups.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Mode {
    /// One task, one lookup at a time: latency.
    Single,
    /// `--tasks` tasks at once on the multi-thread runtime: throughput.
    Multi,
}

impl Mode {
    const ALL: [Self; 2] = [Self::Single, Self::Multi];

    fn name(self) -> &'static str {
        match self {
            Self::Single => "single",
            Self::Multi => "multi",
        }
    }
}

/// The benchmark's command line. Every list flag takes comma-separated
/// values and, when absent, means all of them.
#[derive(Parser, Debug)]
#[command(about = "Measures icetray's in-process lookup and load paths")]
struct Cli {
    /// Scenarios to run.
    #[arg(long, value_enum, value_delimiter = ',')]
    scenario: Vec<Scenario>,
    /// Residency tiers to run them in.
    #[arg(long, value_enum, value_delimiter = ',')]
    tier: Vec<Tier>,
    /// Tables to run them over.
    #[arg(long, value_enum, value_delimiter = ',')]
    table: Vec<TableName>,
    /// Single-task latency, multi-task throughput, or both.
    #[arg(long, value_enum, value_delimiter = ',')]
    mode: Vec<Mode>,
    /// Seconds each lookup scenario is measured for.
    #[arg(long, default_value_t = 5, value_parser = clap::value_parser!(u64).range(1..))]
    duration: u64,
    /// Seconds each lookup scenario runs, unmeasured, before it is measured.
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u64).range(1..))]
    warmup: u64,
    /// Tasks of a multi-task run; the machine's cores when absent.
    #[arg(long)]
    tasks: Option<usize>,
    /// Loads, and refreshes, per table and tier.
    #[arg(long, default_value_t = 3, value_parser = clap::value_parser!(u32).range(1..))]
    load_rounds: u32,
    /// Where the tables are written and, on later runs, reused from.
    #[arg(long, default_value = "target/bench/tables")]
    data_dir: PathBuf,
    /// Where the JSON results go.
    #[arg(long, default_value = "target/bench")]
    out_dir: PathBuf,
    /// Write the tables again even when `--data-dir` holds them.
    #[arg(long)]
    rewrite: bool,
    /// Write or reuse the tables, print one line per table (name, key
    /// type, rows, and the first build's metadata file), and exit.
    #[arg(long)]
    prepare: bool,
    /// Measure without the binary's Prometheus recorder installed, so the
    /// difference against a run with it is the metrics' cost.
    #[arg(long)]
    no_metrics: bool,
}

// ---------------------------------------------------------------------------
// The tables.

/// Rows of each benchmark table's first build.
const ROWS: u64 = 2_000_000;

/// Rows the second build appends above the first's ids, one percent of
/// `ROWS`: a small file per bucket, the refresh scenario's new snapshot.
const APPENDED_ROWS: u64 = 20_000;

/// The file prefix of the second build.
const SECOND_BUILD_PREFIX: &str = "build2";

/// The name of the file under `--data-dir` recording what was written.
const MANIFEST_FILE: &str = "manifest.json";

/// The scheme the fixture writer's local catalog records locations under.
const FILE_URI: &str = "file://";

/// A benchmark table as written: its name, key type, and the bytes a row
/// holds before compression, from which the bucket count follows.
#[derive(Clone, Copy, Debug)]
struct TableShape {
    name: TableName,
    key_type: KeyType,
    key_bytes: usize,
}

const SHAPES: [TableShape; 2] = [
    TableShape {
        name: TableName::Long,
        key_type: KeyType::Long,
        key_bytes: size_of::<i64>(),
    },
    TableShape {
        name: TableName::String,
        key_type: KeyType::String,
        key_bytes: STRING_KEY_FIXED_BYTES,
    },
];

impl TableName {
    fn as_str(self) -> &'static str {
        match self {
            Self::Long => "long",
            Self::String => "string",
        }
    }
}

impl TableShape {
    /// One file per bucket at the contract's upper file size: the raw row
    /// bytes over `TARGET_FILE_BYTES_MAX`, rounded up. Values are random
    /// bytes, which zstd does not shrink, so the written files come close
    /// to the raw size, inside the contract's 32 to 64 MiB.
    fn buckets(self) -> u32 {
        let row_bytes = approx::count(self.key_bytes + VALUE_BYTES);
        u32::try_from((ROWS * row_bytes).div_ceil(TARGET_FILE_BYTES_MAX))
            .expect("a few hundred megabytes over 64 MiB is a handful of buckets")
    }
}

/// A written table: where its root is and the two builds' metadata files.
#[derive(Clone, Debug)]
struct Prepared {
    shape: TableShape,
    root: String,
    first: String,
    second: String,
}

impl Prepared {
    fn to_json(&self) -> Json {
        json!({
            "name": self.shape.name.as_str(),
            "buckets": self.shape.buckets(),
            "root": self.root,
            "first": self.first,
            "second": self.second,
        })
    }
}

/// The tables under `dir`: reused when its manifest records the same rows
/// and tables, else written again from an empty directory.
async fn prepare(dir: &Path, rewrite: bool) -> BenchResult<Vec<Prepared>> {
    if !rewrite && let Some(prepared) = read_manifest(dir)? {
        eprintln!("reusing the tables under {}", dir.display());
        return Ok(prepared);
    }
    if dir.exists() {
        std::fs::remove_dir_all(dir)?;
    }
    std::fs::create_dir_all(dir)?;
    let catalog = memory_catalog_local(&std::path::absolute(dir)?).await?;
    let mut prepared = Vec::with_capacity(SHAPES.len());
    for shape in SHAPES {
        let started = Instant::now();
        let spec = FixtureSpec {
            buckets: shape.buckets(),
            ids: 0..ROWS,
            ..FixtureSpec::small(shape.name.as_str(), shape.key_type)
        };
        let mut fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
        let first = fixture.metadata_location().to_string();
        fixture
            .append(
                catalog.as_ref(),
                ROWS..ROWS + APPENDED_ROWS,
                SECOND_BUILD_PREFIX,
            )
            .await?;
        eprintln!(
            "wrote {} ({ROWS} rows, {} buckets, then {APPENDED_ROWS} more): {:.1?}",
            shape.name.as_str(),
            shape.buckets(),
            started.elapsed()
        );
        prepared.push(Prepared {
            shape,
            root: fixture.root().to_string(),
            first,
            second: fixture.metadata_location().to_string(),
        });
    }
    let manifest = json!({
        "rows": ROWS,
        "appended_rows": APPENDED_ROWS,
        "tables": prepared.iter().map(Prepared::to_json).collect::<Vec<_>>(),
    });
    std::fs::write(
        dir.join(MANIFEST_FILE),
        serde_json::to_vec_pretty(&manifest)?,
    )?;
    Ok(prepared)
}

/// The manifest under `dir` when it describes the tables this program
/// writes now; `None` when it is absent or describes anything else.
fn read_manifest(dir: &Path) -> BenchResult<Option<Vec<Prepared>>> {
    let path = dir.join(MANIFEST_FILE);
    if !path.exists() {
        return Ok(None);
    }
    let manifest: Json = serde_json::from_slice(&std::fs::read(path)?)?;
    if manifest["rows"].as_u64() != Some(ROWS)
        || manifest["appended_rows"].as_u64() != Some(APPENDED_ROWS)
    {
        return Ok(None);
    }
    let Some(tables) = manifest["tables"].as_array() else {
        return Ok(None);
    };
    let mut prepared = Vec::with_capacity(SHAPES.len());
    for (shape, table) in SHAPES.iter().zip(tables) {
        let text = |field: &str| table[field].as_str().map(str::to_string);
        let (Some(root), Some(first), Some(second)) = (text("root"), text("first"), text("second"))
        else {
            return Ok(None);
        };
        if table["name"].as_str() != Some(shape.name.as_str())
            || table["buckets"].as_u64() != Some(u64::from(shape.buckets()))
        {
            return Ok(None);
        }
        prepared.push(Prepared {
            shape: *shape,
            root,
            first,
            second,
        });
    }
    Ok((prepared.len() == SHAPES.len()).then_some(prepared))
}

/// Reads every file under `dir` once, so the operating system's page cache
/// holds the data files and a cold hit's read is a memory copy, not a disk
/// seek: the benchmark measures icetray, not the drive.
fn prime_os_cache(dir: &Path) -> BenchResult<u64> {
    let mut read = 0;
    let mut pending = vec![dir.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(dir)? {
            let path = entry?.path();
            if path.is_dir() {
                pending.push(path);
            } else {
                read += std::io::copy(&mut std::fs::File::open(path)?, &mut std::io::sink())?;
            }
        }
    }
    Ok(read)
}

/// A location the local catalog recorded, as the path it names.
fn local_path(location: &str) -> BenchResult<PathBuf> {
    location
        .strip_prefix(FILE_URI)
        .map(PathBuf::from)
        .ok_or_else(|| format!("{location} is not a {FILE_URI} location").into())
}

// ---------------------------------------------------------------------------
// Serving.

/// The page cache of the warm and miss scenarios: large enough to hold
/// every value page of either table (about 200 MB of 100-byte values) and,
/// under the Bloom tier, every key page besides, so nothing is evicted.
const WARM_CACHE_BYTES: usize = 1 << 30;

/// The page cache of the cold-hit scenario: one mebibyte, about ten of a
/// table's two thousand value pages, so nearly every uniformly random hit
/// misses the cache, reads its page from the file, and decodes it. The run
/// reports the page fetches it measured per lookup.
const COLD_CACHE_BYTES: usize = 1 << 20;

/// One table served over storage of its own, so its counters are its own,
/// and a local directory that lives as long as it does under the Disk tier.
struct Served {
    table: Arc<Table>,
    storage: Arc<Storage>,
    cache: PageCache,
    _local: Option<TempDir>,
}

/// `prepared` in `tier` from `source`, over the production storage
/// defaults (hedging on) and a `cache_bytes` page cache, not yet loaded.
/// The Disk tier gets a fresh local directory with no page-cache disk
/// level, or `reuse`'s when given, whose chunk files a load then reuses.
async fn serve(
    prepared: &Prepared,
    tier: Tier,
    cache_bytes: usize,
    source: SourceConfig,
    reuse: Option<TempDir>,
) -> BenchResult<Served> {
    let storage = Arc::new(Storage::new(StorageConfig::default()));
    let (local, dir) = local_for(tier, &storage, reuse)?;
    let cache = PageCache::new(cache_bytes, None).await?;
    let handles = TableHandles {
        storage: Arc::clone(&storage),
        cache: cache.clone(),
        local,
        calls: CallsInFlight::new(),
    };
    let table = table_over(prepared, tier, source, handles).await?;
    Ok(Served {
        table: Arc::new(table),
        storage,
        cache,
        _local: dir,
    })
}

/// Under the Disk tier, a local directory over `storage` with no
/// page-cache disk level, in `reuse` when given or a fresh temporary
/// directory, and the directory, which must live as long as the tables
/// served over it; nothing under the other tiers.
fn local_for(
    tier: Tier,
    storage: &Storage,
    reuse: Option<TempDir>,
) -> BenchResult<(Option<Arc<LocalDir>>, Option<TempDir>)> {
    match tier {
        Tier::Disk => {
            let dir = match reuse {
                Some(dir) => dir,
                None => tempfile::tempdir()?,
            };
            let config = LocalConfig {
                dir: dir.path().to_path_buf(),
                index_budget_bytes: None,
                page_cache_disk_bytes: 0,
            };
            Ok((
                Some(Arc::new(LocalDir::open(&config, storage.stats())?)),
                Some(dir),
            ))
        }
        Tier::Full | Tier::Bloom => Ok((None, None)),
    }
}

/// `prepared` as a `Table` in `tier` from `source`, strict, over
/// `handles`, with the default deadlines.
async fn table_over(
    prepared: &Prepared,
    tier: Tier,
    source: SourceConfig,
    handles: TableHandles,
) -> BenchResult<Table> {
    let config = TableConfig {
        name: prepared.shape.name.as_str().to_string(),
        source: source.clone(),
        r#ref: MAIN_BRANCH.to_string(),
        key_column: KEY_COLUMN.to_string(),
        value_column: VALUE_COLUMN.to_string(),
        strict: true,
        residency: tier.residency(),
        resident_budget_bytes: None,
    };
    let server = ServerConfig::default();
    Ok(Table::new(
        config,
        TableSource::from_config(&source).await?,
        handles,
        server.refresh_deadline,
        server.get_deadline,
    ))
}

/// The first build's metadata file, which never advances.
fn pinned(prepared: &Prepared) -> SourceConfig {
    SourceConfig::Static {
        location: prepared.first.clone(),
    }
}

// ---------------------------------------------------------------------------
// Lookups.

/// Keys in each scenario's pool; a run cycles through them. A million is
/// half the table, so a warm run touches every page and no run repeats a
/// key before it has used many thousands of others.
const KEY_POOL: usize = 1 << 20;

/// The seed of the key pools, fixed so every run asks the same keys.
const KEY_SEED: u64 = 0x1ce7_4a7b_e4c4_0001;

/// Ids between two keys of the pass that fills the warm cache: a value page
/// holds a thousand rows of one bucket, which span a few thousand ids, so
/// every page is asked many times over.
const PRIME_STRIDE: u64 = 100;

/// The samples a run reserves beyond what its warm-up rate predicts,
/// relative to that prediction: twice, so a run faster than its warm-up
/// still records into memory reserved before it started.
const SAMPLE_HEADROOM: usize = 2;

/// Pools of hits and misses over `shape`'s key space: uniform random ids of
/// the first build, as keys and as the absent key just above each.
fn key_pools(shape: TableShape) -> (Arc<Vec<Key>>, Arc<Vec<Key>>) {
    let mut rng = StdRng::seed_from_u64(KEY_SEED);
    let hits: Vec<Key> = (0..KEY_POOL)
        .map(|_| key_at(rng.random_range(0..ROWS), shape.key_type))
        .collect();
    let misses = hits.iter().map(absent_key_from).collect();
    (Arc::new(hits), Arc::new(misses))
}

/// What one operation of a run does.
#[derive(Clone)]
enum Op {
    /// One `get` of the next pool key, which must answer present, or absent
    /// when `present` is false.
    Get { keys: Arc<Vec<Key>>, present: bool },
    /// One `multi_get` of the next `batch` pool keys, all present.
    MultiGet { keys: Arc<Vec<Key>>, batch: usize },
}

impl Op {
    /// Runs operation `i`; an answer of the wrong presence is an error, so
    /// a run that measured nothing but failures cannot pass for fast.
    async fn run(&self, table: &Table, i: usize) -> BenchResult<()> {
        match self {
            Self::Get { keys, present } => {
                let lookup = table.get(&keys[i % keys.len()]).await?;
                let found = matches!(lookup.value, Value::Present(_));
                if found != *present {
                    return Err(format!("key {i} answered {:?}", lookup.value).into());
                }
            }
            Self::MultiGet { keys, batch } => {
                let start = (i * batch) % (keys.len() - batch);
                let lookup = table.multi_get(&keys[start..start + batch]).await?;
                if !lookup
                    .values
                    .iter()
                    .all(|value| matches!(value, Value::Present(_)))
                {
                    return Err(format!("batch {i} answered a key absent").into());
                }
            }
        }
        Ok(())
    }
}

/// Whether a task loop gives its worker up between operations.
#[derive(Clone, Copy, Debug)]
enum Pace {
    /// One operation straight after the other. A warm lookup never waits,
    /// so the loop holds its worker until the run ends.
    Straight,
    /// A yield to the scheduler after each operation, untimed, as a
    /// server's requests end their tasks: other tasks queued on the worker
    /// run between operations.
    Yielding,
}

/// One task's share of a run: operations `task`, `task + tasks`, ... until
/// `until`, each timed into `samples` when it is given.
async fn task_loop(
    table: Arc<Table>,
    op: Op,
    (task, tasks): (usize, usize),
    pace: Pace,
    until: Instant,
    mut samples: Option<Vec<Duration>>,
) -> BenchResult<(usize, Vec<Duration>)> {
    let mut done = 0;
    while Instant::now() < until {
        let started = Instant::now();
        op.run(&table, done * tasks + task).await?;
        if let Some(samples) = samples.as_mut() {
            samples.push(started.elapsed());
        }
        done += 1;
        if let Pace::Yielding = pace {
            tokio::task::yield_now().await;
        }
    }
    Ok((done, samples.unwrap_or_default()))
}

/// The task loops of a run, as they are spawned.
type Loops = Vec<tokio::task::JoinHandle<BenchResult<(usize, Vec<Duration>)>>>;

/// `tasks` task loops of `op` over `table` at `pace` until `until`, each
/// reserving `reserve` samples when recording, spawned and not yet joined.
fn spawn_loops(
    table: &Arc<Table>,
    (op, pace): (&Op, Pace),
    tasks: usize,
    until: Instant,
    reserve: Option<usize>,
) -> Loops {
    (0..tasks)
        .map(|task| {
            tokio::spawn(task_loop(
                Arc::clone(table),
                op.clone(),
                (task, tasks),
                pace,
                until,
                reserve.map(Vec::with_capacity),
            ))
        })
        .collect()
}

/// `tasks` task loops for `length`, each reserving `reserve` samples when
/// recording; the operations done and the samples, merged.
async fn spawn_run(
    served: &Served,
    op: &Op,
    tasks: usize,
    length: Duration,
    reserve: Option<usize>,
) -> BenchResult<(usize, Vec<Duration>)> {
    let until = Instant::now() + length;
    join_loops(spawn_loops(
        &served.table,
        (op, Pace::Straight),
        tasks,
        until,
        reserve,
    ))
    .await
}

/// Waits for every loop of `handles`; the operations they did and their
/// samples, merged.
async fn join_loops(handles: Loops) -> BenchResult<(usize, Vec<Duration>)> {
    let mut done = 0;
    let mut samples = Vec::new();
    for handle in handles {
        let (ops, mut task_samples) = handle.await??;
        done += ops;
        samples.append(&mut task_samples);
    }
    Ok((done, samples))
}

/// Counters of a served table and the process, read before and after a
/// measured window.
#[derive(Clone, Copy, Debug, Default)]
struct Counters {
    allocations: Allocations,
    gets: u64,
    local_reads: u64,
    hedges: u64,
    fetches: u64,
    hits: u64,
}

impl Counters {
    fn read(served: &Served) -> Self {
        let stats = served.storage.stats();
        Self {
            allocations: Allocations::now(),
            gets: stats.gets(),
            local_reads: stats.local_reads(),
            hedges: stats.hedges(),
            fetches: served.cache.fetches(),
            hits: served.cache.hits(),
        }
    }

    fn since(self, earlier: Self) -> Self {
        Self {
            allocations: self.allocations.since(earlier.allocations),
            gets: self.gets - earlier.gets,
            local_reads: self.local_reads - earlier.local_reads,
            hedges: self.hedges - earlier.hedges,
            fetches: self.fetches - earlier.fetches,
            hits: self.hits - earlier.hits,
        }
    }
}

/// What a measured run is of.
#[derive(Clone, Copy, Debug)]
struct RunLabel {
    scenario: Scenario,
    table: TableName,
    tier: Tier,
    mode: Mode,
    tasks: usize,
}

/// A measured run.
struct Measured {
    label: RunLabel,
    ops: usize,
    /// Lookups per operation: one, or a `MultiGet`'s batch.
    keys_per_op: usize,
    elapsed: Duration,
    /// Every operation's latency, ascending.
    sorted: Vec<Duration>,
    counters: Counters,
}

/// One second, the unit of the throughput figure.
const SECOND: Duration = Duration::from_secs(1);

/// One microsecond, the unit latencies are written to JSON in.
const MICROSECOND: Duration = Duration::from_micros(1);

/// The percentiles reported, in thousandths, beside the maximum.
const P50: usize = 500;
const P90: usize = 900;
const P99: usize = 990;
const PER_MILLE: usize = 1_000;

impl Measured {
    /// The nearest-rank percentile at `per_mille` thousandths.
    fn percentile(&self, per_mille: usize) -> Duration {
        let rank = (self.sorted.len() * per_mille).div_ceil(PER_MILLE);
        self.sorted
            .get(rank.saturating_sub(1))
            .copied()
            .unwrap_or_default()
    }

    fn max(&self) -> Duration {
        self.sorted.last().copied().unwrap_or_default()
    }

    fn ops_per_second(&self) -> f64 {
        approx::gauge(approx::count(self.ops)) * SECOND.div_duration_f64(self.elapsed)
    }

    fn per_op(&self, total: u64) -> f64 {
        approx::ratio(total, approx::count(self.ops))
    }

    fn to_json(&self) -> Json {
        let micros = |duration: Duration| duration.div_duration_f64(MICROSECOND);
        let counters = self.counters;
        json!({
            "scenario": self.label.scenario.name(),
            "table": self.label.table.as_str(),
            "tier": format!("{:?}", self.label.tier).to_lowercase(),
            "mode": self.label.mode.name(),
            "tasks": self.label.tasks,
            "keys_per_op": self.keys_per_op,
            "ops": self.ops,
            "elapsed_us": micros(self.elapsed),
            "ops_per_second": self.ops_per_second(),
            "p50_us": micros(self.percentile(P50)),
            "p90_us": micros(self.percentile(P90)),
            "p99_us": micros(self.percentile(P99)),
            "max_us": micros(self.max()),
            "allocations_per_op": self.per_op(counters.allocations.calls),
            "allocated_bytes_per_op": self.per_op(counters.allocations.bytes),
            "page_fetches_per_op": self.per_op(counters.fetches),
            "page_cache_hits_per_op": self.per_op(counters.hits),
            "gets_per_op": self.per_op(counters.gets),
            "local_reads_per_op": self.per_op(counters.local_reads),
            "hedges": counters.hedges,
        })
    }
}

/// Runs `op` over `served` for the warm-up and then for the measured
/// window, the samples of the window reserved from the warm-up's rate
/// before the counters are first read, so recording allocates nothing
/// inside the window.
async fn measure(
    served: &Served,
    op: &Op,
    label: RunLabel,
    warmup: Duration,
    duration: Duration,
    windows: u64,
) -> BenchResult<Measured> {
    let (warm_ops, _) = spawn_run(served, op, label.tasks, warmup, None).await?;
    let windows = approx::length(windows);
    let reserve = (warm_ops / label.tasks + 1) * windows * SAMPLE_HEADROOM;
    let before = Counters::read(served);
    let started = Instant::now();
    let (ops, mut sorted) = spawn_run(served, op, label.tasks, duration, Some(reserve)).await?;
    let elapsed = started.elapsed();
    let counters = Counters::read(served).since(before);
    sorted.sort_unstable();
    let keys_per_op = match op {
        Op::Get { .. } => 1,
        Op::MultiGet { batch, .. } => *batch,
    };
    Ok(Measured {
        label,
        ops,
        keys_per_op,
        elapsed,
        sorted,
        counters,
    })
}

/// Asks every page of the table once, `PRIME_STRIDE` ids apart, so the
/// warm cache holds every value page (and under Bloom every key page)
/// before a warm run starts.
async fn prime_cache(table: &Table, shape: TableShape) -> BenchResult<()> {
    for id in (0..ROWS).step_by(approx::length(PRIME_STRIDE)) {
        table.get(&key_at(id, shape.key_type)).await?;
    }
    Ok(())
}

/// Settings every lookup run shares.
struct RunSettings {
    modes: Vec<Mode>,
    tasks: usize,
    warmup: Duration,
    duration: Duration,
    /// `duration` over `warmup`, rounded up: how many warm-ups long the
    /// measured window is, which sizes its sample reservation.
    windows: u64,
}

/// Every selected lookup scenario of one table in one tier, each in every
/// selected mode, over a warm table and, for the cold hit, a cold one.
async fn lookups(
    prepared: &Prepared,
    tier: Tier,
    scenarios: &[Scenario],
    settings: &RunSettings,
) -> BenchResult<Vec<Measured>> {
    let shape = prepared.shape;
    let (hits, misses) = key_pools(shape);
    let warm_needed = scenarios.iter().any(|s| *s != Scenario::ColdHit);
    let warm = if warm_needed {
        let served = serve(prepared, tier, WARM_CACHE_BYTES, pinned(prepared), None).await?;
        served.table.load().await?;
        prime_cache(&served.table, shape).await?;
        Some(served)
    } else {
        None
    };
    let cold = if scenarios.contains(&Scenario::ColdHit) {
        let served = serve(prepared, tier, COLD_CACHE_BYTES, pinned(prepared), None).await?;
        served.table.load().await?;
        Some(served)
    } else {
        None
    };
    let mut results = Vec::new();
    for &scenario in scenarios {
        let (served, op) = match scenario {
            Scenario::WarmHit => (&warm, get(&hits, true)),
            Scenario::ColdHit => (&cold, get(&hits, true)),
            Scenario::Miss => (&warm, get(&misses, false)),
            Scenario::MultiGet100 => (&warm, multi(&hits, BATCH_100)),
            Scenario::MultiGet1000 => (&warm, multi(&hits, BATCH_1000)),
            Scenario::Load | Scenario::Refresh | Scenario::TwoTable => continue,
        };
        let served = served
            .as_ref()
            .expect("a table is served for every selected scenario");
        for &mode in &settings.modes {
            let tasks = match mode {
                Mode::Single => 1,
                Mode::Multi => settings.tasks,
            };
            let label = RunLabel {
                scenario,
                table: shape.name,
                tier,
                mode,
                tasks,
            };
            let run = measure(
                served,
                &op,
                label,
                settings.warmup,
                settings.duration,
                settings.windows,
            )
            .await?;
            eprintln!("{}", row(&run));
            results.push(run);
        }
    }
    Ok(results)
}

/// The two `MultiGet` sizes measured.
const BATCH_100: usize = 100;
const BATCH_1000: usize = 1_000;

fn get(keys: &Arc<Vec<Key>>, present: bool) -> Op {
    Op::Get {
        keys: Arc::clone(keys),
        present,
    }
}

fn multi(keys: &Arc<Vec<Key>>, batch: usize) -> Op {
    Op::MultiGet {
        keys: Arc::clone(keys),
        batch,
    }
}

/// Both tables in `tier` over one storage, one `WARM_CACHE_BYTES` page
/// cache, under Disk one local directory, and one count of calls in
/// flight, as `open_with` opens a process's tables; loaded, each primed as
/// the warm scenarios are. The tables
/// come in `prepared`'s order, beside the directory, which must outlive
/// them.
async fn serve_together(
    prepared: &[&Prepared],
    tier: Tier,
) -> BenchResult<(Vec<Arc<Table>>, Option<TempDir>)> {
    let storage = Arc::new(Storage::new(StorageConfig::default()));
    let (local, dir) = local_for(tier, &storage, None)?;
    let handles = TableHandles {
        storage,
        cache: PageCache::new(WARM_CACHE_BYTES, None).await?,
        local,
        calls: CallsInFlight::new(),
    };
    let mut tables = Vec::with_capacity(prepared.len());
    for table in prepared {
        let served = table_over(table, tier, pinned(table), handles.clone()).await?;
        served.load().await?;
        prime_cache(&served, table.shape).await?;
        tables.push(Arc::new(served));
    }
    Ok((tables, dir))
}

/// The two-table scenario in `tier`: `settings.tasks` loops of warm hits on
/// the long table beside one loop of `MultiGet` 1,000 warm hits on the
/// string table, run together for the warm-up and then for the measured
/// window. Every loop yields between operations (`Pace::Yielding`): the hit
/// loops would otherwise hold every worker, and the batch and its runs
/// would wait for the window to end. One row per table. Neither row carries counters: both tables'
/// operations run in one window over one storage and one cache, so no
/// count belongs to either row alone.
async fn two_tables(
    prepared: &[Prepared],
    tier: Tier,
    settings: &RunSettings,
) -> BenchResult<Vec<Measured>> {
    let find = |name: TableName| {
        prepared
            .iter()
            .find(|table| table.shape.name == name)
            .ok_or_else(|| format!("no {} table was prepared", name.as_str()))
    };
    let (long, string) = (find(TableName::Long)?, find(TableName::String)?);
    let (tables, _dir) = serve_together(&[long, string], tier).await?;
    let (long_table, string_table) = (&tables[0], &tables[1]);
    let hit_op = get(&key_pools(long.shape).0, true);
    let batch_op = multi(&key_pools(string.shape).0, BATCH_1000);
    let hits = (&hit_op, Pace::Yielding);
    let batches = (&batch_op, Pace::Yielding);
    let tasks = settings.tasks;
    let one = 1;

    let until = Instant::now() + settings.warmup;
    let hit_loops = spawn_loops(long_table, hits, tasks, until, None);
    let batch_loops = spawn_loops(string_table, batches, one, until, None);
    let (warm_hits, _) = join_loops(hit_loops).await?;
    let (warm_batches, _) = join_loops(batch_loops).await?;

    let windows = approx::length(settings.windows);
    let reserve = |ops: usize, tasks: usize| Some((ops / tasks + 1) * windows * SAMPLE_HEADROOM);
    let started = Instant::now();
    let until = started + settings.duration;
    let hit_loops = spawn_loops(long_table, hits, tasks, until, reserve(warm_hits, tasks));
    let batch_loops = spawn_loops(
        string_table,
        batches,
        one,
        until,
        reserve(warm_batches, one),
    );
    let (hit_ops, mut hit_samples) = join_loops(hit_loops).await?;
    let (batch_ops, mut batch_samples) = join_loops(batch_loops).await?;
    let elapsed = started.elapsed();
    hit_samples.sort_unstable();
    batch_samples.sort_unstable();

    let label = |table, mode, tasks| RunLabel {
        scenario: Scenario::TwoTable,
        table,
        tier,
        mode,
        tasks,
    };
    let rows = vec![
        Measured {
            label: label(TableName::String, Mode::Single, one),
            ops: batch_ops,
            keys_per_op: BATCH_1000,
            elapsed,
            sorted: batch_samples,
            counters: Counters::default(),
        },
        Measured {
            label: label(TableName::Long, Mode::Multi, tasks),
            ops: hit_ops,
            keys_per_op: 1,
            elapsed,
            sorted: hit_samples,
            counters: Counters::default(),
        },
    ];
    for run in &rows {
        eprintln!("{}", row(run));
    }
    Ok(rows)
}

// ---------------------------------------------------------------------------
// Loads and refreshes.

/// How often the resident set is sampled while a load runs, for its peak.
const RSS_SAMPLE_INTERVAL: Duration = Duration::from_millis(1);

/// The process's resident set sampled on a thread of its own until stopped;
/// the largest sample is the peak of the window.
struct RssSampler {
    stop: Arc<AtomicBool>,
    handle: JoinHandle<u64>,
}

impl RssSampler {
    fn start() -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let handle = std::thread::spawn(move || {
            let mut peak = 0;
            while !flag.load(Ordering::Relaxed) {
                peak = peak.max(resident_bytes());
                std::thread::sleep(RSS_SAMPLE_INTERVAL);
            }
            peak.max(resident_bytes())
        });
        Self { stop, handle }
    }

    fn peak(self) -> u64 {
        self.stop.store(true, Ordering::Relaxed);
        self.handle.join().unwrap_or(0)
    }
}

/// The process's resident set now, or zero where the platform reports none.
fn resident_bytes() -> u64 {
    memory_stats::memory_stats().map_or(0, |stats| approx::count(stats.physical_mem))
}

/// The process's user and system CPU time so far, and its peak resident
/// set over its whole life as the kernel reports it (bytes on macOS,
/// kibibytes on Linux, recorded as reported with the platform beside it).
#[derive(Clone, Copy, Debug, Default)]
struct Usage {
    user: Duration,
    system: Duration,
    max_rss: u64,
}

impl Usage {
    fn now() -> Self {
        // SAFETY: `getrusage` writes one `rusage` into the zeroed struct it
        // is handed and reads nothing else.
        let usage = unsafe {
            let mut usage: libc::rusage = std::mem::zeroed();
            if libc::getrusage(libc::RUSAGE_SELF, &raw mut usage) != 0 {
                return Self::default();
            }
            usage
        };
        Self {
            user: cpu_time(usage.ru_utime),
            system: cpu_time(usage.ru_stime),
            max_rss: u64::try_from(usage.ru_maxrss).unwrap_or(0),
        }
    }
}

/// A `timeval` from `getrusage` as a `Duration`, at the kernel's boundary,
/// where the time arrives as bare seconds and microseconds.
fn cpu_time(value: libc::timeval) -> Duration {
    Duration::from_secs(u64::try_from(value.tv_sec).unwrap_or(0))
        + Duration::from_micros(u64::try_from(value.tv_usec).unwrap_or(0))
}

/// One measured load or refresh.
struct Loaded {
    what: &'static str,
    table: TableName,
    tier: Tier,
    round: u32,
    wall: Duration,
    cpu: Duration,
    allocations: Allocations,
    rss_before: u64,
    rss_peak: u64,
    max_rss: u64,
    gets: u64,
    local_reads: u64,
    files: usize,
    index_bytes: usize,
}

impl Loaded {
    fn to_json(&self) -> Json {
        let micros = |duration: Duration| duration.div_duration_f64(MICROSECOND);
        json!({
            "what": self.what,
            "table": self.table.as_str(),
            "tier": format!("{:?}", self.tier).to_lowercase(),
            "round": self.round,
            "wall_us": micros(self.wall),
            "cpu_us": micros(self.cpu),
            "allocations": self.allocations.calls,
            "allocated_bytes": self.allocations.bytes,
            "rss_before_bytes": self.rss_before,
            "rss_peak_bytes": self.rss_peak,
            "rss_peak_delta_bytes": self.rss_peak.saturating_sub(self.rss_before),
            "getrusage_max_rss": self.max_rss,
            "gets": self.gets,
            "local_reads": self.local_reads,
            "files": self.files,
            "index_bytes": self.index_bytes,
        })
    }
}

/// Times `work` against `served`: wall and CPU time, allocations, the
/// resident set before and at its sampled peak, and the storage reads.
async fn timed<F: Future<Output = BenchResult<()>>>(
    served: &Served,
    what: &'static str,
    (table, tier, round): (TableName, Tier, u32),
    work: F,
) -> BenchResult<Loaded> {
    let rss_before = resident_bytes();
    let before = Counters::read(served);
    let usage = Usage::now();
    let sampler = RssSampler::start();
    let started = Instant::now();
    work.await?;
    let wall = started.elapsed();
    let rss_peak = sampler.peak();
    let after = Usage::now();
    let counters = Counters::read(served).since(before);
    let generation = served.table.serving()?;
    Ok(Loaded {
        what,
        table,
        tier,
        round,
        wall,
        cpu: (after.user + after.system).saturating_sub(usage.user + usage.system),
        allocations: counters.allocations,
        rss_before,
        rss_peak,
        max_rss: after.max_rss,
        gets: counters.gets,
        local_reads: counters.local_reads,
        files: generation.files.len(),
        index_bytes: generation.index_bytes,
    })
}

/// `rounds` first loads of `prepared` in `tier`, each over fresh storage,
/// cache, and local directory; under Disk, each followed by a load over the
/// same directory, which reuses the chunk files the first wrote.
async fn loads(prepared: &Prepared, tier: Tier, rounds: u32) -> BenchResult<Vec<Loaded>> {
    let mut results = Vec::new();
    for round in 0..rounds {
        let key = (prepared.shape.name, tier, round);
        let served = serve(prepared, tier, WARM_CACHE_BYTES, pinned(prepared), None).await?;
        let run = timed(&served, "load", key, async {
            Ok(served.table.load().await?)
        })
        .await?;
        eprintln!("{}", load_row(&run));
        results.push(run);
        if tier == Tier::Disk {
            let Served { _local: dir, .. } = served;
            let again = serve(prepared, tier, WARM_CACHE_BYTES, pinned(prepared), dir).await?;
            let run = timed(&again, "load-reusing-chunk-files", key, async {
                Ok(again.table.load().await?)
            })
            .await?;
            eprintln!("{}", load_row(&run));
            results.push(run);
        }
    }
    Ok(results)
}

/// `rounds` refreshes of `prepared` in `tier`: the table follows its root's
/// version hint, is loaded at the first build, and the hint is moved to the
/// second, whose appended files the refresh loads beside the reused ones;
/// then an unchanged poll, which reads the hint alone.
async fn refreshes(prepared: &Prepared, tier: Tier, rounds: u32) -> BenchResult<Vec<Loaded>> {
    let hint = local_path(&catalog::hint_location(&prepared.root))?;
    let source = SourceConfig::Static {
        location: prepared.root.clone(),
    };
    let mut results = Vec::new();
    for round in 0..rounds {
        let key = (prepared.shape.name, tier, round);
        std::fs::write(&hint, &prepared.first)?;
        let served = serve(prepared, tier, WARM_CACHE_BYTES, source.clone(), None).await?;
        served.table.load().await?;
        std::fs::write(&hint, &prepared.second)?;
        let run = timed(&served, "refresh-appended", key, async {
            if served.table.refresh().await? {
                Ok(())
            } else {
                Err("the moved hint did not swap a generation".into())
            }
        })
        .await?;
        eprintln!("{}", load_row(&run));
        results.push(run);
        let run = timed(&served, "refresh-unchanged", key, async {
            if served.table.refresh().await? {
                Err("an unchanged hint swapped a generation".into())
            } else {
                Ok(())
            }
        })
        .await?;
        eprintln!("{}", load_row(&run));
        results.push(run);
    }
    std::fs::write(&hint, &prepared.first)?;
    Ok(results)
}

// ---------------------------------------------------------------------------
// Reporting.

/// Bytes per mebibyte, for the printed sizes.
const MEBIBYTE: f64 = 1_048_576.0;

fn mib(bytes: u64) -> f64 {
    approx::gauge(bytes) / MEBIBYTE
}

/// The column headings of the lookup table.
fn header() -> String {
    format!(
        "{:<15} {:<7} {:<6} {:<7} {:>5} {:>11} {:>10} {:>10} {:>10} {:>10} {:>9} {:>10} {:>8} {:>8}",
        "scenario",
        "table",
        "tier",
        "mode",
        "tasks",
        "ops/s",
        "p50",
        "p90",
        "p99",
        "max",
        "allocs/op",
        "bytes/op",
        "fetch/op",
        "reads/op"
    )
}

fn row(run: &Measured) -> String {
    let counters = run.counters;
    format!(
        "{:<15} {:<7} {:<6} {:<7} {:>5} {:>11.0} {:>10.2?} {:>10.2?} {:>10.2?} {:>10.2?} {:>9.1} {:>10.0} {:>8.3} {:>8.3}",
        run.label.scenario.name(),
        run.label.table.as_str(),
        format!("{:?}", run.label.tier).to_lowercase(),
        run.label.mode.name(),
        run.label.tasks,
        run.ops_per_second(),
        run.percentile(P50),
        run.percentile(P90),
        run.percentile(P99),
        run.max(),
        run.per_op(counters.allocations.calls),
        run.per_op(counters.allocations.bytes),
        run.per_op(counters.fetches),
        run.per_op(counters.gets + counters.local_reads),
    )
}

fn load_header() -> String {
    format!(
        "{:<26} {:<7} {:<6} {:>5} {:>10} {:>10} {:>11} {:>10} {:>10} {:>9} {:>6} {:>10}",
        "what",
        "table",
        "tier",
        "round",
        "wall",
        "cpu",
        "allocs",
        "alloc MiB",
        "peak +MiB",
        "reads",
        "files",
        "index MiB"
    )
}

fn load_row(run: &Loaded) -> String {
    format!(
        "{:<26} {:<7} {:<6} {:>5} {:>10.1?} {:>10.1?} {:>11} {:>10.1} {:>10.1} {:>9} {:>6} {:>10.1}",
        run.what,
        run.table.as_str(),
        format!("{:?}", run.tier).to_lowercase(),
        run.round,
        run.wall,
        run.cpu,
        run.allocations.calls,
        mib(run.allocations.bytes),
        mib(run.rss_peak.saturating_sub(run.rss_before)),
        run.gets + run.local_reads,
        run.files,
        mib(approx::count(run.index_bytes)),
    )
}

/// The first line `program args` prints, trimmed, or `None` when it cannot
/// run or prints nothing.
fn command_line(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program).args(args).output().ok()?;
    let text = String::from_utf8(output.stdout).ok()?;
    let line = text.lines().next()?.trim().to_string();
    (!line.is_empty()).then_some(line)
}

/// The CPU model: `sysctl` on macOS, `/proc/cpuinfo` on Linux.
fn cpu_model() -> String {
    if let Some(model) = command_line("sysctl", &["-n", "machdep.cpu.brand_string"]) {
        return model;
    }
    let mut text = String::new();
    if let Ok(mut file) = std::fs::File::open("/proc/cpuinfo")
        && file.read_to_string(&mut text).is_ok()
        && let Some(line) = text.lines().find(|line| line.starts_with("model name"))
        && let Some((_, model)) = line.split_once(':')
    {
        return model.trim().to_string();
    }
    "unknown".to_string()
}

/// The machine and build the numbers were measured on.
fn machine(tasks: usize, metrics: bool) -> Json {
    json!({
        "cpu": cpu_model(),
        "cores": std::thread::available_parallelism().map_or(0, std::num::NonZero::get),
        "tasks": tasks,
        "os": command_line("uname", &["-srm"]).unwrap_or_else(|| std::env::consts::OS.to_string()),
        "os_version": command_line("sw_vers", &["-productVersion"]),
        "memory_bytes": command_line("sysctl", &["-n", "hw.memsize"]),
        "commit": command_line("git", &["rev-parse", "--short", "HEAD"]),
        "optimized": !cfg!(debug_assertions),
        "metrics_recorder": metrics,
        "allocator": "jemalloc behind a counting wrapper",
    })
}

/// Writes `report` to `<out_dir>/<timestamp>.json` and returns the path.
fn write_report(out_dir: &Path, report: &Json) -> BenchResult<PathBuf> {
    std::fs::create_dir_all(out_dir)?;
    let stamp = time::log_timestamp(time::now()).replace(':', "-");
    let path = out_dir.join(format!("{stamp}.json"));
    std::fs::write(&path, serde_json::to_vec_pretty(report)?)?;
    Ok(path)
}

// ---------------------------------------------------------------------------
// The program.

/// A selection flag's values, or every value when the flag was not given.
fn or_all<T: Copy>(chosen: &[T], all: &[T]) -> Vec<T> {
    if chosen.is_empty() {
        all.to_vec()
    } else {
        chosen.to_vec()
    }
}

/// Installs the binary's metrics recorder and drains its histograms on the
/// binary's schedule, as `serve` does, so the measured paths pay what the
/// daemon's do.
fn install_recorder() -> BenchResult<()> {
    let handle = icetray::telemetry::install()?;
    let period = ServerConfig::default().poll_interval;
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(period);
        loop {
            ticker.tick().await;
            handle.run_upkeep();
        }
    });
    Ok(())
}

#[tokio::main]
async fn main() -> BenchResult<()> {
    let cli = Cli::parse();
    if cfg!(debug_assertions) {
        eprintln!("warning: an unoptimized build; pass --release or --profile profiling");
    }
    let prepared = prepare(&cli.data_dir, cli.rewrite).await?;
    if cli.prepare {
        for table in &prepared {
            println!(
                "{}\t{:?}\t{ROWS}\t{}",
                table.shape.name.as_str(),
                table.shape.key_type,
                table.first
            );
        }
        return Ok(());
    }
    if !cli.no_metrics {
        install_recorder()?;
    }
    let primed = prime_os_cache(&cli.data_dir)?;
    eprintln!("read {:.0} MiB to prime the OS page cache", mib(primed));
    let tasks = cli
        .tasks
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, std::num::NonZero::get));
    let settings = RunSettings {
        modes: or_all(&cli.mode, &Mode::ALL),
        tasks,
        warmup: Duration::from_secs(cli.warmup),
        duration: Duration::from_secs(cli.duration),
        windows: cli.duration.div_ceil(cli.warmup),
    };
    let scenarios = or_all(&cli.scenario, &Scenario::ALL);
    let lookup_scenarios: Vec<Scenario> = scenarios
        .iter()
        .copied()
        .filter(|s| Scenario::LOOKUPS.contains(s))
        .collect();
    let tiers = or_all(&cli.tier, &Tier::ALL);
    let tables: Vec<&Prepared> = prepared
        .iter()
        .filter(|table| cli.table.is_empty() || cli.table.contains(&table.shape.name))
        .collect();
    let machine = machine(tasks, !cli.no_metrics);
    eprintln!("{machine}");
    eprintln!("{}", header());
    let mut measured = Vec::new();
    let mut loaded = Vec::new();
    for table in &tables {
        for &tier in &tiers {
            if !lookup_scenarios.is_empty() {
                measured.extend(lookups(table, tier, &lookup_scenarios, &settings).await?);
            }
            if scenarios.contains(&Scenario::Load) {
                loaded.extend(loads(table, tier, cli.load_rounds).await?);
            }
            if scenarios.contains(&Scenario::Refresh) {
                loaded.extend(refreshes(table, tier, cli.load_rounds).await?);
            }
        }
    }
    if scenarios.contains(&Scenario::TwoTable) {
        for &tier in &tiers {
            measured.extend(two_tables(&prepared, tier, &settings).await?);
        }
    }
    println!("{machine}");
    println!("{}", header());
    for run in &measured {
        println!("{}", row(run));
    }
    if !loaded.is_empty() {
        println!("{}", load_header());
        for run in &loaded {
            println!("{}", load_row(run));
        }
    }
    let report = json!({
        "machine": machine,
        "settings": {
            "rows": ROWS,
            "appended_rows": APPENDED_ROWS,
            "key_pool": KEY_POOL,
            "key_seed": KEY_SEED,
            "warm_cache_bytes": WARM_CACHE_BYTES,
            "cold_cache_bytes": COLD_CACHE_BYTES,
            "warmup_seconds": cli.warmup,
            "duration_seconds": cli.duration,
            "tables": prepared.iter().map(Prepared::to_json).collect::<Vec<_>>(),
        },
        "lookups": measured.iter().map(Measured::to_json).collect::<Vec<_>>(),
        "loads": loaded.iter().map(Loaded::to_json).collect::<Vec<_>>(),
    });
    let path = write_report(&cli.out_dir, &report)?;
    println!("results: {}", path.display());
    Ok(())
}
