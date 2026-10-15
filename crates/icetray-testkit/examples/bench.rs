//! The benchmark program, a program and not a test: the in-process lookup
//! and load paths measured over contract-shaped tables on the local
//! filesystem, so every number is icetray's own CPU and allocation cost with
//! no object-store latency in it. It writes a long-key and a string-key
//! table of `ROWS` rows each, once with random values and once with text
//! values (`Values`); two long-key tables of protobuf values beside a
//! second value column, one of `ROWS` rows at the other tables' value size
//! and one of `LARGE_VALUE_ROWS` rows of 4 KiB values, served with `value`
//! declared protobuf (RFD 022, RFD 024); ordered tables (RFD 025): one of
//! `ORDERED_KEYS` long keys of `ORDERED_ROWS_PER_KEY` rows each by a
//! timestamp order column, which the first Scan and `MultiScan` scenarios
//! read and beside which the key-value scenarios also run, two of
//! `LONG_RUN_ROWS` rows per key, one by a timestamp and one by a string
//! order column whose values share a long prefix inside which the writer
//! cut the column index, which the long-run Scan scenarios read, and two
//! only loaded and refreshed, one of files rolled by size that share edge
//! keys and one of overlapping appends of short runs; and, when `--table
//! scattered` names it, a table shaped like a serving table
//! (`SCATTERED_ROWS` rows of random-looking string keys and 4 KiB values in
//! byte-bounded pages, `SCATTERED_ROWS_PER_FILE` rows per file), through
//! the fixture writer (once, then reused from `--data-dir`), reports how
//! far zstd compressed each table's key and value columns, serves them
//! through `Table` over `Storage`'s `file://` store in every residency
//! tier, and reports latency percentiles, throughput, allocations per
//! operation, the bytes an operation read from the store and the local
//! directory, the value bytes it read and returned, and for a Scan the
//! rounds of GETs it waited on (`RoundsStore`); plus the wall time, CPU
//! time, allocations, reads, and peak resident set of a load, of a load
//! over the chunk files a Disk load left (a restart), and of a refresh,
//! with the resident index's bytes by part and the chunk files' bytes.
//! Results print as a table and are written as JSON under `--out-dir`. Run
//! with `cargo run -p icetray-testkit --example bench --release -- --help`;
//! a profile of one scenario narrows `--scenario`, `--tier`, `--table`,
//! `--values`, and `--mode`, and lengthens `--duration`.

use std::alloc::{GlobalAlloc, Layout};
use std::error::Error;
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use iceberg::spec::MAIN_BRANCH;
use icetray_core::ColumnRead;
use icetray_core::config::{
    LocalConfig, Residency, RewriteRule, ServerConfig, SourceConfig, StorageConfig, TableConfig,
    ValueColumnConfig, ValueFormat,
};
use icetray_core::contract::{TARGET_FILE_BYTES_MAX, VALUE_PAGE_BYTES};
use icetray_core::{
    CallsInFlight, Direction, FILE_PREFIX, FindingLog, Key, KeyType, Level, LocalDir, OrderValue,
    PageCache, ResidentParts, Row, ScanBounds, ScanRow, Scanned, Storage, Table, TableHandles,
    TableSource,
};
use icetray_core::{approx, catalog, time};
use icetray_testkit::fixture::{NAMESPACE, SECOND_VALUE_COLUMN, memory_catalog_local};
use icetray_testkit::keys::{
    FIRST_VERSION, SCATTERED_ID_SPACE, SCATTERED_KEY_FIXED_BYTES, STRING_KEY_FIXED_BYTES,
    VALUE_BYTES,
};
use icetray_testkit::latency::{Latencies, P50, P90, P99, micros};
use icetray_testkit::layout::{KEY_COLUMN, Layout as FileLayout, VALUE_COLUMN};
use icetray_testkit::protobuf::{
    CHECKSUM_FIELD, FILLER_FIELD, FLAGS_FIELD, GROUP_FIELD, ID_FIELD, NAME_FIELD, SCORES_FIELD,
};
use icetray_testkit::{
    FixtureSpec, KeyShape, MemoryCatalog, ORDER_COLUMN, OrderKind, Ordered, RoundsStore, TestScan,
    ValueKind, absent_key_from, key_at_shaped, order_value, row_value, scan_model, value_of_kind,
    write_fixture,
};
use object_store::ObjectStore;
use parquet::file::reader::{FileReader as _, SerializedFileReader};
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
    /// Two tables served in one process: `--tasks` tasks of warm hits on
    /// the long table of random values beside one task looping a
    /// `MultiGet` of 1,000 warm hits on the string table of random values.
    /// Serves those two whatever `--table` and `--values` select.
    TwoTable,
    /// A warm hit naming both value columns, `value` and `detail`, whole:
    /// against `warm-hit` on the same table, the cost of a second column.
    /// Protobuf tables only.
    WarmHitBoth,
    /// A warm hit naming no column, which answers presence alone.
    PresenceHit,
    /// A warm hit projecting `value` to `PROJECT_FEW`: against `warm-hit`
    /// on the same table, the walk's cost and the bytes it saves. Protobuf
    /// tables only.
    WarmHitProjectFew,
    /// A warm hit projecting `value` to `PROJECT_MOST`, a walk that copies
    /// nearly the whole value. Protobuf tables only.
    WarmHitProjectMost,
    /// A `MultiGet` of 100 warm hits naming both value columns. Protobuf
    /// tables only.
    #[value(name = "multi-get-100-both")]
    MultiGet100Both,
    /// A `MultiGet` of 100 warm hits projecting `value` to `PROJECT_FEW`.
    /// Protobuf tables only.
    #[value(name = "multi-get-100-project")]
    MultiGet100Project,
    /// A cold hit naming both value columns: against `cold-hit`, what a
    /// second column's concurrent GET adds to the latency. Protobuf tables
    /// only.
    ColdHitBoth,
    /// A cold hit projecting `value` to `PROJECT_FEW`: against `cold-hit`,
    /// the walk's cost beside a GET. Protobuf tables only.
    ColdHitProjectFew,
    /// A `MultiGet` of 100 cold hits: the batch fan-out under GETs.
    #[value(name = "multi-get-100-cold")]
    MultiGet100Cold,
    /// A `MultiGet` of 100 cold hits naming both value columns, two GETs a
    /// key under one fan-out. Protobuf tables only.
    #[value(name = "multi-get-100-cold-both")]
    MultiGet100ColdBoth,
    /// A Scan of one key's whole run, newest first, whose pages the scan
    /// cache holds. Ordered table only.
    ScanWarm,
    /// A Scan of one key's whole run through the `COLD_CACHE_BYTES` scan
    /// cache, which holds almost none of the table's pages. Ordered table
    /// only.
    ScanCold,
    /// A Scan of `SCAN_PAGE_ROWS` rows, newest first, inside a window over
    /// the middle half of the key's run, warm: one page of a read paged by
    /// order value. Ordered table only.
    ScanBounded,
    /// A `MultiScan` of `MULTI_SCAN_KEYS` keys at the largest limit
    /// `server.max_multi_scan_values` admits for them, warm. Ordered table
    /// only.
    #[value(name = "multi-scan-10")]
    MultiScan10,
    /// `multi-scan-10` through the `COLD_CACHE_BYTES` scan cache. Ordered
    /// table only.
    #[value(name = "multi-scan-10-cold")]
    MultiScan10Cold,
    /// The key-value scenarios of `BESIDE_ORDERED` on the long table of
    /// random values, in a process that also serves the ordered table over
    /// the same handles, the ordered table loaded, primed, and idle. Serves
    /// those two whatever `--table` and `--values` select.
    KvBesideOrdered,
    /// A Scan of `SCAN_PAGE_ROWS` rows, newest first, before an order value
    /// drawn from the middle half of a key's `LONG_RUN_ROWS`-row run
    /// (`bounded_in_run`), warm: a page deep in a long history. Long-run
    /// table only.
    ScanBoundedLong,
    /// `scan-bounded-long` through the `COLD_CACHE_BYTES` scan cache.
    /// Long-run table only.
    ScanBoundedLongCold,
    /// A Scan of the `HEAD_ROWS` newest rows of a key's long run, no bound,
    /// warm. Long-run table only.
    ScanHeadLong,
    /// `scan-bounded-long` over the string-ordered table, whose order
    /// values share a long prefix inside which the writer truncated the
    /// order column's index. String-ordered table only.
    ScanBoundedString,
    /// `scan-bounded-string` through the `COLD_CACHE_BYTES` scan cache.
    /// String-ordered table only.
    ScanBoundedStringCold,
}

impl Scenario {
    const ALL: [Self; 29] = [
        Self::WarmHit,
        Self::ColdHit,
        Self::Miss,
        Self::MultiGet100,
        Self::MultiGet1000,
        Self::Load,
        Self::Refresh,
        Self::TwoTable,
        Self::WarmHitBoth,
        Self::PresenceHit,
        Self::WarmHitProjectFew,
        Self::WarmHitProjectMost,
        Self::MultiGet100Both,
        Self::MultiGet100Project,
        Self::ColdHitBoth,
        Self::ColdHitProjectFew,
        Self::MultiGet100Cold,
        Self::MultiGet100ColdBoth,
        Self::ScanWarm,
        Self::ScanCold,
        Self::ScanBounded,
        Self::MultiScan10,
        Self::MultiScan10Cold,
        Self::KvBesideOrdered,
        Self::ScanBoundedLong,
        Self::ScanBoundedLongCold,
        Self::ScanHeadLong,
        Self::ScanBoundedString,
        Self::ScanBoundedStringCold,
    ];

    const LOOKUPS: [Self; 25] = [
        Self::WarmHit,
        Self::ColdHit,
        Self::Miss,
        Self::MultiGet100,
        Self::MultiGet1000,
        Self::WarmHitBoth,
        Self::PresenceHit,
        Self::WarmHitProjectFew,
        Self::WarmHitProjectMost,
        Self::MultiGet100Both,
        Self::MultiGet100Project,
        Self::ColdHitBoth,
        Self::ColdHitProjectFew,
        Self::MultiGet100Cold,
        Self::MultiGet100ColdBoth,
        Self::ScanWarm,
        Self::ScanCold,
        Self::ScanBounded,
        Self::MultiScan10,
        Self::MultiScan10Cold,
        Self::ScanBoundedLong,
        Self::ScanBoundedLongCold,
        Self::ScanHeadLong,
        Self::ScanBoundedString,
        Self::ScanBoundedStringCold,
    ];

    /// The key-value scenarios `kv-beside-ordered` runs on the long table
    /// beside the ordered table.
    const BESIDE_ORDERED: [Self; 4] = [Self::WarmHit, Self::ColdHit, Self::Miss, Self::MultiGet100];

    /// Whether the scenario measures a page cache primed to hold every
    /// value page.
    fn is_warm(self) -> bool {
        match self {
            Self::WarmHit
            | Self::MultiGet100
            | Self::MultiGet1000
            | Self::TwoTable
            | Self::WarmHitBoth
            | Self::PresenceHit
            | Self::WarmHitProjectFew
            | Self::WarmHitProjectMost
            | Self::MultiGet100Both
            | Self::MultiGet100Project
            | Self::ScanWarm
            | Self::ScanBounded
            | Self::MultiScan10
            | Self::KvBesideOrdered
            | Self::ScanBoundedLong
            | Self::ScanHeadLong
            | Self::ScanBoundedString => true,
            Self::ColdHit
            | Self::Miss
            | Self::Load
            | Self::Refresh
            | Self::ColdHitBoth
            | Self::ColdHitProjectFew
            | Self::MultiGet100Cold
            | Self::MultiGet100ColdBoth
            | Self::ScanCold
            | Self::MultiScan10Cold
            | Self::ScanBoundedLongCold
            | Self::ScanBoundedStringCold => false,
        }
    }

    /// Whether the scenario measures reads through the `COLD_CACHE_BYTES`
    /// page cache and scan cache, which hold almost none of the table's
    /// value pages.
    fn is_cold(self) -> bool {
        matches!(
            self,
            Self::ColdHit
                | Self::ColdHitBoth
                | Self::ColdHitProjectFew
                | Self::MultiGet100Cold
                | Self::MultiGet100ColdBoth
                | Self::ScanCold
                | Self::MultiScan10Cold
                | Self::ScanBoundedLongCold
                | Self::ScanBoundedStringCold
        )
    }

    /// Whether the scenario reads `SECOND_VALUE_COLUMN` or projects
    /// `VALUE_COLUMN`, which only a protobuf table serves.
    fn needs_protobuf(self) -> bool {
        match self {
            Self::WarmHitBoth
            | Self::WarmHitProjectFew
            | Self::WarmHitProjectMost
            | Self::MultiGet100Both
            | Self::MultiGet100Project
            | Self::ColdHitBoth
            | Self::ColdHitProjectFew
            | Self::MultiGet100ColdBoth => true,
            Self::WarmHit
            | Self::ColdHit
            | Self::Miss
            | Self::MultiGet100
            | Self::MultiGet1000
            | Self::Load
            | Self::Refresh
            | Self::TwoTable
            | Self::PresenceHit
            | Self::MultiGet100Cold
            | Self::ScanWarm
            | Self::ScanCold
            | Self::ScanBounded
            | Self::MultiScan10
            | Self::MultiScan10Cold
            | Self::KvBesideOrdered
            | Self::ScanBoundedLong
            | Self::ScanBoundedLongCold
            | Self::ScanHeadLong
            | Self::ScanBoundedString
            | Self::ScanBoundedStringCold => false,
        }
    }

    /// Whether the scenario projects `VALUE_COLUMN`, whose bytes read per
    /// key are then the whole value's, not what it returns.
    fn projects(self) -> bool {
        match self {
            Self::WarmHitProjectFew
            | Self::WarmHitProjectMost
            | Self::MultiGet100Project
            | Self::ColdHitProjectFew => true,
            Self::WarmHit
            | Self::ColdHit
            | Self::Miss
            | Self::MultiGet100
            | Self::MultiGet1000
            | Self::Load
            | Self::Refresh
            | Self::TwoTable
            | Self::WarmHitBoth
            | Self::PresenceHit
            | Self::MultiGet100Both
            | Self::ColdHitBoth
            | Self::MultiGet100Cold
            | Self::MultiGet100ColdBoth
            | Self::ScanWarm
            | Self::ScanCold
            | Self::ScanBounded
            | Self::MultiScan10
            | Self::MultiScan10Cold
            | Self::KvBesideOrdered
            | Self::ScanBoundedLong
            | Self::ScanBoundedLongCold
            | Self::ScanHeadLong
            | Self::ScanBoundedString
            | Self::ScanBoundedStringCold => false,
        }
    }

    /// The ordered table a Scan or `MultiScan` scenario reads, the only one
    /// it runs on; `None` for the other scenarios, which only a key-value
    /// table serves.
    fn scan_table(self) -> Option<TableName> {
        match self {
            Self::ScanWarm
            | Self::ScanCold
            | Self::ScanBounded
            | Self::MultiScan10
            | Self::MultiScan10Cold => Some(TableName::Ordered),
            Self::ScanBoundedLong | Self::ScanBoundedLongCold | Self::ScanHeadLong => {
                Some(TableName::OrderedLong)
            }
            Self::ScanBoundedString | Self::ScanBoundedStringCold => Some(TableName::OrderedString),
            Self::WarmHit
            | Self::ColdHit
            | Self::Miss
            | Self::MultiGet100
            | Self::MultiGet1000
            | Self::Load
            | Self::Refresh
            | Self::TwoTable
            | Self::WarmHitBoth
            | Self::PresenceHit
            | Self::WarmHitProjectFew
            | Self::WarmHitProjectMost
            | Self::MultiGet100Both
            | Self::MultiGet100Project
            | Self::ColdHitBoth
            | Self::ColdHitProjectFew
            | Self::MultiGet100Cold
            | Self::MultiGet100ColdBoth
            | Self::KvBesideOrdered => None,
        }
    }

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
            Self::WarmHitBoth => "warm-hit-both",
            Self::PresenceHit => "presence-hit",
            Self::WarmHitProjectFew => "warm-hit-project-few",
            Self::WarmHitProjectMost => "warm-hit-project-most",
            Self::MultiGet100Both => "multi-get-100-both",
            Self::MultiGet100Project => "multi-get-100-project",
            Self::ColdHitBoth => "cold-hit-both",
            Self::ColdHitProjectFew => "cold-hit-project-few",
            Self::MultiGet100Cold => "multi-get-100-cold",
            Self::MultiGet100ColdBoth => "multi-get-100-cold-both",
            Self::ScanWarm => "scan-warm",
            Self::ScanCold => "scan-cold",
            Self::ScanBounded => "scan-bounded",
            Self::MultiScan10 => "multi-scan-10",
            Self::MultiScan10Cold => "multi-scan-10-cold",
            Self::KvBesideOrdered => "kv-beside-ordered",
            Self::ScanBoundedLong => "scan-bounded-long",
            Self::ScanBoundedLongCold => "scan-bounded-long-cold",
            Self::ScanHeadLong => "scan-head-long",
            Self::ScanBoundedString => "scan-bounded-string",
            Self::ScanBoundedStringCold => "scan-bounded-string-cold",
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

/// A benchmark table, by its keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum TableName {
    /// Long keys, contract layout; under `--values protobuf-large`, value
    /// pages bounded by bytes.
    Long,
    /// Sequential string keys sharing a long prefix, contract layout.
    String,
    /// A table shaped like a serving table: random-looking string keys,
    /// 4 KiB random values in 64 KiB value pages, and about 37,500 rows per
    /// file. Runs only when named, and only its load, refresh, cold-hit,
    /// and miss scenarios: its value pages outrun the warm cache.
    Scattered,
    /// Long keys of `ORDERED_ROWS_PER_KEY` rows each by a timestamp order
    /// column, contract layout: the table the first Scan and `MultiScan`
    /// scenarios read, and the only one they read.
    Ordered,
    /// Long keys of `LONG_RUN_ROWS` rows each by a timestamp order column,
    /// contract layout, so a run spans many order and value pages: the
    /// table `scan-bounded-long` and `scan-head-long` read.
    OrderedLong,
    /// Long keys of `LONG_RUN_ROWS` rows each by a string order column of
    /// `STRING_ORDER_WIDTH` digits, whose values share their first
    /// `STRING_ORDER_PREFIX_BYTES` bytes, written with the column index cut
    /// to `ORDER_INDEX_TRUNCATE_BYTES`, inside that prefix: the table
    /// `scan-bounded-string` reads.
    OrderedString,
    /// The long-run table's rows in files rolled at `ROLLED_FILE_BYTES`, as
    /// a compaction that rolls files by size writes them: several files of
    /// one snapshot per bucket, which meet where one file's last key is the
    /// next one's first. Served at `balanced`; loads and refreshes only.
    OrderedRolled,
    /// `ORDERED_KEYS` long keys of `SHORT_RUN_ROWS` rows each, then
    /// `OVERLAPPING_APPENDS` appends of `SHORT_RUN_ROWS` later rows per key,
    /// each starting at a key's own offset (`overlapping_rows`), so every
    /// append's order range overlaps the one before it while no key's runs
    /// do; the second build is one more such append. Served at
    /// `permissive`; loads and refreshes only.
    OrderedAppended,
}

/// What a benchmark table's values are made of.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Values {
    /// Pseudo-random bytes, which zstd does not shrink: a value page's
    /// decompression is near its cheapest.
    Random,
    /// JSON-like records with repeated field names and a skewed vocabulary
    /// (`text_value_for_len`), which zstd compresses as structured values
    /// compress.
    Text,
    /// Protobuf messages of every wire type padded by a repeated filler
    /// field (`protobuf::value`), at the random table's value size, in
    /// `value` declared protobuf, beside `detail`, each row's value
    /// reversed, opaque.
    Protobuf,
    /// As `Protobuf`, at `SERVING_VALUE_BYTES` per value and
    /// `LARGE_VALUE_ROWS` rows, in value pages bounded by bytes as a
    /// serving table's are.
    ProtobufLarge,
}

impl Values {
    const ALL: [Self; 4] = [
        Self::Random,
        Self::Text,
        Self::Protobuf,
        Self::ProtobufLarge,
    ];

    /// The values' name in the results and, after the key column's, in a
    /// table's name.
    fn as_str(self) -> &'static str {
        match self {
            Self::Random => "random",
            Self::Text => "text",
            Self::Protobuf => "protobuf",
            Self::ProtobufLarge => "protobuf_large",
        }
    }

    fn kind(self) -> ValueKind {
        match self {
            Self::Random => ValueKind::Random,
            Self::Text => ValueKind::Text,
            Self::Protobuf | Self::ProtobufLarge => ValueKind::Protobuf,
        }
    }

    /// Whether the table holds `SECOND_VALUE_COLUMN` and is served with
    /// `VALUE_COLUMN` declared protobuf.
    fn is_protobuf(self) -> bool {
        match self {
            Self::Protobuf | Self::ProtobufLarge => true,
            Self::Random | Self::Text => false,
        }
    }
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
    /// Tables to run them over, by keys; `scattered` runs only when named.
    #[arg(long, value_enum, value_delimiter = ',')]
    table: Vec<TableName>,
    /// Tables to run them over, by values; the protobuf values are on long
    /// keys alone.
    #[arg(long, value_enum, value_delimiter = ',')]
    values: Vec<Values>,
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
    /// Write the tables again even when `--data-dir` holds them; the
    /// directory is emptied first.
    #[arg(long)]
    rewrite: bool,
    /// Write or reuse the selected tables, print one line per table (name,
    /// key type, rows, and the first build's metadata file), and exit.
    #[arg(long)]
    prepare: bool,
    /// Measure without the binary's Prometheus recorder installed, so the
    /// difference against a run with it is the metrics' cost.
    #[arg(long)]
    no_metrics: bool,
    /// A location rewrite, `FROM=TO`, applied to every location the tables'
    /// metadata names, as `[[storage.rewrite]]` applies it; repeatable. A
    /// table written under a local directory and copied to object storage
    /// is read from the copy with
    /// `--rewrite-location file:///dir/=s3://bucket/dir/`.
    #[arg(long, value_parser = parse_rewrite)]
    rewrite_location: Vec<RewriteRule>,
    /// The GETs one `MultiGet` holds in flight on every table of the run,
    /// as `server.batch_fan_out` bounds them; its default when absent.
    #[arg(
        long,
        default_value_t = ServerConfig::default().batch_fan_out,
        value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..),
    )]
    batch_fan_out: usize,
}

/// What separates a `--rewrite-location` rule's two prefixes.
const REWRITE_SEPARATOR: char = '=';

/// A `--rewrite-location` rule from `FROM=TO`, split at the first `=`.
fn parse_rewrite(text: &str) -> Result<RewriteRule, String> {
    let (from, to) = text
        .split_once(REWRITE_SEPARATOR)
        .ok_or_else(|| format!("{text:?} is not FROM=TO"))?;
    Ok(RewriteRule {
        from: from.to_string(),
        to: to.to_string(),
    })
}

/// How every table of the run is served: over the production storage
/// defaults with the run's `--rewrite-location` rules, under the run's
/// `--batch-fan-out`.
#[derive(Clone, Debug)]
struct Serving {
    storage: StorageConfig,
    batch_fan_out: usize,
}

impl Default for Serving {
    fn default() -> Self {
        Self {
            storage: StorageConfig::default(),
            batch_fan_out: ServerConfig::default().batch_fan_out,
        }
    }
}

/// The run's `Serving`.
static SERVING: OnceLock<Serving> = OnceLock::new();

/// Sets `SERVING` from `cli`'s `--rewrite-location` rules and
/// `--batch-fan-out`, once.
fn set_serving(cli: &Cli) {
    SERVING
        .set(Serving {
            storage: StorageConfig {
                rewrite: cli.rewrite_location.clone(),
                ..StorageConfig::default()
            },
            batch_fan_out: cli.batch_fan_out,
        })
        .expect("main sets the serving configuration once");
}

/// `SERVING`, or the defaults before `main` sets it.
fn serving() -> Serving {
    SERVING.get().cloned().unwrap_or_default()
}

// ---------------------------------------------------------------------------
// The tables.

/// Rows of each contract-shaped benchmark table's first build.
const ROWS: u64 = 2_000_000;

/// A table's first build over its second: the second appends one percent,
/// a small file per bucket, the refresh scenario's new snapshot.
const APPEND_DIVISOR: u64 = 100;

/// Rows the second build appends above the first's ids.
const APPENDED_ROWS: u64 = ROWS / APPEND_DIVISOR;

/// Rows per file of the scattered table: the mean of a serving table of
/// 1.2 billion rows over 32,768 buckets, so the table's per-file work
/// (footers, page indexes, key chunks) is in proportion to its rows as it
/// is there.
const SCATTERED_ROWS_PER_FILE: u64 = 37_500;

/// Buckets of the scattered table, one file each: enough that per-file
/// costs show in a load's profile, few enough that the table writes in a
/// few minutes and takes about 5 GB of disk.
const SCATTERED_BUCKETS: u64 = 32;

/// Rows of the scattered table's first build: `SCATTERED_ROWS_PER_FILE`
/// in each of `SCATTERED_BUCKETS`, 1.2 million.
const SCATTERED_ROWS: u64 = SCATTERED_BUCKETS * SCATTERED_ROWS_PER_FILE;

/// Rows the scattered table's second build appends.
const SCATTERED_APPENDED_ROWS: u64 = SCATTERED_ROWS / APPEND_DIVISOR;

// Both builds' ids have scattered keys.
const _: () = assert!(SCATTERED_ROWS + SCATTERED_APPENDED_ROWS <= SCATTERED_ID_SPACE);

/// Bytes per value of the scattered table and of the large protobuf
/// table: 4 KiB, a serving table's value, so a 64 KiB value page holds
/// about sixteen.
const SERVING_VALUE_BYTES: usize = 4_096;

/// Rows of the large protobuf table's first build: two columns of
/// `SERVING_VALUE_BYTES` per row, about 780 MiB of value pages, which
/// `WARM_CACHE_BYTES` holds with the key pages beside, so the table runs
/// warm, and about 800 MiB of disk.
const LARGE_VALUE_ROWS: u64 = 100_000;

/// Rows the large protobuf table's second build appends.
const LARGE_VALUE_APPENDED_ROWS: u64 = LARGE_VALUE_ROWS / APPEND_DIVISOR;

/// Rows per key of the ordered table: a time series of 100 points, a
/// tenth of the default `max_scan_rows`, so a Scan of limit 0 returns the
/// whole run, and a run of 100-byte values lies in one or two of the
/// contract's 1,000-row value pages.
const ORDERED_ROWS_PER_KEY: u64 = 100;

/// Keys of the ordered table's first build: `ROWS` rows at
/// `ORDERED_ROWS_PER_KEY` a key, as many rows as a key-value table holds.
const ORDERED_KEYS: u64 = ROWS / ORDERED_ROWS_PER_KEY;

/// Keys the ordered table's second build appends, each with its whole run.
const ORDERED_APPENDED_KEYS: u64 = ORDERED_KEYS / APPEND_DIVISOR;

/// The timestamp of every key's first row: 2026-01-01T00:00:00Z, in the
/// order column's microseconds.
const ORDER_START: i64 = 1_767_225_600_000_000;

/// The raw distance between a key's consecutive rows: one minute, in the
/// order column's microseconds.
const ORDER_STEP: i64 = 60_000_000;

/// The order column of the ordered table: every key holds
/// `ORDERED_ROWS_PER_KEY` rows a minute apart from `ORDER_START`, the same
/// order values for every key, so one answer of the model serves a Scan
/// scenario's every key (`ScanCase`).
const ORDERED: Ordered = Ordered {
    kind: OrderKind::Timestamp,
    rows_per_key: ordered_rows_per_key,
    start: ORDER_START,
    step: ORDER_STEP,
};

/// `ORDERED_ROWS_PER_KEY` for every id.
fn ordered_rows_per_key(_: u64) -> u64 {
    ORDERED_ROWS_PER_KEY
}

/// Rows per key of the long-run tables: ten times the default
/// `max_scan_rows`, so a run spans ten of the contract's 1,000-row order
/// and value pages, and reading a whole run takes ten Scans paged by order
/// value.
const LONG_RUN_ROWS: u64 = 10_000;

/// Keys of each long-run table's first build: `ROWS` rows at
/// `LONG_RUN_ROWS` a key, as many rows as a key-value table holds.
const LONG_RUN_KEYS: u64 = ROWS / LONG_RUN_ROWS;

/// Keys a long-run table's second build appends, each with its whole run.
const LONG_RUN_APPENDED_KEYS: u64 = LONG_RUN_KEYS / APPEND_DIVISOR;

/// `LONG_RUN_ROWS` for every id.
fn long_run_rows(_: u64) -> u64 {
    LONG_RUN_ROWS
}

/// The order column of the long-run and rolled tables: `ORDERED`'s
/// timestamps, `LONG_RUN_ROWS` of them a key.
const LONG_RUN: Ordered = Ordered {
    rows_per_key: long_run_rows,
    ..ORDERED
};

/// Decimal digits of the largest long, which `OrderKind::Str` pads to.
const LONG_DIGITS: u32 = i64::MAX.ilog10() + 1;

/// The bytes every order value of the string-ordered table shares at its
/// start: 64, four times `ORDER_INDEX_TRUNCATE_BYTES`, as a history whose
/// order values carry a long common prefix (a source, a partition path)
/// before the part that varies.
const STRING_ORDER_PREFIX_BYTES: u32 = 64;

/// The digits the string-ordered table's values are padded to:
/// `LONG_DIGITS` past the shared prefix, so every value starts with at
/// least `STRING_ORDER_PREFIX_BYTES` zeros.
const STRING_ORDER_WIDTH: u32 = STRING_ORDER_PREFIX_BYTES + LONG_DIGITS;

/// The column-index truncation of the string-ordered table: 16 bytes,
/// inside the shared prefix, so every order page's cut minimum and maximum
/// are the same, and the index places no bound on a page.
const ORDER_INDEX_TRUNCATE_BYTES: usize = 16;

/// The order column of the string-ordered table: `LONG_RUN`'s values as
/// strings of `STRING_ORDER_WIDTH` digits.
const LONG_STRING_RUN: Ordered = Ordered {
    kind: OrderKind::PaddedStr(STRING_ORDER_WIDTH),
    ..LONG_RUN
};

/// The file size target of the rolled table: 8 MiB, an eighth of the
/// contract's upper size, so the rolling writer splits each bucket's
/// ~58 MB into about seven files. It rolls between batches of a page's
/// rows, which a key's `LONG_RUN_ROWS` span ten of, so most files end
/// inside a key's run and share that key with the next.
const ROLLED_FILE_BYTES: usize = 8 * 1024 * 1024;

/// Rows per key in each file of the appended table: a short run, so a
/// 1,000-row page holds the runs of fifty keys.
const SHORT_RUN_ROWS: u64 = 20;

/// The appends the appended table's first build makes after its first
/// commit, each of every key's next `SHORT_RUN_ROWS` rows: five files per
/// bucket in all.
const OVERLAPPING_APPENDS: u64 = 4;

/// `SHORT_RUN_ROWS` for every id.
fn short_run_rows(_: u64) -> u64 {
    SHORT_RUN_ROWS
}

/// The order column of the appended table's first commit: `ORDERED`'s
/// timestamps, `SHORT_RUN_ROWS` of them a key; the appends that follow
/// write later ones (`overlapping_rows`).
const SHORT_RUN: Ordered = Ordered {
    rows_per_key: short_run_rows,
    ..ORDERED
};

/// The file prefix of an append within the appended table's first build,
/// followed by the append's number.
const APPEND_PREFIX: &str = "append";

/// The file size target of the scattered table: 256 MiB, above the
/// ~155 MB a bucket's incompressible rows take, so the rolling writer
/// keeps each bucket in one file of `SCATTERED_ROWS_PER_FILE` rows.
const SCATTERED_FILE_BYTES: usize = 256 * 1024 * 1024;

/// The column-index truncation of the scattered table: 256 bytes, what the
/// serving recipe sets (`parquet.columnindex.truncate.length`), so its
/// value column's index holds a truncated minimum and maximum per page as a
/// recipe-written file's does, not whole 4 KiB values.
const SCATTERED_COLUMN_INDEX_TRUNCATE: usize = 256;

/// The file prefix of the second build.
const SECOND_BUILD_PREFIX: &str = "build2";

/// The name of the file under `--data-dir` recording what was written.
const MANIFEST_FILE: &str = "manifest.json";

/// The scheme the fixture writer's local catalog records locations under.
const FILE_URI: &str = "file://";

/// A benchmark table as written: its name, key type and shape, the bytes a
/// key and a value hold before compression, from which a contract table's
/// bucket count follows, what its values are made of, its two builds'
/// keys, and for an ordered table its order column and rows per key. A
/// key-value table has one row per key, so `rows` and `appended_rows` are
/// its rows. The appended table's second build adds rows to its first
/// build's keys, so its `appended_rows` are the keys it touches.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TableShape {
    name: TableName,
    key_type: KeyType,
    key_shape: KeyShape,
    key_bytes: usize,
    value_bytes: usize,
    values: Values,
    rows: u64,
    appended_rows: u64,
    ordered: Option<Ordered>,
}

const SHAPES: [TableShape; 12] = [
    TableShape {
        name: TableName::Long,
        key_type: KeyType::Long,
        key_shape: KeyShape::Sequential,
        key_bytes: size_of::<i64>(),
        value_bytes: VALUE_BYTES,
        values: Values::Random,
        rows: ROWS,
        appended_rows: APPENDED_ROWS,
        ordered: None,
    },
    TableShape {
        name: TableName::String,
        key_type: KeyType::String,
        key_shape: KeyShape::Sequential,
        key_bytes: STRING_KEY_FIXED_BYTES,
        value_bytes: VALUE_BYTES,
        values: Values::Random,
        rows: ROWS,
        appended_rows: APPENDED_ROWS,
        ordered: None,
    },
    TableShape {
        name: TableName::Long,
        key_type: KeyType::Long,
        key_shape: KeyShape::Sequential,
        key_bytes: size_of::<i64>(),
        value_bytes: VALUE_BYTES,
        values: Values::Text,
        rows: ROWS,
        appended_rows: APPENDED_ROWS,
        ordered: None,
    },
    TableShape {
        name: TableName::String,
        key_type: KeyType::String,
        key_shape: KeyShape::Sequential,
        key_bytes: STRING_KEY_FIXED_BYTES,
        value_bytes: VALUE_BYTES,
        values: Values::Text,
        rows: ROWS,
        appended_rows: APPENDED_ROWS,
        ordered: None,
    },
    TableShape {
        name: TableName::Long,
        key_type: KeyType::Long,
        key_shape: KeyShape::Sequential,
        key_bytes: size_of::<i64>(),
        value_bytes: VALUE_BYTES,
        values: Values::Protobuf,
        rows: ROWS,
        appended_rows: APPENDED_ROWS,
        ordered: None,
    },
    TableShape {
        name: TableName::Long,
        key_type: KeyType::Long,
        key_shape: KeyShape::Sequential,
        key_bytes: size_of::<i64>(),
        value_bytes: SERVING_VALUE_BYTES,
        values: Values::ProtobufLarge,
        rows: LARGE_VALUE_ROWS,
        appended_rows: LARGE_VALUE_APPENDED_ROWS,
        ordered: None,
    },
    TableShape {
        name: TableName::Scattered,
        key_type: KeyType::String,
        key_shape: KeyShape::Scattered,
        key_bytes: SCATTERED_KEY_FIXED_BYTES,
        value_bytes: SERVING_VALUE_BYTES,
        values: Values::Random,
        rows: SCATTERED_ROWS,
        appended_rows: SCATTERED_APPENDED_ROWS,
        ordered: None,
    },
    TableShape {
        name: TableName::Ordered,
        key_type: KeyType::Long,
        key_shape: KeyShape::Sequential,
        key_bytes: size_of::<i64>(),
        value_bytes: VALUE_BYTES,
        values: Values::Random,
        rows: ORDERED_KEYS,
        appended_rows: ORDERED_APPENDED_KEYS,
        ordered: Some(ORDERED),
    },
    TableShape {
        name: TableName::OrderedLong,
        key_type: KeyType::Long,
        key_shape: KeyShape::Sequential,
        key_bytes: size_of::<i64>(),
        value_bytes: VALUE_BYTES,
        values: Values::Random,
        rows: LONG_RUN_KEYS,
        appended_rows: LONG_RUN_APPENDED_KEYS,
        ordered: Some(LONG_RUN),
    },
    TableShape {
        name: TableName::OrderedString,
        key_type: KeyType::Long,
        key_shape: KeyShape::Sequential,
        key_bytes: size_of::<i64>(),
        value_bytes: VALUE_BYTES,
        values: Values::Random,
        rows: LONG_RUN_KEYS,
        appended_rows: LONG_RUN_APPENDED_KEYS,
        ordered: Some(LONG_STRING_RUN),
    },
    TableShape {
        name: TableName::OrderedRolled,
        key_type: KeyType::Long,
        key_shape: KeyShape::Sequential,
        key_bytes: size_of::<i64>(),
        value_bytes: VALUE_BYTES,
        values: Values::Random,
        rows: LONG_RUN_KEYS,
        appended_rows: LONG_RUN_APPENDED_KEYS,
        ordered: Some(LONG_RUN),
    },
    TableShape {
        name: TableName::OrderedAppended,
        key_type: KeyType::Long,
        key_shape: KeyShape::Sequential,
        key_bytes: size_of::<i64>(),
        value_bytes: VALUE_BYTES,
        values: Values::Random,
        rows: ORDERED_KEYS,
        appended_rows: ORDERED_KEYS,
        ordered: Some(SHORT_RUN),
    },
];

/// What separates the key column's name from the values' in the name of
/// a table whose values are not random.
const VALUES_SEPARATOR: &str = "_";

impl TableName {
    fn as_str(self) -> &'static str {
        match self {
            Self::Long => "long",
            Self::String => "string",
            Self::Scattered => "scattered",
            Self::Ordered => "ordered",
            Self::OrderedLong => "ordered_long",
            Self::OrderedString => "ordered_string",
            Self::OrderedRolled => "ordered_rolled",
            Self::OrderedAppended => "ordered_appended",
        }
    }
}

impl TableShape {
    /// The table's name in the catalog and in the results: the key
    /// column's, followed after `VALUES_SEPARATOR` by the values' unless
    /// they are random.
    fn table_name(self) -> String {
        match self.values {
            Values::Random => self.name.as_str().to_string(),
            Values::Text | Values::Protobuf | Values::ProtobufLarge => format!(
                "{}{VALUES_SEPARATOR}{}",
                self.name.as_str(),
                self.values.as_str()
            ),
        }
    }

    /// Every value column the table serves, each read whole: `value`, and
    /// `detail` beside it on a protobuf table.
    fn whole_reads(self) -> &'static [ColumnRead<'static>] {
        if self.values.is_protobuf() {
            &BOTH_READS
        } else {
            &VALUE_READS
        }
    }

    /// The value columns a configuration serves: `value` opaque, or on a
    /// protobuf table `value` declared protobuf and `detail` opaque.
    fn value_columns(self) -> Vec<ValueColumnConfig> {
        if self.values.is_protobuf() {
            vec![
                ValueColumnConfig::of(VALUE_COLUMN, ValueFormat::Protobuf),
                ValueColumnConfig::named(SECOND_VALUE_COLUMN),
            ]
        } else {
            vec![ValueColumnConfig::named(VALUE_COLUMN)]
        }
    }

    /// The bytes of a row's values before compression: `value_bytes` for
    /// each column the table serves.
    fn row_value_bytes(self) -> usize {
        self.value_bytes * self.whole_reads().len()
    }

    /// Rows per key of the first commit: one in a key-value table, and in
    /// an ordered table as many as its first key has, which every key has.
    fn rows_per_key(self) -> u64 {
        self.ordered.map_or(1, |ordered| (ordered.rows_per_key)(0))
    }

    /// The appends of every key's later rows the first build makes after
    /// its first commit: `OVERLAPPING_APPENDS` for the appended table, none
    /// for the others.
    fn overlapping_appends(self) -> u64 {
        match self.name {
            TableName::OrderedAppended => OVERLAPPING_APPENDS,
            TableName::Long
            | TableName::String
            | TableName::Scattered
            | TableName::Ordered
            | TableName::OrderedLong
            | TableName::OrderedString
            | TableName::OrderedRolled => 0,
        }
    }

    /// The rows of the first build: its keys, the rows of each in the
    /// first commit, and those of every overlapping append.
    fn first_build_rows(self) -> u64 {
        self.rows * self.rows_per_key() * (1 + self.overlapping_appends())
    }

    /// The level the table is served at: `balanced` for the rolled table,
    /// whose files share edge keys, `permissive` for the appended one,
    /// whose files overlap, and the best its tier reaches for the others
    /// (RFD 020, RFD 025), so `strict` loads every table.
    fn lookups(self) -> Option<Level> {
        match self.name {
            TableName::OrderedRolled => Some(Level::Balanced),
            TableName::OrderedAppended => Some(Level::Permissive),
            TableName::Long
            | TableName::String
            | TableName::Scattered
            | TableName::Ordered
            | TableName::OrderedLong
            | TableName::OrderedString => None,
        }
    }

    /// The bytes of a row before compression: its key, its order value in
    /// an ordered table, and its values.
    fn row_bytes(self) -> usize {
        let order_bytes = self
            .ordered
            .map_or(0, |ordered| order_value_bytes(ordered.kind));
        self.key_bytes + order_bytes + self.row_value_bytes()
    }

    /// A contract table has one file per bucket at the contract's upper
    /// file size: the raw row bytes, its key, its order value in an ordered
    /// table, and every value column, over `TARGET_FILE_BYTES_MAX`, rounded
    /// up. Random values do not shrink under zstd, so those files come
    /// close to the raw size, inside the contract's 32 to 64 MiB. A text
    /// table keeps the bucket count of the random table with its key
    /// column, so the two differ in their values alone; its files come out
    /// smaller by the values' compression. The scattered table has one
    /// file of `SCATTERED_ROWS_PER_FILE` rows per bucket. The rolled and
    /// appended tables take the buckets a contract table of their first
    /// build's rows has, which a compaction of them would write.
    fn buckets(self) -> u32 {
        let buckets = match self.name {
            TableName::Long
            | TableName::String
            | TableName::Ordered
            | TableName::OrderedLong
            | TableName::OrderedString
            | TableName::OrderedRolled
            | TableName::OrderedAppended => {
                let row_bytes = approx::count(self.row_bytes());
                (self.first_build_rows() * row_bytes).div_ceil(TARGET_FILE_BYTES_MAX)
            }
            TableName::Scattered => self.rows.div_ceil(SCATTERED_ROWS_PER_FILE),
        };
        u32::try_from(buckets).expect("a benchmark table has a few dozen buckets at most")
    }

    /// The Parquet layout: the contract's, or for the scattered table and
    /// the large protobuf table value pages bounded at the contract's
    /// `VALUE_PAGE_BYTES` (the byte bound shortens the writer's page-size
    /// check to every two rows, so a page overshoots it by one value at
    /// most), and for the scattered table files rolled at
    /// `SCATTERED_FILE_BYTES`; for the string-ordered table the column
    /// index cut to `ORDER_INDEX_TRUNCATE_BYTES`, and for the rolled table
    /// files rolled at `ROLLED_FILE_BYTES`.
    fn layout(self) -> FileLayout {
        let byte_pages = FileLayout {
            page_bytes: Some(approx::length(VALUE_PAGE_BYTES)),
            ..FileLayout::contract()
        };
        match self.name {
            TableName::Ordered | TableName::OrderedLong | TableName::OrderedAppended => {
                FileLayout::contract()
            }
            TableName::OrderedString => FileLayout {
                column_index_truncate: Some(ORDER_INDEX_TRUNCATE_BYTES),
                ..FileLayout::contract()
            },
            TableName::OrderedRolled => FileLayout {
                target_file_bytes: ROLLED_FILE_BYTES,
                ..FileLayout::contract()
            },
            TableName::Long | TableName::String => match self.values {
                Values::Random | Values::Text | Values::Protobuf => FileLayout::contract(),
                Values::ProtobufLarge => byte_pages,
            },
            TableName::Scattered => FileLayout {
                target_file_bytes: SCATTERED_FILE_BYTES,
                column_index_truncate: Some(SCATTERED_COLUMN_INDEX_TRUNCATE),
                ..byte_pages
            },
        }
    }

    /// Ids between two keys of the pass that fills the warm cache: every
    /// id when value pages are bounded by bytes, since such a page holds a
    /// few rows of its bucket, which a stride would step over, and every id
    /// of the ordered table, whose page holds the runs of a few keys of its
    /// bucket; `PRIME_STRIDE` under the contract's pages otherwise.
    fn prime_stride(self) -> u64 {
        match (self.ordered, self.layout().page_bytes) {
            (Some(_), _) | (None, Some(_)) => EVERY_ID_STRIDE,
            (None, None) => PRIME_STRIDE,
        }
    }

    /// Whether the warm scenarios' page cache holds every value page of the
    /// table, of every column it serves, which their measurement assumes.
    fn fits_warm_cache(self) -> bool {
        approx::length(self.first_build_rows()) * self.row_value_bytes() <= WARM_CACHE_BYTES
    }

    /// The table's `i`-th key.
    fn key(self, i: u64) -> Key {
        key_at_shaped(i, self.key_type, 0, self.key_shape)
    }

    /// Whether the command line selects the table: named by `--table`, or
    /// `--table` absent and the table not the scattered one, and its values
    /// among `values`.
    fn selected(self, tables: &[TableName], values: &[Values]) -> bool {
        let named = if tables.is_empty() {
            self.name != TableName::Scattered
        } else {
            tables.contains(&self.name)
        };
        named && values.contains(&self.values)
    }
}

/// The bytes of one order value of `kind` before compression: a long's for
/// the integer and time kinds and for binary, the padded digits of a
/// string, and a fixed value's width.
fn order_value_bytes(kind: OrderKind) -> usize {
    let digits = |width: u32| approx::length(width.max(LONG_DIGITS));
    match kind {
        OrderKind::Long
        | OrderKind::Int
        | OrderKind::Date
        | OrderKind::Timestamp
        | OrderKind::TimestampTz
        | OrderKind::TimestampNs
        | OrderKind::TimestampTzNs
        | OrderKind::Bin => size_of::<i64>(),
        OrderKind::Str => digits(LONG_DIGITS),
        OrderKind::PaddedStr(width) => digits(width),
        OrderKind::Fixed(width) => approx::length(width),
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
    /// The table as the manifest records it; `rows` and `appended_rows`
    /// are keys, an ordered table records its `rows_per_key` beside them,
    /// and the appended table its `overlapping_appends`.
    fn to_json(&self) -> Json {
        let mut table = json!({
            "name": self.shape.table_name(),
            "values": self.shape.values.as_str(),
            "buckets": self.shape.buckets(),
            "rows": self.shape.rows,
            "appended_rows": self.shape.appended_rows,
            "value_bytes": self.shape.value_bytes,
            "root": self.root,
            "first": self.first,
            "second": self.second,
        });
        if self.shape.ordered.is_some() {
            table["rows_per_key"] = json!(self.shape.rows_per_key());
        }
        if self.shape.overlapping_appends() > 0 {
            table["overlapping_appends"] = json!(self.shape.overlapping_appends());
        }
        table
    }
}

/// `shapes` under `dir`, in that order: each reused when the manifest
/// records it as this program writes it now, else written, alone, in place
/// of whatever its directory held. `rewrite` empties `dir` first. The
/// manifest keeps every table it records that is still current, selected
/// or not, so selecting one table never rewrites another.
async fn prepare(dir: &Path, rewrite: bool, shapes: &[TableShape]) -> BenchResult<Vec<Prepared>> {
    if rewrite && dir.exists() {
        std::fs::remove_dir_all(dir)?;
    }
    let mut recorded = read_manifest(dir)?;
    std::fs::create_dir_all(dir)?;
    let catalog = memory_catalog_local(&std::path::absolute(dir)?).await?;
    let mut prepared = Vec::with_capacity(shapes.len());
    for &shape in shapes {
        if let Some(table) = recorded.iter().find(|table| table.shape == shape) {
            eprintln!("reusing {} under {}", shape.table_name(), dir.display());
            prepared.push(table.clone());
            continue;
        }
        let table = write_table(catalog.as_ref(), dir, shape).await?;
        recorded.retain(|other| other.shape.table_name() != shape.table_name());
        recorded.push(table.clone());
        write_manifest(dir, &recorded)?;
        prepared.push(table);
    }
    Ok(prepared)
}

/// Writes `shape` under `dir` through `catalog`, after removing what its
/// directory held: the first build of `shape.rows` keys, then the second,
/// `shape.appended_rows` more above them, each key with its rows; or, for
/// a table with overlapping appends, the first build's first commit and
/// its appends, then one more append as the second build
/// (`overlapping_rows`).
async fn write_table(
    catalog: &MemoryCatalog,
    dir: &Path,
    shape: TableShape,
) -> BenchResult<Prepared> {
    let name = shape.table_name();
    let table_dir = dir.join(NAMESPACE).join(&name);
    if table_dir.exists() {
        std::fs::remove_dir_all(&table_dir)?;
    }
    let started = Instant::now();
    let spec = FixtureSpec {
        buckets: shape.buckets(),
        ids: 0..shape.rows,
        key_shape: shape.key_shape,
        value_bytes: shape.value_bytes,
        value_kind: shape.values.kind(),
        layout: shape.layout(),
        second_value: shape.values.is_protobuf(),
        ordered: shape.ordered,
        ..FixtureSpec::small(&name, shape.key_type)
    };
    let mut fixture = write_fixture(catalog, NAMESPACE, spec).await?;
    let appends = shape.overlapping_appends();
    for append in 1..=appends {
        fixture
            .append_ordered(
                catalog,
                &overlapping_rows(shape, append)?,
                &format!("{APPEND_PREFIX}{append}"),
            )
            .await?;
    }
    let first = fixture.metadata_location().to_string();
    if appends > 0 {
        fixture
            .append_ordered(
                catalog,
                &overlapping_rows(shape, appends + 1)?,
                SECOND_BUILD_PREFIX,
            )
            .await?;
    } else {
        fixture
            .append(
                catalog,
                shape.rows..shape.rows + shape.appended_rows,
                SECOND_BUILD_PREFIX,
            )
            .await?;
    }
    eprintln!(
        "wrote {name} ({} keys of {} rows, {} overlapping appends, {} buckets, then a second build of {} keys): {:.1?}",
        shape.rows,
        shape.rows_per_key(),
        appends,
        shape.buckets(),
        shape.appended_rows,
        started.elapsed()
    );
    Ok(Prepared {
        shape,
        root: fixture.root().to_string(),
        first,
        second: fixture.metadata_location().to_string(),
    })
}

/// The rows append `append` (from one) of a table with overlapping appends
/// writes, as (id, raw order value), ascending: for each of the table's
/// keys, `SHORT_RUN_ROWS` consecutive order values starting
/// `append * SHORT_RUN_ROWS` steps past the first commit's first, plus the
/// key's own offset of `id % SHORT_RUN_ROWS` steps. Each key's runs follow
/// one another without a shared pair, while each append's order range
/// overlaps the one before it by the offsets.
fn overlapping_rows(shape: TableShape, append: u64) -> BenchResult<Vec<(u64, i64)>> {
    let ordered = shape
        .ordered
        .ok_or_else(|| format!("{} has no order column", shape.table_name()))?;
    let mut rows = Vec::with_capacity(approx::length(shape.rows * SHORT_RUN_ROWS));
    for id in 0..shape.rows {
        let first = append * SHORT_RUN_ROWS + id % SHORT_RUN_ROWS;
        for step in first..first + SHORT_RUN_ROWS {
            let order = ordered
                .step
                .checked_mul(i64::try_from(step)?)
                .and_then(|offset| ordered.start.checked_add(offset))
                .ok_or_else(|| format!("append {append} of id {id} overflows its order value"))?;
            rows.push((id, order));
        }
    }
    Ok(rows)
}

/// Records `tables` as the manifest under `dir`.
fn write_manifest(dir: &Path, tables: &[Prepared]) -> BenchResult<()> {
    let manifest = json!({
        "tables": tables.iter().map(Prepared::to_json).collect::<Vec<_>>(),
    });
    std::fs::write(
        dir.join(MANIFEST_FILE),
        serde_json::to_vec_pretty(&manifest)?,
    )?;
    Ok(())
}

/// The tables the manifest under `dir` records as this program writes them
/// now, whose builds' metadata files are still there; none when it is
/// absent.
fn read_manifest(dir: &Path) -> BenchResult<Vec<Prepared>> {
    let path = dir.join(MANIFEST_FILE);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let manifest: Json = serde_json::from_slice(&std::fs::read(path)?)?;
    let Some(tables) = manifest["tables"].as_array() else {
        return Ok(Vec::new());
    };
    let mut recorded = Vec::with_capacity(tables.len());
    for table in tables {
        let Some(shape) = SHAPES
            .into_iter()
            .find(|shape| table["name"].as_str() == Some(shape.table_name().as_str()))
        else {
            continue;
        };
        let text = |field: &str| table[field].as_str().map(str::to_string);
        let (Some(root), Some(first), Some(second)) = (text("root"), text("first"), text("second"))
        else {
            continue;
        };
        let current = table["values"].as_str() == Some(shape.values.as_str())
            && table["buckets"].as_u64() == Some(u64::from(shape.buckets()))
            && table["rows"].as_u64() == Some(shape.rows)
            && table["appended_rows"].as_u64() == Some(shape.appended_rows)
            && table["value_bytes"].as_u64() == Some(approx::count(shape.value_bytes))
            && table["rows_per_key"].as_u64() == shape.ordered.map(|_| shape.rows_per_key())
            && table["overlapping_appends"].as_u64()
                == Some(shape.overlapping_appends()).filter(|&appends| appends > 0);
        if current && local_path(&first)?.exists() && local_path(&second)?.exists() {
            recorded.push(Prepared {
                shape,
                root,
                first,
                second,
            });
        }
    }
    Ok(recorded)
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

/// Bytes of one column across a table's data files, as their footers
/// record them: compressed on disk, and before compression.
#[derive(Clone, Copy, Debug, Default)]
struct ColumnBytes {
    compressed: u64,
    uncompressed: u64,
}

impl ColumnBytes {
    /// Uncompressed over compressed bytes: what zstd achieved.
    fn ratio(self) -> f64 {
        approx::ratio(self.uncompressed, self.compressed)
    }

    fn to_json(self) -> Json {
        json!({
            "compressed_bytes": self.compressed,
            "uncompressed_bytes": self.uncompressed,
            "ratio": self.ratio(),
        })
    }
}

/// The bytes of a table's columns over every data file of both builds.
#[derive(Clone, Copy, Debug, Default)]
struct TableBytes {
    key: ColumnBytes,
    value: ColumnBytes,
    /// `SECOND_VALUE_COLUMN`, zero on a table without it.
    detail: ColumnBytes,
}

/// The key and value columns' bytes over every data file of `prepared`
/// (both builds), summed from the files' footers.
fn column_bytes(prepared: &Prepared) -> BenchResult<TableBytes> {
    let mut bytes = TableBytes::default();
    let mut pending = vec![local_path(&prepared.root)?];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(dir)? {
            let path = entry?.path();
            if path.is_dir() {
                pending.push(path);
                continue;
            }
            if path
                .extension()
                .is_none_or(|extension| extension != PARQUET_EXTENSION)
            {
                continue;
            }
            let reader = SerializedFileReader::new(std::fs::File::open(&path)?)?;
            for group in reader.metadata().row_groups() {
                for column in group.columns() {
                    let sum = match column.column_descr().name() {
                        KEY_COLUMN => &mut bytes.key,
                        VALUE_COLUMN => &mut bytes.value,
                        SECOND_VALUE_COLUMN => &mut bytes.detail,
                        _ => continue,
                    };
                    sum.compressed += u64::try_from(column.compressed_size())?;
                    sum.uncompressed += u64::try_from(column.uncompressed_size())?;
                }
            }
        }
    }
    Ok(bytes)
}

/// Every table's key and value column bytes (`column_bytes`), printed and
/// as the report's JSON, `detail` among them on a protobuf table.
fn compression(prepared: &[Prepared]) -> BenchResult<Vec<Json>> {
    let mut report = Vec::with_capacity(prepared.len());
    for table in prepared {
        let TableBytes { key, value, detail } = column_bytes(table)?;
        let protobuf = table.shape.values.is_protobuf();
        let detail_ratio = if protobuf {
            format!(", detail column {:.2}x", detail.ratio())
        } else {
            String::new()
        };
        eprintln!(
            "{}: key column compresses {:.2}x, value column {:.2}x{detail_ratio} ({:.1} MiB on disk)",
            table.shape.table_name(),
            key.ratio(),
            value.ratio(),
            mib(key.compressed + value.compressed + detail.compressed)
        );
        let mut entry = json!({
            "table": table.shape.table_name(),
            "key": key.to_json(),
            "value": value.to_json(),
        });
        if protobuf {
            entry["detail"] = detail.to_json();
        }
        report.push(entry);
    }
    Ok(report)
}

/// The extension of a data file.
const PARQUET_EXTENSION: &str = "parquet";

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
/// every value page of a table of 100-byte values (about 200 MB) and,
/// under the Bloom tier, every key page besides, so nothing is evicted. A
/// table whose pages do not fit is not run warm (`fits_warm_cache`).
const WARM_CACHE_BYTES: usize = 1 << 30;

/// The page cache of the cold-hit scenario: one mebibyte, about ten of a
/// table's two thousand value pages, so nearly every uniformly random hit
/// misses the cache, reads its page from the file, and decodes it. The run
/// reports the page fetches it measured per lookup.
const COLD_CACHE_BYTES: usize = 1 << 20;

/// One table served over storage of its own, so its counters are its own,
/// beside at most one other table over the same handles, and a local
/// directory that lives as long as they do under the Disk tier.
struct Served {
    table: Arc<Table>,
    /// The table served beside `table` over the same storage, caches,
    /// local directory, and count of calls in flight, loaded and primed:
    /// idle unless a scenario drives it.
    beside: Option<Arc<Table>>,
    storage: Arc<Storage>,
    cache: PageCache,
    scan_cache: PageCache,
    /// The local directory under the Disk tier, whose chunk files the
    /// sizing reports.
    local: Option<Arc<LocalDir>>,
    /// The store every `file://` read of an ordered table goes through,
    /// counting the rounds of a Scan's GETs (`count_rounds`); `None` for a
    /// key-value table, whose reads keep the store `Storage` builds, and
    /// under `--rewrite-location`, whose reads leave `file://`.
    rounds: Option<Arc<RoundsStore>>,
    _local: Option<TempDir>,
}

/// `prepared` in `tier` from `source`, over the production storage
/// defaults (hedging on), a `cache_bytes` page cache, and a `cache_bytes`
/// scan cache, not yet loaded; beside it, over the same handles as
/// `open_with` gives a process's tables, `beside` pinned, loaded, and
/// primed as the warm scenarios are. The Disk tier gets a fresh local
/// directory with no page-cache disk level, or `reuse`'s when given, whose
/// chunk files a load then reuses. An ordered table's `file://` reads go
/// through a `RoundsStore` when no location is rewritten.
async fn serve(
    prepared: &Prepared,
    beside: Option<&Prepared>,
    tier: Tier,
    cache_bytes: usize,
    source: SourceConfig,
    reuse: Option<TempDir>,
) -> BenchResult<Served> {
    let serving = serving();
    let counts_rounds = prepared.shape.ordered.is_some() && serving.storage.rewrite.is_empty();
    let storage = Arc::new(Storage::with_sdk_defaults(serving.storage));
    let rounds = counts_rounds.then(|| Arc::new(RoundsStore::local()));
    if let Some(store) = &rounds {
        storage.register(FILE_PREFIX, Arc::clone(store) as Arc<dyn ObjectStore>);
    }
    let (local, dir) = local_for(tier, &storage, reuse)?;
    let cache = PageCache::new(cache_bytes, None).await?;
    let scan_cache = PageCache::for_scans(cache_bytes);
    let handles = TableHandles {
        storage: Arc::clone(&storage),
        cache: cache.clone(),
        local: local.clone(),
        calls: CallsInFlight::new(),
        findings: FindingLog::Events,
        batch_fan_out: serving.batch_fan_out,
        scan_cache: scan_cache.clone(),
        max_multi_scan_values: ServerConfig::default().max_multi_scan_values,
        max_scan_bytes: ServerConfig::default().max_scan_bytes,
    };
    let beside = match beside {
        Some(other) => {
            let table = table_over(other, tier, pinned(other), handles.clone()).await?;
            table.load().await?;
            prime_cache(&table, other.shape).await?;
            Some(Arc::new(table))
        }
        None => None,
    };
    let table = table_over(prepared, tier, source, handles).await?;
    Ok(Served {
        table: Arc::new(table),
        beside,
        storage,
        cache,
        scan_cache,
        local,
        rounds,
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

/// `prepared` as a `Table` in `tier` from `source`, strict at its shape's
/// level (`TableShape::lookups`), over `handles`, with the default
/// deadlines, an ordered table with its order column at the default
/// `max_scan_rows`.
async fn table_over(
    prepared: &Prepared,
    tier: Tier,
    source: SourceConfig,
    handles: TableHandles,
) -> BenchResult<Table> {
    let config = TableConfig {
        name: prepared.shape.table_name(),
        source: source.clone(),
        r#ref: MAIN_BRANCH.to_string(),
        key_column: KEY_COLUMN.to_string(),
        value_columns: prepared.shape.value_columns(),
        order_column: prepared.shape.ordered.map(|_| ORDER_COLUMN.to_string()),
        max_scan_rows: None,
        strict: true,
        lookups: prepared.shape.lookups(),
        residency: tier.residency(),
        resident_budget_bytes: None,
    };
    let server = ServerConfig::default();
    Ok(Table::new(
        config,
        TableSource::from_config(&source).await?,
        handles,
        server.load_deadline,
        server.refresh_deadline,
        server.get_deadline,
        server.scan_deadline,
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
/// half a table of `ROWS`, so a warm run touches every page and no run repeats a
/// key before it has used many thousands of others.
const KEY_POOL: usize = 1 << 20;

/// The seed of the key pools, fixed so every run asks the same keys.
const KEY_SEED: u64 = 0x1ce7_4a7b_e4c4_0001;

/// Ids between two keys of the pass that fills the warm cache: a value page
/// of 100-byte values holds a thousand rows of one bucket, which span a few thousand ids, so
/// every page is asked many times over.
const PRIME_STRIDE: u64 = 100;

/// Ids between two keys of the pass that fills the warm cache of a table
/// whose value pages are bounded by bytes, or of the ordered table: every
/// id.
const EVERY_ID_STRIDE: u64 = 1;

/// The samples a run reserves beyond what its warm-up rate predicts,
/// relative to that prediction: twice, so a run faster than its warm-up
/// still records into memory reserved before it started.
const SAMPLE_HEADROOM: usize = 2;

/// The ids of the hit pool over `shape`'s key space, in its order: uniform
/// random ids of the first build.
fn hit_ids(shape: TableShape) -> Vec<u64> {
    let mut rng = StdRng::seed_from_u64(KEY_SEED);
    (0..KEY_POOL)
        .map(|_| rng.random_range(0..shape.rows))
        .collect()
}

/// Pools of hits and misses over `shape`'s key space: the `hit_ids` as
/// keys and as the absent key just above each.
fn key_pools(shape: TableShape) -> (Arc<Vec<Key>>, Arc<Vec<Key>>) {
    let hits: Vec<Key> = hit_ids(shape).into_iter().map(|id| shape.key(id)).collect();
    let misses = hits.iter().map(absent_key_from).collect();
    (Arc::new(hits), Arc::new(misses))
}

/// The top-level fields the few-field projections keep: two scalars and
/// a short text, none of the filler that is most of a value's bytes, so a
/// projection returns a few dozen bytes whatever the value's length.
const PROJECT_FEW: [u32; 3] = [ID_FIELD, NAME_FIELD, FLAGS_FIELD];

/// The top-level fields the most-field projection keeps: every field a
/// value holds but `NAME_FIELD`, the filler included, so the walk copies
/// nearly the whole value.
const PROJECT_MOST: [u32; 6] = [
    ID_FIELD,
    CHECKSUM_FIELD,
    SCORES_FIELD,
    FLAGS_FIELD,
    GROUP_FIELD,
    FILLER_FIELD,
];

/// `VALUE_COLUMN` whole: what every scenario reads unless it says
/// otherwise.
const VALUE_READS: [ColumnRead<'static>; 1] = [ColumnRead::whole(VALUE_COLUMN)];

/// Both value columns of a protobuf table, whole.
const BOTH_READS: [ColumnRead<'static>; 2] = [
    ColumnRead::whole(VALUE_COLUMN),
    ColumnRead::whole(SECOND_VALUE_COLUMN),
];

/// `VALUE_COLUMN` projected to `PROJECT_FEW`.
const FEW_READS: [ColumnRead<'static>; 1] = [ColumnRead {
    name: VALUE_COLUMN,
    fields: &PROJECT_FEW,
}];

/// `VALUE_COLUMN` projected to `PROJECT_MOST`.
const MOST_READS: [ColumnRead<'static>; 1] = [ColumnRead {
    name: VALUE_COLUMN,
    fields: &PROJECT_MOST,
}];

/// No column: a lookup that answers presence alone.
const NO_READS: [ColumnRead<'static>; 0] = [];

/// What an operation reads of each row it finds. The reads are static, so
/// naming them allocates nothing inside a measured operation.
#[derive(Clone, Debug)]
enum Reads {
    /// Columns read whole, or none: the bytes read are those returned.
    Whole(&'static [ColumnRead<'static>]),
    /// Columns of which `VALUE_COLUMN` is projected, and per key of the
    /// operation's pool its whole value's bytes, which the walk reads
    /// whatever it returns.
    Projected {
        columns: &'static [ColumnRead<'static>],
        whole: Arc<Vec<usize>>,
    },
}

/// `VALUE_COLUMN` read whole.
const WHOLE_VALUE: Reads = Reads::Whole(&VALUE_READS);

impl Reads {
    fn columns(&self) -> &'static [ColumnRead<'static>] {
        match self {
            Self::Whole(columns) | Self::Projected { columns, .. } => columns,
        }
    }

    /// The value bytes read for the pool keys `at`, which returned
    /// `returned`.
    fn read_bytes(&self, at: std::ops::Range<usize>, returned: usize) -> usize {
        match self {
            Self::Whole(_) => returned,
            Self::Projected { whole, .. } => whole[at].iter().sum(),
        }
    }
}

/// The value bytes a run's operations read and returned, summed over every
/// column they named. A projection reads the whole value and returns what
/// it keeps; a whole read returns what it reads.
#[derive(Clone, Copy, Debug, Default)]
struct ValueBytes {
    read: u64,
    returned: u64,
}

impl ValueBytes {
    fn of(read: usize, returned: usize) -> Self {
        Self {
            read: approx::count(read),
            returned: approx::count(returned),
        }
    }

    fn plus(self, other: Self) -> Self {
        Self {
            read: self.read + other.read,
            returned: self.returned + other.returned,
        }
    }
}

/// The bytes `row` returned over the `columns` values it carries, or `None`
/// when its key was absent or it does not carry a value for each column:
/// a null, which no benchmark table holds, or a wrong count.
fn returned_bytes(row: &Row, columns: usize) -> Option<usize> {
    match row {
        Row::Present(values) if values.len() == columns => values
            .iter()
            .try_fold(0, |sum, value| Some(sum + value.as_ref()?.len())),
        Row::Present(_) | Row::Absent => None,
    }
}

/// Keys of a `MultiScan` scenario's batch.
const MULTI_SCAN_KEYS: usize = 10;

/// Rows of the bounded Scan's page: a page of a read paged by order value.
const SCAN_PAGE_ROWS: u32 = 10;

/// A key's run over the rows the bounded Scan's window leaves out at each
/// end: a quarter, so the window is the run's middle half, which holds
/// more rows than `SCAN_PAGE_ROWS`.
const WINDOW_MARGIN_DIVISOR: usize = 4;

/// Operations of a Scan scenario whose answers are compared whole with
/// the model's before it is measured (`verify_scans`).
const VERIFIED_OPS: usize = 100;

/// Operations of a Scan scenario whose rounds of GETs are counted, one at
/// a time, before it is measured (`count_rounds`), after the
/// `VERIFIED_OPS` that `verify_scans` ran, so a cold scenario counts
/// operations whose pages those did not just read.
const ROUNDS_OPS: usize = 100;

/// Rows of the head Scan of a long run: the newest hundred, a tenth of
/// the default `max_scan_rows`.
const HEAD_ROWS: u32 = 100;

/// Bounds a long-run Scan scenario draws its `before` from, one per
/// request in turn (`bounded_in_run`): enough that the bounds fall all
/// over the middle half of a run, and the operations of a run cycle
/// through them.
const BOUND_POOL: usize = 1_024;

/// The seed of the long-run Scans' bounds, fixed so every run asks the
/// same requests.
const BOUND_SEED: u64 = 0x1ce7_4a7b_e4c4_0002;

/// A Scan scenario's request and the model's answer to it as order values
/// (`scan_model`), which is every key's answer: every key of an ordered
/// table the Scan scenarios read holds the same order values.
struct ScanCase {
    /// The request in raw order values, as the model takes it.
    test: TestScan,
    /// The request as the table takes it.
    bounds: ScanBounds,
    /// The order value of each row the model returns, in order.
    orders: Vec<OrderValue>,
    /// Whether the model leaves live rows inside the bounds past the last.
    more: bool,
}

impl ScanCase {
    /// The case of `test` over `shape`, which must be an ordered table.
    fn of(shape: TableShape, test: TestScan) -> BenchResult<Self> {
        let ordered = shape
            .ordered
            .ok_or_else(|| format!("{} has no order column", shape.table_name()))?;
        let rows = ordered.orders_of(0)?.into_iter().map(|order| (order, ()));
        let (rows, more) = scan_model(rows, &test);
        Ok(Self {
            bounds: ScanBounds {
                direction: test.direction,
                limit: test.limit,
                after: test.after.map(|raw| order_value(ordered.kind, raw)),
                before: test.before.map(|raw| order_value(ordered.kind, raw)),
            },
            orders: rows
                .into_iter()
                .map(|(order, ())| order_value(ordered.kind, order))
                .collect(),
            more,
            test,
        })
    }

    /// The cases of `tests` over `shape`, which operation `i` takes in
    /// turn (`Op::case`).
    fn pool(shape: TableShape, tests: &[TestScan]) -> BenchResult<Arc<Vec<Self>>> {
        Ok(Arc::new(
            tests
                .iter()
                .map(|&test| Self::of(shape, test))
                .collect::<BenchResult<_>>()?,
        ))
    }

    /// The value bytes `scanned` returned, or what in it disagrees with the
    /// model: another `more`, another row count or order value, or a row
    /// without one value of `value_bytes` bytes. Allocates nothing when it
    /// agrees, so the check costs a measured operation no allocation.
    fn check(&self, scanned: &Scanned, value_bytes: usize) -> Result<usize, String> {
        if scanned.more != self.more || scanned.rows.len() != self.orders.len() {
            return Err(format!(
                "{} rows, more {}, where the model has {} rows, more {}",
                scanned.rows.len(),
                scanned.more,
                self.orders.len(),
                self.more
            ));
        }
        let mut returned = 0;
        for (row, order) in scanned.rows.iter().zip(&self.orders) {
            if row.order != *order {
                return Err(format!(
                    "a row at {} where the model has {order}",
                    row.order
                ));
            }
            match row.values.as_slice() {
                [Some(value)] if value.len() == value_bytes => returned += value.len(),
                values => return Err(format!("the row at {order} carries {values:?}")),
            }
        }
        Ok(returned)
    }
}

/// What a Scan of `shape`'s key `id` under `test` answers by the model:
/// `scan_model` over the rows the fixture writer wrote for the key, each
/// its order value and its `VALUE_COLUMN` value (`row_value` at
/// `FIRST_VERSION`).
fn modeled_scan(
    shape: TableShape,
    ordered: Ordered,
    id: u64,
    test: &TestScan,
) -> BenchResult<Scanned> {
    let key = shape.key(id);
    let rows = ordered.orders_of(id)?.into_iter().map(|order| {
        let value = row_value(
            &key,
            order,
            shape.value_bytes,
            shape.values.kind(),
            FIRST_VERSION,
        );
        (order, value)
    });
    let (rows, more) = scan_model(rows, test);
    Ok(Scanned {
        rows: rows
            .into_iter()
            .map(|(order, value)| ScanRow {
                order: order_value(ordered.kind, order),
                values: vec![Some(value.into())],
            })
            .collect(),
        more,
    })
}

/// What one operation of a run does.
#[derive(Clone)]
enum Op {
    /// One `get` of the next pool key reading `reads`, which must answer
    /// present with a value per column, or absent when `present` is false.
    Get {
        keys: Arc<Vec<Key>>,
        present: bool,
        reads: Reads,
    },
    /// One `multi_get` of the next `batch` pool keys reading `reads`, all
    /// present with a value per column.
    MultiGet {
        keys: Arc<Vec<Key>>,
        batch: usize,
        reads: Reads,
    },
    /// One `scan` of the next pool key reading `VALUE_COLUMN` under the
    /// bounds of the operation's case (`Op::case`), which must return the
    /// model's rows, each with a value of `value_bytes` bytes
    /// (`ScanCase::check`).
    Scan {
        keys: Arc<Vec<Key>>,
        cases: Arc<Vec<ScanCase>>,
        value_bytes: usize,
    },
    /// One `multi_scan` of the next `batch` pool keys under the
    /// operation's case, each answered as `Scan` requires.
    MultiScan {
        keys: Arc<Vec<Key>>,
        batch: usize,
        cases: Arc<Vec<ScanCase>>,
        value_bytes: usize,
    },
}

impl Op {
    /// The positions in the key pool that operation `i` asks: the next
    /// key, or the next batch, which wraps before it would run past the
    /// pool's end.
    fn pool_range(&self, i: usize) -> std::ops::Range<usize> {
        match self {
            Self::Get { keys, .. } | Self::Scan { keys, .. } => {
                let at = i % keys.len();
                at..at + 1
            }
            Self::MultiGet { keys, batch, .. } | Self::MultiScan { keys, batch, .. } => {
                let start = (i * batch) % (keys.len() - batch);
                start..start + batch
            }
        }
    }

    /// The case operation `i` of a Scan op asks, its pool's cases in turn.
    fn case(cases: &[ScanCase], i: usize) -> &ScanCase {
        &cases[i % cases.len()]
    }

    /// Lookups per operation: one, or a batch's keys.
    fn keys_per_op(&self) -> usize {
        match self {
            Self::Get { .. } | Self::Scan { .. } => 1,
            Self::MultiGet { batch, .. } | Self::MultiScan { batch, .. } => *batch,
        }
    }

    /// Runs operation `i` and returns the value bytes it read and
    /// returned; an answer of the wrong presence, or a Scan's answer that
    /// disagrees with the model, is an error, so a run that measured
    /// nothing but failures cannot pass for fast.
    async fn run(&self, table: &Table, i: usize) -> BenchResult<ValueBytes> {
        let at = self.pool_range(i);
        match self {
            Self::Get {
                keys,
                present,
                reads,
            } => {
                let lookup = table.get(&keys[at.start], reads.columns()).await?;
                let returned = returned_bytes(&lookup.row, reads.columns().len());
                if returned.is_some() != *present {
                    return Err(format!("key {i} answered {:?}", lookup.row).into());
                }
                let returned = returned.unwrap_or(0);
                Ok(ValueBytes::of(reads.read_bytes(at, returned), returned))
            }
            Self::MultiGet { keys, reads, .. } => {
                let lookup = table.multi_get(&keys[at.clone()], reads.columns()).await?;
                let mut returned = 0;
                for row in &lookup.rows {
                    returned += returned_bytes(row, reads.columns().len())
                        .ok_or_else(|| format!("batch {i} answered a key absent"))?;
                }
                Ok(ValueBytes::of(reads.read_bytes(at, returned), returned))
            }
            Self::Scan {
                keys,
                cases,
                value_bytes,
            } => {
                let case = Self::case(cases, i);
                let lookup = table
                    .scan(&keys[at.start], &VALUE_READS, &case.bounds)
                    .await?;
                let returned = case
                    .check(&lookup.scanned, *value_bytes)
                    .map_err(|fault| format!("scan {i}: {fault}"))?;
                Ok(ValueBytes::of(returned, returned))
            }
            Self::MultiScan {
                keys,
                cases,
                value_bytes,
                ..
            } => {
                let case = Self::case(cases, i);
                let lookup = table
                    .multi_scan(&keys[at.clone()], &VALUE_READS, &case.bounds)
                    .await?;
                if lookup.results.len() != at.len() {
                    return Err(format!(
                        "batch {i} answered {} of {} keys",
                        lookup.results.len(),
                        at.len()
                    )
                    .into());
                }
                let mut returned = 0;
                for scanned in &lookup.results {
                    returned += case
                        .check(scanned, *value_bytes)
                        .map_err(|fault| format!("batch {i}: {fault}"))?;
                }
                Ok(ValueBytes::of(returned, returned))
            }
        }
    }
}

/// The first `VERIFIED_OPS` operations of a Scan or `MultiScan` op over
/// `shape`, each key's answer compared whole, every value's bytes
/// included, with the model's (`modeled_scan`), before the scenario is
/// measured: `Op::run` checks the order values and value lengths alone, so
/// this is what fails a run whose Scans return wrong bytes. A `Get` or
/// `MultiGet` op is checked by `Op::run` alone.
async fn verify_scans(op: &Op, table: &Table, shape: TableShape) -> BenchResult<()> {
    let (keys, cases) = match op {
        Op::Scan { keys, cases, .. } | Op::MultiScan { keys, cases, .. } => (keys, cases),
        Op::Get { .. } | Op::MultiGet { .. } => return Ok(()),
    };
    let ordered = shape
        .ordered
        .ok_or_else(|| format!("{} has no order column", shape.table_name()))?;
    let ids = hit_ids(shape);
    for i in 0..VERIFIED_OPS {
        let at = op.pool_range(i);
        let case = Op::case(cases, i);
        let answers = table
            .multi_scan(&keys[at.clone()], &VALUE_READS, &case.bounds)
            .await?
            .results;
        let single = table
            .scan(&keys[at.start], &VALUE_READS, &case.bounds)
            .await?
            .scanned;
        if single != answers[0] {
            return Err(
                format!("operation {i}: a Scan and a MultiScan of one key disagree").into(),
            );
        }
        for (answer, &id) in answers.iter().zip(&ids[at]) {
            if *answer != modeled_scan(shape, ordered, id, &case.test)? {
                return Err(format!("operation {i}: key {id} disagrees with the model").into());
            }
        }
    }
    Ok(())
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

/// What a run's task loops did: the operations, their samples when
/// recorded, and the value bytes they read and returned.
type Done = (usize, Vec<Duration>, ValueBytes);

/// One task's share of a run: operations `task`, `task + tasks`, ... until
/// `until`, each timed into `samples` when it is given.
async fn task_loop(
    table: Arc<Table>,
    op: Op,
    (task, tasks): (usize, usize),
    pace: Pace,
    until: Instant,
    mut samples: Option<Vec<Duration>>,
) -> BenchResult<Done> {
    let mut done = 0;
    let mut bytes = ValueBytes::default();
    while Instant::now() < until {
        let started = Instant::now();
        let op_bytes = op.run(&table, done * tasks + task).await?;
        if let Some(samples) = samples.as_mut() {
            samples.push(started.elapsed());
        }
        bytes = bytes.plus(op_bytes);
        done += 1;
        if let Pace::Yielding = pace {
            tokio::task::yield_now().await;
        }
    }
    Ok((done, samples.unwrap_or_default(), bytes))
}

/// The task loops of a run, as they are spawned.
type Loops = Vec<tokio::task::JoinHandle<BenchResult<Done>>>;

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
/// recording; the operations done, the samples, and the value bytes,
/// merged.
async fn spawn_run(
    served: &Served,
    op: &Op,
    tasks: usize,
    length: Duration,
    reserve: Option<usize>,
) -> BenchResult<Done> {
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

/// Waits for every loop of `handles`; the operations they did, their
/// samples, and their value bytes, merged.
async fn join_loops(handles: Loops) -> BenchResult<Done> {
    let mut done = 0;
    let mut samples = Vec::new();
    let mut bytes = ValueBytes::default();
    for handle in handles {
        let (ops, mut task_samples, task_bytes) = handle.await??;
        done += ops;
        samples.append(&mut task_samples);
        bytes = bytes.plus(task_bytes);
    }
    Ok((done, samples, bytes))
}

/// Counters of a served table and the process, read before and after a
/// measured window; page fetches and hits are the page cache's and the
/// scan cache's together, since an operation reads one or the other.
#[derive(Clone, Copy, Debug, Default)]
struct Counters {
    allocations: Allocations,
    gets: u64,
    /// The bytes those GETs requested.
    get_bytes: u64,
    local_reads: u64,
    /// The bytes those local reads read.
    local_bytes: u64,
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
            get_bytes: stats.bytes(),
            local_reads: stats.local_reads(),
            local_bytes: stats.local_bytes(),
            hedges: stats.hedges(),
            fetches: served.cache.fetches() + served.scan_cache.fetches(),
            hits: served.cache.hits() + served.scan_cache.hits(),
        }
    }

    fn since(self, earlier: Self) -> Self {
        Self {
            allocations: self.allocations.since(earlier.allocations),
            gets: self.gets - earlier.gets,
            get_bytes: self.get_bytes - earlier.get_bytes,
            local_reads: self.local_reads - earlier.local_reads,
            local_bytes: self.local_bytes - earlier.local_bytes,
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
    table: TableShape,
    tier: Tier,
    mode: Mode,
    tasks: usize,
    /// The bound its tables were served under, `Serving::batch_fan_out`.
    batch_fan_out: usize,
    /// The table its process also served, idle, under `kv-beside-ordered`.
    beside: Option<TableShape>,
}

impl RunLabel {
    /// The measured table's name, followed after `BESIDE_SEPARATOR` by the
    /// name of the table served beside it.
    fn table_label(&self) -> String {
        match self.beside {
            Some(beside) => format!(
                "{}{BESIDE_SEPARATOR}{}",
                self.table.table_name(),
                beside.table_name()
            ),
            None => self.table.table_name(),
        }
    }
}

/// What separates a measured table's name from the one served beside it.
const BESIDE_SEPARATOR: &str = "+";

/// A measured run.
struct Measured {
    label: RunLabel,
    ops: usize,
    /// Lookups per operation: one, or a `MultiGet`'s batch.
    keys_per_op: usize,
    elapsed: Duration,
    /// Every operation's latency.
    latencies: Latencies,
    counters: Counters,
    /// The value bytes the operations read and returned.
    bytes: ValueBytes,
    /// The rounds of GETs an operation waits on, counted apart from the
    /// window (`count_rounds`); `None` where they are not counted.
    rounds: Option<Rounds>,
}

/// One second, the unit of the throughput figure.
const SECOND: Duration = Duration::from_secs(1);

impl Measured {
    fn ops_per_second(&self) -> f64 {
        approx::gauge(approx::count(self.ops)) * SECOND.div_duration_f64(self.elapsed)
    }

    fn per_op(&self, total: u64) -> f64 {
        approx::ratio(total, approx::count(self.ops))
    }

    fn to_json(&self) -> Json {
        let counters = self.counters;
        let latencies = &self.latencies;
        json!({
            "scenario": self.label.scenario.name(),
            "table": self.label.table.name.as_str(),
            "values": self.label.table.values.as_str(),
            "tier": format!("{:?}", self.label.tier).to_lowercase(),
            "mode": self.label.mode.name(),
            "tasks": self.label.tasks,
            "batch_fan_out": self.label.batch_fan_out,
            "beside": self.label.beside.map(TableShape::table_name),
            "keys_per_op": self.keys_per_op,
            "ops": self.ops,
            "elapsed_us": micros(self.elapsed),
            "ops_per_second": self.ops_per_second(),
            "p50_us": micros(latencies.percentile(P50)),
            "p90_us": micros(latencies.percentile(P90)),
            "p99_us": micros(latencies.percentile(P99)),
            "max_us": micros(latencies.max()),
            "allocations_per_op": self.per_op(counters.allocations.calls),
            "allocated_bytes_per_op": self.per_op(counters.allocations.bytes),
            "page_fetches_per_op": self.per_op(counters.fetches),
            "page_cache_hits_per_op": self.per_op(counters.hits),
            "gets_per_op": self.per_op(counters.gets),
            "get_bytes_per_op": self.per_op(counters.get_bytes),
            "local_reads_per_op": self.per_op(counters.local_reads),
            "local_bytes_per_op": self.per_op(counters.local_bytes),
            "hedges": counters.hedges,
            "value_bytes_read_per_op": self.per_op(self.bytes.read),
            "value_bytes_returned_per_op": self.per_op(self.bytes.returned),
            "rounds_per_op": self.rounds.map(Rounds::per_op),
            "rounds_max": self.rounds.map(|rounds| rounds.most),
            "rounds_ops": self.rounds.map(|rounds| rounds.ops),
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
    let (warm_ops, _, _) = spawn_run(served, op, label.tasks, warmup, None).await?;
    let windows = approx::length(windows);
    let reserve = (warm_ops / label.tasks + 1) * windows * SAMPLE_HEADROOM;
    let before = Counters::read(served);
    let started = Instant::now();
    let (ops, samples, bytes) = spawn_run(served, op, label.tasks, duration, Some(reserve)).await?;
    let elapsed = started.elapsed();
    let counters = Counters::read(served).since(before);
    Ok(Measured {
        label,
        ops,
        keys_per_op: op.keys_per_op(),
        elapsed,
        latencies: Latencies::of(samples),
        counters,
        bytes,
        rounds: None,
    })
}

/// Asks every page of the table, of every column it serves, at least once,
/// `prime_stride` ids apart, so the warm cache holds every value page (and
/// under Bloom every key page) before a warm run starts: a Get of each key
/// of a key-value table, and of an ordered table Scans of each key's whole
/// run, paged by order value as a caller pages a run longer than
/// `max_scan_rows`, which fill the scan cache.
async fn prime_cache(table: &Table, shape: TableShape) -> BenchResult<()> {
    for id in (0..shape.rows).step_by(approx::length(shape.prime_stride())) {
        let key = shape.key(id);
        match shape.ordered {
            Some(_) => {
                let mut page = ScanBounds {
                    direction: Direction::NewestFirst,
                    limit: 0,
                    after: None,
                    before: None,
                };
                loop {
                    let scanned = table.scan(&key, shape.whole_reads(), &page).await?.scanned;
                    match scanned.rows.last() {
                        Some(oldest) if scanned.more => page.before = Some(oldest.order.clone()),
                        Some(_) | None => break,
                    }
                }
            }
            None => {
                table.get(&key, shape.whole_reads()).await?;
            }
        }
    }
    Ok(())
}

/// Per key of the hit pool (`hit_ids`), the bytes of its whole
/// `VALUE_COLUMN` value as the first build wrote it (`value_of_kind`), which
/// a whole read of it returns: what a projection of that key reads. Each
/// row's value is built once, outside any measured window.
fn whole_lengths(shape: TableShape) -> Arc<Vec<usize>> {
    let by_id: Vec<usize> = (0..shape.rows)
        .map(|id| value_of_kind(&shape.key(id), shape.value_bytes, shape.values.kind()).len())
        .collect();
    Arc::new(
        hit_ids(shape)
            .into_iter()
            .map(|id| by_id[approx::length(id)])
            .collect(),
    )
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
/// selected mode, over a warm table and, for the cold scenarios, a cold
/// one; each served beside `beside` when given (`serve`). A Scan scenario
/// is checked against the model whole (`verify_scans`) before it is
/// measured.
async fn lookups(
    prepared: &Prepared,
    beside: Option<&Prepared>,
    tier: Tier,
    scenarios: &[Scenario],
    settings: &RunSettings,
) -> BenchResult<Vec<Measured>> {
    let shape = prepared.shape;
    let (hits, misses) = key_pools(shape);
    let warm = if scenarios.iter().any(|s| !s.is_cold()) {
        let served = serve_loaded(prepared, beside, tier, WARM_CACHE_BYTES).await?;
        prime_cache(&served.table, shape).await?;
        Some(served)
    } else {
        None
    };
    let cold = if scenarios.iter().any(|s| s.is_cold()) {
        Some(serve_loaded(prepared, beside, tier, COLD_CACHE_BYTES).await?)
    } else {
        None
    };
    let whole = scenarios
        .iter()
        .any(|s| s.projects())
        .then(|| whole_lengths(shape));
    let projected = |columns| Reads::Projected {
        columns,
        whole: Arc::clone(
            whole
                .as_ref()
                .expect("whole lengths are read when a projecting scenario is selected"),
        ),
    };
    let scan_pool =
        |tests: &[TestScan]| ScanCase::pool(shape, tests).map(|cases| scan(&hits, cases, shape));
    let scan_one = |test| scan_pool(&[test]);
    let scan_batch = |test| {
        ScanCase::pool(shape, &[test]).map(|cases| multi_scan(&hits, MULTI_SCAN_KEYS, cases, shape))
    };
    let mut results = Vec::new();
    for &scenario in scenarios {
        let (served, op) = match scenario {
            Scenario::WarmHit => (&warm, get(&hits, true, WHOLE_VALUE)),
            Scenario::ColdHit => (&cold, get(&hits, true, WHOLE_VALUE)),
            Scenario::Miss => (&warm, get(&misses, false, WHOLE_VALUE)),
            Scenario::MultiGet100 => (&warm, multi(&hits, BATCH_100, WHOLE_VALUE)),
            Scenario::MultiGet1000 => (&warm, multi(&hits, BATCH_1000, WHOLE_VALUE)),
            Scenario::WarmHitBoth => (&warm, get(&hits, true, Reads::Whole(&BOTH_READS))),
            Scenario::PresenceHit => (&warm, get(&hits, true, Reads::Whole(&NO_READS))),
            Scenario::WarmHitProjectFew => (&warm, get(&hits, true, projected(&FEW_READS))),
            Scenario::WarmHitProjectMost => (&warm, get(&hits, true, projected(&MOST_READS))),
            Scenario::MultiGet100Both => {
                (&warm, multi(&hits, BATCH_100, Reads::Whole(&BOTH_READS)))
            }
            Scenario::MultiGet100Project => (&warm, multi(&hits, BATCH_100, projected(&FEW_READS))),
            Scenario::ColdHitBoth => (&cold, get(&hits, true, Reads::Whole(&BOTH_READS))),
            Scenario::ColdHitProjectFew => (&cold, get(&hits, true, projected(&FEW_READS))),
            Scenario::MultiGet100Cold => (&cold, multi(&hits, BATCH_100, WHOLE_VALUE)),
            Scenario::MultiGet100ColdBoth => {
                (&cold, multi(&hits, BATCH_100, Reads::Whole(&BOTH_READS)))
            }
            Scenario::ScanWarm => (&warm, scan_one(TestScan::default())?),
            Scenario::ScanCold => (&cold, scan_one(TestScan::default())?),
            Scenario::ScanBounded => (&warm, scan_one(bounded_page()?)?),
            Scenario::MultiScan10 => (&warm, scan_batch(multi_scan_whole_runs())?),
            Scenario::MultiScan10Cold => (&cold, scan_batch(multi_scan_whole_runs())?),
            Scenario::ScanBoundedLong | Scenario::ScanBoundedString => {
                (&warm, scan_pool(&bounded_in_run(shape)?)?)
            }
            Scenario::ScanBoundedLongCold | Scenario::ScanBoundedStringCold => {
                (&cold, scan_pool(&bounded_in_run(shape)?)?)
            }
            Scenario::ScanHeadLong => (&warm, scan_one(head_of_run())?),
            Scenario::Load | Scenario::Refresh | Scenario::TwoTable | Scenario::KvBesideOrdered => {
                continue;
            }
        };
        let served = served
            .as_ref()
            .expect("a table is served for every selected scenario");
        verify_scans(&op, &served.table, shape).await?;
        let rounds = count_rounds(served, &op).await?;
        let label = RunLabel {
            scenario,
            table: shape,
            tier,
            mode: Mode::Single,
            tasks: 1,
            batch_fan_out: serving().batch_fan_out,
            beside: beside.map(|other| other.shape),
        };
        results.extend(measure_modes(served, &op, (label, rounds), settings).await?);
    }
    Ok(results)
}

/// The rounds of GETs a Scan op's operations wait on, from
/// `served.rounds`: `ROUNDS_OPS` operations after the `VERIFIED_OPS` that
/// `verify_scans` ran, one at a time, each counted on its own. `None` for
/// a `Get` or `MultiGet` op, for a table served without the store that
/// counts them, and for a `MultiScan`: its keys' reads run concurrently
/// over one count, so a GET one key issues after another key's GET
/// answered lands in a later round although it waits on nothing, and the
/// count is no key's dependency depth.
async fn count_rounds(served: &Served, op: &Op) -> BenchResult<Option<Rounds>> {
    let (Some(store), Op::Scan { .. }) = (&served.rounds, op) else {
        return Ok(None);
    };
    let mut rounds = Rounds::default();
    for i in VERIFIED_OPS..VERIFIED_OPS + ROUNDS_OPS {
        store.restart();
        op.run(&served.table, i).await?;
        rounds.total += store.rounds();
        rounds.most = rounds.most.max(store.rounds());
    }
    rounds.ops = approx::count(ROUNDS_OPS);
    Ok(Some(rounds))
}

/// The rounds of GETs counted over a run's operations (`count_rounds`).
#[derive(Clone, Copy, Debug, Default)]
struct Rounds {
    ops: u64,
    total: u64,
    most: u64,
}

impl Rounds {
    fn per_op(self) -> f64 {
        approx::ratio(self.total, self.ops)
    }
}

/// `prepared` served beside `beside` in `tier` from its first build over
/// `cache_bytes` caches (`serve`), and loaded.
async fn serve_loaded(
    prepared: &Prepared,
    beside: Option<&Prepared>,
    tier: Tier,
    cache_bytes: usize,
) -> BenchResult<Served> {
    let served = serve(prepared, beside, tier, cache_bytes, pinned(prepared), None).await?;
    served.table.load().await?;
    Ok(served)
}

/// `op` over `served` measured in every selected mode, each row labeled
/// `label` with that mode and its tasks; the single-task row carries
/// `rounds`, counted one operation at a time as that row runs them.
async fn measure_modes(
    served: &Served,
    op: &Op,
    (label, rounds): (RunLabel, Option<Rounds>),
    settings: &RunSettings,
) -> BenchResult<Vec<Measured>> {
    let mut runs = Vec::with_capacity(settings.modes.len());
    for &mode in &settings.modes {
        let tasks = match mode {
            Mode::Single => 1,
            Mode::Multi => settings.tasks,
        };
        let label = RunLabel {
            mode,
            tasks,
            ..label
        };
        let mut run = measure(
            served,
            op,
            label,
            settings.warmup,
            settings.duration,
            settings.windows,
        )
        .await?;
        run.rounds = match mode {
            Mode::Single => rounds,
            Mode::Multi => None,
        };
        eprintln!("{}", row(&run));
        runs.push(run);
    }
    Ok(runs)
}

/// The bounded Scan's request: `SCAN_PAGE_ROWS` rows, newest first,
/// strictly between the order values of the rows a quarter and three
/// quarters into a key's run (`WINDOW_MARGIN_DIVISOR`), counted oldest
/// first.
fn bounded_page() -> BenchResult<TestScan> {
    let orders = ORDERED.orders_of(0)?;
    let margin = orders.len() / WINDOW_MARGIN_DIVISOR;
    Ok(TestScan {
        direction: Direction::NewestFirst,
        limit: SCAN_PAGE_ROWS,
        after: Some(orders[margin]),
        before: Some(orders[orders.len() - margin]),
        ..TestScan::default()
    })
}

/// The long-run Scans' requests (`scan-bounded-long`,
/// `scan-bounded-string`): `SCAN_PAGE_ROWS` rows, newest first, before an
/// order value drawn uniformly from the middle half of a key's run of
/// `shape` (`WINDOW_MARGIN_DIVISOR`), `BOUND_POOL` of them, so a bound
/// lands anywhere in a run's pages.
fn bounded_in_run(shape: TableShape) -> BenchResult<Vec<TestScan>> {
    let ordered = shape
        .ordered
        .ok_or_else(|| format!("{} has no order column", shape.table_name()))?;
    let orders = ordered.orders_of(0)?;
    let margin = orders.len() / WINDOW_MARGIN_DIVISOR;
    let mut rng = StdRng::seed_from_u64(BOUND_SEED);
    Ok((0..BOUND_POOL)
        .map(|_| TestScan {
            direction: Direction::NewestFirst,
            limit: SCAN_PAGE_ROWS,
            before: Some(orders[rng.random_range(margin..orders.len() - margin)]),
            ..TestScan::default()
        })
        .collect())
}

/// The head Scan of a long run (`scan-head-long`): its `HEAD_ROWS` newest
/// rows, no bound.
fn head_of_run() -> TestScan {
    TestScan {
        direction: Direction::NewestFirst,
        limit: HEAD_ROWS,
        ..TestScan::default()
    }
}

/// The `MultiScan` scenarios' request: every row of each key, newest
/// first, at the largest limit `server.max_multi_scan_values` admits for
/// `MULTI_SCAN_KEYS` keys reading one column, each row counting its order
/// value beside the column's.
fn multi_scan_whole_runs() -> TestScan {
    let values_per_row = VALUE_READS.len() + 1;
    let limit = ServerConfig::default().max_multi_scan_values / (MULTI_SCAN_KEYS * values_per_row);
    TestScan {
        limit: u32::try_from(limit).expect("the default limit fits u32"),
        ..TestScan::default()
    }
}

/// The two `MultiGet` sizes measured.
const BATCH_100: usize = 100;
const BATCH_1000: usize = 1_000;

fn get(keys: &Arc<Vec<Key>>, present: bool, reads: Reads) -> Op {
    Op::Get {
        keys: Arc::clone(keys),
        present,
        reads,
    }
}

fn multi(keys: &Arc<Vec<Key>>, batch: usize, reads: Reads) -> Op {
    Op::MultiGet {
        keys: Arc::clone(keys),
        batch,
        reads,
    }
}

fn scan(keys: &Arc<Vec<Key>>, cases: Arc<Vec<ScanCase>>, shape: TableShape) -> Op {
    Op::Scan {
        keys: Arc::clone(keys),
        cases,
        value_bytes: shape.value_bytes,
    }
}

fn multi_scan(
    keys: &Arc<Vec<Key>>,
    batch: usize,
    cases: Arc<Vec<ScanCase>>,
    shape: TableShape,
) -> Op {
    Op::MultiScan {
        keys: Arc::clone(keys),
        batch,
        cases,
        value_bytes: shape.value_bytes,
    }
}

/// The table of random values named `name` among `prepared`.
fn random_table(prepared: &[Prepared], name: TableName) -> BenchResult<&Prepared> {
    prepared
        .iter()
        .find(|table| table.shape.name == name && table.shape.values == Values::Random)
        .ok_or_else(|| format!("no {} table of random values", name.as_str()).into())
}

/// The `kv-beside-ordered` scenario in `tier`: the key-value scenarios of
/// `Scenario::BESIDE_ORDERED` on the long table of random values, served
/// beside the ordered table over one storage, page cache, scan cache, under
/// Disk one local directory, and one count of calls in flight, as
/// `open_with` opens a process serving both kinds. The ordered table is
/// loaded, primed, and idle, so a row against the same scenario on the long
/// table alone is what serving an ordered table beside it costs the
/// key-value path.
async fn kv_beside_ordered(
    prepared: &[Prepared],
    tier: Tier,
    settings: &RunSettings,
) -> BenchResult<Vec<Measured>> {
    let long = random_table(prepared, TableName::Long)?;
    let ordered = random_table(prepared, TableName::Ordered)?;
    lookups(
        long,
        Some(ordered),
        tier,
        &Scenario::BESIDE_ORDERED,
        settings,
    )
    .await
}

/// The two-table scenario in `tier`: `settings.tasks` loops of warm hits on
/// the long table beside one loop of `MultiGet` 1,000 warm hits on the
/// string table, both served over one storage, one `WARM_CACHE_BYTES` page
/// cache, under Disk one local directory, and one count of calls in flight,
/// as `open_with` opens a process's tables, each loaded and primed, run
/// together for the warm-up and then for the measured window. Every loop yields between operations (`Pace::Yielding`): the hit
/// loops would otherwise hold every worker, and the batch and its runs
/// would wait for the window to end. One row per table. Neither row carries counters: both tables'
/// operations run in one window over one storage and one cache, so no
/// count belongs to either row alone; each carries the value bytes its own
/// loops read and returned.
async fn two_tables(
    prepared: &[Prepared],
    tier: Tier,
    settings: &RunSettings,
) -> BenchResult<Vec<Measured>> {
    let long = random_table(prepared, TableName::Long)?;
    let string = random_table(prepared, TableName::String)?;
    let served = serve(
        long,
        Some(string),
        tier,
        WARM_CACHE_BYTES,
        pinned(long),
        None,
    )
    .await?;
    served.table.load().await?;
    prime_cache(&served.table, long.shape).await?;
    let long_table = &served.table;
    let string_table = served
        .beside
        .as_ref()
        .expect("the string table is served beside the long one");
    let hit_op = get(&key_pools(long.shape).0, true, WHOLE_VALUE);
    let batch_op = multi(&key_pools(string.shape).0, BATCH_1000, WHOLE_VALUE);
    let hits = (&hit_op, Pace::Yielding);
    let batches = (&batch_op, Pace::Yielding);
    let tasks = settings.tasks;
    let one = 1;

    let until = Instant::now() + settings.warmup;
    let hit_loops = spawn_loops(long_table, hits, tasks, until, None);
    let batch_loops = spawn_loops(string_table, batches, one, until, None);
    let (warm_hits, _, _) = join_loops(hit_loops).await?;
    let (warm_batches, _, _) = join_loops(batch_loops).await?;

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
    let (hit_ops, hit_samples, hit_bytes) = join_loops(hit_loops).await?;
    let (batch_ops, batch_samples, batch_bytes) = join_loops(batch_loops).await?;
    let elapsed = started.elapsed();

    let batch_fan_out = serving().batch_fan_out;
    let label = |table, mode, tasks| RunLabel {
        scenario: Scenario::TwoTable,
        table,
        tier,
        mode,
        tasks,
        batch_fan_out,
        beside: None,
    };
    let rows = vec![
        Measured {
            label: label(string.shape, Mode::Single, one),
            ops: batch_ops,
            keys_per_op: BATCH_1000,
            elapsed,
            latencies: Latencies::of(batch_samples),
            counters: Counters::default(),
            bytes: batch_bytes,
            rounds: None,
        },
        Measured {
            label: label(long.shape, Mode::Multi, tasks),
            ops: hit_ops,
            keys_per_op: 1,
            elapsed,
            latencies: Latencies::of(hit_samples),
            counters: Counters::default(),
            bytes: hit_bytes,
            rounds: None,
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

/// One measured load or refresh, and the size of the generation it left.
struct Loaded {
    what: &'static str,
    table: TableShape,
    tier: Tier,
    round: u32,
    wall: Duration,
    cpu: Duration,
    allocations: Allocations,
    rss_before: u64,
    rss_peak: u64,
    max_rss: u64,
    gets: u64,
    /// The bytes those GETs requested.
    get_bytes: u64,
    local_reads: u64,
    /// The bytes those local reads read.
    local_bytes: u64,
    files: usize,
    index_bytes: usize,
    /// The resident memory of the generation's file indexes by part, summed
    /// over its files.
    parts: ResidentParts,
    /// Under Disk, the bytes of the chunk files in the local directory.
    chunk_file_bytes: Option<u64>,
}

impl Loaded {
    fn to_json(&self) -> Json {
        json!({
            "what": self.what,
            "table": self.table.name.as_str(),
            "values": self.table.values.as_str(),
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
            "get_bytes": self.get_bytes,
            "local_reads": self.local_reads,
            "local_bytes": self.local_bytes,
            "files": self.files,
            "index_bytes": self.index_bytes,
            "resident_parts": self.parts,
            "resident_parts_bytes": self.parts.total(),
            "chunk_file_bytes": self.chunk_file_bytes,
        })
    }
}

/// Times `work` against `served`: wall and CPU time, allocations, the
/// resident set before and at its sampled peak, and the storage reads;
/// then, outside the timing, the generation's resident parts and under
/// Disk its chunk files' bytes.
async fn timed<F: Future<Output = BenchResult<()>>>(
    served: &Served,
    what: &'static str,
    (table, tier, round): (TableShape, Tier, u32),
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
    let parts = generation
        .files
        .iter()
        .fold(ResidentParts::default(), |sum, file| {
            sum.plus(file.index.resident_parts())
        });
    let chunk_file_bytes = served
        .local
        .as_ref()
        .map(|local| local.index_bytes_in_use())
        .transpose()?;
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
        get_bytes: counters.get_bytes,
        local_reads: counters.local_reads,
        local_bytes: counters.local_bytes,
        files: generation.files.len(),
        index_bytes: generation.index_bytes,
        parts,
        chunk_file_bytes,
    })
}

/// `rounds` first loads of `prepared` in `tier`, each over fresh storage,
/// cache, and local directory; under Disk, each followed by a load over the
/// same directory, which reuses the chunk files the first wrote.
async fn loads(prepared: &Prepared, tier: Tier, rounds: u32) -> BenchResult<Vec<Loaded>> {
    let mut results = Vec::new();
    for round in 0..rounds {
        let key = (prepared.shape, tier, round);
        let served = serve(
            prepared,
            None,
            tier,
            WARM_CACHE_BYTES,
            pinned(prepared),
            None,
        )
        .await?;
        let run = timed(&served, "load", key, async {
            Ok(served.table.load().await?)
        })
        .await?;
        eprintln!("{}", load_row(&run));
        results.push(run);
        if tier == Tier::Disk {
            let Served { _local: dir, .. } = served;
            let again = serve(
                prepared,
                None,
                tier,
                WARM_CACHE_BYTES,
                pinned(prepared),
                dir,
            )
            .await?;
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
        let key = (prepared.shape, tier, round);
        std::fs::write(&hint, &prepared.first)?;
        let served = serve(prepared, None, tier, WARM_CACHE_BYTES, source.clone(), None).await?;
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
        "{:<24} {:<19} {:<6} {:<7} {:>5} {:>11} {:>10} {:>10} {:>10} {:>10} {:>9} {:>10} {:>8} {:>8} {:>10} {:>10} {:>11} {:>6}",
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
        "reads/op",
        "read B/op",
        "ret B/op",
        "store B/op",
        "rounds"
    )
}

fn row(run: &Measured) -> String {
    let counters = run.counters;
    format!(
        "{:<24} {:<19} {:<6} {:<7} {:>5} {:>11.0} {:>10.2?} {:>10.2?} {:>10.2?} {:>10.2?} {:>9.1} {:>10.0} {:>8.3} {:>8.3} {:>10.0} {:>10.0} {:>11.0} {:>6}",
        run.label.scenario.name(),
        run.label.table_label(),
        format!("{:?}", run.label.tier).to_lowercase(),
        run.label.mode.name(),
        run.label.tasks,
        run.ops_per_second(),
        run.latencies.percentile(P50),
        run.latencies.percentile(P90),
        run.latencies.percentile(P99),
        run.latencies.max(),
        run.per_op(counters.allocations.calls),
        run.per_op(counters.allocations.bytes),
        run.per_op(counters.fetches),
        run.per_op(counters.gets + counters.local_reads),
        run.per_op(run.bytes.read),
        run.per_op(run.bytes.returned),
        run.per_op(counters.get_bytes + counters.local_bytes),
        run.rounds.map_or_else(
            || "-".to_string(),
            |rounds| format!("{:.2}", rounds.per_op())
        ),
    )
}

fn load_header() -> String {
    format!(
        "{:<26} {:<19} {:<6} {:>5} {:>10} {:>10} {:>11} {:>10} {:>10} {:>9} {:>9} {:>6} {:>10} {:>10}",
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
        "read MiB",
        "files",
        "index MiB",
        "chunk MiB"
    )
}

fn load_row(run: &Loaded) -> String {
    format!(
        "{:<26} {:<19} {:<6} {:>5} {:>10.1?} {:>10.1?} {:>11} {:>10.1} {:>10.1} {:>9} {:>9.1} {:>6} {:>10.1} {:>10}",
        run.what,
        run.table.table_name(),
        format!("{:?}", run.tier).to_lowercase(),
        run.round,
        run.wall,
        run.cpu,
        run.allocations.calls,
        mib(run.allocations.bytes),
        mib(run.rss_peak.saturating_sub(run.rss_before)),
        run.gets + run.local_reads,
        mib(run.get_bytes + run.local_bytes),
        run.files,
        mib(approx::count(run.index_bytes)),
        run.chunk_file_bytes
            .map_or_else(|| "-".to_string(), |bytes| format!("{:.1}", mib(bytes))),
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

/// One line per table on stdout: its name, key type, rows, and the first
/// build's metadata file, tab-separated.
fn print_prepared(prepared: &[Prepared]) {
    for table in prepared {
        println!(
            "{}\t{:?}\t{}\t{}",
            table.shape.table_name(),
            table.shape.key_type,
            table.shape.rows,
            table.first
        );
    }
}

/// The tables the command line measures, and beside them every table to
/// prepare: those, under the two-table scenario the long and string tables
/// of random values it serves, and under `kv-beside-ordered` the long table
/// of random values and the ordered table.
fn select_shapes(cli: &Cli, scenarios: &[Scenario]) -> (Vec<TableShape>, Vec<TableShape>) {
    let values = or_all(&cli.values, &Values::ALL);
    let measured: Vec<TableShape> = SHAPES
        .into_iter()
        .filter(|shape| shape.selected(&cli.table, &values))
        .collect();
    let two_table = scenarios.contains(&Scenario::TwoTable) && !cli.prepare;
    let beside_ordered = scenarios.contains(&Scenario::KvBesideOrdered) && !cli.prepare;
    let needed = SHAPES
        .into_iter()
        .filter(|shape| {
            let random = shape.values == Values::Random;
            measured.contains(shape)
                || (two_table
                    && random
                    && matches!(shape.name, TableName::Long | TableName::String))
                || (beside_ordered
                    && random
                    && matches!(shape.name, TableName::Long | TableName::Ordered))
        })
        .collect();
    (measured, needed)
}

/// The lookup scenarios of `scenarios` that measure what they say on
/// `shape`: all of them, without the warm ones when the warm cache cannot
/// hold the table's value pages, without those reading a second column or
/// a projection when the table is not a protobuf table, and without the
/// Scan scenarios of another table (`Scenario::scan_table`) or the
/// key-value scenarios on an ordered table, each of which it says.
fn lookups_for(shape: TableShape, scenarios: &[Scenario]) -> Vec<Scenario> {
    let ordered = shape.ordered.is_some();
    let kind_kept: Vec<Scenario> = scenarios
        .iter()
        .copied()
        .filter(|scenario| {
            scenario
                .scan_table()
                .map_or(!ordered, |table| table == shape.name)
        })
        .collect();
    if kind_kept.len() < scenarios.len() {
        eprintln!(
            "{}: scenarios of another table or table kind skipped",
            shape.table_name()
        );
    }
    let scenarios = kind_kept.as_slice();
    let fits = shape.fits_warm_cache();
    let protobuf = shape.values.is_protobuf();
    let warm_kept: Vec<Scenario> = scenarios
        .iter()
        .copied()
        .filter(|scenario| fits || !scenario.is_warm())
        .collect();
    if warm_kept.len() < scenarios.len() {
        eprintln!(
            "{}: warm scenarios skipped, its value pages outrun the warm cache",
            shape.table_name()
        );
    }
    let kept: Vec<Scenario> = warm_kept
        .iter()
        .copied()
        .filter(|scenario| protobuf || !scenario.needs_protobuf())
        .collect();
    if kept.len() < warm_kept.len() {
        eprintln!(
            "{}: column and projection scenarios skipped, it is not a protobuf table",
            shape.table_name()
        );
    }
    kept
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

/// The machine, then a row per measured lookup run and per load or
/// refresh, on stdout.
fn print_results(machine: &Json, measured: &[Measured], loaded: &[Loaded]) {
    println!("{machine}");
    println!("{}", header());
    for run in measured {
        println!("{}", row(run));
    }
    if !loaded.is_empty() {
        println!("{}", load_header());
        for run in loaded {
            println!("{}", load_row(run));
        }
    }
}

/// The settings a report was measured under: the run's command line, the
/// constants that shape the tables and scenarios, and the tables as
/// written with their columns' compression.
fn report_settings(cli: &Cli, prepared: &[Prepared], compression: &[Json]) -> Json {
    json!({
        "key_pool": KEY_POOL,
        "key_seed": KEY_SEED,
        "warm_cache_bytes": WARM_CACHE_BYTES,
        "project_few_fields": PROJECT_FEW,
        "project_most_fields": PROJECT_MOST,
        "cold_cache_bytes": COLD_CACHE_BYTES,
        "warmup_seconds": cli.warmup,
        "duration_seconds": cli.duration,
        "batch_fan_out": cli.batch_fan_out,
        "ordered_rows_per_key": ORDERED_ROWS_PER_KEY,
        "long_run_rows": LONG_RUN_ROWS,
        "string_order_prefix_bytes": STRING_ORDER_PREFIX_BYTES,
        "string_order_width": STRING_ORDER_WIDTH,
        "order_index_truncate_bytes": ORDER_INDEX_TRUNCATE_BYTES,
        "rolled_file_bytes": ROLLED_FILE_BYTES,
        "short_run_rows": SHORT_RUN_ROWS,
        "overlapping_appends": OVERLAPPING_APPENDS,
        "head_rows": HEAD_ROWS,
        "bound_pool": BOUND_POOL,
        "bound_seed": BOUND_SEED,
        "verified_ops": VERIFIED_OPS,
        "rounds_ops": ROUNDS_OPS,
        "scan_page_rows": SCAN_PAGE_ROWS,
        "multi_scan_keys": MULTI_SCAN_KEYS,
        "multi_scan_limit": multi_scan_whole_runs().limit,
        "tables": prepared.iter().map(Prepared::to_json).collect::<Vec<_>>(),
        "compression": compression,
    })
}

#[tokio::main]
async fn main() -> BenchResult<()> {
    let cli = Cli::parse();
    set_serving(&cli);
    if cfg!(debug_assertions) {
        eprintln!("warning: an unoptimized build; pass --release or --profile profiling");
    }
    let scenarios = or_all(&cli.scenario, &Scenario::ALL);
    let (measured_shapes, needed) = select_shapes(&cli, &scenarios);
    let prepared = prepare(&cli.data_dir, cli.rewrite, &needed).await?;
    let compression = compression(&prepared)?;
    if cli.prepare {
        print_prepared(&prepared);
        return Ok(());
    }
    if !cli.no_metrics {
        install_recorder()?;
    }
    let mut primed = 0;
    for table in &prepared {
        primed += prime_os_cache(&local_path(&table.root)?)?;
    }
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
    let lookup_scenarios: Vec<Scenario> = scenarios
        .iter()
        .copied()
        .filter(|s| Scenario::LOOKUPS.contains(s))
        .collect();
    let tiers = or_all(&cli.tier, &Tier::ALL);
    let tables: Vec<&Prepared> = prepared
        .iter()
        .filter(|table| measured_shapes.contains(&table.shape))
        .collect();
    let machine = machine(tasks, !cli.no_metrics);
    eprintln!("{machine}");
    eprintln!("{}", header());
    let mut measured = Vec::new();
    let mut loaded = Vec::new();
    for table in &tables {
        let table_lookups = lookups_for(table.shape, &lookup_scenarios);
        for &tier in &tiers {
            if !table_lookups.is_empty() {
                measured.extend(lookups(table, None, tier, &table_lookups, &settings).await?);
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
    if scenarios.contains(&Scenario::KvBesideOrdered) {
        for &tier in &tiers {
            measured.extend(kv_beside_ordered(&prepared, tier, &settings).await?);
        }
    }
    print_results(&machine, &measured, &loaded);
    let report = json!({
        "machine": machine,
        "settings": report_settings(&cli, &prepared, &compression),
        "lookups": measured.iter().map(Measured::to_json).collect::<Vec<_>>(),
        "loads": loaded.iter().map(Loaded::to_json).collect::<Vec<_>>(),
    });
    let path = write_report(&cli.out_dir, &report)?;
    println!("results: {}", path.display());
    Ok(())
}
