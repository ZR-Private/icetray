//! The shared value-page cache (RFD 004): pages keyed by file and page
//! offset, never invalidated because a data file never changes, evicted by
//! size alone, with concurrent misses on one page coalesced into one fetch.
//! With a local directory (RFD 017) the cache has a disk level: a page
//! evicted from memory is written there, served back with a local read and
//! no GET, and recovered when the next process opens the same directory.

use std::future::Future;
use std::io::{self, Read, Write};
use std::mem::size_of;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use foyer::{
    BlockEngineConfig, Cache, CacheBuilder, Code, DeviceBuilder as _, ErrorKind, FsDeviceBuilder,
    HybridCache, HybridCacheBuilder, HybridCachePolicy, RecoverMode, Source,
};

use crate::approx;
use crate::contract::{PAGE_CACHE_DISK_BLOCK_BYTES, VALUE_PAGE_BYTES};
use crate::error::{Error, Result};
use crate::local::DiskLevel;
use crate::metric;
use crate::storage::IoStats;

/// The name foyer files its own log lines under. Its metrics stay on its
/// no-op registry; the crate exports the page cache's counters itself.
const CACHE_NAME: &str = "icetray_pages";

/// Bytes charged against the memory budget per entry on top of the page
/// bytes: foyer's record (hash, weight, reference count, list links), the
/// key's `Arc<str>` pointer and offset, and the `Bytes` handle come to about
/// this on a 64-bit target. The file id's own bytes are shared by every
/// page of the file through the `Arc` and are not charged per entry.
const ENTRY_OVERHEAD_BYTES: usize = 64;

/// Shards of the memory level. foyer splits the memory budget evenly across
/// its shards and evicts within each, so a page is held only beside the
/// pages of its own shard, in that shard's share. Eight is foyer's default,
/// enough to spread concurrent lookups over separate locks; it is pinned
/// here so that `min_memory_bytes` is computed from the count the cache
/// uses.
pub(crate) const MEMORY_SHARDS: usize = 8;

/// The smallest memory budget the page cache is given: one contract value
/// page (`contract::VALUE_PAGE_BYTES`) weighed with its entry overhead, for
/// each of `MEMORY_SHARDS`. Below it a shard's share cannot hold one such
/// page, and foyer keeps the shard's latest page alone whatever its share,
/// so the cache would hold a page per shard however small the budget and
/// the configured bytes would bound nothing. `Config::parse` refuses less
/// as `ConfigViolation::PageCacheTooSmall`.
pub(crate) fn min_memory_bytes() -> u64 {
    approx::count(MEMORY_SHARDS) * (VALUE_PAGE_BYTES + approx::count(ENTRY_OVERHEAD_BYTES))
}

/// The width of the length prefix on an encoded file id: a `u32`, since a
/// file id is a location and never approaches four gibibytes.
const FILE_ID_LEN_BYTES: usize = size_of::<u32>();

/// The width of an encoded page offset: a `u64`, little-endian.
const OFFSET_BYTES: usize = size_of::<u64>();

/// Why the page cache could not produce a page.
#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    /// The fetch the cache ran for the caller failed. Every waiter coalesced
    /// onto that fetch receives the same failure, so it is shared.
    #[error("fetch failed: {0}")]
    Fetch(Arc<Error>),
    /// The fetch the cache ran for the caller did not finish within the
    /// fetch deadline of the handle that ran it, so the fetch was dropped,
    /// and an S3 GET it had open was cancelled with it. Every waiter
    /// coalesced onto that fetch receives this, and nothing is cached for
    /// the page.
    #[error("fetch of {path} at offset {offset} did not finish within {after:?}")]
    FetchDeadline {
        /// The data file's location.
        path: String,
        /// The page's byte offset in the file.
        offset: u64,
        /// The deadline that passed.
        after: Duration,
    },
    /// The cache itself failed: the disk level could not be opened or read,
    /// or foyer refused an operation. foyer's error is kept whole, since it
    /// is `std::error::Error + Send + Sync` and carries its kind for a
    /// caller that matches on it.
    #[error("backend: {0}")]
    Backend(#[from] foyer::Error),
}

/// A page's identity: the file it belongs to and the byte offset of its
/// header, which is where the offset index places it, so the offset alone
/// distinguishes pages within a file. A file never changes, so a key never
/// goes stale. Encoded for the disk level as the file id behind a `u32`
/// length and the offset as eight little-endian bytes.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct PageKey(Arc<str>, u64);

impl Code for PageKey {
    fn encode(&self, writer: &mut impl Write) -> foyer::Result<()> {
        let len = u32::try_from(self.0.len()).map_err(|error| {
            foyer::Error::new(
                ErrorKind::OutOfRange,
                format!(
                    "file id of {} bytes is longer than its length prefix carries",
                    self.0.len()
                ),
            )
            .with_source(error)
        })?;
        writer
            .write_all(&len.to_le_bytes())
            .map_err(foyer::Error::io_error)?;
        writer
            .write_all(self.0.as_bytes())
            .map_err(foyer::Error::io_error)?;
        writer
            .write_all(&self.1.to_le_bytes())
            .map_err(foyer::Error::io_error)
    }

    fn decode(reader: &mut impl Read) -> foyer::Result<Self> {
        let mut len = [0; FILE_ID_LEN_BYTES];
        reader
            .read_exact(&mut len)
            .map_err(foyer::Error::io_error)?;
        let len = u64::from(u32::from_le_bytes(len));
        // Sized by the bytes present, never by the prefix: a prefix read
        // off a damaged block must not size an allocation.
        let mut id = Vec::new();
        reader
            .by_ref()
            .take(len)
            .read_to_end(&mut id)
            .map_err(foyer::Error::io_error)?;
        if approx::count(id.len()) != len {
            return Err(foyer::Error::io_error(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("file id of {len} bytes ends after {} bytes", id.len()),
            )));
        }
        let id = String::from_utf8(id).map_err(|error| {
            foyer::Error::new(ErrorKind::Parse, "file id is not utf-8").with_source(error)
        })?;
        let mut offset = [0; OFFSET_BYTES];
        reader
            .read_exact(&mut offset)
            .map_err(foyer::Error::io_error)?;
        Ok(Self(Arc::from(id), u64::from_le_bytes(offset)))
    }

    fn estimated_size(&self) -> usize {
        FILE_ID_LEN_BYTES + self.0.len() + OFFSET_BYTES
    }
}

/// A page's bytes as the object store returned them, compressed as the file
/// holds them. Encoded for the disk level as the raw bytes and nothing
/// else: foyer hands the decoder exactly the bytes it wrote.
#[derive(Clone, Debug)]
struct PageValue(Bytes);

impl Code for PageValue {
    fn encode(&self, writer: &mut impl Write) -> foyer::Result<()> {
        writer.write_all(&self.0).map_err(foyer::Error::io_error)
    }

    fn decode(reader: &mut impl Read) -> foyer::Result<Self> {
        let mut bytes = Vec::new();
        reader
            .read_to_end(&mut bytes)
            .map_err(foyer::Error::io_error)?;
        Ok(Self(Bytes::from(bytes)))
    }

    fn estimated_size(&self) -> usize {
        self.0.len()
    }
}

/// Why a fetch the cache ran produced no page. It travels through foyer as
/// the source of foyer's own error, so that every coalesced waiter recovers
/// it whole, the one `Arc<Error>` of a failure included, and it is never
/// flattened to text.
#[derive(Clone, Debug, thiserror::Error)]
enum FetchFailed {
    /// The caller's fetch future failed.
    #[error(transparent)]
    Failed(Arc<Error>),
    /// The fetch ran past the deadline of the handle that ran it and was
    /// dropped.
    #[error("fetch of {} at offset {} did not finish within {after:?}", .page.0, .page.1)]
    Deadline { page: PageKey, after: Duration },
}

/// The cache's levels: memory alone, or memory over a disk level. Both are
/// one type to the rest of the crate; which one runs is decided once, by
/// whether a local directory is configured (RFD 017).
#[derive(Debug)]
enum Level {
    /// Memory only: an evicted page is gone, and the next request for it
    /// fetches it again.
    Memory(Cache<PageKey, PageValue>),
    /// Memory over a disk level in the local directory. A page evicted from
    /// memory is written there, served back with a local read charged to
    /// `stats`, and recovered when the level is reopened.
    Hybrid {
        cache: HybridCache<PageKey, PageValue>,
        stats: Arc<IoStats>,
    },
}

/// Where one call's page came from, as the `result` label of a lookup
/// distinguishes it: fetched from outside, or served by the cache. A caller
/// that joined another caller's fetch counts as `Fetched`, since it waited
/// on the GET; a page from the disk level is `Cached`, since it cost no GET.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PageOrigin {
    /// The page came from the caller's fetch, run or joined.
    Fetched,
    /// The page was in memory or on the disk level.
    Cached,
}

/// The origin of a page from where foyer says the entry came.
fn origin_of(source: Source) -> PageOrigin {
    match source {
        Source::Outer => PageOrigin::Fetched,
        Source::Memory | Source::Disk => PageOrigin::Cached,
    }
}

/// The shared value-page cache. Clones share one cache and one set of
/// counters: every table of a process holds a clone of the same cache. A
/// clone may carry a fetch deadline of its own (`with_fetch_deadline`),
/// which bounds the fetches that clone runs.
#[derive(Clone, Debug)]
pub struct PageCache {
    level: Arc<Level>,
    hits: Arc<AtomicU64>,
    fetches: Arc<AtomicU64>,
    /// How long a fetch this handle runs may take; `None` is no bound.
    fetch_deadline: Option<Duration>,
}

impl PageCache {
    /// Opens a cache holding at most `capacity_bytes` of pages in memory,
    /// each weighed as its bytes plus a fixed per-entry overhead, split
    /// evenly over `MEMORY_SHARDS`; `Config::parse` admits no budget below
    /// `min_memory_bytes`. `disk` is
    /// the local level of RFD 017: a page evicted from memory is written
    /// there, served back with a local read and no GET, and recovered when
    /// a new cache opens over the same directory, so it survives a restart.
    /// Opening the level opens its files and reads back what they hold,
    /// which is why this is async and can fail; a failure is
    /// `CacheError::Backend`.
    pub async fn new(capacity_bytes: usize, disk: Option<DiskLevel>) -> Result<Self> {
        let level = match disk {
            None => Level::Memory(
                CacheBuilder::new(capacity_bytes)
                    .with_name(CACHE_NAME)
                    .with_shards(MEMORY_SHARDS)
                    .with_weighter(weigh)
                    .build(),
            ),
            Some(disk) => Level::Hybrid {
                cache: open_hybrid(capacity_bytes, &disk).await?,
                stats: Arc::clone(disk.stats()),
            },
        };
        Ok(Self {
            level: Arc::new(level),
            hits: Arc::default(),
            fetches: Arc::default(),
            fetch_deadline: None,
        })
    }

    /// This handle on the same cache and counters, its fetches ending at
    /// `deadline` from their start: past it the fetch future is dropped,
    /// cancelling an S3 GET it has open, and every caller waiting on it receives
    /// `CacheError::FetchDeadline`. A `Table` holds its cache this way,
    /// bounded by `server.get_deadline`.
    pub(crate) fn with_fetch_deadline(self, deadline: Duration) -> Self {
        Self {
            fetch_deadline: Some(deadline),
            ..self
        }
    }

    /// The page at `key` and where it came from: from memory, from the disk
    /// level, or from `fetch`. Concurrent callers for one key share one
    /// fetch and one disk read. `fetch` may be called for a caller that then
    /// joins another caller's fetch; the future it returns runs only when
    /// the cache needs the page from outside, and `fetches` counts those
    /// runs. A disk level that fails to read the page falls through to the
    /// fetch (RFD 017: the directory is disposable). A failure of the fetch
    /// future reaches every waiting caller as `CacheError::Fetch` and leaves
    /// nothing cached; a failure of the cache itself is `CacheError::Backend`.
    /// foyer runs the fetch on a task of its own, so a caller that stops
    /// waiting does not end it; on a handle with a fetch deadline the fetch
    /// ends at that deadline from its start, its future dropped, and every
    /// waiting caller receives `CacheError::FetchDeadline` with nothing
    /// cached. The disk level's read is not bounded by it.
    /// The origin is what the `result` label of a lookup is decided by, per
    /// call rather than by differencing the process-wide counters, which
    /// concurrent lookups would confuse.
    pub async fn get_or_fetch<F, Fut>(
        &self,
        key: (Arc<str>, u64),
        fetch: F,
    ) -> Result<(Bytes, PageOrigin)>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Bytes>> + Send + 'static,
    {
        let key = PageKey(key.0, key.1);
        let run = Run {
            page: key.clone(),
            fetches: Arc::clone(&self.fetches),
            deadline: self.fetch_deadline,
        };
        // One call to foyer and not a `get` first: foyer's `get` reports a
        // disk level that fails to read as an error while its `get_or_fetch`
        // moves on to the fetch, which is what a disposable directory calls
        // for (RFD 017). The levels' entries differ in type, so each arm
        // reduces its own to the source and the page.
        let (served, stats) = match self.level.as_ref() {
            Level::Memory(cache) => (
                cache
                    .get_or_fetch(&key, || run.fetch(fetch()))
                    .await
                    .map(|entry| (entry.source(), entry.value().0.clone())),
                None,
            ),
            Level::Hybrid { cache, stats } => (
                cache
                    .get_or_fetch(&key, || run.fetch(fetch()))
                    .await
                    .map(|entry| (entry.source(), entry.value().0.clone())),
                Some(stats.as_ref()),
            ),
        };
        let (source, page) = served.map_err(cache_error)?;
        self.record(source, stats, &page);
        Ok((page, origin_of(source)))
    }

    /// Lookups served without running a fetch: from memory, or from the
    /// disk level with a local read. Counted per lookup served, so several
    /// lookups coalesced onto one disk read each count one; a lookup that
    /// joined another's fetch counts under neither `hits` nor `fetches`.
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Fetch futures run, after coalescing: concurrent misses on one key run
    /// one and count one.
    pub fn fetches(&self) -> u64 {
        self.fetches.load(Ordering::Relaxed)
    }

    /// Bytes charged against the memory budget: each page in memory at its
    /// length plus the per-entry overhead. The disk level is not counted.
    pub fn usage(&self) -> usize {
        match self.level.as_ref() {
            Level::Memory(cache) => cache.usage(),
            Level::Hybrid { cache, .. } => cache.memory().usage(),
        }
    }

    /// Flushes the disk level and closes it, so what memory still held
    /// reaches disk before the process exits; `serve` calls it while
    /// draining (RFD 012). A memory-only cache has nothing to flush and
    /// returns `Ok`. A failure of the level is `CacheError::Backend`.
    pub async fn close(&self) -> Result<()> {
        match self.level.as_ref() {
            Level::Memory(_) => Ok(()),
            Level::Hybrid { cache, .. } => cache.close().await.map_err(backend),
        }
    }

    /// Counts how a page was served. From memory: a hit. From the disk
    /// level, which only `Level::Hybrid` has (`stats` is its counter;
    /// `Level::Memory` is never told `Disk`): a hit and one local read of the
    /// page's length, charged beside GETs so a lookup's whole cost is one
    /// structure (RFD 003). Both are counted per lookup served: foyer
    /// reports the source to every lookup coalesced onto one disk read
    /// alike and names no leader, so several lookups served by one read
    /// each count a hit and a local read, as each would under
    /// `DiskChunks::page`; GETs, by contrast, are counted per fetch run.
    /// From a fetch: nothing here, since `Run::fetch` counted the one run for
    /// every lookup it served.
    fn record(&self, source: Source, stats: Option<&IoStats>, page: &Bytes) {
        match source {
            Source::Outer => self.record_insert(),
            Source::Memory => self.record_hit(),
            Source::Disk => {
                self.record_hit();
                if let Some(stats) = stats {
                    stats.record_local_read(approx::count(page.len()));
                }
            }
        }
    }

    /// One page served without a fetch.
    fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
        metrics::counter!(metric::PAGE_CACHE_HITS_TOTAL).increment(1);
    }

    /// One page fetched and inserted: the one event that raises the memory
    /// budget's usage, so the gauge is set here and not on every lookup.
    fn record_insert(&self) {
        metrics::gauge!(metric::PAGE_CACHE_BYTES).set(approx::gauge_len(self.usage()));
    }
}

/// The hybrid cache over `disk`: memory of `capacity_bytes`, a block engine
/// on a file device in `disk.dir()` bounded by `disk.capacity_bytes()`,
/// written on eviction (RFD 017: a page leaves memory for disk, never for
/// nowhere), and recovered from whatever the directory holds before the
/// first request is served. Recovery is quiet: a block that does not
/// decode is skipped, since everything here is rebuilt from object storage.
/// The block is `PAGE_CACHE_DISK_BLOCK_BYTES`, and `disk.capacity_bytes()`
/// holds at least one (`Config::parse` checks it), so the engine has a
/// block to write into.
async fn open_hybrid(
    capacity_bytes: usize,
    disk: &DiskLevel,
) -> Result<HybridCache<PageKey, PageValue>> {
    let device = FsDeviceBuilder::new(disk.dir())
        .with_capacity(approx::length(disk.capacity_bytes()))
        .build()
        .map_err(backend)?;
    HybridCacheBuilder::new()
        .with_name(CACHE_NAME)
        .with_policy(HybridCachePolicy::WriteOnEviction)
        .memory(capacity_bytes)
        .with_shards(MEMORY_SHARDS)
        .with_weighter(weigh)
        .storage()
        .with_engine_config(
            BlockEngineConfig::new(device)
                .with_block_size(approx::length(PAGE_CACHE_DISK_BLOCK_BYTES)),
        )
        .with_recover_mode(RecoverMode::Quiet)
        .build()
        .await
        .map_err(backend)
}

/// What an entry costs the memory budget.
fn weigh(_: &PageKey, value: &PageValue) -> usize {
    value.0.len() + ENTRY_OVERHEAD_BYTES
}

/// What one caller's fetch runs with, should foyer run it: the page it is
/// for, the counter it counts in, and the deadline of the handle it came
/// through.
struct Run {
    page: PageKey,
    fetches: Arc<AtomicU64>,
    deadline: Option<Duration>,
}

impl Run {
    /// Runs a caller's fetch once foyer has decided the page must come from
    /// outside, counting the run as it starts so that a fetch that fails
    /// still counts. With a deadline, `fut` is dropped when it passes, and
    /// the run fails as `FetchFailed::Deadline`.
    async fn fetch<Fut>(self, fut: Fut) -> std::result::Result<PageValue, FetchFailed>
    where
        Fut: Future<Output = Result<Bytes>>,
    {
        self.fetches.fetch_add(1, Ordering::Relaxed);
        metrics::counter!(metric::PAGE_CACHE_FETCHES_TOTAL).increment(1);
        let fetched = match self.deadline {
            None => fut.await,
            Some(after) => tokio::time::timeout(after, fut).await.map_err(|_elapsed| {
                FetchFailed::Deadline {
                    page: self.page,
                    after,
                }
            })?,
        };
        fetched
            .map(PageValue)
            .map_err(|error| FetchFailed::Failed(Arc::new(error)))
    }
}

/// A foyer failure as the crate's error: the caller's own fetch failure or
/// its deadline when that is what foyer carries, otherwise the backend's.
fn cache_error(error: foyer::Error) -> Error {
    match error.downcast_ref::<FetchFailed>() {
        Some(FetchFailed::Failed(failed)) => Error::Cache(CacheError::Fetch(Arc::clone(failed))),
        Some(FetchFailed::Deadline { page, after }) => Error::Cache(CacheError::FetchDeadline {
            path: page.0.to_string(),
            offset: page.1,
            after: *after,
        }),
        None => backend(error),
    }
}

/// A failure of the cache itself as the crate's error.
fn backend(error: foyer::Error) -> Error {
    Error::Cache(CacheError::Backend(error))
}

#[cfg(test)]
mod tests {
    use std::fs::{self, OpenOptions};
    use std::os::unix::fs::FileExt;
    use std::path::Path;
    use std::sync::atomic::AtomicBool;

    use futures::future::{join, join_all};
    use tokio::sync::Notify;

    use super::*;
    use crate::approx::count;
    use crate::config::LocalConfig;
    use crate::generation::testing::TABLE;
    use crate::local::LocalDir;

    /// A memory budget with room for every page a test inserts.
    const ROOMY_BYTES: usize = 1024 * 1024;
    /// A memory budget below one page's weight: an insert evicts whatever
    /// its shard held, so a page leaves memory as soon as the next page
    /// lands in its shard.
    const TINY_BYTES: usize = 1;
    /// A disk level of sixteen blocks: room for every page the disk tests
    /// evict into it, so a page that left memory is on disk when read back.
    const DISK_BYTES: u64 = 16 * PAGE_CACHE_DISK_BLOCK_BYTES;
    /// Callers racing for one page: enough that coalescing is exercised and
    /// a fetch run per caller would show in the count.
    const CONCURRENT: usize = 8;
    /// The file every test page is keyed under; the cache never reads it,
    /// so any location serves.
    const FILE_ID: &str = "mem://w/db/t/data/f0.parquet";
    /// The page offset the first test page is keyed by; a key never reaches
    /// the file, so the value is arbitrary.
    const FIRST_OFFSET: u64 = 4;
    /// A second offset in the same file, so two pages share a file id and
    /// differ in offset alone.
    const SECOND_OFFSET: u64 = 4_096;
    /// The bytes the first test page holds; served pages are compared to it.
    const PAGE: &[u8] = b"page-bytes";
    /// The second test page's bytes, a different length from `PAGE`.
    const OTHER_PAGE: &[u8] = b"other-page-bytes";
    /// The fetch deadline the deadline test bounds its handle by: short, so
    /// the stalled fetch ends quickly.
    const FETCH_DEADLINE: Duration = Duration::from_millis(50);
    /// How long the deadline test lets its callers wait before it fails:
    /// far past `FETCH_DEADLINE`, so only a fetch that runs on reaches it.
    const FETCH_GUARD: Duration = Duration::from_secs(10);
    /// Offsets tried to find one whose key shares a shard with an anchor's;
    /// with foyer's shard count in the single digits, the first few suffice.
    const SHARD_SEARCH_OFFSETS: u64 = 1024;
    /// Bytes cut from the end of an encoded key so that its file id ends
    /// one byte short of its length prefix.
    const ONE_ID_BYTE_SHORT: usize = OFFSET_BYTES + 1;

    fn key(offset: u64) -> (Arc<str>, u64) {
        (Arc::from(FILE_ID), offset)
    }

    fn page_bytes(page: &[u8]) -> u64 {
        count(page.len())
    }

    /// A fetch whose future counts its runs in `runs` and answers `page`.
    fn answering(
        page: &'static [u8],
        runs: &Arc<AtomicU64>,
    ) -> impl Future<Output = Result<Bytes>> + Send + 'static {
        let runs = Arc::clone(runs);
        async move {
            runs.fetch_add(1, Ordering::Relaxed);
            Ok(Bytes::from_static(page))
        }
    }

    /// A fetch whose future counts its run, waits for `gate`, then answers
    /// `page`; concurrent callers all enqueue before it answers.
    fn gated(
        page: &'static [u8],
        runs: &Arc<AtomicU64>,
        gate: &Arc<Notify>,
    ) -> impl Future<Output = Result<Bytes>> + Send + 'static {
        let runs = Arc::clone(runs);
        let gate = Arc::clone(gate);
        async move {
            runs.fetch_add(1, Ordering::Relaxed);
            gate.notified().await;
            Ok(Bytes::from_static(page))
        }
    }

    /// A fetch whose future counts its run, waits for `gate`, then fails
    /// with a typed error.
    fn failing(
        runs: &Arc<AtomicU64>,
        gate: &Arc<Notify>,
    ) -> impl Future<Output = Result<Bytes>> + Send + 'static {
        let runs = Arc::clone(runs);
        let gate = Arc::clone(gate);
        async move {
            runs.fetch_add(1, Ordering::Relaxed);
            gate.notified().await;
            Err(Error::NotReady {
                table: TABLE.to_string(),
            })
        }
    }

    /// A fetch whose future counts its run and never answers, as a GET to a
    /// store that has stopped answering; `dropped` is set when the future is
    /// dropped.
    fn stalled(
        runs: &Arc<AtomicU64>,
        dropped: &Arc<AtomicBool>,
    ) -> impl Future<Output = Result<Bytes>> + Send + 'static {
        let runs = Arc::clone(runs);
        let probe = DropProbe(Arc::clone(dropped));
        async move {
            let _probe = probe;
            runs.fetch_add(1, Ordering::Relaxed);
            std::future::pending().await
        }
    }

    /// Sets its flag when dropped.
    struct DropProbe(Arc<AtomicBool>);

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Relaxed);
        }
    }

    /// A fetch that must not run: a page the cache holds is served without
    /// it, and a run fails the test.
    async fn never() -> Result<Bytes> {
        panic!("fetch ran for a page the cache holds")
    }

    /// A disk level over `dir` with a fresh counter, as `LocalDir` hands it out.
    fn disk_level(dir: &Path) -> (DiskLevel, Arc<IoStats>) {
        let stats = Arc::new(IoStats::default());
        let config = LocalConfig {
            dir: dir.to_path_buf(),
            index_budget_bytes: None,
            page_cache_disk_bytes: DISK_BYTES,
        };
        let local = LocalDir::open(&config, Arc::clone(&stats)).unwrap();
        (local.page_cache_level().unwrap(), stats)
    }

    /// The hybrid cache behind a `PageCache` built with a disk level.
    fn hybrid(cache: &PageCache) -> &HybridCache<PageKey, PageValue> {
        let Level::Hybrid { cache, .. } = cache.level.as_ref() else {
            panic!("a cache with a disk level is hybrid");
        };
        cache
    }

    /// The first offset after `anchor`'s whose key lands in the same memory
    /// shard as `anchor`. Eviction is per shard, so a page is displaced by
    /// a page of its own shard and by nothing else.
    fn offset_sharing_shard(cache: &HybridCache<PageKey, PageValue>, anchor: &PageKey) -> u64 {
        let memory = cache.memory();
        let shards = count(memory.shards());
        let shard_of = |key: &PageKey| memory.hash(key) % shards;
        (anchor.1 + 1..anchor.1 + 1 + SHARD_SEARCH_OFFSETS)
            .find(|offset| shard_of(&PageKey(Arc::clone(&anchor.0), *offset)) == shard_of(anchor))
            .unwrap()
    }

    /// Runs `CONCURRENT` callers for one key against `cache`, each with its
    /// own fetch from `fetch`, all enqueued before `gate` opens.
    async fn race<Fut>(
        cache: &PageCache,
        offset: u64,
        gate: &Arc<Notify>,
        fetch: impl Fn() -> Fut,
    ) -> Vec<Result<(Bytes, PageOrigin)>>
    where
        Fut: Future<Output = Result<Bytes>> + Send + 'static,
    {
        let callers = (0..CONCURRENT).map(|_| {
            let fut = fetch();
            async move { cache.get_or_fetch(key(offset), move || fut).await }
        });
        let (answers, ()) = join(join_all(callers), async { gate.notify_one() }).await;
        answers
    }

    /// Puts `PAGE` at `FIRST_OFFSET` through `cache`, displaces it from
    /// memory with `OTHER_PAGE` at an offset of the same shard, and waits
    /// until the level has written it; two fetches run. Deterministic on
    /// the current-thread test runtime: the eviction reaches the level's
    /// queue inside foyer's fetch task before this task resumes, so the
    /// wait is queued after it. A multi-thread runtime would race the two.
    async fn evict_to_disk(cache: &PageCache, runs: &Arc<AtomicU64>) {
        cache
            .get_or_fetch(key(FIRST_OFFSET), || answering(PAGE, runs))
            .await
            .unwrap();
        let displacing =
            offset_sharing_shard(hybrid(cache), &PageKey(Arc::from(FILE_ID), FIRST_OFFSET));
        cache
            .get_or_fetch(key(displacing), || answering(OTHER_PAGE, runs))
            .await
            .unwrap();
        hybrid(cache).storage().wait().await;
    }

    /// Overwrites every copy of `page` in the level's files under
    /// `cache_dir` with its bitwise complement, so the entry's checksum no
    /// longer matches what its block holds; returns how many were
    /// overwritten. Each copy is written in place: foyer sizes a partition
    /// file to the level's capacity as a sparse file, and rewriting the
    /// whole file would make every hole real and fill the temp volume when
    /// tests run side by side.
    fn corrupt_on_disk(cache_dir: &Path, page: &[u8]) -> usize {
        let mut copies = 0;
        for entry in fs::read_dir(cache_dir).unwrap() {
            let path = entry.unwrap().path();
            let bytes = fs::read(&path).unwrap();
            let file = OpenOptions::new().write(true).open(&path).unwrap();
            let flipped: Vec<u8> = page.iter().map(|byte| !byte).collect();
            let mut from = 0;
            while let Some(found) = bytes[from..]
                .windows(page.len())
                .position(|window| window == page)
            {
                let at = from + found;
                file.write_all_at(&flipped, count(at))
                    .unwrap_or_else(|error| panic!("writing {}: {error}", path.display()));
                copies += 1;
                from = at + page.len();
            }
        }
        copies
    }

    #[test]
    fn test_encode_page_key_round_trips() {
        let cases = [
            PageKey(Arc::from(""), 0),
            PageKey(Arc::from(FILE_ID), SECOND_OFFSET),
            PageKey(Arc::from(FILE_ID), u64::MAX),
        ];
        for key in cases {
            let mut encoded = Vec::new();
            key.encode(&mut encoded).unwrap();
            assert_eq!(encoded.len(), key.estimated_size());
            assert_eq!(PageKey::decode(&mut encoded.as_slice()).unwrap(), key);
        }
    }

    #[test]
    fn test_decode_page_key_refuses_a_file_id_shorter_than_its_prefix() {
        let key = PageKey(Arc::from(FILE_ID), SECOND_OFFSET);
        let mut encoded = Vec::new();
        key.encode(&mut encoded).unwrap();
        encoded.truncate(encoded.len() - ONE_ID_BYTE_SHORT);
        let error = PageKey::decode(&mut encoded.as_slice()).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Io);
    }

    #[test]
    fn test_encode_page_value_is_the_raw_bytes() {
        let block = vec![0; approx::length(PAGE_CACHE_DISK_BLOCK_BYTES)];
        let cases: [&[u8]; 3] = [PAGE, b"", &block];
        for page in cases {
            let value = PageValue(Bytes::copy_from_slice(page));
            let mut encoded = Vec::new();
            value.encode(&mut encoded).unwrap();
            assert_eq!(encoded, page);
            assert_eq!(encoded.len(), value.estimated_size());
            assert_eq!(PageValue::decode(&mut encoded.as_slice()).unwrap().0, page);
        }
    }

    #[tokio::test]
    async fn test_new_without_disk_is_memory_only() {
        let cache = PageCache::new(ROOMY_BYTES, None).await.unwrap();
        assert!(matches!(cache.level.as_ref(), Level::Memory(_)));
        let runs = Arc::new(AtomicU64::new(0));
        let (page, origin) = cache
            .get_or_fetch(key(FIRST_OFFSET), || answering(PAGE, &runs))
            .await
            .unwrap();
        assert_eq!(&page[..], PAGE);
        assert_eq!(origin, PageOrigin::Fetched);
        let (again, origin) = cache.get_or_fetch(key(FIRST_OFFSET), never).await.unwrap();
        assert_eq!(&again[..], PAGE);
        assert_eq!(origin, PageOrigin::Cached);
        assert_eq!(runs.load(Ordering::Relaxed), 1);
        assert_eq!(cache.fetches(), 1);
        assert_eq!(cache.hits(), 1);
    }

    #[tokio::test]
    async fn test_close_memory_level_is_ok_and_keeps_serving() {
        let cache = PageCache::new(ROOMY_BYTES, None).await.unwrap();
        let runs = Arc::new(AtomicU64::new(0));
        cache
            .get_or_fetch(key(FIRST_OFFSET), || answering(PAGE, &runs))
            .await
            .unwrap();
        cache.close().await.unwrap();
        let (again, _) = cache.get_or_fetch(key(FIRST_OFFSET), never).await.unwrap();
        assert_eq!(&again[..], PAGE);
        assert_eq!(cache.fetches(), 1);
        assert_eq!(cache.hits(), 1);
    }

    #[tokio::test]
    async fn test_get_or_fetch_coalesces_concurrent_misses() {
        let cache = PageCache::new(ROOMY_BYTES, None).await.unwrap();
        let runs = Arc::new(AtomicU64::new(0));
        let gate = Arc::new(Notify::new());
        let answers = race(&cache, FIRST_OFFSET, &gate, || gated(PAGE, &runs, &gate)).await;
        assert_eq!(answers.len(), CONCURRENT);
        for answer in answers {
            let (page, origin) = answer.unwrap();
            assert_eq!(&page[..], PAGE);
            assert_eq!(origin, PageOrigin::Fetched);
        }
        assert_eq!(runs.load(Ordering::Relaxed), 1);
        assert_eq!(cache.fetches(), 1);
        assert_eq!(cache.hits(), 0);

        let (again, origin) = cache.get_or_fetch(key(FIRST_OFFSET), never).await.unwrap();
        assert_eq!(&again[..], PAGE);
        assert_eq!(origin, PageOrigin::Cached);
        assert_eq!(cache.fetches(), 1);
        assert_eq!(cache.hits(), 1);
    }

    #[tokio::test]
    async fn test_get_or_fetch_fetch_error_reaches_every_waiter() {
        let cache = PageCache::new(ROOMY_BYTES, None).await.unwrap();
        let runs = Arc::new(AtomicU64::new(0));
        let gate = Arc::new(Notify::new());
        let answers = race(&cache, FIRST_OFFSET, &gate, || failing(&runs, &gate)).await;
        assert_eq!(answers.len(), CONCURRENT);
        let mut shared: Vec<Arc<Error>> = Vec::with_capacity(CONCURRENT);
        for answer in answers {
            let Err(Error::Cache(CacheError::Fetch(error))) = answer else {
                panic!("every waiter receives the fetch failure");
            };
            assert!(matches!(*error, Error::NotReady { .. }));
            shared.push(error);
        }
        assert!(shared.iter().all(|error| Arc::ptr_eq(error, &shared[0])));
        assert_eq!(runs.load(Ordering::Relaxed), 1);
        assert_eq!(cache.fetches(), 1);
        assert_eq!(cache.hits(), 0);

        let (page, _) = cache
            .get_or_fetch(key(FIRST_OFFSET), || answering(PAGE, &runs))
            .await
            .unwrap();
        assert_eq!(&page[..], PAGE);
        assert_eq!(cache.fetches(), 2);
    }

    /// A fetch that outlasts its handle's deadline is dropped at it, and
    /// every coalesced waiter receives the typed deadline error for the
    /// page; nothing is cached, so the next caller runs a fetch of its own.
    #[tokio::test]
    async fn test_get_or_fetch_ends_a_fetch_at_its_deadline_for_every_waiter() {
        let cache = PageCache::new(ROOMY_BYTES, None)
            .await
            .unwrap()
            .with_fetch_deadline(FETCH_DEADLINE);
        let runs = Arc::new(AtomicU64::new(0));
        let dropped = Arc::new(AtomicBool::new(false));
        let gate = Arc::new(Notify::new());
        let started = tokio::time::Instant::now();
        let answers = tokio::time::timeout(
            FETCH_GUARD,
            race(&cache, FIRST_OFFSET, &gate, || stalled(&runs, &dropped)),
        )
        .await
        .expect("the fetch ends at its deadline, before the guard");
        assert!(started.elapsed() >= FETCH_DEADLINE);
        assert_eq!(answers.len(), CONCURRENT);
        for answer in answers {
            let Err(Error::Cache(CacheError::FetchDeadline {
                path,
                offset,
                after,
            })) = answer
            else {
                panic!("every waiter receives the deadline: {answer:?}");
            };
            assert_eq!((path.as_str(), offset), (FILE_ID, FIRST_OFFSET));
            assert_eq!(after, FETCH_DEADLINE);
        }
        assert!(dropped.load(Ordering::Relaxed), "the fetch was dropped");
        assert_eq!(runs.load(Ordering::Relaxed), 1);
        assert_eq!(cache.fetches(), 1);

        let (page, origin) = cache
            .get_or_fetch(key(FIRST_OFFSET), || answering(PAGE, &runs))
            .await
            .unwrap();
        assert_eq!(&page[..], PAGE);
        assert_eq!(origin, PageOrigin::Fetched);
        assert_eq!(cache.fetches(), 2);
    }

    #[tokio::test]
    async fn test_usage_grows_by_page_bytes_plus_overhead() {
        let cache = PageCache::new(ROOMY_BYTES, None).await.unwrap();
        assert_eq!(cache.usage(), 0);
        let runs = Arc::new(AtomicU64::new(0));
        cache
            .get_or_fetch(key(FIRST_OFFSET), || answering(PAGE, &runs))
            .await
            .unwrap();
        assert_eq!(cache.usage(), PAGE.len() + ENTRY_OVERHEAD_BYTES);
        cache
            .get_or_fetch(key(SECOND_OFFSET), || answering(OTHER_PAGE, &runs))
            .await
            .unwrap();
        assert_eq!(
            cache.usage(),
            PAGE.len() + OTHER_PAGE.len() + 2 * ENTRY_OVERHEAD_BYTES
        );
    }

    #[tokio::test]
    async fn test_get_or_fetch_serves_a_hybrid_page_from_memory_before_eviction() {
        let dir = tempfile::tempdir().unwrap();
        let (level, stats) = disk_level(dir.path());
        let cache = PageCache::new(ROOMY_BYTES, Some(level)).await.unwrap();
        let runs = Arc::new(AtomicU64::new(0));
        cache
            .get_or_fetch(key(FIRST_OFFSET), || answering(PAGE, &runs))
            .await
            .unwrap();
        assert_eq!(cache.fetches(), 1);
        assert_eq!(cache.hits(), 0);

        let (page, origin) = cache.get_or_fetch(key(FIRST_OFFSET), never).await.unwrap();
        assert_eq!(&page[..], PAGE);
        assert_eq!(origin, PageOrigin::Cached);
        assert_eq!(runs.load(Ordering::Relaxed), 1);
        assert_eq!(cache.fetches(), 1);
        assert_eq!(cache.hits(), 1);
        assert_eq!(stats.local_reads(), 0);
    }

    #[tokio::test]
    async fn test_get_or_fetch_serves_evicted_page_from_disk() {
        let dir = tempfile::tempdir().unwrap();
        let (level, stats) = disk_level(dir.path());
        let cache = PageCache::new(TINY_BYTES, Some(level)).await.unwrap();
        let runs = Arc::new(AtomicU64::new(0));
        evict_to_disk(&cache, &runs).await;
        assert_eq!(cache.fetches(), 2);
        assert_eq!(cache.hits(), 0);
        assert_eq!(stats.local_reads(), 0);

        let (page, origin) = cache.get_or_fetch(key(FIRST_OFFSET), never).await.unwrap();
        assert_eq!(&page[..], PAGE);
        assert_eq!(origin, PageOrigin::Cached);
        assert_eq!(runs.load(Ordering::Relaxed), 2);
        assert_eq!(cache.fetches(), 2);
        assert_eq!(cache.hits(), 1);
        assert_eq!(stats.local_reads(), 1);
        assert_eq!(stats.local_bytes(), page_bytes(PAGE));
    }

    /// Lookups coalesced onto one disk read each count a hit and a local
    /// read: the count is per lookup served, as under `DiskChunks::page`.
    /// Deterministic on the current-thread runtime for the same reason as
    /// `race`: every caller enqueues before foyer's fetch task runs.
    #[tokio::test]
    async fn test_get_or_fetch_charges_every_lookup_coalesced_onto_one_disk_read() {
        let dir = tempfile::tempdir().unwrap();
        let (level, stats) = disk_level(dir.path());
        let cache = PageCache::new(TINY_BYTES, Some(level)).await.unwrap();
        let runs = Arc::new(AtomicU64::new(0));
        evict_to_disk(&cache, &runs).await;
        let gate = Arc::new(Notify::new());

        let answers = race(&cache, FIRST_OFFSET, &gate, never).await;
        assert_eq!(answers.len(), CONCURRENT);
        for answer in answers {
            let (page, origin) = answer.unwrap();
            assert_eq!(&page[..], PAGE);
            assert_eq!(origin, PageOrigin::Cached);
        }
        assert_eq!(runs.load(Ordering::Relaxed), 2);
        assert_eq!(cache.fetches(), 2);
        assert_eq!(cache.hits(), count(CONCURRENT));
        assert_eq!(stats.local_reads(), count(CONCURRENT));
        assert_eq!(stats.local_bytes(), count(CONCURRENT) * page_bytes(PAGE));
    }

    /// The disk level as production reaches it: the page leaves memory by
    /// eviction, is written by the level, and a new cache over the same
    /// directory serves it with a local read and no fetch. Eviction, not
    /// the close, puts it there: it has left memory before the first cache
    /// closes, so the close can flush only the displacing page.
    #[tokio::test]
    async fn test_new_recovers_the_disk_level_on_restart() {
        let dir = tempfile::tempdir().unwrap();
        let (level, first_stats) = disk_level(dir.path());
        let first = PageCache::new(TINY_BYTES, Some(level)).await.unwrap();
        let runs = Arc::new(AtomicU64::new(0));
        evict_to_disk(&first, &runs).await;
        assert_eq!(first_stats.local_reads(), 0);
        first.close().await.unwrap();
        drop(first);

        let (level, stats) = disk_level(dir.path());
        let second = PageCache::new(ROOMY_BYTES, Some(level)).await.unwrap();
        let (page, origin) = second.get_or_fetch(key(FIRST_OFFSET), never).await.unwrap();
        assert_eq!(&page[..], PAGE);
        assert_eq!(origin, PageOrigin::Cached);
        assert_eq!(runs.load(Ordering::Relaxed), 2);
        assert_eq!(second.fetches(), 0);
        assert_eq!(second.hits(), 1);
        assert_eq!(stats.local_reads(), 1);
        assert_eq!(stats.local_bytes(), page_bytes(PAGE));

        let (again, _) = second.get_or_fetch(key(FIRST_OFFSET), never).await.unwrap();
        assert_eq!(&again[..], PAGE);
        assert_eq!(second.hits(), 2);
        assert_eq!(stats.local_reads(), 1);
    }

    /// A page whose disk copy no longer matches its checksum is refused by
    /// the level and fetched instead, with no error to the caller (RFD 017:
    /// the directory is disposable). The copy is damaged after the first
    /// cache closes, so the level's index still names it when the second
    /// cache recovers.
    #[tokio::test]
    async fn test_get_or_fetch_fetches_a_page_whose_disk_copy_is_corrupt() {
        let dir = tempfile::tempdir().unwrap();
        let (level, _) = disk_level(dir.path());
        let cache_dir = level.dir().to_path_buf();
        let first = PageCache::new(TINY_BYTES, Some(level)).await.unwrap();
        let runs = Arc::new(AtomicU64::new(0));
        evict_to_disk(&first, &runs).await;
        first.close().await.unwrap();
        drop(first);
        assert!(corrupt_on_disk(&cache_dir, PAGE) >= 1);

        let (level, stats) = disk_level(dir.path());
        let second = PageCache::new(ROOMY_BYTES, Some(level)).await.unwrap();
        let (page, origin) = second
            .get_or_fetch(key(FIRST_OFFSET), || answering(PAGE, &runs))
            .await
            .unwrap();
        assert_eq!(&page[..], PAGE);
        assert_eq!(origin, PageOrigin::Fetched);
        assert_eq!(runs.load(Ordering::Relaxed), 3);
        assert_eq!(second.fetches(), 1);
        assert_eq!(second.hits(), 0);
        assert_eq!(stats.local_reads(), 0);
    }
}
