//! A store that logs every GET it answers: the object's file name, the
//! byte range the store returned, and the latency to the answer and to its
//! body's last byte, so the benchmark program can say how a GET's latency
//! follows its bytes and the pages it spans (RFD 025, Consequences: whether
//! a span GET's latency on object storage follows its pages or a fixed
//! first-byte time). Each GET writes one slot of a log sized before the
//! reads start: one atomic increment claims it, and relaxed stores fill it,
//! with no lock and no map on the GET's path; the object is named by a hash
//! of its file name, so naming it allocates nothing. Beyond the store's own
//! work a GET costs that, a hash of the file name, three clock reads, and
//! one boxed body stream that notes when the body ends. A full log records
//! nothing more and counts what it drops.

use std::fmt;
use std::hash::{DefaultHasher, Hash as _, Hasher as _};
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{BoxStream, Stream, StreamExt as _};
use icetray_core::approx;
use icetray_core::time::{from_micros, micros};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

/// What a slot field holds until the GET writes it: the range of a GET the
/// store never answered, or a latency not reached.
const UNSET: u64 = u64::MAX;

/// One GET's record, written by that GET alone and read once no GET is in
/// flight.
#[derive(Debug)]
struct Slot {
    /// `GetLog::name_of_location` of the object.
    name: AtomicU64,
    /// The ranges the call asked for: one for a GET, any number for a
    /// ranged read of several.
    parts: AtomicU64,
    /// The first byte the store returned.
    start: AtomicU64,
    /// One past the last.
    end: AtomicU64,
    /// Microseconds from the request to the store's answer.
    first_byte: AtomicU64,
    /// Microseconds from the request to the body's last byte.
    last_byte: AtomicU64,
}

impl Slot {
    fn new() -> Self {
        Self {
            name: AtomicU64::new(0),
            parts: AtomicU64::new(0),
            start: AtomicU64::new(UNSET),
            end: AtomicU64::new(UNSET),
            first_byte: AtomicU64::new(UNSET),
            last_byte: AtomicU64::new(UNSET),
        }
    }

    /// Starts the record of a GET of `name` asking `parts` ranges.
    fn claim(&self, name: u64, parts: usize) {
        self.name.store(name, Ordering::Relaxed);
        self.parts.store(approx::count(parts), Ordering::Relaxed);
        for field in [&self.start, &self.end, &self.first_byte, &self.last_byte] {
            field.store(UNSET, Ordering::Relaxed);
        }
    }

    /// Records the store's answer: the bytes it returned, `elapsed` after
    /// the request.
    fn answered(&self, range: &Range<u64>, elapsed: Duration) {
        self.start.store(range.start, Ordering::Relaxed);
        self.end.store(range.end, Ordering::Relaxed);
        self.first_byte.store(micros(elapsed), Ordering::Relaxed);
    }

    /// Records the body's last byte, `elapsed` after the request.
    fn finished(&self, elapsed: Duration) {
        self.last_byte.store(micros(elapsed), Ordering::Relaxed);
    }

    fn read(&self) -> LoggedGet {
        let latency = |field: &AtomicU64| {
            let value = field.load(Ordering::Relaxed);
            (value != UNSET).then(|| from_micros(value))
        };
        let (start, end) = (
            self.start.load(Ordering::Relaxed),
            self.end.load(Ordering::Relaxed),
        );
        LoggedGet {
            name: self.name.load(Ordering::Relaxed),
            parts: self.parts.load(Ordering::Relaxed),
            range: (start != UNSET).then_some(start..end),
            first_byte: latency(&self.first_byte),
            last_byte: latency(&self.last_byte),
        }
    }
}

/// One GET as the log recorded it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoggedGet {
    /// `GetLog::name_of_location` of the object.
    pub name: u64,
    /// The ranges the call asked for: one for a GET, more for a ranged
    /// read of several, which the log records as one call.
    pub parts: u64,
    /// The bytes the store returned, from the first range's start to the
    /// last range's end for a read of several; `None` when the store never
    /// answered, as for a request dropped or refused.
    pub range: Option<Range<u64>>,
    /// The time from the request to the store's answer: for a GET, its
    /// headers; for a ranged read of several, its whole answer.
    pub first_byte: Option<Duration>,
    /// The time from the request to the body's last byte; `None` for a
    /// body not read to its end, and for a local file, which its reader
    /// reads after the store answered.
    pub last_byte: Option<Duration>,
}

impl LoggedGet {
    /// The bytes the store returned; zero when it never answered.
    pub fn bytes(&self) -> u64 {
        self.range
            .as_ref()
            .map_or(0, |range| range.end - range.start)
    }
}

/// A store over `inner` that logs every GET into slots reserved when it is
/// built (`GetLog::over`). The log is the GETs' since the last `restart`,
/// which a caller makes while no GET is in flight: a GET that outlives a
/// restart writes into a slot the next run reuses.
pub struct GetLog {
    inner: Arc<dyn ObjectStore>,
    slots: Arc<[Slot]>,
    /// The GETs claimed since the last restart, logged or dropped.
    next: AtomicUsize,
}

impl GetLog {
    /// The bytes one slot of the log holds, what a log of `slots` slots
    /// reserves for each.
    pub const SLOT_BYTES: usize = size_of::<Slot>();

    /// A log over `inner` with room for `slots` GETs between restarts.
    pub fn over(inner: Arc<dyn ObjectStore>, slots: usize) -> Self {
        Self {
            inner,
            slots: (0..slots).map(|_| Slot::new()).collect(),
            next: AtomicUsize::new(0),
        }
    }

    /// The name a GET of the object at `location` is logged under: a hash
    /// of its file name, the last `/` segment, so a store's path and a
    /// manifest's URI of one file name it alike, the GET allocates nothing
    /// to name it, and every log of the process names it the same.
    pub fn name_of_location(location: &str) -> u64 {
        let file_name = location.rsplit('/').next().unwrap_or_default();
        let mut hasher = DefaultHasher::new();
        file_name.hash(&mut hasher);
        hasher.finish()
    }

    /// Starts the log anew; call it while no GET is in flight.
    pub fn restart(&self) {
        self.next.store(0, Ordering::Relaxed);
    }

    /// The GETs logged since the last restart, in the order they were
    /// issued; read while none is in flight.
    pub fn records(&self) -> Vec<LoggedGet> {
        let logged = self.next.load(Ordering::Relaxed).min(self.slots.len());
        self.slots[..logged].iter().map(Slot::read).collect()
    }

    /// The GETs since the last restart that found the log full.
    pub fn dropped(&self) -> u64 {
        let claimed = self.next.load(Ordering::Relaxed);
        approx::count(claimed.saturating_sub(self.slots.len()))
    }

    /// The slot of a GET of `location` asking `parts` ranges, claimed;
    /// `None` when the log is full.
    fn claim(&self, location: &Path, parts: usize) -> Option<usize> {
        let at = self.next.fetch_add(1, Ordering::Relaxed);
        let slot = self.slots.get(at)?;
        slot.claim(Self::name_of_location(location.as_ref()), parts);
        Some(at)
    }
}

impl fmt::Debug for GetLog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GetLog")
            .field("inner", &self.inner)
            .field("slots", &self.slots.len())
            .finish_non_exhaustive()
    }
}

impl fmt::Display for GetLog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GetLog({})", self.inner)
    }
}

/// A GET's body, which records in its slot when it yields its last byte.
struct TimedBody {
    body: BoxStream<'static, object_store::Result<Bytes>>,
    slots: Arc<[Slot]>,
    at: usize,
    started: Instant,
}

impl Stream for TimedBody {
    type Item = object_store::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let polled = self.body.poll_next_unpin(cx);
        if let Poll::Ready(None) = polled {
            self.slots[self.at].finished(self.started.elapsed());
        }
        polled
    }
}

#[async_trait]
impl ObjectStore for GetLog {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let Some(at) = self.claim(location, 1) else {
            return self.inner.get_opts(location, options).await;
        };
        let started = Instant::now();
        let mut result = self.inner.get_opts(location, options).await?;
        self.slots[at].answered(&result.range, started.elapsed());
        result.payload = match result.payload {
            GetResultPayload::Stream(body) => GetResultPayload::Stream(Box::pin(TimedBody {
                body,
                slots: Arc::clone(&self.slots),
                at,
                started,
            })),
            file @ GetResultPayload::File(..) => file,
        };
        Ok(result)
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        let Some(at) = self.claim(location, ranges.len()) else {
            return self.inner.get_ranges(location, ranges).await;
        };
        let started = Instant::now();
        let answer = self.inner.get_ranges(location, ranges).await?;
        let elapsed = started.elapsed();
        let covering = ranges.iter().map(|range| range.start).min().unwrap_or(0)
            ..ranges.iter().map(|range| range.end).max().unwrap_or(0);
        self.slots[at].answered(&covering, elapsed);
        self.slots[at].finished(elapsed);
        Ok(answer)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use object_store::ObjectStoreExt as _;
    use object_store::memory::InMemory;

    use super::*;

    /// The object every test reads, and its file name.
    const OBJECT: &str = "data/f0.parquet";
    const FILE_NAME: &str = "f0.parquet";

    /// The bytes it holds.
    const CONTENT: &[u8] = b"0123456789";

    /// A range inside the object, and another apart from it.
    const INNER_RANGE: Range<u64> = 1..3;
    const LATER_RANGE: Range<u64> = 6..9;

    /// Room for every GET a test makes.
    const ROOMY: usize = 16;

    /// A log of `slots` over an in-memory store holding `OBJECT`, which
    /// answers with a stream body as an S3 store does.
    async fn log(slots: usize) -> (GetLog, Path) {
        let inner = InMemory::new();
        let path = Path::from(OBJECT);
        inner.put(&path, CONTENT.into()).await.unwrap();
        (GetLog::over(Arc::new(inner), slots), path)
    }

    #[tokio::test]
    async fn test_get_opts_logs_the_range_and_both_latencies_of_a_read_body() {
        let (log, path) = log(ROOMY).await;
        let bytes = log.get_range(&path, INNER_RANGE).await.unwrap();
        assert_eq!(&bytes[..], &CONTENT[1..3]);
        let records = log.records();
        assert_eq!(records.len(), 1);
        let get = &records[0];
        assert_eq!(get.name, GetLog::name_of_location(FILE_NAME));
        assert_eq!(
            GetLog::name_of_location(&format!("s3://bucket/{OBJECT}")),
            get.name,
            "a URI of the file names it as the store's path does"
        );
        assert_eq!((get.parts, get.range.clone()), (1, Some(INNER_RANGE)));
        assert_eq!(get.bytes(), INNER_RANGE.end - INNER_RANGE.start);
        let (Some(first), Some(last)) = (get.first_byte, get.last_byte) else {
            panic!("a GET read to its end has both latencies: {get:?}");
        };
        assert!(first <= last);
        assert_eq!(log.dropped(), 0);
    }

    #[tokio::test]
    async fn test_get_opts_of_a_body_not_read_has_no_last_byte_until_it_is() {
        let (log, path) = log(ROOMY).await;
        let result = log.get_opts(&path, GetOptions::default()).await.unwrap();
        let unread = &log.records()[0];
        assert!(unread.first_byte.is_some() && unread.last_byte.is_none());
        assert_eq!(unread.range, Some(0..approx::count(CONTENT.len())));
        result.bytes().await.unwrap();
        assert!(log.records()[0].last_byte.is_some());
    }

    #[tokio::test]
    async fn test_get_ranges_logs_one_call_over_the_ranges_it_asks() {
        let (log, path) = log(ROOMY).await;
        log.get_ranges(&path, &[INNER_RANGE, LATER_RANGE])
            .await
            .unwrap();
        let records = log.records();
        assert_eq!(records.len(), 1);
        let ranges_asked = 2;
        assert_eq!(records[0].parts, ranges_asked);
        assert_eq!(records[0].range, Some(INNER_RANGE.start..LATER_RANGE.end));
        assert!(records[0].first_byte.is_some() && records[0].last_byte.is_some());
    }

    #[tokio::test]
    async fn test_get_opts_refused_logs_no_range_and_no_latency() {
        let (log, _) = log(ROOMY).await;
        let missing = Path::from("data/absent.parquet");
        assert!(log.get(&missing).await.is_err());
        let refused = &log.records()[0];
        assert_eq!(refused.name, GetLog::name_of_location("absent.parquet"));
        assert_eq!((refused.range.clone(), refused.first_byte), (None, None));
        assert_eq!(refused.bytes(), 0);
    }

    #[tokio::test]
    async fn test_records_of_a_full_log_count_the_dropped_and_restart_empties_it() {
        let one_slot = 1;
        let (log, path) = log(one_slot).await;
        let gets = 3;
        for _ in 0..gets {
            log.get_range(&path, INNER_RANGE).await.unwrap();
        }
        assert_eq!(log.records().len(), one_slot);
        assert_eq!(log.dropped(), gets - 1);
        log.restart();
        assert_eq!((log.records().len(), log.dropped()), (0, 0));
        log.get_range(&path, LATER_RANGE).await.unwrap();
        assert_eq!(log.records()[0].range, Some(LATER_RANGE));
    }
}
