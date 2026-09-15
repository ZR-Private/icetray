//! Parquet byte access for point reads (RFD 004, RFD 006): a handle over
//! one data file that reads through `Storage`, the footer load that brings
//! both page indexes with it in one GET, and `Segments`, an in-memory
//! `ChunkReader` over the byte ranges a lookup already holds that refuses
//! any byte outside them rather than fetching it.

use std::ops::Range;
use std::sync::Arc;

use bytes::{Buf, Bytes};
use futures::future::BoxFuture;
use parquet::arrow::async_reader::MetadataFetch;
use parquet::errors::ParquetError;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use parquet::file::reader::{ChunkReader, Length};

use crate::cache::PageCache;
use crate::error::{Error, Result};
use crate::storage::{HedgeWindow, Located, Storage};

/// Bytes read from the tail of a file in the first metadata request: the
/// footer's own eight bytes, the thrift metadata before them, and the page
/// indexes before those. A contract-shaped file (RFD 002: 32 to 64
/// mebibytes, 1,000-row pages, values bounded at 64 kibibytes) has on the
/// order of a thousand pages per column; its offset indexes cost a few tens
/// of bytes per page and the key column's column index a key's length per
/// page, so the footer and both indexes fit in half a mebibyte and
/// `load_metadata` costs one GET. A file whose tail is larger is not an
/// error: the reader issues one more GET when the page indexes start
/// before the prefetched tail, and two more when the thrift metadata
/// itself does not fit, each counted like the first as a load GET.
const FOOTER_PREFETCH_BYTES: usize = 512 * 1024;

/// One data file: where it is, how big the manifest says it is, and its
/// table's hedge window for the reads a client waits on (RFD 013). Opening
/// issues no request: the size comes from the manifest, never from a HEAD.
#[derive(Debug)]
pub struct FileHandle {
    /// The location as the manifest recorded it, before any rewriting.
    pub location: String,
    located: Located,
    size: u64,
    storage: Arc<Storage>,
    file_id: Arc<str>,
    hedge: Arc<HedgeWindow>,
}

impl FileHandle {
    /// Resolves `location` through `storage`. `size` is the manifest's;
    /// `hedge` is the window of the table the file belongs to, so the
    /// lookup reads this handle issues are hedged against that table's own
    /// latencies and never another's. Fails only when no store serves the
    /// location.
    pub fn open(
        storage: Arc<Storage>,
        location: &str,
        size: u64,
        hedge: Arc<HedgeWindow>,
    ) -> Result<Self> {
        let located = storage.locate(location)?;
        Ok(Self {
            location: location.to_string(),
            located,
            size,
            storage,
            file_id: Arc::from(location),
            hedge,
        })
    }

    /// The file's size in bytes, as the manifest recorded it.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// The storage this handle reads through, whose counters price its reads.
    pub fn storage(&self) -> &Arc<Storage> {
        &self.storage
    }

    /// One byte range on the load path: one load GET, never hedged.
    pub async fn read_range(&self, range: Range<u64>) -> Result<Bytes> {
        self.storage.read_range(&self.located, range).await
    }

    /// Several byte ranges on the load path in one call to the store: one
    /// load GET per range, never hedged.
    pub async fn read_ranges(&self, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.storage.read_ranges(&self.located, ranges).await
    }

    /// One byte range on the lookup path: one lookup GET, hedged against
    /// the handle's table window and recorded into it (RFD 013).
    pub async fn read_range_hedged(&self, range: Range<u64>) -> Result<Bytes> {
        self.storage
            .read_range_hedged(&self.located, range, &self.hedge)
            .await
    }

    /// One page on the lookup path. Through `cache` when given, keyed by
    /// the file id and `range.start`: parquet pages are always addressed at
    /// their offset-index position, so a page is one entry however it is
    /// reached, and concurrent lookups for one page share one fetch. That
    /// fetch, and every read without a cache, is one hedged lookup GET.
    pub async fn read_page(
        self: &Arc<Self>,
        range: Range<u64>,
        cache: Option<&PageCache>,
    ) -> Result<Bytes> {
        match cache {
            Some(cache) => {
                let handle = Arc::clone(self);
                let key = (Arc::clone(&self.file_id), range.start);
                cache
                    .get_or_fetch(
                        key,
                        move || async move { handle.read_range_hedged(range).await },
                    )
                    .await
            }
            None => self.read_range_hedged(range).await,
        }
    }

    /// The footer with both page indexes, kept for the life of a
    /// generation. One load GET when the tail fits `FOOTER_PREFETCH_BYTES`;
    /// one more when the page indexes start before the prefetched tail;
    /// two more when the thrift metadata itself does not fit, since the
    /// reader then fetches the metadata and the indexes separately. Every
    /// one is a load read: never hedged, and counted as a load GET. The
    /// page index is required, so a file written without one is
    /// `Error::Parquet`; a fetch that failed comes back as the storage
    /// error it was.
    pub async fn load_metadata(self: &Arc<Self>) -> Result<Arc<ParquetMetaData>> {
        self.load_metadata_prefetching(FOOTER_PREFETCH_BYTES).await
    }

    /// `load_metadata` with a tail read of `prefetch` bytes, so a test
    /// reaches the paths a tail larger than the prefetch takes.
    async fn load_metadata_prefetching(
        self: &Arc<Self>,
        prefetch: usize,
    ) -> Result<Arc<ParquetMetaData>> {
        let metadata = ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Required)
            .with_prefetch_hint(Some(prefetch))
            .load_and_finish(LoadFetch(Arc::clone(self)), self.size)
            .await
            .map_err(reclaim)?;
        Ok(Arc::new(metadata))
    }
}

/// `MetadataFetch` over a handle: every range the metadata reader asks for
/// is a load read, since a footer is bulk work nobody waits on (RFD 013).
struct LoadFetch(Arc<FileHandle>);

impl MetadataFetch for LoadFetch {
    fn fetch(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        Box::pin(async move { self.0.read_range(range).await.map_err(external) })
    }
}

/// The crate's error inside parquet's, for a fetch the parquet reader drove.
fn external(error: Error) -> ParquetError {
    ParquetError::External(Box::new(error))
}

/// A parquet failure as the crate's error. A fetch failure went in through
/// `external` and comes back out as the storage error it was, so a caller
/// matches on `Error::ObjectStore` and not on a parquet wrapper; anything
/// else is the parquet library's own.
fn reclaim(error: ParquetError) -> Error {
    let ParquetError::External(source) = error else {
        return Error::Parquet(error);
    };
    match source.downcast::<Error>() {
        Ok(ours) => *ours,
        Err(foreign) => Error::Parquet(ParquetError::External(foreign)),
    }
}

/// Why a `Segments` refused a read (RFD 006): the byte was not in a held
/// range, or the read started inside one and ran past its end. Carried
/// inside `ParquetError::External`; `Segments::fault` gets it back out, so
/// a test asserts the variant and never message text.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SegmentsFault {
    /// No held range contains the byte.
    #[error("byte {offset} is not resident")]
    NotResident {
        /// The file offset asked for.
        offset: u64,
    },
    /// The read began inside a held range and ended past it.
    #[error("read {start}..{end} runs past the resident range {held_start}..{held_end}")]
    CrossesResidentRange {
        /// The first byte asked for.
        start: u64,
        /// One past the last byte asked for.
        end: u64,
        /// The held range's first byte.
        held_start: u64,
        /// One past the held range's last byte.
        held_end: u64,
    },
}

/// Byte ranges of one file held in memory, addressed by file offset. Any
/// read outside them is an error, never a fetch, so a lookup that would
/// touch the network fails loudly instead of quietly costing a GET (RFD
/// 006).
#[derive(Clone, Debug)]
pub struct Segments {
    parts: Vec<(Range<u64>, Bytes)>,
    file_size: u64,
}

impl Segments {
    /// Holds `parts` of a file `file_size` bytes long. Each part's bytes
    /// are exactly its range's length; a part that is not is a caller's
    /// fault, checked in debug builds.
    pub fn new(mut parts: Vec<(Range<u64>, Bytes)>, file_size: u64) -> Self {
        debug_assert!(
            parts.iter().all(|(range, bytes)| {
                u64::try_from(bytes.len()).is_ok_and(|len| len == range.end - range.start)
            }),
            "each part's bytes are exactly its range's length"
        );
        parts.sort_by_key(|(range, _)| range.start);
        Self { parts, file_size }
    }

    /// Bytes held, summed over the parts.
    pub fn resident_len(&self) -> usize {
        self.parts.iter().map(|(_, bytes)| bytes.len()).sum()
    }

    /// The `SegmentsFault` a parquet error carries, when it is one: a
    /// reader that decoded over a `Segments` and failed learns whether it
    /// asked for a byte it was never given.
    pub fn fault(error: &ParquetError) -> Option<&SegmentsFault> {
        if let ParquetError::External(source) = error {
            source.downcast_ref::<SegmentsFault>()
        } else {
            None
        }
    }

    /// The held range containing `start`, its bytes, and the offset of
    /// `start` within them.
    fn find(&self, start: u64) -> parquet::errors::Result<(&Range<u64>, &Bytes, usize)> {
        self.parts
            .iter()
            .find(|(range, _)| range.contains(&start))
            .map(|(range, bytes)| (range, bytes, offset_within(range, start)))
            .ok_or_else(|| refusal(SegmentsFault::NotResident { offset: start }))
    }
}

/// The offset of `start` within a range that contains it.
fn offset_within(range: &Range<u64>, start: u64) -> usize {
    usize::try_from(start - range.start)
        .expect("a u64 fits usize on the 64-bit targets the crate supports")
}

/// A `SegmentsFault` as the parquet reader receives it.
fn refusal(fault: SegmentsFault) -> ParquetError {
    ParquetError::External(Box::new(fault))
}

impl Length for Segments {
    fn len(&self) -> u64 {
        self.file_size
    }
}

impl ChunkReader for Segments {
    type T = bytes::buf::Reader<Bytes>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        let (_, bytes, offset) = self.find(start)?;
        Ok(bytes.slice(offset..).reader())
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        let (range, bytes, offset) = self.find(start)?;
        // Saturating: a length that would carry the end past `u64::MAX`
        // cannot lie inside any held range and is refused like one that
        // merely runs past this range's end.
        let end = start.saturating_add(u64::try_from(length).unwrap_or(u64::MAX));
        if end > range.end {
            return Err(refusal(SegmentsFault::CrossesResidentRange {
                start,
                end,
                held_start: range.start,
                held_end: range.end,
            }));
        }
        Ok(bytes.slice(offset..offset + length))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read as _;

    use arrow::array::{ArrayRef, BinaryArray, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use futures::future::join_all;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStoreExt as _, PutPayload};
    use parquet::arrow::ArrowWriter;
    use parquet::file::FOOTER_SIZE;
    use parquet::file::metadata::FooterTail;
    use parquet::file::properties::WriterProperties;

    use super::*;
    use crate::config::{HedgeConfig, StorageConfig};
    use crate::contract::PAGE_ROWS;

    /// The synthetic prefix the in-memory store is registered under.
    const MEM_PREFIX: &str = "mem://w/";
    /// The fixture file's key under the prefix.
    const FILE_KEY: &str = "db/t/data/f0.parquet";
    /// A key under the prefix that no object is stored at.
    const MISSING_KEY: &str = "db/t/data/missing.parquet";
    /// The fixture's key column.
    const KEY_COLUMN: &str = "id";
    /// The fixture's value column.
    const VALUE_COLUMN: &str = "payload";
    /// The fixture's columns: the key and the value.
    const COLUMNS: usize = 2;
    /// Row groups in the fixture, so each page index has several entries.
    const ROW_GROUPS: usize = 3;
    /// Pages per row group, so a row group has more than one page.
    const PAGES_PER_ROW_GROUP: usize = 2;
    /// Bytes per value: long enough that the file outgrows
    /// `FOOTER_PREFETCH_BYTES`, so one GET proves the prefetch covers the
    /// tail rather than the whole file.
    const VALUE_BYTES: usize = 128;
    /// Concurrent lookups for one page.
    const CONCURRENT: usize = 8;
    /// The percentile and rate that make a one-sample window hedge-ready.
    const TOP: f64 = 1.0;
    /// The memory budget of a test page cache.
    const CACHE_BYTES: usize = 1024 * 1024;
    /// The first held range of the `Segments` fixture.
    const FIRST_HELD: Range<u64> = 10..16;
    /// The bytes `FIRST_HELD` holds.
    const FIRST_BYTES: &[u8] = b"abcdef";
    /// The second held range, after a gap.
    const SECOND_HELD: Range<u64> = 20..23;
    /// The bytes `SECOND_HELD` holds.
    const SECOND_BYTES: &[u8] = b"xyz";
    /// The file the `Segments` fixture stands for.
    const SEGMENTS_FILE_SIZE: u64 = 100;
    /// A byte in the gap between the held ranges.
    const GAP_OFFSET: u64 = 17;
    /// A read that starts inside `FIRST_HELD` and ends past it.
    const STRADDLING: Range<u64> = 14..18;

    fn page_rows() -> usize {
        usize::try_from(PAGE_ROWS).unwrap()
    }

    fn rows_per_group() -> usize {
        page_rows() * PAGES_PER_ROW_GROUP
    }

    /// A Parquet file of `ROW_GROUPS` row groups of `PAGES_PER_ROW_GROUP`
    /// pages each, with a key and a value column, page indexes on, and no
    /// dictionary so every value page is a data page at its offset-index
    /// position.
    fn write_fixture() -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![
            Field::new(KEY_COLUMN, DataType::Int64, false),
            Field::new(VALUE_COLUMN, DataType::Binary, false),
        ]));
        let properties = WriterProperties::builder()
            .set_data_page_row_count_limit(page_rows())
            .set_write_batch_size(page_rows())
            .set_max_row_group_row_count(Some(rows_per_group()))
            .set_dictionary_enabled(false)
            .build();
        let rows = rows_per_group() * ROW_GROUPS;
        let keys = Int64Array::from_iter_values((0..rows).map(|row| i64::try_from(row).unwrap()));
        let values =
            BinaryArray::from_iter_values((0..rows).map(|row| format!("{row:0VALUE_BYTES$}")));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(keys) as ArrayRef, Arc::new(values) as ArrayRef],
        )
        .unwrap();
        let mut out = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut out, schema, Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        out
    }

    fn hedge_config(window: usize) -> HedgeConfig {
        HedgeConfig {
            enabled: true,
            percentile: TOP,
            window,
            max_rate: TOP,
        }
    }

    /// The fixture in an in-memory store behind a fresh storage, opened
    /// with a window built from `config`; returns the file's bytes too, so
    /// a page read is checked against the file itself.
    async fn open_fixture(config: &HedgeConfig) -> (Arc<FileHandle>, Arc<HedgeWindow>, Bytes) {
        let file = Bytes::from(write_fixture());
        let size = u64::try_from(file.len()).unwrap();
        let store = Arc::new(InMemory::new());
        store
            .put(&Path::from(FILE_KEY), PutPayload::from(file.clone()))
            .await
            .unwrap();
        let storage = Arc::new(Storage::new(StorageConfig::default()));
        storage.register(MEM_PREFIX, store);
        let window = Arc::new(HedgeWindow::new(config));
        let handle = FileHandle::open(
            storage,
            &format!("{MEM_PREFIX}{FILE_KEY}"),
            size,
            Arc::clone(&window),
        )
        .unwrap();
        (Arc::new(handle), window, file)
    }

    /// The byte range of the first value page of the first row group.
    fn first_value_page(metadata: &ParquetMetaData) -> Range<u64> {
        let location = &metadata.offset_index().unwrap()[0][1].page_locations()[0];
        let start = u64::try_from(location.offset).unwrap();
        start..start + u64::try_from(location.compressed_page_size).unwrap()
    }

    fn file_slice<'a>(file: &'a Bytes, range: &Range<u64>) -> &'a [u8] {
        &file[usize::try_from(range.start).unwrap()..usize::try_from(range.end).unwrap()]
    }

    fn prefetch_bytes() -> u64 {
        u64::try_from(FOOTER_PREFETCH_BYTES).unwrap()
    }

    /// The thrift metadata's length, read from the file's own footer.
    fn metadata_len(file: &Bytes) -> usize {
        let tail: [u8; FOOTER_SIZE] = file[file.len() - FOOTER_SIZE..].try_into().unwrap();
        FooterTail::try_from(tail).unwrap().metadata_length()
    }

    /// Both page indexes present for every column of every row group, with
    /// `PAGES_PER_ROW_GROUP` locations each.
    fn assert_both_indexes(metadata: &ParquetMetaData) {
        assert_eq!(metadata.num_row_groups(), ROW_GROUPS);
        let offset_index = metadata.offset_index().unwrap();
        let column_index = metadata.column_index().unwrap();
        assert_eq!(offset_index.len(), ROW_GROUPS);
        assert_eq!(column_index.len(), ROW_GROUPS);
        for (offsets, columns) in offset_index.iter().zip(column_index) {
            assert_eq!(offsets.len(), COLUMNS);
            assert_eq!(columns.len(), COLUMNS);
            for column in offsets {
                assert_eq!(column.page_locations().len(), PAGES_PER_ROW_GROUP);
            }
        }
    }

    fn segments() -> Segments {
        Segments::new(
            vec![
                (SECOND_HELD, Bytes::from_static(SECOND_BYTES)),
                (FIRST_HELD, Bytes::from_static(FIRST_BYTES)),
            ],
            SEGMENTS_FILE_SIZE,
        )
    }

    fn range_len(range: &Range<u64>) -> usize {
        usize::try_from(range.end - range.start).unwrap()
    }

    #[tokio::test]
    async fn test_load_metadata_costs_one_get_and_carries_both_indexes() {
        let (handle, _, file) = open_fixture(&HedgeConfig::default()).await;
        let stats = handle.storage().stats();
        assert!(handle.size() > prefetch_bytes());
        assert_eq!(handle.size(), u64::try_from(file.len()).unwrap());

        let metadata = handle.load_metadata().await.unwrap();
        assert_eq!(stats.gets(), 1);
        assert_eq!(stats.bytes(), prefetch_bytes());
        assert_eq!(stats.hedges(), 0);
        assert_both_indexes(&metadata);

        handle.load_metadata().await.unwrap();
        assert_eq!(stats.gets(), 2);
    }

    #[tokio::test]
    async fn test_load_metadata_costs_two_gets_when_the_indexes_start_before_the_tail() {
        let (handle, _, file) = open_fixture(&HedgeConfig::default()).await;
        let stats = handle.storage().stats();
        // The footer and the thrift metadata fit exactly; the page indexes
        // before them do not.
        let footer_and_metadata = FOOTER_SIZE + metadata_len(&file);

        let metadata = handle
            .load_metadata_prefetching(footer_and_metadata)
            .await
            .unwrap();
        assert_eq!(stats.gets(), 2);
        assert_eq!(stats.hedges(), 0);
        assert_both_indexes(&metadata);
    }

    #[tokio::test]
    async fn test_load_metadata_costs_three_gets_when_the_metadata_does_not_fit() {
        let (handle, _, _) = open_fixture(&HedgeConfig::default()).await;
        let stats = handle.storage().stats();

        // A prefetch of the footer's eight bytes alone: the thrift metadata
        // costs a second GET and, with nothing left over, the indexes a
        // third.
        let metadata = handle.load_metadata_prefetching(FOOTER_SIZE).await.unwrap();
        assert_eq!(stats.gets(), 3);
        assert_eq!(stats.hedges(), 0);
        assert_both_indexes(&metadata);
    }

    #[tokio::test]
    async fn test_load_metadata_fetch_failure_is_the_storage_error() {
        let (handle, window, _) = open_fixture(&HedgeConfig::default()).await;
        let stats = handle.storage().stats();
        let missing = Arc::new(
            FileHandle::open(
                Arc::clone(handle.storage()),
                &format!("{MEM_PREFIX}{MISSING_KEY}"),
                handle.size(),
                window,
            )
            .unwrap(),
        );

        let error = missing.load_metadata().await.unwrap_err();
        assert!(matches!(
            error,
            Error::ObjectStore(object_store::Error::NotFound { .. })
        ));
        assert_eq!(stats.gets(), 1);
    }

    #[tokio::test]
    async fn test_read_page_through_cache_costs_one_get_cold_and_none_warm() {
        let (handle, _, file) = open_fixture(&HedgeConfig::default()).await;
        let stats = handle.storage().stats();
        let metadata = handle.load_metadata().await.unwrap();
        let range = first_value_page(&metadata);
        let cache = PageCache::new(CACHE_BYTES, None).await.unwrap();
        let before = stats.gets();

        let cold = handle.read_page(range.clone(), Some(&cache)).await.unwrap();
        assert_eq!(&cold[..], file_slice(&file, &range));
        assert_eq!(stats.gets(), before + 1);
        assert_eq!(cache.fetches(), 1);
        assert_eq!(cache.hits(), 0);

        let warm = handle.read_page(range.clone(), Some(&cache)).await.unwrap();
        assert_eq!(&warm[..], file_slice(&file, &range));
        assert_eq!(stats.gets(), before + 1);
        assert_eq!(cache.fetches(), 1);
        assert_eq!(cache.hits(), 1);
        assert_eq!(stats.hedges(), 0);
    }

    #[tokio::test]
    async fn test_read_page_coalesces_concurrent_lookups_for_one_page() {
        let (handle, _, file) = open_fixture(&HedgeConfig::default()).await;
        let stats = handle.storage().stats();
        let metadata = handle.load_metadata().await.unwrap();
        let range = first_value_page(&metadata);
        let cache = PageCache::new(CACHE_BYTES, None).await.unwrap();
        let before = stats.gets();

        let pages =
            join_all((0..CONCURRENT).map(|_| handle.read_page(range.clone(), Some(&cache)))).await;
        assert_eq!(pages.len(), CONCURRENT);
        for page in pages {
            assert_eq!(&page.unwrap()[..], file_slice(&file, &range));
        }
        assert_eq!(stats.gets(), before + 1);
        assert_eq!(cache.fetches(), 1);
        assert_eq!(cache.hits(), 0);
    }

    #[tokio::test]
    async fn test_read_page_without_cache_costs_one_get_each_time() {
        let (handle, _, file) = open_fixture(&HedgeConfig::default()).await;
        let stats = handle.storage().stats();
        let metadata = handle.load_metadata().await.unwrap();
        let range = first_value_page(&metadata);
        let before = stats.gets();

        let first = handle.read_page(range.clone(), None).await.unwrap();
        assert_eq!(&first[..], file_slice(&file, &range));
        assert_eq!(stats.gets(), before + 1);
        let second = handle.read_page(range.clone(), None).await.unwrap();
        assert_eq!(&second[..], file_slice(&file, &range));
        assert_eq!(stats.gets(), before + 2);
        assert_eq!(stats.hedges(), 0);
    }

    #[tokio::test]
    async fn test_read_range_and_read_ranges_never_move_the_hedge_window() {
        let (handle, window, file) = open_fixture(&hedge_config(1)).await;
        let stats = handle.storage().stats();
        let metadata = handle.load_metadata().await.unwrap();
        assert_eq!(window.delay(), None);
        let range = first_value_page(&metadata);
        let second = &metadata.offset_index().unwrap()[1][1].page_locations()[0];
        let second_start = u64::try_from(second.offset).unwrap();
        let second_range =
            second_start..second_start + u64::try_from(second.compressed_page_size).unwrap();
        let before = stats.gets();

        let page = handle.read_range(range.clone()).await.unwrap();
        assert_eq!(&page[..], file_slice(&file, &range));
        assert_eq!(stats.gets(), before + 1);
        assert_eq!(window.delay(), None);

        let pages = handle
            .read_ranges(&[range.clone(), second_range.clone()])
            .await
            .unwrap();
        assert_eq!(pages.len(), 2);
        assert_eq!(&pages[0][..], file_slice(&file, &range));
        assert_eq!(&pages[1][..], file_slice(&file, &second_range));
        assert_eq!(stats.gets(), before + 3);
        assert_eq!(window.delay(), None);
        assert_eq!(stats.hedges(), 0);

        let looked_up = handle.read_page(range.clone(), None).await.unwrap();
        assert_eq!(&looked_up[..], file_slice(&file, &range));
        assert_eq!(stats.gets(), before + 4);
        assert!(window.delay().is_some());
        assert_eq!(stats.hedges(), 0);
    }

    #[test]
    fn test_get_bytes_and_get_read_serve_bytes_inside_a_held_range() {
        let segments = segments();
        assert_eq!(segments.len(), SEGMENTS_FILE_SIZE);
        assert_eq!(
            segments.resident_len(),
            range_len(&FIRST_HELD) + range_len(&SECOND_HELD)
        );
        assert_eq!(
            &segments.get_bytes(FIRST_HELD.start + 2, 2).unwrap()[..],
            b"cd"
        );
        assert_eq!(
            &segments
                .get_bytes(FIRST_HELD.start, range_len(&FIRST_HELD))
                .unwrap()[..],
            FIRST_BYTES
        );
        let mut rest = Vec::new();
        segments
            .get_read(SECOND_HELD.start + 1)
            .unwrap()
            .read_to_end(&mut rest)
            .unwrap();
        assert_eq!(rest, &SECOND_BYTES[1..]);
    }

    #[test]
    fn test_get_bytes_and_get_read_refuse_a_byte_outside_every_held_range() {
        let segments = segments();
        let error = segments.get_bytes(GAP_OFFSET, 1).unwrap_err();
        assert_eq!(
            Segments::fault(&error),
            Some(&SegmentsFault::NotResident { offset: GAP_OFFSET })
        );
        let error = segments.get_read(FIRST_HELD.end).unwrap_err();
        assert_eq!(
            Segments::fault(&error),
            Some(&SegmentsFault::NotResident {
                offset: FIRST_HELD.end
            })
        );
        let error = segments.get_bytes(0, 1).unwrap_err();
        assert_eq!(
            Segments::fault(&error),
            Some(&SegmentsFault::NotResident { offset: 0 })
        );
    }

    #[test]
    fn test_get_bytes_refuses_a_read_crossing_a_held_range_end() {
        let segments = segments();
        let error = segments
            .get_bytes(STRADDLING.start, range_len(&STRADDLING))
            .unwrap_err();
        assert_eq!(
            Segments::fault(&error),
            Some(&SegmentsFault::CrossesResidentRange {
                start: STRADDLING.start,
                end: STRADDLING.end,
                held_start: FIRST_HELD.start,
                held_end: FIRST_HELD.end,
            })
        );
        // A length that would carry the end past `u64::MAX` is refused the
        // same way, with the end saturated.
        let error = segments
            .get_bytes(FIRST_HELD.start, usize::MAX)
            .unwrap_err();
        assert_eq!(
            Segments::fault(&error),
            Some(&SegmentsFault::CrossesResidentRange {
                start: FIRST_HELD.start,
                end: u64::MAX,
                held_start: FIRST_HELD.start,
                held_end: FIRST_HELD.end,
            })
        );
    }

    #[test]
    fn test_fault_is_none_for_a_foreign_parquet_error() {
        let error = ParquetError::EOF(String::new());
        assert_eq!(Segments::fault(&error), None);
    }
}
