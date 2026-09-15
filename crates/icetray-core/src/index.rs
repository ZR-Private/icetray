//! The per-file resident index (RFD 004, RFD 006, RFD 017): the footer and
//! page indexes as kept, the exact first and last key of the file and of
//! every row group, the fence array (the first key of every key page), and
//! per residency tier the key chunks in memory, a handle on them in a local
//! chunk file, or the key column's bloom filters. A lookup chooses its row
//! group and key page from those exact keys and never from column
//! statistics, decodes exactly one key page with parquet's column reader
//! over bytes it already holds, and fetches at most one value page through
//! the page cache; a miss inside the file's range fetches nothing.

use std::cmp::Ordering;
use std::mem::size_of;
use std::ops::Range;
use std::sync::{Arc, Mutex, PoisonError};

use bytes::Bytes;
use parquet::basic::Type as PhysicalType;
use parquet::bloom_filter::Sbbf;
use parquet::column::page::{Page, PageMetadata, PageReader};
use parquet::column::reader::ColumnReaderImpl;
use parquet::data_type::{
    ByteArray, ByteArrayType, DataType as ParquetDataType, FixedLenByteArrayType, Int64Type,
};
use parquet::file::metadata::{ColumnChunkMetaData, ParquetMetaData};
use parquet::file::page_index::column_index::ColumnIndexMetaData;
use parquet::file::page_index::offset_index::PageLocation;
use parquet::file::serialized_reader::SerializedPageReader;
use parquet::schema::types::{ColumnDescPtr, ColumnDescriptor, SchemaDescriptor};
use tokio::runtime::{Handle, RuntimeFlavor};

use crate::approx;
use crate::cache::PageCache;
use crate::config::Residency;
use crate::contract::MAX_KEY_BYTES;
use crate::error::{Error, RefusalReason, Result};
use crate::key::{Key, KeyType, UUID_BYTES};
use crate::local::{DataFileIdentity, DiskChunks, LocalDir};
use crate::parquet_io::{FileHandle, Segments};

/// A row's position in a file: its row group and its row within that group.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RowRef {
    /// The row group holding the row.
    pub row_group: usize,
    /// The row's index within the row group.
    pub row: i64,
}

/// What makes a data file unservable beyond the contract's refusals. The set
/// is closed; a fault outside it is a library error, not a file fault.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum FileFault {
    /// The table's schema names a column the file does not have.
    #[error("column {0} is not in the file")]
    MissingColumn(String),
    /// The file has no offset index on the key or the value column; page
    /// locations are how a lookup names the one page it decodes.
    #[error("no page index")]
    MissingPageIndex,
    /// The file has no rows, so it has no key range and cannot be routed to.
    #[error("no rows")]
    NoRows,
    /// The footer's row counts disagree with one another: the file's or a
    /// row group's count is negative, or the row groups do not sum to the
    /// file's.
    #[error("the footer's row counts disagree with one another")]
    InconsistentRowCounts,
    /// A column's offset index does not describe its pages in a row group:
    /// a row group with rows has no page, a page has a negative offset or
    /// size or lies outside its column chunk, the first page does not start
    /// at row zero, or the pages' first rows are not strictly increasing
    /// within the row group.
    #[error("the offset index of row group {row_group} does not describe its pages")]
    InconsistentOffsetIndex {
        /// The row group whose offset index is inconsistent.
        row_group: usize,
    },
    /// A data page holds a different number of rows than the offset index
    /// gives it, by its header's count or by what it decodes, so no row
    /// position in that row group can be trusted.
    #[error(
        "page {page} of row group {row_group} holds a different number of rows than its offset index says"
    )]
    PageRowsMismatch {
        /// The row group holding the page.
        row_group: usize,
        /// The page, counted among the column's data pages in the row group.
        page: usize,
    },
    /// A row position lies outside every page of its row group.
    #[error("row {row} is outside every page of row group {row_group}")]
    PageOutOfRange {
        /// The row position that no page covers.
        row: i64,
        /// The row group it was looked for in.
        row_group: usize,
    },
    /// A bloom filter that does not decode from the bytes the footer bounds.
    #[error("bloom filter of row group {row_group} does not decode")]
    CorruptBloomFilter {
        /// The row group whose filter is unreadable.
        row_group: usize,
    },
    /// The footer records where a row group's bloom filter starts but not
    /// how long it is, so the filter cannot be read as one bounded range.
    #[error("bloom filter of row group {row_group} has no length in the footer")]
    UnsizedBloomFilter {
        /// The row group whose filter has no length.
        row_group: usize,
    },
    /// A string key column holds a value that is not UTF-8. A `Key::Str` is
    /// text, and a lossy conversion would serve a different key than the
    /// one written.
    #[error("a key of row group {row_group} is not utf-8")]
    KeyNotUtf8 {
        /// The row group holding the value.
        row_group: usize,
    },
}

/// Every tier's resident bytes for one file, from the footer alone (RFD
/// 017), so the budget check and the table owner see the numbers before any
/// key chunk is fetched. `bloom` and `bloom_bits_per_key` are `None` when a
/// row group lacks a key bloom filter or the footer does not bound one.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ResidentBytes {
    /// Footer, page indexes, key chunks, and fences.
    pub full: u64,
    /// Footer, page indexes, and fences: the memory the disk tier keeps.
    pub disk_memory: u64,
    /// The key chunks: what the disk tier writes to its chunk file.
    pub disk_bytes: u64,
    /// Footer, page indexes, filters, and fences.
    pub bloom: Option<u64>,
    /// Filter bits per row of the file; the bloom tier's miss cost follows it.
    pub bloom_bits_per_key: Option<f64>,
}

/// The page a key would decode and, when it can be named without I/O, the
/// value page its row would fetch; for tests and the doctor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PageRef {
    /// The row group the fences name.
    pub row_group: usize,
    /// The key page within that row group the fences name.
    pub key_page: usize,
    /// The file offset of the value page a hit on `key` would fetch. `Some`
    /// only in the Full tier for a present key, where the key page is
    /// resident and decodes without I/O; `None` for an absent key, and in
    /// the Disk and Bloom tiers, where naming the value page would take a
    /// read.
    pub value_page_offset: Option<u64>,
}

/// A decoded run of keys in file order, one arm per key type, holding the
/// values parquet decoded so that a run of a thousand keys costs no
/// allocation per key. Order and search compare the served order of
/// `Key`: longs numerically, everything else as unsigned bytes.
#[derive(Clone, Debug)]
pub enum Keys {
    /// Long keys.
    Long(Vec<i64>),
    /// String keys as their UTF-8 bytes. `verify` checks every value of the
    /// file at build and `decode_keys` checks what it returns, so `get`
    /// builds a `Key::Str` without a lossy conversion; a lookup's page is
    /// bytes `verify` checked and is only searched, never read back through
    /// `get`. A run built any other way must hold UTF-8 too.
    Str(Vec<ByteArray>),
    /// Binary keys, from a `BYTE_ARRAY` or a `FIXED_LEN_BYTE_ARRAY` column.
    Bin(Vec<ByteArray>),
    /// Uuid keys as the sixteen bytes parquet stores.
    Uuid(Vec<[u8; UUID_BYTES]>),
}

impl Keys {
    /// The key type of the run.
    pub fn key_type(&self) -> KeyType {
        match self {
            Self::Long(_) => KeyType::Long,
            Self::Str(_) => KeyType::String,
            Self::Bin(_) => KeyType::Binary,
            Self::Uuid(_) => KeyType::Uuid,
        }
    }

    /// The number of keys.
    pub fn len(&self) -> usize {
        match self {
            Self::Long(run) => run.len(),
            Self::Str(run) | Self::Bin(run) => run.len(),
            Self::Uuid(run) => run.len(),
        }
    }

    /// Whether the run holds no key.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The key at `at`, which is below `len`.
    pub fn get(&self, at: usize) -> Key {
        match self {
            Self::Long(run) => Key::Long(run[at]),
            Self::Str(run) => {
                Key::Str(String::from_utf8(run[at].data().to_vec()).expect(
                    "a Keys::Str run holds UTF-8: verify and decode_keys check every value",
                ))
            }
            Self::Bin(run) => Key::Bytes(run[at].data().to_vec()),
            Self::Uuid(run) => Key::Uuid(run[at]),
        }
    }

    /// The position of `key`, or where it would be inserted, over a run in
    /// served order. A key of another type than the run's is at position
    /// zero and absent: keys of different types never share a table.
    pub fn binary_search(&self, key: &Key) -> std::result::Result<usize, usize> {
        match (self, key) {
            (Self::Long(run), Key::Long(wanted)) => run.binary_search(wanted),
            (Self::Str(run), Key::Str(wanted)) => {
                run.binary_search_by(|held| held.data().cmp(wanted.as_bytes()))
            }
            (Self::Bin(run), Key::Bytes(wanted)) => {
                run.binary_search_by(|held| held.data().cmp(wanted.as_slice()))
            }
            (Self::Uuid(run), Key::Uuid(wanted)) => run.binary_search(wanted),
            (
                Self::Long(_) | Self::Str(_) | Self::Bin(_) | Self::Uuid(_),
                Key::Long(_) | Key::Str(_) | Key::Bytes(_) | Key::Uuid(_),
            ) => Err(0),
        }
    }

    /// Compares key `at` of this run with key `other_at` of `other` in
    /// served order. Runs of different types never meet within one table;
    /// when they do, the order is `Key`'s derived order across variants,
    /// which is total and carries no meaning.
    pub fn cmp_at(&self, at: usize, other: &Self, other_at: usize) -> Ordering {
        match (self, other) {
            (Self::Long(mine), Self::Long(theirs)) => mine[at].cmp(&theirs[other_at]),
            (Self::Str(mine) | Self::Bin(mine), Self::Str(theirs) | Self::Bin(theirs)) => {
                mine[at].data().cmp(theirs[other_at].data())
            }
            (Self::Uuid(mine), Self::Uuid(theirs)) => mine[at].cmp(&theirs[other_at]),
            (
                Self::Long(_) | Self::Str(_) | Self::Bin(_) | Self::Uuid(_),
                Self::Long(_) | Self::Str(_) | Self::Bin(_) | Self::Uuid(_),
            ) => self.get(at).cmp(&other.get(other_at)),
        }
    }

    /// The length in bytes of the longest key: eight for longs, sixteen for
    /// uuids, the longest value otherwise, and zero for an empty run.
    pub fn max_len(&self) -> usize {
        match self {
            Self::Long(run) => {
                if run.is_empty() {
                    0
                } else {
                    size_of::<i64>()
                }
            }
            Self::Str(run) | Self::Bin(run) => run.iter().map(ByteArray::len).max().unwrap_or(0),
            Self::Uuid(run) => {
                if run.is_empty() {
                    0
                } else {
                    UUID_BYTES
                }
            }
        }
    }

    /// Whether every key is above the one before it: sorted and without a
    /// duplicate.
    pub fn is_strictly_increasing(&self) -> bool {
        self.first_not_increasing().is_none()
    }

    /// The position of the first key that is not above the one before it,
    /// with how it compares to that key: `Equal` for a duplicate, `Less`
    /// for a key out of order. `None` for a strictly increasing run.
    pub fn first_not_increasing(&self) -> Option<(usize, Ordering)> {
        let at = match self {
            Self::Long(run) => run.windows(2).position(|pair| pair[0] >= pair[1]),
            Self::Str(run) | Self::Bin(run) => run
                .windows(2)
                .position(|pair| pair[0].data() >= pair[1].data()),
            Self::Uuid(run) => run.windows(2).position(|pair| pair[0] >= pair[1]),
        }? + 1;
        Some((at, self.cmp_at(at, self, at - 1)))
    }
}

/// How the key column's values decode, fixed at build from the key type and
/// the column's physical type, so every decode matches on these five and
/// none of its arms is impossible.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum KeyShape {
    /// `INT64`.
    Long,
    /// `BYTE_ARRAY` holding UTF-8.
    Str,
    /// `BYTE_ARRAY` holding opaque bytes.
    Bin,
    /// `FIXED_LEN_BYTE_ARRAY` holding opaque bytes.
    FixedBin,
    /// `FIXED_LEN_BYTE_ARRAY` of `UUID_BYTES`.
    Uuid,
}

/// How the value column's values decode, fixed at build.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ValueShape {
    /// `BYTE_ARRAY`.
    Variable,
    /// `FIXED_LEN_BYTE_ARRAY`.
    Fixed,
}

/// What the tier keeps of the key column (RFD 017).
#[derive(Debug)]
enum KeyResidency {
    /// Full: every key column chunk, by file offset.
    Chunks(Arc<Segments>),
    /// Disk: the chunk file holding them; no key bytes in memory.
    Disk(DiskChunks),
    /// Bloom: one filter per row group; no key bytes anywhere local.
    Filters(Vec<Sbbf>),
}

/// The exact first and last key of a row group that has rows, decoded at
/// build. Row groups without rows have no entry.
#[derive(Clone, Debug, PartialEq, Eq)]
struct GroupRange {
    row_group: usize,
    first: Key,
    last: Key,
}

/// Where a lookup's page bytes come from: the store through the page cache
/// (the value page in every tier, the key page in the Bloom tier), or the
/// disk tier's chunk file.
#[derive(Clone, Copy)]
enum PageSource<'a> {
    Store(Option<&'a PageCache>),
    Disk(&'a DiskChunks),
}

/// Everything resident for one data file, built once per generation and
/// shared by every lookup: `Send + Sync`, since a `Table` shares it across
/// its tasks.
#[derive(Debug)]
pub struct FileIndex {
    handle: Arc<FileHandle>,
    /// The footer with the key column's page indexes and every column's
    /// offset index; every other column index is dropped at build.
    metadata: Arc<ParquetMetaData>,
    key_leaf: usize,
    value_leaf: usize,
    key_shape: KeyShape,
    value_shape: ValueShape,
    keys: KeyResidency,
    /// Per row group, the first key of every key page.
    fences: Vec<Vec<Key>>,
    /// Ascending by first key; how a lookup chooses its row group.
    groups: Vec<GroupRange>,
    /// The exact first and last key of the file.
    key_range: (Key, Key),
    max_key_bytes: usize,
    rows: i64,
}

impl FileIndex {
    /// Loads the footer (one GET), checks the file against the table, and
    /// streams the key column chunks once for verification in every tier.
    /// Refuses, typed under `Error::Refused` for `table`: a key column
    /// whose physical type does not match `key_type` (`Binary` accepts
    /// `BYTE_ARRAY` and `FIXED_LEN_BYTE_ARRAY`, `Uuid` a
    /// `FIXED_LEN_BYTE_ARRAY` of `UUID_BYTES`), a value column that is
    /// neither `BYTE_ARRAY` nor `FIXED_LEN_BYTE_ARRAY`, in the Disk tier
    /// the absence of a local directory, in the Bloom tier a row group
    /// without a key bloom filter, then, once the chunks are decoded, a
    /// null key, a key below the one before it or a row group starting
    /// below the previous one's last key, a key repeated, and a key over
    /// `contract::MAX_KEY_BYTES`. A file that is malformed for serving is
    /// `Error::File`: a missing column, no rows, row counts that disagree,
    /// no offset index on either column or one that does not describe its
    /// pages, an unbounded or undecodable bloom filter, a string key that
    /// is not UTF-8, a key page holding other rows than its offset index
    /// says. Footer checks run before any chunk is fetched.
    ///
    /// Full keeps the chunks in memory. Disk reuses a chunk file in `local`
    /// whose identity matches and holds every chunk (reading the chunks
    /// back as counted local reads, no GET) or fetches them, verifies, and
    /// writes them; it keeps no key bytes in memory. Bloom fetches and
    /// verifies the chunks, drops them, and keeps the filters. Every tier
    /// keeps the footer, both page indexes of the key column, every
    /// column's offset index, and the fences; the column index of every
    /// column but the key, the value column's above all, is never resident
    /// (RFD 017).
    pub async fn build(
        table: &str,
        handle: Arc<FileHandle>,
        key_column: &str,
        value_column: &str,
        key_type: KeyType,
        residency: Residency,
        local: Option<&LocalDir>,
    ) -> Result<Self> {
        let loaded = handle.load_metadata().await?;
        let layout = Layout::inspect(table, &handle, &loaded, key_column, value_column, key_type)?;
        let metadata = Arc::new(kept_footer(&loaded, layout.key_leaf));
        drop(loaded);
        let predicted = Self::predicted_resident_bytes(&metadata, layout.key_leaf);
        tracing::debug!(
            path = %handle.location,
            %residency,
            full = predicted.full,
            disk_memory = predicted.disk_memory,
            disk_bytes = predicted.disk_bytes,
            bloom = ?predicted.bloom,
            "predicted resident bytes"
        );
        let column = Column {
            metadata: &metadata,
            leaf: layout.key_leaf,
            path: &handle.location,
        };
        let ranges = key_chunk_ranges(&metadata, layout.key_leaf);
        let (keys, verified) = match residency {
            Residency::Full => {
                let parts = fetch_chunk_parts(&handle, &ranges).await?;
                let chunks = Arc::new(Segments::new(parts, handle.size()));
                let verified = verify(table, &column, layout.key_shape, &chunks)?;
                (KeyResidency::Chunks(chunks), verified)
            }
            Residency::Disk => {
                let local = local.ok_or_else(|| refused(table, RefusalReason::NoLocalDir))?;
                let (disk, verified) =
                    disk_chunks(table, &handle, local, &column, layout.key_shape, &ranges).await?;
                (KeyResidency::Disk(disk), verified)
            }
            Residency::Bloom => {
                let filter_ranges = bloom_ranges(table, &handle, &metadata, layout.key_leaf)?;
                let parts = fetch_chunk_parts(&handle, &ranges).await?;
                let chunks = Arc::new(Segments::new(parts, handle.size()));
                let verified = verify(table, &column, layout.key_shape, &chunks)?;
                drop(chunks);
                let filters =
                    read_bloom_filters(&handle, &metadata, layout.key_leaf, filter_ranges).await?;
                (KeyResidency::Filters(filters), verified)
            }
        };
        let rows = metadata.file_metadata().num_rows();
        Ok(Self {
            handle,
            metadata,
            key_leaf: layout.key_leaf,
            value_leaf: layout.value_leaf,
            key_shape: layout.key_shape,
            value_shape: layout.value_shape,
            keys,
            fences: verified.fences,
            groups: verified.groups,
            key_range: verified.key_range,
            max_key_bytes: verified.max_key_bytes,
            rows,
        })
    }

    /// Every tier's resident bytes for this file, from the footer alone
    /// (RFD 017), the same given the footer as loaded or as `FileIndex`
    /// keeps it (`metadata()`): the column index of every column but the
    /// key is never resident and is not counted either way, so a budget
    /// check at the footer stage predicts what `resident_bytes` will
    /// report. Exact: the footer and the page indexes kept
    /// (`ParquetMetaData::memory_size` of the footer as kept), the key
    /// chunks (the footer's compressed sizes, which Full holds and Disk
    /// writes byte for byte), and the filters (the footer's lengths, header
    /// included; the in-memory filter drops the header, a few bytes per row
    /// group). Estimated: the fences, one `Key` per key page plus, for
    /// `BYTE_ARRAY` keys, the page's minimum from the column index when the
    /// file has one, which a writer may truncate, else the chunk's mean
    /// encoded bytes per value, and for `FIXED_LEN_BYTE_ARRAY` keys the
    /// column's width, which over-counts a uuid key, held inline, by
    /// `UUID_BYTES` per page. `resident_bytes` shares the definitions.
    pub fn predicted_resident_bytes(metadata: &ParquetMetaData, key_leaf: usize) -> ResidentBytes {
        tiers(&ResidentSet::from_footer(metadata, key_leaf, None))
    }

    /// The tier the index was built in.
    pub fn residency(&self) -> Residency {
        match self.keys {
            KeyResidency::Chunks(_) => Residency::Full,
            KeyResidency::Disk(_) => Residency::Disk,
            KeyResidency::Filters(_) => Residency::Bloom,
        }
    }

    /// The file the index describes.
    pub fn handle(&self) -> &Arc<FileHandle> {
        &self.handle
    }

    /// The footer as kept: both page indexes of the key column, every
    /// column's offset index, and no other column's column index.
    pub fn metadata(&self) -> &Arc<ParquetMetaData> {
        &self.metadata
    }

    /// Rows in the file; above zero.
    pub fn rows(&self) -> i64 {
        self.rows
    }

    /// The leaf column index of the key.
    pub fn key_leaf(&self) -> usize {
        self.key_leaf
    }

    /// The leaf column index of the value.
    pub fn value_leaf(&self) -> usize {
        self.value_leaf
    }

    /// The first key of every key page, per row group; a row group without
    /// rows has no fences.
    pub fn fences(&self) -> &[Vec<Key>] {
        &self.fences
    }

    /// Key pages in `row_group`, which is below the footer's row group count.
    pub fn page_count(&self, row_group: usize) -> usize {
        self.fences[row_group].len()
    }

    /// The rows key page `page` of `row_group` holds, within the row group;
    /// `page` is below `page_count(row_group)`.
    pub fn page_rows(&self, row_group: usize, page: usize) -> Range<i64> {
        self.key_column().page_rows(row_group, page)
    }

    /// The length in bytes of the file's longest key; at most
    /// `contract::MAX_KEY_BYTES`.
    pub fn max_key_bytes(&self) -> usize {
        self.max_key_bytes
    }

    /// The exact first and last key of the file, decoded at build; never a
    /// truncated bound.
    pub fn key_range(&self) -> (&Key, &Key) {
        (&self.key_range.0, &self.key_range.1)
    }

    /// Whether `key` lies within the file's exact key range.
    pub fn may_contain(&self, key: &Key) -> bool {
        let (first, last) = self.key_range();
        first <= key && key <= last
    }

    /// Bytes held in memory for this file: the footer and page indexes as
    /// kept, the tier's structure (chunks, filters, or nothing for Disk),
    /// and the fences, under the definitions of `predicted_resident_bytes`
    /// with the fences exact.
    pub fn resident_bytes(&self) -> usize {
        let all = tiers(&ResidentSet::from_footer(
            &self.metadata,
            self.key_leaf,
            Some(&self.fences),
        ));
        let mine = match self.keys {
            KeyResidency::Chunks(_) => all.full,
            KeyResidency::Disk(_) => all.disk_memory,
            KeyResidency::Filters(_) => all
                .bloom
                .expect("a bloom index has a bounded filter per row group; build refused the file otherwise"),
        };
        usize::try_from(mine)
            .expect("resident bytes fit usize on the 64-bit targets the crate supports")
    }

    /// The exact position of `key`, or `None` for a key the file does not
    /// hold, including one between two of its rows. Row group and key page
    /// come from the exact keys decoded at build, never from statistics.
    /// Full decodes the one resident key page and does no I/O. Disk reads
    /// that page from the chunk file: one local read, no GET (one more
    /// local read for the chunk's dictionary page when the column has one).
    /// Bloom asks the row group's filter first: a negative is `None` with no
    /// I/O; a positive fetches the one key page the fences name through
    /// `cache`, one GET, so a false positive costs exactly that (and a
    /// dictionary-encoded key column its dictionary page besides). A key
    /// outside the file's range costs nothing in every tier.
    pub async fn find(&self, key: &Key, cache: Option<&PageCache>) -> Result<Option<RowRef>> {
        let Some((row_group, page)) = self.locate(key) else {
            return Ok(None);
        };
        let segments = match &self.keys {
            KeyResidency::Chunks(chunks) => Arc::clone(chunks),
            KeyResidency::Disk(disk) => {
                self.fetch_pages(&self.key_column(), row_group, page, PageSource::Disk(disk))
                    .await?
            }
            KeyResidency::Filters(filters) => {
                if !filters[row_group].check(&*key.as_bytes()) {
                    return Ok(None);
                }
                self.fetch_pages(
                    &self.key_column(),
                    row_group,
                    page,
                    PageSource::Store(cache),
                )
                .await?
            }
        };
        self.search(key, row_group, page, segments)
    }

    /// The value at `row`: the one value page holding it through `cache`
    /// (one GET cold, none warm) and, when the value column chunk has a
    /// dictionary page, that page too (the one extra GET RFD 007 prices),
    /// then exactly one row decoded. `Some` carries the bytes unchanged;
    /// `None` is a null value, distinct from an absent key. A row outside
    /// every page of its row group is `FileFault::PageOutOfRange`.
    pub async fn read_value(
        &self,
        row: RowRef,
        cache: Option<&PageCache>,
    ) -> Result<Option<Bytes>> {
        let page = self.value_page(row)?;
        let column = self.value_column();
        let segments = self
            .fetch_pages(&column, row.row_group, page, PageSource::Store(cache))
            .await?;
        let value = match self.value_shape {
            ValueShape::Variable => read_one::<ByteArrayType>(&column, row, segments)?
                .map(|value| Bytes::copy_from_slice(value.data())),
            ValueShape::Fixed => read_one::<FixedLenByteArrayType>(&column, row, segments)?
                .map(|value| Bytes::copy_from_slice(value.data())),
        };
        Ok(value)
    }

    /// The key page `find` would decode for `key` and, in the Full tier for
    /// a present key, the value page `read_value` would fetch. `None` when
    /// `find` would decode nothing: the key is outside the file's range,
    /// between two row groups, or ruled out by the Bloom tier's filter.
    /// Synchronous and free of I/O in every tier, so `Generation::plan` can
    /// predict from it the GET a lookup issues for the key page.
    pub fn page_of(&self, key: &Key) -> Option<PageRef> {
        let (row_group, key_page) = self.locate(key)?;
        if let KeyResidency::Filters(filters) = &self.keys
            && !filters[row_group].check(&*key.as_bytes())
        {
            return None;
        }
        let value_page_offset = match &self.keys {
            KeyResidency::Chunks(chunks) => self
                .search(key, row_group, key_page, Arc::clone(chunks))
                .expect("build decoded this key page from these resident bytes and checked it against its offset index; decoding it again cannot fail")
                .and_then(|row| self.value_page_index(row))
                .map(|page| self.value_column().page_range(row_group, page).start),
            KeyResidency::Disk(_) | KeyResidency::Filters(_) => None,
        };
        Some(PageRef {
            row_group,
            key_page,
            value_page_offset,
        })
    }

    /// The key column of `row_group` decoded from `chunks`, or one page of
    /// it; no I/O, and a byte not in `chunks` is a `Segments` fault inside
    /// `Error::Parquet`. `row_group` is below the footer's row group count
    /// and `page` below `page_count(row_group)`.
    pub fn decode_keys(
        &self,
        row_group: usize,
        page: Option<usize>,
        chunks: &Segments,
    ) -> Result<Keys> {
        let column = self.key_column();
        let rows = match page {
            Some(page) => column.page_rows(row_group, page),
            None => 0..column.rows(row_group),
        };
        let keys = decode_run(
            &column,
            self.key_shape,
            row_group,
            rows,
            Arc::new(chunks.clone()),
        )?;
        check_utf8(&column, row_group, &keys)?;
        Ok(keys)
    }

    /// The key column chunks for overlap verification, which decodes every
    /// key of the file: Full hands out the resident chunks at no cost; Disk
    /// reads every chunk back from the chunk file, one counted local read
    /// per row group and no GET; Bloom holds no key bytes and fetches them
    /// again from storage, one counted load GET per row group.
    pub async fn key_chunks_for_verification(&self) -> Result<Arc<Segments>> {
        match &self.keys {
            KeyResidency::Chunks(chunks) => Ok(Arc::clone(chunks)),
            KeyResidency::Disk(disk) => {
                let ranges = key_chunk_ranges(&self.metadata, self.key_leaf);
                chunks_from_disk(disk, &ranges, self.handle.size()).await
            }
            KeyResidency::Filters(_) => {
                let ranges = key_chunk_ranges(&self.metadata, self.key_leaf);
                let parts = fetch_chunk_parts(&self.handle, &ranges).await?;
                Ok(Arc::new(Segments::new(parts, self.handle.size())))
            }
        }
    }

    fn key_column(&self) -> Column<'_> {
        Column {
            metadata: &self.metadata,
            leaf: self.key_leaf,
            path: &self.handle.location,
        }
    }

    fn value_column(&self) -> Column<'_> {
        Column {
            metadata: &self.metadata,
            leaf: self.value_leaf,
            path: &self.handle.location,
        }
    }

    /// The row group and key page that can hold `key`, from the exact keys
    /// alone: the last row group whose first key is at or below `key`,
    /// provided its last key is at or above it, then the last page of that
    /// row group whose first key is at or below `key`. `None` for a key
    /// outside the file's range or between two row groups.
    fn locate(&self, key: &Key) -> Option<(usize, usize)> {
        if !self.may_contain(key) {
            return None;
        }
        let at = self.groups.partition_point(|group| group.first <= *key);
        let group = self.groups.get(at.checked_sub(1)?)?;
        if group.last < *key {
            return None;
        }
        let fences = &self.fences[group.row_group];
        let page = fences
            .partition_point(|fence| fence <= key)
            .checked_sub(1)?;
        Some((group.row_group, page))
    }

    /// Decodes key page `page` of `row_group` from `segments` and finds
    /// `key` in it. The page's bytes are ones `verify` checked at build, so
    /// they are searched as decoded and not checked as UTF-8 again.
    fn search(
        &self,
        key: &Key,
        row_group: usize,
        page: usize,
        segments: Arc<Segments>,
    ) -> Result<Option<RowRef>> {
        let column = self.key_column();
        let rows = column.page_rows(row_group, page);
        let keys = decode_run(&column, self.key_shape, row_group, rows.clone(), segments)?;
        Ok(keys.binary_search(key).ok().map(|at| RowRef {
            row_group,
            row: rows.start + row_offset(at),
        }))
    }

    /// The value page holding `row`, or `None` for a row outside its row
    /// group or a row group the file does not have.
    fn value_page_index(&self, row: RowRef) -> Option<usize> {
        let rows = self.metadata.row_groups().get(row.row_group)?.num_rows();
        if row.row < 0 || row.row >= rows {
            return None;
        }
        self.value_column()
            .page_locations(row.row_group)
            .partition_point(|location| location.first_row_index <= row.row)
            .checked_sub(1)
    }

    /// The value page holding `row`, or `FileFault::PageOutOfRange`.
    fn value_page(&self, row: RowRef) -> Result<usize> {
        self.value_page_index(row).ok_or_else(|| {
            file_fault(
                &self.handle.location,
                FileFault::PageOutOfRange {
                    row: row.row,
                    row_group: row.row_group,
                },
            )
        })
    }

    /// Page `page` of `column` in `row_group` and, when the column chunk has
    /// a dictionary page, that page too, from `source`, as the `Segments`
    /// the column reader decodes from without further I/O. The dictionary
    /// page is found the way the reader finds it, as the gap between the
    /// chunk's start and its first data page, so the reader asks for exactly
    /// what is held. Both ranges are known before either read starts, so
    /// the two reads run together: the dictionary costs its GET or local
    /// read, never a second round trip's wait.
    async fn fetch_pages(
        &self,
        column: &Column<'_>,
        row_group: usize,
        page: usize,
        source: PageSource<'_>,
    ) -> Result<Arc<Segments>> {
        let range = column.page_range(row_group, page);
        let dictionary = column.dictionary_range(row_group);
        let read_dictionary = async {
            match dictionary.clone() {
                Some(dictionary) => self.read_from(source, dictionary).await.map(Some),
                None => Ok(None),
            }
        };
        let (page_bytes, dictionary_bytes) =
            futures::try_join!(self.read_from(source, range.clone()), read_dictionary)?;
        let mut parts = Vec::with_capacity(2);
        parts.push((range, page_bytes));
        if let (Some(dictionary), Some(bytes)) = (dictionary, dictionary_bytes) {
            parts.push((dictionary, bytes));
        }
        Ok(Arc::new(Segments::new(parts, self.handle.size())))
    }

    /// One byte range of the file from `source`: a hedged lookup GET through
    /// the cache, or one counted local read.
    async fn read_from(&self, source: PageSource<'_>, range: Range<u64>) -> Result<Bytes> {
        match source {
            PageSource::Store(cache) => self.handle.read_page(range, cache).await,
            PageSource::Disk(disk) => disk.page(range).await,
        }
    }
}

/// One column of the file as the decoders see it: the footer, the leaf,
/// and the path errors name.
#[derive(Clone, Copy)]
struct Column<'a> {
    metadata: &'a ParquetMetaData,
    leaf: usize,
    path: &'a str,
}

impl Column<'_> {
    fn chunk(&self, row_group: usize) -> &ColumnChunkMetaData {
        self.metadata.row_group(row_group).column(self.leaf)
    }

    fn rows(&self, row_group: usize) -> i64 {
        self.metadata.row_group(row_group).num_rows()
    }

    fn descriptor(&self) -> ColumnDescPtr {
        self.metadata
            .file_metadata()
            .schema_descr()
            .column(self.leaf)
    }

    /// The column's page locations in `row_group`; build required them.
    fn page_locations(&self, row_group: usize) -> &[PageLocation] {
        self.metadata
            .offset_index()
            .expect("build required an offset index on the key and value columns")[row_group]
            [self.leaf]
            .page_locations()
    }

    fn page_range(&self, row_group: usize, page: usize) -> Range<u64> {
        page_range(&self.page_locations(row_group)[page])
    }

    /// The dictionary page's range when the chunk has one: the bytes between
    /// the chunk's start and its first data page.
    fn dictionary_range(&self, row_group: usize) -> Option<Range<u64>> {
        let (start, _) = self.chunk(row_group).byte_range();
        let first = page_range(self.page_locations(row_group).first()?).start;
        (first != start).then_some(start..first)
    }

    /// The rows page `page` holds, within the row group.
    fn page_rows(&self, row_group: usize, page: usize) -> Range<i64> {
        let locations = self.page_locations(row_group);
        let start = locations[page].first_row_index;
        let end = locations
            .get(page + 1)
            .map_or_else(|| self.rows(row_group), |next| next.first_row_index);
        start..end
    }

    /// A page reader over `segments` for the column chunk, positioned at its
    /// start and given every page location, so skipping a page reads no
    /// byte of it.
    fn page_reader(
        &self,
        row_group: usize,
        segments: Arc<Segments>,
    ) -> Result<SerializedPageReader<Segments>> {
        let locations = self.page_locations(row_group).to_vec();
        let total_rows = row_index(self.rows(row_group));
        Ok(SerializedPageReader::new(
            segments,
            self.chunk(row_group),
            total_rows,
            Some(locations),
        )?)
    }

    /// The page of `row_group` holding row `row`, which is inside the row
    /// group; the last page for a row at or past its end.
    fn page_holding(&self, row_group: usize, row: usize) -> usize {
        let locations = self.page_locations(row_group);
        locations
            .partition_point(|location| row_index(location.first_row_index) <= row)
            .saturating_sub(1)
            .min(locations.len().saturating_sub(1))
    }

    /// Whether the data pages a reader took from `row_group` are the ones
    /// the offset index names for rows `rows`, each holding the rows the
    /// index gives it: `seen` is the value count in the header of every
    /// data page the reader read, in order, starting at the page holding
    /// the first row (the pages before it were skipped from their
    /// locations, unread), and `decoded` is how many rows it produced. No
    /// rows wanted means no page read. A disagreement is
    /// `FileFault::PageRowsMismatch` naming the first page that does not
    /// fit.
    fn check_pages_read(
        &self,
        row_group: usize,
        rows: Range<usize>,
        seen: &[usize],
        decoded: usize,
    ) -> Result<()> {
        let (first, expected) = if rows.is_empty() {
            (0, 0)
        } else {
            let first = self.page_holding(row_group, rows.start);
            let last = self.page_holding(row_group, rows.end - 1);
            (first, last - first + 1)
        };
        let mismatch =
            |page| file_fault(self.path, FileFault::PageRowsMismatch { row_group, page });
        for (page, values) in (first..first + expected).zip(seen) {
            let span = self.page_rows(row_group, page);
            if *values != row_index(span.end - span.start) {
                return Err(mismatch(page));
            }
        }
        if seen.len() != expected {
            return Err(mismatch(first + seen.len().min(expected.saturating_sub(1))));
        }
        if decoded != rows.end - rows.start {
            return Err(mismatch(self.page_holding(row_group, rows.start + decoded)));
        }
        Ok(())
    }
}

/// A page reader that records the value count in the header of every data
/// page it hands the column reader, so that what a decode produced can be
/// checked against the rows the offset index gives those pages. The
/// dictionary page is handed through uncounted; the pages the column reader
/// skips from their locations are never read and never seen.
struct CountedPages {
    inner: SerializedPageReader<Segments>,
    seen: Arc<Mutex<Vec<usize>>>,
}

impl Iterator for CountedPages {
    type Item = parquet::errors::Result<Page>;

    fn next(&mut self) -> Option<Self::Item> {
        self.get_next_page().transpose()
    }
}

impl PageReader for CountedPages {
    fn get_next_page(&mut self) -> parquet::errors::Result<Option<Page>> {
        let page = self.inner.get_next_page()?;
        match &page {
            Some(Page::DataPage { num_values, .. } | Page::DataPageV2 { num_values, .. }) => {
                let values = usize::try_from(*num_values)
                    .expect("a page's value count fits usize on the supported systems");
                self.seen
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(values);
            }
            Some(Page::DictionaryPage { .. }) | None => {}
        }
        Ok(page)
    }

    fn peek_next_page(&mut self) -> parquet::errors::Result<Option<PageMetadata>> {
        self.inner.peek_next_page()
    }

    fn skip_next_page(&mut self) -> parquet::errors::Result<()> {
        self.inner.skip_next_page()
    }

    fn at_record_boundary(&mut self) -> parquet::errors::Result<bool> {
        self.inner.at_record_boundary()
    }
}

/// The key and value leaves of a file and how each decodes, from the footer.
struct Layout {
    key_leaf: usize,
    value_leaf: usize,
    key_shape: KeyShape,
    value_shape: ValueShape,
}

impl Layout {
    /// The checks `build` makes on the footer, in order: both columns
    /// exist, the key's physical type matches the key type, the value's is
    /// a byte array, the file has rows and its row counts agree, both
    /// columns have an offset index that describes their pages.
    fn inspect(
        table: &str,
        handle: &FileHandle,
        metadata: &ParquetMetaData,
        key_column: &str,
        value_column: &str,
        key_type: KeyType,
    ) -> Result<Self> {
        let path = handle.location.as_str();
        let schema = metadata.file_metadata().schema_descr();
        let key_leaf = leaf(path, schema, key_column)?;
        let value_leaf = leaf(path, schema, value_column)?;
        let key_shape = key_shape(table, path, key_type, &schema.column(key_leaf))?;
        let value_shape = value_shape(table, path, &schema.column(value_leaf))?;
        if metadata.file_metadata().num_rows() == 0 {
            return Err(file_fault(path, FileFault::NoRows));
        }
        check_row_counts(path, metadata)?;
        check_offset_index(path, metadata, key_leaf)?;
        check_offset_index(path, metadata, value_leaf)?;
        Ok(Self {
            key_leaf,
            value_leaf,
            key_shape,
            value_shape,
        })
    }
}

/// The leaf index of the top-level column `name`, which must itself be a
/// leaf.
fn leaf(path: &str, schema: &SchemaDescriptor, name: &str) -> Result<usize> {
    schema
        .columns()
        .iter()
        .position(|column| matches!(column.path().parts(), [part] if part == name))
        .ok_or_else(|| file_fault(path, FileFault::MissingColumn(name.to_string())))
}

/// How the key column decodes, or `RefusalReason::KeyTypeMismatch` when its
/// physical type is not the one `key_type` maps to.
fn key_shape(
    table: &str,
    path: &str,
    key_type: KeyType,
    descr: &ColumnDescriptor,
) -> Result<KeyShape> {
    let physical = descr.physical_type();
    let uuid_width = usize::try_from(descr.type_length()).is_ok_and(|len| len == UUID_BYTES);
    let shape = match (key_type, physical) {
        (KeyType::Long, PhysicalType::INT64) => Some(KeyShape::Long),
        (KeyType::String, PhysicalType::BYTE_ARRAY) => Some(KeyShape::Str),
        (KeyType::Binary, PhysicalType::BYTE_ARRAY) => Some(KeyShape::Bin),
        (KeyType::Binary, PhysicalType::FIXED_LEN_BYTE_ARRAY) => Some(KeyShape::FixedBin),
        (KeyType::Uuid, PhysicalType::FIXED_LEN_BYTE_ARRAY) if uuid_width => Some(KeyShape::Uuid),
        (
            KeyType::Long | KeyType::String | KeyType::Binary | KeyType::Uuid,
            PhysicalType::BOOLEAN
            | PhysicalType::INT32
            | PhysicalType::INT64
            | PhysicalType::INT96
            | PhysicalType::FLOAT
            | PhysicalType::DOUBLE
            | PhysicalType::BYTE_ARRAY
            | PhysicalType::FIXED_LEN_BYTE_ARRAY,
        ) => None,
    };
    shape.ok_or_else(|| {
        refused(
            table,
            RefusalReason::KeyTypeMismatch {
                path: path.to_string(),
                expected: key_type,
                got: physical,
            },
        )
    })
}

/// How the value column decodes, or `RefusalReason::ValueTypeUnsupported`
/// when it is not a byte array of either width.
fn value_shape(table: &str, path: &str, descr: &ColumnDescriptor) -> Result<ValueShape> {
    let physical = descr.physical_type();
    match physical {
        PhysicalType::BYTE_ARRAY => Ok(ValueShape::Variable),
        PhysicalType::FIXED_LEN_BYTE_ARRAY => Ok(ValueShape::Fixed),
        PhysicalType::BOOLEAN
        | PhysicalType::INT32
        | PhysicalType::INT64
        | PhysicalType::INT96
        | PhysicalType::FLOAT
        | PhysicalType::DOUBLE => Err(refused(
            table,
            RefusalReason::ValueTypeUnsupported {
                path: path.to_string(),
                got: physical,
            },
        )),
    }
}

/// The footer's row counts, checked once so that the lookup path converts
/// its numbers with no fallible step: the file's count and every row
/// group's are non-negative and the row groups sum to the file's, as the
/// format requires. A footer that disagrees with itself is
/// `FileFault::InconsistentRowCounts`.
fn check_row_counts(path: &str, metadata: &ParquetMetaData) -> Result<()> {
    let rows = metadata.file_metadata().num_rows();
    let group_rows = metadata.row_groups().iter().try_fold(0i64, |sum, group| {
        let rows = group.num_rows();
        (rows >= 0).then(|| sum.checked_add(rows)).flatten()
    });
    if rows < 0 || group_rows != Some(rows) {
        return Err(file_fault(path, FileFault::InconsistentRowCounts));
    }
    Ok(())
}

/// The offset index of `leaf` in every row group, checked once so that the
/// lookup path converts its numbers with no fallible step: an index is
/// present, every page has a non-negative offset and size and lies inside
/// its column chunk, the first page is at row zero, first rows are strictly
/// increasing and inside the row group, and a row group with rows has a
/// page. A footer without the index is `FileFault::MissingPageIndex`; one
/// whose index fails a check is `FileFault::InconsistentOffsetIndex`.
fn check_offset_index(path: &str, metadata: &ParquetMetaData, leaf: usize) -> Result<()> {
    let offset_index = metadata
        .offset_index()
        .ok_or_else(|| file_fault(path, FileFault::MissingPageIndex))?;
    for (row_group, group) in metadata.row_groups().iter().enumerate() {
        let inconsistent = || file_fault(path, FileFault::InconsistentOffsetIndex { row_group });
        let rows = group.num_rows();
        let locations = offset_index
            .get(row_group)
            .and_then(|columns| columns.get(leaf))
            .ok_or_else(|| file_fault(path, FileFault::MissingPageIndex))?
            .page_locations();
        if rows > 0 && locations.is_empty() {
            return Err(inconsistent());
        }
        let (chunk_start, chunk_len) = group.column(leaf).byte_range();
        let chunk_end = chunk_start.saturating_add(chunk_len);
        let mut previous: Option<i64> = None;
        for location in locations {
            let in_chunk = u64::try_from(location.offset).is_ok_and(|start| {
                u64::try_from(location.compressed_page_size)
                    .is_ok_and(|len| start >= chunk_start && start.saturating_add(len) <= chunk_end)
            });
            let in_group = location.first_row_index >= 0 && location.first_row_index < rows;
            let in_order = previous.map_or(location.first_row_index == 0, |first| {
                first < location.first_row_index
            });
            if !in_chunk || !in_group || !in_order {
                return Err(inconsistent());
            }
            previous = Some(location.first_row_index);
        }
    }
    Ok(())
}

/// The footer as the index keeps it: a copy of `metadata` with the column
/// index of every column but the key replaced by `NONE` in every row group,
/// so the page minimums and maximums of the value column, which the reader
/// never consults and which dominate the index of a large-value table, are
/// not resident (RFD 017). The key column keeps both indexes and every
/// column its offset index. Always a copy, never the footer moved: the
/// bytes `ParquetMetaData::memory_size` reports follow each vector's
/// capacity, which a copy sets from its contents alone, so the footer as
/// kept and a copy of the footer as loaded measure the same and
/// `predicted_resident_bytes` is exact from either.
fn kept_footer(metadata: &ParquetMetaData, key_leaf: usize) -> ParquetMetaData {
    let mut builder = metadata.clone().into_builder();
    if let Some(mut column_index) = builder.take_column_index() {
        for row_group in &mut column_index {
            for (leaf, entry) in row_group.iter_mut().enumerate() {
                if leaf != key_leaf {
                    *entry = ColumnIndexMetaData::NONE;
                }
            }
        }
        builder = builder.set_column_index(Some(column_index));
    }
    builder.build()
}

/// The byte range of the key column chunk of every row group, in row group
/// order.
fn key_chunk_ranges(metadata: &ParquetMetaData, key_leaf: usize) -> Vec<Range<u64>> {
    metadata
        .row_groups()
        .iter()
        .map(|group| {
            let (start, len) = group.column(key_leaf).byte_range();
            start..start + len
        })
        .collect()
}

/// Every key column chunk in one `read_ranges` call: one load GET per row
/// group. Each chunk is copied out of the buffer the store returned: a
/// store may serve neighbouring ranges from one fetch and hand back slices
/// of it, and a slice keeps the whole fetch alive, so the bytes held would
/// exceed the chunks by whatever lay between them. The copy is paid once at
/// load.
async fn fetch_chunk_parts(
    handle: &FileHandle,
    ranges: &[Range<u64>],
) -> Result<Vec<(Range<u64>, Bytes)>> {
    let chunks = handle.read_ranges(ranges).await?;
    Ok(ranges
        .iter()
        .cloned()
        .zip(chunks)
        .map(|(range, bytes)| (range, Bytes::copy_from_slice(&bytes)))
        .collect())
}

/// Every key column chunk read back from the disk tier's chunk file: one
/// counted local read per row group, no GET.
async fn chunks_from_disk(
    disk: &DiskChunks,
    ranges: &[Range<u64>],
    file_size: u64,
) -> Result<Arc<Segments>> {
    let mut parts = Vec::with_capacity(ranges.len());
    for range in ranges {
        parts.push((range.clone(), disk.page(range.clone()).await?));
    }
    Ok(Arc::new(Segments::new(parts, file_size)))
}

/// The disk tier's key chunks: a chunk file in `local` whose identity
/// (path, size, footer digest) matches and holds every chunk range is read
/// back and verified, no GET; otherwise the chunks are fetched, verified,
/// and written, replacing any file at that name.
async fn disk_chunks(
    table: &str,
    handle: &FileHandle,
    local: &LocalDir,
    column: &Column<'_>,
    shape: KeyShape,
    ranges: &[Range<u64>],
) -> Result<(DiskChunks, Verified)> {
    let identity = DataFileIdentity {
        path: handle.location.clone(),
        size: handle.size(),
        footer_digest: handle
            .footer_digest()
            .expect("load_metadata set the footer digest before build read the footer"),
    };
    if let Some(disk) = local.chunks_for(&identity)?
        && ranges.iter().all(|range| disk.holds(range))
    {
        let chunks = chunks_from_disk(&disk, ranges, handle.size()).await?;
        let verified = verify(table, column, shape, &chunks)?;
        return Ok((disk, verified));
    }
    let parts = fetch_chunk_parts(handle, ranges).await?;
    let chunks = Arc::new(Segments::new(parts.clone(), handle.size()));
    let verified = verify(table, column, shape, &chunks)?;
    drop(chunks);
    let disk = write_chunks_blocking(local, &identity, parts)?;
    Ok((disk, verified))
}

/// Writes a chunk file off the async worker. `LocalDir` arrives borrowed,
/// which rules out `spawn_blocking` and its `'static` bound, so on the
/// multi-thread runtime the worker steps aside with `block_in_place`; the
/// current-thread runtime has no other worker to hand the write to and runs
/// it inline.
fn write_chunks_blocking(
    local: &LocalDir,
    identity: &DataFileIdentity,
    parts: Vec<(Range<u64>, Bytes)>,
) -> Result<DiskChunks> {
    let write = || local.write_chunks(identity, parts.into_iter());
    if Handle::current().runtime_flavor() == RuntimeFlavor::MultiThread {
        tokio::task::block_in_place(write)
    } else {
        write()
    }
}

/// The byte range of the key column's bloom filter in every row group. The
/// Bloom tier stands the filter in for the key chunk, so a row group without
/// one is `RefusalReason::NoBloomFilter`, and one whose filter the footer
/// does not bound is `FileFault::UnsizedBloomFilter`.
fn bloom_ranges(
    table: &str,
    handle: &FileHandle,
    metadata: &ParquetMetaData,
    key_leaf: usize,
) -> Result<Vec<Range<u64>>> {
    let path = handle.location.as_str();
    metadata
        .row_groups()
        .iter()
        .enumerate()
        .map(|(row_group, group)| {
            let column = group.column(key_leaf);
            let offset = column
                .bloom_filter_offset()
                .and_then(|offset| u64::try_from(offset).ok())
                .ok_or_else(|| {
                    refused(
                        table,
                        RefusalReason::NoBloomFilter {
                            path: path.to_string(),
                            row_group,
                        },
                    )
                })?;
            let len = column
                .bloom_filter_length()
                .and_then(|len| u64::try_from(len).ok())
                .ok_or_else(|| file_fault(path, FileFault::UnsizedBloomFilter { row_group }))?;
            Ok(offset..offset + len)
        })
        .collect()
}

/// The key column's bloom filter of every row group, one `read_ranges` call
/// (one load GET per row group), each parsed from exactly the bytes the
/// footer bounds; a filter that does not decode from them is
/// `FileFault::CorruptBloomFilter`.
async fn read_bloom_filters(
    handle: &FileHandle,
    metadata: &ParquetMetaData,
    key_leaf: usize,
    ranges: Vec<Range<u64>>,
) -> Result<Vec<Sbbf>> {
    let bytes = handle.read_ranges(&ranges).await?;
    ranges
        .into_iter()
        .zip(bytes)
        .enumerate()
        .map(|(row_group, (range, bytes))| {
            let segments = Segments::new(vec![(range, bytes)], handle.size());
            let column = metadata.row_group(row_group).column(key_leaf);
            match Sbbf::read_from_column_chunk(column, &segments) {
                Ok(Some(filter)) => Ok(filter),
                Ok(None) => Err(file_fault(
                    &handle.location,
                    FileFault::CorruptBloomFilter { row_group },
                )),
                Err(error) => {
                    tracing::debug!(%error, path = %handle.location, row_group, "bloom filter does not decode");
                    Err(file_fault(
                        &handle.location,
                        FileFault::CorruptBloomFilter { row_group },
                    ))
                }
            }
        })
        .collect()
}

/// What verifying the key column establishes.
struct Verified {
    fences: Vec<Vec<Key>>,
    groups: Vec<GroupRange>,
    key_range: (Key, Key),
    max_key_bytes: usize,
}

/// Decodes every key of every row group from `chunks` and checks what the
/// contract requires of them (RFD 007): no null, UTF-8 in a string column,
/// every key above the one before it within a row group and across row
/// groups (a key equal to the one before it is a duplicate, a key below
/// it is unsorted), none over `contract::MAX_KEY_BYTES`. Every key page is
/// checked against the rows its offset index gives it on the way. Records
/// the exact first and last key of the file and of every row group, the
/// longest key, and the fences. Nothing here consults column statistics.
fn verify(
    table: &str,
    column: &Column<'_>,
    shape: KeyShape,
    chunks: &Arc<Segments>,
) -> Result<Verified> {
    let path = column.path;
    let total = column.metadata.num_row_groups();
    let mut fences = Vec::with_capacity(total);
    let mut groups: Vec<GroupRange> = Vec::with_capacity(total);
    let mut longest = 0;
    for row_group in 0..total {
        let rows = column.rows(row_group);
        let keys = decode_run(column, shape, row_group, 0..rows, Arc::clone(chunks))?;
        if keys.len() < row_index(rows) {
            let reason = RefusalReason::NullKeys {
                path: path.to_string(),
                row_group,
            };
            return Err(refused(table, reason));
        }
        check_utf8(column, row_group, &keys)?;
        longest = longest.max(keys.max_len());
        if keys.is_empty() {
            fences.push(Vec::new());
            continue;
        }
        if let Some((at, order)) = keys.first_not_increasing() {
            let reason = not_above_previous(path, row_group, order, keys.get(at));
            return Err(refused(table, reason));
        }
        let first = keys.get(0);
        let last = keys.get(keys.len() - 1);
        if let Some(previous) = groups.last() {
            let order = first.cmp(&previous.last);
            if order != Ordering::Greater {
                return Err(refused(
                    table,
                    not_above_previous(path, row_group, order, first),
                ));
            }
        }
        fences.push(
            column
                .page_locations(row_group)
                .iter()
                .map(|location| keys.get(row_index(location.first_row_index)))
                .collect(),
        );
        groups.push(GroupRange {
            row_group,
            first,
            last,
        });
    }
    if longest > MAX_KEY_BYTES {
        let reason = RefusalReason::KeyTooLong {
            path: path.to_string(),
            bytes: longest,
        };
        return Err(refused(table, reason));
    }
    let has_keys = "inspect checked that the row groups sum to the file's rows, which are above zero, so a row group holds keys";
    let head = groups.first().expect(has_keys);
    let tail = groups.last().expect(has_keys);
    Ok(Verified {
        key_range: (head.first.clone(), tail.last.clone()),
        fences,
        groups,
        max_key_bytes: longest,
    })
}

/// The refusal for `key`, which is not above the key before it in `path`:
/// `Equal` is a duplicate, anything else is out of order.
fn not_above_previous(path: &str, row_group: usize, order: Ordering, key: Key) -> RefusalReason {
    match order {
        Ordering::Equal => RefusalReason::DuplicateKeys {
            path: path.to_string(),
            key,
        },
        Ordering::Less | Ordering::Greater => RefusalReason::Unsorted {
            path: path.to_string(),
            row_group,
        },
    }
}

/// Rows `rows` of the key column of `row_group` decoded from `chunks`, which
/// hold at least the pages those rows are in. Exactly those rows come back,
/// as `read_column` guarantees; a string column's values are not checked
/// as UTF-8 here, `check_utf8` does that where the run is read back through
/// `Keys::get`.
fn decode_run(
    column: &Column<'_>,
    shape: KeyShape,
    row_group: usize,
    rows: Range<i64>,
    chunks: Arc<Segments>,
) -> Result<Keys> {
    let skip = row_index(rows.start);
    let take = row_index(rows.end - rows.start);
    Ok(match shape {
        KeyShape::Long => Keys::Long(read_column::<Int64Type>(
            column, row_group, chunks, skip, take,
        )?),
        KeyShape::Str => Keys::Str(read_column::<ByteArrayType>(
            column, row_group, chunks, skip, take,
        )?),
        KeyShape::Bin => Keys::Bin(read_column::<ByteArrayType>(
            column, row_group, chunks, skip, take,
        )?),
        KeyShape::FixedBin => Keys::Bin(
            read_column::<FixedLenByteArrayType>(column, row_group, chunks, skip, take)?
                .into_iter()
                .map(ByteArray::from)
                .collect(),
        ),
        KeyShape::Uuid => Keys::Uuid(
            read_column::<FixedLenByteArrayType>(column, row_group, chunks, skip, take)?
                .iter()
                .map(|value| {
                    <[u8; UUID_BYTES]>::try_from(value.data()).expect(
                        "a FIXED_LEN_BYTE_ARRAY value is its column's width, checked at build to be UUID_BYTES",
                    )
                })
                .collect(),
        ),
    })
}

/// Every value of a `Keys::Str` run is UTF-8, or the file is
/// `FileFault::KeyNotUtf8`; the other arms hold what their bytes are.
fn check_utf8(column: &Column<'_>, row_group: usize, keys: &Keys) -> Result<()> {
    if let Keys::Str(run) = keys
        && run
            .iter()
            .any(|value| std::str::from_utf8(value.data()).is_err())
    {
        return Err(file_fault(column.path, FileFault::KeyNotUtf8 { row_group }));
    }
    Ok(())
}

/// `take` records of `column` in `row_group` after skipping `skip`, decoded
/// from `segments`, which hold at least the pages those rows are in and the
/// chunk's dictionary page when it has one. Whole pages before the rows
/// wanted are skipped from their locations without a read; the dictionary
/// page is read on the way, as the reader requires. Definition levels are
/// consumed when the column is nullable, and a null yields no value, so
/// fewer values than `take` may come back. On `Ok` exactly `take` records
/// were decoded, from exactly the pages the offset index names for them,
/// each page holding the rows the index gives it; a page that does not is
/// `FileFault::PageRowsMismatch`, so no row position rests on an offset
/// index the pages contradict.
fn read_column<T: ParquetDataType>(
    column: &Column<'_>,
    row_group: usize,
    segments: Arc<Segments>,
    skip: usize,
    take: usize,
) -> Result<Vec<T::T>> {
    let descr = column.descriptor();
    let nullable = descr.max_def_level() > 0;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let pages = CountedPages {
        inner: column.page_reader(row_group, segments)?,
        seen: Arc::clone(&seen),
    };
    let mut reader: ColumnReaderImpl<T> = ColumnReaderImpl::new(descr, Box::new(pages));
    let skipped = if skip > 0 {
        reader.skip_records(skip)?
    } else {
        0
    };
    let mut values = Vec::with_capacity(take);
    let mut def_levels = Vec::new();
    let mut records = 0;
    while skipped == skip && records < take {
        let (read, _, _) = reader.read_records(
            take - records,
            nullable.then_some(&mut def_levels),
            None,
            &mut values,
        )?;
        if read == 0 {
            break;
        }
        records += read;
    }
    let seen = seen.lock().unwrap_or_else(PoisonError::into_inner);
    column.check_pages_read(row_group, skip..skip + take, &seen, records)?;
    Ok(values)
}

/// Exactly one row of the value column from `segments`: `Some` with the
/// value, `None` for a null.
fn read_one<T: ParquetDataType>(
    column: &Column<'_>,
    row: RowRef,
    segments: Arc<Segments>,
) -> Result<Option<T::T>> {
    let mut values = read_column::<T>(column, row.row_group, segments, row_index(row.row), 1)?;
    Ok(values.pop())
}

/// The byte range of a page from its offset-index location; build checked
/// every location's offset and size.
fn page_range(location: &PageLocation) -> Range<u64> {
    let start =
        u64::try_from(location.offset).expect("page offsets were checked non-negative at build");
    let len = u64::try_from(location.compressed_page_size)
        .expect("page sizes were checked non-negative at build");
    start..start + len
}

/// A row count or row position as the reader takes it; build checked every
/// one non-negative.
fn row_index(rows: i64) -> usize {
    usize::try_from(rows).expect("row counts and positions were checked non-negative at build")
}

/// A position within a page as a row offset.
fn row_offset(at: usize) -> i64 {
    i64::try_from(at).expect("a page's rows fit i64")
}

/// A length in memory as the byte totals take it.
fn bytes(len: usize) -> u64 {
    u64::try_from(len).expect("a length in memory fits u64 on the supported systems")
}

/// The parts every tier's resident bytes are made of, from the footer, so
/// `predicted_resident_bytes` and `resident_bytes` cannot drift; the doc of
/// the former says which are exact and which estimated.
struct ResidentSet {
    footer: u64,
    key_chunks: u64,
    fences: u64,
    filters: Option<u64>,
    rows: u64,
}

impl ResidentSet {
    /// From `metadata`, with the fences exact when `fences` is given and
    /// estimated from the footer otherwise.
    fn from_footer(
        metadata: &ParquetMetaData,
        key_leaf: usize,
        fences: Option<&[Vec<Key>]>,
    ) -> Self {
        let key_columns = || {
            metadata
                .row_groups()
                .iter()
                .map(|group| group.column(key_leaf))
        };
        let filters = key_columns()
            .map(|column| {
                column
                    .bloom_filter_offset()
                    .and(column.bloom_filter_length())
                    .and_then(|len| u64::try_from(len).ok())
            })
            .sum::<Option<u64>>();
        Self {
            footer: bytes(kept_footer(metadata, key_leaf).memory_size()),
            key_chunks: key_columns().map(|column| column.byte_range().1).sum(),
            fences: fences.map_or_else(
                || estimated_fence_bytes(metadata, key_leaf),
                exact_fence_bytes,
            ),
            filters,
            // intentional: a negative row count is refused at build; here,
            // on any footer, it estimates as zero rows.
            rows: u64::try_from(metadata.file_metadata().num_rows()).unwrap_or(0),
        }
    }
}

/// The tiers' totals from their parts.
fn tiers(set: &ResidentSet) -> ResidentBytes {
    ResidentBytes {
        full: set.footer + set.key_chunks + set.fences,
        disk_memory: set.footer + set.fences,
        disk_bytes: set.key_chunks,
        bloom: set.filters.map(|filters| set.footer + filters + set.fences),
        bloom_bits_per_key: set
            .filters
            .map(|filters| approx::ratio(filters.saturating_mul(u64::from(u8::BITS)), set.rows)),
    }
}

/// The fence array's bytes: one `Key` per key page plus what each key holds
/// on the heap. The fences are built with exact allocations.
fn exact_fence_bytes(fences: &[Vec<Key>]) -> u64 {
    fences
        .iter()
        .flatten()
        .map(|key| {
            let heap = match key {
                Key::Long(_) | Key::Uuid(_) => 0,
                Key::Str(text) => text.len(),
                Key::Bytes(raw) => raw.len(),
            };
            bytes(size_of::<Key>() + heap)
        })
        .sum()
}

/// The fence array's bytes before the keys are decoded: one `Key` per key
/// page from the offset index plus, per page, the heap a first key of the
/// column's physical type would hold, as `predicted_resident_bytes`
/// documents.
fn estimated_fence_bytes(metadata: &ParquetMetaData, key_leaf: usize) -> u64 {
    let descr = metadata.file_metadata().schema_descr().column(key_leaf);
    let mut total = 0;
    for (row_group, group) in metadata.row_groups().iter().enumerate() {
        let pages = metadata
            .offset_index()
            .and_then(|index| index.get(row_group))
            .and_then(|columns| columns.get(key_leaf))
            .map_or(0, |column| column.page_locations().len());
        let column_index = metadata
            .column_index()
            .and_then(|index| index.get(row_group))
            .and_then(|columns| columns.get(key_leaf));
        let heap = match descr.physical_type() {
            PhysicalType::BYTE_ARRAY => {
                byte_array_fence_heap(column_index, group.column(key_leaf), pages)
            }
            PhysicalType::FIXED_LEN_BYTE_ARRAY => {
                // intentional: a fixed-width column's width is positive; a
                // footer that says otherwise estimates as no heap per fence.
                bytes(pages) * u64::try_from(descr.type_length()).unwrap_or(0)
            }
            PhysicalType::BOOLEAN
            | PhysicalType::INT32
            | PhysicalType::INT64
            | PhysicalType::INT96
            | PhysicalType::FLOAT
            | PhysicalType::DOUBLE => 0,
        };
        total += bytes(pages * size_of::<Key>()) + heap;
    }
    total
}

/// The heap bytes the fences of one row group of a `BYTE_ARRAY` key column
/// would hold: the page minimums' lengths from the column index when the
/// file has one, else the chunk's mean encoded bytes per value for every
/// page.
fn byte_array_fence_heap(
    column_index: Option<&ColumnIndexMetaData>,
    chunk: &ColumnChunkMetaData,
    pages: usize,
) -> u64 {
    match column_index {
        Some(ColumnIndexMetaData::BYTE_ARRAY(index)) => index
            .min_values_iter()
            .map(|min| bytes(min.map_or(0, <[u8]>::len)))
            .sum(),
        None
        | Some(
            ColumnIndexMetaData::NONE
            | ColumnIndexMetaData::BOOLEAN(_)
            | ColumnIndexMetaData::INT32(_)
            | ColumnIndexMetaData::INT64(_)
            | ColumnIndexMetaData::INT96(_)
            | ColumnIndexMetaData::FLOAT(_)
            | ColumnIndexMetaData::DOUBLE(_)
            | ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(_),
        ) => {
            // intentional: a chunk's size and value count are non-negative
            // in any footer the writer produced; one that says otherwise
            // estimates as no heap per fence, as does a chunk of no values.
            let encoded = u64::try_from(chunk.uncompressed_size()).unwrap_or(0);
            let values = u64::try_from(chunk.num_values()).unwrap_or(0);
            let mean = encoded.checked_div(values).unwrap_or(0);
            bytes(pages) * mean
        }
    }
}

/// `Error::Refused` for `table`.
fn refused(table: &str, reason: RefusalReason) -> Error {
    Error::Refused {
        table: table.to_string(),
        reason,
    }
}

/// `Error::File` for the file at `path`.
fn file_fault(path: &str, fault: FileFault) -> Error {
    Error::File {
        path: path.to_string(),
        fault,
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use arrow::array::{
        ArrayRef, BinaryArray, FixedSizeBinaryArray, Int64Array, RecordBatch, StringArray,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use object_store::memory::InMemory;
    use object_store::path::Path as ObjectPath;
    use object_store::{ObjectStoreExt as _, PutPayload};
    use parquet::arrow::ArrowWriter;
    use parquet::basic::{Compression, ZstdLevel};
    use parquet::file::metadata::{FileMetaData, ParquetMetaDataBuilder, ParquetMetaDataReader};
    use parquet::file::page_index::offset_index::OffsetIndexMetaData;
    use parquet::file::properties::{
        DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH, EnabledStatistics, WriterProperties,
    };
    use parquet::schema::types::ColumnPath;

    use super::*;
    use crate::config::{HedgeConfig, LocalConfig, StorageConfig};
    use crate::contract::{PAGE_ROWS, VALUE_PAGE_BYTES};
    use crate::storage::{HedgeWindow, IoStats, Storage};

    /// The table every fixture is built for.
    const TABLE: &str = "kv";
    /// The synthetic prefix the in-memory store is registered under.
    const MEM_PREFIX: &str = "mem://w/";
    /// The fixture file's key under the prefix.
    const FILE_KEY: &str = "db/kv/data/f0.parquet";
    /// The fixture's key column.
    const KEY_COLUMN: &str = "key";
    /// The fixture's value column.
    const VALUE_COLUMN: &str = "value";
    /// A column name no fixture has.
    const ABSENT_COLUMN: &str = "absent";
    /// Key pages per row group in the contract fixture.
    const PAGES_PER_GROUP: usize = 2;
    /// Row groups in the contract fixture, so row-group choice is exercised.
    const ROW_GROUPS: usize = 3;
    /// Row groups in the fixtures that route across truncated or absent
    /// statistics, so an earlier row group's bound covers later ones.
    const MANY_ROW_GROUPS: usize = 4;
    /// Bytes per value in the contract fixture.
    const VALUE_BYTES: usize = 64;
    /// Bytes per value where value pages must be bounded by bytes.
    const LARGE_VALUE_BYTES: usize = 4096;
    /// Bytes per value in the misaligned fixture: enough that the page byte
    /// bound splits value pages far more often than the row limit.
    const MISALIGNED_VALUE_BYTES: usize = 512;
    /// The step between consecutive keys, so the key after any written key
    /// is absent and inside the file's range.
    const KEY_STEP: usize = 2;
    /// The byte every binary key starts with, above zero so a below-range
    /// binary key exists.
    const BINARY_KEY_TAG: u8 = 0xff;
    /// The byte the first half of every uuid key holds, above zero so a
    /// below-range uuid exists.
    const UUID_KEY_FILL: u8 = 0x0a;
    /// The width of the fixed-width binary keys: a `u64`.
    const FIXED_KEY_BYTES: i32 = 8;
    /// The multipliers and modulus that make a value a function of its row,
    /// distinct for neighbouring rows.
    const VALUE_ROW_MIX: usize = 31;
    const VALUE_BYTE_MIX: usize = 7;
    const VALUE_MODULUS: usize = 251;
    /// The memory budget of a test page cache: every page a test touches fits.
    const CACHE_BYTES: usize = 64 * 1024 * 1024;
    /// The false-positive rate the fixtures' filters are sized at.
    const BLOOM_FPP: f64 = 0.01;
    /// Rows in the small fixtures whose keys are written by hand.
    const FEW_ROWS: usize = 8;
    /// Rows per row group in the hand-written fixtures with two row groups.
    const TINY_GROUP_ROWS: usize = 2;
    /// Bytes every long-prefix key shares, longer than `TRUNCATE_BYTES`, so
    /// every truncated statistic of the file is one and the same prefix.
    const LONG_PREFIX_BYTES: usize = 32;
    /// The truncation length of the long-prefix fixture's statistics and
    /// column index.
    const TRUNCATE_BYTES: usize = 16;
    /// The byte the long binary prefix repeats.
    const PREFIX_BYTE: u8 = 0x01;
    /// The page byte bound of the misaligned fixture: above a key page of
    /// `PAGE_ROWS` longs, below one of `PAGE_ROWS` string keys, and far
    /// below `PAGE_ROWS` values of `MISALIGNED_VALUE_BYTES`.
    const MISALIGNED_PAGE_BYTES: usize = 16 * 1024;
    /// The write batch under a byte bound: the writer checks the bound
    /// between batches, so a small batch makes the bound bind (RFD 002).
    const SMALL_BATCH: usize = 8;
    /// Every `NULL_EVERY`th value of the nullable fixture is null.
    const NULL_EVERY: usize = 7;
    /// The bytes overwritten at the start of a bloom filter to corrupt it,
    /// and the byte they are overwritten with: no thrift field header.
    const GARBAGE_BYTES: usize = 32;
    const GARBAGE: u8 = 0xff;
    /// The resident bytes of a byte-bounded large-value file stay within
    /// this factor of its key chunk bytes once the value column's column
    /// index is dropped; with the index kept they run to tens of times.
    const RESIDENT_TO_KEY_CHUNK_FACTOR: u64 = 6;
    /// The residency tiers.
    const TIERS: [Residency; 3] = [Residency::Full, Residency::Disk, Residency::Bloom];
    /// The key types.
    const KEY_TYPES: [KeyType; 4] = [
        KeyType::Long,
        KeyType::String,
        KeyType::Binary,
        KeyType::Uuid,
    ];

    fn page_rows() -> usize {
        usize::try_from(PAGE_ROWS).unwrap()
    }

    fn group_rows() -> usize {
        page_rows() * PAGES_PER_GROUP
    }

    fn contract_rows() -> usize {
        group_rows() * ROW_GROUPS
    }

    fn count(n: usize) -> u64 {
        u64::try_from(n).unwrap()
    }

    fn location() -> String {
        format!("{MEM_PREFIX}{FILE_KEY}")
    }

    /// The `i`th key of a fixture of `key_type`: `KEY_STEP * i` rendered in
    /// the type, ascending in the served order.
    fn key_for(i: usize, key_type: KeyType) -> Key {
        let n = KEY_STEP * i;
        match key_type {
            KeyType::Long => Key::Long(i64::try_from(n).unwrap()),
            KeyType::String => Key::Str(format!("user:{n:012}")),
            KeyType::Binary => {
                let mut bytes = vec![BINARY_KEY_TAG];
                bytes.extend_from_slice(&u64::try_from(n).unwrap().to_be_bytes());
                Key::Bytes(bytes)
            }
            KeyType::Uuid => {
                let mut uuid = [UUID_KEY_FILL; UUID_BYTES];
                uuid[UUID_BYTES / 2..].copy_from_slice(&u64::try_from(n).unwrap().to_be_bytes());
                Key::Uuid(uuid)
            }
        }
    }

    /// The key just after `key` in served order, which no fixture holds:
    /// keys step by `KEY_STEP`, and strings and bytes are extended.
    fn absent_after(key: &Key) -> Key {
        match key {
            Key::Long(value) => Key::Long(value + 1),
            Key::Str(text) => Key::Str(format!("{text}x")),
            Key::Bytes(bytes) => {
                let mut longer = bytes.clone();
                longer.push(0);
                Key::Bytes(longer)
            }
            Key::Uuid(uuid) => {
                let mut next = *uuid;
                next[UUID_BYTES - 1] += 1;
                Key::Uuid(next)
            }
        }
    }

    /// A key below every key `key_for` produces.
    fn below_range(key_type: KeyType) -> Key {
        match key_type {
            KeyType::Long => Key::Long(-1),
            KeyType::String => Key::Str("a".to_string()),
            KeyType::Binary => Key::Bytes(vec![0]),
            KeyType::Uuid => Key::Uuid([0; UUID_BYTES]),
        }
    }

    /// A key above every key `key_for` produces.
    fn above_range(key_type: KeyType) -> Key {
        match key_type {
            KeyType::Long => Key::Long(i64::MAX),
            KeyType::String => Key::Str("v".to_string()),
            KeyType::Binary => Key::Bytes(vec![BINARY_KEY_TAG, BINARY_KEY_TAG]),
            KeyType::Uuid => Key::Uuid([u8::MAX; UUID_BYTES]),
        }
    }

    /// The length in bytes of every key `key_for` produces for `key_type`.
    fn key_bytes(key_type: KeyType) -> usize {
        key_for(0, key_type).as_bytes().len()
    }

    /// `len` bytes that are a function of the row.
    fn value_of_len(i: usize, len: usize) -> Vec<u8> {
        (0..len)
            .map(|j| {
                u8::try_from((i * VALUE_ROW_MIX + j * VALUE_BYTE_MIX) % VALUE_MODULUS).unwrap()
            })
            .collect()
    }

    fn value_for(i: usize) -> Vec<u8> {
        value_of_len(i, VALUE_BYTES)
    }

    /// How a fixture's key column is typed in arrow.
    #[derive(Clone, Copy)]
    enum KeyColumn {
        Int64,
        Utf8,
        Binary,
        FixedBinary(i32),
    }

    /// How a fixture's value column is typed in arrow; `Int64` is the type
    /// the contract refuses.
    #[derive(Clone, Copy)]
    enum ValueColumn {
        Binary,
        FixedBinary(i32),
        Int64,
    }

    fn column_of(key_type: KeyType) -> KeyColumn {
        match key_type {
            KeyType::Long => KeyColumn::Int64,
            KeyType::String => KeyColumn::Utf8,
            KeyType::Binary => KeyColumn::Binary,
            KeyType::Uuid => KeyColumn::FixedBinary(i32::try_from(UUID_BYTES).unwrap()),
        }
    }

    /// Which columns a fixture dictionary-encodes.
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Dictionary {
        Off,
        /// The key column alone.
        Key,
        /// Every column.
        All,
    }

    /// The writer's layout knobs; `contract` is RFD 002's shape.
    #[derive(Clone, Copy)]
    struct Layout {
        page_rows: usize,
        row_group_rows: usize,
        page_bytes: usize,
        write_batch: usize,
        dictionary: Dictionary,
        statistics: EnabledStatistics,
        truncate: Option<usize>,
        bloom: bool,
        offset_index: bool,
    }

    impl Layout {
        fn contract() -> Self {
            Self {
                page_rows: page_rows(),
                row_group_rows: group_rows(),
                page_bytes: usize::MAX,
                write_batch: page_rows(),
                dictionary: Dictionary::Off,
                statistics: EnabledStatistics::Page,
                truncate: DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH,
                bloom: true,
                offset_index: true,
            }
        }

        /// The filter setters enable a filter on the column they name, so
        /// the key column gets one only when `bloom` asks and the value
        /// column never does. Page statistics keep the offset index on
        /// whatever `offset_index` says, so a fixture without one also
        /// writes no statistics.
        fn properties(&self) -> WriterProperties {
            let mut builder = WriterProperties::builder()
                .set_max_row_group_row_count(Some(self.row_group_rows))
                .set_write_batch_size(self.write_batch)
                .set_data_page_row_count_limit(self.page_rows)
                .set_data_page_size_limit(self.page_bytes)
                .set_dictionary_enabled(self.dictionary == Dictionary::All)
                .set_statistics_enabled(self.statistics)
                .set_column_index_truncate_length(self.truncate)
                .set_statistics_truncate_length(self.truncate)
                .set_compression(Compression::ZSTD(ZstdLevel::default()))
                .set_bloom_filter_enabled(false)
                .set_offset_index_disabled(!self.offset_index);
            if self.dictionary == Dictionary::Key {
                builder = builder.set_column_dictionary_enabled(ColumnPath::from(KEY_COLUMN), true);
            }
            if self.bloom {
                let key = ColumnPath::from(KEY_COLUMN);
                builder = builder
                    .set_column_bloom_filter_enabled(key.clone(), true)
                    .set_column_bloom_filter_fpp(key.clone(), BLOOM_FPP)
                    .set_column_bloom_filter_ndv(key, count(self.row_group_rows));
            }
            builder.build()
        }
    }

    /// One synthetic data file: what its columns hold and how the writer
    /// lays it out. A `None` key or value is a null, and makes its column
    /// nullable.
    struct Fixture {
        key_column: KeyColumn,
        value_column: ValueColumn,
        keys: Vec<Option<Key>>,
        values: Vec<Option<Vec<u8>>>,
        layout: Layout,
    }

    impl Fixture {
        /// RFD 002's shape with `rows` keys of `key_type` from `key_for`.
        fn contract(key_type: KeyType, rows: usize) -> Self {
            Self::with_keys(
                column_of(key_type),
                (0..rows).map(|i| Some(key_for(i, key_type))).collect(),
            )
        }

        /// RFD 002's shape over hand-written keys with `VALUE_BYTES` values.
        fn with_keys(key_column: KeyColumn, keys: Vec<Option<Key>>) -> Self {
            let values = (0..keys.len()).map(|i| Some(value_for(i))).collect();
            Self {
                key_column,
                value_column: ValueColumn::Binary,
                keys,
                values,
                layout: Layout::contract(),
            }
        }

        fn write(&self) -> Bytes {
            let key_nullable = self.keys.iter().any(Option::is_none);
            let value_nullable = self.values.iter().any(Option::is_none);
            let schema = Arc::new(Schema::new(vec![
                Field::new(KEY_COLUMN, key_data_type(self.key_column), key_nullable),
                Field::new(
                    VALUE_COLUMN,
                    value_data_type(self.value_column),
                    value_nullable,
                ),
            ]));
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    key_array(self.key_column, &self.keys),
                    value_array(self.value_column, &self.values),
                ],
            )
            .unwrap();
            let mut out = Vec::new();
            let mut writer =
                ArrowWriter::try_new(&mut out, schema, Some(self.layout.properties())).unwrap();
            if batch.num_rows() > 0 {
                writer.write(&batch).unwrap();
            }
            writer.close().unwrap();
            Bytes::from(out)
        }
    }

    fn key_data_type(column: KeyColumn) -> DataType {
        match column {
            KeyColumn::Int64 => DataType::Int64,
            KeyColumn::Utf8 => DataType::Utf8,
            KeyColumn::Binary => DataType::Binary,
            KeyColumn::FixedBinary(width) => DataType::FixedSizeBinary(width),
        }
    }

    fn value_data_type(column: ValueColumn) -> DataType {
        match column {
            ValueColumn::Binary => DataType::Binary,
            ValueColumn::FixedBinary(width) => DataType::FixedSizeBinary(width),
            ValueColumn::Int64 => DataType::Int64,
        }
    }

    fn key_array(column: KeyColumn, keys: &[Option<Key>]) -> ArrayRef {
        let raw = || {
            keys.iter()
                .map(|key| key.as_ref().map(|key| key.as_bytes().into_owned()))
        };
        match column {
            KeyColumn::Int64 => Arc::new(Int64Array::from(
                keys.iter()
                    .map(|key| {
                        key.as_ref().map(|key| {
                            let Key::Long(value) = key else {
                                panic!("a long fixture holds long keys");
                            };
                            *value
                        })
                    })
                    .collect::<Vec<_>>(),
            )),
            KeyColumn::Utf8 => Arc::new(
                keys.iter()
                    .map(|key| {
                        key.as_ref().map(|key| {
                            let Key::Str(text) = key else {
                                panic!("a string fixture holds string keys");
                            };
                            text.clone()
                        })
                    })
                    .collect::<StringArray>(),
            ),
            KeyColumn::Binary => Arc::new(raw().collect::<BinaryArray>()),
            KeyColumn::FixedBinary(width) => Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(raw(), width).unwrap(),
            ),
        }
    }

    fn value_array(column: ValueColumn, values: &[Option<Vec<u8>>]) -> ArrayRef {
        match column {
            ValueColumn::Binary => Arc::new(values.iter().cloned().collect::<BinaryArray>()),
            ValueColumn::FixedBinary(width) => Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(values.iter().cloned(), width)
                    .unwrap(),
            ),
            ValueColumn::Int64 => Arc::new(Int64Array::from_iter_values(
                (0..values.len()).map(|i| i64::try_from(i).unwrap()),
            )),
        }
    }

    /// A fixture file in an in-memory store behind a fresh storage, with a
    /// handle over it and a fresh hedge window.
    struct Opened {
        storage: Arc<Storage>,
        handle: Arc<FileHandle>,
        file: Bytes,
    }

    async fn store(fixture: &Fixture) -> Opened {
        store_file(fixture.write()).await
    }

    async fn store_file(file: Bytes) -> Opened {
        let object_store = Arc::new(InMemory::new());
        object_store
            .put(&ObjectPath::from(FILE_KEY), PutPayload::from(file.clone()))
            .await
            .unwrap();
        let storage = Arc::new(Storage::new(StorageConfig::default()));
        storage.register(MEM_PREFIX, object_store);
        let handle = FileHandle::open(
            Arc::clone(&storage),
            &location(),
            count(file.len()),
            Arc::new(HedgeWindow::new(&HedgeConfig::default())),
        )
        .unwrap();
        Opened {
            storage,
            handle: Arc::new(handle),
            file,
        }
    }

    impl Opened {
        fn stats(&self) -> Arc<IoStats> {
            self.storage.stats()
        }

        async fn build(
            &self,
            key_type: KeyType,
            residency: Residency,
            local: Option<&LocalDir>,
        ) -> Result<FileIndex> {
            FileIndex::build(
                TABLE,
                Arc::clone(&self.handle),
                KEY_COLUMN,
                VALUE_COLUMN,
                key_type,
                residency,
                local,
            )
            .await
        }

        /// A build in `residency`, with a local directory under `dir` for
        /// the Disk tier and none for the others.
        async fn build_in(
            &self,
            key_type: KeyType,
            residency: Residency,
            dir: &Path,
        ) -> Result<FileIndex> {
            let local = local_dir(dir, self.stats());
            let local = (residency == Residency::Disk).then_some(&local);
            self.build(key_type, residency, local).await
        }

        async fn index(&self, key_type: KeyType) -> FileIndex {
            self.build(key_type, Residency::Full, None).await.unwrap()
        }

        /// The key column's bloom filters read from the file itself: the
        /// oracle for whether a miss reaches the key page in the Bloom tier.
        fn filters(&self, index: &FileIndex) -> Vec<Sbbf> {
            index
                .metadata()
                .row_groups()
                .iter()
                .map(|group| {
                    Sbbf::read_from_column_chunk(group.column(index.key_leaf()), &self.file)
                        .unwrap()
                        .unwrap()
                })
                .collect()
        }
    }

    /// A local directory under `dir` charging its reads to `stats`, with no
    /// page cache disk level.
    fn local_dir(dir: &Path, stats: Arc<IoStats>) -> LocalDir {
        LocalDir::open(
            &LocalConfig {
                dir: dir.to_path_buf(),
                index_budget_bytes: None,
                page_cache_disk_bytes: 0,
            },
            stats,
        )
        .unwrap()
    }

    async fn cache() -> PageCache {
        PageCache::new(CACHE_BYTES, None).await.unwrap()
    }

    /// The refusal a build ended in, for `TABLE`.
    fn refusal_of(result: Result<FileIndex>) -> RefusalReason {
        let Err(Error::Refused { table, reason }) = result else {
            panic!("expected a refusal, got {result:?}");
        };
        assert_eq!(table, TABLE);
        reason
    }

    /// The file fault a build or a check ended in, for the fixture file.
    fn fault_of<T: std::fmt::Debug>(result: Result<T>) -> FileFault {
        let Err(Error::File { path, fault }) = result else {
            panic!("expected a file fault, got {result:?}");
        };
        assert_eq!(path, location());
        fault
    }

    /// `metadata` with the file's row count replaced.
    fn with_file_rows(metadata: &ParquetMetaData, rows: i64) -> ParquetMetaData {
        let file = metadata.file_metadata();
        let file = FileMetaData::new(
            file.version(),
            rows,
            file.created_by().map(str::to_string),
            file.key_value_metadata().cloned(),
            file.schema_descr_ptr(),
            file.column_orders().cloned(),
        );
        ParquetMetaDataBuilder::new(file)
            .set_row_groups(metadata.row_groups().to_vec())
            .set_column_index(metadata.column_index().cloned())
            .set_offset_index(metadata.offset_index().cloned())
            .build()
    }

    /// `metadata` with row group `row_group`'s row count replaced.
    fn with_group_rows(metadata: &ParquetMetaData, row_group: usize, rows: i64) -> ParquetMetaData {
        let mut builder = metadata.clone().into_builder();
        let mut groups = builder.take_row_groups();
        let group = groups.remove(row_group);
        groups.insert(
            row_group,
            group.into_builder().set_num_rows(rows).build().unwrap(),
        );
        builder.set_row_groups(groups).build()
    }

    /// `metadata` with column `leaf`'s page locations in row group 0
    /// replaced.
    fn with_pages(
        metadata: &ParquetMetaData,
        leaf: usize,
        page_locations: Vec<PageLocation>,
    ) -> ParquetMetaData {
        let mut builder = metadata.clone().into_builder();
        let mut offset_index = builder.take_offset_index().unwrap();
        offset_index[0][leaf] = OffsetIndexMetaData {
            page_locations,
            unencoded_byte_array_data_bytes: None,
        };
        builder.set_offset_index(Some(offset_index)).build()
    }

    /// Column `leaf`'s page locations in row group 0.
    fn pages_of(metadata: &ParquetMetaData, leaf: usize) -> Vec<PageLocation> {
        metadata.offset_index().unwrap()[0][leaf]
            .page_locations()
            .clone()
    }

    impl FileIndex {
        /// This index over `metadata` in place of the footer it was built
        /// with, so a lookup runs against a footer the file contradicts.
        fn with_metadata(self, metadata: ParquetMetaData) -> Self {
            Self {
                metadata: Arc::new(metadata),
                ..self
            }
        }
    }

    /// The row `key_for(i)` sits at in the contract fixture.
    fn row_of(i: usize, group_rows: usize) -> RowRef {
        RowRef {
            row_group: i / group_rows,
            row: i64::try_from(i % group_rows).unwrap(),
        }
    }

    /// Finds every written key at its row and reads its value back byte for
    /// byte through `cache`, checking that a find costs no GET, a cold hit
    /// one, and a warm hit none. Returns the distinct value pages fetched.
    async fn check_every_key(
        index: &FileIndex,
        stats: &IoStats,
        key_type: KeyType,
        rows: usize,
    ) -> usize {
        let cache = cache().await;
        let group_rows = usize::try_from(index.metadata().row_group(0).num_rows()).unwrap();
        let before = stats.gets();
        let mut fetched = 0;
        for i in 0..rows {
            let key = key_for(i, key_type);
            let row = index
                .find(&key, Some(&cache))
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("key {key} not found"));
            assert_eq!(row, row_of(i, group_rows), "key {key}");
            assert_eq!(stats.gets(), before + fetched, "find costs no GET");
            let cold = cache.fetches() == fetched;
            let value = index.read_value(row, Some(&cache)).await.unwrap();
            assert_eq!(value.as_deref(), Some(value_for(i).as_slice()), "key {key}");
            if cache.fetches() > fetched {
                assert!(cold);
                fetched += 1;
            }
            assert_eq!(
                stats.gets(),
                before + fetched,
                "a hit costs one GET cold and none warm"
            );
        }
        assert_eq!(stats.hedges(), 0);
        assert_eq!(cache.fetches(), fetched);
        assert_eq!(cache.hits(), count(rows) - fetched);
        usize::try_from(fetched).unwrap()
    }

    #[tokio::test]
    async fn test_find_and_read_value_every_key_per_type() {
        for key_type in KEY_TYPES {
            let rows = contract_rows();
            let opened = store(&Fixture::contract(key_type, rows)).await;
            let index = opened.index(key_type).await;
            let stats = opened.stats();
            assert_eq!(
                stats.gets(),
                1 + count(ROW_GROUPS),
                "footer, then one chunk per row group"
            );
            assert_eq!(index.residency(), Residency::Full);
            assert_eq!(index.rows(), i64::try_from(rows).unwrap());
            assert_eq!(index.fences().len(), ROW_GROUPS);
            for row_group in 0..ROW_GROUPS {
                assert_eq!(index.page_count(row_group), PAGES_PER_GROUP);
                assert_eq!(
                    index.fences()[row_group][0],
                    key_for(row_group * group_rows(), key_type)
                );
                assert_eq!(index.page_rows(row_group, 1), PAGE_ROWS..PAGE_ROWS * 2);
            }
            assert_eq!(
                index.key_range(),
                (&key_for(0, key_type), &key_for(rows - 1, key_type))
            );
            assert_eq!(index.max_key_bytes(), key_bytes(key_type));
            assert_eq!(index.metadata().num_row_groups(), ROW_GROUPS);
            assert!(Arc::ptr_eq(index.handle(), &opened.handle));

            let pages = check_every_key(&index, &stats, key_type, rows).await;
            assert_eq!(pages, ROW_GROUPS * PAGES_PER_GROUP);
        }
    }

    #[tokio::test]
    async fn test_find_miss_costs_no_get_inside_or_outside_the_range_per_type() {
        for key_type in KEY_TYPES {
            let opened = store(&Fixture::contract(key_type, contract_rows())).await;
            let index = opened.index(key_type).await;
            let stats = opened.stats();
            let cache = cache().await;
            let before = stats.gets();
            let inside = [
                0,
                page_rows() - 1,
                page_rows(),
                group_rows() - 1,
                group_rows(),
                contract_rows() - 2,
            ];
            for i in inside {
                let key = absent_after(&key_for(i, key_type));
                assert!(index.may_contain(&key), "{key}");
                // The key after a row group's last key falls between two row
                // groups: no page can hold it, so none is named.
                let between_groups = (i + 1) % group_rows() == 0;
                assert_eq!(index.page_of(&key).is_some(), !between_groups, "{key}");
                assert_eq!(index.find(&key, Some(&cache)).await.unwrap(), None, "{key}");
            }
            for key in [below_range(key_type), above_range(key_type)] {
                assert!(!index.may_contain(&key));
                assert_eq!(index.page_of(&key), None);
                assert_eq!(index.find(&key, Some(&cache)).await.unwrap(), None);
            }
            assert_eq!(stats.gets(), before);
            assert_eq!(cache.fetches(), 0);
        }
    }

    /// `key_for` behind a shared prefix longer than the statistics
    /// truncation; longs and uuids have no prefix to share.
    fn long_prefix_key(i: usize, key_type: KeyType) -> Key {
        match key_for(i, key_type) {
            Key::Str(text) => Key::Str(format!("{}{text}", "p".repeat(LONG_PREFIX_BYTES))),
            Key::Bytes(bytes) => Key::Bytes([vec![PREFIX_BYTE; LONG_PREFIX_BYTES], bytes].concat()),
            Key::Long(_) | Key::Uuid(_) => panic!("prefixed keys are strings or bytes"),
        }
    }

    /// Every key of a `MANY_ROW_GROUPS` file whose keys share a prefix
    /// longer than `TRUNCATE_BYTES` is found in every tier: row-group and
    /// page choice is by the exact keys, never by the truncated bounds, which
    /// are one prefix for every row group.
    async fn check_long_prefix_lookups(key_type: KeyType, residency: Residency) {
        let rows = contract_rows();
        let dir = tempfile::tempdir().unwrap();
        let mut fixture = Fixture::with_keys(
            column_of(key_type),
            (0..rows)
                .map(|i| Some(long_prefix_key(i, key_type)))
                .collect(),
        );
        fixture.layout.truncate = Some(TRUNCATE_BYTES);
        fixture.layout.row_group_rows = rows / MANY_ROW_GROUPS;
        let opened = store(&fixture).await;
        let index = opened
            .build_in(key_type, residency, dir.path())
            .await
            .unwrap();
        assert_eq!(index.residency(), residency);
        assert_eq!(index.metadata().num_row_groups(), MANY_ROW_GROUPS);
        assert!(index.max_key_bytes() > LONG_PREFIX_BYTES);
        let statistics = index
            .metadata()
            .row_group(0)
            .column(index.key_leaf())
            .statistics()
            .unwrap();
        assert!(
            !statistics.max_is_exact()
                && statistics.max_bytes_opt().unwrap().len() <= TRUNCATE_BYTES
        );
        let cache = cache().await;
        for i in 0..rows {
            let key = long_prefix_key(i, key_type);
            let row = index
                .find(&key, Some(&cache))
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("key {key} not found in {residency}"));
            assert_eq!(row, row_of(i, rows / MANY_ROW_GROUPS), "key {key}");
            let value = index.read_value(row, Some(&cache)).await.unwrap();
            assert_eq!(value.as_deref(), Some(value_for(i).as_slice()));
        }
        let group_rows = rows / MANY_ROW_GROUPS;
        for i in [0, group_rows - 1, group_rows, rows - 2] {
            let key = absent_after(&long_prefix_key(i, key_type));
            assert_eq!(index.find(&key, Some(&cache)).await.unwrap(), None, "{key}");
        }
    }

    #[tokio::test]
    async fn test_find_long_prefix_string_keys_across_truncated_row_group_statistics() {
        for residency in TIERS {
            check_long_prefix_lookups(KeyType::String, residency).await;
        }
    }

    #[tokio::test]
    async fn test_find_long_prefix_binary_keys_across_truncated_row_group_statistics() {
        for residency in TIERS {
            check_long_prefix_lookups(KeyType::Binary, residency).await;
        }
    }

    #[tokio::test]
    async fn test_find_every_key_with_statistics_disabled_across_row_groups_per_type() {
        for key_type in KEY_TYPES {
            let rows = contract_rows();
            let mut fixture = Fixture::contract(key_type, rows);
            fixture.layout.statistics = EnabledStatistics::None;
            fixture.layout.row_group_rows = rows / MANY_ROW_GROUPS;
            let opened = store(&fixture).await;
            let index = opened.index(key_type).await;
            assert_eq!(index.metadata().num_row_groups(), MANY_ROW_GROUPS);
            let key_chunk = index.metadata().row_group(0).column(index.key_leaf());
            assert!(key_chunk.statistics().is_none(), "no row-group statistics");
            assert_eq!(
                index.metadata().column_index().unwrap()[0][index.key_leaf()],
                ColumnIndexMetaData::NONE,
                "no column index"
            );
            let pages = check_every_key(&index, &opened.stats(), key_type, rows).await;
            let group_rows = rows / MANY_ROW_GROUPS;
            assert_eq!(pages, MANY_ROW_GROUPS * group_rows.div_ceil(page_rows()));
            let cache = cache().await;
            for i in [group_rows - 1, group_rows, 2 * group_rows - 1] {
                let key = absent_after(&key_for(i, key_type));
                assert_eq!(index.find(&key, Some(&cache)).await.unwrap(), None, "{key}");
            }
        }
    }

    #[tokio::test]
    async fn test_read_value_resolves_the_right_page_when_key_and_value_pages_misalign() {
        for key_type in [KeyType::Long, KeyType::String] {
            let rows = contract_rows();
            let mut fixture = Fixture::contract(key_type, rows);
            fixture.values = (0..rows)
                .map(|i| Some(value_of_len(i, MISALIGNED_VALUE_BYTES)))
                .collect();
            fixture.layout.page_bytes = MISALIGNED_PAGE_BYTES;
            fixture.layout.write_batch = SMALL_BATCH;
            let opened = store(&fixture).await;
            let index = opened.index(key_type).await;
            let offset_index = index.metadata().offset_index().unwrap();
            let key_pages = offset_index[0][index.key_leaf()].page_locations().len();
            let value_pages = offset_index[0][index.value_leaf()].page_locations().len();
            assert!(
                value_pages > key_pages,
                "{value_pages} value pages against {key_pages} key pages"
            );
            let cache = cache().await;
            for i in 0..rows {
                let row = index
                    .find(&key_for(i, key_type), Some(&cache))
                    .await
                    .unwrap()
                    .unwrap();
                let value = index.read_value(row, Some(&cache)).await.unwrap();
                assert_eq!(
                    value.as_deref(),
                    Some(value_of_len(i, MISALIGNED_VALUE_BYTES).as_slice())
                );
            }
            let total_value_pages: usize = (0..ROW_GROUPS)
                .map(|row_group| {
                    offset_index[row_group][index.value_leaf()]
                        .page_locations()
                        .len()
                })
                .sum();
            assert_eq!(
                cache.fetches(),
                count(total_value_pages),
                "one GET per value page"
            );
        }
    }

    #[tokio::test]
    async fn test_read_value_dictionary_encoded_value_costs_one_extra_get_and_key_none() {
        let rows = contract_rows();
        let mut fixture = Fixture::contract(KeyType::Long, rows);
        fixture.layout.dictionary = Dictionary::All;
        let opened = store(&fixture).await;
        let index = opened.index(KeyType::Long).await;
        let stats = opened.stats();
        let group = index.metadata().row_group(0);
        assert!(
            group
                .column(index.key_leaf())
                .dictionary_page_offset()
                .is_some()
        );
        assert!(
            group
                .column(index.value_leaf())
                .dictionary_page_offset()
                .is_some()
        );
        let cache = cache().await;
        let before = stats.gets();
        let first = page_rows() + 1;
        let row = index
            .find(&key_for(first, KeyType::Long), Some(&cache))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            stats.gets(),
            before,
            "the key dictionary page is in the resident chunk"
        );
        let value = index.read_value(row, Some(&cache)).await.unwrap();
        assert_eq!(value.as_deref(), Some(value_for(first).as_slice()));
        assert_eq!(
            stats.gets(),
            before + 2,
            "the value page and its dictionary page"
        );
        let second = first + 1;
        let row = index
            .find(&key_for(second, KeyType::Long), Some(&cache))
            .await
            .unwrap()
            .unwrap();
        let value = index.read_value(row, Some(&cache)).await.unwrap();
        assert_eq!(value.as_deref(), Some(value_for(second).as_slice()));
        assert_eq!(stats.gets(), before + 2, "both pages cached");
        let other_page = 0;
        let row = index
            .find(&key_for(other_page, KeyType::Long), Some(&cache))
            .await
            .unwrap()
            .unwrap();
        index.read_value(row, Some(&cache)).await.unwrap();
        assert_eq!(
            stats.gets(),
            before + 3,
            "another value page, the dictionary cached"
        );
    }

    #[tokio::test]
    async fn test_read_value_null_is_none_at_the_same_get_count() {
        let rows = contract_rows();
        // Nulls every `NULL_EVERY`th row and at both sides of a page edge
        // and at a row group's last row, so the skip over definition levels
        // to the last row of a page and the whole-page skip to the first row
        // of the next both land on a null.
        let edge_nulls = [page_rows() - 1, page_rows(), group_rows() - 1];
        let is_null = |i: usize| i.is_multiple_of(NULL_EVERY) || edge_nulls.contains(&i);
        let mut fixture = Fixture::contract(KeyType::Long, rows);
        fixture.values = (0..rows)
            .map(|i| (!is_null(i)).then(|| value_for(i)))
            .collect();
        let opened = store(&fixture).await;
        let index = opened.index(KeyType::Long).await;
        let stats = opened.stats();
        let cache = cache().await;
        let before = stats.gets();
        let null_row = NULL_EVERY * 3;
        assert!(null_row < page_rows() && !edge_nulls.contains(&null_row));
        let row = index
            .find(&key_for(null_row, KeyType::Long), Some(&cache))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(index.read_value(row, Some(&cache)).await.unwrap(), None);
        assert_eq!(
            stats.gets(),
            before + 1,
            "a null costs the same one GET cold"
        );
        let present = null_row + 1;
        let row = index
            .find(&key_for(present, KeyType::Long), Some(&cache))
            .await
            .unwrap()
            .unwrap();
        let value = index.read_value(row, Some(&cache)).await.unwrap();
        assert_eq!(value.as_deref(), Some(value_for(present).as_slice()));
        assert_eq!(stats.gets(), before + 1, "and none warm");
        let row = index
            .find(&key_for(0, KeyType::Long), Some(&cache))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(index.read_value(row, Some(&cache)).await.unwrap(), None);
        assert_eq!(stats.gets(), before + 1);
        // The last row of value page 0 (cached), the first row of value
        // page 1 (one GET), and the last row of the row group (page 1 again).
        for (i, extra_gets) in [
            (page_rows() - 1, 0),
            (page_rows(), 1),
            (group_rows() - 1, 1),
        ] {
            let row = index
                .find(&key_for(i, KeyType::Long), Some(&cache))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                index.read_value(row, Some(&cache)).await.unwrap(),
                None,
                "row {i}"
            );
            assert_eq!(stats.gets(), before + 1 + extra_gets, "row {i}");
            let neighbour = i - 1;
            let row = index
                .find(&key_for(neighbour, KeyType::Long), Some(&cache))
                .await
                .unwrap()
                .unwrap();
            let value = index.read_value(row, Some(&cache)).await.unwrap();
            let expected = (!is_null(neighbour)).then(|| value_for(neighbour));
            assert_eq!(value.as_deref(), expected.as_deref(), "row {neighbour}");
        }
    }

    /// A dictionary-encoded key column's dictionary page is in the resident
    /// chunk in the Full tier and costs a lookup nothing; in the Disk tier
    /// it is one more local read per find, since a chunk file caches
    /// nothing, and in the Bloom tier one more GET on the first positive in
    /// a row group and none after, since the page cache keeps it by offset.
    /// The value column stays plain, so a hit's value page costs what it
    /// does in the contract layout.
    #[tokio::test]
    async fn test_find_dictionary_encoded_key_costs_per_tier() {
        let rows = contract_rows();
        let dir = tempfile::tempdir().unwrap();
        let mut fixture = Fixture::contract(KeyType::Long, rows);
        fixture.layout.dictionary = Dictionary::Key;
        let opened = store(&fixture).await;
        let stats = opened.stats();
        for residency in TIERS {
            let index = opened
                .build_in(KeyType::Long, residency, dir.path())
                .await
                .unwrap();
            let group = index.metadata().row_group(1);
            assert!(
                group
                    .column(index.key_leaf())
                    .dictionary_page_offset()
                    .is_some()
            );
            assert!(
                group
                    .column(index.value_leaf())
                    .dictionary_page_offset()
                    .is_none()
            );
            let cache = cache().await;
            // Two keys on different key pages of row group 1, so the second
            // find reads another key page but the same dictionary page.
            let first = group_rows() + 1;
            let second = group_rows() + page_rows() + 1;
            let (find_gets, find_reads, again_gets, again_reads) = match residency {
                Residency::Full => (0, 0, 0, 0),
                Residency::Disk => (0, 2, 0, 2),
                Residency::Bloom => (2, 0, 1, 0),
            };
            let (gets, reads) = (stats.gets(), stats.local_reads());
            let row = index
                .find(&key_for(first, KeyType::Long), Some(&cache))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(row, row_of(first, group_rows()));
            assert_eq!(
                (stats.gets(), stats.local_reads()),
                (gets + find_gets, reads + find_reads),
                "{residency}: a find with a dictionary-encoded key column"
            );
            let value = index.read_value(row, Some(&cache)).await.unwrap();
            assert_eq!(value.as_deref(), Some(value_for(first).as_slice()));
            assert_eq!(
                (stats.gets(), stats.local_reads()),
                (gets + find_gets + 1, reads + find_reads),
                "{residency}: the value page is one GET"
            );
            let (gets, reads) = (stats.gets(), stats.local_reads());
            let row = index
                .find(&key_for(second, KeyType::Long), Some(&cache))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(row, row_of(second, group_rows()));
            assert_eq!(
                (stats.gets(), stats.local_reads()),
                (gets + again_gets, reads + again_reads),
                "{residency}: another key page of the same row group"
            );
        }
    }

    /// `FEW_ROWS` string keys of exactly `len` bytes: zero-padded decimals.
    fn keys_of_len(len: usize) -> Vec<Option<Key>> {
        (0..FEW_ROWS)
            .map(|i| Some(Key::Str(format!("{:0>len$}", KEY_STEP * i))))
            .collect()
    }

    #[tokio::test]
    async fn test_build_serves_keys_of_max_key_bytes_and_refuses_one_byte_over() {
        let opened = store(&Fixture::with_keys(
            KeyColumn::Utf8,
            keys_of_len(MAX_KEY_BYTES),
        ))
        .await;
        let index = opened.index(KeyType::String).await;
        assert_eq!(index.max_key_bytes(), MAX_KEY_BYTES);
        for i in 0..FEW_ROWS {
            let key = Key::Str(format!("{:0>MAX_KEY_BYTES$}", KEY_STEP * i));
            let row = index.find(&key, None).await.unwrap().unwrap();
            assert_eq!(
                row,
                RowRef {
                    row_group: 0,
                    row: i64::try_from(i).unwrap()
                }
            );
        }

        let over = store(&Fixture::with_keys(
            KeyColumn::Utf8,
            keys_of_len(MAX_KEY_BYTES + 1),
        ))
        .await;
        for residency in TIERS {
            let dir = tempfile::tempdir().unwrap();
            assert_eq!(
                refusal_of(over.build_in(KeyType::String, residency, dir.path()).await),
                RefusalReason::KeyTooLong {
                    path: location(),
                    bytes: MAX_KEY_BYTES + 1,
                }
            );
        }
    }

    fn longs(values: &[i64]) -> Vec<Option<Key>> {
        values.iter().map(|value| Some(Key::Long(*value))).collect()
    }

    #[tokio::test]
    async fn test_build_refuses_an_unsorted_row_group_in_every_tier() {
        let opened = store(&Fixture::with_keys(KeyColumn::Int64, longs(&[5, 3, 9]))).await;
        for residency in TIERS {
            let dir = tempfile::tempdir().unwrap();
            assert_eq!(
                refusal_of(opened.build_in(KeyType::Long, residency, dir.path()).await),
                RefusalReason::Unsorted {
                    path: location(),
                    row_group: 0,
                }
            );
        }
        assert_eq!(opened.stats().local_reads(), 0);
    }

    #[tokio::test]
    async fn test_build_refuses_a_row_group_starting_below_the_previous_last_key() {
        let mut fixture = Fixture::with_keys(KeyColumn::Int64, longs(&[10, 20, 5, 30]));
        fixture.layout.row_group_rows = TINY_GROUP_ROWS;
        let opened = store(&fixture).await;
        for residency in TIERS {
            let dir = tempfile::tempdir().unwrap();
            let index = opened.build_in(KeyType::Long, residency, dir.path()).await;
            assert_eq!(
                refusal_of(index),
                RefusalReason::Unsorted {
                    path: location(),
                    row_group: 1,
                }
            );
        }
        let mut sorted = Fixture::with_keys(KeyColumn::Int64, longs(&[10, 20, 25, 30]));
        sorted.layout.row_group_rows = TINY_GROUP_ROWS;
        let index = store(&sorted).await.index(KeyType::Long).await;
        assert_eq!(index.metadata().num_row_groups(), 2);
        assert_eq!(
            index.find(&Key::Long(25), None).await.unwrap(),
            Some(RowRef {
                row_group: 1,
                row: 0
            })
        );
        assert_eq!(index.find(&Key::Long(22), None).await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_build_refuses_a_duplicate_key_in_every_tier() {
        let within = store(&Fixture::with_keys(KeyColumn::Int64, longs(&[1, 2, 2, 3]))).await;
        let mut across = Fixture::with_keys(KeyColumn::Int64, longs(&[10, 20, 20, 30]));
        across.layout.row_group_rows = TINY_GROUP_ROWS;
        let across = store(&across).await;
        for residency in TIERS {
            let dir = tempfile::tempdir().unwrap();
            assert_eq!(
                refusal_of(within.build_in(KeyType::Long, residency, dir.path()).await),
                RefusalReason::DuplicateKeys {
                    path: location(),
                    key: Key::Long(2),
                }
            );
            assert_eq!(
                refusal_of(across.build_in(KeyType::Long, residency, dir.path()).await),
                RefusalReason::DuplicateKeys {
                    path: location(),
                    key: Key::Long(20),
                }
            );
        }
        assert_eq!(
            across
                .handle
                .load_metadata()
                .await
                .unwrap()
                .num_row_groups(),
            2
        );
    }

    #[tokio::test]
    async fn test_build_refuses_a_null_key_in_every_tier() {
        let opened = store(&Fixture::with_keys(
            KeyColumn::Int64,
            vec![Some(Key::Long(1)), None, Some(Key::Long(3))],
        ))
        .await;
        for residency in TIERS {
            let dir = tempfile::tempdir().unwrap();
            assert_eq!(
                refusal_of(opened.build_in(KeyType::Long, residency, dir.path()).await),
                RefusalReason::NullKeys {
                    path: location(),
                    row_group: 0,
                }
            );
        }
    }

    #[tokio::test]
    async fn test_build_refuses_a_key_column_of_the_wrong_physical_type() {
        let cases = [
            (KeyColumn::Int64, KeyType::String, PhysicalType::INT64),
            (KeyColumn::Int64, KeyType::Uuid, PhysicalType::INT64),
            (KeyColumn::Utf8, KeyType::Long, PhysicalType::BYTE_ARRAY),
            (KeyColumn::Binary, KeyType::Uuid, PhysicalType::BYTE_ARRAY),
            (
                KeyColumn::FixedBinary(FIXED_KEY_BYTES),
                KeyType::Uuid,
                PhysicalType::FIXED_LEN_BYTE_ARRAY,
            ),
            (
                KeyColumn::FixedBinary(FIXED_KEY_BYTES),
                KeyType::String,
                PhysicalType::FIXED_LEN_BYTE_ARRAY,
            ),
        ];
        for (column, key_type, got) in cases {
            let keys = match column {
                KeyColumn::Int64 => longs(&[1, 2, 3]),
                KeyColumn::Utf8 => (0..FEW_ROWS)
                    .map(|i| Some(key_for(i, KeyType::String)))
                    .collect(),
                KeyColumn::Binary => (0..FEW_ROWS)
                    .map(|i| Some(key_for(i, KeyType::Binary)))
                    .collect(),
                KeyColumn::FixedBinary(_) => (0..FEW_ROWS)
                    .map(|i| Some(Key::Bytes(count(KEY_STEP * i).to_be_bytes().to_vec())))
                    .collect(),
            };
            let opened = store(&Fixture::with_keys(column, keys)).await;
            assert_eq!(
                refusal_of(opened.build(key_type, Residency::Full, None).await),
                RefusalReason::KeyTypeMismatch {
                    path: location(),
                    expected: key_type,
                    got,
                }
            );
            assert_eq!(opened.stats().gets(), 1, "refused at the footer");
        }
    }

    #[tokio::test]
    async fn test_find_fixed_width_binary_keys_as_binary() {
        let keys: Vec<Option<Key>> = (0..FEW_ROWS)
            .map(|i| Some(Key::Bytes(count(KEY_STEP * i).to_be_bytes().to_vec())))
            .collect();
        let opened = store(&Fixture::with_keys(
            KeyColumn::FixedBinary(FIXED_KEY_BYTES),
            keys.clone(),
        ))
        .await;
        let index = opened.index(KeyType::Binary).await;
        assert_eq!(
            index.max_key_bytes(),
            usize::try_from(FIXED_KEY_BYTES).unwrap()
        );
        for (i, key) in keys.iter().enumerate() {
            let key = key.as_ref().unwrap();
            let row = index.find(key, None).await.unwrap().unwrap();
            assert_eq!(
                row,
                RowRef {
                    row_group: 0,
                    row: i64::try_from(i).unwrap()
                }
            );
            let value = index.read_value(row, None).await.unwrap();
            assert_eq!(value.as_deref(), Some(value_for(i).as_slice()));
            assert_eq!(index.find(&absent_after(key), None).await.unwrap(), None);
        }
    }

    #[tokio::test]
    async fn test_read_value_fixed_width_values_byte_for_byte() {
        let mut fixture = Fixture::contract(KeyType::Long, FEW_ROWS);
        fixture.value_column = ValueColumn::FixedBinary(i32::try_from(VALUE_BYTES).unwrap());
        let opened = store(&fixture).await;
        let index = opened.index(KeyType::Long).await;
        for i in 0..FEW_ROWS {
            let row = index
                .find(&key_for(i, KeyType::Long), None)
                .await
                .unwrap()
                .unwrap();
            let value = index.read_value(row, None).await.unwrap();
            assert_eq!(value.as_deref(), Some(value_for(i).as_slice()));
        }
    }

    #[tokio::test]
    async fn test_build_refuses_an_int64_value_column() {
        let mut fixture = Fixture::contract(KeyType::Long, FEW_ROWS);
        fixture.value_column = ValueColumn::Int64;
        let opened = store(&fixture).await;
        assert_eq!(
            refusal_of(opened.build(KeyType::Long, Residency::Full, None).await),
            RefusalReason::ValueTypeUnsupported {
                path: location(),
                got: PhysicalType::INT64,
            }
        );
        assert_eq!(opened.stats().gets(), 1, "refused at the footer");
    }

    #[tokio::test]
    async fn test_build_refuses_a_string_key_that_is_not_utf8() {
        let opened = store(&Fixture::with_keys(
            KeyColumn::Binary,
            (0..FEW_ROWS)
                .map(|i| Some(key_for(i, KeyType::Binary)))
                .collect(),
        ))
        .await;
        assert_eq!(
            fault_of(opened.build(KeyType::String, Residency::Full, None).await),
            FileFault::KeyNotUtf8 { row_group: 0 }
        );
    }

    #[tokio::test]
    async fn test_build_missing_column_is_a_file_fault() {
        let opened = store(&Fixture::contract(KeyType::Long, FEW_ROWS)).await;
        for (key_column, value_column) in
            [(ABSENT_COLUMN, VALUE_COLUMN), (KEY_COLUMN, ABSENT_COLUMN)]
        {
            let result = FileIndex::build(
                TABLE,
                Arc::clone(&opened.handle),
                key_column,
                value_column,
                KeyType::Long,
                Residency::Full,
                None,
            )
            .await;
            assert_eq!(
                fault_of(result),
                FileFault::MissingColumn(ABSENT_COLUMN.to_string())
            );
        }
    }

    #[tokio::test]
    async fn test_build_without_page_index_is_a_file_fault() {
        let mut fixture = Fixture::contract(KeyType::Long, FEW_ROWS);
        fixture.layout.offset_index = false;
        fixture.layout.statistics = EnabledStatistics::None;
        let opened = store(&fixture).await;
        assert_eq!(
            fault_of(opened.build(KeyType::Long, Residency::Full, None).await),
            FileFault::MissingPageIndex
        );
        assert_eq!(opened.stats().gets(), 1, "refused at the footer");
    }

    #[tokio::test]
    async fn test_inspect_refuses_row_counts_that_disagree() {
        let opened = store(&Fixture::contract(KeyType::Long, contract_rows())).await;
        let loaded = opened.handle.load_metadata().await.unwrap();
        let inspect = |metadata: &ParquetMetaData| {
            super::Layout::inspect(
                TABLE,
                &opened.handle,
                metadata,
                KEY_COLUMN,
                VALUE_COLUMN,
                KeyType::Long,
            )
            .map(|layout| (layout.key_leaf, layout.value_leaf))
        };
        assert_eq!(inspect(&loaded).unwrap(), (0, 1));
        let rows = loaded.file_metadata().num_rows();
        let disagreeing = [
            with_file_rows(&loaded, -1),
            with_file_rows(&loaded, rows + 1),
            with_group_rows(&loaded, 1, -1),
            with_group_rows(&loaded, 1, PAGE_ROWS),
        ];
        for patched in &disagreeing {
            assert_eq!(fault_of(inspect(patched)), FileFault::InconsistentRowCounts);
        }
        assert_eq!(
            FileIndex::predicted_resident_bytes(&disagreeing[0], 0).bloom_bits_per_key,
            Some(0.0),
            "a negative row count estimates as no rows"
        );
    }

    #[tokio::test]
    async fn test_check_offset_index_refuses_an_index_that_contradicts_its_pages() {
        let opened = store(&Fixture::contract(KeyType::Long, contract_rows())).await;
        let loaded = opened.handle.load_metadata().await.unwrap();
        let path = location();
        for leaf in [0, 1] {
            assert!(check_offset_index(&path, &loaded, leaf).is_ok());
            let pages = pages_of(&loaded, leaf);
            assert_eq!(pages.len(), PAGES_PER_GROUP);
            let (chunk_start, chunk_len) = loaded.row_group(0).column(leaf).byte_range();
            let mut first_row_one = pages.clone();
            first_row_one[0].first_row_index = 1;
            let mut negative_offset = pages.clone();
            negative_offset[0].offset = -1;
            let mut negative_size = pages.clone();
            negative_size[1].compressed_page_size = -1;
            let mut before_chunk = pages.clone();
            before_chunk[0].offset = i64::try_from(chunk_start).unwrap() - 1;
            let mut past_chunk = pages.clone();
            past_chunk[1].offset = i64::try_from(chunk_start + chunk_len).unwrap();
            let mut not_increasing = pages.clone();
            not_increasing[1].first_row_index = 0;
            let mut past_group = pages.clone();
            past_group[1].first_row_index = loaded.row_group(0).num_rows();
            for contradicting in [
                first_row_one,
                negative_offset,
                negative_size,
                before_chunk,
                past_chunk,
                not_increasing,
                past_group,
                Vec::new(),
            ] {
                let patched = with_pages(&loaded, leaf, contradicting);
                assert_eq!(
                    fault_of(check_offset_index(&path, &patched, leaf)),
                    FileFault::InconsistentOffsetIndex { row_group: 0 }
                );
            }
            assert!(check_offset_index(&path, &with_pages(&loaded, leaf, pages), leaf).is_ok());
        }
    }

    /// The bytes of the fixture's key column chunks, as `build` fetches
    /// them for verification.
    async fn key_chunks(
        opened: &Opened,
        metadata: &ParquetMetaData,
        key_leaf: usize,
    ) -> Arc<Segments> {
        let ranges = key_chunk_ranges(metadata, key_leaf);
        let parts = fetch_chunk_parts(&opened.handle, &ranges).await.unwrap();
        Arc::new(Segments::new(parts, opened.handle.size()))
    }

    /// A key page whose header counts other rows than the span its offset
    /// index gives it is refused at build, whether the row group's count
    /// shortens the last page's span or a shifted first row moves a page
    /// boundary; without the check, the second footer would read row 999 as
    /// page 1's first row and serve row 1000's value for it.
    #[tokio::test]
    async fn test_verify_refuses_a_key_page_whose_rows_differ_from_its_offset_index() {
        let opened = store(&Fixture::contract(KeyType::Long, contract_rows())).await;
        let loaded = opened.handle.load_metadata().await.unwrap();
        let key_leaf = 0;
        let chunks = key_chunks(&opened, &loaded, key_leaf).await;
        let path = location();
        let verify_with = |metadata: &ParquetMetaData| {
            let column = Column {
                metadata,
                leaf: key_leaf,
                path: &path,
            };
            verify(TABLE, &column, KeyShape::Long, &chunks).map(|verified| verified.groups.len())
        };
        assert_eq!(verify_with(&loaded).unwrap(), ROW_GROUPS);
        // A row group without rows, and so without pages, reads no page and
        // holds no keys; the other row groups verify as before.
        let empty_group = with_pages(&with_group_rows(&loaded, 0, 0), key_leaf, Vec::new());
        assert_eq!(verify_with(&empty_group).unwrap(), ROW_GROUPS - 1);

        let short_group = with_group_rows(&loaded, 0, PAGE_ROWS + PAGE_ROWS / 2);
        assert_eq!(
            fault_of(verify_with(&short_group)),
            FileFault::PageRowsMismatch {
                row_group: 0,
                page: 1,
            }
        );
        let mut shifted = pages_of(&loaded, key_leaf);
        shifted[1].first_row_index -= 1;
        let shifted = with_pages(&loaded, key_leaf, shifted);
        assert_eq!(
            fault_of(verify_with(&shifted)),
            FileFault::PageRowsMismatch {
                row_group: 0,
                page: 0,
            }
        );
    }

    /// The same check on the lookup path, for the one key page a find
    /// decodes and the one value page a read fetches: a page boundary the
    /// offset index has shifted by one row is a fault, never a neighbour's
    /// row or value.
    #[tokio::test]
    async fn test_lookup_refuses_a_page_whose_rows_differ_from_its_offset_index() {
        let opened = store(&Fixture::contract(KeyType::Long, contract_rows())).await;
        let index = opened.index(KeyType::Long).await;
        let (key_leaf, value_leaf) = (index.key_leaf(), index.value_leaf());
        let kept = (**index.metadata()).clone();
        let last_of_page_zero = page_rows() - 1;

        let mut shifted_values = pages_of(&kept, value_leaf);
        shifted_values[1].first_row_index -= 1;
        let index = index.with_metadata(with_pages(&kept, value_leaf, shifted_values));
        let row = index
            .find(&key_for(last_of_page_zero, KeyType::Long), None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row, row_of(last_of_page_zero, group_rows()));
        assert_eq!(
            fault_of(index.read_value(row, None).await),
            FileFault::PageRowsMismatch {
                row_group: 0,
                page: 1,
            }
        );

        let mut shifted_keys = pages_of(&kept, key_leaf);
        shifted_keys[1].first_row_index -= 1;
        let index = index.with_metadata(with_pages(&kept, key_leaf, shifted_keys));
        assert_eq!(
            fault_of(
                index
                    .find(&key_for(last_of_page_zero, KeyType::Long), None)
                    .await
            ),
            FileFault::PageRowsMismatch {
                row_group: 0,
                page: 0,
            }
        );
        let restored = index.with_metadata(kept);
        assert_eq!(
            restored
                .find(&key_for(last_of_page_zero, KeyType::Long), None)
                .await
                .unwrap(),
            Some(row)
        );
    }

    #[tokio::test]
    async fn test_build_zero_rows_is_a_file_fault() {
        let opened = store(&Fixture::with_keys(KeyColumn::Int64, Vec::new())).await;
        assert_eq!(
            fault_of(opened.build(KeyType::Long, Residency::Full, None).await),
            FileFault::NoRows
        );
    }

    #[tokio::test]
    async fn test_build_drops_the_value_column_index_and_resident_bytes_track_key_bytes() {
        let rows = contract_rows();
        let mut fixture = Fixture::contract(KeyType::Long, rows);
        fixture.values = (0..rows)
            .map(|i| Some(value_of_len(i, LARGE_VALUE_BYTES)))
            .collect();
        fixture.layout.page_bytes = usize::try_from(VALUE_PAGE_BYTES).unwrap();
        fixture.layout.write_batch = SMALL_BATCH;
        let opened = store(&fixture).await;
        let loaded = opened.handle.load_metadata().await.unwrap();
        let index = opened.index(KeyType::Long).await;
        let kept = index.metadata();
        let mut value_pages = 0;
        for row_group in 0..ROW_GROUPS {
            let column_index = &kept.column_index().unwrap()[row_group];
            assert_eq!(column_index[index.value_leaf()], ColumnIndexMetaData::NONE);
            assert!(matches!(
                column_index[index.key_leaf()],
                ColumnIndexMetaData::INT64(_)
            ));
            assert!(matches!(
                loaded.column_index().unwrap()[row_group][index.value_leaf()],
                ColumnIndexMetaData::BYTE_ARRAY(_)
            ));
            value_pages += kept.offset_index().unwrap()[row_group][index.value_leaf()]
                .page_locations()
                .len();
        }
        assert!(
            value_pages > rows / page_rows() * PAGES_PER_GROUP,
            "{value_pages} value pages"
        );
        // The dropped index held a truncated minimum and maximum per value page.
        let truncated = DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH.unwrap();
        assert!(loaded.memory_size() - kept.memory_size() >= value_pages * 2 * truncated);
        let key_chunk_bytes: u64 = kept
            .row_groups()
            .iter()
            .map(|group| group.column(index.key_leaf()).byte_range().1)
            .sum();
        let resident = count(index.resident_bytes());
        assert!(
            resident <= RESIDENT_TO_KEY_CHUNK_FACTOR * key_chunk_bytes,
            "{resident} resident bytes against {key_chunk_bytes} key chunk bytes"
        );
        assert!(count(loaded.memory_size()) > RESIDENT_TO_KEY_CHUNK_FACTOR * key_chunk_bytes);
        // The budget check at the footer stage holds the footer as loaded;
        // its prediction is the kept footer's, and what the index reports.
        let from_loaded = FileIndex::predicted_resident_bytes(&loaded, index.key_leaf());
        assert_eq!(from_loaded, predicted_of(&index));
        assert_eq!(from_loaded.full, resident);
        assert!(from_loaded.full <= RESIDENT_TO_KEY_CHUNK_FACTOR * key_chunk_bytes);
    }

    /// The Bloom tier's costs on a `MANY_ROW_GROUPS` contract fixture: a
    /// cold hit is exactly two GETs, a warm hit none, a miss the filter
    /// rules out none and one it passes one, a key between two row groups
    /// or outside the range none, with `page_of` naming a page exactly when
    /// `find` fetches one. At least one probed miss is a filter negative, so
    /// the no-GET branch is known to have run.
    async fn check_bloom_costs(key_type: KeyType) {
        let rows = contract_rows();
        let mut fixture = Fixture::contract(key_type, rows);
        fixture.layout.row_group_rows = rows / MANY_ROW_GROUPS;
        let opened = store(&fixture).await;
        let index = opened
            .build(key_type, Residency::Bloom, None)
            .await
            .unwrap();
        let stats = opened.stats();
        assert_eq!(index.residency(), Residency::Bloom);
        assert_eq!(
            stats.gets(),
            1 + 2 * count(MANY_ROW_GROUPS),
            "footer, chunks, filters"
        );
        let filters = opened.filters(&index);
        let cache = cache().await;
        let group_rows = rows / MANY_ROW_GROUPS;

        let before = stats.gets();
        let hit = group_rows + page_rows() + 1;
        let row = index
            .find(&key_for(hit, key_type), Some(&cache))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row, row_of(hit, group_rows));
        assert_eq!(stats.gets(), before + 1, "a positive fetches its key page");
        let value = index.read_value(row, Some(&cache)).await.unwrap();
        assert_eq!(value.as_deref(), Some(value_for(hit).as_slice()));
        assert_eq!(stats.gets(), before + 2, "a cold hit is two GETs");
        let warm = hit + 1;
        let row = index
            .find(&key_for(warm, key_type), Some(&cache))
            .await
            .unwrap()
            .unwrap();
        let value = index.read_value(row, Some(&cache)).await.unwrap();
        assert_eq!(value.as_deref(), Some(value_for(warm).as_slice()));
        assert_eq!(stats.gets(), before + 2, "a warm hit is none");

        let mut negatives = 0;
        for i in [0, page_rows() - 1, group_rows - 1, group_rows, rows - 2] {
            let key = absent_after(&key_for(i, key_type));
            // The key after a row group's last key falls between two row
            // groups and never reaches the filter.
            let between_groups = (i + 1) % group_rows == 0;
            let passes = filters[i / group_rows].check(&*key.as_bytes());
            let fetches_page = !between_groups && passes;
            negatives += usize::from(!between_groups && !passes);
            assert_eq!(index.page_of(&key).is_some(), fetches_page, "{key}");
            let before = stats.gets();
            assert_eq!(index.find(&key, Some(&cache)).await.unwrap(), None, "{key}");
            assert_eq!(
                stats.gets(),
                before + u64::from(fetches_page),
                "{key}: the filter decides"
            );
        }
        assert!(negatives >= 1, "no probed miss was a filter negative");
        let before = stats.gets();
        for key in [below_range(key_type), above_range(key_type)] {
            assert_eq!(index.page_of(&key), None);
            assert_eq!(index.find(&key, Some(&cache)).await.unwrap(), None);
        }
        assert_eq!(
            stats.gets(),
            before,
            "a key outside the range never reaches the filter"
        );
        assert_eq!(stats.local_reads(), 0);
    }

    #[tokio::test]
    async fn test_bloom_tier_costs_per_type() {
        for key_type in KEY_TYPES {
            check_bloom_costs(key_type).await;
        }
    }

    #[tokio::test]
    async fn test_bloom_tier_refuses_a_file_without_a_filter_that_full_serves() {
        let mut fixture = Fixture::contract(KeyType::Long, FEW_ROWS);
        fixture.layout.bloom = false;
        let opened = store(&fixture).await;
        assert_eq!(
            refusal_of(opened.build(KeyType::Long, Residency::Bloom, None).await),
            RefusalReason::NoBloomFilter {
                path: location(),
                row_group: 0,
            }
        );
        assert_eq!(
            opened.stats().gets(),
            1,
            "refused at the footer, no chunk fetched"
        );
        let index = opened.index(KeyType::Long).await;
        assert_eq!(index.residency(), Residency::Full);
        assert_eq!(predicted_of(&index).bloom, None);
    }

    /// `predicted_resident_bytes` of a built index's own footer.
    fn predicted_of(index: &FileIndex) -> ResidentBytes {
        FileIndex::predicted_resident_bytes(index.metadata(), index.key_leaf())
    }

    /// `rows` binary keys that do not compress: digests of the row number,
    /// sorted. The Bloom tier saves memory on keys shaped like these (RFD 017).
    fn random_binary_keys(rows: usize) -> Vec<Option<Key>> {
        let mut digests: Vec<Vec<u8>> = (0..rows)
            .map(|i| crate::local::digest(&i.to_le_bytes()).to_vec())
            .collect();
        digests.sort();
        digests
            .into_iter()
            .map(|digest| Some(Key::Bytes(digest)))
            .collect()
    }

    #[tokio::test]
    async fn test_bloom_tier_holds_no_key_bytes_after_build() {
        let rows = contract_rows();
        let opened = store(&Fixture::with_keys(
            KeyColumn::Binary,
            random_binary_keys(rows),
        ))
        .await;
        let full = opened.index(KeyType::Binary).await;
        let bloom = opened
            .build(KeyType::Binary, Residency::Bloom, None)
            .await
            .unwrap();
        assert!(
            bloom.resident_bytes() < full.resident_bytes(),
            "{} bloom against {} full",
            bloom.resident_bytes(),
            full.resident_bytes()
        );
        let predicted = predicted_of(&full);
        assert_eq!(count(full.resident_bytes()), predicted.full);
        assert_eq!(count(bloom.resident_bytes()), predicted.bloom.unwrap());
        let stats = opened.stats();
        let before = stats.gets();
        let chunks = bloom.key_chunks_for_verification().await.unwrap();
        assert_eq!(
            stats.gets(),
            before + count(ROW_GROUPS),
            "the chunks are fetched again"
        );
        assert_eq!(
            chunks.resident_len(),
            full.key_chunks_for_verification()
                .await
                .unwrap()
                .resident_len()
        );
        assert_eq!(
            stats.gets(),
            before + count(ROW_GROUPS),
            "full hands out its resident chunks"
        );
    }

    #[tokio::test]
    async fn test_bloom_tier_corrupt_filter_is_a_file_fault() {
        let fixture = Fixture::contract(KeyType::Long, FEW_ROWS);
        let file = fixture.write();
        let metadata = ParquetMetaDataReader::new()
            .parse_and_finish(&file)
            .unwrap();
        let key_leaf = 0;
        let offset = usize::try_from(
            metadata
                .row_group(0)
                .column(key_leaf)
                .bloom_filter_offset()
                .unwrap(),
        )
        .unwrap();
        let mut corrupted = file.to_vec();
        corrupted[offset..offset + GARBAGE_BYTES].fill(GARBAGE);
        let opened = store_file(Bytes::from(corrupted)).await;
        assert_eq!(
            fault_of(opened.build(KeyType::Long, Residency::Bloom, None).await),
            FileFault::CorruptBloomFilter { row_group: 0 }
        );
        let index = opened.index(KeyType::Long).await;
        assert_eq!(index.residency(), Residency::Full);
    }

    #[tokio::test]
    async fn test_bloom_ranges_without_a_filter_length_is_a_file_fault() {
        let opened = store(&Fixture::contract(KeyType::Long, FEW_ROWS)).await;
        let index = opened.index(KeyType::Long).await;
        let key_leaf = index.key_leaf();
        let mut builder = (**index.metadata()).clone().into_builder();
        let mut groups = builder.take_row_groups();
        let group = groups.remove(0);
        let mut columns = group.columns().to_vec();
        columns[key_leaf] = columns[key_leaf]
            .clone()
            .into_builder()
            .set_bloom_filter_length(None)
            .build()
            .unwrap();
        groups.insert(
            0,
            group
                .into_builder()
                .set_column_metadata(columns)
                .build()
                .unwrap(),
        );
        let patched = builder.set_row_groups(groups).build();
        let result = bloom_ranges(TABLE, &opened.handle, &patched, key_leaf);
        let Err(Error::File { path, fault }) = result else {
            panic!("expected a file fault, got {result:?}");
        };
        assert_eq!(path, location());
        assert_eq!(fault, FileFault::UnsizedBloomFilter { row_group: 0 });
        assert_eq!(
            FileIndex::predicted_resident_bytes(&patched, key_leaf).bloom,
            None
        );
        assert!(bloom_ranges(TABLE, &opened.handle, index.metadata(), key_leaf).is_ok());
    }

    #[tokio::test]
    async fn test_disk_tier_lookup_costs() {
        let rows = contract_rows();
        let dir = tempfile::tempdir().unwrap();
        let opened = store(&Fixture::contract(KeyType::Long, rows)).await;
        let stats = opened.stats();
        let local = local_dir(dir.path(), opened.stats());
        let index = opened
            .build(KeyType::Long, Residency::Disk, Some(&local))
            .await
            .unwrap();
        assert_eq!(index.residency(), Residency::Disk);
        assert_eq!(
            stats.gets(),
            1 + count(ROW_GROUPS),
            "footer, then the chunks"
        );
        assert_eq!(
            stats.local_reads(),
            0,
            "a fresh directory has nothing to read"
        );
        assert_eq!(
            count(index.resident_bytes()),
            predicted_of(&index).disk_memory
        );

        let cache = cache().await;
        let (gets, reads) = (stats.gets(), stats.local_reads());
        let hit = group_rows() + 1;
        let row = index
            .find(&key_for(hit, KeyType::Long), Some(&cache))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row, row_of(hit, group_rows()));
        assert_eq!(
            (stats.gets(), stats.local_reads()),
            (gets, reads + 1),
            "a find is one local read"
        );
        let value = index.read_value(row, Some(&cache)).await.unwrap();
        assert_eq!(value.as_deref(), Some(value_for(hit).as_slice()));
        assert_eq!(
            (stats.gets(), stats.local_reads()),
            (gets + 1, reads + 1),
            "a cold hit adds one GET"
        );
        let warm = hit + 1;
        let row = index
            .find(&key_for(warm, KeyType::Long), Some(&cache))
            .await
            .unwrap()
            .unwrap();
        let value = index.read_value(row, Some(&cache)).await.unwrap();
        assert_eq!(value.as_deref(), Some(value_for(warm).as_slice()));
        assert_eq!(
            (stats.gets(), stats.local_reads()),
            (gets + 1, reads + 2),
            "a warm hit is one local read for the key page and no GET"
        );
        let key = absent_after(&key_for(hit, KeyType::Long));
        assert_eq!(index.find(&key, Some(&cache)).await.unwrap(), None);
        assert_eq!(
            (stats.gets(), stats.local_reads()),
            (gets + 1, reads + 3),
            "a miss inside the range is one local read"
        );
        assert_eq!(
            index
                .find(&below_range(KeyType::Long), Some(&cache))
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            (stats.gets(), stats.local_reads()),
            (gets + 1, reads + 3),
            "a miss outside costs nothing"
        );
    }

    #[tokio::test]
    async fn test_disk_tier_reuses_a_matching_chunk_file_and_rewrites_a_stale_one() {
        let rows = contract_rows();
        let dir = tempfile::tempdir().unwrap();
        let opened = store(&Fixture::contract(KeyType::Long, rows)).await;
        let stats = opened.stats();
        let local = local_dir(dir.path(), opened.stats());
        let index = opened
            .build(KeyType::Long, Residency::Disk, Some(&local))
            .await
            .unwrap();
        assert_eq!(stats.gets(), 1 + count(ROW_GROUPS));
        let cache = cache().await;
        let hit = group_rows() + 1;

        // A restart: a fresh handle over the same bytes, whose footer digest
        // is computed anew, finds the chunk file by identity.
        let reads = stats.local_reads();
        let restarted = store_file(opened.file.clone()).await;
        let again = restarted
            .build(KeyType::Long, Residency::Disk, Some(&local))
            .await
            .unwrap();
        assert_eq!(restarted.stats().gets(), 1, "the footer only");
        assert_eq!(
            stats.local_reads(),
            reads + count(ROW_GROUPS),
            "the chunks come from the chunk file"
        );
        assert_eq!(again.key_range(), index.key_range());
        assert_eq!(again.fences(), index.fences());
        let row = again
            .find(&key_for(hit, KeyType::Long), Some(&cache))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row, row_of(hit, group_rows()));

        // Another file at the same location, whose size and footer differ:
        // the chunk file on disk is stale for it, so the chunks are fetched
        // and the file rewritten, and a build over the original file after
        // that fetches again.
        let reads = stats.local_reads();
        let other = store(&Fixture::contract(KeyType::Long, rows + page_rows())).await;
        assert_ne!(other.file.len(), opened.file.len());
        let rewritten = other
            .build(KeyType::Long, Residency::Disk, Some(&local))
            .await
            .unwrap();
        assert_eq!(
            other.stats().gets(),
            1 + count(ROW_GROUPS + 1),
            "footer, then every chunk of the other file"
        );
        assert_eq!(stats.local_reads(), reads, "nothing was read back");
        assert_eq!(rewritten.rows(), i64::try_from(rows + page_rows()).unwrap());
        let original = store_file(opened.file.clone()).await;
        original
            .build(KeyType::Long, Residency::Disk, Some(&local))
            .await
            .unwrap();
        assert_eq!(
            original.stats().gets(),
            1 + count(ROW_GROUPS),
            "the chunk file now holds the other file's chunks"
        );
        assert_eq!(stats.local_reads(), reads);
    }

    #[tokio::test]
    async fn test_disk_tier_without_a_local_dir_is_refused_at_the_footer() {
        let opened = store(&Fixture::contract(KeyType::Long, FEW_ROWS)).await;
        assert_eq!(
            refusal_of(opened.build(KeyType::Long, Residency::Disk, None).await),
            RefusalReason::NoLocalDir
        );
        assert_eq!(opened.stats().gets(), 1, "no chunk fetched");
    }

    #[tokio::test]
    async fn test_page_of_names_the_pages_find_decodes_and_read_value_fetches() {
        let rows = contract_rows();
        let opened = store(&Fixture::contract(KeyType::String, rows)).await;
        let index = opened.index(KeyType::String).await;
        let offset_index = index.metadata().offset_index().unwrap();
        for i in [0, 1, page_rows() - 1, page_rows(), group_rows(), rows - 1] {
            let key = key_for(i, KeyType::String);
            let page = index.page_of(&key).unwrap();
            let row = index.find(&key, None).await.unwrap().unwrap();
            assert_eq!(page.row_group, row.row_group);
            assert!(
                index
                    .page_rows(page.row_group, page.key_page)
                    .contains(&row.row)
            );
            let value_pages = offset_index[row.row_group][index.value_leaf()].page_locations();
            let holding = value_pages
                .iter()
                .rev()
                .find(|location| location.first_row_index <= row.row)
                .unwrap();
            assert_eq!(
                page.value_page_offset,
                Some(u64::try_from(holding.offset).unwrap())
            );
            let stats = opened.stats();
            let before = stats.bytes();
            index.read_value(row, None).await.unwrap();
            assert_eq!(
                stats.bytes() - before,
                u64::try_from(holding.compressed_page_size).unwrap()
            );
        }
        let absent = absent_after(&key_for(page_rows(), KeyType::String));
        let page = index.page_of(&absent).unwrap();
        assert_eq!(
            (page.row_group, page.key_page, page.value_page_offset),
            (0, 1, None)
        );
    }

    #[tokio::test]
    async fn test_page_of_names_no_value_page_in_the_disk_and_bloom_tiers() {
        let dir = tempfile::tempdir().unwrap();
        let opened = store(&Fixture::contract(KeyType::Long, contract_rows())).await;
        for residency in [Residency::Disk, Residency::Bloom] {
            let index = opened
                .build_in(KeyType::Long, residency, dir.path())
                .await
                .unwrap();
            let key = key_for(group_rows() + page_rows(), KeyType::Long);
            assert_eq!(
                index.page_of(&key),
                Some(PageRef {
                    row_group: 1,
                    key_page: 1,
                    value_page_offset: None,
                })
            );
            assert_eq!(index.page_of(&above_range(KeyType::Long)), None);
        }
    }

    #[tokio::test]
    async fn test_key_chunks_for_verification_decodes_the_same_keys_in_every_tier() {
        let rows = contract_rows();
        let dir = tempfile::tempdir().unwrap();
        let opened = store(&Fixture::contract(KeyType::Uuid, rows)).await;
        let stats = opened.stats();
        for residency in TIERS {
            let index = opened
                .build_in(KeyType::Uuid, residency, dir.path())
                .await
                .unwrap();
            let (gets, reads) = (stats.gets(), stats.local_reads());
            let chunks = index.key_chunks_for_verification().await.unwrap();
            let expected = match residency {
                Residency::Full => (gets, reads),
                Residency::Disk => (gets, reads + count(ROW_GROUPS)),
                Residency::Bloom => (gets + count(ROW_GROUPS), reads),
            };
            assert_eq!((stats.gets(), stats.local_reads()), expected, "{residency}");
            let mut decoded = 0;
            for row_group in 0..ROW_GROUPS {
                let keys = index.decode_keys(row_group, None, &chunks).unwrap();
                assert_eq!(keys.key_type(), KeyType::Uuid);
                assert_eq!(keys.len(), group_rows());
                assert!(keys.is_strictly_increasing());
                for at in 0..keys.len() {
                    assert_eq!(
                        keys.get(at),
                        key_for(row_group * group_rows() + at, KeyType::Uuid)
                    );
                    decoded += 1;
                }
                let page = index.decode_keys(row_group, Some(1), &chunks).unwrap();
                assert_eq!(page.len(), page_rows());
                assert_eq!(page.get(0), keys.get(page_rows()));
            }
            assert_eq!(decoded, rows);
        }
    }

    #[tokio::test]
    async fn test_predicted_resident_bytes_agrees_with_resident_bytes_per_tier_and_type() {
        let rows = contract_rows();
        let dir = tempfile::tempdir().unwrap();
        for key_type in KEY_TYPES {
            let opened = store(&Fixture::contract(key_type, rows)).await;
            for residency in TIERS {
                let index = opened
                    .build_in(key_type, residency, dir.path())
                    .await
                    .unwrap();
                let predicted = predicted_of(&index);
                let expected = match residency {
                    Residency::Full => predicted.full,
                    Residency::Disk => predicted.disk_memory,
                    Residency::Bloom => predicted.bloom.unwrap(),
                };
                // A uuid key is held inline; the estimate counts the column's width per page.
                let fence_over = match key_type {
                    KeyType::Uuid => count(ROW_GROUPS * PAGES_PER_GROUP * UUID_BYTES),
                    KeyType::Long | KeyType::String | KeyType::Binary => 0,
                };
                assert_eq!(
                    count(index.resident_bytes()) + fence_over,
                    expected,
                    "{key_type} {residency}"
                );
                assert!(predicted.disk_bytes > 0 && predicted.disk_memory < predicted.full);
                assert!(predicted.bloom_bits_per_key.unwrap() > 0.0);
            }
            let stats = opened.stats();
            let local = local_dir(dir.path(), opened.stats());
            let disk = opened
                .build(key_type, Residency::Disk, Some(&local))
                .await
                .unwrap();
            let before = stats.local_bytes();
            disk.key_chunks_for_verification().await.unwrap();
            assert_eq!(stats.local_bytes() - before, predicted_of(&disk).disk_bytes);
        }
    }

    #[tokio::test]
    async fn test_predicted_resident_bytes_without_a_column_index_is_within_the_fence_estimate() {
        let rows = contract_rows();
        let mut fixture = Fixture::contract(KeyType::String, rows);
        fixture.layout.statistics = EnabledStatistics::None;
        let opened = store(&fixture).await;
        let index = opened.index(KeyType::String).await;
        let predicted = predicted_of(&index).full;
        let resident = count(index.resident_bytes());
        let pages = count(ROW_GROUPS * PAGES_PER_GROUP);
        let bound = pages * count(index.max_key_bytes());
        assert!(
            predicted.abs_diff(resident) <= bound,
            "{predicted} predicted, {resident} resident"
        );
    }

    #[tokio::test]
    async fn test_read_value_row_outside_every_page_is_a_file_fault() {
        let opened = store(&Fixture::contract(KeyType::Long, FEW_ROWS)).await;
        let index = opened.index(KeyType::Long).await;
        let rows = i64::try_from(FEW_ROWS).unwrap();
        for row in [
            RowRef {
                row_group: 0,
                row: rows,
            },
            RowRef {
                row_group: 0,
                row: -1,
            },
            RowRef {
                row_group: 1,
                row: 0,
            },
        ] {
            let result = index.read_value(row, None).await;
            let Err(Error::File { path, fault }) = result else {
                panic!("expected a file fault, got {result:?}");
            };
            assert_eq!(path, location());
            assert_eq!(
                fault,
                FileFault::PageOutOfRange {
                    row: row.row,
                    row_group: row.row_group,
                }
            );
        }
        assert_eq!(opened.stats().gets(), 2, "nothing was fetched for them");
    }

    #[test]
    fn test_keys_search_compare_and_order_per_arm() {
        let text = |value: &str| ByteArray::from(value.as_bytes().to_vec());
        let runs = [
            Keys::Long(vec![1, 3, 5]),
            Keys::Str(vec![text("a"), text("b"), text("é")]),
            Keys::Bin(vec![
                ByteArray::from(vec![1]),
                ByteArray::from(vec![1, 1]),
                ByteArray::from(vec![2]),
            ]),
            Keys::Uuid(vec![[0; UUID_BYTES], [1; UUID_BYTES], [2; UUID_BYTES]]),
        ];
        for run in &runs {
            assert_eq!(run.len(), 3);
            assert!(!run.is_empty());
            assert!(run.is_strictly_increasing());
            for at in 0..run.len() {
                assert_eq!(run.binary_search(&run.get(at)), Ok(at));
                assert_eq!(run.cmp_at(at, run, at), std::cmp::Ordering::Equal);
            }
            assert_eq!(run.cmp_at(0, run, 2), std::cmp::Ordering::Less);
            assert_eq!(run.binary_search(&absent_after(&run.get(0))), Err(1));
            for other in &runs {
                if other.key_type() != run.key_type() {
                    assert_eq!(run.binary_search(&other.get(1)), Err(0));
                }
            }
        }
        assert_eq!(runs[0].max_len(), size_of::<i64>());
        assert_eq!(runs[1].max_len(), "é".len());
        assert_eq!(runs[2].max_len(), 2);
        assert_eq!(runs[3].max_len(), UUID_BYTES);
        assert_eq!(Keys::Long(Vec::new()).max_len(), 0);
        assert!(!Keys::Long(vec![1, 1]).is_strictly_increasing());
        assert!(!Keys::Str(vec![text("b"), text("a")]).is_strictly_increasing());
        assert_eq!(
            Keys::Long(vec![1, 2, 2, 3]).first_not_increasing(),
            Some((2, std::cmp::Ordering::Equal))
        );
        assert_eq!(
            Keys::Str(vec![text("a"), text("c"), text("b")]).first_not_increasing(),
            Some((2, std::cmp::Ordering::Less))
        );
        assert_eq!(
            Keys::Uuid(vec![[1; UUID_BYTES], [1; UUID_BYTES]]).first_not_increasing(),
            Some((1, std::cmp::Ordering::Equal))
        );
        assert_eq!(runs[1].get(2), Key::Str("é".to_string()));
    }

    #[tokio::test]
    async fn test_file_index_and_its_futures_are_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        fn assert_send<T: Send>(_: &T) {}
        assert_send_sync::<FileIndex>();
        assert_send_sync::<Keys>();
        let opened = store(&Fixture::contract(KeyType::Long, FEW_ROWS)).await;
        let index = opened.index(KeyType::Long).await;
        let cache = cache().await;
        let key = key_for(1, KeyType::Long);
        let find = index.find(&key, Some(&cache));
        assert_send(&find);
        let row = find.await.unwrap().unwrap();
        let read = index.read_value(row, Some(&cache));
        assert_send(&read);
        assert_eq!(
            read.await.unwrap().as_deref(),
            Some(value_for(1).as_slice())
        );
        let chunks = index.key_chunks_for_verification();
        assert_send(&chunks);
        chunks.await.unwrap();
    }
}
