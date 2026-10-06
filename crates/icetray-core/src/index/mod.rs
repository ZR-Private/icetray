//! The per-file resident index (RFD 004, RFD 006, RFD 017): the footer and
//! page indexes as kept, the exact first and last key of the file and of
//! every row group, the fence array (the first key of every key page), and
//! per residency tier the key chunks in memory, a handle on them in a local
//! chunk file, or the key column's bloom filters. A lookup chooses its row
//! group and key page from those exact keys and never from column
//! statistics, decodes exactly one key page with parquet's column reader
//! over bytes it already holds, and fetches at most one value page through
//! the page cache; a miss inside the file's range fetches nothing.

mod build;
mod inspect;
mod page_header;
mod pages;
mod plain;
mod record;
#[cfg(test)]
pub(crate) mod testing;

use std::cmp::Ordering;
use std::mem::size_of;
use std::ops::Range;
use std::panic::UnwindSafe;
use std::sync::Arc;

use bytes::Bytes;
use parquet::basic::Type as PhysicalType;
use parquet::bloom_filter::Sbbf;
use parquet::data_type::{ByteArray, ByteArrayType, FixedLenByteArrayType};
use parquet::file::metadata::{ColumnChunkMetaData, ParquetMetaData};
use parquet::file::page_index::offset_index::PageLocation;
use parquet::schema::types::ColumnDescPtr;

use crate::approx;
use crate::cache::{PageCache, PageOrigin};
use crate::config::Residency;
use crate::deletes::DeleteFault;
use crate::error::{Error, Recurrence, Recurs, Result};
use crate::key::{Key, KeyType, UUID_BYTES};
use crate::local::{DataFileIdentity, DiskChunks};
use crate::parquet_io::{FileHandle, Segments};
use build::{check_utf8, chunks_from_disk, decode_run, fetch_chunk_parts, read_one};
pub(crate) use inspect::Inspected;
use inspect::{Layout, ResidentSet, kept_footer, key_chunk_ranges, tiers};
pub(crate) use page_header::check_chunk_page_headers;
use page_header::check_value_page_headers;
pub use page_header::{ChunkPage, HeaderField, MOST_VALUE_PAGE_BYTES, PageHeaderFault, WireType};

/// A row's position in a file: its row group and its row within that group.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct RowRef {
    /// The row group holding the row.
    pub row_group: usize,
    /// The row's index within the row group.
    pub row: i64,
}

/// A row's position in a data file counted from the file's first row
/// across every row group: how Iceberg's position deletes and deletion
/// vectors name a row (RFD 021). A newtype, so that it is never taken for a
/// row within one row group.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct RowPosition(pub(crate) u64);

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
    /// the column chunk the pages lie in has a negative offset or size or
    /// ends past the file's last byte, a row group with rows has no page, a page has a negative offset or
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
    /// A key page's header, or that of a value page or value dictionary
    /// page a lookup fetched, does not decode as the compact-thrift
    /// `PageHeader` parquet reads it as, or is not a header of the page its
    /// place in the chunk holds; checked before parquet reads it (RFD 004).
    #[error("the header of {page} of row group {row_group} does not decode: {fault}")]
    MalformedPageHeader {
        /// The row group holding the page.
        row_group: usize,
        /// The page.
        page: ChunkPage,
        /// What is wrong with its header.
        fault: PageHeaderFault,
    },
    /// A page's header declares more than its page can hold: for a key
    /// page, an uncompressed size above what its rows could encode to at
    /// the key cap or above the key page ceiling of 64 MiB; for a value
    /// page or value dictionary page a lookup fetched, an uncompressed size
    /// above the value page ceiling of 64 MiB; for either, a dictionary of
    /// more values than the row group has rows or than the ceiling holds at
    /// the larger of the fewest bytes an entry encodes to and the bytes
    /// parquet holds a decoded entry in, a compressed size past the page's
    /// place in the chunk, or v2 level lengths whose sum is above the
    /// uncompressed size. Parquet sizes a buffer by the first two before it
    /// reads a byte of the page, and adds the level lengths as `i32`s, so
    /// the header is refused before parquet reads it (RFD 004).
    #[error(
        "{page} of row group {row_group} declares {field} {declared}, above the {allowed} it can hold"
    )]
    PageHeaderOverBound {
        /// The row group holding the page.
        row_group: usize,
        /// The page.
        page: ChunkPage,
        /// The header field that declares too much.
        field: HeaderField,
        /// What the header declares.
        declared: u64,
        /// The most the page can hold.
        allowed: u64,
    },
    /// A row position lies outside every page of its row group.
    #[error("row {row} is outside every page of row group {row_group}")]
    PageOutOfRange {
        /// The row position that no page covers.
        row: i64,
        /// The row group it was looked for in.
        row_group: usize,
    },
    /// A bloom filter that does not decode from exactly the bytes the
    /// footer bounds, or decodes to a bitset of no block.
    #[error("bloom filter of row group {row_group} does not decode")]
    CorruptBloomFilter {
        /// The row group whose filter is unreadable.
        row_group: usize,
    },
    /// The footer names a row group's bloom filter but places it nowhere the
    /// file holds: a negative offset, no length, a negative length, or an
    /// end past the file's last byte, so the filter cannot be read as one
    /// range of the file. A footer with no filter at all is a refusal, not a
    /// fault.
    #[error("bloom filter of row group {row_group} has no place the footer can hold")]
    UnplaceableBloomFilter {
        /// The row group whose filter the footer cannot place.
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
    /// The value column's physical type is neither `BYTE_ARRAY` nor
    /// `FIXED_LEN_BYTE_ARRAY`. The snapshot's schema admitted the column as
    /// binary, fixed, or string (`TableLayout::derive` refuses anything else
    /// as `RefusalReason::ValueTypeUnsupported`), and a spec-conformant
    /// writer stores those three as byte arrays, so a file that does not is
    /// malformed.
    #[error("value column is stored as {got}, not a byte array")]
    ValuePhysicalType {
        /// The physical type the file stores the column as.
        got: PhysicalType,
    },
    /// The value column is a `FIXED_LEN_BYTE_ARRAY` of width 0, which holds
    /// no byte of any value and on which parquet's decoder asserts a width
    /// above zero when it decodes a value, so a lookup would panic there.
    #[error("value column is a FIXED_LEN_BYTE_ARRAY of width 0")]
    ZeroWidthValue,
    /// Parquet panicked decoding part of the file. Parquet panics, in
    /// release builds too, on malformed input no check ahead of it reads:
    /// a byte-array page whose body holds fewer values than its header
    /// counts, among others. The footer load, a bloom filter's parse, and
    /// the decode of a column chunk's pages run under `catch_unwind`, so
    /// such a panic ends that load or that lookup with this fault instead
    /// of the task running it.
    #[error("parquet panicked decoding {part}: {message}")]
    DecoderPanicked {
        /// What parquet was decoding.
        part: DecodedPart,
        /// The panic's message, for a person reading the log; nothing
        /// decides on it.
        message: String,
    },
    /// A delete file, or a data file's deletes, is unservable (RFD 021).
    #[error("{0}")]
    Delete(DeleteFault),
}

/// A fault the footer shows, or a bloom filter read from the data file,
/// is fixed for the file. A fault met decoding a page is not known to be:
/// the page may be a key chunk the Disk tier reads back from its chunk
/// file, for the overlap check or a lookup, with no checksum on that read,
/// and a chunk file changed on disk is not the data file. The fault does
/// not say which bytes it met.
impl Recurs for FileFault {
    fn recurrence(&self) -> Recurrence {
        match self {
            Self::MissingColumn(_)
            | Self::MissingPageIndex
            | Self::NoRows
            | Self::InconsistentRowCounts
            | Self::InconsistentOffsetIndex { .. }
            | Self::UnplaceableBloomFilter { .. }
            | Self::ValuePhysicalType { .. }
            | Self::ZeroWidthValue
            // Bloom filters are read from the data file at every load and
            // never kept on local disk.
            | Self::CorruptBloomFilter { .. }
            // Parquet panics on the same bytes the same way, and the footer,
            // the bloom filters, and a position delete file are always the
            // file's own bytes.
            | Self::DecoderPanicked {
                part:
                    DecodedPart::Footer
                    | DecodedPart::BloomFilter { .. }
                    | DecodedPart::PositionDeletes,
                ..
            } => Recurrence::Deterministic,
            Self::Delete(fault) => fault.recurrence(),
            Self::PageRowsMismatch { .. }
            | Self::MalformedPageHeader { .. }
            | Self::PageHeaderOverBound { .. }
            | Self::KeyNotUtf8 { .. }
            | Self::DecoderPanicked {
                part: DecodedPart::Pages { .. },
                ..
            }
            // A row a key page found, and that page may be a local copy.
            | Self::PageOutOfRange { .. } => Recurrence::Transient,
        }
    }
}

/// The part of a file parquet was decoding when it panicked.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecodedPart {
    /// The footer: the thrift metadata and the page indexes.
    Footer,
    /// The key column's bloom filter of a row group.
    BloomFilter {
        /// The row group whose filter was decoding.
        row_group: usize,
    },
    /// Pages of a column chunk.
    Pages {
        /// The row group holding the chunk.
        row_group: usize,
        /// The last page the page reader took up for the column reader
        /// before the panic; `None` when it had taken up none.
        page: Option<ChunkPage>,
    },
    /// A Parquet position delete file, read whole.
    PositionDeletes,
}

impl std::fmt::Display for DecodedPart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Footer => f.write_str("the footer"),
            Self::BloomFilter { row_group } => {
                write!(f, "the bloom filter of row group {row_group}")
            }
            Self::Pages {
                row_group,
                page: Some(page),
            } => write!(f, "row group {row_group} at {page}"),
            Self::Pages {
                row_group,
                page: None,
            } => write!(f, "row group {row_group} before its first page"),
            Self::PositionDeletes => f.write_str("the position delete file"),
        }
    }
}

/// Every tier's resident bytes for one file, from the footer alone (RFD
/// 017), so the budget check and the table owner see the numbers before any
/// key chunk is fetched, and the two parts the tiers differ by. `bloom`,
/// `filter_bytes`, and `bloom_bits_per_key` are `None` when a row group
/// lacks a key bloom filter or the footer does not bound one. Serializes
/// with these field names: the doctor's summary carries it.
#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize)]
pub struct ResidentBytes {
    /// Footer, page indexes, key chunks, and fences.
    pub full: u64,
    /// Footer, page indexes, and fences: the memory the disk tier keeps.
    pub disk_memory: u64,
    /// The chunk file the disk tier writes, its envelope and header and the
    /// key chunks: what `LocalConfig::index_budget_bytes` is charged for the
    /// file, so a budget sized from this figure admits the load.
    pub disk_bytes: u64,
    /// Footer, page indexes, filters, and fences.
    pub bloom: Option<u64>,
    /// Filter bits per row of the file; the bloom tier's miss cost follows it.
    pub bloom_bits_per_key: Option<f64>,
    /// The key column's compressed chunk bytes: what the Full tier holds
    /// beyond the Disk tier's memory, and the chunk file's payload.
    pub key_chunk_bytes: u64,
    /// The key column's bloom filter bytes as the footer bounds them: what
    /// the Bloom tier holds beyond the Disk tier's memory.
    pub filter_bytes: Option<u64>,
}

impl ResidentBytes {
    /// The memory `residency` holds: `full`, `disk_memory`, or `bloom`, the
    /// one mapping from a tier to its field, so a budget check and a test
    /// name the tier and not the field. `None` only for `Bloom` when
    /// `bloom` is.
    pub fn in_tier(&self, residency: Residency) -> Option<u64> {
        match residency {
            Residency::Full => Some(self.full),
            Residency::Disk => Some(self.disk_memory),
            Residency::Bloom => self.bloom,
        }
    }
}

/// Filter bits per key: `bytes` of filter over `rows` keys, the unit RFD
/// 016 judges a filter's size by and `ResidentBytes::bloom_bits_per_key`
/// reports. Finite, zero for no rows.
pub(crate) fn bits_per_key(bytes: u64, rows: u64) -> f64 {
    approx::ratio(bytes.saturating_mul(u64::from(u8::BITS)), rows)
}

/// A total of per-row-group or per-file predictions that stays at
/// `u64::MAX` where the plain sum would overflow. A footer that lies can
/// saturate one file's prediction (see `ResidentSet`), so a total over two
/// such files would otherwise panic in a debug build and wrap to a small
/// number in a release one.
pub(crate) fn saturating_sum(values: impl IntoIterator<Item = u64>) -> u64 {
    values.into_iter().fold(0, u64::saturating_add)
}

/// `saturating_sum` of predictions any of which may be absent, as a
/// missing bloom filter makes one: `None` when any value is.
pub(crate) fn saturating_sum_all(values: impl IntoIterator<Item = Option<u64>>) -> Option<u64> {
    values
        .into_iter()
        .collect::<Option<Vec<u64>>>()
        .map(saturating_sum)
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
    /// The position of `key`'s row in the file, under the same terms as
    /// `value_page_offset`: `Some` only where the key page is resident and
    /// holds the key.
    pub(crate) position: Option<RowPosition>,
}

/// A decoded run of keys in file order, one arm per key type, holding the
/// values parquet decoded so that a run of a thousand keys costs no
/// allocation per key. Order and search compare the served order of
/// `Key`: longs numerically, everything else as unsigned bytes.
#[derive(Clone, Debug)]
pub(crate) enum Keys {
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
    /// The number of keys.
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Long(run) => run.len(),
            Self::Str(run) | Self::Bin(run) => run.len(),
            Self::Uuid(run) => run.len(),
        }
    }

    /// Whether the run holds no key.
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The run's first key; `None` for an empty run.
    pub(crate) fn first(&self) -> Option<Key> {
        (!self.is_empty()).then(|| self.get(0))
    }

    /// The run's last key; `None` for an empty run.
    pub(crate) fn last(&self) -> Option<Key> {
        self.len().checked_sub(1).map(|at| self.get(at))
    }

    /// The key at `at`, which is below `len`.
    pub(crate) fn get(&self, at: usize) -> Key {
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
    pub(crate) fn binary_search(&self, key: &Key) -> std::result::Result<usize, usize> {
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
    pub(crate) fn cmp_at(&self, at: usize, other: &Self, other_at: usize) -> Ordering {
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
    pub(crate) fn max_len(&self) -> usize {
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

    /// A `Long` run's values, for the buffer they were decoded into to be
    /// reused; empty for another arm.
    pub(crate) fn into_longs(self) -> Vec<i64> {
        match self {
            Self::Long(run) => run,
            Self::Str(_) | Self::Bin(_) | Self::Uuid(_) => Vec::new(),
        }
    }

    /// A `Str` or `Bin` run's values, for the buffer they were decoded
    /// into to be reused; empty for another arm.
    pub(crate) fn into_byte_arrays(self) -> Vec<ByteArray> {
        match self {
            Self::Str(run) | Self::Bin(run) => run,
            Self::Long(_) | Self::Uuid(_) => Vec::new(),
        }
    }

    /// The position of the first key that is not above the one before it,
    /// with how it compares to that key: `Equal` for a duplicate, `Less`
    /// for a key out of order. `None` for a strictly increasing run.
    pub(crate) fn first_not_increasing(&self) -> Option<(usize, Ordering)> {
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
    /// `FIXED_LEN_BYTE_ARRAY` of at least one byte holding opaque bytes.
    FixedBin,
    /// `FIXED_LEN_BYTE_ARRAY` of `UUID_BYTES`.
    Uuid,
}

impl KeyShape {
    /// The key type the shape decodes keys of.
    fn key_type(self) -> KeyType {
        match self {
            Self::Long => KeyType::Long,
            Self::Str => KeyType::String,
            Self::Bin | Self::FixedBin => KeyType::Binary,
            Self::Uuid => KeyType::Uuid,
        }
    }
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
    /// Per row group, the position in the file of its first row: the rows
    /// of every earlier row group, summed once at build so that `position`
    /// is one addition.
    group_starts: Vec<RowPosition>,
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
    /// `BYTE_ARRAY` and a `FIXED_LEN_BYTE_ARRAY` at least one byte wide,
    /// `Uuid` a `FIXED_LEN_BYTE_ARRAY` of `UUID_BYTES`), in the Bloom tier a row
    /// group without a key bloom filter, then, once the chunks are decoded, a null key, a
    /// key below the one before it or a row group starting below the
    /// previous one's last key, a key repeated, and a key over
    /// `contract::MAX_KEY_BYTES`. A file that is malformed for serving is
    /// `Error::File`: a missing column, a value column that is neither
    /// `BYTE_ARRAY` nor `FIXED_LEN_BYTE_ARRAY` (the schema admitted its
    /// Iceberg type, so the file contradicts it) or is a
    /// `FIXED_LEN_BYTE_ARRAY` of width 0, no rows, row counts that
    /// disagree, no offset index on either column or one that does not
    /// describe its pages, an unbounded or undecodable bloom filter, a
    /// string key that is not UTF-8, a key page header parquet would not
    /// read to the numbers the index reads or that declares more than its
    /// page can hold, a key page holding other rows than its offset index
    /// says, a footer, key page, or bloom filter parquet panics decoding
    /// (`FileFault::DecoderPanicked`). Footer checks run before any chunk is
    /// fetched; `inspect` is that stage on its own. The Disk tier with no
    /// local directory is `Error::Config` with
    /// `ConfigViolation::DiskResidencyWithoutLocalDir`, before any chunk is
    /// fetched.
    ///
    /// Full keeps the chunks in memory. Disk trusts a chunk file in `local`
    /// whose identity, verification context, and payload checksum match
    /// and which stores exactly the key chunks, taking what verification
    /// established from the record it stores (no GET, no chunk read, no key
    /// decoded), or fetches the chunks, verifies them, and writes them with
    /// that record; it keeps no key bytes in memory. Bloom fetches and
    /// verifies the chunks, drops them, and keeps the filters. Every tier
    /// keeps the footer, both page indexes of the key column, every
    /// column's offset index, and the fences; the column index of every
    /// column but the key, the value column's above all, is never resident
    /// (RFD 017).
    ///
    /// Only tests call this: the loader runs `inspect` and
    /// `Inspected::build` apart, so its budget check sits between them.
    #[cfg(test)]
    pub(crate) async fn build(
        table: &str,
        handle: Arc<FileHandle>,
        key_column: &str,
        value_column: &str,
        key_type: KeyType,
        residency: Residency,
        local: Option<&Arc<crate::local::LocalDir>>,
    ) -> Result<Self> {
        Self::inspect(table, handle, key_column, value_column, key_type)
            .await?
            .build(residency, local, None)
            .await
    }

    /// The footer stage of `build`, on its own so that a caller can check
    /// every file's predicted resident bytes against a budget before any key
    /// chunk is fetched (RFD 017): loads the footer (one GET), makes every
    /// footer check `build` documents, trims the footer to what the index
    /// keeps, and computes the prediction. Nothing past the footer is read.
    pub(crate) async fn inspect(
        table: &str,
        handle: Arc<FileHandle>,
        key_column: &str,
        value_column: &str,
        key_type: KeyType,
    ) -> Result<Inspected> {
        let loaded = handle.load_metadata().await?;
        let layout = Layout::inspect(table, &handle, &loaded, key_column, value_column, key_type)?;
        let metadata = Arc::new(kept_footer(&loaded, layout.key_leaf));
        drop(loaded);
        let predicted =
            Self::predicted_resident_bytes(&metadata, layout.key_leaf, key_type, &handle);
        tracing::debug!(
            path = %handle.location,
            full = predicted.full,
            disk_memory = predicted.disk_memory,
            disk_bytes = predicted.disk_bytes,
            bloom = ?predicted.bloom,
            "predicted resident bytes"
        );
        Ok(Inspected {
            table: table.to_string(),
            handle,
            metadata,
            layout,
            predicted,
            reuse: None,
        })
    }

    /// Every tier's resident bytes for the file `handle` opens, from its
    /// footer alone (RFD 017), the same given the footer as loaded or as
    /// `FileIndex` keeps it (`metadata()`): the column index of every column
    /// but the key is never resident and is not counted either way, so a
    /// budget check at the footer stage predicts what `resident_bytes` will
    /// report. Exact: the footer and the page indexes kept
    /// (`ParquetMetaData::memory_size` of the footer as kept), the key
    /// chunks (the footer's compressed sizes, which Full holds and Disk
    /// writes byte for byte; a chunk the footer cannot place, by a negative
    /// field or an end past the file's size, counts as zero, and `inspect`
    /// refuses such a footer before any budget check reads this), and the
    /// filters (the footer's lengths, header included; the in-memory filter
    /// drops the header, a few bytes per row group; `bloom` is `None` when a
    /// row group's filter is absent or one a Bloom-tier `build` refuses as
    /// `FileFault::UnplaceableBloomFilter`), and the chunk file (the
    /// envelope and the header the Disk tier writes for the handle's
    /// identity over the key chunks, the verification record, then the
    /// chunks; the identity carries the footer digest, so `handle` has
    /// loaded its footer), except that the record, a key per key page, is
    /// exact only for keys of one width (longs, uuids, a fixed-width binary
    /// column) and estimated as the fences are for string and binary keys,
    /// `key_type` saying which. Every total saturates at `u64::MAX` rather
    /// than wrapping. Estimated: the fences, one `Key` per key page plus, for
    /// `BYTE_ARRAY` keys, the page's minimum from the column index when the
    /// file has one, which a writer may truncate, else the chunk's mean
    /// encoded bytes per value, and for `FIXED_LEN_BYTE_ARRAY` keys the
    /// column's width, which over-counts a uuid key, held inline, by
    /// `UUID_BYTES` per page. `resident_bytes` shares the definitions.
    pub(crate) fn predicted_resident_bytes(
        metadata: &ParquetMetaData,
        key_leaf: usize,
        key_type: KeyType,
        handle: &FileHandle,
    ) -> ResidentBytes {
        tiers(&ResidentSet::from_footer(
            metadata, key_leaf, key_type, handle, None,
        ))
    }

    /// The tier the index was built in.
    pub(crate) fn residency(&self) -> Residency {
        match self.keys {
            KeyResidency::Chunks(_) => Residency::Full,
            KeyResidency::Disk(_) => Residency::Disk,
            KeyResidency::Filters(_) => Residency::Bloom,
        }
    }

    /// The file the index describes.
    pub(crate) fn handle(&self) -> &Arc<FileHandle> {
        &self.handle
    }

    /// The footer as kept: both page indexes of the key column, every
    /// column's offset index, and no other column's column index.
    pub fn metadata(&self) -> &Arc<ParquetMetaData> {
        &self.metadata
    }

    /// Rows in the file; above zero.
    pub(crate) fn rows(&self) -> i64 {
        self.rows
    }

    /// The key type the file's keys were verified as.
    pub(crate) fn key_type(&self) -> KeyType {
        self.key_shape.key_type()
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
    pub(crate) fn fences(&self) -> &[Vec<Key>] {
        &self.fences
    }

    /// Key pages in `row_group`, which is below the footer's row group count.
    pub(crate) fn page_count(&self, row_group: usize) -> usize {
        self.fences[row_group].len()
    }

    /// The length in bytes of the file's longest key; at most
    /// `contract::MAX_KEY_BYTES`.
    pub(crate) fn max_key_bytes(&self) -> usize {
        self.max_key_bytes
    }

    /// The exact first and last key of the file, decoded at build; never a
    /// truncated bound.
    pub fn key_range(&self) -> (&Key, &Key) {
        (&self.key_range.0, &self.key_range.1)
    }

    /// Whether `key` lies within the file's exact key range.
    pub(crate) fn may_contain(&self, key: &Key) -> bool {
        let (first, last) = self.key_range();
        first <= key && key <= last
    }

    /// Bytes held in memory for this file: the footer and page indexes as
    /// kept, the tier's structure (chunks, filters, or nothing for Disk),
    /// and the fences, under the definitions of `predicted_resident_bytes`
    /// with the fences exact.
    pub(crate) fn resident_bytes(&self) -> usize {
        let all = tiers(&ResidentSet::from_footer(
            &self.metadata,
            self.key_leaf,
            self.key_type(),
            &self.handle,
            Some(&self.fences),
        ));
        let mine = all.in_tier(self.residency()).expect(
            "a bloom index has a bounded filter per row group; build refused the file otherwise",
        );
        approx::length(mine)
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
    pub(crate) async fn find(
        &self,
        key: &Key,
        cache: Option<&PageCache>,
    ) -> Result<Option<RowRef>> {
        let Some((row_group, page)) = self.locate(key) else {
            return Ok(None);
        };
        // Only the value page decides a lookup's label; a key page's origin
        // is not kept.
        let chunk = self.key_column().group(row_group);
        let segments = match &self.keys {
            KeyResidency::Chunks(chunks) => Arc::clone(chunks),
            KeyResidency::Disk(disk) => {
                let (segments, _origin) = self
                    .fetch_pages(chunk, page, PageSource::Disk(disk))
                    .await?;
                segments
            }
            KeyResidency::Filters(filters) => {
                if !filters[row_group].check(&*key.as_bytes()) {
                    return Ok(None);
                }
                let (segments, _origin) = self
                    .fetch_pages(chunk, page, PageSource::Store(cache))
                    .await?;
                segments
            }
        };
        self.search(key, row_group, page, segments)
    }

    /// The value at `row` and where its page came from: the one value page
    /// holding it through `cache` (one GET cold, none warm) and, when the
    /// value column chunk has a dictionary page, that page too (the one
    /// extra GET of RFD 007's penalty), then exactly one row decoded.
    /// `Some` carries the bytes unchanged, a slice of the fetched page where
    /// parquet decoded them in place and a copy otherwise (`value_bytes`);
    /// `None` is a null value, distinct from an absent key. The origin is
    /// the value page's: `Fetched` for the GET of a cold hit, `Cached` for a warm one; the dictionary page, when there
    /// is one, does not decide it. A row outside every page of its row group
    /// is `FileFault::PageOutOfRange`. The headers of the value page and of
    /// its dictionary page are read from the fetched bytes before parquet
    /// decodes them, and one that parquet would not read to the same
    /// numbers or that declares more than its page can hold is
    /// `FileFault::MalformedPageHeader` or `FileFault::PageHeaderOverBound`
    /// (`check_value_page_headers`). A page parquet panics decoding is
    /// `FileFault::DecoderPanicked`.
    pub(crate) async fn read_value(
        &self,
        row: RowRef,
        cache: Option<&PageCache>,
    ) -> Result<(Option<Bytes>, PageOrigin)> {
        let page = self.value_page(row)?;
        let chunk = self.value_column().group(row.row_group);
        let (segments, origin) = self
            .fetch_pages(chunk, page, PageSource::Store(cache))
            .await?;
        check_value_page_headers(chunk, self.value_shape, page, &segments)?;
        let value = match self.value_shape {
            ValueShape::Variable => {
                read_one::<ByteArrayType>(chunk, row.row, Arc::clone(&segments))?
                    .map(|value| value_bytes(&segments, value.data()))
            }
            ValueShape::Fixed => {
                read_one::<FixedLenByteArrayType>(chunk, row.row, Arc::clone(&segments))?
                    .map(|value| value_bytes(&segments, value.data()))
            }
        };
        Ok((value, origin))
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
        let row = match &self.keys {
            KeyResidency::Chunks(chunks) => self
                .search(key, row_group, key_page, Arc::clone(chunks))
                .expect("build decoded this key page from these resident bytes and checked it against its offset index; decoding it again cannot fail"),
            KeyResidency::Disk(_) | KeyResidency::Filters(_) => None,
        };
        Some(PageRef {
            row_group,
            key_page,
            value_page_offset: row
                .and_then(|row| self.value_page_index(row))
                .map(|page| self.value_column().group(row_group).page_range(page).start),
            position: row.map(|row| self.position(row)),
        })
    }

    /// The position in the file of the first row of key page `page` of
    /// `row_group`, which is below `page_count(row_group)`.
    pub(crate) fn key_page_start(&self, row_group: usize, page: usize) -> RowPosition {
        self.position(RowRef {
            row_group,
            row: self.key_column().group(row_group).page_rows(page).start,
        })
    }

    /// The position of `row` in the file: the rows of every earlier row
    /// group, then the row within its own.
    pub(crate) fn position(&self, row: RowRef) -> RowPosition {
        RowPosition(self.group_starts[row.row_group].0 + approx::count(row_index(row.row)))
    }

    /// The key column of `row_group` decoded from `chunks`, or one page of
    /// it; no I/O, and a byte not in `chunks` is a `Segments` fault inside
    /// `Error::Parquet`. `row_group` is below the footer's row group count
    /// and `page` below `page_count(row_group)`.
    pub(crate) fn decode_keys(
        &self,
        row_group: usize,
        page: Option<usize>,
        chunks: &Segments,
    ) -> Result<Keys> {
        let chunk = self.key_column().group(row_group);
        let rows = match page {
            Some(page) => chunk.page_rows(page),
            None => 0..chunk.rows(),
        };
        let keys = decode_run(chunk, self.key_shape, rows, Arc::new(chunks.clone()))?;
        check_utf8(chunk, &keys)?;
        Ok(keys)
    }

    /// The key column chunks for overlap verification, which decodes every
    /// key of the file: Full hands out the resident chunks at no cost; Disk
    /// reads every chunk back from the chunk file, one counted local read
    /// per row group and no GET; Bloom holds no key bytes and fetches them
    /// again from storage, one counted load GET per row group.
    pub(crate) async fn key_chunks_for_verification(&self) -> Result<Arc<Segments>> {
        match &self.keys {
            KeyResidency::Chunks(chunks) => Ok(Arc::clone(chunks)),
            KeyResidency::Disk(disk) => {
                let ranges = key_chunk_ranges(&self.metadata, self.key_leaf, self.handle.size());
                chunks_from_disk(disk, &ranges, self.handle.size()).await
            }
            KeyResidency::Filters(_) => {
                let ranges = key_chunk_ranges(&self.metadata, self.key_leaf, self.handle.size());
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
        let chunk = self.key_column().group(row_group);
        let rows = chunk.page_rows(page);
        let keys = decode_run(chunk, self.key_shape, rows.clone(), segments)?;
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
            .group(row.row_group)
            .page_locations()
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

    /// Page `page` of `chunk` and, when the column chunk has a dictionary
    /// page, that page too, from `source`, as the `Segments`
    /// the column reader decodes from without further I/O. The dictionary
    /// page is found the way the reader finds it, as the gap between the
    /// chunk's start and its first data page, so the reader asks for exactly
    /// what is held. Both ranges are known before either read starts, so
    /// the two reads run together: the dictionary costs its GET or local
    /// read, never a second round trip's wait. The origin returned is the
    /// page's own; the dictionary page's is not kept.
    async fn fetch_pages(
        &self,
        chunk: ColumnChunk<'_>,
        page: usize,
        source: PageSource<'_>,
    ) -> Result<(Arc<Segments>, PageOrigin)> {
        let range = chunk.page_range(page);
        let dictionary = chunk.dictionary_range();
        let read_dictionary = async {
            match dictionary.clone() {
                Some(dictionary) => self
                    .read_from(source, dictionary)
                    .await
                    .map(|(bytes, _origin)| Some(bytes)),
                None => Ok(None),
            }
        };
        let ((page_bytes, origin), dictionary_bytes) =
            futures::try_join!(self.read_from(source, range.clone()), read_dictionary)?;
        let mut parts = Vec::with_capacity(2);
        parts.push((range, page_bytes));
        if let (Some(dictionary), Some(bytes)) = (dictionary, dictionary_bytes) {
            parts.push((dictionary, bytes));
        }
        Ok((Arc::new(Segments::new(parts, self.handle.size())), origin))
    }

    /// One byte range of the file from `source`: a hedged lookup GET through
    /// the cache, or one counted local read. A chunk-file read is `Cached`:
    /// it cost no GET, which is what the origin distinguishes.
    async fn read_from(
        &self,
        source: PageSource<'_>,
        range: Range<u64>,
    ) -> Result<(Bytes, PageOrigin)> {
        match source {
            PageSource::Store(cache) => self.handle.read_page(range, cache).await,
            PageSource::Disk(disk) => disk
                .page(range)
                .await
                .map(|bytes| (bytes, PageOrigin::Cached)),
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

impl<'a> Column<'a> {
    fn descriptor(&self) -> ColumnDescPtr {
        self.metadata
            .file_metadata()
            .schema_descr()
            .column(self.leaf)
    }

    /// The column's chunk in `row_group`, which is below the footer's row
    /// group count. Every question about one row group is asked of the
    /// chunk, so the index is named once and cannot drift between calls.
    fn group(self, row_group: usize) -> ColumnChunk<'a> {
        ColumnChunk {
            column: self,
            row_group,
        }
    }
}

/// One column chunk: a `Column` in one row group, the row group index
/// carried once for every per-row-group read and check.
#[derive(Clone, Copy)]
struct ColumnChunk<'a> {
    column: Column<'a>,
    row_group: usize,
}

impl<'a> ColumnChunk<'a> {
    /// The row group the chunk lies in; what a fault names.
    fn row_group(&self) -> usize {
        self.row_group
    }

    /// The chunk's entry in the footer.
    fn metadata(&self) -> &'a ColumnChunkMetaData {
        self.column
            .metadata
            .row_group(self.row_group)
            .column(self.column.leaf)
    }

    fn rows(&self) -> i64 {
        self.column.metadata.row_group(self.row_group).num_rows()
    }

    /// The chunk's page locations; build required them.
    fn page_locations(&self) -> &'a [PageLocation] {
        self.column
            .metadata
            .offset_index()
            .expect("build required an offset index on the key and value columns")[self.row_group]
            [self.column.leaf]
            .page_locations()
    }

    fn page_range(&self, page: usize) -> Range<u64> {
        page_range(&self.page_locations()[page])
    }

    /// The dictionary page's range when the chunk has one: the bytes between
    /// the chunk's start and its first data page.
    fn dictionary_range(&self) -> Option<Range<u64>> {
        let (start, _) = self.metadata().byte_range();
        let first = page_range(self.page_locations().first()?).start;
        (first != start).then_some(start..first)
    }

    /// The rows page `page` holds, within the row group.
    fn page_rows(&self, page: usize) -> Range<i64> {
        let locations = self.page_locations();
        let start = locations[page].first_row_index;
        let end = locations
            .get(page + 1)
            .map_or_else(|| self.rows(), |next| next.first_row_index);
        start..end
    }

    /// The page holding row `row`, which is inside the row group; the last
    /// page for a row at or past its end.
    fn page_holding(&self, row: usize) -> usize {
        let locations = self.page_locations();
        locations
            .partition_point(|location| row_index(location.first_row_index) <= row)
            .saturating_sub(1)
            .min(locations.len().saturating_sub(1))
    }

    /// Whether the data pages a reader took from the chunk are the ones
    /// the offset index names for rows `rows`, each holding the rows the
    /// index gives it: `seen` is the value count in the header of every
    /// data page the reader read, in order, starting at the page holding
    /// the first row (no page before it is read), and `decoded` is how many
    /// rows it produced, or the error the decode ended in. No rows wanted
    /// means no page read. A disagreement is `FileFault::PageRowsMismatch`
    /// naming the first page that does not fit. The header counts are
    /// checked before a decode error is returned, so a page that ran out
    /// of values short of the count its header claims is refused as the
    /// mismatch it is.
    fn check_pages_read(
        &self,
        rows: Range<usize>,
        seen: &[usize],
        decoded: parquet::errors::Result<usize>,
    ) -> Result<()> {
        let (first, expected) = if rows.is_empty() {
            (0, 0)
        } else {
            let first = self.page_holding(rows.start);
            let last = self.page_holding(rows.end - 1);
            (first, last - first + 1)
        };
        for (page, values) in (first..first + expected).zip(seen) {
            let span = self.page_rows(page);
            if *values != row_index(span.end - span.start) {
                return Err(self.page_rows_mismatch(page));
            }
        }
        let decoded = decoded?;
        if seen.len() != expected {
            return Err(self.page_rows_mismatch(first + seen.len().min(expected.saturating_sub(1))));
        }
        if decoded != rows.end - rows.start {
            return Err(self.page_rows_mismatch(self.page_holding(rows.start + decoded)));
        }
        Ok(())
    }

    /// `FileFault::PageRowsMismatch` for data page `page` of the chunk.
    fn page_rows_mismatch(&self, page: usize) -> Error {
        file_fault(
            self.column.path,
            FileFault::PageRowsMismatch {
                row_group: self.row_group,
                page,
            },
        )
    }
}

/// A decoded value as `Bytes`: a slice of the fetched page in `segments`
/// when parquet decoded it in place there, as it does from an uncompressed
/// page, so the value shares the page's buffer and holds it only while the
/// value is referenced; otherwise a copy, since a value decoded from a
/// compressed page lies in the buffer the page was decompressed into, and a
/// slice of it would hold the whole decompressed page for as long as the
/// value is referenced.
fn value_bytes(segments: &Segments, value: &[u8]) -> Bytes {
    segments
        .parts()
        .iter()
        .map(|(_, bytes)| bytes)
        .find(|bytes| lies_within(bytes, value))
        .map_or_else(
            || Bytes::copy_from_slice(value),
            |bytes| bytes.slice_ref(value),
        )
}

/// Whether `inner` is a subslice of `outer`'s memory.
fn lies_within(outer: &[u8], inner: &[u8]) -> bool {
    let outer = outer.as_ptr_range();
    let inner = inner.as_ptr_range();
    outer.start <= inner.start && inner.end <= outer.end
}

/// The identity a chunk file is named by and checked against, for a handle
/// whose footer has been loaded.
pub(crate) fn identity_of(handle: &FileHandle) -> DataFileIdentity {
    DataFileIdentity {
        path: handle.location.clone(),
        size: handle.size(),
        footer_digest: handle
            .footer_digest()
            .expect("load_metadata set the footer digest before the footer was inspected"),
    }
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

/// Per row group of `metadata`, the position in the file of its first row,
/// for `FileIndex::position`. Inspect checked every row count non-negative
/// and their sum within `i64`, so no sum overflows.
fn group_starts(metadata: &ParquetMetaData) -> Vec<RowPosition> {
    metadata
        .row_groups()
        .iter()
        .scan(0, |before, group| {
            let start = RowPosition(*before);
            *before += approx::count(row_index(group.num_rows()));
            Some(start)
        })
        .collect()
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

/// `decode`, a synchronous call that hands bytes of the file at `path` to
/// parquet, with a panic inside it as `FileFault::DecoderPanicked` at the
/// part `part` names once the panic is caught. The caller states why
/// `decode` is unwind-safe: what it mutates is dropped with the panic, or
/// left whole.
pub(crate) fn unless_panicked<T>(
    path: &str,
    decode: impl FnOnce() -> T + UnwindSafe,
    part: impl FnOnce() -> DecodedPart,
) -> Result<T> {
    std::panic::catch_unwind(decode).map_err(|payload| decoder_panicked(path, part(), &*payload))
}

/// `FileFault::DecoderPanicked` at `part` of the file at `path`, for a
/// panic whose payload is `payload`.
pub(crate) fn decoder_panicked(
    path: &str,
    part: DecodedPart,
    payload: &(dyn std::any::Any + Send),
) -> Error {
    let message = payload
        .downcast_ref::<&str>()
        .map(|message| (*message).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "a panic that carries no message".to_string());
    file_fault(path, FileFault::DecoderPanicked { part, message })
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

    use parquet::file::page_index::column_index::ColumnIndexMetaData;

    use super::*;
    use crate::approx::count;
    use crate::contract::PAGE_ROWS;
    use crate::generation::testing::{self, Dictionary, KeyColumn, cache, column_of, page_rows};
    use crate::index::page_header::{MOST_VALUE_PAGE_BYTES, PageBody, PageHeader};
    use crate::index::testing::{
        BINARY_KEY_TAG, FEW_ROWS, FIXED_KEY_BYTES, Fixture, INFLATED_GROUP_ROWS, Opened,
        PAGES_PER_GROUP, ROW_GROUPS, SMALL_BATCH, VALUE_BYTES, ValueColumn, absent_after,
        contract_rows, fault_of, group_rows, header_at, key_for, location, moved_by_header,
        pages_of, predicted_of, row_of, stepped, store, store_file, value_for, value_of_len,
        with_dictionary_one_entry_short, with_group_rows, with_header_claims, with_pages,
    };
    use crate::storage::IoStats;

    use parquet::basic::{Compression, ZstdLevel};
    use parquet::file::properties::{EnabledStatistics, WriterVersion};

    /// Row groups in the fixtures that route across truncated or absent
    /// statistics, so an earlier row group's bound covers later ones.
    const MANY_ROW_GROUPS: usize = 4;

    /// Bytes per value in the misaligned fixture: enough that the page byte
    /// bound splits value pages far more often than the row limit.
    const MISALIGNED_VALUE_BYTES: usize = 512;
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
    /// Every `NULL_EVERY`th value of the nullable fixture is null.
    const NULL_EVERY: usize = 7;
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

    /// Whether `value` lies in the memory of one of the bytes `segments`
    /// holds.
    fn shares_a_buffer(segments: &Segments, value: &Bytes) -> bool {
        segments
            .parts()
            .iter()
            .any(|(_, bytes)| lies_within(bytes, value))
    }

    /// A value read from an uncompressed value page is a slice of the page
    /// the cache holds, not a copy; one read from a zstd page, the
    /// contract's codec, is a copy out of the buffer the page was
    /// decompressed into, and shares no fetched page.
    #[tokio::test]
    async fn test_read_value_shares_the_buffer_of_an_uncompressed_page() {
        for (compression, shared) in [
            (Compression::UNCOMPRESSED, true),
            (Compression::ZSTD(ZstdLevel::default()), false),
        ] {
            let mut fixture = Fixture::contract(KeyType::Long, contract_rows());
            fixture.layout.compression = compression;
            let opened = store(&fixture).await;
            let index = opened.index(KeyType::Long).await;
            let cache = cache().await;
            let at = page_rows() + 1;
            let row = index
                .find(&key_for(at, KeyType::Long), Some(&cache))
                .await
                .unwrap()
                .unwrap();
            let (value, _) = index.read_value(row, Some(&cache)).await.unwrap();
            let value = value.unwrap();
            assert_eq!(value, value_for(at).as_slice());
            let chunk = index.value_column().group(row.row_group);
            let page = index.value_page(row).unwrap();
            let (cached, origin) = index
                .fetch_pages(chunk, page, PageSource::Store(Some(&cache)))
                .await
                .unwrap();
            assert_eq!(origin, PageOrigin::Cached);
            assert_eq!(shares_a_buffer(&cached, &value), shared, "{compression}");
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
            let (value, origin) = index.read_value(row, Some(&cache)).await.unwrap();
            assert_eq!(value.as_deref(), Some(value_for(i).as_slice()), "key {key}");
            if cache.fetches() > fetched {
                assert!(cold);
                assert_eq!(origin, PageOrigin::Fetched, "key {key}");
                fetched += 1;
            } else {
                assert_eq!(origin, PageOrigin::Cached, "key {key}");
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
        approx::length(fetched)
    }

    #[tokio::test]
    async fn test_find_and_read_value_every_key_per_type() {
        for key_type in KeyType::ALL {
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
                assert_eq!(
                    index.key_column().group(row_group).page_rows(1),
                    PAGE_ROWS..PAGE_ROWS * 2
                );
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
        for key_type in KeyType::ALL {
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
            let (value, _) = index.read_value(row, Some(&cache)).await.unwrap();
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
        for residency in Residency::ALL {
            check_long_prefix_lookups(KeyType::String, residency).await;
        }
    }

    #[tokio::test]
    async fn test_find_long_prefix_binary_keys_across_truncated_row_group_statistics() {
        for residency in Residency::ALL {
            check_long_prefix_lookups(KeyType::Binary, residency).await;
        }
    }

    #[tokio::test]
    async fn test_find_every_key_with_statistics_disabled_across_row_groups_per_type() {
        for key_type in KeyType::ALL {
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
                let (value, _) = index.read_value(row, Some(&cache)).await.unwrap();
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
        let (value, origin) = index.read_value(row, Some(&cache)).await.unwrap();
        assert_eq!(value.as_deref(), Some(value_for(first).as_slice()));
        assert_eq!(origin, PageOrigin::Fetched);
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
        let (value, origin) = index.read_value(row, Some(&cache)).await.unwrap();
        assert_eq!(value.as_deref(), Some(value_for(second).as_slice()));
        assert_eq!(origin, PageOrigin::Cached);
        assert_eq!(stats.gets(), before + 2, "both pages cached");
        let other_page = 0;
        let row = index
            .find(&key_for(other_page, KeyType::Long), Some(&cache))
            .await
            .unwrap()
            .unwrap();
        let (_, origin) = index.read_value(row, Some(&cache)).await.unwrap();
        assert_eq!(origin, PageOrigin::Fetched);
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
        assert_eq!(
            index.read_value(row, Some(&cache)).await.unwrap(),
            (None, PageOrigin::Fetched)
        );
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
        let (value, origin) = index.read_value(row, Some(&cache)).await.unwrap();
        assert_eq!(value.as_deref(), Some(value_for(present).as_slice()));
        assert_eq!(origin, PageOrigin::Cached);
        assert_eq!(stats.gets(), before + 1, "and none warm");
        let row = index
            .find(&key_for(0, KeyType::Long), Some(&cache))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            index.read_value(row, Some(&cache)).await.unwrap(),
            (None, PageOrigin::Cached)
        );
        assert_eq!(stats.gets(), before + 1);
        // The last row of value page 0 (cached), the first row of value
        // page 1 (one GET), and the last row of the row group (page 1 again).
        for (i, extra_gets, origin) in [
            (page_rows() - 1, 0, PageOrigin::Cached),
            (page_rows(), 1, PageOrigin::Fetched),
            (group_rows() - 1, 1, PageOrigin::Cached),
        ] {
            let row = index
                .find(&key_for(i, KeyType::Long), Some(&cache))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                index.read_value(row, Some(&cache)).await.unwrap(),
                (None, origin),
                "row {i}"
            );
            assert_eq!(stats.gets(), before + 1 + extra_gets, "row {i}");
            let neighbor = i - 1;
            let row = index
                .find(&key_for(neighbor, KeyType::Long), Some(&cache))
                .await
                .unwrap()
                .unwrap();
            let (value, _) = index.read_value(row, Some(&cache)).await.unwrap();
            let expected = (!is_null(neighbor)).then(|| value_for(neighbor));
            assert_eq!(value.as_deref(), expected.as_deref(), "row {neighbor}");
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
        for residency in Residency::ALL {
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
            let (value, origin) = index.read_value(row, Some(&cache)).await.unwrap();
            assert_eq!(value.as_deref(), Some(value_for(first).as_slice()));
            assert_eq!(origin, PageOrigin::Fetched, "{residency}");
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

    #[tokio::test]
    async fn test_find_fixed_width_binary_keys_as_binary() {
        let keys: Vec<Option<Key>> = (0..FEW_ROWS)
            .map(|i| Some(Key::Bytes(count(stepped(i)).to_be_bytes().to_vec())))
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
            let (value, origin) = index.read_value(row, None).await.unwrap();
            assert_eq!(value.as_deref(), Some(value_for(i).as_slice()));
            assert_eq!(origin, PageOrigin::Fetched);
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
            let (value, origin) = index.read_value(row, None).await.unwrap();
            assert_eq!(value.as_deref(), Some(value_for(i).as_slice()));
            assert_eq!(origin, PageOrigin::Fetched);
        }
    }

    /// The same check on the lookup path, for the one key page a find
    /// decodes and the one value page a read fetches: a page boundary the
    /// offset index has shifted by one row is a fault, never a neighbor's
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
        let (value, origin) = index.read_value(row, Some(&cache)).await.unwrap();
        assert_eq!(value.as_deref(), Some(value_for(hit).as_slice()));
        assert_eq!(origin, PageOrigin::Fetched);
        assert_eq!(stats.gets(), before + 2, "a cold hit is two GETs");
        let warm = hit + 1;
        let row = index
            .find(&key_for(warm, key_type), Some(&cache))
            .await
            .unwrap()
            .unwrap();
        let (value, origin) = index.read_value(row, Some(&cache)).await.unwrap();
        assert_eq!(value.as_deref(), Some(value_for(warm).as_slice()));
        assert_eq!(origin, PageOrigin::Cached);
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
        for key_type in KeyType::ALL {
            check_bloom_costs(key_type).await;
        }
    }

    #[tokio::test]
    async fn test_disk_tier_lookup_costs() {
        let rows = contract_rows();
        let dir = tempfile::tempdir().unwrap();
        let opened = store(&Fixture::contract(KeyType::Long, rows)).await;
        let stats = opened.stats();
        let local = testing::local_dir(dir.path(), opened.stats(), None);
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
        let (value, origin) = index.read_value(row, Some(&cache)).await.unwrap();
        assert_eq!(value.as_deref(), Some(value_for(hit).as_slice()));
        assert_eq!(origin, PageOrigin::Fetched);
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
        let (value, origin) = index.read_value(row, Some(&cache)).await.unwrap();
        assert_eq!(value.as_deref(), Some(value_for(warm).as_slice()));
        assert_eq!(origin, PageOrigin::Cached);
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
            let position = RowPosition(approx::count(i));
            assert_eq!(
                index.position(row),
                position,
                "row {i} counts from the file's first row"
            );
            assert_eq!(page.position, Some(position));
            assert!(
                index
                    .key_column()
                    .group(page.row_group)
                    .page_rows(page.key_page)
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
            (
                page.row_group,
                page.key_page,
                page.value_page_offset,
                page.position
            ),
            (0, 1, None, None)
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
                    position: None,
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
        for residency in Residency::ALL {
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
                assert!(matches!(keys, Keys::Uuid(_)));
                assert_eq!(keys.len(), group_rows());
                assert_eq!(keys.first_not_increasing(), None);
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

    impl FileIndex {
        /// This index reading its pages from `handle`'s file over
        /// `metadata`, so a lookup meets page bytes the index was not built
        /// from.
        fn with_file(self, handle: Arc<FileHandle>, metadata: ParquetMetaData) -> Self {
            Self {
                handle,
                ..self.with_metadata(metadata)
            }
        }
    }

    /// `index`, built over `opened`'s file with footer `kept`, reading
    /// from a copy of the file in which the header of `page` of row group
    /// 0's value chunk claims each of `claims`, and over `kept` moved to
    /// match, its row group 0 claiming `group_rows` rows when that is
    /// `Some`. What lies past row group 0's value chunk in the file is not
    /// where that footer places it.
    async fn with_value_header(
        index: FileIndex,
        opened: &Opened,
        kept: &ParquetMetaData,
        page: ChunkPage,
        claims: &[(HeaderField, i32)],
        group_rows: Option<i64>,
    ) -> FileIndex {
        let value_leaf = index.value_leaf();
        let (chunk_start, _) = kept.row_group(0).column(value_leaf).byte_range();
        let at = approx::length(chunk_start) + header_at(kept, value_leaf, page);
        let patched = with_header_claims(&opened.file, at, claims);
        let grown = i64::try_from(patched.len() - opened.file.len()).unwrap();
        let moved = moved_by_header(kept, value_leaf, page, grown);
        let metadata = match group_rows {
            Some(rows) => with_group_rows(&moved, 0, rows),
            None => moved,
        };
        let reopened = store_file(Bytes::from(patched)).await;
        index.with_file(Arc::clone(&reopened.handle), metadata)
    }

    /// The header of `page` of row group 0's value chunk in `opened`'s file.
    fn value_header(index: &FileIndex, opened: &Opened, page: ChunkPage) -> PageHeader {
        let value_leaf = index.value_leaf();
        let metadata = index.metadata();
        let (chunk_start, _) = metadata.row_group(0).column(value_leaf).byte_range();
        let at = approx::length(chunk_start) + header_at(metadata, value_leaf, page);
        PageHeader::read(&opened.file[at..]).unwrap()
    }

    /// The fault `FileFault::PageHeaderOverBound` names for row group 0.
    fn over_bound(page: ChunkPage, field: HeaderField, declared: u64, allowed: u64) -> FileFault {
        FileFault::PageHeaderOverBound {
            row_group: 0,
            page,
            field,
            declared,
            allowed,
        }
    }

    /// A value page a lookup fetches whose header declares more than
    /// `MOST_VALUE_PAGE_BYTES` uncompressed, a compressed size past its
    /// place in the offset index, or v2 level lengths past its
    /// uncompressed size is refused as a file fault before parquet reads
    /// it: parquet would reserve the declared size, and add the level
    /// lengths as `i32`s. The same header rewritten to the numbers it held
    /// serves the value.
    #[tokio::test]
    async fn test_read_value_refuses_a_value_page_header_over_its_bounds() {
        let mut fixture = Fixture::contract(KeyType::Long, contract_rows());
        fixture.layout.writer_version = WriterVersion::PARQUET_2_0;
        let opened = store(&fixture).await;
        let page_at = 1;
        let page = ChunkPage::Data(page_at);
        let row = page_rows() * page_at;
        let at = row_of(row, group_rows());
        let index = opened.index(KeyType::Long).await;
        let written = value_header(&index, &opened, page);
        assert!(
            matches!(written.body, PageBody::DataV2 { .. }),
            "{:?}",
            written.body
        );
        let uncompressed = i32::try_from(written.uncompressed_page_size).unwrap();
        let compressed = i32::try_from(written.compressed_page_size).unwrap();
        let over_ceiling = i32::try_from(MOST_VALUE_PAGE_BYTES + 1).unwrap();
        let ceiling = |declared: i32| {
            over_bound(
                page,
                HeaderField::UncompressedPageSize,
                u64::from(declared.unsigned_abs()),
                MOST_VALUE_PAGE_BYTES,
            )
        };
        let cases = [
            (
                vec![
                    (HeaderField::UncompressedPageSize, uncompressed),
                    (HeaderField::CompressedPageSize, compressed),
                ],
                None,
            ),
            (
                vec![(HeaderField::UncompressedPageSize, over_ceiling)],
                Some(ceiling(over_ceiling)),
            ),
            (
                vec![(HeaderField::UncompressedPageSize, i32::MAX)],
                Some(ceiling(i32::MAX)),
            ),
            (
                vec![(HeaderField::CompressedPageSize, compressed + 1)],
                Some(over_bound(
                    page,
                    HeaderField::CompressedPageSize,
                    u64::from((compressed + 1).unsigned_abs()),
                    u64::from(compressed.unsigned_abs()),
                )),
            ),
            (
                vec![
                    (HeaderField::DefinitionLevelsByteLength, i32::MAX),
                    (HeaderField::RepetitionLevelsByteLength, i32::MAX),
                ],
                Some(over_bound(
                    page,
                    HeaderField::LevelsByteLength,
                    u64::from(i32::MAX.unsigned_abs()) + u64::from(i32::MAX.unsigned_abs()),
                    u64::from(uncompressed.unsigned_abs()),
                )),
            ),
        ];
        let kept = (**index.metadata()).clone();
        let mut index = index;
        for (claims, refused) in cases {
            index = with_value_header(index, &opened, &kept, page, &claims, None).await;
            let read = index.read_value(at, None).await;
            match refused {
                None => assert_eq!(
                    read.unwrap().0.as_deref(),
                    Some(value_for(row).as_slice()),
                    "{claims:?} as written"
                ),
                Some(fault) => assert_eq!(fault_of(read), fault, "{claims:?}"),
            }
        }
    }

    /// A value column's dictionary page a lookup fetches whose header
    /// counts more entries than its row group has rows, or than
    /// `MOST_VALUE_PAGE_BYTES` holds at the bytes parquet keeps a decoded
    /// byte array in when the footer claims more rows than that, is refused
    /// before parquet sizes the dictionary by the count; one declaring more
    /// than the ceiling uncompressed is refused as well. The count as
    /// written serves the value.
    #[tokio::test]
    async fn test_read_value_refuses_a_value_dictionary_over_its_bounds() {
        let mut fixture = Fixture::contract(KeyType::Long, contract_rows());
        fixture.layout.dictionary = Dictionary::Value;
        let opened = store(&fixture).await;
        let index = opened.index(KeyType::Long).await;
        let page = ChunkPage::Dictionary;
        let PageBody::Dictionary { num_values } = value_header(&index, &opened, page).body else {
            panic!("the value chunk starts with its dictionary page");
        };
        let written = i32::try_from(num_values).unwrap();
        let rows = count(group_rows());
        let row = 0;
        let at = row_of(row, group_rows());
        let rows_claim = i32::try_from(rows).unwrap();
        let held = MOST_VALUE_PAGE_BYTES / count(size_of::<ByteArray>());
        let held_claim = i32::try_from(held + 1).unwrap();
        let over_ceiling = i32::try_from(MOST_VALUE_PAGE_BYTES + 1).unwrap();
        let cases = [
            (HeaderField::NumValues, written, None, None),
            (
                HeaderField::NumValues,
                rows_claim + 1,
                None,
                Some((rows_claim + 1, rows)),
            ),
            (
                HeaderField::UncompressedPageSize,
                over_ceiling,
                None,
                Some((over_ceiling, MOST_VALUE_PAGE_BYTES)),
            ),
            (
                HeaderField::NumValues,
                held_claim,
                Some(INFLATED_GROUP_ROWS),
                Some((held_claim, held)),
            ),
        ];
        let kept = (**index.metadata()).clone();
        let mut index = index;
        for (field, claimed, group_rows, refused) in cases {
            let claims = [(field, claimed)];
            index = with_value_header(index, &opened, &kept, page, &claims, group_rows).await;
            let read = index.read_value(at, None).await;
            match refused {
                None => assert_eq!(
                    read.unwrap().0.as_deref(),
                    Some(value_for(row).as_slice()),
                    "{field} {claimed} as written"
                ),
                Some((declared, allowed)) => assert_eq!(
                    fault_of(read),
                    over_bound(page, field, u64::from(declared.unsigned_abs()), allowed),
                    "{field} {claimed}"
                ),
            }
        }
    }

    /// A value dictionary whose header counts one entry more than its body
    /// holds passes every header bound, the count below the row group's
    /// rows. Parquet panics decoding the entry that is not there; the
    /// lookup ends in `FileFault::DecoderPanicked` naming the dictionary
    /// page, and a lookup into another row group still serves.
    #[tokio::test]
    async fn test_read_value_a_value_dictionary_short_of_its_count_is_a_file_fault() {
        let mut fixture = Fixture::contract(KeyType::Long, contract_rows());
        fixture.layout.dictionary = Dictionary::Value;
        let value_leaf = 1;
        let (short, written) = with_dictionary_one_entry_short(&fixture.write(), value_leaf);
        assert!(u64::from(written) < count(group_rows()), "{written}");
        let opened = store_file(short).await;
        let index = opened.index(KeyType::Long).await;
        let fault = fault_of(index.read_value(row_of(0, group_rows()), None).await);
        assert!(
            matches!(
                fault,
                FileFault::DecoderPanicked {
                    part: DecodedPart::Pages {
                        row_group: 0,
                        page: Some(ChunkPage::Dictionary),
                    },
                    ..
                }
            ),
            "{fault:?}"
        );
        let elsewhere = group_rows();
        let (value, _) = index
            .read_value(row_of(elsewhere, group_rows()), None)
            .await
            .unwrap();
        assert_eq!(value.as_deref(), Some(value_for(elsewhere).as_slice()));
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
            assert_eq!(run.first_not_increasing(), None);
            for at in 0..run.len() {
                assert_eq!(run.binary_search(&run.get(at)), Ok(at));
                assert_eq!(run.cmp_at(at, run, at), std::cmp::Ordering::Equal);
            }
            assert_eq!(run.cmp_at(0, run, 2), std::cmp::Ordering::Less);
            assert_eq!(run.binary_search(&absent_after(&run.get(0))), Err(1));
            for other in &runs {
                if std::mem::discriminant(other) != std::mem::discriminant(run) {
                    assert_eq!(run.binary_search(&other.get(1)), Err(0));
                }
            }
        }
        assert_eq!(runs[0].max_len(), size_of::<i64>());
        assert_eq!(runs[1].max_len(), "é".len());
        assert_eq!(runs[2].max_len(), 2);
        assert_eq!(runs[3].max_len(), UUID_BYTES);
        assert_eq!(Keys::Long(Vec::new()).max_len(), 0);
        assert!(Keys::Long(vec![1, 1]).first_not_increasing().is_some());
        assert!(
            Keys::Str(vec![text("b"), text("a")])
                .first_not_increasing()
                .is_some()
        );
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
        let (value, _) = read.await.unwrap();
        assert_eq!(value.as_deref(), Some(value_for(1).as_slice()));
        let chunks = index.key_chunks_for_verification();
        assert_send(&chunks);
        chunks.await.unwrap();
    }
}
