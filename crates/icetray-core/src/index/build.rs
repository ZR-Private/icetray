//! The chunk stage of the index: the key column chunks fetched once, kept
//! in a chunk file, or dropped for their filters, and every key of the file
//! decoded and checked against the contract (RFD 007, RFD 017), unless a
//! chunk file already holds what that check established.

use std::cmp::Ordering;
use std::fmt;
use std::ops::Range;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex, PoisonError};

use bytes::Bytes;
use parquet::bloom_filter::Sbbf;
use parquet::column::reader::ColumnReaderImpl;
use parquet::data_type::{
    ByteArray, ByteArrayType, DataType as ParquetDataType, FixedLenByteArray,
    FixedLenByteArrayType, Int64Type,
};
use parquet::file::metadata::ParquetMetaData;
use parquet::file::page_index::offset_index::PageLocation;

use super::inspect::{bloom_ranges, key_chunk_ranges};
use super::page_header::check_page_headers;
use super::pages::{ChunkPages, PageTally};
use super::plain::{self, ByteKeys};
use super::record::{self, RecordFault};
use super::{
    Column, ColumnChunk, DecodedPart, FileFault, FileIndex, GroupRange, Inspected, KeyResidency,
    KeyShape, Keys, file_fault, identity_of, row_index, unless_panicked,
};
use crate::blocking::on_blocking_pool;
use crate::config::{ConfigError, ConfigViolation, Residency};
use crate::contract::{MAX_KEY_BYTES, PAGE_ROWS};
use crate::error::{Error, RefusalReason, Result};
use crate::key::{Key, KeyType, UUID_BYTES};
use crate::local::{
    ChunkFileLookup, DiskChunks, LocalDir, LocalFault, Rejection, Reservation, VerifiedUnder,
};
use crate::parquet_io::{FileHandle, Segments};

impl Inspected {
    /// The chunk stage of `FileIndex::build`: streams the key chunks once
    /// for verification and keeps what `residency` keeps, as `build`
    /// documents. The Disk tier with no local directory is the one rule the
    /// loader re-checks, since a configuration built in code may skip
    /// `Config::validated`; it is raised here as the same
    /// `ConfigViolation`, before any chunk is fetched. A Disk-tier chunk
    /// file write holds `reservation`, the load's hold on `local`'s budget,
    /// until the write ends.
    pub(crate) async fn build(
        self,
        residency: Residency,
        local: Option<&Arc<LocalDir>>,
        reservation: Option<&Arc<Reservation>>,
    ) -> Result<FileIndex> {
        let to_verify = KeysToVerify::of(&self);
        let Self {
            handle,
            metadata,
            layout,
            ..
        } = self;
        let ranges = key_chunk_ranges(&metadata, layout.key_leaf, handle.size());
        let (keys, verified) = match residency {
            Residency::Full => {
                let (chunks, verified) = fetch_verified(&to_verify, &ranges).await?;
                (KeyResidency::Chunks(chunks), verified)
            }
            Residency::Disk => {
                let local = local.ok_or_else(|| {
                    Error::Config(ConfigError::Invalid(
                        ConfigViolation::DiskResidencyWithoutLocalDir {
                            table: to_verify.table.clone(),
                        },
                    ))
                })?;
                let (disk, verified) = disk_chunks(&to_verify, local, reservation, &ranges).await?;
                (KeyResidency::Disk(disk), verified)
            }
            Residency::Bloom => {
                let filter_ranges =
                    bloom_ranges(&to_verify.table, &handle, &metadata, layout.key_leaf)?;
                let (chunks, verified) = fetch_verified(&to_verify, &ranges).await?;
                drop(chunks);
                let filters = read_bloom_filters(&handle, &filter_ranges).await?;
                (KeyResidency::Filters(filters), verified)
            }
        };
        let rows = metadata.file_metadata().num_rows();
        Ok(FileIndex {
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
}

/// The key column of one file being built, owned, so that its
/// verification can move to the blocking pool.
struct KeysToVerify {
    table: String,
    handle: Arc<FileHandle>,
    metadata: Arc<ParquetMetaData>,
    leaf: usize,
    shape: KeyShape,
}

impl KeysToVerify {
    /// The key column of `inspected`.
    fn of(inspected: &Inspected) -> Self {
        Self {
            table: inspected.table.clone(),
            handle: Arc::clone(&inspected.handle),
            metadata: Arc::clone(&inspected.metadata),
            leaf: inspected.layout.key_leaf,
            shape: inspected.layout.key_shape,
        }
    }

    /// `verify` of this column over `chunks`, run on the blocking pool
    /// (`on_blocking_pool`): decoding every key of a file is CPU-bound
    /// work that grows with the file, and inline it would hold the async
    /// worker that also answers lookups. The builds a load has in flight
    /// each wait on at most one verification, so a load runs at most
    /// `generation::load_concurrency()` of them at once, each on its own blocking-pool
    /// thread. A canceled task is `Error::VerifyCanceled` for the file.
    async fn verify(&self, chunks: &Arc<Segments>) -> Result<Verified> {
        let table = self.table.clone();
        let handle = Arc::clone(&self.handle);
        let metadata = Arc::clone(&self.metadata);
        let (leaf, shape) = (self.leaf, self.shape);
        let chunks = Arc::clone(chunks);
        on_blocking_pool(move || {
            let column = Column {
                metadata: &metadata,
                leaf,
                path: &handle.location,
            };
            verify(&table, &column, shape, &chunks)
        })
        .await
        .map_err(|canceled| Error::verify_canceled(&self.handle.location, canceled))?
    }
}

/// The key column chunks of `ranges`, fetched and verified: the chunks a
/// tier keeps (Full), writes (Disk), or drops (Bloom), and what
/// verification recorded.
async fn fetch_verified(
    to_verify: &KeysToVerify,
    ranges: &[Range<u64>],
) -> Result<(Arc<Segments>, Verified)> {
    let handle = &to_verify.handle;
    let parts = fetch_chunk_parts(handle, ranges).await?;
    let chunks = Arc::new(Segments::new(parts, handle.size()));
    let verified = to_verify.verify(&chunks).await?;
    Ok((chunks, verified))
}

/// Every key column chunk in one `read_ranges` call: one load GET per row
/// group. Each chunk is copied out of the buffer the store returned: a
/// store may serve neighboring ranges from one fetch and hand back slices
/// of it, and a slice keeps the whole fetch alive, so the bytes held would
/// exceed the chunks by whatever lay between them. The copy is paid once at
/// load.
pub(super) async fn fetch_chunk_parts(
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
/// counted local read per row group, no GET. For the overlap check and the
/// doctor, which decode every key; a build never reads chunks back.
pub(super) async fn chunks_from_disk(
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

/// The disk tier's key chunks. A chunk file in `local` that is reusable
/// (`reusable_disk_chunks`) and whose payload checksum matches is trusted
/// with the verification record it stores: no GET, no chunk read back, no
/// key decoded (`trusted`). Any other file, or none, is fetched, verified,
/// and written, replacing whatever is at that name; a file that was there
/// is logged at info with why it was not reused. Nothing about a stale file
/// fails the load. The write runs on the blocking pool (`on_blocking_pool`)
/// and holds `reservation` until it ends, so bytes it lands after its
/// caller stopped waiting stay reserved until they are on disk. A canceled
/// write or checksum pass is `Error::Local` on the chunk file's path with
/// `LocalFault::Join`.
async fn disk_chunks(
    to_verify: &KeysToVerify,
    local: &Arc<LocalDir>,
    reservation: Option<&Arc<Reservation>>,
    ranges: &[Range<u64>],
) -> Result<(DiskChunks, Verified)> {
    let handle = &to_verify.handle;
    let identity = identity_of(handle);
    let path = local.chunk_path(&identity);
    match reused(to_verify, local, ranges).await? {
        Ok(reused) => return Ok(reused),
        Err(None) => {}
        Err(Some(stale)) => tracing::info!(
            path = %path.display(),
            data_file = %handle.location,
            reason = %stale,
            detail = ?stale,
            "chunk file is stale; fetching its chunks to rewrite it"
        ),
    }
    let under = verified_under(to_verify.shape.key_type());
    let (chunks, verified) = fetch_verified(to_verify, ranges).await?;
    let verification = record::encode(&verified);
    let (local, reservation, written_path) =
        (Arc::clone(local), reservation.cloned(), path.clone());
    let disk = on_blocking_pool(move || {
        let written = local.write_chunks(
            &identity,
            under,
            &verification,
            chunks.parts().iter().cloned(),
        );
        drop(reservation);
        written
    })
    .await
    .map_err(|canceled| Error::Local {
        path: written_path,
        fault: LocalFault::Join(canceled),
    })??;
    Ok((disk, verified))
}

/// The chunk file in `local` for `to_verify`'s data file, whose key chunks
/// are `ranges`, with what it stores of their verification, when a build
/// may trust it: it is reusable by its header (`reusable_disk_chunks`) and
/// by its payload (`trusted`). Otherwise why not, `None` for no file.
async fn reused(
    to_verify: &KeysToVerify,
    local: &LocalDir,
    ranges: &[Range<u64>],
) -> Result<std::result::Result<(DiskChunks, Verified), Option<Stale>>> {
    let handle = &to_verify.handle;
    let under = verified_under(to_verify.shape.key_type());
    Ok(match reusable_disk_chunks(handle, local, ranges, under)? {
        OnDisk::Reusable(disk) => {
            let path = local.chunk_path(&identity_of(handle));
            trusted(to_verify, disk, &path).await?.map_err(Some)
        }
        OnDisk::Stale(stale) => Err(Some(stale)),
        OnDisk::Absent => Err(None),
    })
}

/// `disk` with the verification record it stores, once the payload
/// checksum matches (`DiskChunks::checked_verification`, bounded by
/// `record::most_bytes`) and the record describes the file the footer does
/// (`record::decode`); otherwise why not. Runs on the blocking pool: the
/// checksum pass reads the whole payload, a sequential local read far
/// cheaper than decoding it.
async fn trusted(
    to_verify: &KeysToVerify,
    disk: DiskChunks,
    path: &std::path::Path,
) -> Result<std::result::Result<(DiskChunks, Verified), Stale>> {
    let handle = Arc::clone(&to_verify.handle);
    let metadata = Arc::clone(&to_verify.metadata);
    let (leaf, shape) = (to_verify.leaf, to_verify.shape);
    on_blocking_pool(move || {
        let column = Column {
            metadata: &metadata,
            leaf,
            path: &handle.location,
        };
        let verification = match disk.checked_verification(record::most_bytes(&column))? {
            Ok(verification) => verification,
            Err(rejection) => return Ok(Err(Stale::Rejected(rejection))),
        };
        Ok(record::decode(&verification, &column, shape)
            .map(|verified| (disk, verified))
            .map_err(Stale::Record))
    })
    .await
    .map_err(|canceled| Error::Local {
        path: path.to_path_buf(),
        fault: LocalFault::Join(canceled),
    })?
}

/// What a Disk-tier build finds in `local` for a data file
/// (`reusable_disk_chunks`).
#[derive(Debug)]
pub(super) enum OnDisk {
    /// No chunk file.
    Absent,
    /// A chunk file the build does not reuse, and why.
    Stale(Stale),
    /// A chunk file whose header says it is the one the build would write;
    /// its payload is still to be checked (`trusted`).
    Reusable(DiskChunks),
}

/// Why a Disk-tier build does not reuse the chunk file under its data
/// file's name and fetches, verifies, and rewrites it instead (RFD 017).
/// Not an error: the load goes on as for a missing file. Display gives the
/// `snake_case` name the info log carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Stale {
    /// The file failed a check of its envelope, header, identity, or
    /// payload checksum.
    Rejected(Rejection),
    /// The file's keys were verified as another key type.
    KeyType {
        /// The key type the header carries.
        stored: KeyType,
    },
    /// The file's keys were verified under other rules.
    VerifierVersion {
        /// The verifier version the header carries.
        stored: u32,
    },
    /// The file does not store exactly the key column chunks the footer
    /// names.
    Chunks,
    /// The verification record does not describe the file the footer does.
    Record(RecordFault),
}

impl fmt::Display for Stale {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected(rejection) => rejection.fmt(f),
            Self::KeyType { .. } => f.write_str("key_type_mismatch"),
            Self::VerifierVersion { .. } => f.write_str("verifier_version_mismatch"),
            Self::Chunks => f.write_str("chunks_mismatch"),
            Self::Record(fault) => write!(f, "record_{fault}"),
        }
    }
}

/// The chunk file in `local` a Disk-tier build of `handle`'s file may
/// reuse, from its envelope and header alone: one whose identity (path,
/// size, footer digest) matches, whose keys were verified `under` the
/// build's key type and verifier version, and which stores exactly
/// `ranges`, the key column chunks. `Stale` with the first check a file
/// there fails, `Absent` when there is none.
pub(super) fn reusable_disk_chunks(
    handle: &FileHandle,
    local: &LocalDir,
    ranges: &[Range<u64>],
    under: VerifiedUnder,
) -> Result<OnDisk> {
    let disk = match local.lookup(&identity_of(handle))? {
        ChunkFileLookup::Absent => return Ok(OnDisk::Absent),
        ChunkFileLookup::Rejected(rejection) => {
            return Ok(OnDisk::Stale(Stale::Rejected(rejection)));
        }
        ChunkFileLookup::Found(disk) => disk,
    };
    let stored = disk.verified_under();
    Ok(if stored.key_type != under.key_type {
        OnDisk::Stale(Stale::KeyType {
            stored: stored.key_type,
        })
    } else if stored.verifier_version != under.verifier_version {
        OnDisk::Stale(Stale::VerifierVersion {
            stored: stored.verifier_version,
        })
    } else if !disk.stores_exactly(ranges) {
        OnDisk::Stale(Stale::Chunks)
    } else {
        OnDisk::Reusable(disk)
    })
}

/// The key column's bloom filter of every row group, one `read_ranges` call
/// (one load GET per row group), each parsed from exactly the bytes the
/// footer bounds: a header, then a bitset of exactly the header's length,
/// holding at least one block. A filter that does not decode so, or whose
/// bitset holds no block (fewer than `parquet::bloom_filter::BITSET_MIN_LENGTH`
/// bytes; a check indexes the first block), is `FileFault::CorruptBloomFilter`,
/// and one parquet panics parsing is `FileFault::DecoderPanicked`.
async fn read_bloom_filters(handle: &FileHandle, ranges: &[Range<u64>]) -> Result<Vec<Sbbf>> {
    let bytes = handle.read_ranges(ranges).await?;
    bytes
        .iter()
        .enumerate()
        .map(|(row_group, bytes)| {
            let corrupt = || {
                file_fault(
                    &handle.location,
                    FileFault::CorruptBloomFilter { row_group },
                )
            };
            // Unwind-safe: `from_bytes` reads the bytes and mutates nothing
            // it does not return.
            let parsed = unless_panicked(
                &handle.location,
                || Sbbf::from_bytes(bytes),
                || DecodedPart::BloomFilter { row_group },
            )?;
            let filter = parsed.map_err(|error| {
                tracing::debug!(%error, path = %handle.location, row_group, "bloom filter does not decode");
                corrupt()
            })?;
            if filter.num_blocks() == 0 {
                return Err(corrupt());
            }
            Ok(filter)
        })
        .collect()
}

/// The version of the rules `verify` enforces, which a Disk-tier chunk
/// file's verification record was computed under (RFD 017). Raise it with
/// any change to what `verify` refuses or records: a check added, dropped,
/// or loosened, `contract::MAX_KEY_BYTES`, how a fence is chosen. A record
/// of any other version is stale and the file is verified again from
/// object storage, so a result computed under other rules is never trusted.
pub(super) const VERIFIER_VERSION: u32 = 1;

/// The verification context of this build for keys of `key_type`.
pub(super) fn verified_under(key_type: KeyType) -> VerifiedUnder {
    VerifiedUnder {
        key_type,
        verifier_version: VERIFIER_VERSION,
    }
}

/// What verifying the key column establishes, and what a Disk-tier chunk
/// file stores of it (`record`).
#[derive(Debug, PartialEq)]
pub(super) struct Verified {
    pub(super) fences: Vec<Vec<Key>>,
    pub(super) groups: Vec<GroupRange>,
    pub(super) key_range: (Key, Key),
    pub(super) max_key_bytes: usize,
}

/// Decodes every key of every row group from `chunks` and checks what the
/// contract requires of them (RFD 007): no null, UTF-8 in a string column,
/// every key above the one before it within a row group and across row
/// groups (a key equal to the one before it is a duplicate, a key below
/// it is unsorted), none over `contract::MAX_KEY_BYTES`. Every key page
/// header of a row group is checked against what its page can hold before
/// the row group is decoded (`check_page_headers`), and every key page
/// against the rows its offset index gives it on the way. Records
/// the exact first and last key of the file and of every row group, the
/// longest key, and the fences. Nothing here consults column statistics.
/// A row group's keys are checked a decode batch at a time (`RunCheck`),
/// so at most one batch of them is held however long the row group is. A
/// string or binary key chunk of `PLAIN` pages without a dictionary is
/// read in place (`plain::checked_run`), and any other chunk, or one of
/// those holding a page it does not read exactly, by parquet's column
/// reader (`check_run`); either way the keys and every refusal and fault
/// are the column reader's.
fn verify(
    table: &str,
    column: &Column<'_>,
    shape: KeyShape,
    chunks: &Arc<Segments>,
) -> Result<Verified> {
    verify_with(table, column, shape, chunks, KeyDecoder::Plain)
}

/// `verify`, each row group decoded by `decoder` where it can be
/// (`check_run`). The decoder is a parameter so that tests can hold the
/// plain decoder to the column reader's answers.
fn verify_with(
    table: &str,
    column: &Column<'_>,
    shape: KeyShape,
    chunks: &Arc<Segments>,
    decoder: KeyDecoder,
) -> Result<Verified> {
    let path = column.path;
    let total = column.metadata.num_row_groups();
    let mut fences = Vec::with_capacity(total);
    let mut groups: Vec<GroupRange> = Vec::with_capacity(total);
    let mut longest = 0;
    for row_group in 0..total {
        let chunk = column.group(row_group);
        let rows = chunk.rows();
        check_page_headers(chunk, shape, chunks)?;
        let (run, _) = check_run(chunk, shape, Arc::clone(chunks), decoder)?;
        if run.values < row_index(rows) {
            let reason = RefusalReason::NullKeys {
                path: path.to_string(),
                row_group,
            };
            return Err(Error::refused(table, reason));
        }
        if run.not_utf8 {
            return Err(not_utf8(chunk));
        }
        if let Some((order, key)) = run.disorder {
            return Err(Error::refused(table, not_above_previous(chunk, order, key)));
        }
        longest = longest.max(run.longest);
        let Some((first, last)) = run.first.zip(run.last) else {
            fences.push(Vec::new());
            continue;
        };
        if let Some(previous) = groups.last() {
            let order = first.cmp(&previous.last);
            if order != Ordering::Greater {
                return Err(Error::refused(
                    table,
                    not_above_previous(chunk, order, first),
                ));
            }
        }
        fences.push(run.fences);
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
        return Err(Error::refused(table, reason));
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

/// One decode batch of a row group's keys, in file order, as `RunCheck`
/// reads it: values parquet's column reader decoded (`Keys`), or values
/// sliced from a plain page in place (`plain::checked_run`).
pub(super) trait KeyBatch {
    /// The number of keys.
    fn len(&self) -> usize;

    /// Whether the batch holds no key.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether every key is UTF-8, in a column of strings; true in any
    /// other.
    fn is_utf8(&self) -> bool;

    /// The length in bytes of the longest key; zero for an empty batch.
    fn max_len(&self) -> usize;

    /// The key at `at`, which is below `len`. A string key is built from
    /// its bytes as UTF-8, so a batch of strings is read back only once
    /// `is_utf8` holds for it.
    fn key(&self, at: usize) -> Key;

    /// The position of the first key not above the one before it, with how
    /// it compares to that key: `Equal` for a duplicate, `Less` for a key
    /// out of order. `None` for a strictly increasing batch.
    fn first_not_increasing(&self) -> Option<(usize, Ordering)>;
}

impl KeyBatch for Keys {
    fn len(&self) -> usize {
        Keys::len(self)
    }

    fn is_utf8(&self) -> bool {
        keys_are_utf8(self)
    }

    fn max_len(&self) -> usize {
        Keys::max_len(self)
    }

    fn key(&self, at: usize) -> Key {
        self.get(at)
    }

    fn first_not_increasing(&self) -> Option<(usize, Ordering)> {
        Keys::first_not_increasing(self)
    }
}

/// What `verify` learns of one row group's keys, batch by batch as they are
/// decoded, so that no row group's keys are held at once: the keys
/// decoded, the longest, the first and last, the key at each page's first
/// row (the fences), whether every string key is UTF-8, and the first key
/// not above the one before it. Nothing is decided here; `verify` applies
/// what was found once the whole row group has decoded, in the order it
/// checks a row group in, so a decode failure anywhere in it stands before
/// a null, a null before a key that is not UTF-8, and that before a key
/// out of order. Once a batch holds a string that is not UTF-8, later
/// batches are only counted, since `KeyBatch::key` builds a string key
/// from UTF-8 alone. What is found does not depend on where the batches
/// begin and end, so the two decoders, whose batches differ, find the same.
pub(super) struct RunCheck<'a> {
    values: usize,
    longest: usize,
    first: Option<Key>,
    last: Option<Key>,
    fences: Vec<Key>,
    /// The first rows of the pages whose key is a fence, in order, and how
    /// many of them have been taken.
    fence_rows: &'a [PageLocation],
    fences_taken: usize,
    not_utf8: bool,
    /// The first key not above the one before it, and how it compares.
    disorder: Option<(Ordering, Key)>,
}

impl<'a> RunCheck<'a> {
    pub(super) fn new(chunk: ColumnChunk<'a>) -> Self {
        let fence_rows = chunk.page_locations();
        Self {
            values: 0,
            longest: 0,
            first: None,
            last: None,
            fences: Vec::with_capacity(fence_rows.len()),
            fence_rows,
            fences_taken: 0,
            not_utf8: false,
            disorder: None,
        }
    }

    /// The next batch of the row group's keys, in file order.
    pub(super) fn visit(&mut self, keys: &impl KeyBatch) {
        let base = self.values;
        self.values += keys.len();
        if self.not_utf8 || keys.is_empty() {
            return;
        }
        if !keys.is_utf8() {
            self.not_utf8 = true;
            return;
        }
        if self.disorder.is_some() {
            return;
        }
        self.longest = self.longest.max(keys.max_len());
        let head = keys.key(0);
        if let Some(previous) = &self.last {
            let order = head.cmp(previous);
            if order != Ordering::Greater {
                self.disorder = Some((order, head));
                return;
            }
        }
        if let Some((at, order)) = keys.first_not_increasing() {
            self.disorder = Some((order, keys.key(at)));
            return;
        }
        while let Some(location) = self.fence_rows.get(self.fences_taken) {
            let row = row_index(location.first_row_index);
            if row >= self.values {
                break;
            }
            self.fences.push(keys.key(row - base));
            self.fences_taken += 1;
        }
        self.last = Some(keys.key(keys.len() - 1));
        self.first.get_or_insert(head);
    }
}

/// Which decoder reads a row group's keys for `verify`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum KeyDecoder {
    /// A string or binary key chunk of plain pages sliced in place
    /// (`plain::checked_run`), where the chunk is one it reads; the column
    /// reader otherwise.
    Plain,
    /// Parquet's column reader (`read_column_batches`).
    ColumnReader,
}

/// Every key of row group `chunk` decoded from `chunks` and checked
/// (`RunCheck`), with the decoder that read them: the plain reader when
/// `decoder` asks for it and it reads the chunk, which it does exactly as
/// the column reader would or not at all, and the column reader otherwise.
/// A row group of plain pages that ends in any fault is read twice, the
/// second time by the column reader, which reports the fault.
fn check_run(
    chunk: ColumnChunk<'_>,
    shape: KeyShape,
    chunks: Arc<Segments>,
    decoder: KeyDecoder,
) -> Result<(RunCheck<'_>, KeyDecoder)> {
    let byte_keys = match shape {
        KeyShape::Str => Some(ByteKeys::Text),
        KeyShape::Bin => Some(ByteKeys::Bytes),
        KeyShape::Long | KeyShape::FixedBin | KeyShape::Uuid => None,
    };
    if decoder == KeyDecoder::Plain
        && let Some(keys) = byte_keys
        && let Some(run) = plain::checked_run(chunk, keys, Arc::clone(&chunks))
    {
        return Ok((run, KeyDecoder::Plain));
    }
    let mut run = RunCheck::new(chunk);
    read_run(chunk, shape, chunks, &mut run)?;
    Ok((run, KeyDecoder::ColumnReader))
}

/// Every key of row group `chunk` decoded from `chunks` by the column
/// reader a batch at a time (`read_column_batches`) and handed to `run`.
/// Each batch's buffer is handed back to the decoder for the next, so a
/// row group's keys cost one batch's buffer rather than one buffer for all
/// of them.
fn read_run(
    chunk: ColumnChunk<'_>,
    shape: KeyShape,
    chunks: Arc<Segments>,
    run: &mut RunCheck<'_>,
) -> Result<()> {
    let take = row_index(chunk.rows());
    match shape {
        KeyShape::Long => read_column_batches::<Int64Type>(chunk, chunks, 0, take, &mut |batch| {
            let keys = Keys::Long(std::mem::take(batch));
            run.visit(&keys);
            *batch = keys.into_longs();
        }),
        KeyShape::Str => {
            read_column_batches::<ByteArrayType>(chunk, chunks, 0, take, &mut |batch| {
                let keys = Keys::Str(std::mem::take(batch));
                run.visit(&keys);
                *batch = keys.into_byte_arrays();
            })
        }
        KeyShape::Bin => {
            read_column_batches::<ByteArrayType>(chunk, chunks, 0, take, &mut |batch| {
                let keys = Keys::Bin(std::mem::take(batch));
                run.visit(&keys);
                *batch = keys.into_byte_arrays();
            })
        }
        KeyShape::FixedBin => {
            read_column_batches::<FixedLenByteArrayType>(chunk, chunks, 0, take, &mut |batch| {
                run.visit(&Keys::Bin(fixed_as_binary(std::mem::take(batch))));
            })
        }
        KeyShape::Uuid => {
            read_column_batches::<FixedLenByteArrayType>(chunk, chunks, 0, take, &mut |batch| {
                run.visit(&Keys::Uuid(fixed_as_uuids(batch)));
            })
        }
    }
}

/// `FIXED_LEN_BYTE_ARRAY` values as the binary keys they are.
fn fixed_as_binary(values: Vec<FixedLenByteArray>) -> Vec<ByteArray> {
    values.into_iter().map(ByteArray::from).collect()
}

/// `FIXED_LEN_BYTE_ARRAY` values of a uuid column as uuid keys.
fn fixed_as_uuids(values: &[FixedLenByteArray]) -> Vec<[u8; UUID_BYTES]> {
    values
        .iter()
        .map(|value| {
            <[u8; UUID_BYTES]>::try_from(value.data()).expect(
                "a FIXED_LEN_BYTE_ARRAY value is its column's width, checked at build to be UUID_BYTES",
            )
        })
        .collect()
}

/// The refusal for `key`, which is not above the key before it in `chunk`:
/// `Equal` is a duplicate, anything else is out of order.
fn not_above_previous(chunk: ColumnChunk<'_>, order: Ordering, key: Key) -> RefusalReason {
    let path = chunk.column.path.to_string();
    match order {
        Ordering::Equal => RefusalReason::DuplicateKeys { path, key },
        Ordering::Less | Ordering::Greater => RefusalReason::Unsorted {
            path,
            row_group: chunk.row_group(),
        },
    }
}

/// Rows `rows` of the key column chunk `chunk` decoded from `chunks`, which
/// hold at least the pages those rows are in. Exactly those rows come back,
/// as `read_column` guarantees; a string column's values are not checked
/// as UTF-8 here, `check_utf8` does that where the run is read back through
/// `Keys::get`.
pub(super) fn decode_run(
    chunk: ColumnChunk<'_>,
    shape: KeyShape,
    rows: Range<i64>,
    chunks: Arc<Segments>,
) -> Result<Keys> {
    let skip = row_index(rows.start);
    let take = row_index(rows.end - rows.start);
    Ok(match shape {
        KeyShape::Long => Keys::Long(read_column::<Int64Type>(chunk, chunks, skip, take)?),
        KeyShape::Str => Keys::Str(read_column::<ByteArrayType>(chunk, chunks, skip, take)?),
        KeyShape::Bin => Keys::Bin(read_column::<ByteArrayType>(chunk, chunks, skip, take)?),
        KeyShape::FixedBin => Keys::Bin(fixed_as_binary(read_column::<FixedLenByteArrayType>(
            chunk, chunks, skip, take,
        )?)),
        KeyShape::Uuid => Keys::Uuid(fixed_as_uuids(&read_column::<FixedLenByteArrayType>(
            chunk, chunks, skip, take,
        )?)),
    })
}

/// Every value of a `Keys::Str` run is UTF-8, or the file is
/// `FileFault::KeyNotUtf8`; the other arms hold what their bytes are.
pub(super) fn check_utf8(chunk: ColumnChunk<'_>, keys: &Keys) -> Result<()> {
    if keys_are_utf8(keys) {
        Ok(())
    } else {
        Err(not_utf8(chunk))
    }
}

/// `FileFault::KeyNotUtf8` for the row group `chunk`.
fn not_utf8(chunk: ColumnChunk<'_>) -> Error {
    file_fault(
        chunk.column.path,
        FileFault::KeyNotUtf8 {
            row_group: chunk.row_group(),
        },
    )
}

/// Whether every value of a `Keys::Str` run is UTF-8; true for the other
/// arms. A value is first checked to be ASCII, which the standard library
/// does a word at a time inline, and only a value that is not goes through
/// the full validator. Joining a batch's values into one buffer and
/// validating that once costs as much in the copy as it saves in the
/// validator for keys of a few dozen bytes (measured on the benchmark's
/// string table).
fn keys_are_utf8(keys: &Keys) -> bool {
    let Keys::Str(run) = keys else {
        return true;
    };
    run.iter().all(|value| is_utf8(value.data()))
}

/// Whether `value` is UTF-8, checked as ASCII first (`keys_are_utf8`).
pub(super) fn is_utf8(value: &[u8]) -> bool {
    value.is_ascii() || std::str::from_utf8(value).is_ok()
}

/// The most records one `read_records` call asks the column reader for:
/// sixteen pages of the served layout, so a call's overhead is spread over
/// many pages. The reader sizes the value and level buffers of a call by
/// the records asked for, capped by the values its page has left, so the
/// buffers a call grows hold at most this many entries, under a mebibyte
/// for any Parquet type, whatever a footer claims for a row group or a
/// header for a page.
pub(super) const DECODE_BATCH_RECORDS: i64 = 16 * PAGE_ROWS;

/// `take` records of `chunk` after skipping `skip`, decoded from
/// `segments`, which hold at least the pages those rows are in and the
/// chunk's dictionary page when it has one. Only those pages are read,
/// the dictionary page first, as the reader requires (`ChunkPages`), and
/// the column reader skips the rows before `skip` within the first of
/// them; no page before it is read or walked past. Definition levels are
/// consumed when the column is nullable, and a null yields no value, so
/// fewer values than `take` may come back. On `Ok` exactly `take` records
/// were decoded, from exactly the pages the offset index names for them,
/// each page holding the rows the index gives it; a page that does not is
/// `FileFault::PageRowsMismatch`, so no row position rests on an offset
/// index the pages contradict.
///
/// `take` is the footer's number and sizes no buffer by itself. The column
/// reader is asked for at most `DECODE_BATCH_RECORDS` records a call, into
/// a batch buffer and a level buffer cleared before each call, and the
/// batch is appended to the result after it. The column reader sizes the
/// values and levels of a call by the smaller of the records asked for and
/// the values its page has left, so no value or level buffer it grows
/// holds more than a batch, whatever the footer claims for the row group
/// or a header for its page, and the result grows only with values
/// decoded. A data page whose header claims more values than the offset
/// index gives it rows is refused before the column reader decodes it. A
/// page whose values run out short of its header's count ends the decode
/// in an error, which is `FileFault::PageRowsMismatch` when a header read
/// on the way disagrees with the offset index and `Error::Parquet`
/// otherwise, or, for a byte-array page, in a panic inside parquet.
///
/// Three allocations happen before this function sees what sized them.
/// A page's uncompressed size, from its header, is reserved when the page
/// is decompressed, by `ChunkPages` for a zstd page and by parquet's page
/// reader for another codec (a Snappy chunk zero-fills it), and a
/// dictionary page's value count, from its header, sizes the dictionary
/// parquet decodes into. The callers read those headers ahead of the page
/// reader and refuse one that declares more than its page can hold: `verify`
/// every key page's before the row group's first decode
/// (`check_page_headers`), and `FileIndex::read_value` the value page's
/// and its dictionary page's before `read_one` (`check_value_page_headers`).
/// A key page decoded at lookup is one `verify` checked at load, read from
/// the resident chunks, the chunk file, or, in the Bloom tier, the data
/// file again. A delta-length or delta byte-array page sizes its lengths
/// by the count in its encoded data, which no check reads.
///
/// Parquet panics on some page bodies the headers do not reveal: a
/// byte-array page holding fewer values than its header counts, a
/// dictionary page among them, and others. The decode, decompression
/// included, runs under `catch_unwind`, so a panic is
/// `FileFault::DecoderPanicked` at the last page the page reader took up,
/// and ends this load or lookup rather than the task running it.
fn read_column<T: ParquetDataType>(
    chunk: ColumnChunk<'_>,
    segments: Arc<Segments>,
    skip: usize,
    take: usize,
) -> Result<Vec<T::T>> {
    let mut values = Vec::new();
    read_column_batches::<T>(chunk, segments, skip, take, &mut |batch| {
        values.append(batch);
    })?;
    Ok(values)
}

/// `read_column`, with each batch of at most `DECODE_BATCH_RECORDS` records
/// handed to `sink` as it is decoded instead of gathered: the values of a
/// batch, in file order, in a buffer the sink may read or take. What the
/// sink leaves in the buffer is cleared before the next batch is decoded
/// into it, so a sink that hands a buffer back lets every batch reuse it.
/// The sink runs inside the decode's `catch_unwind`; everything
/// `read_column` checks once the decode ends is checked after the last
/// batch, so a sink sees batches of a decode that may still fail.
fn read_column_batches<T: ParquetDataType>(
    chunk: ColumnChunk<'_>,
    segments: Arc<Segments>,
    skip: usize,
    take: usize,
    sink: &mut dyn FnMut(&mut Vec<T::T>),
) -> Result<()> {
    let tally = Arc::new(Mutex::new(PageTally::default()));
    // Unwind-safe: the page reader, the column reader, and the buffers are
    // built inside `decode` and dropped with a panic; the footer `chunk`
    // borrows is only read, the tally `decode` shares holds each page's
    // record whole, written under its lock, the thread's zstd context
    // starts each page afresh, and what the sink gathered is dropped with
    // the error a panic becomes.
    let decode =
        AssertUnwindSafe(|| decode_records::<T>(chunk, segments, skip, take, &tally, sink));
    let decoded = unless_panicked(chunk.column.path, decode, || DecodedPart::Pages {
        row_group: chunk.row_group(),
        page: tally.lock().unwrap_or_else(PoisonError::into_inner).taken,
    })??;
    let tally = tally.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(page) = tally.refused {
        return Err(chunk.page_rows_mismatch(page));
    }
    chunk.check_pages_read(skip..skip + take, &tally.seen, decoded)
}

/// The decode `read_column_batches` runs under `catch_unwind`: `take`
/// records after skipping `skip`, each batch handed to `sink`, and how many
/// records the column reader produced or the error it ended in, with what
/// the page reader saw recorded in `tally`.
fn decode_records<T: ParquetDataType>(
    chunk: ColumnChunk<'_>,
    segments: Arc<Segments>,
    skip: usize,
    take: usize,
    tally: &Arc<Mutex<PageTally>>,
    sink: &mut dyn FnMut(&mut Vec<T::T>),
) -> Result<parquet::errors::Result<usize>> {
    let descr = chunk.column.descriptor();
    let nullable = descr.max_def_level() > 0;
    let pages = ChunkPages::new(chunk, skip..skip + take, segments, Arc::clone(tally))?;
    let skip = skip.checked_sub(pages.first_row()).expect(
        "the first page read is the one holding row `skip`, and inspect checked that a chunk's first page starts at row zero",
    );
    let mut reader: ColumnReaderImpl<T> = ColumnReaderImpl::new(descr, Box::new(pages));
    let batch_records = row_index(DECODE_BATCH_RECORDS);
    let mut batch = Vec::new();
    let mut def_levels = Vec::new();
    let mut decode = || -> parquet::errors::Result<usize> {
        let skipped = if skip > 0 {
            reader.skip_records(skip)?
        } else {
            0
        };
        let mut records = 0;
        while skipped == skip && records < take {
            batch.clear();
            def_levels.clear();
            let (read, _, _) = reader.read_records(
                (take - records).min(batch_records),
                nullable.then_some(&mut def_levels),
                None,
                &mut batch,
            )?;
            sink(&mut batch);
            if read == 0 {
                break;
            }
            records += read;
        }
        Ok(records)
    };
    Ok(decode())
}

/// Exactly row `row` of the value column chunk `chunk` from `segments`:
/// `Some` with the value, `None` for a null. The caller has checked the
/// headers of the value page and of the chunk's dictionary page in
/// `segments` (`check_value_page_headers`); `read_column` says why.
pub(super) fn read_one<T: ParquetDataType>(
    chunk: ColumnChunk<'_>,
    row: i64,
    segments: Arc<Segments>,
) -> Result<Option<T::T>> {
    let mut values = read_column::<T>(chunk, segments, row_index(row), 1)?;
    Ok(values.pop())
}

#[cfg(test)]
mod tests {
    use parquet::basic::{Compression, Encoding};
    use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};
    use parquet::file::properties::EnabledStatistics;

    use super::*;
    use crate::approx::{self, count};
    use crate::config::{ConfigError, ConfigViolation};
    use crate::error::ErrorKind;
    use crate::generation::testing::Dictionary;
    use crate::generation::testing::{
        self, KeyColumn, TABLE, cache, column_of, page_rows, refusal_of,
    };
    use crate::index::page_header::{
        BYTE_ARRAY_LENGTH_PREFIX_BYTES, MOST_BYTES_PER_VALUE, MOST_KEY_PAGE_BYTES, NIBBLE_MASK,
        PAGE_ENCODING_ALLOWANCE_BYTES, PageBody, PageHeader,
    };
    use crate::index::testing::{
        FEW_ROWS, FIXED_KEY_BYTES, Fixture, INFLATED_GROUP_ROWS, Opened, PAGES_PER_GROUP,
        ROW_GROUPS, SMALL_BATCH, absent_after, contract_rows, fault_of, group_rows, header_at,
        key_for, location, longs, moved_by_header, pages_of, predicted_of, row_of, stepped, store,
        store_file, with_column_chunk, with_dictionary_one_entry_short, with_group_rows,
        with_header_claims, with_pages,
    };
    use crate::index::{
        ChunkPage, Column, DecodedPart, FileFault, HeaderField, KeyShape, PageHeaderFault, RowRef,
        WireType,
    };
    use crate::key::KeyType;
    use crate::local::testing as local_testing;
    use crate::manifest::testing::{KEY_COLUMN, VALUE_COLUMN};
    use parquet::basic::PageType;
    use parquet::file::properties::WriterVersion;

    /// Rows per row group in the hand-written fixtures with two row groups.
    const TINY_GROUP_ROWS: usize = 2;

    /// A row count a footer may claim for one row group that no file holds:
    /// two to the fiftieth, eight pebibytes of long keys, beyond the address
    /// space of the supported targets, so a buffer reserved for that many
    /// keys aborts the process before any check runs.
    const UNALLOCATABLE_ROWS: i64 = 1 << 50;
    /// The bytes overwritten at the start of a bloom filter to corrupt it,
    /// and the byte they are overwritten with: no thrift field header.
    const GARBAGE_BYTES: usize = 32;
    const GARBAGE: u8 = 0xff;
    /// `FEW_ROWS` string keys of exactly `len` bytes: zero-padded decimals.
    fn keys_of_len(len: usize) -> Vec<Option<Key>> {
        (0..FEW_ROWS)
            .map(|i| Some(Key::Str(format!("{:0>len$}", stepped(i)))))
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
            let key = Key::Str(format!("{:0>MAX_KEY_BYTES$}", stepped(i)));
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
        for residency in Residency::ALL {
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

    #[tokio::test]
    async fn test_build_refuses_an_unsorted_row_group_in_every_tier() {
        let opened = store(&Fixture::with_keys(KeyColumn::Int64, longs(&[5, 3, 9]))).await;
        for residency in Residency::ALL {
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
        for residency in Residency::ALL {
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
        for residency in Residency::ALL {
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
        for residency in Residency::ALL {
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

    /// A row group longer than one decode batch (`DECODE_BATCH_RECORDS`)
    /// is checked across the batches it decodes in: a clean one records
    /// every page's first key as a fence wherever its batch begins, a
    /// duplicate whose copies end one batch and begin the next is refused,
    /// and a key that is not UTF-8 in the second batch is found.
    #[tokio::test]
    async fn test_verify_checks_a_row_group_across_its_decode_batches() {
        let batch = row_index(DECODE_BATCH_RECORDS);
        let rows = 2 * batch;
        let one_group = |mut fixture: Fixture| {
            fixture.layout.row_group_rows = rows;
            fixture
        };
        let clean = store(&one_group(Fixture::contract(KeyType::Long, rows))).await;
        let fences: Vec<Key> = (0..rows)
            .step_by(page_rows())
            .map(|i| key_for(i, KeyType::Long))
            .collect();
        assert_eq!(clean.index(KeyType::Long).await.fences, [fences]);

        let mut keys: Vec<Option<Key>> =
            (0..rows).map(|i| Some(key_for(i, KeyType::Long))).collect();
        keys[batch] = keys[batch - 1].clone();
        let straddling = store(&one_group(Fixture::with_keys(KeyColumn::Int64, keys))).await;
        assert_eq!(
            refusal_of(straddling.build(KeyType::Long, Residency::Full, None).await),
            RefusalReason::DuplicateKeys {
                path: location(),
                key: key_for(batch - 1, KeyType::Long),
            }
        );

        let not_utf8_at = batch + 1;
        let strings: Vec<Option<Key>> = (0..rows)
            .map(|i| {
                let mut bytes = key_for(i, KeyType::String).as_bytes().to_vec();
                if i == not_utf8_at {
                    // Above the key itself and below the next: order holds.
                    bytes.push(u8::MAX);
                }
                Some(Key::Bytes(bytes))
            })
            .collect();
        let late = store(&one_group(Fixture::with_keys(KeyColumn::Binary, strings))).await;
        assert_eq!(
            fault_of(late.build(KeyType::String, Residency::Full, None).await),
            FileFault::KeyNotUtf8 { row_group: 0 }
        );
    }

    /// ASCII values pass on the fast check, a value with a multi-byte
    /// character passes the full validator, and half of one character is
    /// refused, alone or after ASCII values.
    #[test]
    fn test_keys_are_utf8_takes_ascii_and_multibyte_and_refuses_a_cut_character() {
        let text = |values: &[&[u8]]| {
            Keys::Str(
                values
                    .iter()
                    .map(|value| ByteArray::from(value.to_vec()))
                    .collect(),
            )
        };
        let e_acute = "\u{e9}".as_bytes();
        assert!(keys_are_utf8(&text(&[b"a", e_acute, b"b"])));
        assert!(!keys_are_utf8(&text(&[&e_acute[..1], &e_acute[1..]])));
        assert!(!keys_are_utf8(&text(&[b"a", &e_acute[..1]])));
        assert!(keys_are_utf8(&Keys::Bin(vec![ByteArray::from(vec![
            u8::MAX
        ])])));
    }

    /// A footer that claims more rows for a row group than any page could
    /// hold is refused by the pages read, and nothing is reserved for the
    /// claimed rows on the way: the buffer grows with what the pages hold.
    #[tokio::test]
    async fn test_verify_refuses_an_unallocatable_row_count_without_reserving_for_it() {
        let opened = store(&Fixture::contract(KeyType::Long, contract_rows())).await;
        let loaded = opened.handle.load_metadata().await.unwrap();
        let key_leaf = 0;
        let chunks = key_chunks(&opened, &loaded, key_leaf).await;
        let path = location();
        let inflated = with_group_rows(&loaded, 0, UNALLOCATABLE_ROWS);
        let column = Column {
            metadata: &inflated,
            leaf: key_leaf,
            path: &path,
        };
        assert_eq!(
            fault_of(
                verify(TABLE, &column, KeyShape::Long, &chunks).map(|verified| verified.groups)
            ),
            FileFault::PageRowsMismatch {
                row_group: 0,
                page: PAGES_PER_GROUP - 1,
            }
        );
    }

    /// The bytes of the fixture's key column chunks, as `build` fetches
    /// them for verification.
    async fn key_chunks(
        opened: &Opened,
        metadata: &ParquetMetaData,
        key_leaf: usize,
    ) -> Arc<Segments> {
        let ranges = key_chunk_ranges(metadata, key_leaf, opened.handle.size());
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

    /// The header of `page` of row group 0's key chunk, as read ahead of
    /// parquet.
    fn header_of(
        metadata: &ParquetMetaData,
        key_leaf: usize,
        chunks: &Segments,
        page: ChunkPage,
    ) -> PageHeader {
        let (range, bytes) = &chunks.parts()[0];
        assert_eq!(
            range.start,
            metadata.row_group(0).column(key_leaf).byte_range().0
        );
        PageHeader::read(&bytes[header_at(metadata, key_leaf, page)..]).unwrap()
    }

    /// The fixture's key chunks and footer with `field` in the header of
    /// `page` of row group 0 claiming `claimed`: the varint rewritten in the
    /// chunk's bytes, and the page, the chunk, and every later page of the
    /// chunk moved in the footer by the change in the varint's width.
    async fn with_first_group_header(
        opened: &Opened,
        loaded: &ParquetMetaData,
        key_leaf: usize,
        page: ChunkPage,
        field: HeaderField,
        claimed: i32,
    ) -> (ParquetMetaData, Arc<Segments>) {
        with_first_group_headers(opened, loaded, key_leaf, page, &[(field, claimed)]).await
    }

    /// `with_first_group_header` with each of `claims`, a field and what it
    /// claims, rewritten in turn.
    async fn with_first_group_headers(
        opened: &Opened,
        loaded: &ParquetMetaData,
        key_leaf: usize,
        page: ChunkPage,
        claims: &[(HeaderField, i32)],
    ) -> (ParquetMetaData, Arc<Segments>) {
        let ranges = key_chunk_ranges(loaded, key_leaf, opened.handle.size());
        let mut parts = fetch_chunk_parts(&opened.handle, &ranges).await.unwrap();
        let (range, bytes) = &parts[0];
        let patched = with_header_claims(bytes, header_at(loaded, key_leaf, page), claims);
        let grown = i64::try_from(patched.len() - bytes.len()).unwrap();
        let start = range.start;
        parts[0] = (start..start + count(patched.len()), Bytes::from(patched));
        let metadata = moved_by_header(loaded, key_leaf, page, grown);
        (
            metadata,
            Arc::new(Segments::new(parts, opened.handle.size())),
        )
    }

    /// `verify` of the key column over `metadata` and `chunks`, down to the
    /// row groups it verified.
    fn verify_groups(
        metadata: &ParquetMetaData,
        key_leaf: usize,
        shape: KeyShape,
        chunks: &Arc<Segments>,
    ) -> Result<usize> {
        let path = location();
        let column = Column {
            metadata,
            leaf: key_leaf,
            path: &path,
        };
        verify(TABLE, &column, shape, chunks).map(|verified| verified.groups.len())
    }

    /// A key page whose header claims more values than the rows its offset
    /// index gives it is refused before the column reader decodes it, so the
    /// claim sizes no buffer: the reader would size its first decode by the
    /// smaller of the claim and the row group's rows.
    #[tokio::test]
    async fn test_verify_refuses_a_page_header_claiming_more_values_than_its_rows() {
        let opened = store(&Fixture::contract(KeyType::Long, contract_rows())).await;
        let loaded = opened.handle.load_metadata().await.unwrap();
        let key_leaf = 0;
        let path = location();
        let span = i32::try_from(PAGE_ROWS).unwrap();
        for claimed in [span, span + 1, i32::MAX] {
            let (metadata, chunks) = with_first_group_header(
                &opened,
                &loaded,
                key_leaf,
                ChunkPage::Data(0),
                HeaderField::NumValues,
                claimed,
            )
            .await;
            let column = Column {
                metadata: &metadata,
                leaf: key_leaf,
                path: &path,
            };
            let verified =
                verify(TABLE, &column, KeyShape::Long, &chunks).map(|verified| verified.groups);
            if claimed == span {
                assert_eq!(verified.unwrap().len(), ROW_GROUPS, "the count as written");
                continue;
            }
            assert_eq!(
                fault_of(verified),
                FileFault::PageRowsMismatch {
                    row_group: 0,
                    page: 0,
                },
                "{claimed} values claimed"
            );
        }
    }

    /// A row count a footer may claim for one row group: two to the
    /// thirty-first, one more than `i32::MAX`, so a row group with a single
    /// page gives that page a span with room for a header claiming
    /// `i32::MAX` values, and the span check lets the claim through.
    const SPAN_OF_I32_MAX_ROWS: i64 = 1 << 31;

    /// A footer that gives row group 0 one key page spanning
    /// `SPAN_OF_I32_MAX_ROWS` rows, whose header claims `i32::MAX` values
    /// over the thousand the page holds, is refused as a page that does not
    /// hold the rows its header claims, and the claim sizes no buffer on the
    /// way. The column reader sizes each decode by the smaller of the
    /// records a call asks for and the values its page has left: with one
    /// call for the whole row group that is `i32::MAX` longs, 16 GiB,
    /// reserved and zeroed before the page runs out. `read_column` asks for
    /// at most `DECODE_BATCH_RECORDS` records a call, into buffers it clears
    /// before each call, so no value or level buffer the reader grows holds
    /// more than a batch, whatever the footer or the header claims; the
    /// decode then fails where the page's values end, and the header's
    /// count, checked against the offset index, is the refusal.
    #[tokio::test]
    async fn test_verify_refuses_a_page_claiming_i32_max_values_in_a_span_that_holds_them() {
        let opened = store(&Fixture::contract(KeyType::Long, contract_rows())).await;
        let loaded = opened.handle.load_metadata().await.unwrap();
        let key_leaf = 0;
        let path = location();
        let (metadata, chunks) = with_first_group_header(
            &opened,
            &loaded,
            key_leaf,
            ChunkPage::Data(0),
            HeaderField::NumValues,
            i32::MAX,
        )
        .await;
        let first_page = pages_of(&metadata, key_leaf)[0].clone();
        let one_page = with_pages(&metadata, key_leaf, vec![first_page]);
        let lying = with_group_rows(&one_page, 0, SPAN_OF_I32_MAX_ROWS);
        let column = Column {
            metadata: &lying,
            leaf: key_leaf,
            path: &path,
        };
        assert_eq!(
            fault_of(
                verify(TABLE, &column, KeyShape::Long, &chunks).map(|verified| verified.groups)
            ),
            FileFault::PageRowsMismatch {
                row_group: 0,
                page: 0,
            }
        );
    }

    /// The bound on an uncompressed page of `values` values: every value a
    /// key at the cap with its encoding, plus the page's allowance.
    fn page_bound(values: u64) -> u64 {
        values * MOST_BYTES_PER_VALUE + PAGE_ENCODING_ALLOWANCE_BYTES
    }

    /// The fault `FileFault::PageHeaderOverBound` names for row group 0.
    fn over_bound(page: ChunkPage, field: HeaderField, declared: i32, allowed: u64) -> FileFault {
        FileFault::PageHeaderOverBound {
            row_group: 0,
            page,
            field,
            declared: u64::try_from(declared).unwrap(),
            allowed,
        }
    }

    /// A key page whose header declares an uncompressed size above what its
    /// rows could encode to with every key at the cap is refused before
    /// parquet reads the page, which would reserve the declared size and
    /// decompress into it; a size at the bound is left to parquet, whose
    /// decompression then finds the page's real size.
    #[tokio::test]
    async fn test_verify_refuses_a_key_page_declaring_more_uncompressed_bytes_than_its_rows_hold() {
        let opened = store(&Fixture::contract(KeyType::Long, contract_rows())).await;
        let loaded = opened.handle.load_metadata().await.unwrap();
        let key_leaf = 0;
        let chunks = key_chunks(&opened, &loaded, key_leaf).await;
        let page = ChunkPage::Data(0);
        let written = header_of(&loaded, key_leaf, &chunks, page).uncompressed_page_size;
        let allowed = page_bound(count(page_rows()));
        let at_bound = i32::try_from(allowed).unwrap();
        for claimed in [
            i32::try_from(written).unwrap(),
            at_bound,
            at_bound + 1,
            i32::MAX,
        ] {
            let (metadata, chunks) = with_first_group_header(
                &opened,
                &loaded,
                key_leaf,
                page,
                HeaderField::UncompressedPageSize,
                claimed,
            )
            .await;
            let verified = verify_groups(&metadata, key_leaf, KeyShape::Long, &chunks);
            if claimed == at_bound {
                assert!(
                    matches!(verified, Err(Error::Parquet(_))),
                    "at the bound parquet decodes the page: {verified:?}"
                );
                continue;
            }
            if claimed < at_bound {
                assert_eq!(verified.unwrap(), ROW_GROUPS, "the size as written");
                continue;
            }
            assert_eq!(
                fault_of(verified),
                over_bound(page, HeaderField::UncompressedPageSize, claimed, allowed),
                "{claimed} bytes declared"
            );
        }
    }

    /// A dictionary page declaring more values than its row group has rows
    /// is refused before parquet sizes the dictionary by the count, and so
    /// is one whose uncompressed size is above what that count of keys at
    /// the cap could encode to.
    #[tokio::test]
    async fn test_verify_refuses_a_dictionary_declaring_more_than_its_row_group_holds() {
        let mut fixture = Fixture::contract(KeyType::Long, contract_rows());
        fixture.layout.dictionary = Dictionary::Key;
        let opened = store(&fixture).await;
        let loaded = opened.handle.load_metadata().await.unwrap();
        let key_leaf = 0;
        let chunks = key_chunks(&opened, &loaded, key_leaf).await;
        let page = ChunkPage::Dictionary;
        let written = header_of(&loaded, key_leaf, &chunks, page);
        let rows = count(group_rows());
        assert_eq!(
            written.body,
            PageBody::Dictionary {
                num_values: u32::try_from(rows).unwrap()
            },
            "every key of the row group is in its dictionary"
        );
        let rows_claim = i32::try_from(rows).unwrap();
        for claimed in [rows_claim, rows_claim + 1, i32::MAX] {
            let (metadata, chunks) = with_first_group_header(
                &opened,
                &loaded,
                key_leaf,
                page,
                HeaderField::NumValues,
                claimed,
            )
            .await;
            let verified = verify_groups(&metadata, key_leaf, KeyShape::Long, &chunks);
            if claimed == rows_claim {
                assert_eq!(verified.unwrap(), ROW_GROUPS, "the count as written");
                continue;
            }
            assert_eq!(
                fault_of(verified),
                over_bound(page, HeaderField::NumValues, claimed, rows),
                "{claimed} values declared"
            );
        }
        let (metadata, chunks) = with_first_group_header(
            &opened,
            &loaded,
            key_leaf,
            page,
            HeaderField::UncompressedPageSize,
            i32::MAX,
        )
        .await;
        assert_eq!(
            fault_of(verify_groups(&metadata, key_leaf, KeyShape::Long, &chunks)),
            over_bound(
                page,
                HeaderField::UncompressedPageSize,
                i32::MAX,
                page_bound(rows)
            )
        );
    }

    /// A key page declaring more than `MOST_KEY_PAGE_BYTES` uncompressed is
    /// refused even when the footer claims so many rows for its row group
    /// that the span bound alone would admit it.
    #[tokio::test]
    async fn test_verify_refuses_a_key_page_over_the_ceiling_whatever_its_footer_claims() {
        let opened = store(&Fixture::contract(KeyType::Long, contract_rows())).await;
        let loaded = opened.handle.load_metadata().await.unwrap();
        let key_leaf = 0;
        let last = PAGES_PER_GROUP - 1;
        let page = ChunkPage::Data(last);
        let claimed = i32::try_from(MOST_KEY_PAGE_BYTES + 1).unwrap();
        let (patched, chunks) = with_first_group_header(
            &opened,
            &loaded,
            key_leaf,
            page,
            HeaderField::UncompressedPageSize,
            claimed,
        )
        .await;
        let metadata = with_group_rows(&patched, 0, INFLATED_GROUP_ROWS);
        let span = INFLATED_GROUP_ROWS - pages_of(&metadata, key_leaf)[last].first_row_index;
        assert!(
            page_bound(u64::try_from(span).unwrap()) > u64::try_from(claimed).unwrap(),
            "the span bound alone admits the claim"
        );
        assert_eq!(
            fault_of(verify_groups(&metadata, key_leaf, KeyShape::Long, &chunks)),
            over_bound(
                page,
                HeaderField::UncompressedPageSize,
                claimed,
                MOST_KEY_PAGE_BYTES
            )
        );
    }

    /// A key dictionary declaring more entries than `MOST_KEY_PAGE_BYTES`
    /// holds at the fewest bytes an `INT64` entry takes is refused, even
    /// when the footer claims more rows for its row group than that.
    #[tokio::test]
    async fn test_verify_refuses_a_dictionary_counting_more_entries_than_the_ceiling_holds() {
        let mut fixture = Fixture::contract(KeyType::Long, contract_rows());
        fixture.layout.dictionary = Dictionary::Key;
        let opened = store(&fixture).await;
        let loaded = opened.handle.load_metadata().await.unwrap();
        let key_leaf = 0;
        let page = ChunkPage::Dictionary;
        let most = MOST_KEY_PAGE_BYTES / count(size_of::<i64>());
        let claimed = i32::try_from(most + 1).unwrap();
        let (patched, chunks) = with_first_group_header(
            &opened,
            &loaded,
            key_leaf,
            page,
            HeaderField::NumValues,
            claimed,
        )
        .await;
        let metadata = with_group_rows(&patched, 0, INFLATED_GROUP_ROWS);
        assert!(
            INFLATED_GROUP_ROWS > i64::from(claimed),
            "the row bound alone admits the claim"
        );
        assert_eq!(
            fault_of(verify_groups(&metadata, key_leaf, KeyShape::Long, &chunks)),
            over_bound(page, HeaderField::NumValues, claimed, most)
        );
    }

    /// A byte-array key dictionary counting fewer entries than
    /// `MOST_KEY_PAGE_BYTES` holds at the fewest bytes an entry encodes to,
    /// but more than it holds at the bytes parquet keeps a decoded entry
    /// in, is refused before parquet sizes the dictionary by the count.
    #[tokio::test]
    async fn test_verify_refuses_a_key_dictionary_over_the_ceiling_in_memory() {
        let mut fixture = Fixture::contract(KeyType::String, contract_rows());
        fixture.layout.dictionary = Dictionary::Key;
        let opened = store(&fixture).await;
        let loaded = opened.handle.load_metadata().await.unwrap();
        let key_leaf = 0;
        let page = ChunkPage::Dictionary;
        let held = MOST_KEY_PAGE_BYTES / count(size_of::<ByteArray>());
        let encoded = MOST_KEY_PAGE_BYTES / count(size_of::<u32>());
        let claimed = i32::try_from(held + 1).unwrap();
        assert!(
            u64::try_from(claimed).unwrap() <= encoded,
            "the encoded bound alone admits the claim"
        );
        let (patched, chunks) = with_first_group_header(
            &opened,
            &loaded,
            key_leaf,
            page,
            HeaderField::NumValues,
            claimed,
        )
        .await;
        let metadata = with_group_rows(&patched, 0, INFLATED_GROUP_ROWS);
        assert_eq!(
            fault_of(verify_groups(&metadata, key_leaf, KeyShape::Str, &chunks)),
            over_bound(page, HeaderField::NumValues, claimed, held)
        );
    }

    /// A byte-array key dictionary whose header counts one entry more than
    /// its body holds passes every header bound: the count stays below the
    /// row group's rows, which repeated keys leave above the distinct keys
    /// the dictionary holds. Parquet panics decoding the entry that is not
    /// there; the load ends in `FileFault::DecoderPanicked` naming the
    /// dictionary page, in every tier, before the repeated keys are seen.
    #[tokio::test]
    async fn test_build_a_key_dictionary_short_of_its_count_is_a_file_fault() {
        let repeats = 2;
        let keys = (0..contract_rows())
            .map(|i| Some(key_for(i / repeats, KeyType::String)))
            .collect();
        let mut fixture = Fixture::with_keys(column_of(KeyType::String), keys);
        fixture.layout.dictionary = Dictionary::Key;
        let key_leaf = 0;
        let (short, written) = with_dictionary_one_entry_short(&fixture.write(), key_leaf);
        assert_eq!(u64::from(written), count(group_rows() / repeats));
        let opened = store_file(short).await;
        for residency in Residency::ALL {
            let dir = tempfile::tempdir().unwrap();
            let fault = fault_of(
                opened
                    .build_in(KeyType::String, residency, dir.path())
                    .await,
            );
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
                "{residency:?}: {fault:?}"
            );
        }
    }

    /// A key page whose header declares compressed bytes past its place in
    /// the offset index is refused.
    #[tokio::test]
    async fn test_verify_refuses_a_key_page_whose_compressed_size_runs_past_its_place() {
        let opened = store(&Fixture::contract(KeyType::Long, contract_rows())).await;
        let loaded = opened.handle.load_metadata().await.unwrap();
        let key_leaf = 0;
        let chunks = key_chunks(&opened, &loaded, key_leaf).await;
        let page = ChunkPage::Data(0);
        let written = header_of(&loaded, key_leaf, &chunks, page).compressed_page_size;
        let written = i32::try_from(written).unwrap();
        for claimed in [written, written + 1] {
            let (metadata, chunks) = with_first_group_header(
                &opened,
                &loaded,
                key_leaf,
                page,
                HeaderField::CompressedPageSize,
                claimed,
            )
            .await;
            let verified = verify_groups(&metadata, key_leaf, KeyShape::Long, &chunks);
            if claimed == written {
                assert_eq!(verified.unwrap(), ROW_GROUPS, "the size as written");
                continue;
            }
            assert_eq!(
                fault_of(verified),
                over_bound(
                    page,
                    HeaderField::CompressedPageSize,
                    claimed,
                    u64::try_from(written).unwrap()
                )
            );
        }
    }

    /// A v2 key page whose level lengths are negative, or add up past its
    /// uncompressed size, is refused before parquet reads the page, which
    /// adds the two as `i32`s: two near `i32::MAX` overflow the add. The
    /// file as written, a v2 file, loads.
    #[tokio::test]
    async fn test_verify_refuses_a_v2_key_page_whose_level_lengths_overflow_its_size() {
        let mut fixture = Fixture::contract(KeyType::Long, contract_rows());
        fixture.layout.writer_version = WriterVersion::PARQUET_2_0;
        let opened = store(&fixture).await;
        let loaded = opened.handle.load_metadata().await.unwrap();
        let key_leaf = 0;
        let chunks = key_chunks(&opened, &loaded, key_leaf).await;
        let page = ChunkPage::Data(0);
        let written = header_of(&loaded, key_leaf, &chunks, page);
        assert!(
            matches!(written.body, PageBody::DataV2 { .. }),
            "{:?} is not a v2 page",
            written.body
        );
        assert_eq!(
            verify_groups(&loaded, key_leaf, KeyShape::Long, &chunks).unwrap(),
            ROW_GROUPS,
            "the file as written"
        );
        let near_max = i32::MAX;
        let (metadata, chunks) = with_first_group_headers(
            &opened,
            &loaded,
            key_leaf,
            page,
            &[
                (HeaderField::DefinitionLevelsByteLength, near_max),
                (HeaderField::RepetitionLevelsByteLength, near_max),
            ],
        )
        .await;
        let near_max = u64::try_from(near_max).unwrap();
        assert_eq!(
            fault_of(verify_groups(&metadata, key_leaf, KeyShape::Long, &chunks)),
            FileFault::PageHeaderOverBound {
                row_group: 0,
                page,
                field: HeaderField::LevelsByteLength,
                declared: near_max + near_max,
                allowed: u64::from(written.uncompressed_page_size),
            }
        );
        for field in [
            HeaderField::DefinitionLevelsByteLength,
            HeaderField::RepetitionLevelsByteLength,
        ] {
            let (metadata, chunks) =
                with_first_group_header(&opened, &loaded, key_leaf, page, field, -1).await;
            assert_eq!(
                fault_of(verify_groups(&metadata, key_leaf, KeyShape::Long, &chunks)),
                FileFault::MalformedPageHeader {
                    row_group: 0,
                    page,
                    fault: PageHeaderFault::Negative(field),
                },
                "{field}"
            );
        }
    }

    /// A key page header that parquet would not read to the numbers the
    /// pre-scan reads, or that is not a data page where the offset index
    /// places one, is refused as malformed.
    #[tokio::test]
    async fn test_verify_refuses_a_malformed_key_page_header() {
        let opened = store(&Fixture::contract(KeyType::Long, contract_rows())).await;
        let loaded = opened.handle.load_metadata().await.unwrap();
        let key_leaf = 0;
        let page = ChunkPage::Data(0);
        let malformed = |fault| FileFault::MalformedPageHeader {
            row_group: 0,
            page,
            fault,
        };
        let past_known_types = PageType::DATA_PAGE_V2 as i32 + 1;
        for (field, claimed, fault) in [
            (
                HeaderField::PageType,
                PageType::INDEX_PAGE as i32,
                PageHeaderFault::UnexpectedPageType(PageType::INDEX_PAGE),
            ),
            (
                HeaderField::PageType,
                past_known_types,
                PageHeaderFault::UnknownPageType(past_known_types),
            ),
            (
                HeaderField::UncompressedPageSize,
                -1,
                PageHeaderFault::Negative(HeaderField::UncompressedPageSize),
            ),
            (
                HeaderField::NumValues,
                -1,
                PageHeaderFault::Negative(HeaderField::NumValues),
            ),
        ] {
            let (metadata, chunks) =
                with_first_group_header(&opened, &loaded, key_leaf, page, field, claimed).await;
            assert_eq!(
                fault_of(verify_groups(&metadata, key_leaf, KeyShape::Long, &chunks)),
                malformed(fault),
                "{field} {claimed}"
            );
        }
        let ranges = key_chunk_ranges(&loaded, key_leaf, opened.handle.size());
        let mut parts = fetch_chunk_parts(&opened.handle, &ranges).await.unwrap();
        let mut bytes = parts[0].1.to_vec();
        let past_known_wire_types = WireType::Struct.code() + 1;
        let at = header_at(&loaded, key_leaf, page);
        bytes[at] = (bytes[at] & !NIBBLE_MASK) | past_known_wire_types;
        parts[0].1 = Bytes::from(bytes);
        let chunks = Arc::new(Segments::new(parts, opened.handle.size()));
        assert_eq!(
            fault_of(verify_groups(&loaded, key_leaf, KeyShape::Long, &chunks)),
            malformed(PageHeaderFault::UnknownWireType(past_known_wire_types))
        );
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
        let held = |segments: &Segments| -> usize {
            segments.parts().iter().map(|(_, bytes)| bytes.len()).sum()
        };
        assert_eq!(
            held(&chunks),
            held(&full.key_chunks_for_verification().await.unwrap())
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

    /// The key column's filters of the fixture behind `opened`, read as
    /// `build` reads them, from the ranges `metadata` bounds.
    async fn filters_of(opened: &Opened, metadata: &ParquetMetaData) -> Result<Vec<Sbbf>> {
        let key_leaf = 0;
        let ranges = bloom_ranges(TABLE, &opened.handle, metadata, key_leaf)?;
        read_bloom_filters(&opened.handle, &ranges).await
    }

    /// A filter whose bitset holds no block, or whose footer length cuts
    /// the bitset short, is refused at build: a filter of no block has no
    /// first block to index on a check, and a cut bitset answers for keys
    /// it never saw.
    #[tokio::test]
    async fn test_read_bloom_filters_refuses_a_filter_of_no_block_or_one_cut_short() {
        let fixture = Fixture::contract(KeyType::Long, FEW_ROWS);
        let file = fixture.write();
        let metadata = ParquetMetaDataReader::new()
            .parse_and_finish(&file)
            .unwrap();
        let key_leaf = 0;
        let column = metadata.row_group(0).column(key_leaf);
        let offset = usize::try_from(column.bloom_filter_offset().unwrap()).unwrap();
        let length = column.bloom_filter_length().unwrap();
        let intact = store_file(file.clone()).await;
        let filters = filters_of(&intact, &metadata).await.unwrap();
        assert_eq!(filters.len(), 1);
        assert!(filters[0].num_blocks() > 0);

        // A filter of no block as parquet serializes one: a header saying
        // zero bitset bytes and nothing after it, in place of the written
        // filter, with the footer bounding exactly those bytes.
        let mut no_block = Vec::new();
        Sbbf::new(&[]).write(&mut no_block).unwrap();
        let mut overwritten = file.to_vec();
        overwritten[offset..offset + no_block.len()].copy_from_slice(&no_block);
        let opened = store_file(Bytes::from(overwritten)).await;
        let no_block_footer = with_column_chunk(&metadata, 0, key_leaf, |chunk| {
            chunk.set_bloom_filter_length(Some(i32::try_from(no_block.len()).unwrap()))
        });
        assert_eq!(
            fault_of(filters_of(&opened, &no_block_footer).await),
            FileFault::CorruptBloomFilter { row_group: 0 }
        );

        // The footer bounds one byte less than the header and bitset take.
        let cut_short = with_column_chunk(&metadata, 0, key_leaf, |chunk| {
            chunk.set_bloom_filter_length(Some(length - 1))
        });
        assert_eq!(
            fault_of(filters_of(&intact, &cut_short).await),
            FileFault::CorruptBloomFilter { row_group: 0 }
        );
    }

    #[tokio::test]
    async fn test_disk_tier_reuses_a_matching_chunk_file_and_rewrites_a_stale_one() {
        let rows = contract_rows();
        let dir = tempfile::tempdir().unwrap();
        let opened = store(&Fixture::contract(KeyType::Long, rows)).await;
        let stats = opened.stats();
        let local = testing::local_dir(dir.path(), opened.stats(), None);
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
            reads,
            "no chunk is read back: the chunk file's verification record stands in for them"
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

    /// Every how many keys of the contract fixture the stale-file tests
    /// probe: a prime, so the sample falls at every offset within a page.
    const PROBE_STRIDE: usize = 97;

    /// Every `stride`th key of the contract fixture of `key_type`, the last
    /// key, and the absent key just after each.
    fn probes(key_type: KeyType, stride: usize) -> Vec<Key> {
        (0..contract_rows())
            .step_by(stride)
            .chain([contract_rows() - 1])
            .flat_map(|i| {
                let key = key_for(i, key_type);
                [absent_after(&key), key]
            })
            .collect()
    }

    /// Asserts that `index` holds the range, fences, and longest key
    /// `oracle` does, and answers every `stride`th probe of `key_type` as
    /// `oracle` does: the same row for a present key, none for an absent
    /// one.
    async fn assert_answers_as(
        index: &FileIndex,
        oracle: &FileIndex,
        key_type: KeyType,
        stride: usize,
    ) {
        assert_eq!(index.key_range(), oracle.key_range());
        assert_eq!(index.fences(), oracle.fences());
        assert_eq!(index.max_key_bytes(), oracle.max_key_bytes());
        for key in probes(key_type, stride) {
            assert_eq!(
                index.find(&key, None).await.unwrap(),
                oracle.find(&key, None).await.unwrap(),
                "{key_type} {key}"
            );
        }
    }

    #[tokio::test]
    async fn test_disk_tier_reuse_decodes_no_key_page_and_answers_as_a_fresh_load() {
        for key_type in KeyType::ALL {
            let dir = tempfile::tempdir().unwrap();
            let opened = store(&Fixture::contract(key_type, contract_rows())).await;
            let local = testing::local_dir(dir.path(), opened.stats(), None);
            opened
                .build(key_type, Residency::Disk, Some(&local))
                .await
                .unwrap();
            let oracle = opened.index(key_type).await;

            let reads = opened.stats().local_reads();
            let restarted = store_file(opened.file.clone()).await;
            let reused = restarted
                .build(key_type, Residency::Disk, Some(&local))
                .await
                .unwrap();
            assert_eq!(restarted.stats().gets(), 1, "{key_type}: the footer only");
            assert_eq!(
                opened.stats().local_reads(),
                reads,
                "{key_type}: no chunk was read back to be decoded"
            );
            assert_answers_as(&reused, &oracle, key_type, 1).await;
        }
    }

    /// The chunk file `local` holds for `opened`'s data file.
    fn chunk_file_of(opened: &Opened, local: &LocalDir) -> std::path::PathBuf {
        local.chunk_path(&identity_of(&opened.handle))
    }

    /// A Disk-tier build of the contract fixture of `key_type` in a fresh
    /// local directory: the file, the directory, and its chunk file.
    struct Written {
        _dir: tempfile::TempDir,
        opened: Opened,
        local: Arc<LocalDir>,
        path: std::path::PathBuf,
    }

    /// An edit a test makes to a written chunk file.
    type ChunkFileEdit = fn(&Written);

    async fn written(key_type: KeyType) -> Written {
        let dir = tempfile::tempdir().unwrap();
        let opened = store(&Fixture::contract(key_type, contract_rows())).await;
        let local = testing::local_dir(dir.path(), opened.stats(), None);
        opened
            .build(key_type, Residency::Disk, Some(&local))
            .await
            .unwrap();
        let path = chunk_file_of(&opened, &local);
        Written {
            _dir: dir,
            opened,
            local,
            path,
        }
    }

    impl Written {
        /// A restarted Disk-tier build as `key_type`: a fresh handle over
        /// the same bytes, and its GETs.
        async fn restart(&self, key_type: KeyType) -> (FileIndex, u64) {
            let restarted = store_file(self.opened.file.clone()).await;
            let index = restarted
                .build(key_type, Residency::Disk, Some(&self.local))
                .await
                .unwrap();
            (index, restarted.stats().gets())
        }

        /// Why a restarted Disk-tier build as `key_type` would not trust
        /// the chunk file, `None` for no file; panics when it would.
        async fn stale(&self, key_type: KeyType) -> Option<Stale> {
            let restarted = store_file(self.opened.file.clone()).await;
            let inspected = FileIndex::inspect(
                TABLE,
                Arc::clone(&restarted.handle),
                KEY_COLUMN,
                VALUE_COLUMN,
                key_type,
            )
            .await
            .unwrap();
            let ranges = key_chunk_ranges(
                &inspected.metadata,
                inspected.layout.key_leaf,
                inspected.handle.size(),
            );
            let to_verify = KeysToVerify::of(&inspected);
            reused(&to_verify, &self.local, &ranges)
                .await
                .unwrap()
                .expect_err("the chunk file is trusted")
        }

        /// Asserts that a restart as `key_type` does not trust the chunk
        /// file, for the reason `stale`: it fetches every chunk again,
        /// answers as a fresh load, and rewrites the file, which the
        /// restart after it trusts.
        async fn assert_rebuilt_then_reused(
            &self,
            key_type: KeyType,
            case: &str,
            stale: Option<Stale>,
        ) {
            assert_eq!(self.stale(key_type).await, stale, "{case}");
            let oracle = self.opened.index(key_type).await;
            let (rebuilt, gets) = self.restart(key_type).await;
            assert_eq!(
                gets,
                1 + count(ROW_GROUPS),
                "{case}: footer, then every chunk"
            );
            assert_answers_as(&rebuilt, &oracle, key_type, PROBE_STRIDE).await;
            let reads = self.opened.stats().local_reads();
            let (reused, gets) = self.restart(key_type).await;
            assert_eq!(gets, 1, "{case}: the rewritten file is reused");
            assert_eq!(self.opened.stats().local_reads(), reads, "{case}");
            assert_answers_as(&reused, &oracle, key_type, PROBE_STRIDE).await;
        }
    }

    /// The bytes at the end of a torn chunk file that a crash left
    /// unwritten: zeros, which a filesystem returns for blocks it allocated
    /// and never wrote.
    const UNWRITTEN_TAIL_BYTES: usize = 64;

    /// Sets the length of the file at `path`.
    fn set_len(path: &std::path::Path, len: u64) {
        std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .unwrap()
            .set_len(len)
            .unwrap();
    }

    #[tokio::test]
    async fn test_disk_tier_rebuilds_every_stale_chunk_file_rather_than_failing() {
        let key_type = KeyType::String;
        let cases: [(&str, ChunkFileEdit, Stale); 8] = [
            (
                "payload byte flipped",
                |written| {
                    let len = std::fs::metadata(&written.path).unwrap().len();
                    let last = std::fs::read(&written.path).unwrap()[approx::length(len - 1)];
                    local_testing::overwrite(&written.path, len - 1, &[last ^ GARBAGE]);
                },
                Stale::Rejected(Rejection::ChecksumMismatch),
            ),
            (
                "payload truncated",
                |written| {
                    let len = std::fs::metadata(&written.path).unwrap().len();
                    set_len(&written.path, len - 1);
                },
                Stale::Rejected(Rejection::ShortPayload),
            ),
            (
                "tail never written",
                |written| {
                    let len = std::fs::metadata(&written.path).unwrap().len();
                    let unwritten = [0; UNWRITTEN_TAIL_BYTES];
                    let at = len - count(unwritten.len());
                    local_testing::overwrite(&written.path, at, &unwritten);
                },
                Stale::Rejected(Rejection::ChecksumMismatch),
            ),
            (
                "previous format version",
                |written| {
                    local_testing::set_version_byte(&written.path, local_testing::PREVIOUS_VERSION);
                },
                Stale::Rejected(Rejection::UnsupportedVersion {
                    version: local_testing::PREVIOUS_VERSION,
                }),
            ),
            (
                "verifier version bumped",
                |written| {
                    local_testing::set_verifier_version(&written.path, VERIFIER_VERSION + 1);
                },
                Stale::VerifierVersion {
                    stored: VERIFIER_VERSION + 1,
                },
            ),
            (
                "header garbage",
                |written| local_testing::fill_header(&written.path, GARBAGE),
                Stale::Rejected(Rejection::UndecodableHeader),
            ),
            (
                "record with a key over the longest",
                |written| {
                    local_testing::edit_verification(&written.path, |bytes| {
                        *bytes = record::with_fences(bytes, |fences| {
                            fences[0][0] = vec![b'u'; MAX_KEY_BYTES + 1];
                        });
                    });
                },
                Stale::Record(RecordFault::Key { row_group: 0 }),
            ),
            (
                "record with a fence too many",
                |written| {
                    local_testing::edit_verification(&written.path, |bytes| {
                        *bytes = record::with_fences(bytes, |fences| {
                            let repeated = fences[0][0].clone();
                            fences[0].push(repeated);
                        });
                    });
                },
                Stale::Record(RecordFault::Fences {
                    row_group: 0,
                    stored: PAGES_PER_GROUP + 1,
                }),
            ),
        ];
        for (case, edit, stale) in cases {
            let written = written(key_type).await;
            edit(&written);
            written
                .assert_rebuilt_then_reused(key_type, case, Some(stale))
                .await;
        }
    }

    #[tokio::test]
    async fn test_disk_tier_rebuilds_a_chunk_file_whose_record_is_over_the_footer_bound() {
        let key_type = KeyType::Long;
        let written = written(key_type).await;
        let most = record::most_bytes(&written.opened.index(key_type).await.key_column());
        local_testing::edit_verification(&written.path, |bytes| {
            bytes.resize(approx::length(most + 1), 0);
        });
        written
            .assert_rebuilt_then_reused(
                key_type,
                "record over the bound",
                Some(Stale::Rejected(Rejection::VerificationTooLarge {
                    len: most + 1,
                    most,
                })),
            )
            .await;
    }

    #[tokio::test]
    async fn test_disk_tier_rebuilds_a_chunk_file_verified_as_another_key_type() {
        let written = written(KeyType::String).await;
        written
            .assert_rebuilt_then_reused(
                KeyType::Binary,
                "string file read as binary",
                Some(Stale::KeyType {
                    stored: KeyType::String,
                }),
            )
            .await;
        written
            .assert_rebuilt_then_reused(
                KeyType::String,
                "and back",
                Some(Stale::KeyType {
                    stored: KeyType::Binary,
                }),
            )
            .await;
    }

    #[tokio::test]
    async fn test_disk_tier_serves_through_a_write_torn_before_its_rename() {
        let key_type = KeyType::String;
        let written = written(key_type).await;
        let bytes = std::fs::read(&written.path).unwrap();
        let leftover = local_testing::stale_temp(&written.path);
        std::fs::write(&leftover, &bytes[..bytes.len() / 2]).unwrap();
        std::fs::remove_file(&written.path).unwrap();
        written
            .assert_rebuilt_then_reused(key_type, "temporary file left without its rename", None)
            .await;
        let identity = identity_of(&written.opened.handle);
        assert_eq!(
            written
                .local
                .retain(&std::collections::HashSet::from([identity]))
                .unwrap(),
            approx::count(bytes.len() / 2),
            "the leftover is swept, the chunk file kept"
        );
    }

    #[tokio::test]
    async fn test_disk_tier_without_a_local_dir_is_a_config_violation_at_the_footer() {
        let opened = store(&Fixture::contract(KeyType::Long, FEW_ROWS)).await;
        let error = opened
            .build(KeyType::Long, Residency::Disk, None)
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                Error::Config(ConfigError::Invalid(
                    ConfigViolation::DiskResidencyWithoutLocalDir { .. }
                ))
            ),
            "{error:?}"
        );
        assert_eq!(opened.stats().gets(), 1, "no chunk fetched");
    }

    /// The fixture's key column.
    const KEY_LEAF: usize = 0;

    /// How a `verify` ended, as two decoders' ends are compared: what it
    /// established, a refusal, a file fault, or the kind of any other error.
    #[derive(Debug, PartialEq)]
    enum Outcome {
        Verified(Verified),
        Refused(RefusalReason),
        Fault(FileFault),
        Other(ErrorKind),
    }

    fn outcome(result: Result<Verified>) -> Outcome {
        match result {
            Ok(verified) => Outcome::Verified(verified),
            Err(Error::Refused { reason, .. }) => Outcome::Refused(reason),
            Err(Error::File { fault, .. }) => Outcome::Fault(fault),
            Err(error) => Outcome::Other(error.kind()),
        }
    }

    /// The key column of `metadata` over `chunks` verified by each decoder:
    /// the outcome, which the plain decoder reaches exactly as the column
    /// reader does, and the decoder that read each row group when the
    /// plain one is asked for. A row group whose decode fails was read by
    /// the column reader, since the plain decoder hands every page it
    /// cannot read exactly to it.
    fn verify_by_both(
        metadata: &ParquetMetaData,
        shape: KeyShape,
        chunks: &Arc<Segments>,
    ) -> (Outcome, Vec<KeyDecoder>) {
        let path = location();
        let column = Column {
            metadata,
            leaf: KEY_LEAF,
            path: &path,
        };
        let plain = outcome(verify_with(
            TABLE,
            &column,
            shape,
            chunks,
            KeyDecoder::Plain,
        ));
        let reader = outcome(verify_with(
            TABLE,
            &column,
            shape,
            chunks,
            KeyDecoder::ColumnReader,
        ));
        assert_eq!(plain, reader, "the plain decoder against the column reader");
        let taken = (0..metadata.num_row_groups())
            .map(|row_group| {
                check_run(
                    column.group(row_group),
                    shape,
                    Arc::clone(chunks),
                    KeyDecoder::Plain,
                )
                .map_or(KeyDecoder::ColumnReader, |(_, decoder)| decoder)
            })
            .collect();
        (plain, taken)
    }

    /// `verify_by_both` over the fixture file behind `opened`.
    async fn verified_by_both(opened: &Opened, shape: KeyShape) -> (Outcome, Vec<KeyDecoder>) {
        let loaded = opened.handle.load_metadata().await.unwrap();
        let chunks = key_chunks(opened, &loaded, KEY_LEAF).await;
        verify_by_both(&loaded, shape, &chunks)
    }

    /// `verified_by_both` of the string keys of `file`.
    async fn file_by_both(file: Bytes) -> (Outcome, Vec<KeyDecoder>) {
        verified_by_both(&store_file(file).await, KeyShape::Str).await
    }

    /// `verified_by_both` of `fixture` read as `shape`.
    async fn fixture_by_both(fixture: &Fixture, shape: KeyShape) -> (Outcome, Vec<KeyDecoder>) {
        verified_by_both(&store(fixture).await, shape).await
    }

    /// The plain decoder for every row group of the contract layout's
    /// string and binary tables: a regression to the column reader is
    /// caught here rather than in a profile.
    #[tokio::test]
    async fn test_check_run_reads_the_contract_string_and_binary_tables_plain() {
        for (key_type, shape) in [
            (KeyType::String, KeyShape::Str),
            (KeyType::Binary, KeyShape::Bin),
        ] {
            let fixture = Fixture::contract(key_type, contract_rows());
            let (outcome, taken) = fixture_by_both(&fixture, shape).await;
            assert!(matches!(outcome, Outcome::Verified(_)), "{outcome:?}");
            assert_eq!(taken, [KeyDecoder::Plain; ROW_GROUPS], "{key_type}");
        }
    }

    /// A layout edit a fixture is written under, named for the messages,
    /// and whether it leaves a string or binary key column one the plain
    /// decoder reads: `PLAIN` pages and no dictionary.
    type Edit = (&'static str, fn(&mut Fixture), bool);

    /// The layouts the plain decoder is held to the column reader on: the
    /// contract's, and every writer knob the index tests turn, alone and in
    /// the combinations that change where the values start in a page. A v2
    /// writer encodes byte arrays by deltas unless told `PLAIN`.
    const EDITS: &[Edit] = &[
        ("contract", |_| {}, true),
        (
            "one row group",
            |fixture| fixture.layout.row_group_rows = contract_rows(),
            true,
        ),
        (
            "pages cut by bytes",
            |fixture| {
                fixture.layout.page_bytes = SMALL_PAGE_BYTES;
                fixture.layout.write_batch = SMALL_BATCH;
            },
            true,
        ),
        (
            "no statistics",
            |fixture| fixture.layout.statistics = EnabledStatistics::None,
            true,
        ),
        (
            "dictionary",
            |fixture| fixture.layout.dictionary = Dictionary::Key,
            false,
        ),
        (
            "dictionary, nullable, v2",
            |fixture| {
                fixture.layout.dictionary = Dictionary::Key;
                fixture.nullable_key = true;
                fixture.layout.writer_version = WriterVersion::PARQUET_2_0;
            },
            false,
        ),
        ("nullable", |fixture| fixture.nullable_key = true, true),
        (
            "v2",
            |fixture| fixture.layout.writer_version = WriterVersion::PARQUET_2_0,
            false,
        ),
        (
            "v2 plain",
            |fixture| {
                fixture.layout.writer_version = WriterVersion::PARQUET_2_0;
                fixture.layout.key_encoding = Some(Encoding::PLAIN);
            },
            true,
        ),
        (
            "nullable, v2 plain",
            |fixture| {
                fixture.nullable_key = true;
                fixture.layout.writer_version = WriterVersion::PARQUET_2_0;
                fixture.layout.key_encoding = Some(Encoding::PLAIN);
            },
            true,
        ),
        (
            "uncompressed",
            |fixture| fixture.layout.compression = Compression::UNCOMPRESSED,
            true,
        ),
        (
            "uncompressed, nullable",
            |fixture| {
                fixture.layout.compression = Compression::UNCOMPRESSED;
                fixture.nullable_key = true;
            },
            true,
        ),
        (
            "uncompressed, nullable, v2 plain",
            |fixture| {
                fixture.layout.compression = Compression::UNCOMPRESSED;
                fixture.nullable_key = true;
                fixture.layout.writer_version = WriterVersion::PARQUET_2_0;
                fixture.layout.key_encoding = Some(Encoding::PLAIN);
            },
            true,
        ),
        (
            "snappy",
            |fixture| fixture.layout.compression = Compression::SNAPPY,
            true,
        ),
        (
            "snappy, nullable, v2 plain",
            |fixture| {
                fixture.layout.compression = Compression::SNAPPY;
                fixture.nullable_key = true;
                fixture.layout.writer_version = WriterVersion::PARQUET_2_0;
                fixture.layout.key_encoding = Some(Encoding::PLAIN);
            },
            true,
        ),
    ];

    /// A page byte bound under a key page of the contract's rows of any
    /// key type, so pages end by bytes at uneven row counts.
    const SMALL_PAGE_BYTES: usize = 1024;

    /// Every key type and every layout of `EDITS`: the plain decoder
    /// reaches what the column reader reaches, and reads every row group of
    /// a string or binary key column written without a dictionary.
    #[tokio::test]
    async fn test_verify_plain_decoder_matches_the_column_reader_on_every_fixture_shape() {
        let fixed_keys: Vec<Option<Key>> = (0..contract_rows())
            .map(|i| Some(Key::Bytes(count(stepped(i)).to_be_bytes().to_vec())))
            .collect();
        let fixtures: [(Fixture, KeyShape); 5] = [
            (
                Fixture::contract(KeyType::String, contract_rows()),
                KeyShape::Str,
            ),
            (
                Fixture::contract(KeyType::Binary, contract_rows()),
                KeyShape::Bin,
            ),
            (
                Fixture::contract(KeyType::Long, contract_rows()),
                KeyShape::Long,
            ),
            (
                Fixture::contract(KeyType::Uuid, contract_rows()),
                KeyShape::Uuid,
            ),
            (
                Fixture::with_keys(KeyColumn::FixedBinary(FIXED_KEY_BYTES), fixed_keys),
                KeyShape::FixedBin,
            ),
        ];
        let expected_compared = fixtures.len() * EDITS.len();
        let mut compared = 0;
        for (mut fixture, shape) in fixtures {
            let as_written = fixture.layout;
            for (label, edit, plain_pages) in EDITS {
                fixture.layout = as_written;
                fixture.nullable_key = false;
                edit(&mut fixture);
                let (outcome, taken) = fixture_by_both(&fixture, shape).await;
                assert!(
                    matches!(outcome, Outcome::Verified(_)),
                    "{shape:?} {label}: {outcome:?}"
                );
                let expected = if *plain_pages && matches!(shape, KeyShape::Str | KeyShape::Bin) {
                    KeyDecoder::Plain
                } else {
                    KeyDecoder::ColumnReader
                };
                assert!(
                    taken.iter().all(|decoder| *decoder == expected),
                    "{shape:?} {label}: {taken:?}"
                );
                compared += 1;
            }
        }
        assert_eq!(compared, expected_compared);
    }

    /// `FEW_ROWS` string keys from `key_for`, with `edit` applied.
    fn few_strings(edit: impl FnOnce(&mut Vec<Option<Key>>)) -> Fixture {
        let mut keys = (0..FEW_ROWS)
            .map(|i| Some(key_for(i, KeyType::String)))
            .collect();
        edit(&mut keys);
        Fixture::with_keys(KeyColumn::Utf8, keys)
    }

    /// The refusals a row group's keys draw are drawn through the plain
    /// decoder as through the column reader: a null, which the plain
    /// decoder leaves to the column reader since the page's levels are not
    /// all present, and, read plain, a key that is not UTF-8, a key out of
    /// order, a duplicate, and a key over the cap.
    #[tokio::test]
    async fn test_verify_plain_decoder_refuses_what_the_column_reader_refuses() {
        let path = location();
        let at = FEW_ROWS / 2;
        let null = few_strings(|keys| keys[at] = None);
        assert_eq!(
            fixture_by_both(&null, KeyShape::Str).await,
            (
                Outcome::Refused(RefusalReason::NullKeys {
                    path: path.clone(),
                    row_group: 0
                }),
                vec![KeyDecoder::ColumnReader]
            )
        );

        let not_utf8 = Fixture::with_keys(
            KeyColumn::Binary,
            (0..FEW_ROWS)
                .map(|i| Some(key_for(i, KeyType::Binary)))
                .collect(),
        );
        assert_eq!(
            fixture_by_both(&not_utf8, KeyShape::Str).await,
            (
                Outcome::Fault(FileFault::KeyNotUtf8 { row_group: 0 }),
                vec![KeyDecoder::Plain]
            )
        );

        let unsorted = few_strings(|keys| keys.swap(at, at + 1));
        assert_eq!(
            fixture_by_both(&unsorted, KeyShape::Str).await,
            (
                Outcome::Refused(RefusalReason::Unsorted {
                    path: path.clone(),
                    row_group: 0
                }),
                vec![KeyDecoder::Plain]
            )
        );

        let duplicate = few_strings(|keys| keys[at + 1] = keys[at].clone());
        assert_eq!(
            fixture_by_both(&duplicate, KeyShape::Str).await,
            (
                Outcome::Refused(RefusalReason::DuplicateKeys {
                    path: path.clone(),
                    key: key_for(at, KeyType::String)
                }),
                vec![KeyDecoder::Plain]
            )
        );

        let over = Fixture::with_keys(KeyColumn::Utf8, keys_of_len(MAX_KEY_BYTES + 1));
        assert_eq!(
            fixture_by_both(&over, KeyShape::Str).await,
            (
                Outcome::Refused(RefusalReason::KeyTooLong {
                    path,
                    bytes: MAX_KEY_BYTES + 1
                }),
                vec![KeyDecoder::Plain]
            )
        );
    }

    /// A duplicate whose copies straddle a page boundary, a decode batch
    /// (`DECODE_BATCH_RECORDS`) inside one page, or a row group boundary is
    /// refused through the plain decoder, and so is a key that is not UTF-8
    /// in a page's second batch; the same row groups without the defect
    /// verify alike through both decoders, fences and all.
    #[tokio::test]
    async fn test_verify_plain_decoder_checks_across_pages_batches_and_row_groups() {
        let batch = row_index(DECODE_BATCH_RECORDS);
        let rows = 2 * batch;
        let written_as = |column: KeyColumn, edit: &dyn Fn(&mut Vec<Option<Key>>)| {
            let mut keys: Vec<Option<Key>> = (0..rows)
                .map(|i| Some(key_for(i, KeyType::String)))
                .collect();
            edit(&mut keys);
            let mut fixture = Fixture::with_keys(column, keys);
            fixture.layout.row_group_rows = rows;
            fixture
        };
        let strings = |edit: &dyn Fn(&mut Vec<Option<Key>>)| written_as(KeyColumn::Utf8, edit);
        let one_page = |mut fixture: Fixture| {
            fixture.layout.page_rows = rows;
            fixture
        };
        let duplicate_at =
            |at: usize| move |keys: &mut Vec<Option<Key>>| keys[at] = keys[at - 1].clone();
        let refused_at = |at: usize| {
            Outcome::Refused(RefusalReason::DuplicateKeys {
                path: location(),
                key: key_for(at - 1, KeyType::String),
            })
        };

        let (clean, taken) = fixture_by_both(&strings(&|_| {}), KeyShape::Str).await;
        let Outcome::Verified(clean) = clean else {
            panic!("{clean:?}");
        };
        let fences: Vec<Key> = (0..rows)
            .step_by(page_rows())
            .map(|i| key_for(i, KeyType::String))
            .collect();
        assert_eq!(clean.fences, [fences]);
        assert_eq!(taken, [KeyDecoder::Plain]);
        let (clean, taken) = fixture_by_both(&one_page(strings(&|_| {})), KeyShape::Str).await;
        let Outcome::Verified(clean) = clean else {
            panic!("{clean:?}");
        };
        assert_eq!(clean.fences, [vec![key_for(0, KeyType::String)]]);
        assert_eq!(taken, [KeyDecoder::Plain]);

        let across_pages = strings(&duplicate_at(page_rows()));
        assert_eq!(
            fixture_by_both(&across_pages, KeyShape::Str).await,
            (refused_at(page_rows()), vec![KeyDecoder::Plain])
        );
        let across_batches = one_page(strings(&duplicate_at(batch)));
        assert_eq!(
            fixture_by_both(&across_batches, KeyShape::Str).await,
            (refused_at(batch), vec![KeyDecoder::Plain])
        );
        let not_utf8_at = batch + 1;
        let late = one_page(written_as(KeyColumn::Binary, &|keys| {
            let mut bytes = key_for(not_utf8_at, KeyType::String).as_bytes().to_vec();
            // Above the key itself and below the next: order holds.
            bytes.push(u8::MAX);
            keys[not_utf8_at] = Some(Key::Bytes(bytes));
        }));
        assert_eq!(
            fixture_by_both(&late, KeyShape::Str).await,
            (
                Outcome::Fault(FileFault::KeyNotUtf8 { row_group: 0 }),
                vec![KeyDecoder::Plain]
            )
        );

        let mut across_groups =
            few_strings(|keys| keys[TINY_GROUP_ROWS] = keys[TINY_GROUP_ROWS - 1].clone());
        across_groups.layout.row_group_rows = TINY_GROUP_ROWS;
        let (outcome, taken) = fixture_by_both(&across_groups, KeyShape::Str).await;
        assert_eq!(outcome, refused_at(TINY_GROUP_ROWS));
        assert!(
            taken.iter().all(|decoder| *decoder == KeyDecoder::Plain),
            "{taken:?}"
        );
    }

    /// A string key page whose rows contradict its offset index, however the
    /// footer contradicts them, is left by the plain decoder to the column
    /// reader, and refused as it refuses it.
    #[tokio::test]
    async fn test_verify_plain_decoder_leaves_a_page_its_offset_index_contradicts_to_the_column_reader()
     {
        let opened = store(&Fixture::contract(KeyType::String, contract_rows())).await;
        let loaded = opened.handle.load_metadata().await.unwrap();
        let chunks = key_chunks(&opened, &loaded, KEY_LEAF).await;
        let mismatch = |page| Outcome::Fault(FileFault::PageRowsMismatch { row_group: 0, page });

        let short_group = with_group_rows(&loaded, 0, PAGE_ROWS + PAGE_ROWS / 2);
        let (outcome, taken) = verify_by_both(&short_group, KeyShape::Str, &chunks);
        assert_eq!(outcome, mismatch(1));
        assert_eq!(taken[0], KeyDecoder::ColumnReader);

        let mut shifted = pages_of(&loaded, KEY_LEAF);
        shifted[1].first_row_index -= 1;
        let shifted = with_pages(&loaded, KEY_LEAF, shifted);
        let (outcome, taken) = verify_by_both(&shifted, KeyShape::Str, &chunks);
        assert_eq!(outcome, mismatch(0));
        assert_eq!(taken[0], KeyDecoder::ColumnReader);

        let inflated = with_group_rows(&loaded, 0, UNALLOCATABLE_ROWS);
        let (outcome, taken) = verify_by_both(&inflated, KeyShape::Str, &chunks);
        assert_eq!(outcome, mismatch(PAGES_PER_GROUP - 1));
        assert_eq!(taken[0], KeyDecoder::ColumnReader);
    }

    /// The offset, within `file`, of the length prefix of every value of
    /// row group 0's first key page, a page stored uncompressed whose body
    /// is its values alone, and the lengths those prefixes hold.
    fn value_prefixes(file: &[u8], metadata: &ParquetMetaData) -> Vec<(usize, u32)> {
        let page = &pages_of(metadata, KEY_LEAF)[0];
        let start = usize::try_from(page.offset).unwrap();
        let header = PageHeader::read(&file[start..]).unwrap();
        let PageBody::Data { num_values } = header.body else {
            panic!("{header:?} is not a v1 data page");
        };
        let mut at = start + header.header_len;
        (0..num_values)
            .map(|_| {
                let len = u32::from_le_bytes(
                    file[at..at + BYTE_ARRAY_LENGTH_PREFIX_BYTES]
                        .try_into()
                        .unwrap(),
                );
                let prefix = (at, len);
                at += BYTE_ARRAY_LENGTH_PREFIX_BYTES + usize::try_from(len).unwrap();
                prefix
            })
            .collect()
    }

    /// `file` with the length prefix at `at` holding `len`.
    fn with_prefix(file: &Bytes, at: usize, len: u32) -> Bytes {
        let mut patched = file.to_vec();
        patched[at..at + BYTE_ARRAY_LENGTH_PREFIX_BYTES].copy_from_slice(&len.to_le_bytes());
        Bytes::from(patched)
    }

    /// A page whose values do not fill its bytes exactly is left by the
    /// plain decoder to the column reader, which decides it: a length
    /// prefix running past the page's end is a parquet error,
    /// a length prefix the page's end cuts short a panic, and a byte left
    /// after the last value is read past, so the page verifies with the last
    /// value it read.
    #[tokio::test]
    async fn test_verify_plain_decoder_leaves_a_page_it_cannot_read_exactly_to_the_column_reader() {
        let mut fixture = few_strings(|keys| {
            let Some(Key::Str(last)) = keys.last_mut().unwrap() else {
                panic!("a string fixture holds string keys");
            };
            // Cut off by a prefix one byte shorter, the key is the one
            // `key_for` gives, still above the key before it.
            last.push('z');
        });
        fixture.layout.compression = Compression::UNCOMPRESSED;
        let file = fixture.write();
        let metadata = ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Required)
            .parse_and_finish(&file)
            .unwrap();
        let prefixes = value_prefixes(&file, &metadata);
        let (last_at, last_len) = prefixes[FEW_ROWS - 1];
        let (before_at, before_len) = prefixes[FEW_ROWS - 2];
        let (outcome, taken) = file_by_both(file.clone()).await;
        assert!(matches!(outcome, Outcome::Verified(_)), "{outcome:?}");
        assert_eq!(taken, [KeyDecoder::Plain], "the file as written");

        let past_end = with_prefix(&file, last_at, last_len + 1);
        assert_eq!(
            file_by_both(past_end).await,
            (
                Outcome::Other(ErrorKind::Parquet),
                vec![KeyDecoder::ColumnReader]
            )
        );

        // The value before the last grown over all but two bytes of the
        // last value's prefix, which the page's end then cuts short.
        let prefix_bytes_left = 2;
        let grown =
            u32::try_from(BYTE_ARRAY_LENGTH_PREFIX_BYTES).unwrap() + last_len - prefix_bytes_left;
        let (outcome, taken) =
            file_by_both(with_prefix(&file, before_at, before_len + grown)).await;
        assert!(
            matches!(
                outcome,
                Outcome::Fault(FileFault::DecoderPanicked {
                    part: DecodedPart::Pages {
                        row_group: 0,
                        page: Some(ChunkPage::Data(0))
                    },
                    ..
                })
            ),
            "{outcome:?}"
        );
        assert_eq!(taken, [KeyDecoder::ColumnReader]);

        let (outcome, taken) = file_by_both(with_prefix(&file, last_at, last_len - 1)).await;
        let Outcome::Verified(verified) = outcome else {
            panic!("{outcome:?}");
        };
        assert_eq!(verified.key_range.1, key_for(FEW_ROWS - 1, KeyType::String));
        assert_eq!(taken, [KeyDecoder::ColumnReader]);
    }
}
