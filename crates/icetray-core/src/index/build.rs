//! The chunk stage of the index: the key column chunks fetched once, read
//! back from a chunk file, or dropped for their filters, and every key of
//! the file decoded and checked against the contract (RFD 007, RFD 017).

use std::cmp::Ordering;
use std::ops::Range;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex, PoisonError};

use bytes::Bytes;
use parquet::bloom_filter::Sbbf;
use parquet::column::page::{Page, PageMetadata, PageReader};
use parquet::column::reader::ColumnReaderImpl;
use parquet::data_type::{
    ByteArray, ByteArrayType, DataType as ParquetDataType, FixedLenByteArrayType, Int64Type,
};
use parquet::errors::ParquetError;
use parquet::file::serialized_reader::SerializedPageReader;

use super::inspect::{bloom_ranges, key_chunk_ranges};
use super::page_header::check_page_headers;
use super::{
    ChunkPage, Column, ColumnChunk, DecodedPart, FileFault, FileIndex, GroupRange, Inspected,
    KeyResidency, KeyShape, Keys, file_fault, identity_of, off_worker, row_index, unless_panicked,
};
use crate::approx;
use crate::config::{ConfigError, ConfigViolation, Residency};
use crate::contract::{MAX_KEY_BYTES, PAGE_ROWS};
use crate::error::{Error, RefusalReason, Result};
use crate::key::{Key, UUID_BYTES};
use crate::local::{DiskChunks, LocalDir};
use crate::parquet_io::{FileHandle, Segments};

impl Inspected {
    /// The chunk stage of `FileIndex::build`: streams the key chunks once
    /// for verification and keeps what `residency` keeps, as `build`
    /// documents. The Disk tier with no local directory is the one rule the
    /// loader re-checks, since a configuration built in code may skip
    /// `Config::validated`; it is raised here as the same
    /// `ConfigViolation`, before any chunk is fetched.
    pub(crate) async fn build(
        self,
        residency: Residency,
        local: Option<&LocalDir>,
    ) -> Result<FileIndex> {
        let Self {
            table,
            handle,
            metadata,
            layout,
            predicted: _,
        } = self;
        let column = Column {
            metadata: &metadata,
            leaf: layout.key_leaf,
            path: &handle.location,
        };
        let ranges = key_chunk_ranges(&metadata, layout.key_leaf, handle.size());
        let (keys, verified) = match residency {
            Residency::Full => {
                let (chunks, verified) =
                    fetch_verified(&table, &handle, &column, layout.key_shape, &ranges).await?;
                (KeyResidency::Chunks(chunks), verified)
            }
            Residency::Disk => {
                let local = local.ok_or(Error::Config(ConfigError::Invalid(
                    ConfigViolation::DiskResidencyWithoutLocalDir,
                )))?;
                let (disk, verified) =
                    disk_chunks(&table, &handle, local, &column, layout.key_shape, &ranges).await?;
                (KeyResidency::Disk(disk), verified)
            }
            Residency::Bloom => {
                let filter_ranges = bloom_ranges(&table, &handle, &metadata, layout.key_leaf)?;
                let (chunks, verified) =
                    fetch_verified(&table, &handle, &column, layout.key_shape, &ranges).await?;
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

/// A page reader that checks the value count in the header of every data
/// page before the column reader sees it, and records it, so that what a
/// decode produced can be checked against the rows the offset index gives
/// those pages. The column reader takes the header's count as the values
/// the page holds, so a data page claiming more values than the offset
/// index gives it rows (`check_pages_read` requires one value per row of
/// every page read, nulls included) is refused here, before the reader
/// holds it, and recorded as refused. The dictionary page is handed through unchecked and uncounted; the pages
/// the column reader skips from their locations are never read and never
/// seen.
struct CountedPages {
    inner: SerializedPageReader<Segments>,
    /// The rows the offset index gives each of the chunk's data pages, in
    /// page order: the pages the inner reader hands out or skips, one for
    /// one, since it reads by those locations.
    page_rows: Vec<usize>,
    /// The position among the chunk's data pages of the next data page
    /// handed out or skipped.
    next_page: usize,
    tally: Arc<Mutex<PageTally>>,
}

/// What a `CountedPages` saw, shared with the `read_column` call that reads
/// through it, since the column reader owns the page reader.
#[derive(Default)]
struct PageTally {
    /// The header value count of every data page handed to the column
    /// reader, in order.
    seen: Vec<usize>,
    /// The data page refused for a header that claims more values than its
    /// rows; the reader's error then stands for this refusal.
    refused: Option<usize>,
    /// The last page handed to the column reader, dictionary or data: the
    /// page a panic in parquet's decoding is reported at.
    taken: Option<ChunkPage>,
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
                let values = approx::length(*num_values);
                let at = self.next_page;
                self.next_page += 1;
                let rows = self.page_rows[at];
                let mut tally = self.tally.lock().unwrap_or_else(PoisonError::into_inner);
                if values > rows {
                    tally.refused = Some(at);
                    return Err(ParquetError::General(format!(
                        "data page {at} claims {values} values for {rows} rows"
                    )));
                }
                tally.seen.push(values);
                tally.taken = Some(ChunkPage::Data(at));
            }
            Some(Page::DictionaryPage { .. }) => {
                self.tally
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .taken = Some(ChunkPage::Dictionary);
            }
            None => {}
        }
        Ok(page)
    }

    fn peek_next_page(&mut self) -> parquet::errors::Result<Option<PageMetadata>> {
        self.inner.peek_next_page()
    }

    fn skip_next_page(&mut self) -> parquet::errors::Result<()> {
        let data_page = self
            .inner
            .peek_next_page()?
            .is_some_and(|page| !page.is_dict);
        self.inner.skip_next_page()?;
        if data_page {
            self.next_page += 1;
        }
        Ok(())
    }

    fn at_record_boundary(&mut self) -> parquet::errors::Result<bool> {
        self.inner.at_record_boundary()
    }
}

/// The key column chunks of `ranges`, fetched and verified: the chunks a
/// tier keeps (Full), writes (Disk), or drops (Bloom), and what
/// verification recorded.
async fn fetch_verified(
    table: &str,
    handle: &FileHandle,
    column: &Column<'_>,
    shape: KeyShape,
    ranges: &[Range<u64>],
) -> Result<(Arc<Segments>, Verified)> {
    let parts = fetch_chunk_parts(handle, ranges).await?;
    let chunks = Arc::new(Segments::new(parts, handle.size()));
    let verified = verify(table, column, shape, &chunks)?;
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
/// counted local read per row group, no GET.
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
    if let Some(disk) = reusable_disk_chunks(handle, local, ranges)? {
        let chunks = chunks_from_disk(&disk, ranges, handle.size()).await?;
        let verified = verify(table, column, shape, &chunks)?;
        return Ok((disk, verified));
    }
    let (chunks, verified) = fetch_verified(table, handle, column, shape, ranges).await?;
    let disk =
        off_worker(|| local.write_chunks(&identity_of(handle), chunks.parts().iter().cloned()))?;
    Ok((disk, verified))
}

/// The chunk file in `local` a Disk-tier build reuses for `handle`'s file:
/// one whose identity (path, size, footer digest) matches and which holds
/// every one of `ranges`, the key column chunks. `None` when there is none,
/// and the chunks are fetched and written instead.
pub(super) fn reusable_disk_chunks(
    handle: &FileHandle,
    local: &LocalDir,
    ranges: &[Range<u64>],
) -> Result<Option<DiskChunks>> {
    Ok(local
        .chunks_for(&identity_of(handle))?
        .filter(|disk| ranges.iter().all(|range| disk.holds(range))))
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
/// it is unsorted), none over `contract::MAX_KEY_BYTES`. Every key page
/// header of a row group is checked against what its page can hold before
/// the row group is decoded (`check_page_headers`), and every key page
/// against the rows its offset index gives it on the way. Records
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
        let chunk = column.group(row_group);
        let rows = chunk.rows();
        check_page_headers(chunk, shape, chunks)?;
        let keys = decode_run(chunk, shape, 0..rows, Arc::clone(chunks))?;
        if keys.len() < row_index(rows) {
            let reason = RefusalReason::NullKeys {
                path: path.to_string(),
                row_group,
            };
            return Err(Error::refused(table, reason));
        }
        check_utf8(chunk, &keys)?;
        longest = longest.max(keys.max_len());
        if keys.is_empty() {
            fences.push(Vec::new());
            continue;
        }
        if let Some((at, order)) = keys.first_not_increasing() {
            let reason = not_above_previous(chunk, order, keys.get(at));
            return Err(Error::refused(table, reason));
        }
        let first = keys.get(0);
        let last = keys.get(keys.len() - 1);
        if let Some(previous) = groups.last() {
            let order = first.cmp(&previous.last);
            if order != Ordering::Greater {
                return Err(Error::refused(
                    table,
                    not_above_previous(chunk, order, first),
                ));
            }
        }
        fences.push(
            chunk
                .page_locations()
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
        KeyShape::FixedBin => Keys::Bin(
            read_column::<FixedLenByteArrayType>(chunk, chunks, skip, take)?
                .into_iter()
                .map(ByteArray::from)
                .collect(),
        ),
        KeyShape::Uuid => Keys::Uuid(
            read_column::<FixedLenByteArrayType>(chunk, chunks, skip, take)?
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
pub(super) fn check_utf8(chunk: ColumnChunk<'_>, keys: &Keys) -> Result<()> {
    if let Keys::Str(run) = keys
        && run
            .iter()
            .any(|value| std::str::from_utf8(value.data()).is_err())
    {
        return Err(file_fault(
            chunk.column.path,
            FileFault::KeyNotUtf8 {
                row_group: chunk.row_group(),
            },
        ));
    }
    Ok(())
}

/// The most records one `read_records` call asks the column reader for:
/// sixteen pages of the served layout, so a call's overhead is spread over
/// many pages. The reader sizes the value and level buffers of a call by
/// the records asked for, capped by the values its page has left, so the
/// buffers a call grows hold at most this many entries, under a mebibyte
/// for any Parquet type, whatever a footer claims for a row group or a
/// header for a page.
const DECODE_BATCH_RECORDS: i64 = 16 * PAGE_ROWS;

/// `take` records of `chunk` after skipping `skip`, decoded from
/// `segments`, which hold at least the pages those rows are in and the
/// chunk's dictionary page when it has one. Whole pages before the rows
/// wanted are skipped from their locations without a read; the dictionary
/// page is read on the way, as the reader requires. Definition levels are
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
/// Three allocations happen inside parquet before this function sees what
/// sized them. The page reader reserves a page's uncompressed size from its
/// header when it decodes the page (a Snappy chunk zero-fills it), and a
/// dictionary page's value count, from its header, sizes the dictionary it
/// decodes into. The callers read those headers ahead of the page reader
/// and refuse one that declares more than its page can hold: `verify`
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
/// dictionary page among them, and others. The decode runs under
/// `catch_unwind`, so a panic is `FileFault::DecoderPanicked` at the last
/// page the column reader took, and ends this load or lookup rather than
/// the task running it.
fn read_column<T: ParquetDataType>(
    chunk: ColumnChunk<'_>,
    segments: Arc<Segments>,
    skip: usize,
    take: usize,
) -> Result<Vec<T::T>> {
    let tally = Arc::new(Mutex::new(PageTally::default()));
    // Unwind-safe: the page reader, the column reader, and the buffers are
    // built inside `decode` and dropped with a panic; the footer `chunk`
    // borrows is only read, and the tally `decode` shares holds each
    // page's record whole, written under its lock.
    let decode = AssertUnwindSafe(|| decode_records::<T>(chunk, segments, skip, take, &tally));
    let (values, decoded) = unless_panicked(chunk.column.path, decode, || DecodedPart::Pages {
        row_group: chunk.row_group(),
        page: tally.lock().unwrap_or_else(PoisonError::into_inner).taken,
    })??;
    let tally = tally.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(page) = tally.refused {
        return Err(chunk.page_rows_mismatch(page));
    }
    chunk.check_pages_read(skip..skip + take, &tally.seen, decoded)?;
    Ok(values)
}

/// The decode `read_column` runs under `catch_unwind`: the values of
/// `take` records after skipping `skip`, and how many records the column
/// reader produced or the error it ended in, with what the page reader
/// saw recorded in `tally`.
fn decode_records<T: ParquetDataType>(
    chunk: ColumnChunk<'_>,
    segments: Arc<Segments>,
    skip: usize,
    take: usize,
    tally: &Arc<Mutex<PageTally>>,
) -> Result<(Vec<T::T>, parquet::errors::Result<usize>)> {
    let descr = chunk.column.descriptor();
    let nullable = descr.max_def_level() > 0;
    let pages = CountedPages {
        inner: chunk.page_reader(segments)?,
        page_rows: (0..chunk.page_locations().len())
            .map(|page| {
                let rows = chunk.page_rows(page);
                row_index(rows.end - rows.start)
            })
            .collect(),
        next_page: 0,
        tally: Arc::clone(tally),
    };
    let mut reader: ColumnReaderImpl<T> = ColumnReaderImpl::new(descr, Box::new(pages));
    let batch_records = row_index(DECODE_BATCH_RECORDS);
    let mut values = Vec::new();
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
            values.append(&mut batch);
            if read == 0 {
                break;
            }
            records += read;
        }
        Ok(records)
    };
    let decoded = decode();
    Ok((values, decoded))
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
    use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};

    use super::*;
    use crate::approx::count;
    use crate::config::{ConfigError, ConfigViolation};
    use crate::generation::testing::Dictionary;
    use crate::generation::testing::{
        self, KeyColumn, TABLE, cache, column_of, page_rows, refusal_of,
    };
    use crate::index::page_header::{
        MOST_BYTES_PER_VALUE, MOST_KEY_PAGE_BYTES, NIBBLE_MASK, PAGE_ENCODING_ALLOWANCE_BYTES,
        PageBody, PageHeader,
    };
    use crate::index::testing::{
        FEW_ROWS, Fixture, INFLATED_GROUP_ROWS, Opened, PAGES_PER_GROUP, ROW_GROUPS, contract_rows,
        fault_of, group_rows, header_at, key_for, location, longs, moved_by_header, pages_of,
        predicted_of, row_of, stepped, store, store_file, with_column_chunk,
        with_dictionary_one_entry_short, with_group_rows, with_header_claims, with_pages,
    };
    use crate::index::{
        ChunkPage, Column, DecodedPart, FileFault, HeaderField, KeyShape, PageHeaderFault, RowRef,
        WireType,
    };
    use crate::key::KeyType;
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
                    ConfigViolation::DiskResidencyWithoutLocalDir
                ))
            ),
            "{error:?}"
        );
        assert_eq!(opened.stats().gets(), 1, "no chunk fetched");
    }
}
