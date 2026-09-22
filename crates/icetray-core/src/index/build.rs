//! The chunk stage of the index: the key column chunks fetched once, read
//! back from a chunk file, or dropped for their filters, and every key of
//! the file decoded and checked against the contract (RFD 007, RFD 017).

use std::cmp::Ordering;
use std::ops::Range;
use std::sync::{Arc, Mutex, PoisonError};

use bytes::Bytes;
use parquet::bloom_filter::Sbbf;
use parquet::column::page::{Page, PageMetadata, PageReader};
use parquet::column::reader::ColumnReaderImpl;
use parquet::data_type::{
    ByteArray, ByteArrayType, DataType as ParquetDataType, FixedLenByteArrayType, Int64Type,
};
use parquet::file::serialized_reader::SerializedPageReader;

use super::inspect::{bloom_ranges, key_chunk_ranges};
use super::{
    Column, ColumnChunk, FileFault, FileIndex, GroupRange, Inspected, KeyResidency, KeyShape, Keys,
    file_fault, identity_of, off_worker, row_index,
};
use crate::config::Residency;
use crate::contract::MAX_KEY_BYTES;
use crate::error::{Error, RefusalReason, Result};
use crate::key::{Key, UUID_BYTES};
use crate::local::{DiskChunks, LocalDir};
use crate::parquet_io::{FileHandle, Segments};

impl Inspected {
    /// The chunk stage of `FileIndex::build`: streams the key chunks once
    /// for verification and keeps what `residency` keeps, as `build`
    /// documents. The Disk tier without a local directory is refused here,
    /// before any chunk is fetched.
    pub async fn build(self, residency: Residency, local: Option<&LocalDir>) -> Result<FileIndex> {
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
        let ranges = key_chunk_ranges(&metadata, layout.key_leaf);
        let (keys, verified) = match residency {
            Residency::Full => {
                let (chunks, verified) =
                    fetch_verified(&table, &handle, &column, layout.key_shape, &ranges).await?;
                (KeyResidency::Chunks(chunks), verified)
            }
            Residency::Disk => {
                let local =
                    local.ok_or_else(|| Error::refused(&table, RefusalReason::NoLocalDir))?;
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
/// store may serve neighbouring ranges from one fetch and hand back slices
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
/// bytes; a check indexes the first block), is `FileFault::CorruptBloomFilter`.
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
            let filter = Sbbf::from_bytes(bytes).map_err(|error| {
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
        let chunk = column.group(row_group);
        let rows = chunk.rows();
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
/// index the pages contradict. `take` is the footer's number and sizes no
/// buffer: the values grow with what the pages hold, so a footer claiming
/// more rows than any file holds costs no reservation before the check
/// refuses it.
fn read_column<T: ParquetDataType>(
    chunk: ColumnChunk<'_>,
    segments: Arc<Segments>,
    skip: usize,
    take: usize,
) -> Result<Vec<T::T>> {
    let descr = chunk.column.descriptor();
    let nullable = descr.max_def_level() > 0;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let pages = CountedPages {
        inner: chunk.page_reader(segments)?,
        seen: Arc::clone(&seen),
    };
    let mut reader: ColumnReaderImpl<T> = ColumnReaderImpl::new(descr, Box::new(pages));
    let skipped = if skip > 0 {
        reader.skip_records(skip)?
    } else {
        0
    };
    let mut values = Vec::new();
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
    chunk.check_pages_read(skip..skip + take, &seen, records)?;
    Ok(values)
}

/// Exactly row `row` of the value column chunk `chunk` from `segments`:
/// `Some` with the value, `None` for a null.
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
    use crate::contract::PAGE_ROWS;
    use crate::generation::testing::{self, KeyColumn, TABLE, cache, refusal_of};
    use crate::index::testing::{
        FEW_ROWS, Fixture, Opened, PAGES_PER_GROUP, ROW_GROUPS, contract_rows, fault_of,
        group_rows, key_for, location, longs, page_rows, pages_of, predicted_of, row_of, stepped,
        store, store_file, with_column_chunk, with_group_rows, with_pages,
    };
    use crate::index::{Column, FileFault, KeyShape, RowRef};
    use crate::key::KeyType;

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
    async fn test_disk_tier_without_a_local_dir_is_refused_at_the_footer() {
        let opened = store(&Fixture::contract(KeyType::Long, FEW_ROWS)).await;
        assert_eq!(
            refusal_of(opened.build(KeyType::Long, Residency::Disk, None).await),
            RefusalReason::NoLocalDir
        );
        assert_eq!(opened.stats().gets(), 1, "no chunk fetched");
    }
}
