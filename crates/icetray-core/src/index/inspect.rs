//! The footer stage of the index: the checks `FileIndex::inspect` makes on
//! a footer before any key chunk is fetched, the footer as kept, and every
//! tier's resident bytes predicted from it (RFD 017).

use std::mem::size_of;
use std::ops::Range;
use std::sync::Arc;

use parquet::basic::Type as PhysicalType;
use parquet::file::metadata::{ColumnChunkMetaData, ParquetMetaData};
use parquet::file::page_index::column_index::ColumnIndexMetaData;
use parquet::schema::types::{ColumnDescriptor, SchemaDescriptor};

use super::build::reusable_disk_chunks;
use super::{FileFault, KeyShape, ResidentBytes, ValueShape, file_fault, identity_of};
use crate::approx;
use crate::error::{Error, RefusalReason, Result};
use crate::key::{Key, KeyType, UUID_BYTES};
use crate::local::{LocalDir, chunk_file_bytes};
use crate::parquet_io::FileHandle;

/// A data file past the footer stage of `FileIndex::build`: its footer
/// checked and trimmed, its resident bytes predicted, and no key chunk
/// fetched yet. `build` is the chunk stage; between the two a caller holds
/// the budget check of RFD 017.
#[derive(Debug)]
pub struct Inspected {
    pub(super) table: String,
    pub(super) handle: Arc<FileHandle>,
    pub(super) metadata: Arc<ParquetMetaData>,
    pub(super) layout: Layout,
    pub(super) predicted: ResidentBytes,
}

impl Inspected {
    /// Every tier's resident bytes for the file, from its footer alone.
    pub fn predicted(&self) -> &ResidentBytes {
        &self.predicted
    }

    /// The file the footer belongs to.
    pub fn handle(&self) -> &Arc<FileHandle> {
        &self.handle
    }

    /// The bytes a Disk-tier `build` would add to `local`'s `index/`: zero
    /// when a chunk file with this file's identity already holds every key
    /// chunk, since `build` reuses it and writes nothing, otherwise the
    /// chunk file `build` would write, its header and the key chunks
    /// (`predicted().disk_bytes`), which is what `LocalDir::index_bytes_in_use`
    /// counts for the file once written, so the budget is checked in the
    /// units it is measured in. The disk budget is checked against this
    /// rather than against the prediction alone, so a restart over a
    /// directory that already holds the table's chunk files is not charged
    /// for them twice (RFD 017). Reads the chunk file's header and no key
    /// chunk; a chunk file that fails a check counts as absent, as
    /// `LocalDir::chunks_for` treats it.
    pub fn disk_bytes_needed(&self, local: &LocalDir) -> Result<u64> {
        let ranges = key_chunk_ranges(&self.metadata, self.layout.key_leaf);
        Ok(match reusable_disk_chunks(&self.handle, local, &ranges)? {
            Some(_) => 0,
            None => chunk_file_bytes(&identity_of(&self.handle), &ranges),
        })
    }
}

/// The key and value leaves of a file and how each decodes, from the footer.
#[derive(Debug)]
pub(super) struct Layout {
    pub(super) key_leaf: usize,
    pub(super) value_leaf: usize,
    pub(super) key_shape: KeyShape,
    pub(super) value_shape: ValueShape,
}

impl Layout {
    /// The checks `FileIndex::inspect` makes on the footer, in order: both
    /// columns exist, the key's physical type matches the key type, the
    /// value's is a byte array, the file has rows and its row counts agree,
    /// both columns have an offset index that describes their pages.
    pub(super) fn inspect(
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
        let value_shape = value_shape(path, &schema.column(value_leaf))?;
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
        Error::refused(
            table,
            RefusalReason::KeyTypeMismatch {
                path: path.to_string(),
                expected: key_type,
                got: physical,
            },
        )
    })
}

/// How the value column decodes, or `FileFault::ValuePhysicalType` when it
/// is not a byte array of either width.
fn value_shape(path: &str, descr: &ColumnDescriptor) -> Result<ValueShape> {
    let physical = descr.physical_type();
    match physical {
        PhysicalType::BYTE_ARRAY => Ok(ValueShape::Variable),
        PhysicalType::FIXED_LEN_BYTE_ARRAY => Ok(ValueShape::Fixed),
        PhysicalType::BOOLEAN
        | PhysicalType::INT32
        | PhysicalType::INT64
        | PhysicalType::INT96
        | PhysicalType::FLOAT
        | PhysicalType::DOUBLE => Err(file_fault(
            path,
            FileFault::ValuePhysicalType { got: physical },
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
/// present, the column chunk has a non-negative offset and size, every
/// page has a non-negative offset and size and lies inside the chunk, the
/// first page is at row zero, first rows are strictly increasing and inside
/// the row group, and a row group with rows has a page. A footer without
/// the index is `FileFault::MissingPageIndex`; one whose index fails a
/// check is `FileFault::InconsistentOffsetIndex`.
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
        let chunk = chunk_range(group.column(leaf)).ok_or_else(inconsistent)?;
        let mut previous: Option<i64> = None;
        for location in locations {
            let in_chunk = u64::try_from(location.offset).is_ok_and(|start| {
                u64::try_from(location.compressed_page_size)
                    .is_ok_and(|len| start >= chunk.start && start.saturating_add(len) <= chunk.end)
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
pub(super) fn kept_footer(metadata: &ParquetMetaData, key_leaf: usize) -> ParquetMetaData {
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

/// The byte range of a column chunk from its footer fields: from the
/// dictionary page when the chunk has one, else the first data page, for
/// the chunk's compressed size. `None` when a field is negative or the end
/// does not fit `u64`. Parquet's `byte_range` asserts on a negative field,
/// so every chunk the index reads goes through this check at build, before
/// anything asks parquet for its range.
fn chunk_range(chunk: &ColumnChunkMetaData) -> Option<Range<u64>> {
    let negative = chunk.data_page_offset() < 0
        || chunk.compressed_size() < 0
        || chunk
            .dictionary_page_offset()
            .is_some_and(|offset| offset < 0);
    if negative {
        return None;
    }
    let (start, len) = chunk.byte_range();
    Some(start..start.checked_add(len)?)
}

/// The byte range of the key column chunk of every row group, in row group
/// order; build checked every chunk's offset and size.
pub(super) fn key_chunk_ranges(metadata: &ParquetMetaData, key_leaf: usize) -> Vec<Range<u64>> {
    metadata
        .row_groups()
        .iter()
        .map(|group| {
            chunk_range(group.column(key_leaf))
                .expect("column chunk offsets and sizes were checked at build")
        })
        .collect()
}

/// The byte range of the key column's bloom filter in every row group. The
/// Bloom tier stands the filter in for the key chunk, so a row group without
/// one is `RefusalReason::NoBloomFilter`, and one whose filter the footer
/// places nowhere a file can hold, with a negative offset, no length, or a
/// negative or overflowing length, is `FileFault::UnplaceableBloomFilter`.
pub(super) fn bloom_ranges(
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
            let Some(offset) = column.bloom_filter_offset() else {
                return Err(Error::refused(
                    table,
                    RefusalReason::NoBloomFilter {
                        path: path.to_string(),
                        row_group,
                    },
                ));
            };
            let unplaceable = || file_fault(path, FileFault::UnplaceableBloomFilter { row_group });
            let offset = u64::try_from(offset).map_err(|_| unplaceable())?;
            let len = column
                .bloom_filter_length()
                .and_then(|len| u64::try_from(len).ok())
                .ok_or_else(unplaceable)?;
            let end = offset.checked_add(len).ok_or_else(unplaceable)?;
            Ok(offset..end)
        })
        .collect()
}

/// The parts every tier's resident bytes are made of, from the footer, so
/// `predicted_resident_bytes` and `resident_bytes` cannot drift; the doc of
/// the former says which are exact and which estimated.
pub(super) struct ResidentSet {
    footer: u64,
    key_chunks: u64,
    fences: u64,
    filters: Option<u64>,
    rows: u64,
}

impl ResidentSet {
    /// From `metadata`, with the fences exact when `fences` is given and
    /// estimated from the footer otherwise.
    pub(super) fn from_footer(
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
            footer: approx::count(kept_footer(metadata, key_leaf).memory_size()),
            key_chunks: key_columns().map(|column| column.byte_range().1).sum(),
            fences: fences.map_or_else(
                || estimated_fence_bytes(metadata, key_leaf),
                exact_fence_bytes,
            ),
            filters,
            rows: approx::measured(metadata.file_metadata().num_rows()),
        }
    }
}

/// The tiers' totals from their parts.
pub(super) fn tiers(set: &ResidentSet) -> ResidentBytes {
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
            approx::count(size_of::<Key>() + heap)
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
                approx::count(pages) * approx::measured(descr.type_length())
            }
            PhysicalType::BOOLEAN
            | PhysicalType::INT32
            | PhysicalType::INT64
            | PhysicalType::INT96
            | PhysicalType::FLOAT
            | PhysicalType::DOUBLE => 0,
        };
        total += approx::count(pages * size_of::<Key>()) + heap;
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
            .map(|min| approx::count(min.map_or(0, <[u8]>::len)))
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
            let encoded = approx::measured(chunk.uncompressed_size());
            let values = approx::measured(chunk.num_values());
            // A chunk of no values has no mean, and no heap per fence.
            let mean = encoded.checked_div(values).unwrap_or(0);
            approx::count(pages) * mean
        }
    }
}

#[cfg(test)]
mod tests {
    use parquet::file::metadata::{FileMetaData, ParquetMetaDataBuilder};
    use parquet::file::properties::{DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH, EnabledStatistics};

    use super::*;
    use crate::approx::count;
    use crate::config::Residency;
    use crate::contract::{PAGE_ROWS, VALUE_PAGE_BYTES};
    use crate::generation::testing::{self, Bloom, KeyColumn, TABLE, refusal_of};
    use crate::index::testing::{
        FEW_ROWS, FIXED_KEY_BYTES, Fixture, KEY_TYPES, Opened, PAGES_PER_GROUP, ROW_GROUPS,
        SMALL_BATCH, ValueColumn, contract_rows, fault_of, key_for, location, longs, page_rows,
        pages_of, predicted_of, stepped, store, value_of_len, with_column_chunk, with_group_rows,
        with_pages,
    };
    use crate::index::{FileFault, FileIndex};
    use crate::manifest::testing::{KEY_COLUMN, VALUE_COLUMN};

    /// A column name no fixture has.
    const ABSENT_COLUMN: &str = "absent";
    /// A bloom filter offset no footer can mean: before the file's first byte.
    const NEGATIVE_OFFSET: i64 = -1;
    /// Bytes per value where value pages must be bounded by bytes.
    const LARGE_VALUE_BYTES: usize = 4096;
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
                    .map(|i| Some(Key::Bytes(count(stepped(i)).to_be_bytes().to_vec())))
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
    async fn test_build_an_int64_value_column_is_a_file_fault() {
        let mut fixture = Fixture::contract(KeyType::Long, FEW_ROWS);
        fixture.value_column = ValueColumn::Int64;
        let opened = store(&fixture).await;
        assert_eq!(
            fault_of(opened.build(KeyType::Long, Residency::Full, None).await),
            FileFault::ValuePhysicalType {
                got: PhysicalType::INT64,
            }
        );
        assert_eq!(opened.stats().gets(), 1, "faulted at the footer");
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

    /// A column chunk whose footer fields are negative is refused where the
    /// offset index is checked against it, before anything asks parquet for
    /// the chunk's byte range, which asserts on a negative field.
    #[tokio::test]
    async fn test_check_offset_index_refuses_a_column_chunk_with_a_negative_offset_or_size() {
        let opened = store(&Fixture::contract(KeyType::Long, contract_rows())).await;
        let loaded = opened.handle.load_metadata().await.unwrap();
        let path = location();
        for leaf in [0, 1] {
            let negative = [
                with_column_chunk(&loaded, 0, leaf, |chunk| chunk.set_data_page_offset(-1)),
                with_column_chunk(&loaded, 0, leaf, |chunk| {
                    chunk.set_dictionary_page_offset(Some(-1))
                }),
                with_column_chunk(&loaded, 0, leaf, |chunk| {
                    chunk.set_total_compressed_size(-1)
                }),
            ];
            for patched in &negative {
                assert_eq!(
                    fault_of(check_offset_index(&path, patched, leaf)),
                    FileFault::InconsistentOffsetIndex { row_group: 0 }
                );
            }
        }
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
    async fn test_build_drops_the_value_column_index_and_resident_bytes_are_as_predicted() {
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
        // The budget check at the footer stage holds the footer as loaded;
        // its prediction is the kept footer's, and what the index reports.
        let from_loaded = FileIndex::predicted_resident_bytes(&loaded, index.key_leaf());
        assert_eq!(from_loaded, predicted_of(&index));
        assert_eq!(from_loaded.full, count(index.resident_bytes()));
    }

    #[tokio::test]
    async fn test_bloom_tier_refuses_a_file_without_a_filter_that_full_serves() {
        let mut fixture = Fixture::contract(KeyType::Long, FEW_ROWS);
        fixture.layout.bloom = Bloom::Off;
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

    #[tokio::test]
    async fn test_bloom_ranges_with_a_negative_filter_offset_is_a_file_fault() {
        let opened = store(&Fixture::contract(KeyType::Long, FEW_ROWS)).await;
        let index = opened.index(KeyType::Long).await;
        let key_leaf = index.key_leaf();
        let patched = with_column_chunk(index.metadata(), 0, key_leaf, |chunk| {
            chunk.set_bloom_filter_offset(Some(NEGATIVE_OFFSET))
        });
        let result = bloom_ranges(TABLE, &opened.handle, &patched, key_leaf);
        let Err(Error::File { path, fault }) = result else {
            panic!("expected a file fault, got {result:?}");
        };
        assert_eq!(path, location());
        assert_eq!(fault, FileFault::UnplaceableBloomFilter { row_group: 0 });
    }

    #[tokio::test]
    async fn test_bloom_ranges_without_a_filter_length_is_a_file_fault() {
        let opened = store(&Fixture::contract(KeyType::Long, FEW_ROWS)).await;
        let index = opened.index(KeyType::Long).await;
        let key_leaf = index.key_leaf();
        let patched = with_column_chunk(index.metadata(), 0, key_leaf, |chunk| {
            chunk.set_bloom_filter_length(None)
        });
        let result = bloom_ranges(TABLE, &opened.handle, &patched, key_leaf);
        let Err(Error::File { path, fault }) = result else {
            panic!("expected a file fault, got {result:?}");
        };
        assert_eq!(path, location());
        assert_eq!(fault, FileFault::UnplaceableBloomFilter { row_group: 0 });
        assert_eq!(
            FileIndex::predicted_resident_bytes(&patched, key_leaf).bloom,
            None
        );
        assert!(bloom_ranges(TABLE, &opened.handle, index.metadata(), key_leaf).is_ok());
    }

    /// The footer stage over the fixture: `FileIndex::inspect` for `opened`.
    async fn inspect(opened: &Opened, key_type: KeyType) -> Inspected {
        FileIndex::inspect(
            TABLE,
            Arc::clone(&opened.handle),
            KEY_COLUMN,
            VALUE_COLUMN,
            key_type,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn test_inspect_costs_the_footer_alone_and_predicts_what_build_keeps() {
        let opened = store(&Fixture::contract(KeyType::Long, contract_rows())).await;
        let inspected = inspect(&opened, KeyType::Long).await;
        assert_eq!(opened.stats().gets(), 1, "the footer alone");
        assert!(Arc::ptr_eq(inspected.handle(), &opened.handle));
        let predicted = *inspected.predicted();
        let index = inspected.build(Residency::Full, None).await.unwrap();
        assert_eq!(
            opened.stats().gets(),
            1 + count(ROW_GROUPS),
            "then the chunks"
        );
        assert_eq!(predicted, predicted_of(&index));
        assert_eq!(count(index.resident_bytes()), predicted.full);
    }

    #[tokio::test]
    async fn test_disk_bytes_needed_is_zero_with_a_matching_chunk_file_and_the_prediction_without()
    {
        let dir = tempfile::tempdir().unwrap();
        let opened = store(&Fixture::contract(KeyType::Long, contract_rows())).await;
        let local = testing::local_dir(dir.path(), opened.stats(), None);
        let fresh = inspect(&opened, KeyType::Long).await;
        let disk_bytes = fresh.predicted().disk_bytes;
        assert!(disk_bytes > 0);
        let needed = fresh.disk_bytes_needed(&local).unwrap();
        assert!(
            needed > disk_bytes,
            "an empty directory takes every chunk and the header before them"
        );
        fresh.build(Residency::Disk, Some(&local)).await.unwrap();
        assert_eq!(
            local.index_bytes_in_use().unwrap(),
            needed,
            "the budget check predicts the bytes the directory then holds"
        );
        assert_eq!(
            inspect(&opened, KeyType::Long)
                .await
                .disk_bytes_needed(&local)
                .unwrap(),
            0,
            "the chunk file written is reused"
        );

        // Another file at the same location: the chunk file on disk has
        // another identity and counts as absent.
        let other = store(&Fixture::contract(
            KeyType::Long,
            contract_rows() + page_rows(),
        ))
        .await;
        let stale = inspect(&other, KeyType::Long).await;
        assert!(stale.disk_bytes_needed(&local).unwrap() > stale.predicted().disk_bytes);
        assert_eq!(other.stats().gets(), 1, "no chunk is read for the check");
    }

    #[tokio::test]
    async fn test_predicted_resident_bytes_agrees_with_resident_bytes_per_tier_and_type() {
        let rows = contract_rows();
        let dir = tempfile::tempdir().unwrap();
        for key_type in KEY_TYPES {
            let opened = store(&Fixture::contract(key_type, rows)).await;
            for residency in Residency::ALL {
                let index = opened
                    .build_in(key_type, residency, dir.path())
                    .await
                    .unwrap();
                let predicted = predicted_of(&index);
                let expected = predicted.in_tier(residency).unwrap();
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
            let local = testing::local_dir(dir.path(), opened.stats(), None);
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
}
