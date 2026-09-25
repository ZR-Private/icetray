//! The synthetic data files the index tests are built over and the footer
//! edits that make a file contradict itself; shared by every stage's tests.

use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryArray, FixedSizeBinaryArray, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::bloom_filter::Sbbf;
use parquet::file::metadata::{
    ColumnChunkMetaDataBuilder, PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader,
};
use parquet::file::page_index::offset_index::{OffsetIndexMetaData, PageLocation};
use parquet::file::properties::{
    DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH, EnabledStatistics, WriterProperties, WriterVersion,
};

use crate::approx::{self, count};
use crate::config::{HedgeConfig, Residency};
use crate::error::{Error, Result};
use crate::generation::testing::{
    self, Bloom, Dictionary, KEY_STEP, KeyColumn, TABLE, column_of, key_array, key_data_type,
    page_rows, with_dictionary, with_key_bloom,
};
use crate::index::page_header::{
    COMPRESSED_PAGE_SIZE_FIELD, DATA_PAGE_HEADER_FIELD, DATA_PAGE_HEADER_V2_FIELD,
    DEFINITION_LEVELS_BYTE_LENGTH_FIELD, DICTIONARY_PAGE_HEADER_FIELD, NIBBLE_BITS, NIBBLE_MASK,
    NUM_VALUES_FIELD, PageBody, PageHeader, REPETITION_LEVELS_BYTE_LENGTH_FIELD, TYPE_FIELD,
    UNCOMPRESSED_PAGE_SIZE_FIELD, VARINT_CONTINUES,
};
use crate::index::{ChunkPage, FileFault, FileIndex, HeaderField, ResidentBytes, RowRef, WireType};
use crate::key::testing::UUID_HALF_BYTES;
use crate::key::{Key, KeyType, UUID_BYTES};
use crate::local::LocalDir;
use crate::manifest::testing::{KEY_COLUMN, VALUE_COLUMN};
use crate::parquet_io::FileHandle;
use crate::storage::testing::{MEM_PREFIX, MemStore};
use crate::storage::{HedgeWindow, IoStats, Storage};

/// The fixture file's key under the prefix.
const FILE_KEY: &str = "db/kv/data/f0.parquet";
/// Key pages per row group in the contract fixture.
pub(super) const PAGES_PER_GROUP: usize = 2;
/// Row groups in the contract fixture, so row-group choice is exercised.
pub(super) const ROW_GROUPS: usize = 3;
/// Bytes per value in the contract fixture.
pub(super) const VALUE_BYTES: usize = 64;
/// The byte every binary key starts with, above zero so a below-range
/// binary key exists.
pub(super) const BINARY_KEY_TAG: u8 = 0xff;
/// The byte the first half of every uuid key holds, above zero so a
/// below-range uuid exists.
const UUID_KEY_FILL: u8 = 0x0a;
/// The width of the fixed-width binary keys: a `u64`.
pub(super) const FIXED_KEY_BYTES: i32 = 8;
/// A row count a footer may claim for a row group of `PAGES_PER_GROUP`
/// pages: two to the thirty-first, which widens the last page's span,
/// and with it the span-derived bounds, past any page a header can
/// declare, and lets a dictionary count more entries than a page ceiling
/// holds without passing the row group's rows.
pub(super) const INFLATED_GROUP_ROWS: i64 = 1 << 31;
/// Rows in the small fixtures whose keys are written by hand.
pub(super) const FEW_ROWS: usize = 8;
/// The write batch under a byte bound: the writer checks the bound
/// between batches, so a small batch makes the bound bind (RFD 002).
pub(super) const SMALL_BATCH: usize = 8;
pub(super) fn group_rows() -> usize {
    page_rows() * PAGES_PER_GROUP
}

pub(super) fn contract_rows() -> usize {
    group_rows() * ROW_GROUPS
}

pub(super) fn location() -> String {
    format!("{MEM_PREFIX}{FILE_KEY}")
}

/// The number of the `i`th written key: `KEY_STEP * i`, so the key after
/// any written key is absent and inside the file's range.
pub(super) fn stepped(i: usize) -> usize {
    usize::try_from(KEY_STEP).unwrap() * i
}

/// The `i`th key of a fixture of `key_type`: `stepped(i)` rendered in
/// the type, ascending in the served order.
pub(super) fn key_for(i: usize, key_type: KeyType) -> Key {
    let n = stepped(i);
    match key_type {
        KeyType::Long => Key::Long(i64::try_from(n).unwrap()),
        KeyType::String => Key::Str(format!("user:{n:012}")),
        KeyType::Binary => {
            let mut bytes = vec![BINARY_KEY_TAG];
            bytes.extend_from_slice(&count(n).to_be_bytes());
            Key::Bytes(bytes)
        }
        KeyType::Uuid => {
            let mut uuid = [UUID_KEY_FILL; UUID_BYTES];
            uuid[UUID_HALF_BYTES..].copy_from_slice(&count(n).to_be_bytes());
            Key::Uuid(uuid)
        }
    }
}

/// `len` bytes that are a function of row `i`.
pub(super) fn value_of_len(i: usize, len: usize) -> Vec<u8> {
    testing::value_of_len(i64::try_from(i).unwrap(), len)
}

pub(super) fn value_for(i: usize) -> Vec<u8> {
    value_of_len(i, VALUE_BYTES)
}

/// How a fixture's value column is typed in arrow; `Int64` is the type
/// the contract refuses.
#[derive(Clone, Copy)]
pub(super) enum ValueColumn {
    Binary,
    FixedBinary(i32),
    Int64,
}

/// The writer's layout knobs; `contract` is RFD 002's shape.
#[derive(Clone, Copy)]
pub(super) struct Layout {
    page_rows: usize,
    pub(super) row_group_rows: usize,
    pub(super) page_bytes: usize,
    pub(super) write_batch: usize,
    pub(super) dictionary: Dictionary,
    pub(super) statistics: EnabledStatistics,
    pub(super) truncate: Option<usize>,
    pub(super) bloom: Bloom,
    pub(super) offset_index: bool,
    pub(super) writer_version: WriterVersion,
}

impl Layout {
    pub(super) fn contract() -> Self {
        Self {
            page_rows: page_rows(),
            row_group_rows: group_rows(),
            page_bytes: usize::MAX,
            write_batch: page_rows(),
            dictionary: Dictionary::Off,
            statistics: EnabledStatistics::Page,
            truncate: DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH,
            bloom: Bloom::Sized,
            offset_index: true,
            writer_version: WriterVersion::PARQUET_1_0,
        }
    }

    /// The key column's filter is sized to one row group's keys. Page
    /// statistics keep the offset index on whatever `offset_index`
    /// says, so a fixture without one also writes no statistics.
    fn properties(&self) -> WriterProperties {
        let builder = WriterProperties::builder()
            .set_max_row_group_row_count(Some(self.row_group_rows))
            .set_write_batch_size(self.write_batch)
            .set_data_page_row_count_limit(self.page_rows)
            .set_data_page_size_limit(self.page_bytes)
            .set_statistics_enabled(self.statistics)
            .set_column_index_truncate_length(self.truncate)
            .set_statistics_truncate_length(self.truncate)
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_bloom_filter_enabled(false)
            .set_offset_index_disabled(!self.offset_index)
            .set_writer_version(self.writer_version);
        let builder = with_dictionary(builder, self.dictionary);
        with_key_bloom(builder, self.bloom, count(self.row_group_rows)).build()
    }
}

/// One synthetic data file: what its columns hold and how the writer
/// lays it out. A `None` key or value is a null, and makes its column
/// nullable.
pub(super) struct Fixture {
    key_column: KeyColumn,
    pub(super) value_column: ValueColumn,
    keys: Vec<Option<Key>>,
    pub(super) values: Vec<Option<Vec<u8>>>,
    pub(super) layout: Layout,
}

impl Fixture {
    /// RFD 002's shape with `rows` keys of `key_type` from `key_for`.
    pub(super) fn contract(key_type: KeyType, rows: usize) -> Self {
        Self::with_keys(
            column_of(key_type),
            (0..rows).map(|i| Some(key_for(i, key_type))).collect(),
        )
    }

    /// RFD 002's shape over hand-written keys with `VALUE_BYTES` values.
    pub(super) fn with_keys(key_column: KeyColumn, keys: Vec<Option<Key>>) -> Self {
        let values = (0..keys.len()).map(|i| Some(value_for(i))).collect();
        Self {
            key_column,
            value_column: ValueColumn::Binary,
            keys,
            values,
            layout: Layout::contract(),
        }
    }

    pub(super) fn write(&self) -> Bytes {
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

fn value_data_type(column: ValueColumn) -> DataType {
    match column {
        ValueColumn::Binary => DataType::Binary,
        ValueColumn::FixedBinary(width) => DataType::FixedSizeBinary(width),
        ValueColumn::Int64 => DataType::Int64,
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
pub(super) struct Opened {
    storage: Arc<Storage>,
    pub(super) handle: Arc<FileHandle>,
    pub(super) file: Bytes,
}

pub(super) async fn store(fixture: &Fixture) -> Opened {
    store_file(fixture.write()).await
}

pub(super) async fn store_file(file: Bytes) -> Opened {
    let mem = MemStore::new();
    let handle = mem
        .open(
            &location(),
            file.clone(),
            Arc::new(HedgeWindow::new(&HedgeConfig::default())),
        )
        .await;
    Opened {
        storage: mem.storage,
        handle,
        file,
    }
}

impl Opened {
    pub(super) fn stats(&self) -> Arc<IoStats> {
        self.storage.stats()
    }

    pub(super) async fn build(
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
    pub(super) async fn build_in(
        &self,
        key_type: KeyType,
        residency: Residency,
        dir: &Path,
    ) -> Result<FileIndex> {
        let local = testing::local_dir(dir, self.stats(), None);
        let local = (residency == Residency::Disk).then_some(&local);
        self.build(key_type, residency, local).await
    }

    pub(super) async fn index(&self, key_type: KeyType) -> FileIndex {
        self.build(key_type, Residency::Full, None).await.unwrap()
    }

    /// The key column's bloom filters read from the file itself: the
    /// oracle for whether a miss reaches the key page in the Bloom tier.
    pub(super) fn filters(&self, index: &FileIndex) -> Vec<Sbbf> {
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

/// The file fault a build or a check ended in, for the fixture file.
pub(super) fn fault_of<T: std::fmt::Debug>(result: Result<T>) -> FileFault {
    let Err(Error::File { path, fault }) = result else {
        panic!("expected a file fault, got {result:?}");
    };
    assert_eq!(path, location());
    fault
}

/// `metadata` with row group `row_group`'s row count replaced.
pub(super) fn with_group_rows(
    metadata: &ParquetMetaData,
    row_group: usize,
    rows: i64,
) -> ParquetMetaData {
    let mut builder = metadata.clone().into_builder();
    let mut groups = builder.take_row_groups();
    let group = groups.remove(row_group);
    groups.insert(
        row_group,
        group.into_builder().set_num_rows(rows).build().unwrap(),
    );
    builder.set_row_groups(groups).build()
}

/// `metadata` with column `leaf`'s chunk metadata in `row_group`
/// rebuilt through `edit`.
pub(super) fn with_column_chunk(
    metadata: &ParquetMetaData,
    row_group: usize,
    leaf: usize,
    edit: impl FnOnce(ColumnChunkMetaDataBuilder) -> ColumnChunkMetaDataBuilder,
) -> ParquetMetaData {
    let mut builder = metadata.clone().into_builder();
    let mut groups = builder.take_row_groups();
    let group = groups.remove(row_group);
    let mut columns = group.columns().to_vec();
    columns[leaf] = edit(columns[leaf].clone().into_builder()).build().unwrap();
    groups.insert(
        row_group,
        group
            .into_builder()
            .set_column_metadata(columns)
            .build()
            .unwrap(),
    );
    builder.set_row_groups(groups).build()
}

/// `metadata` with column `leaf`'s page locations in row group 0
/// replaced.
pub(super) fn with_pages(
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
pub(super) fn pages_of(metadata: &ParquetMetaData, leaf: usize) -> Vec<PageLocation> {
    metadata.offset_index().unwrap()[0][leaf]
        .page_locations()
        .clone()
}

/// The row `key_for(i)` sits at in the contract fixture.
pub(super) fn row_of(i: usize, group_rows: usize) -> RowRef {
    RowRef {
        row_group: i / group_rows,
        row: i64::try_from(i % group_rows).unwrap(),
    }
}

pub(super) fn longs(values: &[i64]) -> Vec<Option<Key>> {
    values.iter().map(|value| Some(Key::Long(*value))).collect()
}

/// `predicted_resident_bytes` of a built index's own footer.
pub(super) fn predicted_of(index: &FileIndex) -> ResidentBytes {
    FileIndex::predicted_resident_bytes(index.metadata(), index.key_leaf(), index.handle())
}

/// The byte range of `field`'s varint in a page header in compact
/// thrift, as parquet writes a data or dictionary page's header: `i32`
/// fields, then the page-specific struct, whose fields up to the
/// levels of a v2 header are `i32`s. `field` is one of the header's
/// `i32` fields or one of the page-specific struct's.
fn header_varint(header: &[u8], field: HeaderField) -> Range<usize> {
    let varint_end = |from: usize| {
        from + header[from..]
            .iter()
            .position(|byte| byte & VARINT_CONTINUES == 0)
            .unwrap()
            + 1
    };
    let (top, nested) = match field {
        HeaderField::PageType => (Some(TYPE_FIELD), None),
        HeaderField::UncompressedPageSize => (Some(UNCOMPRESSED_PAGE_SIZE_FIELD), None),
        HeaderField::CompressedPageSize => (Some(COMPRESSED_PAGE_SIZE_FIELD), None),
        HeaderField::NumValues => (None, Some(NUM_VALUES_FIELD)),
        HeaderField::DefinitionLevelsByteLength => {
            (None, Some(DEFINITION_LEVELS_BYTE_LENGTH_FIELD))
        }
        HeaderField::RepetitionLevelsByteLength => {
            (None, Some(REPETITION_LEVELS_BYTE_LENGTH_FIELD))
        }
        HeaderField::DataPageHeader
        | HeaderField::DataPageHeaderV2
        | HeaderField::DictionaryPageHeader => panic!("{field} is a struct, not a varint"),
        HeaderField::LevelsByteLength => panic!("{field} is a sum, not a varint"),
    };
    let mut at = 0;
    let mut id = 0;
    loop {
        let byte = header[at];
        id += i16::from(byte >> NIBBLE_BITS);
        at += 1;
        let code = byte & NIBBLE_MASK;
        if code == WireType::I32.code() {
            let end = varint_end(at);
            if top == Some(id) {
                return at..end;
            }
            at = end;
        } else if code == WireType::Struct.code()
            && let Some(wanted) = nested
        {
            assert!(
                [
                    DATA_PAGE_HEADER_FIELD,
                    DICTIONARY_PAGE_HEADER_FIELD,
                    DATA_PAGE_HEADER_V2_FIELD
                ]
                .contains(&id),
                "field {id} is not a page-specific header"
            );
            let mut inner = 0;
            loop {
                let byte = header[at];
                inner += i16::from(byte >> NIBBLE_BITS);
                at += 1;
                assert_eq!(
                    byte & NIBBLE_MASK,
                    WireType::I32.code(),
                    "field {inner} of the page-specific header is not an i32"
                );
                let end = varint_end(at);
                if inner == wanted {
                    return at..end;
                }
                at = end;
            }
        } else {
            panic!("field {id} has type {code}, not in a page header as parquet writes it");
        }
    }
}

/// The most bytes a varint of an `i32` takes: seven bits per byte.
const I32_VARINT_BYTES: usize = 5;

/// `value` as compact thrift reads an `i32`: zigzag, then a varint of
/// `width` bytes, padded with continuation bytes. `with_header_claims`
/// pads every value to `I32_VARINT_BYTES`, so every value moves the pages
/// after it by the same bytes and the value the header held already
/// proves the move right; `with_claim_in_place` keeps the width the
/// header had. `value` fits `width`.
fn zigzag_varint(value: i32, width: usize) -> Vec<u8> {
    let mut rest = ((value << 1) ^ (value >> (i32::BITS - 1))).cast_unsigned();
    let varint = (0..width)
        .map(|at| {
            let low = u8::try_from(rest & u32::from(!VARINT_CONTINUES)).unwrap();
            rest >>= u8::BITS - 1;
            if at + 1 == width {
                low
            } else {
                low | VARINT_CONTINUES
            }
        })
        .collect();
    assert_eq!(rest, 0, "{value} does not fit a varint of {width} bytes");
    varint
}

/// Where the header of `page` of row group 0's `leaf` chunk starts, as an
/// offset into the chunk's bytes.
pub(super) fn header_at(metadata: &ParquetMetaData, leaf: usize, page: ChunkPage) -> usize {
    let pages = pages_of(metadata, leaf);
    let (chunk_start, _) = metadata.row_group(0).column(leaf).byte_range();
    let start = match page {
        ChunkPage::Dictionary => chunk_start,
        ChunkPage::Data(at) => u64::try_from(pages[at].offset).unwrap(),
    };
    approx::length(start - chunk_start)
}

/// `bytes` with each of `claims`, a header field and what it claims,
/// rewritten in turn in the page header at `header_at`, each varint padded
/// to `I32_VARINT_BYTES`.
pub(super) fn with_header_claims(
    bytes: &[u8],
    header_at: usize,
    claims: &[(HeaderField, i32)],
) -> Vec<u8> {
    let mut patched = bytes.to_vec();
    for (field, claimed) in claims {
        let varint = header_varint(&patched[header_at..], *field);
        let varint = header_at + varint.start..header_at + varint.end;
        patched.splice(varint, zigzag_varint(*claimed, I32_VARINT_BYTES));
    }
    patched
}

/// `file` with `field` of the header of `page` of row group 0's `leaf`
/// chunk claiming `claimed`, in a varint as wide as the one it replaces:
/// no byte after it moves, so the file's own footer still places every
/// page and the file loads as written.
fn with_claim_in_place(
    file: &[u8],
    metadata: &ParquetMetaData,
    leaf: usize,
    page: ChunkPage,
    field: HeaderField,
    claimed: i32,
) -> Bytes {
    let (chunk_start, _) = metadata.row_group(0).column(leaf).byte_range();
    let at = approx::length(chunk_start) + header_at(metadata, leaf, page);
    let varint = header_varint(&file[at..], field);
    let varint = at + varint.start..at + varint.end;
    let mut patched = file.to_vec();
    let width = varint.len();
    patched.splice(varint, zigzag_varint(claimed, width));
    Bytes::from(patched)
}

/// `file` with the dictionary page header of row group 0's `leaf` chunk
/// counting one entry more than its body holds, in place
/// (`with_claim_in_place`), and the count it held.
pub(crate) fn with_dictionary_one_entry_short(file: &Bytes, leaf: usize) -> (Bytes, u32) {
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Required)
        .parse_and_finish(file)
        .unwrap();
    let (chunk_start, _) = metadata.row_group(0).column(leaf).byte_range();
    let PageBody::Dictionary { num_values } =
        PageHeader::read(&file[approx::length(chunk_start)..])
            .unwrap()
            .body
    else {
        panic!("column {leaf} of row group 0 starts with its dictionary page");
    };
    let claimed = i32::try_from(num_values + 1).unwrap();
    let patched = with_claim_in_place(
        file,
        &metadata,
        leaf,
        ChunkPage::Dictionary,
        HeaderField::NumValues,
        claimed,
    );
    (patched, num_values)
}

/// `loaded` with the header of `page` of row group 0's `leaf` chunk grown
/// by `grown` bytes: the page, the chunk, and every later page of the
/// chunk moved in the footer by it.
pub(super) fn moved_by_header(
    loaded: &ParquetMetaData,
    leaf: usize,
    page: ChunkPage,
    grown: i64,
) -> ParquetMetaData {
    let mut pages = pages_of(loaded, leaf);
    let moved = match page {
        ChunkPage::Dictionary => 0,
        ChunkPage::Data(at) => {
            pages[at].compressed_page_size += i32::try_from(grown).unwrap();
            at + 1
        }
    };
    for page in &mut pages[moved..] {
        page.offset += grown;
    }
    let chunk = loaded.row_group(0).column(leaf);
    let (chunk_len, first_data_page) = (chunk.compressed_size(), chunk.data_page_offset());
    with_column_chunk(&with_pages(loaded, leaf, pages), 0, leaf, |c| {
        let c = c.set_total_compressed_size(chunk_len + grown);
        match page {
            ChunkPage::Dictionary => c.set_data_page_offset(first_data_page + grown),
            ChunkPage::Data(_) => c,
        }
    })
}
