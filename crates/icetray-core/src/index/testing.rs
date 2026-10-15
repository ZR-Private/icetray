//! The synthetic data files the index tests are built over and the footer
//! edits that make a file contradict itself; shared by every stage's tests.

use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryArray, FixedSizeBinaryArray, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::bloom_filter::Sbbf;
use parquet::file::metadata::{
    ColumnChunkMetaDataBuilder, PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader,
};
use parquet::file::page_index::offset_index::{OffsetIndexMetaData, PageLocation};
use parquet::file::properties::{
    DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH, EnabledStatistics, WriterProperties, WriterVersion,
};
use parquet::schema::types::ColumnPath;

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
    UNCOMPRESSED_PAGE_SIZE_FIELD,
};
use crate::index::{ChunkPage, FileFault, FileIndex, HeaderField, ResidentBytes, RowRef, WireType};
use crate::key::testing::UUID_HALF_BYTES;
use crate::key::{Key, KeyType, UUID_BYTES};
use crate::local::LocalDir;
use crate::manifest::TableLayout;
use crate::manifest::testing::{self as manifest_testing, KEY_COLUMN, VALUE_COLUMN};
use crate::order::{OrderColumnLayout, OrderType};
use crate::parquet_io::FileHandle;
use crate::storage::testing::{Gauged, MEM_PREFIX, MemStore};
use crate::storage::{HedgeWindow, IoStats, Storage};
use crate::varint::{CONTINUES, PAYLOAD, PAYLOAD_BITS};

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

/// The key just after `key` in served order, which no fixture holds:
/// keys step by `KEY_STEP`, and strings and bytes are extended.
pub(super) fn absent_after(key: &Key) -> Key {
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
            let low = u64::from_be_bytes(next[UUID_HALF_BYTES..].try_into().unwrap());
            next[UUID_HALF_BYTES..].copy_from_slice(&(low + 1).to_be_bytes());
            Key::Uuid(next)
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

/// The name prefix of a fixture's extra columns (`Fixture::extra_columns`).
pub(super) const EXTRA_COLUMN: &str = "extra";

/// The order column of an ordered fixture (`Fixture::orders`), written
/// after the value column and the extra columns.
pub(super) const ORDER_COLUMN: &str = "ts";
/// The order column's field id in `ordered_layout`, after the key's and
/// the value's.
pub(super) const ORDER_FIELD_ID: i32 = 3;

/// What an ordered fixture's order column holds, a `None` a null, which
/// makes the column nullable.
#[derive(Clone)]
pub(crate) enum Orders {
    /// An `INT64` column, which a `long` order column serves.
    Long(Vec<Option<i64>>),
    /// A `BYTE_ARRAY` column without annotation, which a `string` or a
    /// `binary` order column serves; under `string` its bytes must be
    /// UTF-8.
    Bytes(Vec<Option<Vec<u8>>>),
    /// A `FIXED_LEN_BYTE_ARRAY` column of this width, which a `fixed`
    /// order column of the width serves.
    Fixed(i32, Vec<Option<Vec<u8>>>),
}

impl Orders {
    fn data_type(&self) -> DataType {
        match self {
            Self::Long(_) => DataType::Int64,
            Self::Bytes(_) => DataType::Binary,
            Self::Fixed(width, _) => DataType::FixedSizeBinary(*width),
        }
    }

    fn nullable(&self) -> bool {
        match self {
            Self::Long(values) => values.iter().any(Option::is_none),
            Self::Bytes(values) | Self::Fixed(_, values) => values.iter().any(Option::is_none),
        }
    }

    fn array(&self) -> ArrayRef {
        match self {
            Self::Long(values) => Arc::new(values.iter().copied().collect::<Int64Array>()),
            Self::Bytes(values) => Arc::new(values.iter().cloned().collect::<BinaryArray>()),
            Self::Fixed(width, values) => Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    values.iter().cloned(),
                    *width,
                )
                .unwrap(),
            ),
        }
    }
}

/// `values` as a long order column.
pub(crate) fn long_orders(values: &[i64]) -> Orders {
    Orders::Long(values.iter().copied().map(Some).collect())
}

/// The contract layout of `key_type` served as an ordered table whose
/// order column is `ORDER_COLUMN` of `order_type`.
pub(crate) fn ordered_layout(key_type: KeyType, order_type: OrderType) -> TableLayout {
    TableLayout {
        order: Some(OrderColumnLayout {
            name: ORDER_COLUMN.to_string(),
            field_id: ORDER_FIELD_ID,
            order_type,
        }),
        ..manifest_testing::layout(key_type, None)
    }
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
pub(crate) struct Layout {
    pub(crate) page_rows: usize,
    pub(crate) row_group_rows: usize,
    pub(super) page_bytes: usize,
    pub(crate) write_batch: usize,
    pub(super) dictionary: Dictionary,
    pub(super) statistics: EnabledStatistics,
    /// The statistics of an ordered fixture's order column where they
    /// differ from `statistics`; below `Page`, the file holds no column
    /// index for it.
    pub(super) order_statistics: Option<EnabledStatistics>,
    pub(super) truncate: Option<usize>,
    pub(super) bloom: Bloom,
    pub(super) offset_index: bool,
    pub(super) writer_version: WriterVersion,
    pub(super) compression: Compression,
    /// The key column's encoding where no dictionary is written; the
    /// writer's own choice for the writer version when `None` (`PLAIN` for
    /// v1, a delta encoding of byte arrays for v2).
    pub(super) key_encoding: Option<Encoding>,
    /// The value column's encoding where no dictionary is written, as
    /// `key_encoding` is the key column's.
    pub(super) value_encoding: Option<Encoding>,
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
            order_statistics: None,
            truncate: DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH,
            bloom: Bloom::Sized,
            offset_index: true,
            writer_version: WriterVersion::PARQUET_1_0,
            compression: Compression::ZSTD(ZstdLevel::default()),
            key_encoding: None,
            value_encoding: None,
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
            .set_compression(self.compression)
            .set_bloom_filter_enabled(false)
            .set_offset_index_disabled(!self.offset_index)
            .set_writer_version(self.writer_version);
        let builder = match self.key_encoding {
            Some(encoding) => builder.set_column_encoding(ColumnPath::from(KEY_COLUMN), encoding),
            None => builder,
        };
        let builder = match self.value_encoding {
            Some(encoding) => builder.set_column_encoding(ColumnPath::from(VALUE_COLUMN), encoding),
            None => builder,
        };
        let builder = match self.order_statistics {
            Some(statistics) => {
                builder.set_column_statistics_enabled(ColumnPath::from(ORDER_COLUMN), statistics)
            }
            None => builder,
        };
        let builder = with_dictionary(builder, self.dictionary);
        with_key_bloom(builder, self.bloom, count(self.row_group_rows)).build()
    }
}

/// One synthetic data file: what its columns hold and how the writer
/// lays it out. A `None` key or value is a null, and makes its column
/// nullable; `nullable_key` makes the key column nullable without one, as
/// an Iceberg column not declared `NOT NULL` is.
pub(crate) struct Fixture {
    key_column: KeyColumn,
    pub(super) nullable_key: bool,
    pub(super) value_column: ValueColumn,
    /// Binary columns written after the value column, named
    /// `EXTRA_COLUMN` and a number, each holding the values: columns no
    /// table layout of the tests serves.
    pub(super) extra_columns: usize,
    keys: Vec<Option<Key>>,
    pub(super) values: Vec<Option<Vec<u8>>>,
    /// The order column of an ordered fixture, as many values as keys;
    /// `None` writes none.
    pub(crate) orders: Option<Orders>,
    pub(crate) layout: Layout,
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
    pub(crate) fn with_keys(key_column: KeyColumn, keys: Vec<Option<Key>>) -> Self {
        let values = (0..keys.len()).map(|i| Some(value_for(i))).collect();
        Self {
            key_column,
            nullable_key: false,
            value_column: ValueColumn::Binary,
            extra_columns: 0,
            keys,
            values,
            orders: None,
            layout: Layout::contract(),
        }
    }

    /// `with_keys` over long keys `keys` with `orders` as the order column.
    pub(crate) fn ordered(keys: &[i64], orders: Orders) -> Self {
        Self {
            orders: Some(orders),
            ..Self::with_keys(KeyColumn::Int64, longs(keys))
        }
    }

    pub(super) fn write(&self) -> Bytes {
        let key_nullable = self.nullable_key || self.keys.iter().any(Option::is_none);
        let value_nullable = self.values.iter().any(Option::is_none);
        let mut fields = vec![
            Field::new(KEY_COLUMN, key_data_type(self.key_column), key_nullable),
            Field::new(
                VALUE_COLUMN,
                value_data_type(self.value_column),
                value_nullable,
            ),
        ];
        let mut arrays = vec![
            key_array(self.key_column, &self.keys),
            value_array(self.value_column, &self.values),
        ];
        for at in 0..self.extra_columns {
            fields.push(Field::new(
                format!("{EXTRA_COLUMN}{at}"),
                DataType::Binary,
                value_nullable,
            ));
            arrays.push(value_array(ValueColumn::Binary, &self.values));
        }
        if let Some(orders) = &self.orders {
            fields.push(Field::new(
                ORDER_COLUMN,
                orders.data_type(),
                orders.nullable(),
            ));
            arrays.push(orders.array());
        }
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(Arc::clone(&schema), arrays).unwrap();
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
pub(crate) struct Opened {
    storage: Arc<Storage>,
    pub(crate) handle: Arc<FileHandle>,
    pub(super) file: Bytes,
}

pub(crate) async fn store(fixture: &Fixture) -> Opened {
    store_file(fixture.write()).await
}

pub(super) async fn store_file(file: Bytes) -> Opened {
    store_file_hedged_by(file, Arc::new(HedgeWindow::new(&HedgeConfig::default()))).await
}

/// `store` with the file's lookup reads hedged against `window`.
pub(super) async fn store_hedged_by(fixture: &Fixture, window: Arc<HedgeWindow>) -> Opened {
    store_file_hedged_by(fixture.write(), window).await
}

/// `store` through a `Gauged` store over the in-memory one, so a test
/// sees how many GETs a read runs together.
pub(super) async fn store_gauged(fixture: &Fixture) -> (Opened, Arc<Gauged>) {
    let mem = MemStore::new();
    let inner: Arc<dyn ObjectStore> = Arc::<InMemory>::clone(&mem.store);
    let gauged = Arc::new(Gauged::new(inner));
    mem.storage
        .register(MEM_PREFIX, Arc::<Gauged>::clone(&gauged));
    let file = fixture.write();
    let window = Arc::new(HedgeWindow::new(&HedgeConfig::default()));
    let handle = mem.open(&location(), file.clone(), window).await;
    let opened = Opened {
        storage: mem.storage,
        handle,
        file,
    };
    (opened, gauged)
}

async fn store_file_hedged_by(file: Bytes, window: Arc<HedgeWindow>) -> Opened {
    let mem = MemStore::new();
    let handle = mem.open(&location(), file.clone(), window).await;
    Opened {
        storage: mem.storage,
        handle,
        file,
    }
}

impl Opened {
    pub(crate) fn stats(&self) -> Arc<IoStats> {
        self.storage.stats()
    }

    pub(super) async fn build(
        &self,
        key_type: KeyType,
        residency: Residency,
        local: Option<&Arc<LocalDir>>,
    ) -> Result<FileIndex> {
        self.build_under(&manifest_testing::layout(key_type, None), residency, local)
            .await
    }

    /// A build under `layout`.
    pub(crate) async fn build_under(
        &self,
        layout: &TableLayout,
        residency: Residency,
        local: Option<&Arc<LocalDir>>,
    ) -> Result<FileIndex> {
        FileIndex::build(TABLE, Arc::clone(&self.handle), layout, residency, local).await
    }

    /// A build of long keys under `ordered_layout` with an order column of
    /// `order_type`, in `residency`, with a local directory under `dir` for
    /// the Disk tier and none for the others.
    pub(crate) async fn build_ordered_in(
        &self,
        order_type: OrderType,
        residency: Residency,
        dir: &Path,
    ) -> Result<FileIndex> {
        let local = testing::local_dir(dir, self.stats(), None);
        let local = (residency == Residency::Disk).then_some(&local);
        self.build_under(&ordered_layout(KeyType::Long, order_type), residency, local)
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
    FileIndex::predicted_resident_bytes(
        index.metadata(),
        index.key_leaf(),
        index.order_column(),
        &index.held_value_leaves(),
        index.key_type(),
        index.handle(),
    )
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
            .position(|byte| byte & CONTINUES == 0)
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
            let low = u8::try_from(rest & u32::from(PAYLOAD)).unwrap();
            rest >>= PAYLOAD_BITS;
            if at + 1 == width {
                low
            } else {
                low | CONTINUES
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

/// `file` with the header of row group 0's first data page in its `leaf`
/// chunk claiming `claimed` values, in place (`with_claim_in_place`).
pub(crate) fn with_first_data_page_claiming(file: &Bytes, leaf: usize, claimed: i32) -> Bytes {
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Required)
        .parse_and_finish(file)
        .unwrap();
    with_claim_in_place(
        file,
        &metadata,
        leaf,
        ChunkPage::Data(0),
        HeaderField::NumValues,
        claimed,
    )
}

/// `chunk`, the bytes of a column chunk that starts with a data page, with
/// that page's header claiming `claimed` values, in a varint as wide as the
/// one it replaces, so no byte after it moves.
pub(crate) fn with_first_page_claiming(chunk: &[u8], claimed: i32) -> Vec<u8> {
    let varint = header_varint(chunk, HeaderField::NumValues);
    let width = varint.len();
    let mut patched = chunk.to_vec();
    patched.splice(varint, zigzag_varint(claimed, width));
    patched
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
