//! Fixture tables written with the Iceberg library's own writer (RFD 014):
//! a `bucket(N, key)`-partitioned, key-sorted table in the serving layout of
//! RFD 002, or in any layout a test names through `Layout`. Values are a
//! function of the key, so any answer is checkable without a stored oracle.
//! What the library's writer cannot produce (a delete file, a dropped data
//! file, a null key, a key column whose Parquet type contradicts the schema)
//! is committed as a raw file or as a snapshot written by hand.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, FixedSizeBinaryArray, Int32Array, Int64Array, RecordBatch, StringArray,
};
use arrow::buffer::Buffer;
use arrow::datatypes::Schema as ArrowSchema;
use bytes::Bytes;
use iceberg::io::{LocalFsStorageFactory, StorageFactory};
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalog, MemoryCatalogBuilder};
use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, Datum, Literal, MAIN_BRANCH,
    ManifestListWriter, ManifestWriterBuilder, NestedField, NullOrder, Operation, PartitionKey,
    PartitionSpec, PrimitiveType, Schema, Snapshot, SortDirection, SortField, SortOrder, Struct,
    Summary, TableMetadata, Transform, Type, UnboundPartitionSpec,
};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction as _, Transaction};
use iceberg::transform::create_transform_function;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::partitioning::PartitioningWriter as _;
use iceberg::writer::partitioning::clustered_writer::ClusteredWriter;
use iceberg::{Catalog, CatalogBuilder as _, ErrorKind, NamespaceIdent, TableCreation, TableIdent};
use iceberg_storage_opendal::OpenDalStorageFactory;
use icetray_core::catalog::{TableSource, hint_location, version_file};
use icetray_core::config::{Residency, SourceConfig, TableConfig};
use icetray_core::key::UUID_BYTES;
use icetray_core::{Key, KeyType, Value};
use parquet::arrow::ArrowWriter;
use tempfile::TempDir;

use crate::error::{Result, SpecFault};
use crate::keys::{VALUE_BYTES, absent_key_from, id_of, key_at_padded, value_for_len};
use crate::layout::{KEY_COLUMN, Layout, VALUE_COLUMN};
use crate::s3::S3Fixture;
use crate::verify::count;

/// Ids of the small fixture: twenty thousand rows, about 1,250 per bucket
/// of `SMALL_BUCKETS`, so every bucket's file holds two contract pages.
pub const SMALL_IDS: Range<u64> = 0..20_000;

/// Buckets of the small fixture: one file per bucket at the contract's
/// file size target.
pub const SMALL_BUCKETS: u32 = 16;

/// Ids sampled on the small fixture: spread over buckets and pages so that
/// under every key type no two land on one value page, and on distinct
/// thousands so the same holds for an unpartitioned fixture of 1,000-row
/// files; a cold batch over it costs one GET per id, which the lookup suite
/// proves through the plan.
pub const SAMPLE: [u64; 12] = [
    0, 1_500, 2_134, 3_783, 5_000, 6_402, 7_372, 8_191, 9_894, 10_000, 12_416, 17_000,
];

/// Ids of the interleaved fixture, written at `INTERLEAVE_STRIDE` so the
/// other residue classes (`residue_class`) can be appended between them.
pub const INTERLEAVED_IDS: Range<u64> = 0..8_000;

/// The stride of the interleaved fixture's first build: four residue
/// classes, one written and three left for appends.
pub const INTERLEAVE_STRIDE: u64 = 4;

/// Buckets of the interleaved fixture: one file per bucket per build, so an
/// appended class widens every bucket's overlap group by one file.
pub const INTERLEAVED_BUCKETS: u32 = 4;

/// The field id of the key column in every fixture schema.
const KEY_FIELD_ID: i32 = 1;

/// The field id of the value column in every fixture schema.
const VALUE_FIELD_ID: i32 = 2;

/// The schema id every fixture table is created with.
const SCHEMA_ID: i32 = 0;

/// The partition spec id every fixture table is created with.
const SPEC_ID: i32 = 0;

/// The sort order id of the key sort every fixture table declares.
const SORT_ORDER_ID: i64 = 1;

/// The partition field's name under a bucket spec.
const KEY_BUCKET_FIELD: &str = "key_bucket";

/// The bucket every file of an unpartitioned table is grouped under.
const UNPARTITIONED_BUCKET: i32 = 0;

/// The file prefix of a fixture's first commit.
const FIRST_BUILD_PREFIX: &str = "build1";

/// The directory under a table root holding data files: the library's
/// default, where raw files are uploaded beside the writer's and where a
/// by-hand snapshot's delete file is named.
pub const DATA_DIR: &str = "data";

/// The directory under a table root holding manifests and manifest lists,
/// beside the metadata files, as every Iceberg writer lays it out.
const MANIFEST_DIR: &str = "metadata";

/// The extension of a manifest or manifest list.
const MANIFEST_SUFFIX: &str = ".avro";

/// The id of a by-hand snapshot on a table with none.
const FIRST_SNAPSHOT_ID: i64 = 1;

/// The name the memory catalog is registered under; nothing reads it back.
const MEMORY_CATALOG_NAME: &str = "memory";

/// What to write.
#[derive(Clone, Debug, PartialEq)]
pub struct FixtureSpec {
    /// The table name within the namespace.
    pub name: String,
    /// The key column's type.
    pub key_type: KeyType,
    /// Buckets of the `bucket(N, key)` spec; zero writes an unpartitioned
    /// table.
    pub buckets: u32,
    /// The ids written, in fixture key space (`key_at_padded`).
    pub ids: Range<u64>,
    /// Write every `id_stride`-th id of `ids`; one (or zero) writes every
    /// id. A later `append` of the skipped ids interleaves keys into the
    /// existing files' ranges.
    pub id_stride: u64,
    /// The order ids are handed to the writer within each bucket.
    pub order: Order,
    /// Extra prefix bytes on every string or binary key, for long keys; a
    /// long or uuid key takes none.
    pub key_pad: usize,
    /// Bytes per value.
    pub value_bytes: usize,
    /// Which ids get a null value.
    pub nulls: Nulls,
    /// The Parquet layout every data file is written in.
    pub layout: Layout,
}

/// The order ids are handed to the writer within each bucket. `Descending`
/// produces files whose keys descend, which the reader refuses as unsorted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Order {
    /// Keys ascend within every file: the layout's clause.
    Ascending,
    /// Keys descend within every file.
    Descending,
}

/// Which ids get a null value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Nulls {
    /// Every value is present.
    None,
    /// Every id that is a multiple of this gets a null; zero names no id.
    EveryNth(u64),
}

impl FixtureSpec {
    /// The small fixture: `SMALL_IDS` in `SMALL_BUCKETS`, every id, ascending,
    /// no padding, `VALUE_BYTES` per value, no nulls, the contract layout.
    /// Every other shape is a struct update on it.
    pub fn small(name: &str, key_type: KeyType) -> Self {
        Self {
            name: name.to_string(),
            key_type,
            buckets: SMALL_BUCKETS,
            ids: SMALL_IDS,
            id_stride: 1,
            order: Order::Ascending,
            key_pad: 0,
            value_bytes: VALUE_BYTES,
            nulls: Nulls::None,
            layout: Layout::contract(),
        }
    }

    /// The interleaved fixture: every `INTERLEAVE_STRIDE`-th id of
    /// `INTERLEAVED_IDS` in `INTERLEAVED_BUCKETS` buckets, long keys,
    /// otherwise the small fixture's shape. An `append` of another
    /// `residue_class` writes one file per bucket whose range interleaves
    /// with the first build's, so every bucket holds an overlap group.
    pub fn interleaved(name: &str) -> Self {
        Self {
            ids: INTERLEAVED_IDS,
            id_stride: INTERLEAVE_STRIDE,
            buckets: INTERLEAVED_BUCKETS,
            ..Self::small(name, KeyType::Long)
        }
    }

    /// Whether `id`'s value is null.
    fn is_null(&self, id: u64) -> bool {
        match self.nulls {
            Nulls::None => false,
            Nulls::EveryNth(n) => id.checked_rem(n) == Some(0),
        }
    }

    /// The ids the first commit writes, in `order`.
    fn written_ids(&self) -> Vec<u64> {
        let stride = self.id_stride.max(1);
        let start = self.ids.start;
        let mut ids: Vec<u64> = self
            .ids
            .clone()
            .filter(|id| (id - start).is_multiple_of(stride))
            .collect();
        if self.order == Order::Descending {
            ids.reverse();
        }
        ids
    }
}

/// A Parquet file to commit as it is, for what the library's writer cannot
/// produce: a key column whose Parquet type contradicts the schema, a null
/// key, or a table of a type icetray refuses. Raw fixtures use unpartitioned
/// tables, so the partition tuple is `Struct::empty()`.
#[derive(Clone, Debug)]
pub struct RawFile {
    /// The file's name under the table's data directory, extension included.
    pub name: String,
    /// The file's bytes.
    pub bytes: Bytes,
    /// The rows the manifest records for it.
    pub records: u64,
    /// The partition tuple the manifest records for it.
    pub partition: Struct,
    /// The key bounds the manifest records, when any.
    pub bounds: Option<(Key, Key)>,
}

impl RawFile {
    /// `batch` written as one Parquet file under `layout`'s properties.
    pub fn from_batch(
        name: &str,
        batch: &RecordBatch,
        layout: &Layout,
        partition: Struct,
        bounds: Option<(Key, Key)>,
    ) -> Result<Self> {
        let mut out = Vec::new();
        let mut writer = ArrowWriter::try_new(
            &mut out,
            batch.schema(),
            Some(layout.properties(batch.num_rows())),
        )?;
        writer.write(batch)?;
        writer.close()?;
        Ok(Self {
            name: name.to_string(),
            bytes: Bytes::from(out),
            records: count(batch.num_rows()),
            partition,
            bounds,
        })
    }
}

/// A snapshot to commit by hand: the data files it lists, added, and the
/// delete files it lists, added as position deletes.
#[derive(Clone, Debug, Default)]
pub struct HandSnapshot {
    /// Every data file of the snapshot.
    pub data: Vec<DataFile>,
    /// The delete files of the snapshot; any bytes, since content decides.
    pub deletes: Vec<DataFile>,
}

/// A written fixture: the table as this fixture last committed it, and the
/// one source of truth for what it holds.
#[derive(Debug)]
pub struct Fixture {
    /// The table after the last catalog commit: its `FileIO`, location,
    /// schema, and spec.
    table: Table,
    ident: TableIdent,
    spec: FixtureSpec,
    /// Every id written so far; `keys`, `ids`, and `id_range` derive from it.
    ids: BTreeSet<u64>,
    /// The current snapshot as this fixture knows it, catalog or by hand.
    metadata: TableMetadata,
    metadata_location: String,
    /// Every data file the current snapshot lists.
    files: Vec<DataFile>,
    /// Whether a snapshot was committed by hand, after which the catalog's
    /// record is behind and only `commit_by_hand` may advance the table.
    by_hand: bool,
    /// By-hand commits so far; the next writes metadata version one above.
    hand_commits: u64,
}

impl Fixture {
    /// A fixture over a table with no id written yet.
    fn new(table: Table, ident: TableIdent, spec: FixtureSpec) -> Result<Self> {
        let metadata = table.metadata().clone();
        let metadata_location = table.metadata_location_result()?.to_string();
        Ok(Self {
            table,
            ident,
            spec,
            ids: BTreeSet::new(),
            metadata,
            metadata_location,
            files: Vec::new(),
            by_hand: false,
            hand_commits: 0,
        })
    }

    /// The specification the fixture was written from.
    pub fn spec(&self) -> &FixtureSpec {
        &self.spec
    }

    /// The table's identifier in its catalog.
    pub fn ident(&self) -> &TableIdent {
        &self.ident
    }

    /// The table root.
    pub fn root(&self) -> &str {
        self.metadata.location()
    }

    /// The current metadata file's location.
    pub fn metadata_location(&self) -> &str {
        &self.metadata_location
    }

    /// The current metadata.
    pub fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }

    /// The `i`-th key of this fixture, in its key type and padding.
    pub fn key(&self, i: u64) -> Key {
        key_at_padded(i, self.spec.key_type, self.spec.key_pad)
    }

    /// A key just above the `i`-th that no fixture holds.
    pub fn absent_near(&self, i: u64) -> Key {
        absent_key_from(&self.key(i))
    }

    /// The value stored for `key`: `None` for an id `Nulls` names, else the
    /// `value_bytes` bytes derived from the key.
    pub fn value(&self, key: &Key) -> Option<Vec<u8>> {
        let null = id_of(key, self.spec.key_pad).is_some_and(|id| self.spec.is_null(id));
        (!null).then(|| value_for_len(key, self.spec.value_bytes))
    }

    /// The lookup outcome for a key this fixture holds: `Present` with its
    /// value, or `Null`.
    pub fn expected(&self, key: &Key) -> Value {
        match self.value(key) {
            Some(value) => Value::Present(Bytes::from(value)),
            None => Value::Null,
        }
    }

    /// Every key written, ascending by id.
    pub fn keys(&self) -> impl Iterator<Item = Key> + '_ {
        self.ids.iter().map(|&id| self.key(id))
    }

    /// The keys at `ids`, in that order: a sample as the plan and the
    /// lookups take it.
    pub fn keys_at(&self, ids: &[u64]) -> Vec<Key> {
        ids.iter().map(|&id| self.key(id)).collect()
    }

    /// Every id written, ascending.
    pub fn ids(&self) -> impl Iterator<Item = u64> + '_ {
        self.ids.iter().copied()
    }

    /// The smallest id written to one above the largest; empty when none.
    pub fn id_range(&self) -> Range<u64> {
        match (self.ids.first(), self.ids.last()) {
            (Some(&first), Some(&last)) => first..last + 1,
            (None, _) | (_, None) => 0..0,
        }
    }

    /// Data files the current snapshot lists.
    pub fn file_count(&self) -> usize {
        self.files.len()
    }

    /// Every data file the current snapshot lists, as its manifest records
    /// it; what a `HandSnapshot` that drops or keeps files is built from.
    pub fn files(&self) -> &[DataFile] {
        &self.files
    }

    /// The table through `catalog`, by identifier.
    pub fn catalog_source(&self, catalog: Arc<dyn Catalog>) -> TableSource {
        TableSource::Catalog {
            catalog,
            ident: self.ident.clone(),
        }
    }

    /// The table root, followed through its version hint (`write_hint`).
    pub fn root_source(&self) -> SourceConfig {
        SourceConfig::Static {
            location: self.root().to_string(),
        }
    }

    /// The current metadata file itself, which never advances.
    pub fn pinned_source(&self) -> SourceConfig {
        SourceConfig::Static {
            location: self.metadata_location.clone(),
        }
    }

    /// A configuration serving this fixture's main branch in `residency`
    /// from the pinned source; a caller that follows the root or a catalog
    /// overrides `source`. No resident-bytes budget.
    pub fn config(&self, residency: Residency, strict: bool) -> TableConfig {
        self.config_with_budget(residency, strict, None)
    }

    /// `config` with `budget` as the resident-bytes budget.
    pub fn config_with_budget(
        &self,
        residency: Residency,
        strict: bool,
        budget: Option<u64>,
    ) -> TableConfig {
        TableConfig {
            name: self.spec.name.clone(),
            source: self.pinned_source(),
            r#ref: MAIN_BRANCH.to_string(),
            key_column: KEY_COLUMN.to_string(),
            value_column: VALUE_COLUMN.to_string(),
            strict,
            residency,
            resident_budget_bytes: budget,
        }
    }

    /// Writes `ids` in the given order as new data files and commits them
    /// through `catalog` with a fast append; the table advances. Refused
    /// once a snapshot was committed by hand.
    pub async fn append(
        &mut self,
        catalog: &dyn Catalog,
        ids: impl IntoIterator<Item = u64>,
        prefix: &str,
    ) -> Result<()> {
        if self.by_hand {
            return Err(SpecFault::CatalogBehind.into());
        }
        let ids: Vec<u64> = ids.into_iter().collect();
        let files = write_data_files(&self.table, &self.spec, ids.iter().copied(), prefix).await?;
        self.commit_files(catalog, files).await?;
        self.ids.extend(ids);
        Ok(())
    }

    /// Uploads `file` under the table's data directory through the table's
    /// `FileIO` and commits it through `catalog` with a fast append, which
    /// admits any data file. Refused once a snapshot was committed by hand.
    pub async fn append_raw(&mut self, catalog: &dyn Catalog, file: RawFile) -> Result<()> {
        if self.by_hand {
            return Err(SpecFault::CatalogBehind.into());
        }
        let path = format!("{}/{DATA_DIR}/{}", self.root(), file.name);
        self.table
            .file_io()
            .new_output(&path)?
            .write(file.bytes.clone())
            .await?;
        let data_file = data_file(
            &path,
            self.metadata.default_partition_spec_id(),
            file.partition,
            file.records,
            count(file.bytes.len()),
            file.bounds.as_ref(),
            DataContentType::Data,
        )?;
        self.commit_files(catalog, vec![data_file]).await
    }

    /// Commits `files` as one fast append and takes the table as the
    /// catalog returns it.
    async fn commit_files(&mut self, catalog: &dyn Catalog, files: Vec<DataFile>) -> Result<()> {
        let tx = Transaction::new(&self.table);
        let tx = tx
            .fast_append()
            .add_data_files(files.iter().cloned())
            .apply(tx)?;
        self.table = tx.commit(catalog).await?;
        self.metadata = self.table.metadata().clone();
        self.metadata_location = self.table.metadata_location_result()?.to_string();
        self.files.extend(files);
        Ok(())
    }

    /// Commits `snapshot` by hand, past the catalog: one data manifest
    /// listing `snapshot.data` as added entries, one delete manifest listing
    /// `snapshot.deletes` as added position deletes when there are any, the
    /// manifest list, a snapshot one id and one sequence number above the
    /// current with the current as parent, and the metadata with the main
    /// branch moved to it, written as `version_file(root, n)` for the `n`-th
    /// by-hand commit and named by the version hint. This is how a snapshot
    /// carrying a delete file or dropping a data file is produced, neither
    /// of which a fast append can commit. From here on the table is followed
    /// through its root: its catalog record is behind, and `append` and
    /// `append_raw` refuse with `SpecFault::CatalogBehind`.
    pub async fn commit_by_hand(&mut self, snapshot: HandSnapshot) -> Result<()> {
        let root = self.root().to_string();
        let file_io = self.table.file_io();
        let (parent, snapshot_id) = match self.metadata.current_snapshot() {
            Some(current) => (Some(current.snapshot_id()), current.snapshot_id() + 1),
            None => (None, FIRST_SNAPSHOT_ID),
        };
        let sequence = self.metadata.last_sequence_number() + 1;
        let schema = self.metadata.current_schema().clone();
        let spec = PartitionSpec::clone(self.metadata.default_partition_spec());
        let mut manifests = Vec::new();
        let listings = [
            (Some(snapshot.data.as_slice()), false),
            (
                (!snapshot.deletes.is_empty()).then_some(snapshot.deletes.as_slice()),
                true,
            ),
        ];
        for (index, (files, deletes)) in listings.into_iter().enumerate() {
            let Some(files) = files else {
                continue;
            };
            let path = format!("{root}/{MANIFEST_DIR}/{snapshot_id}-m{index}{MANIFEST_SUFFIX}");
            let builder = ManifestWriterBuilder::new(
                file_io.new_output(&path)?,
                Some(snapshot_id),
                Arc::clone(&schema),
                spec.clone(),
            );
            let mut writer = if deletes {
                builder.build_v2_deletes()
            } else {
                builder.build_v2_data()
            };
            for file in files {
                writer.add_file(file.clone(), sequence)?;
            }
            manifests.push(writer.write_manifest_file().await?);
        }
        let list_path = format!("{root}/{MANIFEST_DIR}/snap-{snapshot_id}{MANIFEST_SUFFIX}");
        let mut list = ManifestListWriter::v2(
            file_io.new_output(&list_path)?.writer().await?,
            snapshot_id,
            parent,
            sequence,
        );
        list.add_manifests(manifests.into_iter())?;
        list.close().await?;
        let committed = Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(parent)
            .with_sequence_number(sequence)
            .with_timestamp_ms(self.metadata.last_updated_ms() + 1)
            .with_manifest_list(list_path)
            .with_summary(Summary {
                operation: Operation::Overwrite,
                additional_properties: HashMap::new(),
            })
            .with_schema_id(self.metadata.current_schema_id())
            .build();
        let metadata = self
            .metadata
            .clone()
            .into_builder(Some(self.metadata_location.clone()))
            .set_branch_snapshot(committed, MAIN_BRANCH)?
            .build()?
            .metadata;
        self.hand_commits += 1;
        let location = version_file(&root, self.hand_commits);
        file_io
            .new_output(&location)?
            .write(Bytes::from(serde_json::to_vec(&metadata)?))
            .await?;
        self.metadata = metadata;
        self.metadata_location = location;
        self.files = snapshot.data;
        self.by_hand = true;
        self.write_hint().await
    }

    /// Writes the current metadata location as the whole content of the
    /// root's version hint through the table's `FileIO`, so a
    /// `SourceConfig::Static` on the root follows this fixture. Called by
    /// `commit_by_hand`, and by a test that follows the root after a catalog
    /// commit; the testkit owns hint writing, core owns the hint's location.
    pub async fn write_hint(&self) -> Result<()> {
        self.table
            .file_io()
            .new_output(hint_location(self.root()))?
            .write(Bytes::from(self.metadata_location.clone()))
            .await?;
        Ok(())
    }
}

/// The ids of `class` modulo `INTERLEAVE_STRIDE` inside `INTERLEAVED_IDS`:
/// what an `append` on the interleaved fixture adds, one class at a time.
pub fn residue_class(class: u64) -> Vec<u64> {
    INTERLEAVED_IDS
        .filter(|id| id % INTERLEAVE_STRIDE == class)
        .collect()
}

/// `memory_catalog_local` over a fresh temporary directory, which lives as
/// long as the returned `TempDir`: the opening of every in-process test.
pub async fn local_catalog() -> Result<(TempDir, Arc<dyn Catalog>)> {
    let dir = tempfile::tempdir()?;
    let catalog = memory_catalog_local(dir.path()).await?;
    Ok((dir, catalog))
}

/// A memory catalog whose files live under `dir` on local disk, through the
/// library's own filesystem storage. It resolves a table with no
/// object-storage read through icetray's `Storage`, which makes it the
/// in-process stand-in for a catalog record.
pub async fn memory_catalog_local(dir: &Path) -> Result<Arc<MemoryCatalog>> {
    std::fs::create_dir_all(dir)?;
    let warehouse = format!("file://{}", dir.display());
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            MEMORY_CATALOG_NAME,
            HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse)]),
        )
        .await?;
    Ok(Arc::new(catalog))
}

/// A memory catalog whose files live under `prefix` in the fixture server's
/// bucket, through the library's S3 storage.
pub async fn memory_catalog_s3(s3: &S3Fixture, prefix: &str) -> Result<Arc<MemoryCatalog>> {
    let mut props = s3.file_io_props();
    props.insert(MEMORY_CATALOG_WAREHOUSE.to_string(), s3.uri(prefix));
    let factory: Arc<dyn StorageFactory> = Arc::new(OpenDalStorageFactory::S3 {
        customized_credential_load: None,
    });
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(factory)
        .load(MEMORY_CATALOG_NAME, props)
        .await?;
    Ok(Arc::new(catalog))
}

/// Creates the table `spec` describes in `namespace` (creating the namespace
/// when absent) and commits `spec.ids` at `spec.id_stride` in `spec.order`
/// as its first snapshot. A `key_pad` on a long or uuid key is
/// `SpecFault::PadOnFixedWidthKey`.
pub async fn write_fixture(
    catalog: &dyn Catalog,
    namespace: &str,
    spec: FixtureSpec,
) -> Result<Fixture> {
    if spec.key_pad > 0 && matches!(spec.key_type, KeyType::Long | KeyType::Uuid) {
        return Err(SpecFault::PadOnFixedWidthKey {
            key_type: spec.key_type,
        }
        .into());
    }
    let (table, ident) = create_table(
        catalog,
        namespace,
        &spec.name,
        primitive_of(spec.key_type),
        PrimitiveType::Binary,
        spec.buckets,
    )
    .await?;
    let ids = spec.written_ids();
    let mut fixture = Fixture::new(table, ident, spec)?;
    fixture.append(catalog, ids, FIRST_BUILD_PREFIX).await?;
    Ok(fixture)
}

/// Creates a table whose key and value columns have the given Iceberg types
/// and commits one raw data file, for the tables whose types icetray refuses
/// from the schema alone. The fixture's spec names the key type
/// `KeyType::from_iceberg` gives the key, or `Binary` for a type icetray
/// does not admit; no id is derived for such a table.
pub async fn write_table_of_types(
    catalog: &dyn Catalog,
    namespace: &str,
    name: &str,
    key: PrimitiveType,
    value: PrimitiveType,
    buckets: u32,
    file: RawFile,
) -> Result<Fixture> {
    let key_type = KeyType::from_iceberg(&Type::Primitive(key.clone())).unwrap_or(KeyType::Binary);
    let (table, ident) = create_table(catalog, namespace, name, key, value, buckets).await?;
    let spec = FixtureSpec {
        ids: 0..0,
        buckets,
        ..FixtureSpec::small(name, key_type)
    };
    let mut fixture = Fixture::new(table, ident, spec)?;
    fixture.append_raw(catalog, file).await?;
    Ok(fixture)
}

/// The Iceberg primitive a key type is written as.
fn primitive_of(key_type: KeyType) -> PrimitiveType {
    match key_type {
        KeyType::Long => PrimitiveType::Long,
        KeyType::String => PrimitiveType::String,
        KeyType::Binary => PrimitiveType::Binary,
        KeyType::Uuid => PrimitiveType::Uuid,
    }
}

/// Creates `name` in `namespace`: a required `key` of `key` type (field
/// `KEY_FIELD_ID`), an optional `value` of `value` type (field
/// `VALUE_FIELD_ID`, so a null value is a legal row per RFD 002),
/// `bucket(buckets, key)` when `buckets` is above zero, and a sort order on
/// the key. Creates the namespace when absent.
async fn create_table(
    catalog: &dyn Catalog,
    namespace: &str,
    name: &str,
    key: PrimitiveType,
    value: PrimitiveType,
    buckets: u32,
) -> Result<(Table, TableIdent)> {
    let namespace = NamespaceIdent::new(namespace.to_string());
    if !catalog.namespace_exists(&namespace).await? {
        catalog.create_namespace(&namespace, HashMap::new()).await?;
    }
    let schema = Schema::builder()
        .with_schema_id(SCHEMA_ID)
        .with_fields([
            NestedField::required(KEY_FIELD_ID, KEY_COLUMN, Type::Primitive(key)).into(),
            NestedField::optional(VALUE_FIELD_ID, VALUE_COLUMN, Type::Primitive(value)).into(),
        ])
        .build()?;
    let mut partition_spec = UnboundPartitionSpec::builder().with_spec_id(SPEC_ID);
    if buckets > 0 {
        partition_spec = partition_spec.add_partition_field(
            KEY_FIELD_ID,
            KEY_BUCKET_FIELD,
            Transform::Bucket(buckets),
        )?;
    }
    let sort_order = SortOrder::builder()
        .with_order_id(SORT_ORDER_ID)
        .with_sort_field(
            SortField::builder()
                .source_id(KEY_FIELD_ID)
                .transform(Transform::Identity)
                .direction(SortDirection::Ascending)
                .null_order(NullOrder::First)
                .build(),
        )
        .build_unbound()?;
    let creation = TableCreation::builder()
        .name(name.to_string())
        .schema(schema)
        .partition_spec(partition_spec.build())
        .sort_order(sort_order)
        .build();
    let table = catalog.create_table(&namespace, creation).await?;
    Ok((table, TableIdent::new(namespace, name.to_string())))
}

/// The rows of one bucket, in the order they reach the writer.
#[derive(Default)]
struct BucketRows {
    ids: Vec<u64>,
    keys: Vec<Key>,
}

/// Writes `ids` into `table`'s layout as new data files, not yet committed:
/// keys through `key_at_padded`, grouped by bucket through the library's own
/// bucket transform, in the given order within a bucket, through a
/// `ClusteredWriter` over the rolling data file writer under
/// `spec.layout`'s properties and file size target, in batches of
/// `spec.layout.page_rows` rows.
pub(crate) async fn write_data_files(
    table: &Table,
    spec: &FixtureSpec,
    ids: impl IntoIterator<Item = u64>,
    prefix: &str,
) -> Result<Vec<DataFile>> {
    let schema = table.metadata().current_schema().clone();
    let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(&schema)?);
    let partition_spec = PartitionSpec::clone(table.metadata().default_partition_spec());
    let by_bucket = group_by_bucket(spec, ids)?;
    let rows = by_bucket.values().map(|bucket| bucket.ids.len()).sum();
    let parquet = ParquetWriterBuilder::new(spec.layout.properties(rows), Arc::clone(&schema));
    let rolling = RollingFileWriterBuilder::new(
        parquet,
        spec.layout.target_file_bytes,
        table.file_io().clone(),
        DefaultLocationGenerator::new(table.metadata())?,
        DefaultFileNameGenerator::new(prefix.to_string(), None, DataFileFormat::Parquet),
    );
    let mut writer = ClusteredWriter::new(DataFileWriterBuilder::new(rolling));
    let batch_rows = spec.layout.page_rows.max(1);
    for (bucket, rows) in &by_bucket {
        let partition = if spec.buckets == 0 {
            Struct::empty()
        } else {
            Struct::from_iter([Some(Literal::int(*bucket))])
        };
        let key = PartitionKey::new(partition_spec.clone(), Arc::clone(&schema), partition);
        for (ids, keys) in rows
            .ids
            .chunks(batch_rows)
            .zip(rows.keys.chunks(batch_rows))
        {
            writer
                .write(key.clone(), batch(&arrow_schema, spec, ids, keys)?)
                .await?;
        }
    }
    Ok(writer.close().await?)
}

/// `ids` as keys, grouped by the bucket the library's transform gives each,
/// ascending by bucket and in the given order within one; every id under
/// `UNPARTITIONED_BUCKET` for a table without a bucket spec.
fn group_by_bucket(
    spec: &FixtureSpec,
    ids: impl IntoIterator<Item = u64>,
) -> Result<BTreeMap<i32, BucketRows>> {
    let ids: Vec<u64> = ids.into_iter().collect();
    let keys: Vec<Key> = ids
        .iter()
        .map(|&id| key_at_padded(id, spec.key_type, spec.key_pad))
        .collect();
    let buckets: Vec<i32> = if spec.buckets == 0 {
        vec![UNPARTITIONED_BUCKET; ids.len()]
    } else {
        let transform = create_transform_function(&Transform::Bucket(spec.buckets))?;
        let array = transform.transform(key_array(spec.key_type, &keys))?;
        array
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("the bucket transform yields an Int32 array for every admitted key type")
            .values()
            .to_vec()
    };
    let mut by_bucket: BTreeMap<i32, BucketRows> = BTreeMap::new();
    for ((id, key), bucket) in ids.into_iter().zip(keys).zip(buckets) {
        let rows = by_bucket.entry(bucket).or_default();
        rows.ids.push(id);
        rows.keys.push(key);
    }
    Ok(by_bucket)
}

/// One batch of `keys` with their values, a null for every id `spec.nulls`
/// names, cast to the arrow types the library's schema conversion gives the
/// two columns.
fn batch(
    arrow_schema: &Arc<ArrowSchema>,
    spec: &FixtureSpec,
    ids: &[u64],
    keys: &[Key],
) -> Result<RecordBatch> {
    let key = key_array(spec.key_type, keys);
    let values: BinaryArray = ids
        .iter()
        .zip(keys)
        .map(|(&id, key)| (!spec.is_null(id)).then(|| value_for_len(key, spec.value_bytes)))
        .collect();
    let value: ArrayRef = Arc::new(values);
    let columns = vec![
        arrow::compute::cast(&key, arrow_schema.field_with_name(KEY_COLUMN)?.data_type())?,
        arrow::compute::cast(
            &value,
            arrow_schema.field_with_name(VALUE_COLUMN)?.data_type(),
        )?,
    ];
    Ok(RecordBatch::try_new(Arc::clone(arrow_schema), columns)?)
}

/// The message of the impossible case: `key_at_padded` builds every key
/// `key_array` sees, in the type it is given.
const MIXED_KEYS: &str = "key_at_padded builds keys of the type the fixture names";

/// `keys`, all of `key_type`, as the key column's array: `Int64`, `Utf8`,
/// `Binary`, or `FixedSizeBinary(16)`, what the library's schema conversion
/// gives a uuid column. An empty slice is an empty array of that type.
pub(crate) fn key_array(key_type: KeyType, keys: &[Key]) -> ArrayRef {
    match key_type {
        KeyType::Long => Arc::new(Int64Array::from(
            keys.iter()
                .map(|key| match key {
                    Key::Long(value) => *value,
                    Key::Str(_) | Key::Bytes(_) | Key::Uuid(_) => unreachable!("{MIXED_KEYS}"),
                })
                .collect::<Vec<i64>>(),
        )),
        KeyType::String => Arc::new(StringArray::from(
            keys.iter()
                .map(|key| match key {
                    Key::Str(text) => text.as_str(),
                    Key::Long(_) | Key::Bytes(_) | Key::Uuid(_) => unreachable!("{MIXED_KEYS}"),
                })
                .collect::<Vec<&str>>(),
        )),
        KeyType::Binary => Arc::new(BinaryArray::from(
            keys.iter()
                .map(|key| match key {
                    Key::Bytes(bytes) => bytes.as_slice(),
                    Key::Long(_) | Key::Str(_) | Key::Uuid(_) => unreachable!("{MIXED_KEYS}"),
                })
                .collect::<Vec<&[u8]>>(),
        )),
        KeyType::Uuid => {
            let mut bytes = Vec::with_capacity(keys.len() * UUID_BYTES);
            for key in keys {
                match key {
                    Key::Uuid(uuid) => bytes.extend_from_slice(uuid),
                    Key::Long(_) | Key::Str(_) | Key::Bytes(_) => unreachable!("{MIXED_KEYS}"),
                }
            }
            Arc::new(FixedSizeBinaryArray::new(
                i32::try_from(UUID_BYTES).expect("sixteen fits i32"),
                Buffer::from(bytes),
                None,
            ))
        }
    }
}

/// A key as the datum a manifest bound carries.
fn datum(key: &Key) -> Datum {
    match key {
        Key::Long(value) => Datum::long(*value),
        Key::Str(text) => Datum::string(text),
        Key::Bytes(bytes) => Datum::binary(bytes.clone()),
        Key::Uuid(bytes) => Datum::uuid(uuid::Uuid::from_bytes(*bytes)),
    }
}

/// A data file of `content` at `path` under `spec_id`, with `records` rows,
/// `size` bytes, and key bounds when given.
fn data_file(
    path: &str,
    spec_id: i32,
    partition: Struct,
    records: u64,
    size: u64,
    bounds: Option<&(Key, Key)>,
    content: DataContentType,
) -> Result<DataFile> {
    let mut builder = DataFileBuilder::default();
    builder
        .content(content)
        .file_path(path.to_string())
        .file_format(DataFileFormat::Parquet)
        .partition(partition)
        .partition_spec_id(spec_id)
        .record_count(records)
        .file_size_in_bytes(size);
    if let Some((min, max)) = bounds {
        builder
            .lower_bounds(HashMap::from([(KEY_FIELD_ID, datum(min))]))
            .upper_bounds(HashMap::from([(KEY_FIELD_ID, datum(max))]));
    }
    builder.build().map_err(|error| {
        iceberg::Error::new(
            ErrorKind::DataInvalid,
            format!("data file entry incomplete: {error}"),
        )
        .into()
    })
}

/// A position-delete file entry at `path` under `spec_id` with `partition`
/// as its tuple, for `HandSnapshot::deletes`: the file need not exist, since
/// the content type alone decides the refusal.
pub fn position_delete_file(
    path: &str,
    spec_id: i32,
    partition: Struct,
    records: u64,
    size: u64,
) -> Result<DataFile> {
    data_file(
        path,
        spec_id,
        partition,
        records,
        size,
        None,
        DataContentType::PositionDeletes,
    )
}
