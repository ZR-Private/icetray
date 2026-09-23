//! From a snapshot to its live data files (RFD 005, RFD 009). The table's
//! layout (key and value columns, bucket transform) is derived from the
//! snapshot's own schema, and every manifest is read under its own
//! partition spec, so a schema or spec change between snapshots cannot
//! misroute a key; the table's current schema plays no part. Delete
//! manifests are read like data manifests, so a delete file is refused
//! rather than skipped.

use std::cmp::Ordering;
use std::collections::HashMap;

use futures::stream::{self, StreamExt as _, TryStreamExt as _};
use iceberg::spec::{
    DataContentType, DataFile, Datum, Literal, Manifest, ManifestFile, ManifestList,
    ManifestStatus, PartitionSpec, PrimitiveLiteral, PrimitiveType, Snapshot, TableMetadata,
    Transform, Type,
};

use crate::bucket;
use crate::catalog::TableFault;
use crate::error::{Error, RefusalReason, Result};
use crate::key::{Key, KeyType};
use crate::storage::Storage;

/// Manifests read at once while a snapshot's files are listed. A manifest
/// is one whole-object read of tens of kilobytes to a few megabytes, so
/// sixteen in flight bring a snapshot of hundreds of manifests down to a
/// few round trips of latency while holding a few dozen megabytes of Avro
/// at most.
const MANIFEST_READ_CONCURRENCY: usize = 16;

/// The bucket a file of a table without a bucket transform on the key is
/// filed under: every such file is in the one bucket.
pub(crate) const UNBUCKETED: u32 = 0;

/// The bucket transform on a table's key column, `bucket[count]` over the
/// field `source_field_id` (RFD 008). Two files of one table may carry
/// different bucketings when the partition spec evolved between them; a
/// key's bucket differs under each.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Bucketing {
    /// The number of buckets; above zero.
    pub count: u32,
    /// The field id of the key column the transform is over.
    pub source_field_id: i32,
}

impl Bucketing {
    /// The bucket `key` falls in under this transform.
    pub fn bucket_of(&self, key: &Key) -> u32 {
        bucket::bucket(key, self.count)
    }
}

/// What routing needs to know about a table, derived once per generation
/// from the snapshot's own schema and the table's default partition spec.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableLayout {
    /// The bucket transform on the key in the default partition spec, the
    /// one new files are written under; `None` when it has none. Each data
    /// file routes by its own manifest's spec (`DataFileEntry::bucketing`);
    /// this is what `TableInfo` and the doctor report.
    pub bucketing: Option<Bucketing>,
    /// The key column's name.
    pub key_column: String,
    /// The key column's field id in the snapshot's schema.
    pub key_field_id: i32,
    /// The key column's type.
    pub key_type: KeyType,
    /// The value column's name.
    pub value_column: String,
    /// The value column's field id in the snapshot's schema.
    pub value_field_id: i32,
}

impl TableLayout {
    /// The layout for `snapshot` of `table`. The schema is the snapshot's
    /// own, `snapshot.schema_id()` looked up in `md`; the table's current
    /// schema is consulted only for a snapshot that records no schema id
    /// (written before Iceberg recorded them), which Iceberg readers read
    /// under the current schema too. Refuses a key column
    /// whose type `KeyType::from_iceberg` does not admit as
    /// `RefusalReason::KeyTypeUnsupported` and a value column that is not
    /// binary, fixed, or string as `RefusalReason::ValueTypeUnsupported`
    /// (RFD 002). A column absent from the schema is
    /// `TableFault::ColumnMissing`, a schema id the metadata lacks
    /// `TableFault::SchemaMissing`. Bucketing comes from `md`'s default
    /// partition spec, since a snapshot records no spec of its own; a
    /// bucket count of zero there is `TableFault::ZeroBuckets`.
    pub fn derive(
        table: &str,
        md: &TableMetadata,
        snapshot: &Snapshot,
        key_column: &str,
        value_column: &str,
    ) -> Result<Self> {
        let schema = match snapshot.schema_id() {
            Some(schema_id) => md.schema_by_id(schema_id).ok_or_else(|| {
                Error::table_fault(table, TableFault::SchemaMissing { schema_id })
            })?,
            None => md.current_schema(),
        };
        let column = |name: &str| {
            schema.field_by_name(name).ok_or_else(|| {
                Error::table_fault(
                    table,
                    TableFault::ColumnMissing {
                        column: name.to_string(),
                    },
                )
            })
        };
        let key_field = column(key_column)?;
        let value_field = column(value_column)?;
        let key_type = KeyType::from_iceberg(&key_field.field_type)
            .ok_or_else(|| Error::refused(table, RefusalReason::KeyTypeUnsupported))?;
        check_value_type(table, &value_field.field_type)?;
        let bucketing = bucket_field(table, md.default_partition_spec(), key_field.id)?
            .map(|field| field.bucketing);
        Ok(Self {
            bucketing,
            key_column: key_column.to_string(),
            key_field_id: key_field.id,
            key_type,
            value_column: value_column.to_string(),
            value_field_id: value_field.id,
        })
    }
}

/// The value column is served as opaque bytes (RFD 010), so its Iceberg
/// type must be one whose bytes are the value: binary, fixed, or string.
/// Anything else is `RefusalReason::ValueTypeUnsupported`, carrying the
/// type's `Display`.
fn check_value_type(table: &str, ty: &Type) -> Result<()> {
    match ty {
        Type::Primitive(
            PrimitiveType::Binary | PrimitiveType::Fixed(_) | PrimitiveType::String,
        ) => Ok(()),
        Type::Primitive(
            PrimitiveType::Boolean
            | PrimitiveType::Int
            | PrimitiveType::Long
            | PrimitiveType::Float
            | PrimitiveType::Double
            | PrimitiveType::Decimal { .. }
            | PrimitiveType::Date
            | PrimitiveType::Time
            | PrimitiveType::Timestamp
            | PrimitiveType::Timestamptz
            | PrimitiveType::TimestampNs
            | PrimitiveType::TimestamptzNs
            | PrimitiveType::Uuid,
        )
        | Type::Struct(_)
        | Type::List(_)
        | Type::Map(_) => Err(Error::refused(
            table,
            RefusalReason::ValueTypeUnsupported {
                got: ty.to_string(),
            },
        )),
    }
}

/// The key's bucket field in one partition spec: the transform, and the
/// position of its value in the spec's partition tuple.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BucketField {
    bucketing: Bucketing,
    position: usize,
}

/// The first field of `spec`, in spec order, that buckets the key column
/// `key_field_id`; `None` when the spec has none. A count of zero is
/// `TableFault::ZeroBuckets`. Any other transform on the key is not
/// routing and is passed over.
fn bucket_field(
    table: &str,
    spec: &PartitionSpec,
    key_field_id: i32,
) -> Result<Option<BucketField>> {
    for (position, field) in spec.fields().iter().enumerate() {
        if field.source_id != key_field_id {
            continue;
        }
        match field.transform {
            Transform::Bucket(0) => {
                return Err(Error::table_fault(
                    table,
                    TableFault::ZeroBuckets {
                        spec_id: spec.spec_id(),
                    },
                ));
            }
            Transform::Bucket(count) => {
                return Ok(Some(BucketField {
                    bucketing: Bucketing {
                        count,
                        source_field_id: key_field_id,
                    },
                    position,
                }));
            }
            Transform::Identity
            | Transform::Truncate(_)
            | Transform::Year
            | Transform::Month
            | Transform::Day
            | Transform::Hour
            | Transform::Void
            | Transform::Unknown => {}
        }
    }
    Ok(None)
}

/// One live data file of a snapshot, as its manifest records it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataFileEntry {
    /// The file's location as the manifest recorded it.
    pub path: String,
    /// The bucket the file holds, from its partition tuple; `UNBUCKETED`
    /// when `bucketing` is `None`. Routing keys files by `bucketing` first
    /// and this second, so the sentinel meets no bucketed file's zero there;
    /// it is a bucket number only where the doctor reports by number alone.
    pub bucket: u32,
    /// The bucket transform of the manifest's own partition spec (RFD 005),
    /// `None` when that spec has no bucket on the key. Files of one
    /// snapshot may differ here after a spec evolution.
    pub bucketing: Option<Bucketing>,
    /// The file's row count, from the manifest.
    pub records: u64,
    /// The file's size in bytes, from the manifest; what `FileHandle::open`
    /// takes in place of a request.
    pub size: u64,
    /// The manifest's lower bound on the key, when it recorded one it can
    /// be read as a key. Advice only: a writer may truncate it, so routing
    /// never rests on it (RFD 007).
    pub key_min: Option<Key>,
    /// The manifest's upper bound on the key, under the same terms.
    pub key_max: Option<Key>,
}

/// The live data files of `snapshot`: the manifest list and every manifest
/// it names, data and delete manifests alike, read through `storage` with
/// `MANIFEST_READ_CONCURRENCY` manifests in flight. An entry with status
/// `Deleted` is skipped; a live entry whose content is not data (a
/// positional or equality delete file, wherever it is listed) is
/// `RefusalReason::DeleteFiles` (RFD 009). Each file's bucket comes from
/// its own manifest's partition spec, looked up in `md` by the id the
/// manifest list records (missing is `TableFault::SpecMissing`): the spec's
/// bucket field on the key names the position in the partition tuple whose
/// integer is the bucket (a non-integer there is
/// `TableFault::BucketValueMissing`, one at or past the spec's count
/// `TableFault::BucketValueOutOfRange`), and a spec with no such field files
/// the entry under `UNBUCKETED` with no bucketing. Key bounds come from the
/// manifest's lower and upper bounds for the key field through
/// `Key::from_datum`. The result is ordered by (bucketing, bucket, `key_min`
/// with `None` first, path), the same for the same manifests in any order.
pub async fn list_data_files(
    table: &str,
    md: &TableMetadata,
    snapshot: &Snapshot,
    layout: &TableLayout,
    storage: &Storage,
) -> Result<Vec<DataFileEntry>> {
    let list_bytes = storage.read_all(snapshot.manifest_list()).await?;
    let list = ManifestList::parse_with_version(&list_bytes, md.format_version())?;
    // Collected before streaming: the library returns an opaque
    // `impl IntoIterator`, whose iterator the compiler cannot prove `Send`,
    // and a load must be spawnable onto a multi-thread runtime.
    let files: Vec<ManifestFile> = list.consume_entries().into_iter().collect();
    let manifests: Vec<(ManifestFile, Manifest)> = stream::iter(files)
        .map(|file| async move {
            let bytes = storage.read_all(&file.manifest_path).await?;
            let manifest = Manifest::parse_avro(&bytes)?;
            Ok::<_, Error>((file, manifest))
        })
        .buffered(MANIFEST_READ_CONCURRENCY)
        .try_collect()
        .await?;
    let mut entries = Vec::new();
    for (file, manifest) in &manifests {
        let spec = md
            .partition_spec_by_id(file.partition_spec_id)
            .ok_or_else(|| {
                Error::table_fault(
                    table,
                    TableFault::SpecMissing {
                        spec_id: file.partition_spec_id,
                    },
                )
            })?;
        let bucket_field = bucket_field(table, spec, layout.key_field_id)?;
        for entry in manifest.entries() {
            match entry.status() {
                ManifestStatus::Deleted => continue,
                ManifestStatus::Added | ManifestStatus::Existing => {}
            }
            let data_file = entry.data_file();
            match data_file.content_type() {
                DataContentType::Data => {}
                DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                    return Err(Error::refused(table, RefusalReason::DeleteFiles));
                }
            }
            let (bucketing, bucket) = match bucket_field {
                None => (None, UNBUCKETED),
                Some(field) => (
                    Some(field.bucketing),
                    bucket_value(table, data_file, spec.spec_id(), field)?,
                ),
            };
            entries.push(DataFileEntry {
                path: data_file.file_path().to_string(),
                bucket,
                bucketing,
                records: data_file.record_count(),
                size: data_file.file_size_in_bytes(),
                key_min: key_bound(data_file.lower_bounds(), layout.key_field_id),
                key_max: key_bound(data_file.upper_bounds(), layout.key_field_id),
            });
        }
    }
    entries.sort_by(entry_order);
    Ok(entries)
}

/// The bucket in `data_file`'s partition tuple at `field`'s position: a
/// non-negative integer literal there, or `TableFault::BucketValueMissing`;
/// one at or past `field`'s bucket count, which no key hashes to, is
/// `TableFault::BucketValueOutOfRange`.
fn bucket_value(
    table: &str,
    data_file: &DataFile,
    spec_id: i32,
    field: BucketField,
) -> Result<u32> {
    let literal = data_file
        .partition()
        .fields()
        .get(field.position)
        .and_then(Option::as_ref);
    let bucket = match literal {
        Some(Literal::Primitive(PrimitiveLiteral::Int(value))) => u32::try_from(*value).ok(),
        Some(
            Literal::Primitive(
                PrimitiveLiteral::Boolean(_)
                | PrimitiveLiteral::Long(_)
                | PrimitiveLiteral::Float(_)
                | PrimitiveLiteral::Double(_)
                | PrimitiveLiteral::String(_)
                | PrimitiveLiteral::Binary(_)
                | PrimitiveLiteral::Int128(_)
                | PrimitiveLiteral::UInt128(_)
                | PrimitiveLiteral::AboveMax
                | PrimitiveLiteral::BelowMin,
            )
            | Literal::Struct(_)
            | Literal::List(_)
            | Literal::Map(_),
        )
        | None => None,
    };
    let bucket = bucket.ok_or_else(|| {
        Error::table_fault(
            table,
            TableFault::BucketValueMissing {
                path: data_file.file_path().to_string(),
                spec_id,
            },
        )
    })?;
    let count = field.bucketing.count;
    if bucket >= count {
        return Err(Error::table_fault(
            table,
            TableFault::BucketValueOutOfRange {
                path: data_file.file_path().to_string(),
                spec_id,
                bucket,
                count,
            },
        ));
    }
    Ok(bucket)
}

/// The bound recorded for the key field, as a key; `None` when the
/// manifest recorded none or one that is not a key literal.
fn key_bound(bounds: &HashMap<i32, Datum>, key_field_id: i32) -> Option<Key> {
    bounds.get(&key_field_id).and_then(Key::from_datum)
}

/// The order of `list_data_files`'s result.
fn entry_order(a: &DataFileEntry, b: &DataFileEntry) -> Ordering {
    a.bucketing
        .cmp(&b.bucketing)
        .then(a.bucket.cmp(&b.bucket))
        .then_with(|| a.key_min.cmp(&b.key_min))
        .then_with(|| a.path.cmp(&b.path))
}

/// Iceberg tables written with iceberg-rust's own writers, for the tests of
/// this module and of `generation`: metadata from `TableMetadataBuilder`,
/// manifests and manifest lists written through a filesystem `FileIO` into
/// a temporary directory, and read back through `Storage`'s `file://`
/// store. Nothing here is compiled outside tests.
#[cfg(test)]
pub(crate) mod testing {
    use std::collections::HashMap;
    use std::sync::Arc;

    use bytes::Bytes;
    use iceberg::io::FileIO;
    use iceberg::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Datum, FormatVersion, Literal,
        MAIN_BRANCH, ManifestContentType, ManifestListWriter, ManifestStatus,
        ManifestWriterBuilder, NestedField, Operation, PartitionSpec, PrimitiveType, Schema,
        Snapshot, SortOrder, Struct, Summary, TableMetadata, TableMetadataBuilder, Transform, Type,
        UnboundPartitionSpec,
    };

    use crate::catalog::{METADATA_DIR, hint_location, version_file};
    use crate::key::Key;
    use crate::time;

    /// The key column of every fixture table.
    pub(crate) const KEY_COLUMN: &str = "key";
    /// The value column of every fixture table.
    pub(crate) const VALUE_COLUMN: &str = "value";
    /// The field id `TableMetadataBuilder::new` assigns the first column.
    pub(crate) const KEY_FIELD_ID: i32 = 1;
    /// The field id it assigns the second column.
    pub(crate) const VALUE_FIELD_ID: i32 = 2;
    /// The directory under a table root holding its data files.
    pub(crate) const DATA_DIR: &str = "data";
    /// The timestamp of fixture snapshot `sequence` of `md`: after the
    /// metadata's last update, which the builder set to now, and increasing
    /// with the sequence, as the builder requires of a commit.
    pub(crate) fn snapshot_timestamp(md: &TableMetadata, sequence: i64) -> i64 {
        time::millis_after(md.last_updated_ms(), sequence)
    }

    /// A `file://` location for a temporary directory.
    pub(crate) fn file_root(dir: &std::path::Path) -> String {
        format!("file://{}", dir.display())
    }

    /// A schema of `KEY_COLUMN` of `key_type` and a nullable `VALUE_COLUMN`
    /// of `value_type`, fields `KEY_FIELD_ID` and `VALUE_FIELD_ID`.
    pub(crate) fn schema(key_type: Type, value_type: Type) -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::required(KEY_FIELD_ID, KEY_COLUMN, key_type).into(),
                NestedField::optional(VALUE_FIELD_ID, VALUE_COLUMN, value_type).into(),
            ])
            .build()
            .unwrap()
    }

    /// The long-keyed, binary-valued schema most fixtures use.
    pub(crate) fn long_schema() -> Schema {
        schema(
            Type::Primitive(PrimitiveType::Long),
            Type::Primitive(PrimitiveType::Binary),
        )
    }

    /// A spec bucketing the key into `count` buckets, its field named by the
    /// count so two specs of one table never share a field name.
    pub(crate) fn bucket_spec(count: u32) -> UnboundPartitionSpec {
        UnboundPartitionSpec::builder()
            .add_partition_field(
                KEY_FIELD_ID,
                format!("key_bucket_{count}"),
                Transform::Bucket(count),
            )
            .unwrap()
            .build()
    }

    /// A spec with no partition field.
    pub(crate) fn unpartitioned() -> UnboundPartitionSpec {
        UnboundPartitionSpec::builder().build()
    }

    /// Metadata for a table at `root` under `schema` and `spec`, with no
    /// snapshot yet; format version 2.
    pub(crate) fn metadata(
        root: &str,
        schema: Schema,
        spec: UnboundPartitionSpec,
    ) -> TableMetadata {
        TableMetadataBuilder::new(
            schema,
            spec,
            SortOrder::unsorted_order(),
            root.to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata
    }

    /// `md` with `snapshot` added and the main branch moved to it.
    pub(crate) fn with_snapshot(md: TableMetadata, snapshot: Snapshot) -> TableMetadata {
        md.into_builder(None)
            .set_branch_snapshot(snapshot, MAIN_BRANCH)
            .unwrap()
            .build()
            .unwrap()
            .metadata
    }

    /// What a fixture file's partition tuple holds.
    #[derive(Clone, Copy, Debug)]
    pub(crate) enum Partition {
        /// A bucket under a bucket spec.
        Bucket(u32),
        /// A null where a bucket spec expects the bucket.
        NullBucket,
        /// The empty tuple of an unpartitioned spec.
        None,
    }

    /// The partition tuple `partition` describes.
    pub(crate) fn partition(partition: Partition) -> Struct {
        match partition {
            Partition::Bucket(bucket) => {
                Struct::from_iter([Some(Literal::int(i32::try_from(bucket).unwrap()))])
            }
            Partition::NullBucket => Struct::from_iter([None]),
            Partition::None => Struct::empty(),
        }
    }

    /// A key as the datum a manifest bound carries.
    pub(crate) fn datum(key: &Key) -> Datum {
        match key {
            Key::Long(value) => Datum::long(*value),
            Key::Str(text) => Datum::string(text),
            Key::Bytes(bytes) => Datum::binary(bytes.clone()),
            Key::Uuid(bytes) => Datum::uuid(uuid::Uuid::from_bytes(*bytes)),
        }
    }

    /// A data file of `content` at `path` under `spec_id`, with `records`
    /// rows, `size` bytes, and key bounds when given.
    pub(crate) fn data_file(
        path: &str,
        spec_id: i32,
        partition: Struct,
        records: u64,
        size: u64,
        bounds: Option<(&Key, &Key)>,
        content: DataContentType,
    ) -> DataFile {
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
        builder.build().unwrap()
    }

    /// One manifest to write: the spec its files are under, its content,
    /// and its entries with their status.
    pub(crate) struct ManifestSpec {
        /// The partition spec the manifest's files are under.
        pub(crate) spec_id: i32,
        /// Whether it lists data files or delete files.
        pub(crate) content: ManifestContentType,
        /// Its entries, each with the status the manifest records for it.
        pub(crate) entries: Vec<(DataFile, ManifestStatus)>,
    }

    /// Writes one manifest per `manifests` and the manifest list naming
    /// them under `md`'s root, and returns the snapshot that points at the
    /// list, ready for `with_snapshot`. `sequence` is the snapshot's and
    /// every entry's sequence number.
    pub(crate) async fn write_snapshot(
        md: &TableMetadata,
        snapshot_id: i64,
        parent: Option<i64>,
        sequence: i64,
        schema_id: i32,
        manifests: Vec<ManifestSpec>,
    ) -> Snapshot {
        let file_io = FileIO::new_with_fs();
        let root = md.location();
        let schema = Arc::clone(md.schema_by_id(schema_id).unwrap());
        let mut files = Vec::with_capacity(manifests.len());
        for (index, manifest) in manifests.into_iter().enumerate() {
            let path = format!("{root}/{METADATA_DIR}/{snapshot_id}-m{index}.avro");
            let spec = PartitionSpec::clone(md.partition_spec_by_id(manifest.spec_id).unwrap());
            let builder = ManifestWriterBuilder::new(
                file_io.new_output(&path).unwrap(),
                Some(snapshot_id),
                Arc::clone(&schema),
                spec,
            );
            let mut writer = match manifest.content {
                ManifestContentType::Data => builder.build_v2_data(),
                ManifestContentType::Deletes => builder.build_v2_deletes(),
            };
            for (data_file, status) in manifest.entries {
                match status {
                    ManifestStatus::Added => writer.add_file(data_file, sequence).unwrap(),
                    ManifestStatus::Existing => writer
                        .add_existing_file(data_file, snapshot_id, sequence, Some(sequence))
                        .unwrap(),
                    ManifestStatus::Deleted => writer
                        .add_delete_file(data_file, sequence, Some(sequence))
                        .unwrap(),
                }
            }
            files.push(writer.write_manifest_file().await.unwrap());
        }
        let list_path = format!("{root}/{METADATA_DIR}/snap-{snapshot_id}.avro");
        let mut list = ManifestListWriter::v2(
            file_io
                .new_output(&list_path)
                .unwrap()
                .writer()
                .await
                .unwrap(),
            snapshot_id,
            parent,
            sequence,
        );
        list.add_manifests(files.into_iter()).unwrap();
        list.close().await.unwrap();
        Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(parent)
            .with_sequence_number(sequence)
            .with_timestamp_ms(snapshot_timestamp(md, sequence))
            .with_manifest_list(list_path)
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .with_schema_id(schema_id)
            .build()
    }

    /// Writes `md` as metadata file `version` under its root and points the
    /// root's version hint at it; returns the file's location.
    pub(crate) async fn write_metadata(md: &TableMetadata, version: u64) -> String {
        let file_io = FileIO::new_with_fs();
        let location = version_file(md.location(), version);
        let json = serde_json::to_vec(md).unwrap();
        write_file(&file_io, &location, Bytes::from(json)).await;
        write_file(
            &file_io,
            &hint_location(md.location()),
            Bytes::from(version.to_string()),
        )
        .await;
        location
    }

    /// Writes `bytes` at `location` through `file_io`, creating directories
    /// on the way.
    pub(crate) async fn write_file(file_io: &FileIO, location: &str, bytes: Bytes) {
        file_io
            .new_output(location)
            .unwrap()
            .write(bytes)
            .await
            .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use iceberg::spec::{
        DataContentType, ManifestContentType, ManifestStatus, NestedField, PrimitiveType, Schema,
        Snapshot, TableMetadataBuilder, Type,
    };

    use super::testing::{self, DATA_DIR, KEY_COLUMN, KEY_FIELD_ID, VALUE_COLUMN, VALUE_FIELD_ID};
    use super::*;
    use crate::bucket;
    use crate::config::StorageConfig;
    use crate::generation::testing::{TABLE, refusal_of, table_fault_of};
    use crate::key::UUID_BYTES;

    /// A column name no fixture schema has.
    const ABSENT_COLUMN: &str = "absent";
    /// The second value column name of the evolved schema.
    const RENAMED_VALUE_COLUMN: &str = "payload";
    /// The field id of the evolved schema's new column.
    const RENAMED_VALUE_FIELD_ID: i32 = 3;
    /// A schema id no fixture metadata has.
    const ABSENT_SCHEMA_ID: i32 = 7;
    /// The bucket count of the first spec.
    const BUCKETS: u32 = 4;
    /// The bucket count of the evolved spec.
    const MORE_BUCKETS: u32 = 8;
    /// The ids of the two fixture snapshots, distinct from every sequence
    /// number so a test that confuses the two fails.
    const SNAPSHOT_A: i64 = 11;
    const SNAPSHOT_B: i64 = 22;
    /// Their sequence numbers, in commit order.
    const SEQUENCE_A: i64 = 1;
    const SEQUENCE_B: i64 = 2;
    /// The record count every fixture data file claims; nothing here reads
    /// a data file, so any count serves.
    const RECORDS: u64 = 1_000;
    /// The size every fixture data file claims, for the same reason.
    const SIZE: u64 = 4_096;
    /// Key bounds a fixture file claims, `LOW..=HIGH`.
    const LOW: i64 = 10;
    const HIGH: i64 = 20;
    /// Key bounds of a file below it, `LOWER..=HIGHER`, so the two ranges
    /// are disjoint.
    const LOWER: i64 = 5;
    const HIGHER: i64 = 7;
    /// Keys the routing tests hash.
    const SAMPLE_KEYS: [i64; 5] = [0, 1, 7, 1_000, -3];
    /// A snapshot id for the placeholder snapshots that derive takes.
    const PLACEHOLDER_SNAPSHOT: i64 = 1;
    /// The width of the `fixed` value column of the value-type test; any
    /// width is admitted, so one is enough.
    const FIXED_VALUE_BYTES: u64 = 8;

    fn storage() -> Storage {
        Storage::new(StorageConfig::default())
    }

    /// A snapshot recording `schema_id`, or none, for `derive`, which reads
    /// nothing else of it.
    fn snapshot_with_schema(schema_id: Option<i32>) -> Snapshot {
        let builder = Snapshot::builder()
            .with_snapshot_id(PLACEHOLDER_SNAPSHOT)
            .with_sequence_number(SEQUENCE_A)
            .with_timestamp_ms(0)
            .with_manifest_list("mem://w/db/kv/metadata/snap.avro")
            .with_summary(iceberg::spec::Summary {
                operation: iceberg::spec::Operation::Append,
                additional_properties: std::collections::HashMap::new(),
            });
        match schema_id {
            Some(schema_id) => builder.with_schema_id(schema_id).build(),
            None => builder.build(),
        }
    }

    fn derive(md: &TableMetadata, snapshot: &Snapshot) -> Result<TableLayout> {
        TableLayout::derive(TABLE, md, snapshot, KEY_COLUMN, VALUE_COLUMN)
    }

    /// The table fault `result` ended in, under `TABLE`.
    fn fault_of<T: std::fmt::Debug>(result: Result<T>) -> TableFault {
        table_fault_of(result, TABLE)
    }

    fn data_path(root: &str, name: &str) -> String {
        format!("{root}/{DATA_DIR}/{name}.parquet")
    }

    /// A live data file in `bucket` of `spec_id` with the given bounds.
    fn live(
        root: &str,
        name: &str,
        spec_id: i32,
        bucket: Option<u32>,
        bounds: Option<(i64, i64)>,
    ) -> (iceberg::spec::DataFile, ManifestStatus) {
        let bounds = bounds.map(|(min, max)| (Key::Long(min), Key::Long(max)));
        (
            testing::data_file(
                &data_path(root, name),
                spec_id,
                testing::partition(
                    bucket.map_or(testing::Partition::None, testing::Partition::Bucket),
                ),
                RECORDS,
                SIZE,
                bounds.as_ref().map(|(min, max)| (min, max)),
                DataContentType::Data,
            ),
            ManifestStatus::Added,
        )
    }

    fn bucketing(count: u32) -> Bucketing {
        Bucketing {
            count,
            source_field_id: KEY_FIELD_ID,
        }
    }

    #[test]
    fn test_derive_admits_every_key_type_including_uuid_and_fixed() {
        let dir = tempfile::tempdir().unwrap();
        let cases = [
            (PrimitiveType::Long, KeyType::Long),
            (PrimitiveType::String, KeyType::String),
            (PrimitiveType::Binary, KeyType::Binary),
            (
                PrimitiveType::Fixed(u64::try_from(UUID_BYTES).unwrap()),
                KeyType::Binary,
            ),
            (PrimitiveType::Uuid, KeyType::Uuid),
        ];
        for (primitive, expected) in cases {
            let md = testing::metadata(
                &testing::file_root(dir.path()),
                testing::schema(
                    Type::Primitive(primitive),
                    Type::Primitive(PrimitiveType::Binary),
                ),
                testing::bucket_spec(BUCKETS),
            );
            let layout = derive(&md, &snapshot_with_schema(Some(md.current_schema_id()))).unwrap();
            assert_eq!(
                layout,
                TableLayout {
                    bucketing: Some(bucketing(BUCKETS)),
                    key_column: KEY_COLUMN.to_string(),
                    key_field_id: KEY_FIELD_ID,
                    key_type: expected,
                    value_column: VALUE_COLUMN.to_string(),
                    value_field_id: VALUE_FIELD_ID,
                }
            );
        }
    }

    #[test]
    fn test_derive_refuses_an_unsupported_key_type() {
        let dir = tempfile::tempdir().unwrap();
        let md = testing::metadata(
            &testing::file_root(dir.path()),
            testing::schema(
                Type::Primitive(PrimitiveType::Int),
                Type::Primitive(PrimitiveType::Binary),
            ),
            testing::unpartitioned(),
        );
        assert_eq!(
            refusal_of(derive(&md, &snapshot_with_schema(None))),
            RefusalReason::KeyTypeUnsupported
        );
    }

    #[test]
    fn test_derive_admits_binary_fixed_and_string_values_and_refuses_the_rest() {
        let dir = tempfile::tempdir().unwrap();
        let root = testing::file_root(dir.path());
        for admitted in [
            PrimitiveType::Binary,
            PrimitiveType::Fixed(FIXED_VALUE_BYTES),
            PrimitiveType::String,
        ] {
            let md = testing::metadata(
                &root,
                testing::schema(
                    Type::Primitive(PrimitiveType::Long),
                    Type::Primitive(admitted),
                ),
                testing::unpartitioned(),
            );
            assert!(derive(&md, &snapshot_with_schema(None)).is_ok());
        }
        for refused in [
            Type::Primitive(PrimitiveType::Long),
            Type::Primitive(PrimitiveType::Uuid),
            Type::Primitive(PrimitiveType::Double),
        ] {
            let md = testing::metadata(
                &root,
                testing::schema(Type::Primitive(PrimitiveType::Long), refused.clone()),
                testing::unpartitioned(),
            );
            assert_eq!(
                refusal_of(derive(&md, &snapshot_with_schema(None))),
                RefusalReason::ValueTypeUnsupported {
                    got: refused.to_string(),
                }
            );
        }
    }

    #[test]
    fn test_derive_missing_column_is_a_table_fault() {
        let dir = tempfile::tempdir().unwrap();
        let md = testing::metadata(
            &testing::file_root(dir.path()),
            testing::long_schema(),
            testing::unpartitioned(),
        );
        let snapshot = snapshot_with_schema(None);
        for (key_column, value_column) in
            [(ABSENT_COLUMN, VALUE_COLUMN), (KEY_COLUMN, ABSENT_COLUMN)]
        {
            assert_eq!(
                fault_of(TableLayout::derive(
                    TABLE,
                    &md,
                    &snapshot,
                    key_column,
                    value_column
                )),
                TableFault::ColumnMissing {
                    column: ABSENT_COLUMN.to_string(),
                }
            );
        }
    }

    /// The evolved schema: the value column renamed, under a new field id.
    fn renamed_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::required(
                    KEY_FIELD_ID,
                    KEY_COLUMN,
                    Type::Primitive(PrimitiveType::Long),
                )
                .into(),
                NestedField::optional(
                    RENAMED_VALUE_FIELD_ID,
                    RENAMED_VALUE_COLUMN,
                    Type::Primitive(PrimitiveType::Binary),
                )
                .into(),
            ])
            .build()
            .unwrap()
    }

    #[test]
    fn test_derive_uses_the_snapshots_schema_not_the_current_one() {
        let dir = tempfile::tempdir().unwrap();
        let first = testing::metadata(
            &testing::file_root(dir.path()),
            testing::long_schema(),
            testing::unpartitioned(),
        );
        let first_schema_id = first.current_schema_id();
        let md = first
            .into_builder(None)
            .add_schema(renamed_schema())
            .unwrap()
            .set_current_schema(TableMetadataBuilder::LAST_ADDED)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let current_schema_id = md.current_schema_id();
        assert_ne!(current_schema_id, first_schema_id);
        assert!(md.current_schema().field_by_name(VALUE_COLUMN).is_none());

        // A snapshot written under the first schema still finds `value`.
        let old = derive(&md, &snapshot_with_schema(Some(first_schema_id))).unwrap();
        assert_eq!(old.value_field_id, VALUE_FIELD_ID);
        // One written under the current schema does not.
        assert_eq!(
            fault_of(derive(&md, &snapshot_with_schema(Some(current_schema_id)))),
            TableFault::ColumnMissing {
                column: VALUE_COLUMN.to_string(),
            }
        );
        let renamed = TableLayout::derive(
            TABLE,
            &md,
            &snapshot_with_schema(Some(current_schema_id)),
            KEY_COLUMN,
            RENAMED_VALUE_COLUMN,
        )
        .unwrap();
        assert_eq!(renamed.value_field_id, RENAMED_VALUE_FIELD_ID);
        // A snapshot without a schema id uses the current schema.
        let by_default = TableLayout::derive(
            TABLE,
            &md,
            &snapshot_with_schema(None),
            KEY_COLUMN,
            RENAMED_VALUE_COLUMN,
        )
        .unwrap();
        assert_eq!(by_default.value_field_id, RENAMED_VALUE_FIELD_ID);
        assert_eq!(
            fault_of(derive(&md, &snapshot_with_schema(Some(ABSENT_SCHEMA_ID)))),
            TableFault::SchemaMissing {
                schema_id: ABSENT_SCHEMA_ID,
            }
        );
    }

    #[test]
    fn test_derive_bucketing_present_and_absent() {
        let dir = tempfile::tempdir().unwrap();
        let root = testing::file_root(dir.path());
        let unbucketed = testing::metadata(&root, testing::long_schema(), testing::unpartitioned());
        let layout = derive(&unbucketed, &snapshot_with_schema(None)).unwrap();
        assert_eq!(layout.bucketing, None);
        let bucketed =
            testing::metadata(&root, testing::long_schema(), testing::bucket_spec(BUCKETS));
        let routed = derive(&bucketed, &snapshot_with_schema(None)).unwrap();
        assert_eq!(routed.bucketing, Some(bucketing(BUCKETS)));
        for key in SAMPLE_KEYS.map(Key::Long) {
            assert_eq!(
                bucketing(BUCKETS).bucket_of(&key),
                bucket::bucket(&key, BUCKETS)
            );
        }
    }

    /// A spec bucketing the key twice, by `BUCKETS` and then `MORE_BUCKETS`.
    fn twice_bucketed() -> iceberg::spec::UnboundPartitionSpec {
        iceberg::spec::UnboundPartitionSpec::builder()
            .add_partition_field(KEY_FIELD_ID, "first_bucket", Transform::Bucket(BUCKETS))
            .unwrap()
            .add_partition_field(
                KEY_FIELD_ID,
                "second_bucket",
                Transform::Bucket(MORE_BUCKETS),
            )
            .unwrap()
            .build()
    }

    #[tokio::test]
    async fn test_derive_and_list_data_files_take_the_first_bucket_field_on_the_key() {
        let dir = tempfile::tempdir().unwrap();
        let root = testing::file_root(dir.path());
        let md = testing::metadata(&root, testing::long_schema(), twice_bucketed());
        let spec_id = md.default_partition_spec_id();
        let layout = derive(&md, &snapshot_with_schema(None)).unwrap();
        assert_eq!(layout.bucketing, Some(bucketing(BUCKETS)));
        // The partition tuple carries both buckets; the first is the route.
        let (first_bucket, second_bucket) = (2, 5);
        let path = data_path(&root, "twice");
        let snapshot = testing::write_snapshot(
            &md,
            SNAPSHOT_A,
            None,
            SEQUENCE_A,
            md.current_schema_id(),
            vec![testing::ManifestSpec {
                spec_id,
                content: ManifestContentType::Data,
                entries: vec![(
                    testing::data_file(
                        &path,
                        spec_id,
                        iceberg::spec::Struct::from_iter([
                            Some(iceberg::spec::Literal::int(first_bucket)),
                            Some(iceberg::spec::Literal::int(second_bucket)),
                        ]),
                        RECORDS,
                        SIZE,
                        None,
                        DataContentType::Data,
                    ),
                    ManifestStatus::Added,
                )],
            }],
        )
        .await;
        let md = testing::with_snapshot(md, snapshot.clone());
        let entries = list_data_files(TABLE, &md, &snapshot, &layout, &storage())
            .await
            .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].bucketing, Some(bucketing(BUCKETS)));
        assert_eq!(entries[0].bucket, u32::try_from(first_bucket).unwrap());
    }

    #[test]
    fn test_derive_zero_buckets_is_a_table_fault() {
        let dir = tempfile::tempdir().unwrap();
        let md = testing::metadata(
            &testing::file_root(dir.path()),
            testing::long_schema(),
            testing::bucket_spec(0),
        );
        assert_eq!(
            fault_of(derive(&md, &snapshot_with_schema(None))),
            TableFault::ZeroBuckets {
                spec_id: md.default_partition_spec_id(),
            }
        );
    }

    #[tokio::test]
    async fn test_list_data_files_skips_deleted_entries_and_orders_by_bucket_and_bounds() {
        let dir = tempfile::tempdir().unwrap();
        let root = testing::file_root(dir.path());
        let md = testing::metadata(&root, testing::long_schema(), testing::bucket_spec(BUCKETS));
        let spec_id = md.default_partition_spec_id();
        let mut deleted = live(&root, "f3", spec_id, Some(0), Some((LOW, HIGH)));
        deleted.1 = ManifestStatus::Deleted;
        let snapshot = testing::write_snapshot(
            &md,
            SNAPSHOT_A,
            None,
            SEQUENCE_A,
            md.current_schema_id(),
            vec![testing::ManifestSpec {
                spec_id,
                content: ManifestContentType::Data,
                entries: vec![
                    live(&root, "f2", spec_id, Some(3), Some((LOWER, HIGHER))),
                    live(&root, "f0", spec_id, Some(1), Some((LOW, HIGH))),
                    deleted,
                    live(&root, "f1", spec_id, Some(1), None),
                ],
            }],
        )
        .await;
        let md = testing::with_snapshot(md, snapshot.clone());
        let layout = derive(&md, &snapshot).unwrap();
        let storage = storage();
        let entries = list_data_files(TABLE, &md, &snapshot, &layout, &storage)
            .await
            .unwrap();
        assert_eq!(storage.stats().gets(), 2, "the list and one manifest");
        let entry = |name: &str, bucket: u32, bounds: Option<(i64, i64)>| DataFileEntry {
            path: data_path(&root, name),
            bucket,
            bucketing: Some(bucketing(BUCKETS)),
            records: RECORDS,
            size: SIZE,
            key_min: bounds.map(|(min, _)| Key::Long(min)),
            key_max: bounds.map(|(_, max)| Key::Long(max)),
        };
        assert_eq!(
            entries,
            vec![
                entry("f1", 1, None),
                entry("f0", 1, Some((LOW, HIGH))),
                entry("f2", 3, Some((LOWER, HIGHER))),
            ]
        );
    }

    #[tokio::test]
    async fn test_list_data_files_unpartitioned_files_carry_no_bucketing() {
        let dir = tempfile::tempdir().unwrap();
        let root = testing::file_root(dir.path());
        let md = testing::metadata(&root, testing::long_schema(), testing::unpartitioned());
        let spec_id = md.default_partition_spec_id();
        let snapshot = testing::write_snapshot(
            &md,
            SNAPSHOT_A,
            None,
            SEQUENCE_A,
            md.current_schema_id(),
            vec![testing::ManifestSpec {
                spec_id,
                content: ManifestContentType::Data,
                entries: vec![
                    live(&root, "b", spec_id, None, Some((LOW, HIGH))),
                    live(&root, "a", spec_id, None, Some((LOW, HIGH))),
                ],
            }],
        )
        .await;
        let md = testing::with_snapshot(md, snapshot.clone());
        let layout = derive(&md, &snapshot).unwrap();
        let entries = list_data_files(TABLE, &md, &snapshot, &layout, &storage())
            .await
            .unwrap();
        let paths: Vec<&str> = entries.iter().map(|entry| entry.path.as_str()).collect();
        assert_eq!(paths, [data_path(&root, "a"), data_path(&root, "b")]);
        for entry in &entries {
            assert_eq!((entry.bucketing, entry.bucket), (None, UNBUCKETED));
        }
    }

    #[tokio::test]
    async fn test_list_data_files_refuses_a_live_delete_file_and_reads_delete_manifests() {
        let dir = tempfile::tempdir().unwrap();
        let root = testing::file_root(dir.path());
        let md = testing::metadata(&root, testing::long_schema(), testing::bucket_spec(BUCKETS));
        let spec_id = md.default_partition_spec_id();
        let position_delete = |name: &str, status: ManifestStatus| {
            (
                testing::data_file(
                    &data_path(&root, name),
                    spec_id,
                    testing::partition(testing::Partition::Bucket(1)),
                    RECORDS,
                    SIZE,
                    None,
                    DataContentType::PositionDeletes,
                ),
                status,
            )
        };
        let with_deletes = testing::write_snapshot(
            &md,
            SNAPSHOT_A,
            None,
            SEQUENCE_A,
            md.current_schema_id(),
            vec![
                testing::ManifestSpec {
                    spec_id,
                    content: ManifestContentType::Data,
                    entries: vec![live(&root, "f0", spec_id, Some(1), None)],
                },
                testing::ManifestSpec {
                    spec_id,
                    content: ManifestContentType::Deletes,
                    entries: vec![position_delete("d0", ManifestStatus::Added)],
                },
            ],
        )
        .await;
        let md = testing::with_snapshot(md, with_deletes.clone());
        let layout = derive(&md, &with_deletes).unwrap();
        let storage = storage();
        assert_eq!(
            refusal_of(list_data_files(TABLE, &md, &with_deletes, &layout, &storage).await),
            RefusalReason::DeleteFiles
        );
        assert_eq!(storage.stats().gets(), 3, "the list and both manifests");

        // A delete manifest whose only entry is itself deleted carries no
        // live delete file; the snapshot serves.
        let removed = testing::write_snapshot(
            &md,
            SNAPSHOT_B,
            Some(SNAPSHOT_A),
            SEQUENCE_B,
            md.current_schema_id(),
            vec![
                testing::ManifestSpec {
                    spec_id,
                    content: ManifestContentType::Data,
                    entries: vec![live(&root, "f0", spec_id, Some(1), None)],
                },
                testing::ManifestSpec {
                    spec_id,
                    content: ManifestContentType::Deletes,
                    entries: vec![position_delete("d0", ManifestStatus::Deleted)],
                },
            ],
        )
        .await;
        let md = testing::with_snapshot(md, removed.clone());
        let entries = list_data_files(TABLE, &md, &removed, &layout, &storage)
            .await
            .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].path, data_path(&root, "f0"));
    }

    /// Metadata whose default spec buckets by `MORE_BUCKETS` after a first
    /// spec of `BUCKETS`, with both spec ids.
    fn evolved(root: &str) -> (TableMetadata, i32, i32) {
        let first = testing::metadata(root, testing::long_schema(), testing::bucket_spec(BUCKETS));
        let first_spec = first.default_partition_spec_id();
        let md = first
            .into_builder(None)
            .add_partition_spec(testing::bucket_spec(MORE_BUCKETS))
            .unwrap()
            .set_default_partition_spec(TableMetadataBuilder::LAST_ADDED)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let second_spec = md.default_partition_spec_id();
        assert_ne!(first_spec, second_spec);
        (md, first_spec, second_spec)
    }

    #[tokio::test]
    async fn test_list_data_files_evolved_spec_gives_each_manifests_bucketing() {
        let dir = tempfile::tempdir().unwrap();
        let root = testing::file_root(dir.path());
        let (md, first_spec, second_spec) = evolved(&root);
        let snapshot = testing::write_snapshot(
            &md,
            SNAPSHOT_A,
            None,
            SEQUENCE_A,
            md.current_schema_id(),
            vec![
                testing::ManifestSpec {
                    spec_id: second_spec,
                    content: ManifestContentType::Data,
                    entries: vec![live(&root, "new", second_spec, Some(5), Some((LOW, HIGH)))],
                },
                testing::ManifestSpec {
                    spec_id: first_spec,
                    content: ManifestContentType::Data,
                    entries: vec![live(&root, "old", first_spec, Some(2), Some((LOW, HIGH)))],
                },
            ],
        )
        .await;
        let md = testing::with_snapshot(md, snapshot.clone());
        let layout = derive(&md, &snapshot).unwrap();
        assert_eq!(
            layout.bucketing,
            Some(bucketing(MORE_BUCKETS)),
            "the default spec's"
        );
        let entries = list_data_files(TABLE, &md, &snapshot, &layout, &storage())
            .await
            .unwrap();
        let routed: Vec<(&str, Option<Bucketing>, u32)> = entries
            .iter()
            .map(|entry| (entry.path.as_str(), entry.bucketing, entry.bucket))
            .collect();
        assert_eq!(
            routed,
            [
                (
                    data_path(&root, "old").as_str(),
                    Some(bucketing(BUCKETS)),
                    2
                ),
                (
                    data_path(&root, "new").as_str(),
                    Some(bucketing(MORE_BUCKETS)),
                    5
                ),
            ]
        );
    }

    #[tokio::test]
    async fn test_list_data_files_missing_spec_is_a_table_fault() {
        let dir = tempfile::tempdir().unwrap();
        let root = testing::file_root(dir.path());
        let (two_specs, _, second_spec) = evolved(&root);
        let snapshot = testing::write_snapshot(
            &two_specs,
            SNAPSHOT_A,
            None,
            SEQUENCE_A,
            two_specs.current_schema_id(),
            vec![testing::ManifestSpec {
                spec_id: second_spec,
                content: ManifestContentType::Data,
                entries: vec![live(&root, "new", second_spec, Some(5), None)],
            }],
        )
        .await;
        // Metadata that never saw the second spec lists the same manifests.
        let one_spec = testing::with_snapshot(
            testing::metadata(&root, testing::long_schema(), testing::bucket_spec(BUCKETS)),
            snapshot.clone(),
        );
        assert!(one_spec.partition_spec_by_id(second_spec).is_none());
        let layout = derive(&one_spec, &snapshot).unwrap();
        assert_eq!(
            fault_of(list_data_files(TABLE, &one_spec, &snapshot, &layout, &storage()).await),
            TableFault::SpecMissing {
                spec_id: second_spec,
            }
        );
    }

    #[tokio::test]
    async fn test_list_data_files_null_bucket_value_is_a_table_fault() {
        let dir = tempfile::tempdir().unwrap();
        let root = testing::file_root(dir.path());
        let md = testing::metadata(&root, testing::long_schema(), testing::bucket_spec(BUCKETS));
        let spec_id = md.default_partition_spec_id();
        let path = data_path(&root, "nobucket");
        let snapshot = testing::write_snapshot(
            &md,
            SNAPSHOT_A,
            None,
            SEQUENCE_A,
            md.current_schema_id(),
            vec![testing::ManifestSpec {
                spec_id,
                content: ManifestContentType::Data,
                entries: vec![(
                    testing::data_file(
                        &path,
                        spec_id,
                        testing::partition(testing::Partition::NullBucket),
                        RECORDS,
                        SIZE,
                        None,
                        DataContentType::Data,
                    ),
                    ManifestStatus::Added,
                )],
            }],
        )
        .await;
        let md = testing::with_snapshot(md, snapshot.clone());
        let layout = derive(&md, &snapshot).unwrap();
        assert_eq!(
            fault_of(list_data_files(TABLE, &md, &snapshot, &layout, &storage()).await),
            TableFault::BucketValueMissing { path, spec_id }
        );
    }

    /// A bucket value at the spec's count names a bucket no key hashes to;
    /// admitted, the file would never be consulted and its keys answered
    /// absent.
    #[tokio::test]
    async fn test_list_data_files_bucket_value_at_or_past_the_count_is_a_table_fault() {
        let dir = tempfile::tempdir().unwrap();
        let root = testing::file_root(dir.path());
        let md = testing::metadata(&root, testing::long_schema(), testing::bucket_spec(BUCKETS));
        let spec_id = md.default_partition_spec_id();
        let snapshot = testing::write_snapshot(
            &md,
            SNAPSHOT_A,
            None,
            SEQUENCE_A,
            md.current_schema_id(),
            vec![testing::ManifestSpec {
                spec_id,
                content: ManifestContentType::Data,
                entries: vec![
                    live(&root, "f0", spec_id, Some(BUCKETS - 1), Some((LOW, HIGH))),
                    live(&root, "f1", spec_id, Some(BUCKETS), Some((LOW, HIGH))),
                ],
            }],
        )
        .await;
        let md = testing::with_snapshot(md, snapshot.clone());
        let layout = derive(&md, &snapshot).unwrap();
        assert_eq!(
            fault_of(list_data_files(TABLE, &md, &snapshot, &layout, &storage()).await),
            TableFault::BucketValueOutOfRange {
                path: data_path(&root, "f1"),
                spec_id,
                bucket: BUCKETS,
                count: BUCKETS,
            }
        );
    }

    #[tokio::test]
    async fn test_list_data_files_order_is_the_same_for_shuffled_manifests() {
        let dir = tempfile::tempdir().unwrap();
        let root = testing::file_root(dir.path());
        let md = testing::metadata(&root, testing::long_schema(), testing::bucket_spec(BUCKETS));
        let spec_id = md.default_partition_spec_id();
        let files = || {
            vec![
                live(&root, "f0", spec_id, Some(1), Some((LOW, HIGH))),
                live(&root, "f1", spec_id, Some(1), None),
                live(&root, "f2", spec_id, Some(3), Some((LOWER, HIGHER))),
                live(&root, "f3", spec_id, Some(0), Some((LOW, HIGH))),
            ]
        };
        let forward = files();
        let mut backward = files();
        backward.reverse();
        let (first_half, second_half) = backward.split_at(2);
        let in_order = testing::write_snapshot(
            &md,
            SNAPSHOT_A,
            None,
            SEQUENCE_A,
            md.current_schema_id(),
            vec![testing::ManifestSpec {
                spec_id,
                content: ManifestContentType::Data,
                entries: forward,
            }],
        )
        .await;
        let shuffled = testing::write_snapshot(
            &md,
            SNAPSHOT_B,
            None,
            SEQUENCE_B,
            md.current_schema_id(),
            vec![
                testing::ManifestSpec {
                    spec_id,
                    content: ManifestContentType::Data,
                    entries: first_half.to_vec(),
                },
                testing::ManifestSpec {
                    spec_id,
                    content: ManifestContentType::Data,
                    entries: second_half.to_vec(),
                },
            ],
        )
        .await;
        let md = testing::with_snapshot(md, in_order.clone());
        let md = testing::with_snapshot(md, shuffled.clone());
        let layout = derive(&md, &in_order).unwrap();
        let storage = storage();
        let a = list_data_files(TABLE, &md, &in_order, &layout, &storage)
            .await
            .unwrap();
        let b = list_data_files(TABLE, &md, &shuffled, &layout, &storage)
            .await
            .unwrap();
        assert_eq!(a, b);
        assert_eq!(a.len(), 4);
        let order: Vec<(u32, Option<Key>)> = a
            .iter()
            .map(|entry| (entry.bucket, entry.key_min.clone()))
            .collect();
        assert_eq!(
            order,
            [
                (0, Some(Key::Long(LOW))),
                (1, None),
                (1, Some(Key::Long(LOW))),
                (3, Some(Key::Long(LOWER))),
            ]
        );
    }
}
