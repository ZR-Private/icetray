//! The brute-force merge-on-read oracle (RFD 021, Measurements): every row
//! of every live data file of a fixture's current snapshot and every live
//! delete, read back with arrow, iceberg's own manifest reader (which
//! applies Iceberg's sequence number inheritance itself), iceberg's Puffin
//! reader, and this crate's own deletion vector decoding, with Iceberg's
//! rules applied by hand: a delete applies to the data file it names when
//! the file's data sequence number is no greater than the delete's and the
//! two entries record one partition (spec id and tuple), a deletion vector
//! masks position deletes for its file, and at most one deletion vector
//! applies to a file. It shares no code with icetray's reader, so a test
//! checks every answer the server gives against it.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use arrow::array::{
    Array, BinaryArray, FixedSizeBinaryArray, Int64Array, RecordBatch, StringArray,
};
use arrow::datatypes::DataType;
use bytes::Bytes;
use iceberg::io::FileIO;
use iceberg::puffin::PuffinReader;
use iceberg::spec::{DataContentType, DataFile, DataFileFormat, ManifestList};
use icetray_core::deletes::{
    CARDINALITY_PROPERTY, FILE_PATH_COLUMN, POS_COLUMN, REFERENCED_DATA_FILE_PROPERTY,
};
use icetray_core::key::UUID_BYTES;
use icetray_core::{Key, Value};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use roaring::RoaringTreemap;

use crate::delete_files::{VECTOR_FRAME_BYTES, VECTOR_MAGIC};
use crate::error::{OracleFault, Result};
use crate::fixture::Fixture;
use crate::layout::{KEY_COLUMN, VALUE_COLUMN};

/// One copy of a key: a row of a live data file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowCopy {
    /// The data file's location.
    pub path: String,
    /// The row's position in the file, counted from its first row.
    pub position: u64,
    /// The row's value; `None` is a null.
    pub value: Option<Vec<u8>>,
    /// Whether a delete that applies to the file names the row.
    pub deleted: bool,
}

/// Every copy of every key of a snapshot, with the deletes applied.
#[derive(Clone, Debug)]
pub struct MergeOracle {
    copies: BTreeMap<Key, Vec<RowCopy>>,
    deleted: HashMap<String, BTreeSet<u64>>,
}

/// One live content file, data or delete, with its data sequence number
/// and the partition spec its manifest is written under.
struct Live {
    file: DataFile,
    sequence: i64,
    spec_id: i32,
}

impl Live {
    /// Whether `delete` applies to this data file by the spec's rules
    /// short of what it names: by sequence number, and within one
    /// partition.
    fn applies(&self, delete: &Live) -> bool {
        delete.sequence >= self.sequence
            && delete.spec_id == self.spec_id
            && delete.file.partition() == self.file.partition()
    }
}

impl MergeOracle {
    /// The oracle of `fixture`'s current snapshot. Two live copies of one
    /// key are `OracleFault::LiveDuplicate`, two deletion vectors applying
    /// to one data file `OracleFault::TwoVectors`, and a deletion vector
    /// that does not decode or contradicts its entry
    /// `OracleFault::MalformedVector`.
    pub async fn read(fixture: &Fixture) -> Result<Self> {
        let file_io = fixture.file_io();
        let metadata = fixture.metadata();
        let Some(snapshot) = metadata.current_snapshot() else {
            return Ok(Self {
                copies: BTreeMap::new(),
                deleted: HashMap::new(),
            });
        };
        let list_bytes = file_io.new_input(snapshot.manifest_list())?.read().await?;
        let list = ManifestList::parse_with_version(&list_bytes, metadata.format_version())?;
        let mut data = Vec::new();
        let mut live_deletes = Vec::new();
        for manifest_file in list.entries() {
            let manifest = manifest_file.load_manifest(file_io).await?;
            for entry in manifest.entries() {
                if !entry.is_alive() {
                    continue;
                }
                let sequence = entry
                    .sequence_number()
                    .ok_or_else(|| OracleFault::NoSequence {
                        path: entry.file_path().to_string(),
                    })?;
                let live = Live {
                    file: entry.data_file().clone(),
                    sequence,
                    spec_id: manifest_file.partition_spec_id,
                };
                match live.file.content_type() {
                    DataContentType::Data => data.push(live),
                    DataContentType::PositionDeletes => live_deletes.push(live),
                    DataContentType::EqualityDeletes => {
                        return Err(OracleFault::EqualityDelete {
                            path: live.file.file_path().to_string(),
                        }
                        .into());
                    }
                }
            }
        }
        let deleted = apply(file_io, &data, &live_deletes).await?;
        let mut copies: BTreeMap<Key, Vec<RowCopy>> = BTreeMap::new();
        for live in &data {
            let path = live.file.file_path();
            let gone = deleted.get(path);
            for (position, (key, value)) in (0_u64..).zip(read_rows(file_io, path).await?) {
                copies.entry(key).or_default().push(RowCopy {
                    path: path.to_string(),
                    position,
                    value,
                    deleted: gone.is_some_and(|gone| gone.contains(&position)),
                });
            }
        }
        for (key, held) in &copies {
            let mut live = held.iter().filter(|copy| !copy.deleted);
            if let (Some(first), Some(second)) = (live.next(), live.next()) {
                return Err(OracleFault::LiveDuplicate {
                    key: key.clone(),
                    first: first.path.clone(),
                    second: second.path.clone(),
                }
                .into());
            }
        }
        Ok(Self { copies, deleted })
    }

    /// What a lookup of `key` answers: its one live copy's value, or
    /// `Absent` when every copy is deleted or there is none.
    pub fn expected(&self, key: &Key) -> Value {
        match self.live_copy(key) {
            Some(RowCopy {
                value: Some(value), ..
            }) => Value::Present(Bytes::from(value.clone())),
            Some(RowCopy { value: None, .. }) => Value::Null,
            None => Value::Absent,
        }
    }

    /// The live copy of `key`, when it has one.
    pub fn live_copy(&self, key: &Key) -> Option<&RowCopy> {
        self.copies(key).iter().find(|copy| !copy.deleted)
    }

    /// Every copy of `key`, live and deleted, in the order the data files
    /// are listed.
    pub fn copies(&self, key: &Key) -> &[RowCopy] {
        self.copies.get(key).map_or(&[], Vec::as_slice)
    }

    /// Every key with a copy, live or deleted, ascending.
    pub fn keys(&self) -> impl Iterator<Item = &Key> {
        self.copies.keys()
    }

    /// The positions of the data file at `path` that the applying deletes
    /// name, whether or not the file holds a row there.
    pub fn deleted_positions(&self, path: &str) -> BTreeSet<u64> {
        self.deleted.get(path).cloned().unwrap_or_default()
    }

    /// Rows the applying deletes remove across every data file.
    pub fn deleted_rows(&self) -> u64 {
        self.copies
            .values()
            .flatten()
            .map(|copy| u64::from(copy.deleted))
            .sum()
    }
}

/// Per data file of `data`, the positions the deletes that apply to it
/// name: its one applying deletion vector, else every applying position
/// delete file's rows naming it. A delete applies by sequence number and
/// only within the data file's partition, as the spec's scan planning
/// rules have it.
async fn apply(
    file_io: &FileIO,
    data: &[Live],
    deletes: &[Live],
) -> Result<HashMap<String, BTreeSet<u64>>> {
    let mut positions: HashMap<String, Vec<(String, u64)>> = HashMap::new();
    for delete in deletes {
        if delete.file.file_format() != DataFileFormat::Puffin {
            let path = delete.file.file_path();
            positions.insert(path.to_string(), read_positions(file_io, path).await?);
        }
    }
    let mut by_file = HashMap::new();
    for live in data {
        let path = live.file.file_path();
        let applies = |delete: &&Live| live.applies(delete);
        let vectors: Vec<&Live> = deletes
            .iter()
            .filter(applies)
            .filter(|delete| {
                delete.file.file_format() == DataFileFormat::Puffin
                    && delete.file.referenced_data_file().as_deref() == Some(path)
            })
            .collect();
        let rows: BTreeSet<u64> = match vectors.as_slice() {
            [] => deletes
                .iter()
                .filter(applies)
                .filter_map(|delete| positions.get(delete.file.file_path()))
                .flatten()
                .filter(|(named, _)| named == path)
                .map(|&(_, position)| position)
                .collect(),
            [vector] => read_vector(file_io, &vector.file).await?.iter().collect(),
            [_, _, ..] => {
                return Err(OracleFault::TwoVectors {
                    data_file: path.to_string(),
                }
                .into());
            }
        };
        by_file.insert(path.to_string(), rows);
    }
    Ok(by_file)
}

/// The rows of the Parquet file at `path`, in file order, as (key, value)
/// from the fixture's key and value columns.
pub(crate) async fn read_rows(file_io: &FileIO, path: &str) -> Result<Vec<(Key, Option<Vec<u8>>)>> {
    let mut rows = Vec::new();
    for batch in read_batches(file_io, path).await? {
        let keys = batch
            .column_by_name(KEY_COLUMN)
            .ok_or_else(|| missing_column(path, KEY_COLUMN))?;
        let values = arrow::compute::cast(
            batch
                .column_by_name(VALUE_COLUMN)
                .ok_or_else(|| missing_column(path, VALUE_COLUMN))?,
            &DataType::Binary,
        )?;
        let values = downcast::<BinaryArray>(&values, path, VALUE_COLUMN)?;
        for row in 0..batch.num_rows() {
            let value = values.is_valid(row).then(|| values.value(row).to_vec());
            rows.push((key_at(keys.as_ref(), row, path)?, value));
        }
    }
    Ok(rows)
}

/// The key at `row` of a key column, which holds longs, strings, bytes,
/// or uuids as fixed bytes.
fn key_at(keys: &dyn Array, row: usize, path: &str) -> Result<Key> {
    let any = keys.as_any();
    if let Some(longs) = any.downcast_ref::<Int64Array>() {
        return Ok(Key::Long(longs.value(row)));
    }
    if let Some(strings) = any.downcast_ref::<StringArray>() {
        return Ok(Key::Str(strings.value(row).to_string()));
    }
    if let Some(bytes) = any.downcast_ref::<BinaryArray>() {
        return Ok(Key::Bytes(bytes.value(row).to_vec()));
    }
    if let Some(fixed) = any.downcast_ref::<FixedSizeBinaryArray>()
        && let Ok(uuid) = <[u8; UUID_BYTES]>::try_from(fixed.value(row))
    {
        return Ok(Key::Uuid(uuid));
    }
    Err(OracleFault::UnexpectedColumn {
        path: path.to_string(),
        column: KEY_COLUMN.to_string(),
        found: keys.data_type().to_string(),
    }
    .into())
}

/// Every row of the position delete file at `path`, as (data file, pos).
pub(crate) async fn read_positions(file_io: &FileIO, path: &str) -> Result<Vec<(String, u64)>> {
    let mut rows = Vec::new();
    for batch in read_batches(file_io, path).await? {
        let paths = batch
            .column_by_name(FILE_PATH_COLUMN)
            .ok_or_else(|| missing_column(path, FILE_PATH_COLUMN))?;
        let positions = batch
            .column_by_name(POS_COLUMN)
            .ok_or_else(|| missing_column(path, POS_COLUMN))?;
        let paths = downcast::<StringArray>(paths, path, FILE_PATH_COLUMN)?;
        let positions = downcast::<Int64Array>(positions, path, POS_COLUMN)?;
        for row in 0..batch.num_rows() {
            let position =
                u64::try_from(positions.value(row)).map_err(|_| OracleFault::NegativePosition {
                    path: path.to_string(),
                })?;
            rows.push((paths.value(row).to_string(), position));
        }
    }
    Ok(rows)
}

/// The positions the deletion vector `entry` names: its blob found in the
/// Puffin footer by the entry's offset and length, its properties checked
/// against the entry, and its bytes decoded here.
pub(crate) async fn read_vector(file_io: &FileIO, entry: &DataFile) -> Result<RoaringTreemap> {
    let path = entry.file_path();
    let malformed = || OracleFault::MalformedVector {
        path: path.to_string(),
    };
    let reader = PuffinReader::new(file_io.new_input(path)?);
    let offset = entry
        .content_offset()
        .and_then(|offset| u64::try_from(offset).ok());
    let length = entry
        .content_size_in_bytes()
        .and_then(|length| u64::try_from(length).ok());
    let metadata = reader
        .file_metadata()
        .await?
        .blobs()
        .iter()
        .find(|blob| Some(blob.offset()) == offset && Some(blob.length()) == length)
        .cloned()
        .ok_or_else(malformed)?;
    let properties = metadata.properties();
    if properties.get(REFERENCED_DATA_FILE_PROPERTY) != entry.referenced_data_file().as_ref()
        || properties.get(CARDINALITY_PROPERTY) != Some(&entry.record_count().to_string())
    {
        return Err(malformed().into());
    }
    let blob = reader.blob(&metadata).await?;
    let positions = decode_vector(blob.data()).ok_or_else(malformed)?;
    if positions.len() != entry.record_count() {
        return Err(malformed().into());
    }
    Ok(positions)
}

/// A deletion vector blob's positions, or `None` when its length, magic,
/// checksum, or vector is malformed.
pub(crate) fn decode_vector(blob: &[u8]) -> Option<RoaringTreemap> {
    let (length, rest) = blob.split_first_chunk::<VECTOR_FRAME_BYTES>()?;
    let body_len = usize::try_from(u32::from_be_bytes(*length)).ok()?;
    let (body, checksum) = rest.split_at_checked(body_len)?;
    let checksum: [u8; VECTOR_FRAME_BYTES] = checksum.try_into().ok()?;
    if u32::from_be_bytes(checksum) != crc32fast::hash(body) {
        return None;
    }
    let vector = body.strip_prefix(VECTOR_MAGIC.as_slice())?;
    let mut reader = vector;
    let positions = RoaringTreemap::deserialize_from(&mut reader).ok()?;
    reader.is_empty().then_some(positions)
}

/// Every record batch of the Parquet file at `path`.
async fn read_batches(file_io: &FileIO, path: &str) -> Result<Vec<RecordBatch>> {
    let bytes = file_io.new_input(path)?.read().await?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)?.build()?;
    Ok(reader.collect::<std::result::Result<Vec<_>, _>>()?)
}

/// `array` as the arrow array type its column is read as.
fn downcast<'a, T: 'static>(array: &'a dyn Array, path: &str, column: &str) -> Result<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        OracleFault::UnexpectedColumn {
            path: path.to_string(),
            column: column.to_string(),
            found: array.data_type().to_string(),
        }
        .into()
    })
}

/// `OracleFault::MissingColumn` for a column the file lacks.
fn missing_column(path: &str, column: &str) -> crate::error::TestkitError {
    OracleFault::MissingColumn {
        path: path.to_string(),
        column: column.to_string(),
    }
    .into()
}

#[cfg(test)]
mod tests {
    use icetray_core::KeyType;

    use super::*;
    use crate::delete_files::{PositionDeletes, write_position_deletes};
    use crate::fixture::{
        DeleteForm, Fixture, FixtureSpec, HandSnapshot, IDS_2K, NAMESPACE, TWO_BUCKETS,
        UNPARTITIONED_FILE_BYTES,
    };
    use crate::fixture::{local_catalog, write_fixture};
    use crate::keys::FIRST_VERSION;
    use crate::{Layout, TestkitError, value_of_kind};

    /// Ids the tests delete or update: one in each thousand-row file of an
    /// unpartitioned `IDS_2K` fixture, and a neighbor of the first.
    const IN_FIRST: u64 = 5;
    const NEIGHBOR: u64 = 6;
    const IN_SECOND: u64 = 1_500;

    /// An unpartitioned `IDS_2K` fixture at `format_version`, one file per
    /// thousand ids.
    async fn unpartitioned(
        catalog: &dyn iceberg::Catalog,
        name: &str,
        format_version: iceberg::spec::FormatVersion,
    ) -> Fixture {
        let spec = FixtureSpec {
            ids: IDS_2K,
            buckets: 0,
            format_version,
            layout: Layout {
                target_file_bytes: UNPARTITIONED_FILE_BYTES,
                ..Layout::contract()
            },
            ..FixtureSpec::small(name, KeyType::Long)
        };
        write_fixture(catalog, NAMESPACE, spec).await.unwrap()
    }

    /// Every key of `fixture` the oracle holds answers what the fixture's
    /// own bookkeeping says: its live ids at their versions, the rest
    /// absent.
    fn assert_agrees(oracle: &MergeOracle, fixture: &Fixture) {
        let live: BTreeSet<Key> = fixture.keys().collect();
        for key in oracle.keys() {
            let expected = if live.contains(key) {
                fixture.expected(key)
            } else {
                Value::Absent
            };
            assert_eq!(oracle.expected(key), expected, "key {key}");
        }
        assert_eq!(
            oracle
                .keys()
                .filter(|key| oracle.live_copy(key).is_some())
                .count(),
            live.len()
        );
    }

    #[tokio::test]
    async fn test_read_applies_each_delete_form() {
        let forms = [
            DeleteForm::Vectors,
            DeleteForm::ScopedPositions,
            DeleteForm::Positions,
        ];
        for (at, form) in forms.into_iter().enumerate() {
            let (_dir, catalog) = local_catalog().await.unwrap();
            let mut fixture = unpartitioned(
                catalog.as_ref(),
                &format!("kv_forms_{at}"),
                iceberg::spec::FormatVersion::V2,
            )
            .await;
            assert_eq!(fixture.file_count(), 2);
            fixture
                .delete(&[IN_FIRST, IN_SECOND], form, "deletes")
                .await
                .unwrap();
            let oracle = MergeOracle::read(&fixture).await.unwrap();
            assert_eq!(oracle.deleted_rows(), 2, "{form:?}");
            for id in [IN_FIRST, IN_SECOND] {
                let key = fixture.key(id);
                assert_eq!(oracle.expected(&key), Value::Absent, "{form:?}");
                assert_eq!(oracle.copies(&key).len(), 1);
            }
            assert!(!fixture.ids().any(|id| id == IN_FIRST));
            assert_agrees(&oracle, &fixture);
        }
    }

    #[tokio::test]
    async fn test_read_answers_an_update_from_the_new_version() {
        let (_dir, catalog) = local_catalog().await.unwrap();
        let mut fixture = unpartitioned(
            catalog.as_ref(),
            "kv_update",
            iceberg::spec::FormatVersion::V2,
        )
        .await;
        fixture
            .update(&[IN_FIRST], DeleteForm::ScopedPositions, "updated")
            .await
            .unwrap();
        let key = fixture.key(IN_FIRST);
        let oracle = MergeOracle::read(&fixture).await.unwrap();
        let copies = oracle.copies(&key);
        assert_eq!(copies.len(), 2, "the deleted old row and the new one");
        assert_eq!(fixture.version_of(IN_FIRST), FIRST_VERSION + 1);
        let first = value_of_kind(&key, fixture.spec().value_bytes, fixture.spec().value_kind);
        let live = oracle.live_copy(&key).unwrap();
        assert_ne!(
            live.value.as_ref(),
            Some(&first),
            "the value carries its version"
        );
        assert_eq!(oracle.expected(&key), fixture.expected(&key));
        assert_agrees(&oracle, &fixture);
    }

    #[tokio::test]
    async fn test_read_takes_a_replaced_vector_and_a_v3_table() {
        let (_dir, catalog) = local_catalog().await.unwrap();
        let mut fixture = unpartitioned(
            catalog.as_ref(),
            "kv_replaced",
            iceberg::spec::FormatVersion::V3,
        )
        .await;
        fixture
            .delete(&[IN_FIRST, IN_SECOND], DeleteForm::Vectors, "first")
            .await
            .unwrap();
        assert_eq!(
            fixture.metadata().format_version(),
            iceberg::spec::FormatVersion::V3
        );
        let first_puffin = fixture.deletes()[0].file_path().to_string();
        fixture
            .delete(&[NEIGHBOR], DeleteForm::Vectors, "second")
            .await
            .unwrap();
        let vectors: Vec<(&str, Option<String>)> = fixture
            .deletes()
            .iter()
            .map(|delete| (delete.file_path(), delete.referenced_data_file()))
            .collect();
        assert_eq!(vectors.len(), 2, "one vector per data file: {vectors:?}");
        assert_eq!(
            vectors
                .iter()
                .filter(|(path, _)| *path == first_puffin)
                .count(),
            1,
            "the first Puffin file keeps its other file's vector live"
        );
        let oracle = MergeOracle::read(&fixture).await.unwrap();
        let holder = oracle
            .live_copy(&fixture.key(NEIGHBOR + 1))
            .unwrap()
            .path
            .clone();
        assert_eq!(
            oracle.deleted_positions(&holder).len(),
            2,
            "the replacing vector holds the old row and the new one"
        );
        assert_eq!(oracle.deleted_rows(), 3);
        assert_agrees(&oracle, &fixture);
    }

    #[tokio::test]
    async fn test_read_follows_a_ref_moved_back_past_a_delete() {
        let (_dir, catalog) = local_catalog().await.unwrap();
        let mut fixture = unpartitioned(
            catalog.as_ref(),
            "kv_moved",
            iceberg::spec::FormatVersion::V2,
        )
        .await;
        let before = fixture.snapshot_id().unwrap();
        fixture
            .delete(&[IN_FIRST], DeleteForm::Vectors, "gone")
            .await
            .unwrap();
        let key = fixture.key(IN_FIRST);
        assert_eq!(
            MergeOracle::read(&fixture).await.unwrap().expected(&key),
            Value::Absent
        );
        fixture.move_ref(before).await.unwrap();
        let oracle = MergeOracle::read(&fixture).await.unwrap();
        assert_eq!(oracle.expected(&key), fixture.expected(&key));
        assert_ne!(oracle.expected(&key), Value::Absent);
        assert_agrees(&oracle, &fixture);
    }

    #[tokio::test]
    async fn test_read_refuses_two_live_copies_and_two_applying_vectors() {
        let (_dir, catalog) = local_catalog().await.unwrap();
        let mut fixture = unpartitioned(
            catalog.as_ref(),
            "kv_conflicts",
            iceberg::spec::FormatVersion::V2,
        )
        .await;
        let mut data = fixture.files().to_vec();
        data.extend(
            fixture
                .write_rows(&[IN_FIRST], FIRST_VERSION + 1, "again")
                .await
                .unwrap(),
        );
        fixture
            .commit_by_hand(HandSnapshot {
                data,
                deletes: Vec::new(),
            })
            .await
            .unwrap();
        assert!(matches!(
            MergeOracle::read(&fixture).await,
            Err(TestkitError::Oracle(OracleFault::LiveDuplicate { key, .. })) if key == fixture.key(IN_FIRST)
        ));

        let (_dir, catalog) = local_catalog().await.unwrap();
        let mut fixture = unpartitioned(
            catalog.as_ref(),
            "kv_two_vectors",
            iceberg::spec::FormatVersion::V2,
        )
        .await;
        fixture
            .delete(&[IN_FIRST], DeleteForm::Vectors, "one")
            .await
            .unwrap();
        let first = fixture.deletes().to_vec();
        fixture
            .delete(&[NEIGHBOR], DeleteForm::Vectors, "two")
            .await
            .unwrap();
        let mut both = fixture.deletes().to_vec();
        both.extend(first);
        fixture
            .commit_by_hand(HandSnapshot {
                data: fixture.files().to_vec(),
                deletes: both,
            })
            .await
            .unwrap();
        assert!(matches!(
            MergeOracle::read(&fixture).await,
            Err(TestkitError::Oracle(OracleFault::TwoVectors { .. }))
        ));
    }

    #[tokio::test]
    async fn test_read_applies_an_unscoped_position_delete_in_its_partition_alone() {
        let (_dir, catalog) = local_catalog().await.unwrap();
        let spec = FixtureSpec {
            ids: IDS_2K,
            buckets: TWO_BUCKETS,
            ..FixtureSpec::small("kv_partitions", KeyType::Long)
        };
        let mut fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec)
            .await
            .unwrap();
        let before = MergeOracle::read(&fixture).await.unwrap();
        let [own, other] = [0, 1].map(|at| fixture.files()[at].clone());
        assert_ne!(own.partition(), other.partition(), "one file per bucket");
        let first_copy = |file: &DataFile| {
            fixture
                .ids()
                .map(|id| before.copies(&fixture.key(id))[0].clone())
                .find(|copy| copy.path == file.file_path())
                .unwrap()
        };
        let (in_own, in_other) = (first_copy(&own), first_copy(&other));
        // One unscoped file in `own`'s partition naming a row of each file.
        let deletes = PositionDeletes {
            rows: vec![
                (in_own.path.clone(), in_own.position),
                (in_other.path.clone(), in_other.position),
            ],
            referenced: None,
            spec_id: fixture.metadata().default_partition_spec_id(),
            partition: own.partition().clone(),
        };
        let entry = write_position_deletes(
            fixture.file_io(),
            &fixture.delete_path("own.parquet"),
            &deletes,
        )
        .await
        .unwrap();
        fixture
            .commit_by_hand(HandSnapshot {
                data: fixture.files().to_vec(),
                deletes: vec![entry],
            })
            .await
            .unwrap();
        let oracle = MergeOracle::read(&fixture).await.unwrap();
        assert_eq!(
            oracle.deleted_positions(&in_own.path),
            BTreeSet::from([in_own.position])
        );
        assert!(
            oracle.deleted_positions(&in_other.path).is_empty(),
            "a row of another partition's file stays live"
        );
        assert_eq!(oracle.deleted_rows(), 1);
    }

    /// `crc32fast` computes the CRC-32 the Iceberg spec fixes for a
    /// deletion vector, ISO-HDLC, and not CRC-32C: its catalogue check
    /// value, the checksum of the nine ASCII digits.
    #[test]
    fn test_decode_vector_checksum_is_crc32_iso_hdlc() {
        let iso_hdlc_check_value = 0xCBF4_3926;
        assert_eq!(crc32fast::hash(b"123456789"), iso_hdlc_check_value);
    }
}
