//! Parquet position delete files (RFD 021): read whole through the storage
//! stack and decoded with parquet's own column reader on the blocking pool,
//! since the index's readers are built around one data file's key and
//! value leaves. Every page header is checked before parquet reads it, as
//! a value page's is (`index::check_chunk_page_headers`).

use std::collections::HashMap;

use bytes::Bytes;
use parquet::basic::Type as PhysicalType;
use parquet::column::reader::{ColumnReader, ColumnReaderImpl};
use parquet::data_type::{ByteArrayType, DataType, Int64Type};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::types::ColumnDescriptor;

use super::DeleteFault;
use crate::blocking;
use crate::error::{Error, Result};
use crate::index::{self, DecodedPart};
use crate::manifest::DeleteFileEntry;
use crate::storage::Storage;

/// The column of a position delete file naming the data file of a deleted
/// row, as the Iceberg spec names it.
pub const FILE_PATH_COLUMN: &str = "file_path";

/// The column of a position delete file naming a deleted row's position in
/// its data file, counted from the file's first row, as the Iceberg spec
/// names it.
pub const POS_COLUMN: &str = "pos";

/// The field id the Iceberg spec reserves for `FILE_PATH_COLUMN`.
pub const FILE_PATH_FIELD_ID: i32 = 2_147_483_546;

/// The field id the Iceberg spec reserves for `POS_COLUMN`.
pub const POS_FIELD_ID: i32 = 2_147_483_545;

/// The most records one `read_records` call asks for, so the buffers one
/// call grows stay small whatever a footer claims.
const READ_BATCH_RECORDS: usize = 8 * 1024;

/// The deepest definition level a position column may have: a required
/// column has none, and an optional one, which holds no null, one.
const MAX_DEF_LEVEL: i16 = 1;

/// What one position delete file deletes: per data file location a row
/// names, the positions it names there, as written.
pub(super) type Positions = HashMap<String, Vec<i64>>;

/// The position delete file `delete` names, read with one GET and decoded
/// on the blocking pool. A file without a `file_path` byte array and a
/// `pos` 64-bit integer column, matched by name and physical type and,
/// where the file records field ids, by Iceberg's reserved ids, neither
/// repeated, or whose two columns differ in length, is
/// `DeleteFault::PositionSchema`; a null in either column, which a column
/// declared optional may hold, is `DeleteFault::PositionNull`. When the
/// entry names one referenced data file, a row naming another is
/// `DeleteFault::ReferencedFileMismatch`; rows other than the entry's
/// record count are `DeleteFault::RecordsDecoded`. A page header that
/// overstates its page is refused before parquet reads it, a decode error
/// is `Error::Parquet`, and a panic inside parquet
/// `FileFault::DecoderPanicked`.
pub(super) async fn read(storage: &Storage, delete: &DeleteFileEntry) -> Result<Positions> {
    let bytes = storage.read_all(&delete.path).await?;
    let path = delete.path.clone();
    let referenced = delete.referenced.clone();
    let records = delete.records;
    blocking::on_blocking_pool(move || {
        index::unless_panicked(
            &path,
            // Everything the decode builds is dropped with a panic.
            std::panic::AssertUnwindSafe(|| decode(&path, &bytes, referenced.as_deref(), records)),
            || DecodedPart::PositionDeletes,
        )?
    })
    .await
    .map_err(|canceled| Error::verify_canceled(&delete.path, canceled))?
}

/// `read`'s decode of the file's bytes.
fn decode(path: &str, bytes: &Bytes, referenced: Option<&str>, records: u64) -> Result<Positions> {
    let fault = |fault| super::fault(path, fault);
    let reader = SerializedFileReader::new(bytes.clone())?;
    let metadata = reader.metadata();
    let schema = metadata.file_metadata().schema_descr_ptr();
    let column = |name: &str, physical: PhysicalType, id: i32| {
        schema.columns().iter().position(|column| {
            column.name() == name
                && column.physical_type() == physical
                && column.max_def_level() <= MAX_DEF_LEVEL
                && column.max_rep_level() == 0
                && field_id_matches(column, id)
        })
    };
    let file_paths = column(
        FILE_PATH_COLUMN,
        PhysicalType::BYTE_ARRAY,
        FILE_PATH_FIELD_ID,
    )
    .ok_or_else(|| fault(DeleteFault::PositionSchema))?;
    let positions = column(POS_COLUMN, PhysicalType::INT64, POS_FIELD_ID)
        .ok_or_else(|| fault(DeleteFault::PositionSchema))?;
    let mut held = Positions::new();
    let mut decoded: u64 = 0;
    for row_group in 0..reader.num_row_groups() {
        for leaf in [file_paths, positions] {
            index::check_chunk_page_headers(path, bytes, metadata, row_group, leaf)?;
        }
        let group = reader.get_row_group(row_group)?;
        let ColumnReader::ByteArrayColumnReader(mut path_column) =
            group.get_column_reader(file_paths)?
        else {
            return Err(fault(DeleteFault::PositionSchema));
        };
        let ColumnReader::Int64ColumnReader(mut pos_column) = group.get_column_reader(positions)?
        else {
            return Err(fault(DeleteFault::PositionSchema));
        };
        let mut path_batch = Batch::<ByteArrayType>::default();
        let mut pos_batch = Batch::<Int64Type>::default();
        loop {
            let paths_read = path_batch
                .read(&mut path_column)
                .map_err(|null| match null {
                    Unread::Null => fault(DeleteFault::PositionNull),
                    Unread::Parquet(error) => error.into(),
                })?;
            let positions_read = pos_batch.read(&mut pos_column).map_err(|null| match null {
                Unread::Null => fault(DeleteFault::PositionNull),
                Unread::Parquet(error) => error.into(),
            })?;
            if paths_read != positions_read {
                return Err(fault(DeleteFault::PositionSchema));
            }
            if paths_read == 0 {
                break;
            }
            decoded = decoded.saturating_add(crate::approx::count(paths_read));
            for (name, &position) in path_batch.values.iter().zip(&pos_batch.values) {
                let name = name
                    .as_utf8()
                    .map_err(|_| fault(DeleteFault::PositionSchema))?;
                if let Some(referenced) = referenced
                    && name != referenced
                {
                    return Err(fault(DeleteFault::ReferencedFileMismatch {
                        referenced: referenced.to_string(),
                        found: Some(name.to_string()),
                    }));
                }
                match held.get_mut(name) {
                    Some(named) => named.push(position),
                    None => {
                        held.insert(name.to_string(), vec![position]);
                    }
                }
            }
        }
    }
    if decoded != records {
        return Err(fault(DeleteFault::RecordsDecoded {
            recorded: records,
            decoded,
        }));
    }
    Ok(held)
}

/// One column's buffers for a `read_records` call.
struct Batch<T: DataType> {
    levels: Vec<i16>,
    values: Vec<T::T>,
}

impl<T: DataType> Default for Batch<T> {
    fn default() -> Self {
        Self {
            levels: Vec::new(),
            values: Vec::new(),
        }
    }
}

/// Why `Batch::read` read no batch.
enum Unread {
    /// A record of the batch is a null.
    Null,
    /// Parquet could not read it.
    Parquet(parquet::errors::ParquetError),
}

impl<T: DataType> Batch<T> {
    /// The next `READ_BATCH_RECORDS` records of `column` at most, in
    /// `values`, and how many; a null among them, which leaves fewer
    /// values than records, is `Unread::Null`.
    fn read(&mut self, column: &mut ColumnReaderImpl<T>) -> std::result::Result<usize, Unread> {
        self.levels.clear();
        self.values.clear();
        let (records, values, _) = column
            .read_records(
                READ_BATCH_RECORDS,
                Some(&mut self.levels),
                None,
                &mut self.values,
            )
            .map_err(Unread::Parquet)?;
        if values == records {
            Ok(records)
        } else {
            Err(Unread::Null)
        }
    }
}

/// Whether `column` carries no field id, or `id`.
fn field_id_matches(column: &ColumnDescriptor, id: i32) -> bool {
    let info = column.self_type().get_basic_info();
    !info.has_id() || info.id() == id
}

#[cfg(test)]
pub(super) mod testing {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};

    use super::{FILE_PATH_COLUMN, FILE_PATH_FIELD_ID, POS_COLUMN, POS_FIELD_ID};

    /// How a test's position delete file declares its columns.
    #[derive(Clone, Copy)]
    pub(in crate::deletes) struct Columns<'a> {
        /// The two columns' names.
        pub(in crate::deletes) names: (&'a str, &'a str),
        /// Whether both are declared optional.
        pub(in crate::deletes) optional: bool,
    }

    impl Columns<'_> {
        /// The spec's names, both columns required.
        pub(in crate::deletes) const SPEC: Self = Self {
            names: (FILE_PATH_COLUMN, POS_COLUMN),
            optional: false,
        };
    }

    /// A Parquet position delete file of `rows`, in the order given, its
    /// columns declared as `columns` with Iceberg's reserved field ids; a
    /// `None` is a null.
    pub(in crate::deletes) fn position_file_of(
        rows: &[(Option<&str>, Option<i64>)],
        columns: Columns<'_>,
    ) -> Vec<u8> {
        let field = |name: &str, data_type, id: i32| {
            Field::new(name, data_type, columns.optional).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                id.to_string(),
            )]))
        };
        let schema = Arc::new(Schema::new(vec![
            field(columns.names.0, DataType::Utf8, FILE_PATH_FIELD_ID),
            field(columns.names.1, DataType::Int64, POS_FIELD_ID),
        ]));
        let paths: StringArray = rows.iter().map(|(path, _)| *path).collect();
        let positions: Int64Array = rows.iter().map(|&(_, at)| at).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(paths) as ArrayRef, Arc::new(positions) as ArrayRef],
        )
        .unwrap();
        let mut out = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut out, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        out
    }

    /// `position_file_of` under the spec's columns, no row null.
    pub(in crate::deletes) fn position_file(rows: &[(&str, i64)]) -> Vec<u8> {
        let rows: Vec<(Option<&str>, Option<i64>)> = rows
            .iter()
            .map(|&(path, at)| (Some(path), Some(at)))
            .collect();
        position_file_of(&rows, Columns::SPEC)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::testing::{Columns, position_file, position_file_of};
    use super::*;
    use crate::config::StorageConfig;
    use crate::index::testing::{with_dictionary_one_entry_short, with_first_data_page_claiming};
    use crate::index::{FileFault, HeaderField};
    use crate::manifest::{DeleteKind, Partition};
    use crate::snapshot::SequenceNumber;

    /// The position delete file `bytes` in `dir`, as a manifest entry naming
    /// `referenced` that records `records` rows.
    fn stored(
        dir: &tempfile::TempDir,
        bytes: &[u8],
        referenced: Option<&str>,
        records: u64,
    ) -> DeleteFileEntry {
        std::fs::write(dir.path().join("d.parquet"), bytes).unwrap();
        DeleteFileEntry {
            path: format!("file://{}/d.parquet", dir.path().display()),
            records,
            size: crate::approx::count(bytes.len()),
            sequence: SequenceNumber(1),
            referenced: referenced.map(str::to_string),
            partition: Partition::unpartitioned(),
            kind: DeleteKind::Positions,
        }
    }

    /// The delete fault reading `bytes` as a file of `records` rows ends in.
    async fn fault_of(bytes: &[u8], referenced: Option<&str>, records: u64) -> FileFault {
        let dir = tempfile::tempdir().unwrap();
        let entry = stored(&dir, bytes, referenced, records);
        let storage = Storage::new(StorageConfig::default());
        match read(&storage, &entry).await {
            Err(Error::File { fault, .. }) => fault,
            other => panic!("expected a file fault, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_read_keeps_every_named_files_positions_in_one_get() {
        let dir = tempfile::tempdir().unwrap();
        let rows = [("a", 1), ("a", 4), ("b", 2), ("c", 0)];
        let entry = stored(&dir, &position_file(&rows), None, 4);
        let storage = Storage::new(StorageConfig::default());
        let read = read(&storage, &entry).await.unwrap();
        assert_eq!(storage.stats().gets(), 1, "the whole file in one GET");
        assert_eq!(read.len(), 3);
        assert_eq!(read["a"], [1, 4]);
        assert_eq!(read["b"], [2]);
        assert_eq!(read["c"], [0]);
    }

    #[tokio::test]
    async fn test_read_takes_optional_columns_without_nulls_and_refuses_a_null() {
        let optional = Columns {
            optional: true,
            ..Columns::SPEC
        };
        let dir = tempfile::tempdir().unwrap();
        let rows = [(Some("a"), Some(1)), (Some("a"), Some(3))];
        let entry = stored(&dir, &position_file_of(&rows, optional), None, 2);
        let storage = Storage::new(StorageConfig::default());
        assert_eq!(read(&storage, &entry).await.unwrap()["a"], [1, 3]);
        for nulled in [[(None, Some(1))], [(Some("a"), None)]] {
            assert_eq!(
                fault_of(&position_file_of(&nulled, optional), None, 1).await,
                FileFault::Delete(DeleteFault::PositionNull)
            );
        }
    }

    #[tokio::test]
    async fn test_read_refuses_a_row_naming_another_file_than_the_referenced_one() {
        let fault = fault_of(&position_file(&[("a", 1), ("b", 2)]), Some("a"), 2).await;
        assert_eq!(
            fault,
            FileFault::Delete(DeleteFault::ReferencedFileMismatch {
                referenced: "a".to_string(),
                found: Some("b".to_string()),
            })
        );
    }

    #[tokio::test]
    async fn test_read_refuses_rows_other_than_the_entry_records() {
        let fault = fault_of(&position_file(&[("a", 1), ("a", 2)]), None, 3).await;
        assert_eq!(
            fault,
            FileFault::Delete(DeleteFault::RecordsDecoded {
                recorded: 3,
                decoded: 2,
            })
        );
    }

    #[tokio::test]
    async fn test_read_refuses_a_file_without_the_position_columns() {
        let rows = [(Some("a"), Some(1))];
        let renamed = Columns {
            names: ("path", POS_COLUMN),
            ..Columns::SPEC
        };
        assert_eq!(
            fault_of(&position_file_of(&rows, renamed), None, 1).await,
            FileFault::Delete(DeleteFault::PositionSchema)
        );
    }

    /// The `pos` column's one data page claims a value fewer than it
    /// holds: parquet reads one position for the two paths, and the two
    /// columns differ in length.
    #[tokio::test]
    async fn test_read_refuses_columns_of_different_lengths() {
        let file = Bytes::from(position_file(&[("a", 1), ("a", 2)]));
        let pos_leaf = 1;
        let short = with_first_data_page_claiming(&file, pos_leaf, 1);
        assert_eq!(
            fault_of(&short, None, 2).await,
            FileFault::Delete(DeleteFault::PositionSchema)
        );
    }

    /// The `file_path` dictionary claims an entry more than its body holds,
    /// which no header bound sees: parquet panics decoding it, and the read
    /// ends in the panic's fault rather than the task.
    #[tokio::test]
    async fn test_read_turns_a_parquet_panic_into_a_file_fault() {
        let rows = [("a", 1), ("a", 2), ("b", 3)];
        let file = Bytes::from(position_file(&rows));
        let file_path_leaf = 0;
        let (short, _) = with_dictionary_one_entry_short(&file, file_path_leaf);
        let fault = fault_of(&short, None, 3).await;
        assert!(
            matches!(
                fault,
                FileFault::DecoderPanicked {
                    part: DecodedPart::PositionDeletes,
                    ..
                }
            ),
            "{fault:?}"
        );
    }

    /// A `file_path` dictionary claiming more entries than its row group
    /// has rows is refused before parquet reads it, where it would panic.
    #[tokio::test]
    async fn test_read_checks_every_page_header_before_parquet_reads_it() {
        let file = Bytes::from(position_file(&[("a", 1)]));
        let file_path_leaf = 0;
        let (claiming, _) = with_dictionary_one_entry_short(&file, file_path_leaf);
        let fault = fault_of(&claiming, None, 1).await;
        assert!(
            matches!(
                fault,
                FileFault::PageHeaderOverBound {
                    field: HeaderField::NumValues,
                    declared: 2,
                    allowed: 1,
                    ..
                }
            ),
            "{fault:?}"
        );
    }
}
