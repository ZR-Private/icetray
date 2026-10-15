//! Merge-on-read deletes (RFD 021): which deletion vectors and Parquet
//! position deletes apply to each data file of a generation, decided from
//! the snapshot's manifests every generation, and the rows they delete,
//! read through the storage stack (RFD 003) and held per data file as one
//! applied bitmap with the set of deletes it came from.
//!
//! A delete applies to the data file it names when the file's data
//! sequence number is no greater than its own (`applies`); a position
//! delete file that names no referenced data file applies only within its
//! own partition. A deletion vector masks the position deletes of its data
//! file, and two that apply to one file are a fault. A file's applied
//! bitmap is reused as it is when its sources are unchanged, extended with
//! the new sources' rows when they grew, and rebuilt from its remaining
//! sources when one dropped away, as a ref moved backward drops it; a
//! source's rows are read again only then.
//!
//! What stays resident after a load beside the bitmaps is kept small
//! (`Known`): per position delete file without a referenced data file, the
//! data files it names, by index, so it is not read again while every data
//! file it could apply to was there when it was read; and of a Puffin file
//! that still lists a vector no data file takes, the blobs of those vectors
//! as their footer passed them, so a later load reading one needs no footer
//! GET. A Puffin footer whose listed blobs are all applied is dropped.

mod bound;
mod position;
mod puffin;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::mem::size_of;
use std::ops::Range;
use std::sync::Arc;

use futures::stream::{self, StreamExt as _, TryStreamExt as _};
use iceberg::puffin::CompressionCodec;
use roaring::RoaringTreemap;

pub(crate) use bound::bound;
#[cfg(test)]
pub(crate) use position::testing::position_file;
pub use position::{FILE_PATH_COLUMN, FILE_PATH_FIELD_ID, POS_COLUMN, POS_FIELD_ID};
pub use puffin::{CARDINALITY_PROPERTY, REFERENCED_DATA_FILE_PROPERTY};

use crate::approx;
use crate::blocking;
use crate::error::{Error, Recurrence, Recurs, Result};
use crate::generation::load_concurrency;
use crate::index::{DeletedRows, FileFault, RowPosition};
use crate::manifest::{DataFileEntry, DeleteFileEntry, DeleteKind, Partition};
use crate::snapshot::SequenceNumber;
use crate::storage::Storage;

/// What makes a delete file, or a data file's deletes, unservable (RFD
/// 021); closed. Each is raised as `FileFault::Delete` on the file it
/// names: the position delete file, the Puffin file, or the data file.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum DeleteFault {
    /// A Puffin file's tail is not a Puffin footer: a wrong magic, a
    /// payload length that overruns the file, or a payload that is not the
    /// footer's JSON.
    #[error("the puffin footer does not read")]
    FooterFraming,
    /// A deletion vector blob's framing is wrong.
    #[error("the deletion vector blob's {0} is wrong")]
    BlobFraming(BlobFraming),
    /// No blob of the Puffin footer is at the offset and length the
    /// manifest entry names.
    #[error("the puffin footer has no blob at offset {offset} of length {length}")]
    VectorUnlisted {
        /// The offset the entry names.
        offset: u64,
        /// The length the entry names.
        length: u64,
    },
    /// The footer places the blob a manifest entry names before the end of
    /// the file's opening magic or past the footer's start, where no blob
    /// lies.
    #[error(
        "the blob at offset {offset} of length {length} lies outside the blobs, which end at {blobs_end}"
    )]
    BlobOutOfRange {
        /// The blob's offset.
        offset: u64,
        /// The blob's length.
        length: u64,
        /// Where the file's blobs end: its footer's start.
        blobs_end: u64,
    },
    /// Two manifest entries of one Puffin file record different file sizes,
    /// so its footer cannot be placed.
    #[error("manifest entries record the puffin file at {recorded} bytes and at {other}")]
    PuffinSizeMismatch {
        /// The size the first entry records.
        recorded: u64,
        /// Another entry's.
        other: u64,
    },
    /// A deletion vector blob is compressed, which the spec forbids.
    #[error("the deletion vector blob is compressed with {}", codec.name())]
    BlobCompressed {
        /// The codec the footer names.
        codec: CompressionCodec,
    },
    /// A delete names another data file than its manifest entry: a vector
    /// whose `referenced-data-file` property, or a position delete row of a
    /// file whose entry names one data file, names another.
    #[error("the delete names data file {found:?}, but its manifest entry names {referenced}")]
    ReferencedFileMismatch {
        /// The data file the manifest entry names.
        referenced: String,
        /// What the delete names instead; `None` for no property.
        found: Option<String>,
    },
    /// A deletion vector's `cardinality` property differs from the rows its
    /// manifest entry records, or is missing or not a count.
    #[error(
        "the deletion vector's cardinality property is {found:?}, but its manifest entry records {recorded} rows"
    )]
    CardinalityProperty {
        /// The rows the manifest entry records.
        recorded: u64,
        /// The property as written; `None` when missing.
        found: Option<String>,
    },
    /// A deletion vector or a position delete file decodes to another
    /// number of rows than its manifest entry records.
    #[error("the delete decodes to {decoded} rows, but its manifest entry records {recorded}")]
    RecordsDecoded {
        /// The rows the manifest entry records.
        recorded: u64,
        /// The rows the delete holds.
        decoded: u64,
    },
    /// A deletion vector passes its checksum but is not a portable 64-bit
    /// roaring bitmap that fills its blob.
    #[error("the deletion vector does not decode as a portable roaring bitmap")]
    VectorMalformed,
    /// A delete names a position at or past its data file's row count,
    /// which the spec does not forbid but icetray refuses: a writer that
    /// wrote it has misread the file.
    #[error("the delete removes position {position} of {data_file}, which has {records} rows")]
    PositionPastEnd {
        /// The data file.
        data_file: String,
        /// The position.
        position: u64,
        /// The data file's row count, from its manifest entry.
        records: u64,
    },
    /// A position delete row names a negative position.
    #[error("the delete removes negative position {position} of {data_file}")]
    NegativePosition {
        /// The data file.
        data_file: String,
        /// The position.
        position: i64,
    },
    /// Two deletion vectors apply to one data file, which the spec allows
    /// once.
    #[error("the deletion vector and one in {other} both apply to {data_file}")]
    TwoVectors {
        /// The data file.
        data_file: String,
        /// The other vector's Puffin file.
        other: String,
    },
    /// A position delete file lacks a `file_path` string and a `pos` long
    /// column, neither repeated, or its columns differ in length.
    #[error("the position delete file has no file_path string and pos long columns of one length")]
    PositionSchema,
    /// A position delete file holds a null `file_path` or `pos`, which a
    /// column declared optional may hold and the spec does not.
    #[error("the position delete file holds a null file_path or pos")]
    PositionNull,
    /// A data file with deletes that apply records another row count in
    /// its manifest entry than its footer gives, so positions cannot be
    /// checked against it.
    #[error("the data file's manifest entry records {manifest} rows, its footer {footer}")]
    RecordCountMismatch {
        /// The rows the manifest entry records.
        manifest: u64,
        /// The rows the footer gives.
        footer: i64,
    },
}

/// Which part of a deletion vector blob's framing is wrong.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlobFraming {
    /// The length prefix does not match the blob's length.
    Length,
    /// The bytes after the length are not the vector magic.
    Magic,
    /// The CRC-32 does not match the magic and the vector.
    Checksum,
}

impl std::fmt::Display for BlobFraming {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Length => "length",
            Self::Magic => "magic",
            Self::Checksum => "checksum",
        })
    }
}

/// A delete's framing is read before anything it says is judged: a read
/// damaged in flight cannot be told from a damaged file, so wrong framing
/// is transient (RFD 005). What a delete whose framing holds says against
/// its manifests is fixed for the snapshot.
impl Recurs for DeleteFault {
    fn recurrence(&self) -> Recurrence {
        match self {
            Self::FooterFraming | Self::BlobFraming(_) => Recurrence::Transient,
            Self::VectorUnlisted { .. }
            | Self::BlobOutOfRange { .. }
            | Self::PuffinSizeMismatch { .. }
            | Self::BlobCompressed { .. }
            | Self::ReferencedFileMismatch { .. }
            | Self::CardinalityProperty { .. }
            | Self::RecordsDecoded { .. }
            | Self::VectorMalformed
            | Self::PositionPastEnd { .. }
            | Self::NegativePosition { .. }
            | Self::TwoVectors { .. }
            | Self::PositionSchema
            | Self::PositionNull
            | Self::RecordCountMismatch { .. } => Recurrence::Deterministic,
        }
    }
}

/// `Error::File` for `fault` of the file at `path`.
fn fault(path: &str, fault: DeleteFault) -> Error {
    Error::File {
        path: path.to_string(),
        fault: FileFault::Delete(fault),
    }
}

/// The path and delete fault `result` ended in; for tests of this module
/// and its children.
#[cfg(test)]
fn delete_fault_of<T: std::fmt::Debug>(result: Result<T>) -> (String, DeleteFault) {
    match result {
        Err(Error::File {
            path,
            fault: FileFault::Delete(fault),
        }) => (path, fault),
        other => panic!("expected a delete fault, got {other:?}"),
    }
}

/// The bytes the allocator hands an `Arc<str>` of `len` bytes: the two
/// reference counts and the bytes, rounded up to the counts' alignment.
const fn arc_str_bytes(len: usize) -> u64 {
    approx::count((2 * size_of::<usize>() + len).next_multiple_of(size_of::<usize>()))
}

/// The bytes the allocator hands a `Box<str>` of `len` bytes; none for an
/// empty one.
const fn boxed_str_bytes(len: usize) -> u64 {
    approx::count(len)
}

/// Whether a delete at `delete`'s data sequence number applies to a data
/// file at `sequence`: the spec's rule for deletion vectors and position
/// deletes alike, which lets a delete committed with its data file apply
/// to it.
fn applies(delete: &DeleteFileEntry, sequence: SequenceNumber) -> bool {
    delete.sequence >= sequence
}

/// One delete a data file's applied rows came from, with the rows its
/// manifest entry records, so an entry that records others is checked
/// again: a position delete file by its path, or a deletion vector by its
/// Puffin file, offset, and length.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum DeleteSource {
    /// A Parquet position delete file.
    Positions {
        /// The file.
        path: Arc<str>,
        /// The rows its entry records.
        records: u64,
    },
    /// A deletion vector.
    Vector {
        /// The Puffin file.
        path: Arc<str>,
        /// The blob's offset.
        offset: u64,
        /// The blob's length.
        length: u64,
        /// The rows its entry records.
        records: u64,
    },
}

/// The bytes one source takes in a `FileDeletes`'s slice, beside its path.
const SOURCE_SLOT_BYTES: u64 = approx::count(size_of::<DeleteSource>());

impl DeleteSource {
    /// The source of `entry`, whose path is `path`, interned for the load.
    fn of(entry: &DeleteFileEntry, path: &Arc<str>) -> Self {
        let path = Arc::clone(path);
        let records = entry.records;
        match entry.kind {
            DeleteKind::Positions => Self::Positions { path, records },
            DeleteKind::Vector { offset, length } => Self::Vector {
                path,
                offset,
                length,
                records,
            },
        }
    }

    /// The delete file's path.
    fn path(&self) -> &Arc<str> {
        match self {
            Self::Positions { path, .. } | Self::Vector { path, .. } => path,
        }
    }

    /// The bytes this source takes in a `FileDeletes`: its slot, and its
    /// path, counted whole although a load shares it.
    fn resident_bytes(&self) -> u64 {
        SOURCE_SLOT_BYTES + arc_str_bytes(self.path().len())
    }
}

/// One data file's deleted rows as a generation applies them, and the
/// deletes they came from, sorted.
#[derive(Debug)]
pub(crate) struct FileDeletes {
    rows: RoaringTreemap,
    sources: Box<[DeleteSource]>,
}

impl FileDeletes {
    /// The applied state of `rows` from `sources`, sorted: the rows
    /// compacted (`compact`), as a load builds it and `bound` counts it.
    fn built(rows: RoaringTreemap, sources: impl IntoIterator<Item = DeleteSource>) -> Arc<Self> {
        Arc::new(Self {
            rows: compact(rows),
            sources: sources.into_iter().collect(),
        })
    }

    /// Whether the row at `position` is deleted.
    pub(crate) fn contains(&self, position: RowPosition) -> bool {
        self.rows.contains(position.0)
    }

    /// Whether every row of `positions` is deleted; true for none.
    pub(crate) fn contains_all(&self, positions: Range<RowPosition>) -> bool {
        positions.is_empty() || self.rows.contains_range(positions.start.0..positions.end.0)
    }

    /// The bytes these rows and sources hold, as `bound` counts them.
    pub(crate) fn resident_bytes(&self) -> u64 {
        self.sources
            .iter()
            .map(DeleteSource::resident_bytes)
            .fold(bound::held(&self.rows), u64::saturating_add)
    }

    /// Whether these rows hold every row `before` deleted: false when a
    /// delete dropped away and rows came back to life, which a ref moved
    /// backward does (RFD 021, Refresh). An empty bitmap of either holds
    /// no row and is passed over.
    fn holds(&self, before: &Self) -> bool {
        let mut now = self
            .rows
            .bitmaps()
            .filter(|(_, bitmap)| !bitmap.is_empty())
            .peekable();
        before
            .rows
            .bitmaps()
            .filter(|(_, bitmap)| !bitmap.is_empty())
            .all(|(high, held)| {
                while now.next_if(|&(at, _)| at < high).is_some() {}
                now.peek()
                    .is_some_and(|&(at, bitmap)| at == high && held.is_subset(bitmap))
            })
    }
}

/// A data file's deleted rows as a read counts them: none for a file no
/// delete applies to.
impl DeletedRows for Option<&FileDeletes> {
    fn deleted(&self, position: RowPosition) -> bool {
        self.is_some_and(|rows| rows.contains(position))
    }

    fn all_deleted(&self, positions: Range<RowPosition>) -> bool {
        match self {
            Some(rows) => rows.contains_all(positions),
            None => positions.is_empty(),
        }
    }
}

/// What a generation's delete state comes to (RFD 021, Measurements):
/// what the delete gauges show while it serves and the doctor's summary
/// reports. Every count is of deletes that apply to a data file the
/// generation holds, so the dangling deletes a compaction leaves behind
/// count in none of them.
#[derive(Clone, Copy, Debug, Default, PartialEq, serde::Serialize)]
pub struct DeleteSummary {
    /// Rows the deletes remove, each once however many deletes name it.
    pub rows: u64,
    /// Live position delete files and deletion vectors whose rows are in a
    /// data file's applied rows; a position delete file masked by a
    /// deletion vector is not.
    pub files: u64,
    /// `rows` over the rows of the snapshot's data files; 0 for a snapshot
    /// with none.
    pub fraction: f64,
    /// Bytes the delete state holds: every data file's applied rows and the
    /// deletes they came from, and what the deletes keep for the next load;
    /// counted in the generation's `index_bytes` too.
    pub state_bytes: usize,
}

impl DeleteSummary {
    /// The summary of the applied rows `rows` of a generation of `records`
    /// data rows, beside `known`, what its deletes keep for the next load.
    pub(crate) fn of(rows: &[Option<Arc<FileDeletes>>], known: &Known, records: u64) -> Self {
        let applied: Vec<&FileDeletes> = rows.iter().flatten().map(Arc::as_ref).collect();
        let deleted = applied
            .iter()
            .map(|file| file.rows.len())
            .fold(0, u64::saturating_add);
        let sources: HashSet<&DeleteSource> = applied
            .iter()
            .flat_map(|file| file.sources.iter())
            .collect();
        let bytes = applied
            .iter()
            .map(|file| file.resident_bytes())
            .fold(known.resident_bytes(), u64::saturating_add);
        Self {
            rows: deleted,
            files: approx::count(sources.len()),
            fraction: approx::ratio(deleted, records),
            state_bytes: approx::length(bytes),
        }
    }
}

#[cfg(test)]
impl FileDeletes {
    /// `rows` deleted by no source, for a test that assembles a generation
    /// by hand.
    pub(crate) fn of_rows(rows: impl IntoIterator<Item = u64>) -> Self {
        Self {
            rows: rows.into_iter().collect(),
            sources: Box::new([]),
        }
    }
}

/// `rows` optimized, stripped of empty bitmaps, and copied through
/// roaring's own serialization, so every container is the cheaper of
/// array, bitset, and runs, every vector is allocated to its length, and
/// every bitmap holds a row, as `bound` counts them.
fn compact(mut rows: RoaringTreemap) -> RoaringTreemap {
    rows.optimize();
    if rows.bitmaps().any(|(_, bitmap)| bitmap.is_empty()) {
        rows = RoaringTreemap::from_bitmaps(
            rows.bitmaps()
                .filter(|(_, bitmap)| !bitmap.is_empty())
                .map(|(high, bitmap)| (high, bitmap.clone())),
        );
    }
    let mut bytes = Vec::with_capacity(rows.serialized_size());
    rows.serialize_into(&mut bytes)
        .expect("serializing into memory cannot fail");
    RoaringTreemap::deserialize_unchecked_from(bytes.as_slice())
        .expect("roaring reads back what it wrote")
}

/// What a load of deletes need not read again (the module's `Known`): per
/// position delete file without a referenced data file, by path, sorted,
/// the data files it names, by index; and per Puffin file that lists a
/// vector no data file took, by path, sorted, those vectors' blobs as its
/// footer passed them. The indices are into a generation's files where a
/// generation holds this, and into a load's data targets where a load
/// starts from it or returns it (`reindexed`).
#[derive(Clone, Debug, Default)]
pub(crate) struct Known {
    named: Box<[Named]>,
    footers: Box<[KeptFooter]>,
}

/// One position delete file `Known` holds: its path and the data files it
/// names, by index.
type Named = (Arc<str>, Box<[u32]>);

/// One Puffin file `Known` holds: its path and its kept blobs.
type KeptFooter = (Arc<str>, Arc<puffin::Retained>);

/// Per data target, the rows its deletes remove; `None` where none applies.
type TargetRows = Vec<Option<Arc<FileDeletes>>>;

/// The bytes one `Known::named` entry takes in its slice, beside its path
/// and indices.
const NAMED_ENTRY_BYTES: u64 = approx::count(size_of::<Named>());

/// The bytes one index of `Known::named` takes.
const NAMED_INDEX_BYTES: u64 = approx::count(size_of::<u32>());

/// The bytes one `Known::footers` entry takes in its slice and in its
/// shared `Retained`, beside the blobs that holds.
const FOOTER_ENTRY_BYTES: u64 =
    approx::count(size_of::<KeptFooter>() + 2 * size_of::<usize>() + size_of::<puffin::Retained>());

impl Known {
    /// The data files the position delete file at `path` names, when it
    /// was read.
    fn named(&self, path: &str) -> Option<&[u32]> {
        self.named
            .binary_search_by(|(held, _)| (**held).cmp(path))
            .ok()
            .map(|at| &*self.named[at].1)
    }

    /// The kept blobs of the Puffin file at `path`.
    fn footer(&self, path: &str) -> Option<&Arc<puffin::Retained>> {
        self.footers
            .binary_search_by(|(held, _)| (**held).cmp(path))
            .ok()
            .map(|at| &self.footers[at].1)
    }

    /// This with every index `at` of `named` moved to `to[at]`, and dropped
    /// where that is `None` or out of `to`.
    pub(crate) fn reindexed(&self, to: &[Option<u32>]) -> Self {
        let move_to = |at: &u32| to.get(approx::length(*at)).copied().flatten();
        Self {
            named: self
                .named
                .iter()
                .map(|(path, named)| {
                    let mut moved: Vec<u32> = named.iter().filter_map(move_to).collect();
                    moved.sort_unstable();
                    (Arc::clone(path), moved.into_boxed_slice())
                })
                .collect(),
            footers: self.footers.clone(),
        }
    }

    /// The bytes this holds: every entry, path, index, and kept blob.
    pub(crate) fn resident_bytes(&self) -> u64 {
        let named = self.named.iter().map(|(path, named)| {
            NAMED_ENTRY_BYTES
                + arc_str_bytes(path.len())
                + NAMED_INDEX_BYTES * approx::count(named.len())
        });
        let footers = self.footers.iter().map(|(path, retained)| {
            FOOTER_ENTRY_BYTES + arc_str_bytes(path.len()) + retained.resident_bytes()
        });
        named.chain(footers).fold(0, u64::saturating_add)
    }
}

/// What a load starts from: the previous generation's applied rows of the
/// data files it reuses, and what earlier loads read.
#[derive(Debug, Default)]
pub(crate) struct Previous {
    /// Per data file the load reuses, in the order its data targets list
    /// them first, the rows its deletes removed there.
    pub(crate) applied: TargetRows,
    /// What earlier loads read, `named` by index into those targets.
    pub(crate) known: Known,
}

/// A load's applied deletes, each by index into its data targets.
#[derive(Debug, Default)]
pub(crate) struct Applied {
    /// Per data target, the rows its deletes remove; `None` where none
    /// applies.
    pub(crate) rows: TargetRows,
    /// Per data target, whether its applied rows no longer hold every row
    /// the previous generation's did, so its overlap groups are verified
    /// again.
    pub(crate) shrank: Vec<bool>,
    /// What the next load need not read again, `named` by data target.
    pub(crate) known: Known,
    /// The least data sequence number of the deletes in any target's
    /// applied rows; `None` where no delete applies.
    pub(crate) oldest: Option<SequenceNumber>,
}

/// One data file of a snapshot as the deletes see it.
#[derive(Clone, Debug)]
pub(crate) struct DataTarget {
    /// The file's location.
    path: String,
    /// Its data sequence number.
    sequence: SequenceNumber,
    /// Its rows, from the manifest.
    records: u64,
    /// Its partition.
    partition: Partition,
}

impl DataTarget {
    /// The data file `entry` names.
    pub(crate) fn of(entry: &DataFileEntry) -> Self {
        Self {
            path: entry.path.clone(),
            sequence: entry.sequence,
            records: entry.records,
            partition: entry.partition.clone(),
        }
    }
}

/// A snapshot's readable deletes by what they apply to, as indices into
/// its delete entries: deletion vectors and position delete files that
/// name a data file, by that file, position delete files that name none,
/// and every vector by its Puffin file.
pub(crate) struct Sorted<'a> {
    deletes: &'a [DeleteFileEntry],
    vectors: HashMap<&'a str, Vec<u32>>,
    scoped: HashMap<&'a str, Vec<u32>>,
    unscoped: Vec<u32>,
    puffins: BTreeMap<&'a str, Vec<u32>>,
}

impl<'a> Sorted<'a> {
    /// `deletes` sorted. A vector always names its data file: one that
    /// does not is `TableFault::VectorUnplaced` at listing.
    pub(crate) fn of(deletes: &'a [DeleteFileEntry]) -> Self {
        let mut sorted = Self {
            deletes,
            vectors: HashMap::new(),
            scoped: HashMap::new(),
            unscoped: Vec::new(),
            puffins: BTreeMap::new(),
        };
        for (at, delete) in (0_u32..).zip(deletes) {
            match (&delete.kind, delete.referenced.as_deref()) {
                (DeleteKind::Vector { .. }, Some(referenced)) => {
                    sorted.vectors.entry(referenced).or_default().push(at);
                    sorted.puffins.entry(&delete.path).or_default().push(at);
                }
                (DeleteKind::Vector { .. }, None) => {}
                (DeleteKind::Positions, Some(referenced)) => {
                    sorted.scoped.entry(referenced).or_default().push(at);
                }
                (DeleteKind::Positions, None) => sorted.unscoped.push(at),
            }
        }
        sorted
    }

    /// The delete at `at`.
    fn entry(&self, at: u32) -> &'a DeleteFileEntry {
        &self.deletes[approx::length(at)]
    }

    /// The deletes at `held` that apply to a file at `sequence`.
    fn applying<'s>(
        &'s self,
        held: Option<&'s Vec<u32>>,
        sequence: SequenceNumber,
    ) -> impl Iterator<Item = u32> + 's {
        held.into_iter()
            .flatten()
            .copied()
            .filter(move |&at| applies(self.entry(at), sequence))
    }

    /// The deletion vectors that apply to the data file at `path` of
    /// data sequence number `sequence`.
    fn vectors_of(&self, path: &str, sequence: SequenceNumber) -> impl Iterator<Item = u32> + '_ {
        self.applying(self.vectors.get(path), sequence)
    }

    /// Whether a deletion vector applies to the data file at `path` of
    /// data sequence number `sequence`, which masks every position delete
    /// of that file, readable or not.
    pub(crate) fn masks(&self, path: &str, sequence: SequenceNumber) -> bool {
        self.vectors_of(path, sequence).next().is_some()
    }

    /// The position delete files that name `target` and apply to it.
    fn scoped_of(&self, target: &DataTarget) -> impl Iterator<Item = u32> + '_ {
        self.applying(self.scoped.get(target.path.as_str()), target.sequence)
    }

    /// Per data target, the deletion vector that applies to it, which
    /// masks its position deletes; two that apply to one file are
    /// `DeleteFault::TwoVectors` on the first by path, offset, and length.
    fn masking(&self, data: &[DataTarget]) -> Result<Vec<Option<u32>>> {
        data.iter()
            .map(|target| {
                let mut applying: Vec<u32> =
                    self.vectors_of(&target.path, target.sequence).collect();
                applying.sort_by_key(|&at| {
                    let entry = self.entry(at);
                    (&entry.path, vector_range(entry))
                });
                match applying.as_slice() {
                    [] => Ok(None),
                    [vector] => Ok(Some(*vector)),
                    [first, second, ..] => Err(fault(
                        &self.entry(*first).path,
                        DeleteFault::TwoVectors {
                            data_file: target.path.clone(),
                            other: self.entry(*second).path.clone(),
                        },
                    )),
                }
            })
            .collect()
    }

    /// The size every manifest entry of the Puffin file at `path` records,
    /// or `DeleteFault::PuffinSizeMismatch` when two differ.
    fn puffin_size(&self, path: &str) -> Result<u64> {
        let mut sizes = self
            .puffins
            .get(path)
            .into_iter()
            .flatten()
            .map(|&at| self.entry(at).size);
        let recorded = sizes.next().unwrap_or(0);
        match sizes.find(|&other| other != recorded) {
            Some(other) => Err(fault(
                path,
                DeleteFault::PuffinSizeMismatch { recorded, other },
            )),
            None => Ok(recorded),
        }
    }
}

/// The blob range a vector entry names; `(0, 0)` for a position delete.
fn vector_range(entry: &DeleteFileEntry) -> (u64, u64) {
    match entry.kind {
        DeleteKind::Vector { offset, length } => (offset, length),
        DeleteKind::Positions => (0, 0),
    }
}

/// Per partition, the least data sequence number of `files`, each a data
/// file's partition and data sequence number.
pub(crate) fn oldest_by_partition<'p>(
    files: impl IntoIterator<Item = (&'p Partition, SequenceNumber)>,
) -> HashMap<&'p Partition, SequenceNumber> {
    let mut oldest: HashMap<&Partition, SequenceNumber> = HashMap::new();
    for (partition, sequence) in files {
        oldest
            .entry(partition)
            .and_modify(|held| *held = (*held).min(sequence))
            .or_insert(sequence);
    }
    oldest
}

/// What one load reads its deletes against: its table and storage, the
/// snapshot's deletes sorted, with one interned path each, and its data
/// targets, with each target's index by path.
struct Context<'a> {
    table: &'a str,
    storage: &'a Storage,
    sorted: Sorted<'a>,
    paths: Vec<Arc<str>>,
    data: &'a [DataTarget],
    by_path: HashMap<&'a str, u32>,
}

/// The rows each source read in this load deletes: per position delete
/// file, by its index among the deletes, the positions it names in each
/// data target it applies to; per vector, its rows.
#[derive(Default)]
struct Read {
    positions: HashMap<u32, HashMap<u32, Vec<i64>>>,
    vectors: HashMap<DeleteSource, RoaringTreemap>,
}

/// The deletes of a snapshot of `data` and `deletes` applied (the module's
/// rules), reading only what `previous` does not hold, `load_concurrency()`
/// delete files at a time, and decoding, applying, and compacting on the
/// blocking pool. A delete that applies to no data file is not read, except
/// a position delete file without a referenced data file, which is read
/// once to learn which files of its partition it names, and again only when
/// a data file it could apply to was not there when it was.
pub(crate) async fn load(
    table: &str,
    storage: &Storage,
    data: Vec<DataTarget>,
    deletes: &[DeleteFileEntry],
    previous: Previous,
) -> Result<Applied> {
    let context = Context {
        table,
        storage,
        sorted: Sorted::of(deletes),
        paths: interned(deletes),
        data: &data,
        by_path: (0_u32..)
            .zip(&data)
            .map(|(at, target)| (target.path.as_str(), at))
            .collect(),
    };
    let masking = context.sorted.masking(&data)?;
    let mut read = Read::default();
    let named = named_by_unscoped(&context, &masking, &previous, &mut read).await?;
    let mut naming: HashMap<u32, Vec<u32>> = HashMap::new();
    for (&delete, targets) in &named {
        for &target in targets {
            naming.entry(target).or_default().push(delete);
        }
    }
    let plans: Vec<Plan> = (0_u32..)
        .zip(&data)
        .filter_map(|(at, target)| {
            let sources: Vec<u32> = match masking[approx::length(at)] {
                Some(vector) => vec![vector],
                None => context
                    .sorted
                    .scoped_of(target)
                    .chain(naming.get(&at).into_iter().flatten().copied())
                    .collect(),
            };
            let previous = previous.applied.get(approx::length(at)).cloned().flatten();
            Plan::of(at, &context, sources, previous)
        })
        .collect();
    let oldest = plans
        .iter()
        .flat_map(|plan| &plan.sources)
        .map(|&(_, at)| context.sorted.entry(at).sequence)
        .min();
    let fresh = read_needed(&context, &plans, &previous.known, &mut read).await?;
    let footers = retained_footers(&context, &plans, &fresh, &previous.known);
    let mut named: Vec<Named> = named
        .into_iter()
        .map(|(delete, targets)| {
            let path = Arc::clone(&context.paths[approx::length(delete)]);
            (path, targets)
        })
        .collect();
    named.sort_by(|a, b| a.0.cmp(&b.0));
    named.dedup_by(|a, b| a.0 == b.0);
    let data: Arc<[DataTarget]> = data.into();
    let first = data
        .first()
        .map_or_else(String::new, |target| target.path.clone());
    let (rows, shrank) =
        blocking::on_blocking_pool(move || applied(&data, plans, &read, &previous.applied))
            .await
            .map_err(|canceled| Error::verify_canceled(&first, canceled))??;
    Ok(Applied {
        rows,
        shrank,
        known: Known {
            named: named.into_boxed_slice(),
            footers,
        },
        oldest,
    })
}

/// One `Arc<str>` per distinct path of `deletes`, by delete.
fn interned(deletes: &[DeleteFileEntry]) -> Vec<Arc<str>> {
    let mut held: HashMap<&str, Arc<str>> = HashMap::new();
    deletes
        .iter()
        .map(|delete| {
            Arc::clone(
                held.entry(&delete.path)
                    .or_insert_with(|| Arc::from(delete.path.as_str())),
            )
        })
        .collect()
}

/// Per position delete file of the context's without a referenced data file
/// that may apply to a data target of its partition, by its index, the
/// targets it names, sorted: from `previous` when every target it could
/// apply to (of its partition, at a sequence number no greater than its
/// own) was one `previous` covers, else read, its positions kept in `read`.
/// One that may apply to an unmasked target is read when `previous` does
/// not cover it; one that may apply only to masked targets is not, and is
/// not kept unless `previous` covers it.
async fn named_by_unscoped(
    context: &Context<'_>,
    masking: &[Option<u32>],
    previous: &Previous,
    read: &mut Read,
) -> Result<HashMap<u32, Box<[u32]>>> {
    let Context {
        storage,
        sorted,
        data,
        by_path,
        ..
    } = context;
    let unmasked = oldest_by_partition(
        data.iter()
            .zip(masking)
            .filter(|(_, vector)| vector.is_none())
            .map(|(target, _)| (&target.partition, target.sequence)),
    );
    let uncovered = oldest_by_partition(
        data.iter()
            .skip(previous.applied.len())
            .map(|target| (&target.partition, target.sequence)),
    );
    let reaches = |oldest: &HashMap<&Partition, SequenceNumber>, delete: &DeleteFileEntry| {
        oldest
            .get(&delete.partition)
            .is_some_and(|&sequence| applies(delete, sequence))
    };
    let mut named = HashMap::new();
    let mut to_read = Vec::new();
    for &at in &sorted.unscoped {
        let delete = sorted.entry(at);
        match previous.known.named(&delete.path) {
            Some(held) if !reaches(&uncovered, delete) => {
                named.insert(at, held.into());
            }
            Some(_) | None => {
                if reaches(&unmasked, delete) {
                    to_read.push(at);
                }
            }
        }
    }
    let entries: Vec<&DeleteFileEntry> = to_read.iter().map(|&at| sorted.entry(at)).collect();
    for (&at, positions) in to_read.iter().zip(read_positions(storage, &entries).await?) {
        let delete = sorted.entry(at);
        let rows = by_target(positions, by_path, |target| {
            let target = &data[approx::length(target)];
            target.partition == delete.partition && applies(delete, target.sequence)
        });
        let mut targets: Vec<u32> = rows.keys().copied().collect();
        targets.sort_unstable();
        named.insert(at, targets.into_boxed_slice());
        read.positions.insert(at, rows);
    }
    Ok(named)
}

/// `positions` by the data target each names, for the targets `kept`
/// takes.
fn by_target(
    positions: position::Positions,
    by_path: &HashMap<&str, u32>,
    kept: impl Fn(u32) -> bool,
) -> HashMap<u32, Vec<i64>> {
    positions
        .into_iter()
        .filter_map(|(path, rows)| {
            by_path
                .get(path.as_str())
                .copied()
                .filter(|&target| kept(target))
                .map(|target| (target, rows))
        })
        .collect()
}

/// What one data target's applied rows are built from this load.
struct Plan {
    target: u32,
    /// Every source that applies, sorted, with its index among the deletes.
    sources: Vec<(DeleteSource, u32)>,
    /// The previous generation's rows for the file, to reuse or extend:
    /// only when every source they came from still applies.
    base: Option<Arc<FileDeletes>>,
    /// The sources whose rows this load must read: every one the base
    /// does not hold.
    to_read: Vec<usize>,
}

impl Plan {
    /// The plan of target `target` from the context's deletes at `sources`,
    /// beside the previous rows for it; `None` when no delete applies.
    fn of(
        target: u32,
        context: &Context<'_>,
        sources: Vec<u32>,
        previous: Option<Arc<FileDeletes>>,
    ) -> Option<Self> {
        let mut sources: Vec<(DeleteSource, u32)> = sources
            .into_iter()
            .map(|at| {
                let path = &context.paths[approx::length(at)];
                (DeleteSource::of(context.sorted.entry(at), path), at)
            })
            .collect();
        if sources.is_empty() {
            return None;
        }
        sources.sort_by(|a, b| a.0.cmp(&b.0));
        sources.dedup_by(|a, b| a.0 == b.0);
        let held = |source: &DeleteSource| {
            sources
                .binary_search_by(|(listed, _)| listed.cmp(source))
                .is_ok()
        };
        let base = previous.filter(|previous| previous.sources.iter().all(held));
        let to_read = (0..sources.len())
            .filter(|&at| {
                base.as_ref()
                    .is_none_or(|base| base.sources.binary_search(&sources[at].0).is_err())
            })
            .collect();
        Some(Self {
            target,
            sources,
            base,
            to_read,
        })
    }

    /// The file's applied rows: the base when nothing is to read, else the
    /// base's rows with the rows of every source read, each position
    /// checked against the file's row count, compacted.
    fn apply(self, target: &DataTarget, read: &Read) -> Result<Arc<FileDeletes>> {
        if let Some(base) = &self.base
            && self.to_read.is_empty()
        {
            return Ok(Arc::clone(base));
        }
        let mut rows = self
            .base
            .as_ref()
            .map_or_else(RoaringTreemap::new, |base| base.rows.clone());
        let records = target.records;
        for &at in &self.to_read {
            let (source, delete) = &self.sources[at];
            let past_end = |position| {
                fault(
                    source.path(),
                    DeleteFault::PositionPastEnd {
                        data_file: target.path.clone(),
                        position,
                        records,
                    },
                )
            };
            match source {
                DeleteSource::Vector { .. } => {
                    let vector = &read.vectors[source];
                    if let Some(last) = vector.max()
                        && last >= records
                    {
                        return Err(past_end(last));
                    }
                    rows |= vector;
                }
                DeleteSource::Positions { .. } => {
                    let named = read.positions[delete]
                        .get(&self.target)
                        .into_iter()
                        .flatten();
                    for &position in named {
                        let position = u64::try_from(position).map_err(|_| {
                            fault(
                                source.path(),
                                DeleteFault::NegativePosition {
                                    data_file: target.path.clone(),
                                    position,
                                },
                            )
                        })?;
                        if position >= records {
                            return Err(past_end(position));
                        }
                        rows.insert(position);
                    }
                }
            }
        }
        Ok(FileDeletes::built(
            rows,
            self.sources.into_iter().map(|(source, _)| source),
        ))
    }
}

/// Every plan of `plans` applied to its target of `data` from `read`, and
/// per target whether its rows shrank against `previous`, the previous
/// rows of the targets it covers: some row `previous` deleted is live
/// again (`FileDeletes::holds`).
fn applied(
    data: &[DataTarget],
    plans: Vec<Plan>,
    read: &Read,
    previous: &[Option<Arc<FileDeletes>>],
) -> Result<(TargetRows, Vec<bool>)> {
    let mut rows: TargetRows = vec![None; data.len()];
    for plan in plans {
        let at = approx::length(plan.target);
        rows[at] = Some(plan.apply(&data[at], read)?);
    }
    let mut shrank = vec![false; data.len()];
    for (at, before) in previous.iter().enumerate() {
        if let Some(before) = before
            && !before.rows.is_empty()
        {
            shrank[at] = rows[at]
                .as_ref()
                .is_none_or(|now| !Arc::ptr_eq(before, now) && !now.holds(before));
        }
    }
    Ok((rows, shrank))
}

/// Reads every position delete file of `deletes`, `load_concurrency()` at
/// a time, in order. The positions are streamed by value: a closure taking
/// a reference and returning an `async` block makes every future up the
/// call chain unprovably `Send`, and a load must be spawnable.
async fn read_positions(
    storage: &Storage,
    deletes: &[&DeleteFileEntry],
) -> Result<Vec<position::Positions>> {
    stream::iter(0..deletes.len())
        .map(|at| async move { position::read(storage, deletes[at]).await })
        .buffered(load_concurrency())
        .try_collect()
        .await
}

/// One deletion vector a load reads: its source, its entry, and the data
/// file it applies to.
struct VectorRead<'a> {
    source: &'a DeleteSource,
    entry: &'a DeleteFileEntry,
    referenced: &'a str,
}

/// Reads into `read` the rows of every source a plan of `plans` reads that
/// `read` does not hold: each position delete file whole, once, and per
/// Puffin file every vector needed, checked against its footer (kept in
/// `known`, else read), in one `read_ranges` call. The footers read, by
/// path.
async fn read_needed(
    context: &Context<'_>,
    plans: &[Plan],
    known: &Known,
    read: &mut Read,
) -> Result<HashMap<Arc<str>, puffin::Footer>> {
    let Context {
        table,
        storage,
        sorted,
        data,
        by_path,
        ..
    } = context;
    let mut positions: Vec<u32> = Vec::new();
    let mut positions_seen = HashSet::new();
    let mut by_puffin: BTreeMap<&Arc<str>, Vec<VectorRead<'_>>> = BTreeMap::new();
    let mut vectors_seen = HashSet::new();
    for plan in plans {
        for &at in &plan.to_read {
            let (source, delete) = &plan.sources[at];
            match source {
                DeleteSource::Positions { .. } => {
                    if !read.positions.contains_key(delete) && positions_seen.insert(*delete) {
                        positions.push(*delete);
                    }
                }
                DeleteSource::Vector { path, .. } => {
                    if vectors_seen.insert(source) {
                        by_puffin.entry(path).or_default().push(VectorRead {
                            source,
                            entry: sorted.entry(*delete),
                            referenced: &data[approx::length(plan.target)].path,
                        });
                    }
                }
            }
        }
    }
    let entries: Vec<&DeleteFileEntry> = positions.iter().map(|&at| sorted.entry(at)).collect();
    for (&at, held) in positions
        .iter()
        .zip(read_positions(storage, &entries).await?)
    {
        read.positions
            .insert(at, by_target(held, by_path, |_| true));
    }
    let puffins: Vec<(&Arc<str>, Vec<VectorRead<'_>>)> = by_puffin.into_iter().collect();
    let puffins = &puffins;
    let fetched: Vec<(Option<puffin::Footer>, Vec<RoaringTreemap>)> =
        stream::iter(0..puffins.len())
            .map(|at| async move {
                let (path, vectors) = &puffins[at];
                let size = sorted.puffin_size(path)?;
                read_vectors(table, storage, path, size, vectors, known.footer(path)).await
            })
            .buffered(load_concurrency())
            .try_collect()
            .await?;
    let mut footers = HashMap::new();
    for ((path, vectors), (footer, decoded)) in puffins.iter().zip(fetched) {
        if let Some(footer) = footer {
            footers.insert(Arc::clone(path), footer);
        }
        for (vector, rows) in vectors.iter().zip(decoded) {
            read.vectors.insert(vector.source.clone(), rows);
        }
    }
    Ok(footers)
}

/// Each of `vectors` in the Puffin file at `path` of `size` bytes, checked
/// against its footer, read in one `read_ranges` call, and decoded on the
/// blocking pool, each holding the rows its entry records, else
/// `DeleteFault::RecordsDecoded`; and the footer, when it was read. The
/// footer is read unless `retained` holds every vector as its entry
/// records it.
async fn read_vectors(
    table: &str,
    storage: &Storage,
    path: &str,
    size: u64,
    vectors: &[VectorRead<'_>],
    retained: Option<&Arc<puffin::Retained>>,
) -> Result<(Option<puffin::Footer>, Vec<RoaringTreemap>)> {
    let held = retained.is_some_and(|retained| {
        vectors.iter().all(|vector| {
            retained.holds(
                vector_range(vector.entry),
                vector.referenced,
                vector.entry.records,
            )
        })
    });
    let footer = if held {
        None
    } else {
        let footer = puffin::read_footer(table, storage, path, size).await?;
        for vector in vectors {
            puffin::checked_blob(
                table,
                path,
                &footer,
                vector_range(vector.entry),
                vector.referenced,
                vector.entry.records,
            )?;
        }
        Some(footer)
    };
    let ranges: Vec<std::ops::Range<u64>> = vectors
        .iter()
        .map(|vector| {
            let (offset, length) = vector_range(vector.entry);
            offset..offset + length
        })
        .collect();
    let located = storage.locate(path).await?;
    let blobs = storage.read_ranges(&located, &ranges).await?;
    let recorded: Vec<u64> = vectors.iter().map(|vector| vector.entry.records).collect();
    let owned_path = path.to_string();
    let decoded = blocking::on_blocking_pool(move || {
        blobs
            .iter()
            .zip(recorded)
            .map(|(blob, recorded)| {
                let rows = puffin::decode(blob).map_err(|framing| fault(&owned_path, framing))?;
                if rows.len() == recorded {
                    Ok(rows)
                } else {
                    Err(fault(
                        &owned_path,
                        DeleteFault::RecordsDecoded {
                            recorded,
                            decoded: rows.len(),
                        },
                    ))
                }
            })
            .collect::<Result<Vec<RoaringTreemap>>>()
    })
    .await
    .map_err(|canceled| Error::verify_canceled(path, canceled))??;
    Ok((footer, decoded))
}

/// Per Puffin file of the context's that lists a vector no plan of `plans`
/// applies, by path, sorted, those vectors' blobs as its footer passes
/// them: the footer read this load (`fresh`), else what `known` kept of
/// it. A file none of whose listed vectors is left over keeps nothing.
fn retained_footers(
    context: &Context<'_>,
    plans: &[Plan],
    fresh: &HashMap<Arc<str>, puffin::Footer>,
    known: &Known,
) -> Box<[KeptFooter]> {
    let applied: HashSet<u32> = plans
        .iter()
        .flat_map(|plan| plan.sources.iter().map(|&(_, at)| at))
        .collect();
    let sorted = &context.sorted;
    sorted
        .puffins
        .iter()
        .filter_map(|(&path, listed)| {
            let left: Vec<((u64, u64), &str, u64)> = listed
                .iter()
                .filter(|at| !applied.contains(at))
                .filter_map(|&at| {
                    let entry = sorted.entry(at);
                    let referenced = entry.referenced.as_deref()?;
                    Some((vector_range(entry), referenced, entry.records))
                })
                .collect();
            if left.is_empty() {
                return None;
            }
            let retained = match fresh.get(path) {
                Some(footer) => puffin::Retained::of(context.table, path, footer, left.into_iter()),
                None => known.footer(path)?.kept(left.into_iter()),
            }?;
            Some((Arc::from(path), Arc::new(retained)))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use iceberg::puffin::{CompressionCodec, DELETION_VECTOR_V1};

    use super::position::testing::position_file;
    use super::puffin::testing::{properties, vector_blob, write_puffin};
    use super::*;
    use crate::config::StorageConfig;
    use crate::generation::testing::TABLE;

    /// Rows of every data file of these tests.
    const RECORDS: u64 = 100;

    /// Files of one test in a temporary directory, read through one
    /// storage whose GETs are counted.
    struct Files {
        dir: tempfile::TempDir,
        storage: Storage,
    }

    impl Files {
        fn new() -> Self {
            Self {
                dir: tempfile::tempdir().unwrap(),
                storage: Storage::new(StorageConfig::default()),
            }
        }

        fn location(&self, name: &str) -> String {
            format!("file://{}/{name}", self.dir.path().display())
        }

        fn gets(&self) -> u64 {
            self.storage.stats().gets()
        }

        /// A position delete file `name` of `rows` at `sequence` in
        /// `partition`, naming `referenced` when given.
        fn positions_in(
            &self,
            name: &str,
            rows: &[(&str, i64)],
            sequence: i64,
            partition: Partition,
            referenced: Option<&str>,
        ) -> DeleteFileEntry {
            let rows: Vec<(String, i64)> = rows
                .iter()
                .map(|&(file, at)| (self.location(file), at))
                .collect();
            let borrowed: Vec<(&str, i64)> =
                rows.iter().map(|(file, at)| (file.as_str(), *at)).collect();
            let bytes = position_file(&borrowed);
            std::fs::write(self.dir.path().join(name), &bytes).unwrap();
            DeleteFileEntry {
                path: self.location(name),
                records: approx::count(rows.len()),
                size: approx::count(bytes.len()),
                sequence: SequenceNumber(sequence),
                referenced: referenced.map(|file| self.location(file)),
                partition,
                kind: DeleteKind::Positions,
            }
        }

        /// `positions_in` the unpartitioned spec.
        fn positions(
            &self,
            name: &str,
            rows: &[(&str, i64)],
            sequence: i64,
            referenced: Option<&str>,
        ) -> DeleteFileEntry {
            self.positions_in(name, rows, sequence, Partition::unpartitioned(), referenced)
        }

        /// A Puffin file `name` holding one vector per (data file, rows),
        /// each at `sequence`.
        async fn vectors(
            &self,
            name: &str,
            vectors: &[(&str, &[u64])],
            sequence: i64,
        ) -> Vec<DeleteFileEntry> {
            let blobs = vectors
                .iter()
                .map(|(file, rows)| {
                    let rows: RoaringTreemap = rows.iter().copied().collect();
                    (
                        DELETION_VECTOR_V1,
                        vector_blob(&rows),
                        properties(&self.location(file), rows.len()),
                    )
                })
                .collect();
            let ranges = write_puffin(&self.location(name), blobs, CompressionCodec::None).await;
            let size = std::fs::metadata(self.dir.path().join(name)).unwrap().len();
            vectors
                .iter()
                .zip(ranges)
                .map(|((file, rows), range)| DeleteFileEntry {
                    path: self.location(name),
                    records: approx::count(rows.len()),
                    size,
                    sequence: SequenceNumber(sequence),
                    referenced: Some(self.location(file)),
                    partition: Partition::unpartitioned(),
                    kind: DeleteKind::Vector {
                        offset: range.start,
                        length: range.end - range.start,
                    },
                })
                .collect()
        }

        /// Data file `name` at `sequence` of `RECORDS` rows in `partition`.
        fn data_in(&self, name: &str, sequence: i64, partition: Partition) -> DataTarget {
            DataTarget {
                path: self.location(name),
                sequence: SequenceNumber(sequence),
                records: RECORDS,
                partition,
            }
        }

        /// `data_in` the unpartitioned spec.
        fn data(&self, name: &str, sequence: i64) -> DataTarget {
            self.data_in(name, sequence, Partition::unpartitioned())
        }

        async fn load(
            &self,
            data: &[DataTarget],
            deletes: &[DeleteFileEntry],
            previous: Previous,
        ) -> Result<Applied> {
            load(TABLE, &self.storage, data.to_vec(), deletes, previous).await
        }
    }

    /// What a load reusing every data file starts from after `applied`.
    fn previous(applied: &Applied) -> Previous {
        Previous {
            applied: applied.rows.clone(),
            known: applied.known.clone(),
        }
    }

    /// The rows `applied` deletes in its data target at `at`.
    fn rows(applied: &Applied, at: usize) -> Vec<u64> {
        applied.rows[at].as_ref().unwrap().rows.iter().collect()
    }

    #[tokio::test]
    async fn test_load_reuses_extends_and_rebuilds_a_files_rows_by_its_sources() {
        let files = Files::new();
        let data = [files.data("a", 1)];
        let first = files.positions("p1.parquet", &[("a", 1), ("a", 2)], 2, Some("a"));
        let second = files.positions("p2.parquet", &[("a", 5)], 3, Some("a"));
        let one = files
            .load(&data, std::slice::from_ref(&first), Previous::default())
            .await
            .unwrap();
        assert_eq!(files.gets(), 1);
        assert_eq!(rows(&one, 0), [1, 2]);

        let gets = files.gets();
        let same = files
            .load(&data, std::slice::from_ref(&first), previous(&one))
            .await
            .unwrap();
        assert_eq!(files.gets(), gets, "unchanged sources read nothing");
        assert!(Arc::ptr_eq(
            one.rows[0].as_ref().unwrap(),
            same.rows[0].as_ref().unwrap()
        ));
        assert_eq!(same.shrank, [false]);

        let both = [first.clone(), second.clone()];
        let grown = files.load(&data, &both, previous(&same)).await.unwrap();
        assert_eq!(files.gets(), gets + 1, "only the new source");
        assert_eq!(rows(&grown, 0), [1, 2, 5]);
        assert_eq!(grown.shrank, [false]);

        let gets = files.gets();
        let dropped = files
            .load(&data, &[second], previous(&grown))
            .await
            .unwrap();
        assert_eq!(files.gets(), gets + 1, "the remaining source, read again");
        assert_eq!(rows(&dropped, 0), [5]);
        assert_eq!(dropped.shrank, [true]);

        let none = files.load(&data, &[], previous(&dropped)).await.unwrap();
        assert!(none.rows[0].is_none());
        assert_eq!(none.shrank, [true]);
    }

    #[tokio::test]
    async fn test_load_checks_a_reused_vector_again_when_its_entry_changes() {
        let files = Files::new();
        let data = [files.data("a", 1)];
        let vector = files.vectors("v.puffin", &[("a", &[3])], 2).await;
        let one = files
            .load(&data, &vector, Previous::default())
            .await
            .unwrap();
        let mut recounted = vector.clone();
        recounted[0].records += 1;
        let (path, fault) = delete_fault_of(files.load(&data, &recounted, previous(&one)).await);
        assert_eq!(path, files.location("v.puffin"));
        assert_eq!(
            fault,
            DeleteFault::CardinalityProperty {
                recorded: 2,
                found: Some("1".to_string()),
            },
            "not reused: its footer is read and checked again"
        );
    }

    #[tokio::test]
    async fn test_load_applies_by_sequence_number_and_a_vector_masks_positions() {
        let files = Files::new();
        let data = [files.data("a", 2), files.data("b", 2)];
        let mut deletes = files.vectors("v.puffin", &[("a", &[3, 4])], 2).await;
        deletes.push(files.positions("p.parquet", &[("a", 9), ("b", 8)], 2, None));
        deletes.push(files.positions("old.parquet", &[("b", 1)], 1, Some("b")));
        let applied = files
            .load(&data, &deletes, Previous::default())
            .await
            .unwrap();
        assert_eq!(
            rows(&applied, 0),
            [3, 4],
            "the vector alone, at the file's own number"
        );
        assert_eq!(rows(&applied, 1), [8], "the older delete does not apply");
        assert_eq!(
            files.gets(),
            1 + 1 + 1,
            "the unscoped file, the footer, and the one blob; the older file is not read"
        );
    }

    #[tokio::test]
    async fn test_load_applies_an_unscoped_position_delete_in_its_partition_alone() {
        let files = Files::new();
        let (own, other) = (
            Partition::new(
                0,
                iceberg::spec::Struct::from_iter([Some(iceberg::spec::Literal::int(0))]),
            ),
            Partition::new(
                0,
                iceberg::spec::Struct::from_iter([Some(iceberg::spec::Literal::int(1))]),
            ),
        );
        let data = [
            files.data_in("a", 1, own.clone()),
            files.data_in("b", 1, other),
        ];
        let deletes = [files.positions_in("p.parquet", &[("a", 1), ("b", 2)], 2, own, None)];
        let applied = files
            .load(&data, &deletes, Previous::default())
            .await
            .unwrap();
        assert_eq!(rows(&applied, 0), [1]);
        assert!(
            applied.rows[1].is_none(),
            "a file of another partition keeps its row"
        );
    }

    #[tokio::test]
    async fn test_load_reads_an_unscoped_file_again_for_a_data_file_it_did_not_cover() {
        let files = Files::new();
        let deletes = [files.positions("p.parquet", &[("a", 1), ("b", 2)], 2, None)];
        let first = files
            .load(&[files.data("a", 1)], &deletes, Previous::default())
            .await
            .unwrap();
        assert_eq!(files.gets(), 1);
        assert_eq!(first.known.named(&deletes[0].path), Some([0].as_slice()));
        // `b` returns, older than the delete: what the delete names in it
        // was not kept, so the delete is read again.
        let data = [files.data("a", 1), files.data("b", 1)];
        let again = files.load(&data, &deletes, previous(&first)).await.unwrap();
        assert_eq!(files.gets(), 2);
        assert_eq!(rows(&again, 1), [2]);
        // Both covered now: nothing is read.
        let still = files.load(&data, &deletes, previous(&again)).await.unwrap();
        assert_eq!(files.gets(), 2);
        assert_eq!(rows(&still, 1), [2]);
    }

    #[tokio::test]
    async fn test_load_reads_an_unscoped_dangling_file_once_and_a_dangling_vector_never() {
        let files = Files::new();
        let data = [files.data("a", 1)];
        let mut deletes = files.vectors("v.puffin", &[("gone", &[1])], 2).await;
        deletes.push(files.positions("p.parquet", &[("gone", 1)], 2, None));
        let first = files
            .load(&data, &deletes, Previous::default())
            .await
            .unwrap();
        assert!(first.rows[0].is_none());
        assert_eq!(files.gets(), 1, "the unscoped file alone");
        let again = files.load(&data, &deletes, previous(&first)).await.unwrap();
        assert!(again.rows[0].is_none());
        assert_eq!(files.gets(), 1, "remembered by its path");
    }

    #[tokio::test]
    async fn test_load_keeps_a_footer_while_it_lists_a_vector_no_file_takes() {
        let files = Files::new();
        let data = [files.data("a", 1)];
        let deletes = files
            .vectors("v.puffin", &[("a", &[1]), ("b", &[2])], 2)
            .await;
        let first = files
            .load(&data, &deletes, Previous::default())
            .await
            .unwrap();
        assert_eq!(files.gets(), 2, "the footer and the one blob");
        assert!(
            first.known.footer(&deletes[0].path).is_some(),
            "b's is left"
        );
        assert!(first.known.resident_bytes() > 0);
        // `b` arrives older than its vector: its blob alone, no footer GET.
        let data = [files.data("a", 1), files.data("b", 1)];
        let second = files.load(&data, &deletes, previous(&first)).await.unwrap();
        assert_eq!(files.gets(), 3, "the kept footer serves; the blob alone");
        assert_eq!(rows(&second, 1), [2]);
        assert!(
            second.known.footer(&deletes[0].path).is_none(),
            "every listed blob applied: the footer is dropped"
        );
    }

    #[tokio::test]
    async fn test_load_refuses_two_vectors_a_position_past_the_end_and_a_negative_one() {
        let files = Files::new();
        let data = [files.data("a", 1)];
        let mut deletes = files.vectors("v1.puffin", &[("a", &[1])], 2).await;
        deletes.extend(files.vectors("v2.puffin", &[("a", &[2])], 3).await);
        assert_eq!(
            delete_fault_of(files.load(&data, &deletes, Previous::default()).await),
            (
                files.location("v1.puffin"),
                DeleteFault::TwoVectors {
                    data_file: files.location("a"),
                    other: files.location("v2.puffin"),
                }
            )
        );
        let past = i64::try_from(RECORDS).unwrap();
        let deletes = [files.positions("p.parquet", &[("a", past)], 2, Some("a"))];
        assert_eq!(
            delete_fault_of(files.load(&data, &deletes, Previous::default()).await),
            (
                files.location("p.parquet"),
                DeleteFault::PositionPastEnd {
                    data_file: files.location("a"),
                    position: RECORDS,
                    records: RECORDS,
                }
            )
        );
        let vector = files.vectors("v3.puffin", &[("a", &[RECORDS])], 2).await;
        assert!(matches!(
            delete_fault_of(files.load(&data, &vector, Previous::default()).await).1,
            DeleteFault::PositionPastEnd {
                position: RECORDS,
                ..
            }
        ));
        let negative = -1;
        let deletes = [files.positions("n.parquet", &[("a", negative)], 2, Some("a"))];
        assert_eq!(
            delete_fault_of(files.load(&data, &deletes, Previous::default()).await),
            (
                files.location("n.parquet"),
                DeleteFault::NegativePosition {
                    data_file: files.location("a"),
                    position: negative,
                }
            )
        );
    }

    /// A well-formed vector whose key exceeds `Integer.MAX_VALUE`, which Java
    /// Iceberg refuses to deserialize, names a position above `i64::MAX`, past
    /// any data file's record count, which is an `i64`: the load refuses it
    /// as past the end, deterministically.
    #[tokio::test]
    async fn test_load_refuses_a_vector_position_above_icebergs_maximum_as_past_the_end() {
        let files = Files::new();
        let data = [files.data("a", 1)];
        let above_iceberg = u64::try_from(i64::MAX).unwrap() + 1;
        for position in [above_iceberg, u64::MAX] {
            let vector = files.vectors("high.puffin", &[("a", &[position])], 2).await;
            let fault = delete_fault_of(files.load(&data, &vector, Previous::default()).await).1;
            assert_eq!(
                fault,
                DeleteFault::PositionPastEnd {
                    data_file: files.location("a"),
                    position,
                    records: RECORDS,
                }
            );
            assert_eq!(fault.recurrence(), Recurrence::Deterministic);
        }
    }

    #[tokio::test]
    async fn test_load_refuses_a_puffin_file_whose_entries_disagree_on_its_size() {
        let files = Files::new();
        let data = [files.data("a", 1), files.data("b", 1)];
        let mut deletes = files
            .vectors("v.puffin", &[("a", &[1]), ("b", &[2])], 2)
            .await;
        let recorded = deletes[0].size;
        deletes[1].size += 1;
        assert_eq!(
            delete_fault_of(files.load(&data, &deletes, Previous::default()).await),
            (
                files.location("v.puffin"),
                DeleteFault::PuffinSizeMismatch {
                    recorded,
                    other: recorded + 1,
                }
            )
        );
    }

    #[test]
    fn test_holds_passes_over_empty_bitmaps() {
        let low: roaring::RoaringBitmap = [1].into_iter().collect();
        let empty_segment = 1;
        let before = FileDeletes {
            rows: RoaringTreemap::from_bitmaps([
                (0, low),
                (empty_segment, roaring::RoaringBitmap::new()),
            ]),
            sources: Box::new([]),
        };
        assert_eq!(before.rows.bitmaps().count(), 2, "an empty bitmap is held");
        assert_eq!(
            compact(before.rows.clone()).bitmaps().count(),
            1,
            "and compacted away"
        );
        let now = FileDeletes::of_rows([1]);
        assert!(now.holds(&before));
        assert!(before.holds(&now));
        assert!(!FileDeletes::of_rows([2]).holds(&before));
    }

    #[test]
    fn test_deleted_rows_answers_a_range_whole_or_not_at_all() {
        let rows = FileDeletes::of_rows([3, 4, 5]);
        let deleted = Some(&rows);
        let range = |start, end| RowPosition(start)..RowPosition(end);
        assert!(deleted.deleted(RowPosition(4)));
        assert!(!deleted.deleted(RowPosition(6)));
        assert!(deleted.all_deleted(range(3, 6)));
        assert!(deleted.all_deleted(range(4, 5)));
        assert!(!deleted.all_deleted(range(3, 7)), "a live row after");
        assert!(!deleted.all_deleted(range(2, 6)), "a live row before");
        assert!(deleted.all_deleted(range(9, 9)), "no row");
        let none: Option<&FileDeletes> = None;
        assert!(!none.deleted(RowPosition(4)));
        assert!(!none.all_deleted(range(3, 6)));
        assert!(none.all_deleted(range(3, 3)), "no row");
    }
}
