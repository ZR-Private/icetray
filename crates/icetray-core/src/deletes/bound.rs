//! The delete state's memory bound (RFD 021, Delete state), from the
//! manifests alone, so a `full` table whose deletes would exceed its
//! resident budget is refused before any delete file is read.
//!
//! A data file's deleted rows are a `RoaringTreemap`: one bitmap per
//! `SEGMENT_ROWS` rows of the file, each a run of containers of
//! `BLOCK_ROWS` rows. Once optimized and copied without slack
//! (`super::compact`), a container holds `BYTES_PER_ROW` bytes per deleted
//! row as an array, or at most `BLOCK_BYTES_MAX` as a bitset or runs, and
//! costs `CONTAINER_BYTES` beside them; each bitmap, with the file's
//! applied state around it, costs at most `FILE_BYTES`. For a file of `r`
//! rows with `D` deleted, in `n = ceil(r / BLOCK_ROWS)` blocks and
//! `b = ceil(r / SEGMENT_ROWS)` segments, the rows are bounded by
//!
//! ```text
//! min(BYTES_PER_ROW * D, BLOCK_BYTES_MAX * n)
//!     + CONTAINER_BYTES * min(D, n) + FILE_BYTES * min(D, b)
//! ```
//!
//! and each delete the file's rows came from adds its slot in the state's
//! sources and its path (`DeleteSource::resident_bytes`), counted per data
//! file although a load shares one path among them. What `Known` keeps is
//! bounded beside: per position delete file that names no data file, its
//! path and an index per data file it could name; per Puffin file that
//! lists a vector no data file takes, those vectors' blobs.
//!
//! The constants are measured from roaring's own allocations (the tests
//! below), not taken from its documentation.

use std::collections::HashMap;

use roaring::RoaringTreemap;

use super::puffin::CHECKED_BLOB_BYTES;
use super::{
    DataTarget, FOOTER_ENTRY_BYTES, NAMED_ENTRY_BYTES, NAMED_INDEX_BYTES, SOURCE_SLOT_BYTES,
    Sorted, applies, arc_str_bytes, boxed_str_bytes,
};
use crate::approx;
use crate::manifest::{DeleteFileEntry, Partition};
use crate::snapshot::SequenceNumber;

/// Rows one roaring container covers: the low sixteen bits of a position.
const BLOCK_ROWS: u64 = 1 << 16;

/// Rows one bitmap of a `RoaringTreemap` covers: the low thirty-two bits of
/// a position. A data file under four billion rows has one.
const SEGMENT_ROWS: u64 = 1 << 32;

/// Bytes one deleted row takes in an array container: one `u16`.
const BYTES_PER_ROW: u64 = 2;

/// The most rows an array container holds before roaring makes it a
/// bitset: at more, the bitset's `BLOCK_BYTES_MAX` is the smaller.
const ARRAY_ROWS_MAX: u64 = 4_096;

/// The most bytes an optimized container's values take: a bitset of
/// `BLOCK_ROWS` bits. `optimize` turns runs that would take more into a
/// bitset or an array, and an array above `ARRAY_ROWS_MAX` rows is a
/// bitset.
const BLOCK_BYTES_MAX: u64 = BLOCK_ROWS / 8;

// At its most rows an array takes a bitset's bytes, so `BLOCK_BYTES_MAX`
// caps an array container too.
const _: () = assert!(ARRAY_ROWS_MAX * BYTES_PER_ROW == BLOCK_BYTES_MAX);

/// Bytes a container takes beside its values: its key and its store's
/// header in its bitmap's vector of containers, which `compact` sizes
/// exactly. Measured as each sparse container's cost past its two value
/// bytes (`test_bound_constants_are_roarings_own_overhead`), for roaring
/// 0.11 on a 64-bit target; another version or layout re-measures it.
const CONTAINER_BYTES: u64 = 40;

/// Bytes a data file's applied state takes beside its containers and its
/// sources, per bitmap: the leaf node the treemap's map allocates for its
/// first bitmap and the shared `FileDeletes` the generation holds; a
/// further bitmap of a file past `SEGMENT_ROWS` rows costs less than its
/// first. Measured as a one-row state's cost past its one container and
/// its one source, for roaring 0.11 on a 64-bit target, as
/// `CONTAINER_BYTES` is.
const FILE_BYTES: u64 = 376;

/// The most bytes a deleted row can add to any file's rows: its value
/// bytes, a new container, and a new bitmap.
const ROW_BYTES_MAX: u64 = BYTES_PER_ROW + CONTAINER_BYTES + FILE_BYTES;

/// The bound on the rows of a file of `records` rows with `deleted` rows
/// deleted (the module's formula); `deleted` above `records` counts as
/// `records`, since a position past the end is refused and a repeated one
/// is one row.
fn file_bound(records: u64, deleted: u64) -> u64 {
    let deleted = deleted.min(records);
    let blocks = records.div_ceil(BLOCK_ROWS);
    let segments = records.div_ceil(SEGMENT_ROWS);
    BYTES_PER_ROW
        .saturating_mul(deleted)
        .min(BLOCK_BYTES_MAX.saturating_mul(blocks))
        .saturating_add(CONTAINER_BYTES.saturating_mul(deleted.min(blocks)))
        .saturating_add(FILE_BYTES.saturating_mul(deleted.min(segments)))
}

/// The bytes `delete` adds to the state of each data file it applies to:
/// its source's slot and its path.
fn pair_bytes(delete: &DeleteFileEntry) -> u64 {
    SOURCE_SLOT_BYTES + arc_str_bytes(delete.path.len())
}

/// The data targets of one partition, as the position delete files of it
/// that name no data file may reach them.
#[derive(Default)]
struct Reach {
    /// The bytes their rows can grow by past what their own deletes bound,
    /// for the unmasked targets an unscoped delete may apply to.
    room: u64,
    /// The sequence numbers of the unmasked targets, ascending.
    unmasked: Vec<SequenceNumber>,
    /// The sequence numbers of every target, ascending.
    every: Vec<SequenceNumber>,
    /// The rows of the unscoped deletes that may apply to an unmasked one.
    rows: u64,
}

/// How many of `sequences`, ascending, `delete` applies to.
fn reached(sequences: &[SequenceNumber], delete: &DeleteFileEntry) -> u64 {
    approx::count(sequences.partition_point(|&sequence| applies(delete, sequence)))
}

/// The bound on the delete state a generation of `data` builds from the
/// deletes `sorted` holds, from their manifest entries alone, beside what
/// it keeps for the next load. A file with a deletion vector that applies
/// counts the rows and sources of every such vector, and its position
/// deletes not at all, as they are masked; otherwise those of every
/// position delete file that names it as its referenced data file and
/// applies. A position delete file that names no data file may put its
/// rows in any unmasked file of its partition it applies to by sequence
/// number, so those rows are bounded together per partition: each adds at
/// most `ROW_BYTES_MAX`, and together they fill those files no further than
/// their every row; and it is a source of at most as many of those files as
/// it has rows. The sums saturate.
pub(crate) fn bound(sorted: &Sorted<'_>, data: &[DataTarget]) -> u64 {
    let mut highest: HashMap<&Partition, SequenceNumber> = HashMap::new();
    for &at in &sorted.unscoped {
        let delete = sorted.entry(at);
        highest
            .entry(&delete.partition)
            .and_modify(|held| *held = (*held).max(delete.sequence))
            .or_insert(delete.sequence);
    }
    let mut total: u64 = 0;
    let mut reach: HashMap<&Partition, Reach> = HashMap::new();
    for target in data {
        let vectors: Vec<u32> = sorted.vectors_of(&target.path, target.sequence).collect();
        let masked = !vectors.is_empty();
        let sources = if masked {
            vectors
        } else {
            sorted.scoped_of(target).collect()
        };
        let removed = sources
            .iter()
            .map(|&at| sorted.entry(at).records)
            .fold(0, u64::saturating_add);
        let own = file_bound(target.records, removed);
        total = sources
            .iter()
            .map(|&at| pair_bytes(sorted.entry(at)))
            .fold(total.saturating_add(own), u64::saturating_add);
        let partition = reach.entry(&target.partition).or_default();
        partition.every.push(target.sequence);
        if !masked {
            partition.unmasked.push(target.sequence);
            if highest
                .get(&target.partition)
                .is_some_and(|&highest| highest >= target.sequence)
            {
                let whole = file_bound(target.records, target.records);
                partition.room = partition.room.saturating_add(whole - own);
            }
        }
    }
    for partition in reach.values_mut() {
        partition.unmasked.sort_unstable();
        partition.every.sort_unstable();
    }
    for &at in &sorted.unscoped {
        let delete = sorted.entry(at);
        let (unmasked, every) = reach
            .get_mut(&delete.partition)
            .map_or((0, 0), |partition| {
                let unmasked = reached(&partition.unmasked, delete);
                if unmasked > 0 {
                    partition.rows = partition.rows.saturating_add(delete.records);
                }
                (unmasked, reached(&partition.every, delete))
            });
        let named = NAMED_ENTRY_BYTES
            + arc_str_bytes(delete.path.len())
            + NAMED_INDEX_BYTES.saturating_mul(delete.records.min(every));
        total = total
            .saturating_add(pair_bytes(delete).saturating_mul(delete.records.min(unmasked)))
            .saturating_add(named);
    }
    total = reach
        .values()
        .map(|partition| {
            ROW_BYTES_MAX
                .saturating_mul(partition.rows)
                .min(partition.room)
        })
        .fold(total, u64::saturating_add);
    total.saturating_add(retained_bound(sorted, data))
}

/// The bound on what a load keeps of Puffin footers: per Puffin file that
/// lists a vector applying to no data file of `data`, its entry and path,
/// and each such vector's kept blob and referenced path.
fn retained_bound(sorted: &Sorted<'_>, data: &[DataTarget]) -> u64 {
    let by_path: HashMap<&str, &DataTarget> = data
        .iter()
        .map(|target| (target.path.as_str(), target))
        .collect();
    sorted
        .puffins
        .iter()
        .map(|(path, listed)| {
            let left: Vec<u64> = listed
                .iter()
                .map(|&at| sorted.entry(at))
                .filter(|delete| {
                    !delete
                        .referenced
                        .as_deref()
                        .and_then(|referenced| by_path.get(referenced))
                        .is_some_and(|target| applies(delete, target.sequence))
                })
                .map(|delete| {
                    let referenced = delete.referenced.as_deref().map_or(0, str::len);
                    CHECKED_BLOB_BYTES + boxed_str_bytes(referenced)
                })
                .collect();
            if left.is_empty() {
                0
            } else {
                left.into_iter().fold(
                    FOOTER_ENTRY_BYTES + arc_str_bytes(path.len()),
                    u64::saturating_add,
                )
            }
        })
        .fold(0, u64::saturating_add)
}

/// The bytes `rows` holds as `file_bound` counts them, from roaring's own
/// statistics: each array container's rows at `BYTES_PER_ROW`, each bitset
/// at `BLOCK_BYTES_MAX`, each run container at its serialized bytes, which
/// exceed what its runs hold, every container at `CONTAINER_BYTES`, and
/// every bitmap at `FILE_BYTES`.
pub(super) fn held(rows: &RoaringTreemap) -> u64 {
    rows.bitmaps()
        .map(|(_, bitmap)| {
            let statistics = bitmap.statistics();
            BYTES_PER_ROW * u64::from(statistics.n_values_array_containers)
                + BLOCK_BYTES_MAX * u64::from(statistics.n_bitset_containers)
                + statistics.n_bytes_run_containers
                + CONTAINER_BYTES * u64::from(statistics.n_containers)
                + FILE_BYTES
        })
        .fold(0, u64::saturating_add)
}

#[cfg(test)]
mod tests {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;
    use std::sync::Arc;

    use super::*;
    use crate::deletes::{DeleteSource, FileDeletes, interned};
    use crate::manifest::DeleteKind;
    use crate::manifest::testing::DELETE_BYTES;

    thread_local! {
        /// Bytes this thread holds from the global allocator, net of what
        /// it freed, so a test measures its own allocations alone while
        /// other tests run on other threads.
        static HELD: Cell<i64> = const { Cell::new(0) };
    }

    /// The test binary's allocator: the system's, counting each thread's
    /// bytes in `HELD`. A `#[global_allocator]` serves the whole binary, so
    /// every unit test of icetray-core allocates through it, not only these.
    struct Counting;

    /// `bytes` more held by this thread, or fewer when negative. During
    /// thread teardown the counter may be gone; nothing measures then.
    fn record(bytes: usize, sign: i64) {
        let bytes = i64::try_from(bytes).unwrap_or(i64::MAX);
        // intentional: a thread tearing down has no counter, and no test
        // measures on one.
        let _ = HELD.try_with(|held| held.set(held.get() + sign * bytes));
    }

    // SAFETY: every call forwards to `System` with the caller's arguments;
    // the counting touches a thread-local `Cell`, which allocates nothing.
    unsafe impl GlobalAlloc for Counting {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            record(layout.size(), 1);
            // SAFETY: the caller's contract is `System`'s.
            unsafe { System.alloc(layout) }
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            record(layout.size(), 1);
            // SAFETY: the caller's contract is `System`'s.
            unsafe { System.alloc_zeroed(layout) }
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            record(layout.size(), -1);
            // SAFETY: the caller's contract is `System`'s.
            unsafe { System.dealloc(ptr, layout) }
        }

        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            record(layout.size(), -1);
            record(new_size, 1);
            // SAFETY: the caller's contract is `System`'s.
            unsafe { System.realloc(ptr, layout, new_size) }
        }
    }

    #[global_allocator]
    static ALLOCATOR: Counting = Counting;

    /// The bytes `build` leaves allocated on this thread, and what it
    /// built, which must live until measured.
    fn allocated<T>(build: impl FnOnce() -> T) -> (u64, T) {
        let before = HELD.with(Cell::get);
        let value = build();
        let after = HELD.with(Cell::get);
        (u64::try_from(after - before).unwrap(), value)
    }

    /// The bytes one data file's applied state holds for `rows` from one
    /// source, built as a load builds it (`FileDeletes::built`), measured
    /// from the allocator, the source's path, interned before, aside.
    fn measured(rows: &RoaringTreemap) -> (u64, Arc<FileDeletes>) {
        let source = DeleteSource::Positions {
            path: Arc::from("file:///deletes.parquet"),
            records: rows.len(),
        };
        allocated(|| FileDeletes::built(rows.clone(), [source]))
    }

    /// Rows of a file the shapes below delete from: two segments, so a
    /// shape can reach the second.
    const RECORDS: u64 = SEGMENT_ROWS + BLOCK_ROWS;

    /// Shapes of deleted rows: one row; one row in each of many blocks, the
    /// scattered case; whole blocks, which runs hold; every other row of a
    /// block, a bitset; an array at its largest, spread across one block;
    /// and rows in both segments.
    fn shapes() -> Vec<(&'static str, RoaringTreemap)> {
        let sparse_blocks = 1_000;
        let array_stride = BLOCK_ROWS / ARRAY_ROWS_MAX;
        vec![
            ("one row", [7].into_iter().collect()),
            (
                "one row per block",
                (0..sparse_blocks)
                    .map(|block| block * BLOCK_ROWS + 3)
                    .collect(),
            ),
            ("whole blocks", (0..3 * BLOCK_ROWS).collect()),
            ("every other row", (0..BLOCK_ROWS).step_by(2).collect()),
            (
                "the largest array",
                (0..ARRAY_ROWS_MAX).map(|at| at * array_stride).collect(),
            ),
            ("both segments", [1, SEGMENT_ROWS + 1].into_iter().collect()),
        ]
    }

    #[test]
    fn test_bound_holds_roarings_measured_allocations() {
        let source = SOURCE_SLOT_BYTES;
        for (shape, rows) in shapes() {
            let (held, state) = measured(&rows);
            let bound = file_bound(RECORDS, rows.len()) + source;
            assert!(
                held <= bound,
                "{shape}: held {held}, bound {bound}; re-measure CONTAINER_BYTES and FILE_BYTES"
            );
            assert!(
                held <= state.resident_bytes(),
                "{shape}: held {held}, counted {}; re-measure CONTAINER_BYTES and FILE_BYTES",
                state.resident_bytes()
            );
        }
    }

    /// `CONTAINER_BYTES` and `FILE_BYTES` are what roaring allocates, not
    /// an allowance: a one-row state costs exactly one row, one container,
    /// one file, and one source's slot, and each further scattered row one
    /// row and one container more.
    #[test]
    fn test_bound_constants_are_roarings_own_overhead() {
        let one: RoaringTreemap = [7].into_iter().collect();
        assert_eq!(
            measured(&one).0,
            BYTES_PER_ROW + CONTAINER_BYTES + FILE_BYTES + SOURCE_SLOT_BYTES,
            "re-measure CONTAINER_BYTES and FILE_BYTES"
        );
        let blocks = 64;
        let scattered: RoaringTreemap = (0..blocks).map(|block| block * BLOCK_ROWS).collect();
        assert_eq!(
            measured(&scattered).0,
            blocks * (BYTES_PER_ROW + CONTAINER_BYTES) + FILE_BYTES + SOURCE_SLOT_BYTES,
            "re-measure CONTAINER_BYTES and FILE_BYTES"
        );
    }

    #[test]
    fn test_file_bound_caps_dense_blocks_and_counts_rows_past_the_file_once() {
        let blocks = 4;
        let records = blocks * BLOCK_ROWS;
        assert_eq!(file_bound(records, 0), 0);
        assert_eq!(
            file_bound(records, records),
            blocks * (BLOCK_BYTES_MAX + CONTAINER_BYTES) + FILE_BYTES
        );
        assert_eq!(
            file_bound(records, records * 2),
            file_bound(records, records),
            "rows past the file are refused, not held"
        );
    }

    /// A data target of `records` rows at `sequence`, unpartitioned.
    fn data(path: &str, records: u64, sequence: i64) -> DataTarget {
        DataTarget {
            path: path.to_string(),
            sequence: SequenceNumber(sequence),
            records,
            partition: Partition::unpartitioned(),
        }
    }

    /// A delete entry of `records` rows at `sequence` naming `referenced`.
    fn delete(
        path: &str,
        records: u64,
        sequence: i64,
        referenced: Option<&str>,
        kind: DeleteKind,
    ) -> DeleteFileEntry {
        DeleteFileEntry {
            path: path.to_string(),
            records,
            size: DELETE_BYTES,
            sequence: SequenceNumber(sequence),
            referenced: referenced.map(str::to_string),
            partition: Partition::unpartitioned(),
            kind,
        }
    }

    /// A data file of a thousand single-row position delete files, one row
    /// per block: its state, interned paths and all, as a load builds it,
    /// is within the bound, and within what it counts.
    #[test]
    fn test_bound_holds_a_file_of_many_single_row_sources() {
        let sources = 1_000;
        let target = data("file:///data/a.parquet", RECORDS, 1);
        let deletes: Vec<DeleteFileEntry> = (0..sources)
            .map(|at| {
                delete(
                    &format!("file:///deletes/{at}.parquet"),
                    1,
                    2,
                    Some(&target.path),
                    DeleteKind::Positions,
                )
            })
            .collect();
        let rows: RoaringTreemap = (0..sources).map(|at| at * BLOCK_ROWS).collect();
        let (held, state) = allocated(|| {
            let paths = interned(&deletes);
            let sources: Vec<DeleteSource> = deletes
                .iter()
                .zip(&paths)
                .map(|(delete, path)| DeleteSource::of(delete, path))
                .collect();
            drop(paths);
            FileDeletes::built(rows.clone(), sources)
        });
        let bound = bound(&Sorted::of(&deletes), std::slice::from_ref(&target));
        assert!(
            held <= bound,
            "held {held}, bound {bound}; re-measure CONTAINER_BYTES and FILE_BYTES"
        );
        assert!(held <= state.resident_bytes());
        assert!(
            bound > file_bound(RECORDS, sources),
            "the sources are counted"
        );
    }

    /// Bytes a test entry's path adds as a source.
    fn pair(path: &str) -> u64 {
        SOURCE_SLOT_BYTES + arc_str_bytes(path.len())
    }

    /// Bytes `Known` may keep for an unscoped position delete file at
    /// `path` naming `named` data files.
    fn named(path: &str, named: u64) -> u64 {
        NAMED_ENTRY_BYTES + arc_str_bytes(path.len()) + NAMED_INDEX_BYTES * named
    }

    /// Bytes `Known` may keep for a Puffin file at `path` whose vectors for
    /// `referenced` no data file takes.
    fn footer(path: &str, referenced: &[&str]) -> u64 {
        referenced
            .iter()
            .map(|referenced| CHECKED_BLOB_BYTES + boxed_str_bytes(referenced.len()))
            .sum::<u64>()
            + FOOTER_ENTRY_BYTES
            + arc_str_bytes(path.len())
    }

    #[test]
    fn test_bound_counts_applying_deletes_and_lets_a_vector_mask_positions() {
        let records = 10 * BLOCK_ROWS;
        let files = [data("a", records, 2), data("b", records, 2)];
        let vector = DeleteKind::Vector {
            offset: 4,
            length: 1,
        };
        let positions = DeleteKind::Positions;
        let unscoped_rows = 3;
        let cases = [
            (vec![], 0),
            (
                vec![delete("v", 5, 2, Some("a"), vector)],
                file_bound(records, 5) + pair("v"),
            ),
            (
                vec![delete("old", 5, 1, Some("a"), vector)],
                footer("old", &["a"]),
            ),
            (
                vec![
                    delete("v", 5, 3, Some("a"), vector),
                    delete("p", 9, 3, Some("a"), positions),
                ],
                file_bound(records, 5) + pair("v"),
            ),
            (
                vec![
                    delete("p", 9, 3, Some("a"), positions),
                    delete("q", 2, 3, Some("a"), positions),
                ],
                file_bound(records, 11) + pair("p") + pair("q"),
            ),
            (
                vec![delete("u", unscoped_rows, 2, None, positions)],
                unscoped_rows * ROW_BYTES_MAX + 2 * pair("u") + named("u", 2),
            ),
            (
                vec![
                    delete("v", 5, 2, Some("a"), vector),
                    delete("u", records * 3, 2, None, positions),
                ],
                file_bound(records, 5)
                    + pair("v")
                    + file_bound(records, records)
                    + pair("u")
                    + named("u", 2),
            ),
        ];
        for (deletes, expected) in cases {
            assert_eq!(
                bound(&Sorted::of(&deletes), &files),
                expected,
                "{deletes:?}"
            );
        }
    }

    #[test]
    fn test_bound_reaches_only_the_partition_of_a_delete_naming_no_file() {
        let records = 10 * BLOCK_ROWS;
        let other = Partition::new(1, iceberg::spec::Struct::empty());
        let mut elsewhere = data("b", records, 2);
        elsewhere.partition = other;
        let files = [data("a", records, 2), elsewhere];
        let rows = records * 3;
        let deletes = [delete("u", rows, 2, None, DeleteKind::Positions)];
        assert_eq!(
            bound(&Sorted::of(&deletes), &files),
            file_bound(records, records) + pair("u") + named("u", 1),
            "only the file of its own partition"
        );
    }
}
