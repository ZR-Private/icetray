//! The read side of the local directory: positioned, counted reads of the
//! pages a chunk file holds, and the page cache's disk level (RFD 017).

use std::fs::File;
use std::ops::Range;
use std::os::unix::fs::FileExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;

use super::format::{ParsedChunkFile, StoredSpan};
use super::{LocalFault, local_fault};
use crate::approx;
use crate::error::{Error, Result};
use crate::storage::IoStats;

/// The key chunks of one data file, read from its chunk file on demand.
/// Clone-cheap: one handle is shared by the index and every read in
/// flight. It is not a `ChunkReader`; the index wraps the bytes a page read
/// returns in a `Segments`.
#[derive(Clone, Debug)]
pub struct DiskChunks {
    pub(super) shared: Arc<ChunkFile>,
}

/// What every clone of a `DiskChunks` shares: the open file, the span
/// table, and the counter.
#[derive(Debug)]
pub(super) struct ChunkFile {
    /// Read only through positioned reads, so the handle has no cursor to
    /// share and is never mapped.
    file: File,
    /// For errors.
    path: PathBuf,
    /// Ascending by data offset, so a page's span is found by binary search.
    pub(super) spans: Vec<StoredSpan>,
    stats: Arc<IoStats>,
}

impl DiskChunks {
    pub(super) fn new(
        file: File,
        path: PathBuf,
        parsed: ParsedChunkFile,
        stats: Arc<IoStats>,
    ) -> Self {
        Self {
            shared: Arc::new(ChunkFile {
                file,
                path,
                spans: parsed.spans,
                stats,
            }),
        }
    }

    /// The bytes of `range` of the data file, from the one stored chunk
    /// that holds the whole range; a range no chunk holds whole is
    /// `LocalFault::RangeNotResident` and costs nothing. The read is
    /// positioned, runs on the blocking pool so no worker stalls on disk,
    /// and counts as one local read of the range's length (RFD 017), counted
    /// before it is issued as GETs are.
    pub async fn page(&self, range: Range<u64>) -> Result<Bytes> {
        let Range { start, end } = range;
        let (file_offset, len) = self.shared.locate(start, end).ok_or_else(|| {
            self.shared
                .error(LocalFault::RangeNotResident { start, end })
        })?;
        self.shared.stats.record_local_read(len);
        let shared = Arc::clone(&self.shared);
        tokio::task::spawn_blocking(move || shared.read_at(file_offset, len))
            .await
            .map_err(|error| match error.try_into_panic() {
                Ok(payload) => std::panic::resume_unwind(payload),
                Err(cancelled) => self.shared.error(LocalFault::Join(cancelled)),
            })?
    }

    /// Whether one stored chunk holds the whole of `range`, so `page` would
    /// serve it: what the index checks before it skips a chunk fetch on a
    /// reused file, since the identity does not say which column's chunks
    /// the file holds.
    pub fn holds(&self, range: &Range<u64>) -> bool {
        self.shared.locate(range.start, range.end).is_some()
    }
}

impl ChunkFile {
    /// The chunk-file offset and the length of `start..end`, when one span
    /// holds the whole range; `None` for a range that straddles spans, falls
    /// outside them all, is empty, or is inverted.
    fn locate(&self, start: u64, end: u64) -> Option<(u64, u64)> {
        let len = end.checked_sub(start).filter(|len| *len > 0)?;
        let after = self.spans.partition_point(|span| span.data_offset <= start);
        let span = self.spans.get(after.checked_sub(1)?)?;
        let within = start - span.data_offset;
        if within.checked_add(len)? > span.length {
            return None;
        }
        Some((span.file_offset + within, len))
    }

    /// `len` bytes at `offset` of the chunk file by positioned read: no seek
    /// on the shared handle, and never a map.
    fn read_at(&self, offset: u64, len: u64) -> Result<Bytes> {
        // A span's length is bounded by the chunk file's size, which
        // `parse_chunk_file` checked against the file on disk.
        let len = approx::length(len);
        let mut buf = vec![0u8; len];
        self.file
            .read_exact_at(&mut buf, offset)
            .map_err(|error| self.error(LocalFault::Io(error)))?;
        Ok(Bytes::from(buf))
    }

    /// `fault` at this chunk file's path.
    fn error(&self, fault: LocalFault) -> Error {
        local_fault(self.path.clone(), fault)
    }
}

/// The page cache's disk level (RFD 017): the `cache/` directory, its size
/// bound, and the counter a page served from it is charged to, handed to
/// `PageCache` by `LocalDir::page_tier`. Pages there are keyed by file and
/// offset and evicted by size alone, never for staleness, since a data
/// file never changes.
#[derive(Clone, Debug)]
pub struct DiskTier {
    pub(super) dir: PathBuf,
    pub(super) capacity_bytes: u64,
    pub(super) stats: Arc<IoStats>,
}

impl DiskTier {
    /// The directory the level lives in; it exists once `LocalDir::open`
    /// has returned.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The bytes the level may occupy; above zero, or there is no level.
    pub fn capacity_bytes(&self) -> u64 {
        self.capacity_bytes
    }

    /// The counter every page served from the level is charged to as one
    /// local read (RFD 003): the same one `LocalDir` charges chunk reads to.
    pub(crate) fn stats(&self) -> &Arc<IoStats> {
        &self.stats
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::local::testing::{
        EMPTY_AT_FIRST_END, FIRST_BYTES, FIRST_RANGE, INNER_BYTES, INNER_RANGE, SECOND_BYTES,
        SECOND_RANGE, inverted_range, open, range_len, write,
    };
    use crate::local::{DiskChunks, LocalDir, LocalFault};

    /// A range that starts in the first chunk and ends in the second.
    const STRADDLING_RANGE: Range<u64> = 104..202;
    /// A range before every chunk.
    const OUTSIDE_RANGE: Range<u64> = 0..3;
    /// A range in the gap between the chunks.
    const GAP_RANGE: Range<u64> = 106..200;
    /// A range one byte longer than the first chunk.
    const PAST_FIRST_END: Range<u64> = 100..107;
    /// The last byte of the first chunk alone.
    const LAST_OF_FIRST: Range<u64> = 105..106;

    fn not_resident(result: &Result<Bytes>) -> (u64, u64) {
        let Err(Error::Local {
            fault: LocalFault::RangeNotResident { start, end },
            ..
        }) = result
        else {
            panic!("expected a non-resident range, got {result:?}");
        };
        (*start, *end)
    }

    #[tokio::test]
    async fn test_page_returns_each_span_and_counts_one_read_of_its_length() {
        let dir = tempfile::tempdir().unwrap();
        let (local, stats) = open(dir.path());
        let disk = write(&local);
        let first = disk.page(FIRST_RANGE).await.unwrap();
        assert_eq!(&first[..], FIRST_BYTES);
        assert_eq!(stats.local_reads(), 1);
        assert_eq!(stats.local_bytes(), range_len(&FIRST_RANGE));
        let second = disk.page(SECOND_RANGE).await.unwrap();
        assert_eq!(&second[..], SECOND_BYTES);
        assert_eq!(stats.local_reads(), 2);
        assert_eq!(
            stats.local_bytes(),
            range_len(&FIRST_RANGE) + range_len(&SECOND_RANGE)
        );
        assert_eq!(stats.gets(), 0);
    }

    #[tokio::test]
    async fn test_page_sub_range_inside_a_span() {
        let dir = tempfile::tempdir().unwrap();
        let (local, stats) = open(dir.path());
        let disk = write(&local);
        let inner = disk.page(INNER_RANGE).await.unwrap();
        assert_eq!(&inner[..], INNER_BYTES);
        assert_eq!(stats.local_reads(), 1);
        assert_eq!(stats.local_bytes(), range_len(&INNER_RANGE));
    }

    #[tokio::test]
    async fn test_page_straddling_two_spans_is_not_resident_and_counts_no_read() {
        let dir = tempfile::tempdir().unwrap();
        let (local, stats) = open(dir.path());
        let disk = write(&local);
        let (start, end) = not_resident(&disk.page(STRADDLING_RANGE).await);
        assert_eq!(start..end, STRADDLING_RANGE);
        assert_eq!(stats.local_reads(), 0);
        assert_eq!(stats.local_bytes(), 0);
    }

    #[tokio::test]
    async fn test_page_outside_every_span_is_not_resident_and_counts_no_read() {
        let dir = tempfile::tempdir().unwrap();
        let (local, stats) = open(dir.path());
        let disk = write(&local);
        for range in [OUTSIDE_RANGE, GAP_RANGE, inverted_range()] {
            let (start, end) = not_resident(&disk.page(range.clone()).await);
            assert_eq!(start..end, range);
        }
        assert_eq!(stats.local_reads(), 0);
        assert_eq!(stats.local_bytes(), 0);
    }

    #[tokio::test]
    async fn test_page_at_a_span_end_serves_the_last_byte_and_nothing_past_it() {
        let dir = tempfile::tempdir().unwrap();
        let (local, stats) = open(dir.path());
        let disk = write(&local);
        let last = disk.page(LAST_OF_FIRST).await.unwrap();
        assert_eq!(&last[..], &FIRST_BYTES[FIRST_BYTES.len() - 1..]);
        assert_eq!(stats.local_reads(), 1);
        for range in [PAST_FIRST_END, EMPTY_AT_FIRST_END] {
            let (start, end) = not_resident(&disk.page(range.clone()).await);
            assert_eq!(start..end, range);
        }
        assert_eq!(stats.local_reads(), 1);
        assert_eq!(stats.local_bytes(), range_len(&LAST_OF_FIRST));
    }

    #[test]
    fn test_holds_is_true_for_a_range_one_span_covers_whole() {
        let dir = tempfile::tempdir().unwrap();
        let (local, stats) = open(dir.path());
        let disk = write(&local);
        for range in [FIRST_RANGE, SECOND_RANGE, INNER_RANGE, LAST_OF_FIRST] {
            assert!(disk.holds(&range), "{range:?}");
        }
        for range in [
            STRADDLING_RANGE,
            OUTSIDE_RANGE,
            GAP_RANGE,
            PAST_FIRST_END,
            EMPTY_AT_FIRST_END,
            inverted_range(),
        ] {
            assert!(!disk.holds(&range), "{range:?}");
        }
        assert_eq!(stats.local_reads(), 0);
    }

    #[test]
    fn test_disk_chunks_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<DiskChunks>();
        assert_send_sync::<LocalDir>();
    }
}
