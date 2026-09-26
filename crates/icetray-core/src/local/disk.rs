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
use crate::blocking::on_blocking_pool;
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
    /// positioned and counts as one local read of the range's length
    /// (RFD 017), counted before it is issued as GETs are, wherever it runs.
    ///
    /// No worker waits for data from the disk (RFD 017): a read the
    /// operating system's page cache can serve without blocking may run on
    /// the calling worker, and a read that would block runs on the blocking
    /// pool. On Linux the read is first tried on the worker with
    /// `RWF_NOWAIT` (`ChunkFile::read_cached`), and goes to the blocking
    /// pool when that attempt does not return the whole range; on other
    /// systems it goes to the blocking pool.
    pub(crate) async fn page(&self, range: Range<u64>) -> Result<Bytes> {
        let Range { start, end } = range;
        let (file_offset, len) = self.shared.locate(start, end).ok_or_else(|| {
            self.shared
                .error(LocalFault::RangeNotResident { start, end })
        })?;
        self.shared.stats.record_local_read(len);
        #[cfg(target_os = "linux")]
        if let Some(bytes) = self.shared.read_cached(file_offset, len) {
            return Ok(bytes);
        }
        let shared = Arc::clone(&self.shared);
        on_blocking_pool(move || shared.read_at(file_offset, len))
            .await
            .map_err(|canceled| self.shared.error(LocalFault::Join(canceled)))?
    }

    /// Whether one stored chunk holds the whole of `range`, so `page` would
    /// serve it: what the index checks before it skips a chunk fetch on a
    /// reused file, since the identity does not say which column's chunks
    /// the file holds.
    pub(crate) fn holds(&self, range: &Range<u64>) -> bool {
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

    /// `len` bytes at `offset` of the chunk file when the operating
    /// system's page cache holds all of them, read without blocking by
    /// `preadv2` with `RWF_NOWAIT`, so the calling worker can run it.
    /// `None` when it cannot: the kernel returns `EAGAIN` when the range's
    /// first page is not cached, fewer bytes than asked when a later one is
    /// not, and `EOPNOTSUPP` when the kernel (before 4.14) or the file's
    /// filesystem (overlayfs, as of Linux 6.8) does not support the flag.
    /// A `None` is left to `read_at` on the blocking pool, which meets and
    /// reports a real I/O error itself.
    ///
    /// A cached read can still update the file's access time, which a
    /// journaling filesystem may write through its journal and wait on;
    /// under `relatime` the kernel does so on the first read after the file
    /// is written and then at most once a day, and on a directory mounted
    /// `noatime` not at all.
    #[cfg(target_os = "linux")]
    fn read_cached(&self, offset: u64, len: u64) -> Option<Bytes> {
        use std::os::fd::AsRawFd as _;
        /// One buffer takes the whole range.
        const BUFFERS: libc::c_int = 1;
        let offset = libc::off_t::try_from(offset).ok()?;
        let mut buf = vec![0u8; approx::length(len)];
        let target = libc::iovec {
            iov_base: buf.as_mut_ptr().cast(),
            iov_len: buf.len(),
        };
        // SAFETY: `target` describes `buf`: a live allocation of
        // `iov_len` initialized bytes, which nothing else borrows or moves
        // until the call returns, and `preadv2` writes at most `iov_len`
        // bytes into it. `BUFFERS` counts the one `iovec` passed. The
        // descriptor is `self.file`'s, which the `&self` borrow keeps open
        // for the call. An unsupported flag is `EOPNOTSUPP`, not undefined
        // behavior.
        let read = unsafe {
            libc::preadv2(
                self.file.as_raw_fd(),
                &raw const target,
                BUFFERS,
                offset,
                libc::RWF_NOWAIT,
            )
        };
        (usize::try_from(read).ok()? == buf.len()).then(|| Bytes::from(buf))
    }

    /// `fault` at this chunk file's path.
    fn error(&self, fault: LocalFault) -> Error {
        local_fault(self.path.clone(), fault)
    }
}

/// The page cache's disk level (RFD 017): the `cache/` directory, its size
/// bound, and the counter a page served from it is charged to, handed to
/// `PageCache` by `LocalDir::page_cache_level`. Pages there are keyed by
/// file and offset and evicted by size alone, never for staleness, since a
/// data file never changes.
#[derive(Clone, Debug)]
pub struct DiskLevel {
    pub(super) dir: PathBuf,
    pub(super) capacity_bytes: u64,
    pub(super) stats: Arc<IoStats>,
}

impl DiskLevel {
    /// The directory the level lives in; it exists once `LocalDir::open`
    /// has returned.
    pub(crate) fn dir(&self) -> &Path {
        &self.dir
    }

    /// The bytes the level may occupy; above zero, or there is no level.
    pub(crate) fn capacity_bytes(&self) -> u64 {
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

    /// Drops the chunk file's pages from the operating system's page cache,
    /// written back first so that none is dirty and each can be dropped.
    #[cfg(target_os = "linux")]
    fn evict(path: &std::path::Path) {
        use std::os::fd::AsRawFd as _;
        let file = File::open(path).unwrap();
        file.sync_all().unwrap();
        // SAFETY: `posix_fadvise` takes no pointer; the descriptor is
        // `file`'s, open for the call. Offset 0 and length 0 cover the file.
        let advised =
            unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
        assert_eq!(advised, 0, "posix_fadvise failed");
    }

    /// A page the operating system's page cache holds (the chunk file was
    /// just written) is read on the worker, to the bytes the blocking-pool
    /// read returns, and `page` counts it as one read. The temporary
    /// directory must be on a filesystem that serves `RWF_NOWAIT` reads
    /// (ext4, xfs, btrfs), not overlayfs: set `TMPDIR` inside a container.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_page_reads_a_cached_page_on_the_worker_to_the_pool_read_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let (local, stats) = open(dir.path());
        let disk = write(&local);
        for range in [FIRST_RANGE, SECOND_RANGE, INNER_RANGE] {
            let (offset, len) = disk.shared.locate(range.start, range.end).unwrap();
            let inline = disk.shared.read_cached(offset, len).expect(
                "a just-written page was not served without blocking: is the temporary \
                 directory on overlayfs or another filesystem without RWF_NOWAIT reads?",
            );
            let pooled = disk.shared.read_at(offset, len).unwrap();
            assert_eq!(inline, pooled, "{range:?}");
        }
        let first = disk.page(FIRST_RANGE).await.unwrap();
        assert_eq!(&first[..], FIRST_BYTES);
        assert_eq!(stats.local_reads(), 1);
        assert_eq!(stats.local_bytes(), range_len(&FIRST_RANGE));
    }

    /// A page dropped from the operating system's page cache is one the
    /// inline attempt cannot serve (`EAGAIN`), and `page` reads it on the
    /// blocking pool instead, to the right bytes, counted as one read.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_page_reads_an_uncached_page_on_the_blocking_pool() {
        let dir = tempfile::tempdir().unwrap();
        let (local, stats) = open(dir.path());
        let disk = write(&local);
        evict(&crate::local::testing::chunk_file(&local));
        let (offset, len) = disk
            .shared
            .locate(FIRST_RANGE.start, FIRST_RANGE.end)
            .unwrap();
        assert!(
            disk.shared.read_cached(offset, len).is_none(),
            "an evicted page was served without blocking: is the temporary directory on tmpfs?"
        );
        let first = disk.page(FIRST_RANGE).await.unwrap();
        assert_eq!(&first[..], FIRST_BYTES);
        assert_eq!(stats.local_reads(), 1);
        assert_eq!(stats.local_bytes(), range_len(&FIRST_RANGE));
    }

    #[test]
    fn test_disk_chunks_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<DiskChunks>();
        assert_send_sync::<LocalDir>();
    }
}
