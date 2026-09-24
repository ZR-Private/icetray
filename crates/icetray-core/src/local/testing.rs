//! The chunk-file fixtures the local directory's tests share: one data-file
//! identity, its chunks, and the edits that make a chunk file fail a check.

use std::fs::{self, OpenOptions};
use std::ops::Range;
use std::os::unix::fs::FileExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;

use crate::config::LocalConfig;
use crate::local::{DIGEST_BYTES, DataFileIdentity, DiskChunks, LocalDir};
use crate::storage::IoStats;

/// A synthetic data file path; the chunk file's name is its digest.
pub(super) const DATA_PATH: &str = "s3://example-warehouse/db/kv/data/part-00000.parquet";
/// A second synthetic path, so a second chunk file has another name.
pub(super) const OTHER_PATH: &str = "s3://example-warehouse/db/kv/data/part-00001.parquet";
/// The size the fixture identity records: a contract-sized data file.
pub(super) const DATA_SIZE: u64 = 40_000_000;
/// The footer digest the fixture identity carries; any 32 bytes.
pub(super) const FOOTER_DIGEST: [u8; DIGEST_BYTES] = [7; DIGEST_BYTES];
/// The first stored chunk's range in the data file.
pub(super) const FIRST_RANGE: Range<u64> = 100..106;
/// The first stored chunk's bytes.
pub(super) const FIRST_BYTES: &[u8] = b"abcdef";
/// The second stored chunk's range, with a gap after the first.
pub(super) const SECOND_RANGE: Range<u64> = 200..204;
/// The second stored chunk's bytes.
pub(super) const SECOND_BYTES: &[u8] = b"wxyz";
/// A range strictly inside the first chunk.
pub(super) const INNER_RANGE: Range<u64> = 102..105;
/// The bytes `INNER_RANGE` covers.
pub(super) const INNER_BYTES: &[u8] = b"cde";
/// A page cache disk bound above zero, so `page_tier` is `Some`.
pub(super) const DISK_CACHE_BYTES: u64 = 1024;
/// The empty range at the first chunk's end.
pub(super) const EMPTY_AT_FIRST_END: Range<u64> = 106..106;

pub(super) fn identity() -> DataFileIdentity {
    DataFileIdentity {
        path: DATA_PATH.to_string(),
        size: DATA_SIZE,
        footer_digest: FOOTER_DIGEST,
    }
}

/// `INNER_RANGE` with its ends swapped: inside the first chunk, but
/// asking for nothing a span can hold.
pub(super) fn inverted_range() -> Range<u64> {
    Range {
        start: INNER_RANGE.end,
        end: INNER_RANGE.start,
    }
}

pub(super) fn config(dir: &Path, page_cache_disk_bytes: u64) -> LocalConfig {
    LocalConfig {
        dir: dir.to_path_buf(),
        index_budget_bytes: None,
        page_cache_disk_bytes,
    }
}

/// A local directory over `dir` with a fresh counter.
pub(super) fn open(dir: &Path) -> (LocalDir, Arc<IoStats>) {
    let stats = Arc::new(IoStats::default());
    let local = LocalDir::open(&config(dir, DISK_CACHE_BYTES), Arc::clone(&stats)).unwrap();
    (local, stats)
}

pub(super) fn chunks() -> Vec<(Range<u64>, Bytes)> {
    vec![
        (FIRST_RANGE, Bytes::from_static(FIRST_BYTES)),
        (SECOND_RANGE, Bytes::from_static(SECOND_BYTES)),
    ]
}

pub(super) fn payload_len() -> u64 {
    crate::approx::count(FIRST_BYTES.len() + SECOND_BYTES.len())
}

pub(super) fn range_len(range: &Range<u64>) -> u64 {
    range.end - range.start
}

/// Writes the fixture chunks for the fixture identity.
pub(super) fn write(local: &LocalDir) -> DiskChunks {
    local
        .write_chunks(&identity(), chunks().into_iter())
        .unwrap()
}

pub(super) fn chunk_file(local: &LocalDir) -> PathBuf {
    local.chunk_path(&identity())
}

pub(super) fn overwrite(path: &Path, offset: u64, bytes: &[u8]) {
    OpenOptions::new()
        .write(true)
        .open(path)
        .unwrap()
        .write_all_at(bytes, offset)
        .unwrap();
}

pub(super) fn entries(dir: &Path) -> Vec<PathBuf> {
    let mut entries: Vec<PathBuf> = fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    entries.sort();
    entries
}
