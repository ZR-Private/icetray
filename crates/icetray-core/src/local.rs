//! The optional local directory (RFD 017, RFD 012): chunk files for disk
//! residency under `index/`, each named by a digest of its data file's path
//! and carrying that file's identity in a header inside the persisted-state
//! envelope of RFD 011, so a restarted replica reuses them and a generation
//! that drops a file deletes them; the page cache's disk level under
//! `cache/`; and positioned, counted reads of the pages the chunk files
//! hold. Everything here is rebuilt from object storage and needs no backup.
//!
//! A chunk file's payload carries no checksum: the header's identity covers
//! the data file's footer and the length check covers the payload's extent,
//! and nothing covers the payload's bytes.

use std::collections::HashSet;
use std::fmt;
use std::fs::{self, File, Metadata, OpenOptions};
use std::io::{self, Write as _};
use std::ops::Range;
use std::os::unix::fs::FileExt as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bytes::Bytes;
use prost::Message as _;
use sha2::{Digest as _, Sha256};

use crate::config::LocalConfig;
use crate::error::{Error, Result};
use crate::key::HEX_DIGITS_PER_BYTE;
use crate::storage::IoStats;

/// The directory under `LocalConfig::dir` that holds chunk files.
const INDEX_DIR: &str = "index";

/// The directory under `LocalConfig::dir` that holds the page cache's disk
/// level.
const CACHE_DIR: &str = "cache";

/// The extension of a chunk file still being written. A file carrying it is
/// incomplete: `write_chunks` renames it away once it is synced, and
/// `retain` deletes any that no writer in this process has open, which is
/// what a process that died mid-write leaves behind.
const TEMP_EXTENSION: &str = "tmp";

/// The first byte of a chunk file (RFD 011): identifies the file as an
/// icetray chunk file before any of it is parsed. `0xC1` is never the first
/// byte of UTF-8 text, so no configuration file, log, or JSON document
/// passes the check by accident.
const CHUNK_FILE_MAGIC: u8 = 0xC1;

/// The version byte of the chunk file format this build writes and the only
/// one it reads (RFD 011). A build that changes the layout raises it and
/// decides whether to convert or refuse older files.
const CHUNK_FILE_VERSION: u8 = 1;

/// Where the magic byte sits in a chunk file.
const MAGIC_OFFSET: u64 = 0;

/// Where the version byte sits: right after the magic byte.
const VERSION_OFFSET: u64 = MAGIC_OFFSET + 1;

/// Where the length prefix of the encoded header sits: right after the
/// version byte.
const LENGTH_OFFSET: u64 = VERSION_OFFSET + 1;

/// The width of the header's length prefix: a little-endian `u32`.
/// `MAX_HEADER_BYTES` bounds what it may hold.
const LENGTH_PREFIX_BYTES: usize = size_of::<u32>();

/// Where the encoded header starts: right after its length prefix. The
/// payload starts where the header ends.
const HEADER_OFFSET: u64 = LENGTH_OFFSET + LENGTH_PREFIX_BYTES as u64;

/// The largest encoded header a chunk file may carry, written or read. A
/// header is one data file path, one digest, and about twenty bytes per
/// stored chunk, one per row group, so a mebibyte holds tens of thousands
/// of row groups, more than any data file the layout targets has. The
/// reader checks the length prefix against this before it allocates, so a
/// corrupt prefix cannot ask for gigabytes.
const MAX_HEADER_BYTES: usize = 1024 * 1024;

/// The width of the digest a chunk file is named by and a footer is
/// identified by: the width of SHA-256, which `digest` computes.
pub const DIGEST_BYTES: usize = 32;

/// The digest the chunk file format uses, of a data file's footer for its
/// identity and of its path for the file name: SHA-256. The one place the
/// algorithm is named, so the loader that digests a footer and this module
/// that checks it agree.
pub fn digest(bytes: &[u8]) -> [u8; DIGEST_BYTES] {
    Sha256::digest(bytes).into()
}

/// What a chunk file is named by and validated against (RFD 017). The name
/// is the hex `digest` of `path`, so no path appears in a file name; the
/// header carries all three fields, and a chunk file whose header disagrees
/// with the identity the loader holds is stale and never reused.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DataFileIdentity {
    /// The data file's location as the manifest records it.
    pub path: String,
    /// The data file's size in bytes, as the manifest records it.
    pub size: u64,
    /// `digest` of the footer bytes the loader read. This module stores and
    /// compares it and never computes it.
    pub footer_digest: [u8; DIGEST_BYTES],
}

/// Why the local directory failed; the set is closed. The path the
/// operation was on is the `path` of `Error::Local`.
#[derive(Debug, thiserror::Error)]
pub enum LocalFault {
    /// The operation's I/O error.
    #[error("{0}")]
    Io(std::io::Error),
    /// A page read for a byte range no stored chunk contains whole: the
    /// disk analogue of `Segments` refusing a non-resident byte. Never a
    /// fetch, and never counted as a read.
    #[error("bytes {start}..{end} are in no stored chunk")]
    RangeNotResident {
        /// The first byte asked for.
        start: u64,
        /// One past the last byte asked for.
        end: u64,
    },
    /// A chunk handed to `write_chunks` whose bytes are not its range's
    /// length, an empty or inverted range included; the file would promise
    /// one and hold the other.
    #[error("chunk {start}..{end} came with {len} bytes")]
    ChunkLengthMismatch {
        /// The first byte of the data file the chunk claims to hold.
        start: u64,
        /// One past the last byte it claims to hold.
        end: u64,
        /// The bytes it came with.
        len: u64,
    },
    /// A chunk handed to `write_chunks` whose range overlaps the chunk
    /// before it in data order; a page in the overlap could come from
    /// either.
    #[error("chunk {start}..{end} overlaps the chunk before it")]
    ChunkOverlap {
        /// The first byte of the data file the chunk claims to hold.
        start: u64,
        /// One past the last byte it claims to hold.
        end: u64,
    },
    /// An encoded header longer than `MAX_HEADER_BYTES`, which no reader
    /// accepts.
    #[error("header of {len} bytes exceeds {MAX_HEADER_BYTES} bytes")]
    HeaderTooLarge {
        /// The encoded header's length.
        len: usize,
    },
    /// The blocking task that ran a page read was cancelled: the runtime
    /// shut down under it. A panic in that task is resumed on the caller
    /// instead, never turned into an error.
    #[error("page read task was cancelled: {0}")]
    Join(tokio::task::JoinError),
}

/// The check a chunk file failed. Not an error: the file is foreign, stale,
/// or incomplete, `chunks_for` treats it as absent, and `retain` deletes
/// it. Display gives the `snake_case` name the warn log carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Rejection {
    /// The first byte is not `CHUNK_FILE_MAGIC`: not an icetray chunk file.
    WrongMagic,
    /// Any version byte other than `CHUNK_FILE_VERSION`; this build reads
    /// exactly one layout.
    UnsupportedVersion {
        /// The version byte the file carries.
        version: u8,
    },
    /// The length prefix promises a header longer than `MAX_HEADER_BYTES`,
    /// which no writer produces. Checked before the file's length, so a
    /// corrupt prefix is named for what it is.
    HeaderTooLarge {
        /// The length the prefix promises.
        len: usize,
    },
    /// The file ends inside the envelope or before the end of the header
    /// its length prefix promises.
    TruncatedHeader,
    /// The header bytes do not decode as a `ChunkFileHeader`.
    UndecodableHeader,
    /// The header's footer digest is not `DIGEST_BYTES` long.
    DigestLength {
        /// The digest's length in the header.
        len: usize,
    },
    /// The header's spans are not what a page read's binary search needs:
    /// one is empty, starts before the end of the one before it, or ends
    /// past `u64::MAX`.
    SpanLayout,
    /// The payload is shorter than the spans promise.
    ShortPayload,
    /// The file goes on past the payload the spans promise: not a file this
    /// writer produced whole.
    TrailingBytes,
    /// The header's path is not the one looked up.
    PathMismatch,
    /// The header's size is not the one looked up.
    SizeMismatch,
    /// The header's footer digest is not the one looked up.
    DigestMismatch,
    /// The file's name is not the digest of the header's path, so no lookup
    /// ever finds it.
    NameMismatch,
}

impl fmt::Display for Rejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::WrongMagic => "wrong_magic",
            Self::UnsupportedVersion { .. } => "unsupported_version",
            Self::HeaderTooLarge { .. } => "header_too_large",
            Self::TruncatedHeader => "truncated_header",
            Self::UndecodableHeader => "undecodable_header",
            Self::DigestLength { .. } => "digest_length",
            Self::SpanLayout => "span_layout",
            Self::ShortPayload => "short_payload",
            Self::TrailingBytes => "trailing_bytes",
            Self::PathMismatch => "path_mismatch",
            Self::SizeMismatch => "size_mismatch",
            Self::DigestMismatch => "digest_mismatch",
            Self::NameMismatch => "name_mismatch",
        };
        f.write_str(name)
    }
}

/// Why a chunk file could not be parsed: the file system failed, or the
/// file failed a check. The two are handled apart: an I/O failure is an
/// error, a failed check makes the file absent.
#[derive(Debug)]
pub(crate) enum ParseFailure {
    /// The operation's I/O error.
    Io(io::Error),
    /// The check the file failed.
    Rejected(Rejection),
}

impl From<io::Error> for ParseFailure {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<Rejection> for ParseFailure {
    fn from(rejection: Rejection) -> Self {
        Self::Rejected(rejection)
    }
}

/// The header of a chunk file, inside the RFD 011 envelope: the identity of
/// the data file whose key chunks the payload holds, and one span per chunk
/// saying which bytes of that data file it is. The spans are what lets a
/// restarted process serve pages without the Parquet footer. Nothing
/// outside icetray reads a chunk file, so the message is declared here and
/// not in the client contract. Field numbers are never reused.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub(crate) struct ChunkFileHeader {
    /// The data file's location as the manifest records it.
    #[prost(string, tag = "1")]
    pub data_file_path: String,
    /// The data file's size in bytes, as the manifest records it.
    #[prost(uint64, tag = "2")]
    pub data_file_size: u64,
    /// SHA-256 of the footer bytes the loader read; `DIGEST_BYTES` long.
    #[prost(bytes = "vec", tag = "3")]
    pub footer_digest: Vec<u8>,
    /// The stored chunks in payload order: each non-empty, ascending by
    /// data offset, and disjoint. A reader rejects any other layout.
    #[prost(message, repeated, tag = "4")]
    pub chunks: Vec<ChunkSpan>,
}

/// One stored chunk: the byte range of the data file it holds. Its bytes
/// sit in the payload right after the bytes of every span before it.
#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub(crate) struct ChunkSpan {
    /// Where the chunk starts in the data file.
    #[prost(uint64, tag = "1")]
    pub data_offset: u64,
    /// The chunk's length in bytes.
    #[prost(uint64, tag = "2")]
    pub length: u64,
}

/// One stored chunk with where its bytes sit in the chunk file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct StoredSpan {
    /// Where the chunk starts in the data file.
    data_offset: u64,
    /// The chunk's length in bytes.
    length: u64,
    /// Where the chunk's bytes start in the chunk file.
    file_offset: u64,
}

/// A chunk file that passed every check of its envelope and header.
#[derive(Debug)]
pub(crate) struct ParsedChunkFile {
    /// The identity the header carries.
    identity: DataFileIdentity,
    /// The spans in header order with their payload offsets.
    spans: Vec<StoredSpan>,
    /// The payload's length: the sum of the spans.
    payload_bytes: u64,
}

/// The local directory, opened once per process and shared by every table
/// (RFD 017). `index/` holds chunk files and `cache/` the page cache's disk
/// level; both exist once `open` has returned. Everything in it is rebuilt
/// from object storage.
///
/// Every method is synchronous and does its file I/O on the caller's
/// thread; the caller decides whether to run one on a blocking thread. The
/// header reads that validate or sweep chunk files are not local reads in
/// the sense of RFD 003; only `DiskChunks::page` counts one.
#[derive(Debug)]
pub struct LocalDir {
    index_dir: PathBuf,
    cache_dir: PathBuf,
    index_budget: Option<u64>,
    page_cache_disk_bytes: u64,
    stats: Arc<IoStats>,
    /// The temporary files `write_chunks` calls in this process have open.
    /// `retain` keeps these and deletes every other temporary file, so a
    /// sweep never removes a write in progress from under its writer.
    in_flight: Mutex<HashSet<PathBuf>>,
    /// The sequence number of the next temporary file name.
    next_temp: AtomicU64,
}

impl LocalDir {
    /// Opens the directory, creating `index/` and `cache/` under it when
    /// they are absent. A directory that cannot be created is
    /// `Error::Local` on that directory.
    pub fn open(config: &LocalConfig, stats: Arc<IoStats>) -> Result<Self> {
        let index_dir = config.dir.join(INDEX_DIR);
        let cache_dir = config.dir.join(CACHE_DIR);
        for dir in [&index_dir, &cache_dir] {
            fs::create_dir_all(dir).map_err(|error| local_error(dir.clone(), error))?;
        }
        Ok(Self {
            index_dir,
            cache_dir,
            index_budget: config.index_budget_bytes,
            page_cache_disk_bytes: config.page_cache_disk_bytes,
            stats,
            in_flight: Mutex::new(HashSet::new()),
            next_temp: AtomicU64::new(0),
        })
    }

    /// The chunk file for a data file, identified by path, size, and footer
    /// digest; `None` if absent or stale. A file that fails a check of its
    /// envelope, header, or identity is logged at warn with the check it
    /// failed and treated as absent; `retain` deletes it later. Only an I/O
    /// failure is an error.
    pub fn chunks_for(&self, identity: &DataFileIdentity) -> Result<Option<DiskChunks>> {
        let path = self.chunk_path(identity);
        let file = match File::open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(local_error(path, error)),
        };
        match validate(&file, identity) {
            Ok(parsed) => Ok(Some(DiskChunks::new(
                file,
                path,
                parsed,
                Arc::clone(&self.stats),
            ))),
            Err(ParseFailure::Rejected(rejection)) => {
                warn_rejected(&path, rejection);
                Ok(None)
            }
            Err(ParseFailure::Io(error)) => Err(local_error(path, error)),
        }
    }

    /// Writes verified key chunks and returns the handle the index keeps.
    /// The header names every span before the first payload byte, so the
    /// chunks are collected first and sorted by data offset; one file's
    /// chunks is the disk tier's stated load peak (RFD 017). The file is
    /// written to a temporary name unique to this call, synced, and renamed
    /// over `index/<name>`, so a reader never sees a partial file and two
    /// writers of one identity never share a temporary file; the handle
    /// returned is over the file this call wrote, whichever writer's rename
    /// lands last. The directory itself is not synced: durability across a
    /// power loss is not promised, since the directory is disposable and a
    /// lost rename costs one re-fetch. Every chunk's bytes must be its
    /// range's length and no two chunks may overlap.
    pub fn write_chunks(
        &self,
        identity: &DataFileIdentity,
        chunks: impl Iterator<Item = (Range<u64>, Bytes)>,
    ) -> Result<DiskChunks> {
        let path = self.chunk_path(identity);
        let mut chunks: Vec<(Range<u64>, Bytes)> = chunks.collect();
        chunks.sort_by_key(|(range, _)| range.start);
        let header = header_for(identity, &chunks).map_err(|fault| Error::Local {
            path: path.clone(),
            fault,
        })?;
        let temp = self.begin_temp(&path);
        let file = write_envelope(&temp.path, &header, &chunks).map_err(|fault| Error::Local {
            path: temp.path.clone(),
            fault,
        })?;
        fs::rename(&temp.path, &path).map_err(|error| local_error(path.clone(), error))?;
        drop(temp);
        let encoded_len = u64::try_from(header.encoded_len())
            .expect("an encoded header's length fits u64 on the supported systems");
        let (spans, payload_bytes) = stored_spans(&header.chunks, HEADER_OFFSET + encoded_len)
            .expect("header_for admits only non-empty, ascending, disjoint spans");
        let parsed = ParsedChunkFile {
            identity: identity.clone(),
            spans,
            payload_bytes,
        };
        Ok(DiskChunks::new(file, path, parsed, Arc::clone(&self.stats)))
    }

    /// Deletes every chunk file no current generation references and every
    /// temporary file no `write_chunks` in this process has open, and
    /// returns the bytes freed. A chunk file is kept only when it is
    /// complete, passes every check, is named by its own header's path, and
    /// its identity is in `live`; such a file is never touched. The
    /// directory is shared by every table, so `live` is the union over every
    /// table of every identity its serving generation and any load in
    /// progress reference; the caller that owns all the tables builds it. A
    /// file that is gone by the time it is reached was freed by someone else
    /// and counts nothing.
    pub fn retain(&self, live: &HashSet<DataFileIdentity>) -> Result<u64> {
        let mut freed = 0;
        for (path, metadata) in self.index_files()? {
            if self.keeps(&path, live)? {
                continue;
            }
            freed += remove_if_present(&path, metadata.len())?;
        }
        Ok(freed)
    }

    /// The bytes every file under `index/` occupies, a write in progress
    /// included, since the budget is disk. `generation` compares this and
    /// `index_budget` with the bytes a disk-tier load needs, at the footer
    /// stage (RFD 017); this module refuses nothing itself.
    pub fn index_bytes_in_use(&self) -> Result<u64> {
        Ok(self
            .index_files()?
            .iter()
            .map(|(_, metadata)| metadata.len())
            .sum())
    }

    /// The bytes chunk files may occupy across every table, from
    /// `LocalConfig::index_budget_bytes`; `None` is unbounded.
    pub fn index_budget(&self) -> Option<u64> {
        self.index_budget
    }

    /// The page cache's disk level, or `None` when
    /// `LocalConfig::page_cache_disk_bytes` is zero and there is no level.
    /// A level is at least one block, `contract::PAGE_CACHE_DISK_BLOCK_BYTES`,
    /// which `Config::parse` checks: below that the level has no block to
    /// write into and would hold nothing.
    pub fn page_tier(&self) -> Option<DiskTier> {
        (self.page_cache_disk_bytes > 0).then(|| DiskTier {
            dir: self.cache_dir.clone(),
            capacity_bytes: self.page_cache_disk_bytes,
            stats: Arc::clone(&self.stats),
        })
    }

    /// The final path of the chunk file for `identity`.
    fn chunk_path(&self, identity: &DataFileIdentity) -> PathBuf {
        self.index_dir.join(chunk_name(&identity.path))
    }

    /// Every regular file directly under `index/` with its metadata.
    /// Subdirectories and links are nothing this module wrote and are left
    /// alone, and so is an entry renamed or deleted between the listing and
    /// its stat, which a concurrent `write_chunks` or `retain` does.
    fn index_files(&self) -> Result<Vec<(PathBuf, Metadata)>> {
        let entries = fs::read_dir(&self.index_dir)
            .map_err(|error| local_error(self.index_dir.clone(), error))?;
        let mut files = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|error| local_error(self.index_dir.clone(), error))?;
            let path = entry.path();
            let metadata = match entry.metadata() {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(local_error(path, error)),
            };
            if metadata.is_file() {
                files.push((path, metadata));
            }
        }
        Ok(files)
    }

    /// Whether `retain` keeps a file under `index/`: a temporary file is
    /// kept while a `write_chunks` in this process has it open; a chunk file
    /// is kept when it passes every check, its name is its header's path
    /// digest, and its identity is in `live`. A file already gone is not
    /// kept, and deleting it then frees nothing.
    fn keeps(&self, path: &Path, live: &HashSet<DataFileIdentity>) -> Result<bool> {
        if is_temp(path) {
            return Ok(self.lock_in_flight().contains(path));
        }
        let file = match File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(local_error(path.to_path_buf(), error)),
        };
        let parsed = match parse_chunk_file(&file) {
            Ok(parsed) => parsed,
            Err(ParseFailure::Rejected(rejection)) => {
                warn_rejected(path, rejection);
                return Ok(false);
            }
            Err(ParseFailure::Io(error)) => return Err(local_error(path.to_path_buf(), error)),
        };
        if path != self.chunk_path(&parsed.identity) {
            warn_rejected(path, Rejection::NameMismatch);
            return Ok(false);
        }
        Ok(live.contains(&parsed.identity))
    }

    /// A temporary path beside `path` for one `write_chunks` call,
    /// registered in `in_flight` until the guard drops. The name carries the
    /// process id and a sequence number, so two writers of one identity
    /// never share a temporary file and a leftover of a dead process never
    /// collides with a live write.
    fn begin_temp(&self, path: &Path) -> InFlight<'_> {
        let sequence = self.next_temp.fetch_add(1, Ordering::Relaxed);
        let mut name = path.as_os_str().to_owned();
        name.push(format!(
            ".{}-{sequence}.{TEMP_EXTENSION}",
            std::process::id()
        ));
        let path = PathBuf::from(name);
        self.lock_in_flight().insert(path.clone());
        InFlight { dir: self, path }
    }

    /// The registry of temporary files being written. A panic while it was
    /// held leaves the set whole (an insert or remove of one path either
    /// happened or did not), so poisoning is recovered from rather than
    /// propagated.
    fn lock_in_flight(&self) -> MutexGuard<'_, HashSet<PathBuf>> {
        self.in_flight
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

/// A temporary file one `write_chunks` call has open, held in
/// `LocalDir::in_flight` so `retain` skips it; unregistered when dropped,
/// whether the write succeeded or not. A file whose write failed stays on
/// disk unregistered and the next `retain` deletes it.
struct InFlight<'a> {
    dir: &'a LocalDir,
    path: PathBuf,
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.dir.lock_in_flight().remove(&self.path);
    }
}

/// The key chunks of one data file, read from its chunk file on demand.
/// Clone-cheap: one handle is shared by the index and every read in
/// flight. It is not a `ChunkReader`; the index wraps the bytes a page read
/// returns in a `Segments`.
#[derive(Clone, Debug)]
pub struct DiskChunks {
    shared: Arc<ChunkFile>,
}

/// What every clone of a `DiskChunks` shares: the open file, the span
/// table, and the counter.
#[derive(Debug)]
struct ChunkFile {
    /// Read only through positioned reads, so the handle has no cursor to
    /// share and is never mapped.
    file: File,
    /// For errors.
    path: PathBuf,
    /// Ascending by data offset, so a page's span is found by binary search.
    spans: Vec<StoredSpan>,
    /// The payload's length: the sum of the spans.
    payload_bytes: u64,
    stats: Arc<IoStats>,
}

impl DiskChunks {
    fn new(file: File, path: PathBuf, parsed: ParsedChunkFile, stats: Arc<IoStats>) -> Self {
        Self {
            shared: Arc::new(ChunkFile {
                file,
                path,
                spans: parsed.spans,
                payload_bytes: parsed.payload_bytes,
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

    /// The payload's length: what the chunk file costs the index budget
    /// beyond its header.
    pub fn bytes_on_disk(&self) -> u64 {
        self.shared.payload_bytes
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
        let len = usize::try_from(len).expect(
            "a span's length is bounded by the chunk file's size, which fits usize on the supported 64-bit systems",
        );
        let mut buf = vec![0u8; len];
        self.file
            .read_exact_at(&mut buf, offset)
            .map_err(|error| self.error(LocalFault::Io(error)))?;
        Ok(Bytes::from(buf))
    }

    fn error(&self, fault: LocalFault) -> Error {
        Error::Local {
            path: self.path.clone(),
            fault,
        }
    }
}

/// The page cache's disk level (RFD 017): the `cache/` directory, its size
/// bound, and the counter a page served from it is charged to, handed to
/// `PageCache` by `LocalDir::page_tier`. Pages there are keyed by file and
/// offset and evicted by size alone, never for staleness, since a data
/// file never changes.
#[derive(Clone, Debug)]
pub struct DiskTier {
    dir: PathBuf,
    capacity_bytes: u64,
    stats: Arc<IoStats>,
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

/// Reads the envelope and header of an open chunk file, then checks the
/// header's identity against `identity`. The order of the checks is the
/// order of the `Rejection` variants.
pub(crate) fn validate(
    file: &File,
    identity: &DataFileIdentity,
) -> std::result::Result<ParsedChunkFile, ParseFailure> {
    let parsed = parse_chunk_file(file)?;
    check_identity(&parsed.identity, identity)?;
    Ok(parsed)
}

/// Reads the envelope and header of an open chunk file and checks that the
/// payload is as long as the spans promise. Needs no identity, so `retain`
/// can read the one a file carries.
fn parse_chunk_file(file: &File) -> std::result::Result<ParsedChunkFile, ParseFailure> {
    let mut magic = [0u8; 1];
    read_header_bytes(file, &mut magic, MAGIC_OFFSET)?;
    if magic[0] != CHUNK_FILE_MAGIC {
        return Err(Rejection::WrongMagic.into());
    }
    let mut version = [0u8; 1];
    read_header_bytes(file, &mut version, VERSION_OFFSET)?;
    if version[0] != CHUNK_FILE_VERSION {
        return Err(Rejection::UnsupportedVersion {
            version: version[0],
        }
        .into());
    }
    let mut length = [0u8; LENGTH_PREFIX_BYTES];
    read_header_bytes(file, &mut length, LENGTH_OFFSET)?;
    let header_len = u32::from_le_bytes(length);
    let payload_offset = HEADER_OFFSET + u64::from(header_len);
    let header_len = usize::try_from(header_len)
        .expect("a u32 header length fits usize on the supported 64-bit systems");
    if header_len > MAX_HEADER_BYTES {
        return Err(Rejection::HeaderTooLarge { len: header_len }.into());
    }
    let file_len = file.metadata()?.len();
    if file_len < payload_offset {
        return Err(Rejection::TruncatedHeader.into());
    }
    let mut encoded = vec![0u8; header_len];
    read_header_bytes(file, &mut encoded, HEADER_OFFSET)?;
    let header = match ChunkFileHeader::decode(encoded.as_slice()) {
        Ok(header) => header,
        Err(error) => {
            tracing::debug!(%error, "chunk file header does not decode");
            return Err(Rejection::UndecodableHeader.into());
        }
    };
    let footer_digest =
        <[u8; DIGEST_BYTES]>::try_from(header.footer_digest.as_slice()).map_err(|_| {
            Rejection::DigestLength {
                len: header.footer_digest.len(),
            }
        })?;
    let (spans, payload_bytes) =
        stored_spans(&header.chunks, payload_offset).ok_or(Rejection::SpanLayout)?;
    match file_len.cmp(&(payload_offset + payload_bytes)) {
        std::cmp::Ordering::Less => return Err(Rejection::ShortPayload.into()),
        std::cmp::Ordering::Greater => return Err(Rejection::TrailingBytes.into()),
        std::cmp::Ordering::Equal => {}
    }
    Ok(ParsedChunkFile {
        identity: DataFileIdentity {
            path: header.data_file_path,
            size: header.data_file_size,
            footer_digest,
        },
        spans,
        payload_bytes,
    })
}

/// A positioned read inside the envelope or header; a file that ends inside
/// the bytes asked for is `Rejection::TruncatedHeader`.
fn read_header_bytes(
    file: &File,
    buf: &mut [u8],
    offset: u64,
) -> std::result::Result<(), ParseFailure> {
    match file.read_exact_at(buf, offset) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
            Err(Rejection::TruncatedHeader.into())
        }
        Err(error) => Err(error.into()),
    }
}

/// The spans with their offsets in the chunk file, laid out back to back
/// from `payload_offset`, and the payload's total length. `None` when the
/// spans are not what `ChunkFile::locate` needs, non-empty, ascending, and
/// disjoint, or when they do not sum within `u64`, which no file can hold.
fn stored_spans(chunks: &[ChunkSpan], payload_offset: u64) -> Option<(Vec<StoredSpan>, u64)> {
    let mut spans = Vec::with_capacity(chunks.len());
    let mut file_offset = payload_offset;
    let mut data_end = 0;
    for chunk in chunks {
        data_end = span_end(data_end, *chunk)?;
        spans.push(StoredSpan {
            data_offset: chunk.data_offset,
            length: chunk.length,
            file_offset,
        });
        file_offset = file_offset.checked_add(chunk.length)?;
    }
    Some((spans, file_offset - payload_offset))
}

/// Where `span` ends in the data file, when it may follow spans ending at
/// `previous_end`: it holds at least one byte, starts at or after
/// `previous_end`, and ends within `u64`. `None` otherwise. The one
/// statement of the layout the reader checks and the writer keeps.
fn span_end(previous_end: u64, span: ChunkSpan) -> Option<u64> {
    if span.length == 0 || span.data_offset < previous_end {
        return None;
    }
    span.data_offset.checked_add(span.length)
}

/// The first field of `found` that differs from `expected`, as the check it
/// fails: path, then size, then digest.
fn check_identity(
    found: &DataFileIdentity,
    expected: &DataFileIdentity,
) -> std::result::Result<(), Rejection> {
    if found.path != expected.path {
        return Err(Rejection::PathMismatch);
    }
    if found.size != expected.size {
        return Err(Rejection::SizeMismatch);
    }
    if found.footer_digest != expected.footer_digest {
        return Err(Rejection::DigestMismatch);
    }
    Ok(())
}

/// The header for `identity` over `chunks`, already sorted by data offset.
/// A chunk whose bytes are not its range's length, an empty or inverted
/// range included, is `LocalFault::ChunkLengthMismatch`; one whose range
/// overlaps the chunk before it is `LocalFault::ChunkOverlap`.
fn header_for(
    identity: &DataFileIdentity,
    chunks: &[(Range<u64>, Bytes)],
) -> std::result::Result<ChunkFileHeader, LocalFault> {
    let mut spans = Vec::with_capacity(chunks.len());
    let mut data_end = 0;
    for (range, bytes) in chunks {
        let len = u64::try_from(bytes.len())
            .expect("a buffer's length fits u64 on the supported systems");
        let range_len = range.end.checked_sub(range.start).filter(|len| *len > 0);
        if range_len != Some(len) {
            return Err(LocalFault::ChunkLengthMismatch {
                start: range.start,
                end: range.end,
                len,
            });
        }
        let span = ChunkSpan {
            data_offset: range.start,
            length: len,
        };
        data_end = span_end(data_end, span).ok_or(LocalFault::ChunkOverlap {
            start: range.start,
            end: range.end,
        })?;
        spans.push(span);
    }
    Ok(header_of(identity, spans))
}

/// The header naming `identity` and `spans`, the one layout `write_chunks`
/// writes and `chunk_file_bytes` measures.
fn header_of(identity: &DataFileIdentity, spans: Vec<ChunkSpan>) -> ChunkFileHeader {
    ChunkFileHeader {
        data_file_path: identity.path.clone(),
        data_file_size: identity.size,
        footer_digest: identity.footer_digest.to_vec(),
        chunks: spans,
    }
}

/// The bytes the chunk file `write_chunks` would write for `identity` over
/// `ranges` occupies: the envelope, the encoded header, and the chunks back
/// to back. This is what `LocalDir::index_bytes_in_use` measures for the
/// file once written, so a disk budget checked against it at the footer
/// stage agrees with the directory after the write.
pub(crate) fn chunk_file_bytes(identity: &DataFileIdentity, ranges: &[Range<u64>]) -> u64 {
    let spans = ranges
        .iter()
        .map(|range| ChunkSpan {
            data_offset: range.start,
            length: range.end.saturating_sub(range.start),
        })
        .collect::<Vec<_>>();
    let payload = spans.iter().map(|span| span.length).sum::<u64>();
    let header = u64::try_from(header_of(identity, spans).encoded_len())
        .expect("an encoded header's length fits u64 on the supported systems");
    HEADER_OFFSET + header + payload
}

/// Writes one chunk file at `path`, which must not exist: the magic byte,
/// the version byte, the little-endian `u32` length of the encoded header,
/// the header, then every chunk's bytes back to back in `chunks` order,
/// synced to disk before it returns. `chunks` is in the header's span
/// order. Returns the handle it wrote through, open for reading, so the
/// caller reads the file this call wrote and not whatever later lands at
/// its name.
fn write_envelope(
    path: &Path,
    header: &ChunkFileHeader,
    chunks: &[(Range<u64>, Bytes)],
) -> std::result::Result<File, LocalFault> {
    let encoded = header.encode_to_vec();
    let len = encoded.len();
    let header_len = u32::try_from(len)
        .ok()
        .filter(|_| len <= MAX_HEADER_BYTES)
        .ok_or(LocalFault::HeaderTooLarge { len })?;
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(LocalFault::Io)?;
    file.write_all(&[CHUNK_FILE_MAGIC, CHUNK_FILE_VERSION])
        .and_then(|()| file.write_all(&header_len.to_le_bytes()))
        .and_then(|()| file.write_all(&encoded))
        .map_err(LocalFault::Io)?;
    for (_, bytes) in chunks {
        file.write_all(bytes).map_err(LocalFault::Io)?;
    }
    file.sync_all().map_err(LocalFault::Io)?;
    Ok(file)
}

/// The chunk file name for a data file path: the lowercase hex `digest` of
/// the path's bytes, so no path appears in a file name.
fn chunk_name(path: &str) -> String {
    hex(&digest(path.as_bytes()))
}

/// Lowercase hex, `HEX_DIGITS_PER_BYTE` per byte.
fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * HEX_DIGITS_PER_BYTE);
    for byte in bytes {
        write!(out, "{byte:0HEX_DIGITS_PER_BYTE$x}").expect("a String accepts every write");
    }
    out
}

/// Whether a file under `index/` is a write in progress or left behind.
fn is_temp(path: &Path) -> bool {
    path.extension()
        .is_some_and(|extension| extension == TEMP_EXTENSION)
}

/// Deletes the file at `path` and returns `len`, the bytes that freed; a
/// file already gone was freed by someone else and returns zero.
fn remove_if_present(path: &Path, len: u64) -> Result<u64> {
    match fs::remove_file(path) {
        Ok(()) => Ok(len),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(local_error(path.to_path_buf(), error)),
    }
}

/// An I/O failure at `path` as the crate's error.
fn local_error(path: PathBuf, error: io::Error) -> Error {
    Error::Local {
        path,
        fault: LocalFault::Io(error),
    }
}

/// The one warn line for a chunk file that failed a check: the path, the
/// check, and the check's fields, so a reader knows which file to look at,
/// why it was skipped, and the value that failed.
fn warn_rejected(path: &Path, rejection: Rejection) {
    tracing::warn!(
        path = %path.display(),
        check = %rejection,
        detail = ?rejection,
        "chunk file rejected"
    );
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;

    use super::*;

    /// A synthetic data file path; the chunk file's name is its digest.
    const DATA_PATH: &str = "s3://example-warehouse/db/kv/data/part-00000.parquet";
    /// The final component of `DATA_PATH`, which no hex name can contain.
    const DATA_FILE_NAME: &str = "part-00000.parquet";
    /// A second synthetic path, so a second chunk file has another name.
    const OTHER_PATH: &str = "s3://example-warehouse/db/kv/data/part-00001.parquet";
    /// The size the fixture identity records: a contract-sized data file.
    const DATA_SIZE: u64 = 40_000_000;
    /// One byte more than `DATA_SIZE`: the smallest change that makes an
    /// identity stale.
    const OTHER_SIZE: u64 = DATA_SIZE + 1;
    /// The footer digest the fixture identity carries; any 32 bytes.
    const FOOTER_DIGEST: [u8; DIGEST_BYTES] = [7; DIGEST_BYTES];
    /// A footer digest the fixture identity does not carry.
    const OTHER_DIGEST: [u8; DIGEST_BYTES] = [9; DIGEST_BYTES];
    /// The first stored chunk's range in the data file.
    const FIRST_RANGE: Range<u64> = 100..106;
    /// The first stored chunk's bytes.
    const FIRST_BYTES: &[u8] = b"abcdef";
    /// The second stored chunk's range, with a gap after the first.
    const SECOND_RANGE: Range<u64> = 200..204;
    /// The second stored chunk's bytes.
    const SECOND_BYTES: &[u8] = b"wxyz";
    /// A range strictly inside the first chunk.
    const INNER_RANGE: Range<u64> = 102..105;
    /// The bytes `INNER_RANGE` covers.
    const INNER_BYTES: &[u8] = b"cde";
    /// A range that starts in the first chunk and ends in the second.
    const STRADDLING_RANGE: Range<u64> = 104..202;
    /// A range before every chunk.
    const OUTSIDE_RANGE: Range<u64> = 0..3;
    /// A range in the gap between the chunks.
    const GAP_RANGE: Range<u64> = 106..200;
    /// A page cache disk bound above zero, so `page_tier` is `Some`.
    const DISK_CACHE_BYTES: u64 = 1024;
    /// What a leftover temporary file holds.
    const LEFTOVER: &[u8] = b"partial";
    /// A digest of the wrong width, three bytes.
    const SHORT_DIGEST: [u8; 3] = [1, 2, 3];
    /// A byte that is not a valid protobuf key and cannot start a header.
    const NOT_PROTOBUF: u8 = 0xFF;
    /// A length prefix of all ones: the largest a `u32` can promise.
    const HUGE_LENGTH_PREFIX: [u8; LENGTH_PREFIX_BYTES] = [0xFF; LENGTH_PREFIX_BYTES];
    /// The bytes a second writer of the same identity stores at
    /// `FIRST_RANGE`, distinguishable from `FIRST_BYTES`.
    const RIVAL_BYTES: &[u8] = b"ABCDEF";
    /// A range that overlaps `FIRST_RANGE` from inside it.
    const OVERLAPPING_RANGE: Range<u64> = 104..108;
    /// A range one byte longer than the first chunk.
    const PAST_FIRST_END: Range<u64> = 100..107;
    /// The last byte of the first chunk alone.
    const LAST_OF_FIRST: Range<u64> = 105..106;
    /// The empty range at the first chunk's end.
    const EMPTY_AT_FIRST_END: Range<u64> = 106..106;
    /// A byte appended after the payload.
    const EXTRA: &[u8] = b"!";

    fn identity() -> DataFileIdentity {
        DataFileIdentity {
            path: DATA_PATH.to_string(),
            size: DATA_SIZE,
            footer_digest: FOOTER_DIGEST,
        }
    }

    /// `INNER_RANGE` with its ends swapped: inside the first chunk, but
    /// asking for nothing a span can hold.
    fn inverted_range() -> Range<u64> {
        Range {
            start: INNER_RANGE.end,
            end: INNER_RANGE.start,
        }
    }

    fn config(dir: &Path, page_cache_disk_bytes: u64) -> LocalConfig {
        LocalConfig {
            dir: dir.to_path_buf(),
            index_budget_bytes: None,
            page_cache_disk_bytes,
        }
    }

    /// A local directory over `dir` with a fresh counter.
    fn open(dir: &Path) -> (LocalDir, Arc<IoStats>) {
        let stats = Arc::new(IoStats::default());
        let local = LocalDir::open(&config(dir, DISK_CACHE_BYTES), Arc::clone(&stats)).unwrap();
        (local, stats)
    }

    fn chunks() -> Vec<(Range<u64>, Bytes)> {
        vec![
            (FIRST_RANGE, Bytes::from_static(FIRST_BYTES)),
            (SECOND_RANGE, Bytes::from_static(SECOND_BYTES)),
        ]
    }

    fn payload_len() -> u64 {
        u64::try_from(FIRST_BYTES.len() + SECOND_BYTES.len()).unwrap()
    }

    fn range_len(range: &Range<u64>) -> u64 {
        range.end - range.start
    }

    fn at(offset: u64) -> usize {
        usize::try_from(offset).unwrap()
    }

    /// Writes the fixture chunks for the fixture identity.
    fn write(local: &LocalDir) -> DiskChunks {
        local
            .write_chunks(&identity(), chunks().into_iter())
            .unwrap()
    }

    fn chunk_file(local: &LocalDir) -> PathBuf {
        local.chunk_path(&identity())
    }

    /// The path of a temporary file no writer in this process has open:
    /// what a process that died mid-write leaves behind.
    fn stale_temp(path: &Path) -> PathBuf {
        path.with_extension(TEMP_EXTENSION)
    }

    fn append(path: &Path, bytes: &[u8]) {
        OpenOptions::new()
            .append(true)
            .open(path)
            .unwrap()
            .write_all(bytes)
            .unwrap();
    }

    fn header_len(path: &Path) -> u32 {
        let bytes = fs::read(path).unwrap();
        u32::from_le_bytes(
            bytes[at(LENGTH_OFFSET)..at(HEADER_OFFSET)]
                .try_into()
                .unwrap(),
        )
    }

    fn overwrite(path: &Path, offset: u64, bytes: &[u8]) {
        OpenOptions::new()
            .write(true)
            .open(path)
            .unwrap()
            .write_all_at(bytes, offset)
            .unwrap();
    }

    fn truncate(path: &Path, len: u64) {
        OpenOptions::new()
            .write(true)
            .open(path)
            .unwrap()
            .set_len(len)
            .unwrap();
    }

    /// The check a file fails for `identity`.
    fn rejection_of(path: &Path, identity: &DataFileIdentity) -> Rejection {
        let result = validate(&File::open(path).unwrap(), identity);
        let Err(ParseFailure::Rejected(rejection)) = result else {
            panic!("expected a rejected chunk file, got {result:?}");
        };
        rejection
    }

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

    fn entries(dir: &Path) -> Vec<PathBuf> {
        let mut entries: Vec<PathBuf> = fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        entries.sort();
        entries
    }

    #[test]
    fn test_open_creates_index_and_cache_dirs() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        assert!(dir.path().join(INDEX_DIR).is_dir());
        assert!(dir.path().join(CACHE_DIR).is_dir());
        assert_eq!(local.index_dir, dir.path().join(INDEX_DIR));
        assert_eq!(local.cache_dir, dir.path().join(CACHE_DIR));
        assert_eq!(local.index_budget(), None);
        let again = LocalDir::open(
            &config(dir.path(), DISK_CACHE_BYTES),
            Arc::new(IoStats::default()),
        );
        assert!(again.is_ok(), "existing directories are reused");
    }

    #[test]
    fn test_open_fails_typed_when_a_dir_cannot_be_created() {
        let dir = tempfile::tempdir().unwrap();
        let blocker = dir.path().join("blocker");
        fs::write(&blocker, LEFTOVER).unwrap();
        let result = LocalDir::open(
            &config(&blocker, DISK_CACHE_BYTES),
            Arc::new(IoStats::default()),
        );
        let Err(Error::Local {
            path,
            fault: LocalFault::Io(_),
        }) = result
        else {
            panic!("expected a local I/O error, got {result:?}");
        };
        assert_eq!(path, blocker.join(INDEX_DIR));
    }

    #[test]
    fn test_write_chunks_then_chunks_for_returns_some() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        let written = write(&local);
        assert_eq!(written.bytes_on_disk(), payload_len());
        let found = local.chunks_for(&identity()).unwrap().unwrap();
        assert_eq!(found.bytes_on_disk(), payload_len());
        assert_eq!(found.shared.spans, written.shared.spans);
        assert_eq!(found.shared.spans.len(), 2);
        assert_eq!(entries(&local.index_dir), vec![chunk_file(&local)]);
    }

    #[test]
    fn test_chunks_for_absent_is_none() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        assert!(local.chunks_for(&identity()).unwrap().is_none());
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

    #[test]
    fn test_chunks_for_stale_identity_is_none() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        write(&local);
        let path = chunk_file(&local);
        let cases = [
            (
                DataFileIdentity {
                    size: OTHER_SIZE,
                    ..identity()
                },
                Rejection::SizeMismatch,
            ),
            (
                DataFileIdentity {
                    footer_digest: OTHER_DIGEST,
                    ..identity()
                },
                Rejection::DigestMismatch,
            ),
            (
                DataFileIdentity {
                    path: OTHER_PATH.to_string(),
                    ..identity()
                },
                Rejection::PathMismatch,
            ),
        ];
        for (stale, expected) in cases {
            assert_eq!(rejection_of(&path, &stale), expected);
            assert!(local.chunks_for(&stale).unwrap().is_none());
        }
        assert!(local.chunks_for(&identity()).unwrap().is_some());
    }

    #[test]
    fn test_validate_wrong_magic() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        write(&local);
        let path = chunk_file(&local);
        overwrite(&path, MAGIC_OFFSET, &[CHUNK_FILE_MAGIC.wrapping_add(1)]);
        assert_eq!(rejection_of(&path, &identity()), Rejection::WrongMagic);
        assert!(local.chunks_for(&identity()).unwrap().is_none());
    }

    #[test]
    fn test_validate_other_version() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        write(&local);
        let path = chunk_file(&local);
        for version in [CHUNK_FILE_VERSION + 1, 0] {
            overwrite(&path, VERSION_OFFSET, &[version]);
            assert_eq!(
                rejection_of(&path, &identity()),
                Rejection::UnsupportedVersion { version }
            );
            assert!(local.chunks_for(&identity()).unwrap().is_none());
        }
    }

    #[test]
    fn test_validate_empty_file_is_a_truncated_header() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        let path = chunk_file(&local);
        fs::write(&path, []).unwrap();
        assert_eq!(rejection_of(&path, &identity()), Rejection::TruncatedHeader);
        assert!(local.chunks_for(&identity()).unwrap().is_none());
        assert_eq!(local.retain(&HashSet::new()).unwrap(), 0);
        assert!(entries(&local.index_dir).is_empty());
    }

    #[test]
    fn test_validate_length_prefix_over_the_cap_is_rejected_before_the_file_is_read() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        let path = chunk_file(&local);
        let envelope = [
            &[CHUNK_FILE_MAGIC, CHUNK_FILE_VERSION][..],
            &HUGE_LENGTH_PREFIX,
        ]
        .concat();
        fs::write(&path, &envelope).unwrap();
        assert_eq!(
            rejection_of(&path, &identity()),
            Rejection::HeaderTooLarge {
                len: usize::try_from(u32::MAX).unwrap()
            }
        );
        assert!(local.chunks_for(&identity()).unwrap().is_none());
        let freed = local.retain(&HashSet::new()).unwrap();
        assert_eq!(freed, u64::try_from(envelope.len()).unwrap());
        assert!(entries(&local.index_dir).is_empty());
    }

    #[test]
    fn test_validate_length_prefix_past_the_file_end_is_truncated() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        let path = chunk_file(&local);
        let prefix = u32::try_from(MAX_HEADER_BYTES).unwrap().to_le_bytes();
        let envelope = [&[CHUNK_FILE_MAGIC, CHUNK_FILE_VERSION][..], &prefix].concat();
        fs::write(&path, &envelope).unwrap();
        assert_eq!(rejection_of(&path, &identity()), Rejection::TruncatedHeader);
        assert!(local.chunks_for(&identity()).unwrap().is_none());
    }

    #[test]
    fn test_validate_truncated_header() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        write(&local);
        let path = chunk_file(&local);
        // Inside the header: the length prefix promises more than is there.
        truncate(&path, HEADER_OFFSET + 1);
        assert_eq!(rejection_of(&path, &identity()), Rejection::TruncatedHeader);
        assert!(local.chunks_for(&identity()).unwrap().is_none());
        // Inside the envelope: not even a version byte.
        truncate(&path, VERSION_OFFSET);
        assert_eq!(rejection_of(&path, &identity()), Rejection::TruncatedHeader);
        assert!(local.chunks_for(&identity()).unwrap().is_none());
    }

    #[test]
    fn test_validate_undecodable_header() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        write(&local);
        let path = chunk_file(&local);
        let garbage = vec![NOT_PROTOBUF; at(u64::from(header_len(&path)))];
        overwrite(&path, HEADER_OFFSET, &garbage);
        assert_eq!(
            rejection_of(&path, &identity()),
            Rejection::UndecodableHeader
        );
        assert!(local.chunks_for(&identity()).unwrap().is_none());
    }

    #[test]
    fn test_validate_short_payload() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        write(&local);
        let path = chunk_file(&local);
        let len = fs::metadata(&path).unwrap().len();
        truncate(&path, len - 1);
        assert_eq!(rejection_of(&path, &identity()), Rejection::ShortPayload);
        assert!(local.chunks_for(&identity()).unwrap().is_none());
    }

    #[test]
    fn test_validate_trailing_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        write(&local);
        let path = chunk_file(&local);
        append(&path, EXTRA);
        assert_eq!(rejection_of(&path, &identity()), Rejection::TrailingBytes);
        assert!(local.chunks_for(&identity()).unwrap().is_none());
    }

    #[test]
    fn test_validate_span_layout() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        let path = chunk_file(&local);
        let mut swapped = header_for(&identity(), &chunks()).unwrap();
        swapped.chunks.reverse();
        let mut overlapping = header_for(&identity(), &chunks()).unwrap();
        overlapping.chunks[1].data_offset = OVERLAPPING_RANGE.start;
        let mut empty = header_for(&identity(), &chunks()).unwrap();
        empty.chunks[0].length = 0;
        let mut past_the_end = header_for(&identity(), &chunks()).unwrap();
        past_the_end.chunks[1].data_offset = u64::MAX - 1;
        for header in [swapped, overlapping, empty, past_the_end] {
            write_envelope(&path, &header, &chunks()).unwrap();
            assert_eq!(rejection_of(&path, &identity()), Rejection::SpanLayout);
            assert!(local.chunks_for(&identity()).unwrap().is_none());
            fs::remove_file(&path).unwrap();
        }
    }

    #[test]
    fn test_validate_digest_length() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        let path = chunk_file(&local);
        let mut header = header_for(&identity(), &chunks()).unwrap();
        header.footer_digest = SHORT_DIGEST.to_vec();
        write_envelope(&path, &header, &chunks()).unwrap();
        assert_eq!(
            rejection_of(&path, &identity()),
            Rejection::DigestLength {
                len: SHORT_DIGEST.len()
            }
        );
        assert!(local.chunks_for(&identity()).unwrap().is_none());
    }

    #[tokio::test]
    async fn test_chunks_for_after_restart_serves_the_same_bytes_without_a_write() {
        let dir = tempfile::tempdir().unwrap();
        let (first, _) = open(dir.path());
        write(&first);
        let path = chunk_file(&first);
        let before = fs::metadata(&path).unwrap();
        drop(first);

        let (second, stats) = open(dir.path());
        let disk = second.chunks_for(&identity()).unwrap().unwrap();
        assert_eq!(&disk.page(FIRST_RANGE).await.unwrap()[..], FIRST_BYTES);
        assert_eq!(&disk.page(SECOND_RANGE).await.unwrap()[..], SECOND_BYTES);
        assert_eq!(&disk.page(INNER_RANGE).await.unwrap()[..], INNER_BYTES);
        assert_eq!(stats.local_reads(), 3);
        assert_eq!(stats.gets(), 0);

        let after = fs::metadata(&path).unwrap();
        assert_eq!(after.len(), before.len());
        assert_eq!(
            after.modified().unwrap(),
            before.modified().unwrap(),
            "nothing wrote the chunk file again"
        );
        assert_eq!(entries(&second.index_dir), vec![path]);
    }

    #[test]
    fn test_retain_with_empty_live_set_deletes_chunk_and_temp_files() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        write(&local);
        let path = chunk_file(&local);
        let temp = stale_temp(&path);
        fs::write(&temp, LEFTOVER).unwrap();
        let chunk_len = fs::metadata(&path).unwrap().len();
        let temp_len = u64::try_from(LEFTOVER.len()).unwrap();

        let freed = local.retain(&HashSet::new()).unwrap();
        assert_eq!(freed, chunk_len + temp_len);
        assert!(entries(&local.index_dir).is_empty());
        assert!(local.chunks_for(&identity()).unwrap().is_none());
    }

    #[test]
    fn test_retain_with_the_identity_live_deletes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        write(&local);
        let path = chunk_file(&local);
        let before = fs::metadata(&path).unwrap();

        let freed = local.retain(&HashSet::from([identity()])).unwrap();
        assert_eq!(freed, 0);
        let after = fs::metadata(&path).unwrap();
        assert_eq!(after.len(), before.len());
        assert_eq!(after.modified().unwrap(), before.modified().unwrap());
        assert!(local.chunks_for(&identity()).unwrap().is_some());
    }

    #[test]
    fn test_retain_deletes_a_file_that_fails_a_check() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        write(&local);
        let path = chunk_file(&local);
        overwrite(&path, MAGIC_OFFSET, &[CHUNK_FILE_MAGIC.wrapping_add(1)]);
        let len = fs::metadata(&path).unwrap().len();

        let freed = local.retain(&HashSet::from([identity()])).unwrap();
        assert_eq!(freed, len);
        assert!(entries(&local.index_dir).is_empty());
    }

    #[test]
    fn test_retain_keeps_the_live_file_and_deletes_the_dropped_one() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        write(&local);
        let dropped = DataFileIdentity {
            path: OTHER_PATH.to_string(),
            ..identity()
        };
        local
            .write_chunks(&dropped, chunks().into_iter().take(1))
            .unwrap();
        let dropped_len = fs::metadata(local.chunk_path(&dropped)).unwrap().len();
        assert_eq!(entries(&local.index_dir).len(), 2);

        let freed = local.retain(&HashSet::from([identity()])).unwrap();
        assert_eq!(freed, dropped_len);
        assert_eq!(entries(&local.index_dir), vec![chunk_file(&local)]);
        assert!(local.chunks_for(&identity()).unwrap().is_some());
        assert!(local.chunks_for(&dropped).unwrap().is_none());
    }

    #[test]
    fn test_retain_keeps_a_temp_file_a_writer_has_open_and_deletes_a_leftover() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        let path = chunk_file(&local);
        let open_write = local.begin_temp(&path);
        assert!(is_temp(&open_write.path));
        fs::write(&open_write.path, LEFTOVER).unwrap();
        let leftover = stale_temp(&path);
        fs::write(&leftover, LEFTOVER).unwrap();
        let leftover_len = u64::try_from(LEFTOVER.len()).unwrap();

        assert_eq!(local.retain(&HashSet::new()).unwrap(), leftover_len);
        assert_eq!(entries(&local.index_dir), vec![open_write.path.clone()]);

        drop(open_write);
        assert_eq!(local.retain(&HashSet::new()).unwrap(), leftover_len);
        assert!(entries(&local.index_dir).is_empty());
    }

    #[test]
    fn test_begin_temp_names_differ_per_call() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        let path = chunk_file(&local);
        let first = local.begin_temp(&path);
        let second = local.begin_temp(&path);
        assert_ne!(first.path, second.path);
        assert_eq!(first.path.parent(), Some(local.index_dir.as_path()));
        assert!(is_temp(&first.path) && is_temp(&second.path));
        assert_eq!(local.lock_in_flight().len(), 2);
        drop(first);
        drop(second);
        assert!(local.lock_in_flight().is_empty());
    }

    #[test]
    fn test_remove_if_present_of_a_missing_path_frees_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        let path = chunk_file(&local);
        assert_eq!(remove_if_present(&path, DATA_SIZE).unwrap(), 0);
        fs::write(&path, LEFTOVER).unwrap();
        assert_eq!(remove_if_present(&path, DATA_SIZE).unwrap(), DATA_SIZE);
        assert!(!path.exists());
    }

    #[test]
    fn test_retain_deletes_a_file_not_named_by_its_header() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        write(&local);
        let path = chunk_file(&local);
        let misnamed = local.index_dir.join(chunk_name(OTHER_PATH));
        fs::copy(&path, &misnamed).unwrap();
        let len = fs::metadata(&misnamed).unwrap().len();

        let freed = local.retain(&HashSet::from([identity()])).unwrap();
        assert_eq!(freed, len);
        assert_eq!(entries(&local.index_dir), vec![path]);
    }

    #[test]
    fn test_index_bytes_in_use_is_the_sum_of_the_files() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        assert_eq!(local.index_bytes_in_use().unwrap(), 0);
        write(&local);
        let other = DataFileIdentity {
            path: OTHER_PATH.to_string(),
            ..identity()
        };
        local
            .write_chunks(&other, chunks().into_iter().take(1))
            .unwrap();
        let expected: u64 = entries(&local.index_dir)
            .iter()
            .map(|path| fs::metadata(path).unwrap().len())
            .sum();
        assert_eq!(entries(&local.index_dir).len(), 2);
        assert_eq!(local.index_bytes_in_use().unwrap(), expected);
        // A write in progress is bytes on disk too.
        let temp = stale_temp(&chunk_file(&local));
        fs::write(&temp, LEFTOVER).unwrap();
        assert_eq!(
            local.index_bytes_in_use().unwrap(),
            expected + u64::try_from(LEFTOVER.len()).unwrap()
        );
    }

    #[test]
    fn test_page_tier_none_at_zero_budget() {
        let dir = tempfile::tempdir().unwrap();
        let local = LocalDir::open(&config(dir.path(), 0), Arc::new(IoStats::default())).unwrap();
        assert!(local.page_tier().is_none());
    }

    #[test]
    fn test_page_tier_some_with_the_cache_dir_otherwise() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        let tier = local.page_tier().unwrap();
        assert_eq!(tier.dir(), dir.path().join(CACHE_DIR));
        assert_eq!(tier.capacity_bytes(), DISK_CACHE_BYTES);
        assert!(tier.dir().is_dir());
    }

    #[test]
    fn test_chunk_name_is_the_hex_digest_of_the_path_and_not_the_path() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        write(&local);
        let name = chunk_file(&local)
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let expected: [u8; DIGEST_BYTES] = Sha256::digest(DATA_PATH.as_bytes()).into();
        assert_eq!(name, hex(&expected));
        assert_eq!(name.len(), DIGEST_BYTES * HEX_DIGITS_PER_BYTE);
        assert!(
            name.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
        assert!(!name.contains(DATA_PATH));
        assert!(!name.contains(DATA_FILE_NAME));
        let listed = entries(&local.index_dir);
        assert_eq!(listed, vec![local.index_dir.join(&name)]);
    }

    #[test]
    fn test_write_chunks_lays_out_the_envelope_in_data_order() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        // Reversed, so the file order is the sort and not the iterator's.
        let mut reversed = chunks();
        reversed.reverse();
        let disk = local
            .write_chunks(&identity(), reversed.into_iter())
            .unwrap();
        let bytes = fs::read(chunk_file(&local)).unwrap();

        assert_eq!(bytes[at(MAGIC_OFFSET)], CHUNK_FILE_MAGIC);
        assert_eq!(bytes[at(VERSION_OFFSET)], CHUNK_FILE_VERSION);
        let header_len = at(u64::from(header_len(&chunk_file(&local))));
        let header_end = at(HEADER_OFFSET) + header_len;
        let header = ChunkFileHeader::decode(&bytes[at(HEADER_OFFSET)..header_end]).unwrap();
        assert_eq!(
            header,
            ChunkFileHeader {
                data_file_path: DATA_PATH.to_string(),
                data_file_size: DATA_SIZE,
                footer_digest: FOOTER_DIGEST.to_vec(),
                chunks: vec![
                    ChunkSpan {
                        data_offset: FIRST_RANGE.start,
                        length: range_len(&FIRST_RANGE),
                    },
                    ChunkSpan {
                        data_offset: SECOND_RANGE.start,
                        length: range_len(&SECOND_RANGE),
                    },
                ],
            }
        );
        assert_eq!(header.encoded_len(), header_len);
        assert_eq!(&bytes[header_end..], [FIRST_BYTES, SECOND_BYTES].concat());
        assert_eq!(bytes.len(), header_end + at(payload_len()));
        assert_eq!(disk.bytes_on_disk(), payload_len());
        assert_eq!(
            disk.shared.spans,
            vec![
                StoredSpan {
                    data_offset: FIRST_RANGE.start,
                    length: range_len(&FIRST_RANGE),
                    file_offset: u64::try_from(header_end).unwrap(),
                },
                StoredSpan {
                    data_offset: SECOND_RANGE.start,
                    length: range_len(&SECOND_RANGE),
                    file_offset: u64::try_from(header_end).unwrap() + range_len(&FIRST_RANGE),
                },
            ]
        );
        assert_eq!(entries(&local.index_dir), vec![chunk_file(&local)]);
        assert!(local.lock_in_flight().is_empty());
    }

    /// The chunk length mismatch `write_chunks` refuses one chunk with, and
    /// the proof that nothing was written.
    fn length_mismatch_of(
        local: &LocalDir,
        range: Range<u64>,
        bytes: &'static [u8],
    ) -> (u64, u64, u64) {
        let result = local.write_chunks(
            &identity(),
            std::iter::once((range, Bytes::from_static(bytes))),
        );
        let Err(Error::Local {
            path,
            fault: LocalFault::ChunkLengthMismatch { start, end, len },
        }) = result
        else {
            panic!("expected a chunk length mismatch, got {result:?}");
        };
        assert_eq!(path, chunk_file(local));
        assert!(entries(&local.index_dir).is_empty(), "nothing was written");
        (start, end, len)
    }

    #[test]
    fn test_write_chunks_refuses_a_chunk_of_the_wrong_length() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        let (start, end, len) = length_mismatch_of(&local, FIRST_RANGE, SECOND_BYTES);
        assert_eq!(start..end, FIRST_RANGE);
        assert_eq!(len, u64::try_from(SECOND_BYTES.len()).unwrap());
    }

    #[test]
    fn test_write_chunks_refuses_an_empty_or_inverted_range() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        for range in [inverted_range(), EMPTY_AT_FIRST_END] {
            let (start, end, len) = length_mismatch_of(&local, range.clone(), b"");
            assert_eq!(start..end, range);
            assert_eq!(len, 0);
        }
    }

    #[test]
    fn test_write_chunks_refuses_overlapping_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        let overlapping = vec![
            (OVERLAPPING_RANGE, Bytes::from_static(SECOND_BYTES)),
            (FIRST_RANGE, Bytes::from_static(FIRST_BYTES)),
        ];
        let result = local.write_chunks(&identity(), overlapping.into_iter());
        let Err(Error::Local {
            path,
            fault: LocalFault::ChunkOverlap { start, end },
        }) = result
        else {
            panic!("expected a chunk overlap, got {result:?}");
        };
        assert_eq!(path, chunk_file(&local));
        assert_eq!(start..end, OVERLAPPING_RANGE);
        assert!(entries(&local.index_dir).is_empty(), "nothing was written");
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

    #[tokio::test]
    async fn test_write_chunks_concurrently_for_one_identity_each_handle_reads_its_own_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        let rival = vec![(FIRST_RANGE, Bytes::from_static(RIVAL_BYTES))];
        let (first, second) = std::thread::scope(|scope| {
            let first = scope.spawn(|| local.write_chunks(&identity(), chunks().into_iter()));
            let second = scope.spawn(|| local.write_chunks(&identity(), rival.into_iter()));
            (first.join().unwrap(), second.join().unwrap())
        });
        let first = first.unwrap();
        let second = second.unwrap();
        assert_eq!(&first.page(FIRST_RANGE).await.unwrap()[..], FIRST_BYTES);
        assert_eq!(&second.page(FIRST_RANGE).await.unwrap()[..], RIVAL_BYTES);

        assert_eq!(entries(&local.index_dir), vec![chunk_file(&local)]);
        let landed = local.chunks_for(&identity()).unwrap().unwrap();
        let bytes = landed.page(FIRST_RANGE).await.unwrap();
        assert!(&bytes[..] == FIRST_BYTES || &bytes[..] == RIVAL_BYTES);
        assert!(local.lock_in_flight().is_empty());
    }

    #[test]
    fn test_fmt_rejection_snake_case_names() {
        let cases = [
            (Rejection::WrongMagic, "wrong_magic"),
            (
                Rejection::UnsupportedVersion { version: 0 },
                "unsupported_version",
            ),
            (Rejection::HeaderTooLarge { len: 0 }, "header_too_large"),
            (Rejection::TruncatedHeader, "truncated_header"),
            (Rejection::UndecodableHeader, "undecodable_header"),
            (Rejection::DigestLength { len: 0 }, "digest_length"),
            (Rejection::SpanLayout, "span_layout"),
            (Rejection::ShortPayload, "short_payload"),
            (Rejection::TrailingBytes, "trailing_bytes"),
            (Rejection::PathMismatch, "path_mismatch"),
            (Rejection::SizeMismatch, "size_mismatch"),
            (Rejection::DigestMismatch, "digest_mismatch"),
            (Rejection::NameMismatch, "name_mismatch"),
        ];
        for (rejection, name) in cases {
            assert_eq!(rejection.to_string(), name);
        }
    }

    #[test]
    fn test_disk_chunks_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<DiskChunks>();
        assert_send_sync::<LocalDir>();
    }
}
