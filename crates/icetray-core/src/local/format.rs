//! The chunk file format (RFD 011, RFD 017): the envelope and header a
//! chunk file carries, the checks a reader makes before trusting one, and
//! the bytes a writer lays down. Nothing outside icetray reads a chunk file.
//!
//! After the envelope and the header comes the payload: the verification
//! record, a message the index encodes and this module frames without
//! reading, then the key chunks back to back. The header carries a crc32c of
//! the whole payload, so a file whose payload was torn by a crash, cut
//! short, or changed on disk fails the checksum and is rebuilt; every other
//! header field is either compared with what the reader expects (identity,
//! verification context, spans) or constrained by the file's length.

use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, Write as _};
use std::ops::Range;
use std::os::unix::fs::FileExt as _;
use std::path::Path;

use bytes::Bytes;
use prost::Message as _;

use super::{DIGEST_BYTES, DataFileIdentity, LocalFault, VerifiedUnder, digest};
use crate::approx;
use crate::key::{HEX_DIGITS_PER_BYTE, KeyType};

/// The first byte of a chunk file (RFD 011): identifies the file as an
/// icetray chunk file before any of it is parsed. `0xC1` is never the first
/// byte of UTF-8 text, so no configuration file, log, or JSON document
/// passes the check by accident.
pub(super) const CHUNK_FILE_MAGIC: u8 = 0xC1;
/// The version byte of the chunk file format this build writes and the only
/// one it reads (RFD 011). A build that changes the layout raises it, and a
/// file of another version is stale and rebuilt from object storage, never
/// converted (RFD 017).
pub(super) const CHUNK_FILE_VERSION: u8 = 2;
/// Where the magic byte sits in a chunk file.
pub(super) const MAGIC_OFFSET: u64 = 0;
/// Where the version byte sits: right after the magic byte.
pub(super) const VERSION_OFFSET: u64 = MAGIC_OFFSET + 1;
/// Where the length prefix of the encoded header sits: right after the
/// version byte.
pub(super) const LENGTH_OFFSET: u64 = VERSION_OFFSET + 1;
/// The width of the header's length prefix: a little-endian `u32`.
/// `MAX_HEADER_BYTES` bounds what it may hold.
pub(super) const LENGTH_PREFIX_BYTES: usize = size_of::<u32>();
/// Where the encoded header starts: right after its length prefix. The
/// payload starts where the header ends.
pub(super) const HEADER_OFFSET: u64 = LENGTH_OFFSET + approx::count(LENGTH_PREFIX_BYTES);
/// The largest encoded header a chunk file may carry, written or read. A
/// header is one data file path, one digest, a few fixed fields, and about
/// twenty bytes per stored chunk, one per row group, so a mebibyte holds
/// tens of thousands of row groups, more than any data file the layout
/// targets has. The keys the index keeps are in the verification record,
/// not here, so the header stays this small however long the keys. The
/// reader checks the length prefix against this before it allocates, so a
/// corrupt prefix cannot ask for gigabytes.
pub(super) const MAX_HEADER_BYTES: usize = 1024 * 1024;
/// The check a chunk file failed. Not an error: the file is foreign, stale,
/// incomplete, or torn; `chunks_for` treats it as absent, a Disk-tier build
/// rebuilds it, and `retain` deletes one that fails a check of its envelope
/// or header. Display gives the `snake_case` name the logs carry.
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
    /// The header carries no payload checksum.
    MissingChecksum,
    /// The header's key type is none this build knows.
    UnknownKeyType {
        /// The value the header carries.
        value: i32,
    },
    /// The header's spans are not what a page read's binary search needs:
    /// one is empty, starts before the end of the one before it, or ends
    /// past `u64::MAX`; or the payload, the verification record and the
    /// spans after it, ends past `u64::MAX`.
    SpanLayout,
    /// The payload is shorter than the verification record and the spans
    /// promise.
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
    /// The verification record is longer than the reader's bound for the
    /// data file it describes; checked before the record is read.
    VerificationTooLarge {
        /// The record's length in the header.
        len: u64,
        /// The most the reader accepts.
        most: u64,
    },
    /// The payload's crc32c is not the one the header carries: the file was
    /// torn by a crash, or changed after it was written.
    ChecksumMismatch,
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
            Self::MissingChecksum => "missing_checksum",
            Self::UnknownKeyType { .. } => "unknown_key_type",
            Self::SpanLayout => "span_layout",
            Self::ShortPayload => "short_payload",
            Self::TrailingBytes => "trailing_bytes",
            Self::PathMismatch => "path_mismatch",
            Self::SizeMismatch => "size_mismatch",
            Self::DigestMismatch => "digest_mismatch",
            Self::NameMismatch => "name_mismatch",
            Self::VerificationTooLarge { .. } => "verification_too_large",
            Self::ChecksumMismatch => "checksum_mismatch",
        };
        f.write_str(name)
    }
}

/// Why a chunk file could not be parsed: the file system failed, or the
/// file failed a check. The two are handled apart: an I/O failure is an
/// error, a failed check makes the file absent.
#[derive(Debug)]
pub(super) enum ParseFailure {
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
/// the data file whose key chunks the payload holds, one span per chunk
/// saying which bytes of that data file it is, the length of the
/// verification record before the chunks, what that record was computed
/// under, and the payload's checksum. The spans say where each range of the
/// data file sits in the chunk file, which the footer does not, and the
/// record is what lets a restarted process skip decoding their keys. Nothing outside icetray reads a chunk file, so
/// the message is declared here and not in the client contract. Field
/// numbers are never reused.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub(super) struct ChunkFileHeader {
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
    /// The verification record's length in bytes; it sits right after the
    /// header, and the first chunk right after it.
    #[prost(uint64, tag = "5")]
    pub verification_length: u64,
    /// crc32c of the payload, every byte after the header. Always present
    /// in a file this build writes, and encoded at its fixed width whatever
    /// its value, so a header's length does not depend on it.
    #[prost(fixed32, optional, tag = "6")]
    pub payload_crc32c: Option<u32>,
    /// The key type the record was verified under.
    #[prost(enumeration = "StoredKeyType", tag = "7")]
    pub key_type: i32,
    /// The rules the record was verified under (`index::VERIFIER_VERSION`).
    #[prost(uint32, tag = "8")]
    pub verifier_version: u32,
}

/// `KeyType` as the header stores it. Zero is no key type, so a header
/// that omits the field is rejected rather than read as one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub(super) enum StoredKeyType {
    /// No key type: never written.
    Unspecified = 0,
    /// `KeyType::Long`.
    Long = 1,
    /// `KeyType::String`.
    String = 2,
    /// `KeyType::Binary`.
    Binary = 3,
    /// `KeyType::Uuid`.
    Uuid = 4,
}

impl StoredKeyType {
    /// How the header stores `key_type`.
    fn of(key_type: KeyType) -> Self {
        match key_type {
            KeyType::Long => Self::Long,
            KeyType::String => Self::String,
            KeyType::Binary => Self::Binary,
            KeyType::Uuid => Self::Uuid,
        }
    }

    /// The key type a header's value names; `None` for zero or a value
    /// this build does not know.
    fn key_type(value: i32) -> Option<KeyType> {
        match Self::try_from(value).ok()? {
            Self::Unspecified => None,
            Self::Long => Some(KeyType::Long),
            Self::String => Some(KeyType::String),
            Self::Binary => Some(KeyType::Binary),
            Self::Uuid => Some(KeyType::Uuid),
        }
    }
}

/// One stored chunk: the byte range of the data file it holds. Its bytes
/// sit in the payload right after the bytes of every span before it.
#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub(super) struct ChunkSpan {
    /// Where the chunk starts in the data file.
    #[prost(uint64, tag = "1")]
    pub data_offset: u64,
    /// The chunk's length in bytes.
    #[prost(uint64, tag = "2")]
    pub length: u64,
}

/// One stored chunk with where its bytes sit in the chunk file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct StoredSpan {
    /// Where the chunk starts in the data file.
    pub(super) data_offset: u64,
    /// The chunk's length in bytes.
    pub(super) length: u64,
    /// Where the chunk's bytes start in the chunk file.
    pub(super) file_offset: u64,
}

/// A chunk file that passed every check of its envelope and header.
#[derive(Debug)]
pub(super) struct ParsedChunkFile {
    /// The identity the header carries.
    pub(super) identity: DataFileIdentity,
    /// The spans in header order with their payload offsets.
    pub(super) spans: Vec<StoredSpan>,
    /// What the verification record was computed under.
    pub(super) under: VerifiedUnder,
    /// Where the payload, and with it the verification record, starts.
    pub(super) payload_offset: u64,
    /// The verification record's length.
    pub(super) verification_length: u64,
    /// The payload's checksum as the header carries it.
    pub(super) payload_crc32c: u32,
    /// Where the payload ends, which is the file's length.
    pub(super) end: u64,
}

impl ParsedChunkFile {
    /// Everything a reader takes from `header`, whose payload starts at
    /// `payload_offset`, once the header's own fields pass their checks:
    /// the digest's width, the checksum's presence, a key type this build
    /// knows, and spans laid out after the record within `u64`. The file's
    /// length is the caller's to compare with `end`.
    pub(super) fn from_header(
        header: ChunkFileHeader,
        payload_offset: u64,
    ) -> std::result::Result<Self, Rejection> {
        let footer_digest = <[u8; DIGEST_BYTES]>::try_from(header.footer_digest.as_slice())
            .map_err(|_| Rejection::DigestLength {
                len: header.footer_digest.len(),
            })?;
        let payload_crc32c = header.payload_crc32c.ok_or(Rejection::MissingChecksum)?;
        let key_type =
            StoredKeyType::key_type(header.key_type).ok_or(Rejection::UnknownKeyType {
                value: header.key_type,
            })?;
        let chunks_offset = payload_offset
            .checked_add(header.verification_length)
            .ok_or(Rejection::SpanLayout)?;
        let (spans, chunk_bytes) =
            stored_spans(&header.chunks, chunks_offset).ok_or(Rejection::SpanLayout)?;
        let end = chunks_offset
            .checked_add(chunk_bytes)
            .ok_or(Rejection::SpanLayout)?;
        Ok(Self {
            identity: DataFileIdentity {
                path: header.data_file_path,
                size: header.data_file_size,
                footer_digest,
            },
            spans,
            under: VerifiedUnder {
                key_type,
                verifier_version: header.verifier_version,
            },
            payload_offset,
            verification_length: header.verification_length,
            payload_crc32c,
            end,
        })
    }
}

/// Reads the envelope and header of an open chunk file, then checks the
/// header's identity against `identity`. The order of the checks is the
/// order of the `Rejection` variants.
pub(super) fn validate(
    file: &File,
    identity: &DataFileIdentity,
) -> std::result::Result<ParsedChunkFile, ParseFailure> {
    let parsed = parse_chunk_file(file)?;
    check_identity(&parsed.identity, identity)?;
    Ok(parsed)
}

/// Reads the envelope and header of an open chunk file and checks that the
/// payload is as long as the verification record and the spans promise.
/// Needs no identity, so `retain` can read the one a file carries. Reads no
/// payload byte: the checksum is checked by `DiskChunks::checked_verification`,
/// when a reader is about to trust the record.
pub(super) fn parse_chunk_file(file: &File) -> std::result::Result<ParsedChunkFile, ParseFailure> {
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
    let header_len = approx::length(header_len);
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
    let parsed = ParsedChunkFile::from_header(header, payload_offset)?;
    match file_len.cmp(&parsed.end) {
        std::cmp::Ordering::Less => Err(Rejection::ShortPayload.into()),
        std::cmp::Ordering::Greater => Err(Rejection::TrailingBytes.into()),
        std::cmp::Ordering::Equal => Ok(parsed),
    }
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
/// from `chunks_offset`, and the chunks' total length. `None` when the
/// spans are not what `ChunkFile::locate` needs, non-empty, ascending, and
/// disjoint, or when they do not sum within `u64`, which no file can hold.
fn stored_spans(chunks: &[ChunkSpan], chunks_offset: u64) -> Option<(Vec<StoredSpan>, u64)> {
    let mut spans = Vec::with_capacity(chunks.len());
    let mut file_offset = chunks_offset;
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
    Some((spans, file_offset - chunks_offset))
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

/// The header for `identity` over `verification`, the record verified
/// `under`, and `chunks`, already sorted by data offset, with the crc32c of
/// the payload they make. A chunk whose bytes are not its range's length,
/// an empty or inverted range included, is `LocalFault::ChunkLengthMismatch`;
/// one whose range overlaps the chunk before it is `LocalFault::ChunkOverlap`.
/// Both are checked in every build, not only in debug as `Segments::new`
/// checks its parts: the header is written to a file that outlives the
/// process and is trusted by every later reader, so what it promises is
/// verified here.
pub(super) fn header_for(
    identity: &DataFileIdentity,
    under: VerifiedUnder,
    verification: &[u8],
    chunks: &[(Range<u64>, Bytes)],
) -> std::result::Result<ChunkFileHeader, LocalFault> {
    let mut spans = Vec::with_capacity(chunks.len());
    let mut data_end = 0;
    for (range, bytes) in chunks {
        let len = approx::count(bytes.len());
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
    let crc = chunks
        .iter()
        .fold(crc32c::crc32c(verification), |crc, (_, bytes)| {
            crc32c::crc32c_append(crc, bytes)
        });
    Ok(header_of(
        identity,
        under,
        spans,
        approx::count(verification.len()),
        crc,
    ))
}

/// The header naming `identity`, `under`, `spans`, a verification record of
/// `verification_length` bytes, and the payload checksum `crc`: the one
/// layout `write_chunks` writes and `chunk_file_bytes` measures.
fn header_of(
    identity: &DataFileIdentity,
    under: VerifiedUnder,
    spans: Vec<ChunkSpan>,
    verification_length: u64,
    crc: u32,
) -> ChunkFileHeader {
    ChunkFileHeader {
        data_file_path: identity.path.clone(),
        data_file_size: identity.size,
        footer_digest: identity.footer_digest.to_vec(),
        chunks: spans,
        verification_length,
        payload_crc32c: Some(crc),
        key_type: StoredKeyType::of(under.key_type).into(),
        verifier_version: under.verifier_version,
    }
}

/// The bytes the chunk file `write_chunks` would write for `identity` over
/// `ranges` with a verification record of `verification_length` bytes
/// verified `under` occupies: the envelope, the encoded header, the record,
/// and the chunks back to back. Exact for a record of that length, since the
/// checksum is encoded at its fixed width: this is what
/// `LocalDir::index_bytes_in_use` measures for the file once written.
/// Saturates rather than wraps: every range is bounded by its data file,
/// but a footer that lies may repeat one range in every row group.
pub(crate) fn chunk_file_bytes(
    identity: &DataFileIdentity,
    under: VerifiedUnder,
    ranges: &[Range<u64>],
    verification_length: u64,
) -> u64 {
    let spans = ranges
        .iter()
        .map(|range| ChunkSpan {
            data_offset: range.start,
            length: range.end.saturating_sub(range.start),
        })
        .collect::<Vec<_>>();
    let chunk_bytes = spans
        .iter()
        .map(|span| span.length)
        .fold(0, u64::saturating_add);
    let header = header_of(identity, under, spans, verification_length, 0);
    HEADER_OFFSET
        .saturating_add(approx::count(header.encoded_len()))
        .saturating_add(verification_length)
        .saturating_add(chunk_bytes)
}

/// Writes one chunk file at `path`, which must not exist: the magic byte,
/// the version byte, the little-endian `u32` length of the encoded header,
/// the header, the verification record, then every chunk's bytes back to
/// back in `chunks` order, which is the header's span order. Returns the
/// handle it wrote through, open for reading, so the caller reads the file
/// this call wrote and not whatever later lands at its name.
///
/// The file is not synced. A crash can leave it holding fewer bytes than
/// were written, or its length with blocks never written, and a reader
/// rejects either: the first by its length, the second by the payload
/// checksum. What `write_chunks` promises is only that no reader sees a
/// partial file under its final name, which the rename gives; losing a file
/// to a crash costs one fetch of its chunks, since the directory is
/// disposable.
pub(super) fn write_envelope(
    path: &Path,
    header: &ChunkFileHeader,
    verification: &[u8],
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
        .and_then(|()| file.write_all(verification))
        .map_err(LocalFault::Io)?;
    for (_, bytes) in chunks {
        file.write_all(bytes).map_err(LocalFault::Io)?;
    }
    Ok(file)
}

/// The chunk file name for a data file path: the lowercase hex `digest` of
/// the path's bytes, so no path appears in a file name.
pub(super) fn chunk_name(path: &str) -> String {
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

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::fs;
    use std::fs::OpenOptions;

    use sha2::{Digest as _, Sha256};

    use super::*;
    use crate::error::Error;
    use crate::local::testing::{
        DATA_PATH, DATA_SIZE, EMPTY_AT_FIRST_END, FIRST_BYTES, FIRST_RANGE, FOOTER_DIGEST,
        OTHER_PATH, SECOND_BYTES, SECOND_RANGE, UNDER, VERIFICATION, chunk_file, chunks, entries,
        identity, inverted_range, open, overwrite, payload_len, range_len, write,
    };
    use crate::local::{ChunkFileLookup, LocalDir};

    /// The final component of `DATA_PATH`, which no hex name can contain.
    const DATA_FILE_NAME: &str = "part-00000.parquet";
    /// One byte more than `DATA_SIZE`: the smallest change that makes an
    /// identity stale.
    const OTHER_SIZE: u64 = DATA_SIZE + 1;
    /// A footer digest the fixture identity does not carry.
    const OTHER_DIGEST: [u8; DIGEST_BYTES] = [9; DIGEST_BYTES];
    /// A digest of the wrong width, three bytes.
    const SHORT_DIGEST: [u8; 3] = [1, 2, 3];
    /// A byte that is not a valid protobuf key and cannot start a header.
    const NOT_PROTOBUF: u8 = 0xFF;
    /// A length prefix of all ones: the largest a `u32` can promise.
    const HUGE_LENGTH_PREFIX: [u8; LENGTH_PREFIX_BYTES] = [0xFF; LENGTH_PREFIX_BYTES];
    /// A range that overlaps `FIRST_RANGE` from inside it.
    const OVERLAPPING_RANGE: Range<u64> = 104..108;
    /// A byte appended after the payload.
    const EXTRA: &[u8] = b"!";
    /// A key type value no build assigns.
    const UNKNOWN_KEY_TYPE: i32 = 99;

    /// The header `write` writes for the fixture.
    fn fixture_header() -> ChunkFileHeader {
        header_for(&identity(), UNDER, VERIFICATION, &chunks()).unwrap()
    }

    /// Writes the fixture's record and chunks at `path` under `header`.
    fn write_under(path: &Path, header: &ChunkFileHeader) {
        write_envelope(path, header, VERIFICATION, &chunks()).unwrap();
    }

    fn at(offset: u64) -> usize {
        approx::length(offset)
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
                len: approx::length(u32::MAX)
            }
        );
        assert!(local.chunks_for(&identity()).unwrap().is_none());
        let freed = local.retain(&HashSet::new()).unwrap();
        assert_eq!(freed, approx::count(envelope.len()));
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
        fs::remove_file(&path).unwrap();
        let mut header = fixture_header();
        header.verification_length += 1;
        write_under(&path, &header);
        assert_eq!(rejection_of(&path, &identity()), Rejection::ShortPayload);
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
        let mut swapped = fixture_header();
        swapped.chunks.reverse();
        let mut overlapping = fixture_header();
        overlapping.chunks[1].data_offset = OVERLAPPING_RANGE.start;
        let mut empty = fixture_header();
        empty.chunks[0].length = 0;
        let mut past_the_end = fixture_header();
        past_the_end.chunks[1].data_offset = u64::MAX - 1;
        let mut record_past_the_end = fixture_header();
        record_past_the_end.verification_length = u64::MAX - 1;
        for header in [
            swapped,
            overlapping,
            empty,
            past_the_end,
            record_past_the_end,
        ] {
            write_under(&path, &header);
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
        let mut header = fixture_header();
        header.footer_digest = SHORT_DIGEST.to_vec();
        write_under(&path, &header);
        assert_eq!(
            rejection_of(&path, &identity()),
            Rejection::DigestLength {
                len: SHORT_DIGEST.len()
            }
        );
        assert!(local.chunks_for(&identity()).unwrap().is_none());
    }

    #[test]
    fn test_validate_missing_checksum_and_unknown_key_type() {
        let dir = tempfile::tempdir().unwrap();
        let (local, _) = open(dir.path());
        let path = chunk_file(&local);
        let mut unchecked = fixture_header();
        unchecked.payload_crc32c = None;
        let mut unknown = fixture_header();
        unknown.key_type = UNKNOWN_KEY_TYPE;
        let mut unspecified = fixture_header();
        unspecified.key_type = StoredKeyType::Unspecified.into();
        let cases = [
            (unchecked, Rejection::MissingChecksum),
            (
                unknown,
                Rejection::UnknownKeyType {
                    value: UNKNOWN_KEY_TYPE,
                },
            ),
            (unspecified, Rejection::UnknownKeyType { value: 0 }),
        ];
        for (header, expected) in cases {
            write_under(&path, &header);
            assert_eq!(rejection_of(&path, &identity()), expected);
            assert!(matches!(
                local.lookup(&identity()).unwrap(),
                ChunkFileLookup::Rejected(rejection) if rejection == expected
            ));
            fs::remove_file(&path).unwrap();
        }
    }

    #[test]
    fn test_stored_key_type_round_trips_every_key_type() {
        for key_type in KeyType::ALL {
            let stored: i32 = StoredKeyType::of(key_type).into();
            assert_eq!(StoredKeyType::key_type(stored), Some(key_type));
        }
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
            .write_chunks(&identity(), UNDER, VERIFICATION, reversed.into_iter())
            .unwrap();
        let bytes = fs::read(chunk_file(&local)).unwrap();

        assert_eq!(bytes[at(MAGIC_OFFSET)], CHUNK_FILE_MAGIC);
        assert_eq!(bytes[at(VERSION_OFFSET)], CHUNK_FILE_VERSION);
        let header_len = at(u64::from(header_len(&chunk_file(&local))));
        let header_end = at(HEADER_OFFSET) + header_len;
        let header = ChunkFileHeader::decode(&bytes[at(HEADER_OFFSET)..header_end]).unwrap();
        let payload = [VERIFICATION, FIRST_BYTES, SECOND_BYTES].concat();
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
                verification_length: approx::count(VERIFICATION.len()),
                payload_crc32c: Some(crc32c::crc32c(&payload)),
                key_type: StoredKeyType::of(UNDER.key_type).into(),
                verifier_version: UNDER.verifier_version,
            }
        );
        assert_eq!(header.encoded_len(), header_len);
        assert_eq!(&bytes[header_end..], payload);
        assert_eq!(bytes.len(), header_end + at(payload_len()));
        assert_eq!(
            approx::count(bytes.len()),
            chunk_file_bytes(
                &identity(),
                UNDER,
                &[FIRST_RANGE, SECOND_RANGE],
                approx::count(VERIFICATION.len())
            )
        );
        let chunks_start = approx::count(header_end + VERIFICATION.len());
        assert_eq!(
            disk.shared.spans,
            vec![
                StoredSpan {
                    data_offset: FIRST_RANGE.start,
                    length: range_len(&FIRST_RANGE),
                    file_offset: chunks_start,
                },
                StoredSpan {
                    data_offset: SECOND_RANGE.start,
                    length: range_len(&SECOND_RANGE),
                    file_offset: chunks_start + range_len(&FIRST_RANGE),
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
            UNDER,
            VERIFICATION,
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
        assert_eq!(len, approx::count(SECOND_BYTES.len()));
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
        let result = local.write_chunks(&identity(), UNDER, VERIFICATION, overlapping.into_iter());
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
            (Rejection::MissingChecksum, "missing_checksum"),
            (Rejection::UnknownKeyType { value: 0 }, "unknown_key_type"),
            (Rejection::SpanLayout, "span_layout"),
            (Rejection::ShortPayload, "short_payload"),
            (Rejection::TrailingBytes, "trailing_bytes"),
            (Rejection::PathMismatch, "path_mismatch"),
            (Rejection::SizeMismatch, "size_mismatch"),
            (Rejection::DigestMismatch, "digest_mismatch"),
            (Rejection::NameMismatch, "name_mismatch"),
            (
                Rejection::VerificationTooLarge { len: 0, most: 0 },
                "verification_too_large",
            ),
            (Rejection::ChecksumMismatch, "checksum_mismatch"),
        ];
        for (rejection, name) in cases {
            assert_eq!(rejection.to_string(), name);
        }
    }
}
