//! Deletion vectors in Puffin files (RFD 021): the footer, read from the
//! file's tail and checked against every vector the manifests name in it,
//! and the vector blobs, framed as the Iceberg spec fixes them.
//!
//! A Puffin file starts with the magic and ends in a footer: the magic, a
//! JSON payload, the payload's length as a four-byte little-endian integer,
//! four flag bytes, and the magic again. A deletion vector blob is a
//! four-byte big-endian length of the magic and the vector, the magic
//! `D1 D3 39 64`, the vector in roaring's portable 64-bit format, and a
//! four-byte big-endian CRC-32 of the magic and the vector.

use bytes::{Bytes, BytesMut};
use iceberg::puffin::{BlobMetadata, CompressionCodec, DELETION_VECTOR_V1, FileMetadata};
use roaring::RoaringTreemap;

use super::{BlobFraming, DeleteFault};
use crate::approx;
use crate::error::{Error, RefusalReason, Result, UnsupportedDelete};
use crate::storage::Storage;

/// The bytes read from a Puffin file's tail in the one speculative read of
/// its footer: a footer of up to about two hundred vectors, so a writer
/// that puts one or a few vectors in each file, as Spark does per task,
/// costs one GET, and a larger footer costs one more for the rest.
const FOOTER_PREFETCH_BYTES: u64 = 64 * 1024;

/// The magic that opens a Puffin file and opens and closes its footer.
const PUFFIN_MAGIC: [u8; 4] = *b"PFA1";

/// The bytes of a Puffin magic.
const MAGIC_BYTES: usize = PUFFIN_MAGIC.len();

/// The bytes of a footer's payload length, a four-byte integer.
const PAYLOAD_LENGTH_BYTES: usize = 4;

/// The bytes of a footer's flags.
const FLAGS_BYTES: usize = 4;

/// The fixed tail of a footer: the payload length, the flags, the magic.
const FOOTER_STRUCT_BYTES: usize = PAYLOAD_LENGTH_BYTES + FLAGS_BYTES + MAGIC_BYTES;

/// The flag bit, in the first flag byte, of a compressed footer payload;
/// every other bit of the flags is reserved.
const FLAG_COMPRESSED: u8 = 1;

/// The bytes of a deletion vector blob's length prefix and of its
/// checksum suffix.
const VECTOR_FRAME_BYTES: usize = 4;

/// The magic a deletion vector's bytes start with, after its length: the
/// Puffin spec's `deletion-vector-v1` blob type fixes these four bytes.
const VECTOR_MAGIC: [u8; 4] = [0xD1, 0xD3, 0x39, 0x64];

/// The blob property naming the data file a deletion vector applies to,
/// as the Puffin spec names it.
pub const REFERENCED_DATA_FILE_PROPERTY: &str = "referenced-data-file";

/// The blob property holding a deletion vector's count of deleted rows, as
/// the Puffin spec names it.
pub const CARDINALITY_PROPERTY: &str = "cardinality";

/// A Puffin file's footer, and where its blobs end: the footer's start,
/// past which no blob lies.
#[derive(Debug)]
pub(super) struct Footer {
    metadata: FileMetadata,
    blobs_end: u64,
}

/// The footer of the Puffin file at `path` of `size` bytes, as the
/// manifests record its size: one `FOOTER_PREFETCH_BYTES` read of its tail,
/// clamped to the file, and one more for the part of a longer footer the
/// first did not reach. Its framing is checked whole first: both footer
/// magics, and a payload that fits between the file's opening magic and the
/// footer's tail; wrong framing, or a payload that is not the footer's
/// JSON, is `DeleteFault::FooterFraming`, transient, since a damaged read
/// cannot be told from a damaged file. Then its flags: a compressed payload,
/// which icetray does not decompress, and a reserved flag set, which no
/// reader icetray knows of writes, are each
/// `RefusalReason::DeleteKindUnsupported`.
pub(super) async fn read_footer(
    table: &str,
    storage: &Storage,
    path: &str,
    size: u64,
) -> Result<Footer> {
    let located = storage.locate(path).await?;
    let prefetch = FOOTER_PREFETCH_BYTES.min(size);
    let tail = storage.read_range(&located, size - prefetch..size).await?;
    let framing = || super::fault(path, DeleteFault::FooterFraming);
    let footer_struct = tail
        .len()
        .checked_sub(FOOTER_STRUCT_BYTES)
        .map(|at| &tail[at..])
        .ok_or_else(framing)?;
    let (length, rest) = footer_struct.split_at(PAYLOAD_LENGTH_BYTES);
    let (flags, magic) = rest.split_at(FLAGS_BYTES);
    if magic != PUFFIN_MAGIC {
        return Err(framing());
    }
    let payload = u64::from(u32::from_le_bytes(
        length
            .try_into()
            .expect("split at the payload length's bytes"),
    ));
    let footer_len = payload + approx::count(MAGIC_BYTES + FOOTER_STRUCT_BYTES);
    if footer_len + approx::count(MAGIC_BYTES) > size {
        return Err(framing());
    }
    let tail_len = approx::count(tail.len());
    let footer: Bytes = match footer_len.checked_sub(tail_len) {
        Some(more) if more > 0 => {
            let head = storage
                .read_range(&located, size - footer_len..size - tail_len)
                .await?;
            if approx::count(head.len()) != more {
                return Err(framing());
            }
            let mut whole = BytesMut::with_capacity(head.len() + tail.len());
            whole.extend_from_slice(&head);
            whole.extend_from_slice(&tail);
            whole.freeze()
        }
        Some(_) | None => tail.slice(tail.len() - approx::length(footer_len)..),
    };
    if footer[..MAGIC_BYTES] != PUFFIN_MAGIC {
        return Err(framing());
    }
    let unsupported = |kind| {
        Error::refused(
            table,
            RefusalReason::DeleteKindUnsupported {
                path: path.to_string(),
                kind,
            },
        )
    };
    if flags[0] & FLAG_COMPRESSED != 0 {
        return Err(unsupported(UnsupportedDelete::CompressedFooter));
    }
    let reserved = u32::from_le_bytes(flags.try_into().expect("split at the flags' bytes"))
        & !u32::from(FLAG_COMPRESSED);
    if reserved != 0 {
        return Err(unsupported(UnsupportedDelete::ReservedFooterFlags {
            flags: reserved,
        }));
    }
    let json = &footer[MAGIC_BYTES..footer.len() - FOOTER_STRUCT_BYTES];
    let metadata = serde_json::from_slice(json).map_err(|_not_json| framing())?;
    Ok(Footer {
        metadata,
        blobs_end: size - footer_len,
    })
}

/// The blob of `footer` that the manifest entry at `offset` and `length` of
/// the Puffin file at `path` names, checked against what the entry records:
/// a deletion vector for `referenced` holding `records` rows. No blob there
/// is `DeleteFault::VectorUnlisted`; a blob before the end of the file's
/// opening magic or past the footer's start, `DeleteFault::BlobOutOfRange`;
/// a blob of another type, `RefusalReason::DeleteKindUnsupported`; a
/// compressed blob, which the spec forbids for a vector,
/// `DeleteFault::BlobCompressed`; a `referenced-data-file` or `cardinality`
/// property that differs from the entry,
/// `DeleteFault::ReferencedFileMismatch` or
/// `DeleteFault::CardinalityProperty`.
pub(super) fn checked_blob<'a>(
    table: &str,
    path: &str,
    footer: &'a Footer,
    (offset, length): (u64, u64),
    referenced: &str,
    records: u64,
) -> Result<&'a BlobMetadata> {
    let fault = |fault| super::fault(path, fault);
    let blob = footer
        .metadata
        .blobs()
        .iter()
        .find(|blob| blob.offset() == offset && blob.length() == length)
        .ok_or_else(|| fault(DeleteFault::VectorUnlisted { offset, length }))?;
    let inside = offset >= approx::count(MAGIC_BYTES)
        && offset
            .checked_add(length)
            .is_some_and(|end| end <= footer.blobs_end);
    if !inside {
        return Err(fault(DeleteFault::BlobOutOfRange {
            offset,
            length,
            blobs_end: footer.blobs_end,
        }));
    }
    if blob.blob_type() != DELETION_VECTOR_V1 {
        return Err(Error::refused(
            table,
            RefusalReason::DeleteKindUnsupported {
                path: path.to_string(),
                kind: UnsupportedDelete::PuffinBlob {
                    blob_type: blob.blob_type().to_string(),
                },
            },
        ));
    }
    let codec = blob.compression_codec();
    if codec != CompressionCodec::None {
        return Err(fault(DeleteFault::BlobCompressed { codec }));
    }
    let property = |name: &str| blob.properties().get(name).cloned();
    let found = property(REFERENCED_DATA_FILE_PROPERTY);
    if found.as_deref() != Some(referenced) {
        return Err(fault(DeleteFault::ReferencedFileMismatch {
            referenced: referenced.to_string(),
            found,
        }));
    }
    let found = property(CARDINALITY_PROPERTY);
    if found.as_deref().and_then(|count| count.parse::<u64>().ok()) != Some(records) {
        return Err(fault(DeleteFault::CardinalityProperty {
            recorded: records,
            found,
        }));
    }
    Ok(blob)
}

/// What a load keeps of a Puffin footer whose file still lists vectors no
/// generation applies (RFD 021): each such vector's blob as `checked_blob`
/// passed it against its entry, and nothing else, so a later load whose
/// entry for that blob records the same needs neither the footer nor its
/// GET, and any other reads the footer again.
#[derive(Debug)]
pub(super) struct Retained {
    blobs: Box<[Checked]>,
}

/// A blob `checked_blob` passed against an entry recording `referenced`
/// and `records`.
#[derive(Debug)]
struct Checked {
    offset: u64,
    length: u64,
    records: u64,
    referenced: Box<str>,
}

/// The bytes a `Checked` takes in a `Retained`'s slice, beside its
/// `referenced` path.
pub(super) const CHECKED_BLOB_BYTES: u64 = approx::count(size_of::<Checked>());

impl Retained {
    /// Of `vectors` in the Puffin file at `path` of `table`, each an
    /// entry's blob range, referenced data file, and rows, the ones
    /// `footer` passes (`checked_blob`), kept; `None` for none. A vector the
    /// footer does not pass is not kept, and is checked again from the
    /// footer if a later load reads it.
    pub(super) fn of<'v>(
        table: &str,
        path: &str,
        footer: &Footer,
        vectors: impl Iterator<Item = ((u64, u64), &'v str, u64)>,
    ) -> Option<Self> {
        let blobs: Box<[Checked]> = vectors
            .filter(|&(range, referenced, records)| {
                checked_blob(table, path, footer, range, referenced, records).is_ok()
            })
            .map(|((offset, length), referenced, records)| Checked {
                offset,
                length,
                records,
                referenced: referenced.into(),
            })
            .collect();
        (!blobs.is_empty()).then_some(Self { blobs })
    }

    /// The kept blobs among `vectors`, each an entry's blob range,
    /// referenced data file, and rows, as `of` takes them; `None` for none.
    pub(super) fn kept<'v>(
        &self,
        vectors: impl Iterator<Item = ((u64, u64), &'v str, u64)>,
    ) -> Option<Self> {
        let blobs: Box<[Checked]> = vectors
            .filter_map(|(range, referenced, records)| {
                self.find(range, referenced, records).map(|blob| Checked {
                    offset: blob.offset,
                    length: blob.length,
                    records: blob.records,
                    referenced: blob.referenced.clone(),
                })
            })
            .collect();
        (!blobs.is_empty()).then_some(Self { blobs })
    }

    /// Whether the blob at `range` passed `checked_blob` against an entry
    /// recording `referenced` and `records`.
    pub(super) fn holds(&self, range: (u64, u64), referenced: &str, records: u64) -> bool {
        self.find(range, referenced, records).is_some()
    }

    fn find(
        &self,
        (offset, length): (u64, u64),
        referenced: &str,
        records: u64,
    ) -> Option<&Checked> {
        self.blobs.iter().find(|blob| {
            blob.offset == offset
                && blob.length == length
                && blob.records == records
                && &*blob.referenced == referenced
        })
    }

    /// The bytes this holds: its slice, and every referenced path's.
    pub(super) fn resident_bytes(&self) -> u64 {
        self.blobs
            .iter()
            .map(|blob| CHECKED_BLOB_BYTES + super::boxed_str_bytes(blob.referenced.len()))
            .fold(0, u64::saturating_add)
    }
}

/// The positions the deletion vector `blob` holds: its length, magic, and
/// checksum checked first, each `DeleteFault::BlobFraming` when wrong and
/// transient, then the vector decoded as roaring's portable 64-bit format,
/// which must take exactly the bytes the length gives it, else
/// `DeleteFault::VectorMalformed`, deterministic once the checksum holds.
pub(super) fn decode(blob: &[u8]) -> std::result::Result<RoaringTreemap, DeleteFault> {
    let framing = |which| DeleteFault::BlobFraming(which);
    let (length, rest) = blob
        .split_first_chunk::<VECTOR_FRAME_BYTES>()
        .ok_or(framing(BlobFraming::Length))?;
    let body_len =
        usize::try_from(u32::from_be_bytes(*length)).map_err(|_| framing(BlobFraming::Length))?;
    if rest.len().checked_sub(VECTOR_FRAME_BYTES) != Some(body_len) {
        return Err(framing(BlobFraming::Length));
    }
    let (body, checksum) = rest.split_at(body_len);
    let Some(vector) = body.strip_prefix(VECTOR_MAGIC.as_slice()) else {
        return Err(framing(BlobFraming::Magic));
    };
    let mut checksum_bytes = [0; VECTOR_FRAME_BYTES];
    checksum_bytes.copy_from_slice(checksum);
    // CRC-32 (ISO-HDLC), which the Iceberg spec fixes for deletion vectors;
    // icetray's own chunk files use CRC-32C.
    if u32::from_be_bytes(checksum_bytes) != crc32fast::hash(body) {
        return Err(framing(BlobFraming::Checksum));
    }
    let mut reader = vector;
    let rows =
        RoaringTreemap::deserialize_from(&mut reader).map_err(|_| DeleteFault::VectorMalformed)?;
    if !reader.is_empty() {
        return Err(DeleteFault::VectorMalformed);
    }
    Ok(rows)
}

/// Deletion vector blobs framed as `decode` reads them, and Puffin files of
/// blobs with each blob's range, as a correct writer lays them out; for
/// the tests of this module and its parent.
#[cfg(test)]
pub(super) mod testing {
    use std::collections::HashMap;
    use std::ops::Range;

    use iceberg::io::FileIO;
    use iceberg::puffin::{Blob, CompressionCodec, PuffinReader, PuffinWriter};
    use roaring::RoaringTreemap;

    use super::{CARDINALITY_PROPERTY, REFERENCED_DATA_FILE_PROPERTY, VECTOR_MAGIC};

    /// The snapshot id and sequence number a deletion vector's blob carries
    /// in its footer: the spec fixes both at -1, since the blob is written
    /// before its commit has either.
    const UNASSIGNED: i64 = -1;

    /// `body`, the magic and a vector or any other bytes, framed: its
    /// big-endian length before it and its big-endian CRC-32 after.
    pub(in crate::deletes) fn frame(body: &[u8]) -> Vec<u8> {
        let mut blob = u32::try_from(body.len()).unwrap().to_be_bytes().to_vec();
        blob.extend_from_slice(body);
        blob.extend_from_slice(&crc32fast::hash(body).to_be_bytes());
        blob
    }

    /// `rows` framed as a deletion vector blob.
    pub(in crate::deletes) fn vector_blob(rows: &RoaringTreemap) -> Vec<u8> {
        let mut body = VECTOR_MAGIC.to_vec();
        rows.serialize_into(&mut body).unwrap();
        frame(&body)
    }

    /// The properties of a vector for `data_file` of `cardinality` rows.
    pub(in crate::deletes) fn properties(
        data_file: &str,
        cardinality: u64,
    ) -> HashMap<String, String> {
        HashMap::from([
            (
                REFERENCED_DATA_FILE_PROPERTY.to_string(),
                data_file.to_string(),
            ),
            (CARDINALITY_PROPERTY.to_string(), cardinality.to_string()),
        ])
    }

    /// Writes `blobs`, each (type, bytes, properties), through
    /// `PuffinWriter` at `path`, each compressed with `codec`, with an
    /// uncompressed footer, and returns each blob's range as iceberg's own
    /// reader finds it in the footer.
    pub(in crate::deletes) async fn write_puffin(
        path: &str,
        blobs: Vec<(&str, Vec<u8>, HashMap<String, String>)>,
        codec: CompressionCodec,
    ) -> Vec<Range<u64>> {
        let file_io = FileIO::new_with_fs();
        let output = file_io.new_output(path).unwrap();
        let mut writer = PuffinWriter::new(&output, HashMap::new(), false)
            .await
            .unwrap();
        for (blob_type, bytes, properties) in blobs {
            let blob = Blob::builder()
                .r#type(blob_type.to_string())
                .fields(Vec::new())
                .snapshot_id(UNASSIGNED)
                .sequence_number(UNASSIGNED)
                .data(bytes)
                .properties(properties)
                .build();
            writer.add(blob, codec).await.unwrap();
        }
        writer.close().await.unwrap();
        PuffinReader::new(file_io.new_input(path).unwrap())
            .file_metadata()
            .await
            .unwrap()
            .blobs()
            .iter()
            .map(|blob| blob.offset()..blob.offset() + blob.length())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::testing::{frame, properties, vector_blob, write_puffin};
    use super::*;
    use crate::config::StorageConfig;
    use crate::deletes::delete_fault_of;
    use crate::error::{Recurrence, Recurs};
    use crate::generation::testing::{TABLE, refusal_of};

    /// Rows across two blocks and two segments.
    fn rows() -> RoaringTreemap {
        [0, 7, 70_000, (1 << 32) + 1].into_iter().collect()
    }

    /// The delete fault `result` ended in.
    fn delete_fault<T: std::fmt::Debug>(result: Result<T>) -> DeleteFault {
        delete_fault_of(result).1
    }

    /// A Puffin file of `blobs` compressed with `codec` in a temporary
    /// directory, its location, its size, and each blob's range.
    async fn puffin_with(
        dir: &tempfile::TempDir,
        blobs: Vec<(&str, Vec<u8>, std::collections::HashMap<String, String>)>,
        codec: CompressionCodec,
    ) -> (String, u64, Vec<std::ops::Range<u64>>) {
        let path = format!("file://{}/v.puffin", dir.path().display());
        let ranges = write_puffin(&path, blobs, codec).await;
        let size = std::fs::metadata(dir.path().join("v.puffin"))
            .unwrap()
            .len();
        (path, size, ranges)
    }

    /// `puffin_with` uncompressed.
    async fn puffin(
        dir: &tempfile::TempDir,
        blobs: Vec<(&str, Vec<u8>, std::collections::HashMap<String, String>)>,
    ) -> (String, u64, Vec<std::ops::Range<u64>>) {
        puffin_with(dir, blobs, CompressionCodec::None).await
    }

    /// The (offset, length) of `range`.
    fn placed(range: &std::ops::Range<u64>) -> (u64, u64) {
        (range.start, range.end - range.start)
    }

    #[test]
    fn test_decode_reads_a_framed_vector() {
        assert_eq!(decode(&vector_blob(&rows())).unwrap(), rows());
    }

    /// `crc32fast` computes the CRC-32 the Iceberg spec fixes for a
    /// deletion vector, ISO-HDLC, and not CRC-32C: its catalogue check
    /// value, the checksum of the nine ASCII digits.
    #[test]
    fn test_decode_checksum_is_crc32_iso_hdlc() {
        let iso_hdlc_check_value = 0xCBF4_3926;
        assert_eq!(crc32fast::hash(b"123456789"), iso_hdlc_check_value);
    }

    /// The position Java Iceberg's `TestBitmapPositionDeleteIndex` names by
    /// `position(bitmap, container, value)`: `value` in the container of
    /// that index within the 32-bit bitmap of that index.
    fn java_position(bitmap: u32, container: u16, value: u16) -> u64 {
        (u64::from(bitmap) << u32::BITS) | (u64::from(container) << u16::BITS) | u64::from(value)
    }

    /// Each deletion vector blob Java Iceberg's `BitmapPositionDeleteIndex`
    /// serialized (`testdata/iceberg/README.md`), and the positions its test
    /// deleted before serializing it, ascending. A two-position
    /// `delete(start, end)` in Java deletes `start..end`.
    fn java_vectors() -> [(&'static str, &'static [u8], Vec<u64>); 4] {
        let integer_max = u64::try_from(i32::MAX).unwrap();
        let ranges = [
            java_position(0, 0, 5)..java_position(0, 0, 5) + 1,
            java_position(0, 0, 7)..java_position(0, 0, 7) + 1,
            java_position(0, 1, 1)..java_position(0, 1, 1000),
            java_position(0, 2, 1)..java_position(0, 2, u16::MAX),
            java_position(1, 0, 10)..java_position(1, 0, 10) + 1,
            java_position(1, 0, 20)..java_position(1, 0, 20) + 1,
            java_position(1, 1, 10)..java_position(1, 1, 500),
            java_position(1, 2, 1)..java_position(1, 2, u16::MAX),
        ];
        [
            (
                "empty",
                include_bytes!("../../testdata/iceberg/empty-position-index.bin"),
                Vec::new(),
            ),
            (
                "small alternating values",
                include_bytes!(
                    "../../testdata/iceberg/small-alternating-values-position-index.bin"
                ),
                vec![1, 3, 5, 7, 9],
            ),
            (
                "small and large values",
                include_bytes!("../../testdata/iceberg/small-and-large-values-position-index.bin"),
                vec![100, 101, integer_max + 100, integer_max + 101],
            ),
            (
                "all container types",
                include_bytes!("../../testdata/iceberg/all-container-types-position-index.bin"),
                ranges.into_iter().flatten().collect(),
            ),
        ]
    }

    #[test]
    fn test_decode_reads_the_positions_java_iceberg_wrote() {
        for (name, blob, positions) in java_vectors() {
            let decoded: Vec<u64> = decode(blob).unwrap().iter().collect();
            assert_eq!(decoded, positions, "{name}");
        }
    }

    /// The CRC-32 Java Iceberg stored after each vector is `crc32fast`'s over
    /// the magic and the vector, the bytes between the length and the CRC.
    #[test]
    fn test_decode_checksum_matches_the_crc_java_iceberg_stored() {
        for (name, blob, _) in java_vectors() {
            let (_, rest) = blob.split_first_chunk::<VECTOR_FRAME_BYTES>().unwrap();
            let (body, stored) = rest.split_last_chunk::<VECTOR_FRAME_BYTES>().unwrap();
            assert!(body.starts_with(&VECTOR_MAGIC), "{name}");
            assert_eq!(u32::from_be_bytes(*stored), crc32fast::hash(body), "{name}");
        }
    }

    /// `bitmap`, a bare portable 64-bit roaring bitmap, framed as a
    /// deletion vector blob.
    fn framed_bitmap(bitmap: &[u8]) -> Vec<u8> {
        frame(&[VECTOR_MAGIC.as_slice(), bitmap].concat())
    }

    /// The roaring format spec's examples that Iceberg's
    /// `TestRoaringPositionBitmap` accepts. The spec's testdata README does
    /// not describe these files, so the expected positions are read from
    /// each file's layout: no bitmaps; one bitmap of key 0 holding 0
    /// through 9; ten bitmaps of keys 0 through 9, each holding 0 through 9.
    #[test]
    fn test_decode_reads_the_64_bit_examples_iceberg_accepts() {
        let values_per_bitmap = 10;
        let spread_bitmaps = 10;
        let low: Vec<u64> = (0..values_per_bitmap).collect();
        let spread: Vec<u64> = (0..spread_bitmaps)
            .flat_map(|key| (0..values_per_bitmap).map(move |low| (key << u32::BITS) | low))
            .collect();
        let examples: [(&str, &[u8], Vec<u64>); 3] = [
            (
                "64mapempty",
                include_bytes!("../../testdata/iceberg/64mapempty.bin"),
                Vec::new(),
            ),
            (
                "64map32bitvals",
                include_bytes!("../../testdata/iceberg/64map32bitvals.bin"),
                low,
            ),
            (
                "64mapspreadvals",
                include_bytes!("../../testdata/iceberg/64mapspreadvals.bin"),
                spread,
            ),
        ];
        for (name, bitmap, positions) in examples {
            let decoded: Vec<u64> = decode(&framed_bitmap(bitmap)).unwrap().iter().collect();
            assert_eq!(decoded, positions, "{name}");
        }
    }

    /// The example Iceberg rejects for a key above `Integer.MAX_VALUE` holds,
    /// as Iceberg's copy has it, U+FFFD's UTF-8 bytes wherever a byte at or
    /// above 0x80 belongs, so its first key is 0xEFBDBFEF and the bytes after
    /// it are no 32-bit bitmap: it is malformed, deterministically.
    #[test]
    fn test_decode_refuses_the_high_keys_example_iceberg_rejects_as_malformed() {
        let bitmap = include_bytes!("../../testdata/iceberg/64maphighvals.bin");
        let fault = decode(&framed_bitmap(bitmap)).unwrap_err();
        assert_eq!(fault, DeleteFault::VectorMalformed);
        assert_eq!(fault.recurrence(), Recurrence::Deterministic);
    }

    #[test]
    fn test_decode_refuses_a_wrong_length_magic_or_checksum_as_framing() {
        let blob = vector_blob(&rows());
        let mut long = blob.clone();
        long[VECTOR_FRAME_BYTES - 1] += 1;
        let mut magic = blob.clone();
        magic[VECTOR_FRAME_BYTES] ^= 1;
        let mut checksum = blob.clone();
        *checksum.last_mut().unwrap() ^= 1;
        let mut vector = blob.clone();
        vector[VECTOR_FRAME_BYTES + VECTOR_MAGIC.len() + 1] ^= 1;
        let cases = [
            (long, BlobFraming::Length),
            (blob[..blob.len() - 1].to_vec(), BlobFraming::Length),
            (magic, BlobFraming::Magic),
            (checksum, BlobFraming::Checksum),
            (vector, BlobFraming::Checksum),
        ];
        for (bytes, which) in cases {
            assert_eq!(decode(&bytes), Err(DeleteFault::BlobFraming(which)));
        }
    }

    #[test]
    fn test_decode_refuses_a_checksummed_vector_that_does_not_decode() {
        let mut whole = Vec::new();
        rows().serialize_into(&mut whole).unwrap();
        let mut trailing = VECTOR_MAGIC.to_vec();
        trailing.extend_from_slice(&whole);
        trailing.push(0);
        let mut cut = VECTOR_MAGIC.to_vec();
        cut.extend_from_slice(&whole[..whole.len() / 2]);
        for (body, case) in [(trailing, "a trailing byte"), (cut, "a cut vector")] {
            assert_eq!(
                decode(&frame(&body)),
                Err(DeleteFault::VectorMalformed),
                "{case}"
            );
        }
    }

    #[tokio::test]
    async fn test_read_footer_parses_puffin_writers_footer_in_one_or_two_gets() {
        let dir = tempfile::tempdir().unwrap();
        let data_file = "file:///data/a.parquet";
        let blob = vector_blob(&rows());
        let small = vec![(
            DELETION_VECTOR_V1,
            blob.clone(),
            properties(data_file, rows().len()),
        )];
        let (path, size, ranges) = puffin(&dir, small).await;
        let storage = Storage::new(StorageConfig::default());
        let footer = read_footer(TABLE, &storage, &path, size).await.unwrap();
        assert_eq!(storage.stats().gets(), 1, "one tail read");
        let found = checked_blob(
            TABLE,
            &path,
            &footer,
            placed(&ranges[0]),
            data_file,
            rows().len(),
        )
        .unwrap();
        assert_eq!(found.blob_type(), DELETION_VECTOR_V1);

        // Enough vectors that the footer runs past the prefetch.
        let vectors_past_the_prefetch = 2_000;
        let many: Vec<_> = (0..vectors_past_the_prefetch)
            .map(|at| {
                (
                    DELETION_VECTOR_V1,
                    blob.clone(),
                    properties(&format!("file:///data/{at}.parquet"), rows().len()),
                )
            })
            .collect();
        let dir = tempfile::tempdir().unwrap();
        let (path, size, ranges) = puffin(&dir, many).await;
        let storage = Storage::new(StorageConfig::default());
        let footer = read_footer(TABLE, &storage, &path, size).await.unwrap();
        assert!(size > FOOTER_PREFETCH_BYTES);
        assert_eq!(storage.stats().gets(), 2, "the tail, then the rest");
        assert_eq!(footer.metadata.blobs().len(), ranges.len());
    }

    /// The bytes of `original` at `at` flipped, written over `file`.
    fn damaged(file: &std::path::Path, original: &[u8], at: usize, flip: u8) {
        let mut bytes = original.to_vec();
        bytes[at] ^= flip;
        std::fs::write(file, &bytes).unwrap();
    }

    #[tokio::test]
    async fn test_read_footer_refuses_a_damaged_tail_before_its_flags() {
        let dir = tempfile::tempdir().unwrap();
        let blobs = vec![(DELETION_VECTOR_V1, vector_blob(&rows()), properties("a", 4))];
        let (path, size, _) = puffin(&dir, blobs).await;
        let file = dir.path().join("v.puffin");
        let original = std::fs::read(&file).unwrap();
        let storage = Storage::new(StorageConfig::default());
        let flags_at = original.len() - FOOTER_STRUCT_BYTES + PAYLOAD_LENGTH_BYTES;
        let payload_length_at = original.len() - FOOTER_STRUCT_BYTES;
        let payload_length = u32::from_le_bytes(
            original[payload_length_at..payload_length_at + PAYLOAD_LENGTH_BYTES]
                .try_into()
                .unwrap(),
        );
        let opening_magic = payload_length_at - approx::length(payload_length) - MAGIC_BYTES;
        let payload = opening_magic + MAGIC_BYTES;
        let last = original.len() - 1;
        let high_length_byte = payload_length_at + PAYLOAD_LENGTH_BYTES - 1;
        let flip = 0x41;
        for at in [last, high_length_byte, opening_magic, payload] {
            damaged(&file, &original, at, flip);
            assert_eq!(
                delete_fault(read_footer(TABLE, &storage, &path, size).await),
                DeleteFault::FooterFraming,
                "byte {at}"
            );
        }
        // Damaged framing beside a compressed flag is framing still: the
        // flags are judged only once the framing holds.
        let mut both = original.clone();
        both[flags_at] |= FLAG_COMPRESSED;
        both[opening_magic] ^= flip;
        std::fs::write(&file, &both).unwrap();
        assert_eq!(
            delete_fault(read_footer(TABLE, &storage, &path, size).await),
            DeleteFault::FooterFraming
        );
    }

    #[tokio::test]
    async fn test_read_footer_refuses_a_compressed_footer_and_reserved_flags() {
        let dir = tempfile::tempdir().unwrap();
        let blobs = vec![(DELETION_VECTOR_V1, vector_blob(&rows()), properties("a", 4))];
        let (path, size, _) = puffin(&dir, blobs).await;
        let file = dir.path().join("v.puffin");
        let original = std::fs::read(&file).unwrap();
        let storage = Storage::new(StorageConfig::default());
        let flags_at = original.len() - FOOTER_STRUCT_BYTES + PAYLOAD_LENGTH_BYTES;
        // The flag alone: the writer here cannot compress, and the reader
        // refuses on the flag before it reads the payload.
        damaged(&file, &original, flags_at, FLAG_COMPRESSED);
        assert_eq!(
            refusal_of(read_footer(TABLE, &storage, &path, size).await),
            RefusalReason::DeleteKindUnsupported {
                path: path.clone(),
                kind: UnsupportedDelete::CompressedFooter,
            }
        );
        let reserved_bit = 0b10;
        let last_flag_byte = 0x80;
        for (at, flip, flags) in [
            (flags_at, reserved_bit, u32::from(reserved_bit)),
            (
                flags_at + FLAGS_BYTES - 1,
                last_flag_byte,
                u32::from(last_flag_byte) << 24,
            ),
        ] {
            damaged(&file, &original, at, flip);
            assert_eq!(
                refusal_of(read_footer(TABLE, &storage, &path, size).await),
                RefusalReason::DeleteKindUnsupported {
                    path: path.clone(),
                    kind: UnsupportedDelete::ReservedFooterFlags { flags },
                },
                "byte {at}"
            );
        }
    }

    #[tokio::test]
    async fn test_read_footer_refuses_a_tail_shorter_than_a_footer() {
        let dir = tempfile::tempdir().unwrap();
        let path = format!("file://{}/short.puffin", dir.path().display());
        let short = [PUFFIN_MAGIC.as_slice(), &[0; 3], PUFFIN_MAGIC.as_slice()].concat();
        std::fs::write(dir.path().join("short.puffin"), &short).unwrap();
        let storage = Storage::new(StorageConfig::default());
        let size = approx::count(short.len());
        assert_eq!(
            delete_fault(read_footer(TABLE, &storage, &path, size).await),
            DeleteFault::FooterFraming
        );
    }

    #[tokio::test]
    async fn test_checked_blob_holds_the_footer_to_the_manifest_entry() {
        let dir = tempfile::tempdir().unwrap();
        let data_file = "file:///data/a.parquet";
        let cardinality = rows().len();
        let blobs = vec![
            (
                DELETION_VECTOR_V1,
                vector_blob(&rows()),
                properties(data_file, cardinality),
            ),
            (
                "apache-datasketches-theta-v1",
                vec![1, 2, 3],
                std::collections::HashMap::new(),
            ),
        ];
        let (path, size, ranges) = puffin(&dir, blobs).await;
        let storage = Storage::new(StorageConfig::default());
        let footer = read_footer(TABLE, &storage, &path, size).await.unwrap();
        let vector = placed(&ranges[0]);
        let check = |range, referenced, records| {
            checked_blob(TABLE, &path, &footer, range, referenced, records)
        };
        assert_eq!(
            delete_fault(check((vector.0 + 1, vector.1), data_file, cardinality)),
            DeleteFault::VectorUnlisted {
                offset: vector.0 + 1,
                length: vector.1
            }
        );
        assert_eq!(
            refusal_of(check(placed(&ranges[1]), data_file, cardinality)),
            RefusalReason::DeleteKindUnsupported {
                path: path.clone(),
                kind: UnsupportedDelete::PuffinBlob {
                    blob_type: "apache-datasketches-theta-v1".to_string()
                },
            }
        );
        let elsewhere = "file:///data/b.parquet";
        assert_eq!(
            delete_fault(check(vector, elsewhere, cardinality)),
            DeleteFault::ReferencedFileMismatch {
                referenced: elsewhere.to_string(),
                found: Some(data_file.to_string()),
            }
        );
        assert_eq!(
            delete_fault(check(vector, data_file, cardinality + 1)),
            DeleteFault::CardinalityProperty {
                recorded: cardinality + 1,
                found: Some(cardinality.to_string()),
            }
        );
    }

    #[tokio::test]
    async fn test_checked_blob_refuses_a_compressed_vector() {
        let dir = tempfile::tempdir().unwrap();
        let data_file = "file:///data/a.parquet";
        let blobs = vec![(
            DELETION_VECTOR_V1,
            vector_blob(&rows()),
            properties(data_file, rows().len()),
        )];
        let (path, size, ranges) = puffin_with(&dir, blobs, CompressionCodec::zstd_default()).await;
        let storage = Storage::new(StorageConfig::default());
        let footer = read_footer(TABLE, &storage, &path, size).await.unwrap();
        let fault = delete_fault(checked_blob(
            TABLE,
            &path,
            &footer,
            placed(&ranges[0]),
            data_file,
            rows().len(),
        ));
        assert!(
            matches!(
                fault,
                DeleteFault::BlobCompressed {
                    codec: CompressionCodec::Zstd(_)
                }
            ),
            "{fault:?}"
        );
    }

    /// A footer whose one vector blob lies at `offset` for `length` bytes,
    /// with blobs ending at `blobs_end`.
    fn footer_placing(offset: u64, length: u64, blobs_end: u64) -> Footer {
        let json = serde_json::json!({
            "blobs": [{
                "type": DELETION_VECTOR_V1,
                "fields": [],
                "snapshot-id": -1,
                "sequence-number": -1,
                "offset": offset,
                "length": length,
                "properties": properties("a", 1),
            }]
        });
        Footer {
            metadata: serde_json::from_value(json).unwrap(),
            blobs_end,
        }
    }

    #[test]
    fn test_checked_blob_refuses_a_blob_outside_the_files_blobs() {
        let blobs_end = 100;
        let magic = approx::count(MAGIC_BYTES);
        let inside = 10;
        for (offset, length, held) in [
            (magic, inside, true),
            (blobs_end - inside, inside, true),
            (0, inside, false),
            (magic - 1, inside, false),
            (blobs_end - inside, inside + 1, false),
            (u64::MAX, 2, false),
        ] {
            let footer = footer_placing(offset, length, blobs_end);
            let checked = checked_blob(TABLE, "v.puffin", &footer, (offset, length), "a", 1);
            if held {
                checked.unwrap();
            } else {
                assert_eq!(
                    delete_fault(checked),
                    DeleteFault::BlobOutOfRange {
                        offset,
                        length,
                        blobs_end
                    },
                    "offset {offset} length {length}"
                );
            }
        }
    }

    #[test]
    fn test_retained_keeps_the_blobs_it_passed_and_their_entries() {
        let blobs_end = 100;
        let range = (approx::count(MAGIC_BYTES), 10);
        let footer = footer_placing(range.0, range.1, blobs_end);
        let path = "v.puffin";
        let retained = Retained::of(TABLE, path, &footer, [(range, "a", 1)].into_iter()).unwrap();
        assert!(retained.holds(range, "a", 1));
        assert!(!retained.holds(range, "b", 1), "another referenced file");
        assert!(!retained.holds(range, "a", 2), "another cardinality");
        assert!(Retained::of(TABLE, path, &footer, [(range, "b", 1)].into_iter()).is_none());
        assert!(retained.kept([(range, "a", 2)].into_iter()).is_none());
        assert!(retained.kept([(range, "a", 1)].into_iter()).is_some());
    }
}
