//! The page headers of a key column chunk, read ahead of parquet's page
//! reader and bounded by what their pages can hold (RFD 004). Parquet sizes
//! buffers from two header numbers as it reads a page, before any check on
//! the page it hands out can run: a page's uncompressed size, which it
//! reserves and decompresses into, and a dictionary page's value count,
//! which sizes the dictionary it decodes into. A header that lies about
//! either makes the process allocate what it claims. Parquet also adds a v2
//! data page header's two level lengths as `i32`s, which two lengths near
//! `i32::MAX` overflow. Every replica loads the same snapshot, so such a
//! file would take down each one that loads it; a typed refusal on a
//! refresh keeps the generation before it serving instead.
//!
//! The reader here decodes the Thrift compact encoding of parquet's
//! `PageHeader` as parquet 58 reads it (`PageHeader::read_thrift_without_stats`,
//! the path `SerializedPageReader` takes with the default reader properties),
//! keeps the sizes and the value count, and consumes every other field as
//! parquet does: a field parquet reads, as the type it reads it as, and
//! any other skipped by its wire type. Where parquet would read a field differently from its
//! wire type, or skip one without consuming its bytes, the reader refuses
//! rather than guess, so a header it accepts is one parquet reads to the
//! same numbers. It allocates nothing: every count it reads bounds a loop
//! over input it has, never a buffer.

use std::fmt;
use std::mem::size_of;
use std::ops::Range;

use parquet::basic::PageType;
use parquet::file::reader::ChunkReader;

use super::{ColumnChunk, FileFault, KeyShape, file_fault, row_index};
use crate::approx;
use crate::contract::MAX_KEY_BYTES;
use crate::error::Result;
use crate::parquet_io::Segments;

/// Bytes an encoding writes for one key value beyond the key's own bytes:
/// a `PLAIN` or `DELTA_LENGTH_BYTE_ARRAY` length of 4 bytes, or a
/// `DELTA_BYTE_ARRAY` prefix and suffix length, deltas of `i32` lengths
/// bit-packed at up to 33 bits each, plus a definition level, at most 2
/// bytes when the levels are RLE runs of one value. That is at most 11
/// bytes; 16 leaves room for a writer that packs less tightly.
const VALUE_ENCODING_ALLOWANCE_BYTES: u64 = 16;

/// The most bytes one key value takes in a page body: a key at the cap,
/// `MAX_KEY_BYTES`, and what its encoding adds. A longer key is refused at
/// load (`RefusalReason::KeyTooLong`), so a page whose keys are all at the
/// cap is the largest page an admitted file holds.
pub(super) const MOST_BYTES_PER_VALUE: u64 =
    approx::count(MAX_KEY_BYTES) + VALUE_ENCODING_ALLOWANCE_BYTES;

/// Bytes an encoding writes once per page whatever its value count: the
/// 4-byte length of a v1 page's definition levels, an RLE bit width, a
/// delta encoding's header varints, and the padding of a delta
/// encoding's last block, at most a block of 128 values at 64 bits, a
/// kibibyte, for each of up to two delta streams. That is about 2
/// kibibytes; 64 kibibytes leaves a wide margin and is still nothing
/// beside the 2 gibibytes a header can claim.
pub(super) const PAGE_ENCODING_ALLOWANCE_BYTES: u64 = 64 * 1024;

/// The most uncompressed bytes a key page (data, v2 data, or dictionary)
/// may declare, whatever the footer claims. The contract's largest key page
/// is `PAGE_ROWS` rows of keys at `MAX_KEY_BYTES`, about 4 MiB; writers
/// target pages of about 1 MiB and fall back from dictionary encoding once
/// a dictionary passes about 1 MiB. 64 MiB is sixteen times the largest
/// contract page. The span-derived bounds follow the footer's row counts,
/// which a footer can inflate; this one does not, so it caps the page
/// buffer parquet reserves from a key page header, and with
/// `fewest_entry_bytes` the entry count it sizes a key dictionary by.
pub(super) const MOST_KEY_PAGE_BYTES: u64 = 64 * 1024 * 1024;

/// The bytes a `BYTE_ARRAY` value's `PLAIN` length prefix takes, the
/// fewest a dictionary entry of a byte array key takes: an empty key is
/// its prefix alone.
const BYTE_ARRAY_LENGTH_PREFIX_BYTES: u64 = approx::count(size_of::<u32>());

/// The fewest bytes one entry of a `PLAIN` dictionary of a key column of
/// `shape` takes, `width` being the column's `FIXED_LEN_BYTE_ARRAY` width:
/// an `INT64` is 8 bytes, a byte array its length prefix, a fixed-width
/// value its width. `None` for a width of 0, whose entries take no bytes,
/// so no page size bounds their count.
pub(super) fn fewest_entry_bytes(shape: KeyShape, width: i32) -> Option<u64> {
    match shape {
        KeyShape::Long => Some(approx::count(size_of::<i64>())),
        KeyShape::Str | KeyShape::Bin => Some(BYTE_ARRAY_LENGTH_PREFIX_BYTES),
        KeyShape::FixedBin | KeyShape::Uuid => u64::try_from(width).ok().filter(|&bytes| bytes > 0),
    }
}

/// The deepest parquet's compact reader skips nested structs and lists
/// (`DEFAULT_SKIP_DEPTH` in parquet's `parquet_thrift`); deeper is refused
/// there, and here.
const SKIP_DEPTH: u8 = 64;

/// The low four bits of a field or list header byte: the wire type code.
pub(super) const NIBBLE_MASK: u8 = 0x0f;
/// The shift that brings a field header's high four bits, the field id's
/// delta from the one before, or a list header's element count, down to
/// the low four.
pub(super) const NIBBLE_BITS: u8 = 4;
/// The field header code that ends a struct.
const STOP: u8 = 0;
/// A list header whose count nibble is this carries its count in a
/// varint after it.
const LIST_COUNT_FOLLOWS: u8 = 0x0f;
/// The bit of a varint byte that says another byte follows.
pub(super) const VARINT_CONTINUES: u8 = 0x80;
/// The payload bits of a varint byte.
const VARINT_PAYLOAD_BITS: usize = 7;

/// `PageHeader.type`.
pub(super) const TYPE_FIELD: i16 = 1;
/// `PageHeader.uncompressed_page_size`.
pub(super) const UNCOMPRESSED_PAGE_SIZE_FIELD: i16 = 2;
/// `PageHeader.compressed_page_size`.
pub(super) const COMPRESSED_PAGE_SIZE_FIELD: i16 = 3;
/// `PageHeader.crc`.
const CRC_FIELD: i16 = 4;
/// `PageHeader.data_page_header`.
pub(super) const DATA_PAGE_HEADER_FIELD: i16 = 5;
/// `PageHeader.index_page_header`.
const INDEX_PAGE_HEADER_FIELD: i16 = 6;
/// `PageHeader.dictionary_page_header`.
pub(super) const DICTIONARY_PAGE_HEADER_FIELD: i16 = 7;
/// `PageHeader.data_page_header_v2`.
pub(super) const DATA_PAGE_HEADER_V2_FIELD: i16 = 8;
/// `num_values`, the first field of each of the three page-specific
/// headers.
pub(super) const NUM_VALUES_FIELD: i16 = 1;
/// `DataPageHeaderV2.definition_levels_byte_length`.
pub(super) const DEFINITION_LEVELS_BYTE_LENGTH_FIELD: i16 = 5;
/// `DataPageHeaderV2.repetition_levels_byte_length`.
pub(super) const REPETITION_LEVELS_BYTE_LENGTH_FIELD: i16 = 6;

/// How parquet reads a field of a page-specific header: as an `i32`
/// varint, or as a boolean carried in the field header itself.
#[derive(Clone, Copy)]
enum Scalar {
    I32,
    Bool,
}

/// The fields parquet reads from `DataPageHeader` without statistics, by
/// field id from 1: `num_values` and the three encodings. `statistics`,
/// the next id, is skipped by its wire type.
const DATA_PAGE_HEADER_FIELDS: &[Scalar] = &[Scalar::I32, Scalar::I32, Scalar::I32, Scalar::I32];

/// The fields parquet reads from `DataPageHeaderV2` without statistics, by
/// field id from 1: `num_values`, `num_nulls`, `num_rows`, `encoding`, the
/// two level lengths, and `is_compressed`. `statistics`, the next id, is
/// skipped by its wire type.
const DATA_PAGE_HEADER_V2_FIELDS: &[Scalar] = &[
    Scalar::I32,
    Scalar::I32,
    Scalar::I32,
    Scalar::I32,
    Scalar::I32,
    Scalar::I32,
    Scalar::Bool,
];

/// The fields parquet reads from `DictionaryPageHeader`, by field id from
/// 1: `num_values`, `encoding`, and `is_sorted`.
const DICTIONARY_PAGE_HEADER_FIELDS: &[Scalar] = &[Scalar::I32, Scalar::I32, Scalar::Bool];

/// The Thrift compact protocol's wire types, each with the code a field
/// header or a list header carries for it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum WireType {
    /// A boolean field whose value is true, carried in the field header;
    /// in a list header, like `BoolFalse`, a boolean element.
    BoolTrue = 1,
    /// A boolean field whose value is false, carried in the field header;
    /// in a list header, a boolean element.
    BoolFalse = 2,
    /// One byte.
    Byte = 3,
    /// A zigzag varint.
    I16 = 4,
    /// A zigzag varint.
    I32 = 5,
    /// A zigzag varint.
    I64 = 6,
    /// Eight bytes.
    Double = 7,
    /// A varint length, then that many bytes.
    Binary = 8,
    /// A list header, then its elements.
    List = 9,
    /// A set, which no parquet structure holds.
    Set = 10,
    /// A map, which no parquet structure holds.
    Map = 11,
    /// Fields up to a stop byte.
    Struct = 12,
}

impl WireType {
    const ALL: [Self; 12] = [
        Self::BoolTrue,
        Self::BoolFalse,
        Self::Byte,
        Self::I16,
        Self::I32,
        Self::I64,
        Self::Double,
        Self::Binary,
        Self::List,
        Self::Set,
        Self::Map,
        Self::Struct,
    ];

    /// The code the wire type is sent as.
    pub(super) fn code(self) -> u8 {
        self as u8
    }

    fn from_code(code: u8) -> Option<Self> {
        Self::ALL.into_iter().find(|wire| wire.code() == code)
    }
}

impl fmt::Display for WireType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

/// A `PageHeader` field a fault names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HeaderField {
    /// `type`: which kind of page follows.
    PageType,
    /// `uncompressed_page_size`: the page body's bytes once decompressed,
    /// which parquet reserves before it decompresses.
    UncompressedPageSize,
    /// `compressed_page_size`: the page body's bytes as stored.
    CompressedPageSize,
    /// `data_page_header`, required on a v1 data page.
    DataPageHeader,
    /// `data_page_header_v2`, required on a v2 data page.
    DataPageHeaderV2,
    /// `dictionary_page_header`, required on a dictionary page.
    DictionaryPageHeader,
    /// `num_values` of the page-specific header; a dictionary page's sizes
    /// the dictionary parquet decodes into.
    NumValues,
    /// `definition_levels_byte_length` of a `data_page_header_v2`.
    DefinitionLevelsByteLength,
    /// `repetition_levels_byte_length` of a `data_page_header_v2`.
    RepetitionLevelsByteLength,
    /// The sum of a `data_page_header_v2`'s two level lengths: the bytes at
    /// the start of the page body that parquet copies uncompressed, which
    /// it adds as `i32`s before it compares them with the uncompressed size.
    LevelsByteLength,
}

impl fmt::Display for HeaderField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::PageType => "type",
            Self::UncompressedPageSize => "uncompressed_page_size",
            Self::CompressedPageSize => "compressed_page_size",
            Self::DataPageHeader => "data_page_header",
            Self::DataPageHeaderV2 => "data_page_header_v2",
            Self::DictionaryPageHeader => "dictionary_page_header",
            Self::NumValues => "num_values",
            Self::DefinitionLevelsByteLength => "definition_levels_byte_length",
            Self::RepetitionLevelsByteLength => "repetition_levels_byte_length",
            Self::LevelsByteLength => {
                "definition_levels_byte_length + repetition_levels_byte_length"
            }
        })
    }
}

/// A page of a column chunk, as a fault names it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChunkPage {
    /// The dictionary page: the bytes between the chunk's start and its
    /// first data page.
    Dictionary,
    /// A data page, counted among the chunk's data pages in its offset
    /// index.
    Data(usize),
}

impl fmt::Display for ChunkPage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Dictionary => f.write_str("the dictionary page"),
            Self::Data(page) => write!(f, "data page {page}"),
        }
    }
}

/// Why a page header is not one parquet reads to the numbers this reader
/// reads, or not a header of the page its place in the chunk holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum PageHeaderFault {
    /// The page's bytes end before its header does.
    #[error("the header runs past its page")]
    Truncated,
    /// A varint carries more than 64 bits.
    #[error("a varint carries more than 64 bits")]
    VarintTooLong,
    /// An integer lies outside the width its field is read as.
    #[error("an integer lies outside the width of its field")]
    OutOfRange,
    /// A field id's delta carries it past `i16::MAX`.
    #[error("a field id overflows")]
    FieldIdOverflow,
    /// A type code that is none of the compact protocol's.
    #[error("type code {0} is not a compact thrift type")]
    UnknownWireType(u8),
    /// A field parquet reads, sent as another wire type than the one
    /// parquet reads it as; parquet would read its bytes as its own type
    /// and part ways with this reader over every byte after it.
    #[error("field {field} is sent as {wire}, not as parquet reads it")]
    WireTypeMismatch {
        /// The field's id within its struct.
        field: i16,
        /// The wire type it was sent as.
        wire: WireType,
    },
    /// A set, a map, or a non-empty list of booleans. Parquet refuses to
    /// skip the first two, and skips a boolean element without consuming
    /// its byte, so no reading of the bytes after one agrees with
    /// parquet's.
    #[error("a {0} cannot be skipped as parquet skips it")]
    Unskippable(WireType),
    /// Structs or lists nested deeper than parquet skips.
    #[error("fields nest deeper than {SKIP_DEPTH} levels")]
    TooDeep,
    /// A field the header needs is absent.
    #[error("{0} is missing")]
    Missing(HeaderField),
    /// A size or a count is negative.
    #[error("{0} is negative")]
    Negative(HeaderField),
    /// A page type parquet does not know.
    #[error("page type {0} is unknown")]
    UnknownPageType(i32),
    /// A page type other than its place holds: a data page's location in
    /// the offset index holds a data page, and the bytes before the first
    /// data page hold the dictionary.
    #[error("a {0} page where its place holds another")]
    UnexpectedPageType(PageType),
}

/// What a page header says the page is, with its value count.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PageBody {
    Data { num_values: u32 },
    DataV2 { num_values: u32 },
    Dictionary { num_values: u32 },
    Index,
}

impl PageBody {
    pub(super) fn page_type(self) -> PageType {
        match self {
            Self::Data { .. } => PageType::DATA_PAGE,
            Self::DataV2 { .. } => PageType::DATA_PAGE_V2,
            Self::Dictionary { .. } => PageType::DICTIONARY_PAGE,
            Self::Index => PageType::INDEX_PAGE,
        }
    }
}

/// The numbers of a page header parquet sizes buffers by, and the bytes
/// the header takes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct PageHeader {
    pub(super) body: PageBody,
    pub(super) uncompressed_page_size: u32,
    pub(super) compressed_page_size: u32,
    /// The sum of the level lengths of a `data_page_header_v2`, when the
    /// header carries one, whatever its page type: parquet adds the two
    /// as `i32`s on any page that carries the struct.
    pub(super) levels_byte_length: Option<u64>,
    /// The header's own bytes: the page body starts here.
    pub(super) header_len: usize,
}

impl PageHeader {
    /// The page header at the start of `bytes`, as parquet reads it.
    pub(super) fn read(bytes: &[u8]) -> std::result::Result<Self, PageHeaderFault> {
        let mut input = Compact { bytes, at: 0 };
        let mut page_type = None;
        let mut uncompressed = None;
        let mut compressed = None;
        let mut data = None;
        let mut data_v2 = None;
        let mut dictionary = None;
        let mut last = 0;
        while let Some((wire, id)) = input.field_begin(last)? {
            match id {
                TYPE_FIELD => page_type = Some(input.i32_field(id, wire)?),
                UNCOMPRESSED_PAGE_SIZE_FIELD => uncompressed = Some(input.i32_field(id, wire)?),
                COMPRESSED_PAGE_SIZE_FIELD => compressed = Some(input.i32_field(id, wire)?),
                CRC_FIELD => {
                    input.i32_field(id, wire)?;
                }
                DATA_PAGE_HEADER_FIELD => {
                    data = Some(input.page_specific(id, wire, DATA_PAGE_HEADER_FIELDS)?);
                }
                INDEX_PAGE_HEADER_FIELD => {
                    input.page_specific(id, wire, &[])?;
                }
                DICTIONARY_PAGE_HEADER_FIELD => {
                    dictionary =
                        Some(input.page_specific(id, wire, DICTIONARY_PAGE_HEADER_FIELDS)?);
                }
                DATA_PAGE_HEADER_V2_FIELD => {
                    data_v2 = Some(input.page_specific(id, wire, DATA_PAGE_HEADER_V2_FIELDS)?);
                }
                _ => input.skip(wire, SKIP_DEPTH)?,
            }
            last = id;
        }
        let page_type = page_type.ok_or(PageHeaderFault::Missing(HeaderField::PageType))?;
        let body = match PageType::VARIANTS
            .iter()
            .find(|known| **known as i32 == page_type)
            .ok_or(PageHeaderFault::UnknownPageType(page_type))?
        {
            PageType::DATA_PAGE => PageBody::Data {
                num_values: num_values(data, HeaderField::DataPageHeader)?,
            },
            PageType::DATA_PAGE_V2 => PageBody::DataV2 {
                num_values: num_values(data_v2, HeaderField::DataPageHeaderV2)?,
            },
            PageType::DICTIONARY_PAGE => PageBody::Dictionary {
                num_values: num_values(dictionary, HeaderField::DictionaryPageHeader)?,
            },
            PageType::INDEX_PAGE => PageBody::Index,
        };
        Ok(Self {
            body,
            uncompressed_page_size: non_negative(uncompressed, HeaderField::UncompressedPageSize)?,
            compressed_page_size: non_negative(compressed, HeaderField::CompressedPageSize)?,
            levels_byte_length: data_v2.map(PageSpecific::levels_byte_length).transpose()?,
            header_len: input.at,
        })
    }
}

/// A page-specific header as read: its `num_values` and, in a v2 data
/// page header, its level lengths, each when it has one.
#[derive(Clone, Copy)]
struct PageSpecific {
    num_values: Option<i32>,
    definition_levels_byte_length: Option<i32>,
    repetition_levels_byte_length: Option<i32>,
}

impl PageSpecific {
    /// The sum of a v2 data page header's level lengths, each of which
    /// parquet requires and refuses when negative.
    fn levels_byte_length(self) -> std::result::Result<u64, PageHeaderFault> {
        let definition = non_negative(
            self.definition_levels_byte_length,
            HeaderField::DefinitionLevelsByteLength,
        )?;
        let repetition = non_negative(
            self.repetition_levels_byte_length,
            HeaderField::RepetitionLevelsByteLength,
        )?;
        Ok(u64::from(definition) + u64::from(repetition))
    }
}

/// The value count of page-specific header `field`, `header` as read:
/// missing when the header or its `num_values` is.
fn num_values(
    header: Option<PageSpecific>,
    field: HeaderField,
) -> std::result::Result<u32, PageHeaderFault> {
    let header = header.ok_or(PageHeaderFault::Missing(field))?;
    non_negative(header.num_values, HeaderField::NumValues)
}

fn non_negative(
    value: Option<i32>,
    field: HeaderField,
) -> std::result::Result<u32, PageHeaderFault> {
    let value = value.ok_or(PageHeaderFault::Missing(field))?;
    u32::try_from(value).map_err(|_| PageHeaderFault::Negative(field))
}

/// Compact-protocol input over the bytes of one page.
struct Compact<'a> {
    bytes: &'a [u8],
    at: usize,
}

type Parsed<T> = std::result::Result<T, PageHeaderFault>;

impl Compact<'_> {
    fn byte(&mut self) -> Parsed<u8> {
        let byte = *self.bytes.get(self.at).ok_or(PageHeaderFault::Truncated)?;
        self.at += 1;
        Ok(byte)
    }

    fn advance(&mut self, len: usize) -> Parsed<()> {
        self.at = self
            .at
            .checked_add(len)
            .filter(|end| *end <= self.bytes.len())
            .ok_or(PageHeaderFault::Truncated)?;
        Ok(())
    }

    /// An unsigned varint of at most 64 bits. Parquet reads a longer one
    /// by dropping the bits past 64, so one is refused here.
    fn varint(&mut self) -> Parsed<u64> {
        let mut value = 0;
        for shift in (0..u64::BITS).step_by(VARINT_PAYLOAD_BITS) {
            let byte = self.byte()?;
            let payload = u64::from(byte & !VARINT_CONTINUES);
            let shifted = payload << shift;
            if shifted >> shift != payload {
                return Err(PageHeaderFault::VarintTooLong);
            }
            value |= shifted;
            if byte & VARINT_CONTINUES == 0 {
                return Ok(value);
            }
        }
        Err(PageHeaderFault::VarintTooLong)
    }

    fn zigzag(&mut self) -> Parsed<i64> {
        let value = self.varint()?;
        Ok((value >> 1).cast_signed() ^ -(value & 1).cast_signed())
    }

    /// An `i32`. Parquet truncates a wider zigzag value to 32 bits, so one
    /// is refused here.
    fn i32(&mut self) -> Parsed<i32> {
        i32::try_from(self.zigzag()?).map_err(|_| PageHeaderFault::OutOfRange)
    }

    /// The next field's wire type and id, or `None` at the struct's stop
    /// byte, reading ids as parquet does: a delta from `last` in the high
    /// four bits, or a zigzag `i16` after the header byte when they are
    /// zero.
    fn field_begin(&mut self, last: i16) -> Parsed<Option<(WireType, i16)>> {
        let header = self.byte()?;
        let code = header & NIBBLE_MASK;
        if code == STOP {
            return Ok(None);
        }
        let wire = WireType::from_code(code).ok_or(PageHeaderFault::UnknownWireType(code))?;
        let delta = header >> NIBBLE_BITS;
        let id = if delta == 0 {
            i16::try_from(self.zigzag()?).map_err(|_| PageHeaderFault::OutOfRange)?
        } else {
            last.checked_add(i16::from(delta))
                .ok_or(PageHeaderFault::FieldIdOverflow)?
        };
        Ok(Some((wire, id)))
    }

    /// Field `id`, sent as `wire`, which parquet reads as an `i32` varint.
    fn i32_field(&mut self, id: i16, wire: WireType) -> Parsed<i32> {
        sent_as(id, wire, WireType::I32)?;
        self.i32()
    }

    /// Page-specific header `id`, sent as `wire`: a struct whose fields
    /// `fields` lists, by id from `NUM_VALUES_FIELD`, parquet reads as
    /// their scalar type, and whose other fields it skips. Its `num_values`
    /// and level lengths when it has them; parquet keeps the last of
    /// repeated fields, and so does this.
    fn page_specific(
        &mut self,
        id: i16,
        wire: WireType,
        fields: &[Scalar],
    ) -> Parsed<PageSpecific> {
        sent_as(id, wire, WireType::Struct)?;
        let mut read = PageSpecific {
            num_values: None,
            definition_levels_byte_length: None,
            repetition_levels_byte_length: None,
        };
        let mut last = 0;
        while let Some((wire, id)) = self.field_begin(last)? {
            let scalar = id
                .checked_sub(NUM_VALUES_FIELD)
                .and_then(|at| usize::try_from(at).ok())
                .and_then(|at| fields.get(at));
            match scalar {
                Some(Scalar::I32) => {
                    let value = Some(self.i32_field(id, wire)?);
                    // Of the three headers' `fields`, only a v2 data page
                    // header's reach the level lengths' ids.
                    match id {
                        NUM_VALUES_FIELD => read.num_values = value,
                        DEFINITION_LEVELS_BYTE_LENGTH_FIELD => {
                            read.definition_levels_byte_length = value;
                        }
                        REPETITION_LEVELS_BYTE_LENGTH_FIELD => {
                            read.repetition_levels_byte_length = value;
                        }
                        _ => {}
                    }
                }
                Some(Scalar::Bool) => {
                    if !matches!(wire, WireType::BoolTrue | WireType::BoolFalse) {
                        return Err(PageHeaderFault::WireTypeMismatch { field: id, wire });
                    }
                }
                None => self.skip(wire, SKIP_DEPTH)?,
            }
            last = id;
        }
        Ok(read)
    }

    /// Skips a value of `wire` as parquet's `skip_till_depth` does, `depth`
    /// levels of nesting left.
    fn skip(&mut self, wire: WireType, depth: u8) -> Parsed<()> {
        let inner = depth.checked_sub(1).ok_or(PageHeaderFault::TooDeep)?;
        match wire {
            WireType::BoolTrue | WireType::BoolFalse => Ok(()),
            WireType::Byte => self.advance(size_of::<u8>()),
            WireType::I16 | WireType::I32 | WireType::I64 => self.varint().map(drop),
            WireType::Double => self.advance(size_of::<f64>()),
            WireType::Binary => {
                let len = self.varint()?;
                self.advance(approx::length(len))
            }
            WireType::Struct => {
                let mut last = 0;
                while let Some((wire, id)) = self.field_begin(last)? {
                    self.skip(wire, inner)?;
                    last = id;
                }
                Ok(())
            }
            WireType::List => {
                let (element, count) = self.list_begin()?;
                if count == 0 {
                    return Ok(());
                }
                match element {
                    WireType::BoolTrue | WireType::BoolFalse | WireType::Set | WireType::Map => {
                        Err(PageHeaderFault::Unskippable(element))
                    }
                    WireType::Byte
                    | WireType::I16
                    | WireType::I32
                    | WireType::I64
                    | WireType::Double
                    | WireType::Binary
                    | WireType::List
                    | WireType::Struct => {
                        for _ in 0..count {
                            self.skip(element, inner)?;
                        }
                        Ok(())
                    }
                }
            }
            WireType::Set | WireType::Map => Err(PageHeaderFault::Unskippable(wire)),
        }
    }

    /// A list header's element type and count, as parquet reads them: a
    /// zero byte is an empty list, whatever type it names.
    fn list_begin(&mut self) -> Parsed<(WireType, usize)> {
        let header = self.byte()?;
        if header == 0 {
            return Ok((WireType::Byte, 0));
        }
        let code = header & NIBBLE_MASK;
        let element = WireType::from_code(code).ok_or(PageHeaderFault::UnknownWireType(code))?;
        let count = header >> NIBBLE_BITS;
        let count = if count == LIST_COUNT_FOLLOWS {
            let count = i32::try_from(self.varint()?).map_err(|_| PageHeaderFault::OutOfRange)?;
            usize::try_from(count).map_err(|_| PageHeaderFault::OutOfRange)?
        } else {
            usize::from(count)
        };
        Ok((element, count))
    }
}

/// Field `field`, sent as `wire`, is one parquet reads as `expected`; any
/// other wire type is refused.
fn sent_as(field: i16, wire: WireType, expected: WireType) -> Parsed<()> {
    if wire == expected {
        Ok(())
    } else {
        Err(PageHeaderFault::WireTypeMismatch { field, wire })
    }
}

/// Refuses, before parquet's page reader sees them, the pages of the key
/// column chunk `chunk` in `segments` whose headers do not decode as
/// parquet reads them (`FileFault::MalformedPageHeader`) or declare more
/// than their page can hold (`FileFault::PageHeaderOverBound`):
///
/// - a data page's uncompressed size above its rows in the offset index
///   times `MOST_BYTES_PER_VALUE`, plus `PAGE_ENCODING_ALLOWANCE_BYTES`;
/// - a dictionary page's value count above the row group's rows, since a
///   dictionary holds each distinct value once, and its uncompressed size
///   above that count times `MOST_BYTES_PER_VALUE`, plus the allowance;
/// - any key page's uncompressed size above `MOST_KEY_PAGE_BYTES`, and a
///   dictionary page's value count above the entries that ceiling holds
///   at `fewest_entry_bytes` each;
/// - a compressed size that runs past the page's place in the chunk;
/// - level lengths of a `data_page_header_v2` whose sum is above the
///   page's uncompressed size. Parquet adds the two as `i32`s before it
///   compares them with that size, and two that pass it overflow the add,
///   which panics where overflow checks are on; a sum within an
///   uncompressed size, itself at most `i32::MAX`, cannot overflow.
///
/// Where a span bound and the ceiling both apply, the lower is the one a
/// refusal names. A page whose keys are at most the cap stays within the
/// span bounds, since the allowances cover what an encoding adds to them,
/// and a page of up to sixteen times `PAGE_ROWS` such keys stays within
/// the ceiling. A page of keys far past the cap may exceed them, and is
/// then refused here, before `RefusalReason::KeyTooLong` could name it.
/// The span bounds follow the footer's row counts, which `inspect` checked
/// against one another but not against anything the pages hold, so a
/// footer that claims a row group of billions of rows widens its last
/// page's span bound with it; the ceiling does not move with the footer.
/// Parquet holds a dictionary entry in memory in 8 bytes for an `INT64`
/// and 32 for a byte array's handle, so a dictionary at the entry bound
/// takes up to 64 MiB for an `INT64` key, 512 MiB for a `BYTE_ARRAY` key,
/// and 2 GiB divided by the width for a `FIXED_LEN_BYTE_ARRAY` key (128
/// MiB for a uuid). A `FIXED_LEN_BYTE_ARRAY` key of width 0 has no entry
/// bound from the ceiling, only the row bound. The value counts
/// inside a page body (a delta encoding's lengths) are parquet's to read
/// and are not checked here.
pub(super) fn check_page_headers(
    chunk: ColumnChunk<'_>,
    shape: KeyShape,
    segments: &Segments,
) -> Result<()> {
    let most_entries = fewest_entry_bytes(shape, chunk.column.descriptor().type_length())
        .map(|bytes| MOST_KEY_PAGE_BYTES / bytes);
    if let Some(range) = chunk.dictionary_range() {
        check_page(chunk, ChunkPage::Dictionary, range, most_entries, segments)?;
    }
    for page in 0..chunk.page_locations().len() {
        check_page(
            chunk,
            ChunkPage::Data(page),
            chunk.page_range(page),
            most_entries,
            segments,
        )?;
    }
    Ok(())
}

/// One page of `check_page_headers`, at `range` of the file; a dictionary
/// page may count at most `most_entries` entries, when that is `Some`.
fn check_page(
    chunk: ColumnChunk<'_>,
    page: ChunkPage,
    range: Range<u64>,
    most_entries: Option<u64>,
    segments: &Segments,
) -> Result<()> {
    let row_group = chunk.row_group();
    let bytes = segments.get_bytes(range.start, approx::length(range.end - range.start))?;
    let malformed = |fault| {
        file_fault(
            chunk.column.path,
            FileFault::MalformedPageHeader {
                row_group,
                page,
                fault,
            },
        )
    };
    let over = |field, declared, allowed| {
        file_fault(
            chunk.column.path,
            FileFault::PageHeaderOverBound {
                row_group,
                page,
                field,
                declared,
                allowed,
            },
        )
    };
    let header = PageHeader::read(&bytes).map_err(malformed)?;
    let values = match (page, header.body) {
        (ChunkPage::Dictionary, PageBody::Dictionary { num_values }) => {
            let declared = u64::from(num_values);
            let rows = approx::count(row_index(chunk.rows()));
            let allowed = most_entries.map_or(rows, |most| rows.min(most));
            if declared > allowed {
                return Err(over(HeaderField::NumValues, declared, allowed));
            }
            declared
        }
        (ChunkPage::Data(at), PageBody::Data { .. } | PageBody::DataV2 { .. }) => {
            let span = chunk.page_rows(at);
            approx::count(row_index(span.end - span.start))
        }
        (
            ChunkPage::Dictionary,
            PageBody::Data { .. } | PageBody::DataV2 { .. } | PageBody::Index,
        )
        | (ChunkPage::Data(_), PageBody::Dictionary { .. } | PageBody::Index) => {
            return Err(malformed(PageHeaderFault::UnexpectedPageType(
                header.body.page_type(),
            )));
        }
    };
    let allowed = values
        .saturating_mul(MOST_BYTES_PER_VALUE)
        .saturating_add(PAGE_ENCODING_ALLOWANCE_BYTES)
        .min(MOST_KEY_PAGE_BYTES);
    let uncompressed = u64::from(header.uncompressed_page_size);
    if uncompressed > allowed {
        return Err(over(
            HeaderField::UncompressedPageSize,
            uncompressed,
            allowed,
        ));
    }
    if let Some(levels) = header.levels_byte_length
        && levels > uncompressed
    {
        return Err(over(HeaderField::LevelsByteLength, levels, uncompressed));
    }
    let room = approx::count(bytes.len() - header.header_len);
    let declared = u64::from(header.compressed_page_size);
    if declared > room {
        return Err(over(HeaderField::CompressedPageSize, declared, room));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, FixedSizeBinaryArray, Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::Bytes;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::{Compression, Encoding, ZstdLevel};
    use parquet::column::page::{Page, PageReader};
    use parquet::file::metadata::{ColumnChunkMetaData, PageIndexPolicy, ParquetMetaDataReader};
    use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
    use parquet::file::serialized_reader::SerializedPageReader;
    use parquet::schema::types::ColumnPath;

    use super::*;
    use crate::contract::PAGE_ROWS;
    use crate::generation::testing::page_rows;
    use crate::key::UUID_BYTES;

    /// Row groups in the written files, so every chunk is checked more
    /// than once.
    const ROW_GROUPS: usize = 2;
    /// Pages per row group in the written files.
    const PAGES_PER_GROUP: usize = 3;
    /// Every `NULL_EVERY`th value of the nullable columns is null.
    const NULL_EVERY: usize = 7;
    /// The width of the fixed-width column.
    const FIXED_WIDTH: i32 = 16;

    fn rows() -> usize {
        page_rows() * PAGES_PER_GROUP * ROW_GROUPS
    }

    /// A file of a nullable long, a nullable string, and a fixed-width
    /// column, written with `properties` in row groups of
    /// `PAGES_PER_GROUP` pages.
    fn written(properties: parquet::file::properties::WriterPropertiesBuilder) -> Bytes {
        let rows = rows();
        let nullable = |i: usize| (!i.is_multiple_of(NULL_EVERY)).then_some(i);
        let longs: Int64Array = (0..rows)
            .map(|i| nullable(i).map(|i| i64::try_from(i).unwrap()))
            .collect();
        let strings: StringArray = (0..rows)
            .map(|i| nullable(i).map(|i| format!("key:{i:08}")))
            .collect();
        let width = usize::try_from(FIXED_WIDTH).unwrap();
        let fixed = FixedSizeBinaryArray::try_from_iter(
            (0..rows).map(|i| format!("{i:0>width$}").into_bytes()),
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("long", DataType::Int64, true),
            Field::new("string", DataType::Utf8, true),
            Field::new("fixed", DataType::FixedSizeBinary(FIXED_WIDTH), false),
        ]));
        let columns: Vec<ArrayRef> = vec![Arc::new(longs), Arc::new(strings), Arc::new(fixed)];
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
        let properties = properties
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_data_page_row_count_limit(page_rows())
            .set_write_batch_size(page_rows())
            .set_max_row_group_row_count(Some(page_rows() * PAGES_PER_GROUP))
            .build();
        let mut out = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut out, schema, Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Bytes::from(out)
    }

    /// The value count a header's body declares.
    fn num_values_of(body: PageBody) -> u32 {
        match body {
            PageBody::Data { num_values }
            | PageBody::DataV2 { num_values }
            | PageBody::Dictionary { num_values } => num_values,
            PageBody::Index => panic!("parquet writes no index page"),
        }
    }

    /// Every page of every column chunk of `file`, read here from the
    /// chunk's bytes one header after another, against parquet's page
    /// reader walking the same chunk by the same headers: the same page
    /// type and value count, an uncompressed size that is the length of
    /// the page parquet decoded, a header and compressed size that step to
    /// the next page where parquet's offset index says it starts, level
    /// lengths that add to what parquet read from a v2 page, and sums that
    /// are the chunk's sizes in the footer. The pages compared, and the v2
    /// pages among them whose levels take bytes.
    fn assert_read_as_parquet_reads(file: &Bytes) -> (usize, usize) {
        let metadata = ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Required)
            .parse_and_finish(file)
            .unwrap();
        let offset_index = metadata.offset_index().unwrap();
        let (mut compared, mut with_levels) = (0, 0);
        for (row_group, group) in metadata.row_groups().iter().enumerate() {
            for (leaf, chunk) in group.columns().iter().enumerate() {
                let rows = row_index(group.num_rows());
                let mut parquet =
                    SerializedPageReader::new(Arc::new(file.clone()), chunk, rows, None).unwrap();
                let mut locations = offset_index[row_group][leaf].page_locations().iter();
                let (start, len) = chunk.byte_range();
                let end = approx::length(start + len);
                let mut at = approx::length(start);
                let (mut compressed, mut uncompressed) = (0, 0);
                while let Some(page) = parquet.get_next_page().unwrap() {
                    let header = PageHeader::read(&file[at..end]).unwrap();
                    assert_eq!(header.body.page_type(), page.page_type());
                    assert_eq!(num_values_of(header.body), page.num_values());
                    let levels = match &page {
                        Page::DataPageV2 {
                            def_levels_byte_len,
                            rep_levels_byte_len,
                            ..
                        } => {
                            Some(u64::from(*def_levels_byte_len) + u64::from(*rep_levels_byte_len))
                        }
                        Page::DataPage { .. } | Page::DictionaryPage { .. } => None,
                    };
                    assert_eq!(header.levels_byte_length, levels);
                    if levels.is_some_and(|levels| levels > 0) {
                        with_levels += 1;
                    }
                    assert_eq!(
                        approx::length(header.uncompressed_page_size),
                        page.buffer().len()
                    );
                    let page_len = header.header_len + approx::length(header.compressed_page_size);
                    if page.is_data_page() {
                        let location = locations.next().unwrap();
                        assert_eq!(approx::length(location.offset.cast_unsigned()), at);
                        assert_eq!(
                            approx::length(location.compressed_page_size.cast_unsigned()),
                            page_len
                        );
                    }
                    compressed += page_len;
                    uncompressed +=
                        header.header_len + approx::length(header.uncompressed_page_size);
                    at += page_len;
                    compared += 1;
                }
                assert_eq!(at, end, "row group {row_group} column {leaf}");
                assert!(locations.next().is_none());
                assert_eq!(
                    approx::count(compressed),
                    chunk.compressed_size().cast_unsigned()
                );
                assert_eq!(
                    approx::count(uncompressed),
                    chunk.uncompressed_size().cast_unsigned()
                );
            }
        }
        (compared, with_levels)
    }

    /// How many times the largest contract key page the ceiling holds.
    const CEILING_OVER_CONTRACT_PAGE: u64 = 16;

    /// The ceiling holds the contract's largest key page, `PAGE_ROWS` keys
    /// at the cap with their encoding and the page's allowance, sixteen
    /// times over.
    #[test]
    fn test_the_ceiling_holds_the_largest_contract_key_page_sixteen_times() {
        let largest = u64::try_from(PAGE_ROWS).unwrap() * MOST_BYTES_PER_VALUE
            + PAGE_ENCODING_ALLOWANCE_BYTES;
        assert!(largest * CEILING_OVER_CONTRACT_PAGE <= MOST_KEY_PAGE_BYTES);
    }

    /// A dictionary entry takes at least an `INT64`'s 8 bytes, a byte
    /// array's length prefix, or a fixed-width column's width; a width of
    /// 0 bounds nothing.
    #[test]
    fn test_fewest_entry_bytes_is_the_encoded_minimum_of_each_key_shape() {
        let width = i32::try_from(UUID_BYTES).unwrap();
        assert_eq!(
            fewest_entry_bytes(KeyShape::Long, 0),
            Some(approx::count(size_of::<i64>()))
        );
        for shape in [KeyShape::Str, KeyShape::Bin] {
            assert_eq!(
                fewest_entry_bytes(shape, 0),
                Some(approx::count(size_of::<u32>()))
            );
        }
        for shape in [KeyShape::FixedBin, KeyShape::Uuid] {
            assert_eq!(
                fewest_entry_bytes(shape, width),
                Some(approx::count(UUID_BYTES))
            );
        }
        assert_eq!(fewest_entry_bytes(KeyShape::FixedBin, 0), None);
    }

    #[test]
    fn test_read_matches_parquet_on_every_page_of_written_files() {
        let every = || WriterProperties::builder();
        let delta = |builder: parquet::file::properties::WriterPropertiesBuilder| {
            builder
                .set_dictionary_enabled(false)
                .set_column_encoding(ColumnPath::from("long"), Encoding::DELTA_BINARY_PACKED)
                .set_column_encoding(ColumnPath::from("string"), Encoding::DELTA_BYTE_ARRAY)
                .set_column_encoding(ColumnPath::from("fixed"), Encoding::DELTA_BYTE_ARRAY)
        };
        let files = [
            every().set_dictionary_enabled(false),
            every().set_dictionary_enabled(true),
            every()
                .set_statistics_enabled(EnabledStatistics::Page)
                .set_write_page_header_statistics(true),
            every().set_writer_version(WriterVersion::PARQUET_2_0),
            every()
                .set_writer_version(WriterVersion::PARQUET_2_0)
                .set_dictionary_enabled(false)
                .set_statistics_enabled(EnabledStatistics::Page)
                .set_write_page_header_statistics(true),
            delta(every().set_writer_version(WriterVersion::PARQUET_2_0)),
            delta(every()),
        ];
        let columns = 3;
        let mut with_levels = 0;
        for (file, properties) in files.into_iter().enumerate() {
            let (compared, levels) = assert_read_as_parquet_reads(&written(properties));
            assert!(
                compared >= columns * ROW_GROUPS * PAGES_PER_GROUP,
                "file {file}: {compared} pages"
            );
            with_levels += levels;
        }
        assert!(with_levels > 0, "no v2 page carried levels");
    }

    /// Compact thrift written by hand, for headers parquet's writer does
    /// not produce.
    struct Thrift {
        bytes: Vec<u8>,
        /// The last field id of each struct open, innermost last.
        last: Vec<i16>,
    }

    impl Thrift {
        /// Fields that follow one of id `last` in a struct already open.
        fn after(last: i16) -> Self {
            Self {
                bytes: Vec::new(),
                last: vec![last],
            }
        }

        fn new() -> Self {
            Self::after(0)
        }

        fn byte(&mut self, byte: u8) -> &mut Self {
            self.bytes.push(byte);
            self
        }

        fn varint(&mut self, mut value: u64) -> &mut Self {
            loop {
                let low = u8::try_from(value & u64::from(!VARINT_CONTINUES)).unwrap();
                value >>= VARINT_PAYLOAD_BITS;
                if value == 0 {
                    return self.byte(low);
                }
                self.byte(low | VARINT_CONTINUES);
            }
        }

        fn zigzag(&mut self, value: i64) -> &mut Self {
            self.varint(((value << 1) ^ (value >> (i64::BITS - 1))).cast_unsigned())
        }

        /// A field header: a delta from the last id when it fits the high
        /// four bits, the full id after it otherwise.
        fn field(&mut self, id: i16, wire: WireType) -> &mut Self {
            let last = self.last.last_mut().unwrap();
            let delta = id - *last;
            *last = id;
            match u8::try_from(delta) {
                Ok(delta) if delta > 0 && delta <= NIBBLE_MASK => {
                    self.byte((delta << NIBBLE_BITS) | wire.code())
                }
                Ok(_) | Err(_) => self.byte(wire.code()).zigzag(i64::from(id)),
            }
        }

        fn i32(&mut self, id: i16, value: i32) -> &mut Self {
            self.field(id, WireType::I32).zigzag(i64::from(value))
        }

        fn begin(&mut self, id: i16) -> &mut Self {
            self.field(id, WireType::Struct);
            self.last.push(0);
            self
        }

        fn end(&mut self) -> &mut Self {
            self.last.pop();
            self.byte(STOP)
        }

        fn list(&mut self, id: i16, element: WireType, count: u8) -> &mut Self {
            self.field(id, WireType::List);
            if count < LIST_COUNT_FOLLOWS {
                self.byte((count << NIBBLE_BITS) | element.code())
            } else {
                self.byte((LIST_COUNT_FOLLOWS << NIBBLE_BITS) | element.code())
                    .varint(u64::from(count))
            }
        }

        fn raw(&mut self, bytes: &[u8]) -> &mut Self {
            self.bytes.extend_from_slice(bytes);
            self
        }

        fn binary(&mut self, bytes: &[u8]) -> &mut Self {
            self.varint(approx::count(bytes.len())).raw(bytes)
        }

        fn take(&mut self) -> Vec<u8> {
            std::mem::take(&mut self.bytes)
        }
    }

    /// A one-column file of `page_rows()` longs in one page, its chunk's
    /// metadata, the page's header as read here, and the page's bytes.
    fn one_page() -> (ColumnChunkMetaData, PageHeader, Bytes) {
        let longs: Int64Array = (0..page_rows())
            .map(|i| i64::try_from(i).unwrap())
            .collect();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "long",
            DataType::Int64,
            false,
        )]));
        let columns: Vec<ArrayRef> = vec![Arc::new(longs)];
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
        let properties = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_dictionary_enabled(false)
            .build();
        let mut out = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut out, schema, Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let file = Bytes::from(out);
        let metadata = ParquetMetaDataReader::new()
            .parse_and_finish(&file)
            .unwrap();
        let chunk = metadata.row_group(0).column(0).clone();
        let (start, len) = chunk.byte_range();
        let page = file.slice(approx::length(start)..approx::length(start + len));
        let header = PageHeader::read(&page).unwrap();
        (chunk, header, page)
    }

    /// `crafted`, a header followed by `page`'s body, as parquet's page
    /// reader decodes it: the page, or the error it ends in.
    fn parquet_reads(
        chunk: &ColumnChunkMetaData,
        written: &PageHeader,
        page: &Bytes,
        crafted: &[u8],
    ) -> parquet::errors::Result<(PageType, u32, usize)> {
        let mut bytes = crafted.to_vec();
        bytes.extend_from_slice(&page[written.header_len..]);
        let len = i64::try_from(bytes.len()).unwrap();
        let chunk = chunk
            .clone()
            .into_builder()
            .set_data_page_offset(0)
            .set_dictionary_page_offset(None)
            .set_total_compressed_size(len)
            .build()
            .unwrap();
        let rows = page_rows();
        let mut reader =
            SerializedPageReader::new(Arc::new(Bytes::from(bytes)), &chunk, rows, None)?;
        let page = reader.get_next_page()?.unwrap();
        assert!(reader.get_next_page()?.is_none(), "one page");
        Ok((page.page_type(), page.num_values(), page.buffer().len()))
    }

    /// `written`'s header with `top` inserted after its data page header
    /// struct and `nested` at that struct's end.
    fn spliced(written: &[u8], nested: &[u8], top: &[u8]) -> Vec<u8> {
        let struct_stop = written.len() - 2;
        let mut crafted = written[..struct_stop].to_vec();
        crafted.extend_from_slice(nested);
        crafted.extend_from_slice(&written[struct_stop..=struct_stop]);
        crafted.extend_from_slice(top);
        crafted.push(STOP);
        crafted
    }

    /// Top-level fields after a data page header of every wire type
    /// parquet skips, where parquet's writer puts none: the index and
    /// dictionary page headers parquet reads on any page, unknown fields of
    /// each scalar type, lists of scalars, structs, and lists, an empty
    /// list, a list whose count follows its header, nested structs, and ids
    /// sent in full.
    fn every_wire_type() -> Vec<u8> {
        let mut t = Thrift::after(DATA_PAGE_HEADER_FIELD);
        let list_of_many = LIST_COUNT_FOLLOWS + 1;
        t.begin(INDEX_PAGE_HEADER_FIELD)
            .i32(1, 1)
            .field(2, WireType::Binary)
            .binary(b"index")
            .end()
            .begin(DICTIONARY_PAGE_HEADER_FIELD)
            .i32(NUM_VALUES_FIELD, 1)
            .i32(2, Encoding::PLAIN as i32)
            .field(3, WireType::BoolTrue)
            .end()
            .field(9, WireType::BoolTrue)
            .field(10, WireType::BoolFalse)
            .field(11, WireType::Byte)
            .byte(u8::MAX)
            .field(12, WireType::I16)
            .zigzag(i64::from(i16::MIN))
            .field(13, WireType::I64)
            .zigzag(i64::MIN)
            .field(14, WireType::Double)
            .raw(&std::f64::consts::PI.to_le_bytes())
            .field(15, WireType::Binary)
            .binary(b"binary")
            .list(16, WireType::I32, 3)
            .zigzag(1)
            .zigzag(-1)
            .zigzag(i64::from(i32::MAX));
        t.list(17, WireType::Struct, 2);
        t.last.push(0);
        t.i32(1, 1).end();
        t.last.push(0);
        t.field(2, WireType::Binary).binary(b"x").end();
        t.list(18, WireType::List, 1)
            .byte((2 << NIBBLE_BITS) | WireType::Byte.code())
            .byte(1)
            .byte(2)
            .field(19, WireType::List)
            .byte(0)
            .list(20, WireType::I64, list_of_many);
        for value in 0..list_of_many {
            t.zigzag(i64::from(value));
        }
        t.begin(21)
            .begin(1)
            .field(1, WireType::BoolFalse)
            .end()
            .end();
        t.i32(300, 1).field(-5, WireType::Binary).binary(b"full id");
        t.take()
    }

    /// Fields of every wire type parquet skips, where parquet's writer
    /// puts none, read here to the numbers parquet's page reader reads:
    /// the same page type and value count, and an uncompressed size that
    /// parquet's decompression agrees with.
    #[test]
    fn test_read_skips_every_field_as_parquet_skips_it() {
        let (chunk, written, page) = one_page();
        let header = &page[..written.header_len];
        assert_eq!(
            header[written.header_len - 1],
            STOP,
            "the header ends in its stop"
        );
        let every_wire_type = every_wire_type();
        let statistics = Thrift::after(4)
            .begin(5)
            .field(1, WireType::Binary)
            .binary(&[u8::MAX; 8])
            .field(2, WireType::Binary)
            .binary(&[0; 8])
            .field(3, WireType::I64)
            .zigzag(0)
            .field(7, WireType::BoolTrue)
            .end()
            .list(9, WireType::Binary, 1)
            .binary(b"nested")
            .take();
        let repeated = {
            let values = i32::try_from(num_values_of(written.body)).unwrap();
            Thrift::after(4)
                .i32(NUM_VALUES_FIELD, values + 1)
                .i32(NUM_VALUES_FIELD, values)
                .take()
        };
        let empty: &[u8] = &[];
        for (nested, top) in [
            (empty, empty),
            (empty, every_wire_type.as_slice()),
            (statistics.as_slice(), empty),
            (repeated.as_slice(), every_wire_type.as_slice()),
        ] {
            let crafted = spliced(header, nested, top);
            let read = PageHeader::read(&crafted).unwrap();
            assert_eq!(read.header_len, crafted.len());
            assert_eq!(
                (
                    read.body,
                    read.uncompressed_page_size,
                    read.compressed_page_size
                ),
                (
                    written.body,
                    written.uncompressed_page_size,
                    written.compressed_page_size
                )
            );
            assert_eq!(
                parquet_reads(&chunk, &written, &page, &crafted).unwrap(),
                (
                    PageType::DATA_PAGE,
                    num_values_of(written.body),
                    approx::length(written.uncompressed_page_size)
                )
            );
        }
    }

    /// Structs nested as deep as parquet skips them are skipped, and one
    /// deeper is refused here as parquet refuses it.
    #[test]
    fn test_read_refuses_nesting_deeper_than_parquet_skips() {
        let (chunk, written, page) = one_page();
        let header = &page[..written.header_len];
        let nested = |depth: u8| {
            let mut t = Thrift::after(DATA_PAGE_HEADER_FIELD);
            t.begin(9);
            for _ in 1..depth {
                t.begin(1);
            }
            for _ in 0..depth {
                t.end();
            }
            spliced(header, &[], &t.take())
        };
        let deepest = nested(SKIP_DEPTH);
        assert_eq!(PageHeader::read(&deepest).unwrap().body, written.body);
        assert!(parquet_reads(&chunk, &written, &page, &deepest).is_ok());
        let deeper = nested(SKIP_DEPTH + 1);
        assert_eq!(PageHeader::read(&deeper), Err(PageHeaderFault::TooDeep));
        assert!(parquet_reads(&chunk, &written, &page, &deeper).is_err());
    }

    #[test]
    fn test_read_refuses_every_prefix_of_a_header_as_truncated() {
        let (_, written, page) = one_page();
        for len in 0..written.header_len {
            assert_eq!(
                PageHeader::read(&page[..len]),
                Err(PageHeaderFault::Truncated),
                "{len} bytes"
            );
        }
    }

    /// A data page header of `uncompressed` and `compressed` bytes whose
    /// data page header holds `nested` after its value count, ending in
    /// `top`.
    fn header(uncompressed: i32, compressed: i32, nested: &[u8], top: &[u8]) -> Vec<u8> {
        let mut t = Thrift::new();
        t.i32(TYPE_FIELD, PageType::DATA_PAGE as i32)
            .i32(UNCOMPRESSED_PAGE_SIZE_FIELD, uncompressed)
            .i32(COMPRESSED_PAGE_SIZE_FIELD, compressed)
            .begin(DATA_PAGE_HEADER_FIELD)
            .i32(NUM_VALUES_FIELD, 1);
        t.bytes.extend_from_slice(nested);
        t.end();
        t.bytes.extend_from_slice(top);
        t.byte(STOP);
        t.take()
    }

    #[test]
    fn test_read_refuses_what_parquet_would_read_differently() {
        let size = 1;
        let fault = |nested: &[u8], top: &[u8]| {
            PageHeader::read(&header(size, size, nested, top)).unwrap_err()
        };
        let past_known = WireType::Struct.code() + 1;
        assert_eq!(
            fault(&[], &[(1 << NIBBLE_BITS) | past_known]),
            PageHeaderFault::UnknownWireType(past_known)
        );
        let crc_as_i64 = Thrift::after(DATA_PAGE_HEADER_FIELD)
            .field(CRC_FIELD, WireType::I64)
            .zigzag(0)
            .take();
        assert_eq!(
            fault(&[], &crc_as_i64),
            PageHeaderFault::WireTypeMismatch {
                field: CRC_FIELD,
                wire: WireType::I64
            }
        );
        let encoding_as_binary = Thrift::after(NUM_VALUES_FIELD)
            .field(2, WireType::Binary)
            .binary(&[0])
            .take();
        assert_eq!(
            fault(&encoding_as_binary, &[]),
            PageHeaderFault::WireTypeMismatch {
                field: 2,
                wire: WireType::Binary
            }
        );
        let dictionary_as_list = Thrift::after(DATA_PAGE_HEADER_FIELD)
            .list(DICTIONARY_PAGE_HEADER_FIELD, WireType::Byte, 0)
            .take();
        assert_eq!(
            fault(&[], &dictionary_as_list),
            PageHeaderFault::WireTypeMismatch {
                field: DICTIONARY_PAGE_HEADER_FIELD,
                wire: WireType::List
            }
        );
        let is_sorted_as_i32 = Thrift::after(DATA_PAGE_HEADER_FIELD)
            .begin(DICTIONARY_PAGE_HEADER_FIELD)
            .i32(NUM_VALUES_FIELD, 1)
            .i32(3, 1)
            .end()
            .take();
        assert_eq!(
            fault(&[], &is_sorted_as_i32),
            PageHeaderFault::WireTypeMismatch {
                field: 3,
                wire: WireType::I32
            }
        );
        for wire in [WireType::Set, WireType::Map] {
            let unskippable = Thrift::after(DATA_PAGE_HEADER_FIELD)
                .field(9, wire)
                .byte(0)
                .take();
            assert_eq!(fault(&[], &unskippable), PageHeaderFault::Unskippable(wire));
        }
        let booleans = Thrift::after(DATA_PAGE_HEADER_FIELD)
            .list(9, WireType::BoolFalse, 1)
            .byte(0)
            .take();
        assert_eq!(
            fault(&[], &booleans),
            PageHeaderFault::Unskippable(WireType::BoolFalse)
        );
        let no_booleans = Thrift::after(DATA_PAGE_HEADER_FIELD)
            .list(9, WireType::BoolFalse, 0)
            .take();
        assert!(PageHeader::read(&header(size, size, &[], &no_booleans)).is_ok());
    }

    #[test]
    fn test_read_refuses_integers_parquet_would_truncate() {
        let size = 1;
        let top = |bytes: &[u8]| {
            let mut top = Thrift::after(DATA_PAGE_HEADER_FIELD)
                .field(9, WireType::I64)
                .take();
            top.extend_from_slice(bytes);
            PageHeader::read(&header(size, size, &[], &top))
        };
        let continued = [VARINT_CONTINUES; 9];
        let mut widest = continued.to_vec();
        widest.push(1);
        assert!(top(&widest).is_ok(), "ten bytes carrying 64 bits");
        let mut wider = continued.to_vec();
        wider.push(2);
        assert_eq!(top(&wider), Err(PageHeaderFault::VarintTooLong));
        let mut longer = [VARINT_CONTINUES; 10].to_vec();
        longer.push(0);
        assert_eq!(top(&longer), Err(PageHeaderFault::VarintTooLong));

        let past_i32 = Thrift::new()
            .field(TYPE_FIELD, WireType::I32)
            .zigzag(i64::from(i32::MAX) + 1)
            .take();
        assert_eq!(
            PageHeader::read(&past_i32),
            Err(PageHeaderFault::OutOfRange)
        );
        let past_i16 = Thrift::new()
            .byte(WireType::I32.code())
            .zigzag(i64::from(i16::MAX) + 1)
            .take();
        assert_eq!(
            PageHeader::read(&past_i16),
            Err(PageHeaderFault::OutOfRange)
        );
        let overflowing_id = Thrift::after(DATA_PAGE_HEADER_FIELD)
            .byte(WireType::I32.code())
            .zigzag(i64::from(i16::MAX))
            .zigzag(0)
            .byte((1 << NIBBLE_BITS) | WireType::I32.code())
            .take();
        assert_eq!(
            PageHeader::read(&header(size, size, &[], &overflowing_id)),
            Err(PageHeaderFault::FieldIdOverflow)
        );
    }

    #[test]
    fn test_read_refuses_missing_negative_and_unknown_fields() {
        let size = 1;
        let without = |missing: HeaderField| {
            let mut t = Thrift::new();
            for (field, id, value) in [
                (
                    HeaderField::PageType,
                    TYPE_FIELD,
                    PageType::DATA_PAGE as i32,
                ),
                (
                    HeaderField::UncompressedPageSize,
                    UNCOMPRESSED_PAGE_SIZE_FIELD,
                    size,
                ),
                (
                    HeaderField::CompressedPageSize,
                    COMPRESSED_PAGE_SIZE_FIELD,
                    size,
                ),
            ] {
                if field != missing {
                    t.i32(id, value);
                }
            }
            if missing != HeaderField::DataPageHeader {
                t.begin(DATA_PAGE_HEADER_FIELD);
                if missing != HeaderField::NumValues {
                    t.i32(NUM_VALUES_FIELD, 1);
                }
                t.end();
            }
            PageHeader::read(&t.byte(STOP).take())
        };
        for missing in [
            HeaderField::PageType,
            HeaderField::UncompressedPageSize,
            HeaderField::CompressedPageSize,
            HeaderField::DataPageHeader,
            HeaderField::NumValues,
        ] {
            assert_eq!(without(missing), Err(PageHeaderFault::Missing(missing)));
        }
        assert_eq!(
            PageHeader::read(&header(size, -1, &[], &[])),
            Err(PageHeaderFault::Negative(HeaderField::CompressedPageSize))
        );
        let v2_without_levels = Thrift::new()
            .i32(TYPE_FIELD, PageType::DATA_PAGE_V2 as i32)
            .i32(UNCOMPRESSED_PAGE_SIZE_FIELD, size)
            .i32(COMPRESSED_PAGE_SIZE_FIELD, size)
            .begin(DATA_PAGE_HEADER_V2_FIELD)
            .i32(NUM_VALUES_FIELD, 1)
            .end()
            .byte(STOP)
            .take();
        assert_eq!(
            PageHeader::read(&v2_without_levels),
            Err(PageHeaderFault::Missing(
                HeaderField::DefinitionLevelsByteLength
            ))
        );
        let past_known = PageType::DATA_PAGE_V2 as i32 + 1;
        let unknown = Thrift::new()
            .i32(TYPE_FIELD, past_known)
            .i32(UNCOMPRESSED_PAGE_SIZE_FIELD, size)
            .i32(COMPRESSED_PAGE_SIZE_FIELD, size)
            .byte(STOP)
            .take();
        assert_eq!(
            PageHeader::read(&unknown),
            Err(PageHeaderFault::UnknownPageType(past_known))
        );
    }
}
