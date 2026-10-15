//! The verification record (RFD 017): what `verify` established of a data
//! file's keys (the fences, every row group's first and last key, the
//! longest key) and, on an ordered table, of its order column (the order
//! facts and the order fences, RFD 025), encoded into the Disk tier's
//! chunk file so that a restart rebuilds the index's view of the file
//! without decoding a key or an order value. The chunk file's payload
//! checksum covers the record, so a torn or changed one is never read;
//! what is read is still checked against the footer before it is
//! trusted, since every lookup indexes the footer's pages with the
//! fences (`decode`).

use std::fmt;
use std::mem::size_of;

use prost::Message as _;

use super::build::Verified;
use super::order_fences::{GroupFencesBuilder, OrderFences, Pair, PairPart};
use super::ordered::OrderVerified;
use super::page_header::MOST_VALUE_PAGE_BYTES;
use super::{Column, ColumnChunk, GroupRange, KeyShape, OrderLeaf, OrderShape};
use crate::approx;
use crate::contract::MAX_KEY_BYTES;
use crate::key::Key;
use crate::order::{OrderFacts, OrderValue};

/// The bytes a field's key takes in the record: one, since every field
/// number of the record's messages is below sixteen.
const FIELD_KEY_BYTES: u64 = 1;
/// The most bytes a varint takes: ten, the width of the largest `u64`.
const MOST_VARINT_BYTES: u64 = 10;

/// The record as the chunk file stores it. Keys are in `Key::as_bytes`
/// form: a long's eight little-endian bytes, a string's UTF-8, binary and
/// uuid keys as their bytes; the key type the record was verified under,
/// which the chunk file's header carries, says how they decode. Field
/// numbers are never reused.
#[derive(Clone, PartialEq, prost::Message)]
struct VerificationRecord {
    /// One per row group of the footer, in footer order.
    #[prost(message, repeated, tag = "1")]
    row_groups: Vec<RowGroupRecord>,
    /// The length in bytes of the file's longest key.
    #[prost(uint64, tag = "2")]
    max_key_bytes: u64,
    /// The order facts of a file verified under the ordered rules; absent
    /// under the key-value rules.
    #[prost(message, optional, tag = "3")]
    order: Option<OrderRecord>,
}

/// An ordered file's `OrderFacts` as the record stores them. Order values
/// are a long's eight little-endian bytes, a string's UTF-8, and binary and
/// fixed values as their bytes; the order column's type, which the footer
/// and the snapshot give, says how they decode.
#[derive(Clone, PartialEq, prost::Message)]
struct OrderRecord {
    /// The least order value of the file.
    #[prost(bytes = "vec", tag = "1")]
    min: Vec<u8>,
    /// The greatest order value of the file.
    #[prost(bytes = "vec", tag = "2")]
    max: Vec<u8>,
    /// Per row group of the footer, in footer order, its distinct keys.
    #[prost(uint64, repeated, tag = "3")]
    distinct_keys: Vec<u64>,
    /// The rows of the file's longest run of one key.
    #[prost(uint64, tag = "4")]
    longest_run: u64,
    /// Per row group of the footer, in footer order, the fence of each of
    /// its order pages.
    #[prost(message, repeated, tag = "5")]
    fences: Vec<FenceGroupRecord>,
}

/// One row group's order fences, in page order, as two lists of one entry
/// per order page: each fence's key in `Key::as_bytes` form, as the row
/// groups' keys, and its order value as the order facts hold one, a byte
/// value the prefix the fence keeps of the page's first value.
#[derive(Clone, PartialEq, prost::Message)]
struct FenceGroupRecord {
    #[prost(bytes = "vec", repeated, tag = "1")]
    keys: Vec<Vec<u8>>,
    #[prost(bytes = "vec", repeated, tag = "2")]
    orders: Vec<Vec<u8>>,
}

/// One row group of the record.
#[derive(Clone, PartialEq, prost::Message)]
struct RowGroupRecord {
    /// The key at the first row of each key page, the fences, in page
    /// order; the first is the row group's first key. None for a row group
    /// without rows.
    #[prost(bytes = "vec", repeated, tag = "1")]
    fences: Vec<Vec<u8>>,
    /// The row group's last key; empty for a row group without rows.
    #[prost(bytes = "vec", tag = "2")]
    last: Vec<u8>,
}

/// Why a stored record does not describe the file the footer does. Not an
/// error: the chunk file is stale and rebuilt. Display gives the
/// `snake_case` name the log carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RecordFault {
    /// The bytes do not decode as a record.
    Undecodable,
    /// The record holds another number of row groups than the footer.
    RowGroups {
        /// The row groups the record holds.
        stored: usize,
    },
    /// The longest key is over `contract::MAX_KEY_BYTES`, which `verify`
    /// refuses.
    LongestKey {
        /// The length the record claims.
        bytes: u64,
    },
    /// A row group holds another number of fences than the footer's key
    /// pages give it.
    Fences {
        /// The row group.
        row_group: usize,
        /// The fences the record holds for it.
        stored: usize,
    },
    /// A key that is not one of the key type: longer than the record's
    /// longest key, not the type's width, or a string that is not UTF-8.
    Key {
        /// The row group holding it.
        row_group: usize,
    },
    /// Keys out of the order `verify` establishes: the fences of a row
    /// group ascending and at or below its last key, and each row group
    /// starting above the one before it ends, strictly under the key-value
    /// rules and with equal keys admitted under the ordered rules, where a
    /// key's run may cross a page or a row-group boundary.
    Order {
        /// The row group where the order breaks.
        row_group: usize,
    },
    /// The order facts are absent under the ordered rules or present under
    /// the key-value rules, or do not describe the file: another count of
    /// row groups, a row group with more distinct keys than rows, or none
    /// while it has rows, a longest run of no row or of more than the file
    /// holds, an order value not of the order column's type, or a least
    /// value above the greatest.
    OrderFacts,
    /// The order fences do not describe the file: another count of row
    /// groups, or of fences in a row group than its order pages, a key not
    /// one of the key type or an order value not one of the order column's
    /// or a prefix of one, a fence at or below the one before it, or one
    /// outside its row group's keys, the first on another key than the row
    /// group's first.
    OrderFences {
        /// The row group where the fences fail, or the footer's row group
        /// count when the record holds another number of them.
        row_group: usize,
    },
}

impl fmt::Display for RecordFault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Undecodable => "undecodable",
            Self::RowGroups { .. } => "row_groups",
            Self::LongestKey { .. } => "longest_key",
            Self::Fences { .. } => "fences",
            Self::Key { .. } => "key",
            Self::Order { .. } => "order",
            Self::OrderFacts => "order_facts",
            Self::OrderFences { .. } => "order_fences",
        };
        f.write_str(name)
    }
}

/// `verified` as the chunk file stores it.
pub(super) fn encode(verified: &Verified) -> Vec<u8> {
    let mut groups = verified.groups.iter().peekable();
    let row_groups = verified
        .fences
        .iter()
        .enumerate()
        .map(|(row_group, fences)| RowGroupRecord {
            fences: fences.iter().map(key_bytes).collect(),
            last: groups
                .next_if(|group| group.row_group == row_group)
                .map_or_else(Vec::new, |group| key_bytes(&group.last)),
        })
        .collect();
    VerificationRecord {
        row_groups,
        max_key_bytes: approx::count(verified.max_key_bytes),
        order: verified.order.as_ref().map(|order| OrderRecord {
            min: order_bytes(&order.facts.min),
            max: order_bytes(&order.facts.max),
            distinct_keys: order.facts.distinct_keys.clone(),
            longest_run: order.facts.longest_run,
            fences: order
                .fences
                .groups()
                .iter()
                .map(|group| FenceGroupRecord {
                    keys: group.iter().map(|fence| part_bytes(fence.key)).collect(),
                    orders: group.iter().map(|fence| part_bytes(fence.order)).collect(),
                })
                .collect(),
        }),
    }
    .encode_to_vec()
}

/// A fence's key or order value as the record stores it: a long's eight
/// little-endian bytes, as `Key::as_bytes` and `order_bytes` store one, and
/// bytes as they are.
fn part_bytes(part: PairPart<'_>) -> Vec<u8> {
    match part {
        PairPart::Long(value) => value.to_le_bytes().to_vec(),
        PairPart::Bytes(bytes) => bytes.to_vec(),
    }
}

/// A key as the record stores it.
fn key_bytes(key: &Key) -> Vec<u8> {
    key.as_bytes().to_vec()
}

/// An order value as the record stores it.
fn order_bytes(value: &OrderValue) -> Vec<u8> {
    match value {
        OrderValue::Long(value) => value.to_le_bytes().to_vec(),
        OrderValue::Str(text) => text.as_bytes().to_vec(),
        OrderValue::Bytes(bytes) => bytes.clone(),
    }
}

/// The record in `bytes` as what `verify` of `column`, whose keys decode as
/// `shape`, would establish under the rules `order` names, the ordered
/// rules over that order column or the key-value rules for `None`, once it
/// describes the file the footer does: order facts and order fences under
/// the ordered rules alone (`order_facts`, `order_fences`), one row group
/// per footer row group, as many fences in each as `verify` records for the
/// footer's key pages (`fence_count`), every key one of the key type and no
/// longer than the longest key, which is no longer than
/// `contract::MAX_KEY_BYTES`, and the keys in the order `verify` checks
/// under those rules. A record that does not is the `RecordFault` it fails.
/// The checks cost one comparison per key page, per order page, and per
/// row group, not per row.
pub(super) fn decode(
    bytes: &[u8],
    column: &Column<'_>,
    shape: KeyShape,
    order: Option<OrderLeaf>,
) -> Result<Verified, RecordFault> {
    let record = VerificationRecord::decode(bytes).map_err(|error| {
        tracing::debug!(%error, path = column.path, "verification record does not decode");
        RecordFault::Undecodable
    })?;
    let stored_order = match (order, record.order) {
        (None, None) => None,
        (Some(order), Some(mut stored)) => {
            let fences = std::mem::take(&mut stored.fences);
            Some((order, order_facts(stored, order.shape, column)?, fences))
        }
        (None, Some(_)) | (Some(_), None) => return Err(RecordFault::OrderFacts),
    };
    // Under the ordered rules a key's run may cross a page or a row-group
    // boundary, so a fence may equal the one before it, and a row group
    // may start with the key the one before it ends with.
    let ascending = |before: &Key, after: &Key| match order {
        None => before < after,
        Some(_) => before <= after,
    };
    let total = column.metadata.num_row_groups();
    if record.row_groups.len() != total {
        return Err(RecordFault::RowGroups {
            stored: record.row_groups.len(),
        });
    }
    let longest = usize::try_from(record.max_key_bytes)
        .ok()
        .filter(|bytes| *bytes <= MAX_KEY_BYTES)
        .ok_or(RecordFault::LongestKey {
            bytes: record.max_key_bytes,
        })?;
    let width = usize::try_from(column.descriptor().type_length()).ok();
    let mut fences = Vec::with_capacity(total);
    let mut groups: Vec<GroupRange> = Vec::with_capacity(total);
    for (row_group, stored) in record.row_groups.into_iter().enumerate() {
        let expected = fence_count(column.group(row_group));
        if stored.fences.len() != expected {
            return Err(RecordFault::Fences {
                row_group,
                stored: stored.fences.len(),
            });
        }
        let key = |bytes: Vec<u8>| {
            key_of(shape, bytes, longest, width).ok_or(RecordFault::Key { row_group })
        };
        let run = stored
            .fences
            .into_iter()
            .map(&key)
            .collect::<Result<Vec<Key>, RecordFault>>()?;
        let Some(first) = run.first().cloned() else {
            if !stored.last.is_empty() {
                return Err(RecordFault::Key { row_group });
            }
            fences.push(run);
            continue;
        };
        let last = key(stored.last)?;
        let ordered = run.windows(2).all(|pair| ascending(&pair[0], &pair[1]))
            && run.last().is_some_and(|fence| *fence <= last)
            && groups
                .last()
                .is_none_or(|previous| ascending(&previous.last, &first));
        if !ordered {
            return Err(RecordFault::Order { row_group });
        }
        fences.push(run);
        groups.push(GroupRange {
            row_group,
            first,
            last,
        });
    }
    let has_keys = "inspect checked that the row groups sum to the file's rows, which are above zero, and that each chunk's first page starts at row zero, so a row group has a fence";
    let head = groups.first().expect(has_keys);
    let tail = groups.last().expect(has_keys);
    let key_range = (head.first.clone(), tail.last.clone());
    let order = match stored_order {
        None => None,
        Some((order, facts, stored)) => {
            let keys = Keyed {
                shape,
                longest,
                width,
                groups: &groups,
            };
            let fences = order_fences(stored, column, order, &keys)?;
            Some(OrderVerified { facts, fences })
        }
    };
    Ok(Verified {
        key_range,
        fences,
        groups,
        max_key_bytes: longest,
        order,
    })
}

/// What a fence's key is checked against: the key column's shape, the
/// longest key, a fixed-width column's width (`key_of`), and the row
/// groups' first and last keys.
struct Keyed<'a> {
    shape: KeyShape,
    longest: usize,
    width: Option<usize>,
    groups: &'a [GroupRange],
}

/// The order fences `stored` holds of `column`'s file, whose order column
/// is `order`, once they describe it: one list per footer row group, as
/// many fences in each as the order column's offset index gives it pages,
/// every key one of the key type (`key_of`) and every order value one of
/// the column's type or, for a byte value, a prefix of one (`fence_order`),
/// the fences strictly ascending through the file, and each row group's
/// fences within its keys, the first on its first key. Each fence's key is
/// that of its page's first row, so these are what `verify` records.
/// `RecordFault::OrderFences` otherwise.
fn order_fences(
    stored: Vec<FenceGroupRecord>,
    column: &Column<'_>,
    order: OrderLeaf,
    keys: &Keyed<'_>,
) -> Result<OrderFences, RecordFault> {
    let total = column.metadata.num_row_groups();
    if stored.len() != total {
        return Err(RecordFault::OrderFences { row_group: total });
    }
    let order_column = Column {
        leaf: order.leaf,
        ..*column
    };
    let mut ranges = keys.groups.iter().peekable();
    let mut groups = Vec::with_capacity(total);
    let mut previous: Option<(Key, Vec<u8>)> = None;
    for (row_group, stored) in stored.into_iter().enumerate() {
        let fault = RecordFault::OrderFences { row_group };
        let pages = order_column.group(row_group).page_locations().len();
        if stored.keys.len() != pages || stored.orders.len() != pages {
            return Err(fault);
        }
        let range = ranges.next_if(|range| range.row_group == row_group);
        let mut group = GroupFencesBuilder::new(keys.shape, order.shape, pages);
        for (page, (key, value)) in stored.keys.into_iter().zip(stored.orders).enumerate() {
            let key = key_of(keys.shape, key, keys.longest, keys.width).ok_or(fault)?;
            let within =
                range.is_some_and(|range| (page > 0 || key == range.first) && key <= range.last);
            if !within || !fence_order(order.shape, &value) {
                return Err(fault);
            }
            let fence = Pair {
                key: PairPart::of_key(&key),
                order: order_part(order.shape, &value),
            };
            let ascending = previous.as_ref().is_none_or(|(key, value)| {
                let before = Pair {
                    key: PairPart::of_key(key),
                    order: order_part(order.shape, value),
                };
                before < fence
            });
            if !ascending {
                return Err(fault);
            }
            group.push(fence).map_err(|_| fault)?;
            previous = Some((key, value));
        }
        groups.push(group.finish());
    }
    Ok(OrderFences::new(groups))
}

/// Whether `bytes` are a fence's order value for an order column of
/// `shape`: eight bytes for an integer, which the record stores as a long,
/// at most the column's width for a fixed-width value, and any bytes for a
/// string or binary one, whose prefix may end inside a character.
fn fence_order(shape: OrderShape, bytes: &[u8]) -> bool {
    match shape {
        OrderShape::Int32 | OrderShape::Int64 => bytes.len() == size_of::<i64>(),
        OrderShape::Fixed(width) => u32::try_from(bytes.len()).is_ok_and(|len| len <= width),
        OrderShape::Str | OrderShape::Bin => true,
    }
}

/// A fence's order value, `bytes`, which `fence_order` admits for `shape`,
/// as fences compare it.
fn order_part(shape: OrderShape, bytes: &[u8]) -> PairPart<'_> {
    match shape {
        OrderShape::Int32 | OrderShape::Int64 => PairPart::Long(i64::from_le_bytes(
            bytes
                .try_into()
                .expect("fence_order admits eight bytes alone for an integer"),
        )),
        OrderShape::Str | OrderShape::Bin | OrderShape::Fixed(_) => PairPart::Bytes(bytes),
    }
}

/// The order facts `stored` holds of `column`'s file, whose order column
/// decodes as `shape`, once they describe it: a distinct key count per
/// footer row group, at least one and at most its rows for a row group
/// with rows and none for one without, a longest run of at least one row
/// and at most the file's rows, and a least and a greatest order value of
/// the column's type (`order_of`), the least at or below the greatest.
/// `RecordFault::OrderFacts` otherwise.
fn order_facts(
    stored: OrderRecord,
    shape: OrderShape,
    column: &Column<'_>,
) -> Result<OrderFacts, RecordFault> {
    let groups = column.metadata.row_groups();
    let counted = stored.distinct_keys.len() == groups.len()
        && stored
            .distinct_keys
            .iter()
            .zip(groups)
            .all(|(&distinct, group)| {
                let rows = approx::measured(group.num_rows());
                distinct <= rows && (distinct > 0) == (rows > 0)
            });
    let rows = approx::measured(column.metadata.file_metadata().num_rows());
    let run = (1..=rows).contains(&stored.longest_run);
    let (Some(min), Some(max)) = (order_of(shape, stored.min), order_of(shape, stored.max)) else {
        return Err(RecordFault::OrderFacts);
    };
    if !(counted && run && min <= max) {
        return Err(RecordFault::OrderFacts);
    }
    Ok(OrderFacts {
        min,
        max,
        distinct_keys: stored.distinct_keys,
        longest_run: stored.longest_run,
    })
}

/// The order value `bytes` hold for an order column of `shape`, when they
/// are one: eight bytes for an integer, which the record stores as a long,
/// UTF-8 for a string, the column's width for a fixed-width value.
fn order_of(shape: OrderShape, bytes: Vec<u8>) -> Option<OrderValue> {
    match shape {
        OrderShape::Int32 | OrderShape::Int64 => {
            Some(OrderValue::Long(i64::from_le_bytes(bytes.try_into().ok()?)))
        }
        OrderShape::Str => String::from_utf8(bytes).ok().map(OrderValue::Str),
        OrderShape::Bin => Some(OrderValue::Bytes(bytes)),
        OrderShape::Fixed(width) => u32::try_from(bytes.len())
            .is_ok_and(|len| len == width)
            .then_some(OrderValue::Bytes(bytes)),
    }
}

/// The key `bytes` hold for keys of `shape`, when they are one: at most
/// `longest` bytes, eight for a long, sixteen for a uuid, the column's
/// `width` for a fixed-width binary key, UTF-8 for a string.
fn key_of(shape: KeyShape, bytes: Vec<u8>, longest: usize, width: Option<usize>) -> Option<Key> {
    if bytes.len() > longest {
        return None;
    }
    match shape {
        KeyShape::Long => Some(Key::Long(i64::from_le_bytes(bytes.try_into().ok()?))),
        KeyShape::Str => String::from_utf8(bytes).ok().map(Key::Str),
        KeyShape::Bin => Some(Key::Bytes(bytes)),
        KeyShape::FixedBin => (Some(bytes.len()) == width).then_some(Key::Bytes(bytes)),
        KeyShape::Uuid => Some(Key::Uuid(bytes.try_into().ok()?)),
    }
}

/// The fences `verify` records for row group `chunk`: one per key page
/// whose first row is inside the row group, so none for a row group
/// without rows.
fn fence_count(chunk: ColumnChunk<'_>) -> usize {
    let rows = chunk.rows();
    chunk
        .page_locations()
        .iter()
        .filter(|location| location.first_row_index < rows)
        .count()
}

/// The longest record `encode` can write for the file `column` is in,
/// under the rules `order` names as for `decode`: every fence and every row
/// group's last key at `contract::MAX_KEY_BYTES`, every field at its widest
/// framing, and under the ordered rules the order facts and an order fence
/// per order page, each fence's key at `contract::MAX_KEY_BYTES` and each
/// order value at the most its type holds: eight bytes for an integer, a
/// fixed value's width, and for a string or binary value the uncompressed
/// bytes of the column chunk it lies in, at most `MOST_VALUE_PAGE_BYTES`,
/// the least and greatest value at the largest chunk's. A value, and a
/// fence's prefix of one, lies in one page of one chunk, its dictionary
/// page or a data page, and an order page over that ceiling is refused
/// (`check_order_page_headers`), so the bound follows the footer's chunks.
/// A chunk without a dictionary page holds each page's first value in that
/// page, so its fences' values together are at most its uncompressed
/// bytes; a dictionary-encoded chunk's pages may each start at its longest
/// dictionary entry, so each of its fences is bounded alone.
/// `DiskChunks::checked_verification` refuses a stored record longer than
/// this before reading it, so a record's claimed length cannot ask for
/// more than the footer's pages, chunks, and order type allow; a footer
/// that understates its chunks only makes its chunk file stale.
pub(super) fn most_bytes(column: &Column<'_>, order: Option<OrderLeaf>) -> u64 {
    let groups = column.metadata.num_row_groups();
    let fences: usize = (0..groups)
        .map(|row_group| fence_count(column.group(row_group)))
        .sum();
    let field = FIELD_KEY_BYTES + MOST_VARINT_BYTES;
    let keys = approx::count(fences).saturating_add(approx::count(groups));
    let order_bytes = order.map_or(0, |order| {
        let order_column = Column {
            leaf: order.leaf,
            ..*column
        };
        let value = |chunk: ColumnChunk<'_>| match order.shape {
            OrderShape::Int32 | OrderShape::Int64 => approx::count(size_of::<i64>()),
            OrderShape::Fixed(width) => u64::from(width),
            OrderShape::Str | OrderShape::Bin => {
                approx::measured(chunk.metadata().uncompressed_size()).min(MOST_VALUE_PAGE_BYTES)
            }
        };
        let chunks = || (0..groups).map(|row_group| order_column.group(row_group));
        let largest = chunks().map(value).max().unwrap_or(0);
        let (message, min, max, counts, longest_run) =
            (field, largest + field, largest + field, field, field);
        let facts = [message, min, max, counts, longest_run]
            .into_iter()
            .fold(0, u64::saturating_add)
            .saturating_add(approx::count(groups).saturating_mul(MOST_VARINT_BYTES));
        let fence = |chunk: ColumnChunk<'_>| {
            let pages = approx::count(chunk.page_locations().len());
            let framing = (approx::count(MAX_KEY_BYTES) + field).saturating_add(field);
            let values = match (order.shape, chunk.dictionary_range()) {
                (OrderShape::Str | OrderShape::Bin, None) => {
                    let chunk_bytes = approx::measured(chunk.metadata().uncompressed_size());
                    chunk_bytes.min(pages.saturating_mul(MOST_VALUE_PAGE_BYTES))
                }
                (OrderShape::Str | OrderShape::Bin, Some(_))
                | (OrderShape::Int32 | OrderShape::Int64 | OrderShape::Fixed(_), _) => {
                    pages.saturating_mul(value(chunk))
                }
            };
            pages
                .saturating_mul(framing)
                .saturating_add(values)
                .saturating_add(field)
        };
        chunks().map(fence).fold(facts, u64::saturating_add)
    });
    keys.saturating_mul(approx::count(MAX_KEY_BYTES) + field)
        .saturating_add(approx::count(groups).saturating_mul(field))
        .saturating_add(field)
        .saturating_add(order_bytes)
}

/// The length `encode` writes for row groups given as their fences and the
/// bytes those fences' keys hold together, with each row group's last key
/// taken at its fences' mean length: exact for keys of one width, which
/// longs, uuids, and fixed-width binary keys are, and an estimate for
/// string and binary keys. What the Disk tier's chunk file is predicted to
/// hold at the footer stage, before any key is decoded (RFD 017).
pub(super) fn estimated_len(groups: impl IntoIterator<Item = (u64, u64)>) -> u64 {
    let framed = |len: u64| FIELD_KEY_BYTES + varint_len(len) + len;
    let mut total: u64 = 0;
    let mut longest = 0;
    for (fences, key_bytes) in groups {
        let group = match key_bytes.checked_div(fences) {
            Some(mean) => {
                longest = longest.max(mean);
                let key_framing = FIELD_KEY_BYTES + varint_len(mean);
                let framing = fences.saturating_add(1).saturating_mul(key_framing);
                key_bytes.saturating_add(mean).saturating_add(framing)
            }
            None => 0,
        };
        total = total.saturating_add(framed(group));
    }
    if longest > 0 {
        total = total.saturating_add(FIELD_KEY_BYTES + varint_len(longest));
    }
    total
}

/// One row group's order fences as the record stores them: the fences, and
/// the bytes their keys and their order values hold together.
#[derive(Clone, Copy, Debug)]
pub(super) struct FenceBytes {
    pub(super) pages: u64,
    pub(super) keys: u64,
    pub(super) orders: u64,
}

/// The length `encode` writes for an ordered file's order facts and order
/// fences, given the rows of each row group, the bytes the least and
/// greatest order value hold in the record, `values`, and each row group's
/// fences, `fences`: each row group's distinct keys taken at its rows and
/// the longest run at the file's rows, at least what either takes, and
/// each fence's key and order value at their row group's mean lengths, as
/// `estimated_len` takes keys, so at least the record's length when the
/// lengths are exact, as for integer order values and keys of one width.
/// What the Disk tier's ordered chunk file is predicted to hold for them at
/// the footer stage (RFD 025, Load).
pub(super) fn estimated_order_len(
    group_rows: impl IntoIterator<Item = u64>,
    values: [u64; 2],
    fences: impl IntoIterator<Item = FenceBytes>,
) -> u64 {
    let framed = |len: u64| FIELD_KEY_BYTES + varint_len(len) + len;
    let (mut counts, mut rows) = (0, 0u64);
    for group in group_rows {
        counts += varint_len(group);
        rows = rows.saturating_add(group);
    }
    let parts = |pages: u64, bytes: u64| match bytes.checked_div(pages) {
        Some(mean) => pages
            .saturating_mul(FIELD_KEY_BYTES + varint_len(mean))
            .saturating_add(bytes),
        None => 0,
    };
    let fences = fences.into_iter().fold(0, |total: u64, group| {
        let body = parts(group.pages, group.keys).saturating_add(parts(group.pages, group.orders));
        total.saturating_add(framed(body))
    });
    let message = framed(values[0])
        .saturating_add(framed(values[1]))
        .saturating_add(framed(counts))
        .saturating_add(FIELD_KEY_BYTES + varint_len(rows))
        .saturating_add(fences);
    framed(message)
}

/// The bytes the varint of `value` takes.
fn varint_len(value: u64) -> u64 {
    approx::count(prost::encoding::encoded_len_varint(value))
}

/// `bytes`, a record, with its fences, one list per row group, edited by
/// `edit`: how a test makes a record that decodes but describes another
/// file than the footer does.
#[cfg(test)]
pub(super) fn with_fences(bytes: &[u8], edit: impl FnOnce(&mut Vec<Vec<Vec<u8>>>)) -> Vec<u8> {
    let mut record = VerificationRecord::decode(bytes).unwrap();
    let mut fences: Vec<Vec<Vec<u8>>> = record
        .row_groups
        .iter_mut()
        .map(|group| std::mem::take(&mut group.fences))
        .collect();
    edit(&mut fences);
    record
        .row_groups
        .resize_with(fences.len(), || RowGroupRecord {
            fences: Vec::new(),
            last: Vec::new(),
        });
    for (group, fences) in record.row_groups.iter_mut().zip(fences) {
        group.fences = fences;
    }
    record.encode_to_vec()
}

/// An ordered record's order fences as `with_order_fences` edits them.
#[cfg(test)]
pub(super) type StoredFences = Vec<(Vec<Vec<u8>>, Vec<Vec<u8>>)>;

/// `bytes`, an ordered file's record, with its order fences, per row group
/// a list of keys and a list of order values as the record stores them,
/// edited by `edit`: how a test makes a record whose fences do not describe
/// the file.
#[cfg(test)]
pub(super) fn with_order_fences(bytes: &[u8], edit: impl FnOnce(&mut StoredFences)) -> Vec<u8> {
    let mut record = VerificationRecord::decode(bytes).unwrap();
    let order = record.order.as_mut().unwrap();
    let mut fences: StoredFences = order
        .fences
        .drain(..)
        .map(|group| (group.keys, group.orders))
        .collect();
    edit(&mut fences);
    order.fences = fences
        .into_iter()
        .map(|(keys, orders)| FenceGroupRecord { keys, orders })
        .collect();
    record.encode_to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::approx::count;
    use crate::config::Residency;
    use crate::generation::testing::KeyColumn;
    use crate::generation::testing::page_rows;
    use crate::index::FileIndex;
    use crate::index::build::Verified;
    use crate::index::testing::{
        FEW_ROWS, FIXED_KEY_BYTES, Fixture, Orders, ROW_GROUPS, contract_rows, long_orders,
        stepped, store,
    };
    use crate::key::KeyType;
    use crate::order::{OrderFacts, OrderType, OrderValue};

    /// The record of `index` as `verify` established it.
    fn verified_of(index: &FileIndex) -> Verified {
        Verified {
            fences: index.fences.clone(),
            groups: index.groups.clone(),
            key_range: index.key_range.clone(),
            max_key_bytes: index.max_key_bytes,
            order: index.order.clone(),
        }
    }

    /// An ordered file of `ROW_GROUPS` row groups of two key pages each
    /// whose key changes every third page, a page in, so the first row
    /// group's fences repeat a key and a run crosses the boundary into the
    /// last row group, built in the Full tier under a `long` order column
    /// that rises with the row.
    async fn ordered_index() -> FileIndex {
        let rows = i64::try_from(contract_rows()).unwrap();
        let page = i64::try_from(page_rows()).unwrap();
        let keys: Vec<i64> = (0..rows).map(|i| (i + page) / (3 * page)).collect();
        let orders: Vec<i64> = (0..rows).collect();
        let opened = store(&Fixture::ordered(&keys, long_orders(&orders))).await;
        let dir = tempfile::tempdir().unwrap();
        opened
            .build_ordered_in(OrderType::Long, Residency::Full, dir.path())
            .await
            .unwrap()
    }

    /// What `decode` makes of `bytes` for `index`, under its rules.
    fn decoded_for(index: &FileIndex, bytes: &[u8]) -> Result<Verified, RecordFault> {
        decode(
            bytes,
            &index.key_column(),
            index.layout.key_shape,
            index.layout.order,
        )
    }

    #[tokio::test]
    async fn test_decode_of_encode_round_trips_the_order_facts_and_repeated_fences() {
        let index = ordered_index().await;
        let verified = verified_of(&index);
        assert!(
            verified
                .fences
                .iter()
                .any(|fences| fences.windows(2).any(|pair| pair[0] == pair[1])),
            "a key's run crosses a page boundary"
        );
        assert!(
            verified
                .groups
                .windows(2)
                .any(|pair| pair[0].last == pair[1].first),
            "a key's run crosses a row-group boundary"
        );
        let bytes = encode(&verified);
        assert_eq!(decoded_for(&index, &bytes), Ok(verified.clone()));
        let column = index.key_column();
        assert!(count(bytes.len()) <= most_bytes(&column, index.layout.order));
        // Under key-value rules the same keys are out of order, and a
        // record carrying order facts describes no key-value file.
        assert_eq!(
            decode(
                &encode(&Verified {
                    order: None,
                    ..verified
                }),
                &column,
                index.layout.key_shape,
                None
            ),
            Err(RecordFault::Order { row_group: 0 })
        );
        assert_eq!(
            decode(&bytes, &column, index.layout.key_shape, None),
            Err(RecordFault::OrderFacts)
        );
    }

    #[tokio::test]
    async fn test_decode_refuses_order_facts_that_do_not_describe_the_file() {
        let index = ordered_index().await;
        let verified = verified_of(&index);
        let order = verified.order.clone().unwrap();
        let rows = index.metadata().row_group(0).num_rows();
        let edited = |edit: &dyn Fn(&mut OrderFacts)| {
            let mut order = order.clone();
            edit(&mut order.facts);
            encode(&Verified {
                order: Some(order),
                ..verified.clone()
            })
        };
        let cases = [
            encode(&Verified {
                order: None,
                ..verified.clone()
            }),
            edited(&|facts| {
                facts.distinct_keys.pop();
            }),
            edited(&|facts| facts.distinct_keys[0] = 0),
            edited(&|facts| facts.distinct_keys[0] = u64::try_from(rows).unwrap() + 1),
            edited(&|facts| facts.longest_run = 0),
            edited(&|facts| facts.longest_run = u64::try_from(index.rows()).unwrap() + 1),
            edited(&|facts| std::mem::swap(&mut facts.min, &mut facts.max)),
            edited(&|facts| facts.min = OrderValue::Bytes(vec![1])),
        ];
        for bytes in cases {
            assert_eq!(decoded_for(&index, &bytes), Err(RecordFault::OrderFacts));
        }
    }

    /// Every key type's contract fixture, built in the Full tier.
    async fn indexes() -> Vec<(KeyType, FileIndex)> {
        let mut built = Vec::new();
        for key_type in KeyType::ALL {
            let opened = store(&Fixture::contract(key_type, contract_rows())).await;
            built.push((key_type, opened.index(key_type).await));
        }
        built
    }

    #[tokio::test]
    async fn test_decode_of_encode_is_what_verify_established_for_every_key_type() {
        for (key_type, index) in indexes().await {
            let verified = verified_of(&index);
            let bytes = encode(&verified);
            let column = index.key_column();
            assert_eq!(
                decode(&bytes, &column, index.layout.key_shape, None),
                Ok(verified),
                "{key_type}"
            );
            assert!(
                count(bytes.len()) <= most_bytes(&column, None),
                "{key_type}"
            );
        }
    }

    #[tokio::test]
    async fn test_decode_refuses_a_row_group_without_fences_where_the_footer_has_rows() {
        let mut fixture = Fixture::contract(KeyType::Long, FEW_ROWS);
        fixture.layout.row_group_rows = FEW_ROWS / 2;
        let opened = store(&fixture).await;
        let index = opened.index(KeyType::Long).await;
        let mut verified = verified_of(&index);
        verified.fences[1].clear();
        verified.groups.pop();
        assert_eq!(
            decode(
                &encode(&verified),
                &index.key_column(),
                index.layout.key_shape,
                None
            ),
            Err(RecordFault::Fences {
                row_group: 1,
                stored: 0
            })
        );
    }

    #[tokio::test]
    async fn test_decode_refuses_a_record_that_does_not_describe_the_footer() {
        let opened = store(&Fixture::contract(KeyType::String, contract_rows())).await;
        let index = opened.index(KeyType::String).await;
        let column = index.key_column();
        let bytes = encode(&verified_of(&index));
        let decoded = |bytes: &[u8]| decode(bytes, &column, KeyShape::Str, None);
        let too_long = vec![b'a'; MAX_KEY_BYTES + 1];
        let not_utf8 = vec![0xff];
        let cases = [
            (
                with_fences(&bytes, |fences| {
                    fences.pop();
                }),
                RecordFault::RowGroups {
                    stored: ROW_GROUPS - 1,
                },
            ),
            (
                with_fences(&bytes, |fences| {
                    let repeated = fences[0][0].clone();
                    fences[0].push(repeated);
                }),
                RecordFault::Fences {
                    row_group: 0,
                    stored: index.fences()[0].len() + 1,
                },
            ),
            (
                with_fences(&bytes, |fences| fences[1][0] = too_long.clone()),
                RecordFault::Key { row_group: 1 },
            ),
            (
                with_fences(&bytes, |fences| fences[1][0] = not_utf8.clone()),
                RecordFault::Key { row_group: 1 },
            ),
            (
                with_fences(&bytes, |fences| fences[1].reverse()),
                RecordFault::Order { row_group: 1 },
            ),
            (
                with_fences(&bytes, |fences| fences.swap(0, 2)),
                RecordFault::Order { row_group: 0 },
            ),
            (vec![0xff; FEW_ROWS], RecordFault::Undecodable),
        ];
        for (bytes, expected) in cases {
            assert_eq!(decoded(&bytes), Err(expected));
        }
        let mut record = VerificationRecord::decode(bytes.as_slice()).unwrap();
        record.max_key_bytes = count(MAX_KEY_BYTES) + 1;
        assert_eq!(
            decoded(&record.encode_to_vec()),
            Err(RecordFault::LongestKey {
                bytes: count(MAX_KEY_BYTES) + 1
            })
        );
    }

    #[tokio::test]
    async fn test_decode_refuses_a_key_over_the_longest_or_off_the_type_width() {
        // Every fixture key is as long as the longest, so one byte more is
        // over it for strings and binary keys and off the width for the
        // others.
        for (key_type, index) in indexes().await {
            let bytes = encode(&verified_of(&index));
            let widened = with_fences(&bytes, |fences| fences[0][0].push(0));
            assert_eq!(
                decode(&widened, &index.key_column(), index.layout.key_shape, None).map(|_| ()),
                Err(RecordFault::Key { row_group: 0 }),
                "{key_type}"
            );
        }
        let keys = (0..FEW_ROWS)
            .map(|i| Some(Key::Bytes(count(stepped(i)).to_be_bytes().to_vec())))
            .collect();
        let opened = store(&Fixture::with_keys(
            KeyColumn::FixedBinary(FIXED_KEY_BYTES),
            keys,
        ))
        .await;
        let index = opened.index(KeyType::Binary).await;
        let bytes = encode(&verified_of(&index));
        let shortened = with_fences(&bytes, |fences| {
            fences[0][0].pop();
        });
        assert_eq!(
            decode(
                &shortened,
                &index.key_column(),
                index.layout.key_shape,
                None
            )
            .map(|_| ()),
            Err(RecordFault::Key { row_group: 0 })
        );
    }

    #[tokio::test]
    async fn test_estimated_len_is_exact_for_the_contract_fixtures() {
        // Every contract fixture's keys are of one width within the column
        // index's truncation, which is when the estimate is exact.
        for (key_type, index) in indexes().await {
            let groups = index.fences().iter().map(|fences| {
                let key_bytes = fences.iter().map(|key| count(key.as_bytes().len())).sum();
                (count(fences.len()), key_bytes)
            });
            assert_eq!(
                estimated_len(groups),
                count(encode(&verified_of(&index)).len()),
                "{key_type}"
            );
        }
    }

    #[tokio::test]
    async fn test_decode_refuses_order_fences_that_do_not_describe_the_file() {
        let index = ordered_index().await;
        let bytes = encode(&verified_of(&index));
        let fences = index.order_fences().unwrap();
        assert!((0..ROW_GROUPS).all(|row_group| fences.pages(row_group) > 1));
        let later_key = |row_group: usize| {
            let Key::Long(last) = &index.groups[row_group].last else {
                panic!("the ordered fixture's keys are longs");
            };
            (last + 1).to_le_bytes().to_vec()
        };
        let cases = [
            (
                with_order_fences(&bytes, |groups| {
                    groups.pop();
                }),
                RecordFault::OrderFences {
                    row_group: ROW_GROUPS,
                },
            ),
            (
                with_order_fences(&bytes, |groups| {
                    let (keys, orders) = &mut groups[0];
                    keys.push(keys[0].clone());
                    orders.push(orders[0].clone());
                }),
                RecordFault::OrderFences { row_group: 0 },
            ),
            (
                with_order_fences(&bytes, |groups| groups[1].0[0] = later_key(1)),
                RecordFault::OrderFences { row_group: 1 },
            ),
            (
                with_order_fences(&bytes, |groups| {
                    let last = groups[2].0.len() - 1;
                    groups[2].0[last] = later_key(2);
                }),
                RecordFault::OrderFences { row_group: 2 },
            ),
            (
                with_order_fences(&bytes, |groups| groups[0].1.swap(0, 1)),
                RecordFault::OrderFences { row_group: 0 },
            ),
            (
                with_order_fences(&bytes, |groups| {
                    groups[1].1[1].pop();
                }),
                RecordFault::OrderFences { row_group: 1 },
            ),
        ];
        for (bytes, expected) in cases {
            assert_eq!(decoded_for(&index, &bytes), Err(expected));
        }
    }

    /// The rows per key page and order page of the string-ordered fixture.
    const STRING_ORDER_ROWS: usize = 2_000;

    /// A file of one key whose `string` order values change their second
    /// character from an ASCII letter to a two-byte one at the second order
    /// page, so that page's fence is cut inside a character.
    async fn string_ordered_index() -> FileIndex {
        let page = page_rows();
        let orders = (0..STRING_ORDER_ROWS)
            .map(|i| {
                let letter = if i < page { 'a' } else { 'é' };
                Some(format!("p{letter}{i:06}").into_bytes())
            })
            .collect();
        let keys = vec![0; STRING_ORDER_ROWS];
        let opened = store(&Fixture::ordered(&keys, Orders::Bytes(orders))).await;
        let dir = tempfile::tempdir().unwrap();
        opened
            .build_ordered_in(OrderType::String, Residency::Full, dir.path())
            .await
            .unwrap()
    }

    /// A string order value's fence may be a prefix that is not UTF-8,
    /// and the record keeps it byte for byte.
    #[tokio::test]
    async fn test_decode_of_encode_round_trips_a_string_fence_cut_inside_a_character() {
        let index = string_ordered_index().await;
        let verified = verified_of(&index);
        let cut = verified.order.as_ref().unwrap().fences.fence(0, 1).order;
        let PairPart::Bytes(cut) = cut else {
            panic!("a string order value's fence is bytes");
        };
        assert_eq!(
            cut,
            &"pé".as_bytes()[..2],
            "one byte past the shared prefix"
        );
        assert!(std::str::from_utf8(cut).is_err());
        let bytes = encode(&verified);
        assert_eq!(decoded_for(&index, &bytes), Ok(verified));
    }

    /// The bound on a stored record follows the footer for string and
    /// binary order values too (load review M7): each value at most the
    /// uncompressed bytes of its order chunk, not `MOST_VALUE_PAGE_BYTES`
    /// for each of the least and greatest whatever the file.
    #[tokio::test]
    async fn test_most_bytes_bounds_string_order_values_by_their_chunks() {
        let index = string_ordered_index().await;
        let order = index.layout.order;
        let most = most_bytes(&index.key_column(), order);
        assert!(count(encode(&verified_of(&index)).len()) <= most);
        assert!(
            most < MOST_VALUE_PAGE_BYTES,
            "{most} bytes for a file of {} rows",
            index.rows()
        );
    }
}
