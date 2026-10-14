//! The verification record (RFD 017): what `verify` established of a data
//! file's keys (the fences, every row group's first and last key, the
//! longest key) and, on an ordered table, of its order column (the order
//! facts, RFD 025), encoded into the Disk tier's chunk file so that a
//! restart rebuilds the index's view of the file without decoding a key or
//! an order value. The chunk file's payload checksum covers the record, so
//! a torn or changed one is never read; what is read is still checked
//! against the footer before it is trusted, since every lookup indexes the
//! footer's pages with the fences (`decode`).

use std::fmt;
use std::mem::size_of;

use prost::Message as _;

use super::build::Verified;
use super::page_header::MOST_VALUE_PAGE_BYTES;
use super::{Column, ColumnChunk, GroupRange, KeyShape, OrderShape};
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
        order: verified.order.as_ref().map(|facts| OrderRecord {
            min: order_bytes(&facts.min),
            max: order_bytes(&facts.max),
            distinct_keys: facts.distinct_keys.clone(),
            longest_run: facts.longest_run,
        }),
    }
    .encode_to_vec()
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
/// rules over an order column of that shape or the key-value rules for
/// `None`, once it describes the file the footer does: order facts under
/// the ordered rules alone (`order_facts`), one row group per footer row
/// group, as many fences in each as `verify` records for the footer's key
/// pages (`fence_count`), every key one of the key type and no longer than
/// the longest key, which is no longer than `contract::MAX_KEY_BYTES`, and
/// the keys in the order `verify` checks under those rules. A record that
/// does not is the `RecordFault` it fails. The checks cost one comparison
/// per key page and per row group, not per row.
pub(super) fn decode(
    bytes: &[u8],
    column: &Column<'_>,
    shape: KeyShape,
    order: Option<OrderShape>,
) -> Result<Verified, RecordFault> {
    let record = VerificationRecord::decode(bytes).map_err(|error| {
        tracing::debug!(%error, path = column.path, "verification record does not decode");
        RecordFault::Undecodable
    })?;
    let facts = match (order, record.order) {
        (None, None) => None,
        (Some(shape), Some(stored)) => Some(order_facts(stored, shape, column)?),
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
    Ok(Verified {
        key_range: (head.first.clone(), tail.last.clone()),
        fences,
        groups,
        max_key_bytes: longest,
        order: facts,
    })
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
/// framing, and under the ordered rules the order facts with each order
/// value at the most its type holds, eight bytes for an integer, a fixed
/// value's width, and for a string or binary value `MOST_VALUE_PAGE_BYTES`,
/// since a value lies in one order page and an order page over that
/// ceiling is refused (`check_order_page_headers`).
/// `DiskChunks::checked_verification` refuses a stored record longer than
/// this before reading it, so a record's claimed length cannot ask for
/// more than the footer's key pages and order type allow.
pub(super) fn most_bytes(column: &Column<'_>, order: Option<OrderShape>) -> u64 {
    let groups = column.metadata.num_row_groups();
    let fences: usize = (0..groups)
        .map(|row_group| fence_count(column.group(row_group)))
        .sum();
    let field = FIELD_KEY_BYTES + MOST_VARINT_BYTES;
    let keys = approx::count(fences).saturating_add(approx::count(groups));
    let order_bytes = order.map_or(0, |shape| {
        let value = match shape {
            OrderShape::Int32 | OrderShape::Int64 => approx::count(size_of::<i64>()),
            OrderShape::Fixed(width) => u64::from(width),
            OrderShape::Str | OrderShape::Bin => MOST_VALUE_PAGE_BYTES,
        };
        let (message, min, max, counts, longest_run) =
            (field, value + field, value + field, field, field);
        [message, min, max, counts, longest_run]
            .into_iter()
            .fold(0, u64::saturating_add)
            .saturating_add(approx::count(groups).saturating_mul(MOST_VARINT_BYTES))
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

/// The length `encode` writes for an ordered file's order facts, given
/// the rows of each row group and the bytes the least and greatest order
/// value hold in the record, `values`: each row group's distinct keys
/// taken at its rows and the longest run at the file's rows, at least what
/// either takes, so at least the record's length when the values' lengths
/// are exact. What the Disk tier's ordered chunk file is predicted to hold
/// for them at the footer stage (RFD 025, Load).
pub(super) fn estimated_order_len(
    group_rows: impl IntoIterator<Item = u64>,
    values: [u64; 2],
) -> u64 {
    let framed = |len: u64| FIELD_KEY_BYTES + varint_len(len) + len;
    let (mut counts, mut rows) = (0, 0u64);
    for group in group_rows {
        counts += varint_len(group);
        rows = rows.saturating_add(group);
    }
    let message = framed(values[0])
        .saturating_add(framed(values[1]))
        .saturating_add(framed(counts))
        .saturating_add(FIELD_KEY_BYTES + varint_len(rows));
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
        FEW_ROWS, FIXED_KEY_BYTES, Fixture, ROW_GROUPS, contract_rows, long_orders, stepped, store,
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
            index.layout.order.map(|order| order.shape),
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
        assert!(
            count(bytes.len()) <= most_bytes(&column, index.layout.order.map(|order| order.shape))
        );
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
        let facts = verified.order.clone().unwrap();
        let rows = index.metadata().row_group(0).num_rows();
        let edited = |edit: &dyn Fn(&mut OrderFacts)| {
            let mut facts = facts.clone();
            edit(&mut facts);
            encode(&Verified {
                order: Some(facts),
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
}
