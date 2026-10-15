//! The order fences of an ordered file (RFD 025, Load): for each order
//! page, the shortest pair of key and order value that sorts above the last
//! row of the page before it and at or below the page's first row. Rows are
//! sorted by key and then order value, so a page holds exactly the pairs
//! from its fence up to the next page's, and the fences place any pair on
//! its page whatever the writer's column index holds or leaves out. A key
//! in a fence is exact, as are integer and time order values; a string,
//! binary, or fixed order value is cut to the shortest prefix of the page's
//! first value that keeps that order, and is empty on a page whose first
//! row starts a new key, where the key alone separates the pages. Built by
//! verification over every row, deleted rows included, since fences bound
//! positions (`ordered::PairCheck`), kept per row group beside the order
//! column's offset index, and stored in the ordered chunk file's
//! verification record (`record`).

use std::mem::size_of;

use super::{KeyShape, OrderShape};
use crate::approx;
use crate::key::{Key, KeyType};
use crate::order::OrderValue;

/// One part of a pair, its key or its order value, as fences compare it: a
/// long key and an integer or time order value numerically, every other
/// key and order value as its bytes, the orders RFD 008 gives keys and RFD
/// 025 gives order values. Parts of different variants never meet within
/// one table; when they do, the derived order across variants carries no
/// meaning.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FencePart<'a> {
    /// A long key, or an integer or time order value in Iceberg's unit.
    Long(i64),
    /// A string, binary, or uuid key, or a string, binary, or fixed order
    /// value, as its bytes. In a fence, an order value may be a prefix of
    /// one, and a string's prefix may end inside a character.
    Bytes(&'a [u8]),
}

impl<'a> FencePart<'a> {
    /// `key` as fences compare it.
    pub fn of_key(key: &'a Key) -> Self {
        match key {
            Key::Long(value) => Self::Long(*value),
            Key::Str(text) => Self::Bytes(text.as_bytes()),
            Key::Bytes(bytes) => Self::Bytes(bytes),
            Key::Uuid(bytes) => Self::Bytes(bytes),
        }
    }

    /// `order` as fences compare it.
    pub fn of_order(order: &'a OrderValue) -> Self {
        match order {
            OrderValue::Long(value) => Self::Long(*value),
            OrderValue::Str(text) => Self::Bytes(text.as_bytes()),
            OrderValue::Bytes(bytes) => Self::Bytes(bytes),
        }
    }
}

/// A pair of key and order value, a fence or a pair a row or a bound
/// holds, ordered as an ordered file's rows are: by key, then by order
/// value (the derived order, by field).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Fence<'a> {
    /// The key.
    pub key: FencePart<'a>,
    /// The order value.
    pub order: FencePart<'a>,
}

impl<'a> Fence<'a> {
    /// The pair of `key` and `order`.
    pub fn of(key: &'a Key, order: &'a OrderValue) -> Self {
        Self {
            key: FencePart::of_key(key),
            order: FencePart::of_order(order),
        }
    }

    /// The fence of a page whose first row holds `first`, after a page
    /// whose last row holds `previous`, `None` for the file's first page:
    /// `first`'s key, with `first`'s order value when it is an integer, and
    /// otherwise the shortest prefix of it above `previous`'s order value
    /// where the key continues from `previous`, one byte past the prefix
    /// the two share, and the empty value where the page starts a key. For
    /// `previous` below `first`, as verification requires, the fence is
    /// the shortest pair above `previous` and at or below `first` whose
    /// key is exact and whose order value is `first`'s or a prefix of it.
    pub(super) fn between(previous: Option<Fence<'_>>, first: Self) -> Self {
        let order = match (previous, first.order) {
            (_, FencePart::Long(_)) => first.order,
            (Some(previous), FencePart::Bytes(bytes)) if previous.key == first.key => {
                let shared = match previous.order {
                    FencePart::Bytes(before) => shared_prefix(before, bytes),
                    FencePart::Long(_) => 0,
                };
                FencePart::Bytes(&bytes[..bytes.len().min(shared + 1)])
            }
            (None | Some(_), FencePart::Bytes(_)) => FencePart::Bytes(&[]),
        };
        Self {
            key: first.key,
            order,
        }
    }
}

/// The bytes `a` and `b` share from their start.
fn shared_prefix(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

/// One part of every fence of a row group, in page order: all longs or all
/// bytes, as the column's shape decodes (`for_key`, `for_order`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Parts {
    /// Long keys or integer order values.
    Longs(Vec<i64>),
    /// Byte parts end to end, and where each ends in `data`.
    Bytes { data: Vec<u8>, ends: Vec<usize> },
}

/// Why a part pushed is of its column's variant.
const ONE_VARIANT: &str =
    "a column's fence parts are of the one variant its shape, fixed at build, decodes";

impl Parts {
    /// The key parts of `pages` fences of a key column of `shape`.
    pub(super) fn for_key(shape: KeyShape, pages: usize) -> Self {
        Self::with_capacity(long_keys(shape.key_type()), pages)
    }

    /// The order parts of `pages` fences of an order column of `shape`.
    pub(super) fn for_order(shape: OrderShape, pages: usize) -> Self {
        Self::with_capacity(long_orders(shape), pages)
    }

    /// Room for `pages` parts, longs when `long`.
    fn with_capacity(long: bool, pages: usize) -> Self {
        if long {
            Self::Longs(Vec::with_capacity(pages))
        } else {
            Self::Bytes {
                data: Vec::new(),
                ends: Vec::with_capacity(pages),
            }
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Longs(values) => values.len(),
            Self::Bytes { ends, .. } => ends.len(),
        }
    }

    /// Part `at`, which is below `len`.
    fn get(&self, at: usize) -> FencePart<'_> {
        match self {
            Self::Longs(values) => FencePart::Long(values[at]),
            Self::Bytes { data, ends } => {
                let start = at.checked_sub(1).map_or(0, |before| ends[before]);
                FencePart::Bytes(&data[start..ends[at]])
            }
        }
    }

    /// `part` after the parts held.
    pub(super) fn push(&mut self, part: FencePart<'_>) {
        match (self, part) {
            (Self::Longs(values), FencePart::Long(value)) => values.push(value),
            (Self::Bytes { data, ends }, FencePart::Bytes(bytes)) => {
                data.extend_from_slice(bytes);
                ends.push(data.len());
            }
            (Self::Longs(_), FencePart::Bytes(_)) | (Self::Bytes { .. }, FencePart::Long(_)) => {
                panic!("{ONE_VARIANT}")
            }
        }
    }

    /// The parts with no spare capacity, so `heap_bytes` is what they hold.
    fn shrunk(mut self) -> Self {
        match &mut self {
            Self::Longs(values) => values.shrink_to_fit(),
            Self::Bytes { data, ends } => {
                data.shrink_to_fit();
                ends.shrink_to_fit();
            }
        }
        self
    }

    /// The heap the parts hold once `shrunk`: a long per long part, and per
    /// byte part its bytes and where it ends.
    fn heap_bytes(&self) -> u64 {
        match self {
            Self::Longs(values) => approx::count(values.len() * size_of::<i64>()),
            Self::Bytes { data, ends } => {
                approx::count(data.len() + ends.len() * size_of::<usize>())
            }
        }
    }
}

/// The heap `pages` fence parts of one column take, given `bytes`, what
/// their byte parts hold together; `long` for a column of long parts,
/// which hold no bytes (`Parts::heap_bytes`).
pub(super) fn parts_heap(long: bool, pages: u64, bytes: u64) -> u64 {
    if long {
        pages.saturating_mul(approx::count(size_of::<i64>()))
    } else {
        pages
            .saturating_mul(approx::count(size_of::<usize>()))
            .saturating_add(bytes)
    }
}

/// Whether keys of `key_type` are long fence parts: a long key is, every
/// other key is held as its bytes.
pub(super) fn long_keys(key_type: KeyType) -> bool {
    match key_type {
        KeyType::Long => true,
        KeyType::String | KeyType::Binary | KeyType::Uuid => false,
    }
}

/// Whether an order column of `shape` has long fence parts: an integer or
/// time value does, every other value is held as its bytes.
pub(super) fn long_orders(shape: OrderShape) -> bool {
    match shape {
        OrderShape::Int32 | OrderShape::Int64 => true,
        OrderShape::Str | OrderShape::Bin | OrderShape::Fixed(_) => false,
    }
}

/// The fences of one row group's order pages, one per page in page order;
/// none for a row group without rows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct GroupFences {
    keys: Parts,
    orders: Parts,
}

impl GroupFences {
    /// No fence yet, over `keys` and `orders`, which hold no part.
    pub(super) fn new(keys: Parts, orders: Parts) -> Self {
        Self { keys, orders }
    }

    /// The fences held.
    pub(super) fn len(&self) -> usize {
        self.keys.len()
    }

    /// Fence `page`, which is below `len`.
    pub(super) fn get(&self, page: usize) -> Fence<'_> {
        Fence {
            key: self.keys.get(page),
            order: self.orders.get(page),
        }
    }

    /// Every fence, in page order.
    pub(super) fn iter(&self) -> impl Iterator<Item = Fence<'_>> {
        (0..self.len()).map(|page| self.get(page))
    }

    /// `fence` after the fences held.
    pub(super) fn push(&mut self, fence: Fence<'_>) {
        self.keys.push(fence.key);
        self.orders.push(fence.order);
    }

    /// The fences with no spare capacity.
    pub(super) fn shrunk(self) -> Self {
        Self {
            keys: self.keys.shrunk(),
            orders: self.orders.shrunk(),
        }
    }

    fn heap_bytes(&self) -> u64 {
        self.keys
            .heap_bytes()
            .saturating_add(self.orders.heap_bytes())
    }
}

/// An ordered file's order fences, per row group of the footer, one per
/// order page in page order (RFD 025, Load). Strictly ascending through
/// the file, across row groups too, so a page's pairs lie from its fence
/// up to the next page's, the next row group's first fence after a row
/// group's last page.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OrderFences {
    groups: Vec<GroupFences>,
}

impl OrderFences {
    /// The fences of every row group, in footer order.
    pub(super) fn new(groups: Vec<GroupFences>) -> Self {
        Self { groups }
    }

    /// Every row group's fences, in footer order.
    pub(super) fn groups(&self) -> &[GroupFences] {
        &self.groups
    }

    /// The row groups, the footer's.
    pub fn row_groups(&self) -> usize {
        self.groups.len()
    }

    /// The order pages of `row_group`, which is below `row_groups`: as
    /// many as the order column's offset index gives it, none for a row
    /// group without rows.
    pub fn pages(&self, row_group: usize) -> usize {
        self.groups[row_group].len()
    }

    /// The fence of order page `page` of `row_group`, both in range.
    pub fn fence(&self, row_group: usize, page: usize) -> Fence<'_> {
        self.groups[row_group].get(page)
    }

    /// The order page of `row_group` whose pairs' range holds `pair`: the
    /// last page whose fence is at or below it, `None` when `pair` sorts
    /// below the row group's first fence, before every pair the row group
    /// holds. A pair above the row group's last row lands on its last page,
    /// whose range runs up to the next row group's first fence
    /// (`page_pairs`). One binary search over the row group's fences.
    pub fn page_of(&self, row_group: usize, pair: Fence<'_>) -> Option<usize> {
        let group = &self.groups[row_group];
        let above = partition_point(group.len(), |page| group.get(page) <= pair);
        above.checked_sub(1)
    }

    /// The range of pairs order page `page` of `row_group` holds: from its
    /// fence, inclusive, to the next page's, exclusive, the first fence of
    /// the next row group with pages after a row group's last page, and
    /// unbounded above after the file's last page (`None`).
    pub fn page_pairs(&self, row_group: usize, page: usize) -> (Fence<'_>, Option<Fence<'_>>) {
        let group = &self.groups[row_group];
        let next = if page + 1 < group.len() {
            Some(group.get(page + 1))
        } else {
            self.groups[row_group + 1..]
                .iter()
                .find(|later| later.len() > 0)
                .map(|later| later.get(0))
        };
        (group.get(page), next)
    }

    /// The heap the fences hold: each row group's entry, and its parts.
    pub(super) fn heap_bytes(&self) -> u64 {
        self.groups.iter().fold(
            approx::count(self.groups.len() * size_of::<GroupFences>()),
            |total, group| total.saturating_add(group.heap_bytes()),
        )
    }
}

/// The heap `OrderFences` of `row_groups` row groups take besides their
/// parts (`OrderFences::heap_bytes`).
pub(super) fn groups_heap(row_groups: usize) -> u64 {
    approx::count(row_groups * size_of::<GroupFences>())
}

/// The first of `0..len` for which `below` is false, `below` holding for a
/// prefix of the range: `slice::partition_point` over positions.
fn partition_point(len: usize, below: impl Fn(usize) -> bool) -> usize {
    let (mut low, mut high) = (0, len);
    while low < high {
        let middle = low + (high - low) / 2;
        if below(middle) {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    low
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A pair of a long key and a byte order value.
    fn bytes_pair(key: i64, order: &[u8]) -> Fence<'_> {
        Fence {
            key: FencePart::Long(key),
            order: FencePart::Bytes(order),
        }
    }

    #[test]
    fn test_between_cuts_a_byte_order_to_one_byte_past_the_prefix_it_shares() {
        let shared = [b'p'; 64];
        let before = [&shared[..], b"a-tail"].concat();
        let first = [&shared[..], b"b"].concat();
        let fence = Fence::between(Some(bytes_pair(1, &before)), bytes_pair(1, &first));
        assert_eq!(fence, bytes_pair(1, &first[..=shared.len()]));
        // A first value that extends the one before it keeps one byte more.
        let longer = [&before[..], b"x"].concat();
        let fence = Fence::between(Some(bytes_pair(1, &before)), bytes_pair(1, &longer));
        assert_eq!(fence, bytes_pair(1, &longer[..=before.len()]));
    }

    #[test]
    fn test_between_a_new_key_takes_the_empty_byte_order_and_an_exact_long() {
        let fence = Fence::between(Some(bytes_pair(1, b"zzz")), bytes_pair(2, b"abc"));
        assert_eq!(fence, bytes_pair(2, b""));
        assert_eq!(
            Fence::between(None, bytes_pair(2, b"abc")),
            bytes_pair(2, b"")
        );
        let long = |key, order| Fence {
            key: FencePart::Long(key),
            order: FencePart::Long(order),
        };
        assert_eq!(Fence::between(Some(long(1, 9)), long(2, 3)), long(2, 3));
        assert_eq!(Fence::between(Some(long(1, 3)), long(1, 4)), long(1, 4));
        assert_eq!(Fence::between(None, long(1, 4)), long(1, 4));
    }

    /// Fences over three row groups, the middle one without rows.
    fn three_groups() -> OrderFences {
        let group = |fences: &[(i64, i64)]| {
            let mut group = GroupFences::new(
                Parts::for_key(KeyShape::Long, fences.len()),
                Parts::for_order(OrderShape::Int64, fences.len()),
            );
            for (key, order) in fences {
                group.push(Fence {
                    key: FencePart::Long(*key),
                    order: FencePart::Long(*order),
                });
            }
            group.shrunk()
        };
        OrderFences::new(vec![
            group(&[(1, 0), (1, 10), (2, 5)]),
            group(&[]),
            group(&[(3, 0)]),
        ])
    }

    #[test]
    fn test_page_of_places_a_pair_on_the_last_page_whose_fence_it_reaches() {
        let fences = three_groups();
        let key = Key::Long(1);
        let at = |order: i64| fences.page_of(0, Fence::of(&key, &OrderValue::Long(order)));
        assert_eq!(at(-1), None, "below the row group's first fence");
        assert_eq!(at(0), Some(0));
        assert_eq!(at(9), Some(0));
        assert_eq!(at(10), Some(1));
        let later = Key::Long(9);
        assert_eq!(
            fences.page_of(0, Fence::of(&later, &OrderValue::Long(0))),
            Some(2),
            "past the row group's last row: its last page"
        );
        assert_eq!(
            fences.page_of(1, Fence::of(&key, &OrderValue::Long(0))),
            None
        );
    }

    #[test]
    fn test_page_pairs_end_at_the_next_fence_across_an_empty_row_group() {
        let fences = three_groups();
        let long = |key, order| Fence {
            key: FencePart::Long(key),
            order: FencePart::Long(order),
        };
        assert_eq!(fences.page_pairs(0, 0), (long(1, 0), Some(long(1, 10))));
        assert_eq!(fences.page_pairs(0, 2), (long(2, 5), Some(long(3, 0))));
        assert_eq!(fences.page_pairs(2, 0), (long(3, 0), None));
        assert_eq!((fences.row_groups(), fences.pages(1)), (3, 0));
    }

    #[test]
    fn test_heap_bytes_counts_each_group_and_its_parts() {
        let fences = three_groups();
        let pages = 4;
        let long_parts = 2 * pages * size_of::<i64>();
        assert_eq!(
            fences.heap_bytes(),
            approx::count(3 * size_of::<GroupFences>() + long_parts)
        );
        assert_eq!(
            fences.heap_bytes(),
            groups_heap(3)
                + parts_heap(true, approx::count(pages), 0)
                + parts_heap(true, approx::count(pages), 0)
        );
    }
}
