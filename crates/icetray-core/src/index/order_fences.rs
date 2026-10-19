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
pub enum PairPart<'a> {
    /// A long key, or an integer or time order value in Iceberg's unit.
    Long(i64),
    /// A string, binary, or uuid key, or a string, binary, or fixed order
    /// value, as its bytes. In a fence, an order value may be a prefix of
    /// one, and a string's prefix may end inside a character.
    Bytes(&'a [u8]),
}

impl<'a> PairPart<'a> {
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

/// A pair of key and order value, a row's, a bound's, or a fence, ordered
/// as an ordered file's rows are: by key, then by order value (the derived
/// order, by field).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Pair<'a> {
    /// The key.
    pub key: PairPart<'a>,
    /// The order value.
    pub order: PairPart<'a>,
}

impl<'a> Pair<'a> {
    /// The pair of `key` and `order`.
    pub fn of(key: &'a Key, order: &'a OrderValue) -> Self {
        Self {
            key: PairPart::of_key(key),
            order: PairPart::of_order(order),
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
    pub(super) fn fence(previous: Option<Pair<'_>>, first: Self) -> Self {
        let order = match (previous, first.order) {
            (_, PairPart::Long(_)) => first.order,
            (Some(previous), PairPart::Bytes(bytes)) if previous.key == first.key => {
                let shared = match previous.order {
                    PairPart::Bytes(before) => shared_prefix(before, bytes),
                    PairPart::Long(_) => 0,
                };
                PairPart::Bytes(&bytes[..bytes.len().min(shared + 1)])
            }
            (None | Some(_), PairPart::Bytes(_)) => PairPart::Bytes(&[]),
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

/// Why a part pushed is of its column's variant.
const ONE_VARIANT: &str =
    "a column's fence parts are of the one variant its shape, fixed at build, decodes";

/// One part of every fence of a row group, in page order, as built: all
/// longs or all bytes, as the column's shape decodes.
enum PartsBuilder {
    Longs(Vec<i64>),
    Bytes { data: Vec<u8>, ends: Vec<u32> },
}

impl PartsBuilder {
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

    /// `part` after the parts held; `FencesOverflow` once the byte parts
    /// would end past `u32::MAX`, where `ends` addresses them.
    fn push(&mut self, part: PairPart<'_>) -> Result<(), FencesOverflow> {
        match (self, part) {
            (Self::Longs(values), PairPart::Long(value)) => values.push(value),
            (Self::Bytes { data, ends }, PairPart::Bytes(bytes)) => {
                let end = u32::try_from(data.len() + bytes.len()).map_err(|_| FencesOverflow)?;
                data.extend_from_slice(bytes);
                ends.push(end);
            }
            (Self::Longs(_), PairPart::Bytes(_)) | (Self::Bytes { .. }, PairPart::Long(_)) => {
                panic!("{ONE_VARIANT}")
            }
        }
        Ok(())
    }

    /// The parts, held with no spare capacity.
    fn finish(self) -> Parts {
        match self {
            Self::Longs(values) => Parts::Longs(values.into_boxed_slice()),
            Self::Bytes { data, ends } => Parts::Bytes {
                data: data.into_boxed_slice(),
                ends: ends.into_boxed_slice(),
            },
        }
    }
}

/// The byte parts of one row group's fences would end past `u32::MAX`,
/// beyond where the fences address them (`FileFault::OrderFencesOverflow`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct FencesOverflow;

/// One part of every fence of a row group, in page order, as held: all
/// longs, or the byte parts end to end and where each ends.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Parts {
    Longs(Box<[i64]>),
    Bytes { data: Box<[u8]>, ends: Box<[u32]> },
}

impl Parts {
    fn len(&self) -> usize {
        match self {
            Self::Longs(values) => values.len(),
            Self::Bytes { ends, .. } => ends.len(),
        }
    }

    /// Part `at`, which is below `len`.
    fn get(&self, at: usize) -> PairPart<'_> {
        match self {
            Self::Longs(values) => PairPart::Long(values[at]),
            Self::Bytes { data, ends } => {
                let start = at.checked_sub(1).map_or(0, |before| offset(ends[before]));
                PairPart::Bytes(&data[start..offset(ends[at])])
            }
        }
    }

    /// The heap the parts hold (`parts_heap`).
    fn heap_bytes(&self) -> u64 {
        let (long, bytes) = match self {
            Self::Longs(_) => (true, 0),
            Self::Bytes { data, .. } => (false, data.len()),
        };
        parts_heap(long, approx::count(self.len()), approx::count(bytes))
    }
}

/// An end of the byte parts as an offset into them.
fn offset(end: u32) -> usize {
    approx::length(end)
}

/// The heap `pages` fence parts of one column take, given `bytes`, what
/// their byte parts hold together: a long per long part, and per byte part
/// its bytes and where it ends; `long` for a column of long parts, which
/// hold no bytes. The footer's estimate and the built fences' exact charge
/// (`Parts::heap_bytes`) are both this.
pub(super) fn parts_heap(long: bool, pages: u64, bytes: u64) -> u64 {
    if long {
        pages.saturating_mul(approx::count(size_of::<i64>()))
    } else {
        pages
            .saturating_mul(approx::count(size_of::<u32>()))
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

/// The fences of one row group's order pages as they are recorded, one per
/// page in page order (`finish`).
pub(super) struct GroupFencesBuilder {
    keys: PartsBuilder,
    orders: PartsBuilder,
    /// Whether a push overflowed (`FencesOverflow`); nothing is pushed
    /// after one.
    overflowed: bool,
}

impl GroupFencesBuilder {
    /// No fence yet, with room for `pages` of a key column of `keys` and an
    /// order column of `orders`.
    pub(super) fn new(keys: KeyShape, orders: OrderShape, pages: usize) -> Self {
        Self {
            keys: PartsBuilder::with_capacity(long_keys(keys.key_type()), pages),
            orders: PartsBuilder::with_capacity(long_orders(orders), pages),
            overflowed: false,
        }
    }

    /// `fence` after the fences held; `FencesOverflow` past what the
    /// fences address, and for every push after one.
    pub(super) fn push(&mut self, fence: Pair<'_>) -> Result<(), FencesOverflow> {
        if self.overflowed {
            return Err(FencesOverflow);
        }
        let pushed = self
            .keys
            .push(fence.key)
            .and_then(|()| self.orders.push(fence.order));
        self.overflowed = pushed.is_err();
        pushed
    }

    /// Whether a push overflowed.
    pub(super) fn overflowed(&self) -> bool {
        self.overflowed
    }

    /// The fences recorded.
    pub(super) fn len(&self) -> usize {
        self.keys.len()
    }

    /// The fences, held with no spare capacity.
    pub(super) fn finish(self) -> GroupFences {
        GroupFences {
            keys: self.keys.finish(),
            orders: self.orders.finish(),
        }
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
    /// The fences held.
    pub(super) fn len(&self) -> usize {
        self.keys.len()
    }

    /// Fence `page`, which is below `len`.
    pub(super) fn get(&self, page: usize) -> Pair<'_> {
        Pair {
            key: self.keys.get(page),
            order: self.orders.get(page),
        }
    }

    /// Every fence, in page order.
    pub(super) fn iter(&self) -> impl Iterator<Item = Pair<'_>> {
        (0..self.len()).map(|page| self.get(page))
    }

    /// The last page whose fence is at or below `pair`, `None` below the
    /// first: one binary search.
    fn page_of(&self, pair: Pair<'_>) -> Option<usize> {
        partition_point(self.len(), |page| self.get(page) <= pair).checked_sub(1)
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
    pub fn fence(&self, row_group: usize, page: usize) -> Pair<'_> {
        self.groups[row_group].get(page)
    }

    /// The order page of `row_group` whose pairs' range holds `pair`: the
    /// last page whose fence is at or below it, `None` when `pair` sorts
    /// below the row group's first fence, before every pair the row group
    /// holds. A pair above the row group's last row lands on its last page,
    /// whose range runs up to the next row group's first fence
    /// (`page_pairs`). One binary search over the row group's fences.
    pub fn page_of(&self, row_group: usize, pair: Pair<'_>) -> Option<usize> {
        self.groups[row_group].page_of(pair)
    }

    /// The row group whose pairs' range holds `pair`: the last row group
    /// with pages whose first fence is at or below it, `None` when `pair`
    /// sorts below the file's first fence. A row group's range runs up to
    /// the next row group with pages, so a pair past a row group's last row
    /// and below the next one's first fence lands on the earlier row group.
    /// One binary search over the row groups, each step past the row groups
    /// without rows before the next with pages.
    pub fn row_group_of(&self, pair: Pair<'_>) -> Option<usize> {
        // The first fence from a row group only rises with the row group:
        // the predicate holds for a prefix of the row groups.
        let reached = partition_point(self.groups.len(), |row_group| {
            self.first_fence_from(row_group)
                .is_some_and(|first| first <= pair)
        });
        // The row group before `reached` reaches `pair` and the one at
        // `reached` does not, so the first with pages from it is itself.
        reached.checked_sub(1)
    }

    /// The row group and the order page within it whose pairs' range holds
    /// `pair` (`row_group_of`, then `page_of`); `None` below the file's
    /// first fence.
    pub fn place(&self, pair: Pair<'_>) -> Option<(usize, usize)> {
        let row_group = self.row_group_of(pair)?;
        Some((row_group, self.page_of(row_group, pair)?))
    }

    /// The range of pairs order page `page` of `row_group` holds: from its
    /// fence, inclusive, to the next page's, exclusive, the first fence of
    /// the next row group with pages after a row group's last page, and
    /// unbounded above after the file's last page (`None`).
    pub fn page_pairs(&self, row_group: usize, page: usize) -> (Pair<'_>, Option<Pair<'_>>) {
        let group = &self.groups[row_group];
        let next = if page + 1 < group.len() {
            Some(group.get(page + 1))
        } else {
            self.first_fence_from(row_group + 1)
        };
        (group.get(page), next)
    }

    /// The first fence of the first row group with pages at or after
    /// `row_group`, which is at most `row_groups`; `None` when no later row
    /// group has pages.
    fn first_fence_from(&self, row_group: usize) -> Option<Pair<'_>> {
        self.groups[row_group..]
            .iter()
            .find(|group| group.len() > 0)
            .map(|group| group.get(0))
    }

    /// The heap the fences hold: each row group's entry, and its parts.
    pub(super) fn heap_bytes(&self) -> u64 {
        self.groups
            .iter()
            .fold(groups_heap(self.groups.len()), |total, group| {
                total.saturating_add(group.heap_bytes())
            })
    }
}

/// The heap `OrderFences` of `row_groups` row groups take besides their
/// parts (`OrderFences::heap_bytes`).
pub(super) fn groups_heap(row_groups: usize) -> u64 {
    approx::count(row_groups * size_of::<GroupFences>())
}

#[cfg(test)]
thread_local! {
    /// The steps this thread's binary searches over fences took: what a
    /// test counts a placement's work by.
    static STEPS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// The steps binary searches over fences took on this thread since the
/// last call.
#[cfg(test)]
pub(super) fn take_steps() -> usize {
    STEPS.replace(0)
}

/// The first of `0..len` for which `below` is false, `below` holding for a
/// prefix of the range: `slice::partition_point` over positions.
fn partition_point(len: usize, below: impl Fn(usize) -> bool) -> usize {
    let (mut low, mut high) = (0, len);
    while low < high {
        #[cfg(test)]
        STEPS.set(STEPS.get() + 1);
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
    fn bytes_pair(key: i64, order: &[u8]) -> Pair<'_> {
        Pair {
            key: PairPart::Long(key),
            order: PairPart::Bytes(order),
        }
    }

    /// A pair of a long key and a long order value.
    fn long_pair(key: i64, order: i64) -> Pair<'static> {
        Pair {
            key: PairPart::Long(key),
            order: PairPart::Long(order),
        }
    }

    #[test]
    fn test_fence_cuts_a_byte_order_to_one_byte_past_the_prefix_it_shares() {
        let shared = [b'p'; 64];
        let before = [&shared[..], b"a-tail"].concat();
        let first = [&shared[..], b"b"].concat();
        let fence = Pair::fence(Some(bytes_pair(1, &before)), bytes_pair(1, &first));
        assert_eq!(fence, bytes_pair(1, &first[..=shared.len()]));
        // A first value that extends the one before it keeps one byte more.
        let longer = [&before[..], b"x"].concat();
        let fence = Pair::fence(Some(bytes_pair(1, &before)), bytes_pair(1, &longer));
        assert_eq!(fence, bytes_pair(1, &longer[..=before.len()]));
    }

    #[test]
    fn test_fence_of_a_new_key_takes_the_empty_byte_order_and_an_exact_long() {
        let fence = Pair::fence(Some(bytes_pair(1, b"zzz")), bytes_pair(2, b"abc"));
        assert_eq!(fence, bytes_pair(2, b""));
        assert_eq!(Pair::fence(None, bytes_pair(2, b"abc")), bytes_pair(2, b""));
        assert_eq!(
            Pair::fence(Some(long_pair(1, 9)), long_pair(2, 3)),
            long_pair(2, 3)
        );
        assert_eq!(
            Pair::fence(Some(long_pair(1, 3)), long_pair(1, 4)),
            long_pair(1, 4)
        );
        assert_eq!(Pair::fence(None, long_pair(1, 4)), long_pair(1, 4));
    }

    /// Fences over four row groups, the second and the last without rows.
    fn four_groups() -> OrderFences {
        let group = |fences: &[(i64, i64)]| {
            let mut group =
                GroupFencesBuilder::new(KeyShape::Long, OrderShape::Int64, fences.len());
            for &(key, order) in fences {
                group.push(long_pair(key, order)).unwrap();
            }
            group.finish()
        };
        OrderFences::new(vec![
            group(&[(1, 0), (1, 10), (2, 5)]),
            group(&[]),
            group(&[(3, 0)]),
            group(&[]),
        ])
    }

    #[test]
    fn test_page_of_places_a_pair_on_the_last_page_whose_fence_it_reaches() {
        let fences = four_groups();
        let key = Key::Long(1);
        let at = |order: i64| fences.page_of(0, Pair::of(&key, &OrderValue::Long(order)));
        assert_eq!(at(-1), None, "below the row group's first fence");
        assert_eq!(at(0), Some(0));
        assert_eq!(at(9), Some(0));
        assert_eq!(at(10), Some(1));
        let later = Key::Long(9);
        assert_eq!(
            fences.page_of(0, Pair::of(&later, &OrderValue::Long(0))),
            Some(2),
            "past the row group's last row: its last page"
        );
        assert_eq!(
            fences.page_of(1, Pair::of(&key, &OrderValue::Long(0))),
            None
        );
    }

    #[test]
    fn test_row_group_of_steps_past_the_row_groups_without_rows() {
        let fences = four_groups();
        let cases = [
            (long_pair(0, 0), None),
            (long_pair(1, 0), Some((0, 0))),
            (long_pair(2, 9), Some((0, 2))),
            (long_pair(2, i64::MAX), Some((0, 2))),
            (long_pair(3, 0), Some((2, 0))),
            (long_pair(9, 0), Some((2, 0))),
        ];
        for (pair, placed) in cases {
            assert_eq!(fences.place(pair), placed, "{pair:?}");
            assert_eq!(
                fences.row_group_of(pair),
                placed.map(|(row_group, _)| row_group)
            );
        }
        assert_eq!(OrderFences::default().row_group_of(long_pair(0, 0)), None);
    }

    #[test]
    fn test_page_pairs_end_at_the_next_fence_across_an_empty_row_group() {
        let fences = four_groups();
        assert_eq!(
            fences.page_pairs(0, 0),
            (long_pair(1, 0), Some(long_pair(1, 10)))
        );
        assert_eq!(
            fences.page_pairs(0, 2),
            (long_pair(2, 5), Some(long_pair(3, 0)))
        );
        assert_eq!(fences.page_pairs(2, 0), (long_pair(3, 0), None));
        assert_eq!((fences.row_groups(), fences.pages(1)), (4, 0));
    }

    #[test]
    fn test_heap_bytes_counts_each_group_and_its_parts() {
        let fences = four_groups();
        let pages = 4;
        let long_parts = 2 * pages * size_of::<i64>();
        assert_eq!(
            fences.heap_bytes(),
            approx::count(4 * size_of::<GroupFences>() + long_parts)
        );
        assert_eq!(
            fences.heap_bytes(),
            groups_heap(4)
                + parts_heap(true, approx::count(pages), 0)
                + parts_heap(true, approx::count(pages), 0)
        );
        let mut bytes = GroupFencesBuilder::new(KeyShape::Str, OrderShape::Str, 1);
        let key = Key::Str("k".to_string());
        let order = OrderValue::Str("abc".to_string());
        bytes.push(Pair::of(&key, &order)).unwrap();
        let held = OrderFences::new(vec![bytes.finish()]);
        assert_eq!(
            held.heap_bytes(),
            groups_heap(1) + parts_heap(false, 1, 1) + parts_heap(false, 1, 3)
        );
    }
}
