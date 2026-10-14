//! The ordered rules of verification (RFD 025, Load): an ordered table's
//! order column decoded beside its key column, every row's key and order
//! value strictly above the row's before it, across pages and row groups
//! alike, each order page's values within its column index entry, and the
//! order facts recorded on the way; and, since the entries are then checked
//! bounds, which pages an entry rules out of a range of order values. A
//! key-value table never reaches this module.

use std::cmp::Ordering;
use std::ops::Range;
use std::sync::Arc;

use parquet::data_type::{ByteArray, ByteArrayType, FixedLenByteArrayType, Int32Type, Int64Type};
use parquet::file::page_index::column_index::{
    ByteArrayColumnIndex, ColumnIndexMetaData, PrimitiveColumnIndex,
};

use super::build::{
    DECODE_BATCH_RECORDS, RunCheck, decode_run, fixed_as_binary, is_utf8, read_column,
};
use super::page_header::check_order_page_headers;
use super::{
    Column, ColumnChunk, FileFault, KeyShape, Keys, OrderLeaf, OrderShape, file_fault, row_index,
};
use crate::error::{Error, RefusalReason, Result};
use crate::key::Key;
use crate::order::{OrderFacts, OrderValue};
use crate::parquet_io::Segments;

/// A decoded run of order values in file order, holding the values parquet
/// decoded so that a run costs no allocation per value. Compares as
/// `OrderValue` does: longs numerically, strings and bytes bytewise.
pub(super) enum OrderRun {
    /// Integer and time values, an `INT32` column's widened.
    Long(Vec<i64>),
    /// String values as their bytes, which `is_utf8` checks.
    Str(Vec<ByteArray>),
    /// Binary and fixed-width values.
    Bin(Vec<ByteArray>),
}

impl OrderRun {
    /// The number of values.
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Long(run) => run.len(),
            Self::Str(run) | Self::Bin(run) => run.len(),
        }
    }

    /// Whether every value is UTF-8, in a run of strings; true in any
    /// other.
    pub(super) fn is_utf8(&self) -> bool {
        match self {
            Self::Str(run) => run.iter().all(|value| is_utf8(value.data())),
            Self::Long(_) | Self::Bin(_) => true,
        }
    }

    /// The value at `at`, which is below `len`; a run of strings is read
    /// back only once `is_utf8` holds for it.
    pub(super) fn value(&self, at: usize) -> OrderValue {
        match self {
            Self::Long(run) => OrderValue::Long(run[at]),
            Self::Str(run) => OrderValue::Str(
                String::from_utf8(run[at].data().to_vec())
                    .expect("a run of strings is read back once is_utf8 holds for it"),
            ),
            Self::Bin(run) => OrderValue::Bytes(run[at].data().to_vec()),
        }
    }

    /// Value `at` against value `other_at` of the same run.
    fn cmp_at(&self, at: usize, other_at: usize) -> Ordering {
        match self {
            Self::Long(run) => run[at].cmp(&run[other_at]),
            Self::Str(run) | Self::Bin(run) => run[at].data().cmp(run[other_at].data()),
        }
    }

    /// Value `at` against value `other_at` of `other`, a run of the same
    /// column in another file. Runs of different variants never meet
    /// within one table; when they do, the order is `OrderValue`'s derived
    /// order across variants.
    pub(super) fn cmp_run(&self, at: usize, other: &Self, other_at: usize) -> Ordering {
        match (self, other) {
            (Self::Long(mine), Self::Long(theirs)) => mine[at].cmp(&theirs[other_at]),
            (Self::Str(mine), Self::Str(theirs)) | (Self::Bin(mine), Self::Bin(theirs)) => {
                mine[at].data().cmp(theirs[other_at].data())
            }
            (
                Self::Long(_) | Self::Str(_) | Self::Bin(_),
                Self::Long(_) | Self::Str(_) | Self::Bin(_),
            ) => self.value(at).cmp(&other.value(other_at)),
        }
    }

    /// Value `at` against `value`, one of another run of the same column.
    /// A value of another variant never meets one of this run; when it
    /// does, the order is `OrderValue`'s derived order across variants.
    fn cmp_value(&self, at: usize, value: &OrderValue) -> Ordering {
        match (self, value) {
            (Self::Long(run), OrderValue::Long(other)) => run[at].cmp(other),
            (Self::Str(run), OrderValue::Str(other)) => run[at].data().cmp(other.as_bytes()),
            (Self::Bin(run), OrderValue::Bytes(other)) => run[at].data().cmp(other.as_slice()),
            (
                Self::Long(_) | Self::Str(_) | Self::Bin(_),
                OrderValue::Long(_) | OrderValue::Str(_) | OrderValue::Bytes(_),
            ) => self.value(at).cmp(value),
        }
    }

    /// The positions of the least and the greatest value; `None` for an
    /// empty run.
    fn extremes(&self) -> Option<(usize, usize)> {
        let positions = 0..self.len();
        let least = positions.clone().min_by(|&a, &b| self.cmp_at(a, b))?;
        let greatest = positions.max_by(|&a, &b| self.cmp_at(a, b))?;
        Some((least, greatest))
    }
}

/// Rows `rows` of the order column chunk `chunk` decoded from `chunks`,
/// which hold at least the pages those rows are in, as `decode_run` decodes
/// keys: exactly those rows, nulls left out, and an `INT32` column read as
/// longs. A string column's values are not checked as UTF-8 here.
pub(super) fn decode_orders(
    chunk: ColumnChunk<'_>,
    shape: OrderShape,
    rows: Range<i64>,
    chunks: Arc<Segments>,
) -> Result<OrderRun> {
    let skip = row_index(rows.start);
    let take = row_index(rows.end - rows.start);
    Ok(match shape {
        OrderShape::Int32 => OrderRun::Long(
            read_column::<Int32Type>(chunk, chunks, skip, take)?
                .into_iter()
                .map(i64::from)
                .collect(),
        ),
        OrderShape::Int64 => OrderRun::Long(read_column::<Int64Type>(chunk, chunks, skip, take)?),
        OrderShape::Str => OrderRun::Str(read_column::<ByteArrayType>(chunk, chunks, skip, take)?),
        OrderShape::Bin => OrderRun::Bin(read_column::<ByteArrayType>(chunk, chunks, skip, take)?),
        OrderShape::Fixed(_) => OrderRun::Bin(fixed_as_binary(
            read_column::<FixedLenByteArrayType>(chunk, chunks, skip, take)?,
        )),
    })
}

/// How a row's pair fails to be above the pair before it.
#[derive(Debug)]
enum Disorder {
    /// The key falls, or the key holds and the order value falls.
    Unsorted,
    /// The key and the order value both hold.
    Duplicate { key: Key, order: OrderValue },
}

/// What `PairCheck` learns of one row group, found as its rows decode and
/// applied by `verify` once the whole row group has decoded, in the order
/// it checks a row group in.
pub(super) struct GroupOutcome {
    /// The order values decoded fall short of the row group's rows.
    null_order: bool,
    not_utf8: bool,
    /// The first order page a value of lies outside its column index entry.
    index_mismatch: Option<usize>,
    disorder: Option<Disorder>,
}

impl GroupOutcome {
    /// `RefusalReason::NullOrder` under `table` when the row group `chunk`
    /// holds a null order value.
    pub(super) fn check_nulls(&self, table: &str, chunk: ColumnChunk<'_>) -> Result<()> {
        if self.null_order {
            let reason = RefusalReason::NullOrder {
                path: chunk.column.path.to_string(),
                row_group: chunk.row_group(),
            };
            return Err(Error::refused(table, reason));
        }
        Ok(())
    }

    /// The rest of what the row group `chunk` found, in order: a string
    /// order value that is not UTF-8 (`FileFault::OrderNotUtf8`), an order
    /// page outside its column index entry (`FileFault::OrderIndexMismatch`),
    /// then a pair not above the one before it under `table`, an equal one
    /// `RefusalReason::DuplicateRows` and a falling one
    /// `RefusalReason::Unsorted`.
    pub(super) fn check_values(self, table: &str, chunk: ColumnChunk<'_>) -> Result<()> {
        let path = chunk.column.path;
        let row_group = chunk.row_group();
        if self.not_utf8 {
            return Err(file_fault(path, FileFault::OrderNotUtf8 { row_group }));
        }
        if let Some(page) = self.index_mismatch {
            return Err(file_fault(
                path,
                FileFault::OrderIndexMismatch { row_group, page },
            ));
        }
        let reason = match self.disorder {
            None => return Ok(()),
            Some(Disorder::Unsorted) => RefusalReason::Unsorted {
                path: path.to_string(),
                row_group,
            },
            Some(Disorder::Duplicate { key, order }) => RefusalReason::DuplicateRows {
                path: path.to_string(),
                key,
                order: Box::new(order),
            },
        };
        Err(Error::refused(table, reason))
    }
}

/// One row group's progress through `PairCheck`.
#[derive(Default)]
struct GroupState {
    /// Order values decoded.
    values: usize,
    /// Whether every decode window held a value per row in both columns;
    /// once one did not, a row's key and order value are no longer known
    /// to belong together, and the null that broke it is refused.
    misaligned: bool,
    /// Whether a row of the row group has been compared.
    started: bool,
    distinct: u64,
    not_utf8: bool,
    /// The order page of the row last checked against the column index.
    page: usize,
    index_mismatch: Option<usize>,
    disorder: Option<Disorder>,
}

/// What `verify` learns of an ordered file's rows, a row group at a time
/// and within one a decode window at a time, so that at most a window of
/// keys and order values is held however long the row group is: whether
/// each row's key and order value are above the row's before it, the last
/// pair carried across windows and row groups, whether each order value
/// lies within its page's column index entry, and the order facts.
pub(super) struct PairCheck<'a> {
    /// The order column.
    column: Column<'a>,
    shape: OrderShape,
    min: Option<OrderValue>,
    max: Option<OrderValue>,
    distinct_keys: Vec<u64>,
    longest_run: u64,
    /// The pair of the last row compared, and the rows of its key's run.
    last: Option<(Key, OrderValue)>,
    run: u64,
    group: GroupState,
}

impl<'a> PairCheck<'a> {
    /// A check of the order column `order` of the file `keys` is the key
    /// column of.
    pub(super) fn new(keys: &Column<'a>, order: OrderLeaf) -> Self {
        Self {
            column: Column {
                leaf: order.leaf,
                ..*keys
            },
            shape: order.shape,
            min: None,
            max: None,
            distinct_keys: Vec::with_capacity(keys.metadata.num_row_groups()),
            longest_run: 0,
            last: None,
            run: 0,
            group: GroupState::default(),
        }
    }

    /// Every key of row group `chunk`, whose keys decode as `shape`, and
    /// every order value beside it, decoded from `chunks` a window of
    /// `DECODE_BATCH_RECORDS` rows at a time and checked: the keys by
    /// `RunCheck` with the ordered rules, which leave their order to this
    /// check, and the pairs here. Every order page header of the row group
    /// is checked first (`check_order_page_headers`), as the key pages'
    /// are. A window starting inside a page decodes that page from its
    /// start again, and a dictionary-encoded chunk decodes its dictionary
    /// once a window.
    pub(super) fn check_group<'k>(
        &mut self,
        chunk: ColumnChunk<'k>,
        shape: KeyShape,
        chunks: &Arc<Segments>,
    ) -> Result<(RunCheck<'k>, GroupOutcome)> {
        let order_chunk = self.column.group(chunk.row_group());
        check_order_page_headers(order_chunk, self.shape, chunks)?;
        self.group = GroupState::default();
        let pages = order_chunk.page_locations().len();
        if let Some(index) = order_column_index(order_chunk) {
            let described = usize::try_from(index.num_pages()).unwrap_or(usize::MAX);
            if described != pages {
                self.group.index_mismatch = Some(described.min(pages));
            }
        }
        let rows = row_index(chunk.rows());
        let window = row_index(DECODE_BATCH_RECORDS);
        let mut run = RunCheck::ordered(chunk);
        let mut start = 0;
        while start < rows {
            let end = rows.min(start + window);
            let range = super::row_offset(start)..super::row_offset(end);
            let keys = decode_run(chunk, shape, range.clone(), Arc::clone(chunks))?;
            let orders = decode_orders(order_chunk, self.shape, range, Arc::clone(chunks))?;
            run.visit(&keys);
            self.visit(
                order_chunk,
                start,
                end - start,
                &keys,
                &orders,
                run.not_utf8(),
            );
            start = end;
        }
        let group = std::mem::take(&mut self.group);
        self.distinct_keys.push(group.distinct);
        let outcome = GroupOutcome {
            null_order: group.values < rows,
            not_utf8: group.not_utf8,
            index_mismatch: group.index_mismatch,
            disorder: group.disorder,
        };
        Ok((run, outcome))
    }

    /// What the check recorded of a file whose every row group passed it.
    pub(super) fn facts(self) -> OrderFacts {
        let has_rows = "inspect checked that the row groups sum to the file's rows, which are above zero, so an order value was decoded";
        OrderFacts {
            min: self.min.expect(has_rows),
            max: self.max.expect(has_rows),
            distinct_keys: self.distinct_keys,
            longest_run: self.longest_run,
        }
    }

    /// The next window of the row group: its `rows` rows from row `base`
    /// on, their `keys` and `orders`. Nothing more is compared once a window
    /// holds a null in either column, a key that is not UTF-8
    /// (`keys_not_utf8`, which `RunCheck` found), or an order value that is
    /// not, since a refusal or a fault then stands; the column index is
    /// checked past a disorder, since a fault stands before a refusal.
    fn visit(
        &mut self,
        order_chunk: ColumnChunk<'a>,
        base: usize,
        rows: usize,
        keys: &Keys,
        orders: &OrderRun,
        keys_not_utf8: bool,
    ) {
        self.group.values += orders.len();
        if keys.len() != rows || orders.len() != rows {
            self.group.misaligned = true;
        }
        if self.group.misaligned || keys_not_utf8 || self.group.not_utf8 {
            return;
        }
        if !orders.is_utf8() {
            self.group.not_utf8 = true;
            return;
        }
        self.check_index(order_chunk, base, orders);
        if self.group.disorder.is_none() {
            self.check_pairs(keys, orders);
        }
    }

    /// Whether each of `orders`, rows from `base` on, lies within its
    /// order page's column index entry, when the chunk has one; the first
    /// page that a value does not is the mismatch. A truncated byte-array
    /// entry is still a bound and is compared as one.
    fn check_index(&mut self, chunk: ColumnChunk<'a>, base: usize, orders: &OrderRun) {
        if self.group.index_mismatch.is_some() {
            return;
        }
        let Some(entry) = order_column_index(chunk) else {
            return;
        };
        let locations = chunk.page_locations();
        let page = &mut self.group.page;
        for at in 0..orders.len() {
            let row = base + at;
            while locations
                .get(*page + 1)
                .is_some_and(|next| row_index(next.first_row_index) <= row)
            {
                *page += 1;
            }
            if !within(entry, *page, orders, at) {
                self.group.index_mismatch = Some(*page);
                return;
            }
        }
    }

    /// Each row of the window against the row before it, the first against
    /// the last row of the window or row group before it: a key above it
    /// starts a run, a key equal to it continues the run when its order
    /// value is above, and anything else is the disorder. Every row group's
    /// first row counts its key among the row group's distinct keys, a run
    /// crossing into it included, and the least and greatest order value
    /// and the longest run follow the rows compared.
    fn check_pairs(&mut self, keys: &Keys, orders: &OrderRun) {
        for at in 0..keys.len() {
            let key = if at == 0 {
                self.last.as_ref().map(|(key, _)| keys.get(0).cmp(key))
            } else {
                Some(keys.cmp_at(at, keys, at - 1))
            };
            let continues = match key {
                None | Some(Ordering::Greater) => false,
                Some(Ordering::Less) => {
                    self.group.disorder = Some(Disorder::Unsorted);
                    return;
                }
                Some(Ordering::Equal) => {
                    let order = match (at, &self.last) {
                        (0, Some((_, last))) => orders.cmp_value(0, last),
                        _ => orders.cmp_at(at, at - 1),
                    };
                    match order {
                        Ordering::Greater => true,
                        Ordering::Less => {
                            self.group.disorder = Some(Disorder::Unsorted);
                            return;
                        }
                        Ordering::Equal => {
                            self.group.disorder = Some(Disorder::Duplicate {
                                key: keys.get(at),
                                order: orders.value(at),
                            });
                            return;
                        }
                    }
                }
            };
            self.run = if continues { self.run + 1 } else { 1 };
            if !continues || !self.group.started {
                self.group.distinct += 1;
            }
            self.group.started = true;
            self.longest_run = self.longest_run.max(self.run);
        }
        let tail = keys.len() - 1;
        self.last = Some((keys.get(tail), orders.value(tail)));
        if let Some((least, greatest)) = orders.extremes() {
            let least = orders.value(least);
            let greatest = orders.value(greatest);
            if self.min.as_ref().is_none_or(|min| least < *min) {
                self.min = Some(least);
            }
            if self.max.as_ref().is_none_or(|max| greatest > *max) {
                self.max = Some(greatest);
            }
        }
    }
}

/// The column index entry of `chunk`, an order column chunk; `None` when
/// the file has none for it, which a Scan reads around and the check across
/// files reads every page of.
pub(super) fn order_column_index(chunk: ColumnChunk<'_>) -> Option<&ColumnIndexMetaData> {
    let entry = chunk
        .column
        .metadata
        .column_index()?
        .get(chunk.row_group())?
        .get(chunk.column.leaf)?;
    match entry {
        ColumnIndexMetaData::NONE => None,
        ColumnIndexMetaData::BOOLEAN(_)
        | ColumnIndexMetaData::INT32(_)
        | ColumnIndexMetaData::INT64(_)
        | ColumnIndexMetaData::INT96(_)
        | ColumnIndexMetaData::FLOAT(_)
        | ColumnIndexMetaData::DOUBLE(_)
        | ColumnIndexMetaData::BYTE_ARRAY(_)
        | ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(_) => Some(entry),
    }
}

/// Whether page `page`'s entry of the column index `entry` meets the
/// closed range `orders`: its minimum at or below the range's top and its
/// maximum at or above its bottom, an integer entry against longs and a
/// byte-array entry against bytes. Verification checked that every value
/// of a page lies within its entry (`within`), so a page that does not
/// meet the range holds no value in it. An entry that bounds nothing for
/// these values, of another type or with a bound missing, meets every
/// range.
pub(super) fn page_meets(
    entry: &ColumnIndexMetaData,
    page: usize,
    orders: (&OrderValue, &OrderValue),
) -> bool {
    match entry {
        ColumnIndexMetaData::INT32(index) => primitive_meets(index, page, orders, i64::from),
        ColumnIndexMetaData::INT64(index) => primitive_meets(index, page, orders, |value| value),
        ColumnIndexMetaData::BYTE_ARRAY(index)
        | ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(index) => bytes_meet(index, page, orders),
        ColumnIndexMetaData::NONE
        | ColumnIndexMetaData::BOOLEAN(_)
        | ColumnIndexMetaData::INT96(_)
        | ColumnIndexMetaData::FLOAT(_)
        | ColumnIndexMetaData::DOUBLE(_) => true,
    }
}

/// `page_meets` for an integer entry, its values widened to longs by
/// `widen`.
fn primitive_meets<T: Copy>(
    index: &PrimitiveColumnIndex<T>,
    page: usize,
    orders: (&OrderValue, &OrderValue),
    widen: fn(T) -> i64,
) -> bool {
    match (index.min_value(page), index.max_value(page), orders) {
        (Some(&min), Some(&max), (OrderValue::Long(low), OrderValue::Long(high))) => {
            widen(min) <= *high && *low <= widen(max)
        }
        (None | Some(_), None | Some(_), _) => true,
    }
}

/// `page_meets` for a byte-array entry.
fn bytes_meet(
    index: &ByteArrayColumnIndex,
    page: usize,
    (low, high): (&OrderValue, &OrderValue),
) -> bool {
    match (
        index.min_value(page),
        index.max_value(page),
        order_bytes(low),
        order_bytes(high),
    ) {
        (Some(min), Some(max), Some(low), Some(high)) => min <= high && low <= max,
        (None | Some(_), None | Some(_), None | Some(_), None | Some(_)) => true,
    }
}

/// Where an order page's values lie against a Scan's exclusive bounds, as
/// its column index entry tells (RFD 025, Cost).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PageFit {
    /// Every value of the page is at or below `after`.
    Below,
    /// Every value of the page is at or above `before`.
    Above,
    /// Every value of the page is inside the bounds.
    Inside,
    /// Some may be, or nothing known says: the page is decoded to tell.
    Straddles,
}

/// What is known of the order values of a run's rows on one order page:
/// a least and a greatest value, either missing where nothing bounds that
/// side. Verification checked that every value of a page lies within its
/// column index entry (`within`), a truncated byte-array entry included,
/// so an entry's extremes bound the page's values; a page that holds the
/// edge of a key's run holds a neighbouring key's values too, which its
/// entry covers, so the run's own rows there are bounded more tightly by
/// the run's next or previous page where that page holds the run alone
/// and its entry is exact, an integer one (`after_page`, `before_page`).
#[derive(Clone, Copy, Default)]
pub(super) struct ValueRange<'a> {
    low: Option<Side<'a>>,
    high: Option<Side<'a>>,
}

/// One side of a `ValueRange`: the value, and whether the values bounded
/// are strictly past it rather than possibly at it.
#[derive(Clone, Copy)]
struct Side<'a> {
    value: Extreme<'a>,
    exclusive: bool,
}

impl Side<'_> {
    /// Whether the side is a value some row holds, as an integer entry's
    /// extremes are: a writer truncates only byte-array entries, whose
    /// minimum may then be a prefix and whose maximum a prefix incremented,
    /// bounds that no row holds.
    fn exact(side: &Self) -> bool {
        match side.value {
            Extreme::Long(_) => true,
            Extreme::Bytes(_) => false,
        }
    }
}

impl<'a> ValueRange<'a> {
    /// The extremes the column index entry `entry` gives page `page`, both
    /// held by some value; nothing for a chunk without an entry, `None`,
    /// or an entry that bounds nothing for the page.
    pub(super) fn of_page(entry: Option<&'a ColumnIndexMetaData>, page: usize) -> Self {
        match entry.and_then(|entry| page_extremes(entry, page)) {
            Some((low, high)) => Self {
                low: Some(Side {
                    value: low,
                    exclusive: false,
                }),
                high: Some(Side {
                    value: high,
                    exclusive: false,
                }),
            },
            None => Self::default(),
        }
    }

    /// This range for rows that follow, in a run ascending by order value,
    /// every row `earlier` bounds: above `earlier`'s greatest value, where
    /// that is tighter than the least this range knows. Only an integer
    /// entry's greatest value is one a row holds; a byte-array entry may be
    /// truncated, its greatest value then above every value of its page
    /// (`Side::exact`), so it rules nothing out beyond its own page and this
    /// range is left as it is.
    pub(super) fn after_page(self, earlier: Self) -> Self {
        let Some(bound) = earlier.high.filter(Side::exact) else {
            return self;
        };
        let raised = Side {
            exclusive: true,
            ..bound
        };
        let low = match self.low {
            Some(low) if low.value.cmp_extreme(raised.value) != Some(Ordering::Less) => low,
            Some(_) | None => raised,
        };
        Self {
            low: Some(low),
            ..self
        }
    }

    /// This range for rows that precede every row `later` bounds: below
    /// `later`'s least value, where that is tighter than the greatest this
    /// range knows; only from an integer entry, as for `after_page`, since a
    /// truncated byte-array entry's least value is a prefix below its
    /// page's values.
    pub(super) fn before_page(self, later: Self) -> Self {
        let Some(bound) = later.low.filter(Side::exact) else {
            return self;
        };
        let lowered = Side {
            exclusive: true,
            ..bound
        };
        let high = match self.high {
            Some(high) if high.value.cmp_extreme(lowered.value) != Some(Ordering::Greater) => high,
            Some(_) | None => lowered,
        };
        Self {
            high: Some(high),
            ..self
        }
    }

    /// Where the values this range bounds lie against `after` and
    /// `before`, both exclusive: `Inside` when there is neither bound;
    /// otherwise `Below` when the greatest is at or below `after`, `Above`
    /// when the least is at or above `before`, `Inside` when the least is
    /// above `after` and the greatest below `before`, and `Straddles` for
    /// any other range, one with a side unknown, and one whose sides bound
    /// nothing for these values.
    pub(super) fn fit(self, after: Option<&OrderValue>, before: Option<&OrderValue>) -> PageFit {
        let at_or_below_after = after.is_some_and(|after| {
            self.high
                .is_some_and(|high| high.value.cmp_value(after).is_some_and(Ordering::is_le))
        });
        let at_or_above_before = before.is_some_and(|before| {
            self.low
                .is_some_and(|low| low.value.cmp_value(before).is_some_and(Ordering::is_ge))
        });
        if at_or_below_after {
            return PageFit::Below;
        }
        if at_or_above_before {
            return PageFit::Above;
        }
        let above_after = after.is_none_or(|after| {
            self.low.is_some_and(|low| {
                low.value
                    .cmp_value(after)
                    .is_some_and(|order| order.is_gt() || (low.exclusive && order.is_eq()))
            })
        });
        let below_before = before.is_none_or(|before| {
            self.high.is_some_and(|high| {
                high.value
                    .cmp_value(before)
                    .is_some_and(|order| order.is_lt() || (high.exclusive && order.is_eq()))
            })
        });
        if above_after && below_before {
            PageFit::Inside
        } else {
            PageFit::Straddles
        }
    }
}

/// The least or greatest value a column index entry gives a page, as an
/// integer entry or a byte-array entry holds it.
#[derive(Clone, Copy)]
enum Extreme<'a> {
    Long(i64),
    Bytes(&'a [u8]),
}

impl Extreme<'_> {
    /// The extreme against `value`: a long numerically, bytes against a
    /// string's or binary value's bytes; `None` across kinds, which bound
    /// nothing for each other.
    fn cmp_value(self, value: &OrderValue) -> Option<Ordering> {
        match (self, value) {
            (Self::Long(extreme), OrderValue::Long(value)) => Some(extreme.cmp(value)),
            (Self::Bytes(extreme), OrderValue::Str(_) | OrderValue::Bytes(_)) => {
                order_bytes(value).map(|value| extreme.cmp(value))
            }
            (Self::Long(_), OrderValue::Str(_) | OrderValue::Bytes(_))
            | (Self::Bytes(_), OrderValue::Long(_)) => None,
        }
    }

    /// The extreme against `other`, of another page of the same column;
    /// `None` across kinds.
    fn cmp_extreme(self, other: Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Long(mine), Self::Long(theirs)) => Some(mine.cmp(&theirs)),
            (Self::Bytes(mine), Self::Bytes(theirs)) => Some(mine.cmp(theirs)),
            (Self::Long(_), Self::Bytes(_)) | (Self::Bytes(_), Self::Long(_)) => None,
        }
    }
}

/// The minimum and maximum `entry` gives page `page`; `None` for an entry
/// of a type no order column is stored as, or a page it gives no bound.
fn page_extremes(entry: &ColumnIndexMetaData, page: usize) -> Option<(Extreme<'_>, Extreme<'_>)> {
    match entry {
        ColumnIndexMetaData::INT32(index) => Some((
            Extreme::Long(i64::from(*index.min_value(page)?)),
            Extreme::Long(i64::from(*index.max_value(page)?)),
        )),
        ColumnIndexMetaData::INT64(index) => Some((
            Extreme::Long(*index.min_value(page)?),
            Extreme::Long(*index.max_value(page)?),
        )),
        ColumnIndexMetaData::BYTE_ARRAY(index)
        | ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(index) => Some((
            Extreme::Bytes(index.min_value(page)?),
            Extreme::Bytes(index.max_value(page)?),
        )),
        ColumnIndexMetaData::NONE
        | ColumnIndexMetaData::BOOLEAN(_)
        | ColumnIndexMetaData::INT96(_)
        | ColumnIndexMetaData::FLOAT(_)
        | ColumnIndexMetaData::DOUBLE(_) => None,
    }
}

/// The bytes a string or binary order value compares by; `None` for a long.
fn order_bytes(value: &OrderValue) -> Option<&[u8]> {
    match value {
        OrderValue::Str(text) => Some(text.as_bytes()),
        OrderValue::Bytes(bytes) => Some(bytes),
        OrderValue::Long(_) => None,
    }
}

/// Whether value `at` of `orders` lies within page `page`'s entry of the
/// column index `entry`: at or above its minimum and at or below its
/// maximum, an integer entry against a long numerically and a byte-array
/// entry against bytes bytewise. A page the entry calls all null, or an
/// entry of a type the column's values are not, bounds nothing.
fn within(entry: &ColumnIndexMetaData, page: usize, orders: &OrderRun, at: usize) -> bool {
    match (entry, orders) {
        (ColumnIndexMetaData::INT32(index), OrderRun::Long(run)) => {
            within_primitive(index, page, run[at], i64::from)
        }
        (ColumnIndexMetaData::INT64(index), OrderRun::Long(run)) => {
            within_primitive(index, page, run[at], |value| value)
        }
        (
            ColumnIndexMetaData::BYTE_ARRAY(index)
            | ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(index),
            OrderRun::Str(run) | OrderRun::Bin(run),
        ) => within_bytes(index, page, run[at].data()),
        (
            ColumnIndexMetaData::INT32(_) | ColumnIndexMetaData::INT64(_),
            OrderRun::Str(_) | OrderRun::Bin(_),
        )
        | (
            ColumnIndexMetaData::BYTE_ARRAY(_) | ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(_),
            OrderRun::Long(_),
        )
        | (
            ColumnIndexMetaData::NONE
            | ColumnIndexMetaData::BOOLEAN(_)
            | ColumnIndexMetaData::INT96(_)
            | ColumnIndexMetaData::FLOAT(_)
            | ColumnIndexMetaData::DOUBLE(_),
            OrderRun::Long(_) | OrderRun::Str(_) | OrderRun::Bin(_),
        ) => false,
    }
}

/// `within` for an integer entry, its values widened to longs by `widen`.
fn within_primitive<T: Copy>(
    index: &PrimitiveColumnIndex<T>,
    page: usize,
    value: i64,
    widen: fn(T) -> i64,
) -> bool {
    match (index.min_value(page), index.max_value(page)) {
        (Some(&min), Some(&max)) => widen(min) <= value && value <= widen(max),
        (None | Some(_), None) | (None, Some(_)) => false,
    }
}

/// `within` for a byte-array entry.
fn within_bytes(index: &ByteArrayColumnIndex, page: usize, value: &[u8]) -> bool {
    match (index.min_value(page), index.max_value(page)) {
        (Some(min), Some(max)) => min <= value && value <= max,
        (None | Some(_), None) | (None, Some(_)) => false,
    }
}
