//! The ordered rules of verification (RFD 025, Load): an ordered table's
//! order column decoded beside its key column, every row's key and order
//! value strictly above the row's before it, across pages and row groups
//! alike, and the order facts and the order fences (`order_fences`)
//! recorded on the way. The writer's column index of the order column is
//! neither read nor checked: the fences place every pair whatever it holds.
//! A key-value table never reaches this module.

use std::cmp::Ordering;
use std::sync::Arc;

use super::build::{DECODE_BATCH_RECORDS, RunBatches, RunCheck, keys_are_utf8};
use super::order_fences::{GroupFences, GroupFencesBuilder, OrderFences, Pair, PairPart};
use super::page_header::check_order_page_headers;
use super::{
    Column, ColumnChunk, FileFault, KeyShape, Keys, OrderLeaf, OrderShape, PageStarts, file_fault,
    row_index,
};
use crate::error::{Error, RefusalReason, Result};
use crate::key::Key;
use crate::order::{OrderFacts, OrderValue};
use crate::parquet_io::Segments;

/// A decoded run of order values in file order: the `Keys` run their
/// shape decodes to (`OrderShape::run_shape`), never one of uuids, holding
/// the values parquet decoded so that a run costs no allocation per value.
/// Compares as `OrderValue` does: longs numerically, strings and bytes
/// bytewise.
pub(super) struct OrderRun(pub(super) Keys);

impl OrderRun {
    /// The number of values.
    pub(super) fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether every value is UTF-8, in a run of strings; true in any
    /// other.
    pub(super) fn is_utf8(&self) -> bool {
        keys_are_utf8(&self.0)
    }

    /// The value at `at`, which is below `len`; a run of strings is read
    /// back only once `is_utf8` holds for it.
    pub(super) fn value(&self, at: usize) -> OrderValue {
        match self.0.get(at) {
            Key::Long(value) => OrderValue::Long(value),
            Key::Str(value) => OrderValue::Str(value),
            Key::Bytes(value) => OrderValue::Bytes(value),
            Key::Uuid(value) => OrderValue::Bytes(value.to_vec()),
        }
    }

    /// Value `at`, which is below `len`, as fences compare it.
    fn part(&self, at: usize) -> PairPart<'_> {
        self.0.part(at)
    }

    /// Value `at` against value `other_at` of `other`, this run or a run of
    /// the same column in another file (`Keys::cmp_at`).
    pub(super) fn cmp_at(&self, at: usize, other: &Self, other_at: usize) -> Ordering {
        self.0.cmp_at(at, &other.0, other_at)
    }

    /// Value `at` against `value`, one of another run of the same column.
    /// A value of another variant never meets one of this run; when it
    /// does, the order is `OrderValue`'s derived order across variants.
    pub(super) fn cmp_value(&self, at: usize, value: &OrderValue) -> Ordering {
        match (&self.0, value) {
            (Keys::Long(run), OrderValue::Long(other)) => run[at].cmp(other),
            (Keys::Str(run), OrderValue::Str(other)) => run[at].data().cmp(other.as_bytes()),
            (Keys::Bin(run), OrderValue::Bytes(other)) => run[at].data().cmp(other.as_slice()),
            (
                Keys::Long(_) | Keys::Str(_) | Keys::Bin(_) | Keys::Uuid(_),
                OrderValue::Long(_) | OrderValue::Str(_) | OrderValue::Bytes(_),
            ) => self.value(at).cmp(value),
        }
    }

    /// The positions of the least and the greatest value; `None` for an
    /// empty run.
    fn extremes(&self) -> Option<(usize, usize)> {
        let positions = 0..self.len();
        let least = positions.clone().min_by(|&a, &b| self.cmp_at(a, self, b))?;
        let greatest = positions.max_by(|&a, &b| self.cmp_at(a, self, b))?;
        Some((least, greatest))
    }
}

/// What the ordered rules record of a file that passed them (RFD 025,
/// Load): its order facts and its order fences.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct OrderVerified {
    pub(super) facts: OrderFacts,
    pub(super) fences: OrderFences,
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
    /// The row group's order fences outgrew what they are addressed by.
    fences_overflow: bool,
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
    /// order value that is not UTF-8 (`FileFault::OrderNotUtf8`), order
    /// fences past what they are addressed by
    /// (`FileFault::OrderFencesOverflow`), then a pair not above the one
    /// before it under `table`, an equal one `RefusalReason::DuplicateRows`
    /// and a falling one `RefusalReason::Unsorted`.
    pub(super) fn check_values(self, table: &str, chunk: ColumnChunk<'_>) -> Result<()> {
        let path = chunk.column.path;
        let row_group = chunk.row_group();
        if self.not_utf8 {
            return Err(file_fault(path, FileFault::OrderNotUtf8 { row_group }));
        }
        if self.fences_overflow {
            return Err(file_fault(
                path,
                FileFault::OrderFencesOverflow { row_group },
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
    disorder: Option<Disorder>,
}

/// What `verify` learns of an ordered file's rows, a row group at a time
/// and within one a decode window at a time, so that at most a window of
/// keys and order values is held however long the row group is: whether
/// each row's key and order value are above the row's before it, the last
/// pair carried across windows and row groups, the order facts, and each
/// order page's fence.
pub(super) struct PairCheck<'a> {
    /// The order column.
    column: Column<'a>,
    shape: OrderShape,
    min: Option<OrderValue>,
    max: Option<OrderValue>,
    distinct_keys: Vec<u64>,
    longest_run: u64,
    /// The fences of the row groups checked, in footer order.
    fences: Vec<GroupFences>,
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
            fences: Vec::with_capacity(keys.metadata.num_row_groups()),
            last: None,
            run: 0,
            group: GroupState::default(),
        }
    }

    /// Every key of row group `chunk`, whose keys decode as `shape`, and
    /// every order value beside it, decoded from `chunks` by one column
    /// reader per column in lockstep, a window of `DECODE_BATCH_RECORDS`
    /// rows at a time, and checked: the keys by `RunCheck` with the ordered
    /// rules, which leave their order to this check, and the pairs here,
    /// each order page's fence recorded at its first row. Every order page
    /// header of the row group is checked first (`check_order_page_headers`),
    /// as the key pages' are. Each column's dictionary page is decoded once
    /// for the row group and each page once, whatever windows it spans. A
    /// decode fault stands before anything the windows found, the key
    /// column's before the order column's when both end in one, each at the
    /// rows it was asked for.
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
        let rows = row_index(chunk.rows());
        let window = row_index(DECODE_BATCH_RECORDS);
        let mut run = RunCheck::ordered(chunk);
        let mut fences = GroupFencesBuilder::new(shape, self.shape, pages);
        let mut fence_pages = PageStarts::of(order_chunk);
        let mut key_batches = RunBatches::open(chunk, shape.run_shape(), chunks)?;
        let mut order_batches = RunBatches::open(order_chunk, self.shape.run_shape(), chunks)?;
        let mut start = 0;
        while start < rows {
            let wanted = window.min(rows - start);
            let Some((read, keys)) = key_batches.next(wanted)? else {
                break;
            };
            let Some((order_read, orders)) = order_batches.next(wanted)? else {
                break;
            };
            if read != wanted || order_read != wanted {
                break;
            }
            let orders = OrderRun(orders);
            run.visit(&keys);
            let batch = Window {
                base: start,
                rows: wanted,
                keys: &keys,
                orders: &orders,
            };
            self.visit(&mut fence_pages, &batch, run.not_utf8(), &mut fences);
            start += wanted;
        }
        key_batches.finish()?;
        order_batches.finish()?;
        let group = &self.group;
        debug_assert!(
            group.misaligned
                || group.not_utf8
                || run.not_utf8()
                || group.disorder.is_some()
                || fences.overflowed()
                || fences.len() == pages,
            "a row group that passes the check has a fence per order page"
        );
        let fences_overflow = fences.overflowed();
        self.fences.push(fences.finish());
        let group = std::mem::take(&mut self.group);
        self.distinct_keys.push(group.distinct);
        let outcome = GroupOutcome {
            null_order: group.values < rows,
            not_utf8: group.not_utf8,
            fences_overflow,
            disorder: group.disorder,
        };
        Ok((run, outcome))
    }

    /// What the check recorded of a file whose every row group passed it.
    pub(super) fn verified(self) -> OrderVerified {
        let has_rows = "inspect checked that the row groups sum to the file's rows, which are above zero, so an order value was decoded";
        OrderVerified {
            facts: OrderFacts {
                min: self.min.expect(has_rows),
                max: self.max.expect(has_rows),
                distinct_keys: self.distinct_keys,
                longest_run: self.longest_run,
            },
            fences: OrderFences::new(self.fences),
        }
    }

    /// The next window of the row group. Nothing more is compared or
    /// fenced once a window holds a null in either column, a key that is
    /// not UTF-8 (`keys_not_utf8`, which `RunCheck` found), or an order
    /// value that is not, since a refusal or a fault then stands. Each
    /// order page starting in the window has its fence recorded into
    /// `fences` before the window's pairs are compared, against the pair
    /// before it.
    fn visit(
        &mut self,
        fence_pages: &mut PageStarts<'_>,
        window: &Window<'_>,
        keys_not_utf8: bool,
        fences: &mut GroupFencesBuilder,
    ) {
        let Window {
            rows, keys, orders, ..
        } = *window;
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
        if self.group.disorder.is_none() {
            self.record_fences(fence_pages, window, fences);
            self.check_pairs(keys, orders);
        }
    }

    /// The fence of each order page among `fence_pages` whose first row
    /// lies in `window`, after the pair before that row: the window's row
    /// before it, or for the window's first row the last pair compared, of
    /// the window, row group, or file before (`Pair::fence`). Every row
    /// counts, a row a delete names included, since fences bound positions.
    fn record_fences(
        &self,
        fence_pages: &mut PageStarts<'_>,
        window: &Window<'_>,
        fences: &mut GroupFencesBuilder,
    ) {
        if fences.overflowed() {
            return;
        }
        let pair = |at: usize| Pair {
            key: window.keys.part(at),
            order: window.orders.part(at),
        };
        for at in fence_pages.within(window.base, window.rows) {
            let previous = match at.checked_sub(1) {
                Some(before) => Some(pair(before)),
                None => self.last.as_ref().map(|(key, order)| Pair::of(key, order)),
            };
            if fences.push(Pair::fence(previous, pair(at))).is_err() {
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
                        _ => orders.cmp_at(at, orders, at - 1),
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

/// One window of a row group's rows as `PairCheck` visits it: `rows` rows
/// from row `base` on, their keys and order values, a value per row unless
/// a column holds a null.
#[derive(Clone, Copy)]
struct Window<'w> {
    base: usize,
    rows: usize,
    keys: &'w Keys,
    orders: &'w OrderRun,
}
