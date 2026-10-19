//! What a Scan answers and what makes a Scan request malformed (RFD 025,
//! Wire, Paging, Limits): the rows of one key in the requested direction
//! with the bound of the next page, the answers of a `MultiScan` from one
//! snapshot, the checks a request meets before any read, and the merge of
//! the rows each file holding the key gave into one answer. A key-value
//! table never reaches this module.

use std::fmt;

use bytes::Bytes;

use crate::order::{Direction, OrderValue, OrderVariant, ScanBounds};
use crate::snapshot::SnapshotId;

/// One row a Scan returns: its order value and one value per requested
/// column, in the order the Scan named them, each the column's bytes or
/// `None` for a null.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanRow {
    /// The row's order value.
    pub order: OrderValue,
    /// One value per requested column.
    pub values: Vec<Option<Bytes>>,
}

/// What a Scan of one key answers: the rows in the requested direction, at
/// most the limit, and the next page's bound, present exactly when live
/// rows remain inside the bounds past the last row returned. A key with no
/// rows answers none, with no `next`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Scanned {
    /// The rows, in the requested direction.
    pub rows: Vec<ScanRow>,
    /// The bound of the next page in the same direction, present exactly
    /// when live rows remain inside the bounds past the last row: reading
    /// oldest first, the order value of the first such row, the next
    /// page's `from`; reading newest first, the order value of the oldest
    /// row returned, the next page's `before`.
    pub next: Option<OrderValue>,
}

/// One Scan's answer and the snapshot it came from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanLookup {
    /// The answer.
    pub scanned: Scanned,
    /// The snapshot the answer was read from.
    pub snapshot_id: SnapshotId,
}

/// A `MultiScan`'s answers from one snapshot, one per key in the order the
/// keys were asked, a key listed twice answered at each position. Fails as a
/// whole if any key's read fails, as a `MultiGet` does (RFD 010).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MultiScanLookup {
    /// One answer per key, in order.
    pub results: Vec<Scanned>,
    /// The snapshot every answer was read from.
    pub snapshot_id: SnapshotId,
}

/// What of a refused response would have fit under the bound: the rows a
/// Scan's response could hold, or the leading keys' results a `MultiScan`'s
/// could, a key listed twice counting at each position.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fits {
    /// The longest prefix of a Scan's rows, in the order returned, whose
    /// response is within the bound.
    Rows(usize),
    /// The longest prefix of a `MultiScan`'s results whose response is
    /// within the bound.
    Keys(usize),
}

/// A Scan or `MultiScan` response above `server.max_scan_bytes` (RFD 025,
/// Limits): the size it would encode to, the bound, and what would fit.
/// Display gives the fault for `Error::ResponseTooLarge`'s message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Oversize {
    /// The response's encoded size in bytes.
    pub bytes: usize,
    /// `server.max_scan_bytes`.
    pub bound: usize,
    /// What of the response would fit.
    pub fits: Fits,
}

impl fmt::Display for Oversize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "the response is {} bytes, above server.max_scan_bytes {}; ",
            self.bytes, self.bound
        )?;
        match self.fits {
            Fits::Rows(rows) => write!(f, "the first {rows} rows would fit"),
            Fits::Keys(keys) => write!(f, "the results of the first {keys} keys would fit"),
        }
    }
}

/// Which of a Scan request's two bounds a fault names. Display gives the
/// field's name in the contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BoundSide {
    /// `from`: inclusive, at or below every row returned.
    From,
    /// `before`: exclusive, above every row returned.
    Before,
}

impl fmt::Display for BoundSide {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::From => "from",
            Self::Before => "before",
        })
    }
}

/// What makes a Scan or `MultiScan` request malformed (RFD 025, Wire), each
/// found before any read. Display gives the fault for
/// `Error::ScanRequest`'s message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScanRequestFault {
    /// The limit is 0: a Scan and each key of a `MultiScan` sets the most
    /// rows it reads, from 1 to the table's `max_scan_rows`.
    LimitZero {
        /// The table's `max_scan_rows`.
        max: u32,
    },
    /// The limit is above the table's `max_scan_rows`.
    LimitAboveMax {
        /// The limit asked.
        limit: u32,
        /// The table's `max_scan_rows`.
        max: u32,
    },
    /// A bound is an order value of another variant than the table's
    /// order type takes.
    BoundVariant {
        /// The bound that carries it.
        bound: BoundSide,
        /// The variant the table's order type travels as.
        expected: OrderVariant,
        /// The variant the bound carries.
        got: OrderVariant,
    },
    /// `from` is at or above `before`, an empty window: no order value
    /// is at or above the one and below the other.
    EmptyWindow,
    /// A `MultiScan`'s keys times its limit times one more than its
    /// requested columns is above `server.max_multi_scan_values`.
    TooManyValues {
        /// The keys asked.
        keys: usize,
        /// The limit, resolved.
        limit: u32,
        /// The columns requested.
        columns: usize,
        /// The values the request asks for: keys times limit times one
        /// more than the columns, saturating.
        values: usize,
        /// `server.max_multi_scan_values`.
        max: usize,
    },
}

impl fmt::Display for ScanRequestFault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LimitZero { max } => write!(
                f,
                "limit is 0: a Scan or MultiScan sets the most rows it reads per key, at most max_scan_rows {max}"
            ),
            Self::LimitAboveMax { limit, max } => {
                write!(f, "limit {limit} is above max_scan_rows {max}")
            }
            Self::BoundVariant {
                bound,
                expected,
                got,
            } => write!(
                f,
                "{bound} is an order value of variant {}, not the table's variant, {}",
                variant_name(*got),
                variant_name(*expected)
            ),
            Self::EmptyWindow => {
                f.write_str("from is at or above before, so the window holds no order value")
            }
            Self::TooManyValues {
                keys,
                limit,
                columns,
                values,
                max,
            } => write!(
                f,
                "{keys} {} at limit {limit}, each row with {columns} {} and its order value, ask for {values} values, above max_multi_scan_values {max}",
                plural(*keys, "key", "keys"),
                plural(*columns, "column", "columns"),
            ),
        }
    }
}

/// `one` for a count of 1, else `many`.
fn plural(count: usize, one: &'static str, many: &'static str) -> &'static str {
    if count == 1 { one } else { many }
}

/// The wire name of an order variant, as `OrderValue`'s oneof names it.
fn variant_name(variant: OrderVariant) -> &'static str {
    match variant {
        OrderVariant::Long => "long",
        OrderVariant::Str => "str",
        OrderVariant::Bin => "bin",
    }
}

/// The variant `value` travels as.
fn variant_of(value: &OrderValue) -> OrderVariant {
    match value {
        OrderValue::Long(_) => OrderVariant::Long,
        OrderValue::Str(_) => OrderVariant::Str,
        OrderValue::Bytes(_) => OrderVariant::Bin,
    }
}

/// The limit `bounds` asks for, checked against `max_scan_rows`: a limit of
/// 0 is `ScanRequestFault::LimitZero`, since a Scan and each key of a
/// `MultiScan` sets the rows it reads, and a limit above `max_scan_rows` is
/// `ScanRequestFault::LimitAboveMax`.
pub(crate) fn check_limit(
    bounds: &ScanBounds,
    max_scan_rows: u32,
) -> Result<u32, ScanRequestFault> {
    match bounds.limit {
        0 => Err(ScanRequestFault::LimitZero { max: max_scan_rows }),
        limit if limit > max_scan_rows => Err(ScanRequestFault::LimitAboveMax {
            limit,
            max: max_scan_rows,
        }),
        limit => Ok(limit),
    }
}

/// `ScanRequestFault::BoundVariant` for a bound of another variant than
/// `expected`, and `ScanRequestFault::EmptyWindow` for a `from` at or
/// above `before`. A bound past the range of an `int` or `date` column is
/// no fault: it admits every row on its side.
pub(crate) fn check_bounds(
    bounds: &ScanBounds,
    expected: OrderVariant,
) -> Result<(), ScanRequestFault> {
    for (side, bound) in [
        (BoundSide::From, &bounds.from),
        (BoundSide::Before, &bounds.before),
    ] {
        if let Some(bound) = bound
            && variant_of(bound) != expected
        {
            return Err(ScanRequestFault::BoundVariant {
                bound: side,
                expected,
                got: variant_of(bound),
            });
        }
    }
    if let (Some(from), Some(before)) = (&bounds.from, &bounds.before)
        && from >= before
    {
        return Err(ScanRequestFault::EmptyWindow);
    }
    Ok(())
}

/// `ScanRequestFault::TooManyValues` when `keys` keys at the checked
/// `limit` naming `columns` columns ask for more than `max` values, each row
/// counting its order value and one value per column (RFD 025, Limits).
pub(crate) fn check_multi_scan_values(
    keys: usize,
    limit: u32,
    columns: usize,
    max: usize,
) -> Result<(), ScanRequestFault> {
    let rows = usize::try_from(limit).unwrap_or(usize::MAX);
    let values = keys
        .saturating_mul(rows)
        .saturating_mul(columns.saturating_add(1));
    if values > max {
        return Err(ScanRequestFault::TooManyValues {
            keys,
            limit,
            columns,
            values,
            max,
        });
    }
    Ok(())
}

/// What one file holding the key answered: up to `limit` rows in the
/// Scan's direction, and what lies past them inside the bounds.
pub(crate) struct FileAnswer {
    /// The rows taken, in the Scan's direction.
    pub(crate) rows: Vec<ScanRow>,
    /// The file's next live row inside the bounds past the rows taken.
    pub(crate) past: Past,
}

/// What lies past the rows one file gave, inside the bounds, in the Scan's
/// direction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Past {
    /// No live row.
    Nothing,
    /// A live row, read newest first. The next page's bound is the oldest
    /// row returned, so the row's order value is not needed.
    Row,
    /// A live row at this order value, read oldest first: the next page's
    /// `from` unless another file's row precedes it.
    At(OrderValue),
}

/// The rows every file holding the key answered merged into one Scan's
/// answer: ordered by order value in `direction`, cut to `limit`, with
/// `next` set when a row was cut or any file has a live row inside the
/// bounds past those it gave (`Scanned::next`). Live pairs are unique
/// across files (RFD 025, Uniqueness), so no row is dropped as a repeat. A
/// file has a row past those it gave only when it gave `limit` rows, so
/// that row lies past the last returned, and every live row past the last
/// returned is a row cut or at or past some file's next row: oldest first,
/// the least of the first row cut and the files' next rows is the first
/// live row past the page. One file's rows, as a compacted table's key
/// answers, are already in `direction` and at most `limit`, so they are
/// the answer as given.
pub(crate) fn merge(mut files: Vec<FileAnswer>, direction: Direction, limit: u32) -> Scanned {
    if files.len() == 1
        && let Some(file) = files.pop()
    {
        let next = match file.past {
            Past::Nothing => None,
            Past::Row => file.rows.last().map(|row| row.order.clone()),
            Past::At(next) => Some(next),
        };
        return Scanned {
            rows: file.rows,
            next,
        };
    }
    let limit = usize::try_from(limit).unwrap_or(usize::MAX);
    let mut rows: Vec<ScanRow> = Vec::with_capacity(files.iter().map(|file| file.rows.len()).sum());
    let mut past_a_file = false;
    let mut least_next: Option<OrderValue> = None;
    for file in files {
        rows.extend(file.rows);
        match file.past {
            Past::Nothing => {}
            Past::Row => past_a_file = true,
            Past::At(next) => {
                past_a_file = true;
                least_next = Some(match least_next {
                    Some(least) => least.min(next),
                    None => next,
                });
            }
        }
    }
    match direction {
        Direction::OldestFirst => rows.sort_by(|a, b| a.order.cmp(&b.order)),
        Direction::NewestFirst => rows.sort_by(|a, b| b.order.cmp(&a.order)),
    }
    let first_cut = (rows.len() > limit).then(|| rows.swap_remove(limit));
    rows.truncate(limit);
    let next = match direction {
        Direction::OldestFirst => match (first_cut, least_next) {
            (Some(cut), Some(least)) => Some(cut.order.min(least)),
            (Some(cut), None) => Some(cut.order),
            (None, least) => least,
        },
        Direction::NewestFirst if first_cut.is_some() || past_a_file => {
            rows.last().map(|row| row.order.clone())
        }
        Direction::NewestFirst => None,
    };
    Scanned { rows, next }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A table's `max_scan_rows` in these tests.
    const MAX: u32 = 10;

    /// A limit under `MAX`.
    const UNDER: u32 = 3;

    fn bounds(limit: u32, from: Option<OrderValue>, before: Option<OrderValue>) -> ScanBounds {
        ScanBounds {
            direction: Direction::NewestFirst,
            limit,
            from,
            before,
        }
    }

    fn row(order: i64) -> ScanRow {
        ScanRow {
            order: OrderValue::Long(order),
            values: Vec::new(),
        }
    }

    fn orders(scanned: &Scanned) -> Vec<i64> {
        scanned
            .rows
            .iter()
            .map(|row| match row.order {
                OrderValue::Long(order) => order,
                OrderValue::Str(_) | OrderValue::Bytes(_) => panic!("a long order value"),
            })
            .collect()
    }

    #[test]
    fn test_check_limit_refuses_zero_and_above_max() {
        assert_eq!(
            check_limit(&bounds(0, None, None), MAX),
            Err(ScanRequestFault::LimitZero { max: MAX })
        );
        assert_eq!(check_limit(&bounds(1, None, None), MAX), Ok(1));
        assert_eq!(check_limit(&bounds(UNDER, None, None), MAX), Ok(UNDER));
        assert_eq!(check_limit(&bounds(MAX, None, None), MAX), Ok(MAX));
        assert_eq!(
            check_limit(&bounds(MAX + 1, None, None), MAX),
            Err(ScanRequestFault::LimitAboveMax {
                limit: MAX + 1,
                max: MAX
            })
        );
    }

    #[test]
    fn test_check_bounds_refuses_another_variant_and_an_empty_window() {
        let long = |value| Some(OrderValue::Long(value));
        assert_eq!(
            check_bounds(&bounds(0, long(1), long(2)), OrderVariant::Long),
            Ok(())
        );
        assert_eq!(
            check_bounds(
                &bounds(0, None, Some(OrderValue::Str("a".to_string()))),
                OrderVariant::Long
            ),
            Err(ScanRequestFault::BoundVariant {
                bound: BoundSide::Before,
                expected: OrderVariant::Long,
                got: OrderVariant::Str,
            })
        );
        assert_eq!(
            check_bounds(
                &bounds(0, Some(OrderValue::Bytes(vec![1])), None),
                OrderVariant::Str
            ),
            Err(ScanRequestFault::BoundVariant {
                bound: BoundSide::From,
                expected: OrderVariant::Str,
                got: OrderVariant::Bin,
            })
        );
        for (from, before) in [(2, 2), (3, 2), (i64::MAX, i64::MAX), (i64::MAX, i64::MIN)] {
            assert_eq!(
                check_bounds(&bounds(0, long(from), long(before)), OrderVariant::Long),
                Err(ScanRequestFault::EmptyWindow),
                "{from} {before}"
            );
        }
        let text = |value: &str| Some(OrderValue::Str(value.to_string()));
        assert_eq!(
            check_bounds(&bounds(0, text("a"), text("a")), OrderVariant::Str),
            Err(ScanRequestFault::EmptyWindow),
            "a window of one string holds nothing: before excludes what from includes"
        );
        assert_eq!(
            check_bounds(&bounds(0, text("a"), text("a\0")), OrderVariant::Str),
            Ok(()),
            "the least string above \"a\" admits \"a\" alone"
        );
        assert_eq!(
            check_bounds(
                &bounds(0, long(i64::MIN), long(i64::MAX)),
                OrderVariant::Long
            ),
            Ok(())
        );
    }

    #[test]
    fn test_check_multi_scan_values_counts_the_order_value_beside_each_column() {
        let keys = 2;
        let columns = 1;
        let max = 2 * usize::try_from(MAX).unwrap() * (columns + 1);
        assert_eq!(check_multi_scan_values(keys, MAX, columns, max), Ok(()));
        assert_eq!(
            check_multi_scan_values(keys + 1, MAX, columns, max),
            Err(ScanRequestFault::TooManyValues {
                keys: keys + 1,
                limit: MAX,
                columns,
                values: (keys + 1) * usize::try_from(MAX).unwrap() * (columns + 1),
                max
            })
        );
        assert_eq!(
            check_multi_scan_values(usize::MAX, u32::MAX, usize::MAX, max),
            Err(ScanRequestFault::TooManyValues {
                keys: usize::MAX,
                limit: u32::MAX,
                columns: usize::MAX,
                values: usize::MAX,
                max
            }),
            "the product saturates rather than wrapping"
        );
    }

    /// The order values two files hold of one key, newest first, which
    /// interleave.
    const FIRST_FILE: [i64; 2] = [9, 5];
    const SECOND_FILE: [i64; 2] = [8, 6];

    /// Every order value of `FIRST_FILE` and `SECOND_FILE`, newest first.
    const MERGED: [i64; 4] = [9, 8, 6, 5];

    /// A row of the second file past those it gave oldest first.
    const SECOND_NEXT: i64 = 12;

    /// `FIRST_FILE` and `SECOND_FILE` as answers read in `direction`,
    /// nothing past either.
    fn two_files(direction: Direction) -> Vec<FileAnswer> {
        [FIRST_FILE, SECOND_FILE]
            .into_iter()
            .map(|orders| {
                let mut rows: Vec<ScanRow> = orders.into_iter().map(row).collect();
                if direction == Direction::OldestFirst {
                    rows.reverse();
                }
                FileAnswer {
                    rows,
                    past: Past::Nothing,
                }
            })
            .collect()
    }

    #[test]
    fn test_merge_orders_across_files_and_sets_next_from_a_cut_or_a_files_next_row() {
        let all = merge(
            two_files(Direction::NewestFirst),
            Direction::NewestFirst,
            MAX,
        );
        assert_eq!(orders(&all), MERGED);
        assert_eq!(all.next, None);
        let under = usize::try_from(UNDER).unwrap();
        let cut = merge(
            two_files(Direction::NewestFirst),
            Direction::NewestFirst,
            UNDER,
        );
        assert_eq!(orders(&cut), MERGED[..under]);
        assert_eq!(
            cut.next,
            Some(OrderValue::Long(MERGED[under - 1])),
            "newest first, a cut sets next to the oldest row returned"
        );
        let mut ascending = MERGED;
        ascending.reverse();
        let oldest_cut = merge(
            two_files(Direction::OldestFirst),
            Direction::OldestFirst,
            UNDER,
        );
        assert_eq!(orders(&oldest_cut), ascending[..under]);
        assert_eq!(
            oldest_cut.next,
            Some(OrderValue::Long(ascending[under])),
            "oldest first, a cut sets next to the first row cut"
        );
        let mut with_next = two_files(Direction::OldestFirst);
        with_next[1].past = Past::At(OrderValue::Long(SECOND_NEXT));
        let with_next = merge(with_next, Direction::OldestFirst, MAX);
        assert_eq!(orders(&with_next), ascending);
        assert_eq!(
            with_next.next,
            Some(OrderValue::Long(SECOND_NEXT)),
            "a file holds a row past those it gave"
        );
        let mut newest_past = two_files(Direction::NewestFirst);
        newest_past[0].past = Past::Row;
        let newest_past = merge(newest_past, Direction::NewestFirst, MAX);
        assert_eq!(orders(&newest_past), MERGED);
        assert_eq!(newest_past.next, Some(OrderValue::Long(MERGED[3])));
        let none = merge(Vec::new(), Direction::OldestFirst, MAX);
        assert!(none.rows.is_empty() && none.next.is_none());
    }

    /// Oldest first, the next page starts at the least row past the page
    /// in any file: here a file's next row, below the first row cut.
    #[test]
    fn test_merge_oldest_first_next_is_the_least_row_past_the_page_in_any_file() {
        let limit = 3;
        let file = |orders: [i64; 3], next: i64| FileAnswer {
            rows: orders.into_iter().map(row).collect(),
            past: Past::At(OrderValue::Long(next)),
        };
        let first_next = 4;
        let merged = merge(
            vec![file([1, 2, 3], first_next), file([5, 6, 7], 8)],
            Direction::OldestFirst,
            limit,
        );
        assert_eq!(orders(&merged), [1, 2, 3]);
        assert_eq!(merged.next, Some(OrderValue::Long(first_next)));
        let first_cut = 4;
        let merged = merge(
            vec![file([1, 5, 9], 10), file([2, 3, first_cut], 6)],
            Direction::OldestFirst,
            limit,
        );
        assert_eq!(orders(&merged), [1, 2, 3]);
        assert_eq!(merged.next, Some(OrderValue::Long(first_cut)));
    }

    #[test]
    fn test_merge_answers_one_files_rows_as_given() {
        let file = |past| FileAnswer {
            rows: FIRST_FILE.into_iter().map(row).collect(),
            past,
        };
        let alone = merge(vec![file(Past::Nothing)], Direction::NewestFirst, MAX);
        assert_eq!(orders(&alone), FIRST_FILE);
        assert_eq!(alone.next, None);
        let newest = merge(vec![file(Past::Row)], Direction::NewestFirst, MAX);
        assert_eq!(orders(&newest), FIRST_FILE);
        assert_eq!(
            newest.next,
            Some(OrderValue::Long(FIRST_FILE[1])),
            "newest first, the next page ends below the oldest row returned"
        );
        let oldest = merge(
            vec![file(Past::At(OrderValue::Long(SECOND_NEXT)))],
            Direction::OldestFirst,
            MAX,
        );
        assert_eq!(
            oldest.next,
            Some(OrderValue::Long(SECOND_NEXT)),
            "oldest first, the next page starts at the file's next row"
        );
    }
}
