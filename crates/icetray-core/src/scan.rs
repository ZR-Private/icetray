//! What a Scan answers and what makes a Scan request malformed (RFD 025,
//! Wire, Limits): the rows of one key in the requested direction with the
//! `more` flag, the answers of a `MultiScan` from one snapshot, the checks a
//! request meets before any read, and the merge of the rows each file holding
//! the key gave into one answer. A key-value table never reaches this module.

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
/// most the limit, and whether live rows remain inside the bounds past the
/// last row returned. A key with no rows answers none, with `more` unset.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Scanned {
    /// The rows, in the requested direction.
    pub rows: Vec<ScanRow>,
    /// Whether live rows remain inside the bounds past the last row.
    pub more: bool,
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

/// What makes a Scan or `MultiScan` request malformed (RFD 025, Wire), each
/// found before any read. Display gives the fault for
/// `Error::ScanRequest`'s message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScanRequestFault {
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
        /// The variant the table's order type travels as.
        expected: OrderVariant,
    },
    /// `after` is at or above `before`, so no row lies between them.
    BoundsInverted,
    /// A `MultiScan`'s keys times its limit, a limit of 0 counting as the
    /// table's `max_scan_rows`, times one more than its requested columns,
    /// is above `server.max_multi_scan_values`.
    TooManyValues {
        /// The keys asked.
        keys: usize,
        /// The limit, resolved.
        limit: u32,
        /// The columns requested.
        columns: usize,
        /// `server.max_multi_scan_values`.
        max: usize,
    },
}

impl fmt::Display for ScanRequestFault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LimitAboveMax { limit, max } => {
                write!(f, "limit {limit} is above max_scan_rows {max}")
            }
            Self::BoundVariant { expected } => write!(
                f,
                "a bound is not an order value of the table's variant, {}",
                variant_name(*expected)
            ),
            Self::BoundsInverted => f.write_str("after is at or above before"),
            Self::TooManyValues {
                keys,
                limit,
                columns,
                max,
            } => write!(
                f,
                "{keys} keys at limit {limit} with {columns} columns and the order value ask for more than max_multi_scan_values {max}"
            ),
        }
    }
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

/// The limit `bounds` asks for resolved against `max_scan_rows`: 0 reads up
/// to `max_scan_rows`, and a limit above it is
/// `ScanRequestFault::LimitAboveMax`.
pub(crate) fn resolve_limit(
    bounds: &ScanBounds,
    max_scan_rows: u32,
) -> Result<u32, ScanRequestFault> {
    match bounds.limit {
        0 => Ok(max_scan_rows),
        limit if limit > max_scan_rows => Err(ScanRequestFault::LimitAboveMax {
            limit,
            max: max_scan_rows,
        }),
        limit => Ok(limit),
    }
}

/// `ScanRequestFault::BoundVariant` for a bound of another variant than
/// `expected`, and `ScanRequestFault::BoundsInverted` for an `after` at or
/// above `before`. A bound past the range of an `int` or `date` column is
/// no fault: it admits every row on its side.
pub(crate) fn check_bounds(
    bounds: &ScanBounds,
    expected: OrderVariant,
) -> Result<(), ScanRequestFault> {
    for bound in [&bounds.after, &bounds.before].into_iter().flatten() {
        if variant_of(bound) != expected {
            return Err(ScanRequestFault::BoundVariant { expected });
        }
    }
    if let (Some(after), Some(before)) = (&bounds.after, &bounds.before)
        && after >= before
    {
        return Err(ScanRequestFault::BoundsInverted);
    }
    Ok(())
}

/// `ScanRequestFault::TooManyValues` when `keys` keys at the resolved
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
            max,
        });
    }
    Ok(())
}

/// What one file holding the key answered: up to `limit` rows in the
/// Scan's direction, and the order value of its next live row inside the
/// bounds past them, if any.
pub(crate) struct FileAnswer {
    /// The rows taken, in the Scan's direction.
    pub(crate) rows: Vec<ScanRow>,
    /// The next live row's order value past the rows taken.
    pub(crate) next: Option<OrderValue>,
}

/// The rows every file holding the key answered merged into one Scan's
/// answer: ordered by order value in `direction`, cut to `limit`, with
/// `more` set when a row was cut or any file has a live row inside the
/// bounds past those it gave. Live pairs are unique across files (RFD 025,
/// Uniqueness), so no row is dropped as a repeat, and a file's next row
/// lies past every row it gave, so it lies past the last returned when
/// nothing was cut.
pub(crate) fn merge(files: Vec<FileAnswer>, direction: Direction, limit: u32) -> Scanned {
    let limit = usize::try_from(limit).unwrap_or(usize::MAX);
    let more_in_a_file = files.iter().any(|file| file.next.is_some());
    let mut rows: Vec<ScanRow> = files.into_iter().flat_map(|file| file.rows).collect();
    match direction {
        Direction::OldestFirst => rows.sort_by(|a, b| a.order.cmp(&b.order)),
        Direction::NewestFirst => rows.sort_by(|a, b| b.order.cmp(&a.order)),
    }
    let cut = rows.len() > limit;
    rows.truncate(limit);
    Scanned {
        rows,
        more: cut || more_in_a_file,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A table's `max_scan_rows` in these tests.
    const MAX: u32 = 10;

    /// A limit under `MAX`.
    const UNDER: u32 = 3;

    fn bounds(limit: u32, after: Option<OrderValue>, before: Option<OrderValue>) -> ScanBounds {
        ScanBounds {
            direction: Direction::NewestFirst,
            limit,
            after,
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
    fn test_resolve_limit_reads_zero_as_max_and_refuses_above_it() {
        assert_eq!(resolve_limit(&bounds(0, None, None), MAX), Ok(MAX));
        assert_eq!(resolve_limit(&bounds(UNDER, None, None), MAX), Ok(UNDER));
        assert_eq!(resolve_limit(&bounds(MAX, None, None), MAX), Ok(MAX));
        assert_eq!(
            resolve_limit(&bounds(MAX + 1, None, None), MAX),
            Err(ScanRequestFault::LimitAboveMax {
                limit: MAX + 1,
                max: MAX
            })
        );
    }

    #[test]
    fn test_check_bounds_refuses_another_variant_and_inverted_bounds() {
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
                expected: OrderVariant::Long
            })
        );
        assert_eq!(
            check_bounds(
                &bounds(0, Some(OrderValue::Bytes(vec![1])), None),
                OrderVariant::Str
            ),
            Err(ScanRequestFault::BoundVariant {
                expected: OrderVariant::Str
            })
        );
        for (after, before) in [(2, 2), (3, 2)] {
            assert_eq!(
                check_bounds(&bounds(0, long(after), long(before)), OrderVariant::Long),
                Err(ScanRequestFault::BoundsInverted),
                "{after} {before}"
            );
        }
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
                max
            })
        );
        assert_eq!(
            check_multi_scan_values(usize::MAX, u32::MAX, usize::MAX, max),
            Err(ScanRequestFault::TooManyValues {
                keys: usize::MAX,
                limit: u32::MAX,
                columns: usize::MAX,
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

    #[test]
    fn test_merge_orders_across_files_and_sets_more_from_a_cut_or_a_next_row() {
        let files = || {
            [FIRST_FILE, SECOND_FILE]
                .into_iter()
                .map(|orders| FileAnswer {
                    rows: orders.into_iter().map(row).collect(),
                    next: None,
                })
                .collect::<Vec<_>>()
        };
        let all = merge(files(), Direction::NewestFirst, MAX);
        assert_eq!(orders(&all), MERGED);
        assert!(!all.more);
        let cut = merge(files(), Direction::NewestFirst, UNDER);
        assert_eq!(orders(&cut), MERGED[..usize::try_from(UNDER).unwrap()]);
        assert!(cut.more, "a row was cut");
        let mut oldest = files();
        for file in &mut oldest {
            file.rows.reverse();
        }
        oldest[1].next = Some(OrderValue::Long(SECOND_NEXT));
        let with_next = merge(oldest, Direction::OldestFirst, MAX);
        let mut ascending = MERGED;
        ascending.reverse();
        assert_eq!(orders(&with_next), ascending);
        assert!(with_next.more, "a file holds a row past those it gave");
        let none = merge(Vec::new(), Direction::OldestFirst, MAX);
        assert!(none.rows.is_empty() && !none.more);
    }
}
