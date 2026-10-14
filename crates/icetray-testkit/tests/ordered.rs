//! Scan and `MultiScan` of ordered tables (RFD 025) in every residency tier:
//! the answers of randomly drawn tables, changed by appends, deletes, and
//! updates, against the naive-sort model (`Fixture::expected_scan`); paging
//! by order value; `more` over deleted rows; exclusive bounds at the
//! extremes of the order types; the GETs of a cold Scan; the files a Scan
//! reads; the calls each table kind refuses; and the cache a Scan's pages
//! stay in.

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use iceberg::Catalog;
use iceberg::spec::{FormatVersion, Literal, PrimitiveType};
use icetray_core::approx::count;
use icetray_core::config::{Residency, TableConfig};
use icetray_core::{
    ColumnRead, Direction, Error, Key, KeyType, OrderValue, ScanBounds, ScanRow, Scanned,
    TableKind, bucket,
};
use icetray_testkit::fixture::{
    HandSnapshot, ONE_BUCKET, SECOND_VALUE_COLUMN, SchemaChange, StatedDefault, TWO_BUCKETS,
    local_catalog,
};
use icetray_testkit::layout::VALUE_COLUMN;
use icetray_testkit::verify::{self, Served};
use icetray_testkit::{
    DeleteForm, Dictionary, Fixture, FixtureSpec, Layout, Nulls, OrderKind, Ordered, Result,
    Statistics, TestScan, order_value, write_fixture,
};
use rand::rngs::StdRng;
use rand::{RngExt as _, SeedableRng as _};
use tempfile::TempDir;

/// The seed every random draw of this file starts from, so a failure
/// replays.
const SCAN_SEED: u64 = 0x5ca9_0025;

/// Random tables the model test draws.
const INSTANCES: usize = 40;

/// Scans the model test draws per table, each run in every tier.
const KEYS_PER_INSTANCE: usize = 12;

/// Random tables the `MultiScan` test draws.
const MULTI_SCAN_INSTANCES: usize = 6;

/// Keys one drawn `MultiScan` asks for, repeats included.
const KEYS_PER_MULTI_SCAN: Range<usize> = 1..8;

/// Every residency tier a table can be served in.
const TIERS: [Residency; 3] = [Residency::Full, Residency::Disk, Residency::Bloom];

/// Ids a drawn table writes, and the ids past them an append may add.
const DRAWN_IDS: Range<u64> = 0..12;
const NEW_IDS: u64 = 2;

/// The raw order value a key's first row takes, far enough above zero that
/// a bound below it is still encodable in every kind.
const ORDER_START: i64 = 1_000;

/// The steps between a key's rows a draw takes: at least two, so an append
/// finds raw values between two rows.
const ORDER_STEPS: Range<i64> = 2..5;

/// The key types a draw takes.
const KEY_TYPES: [KeyType; 4] = [
    KeyType::Long,
    KeyType::String,
    KeyType::Binary,
    KeyType::Uuid,
];

/// A fixed width that holds the drawn raw values, narrower than a long.
const NARROW_FIXED: u32 = 4;

/// A fixed width wider than a long: its values share a prefix of zeros.
const WIDE_FIXED: u32 = 12;

/// The order kinds a draw takes. The string and byte kinds' values share
/// long prefixes, a string's zero-padded digits and the bytes' leading
/// zeros, so a truncated column index widens their pages.
const ORDER_KINDS: [OrderKind; 9] = [
    OrderKind::Long,
    OrderKind::Int,
    OrderKind::Date,
    OrderKind::Timestamp,
    OrderKind::TimestampTz,
    OrderKind::Str,
    OrderKind::Bin,
    OrderKind::Fixed(NARROW_FIXED),
    OrderKind::Fixed(WIDE_FIXED),
];

/// The column index truncation a draw may give a table of a string or
/// byte order kind: shorter than the shared prefix, so neighboring pages'
/// entries overlap.
const TRUNCATE_INDEX: usize = 6;

/// The row counts per key a draw takes.
const ROWS_PER_KEY: [fn(u64) -> u64; 4] = [one_row, three_rows, up_to_five, up_to_nine];

fn one_row(_: u64) -> u64 {
    1
}

fn three_rows(_: u64) -> u64 {
    3
}

/// Zero to four rows, so some keys have none.
fn up_to_five(id: u64) -> u64 {
    id % 5
}

/// One to nine rows.
fn up_to_nine(id: u64) -> u64 {
    (id * 7) % 9 + 1
}

/// Rows per page and per row group a draw takes; `None` row groups leave
/// one per file.
const PAGE_ROWS: Range<usize> = 1..5;
const ROW_GROUP_ROWS: Range<usize> = 2..9;

/// Bucket counts a draw takes; zero writes an unpartitioned table.
const BUCKETS: [u32; 4] = [0, ONE_BUCKET, TWO_BUCKETS, 4];

/// The `max_scan_rows` a draw may give a table, small enough that a limit
/// of 0 cuts a run.
const SMALL_MAX_SCAN_ROWS: Range<u32> = 2..6;

/// Bytes per value of a drawn table: small, so the test stays quick.
const DRAWN_VALUE_BYTES: usize = 24;

/// Every third id's values are null where a draw asks for nulls.
const NULL_EVERY: u64 = 3;

/// The most appends, deletes, and updates a draw makes, and the most rows
/// each touches.
const MOST_APPENDS: usize = 2;
const MOST_DELETES: usize = 2;
const MOST_UPDATES: usize = 1;
const MOST_ROWS_PER_CHANGE: usize = 4;

/// The delete forms a draw takes.
const FORMS: [DeleteForm; 3] = [
    DeleteForm::Vectors,
    DeleteForm::ScopedPositions,
    DeleteForm::Positions,
];

/// A drawn table: the fixture, its catalog's directory, the order kind,
/// and the `max_scan_rows` it is served under.
struct Drawn {
    fixture: Fixture,
    _dir: TempDir,
    kind: OrderKind,
    max_scan_rows: u32,
}

/// One table served in every tier, each over storage and caches of its
/// own, the Disk tier's local directory kept alive beside it.
struct Tiers {
    served: Vec<(Residency, Served)>,
    _dirs: Vec<TempDir>,
}

impl Tiers {
    /// Every tier's serving.
    fn all(&self) -> impl Iterator<Item = &(Residency, Served)> {
        self.served.iter()
    }
}

/// `config(residency)` served in every tier.
async fn serve_in_every_tier(config: impl Fn(Residency) -> TableConfig) -> Result<Tiers> {
    let mut served = Vec::new();
    let mut dirs = Vec::new();
    for residency in TIERS {
        let dir = tempfile::tempdir()?;
        served.push((
            residency,
            verify::serve_in(config(residency), dir.path()).await?,
        ));
        dirs.push(dir);
    }
    Ok(Tiers {
        served,
        _dirs: dirs,
    })
}

/// `drawn` served in every tier under its `max_scan_rows`.
async fn serve_drawn(drawn: &Drawn) -> Result<Tiers> {
    serve_in_every_tier(|residency| TableConfig {
        max_scan_rows: Some(drawn.max_scan_rows),
        ..drawn.fixture.config(residency, false)
    })
    .await
}

/// An element of `items` drawn by `rng`.
fn pick<T: Copy>(rng: &mut StdRng, items: &[T]) -> T {
    items[rng.random_range(0..items.len())]
}

/// A random ordered specification named `name`: key and order kinds, rows
/// per key, page and row-group rows, buckets, a truncated column index for
/// a string or byte order kind, an order column without a column index or
/// dictionary-encoded, a dictionary-encoded value column, a second value
/// column, nulls, and a small `max_scan_rows`, each drawn by `rng`.
fn random_ordered_spec(rng: &mut StdRng, name: &str) -> (FixtureSpec, u32) {
    let kind = pick(rng, &ORDER_KINDS);
    let bytewise = matches!(kind, OrderKind::Str | OrderKind::Bin | OrderKind::Fixed(_));
    let layout = Layout {
        page_rows: rng.random_range(PAGE_ROWS),
        row_group_rows: rng
            .random_bool(0.5)
            .then(|| rng.random_range(ROW_GROUP_ROWS)),
        column_index_truncate: (bytewise && rng.random_bool(0.5)).then_some(TRUNCATE_INDEX),
        order_statistics: if rng.random_bool(0.15) {
            Statistics::None
        } else {
            Statistics::Full
        },
        order_dictionary: rng.random_bool(0.15),
        dictionary: if rng.random_bool(0.15) {
            Dictionary::Value
        } else {
            Dictionary::Off
        },
        ..Layout::contract()
    };
    let spec = FixtureSpec {
        ids: DRAWN_IDS,
        buckets: pick(rng, &BUCKETS),
        value_bytes: DRAWN_VALUE_BYTES,
        nulls: if rng.random_bool(0.3) {
            Nulls::EveryNth(NULL_EVERY)
        } else {
            Nulls::None
        },
        second_value: rng.random_bool(0.5),
        layout,
        ..FixtureSpec::ordered(
            name,
            pick(rng, &KEY_TYPES),
            Ordered {
                kind,
                rows_per_key: pick(rng, &ROWS_PER_KEY),
                start: ORDER_START,
                step: rng.random_range(ORDER_STEPS),
            },
        )
    };
    let max_scan_rows = if rng.random_bool(0.5) {
        rng.random_range(SMALL_MAX_SCAN_ROWS)
    } else {
        icetray_core::DEFAULT_MAX_SCAN_ROWS
    };
    (spec, max_scan_rows)
}

/// The raw order values of `id`'s live rows, ascending.
fn live_orders(fixture: &Fixture, id: u64) -> Vec<i64> {
    let all = TestScan {
        direction: Direction::OldestFirst,
        max_scan_rows: u32::MAX,
        ..TestScan::default()
    };
    fixture
        .expected_scan(&fixture.key(id), &[], &all)
        .0
        .into_iter()
        .map(|(order, _)| order)
        .collect()
}

/// Every live (id, raw order value) pair of `fixture`.
fn live_pairs(fixture: &Fixture, ids: Range<u64>) -> Vec<(u64, i64)> {
    ids.flat_map(|id| {
        live_orders(fixture, id)
            .into_iter()
            .map(move |order| (id, order))
    })
    .collect()
}

/// Up to `most` distinct live pairs of `fixture`, drawn by `rng`.
fn some_live(fixture: &Fixture, rng: &mut StdRng, most: usize) -> Vec<(u64, i64)> {
    let mut live = live_pairs(fixture, DRAWN_IDS.start..DRAWN_IDS.end + NEW_IDS);
    let mut drawn = Vec::new();
    for _ in 0..rng.random_range(1..=most) {
        if live.is_empty() {
            break;
        }
        drawn.push(live.swap_remove(rng.random_range(0..live.len())));
    }
    drawn.sort_unstable();
    drawn
}

/// Rows to append, none a live pair: per row an id, new ones included, and
/// a raw order value past its last row, before its first, or between two,
/// ascending by id and then order value.
fn new_rows(fixture: &Fixture, rng: &mut StdRng, step: i64) -> Vec<(u64, i64)> {
    let mut rows: Vec<(u64, i64)> = Vec::new();
    for _ in 0..rng.random_range(1..=MOST_ROWS_PER_CHANGE) {
        let id = rng.random_range(DRAWN_IDS.start..DRAWN_IDS.end + NEW_IDS);
        let live = live_orders(fixture, id);
        let order = match (live.first(), live.last(), rng.random_range(0..3)) {
            (Some(&first), _, 0) => first - 1,
            (_, Some(&last), 1) => last + step * rng.random_range(1..3),
            (Some(_), Some(_), _) => live[rng.random_range(0..live.len())] + 1,
            (None, _, _) | (_, None, _) => ORDER_START,
        };
        if !live.contains(&order) && !rows.contains(&(id, order)) {
            rows.push((id, order));
        }
    }
    rows.sort_unstable();
    rows
}

/// A random ordered table, named after `instance`, written and changed:
/// appends, a compaction now and then, deletes, and updates, each drawn by
/// `rng`.
async fn draw_table(rng: &mut StdRng, instance: usize) -> Result<Drawn> {
    let (spec, max_scan_rows) = random_ordered_spec(rng, &format!("drawn_{instance}"));
    let ordered = spec.ordered.expect("an ordered specification");
    let (dir, catalog) = local_catalog().await?;
    let mut fixture =
        write_fixture(catalog.as_ref(), icetray_testkit::fixture::NAMESPACE, spec).await?;
    for at in 0..rng.random_range(0..=MOST_APPENDS) {
        let rows = new_rows(&fixture, rng, ordered.step);
        fixture
            .append_ordered(catalog.as_ref(), &rows, &format!("append_{at}"))
            .await?;
    }
    if rng.random_bool(0.2) {
        fixture.compact("compacted").await?;
    }
    // One form per table: a position delete of a file that a deletion
    // vector already covers is masked by the vector (RFD 021), which no
    // writer leaves.
    let form = pick(rng, &FORMS);
    for at in 0..rng.random_range(0..=MOST_DELETES) {
        let rows = some_live(&fixture, rng, MOST_ROWS_PER_CHANGE);
        if !rows.is_empty() {
            fixture
                .delete_rows(&rows, form, &format!("delete_{at}"))
                .await?;
        }
    }
    for at in 0..rng.random_range(0..=MOST_UPDATES) {
        let rows = some_live(&fixture, rng, MOST_ROWS_PER_CHANGE);
        if !rows.is_empty() {
            fixture
                .update_rows(&rows, form, &format!("update_{at}"))
                .await?;
        }
    }
    Ok(Drawn {
        fixture,
        _dir: dir,
        kind: ordered.kind,
        max_scan_rows,
    })
}

/// The value columns `fixture` writes.
fn written_columns(fixture: &Fixture) -> Vec<&'static str> {
    if fixture.spec().second_value {
        vec![VALUE_COLUMN, SECOND_VALUE_COLUMN]
    } else {
        vec![VALUE_COLUMN]
    }
}

/// A raw bound for a key whose live rows are `live`: at a row, beside one,
/// or past every row on either side, never negative.
fn random_bound(rng: &mut StdRng, live: &[i64]) -> i64 {
    let (first, last) = match (live.first(), live.last()) {
        (Some(&first), Some(&last)) => (first, last),
        (None, _) | (_, None) => (ORDER_START, ORDER_START),
    };
    match rng.random_range(0..5) {
        0 => first - 1,
        1 => last + 1,
        2 | 3 if !live.is_empty() => live[rng.random_range(0..live.len())],
        _ => rng.random_range(first - 1..=last + 1),
    }
}

/// A random Scan of `drawn`: a key it holds or one it does not, the
/// columns in a random order, a direction, a limit up to its
/// `max_scan_rows` drawn around the key's live rows, and bounds at, beside,
/// and past them.
fn random_scan(drawn: &Drawn, rng: &mut StdRng) -> (Key, Vec<&'static str>, TestScan) {
    let fixture = &drawn.fixture;
    let ids: Vec<u64> = fixture.ids().collect();
    let id = if ids.is_empty() || rng.random_bool(0.1) {
        DRAWN_IDS.end + NEW_IDS
    } else {
        pick(rng, &ids)
    };
    let key = if rng.random_bool(0.05) {
        fixture.absent_near(id)
    } else {
        fixture.key(id)
    };
    let mut columns = written_columns(fixture);
    if rng.random_bool(0.5) {
        columns.reverse();
    }
    columns.truncate(rng.random_range(0..=columns.len()));
    let live = live_orders(fixture, id);
    let held = u32::try_from(live.len()).expect("a key's rows fit u32");
    let limit = match rng.random_range(0..5) {
        0 => 0,
        1 => held,
        2 => held.saturating_sub(1),
        3 => 1,
        _ => rng.random_range(0..=drawn.max_scan_rows),
    }
    .min(drawn.max_scan_rows);
    let mut after = rng.random_bool(0.4).then(|| random_bound(rng, &live));
    let before = rng.random_bool(0.4).then(|| random_bound(rng, &live));
    if let (Some(low), Some(high)) = (after, before)
        && low >= high
    {
        after = None;
    }
    let scan = TestScan {
        direction: if rng.random_bool(0.5) {
            Direction::NewestFirst
        } else {
            Direction::OldestFirst
        },
        limit,
        after,
        before,
        max_scan_rows: drawn.max_scan_rows,
    };
    (key, columns, scan)
}

/// `scan` as the core's bounds over order values of `kind`.
fn bounds_of(kind: OrderKind, scan: &TestScan) -> ScanBounds {
    ScanBounds {
        direction: scan.direction,
        limit: scan.limit,
        after: scan.after.map(|raw| order_value(kind, raw)),
        before: scan.before.map(|raw| order_value(kind, raw)),
    }
}

/// What a Scan of `key` reading `columns` under `scan` answers on
/// `fixture`, by the model, as the core's rows.
fn expected(
    fixture: &Fixture,
    kind: OrderKind,
    key: &Key,
    columns: &[&str],
    scan: &TestScan,
) -> Scanned {
    let (rows, more) = fixture.expected_scan(key, columns, scan);
    Scanned {
        rows: rows
            .into_iter()
            .map(|(order, values)| ScanRow {
                order: order_value(kind, order),
                values: values
                    .into_iter()
                    .map(|value| value.map(Bytes::from))
                    .collect(),
            })
            .collect(),
        more,
    }
}

/// `columns` as whole-value reads.
fn reads<'a>(columns: &[&'a str]) -> Vec<ColumnRead<'a>> {
    columns
        .iter()
        .map(|&name| ColumnRead::whole(name))
        .collect()
}

/// Every drawn table, served in every tier, answers every drawn Scan as
/// the naive sort of its live rows does: the rows, their values, and
/// `more`.
#[tokio::test]
async fn test_scan_matches_the_model_in_every_tier() -> Result<()> {
    let mut rng = StdRng::seed_from_u64(SCAN_SEED);
    let mut rows_compared = 0;
    let mut more_set = 0;
    let mut kinds = Vec::new();
    let (mut truncated, mut unindexed) = (false, false);
    for instance in 0..INSTANCES {
        let drawn = draw_table(&mut rng, instance).await?;
        if !kinds.contains(&drawn.kind) {
            kinds.push(drawn.kind);
        }
        let layout = drawn.fixture.spec().layout;
        truncated |= layout.column_index_truncate.is_some();
        unindexed |= layout.order_statistics == Statistics::None;
        let tiers = serve_drawn(&drawn).await?;
        for _ in 0..KEYS_PER_INSTANCE {
            let (key, columns, scan) = random_scan(&drawn, &mut rng);
            let want = expected(&drawn.fixture, drawn.kind, &key, &columns, &scan);
            let bounds = bounds_of(drawn.kind, &scan);
            for (residency, served) in tiers.all() {
                let got = served.table.scan(&key, &reads(&columns), &bounds).await?;
                assert_eq!(
                    got.scanned,
                    want,
                    "instance {instance} {residency} {key} {columns:?} {scan:?} of {:?}",
                    drawn.fixture.spec()
                );
            }
            rows_compared += want.rows.len();
            more_set += usize::from(want.more);
        }
    }
    assert!(rows_compared > 0, "the scans compared rows");
    assert!(more_set > 0, "some scans left rows past the limit");
    assert_eq!(
        kinds.len(),
        ORDER_KINDS.len(),
        "every order kind: {kinds:?}"
    );
    assert!(truncated, "some table's column index is truncated");
    assert!(unindexed, "some table's order column has no column index");
    Ok(())
}

/// A `MultiScan` of drawn keys, repeats included, answers each position as
/// a Scan of its key does, from the one snapshot the table serves.
#[tokio::test]
async fn test_multi_scan_matches_scan_for_each_key_from_one_snapshot() -> Result<()> {
    let mut rng = StdRng::seed_from_u64(SCAN_SEED ^ 1);
    let mut keys_answered = 0;
    for instance in 0..MULTI_SCAN_INSTANCES {
        let mut drawn = draw_table(&mut rng, instance).await?;
        drawn.max_scan_rows = drawn.max_scan_rows.min(SMALL_MAX_SCAN_ROWS.end);
        let tiers = serve_drawn(&drawn).await?;
        let (first_key, columns, scan) = random_scan(&drawn, &mut rng);
        let mut keys = vec![first_key];
        for _ in 1..rng.random_range(KEYS_PER_MULTI_SCAN) {
            keys.push(random_scan(&drawn, &mut rng).0);
        }
        let repeated = keys[0].clone();
        keys.push(repeated);
        let bounds = bounds_of(drawn.kind, &scan);
        for (residency, served) in tiers.all() {
            let got = served
                .table
                .multi_scan(&keys, &reads(&columns), &bounds)
                .await?;
            assert_eq!(got.snapshot_id, served.current().snapshot_id);
            assert_eq!(got.results.len(), keys.len());
            for (key, answer) in keys.iter().zip(&got.results) {
                let one = served.table.scan(key, &reads(&columns), &bounds).await?;
                assert_eq!(answer, &one.scanned, "{residency} {key}");
                assert_eq!(
                    answer,
                    &expected(&drawn.fixture, drawn.kind, key, &columns, &scan),
                    "{residency} {key} {scan:?}"
                );
            }
            keys_answered += got.results.len();
        }
    }
    assert!(keys_answered > 0);
    Ok(())
}

/// Rows per key of the hand-built tables below.
const HISTORY_ROWS: u64 = 12;

/// The step between a hand-built key's rows: room for an append between
/// two of them.
const HISTORY_STEP: i64 = 4;

/// Ids of the hand-built tables.
const HISTORY_IDS: Range<u64> = 0..8;

fn history_rows(_: u64) -> u64 {
    HISTORY_ROWS
}

/// A hand-built ordered table of `kind`: `HISTORY_IDS`, `HISTORY_ROWS`
/// each, in small pages and row groups, two value columns, written to a
/// fresh catalog.
async fn history_table(
    name: &str,
    kind: OrderKind,
) -> Result<(TempDir, Arc<dyn Catalog>, Fixture)> {
    let (dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: HISTORY_IDS,
        buckets: ONE_BUCKET,
        second_value: true,
        value_bytes: DRAWN_VALUE_BYTES,
        layout: Layout {
            page_rows: 2,
            row_group_rows: Some(5),
            ..Layout::contract()
        },
        ..FixtureSpec::ordered(
            name,
            KeyType::Long,
            Ordered {
                kind,
                rows_per_key: history_rows,
                start: ORDER_START,
                step: HISTORY_STEP,
            },
        )
    };
    let fixture =
        write_fixture(catalog.as_ref(), icetray_testkit::fixture::NAMESPACE, spec).await?;
    Ok((dir, catalog, fixture))
}

/// The page size the paging test reads a history in.
const PAGE_LIMIT: u32 = 3;

/// A key's whole history, read in pages by order value as a caller pages
/// (RFD 025, Paging), each request setting `before` to the oldest value of
/// the last page reading newest first and `after` to the newest reading
/// oldest first, returns every live row once, in order, across the files
/// an append and an update left and around deleted rows, the last page
/// with `more` unset.
#[tokio::test]
async fn test_paging_by_order_value_returns_the_whole_history_once() -> Result<()> {
    let (_dir, catalog, mut fixture) = history_table("paged", OrderKind::Long).await?;
    let id = 3;
    let last = ORDER_START + HISTORY_STEP * i64::try_from(HISTORY_ROWS - 1).unwrap();
    fixture
        .append_ordered(
            catalog.as_ref(),
            &[
                (id, ORDER_START + 1),
                (id, ORDER_START + HISTORY_STEP + 1),
                (id, last + 1),
            ],
            "between",
        )
        .await?;
    fixture
        .delete_rows(
            &[(id, ORDER_START), (id, ORDER_START + 2 * HISTORY_STEP)],
            DeleteForm::Vectors,
            "gone",
        )
        .await?;
    fixture
        .update_rows(&[(id, ORDER_START + 1)], DeleteForm::Vectors, "rewritten")
        .await?;
    let key = fixture.key(id);
    let columns = [VALUE_COLUMN, SECOND_VALUE_COLUMN];
    let all = |direction| TestScan {
        direction,
        ..TestScan::default()
    };
    let tiers = serve_in_every_tier(|residency| fixture.config(residency, false)).await?;
    for direction in [Direction::NewestFirst, Direction::OldestFirst] {
        let whole = expected(&fixture, OrderKind::Long, &key, &columns, &all(direction));
        assert!(whole.rows.len() > usize::try_from(PAGE_LIMIT).unwrap() * 2);
        for (residency, served) in tiers.all() {
            let mut bounds = ScanBounds {
                direction,
                limit: PAGE_LIMIT,
                after: None,
                before: None,
            };
            let mut read: Vec<ScanRow> = Vec::new();
            loop {
                let page = served.table.scan(&key, &reads(&columns), &bounds).await?;
                let last_row = page.scanned.rows.last().map(|row| row.order.clone());
                read.extend(page.scanned.rows);
                if !page.scanned.more {
                    break;
                }
                match direction {
                    Direction::NewestFirst => bounds.before = last_row,
                    Direction::OldestFirst => bounds.after = last_row,
                }
            }
            assert_eq!(read, whole.rows, "{residency} {direction:?}");
        }
    }
    Ok(())
}

/// `more` counts live rows alone (Review Focus 2): with rows deleted at
/// both edges of a key's run and inside it, a limit of the run's live
/// rows leaves `more` unset in either direction, one less sets it, and
/// the deleted rows at the edge are never returned or counted.
#[tokio::test]
async fn test_scan_more_counts_live_rows_only() -> Result<()> {
    let (_dir, _catalog, mut fixture) = history_table("edges", OrderKind::Long).await?;
    let id = 5;
    let held = live_orders(&fixture, id);
    let deleted = [held[0], held[1], held[held.len() / 2], held[held.len() - 1]];
    let pairs: Vec<(u64, i64)> = deleted.iter().map(|&order| (id, order)).collect();
    fixture
        .delete_rows(&pairs, DeleteForm::Positions, "edges")
        .await?;
    let live = u32::try_from(live_orders(&fixture, id).len()).unwrap();
    assert_eq!(usize::try_from(live).unwrap(), held.len() - deleted.len());
    let key = fixture.key(id);
    let tiers = serve_in_every_tier(|residency| fixture.config(residency, false)).await?;
    for direction in [Direction::NewestFirst, Direction::OldestFirst] {
        for (limit, more) in [(live, false), (live - 1, true)] {
            let bounds = ScanBounds {
                direction,
                limit,
                after: None,
                before: None,
            };
            for (residency, served) in tiers.all() {
                let got = served
                    .table
                    .scan(&key, &reads(&[VALUE_COLUMN]), &bounds)
                    .await?
                    .scanned;
                assert_eq!(got.more, more, "{residency} {direction:?} limit {limit}");
                assert_eq!(got.rows.len(), usize::try_from(limit).unwrap());
                assert!(
                    got.rows.iter().all(|row| !deleted
                        .iter()
                        .any(|&order| row.order == OrderValue::Long(order))),
                    "{residency}: no deleted row"
                );
            }
        }
    }
    Ok(())
}

/// A bound beyond the 32-bit range of an `int` or `date` column.
const PAST_32_BITS: i64 = 1 << 40;

/// Bounds are exclusive at the extremes (Review Focus 5): `after` at
/// `i64::MIN` and `before` at `i64::MAX` admit every row of a long column,
/// an `int` or `date` bound beyond the 32-bit range admits every row on its
/// side, and a bound equal to a stored value, long or string, excludes that
/// row and admits the rest on its side.
#[tokio::test]
async fn test_scan_bounds_are_exclusive_at_the_extremes() -> Result<()> {
    let id = 2;
    let columns = [VALUE_COLUMN];
    for kind in [
        OrderKind::Long,
        OrderKind::Int,
        OrderKind::Date,
        OrderKind::Str,
    ] {
        let (_dir, _catalog, fixture) = history_table(&format!("extremes_{kind:?}"), kind).await?;
        let key = fixture.key(id);
        let held = live_orders(&fixture, id);
        let stored = held[held.len() / 2];
        let stored_value = order_value(kind, stored);
        let oldest = |after: Option<i64>, before: Option<i64>| TestScan {
            direction: Direction::OldestFirst,
            after,
            before,
            ..TestScan::default()
        };
        let extreme = match kind {
            OrderKind::Long => Some((i64::MIN, i64::MAX)),
            OrderKind::Int | OrderKind::Date => Some((-PAST_32_BITS, PAST_32_BITS)),
            OrderKind::Timestamp
            | OrderKind::TimestampTz
            | OrderKind::TimestampNs
            | OrderKind::TimestampTzNs
            | OrderKind::Str
            | OrderKind::Bin
            | OrderKind::Fixed(_) => None,
        };
        // Each case: the bounds as sent, the model's scan, and whether the
        // stored value is a bound, which excludes its row.
        let mut cases: Vec<(ScanBounds, TestScan, bool)> = Vec::new();
        if let Some((low, high)) = extreme {
            for (after, before) in [
                (Some(low), None),
                (None, Some(high)),
                (Some(low), Some(high)),
            ] {
                let scan = oldest(after, before);
                let bounds = ScanBounds {
                    after: after.map(OrderValue::Long),
                    before: before.map(OrderValue::Long),
                    ..bounds_of(kind, &oldest(None, None))
                };
                cases.push((bounds, scan, false));
            }
        }
        for (after, before) in [(Some(stored), None), (None, Some(stored))] {
            let scan = oldest(after, before);
            cases.push((bounds_of(kind, &scan), scan, true));
        }
        let tiers = serve_in_every_tier(|residency| fixture.config(residency, false)).await?;
        for (bounds, scan, at_stored) in cases {
            let want = expected(&fixture, kind, &key, &columns, &scan);
            assert!(!want.rows.is_empty(), "{kind:?} {scan:?} admits rows");
            if at_stored {
                assert!(
                    want.rows.iter().all(|row| row.order != stored_value),
                    "a bound at a stored value excludes its row"
                );
            } else {
                assert_eq!(want.rows.len(), held.len(), "an extreme admits every row");
            }
            for (residency, served) in tiers.all() {
                let got = served.table.scan(&key, &reads(&columns), &bounds).await?;
                assert_eq!(got.scanned, want, "{residency} {kind:?} {bounds:?}");
            }
        }
    }
    Ok(())
}

/// Ids of the compacted tables, three rows each, over two buckets, so a
/// middle key's run lies inside one key page of one row group.
const COMPACTED_IDS: Range<u64> = 0..40;

fn compacted_rows(_: u64) -> u64 {
    3
}

/// A compacted ordered table of `COMPACTED_IDS` in the contract layout: one
/// file per bucket, one row group, no dictionary-encoded column, both value
/// columns.
async fn compacted_table(name: &str) -> Result<(TempDir, Arc<dyn Catalog>, Fixture)> {
    let (dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: COMPACTED_IDS,
        buckets: TWO_BUCKETS,
        second_value: true,
        ..FixtureSpec::ordered(
            name,
            KeyType::Long,
            Ordered {
                kind: OrderKind::Long,
                rows_per_key: compacted_rows,
                start: ORDER_START,
                step: HISTORY_STEP,
            },
        )
    };
    let fixture =
        write_fixture(catalog.as_ref(), icetray_testkit::fixture::NAMESPACE, spec).await?;
    Ok((dir, catalog, fixture))
}

/// An id of `fixture` with another id of its bucket on each side, so its
/// run is neither the first nor the last of its file and both its edges
/// lie inside one key page.
fn middle_id(fixture: &Fixture, buckets: u32) -> u64 {
    let ids: Vec<u64> = fixture.ids().collect();
    let bucket_of = |id: u64| bucket(&fixture.key(id), buckets);
    ids.iter()
        .copied()
        .find(|&id| {
            let mates = || {
                ids.iter()
                    .filter(|&&other| bucket_of(other) == bucket_of(id))
            };
            mates().any(|&other| other < id) && mates().any(|&other| other > id)
        })
        .expect("a bucket of more than two ids")
}

/// The value columns a cost test reads, and what reading them costs on top
/// of the order column: one span GET each.
const BOTH: [&str; 2] = [VALUE_COLUMN, SECOND_VALUE_COLUMN];

/// The order column's one span over a run in one row group: a GET under
/// `full` and `bloom`, a read of the chunk file under `disk`.
const ORDER_SPAN: u64 = 1;

/// The key pages a run inside one key page decodes: both its edges lie in
/// that page, which is decoded once.
const KEY_PAGES_OF_A_MIDDLE_RUN: u64 = 1;

/// A cold Scan of a key held in one file, in one row group, with no
/// dictionary-encoded column and nothing cached, costs per tier (RFD 025,
/// Cost; controller ruling): under `full` one GET per requested column and
/// one for the order column; under `disk` one GET per requested column,
/// the order column and the key page read from the chunk file; under
/// `bloom` the key page's GET first, then the columns' and the order
/// column's.
#[tokio::test]
async fn test_cold_scan_of_a_compacted_table_costs_one_get_per_column_and_one_for_order()
-> Result<()> {
    let (_dir, _catalog, fixture) = compacted_table("cold").await?;
    let key = fixture.key(middle_id(&fixture, TWO_BUCKETS));
    let columns = count(BOTH.len());
    let tiers = serve_in_every_tier(|residency| fixture.config(residency, false)).await?;
    for (residency, served) in tiers.all() {
        let stats = served.storage.stats();
        let before = (stats.gets(), stats.local_reads());
        let got = served
            .table
            .scan(
                &key,
                &reads(&BOTH),
                &bounds_of(OrderKind::Long, &TestScan::default()),
            )
            .await?;
        assert_eq!(
            got.scanned,
            expected(&fixture, OrderKind::Long, &key, &BOTH, &TestScan::default())
        );
        let (gets, local_reads) = (stats.gets() - before.0, stats.local_reads() - before.1);
        let (want_gets, want_local) = match residency {
            Residency::Full => (columns + ORDER_SPAN, 0),
            Residency::Disk => (columns, KEY_PAGES_OF_A_MIDDLE_RUN + ORDER_SPAN),
            Residency::Bloom => (KEY_PAGES_OF_A_MIDDLE_RUN + columns + ORDER_SPAN, 0),
        };
        assert_eq!(gets, want_gets, "{residency} GETs");
        assert_eq!(local_reads, want_local, "{residency} local reads");
    }
    Ok(())
}

/// Ids an interleaving append writes again, which the first file's range
/// covers but does not hold: every other id of the compacted table's.
const STRIDE: u64 = 2;

/// A Scan reads only the files that hold its key (RFD 025, Cost): a key
/// the first file holds, inside the range of an interleaved file that
/// does not, costs the one file's GETs; a key both hold costs both files'.
#[tokio::test]
async fn test_scan_reads_only_files_that_hold_the_key() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: COMPACTED_IDS,
        id_stride: STRIDE,
        buckets: ONE_BUCKET,
        second_value: true,
        ..FixtureSpec::ordered(
            "files",
            KeyType::Long,
            Ordered {
                kind: OrderKind::Long,
                rows_per_key: compacted_rows,
                start: ORDER_START,
                step: HISTORY_STEP,
            },
        )
    };
    let mut fixture =
        write_fixture(catalog.as_ref(), icetray_testkit::fixture::NAMESPACE, spec).await?;
    let odd: Vec<u64> = COMPACTED_IDS.filter(|id| id % STRIDE == 1).collect();
    fixture.append(catalog.as_ref(), odd, "odd").await?;
    let even = middle_id(&fixture, ONE_BUCKET) + 1;
    let even = even + even % STRIDE;
    let shared = even + STRIDE;
    // Later rows of the shared key and its two neighbors, so its run lies
    // inside the third file's key page too, and the third file's range
    // leaves the first key out.
    let later: Vec<(u64, i64)> = [shared - 1, shared, shared + 1]
        .into_iter()
        .map(|id| {
            (
                id,
                live_orders(&fixture, id).last().copied().unwrap() + HISTORY_STEP,
            )
        })
        .collect();
    fixture
        .append_ordered(catalog.as_ref(), &later, "later")
        .await?;
    assert_eq!(fixture.file_count(), 3);
    let columns = count(BOTH.len());
    for (key, files) in [(fixture.key(even), 1), (fixture.key(shared), 2)] {
        // Served afresh for each key, so nothing the other key's Scan
        // cached, such as a page both keys' rows share, is warm.
        let tiers = serve_in_every_tier(|residency| fixture.config(residency, false)).await?;
        for (residency, served) in tiers.all() {
            let stats = served.storage.stats();
            let before = stats.gets();
            let scan = TestScan::default();
            let got = served
                .table
                .scan(&key, &reads(&BOTH), &bounds_of(OrderKind::Long, &scan))
                .await?;
            assert_eq!(
                got.scanned,
                expected(&fixture, OrderKind::Long, &key, &BOTH, &scan)
            );
            let per_file = match residency {
                Residency::Full => columns + ORDER_SPAN,
                Residency::Disk => columns,
                Residency::Bloom => KEY_PAGES_OF_A_MIDDLE_RUN + columns + ORDER_SPAN,
            };
            assert_eq!(
                stats.gets() - before,
                files * per_file,
                "{residency} {key}: the files holding the key alone"
            );
        }
    }
    Ok(())
}

/// A Get or `MultiGet` of an ordered table is `Error::WrongTableKind` naming
/// it ordered, in every tier, and a Scan or `MultiScan` of a key-value table
/// the same naming it key-value (RFD 025, Wire).
#[tokio::test]
async fn test_get_on_an_ordered_table_is_wrong_table_kind() -> Result<()> {
    let (_dir, _catalog, fixture) = compacted_table("kinds").await?;
    let key = fixture.key(middle_id(&fixture, TWO_BUCKETS));
    let wrong = |result: std::result::Result<(), Error>, expected: TableKind| match result {
        Err(Error::WrongTableKind { kind, .. }) => assert_eq!(kind, expected),
        other => panic!("expected the wrong kind, got {other:?}"),
    };
    let tiers = serve_in_every_tier(|residency| fixture.config(residency, false)).await?;
    for (_, served) in tiers.all() {
        wrong(
            served.table.get(&key, &reads(&BOTH)).await.map(drop),
            TableKind::Ordered,
        );
        wrong(
            served
                .table
                .multi_get(std::slice::from_ref(&key), &reads(&BOTH))
                .await
                .map(drop),
            TableKind::Ordered,
        );
    }
    let (_kv_dir, kv_catalog) = local_catalog().await?;
    let kv = write_fixture(
        kv_catalog.as_ref(),
        icetray_testkit::fixture::NAMESPACE,
        FixtureSpec {
            ids: COMPACTED_IDS,
            buckets: TWO_BUCKETS,
            ..FixtureSpec::small("plain", KeyType::Long)
        },
    )
    .await?;
    let kv_tiers = serve_in_every_tier(|residency| kv.config(residency, false)).await?;
    let kv_key = kv.key(COMPACTED_IDS.start);
    let scan = bounds_of(OrderKind::Long, &TestScan::default());
    for (_, served) in kv_tiers.all() {
        wrong(
            served
                .table
                .scan(&kv_key, &reads(&[VALUE_COLUMN]), &scan)
                .await
                .map(drop),
            TableKind::KeyValue,
        );
        wrong(
            served
                .table
                .multi_scan(
                    std::slice::from_ref(&kv_key),
                    &reads(&[VALUE_COLUMN]),
                    &scan,
                )
                .await
                .map(drop),
            TableKind::KeyValue,
        );
    }
    Ok(())
}

/// An ordered table's pages, a Bloom-tier key page included, go through
/// the scan cache and never the lookup cache, and a key-value table's
/// lookups never touch the scan cache (RFD 025, Caching and hedging).
#[tokio::test]
async fn test_scan_pages_stay_out_of_the_key_value_cache() -> Result<()> {
    let (_dir, _catalog, fixture) = compacted_table("caches").await?;
    let key = fixture.key(middle_id(&fixture, TWO_BUCKETS));
    let columns = count(BOTH.len());
    let tiers = serve_in_every_tier(|residency| fixture.config(residency, false)).await?;
    for (residency, served) in tiers.all() {
        served
            .table
            .scan(
                &key,
                &reads(&BOTH),
                &bounds_of(OrderKind::Long, &TestScan::default()),
            )
            .await?;
        assert_eq!(
            served.cache.fetches() + served.cache.hits(),
            0,
            "{residency}: the lookup cache saw no page"
        );
        let fetched = match residency {
            Residency::Full => columns + ORDER_SPAN,
            Residency::Disk => columns,
            Residency::Bloom => KEY_PAGES_OF_A_MIDDLE_RUN + columns + ORDER_SPAN,
        };
        assert_eq!(
            served.scan_cache.fetches(),
            fetched,
            "{residency}: every fetched page went through the scan cache"
        );
    }
    let (_kv_dir, kv_catalog) = local_catalog().await?;
    let kv = write_fixture(
        kv_catalog.as_ref(),
        icetray_testkit::fixture::NAMESPACE,
        FixtureSpec {
            ids: COMPACTED_IDS,
            buckets: TWO_BUCKETS,
            ..FixtureSpec::small("plain_caches", KeyType::Long)
        },
    )
    .await?;
    let kv_tiers = serve_in_every_tier(|residency| kv.config(residency, false)).await?;
    for (residency, served) in kv_tiers.all() {
        let kv_key = kv.key(COMPACTED_IDS.start);
        assert_eq!(
            served
                .table
                .get(&kv_key, &reads(&[VALUE_COLUMN]))
                .await?
                .row,
            kv.expected(&kv_key)
        );
        let key_pages = match residency {
            Residency::Full | Residency::Disk => 0,
            Residency::Bloom => KEY_PAGES_OF_A_MIDDLE_RUN,
        };
        assert_eq!(
            served.cache.fetches(),
            key_pages + 1,
            "{residency}: the value page, and under bloom the key page"
        );
        assert_eq!(
            served.scan_cache.fetches() + served.scan_cache.hits(),
            0,
            "{residency}: the scan cache saw no page"
        );
    }
    Ok(())
}

/// The default the added column states.
const ADDED_DEFAULT: &[u8] = b"added later";

/// A column added after a table's files were written answers its stated
/// default for every row of those files, read with no GET of it (RFD 023):
/// a Scan reading it beside the value column costs that column's GET and
/// the order column's alone.
#[tokio::test]
async fn test_scan_answers_a_column_added_after_a_file_with_its_default() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: COMPACTED_IDS,
        buckets: TWO_BUCKETS,
        format_version: FormatVersion::V3,
        ..FixtureSpec::ordered(
            "added",
            KeyType::Long,
            Ordered {
                kind: OrderKind::Long,
                rows_per_key: compacted_rows,
                start: ORDER_START,
                step: HISTORY_STEP,
            },
        )
    };
    let mut fixture =
        write_fixture(catalog.as_ref(), icetray_testkit::fixture::NAMESPACE, spec).await?;
    fixture
        .evolve(&[SchemaChange::Add {
            name: SECOND_VALUE_COLUMN.to_string(),
            ty: PrimitiveType::Binary,
            default: StatedDefault::Literal(Literal::binary(ADDED_DEFAULT.to_vec())),
        }])
        .await?;
    let data = fixture.files().to_vec();
    fixture
        .commit_by_hand(HandSnapshot {
            data,
            deletes: Vec::new(),
        })
        .await?;
    let key = fixture.key(middle_id(&fixture, TWO_BUCKETS));
    let scan = TestScan::default();
    let mut want = expected(&fixture, OrderKind::Long, &key, &[VALUE_COLUMN], &scan);
    assert!(!want.rows.is_empty());
    for row in &mut want.rows {
        row.values
            .insert(0, Some(Bytes::from_static(ADDED_DEFAULT)));
    }
    let asked = [SECOND_VALUE_COLUMN, VALUE_COLUMN];
    let tiers = serve_in_every_tier(|residency| fixture.config_serving(residency, &asked)).await?;
    for (residency, served) in tiers.all() {
        let stats = served.storage.stats();
        let before = stats.gets();
        let got = served
            .table
            .scan(&key, &reads(&asked), &bounds_of(OrderKind::Long, &scan))
            .await?;
        assert_eq!(got.scanned, want, "{residency}");
        let read_columns = 1;
        let gets = match residency {
            Residency::Full => read_columns + ORDER_SPAN,
            Residency::Disk => read_columns,
            Residency::Bloom => KEY_PAGES_OF_A_MIDDLE_RUN + read_columns + ORDER_SPAN,
        };
        assert_eq!(
            stats.gets() - before,
            gets,
            "{residency}: no GET of the added column"
        );
    }
    Ok(())
}
