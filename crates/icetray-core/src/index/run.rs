//! A key's run in one ordered file (RFD 025, Cost): the rows the key holds
//! in each row group its run spans, found from the resident first and last
//! key of each row group and the fences, with at most two key pages
//! decoded per row group; and the rows one Scan reads of it, the order
//! column and the requested value columns over the candidate rows, in one
//! round where the order column's column index places the bounds. A
//! key-value table never reaches this module.

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use metrics::Counter;
use parquet::data_type::{ByteArrayType, FixedLenByteArrayType};
use tokio::sync::Semaphore;

use super::build::{decode_run, read_optional};
use super::ordered::{OrderRun, PageFit, ValueRange, decode_orders, order_column_index};
use super::page_header::{check_order_pages, check_value_pages};
use super::{
    ColumnChunk, FileFault, FileIndex, GroupRange, KeyResidency, OrderLeaf, PageSource,
    RowPosition, RowRef, ValueShape, file_fault, row_index, row_offset, value_bytes,
};
use crate::cache::{Issue, PageCache, PageOrigin};
use crate::error::{Error, Result};
use crate::key::Key;
use crate::local::DiskChunks;
use crate::order::{Direction, OrderValue, ScanBounds};
use crate::parquet_io::Segments;
use crate::storage::HedgeWindow;

/// The rows of a key's run in one row group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RunPart {
    /// The row group.
    pub(crate) row_group: usize,
    /// The run's rows within the row group, ascending by order value;
    /// never empty.
    pub(crate) rows: Range<i64>,
}

/// A key's run in one file: its rows in each row group it spans, in file
/// order, so ascending by order value; at least one part.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Run {
    /// The run's rows per row group.
    pub(crate) parts: Vec<RunPart>,
}

/// What one file answers a Scan of one key (`FileIndex::read_run`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FileRows {
    /// The rows taken, in the Scan's direction: each its order value and
    /// one value per requested column, `None` for a null and for a column
    /// the file does not hold (`FileIndex::value_leaf`), whose default the
    /// caller answers.
    pub(crate) rows: Vec<(OrderValue, Vec<Option<Bytes>>)>,
    /// The order value of the next live row inside the bounds past the
    /// last row taken, in the Scan's direction; `None` when no live row
    /// remains there.
    pub(crate) next: Option<OrderValue>,
    /// Whether any order or value page the read took came from object
    /// storage, a fetch it ran or joined.
    pub(crate) from_store: bool,
}

/// Where a run's pages come from beyond what the tier holds (RFD 025,
/// Caching and hedging): the Scan cache, through which every GET of a
/// page goes; the fan-out a GET holds one permit of while it runs; the
/// table's span hedge window, which span GETs are hedged in; and the
/// counters a GET counts in as it is issued, each value column's at its
/// position in the layout and the order column's. A Bloom-tier key page is
/// fetched through the cache under the file's lookup window and counted
/// nowhere.
#[derive(Clone, Copy)]
pub(crate) struct RunSource<'a> {
    /// The Scan cache.
    pub(crate) cache: &'a PageCache,
    /// The Scan's fan-out (`TableHandles::batch_fan_out`); `None` bounds
    /// nothing.
    pub(crate) fan_out: Option<&'a Arc<Semaphore>>,
    /// The table's span hedge window.
    pub(crate) window: &'a HedgeWindow,
    /// Per value column, by position in the layout, the counter its GETs
    /// count in; a position past the end counts nowhere.
    pub(crate) value_gets: &'a [Counter],
    /// The counter the order column's GETs count in.
    pub(crate) order_gets: Option<&'a Counter>,
}

impl FileIndex {
    /// The key's run: per row group it spans, its exact rows; `None` for a
    /// key the file does not hold. The resident first and last key of each
    /// row group give the row groups the run spans. Within one, the run
    /// lies from the last key page whose fence is below the key to the last
    /// whose fence is at or below it, and every page between those two
    /// holds only the key, so decoding the two gives the run's exact rows;
    /// neither is decoded where the row group's first or last key is the
    /// key, since the run then starts at the row group's first row or ends
    /// at its last. Full decodes the pages from the resident key chunks
    /// with no I/O; Disk reads each from the chunk file, one local read
    /// each and one more for the chunk's dictionary page when the key
    /// column has one, and no GET; Bloom screens each row group with its
    /// filter where the run's presence is not already known, then fetches
    /// the pages of every row group it spans concurrently through
    /// `source`'s cache, one GET each under the file's lookup window and
    /// the key column's dictionary page besides, each holding a permit of
    /// the fan-out. A key outside the file's range or between two row groups
    /// costs nothing in any tier; one inside a row group that the file does
    /// not hold costs one key-page decode in Full, one local read in Disk,
    /// and in Bloom a key-page GET at the filter's false-positive rate.
    pub(crate) async fn locate_run(&self, key: &Key, source: RunSource<'_>) -> Result<Option<Run>> {
        if !self.may_contain(key) {
            return Ok(None);
        }
        let first = self.groups.partition_point(|group| group.last < *key);
        let last = self.groups.partition_point(|group| group.first <= *key);
        let edges: Vec<GroupEdges> = self.groups[first..last.max(first)]
            .iter()
            .filter_map(|group| self.edges(group, key))
            .collect();
        let parts = futures::future::try_join_all(
            edges.iter().map(|edges| self.run_rows(edges, key, source)),
        )
        .await?;
        let parts: Vec<RunPart> = parts.into_iter().flatten().collect();
        Ok((!parts.is_empty()).then_some(Run { parts }))
    }

    /// Where the run of `key` can start and end in `group`, whose key range
    /// holds it: a row where the group's first or last key tells, a key page
    /// to decode otherwise. `None` where the Bloom tier's filter rules the
    /// key out, which it is asked only when neither end is known.
    fn edges(&self, group: &GroupRange, key: &Key) -> Option<GroupEdges> {
        let fences = &self.fences[group.row_group];
        let below = fences.partition_point(|fence| fence < key);
        let through = fences.partition_point(|fence| fence <= key);
        let start = match below.checked_sub(1) {
            None => Edge::Row(0),
            Some(page) => Edge::Page(page),
        };
        let end = if group.last == *key {
            Edge::Row(self.key_column().group(group.row_group).rows())
        } else {
            Edge::Page(through.checked_sub(1)?)
        };
        if let (Edge::Page(_), Edge::Page(_), KeyResidency::Filters(filters)) =
            (start, end, &self.keys)
            && !filters[group.row_group].check(&*key.as_bytes())
        {
            return None;
        }
        Some(GroupEdges {
            row_group: group.row_group,
            start,
            end,
        })
    }

    /// The run's rows in the row group `edges` describes, decoding the key
    /// pages its edges name, each once; `None` where the row group does not
    /// hold the key.
    async fn run_rows(
        &self,
        edges: &GroupEdges,
        key: &Key,
        source: RunSource<'_>,
    ) -> Result<Option<RunPart>> {
        let chunk = self.key_column().group(edges.row_group);
        let mut pages: Vec<usize> = [edges.start, edges.end]
            .iter()
            .filter_map(|edge| match edge {
                Edge::Page(page) => Some(*page),
                Edge::Row(_) => None,
            })
            .collect();
        pages.dedup();
        let segments = self.key_pages(chunk, &pages, source).await?;
        let mut held = Vec::with_capacity(pages.len());
        for &page in &pages {
            let rows = chunk.page_rows(page);
            let keys = decode_run(
                chunk,
                self.layout.key_shape,
                rows.clone(),
                Arc::clone(&segments),
            )?;
            let at = keys.equal_range(key);
            held.push((
                page,
                rows.start + row_offset(at.start)..rows.start + row_offset(at.end),
            ));
        }
        let rows_of = |page| {
            held.iter()
                .find(|(decoded, _)| *decoded == page)
                .map(|(_, rows)| rows.clone())
                .expect("every page an edge names is decoded")
        };
        let start = match edges.start {
            Edge::Row(row) => row,
            Edge::Page(page) => rows_of(page).start,
        };
        let end = match edges.end {
            Edge::Row(row) => row,
            Edge::Page(page) => rows_of(page).end,
        };
        Ok((start < end).then_some(RunPart {
            row_group: edges.row_group,
            rows: start..end,
        }))
    }

    /// The bytes key pages `pages` of `chunk` decode from: the resident
    /// chunks in Full; in Disk and Bloom each page and the chunk's
    /// dictionary page when it has one, read together, from the chunk file
    /// or through `source`'s cache under the file's lookup window. Nothing
    /// is read for no pages.
    async fn key_pages(
        &self,
        chunk: ColumnChunk<'_>,
        pages: &[usize],
        source: RunSource<'_>,
    ) -> Result<Arc<Segments>> {
        let from = match &self.keys {
            KeyResidency::Chunks(chunks) => return Ok(Arc::clone(chunks)),
            KeyResidency::Disk(disk) => PageSource::Disk(disk),
            KeyResidency::Filters(_) => PageSource::Store(
                Some(source.cache),
                Issue {
                    fan_out: source.fan_out,
                    counted: None,
                },
            ),
        };
        let mut ranges: Vec<Range<u64>> =
            pages.iter().map(|&page| chunk.page_range(page)).collect();
        if !ranges.is_empty() {
            ranges.extend(chunk.dictionary_range());
        }
        let parts = futures::future::try_join_all(ranges.into_iter().map(|range| async move {
            let (bytes, _origin) = self.read_from(from, range.clone()).await?;
            Ok::<_, Error>((range, bytes))
        }))
        .await?;
        Ok(Arc::new(Segments::new(parts, self.handle.size())))
    }

    /// The candidate rows of the run for one Scan: up to `limit` live rows
    /// from the direction's end, cut by the bounds, plus one more live
    /// row's order value to set `more`; `bounds.limit` is not read, since
    /// `limit` is the one the table resolved. Live rows are counted against
    /// `deleted` with no I/O. The order column's resident column index
    /// tells, for each order page of the run, whether its values lie below
    /// the bounds, above them, inside them, or across one
    /// (`ValueRange::fit`); a page at the run's edge holds a neighbouring
    /// key's values too, which widen its entry, so it is judged by the page
    /// beside it where that one holds the run alone under an integer entry,
    /// which a writer never truncates, and kept where nothing rules it out.
    /// No page before one below the bounds or after one above them is read
    /// in any column (`pieces`). From the direction's end the live rows of
    /// pages across a bound are read without counting, and the live rows of
    /// pages inside count until `limit` are taken, then the order value of
    /// the next live row is read with no value of it, on a page no row
    /// taken lies on if that is where it is. Every page of the order column
    /// and of each column in `columns`, positions in the layout, over those
    /// rows is read in one round, then the bounds cut the rows exactly.
    ///
    /// Rows across a bound count nothing toward `limit`. Under an exact
    /// entry, an integer one or a byte-array one no writer truncated, they
    /// lie on the page that holds the bound and on the run-edge pages,
    /// since the run ascends and so at most one page that holds the run
    /// alone lies across each bound (`PAGES_ACROSS_A_BOUND`). Where more
    /// lie across either bound, as under a truncated byte-array entry that
    /// a bound shares a prefix with, those order pages are read in a first
    /// round and the value pages of exactly the rows taken in a second; so
    /// are they where a row group of the run has no column index on its
    /// order chunk and a bound is set (`order_index_missing`). The pages
    /// are counted per bound over the whole run, not per row group, so
    /// under a truncated entry a bound with one such page on each side of
    /// a row group boundary takes two rounds: a round more, never a byte
    /// more, and never under an exact entry.
    ///
    /// Per column and row group, the pages are one span GET through
    /// `source`'s cache under its span window for every contiguous run the
    /// cache does not hold, and one GET for the chunk's dictionary page when
    /// it has one, each holding a permit of the fan-out and counted in the
    /// column's counter as it is issued; under Disk the order column's
    /// pages are read from the chunk file instead, one local read for the
    /// span and one for its dictionary page, and no GET. The headers of
    /// every page read are checked before parquet decodes it, as a value
    /// page's are (`check_value_page_headers`, `check_order_pages`); a
    /// span that decodes to fewer order values than rows is
    /// `FileFault::PageRowsMismatch`, and a string order value that is not
    /// UTF-8 `FileFault::OrderNotUtf8`.
    pub(crate) async fn read_run(
        &self,
        run: &Run,
        bounds: &ScanBounds,
        limit: u32,
        columns: &[usize],
        deleted: &(dyn Fn(RowPosition) -> bool + Sync),
        source: RunSource<'_>,
    ) -> Result<FileRows> {
        let order = self.order_leaf_of();
        let cut = Cut {
            after: bounds.after.as_ref(),
            before: bounds.before.as_ref(),
        };
        let pieces = self.pieces(run, order, cut);
        let limit = usize::try_from(limit).unwrap_or(usize::MAX);
        let walk = Walk {
            index: self,
            pieces: &pieces,
            direction: bounds.direction,
            limit,
            deleted,
        };
        let indexed = cut.is_open()
            || run.parts.iter().all(|part| {
                order_column_index(self.order_values(order).group(part.row_group)).is_some()
            });
        let placed = walk.rows(|piece, _| piece.fit);
        let one_round = indexed
            && placed
                .pages_across
                .iter()
                .all(|&pages| pages <= PAGES_ACROSS_A_BOUND);
        let (walked, orders, values) = if one_round {
            let walked = placed.rows;
            let (order_spans, value_spans) = (spans_of(&walked, false), spans_of(&walked, true));
            let (orders, values) = futures::try_join!(
                self.read_orders(order, &order_spans, source),
                self.read_values(columns, &value_spans, source),
            )?;
            (walked, orders, values)
        } else {
            let orders = self
                .read_orders(order, &spans_of(&placed.rows, false), source)
                .await?;
            let walked = walk.rows(|_, row| cut.fit(&orders.value(row))).rows;
            let values = self
                .read_values(columns, &spans_of(&walked, true), source)
                .await?;
            (walked, orders, values)
        };
        let mut rows = Vec::new();
        let mut next = None;
        for walked in &walked {
            let value = orders.value(walked.row);
            if !cut.holds(&value) {
                continue;
            }
            if rows.len() == limit {
                next = Some(value);
                break;
            }
            let row_values = values
                .columns
                .iter()
                .map(|column| column.as_ref().and_then(|column| column.value(walked.row)))
                .collect();
            rows.push((value, row_values));
        }
        Ok(FileRows {
            rows,
            next,
            from_store: orders.from_store || values.from_store,
        })
    }

    /// The run's rows a page of the order column at a time, in file order,
    /// each with where the page's values lie against `cut`: by the page's
    /// column index entry, and for a page at an edge of the run, which
    /// holds a neighbouring key's rows too, also by the next or previous
    /// page where that one holds the run's rows alone and its entry is an
    /// integer one, since the run ascends by order value; a byte-array
    /// entry may be truncated and bounds its own page alone
    /// (`ValueRange::after_page`). Since the run ascends, no row before a
    /// page below the bounds or after one above them lies inside, so only
    /// the pages between the last below and the first above are kept.
    fn pieces(&self, run: &Run, order: OrderLeaf, cut: Cut<'_>) -> Vec<Piece> {
        let mut pages = Vec::new();
        for part in &run.parts {
            let chunk = self.order_values(order).group(part.row_group);
            let index = order_column_index(chunk);
            let first = chunk.page_holding(row_index(part.rows.start));
            let last = chunk.page_holding(row_index(part.rows.end - 1));
            for page in first..=last {
                let held = chunk.page_rows(page);
                let rows = held.start.max(part.rows.start)..held.end.min(part.rows.end);
                let whole = rows == held;
                let range = ValueRange::of_page(index, page);
                pages.push((part.row_group, rows, whole, range));
            }
        }
        let alone = |at: Option<usize>| {
            at.and_then(|at| pages.get(at))
                .filter(|(_, _, whole, _)| *whole)
                .map(|(_, _, _, range)| *range)
        };
        let mut pieces: Vec<Piece> = (0..pages.len())
            .map(|at| {
                let (row_group, rows, whole, mut range) = pages[at].clone();
                if !whole {
                    if let Some(earlier) = alone(at.checked_sub(1)) {
                        range = range.after_page(earlier);
                    }
                    if let Some(later) = alone(Some(at + 1)) {
                        range = range.before_page(later);
                    }
                }
                Piece {
                    row_group,
                    rows,
                    whole,
                    fit: range.fit(cut.after, cut.before),
                }
            })
            .collect();
        let above = pieces
            .iter()
            .position(|piece| piece.fit == PageFit::Above)
            .unwrap_or(pieces.len());
        pieces.truncate(above);
        let below = pieces
            .iter()
            .rposition(|piece| piece.fit == PageFit::Below)
            .map_or(0, |at| at + 1);
        pieces.drain(..below);
        pieces
    }

    /// The order values of `spans`, rows per row group, read with their
    /// pages' headers checked: from the chunk file under Disk, through
    /// `source` otherwise.
    async fn read_orders(
        &self,
        order: OrderLeaf,
        spans: &[(usize, Range<i64>)],
        source: RunSource<'_>,
    ) -> Result<Orders> {
        let local = self.disk_chunks();
        let read = spans.iter().map(|(row_group, rows)| async move {
            let chunk = self.order_values(order).group(*row_group);
            let pages = pages_holding(chunk, rows);
            let (segments, from_store) = self
                .span_pages(chunk, pages.clone(), local, source, source.order_gets)
                .await?;
            check_order_pages(chunk, order.shape, pages, &segments)?;
            let values = decode_orders(chunk, order.shape, rows.clone(), segments)?;
            if values.len() != row_index(rows.end - rows.start) {
                return Err(chunk.page_rows_mismatch(chunk.page_holding(row_index(rows.start))));
            }
            if !values.is_utf8() {
                return Err(file_fault(
                    &self.handle.location,
                    FileFault::OrderNotUtf8 {
                        row_group: *row_group,
                    },
                ));
            }
            Ok::<_, Error>(((*row_group, rows.start, values), from_store))
        });
        let read = futures::future::try_join_all(read).await?;
        let from_store = read.iter().any(|(_, from_store)| *from_store);
        Ok(Orders {
            spans: read.into_iter().map(|(span, _)| span).collect(),
            from_store,
        })
    }

    /// The values of each column of `columns`, positions in the layout,
    /// over `spans`, every column's and row group's pages read together;
    /// `None` for a column the file does not hold.
    async fn read_values(
        &self,
        columns: &[usize],
        spans: &[(usize, Range<i64>)],
        source: RunSource<'_>,
    ) -> Result<Values> {
        let read = columns.iter().map(|&column| async move {
            let Some(value) = self.layout.values[column] else {
                return Ok::<_, Error>((None, false));
            };
            let counted = source.value_gets.get(column);
            let spans =
                futures::future::try_join_all(spans.iter().map(|(row_group, rows)| async move {
                    let chunk = self.value_column(value).group(*row_group);
                    let pages = pages_holding(chunk, rows);
                    let (segments, from_store) = self
                        .span_pages(chunk, pages.clone(), None, source, counted)
                        .await?;
                    check_value_pages(chunk, value.shape, pages, &segments)?;
                    let values = decode_values(chunk, value.shape, rows.clone(), &segments)?;
                    Ok::<_, Error>(((*row_group, rows.start, values), from_store))
                }))
                .await?;
            let from_store = spans.iter().any(|(_, from_store)| *from_store);
            let spans = spans.into_iter().map(|(span, _)| span).collect();
            Ok((Some(ColumnValues { spans }), from_store))
        });
        let read = futures::future::try_join_all(read).await?;
        let from_store = read.iter().any(|(_, from_store)| *from_store);
        Ok(Values {
            columns: read.into_iter().map(|(column, _)| column).collect(),
            from_store,
        })
    }

    /// Data pages `pages` of `chunk` and its dictionary page when it has
    /// one, as the bytes they decode from, and whether any came from object
    /// storage: from `local`, the chunk file, in one read for the pages and
    /// one for the dictionary page; otherwise through `source`'s cache, the
    /// pages it does not hold in span GETs under its span window and the
    /// dictionary page in a GET of its own, read together, each counted in
    /// `counted`.
    async fn span_pages(
        &self,
        chunk: ColumnChunk<'_>,
        pages: Range<usize>,
        local: Option<&DiskChunks>,
        source: RunSource<'_>,
        counted: Option<&Counter>,
    ) -> Result<(Arc<Segments>, bool)> {
        let size = self.handle.size();
        let dictionary = chunk.dictionary_range();
        if let Some(disk) = local {
            let span = chunk.page_range(pages.start).start..chunk.page_range(pages.end - 1).end;
            let read_dictionary = async {
                match dictionary.clone() {
                    Some(range) => disk.page(range).await.map(Some),
                    None => Ok(None),
                }
            };
            let (bytes, dictionary_bytes) =
                futures::try_join!(disk.page(span.clone()), read_dictionary)?;
            let mut parts = vec![(span, bytes)];
            parts.extend(dictionary.zip(dictionary_bytes));
            return Ok((Arc::new(Segments::new(parts, size)), false));
        }
        let issue = Issue {
            fan_out: source.fan_out,
            counted,
        };
        let ranges: Vec<Range<u64>> = pages.map(|page| chunk.page_range(page)).collect();
        let read_dictionary = async {
            match dictionary.clone() {
                Some(range) => self
                    .handle
                    .read_pages(
                        std::slice::from_ref(&range),
                        source.cache,
                        issue,
                        source.window,
                    )
                    .await
                    .map(Some),
                None => Ok(None),
            }
        };
        let ((bytes, origin), dictionary_read) = futures::try_join!(
            self.handle
                .read_pages(&ranges, source.cache, issue, source.window),
            read_dictionary
        )?;
        let mut from_store = origin == PageOrigin::Fetched;
        let mut parts: Vec<(Range<u64>, Bytes)> = ranges.into_iter().zip(bytes).collect();
        if let (Some(range), Some((bytes, origin))) = (dictionary, dictionary_read) {
            from_store |= origin == PageOrigin::Fetched;
            parts.extend(bytes.into_iter().map(|page| (range.clone(), page)));
        }
        Ok((Arc::new(Segments::new(parts, size)), from_store))
    }
}

/// The data pages of `chunk` holding `rows`, which are not empty.
fn pages_holding(chunk: ColumnChunk<'_>, rows: &Range<i64>) -> Range<usize> {
    chunk.page_holding(row_index(rows.start))..chunk.page_holding(row_index(rows.end - 1)) + 1
}

/// Rows `rows` of the value column chunk `chunk`, whose values decode as
/// `shape`, from `segments`: one entry per row, `None` for a null, the
/// bytes as `value_bytes` keeps them.
fn decode_values(
    chunk: ColumnChunk<'_>,
    shape: ValueShape,
    rows: Range<i64>,
    segments: &Arc<Segments>,
) -> Result<Vec<Option<Bytes>>> {
    let keep = |value: &[u8]| value_bytes(segments, value);
    Ok(match shape {
        ValueShape::Variable => read_optional::<ByteArrayType>(chunk, rows, Arc::clone(segments))?
            .iter()
            .map(|value| value.as_ref().map(|value| keep(value.data())))
            .collect(),
        ValueShape::Fixed => {
            read_optional::<FixedLenByteArrayType>(chunk, rows, Arc::clone(segments))?
                .iter()
                .map(|value| value.as_ref().map(|value| keep(value.data())))
                .collect()
        }
    })
}

/// A Scan's exclusive bounds on the order value.
#[derive(Clone, Copy)]
struct Cut<'a> {
    after: Option<&'a OrderValue>,
    before: Option<&'a OrderValue>,
}

impl Cut<'_> {
    /// Whether there is no bound.
    fn is_open(self) -> bool {
        self.after.is_none() && self.before.is_none()
    }

    /// Where `value` lies against the bounds: `Inside`, `Below`, or
    /// `Above`, never `Straddles`.
    fn fit(self, value: &OrderValue) -> PageFit {
        if self.after.is_some_and(|after| value <= after) {
            PageFit::Below
        } else if self.before.is_some_and(|before| value >= before) {
            PageFit::Above
        } else {
            PageFit::Inside
        }
    }

    /// Whether `value` lies inside the bounds.
    fn holds(self, value: &OrderValue) -> bool {
        self.fit(value) == PageFit::Inside
    }
}

/// Where a run can start or end in one row group.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Edge {
    /// At this row, which the row group's first or last key tells.
    Row(i64),
    /// Inside this key page, which is decoded to tell.
    Page(usize),
}

/// The edges of a run in one row group (`FileIndex::edges`).
struct GroupEdges {
    row_group: usize,
    start: Edge,
    end: Edge,
}

/// The rows of a run on one order page, whether they are all the page's
/// rows, and where the page's values lie against the bounds.
struct Piece {
    row_group: usize,
    rows: Range<i64>,
    whole: bool,
    fit: PageFit,
}

/// A row a read takes up, in the Scan's direction: one that may be taken,
/// whose values are read, or the one after the rows taken, whose order
/// value alone is.
struct Walked {
    row: RowRef,
    values: bool,
}

/// The rows a walk takes up, and how many pieces holding the run alone it
/// takes rows up from with their values across each bound, `after`'s and
/// then `before`'s (`Walk::rows`).
struct Taken {
    rows: Vec<Walked>,
    pages_across: [usize; 2],
}

/// The pages holding a run alone that one bound lies across under an
/// exact column index entry: the run ascends by order value, so the page
/// after the first such page lies wholly past the bound. Rows across a
/// bound on more of them, read with values in one round, would exceed RFD
/// 025's Cost promise, which allows the page that holds the bound and one
/// run-edge page past the rows the limit selects.
const PAGES_ACROSS_A_BOUND: usize = 1;

/// The rows of a run a read takes up (`Walk::rows`).
struct Walk<'a> {
    index: &'a FileIndex,
    pieces: &'a [Piece],
    direction: Direction,
    limit: usize,
    deleted: &'a (dyn Fn(RowPosition) -> bool + Sync),
}

impl Walk<'_> {
    /// From the direction's end, every live row of the pieces as `fit`
    /// judges the row, until `limit` rows judged inside are taken up with
    /// their values and one more such row without them; a row judged across
    /// a bound before then is taken up with its values, one after with its
    /// order value alone, and a row judged below or above the bounds is
    /// passed over.
    fn rows(&self, fit: impl Fn(&Piece, RowRef) -> PageFit) -> Taken {
        let pieces: Box<dyn Iterator<Item = &Piece>> = match self.direction {
            Direction::OldestFirst => Box::new(self.pieces.iter()),
            Direction::NewestFirst => Box::new(self.pieces.iter().rev()),
        };
        let mut taken = Taken {
            rows: Vec::new(),
            pages_across: [0; 2],
        };
        let mut inside = 0;
        for piece in pieces {
            let rows: Box<dyn Iterator<Item = i64>> = match self.direction {
                Direction::OldestFirst => Box::new(piece.rows.clone()),
                Direction::NewestFirst => Box::new(piece.rows.clone().rev()),
            };
            let mut across = false;
            for row in rows {
                let row = RowRef {
                    row_group: piece.row_group,
                    row,
                };
                if (self.deleted)(self.index.position(row)) {
                    continue;
                }
                match fit(piece, row) {
                    PageFit::Below | PageFit::Above => {}
                    PageFit::Inside if inside == self.limit => {
                        taken.rows.push(Walked { row, values: false });
                        return taken;
                    }
                    PageFit::Inside => {
                        inside += 1;
                        taken.rows.push(Walked { row, values: true });
                    }
                    PageFit::Straddles { after, before } => {
                        let values = inside < self.limit;
                        if values && piece.whole && !across {
                            across = true;
                            taken.pages_across[0] += usize::from(after);
                            taken.pages_across[1] += usize::from(before);
                        }
                        taken.rows.push(Walked { row, values });
                    }
                }
            }
        }
        taken
    }
}

/// Per row group, the rows from the first to the last of `walked`, all of
/// them or, with `values`, those whose values are read; in row group order.
fn spans_of(walked: &[Walked], values: bool) -> Vec<(usize, Range<i64>)> {
    let mut spans: BTreeMap<usize, Range<i64>> = BTreeMap::new();
    for walked in walked.iter().filter(|walked| walked.values || !values) {
        let row = walked.row.row;
        spans
            .entry(walked.row.row_group)
            .and_modify(|span| {
                span.start = span.start.min(row);
                span.end = span.end.max(row + 1);
            })
            .or_insert(row..row + 1);
    }
    spans.into_iter().collect()
}

/// The order values a read decoded: per row group, the first row and the
/// values from it on.
struct Orders {
    spans: Vec<(usize, i64, OrderRun)>,
    from_store: bool,
}

impl Orders {
    /// The order value of `row`, which the read decoded.
    fn value(&self, row: RowRef) -> OrderValue {
        self.spans
            .iter()
            .find_map(|(row_group, start, values)| {
                (*row_group == row.row_group
                    && *start <= row.row
                    && row.row - start < row_offset(values.len()))
                .then(|| values.value(row_index(row.row - start)))
            })
            .expect("the order values of every row taken up are read")
    }
}

/// The values a read decoded, per requested column; `None` for a column
/// the file does not hold.
struct Values {
    columns: Vec<Option<ColumnValues>>,
    from_store: bool,
}

/// One column's values a read decoded: per row group, the first row and
/// the values from it on.
struct ColumnValues {
    spans: Vec<(usize, i64, Vec<Option<Bytes>>)>,
}

impl ColumnValues {
    /// The value of `row`, `None` for a null; the read decoded it.
    fn value(&self, row: RowRef) -> Option<Bytes> {
        self.spans
            .iter()
            .find_map(|(row_group, start, values)| {
                (*row_group == row.row_group && *start <= row.row)
                    .then(|| values.get(row_index(row.row - start)))
                    .flatten()
            })
            .expect("the values of every row taken are read")
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::ops::RangeInclusive;
    use std::path::Path;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use crate::approx::count;
    use crate::config::{HedgeConfig, Residency};
    use crate::generation::testing::{self as generation_testing, cache};
    use crate::index::testing::{
        EXTRA_COLUMN, Fixture, ORDER_FIELD_ID, Opened, Orders, long_orders, ordered_layout, store,
        store_gauged, value_for,
    };
    use crate::key::KeyType;
    use crate::manifest::testing::binary_column;
    use crate::order::OrderType;
    use crate::storage::IoStats;
    use crate::storage::testing::{DEADLINE_GUARD, Gauged};

    /// Rows per page of every column in every fixture, so pages of the
    /// key, value, and order columns hold the same rows.
    const PAGE: usize = 4;
    /// The gap between consecutive order values of one key's run; each
    /// key's run starts at zero, so neighbouring keys' values overlap, as
    /// timestamps of different users do.
    const ORDER_STEP: i64 = 10;
    /// The field id of the first extra value column, after the order
    /// column's.
    const EXTRA_FIELD_ID: i32 = ORDER_FIELD_ID + 1;
    /// A limit above every run a test reads, so the whole run is taken.
    const WHOLE_RUN: u32 = 1_000;

    /// Keys and row counts `runs`, in key order, written in row groups of
    /// `group` rows and pages of `PAGE`; the `j`th row of each key's run
    /// has order value `j * ORDER_STEP`, and row `i` of the file the value
    /// `value_for(i)` in every value column.
    fn runs_of(runs: &[(i64, usize)], group: usize) -> Fixture {
        let mut keys = Vec::new();
        let mut orders = Vec::new();
        for &(key, rows) in runs {
            for j in 0..rows {
                keys.push(key);
                orders.push(i64::try_from(j).unwrap() * ORDER_STEP);
            }
        }
        let mut fixture = Fixture::ordered(&keys, long_orders(&orders));
        fixture.layout.page_rows = PAGE;
        fixture.layout.write_batch = PAGE;
        fixture.layout.row_group_rows = group;
        fixture
    }

    /// `fixture` with `extra` more value columns, written and served after
    /// the first.
    fn with_extra(mut fixture: Fixture, extra: usize) -> Fixture {
        fixture.extra_columns = extra;
        fixture
    }

    /// The index of `opened` in `residency` under the ordered layout with
    /// `extra` value columns after the first, a local directory under `dir`
    /// for the Disk tier.
    async fn index_in(
        opened: &Opened,
        residency: Residency,
        dir: &Path,
        extra: usize,
    ) -> FileIndex {
        index_of_type(opened, residency, dir, extra, OrderType::Long).await
    }

    /// `index_in` with an order column of `order_type`.
    async fn index_of_type(
        opened: &Opened,
        residency: Residency,
        dir: &Path,
        extra: usize,
        order_type: OrderType,
    ) -> FileIndex {
        let mut layout = ordered_layout(KeyType::Long, order_type);
        for at in 0..extra {
            let field_id = EXTRA_FIELD_ID + i32::try_from(at).unwrap();
            layout
                .value_columns
                .push(binary_column(&format!("{EXTRA_COLUMN}{at}"), field_id));
        }
        let local = generation_testing::local_dir(dir, opened.stats(), None);
        let local = (residency == Residency::Disk).then_some(&local);
        opened.build_under(&layout, residency, local).await.unwrap()
    }

    /// What a read runs through: a fresh Scan cache and span window.
    struct Reads {
        cache: PageCache,
        window: HedgeWindow,
    }

    impl Reads {
        async fn new() -> Self {
            Self {
                cache: cache().await,
                window: HedgeWindow::new(&HedgeConfig::default()),
            }
        }

        fn source(&self) -> RunSource<'_> {
            RunSource {
                cache: &self.cache,
                fan_out: None,
                window: &self.window,
                value_gets: &[],
                order_gets: None,
            }
        }
    }

    /// The GETs and local reads that reading `pages` key pages costs in
    /// `residency`, with no dictionary page: none in Full, a local read
    /// each in Disk, a GET each in Bloom.
    fn key_page_costs(residency: Residency, pages: u64) -> (u64, u64) {
        match residency {
            Residency::Full => (0, 0),
            Residency::Disk => (0, pages),
            Residency::Bloom => (pages, 0),
        }
    }

    /// The GETs and local reads `stats` counted since `before`.
    fn since(stats: &IoStats, before: (u64, u64)) -> (u64, u64) {
        (stats.gets() - before.0, stats.local_reads() - before.1)
    }

    fn costs(stats: &IoStats) -> (u64, u64) {
        (stats.gets(), stats.local_reads())
    }

    /// The run `index` locates for `key`, and what locating it cost.
    async fn located(index: &FileIndex, stats: &IoStats, key: i64) -> (Option<Run>, (u64, u64)) {
        let reads = Reads::new().await;
        let before = costs(stats);
        let run = index
            .locate_run(&Key::Long(key), reads.source())
            .await
            .unwrap();
        (run, since(stats, before))
    }

    fn part(row_group: usize, rows: Range<i64>) -> RunPart {
        RunPart { row_group, rows }
    }

    fn run_of(parts: Vec<RunPart>) -> Option<Run> {
        let run = Run { parts };
        (!run.parts.is_empty()).then_some(run)
    }

    /// The order value of row `j` of a run.
    fn order_at(j: i64) -> OrderValue {
        OrderValue::Long(j * ORDER_STEP)
    }

    /// What a read of one value column answers for file row `row`, `j`th
    /// of its key's run.
    fn row_answer(row: i64, j: i64, columns: usize) -> (OrderValue, Vec<Option<Bytes>>) {
        let value = Bytes::from(value_for(usize::try_from(row).unwrap()));
        (order_at(j), vec![Some(value); columns])
    }

    fn bounds(direction: Direction) -> ScanBounds {
        ScanBounds {
            direction,
            limit: 0,
            after: None,
            before: None,
        }
    }

    fn none_deleted(_: RowPosition) -> bool {
        false
    }

    /// A run inside one key page is that page's rows of the key: one key
    /// page read in Disk and Bloom, none in Full.
    #[tokio::test]
    async fn test_locate_run_inside_one_page() {
        let group = 12;
        let opened = store(&runs_of(&[(1, 1), (2, 2), (3, 5), (4, 4)], group)).await;
        for residency in Residency::ALL {
            let dir = tempfile::tempdir().unwrap();
            let index = index_in(&opened, residency, dir.path(), 0).await;
            let stats = opened.stats();
            let (run, cost) = located(&index, &stats, 2).await;
            assert_eq!(run, run_of(vec![part(0, 1..3)]), "{residency}");
            assert_eq!(cost, key_page_costs(residency, 1), "{residency}");
        }
    }

    /// A run across four key pages of one row group decodes the first and
    /// the last alone; the pages between hold only the key and are never
    /// read.
    #[tokio::test]
    async fn test_locate_run_spanning_pages_decodes_at_most_two() {
        let group = 16;
        let opened = store(&runs_of(&[(1, 2), (2, 12), (3, 2)], group)).await;
        for residency in Residency::ALL {
            let dir = tempfile::tempdir().unwrap();
            let index = index_in(&opened, residency, dir.path(), 0).await;
            assert_eq!(index.page_count(0), 4);
            let stats = opened.stats();
            let (run, cost) = located(&index, &stats, 2).await;
            assert_eq!(run, run_of(vec![part(0, 2..14)]), "{residency}");
            assert_eq!(cost, key_page_costs(residency, 2), "{residency}");
        }
    }

    /// A run that ends one row group, fills the next, and starts the one
    /// after at row zero is one part per row group, found from the resident
    /// first and last keys and one key page at each end; reading it whole
    /// returns every row in order, either way.
    #[tokio::test]
    async fn test_locate_run_spans_row_groups_from_row_zero() {
        let group = 8;
        let opened = store(&runs_of(&[(1, 5), (2, 16), (3, 3)], group)).await;
        for residency in Residency::ALL {
            let dir = tempfile::tempdir().unwrap();
            let index = index_in(&opened, residency, dir.path(), 0).await;
            let stats = opened.stats();
            let (run, cost) = located(&index, &stats, 2).await;
            let expected = vec![part(0, 5..8), part(1, 0..8), part(2, 0..5)];
            assert_eq!(run, run_of(expected), "{residency}");
            assert_eq!(cost, key_page_costs(residency, 2), "{residency}");
            let run = run.unwrap();
            let reads = Reads::new().await;
            let all: Vec<_> = (5..21).map(|row| row_answer(row, row - 5, 1)).collect();
            let oldest = index
                .read_run(
                    &run,
                    &bounds(Direction::OldestFirst),
                    WHOLE_RUN,
                    &[0],
                    &none_deleted,
                    reads.source(),
                )
                .await
                .unwrap();
            assert_eq!(oldest.rows, all, "{residency}");
            assert_eq!(oldest.next, None);
            let newest = index
                .read_run(
                    &run,
                    &bounds(Direction::NewestFirst),
                    WHOLE_RUN,
                    &[0],
                    &none_deleted,
                    reads.source(),
                )
                .await
                .unwrap();
            assert_eq!(newest.rows, all.into_iter().rev().collect::<Vec<_>>());
            assert_eq!(newest.next, None);
        }
    }

    /// A key that is a page's first key, the row group's first key, or a
    /// fence and the row group's last key is located exactly, the pages a
    /// fence or the resident last key rule out never decoded.
    #[tokio::test]
    async fn test_locate_run_for_a_key_equal_to_a_fence() {
        let group = 12;
        let opened = store(&runs_of(&[(1, 4), (2, 5), (3, 3)], group)).await;
        for residency in Residency::ALL {
            let dir = tempfile::tempdir().unwrap();
            let index = index_in(&opened, residency, dir.path(), 0).await;
            assert_eq!(
                index.fences(),
                [vec![Key::Long(1), Key::Long(2), Key::Long(2)]]
            );
            let stats = opened.stats();
            // Key 2 starts page 1: page 0 is decoded to see the run does
            // not start in it, and page 2 to find its end.
            let (run, cost) = located(&index, &stats, 2).await;
            assert_eq!(run, run_of(vec![part(0, 4..9)]), "{residency}");
            assert_eq!(cost, key_page_costs(residency, 2), "{residency}");
            // Key 1 is the row group's first key: its run starts at row 0.
            let (run, cost) = located(&index, &stats, 1).await;
            assert_eq!(run, run_of(vec![part(0, 0..4)]), "{residency}");
            assert_eq!(cost, key_page_costs(residency, 1), "{residency}");
            // Key 3 is the row group's last key: its run ends the group.
            let (run, cost) = located(&index, &stats, 3).await;
            assert_eq!(run, run_of(vec![part(0, 9..12)]), "{residency}");
            assert_eq!(cost, key_page_costs(residency, 1), "{residency}");
        }
    }

    /// A key outside the file, or between two row groups, costs nothing in
    /// any tier; one inside a row group costs no GET in Full or Disk, and
    /// in Bloom a key-page GET only where the filter passes it.
    #[tokio::test]
    async fn test_locate_run_misses_without_a_get() {
        let group = 12;
        let keys = 12;
        let step = 2;
        let runs: Vec<(i64, usize)> = (0..keys).map(|at| (at * step, 2)).collect();
        let opened = store(&runs_of(&runs, group)).await;
        let keys_per_group = group / 2;
        let between_groups = 11;
        let outside = [-1, keys * step];
        let inside: Vec<i64> = (0..keys - 1)
            .map(|at| at * step + 1)
            .filter(|key| *key != between_groups)
            .collect();
        for residency in Residency::ALL {
            let dir = tempfile::tempdir().unwrap();
            let index = index_in(&opened, residency, dir.path(), 0).await;
            let stats = opened.stats();
            for key in outside.into_iter().chain([between_groups]) {
                let (run, cost) = located(&index, &stats, key).await;
                assert_eq!(run, None, "{residency} {key}");
                assert_eq!(cost, (0, 0), "{residency} {key}: nothing read");
            }
            let filters = opened.filters(&index);
            let mut negatives = 0;
            for &key in &inside {
                let (run, cost) = located(&index, &stats, key).await;
                assert_eq!(run, None, "{residency} {key}");
                let group_of = usize::try_from(key / step).unwrap() / keys_per_group;
                let passes = filters[group_of].check(&*Key::Long(key).as_bytes());
                negatives += usize::from(!passes);
                let expected = match residency {
                    Residency::Full => (0, 0),
                    Residency::Disk => (0, 1),
                    Residency::Bloom => (u64::from(passes), 0),
                };
                assert_eq!(cost, expected, "{residency} {key}");
            }
            assert!(negatives >= 1, "no probed key was a filter negative");
        }
    }

    /// The rows of the fixture most read tests share: key 2's run of 14
    /// rows, file rows 1 to 14 across four pages of one row group of 16,
    /// between one row of key 1 and one of key 3.
    fn one_long_run(extra: usize) -> Fixture {
        with_extra(runs_of(&[(1, 1), (2, 14), (3, 1)], 16), extra)
    }

    /// The `j`th row of `one_long_run`'s run is file row `j + 1`.
    const FIRST_RUN_ROW: i64 = 1;

    /// Newest first, the rows taken are the last live ones, deleted rows
    /// passed over with no I/O, and the GETs are one span of order pages
    /// and one of value pages: none for the order column under Disk.
    #[tokio::test]
    async fn test_read_run_newest_first_takes_the_last_live_rows() {
        let opened = store(&one_long_run(0)).await;
        let deleted: BTreeSet<u64> = [14, 12].into_iter().collect();
        let is_deleted = |position: RowPosition| deleted.contains(&position.0);
        let limit = 3;
        for residency in Residency::ALL {
            let dir = tempfile::tempdir().unwrap();
            let index = index_in(&opened, residency, dir.path(), 0).await;
            let stats = opened.stats();
            let reads = Reads::new().await;
            let run = index
                .locate_run(&Key::Long(2), reads.source())
                .await
                .unwrap()
                .unwrap();
            let before = stats.gets();
            let got = index
                .read_run(
                    &run,
                    &bounds(Direction::NewestFirst),
                    limit,
                    &[0],
                    &is_deleted,
                    reads.source(),
                )
                .await
                .unwrap();
            let expected: Vec<_> = [13, 11, 10]
                .into_iter()
                .map(|row| row_answer(row, row - FIRST_RUN_ROW, 1))
                .collect();
            assert_eq!(got.rows, expected, "{residency}");
            assert_eq!(got.next, Some(order_at(9 - FIRST_RUN_ROW)));
            assert!(got.from_store);
            let order_gets = u64::from(residency != Residency::Disk);
            assert_eq!(stats.gets() - before, order_gets + 1, "{residency}");

            let before = stats.gets();
            let oldest = index
                .read_run(
                    &run,
                    &bounds(Direction::OldestFirst),
                    limit,
                    &[0],
                    &is_deleted,
                    reads.source(),
                )
                .await
                .unwrap();
            let expected: Vec<_> = [1, 2, 3]
                .into_iter()
                .map(|row| row_answer(row, row - FIRST_RUN_ROW, 1))
                .collect();
            assert_eq!(oldest.rows, expected, "{residency}");
            assert_eq!(oldest.next, Some(order_at(4 - FIRST_RUN_ROW)));
            assert_eq!(stats.gets() - before, order_gets + 1, "{residency}");
        }
    }

    /// `next` is the next live row past deleted ones, read on an order page
    /// no row taken lies on, with no value page of it; a limit that takes
    /// every live row leaves it unset; a read again is warm.
    #[tokio::test]
    async fn test_read_run_reports_the_next_live_row_for_more() {
        let opened = store(&one_long_run(0)).await;
        let deleted: BTreeSet<u64> = (8..=12).collect();
        let is_deleted = |position: RowPosition| deleted.contains(&position.0);
        let live = 14 - deleted.len();
        for residency in Residency::ALL {
            let dir = tempfile::tempdir().unwrap();
            let index = index_in(&opened, residency, dir.path(), 0).await;
            let stats = opened.stats();
            let reads = Reads::new().await;
            let run = index
                .locate_run(&Key::Long(2), reads.source())
                .await
                .unwrap()
                .unwrap();
            let newest = bounds(Direction::NewestFirst);
            let before = costs(&stats);
            let got = index
                .read_run(&run, &newest, 2, &[0], &is_deleted, reads.source())
                .await
                .unwrap();
            let expected: Vec<_> = [14, 13]
                .into_iter()
                .map(|row| row_answer(row, row - FIRST_RUN_ROW, 1))
                .collect();
            assert_eq!(got.rows, expected, "{residency}");
            assert_eq!(got.next, Some(order_at(7 - FIRST_RUN_ROW)), "{residency}");
            // Order pages 1 to 3 are one span; the value page is page 3's.
            let (gets, local) = since(&stats, before);
            match residency {
                Residency::Full | Residency::Bloom => assert_eq!((gets, local), (2, 0)),
                Residency::Disk => assert_eq!((gets, local), (1, 1)),
            }
            let before = costs(&stats);
            let again = index
                .read_run(&run, &newest, 2, &[0], &is_deleted, reads.source())
                .await
                .unwrap();
            assert_eq!(again.rows, got.rows);
            assert_eq!(again.next, got.next);
            assert_eq!(since(&stats, before).0, 0, "{residency}: warm");
            assert!(!again.from_store, "{residency}");

            let whole = index
                .read_run(
                    &run,
                    &newest,
                    u32::try_from(live).unwrap(),
                    &[0],
                    &is_deleted,
                    reads.source(),
                )
                .await
                .unwrap();
            assert_eq!(whole.rows.len(), live);
            assert_eq!(whole.next, None, "{residency}");
            let none = index
                .read_run(&run, &newest, 0, &[0], &is_deleted, reads.source())
                .await
                .unwrap();
            assert_eq!(none.rows, []);
            assert_eq!(none.next, Some(order_at(14 - FIRST_RUN_ROW)));
        }
    }

    /// The fixture of the bounded reads: key 2's run of 30 rows, file rows
    /// 1 to 30 across eight pages of one row group, with `extra` value
    /// columns after the first.
    fn bounded_run(extra: usize) -> Fixture {
        with_extra(runs_of(&[(1, 1), (2, 30), (3, 1)], 32), extra)
    }

    fn after_answer(columns: usize) -> Vec<(OrderValue, Vec<Option<Bytes>>)> {
        [20, 21]
            .into_iter()
            .map(|j| row_answer(j + FIRST_RUN_ROW, j, columns))
            .collect()
    }

    /// With a column index, a bounded read places the bound by it and reads
    /// the order pages and every requested column's pages together: one
    /// GET per column and one for the order column, all in flight at once,
    /// the order column read locally under Disk.
    #[tokio::test]
    async fn test_read_run_with_a_bound_reads_one_round_with_a_column_index() {
        let extra = 1;
        let columns = [0, 1];
        let (opened, gauged) = store_gauged(&bounded_run(extra)).await;
        for residency in Residency::ALL {
            let dir = tempfile::tempdir().unwrap();
            let index = index_in(&opened, residency, dir.path(), extra).await;
            let stats = opened.stats();
            let reads = Reads::new().await;
            let order_gets = usize::from(residency != Residency::Disk);
            let key_page_gets = usize::from(residency == Residency::Bloom) * 2;
            let before = stats.gets();
            let together = columns.len() + order_gets;
            gauged.fill_to(key_page_gets.max(1));
            let run = index
                .locate_run(&Key::Long(2), reads.source())
                .await
                .unwrap()
                .unwrap();
            gauged.fill_to(together);
            let got = tokio::time::timeout(DEADLINE_GUARD, async {
                let after = ScanBounds {
                    after: Some(order_at(19)),
                    ..bounds(Direction::OldestFirst)
                };
                index
                    .read_run(&run, &after, 2, &columns, &none_deleted, reads.source())
                    .await
                    .unwrap()
            })
            .await
            .expect("every GET of the read is in flight at once");
            assert_eq!(got.rows, after_answer(columns.len()), "{residency}");
            assert_eq!(got.next, Some(order_at(22)));
            assert_eq!(gauged.high_water(), together, "{residency}");
            assert_eq!(
                stats.gets() - before,
                count(key_page_gets + together),
                "{residency}"
            );
        }
    }

    /// Newest first before a bound, the run's last page, which holds the
    /// next key's rows and whose entry spans their values too, is ruled out
    /// by the page before it, which holds the run alone: the read is one
    /// span of order pages and one of value pages.
    #[tokio::test]
    async fn test_read_run_newest_first_before_a_bound_rules_out_the_run_edge_page() {
        let opened = store(&bounded_run(0)).await;
        for residency in Residency::ALL {
            let dir = tempfile::tempdir().unwrap();
            let index = index_in(&opened, residency, dir.path(), 0).await;
            let stats = opened.stats();
            let reads = Reads::new().await;
            let run = index
                .locate_run(&Key::Long(2), reads.source())
                .await
                .unwrap()
                .unwrap();
            let before_ten = ScanBounds {
                before: Some(order_at(10)),
                ..bounds(Direction::NewestFirst)
            };
            let before = costs(&stats);
            let bytes_before = stats.bytes();
            let got = index
                .read_run(&run, &before_ten, 2, &[0], &none_deleted, reads.source())
                .await
                .unwrap();
            let expected: Vec<_> = [9, 8]
                .into_iter()
                .map(|j| row_answer(j + FIRST_RUN_ROW, j, 1))
                .collect();
            assert_eq!(got.rows, expected, "{residency}");
            assert_eq!(got.next, Some(order_at(7)));
            let (gets, local) = since(&stats, before);
            match residency {
                Residency::Full | Residency::Bloom => assert_eq!((gets, local), (2, 0)),
                Residency::Disk => assert_eq!((gets, local), (1, 1)),
            }
            // Pages 1 and 2 of each column hold rows 4 to 11, every row
            // read; none of the run's last page.
            let read = 1..3;
            let value_bytes = page_bytes(&index, index.value_leaf(0).unwrap(), read.clone());
            let order_bytes = page_bytes(&index, index.order_leaf().unwrap(), read);
            let order_bytes_fetched = if residency == Residency::Disk {
                0
            } else {
                order_bytes
            };
            assert_eq!(
                stats.bytes() - bytes_before,
                value_bytes + order_bytes_fetched,
                "{residency}"
            );
        }
    }

    /// The bytes data pages `pages` of leaf `leaf` take in row group 0.
    fn page_bytes(index: &FileIndex, leaf: usize, pages: Range<usize>) -> u64 {
        let locations = index.metadata().offset_index().unwrap()[0][leaf].page_locations();
        locations[pages]
            .iter()
            .map(|location| u64::try_from(location.compressed_page_size).unwrap())
            .sum()
    }

    /// Dictionary-encoded columns cost their dictionary page beside each
    /// span, the key column's under Disk and Bloom, the order column's
    /// locally under Disk; a null value is `None`.
    #[tokio::test]
    async fn test_read_run_reads_each_dictionary_page_beside_its_span_and_a_null_as_none() {
        let mut fixture = one_long_run(0);
        fixture.layout.dictionary = generation_testing::Dictionary::All;
        let null_row = 13;
        fixture.values[usize::try_from(null_row).unwrap()] = None;
        let opened = store(&fixture).await;
        for residency in Residency::ALL {
            let dir = tempfile::tempdir().unwrap();
            let index = index_in(&opened, residency, dir.path(), 0).await;
            assert!(
                index
                    .metadata()
                    .row_group(0)
                    .column(index.key_leaf())
                    .dictionary_page_offset()
                    .is_some()
            );
            let stats = opened.stats();
            let (run, cost) = located(&index, &stats, 2).await;
            let pages_and_dictionary = 3;
            assert_eq!(
                cost,
                key_page_costs(residency, pages_and_dictionary),
                "{residency}"
            );
            let run = run.unwrap();
            let reads = Reads::new().await;
            let before = costs(&stats);
            let got = index
                .read_run(
                    &run,
                    &bounds(Direction::NewestFirst),
                    3,
                    &[0],
                    &none_deleted,
                    reads.source(),
                )
                .await
                .unwrap();
            let expected: Vec<_> = [14, 13, 12]
                .into_iter()
                .map(|row| {
                    let (order, values) = row_answer(row, row - FIRST_RUN_ROW, 1);
                    (order, if row == null_row { vec![None] } else { values })
                })
                .collect();
            assert_eq!(got.rows, expected, "{residency}");
            assert_eq!(got.next, Some(order_at(11 - FIRST_RUN_ROW)));
            let (gets, local) = since(&stats, before);
            match residency {
                Residency::Full | Residency::Bloom => assert_eq!((gets, local), (4, 0)),
                Residency::Disk => assert_eq!((gets, local), (2, 2)),
            }
        }
    }

    /// Without a column index on the order column, a bounded read takes
    /// the run's order pages first, then exactly the value pages of the
    /// rows it takes: two rounds, never two GETs at once.
    #[tokio::test]
    async fn test_read_run_without_an_order_column_index_reads_two_rounds() {
        let mut fixture = bounded_run(0);
        fixture.layout.statistics = parquet::file::properties::EnabledStatistics::Chunk;
        let (opened, gauged) = store_gauged(&fixture).await;
        for residency in Residency::ALL {
            let dir = tempfile::tempdir().unwrap();
            let index = index_in(&opened, residency, dir.path(), 0).await;
            let order = index.order_values(index.order_leaf_of()).group(0);
            assert!(order_column_index(order).is_none());
            let stats = opened.stats();
            let reads = Reads::new().await;
            let run = index
                .locate_run(&Key::Long(2), reads.source())
                .await
                .unwrap()
                .unwrap();
            gauged.fill_to(1);
            let before = costs(&stats);
            let got = read_after_run(&index, &run, &reads).await;
            assert_eq!(got.rows, after_answer(1), "{residency}");
            assert_eq!(got.next, Some(order_at(22)));
            let (gets, local) = since(&stats, before);
            match residency {
                Residency::Full | Residency::Bloom => assert_eq!((gets, local), (2, 0)),
                Residency::Disk => assert_eq!((gets, local), (1, 1)),
            }
            assert_eq!(gauged.high_water(), 1, "{residency}");
        }
    }

    /// Oldest first after the order value of the run's row 19, one column:
    /// rows 20 and 21 of the run, then row 22's order value.
    async fn read_after_run(index: &FileIndex, run: &Run, reads: &Reads) -> FileRows {
        let after = ScanBounds {
            after: Some(order_at(19)),
            ..bounds(Direction::OldestFirst)
        };
        index
            .read_run(run, &after, 2, &[0], &none_deleted, reads.source())
            .await
            .unwrap()
    }

    /// Every GET of a read counts once in its column's counter, the order
    /// column's in its own, and holds one permit of the fan-out while it
    /// runs, so a fan-out of one runs them one at a time and none waits for
    /// a permit while holding one.
    #[tokio::test]
    async fn test_read_run_counts_each_get_under_its_column_and_holds_one_permit() {
        let extra = 1;
        let (opened, gauged) = store_gauged(&one_long_run(extra)).await;
        let dir = tempfile::tempdir().unwrap();
        let index = index_in(&opened, Residency::Full, dir.path(), extra).await;
        let stats = opened.stats();
        let counters: Vec<Arc<AtomicU64>> = (0..3).map(|_| Arc::new(AtomicU64::new(0))).collect();
        let handles: Vec<Counter> = counters
            .iter()
            .map(|counter| Counter::from_arc(Arc::clone(counter)))
            .collect();
        let fan_out = Arc::new(Semaphore::new(1));
        let reads = Reads::new().await;
        let source = RunSource {
            fan_out: Some(&fan_out),
            value_gets: &handles[..2],
            order_gets: Some(&handles[2]),
            ..reads.source()
        };
        let run = index
            .locate_run(&Key::Long(2), source)
            .await
            .unwrap()
            .unwrap();
        gauged.fill_to(1);
        let before = stats.gets();
        let got = index
            .read_run(
                &run,
                &bounds(Direction::OldestFirst),
                2,
                &[0, 1],
                &none_deleted,
                source,
            )
            .await
            .unwrap();
        let expected: Vec<_> = [0, 1]
            .into_iter()
            .map(|j| row_answer(j + FIRST_RUN_ROW, j, 2))
            .collect();
        assert_eq!(got.rows, expected);
        assert_eq!(stats.gets() - before, 3);
        let counted: Vec<u64> = counters
            .iter()
            .map(|counter| counter.load(Ordering::Relaxed))
            .collect();
        assert_eq!(counted, [1, 1, 1], "each column's GET counted once");
        assert_eq!(gauged.high_water(), 1);
        assert_eq!(fan_out.available_permits(), 1, "every permit is back");
    }

    /// Bytes every byte-array order value of `prefixed_run` shares: longer
    /// than `TRUNCATED_INDEX_BYTES`, so every truncated column index entry
    /// of the order column is one and the same prefix, its maximum that
    /// prefix incremented, above every value of the column.
    const SHARED_PREFIX_BYTES: usize = 32;
    /// The column index truncation of `prefixed_run`.
    const TRUNCATED_INDEX_BYTES: usize = 16;
    /// The byte the shared prefix repeats.
    const PREFIX_BYTE: u8 = b'p';
    /// Rows of key 2's run in `prefixed_run`.
    const PREFIXED_RUN_ROWS: usize = 30;

    /// The `j`th byte-array order value of a run in `prefixed_run`: the
    /// shared prefix, then `j` in four digits, ascending with `j`.
    fn prefixed(j: usize) -> Vec<u8> {
        let mut value = vec![PREFIX_BYTE; SHARED_PREFIX_BYTES];
        value.extend_from_slice(format!("{j:04}").as_bytes());
        value
    }

    /// The order value `bytes` travel as under `order_type`.
    fn travelling(order_type: OrderType, bytes: Vec<u8>) -> OrderValue {
        match order_type {
            OrderType::String => OrderValue::Str(String::from_utf8(bytes).unwrap()),
            OrderType::Int
            | OrderType::Long
            | OrderType::Date
            | OrderType::Timestamp
            | OrderType::TimestampTz
            | OrderType::TimestampNs
            | OrderType::TimestampTzNs
            | OrderType::Binary
            | OrderType::Fixed(_) => OrderValue::Bytes(bytes),
        }
    }

    /// Key 2's run of `run_rows` rows between one row each of keys 1 and
    /// 3, in pages of `PAGE` in one row group, every key's `j`th row
    /// ordered by `prefixed(j)`, with the column index truncated to
    /// `truncate` bytes, `None` leaving it whole.
    fn prefixed_run(truncate: Option<usize>, run_rows: usize) -> Fixture {
        prefixed_run_of(truncate, run_rows, prefixed)
    }

    /// `prefixed_run` with every key's `j`th row ordered by `value(j)`.
    fn prefixed_run_of(
        truncate: Option<usize>,
        run_rows: usize,
        value: fn(usize) -> Vec<u8>,
    ) -> Fixture {
        let runs = [(1, 1), (2, run_rows), (3, 1)];
        let mut keys = Vec::new();
        let mut orders = Vec::new();
        for (key, rows) in runs {
            for j in 0..rows {
                keys.push(key);
                orders.push(Some(value(j)));
            }
        }
        let mut fixture = Fixture::ordered(&keys, Orders::Bytes(orders));
        fixture.layout.page_rows = PAGE;
        fixture.layout.write_batch = PAGE;
        fixture.layout.row_group_rows = keys.len();
        fixture.layout.truncate = truncate;
        fixture
    }

    /// A bounded read of a string or binary order column, whole or under a
    /// truncated column index, answers what the model does in both
    /// directions: no row inside the bounds is dropped. A truncated entry
    /// is a bound on its own page alone, never a page's exact extremes, so
    /// a run-edge page beside it is not ruled out by it.
    #[tokio::test]
    async fn test_read_run_bounds_byte_array_orders_under_a_truncated_index() {
        let limit = 3;
        let last = PREFIXED_RUN_ROWS - 1;
        for truncate in [Some(TRUNCATED_INDEX_BYTES), None] {
            let opened = store(&prefixed_run(truncate, PREFIXED_RUN_ROWS)).await;
            for order_type in [OrderType::String, OrderType::Binary] {
                let value = |j: usize| travelling(order_type, prefixed(j));
                // Newest first below the run's last value, and oldest first
                // above its first: each bound sits on a run-edge page.
                let cases = [
                    (
                        ScanBounds {
                            before: Some(value(last)),
                            ..bounds(Direction::NewestFirst)
                        },
                        vec![last - 1, last - 2, last - 3],
                        last - 4,
                    ),
                    (
                        ScanBounds {
                            after: Some(value(0)),
                            ..bounds(Direction::OldestFirst)
                        },
                        vec![1, 2, 3],
                        4,
                    ),
                ];
                for residency in Residency::ALL {
                    let dir = tempfile::tempdir().unwrap();
                    let index = index_of_type(&opened, residency, dir.path(), 0, order_type).await;
                    let reads = Reads::new().await;
                    let run = index
                        .locate_run(&Key::Long(2), reads.source())
                        .await
                        .unwrap()
                        .unwrap();
                    for (scan, taken, next) in &cases {
                        let got = index
                            .read_run(&run, scan, limit, &[0], &none_deleted, reads.source())
                            .await
                            .unwrap();
                        let expected: Vec<_> = taken
                            .iter()
                            .map(|&j| {
                                let row = FIRST_RUN_ROW + i64::try_from(j).unwrap();
                                let value = Bytes::from(value_for(usize::try_from(row).unwrap()));
                                (travelling(order_type, prefixed(j)), vec![Some(value)])
                            })
                            .collect();
                        let case = format!("{residency} {order_type} {truncate:?} {scan:?}");
                        assert_eq!(got.rows, expected, "{case}");
                        assert_eq!(got.next, Some(value(*next)), "{case}");
                    }
                }
            }
        }
    }

    /// Rows of the long runs the bounded probe reads read: many pages
    /// beyond the few a small limit's rows lie on.
    const LONG_PREFIXED_RUN_ROWS: usize = 120;

    /// Pages of each column a bounded read may take beyond the near end of
    /// the rows the limit selects, the end its direction starts from: the
    /// rest of the order page that holds the bound and one run-edge page,
    /// whose minimum and maximum include a neighbouring key's values (RFD
    /// 025, Cost). Where the limit selects no row, the pages a read may
    /// take per bound.
    const BEYOND_THE_LIMIT_PAGES: usize = 2;
    /// Pages of each column a one-round read may take beyond the far end
    /// of the rows the limit selects. Rows across a bound count nothing
    /// toward the limit, so the walk takes as many more live rows past
    /// that end as it took inside the bounds across a bound near it, those
    /// of at most `PAGES_ACROSS_A_BOUND` page holding only the run per
    /// bound and of one run-edge page; and where the limit takes every
    /// live row inside, it reads on over the page that holds the far bound
    /// and the far run-edge page. Counted in pages, which holds while no
    /// deleted rows fill a page there, as in the cost model's fixtures.
    const SHIFTED_PAST_THE_LIMIT_PAGES: usize = 2;
    /// The limits the cost model reads every bound under: none, one, a
    /// few, and more than any run holds.
    const COST_LIMITS: [u32; 4] = [0, 1, 3, WHOLE_RUN];
    /// How many bound points apart the two bounds of a read with both lie.
    const BOUND_GAPS: [usize; 2] = [1, 4];
    /// Half of `ORDER_STEP`: an order value between two rows of a run.
    const HALF_STEP: i64 = ORDER_STEP / 2;
    /// Every how many rows of a run the cost model deletes one, besides the
    /// run's first and last.
    const DELETE_EVERY: usize = 5;
    /// Which row of every `DELETE_EVERY` the cost model deletes.
    const DELETED_OF_EVERY: usize = 2;
    /// A byte above every byte the order values of `prefixed_run` start
    /// with, so a bound of it alone lies above every value.
    const ABOVE_EVERY_PREFIX: u8 = b'z';
    /// The byte the first `LOW_PREFIX_ROWS` values of `halved` repeat,
    /// below `PREFIX_BYTE`.
    const LOW_PREFIX_BYTE: u8 = b'a';
    /// Rows of a run whose order values `halved` starts with
    /// `LOW_PREFIX_BYTE`: a page's worth short of four pages, so the run's
    /// first page holds the previous key's row too.
    const LOW_PREFIX_ROWS: usize = 15;

    /// The `j`th order value of a run whose first `LOW_PREFIX_ROWS` rows
    /// share one prefix and the rest another: a 16-byte truncated entry of
    /// a page of either half is one prefix, which places a bound of the
    /// other half exactly.
    fn halved(j: usize) -> Vec<u8> {
        split_at(LOW_PREFIX_ROWS, j)
    }

    /// Rows of a run whose order values `two_low_pages` starts with
    /// `LOW_PREFIX_BYTE`: the run's first page, which holds the previous
    /// key's row too, and exactly two whole pages.
    const TWO_LOW_PAGES_ROWS: usize = 11;

    /// The `j`th order value of a run whose first `TWO_LOW_PAGES_ROWS` rows
    /// share one prefix and the rest another, as `halved`'s do.
    fn two_low_pages(j: usize) -> Vec<u8> {
        split_at(TWO_LOW_PAGES_ROWS, j)
    }

    /// The `j`th order value of a run whose first `low_rows` rows repeat
    /// `LOW_PREFIX_BYTE` and the rest `PREFIX_BYTE`, then `j` in four
    /// digits.
    fn split_at(low_rows: usize, j: usize) -> Vec<u8> {
        let byte = if j < low_rows {
            LOW_PREFIX_BYTE
        } else {
            PREFIX_BYTE
        };
        let mut value = vec![byte; SHARED_PREFIX_BYTES];
        value.extend_from_slice(format!("{j:04}").as_bytes());
        value
    }

    /// A page of one column: its row group and its index in the chunk.
    type PageAt = (usize, usize);

    /// The page of leaf `leaf` that holds `row`.
    fn page_of(index: &FileIndex, leaf: usize, row: RowRef) -> PageAt {
        let locations =
            index.metadata().offset_index().unwrap()[row.row_group][leaf].page_locations();
        let page = locations.partition_point(|location| location.first_row_index <= row.row) - 1;
        (row.row_group, page)
    }

    /// The pages of leaf `leaf` holding `rows`, in file order, each once.
    fn pages_of(index: &FileIndex, leaf: usize, rows: &[RowRef]) -> Vec<PageAt> {
        let mut pages: Vec<PageAt> = rows.iter().map(|&row| page_of(index, leaf, row)).collect();
        pages.dedup();
        pages
    }

    /// The pages of leaf `leaf` that GETs over `ranges` fetched.
    fn fetched(index: &FileIndex, leaf: usize, ranges: &[Range<u64>]) -> BTreeSet<PageAt> {
        let mut pages = BTreeSet::new();
        let offsets = index.metadata().offset_index().unwrap();
        for (row_group, columns) in offsets.iter().enumerate() {
            for (page, location) in columns[leaf].page_locations().iter().enumerate() {
                let start = u64::try_from(location.offset).unwrap();
                let end = start + u64::try_from(location.compressed_page_size).unwrap();
                if ranges
                    .iter()
                    .any(|range| range.start <= start && end <= range.end)
                {
                    pages.insert((row_group, page));
                }
            }
        }
        pages
    }

    /// The bytes pages `pages` of leaf `leaf` take.
    fn bytes_of(index: &FileIndex, leaf: usize, pages: &BTreeSet<PageAt>) -> u64 {
        let offsets = index.metadata().offset_index().unwrap();
        pages
            .iter()
            .map(|&(row_group, page)| {
                let location = &offsets[row_group][leaf].page_locations()[page];
                u64::try_from(location.compressed_page_size).unwrap()
            })
            .sum()
    }

    /// The rows of `run`, in file order.
    fn rows_of(run: &Run) -> Vec<RowRef> {
        run.parts
            .iter()
            .flat_map(|part| {
                part.rows.clone().map(|row| RowRef {
                    row_group: part.row_group,
                    row,
                })
            })
            .collect()
    }

    /// What the model answers one read of a run: the rows and `next` the
    /// read answers, the run positions of the rows taken, and the rows the
    /// limit selects as run positions, from the first inside the bounds in
    /// the read's direction: for the order column through the row that
    /// sets `next`, and for the value columns through the last row taken;
    /// through the last row inside the bounds where nothing sets `next`, or
    /// for the value columns where the limit takes every live row.
    struct Model {
        rows: Vec<(OrderValue, Vec<Option<Bytes>>)>,
        next: Option<OrderValue>,
        taken: Vec<usize>,
        orders: Option<RangeInclusive<usize>>,
        values: Option<RangeInclusive<usize>>,
    }

    /// The model of reading the run `rows`, ordered by `orders`, under
    /// `scan` and `limit`, with the run positions `deleted` deleted.
    fn model(
        index: &FileIndex,
        rows: &[RowRef],
        orders: &[OrderValue],
        deleted: &BTreeSet<usize>,
        scan: &ScanBounds,
        limit: u32,
    ) -> Model {
        let cut = Cut {
            after: scan.after.as_ref(),
            before: scan.before.as_ref(),
        };
        let limit = usize::try_from(limit).unwrap();
        let mut walk: Vec<usize> = (0..rows.len()).collect();
        if scan.direction == Direction::NewestFirst {
            walk.reverse();
        }
        let inside: Vec<usize> = walk
            .into_iter()
            .filter(|&at| cut.holds(&orders[at]))
            .collect();
        let live: Vec<usize> = inside
            .iter()
            .copied()
            .filter(|at| !deleted.contains(at))
            .collect();
        let taken: Vec<usize> = live.iter().copied().take(limit).collect();
        let more = live.get(limit).copied();
        let from_first = |last: Option<usize>| {
            inside
                .first()
                .zip(last)
                .map(|(&first, last)| first.min(last)..=first.max(last))
        };
        let last_inside = inside.last().copied();
        let values = if taken.is_empty() {
            None
        } else if taken.len() < limit {
            from_first(last_inside)
        } else {
            from_first(taken.last().copied())
        };
        let answer = |at: usize| {
            let position = usize::try_from(index.position(rows[at]).0).unwrap();
            let value = Bytes::from(value_for(position));
            (orders[at].clone(), vec![Some(value)])
        };
        Model {
            rows: taken.iter().map(|&at| answer(at)).collect(),
            next: more.map(|at| orders[at].clone()),
            orders: from_first(more.or(last_inside)),
            values,
            taken,
        }
    }

    /// Asserts that the pages `read` of leaf `leaf` lie among the pages of
    /// the run `rows` that hold the rows `selected` names,
    /// `BEYOND_THE_LIMIT_PAGES` more before them in `scan`'s direction and
    /// `SHIFTED_PAST_THE_LIMIT_PAGES` more after them; where it names none,
    /// that they are no more than `BEYOND_THE_LIMIT_PAGES` per bound `scan`
    /// sets.
    fn assert_within_the_promise(
        index: &FileIndex,
        leaf: usize,
        rows: &[RowRef],
        selected: Option<&RangeInclusive<usize>>,
        scan: &ScanBounds,
        read: &BTreeSet<PageAt>,
        case: &str,
    ) {
        let run = pages_of(index, leaf, rows);
        let Some(selected) = selected else {
            let bounds_set = usize::from(scan.after.is_some()) + usize::from(scan.before.is_some());
            assert!(
                read.len() <= BEYOND_THE_LIMIT_PAGES * bounds_set,
                "{case}: read {read:?} with no row selected"
            );
            return;
        };
        let at = |row: usize| {
            let page = page_of(index, leaf, rows[row]);
            run.iter().position(|held| *held == page).unwrap()
        };
        let (below, above) = match scan.direction {
            Direction::OldestFirst => (BEYOND_THE_LIMIT_PAGES, SHIFTED_PAST_THE_LIMIT_PAGES),
            Direction::NewestFirst => (SHIFTED_PAST_THE_LIMIT_PAGES, BEYOND_THE_LIMIT_PAGES),
        };
        let low = at(*selected.start()).saturating_sub(below);
        let high = (at(*selected.end()) + above).min(run.len() - 1);
        let allowed: BTreeSet<PageAt> = run[low..=high].iter().copied().collect();
        assert!(
            read.is_subset(&allowed),
            "{case}: read {read:?}, beyond {allowed:?}"
        );
    }

    /// Where the order column index entry of page `page` places its values
    /// against `scan`'s bounds.
    fn entry_fit(index: &FileIndex, (row_group, page): PageAt, scan: &ScanBounds) -> PageFit {
        let entry = order_column_index(index.order_values(index.order_leaf_of()).group(row_group));
        ValueRange::of_page(entry, page).fit(scan.after.as_ref(), scan.before.as_ref())
    }

    /// The order pages a read under a byte-array order column index needs
    /// to place `limit` rows and `next`: every page of the run `rows` whose
    /// entry lies across a bound of `scan`, and from the direction's end
    /// the pages whose entries place them inside the bounds, up to the one
    /// that holds the live row past the first `limit` live rows on them;
    /// `deleted` holds run positions.
    fn placing(
        index: &FileIndex,
        rows: &[RowRef],
        scan: &ScanBounds,
        limit: u32,
        deleted: &BTreeSet<usize>,
    ) -> BTreeSet<PageAt> {
        let leaf = index.order_leaf().unwrap();
        let mut pages = pages_of(index, leaf, rows);
        let mut placed: BTreeSet<PageAt> = pages
            .iter()
            .copied()
            .filter(|&page| matches!(entry_fit(index, page, scan), PageFit::Straddles { .. }))
            .collect();
        if scan.direction == Direction::NewestFirst {
            pages.reverse();
        }
        let mut live = 0;
        for page in pages {
            if live > limit {
                break;
            }
            if entry_fit(index, page, scan) == PageFit::Inside {
                placed.insert(page);
                let on_page = (0..rows.len())
                    .filter(|&at| page_of(index, leaf, rows[at]) == page && !deleted.contains(&at));
                live += u32::try_from(on_page.count()).unwrap();
            }
        }
        placed
    }

    /// Every bounded read the cost model runs at `points`, ascending order
    /// values whose first lies below and whose last above every value of
    /// the run: in each direction, each point as `after` and as `before`,
    /// each with the other bound at the extreme point beyond it, which
    /// excludes nothing, and each pair `BOUND_GAPS` apart as both.
    fn bounded_scans(points: &[OrderValue]) -> Vec<ScanBounds> {
        let (first, last) = (points[0].clone(), points[points.len() - 1].clone());
        let mut cuts = Vec::new();
        for (at, point) in points.iter().enumerate() {
            cuts.push((Some(point.clone()), None));
            cuts.push((None, Some(point.clone())));
            cuts.push((Some(point.clone()), Some(last.clone())));
            cuts.push((Some(first.clone()), Some(point.clone())));
            for gap in BOUND_GAPS {
                if let Some(upper) = points.get(at + gap) {
                    cuts.push((Some(point.clone()), Some(upper.clone())));
                }
            }
        }
        [Direction::OldestFirst, Direction::NewestFirst]
            .into_iter()
            .flat_map(|direction| {
                cuts.iter().map(move |(after, before)| ScanBounds {
                    after: after.clone(),
                    before: before.clone(),
                    ..bounds(direction)
                })
            })
            .collect()
    }

    /// The run positions the cost model deletes from a run of `rows` rows:
    /// its first and last, and one of every `DELETE_EVERY` between.
    fn some_deleted(rows: usize) -> BTreeSet<usize> {
        (0..rows)
            .filter(|&at| at == 0 || at == rows - 1 || at % DELETE_EVERY == DELETED_OF_EVERY)
            .collect()
    }

    /// A file the cost model reads: key 2's run, whose `j`th row has order
    /// value `value(j)` under `order_type`, read at the bound values
    /// `points`; `exact` where its order column index is integer or full
    /// length.
    struct CostFixture {
        fixture: Fixture,
        order_type: OrderType,
        value: Box<dyn Fn(usize) -> OrderValue>,
        points: Vec<OrderValue>,
        exact: bool,
    }

    /// A run of an index whose file is read through `gauged`.
    #[derive(Clone, Copy)]
    struct Probe<'a> {
        index: &'a FileIndex,
        gauged: &'a Gauged,
        run: &'a Run,
    }

    impl Probe<'_> {
        /// One read on a fresh cache, and the byte ranges of its GETs.
        async fn read(
            self,
            scan: &ScanBounds,
            limit: u32,
            deleted: &(dyn Fn(RowPosition) -> bool + Sync),
        ) -> (FileRows, Vec<Range<u64>>) {
            self.gauged.take_ranges();
            let reads = Reads::new().await;
            let got = self
                .index
                .read_run(self.run, scan, limit, &[0], deleted, reads.source())
                .await
                .unwrap();
            (got, self.gauged.take_ranges())
        }

        /// Asserts that the read of `scan` runs its `gets` GETs in one
        /// round, all in flight at once, on a fresh cache.
        async fn assert_one_round(
            self,
            scan: &ScanBounds,
            limit: u32,
            deleted: &(dyn Fn(RowPosition) -> bool + Sync),
            gets: usize,
            case: &str,
        ) {
            let reads = Reads::new().await;
            self.gauged.fill_to(gets);
            tokio::time::timeout(
                DEADLINE_GUARD,
                self.index
                    .read_run(self.run, scan, limit, &[0], deleted, reads.source()),
            )
            .await
            .unwrap_or_else(|_| panic!("{case}: the read's {gets} GETs are not in flight at once"))
            .unwrap();
            self.gauged.take_ranges();
        }
    }

    /// The pages of leaf `leaf` holding the rows of the run `rows` from the
    /// first to the last of the run positions `taken`; none for none.
    fn taken_pages(
        index: &FileIndex,
        leaf: usize,
        rows: &[RowRef],
        taken: &[usize],
    ) -> BTreeSet<PageAt> {
        match (taken.iter().min(), taken.iter().max()) {
            (Some(&first), Some(&last)) => pages_of(index, leaf, &rows[first..=last])
                .into_iter()
                .collect(),
            (None | Some(_), None | Some(_)) => BTreeSet::new(),
        }
    }

    /// Reads `cost`'s run at every bounded read of its points, every limit
    /// of `COST_LIMITS`, with no row deleted and with `some_deleted`, in
    /// every residency, and asserts the rows and `next` are the model's
    /// and the pages read in each column are within RFD 025's promise
    /// (`assert_within_the_promise`). Under an exact index the read is one
    /// round. Under a truncated one, the order pages are those the column
    /// index needs to place the limit and `next` (`placing`), and the value
    /// pages those of the rows taken alone, as a two-round read reads them;
    /// a read whose value pages are more is one round, all its GETs in
    /// flight at once. Rounds are seen in Full and Bloom: under Disk the
    /// order pages are local reads, and the choice of rounds is the same
    /// in every residency.
    async fn assert_reads_within_the_cost_promise(cost: &CostFixture) {
        let (opened, gauged) = store_gauged(&cost.fixture).await;
        let mut rounds_seen = 0;
        let mut values_seen = 0;
        for residency in Residency::ALL {
            let dir = tempfile::tempdir().unwrap();
            let index = index_of_type(&opened, residency, dir.path(), 0, cost.order_type).await;
            let run = index
                .locate_run(&Key::Long(2), Reads::new().await.source())
                .await
                .unwrap()
                .unwrap();
            let probe = Probe {
                index: &index,
                gauged: &gauged,
                run: &run,
            };
            let rows = rows_of(&run);
            let orders: Vec<OrderValue> = (0..rows.len()).map(&cost.value).collect();
            let order_leaf = index.order_leaf().unwrap();
            let value_leaf = index.value_leaf(0).unwrap();
            for deleted in [BTreeSet::new(), some_deleted(rows.len())] {
                let positions: BTreeSet<u64> = deleted
                    .iter()
                    .map(|&at| index.position(rows[at]).0)
                    .collect();
                let is_deleted = |position: RowPosition| positions.contains(&position.0);
                for scan in bounded_scans(&cost.points) {
                    for limit in COST_LIMITS {
                        let case = format!(
                            "{residency} {} deleted {deleted:?} limit {limit} {scan:?}",
                            cost.order_type
                        );
                        let expected = model(&index, &rows, &orders, &deleted, &scan, limit);
                        let (got, ranges) = probe.read(&scan, limit, &is_deleted).await;
                        assert_eq!(got.rows, expected.rows, "{case}");
                        assert_eq!(got.next, expected.next, "{case}");
                        let values = fetched(&index, value_leaf, &ranges);
                        let order_pages = fetched(&index, order_leaf, &ranges);
                        values_seen += values.len();
                        let promise = |leaf: usize,
                                       selected: Option<&RangeInclusive<usize>>,
                                       read: &BTreeSet<PageAt>,
                                       column: &str| {
                            let case = format!("{case} {column}");
                            assert_within_the_promise(
                                &index, leaf, &rows, selected, &scan, read, &case,
                            );
                        };
                        promise(value_leaf, expected.values.as_ref(), &values, "values");
                        if cost.exact {
                            promise(order_leaf, expected.orders.as_ref(), &order_pages, "orders");
                        } else {
                            let allowed = placing(&index, &rows, &scan, limit, &deleted);
                            assert!(
                                order_pages.is_subset(&allowed),
                                "{case}: order pages {order_pages:?}, beyond {allowed:?}"
                            );
                        }
                        let taken = taken_pages(&index, value_leaf, &rows, &expected.taken);
                        let one_round = cost.exact || !values.is_subset(&taken);
                        if one_round && ranges.len() > 1 {
                            let case = format!("{case}: value pages {values:?}, taken {taken:?}");
                            probe
                                .assert_one_round(&scan, limit, &is_deleted, ranges.len(), &case)
                                .await;
                            rounds_seen += 1;
                        }
                    }
                }
            }
        }
        assert!(values_seen > 0, "some read fetched a value page");
        assert!(
            !cost.exact || rounds_seen > 0,
            "some read ran more than one GET"
        );
    }

    /// `points` for a run of `rows` rows of `runs_of`: every row's order
    /// value, one between each two, and one below and one above the run.
    fn long_points(rows: usize) -> Vec<OrderValue> {
        let rows = i64::try_from(rows).unwrap();
        (-1..=rows)
            .flat_map(|j| {
                [
                    OrderValue::Long(j * ORDER_STEP),
                    OrderValue::Long(j * ORDER_STEP + HALF_STEP),
                ]
            })
            .collect()
    }

    /// `points` for a run of `rows` rows ordered by `value` under
    /// `order_type`: one below every value, each row's value and one
    /// between it and the next, and one above every value.
    fn byte_points(
        order_type: OrderType,
        rows: usize,
        value: fn(usize) -> Vec<u8>,
    ) -> Vec<OrderValue> {
        let mut points = vec![travelling(order_type, Vec::new())];
        for j in 0..rows {
            let mut between = value(j);
            between.push(b'5');
            points.push(travelling(order_type, value(j)));
            points.push(travelling(order_type, between));
        }
        points.push(travelling(order_type, vec![ABOVE_EVERY_PREFIX]));
        points
    }

    /// The cost fixture of `prefixed_run_of(truncate, rows, value)` read as
    /// `order_type`.
    fn byte_cost(
        truncate: Option<usize>,
        order_type: OrderType,
        value: fn(usize) -> Vec<u8>,
    ) -> CostFixture {
        CostFixture {
            fixture: prefixed_run_of(truncate, PREFIXED_RUN_ROWS, value),
            order_type,
            value: Box::new(move |j| travelling(order_type, value(j))),
            points: byte_points(order_type, PREFIXED_RUN_ROWS, value),
            exact: truncate.is_none(),
        }
    }

    /// Under an integer order column, every bounded read reads one round
    /// within RFD 025's promise: a run in one row group with its edge pages
    /// ruled out by the pages beside them, a run over exactly two pages
    /// both shared with a neighbouring key, and a run across three row
    /// groups.
    #[tokio::test]
    async fn test_read_run_under_an_integer_index_reads_one_round_within_the_promise() {
        let runs = [
            (bounded_run(0), 30),
            (runs_of(&[(1, 3), (2, 4), (3, 3)], 10), 4),
            (runs_of(&[(1, 5), (2, 16), (3, 3)], 8), 16),
        ];
        for (fixture, rows) in runs {
            assert_reads_within_the_cost_promise(&CostFixture {
                fixture,
                order_type: OrderType::Long,
                value: Box::new(|j| order_at(i64::try_from(j).unwrap())),
                points: long_points(rows),
                exact: true,
            })
            .await;
        }
    }

    /// Under a full-length string or binary order column index, every
    /// bounded read reads one round within RFD 025's promise.
    #[tokio::test]
    async fn test_read_run_under_a_full_length_index_reads_one_round_within_the_promise() {
        for order_type in [OrderType::String, OrderType::Binary] {
            assert_reads_within_the_cost_promise(&byte_cost(None, order_type, prefixed)).await;
        }
    }

    /// Under a truncated string or binary order column index, every bounded
    /// read reads the values of the rows it takes alone or in one round
    /// within RFD 025's promise, and the order pages the column index needs
    /// to place the limit: over a run whose values all share a prefix
    /// longer than the index keeps, one whose two halves have different
    /// prefixes, and one whose first prefix fills exactly two whole pages.
    #[tokio::test]
    async fn test_read_run_under_a_truncated_index_reads_the_values_of_the_rows_taken_alone() {
        for order_type in [OrderType::String, OrderType::Binary] {
            let shapes: [fn(usize) -> Vec<u8>; 3] = [prefixed, halved, two_low_pages];
            for value in shapes {
                let cost = byte_cost(Some(TRUNCATED_INDEX_BYTES), order_type, value);
                assert_reads_within_the_cost_promise(&cost).await;
            }
        }
    }

    /// The run position, in `prefixed_run(None, LONG_PREFIXED_RUN_ROWS)`, of
    /// the first row of an order page in the middle of the run.
    const PAGE_START_ROW: usize = 63;
    /// The run position, in `prefixed_run(None, LONG_PREFIXED_RUN_ROWS)`, of
    /// a row in the middle of the page before the run's last page.
    const NEAR_THE_END_ROW: usize = 117;

    /// Newest first before the first value of an order page, under a
    /// full-length index over a long run: every page between the bound and
    /// the run's last page lies outside the bound, and the last page, whose
    /// entry spans the next key's values too, lies past them, so neither is
    /// read in any column; one round reads the page the rows taken lie on.
    #[tokio::test]
    async fn test_read_run_newest_first_reads_no_page_past_one_outside_the_bound() {
        let limit = 3;
        let (opened, gauged) = store_gauged(&prefixed_run(None, LONG_PREFIXED_RUN_ROWS)).await;
        for order_type in [OrderType::String, OrderType::Binary] {
            let value = |j: usize| travelling(order_type, prefixed(j));
            let scan = ScanBounds {
                before: Some(value(PAGE_START_ROW)),
                ..bounds(Direction::NewestFirst)
            };
            let taken = [PAGE_START_ROW - 1, PAGE_START_ROW - 2, PAGE_START_ROW - 3];
            for residency in Residency::ALL {
                let dir = tempfile::tempdir().unwrap();
                let index = index_of_type(&opened, residency, dir.path(), 0, order_type).await;
                let run = index
                    .locate_run(&Key::Long(2), Reads::new().await.source())
                    .await
                    .unwrap()
                    .unwrap();
                let probe = Probe {
                    index: &index,
                    gauged: &gauged,
                    run: &run,
                };
                let rows = rows_of(&run);
                let case = format!("{residency} {order_type}");
                let (got, ranges) = probe.read(&scan, limit, &none_deleted).await;
                let orders: Vec<OrderValue> = (0..rows.len()).map(value).collect();
                let expected = model(&index, &rows, &orders, &BTreeSet::new(), &scan, limit);
                assert_eq!(expected.taken, taken);
                assert_eq!(got.rows, expected.rows, "{case}");
                assert_eq!(got.next, Some(value(PAGE_START_ROW - 4)), "{case}");
                let value_leaf = index.value_leaf(0).unwrap();
                let held: BTreeSet<PageAt> = pages_of(
                    &index,
                    value_leaf,
                    &rows[PAGE_START_ROW - 4..PAGE_START_ROW],
                )
                .into_iter()
                .collect();
                let values = fetched(&index, value_leaf, &ranges);
                let (read, needed) = (
                    bytes_of(&index, value_leaf, &values),
                    bytes_of(&index, value_leaf, &held),
                );
                assert!(needed > 0, "{case}");
                assert_eq!(
                    read, needed,
                    "{case}: value pages {values:?}, needed {held:?}"
                );
                if residency != Residency::Disk {
                    let order_leaf = index.order_leaf().unwrap();
                    assert_eq!(fetched(&index, order_leaf, &ranges), held, "{case}");
                }
                probe
                    .assert_one_round(&scan, limit, &none_deleted, ranges.len(), &case)
                    .await;
            }
        }
    }

    /// Newest first before a value in the middle of the page before the
    /// run's last, under a full-length index over a long run: the page that
    /// holds the bound and the run's last page, whose entry spans the next
    /// key's values too, both straddle the bound, which is what RFD 025
    /// allows past the rows the limit selects, so one round reads those two
    /// and the page the limit's rows end on, in every column, and none of
    /// the rest of the run.
    #[tokio::test]
    async fn test_read_run_beside_a_straddling_run_edge_page_reads_one_round() {
        let limit = 3;
        let (opened, gauged) = store_gauged(&prefixed_run(None, LONG_PREFIXED_RUN_ROWS)).await;
        let last = LONG_PREFIXED_RUN_ROWS - 1;
        for order_type in [OrderType::String, OrderType::Binary] {
            let value = |j: usize| travelling(order_type, prefixed(j));
            let scan = ScanBounds {
                before: Some(value(NEAR_THE_END_ROW)),
                ..bounds(Direction::NewestFirst)
            };
            for residency in Residency::ALL {
                let dir = tempfile::tempdir().unwrap();
                let index = index_of_type(&opened, residency, dir.path(), 0, order_type).await;
                let run = index
                    .locate_run(&Key::Long(2), Reads::new().await.source())
                    .await
                    .unwrap()
                    .unwrap();
                let probe = Probe {
                    index: &index,
                    gauged: &gauged,
                    run: &run,
                };
                let rows = rows_of(&run);
                let case = format!("{residency} {order_type}");
                let (got, ranges) = probe.read(&scan, limit, &none_deleted).await;
                let orders: Vec<OrderValue> = (0..rows.len()).map(value).collect();
                let expected = model(&index, &rows, &orders, &BTreeSet::new(), &scan, limit);
                assert_eq!(got.rows, expected.rows, "{case}");
                assert_eq!(got.next, Some(value(NEAR_THE_END_ROW - 4)), "{case}");
                // From the run's last row down to the row after the limit's
                // rows by the column index: the page they lie on, the
                // bound's, and the run-edge page.
                let walked = &rows[NEAR_THE_END_ROW - 6..=last];
                let order_leaf = index.order_leaf().unwrap();
                let value_leaf = index.value_leaf(0).unwrap();
                let held: BTreeSet<PageAt> =
                    pages_of(&index, value_leaf, walked).into_iter().collect();
                assert_eq!(held.len(), 3, "{case}");
                if residency != Residency::Disk {
                    let order_pages = fetched(&index, order_leaf, &ranges);
                    let whole_run: BTreeSet<PageAt> =
                        pages_of(&index, order_leaf, &rows).into_iter().collect();
                    assert_eq!(
                        bytes_of(&index, order_leaf, &order_pages),
                        bytes_of(&index, order_leaf, &held),
                        "{case}: order pages {order_pages:?} of the run's {}",
                        whole_run.len()
                    );
                }
                assert_eq!(fetched(&index, value_leaf, &ranges), held, "{case}");
                probe
                    .assert_one_round(&scan, limit, &none_deleted, ranges.len(), &case)
                    .await;
            }
        }
    }

    /// A run over exactly two order pages, each shared with a neighbouring
    /// key whose values widen its entry across the bound, is read in one
    /// round: RFD 025 allows the page that holds the bound and one run-edge
    /// page past the rows the limit selects, and these are those two.
    #[tokio::test]
    async fn test_read_run_over_two_shared_pages_reads_one_round() {
        let limit = 1;
        let (opened, gauged) = store_gauged(&runs_of(&[(1, 3), (2, 4), (3, 3)], 10)).await;
        let first_run_row = 3;
        let after_ten = ScanBounds {
            after: Some(order_at(1)),
            ..bounds(Direction::OldestFirst)
        };
        for residency in Residency::ALL {
            let dir = tempfile::tempdir().unwrap();
            let index = index_in(&opened, residency, dir.path(), 0).await;
            let run = index
                .locate_run(&Key::Long(2), Reads::new().await.source())
                .await
                .unwrap()
                .unwrap();
            let probe = Probe {
                index: &index,
                gauged: &gauged,
                run: &run,
            };
            let (got, ranges) = probe.read(&after_ten, limit, &none_deleted).await;
            assert_eq!(
                got.rows,
                [row_answer(first_run_row + 2, 2, 1)],
                "{residency}"
            );
            assert_eq!(got.next, Some(order_at(3)), "{residency}");
            let order_gets = usize::from(residency != Residency::Disk);
            assert_eq!(ranges.len(), 1 + order_gets, "{residency}");
            let case = residency.to_string();
            probe
                .assert_one_round(&after_ten, limit, &none_deleted, ranges.len(), &case)
                .await;
        }
    }

    /// Under a 16-byte truncated index over a long run whose values share a
    /// 32-byte prefix with the bound, oldest first after the run's first
    /// value and newest first before its last, the value pages read are
    /// those of the rows taken alone, in every column and residency; the
    /// order pages read are every page that straddles the bound up to the
    /// page where the column index places the limit's rows inside it: the
    /// whole run where every page shares the prefix, and not where half the
    /// run has another (`halved`).
    #[tokio::test]
    async fn test_read_run_under_a_truncated_shared_prefix_reads_the_values_of_the_rows_taken() {
        let limit = 3;
        let last = LONG_PREFIXED_RUN_ROWS - 1;
        // Under `halved`, the first page past the shared prefix in either
        // direction holds the limit's rows and the one after them.
        let past_low = LOW_PREFIX_ROWS + usize::try_from(limit).unwrap();
        let before_low = LOW_PREFIX_ROWS - usize::try_from(limit).unwrap() - 1;
        let shapes = [
            (prefixed as fn(usize) -> Vec<u8>, 0..=last, 0..=last),
            (halved, 0..=past_low, before_low..=last),
        ];
        for (shape, oldest_orders, newest_orders) in shapes {
            let fixture =
                prefixed_run_of(Some(TRUNCATED_INDEX_BYTES), LONG_PREFIXED_RUN_ROWS, shape);
            let (opened, gauged) = store_gauged(&fixture).await;
            for order_type in [OrderType::String, OrderType::Binary] {
                let value = |j: usize| travelling(order_type, shape(j));
                let cases = [
                    (
                        ScanBounds {
                            after: Some(value(0)),
                            ..bounds(Direction::OldestFirst)
                        },
                        [1, 2, 3],
                        4,
                        oldest_orders.clone(),
                    ),
                    (
                        ScanBounds {
                            before: Some(value(last)),
                            ..bounds(Direction::NewestFirst)
                        },
                        [last - 1, last - 2, last - 3],
                        last - 4,
                        newest_orders.clone(),
                    ),
                ];
                for residency in Residency::ALL {
                    let dir = tempfile::tempdir().unwrap();
                    let index = index_of_type(&opened, residency, dir.path(), 0, order_type).await;
                    let run = index
                        .locate_run(&Key::Long(2), Reads::new().await.source())
                        .await
                        .unwrap()
                        .unwrap();
                    let probe = Probe {
                        index: &index,
                        gauged: &gauged,
                        run: &run,
                    };
                    let rows = rows_of(&run);
                    let orders: Vec<OrderValue> = (0..rows.len()).map(value).collect();
                    let order_leaf = index.order_leaf().unwrap();
                    let value_leaf = index.value_leaf(0).unwrap();
                    for (scan, taken, next, order_rows) in &cases {
                        let case = format!("{residency} {order_type} {scan:?}");
                        let (got, ranges) = probe.read(scan, limit, &none_deleted).await;
                        let expected = model(&index, &rows, &orders, &BTreeSet::new(), scan, limit);
                        assert_eq!(expected.taken, taken, "{case}");
                        assert_eq!(got.rows, expected.rows, "{case}");
                        assert_eq!(got.next, Some(value(*next)), "{case}");
                        let (first, last_taken) =
                            (*taken.iter().min().unwrap(), *taken.iter().max().unwrap());
                        let held: BTreeSet<PageAt> =
                            pages_of(&index, value_leaf, &rows[first..=last_taken])
                                .into_iter()
                                .collect();
                        let values = fetched(&index, value_leaf, &ranges);
                        let (read, needed) = (
                            bytes_of(&index, value_leaf, &values),
                            bytes_of(&index, value_leaf, &held),
                        );
                        assert!(needed > 0, "{case}");
                        assert_eq!(read, needed, "{case}: value pages {values:?}");
                        if residency != Residency::Disk {
                            let order_pages = fetched(&index, order_leaf, &ranges);
                            let walked: BTreeSet<PageAt> =
                                pages_of(&index, order_leaf, &rows[order_rows.clone()])
                                    .into_iter()
                                    .collect();
                            assert_eq!(
                                bytes_of(&index, order_leaf, &order_pages),
                                bytes_of(&index, order_leaf, &walked),
                                "{case}: order pages {order_pages:?}"
                            );
                        }
                    }
                }
            }
        }
    }

    /// The run position, in `two_low_pages`, of the bound of the reads of
    /// `test_read_run_with_two_pages_across_one_of_two_bounds_reads_two_rounds`.
    const LOW_BOUND_ROW: usize = 5;

    /// Under a 16-byte truncated index, oldest first after a value of the
    /// run's low prefix: the two whole pages of that prefix lie across
    /// `after`, more than one per bound, so the read takes two rounds and
    /// reads the value pages of the rows taken alone, and a `before` bound
    /// that excludes nothing changes neither, since the pages across it are
    /// counted against it alone.
    #[tokio::test]
    async fn test_read_run_with_two_pages_across_one_of_two_bounds_reads_two_rounds() {
        let limit = 3;
        let fixture = prefixed_run_of(
            Some(TRUNCATED_INDEX_BYTES),
            PREFIXED_RUN_ROWS,
            two_low_pages,
        );
        let (opened, gauged) = store_gauged(&fixture).await;
        for order_type in [OrderType::String, OrderType::Binary] {
            let value = |j: usize| travelling(order_type, two_low_pages(j));
            let after = ScanBounds {
                after: Some(value(LOW_BOUND_ROW)),
                ..bounds(Direction::OldestFirst)
            };
            let both = ScanBounds {
                before: Some(travelling(order_type, vec![ABOVE_EVERY_PREFIX])),
                ..after.clone()
            };
            let taken = [LOW_BOUND_ROW + 1, LOW_BOUND_ROW + 2, LOW_BOUND_ROW + 3];
            for residency in Residency::ALL {
                let dir = tempfile::tempdir().unwrap();
                let index = index_of_type(&opened, residency, dir.path(), 0, order_type).await;
                let run = index
                    .locate_run(&Key::Long(2), Reads::new().await.source())
                    .await
                    .unwrap()
                    .unwrap();
                let probe = Probe {
                    index: &index,
                    gauged: &gauged,
                    run: &run,
                };
                let rows = rows_of(&run);
                let orders: Vec<OrderValue> = (0..rows.len()).map(value).collect();
                let value_leaf = index.value_leaf(0).unwrap();
                let held: BTreeSet<PageAt> =
                    pages_of(&index, value_leaf, &rows[taken[0]..=taken[2]])
                        .into_iter()
                        .collect();
                let needed = bytes_of(&index, value_leaf, &held);
                assert!(needed > 0);
                for scan in [&after, &both] {
                    let case = format!("{residency} {order_type} {scan:?}");
                    let (got, ranges) = probe.read(scan, limit, &none_deleted).await;
                    let expected = model(&index, &rows, &orders, &BTreeSet::new(), scan, limit);
                    assert_eq!(expected.taken, taken, "{case}");
                    assert_eq!(got.rows, expected.rows, "{case}");
                    assert_eq!(got.next, Some(value(LOW_BOUND_ROW + 4)), "{case}");
                    let values = fetched(&index, value_leaf, &ranges);
                    assert_eq!(
                        bytes_of(&index, value_leaf, &values),
                        needed,
                        "{case}: value pages {values:?}, the rows taken's {held:?}"
                    );
                }
            }
        }
    }

    /// The futures `FileIndex` hands out for a Scan are `Send`, so a
    /// `Table` can run them on any worker.
    #[tokio::test]
    async fn test_run_futures_are_send() {
        fn is_send<T: Send>(_: &T) {}
        let opened = store(&one_long_run(0)).await;
        let dir = tempfile::tempdir().unwrap();
        let index = index_in(&opened, Residency::Full, dir.path(), 0).await;
        let reads = Reads::new().await;
        let key = Key::Long(2);
        let locate = index.locate_run(&key, reads.source());
        is_send(&locate);
        let run = locate.await.unwrap().unwrap();
        let newest = bounds(Direction::NewestFirst);
        let read = index.read_run(&run, &newest, 1, &[0], &none_deleted, reads.source());
        is_send(&read);
        assert_eq!(read.await.unwrap().rows.len(), 1);
    }
}
