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
pub struct RunPart {
    /// The row group.
    pub row_group: usize,
    /// The run's rows within the row group, ascending by order value;
    /// never empty.
    pub rows: Range<i64>,
}

/// A key's run in one file: its rows in each row group it spans, in file
/// order, so ascending by order value; at least one part.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Run {
    /// The run's rows per row group.
    pub parts: Vec<RunPart>,
}

/// What one file answers a Scan of one key (`FileIndex::read_run`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileRows {
    /// The rows taken, in the Scan's direction: each its order value and
    /// one value per requested column, `None` for a null and for a column
    /// the file does not hold (`FileIndex::value_leaf`), whose default the
    /// caller answers.
    pub rows: Vec<(OrderValue, Vec<Option<Bytes>>)>,
    /// The order value of the next live row inside the bounds past the
    /// last row taken, in the Scan's direction; `None` when no live row
    /// remains there.
    pub next: Option<OrderValue>,
    /// Whether any order or value page the read took came from object
    /// storage, a fetch it ran or joined.
    pub from_store: bool,
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
pub struct RunSource<'a> {
    /// The Scan cache.
    pub cache: &'a PageCache,
    /// The Scan's fan-out (`TableHandles::batch_fan_out`); `None` bounds
    /// nothing.
    pub fan_out: Option<&'a Arc<Semaphore>>,
    /// The table's span hedge window.
    pub window: &'a HedgeWindow,
    /// Per value column, by position in the layout, the counter its GETs
    /// count in; a position past the end counts nowhere.
    pub value_gets: &'a [Counter],
    /// The counter the order column's GETs count in.
    pub order_gets: Option<&'a Counter>,
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
    pub async fn locate_run(&self, key: &Key, source: RunSource<'_>) -> Result<Option<Run>> {
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
    /// tells, for each order page of the run, whether its values lie inside
    /// the bounds, outside them, or across one (`ValueRange::fit`); a page
    /// at the run's edge holds a neighbouring key's values too, which widen
    /// its entry, so it is judged by the page beside it where that one holds
    /// the run alone under an integer entry, which a writer never
    /// truncates, and kept where nothing rules it out (`pieces`). From
    /// the direction's end the rows of pages outside are passed over, the
    /// live rows of pages across a bound are read without counting, and
    /// the live rows of pages inside count until `limit` are taken, then the
    /// order value of the next live row is read with no value of it, on a
    /// page no row taken lies on if that is where it is. Every page of the
    /// order column and of each column in `columns`, positions in the
    /// layout, over those rows is read in one round, then the bounds cut
    /// the rows exactly. Where a row group of the run has no column index
    /// on its order chunk and a bound is set, its order pages are read in a
    /// first round and the value pages of exactly the rows taken in a
    /// second (`order_index_missing`).
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
    pub async fn read_run(
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
        let (walked, orders, values) = if indexed {
            let walked = walk.rows(|piece, _| piece.fit);
            let (order_spans, value_spans) = (spans_of(&walked, false), spans_of(&walked, true));
            let (orders, values) = futures::try_join!(
                self.read_orders(order, &order_spans, source),
                self.read_values(columns, &value_spans, source),
            )?;
            (walked, orders, values)
        } else {
            let all: Vec<Walked> = pieces
                .iter()
                .filter(|piece| piece.fit != PageFit::Outside)
                .flat_map(|piece| {
                    piece.rows.clone().map(|row| Walked {
                        row: RowRef {
                            row_group: piece.row_group,
                            row,
                        },
                        values: false,
                    })
                })
                .collect();
            let orders = self
                .read_orders(order, &spans_of(&all, false), source)
                .await?;
            let walked = walk.rows(|_, row| {
                if cut.holds(&orders.value(row)) {
                    PageFit::Inside
                } else {
                    PageFit::Outside
                }
            });
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
    /// (`ValueRange::after_page`).
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
        (0..pages.len())
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
                    fit: range.fit(cut.after, cut.before),
                }
            })
            .collect()
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

    /// Whether `value` lies inside the bounds.
    fn holds(self, value: &OrderValue) -> bool {
        self.after.is_none_or(|after| value > after)
            && self.before.is_none_or(|before| value < before)
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

/// The rows of a run on one order page.
struct Piece {
    row_group: usize,
    rows: Range<i64>,
    fit: PageFit,
}

/// A row a read takes up, in the Scan's direction: one that may be taken,
/// whose values are read, or the one after the rows taken, whose order
/// value alone is.
struct Walked {
    row: RowRef,
    values: bool,
}

/// The rows of a run a read takes up (`Walk::rows`).
struct Walk<'a> {
    index: &'a FileIndex,
    pieces: &'a [Piece],
    direction: Direction,
    limit: usize,
    deleted: &'a (dyn Fn(RowPosition) -> bool + Sync),
}

impl Walk<'_> {
    /// From the direction's end, every live row of a piece not outside the
    /// bounds as `fit` judges the row, until `limit` rows judged inside
    /// are taken up with their values and one more such row without them;
    /// a row judged across a bound before then is taken up with its
    /// values, and one after with its order value alone.
    fn rows(&self, fit: impl Fn(&Piece, RowRef) -> PageFit) -> Vec<Walked> {
        let pieces: Box<dyn Iterator<Item = &Piece>> = match self.direction {
            Direction::OldestFirst => Box::new(self.pieces.iter()),
            Direction::NewestFirst => Box::new(self.pieces.iter().rev()),
        };
        let mut walked = Vec::new();
        let mut inside = 0;
        for piece in pieces.filter(|piece| piece.fit != PageFit::Outside) {
            let rows: Box<dyn Iterator<Item = i64>> = match self.direction {
                Direction::OldestFirst => Box::new(piece.rows.clone()),
                Direction::NewestFirst => Box::new(piece.rows.clone().rev()),
            };
            for row in rows {
                let row = RowRef {
                    row_group: piece.row_group,
                    row,
                };
                if (self.deleted)(self.index.position(row)) {
                    continue;
                }
                match fit(piece, row) {
                    PageFit::Outside => {}
                    PageFit::Inside if inside == self.limit => {
                        walked.push(Walked { row, values: false });
                        return walked;
                    }
                    PageFit::Inside => {
                        inside += 1;
                        walked.push(Walked { row, values: true });
                    }
                    PageFit::Straddles => walked.push(Walked {
                        row,
                        values: inside < self.limit,
                    }),
                }
            }
        }
        walked
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
    use crate::storage::testing::DEADLINE_GUARD;

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

    /// Key 2's run of `PREFIXED_RUN_ROWS` rows between one row each of keys
    /// 1 and 3, in pages of `PAGE` in one row group, every key's `j`th row
    /// ordered by `prefixed(j)`, with the column index truncated to
    /// `truncate` bytes, `None` leaving it whole.
    fn prefixed_run(truncate: Option<usize>) -> Fixture {
        let runs = [(1, 1), (2, PREFIXED_RUN_ROWS), (3, 1)];
        let mut keys = Vec::new();
        let mut orders = Vec::new();
        for (key, rows) in runs {
            for j in 0..rows {
                keys.push(key);
                orders.push(Some(prefixed(j)));
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
            let opened = store(&prefixed_run(truncate)).await;
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
