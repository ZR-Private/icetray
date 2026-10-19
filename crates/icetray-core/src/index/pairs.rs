//! An ordered file's rows as pairs of key and order value, for the check
//! across files that no live pair repeats (RFD 025, Refresh): the reads
//! one row group's chosen order pages take in each tier, planned before
//! they are made, the bytes those pages decode from, and the pairs of one
//! run of them decoded at a time. Which pages a check
//! reads is chosen by the order fences (`generation::ordered`). A key-value
//! table never reaches this module.

use std::cmp::Ordering;
use std::ops::Range;
use std::sync::Arc;

use super::build::{DECODE_BATCH_RECORDS, check_utf8, decode_run};
use super::ordered::OrderRun;
use super::{
    ColumnChunk, FileFault, FileIndex, KeyResidency, Keys, RowPosition, RowRef, file_fault,
    row_index,
};
use crate::approx;
use crate::error::Result;
use crate::key::Key;
use crate::order::OrderValue;
use crate::parquet_io::Segments;

/// Rows of one row group whose pairs decode together: a run of
/// consecutive chosen order pages, of at most `DECODE_BATCH_RECORDS` rows
/// unless one page holds more.
#[derive(Clone, Debug, PartialEq, Eq)]
struct RowSpan {
    row_group: usize,
    rows: Range<i64>,
}

/// The reads one row group's chosen order pages of an ordered file take
/// in its tier, planned before any is made, so a merge knows the bytes a
/// row group will hold before it reads it.
pub(crate) struct PairRead {
    spans: Vec<RowSpan>,
    /// The ranges of the file the tier reads, in the order it reads them.
    ranges: Vec<Range<u64>>,
    /// Their bytes.
    bytes: u64,
}

impl PairRead {
    /// The bytes the reads fetch or read: what the row group holds once
    /// read (`PairChunks::read_bytes`).
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }
}

/// One row group's chosen order pages of an ordered file, in runs, and the
/// bytes they decode from without further I/O.
pub(crate) struct PairChunks {
    spans: Vec<RowSpan>,
    chunks: Arc<Segments>,
    /// The bytes fetched or read from the chunk file for them: what a merge
    /// holds while it reads the row group, a Full table's resident key
    /// chunks not among them.
    read: u64,
}

impl PairChunks {
    /// The runs of rows held.
    pub(crate) fn len(&self) -> usize {
        self.spans.len()
    }

    /// The bytes fetched or read for them.
    pub(crate) fn read_bytes(&self) -> u64 {
        self.read
    }
}

/// The pairs of one span of rows, in file order, with the position in the
/// file of the first.
pub(crate) struct Pairs {
    keys: Keys,
    orders: OrderRun,
    start: RowPosition,
}

impl Pairs {
    /// No pairs, before a span is decoded.
    pub(crate) fn empty() -> Self {
        Self {
            keys: Keys::Long(Vec::new()),
            orders: OrderRun(Keys::Long(Vec::new())),
            start: RowPosition(0),
        }
    }

    /// The number of pairs.
    pub(crate) fn len(&self) -> usize {
        self.keys.len()
    }

    /// Whether there are none.
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Pair `at` against pair `other_at` of `other`: by key, then by order
    /// value.
    pub(crate) fn cmp_at(&self, at: usize, other: &Self, other_at: usize) -> Ordering {
        self.keys
            .cmp_at(at, &other.keys, other_at)
            .then_with(|| self.orders.cmp_at(at, &other.orders, other_at))
    }

    /// The key of pair `at`.
    pub(crate) fn key(&self, at: usize) -> Key {
        self.keys.get(at)
    }

    /// The order value of pair `at`.
    pub(crate) fn order(&self, at: usize) -> OrderValue {
        self.orders.value(at)
    }

    /// The position in the file of pair `at`, counted from the file's first
    /// row as Iceberg's deletes count it.
    pub(crate) fn position(&self, at: usize) -> RowPosition {
        RowPosition(self.start.0 + approx::count(at))
    }
}

impl FileIndex {
    /// The reads order pages `pages` of `row_group`, ascending, decode
    /// from, without I/O: each run of consecutive pages (`RowSpan`) as one
    /// range of the order chunk and one of the key pages its rows span,
    /// and each chunk's dictionary page as one more. Full reads the order
    /// ranges and holds its key chunks; Disk and Bloom read the order and
    /// key ranges together.
    pub(crate) fn pair_read(&self, row_group: usize, pages: &[usize]) -> PairRead {
        let order_chunk = self.order_chunk(row_group);
        let key_chunk = self.key_column().group(row_group);
        let spans = page_runs(order_chunk, pages);
        let mut ranges = Vec::new();
        for &page in pages {
            coalesce(&mut ranges, order_chunk.page_range(page));
        }
        ranges.extend(order_chunk.dictionary_range());
        match &self.keys {
            KeyResidency::Chunks(_) => {}
            KeyResidency::Disk(_) | KeyResidency::Filters(_) => {
                let mut key_ranges = Vec::new();
                for span in &spans {
                    let first = key_chunk.page_holding(row_index(span.rows.start));
                    let last = key_chunk.page_holding(row_index(span.rows.end - 1));
                    for page in first..=last {
                        coalesce(&mut key_ranges, key_chunk.page_range(page));
                    }
                }
                ranges.extend(key_ranges);
                ranges.extend(key_chunk.dictionary_range());
            }
        }
        let bytes = ranges
            .iter()
            .map(|range| range.end - range.start)
            .fold(0, u64::saturating_add);
        PairRead {
            spans,
            ranges,
            bytes,
        }
    }

    /// The bytes `read`, which `pair_read` of this file planned, decodes
    /// from: under Full its ranges by load GET beside the resident key
    /// chunks, under Disk from the chunk file, one counted local read per
    /// range and no GET, and under Bloom by load GET.
    pub(crate) async fn pair_chunks(&self, read: PairRead) -> Result<PairChunks> {
        let mut parts = self.load_parts(&read.ranges).await?;
        if let KeyResidency::Chunks(held) = &self.keys {
            parts.extend(held.parts().iter().cloned());
        }
        Ok(PairChunks {
            spans: read.spans,
            chunks: Arc::new(Segments::new(parts, self.handle.size())),
            read: read.bytes,
        })
    }

    /// The pairs of span `at` of `chunks`, which `pair_chunks` of this file
    /// returned. The rows were verified when
    /// the file was indexed; a span that decodes to fewer keys or order
    /// values than it has rows is `FileFault::PageRowsMismatch` at its
    /// first order page, a key that is not UTF-8 `FileFault::KeyNotUtf8`,
    /// and a string order value that is not `FileFault::OrderNotUtf8`.
    pub(crate) fn decode_pairs(&self, chunks: &PairChunks, at: usize) -> Result<Pairs> {
        let span = &chunks.spans[at];
        let key_chunk = self.key_column().group(span.row_group);
        let order_chunk = self.order_chunk(span.row_group);
        let keys = decode_run(
            key_chunk,
            self.layout.key_shape.run_shape(),
            span.rows.clone(),
            Arc::clone(&chunks.chunks),
        )?;
        check_utf8(key_chunk, &keys)?;
        let orders = OrderRun(decode_run(
            order_chunk,
            self.order_leaf_of().shape.run_shape(),
            span.rows.clone(),
            Arc::clone(&chunks.chunks),
        )?);
        let rows = row_index(span.rows.end - span.rows.start);
        if keys.len() != rows || orders.len() != rows {
            return Err(file_fault(
                &self.handle.location,
                FileFault::PageRowsMismatch {
                    row_group: span.row_group,
                    page: order_chunk.page_holding(row_index(span.rows.start)),
                },
            ));
        }
        if !orders.is_utf8() {
            return Err(file_fault(
                &self.handle.location,
                FileFault::OrderNotUtf8 {
                    row_group: span.row_group,
                },
            ));
        }
        Ok(Pairs {
            keys,
            orders,
            start: self.position(RowRef {
                row_group: span.row_group,
                row: span.rows.start,
            }),
        })
    }
}

/// The runs of consecutive pages among `pages` of the order chunk `chunk`,
/// ascending, each as the rows it holds, a run ending where adding the
/// next page would take it past `DECODE_BATCH_RECORDS` rows. A page
/// without rows is passed over.
fn page_runs(chunk: ColumnChunk<'_>, pages: &[usize]) -> Vec<RowSpan> {
    let mut spans: Vec<RowSpan> = Vec::new();
    for &page in pages {
        let rows = chunk.page_rows(page);
        if rows.is_empty() {
            continue;
        }
        match spans.last_mut() {
            Some(span)
                if span.rows.end == rows.start
                    && rows.end - span.rows.start <= DECODE_BATCH_RECORDS =>
            {
                span.rows.end = rows.end;
            }
            Some(_) | None => spans.push(RowSpan {
                row_group: chunk.row_group(),
                rows,
            }),
        }
    }
    spans
}

/// `range` after `ranges`, which ascend and end at or before it starts,
/// joined to the last of them where the two meet or overlap: a page met
/// again, or the next page of a run.
fn coalesce(ranges: &mut Vec<Range<u64>>, range: Range<u64>) {
    match ranges.last_mut() {
        Some(last) if range.start <= last.end => last.end = last.end.max(range.end),
        Some(_) | None => ranges.push(range),
    }
}
