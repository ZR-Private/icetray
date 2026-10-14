//! An ordered file's rows as pairs of key and order value, for the check
//! across files that no live pair repeats (RFD 025, Refresh): which of the
//! file's order pages can hold rows in a key range and an order range,
//! judged by the key pages each order page's rows span and by the order
//! column's column index; the bytes those pages, or all of the file's rows,
//! decode from in each tier; and the pairs of one span of rows decoded at a
//! time. A key-value table never reaches this module.

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::ops::Range;
use std::sync::Arc;

use super::build::{
    DECODE_BATCH_RECORDS, check_utf8, chunks_from_disk, decode_run, fetch_chunk_parts,
};
use super::inspect::column_chunk_ranges;
use super::ordered::{OrderRun, decode_orders, order_column_index, page_meets};
use super::{
    Column, FileFault, FileIndex, KeyResidency, Keys, OrderLeaf, RowPosition, RowRef, file_fault,
    row_index,
};
use crate::approx;
use crate::error::Result;
use crate::key::Key;
use crate::order::OrderValue;
use crate::parquet_io::Segments;

/// Rows of one row group whose pairs decode together: one order page, or
/// a window of a row group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RowSpan {
    row_group: usize,
    rows: Range<i64>,
}

/// Rows of an ordered file in file order and the bytes they decode from
/// without further I/O.
pub(crate) struct PairChunks {
    spans: Vec<RowSpan>,
    chunks: Arc<Segments>,
}

impl PairChunks {
    /// The spans of rows held.
    pub(crate) fn len(&self) -> usize {
        self.spans.len()
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
            orders: OrderRun::Long(Vec::new()),
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
            .then_with(|| self.orders.cmp_run(at, &other.orders, other_at))
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
    /// The order column of an ordered file.
    pub(super) fn order_leaf_of(&self) -> OrderLeaf {
        self.layout
            .order
            .expect("only an ordered table's files are read as pairs")
    }

    /// The order column as the decoders see it.
    pub(super) fn order_values(&self, order: OrderLeaf) -> Column<'_> {
        Column {
            metadata: &self.metadata,
            leaf: order.leaf,
            path: &self.handle.location,
        }
    }

    /// The exact last key of `row_group`, which has rows; the file's last
    /// key bounds it where the row group has no entry.
    fn group_last(&self, row_group: usize) -> &Key {
        self.groups
            .iter()
            .find(|group| group.row_group == row_group)
            .map_or(&self.key_range.1, |group| &group.last)
    }

    /// The order pages, in file order, that can hold a row whose key lies
    /// in `keys` and whose order value lies in `orders`, both closed: those
    /// whose rows' keys, bounded below by the first key of the key page
    /// holding the page's first row and above by the first key of the key
    /// page after the one holding its last row (the row group's last key
    /// for its last key page), meet `keys`, and whose column index entry
    /// meets `orders`. A file without a column index for a row group's
    /// order chunk keeps every page of it whose keys meet. No I/O.
    pub(crate) fn order_pages_within(
        &self,
        keys: (&Key, &Key),
        orders: (&OrderValue, &OrderValue),
    ) -> Vec<RowSpan> {
        let order = self.order_leaf_of();
        let mut spans = Vec::new();
        for (row_group, fences) in self.fences.iter().enumerate() {
            let Some(first) = fences.first() else {
                continue;
            };
            let last = self.group_last(row_group);
            if !(first <= keys.1 && keys.0 <= last) {
                continue;
            }
            let key_chunk = self.key_column().group(row_group);
            let order_chunk = self.order_values(order).group(row_group);
            let index = order_column_index(order_chunk);
            for page in 0..order_chunk.page_locations().len() {
                let rows = order_chunk.page_rows(page);
                if rows.is_empty() {
                    continue;
                }
                let low = &fences[key_chunk.page_holding(row_index(rows.start))];
                let high = fences
                    .get(key_chunk.page_holding(row_index(rows.end - 1)) + 1)
                    .unwrap_or(last);
                let keys_meet = low <= keys.1 && keys.0 <= high;
                if keys_meet && index.is_none_or(|entry| page_meets(entry, page, orders)) {
                    spans.push(RowSpan { row_group, rows });
                }
            }
        }
        spans
    }

    /// The bytes `spans`, order pages `order_pages_within` chose, decode
    /// from: each page and its chunk's dictionary page, with the key pages
    /// its rows span. Full fetches the order pages by load GET, one per
    /// page and dictionary page, and holds its key chunks; Disk reads both
    /// from the chunk file, one counted local read per page, and no GET;
    /// Bloom fetches both by load GET, the key column's dictionary page
    /// with them.
    pub(crate) async fn order_page_chunks(&self, spans: Vec<RowSpan>) -> Result<PairChunks> {
        let order = self.order_leaf_of();
        let mut order_ranges = BTreeSet::new();
        let mut key_ranges = BTreeSet::new();
        let held = |range: Range<u64>| (range.start, range.end);
        for span in &spans {
            let order_chunk = self.order_values(order).group(span.row_group);
            let page = order_chunk.page_holding(row_index(span.rows.start));
            order_ranges.insert(held(order_chunk.page_range(page)));
            order_ranges.extend(order_chunk.dictionary_range().map(held));
            let key_chunk = self.key_column().group(span.row_group);
            let pages = key_chunk.page_holding(row_index(span.rows.start))
                ..=key_chunk.page_holding(row_index(span.rows.end - 1));
            key_ranges.extend(pages.map(|page| held(key_chunk.page_range(page))));
            key_ranges.extend(key_chunk.dictionary_range().map(held));
        }
        let ranges = |set: &BTreeSet<(u64, u64)>| -> Vec<Range<u64>> {
            set.iter().map(|&(start, end)| start..end).collect()
        };
        let order_ranges = ranges(&order_ranges);
        let both = || {
            let mut both = order_ranges.clone();
            both.extend(ranges(&key_ranges));
            both
        };
        let chunks = self.read_ranges(&order_ranges, both).await?;
        Ok(PairChunks { spans, chunks })
    }

    /// Every row of the file, a window of at most `DECODE_BATCH_RECORDS`
    /// rows of one row group at a time, and the key and order chunks they
    /// decode from: Full holds its key chunks and fetches the order chunks,
    /// one load GET per row group; Disk reads both from the chunk file, one
    /// counted local read per row group per column, and no GET; Bloom
    /// fetches both, one load GET per row group per column.
    pub(crate) async fn all_pair_chunks(&self) -> Result<PairChunks> {
        let order = self.order_leaf_of();
        let size = self.handle.size();
        let order_ranges = column_chunk_ranges(&self.metadata, order.leaf, size);
        let both = || {
            let mut ranges = column_chunk_ranges(&self.metadata, self.layout.key_leaf, size);
            ranges.extend(order_ranges.iter().cloned());
            ranges
        };
        let chunks = self.read_ranges(&order_ranges, both).await?;
        let mut spans = Vec::new();
        for (row_group, group) in self.metadata.row_groups().iter().enumerate() {
            let mut start = 0;
            while start < group.num_rows() {
                let end = group.num_rows().min(start + DECODE_BATCH_RECORDS);
                spans.push(RowSpan {
                    row_group,
                    rows: start..end,
                });
                start = end;
            }
        }
        Ok(PairChunks { spans, chunks })
    }

    /// The bytes of `order_ranges` and of the key column the tier does not
    /// hold: Full fetches `order_ranges` and holds the key chunks; Disk
    /// reads `both`, the order and key ranges together, from the chunk
    /// file; Bloom fetches `both`.
    async fn read_ranges(
        &self,
        order_ranges: &[Range<u64>],
        both: impl FnOnce() -> Vec<Range<u64>>,
    ) -> Result<Arc<Segments>> {
        let size = self.handle.size();
        match &self.keys {
            KeyResidency::Chunks(held) => {
                let mut parts = fetch_chunk_parts(&self.handle, order_ranges).await?;
                parts.extend(held.parts().iter().cloned());
                Ok(Arc::new(Segments::new(parts, size)))
            }
            KeyResidency::Disk(disk) => chunks_from_disk(disk, &both(), size).await,
            KeyResidency::Filters(_) => {
                let parts = fetch_chunk_parts(&self.handle, &both()).await?;
                Ok(Arc::new(Segments::new(parts, size)))
            }
        }
    }

    /// The pairs of span `at` of `chunks`, which `order_page_chunks` or
    /// `all_pair_chunks` of this file returned. The rows were verified when
    /// the file was indexed; a span that decodes to fewer keys or order
    /// values than it has rows is `FileFault::PageRowsMismatch` at its
    /// first order page, a key that is not UTF-8 `FileFault::KeyNotUtf8`,
    /// and a string order value that is not `FileFault::OrderNotUtf8`.
    pub(crate) fn decode_pairs(&self, chunks: &PairChunks, at: usize) -> Result<Pairs> {
        let span = &chunks.spans[at];
        let order = self.order_leaf_of();
        let key_chunk = self.key_column().group(span.row_group);
        let order_chunk = self.order_values(order).group(span.row_group);
        let keys = decode_run(
            key_chunk,
            self.layout.key_shape,
            span.rows.clone(),
            Arc::clone(&chunks.chunks),
        )?;
        check_utf8(key_chunk, &keys)?;
        let orders = decode_orders(
            order_chunk,
            order.shape,
            span.rows.clone(),
            Arc::clone(&chunks.chunks),
        )?;
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
