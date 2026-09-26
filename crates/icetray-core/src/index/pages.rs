//! The pages one decode hands parquet's column reader (RFD 006): the data
//! pages holding the rows asked for and, ahead of them, the column chunk's
//! dictionary page when it has one. No other page of the chunk is read,
//! skipped, or counted, so a lookup reads one data page wherever in its
//! chunk the page lies. Each page is checked against its entry in the
//! offset index before the column reader holds it. Parquet's page reader,
//! given exactly those pages as its locations, reads and decompresses them.

use std::collections::VecDeque;
use std::ops::Range;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use parquet::column::page::{Page, PageMetadata, PageReader};
use parquet::errors::ParquetError;
use parquet::file::metadata::ColumnChunkMetaData;
use parquet::file::page_index::offset_index::PageLocation;
use parquet::file::serialized_reader::SerializedPageReader;

use super::{ChunkPage, ColumnChunk, row_index};
use crate::approx;
use crate::error::Result;
use crate::parquet_io::Segments;

/// What a `ChunkPages` saw, shared with the `read_column` call that reads
/// through it, since the column reader owns the page reader.
#[derive(Default)]
pub(super) struct PageTally {
    /// The header value count of every data page handed to the column
    /// reader, in order.
    pub(super) seen: Vec<usize>,
    /// The data page refused for a header that claims more values than its
    /// rows; the reader's error then stands for this refusal.
    pub(super) refused: Option<usize>,
    /// The last page the page reader took up for the column reader,
    /// dictionary or data: the page a panic in decoding is reported at.
    pub(super) taken: Option<ChunkPage>,
}

/// One page a `ChunkPages` hands out, in the order it hands them.
struct Planned {
    page: ChunkPage,
    /// The page's bytes in the file, header included.
    range: Range<u64>,
    /// For a data page, the rows the offset index gives it; the column
    /// reader takes the header's value count as the values the page holds,
    /// so a header claiming more is refused before the reader holds it.
    rows: usize,
}

/// A page reader over the pages of one column chunk that hold a run of
/// rows, from bytes already held: the dictionary page first when the chunk
/// has one, then each data page from the one holding the run's first row
/// to the one holding its last. A data page whose header claims more
/// values than the offset index gives it rows is refused before the column
/// reader holds it, and recorded as refused, and a page of another kind
/// than its place holds ends the read in an error; the value count of
/// every data page handed out is recorded, so what a decode produced can
/// be checked against the offset index (`ColumnChunk::check_pages_read`).
pub(super) struct ChunkPages {
    pages: VecDeque<Planned>,
    /// The first row, within the row group, of the first data page planned;
    /// zero when none is.
    first_row: usize,
    /// Parquet's page reader over exactly the planned pages, in the plan's
    /// order.
    inner: SerializedPageReader<Segments>,
    tally: Arc<Mutex<PageTally>>,
}

impl ChunkPages {
    /// The pages of `chunk` in `segments` that hold rows `rows`, with what
    /// they hand out recorded in `tally`. No page, not even the
    /// dictionary, when `rows` is empty or the chunk has no page.
    pub(super) fn new(
        chunk: ColumnChunk<'_>,
        rows: Range<usize>,
        segments: Arc<Segments>,
        tally: Arc<Mutex<PageTally>>,
    ) -> Result<Self> {
        let locations = chunk.page_locations();
        let mut pages = VecDeque::new();
        let mut first_row = 0;
        if !rows.is_empty() && !locations.is_empty() {
            let (first, last) = (
                chunk.page_holding(rows.start),
                chunk.page_holding(rows.end - 1),
            );
            first_row = row_index(locations[first].first_row_index);
            if let Some(range) = chunk.dictionary_range() {
                pages.push_back(Planned {
                    page: ChunkPage::Dictionary,
                    range,
                    rows: 0,
                });
            }
            pages.extend((first..=last).map(|page| {
                let span = chunk.page_rows(page);
                Planned {
                    page: ChunkPage::Data(page),
                    range: chunk.page_range(page),
                    rows: row_index(span.end - span.start),
                }
            }));
        }
        let inner = parquet_pages(chunk, &pages, segments)?;
        Ok(Self {
            pages,
            first_row,
            inner,
            tally,
        })
    }

    /// The first row, within the row group, of the first data page handed
    /// out: the column reader's skip counts from it.
    pub(super) fn first_row(&self) -> usize {
        self.first_row
    }

    fn tally(&self) -> MutexGuard<'_, PageTally> {
        self.tally.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Parquet's page reader over exactly the pages of `plan`, in its order:
/// each page's location, the dictionary page's among them, and a column
/// chunk that starts at the first of them, so the reader sees no gap before
/// its first location to take for a dictionary page, and decodes each page
/// by the type its header gives.
fn parquet_pages(
    chunk: ColumnChunk<'_>,
    plan: &VecDeque<Planned>,
    segments: Arc<Segments>,
) -> Result<SerializedPageReader<Segments>> {
    let locations = plan
        .iter()
        .map(|planned| {
            let out_of_range =
                |_| ParquetError::General(format!("{} lies past i64 offsets", planned.page));
            Ok(PageLocation {
                offset: i64::try_from(planned.range.start).map_err(out_of_range)?,
                compressed_page_size: i32::try_from(planned.range.end - planned.range.start)
                    .map_err(|_| {
                        ParquetError::General(format!("{} is over i32 bytes", planned.page))
                    })?,
                first_row_index: 0,
            })
        })
        .collect::<parquet::errors::Result<Vec<_>>>()?;
    let start = locations.first().map_or(0, |location| location.offset);
    let metadata = ColumnChunkMetaData::builder(chunk.column.descriptor())
        .set_compression(chunk.metadata().compression())
        .set_data_page_offset(start)
        .build()?;
    Ok(SerializedPageReader::new(
        segments,
        &metadata,
        row_index(chunk.rows()),
        Some(locations),
    )?)
}

impl Iterator for ChunkPages {
    type Item = parquet::errors::Result<Page>;

    fn next(&mut self) -> Option<Self::Item> {
        self.get_next_page().transpose()
    }
}

impl PageReader for ChunkPages {
    fn get_next_page(&mut self) -> parquet::errors::Result<Option<Page>> {
        let Some(planned) = self.pages.pop_front() else {
            return Ok(None);
        };
        self.tally().taken = Some(planned.page);
        let page = self.inner.get_next_page()?.ok_or_else(|| {
            ParquetError::General(format!("parquet's page reader has no {}", planned.page))
        })?;
        match (planned.page, page.is_data_page()) {
            (ChunkPage::Dictionary, false) => {}
            (ChunkPage::Data(at), true) => {
                let values = approx::length(page.num_values());
                if values > planned.rows {
                    self.tally().refused = Some(at);
                    return Err(ParquetError::General(format!(
                        "data page {at} claims {values} values for {} rows",
                        planned.rows
                    )));
                }
                self.tally().seen.push(values);
            }
            (ChunkPage::Dictionary, true) | (ChunkPage::Data(_), false) => {
                return Err(ParquetError::General(format!(
                    "a {} page lies where {} does",
                    page.page_type(),
                    planned.page
                )));
            }
        }
        Ok(Some(page))
    }

    fn peek_next_page(&mut self) -> parquet::errors::Result<Option<PageMetadata>> {
        Ok(self.pages.front().map(|planned| match planned.page {
            ChunkPage::Dictionary => PageMetadata {
                num_rows: None,
                num_levels: None,
                is_dict: true,
            },
            ChunkPage::Data(_) => PageMetadata {
                num_rows: Some(planned.rows),
                num_levels: None,
                is_dict: false,
            },
        }))
    }

    fn skip_next_page(&mut self) -> parquet::errors::Result<()> {
        if self.pages.pop_front().is_some() {
            self.inner.skip_next_page()?;
        }
        Ok(())
    }

    fn at_record_boundary(&mut self) -> parquet::errors::Result<bool> {
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use parquet::basic::Compression;
    use parquet::column::reader::ColumnReaderImpl;
    use parquet::data_type::{ByteArray, ByteArrayType, DataType, Int64Type};
    use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
    use parquet::file::properties::WriterVersion;

    use super::*;
    use crate::generation::testing::Dictionary;
    use crate::index::build::{decode_run, read_one};
    use crate::index::page_header::{PageBody, PageHeader};
    use crate::index::testing::{Fixture, Layout, contract_rows, location};
    use crate::index::{Column, KeyShape, Keys};
    use crate::key::KeyType;

    /// Every `NULLS_EVERY`th value of a fixture with null values is null.
    const NULLS_EVERY: usize = 7;
    /// The fixture's key column.
    const KEY_LEAF: usize = 0;
    /// The fixture's value column.
    const VALUE_LEAF: usize = 1;

    /// The contract fixture's rows of long keys in one row group, so the
    /// chunk has pages before and after a middle one, with `edit` applied
    /// to its layout and, when `nulls`, a null value every `NULLS_EVERY`th
    /// row.
    fn written(edit: impl FnOnce(&mut Layout), nulls: bool) -> Bytes {
        let mut fixture = Fixture::contract(KeyType::Long, contract_rows());
        fixture.layout.row_group_rows = contract_rows();
        edit(&mut fixture.layout);
        if nulls {
            for value in fixture.values.iter_mut().step_by(NULLS_EVERY) {
                *value = None;
            }
        }
        fixture.write()
    }

    fn footer(file: &Bytes) -> ParquetMetaData {
        ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Required)
            .parse_and_finish(file)
            .unwrap()
    }

    /// Every row of column `leaf` of row group 0 as parquet decodes the whole
    /// chunk, page after page by their headers alone, with no offset index:
    /// the oracle a one-page read is held to. `None` is a null.
    fn parquet_decodes<T: DataType>(
        file: &Bytes,
        metadata: &ParquetMetaData,
        leaf: usize,
    ) -> Vec<Option<T::T>> {
        let chunk = metadata.row_group(0).column(leaf);
        let rows = row_index(metadata.row_group(0).num_rows());
        let pages = SerializedPageReader::new(Arc::new(file.clone()), chunk, rows, None).unwrap();
        let descr = metadata.file_metadata().schema_descr().column(leaf);
        let nullable = descr.max_def_level() > 0;
        let mut reader: ColumnReaderImpl<T> = ColumnReaderImpl::new(descr, Box::new(pages));
        let (mut values, mut levels) = (Vec::new(), Vec::new());
        let (read, _, _) = reader
            .read_records(rows, nullable.then_some(&mut levels), None, &mut values)
            .unwrap();
        assert_eq!(read, rows);
        if !nullable {
            return values.into_iter().map(Some).collect();
        }
        let mut values = values.into_iter();
        levels
            .iter()
            .map(|level| (*level > 0).then(|| values.next().unwrap()))
            .collect()
    }

    /// The bytes of `leaf`'s chunk in row group 0 that a lookup of a row in
    /// `page` holds: that data page and the dictionary page when the chunk
    /// has one, and nothing else of the file, so a read of any other page
    /// fails.
    fn held_for(file: &Bytes, chunk: ColumnChunk<'_>, page: usize) -> Arc<Segments> {
        let part = |range: Range<u64>| {
            let bytes = file.slice(approx::length(range.start)..approx::length(range.end));
            (range, bytes)
        };
        let mut parts = vec![part(chunk.page_range(page))];
        parts.extend(chunk.dictionary_range().map(part));
        Arc::new(Segments::new(parts, approx::count(file.len())))
    }

    /// The first, a middle, and the last data page of the chunk.
    fn pages_to_read(chunk: ColumnChunk<'_>) -> [usize; 3] {
        let last = chunk.page_locations().len() - 1;
        assert!(
            last >= 2,
            "a chunk of {} pages has no middle page",
            last + 1
        );
        [0, last / 2, last]
    }

    /// For the first, a middle, and the last page of row group 0 in
    /// `file`, from that page alone and the chunk's dictionary page: the
    /// page's keys as `decode_run` decodes them, and its first, a middle,
    /// and its last value as `read_one` decodes each, are what parquet
    /// decodes at those rows reading the whole chunk. The pages compared.
    fn assert_one_page_reads_match_parquet(file: &Bytes) -> usize {
        let metadata = footer(file);
        let path = location();
        let column = |leaf| Column {
            metadata: &metadata,
            leaf,
            path: &path,
        };
        let (keys, values) = (column(KEY_LEAF).group(0), column(VALUE_LEAF).group(0));
        let every_key = parquet_decodes::<Int64Type>(file, &metadata, KEY_LEAF);
        let every_value = parquet_decodes::<ByteArrayType>(file, &metadata, VALUE_LEAF);
        let mut compared = 0;
        for page in pages_to_read(keys) {
            let rows = keys.page_rows(page);
            let span = row_index(rows.start)..row_index(rows.end);
            let Keys::Long(decoded) =
                decode_run(keys, KeyShape::Long, rows, held_for(file, keys, page)).unwrap()
            else {
                panic!("a long key column decodes to longs");
            };
            let expected: Vec<i64> = every_key[span].iter().map(|key| key.unwrap()).collect();
            assert_eq!(decoded, expected, "key page {page}");
            compared += 1;
        }
        for page in pages_to_read(values) {
            let rows = values.page_rows(page);
            for row in [rows.start, rows.start.midpoint(rows.end), rows.end - 1] {
                let held = held_for(file, values, page);
                let decoded: Option<ByteArray> =
                    read_one::<ByteArrayType>(values, row, held).unwrap();
                assert_eq!(decoded, every_value[row_index(row)], "value row {row}");
            }
            compared += 1;
        }
        compared
    }

    /// The pages a lookup reads, compared in every test: three key pages
    /// and three value pages.
    const PAGES_COMPARED: usize = 6;

    #[test]
    fn test_chunk_pages_reads_the_first_a_middle_and_the_last_page_alone() {
        let file = written(|_| {}, false);
        assert_eq!(assert_one_page_reads_match_parquet(&file), PAGES_COMPARED);
    }

    #[test]
    fn test_chunk_pages_reads_a_dictionary_encoded_page_with_its_dictionary() {
        for (dictionary, encoded) in [(Dictionary::Key, KEY_LEAF), (Dictionary::Value, VALUE_LEAF)]
        {
            let file = written(|layout| layout.dictionary = dictionary, false);
            let metadata = footer(&file);
            assert!(
                metadata
                    .row_group(0)
                    .column(encoded)
                    .dictionary_page_offset()
                    .is_some(),
                "{dictionary:?}"
            );
            assert_eq!(assert_one_page_reads_match_parquet(&file), PAGES_COMPARED);
        }
    }

    /// v2 pages, with nulls among the values so their definition levels
    /// take bytes ahead of the compressed values.
    #[test]
    fn test_chunk_pages_reads_a_v2_page_with_its_levels() {
        let file = written(
            |layout| layout.writer_version = WriterVersion::PARQUET_2_0,
            true,
        );
        let metadata = footer(&file);
        let path = location();
        let values = Column {
            metadata: &metadata,
            leaf: VALUE_LEAF,
            path: &path,
        }
        .group(0);
        let range = values.page_range(0);
        let header =
            PageHeader::read(&file[approx::length(range.start)..approx::length(range.end)])
                .unwrap();
        assert!(matches!(header.body, PageBody::DataV2 { .. }), "{header:?}");
        assert!(header.levels_byte_length.is_some_and(|levels| levels > 0));
        assert_eq!(assert_one_page_reads_match_parquet(&file), PAGES_COMPARED);
    }

    /// Pages stored uncompressed, and pages of another codec than the
    /// contract's.
    #[test]
    fn test_chunk_pages_reads_a_page_of_another_codec() {
        for compression in [Compression::UNCOMPRESSED, Compression::SNAPPY] {
            let file = written(|layout| layout.compression = compression, false);
            assert_eq!(
                footer(&file).row_group(0).column(KEY_LEAF).compression(),
                compression
            );
            assert_eq!(assert_one_page_reads_match_parquet(&file), PAGES_COMPARED);
        }
    }
}
