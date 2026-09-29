//! The reader `verify` checks the common key column with at load: a
//! `BYTE_ARRAY` column chunk of `PLAIN` data pages and no dictionary page,
//! neither nested nor repeated, whose values are all present. Each page is
//! fetched and decompressed as for parquet's column reader (`ChunkPages`),
//! and its values are sliced from the decompressed page in place, so a key
//! checked costs no allocation and no reference count; only a key `RunCheck`
//! keeps (a fence, a first or last key, a key an error names) is copied
//! out. A chunk of any other shape, and a chunk holding any page this
//! reader cannot read exactly, is left whole to the column reader, which
//! reads it as it reads every other chunk: this reader either decodes a
//! row group to exactly the keys the column reader decodes, from exactly
//! the pages the offset index names, or decodes none of it.

use std::cmp::Ordering;
use std::mem::size_of;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex, PoisonError};

use parquet::basic::Encoding;
use parquet::column::page::{Page, PageReader};

use super::build::{DECODE_BATCH_RECORDS, KeyBatch, RunCheck, is_utf8};
use super::page_header::{BYTE_ARRAY_LENGTH_PREFIX_BYTES, whole_varint};
use super::pages::{ChunkPages, PageTally};
use super::{ColumnChunk, row_index};
use crate::approx;
use crate::key::Key;
use crate::parquet_io::Segments;

/// The definition level of a present value in a column neither nested nor
/// repeated, and so the most such a column's levels reach: a nullable
/// top-level column's.
const PRESENT_LEVEL: i16 = 1;

/// The bits an RLE/bit-packed hybrid run header shifts its count by: the
/// low bit says which kind of run follows, clear for an RLE run.
const RUN_KIND_BITS: u32 = 1;

/// The bytes of a v1 data page's length of its RLE levels: a little-endian
/// `i32`, as parquet reads it.
const LEVELS_LENGTH_BYTES: usize = size_of::<i32>();

/// What a `BYTE_ARRAY` key column holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ByteKeys {
    /// UTF-8 strings.
    Text,
    /// Opaque bytes.
    Bytes,
}

/// Every key of row group `chunk`, a `BYTE_ARRAY` column holding `keys`,
/// read from `segments` plain and checked (`RunCheck`); `None` when the
/// chunk is not of the shape this reader reads, or any page of it is not
/// one it reads exactly, for the column reader to read instead. Exactly:
/// every data page `PLAIN`, its definition levels, where the column has
/// them, one RLE run of the page's value count at `PRESENT_LEVEL`, its
/// values each a length prefix and the bytes it counts, as many as the
/// header's value count, with no byte left after the last, and the pages
/// together the ones `ColumnChunk::check_pages_read` accepts for the row
/// group's rows. Anything the pages fault on (a header, a decompression,
/// a count past the offset index, a panic) ends this read the same way,
/// with nothing reported, so the column reader meets the fault and reports
/// it. The decode runs under `catch_unwind` for that reason: a panic in it
/// is the column reader's to meet and report.
pub(super) fn checked_run(
    chunk: ColumnChunk<'_>,
    keys: ByteKeys,
    segments: Arc<Segments>,
) -> Option<RunCheck<'_>> {
    let descr = chunk.column.descriptor();
    if descr.max_rep_level() != 0
        || descr.max_def_level() > PRESENT_LEVEL
        || chunk.dictionary_range().is_some()
    {
        return None;
    }
    let required = descr.max_def_level() == 0;
    let rows = row_index(chunk.rows());
    let tally = Arc::new(Mutex::new(PageTally::default()));
    let mut run = RunCheck::new(chunk);
    // Unwind-safe: the pages are built inside the call and dropped with a
    // panic, the footer `chunk` borrows is only read, and the run and the
    // tally a panic can leave mid-page are dropped unread.
    let read = std::panic::catch_unwind(AssertUnwindSafe(|| {
        read_pages(chunk, rows, segments, &tally, keys, required, &mut run)
    }));
    // intentional: a panic is the column reader's to meet and report.
    let decoded = read.ok()??;
    let seen = std::mem::take(&mut tally.lock().unwrap_or_else(PoisonError::into_inner).seen);
    // intentional: pages that contradict the offset index are the column
    // reader's to refuse, as `FileFault::PageRowsMismatch`.
    chunk.check_pages_read(0..rows, &seen, Ok(decoded)).ok()?;
    Some(run)
}

/// The data pages of `chunk` holding rows `0..rows`, each read plain and
/// handed to `run` a decode batch at a time, with what the page reader saw
/// recorded in `tally`: the values read, or `None` at the first page not
/// read exactly. A batch holds at most `DECODE_BATCH_RECORDS` values
/// however many a header counts, so no buffer here is sized by a count
/// before the values it counts have been read.
fn read_pages(
    chunk: ColumnChunk<'_>,
    rows: usize,
    segments: Arc<Segments>,
    tally: &Arc<Mutex<PageTally>>,
    keys: ByteKeys,
    required: bool,
    run: &mut RunCheck<'_>,
) -> Option<usize> {
    // intentional: a chunk parquet's page reader cannot plan is the column
    // reader's to report.
    let mut pages = ChunkPages::new(chunk, 0..rows, segments, Arc::clone(tally)).ok()?;
    let batch_records = row_index(DECODE_BATCH_RECORDS);
    let mut decoded = 0;
    // intentional: a page the page reader faults on is the column
    // reader's to report.
    while let Some(page) = pages.get_next_page().ok()? {
        let (mut rest, count) = plain_values(&page, required)?;
        let mut left = count;
        while left > 0 {
            let take = left.min(batch_records);
            let mut values = Vec::with_capacity(take);
            for _ in 0..take {
                values.push(next_value(&mut rest)?);
            }
            left -= take;
            run.visit(&PlainKeys { keys, values });
        }
        if !rest.is_empty() {
            return None;
        }
        decoded += count;
    }
    Some(decoded)
}

/// The values of data page `page` and how many its header counts, when
/// the page is one `checked_run` reads: `PLAIN`, and, where the column is
/// not `required`, with definition levels that are one run of present
/// values (`all_present`). A v1 page's levels are a length, then that many
/// bytes, ahead of the values; a v2 page's are the header's definition
/// level bytes, after its repetition level bytes, which must be none, and
/// its header must count no null. A dictionary page is not one.
fn plain_values(page: &Page, required: bool) -> Option<(&[u8], usize)> {
    match page {
        Page::DataPage {
            buf,
            num_values,
            encoding: Encoding::PLAIN,
            def_level_encoding,
            ..
        } => {
            let count = approx::length(*num_values);
            if required {
                return Some((buf, count));
            }
            if *def_level_encoding != Encoding::RLE {
                return None;
            }
            let (length, rest) = buf.split_first_chunk::<LEVELS_LENGTH_BYTES>()?;
            let length = approx::length(u32::from_le_bytes(*length));
            let (levels, values) = rest.split_at_checked(length)?;
            all_present(levels, count).then_some((values, count))
        }
        Page::DataPageV2 {
            buf,
            num_values,
            encoding: Encoding::PLAIN,
            num_nulls: 0,
            def_levels_byte_len,
            rep_levels_byte_len: 0,
            ..
        } => {
            let count = approx::length(*num_values);
            let (levels, values) = buf.split_at_checked(approx::length(*def_levels_byte_len))?;
            let present = if required {
                levels.is_empty()
            } else {
                all_present(levels, count)
            };
            present.then_some((values, count))
        }
        Page::DataPage { .. } | Page::DataPageV2 { .. } | Page::DictionaryPage { .. } => None,
    }
}

/// Whether `levels`, the RLE/bit-packed hybrid definition levels of a
/// page of `count` values in a column whose levels take one bit, are
/// exactly one RLE run of `count` values at `PRESENT_LEVEL`: the run
/// header, a varint of the count shifted past the run-kind bit, which is
/// clear, then the level in one byte, and nothing after it.
fn all_present(levels: &[u8], count: usize) -> bool {
    let Some((level, header)) = levels.split_last() else {
        return false;
    };
    i16::from(*level) == PRESENT_LEVEL
        && whole_varint(header) == Some(approx::count(count) << RUN_KIND_BITS)
}

/// The `PLAIN` byte array at the start of `rest`, which then starts past
/// it: `None` when `rest` holds less than its length prefix, or less after
/// the prefix than the bytes it counts.
fn next_value<'p>(rest: &mut &'p [u8]) -> Option<&'p [u8]> {
    let (prefix, after) = rest.split_first_chunk::<BYTE_ARRAY_LENGTH_PREFIX_BYTES>()?;
    let (value, after) = after.split_at_checked(approx::length(u32::from_le_bytes(*prefix)))?;
    *rest = after;
    Some(value)
}

/// One decode batch of a plain page's keys, in file order, each a slice of
/// the decompressed page.
struct PlainKeys<'p> {
    keys: ByteKeys,
    values: Vec<&'p [u8]>,
}

impl KeyBatch for PlainKeys<'_> {
    fn len(&self) -> usize {
        self.values.len()
    }

    fn is_utf8(&self) -> bool {
        match self.keys {
            ByteKeys::Text => self.values.iter().all(|value| is_utf8(value)),
            ByteKeys::Bytes => true,
        }
    }

    fn max_len(&self) -> usize {
        self.values
            .iter()
            .map(|value| value.len())
            .max()
            .unwrap_or(0)
    }

    fn key(&self, at: usize) -> Key {
        let value = self.values[at];
        match self.keys {
            ByteKeys::Text => Key::Str(String::from_utf8(value.to_vec()).expect(
                "RunCheck reads a key of a string batch back only once is_utf8 holds for it",
            )),
            ByteKeys::Bytes => Key::Bytes(value.to_vec()),
        }
    }

    fn first_not_increasing(&self) -> Option<(usize, Ordering)> {
        let at = self.values.windows(2).position(|pair| pair[0] >= pair[1])? + 1;
        Some((at, self.values[at].cmp(self.values[at - 1])))
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    /// `values` as a `PLAIN` byte-array body: each length prefix, then its
    /// bytes.
    fn plain(values: &[&[u8]]) -> Vec<u8> {
        values
            .iter()
            .flat_map(|value| {
                let len = u32::try_from(value.len()).unwrap().to_le_bytes();
                len.into_iter().chain(value.iter().copied())
            })
            .collect()
    }

    /// One RLE run of `count` levels at `level`, as a writer encodes one
    /// of a single bit: the run header's varint, then the level's byte.
    fn run_of(count: u8, level: u8) -> Vec<u8> {
        vec![count << RUN_KIND_BITS, level]
    }

    fn v1(buf: Vec<u8>, count: u32, encoding: Encoding, levels: Encoding) -> Page {
        Page::DataPage {
            buf: Bytes::from(buf),
            num_values: count,
            encoding,
            def_level_encoding: levels,
            rep_level_encoding: Encoding::RLE,
            statistics: None,
        }
    }

    fn v2(buf: Vec<u8>, count: u32, def_levels: u32, nulls: u32, rep_levels: u32) -> Page {
        Page::DataPageV2 {
            buf: Bytes::from(buf),
            num_values: count,
            encoding: Encoding::PLAIN,
            num_nulls: nulls,
            num_rows: count,
            def_levels_byte_len: def_levels,
            rep_levels_byte_len: rep_levels,
            is_compressed: true,
            statistics: None,
        }
    }

    /// `levels` as a v1 page lays them out: their length, then the bytes.
    fn v1_levels(levels: &[u8]) -> Vec<u8> {
        let mut laid = u32::try_from(levels.len()).unwrap().to_le_bytes().to_vec();
        laid.extend_from_slice(levels);
        laid
    }

    /// Every value of `body` as `read_pages` reads a page of `count`, or
    /// `None` when it would leave the page to the column reader.
    fn read_exactly(mut body: &[u8], count: usize) -> Option<Vec<&[u8]>> {
        let values = (0..count)
            .map(|_| next_value(&mut body))
            .collect::<Option<Vec<_>>>()?;
        body.is_empty().then_some(values)
    }

    #[test]
    fn test_next_value_reads_exact_values_and_refuses_what_the_page_does_not_hold() {
        let values: [&[u8]; 3] = [b"a", b"", b"xyz"];
        let body = plain(&values);
        assert_eq!(read_exactly(&body, values.len()).unwrap(), values);
        assert_eq!(read_exactly(&[], 0).unwrap(), Vec::<&[u8]>::new());

        let mut past_end = body.clone();
        let last = body.len() - b"xyz".len() - BYTE_ARRAY_LENGTH_PREFIX_BYTES;
        past_end[last] += 1;
        assert_eq!(
            read_exactly(&past_end, values.len()),
            None,
            "one byte past the end"
        );
        let prefix_cut = &body[..last + BYTE_ARRAY_LENGTH_PREFIX_BYTES - 1];
        assert_eq!(
            read_exactly(prefix_cut, values.len()),
            None,
            "a prefix cut short"
        );
        let mut trailing = body.clone();
        trailing.push(0);
        assert_eq!(
            read_exactly(&trailing, values.len()),
            None,
            "a byte after the last"
        );
        assert_eq!(
            read_exactly(&body, values.len() + 1),
            None,
            "a value more than held"
        );
        assert_eq!(
            read_exactly(&body, values.len() - 1),
            None,
            "a value fewer than held"
        );
        let mut huge = body.clone();
        huge[..BYTE_ARRAY_LENGTH_PREFIX_BYTES].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(
            read_exactly(&huge, values.len()),
            None,
            "a prefix of u32::MAX"
        );
    }

    #[test]
    fn test_all_present_takes_one_run_of_the_count_at_the_present_level_only() {
        let count = 5;
        let present = u8::try_from(PRESENT_LEVEL).unwrap();
        assert!(all_present(&run_of(count, present), usize::from(count)));
        assert!(
            !all_present(&run_of(count, 0), usize::from(count)),
            "a run of nulls"
        );
        assert!(!all_present(
            &run_of(count, present),
            usize::from(count) + 1
        ));
        assert!(!all_present(
            &run_of(count, present),
            usize::from(count) - 1
        ));
        let mut padded = run_of(count, present);
        padded.push(0);
        assert!(
            !all_present(&padded, usize::from(count)),
            "a byte after the run"
        );
        let bit_packed = [(1 << RUN_KIND_BITS) | 1, u8::MAX];
        assert!(
            !all_present(&bit_packed, usize::from(count)),
            "a bit-packed run"
        );
        assert!(!all_present(&[], 0), "no run at all");
        // A run of 300 has a header of two varint bytes: 600 is 0xd8 0x04.
        let long_count = 300;
        let long_run = [0xd8, 0x04, present];
        assert!(all_present(&long_run, long_count));
        assert!(
            !all_present(&long_run[1..], long_count),
            "a varint cut short"
        );
        assert!(
            !all_present(&[0xd8, present], long_count),
            "a varint that does not end"
        );
    }

    #[test]
    fn test_plain_values_reads_plain_pages_whose_levels_are_all_present() {
        let values: [&[u8]; 2] = [b"a", b"b"];
        let count = u8::try_from(values.len()).unwrap();
        let body = plain(&values);
        let present = u8::try_from(PRESENT_LEVEL).unwrap();
        let levels = run_of(count, present);
        let values_of = |page: &Page, required| {
            plain_values(page, required).map(|(values, count)| (values.to_vec(), count))
        };
        let expected = Some((body.clone(), values.len()));
        let with = |prefix: &[u8]| [prefix, &body].concat();
        let n = u32::from(count);

        assert_eq!(
            values_of(&v1(body.clone(), n, Encoding::PLAIN, Encoding::RLE), true),
            expected
        );
        let nullable = v1(with(&v1_levels(&levels)), n, Encoding::PLAIN, Encoding::RLE);
        assert_eq!(values_of(&nullable, false), expected);
        let with_null = v1(
            with(&v1_levels(&run_of(count, 0))),
            n,
            Encoding::PLAIN,
            Encoding::RLE,
        );
        assert_eq!(values_of(&with_null, false), None, "levels not all present");
        let other_levels = v1(
            with(&v1_levels(&levels)),
            n,
            Encoding::PLAIN,
            Encoding::PLAIN,
        );
        assert_eq!(values_of(&other_levels, false), None, "levels not RLE");
        let dictionary = v1(body.clone(), n, Encoding::RLE_DICTIONARY, Encoding::RLE);
        assert_eq!(values_of(&dictionary, true), None, "values not PLAIN");
        let mut levels_past_end = v1_levels(&levels);
        levels_past_end[0] = u8::MAX;
        let past = v1(with(&levels_past_end), n, Encoding::PLAIN, Encoding::RLE);
        assert_eq!(
            values_of(&past, false),
            None,
            "a level length past the page"
        );
        let no_length = v1(
            vec![0; LEVELS_LENGTH_BYTES - 1],
            n,
            Encoding::PLAIN,
            Encoding::RLE,
        );
        assert_eq!(
            values_of(&no_length, false),
            None,
            "a level length cut short"
        );

        let levels_len = u32::try_from(levels.len()).unwrap();
        assert_eq!(values_of(&v2(body.clone(), n, 0, 0, 0), true), expected);
        assert_eq!(
            values_of(&v2(with(&levels), n, levels_len, 0, 0), false),
            expected
        );
        assert_eq!(
            values_of(&v2(with(&levels), n, levels_len, 0, 0), true),
            None,
            "levels a required column has none of"
        );
        assert_eq!(
            values_of(&v2(with(&levels), n, levels_len, 1, 0), false),
            None,
            "a null counted"
        );
        assert_eq!(
            values_of(&v2(with(&levels), n, levels_len, 0, 1), false),
            None,
            "repetition levels"
        );
        assert_eq!(
            values_of(&v2(body.clone(), n, u32::MAX, 0, 0), false),
            None,
            "levels past the page"
        );
        let dictionary_page = Page::DictionaryPage {
            buf: Bytes::from(body),
            num_values: n,
            encoding: Encoding::PLAIN,
            is_sorted: true,
        };
        assert_eq!(values_of(&dictionary_page, true), None);
    }
}
