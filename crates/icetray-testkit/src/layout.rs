//! The writer's layout knobs and the Parquet writer properties they become.
//! `Layout::contract()` is RFD 002's shape; every other fixture is a struct
//! update on it, so a test names only the clause it departs from.

use icetray_core::approx::{count, length};
use icetray_core::contract;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::SortingColumn;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::schema::types::ColumnPath;

/// The key column of every fixture table.
pub const KEY_COLUMN: &str = "key";

/// The value column of every fixture table.
pub const VALUE_COLUMN: &str = "value";

/// The false-positive rate the writer recipe sets on the key filter
/// (`write.parquet.bloom-filter-fpp.column.key` in `dev/golden-tables.py`),
/// which `Bloom::Sized` writes at.
pub const KEY_BLOOM_FPP: f64 = 0.01;

/// A false-positive rate far below `KEY_BLOOM_FPP`: one in a trillion takes
/// tens of bytes per key, the oversized filter the doctor advises on.
pub const OVERSIZED_FPP: f64 = 1e-12;

/// A false-positive rate far above `KEY_BLOOM_FPP`: one in two takes a few
/// bits per key, the undersized filter the doctor advises on.
pub const UNDERSIZED_FPP: f64 = 0.5;

/// The write batch under a page byte bound. The byte limit is checked
/// between batches, so a batch larger than the bound would overshoot it by
/// a batch; two rows keeps the overshoot to one value.
const BYTE_BOUNDED_BATCH_ROWS: usize = 2;

/// The key column's position in every fixture schema, which the file's
/// sorting column names.
const KEY_COLUMN_INDEX: i32 = 0;

/// Which fixture column is dictionary-encoded; the other never is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dictionary {
    /// Neither: the contract's shape.
    Off,
    /// The key column, whose dictionary page costs nothing extra once the
    /// chunk is resident.
    Key,
    /// The value column, whose dictionary page is one more GET per hit.
    Value,
}

/// Whether and how the key column gets a bloom filter. The writer sizes the
/// filter for the file's keys and folds it down to the smallest size that
/// meets the false-positive rate, so the rate is the one knob that sets its
/// bytes per key.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Bloom {
    /// None: what the Bloom tier refuses and the doctor advises on.
    Off,
    /// Sized to the file's keys at `KEY_BLOOM_FPP`: the contract's filter.
    Sized,
    /// Sized to the file's keys at this false-positive rate: far below
    /// `KEY_BLOOM_FPP` an oversized filter, far above it an undersized one.
    Fpp(f64),
}

/// What the key column's Parquet statistics carry, which is where the
/// Iceberg writer takes a data file's manifest bounds from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Statistics {
    /// Exact bounds and a whole column index.
    Full,
    /// Row-group statistics cut to this many bytes, the shape Iceberg's
    /// default metrics mode leaves; the Iceberg writer records a manifest
    /// bound only from an exact statistic, so the manifest carries none for
    /// the key, while the column index (cut by `column_index_truncate`
    /// alone) is still written.
    Truncated(usize),
    /// No statistics: no bounds and no column index. The offset index is
    /// still written, which is all the reader needs.
    None,
}

/// The writer's layout knobs.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Layout {
    /// Rows per page, key and value alike.
    pub page_rows: usize,
    /// A byte bound on every page besides the row bound; `None` leaves the
    /// row bound alone.
    pub page_bytes: Option<usize>,
    /// Which column is dictionary-encoded.
    pub dictionary: Dictionary,
    /// The column index's truncation length; `None` keeps whole values.
    pub column_index_truncate: Option<usize>,
    /// What the key column's statistics carry.
    pub statistics: Statistics,
    /// The key column's bloom filter.
    pub bloom: Bloom,
    /// Rows per row group; `None` leaves the writer's default, one row group
    /// for every fixture this crate writes.
    pub row_group_rows: Option<usize>,
    /// The size at which the rolling writer starts a new file.
    pub target_file_bytes: usize,
}

impl Layout {
    /// RFD 002's shape: contract pages bounded by rows alone, no dictionary,
    /// a whole column index, exact statistics, a sized filter, one row
    /// group, files rolled at the contract's target.
    pub fn contract() -> Self {
        Self {
            page_rows: usize::try_from(contract::PAGE_ROWS)
                .expect("the contract's page rows fit usize"),
            page_bytes: None,
            dictionary: Dictionary::Off,
            column_index_truncate: None,
            statistics: Statistics::Full,
            bloom: Bloom::Sized,
            row_group_rows: None,
            target_file_bytes: length(contract::TARGET_FILE_BYTES_MAX),
        }
    }

    /// The writer properties for a file of `rows` rows, which sizes the key
    /// filter. The write batch is the page row limit so that limit binds
    /// exactly, or `BYTE_BOUNDED_BATCH_ROWS` under a byte bound. The filter
    /// setters enable a filter on the column they name, so the value column
    /// never gets one.
    pub(crate) fn properties(&self, rows: usize) -> WriterProperties {
        let batch_rows = if self.page_bytes.is_some() {
            BYTE_BOUNDED_BATCH_ROWS
        } else {
            self.page_rows
        };
        let key = ColumnPath::from(KEY_COLUMN);
        let mut builder = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_data_page_row_count_limit(self.page_rows)
            .set_write_batch_size(batch_rows)
            .set_dictionary_enabled(false)
            .set_column_index_truncate_length(self.column_index_truncate)
            .set_bloom_filter_enabled(false)
            .set_max_row_group_row_count(self.row_group_rows)
            .set_sorting_columns(Some(vec![SortingColumn {
                column_idx: KEY_COLUMN_INDEX,
                descending: false,
                nulls_first: false,
            }]));
        if let Some(page_bytes) = self.page_bytes {
            builder = builder.set_data_page_size_limit(page_bytes);
        }
        let dictionary_column = match self.dictionary {
            Dictionary::Off => None,
            Dictionary::Key => Some(KEY_COLUMN),
            Dictionary::Value => Some(VALUE_COLUMN),
        };
        if let Some(column) = dictionary_column {
            builder = builder.set_column_dictionary_enabled(ColumnPath::from(column), true);
        }
        builder = match self.statistics {
            Statistics::Full => builder
                .set_statistics_enabled(EnabledStatistics::Page)
                .set_statistics_truncate_length(None),
            Statistics::Truncated(bytes) => builder
                .set_statistics_enabled(EnabledStatistics::Page)
                .set_statistics_truncate_length(Some(bytes)),
            Statistics::None => builder.set_statistics_enabled(EnabledStatistics::None),
        };
        let fpp = match self.bloom {
            Bloom::Off => None,
            Bloom::Sized => Some(KEY_BLOOM_FPP),
            Bloom::Fpp(fpp) => Some(fpp),
        };
        if let Some(fpp) = fpp {
            builder = builder
                .set_column_bloom_filter_enabled(key.clone(), true)
                .set_column_bloom_filter_fpp(key.clone(), fpp)
                .set_column_bloom_filter_ndv(key, count(rows));
        }
        builder.build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Rows the property tests size their filter for.
    const ROWS: usize = 1_000;

    /// A false-positive rate other than the recipe's.
    const OTHER_FPP: f64 = 0.5;

    /// A page byte bound for the batch test.
    const PAGE_BYTES: usize = 4_096;

    /// A truncation length for the statistics test.
    const TRUNCATE: usize = 8;

    #[test]
    fn test_contract_derives_from_contract_constants() {
        let layout = Layout::contract();
        assert_eq!(
            count(layout.page_rows),
            u64::try_from(contract::PAGE_ROWS).unwrap()
        );
        assert_eq!(
            count(layout.target_file_bytes),
            contract::TARGET_FILE_BYTES_MAX
        );
        assert_eq!(layout.page_bytes, None);
        assert_eq!(layout.dictionary, Dictionary::Off);
        assert_eq!(layout.column_index_truncate, None);
        assert_eq!(layout.statistics, Statistics::Full);
        assert_eq!(layout.bloom, Bloom::Sized);
        assert_eq!(layout.row_group_rows, None);
    }

    #[test]
    fn test_properties_enable_a_filter_on_the_key_column_only_when_bloom_is_on() {
        let key = ColumnPath::from(KEY_COLUMN);
        let value = ColumnPath::from(VALUE_COLUMN);
        let off = Layout {
            bloom: Bloom::Off,
            ..Layout::contract()
        }
        .properties(ROWS);
        assert!(off.bloom_filter_properties(&key).is_none());
        assert!(off.bloom_filter_properties(&value).is_none());

        let sized = Layout::contract().properties(ROWS);
        let filter = sized.bloom_filter_properties(&key).unwrap();
        assert_eq!(filter.fpp.to_bits(), KEY_BLOOM_FPP.to_bits());
        assert_eq!(filter.ndv, count(ROWS));
        assert!(sized.bloom_filter_properties(&value).is_none());

        let rated = Layout {
            bloom: Bloom::Fpp(OTHER_FPP),
            ..Layout::contract()
        }
        .properties(ROWS);
        assert_eq!(
            rated.bloom_filter_properties(&key).unwrap().fpp.to_bits(),
            OTHER_FPP.to_bits()
        );
        assert!(rated.bloom_filter_properties(&value).is_none());
    }

    #[test]
    fn test_properties_batch_is_the_page_rows_unless_byte_bounded() {
        let row_bounded = Layout::contract().properties(ROWS);
        assert_eq!(row_bounded.write_batch_size(), Layout::contract().page_rows);
        assert_eq!(
            row_bounded.data_page_row_count_limit(),
            Layout::contract().page_rows
        );
        let byte_bounded = Layout {
            page_bytes: Some(PAGE_BYTES),
            ..Layout::contract()
        }
        .properties(ROWS);
        assert_eq!(byte_bounded.write_batch_size(), BYTE_BOUNDED_BATCH_ROWS);
        assert_eq!(
            byte_bounded.column_data_page_size_limit(&ColumnPath::from(VALUE_COLUMN)),
            PAGE_BYTES
        );
    }

    #[test]
    fn test_properties_dictionary_follows_the_named_column() {
        let key = ColumnPath::from(KEY_COLUMN);
        let value = ColumnPath::from(VALUE_COLUMN);
        let cases = [
            (Dictionary::Off, false, false),
            (Dictionary::Key, true, false),
            (Dictionary::Value, false, true),
        ];
        for (dictionary, on_key, on_value) in cases {
            let properties = Layout {
                dictionary,
                ..Layout::contract()
            }
            .properties(ROWS);
            assert_eq!(
                properties.dictionary_enabled(&key),
                on_key,
                "{dictionary:?}"
            );
            assert_eq!(
                properties.dictionary_enabled(&value),
                on_value,
                "{dictionary:?}"
            );
        }
    }

    #[test]
    fn test_properties_statistics_modes() {
        let key = ColumnPath::from(KEY_COLUMN);
        let full = Layout::contract().properties(ROWS);
        assert_eq!(full.statistics_enabled(&key), EnabledStatistics::Page);
        assert_eq!(full.statistics_truncate_length(), None);
        let truncated = Layout {
            statistics: Statistics::Truncated(TRUNCATE),
            ..Layout::contract()
        }
        .properties(ROWS);
        assert_eq!(truncated.statistics_enabled(&key), EnabledStatistics::Page);
        assert_eq!(truncated.statistics_truncate_length(), Some(TRUNCATE));
        let none = Layout {
            statistics: Statistics::None,
            ..Layout::contract()
        }
        .properties(ROWS);
        assert_eq!(none.statistics_enabled(&key), EnabledStatistics::None);
    }
}
