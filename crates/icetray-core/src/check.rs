//! The doctor (RFD 007, RFD 016, RFD 020, RFD 021): the finding codes, the
//! levels they cap, penalties, and thresholds by which a table is
//! classified, applied identically at every load and in the `doctor`
//! subcommand. `diagnose` is a pure function of a loaded `Generation` and
//! the table's declared level: it reads the footers, page indexes, fences,
//! manifest entries, and applied delete state the generation holds and
//! nothing else, so the same generation always gets the same report.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use parquet::basic::Encoding;
use parquet::file::metadata::{ColumnChunkMetaData, ParquetMetaData};
use parquet::file::page_index::column_index::ColumnIndexMetaData;

use crate::approx;
use crate::config::Residency;
use crate::contract;
use crate::deletes::DeleteSummary;
use crate::error::{Error, RefusalReason, Result};
use crate::generation::{Generation, LoadedFile};
use crate::index::{
    FileIndex, Inspected, ResidentBytes, bits_per_key, saturating_sum, saturating_sum_all,
};
use crate::key::{Key, KeyType};
use crate::level::Level;
use crate::manifest::{DataFileEntry, TableLayout};
use crate::snapshot::SnapshotId;

/// The thresholds, in one place (RFD 016): each derives from `contract`
/// where a contract number exists, and each names the analysis section that
/// set it.
pub mod thresholds {
    use crate::contract;

    /// Key page rows above which `large_key_pages` caps the level: twice
    /// `contract::PAGE_ROWS`. A lookup decodes one key page, so its rows
    /// set the CPU a lookup spends (prototype findings, finding 1: the
    /// writer's default 17,900-row pages served at the same GET count and
    /// cost CPU per lookup instead). The slack up to twice the contract is a
    /// chosen margin, so a writer whose page rounding overshoots the
    /// contract draws no finding (RFD 016, consequences).
    pub const KEY_PAGE_ROWS_LARGE: i64 = 2 * contract::PAGE_ROWS;

    /// Mean uncompressed value page bytes above which `large_value_pages`
    /// is advice: 1 MiB, `VALUE_PAGE_ADVICE_FACTOR` times
    /// `contract::VALUE_PAGE_BYTES`. The one value page a cold hit fetches
    /// is the bytes it transfers and decompresses, and that cost is linear
    /// in the page's bytes with no step anywhere (icetray against real S3,
    /// results: 192 KB pages served at the same cold latency as 64
    /// KiB ones), so the line is drawn where the transfer itself reaches
    /// the order of a round trip: at object-store bandwidth a megabyte
    /// takes tens of milliseconds, which shows in a cold hit, while a few
    /// hundred kilobytes do not. A row limit alone leaves pages of many
    /// megabytes for large values (prototype findings, finding 6); the
    /// contract's byte bound of RFD 002 is the recipe for those, and
    /// `FileReport::avg_value_bytes` beside `avg_value_page_bytes` shows an
    /// owner how many values a hit fetches for the one it wanted.
    pub(crate) const VALUE_PAGE_BYTES_ADVICE: u64 =
        VALUE_PAGE_ADVICE_FACTOR * contract::VALUE_PAGE_BYTES;

    /// How many contract value pages the advice threshold spans: sixteen,
    /// which makes it 1 MiB.
    pub(crate) const VALUE_PAGE_ADVICE_FACTOR: u64 = 16;

    /// Mean uncompressed value page bytes above which
    /// `value_pages_over_ceiling` is advice: the lookup's own ceiling on a
    /// value page's declared size, 64 MiB, held where the lookup enforces
    /// it and not redeclared here, so the finding and the refusal it warns
    /// of cannot drift apart.
    pub use crate::index::MOST_VALUE_PAGE_BYTES;

    /// Data file bytes above which `large_file` is advice: twice
    /// `contract::TARGET_FILE_BYTES_MAX`. First-byte latency of a range GET
    /// is three to four times higher on objects of 256 MiB and above than
    /// on 64 MiB and below, and the slow mode begins somewhere in the
    /// unmeasured band between (range-GET latency, what the evidence
    /// implies); 128 MiB is the judgment inside that band RFD 016 records.
    pub const FILE_BYTES_ADVICE: u64 = 2 * contract::TARGET_FILE_BYTES_MAX;

    /// `many_files_per_bucket` is advice when a bucket holds more than this
    /// many times the files its bytes explain at
    /// `contract::TARGET_FILE_BYTES_MAX` per file. A sorted write task that
    /// rolls files at the target is not many files; a batch writer that
    /// splits a bucket across tasks is (prototype findings, finding 9).
    pub const FILES_PER_BUCKET_FACTOR: u64 = 2;

    /// Bloom filter bytes per key above which `bloom_oversized` is advice.
    /// A filter sized to its row group at one percent false positives is
    /// about 1.2 bytes per key; Iceberg 1.10.1 wrote a fixed 1 MiB per row
    /// group, 17 to 20 bytes per key at 62,500 rows and 856 on a small
    /// append file, and the bloom tier's memory follows it (bloom residency
    /// tier, observations; prototype findings, finding 7).
    pub const BLOOM_BYTES_PER_KEY_ADVICE: f64 = 4.0;

    /// Bloom filter bits per key below which `bloom_undersized` is advice.
    /// Four bits per key gave 29 false positives per 100 misses, each a GET
    /// the bloom tier's miss otherwise avoids; at 17 bits none appeared in
    /// 100 (bloom residency tier, observations).
    pub const BLOOM_BITS_PER_KEY_MIN: f64 = 8.0;

    /// The share of key pages with a distinct column-index minimum below
    /// which `column_index_truncated` is advice. parquet-mr cuts page
    /// minimums to 64 bytes by default, so on a table of 91-byte keys every
    /// page of a file reported one minimum; icetray routes by resident keys
    /// and served it at one GET (prototype findings, method and finding 1),
    /// but other readers of the same files cannot skip pages by such an
    /// index (RFD 002).
    pub const DISTINCT_MINS_RATIO_ADVICE: f64 = 0.5;
}

/// The GETs a cold lookup spends on a column chunk's dictionary page beside
/// the page it decodes: for the value column on a hit, the penalty of
/// `dictionary_encoded_value` (RFD 007); for the key column under `bloom`,
/// on each file whose filter answers yes (RFD 017).
const DICTIONARY_PAGE_GETS: u64 = 1;

/// The GETs a cold hit spends on its value page (RFD 006).
const VALUE_PAGE_GETS: u64 = 1;

/// The GETs a cold lookup under `bloom` spends on the key page of each file
/// it probes whose filter answers yes (RFD 017); `full` holds the page and
/// `disk` reads it from a chunk file, neither with a GET.
const KEY_PAGE_GETS: u64 = 1;

/// The files a lookup probes where no files overlap: the one its key routes
/// to.
const FILES_PROBED_ALONE: usize = 1;

/// The files a bucket's bytes explain at the least: one, however few bytes
/// it holds, so a bucket of one small file is never many files.
const MIN_EXPLAINED_FILES: u64 = 1;

/// The distinct page minimums ratio of a key column whose column index
/// cannot show truncation: no index, or an index that lists no page. Every
/// page counts as distinct, and the file draws no
/// `column_index_truncated`.
const DISTINCT_MINS_UNOBSERVABLE: f64 = 1.0;

/// A capping finding's penalty per lookup over the contract layout, in RFD
/// 007's three units; a unit the finding does not affect is `None`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct Penalty {
    /// GETs a hit makes beyond the one value page.
    pub extra_gets_per_hit: Option<u64>,
    /// Keys a lookup decodes, where the contract decodes `contract::PAGE_ROWS`.
    pub keys_decoded_per_lookup: Option<i64>,
    /// Files a lookup may probe, where the contract probes one.
    pub files_probed_per_lookup: Option<usize>,
}

impl Penalty {
    /// A penalty in files probed per lookup alone, the unit every
    /// table-level cap is measured in.
    pub(crate) fn files_probed(files: usize) -> Self {
        Self {
            files_probed_per_lookup: Some(files),
            ..Self::default()
        }
    }
}

/// What one cold lookup pays at worst on a layout in a residency (RFD 020):
/// the numbers behind its level, which rise before the level changes.
/// Observed traffic is `icetray_lookups_total`'s, where a warm hit pays
/// less than `gets_per_hit`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct LookupCost {
    /// GETs a cold hit makes: its value page, the value column's dictionary
    /// page when any file's is dictionary-encoded, and under `bloom` the key
    /// page of every file probed, each filter answering yes, with the key
    /// column's dictionary page when any file's is dictionary-encoded.
    pub gets_per_hit: u64,
    /// GETs a miss makes: none under `full` and `disk`; under `bloom` the
    /// key pages a hit spends, every filter answering yes.
    pub gets_per_miss: u64,
    /// Files a lookup probes at most: the most in any bucket
    /// (`Generation::overlapping_buckets`, which takes unbucketed files as
    /// one bucket), one where no files overlap; an upper bound where files
    /// were written under two or more bucketings.
    pub files_probed: usize,
}

/// A finding's code (RFD 016). Display and serde give the `snake_case` code
/// that appears in the doctor's output and the `code` metric label; code
/// never compares the strings. The derived order is the finding table's
/// order, which the report sorts by after putting capping findings first.
// The serde names and Display are the same strings;
// `test_serialize_finding_code_matches_display` holds them together.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FindingCode {
    /// The value column is dictionary-encoded; its dictionary page costs one
    /// extra GET per hit.
    DictionaryEncodedValue,
    /// Key pages hold more rows than `thresholds::KEY_PAGE_ROWS_LARGE`;
    /// a lookup decodes them all.
    LargeKeyPages,
    /// No bucket transform on the key and more than one file; a lookup
    /// probes every file.
    NoBucketPartition,
    /// Files within one bucket have intersecting exact key ranges; a lookup
    /// probes each of them whose range covers its key.
    OverlappingFiles,
    /// A manifest lacks key bounds, or its bounds are shorter than the
    /// file's exact first or last key.
    BoundsMissing,
    /// The key column's column index is truncated: too few page minimums
    /// are distinct.
    ColumnIndexTruncated,
    /// The mean uncompressed value page is above the advice threshold.
    LargeValuePages,
    /// The mean uncompressed value page is above
    /// `thresholds::MOST_VALUE_PAGE_BYTES`, the most a value page a lookup
    /// reads may declare; a lookup into a page over it is refused with a
    /// file fault.
    ValuePagesOverCeiling,
    /// A key is longer than `contract::KEY_BYTES_ADVICE`.
    LongKeys,
    /// A data file is larger than the advice threshold.
    LargeFile,
    /// A bucket holds more files than its bytes and the file size target
    /// explain.
    ManyFilesPerBucket,
    /// The key column has no bloom filter.
    NoBloomFilter,
    /// The key bloom filter spends more bytes per key than the advice
    /// threshold.
    BloomOversized,
    /// The key bloom filter has fewer bits per key than the minimum; the
    /// Bloom tier's miss cost rises.
    BloomUndersized,
    /// Deletes remove rows of the snapshot's data files (RFD 021); a
    /// compaction would remove them and their delete files, and the state
    /// they hold.
    DeletesPending,
    /// Data files written before the value column was added answer its
    /// default (RFD 023); a rewrite of them writes the column in.
    ColumnDefault,
}

impl FindingCode {
    /// Every code, in finding-table order, for a caller that keeps a series
    /// per code: the `findings` gauge is set for each of these on every
    /// admitted generation, so a code the generation does not raise reads
    /// zero. Complete: a successor match over every variant, which a new
    /// variant fails to compile, is compared to this list in tests.
    pub(crate) const ALL: [Self; 16] = [
        Self::DictionaryEncodedValue,
        Self::LargeKeyPages,
        Self::NoBucketPartition,
        Self::OverlappingFiles,
        Self::BoundsMissing,
        Self::ColumnIndexTruncated,
        Self::LargeValuePages,
        Self::ValuePagesOverCeiling,
        Self::LongKeys,
        Self::LargeFile,
        Self::ManyFilesPerBucket,
        Self::NoBloomFilter,
        Self::BloomOversized,
        Self::BloomUndersized,
        Self::DeletesPending,
        Self::ColumnDefault,
    ];

    /// The level a finding of this code caps a table at (RFD 020), `None`
    /// for advice, which caps nothing. A cost within one file caps it at
    /// `balanced`; more than one file probed, at `permissive`.
    pub fn cap(self) -> Option<Level> {
        match self {
            Self::DictionaryEncodedValue | Self::LargeKeyPages => Some(Level::Balanced),
            Self::NoBucketPartition | Self::OverlappingFiles => Some(Level::Permissive),
            Self::BoundsMissing
            | Self::ColumnIndexTruncated
            | Self::LargeValuePages
            | Self::ValuePagesOverCeiling
            | Self::LongKeys
            | Self::LargeFile
            | Self::ManyFilesPerBucket
            | Self::NoBloomFilter
            | Self::BloomOversized
            | Self::BloomUndersized
            | Self::DeletesPending
            | Self::ColumnDefault => None,
        }
    }
}

impl fmt::Display for FindingCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let code = match self {
            Self::DictionaryEncodedValue => "dictionary_encoded_value",
            Self::LargeKeyPages => "large_key_pages",
            Self::NoBucketPartition => "no_bucket_partition",
            Self::OverlappingFiles => "overlapping_files",
            Self::BoundsMissing => "bounds_missing",
            Self::ColumnIndexTruncated => "column_index_truncated",
            Self::LargeValuePages => "large_value_pages",
            Self::ValuePagesOverCeiling => "value_pages_over_ceiling",
            Self::LongKeys => "long_keys",
            Self::LargeFile => "large_file",
            Self::ManyFilesPerBucket => "many_files_per_bucket",
            Self::NoBloomFilter => "no_bloom_filter",
            Self::BloomOversized => "bloom_oversized",
            Self::BloomUndersized => "bloom_undersized",
            Self::DeletesPending => "deletes_pending",
            Self::ColumnDefault => "column_default",
        };
        f.write_str(code)
    }
}

/// The writer properties the doctor names (RFD 016). Display and serde give
/// the Iceberg or Parquet property string a table owner sets, as the
/// prototype's writer recipe and the Iceberg documentation spell it; code
/// never compares the strings. A property set per column (`MetricsColumn`,
/// `BloomFilterEnabled`, `BloomFilterNdvColumn`) renders its prefix, up to
/// and including the dot before the column name, and the finding's message
/// appends the key column. `RowGroupCheckRecordCount` stands for the pair of
/// properties that set the writer's page-size check interval and renders
/// both; it appears in a message and never as a finding's `property`.
// The serde names and Display are the same strings;
// `test_serialize_writer_property_matches_display` holds them together.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize)]
pub enum WriterProperty {
    /// parquet-mr's dictionary switch, passed through by Iceberg.
    #[serde(rename = "parquet.enable.dictionary")]
    EnableDictionary,
    /// parquet-mr's column index truncation length, passed through by Iceberg.
    #[serde(rename = "parquet.columnindex.truncate.length")]
    ColumnIndexTruncateLength,
    /// Iceberg's rows per page.
    #[serde(rename = "write.parquet.page-row-limit")]
    PageRowLimit,
    /// Iceberg's byte bound on a page.
    #[serde(rename = "write.parquet.page-size-bytes")]
    PageSizeBytes,
    /// Iceberg's minimum and maximum record count between page-size checks,
    /// named for row groups and setting the page-size check interval
    /// (prototype findings, finding 6).
    #[serde(
        rename = "write.parquet.row-group-check-min-record-count and write.parquet.row-group-check-max-record-count"
    )]
    RowGroupCheckRecordCount,
    /// Iceberg's data file size target.
    #[serde(rename = "write.target-file-size-bytes")]
    TargetFileSizeBytes,
    /// Iceberg's manifest metrics mode for one column; `full` keeps whole
    /// bounds (prototype findings, finding 8).
    #[serde(rename = "write.metadata.metrics.column.")]
    MetricsColumn,
    /// Iceberg's per-column bloom filter switch.
    #[serde(rename = "write.parquet.bloom-filter-enabled.column.")]
    BloomFilterEnabled,
    /// Iceberg's cap on a bloom filter's bytes, honored from Iceberg 1.11
    /// (prototype findings, finding 7).
    #[serde(rename = "write.parquet.bloom-filter-max-bytes")]
    BloomFilterMaxBytes,
    /// Iceberg's per-column distinct value count a bloom filter is sized
    /// for, honored from Iceberg 1.11 (prototype findings, finding 7).
    #[serde(rename = "write.parquet.bloom-filter-ndv.column.")]
    BloomFilterNdvColumn,
    /// Spark's adaptive skew splitting, which spreads one bucket's write
    /// over several sorted tasks (prototype findings, finding 9).
    #[serde(rename = "spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled")]
    SkewSplitting,
}

impl fmt::Display for WriterProperty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let property = match self {
            Self::EnableDictionary => "parquet.enable.dictionary",
            Self::ColumnIndexTruncateLength => "parquet.columnindex.truncate.length",
            Self::PageRowLimit => "write.parquet.page-row-limit",
            Self::PageSizeBytes => "write.parquet.page-size-bytes",
            Self::RowGroupCheckRecordCount => {
                "write.parquet.row-group-check-min-record-count and write.parquet.row-group-check-max-record-count"
            }
            Self::TargetFileSizeBytes => "write.target-file-size-bytes",
            Self::MetricsColumn => "write.metadata.metrics.column.",
            Self::BloomFilterEnabled => "write.parquet.bloom-filter-enabled.column.",
            Self::BloomFilterMaxBytes => "write.parquet.bloom-filter-max-bytes",
            Self::BloomFilterNdvColumn => "write.parquet.bloom-filter-ndv.column.",
            Self::SkewSplitting => "spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled",
        };
        f.write_str(property)
    }
}

/// One observation about a generation: its code decides what it means and
/// the level it caps, the message carries the numbers for a reader, and the
/// property names what a writer changes; `penalty` is `Some` exactly when
/// `caps` is. The fields are private and `cost` and `advice` are the only
/// constructors: `cost` takes `caps` from the code (`FindingCode::cap`)
/// with a penalty, `advice` sets neither, and each debug-asserts that its
/// code is one it builds, so a capping code's finding is a cost and every
/// other is advice. Serializes with the field names of its getters, `caps`
/// as null for advice.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct Finding {
    caps: Option<Level>,
    code: FindingCode,
    message: String,
    file: Option<String>,
    penalty: Option<Penalty>,
    property: Option<WriterProperty>,
}

impl Finding {
    /// A capping finding, served with `penalty`; `file` names the data
    /// file, `None` for a table-level one.
    pub(crate) fn cost(
        code: FindingCode,
        file: Option<&str>,
        message: String,
        penalty: Penalty,
        property: Option<WriterProperty>,
    ) -> Self {
        debug_assert!(code.cap().is_some(), "{code} caps no level");
        Self {
            caps: code.cap(),
            code,
            message,
            file: file.map(str::to_string),
            penalty: Some(penalty),
            property,
        }
    }

    /// An advice finding: no answer is wrong and no lookup does more work,
    /// so it caps no level and carries no penalty. Public so a crate that
    /// renders findings can build one in its tests; `cost` is crate-private,
    /// since only the diagnosis measures a penalty.
    pub fn advice(
        code: FindingCode,
        file: Option<&str>,
        message: String,
        property: Option<WriterProperty>,
    ) -> Self {
        debug_assert!(code.cap().is_none(), "{code} caps a level");
        Self {
            caps: None,
            code,
            message,
            file: file.map(str::to_string),
            penalty: None,
            property,
        }
    }

    /// The level the finding caps the table at; `None` for advice.
    pub fn caps(&self) -> Option<Level> {
        self.caps
    }

    /// The finding's code.
    pub fn code(&self) -> FindingCode {
        self.code
    }

    /// The numbers behind the finding and the fix, for a person; nothing
    /// decides on it.
    pub(crate) fn message(&self) -> &str {
        &self.message
    }

    /// The data file the finding is about; `None` for a table-level one.
    pub fn file(&self) -> Option<&str> {
        self.file.as_deref()
    }

    /// The measured penalty; `Some` exactly when `caps` is.
    pub fn penalty(&self) -> Option<Penalty> {
        self.penalty
    }

    /// The writer property that fixes it, when one does; the partition spec
    /// and compaction are named in the message instead.
    pub fn property(&self) -> Option<WriterProperty> {
        self.property
    }
}

/// What the doctor measured about one data file, from its footer, page
/// indexes, fences, and manifest entry.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct FileReport {
    /// The file's location as the manifest recorded it.
    pub path: String,
    /// The bucket the file holds.
    pub bucket: u32,
    /// The table's key type.
    pub key_type: KeyType,
    /// Rows in the file.
    pub rows: i64,
    /// Row groups in the file.
    pub row_groups: usize,
    /// Data pages of the key column across every row group.
    pub key_pages: usize,
    /// Data pages of the value column across every row group.
    pub value_pages: usize,
    /// Rows in the largest key page, from the key column's offset index: the
    /// keys a lookup in that page decodes.
    pub max_key_page_rows: i64,
    /// The length in bytes of the longest key.
    pub max_key_bytes: usize,
    /// Mean uncompressed bytes of a value page, the value column's
    /// uncompressed chunk bytes over its data pages: what a hit
    /// decompresses. A dictionary page's bytes are counted in: the footer
    /// gives its compressed extent and no uncompressed size, so taking them
    /// out would take a read of its header.
    pub avg_value_page_bytes: u64,
    /// Mean uncompressed bytes of one value, the value column's
    /// uncompressed chunk bytes over the file's rows. Against
    /// `avg_value_page_bytes` it says how many values a hit fetches for the
    /// one it wanted; reported, never thresholded, since a membership set
    /// with empty values fetches a page for nothing and is not wrong.
    pub avg_value_bytes: u64,
    /// Whether every value page starts on the same row as a key page in
    /// every row group.
    pub pages_aligned: bool,
    /// The columns, key or value, that a row group dictionary-encodes: a
    /// dictionary page or a dictionary encoding among the pages' encodings.
    pub dictionary_columns: Vec<String>,
    /// The key column's bloom filter bytes as the footer bounds them,
    /// summed over row groups; `None` when a row group lacks a filter.
    pub bloom_bytes: Option<u64>,
    /// Filter bits per row of the file; `None` with `bloom_bytes`.
    pub bloom_bits_per_key: Option<f64>,
    /// The key column's compressed chunk bytes: what the Full tier holds
    /// beyond the Disk tier's memory, and the payload of the chunk file the
    /// Disk tier writes, whose whole size, header included, is the
    /// prediction's `disk_bytes`.
    pub key_chunk_bytes: u64,
    /// The share of key pages whose column-index minimum is distinct; 1.0
    /// when every page is addressable by it, and 1.0 for a file whose
    /// column index cannot show truncation.
    pub distinct_page_mins_ratio: f64,
    /// Whether the manifest carries both key bounds and neither is shorter
    /// than the exact key it bounds, which a truncated bound is.
    pub bounds_present: bool,
    /// Bytes the file's index keeps resident in the tier it was built in.
    pub resident_bytes: usize,
}

/// The doctor's verdict on one generation and the numbers an owner chooses
/// a residency by. What the `doctor` and `info` subcommands serialize, so a
/// new measurement here reaches them without a change there.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct TableSummary {
    /// The table's configured name.
    pub table: String,
    /// The snapshot diagnosed.
    pub snapshot_id: SnapshotId,
    /// The level the snapshot's layout achieves in the loaded tier: the
    /// dearest cap any finding or the residency sets (RFD 020).
    pub level: Level,
    /// The level the table declares (`TableConfig::lookups`).
    pub lookups: Level,
    /// The tier the generation was loaded in.
    pub residency: Residency,
    /// Live data files in the snapshot.
    pub file_count: usize,
    /// The default spec's bucket count; `None` for a table without a bucket
    /// transform on the key.
    pub buckets: Option<u32>,
    /// Bucket numbers with at least one file.
    pub buckets_with_files: usize,
    /// Files in the fullest bucket.
    pub max_files_per_bucket: usize,
    /// Rows across every file, from the manifests.
    pub records: u64,
    /// Bytes resident across every file index, in the loaded tier.
    pub index_bytes: usize,
    /// Every tier's resident bytes, from the footers alone (RFD 017), so an
    /// owner chooses a tier from one run; each tier's memory counts the
    /// delete state at its bound from the manifests (RFD 021), as the
    /// `full` budget does.
    pub predicted: ResidentBytes,
    /// The level the layout achieves in every tier that can load the
    /// table: its findings' dearest cap and the tier's best level together,
    /// so an owner sees which tiers reach a level beside what each costs in
    /// `predicted`. A tier that cannot load it is left out, as Bloom is
    /// when `predicted.bloom` is `None`.
    pub level_by_residency: BTreeMap<Residency, Level>,
    /// What one cold lookup pays at worst in the loaded tier, from the same
    /// measurement as `level`.
    pub lookup_cost: LookupCost,
    /// What the snapshot's deletes come to (RFD 021), from the delete state
    /// the load applied: what the delete gauges show while it serves.
    pub deletes: DeleteSummary,
    /// Capping findings, the count of those whose `caps` is set.
    pub capping: usize,
    /// Advice findings, the count of those whose `caps` is `None`.
    pub advice: usize,
}

/// The whole report: the summary, the capping findings first, and one
/// report per file in generation order.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct TableReport {
    /// The verdict and the table-level numbers.
    pub summary: TableSummary,
    /// Every finding, capping before advice, then in the finding table's
    /// order, then by file in generation order.
    pub findings: Vec<Finding>,
    /// One report per file, in generation order.
    pub files: Vec<FileReport>,
}

impl TableReport {
    /// The distinct codes of the capping findings, in the report's order:
    /// what `judge` chooses a `below_level` refusal's codes from.
    pub fn capping_codes(&self) -> Vec<FindingCode> {
        capping_codes(&self.findings)
    }
}

/// Diagnoses `g` (RFD 016) for a table that declares `lookups`: measures
/// every file from what the generation holds, raises each finding of the
/// finding table whose trigger the measurements meet, the capping ones
/// through `layout_caps`, and sums every tier's predicted resident bytes
/// from the footers. No I/O and no metrics.
pub fn diagnose(g: &Generation, lookups: Level) -> TableReport {
    let caps = layout_caps(&LayoutView::of(g));
    let deletes = g.delete_summary();
    let facts: Vec<FileFacts> = g
        .files
        .iter()
        .map(|file| measure(file, &g.layout))
        .collect();
    let buckets = by_bucket(&facts);
    let pending = deletes_pending(g, &caps, deletes);
    let mut findings = caps.findings;
    findings.extend(
        facts
            .iter()
            .flat_map(|file| file_advice_findings(file, &g.layout)),
    );
    findings.extend(many_files_findings(&buckets));
    findings.extend(pending);
    findings.extend(column_default(g));
    sort_findings(&mut findings);
    let predicted = with_deletes(predicted_total(&facts), g.delete_bound());
    let capping = findings
        .iter()
        .filter(|finding| finding.caps().is_some())
        .count();
    let summary = TableSummary {
        table: g.table.clone(),
        snapshot_id: g.snapshot_id,
        level: caps.level,
        lookups,
        residency: g.residency,
        file_count: g.files.len(),
        buckets: g.layout.bucketing.map(|bucketing| bucketing.count),
        buckets_with_files: buckets.len(),
        max_files_per_bucket: buckets.values().map(Vec::len).max().unwrap_or(0),
        records: g.records,
        index_bytes: g.index_bytes,
        predicted,
        level_by_residency: level_by_residency(caps.layout, &predicted),
        lookup_cost: caps.cost,
        deletes,
        capping,
        advice: findings.len() - capping,
    };
    TableReport {
        summary,
        findings,
        files: facts.into_iter().map(|file| file.report).collect(),
    }
}

/// The level `layout` achieves in each tier that can load the table, with
/// that tier's best level: a tier `predicted` holds no figure for (bloom,
/// when a row group lacks a filter) is left out.
fn level_by_residency(layout: Level, predicted: &ResidentBytes) -> BTreeMap<Residency, Level> {
    Residency::ALL
        .into_iter()
        .filter(|residency| predicted.in_tier(*residency).is_some())
        .map(|residency| (residency, layout.max(residency.best_level())))
        .collect()
}

/// The level gate (RFD 020): `RefusalReason::BelowLevel` under `table`
/// when `level` does not meet `declared` and `strict` is on; `Ok`
/// otherwise, since a table below its level that is not strict is flagged
/// and served. The refusal names those of the capping `codes` whose cap
/// does not meet `declared`, never one the declared level accepts. The load
/// decides whether a snapshot is swapped in through it, and the doctor
/// whether a configured table is reported refused, so the two cannot
/// disagree.
pub fn judge(
    table: &str,
    level: Level,
    codes: &[FindingCode],
    declared: Level,
    strict: bool,
) -> Result<()> {
    if strict && !level.meets(declared) {
        Err(Error::refused(
            table,
            RefusalReason::BelowLevel {
                declared,
                achieved: level,
                findings: codes
                    .iter()
                    .copied()
                    .filter(|code| code.cap().is_some_and(|cap| !cap.meets(declared)))
                    .collect(),
            },
        ))
    } else {
        Ok(())
    }
}

/// The distinct codes of the capping findings, in the order `findings`
/// gives them.
fn capping_codes(findings: &[Finding]) -> Vec<FindingCode> {
    let mut codes = Vec::new();
    for finding in findings.iter().filter(|finding| finding.caps().is_some()) {
        if !codes.contains(&finding.code()) {
            codes.push(finding.code());
        }
    }
    codes
}

/// What a snapshot's level for its layout is judged from (RFD 020): the
/// table's layout and residency, each file's manifest entry and footer, and
/// per bucket the most files a lookup there probes. A built generation
/// gives every part exactly (`LayoutView::of`); none needs a key chunk.
pub(crate) struct LayoutView<'a> {
    pub(crate) layout: &'a TableLayout,
    pub(crate) residency: Residency,
    pub(crate) files: Vec<FileView<'a>>,
    /// Per bucket an overlap group touches, the most files a lookup there
    /// probes, as `Generation::overlapping_buckets` gives it.
    pub(crate) overlapping: &'a BTreeMap<u32, usize>,
}

/// One file as a level is judged from it: its manifest entry and footer,
/// with the key and value columns' leaf positions in the footer's schema;
/// no value leaf for a file written before the value column was added,
/// which answers its default and makes no value-column finding (RFD 023).
pub(crate) struct FileView<'a> {
    entry: &'a DataFileEntry,
    metadata: &'a ParquetMetaData,
    key_leaf: usize,
    value_leaf: Option<usize>,
}

impl<'a> FileView<'a> {
    /// The view of an indexed file.
    pub(crate) fn loaded(file: &'a LoadedFile) -> Self {
        Self {
            entry: &file.entry,
            metadata: file.index.metadata(),
            key_leaf: file.index.key_leaf(),
            value_leaf: file.index.value_leaf(),
        }
    }

    /// The view of a file past its footer stage, whose manifest entry is
    /// `entry`.
    pub(crate) fn inspected(entry: &'a DataFileEntry, inspected: &'a Inspected) -> Self {
        Self {
            entry,
            metadata: inspected.metadata(),
            key_leaf: inspected.key_leaf(),
            value_leaf: inspected.value_leaf(),
        }
    }
}

impl<'a> LayoutView<'a> {
    /// The view of a built generation.
    pub(crate) fn of(g: &'a Generation) -> Self {
        Self {
            layout: &g.layout,
            residency: g.residency,
            files: g.files.iter().map(|file| FileView::loaded(file)).collect(),
            overlapping: g.overlapping_buckets(),
        }
    }
}

/// A snapshot's level for its layout and the findings that set it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Caps {
    /// The level in the view's residency: the dearest of `layout` and the
    /// residency's best level.
    pub(crate) level: Level,
    /// The dearest cap the findings set, whatever the residency; `optimal`
    /// for none.
    pub(crate) layout: Level,
    /// The capping findings in the finding table's order, then by file in
    /// view order.
    pub(crate) findings: Vec<Finding>,
    /// What one cold lookup pays at worst in the view's residency.
    pub(crate) cost: LookupCost,
}

impl Caps {
    /// The distinct codes of the capping findings, in the finding table's
    /// order: what `judge` chooses a `below_level` refusal's codes from.
    pub(crate) fn codes(&self) -> Vec<FindingCode> {
        capping_codes(&self.findings)
    }
}

/// The capping findings `view` raises, the level they and its residency
/// allow, and the cost of a cold lookup there (RFD 020): the one function
/// the doctor's report, the load's level gate, and the cost gauges all
/// measure a layout by, so none can disagree. Per file, a dictionary-encoded
/// value column and large key pages; per table, files written with no
/// bucket partition and overlapping files. No `f64` enters.
pub(crate) fn layout_caps(view: &LayoutView<'_>) -> Caps {
    let mut findings = Vec::new();
    for file in &view.files {
        file_capping_findings(file, view.layout, &mut findings);
    }
    table_capping_findings(view, &mut findings);
    sort_findings(&mut findings);
    let layout = findings
        .iter()
        .filter_map(Finding::caps)
        .fold(Level::Optimal, Level::max);
    Caps {
        level: layout.max(view.residency.best_level()),
        layout,
        cost: lookup_cost(view, &findings),
        findings,
    }
}

/// The cost of one cold lookup on `view`, whose capping findings are
/// `findings`. A hit reads one file's value, so the dearest
/// `extra_gets_per_hit` of any finding is what it adds. The files probed
/// are the most any bucket's lookup probes, the figure `overlapping_files`
/// carries; `no_bucket_partition`'s figure counts every unbucketed file,
/// while a lookup probes only those whose exact key range covers its key,
/// which the overlap figure already narrows to.
fn lookup_cost(view: &LayoutView<'_>, findings: &[Finding]) -> LookupCost {
    let extra_per_hit = findings
        .iter()
        .filter_map(|finding| finding.penalty()?.extra_gets_per_hit)
        .max()
        .unwrap_or(0);
    let files_probed = view
        .overlapping
        .values()
        .copied()
        .max()
        .unwrap_or(FILES_PROBED_ALONE);
    let key_pages = match view.residency {
        Residency::Full | Residency::Disk => 0,
        Residency::Bloom => {
            let key_dictionary = view.files.iter().any(|file| {
                chunk_facts(file.metadata, file.key_leaf, file.value_leaf).key_dictionary
            });
            let dictionary = if key_dictionary {
                DICTIONARY_PAGE_GETS
            } else {
                0
            };
            (KEY_PAGE_GETS + dictionary) * approx::count(files_probed)
        }
    };
    LookupCost {
        gets_per_hit: VALUE_PAGE_GETS + extra_per_hit + key_pages,
        gets_per_miss: key_pages,
        files_probed,
    }
}

/// Everything measured about one file: its report, and what the advice
/// findings need beyond it (the manifest's size, the rows as a count, and
/// the footer's prediction).
struct FileFacts {
    report: FileReport,
    size: u64,
    rows: u64,
    predicted: ResidentBytes,
    bloom_by_row_group: Option<BloomExtremes>,
}

/// The key column's bloom filter sizing at the row groups that size it
/// worst. Filters are per row group and a miss consults only the row group
/// the fences name, so the file's mean can hide one undersized filter beside
/// well-sized ones (RFD 016 judges a filter by its row group's key count).
/// `None` when any row group lacks a bounded filter.
#[derive(Clone, Copy, Debug, PartialEq)]
struct BloomExtremes {
    /// The most filter bytes per key over the row groups, and that row group.
    most_bytes_per_key: (f64, usize),
    /// The fewest filter bits per key over the row groups, and that row group.
    fewest_bits_per_key: (f64, usize),
}

fn bloom_extremes(metadata: &ParquetMetaData, key_leaf: usize) -> Option<BloomExtremes> {
    let mut most: Option<(f64, usize)> = None;
    let mut fewest: Option<(f64, usize)> = None;
    for (row_group, group) in metadata.row_groups().iter().enumerate() {
        let column = group.column(key_leaf);
        column.bloom_filter_offset()?;
        let length = column.bloom_filter_length()?;
        let bytes = approx::measured(length);
        let rows = approx::measured(group.num_rows());
        let bytes_per_key = approx::ratio(bytes, rows);
        let bits_per_key = bits_per_key(bytes, rows);
        if most.is_none_or(|(value, _)| bytes_per_key > value) {
            most = Some((bytes_per_key, row_group));
        }
        if fewest.is_none_or(|(value, _)| bits_per_key < value) {
            fewest = Some((bits_per_key, row_group));
        }
    }
    Some(BloomExtremes {
        most_bytes_per_key: most?,
        fewest_bits_per_key: fewest?,
    })
}

/// The measured files of a generation by bucket number, in generation order
/// within a bucket. Keyed by number alone, as `overlapping_buckets` is:
/// after a spec evolution, buckets of two bucketings that share a number
/// share one entry.
type Buckets<'a> = BTreeMap<u32, Vec<&'a FileFacts>>;

fn by_bucket(facts: &[FileFacts]) -> Buckets<'_> {
    let mut buckets = Buckets::new();
    for file in facts {
        buckets.entry(file.report.bucket).or_default().push(file);
    }
    buckets
}

/// Measures `file` from its index and manifest entry.
fn measure(file: &LoadedFile, layout: &TableLayout) -> FileFacts {
    let index = &file.index;
    let metadata = index.metadata();
    let predicted = FileIndex::predicted_resident_bytes(
        metadata,
        index.key_leaf(),
        index.key_type(),
        index.handle(),
    );
    let pages = page_facts(metadata, index.key_leaf(), index.value_leaf());
    let chunks = chunk_facts(metadata, index.key_leaf(), index.value_leaf());
    let mut dictionary_columns = Vec::new();
    if chunks.key_dictionary {
        dictionary_columns.push(layout.key_column.clone());
    }
    if chunks.value_dictionary {
        dictionary_columns.push(layout.value_column.clone());
    }
    FileFacts {
        report: FileReport {
            path: file.entry.path.clone(),
            bucket: file.entry.bucket,
            key_type: layout.key_type,
            rows: index.rows(),
            row_groups: metadata.num_row_groups(),
            key_pages: pages.key_pages,
            value_pages: pages.value_pages,
            max_key_page_rows: pages.max_key_page_rows,
            max_key_bytes: index.max_key_bytes(),
            avg_value_page_bytes: chunks
                .value_uncompressed_bytes
                .checked_div(approx::count(pages.value_pages))
                .unwrap_or(0),
            avg_value_bytes: chunks
                .value_uncompressed_bytes
                .checked_div(rows_of(index))
                .unwrap_or(0),
            pages_aligned: pages.aligned,
            dictionary_columns,
            bloom_bytes: predicted.filter_bytes,
            bloom_bits_per_key: predicted.bloom_bits_per_key,
            key_chunk_bytes: predicted.key_chunk_bytes,
            distinct_page_mins_ratio: distinct_mins_ratio(metadata, index.key_leaf()),
            bounds_present: bounds_intact(&file.entry, index.key_range()),
            resident_bytes: index.resident_bytes(),
        },
        size: file.entry.size,
        rows: rows_of(index),
        predicted,
        bloom_by_row_group: bloom_extremes(metadata, index.key_leaf()),
    }
}

/// The file's rows as a count.
fn rows_of(index: &FileIndex) -> u64 {
    u64::try_from(index.rows())
        .expect("FileIndex::build refused a negative row count as FileFault::InconsistentRowCounts")
}

/// What the offset indexes say about a file's pages.
struct PageFacts {
    key_pages: usize,
    value_pages: usize,
    max_key_page_rows: i64,
    aligned: bool,
}

/// Counts the key and value pages of every row group, finds the largest key
/// page in rows (a page's rows run to the next page's first row, or the row
/// group's end), and checks that the value pages start on the key pages'
/// rows. A file with no value column has no value pages, and none out of
/// line.
fn page_facts(metadata: &ParquetMetaData, key_leaf: usize, value_leaf: Option<usize>) -> PageFacts {
    let offsets = metadata.offset_index().expect(
        "FileIndex::build refused a file without an offset index as FileFault::MissingPageIndex",
    );
    let mut facts = PageFacts {
        key_pages: 0,
        value_pages: 0,
        max_key_page_rows: 0,
        aligned: true,
    };
    for (row_group, columns) in offsets.iter().enumerate() {
        let key = columns[key_leaf].page_locations();
        if let Some(value_leaf) = value_leaf {
            let value = columns[value_leaf].page_locations();
            facts.value_pages += value.len();
            facts.aligned &= key.len() == value.len()
                && key.iter().zip(value).all(|(key_page, value_page)| {
                    key_page.first_row_index == value_page.first_row_index
                });
        }
        facts.key_pages += key.len();
        let group_rows = metadata.row_group(row_group).num_rows();
        for (at, page) in key.iter().enumerate() {
            let end = key
                .get(at + 1)
                .map_or(group_rows, |next| next.first_row_index);
            facts.max_key_page_rows = facts.max_key_page_rows.max(end - page.first_row_index);
        }
    }
    facts
}

/// What the column chunks say: which columns some row group
/// dictionary-encodes, and the value column's uncompressed bytes, none for
/// a file with no value column.
struct ChunkFacts {
    key_dictionary: bool,
    value_dictionary: bool,
    value_uncompressed_bytes: u64,
}

fn chunk_facts(
    metadata: &ParquetMetaData,
    key_leaf: usize,
    value_leaf: Option<usize>,
) -> ChunkFacts {
    let mut facts = ChunkFacts {
        key_dictionary: false,
        value_dictionary: false,
        value_uncompressed_bytes: 0,
    };
    for group in metadata.row_groups() {
        facts.key_dictionary |= dictionary_encoded(group.column(key_leaf));
        if let Some(value_leaf) = value_leaf {
            let value = group.column(value_leaf);
            facts.value_dictionary |= dictionary_encoded(value);
            facts.value_uncompressed_bytes += approx::measured(value.uncompressed_size());
        }
    }
    facts
}

/// Whether a column chunk is dictionary-encoded: it has a dictionary page,
/// or a dictionary encoding is among its pages' encodings.
fn dictionary_encoded(chunk: &ColumnChunkMetaData) -> bool {
    let encodings = chunk.encodings_mask();
    chunk.dictionary_page_offset().is_some()
        || encodings.is_set(Encoding::RLE_DICTIONARY)
        || encodings.is_set(Encoding::PLAIN_DICTIONARY)
}

/// The share of the key column's pages whose column-index minimum is
/// distinct: 1.0 when every page is addressable by it, lower when a writer
/// cut the minimums to a shared prefix. The key column is `INT64`,
/// `BYTE_ARRAY`, or `FIXED_LEN_BYTE_ARRAY` (`FileIndex::build` refused any
/// other), and a page without a minimum (a null page, which a key column
/// never has) is not counted. A file without a column index, or whose
/// index lists no page, is `DISTINCT_MINS_UNOBSERVABLE`: truncation is not
/// observable there, so it draws no finding.
fn distinct_mins_ratio(metadata: &ParquetMetaData, key_leaf: usize) -> f64 {
    let Some(index) = metadata.column_index() else {
        return DISTINCT_MINS_UNOBSERVABLE;
    };
    let (mut pages, mut distinct) = (0, 0);
    for columns in index {
        match columns.get(key_leaf) {
            Some(ColumnIndexMetaData::INT64(native)) => {
                let mins: Vec<i64> = native.min_values_iter().flatten().copied().collect();
                pages += approx::count(mins.len());
                distinct += approx::count(mins.iter().collect::<BTreeSet<_>>().len());
            }
            Some(
                ColumnIndexMetaData::BYTE_ARRAY(bytes)
                | ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(bytes),
            ) => {
                let mins: Vec<&[u8]> = bytes.min_values_iter().flatten().collect();
                pages += approx::count(mins.len());
                distinct += approx::count(mins.iter().collect::<BTreeSet<_>>().len());
            }
            Some(
                ColumnIndexMetaData::NONE
                | ColumnIndexMetaData::BOOLEAN(_)
                | ColumnIndexMetaData::INT32(_)
                | ColumnIndexMetaData::INT96(_)
                | ColumnIndexMetaData::FLOAT(_)
                | ColumnIndexMetaData::DOUBLE(_),
            )
            | None => {}
        }
    }
    if pages == 0 {
        DISTINCT_MINS_UNOBSERVABLE
    } else {
        approx::ratio(distinct, pages)
    }
}

/// Whether the manifest's key bounds are present and not truncated: both
/// recorded, and each at least as long as the exact key it bounds, since a
/// truncated string or binary bound is a cut prefix, shorter than the key
/// (prototype findings, finding 8). Long and uuid bounds are fixed width and
/// pass once present. The bound and the key share the table's key type.
fn bounds_intact(entry: &DataFileEntry, (first, last): (&Key, &Key)) -> bool {
    match (&entry.key_min, &entry.key_max) {
        (Some(min), Some(max)) => {
            min.as_bytes().len() >= first.as_bytes().len()
                && max.as_bytes().len() >= last.as_bytes().len()
        }
        (None, _) | (_, None) => false,
    }
}

/// Every advice finding of the finding table that one file's
/// measurements trigger, in the table's order.
fn file_advice_findings(facts: &FileFacts, layout: &TableLayout) -> Vec<Finding> {
    let mut findings = Vec::new();
    file_layout_advice(facts, layout, &mut findings);
    bloom_findings(facts, layout, &mut findings);
    findings
}

/// The per-file capping findings, from the file's footer and manifest
/// entry alone: a dictionary-encoded value column and large key pages.
fn file_capping_findings(file: &FileView<'_>, layout: &TableLayout, findings: &mut Vec<Finding>) {
    let path = &file.entry.path;
    let chunks = chunk_facts(file.metadata, file.key_leaf, file.value_leaf);
    if chunks.value_dictionary {
        findings.push(Finding::cost(
            FindingCode::DictionaryEncodedValue,
            Some(path),
            format!(
                "value column {value} is dictionary-encoded; a hit fetches its dictionary page besides the value page, {gets} extra GET ({property}=false)",
                value = layout.value_column,
                gets = DICTIONARY_PAGE_GETS,
                property = WriterProperty::EnableDictionary,
            ),
            Penalty {
                extra_gets_per_hit: Some(DICTIONARY_PAGE_GETS),
                ..Penalty::default()
            },
            Some(WriterProperty::EnableDictionary),
        ));
    }
    let rows = page_facts(file.metadata, file.key_leaf, file.value_leaf).max_key_page_rows;
    if rows > thresholds::KEY_PAGE_ROWS_LARGE {
        findings.push(Finding::cost(
            FindingCode::LargeKeyPages,
            Some(path),
            format!(
                "key pages hold up to {rows} rows, above {limit}; a lookup decodes that many keys ({property}={contract_rows})",
                limit = thresholds::KEY_PAGE_ROWS_LARGE,
                property = WriterProperty::PageRowLimit,
                contract_rows = contract::PAGE_ROWS,
            ),
            Penalty {
                keys_decoded_per_lookup: Some(rows),
                ..Penalty::default()
            },
            Some(WriterProperty::PageRowLimit),
        ));
    }
}

/// The per-file advice apart from the bloom filter: bounds, column index,
/// value pages (over the advice line, and over the lookup's ceiling), key
/// length, file size. The one `f64` comparison is on
/// `distinct_page_mins_ratio`, which `approx::ratio` makes finite (zero for
/// a zero denominator), so a plain comparison is total.
fn file_layout_advice(facts: &FileFacts, layout: &TableLayout, findings: &mut Vec<Finding>) {
    let report = &facts.report;
    if !report.bounds_present {
        findings.push(Finding::advice(
            FindingCode::BoundsMissing,
            Some(&report.path),
            format!(
                "the manifest's key bounds for this file are missing or shorter than its exact first or last key; every file of the bucket looks alike to a reader of the manifest ({property}{key}=full)",
                property = WriterProperty::MetricsColumn,
                key = layout.key_column,
            ),
            Some(WriterProperty::MetricsColumn),
        ));
    }
    if report.distinct_page_mins_ratio < thresholds::DISTINCT_MINS_RATIO_ADVICE {
        findings.push(Finding::advice(
            FindingCode::ColumnIndexTruncated,
            Some(&report.path),
            format!(
                "{ratio:.2} of key pages have a distinct minimum in the column index, below {limit}; readers that route by it cannot tell the pages apart ({property}, at least the longest key)",
                ratio = report.distinct_page_mins_ratio,
                limit = thresholds::DISTINCT_MINS_RATIO_ADVICE,
                property = WriterProperty::ColumnIndexTruncateLength,
            ),
            Some(WriterProperty::ColumnIndexTruncateLength),
        ));
    }
    if report.avg_value_page_bytes > thresholds::VALUE_PAGE_BYTES_ADVICE {
        findings.push(Finding::advice(
            FindingCode::LargeValuePages,
            Some(&report.path),
            format!(
                "value pages decompress to {bytes} bytes each on average, above {limit}; bound them by bytes ({size}={bound}) and shorten the page-size check interval ({check}), which the writer applies between batches",
                bytes = report.avg_value_page_bytes,
                limit = thresholds::VALUE_PAGE_BYTES_ADVICE,
                size = WriterProperty::PageSizeBytes,
                bound = contract::VALUE_PAGE_BYTES,
                check = WriterProperty::RowGroupCheckRecordCount,
            ),
            Some(WriterProperty::PageSizeBytes),
        ));
    }
    if report.avg_value_page_bytes > thresholds::MOST_VALUE_PAGE_BYTES {
        findings.push(Finding::advice(
            FindingCode::ValuePagesOverCeiling,
            Some(&report.path),
            format!(
                "value pages decompress to {bytes} bytes each on average, above the {ceiling}-byte ceiling on a value page a lookup reads; a lookup into a value page above it is refused with a file fault, so its keys are not served. The trigger is the mean of the value column's uncompressed bytes over its data pages, the dictionary page's bytes included: it can miss one oversized page among smaller ones, and a large value dictionary can raise it above the ceiling when no page is ({size}={bound})",
                bytes = report.avg_value_page_bytes,
                ceiling = thresholds::MOST_VALUE_PAGE_BYTES,
                size = WriterProperty::PageSizeBytes,
                bound = contract::VALUE_PAGE_BYTES,
            ),
            Some(WriterProperty::PageSizeBytes),
        ));
    }
    if report.max_key_bytes > contract::KEY_BYTES_ADVICE {
        findings.push(Finding::advice(
            FindingCode::LongKeys,
            Some(&report.path),
            format!(
                "keys run to {bytes} bytes, above {limit}; resident bytes per key grow with key length (shorter keys, or a fixed-width long)",
                bytes = report.max_key_bytes,
                limit = contract::KEY_BYTES_ADVICE,
            ),
            None,
        ));
    }
    if facts.size > thresholds::FILE_BYTES_ADVICE {
        findings.push(Finding::advice(
            FindingCode::LargeFile,
            Some(&report.path),
            format!(
                "data file is {size} bytes, above {limit}; first-byte latency of a range GET rises with object size ({property} between {min} and {max})",
                size = facts.size,
                limit = thresholds::FILE_BYTES_ADVICE,
                property = WriterProperty::TargetFileSizeBytes,
                min = contract::TARGET_FILE_BYTES_MIN,
                max = contract::TARGET_FILE_BYTES_MAX,
            ),
            Some(WriterProperty::TargetFileSizeBytes),
        ));
    }
}

/// The bloom filter advice: none, oversized, or undersized. Sizing is
/// judged at the row group that sizes worst, since a miss consults one row
/// group's filter; the report keeps the file's figures. Bytes per key and
/// bits per key come from `approx::ratio` and are finite (zero for a zero
/// denominator), so the plain comparisons are total.
fn bloom_findings(facts: &FileFacts, layout: &TableLayout, findings: &mut Vec<Finding>) {
    let report = &facts.report;
    let (Some(bytes), Some(extremes)) = (report.bloom_bytes, facts.bloom_by_row_group) else {
        findings.push(Finding::advice(
            FindingCode::NoBloomFilter,
            Some(&report.path),
            format!(
                "no bloom filter on {key}; the bloom residency tier cannot load this file ({property}{key}=true)",
                key = layout.key_column,
                property = WriterProperty::BloomFilterEnabled,
            ),
            Some(WriterProperty::BloomFilterEnabled),
        ));
        return;
    };
    let (bytes_per_key, largest) = extremes.most_bytes_per_key;
    if bytes_per_key > thresholds::BLOOM_BYTES_PER_KEY_ADVICE {
        findings.push(Finding::advice(
            FindingCode::BloomOversized,
            Some(&report.path),
            format!(
                "bloom filter of row group {largest} is {bytes_per_key:.1} bytes per key ({bytes} filter bytes over the file's {rows} rows), above {limit}; the bloom tier's memory follows it ({property}, Iceberg 1.11+)",
                rows = facts.rows,
                limit = thresholds::BLOOM_BYTES_PER_KEY_ADVICE,
                property = WriterProperty::BloomFilterMaxBytes,
            ),
            Some(WriterProperty::BloomFilterMaxBytes),
        ));
    }
    let (bits_per_key, smallest) = extremes.fewest_bits_per_key;
    if bits_per_key < thresholds::BLOOM_BITS_PER_KEY_MIN {
        findings.push(Finding::advice(
            FindingCode::BloomUndersized,
            Some(&report.path),
            format!(
                "bloom filter of row group {smallest} has {bits_per_key:.1} bits per key, below {limit}; the bloom tier's miss cost rises with the false-positive rate ({property}{key}, Iceberg 1.11+)",
                limit = thresholds::BLOOM_BITS_PER_KEY_MIN,
                property = WriterProperty::BloomFilterNdvColumn,
                key = layout.key_column,
            ),
            Some(WriterProperty::BloomFilterNdvColumn),
        ));
    }
}

/// The table-level capping findings: files written with no bucket
/// partition, and overlapping files per bucket.
fn table_capping_findings(view: &LayoutView<'_>, findings: &mut Vec<Finding>) {
    let key = &view.layout.key_column;
    let total = view.files.len();
    // Every file written under a spec with no bucket on the key is a
    // candidate for every lookup; a bucketed file joins them only for the
    // bucket the key hashes to. The default spec says nothing about which
    // files those are after a spec evolution.
    let unbucketed = view
        .files
        .iter()
        .filter(|file| file.entry.bucketing.is_none())
        .count();
    if unbucketed > 0 && total > 1 {
        let probed = unbucketed + usize::from(unbucketed < total);
        findings.push(Finding::cost(
            FindingCode::NoBucketPartition,
            None,
            format!(
                "{unbucketed} of {total} files were written under a spec with no bucket transform on {key}; a lookup probes every one of them whose key range covers the key, plus the bucketed file its key hashes to (partition by bucket(N, {key}) and rewrite the unbucketed files)",
            ),
            Penalty::files_probed(probed),
            None,
        ));
    }
    for (&bucket, &probed) in view.overlapping {
        findings.push(Finding::cost(
            FindingCode::OverlappingFiles,
            None,
            format!(
                "bucket {bucket} holds files with intersecting key ranges, checked free of shared keys at load; a lookup there probes up to {probed} files ({property}=false on the write, or a compaction)",
                property = WriterProperty::SkewSplitting,
            ),
            Penalty::files_probed(probed),
            Some(WriterProperty::SkewSplitting),
        ));
    }
}

/// The table-level advice: many files per bucket.
fn many_files_findings(buckets: &Buckets<'_>) -> Vec<Finding> {
    let mut findings = Vec::new();
    for (&bucket, files) in buckets {
        let bytes: u64 = files.iter().map(|file| file.size).sum();
        let explained = bytes
            .div_ceil(contract::TARGET_FILE_BYTES_MAX)
            .max(MIN_EXPLAINED_FILES);
        let held = approx::count(files.len());
        if held > thresholds::FILES_PER_BUCKET_FACTOR * explained {
            findings.push(Finding::advice(
                FindingCode::ManyFilesPerBucket,
                None,
                format!(
                    "bucket {bucket} holds {held} files where its {bytes} bytes explain {explained} at {target} bytes per file; loads and manifests grow with the file count (compact the bucket to the file size target)",
                    target = contract::TARGET_FILE_BYTES_MAX,
                ),
                None,
            ));
        }
    }
    findings
}

/// `deletes_pending` (RFD 021) when `deletes`, `g`'s delete summary,
/// removes a row: the rows and delete files a compaction would remove and,
/// where updates overlap, the level a compaction restores
/// (`compacted_level`).
/// Where the costs `deletes_pending` states were measured, named in its
/// message so an operator can read the method and the host behind them.
const MERGE_ON_READ_COSTS: &str = "measured in docs/analysis/2026-10-07-merge-on-read-costs.md";

fn deletes_pending(g: &Generation, caps: &Caps, deletes: DeleteSummary) -> Option<Finding> {
    if deletes.rows == 0 {
        return None;
    }
    let rows = deletes.rows;
    let files = deletes.files;
    let bytes = deletes.state_bytes;
    let restored = compacted_level(g, caps).map_or_else(String::new, |level| {
        format!("; files overlap, and with none overlapping the table reaches {level}")
    });
    let fetched = if g.residency == Residency::Bloom {
        " and fetches it, one GET each"
    } else {
        ""
    };
    Some(Finding::advice(
        FindingCode::DeletesPending,
        None,
        format!(
            "deletes remove {rows} rows through {files} position delete files and deletion vectors, holding {bytes} bytes of delete state{restored}; checking a row against them costs a few thousandths of a key-page decode, so a hit pays nothing for them, but a miss on a deleted key decodes the key page of every file holding a deleted copy{fetched} ({MERGE_ON_READ_COSTS}; a compaction rewrites the data files without the deleted rows and drops the position delete files and deletion vectors)",
        ),
        None,
    ))
}

/// Where `caps`, `g`'s layout caps, hold `overlapping_files`, the level
/// `layout_caps` gives `g` in its residency with no files overlapping,
/// which a compaction that rewrites the overlapping files restores, when it
/// is better than `caps`' level; `None` where no files overlap or another
/// cap holds the level where it is.
fn compacted_level(g: &Generation, caps: &Caps) -> Option<Level> {
    caps.findings
        .iter()
        .any(|finding| finding.code() == FindingCode::OverlappingFiles)
        .then(|| {
            layout_caps(&LayoutView {
                overlapping: &BTreeMap::new(),
                ..LayoutView::of(g)
            })
            .level
        })
        .filter(|&compacted| compacted < caps.level)
}

/// `column_default` (RFD 023) when files of `g` were written before its
/// value column was added and answer its default: how many, of how many,
/// and the default they answer. Such a file costs no value-page GET for a
/// hit, so a table mid-evolution costs less than its level and cost gauges
/// predict, which describe the files that hold the column.
fn column_default(g: &Generation) -> Option<Finding> {
    let defaulted = g
        .files
        .iter()
        .filter(|file| file.index.value_leaf().is_none())
        .count();
    if defaulted == 0 {
        return None;
    }
    let total = g.files.len();
    let column = &g.layout.value_column;
    let default = g.layout.value_default.as_ref().map_or_else(
        || "null".to_string(),
        |bytes| format!("its initial-default of {} bytes", bytes.len()),
    );
    Some(Finding::advice(
        FindingCode::ColumnDefault,
        None,
        format!(
            "{defaulted} of {total} data files were written before value column {column} was added and answer {default} for it, reading no value page; a rewrite of those files, such as a compaction, writes the column into them",
        ),
        None,
    ))
}

/// `predicted` with each tier's memory grown by `delete_bound`, the bound on
/// the delete state's bytes, which every tier holds in memory beside its
/// index (RFD 021): a `full` table is budgeted for it, and `disk` and
/// `bloom` report it here. Saturates as the sums do.
fn with_deletes(predicted: ResidentBytes, delete_bound: u64) -> ResidentBytes {
    ResidentBytes {
        full: predicted.full.saturating_add(delete_bound),
        disk_memory: predicted.disk_memory.saturating_add(delete_bound),
        bloom: predicted
            .bloom
            .map(|bloom| bloom.saturating_add(delete_bound)),
        ..predicted
    }
}

/// Capping before advice, then the finding table's order, then by file in
/// generation order, which is the order the findings were raised in and
/// the stable sort keeps, so two runs over one generation print the same
/// report.
fn sort_findings(findings: &mut [Finding]) {
    findings.sort_by_key(|finding| (finding.caps().is_none(), finding.code()));
}

/// Every tier's resident bytes for the table, from the footers alone: each
/// file's prediction summed per tier, saturating at `u64::MAX` as each
/// file's does. `bloom` is `None` when any file's is, since the Bloom tier
/// could not load that file, and its bits per key is the ratio of the
/// summed filter bits to the summed rows.
fn predicted_total(facts: &[FileFacts]) -> ResidentBytes {
    let sum = |field: fn(&ResidentBytes) -> u64| {
        saturating_sum(facts.iter().map(|file| field(&file.predicted)))
    };
    let sum_all = |field: fn(&ResidentBytes) -> Option<u64>| {
        saturating_sum_all(facts.iter().map(|file| field(&file.predicted)))
    };
    let filters = sum_all(|predicted| predicted.filter_bytes);
    let rows = saturating_sum(facts.iter().map(|file| file.rows));
    ResidentBytes {
        full: sum(|predicted| predicted.full),
        disk_memory: sum(|predicted| predicted.disk_memory),
        disk_bytes: sum(|predicted| predicted.disk_bytes),
        bloom: sum_all(|predicted| predicted.bloom),
        bloom_bits_per_key: filters.map(|bytes| bits_per_key(bytes, rows)),
        key_chunk_bytes: sum(|predicted| predicted.key_chunk_bytes),
        filter_bytes: filters,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::Value as Json;

    use super::thresholds::{
        BLOOM_BITS_PER_KEY_MIN, BLOOM_BYTES_PER_KEY_ADVICE, DISTINCT_MINS_RATIO_ADVICE,
        FILE_BYTES_ADVICE, FILES_PER_BUCKET_FACTOR, KEY_PAGE_ROWS_LARGE, MOST_VALUE_PAGE_BYTES,
        VALUE_PAGE_ADVICE_FACTOR, VALUE_PAGE_BYTES_ADVICE,
    };
    use super::*;
    use crate::config::TableConfig;
    use crate::error::Error;
    use crate::generation::testing::{
        self, Bloom, Bounds, Dictionary, FILE_KEYS, FIRST_SEQUENCE, FIRST_SNAPSHOT, FIRST_VERSION,
        FileSpec, KEY_STEP, SECOND_SEQUENCE, SECOND_SNAPSHOT, SECOND_VERSION, TABLE, TWO, Table,
        WriterLayout,
    };
    use crate::index::{FileFault, HeaderField};
    use crate::local::LocalDir;
    use crate::manifest::testing::{
        KEY_COLUMN, Partition, VALUE_COLUMN, bucket_spec, unpartitioned,
    };

    /// A bucket count beside the fixture's `TWO`.
    const ONE: u32 = 1;
    /// Keys in the two-bucket contract fixture, and the fewest either
    /// bucket may get, for the same reason.
    const CONTRACT_KEYS: i64 = 96;
    const MIN_BUCKET_KEYS: usize = 16;
    /// Rows of the large-key-page fixture: one page just above the
    /// threshold where large key pages cap the level.
    const LARGE_PAGE_ROWS: i64 = KEY_PAGE_ROWS_LARGE + 1;
    /// How many advice thresholds the large-value-page fixture's one page
    /// spans: two, so its mean is above the line and not at it.
    const LARGE_VALUE_PAGE_FACTOR: u64 = 2;
    /// Bytes per value of the large-value-page fixture: `FILE_KEYS` of them
    /// make one page of `LARGE_VALUE_PAGE_FACTOR` times the advice threshold.
    const LARGE_VALUE_BYTES: u64 =
        LARGE_VALUE_PAGE_FACTOR * VALUE_PAGE_BYTES_ADVICE / FILE_KEYS.unsigned_abs();
    /// Bytes per value of the over-ceiling fixture: `FILE_KEYS` of them
    /// make one page just over `MOST_VALUE_PAGE_BYTES`.
    const OVER_CEILING_VALUE_BYTES: u64 = MOST_VALUE_PAGE_BYTES / FILE_KEYS.unsigned_abs() + 1;
    /// The over-ceiling fixture's column index truncation: its one page's
    /// minimum and maximum would otherwise be two whole values of megabytes.
    const OVER_CEILING_INDEX_TRUNCATE: usize = 16;
    /// The length of the long-key fixture's keys, just above the advice
    /// length.
    const LONG_KEY_BYTES: usize = contract::KEY_BYTES_ADVICE + 1;
    /// The prefix every key of the string fixtures shares, and the length
    /// the truncated fixtures cut their column index and bounds to, so
    /// every cut minimum and bound is this prefix.
    const KEY_PREFIX: &str = "user:";
    /// Rows per page in the truncated-column-index fixture: eight pages of
    /// `FILE_KEYS`, so the share of distinct minimums can fall to an eighth.
    const TRUNCATED_PAGE_ROWS: usize = 4;
    /// The false-positive rate of the oversized fixture's filter: one in a
    /// trillion takes tens of bytes per key.
    const OVERSIZED_FPP: f64 = 1e-12;
    /// The false-positive rate of the undersized fixture's filter and the
    /// rows of its file: one in two takes a few bits per key, once the
    /// writer's smallest filter is spread over that many rows.
    const UNDERSIZED_FPP: f64 = 0.5;
    const UNDERSIZED_ROWS: i64 = 128;
    /// Files in the many-files fixture: above what one bucket of a few
    /// kilobytes explains.
    const MANY_FILES: u64 = FILES_PER_BUCKET_FACTOR + 1;

    /// The Display of every writer property: the property string a table
    /// owner sets, per-column ones as their prefix.
    const PROPERTIES: &[(WriterProperty, &str)] = &[
        (
            WriterProperty::EnableDictionary,
            "parquet.enable.dictionary",
        ),
        (
            WriterProperty::ColumnIndexTruncateLength,
            "parquet.columnindex.truncate.length",
        ),
        (WriterProperty::PageRowLimit, "write.parquet.page-row-limit"),
        (
            WriterProperty::PageSizeBytes,
            "write.parquet.page-size-bytes",
        ),
        (
            WriterProperty::RowGroupCheckRecordCount,
            "write.parquet.row-group-check-min-record-count and write.parquet.row-group-check-max-record-count",
        ),
        (
            WriterProperty::TargetFileSizeBytes,
            "write.target-file-size-bytes",
        ),
        (
            WriterProperty::MetricsColumn,
            "write.metadata.metrics.column.",
        ),
        (
            WriterProperty::BloomFilterEnabled,
            "write.parquet.bloom-filter-enabled.column.",
        ),
        (
            WriterProperty::BloomFilterMaxBytes,
            "write.parquet.bloom-filter-max-bytes",
        ),
        (
            WriterProperty::BloomFilterNdvColumn,
            "write.parquet.bloom-filter-ndv.column.",
        ),
        (
            WriterProperty::SkewSplitting,
            "spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled",
        ),
    ];

    /// The `snake_case` code of every finding, as the doctor's output and
    /// the `code` metric label carry it; the Display and serde tests pin
    /// each, and the order test holds the table's order.
    const CODES: &[(FindingCode, &str)] = &[
        (
            FindingCode::DictionaryEncodedValue,
            "dictionary_encoded_value",
        ),
        (FindingCode::LargeKeyPages, "large_key_pages"),
        (FindingCode::NoBucketPartition, "no_bucket_partition"),
        (FindingCode::OverlappingFiles, "overlapping_files"),
        (FindingCode::BoundsMissing, "bounds_missing"),
        (FindingCode::ColumnIndexTruncated, "column_index_truncated"),
        (FindingCode::LargeValuePages, "large_value_pages"),
        (
            FindingCode::ValuePagesOverCeiling,
            "value_pages_over_ceiling",
        ),
        (FindingCode::LongKeys, "long_keys"),
        (FindingCode::LargeFile, "large_file"),
        (FindingCode::ManyFilesPerBucket, "many_files_per_bucket"),
        (FindingCode::NoBloomFilter, "no_bloom_filter"),
        (FindingCode::BloomOversized, "bloom_oversized"),
        (FindingCode::BloomUndersized, "bloom_undersized"),
        (FindingCode::DeletesPending, "deletes_pending"),
        (FindingCode::ColumnDefault, "column_default"),
    ];

    /// `rows` values of `len` bytes each, none null.
    fn values_of_len(rows: usize, len: usize) -> Vec<Option<Vec<u8>>> {
        (0..rows)
            .map(|row| Some(testing::value_of_len(i64::try_from(row).unwrap(), len)))
            .collect()
    }

    fn small_values(rows: usize) -> Vec<Option<Vec<u8>>> {
        values_of_len(rows, testing::VALUE_BYTES)
    }

    /// A file of `keys` with small values and exact bounds.
    fn file<'a>(
        name: &'a str,
        keys: &'a [Key],
        partition: Partition,
        layout: WriterLayout,
    ) -> FileSpec<'a> {
        FileSpec {
            name,
            keys,
            values: small_values(keys.len()),
            partition,
            bounds: Bounds::Exact,
            layout,
        }
    }

    /// `count` string keys sharing `KEY_PREFIX`, ascending.
    fn string_keys(count: i64) -> Vec<Key> {
        (0..count)
            .map(|n| Key::Str(format!("{KEY_PREFIX}{n:06}")))
            .collect()
    }

    /// Commits every file added to `table` as `FIRST_SNAPSHOT` and loads it in
    /// `residency`.
    async fn load(
        table: &mut Table,
        residency: Residency,
        local: Option<&Arc<LocalDir>>,
    ) -> Generation {
        table
            .commit(FIRST_SNAPSHOT, None, FIRST_SEQUENCE, FIRST_VERSION)
            .await;
        let config = table.config(residency, None);
        table.load(&config, local, None).await.unwrap()
    }

    /// A one-bucket long-keyed table with one file `f` of `FILE_KEYS` keys
    /// laid out as `layout` says, loaded in the Full tier.
    async fn one_file(layout: WriterLayout) -> (Table, Generation) {
        let mut table = Table::bucketed(ONE);
        let keys = testing::long_keys(&testing::keys_from(0, FILE_KEYS));
        table
            .add_file(file("f", &keys, Partition::Bucket(0), layout))
            .await;
        let generation = load(&mut table, Residency::Full, None).await;
        (table, generation)
    }

    /// A one-bucket string-keyed table with one file `f` of `keys`, its
    /// bounds recorded as `bounds` says, loaded in the Full tier.
    async fn one_string_file(
        keys: &[Key],
        bounds: Bounds,
        layout: WriterLayout,
    ) -> (Table, Generation) {
        let mut table = Table::new(KeyType::String, bucket_spec(ONE));
        table
            .add_file(FileSpec {
                name: "f",
                keys,
                values: small_values(keys.len()),
                partition: Partition::Bucket(0),
                bounds,
                layout,
            })
            .await;
        let generation = load(&mut table, Residency::Full, None).await;
        (table, generation)
    }

    /// Two long-keyed files `a` and `b` with disjoint key runs, `a`'s
    /// below `b`'s, under `partition` in `table`.
    async fn two_disjoint_files(table: &mut Table, partition: Partition, layout: WriterLayout) {
        let first = testing::keys_from(0, FILE_KEYS);
        let second = testing::keys_from(first.last().unwrap() + KEY_STEP, FILE_KEYS);
        for (name, keys) in [("a", &first), ("b", &second)] {
            let typed = testing::long_keys(keys);
            table.add_file(file(name, &typed, partition, layout)).await;
        }
    }

    /// The contract table's files: one per bucket of `TWO`.
    const CONTRACT_FILES: usize = 2;
    /// Rows per row group in the several-row-group fixture; below the page
    /// row limit, so every row group is one key page.
    const ROW_GROUP_ROWS: usize = 4;
    /// Keys above every run `two_disjoint_files` writes, so a file added
    /// after a spec evolution intersects no unbucketed file's range.
    const EVOLVED_KEYS_START: i64 = 1_000_000;
    /// Bits per key just under the minimum, for the hand-built facts.
    const BITS_JUST_UNDER: f64 = 7.9;
    /// Bytes per key just over the advice, for the hand-built facts.
    const BYTES_JUST_OVER: f64 = 4.1;

    /// RFD 002's shape: two buckets, one contract-layout file per bucket
    /// with exact bounds.
    async fn contract_table() -> Table {
        let mut table = Table::bucketed(TWO);
        let all = testing::keys_from(0, CONTRACT_KEYS);
        for bucket in 0..TWO {
            let keys = testing::keys_in_bucket(&all, testing::bucketing(TWO), bucket);
            assert!(
                keys.len() >= MIN_BUCKET_KEYS,
                "bucket {bucket} holds {} keys",
                keys.len()
            );
            table
                .add_long_file(&format!("b{bucket}"), &keys, bucket)
                .await;
        }
        table
    }

    /// A finding as the tests assert it: everything but the message.
    type Shape = (
        FindingCode,
        Option<Level>,
        Option<Penalty>,
        Option<WriterProperty>,
        Option<String>,
    );

    fn shapes_of(findings: &[Finding]) -> Vec<Shape> {
        findings
            .iter()
            .map(|finding| {
                (
                    finding.code(),
                    finding.caps(),
                    finding.penalty(),
                    finding.property(),
                    finding.file().map(str::to_string),
                )
            })
            .collect()
    }

    /// Asserts `report` raised exactly `expected`, in order, that a finding
    /// carries a penalty exactly when it caps the level, and that the
    /// summary's counts and level follow them in the loaded tier.
    fn assert_findings(report: &TableReport, expected: &[Shape]) {
        assert_eq!(shapes_of(&report.findings), expected);
        let capping = expected
            .iter()
            .filter(|(_, caps, ..)| caps.is_some())
            .count();
        assert_eq!(report.summary.capping, capping);
        assert_eq!(report.summary.advice, expected.len() - capping);
        let dearest = expected
            .iter()
            .filter_map(|(_, caps, ..)| *caps)
            .fold(report.summary.residency.best_level(), Level::max);
        assert_eq!(report.summary.level, dearest);
        for finding in &report.findings {
            assert_eq!(
                finding.penalty().is_some(),
                finding.caps().is_some(),
                "{:?}",
                finding.code
            );
        }
    }

    #[test]
    fn test_cap_of_every_code_is_the_rfd_table() {
        let capped: Vec<(FindingCode, Level)> = FindingCode::ALL
            .into_iter()
            .filter_map(|code| code.cap().map(|cap| (code, cap)))
            .collect();
        assert_eq!(
            capped,
            [
                (FindingCode::DictionaryEncodedValue, Level::Balanced),
                (FindingCode::LargeKeyPages, Level::Balanced),
                (FindingCode::NoBucketPartition, Level::Permissive),
                (FindingCode::OverlappingFiles, Level::Permissive),
            ]
        );
    }

    #[test]
    fn test_thresholds_derive_from_contract() {
        assert_eq!(KEY_PAGE_ROWS_LARGE, 2 * contract::PAGE_ROWS);
        assert_eq!(
            VALUE_PAGE_BYTES_ADVICE,
            VALUE_PAGE_ADVICE_FACTOR * contract::VALUE_PAGE_BYTES
        );
        assert_eq!(FILE_BYTES_ADVICE, 2 * contract::TARGET_FILE_BYTES_MAX);
        const { assert!(FILES_PER_BUCKET_FACTOR > MIN_EXPLAINED_FILES) };
        // A filter cannot be oversized in bytes and undersized in bits at once.
        assert!(
            BLOOM_BYTES_PER_KEY_ADVICE * approx::gauge(u64::from(u8::BITS))
                > BLOOM_BITS_PER_KEY_MIN
        );
        const { assert!(DISTINCT_MINS_RATIO_ADVICE < DISTINCT_MINS_UNOBSERVABLE) };
    }

    #[test]
    fn test_fmt_finding_code_is_the_code_column() {
        for &(code, name) in CODES {
            assert_eq!(code.to_string(), name);
        }
    }

    #[test]
    fn test_serialize_finding_code_matches_display() {
        for &(code, name) in CODES {
            let json = serde_json::to_string(&code).unwrap();
            assert_eq!(json, format!("\"{name}\""));
            let back: FindingCode = serde_json::from_str(&json).unwrap();
            assert_eq!(back, code);
        }
    }

    #[test]
    fn test_finding_code_order_is_the_finding_table() {
        assert!(CODES.windows(2).all(|pair| pair[0].0 < pair[1].0));
    }

    /// The code after `code` in the finding table, `None` after the last.
    /// The match names every variant, so a variant added to the enum fails
    /// to compile here until it is placed in the chain, and the chain is
    /// what `ALL` is checked against.
    fn after(code: FindingCode) -> Option<FindingCode> {
        match code {
            FindingCode::DictionaryEncodedValue => Some(FindingCode::LargeKeyPages),
            FindingCode::LargeKeyPages => Some(FindingCode::NoBucketPartition),
            FindingCode::NoBucketPartition => Some(FindingCode::OverlappingFiles),
            FindingCode::OverlappingFiles => Some(FindingCode::BoundsMissing),
            FindingCode::BoundsMissing => Some(FindingCode::ColumnIndexTruncated),
            FindingCode::ColumnIndexTruncated => Some(FindingCode::LargeValuePages),
            FindingCode::LargeValuePages => Some(FindingCode::ValuePagesOverCeiling),
            FindingCode::ValuePagesOverCeiling => Some(FindingCode::LongKeys),
            FindingCode::LongKeys => Some(FindingCode::LargeFile),
            FindingCode::LargeFile => Some(FindingCode::ManyFilesPerBucket),
            FindingCode::ManyFilesPerBucket => Some(FindingCode::NoBloomFilter),
            FindingCode::NoBloomFilter => Some(FindingCode::BloomOversized),
            FindingCode::BloomOversized => Some(FindingCode::BloomUndersized),
            FindingCode::BloomUndersized => Some(FindingCode::DeletesPending),
            FindingCode::DeletesPending => Some(FindingCode::ColumnDefault),
            FindingCode::ColumnDefault => None,
        }
    }

    #[test]
    fn test_all_lists_every_code_once_in_table_order() {
        let mut chain = vec![FindingCode::ALL[0]];
        while let Some(next) = after(*chain.last().unwrap()) {
            chain.push(next);
        }
        assert_eq!(chain, FindingCode::ALL);
        assert!(FindingCode::ALL.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(
            FindingCode::ALL.to_vec(),
            CODES.iter().map(|(code, _)| *code).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_fmt_writer_property_is_the_property_string() {
        for &(property, name) in PROPERTIES {
            assert_eq!(property.to_string(), name);
        }
    }

    #[test]
    fn test_serialize_writer_property_matches_display() {
        for &(property, _) in PROPERTIES {
            assert_eq!(
                serde_json::to_value(property).unwrap(),
                Json::String(property.to_string())
            );
        }
    }

    #[test]
    fn test_bounds_intact_requires_both_bounds_at_least_as_long_as_the_exact_keys() {
        let keys = string_keys(FILE_KEYS);
        let (first, last) = (&keys[0], keys.last().unwrap());
        let entry = |key_min: Option<Key>, key_max: Option<Key>| DataFileEntry {
            path: String::new(),
            bucket: 0,
            bucketing: None,
            records: 0,
            sequence: crate::snapshot::SequenceNumber(0),
            size: 0,
            key_min,
            key_max,
            partition: crate::manifest::Partition::unpartitioned(),
        };
        assert!(bounds_intact(
            &entry(Some(first.clone()), Some(last.clone())),
            (first, last)
        ));
        assert!(!bounds_intact(
            &entry(None, Some(last.clone())),
            (first, last)
        ));
        assert!(!bounds_intact(
            &entry(Some(first.clone()), None),
            (first, last)
        ));
        let cut = Key::Str(KEY_PREFIX.to_string());
        assert!(!bounds_intact(
            &entry(Some(cut.clone()), Some(last.clone())),
            (first, last)
        ));
        assert!(!bounds_intact(
            &entry(Some(first.clone()), Some(cut)),
            (first, last)
        ));
        let (low, high) = (Key::Long(0), Key::Long(FILE_KEYS));
        assert!(bounds_intact(
            &entry(Some(low.clone()), Some(high.clone())),
            (&low, &high)
        ));
    }

    #[tokio::test]
    async fn test_diagnose_contract_fixture_is_optimal_with_no_findings() {
        let mut table = contract_table().await;
        let generation = load(&mut table, Residency::Full, None).await;
        let report = diagnose(&generation, Level::Optimal);
        assert_findings(&report, &[]);
        let summary = &report.summary;
        assert_eq!(summary.level, Level::Optimal);
        assert_eq!(summary.table, TABLE);
        assert_eq!(summary.snapshot_id, SnapshotId(FIRST_SNAPSHOT));
        assert_eq!(summary.residency, Residency::Full);
        assert_eq!(summary.file_count, CONTRACT_FILES);
        assert_eq!(summary.buckets, Some(TWO));
        assert_eq!(summary.buckets_with_files, CONTRACT_FILES);
        assert_eq!(summary.max_files_per_bucket, 1);
        assert_eq!(summary.records, u64::try_from(CONTRACT_KEYS).unwrap());
        assert_eq!(summary.index_bytes, generation.index_bytes);
        assert_eq!(report.files.len(), CONTRACT_FILES);
        for (file, loaded) in report.files.iter().zip(&generation.files) {
            assert_eq!(file.path, loaded.entry.path);
            assert_eq!(file.bucket, loaded.entry.bucket);
            assert_eq!(file.key_type, KeyType::Long);
            assert_eq!(file.rows, loaded.index.rows());
            assert_eq!(file.row_groups, 1);
            assert_eq!(file.key_pages, 1);
            assert_eq!(file.value_pages, 1);
            assert_eq!(file.max_key_page_rows, loaded.index.rows());
            assert!(file.max_key_page_rows <= contract::PAGE_ROWS);
            assert_eq!(file.max_key_bytes, size_of::<i64>());
            assert!(file.avg_value_page_bytes <= VALUE_PAGE_BYTES_ADVICE);
            assert!(file.pages_aligned);
            assert!(file.dictionary_columns.is_empty());
            assert!(file.bloom_bytes.is_some());
            assert!(file.bloom_bits_per_key.unwrap() >= BLOOM_BITS_PER_KEY_MIN);
            assert!(file.key_chunk_bytes > 0);
            assert_eq!(
                file.distinct_page_mins_ratio.to_bits(),
                DISTINCT_MINS_UNOBSERVABLE.to_bits()
            );
            assert!(file.bounds_present);
            assert_eq!(file.resident_bytes, loaded.index.resident_bytes());
        }
    }

    #[tokio::test]
    async fn test_diagnose_dictionary_encoded_value_caps_balanced_by_one_extra_get() {
        let layout = WriterLayout {
            dictionary: Dictionary::Value,
            ..WriterLayout::contract()
        };
        let (table, generation) = one_file(layout).await;
        let report = diagnose(&generation, Level::Optimal);
        assert_findings(
            &report,
            &[(
                FindingCode::DictionaryEncodedValue,
                Some(Level::Balanced),
                Some(Penalty {
                    extra_gets_per_hit: Some(DICTIONARY_PAGE_GETS),
                    ..Penalty::default()
                }),
                Some(WriterProperty::EnableDictionary),
                Some(table.path_of("f")),
            )],
        );
        assert_eq!(report.files[0].dictionary_columns, [VALUE_COLUMN]);
    }

    #[tokio::test]
    async fn test_diagnose_dictionary_encoded_key_is_reported_and_not_a_finding() {
        let layout = WriterLayout {
            dictionary: Dictionary::Key,
            ..WriterLayout::contract()
        };
        let (_table, generation) = one_file(layout).await;
        let report = diagnose(&generation, Level::Optimal);
        assert_findings(&report, &[]);
        assert_eq!(report.files[0].dictionary_columns, [KEY_COLUMN]);
    }

    #[tokio::test]
    async fn test_diagnose_large_key_pages_penalty_is_the_rows_decoded() {
        let mut table = Table::bucketed(ONE);
        let keys = testing::long_keys(&testing::keys_from(0, LARGE_PAGE_ROWS));
        let layout = WriterLayout {
            page_rows: usize::try_from(LARGE_PAGE_ROWS).unwrap(),
            ..WriterLayout::contract()
        };
        table
            .add_file(file("f", &keys, Partition::Bucket(0), layout))
            .await;
        let report = diagnose(
            &load(&mut table, Residency::Full, None).await,
            Level::Optimal,
        );
        assert_findings(
            &report,
            &[(
                FindingCode::LargeKeyPages,
                Some(Level::Balanced),
                Some(Penalty {
                    keys_decoded_per_lookup: Some(LARGE_PAGE_ROWS),
                    ..Penalty::default()
                }),
                Some(WriterProperty::PageRowLimit),
                Some(table.path_of("f")),
            )],
        );
        assert_eq!(report.files[0].key_pages, 1);
        assert_eq!(report.files[0].max_key_page_rows, LARGE_PAGE_ROWS);
    }

    #[tokio::test]
    async fn test_diagnose_no_bucket_partition_penalty_is_every_file_probed() {
        let mut table = Table::new(KeyType::Long, unpartitioned());
        two_disjoint_files(&mut table, Partition::None, WriterLayout::contract()).await;
        let report = diagnose(
            &load(&mut table, Residency::Full, None).await,
            Level::Optimal,
        );
        assert_findings(
            &report,
            &[(
                FindingCode::NoBucketPartition,
                Some(Level::Permissive),
                Some(Penalty::files_probed(2)),
                None,
                None,
            )],
        );
        assert_eq!(report.summary.buckets, None);
        assert_eq!(report.summary.buckets_with_files, 1);
        assert_eq!(report.summary.max_files_per_bucket, 2);
    }

    #[tokio::test]
    async fn test_diagnose_no_bucket_partition_counts_the_files_every_lookup_probes() {
        let mut table = Table::new(KeyType::Long, unpartitioned());
        two_disjoint_files(&mut table, Partition::None, WriterLayout::contract()).await;
        table.evolve(TWO);
        // Bucket 1, so the bucketed file does not share bucket zero's number
        // with the unbucketed files in `many_files_per_bucket`'s count.
        let in_bucket_one: Vec<i64> = testing::keys_from(EVOLVED_KEYS_START, FILE_KEYS)
            .into_iter()
            .filter(|key| crate::bucket::bucket(&Key::Long(*key), TWO) == 1)
            .collect();
        assert!(!in_bucket_one.is_empty());
        table.add_long_file("g1", &in_bucket_one, 1).await;
        let report = diagnose(
            &load(&mut table, Residency::Full, None).await,
            Level::Optimal,
        );
        assert_findings(
            &report,
            &[(
                FindingCode::NoBucketPartition,
                Some(Level::Permissive),
                Some(Penalty::files_probed(3)),
                None,
                None,
            )],
        );
        assert_eq!(report.summary.buckets, Some(TWO), "the default spec's");
        assert_eq!(report.summary.file_count, 3);
    }

    #[tokio::test]
    async fn test_measure_several_row_groups_sizes_bloom_filters_per_row_group() {
        let layout = WriterLayout {
            row_group_rows: Some(ROW_GROUP_ROWS),
            ..WriterLayout::contract()
        };
        let (_table, generation) = one_file(layout).await;
        let facts = measure(&generation.files[0], &generation.layout);
        let groups = usize::try_from(FILE_KEYS).unwrap().div_ceil(ROW_GROUP_ROWS);
        assert!(groups > 1, "the fixture spans several row groups");
        assert_eq!(facts.report.row_groups, groups);
        assert_eq!(facts.report.key_pages, groups, "one page per row group");
        let extremes = facts.bloom_by_row_group.unwrap();
        let file_bits = facts.report.bloom_bits_per_key.unwrap();
        let file_bytes = approx::ratio(facts.report.bloom_bytes.unwrap(), facts.rows);
        assert!(extremes.fewest_bits_per_key.0 <= file_bits);
        assert!(extremes.most_bytes_per_key.0 >= file_bytes);
        assert!(extremes.fewest_bits_per_key.1 < groups);
        assert!(extremes.most_bytes_per_key.1 < groups);
    }

    #[tokio::test]
    async fn test_predicted_total_saturates_as_each_file_does() {
        let (_table, generation) = one_file(WriterLayout::contract()).await;
        let saturated = || {
            let mut facts = measure(&generation.files[0], &generation.layout);
            facts.rows = u64::MAX;
            facts.predicted = ResidentBytes {
                full: u64::MAX,
                disk_memory: u64::MAX,
                disk_bytes: u64::MAX,
                bloom: Some(u64::MAX),
                bloom_bits_per_key: None,
                key_chunk_bytes: u64::MAX,
                filter_bytes: Some(u64::MAX),
            };
            facts
        };
        let total = predicted_total(&[saturated(), saturated()]);
        assert_eq!(
            total,
            ResidentBytes {
                full: u64::MAX,
                disk_memory: u64::MAX,
                disk_bytes: u64::MAX,
                bloom: Some(u64::MAX),
                bloom_bits_per_key: Some(bits_per_key(u64::MAX, u64::MAX)),
                key_chunk_bytes: u64::MAX,
                filter_bytes: Some(u64::MAX),
            }
        );
    }

    #[tokio::test]
    async fn test_bloom_findings_judge_the_worst_row_group_not_the_file() {
        let (_table, generation) = one_file(WriterLayout::contract()).await;
        let mut facts = measure(&generation.files[0], &generation.layout);
        let healthy = facts.bloom_by_row_group.unwrap();
        assert!(bloom_shapes(&facts, &generation.layout).is_empty());
        facts.bloom_by_row_group = Some(BloomExtremes {
            fewest_bits_per_key: (BITS_JUST_UNDER, 1),
            ..healthy
        });
        assert_eq!(
            bloom_shapes(&facts, &generation.layout),
            [(
                FindingCode::BloomUndersized,
                None,
                None,
                Some(WriterProperty::BloomFilterNdvColumn),
                Some(facts.report.path.clone()),
            )],
            "the file's own bits per key are healthy; the row group decides"
        );
        facts.bloom_by_row_group = Some(BloomExtremes {
            most_bytes_per_key: (BYTES_JUST_OVER, 0),
            ..healthy
        });
        assert_eq!(
            bloom_shapes(&facts, &generation.layout),
            [(
                FindingCode::BloomOversized,
                None,
                None,
                Some(WriterProperty::BloomFilterMaxBytes),
                Some(facts.report.path.clone()),
            )]
        );
    }

    /// The shapes `bloom_findings` raises for `facts`.
    fn bloom_shapes(facts: &FileFacts, layout: &TableLayout) -> Vec<Shape> {
        let mut findings = Vec::new();
        bloom_findings(facts, layout, &mut findings);
        shapes_of(&findings)
    }

    #[tokio::test]
    async fn test_diagnose_overlapping_files_penalty_is_the_files_a_lookup_probes() {
        let mut table = Table::bucketed(ONE);
        let first = testing::keys_from(0, FILE_KEYS);
        let interleaved: Vec<i64> = first.iter().map(|key| key + 1).collect();
        table.add_long_file("a", &first, 0).await;
        table.add_long_file("b", &interleaved, 0).await;
        let generation = load(&mut table, Residency::Full, None).await;
        assert_eq!(generation.overlapping_buckets().len(), 1);
        let report = diagnose(&generation, Level::Optimal);
        assert_findings(
            &report,
            &[(
                FindingCode::OverlappingFiles,
                Some(Level::Permissive),
                Some(Penalty::files_probed(2)),
                Some(WriterProperty::SkewSplitting),
                None,
            )],
        );
    }

    #[tokio::test]
    async fn test_diagnose_bounds_missing_is_advice_naming_the_metrics_property() {
        let mut table = Table::bucketed(ONE);
        let keys = testing::long_keys(&testing::keys_from(0, FILE_KEYS));
        table
            .add_file(FileSpec {
                bounds: Bounds::Missing,
                ..file("f", &keys, Partition::Bucket(0), WriterLayout::contract())
            })
            .await;
        let generation = load(&mut table, Residency::Full, None).await;
        assert_eq!(generation.files[0].entry.key_min, None);
        let report = diagnose(&generation, Level::Optimal);
        assert_findings(
            &report,
            &[(
                FindingCode::BoundsMissing,
                None,
                None,
                Some(WriterProperty::MetricsColumn),
                Some(table.path_of("f")),
            )],
        );
        assert!(!report.files[0].bounds_present);
    }

    #[tokio::test]
    async fn test_diagnose_truncated_bounds_are_bounds_missing() {
        let keys = string_keys(FILE_KEYS);
        let (table, generation) = one_string_file(
            &keys,
            Bounds::Truncated(KEY_PREFIX.len()),
            WriterLayout::contract(),
        )
        .await;
        let entry = &generation.files[0].entry;
        assert!(entry.key_min.is_some() && entry.key_max.is_some());
        assert_eq!(generation.layout.key_type, KeyType::String);
        let report = diagnose(&generation, Level::Optimal);
        assert_findings(
            &report,
            &[(
                FindingCode::BoundsMissing,
                None,
                None,
                Some(WriterProperty::MetricsColumn),
                Some(table.path_of("f")),
            )],
        );
        assert!(!report.files[0].bounds_present);
    }

    #[tokio::test]
    async fn test_diagnose_column_index_truncated_is_advice() {
        let keys = string_keys(FILE_KEYS);
        let pages = usize::try_from(FILE_KEYS).unwrap() / TRUNCATED_PAGE_ROWS;
        let small_pages = WriterLayout {
            page_rows: TRUNCATED_PAGE_ROWS,
            ..WriterLayout::contract()
        };
        let (_table, whole) = one_string_file(&keys, Bounds::Exact, small_pages).await;
        let report = diagnose(&whole, Level::Optimal);
        assert_findings(&report, &[]);
        assert_eq!(report.files[0].key_pages, pages);
        assert_eq!(
            report.files[0].distinct_page_mins_ratio.to_bits(),
            DISTINCT_MINS_UNOBSERVABLE.to_bits()
        );

        let truncated = WriterLayout {
            column_index_truncate: Some(KEY_PREFIX.len()),
            ..small_pages
        };
        let (table, generation) = one_string_file(&keys, Bounds::Exact, truncated).await;
        let report = diagnose(&generation, Level::Optimal);
        assert_findings(
            &report,
            &[(
                FindingCode::ColumnIndexTruncated,
                None,
                None,
                Some(WriterProperty::ColumnIndexTruncateLength),
                Some(table.path_of("f")),
            )],
        );
        assert_eq!(report.files[0].key_pages, pages);
        assert_eq!(
            report.files[0].distinct_page_mins_ratio.to_bits(),
            approx::ratio(1, approx::count(pages)).to_bits(),
            "every cut minimum is the shared prefix"
        );
    }

    #[tokio::test]
    async fn test_distinct_mins_ratio_without_a_column_index_is_unobservable() {
        let (_table, generation) = one_file(WriterLayout::contract()).await;
        let index = &generation.files[0].index;
        let stripped = ParquetMetaData::clone(index.metadata())
            .into_builder()
            .set_column_index(None)
            .build();
        assert!(stripped.column_index().is_none());
        assert_eq!(
            distinct_mins_ratio(&stripped, index.key_leaf()).to_bits(),
            DISTINCT_MINS_UNOBSERVABLE.to_bits()
        );
    }

    #[tokio::test]
    async fn test_diagnose_large_value_pages_is_advice() {
        let mut table = Table::bucketed(ONE);
        let keys = testing::long_keys(&testing::keys_from(0, FILE_KEYS));
        table
            .add_file(FileSpec {
                values: values_of_len(keys.len(), approx::length(LARGE_VALUE_BYTES)),
                ..file("f", &keys, Partition::Bucket(0), WriterLayout::contract())
            })
            .await;
        let report = diagnose(
            &load(&mut table, Residency::Full, None).await,
            Level::Optimal,
        );
        assert_findings(
            &report,
            &[(
                FindingCode::LargeValuePages,
                None,
                None,
                Some(WriterProperty::PageSizeBytes),
                Some(table.path_of("f")),
            )],
        );
        assert_eq!(report.files[0].value_pages, 1);
        assert!(report.files[0].avg_value_page_bytes > VALUE_PAGE_BYTES_ADVICE);
    }

    /// A value page just over the lookup ceiling draws `value_pages_over_ceiling`
    /// beside `large_value_pages`, and a lookup into it is refused with the
    /// file fault the message names.
    #[tokio::test]
    async fn test_diagnose_value_pages_over_the_lookup_ceiling_is_advice() {
        let mut table = Table::bucketed(ONE);
        let keys = testing::long_keys(&testing::keys_from(0, FILE_KEYS));
        let layout = WriterLayout {
            column_index_truncate: Some(OVER_CEILING_INDEX_TRUNCATE),
            ..WriterLayout::contract()
        };
        table
            .add_file(FileSpec {
                values: values_of_len(keys.len(), approx::length(OVER_CEILING_VALUE_BYTES)),
                ..file("f", &keys, Partition::Bucket(0), layout)
            })
            .await;
        let generation = load(&mut table, Residency::Full, None).await;
        let report = diagnose(&generation, Level::Optimal);
        let path = Some(table.path_of("f"));
        assert_findings(
            &report,
            &[
                (
                    FindingCode::LargeValuePages,
                    None,
                    None,
                    Some(WriterProperty::PageSizeBytes),
                    path.clone(),
                ),
                (
                    FindingCode::ValuePagesOverCeiling,
                    None,
                    None,
                    Some(WriterProperty::PageSizeBytes),
                    path,
                ),
            ],
        );
        assert_eq!(report.files[0].value_pages, 1);
        assert!(report.files[0].avg_value_page_bytes > MOST_VALUE_PAGE_BYTES);
        let error = generation
            .lookup(&keys[0], &testing::cache().await)
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                Error::File {
                    fault: FileFault::PageHeaderOverBound {
                        field: HeaderField::UncompressedPageSize,
                        allowed: MOST_VALUE_PAGE_BYTES,
                        ..
                    },
                    ..
                }
            ),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn test_diagnose_long_keys_is_advice_without_a_property() {
        let keys: Vec<Key> = (0..FILE_KEYS)
            .map(|n| Key::Str(format!("{n:0>LONG_KEY_BYTES$}")))
            .collect();
        let (table, generation) =
            one_string_file(&keys, Bounds::Exact, WriterLayout::contract()).await;
        let report = diagnose(&generation, Level::Optimal);
        assert_findings(
            &report,
            &[(
                FindingCode::LongKeys,
                None,
                None,
                None,
                Some(table.path_of("f")),
            )],
        );
        assert_eq!(report.files[0].max_key_bytes, LONG_KEY_BYTES);
    }

    #[tokio::test]
    async fn test_file_advice_findings_large_file_above_the_advice_size() {
        let (_table, generation) = one_file(WriterLayout::contract()).await;
        let mut facts = measure(&generation.files[0], &generation.layout);
        assert!(file_advice_findings(&facts, &generation.layout).is_empty());
        facts.size = FILE_BYTES_ADVICE;
        assert!(
            file_advice_findings(&facts, &generation.layout).is_empty(),
            "the threshold itself passes"
        );
        facts.size = FILE_BYTES_ADVICE + 1;
        assert_eq!(
            shapes_of(&file_advice_findings(&facts, &generation.layout)),
            [(
                FindingCode::LargeFile,
                None,
                None,
                Some(WriterProperty::TargetFileSizeBytes),
                Some(facts.report.path.clone()),
            )]
        );
    }

    #[tokio::test]
    async fn test_diagnose_many_files_per_bucket_is_advice() {
        let mut table = Table::bucketed(ONE);
        let mut from = 0;
        for at in 0..MANY_FILES {
            let keys = testing::keys_from(from, FILE_KEYS);
            from = keys.last().unwrap() + KEY_STEP;
            table.add_long_file(&format!("f{at}"), &keys, 0).await;
        }
        let generation = load(&mut table, Residency::Full, None).await;
        assert_eq!(generation.overlapping_buckets().len(), 0);
        let report = diagnose(&generation, Level::Optimal);
        assert_findings(
            &report,
            &[(FindingCode::ManyFilesPerBucket, None, None, None, None)],
        );
        assert_eq!(report.summary.buckets_with_files, 1);
        assert_eq!(
            report.summary.max_files_per_bucket,
            approx::length(MANY_FILES)
        );
    }

    #[tokio::test]
    async fn test_diagnose_no_bloom_filter_is_advice() {
        let layout = WriterLayout {
            bloom: Bloom::Off,
            ..WriterLayout::contract()
        };
        let (table, generation) = one_file(layout).await;
        let report = diagnose(&generation, Level::Optimal);
        assert_findings(
            &report,
            &[(
                FindingCode::NoBloomFilter,
                None,
                None,
                Some(WriterProperty::BloomFilterEnabled),
                Some(table.path_of("f")),
            )],
        );
        assert_eq!(report.files[0].bloom_bytes, None);
        assert_eq!(report.files[0].bloom_bits_per_key, None);
        assert_eq!(report.summary.predicted.bloom, None);
        assert_eq!(report.summary.predicted.bloom_bits_per_key, None);
    }

    #[tokio::test]
    async fn test_diagnose_bloom_oversized_is_advice() {
        let layout = WriterLayout {
            bloom: Bloom::Fpp(OVERSIZED_FPP),
            ..WriterLayout::contract()
        };
        let (table, generation) = one_file(layout).await;
        let report = diagnose(&generation, Level::Optimal);
        assert_findings(
            &report,
            &[(
                FindingCode::BloomOversized,
                None,
                None,
                Some(WriterProperty::BloomFilterMaxBytes),
                Some(table.path_of("f")),
            )],
        );
        let file = &report.files[0];
        let rows = u64::try_from(file.rows).unwrap();
        assert!(approx::ratio(file.bloom_bytes.unwrap(), rows) > BLOOM_BYTES_PER_KEY_ADVICE);
    }

    #[tokio::test]
    async fn test_diagnose_bloom_undersized_is_advice() {
        let mut table = Table::bucketed(ONE);
        let keys = testing::long_keys(&testing::keys_from(0, UNDERSIZED_ROWS));
        let layout = WriterLayout {
            bloom: Bloom::Fpp(UNDERSIZED_FPP),
            ..WriterLayout::contract()
        };
        table
            .add_file(file("f", &keys, Partition::Bucket(0), layout))
            .await;
        let report = diagnose(
            &load(&mut table, Residency::Full, None).await,
            Level::Optimal,
        );
        assert_findings(
            &report,
            &[(
                FindingCode::BloomUndersized,
                None,
                None,
                Some(WriterProperty::BloomFilterNdvColumn),
                Some(table.path_of("f")),
            )],
        );
        assert!(report.files[0].bloom_bits_per_key.unwrap() < BLOOM_BITS_PER_KEY_MIN);
    }

    #[tokio::test]
    async fn test_diagnose_orders_capping_before_advice_then_code_then_file() {
        let mut table = Table::new(KeyType::Long, unpartitioned());
        let layout = WriterLayout {
            dictionary: Dictionary::Value,
            bloom: Bloom::Off,
            ..WriterLayout::contract()
        };
        two_disjoint_files(&mut table, Partition::None, layout).await;
        let generation = load(&mut table, Residency::Full, None).await;
        let (a, b) = (table.path_of("a"), table.path_of("b"));
        assert_eq!(
            generation
                .files
                .iter()
                .map(|file| file.entry.path.clone())
                .collect::<Vec<_>>(),
            [a.clone(), b.clone()]
        );
        let report = diagnose(&generation, Level::Optimal);
        let dictionary = Some(Penalty {
            extra_gets_per_hit: Some(DICTIONARY_PAGE_GETS),
            ..Penalty::default()
        });
        assert_findings(
            &report,
            &[
                (
                    FindingCode::DictionaryEncodedValue,
                    Some(Level::Balanced),
                    dictionary,
                    Some(WriterProperty::EnableDictionary),
                    Some(a.clone()),
                ),
                (
                    FindingCode::DictionaryEncodedValue,
                    Some(Level::Balanced),
                    dictionary,
                    Some(WriterProperty::EnableDictionary),
                    Some(b.clone()),
                ),
                (
                    FindingCode::NoBucketPartition,
                    Some(Level::Permissive),
                    Some(Penalty::files_probed(2)),
                    None,
                    None,
                ),
                (
                    FindingCode::NoBloomFilter,
                    None,
                    None,
                    Some(WriterProperty::BloomFilterEnabled),
                    Some(a),
                ),
                (
                    FindingCode::NoBloomFilter,
                    None,
                    None,
                    Some(WriterProperty::BloomFilterEnabled),
                    Some(b),
                ),
            ],
        );
    }

    #[tokio::test]
    async fn test_diagnose_level_is_the_dearest_cap() {
        let advice_only = WriterLayout {
            bloom: Bloom::Off,
            ..WriterLayout::contract()
        };
        let (_table, generation) = one_file(advice_only).await;
        let report = diagnose(&generation, Level::Optimal);
        assert_eq!(report.summary.level, Level::Optimal, "advice caps nothing");
        assert_eq!((report.summary.capping, report.summary.advice), (0, 1));

        let dictionary = WriterLayout {
            dictionary: Dictionary::Value,
            ..WriterLayout::contract()
        };
        let (_table, generation) = one_file(dictionary).await;
        let report = diagnose(&generation, Level::Optimal);
        assert_eq!(report.summary.level, Level::Balanced);
        assert_eq!((report.summary.capping, report.summary.advice), (1, 0));

        let mut table = Table::new(KeyType::Long, unpartitioned());
        two_disjoint_files(&mut table, Partition::None, dictionary).await;
        let report = diagnose(
            &load(&mut table, Residency::Full, None).await,
            Level::Optimal,
        );
        assert_eq!(
            report.capping_codes(),
            [
                FindingCode::DictionaryEncodedValue,
                FindingCode::NoBucketPartition
            ]
        );
        assert_eq!(
            report.summary.level,
            Level::Permissive,
            "a balanced cap and a permissive one leave the table permissive"
        );
    }

    #[tokio::test]
    async fn test_diagnose_bloom_residency_caps_balanced_with_no_finding() {
        let mut table = contract_table().await;
        let report = diagnose(
            &load(&mut table, Residency::Bloom, None).await,
            Level::Balanced,
        );
        assert_findings(&report, &[]);
        assert_eq!(report.summary.level, Level::Balanced);
        assert_eq!(
            report.summary.level_by_residency,
            BTreeMap::from([
                (Residency::Full, Level::Optimal),
                (Residency::Disk, Level::Optimal),
                (Residency::Bloom, Level::Balanced),
            ])
        );
    }

    #[tokio::test]
    async fn test_diagnose_level_by_residency_combines_the_layout_with_each_tier() {
        let (_table, report, _json) = dictionary_value_report().await;
        assert_eq!(
            report.summary.level_by_residency,
            BTreeMap::from([
                (Residency::Full, Level::Balanced),
                (Residency::Disk, Level::Balanced),
                (Residency::Bloom, Level::Balanced),
            ])
        );
        assert_eq!(report.summary.lookups, Level::Optimal);
        assert_eq!(
            diagnose(
                &one_file(WriterLayout::contract()).await.1,
                Level::Permissive
            )
            .summary
            .lookups,
            Level::Permissive,
            "the declared level is reported as given"
        );
    }

    #[tokio::test]
    async fn test_diagnose_level_by_residency_omits_a_tier_that_cannot_load() {
        let no_filter = WriterLayout {
            bloom: Bloom::Off,
            ..WriterLayout::contract()
        };
        let (_table, generation) = one_file(no_filter).await;
        let report = diagnose(&generation, Level::Optimal);
        assert_eq!(report.summary.predicted.bloom, None);
        assert_eq!(
            report.summary.level_by_residency,
            BTreeMap::from([
                (Residency::Full, Level::Optimal),
                (Residency::Disk, Level::Optimal),
            ])
        );
    }

    #[tokio::test]
    async fn test_judge_refuses_below_level_only_under_strict() {
        let (_table, report, _json) = dictionary_value_report().await;
        let summary = &report.summary;
        let codes = report.capping_codes();
        judge(TABLE, summary.level, &codes, Level::Optimal, false).unwrap();
        let reason = testing::refusal_of(judge(TABLE, summary.level, &codes, Level::Optimal, true));
        assert_eq!(
            reason,
            RefusalReason::BelowLevel {
                declared: Level::Optimal,
                achieved: Level::Balanced,
                findings: vec![FindingCode::DictionaryEncodedValue],
            }
        );
        for declared in [Level::Balanced, Level::Permissive] {
            judge(TABLE, summary.level, &codes, declared, true).unwrap();
        }

        let advice_only = WriterLayout {
            bloom: Bloom::Off,
            ..WriterLayout::contract()
        };
        let (_table, generation) = one_file(advice_only).await;
        let report = diagnose(&generation, Level::Optimal);
        assert_eq!(report.summary.advice, 1);
        judge(
            TABLE,
            report.summary.level,
            &report.capping_codes(),
            Level::Optimal,
            true,
        )
        .unwrap();
    }

    #[tokio::test]
    async fn test_judge_names_only_the_codes_the_declared_level_refuses() {
        let mut table = Table::bucketed(ONE);
        let first = testing::keys_from(0, FILE_KEYS);
        let interleaved: Vec<i64> = first.iter().map(|key| key + 1).collect();
        let layout = WriterLayout {
            dictionary: Dictionary::Value,
            ..WriterLayout::contract()
        };
        for (name, keys) in [("a", &first), ("b", &interleaved)] {
            let typed = testing::long_keys(keys);
            table
                .add_file(file(name, &typed, Partition::Bucket(0), layout))
                .await;
        }
        let report = diagnose(
            &load(&mut table, Residency::Full, None).await,
            Level::Balanced,
        );
        assert_eq!(
            report.capping_codes(),
            [
                FindingCode::DictionaryEncodedValue,
                FindingCode::OverlappingFiles
            ]
        );
        let reason = testing::refusal_of(judge(
            TABLE,
            report.summary.level,
            &report.capping_codes(),
            Level::Balanced,
            true,
        ));
        assert_eq!(
            reason,
            RefusalReason::BelowLevel {
                declared: Level::Balanced,
                achieved: Level::Permissive,
                findings: vec![FindingCode::OverlappingFiles],
            }
        );
    }

    #[tokio::test]
    async fn test_diagnose_predicted_bytes_per_tier_equal_the_loaded_index_bytes() {
        for residency in Residency::ALL {
            let scratch = tempfile::tempdir().unwrap();
            let mut table = contract_table().await;
            let local = testing::local_dir(scratch.path(), table.storage.stats(), None);
            let local = (residency == Residency::Disk).then_some(&local);
            let generation = load(&mut table, residency, local).await;
            let report = diagnose(&generation, Level::Optimal);
            assert_eq!(report.summary.residency, residency);
            assert_eq!(report.summary.index_bytes, generation.index_bytes);
            let predicted = report.summary.predicted;
            assert_eq!(
                predicted.in_tier(residency).unwrap(),
                approx::count(generation.index_bytes),
                "{residency}"
            );
            let resident: usize = report.files.iter().map(|file| file.resident_bytes).sum();
            assert_eq!(resident, generation.index_bytes, "{residency}");
            let per_file: Vec<ResidentBytes> = generation
                .files
                .iter()
                .map(|file| {
                    FileIndex::predicted_resident_bytes(
                        file.index.metadata(),
                        file.index.key_leaf(),
                        file.index.key_type(),
                        file.index.handle(),
                    )
                })
                .collect();
            assert_eq!(
                predicted.full,
                per_file.iter().map(|file| file.full).sum::<u64>()
            );
            assert_eq!(
                predicted.disk_memory,
                per_file.iter().map(|file| file.disk_memory).sum::<u64>()
            );
            assert_eq!(
                predicted.disk_bytes,
                per_file.iter().map(|file| file.disk_bytes).sum::<u64>()
            );
            assert_eq!(
                predicted.key_chunk_bytes,
                report
                    .files
                    .iter()
                    .map(|file| file.key_chunk_bytes)
                    .sum::<u64>()
            );
            assert_eq!(
                predicted.bloom,
                per_file.iter().map(|file| file.bloom).sum::<Option<u64>>()
            );
            assert_eq!(
                predicted.filter_bytes,
                report
                    .files
                    .iter()
                    .map(|file| file.bloom_bytes)
                    .sum::<Option<u64>>()
            );
            assert_eq!(
                predicted.bloom.map(|bloom| bloom - predicted.disk_memory),
                predicted.filter_bytes,
                "the filters are what the bloom tier holds beyond the disk tier's memory"
            );
            assert!(predicted.bloom_bits_per_key.unwrap() >= BLOOM_BITS_PER_KEY_MIN);
        }
    }

    /// The report of a one-file table whose value column is
    /// dictionary-encoded, so it carries exactly one capping finding, with
    /// the JSON it serializes to.
    async fn dictionary_value_report() -> (Table, TableReport, Json) {
        let layout = WriterLayout {
            dictionary: Dictionary::Value,
            ..WriterLayout::contract()
        };
        let (table, generation) = one_file(layout).await;
        let report = diagnose(&generation, Level::Optimal);
        let json = serde_json::to_value(&report).unwrap();
        (table, report, json)
    }

    /// The rows of `deleted_file_table`'s file `f` its delete removes.
    const DELETED_ROWS: [i64; 2] = [1, 3];

    /// A one-bucket table holding one file `f` of `FILE_KEYS` long keys,
    /// committed as `FIRST_SNAPSHOT`, then as `SECOND_SNAPSHOT` beside a
    /// position delete file removing `DELETED_ROWS` of `f`, a dangling one
    /// naming a file the table never held, and `update`, when given, a
    /// file of the deleted rows' keys written again; each snapshot loaded
    /// in `residency` with `strict` off, the second over the first, the
    /// Disk tier's chunk files under `scratch`.
    async fn deleted_file_table(
        residency: Residency,
        update: bool,
        scratch: &std::path::Path,
    ) -> (Table, Generation, Generation) {
        let mut table = Table::bucketed(ONE);
        let local = testing::local_dir(scratch, table.storage.stats(), None);
        let local = (residency == Residency::Disk).then_some(&local);
        let keys = testing::long_keys(&testing::keys_from(0, FILE_KEYS));
        table
            .add_file(file(
                "f",
                &keys,
                Partition::Bucket(0),
                WriterLayout::contract(),
            ))
            .await;
        table
            .commit(FIRST_SNAPSHOT, None, FIRST_SEQUENCE, FIRST_VERSION)
            .await;
        let config = TableConfig {
            strict: false,
            ..table.config(residency, None)
        };
        let before = table.load(&config, local, None).await.unwrap();
        table.add_position_delete("d", "f", &DELETED_ROWS, 0).await;
        table
            .add_position_delete("dangling", "gone", &DELETED_ROWS, 0)
            .await;
        let updated: Vec<Key> = DELETED_ROWS
            .iter()
            .map(|&row| keys[approx::length(u64::try_from(row).unwrap())].clone())
            .collect();
        if update {
            table
                .add_file(file(
                    "u",
                    &updated,
                    Partition::Bucket(0),
                    WriterLayout::contract(),
                ))
                .await;
        }
        table
            .commit(
                SECOND_SNAPSHOT,
                Some(FIRST_SNAPSHOT),
                SECOND_SEQUENCE,
                SECOND_VERSION,
            )
            .await;
        let after = table.load(&config, local, Some(&before)).await.unwrap();
        (table, before, after)
    }

    /// The `deletes_pending` findings of `report`.
    fn pending(report: &TableReport) -> Vec<&Finding> {
        report
            .findings
            .iter()
            .filter(|finding| finding.code() == FindingCode::DeletesPending)
            .collect()
    }

    /// Deletes that remove rows raise one `deletes_pending`, advice of the
    /// table, and the summary gives what the load applied: the rows and
    /// the one delete file that names a held file, not the dangling one.
    /// A snapshot with none raises none.
    #[tokio::test]
    async fn test_diagnose_deletes_pending_reports_the_applied_deletes() {
        let scratch = tempfile::tempdir().unwrap();
        let (_table, before, after) =
            deleted_file_table(Residency::Full, false, scratch.path()).await;
        let clean = diagnose(&before, Level::Optimal);
        assert!(pending(&clean).is_empty());
        assert_eq!(clean.summary.deletes, DeleteSummary::default());

        let report = diagnose(&after, Level::Optimal);
        let findings = pending(&report);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].caps(), None);
        assert_eq!(findings[0].penalty(), None);
        assert_eq!(findings[0].file(), None);
        assert_eq!(report.summary.level, Level::Optimal, "deletes cap nothing");
        assert_eq!(report.summary.advice, report.findings.len());
        let deletes = report.summary.deletes;
        assert_eq!(deletes, after.delete_summary());
        assert_eq!(deletes.rows, approx::count(DELETED_ROWS.len()));
        let applying_delete_files = 1;
        assert_eq!(deletes.files, applying_delete_files, "not the dangling one");
        assert_eq!(
            deletes.fraction.to_bits(),
            approx::ratio(deletes.rows, after.records).to_bits()
        );
        assert!(deletes.state_bytes > 0);
        assert_eq!(
            compacted_level(&after, &layout_caps(&LayoutView::of(&after))),
            None
        );
    }

    /// Every tier's predicted memory counts the delete state at its bound,
    /// which holds what the load applied; the chunk files on disk do not.
    #[tokio::test]
    async fn test_diagnose_predicted_memory_counts_the_delete_bound() {
        for residency in [Residency::Full, Residency::Disk, Residency::Bloom] {
            let scratch = tempfile::tempdir().unwrap();
            let (_table, before, after) =
                deleted_file_table(residency, false, scratch.path()).await;
            let bound = after.delete_bound();
            assert!(bound >= approx::count(after.delete_summary().state_bytes));
            assert_eq!(before.delete_bound(), 0);
            let without = diagnose(&before, Level::Optimal).summary.predicted;
            let with = diagnose(&after, Level::Optimal).summary.predicted;
            assert_eq!(with.full, without.full + bound, "{residency}");
            assert_eq!(with.disk_memory, without.disk_memory + bound, "{residency}");
            assert_eq!(
                with.bloom,
                without.bloom.map(|bloom| bloom + bound),
                "{residency}"
            );
            assert_eq!(with.disk_bytes, without.disk_bytes, "{residency}");
        }
    }

    /// An update, a newer file of the deleted rows' keys, overlaps the file
    /// it updates and caps the level at `permissive`; `deletes_pending`
    /// then names the level a compaction restores, the residency's best.
    #[tokio::test]
    async fn test_diagnose_deletes_pending_names_the_level_a_compaction_restores() {
        let scratch = tempfile::tempdir().unwrap();
        let (_table, _, after) = deleted_file_table(Residency::Full, true, scratch.path()).await;
        let caps = layout_caps(&LayoutView::of(&after));
        assert_eq!(caps.level, Level::Permissive);
        assert_eq!(caps.codes(), [FindingCode::OverlappingFiles]);
        assert_eq!(compacted_level(&after, &caps), Some(Level::Optimal));
        let report = diagnose(&after, Level::Optimal);
        assert_eq!(pending(&report).len(), 1);
        assert_eq!(
            report.summary.deletes.rows,
            approx::count(DELETED_ROWS.len())
        );
    }

    /// An update beside an unbucketed file: with no files overlapping,
    /// `no_bucket_partition` still caps the table at `permissive`, so a
    /// compaction of the overlap restores no level and `deletes_pending`
    /// names none.
    #[tokio::test]
    async fn test_diagnose_deletes_pending_names_no_level_where_another_cap_remains() {
        let mut table = Table::new(KeyType::Long, unpartitioned());
        let stray = testing::long_keys(&testing::keys_from(EVOLVED_KEYS_START, FILE_KEYS));
        table
            .add_file(file("s", &stray, Partition::None, WriterLayout::contract()))
            .await;
        table.evolve(ONE);
        let keys = testing::long_keys(&testing::keys_from(0, FILE_KEYS));
        let updated: Vec<Key> = DELETED_ROWS
            .iter()
            .map(|&row| keys[approx::length(u64::try_from(row).unwrap())].clone())
            .collect();
        for (name, keys) in [("f", &keys), ("u", &updated)] {
            table
                .add_file(file(
                    name,
                    keys,
                    Partition::Bucket(0),
                    WriterLayout::contract(),
                ))
                .await;
        }
        table.add_position_delete("d", "f", &DELETED_ROWS, 0).await;
        table
            .commit(FIRST_SNAPSHOT, None, FIRST_SEQUENCE, FIRST_VERSION)
            .await;
        let config = TableConfig {
            strict: false,
            ..table.config(Residency::Full, None)
        };
        let generation = table.load(&config, None, None).await.unwrap();
        let caps = layout_caps(&LayoutView::of(&generation));
        assert_eq!(caps.level, Level::Permissive);
        assert_eq!(
            caps.codes(),
            [
                FindingCode::NoBucketPartition,
                FindingCode::OverlappingFiles
            ]
        );
        assert_eq!(compacted_level(&generation, &caps), None);
        let report = diagnose(&generation, Level::Optimal);
        assert_eq!(pending(&report).len(), 1);
        assert_eq!(
            report.summary.deletes.rows,
            approx::count(DELETED_ROWS.len())
        );
    }

    #[tokio::test]
    async fn test_serialize_table_summary_snake_case_fields_with_display_strings() {
        let (_table, report, json) = dictionary_value_report().await;
        let summary = json["summary"].as_object().unwrap();
        let names: BTreeSet<&str> = summary.keys().map(String::as_str).collect();
        assert_eq!(
            names,
            BTreeSet::from([
                "table",
                "snapshot_id",
                "level",
                "lookups",
                "residency",
                "file_count",
                "buckets",
                "buckets_with_files",
                "max_files_per_bucket",
                "records",
                "index_bytes",
                "predicted",
                "level_by_residency",
                "lookup_cost",
                "deletes",
                "capping",
                "advice",
            ])
        );
        assert_eq!(
            summary["deletes"],
            serde_json::json!({"rows": 0, "files": 0, "fraction": 0.0, "state_bytes": 0}),
            "a snapshot with no deletes"
        );
        assert_eq!(
            summary["lookup_cost"],
            serde_json::json!({
                "gets_per_hit": VALUE_PAGE_GETS + DICTIONARY_PAGE_GETS,
                "gets_per_miss": 0,
                "files_probed": FILES_PROBED_ALONE,
            }),
            "a full hit fetches the value dictionary page beside the value page"
        );
        assert_eq!(summary["table"], Json::String(TABLE.to_string()));
        assert_eq!(
            summary["snapshot_id"],
            Json::String(FIRST_SNAPSHOT.to_string())
        );
        assert_eq!(summary["level"], Json::String(Level::Balanced.to_string()));
        assert_eq!(summary["lookups"], Json::String(Level::Optimal.to_string()));
        assert_eq!(
            summary["level_by_residency"][Residency::Bloom.name()],
            Json::String(Level::Balanced.to_string())
        );
        assert_eq!(
            summary["residency"],
            Json::String(Residency::Full.to_string())
        );
        assert_eq!(summary["buckets"], Json::from(ONE));
        assert_eq!(summary["capping"], Json::from(1));
        assert_eq!(summary["advice"], Json::from(0));
        assert_eq!(
            summary["predicted"]["full"],
            Json::from(report.summary.predicted.full)
        );
        assert!(summary["predicted"]["bloom_bits_per_key"].is_f64());
        assert_eq!(
            serde_json::to_value(&report.summary).unwrap(),
            json["summary"]
        );
    }

    #[tokio::test]
    async fn test_serialize_finding_snake_case_fields_with_display_strings() {
        let (table, _report, json) = dictionary_value_report().await;
        let finding = &json["findings"][0];
        assert_eq!(finding["caps"], Json::String(Level::Balanced.to_string()));
        assert_eq!(
            finding["code"],
            Json::String(FindingCode::DictionaryEncodedValue.to_string())
        );
        assert_eq!(
            finding["property"],
            Json::String(WriterProperty::EnableDictionary.to_string())
        );
        assert_eq!(
            finding["penalty"]["extra_gets_per_hit"],
            Json::from(DICTIONARY_PAGE_GETS)
        );
        assert_eq!(finding["penalty"]["files_probed_per_lookup"], Json::Null);
        assert_eq!(finding["file"], Json::String(table.path_of("f")));
        assert!(finding["message"].is_string());
        let advice = Finding::advice(FindingCode::LongKeys, None, String::new(), None);
        assert_eq!(serde_json::to_value(&advice).unwrap()["caps"], Json::Null);
    }

    #[tokio::test]
    async fn test_serialize_file_report_snake_case_fields_with_display_strings() {
        let (_table, _report, json) = dictionary_value_report().await;
        let file = json["files"][0].as_object().unwrap();
        let file_names: BTreeSet<&str> = file.keys().map(String::as_str).collect();
        assert_eq!(
            file_names,
            BTreeSet::from([
                "path",
                "bucket",
                "key_type",
                "rows",
                "row_groups",
                "key_pages",
                "value_pages",
                "max_key_page_rows",
                "max_key_bytes",
                "avg_value_page_bytes",
                "avg_value_bytes",
                "pages_aligned",
                "dictionary_columns",
                "bloom_bytes",
                "bloom_bits_per_key",
                "key_chunk_bytes",
                "distinct_page_mins_ratio",
                "bounds_present",
                "resident_bytes",
            ])
        );
        assert_eq!(file["key_type"], Json::String(KeyType::Long.to_string()));
        assert_eq!(
            file["dictionary_columns"],
            Json::Array(vec![Json::String(VALUE_COLUMN.to_string())])
        );
        assert_eq!(file["bounds_present"], Json::Bool(true));
    }
}
