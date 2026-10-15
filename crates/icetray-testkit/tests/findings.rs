//! The contract fixture and one fixture per `FindingCode` (RFD 007, RFD
//! 016), each written through the Iceberg writer and diagnosed after a Full
//! load. Every test asserts the designed code at its cap and the exact set
//! of codes the report raises: the capping set is the designed one (empty
//! for an advice fixture), and advice a layout cannot avoid is named in the
//! test's doc comment and asserted with it. `strict` refuses a capping
//! fixture below `optimal` naming its codes and loads an advice fixture:
//! the doctor's verdicts live here, the GET costs of the same layouts in
//! `lookup.rs` and `refresh.rs`. The completeness of `FindingCode::ALL` is
//! core's to prove (its successor chain in `check.rs`); this file has one
//! test per code by convention, named after the code, and nothing here
//! checks that count.

use std::collections::{BTreeSet, HashSet};
use std::ops::Range;
use std::sync::Arc;

use iceberg::Catalog;
use icetray_core::ColumnRead;
use icetray_core::approx;
use icetray_core::check::thresholds::{
    BLOOM_BITS_PER_KEY_MIN, BLOOM_BYTES_PER_KEY_ADVICE, DISTINCT_MINS_RATIO_ADVICE,
    FILE_BYTES_ADVICE, FILES_PER_BUCKET_FACTOR, KEY_PAGE_ROWS_LARGE, MOST_VALUE_PAGE_BYTES,
    RUN_ROWS_ADVICE,
};
use icetray_core::config::{LocalConfig, Residency, ServerConfig, TableConfig, ValueFormat};
use icetray_core::{
    Capping, Error, FILE_PREFIX, FileFault, Finding, FindingCode, HeaderField, KeyType, Level,
    LocalDir, OrderValue, RefusalReason, Row, ScanLimits, Storage, TableReport, WriterProperty,
    diagnose,
};
use icetray_testkit::fixture::{
    FOUR_BUCKETS, IDS_2K, IDS_4K, LONG_KEY_BYTES, LONG_KEY_PAD, NAMESPACE, ONE_BUCKET, SAMPLE,
    SECOND_CLASS, SMALL_FILE_BYTES, TWO_BUCKETS, TWO_FILES, UNPARTITIONED_FILE_BYTES,
    WIDE_VALUE_BYTES, local_catalog, residue_class,
};
use icetray_testkit::layout::VALUE_COLUMN;
use icetray_testkit::layout::{OVERSIZED_FPP, UNDERSIZED_FPP};
use icetray_testkit::verify::{self, LOAD_METADATA_GETS, METADATA_FILE_GETS, Served};
use icetray_testkit::{
    Bloom, Dictionary, Fixture, FixtureSpec, Layout, OrderKind, Ordered, Result, Statistics,
    TestkitError, write_fixture,
};
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use parquet::file::page_index::column_index::ColumnIndexMetaData;
use tempfile::TempDir;

/// Rows per key page of the large-key-page fixture: twice the threshold, so
/// the finding caps the level and is not the chosen margin.
const LARGE_PAGE_ROWS: i64 = 2 * KEY_PAGE_ROWS_LARGE;

/// Ids of the large-key-page fixture: two such pages in its one file.
const LARGE_PAGE_IDS: Range<u64> = 0..8_000;

/// Bytes per value of the large-key-page fixture: small enough that a
/// 4,000-row value page stays under the large-value-page advice, so the
/// fixture raises the one code it is designed for.
const SMALL_VALUE_BYTES: usize = 24;

/// The bounds fixture cuts its statistics to this many bytes, shorter than
/// any string key.
const BOUNDS_TRUNCATE: usize = 8;

/// The column-index fixture cuts page minimums to this many bytes, inside
/// the shared prefix every key carries, so every cut minimum is the same.
const COLUMN_INDEX_TRUNCATE: usize = 16;

/// The shared prefix of the column-index fixture's keys: well above the
/// truncation length, so the cut never reaches a digit.
const SHARED_PREFIX_PAD: usize = 64;

/// Rows per page of the column-index fixture: three pages per file (250
/// rows per bucket), so one shared cut minimum in three is well below the
/// advice ratio.
const SMALL_PAGE_ROWS: usize = 100;

/// Rows of the over-ceiling fixture, all in one value page: enough that
/// the key filter, sized to them, draws no bloom advice of its own.
const OVER_CEILING_ROWS: u64 = 16;

/// Bytes per value of the over-ceiling fixture: `OVER_CEILING_ROWS` of them
/// make one value page just over `MOST_VALUE_PAGE_BYTES`.
const OVER_CEILING_VALUE_BYTES: u64 = MOST_VALUE_PAGE_BYTES / OVER_CEILING_ROWS + 1;

/// The over-ceiling fixture cuts its statistics and column index to this
/// many bytes, so the value column's page minimum and maximum, each a
/// whole value of megabytes otherwise, stay small in the footer. The key
/// is a long, whose statistics no length cuts, so its bounds stay exact.
const OVER_CEILING_TRUNCATE: usize = 16;

/// Rows of the large-file fixture, at `WIDE_VALUE_BYTES` of incompressible
/// value each: 9,000 rows is 140.6 MiB, ten percent over `FILE_BYTES_ADVICE`
/// (128 MiB), the fewest thousands of rows that clear it.
const LARGE_FILE_ROWS: u64 = 9_000;

/// A file size target above the large-file fixture's one file.
const LARGE_FILE_TARGET_BYTES: usize = 256 * 1024 * 1024;

/// Ids of the many-files fixture: 3,000 per bucket, three batches, so
/// every bucket holds more files than `FILES_PER_BUCKET_FACTOR` explains.
const MANY_FILES_IDS: Range<u64> = 0..48_000;

/// Keys looked up per row group of the statistics-free file: one in each
/// half, so every row group is routed to.
const PROBES_PER_ROW_GROUP: usize = 2;

/// An id inside the statistics-free file's range.
const INSIDE_ID: u64 = 1_234;

/// Writes `spec` and serves it in the Full tier through its catalog.
async fn write_and_serve(
    catalog: &Arc<dyn Catalog>,
    spec: FixtureSpec,
) -> Result<(Fixture, Served)> {
    let fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    let served = verify::serve_catalog(&fixture, catalog, Residency::Full, false).await?;
    Ok((fixture, served))
}

/// The distinct codes of `report` whose findings cap the level.
fn capping_codes(report: &TableReport) -> BTreeSet<FindingCode> {
    report
        .capping()
        .into_iter()
        .map(|capping| capping.code)
        .collect()
}

/// The distinct codes of `report` whose findings cap nothing.
fn advice_codes(report: &TableReport) -> BTreeSet<FindingCode> {
    report
        .findings
        .iter()
        .filter(|finding| finding.caps().is_none())
        .map(Finding::code)
        .collect()
}

/// Asserts `report` raises exactly `capping` among the findings that cap
/// the level and `advice` among those that do not, and that its level is
/// the dearest of their caps.
fn assert_codes(report: &TableReport, capping: &[FindingCode], advice: &[FindingCode]) {
    assert_eq!(
        capping_codes(report),
        capping.iter().copied().collect::<BTreeSet<_>>(),
        "capping findings: {:?}",
        report.findings
    );
    assert_eq!(
        advice_codes(report),
        advice.iter().copied().collect::<BTreeSet<_>>(),
        "advice findings: {:?}",
        report.findings
    );
    let level = capping
        .iter()
        .filter_map(|code| code.cap())
        .fold(Level::Optimal, Level::max);
    assert_eq!(report.summary.level, level);
    assert_eq!(
        report.summary.capping,
        report.findings.len() - report.summary.advice
    );
}

/// Serves `fixture` under `strict` at the Full tier's default `optimal`:
/// the capping codes the `below_level` refusal names, or none when it
/// loads, which advice alone must allow.
async fn strict_refusal(fixture: &Fixture, catalog: &Arc<dyn Catalog>) -> Result<Vec<FindingCode>> {
    let Err(error) = verify::serve_catalog(fixture, catalog, Residency::Full, true).await else {
        return Ok(Vec::new());
    };
    let TestkitError::Core(Error::Refused {
        reason: RefusalReason::BelowLevel { findings, .. },
        ..
    }) = error
    else {
        return Err(error);
    };
    Ok(findings.into_iter().map(|capping| capping.code).collect())
}

/// The one finding of `code` in `report`.
fn finding(report: &TableReport, code: FindingCode) -> &Finding {
    report
        .findings
        .iter()
        .find(|finding| finding.code() == code)
        .unwrap_or_else(|| panic!("{code} in {:?}", report.findings))
}

#[tokio::test]
async fn test_contract_fixture_is_optimal_with_no_findings() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let (_fixture, served) =
        write_and_serve(&catalog, FixtureSpec::small("kv_contract", KeyType::Long)).await?;
    let report = diagnose(
        &served.current(),
        Level::Optimal,
        &ValueFormat::opaque,
        None,
    );
    assert_eq!(report.summary.level, Level::Optimal);
    assert!(report.findings.is_empty(), "{:?}", report.findings);
    assert_eq!((report.summary.capping, report.summary.advice), (0, 0));
    Ok(())
}

/// A dictionary-encoded value column costs one dictionary page per file on
/// a cold cache besides the value pages, and nothing warm.
#[tokio::test]
async fn test_dictionary_encoded_value_caps_balanced_by_one_extra_get() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        layout: Layout {
            dictionary: Dictionary::Value,
            ..Layout::contract()
        },
        ..FixtureSpec::small("kv_dictvalue", KeyType::Long)
    };
    let (fixture, served) = write_and_serve(&catalog, spec).await?;
    let generation = served.current();
    let report = diagnose(&generation, Level::Optimal, &ValueFormat::opaque, None);
    assert_codes(&report, &[FindingCode::DictionaryEncodedValue], &[]);
    assert_eq!(
        finding(&report, FindingCode::DictionaryEncodedValue)
            .penalty()
            .and_then(|penalty| penalty.extra_gets_per_hit),
        Some(1)
    );

    let keys = fixture.keys_at(&SAMPLE);
    let value_pages = verify::expected_cold_gets(&generation, &keys, &[VALUE_COLUMN])?;
    let files_touched: HashSet<String> = generation
        .plan(&keys)
        .into_iter()
        .flatten()
        .map(|(file, _page)| file.entry.path.clone())
        .collect();
    let cold = verify::verify_hits(&served, &fixture, &SAMPLE).await?;
    assert_eq!(
        cold,
        value_pages + approx::count(files_touched.len()),
        "one value page per key and one dictionary page per file"
    );
    let warm = verify::verify_hits(&served, &fixture, &SAMPLE).await?;
    assert_eq!(warm, 0);
    Ok(())
}

#[tokio::test]
async fn test_large_key_pages_penalty_is_the_rows_decoded() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: LARGE_PAGE_IDS,
        buckets: ONE_BUCKET,
        value_bytes: SMALL_VALUE_BYTES,
        layout: Layout {
            page_rows: usize::try_from(LARGE_PAGE_ROWS).unwrap(),
            ..Layout::contract()
        },
        ..FixtureSpec::small("kv_bigpages", KeyType::Long)
    };
    let (_fixture, served) = write_and_serve(&catalog, spec).await?;
    let report = diagnose(
        &served.current(),
        Level::Optimal,
        &ValueFormat::opaque,
        None,
    );
    assert_codes(&report, &[FindingCode::LargeKeyPages], &[]);
    assert_eq!(
        finding(&report, FindingCode::LargeKeyPages)
            .penalty()
            .and_then(|penalty| penalty.keys_decoded_per_lookup),
        Some(LARGE_PAGE_ROWS)
    );
    Ok(())
}

/// Twenty unbucketed files of a few hundred kibibytes in one bucket also
/// draw `ManyFilesPerBucket`; `strict` refuses the level it caps and names only it.
#[tokio::test]
async fn test_no_bucket_partition_caps_permissive() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        buckets: 0,
        layout: Layout {
            target_file_bytes: UNPARTITIONED_FILE_BYTES,
            ..Layout::contract()
        },
        ..FixtureSpec::small("kv_flat", KeyType::Long)
    };
    let (fixture, served) = write_and_serve(&catalog, spec).await?;
    let generation = served.current();
    assert!(generation.files.len() >= TWO_FILES);
    let report = diagnose(&generation, Level::Optimal, &ValueFormat::opaque, None);
    assert_codes(
        &report,
        &[FindingCode::NoBucketPartition],
        &[FindingCode::ManyFilesPerBucket],
    );
    assert_eq!(
        finding(&report, FindingCode::NoBucketPartition)
            .penalty()
            .and_then(|penalty| penalty.files_probed_per_lookup),
        Some(generation.files.len())
    );
    assert_eq!(
        strict_refusal(&fixture, &catalog).await?,
        vec![FindingCode::NoBucketPartition]
    );
    Ok(())
}

#[tokio::test]
async fn test_overlapping_files_caps_permissive() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let mut fixture = write_fixture(
        catalog.as_ref(),
        NAMESPACE,
        FixtureSpec::interleaved("kv_interleaved"),
    )
    .await?;
    fixture
        .append(catalog.as_ref(), residue_class(SECOND_CLASS), "half")
        .await?;
    let served = verify::serve_catalog(&fixture, &catalog, Residency::Full, false).await?;
    let report = diagnose(
        &served.current(),
        Level::Optimal,
        &ValueFormat::opaque,
        None,
    );
    assert_codes(&report, &[FindingCode::OverlappingFiles], &[]);
    assert_eq!(
        finding(&report, FindingCode::OverlappingFiles)
            .penalty()
            .and_then(|penalty| penalty.files_probed_per_lookup),
        Some(TWO_FILES)
    );
    assert_eq!(
        strict_refusal(&fixture, &catalog).await?,
        vec![FindingCode::OverlappingFiles]
    );
    Ok(())
}

/// The Iceberg writer records a manifest bound only from an exact Parquet
/// statistic, so a key column whose statistics are cut yields no bound at
/// all; the file's exact range still routes every lookup.
#[tokio::test]
async fn test_bounds_missing_is_advice() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: IDS_4K,
        layout: Layout {
            statistics: Statistics::Truncated(BOUNDS_TRUNCATE),
            ..Layout::contract()
        },
        ..FixtureSpec::small("kv_cutbounds", KeyType::String)
    };
    let (fixture, served) = write_and_serve(&catalog, spec).await?;
    let generation = served.current();
    let report = diagnose(&generation, Level::Optimal, &ValueFormat::opaque, None);
    assert_codes(&report, &[], &[FindingCode::BoundsMissing]);
    assert!(BOUNDS_TRUNCATE < fixture.key(0).as_bytes().len());
    for file in &generation.files {
        assert_eq!(file.entry.key_min, None, "{}", file.entry.path);
        assert_eq!(file.entry.key_max, None, "{}", file.entry.path);
    }
    assert!(report.files.iter().all(|file| !file.bounds_present));
    Ok(())
}

#[tokio::test]
async fn test_column_index_truncated_is_advice() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: IDS_4K,
        key_pad: SHARED_PREFIX_PAD,
        layout: Layout {
            page_rows: SMALL_PAGE_ROWS,
            column_index_truncate: Some(COLUMN_INDEX_TRUNCATE),
            statistics: Statistics::Full,
            ..Layout::contract()
        },
        ..FixtureSpec::small("kv_cutindex", KeyType::String)
    };
    let (_fixture, served) = write_and_serve(&catalog, spec).await?;
    let report = diagnose(
        &served.current(),
        Level::Optimal,
        &ValueFormat::opaque,
        None,
    );
    assert_codes(&report, &[], &[FindingCode::ColumnIndexTruncated]);
    for file in &report.files {
        assert!(
            file.distinct_page_mins_ratio < DISTINCT_MINS_RATIO_ADVICE,
            "{}: {}",
            file.path,
            file.distinct_page_mins_ratio
        );
    }
    Ok(())
}

#[tokio::test]
async fn test_large_value_pages_is_advice() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: IDS_4K,
        buckets: FOUR_BUCKETS,
        value_bytes: WIDE_VALUE_BYTES,
        ..FixtureSpec::small("kv_widepages", KeyType::Long)
    };
    let (_fixture, served) = write_and_serve(&catalog, spec).await?;
    let report = diagnose(
        &served.current(),
        Level::Optimal,
        &ValueFormat::opaque,
        None,
    );
    assert_codes(&report, &[], &[FindingCode::LargeValuePages]);
    Ok(())
}

/// One value page just over the lookup's ceiling, written by the Iceberg
/// writer as one file of incompressible values: the doctor advises on it
/// beside `large_value_pages`, `strict` still loads it, and a lookup into
/// the page is refused with the file fault the finding's message names.
#[tokio::test]
async fn test_value_pages_over_ceiling_is_advice() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: 0..OVER_CEILING_ROWS,
        buckets: ONE_BUCKET,
        value_bytes: approx::length(OVER_CEILING_VALUE_BYTES),
        layout: Layout {
            statistics: Statistics::Truncated(OVER_CEILING_TRUNCATE),
            column_index_truncate: Some(OVER_CEILING_TRUNCATE),
            ..Layout::contract()
        },
        ..FixtureSpec::small("kv_ceilingpages", KeyType::Long)
    };
    let (fixture, served) = write_and_serve(&catalog, spec).await?;
    let report = diagnose(
        &served.current(),
        Level::Optimal,
        &ValueFormat::opaque,
        None,
    );
    assert_codes(
        &report,
        &[],
        &[
            FindingCode::LargeValuePages,
            FindingCode::ValuePagesOverCeiling,
        ],
    );
    assert_eq!(report.files.len(), 1);
    assert_eq!(report.files[0].value_columns[0].value_pages, 1);
    assert!(report.files[0].value_columns[0].avg_value_page_bytes > MOST_VALUE_PAGE_BYTES);
    assert_eq!(
        finding(&report, FindingCode::ValuePagesOverCeiling).property(),
        Some(WriterProperty::PageSizeBytes)
    );
    let error = served
        .table
        .get(&fixture.key(0), &[ColumnRead::whole(VALUE_COLUMN)])
        .await
        .expect_err("the value page is over the ceiling");
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
    assert!(
        strict_refusal(&fixture, &catalog).await?.is_empty(),
        "an advice-only table loads under strict"
    );
    Ok(())
}

/// Advice alone never refuses: the same fixture loads under `strict`.
#[tokio::test]
async fn test_long_keys_is_advice() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: IDS_4K,
        key_pad: LONG_KEY_PAD,
        ..FixtureSpec::small("kv_longkeys", KeyType::String)
    };
    let (fixture, served) = write_and_serve(&catalog, spec).await?;
    let report = diagnose(
        &served.current(),
        Level::Optimal,
        &ValueFormat::opaque,
        None,
    );
    assert_codes(&report, &[], &[FindingCode::LongKeys]);
    assert!(
        report
            .files
            .iter()
            .all(|file| file.max_key_bytes == LONG_KEY_BYTES)
    );
    assert!(
        strict_refusal(&fixture, &catalog).await?.is_empty(),
        "an advice-only table loads under strict"
    );
    Ok(())
}

/// The one file's sixteen-kibibyte values under the row bound also draw
/// `LargeValuePages`; a byte bound would make the footer's column index
/// larger than the file's key chunk.
#[tokio::test]
async fn test_large_file_is_advice() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: 0..LARGE_FILE_ROWS,
        buckets: ONE_BUCKET,
        value_bytes: WIDE_VALUE_BYTES,
        layout: Layout {
            target_file_bytes: LARGE_FILE_TARGET_BYTES,
            ..Layout::contract()
        },
        ..FixtureSpec::small("kv_largefile", KeyType::Long)
    };
    let (fixture, served) = write_and_serve(&catalog, spec).await?;
    let generation = served.current();
    assert_eq!(generation.files.len(), 1);
    assert!(generation.files[0].entry.size > FILE_BYTES_ADVICE);
    let report = diagnose(&generation, Level::Optimal, &ValueFormat::opaque, None);
    assert_codes(
        &report,
        &[],
        &[FindingCode::LargeFile, FindingCode::LargeValuePages],
    );
    assert_eq!(
        finding(&report, FindingCode::LargeFile).file(),
        Some(fixture.files()[0].file_path())
    );
    Ok(())
}

#[tokio::test]
async fn test_many_files_per_bucket_is_advice() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: MANY_FILES_IDS,
        layout: Layout {
            target_file_bytes: SMALL_FILE_BYTES,
            ..Layout::contract()
        },
        ..FixtureSpec::small("kv_manyfiles", KeyType::Long)
    };
    let (_fixture, served) = write_and_serve(&catalog, spec).await?;
    let report = diagnose(
        &served.current(),
        Level::Optimal,
        &ValueFormat::opaque,
        None,
    );
    assert_codes(&report, &[], &[FindingCode::ManyFilesPerBucket]);
    assert!(
        approx::count(report.summary.max_files_per_bucket) > FILES_PER_BUCKET_FACTOR,
        "{} files in the fullest bucket",
        report.summary.max_files_per_bucket
    );
    Ok(())
}

#[tokio::test]
async fn test_no_bloom_filter_is_advice() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: IDS_2K,
        buckets: TWO_BUCKETS,
        layout: Layout {
            bloom: Bloom::Off,
            ..Layout::contract()
        },
        ..FixtureSpec::small("kv_nobloom", KeyType::Long)
    };
    let (_fixture, served) = write_and_serve(&catalog, spec).await?;
    let report = diagnose(
        &served.current(),
        Level::Optimal,
        &ValueFormat::opaque,
        None,
    );
    assert_codes(&report, &[], &[FindingCode::NoBloomFilter]);
    assert_eq!(report.summary.predicted.bloom, None);
    Ok(())
}

#[tokio::test]
async fn test_bloom_oversized_is_advice() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: IDS_2K,
        buckets: TWO_BUCKETS,
        layout: Layout {
            bloom: Bloom::Fpp(OVERSIZED_FPP),
            ..Layout::contract()
        },
        ..FixtureSpec::small("kv_bigbloom", KeyType::Long)
    };
    let (_fixture, served) = write_and_serve(&catalog, spec).await?;
    let report = diagnose(
        &served.current(),
        Level::Optimal,
        &ValueFormat::opaque,
        None,
    );
    assert_codes(&report, &[], &[FindingCode::BloomOversized]);
    for file in &report.files {
        let rows = u64::try_from(file.rows).unwrap();
        assert!(
            approx::ratio(file.bloom_bytes.unwrap(), rows) > BLOOM_BYTES_PER_KEY_ADVICE,
            "{}: {:?} bytes over {rows} rows",
            file.path,
            file.bloom_bytes
        );
    }
    Ok(())
}

#[tokio::test]
async fn test_bloom_undersized_is_advice() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: IDS_2K,
        buckets: TWO_BUCKETS,
        layout: Layout {
            bloom: Bloom::Fpp(UNDERSIZED_FPP),
            ..Layout::contract()
        },
        ..FixtureSpec::small("kv_smallbloom", KeyType::Long)
    };
    let (_fixture, served) = write_and_serve(&catalog, spec).await?;
    let report = diagnose(
        &served.current(),
        Level::Optimal,
        &ValueFormat::opaque,
        None,
    );
    assert_codes(&report, &[], &[FindingCode::BloomUndersized]);
    for file in &report.files {
        assert!(
            file.bloom_bits_per_key.unwrap() < BLOOM_BITS_PER_KEY_MIN,
            "{}: {:?} bits per key",
            file.path,
            file.bloom_bits_per_key
        );
    }
    Ok(())
}

/// Without statistics a file has no column index and no manifest bounds;
/// its several row groups, one contract page each, load one key chunk each,
/// and routing over the exact fences never yields a false miss.
#[tokio::test]
async fn test_statistics_free_multi_row_group_file_loads_one_chunk_per_row_group() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let contract = Layout::contract();
    let row_group_rows = contract.page_rows;
    let spec = FixtureSpec {
        ids: IDS_4K,
        buckets: ONE_BUCKET,
        layout: Layout {
            statistics: Statistics::None,
            row_group_rows: Some(row_group_rows),
            ..contract
        },
        ..FixtureSpec::small("kv_nostats", KeyType::Long)
    };
    let (fixture, served) = write_and_serve(&catalog, spec).await?;
    let generation = served.current();
    let files = approx::count(generation.files.len());
    let row_groups: u64 = generation
        .files
        .iter()
        .map(|file| approx::count(file.index.metadata().num_row_groups()))
        .sum();
    assert!(row_groups > files, "several row groups per file");
    assert_eq!(
        served.storage.stats().gets(),
        METADATA_FILE_GETS + LOAD_METADATA_GETS + files + row_groups,
        "the metadata, a footer per file, and a chunk per row group"
    );
    assert!(
        generation
            .files
            .iter()
            .all(|file| file.entry.key_min.is_none())
    );
    for id in fixture
        .id_range()
        .step_by(row_group_rows / PROBES_PER_ROW_GROUP)
    {
        let key = fixture.key(id);
        assert_eq!(
            served
                .table
                .get(&key, &[ColumnRead::whole(VALUE_COLUMN)])
                .await?
                .row,
            fixture.expected(&key),
            "key {key}"
        );
    }
    let stats = served.storage.stats();
    let before = stats.gets();
    let inside = fixture.absent_near(INSIDE_ID);
    assert_eq!(
        served
            .table
            .get(&inside, &[ColumnRead::whole(VALUE_COLUMN)])
            .await?
            .row,
        Row::Absent
    );
    assert_eq!(
        stats.gets(),
        before,
        "no false miss and no GET without statistics"
    );
    Ok(())
}

/// The raw order value of a key's first row in the ordered fixtures, and
/// the distance between a key's consecutive rows.
const ORDER_START: i64 = 1_000;
const ORDER_STEP: i64 = 10;

/// Rows per key of the ordered fixtures: three, so a key filter sized to a
/// file's distinct keys holds a third of its bits per row.
const ROWS_PER_KEY: u64 = 3;

fn three_rows(_: u64) -> u64 {
    ROWS_PER_KEY
}

/// An ordered table named `name` of `IDS_2K` in `TWO_BUCKETS`, each key
/// with `ROWS_PER_KEY` rows of a `kind` order column, in the contract
/// layout: one file per bucket, one row group each.
fn ordered_spec(name: &str, kind: OrderKind) -> FixtureSpec {
    FixtureSpec {
        ids: IDS_2K,
        buckets: TWO_BUCKETS,
        ..FixtureSpec::ordered(
            name,
            KeyType::Long,
            Ordered {
                kind,
                rows_per_key: three_rows,
                start: ORDER_START,
                step: ORDER_STEP,
            },
        )
    }
}

/// `ordered_spec` in `layout`.
fn ordered_in(name: &str, layout: Layout) -> FixtureSpec {
    FixtureSpec {
        layout,
        ..ordered_spec(name, OrderKind::Long)
    }
}

/// Ids of the size-rolled fixture: 500 keys of `ROWS_PER_KEY` rows, 1,500
/// rows in one bucket. Rolled at `SMALL_FILE_BYTES`, the writer closes a
/// file after its first write batch of `contract::PAGE_ROWS` rows, so the
/// snapshot holds two files, and row 1,000 is the second row of key 333:
/// that key's run spans both.
const ROLLED_IDS: Range<u64> = 0..500;

/// The ordered table a compaction that rolls files by size writes: one
/// bucket of `ROLLED_IDS` in two files of one snapshot, which meet where
/// the first file's last key is the second's first.
fn rolled_spec(name: &str) -> FixtureSpec {
    FixtureSpec {
        ids: ROLLED_IDS,
        buckets: ONE_BUCKET,
        layout: Layout {
            target_file_bytes: SMALL_FILE_BYTES,
            ..Layout::contract()
        },
        ..ordered_spec(name, OrderKind::Long)
    }
}

/// Asserts `served` holds exactly two files, the first's last key the
/// second's first.
fn assert_two_files_share_an_edge_key(served: &Served) {
    let generation = served.current();
    let [first, second] = &generation.files[..] else {
        panic!("two files, got {}", generation.files.len());
    };
    let (_, last) = first.index.key_range();
    let (next, _) = second.index.key_range();
    assert_eq!(last, next, "the files meet at one edge key");
}

/// `config` of `fixture` served through `catalog` over a fresh local
/// directory, which the returned directory keeps alive; the local
/// directory comes back beside the outcome, so a test reads what the load
/// wrote there whether or not it was refused.
async fn serve_on_disk(
    fixture: &Fixture,
    catalog: &Arc<dyn Catalog>,
    config: TableConfig,
) -> Result<(Result<Served>, Arc<LocalDir>, TempDir)> {
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(Storage::new(verify::storage_config_for_tests()));
    let local = Arc::new(LocalDir::open(
        &LocalConfig {
            dir: dir.path().to_path_buf(),
            index_budget_bytes: None,
            page_cache_disk_bytes: 0,
        },
        storage.stats(),
    )?);
    let served = verify::serve_over(
        storage,
        config,
        fixture.catalog_source(Arc::clone(catalog)),
        Some(Arc::clone(&local)),
    )
    .await;
    Ok((served, local, dir))
}

/// Files one snapshot added that meet where one's last key is the next
/// one's first, as a compaction that rolls files by size writes them, cap
/// the table at `balanced` and name the most files one edge key spans;
/// `strict` refuses the level and names only that code.
#[tokio::test]
async fn test_shared_edge_keys_caps_a_compaction_rolled_by_size_at_balanced() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let (fixture, served) = write_and_serve(&catalog, rolled_spec("ord_rolled")).await?;
    assert_two_files_share_an_edge_key(&served);
    let report = diagnose(
        &served.current(),
        Level::Optimal,
        &ValueFormat::opaque,
        None,
    );
    assert_codes(&report, &[FindingCode::SharedEdgeKeys], &[]);
    let shared = finding(&report, FindingCode::SharedEdgeKeys);
    assert_eq!(shared.caps(), Some(Level::Balanced));
    assert_eq!(
        shared
            .penalty()
            .and_then(|penalty| penalty.files_probed_per_lookup),
        Some(TWO_FILES)
    );
    assert_eq!(report.summary.lookup_cost.files_probed, TWO_FILES);
    assert_eq!(
        strict_refusal(&fixture, &catalog).await?,
        vec![FindingCode::SharedEdgeKeys]
    );
    Ok(())
}

/// Ids an append of the edge-key fixture writes after the compacted file's
/// last key: enough that the new file's filter draws no advice of its own.
const LATER_IDS: u64 = 500;

/// A file of a later snapshot whose first key is an earlier file's last,
/// and which meets it nowhere else, overlaps it: a Scan of that key reads
/// both, and the files of two snapshots are `overlapping_files`, not a
/// shared edge.
#[tokio::test]
async fn test_a_later_file_touching_an_edge_key_is_overlapping_files() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: ROLLED_IDS,
        buckets: ONE_BUCKET,
        ..ordered_spec("ord_touched", OrderKind::Long)
    };
    let mut fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    assert_eq!(fixture.file_count(), 1);
    let edge = ROLLED_IDS.end - 1;
    let later_order = ORDER_START + ORDER_STEP * i64::try_from(ROWS_PER_KEY).unwrap();
    let mut rows: Vec<(u64, i64)> = vec![(edge, later_order)];
    rows.extend((ROLLED_IDS.end..ROLLED_IDS.end + LATER_IDS).map(|id| (id, ORDER_START)));
    fixture
        .append_ordered(catalog.as_ref(), &rows, "later")
        .await?;
    let served = verify::serve_catalog(&fixture, &catalog, Residency::Full, false).await?;
    assert_two_files_share_an_edge_key(&served);
    let report = diagnose(
        &served.current(),
        Level::Optimal,
        &ValueFormat::opaque,
        None,
    );
    assert_codes(&report, &[FindingCode::OverlappingFiles], &[]);
    assert_eq!(
        finding(&report, FindingCode::OverlappingFiles)
            .penalty()
            .and_then(|penalty| penalty.files_probed_per_lookup),
        Some(TWO_FILES)
    );
    assert_eq!(
        strict_refusal(&fixture, &catalog).await?,
        vec![FindingCode::OverlappingFiles]
    );
    Ok(())
}

/// One file per bucket, no two files' key ranges meeting, and runs that
/// span key pages: every Scan reads one file, and the table is `optimal`
/// with no finding, the bloom filter judged by its distinct keys.
#[tokio::test]
async fn test_a_compacted_ordered_table_in_one_file_per_bucket_is_optimal() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let (fixture, served) =
        write_and_serve(&catalog, ordered_spec("ord_compacted", OrderKind::Long)).await?;
    let generation = served.current();
    assert_eq!(generation.files.len(), approx::length(TWO_BUCKETS));
    let report = diagnose(&generation, Level::Optimal, &ValueFormat::opaque, None);
    assert_codes(&report, &[], &[]);
    assert_eq!(report.summary.level, Level::Optimal);
    assert_eq!(report.summary.lookup_cost.files_probed, 1);
    assert!(
        strict_refusal(&fixture, &catalog).await?.is_empty(),
        "an optimal table loads under strict"
    );
    Ok(())
}

/// A dictionary-encoded order column costs every Scan its dictionary page
/// besides the order pages, one more GET per file under `full`.
#[tokio::test]
async fn test_dictionary_encoded_order_caps_at_balanced_under_full() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = ordered_in(
        "ord_dictorder",
        Layout {
            order_dictionary: true,
            ..Layout::contract()
        },
    );
    let (fixture, served) = write_and_serve(&catalog, spec).await?;
    let report = diagnose(
        &served.current(),
        Level::Optimal,
        &ValueFormat::opaque,
        None,
    );
    assert_codes(&report, &[FindingCode::DictionaryEncodedOrder], &[]);
    let dictionary = finding(&report, FindingCode::DictionaryEncodedOrder);
    assert_eq!(dictionary.caps(), Some(Level::Balanced));
    assert_eq!(dictionary.column(), None, "a finding about the table");
    assert_eq!(
        dictionary
            .penalty()
            .and_then(|penalty| penalty.extra_gets_per_hit),
        Some(1)
    );
    assert_eq!(
        dictionary.property(),
        Some(WriterProperty::EnableDictionary)
    );
    assert_eq!(
        strict_refusal(&fixture, &catalog).await?,
        vec![FindingCode::DictionaryEncodedOrder]
    );
    Ok(())
}

/// Under `disk` the order column's dictionary page is a read of the local
/// chunk file, so a dictionary-encoded order column caps nothing there,
/// while the report still names `full`'s level for the same layout.
#[tokio::test]
async fn test_dictionary_encoded_order_leaves_disk_optimal() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = ordered_in(
        "ord_dictorder_disk",
        Layout {
            order_dictionary: true,
            ..Layout::contract()
        },
    );
    let fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    let (served, _local, _scratch) =
        serve_on_disk(&fixture, &catalog, fixture.config(Residency::Disk, true)).await?;
    let report = diagnose(
        &served?.current(),
        Level::Optimal,
        &ValueFormat::opaque,
        None,
    );
    assert_codes(&report, &[], &[]);
    assert_eq!(report.summary.level, Level::Optimal);
    assert_eq!(
        report.summary.level_by_residency.get(&Residency::Full),
        Some(&Level::Balanced),
        "under full the dictionary page is a GET"
    );
    assert_eq!(
        report.summary.level_by_residency.get(&Residency::Disk),
        Some(&Level::Optimal)
    );
    Ok(())
}

/// The footer of the data file at `path`, a local location, with the page
/// indexes its writer wrote, which the server's kept footer does not all
/// hold.
fn written_footer(path: &str) -> Result<ParquetMetaData> {
    let local = path.strip_prefix(FILE_PREFIX).unwrap_or(path);
    Ok(ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .parse_and_finish(&std::fs::File::open(local)?)?)
}

/// A file without an order column index draws no finding: the order
/// fences place a bounded Scan's bounds in one round whatever the writer's
/// column index holds or leaves out, so the table stays `optimal` and
/// `strict` loads it.
#[tokio::test]
async fn test_ordered_table_without_an_order_column_index_draws_no_finding() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = ordered_in(
        "ord_noorderindex",
        Layout {
            order_statistics: Statistics::None,
            ..Layout::contract()
        },
    );
    let (fixture, served) = write_and_serve(&catalog, spec).await?;
    let generation = served.current();
    for file in &generation.files {
        let order = file.index.order_leaf().unwrap();
        let written = written_footer(&file.entry.path)?;
        let index = written.column_index();
        assert!(
            index.is_none_or(|groups| groups
                .iter()
                .all(|group| matches!(group[order], ColumnIndexMetaData::NONE))),
            "{} has no order column index",
            file.entry.path
        );
    }
    let report = diagnose(&generation, Level::Optimal, &ValueFormat::opaque, None);
    assert_codes(&report, &[], &[]);
    assert_eq!(report.summary.level, Level::Optimal);
    assert!(
        strict_refusal(&fixture, &catalog).await?.is_empty(),
        "strict loads the table"
    );
    Ok(())
}

/// The column index truncation of the truncated-order-index fixture:
/// inside the zeros every string order value of it starts with, so every
/// cut page minimum is the same.
const ORDER_INDEX_TRUNCATE: usize = 6;

/// Rows per page of the truncated-order-index fixture: thirty order pages
/// per file.
const ORDER_PAGE_ROWS: usize = 100;

/// A truncated order column index draws no finding, since the order
/// fences, not the writer's column index, place a bounded Scan's bounds;
/// the long key's index is never cut, so the key column draws none either,
/// and `strict` loads the table.
#[tokio::test]
async fn test_ordered_table_with_a_truncated_order_column_index_draws_no_finding() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        layout: Layout {
            page_rows: ORDER_PAGE_ROWS,
            column_index_truncate: Some(ORDER_INDEX_TRUNCATE),
            ..Layout::contract()
        },
        ..ordered_spec("ord_cutorderindex", OrderKind::Str)
    };
    let (fixture, served) = write_and_serve(&catalog, spec).await?;
    let generation = served.current();
    for file in &generation.files {
        let order = file.index.order_leaf().unwrap();
        let facts = file.index.order_facts().unwrap();
        let OrderValue::Str(least) = &facts.min else {
            panic!("a string order column's least value is a string");
        };
        assert!(least.len() > ORDER_INDEX_TRUNCATE, "{least:?}");
        let written = written_footer(&file.entry.path)?;
        let index = written
            .column_index()
            .expect("the fixture writes a column index");
        for group in index {
            let ColumnIndexMetaData::BYTE_ARRAY(entries) = &group[order] else {
                panic!("a string order column's index lists byte arrays");
            };
            assert!(
                entries
                    .min_values_iter()
                    .flatten()
                    .all(|min| min.len() <= ORDER_INDEX_TRUNCATE),
                "{}: every page minimum is cut",
                file.entry.path
            );
        }
    }
    let report = diagnose(&generation, Level::Optimal, &ValueFormat::opaque, None);
    assert_codes(&report, &[], &[]);
    for file in &report.files {
        assert!(
            file.distinct_page_mins_ratio >= DISTINCT_MINS_RATIO_ADVICE,
            "{}: the key column's index is whole",
            file.path
        );
    }
    assert!(
        strict_refusal(&fixture, &catalog).await?.is_empty(),
        "a table with no finding loads under strict"
    );
    Ok(())
}

/// The id of the long-run fixture whose run is one row past the advice.
const LONG_RUN_ID: u64 = 7;

/// One run of `RUN_ROWS_ADVICE` and one more rows; every other key's run is
/// one row.
fn one_long_run(id: u64) -> u64 {
    if id == LONG_RUN_ID {
        u64::from(RUN_ROWS_ADVICE) + 1
    } else {
        1
    }
}

/// A file holding a run of more than `RUN_ROWS_ADVICE` rows is advice: a
/// caller reads the key's history in more than one Scan at the default
/// `max_scan_rows`. Only the file holding the run draws it.
#[tokio::test]
async fn test_long_runs_is_advice_above_one_thousand_rows() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ordered: Some(Ordered {
            kind: OrderKind::Long,
            rows_per_key: one_long_run,
            start: ORDER_START,
            step: ORDER_STEP,
        }),
        ..ordered_spec("ord_longruns", OrderKind::Long)
    };
    let (fixture, served) = write_and_serve(&catalog, spec).await?;
    let generation = served.current();
    let report = diagnose(&generation, Level::Optimal, &ValueFormat::opaque, None);
    assert_codes(&report, &[], &[FindingCode::LongRuns]);
    let holding: Vec<&str> = generation
        .files
        .iter()
        .filter(|file| {
            file.index
                .order_facts()
                .is_some_and(|facts| facts.longest_run > u64::from(RUN_ROWS_ADVICE))
        })
        .map(|file| file.entry.path.as_str())
        .collect();
    let long = report
        .findings
        .iter()
        .filter(|finding| finding.code() == FindingCode::LongRuns)
        .map(|finding| finding.file().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(holding.len(), 1, "one file holds the long run");
    assert_eq!(long, holding);
    assert!(
        strict_refusal(&fixture, &catalog).await?.is_empty(),
        "an advice-only table loads under strict"
    );
    Ok(())
}

/// Bytes per value of the large-run fixture: `RUN_ROWS_ADVICE` rows of
/// them, incompressible, are a fifth over the default
/// `server.max_scan_bytes`.
const LARGE_RUN_VALUE_BYTES: usize = 5_000;

/// Rows per page of the large-run fixture: small enough that a value page
/// stays under the large-value-page advice, so the fixture raises the one
/// code it is designed for.
const LARGE_RUN_PAGE_ROWS: usize = 100;

/// One mebibyte, the lowered `server.max_scan_bytes` of the large-run cases.
const MIB: usize = 1024 * 1024;

/// A limit a tenth of the default `max_scan_rows`: its 100 rows of
/// `LARGE_RUN_VALUE_BYTES` come to about 0.5 MB, under every bound the
/// large-run cases use.
const SMALL_LIMIT: u32 = RUN_ROWS_ADVICE / 10;

/// A limit of 300 rows: about 1.5 MB of values, over the lowered 1 MiB
/// bound and under the default.
const MIDDLE_LIMIT: u32 = 3 * SMALL_LIMIT;

/// A file where `max_scan_rows` rows of the configured value columns, at
/// the footer's uncompressed bytes per row, would exceed
/// `server.max_scan_bytes` is advice: a Scan at the full limit is refused.
/// Raising the bound or lowering the limit past what the fixture's rows
/// come to clears it, and lowering the bound brings it back.
#[tokio::test]
async fn test_large_runs_is_advice_above_the_scan_byte_bound() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        value_bytes: LARGE_RUN_VALUE_BYTES,
        layout: Layout {
            page_rows: LARGE_RUN_PAGE_ROWS,
            ..Layout::contract()
        },
        ..ordered_spec("ord_largeruns", OrderKind::Long)
    };
    let (_fixture, served) = write_and_serve(&catalog, spec).await?;
    let generation = served.current();
    let default_bound = ServerConfig::default().max_scan_bytes;
    // Past the 5 MB the fixture's 1,000 rows of values come to.
    let raised_bound = 2 * default_bound;
    let cases = [
        (RUN_ROWS_ADVICE, default_bound, true),
        (RUN_ROWS_ADVICE, raised_bound, false),
        (SMALL_LIMIT, default_bound, false),
        (MIDDLE_LIMIT, MIB, true),
        (SMALL_LIMIT, MIB, false),
    ];
    for (max_scan_rows, max_scan_bytes, advised) in cases {
        let report = diagnose(
            &generation,
            Level::Optimal,
            &ValueFormat::opaque,
            Some(ScanLimits {
                max_scan_rows,
                max_scan_bytes,
            }),
        );
        let drawn = report
            .findings
            .iter()
            .filter(|finding| finding.code() == FindingCode::LargeRuns)
            .count();
        let case = format!("{max_scan_rows} rows, {max_scan_bytes} bytes");
        if advised {
            assert_codes(&report, &[], &[FindingCode::LargeRuns]);
            assert_eq!(drawn, report.files.len(), "every file draws it: {case}");
        } else {
            assert_codes(&report, &[], &[]);
            assert_eq!(drawn, 0, "{case}");
        }
    }
    let unknown = diagnose(&generation, Level::Optimal, &ValueFormat::opaque, None);
    assert_codes(&unknown, &[], &[]);
    Ok(())
}

/// The bloom findings judge an ordered table's filter by the distinct keys
/// of each row group, which the load counts: a filter the writer sized to
/// the keys holds a third of its bits per row at three rows per key, below
/// the minimum per row, and draws no advice; the report's per-row figure,
/// from the footer alone, shows the difference.
#[tokio::test]
async fn test_bloom_findings_divide_by_distinct_keys_for_an_ordered_table() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let (_fixture, served) =
        write_and_serve(&catalog, ordered_spec("ord_bloom", OrderKind::Long)).await?;
    let generation = served.current();
    let report = diagnose(&generation, Level::Optimal, &ValueFormat::opaque, None);
    assert_codes(&report, &[], &[]);
    for (file, loaded) in report.files.iter().zip(&generation.files) {
        let per_row = file.bloom_bits_per_key.unwrap();
        assert!(
            per_row < BLOOM_BITS_PER_KEY_MIN,
            "{}: {per_row} bits per row",
            file.path
        );
        let keys: u64 = loaded
            .index
            .order_facts()
            .unwrap()
            .distinct_keys
            .iter()
            .sum();
        let bits = file.bloom_bytes.unwrap() * u64::from(u8::BITS);
        assert!(
            approx::ratio(bits, keys) >= BLOOM_BITS_PER_KEY_MIN,
            "{}: {bits} bits over {keys} keys",
            file.path
        );
        assert!(
            approx::ratio(file.bloom_bytes.unwrap(), keys) <= BLOOM_BYTES_PER_KEY_ADVICE,
            "{}",
            file.path
        );
    }
    Ok(())
}

/// The strict gate tells a shared edge key from overlap before any chunk
/// is built (RFD 020, RFD 025): a strict Disk table of the size-rolled
/// fixture declared `optimal` is refused at `balanced` naming
/// `shared_edge_keys` and writes no chunk file, and the same table
/// declared `balanced` loads, which overlap, `permissive`, would refuse.
#[tokio::test]
async fn test_strict_gate_judges_shared_edges_before_any_chunk() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let fixture =
        write_fixture(catalog.as_ref(), NAMESPACE, rolled_spec("ord_rolled_gate")).await?;
    let (served, local, _scratch) =
        serve_on_disk(&fixture, &catalog, fixture.config(Residency::Disk, true)).await?;
    let Err(TestkitError::Core(Error::Refused {
        reason:
            RefusalReason::BelowLevel {
                declared,
                achieved,
                findings,
                ..
            },
        ..
    })) = served
    else {
        panic!("expected a below_level refusal");
    };
    assert_eq!((declared, achieved), (Level::Optimal, Level::Balanced));
    assert_eq!(
        findings,
        [Capping {
            code: FindingCode::SharedEdgeKeys,
            column: None,
        }]
    );
    assert_eq!(local.index_bytes_in_use()?, 0, "no chunk file was written");

    let balanced = TableConfig {
        lookups: Some(Level::Balanced),
        ..fixture.config(Residency::Disk, true)
    };
    let (served, _local, _scratch) = serve_on_disk(&fixture, &catalog, balanced).await?;
    let served = served?;
    assert_two_files_share_an_edge_key(&served);
    assert_eq!(served.current().level, Level::Balanced);
    Ok(())
}
