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
use icetray_core::approx;
use icetray_core::check::thresholds::{
    BLOOM_BITS_PER_KEY_MIN, BLOOM_BYTES_PER_KEY_ADVICE, DISTINCT_MINS_RATIO_ADVICE,
    FILE_BYTES_ADVICE, FILES_PER_BUCKET_FACTOR, KEY_PAGE_ROWS_LARGE, MOST_VALUE_PAGE_BYTES,
};
use icetray_core::config::Residency;
use icetray_core::{
    Error, FileFault, Finding, FindingCode, HeaderField, KeyType, Level, RefusalReason,
    TableReport, Value, WriterProperty, diagnose,
};
use icetray_testkit::fixture::{
    FOUR_BUCKETS, IDS_2K, IDS_4K, LONG_KEY_BYTES, LONG_KEY_PAD, NAMESPACE, ONE_BUCKET, SAMPLE,
    SECOND_CLASS, SMALL_FILE_BYTES, TWO_BUCKETS, TWO_FILES, UNPARTITIONED_FILE_BYTES,
    WIDE_VALUE_BYTES, local_catalog, residue_class,
};
use icetray_testkit::layout::{OVERSIZED_FPP, UNDERSIZED_FPP};
use icetray_testkit::verify::{self, LOAD_METADATA_GETS, Served};
use icetray_testkit::{
    Bloom, Dictionary, Fixture, FixtureSpec, Layout, Result, Statistics, TestkitError,
    write_fixture,
};

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
    report.capping_codes().into_iter().collect()
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
    Ok(findings)
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
    let report = diagnose(&served.current(), Level::Optimal);
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
    let report = diagnose(&generation, Level::Optimal);
    assert_codes(&report, &[FindingCode::DictionaryEncodedValue], &[]);
    assert_eq!(
        finding(&report, FindingCode::DictionaryEncodedValue)
            .penalty()
            .and_then(|penalty| penalty.extra_gets_per_hit),
        Some(1)
    );

    let keys = fixture.keys_at(&SAMPLE);
    let value_pages = verify::expected_cold_gets(&generation, &keys)?;
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
    let report = diagnose(&served.current(), Level::Optimal);
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
    let report = diagnose(&generation, Level::Optimal);
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
    let report = diagnose(&served.current(), Level::Optimal);
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
    let report = diagnose(&generation, Level::Optimal);
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
    let report = diagnose(&served.current(), Level::Optimal);
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
    let report = diagnose(&served.current(), Level::Optimal);
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
    let report = diagnose(&served.current(), Level::Optimal);
    assert_codes(
        &report,
        &[],
        &[
            FindingCode::LargeValuePages,
            FindingCode::ValuePagesOverCeiling,
        ],
    );
    assert_eq!(report.files.len(), 1);
    assert_eq!(report.files[0].value_pages, 1);
    assert!(report.files[0].avg_value_page_bytes > MOST_VALUE_PAGE_BYTES);
    assert_eq!(
        finding(&report, FindingCode::ValuePagesOverCeiling).property(),
        Some(WriterProperty::PageSizeBytes)
    );
    let error = served
        .table
        .get(&fixture.key(0))
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
    let report = diagnose(&served.current(), Level::Optimal);
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
    let report = diagnose(&generation, Level::Optimal);
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
    let report = diagnose(&served.current(), Level::Optimal);
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
    let report = diagnose(&served.current(), Level::Optimal);
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
    let report = diagnose(&served.current(), Level::Optimal);
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
    let report = diagnose(&served.current(), Level::Optimal);
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
        LOAD_METADATA_GETS + files + row_groups,
        "a footer per file and a chunk per row group"
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
        assert!(
            matches!(served.table.get(&key).await?.value, Value::Present(_)),
            "key {key}"
        );
    }
    let stats = served.storage.stats();
    let before = stats.gets();
    let inside = fixture.absent_near(INSIDE_ID);
    assert_eq!(served.table.get(&inside).await?.value, Value::Absent);
    assert_eq!(
        stats.gets(),
        before,
        "no false miss and no GET without statistics"
    );
    Ok(())
}
