//! A scale probe, a program and not a test: many files per generation, with
//! timings to stderr and only the checks that the run did its work. It
//! writes about 150 MB under a temporary directory and takes minutes; run
//! with `cargo run -p icetray-testkit --example scale --release` and read
//! the timings. It lives beside the tests because it uses their fixtures,
//! and outside them because it measures rather than proves.

use std::time::Instant;

use icetray_core::ColumnRead;
use icetray_core::approx;
use icetray_core::config::Residency;
use icetray_core::{KeyType, Row};
use icetray_testkit::fixture::{NAMESPACE, local_catalog};
use icetray_testkit::layout::VALUE_COLUMN;
use icetray_testkit::verify;
use icetray_testkit::{FixtureSpec, Result, write_fixture};

/// Files per generation, one per bucket: the file count a three-billion-row
/// table would have at two million rows per file.
const FILES: u32 = 1_500;

/// Rows per file: one contract page, so the write finishes in seconds and
/// the file count alone sets the scale.
const ROWS_PER_FILE: u64 = 1_000;

/// Rows of the probe's first build: one page in every file.
const ROWS: u64 = FILES as u64 * ROWS_PER_FILE;

/// Lookups per timed batch, and the stride that spreads them over the table.
const LOOKUPS: u32 = 2_000;
const LOOKUP_STRIDE: u64 = 733;

/// Rows the append after the first build adds, across every bucket.
const APPENDED_ROWS: u64 = 150_000;

/// Bytes per mebibyte, for the printed sizes.
const MEBIBYTE: f64 = 1_048_576.0;

fn mib(bytes: u64) -> f64 {
    approx::gauge(bytes) / MEBIBYTE
}

#[tokio::main]
async fn main() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        buckets: FILES,
        ids: 0..ROWS,
        ..FixtureSpec::small("kv_many", KeyType::Long)
    };
    let started = Instant::now();
    let mut fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    eprintln!(
        "write {ROWS} rows into {FILES} files: {:.1?}",
        started.elapsed()
    );

    let started = Instant::now();
    let served = verify::serve_catalog(&fixture, &catalog, Residency::Full, false).await?;
    let load = started.elapsed();
    let generation = served.current();
    let stats = served.storage.stats();
    eprintln!(
        "load: {load:.1?}; files {}, index {:.1} MiB, GETs {}, bytes {:.1} MiB",
        generation.files.len(),
        mib(approx::count(generation.index_bytes)),
        stats.gets(),
        mib(stats.bytes())
    );
    assert_eq!(
        approx::count(generation.files.len()),
        u64::from(FILES),
        "one file per bucket"
    );

    let started = Instant::now();
    for i in 0..u64::from(LOOKUPS) {
        let key = fixture.key(i * LOOKUP_STRIDE % ROWS);
        assert!(
            verify::one_value(
                &served
                    .table
                    .get(&key, &[ColumnRead::whole(VALUE_COLUMN)])
                    .await?
                    .row
            )
            .is_some(),
            "a written key answers present"
        );
    }
    let elapsed = started.elapsed();
    eprintln!(
        "{LOOKUPS} cold-ish lookups: {elapsed:.1?} ({:.0?} each)",
        elapsed / LOOKUPS
    );
    let started = Instant::now();
    for i in 0..u64::from(LOOKUPS) {
        let key = fixture.absent_near(i * LOOKUP_STRIDE % ROWS);
        assert_eq!(
            served
                .table
                .get(&key, &[ColumnRead::whole(VALUE_COLUMN)])
                .await?
                .row,
            Row::Absent,
            "an unwritten key answers absent"
        );
    }
    let elapsed = started.elapsed();
    eprintln!(
        "{LOOKUPS} misses: {elapsed:.1?} ({:.0?} each)",
        elapsed / LOOKUPS
    );

    let started = Instant::now();
    fixture
        .append(catalog.as_ref(), ROWS..ROWS + APPENDED_ROWS, "build2")
        .await?;
    eprintln!(
        "append {APPENDED_ROWS} rows across every bucket: {:.1?}",
        started.elapsed()
    );
    let started = Instant::now();
    assert!(
        served.table.refresh().await?,
        "the append is a new snapshot"
    );
    let generation = served.current();
    eprintln!(
        "refresh: {:.1?}; files {}, index {:.1} MiB",
        started.elapsed(),
        generation.files.len(),
        mib(approx::count(generation.index_bytes))
    );
    Ok(())
}
