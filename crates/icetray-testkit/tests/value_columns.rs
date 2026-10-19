//! A table serving several value columns (RFD 022), in every residency
//! tier: each column answers its own bytes, a lookup naming several gets
//! them in the order it named them, a cold hit costs one value-page GET per
//! column it names and a presence-only lookup none, one `MultiGet` across
//! the columns answers from one generation, and the doctor reports each
//! column of each file.

use std::path::Path;

use icetray_core::ColumnRead;
use icetray_core::config::{Residency, ValueFormat};
use icetray_core::{KeyType, Level, Row, diagnose};
use icetray_testkit::fixture::{NAMESPACE, SAMPLE, SECOND_VALUE_COLUMN, local_catalog};
use icetray_testkit::layout::VALUE_COLUMN;
use icetray_testkit::verify::{self, ColdCosts};
use icetray_testkit::{Fixture, FixtureSpec, Result, write_fixture};

/// Both value columns of the fixture, in one order a lookup names them.
const BOTH: [&str; 2] = [VALUE_COLUMN, SECOND_VALUE_COLUMN];

/// The same columns in the other order.
const BOTH_REVERSED: [&str; 2] = [SECOND_VALUE_COLUMN, VALUE_COLUMN];

/// A small long-keyed fixture with both value columns.
async fn two_column_fixture(dir: &Path, name: &str) -> Result<Fixture> {
    let catalog = icetray_testkit::memory_catalog_local(dir).await?;
    let spec = FixtureSpec {
        second_value: true,
        ..FixtureSpec::small(name, KeyType::Long)
    };
    write_fixture(catalog.as_ref(), NAMESPACE, spec).await
}

/// The fixture's sampled keys, checked to hold different bytes in its two
/// columns, so that no test of order or of one column passes with the
/// other column's bytes.
fn sampled(fixture: &Fixture) -> Vec<icetray_core::Key> {
    let keys = fixture.keys_at(&SAMPLE);
    for key in &keys {
        assert_ne!(
            fixture.value(key),
            fixture.second_value(key),
            "{key}: the columns differ"
        );
        assert_ne!(
            fixture.expected_of(key, &BOTH),
            fixture.expected_of(key, &BOTH_REVERSED),
            "{key}: the two orders differ"
        );
    }
    keys
}

/// What a cold pass over `keys` costs naming `VALUE_COLUMN` alone and
/// naming both columns, whose pages are pages of their own.
async fn cold_costs(
    fixture: &Fixture,
    keys: &[icetray_core::Key],
) -> Result<(ColdCosts, ColdCosts)> {
    let config = fixture.config(Residency::Full, false);
    let one_column = ColdCosts::of(config.clone(), keys, &[VALUE_COLUMN]).await?;
    let both = ColdCosts::of(config, keys, &BOTH).await?;
    assert_eq!(
        both.value_gets,
        2 * one_column.value_gets,
        "the two columns' pages are pages of their own"
    );
    Ok((one_column, both))
}

#[tokio::test]
async fn test_both_columns_answer_in_request_order_at_one_get_per_column_cold() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let fixture = two_column_fixture(dir.path(), "kv_columns").await?;
    let keys = sampled(&fixture);
    let (_, both) = cold_costs(&fixture, &keys).await?;
    for residency in Residency::ALL {
        let scratch = tempfile::tempdir()?;
        let served = verify::serve_in(fixture.config(residency, false), scratch.path()).await?;
        let stats = served.storage.stats();
        let before = stats.gets();
        for key in &keys {
            assert_eq!(
                served.table.get(key, &verify::whole(&BOTH)).await?.row,
                fixture.expected_of(key, &BOTH),
                "{residency} {key}"
            );
        }
        assert_eq!(
            stats.gets() - before,
            both.gets_in(residency),
            "{residency}: one value-page GET per column named, cold"
        );
    }
    Ok(())
}

#[tokio::test]
async fn test_the_other_order_answers_the_other_order_and_warm_pages_cost_nothing() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let fixture = two_column_fixture(dir.path(), "kv_columns_order").await?;
    let keys = sampled(&fixture);
    for residency in Residency::ALL {
        let scratch = tempfile::tempdir()?;
        let served = verify::serve_in(fixture.config(residency, false), scratch.path()).await?;
        for key in &keys {
            served.table.get(key, &verify::whole(&BOTH)).await?;
        }
        let stats = served.storage.stats();
        let before = stats.gets();
        for key in &keys {
            assert_eq!(
                served
                    .table
                    .get(key, &verify::whole(&BOTH_REVERSED))
                    .await?
                    .row,
                fixture.expected_of(key, &BOTH_REVERSED),
                "{residency} {key}"
            );
            assert_eq!(
                served
                    .table
                    .get(key, &[ColumnRead::whole(SECOND_VALUE_COLUMN)])
                    .await?
                    .row,
                fixture.expected_of(key, &[SECOND_VALUE_COLUMN]),
                "{residency} {key}"
            );
        }
        assert_eq!(
            stats.gets() - before,
            0,
            "{residency}: warm pages, key and value, cost nothing"
        );
    }
    Ok(())
}

#[tokio::test]
async fn test_presence_alone_reads_no_value_page_on_get_and_multi_get() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let fixture = two_column_fixture(dir.path(), "kv_columns_presence").await?;
    let keys = sampled(&fixture);
    let (one_column, _) = cold_costs(&fixture, &keys).await?;
    let mut batch = keys.clone();
    batch.push(fixture.absent_near(SAMPLE[0]));
    for residency in Residency::ALL {
        let scratch = tempfile::tempdir()?;
        let served = verify::serve_in(fixture.config(residency, false), scratch.path()).await?;
        let stats = served.storage.stats();
        let before = stats.gets();
        for key in &keys {
            assert_eq!(
                served.table.get(key, &[]).await?.row,
                Row::Present(Vec::new()),
                "{residency} {key}"
            );
        }
        let presence = served.table.multi_get(&batch, &[]).await?;
        let expected: Vec<Row> = keys
            .iter()
            .map(|_| Row::Present(Vec::new()))
            .chain([Row::Absent])
            .collect();
        assert_eq!(presence.rows, expected, "{residency}");
        assert_eq!(
            stats.gets() - before,
            one_column.key_page_gets(residency),
            "{residency}: presence alone reads no value page, the key pages once"
        );
    }
    Ok(())
}

#[tokio::test]
async fn test_one_column_named_reads_that_columns_pages_alone() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let fixture = two_column_fixture(dir.path(), "kv_columns_one").await?;
    let keys = sampled(&fixture);
    let (one_column, _) = cold_costs(&fixture, &keys).await?;
    for residency in Residency::ALL {
        let scratch = tempfile::tempdir()?;
        let served = verify::serve_in(fixture.config(residency, false), scratch.path()).await?;
        let stats = served.storage.stats();
        let before = stats.gets();
        for key in &keys {
            assert_eq!(
                served
                    .table
                    .get(key, &[ColumnRead::whole(VALUE_COLUMN)])
                    .await?
                    .row,
                fixture.expected_of(key, &[VALUE_COLUMN]),
                "{residency} {key}"
            );
        }
        assert_eq!(
            stats.gets() - before,
            one_column.gets_in(residency),
            "{residency}: one column named, one column's pages"
        );
    }
    Ok(())
}

#[tokio::test]
async fn test_multi_get_across_columns_answers_from_one_generation() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let fixture = two_column_fixture(dir.path(), "kv_columns_batch").await?;
    let scratch = tempfile::tempdir()?;
    let served = verify::serve_in(fixture.config(Residency::Full, false), scratch.path()).await?;
    let mut keys = sampled(&fixture);
    keys.push(fixture.absent_near(SAMPLE[0]));
    let expected_gets = verify::expected_cold_gets(&served.current(), &keys, &BOTH_REVERSED)?;
    let stats = served.storage.stats();
    let before = stats.gets();
    let batch = served
        .table
        .multi_get(&keys, &verify::whole(&BOTH_REVERSED))
        .await?;
    assert_eq!(stats.gets() - before, expected_gets);
    assert_eq!(batch.snapshot_id, served.current().snapshot_id);
    let expected: Vec<Row> = keys[..SAMPLE.len()]
        .iter()
        .map(|key| fixture.expected_of(key, &BOTH_REVERSED))
        .chain([Row::Absent])
        .collect();
    assert_eq!(batch.rows, expected);
    Ok(())
}

#[tokio::test]
async fn test_doctor_reports_each_column_of_each_file() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        second_value: true,
        ..FixtureSpec::small("kv_columns_doctor", KeyType::Long)
    };
    let fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    let scratch = tempfile::tempdir()?;
    let served = verify::serve_in(fixture.config(Residency::Full, false), scratch.path()).await?;
    let generation = served.current();
    let report = diagnose(&generation, Level::Optimal, &ValueFormat::opaque, None);
    let names: Vec<&str> = report
        .summary
        .value_columns
        .iter()
        .map(|column| column.column.as_str())
        .collect();
    assert_eq!(names, [SECOND_VALUE_COLUMN, VALUE_COLUMN], "by name");
    for column in &report.summary.value_columns {
        assert_eq!(column.level, Level::Optimal, "{}", column.column);
        assert_eq!(column.gets_per_hit, 1, "{}", column.column);
        assert_eq!(column.files_predating_column, 0, "{}", column.column);
    }
    assert_eq!(report.files.len(), fixture.file_count());
    for file in &report.files {
        let columns: Vec<&str> = file
            .value_columns
            .iter()
            .map(|column| column.column.as_str())
            .collect();
        assert_eq!(
            columns,
            [SECOND_VALUE_COLUMN, VALUE_COLUMN],
            "{}",
            file.path
        );
        for column in &file.value_columns {
            assert!(!column.predates_column, "{} {}", file.path, column.column);
            assert!(column.value_pages > 0, "{} {}", file.path, column.column);
            assert!(column.pages_aligned, "{} {}", file.path, column.column);
        }
    }
    Ok(())
}
