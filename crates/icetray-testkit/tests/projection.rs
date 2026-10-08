//! Field projection over a served table (RFD 024): a binary column declared
//! protobuf beside an opaque one, read whole and trimmed to field numbers
//! in every residency tier at the GETs a whole read costs; a null, a value
//! holding none of the fields, a field list the column's format does not
//! take, a malformed value on `get` and `multi_get`, a format applied in
//! place by a reload, and the doctor's walk of one value page per protobuf
//! column.

use std::path::Path;
use std::sync::Arc;

use icetray_core::config::{
    Config, Residency, ServerConfig, TableConfig, ValueColumnConfig, ValueFormat,
};
use icetray_core::open::open_with;
use icetray_core::{
    ColumnRead, ColumnRequestFault, Error, FindingCode, FindingLog, Key, KeyType, Level, Row,
    Storage, examine,
};
use icetray_testkit::fixture::{NAMESPACE, SAMPLE, SECOND_VALUE_COLUMN};
use icetray_testkit::layout::VALUE_COLUMN;
use icetray_testkit::protobuf::{
    ABSENT_FIELD, CHECKSUM_FIELD, FIELDS, GROUP_FIELD, ID_FIELD, NAME_FIELD, SCORES_FIELD,
    malformed_key,
};
use icetray_testkit::verify::{self, Served};
use icetray_testkit::{
    Fixture, FixtureSpec, Nulls, Result, ValueKind, memory_catalog_local, write_fixture,
};

/// Every residency tier a table can be served in.
const RESIDENCIES: [Residency; 3] = [Residency::Full, Residency::Disk, Residency::Bloom];

/// The field lists the tests trim `VALUE_COLUMN` to: a singular field
/// written twice, a packed field split in two records, two adjacent
/// fields, a group, every field, and a number no value holds.
const FIELD_LISTS: [&[u32]; 6] = [
    &[ID_FIELD],
    &[SCORES_FIELD],
    &[CHECKSUM_FIELD, NAME_FIELD],
    &[GROUP_FIELD, ID_FIELD],
    &FIELDS,
    &[ABSENT_FIELD],
];

/// A small long-keyed fixture of both value columns, `VALUE_COLUMN`
/// holding values of `kind`, every `nulls` id null.
async fn fixture_of(dir: &Path, name: &str, kind: ValueKind, nulls: Nulls) -> Result<Fixture> {
    let catalog = memory_catalog_local(dir).await?;
    let spec = FixtureSpec {
        second_value: true,
        value_kind: kind,
        nulls,
        ..FixtureSpec::small(name, KeyType::Long)
    };
    write_fixture(catalog.as_ref(), NAMESPACE, spec).await
}

/// The fixture's entry in `residency` serving `VALUE_COLUMN` declared
/// `format` and `SECOND_VALUE_COLUMN` opaque.
fn entry(fixture: &Fixture, residency: Residency, format: ValueFormat) -> TableConfig {
    TableConfig {
        value_columns: vec![
            ValueColumnConfig::of(VALUE_COLUMN, format),
            ValueColumnConfig::named(SECOND_VALUE_COLUMN),
        ],
        ..fixture.config(residency, false)
    }
}

/// `config` served over a fresh storage and cache, a local directory under
/// `scratch` in the Disk tier.
async fn serve(fixture: &Fixture, config: TableConfig, scratch: &Path) -> Result<Served> {
    let source = icetray_core::TableSource::from_config(&fixture.pinned_source()).await?;
    verify::build_in(config, source, scratch, verify::TEST_PAGE_CACHE_BYTES)
        .await?
        .load()
        .await
}

/// The reads of a lookup trimming `VALUE_COLUMN` to `fields` and reading
/// `SECOND_VALUE_COLUMN` whole.
fn reads(fields: &[u32]) -> [ColumnRead<'_>; 2] {
    [
        ColumnRead {
            name: VALUE_COLUMN,
            fields,
        },
        ColumnRead::whole(SECOND_VALUE_COLUMN),
    ]
}

/// The value-page GETs a cold pass over `keys` naming both columns costs,
/// from the Full tier's oracle, which knows nothing of projection, and the
/// key pages a Bloom-tier pass adds.
async fn cold_costs(fixture: &Fixture, keys: &[Key]) -> Result<(u64, u64)> {
    let scratch = tempfile::tempdir()?;
    let full = serve(
        fixture,
        entry(fixture, Residency::Full, ValueFormat::Protobuf),
        scratch.path(),
    )
    .await?;
    let oracle = full.current();
    let values = verify::expected_cold_gets(&oracle, keys, &[VALUE_COLUMN, SECOND_VALUE_COLUMN])?;
    assert!(values > 0);
    Ok((values, verify::expected_key_pages(&oracle, keys)))
}

#[tokio::test]
async fn test_projected_reads_answer_the_named_fields_at_a_whole_read_s_gets() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let fixture = fixture_of(dir.path(), "kv_project", ValueKind::Protobuf, Nulls::None).await?;
    let keys = fixture.keys_at(&SAMPLE);
    let (value_gets, key_pages) = cold_costs(&fixture, &keys).await?;
    for residency in RESIDENCIES {
        for fields in FIELD_LISTS {
            let scratch = tempfile::tempdir()?;
            let config = entry(&fixture, residency, ValueFormat::Protobuf);
            let served = serve(&fixture, config, scratch.path()).await?;
            let stats = served.storage.stats();
            let before = stats.gets();
            for key in &keys {
                let row = served.table.get(key, &reads(fields)).await?.row;
                assert_eq!(
                    row,
                    fixture.expected_reads(key, &reads(fields)),
                    "{residency} {fields:?} {key}"
                );
                if fields == FIELDS {
                    assert_eq!(
                        row,
                        fixture.expected_of(key, &[VALUE_COLUMN, SECOND_VALUE_COLUMN])
                    );
                }
            }
            let key_page_gets = match residency {
                Residency::Full | Residency::Disk => 0,
                Residency::Bloom => key_pages,
            };
            assert_eq!(
                stats.gets() - before,
                value_gets + key_page_gets,
                "{residency} {fields:?}: a projected cold pass costs a whole one's GETs"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_projected_multi_get_answers_every_key_from_one_generation() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let fixture = fixture_of(
        dir.path(),
        "kv_project_batch",
        ValueKind::Protobuf,
        Nulls::None,
    )
    .await?;
    let present = fixture.keys_at(&SAMPLE);
    let (value_gets, key_pages) = cold_costs(&fixture, &present).await?;
    let mut keys = present.clone();
    keys.push(fixture.absent_near(SAMPLE[0]));
    let fields = [SCORES_FIELD, GROUP_FIELD];
    for residency in RESIDENCIES {
        let scratch = tempfile::tempdir()?;
        let config = entry(&fixture, residency, ValueFormat::Protobuf);
        let served = serve(&fixture, config, scratch.path()).await?;
        let stats = served.storage.stats();
        let before = stats.gets();
        let batch = served.table.multi_get(&present, &reads(&fields)).await?;
        let expected: Vec<Row> = present
            .iter()
            .map(|key| fixture.expected_reads(key, &reads(&fields)))
            .collect();
        assert_eq!(batch.rows, expected, "{residency}");
        assert_eq!(batch.snapshot_id, served.current().snapshot_id);
        let key_page_gets = match residency {
            Residency::Full | Residency::Disk => 0,
            Residency::Bloom => key_pages,
        };
        assert_eq!(
            stats.gets() - before,
            value_gets + key_page_gets,
            "{residency}: a projected batch costs a whole one's GETs"
        );
        let with_absent = served.table.multi_get(&keys, &reads(&fields)).await?;
        assert_eq!(
            with_absent.rows[..present.len()],
            expected[..],
            "{residency}"
        );
        assert_eq!(with_absent.rows[present.len()], Row::Absent, "{residency}");
    }
    Ok(())
}

#[tokio::test]
async fn test_a_null_stays_null_and_a_value_of_none_of_the_fields_is_empty() -> Result<()> {
    let every_third = 3;
    let dir = tempfile::tempdir()?;
    let fixture = fixture_of(
        dir.path(),
        "kv_project_nulls",
        ValueKind::Protobuf,
        Nulls::EveryNth(every_third),
    )
    .await?;
    let scratch = tempfile::tempdir()?;
    let config = entry(&fixture, Residency::Full, ValueFormat::Protobuf);
    let served = serve(&fixture, config, scratch.path()).await?;
    let null = fixture.key(0);
    let present = fixture.key(1);
    assert_eq!(fixture.value(&null), None);
    let none_of_them = [ABSENT_FIELD];
    let answered = |row: Row| match row {
        Row::Present(values) => values[0].clone(),
        Row::Absent => panic!("the key is present"),
    };
    assert_eq!(
        answered(served.table.get(&null, &reads(&none_of_them)).await?.row),
        None,
        "a null stays null"
    );
    assert_eq!(
        answered(served.table.get(&present, &reads(&none_of_them)).await?.row),
        Some(bytes::Bytes::new()),
        "a value holding none of the fields is present with zero bytes"
    );
    Ok(())
}

#[tokio::test]
async fn test_a_field_list_the_format_does_not_take_is_refused_before_any_read() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let fixture = fixture_of(
        dir.path(),
        "kv_project_bad",
        ValueKind::Protobuf,
        Nulls::None,
    )
    .await?;
    let scratch = tempfile::tempdir()?;
    let config = entry(&fixture, Residency::Full, ValueFormat::Protobuf);
    let served = serve(&fixture, config, scratch.path()).await?;
    let key = fixture.key(SAMPLE[1]);
    let one = [ID_FIELD];
    let cases = [
        (
            ColumnRead {
                name: SECOND_VALUE_COLUMN,
                fields: &one,
            },
            SECOND_VALUE_COLUMN,
            ColumnRequestFault::NotProtobuf,
        ),
        (
            ColumnRead {
                name: VALUE_COLUMN,
                fields: &[0],
            },
            VALUE_COLUMN,
            ColumnRequestFault::FieldOutOfRange { field: 0 },
        ),
        (
            ColumnRead {
                name: VALUE_COLUMN,
                fields: &[icetray_core::MAX_FIELD_NUMBER + 1],
            },
            VALUE_COLUMN,
            ColumnRequestFault::FieldOutOfRange {
                field: icetray_core::MAX_FIELD_NUMBER + 1,
            },
        ),
        (
            ColumnRead {
                name: VALUE_COLUMN,
                fields: &[ID_FIELD, NAME_FIELD, ID_FIELD],
            },
            VALUE_COLUMN,
            ColumnRequestFault::RepeatedField { field: ID_FIELD },
        ),
    ];
    let stats = served.storage.stats();
    let before = stats.gets();
    for (read, column, fault) in cases {
        let refused = [
            served.table.get(&key, &[read]).await.unwrap_err(),
            served
                .table
                .multi_get(std::slice::from_ref(&key), &[read])
                .await
                .unwrap_err(),
        ];
        for error in refused {
            let Error::ColumnRequest {
                column: named,
                fault: got,
                ..
            } = error
            else {
                panic!("expected a column request fault, got {error:?}");
            };
            assert_eq!((named.as_str(), got), (column, fault));
        }
    }
    assert_eq!(stats.gets(), before, "a refused request reads nothing");
    Ok(())
}

#[tokio::test]
async fn test_a_malformed_value_fails_get_and_its_multi_get_naming_the_key() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let fixture = fixture_of(
        dir.path(),
        "kv_project_malformed",
        ValueKind::Random,
        Nulls::None,
    )
    .await?;
    let scratch = tempfile::tempdir()?;
    let config = entry(&fixture, Residency::Full, ValueFormat::Protobuf);
    let served = serve(&fixture, config, scratch.path()).await?;
    let (malformed, expected) =
        malformed_key(&fixture).expect("some random value starts with an undefined wire type");
    let fields = [ID_FIELD];
    let check = |error: Error| {
        let Error::MalformedValue {
            table,
            column,
            key,
            fault,
        } = error
        else {
            panic!("expected a malformed value, got {error:?}");
        };
        assert_eq!(
            (table.as_str(), column.as_str(), key, fault),
            (
                served.table.name(),
                VALUE_COLUMN,
                malformed.clone(),
                expected
            )
        );
    };
    check(
        served
            .table
            .get(&malformed, &reads(&fields))
            .await
            .unwrap_err(),
    );
    let batch = [fixture.absent_near(SAMPLE[0]), malformed.clone()];
    check(
        served
            .table
            .multi_get(&batch, &reads(&fields))
            .await
            .unwrap_err(),
    );
    assert_eq!(
        served.table.get(&malformed, &reads(&[])).await?.row,
        fixture.expected_of(&malformed, &[VALUE_COLUMN, SECOND_VALUE_COLUMN]),
        "a whole read returns the bytes unchanged"
    );
    Ok(())
}

#[tokio::test]
async fn test_a_reload_that_declares_a_format_applies_in_place() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let fixture = fixture_of(
        dir.path(),
        "kv_project_retune",
        ValueKind::Protobuf,
        Nulls::None,
    )
    .await?;
    let config = |format| Config {
        server: ServerConfig::default(),
        storage: verify::storage_config_for_tests(),
        tables: vec![entry(&fixture, Residency::Full, format)],
    };
    let opaque = config(ValueFormat::Opaque);
    let storage = Arc::new(Storage::new(opaque.storage.clone()));
    let opened = open_with(&opaque, None, storage, FindingLog::Events).await?;
    let table = Arc::clone(&opened.tables()[0]);
    table.load().await?;
    let serving = table.current().expect("loaded");
    let key = fixture.key(SAMPLE[2]);
    let fields = [NAME_FIELD];
    assert!(matches!(
        table.get(&key, &reads(&fields)).await,
        Err(Error::ColumnRequest {
            fault: ColumnRequestFault::NotProtobuf,
            ..
        })
    ));

    let mut retuned = config(ValueFormat::Protobuf);
    retuned.tables[0].lookups = Some(Level::Balanced);
    retuned.tables[0].strict = true;
    let applied = opened
        .apply(&retuned.tables)
        .await
        .expect("a format with lookups and strict applies");
    assert_eq!(
        (applied.retuned.len(), applied.started.len()),
        (1, 0),
        "retuned, not reloaded"
    );
    let after = Arc::clone(&opened.tables()[0]);
    assert!(Arc::ptr_eq(&after, &table), "the same table");
    assert!(
        Arc::ptr_eq(&after.current().expect("serving"), &serving),
        "the same generation"
    );
    assert_eq!(
        after.config().lookups(),
        Level::Balanced,
        "the entry is the new one"
    );
    assert_eq!(
        after.get(&key, &reads(&fields)).await?.row,
        fixture.expected_reads(&key, &reads(&fields)),
        "the field list is taken at once"
    );

    opened
        .apply(&opaque.tables)
        .await
        .expect("a format removed applies");
    assert!(matches!(
        after.get(&key, &reads(&fields)).await,
        Err(Error::ColumnRequest {
            fault: ColumnRequestFault::NotProtobuf,
            ..
        })
    ));
    Ok(())
}

/// One null in this many ids, in the doctor's fixtures with nulls: the
/// walk checks the page's other values.
const DOCTOR_NULL_EVERY: u64 = 3;

#[tokio::test]
async fn test_doctor_walks_one_value_page_per_protobuf_column() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let cases = [
        (ValueKind::Protobuf, Nulls::None, false),
        (ValueKind::Random, Nulls::None, true),
        (
            ValueKind::Protobuf,
            Nulls::EveryNth(DOCTOR_NULL_EVERY),
            false,
        ),
        (ValueKind::Random, Nulls::EveryNth(DOCTOR_NULL_EVERY), true),
    ];
    for (kind, nulls, malformed) in cases {
        let fixture = fixture_of(
            dir.path(),
            &format!("kv_doctor_{kind:?}_{}", matches!(nulls, Nulls::None)),
            kind,
            nulls,
        )
        .await?;
        for format in [ValueFormat::Opaque, ValueFormat::Protobuf] {
            let scratch = tempfile::tempdir()?;
            let config = entry(&fixture, Residency::Full, format);
            let served = serve(&fixture, config.clone(), scratch.path()).await?;
            let generation = served.current();
            let stats = served.storage.stats();
            let before = stats.gets();
            let examined = examine(&generation, Level::Optimal, &config.formats()).await;
            assert!(examined.unwalked.is_empty(), "{:?}", examined.unwalked);
            let report = examined.report;
            let walked = format == ValueFormat::Protobuf;
            assert_eq!(
                stats.gets() - before,
                u64::from(walked),
                "{kind:?} {format}: one GET per protobuf column"
            );
            let found: Vec<_> = report
                .findings
                .iter()
                .filter(|finding| finding.code() == FindingCode::MalformedProtobuf)
                .collect();
            if walked && malformed {
                let [finding] = found[..] else {
                    panic!("{kind:?} {format}: one finding, got {found:?}");
                };
                assert_eq!(finding.column(), Some(VALUE_COLUMN));
                assert_eq!(finding.caps(), None, "advice");
                assert_eq!(
                    finding.file(),
                    Some(generation.files[0].entry.path.as_str())
                );
            } else {
                assert!(found.is_empty(), "{kind:?} {format}: {found:?}");
            }
            assert_eq!(
                report.summary.level, generation.level,
                "advice caps nothing"
            );
            assert_eq!(
                report.summary.advice + report.summary.capping,
                report.findings.len()
            );
            let formats: Vec<(&str, ValueFormat)> = report
                .summary
                .value_columns
                .iter()
                .map(|column| (column.column.as_str(), column.format))
                .collect();
            assert_eq!(
                formats,
                [
                    (SECOND_VALUE_COLUMN, ValueFormat::Opaque),
                    (VALUE_COLUMN, format)
                ]
            );
        }
    }
    Ok(())
}

/// Removes every data file under `dir`, the fixture's warehouse, and
/// returns how many it removed.
fn remove_data_files(dir: &Path) -> Result<usize> {
    let mut removed = 0;
    let mut pending = vec![dir.to_path_buf()];
    while let Some(at) = pending.pop() {
        for entry in std::fs::read_dir(&at)? {
            let path = entry?.path();
            if path.is_dir() {
                pending.push(path);
            } else if path
                .extension()
                .is_some_and(|extension| extension == "parquet")
            {
                std::fs::remove_file(&path)?;
                removed += 1;
            }
        }
    }
    Ok(removed)
}

/// A walk whose page read fails leaves its column unwalked and the rest of
/// the report whole: the summary, the load's findings, and the level.
#[tokio::test]
async fn test_doctor_reports_a_failed_walk_beside_a_whole_report() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let fixture = fixture_of(
        dir.path(),
        "kv_doctor_unread",
        ValueKind::Protobuf,
        Nulls::None,
    )
    .await?;
    let scratch = tempfile::tempdir()?;
    let config = entry(&fixture, Residency::Full, ValueFormat::Protobuf);
    let served = serve(&fixture, config.clone(), scratch.path()).await?;
    let generation = served.current();
    let formats = config.formats();
    let whole = icetray_core::diagnose(&generation, Level::Optimal, &formats);
    assert!(remove_data_files(dir.path())? > 0);
    let examined = examine(&generation, Level::Optimal, &formats).await;
    let columns: Vec<&str> = examined
        .unwalked
        .iter()
        .map(|unwalked| unwalked.column.as_str())
        .collect();
    assert_eq!(columns, [VALUE_COLUMN], "the protobuf column alone");
    assert_eq!(examined.report.summary.level, whole.summary.level);
    assert_eq!(examined.report.findings.len(), whole.findings.len());
    assert_eq!(examined.report.files.len(), whole.files.len());
    Ok(())
}
