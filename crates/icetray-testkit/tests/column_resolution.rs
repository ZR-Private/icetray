//! How a configured column is found in each data file (RFD 023), one
//! fixture per row of its table and per case it names: a column served by
//! name and field id, a swap and a rename refused, a column added after a
//! file answering its default with no value-page GET, a reused file whose
//! name was dropped and added again refused on refresh while the last
//! generation serves, a default that does not read, a `fixed` column at
//! another width, a file without its key column, and files that record no
//! field ids or only some.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryArray, FixedSizeBinaryArray, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use iceberg::TableIdent;
use iceberg::spec::{FormatVersion, Literal, PrimitiveType, Struct};
use icetray_core::ColumnRead;
use icetray_core::approx::{self, count};
use icetray_core::config::{Residency, TableConfig, ValueColumnConfig, ValueFormat};
use icetray_core::metric::{self, LookupResult};
use icetray_core::{
    ColumnMismatch, Error, ErrorKind, FindingCode, Key, KeyType, Level, RefusalReason, Row,
    TableFault, TableSource, TableState, diagnose, examine,
};
use icetray_testkit::error::SpecFault;
use icetray_testkit::fixture::{
    IDS_2K, KEY_FIELD_ID, NAMESPACE, OLD_ID, SECOND_VALUE_COLUMN, SECOND_VALUE_FIELD_ID, STRAY_IDS,
    SchemaChange, StatedDefault, TWO_BUCKETS, VALUE_FIELD_ID, field_with_id, local_catalog,
    write_table_of_types,
};
use icetray_testkit::keys::FIRST_VERSION;
use icetray_testkit::layout::{KEY_COLUMN, VALUE_COLUMN};
use icetray_testkit::protobuf;
use icetray_testkit::verify::{
    self, HINT_POLL_GETS, LOAD_METADATA_GETS, METADATA_FILE_GETS, SeenPages, Served,
};
use icetray_testkit::{
    DeleteForm, Fixture, FixtureSpec, HandSnapshot, Layout, RawFile, Result, TestkitError,
    value_for, write_fixture,
};

/// The long keys of a raw file, ascending.
const RAW_KEYS: [i64; 3] = [0, 3, 6];

/// The name of every raw file under its table's data directory.
const RAW_FILE: &str = "raw.parquet";

/// A column of a raw file that no fixture schema names.
const OTHER_COLUMN: &str = "other";

/// The name a rename gives the value column, and the one a swap parks it
/// under while the other column takes its name.
const RENAMED: &str = "payload";
const SWAP_SCRATCH: &str = "swapping";

/// The width of the `fixed` value column of the width fixture, and the
/// narrower width its raw file stores the column at.
const FIXED_WIDTH: u64 = 8;
const NARROWER_WIDTH: i32 = 4;

/// The name the catalog records the evolved metadata of the catalog test
/// under.
const EVOLVED_TABLE: &str = "kv_unread_catalog_evolved";

/// The bytes a v3 table states as an added column's `initial-default`.
const DEFAULT_BYTES: &[u8] = b"written before the column";

/// The bytes the protobuf message a v3 table states as an added column's
/// `initial-default` is padded to, in the projection tests.
const PROTOBUF_DEFAULT_BYTES: usize = 64;

/// Ids sampled from the first build of an `IDS_2K` fixture: both ends and
/// the middle, in both buckets.
const FIRST_BUILD_SAMPLE: [u64; 4] = [0, 1, 999, 1_999];

/// An id of the first build that a delete removes, outside
/// `FIRST_BUILD_SAMPLE`.
const DELETED_ID: u64 = 500;

/// A two-bucket fixture of `IDS_2K` named `name`, at `format_version`,
/// with `SECOND_VALUE_COLUMN` when `second_value`.
async fn two_bucket_fixture(
    name: &str,
    format_version: FormatVersion,
    second_value: bool,
) -> Result<(tempfile::TempDir, Fixture)> {
    let (dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: IDS_2K,
        buckets: TWO_BUCKETS,
        format_version,
        second_value,
        ..FixtureSpec::small(name, KeyType::Long)
    };
    let fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    Ok((dir, fixture))
}

/// `fixture`'s table entry in `residency` serving `columns`, from its root
/// followed through the version hint, and the source it names.
async fn root_entry(
    fixture: &Fixture,
    columns: &[&str],
    residency: Residency,
) -> Result<(TableConfig, TableSource)> {
    let source = fixture.root_source();
    let config = TableConfig {
        source: source.clone(),
        value_columns: columns
            .iter()
            .map(|&column| ValueColumnConfig::named(column))
            .collect(),
        ..fixture.config(residency, false)
    };
    Ok((config, TableSource::from_config(&source).await?))
}

/// `fixture` served from its root in `residency` serving `columns`, over a
/// fresh storage, with a local directory under `scratch` in the Disk tier.
async fn serve_root_in(
    fixture: &Fixture,
    columns: &[&str],
    residency: Residency,
    scratch: &Path,
) -> Result<Served> {
    let (config, source) = root_entry(fixture, columns, residency).await?;
    verify::build_in(config, source, scratch, verify::TEST_PAGE_CACHE_BYTES)
        .await?
        .load()
        .await
}

/// `fixture` served from its root in the Full tier serving `columns`.
async fn serve_root(fixture: &Fixture, columns: &[&str]) -> Result<Served> {
    let (config, source) = root_entry(fixture, columns, Residency::Full).await?;
    verify::serve(config, source, verify::storage_config_for_tests()).await
}

/// The error the first load of `config` from `source` fails with; the
/// table is left with no generation.
async fn load_error(config: TableConfig, source: TableSource) -> Result<Error> {
    let served = verify::build(
        Arc::new(icetray_core::Storage::new(
            verify::storage_config_for_tests(),
        )),
        config,
        source,
        None,
        verify::TEST_PAGE_CACHE_BYTES,
    )
    .await?;
    let error = served
        .table
        .load()
        .await
        .expect_err("the fixture is designed to be refused");
    assert!(served.table.current().is_none(), "nothing serves");
    Ok(error)
}

/// The error the first load of `fixture` from its root serving
/// `value_column` alone fails with.
async fn first_load_error(fixture: &Fixture, value_column: &str) -> Result<Error> {
    let (config, source) = root_entry(fixture, &[value_column], Residency::Full).await?;
    load_error(config, source).await
}

/// The column mismatch `error` refuses with: the column, the file, and the
/// case.
fn mismatch_of(error: &Error) -> (String, Option<String>, ColumnMismatch) {
    let Error::Refused {
        reason:
            RefusalReason::ColumnMismatch {
                column,
                path,
                mismatch,
            },
        ..
    } = error
    else {
        panic!("expected a column mismatch, got {error:?}");
    };
    (column.clone(), path.clone(), mismatch.clone())
}

/// Whether `path` is one of the data files `fixture` lists.
fn is_listed(fixture: &Fixture, path: Option<&str>) -> bool {
    path.is_some_and(|path| fixture.files().iter().any(|file| file.file_path() == path))
}

/// The field id the current schema of `fixture` gives `name`.
fn field_id(fixture: &Fixture, name: &str) -> i32 {
    fixture
        .metadata()
        .current_schema()
        .as_struct()
        .field_by_name(name)
        .expect("the schema holds the column")
        .id
}

#[tokio::test]
async fn test_swapped_names_are_refused() -> Result<()> {
    let (_dir, mut fixture) = two_bucket_fixture("kv_swap", FormatVersion::V2, true).await?;
    fixture
        .evolve(&[
            SchemaChange::Rename {
                from: VALUE_COLUMN.to_string(),
                to: SWAP_SCRATCH.to_string(),
            },
            SchemaChange::Rename {
                from: SECOND_VALUE_COLUMN.to_string(),
                to: VALUE_COLUMN.to_string(),
            },
            SchemaChange::Rename {
                from: SWAP_SCRATCH.to_string(),
                to: SECOND_VALUE_COLUMN.to_string(),
            },
        ])
        .await?;
    let error = first_load_error(&fixture, VALUE_COLUMN).await?;
    let (column, path, mismatch) = mismatch_of(&error);
    assert_eq!(column, VALUE_COLUMN);
    assert!(is_listed(&fixture, path.as_deref()), "{path:?}");
    assert_eq!(
        mismatch,
        ColumnMismatch::OtherFieldId {
            expected: SECOND_VALUE_FIELD_ID,
            found: VALUE_FIELD_ID,
        }
    );
    Ok(())
}

#[tokio::test]
async fn test_a_rename_is_refused_under_either_name() -> Result<()> {
    let (_dir, mut fixture) = two_bucket_fixture("kv_rename", FormatVersion::V2, false).await?;
    fixture
        .evolve(&[SchemaChange::Rename {
            from: VALUE_COLUMN.to_string(),
            to: RENAMED.to_string(),
        }])
        .await?;
    let error = first_load_error(&fixture, RENAMED).await?;
    let (column, path, mismatch) = mismatch_of(&error);
    assert_eq!(column, RENAMED);
    assert!(is_listed(&fixture, path.as_deref()), "{path:?}");
    assert_eq!(
        mismatch,
        ColumnMismatch::Renamed {
            field_id: VALUE_FIELD_ID,
            found: VALUE_COLUMN.to_string(),
        }
    );
    // The old name is no longer in the schema at all.
    let error = first_load_error(&fixture, VALUE_COLUMN).await?;
    assert!(
        matches!(
            &error,
            Error::Table {
                fault: TableFault::ColumnMissing { column },
                ..
            } if column == VALUE_COLUMN
        ),
        "{error:?}"
    );
    Ok(())
}

/// Adds `SECOND_VALUE_COLUMN` stating `default` to `fixture` and commits
/// files holding it for `STRAY_IDS` beside the first build's files, which
/// predate it.
async fn add_column_then_rows(fixture: &mut Fixture, default: StatedDefault) -> Result<Vec<u64>> {
    fixture
        .evolve(&[SchemaChange::Add {
            name: SECOND_VALUE_COLUMN.to_string(),
            ty: PrimitiveType::Binary,
            default,
        }])
        .await?;
    let later: Vec<u64> = STRAY_IDS.collect();
    let written = fixture.write_rows(&later, FIRST_VERSION, "later").await?;
    let mut data = fixture.files().to_vec();
    data.extend(written);
    fixture
        .commit_by_hand(HandSnapshot {
            data,
            deletes: Vec::new(),
        })
        .await?;
    Ok(later)
}

/// Looks up every one of `ids` in `served` naming `columns`, asserting
/// each answers `expected` of its key, and returns the GETs the lookups
/// made.
async fn gets_answering(
    served: &Served,
    fixture: &Fixture,
    ids: &[u64],
    columns: &[&str],
    expected: impl Fn(&Key) -> Row,
) -> Result<u64> {
    let stats = served.storage.stats();
    let before = stats.gets();
    for &id in ids {
        let key = fixture.key(id);
        assert_eq!(
            served.table.get(&key, &verify::whole(columns)).await?.row,
            expected(&key),
            "{id}"
        );
    }
    Ok(stats.gets() - before)
}

#[tokio::test]
async fn test_a_column_added_after_a_file_answers_null_there_with_no_get() -> Result<()> {
    let (_dir, mut fixture) = two_bucket_fixture("kv_added", FormatVersion::V2, false).await?;
    let earlier = fixture.files().len();
    let later = add_column_then_rows(&mut fixture, StatedDefault::None).await?;
    fixture
        .delete(&[DELETED_ID], DeleteForm::ScopedPositions, "gone")
        .await?;
    let served = serve_root(&fixture, &[SECOND_VALUE_COLUMN]).await?;
    assert_eq!(
        gets_answering(
            &served,
            &fixture,
            &FIRST_BUILD_SAMPLE,
            &[SECOND_VALUE_COLUMN],
            |_| Row::Present(vec![None]),
        )
        .await?,
        0,
        "a row answered by its default reads no value page"
    );
    assert_eq!(
        served
            .table
            .get(
                &fixture.key(DELETED_ID),
                &[ColumnRead::whole(SECOND_VALUE_COLUMN)]
            )
            .await?
            .row,
        Row::Absent,
        "a deleted row is deleted whatever its column answers"
    );
    let gets = gets_answering(&served, &fixture, &later, &[SECOND_VALUE_COLUMN], |key| {
        fixture.expected_of(key, &[SECOND_VALUE_COLUMN])
    })
    .await?;
    assert!(
        gets > 0,
        "the later files' values are read from their pages"
    );

    // Each column is resolved on its own (RFD 022): an earlier file serves
    // the value column from its pages and answers the added one by its
    // default, one value-page GET for the column it holds and none for the
    // other, while a later file serves both.
    let both = [VALUE_COLUMN, SECOND_VALUE_COLUMN];
    let served_both = serve_root(&fixture, &both).await?;
    let one_column_gets = verify::expected_cold_gets(
        &served_both.current(),
        &fixture.keys_at(&FIRST_BUILD_SAMPLE),
        &[VALUE_COLUMN],
    )?;
    assert!(one_column_gets > 0);
    let earlier_gets = gets_answering(&served_both, &fixture, &FIRST_BUILD_SAMPLE, &both, |key| {
        Row::Present(vec![fixture.value(key).map(Into::into), None])
    })
    .await?;
    assert_eq!(
        earlier_gets, one_column_gets,
        "the column a file holds costs its pages, the defaulted one none"
    );
    gets_answering(&served_both, &fixture, &later, &both, |key| {
        fixture.expected_of(key, &both)
    })
    .await?;

    let generation = served.current();
    let report = diagnose(&generation, Level::Optimal, &ValueFormat::opaque);
    let defaults: Vec<_> = report
        .findings
        .iter()
        .filter(|finding| finding.code() == FindingCode::ColumnDefault)
        .collect();
    assert_eq!(defaults.len(), 1, "{:?}", report.findings);
    assert_eq!(defaults[0].caps(), None, "advice caps nothing");
    let without_column = generation
        .files
        .iter()
        .filter(|file| file.index.value_leaf(0).is_none())
        .count();
    assert_eq!(without_column, earlier);
    assert_eq!(
        report
            .files
            .iter()
            .filter(|file| file.value_columns[0].answers_default
                && file.value_columns[0].value_pages == 0)
            .count(),
        earlier,
        "a file answering by default has no value pages to measure"
    );
    Ok(())
}

/// In the Bloom tier a row answered by its default costs the key page its
/// probe decodes and no value page; in the Disk tier it costs no GET, the
/// key page being on local disk. One `multi_get` answers keys of files
/// that hold the column and files that predate it.
#[tokio::test]
async fn test_a_default_costs_no_value_get_in_the_disk_and_bloom_tiers() -> Result<()> {
    let (_dir, mut fixture) =
        two_bucket_fixture("kv_added_tiers", FormatVersion::V2, false).await?;
    let later = add_column_then_rows(&mut fixture, StatedDefault::None).await?;
    let scratch = tempfile::tempdir()?;
    let full = serve_root(&fixture, &[SECOND_VALUE_COLUMN]).await?;
    let full_generation = full.current();
    let earlier: Vec<Key> = FIRST_BUILD_SAMPLE
        .iter()
        .map(|&id| fixture.key(id))
        .collect();
    for residency in [Residency::Bloom, Residency::Disk] {
        let served =
            serve_root_in(&fixture, &[SECOND_VALUE_COLUMN], residency, scratch.path()).await?;
        let generation = served.current();
        let mut seen = SeenPages::default();
        let expected: u64 = earlier
            .iter()
            .map(|key| {
                verify::expected_lookup_gets(
                    &full_generation,
                    &generation,
                    SECOND_VALUE_COLUMN,
                    key,
                    true,
                    &mut seen,
                )
            })
            .sum();
        let gets = gets_answering(
            &served,
            &fixture,
            &FIRST_BUILD_SAMPLE,
            &[SECOND_VALUE_COLUMN],
            |_| Row::Present(vec![None]),
        )
        .await?;
        assert_eq!(gets, expected, "{residency}");
        match residency {
            Residency::Bloom => {
                assert!(gets > 0, "the key pages are fetched");
                assert_eq!(
                    gets,
                    verify::expected_key_pages(&generation, &earlier),
                    "the distinct key pages and no value page"
                );
            }
            Residency::Disk => assert_eq!(gets, 0, "the key pages are on local disk"),
            Residency::Full => unreachable!("the loop serves the Bloom and Disk tiers"),
        }
    }

    let later: Vec<Key> = later.iter().map(|&id| fixture.key(id)).collect();
    let mixed: Vec<Key> = earlier.iter().chain(&later).cloned().collect();
    let expected: Vec<Row> = earlier
        .iter()
        .map(|_| Row::Present(vec![None]))
        .chain(
            later
                .iter()
                .map(|key| fixture.expected_of(key, &[SECOND_VALUE_COLUMN])),
        )
        .collect();
    assert_eq!(
        full.table
            .multi_get(&mixed, &[ColumnRead::whole(SECOND_VALUE_COLUMN)])
            .await?
            .rows,
        expected
    );
    Ok(())
}

/// A row answered by its default reads no value page, so its lookup is
/// counted a warm hit, never a cold one.
#[tokio::test]
async fn test_a_default_answer_is_counted_a_warm_hit() -> Result<()> {
    let recorder = icetray::telemetry::install().expect("the recorder installs");
    let (_dir, mut fixture) = two_bucket_fixture("kv_added_warm", FormatVersion::V2, false).await?;
    add_column_then_rows(&mut fixture, StatedDefault::None).await?;
    let served = serve_root(&fixture, &[SECOND_VALUE_COLUMN]).await?;
    gets_answering(
        &served,
        &fixture,
        &FIRST_BUILD_SAMPLE,
        &[SECOND_VALUE_COLUMN],
        |_| Row::Present(vec![None]),
    )
    .await?;
    served.table.publish_lookup_counts();
    let scrape = recorder.render();
    let lookups = |result: &str| {
        verify::series_value(
            &scrape,
            metric::LOOKUPS_TOTAL,
            &[
                (metric::LABEL_TABLE, served.table.name()),
                (metric::LABEL_RESULT, result),
            ],
        )
        .unwrap_or(0.0)
    };
    assert_eq!(
        lookups(&LookupResult::WarmHit.to_string()).to_bits(),
        approx::gauge(count(FIRST_BUILD_SAMPLE.len())).to_bits()
    );
    assert_eq!(
        lookups(&LookupResult::ColdHit.to_string()).to_bits(),
        0.0_f64.to_bits()
    );
    Ok(())
}

#[tokio::test]
async fn test_a_column_added_with_a_v3_initial_default_answers_its_bytes() -> Result<()> {
    let (_dir, mut fixture) = two_bucket_fixture("kv_default", FormatVersion::V3, false).await?;
    add_column_then_rows(
        &mut fixture,
        StatedDefault::Literal(Literal::binary(DEFAULT_BYTES.to_vec())),
    )
    .await?;
    let served = serve_root(&fixture, &[SECOND_VALUE_COLUMN]).await?;
    assert_eq!(
        gets_answering(
            &served,
            &fixture,
            &FIRST_BUILD_SAMPLE,
            &[SECOND_VALUE_COLUMN],
            |_| { Row::Present(vec![Some(DEFAULT_BYTES.into())]) }
        )
        .await?,
        0,
        "a row answered by its default reads no value page"
    );
    Ok(())
}

/// The protobuf message the projection tests state as a default: the
/// testkit's message drawn from the stream of `seed`, and what a read of
/// `fields` trims it to.
fn protobuf_default(seed: &Key, fields: &[u32]) -> (Vec<u8>, Vec<u8>) {
    (
        protobuf::value(seed, PROTOBUF_DEFAULT_BYTES, FIRST_VERSION),
        protobuf::projected(seed, PROTOBUF_DEFAULT_BYTES, FIRST_VERSION, fields),
    )
}

/// `fixture` served from its root in `residency` with `SECOND_VALUE_COLUMN`
/// declared protobuf, a local directory under `scratch` in the Disk tier.
async fn serve_protobuf_default(
    fixture: &Fixture,
    residency: Residency,
    scratch: &Path,
) -> Result<Served> {
    let (config, source) = root_entry(fixture, &[SECOND_VALUE_COLUMN], residency).await?;
    let config = TableConfig {
        value_columns: vec![ValueColumnConfig::of(
            SECOND_VALUE_COLUMN,
            ValueFormat::Protobuf,
        )],
        ..config
    };
    verify::build_in(config, source, scratch, verify::TEST_PAGE_CACHE_BYTES)
        .await?
        .load()
        .await
}

/// A default answered for a protobuf column is projected as a value (RFD
/// 024), still with no value-page GET: in the Disk tier no GET, in the
/// Bloom tier the key pages alone.
#[tokio::test]
async fn test_a_projected_default_is_trimmed_with_no_value_get_in_the_disk_and_bloom_tiers()
-> Result<()> {
    let (_dir, mut fixture) =
        two_bucket_fixture("kv_default_projected", FormatVersion::V3, false).await?;
    let fields = [protobuf::NAME_FIELD, protobuf::ID_FIELD];
    let (default, trimmed) = protobuf_default(&fixture.key(0), &fields);
    assert!(!trimmed.is_empty() && trimmed.len() < default.len());
    add_column_then_rows(
        &mut fixture,
        StatedDefault::Literal(Literal::binary(default)),
    )
    .await?;
    let earlier: Vec<Key> = FIRST_BUILD_SAMPLE
        .iter()
        .map(|&id| fixture.key(id))
        .collect();
    let reads = [ColumnRead {
        name: SECOND_VALUE_COLUMN,
        fields: &fields,
    }];
    let scratch = tempfile::tempdir()?;
    for residency in [Residency::Bloom, Residency::Disk] {
        let served = serve_protobuf_default(&fixture, residency, scratch.path()).await?;
        let generation = served.current();
        let stats = served.storage.stats();
        let before = stats.gets();
        for key in &earlier {
            assert_eq!(
                served.table.get(key, &reads).await?.row,
                Row::Present(vec![Some(trimmed.clone().into())]),
                "{residency} {key}"
            );
        }
        let key_pages = match residency {
            Residency::Bloom => verify::expected_key_pages(&generation, &earlier),
            Residency::Disk => 0,
            Residency::Full => unreachable!("the loop serves the Bloom and Disk tiers"),
        };
        assert_eq!(
            stats.gets() - before,
            key_pages,
            "{residency}: no value page"
        );
        let report = examine(&generation, Level::Optimal, &|name| {
            served.table.config().format_of(name).unwrap_or_default()
        })
        .await
        .report;
        assert!(
            !report.findings.iter().any(|finding| {
                finding.code() == FindingCode::MalformedProtobuf && finding.file().is_none()
            }),
            "a protobuf default walks: {:?}",
            report.findings
        );
    }
    Ok(())
}

/// The doctor walks a protobuf column's stated default with no GET, and a
/// default that is not protobuf is `malformed_protobuf` naming the column
/// and no file.
#[tokio::test]
async fn test_doctor_walks_a_protobuf_column_s_stated_default() -> Result<()> {
    let (_dir, mut fixture) =
        two_bucket_fixture("kv_default_malformed", FormatVersion::V3, false).await?;
    assert!(
        protobuf::first_byte_fault(DEFAULT_BYTES).is_some(),
        "the text default is not protobuf"
    );
    fixture
        .evolve(&[SchemaChange::Add {
            name: SECOND_VALUE_COLUMN.to_string(),
            ty: PrimitiveType::Binary,
            default: StatedDefault::Literal(Literal::binary(DEFAULT_BYTES.to_vec())),
        }])
        .await?;
    let data = fixture.files().to_vec();
    fixture
        .commit_by_hand(HandSnapshot {
            data,
            deletes: Vec::new(),
        })
        .await?;
    let scratch = tempfile::tempdir()?;
    let served = serve_protobuf_default(&fixture, Residency::Full, scratch.path()).await?;
    let generation = served.current();
    let stats = served.storage.stats();
    let before = stats.gets();
    let examined = examine(&generation, Level::Optimal, &|name| {
        served.table.config().format_of(name).unwrap_or_default()
    })
    .await;
    assert_eq!(
        stats.gets(),
        before,
        "every file answers the default, so no page is read"
    );
    assert!(examined.unwalked.is_empty());
    let found: Vec<_> = examined
        .report
        .findings
        .iter()
        .filter(|finding| finding.code() == FindingCode::MalformedProtobuf)
        .collect();
    let [finding] = found[..] else {
        panic!("one finding, got {found:?}");
    };
    assert_eq!(
        (finding.column(), finding.file(), finding.caps()),
        (Some(SECOND_VALUE_COLUMN), None, None)
    );
    Ok(())
}

#[tokio::test]
async fn test_a_default_that_does_not_read_is_refused() -> Result<()> {
    let (_dir, mut fixture) = two_bucket_fixture("kv_unread", FormatVersion::V3, false).await?;
    fixture
        .evolve(&[SchemaChange::Add {
            name: SECOND_VALUE_COLUMN.to_string(),
            ty: PrimitiveType::Binary,
            default: StatedDefault::Raw(serde_json::json!("not hex")),
        }])
        .await?;
    let error = first_load_error(&fixture, SECOND_VALUE_COLUMN).await?;
    assert_eq!(
        mismatch_of(&error),
        (
            SECOND_VALUE_COLUMN.to_string(),
            None,
            ColumnMismatch::UnreadableDefault
        )
    );
    Ok(())
}

/// A catalog's client parses a default it cannot read as none; the
/// metadata file the catalog names still states it, and the snapshot is
/// refused as from a static root.
#[tokio::test]
async fn test_a_default_that_does_not_read_is_refused_through_a_catalog() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        format_version: FormatVersion::V3,
        ..FixtureSpec::small("kv_unread_catalog", KeyType::Long)
    };
    let mut fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    fixture
        .evolve(&[SchemaChange::Add {
            name: SECOND_VALUE_COLUMN.to_string(),
            ty: PrimitiveType::Binary,
            default: StatedDefault::Raw(serde_json::json!("not hex")),
        }])
        .await?;
    // A by-hand commit leaves the catalog's record behind; a record of its
    // metadata file is the catalog serving it.
    let ident = TableIdent::new(
        fixture.ident().namespace().clone(),
        EVOLVED_TABLE.to_string(),
    );
    catalog
        .register_table(&ident, fixture.metadata_location().to_string())
        .await?;
    let config = TableConfig {
        value_columns: vec![ValueColumnConfig::named(SECOND_VALUE_COLUMN)],
        ..fixture.config(Residency::Full, false)
    };
    let error = load_error(config, TableSource::Catalog { catalog, ident }).await?;
    assert_eq!(
        mismatch_of(&error),
        (
            SECOND_VALUE_COLUMN.to_string(),
            None,
            ColumnMismatch::UnreadableDefault
        )
    );
    Ok(())
}

#[tokio::test]
async fn test_a_reused_file_under_a_dropped_and_readded_name_is_refused_on_refresh() -> Result<()> {
    let (_dir, mut fixture) = two_bucket_fixture("kv_readd", FormatVersion::V2, false).await?;
    fixture.write_hint().await?;
    let served = serve_root(&fixture, &[VALUE_COLUMN]).await?;
    let first = served.current().snapshot_id;

    fixture
        .evolve(&[
            SchemaChange::Drop {
                name: VALUE_COLUMN.to_string(),
            },
            SchemaChange::Add {
                name: VALUE_COLUMN.to_string(),
                ty: PrimitiveType::Binary,
                default: StatedDefault::None,
            },
        ])
        .await?;
    let readded = field_id(&fixture, VALUE_COLUMN);
    assert_ne!(readded, VALUE_FIELD_ID);
    let stats = served.storage.stats();
    let before = stats.gets();
    let error = served.table.refresh().await.unwrap_err();
    assert!(!fixture.files().is_empty());
    assert_eq!(
        stats.gets() - before,
        HINT_POLL_GETS + METADATA_FILE_GETS + LOAD_METADATA_GETS,
        "the hint, the metadata, the manifest list and its manifest: every file \
         is reused and checked from the footer it keeps, and no footer is read"
    );
    let (column, path, mismatch) = mismatch_of(&error);
    assert_eq!(column, VALUE_COLUMN);
    assert!(is_listed(&fixture, path.as_deref()), "{path:?}");
    assert_eq!(
        mismatch,
        ColumnMismatch::OtherFieldId {
            expected: readded,
            found: VALUE_FIELD_ID,
        }
    );

    assert_eq!(
        served.current().snapshot_id,
        first,
        "the last generation serves"
    );
    let old_key = fixture.key(OLD_ID);
    assert_eq!(
        served
            .table
            .get(&old_key, &[ColumnRead::whole(VALUE_COLUMN)])
            .await?
            .row,
        fixture.expected(&old_key)
    );
    assert!(
        matches!(
            served.table.state(),
            TableState::Degraded {
                serving: Some(serving),
                reason: ErrorKind::Refused(RefusalReason::ColumnMismatch { .. }),
                ..
            } if serving == first
        ),
        "{:?}",
        served.table.state()
    );
    let before = stats.gets();
    assert!(!served.table.refresh().await?, "the refusal is remembered");
    assert_eq!(
        stats.gets() - before,
        HINT_POLL_GETS,
        "a poll of the remembered location reads the hint alone"
    );
    Ok(())
}

#[tokio::test]
async fn test_evolve_refuses_to_rename_or_drop_a_column_the_schema_lacks() -> Result<()> {
    let (_dir, mut fixture) =
        two_bucket_fixture("kv_evolve_absent", FormatVersion::V2, false).await?;
    let version = fixture.metadata_location().to_string();
    for change in [
        SchemaChange::Rename {
            from: OTHER_COLUMN.to_string(),
            to: RENAMED.to_string(),
        },
        SchemaChange::Drop {
            name: OTHER_COLUMN.to_string(),
        },
    ] {
        let refused = fixture.evolve(&[change]).await;
        assert!(
            matches!(
                &refused,
                Err(TestkitError::Spec(SpecFault::ColumnNotInSchema { name })) if name == OTHER_COLUMN
            ),
            "{refused:?}"
        );
    }
    assert_eq!(
        fixture.metadata_location(),
        version,
        "nothing was committed"
    );
    Ok(())
}

/// A raw file of `RAW_KEYS` with `columns`, each a field and its values,
/// under the contract layout with no manifest bounds.
fn raw_file(columns: Vec<(Field, ArrayRef)>) -> Result<RawFile> {
    let (fields, arrays): (Vec<Field>, Vec<ArrayRef>) = columns.into_iter().unzip();
    let batch = RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), arrays)?;
    RawFile::from_batch(RAW_FILE, &batch, &Layout::contract(), Struct::empty(), None)
}

fn raw_keys() -> ArrayRef {
    Arc::new(Int64Array::from(RAW_KEYS.to_vec()))
}

/// The binary values of `RAW_KEYS`.
fn raw_values() -> ArrayRef {
    let values: Vec<Vec<u8>> = RAW_KEYS
        .iter()
        .map(|&key| value_for(&Key::Long(key)))
        .collect();
    Arc::new(BinaryArray::from_iter_values(values))
}

/// A long-keyed table whose value column is of `value` type and whose one
/// data file is `file`, named `name`.
async fn raw_table(
    name: &str,
    value: PrimitiveType,
    file: RawFile,
) -> Result<(tempfile::TempDir, Fixture)> {
    let (dir, catalog) = local_catalog().await?;
    let fixture = write_table_of_types(
        catalog.as_ref(),
        NAMESPACE,
        name,
        PrimitiveType::Long,
        value,
        0,
        file,
    )
    .await?;
    Ok((dir, fixture))
}

/// The error the first load of `fixture` from its pinned metadata file
/// fails with.
async fn pinned_load_error(fixture: &Fixture) -> Result<Error> {
    let config = fixture.config(Residency::Full, false);
    let source = TableSource::from_config(&config.source).await?;
    load_error(config, source).await
}

#[tokio::test]
async fn test_a_file_without_field_ids_holding_the_name_is_served() -> Result<()> {
    let file = raw_file(vec![
        (Field::new(KEY_COLUMN, DataType::Int64, false), raw_keys()),
        (
            Field::new(VALUE_COLUMN, DataType::Binary, true),
            raw_values(),
        ),
    ])?;
    let (_dir, fixture) = raw_table("kv_no_ids", PrimitiveType::Binary, file).await?;
    let config = fixture.config(Residency::Full, false);
    let source = TableSource::from_config(&config.source).await?;
    let served = verify::serve(config, source, verify::storage_config_for_tests()).await?;
    for key in RAW_KEYS {
        let key = Key::Long(key);
        assert_eq!(
            served
                .table
                .get(&key, &[ColumnRead::whole(VALUE_COLUMN)])
                .await?
                .row,
            Row::Present(vec![Some(value_for(&key).into())])
        );
    }
    Ok(())
}

#[tokio::test]
async fn test_a_file_without_field_ids_lacking_the_value_column_is_refused() -> Result<()> {
    let file = raw_file(vec![
        (Field::new(KEY_COLUMN, DataType::Int64, false), raw_keys()),
        (
            Field::new(OTHER_COLUMN, DataType::Binary, true),
            raw_values(),
        ),
    ])?;
    let (_dir, fixture) = raw_table("kv_no_ids_absent", PrimitiveType::Binary, file).await?;
    let (column, path, mismatch) = mismatch_of(&pinned_load_error(&fixture).await?);
    assert_eq!(column, VALUE_COLUMN);
    assert!(is_listed(&fixture, path.as_deref()), "{path:?}");
    assert_eq!(mismatch, ColumnMismatch::AbsentWithoutFieldIds);
    Ok(())
}

/// Were the file's one field id taken for a file that records ids, the
/// absent value column would answer its default; a file where only some
/// columns carry one records none, so it is refused.
#[tokio::test]
async fn test_a_file_with_field_ids_on_only_some_columns_records_none() -> Result<()> {
    let file = raw_file(vec![
        (
            field_with_id(KEY_COLUMN, DataType::Int64, false, KEY_FIELD_ID),
            raw_keys(),
        ),
        (
            Field::new(OTHER_COLUMN, DataType::Binary, true),
            raw_values(),
        ),
    ])?;
    let (_dir, fixture) = raw_table("kv_some_ids", PrimitiveType::Binary, file).await?;
    let (column, _, mismatch) = mismatch_of(&pinned_load_error(&fixture).await?);
    assert_eq!(column, VALUE_COLUMN);
    assert_eq!(mismatch, ColumnMismatch::AbsentWithoutFieldIds);
    Ok(())
}

#[tokio::test]
async fn test_a_file_without_its_key_column_is_refused() -> Result<()> {
    let file = raw_file(vec![(
        field_with_id(VALUE_COLUMN, DataType::Binary, true, VALUE_FIELD_ID),
        raw_values(),
    )])?;
    let (_dir, fixture) = raw_table("kv_keyless", PrimitiveType::Binary, file).await?;
    let (column, path, mismatch) = mismatch_of(&pinned_load_error(&fixture).await?);
    assert_eq!(column, KEY_COLUMN);
    assert!(is_listed(&fixture, path.as_deref()), "{path:?}");
    assert_eq!(mismatch, ColumnMismatch::KeyAbsent);
    Ok(())
}

#[tokio::test]
async fn test_a_fixed_value_column_at_another_width_is_refused() -> Result<()> {
    let narrower: ArrayRef = Arc::new(
        FixedSizeBinaryArray::try_from_iter(
            RAW_KEYS
                .iter()
                .map(|_| vec![0_u8; usize::try_from(NARROWER_WIDTH).unwrap()]),
        )
        .unwrap(),
    );
    let cases = [
        (
            "kv_fixed_narrow",
            field_with_id(
                VALUE_COLUMN,
                DataType::FixedSizeBinary(NARROWER_WIDTH),
                true,
                VALUE_FIELD_ID,
            ),
            narrower,
            Some(NARROWER_WIDTH),
        ),
        (
            "kv_fixed_variable",
            field_with_id(VALUE_COLUMN, DataType::Binary, true, VALUE_FIELD_ID),
            raw_values(),
            None,
        ),
    ];
    for (name, field, values, stored) in cases {
        let file = raw_file(vec![
            (
                field_with_id(KEY_COLUMN, DataType::Int64, false, KEY_FIELD_ID),
                raw_keys(),
            ),
            (field, values),
        ])?;
        let (_dir, fixture) = raw_table(name, PrimitiveType::Fixed(FIXED_WIDTH), file).await?;
        let (column, _, mismatch) = mismatch_of(&pinned_load_error(&fixture).await?);
        assert_eq!(column, VALUE_COLUMN);
        assert_eq!(
            mismatch,
            ColumnMismatch::FixedWidth {
                expected: FIXED_WIDTH,
                stored,
            },
            "{name}"
        );
    }
    Ok(())
}
