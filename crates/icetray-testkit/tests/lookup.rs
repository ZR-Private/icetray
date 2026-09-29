//! Serving fixture tables from a memory catalog on local disk in the Full
//! tier (the P tier of RFD 014): routing over every key type, one GET per
//! cold hit and none per warm hit or miss, `Generation::plan` as the oracle
//! for `multi_get`, and the GET costs of the layouts the doctor prices or
//! advises on; `findings.rs` holds the doctor's verdicts on them.

use icetray_core::approx::{count, length};
use icetray_core::config::Residency;
use icetray_core::contract::{PAGE_ROWS, VALUE_PAGE_BYTES};
use icetray_core::{Error, FindingCode, Generation, Key, KeyType, Value, diagnose};
use icetray_testkit::error::{Precondition, SpecFault};
use icetray_testkit::fixture::{
    FOUR_BUCKETS, IDS_4K, LONG_KEY_BYTES, LONG_KEY_PAD, NAMESPACE, SAMPLE, SAMPLE_4K,
    SMALL_BUCKETS, SMALL_FILE_BYTES, SMALL_IDS, UNPARTITIONED_FILE_BYTES, WIDE_VALUE_BYTES,
    local_catalog,
};
use icetray_testkit::keys::SCATTERED_ID_SPACE;
use icetray_testkit::verify;
use icetray_testkit::{
    Fixture, FixtureSpec, KeyShape, Layout, Nulls, Result, TestkitError, scattered_key_at,
    write_fixture,
};
use parquet::file::page_index::column_index::ColumnIndexMetaData;

/// Every this-many-th id is looked up once: routing over every bucket and
/// page of the small fixture.
const SWEEP_STRIDE: usize = 97;

/// Keys sharing one value page in the coalescing test.
const SHARED_PAGE_KEYS: usize = 10;

/// A key of another type than the long fixtures'.
const OTHER_TYPE_KEY: &str = "nope";

/// The nulls fixture's period: every seventh id has a null value, so the
/// sample mixes null and present ids.
const NULL_EVERY: u64 = 7;

/// A null id outside `SAMPLE`, and the present id after it.
const NULL_ID: u64 = 14;
const PRESENT_ID: u64 = 15;

/// A row-limited page carries about a thousand values, so the bytes one
/// cold hit transfers are at least this share of a full page.
const ROW_BOUNDED_PAGE_SHARE: u64 = 2;

/// The bytes a byte-bounded page may reach: the bound plus the overshoot
/// the write batch allows, with margin.
const BYTE_BOUND_OVERSHOOT: u64 = 4;

/// The bytes a byte-bounded cold hit transfers at most.
const BYTE_BOUNDED_PAGE_MAX_BYTES: u64 = BYTE_BOUND_OVERSHOOT * VALUE_PAGE_BYTES;

/// The bytes a row-bounded cold hit transfers at least: half of
/// `page_rows * WIDE_VALUE_BYTES`.
fn row_bounded_page_min_bytes() -> u64 {
    u64::try_from(PAGE_ROWS).expect("the contract page rows fit u64") * count(WIDE_VALUE_BYTES)
        / ROW_BOUNDED_PAGE_SHARE
}

/// The plan names one value page per `SAMPLE` id, so a cold batch over it
/// costs one GET per id.
fn assert_sample_pages_distinct(fixture: &Fixture, generation: &Generation) -> Result<()> {
    let cold = verify::expected_cold_gets(generation, &fixture.keys_at(&SAMPLE))?;
    assert_eq!(
        cold,
        count(SAMPLE.len()),
        "no two sample ids share a value page"
    );
    Ok(())
}

/// Writes `spec`, serves it, and checks shape, hit cost, misses, and a
/// sweep over every 97th id. The cold GET count of `SAMPLE` is pinned to
/// `SAMPLE.len()` for every key type by `assert_sample_pages_distinct`, so
/// a uuid-keyed batch costs what a long-keyed one does.
async fn round_trip(spec: FixtureSpec) -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    let served = verify::serve_catalog(&fixture, &catalog, Residency::Full, false).await?;
    let generation = served.current();
    assert_eq!(
        generation.files.len(),
        length(SMALL_BUCKETS),
        "one file per bucket"
    );
    assert_eq!(generation.records, SMALL_IDS.end);
    assert!(
        generation
            .files
            .iter()
            .all(|file| file.entry.key_min.is_some() && file.entry.key_max.is_some()),
        "every manifest entry carries both bounds"
    );
    assert_sample_pages_distinct(&fixture, &generation)?;
    let served = verify::verify_hit_cost(served, &fixture, &SAMPLE).await?;
    verify::verify_misses(&served, &fixture, &SAMPLE).await?;
    for id in fixture.id_range().step_by(SWEEP_STRIDE) {
        let key = fixture.key(id);
        assert!(
            matches!(served.table.get(&key).await?.value, Value::Present(_)),
            "key {key}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn test_get_long_keys() -> Result<()> {
    round_trip(FixtureSpec::small("kv_long", KeyType::Long)).await
}

#[tokio::test]
async fn test_get_string_keys() -> Result<()> {
    round_trip(FixtureSpec::small("kv_str", KeyType::String)).await
}

#[tokio::test]
async fn test_get_binary_keys() -> Result<()> {
    round_trip(FixtureSpec::small("kv_bin", KeyType::Binary)).await
}

/// A uuid-keyed lookup costs what a long-keyed one does: `round_trip` pins
/// both cold batches to `SAMPLE.len()` GETs.
#[tokio::test]
async fn test_get_uuid_keys() -> Result<()> {
    round_trip(FixtureSpec::small("kv_uuid", KeyType::Uuid)).await
}

#[tokio::test]
async fn test_get_several_files_per_bucket() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        layout: Layout {
            target_file_bytes: SMALL_FILE_BYTES,
            ..Layout::contract()
        },
        ..FixtureSpec::small("kv_split", KeyType::Long)
    };
    let fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    let served = verify::serve_catalog(&fixture, &catalog, Residency::Full, false).await?;
    let generation = served.current();
    assert!(
        generation.files.len() > length(SMALL_BUCKETS),
        "rolling produced several files per bucket: {}",
        generation.files.len()
    );
    for key in fixture.keys_at(&SAMPLE) {
        assert_eq!(generation.candidates(&key).count(), 1, "key {key}");
    }
    let served = verify::verify_hit_cost(served, &fixture, &SAMPLE).await?;
    verify::verify_misses(&served, &fixture, &SAMPLE).await
}

#[tokio::test]
async fn test_multi_get_coalesces_one_page_and_answers_from_one_snapshot() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let fixture = write_fixture(
        catalog.as_ref(),
        NAMESPACE,
        FixtureSpec::small("kv_multi", KeyType::Long),
    )
    .await?;
    let served = verify::serve_catalog(&fixture, &catalog, Residency::Full, false).await?;
    let generation = served.current();
    let page_of = |key: &Key| {
        generation
            .plan(std::slice::from_ref(key))
            .into_iter()
            .next()
            .flatten()
            .map(|(file, page)| (file.entry.path.clone(), page.value_page_offset))
    };
    let first = fixture.key(SAMPLE[0]);
    let target = page_of(&first).expect("the first sample key is present");
    let same_page: Vec<Key> = fixture
        .keys()
        .filter(|key| page_of(key) == Some(target.clone()))
        .take(SHARED_PAGE_KEYS)
        .collect();
    assert_eq!(same_page.len(), SHARED_PAGE_KEYS);
    let stats = served.storage.stats();

    let before = stats.gets();
    let multi = served.table.multi_get(&same_page).await?;
    assert_eq!(stats.gets() - before, 1, "one page, one GET, ten lookups");
    assert_eq!(multi.snapshot_id, generation.snapshot_id);
    for (key, value) in same_page.iter().zip(&multi.values) {
        assert_eq!(*value, fixture.expected(key), "key {key}");
    }

    let keys = fixture.keys_at(&SAMPLE);
    let expected = verify::expected_cold_gets(&generation, &keys)?;
    let before = stats.gets();
    let multi = served.table.multi_get(&keys).await?;
    assert_eq!(
        stats.gets() - before,
        expected - 1,
        "one GET per distinct page, the shared one already cached"
    );
    for (key, value) in keys.iter().zip(&multi.values) {
        assert_eq!(*value, fixture.expected(key), "key {key}");
    }
    Ok(())
}

#[tokio::test]
async fn test_get_refuses_a_key_of_another_type() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let fixture = write_fixture(
        catalog.as_ref(),
        NAMESPACE,
        FixtureSpec::small("kv_type", KeyType::Long),
    )
    .await?;
    let served = verify::serve_catalog(&fixture, &catalog, Residency::Full, false).await?;
    let before = served.storage.stats().gets();
    let error = served
        .table
        .get(&Key::Str(OTHER_TYPE_KEY.to_string()))
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            Error::KeyType {
                expected: KeyType::Long,
                got: KeyType::String,
                ..
            }
        ),
        "{error:?}"
    );
    assert_eq!(
        served.storage.stats().gets(),
        before,
        "refused before any read"
    );
    Ok(())
}

/// The cold-cost assertion measures nothing on a cache that has served: a
/// table with one lookup behind it is refused with the cache's counters.
#[tokio::test]
async fn test_verify_hit_cost_refuses_a_warm_cache() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: IDS_4K,
        ..FixtureSpec::small("kv_warm", KeyType::Long)
    };
    let fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    let served = verify::serve_catalog(&fixture, &catalog, Residency::Full, false).await?;
    served.table.get(&fixture.key(SAMPLE_4K[0])).await?;
    let error = verify::verify_hit_cost(served, &fixture, &SAMPLE_4K)
        .await
        .err()
        .unwrap();
    assert!(
        matches!(
            error,
            TestkitError::Precondition(Precondition::CacheNotCold {
                hits: 0,
                fetches: 1
            })
        ),
        "{error:?}"
    );
    Ok(())
}

/// A sorted table without a bucket partition serves by exact key ranges
/// alone, at the Full tier's GET costs.
#[tokio::test]
async fn test_unpartitioned_table_serves() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        buckets: 0,
        layout: Layout {
            target_file_bytes: UNPARTITIONED_FILE_BYTES,
            ..Layout::contract()
        },
        ..FixtureSpec::small("kv_flat", KeyType::Long)
    };
    let fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    let served = verify::serve_catalog(&fixture, &catalog, Residency::Full, false).await?;
    let generation = served.current();
    assert_eq!(generation.layout.bucketing, None);
    assert!(
        generation.files.len() > 1,
        "{} files",
        generation.files.len()
    );
    for key in fixture.keys_at(&SAMPLE) {
        assert_eq!(generation.candidates(&key).count(), 1, "key {key}");
    }
    assert_sample_pages_distinct(&fixture, &generation)?;
    let served = verify::verify_hit_cost(served, &fixture, &SAMPLE).await?;
    verify::verify_misses(&served, &fixture, &SAMPLE).await
}

/// Keys over the advice length serve at one GET per hit.
#[tokio::test]
async fn test_long_keys_serve_at_one_get_per_hit() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        key_pad: LONG_KEY_PAD,
        ids: IDS_4K,
        ..FixtureSpec::small("kv_longkeys", KeyType::String)
    };
    let fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    assert_eq!(fixture.key(0).as_bytes().len(), LONG_KEY_BYTES);
    let served = verify::serve_catalog(&fixture, &catalog, Residency::Full, false).await?;
    let served = verify::verify_hit_cost(served, &fixture, &SAMPLE_4K).await?;
    verify::verify_misses(&served, &fixture, &SAMPLE_4K).await
}

/// Scattered string keys route, serve at one GET per hit, and miss beside
/// every sampled key.
#[tokio::test]
async fn test_scattered_keys_serve_at_one_get_per_hit() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        key_shape: KeyShape::Scattered,
        ..FixtureSpec::small("kv_scattered", KeyType::String)
    };
    let fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    assert_eq!(fixture.key(SAMPLE[1]), scattered_key_at(SAMPLE[1], 0));
    let served = verify::serve_catalog(&fixture, &catalog, Residency::Full, false).await?;
    let served = verify::verify_hit_cost(served, &fixture, &SAMPLE).await?;
    verify::verify_misses(&served, &fixture, &SAMPLE).await
}

/// A scattered shape on a key other than a string, and a scattered id
/// with no key, are refused.
#[tokio::test]
async fn test_scattered_keys_refuse_other_types_and_ids_outside_the_space() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let long = FixtureSpec {
        key_shape: KeyShape::Scattered,
        ..FixtureSpec::small("kv_scattered_long", KeyType::Long)
    };
    let refused = write_fixture(catalog.as_ref(), NAMESPACE, long).await;
    assert!(matches!(
        refused,
        Err(TestkitError::Spec(SpecFault::ScatteredNonStringKey {
            key_type: KeyType::Long
        }))
    ));
    let beyond = FixtureSpec {
        key_shape: KeyShape::Scattered,
        ids: SCATTERED_ID_SPACE - 1..SCATTERED_ID_SPACE + 1,
        ..FixtureSpec::small("kv_scattered_beyond", KeyType::String)
    };
    let refused = write_fixture(catalog.as_ref(), NAMESPACE, beyond).await;
    assert!(matches!(
        refused,
        Err(TestkitError::Spec(SpecFault::IdOutsideScatteredSpace { id }))
            if id == SCATTERED_ID_SPACE
    ));
    Ok(())
}

/// A null value is served as present with null, distinct from absent, at
/// the same GET count as a value.
#[tokio::test]
async fn test_null_values_are_present_with_null() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        nulls: Nulls::EveryNth(NULL_EVERY),
        ..FixtureSpec::small("kv_nulls", KeyType::Long)
    };
    let fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    assert!(SAMPLE.iter().any(|id| id % NULL_EVERY == 0));
    assert!(SAMPLE.iter().any(|id| id % NULL_EVERY != 0));
    let served = verify::serve_catalog(&fixture, &catalog, Residency::Full, false).await?;
    verify::verify_hit_cost(served, &fixture, &SAMPLE).await?;

    let served = verify::serve_catalog(&fixture, &catalog, Residency::Full, false).await?;
    let stats = served.storage.stats();
    let null_key = fixture.key(NULL_ID);
    assert_eq!(fixture.expected(&null_key), Value::Null);
    let before = stats.gets();
    assert_eq!(served.table.get(&null_key).await?.value, Value::Null);
    assert_eq!(stats.gets() - before, 1, "a null costs its value page cold");
    let before = stats.gets();
    assert_eq!(served.table.get(&null_key).await?.value, Value::Null);
    assert_eq!(stats.gets(), before, "and nothing warm");
    let present = fixture.key(PRESENT_ID);
    assert!(matches!(
        served.table.get(&present).await?.value,
        Value::Present(_)
    ));
    assert_eq!(
        served
            .table
            .get(&fixture.absent_near(PRESENT_ID))
            .await?
            .value,
        Value::Absent
    );
    Ok(())
}

/// Large values: with the row limit alone a hit fetches a page of about a
/// thousand values; with a byte bound as well, a few. Every cold hit is at
/// most one GET either way, and the doctor points at the first layout. In
/// both layouts every loaded footer keeps the key column's column index
/// and none of the value column's, whose many byte-bounded pages would
/// otherwise carry a truncated minimum and maximum each into memory.
#[tokio::test]
async fn test_large_values_row_bounded_and_byte_bounded() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let mut per_layout = Vec::new();
    let byte_bound = length(VALUE_PAGE_BYTES);
    for (name, page_bytes) in [("kv_wide_rows", None), ("kv_wide_bytes", Some(byte_bound))] {
        let spec = FixtureSpec {
            value_bytes: WIDE_VALUE_BYTES,
            buckets: FOUR_BUCKETS,
            ids: IDS_4K,
            layout: Layout {
                page_bytes,
                ..Layout::contract()
            },
            ..FixtureSpec::small(name, KeyType::Long)
        };
        let fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
        let served = verify::serve_catalog(&fixture, &catalog, Residency::Full, false).await?;
        let stats = served.storage.stats();
        let (gets_before, bytes_before) = (stats.gets(), stats.bytes());
        for id in SAMPLE_4K {
            let key = fixture.key(id);
            let before = stats.gets();
            assert_eq!(served.table.get(&key).await?.value, fixture.expected(&key));
            assert!(stats.gets() - before <= 1, "a hit is at most one GET");
        }
        let gets = stats.gets() - gets_before;
        assert!(gets >= 1);
        let bytes_per_get = (stats.bytes() - bytes_before) / gets;
        let generation = served.current();
        let report = diagnose(&generation);
        let flagged = report
            .findings
            .iter()
            .any(|finding| finding.code() == FindingCode::LargeValuePages);
        for file in &generation.files {
            let column_index = file
                .index
                .metadata()
                .column_index()
                .expect("the contract layout writes a column index");
            for row_group in column_index {
                assert_eq!(
                    row_group[file.index.value_leaf()],
                    ColumnIndexMetaData::NONE,
                    "{}: the value column's column index is dropped on load",
                    file.entry.path
                );
                assert_ne!(
                    row_group[file.index.key_leaf()],
                    ColumnIndexMetaData::NONE,
                    "{}: the key column's is kept",
                    file.entry.path
                );
            }
        }
        per_layout.push((bytes_per_get, flagged));
    }
    let (row_bounded, row_bounded_flagged) = per_layout[0];
    let (byte_bounded, byte_bounded_flagged) = per_layout[1];
    assert!(
        row_bounded >= row_bounded_page_min_bytes(),
        "row-limited pages carry about a thousand values: {row_bounded} bytes per GET"
    );
    assert!(
        byte_bounded < BYTE_BOUNDED_PAGE_MAX_BYTES,
        "byte-limited pages carry a few values: {byte_bounded} bytes per GET"
    );
    assert!(
        row_bounded_flagged,
        "the doctor flags the row-limited table"
    );
    assert!(!byte_bounded_flagged, "and not the byte-limited one");
    Ok(())
}
