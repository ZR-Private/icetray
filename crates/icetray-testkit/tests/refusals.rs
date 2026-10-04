//! One fixture per `RefusalReason` variant (RFD 007, RFD 017): each fails
//! its first load with the named reason and leaves the table with no
//! generation, `Degraded` with nothing serving.
//! The variant set itself is bound in core: the label and message-detail
//! matches in `error.rs` name every variant, so a new one fails to compile
//! there; one fixture per variant here is by convention, as for findings.

use std::ops::Range;
use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryArray, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use iceberg::Catalog;
use iceberg::spec::{PrimitiveType, Struct};
use icetray_core::approx::count;
use icetray_core::config::{ConfigError, ConfigViolation, Residency, TableConfig};
use icetray_core::contract::MAX_KEY_BYTES;
use icetray_core::{
    Error, ErrorKind, Key, KeyType, RefusalReason, Storage, TableSource, TableState, bucket,
};
use icetray_testkit::fixture::{
    DATA_DIR, IDS_2K, IDS_4K, NAMESPACE, ONE_BUCKET, OVERLAPPING_APPEND, STRAY_IDS, TINY_BUDGET,
    TWO_BUCKETS, local_catalog, position_delete_file, write_table_of_types,
};
use icetray_testkit::layout::{KEY_COLUMN, VALUE_COLUMN};
use icetray_testkit::verify::{self, LOAD_METADATA_GETS};
use icetray_testkit::{
    Bloom, Fixture, FixtureSpec, HandSnapshot, Layout, Order, RawFile, Result, value_for,
    write_fixture,
};

/// The long keys of a raw file, ascending, and the same values as their
/// arrow arrays for a raw file whose key column is of another type.
const RAW_KEYS: [i64; 3] = [0, 3, 6];
const RAW_STRING_KEYS: [&str; 3] = ["a", "b", "c"];
const RAW_INT_KEYS: [i32; 3] = [0, 3, 6];

/// The position of the null in the null-key raw file.
const NULL_KEY_AT: usize = 1;

/// The name of every raw file under its table's data directory, and of the
/// delete file a by-hand snapshot lists.
const RAW_FILE: &str = "raw.parquet";
const DELETE_FILE: &str = "deletes.parquet";

/// Records and bytes the manifest records for the delete file that need
/// not exist.
const DELETE_RECORDS: u64 = 1;
const DELETE_BYTES: u64 = 1;

/// A key pad that alone exceeds the cap, so every key is over it.
const OVER_CAP_PAD: usize = MAX_KEY_BYTES;

/// Rows of the over-cap fixture: few, each a few kibibytes, in one bucket.
const OVER_CAP_IDS: Range<u64> = 0..300;

/// A table whose first load was refused: the reason, and the storage left
/// behind for the GET assertions.
struct Refused {
    reason: RefusalReason,
    storage: Arc<Storage>,
}

/// Builds `config` from `source` over hedge-free storage and no local
/// directory and loads it, expecting a refusal; asserts the table has no
/// generation and is `Degraded` with nothing serving and the refusal as its
/// reason.
async fn load_refused(config: TableConfig, source: TableSource) -> Result<Refused> {
    let served = verify::build(
        Arc::new(Storage::new(verify::storage_config_for_tests())),
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
    let Error::Refused { reason, .. } = error else {
        panic!("expected a refusal, got {error:?}");
    };
    assert!(
        served.table.current().is_none(),
        "no generation after a refused first load"
    );
    assert!(
        matches!(
            served.table.state(),
            TableState::Degraded {
                serving: None,
                reason: ErrorKind::Refused(ref degraded),
                ..
            } if *degraded == reason
        ),
        "{:?}",
        served.table.state()
    );
    Ok(Refused {
        reason,
        storage: served.storage,
    })
}

/// `load_refused` of `fixture` through its catalog under `config`.
async fn refused_through_catalog(
    fixture: &Fixture,
    catalog: &Arc<dyn Catalog>,
    config: TableConfig,
) -> Result<Refused> {
    load_refused(config, fixture.catalog_source(Arc::clone(catalog))).await
}

/// A raw file of three rows whose key column is `key` and whose value
/// column holds `values`, under the contract layout with no manifest
/// bounds.
fn raw_file(key: ArrayRef, values: &[Vec<u8>]) -> Result<RawFile> {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new(KEY_COLUMN, key.data_type().clone(), key.is_nullable()),
        Field::new(VALUE_COLUMN, DataType::Binary, true),
    ]));
    let value: ArrayRef = Arc::new(BinaryArray::from_iter_values(values.iter()));
    let batch = RecordBatch::try_new(schema, vec![key, value])?;
    RawFile::from_batch(RAW_FILE, &batch, &Layout::contract(), Struct::empty(), None)
}

/// The values of `RAW_KEYS`.
fn raw_values() -> Vec<Vec<u8>> {
    RAW_KEYS
        .iter()
        .map(|&key| value_for(&Key::Long(key)))
        .collect()
}

#[tokio::test]
async fn test_unsorted_file_is_refused() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: IDS_4K,
        order: Order::Descending,
        ..FixtureSpec::small("kv_unsorted", KeyType::Long)
    };
    let fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    let refused =
        refused_through_catalog(&fixture, &catalog, fixture.config(Residency::Full, false)).await?;
    let RefusalReason::Unsorted { path, .. } = &refused.reason else {
        panic!("{:?}", refused.reason);
    };
    assert!(
        fixture.files().iter().any(|file| file.file_path() == path),
        "{path} is one of the fixture's files"
    );
    Ok(())
}

#[tokio::test]
async fn test_null_key_is_refused() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let keys: Vec<Option<i64>> = RAW_KEYS
        .iter()
        .enumerate()
        .map(|(at, &key)| (at != NULL_KEY_AT).then_some(key))
        .collect();
    let raw = raw_file(Arc::new(Int64Array::from(keys)), &raw_values())?;
    let fixture = write_table_of_types(
        catalog.as_ref(),
        NAMESPACE,
        "kv_nullkey",
        PrimitiveType::Long,
        PrimitiveType::Binary,
        0,
        raw,
    )
    .await?;
    let refused =
        refused_through_catalog(&fixture, &catalog, fixture.config(Residency::Full, false)).await?;
    assert!(
        matches!(refused.reason, RefusalReason::NullKeys { .. }),
        "{:?}",
        refused.reason
    );
    Ok(())
}

#[tokio::test]
async fn test_duplicate_keys_across_files_are_refused() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: IDS_4K,
        ..FixtureSpec::small("kv_dupfiles", KeyType::Long)
    };
    let mut fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    fixture
        .append(catalog.as_ref(), OVERLAPPING_APPEND, "build2")
        .await?;
    let refused =
        refused_through_catalog(&fixture, &catalog, fixture.config(Residency::Full, false)).await?;
    assert!(
        matches!(refused.reason, RefusalReason::DuplicateKeys { .. }),
        "{:?}",
        refused.reason
    );
    Ok(())
}

#[tokio::test]
async fn test_key_physical_type_mismatch_is_refused() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let raw = raw_file(
        Arc::new(StringArray::from(RAW_STRING_KEYS.to_vec())),
        &raw_values(),
    )?;
    let fixture = write_table_of_types(
        catalog.as_ref(),
        NAMESPACE,
        "kv_mismatch",
        PrimitiveType::Long,
        PrimitiveType::Binary,
        0,
        raw,
    )
    .await?;
    let refused =
        refused_through_catalog(&fixture, &catalog, fixture.config(Residency::Full, false)).await?;
    assert!(
        matches!(
            refused.reason,
            RefusalReason::KeyTypeMismatch {
                expected: KeyType::Long,
                got: parquet::basic::Type::BYTE_ARRAY,
                ..
            }
        ),
        "{:?}",
        refused.reason
    );
    Ok(())
}

/// The schema refuses before any file is read; the raw file exists only so
/// the branch has a snapshot.
#[tokio::test]
async fn test_unsupported_key_type_is_refused() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let raw = raw_file(
        Arc::new(Int32Array::from(RAW_INT_KEYS.to_vec())),
        &raw_values(),
    )?;
    let fixture = write_table_of_types(
        catalog.as_ref(),
        NAMESPACE,
        "kv_intkey",
        PrimitiveType::Int,
        PrimitiveType::Binary,
        0,
        raw,
    )
    .await?;
    let refused =
        refused_through_catalog(&fixture, &catalog, fixture.config(Residency::Full, false)).await?;
    assert_eq!(refused.reason, RefusalReason::KeyTypeUnsupported);
    assert_eq!(
        refused.storage.stats().gets(),
        0,
        "refused from the schema alone"
    );
    Ok(())
}

#[tokio::test]
async fn test_unsupported_value_type_is_refused() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let key: ArrayRef = Arc::new(Int64Array::from(RAW_KEYS.to_vec()));
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new(KEY_COLUMN, DataType::Int64, false),
        Field::new(VALUE_COLUMN, DataType::Int64, true),
    ]));
    let value: ArrayRef = Arc::new(Int64Array::from(RAW_KEYS.to_vec()));
    let batch = RecordBatch::try_new(schema, vec![key, value])?;
    let raw = RawFile::from_batch(RAW_FILE, &batch, &Layout::contract(), Struct::empty(), None)?;
    let fixture = write_table_of_types(
        catalog.as_ref(),
        NAMESPACE,
        "kv_longvalue",
        PrimitiveType::Long,
        PrimitiveType::Long,
        0,
        raw,
    )
    .await?;
    let refused =
        refused_through_catalog(&fixture, &catalog, fixture.config(Residency::Full, false)).await?;
    assert!(
        matches!(refused.reason, RefusalReason::ValueTypeUnsupported { .. }),
        "{:?}",
        refused.reason
    );
    assert_eq!(
        refused.storage.stats().gets(),
        0,
        "refused from the schema alone"
    );
    Ok(())
}

#[tokio::test]
async fn test_delete_file_is_refused() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: IDS_2K,
        buckets: 0,
        ..FixtureSpec::small("kv_deletes", KeyType::Long)
    };
    let mut fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    let delete = position_delete_file(
        &format!("{}/{DATA_DIR}/{DELETE_FILE}", fixture.root()),
        fixture.metadata().default_partition_spec_id(),
        Struct::empty(),
        DELETE_RECORDS,
        DELETE_BYTES,
    )?;
    fixture
        .commit_by_hand(HandSnapshot {
            data: fixture.files().to_vec(),
            deletes: vec![delete],
        })
        .await?;
    let source = fixture.root_source();
    let refused = load_refused(
        TableConfig {
            source: source.clone(),
            ..fixture.config(Residency::Full, false)
        },
        TableSource::from_config(&source).await?,
    )
    .await?;
    assert_eq!(refused.reason, RefusalReason::DeleteFiles);
    Ok(())
}

#[tokio::test]
async fn test_key_over_the_cap_is_refused() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        key_pad: OVER_CAP_PAD,
        ids: OVER_CAP_IDS,
        buckets: ONE_BUCKET,
        ..FixtureSpec::small("kv_hugekeys", KeyType::String)
    };
    let fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    let refused =
        refused_through_catalog(&fixture, &catalog, fixture.config(Residency::Full, false)).await?;
    let RefusalReason::KeyTooLong { bytes, .. } = refused.reason else {
        panic!("{:?}", refused.reason);
    };
    assert!(bytes > MAX_KEY_BYTES, "{bytes} bytes");
    Ok(())
}

/// A raw file whose keys all hash to bucket 0, recorded under bucket 1:
/// its first key is a fence, so the load refuses it rather than answer its
/// keys `Absent`.
#[tokio::test]
async fn test_file_recorded_under_another_bucket_is_refused() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: IDS_2K,
        buckets: TWO_BUCKETS,
        ..FixtureSpec::small("kv_misbucketed", KeyType::Long)
    };
    let mut fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    let (hashed, recorded) = (0, 1);
    let ids: Vec<u64> = STRAY_IDS
        .filter(|&id| bucket(&fixture.key(id), TWO_BUCKETS) == hashed)
        .collect();
    let raw = fixture.raw_file_in_bucket(RAW_FILE, &ids, i32::try_from(recorded).unwrap())?;
    fixture.append_raw(catalog.as_ref(), raw).await?;
    let refused =
        refused_through_catalog(&fixture, &catalog, fixture.config(Residency::Full, false)).await?;
    let RefusalReason::KeyOutsideBucket {
        path,
        key,
        recorded: recorded_at,
        computed,
    } = &refused.reason
    else {
        panic!("{:?}", refused.reason);
    };
    assert_eq!(path, &format!("{}/{DATA_DIR}/{RAW_FILE}", fixture.root()));
    assert_eq!(key, &fixture.key(ids[0]), "the first key, a fence");
    assert_eq!((*recorded_at, *computed), (recorded, hashed));
    Ok(())
}

/// The budget is checked after every footer and before any chunk.
#[tokio::test]
async fn test_full_tier_over_budget_is_refused_at_the_footer_stage() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let fixture = write_fixture(
        catalog.as_ref(),
        NAMESPACE,
        FixtureSpec::small("kv_budget", KeyType::Long),
    )
    .await?;
    let refused = refused_through_catalog(
        &fixture,
        &catalog,
        fixture.config_with_budget(Residency::Full, true, Some(TINY_BUDGET)),
    )
    .await?;
    let RefusalReason::OverBudget { needed, budget } = refused.reason else {
        panic!("{:?}", refused.reason);
    };
    assert_eq!(budget, TINY_BUDGET);
    assert!(needed > budget);
    assert_eq!(
        refused.storage.stats().gets(),
        LOAD_METADATA_GETS + count(fixture.file_count()),
        "every footer and no chunk"
    );
    Ok(())
}

#[tokio::test]
async fn test_bloom_tier_without_a_filter_is_refused() -> Result<()> {
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
    let fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    let refused =
        refused_through_catalog(&fixture, &catalog, fixture.config(Residency::Bloom, false))
            .await?;
    assert!(
        matches!(refused.reason, RefusalReason::NoBloomFilter { .. }),
        "{:?}",
        refused.reason
    );
    Ok(())
}

/// Disk residency with no local directory is not a refusal but the
/// configuration violation `Config::parse` refuses; a configuration built
/// by hand, as this one is, meets it at the loader instead, before any
/// chunk is fetched, and the table is left as a refused first load leaves
/// it: no generation, `Degraded` with the failure's kind.
#[tokio::test]
async fn test_disk_tier_without_a_local_directory_is_a_config_violation() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: IDS_2K,
        buckets: TWO_BUCKETS,
        ..FixtureSpec::small("kv_nolocal", KeyType::Long)
    };
    let fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    let served = verify::build(
        Arc::new(Storage::new(verify::storage_config_for_tests())),
        fixture.config(Residency::Disk, false),
        fixture.catalog_source(Arc::clone(&catalog)),
        None,
        verify::TEST_PAGE_CACHE_BYTES,
    )
    .await?;
    let error = served
        .table
        .load()
        .await
        .expect_err("the fixture is designed to be refused");
    assert!(
        matches!(
            error,
            Error::Config(ConfigError::Invalid(
                ConfigViolation::DiskResidencyWithoutLocalDir { .. }
            ))
        ),
        "{error:?}"
    );
    assert!(served.table.current().is_none());
    assert!(
        matches!(
            served.table.state(),
            TableState::Degraded {
                serving: None,
                reason: ErrorKind::Config,
                ..
            }
        ),
        "{:?}",
        served.table.state()
    );
    Ok(())
}
