//! The ref moves and the table follows (RFD 005): a second snapshot appends
//! files, the table advances on refresh reusing every unchanged file, a
//! duplicate key is refused while the last generation keeps serving, and
//! overlapping files are verified once per group that gained a file.

use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;

use icetray_core::check::Tier;
use icetray_core::config::Residency;
use icetray_core::{
    Error, ErrorKind, FindingCode, KeyType, RefusalReason, Serviceability, TableSource, TableState,
    Value, diagnose,
};
use icetray_testkit::fixture::{
    APPENDED_4K_6K, IDS_4K, INTERLEAVED_BUCKETS, INTERLEAVED_SAMPLE, NAMESPACE, NEW_ID,
    NEWEST_SAMPLE, OLD_ID, OVERLAPPING_APPEND, SAMPLE_4K, SECOND_CLASS, TAIL, THIRD_CLASS,
    THREE_FILES, TWO_FILES, local_catalog, residue_class,
};
use icetray_testkit::verify::{self, HINT_POLL_GETS, LOAD_GETS_PER_FILE, Served};
use icetray_testkit::{Fixture, FixtureSpec, HandSnapshot, Result, TestkitError, write_fixture};

/// The first build of the reuse fixture and the range its second appends.
const IDS_8K: Range<u64> = 0..8_000;
const APPENDED_8K_12K: Range<u64> = 8_000..12_000;

/// An id the second build adds, absent before it.
const FUTURE_ID: u64 = 9_000;

/// Refresh GETs beyond the per-file ones after one append through the
/// catalog: the manifest list and two manifests.
const REFRESH_METADATA_GETS: u64 = 3;

/// Files of the interleaved fixture after the second class: two per bucket.
const INTERLEAVED_FILES: usize = 8;

/// Ids written by the first two classes.
const INTERLEAVED_WRITTEN: usize = 4_000;

/// `fixture` served in the Full tier from its root, through the hint.
async fn serve_root(fixture: &Fixture) -> Result<Served> {
    let source = fixture.root_source();
    verify::serve(
        icetray_core::config::TableConfig {
            source: source.clone(),
            ..fixture.config(Residency::Full, false)
        },
        TableSource::from_config(&source).await?,
        verify::storage_config_for_tests(),
    )
    .await
}

#[tokio::test]
async fn test_refresh_follows_the_catalog_and_reuses_files() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: IDS_8K,
        ..FixtureSpec::small("kv_adv", KeyType::Long)
    };
    let mut fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    let served = verify::serve_catalog(&fixture, &catalog, Residency::Full, false).await?;
    let v1 = served.current();
    let new_key = fixture.key(FUTURE_ID);
    assert_eq!(served.table.get(&new_key).await?.value, Value::Absent);
    assert!(!served.table.refresh().await?, "nothing moved yet");

    fixture
        .append(catalog.as_ref(), APPENDED_8K_12K, "build2")
        .await?;
    let stats = served.storage.stats();
    let before = stats.gets();
    assert!(served.table.refresh().await?, "the ref moved");
    let v2 = served.current();
    assert_ne!(v2.snapshot_id, v1.snapshot_id);
    assert_eq!(v2.files.len(), 2 * v1.files.len());
    let reused = v2
        .files
        .iter()
        .filter(|file| v1.files.iter().any(|old| Arc::ptr_eq(old, file)))
        .count();
    assert_eq!(reused, v1.files.len(), "v1's files are shared by pointer");
    let new_files = u64::try_from(v2.files.len() - reused).unwrap();
    assert_eq!(
        stats.gets() - before,
        REFRESH_METADATA_GETS + LOAD_GETS_PER_FILE * new_files,
        "manifest list, two manifests, then a footer and a chunk per new file"
    );

    let lookup = served.table.get(&new_key).await?;
    assert_eq!(lookup.snapshot_id, v2.snapshot_id);
    assert_eq!(lookup.value, fixture.expected(&new_key));
    let old_key = fixture.key(OLD_ID);
    assert_eq!(
        served.table.get(&old_key).await?.value,
        fixture.expected(&old_key)
    );
    Ok(())
}

#[tokio::test]
async fn test_refresh_follows_a_static_root_through_its_hint() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: IDS_4K,
        ..FixtureSpec::small("kv_hint", KeyType::Long)
    };
    let mut fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    fixture.write_hint().await?;
    let served = serve_root(&fixture).await?;
    let v1 = served.current();
    assert_eq!(v1.metadata_location, fixture.metadata_location());

    fixture
        .append(catalog.as_ref(), APPENDED_4K_6K, "build2")
        .await?;
    let stats = served.storage.stats();
    let before = stats.gets();
    assert!(!served.table.refresh().await?, "the hint still names v1");
    assert_eq!(stats.gets() - before, HINT_POLL_GETS);

    fixture.write_hint().await?;
    assert!(served.table.refresh().await?);
    assert_ne!(served.current().snapshot_id, v1.snapshot_id);
    let new_key = fixture.key(NEW_ID);
    assert_eq!(
        served.table.get(&new_key).await?.value,
        fixture.expected(&new_key)
    );
    Ok(())
}

#[tokio::test]
async fn test_refresh_refuses_duplicate_keys_and_keeps_serving() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: IDS_4K,
        ..FixtureSpec::small("kv_dup", KeyType::Long)
    };
    let mut fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    let served = verify::serve_catalog(&fixture, &catalog, Residency::Full, false).await?;
    let v1 = served.current().snapshot_id;

    fixture
        .append(catalog.as_ref(), OVERLAPPING_APPEND, "build2")
        .await?;
    let error = served.table.refresh().await.unwrap_err();
    assert!(
        matches!(
            error,
            Error::Refused {
                reason: RefusalReason::DuplicateKeys { .. },
                ..
            }
        ),
        "{error:?}"
    );
    assert_eq!(served.current().snapshot_id, v1);
    let old_key = fixture.key(OLD_ID);
    assert_eq!(
        served.table.get(&old_key).await?.value,
        fixture.expected(&old_key)
    );
    assert!(
        matches!(
            served.table.state(),
            TableState::Degraded {
                serving: Some(serving),
                reason: ErrorKind::Refused(RefusalReason::DuplicateKeys { .. }),
                ..
            } if serving == v1
        ),
        "{:?}",
        served.table.state()
    );
    Ok(())
}

#[tokio::test]
async fn test_refresh_recovers_when_the_offending_file_is_dropped() -> Result<()> {
    let (_dir, catalog) = local_catalog().await?;
    let spec = FixtureSpec {
        ids: IDS_4K,
        ..FixtureSpec::small("kv_recover", KeyType::Long)
    };
    let mut fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    fixture.write_hint().await?;
    let served = serve_root(&fixture).await?;
    let v1 = served.current();
    let v1_paths: HashSet<String> = v1
        .files
        .iter()
        .map(|file| file.entry.path.clone())
        .collect();

    fixture.append(catalog.as_ref(), [OLD_ID], "dup").await?;
    fixture.write_hint().await?;
    let error = served.table.refresh().await.unwrap_err();
    assert!(
        matches!(
            error,
            Error::Refused {
                reason: RefusalReason::DuplicateKeys { .. },
                ..
            }
        ),
        "{error:?}"
    );
    assert_eq!(fixture.file_count(), v1.files.len() + 1);

    let data: Vec<_> = fixture
        .files()
        .iter()
        .filter(|file| v1_paths.contains(file.file_path()))
        .cloned()
        .collect();
    assert_eq!(
        data.len(),
        v1.files.len(),
        "the one duplicating file is dropped"
    );
    fixture
        .commit_by_hand(HandSnapshot {
            data,
            deletes: Vec::new(),
        })
        .await?;
    assert!(served.table.refresh().await?);
    assert!(matches!(served.table.state(), TableState::Serving { .. }));
    assert_ne!(served.current().snapshot_id, v1.snapshot_id);
    for id in SAMPLE_4K {
        let key = fixture.key(id);
        assert_eq!(served.table.get(&key).await?.value, fixture.expected(&key));
    }
    let appended_error = fixture
        .append(catalog.as_ref(), [OLD_ID], "again")
        .await
        .unwrap_err();
    assert!(matches!(
        appended_error,
        TestkitError::Spec(icetray_testkit::error::SpecFault::CatalogBehind)
    ));
    Ok(())
}

/// Files in one bucket whose key ranges interleave but share no key are
/// served at the cost of probing each; the doctor prices it and `strict`
/// refuses it. Uniqueness is proved once per group: a range-disjoint append
/// verifies nothing again, an interleaving one verifies its groups.
#[tokio::test]
async fn test_overlapping_files_serve_and_reverify_only_grown_groups() -> Result<()> {
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
    let generation = served.current();
    assert_eq!(
        generation.files.len(),
        INTERLEAVED_FILES,
        "two files per bucket"
    );
    assert_eq!(
        generation.overlapping_buckets().len(),
        usize::try_from(INTERLEAVED_BUCKETS).unwrap()
    );
    assert!(
        generation
            .overlapping_buckets()
            .values()
            .all(|&files| files == TWO_FILES)
    );
    assert_eq!(
        generation.overlap_groups_verified(),
        usize::try_from(INTERLEAVED_BUCKETS).unwrap()
    );
    let served = verify::verify_hit_cost(served, &fixture, &INTERLEAVED_SAMPLE).await?;
    verify::verify_misses(&served, &fixture, &INTERLEAVED_SAMPLE).await?;
    assert_eq!(fixture.ids().count(), INTERLEAVED_WRITTEN);

    let report = diagnose(&generation);
    assert_eq!(report.summary.serviceability, Serviceability::Inefficient);
    let overlap = report
        .findings
        .iter()
        .find(|finding| finding.code == FindingCode::OverlappingFiles)
        .expect("priced as a cost");
    assert_eq!(overlap.tier, Tier::Cost);
    assert_eq!(
        overlap
            .penalty
            .and_then(|penalty| penalty.files_probed_per_lookup),
        Some(TWO_FILES)
    );

    fixture.append(catalog.as_ref(), TAIL, "tail").await?;
    assert!(served.table.refresh().await?);
    let generation = served.current();
    assert_eq!(
        generation.overlap_groups_verified(),
        0,
        "disjoint files join no group"
    );
    assert!(
        generation
            .overlapping_buckets()
            .values()
            .all(|&files| files == TWO_FILES)
    );

    fixture
        .append(catalog.as_ref(), residue_class(THIRD_CLASS), "quarter")
        .await?;
    assert!(served.table.refresh().await?);
    let generation = served.current();
    assert_eq!(
        generation.overlap_groups_verified(),
        usize::try_from(INTERLEAVED_BUCKETS).unwrap()
    );
    assert!(
        generation
            .overlapping_buckets()
            .values()
            .all(|&files| files == THREE_FILES)
    );
    let fresh = verify::serve_catalog(&fixture, &catalog, Residency::Full, false).await?;
    verify::verify_hit_cost(fresh, &fixture, &NEWEST_SAMPLE).await?;
    assert_eq!(
        generation.records,
        u64::try_from(fixture.ids().count()).unwrap()
    );

    let error = verify::serve_catalog(&fixture, &catalog, Residency::Full, true)
        .await
        .err()
        .unwrap();
    let TestkitError::Core(Error::Inefficient { findings, .. }) = error else {
        panic!("strict refuses overlapping files: {error:?}");
    };
    assert_eq!(findings, vec![FindingCode::OverlappingFiles]);
    Ok(())
}
