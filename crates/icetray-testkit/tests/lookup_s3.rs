//! Serving a fixture from the in-process S3 server (the P tier of RFD 014
//! over HTTP): the whole read path through the real `object_store` client,
//! every GET counted where it is issued.

use std::sync::Arc;

use iceberg::Catalog;
use icetray_core::KeyType;
use icetray_core::config::Residency;
use icetray_testkit::verify::{self, count};
use icetray_testkit::{FixtureSpec, Result, S3Fixture, memory_catalog_s3, write_fixture};

/// The namespace every fixture is written in.
const NAMESPACE: &str = "ns";

/// The fixture server's bucket and the warehouse prefix inside it.
const BUCKET: &str = "icetray-test";
const WAREHOUSE: &str = "warehouse";

/// Ids sampled on the small fixture, spread over its buckets.
const SAMPLE: [u64; 8] = [0, 999, 1_000, 4_242, 9_999, 12_345, 16_000, 19_999];

/// Load GETs beyond the per-file ones over a catalog source: the manifest
/// list and the one manifest; the metadata came from the catalog.
const LOAD_METADATA_GETS: u64 = 2;

/// Load GETs per single-row-group file: a footer and a key chunk.
const LOAD_GETS_PER_FILE: u64 = 2;

#[tokio::test]
async fn test_get_over_s3() -> Result<()> {
    let s3 = S3Fixture::start(BUCKET).await?;
    let catalog: Arc<dyn Catalog> = memory_catalog_s3(&s3, WAREHOUSE).await?;
    let fixture = write_fixture(
        catalog.as_ref(),
        NAMESPACE,
        FixtureSpec::small("kv_s3", KeyType::Long),
    )
    .await?;
    assert!(
        fixture.metadata_location().starts_with(&s3.uri(WAREHOUSE)),
        "metadata lives in the bucket: {}",
        fixture.metadata_location()
    );
    let served = verify::serve_catalog_with(
        &fixture,
        &catalog,
        Residency::Full,
        false,
        s3.storage_config(),
    )
    .await?;
    let files = count(served.current().files.len());
    assert_eq!(
        served.storage.stats().gets(),
        LOAD_METADATA_GETS + LOAD_GETS_PER_FILE * files,
        "manifest list and manifest, then a footer and a key chunk per file"
    );
    let served = verify::verify_hit_cost(served, &fixture, &SAMPLE).await?;
    verify::verify_misses(&served, &fixture, &SAMPLE).await
}

#[tokio::test]
async fn test_refresh_on_a_catalog_source_reads_no_object() -> Result<()> {
    let s3 = S3Fixture::start(BUCKET).await?;
    let catalog: Arc<dyn Catalog> = memory_catalog_s3(&s3, WAREHOUSE).await?;
    let fixture = write_fixture(
        catalog.as_ref(),
        NAMESPACE,
        FixtureSpec::small("kv_s3_poll", KeyType::Long),
    )
    .await?;
    let served = verify::serve_catalog_with(
        &fixture,
        &catalog,
        Residency::Full,
        false,
        s3.storage_config(),
    )
    .await?;
    let before = served.storage.stats().gets();
    assert!(!served.table.refresh().await?, "nothing moved");
    assert_eq!(
        served.storage.stats().gets(),
        before,
        "the one read is the catalog record"
    );
    Ok(())
}
