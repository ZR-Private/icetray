//! The AWS catalog sources of RFD 019 against the stand-in catalog: a
//! table written to a local warehouse, its metadata location answered by
//! Glue's `GetTable` or S3 Tables' `GetTableMetadataLocation`, loads,
//! answers a lookup, polls with one catalog call and no object read while
//! the location holds, and follows the location when it moves. A table
//! Glue does not hold as Iceberg, or holds without a location, is refused.
//! Every call is signed with the storage's own static keys.

use icetray_core::config::{Residency, S3Config, SourceConfig, StorageConfig, TableConfig};
use icetray_core::{Error, KeyType, TableFault, TableSource};
use icetray_testkit::aws_catalog::{CatalogEntry, FakeAwsCatalog};
use icetray_testkit::fixture::{APPENDED_4K_6K, IDS_4K, NAMESPACE, NEW_ID, OLD_ID, local_catalog};
use icetray_testkit::layout::VALUE_COLUMN;
use icetray_testkit::verify::{self, Served};
use icetray_testkit::{Fixture, FixtureSpec, Result, TestkitError, write_fixture};

/// The access key id the storage signs with; the stand-in checks no
/// signature, so any pair serves.
const KEY_ID: &str = "AKIAICETRAYTEST";
/// The secret beside it.
const SECRET: &str = "icetray-test-secret";
/// The region the sources name.
const REGION: &str = "us-test-1";
/// The table bucket the `s3tables` source names.
const TABLE_BUCKET_ARN: &str = "arn:aws:s3tables:us-test-1:111122223333:bucket/warehouse";

/// Hedge-free test storage that signs with the static keys.
fn signed_storage() -> StorageConfig {
    StorageConfig {
        s3: Some(S3Config {
            access_key_id: Some(KEY_ID.to_string()),
            secret_access_key: Some(SECRET.to_string()),
            ..S3Config::default()
        }),
        ..verify::storage_config_for_tests()
    }
}

/// A Glue source for `fixture` in the stand-in.
fn glue(catalog: &FakeAwsCatalog, fixture: &Fixture) -> SourceConfig {
    SourceConfig::Glue {
        database: NAMESPACE.to_string(),
        table: fixture.ident().name().to_string(),
        catalog_id: None,
        region: Some(REGION.to_string()),
        endpoint: Some(catalog.url()),
    }
}

/// An S3 Tables source for `fixture` in the stand-in.
fn s3tables(catalog: &FakeAwsCatalog, fixture: &Fixture) -> SourceConfig {
    SourceConfig::S3Tables {
        table_bucket_arn: TABLE_BUCKET_ARN.to_string(),
        namespace: NAMESPACE.to_string(),
        table: fixture.ident().name().to_string(),
        region: None,
        endpoint: Some(catalog.url()),
    }
}

/// `fixture` served in the Full tier from `source`.
async fn serve(fixture: &Fixture, source: SourceConfig) -> Result<Served> {
    verify::serve(
        TableConfig {
            source: source.clone(),
            ..fixture.config(Residency::Full, false)
        },
        TableSource::from_config(&source).await?,
        signed_storage(),
    )
    .await
}

/// The fixture both success tests write: 4,000 long keys.
fn spec(name: &str) -> FixtureSpec {
    FixtureSpec {
        ids: IDS_4K,
        ..FixtureSpec::small(name, KeyType::Long)
    }
}

/// Asserts every call the stand-in answered was signed with `KEY_ID`.
fn assert_signed_with_storage_keys(catalog: &FakeAwsCatalog) {
    let key_ids = catalog.key_ids();
    assert!(!key_ids.is_empty(), "the catalog was called");
    assert!(key_ids.iter().all(|key_id| key_id == KEY_ID), "{key_ids:?}");
}

#[tokio::test]
async fn test_glue_source_loads_answers_polls_with_one_call_and_follows_a_move() -> Result<()> {
    let (_dir, writer) = local_catalog().await?;
    let mut fixture = write_fixture(writer.as_ref(), NAMESPACE, spec("kv_glue")).await?;
    let catalog = FakeAwsCatalog::start()?;
    let name = fixture.ident().name().to_string();
    catalog.set(
        NAMESPACE,
        &name,
        CatalogEntry::Iceberg(fixture.metadata_location().to_string()),
    );
    let served = serve(&fixture, glue(&catalog, &fixture)).await?;
    assert_eq!(catalog.calls(), 1, "the load asked the catalog once");
    let old_key = fixture.key(OLD_ID);
    assert_eq!(
        served.table.get(&old_key, &[VALUE_COLUMN]).await?.row,
        fixture.expected(&old_key)
    );

    let stats = served.storage.stats();
    let before = stats.gets();
    assert!(!served.table.refresh().await?, "nothing moved");
    assert_eq!(catalog.calls(), 2, "an unchanged poll is one call");
    assert_eq!(stats.gets(), before, "and reads no object");

    fixture
        .append(writer.as_ref(), APPENDED_4K_6K, "build2")
        .await?;
    catalog.set(
        NAMESPACE,
        &name,
        CatalogEntry::Iceberg(fixture.metadata_location().to_string()),
    );
    assert!(served.table.refresh().await?, "the location moved");
    assert_eq!(catalog.calls(), 3);
    let new_key = fixture.key(NEW_ID);
    assert_eq!(
        served.table.get(&new_key, &[VALUE_COLUMN]).await?.row,
        fixture.expected(&new_key)
    );
    assert!(verify::one_value(&served.table.get(&old_key, &[VALUE_COLUMN]).await?.row).is_some());
    assert_signed_with_storage_keys(&catalog);
    Ok(())
}

#[tokio::test]
async fn test_s3tables_source_loads_answers_and_polls_with_one_call() -> Result<()> {
    let (_dir, writer) = local_catalog().await?;
    let fixture = write_fixture(writer.as_ref(), NAMESPACE, spec("kv_s3tables")).await?;
    let catalog = FakeAwsCatalog::start()?;
    catalog.set(
        NAMESPACE,
        fixture.ident().name(),
        CatalogEntry::Iceberg(fixture.metadata_location().to_string()),
    );
    let served = serve(&fixture, s3tables(&catalog, &fixture)).await?;
    let old_key = fixture.key(OLD_ID);
    assert_eq!(
        served.table.get(&old_key, &[VALUE_COLUMN]).await?.row,
        fixture.expected(&old_key)
    );
    let stats = served.storage.stats();
    let before = stats.gets();
    assert!(!served.table.refresh().await?, "nothing moved");
    assert_eq!(catalog.calls(), 2, "the load and the poll, one call each");
    assert_eq!(stats.gets(), before, "an unchanged poll reads no object");
    assert_signed_with_storage_keys(&catalog);
    Ok(())
}

/// The fault a refused load of `source` raised, under the table's catalog
/// name.
async fn refused_fault(fixture: &Fixture, source: SourceConfig, named: &str) -> TableFault {
    match serve(fixture, source).await {
        Err(TestkitError::Core(Error::Table { table, fault })) => {
            assert_eq!(table, named);
            fault
        }
        Err(other) => panic!("expected a table fault, got {other:?}"),
        Ok(_) => panic!("expected a table fault, got a table that loaded"),
    }
}

#[tokio::test]
async fn test_aws_sources_refuse_a_table_that_is_not_iceberg_or_has_no_location() -> Result<()> {
    let (_dir, writer) = local_catalog().await?;
    let fixture = write_fixture(writer.as_ref(), NAMESPACE, spec("kv_refused")).await?;
    let catalog = FakeAwsCatalog::start()?;
    let name = fixture.ident().name().to_string();
    let glue_name = format!("{NAMESPACE}.{name}");

    catalog.set(
        NAMESPACE,
        &name,
        CatalogEntry::TableType {
            table_type: "HIVE".to_string(),
            location: fixture.metadata_location().to_string(),
        },
    );
    assert_eq!(
        refused_fault(&fixture, glue(&catalog, &fixture), &glue_name).await,
        TableFault::NotIceberg {
            table_type: Some("HIVE".to_string()),
        }
    );

    catalog.set(NAMESPACE, &name, CatalogEntry::NoLocation);
    assert_eq!(
        refused_fault(&fixture, glue(&catalog, &fixture), &glue_name).await,
        TableFault::MetadataLocationUnknown
    );
    assert_eq!(
        refused_fault(
            &fixture,
            s3tables(&catalog, &fixture),
            &format!("{NAMESPACE}.{name} in {TABLE_BUCKET_ARN}")
        )
        .await,
        TableFault::MetadataLocationUnknown
    );
    assert_signed_with_storage_keys(&catalog);
    Ok(())
}
