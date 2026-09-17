//! A `TableSource::Catalog` built by `from_config` resolving through a real
//! REST catalog (the C tier of RFD 014): the one test that needs a
//! container, run by `dev/test-int.sh` and ignored otherwise. Forced without
//! `ICETRAY_REST_URI`, it fails on `Precondition::RestUriUnset` rather than
//! passing.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use iceberg::CatalogBuilder as _;
use iceberg::io::LocalFsStorageFactory;
use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, RestCatalogBuilder};
use icetray_core::KeyType;
use icetray_core::config::{Residency, SourceConfig, TableConfig};
use icetray_core::{TableSource, Value};
use icetray_testkit::error::{Precondition, REST_URI_VAR, required_var};
use icetray_testkit::verify;
use icetray_testkit::{FixtureSpec, Result, write_fixture};

/// The namespace the integration fixtures are written in.
const NAMESPACE: &str = "icetray_it";

/// The name the REST client is registered under; nothing reads it back.
const CATALOG_NAME: &str = "rest";

/// Ids of the fixture and the range appended to it.
const IDS_4K: Range<u64> = 0..4_000;
const APPENDED_4K_6K: Range<u64> = 4_000..6_000;

/// An id of the first build, and one the append adds.
const OLD_ID: u64 = 10;
const NEW_ID: u64 = 5_000;

#[tokio::test]
#[ignore = "needs a REST catalog at ICETRAY_REST_URI; run dev/test-int.sh"]
async fn test_rest_catalog_source_loads_polls_and_advances() -> Result<()> {
    let uri = required_var(REST_URI_VAR, Precondition::RestUriUnset)?;
    let catalog = Arc::new(
        RestCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                CATALOG_NAME,
                HashMap::from([(REST_CATALOG_PROP_URI.to_string(), uri.clone())]),
            )
            .await?,
    );
    let name = format!("kv_{}", uuid::Uuid::new_v4().simple());
    let spec = FixtureSpec {
        ids: IDS_4K,
        ..FixtureSpec::small(&name, KeyType::Long)
    };
    let mut fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    let source = SourceConfig::Rest {
        uri,
        warehouse: None,
        namespace: NAMESPACE.to_string(),
        table: name,
    };
    let served = verify::serve(
        TableConfig {
            source: source.clone(),
            ..fixture.config(Residency::Full, false)
        },
        TableSource::from_config(&source).await?,
        verify::storage_config_for_tests(),
    )
    .await?;
    let generation = served.table.current().expect("the table was loaded");
    assert_eq!(
        generation.records,
        u64::try_from(fixture.ids().count()).unwrap()
    );
    let old_key = fixture.key(OLD_ID);
    assert_eq!(
        served.table.get(&old_key).await?.value,
        fixture.expected(&old_key)
    );

    let stats = served.storage.stats();
    let before = stats.gets();
    assert!(!served.table.refresh().await?, "nothing moved");
    assert_eq!(stats.gets(), before, "the one read is the catalog record");

    fixture
        .append(catalog.as_ref(), APPENDED_4K_6K, "build2")
        .await?;
    assert!(served.table.refresh().await?);
    let new_key = fixture.key(NEW_ID);
    assert_eq!(
        served.table.get(&new_key).await?.value,
        fixture.expected(&new_key)
    );
    assert!(matches!(
        served.table.get(&old_key).await?.value,
        Value::Present(_)
    ));
    Ok(())
}
