//! Two table entries over one Iceberg table, each naming a different value
//! column: each answers its own column's bytes, the value pages of both
//! share one page cache, and in the Disk tier both entries share one chunk
//! file per data file, which `retain` keeps while either serves it.

use std::sync::Arc;

use icetray_core::ColumnRead;
use icetray_core::config::{
    Config, LocalConfig, Residency, ServerConfig, StorageConfig, TableConfig, ValueColumnConfig,
};
use icetray_core::open::open_with;
use icetray_core::{FindingLog, KeyType, Row, Storage};
use icetray_testkit::fixture::{NAMESPACE, SAMPLE, SECOND_VALUE_COLUMN, local_catalog};
use icetray_testkit::layout::VALUE_COLUMN;
use icetray_testkit::{FixtureSpec, Result, verify, write_fixture};

/// The entry that names the fixture's `value` column.
const VALUE_TABLE: &str = "jobs_value";
/// The entry that names `SECOND_VALUE_COLUMN`.
const DETAIL_TABLE: &str = "jobs_detail";

/// The read of the fixture's `value` column, whole.
const VALUE_READ: [ColumnRead<'static>; 1] = [ColumnRead::whole(VALUE_COLUMN)];
/// The read of `SECOND_VALUE_COLUMN`, whole.
const DETAIL_READ: [ColumnRead<'static>; 1] = [ColumnRead::whole(SECOND_VALUE_COLUMN)];

/// The local directory's chunk file subdirectory.
const INDEX_DIR: &str = "index";

#[tokio::test]
async fn test_two_entries_over_one_table_serve_their_own_value_columns() -> Result<()> {
    for residency in [Residency::Full, Residency::Disk] {
        let (_dir, catalog) = local_catalog().await?;
        let scratch = tempfile::tempdir()?;
        let spec = FixtureSpec {
            second_value: true,
            ..FixtureSpec::small("jobs", KeyType::Long)
        };
        let fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
        let value = fixture.config(residency, false);
        let detail = TableConfig {
            name: DETAIL_TABLE.to_string(),
            value_columns: vec![ValueColumnConfig::named(SECOND_VALUE_COLUMN)],
            ..value.clone()
        };
        let value = TableConfig {
            name: VALUE_TABLE.to_string(),
            ..value
        };
        let config = Config {
            server: ServerConfig::default(),
            storage: StorageConfig {
                local: Some(LocalConfig {
                    dir: scratch.path().to_path_buf(),
                    index_budget_bytes: None,
                    page_cache_disk_bytes: 0,
                }),
                ..verify::storage_config_for_tests()
            },
            tables: vec![value, detail],
        };
        let storage = Arc::new(Storage::new(config.storage.clone()));
        let opened = open_with(&config, None, Arc::clone(&storage), FindingLog::Events).await?;
        let tables = opened.tables();
        let (value, detail) = (&tables[0], &tables[1]);
        value.load().await?;
        detail.load().await?;

        let stats = storage.stats();
        for id in SAMPLE {
            let key = fixture.key(id);
            assert_eq!(
                value.get(&key, &VALUE_READ).await?.row,
                fixture.expected(&key),
                "{residency:?} {key}"
            );
            assert_eq!(
                detail.get(&key, &DETAIL_READ).await?.row,
                fixture.expected_of(&key, &[SECOND_VALUE_COLUMN]),
                "{residency:?} {key}"
            );
            assert_ne!(
                value.get(&key, &VALUE_READ).await?.row,
                detail.get(&key, &DETAIL_READ).await?.row,
                "the entries read different columns"
            );
        }
        let gets = stats.gets();
        for id in SAMPLE {
            let key = fixture.key(id);
            value.get(&key, &VALUE_READ).await?;
            detail.get(&key, &DETAIL_READ).await?;
        }
        assert_eq!(
            stats.gets(),
            gets,
            "{residency:?}: both columns' pages stay in the one page cache"
        );
        let absent = fixture.absent_near(SAMPLE[0]);
        assert_eq!(value.get(&absent, &VALUE_READ).await?.row, Row::Absent);
        assert_eq!(detail.get(&absent, &DETAIL_READ).await?.row, Row::Absent);

        if residency == Residency::Disk {
            let chunk_files = std::fs::read_dir(scratch.path().join(INDEX_DIR))?.count();
            assert_eq!(
                chunk_files,
                fixture.file_count(),
                "one chunk file per data file, shared by both entries"
            );
            assert_eq!(opened.retain().await?, 0, "both entries' chunks are live");
        }
    }
    Ok(())
}
