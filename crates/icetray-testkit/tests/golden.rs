//! The golden tables `dev/golden-tables.py` wrote with a real batch writer
//! (the G tier of RFD 014), located through `ICETRAY_GOLDEN_WAREHOUSE` and
//! opened through `icetray_core::open`. Every test is ignored by default and
//! run by `dev/test-golden.sh`; forced without the variable, each fails on
//! `Precondition::GoldenUnset` rather than passing.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;

use iceberg::spec::{
    DataContentType, DataFile, DataFileFormat, FormatVersion, Manifest, ManifestList, TableMetadata,
};
use icetray_core::ColumnRead;
use icetray_core::approx::count;
use icetray_core::config::{Config, LocalConfig, ServerConfig, StorageConfig, ValueFormat};
use icetray_core::{
    DEFAULT_MAX_SCAN_ROWS, Direction, Error, Finding, FindingCode, FindingLog, Generation, Key,
    Level, LookupCost, OrderType, RefusalReason, Residency, Row, ScanBounds, ScanRow, Scanned,
    Storage, Table, TableReport, diagnose, open,
};
use icetray_testkit::golden::{
    self, CONTRACT_REPEATS, GoldenTable, HEX_DIGEST_BYTES, MERGE_KEY_IDS, MOR_DELETED_IDS,
    MOR_UPDATED_IDS, ORDERED_KEYS, ORDERED_ROWS_PER_KEY, OrderedRow, WIDE_REPEATS,
};
use icetray_testkit::keys::KEY_STEP;
use icetray_testkit::layout::VALUE_COLUMN;
use icetray_testkit::verify::{self, SeenPages};
use icetray_testkit::{Result, TestScan, absent_key_from, scan_model};

/// Ids looked up on the two-million-row tables: the ends and the middle.
const IDS_2M: [u64; 5] = [0, 1, 12_345, 999_999, 1_999_999];

/// Ids looked up on the one-million-row table.
const IDS_1M: [u64; 5] = [0, 1, 12_345, 499_999, 999_999];

/// Ids looked up on the half-million-row tables.
const IDS_500K: [u64; 5] = [0, 1, 12_345, 249_999, 499_999];

/// Ids looked up on the quarter-million-row tables.
const IDS_250K: [u64; 5] = [0, 1, 12_345, 123_456, 249_999];

/// Ids looked up on the fifty-thousand-row table.
const IDS_50K: [u64; 5] = [0, 1, 12_345, 24_999, 49_999];

/// The value a byte of a binary key is flipped with to make an absent key.
const FLIP: u8 = 0xff;

/// A golden table opened and loaded through `icetray_core::open` under the
/// default storage settings, every key of it checked against its file's
/// recorded bucket (`Generation::verify_buckets`), which a table a real
/// writer bucketed passes.
struct Golden {
    storage: Arc<Storage>,
    table: Arc<Table>,
}

impl Golden {
    /// The generation the golden table serves; `open_golden` loaded it.
    fn current(&self) -> Arc<Generation> {
        self.table
            .current()
            .expect("open_golden loaded the table before handing it out")
    }

    /// The doctor's report on the serving generation, at the table's
    /// declared level.
    fn report(&self) -> TableReport {
        diagnose(
            &self.current(),
            self.table.config().lookups(),
            &ValueFormat::opaque,
            self.table
                .config()
                .scan_limits(ServerConfig::default().max_scan_bytes),
        )
    }
}

async fn open_golden(table: GoldenTable, strict: bool) -> Result<Golden> {
    let warehouse = golden::warehouse()?;
    open_config(&golden::config(&warehouse, table, strict)).await
}

/// `table` opened as `open_golden` does in `residency`, over hedge-free
/// storage (`verify::storage_config_for_tests`), so no hedge adds a GET to
/// a count the oracle predicts; the Disk tier's local directory lives
/// under `scratch`.
async fn open_in(table: GoldenTable, residency: Residency, scratch: &Path) -> Result<Golden> {
    let warehouse = golden::warehouse()?;
    let mut config = golden::config(&warehouse, table, false);
    config.tables[0].residency = residency;
    let local = match residency {
        Residency::Disk => Some(LocalConfig {
            dir: scratch.join(table.to_string()),
            index_budget_bytes: None,
            page_cache_disk_bytes: 0,
        }),
        Residency::Full | Residency::Bloom => None,
    };
    config.storage = StorageConfig {
        local,
        ..verify::storage_config_for_tests()
    };
    open_config(&config).await
}

/// The one table `config` serves, opened through `icetray_core::open` and
/// loaded, with every key checked against its file's recorded bucket.
async fn open_config(config: &Config) -> Result<Golden> {
    let opened = open(config, None, FindingLog::Events).await?;
    let table = Arc::clone(&opened.tables()[0]);
    table.load().await?;
    table.serving()?.verify_buckets().await?;
    Ok(Golden {
        storage: opened.storage,
        table,
    })
}

/// Looks every key up, checks its value against `expected`, and returns
/// the GETs the batch cost.
async fn hits(golden: &Golden, keys: &[Key], expected: impl Fn(&Key) -> Vec<u8>) -> Result<u64> {
    let stats = golden.storage.stats();
    let before = stats.gets();
    for key in keys {
        let lookup = golden
            .table
            .get(key, &[ColumnRead::whole(VALUE_COLUMN)])
            .await?;
        let Some(bytes) = verify::one_value(&lookup.row) else {
            panic!("key {key} is {:?}", lookup.row);
        };
        assert_eq!(&bytes[..], &expected(key)[..], "value for key {key}");
    }
    Ok(stats.gets() - before)
}

/// Every key is absent and costs nothing.
async fn misses(golden: &Golden, keys: &[Key]) -> Result<()> {
    let stats = golden.storage.stats();
    let before = stats.gets();
    for key in keys {
        assert_eq!(
            golden
                .table
                .get(key, &[ColumnRead::whole(VALUE_COLUMN)])
                .await?
                .row,
            Row::Absent,
            "key {key}"
        );
    }
    assert_eq!(stats.gets(), before, "misses touch no storage");
    Ok(())
}

/// Cold hits cost exactly the value pages the oracle names (two sample ids
/// may share a page, so the count is the plan's, never the key count),
/// misses at the key after each nothing.
async fn verify_contract_hits(
    golden: &Golden,
    keys: &[Key],
    expected: impl Fn(&Key) -> Vec<u8>,
) -> Result<()> {
    let generation = golden.current();
    let cold = verify::expected_cold_gets(&generation, keys, &[VALUE_COLUMN])?;
    assert!(cold > 0);
    assert_eq!(hits(golden, keys, expected).await?, cold);
    let absent: Vec<Key> = keys.iter().map(absent_key_from).collect();
    misses(golden, &absent).await
}

/// The decimal text Spark casts a long key to.
fn long_text(key: &Key) -> String {
    let Key::Long(value) = key else {
        panic!("a long table has long keys, got {key}");
    };
    value.to_string()
}

/// The string a string key is.
fn string_text(key: &Key) -> String {
    let Key::Str(text) = key else {
        panic!("a string table has string keys, got {key}");
    };
    text.clone()
}

fn capping_codes(golden: &Golden) -> Vec<FindingCode> {
    codes_of(&golden.report())
}

/// The codes of `report`'s capping findings, in order.
fn codes_of(report: &TableReport) -> Vec<FindingCode> {
    report
        .capping()
        .into_iter()
        .map(|capping| capping.code)
        .collect()
}

fn advice_codes(golden: &Golden) -> Vec<FindingCode> {
    golden
        .report()
        .findings
        .iter()
        .filter(|finding| finding.caps().is_none())
        .map(Finding::code)
        .collect()
}

/// The level the table's layout achieves, as the doctor reports it.
fn level(golden: &Golden) -> Level {
    golden.report().summary.level
}

/// The doctor's and the serving generation's lookup cost are `expected`,
/// and the oracle prices a cold hit on each of `keys`, alone, at
/// `expected.gets_per_hit`: every golden table is served in the Full
/// tier, where a hit's GETs are the value pages the plan names.
fn assert_lookup_cost(golden: &Golden, keys: &[Key], expected: LookupCost) -> Result<()> {
    let generation = golden.current();
    assert_eq!(golden.report().summary.lookup_cost, Some(expected));
    assert_eq!(generation.lookup_cost, expected);
    for key in keys {
        assert_eq!(
            verify::expected_cold_gets(&generation, std::slice::from_ref(key), &[VALUE_COLUMN])?,
            expected.gets_per_hit,
            "key {key}"
        );
    }
    Ok(())
}

#[tokio::test]
#[ignore = "needs ICETRAY_GOLDEN_WAREHOUSE; run dev/test-golden.sh"]
async fn test_golden_long_contract_serves_one_get_per_hit() -> Result<()> {
    let golden = open_golden(GoldenTable::Long2m, false).await?;
    let report = golden.report();
    assert!(
        report.findings.is_empty(),
        "the contract table draws no finding: {:?}",
        report.findings
    );
    assert_eq!(report.summary.level, Level::Optimal);
    let keys: Vec<Key> = IDS_2M.iter().map(|&id| golden::long_key(id)).collect();
    let contract = LookupCost {
        gets_per_hit: 1,
        gets_per_miss: 0,
        files_probed: 1,
    };
    assert_lookup_cost(&golden, &keys, contract)?;
    verify_contract_hits(&golden, &keys, |key| golden::value(&long_text(key))).await
}

#[tokio::test]
#[ignore = "needs ICETRAY_GOLDEN_WAREHOUSE; run dev/test-golden.sh"]
async fn test_golden_string_contract_serves() -> Result<()> {
    let golden = open_golden(GoldenTable::Str2m, false).await?;
    assert!(capping_codes(&golden).is_empty());
    assert_eq!(level(&golden), Level::Optimal);
    let keys: Vec<Key> = IDS_2M.iter().map(|&id| golden::string_key(id)).collect();
    verify_contract_hits(&golden, &keys, |key| golden::value(&string_text(key))).await
}

/// The binary keys are MD5 digests the recipe derives in Spark; the test
/// takes one per file from the manifest's exact bounds and checks presence
/// and value length.
#[tokio::test]
#[ignore = "needs ICETRAY_GOLDEN_WAREHOUSE; run dev/test-golden.sh"]
async fn test_golden_bin16_contract_serves() -> Result<()> {
    let golden = open_golden(GoldenTable::Bin16_500k, false).await?;
    assert!(capping_codes(&golden).is_empty());
    assert_eq!(level(&golden), Level::Optimal);
    let generation = golden.current();
    let keys: Vec<Key> = generation
        .files
        .iter()
        .map(|file| {
            file.entry
                .key_min
                .clone()
                .expect("full metrics record exact bounds")
        })
        .collect();
    assert!(!keys.is_empty());
    let stats = golden.storage.stats();
    for key in &keys {
        let row = golden
            .table
            .get(key, &[ColumnRead::whole(VALUE_COLUMN)])
            .await?
            .row;
        let Some(bytes) = verify::one_value(&row) else {
            panic!("key {key} is absent");
        };
        assert_eq!(bytes.len(), CONTRACT_REPEATS * HEX_DIGEST_BYTES);
    }
    let flipped: Vec<Key> = keys
        .iter()
        .map(|key| {
            let Key::Bytes(bytes) = key else {
                panic!("a binary table has binary keys, got {key}");
            };
            let mut absent = bytes.clone();
            if let Some(last) = absent.last_mut() {
                *last ^= FLIP;
            }
            Key::Bytes(absent)
        })
        .collect();
    let before = stats.gets();
    for key in &flipped {
        assert_eq!(
            golden
                .table
                .get(key, &[ColumnRead::whole(VALUE_COLUMN)])
                .await?
                .row,
            Row::Absent,
            "key {key}"
        );
    }
    assert_eq!(stats.gets(), before);
    Ok(())
}

#[tokio::test]
#[ignore = "needs ICETRAY_GOLDEN_WAREHOUSE; run dev/test-golden.sh"]
async fn test_golden_wide_values_are_byte_bounded() -> Result<()> {
    let golden = open_golden(GoldenTable::LongWide250k, false).await?;
    assert!(!advice_codes(&golden).contains(&FindingCode::LargeValuePages));
    assert_eq!(level(&golden), Level::Optimal);
    let keys: Vec<Key> = IDS_250K.iter().map(|&id| golden::long_key(id)).collect();
    verify_contract_hits(&golden, &keys, |key| {
        let value = golden::wide_value(&long_text(key));
        assert_eq!(value.len(), WIDE_REPEATS * HEX_DIGEST_BYTES);
        value
    })
    .await
}

/// A contract table in table format version 3 serves as version 2 does:
/// the doctor finds it optimal with no capping finding, and a cold hit costs
/// the value pages the oracle names. Its files are small, so the writer's
/// fixed-size bloom filter may draw advice; that is the doctor's to report.
#[tokio::test]
#[ignore = "needs ICETRAY_GOLDEN_WAREHOUSE; run dev/test-golden.sh"]
async fn test_golden_format_v3_contract_serves_one_get_per_hit() -> Result<()> {
    let golden = open_golden(GoldenTable::LongV3_50k, false).await?;
    let generation = golden.current();
    let metadata: TableMetadata = serde_json::from_slice(
        &golden
            .storage
            .read_all(&generation.metadata_location)
            .await?,
    )?;
    assert_eq!(metadata.format_version(), FormatVersion::V3);
    let report = golden.report();
    assert_eq!(report.summary.level, Level::Optimal);
    assert!(report.capping().is_empty(), "{:?}", report.findings);
    let keys: Vec<Key> = IDS_50K.iter().map(|&id| golden::long_key(id)).collect();
    verify_contract_hits(&golden, &keys, |key| golden::value(&long_text(key))).await
}

#[tokio::test]
#[ignore = "needs ICETRAY_GOLDEN_WAREHOUSE; run dev/test-golden.sh"]
async fn test_golden_default_layout_is_balanced() -> Result<()> {
    let golden = open_golden(GoldenTable::DefaultLayout1m, false).await?;
    let report = golden.report();
    assert_eq!(report.summary.level, Level::Balanced);
    assert!(codes_of(&report).contains(&FindingCode::LargeKeyPages));
    let advice = advice_codes(&golden);
    assert!(advice.contains(&FindingCode::NoBloomFilter), "{advice:?}");
    assert!(advice.contains(&FindingCode::LargeValuePages), "{advice:?}");
    let keys: Vec<Key> = IDS_1M.iter().map(|&id| golden::long_key(id)).collect();
    verify_contract_hits(&golden, &keys, |key| golden::value(&long_text(key))).await
}

#[tokio::test]
#[ignore = "needs ICETRAY_GOLDEN_WAREHOUSE; run dev/test-golden.sh"]
async fn test_golden_strict_refuses_the_default_layout() -> Result<()> {
    let warehouse = golden::warehouse()?;
    let opened = open(
        &golden::config(&warehouse, GoldenTable::DefaultLayout1m, true),
        None,
        FindingLog::Events,
    )
    .await?;
    let error = opened.tables()[0].load().await.unwrap_err();
    let Error::Refused {
        reason: RefusalReason::BelowLevel { findings, .. },
        ..
    } = error
    else {
        panic!("strict refuses the default layout: {error:?}");
    };
    assert!(
        findings
            .iter()
            .any(|capping| capping.code == FindingCode::LargeKeyPages),
        "{findings:?}"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "needs ICETRAY_GOLDEN_WAREHOUSE; run dev/test-golden.sh"]
async fn test_golden_unpartitioned_is_permissive() -> Result<()> {
    let golden = open_golden(GoldenTable::Unpartitioned500k, false).await?;
    assert!(capping_codes(&golden).contains(&FindingCode::NoBucketPartition));
    assert_eq!(level(&golden), Level::Permissive);
    let keys: Vec<Key> = IDS_500K.iter().map(|&id| golden::long_key(id)).collect();
    verify_contract_hits(&golden, &keys, |key| golden::value(&long_text(key))).await
}

#[tokio::test]
#[ignore = "needs ICETRAY_GOLDEN_WAREHOUSE; run dev/test-golden.sh"]
async fn test_golden_overlap_serves_and_reports_overlap_permissive() -> Result<()> {
    let golden = open_golden(GoldenTable::Overlap250k, false).await?;
    let keys: Vec<Key> = IDS_250K
        .iter()
        .map(|&id| golden::permuted_key(id))
        .collect();
    let generation = golden.current();
    let cold = verify::expected_cold_gets(&generation, &keys, &[VALUE_COLUMN])?;
    assert_eq!(
        hits(&golden, &keys, |key| golden::wide_value(&long_text(key))).await?,
        cold
    );
    assert!(
        !generation.overlapping_buckets().is_empty(),
        "overlap_250k has no overlapping files: dev/golden-tables.py writes it with \
         skew_splitting(spark, True) \
         (spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled), \
         so each bucket is split across several sorted tasks; regenerate the table"
    );
    let capping = capping_codes(&golden);
    assert!(
        capping.contains(&FindingCode::OverlappingFiles),
        "{capping:?}"
    );
    assert_eq!(level(&golden), Level::Permissive);
    // A lookup probes the files whose exact range covers its key, and the
    // most of them any key reaches is reached at some file's first key; the
    // skew-split tasks chain their ranges, so fewer than a bucket holds.
    let deepest = generation
        .files
        .iter()
        .map(|file| generation.candidates(file.index.key_range().0).count())
        .max()
        .expect("the table has files");
    assert!(deepest > 1, "overlapping files share a key range");
    assert!(deepest <= golden.report().summary.max_files_per_bucket);
    assert_lookup_cost(
        &golden,
        &keys,
        LookupCost {
            gets_per_hit: 1,
            gets_per_miss: 0,
            files_probed: deepest,
        },
    )?;
    Ok(())
}

/// The merge-on-read tables that keep the commands' deletes and new files
/// uncompacted.
const MOR_CHANGED: [GoldenTable; 2] = [GoldenTable::LongMorV2_100k, GoldenTable::LongMorV3_100k];

/// The merge-on-read tables compacted after the commands.
const MOR_COMPACTED: [GoldenTable; 2] = [
    GoldenTable::LongMorV2Compacted100k,
    GoldenTable::LongMorV3Compacted100k,
];

/// Rows the recipe's commands delete: the DELETE's ids, and the UPDATE's,
/// whose old copies its deletes remove.
const MOR_CHANGED_ROWS: u64 =
    MOR_DELETED_IDS.end - MOR_DELETED_IDS.start + MOR_UPDATED_IDS.end - MOR_UPDATED_IDS.start;

/// Ids of a merge-on-read table whose next key, absent, each tier looks
/// up: each end of the table and of the ranges the commands touch.
fn mor_edge_ids(table: GoldenTable) -> [u64; 6] {
    [
        0,
        MOR_DELETED_IDS.start,
        MOR_DELETED_IDS.end - 1,
        MOR_UPDATED_IDS.start,
        MOR_UPDATED_IDS.end - 1,
        table.rows() - 1,
    ]
}

/// Ids of a merge-on-read table whose key the commands leave live: the
/// updated range's ends and the first untouched id after each range.
const MOR_LIVE_IDS: [u64; 4] = [
    MOR_DELETED_IDS.end,
    MOR_UPDATED_IDS.start,
    MOR_UPDATED_IDS.end - 1,
    MOR_UPDATED_IDS.end,
];

/// Every id `table` was filled with answers in every tier as the recipe's
/// commands leave it (`golden::merge_on_read_value`): absent where deleted,
/// the update's value where updated, the first value elsewhere; the key
/// after each of `mor_edge_ids` is absent. Each lookup costs the GETs
/// `verify::expected_lookup_gets` predicts from the Full tier's plan, past
/// the pages the tier fetched before.
async fn assert_merge_on_read_serves(table: GoldenTable) -> Result<()> {
    let scratch = tempfile::tempdir()?;
    let mut tiers = Vec::new();
    for residency in Residency::ALL {
        tiers.push((residency, open_in(table, residency, scratch.path()).await?));
    }
    let full = tiers[0].1.current();
    assert_plan_names_the_update_s_copy(table, &full);
    let absent: Vec<Key> = mor_edge_ids(table)
        .iter()
        .map(|&id| absent_key_from(&golden::long_key(id)))
        .collect();
    for (residency, served) in &tiers {
        let generation = served.current();
        assert_eq!(generation.residency, *residency);
        let stats = served.storage.stats();
        let mut seen = SeenPages::default();
        let lookups = (0..table.rows())
            .map(|id| (golden::long_key(id), golden::merge_on_read_value(id)))
            .chain(absent.iter().map(|key| (key.clone(), None)));
        for (key, expected) in lookups {
            let gets = verify::expected_lookup_gets(
                &full,
                &generation,
                VALUE_COLUMN,
                &key,
                expected.is_some(),
                &mut seen,
            );
            let before = stats.gets();
            let answer = served
                .table
                .get(&key, &[ColumnRead::whole(VALUE_COLUMN)])
                .await?
                .row;
            match &expected {
                Some(value) => {
                    let Some(bytes) = verify::one_value(&answer) else {
                        panic!("{table} {residency} key {key} is {answer:?}");
                    };
                    assert_eq!(&bytes[..], &value[..], "{table} {residency} key {key}");
                }
                None => assert_eq!(answer, Row::Absent, "{table} {residency} key {key}"),
            }
            assert_eq!(
                stats.gets() - before,
                gets,
                "{table} {residency} GETs for key {key}"
            );
        }
    }
    Ok(())
}

/// The Full tier's plan names, for every key the UPDATE rewrote, the copy
/// the UPDATE wrote, skipping the one its deletes remove: a file at the
/// newest data sequence number. In a table not compacted since, that is
/// after the INSERT's files, so the check tells the two copies apart; a
/// compacted table holds one copy, and the check holds trivially there.
fn assert_plan_names_the_update_s_copy(table: GoldenTable, full: &Generation) {
    let sequences = full.files.iter().map(|file| file.entry.sequence);
    let newest = sequences.clone().max().expect("a golden table has files");
    let oldest = sequences.min().expect("a golden table has files");
    let compacted = matches!(
        table,
        GoldenTable::LongMorV2Compacted100k | GoldenTable::LongMorV3Compacted100k
    );
    assert_eq!(newest > oldest, !compacted, "{table} sequences");
    for id in MOR_UPDATED_IDS {
        let key = golden::long_key(id);
        let (file, _) = full.plan(std::slice::from_ref(&key))[0]
            .clone()
            .unwrap_or_else(|| panic!("{table} plans no file for updated key {key}"));
        assert_eq!(
            file.entry.sequence, newest,
            "{table} key {key} is planned at {}, a deleted copy",
            file.entry.path
        );
    }
}

/// The live delete entries of the snapshot `golden` serves, from its
/// manifests as Iceberg's own reader parses them.
async fn listed_deletes(golden: &Golden) -> Result<Vec<DataFile>> {
    let generation = golden.current();
    let metadata: TableMetadata = serde_json::from_slice(
        &golden
            .storage
            .read_all(&generation.metadata_location)
            .await?,
    )?;
    let snapshot = metadata
        .current_snapshot()
        .expect("a served table has a current snapshot");
    assert_eq!(snapshot.snapshot_id(), generation.snapshot_id.0);
    let list = ManifestList::parse_with_version(
        &golden.storage.read_all(snapshot.manifest_list()).await?,
        metadata.format_version(),
    )?;
    let mut deletes = Vec::new();
    for manifest_file in list.entries() {
        let manifest = Manifest::parse_avro(
            &golden
                .storage
                .read_all(&manifest_file.manifest_path)
                .await?,
        )?;
        deletes.extend(
            manifest
                .entries()
                .iter()
                .filter(|entry| entry.is_alive() && entry.content_type() != DataContentType::Data)
                .map(|entry| entry.data_file().clone()),
        );
    }
    Ok(deletes)
}

/// The deepest any key's candidates reach in `golden`'s serving
/// generation, reached at some file's first key.
fn deepest_probe(golden: &Golden) -> usize {
    let generation = golden.current();
    generation
        .files
        .iter()
        .map(|file| generation.candidates(file.index.key_range().0).count())
        .max()
        .expect("the table has files")
}

#[tokio::test]
#[ignore = "needs ICETRAY_GOLDEN_WAREHOUSE; run dev/test-golden.sh"]
async fn test_golden_merge_on_read_v2_serves_every_key_in_every_tier() -> Result<()> {
    assert_merge_on_read_serves(GoldenTable::LongMorV2_100k).await
}

#[tokio::test]
#[ignore = "needs ICETRAY_GOLDEN_WAREHOUSE; run dev/test-golden.sh"]
async fn test_golden_merge_on_read_v3_serves_every_key_in_every_tier() -> Result<()> {
    assert_merge_on_read_serves(GoldenTable::LongMorV3_100k).await
}

#[tokio::test]
#[ignore = "needs ICETRAY_GOLDEN_WAREHOUSE; run dev/test-golden.sh"]
async fn test_golden_merge_on_read_v2_compacted_serves_every_key_in_every_tier() -> Result<()> {
    assert_merge_on_read_serves(GoldenTable::LongMorV2Compacted100k).await
}

#[tokio::test]
#[ignore = "needs ICETRAY_GOLDEN_WAREHOUSE; run dev/test-golden.sh"]
async fn test_golden_merge_on_read_v3_compacted_serves_every_key_in_every_tier() -> Result<()> {
    assert_merge_on_read_serves(GoldenTable::LongMorV3Compacted100k).await
}

/// The UPDATE adds one file per bucket over the older file's key range, so
/// the doctor caps the table at permissive for overlapping files and
/// advises a compaction; a cold hit still costs one GET and a miss none,
/// and a lookup probes both files.
#[tokio::test]
#[ignore = "needs ICETRAY_GOLDEN_WAREHOUSE; run dev/test-golden.sh"]
async fn test_golden_merge_on_read_updates_overlap_permissive() -> Result<()> {
    for table in MOR_CHANGED {
        let golden = open_golden(table, false).await?;
        let generation = golden.current();
        assert_eq!(
            generation.overlapping_buckets().len(),
            usize::try_from(table.buckets()).unwrap(),
            "{table}: every bucket gains an overlapping file"
        );
        let report = golden.report();
        assert_eq!(report.summary.level, Level::Permissive, "{table}");
        let capping = codes_of(&report);
        assert!(
            capping.contains(&FindingCode::OverlappingFiles),
            "{table} {capping:?}"
        );
        let advice = advice_codes(&golden);
        assert!(
            advice.contains(&FindingCode::DeletesPending),
            "{table} {advice:?}"
        );
        let updated_and_older = 2;
        assert_eq!(deepest_probe(&golden), updated_and_older, "{table}");
        let keys: Vec<Key> = MOR_LIVE_IDS
            .iter()
            .map(|&id| golden::long_key(id))
            .collect();
        assert_lookup_cost(
            &golden,
            &keys,
            LookupCost {
                gets_per_hit: 1,
                gets_per_miss: 0,
                files_probed: updated_and_older,
            },
        )?;
    }
    Ok(())
}

/// A compaction rewrites each bucket into one sorted file, so the table
/// serves at its floor again with no deletes left to advise on.
#[tokio::test]
#[ignore = "needs ICETRAY_GOLDEN_WAREHOUSE; run dev/test-golden.sh"]
async fn test_golden_merge_on_read_compaction_restores_optimal() -> Result<()> {
    for table in MOR_COMPACTED {
        let golden = open_golden(table, false).await?;
        let report = golden.report();
        assert_eq!(report.summary.level, Level::Optimal, "{table}");
        assert!(report.capping().is_empty(), "{table} {:?}", report.findings);
        let advice = advice_codes(&golden);
        assert!(
            !advice.contains(&FindingCode::DeletesPending),
            "{table} {advice:?}"
        );
        assert!(golden.current().overlapping_buckets().is_empty(), "{table}");
        let keys: Vec<Key> = MOR_LIVE_IDS
            .iter()
            .map(|&id| golden::long_key(id))
            .collect();
        assert_lookup_cost(
            &golden,
            &keys,
            LookupCost {
                gets_per_hit: 1,
                gets_per_miss: 0,
                files_probed: 1,
            },
        )?;
    }
    Ok(())
}

/// What Spark records for the commands, read from the manifests, and the
/// doctor's deletes summary beside it. In format version 2 the deletes are
/// Parquet position delete files whose entries name no data file, so a
/// load reads each to learn what it names; in version 3, deletion vectors
/// in a Puffin file, each naming its data file. Neither writes an equality
/// delete. Uncompacted, the summary's rows are the rows the commands
/// changed and its files the live deletes listed. Compacted, nothing
/// applies: in version 2 the position delete files stay listed, naming
/// only files the rewrite removed, and in version 3 the rewrite dropped
/// the vectors.
#[tokio::test]
#[ignore = "needs ICETRAY_GOLDEN_WAREHOUSE; run dev/test-golden.sh"]
async fn test_golden_merge_on_read_deletes_summary_matches_the_manifests() -> Result<()> {
    let tables = [
        (GoldenTable::LongMorV2_100k, DataFileFormat::Parquet),
        (GoldenTable::LongMorV3_100k, DataFileFormat::Puffin),
        (GoldenTable::LongMorV2Compacted100k, DataFileFormat::Parquet),
        (GoldenTable::LongMorV3Compacted100k, DataFileFormat::Puffin),
    ];
    for (table, format) in tables {
        let golden = open_golden(table, false).await?;
        let deletes = listed_deletes(&golden).await?;
        for delete in &deletes {
            let path = delete.file_path();
            assert_eq!(
                delete.content_type(),
                DataContentType::PositionDeletes,
                "{table} {path}"
            );
            assert_eq!(delete.file_format(), format, "{table} {path}");
            assert_eq!(
                delete.referenced_data_file().is_some(),
                format == DataFileFormat::Puffin,
                "{table} {path}: a deletion vector names its data file and \
                 Spark's position delete files name none"
            );
        }
        let listed_rows: u64 = deletes.iter().map(DataFile::record_count).sum();
        let summary = golden.report().summary;
        if MOR_CHANGED.contains(&table) {
            assert_eq!(listed_rows, MOR_CHANGED_ROWS, "{table}");
            assert_eq!(summary.deletes.rows, listed_rows, "{table}");
            assert_eq!(summary.deletes.files, count(deletes.len()), "{table}");
            let updated_rows = MOR_UPDATED_IDS.end - MOR_UPDATED_IDS.start;
            assert_eq!(summary.records, table.rows() + updated_rows, "{table}");
        } else {
            let dangling = format == DataFileFormat::Parquet;
            assert_eq!(!deletes.is_empty(), dangling, "{table}: {deletes:?}");
            if dangling {
                assert_eq!(listed_rows, MOR_CHANGED_ROWS, "{table}");
            }
            assert_eq!(summary.deletes.rows, 0, "{table}");
            assert_eq!(summary.deletes.files, 0, "{table}");
            let deleted_rows = MOR_DELETED_IDS.end - MOR_DELETED_IDS.start;
            assert_eq!(summary.records, table.rows() - deleted_rows, "{table}");
        }
    }
    Ok(())
}

/// Between two sampled key indexes of an ordered table: prime, so the
/// sampled keys land at varied offsets in the pages and buckets.
const ORDERED_SAMPLE_STRIDE: usize = 37;

/// The key no ordered golden table holds: below every key the recipe writes.
const ABSENT_ORDERED_KEY: i64 = -1;

/// The limit of a Scan that cuts a key's run short.
const SHORT_LIMIT: u32 = 7;

/// The limit of a Scan that reads a page past a bound.
const PAGE_LIMIT: u32 = 5;

/// The key indexes whose Scans an ordered table is checked on: a stride
/// through the table, its last key, the ends and just beyond of the keys a
/// MERGE changes, and `extra`.
fn ordered_sample(extra: &BTreeSet<u64>) -> BTreeSet<u64> {
    let mut ids: BTreeSet<u64> = (0..ORDERED_KEYS).step_by(ORDERED_SAMPLE_STRIDE).collect();
    ids.insert(ORDERED_KEYS - 1);
    ids.extend([
        MERGE_KEY_IDS.start - 1,
        MERGE_KEY_IDS.start,
        MERGE_KEY_IDS.end - 1,
        MERGE_KEY_IDS.end,
    ]);
    ids.extend(extra);
    ids
}

/// The Scans checked on a key holding `rows`, in both directions: its whole
/// run, a cut one, bounds on rows and beside them, a page past a bound, and
/// bounds that leave nothing.
fn ordered_scans(rows: &[OrderedRow]) -> Vec<TestScan> {
    let orders: Vec<i64> = rows.iter().map(|row| row.order).collect();
    let (first, last) = (orders[0], orders[orders.len() - 1]);
    let (quarter, middle, three_quarters) = (
        orders[orders.len() / 4],
        orders[orders.len() / 2],
        orders[orders.len() * 3 / 4],
    );
    let mut scans = Vec::new();
    for direction in [Direction::NewestFirst, Direction::OldestFirst] {
        let scan = |limit, after, before| TestScan {
            direction,
            limit,
            after,
            before,
            max_scan_rows: DEFAULT_MAX_SCAN_ROWS,
        };
        scans.extend([
            scan(0, None, None),
            scan(SHORT_LIMIT, None, None),
            scan(0, Some(quarter), Some(three_quarters)),
            scan(SHORT_LIMIT, Some(quarter), Some(three_quarters)),
            scan(PAGE_LIMIT, Some(middle), None),
            scan(PAGE_LIMIT, None, Some(middle + 1)),
            scan(0, Some(middle - 1), Some(middle + 1)),
            scan(0, Some(last), None),
            scan(0, None, Some(first)),
            scan(0, Some(first - 1), Some(last + 1)),
        ]);
    }
    scans
}

/// What a Scan of key index `id` answers under `scan`, from the recipe's
/// derivation of the key's rows.
fn derived_scan(table: GoldenTable, id: u64, rows: &[OrderedRow], scan: &TestScan) -> Scanned {
    let live = rows.iter().map(|row| (row.order, row.version));
    let (answer, more) = scan_model(live, scan);
    Scanned {
        rows: answer
            .into_iter()
            .map(|(order, version)| ScanRow {
                order: golden::ordered_order_value(table, id, order),
                values: vec![Some(Bytes::from(golden::ordered_value(
                    table, id, order, version,
                )))],
            })
            .collect(),
        more,
    }
}

/// Every sampled key of `table`, and the key indexes in `extra`, answers
/// every Scan of `ordered_scans` in every tier as the derivation says, and a
/// key the table lacks answers nothing.
async fn assert_ordered_scans_match_the_derivation(
    table: GoldenTable,
    extra: &BTreeSet<u64>,
) -> Result<()> {
    let scratch = tempfile::tempdir()?;
    let mut compared = 0_u64;
    let (mut cut, mut empty) = (0_u64, 0_u64);
    for residency in Residency::ALL {
        let served = open_in(table, residency, scratch.path()).await?;
        for id in ordered_sample(extra) {
            let key = golden::ordered_key(table, id);
            let rows = golden::ordered_rows(table, id);
            for scan in ordered_scans(&rows) {
                let want = derived_scan(table, id, &rows, &scan);
                let got = served
                    .table
                    .scan(
                        &key,
                        &[ColumnRead::whole(VALUE_COLUMN)],
                        &scan.bounds_by(|order| golden::ordered_order_value(table, id, order)),
                    )
                    .await?
                    .scanned;
                assert_eq!(got, want, "{table} {residency} {key} {scan:?}");
                compared += count(want.rows.len());
                cut += u64::from(want.more);
                empty += u64::from(want.rows.is_empty());
            }
        }
        let absent = Key::Long(ABSENT_ORDERED_KEY);
        let none = served
            .table
            .scan(
                &absent,
                &[ColumnRead::whole(VALUE_COLUMN)],
                &ScanBounds {
                    direction: Direction::NewestFirst,
                    limit: 0,
                    after: None,
                    before: None,
                },
            )
            .await?
            .scanned;
        assert!(none.rows.is_empty() && !none.more, "{table} {residency}");
    }
    assert!(compared > 0, "{table}: the scans compared rows");
    assert!(cut > 0, "{table}: some scans left rows past their limit");
    assert!(
        empty > 0,
        "{table}: some scans left nothing inside their bounds"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "needs ICETRAY_GOLDEN_WAREHOUSE; run dev/test-golden.sh"]
async fn test_golden_ordered_compacted_scans_match_the_derived_rows() -> Result<()> {
    assert_ordered_scans_match_the_derivation(GoldenTable::OrderedCompacted100k, &BTreeSet::new())
        .await
}

#[tokio::test]
#[ignore = "needs ICETRAY_GOLDEN_WAREHOUSE; run dev/test-golden.sh"]
async fn test_golden_ordered_appended_scans_match_the_derived_rows() -> Result<()> {
    assert_ordered_scans_match_the_derivation(GoldenTable::OrderedAppended100k, &BTreeSet::new())
        .await
}

#[tokio::test]
#[ignore = "needs ICETRAY_GOLDEN_WAREHOUSE; run dev/test-golden.sh"]
async fn test_golden_ordered_overlap_scans_match_the_derived_rows() -> Result<()> {
    assert_ordered_scans_match_the_derivation(GoldenTable::OrderedOverlap100k, &BTreeSet::new())
        .await
}

#[tokio::test]
#[ignore = "needs ICETRAY_GOLDEN_WAREHOUSE; run dev/test-golden.sh"]
async fn test_golden_ordered_merge_v2_scans_match_the_derived_rows() -> Result<()> {
    assert_ordered_scans_match_the_derivation(GoldenTable::OrderedMergeV2_100k, &BTreeSet::new())
        .await
}

#[tokio::test]
#[ignore = "needs ICETRAY_GOLDEN_WAREHOUSE; run dev/test-golden.sh"]
async fn test_golden_ordered_merge_v3_scans_match_the_derived_rows() -> Result<()> {
    assert_ordered_scans_match_the_derivation(GoldenTable::OrderedMergeV3_100k, &BTreeSet::new())
        .await
}

#[tokio::test]
#[ignore = "needs ICETRAY_GOLDEN_WAREHOUSE; run dev/test-golden.sh"]
async fn test_golden_ordered_merge_v3_compacted_scans_match_the_derived_rows() -> Result<()> {
    assert_ordered_scans_match_the_derivation(
        GoldenTable::OrderedMergeV3Compacted100k,
        &BTreeSet::new(),
    )
    .await
}

#[tokio::test]
#[ignore = "needs ICETRAY_GOLDEN_WAREHOUSE; run dev/test-golden.sh"]
async fn test_golden_ordered_string_scans_match_the_derived_rows() -> Result<()> {
    assert_ordered_scans_match_the_derivation(GoldenTable::OrderedString100k, &BTreeSet::new())
        .await
}

/// The ordered tables a sorting compaction rewrote last, one file a bucket.
const ORDERED_COMPACTED: [GoldenTable; 3] = [
    GoldenTable::OrderedCompacted100k,
    GoldenTable::OrderedMergeV3Compacted100k,
    GoldenTable::OrderedString100k,
];

/// The ordered tables Spark left with a second file over a bucket's key
/// range, the append's and the MERGE's.
const ORDERED_OVERLAPPING: [GoldenTable; 3] = [
    GoldenTable::OrderedAppended100k,
    GoldenTable::OrderedMergeV2_100k,
    GoldenTable::OrderedMergeV3_100k,
];

/// The most rows any key of `table` holds, as the derivation says.
fn longest_derived_run(table: GoldenTable) -> u64 {
    (0..ORDERED_KEYS)
        .map(|id| count(golden::ordered_rows(table, id).len()))
        .max()
        .expect("an ordered table has keys")
}

/// What the doctor reports of every ordered table: it is read as an ordered
/// table of the order column's type, and its longest run is a run the
/// recipe wrote.
#[tokio::test]
#[ignore = "needs ICETRAY_GOLDEN_WAREHOUSE; run dev/test-golden.sh"]
async fn test_golden_ordered_doctor_reads_the_order_column_and_runs() -> Result<()> {
    for table in GoldenTable::ORDERED {
        let golden = open_golden(table, false).await?;
        let summary = golden.report().summary;
        let order = summary
            .order
            .expect("an ordered table reports its order column");
        let expected_type = if table == GoldenTable::OrderedString100k {
            OrderType::String
        } else {
            OrderType::Long
        };
        assert_eq!(order.order_type, expected_type, "{table}");
        assert_eq!(order.max_scan_rows, DEFAULT_MAX_SCAN_ROWS, "{table}");
        assert!(order.longest_run > 0, "{table}");
        let derived = longest_derived_run(table);
        if ORDERED_COMPACTED.contains(&table) {
            assert_eq!(order.longest_run, derived, "{table}");
        } else {
            assert!(order.longest_run <= derived, "{table}");
        }
        let advice = advice_codes(&golden);
        assert!(
            !advice.contains(&FindingCode::LongRuns),
            "{table} {advice:?}"
        );
        assert!(
            !golden.report().files.is_empty()
                && golden
                    .report()
                    .files
                    .iter()
                    .all(|file| file.row_groups == 1),
            "{table}: Spark writes one row group a file at this size"
        );
    }
    Ok(())
}

/// A sorting compaction leaves one file in each bucket, as Spark sizes the
/// output for the target file size, so a Scan reads one file: `optimal`
/// with no capping finding, whatever the run length. The string table's
/// writer cuts its order column index, which icetray never reads, and it
/// stays `optimal`.
#[tokio::test]
#[ignore = "needs ICETRAY_GOLDEN_WAREHOUSE; run dev/test-golden.sh"]
async fn test_golden_ordered_compacted_is_optimal_with_one_file_per_bucket() -> Result<()> {
    for table in ORDERED_COMPACTED {
        let golden = open_golden(table, false).await?;
        let report = golden.report();
        assert_eq!(report.summary.level, Level::Optimal, "{table}");
        assert!(report.capping().is_empty(), "{table} {:?}", report.findings);
        assert_eq!(report.summary.max_files_per_bucket, 1, "{table}");
        assert_eq!(
            report.summary.buckets_with_files,
            usize::try_from(table.buckets()).unwrap(),
            "{table}"
        );
        assert!(golden.current().overlapping_buckets().is_empty(), "{table}");
        assert_eq!(report.summary.lookup_cost, None, "{table}");
        let order = report.summary.order.as_ref().expect("an ordered table");
        assert_eq!(order.files_probed, 1, "{table}");
        assert_eq!(
            Some(order.gets_per_scan),
            golden.current().gets_per_scan,
            "{table}: the doctor and the gauge predict one Scan"
        );
        let advice = advice_codes(&golden);
        assert!(!advice.contains(&FindingCode::DeletesPending), "{table}");
    }
    Ok(())
}

/// Appends and a MERGE add a file over each bucket's key range, so a Scan
/// reads two files for a key both hold: `permissive` under
/// `overlapping_files`, and the MERGE's deletes are pending until a
/// compaction. A MERGE's deletes are the rows it matched, recorded once per
/// bucket whichever the table's format version.
#[tokio::test]
#[ignore = "needs ICETRAY_GOLDEN_WAREHOUSE; run dev/test-golden.sh"]
async fn test_golden_ordered_appends_and_merges_are_permissive_for_overlapping_files() -> Result<()>
{
    let merged_keys = MERGE_KEY_IDS.end - MERGE_KEY_IDS.start;
    let matched_rows = merged_keys * (ORDERED_ROWS_PER_KEY / golden::MERGE_UPDATE_EVERY);
    let inserted_rows = merged_keys * (ORDERED_ROWS_PER_KEY / golden::MERGE_INSERT_EVERY);
    for table in ORDERED_OVERLAPPING {
        let golden = open_golden(table, false).await?;
        let report = golden.report();
        let buckets = usize::try_from(table.buckets()).unwrap();
        assert_eq!(report.summary.level, Level::Permissive, "{table}");
        assert_eq!(
            codes_of(&report),
            [FindingCode::OverlappingFiles],
            "{table}"
        );
        assert_eq!(
            golden.current().overlapping_buckets().len(),
            buckets,
            "{table}"
        );
        let two_files = 2;
        assert_eq!(report.summary.max_files_per_bucket, two_files, "{table}");
        assert_eq!(report.summary.lookup_cost, None, "{table}");
        assert_eq!(
            report
                .summary
                .order
                .as_ref()
                .expect("an ordered table")
                .files_probed,
            two_files,
            "{table}"
        );
        let advice = advice_codes(&golden);
        if table == GoldenTable::OrderedAppended100k {
            assert!(!advice.contains(&FindingCode::DeletesPending), "{table}");
            let appended = ORDERED_KEYS * golden::APPEND_ROWS_PER_KEY;
            assert_eq!(report.summary.records, table.rows() + appended, "{table}");
            assert_eq!(report.summary.deletes.rows, 0, "{table}");
        } else {
            assert!(
                advice.contains(&FindingCode::DeletesPending),
                "{table} {advice:?}"
            );
            assert_eq!(report.summary.deletes.rows, matched_rows, "{table}");
            assert_eq!(report.summary.deletes.files, count(buckets), "{table}");
            assert_eq!(
                report.summary.records,
                table.rows() + inserted_rows + matched_rows,
                "{table}"
            );
        }
    }
    Ok(())
}

/// A compaction after the MERGE leaves the merged rows, the deletes applied
/// and none listed.
#[tokio::test]
#[ignore = "needs ICETRAY_GOLDEN_WAREHOUSE; run dev/test-golden.sh"]
async fn test_golden_ordered_merge_compaction_applies_the_deletes() -> Result<()> {
    let table = GoldenTable::OrderedMergeV3Compacted100k;
    let golden = open_golden(table, false).await?;
    let summary = golden.report().summary;
    let merged_keys = MERGE_KEY_IDS.end - MERGE_KEY_IDS.start;
    let inserted_rows = merged_keys * (ORDERED_ROWS_PER_KEY / golden::MERGE_INSERT_EVERY);
    assert_eq!(summary.deletes.rows, 0);
    assert_eq!(summary.deletes.files, 0);
    assert_eq!(summary.records, table.rows() + inserted_rows);
    Ok(())
}

/// Skew splitting cuts each bucket's write into several sorted tasks over
/// keys uncorrelated with the order they arrive in, so a bucket holds
/// several files whose key ranges overlap: `permissive` under
/// `overlapping_files`, and a Scan probes every file whose range holds its
/// key.
#[tokio::test]
#[ignore = "needs ICETRAY_GOLDEN_WAREHOUSE; run dev/test-golden.sh"]
async fn test_golden_ordered_overlap_is_permissive_for_overlapping_files() -> Result<()> {
    let table = GoldenTable::OrderedOverlap100k;
    let golden = open_golden(table, false).await?;
    let report = golden.report();
    let generation = golden.current();
    assert_eq!(report.summary.level, Level::Permissive);
    assert_eq!(codes_of(&report), [FindingCode::OverlappingFiles]);
    assert!(
        report.summary.max_files_per_bucket > 1,
        "dev/golden-tables.py writes ordered_overlap_100k with skew splitting and a \
         small write.spark.advisory-partition-size-bytes, so each bucket is split across \
         several sorted tasks; regenerate the table"
    );
    assert!(!generation.overlapping_buckets().is_empty());
    assert_eq!(
        report.summary.records,
        table.rows(),
        "no key is repeated across files with the same order value"
    );
    let deepest = generation
        .files
        .iter()
        .map(|file| generation.candidates(file.index.key_range().0).count())
        .max()
        .expect("the table has files");
    assert!(deepest > 1, "overlapping files share a key range");
    assert_eq!(report.summary.lookup_cost, None);
    assert_eq!(
        report
            .summary
            .order
            .as_ref()
            .expect("an ordered table")
            .files_probed,
        deepest
    );
    Ok(())
}

/// The key indexes of the keys two files of one bucket share as an edge: one
/// file's greatest key is the next file's least.
fn shared_edge_ids(golden: &Golden) -> BTreeSet<u64> {
    let generation = golden.current();
    let mut ids = BTreeSet::new();
    for (index, file) in generation.files.iter().enumerate() {
        let (_, greatest) = file.index.key_range();
        let sharing = generation.files[index + 1..]
            .iter()
            .filter(|other| other.entry.bucket == file.entry.bucket)
            .filter(|other| other.index.key_range().0 == greatest);
        if sharing.count() > 0 {
            let Key::Long(value) = greatest else {
                panic!("an ordered golden table has long keys, got {greatest}");
            };
            ids.insert(u64::try_from(*value).expect("a golden key is not negative") / KEY_STEP);
        }
    }
    ids
}

/// A compaction that rolls each bucket by size writes several files whose
/// key ranges meet where the sort cut them, inside a key's run: `balanced`
/// under `shared_edge_keys`, and no more. A Scan of a shared edge key reads
/// both files that hold it and still answers as the derivation says.
#[tokio::test]
#[ignore = "needs ICETRAY_GOLDEN_WAREHOUSE; run dev/test-golden.sh"]
async fn test_golden_ordered_rolled_is_balanced_for_shared_edge_keys() -> Result<()> {
    let table = GoldenTable::OrderedRolled100k;
    let golden = open_golden(table, false).await?;
    let report = golden.report();
    assert_eq!(report.summary.level, Level::Balanced);
    assert_eq!(codes_of(&report), [FindingCode::SharedEdgeKeys]);
    assert!(
        report.summary.max_files_per_bucket > 1,
        "dev/golden-tables.py compacts ordered_rolled_100k with a target file size below \
         a bucket's bytes, so each bucket rolls into several files; regenerate the table"
    );
    assert!(
        golden.current().overlapping_buckets().is_empty(),
        "rolled files meet at edge keys and overlap nowhere"
    );
    let edges = shared_edge_ids(&golden);
    assert!(!edges.is_empty(), "the compaction shares edge keys");
    let generation = golden.current();
    for &id in &edges {
        let key = golden::ordered_key(table, id);
        let two_files = 2;
        assert_eq!(
            generation.candidates(&key).count(),
            two_files,
            "edge key {key}"
        );
    }
    assert_ordered_scans_match_the_derivation(table, &edges).await
}

/// The MERGE's deletes are what the table's format version records: Parquet
/// position delete files naming no data file in version 2, deletion vectors
/// in Puffin files, each naming its data file, in version 3. Each holds the
/// rows the MERGE matched, one file for each bucket's base file.
#[tokio::test]
#[ignore = "needs ICETRAY_GOLDEN_WAREHOUSE; run dev/test-golden.sh"]
async fn test_golden_ordered_merge_records_the_deletes_of_its_format_version() -> Result<()> {
    let merged_keys = MERGE_KEY_IDS.end - MERGE_KEY_IDS.start;
    let matched_rows = merged_keys * (ORDERED_ROWS_PER_KEY / golden::MERGE_UPDATE_EVERY);
    let tables = [
        (GoldenTable::OrderedMergeV2_100k, DataFileFormat::Parquet),
        (GoldenTable::OrderedMergeV3_100k, DataFileFormat::Puffin),
    ];
    for (table, format) in tables {
        let golden = open_golden(table, false).await?;
        let deletes = listed_deletes(&golden).await?;
        assert_eq!(
            deletes.len(),
            usize::try_from(table.buckets()).unwrap(),
            "{table}"
        );
        for delete in &deletes {
            let path = delete.file_path();
            assert_eq!(
                delete.content_type(),
                DataContentType::PositionDeletes,
                "{table} {path}"
            );
            assert_eq!(delete.file_format(), format, "{table} {path}");
            assert_eq!(
                delete.referenced_data_file().is_some(),
                format == DataFileFormat::Puffin,
                "{table} {path}"
            );
        }
        let listed_rows: u64 = deletes.iter().map(DataFile::record_count).sum();
        assert_eq!(listed_rows, matched_rows, "{table}");
    }
    Ok(())
}
