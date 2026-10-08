//! The golden tables `dev/golden-tables.py` wrote with a real batch writer
//! (the G tier of RFD 014), located through `ICETRAY_GOLDEN_WAREHOUSE` and
//! opened through `icetray_core::open`. Every test is ignored by default and
//! run by `dev/test-golden.sh`; forced without the variable, each fails on
//! `Precondition::GoldenUnset` rather than passing.

use std::path::Path;
use std::sync::Arc;

use iceberg::spec::{
    DataContentType, DataFile, DataFileFormat, FormatVersion, Manifest, ManifestList, TableMetadata,
};
use icetray_core::approx::count;
use icetray_core::config::{Config, LocalConfig, StorageConfig};
use icetray_core::{
    Error, Finding, FindingCode, FindingLog, Generation, Key, Level, LookupCost, RefusalReason,
    Residency, Row, Storage, Table, TableReport, diagnose, open,
};
use icetray_testkit::golden::{
    self, CONTRACT_REPEATS, GoldenTable, HEX_DIGEST_BYTES, MOR_DELETED_IDS, MOR_UPDATED_IDS,
    WIDE_REPEATS,
};
use icetray_testkit::layout::VALUE_COLUMN;
use icetray_testkit::verify::{self, SeenPages};
use icetray_testkit::{Result, absent_key_from};

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
        diagnose(&self.current(), self.table.config().lookups())
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
        let lookup = golden.table.get(key, &[VALUE_COLUMN]).await?;
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
            golden.table.get(key, &[VALUE_COLUMN]).await?.row,
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
    assert_eq!(golden.report().summary.lookup_cost, expected);
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
        let row = golden.table.get(key, &[VALUE_COLUMN]).await?.row;
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
            golden.table.get(key, &[VALUE_COLUMN]).await?.row,
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
    for residency in [Residency::Full, Residency::Disk, Residency::Bloom] {
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
            let answer = served.table.get(&key, &[VALUE_COLUMN]).await?.row;
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
