//! The golden tables `dev/golden-tables.py` wrote with a real batch writer
//! (the G tier of RFD 014), located through `ICETRAY_GOLDEN_WAREHOUSE` and
//! opened through `icetray_core::open`. Every test is ignored by default and
//! run by `dev/test-golden.sh`; forced without the variable, each fails on
//! `Precondition::GoldenUnset` rather than passing.

use std::sync::Arc;

use iceberg::spec::{FormatVersion, TableMetadata};
use icetray_core::check::Tier;
use icetray_core::{
    Error, Finding, FindingCode, Generation, Key, Serviceability, Storage, Table, Value, diagnose,
    open,
};
use icetray_testkit::golden::{
    self, CONTRACT_REPEATS, GoldenTable, HEX_DIGEST_BYTES, WIDE_REPEATS,
};
use icetray_testkit::verify;
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
}

async fn open_golden(table: GoldenTable, strict: bool) -> Result<Golden> {
    let warehouse = golden::warehouse()?;
    let opened = open(&golden::config(&warehouse, table, strict), None).await?;
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
        let lookup = golden.table.get(key).await?;
        let Value::Present(bytes) = lookup.value else {
            panic!("key {key} is {:?}", lookup.value);
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
            golden.table.get(key).await?.value,
            Value::Absent,
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
    let cold = verify::expected_cold_gets(&generation, keys)?;
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

fn inefficient_codes(golden: &Golden) -> Vec<FindingCode> {
    diagnose(&golden.current())
        .findings
        .iter()
        .filter(|finding| finding.tier() == Tier::Inefficient)
        .map(Finding::code)
        .collect()
}

fn advice_codes(golden: &Golden) -> Vec<FindingCode> {
    diagnose(&golden.current())
        .findings
        .iter()
        .filter(|finding| finding.tier() == Tier::Advice)
        .map(Finding::code)
        .collect()
}

#[tokio::test]
#[ignore = "needs ICETRAY_GOLDEN_WAREHOUSE; run dev/test-golden.sh"]
async fn test_golden_long_contract_serves_one_get_per_hit() -> Result<()> {
    let golden = open_golden(GoldenTable::Long2m, false).await?;
    let report = diagnose(&golden.current());
    assert!(
        report.findings.is_empty(),
        "the contract table draws no finding: {:?}",
        report.findings
    );
    assert_eq!(report.summary.serviceability, Serviceability::Efficient);
    let keys: Vec<Key> = IDS_2M.iter().map(|&id| golden::long_key(id)).collect();
    verify_contract_hits(&golden, &keys, |key| golden::value(&long_text(key))).await
}

#[tokio::test]
#[ignore = "needs ICETRAY_GOLDEN_WAREHOUSE; run dev/test-golden.sh"]
async fn test_golden_string_contract_serves() -> Result<()> {
    let golden = open_golden(GoldenTable::Str2m, false).await?;
    assert!(inefficient_codes(&golden).is_empty());
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
    assert!(inefficient_codes(&golden).is_empty());
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
        let Value::Present(bytes) = golden.table.get(key).await?.value else {
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
            golden.table.get(key).await?.value,
            Value::Absent,
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
    let keys: Vec<Key> = IDS_250K.iter().map(|&id| golden::long_key(id)).collect();
    verify_contract_hits(&golden, &keys, |key| {
        let value = golden::wide_value(&long_text(key));
        assert_eq!(value.len(), WIDE_REPEATS * HEX_DIGEST_BYTES);
        value
    })
    .await
}

/// A contract table in table format version 3 serves as version 2 does:
/// the doctor finds it efficient with no inefficient finding, and a cold hit costs
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
    let report = diagnose(&generation);
    assert_eq!(report.summary.serviceability, Serviceability::Efficient);
    assert!(
        inefficient_codes(&golden).is_empty(),
        "{:?}",
        report.findings
    );
    let keys: Vec<Key> = IDS_50K.iter().map(|&id| golden::long_key(id)).collect();
    verify_contract_hits(&golden, &keys, |key| golden::value(&long_text(key))).await
}

#[tokio::test]
#[ignore = "needs ICETRAY_GOLDEN_WAREHOUSE; run dev/test-golden.sh"]
async fn test_golden_default_layout_is_inefficient() -> Result<()> {
    let golden = open_golden(GoldenTable::DefaultLayout1m, false).await?;
    let report = diagnose(&golden.current());
    assert_eq!(report.summary.serviceability, Serviceability::Inefficient);
    assert!(inefficient_codes(&golden).contains(&FindingCode::LargeKeyPages));
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
    )
    .await?;
    let error = opened.tables()[0].load().await.unwrap_err();
    let Error::Inefficient { findings, .. } = error else {
        panic!("strict refuses the default layout: {error:?}");
    };
    assert!(
        findings.contains(&FindingCode::LargeKeyPages),
        "{findings:?}"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "needs ICETRAY_GOLDEN_WAREHOUSE; run dev/test-golden.sh"]
async fn test_golden_unpartitioned_is_inefficient() -> Result<()> {
    let golden = open_golden(GoldenTable::Unpartitioned500k, false).await?;
    assert!(inefficient_codes(&golden).contains(&FindingCode::NoBucketPartition));
    let keys: Vec<Key> = IDS_500K.iter().map(|&id| golden::long_key(id)).collect();
    verify_contract_hits(&golden, &keys, |key| golden::value(&long_text(key))).await
}

#[tokio::test]
#[ignore = "needs ICETRAY_GOLDEN_WAREHOUSE; run dev/test-golden.sh"]
async fn test_golden_overlap_serves_and_reports_overlap_inefficient() -> Result<()> {
    let golden = open_golden(GoldenTable::Overlap250k, false).await?;
    let keys: Vec<Key> = IDS_250K
        .iter()
        .map(|&id| golden::permuted_key(id))
        .collect();
    let generation = golden.current();
    let cold = verify::expected_cold_gets(&generation, &keys)?;
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
    let inefficient = inefficient_codes(&golden);
    assert!(
        inefficient.contains(&FindingCode::OverlappingFiles),
        "{inefficient:?}"
    );
    Ok(())
}
