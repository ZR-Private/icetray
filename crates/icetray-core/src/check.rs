//! The doctor (RFD 007, RFD 016): the tiers, finding codes, penalties, and
//! thresholds by which a table is classified, applied identically at every
//! load and in the `doctor` subcommand.

use std::fmt;

/// A finding's code (RFD 016). Display and serde give the `snake_case` code
/// that appears in the doctor's output and the `code` metric label; code
/// never compares the strings.
// The serde names and Display are the same strings;
// `test_serialize_finding_code_matches_display` holds them together.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FindingCode {
    /// The value column is dictionary-encoded; its dictionary page costs one
    /// extra GET per hit.
    DictionaryEncodedValue,
    /// Key pages hold more rows than the cost threshold; a lookup decodes
    /// them all.
    LargeKeyPages,
    /// No bucket transform on the key and more than one file; a lookup
    /// probes every file.
    NoBucketPartition,
    /// Files within one bucket have intersecting exact key ranges; a lookup
    /// probes each of them.
    OverlappingFiles,
    /// A manifest lacks key bounds, or its bounds are shorter than the
    /// file's exact first or last key.
    BoundsMissing,
    /// The key column's column index is truncated: too few page minimums
    /// are distinct.
    ColumnIndexTruncated,
    /// The mean uncompressed value page is above the advice threshold.
    LargeValuePages,
    /// A key is longer than `contract::KEY_BYTES_ADVICE`.
    LongKeys,
    /// A data file is larger than the advice threshold.
    LargeFile,
    /// A bucket holds more files than its bytes and the file size target
    /// explain.
    ManyFilesPerBucket,
    /// The key column has no bloom filter.
    NoBloomFilter,
    /// The key bloom filter spends more bytes per key than the advice
    /// threshold.
    BloomOversized,
    /// The key bloom filter has fewer bits per key than the minimum; the
    /// Bloom tier's miss cost rises.
    BloomUndersized,
}

impl fmt::Display for FindingCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let code = match self {
            Self::DictionaryEncodedValue => "dictionary_encoded_value",
            Self::LargeKeyPages => "large_key_pages",
            Self::NoBucketPartition => "no_bucket_partition",
            Self::OverlappingFiles => "overlapping_files",
            Self::BoundsMissing => "bounds_missing",
            Self::ColumnIndexTruncated => "column_index_truncated",
            Self::LargeValuePages => "large_value_pages",
            Self::LongKeys => "long_keys",
            Self::LargeFile => "large_file",
            Self::ManyFilesPerBucket => "many_files_per_bucket",
            Self::NoBloomFilter => "no_bloom_filter",
            Self::BloomOversized => "bloom_oversized",
            Self::BloomUndersized => "bloom_undersized",
        };
        f.write_str(code)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CODES: &[(FindingCode, &str)] = &[
        (
            FindingCode::DictionaryEncodedValue,
            "dictionary_encoded_value",
        ),
        (FindingCode::LargeKeyPages, "large_key_pages"),
        (FindingCode::NoBucketPartition, "no_bucket_partition"),
        (FindingCode::OverlappingFiles, "overlapping_files"),
        (FindingCode::BoundsMissing, "bounds_missing"),
        (FindingCode::ColumnIndexTruncated, "column_index_truncated"),
        (FindingCode::LargeValuePages, "large_value_pages"),
        (FindingCode::LongKeys, "long_keys"),
        (FindingCode::LargeFile, "large_file"),
        (FindingCode::ManyFilesPerBucket, "many_files_per_bucket"),
        (FindingCode::NoBloomFilter, "no_bloom_filter"),
        (FindingCode::BloomOversized, "bloom_oversized"),
        (FindingCode::BloomUndersized, "bloom_undersized"),
    ];

    #[test]
    fn test_fmt_finding_code_is_the_code_column() {
        for &(code, name) in CODES {
            assert_eq!(code.to_string(), name);
        }
    }

    #[test]
    fn test_serialize_finding_code_matches_display() {
        for &(code, name) in CODES {
            let json = serde_json::to_string(&code).unwrap();
            assert_eq!(json, format!("\"{name}\""));
            let back: FindingCode = serde_json::from_str(&json).unwrap();
            assert_eq!(back, code);
        }
    }
}
