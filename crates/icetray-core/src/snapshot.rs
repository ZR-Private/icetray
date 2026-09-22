//! The snapshot id newtype: the one identifier of an Iceberg snapshot that
//! crosses from metadata into lookups, reports, and metric labels.

use std::fmt;

/// A snapshot id. It is a newtype so that it is never compared with or
/// added to a bare integer, and it has no order: Iceberg ids are random, and
/// snapshots order by sequence number. It is exported as a metric label
/// rather than a gauge value, where an `f64` would lose its low digits
/// (RFD 015). Serde is transparent: the id is the integer on the wire and in
/// JSON.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct SnapshotId(pub i64);

impl fmt::Display for SnapshotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An id wider than 32 bits, so the round trip exercises the whole `i64`.
    const SAMPLE_SNAPSHOT_ID: i64 = 5_123_456_789_012;

    #[test]
    fn test_fmt_snapshot_id_is_the_integer() {
        assert_eq!(
            SnapshotId(SAMPLE_SNAPSHOT_ID).to_string(),
            SAMPLE_SNAPSHOT_ID.to_string()
        );
    }

    #[test]
    fn test_serialize_snapshot_id_transparent() {
        let json = serde_json::to_string(&SnapshotId(SAMPLE_SNAPSHOT_ID)).unwrap();
        assert_eq!(json, SAMPLE_SNAPSHOT_ID.to_string());
    }

    #[test]
    fn test_deserialize_snapshot_id_transparent() {
        let id: SnapshotId = serde_json::from_str(&SAMPLE_SNAPSHOT_ID.to_string()).unwrap();
        assert_eq!(id, SnapshotId(SAMPLE_SNAPSHOT_ID));
    }
}
