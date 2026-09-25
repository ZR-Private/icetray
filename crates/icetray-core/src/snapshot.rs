//! The newtypes of an Iceberg snapshot that cross from metadata into
//! lookups, reports, and metrics: its id, which names it, and its sequence
//! number, which orders it among the table's commits.

use std::fmt;

/// A snapshot id. It is a newtype so that it is never compared with or
/// added to a bare integer, and it has no order: Iceberg ids are random, and
/// snapshots order by sequence number. It is not a gauge value, where an
/// `f64` would lose its low digits (RFD 015), nor a metric label, where each
/// swap would add a series that stays until the process exits; logs and the
/// table-info reply carry it. Serde is transparent: the id is the integer on the wire and in JSON.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct SnapshotId(pub i64);

impl fmt::Display for SnapshotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A snapshot's sequence number: Iceberg's count of the table's commits,
/// which rises with each one. A newtype, so that it cannot be passed where
/// the snapshot id beside it is expected. Unlike the id it is a gauge value
/// (`metric::GENERATION_SEQUENCE_NUMBER`): a count of commits stays exact
/// in an `f64` up to two to the fifty-third of them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SequenceNumber(pub i64);

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
