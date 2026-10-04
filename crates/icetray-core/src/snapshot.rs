//! The newtypes of an Iceberg snapshot that cross from metadata into
//! lookups, reports, and metrics: its id, which names it, and its sequence
//! number, which orders it among the table's commits.

use std::fmt;

/// A snapshot id. It is a newtype so that it is never compared with or
/// added to a bare integer, and it has no order: Iceberg ids are random, and
/// snapshots order by sequence number. It is not a gauge value, where an
/// `f64` would lose its low digits (RFD 015), nor a metric label, where each
/// swap would add a series that stays until the process exits; logs and the
/// table-info reply carry it. It serializes as its decimal string, as
/// proto3's JSON mapping renders an int64: ids routinely exceed two to the
/// fifty-third, past which a JSON reader that holds numbers as `f64` (jq,
/// JavaScript) rounds an integer to a neighboring id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SnapshotId(pub i64);

impl fmt::Display for SnapshotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl serde::Serialize for SnapshotId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
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

    /// The first integer an `f64` cannot hold, so a reader that holds JSON
    /// numbers as `f64` rounds it.
    const PAST_F64_INTEGERS: i64 = (1 << f64::MANTISSA_DIGITS) + 1;

    /// A JSON scalar as a reader that holds every number as an `f64` reads
    /// it: jq, or JavaScript's `JSON.parse`.
    #[derive(Debug, PartialEq, serde::Deserialize)]
    #[serde(untagged)]
    enum F64Reader {
        Number(f64),
        Text(String),
    }

    #[test]
    fn test_serialize_snapshot_id_as_its_decimal_string() {
        let json = serde_json::to_string(&SnapshotId(SAMPLE_SNAPSHOT_ID)).unwrap();
        assert_eq!(json, format!("\"{SAMPLE_SNAPSHOT_ID}\""));
    }

    #[test]
    fn test_serialize_snapshot_id_past_f64_integers_survives_an_f64_reader() {
        let as_number: F64Reader = serde_json::from_str(&PAST_F64_INTEGERS.to_string()).unwrap();
        let F64Reader::Number(rounded) = as_number else {
            panic!("a bare integer reads as a number");
        };
        assert_ne!(
            rounded.to_string(),
            PAST_F64_INTEGERS.to_string(),
            "the id as a number does not survive the reader"
        );
        let json = serde_json::to_string(&SnapshotId(PAST_F64_INTEGERS)).unwrap();
        let read: F64Reader = serde_json::from_str(&json).unwrap();
        let F64Reader::Text(text) = read else {
            panic!("the id reads as text, not a number: {json}");
        };
        assert_eq!(text.parse::<i64>().unwrap(), PAST_F64_INTEGERS);
    }
}
