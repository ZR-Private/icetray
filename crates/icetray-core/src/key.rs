//! Lookup keys (RFD 008): the key types icetray serves and the order it
//! compares them in. A composite key is the caller's encoding into one of
//! them; icetray moves the bytes unchanged.

use std::fmt;

/// The Iceberg type of a table's key column (RFD 008), one of four; Iceberg
/// `fixed` is served as `Binary`. Display and serde give the lowercase
/// Iceberg type name that appears in the doctor's output and in logs.
// The serde names and Display are the same strings;
// `test_serialize_key_type_matches_display` holds them together.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KeyType {
    /// Iceberg `long`: a 64-bit signed integer, compared numerically.
    Long,
    /// Iceberg `string`: UTF-8 text, compared as its bytes.
    String,
    /// Iceberg `binary` or `fixed`: opaque bytes, compared bytewise.
    Binary,
    /// Iceberg `uuid`: sixteen bytes, compared in the order the Iceberg
    /// specification defines for the type, which is not assumed bytewise.
    Uuid,
}

impl fmt::Display for KeyType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Long => "long",
            Self::String => "string",
            Self::Binary => "binary",
            Self::Uuid => "uuid",
        };
        f.write_str(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fmt_key_type_lowercase_iceberg_names() {
        let cases = [
            (KeyType::Long, "long"),
            (KeyType::String, "string"),
            (KeyType::Binary, "binary"),
            (KeyType::Uuid, "uuid"),
        ];
        for (key_type, name) in cases {
            assert_eq!(key_type.to_string(), name);
        }
    }

    #[test]
    fn test_serialize_key_type_matches_display() {
        for key_type in [
            KeyType::Long,
            KeyType::String,
            KeyType::Binary,
            KeyType::Uuid,
        ] {
            let json = serde_json::to_string(&key_type).unwrap();
            assert_eq!(json, format!("\"{key_type}\""));
            let back: KeyType = serde_json::from_str(&json).unwrap();
            assert_eq!(back, key_type);
        }
    }
}
