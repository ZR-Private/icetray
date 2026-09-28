//! The truth file a check replays against a server: JSON Lines, one object
//! per key, `{"key": "user:0042", "value_base64": "aGk="}`, where `key` is a
//! string key as the table holds it and `value_base64` is the value the
//! table must answer, in standard base64 with padding, or `null` for a key
//! the table must not hold. A key held with a null value has no line form.
//! Blank lines are skipped; any other line that is not that object fails
//! the whole file, naming its line.

use std::io::BufRead;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde::Deserialize;

use crate::error::Result;

/// One key and what the table must answer for it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    /// The key, as the table's string key column holds it.
    pub key: String,
    /// The value's bytes, or `None` for a key that must be absent.
    pub expected: Option<Vec<u8>>,
}

/// What makes a truth-file line unreadable; closed. `line` counts from
/// one, blank lines included, as an editor numbers them.
#[derive(Debug, thiserror::Error)]
pub enum TruthFault {
    /// The line is not the object the format describes.
    #[error("line {line}: {source}")]
    Json {
        /// The line's number.
        line: usize,
        /// Why the line is not that object.
        source: serde_json::Error,
    },
    /// The line's `value_base64` is not standard base64.
    #[error("line {line}: value_base64: {source}")]
    Base64 {
        /// The line's number.
        line: usize,
        /// Why the value does not decode.
        source: base64::DecodeError,
    },
}

/// One line as it is written. Both fields are required and no other is
/// allowed, so a misspelled or missing `value_base64` fails the line
/// instead of reading as a key that must be absent; `deserialize_with`
/// is what keeps serde from defaulting the missing `Option` to `None`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Line {
    key: String,
    #[serde(deserialize_with = "Option::deserialize")]
    value_base64: Option<String>,
}

/// Every entry of the truth file `reader` holds, in file order.
pub fn parse(reader: impl BufRead) -> Result<Vec<Entry>> {
    let mut entries = Vec::new();
    for (index, text) in reader.lines().enumerate() {
        let text = text?;
        if text.trim().is_empty() {
            continue;
        }
        let line = index + 1;
        let parsed: Line =
            serde_json::from_str(&text).map_err(|source| TruthFault::Json { line, source })?;
        let expected = parsed
            .value_base64
            .map(|encoded| STANDARD.decode(encoded))
            .transpose()
            .map_err(|source| TruthFault::Base64 { line, source })?;
        entries.push(Entry {
            key: parsed.key,
            expected,
        });
    }
    Ok(entries)
}

/// How one answer compares with the entry it answers; closed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The answer is what the entry expects: the same bytes, or absent.
    Match,
    /// Expected present and answered present with other bytes.
    WrongValue {
        /// The length of the expected value.
        expected_len: usize,
        /// The length of the value answered.
        actual_len: usize,
    },
    /// Expected present, answered absent.
    MissingValue,
    /// Expected absent, answered present.
    UnexpectedValue,
}

/// How the answer `actual` compares with `expected`, `None` meaning absent
/// on both sides.
pub fn compare(expected: Option<&[u8]>, actual: Option<&[u8]>) -> Outcome {
    match (expected, actual) {
        (Some(expected), Some(actual)) if expected == actual => Outcome::Match,
        (Some(expected), Some(actual)) => Outcome::WrongValue {
            expected_len: expected.len(),
            actual_len: actual.len(),
        },
        (None, None) => Outcome::Match,
        (Some(_), None) => Outcome::MissingValue,
        (None, Some(_)) => Outcome::UnexpectedValue,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TestkitError;

    /// A present line and an absent line, the value `hi` in base64.
    const TWO_LINES: &str = concat!(
        "{\"key\":\"user:0042\",\"value_base64\":\"aGk=\"}\n",
        "\n",
        "{\"key\":\"user:0043\",\"value_base64\":null}\n",
    );

    #[test]
    fn test_parse_reads_a_present_and_an_absent_line() {
        let entries = parse(TWO_LINES.as_bytes()).unwrap();
        assert_eq!(
            entries,
            [
                Entry {
                    key: "user:0042".into(),
                    expected: Some(b"hi".to_vec()),
                },
                Entry {
                    key: "user:0043".into(),
                    expected: None,
                },
            ],
            "the blank line between them is skipped"
        );
    }

    #[test]
    fn test_parse_names_the_line_of_bad_base64() {
        let text = concat!(
            "{\"key\":\"user:0042\",\"value_base64\":\"aGk=\"}\n",
            "{\"key\":\"user:0043\",\"value_base64\":\"!!\"}\n",
        );
        let error = parse(text.as_bytes()).unwrap_err();
        assert!(
            matches!(
                error,
                TestkitError::Truth(TruthFault::Base64 { line: 2, .. })
            ),
            "{error}"
        );
    }

    #[test]
    fn test_parse_names_the_line_of_bad_json() {
        let text = concat!(
            "{\"key\":\"user:0042\",\"value_base64\":null}\n",
            "\n",
            "{\"value_base64\":null}\n",
        );
        let error = parse(text.as_bytes()).unwrap_err();
        assert!(
            matches!(error, TestkitError::Truth(TruthFault::Json { line: 3, .. })),
            "{error}"
        );
    }

    #[test]
    fn test_parse_refuses_a_missing_or_misspelled_value() {
        let missing = "{\"key\":\"user:0042\"}\n";
        let misspelled = "{\"key\":\"user:0042\",\"value_b64\":null}\n";
        for text in [missing, misspelled] {
            let error = parse(text.as_bytes()).unwrap_err();
            assert!(
                matches!(error, TestkitError::Truth(TruthFault::Json { line: 1, .. })),
                "{text}: {error}"
            );
        }
    }

    #[test]
    fn test_compare_covers_every_combination() {
        let hi: &[u8] = b"hi";
        let hello: &[u8] = b"hello";
        assert_eq!(compare(Some(hi), Some(hi)), Outcome::Match);
        assert_eq!(compare(None, None), Outcome::Match);
        assert_eq!(
            compare(Some(hi), Some(hello)),
            Outcome::WrongValue {
                expected_len: hi.len(),
                actual_len: hello.len(),
            }
        );
        assert_eq!(compare(Some(hi), None), Outcome::MissingValue);
        assert_eq!(compare(None, Some(hi)), Outcome::UnexpectedValue);
    }
}
