//! The built binary against the golden tables `dev/golden-tables.py` wrote
//! (the G tier of RFD 014), located through `ICETRAY_GOLDEN_WAREHOUSE` and
//! read through the rules of `ICETRAY_GOLDEN_REWRITE`, each passed as a
//! `--rewrite`.
//! Every test is ignored by default and run by `dev/test-golden.sh`; forced
//! without the variable, each fails on `Precondition::GoldenUnset` rather
//! than passing.

use std::process::Command;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use icetray::EXIT_OK;
use icetray::cli::ENV_CONFIG;
use icetray_core::Key;
use icetray_testkit::Result;
use icetray_testkit::golden::{self, GoldenTable};
use icetray_testkit::layout::VALUE_COLUMN;
use serde_json::Value as Json;

/// The built binary under test.
const BIN: &str = env!("CARGO_BIN_EXE_icetray");

/// Ids whose keys each test looks up on a two-million-row table: the ends
/// and the middle.
const IDS_2M: [u64; 3] = [0, 999_999, 1_999_999];

/// The one `lookup` line of `icetray get --location <table> --value-column
/// value --column value -- <text>`, with a `--rewrite` for each of the
/// warehouse's rules, which must exit 0.
fn get(warehouse: &golden::Warehouse, table: GoldenTable, text: &str) -> Result<Json> {
    let rewrites = warehouse
        .rewrite
        .iter()
        .flat_map(|rule| ["--rewrite".to_string(), rule.to_string()]);
    let output = Command::new(BIN)
        .args([
            "get",
            "--location",
            &golden::location(&warehouse.location, table),
        ])
        .args(rewrites)
        .args(["--value-column", VALUE_COLUMN, "--column", VALUE_COLUMN])
        .args(["--", text])
        .env_remove(ENV_CONFIG)
        .output()?;
    assert_eq!(
        output.status.code(),
        Some(i32::from(EXIT_OK)),
        "{table} {text}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines.len(), 1, "{table} {text}: {stdout}");
    Ok(serde_json::from_str(lines[0])?)
}

/// For each id, a `get` of the key's Spark text (a long's digits, a string
/// itself) prints that text as `key`, and a `get` of the printed `key`
/// finds the same key with the value the recipe derives from that text.
fn assert_key_field_round_trips(table: GoldenTable, key_of: fn(u64) -> Key) -> Result<()> {
    let warehouse = golden::Warehouse::from_env()?;
    for id in IDS_2M {
        let text = match key_of(id) {
            Key::Long(value) => value.to_string(),
            Key::Str(text) => text,
            other @ (Key::Bytes(_) | Key::Uuid(_)) => {
                panic!("{table} has long or string keys, got {other}")
            }
        };
        let first = get(&warehouse, table, &text)?;
        assert_eq!(first["key"], Json::String(text.clone()), "{first}");
        let Json::String(printed) = &first["key"] else {
            panic!("the key is a JSON string in {first}");
        };
        let again = get(&warehouse, table, printed)?;
        assert_eq!(again["presence"], "present", "{again}");
        assert_eq!(again["key"], first["key"], "{again}");
        let expected = STANDARD.encode(golden::value(&text));
        assert_eq!(
            again["values"][VALUE_COLUMN]["base64"],
            Json::String(expected),
            "{again}"
        );
    }
    Ok(())
}

#[test]
#[ignore = "needs ICETRAY_GOLDEN_WAREHOUSE; run dev/test-golden.sh"]
fn test_get_str_2m_key_field_passed_back_looks_up_the_same_key() -> Result<()> {
    assert_key_field_round_trips(GoldenTable::Str2m, golden::string_key)
}

#[test]
#[ignore = "needs ICETRAY_GOLDEN_WAREHOUSE; run dev/test-golden.sh"]
fn test_get_long_2m_key_field_passed_back_looks_up_the_same_key() -> Result<()> {
    assert_key_field_round_trips(GoldenTable::Long2m, golden::long_key)
}
