//! Every TOML example in the README and the documents under `docs/`
//! parses as a configuration, so an example cannot drift from the schema
//! it shows. A whole file (one holding `[[tables]]`) is parsed as written;
//! a fragment is completed with a minimal table around it first. The design
//! records under `docs/rfd/` and `docs/analysis/` are left out: they record
//! what was decided or measured at the time, in the shape of the time.

use std::fs;
use std::path::{Path, PathBuf};

use icetray_core::Config;

/// The fence that opens a TOML example in Markdown.
const TOML_FENCE: &str = "```toml";

/// The fence that closes any example.
const FENCE: &str = "```";

/// A table entry's keys up to its source: what a `[tables.source]`
/// fragment is completed with in front.
const TABLE_HEAD: &str = r#"[[tables]]
name = "example"
ref = "main"
key_column = "key"
value_columns = ["value"]
"#;

/// A whole table entry: what a fragment of another section is completed
/// with behind, since a configuration lists at least one table.
const TABLE: &str = r#"
[[tables]]
name = "example"
ref = "main"
key_column = "key"
value_columns = ["value"]

[tables.source]
type = "static"
location = "/data/example"
"#;

/// The repository root, two levels above this crate.
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// The README and every Markdown document directly under `docs/`.
fn documents() -> Vec<PathBuf> {
    let root = repo_root();
    let mut found = vec![root.join("README.md")];
    let mut docs: Vec<PathBuf> = fs::read_dir(root.join("docs"))
        .expect("docs/ is readable")
        .map(|entry| entry.expect("a docs/ entry is readable").path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "md"))
        .collect();
    docs.sort();
    found.extend(docs);
    found
}

/// Each TOML example in `text`, with the line its fence opens on.
fn toml_examples(text: &str) -> Vec<(usize, String)> {
    let mut examples = Vec::new();
    let mut open: Option<(usize, String)> = None;
    for (index, line) in text.lines().enumerate() {
        match &mut open {
            None if line.trim_start() == TOML_FENCE => open = Some((index + 1, String::new())),
            None => {}
            Some(_) if line.trim_start() == FENCE => {
                examples.extend(open.take());
            }
            Some((_, body)) => {
                body.push_str(line);
                body.push('\n');
            }
        }
    }
    examples
}

/// `example` as a whole configuration: as written when it lists a table,
/// otherwise completed with a minimal one.
fn completed(example: &str) -> String {
    if example.contains("[[tables]]") {
        example.to_string()
    } else if example.trim_start().starts_with("[tables.source]") {
        format!("{TABLE_HEAD}\n{example}")
    } else {
        format!("{example}{TABLE}")
    }
}

#[test]
fn test_every_toml_example_in_the_docs_parses() {
    let mut checked = 0;
    let mut failures = Vec::new();
    for path in documents() {
        let text = fs::read_to_string(&path).expect("a document is readable");
        for (line, example) in toml_examples(&text) {
            checked += 1;
            if let Err(error) = Config::parse(&completed(&example)) {
                failures.push(format!("{}:{line}: {error}", path.display()));
            }
        }
    }
    assert!(checked > 0, "the documents hold TOML examples");
    assert!(
        failures.is_empty(),
        "examples that do not parse:\n{}",
        failures.join("\n")
    );
}

#[test]
fn test_toml_examples_finds_each_fenced_block_with_its_line() {
    let text = "prose\n```toml\na = 1\n```\n```sh\nls\n```\n```toml\nb = 2\n```\n";
    assert_eq!(
        toml_examples(text),
        [(2, "a = 1\n".to_string()), (8, "b = 2\n".to_string())]
    );
}

#[test]
fn test_completed_fragments_parse_and_a_stale_key_fails() {
    for fragment in [
        "[server]\nlisten = \"0.0.0.0:7433\"\n",
        "[tables.source]\ntype = \"static\"\nlocation = \"/data/kv\"\n",
    ] {
        assert!(Config::parse(&completed(fragment)).is_ok(), "{fragment}");
    }
    let stale = "[[tables]]\nname = \"kv\"\nref = \"main\"\nkey_column = \"key\"\nvalue_column = \"value\"\n\n[tables.source]\ntype = \"static\"\nlocation = \"/data/kv\"\n";
    assert!(
        Config::parse(&completed(stale)).is_err(),
        "a key the schema no longer knows fails"
    );
}
