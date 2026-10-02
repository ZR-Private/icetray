//! The built binary end to end (RFD 016's exit codes, the JSON lines of
//! blueprint section 5, the twelve-factor start of RFD 012): fixtures on
//! local disk, the binary run as a process, its stdout parsed line by line.

use std::io::{BufRead as _, BufReader};
use std::net::SocketAddr;
use std::path::Path;
use std::process::{Child, ChildStdout, Command, ExitStatus, Output, Stdio};
use std::time::{Duration, Instant};

use base64::Engine as _;
use hyper::StatusCode;
use icetray::cli::{ENV_CONFIG, LOCATION_TABLE_NAME};
use icetray::listen::{METRICS_PATH, NOT_READY_BODY, READY_BODY, READY_PATH};
use icetray::reload::ReloadResult;
use icetray::serve::OVERALL_SERVICE;
use icetray::{EXIT_FAILURE, EXIT_INEFFICIENT, EXIT_OK, EXIT_USAGE};
use icetray_core::approx;
use icetray_core::contract::VALUE_PAGE_BYTES;
use icetray_core::metric::{
    ALLOCATOR_ALLOCATED_BYTES, ALLOCATOR_RESIDENT_BYTES, CONFIG_PENDING, CONFIG_RELOADS_TOTAL,
    LABEL_RESULT, LABEL_STATE, LABEL_TABLE, PROCESS_RSS_BYTES, TABLE_STATE,
};
use icetray_core::{Key, KeyType, SnapshotId, TableState, bucket};
use icetray_testkit::fixture::{
    IDS_2K, IDS_4K, NAMESPACE, SAMPLE, SMALL_BUCKETS, SMALL_IDS, STRAY_IDS, TWO_BUCKETS,
    UNPARTITIONED_FILE_BYTES,
};
use icetray_testkit::layout::Layout;
use icetray_testkit::verify::series_value;
use icetray_testkit::{Fixture, FixtureSpec, Nulls, Order, memory_catalog_local, write_fixture};
use serde_json::Value as Json;
use tonic::transport::Channel;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;

mod common;

use common::{
    BoxError, NULL_EVERY, NULL_ID, PRESENT_ID, SHUTDOWN_WAIT, health_status, http_get, poll_until,
};

/// What every test returns.
type TestResult = Result<(), BoxError>;

/// The built binary under test.
const BIN: &str = env!("CARGO_BIN_EXE_icetray");

/// Present keys the broken-pipe test asks for: at a few hundred bytes a
/// line, well over the 64 KiB a pipe buffers, so the binary blocks on a
/// write until the reader closes and then meets the broken pipe.
const MANY_KEYS: u64 = 1_000;

/// How long `serve` may take to become ready.
const READY_WAIT: Duration = Duration::from_secs(10);

/// The names of the two tables in the two-table configuration.
const TABLE_A: &str = "a";
const TABLE_B: &str = "b";

/// The renamed log fields and the static field the serve test configures.
const TIME_FIELD: &str = "ts";
const LEVEL_FIELD: &str = "lvl";
const MESSAGE_FIELD: &str = "msg";
const SERVICE_FIELD: &str = "service";
const SERVICE_VALUE: &str = "icetray";

/// Runs the binary with `args` and `env`, with the configuration variable
/// cleared first: a run with no target must be a usage error whatever the
/// developer's shell exports, and a test that wants the variable passes it
/// in `env`.
fn run(args: &[&str], env: &[(&str, &str)]) -> Output {
    Command::new(BIN)
        .args(args)
        .env_remove(ENV_CONFIG)
        .envs(env.iter().copied())
        .output()
        .expect("the binary runs")
}

/// The exit code of a finished run.
fn code(output: &Output) -> i32 {
    output.status.code().expect("the binary exited")
}

/// Asserts the run exited with `expected`, showing its stderr when it did not.
fn assert_exit(output: &Output, expected: u8) {
    assert_eq!(code(output), i32::from(expected), "{}", stderr(output));
}

/// `stdout` parsed line by line.
fn lines(stdout: &[u8]) -> Vec<Json> {
    String::from_utf8_lossy(stdout)
        .lines()
        .map(|line| serde_json::from_str(line).unwrap_or_else(|error| panic!("{line}: {error}")))
        .collect()
}

/// The stderr of a run, for a failing assertion's message.
fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// Writes `spec` as a fixture under `dir` through a memory catalog.
async fn local_fixture(dir: &Path, spec: FixtureSpec) -> Result<Fixture, BoxError> {
    let catalog = memory_catalog_local(dir).await?;
    Ok(write_fixture(catalog.as_ref(), NAMESPACE, spec).await?)
}

/// `--location <the fixture's metadata file>`.
fn location(fixture: &Fixture) -> [&str; 2] {
    ["--location", fixture.metadata_location()]
}

/// A table of the two-table configuration, in TOML, pinned to `fixture`.
fn toml_table(name: &str, fixture: &Fixture) -> String {
    format!(
        "[[tables]]\nname = \"{name}\"\nsource = {{ type = \"static\", location = \"{}\" }}\nref = \"main\"\nkey_column = \"key\"\nvalue_column = \"value\"\nstrict = false\n",
        fixture.metadata_location()
    )
}

/// A table of a JSON configuration, pinned to `fixture`.
fn json_table(name: &str, fixture: &Fixture) -> Json {
    serde_json::json!({
        "name": name,
        "source": {"type": "static", "location": fixture.metadata_location()},
        "ref": "main",
        "key_column": "key",
        "value_column": "value",
        "strict": false
    })
}

/// A one-table configuration pinned to `fixture`, as JSON, with the
/// listeners on loopback at OS-assigned ports and the renamed log fields.
fn json_config(name: &str, fixture: &Fixture) -> String {
    serde_json::json!({
        "server": {
            "listen": "127.0.0.1:0",
            "metrics_listen": "127.0.0.1:0",
            "log": {
                "fields": {"time": TIME_FIELD, "level": LEVEL_FIELD, "message": MESSAGE_FIELD},
                "static_fields": {SERVICE_FIELD: SERVICE_VALUE}
            }
        },
        "tables": [json_table(name, fixture)]
    })
    .to_string()
}

/// A spawned `icetray serve`, killed when the test ends however it ends, so
/// a failed assertion leaves no daemon serving on loopback.
struct Daemon(Child);

impl Drop for Daemon {
    fn drop(&mut self) {
        // intentional: the child may already have exited, and a kill or a
        // wait that fails then has nothing left to clean up.
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Writes `text` to `name` under `dir` and returns its path as text.
fn write_file(dir: &Path, name: &str, text: &str) -> Result<String, BoxError> {
    let path = dir.join(name);
    std::fs::write(&path, text)?;
    Ok(path
        .to_str()
        .expect("a temporary path is UTF-8")
        .to_string())
}

/// Reads a daemon's log lines up to its `listening` line, checking each,
/// and returns the gRPC and metrics addresses it names.
fn until_listening(
    reader: &mut BufReader<ChildStdout>,
) -> Result<(SocketAddr, SocketAddr), BoxError> {
    loop {
        let mut text = String::new();
        assert!(
            reader.read_line(&mut text)? > 0,
            "the daemon exited before listening"
        );
        let line: Json = serde_json::from_str(&text)?;
        check_log_line(&line);
        if line[MESSAGE_FIELD] == "listening" {
            let address = |field: &str| line[field].as_str().map(str::parse::<SocketAddr>);
            let grpc = address("grpc").expect("the grpc address")?;
            let metrics = address("metrics").expect("the metrics address")?;
            return Ok((grpc, metrics));
        }
    }
}

/// Waits for `child` to exit, up to `wait`.
async fn wait_within(child: &mut Child, wait: Duration) -> Result<ExitStatus, BoxError> {
    poll_until(wait, "the child exiting", async || Ok(child.try_wait()?)).await
}

#[tokio::test]
async fn test_info_serializes_the_summary() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(dir.path(), FixtureSpec::small("kv_info", KeyType::Long)).await?;
    let output = run(&[&["info"][..], &location(&fixture)].concat(), &[]);
    assert_exit(&output, EXIT_OK);
    let raw = String::from_utf8(output.stdout.clone())?;
    assert!(
        raw.starts_with(&format!(
            "{{\"kind\":\"table\",\"table\":\"{LOCATION_TABLE_NAME}\","
        )),
        "{raw}"
    );
    let info = lines(&output.stdout);
    assert_eq!(info.len(), 1);
    assert_eq!(info[0]["kind"], "table");
    assert_eq!(info[0]["table"], LOCATION_TABLE_NAME);
    assert_eq!(info[0]["file_count"], Json::from(SMALL_BUCKETS));
    assert_eq!(
        info[0]["records"],
        Json::from(SMALL_IDS.end - SMALL_IDS.start)
    );
    assert_eq!(info[0]["serviceability"], "efficient");
    assert_eq!(info[0]["residency"], "full");
    Ok(())
}

#[tokio::test]
async fn test_doctor_reports_the_contract_fixture_as_efficient() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(dir.path(), FixtureSpec::small("kv_doctor", KeyType::Long)).await?;
    let output = run(
        &[&["doctor"][..], &location(&fixture), &["--files"]].concat(),
        &[],
    );
    assert_exit(&output, EXIT_OK);
    let report = lines(&output.stdout);
    assert_eq!(report[0]["kind"], "doctor");
    assert_eq!(report[0]["table"], LOCATION_TABLE_NAME);
    assert_eq!(report[0]["serviceability"], "efficient");
    assert_eq!(report[0]["costs"], Json::from(0));
    assert_eq!(report[0]["advice"], Json::from(0));
    assert!(report.iter().all(|line| line["kind"] != "finding"));
    assert_eq!(
        report.iter().filter(|line| line["kind"] == "file").count(),
        approx::length(SMALL_BUCKETS)
    );
    Ok(())
}

#[tokio::test]
async fn test_doctor_prices_an_unpartitioned_table_and_exits_2() -> TestResult {
    let dir = tempfile::tempdir()?;
    let spec = FixtureSpec {
        buckets: 0,
        layout: Layout {
            target_file_bytes: UNPARTITIONED_FILE_BYTES,
            ..Layout::contract()
        },
        ..FixtureSpec::small("kv_flat", KeyType::Long)
    };
    let fixture = local_fixture(dir.path(), spec).await?;
    let output = run(&[&["doctor"][..], &location(&fixture)].concat(), &[]);
    assert_exit(&output, EXIT_INEFFICIENT);
    let report = lines(&output.stdout);
    assert_eq!(report[0]["kind"], "doctor");
    assert_eq!(report[0]["serviceability"], "inefficient");
    assert!(
        report.iter().any(|line| line["kind"] == "finding"
            && line["code"] == "no_bucket_partition"
            && line["tier"] == "cost"),
        "{report:?}"
    );
    Ok(())
}

#[tokio::test]
async fn test_doctor_reports_a_refusal_and_exits_1() -> TestResult {
    let dir = tempfile::tempdir()?;
    let spec = FixtureSpec {
        ids: IDS_4K,
        order: Order::Descending,
        ..FixtureSpec::small("kv_unsorted", KeyType::Long)
    };
    let fixture = local_fixture(dir.path(), spec).await?;
    let output = run(&[&["doctor"][..], &location(&fixture)].concat(), &[]);
    assert_exit(&output, EXIT_FAILURE);
    let report = lines(&output.stdout);
    assert_eq!(report.len(), 1);
    assert_eq!(report[0]["kind"], "refused");
    assert_eq!(report[0]["table"], LOCATION_TABLE_NAME);
    assert_eq!(report[0]["reason"], "unsorted");
    assert!(report[0]["message"].is_string());
    Ok(())
}

/// A raw file of bucket-1 keys with one bucket-0 key between its first
/// and last, recorded under bucket 1, beside a fixture of two buckets: the
/// stray key is no fence, so the load serves the table and `get` answers
/// that key absent, and the doctor's full bucket check refuses the table.
#[tokio::test]
async fn test_doctor_refuses_a_key_outside_its_bucket_that_the_load_serves() -> TestResult {
    let dir = tempfile::tempdir()?;
    let catalog = memory_catalog_local(dir.path()).await?;
    let spec = FixtureSpec {
        ids: IDS_2K,
        buckets: TWO_BUCKETS,
        ..FixtureSpec::small("kv_stray", KeyType::Long)
    };
    let mut fixture = write_fixture(catalog.as_ref(), NAMESPACE, spec).await?;
    let (recorded, hashed) = (1, 0);
    let buckets: Vec<(u64, u32)> = STRAY_IDS
        .map(|id| (id, bucket(&fixture.key(id), TWO_BUCKETS)))
        .collect();
    let mut ids: Vec<u64> = buckets
        .iter()
        .filter(|(_, at)| *at == recorded)
        .map(|(id, _)| *id)
        .collect();
    let (first, last) = (ids[0], ids[ids.len() - 1]);
    let stray = buckets
        .iter()
        .find(|(id, at)| *at == hashed && first < *id && *id < last)
        .map(|(id, _)| *id)
        .expect("a bucket-0 id between the first and last bucket-1 id");
    ids.push(stray);
    ids.sort_unstable();
    let raw = fixture.raw_file_in_bucket("stray.parquet", &ids, i32::try_from(recorded)?)?;
    fixture.append_raw(catalog.as_ref(), raw).await?;
    let Key::Long(stray_key) = fixture.key(stray) else {
        panic!("a long-keyed fixture");
    };

    let get = run(
        &[&["get"][..], &location(&fixture), &[&stray_key.to_string()]].concat(),
        &[],
    );
    assert_exit(&get, EXIT_OK);
    assert_eq!(lines(&get.stdout)[0]["presence"], "absent");

    let output = run(&[&["doctor"][..], &location(&fixture)].concat(), &[]);
    assert_exit(&output, EXIT_FAILURE);
    let report = lines(&output.stdout);
    assert_eq!(report.len(), 1);
    assert_eq!(report[0]["kind"], "refused");
    assert_eq!(report[0]["table"], LOCATION_TABLE_NAME);
    assert_eq!(report[0]["reason"], "key_outside_bucket");
    Ok(())
}

#[tokio::test]
async fn test_get_answers_present_null_and_absent() -> TestResult {
    let dir = tempfile::tempdir()?;
    let spec = FixtureSpec {
        nulls: Nulls::EveryNth(NULL_EVERY),
        ..FixtureSpec::small("kv_nulls", KeyType::Long)
    };
    let fixture = local_fixture(dir.path(), spec).await?;
    let present = fixture.key(PRESENT_ID);
    let null = fixture.key(NULL_ID);
    let absent = fixture.absent_near(PRESENT_ID);
    let value = fixture.value(&present).expect("a present value");
    assert_eq!(fixture.value(&null), None);
    let keys = [present.to_string(), null.to_string(), absent.to_string()];
    let output = run(
        &[
            &["get"][..],
            &location(&fixture),
            &keys.iter().map(String::as_str).collect::<Vec<_>>(),
        ]
        .concat(),
        &[],
    );
    assert_exit(&output, EXIT_OK);
    let got = lines(&output.stdout);
    assert_eq!(got.len(), keys.len());
    for (line, key) in got.iter().zip(&keys) {
        assert_eq!(line["kind"], "get");
        assert_eq!(line["table"], LOCATION_TABLE_NAME);
        assert_eq!(line["key"], Json::String(key.clone()));
        assert_eq!(line["snapshot_id"], got[0]["snapshot_id"]);
    }
    assert_eq!(got[0]["presence"], "present");
    assert_eq!(
        got[0]["value_base64"],
        Json::String(base64::engine::general_purpose::STANDARD.encode(&value))
    );
    assert_eq!(got[0]["value_bytes"], Json::from(value.len()));
    assert_eq!(got[0]["gets"], Json::from(1));
    assert_eq!(got[1]["presence"], "null");
    assert!(got[1].get("value_base64").is_none());
    assert_eq!(got[1]["value_bytes"], Json::from(0));
    assert_eq!(got[1]["gets"], Json::from(1));
    assert_eq!(got[2]["presence"], "absent");
    assert!(got[2].get("value_base64").is_none());
    assert_eq!(got[2]["value_bytes"], Json::from(0));
    assert_eq!(got[2]["gets"], Json::from(0));
    Ok(())
}

#[tokio::test]
async fn test_get_parses_every_key_type_as_display_prints_it() -> TestResult {
    let dir = tempfile::tempdir()?;
    let cases = [
        ("kv_str", KeyType::String),
        ("kv_bin", KeyType::Binary),
        ("kv_uuid", KeyType::Uuid),
    ];
    for (name, key_type) in cases {
        let fixture = local_fixture(dir.path(), FixtureSpec::small(name, key_type)).await?;
        let key = fixture.key(SAMPLE[1]);
        // A string key's Display is Debug-quoted; the command line takes the
        // text itself. Every other type is passed as Display prints it.
        let text = match &key {
            Key::Str(text) => text.clone(),
            Key::Long(_) | Key::Bytes(_) | Key::Uuid(_) => key.to_string(),
        };
        let output = run(&[&["get"][..], &location(&fixture), &[&text]].concat(), &[]);
        assert_exit(&output, EXIT_OK);
        let got = lines(&output.stdout);
        assert_eq!(got.len(), 1, "{name}");
        assert_eq!(got[0]["presence"], "present", "{name}");
        assert_eq!(got[0]["key"], Json::String(key.to_string()), "{name}");
        assert_eq!(got[0]["gets"], Json::from(1), "{name}");

        if let Key::Bytes(_) = &key {
            let uppercase = key.to_string().trim_start_matches("0x").to_uppercase();
            let output = run(
                &[&["get"][..], &location(&fixture), &[&uppercase]].concat(),
                &[],
            );
            assert_exit(&output, EXIT_OK);
            let got = lines(&output.stdout);
            assert_eq!(got[0]["presence"], "present");
            assert_eq!(got[0]["key"], Json::String(key.to_string()));
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_get_rejects_a_key_that_does_not_parse() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(dir.path(), FixtureSpec::small("kv_badkey", KeyType::Long)).await?;
    let output = run(&[&["get"][..], &location(&fixture), &["abc"]].concat(), &[]);
    assert_exit(&output, EXIT_USAGE);
    assert!(output.stdout.is_empty());
    Ok(())
}

#[tokio::test]
async fn test_get_needs_a_table_name_when_several_are_configured() -> TestResult {
    let dir = tempfile::tempdir()?;
    let catalog = memory_catalog_local(dir.path()).await?;
    let a = write_fixture(
        catalog.as_ref(),
        NAMESPACE,
        FixtureSpec::small("kv_a", KeyType::Long),
    )
    .await?;
    let b = write_fixture(
        catalog.as_ref(),
        NAMESPACE,
        FixtureSpec::small("kv_b", KeyType::Long),
    )
    .await?;
    let toml = format!("{}\n{}", toml_table(TABLE_A, &a), toml_table(TABLE_B, &b));
    let config = write_file(dir.path(), "two.toml", &toml)?;
    let key = a.key(SAMPLE[0]).to_string();

    let unnamed = run(&["get", "--config", &config, &key], &[]);
    assert_exit(&unnamed, EXIT_USAGE);
    assert!(unnamed.stdout.is_empty());

    let named = run(&["get", "--config", &config, "--table", TABLE_A, &key], &[]);
    assert_exit(&named, EXIT_OK);
    let got = lines(&named.stdout);
    assert_eq!(got.len(), 1);
    assert_eq!(got[0]["table"], TABLE_A);
    assert_eq!(got[0]["presence"], "present");
    Ok(())
}

#[tokio::test]
async fn test_config_toml_and_json_open_the_same_table() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(dir.path(), FixtureSpec::small("kv_both", KeyType::Long)).await?;
    let toml = write_file(dir.path(), "one.toml", &toml_table(TABLE_A, &fixture))?;
    let json = serde_json::json!({ "tables": [json_table(TABLE_A, &fixture)] }).to_string();
    let json = write_file(dir.path(), "one.json", &json)?;
    let from_toml = run(&["info", "--config", &toml], &[]);
    let from_json = run(&["info", "--config", &json], &[]);
    assert_exit(&from_toml, EXIT_OK);
    assert_exit(&from_json, EXIT_OK);
    assert_eq!(from_toml.stdout, from_json.stdout);
    let info = lines(&from_toml.stdout);
    assert_eq!(info.len(), 1);
    assert_eq!(info[0]["table"], TABLE_A);
    Ok(())
}

#[tokio::test]
async fn test_location_only_flags_are_rejected_under_config() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(dir.path(), FixtureSpec::small("kv_flags", KeyType::Long)).await?;
    let config = write_file(dir.path(), "one.toml", &toml_table(TABLE_A, &fixture))?;

    let flagged = run(&["info", "--config", &config, "--key-column", "key"], &[]);
    assert_exit(&flagged, EXIT_USAGE);
    assert!(flagged.stdout.is_empty());

    let targetless = run(&["info"], &[]);
    assert_exit(&targetless, EXIT_USAGE);
    assert!(targetless.stdout.is_empty());

    let unknown = run(
        &[&["info"][..], &location(&fixture), &["--table", "nobody"]].concat(),
        &[],
    );
    assert_exit(&unknown, EXIT_USAGE);
    assert!(unknown.stdout.is_empty());
    Ok(())
}

/// A `--location` run reads no configuration file, so the variable that
/// names one is ignored; a run with no target flag at all reads it.
#[tokio::test]
async fn test_location_ignores_the_environment_and_a_bare_run_reads_it() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(dir.path(), FixtureSpec::small("kv_env", KeyType::Long)).await?;
    let missing = dir.path().join("missing.toml");
    let missing = missing.to_str().expect("a utf-8 temporary path");
    let located = run(
        &[&["info"][..], &location(&fixture)].concat(),
        &[(ENV_CONFIG, missing)],
    );
    assert_exit(&located, EXIT_OK);
    assert_eq!(lines(&located.stdout)[0]["table"], LOCATION_TABLE_NAME);

    let config = write_file(dir.path(), "env.toml", &toml_table(TABLE_A, &fixture))?;
    let bare = run(&["info"], &[(ENV_CONFIG, &config)]);
    assert_exit(&bare, EXIT_OK);
    assert_eq!(lines(&bare.stdout)[0]["table"], TABLE_A);
    Ok(())
}

/// A `--location` table runs full or bloom; `disk` is not among the
/// residencies it offers, so clap refuses it as a usage error before
/// anything is opened. A file's disk table with no local directory is the
/// configuration violation, exit 1.
#[tokio::test]
async fn test_location_offers_full_and_bloom_and_refuses_disk_as_usage() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(dir.path(), FixtureSpec::small("kv_disk", KeyType::Long)).await?;
    let located = |residency: &str| {
        run(
            &[
                &["info"][..],
                &location(&fixture),
                &["--residency", residency],
            ]
            .concat(),
            &[],
        )
    };
    let disk = located("disk");
    assert_exit(&disk, EXIT_USAGE);
    assert!(disk.stdout.is_empty(), "{}", stderr(&disk));
    for residency in ["full", "bloom"] {
        let output = located(residency);
        assert_exit(&output, EXIT_OK);
        assert_eq!(lines(&output.stdout)[0]["residency"], residency);
    }

    let disk_table = format!("{}residency = \"disk\"\n", toml_table(TABLE_A, &fixture));
    let config = write_file(dir.path(), "disk.toml", &disk_table)?;
    let from_file = run(&["doctor", "--config", &config], &[]);
    assert_exit(&from_file, EXIT_FAILURE);
    assert!(from_file.stdout.is_empty(), "{}", stderr(&from_file));
    Ok(())
}

/// A log field mapping the configuration refuses, a key that names no
/// standard field or two standard fields under one name, is a configuration
/// mistake like any other: refused before anything is opened, exit 1.
#[tokio::test]
async fn test_a_bad_log_field_mapping_exits_1() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(dir.path(), FixtureSpec::small("kv_log", KeyType::Long)).await?;
    let mappings = [
        ("unknown.toml", "timestamp = \"ts\"\n"),
        ("collision.toml", "time = \"x\"\nlevel = \"x\"\n"),
    ];
    for (name, fields) in mappings {
        let text = format!(
            "[server.log.fields]\n{fields}\n{}",
            toml_table(TABLE_A, &fixture)
        );
        let config = write_file(dir.path(), name, &text)?;
        let output = run(&["info", "--config", &config], &[]);
        assert_exit(&output, EXIT_FAILURE);
        assert!(output.stdout.is_empty(), "{name}: {}", stderr(&output));
    }
    Ok(())
}

/// A `--rewrite` rule for a prefix the file also maps is tried first, so it
/// wins: the file's rule alone sends every read to a directory that does not
/// exist, and the command line's rule for the same prefix keeps the reads
/// where the table is.
#[tokio::test]
async fn test_a_command_line_rewrite_takes_precedence_over_the_file_s() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture =
        local_fixture(dir.path(), FixtureSpec::small("kv_rewrite", KeyType::Long)).await?;
    let warehouse = format!("file://{}/", dir.path().display());
    let moved = format!("file://{}/", dir.path().join("moved").display());
    let text = format!(
        "[[storage.rewrite]]\nfrom = \"{warehouse}\"\nto = \"{moved}\"\n\n{}",
        toml_table(TABLE_A, &fixture)
    );
    let config = write_file(dir.path(), "rewrite.toml", &text)?;

    let file_rule_only = run(&["info", "--config", &config], &[]);
    assert_exit(&file_rule_only, EXIT_FAILURE);

    let identity = format!("{warehouse}={warehouse}");
    let overridden = run(&["info", "--config", &config, "--rewrite", &identity], &[]);
    assert_exit(&overridden, EXIT_OK);
    assert_eq!(lines(&overridden.stdout)[0]["table"], TABLE_A);
    Ok(())
}

/// An empty `--rewrite` prefix is the same violation as an empty `from` in
/// the file, refused before anything is opened with exit 1.
#[tokio::test]
async fn test_an_empty_rewrite_prefix_exits_1() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(dir.path(), FixtureSpec::small("kv_empty", KeyType::Long)).await?;
    let output = run(
        &[
            &["info"][..],
            &location(&fixture),
            &["--rewrite", "=file:///elsewhere/"],
        ]
        .concat(),
        &[],
    );
    assert_exit(&output, EXIT_FAILURE);
    assert!(output.stdout.is_empty(), "{}", stderr(&output));
    Ok(())
}

#[tokio::test]
async fn test_broken_pipe_ends_the_run_with_exit_0() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(dir.path(), FixtureSpec::small("kv_pipe", KeyType::Long)).await?;
    let keys: Vec<String> = (0..MANY_KEYS)
        .map(|id| fixture.key(id).to_string())
        .collect();
    let mut child = Command::new(BIN)
        .arg("get")
        .args(location(&fixture))
        .args(&keys)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let stdout = child.stdout.take().expect("stdout is piped");
    let mut reader = BufReader::new(stdout);
    let mut first = String::new();
    reader.read_line(&mut first)?;
    let line: Json = serde_json::from_str(&first)?;
    assert_eq!(line["kind"], "get");
    drop(reader);
    let output = child.wait_with_output()?;
    assert_exit(&output, EXIT_OK);
    Ok(())
}

/// One line of the daemon's log, checked for the configured field names.
fn check_log_line(line: &Json) {
    let object = line.as_object().expect("a JSON object per line");
    for field in [TIME_FIELD, LEVEL_FIELD, MESSAGE_FIELD] {
        assert!(object.contains_key(field), "{line}");
    }
    assert_eq!(line[SERVICE_FIELD], SERVICE_VALUE, "{line}");
    for default in ["time", "level", "message"] {
        assert!(!object.contains_key(default), "{line}");
    }
}

/// A daemon that cannot bind its gRPC address fails its start, and the
/// failure reaches exit 1 like any other: the address is held by this test
/// for the length of the run.
#[tokio::test(flavor = "multi_thread")]
async fn test_serve_exits_1_when_its_listener_is_taken() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(dir.path(), FixtureSpec::small("kv_bind", KeyType::Long)).await?;
    let taken = std::net::TcpListener::bind("127.0.0.1:0")?;
    let mut config: Json = serde_json::from_str(&json_config(TABLE_A, &fixture))?;
    config["server"]["listen"] = Json::String(taken.local_addr()?.to_string());
    let path = write_file(dir.path(), "serve.json", &config.to_string())?;
    let output = run(&["serve", "--config", &path], &[]);
    assert_exit(&output, EXIT_FAILURE);
    Ok(())
}

/// The variable tokio's multi-thread runtime reads its worker count from.
const TOKIO_WORKER_THREADS: &str = "TOKIO_WORKER_THREADS";

/// A page cache budget the file admits, which holds a value page for one
/// shard of the cache but not for the two a two-worker runtime gives it, is
/// refused as the daemon opens the cache, and the failure reaches exit 1.
#[tokio::test(flavor = "multi_thread")]
async fn test_serve_exits_1_when_the_page_cache_is_below_its_shards_minimum() -> TestResult {
    let workers = 2;
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(dir.path(), FixtureSpec::small("kv_cache", KeyType::Long)).await?;
    let mut config: Json = serde_json::from_str(&json_config(TABLE_A, &fixture))?;
    let one_shard_not_two = VALUE_PAGE_BYTES * workers;
    config["server"]["page_cache_bytes"] = Json::from(one_shard_not_two);
    let path = write_file(dir.path(), "serve.json", &config.to_string())?;
    // Spawned and waited on within a bound rather than run to its end, so
    // a daemon that wrongly starts serving fails the test instead of
    // holding it open.
    let mut daemon = Daemon(
        Command::new(BIN)
            .args(["serve", "--config", &path])
            .env_remove(ENV_CONFIG)
            .env(TOKIO_WORKER_THREADS, workers.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    );
    let status = wait_within(&mut daemon.0, READY_WAIT).await?;
    assert_eq!(status.code(), Some(i32::from(EXIT_FAILURE)));
    Ok(())
}

/// The `state` label value of a table that serves, as core renders the
/// state; the state's fields do not reach the label, so any snapshot and
/// instant give it.
fn serving_state() -> String {
    TableState::Serving {
        snapshot_id: SnapshotId(0),
        since: Instant::now(),
    }
    .to_string()
}

/// The conformance run of RFD 012, which the workspace test run carries:
/// the binary started as an orchestrator starts it (the configuration path
/// from the environment, a JSON file, no flags), its logs under the
/// configured field names, the health service answering while the table
/// loads, readiness turning on once it serves, the metrics scrape carrying
/// the table's state and the process's memory, and the drain on SIGTERM.
#[tokio::test(flavor = "multi_thread")]
async fn test_serve_conformance_from_the_environment() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(dir.path(), FixtureSpec::small("kv_serve", KeyType::Long)).await?;
    let config = write_file(dir.path(), "serve.json", &json_config(TABLE_A, &fixture))?;
    let mut daemon = Daemon(
        Command::new(BIN)
            .arg("serve")
            .env(ENV_CONFIG, &config)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?,
    );
    let stdout = daemon.0.stdout.take().expect("stdout is piped");
    let mut reader = BufReader::new(stdout);
    let (grpc, metrics) = until_listening(&mut reader)?;

    // The health service answers as soon as the listener is up, while the
    // table may still be loading: the answer is one of the two states,
    // never a transport failure.
    let channel = Channel::from_shared(format!("http://{grpc}"))?
        .connect()
        .await?;
    let mut health = HealthClient::new(channel);
    assert!(matches!(
        health_status(&mut health, OVERALL_SERVICE).await?,
        ServingStatus::NotServing | ServingStatus::Serving
    ));

    poll_until(READY_WAIT, "readiness", async || {
        let (status, body) = http_get(metrics, READY_PATH).await?;
        if status == StatusCode::OK {
            assert_eq!(body, READY_BODY);
            return Ok(Some(()));
        }
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body, NOT_READY_BODY);
        Ok(None)
    })
    .await?;
    assert_eq!(
        health_status(&mut health, OVERALL_SERVICE).await?,
        ServingStatus::Serving
    );

    let (status, exposition) = http_get(metrics, METRICS_PATH).await?;
    assert_eq!(status, StatusCode::OK);
    let rss = series_value(&exposition, PROCESS_RSS_BYTES, &[])
        .expect("the scrape carries the process's memory");
    assert!(rss > 0.0, "a running process holds memory: {rss}");
    let allocated = series_value(&exposition, ALLOCATOR_ALLOCATED_BYTES, &[])
        .expect("the scrape carries jemalloc's live bytes");
    let resident = series_value(&exposition, ALLOCATOR_RESIDENT_BYTES, &[])
        .expect("the scrape carries jemalloc's resident bytes");
    assert!(
        allocated > 0.0 && allocated <= resident,
        "the server allocates through jemalloc: {allocated} allocated, {resident} resident"
    );
    assert_eq!(
        series_value(
            &exposition,
            TABLE_STATE,
            &[(LABEL_TABLE, TABLE_A), (LABEL_STATE, &serving_state())]
        )
        .map(f64::to_bits),
        Some(1.0_f64.to_bits()),
        "the scrape carries the table's state"
    );

    let killed = Command::new("kill")
        .arg("-TERM")
        .arg(daemon.0.id().to_string())
        .status()?;
    assert!(killed.success());
    let status = wait_within(&mut daemon.0, SHUTDOWN_WAIT).await?;
    assert_eq!(status.code(), Some(i32::from(EXIT_OK)));
    let mut drained = String::new();
    std::io::Read::read_to_string(&mut reader, &mut drained)?;
    for line in &lines(drained.as_bytes()) {
        check_log_line(line);
    }
    Ok(())
}

/// A poll interval no test outlasts, so only a SIGHUP makes the daemon read
/// its file again.
const POLL_NEVER: &str = "24h";

/// `json_config` over `tables`, each pinned to its fixture, polling every
/// `POLL_NEVER`.
fn json_config_polling_never(tables: &[(&str, &Fixture)]) -> String {
    let mut config: Json =
        serde_json::from_str(&json_config(tables[0].0, tables[0].1)).expect("json_config is JSON");
    config["server"]["poll_interval"] = Json::from(POLL_NEVER);
    config["tables"] = tables
        .iter()
        .map(|(name, fixture)| json_table(name, fixture))
        .collect();
    config.to_string()
}

/// SIGHUP makes the daemon read its file at once, with no poll due, and
/// apply the table it adds; the process lives through the signal, whose
/// default action would end it, and drains on SIGTERM as before.
#[tokio::test(flavor = "multi_thread")]
async fn test_serve_reloads_its_file_on_sighup() -> TestResult {
    let dir = tempfile::tempdir()?;
    let first = local_fixture(dir.path(), FixtureSpec::small("kv_first", KeyType::Long)).await?;
    let second = local_fixture(dir.path(), FixtureSpec::small("kv_second", KeyType::Long)).await?;
    let config = write_file(
        dir.path(),
        "serve.json",
        &json_config_polling_never(&[(TABLE_A, &first)]),
    )?;
    let mut daemon = Daemon(
        Command::new(BIN)
            .args(["serve", "--config", &config])
            .env_remove(ENV_CONFIG)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?,
    );
    let stdout = daemon.0.stdout.take().expect("stdout is piped");
    let mut reader = BufReader::new(stdout);
    let (_, metrics) = until_listening(&mut reader)?;
    poll_until(READY_WAIT, "readiness", async || {
        let (status, _) = http_get(metrics, READY_PATH).await?;
        Ok((status == StatusCode::OK).then_some(()))
    })
    .await?;

    std::fs::write(
        &config,
        json_config_polling_never(&[(TABLE_A, &first), (TABLE_B, &second)]),
    )?;
    let hung_up = Command::new("kill")
        .arg("-HUP")
        .arg(daemon.0.id().to_string())
        .status()?;
    assert!(hung_up.success());
    let applied = ReloadResult::Applied.to_string();
    let exposition = poll_until(READY_WAIT, "the reload applied", async || {
        let (_, exposition) = http_get(metrics, METRICS_PATH).await?;
        let reloads = series_value(
            &exposition,
            CONFIG_RELOADS_TOTAL,
            &[(LABEL_RESULT, &applied)],
        );
        Ok(reloads.is_some().then_some(exposition))
    })
    .await?;
    assert_eq!(
        series_value(
            &exposition,
            CONFIG_RELOADS_TOTAL,
            &[(LABEL_RESULT, &applied)]
        )
        .map(f64::to_bits),
        Some(1.0_f64.to_bits())
    );
    assert_eq!(
        series_value(
            &exposition,
            TABLE_STATE,
            &[(LABEL_TABLE, TABLE_B), (LABEL_STATE, &serving_state())]
        )
        .map(f64::to_bits),
        Some(1.0_f64.to_bits()),
        "the added table serves"
    );
    assert_eq!(
        series_value(&exposition, CONFIG_PENDING, &[]).map(f64::to_bits),
        Some(0.0_f64.to_bits())
    );
    assert!(
        daemon.0.try_wait()?.is_none(),
        "the daemon lives through SIGHUP"
    );

    let killed = Command::new("kill")
        .arg("-TERM")
        .arg(daemon.0.id().to_string())
        .status()?;
    assert!(killed.success());
    let status = wait_within(&mut daemon.0, SHUTDOWN_WAIT).await?;
    assert_eq!(status.code(), Some(i32::from(EXIT_OK)));
    Ok(())
}
