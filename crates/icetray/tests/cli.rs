//! The built binary end to end (RFD 016's exit codes, the JSON lines of
//! blueprint section 5, the twelve-factor start of RFD 012): fixtures on
//! local disk, the binary run as a process, its stdout parsed line by line.

use std::io::{BufRead as _, BufReader};
use std::net::SocketAddr;
use std::ops::Range;
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Output, Stdio};
use std::time::{Duration, Instant};

use base64::Engine as _;
use hyper::StatusCode;
use icetray::cli::{ENV_CONFIG, LOCATION_TABLE_NAME};
use icetray::listen::READY_PATH;
use icetray::serve::OVERALL_SERVICE;
use icetray::{EXIT_FAILURE, EXIT_INEFFICIENT, EXIT_OK, EXIT_USAGE};
use icetray_core::{Key, KeyType};
use icetray_testkit::fixture::{SAMPLE, SMALL_BUCKETS, SMALL_IDS};
use icetray_testkit::layout::Layout;
use icetray_testkit::{Fixture, FixtureSpec, Nulls, Order, memory_catalog_local, write_fixture};
use serde_json::Value as Json;
use tonic::transport::Channel;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;

mod common;

use common::{
    BoxError, NAMESPACE, NULL_EVERY, NULL_ID, POLL_STEP, PRESENT_ID, SHUTDOWN_WAIT, http_get,
};

/// What every test returns.
type TestResult = Result<(), BoxError>;

/// The built binary under test.
const BIN: &str = env!("CARGO_BIN_EXE_icetray");

/// Ids of the fixtures that need no more than a page per file.
const IDS_4K: Range<u64> = 0..4_000;

/// A file size target below one write batch of the unpartitioned fixture,
/// so every batch is its own file and the table is priced as files probed.
const UNPARTITIONED_FILE_BYTES: usize = 64 * 1024;

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
/// cleared first so a test's environment never leaks into another's.
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

/// Waits for `child` to exit, up to `wait`.
fn wait_within(child: &mut Child, wait: Duration) -> Result<ExitStatus, BoxError> {
    let deadline = Instant::now() + wait;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        assert!(
            Instant::now() < deadline,
            "the child did not exit within {wait:?}"
        );
        std::thread::sleep(POLL_STEP);
    }
}

#[tokio::test]
async fn test_info_serializes_the_summary() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(dir.path(), FixtureSpec::small("kv_info", KeyType::Long)).await?;
    let output = run(&[&["info"][..], &location(&fixture)].concat(), &[]);
    assert_eq!(code(&output), i32::from(EXIT_OK), "{}", stderr(&output));
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
    assert_eq!(code(&output), i32::from(EXIT_OK), "{}", stderr(&output));
    let report = lines(&output.stdout);
    assert_eq!(report[0]["kind"], "doctor");
    assert_eq!(report[0]["table"], LOCATION_TABLE_NAME);
    assert_eq!(report[0]["serviceability"], "efficient");
    assert_eq!(report[0]["costs"], Json::from(0));
    assert_eq!(report[0]["advice"], Json::from(0));
    assert!(report.iter().all(|line| line["kind"] != "finding"));
    assert_eq!(
        report.iter().filter(|line| line["kind"] == "file").count(),
        usize::try_from(SMALL_BUCKETS)?
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
    assert_eq!(
        code(&output),
        i32::from(EXIT_INEFFICIENT),
        "{}",
        stderr(&output)
    );
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
    assert_eq!(
        code(&output),
        i32::from(EXIT_FAILURE),
        "{}",
        stderr(&output)
    );
    let report = lines(&output.stdout);
    assert_eq!(report.len(), 1);
    assert_eq!(report[0]["kind"], "refused");
    assert_eq!(report[0]["table"], LOCATION_TABLE_NAME);
    assert_eq!(report[0]["reason"], "unsorted");
    assert!(report[0]["message"].is_string());
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
    assert_eq!(code(&output), i32::from(EXIT_OK), "{}", stderr(&output));
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
        assert_eq!(
            code(&output),
            i32::from(EXIT_OK),
            "{name}: {}",
            stderr(&output)
        );
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
            assert_eq!(code(&output), i32::from(EXIT_OK), "{}", stderr(&output));
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
    assert_eq!(code(&output), i32::from(EXIT_USAGE), "{}", stderr(&output));
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
    assert_eq!(
        code(&unnamed),
        i32::from(EXIT_USAGE),
        "{}",
        stderr(&unnamed)
    );
    assert!(unnamed.stdout.is_empty());

    let named = run(&["get", "--config", &config, "--table", TABLE_A, &key], &[]);
    assert_eq!(code(&named), i32::from(EXIT_OK), "{}", stderr(&named));
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
    assert_eq!(
        code(&from_toml),
        i32::from(EXIT_OK),
        "{}",
        stderr(&from_toml)
    );
    assert_eq!(
        code(&from_json),
        i32::from(EXIT_OK),
        "{}",
        stderr(&from_json)
    );
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
    assert_eq!(
        code(&flagged),
        i32::from(EXIT_USAGE),
        "{}",
        stderr(&flagged)
    );
    assert!(flagged.stdout.is_empty());

    let targetless = run(&["info"], &[]);
    assert_eq!(
        code(&targetless),
        i32::from(EXIT_USAGE),
        "{}",
        stderr(&targetless)
    );
    assert!(targetless.stdout.is_empty());

    let unknown = run(
        &[&["info"][..], &location(&fixture), &["--table", "nobody"]].concat(),
        &[],
    );
    assert_eq!(
        code(&unknown),
        i32::from(EXIT_USAGE),
        "{}",
        stderr(&unknown)
    );
    assert!(unknown.stdout.is_empty());
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
        .env_remove(ENV_CONFIG)
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
    assert_eq!(code(&output), i32::from(EXIT_OK), "{}", stderr(&output));
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

#[tokio::test(flavor = "multi_thread")]
async fn test_serve_reads_its_config_from_the_environment_and_drains_on_sigterm() -> TestResult {
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
    let mut listening = None;
    while listening.is_none() {
        let mut text = String::new();
        assert!(
            reader.read_line(&mut text)? > 0,
            "the daemon exited before listening"
        );
        let line: Json = serde_json::from_str(&text)?;
        check_log_line(&line);
        if line[MESSAGE_FIELD] == "listening" {
            listening = Some(line);
        }
    }
    let listening = listening.expect("the listening line was read");
    let grpc: SocketAddr = listening["grpc"]
        .as_str()
        .expect("the grpc address")
        .parse()?;
    let metrics: SocketAddr = listening["metrics"]
        .as_str()
        .expect("the metrics address")
        .parse()?;

    let deadline = Instant::now() + READY_WAIT;
    loop {
        let (status, body) = http_get(metrics, READY_PATH).await?;
        if status == StatusCode::OK {
            assert_eq!(body, "ready");
            break;
        }
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(Instant::now() < deadline, "not ready within {READY_WAIT:?}");
        tokio::time::sleep(POLL_STEP).await;
    }
    let channel = Channel::from_shared(format!("http://{grpc}"))?
        .connect()
        .await?;
    let mut health = HealthClient::new(channel);
    let status = health
        .check(HealthCheckRequest {
            service: OVERALL_SERVICE.to_string(),
        })
        .await?
        .into_inner()
        .status;
    assert_eq!(ServingStatus::try_from(status)?, ServingStatus::Serving);

    let killed = Command::new("kill")
        .arg("-TERM")
        .arg(daemon.0.id().to_string())
        .status()?;
    assert!(killed.success());
    let status = wait_within(&mut daemon.0, SHUTDOWN_WAIT)?;
    assert_eq!(status.code(), Some(i32::from(EXIT_OK)));
    let mut drained = String::new();
    std::io::Read::read_to_string(&mut reader, &mut drained)?;
    for line in &lines(drained.as_bytes()) {
        check_log_line(line);
    }
    Ok(())
}
