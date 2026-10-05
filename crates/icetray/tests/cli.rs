//! The built binary end to end (RFD 016's exit codes, the JSON lines of
//! the output contract in `docs/configuration.md`, the twelve-factor start
//! of RFD 012): fixtures on
//! local disk, the binary run as a process, its stdout parsed line by line.

use std::io::{BufRead as _, BufReader};
use std::net::SocketAddr;
use std::path::Path;
use std::process::{Child, ChildStdout, Command, ExitStatus, Output, Stdio};
use std::time::{Duration, Instant};

use base64::Engine as _;
use hyper::StatusCode;
use icetray::cli::ENV_CONFIG;
use icetray::error::{ERROR_PREFIX, HINT_PREFIX};
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

/// Asserts the run ended by reporting its failure as plain text on stderr:
/// after whatever warnings it logged there, each a JSON log event, one
/// error line in clap's form, then at most one hint line. Returns whether
/// the hint line is there.
fn assert_plain_error(output: &Output) -> bool {
    let text = stderr(output);
    let is_event =
        |line: &str| serde_json::from_str::<Json>(line).is_ok_and(|json| json.is_object());
    let lines: Vec<&str> = text.lines().collect();
    let report = lines
        .iter()
        .position(|line| !is_event(line))
        .map_or(&[][..], |first| &lines[first..]);
    assert!(
        report
            .first()
            .is_some_and(|line| line.starts_with(ERROR_PREFIX)),
        "{text}"
    );
    match &report[1..] {
        [] => false,
        [hint] if hint.starts_with(HINT_PREFIX) => true,
        _ => panic!("more than an error and a hint after the log events: {text}"),
    }
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

/// A table of the two-table configuration, in TOML, pinned to `fixture`,
/// with `strict` off.
fn toml_table(name: &str, fixture: &Fixture) -> String {
    format!(
        "{}strict = false\n",
        toml_table_default_strict(name, fixture)
    )
}

/// `toml_table` with `strict` left out, so the table takes the default.
fn toml_table_default_strict(name: &str, fixture: &Fixture) -> String {
    format!(
        "[[tables]]\nname = \"{name}\"\nsource = {{ type = \"static\", location = \"{}\" }}\nref = \"main\"\nkey_column = \"key\"\nvalue_column = \"value\"\n",
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

/// A `--location` that is a relative path opens the table under the
/// working directory, named after its root, while a configuration file
/// holding the same relative path is refused: a file means the same from
/// any directory.
#[tokio::test]
async fn test_info_resolves_a_relative_location_but_not_a_file_s() -> TestResult {
    let dir = tempfile::tempdir()?;
    let name = "kv_relative";
    let fixture = local_fixture(dir.path(), FixtureSpec::small(name, KeyType::Long)).await?;
    let under_dir = format!("file://{}/", dir.path().display());
    let relative = fixture
        .metadata_location()
        .strip_prefix(&under_dir)
        .ok_or("the fixture lives under the directory")?;
    let in_dir = |args: &[&str]| {
        Command::new(BIN)
            .args(args)
            .current_dir(dir.path())
            .env_remove(ENV_CONFIG)
            .output()
    };

    let located = in_dir(&["info", "--location", relative])?;
    assert_exit(&located, EXIT_OK);
    let info = lines(&located.stdout);
    assert_eq!(info.len(), 1);
    assert_eq!(info[0]["table"], name);

    let text = toml_table(TABLE_A, &fixture).replace(fixture.metadata_location(), relative);
    let config = write_file(dir.path(), "relative.toml", &text)?;
    let configured = in_dir(&["info", "--config", &config])?;
    assert_exit(&configured, EXIT_FAILURE);
    assert!(configured.stdout.is_empty());
    Ok(())
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
            "{{\"kind\":\"table\",\"table\":\"{}\",",
            fixture.ident().name()
        )),
        "{raw}"
    );
    let info = lines(&output.stdout);
    assert_eq!(info.len(), 1);
    assert_eq!(info[0]["kind"], "table");
    assert_eq!(info[0]["table"], fixture.ident().name());
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
    assert_eq!(report[0]["kind"], "table");
    assert_eq!(report[0]["table"], fixture.ident().name());
    assert_eq!(report[0]["serviceability"], "efficient");
    assert_eq!(report[0]["inefficient"], Json::from(0));
    assert_eq!(report[0]["advice"], Json::from(0));
    assert!(report.iter().all(|line| line["kind"] != "finding"));
    assert_eq!(
        report.iter().filter(|line| line["kind"] == "file").count(),
        approx::length(SMALL_BUCKETS)
    );
    Ok(())
}

/// A small fixture with no bucket partition, which the doctor finds
/// inefficient.
async fn unpartitioned_fixture(dir: &Path, name: &str) -> Result<Fixture, BoxError> {
    let spec = FixtureSpec {
        buckets: 0,
        layout: Layout {
            target_file_bytes: UNPARTITIONED_FILE_BYTES,
            ..Layout::contract()
        },
        ..FixtureSpec::small(name, KeyType::Long)
    };
    local_fixture(dir, spec).await
}

/// Asserts `report` opens with an inefficient `table` line and carries
/// the `no_bucket_partition` finding at the inefficient tier.
fn assert_unpartitioned_findings(report: &[Json]) {
    assert_eq!(report[0]["kind"], "table");
    assert_eq!(report[0]["serviceability"], "inefficient");
    assert!(
        report.iter().any(|line| line["kind"] == "finding"
            && line["code"] == "no_bucket_partition"
            && line["tier"] == "inefficient"),
        "{report:?}"
    );
}

#[tokio::test]
async fn test_doctor_reports_an_unpartitioned_table_inefficient_and_exits_2() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = unpartitioned_fixture(dir.path(), "kv_flat").await?;
    let output = run(&[&["doctor"][..], &location(&fixture)].concat(), &[]);
    assert_exit(&output, EXIT_INEFFICIENT);
    let report = lines(&output.stdout);
    assert_unpartitioned_findings(&report);
    assert!(report.iter().all(|line| line["kind"] != "refusal"));
    Ok(())
}

/// A configured table keeps its `strict`: under the default, on, the
/// doctor reports the inefficient table's findings and then refuses it, as
/// `info` and `serve` do, with exit 1; with `strict` off it is inefficient,
/// exit 2.
#[tokio::test]
async fn test_doctor_refuses_an_inefficient_table_its_configuration_makes_strict() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = unpartitioned_fixture(dir.path(), "kv_flat_strict").await?;
    let strict = write_file(
        dir.path(),
        "strict.toml",
        &toml_table_default_strict(TABLE_A, &fixture),
    )?;
    let output = run(&["doctor", "--config", &strict], &[]);
    assert_exit(&output, EXIT_FAILURE);
    let report = lines(&output.stdout);
    assert_unpartitioned_findings(&report);
    let refused: Vec<&Json> = report
        .iter()
        .filter(|line| line["kind"] == "refusal")
        .collect();
    assert_eq!(refused.len(), 1, "{report:?}");
    assert_eq!(refused[0]["table"], TABLE_A);
    assert_eq!(refused[0]["reason"], "inefficient");
    assert_eq!(
        report.last(),
        Some(refused[0]),
        "the refusal follows the findings"
    );
    assert_exit(&run(&["info", "--config", &strict], &[]), EXIT_FAILURE);

    let lenient = write_file(dir.path(), "lenient.toml", &toml_table(TABLE_A, &fixture))?;
    let output = run(&["doctor", "--config", &lenient], &[]);
    assert_exit(&output, EXIT_INEFFICIENT);
    let report = lines(&output.stdout);
    assert_unpartitioned_findings(&report);
    assert!(report.iter().all(|line| line["kind"] != "refusal"));
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
    assert_eq!(report[0]["kind"], "refusal");
    assert_eq!(report[0]["table"], fixture.ident().name());
    assert_eq!(report[0]["reason"], "unsorted");
    assert!(report[0].get("error_kind").is_none(), "{report:?}");
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
    assert_eq!(report[0]["kind"], "refusal");
    assert_eq!(report[0]["table"], fixture.ident().name());
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
        assert_eq!(line["kind"], "lookup");
        assert_eq!(line["table"], fixture.ident().name());
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

/// Removes every Parquet data file under `dir`, so a load of a table there
/// fails while its metadata still reads; returns how many it removed.
fn remove_data_files(dir: &Path) -> Result<usize, BoxError> {
    let mut removed = 0;
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            removed += remove_data_files(&path)?;
        } else if path
            .extension()
            .is_some_and(|extension| extension == "parquet")
        {
            std::fs::remove_file(&path)?;
            removed += 1;
        }
    }
    Ok(removed)
}

/// A key that does not parse under the table's key type is a usage error
/// found from the metadata alone, before the load: with every data file
/// gone, so a load fails with exit 1, a bad key still exits 64.
#[tokio::test]
async fn test_get_rejects_a_key_that_does_not_parse_before_the_load() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(dir.path(), FixtureSpec::small("kv_badkey", KeyType::Long)).await?;
    let output = run(&[&["get"][..], &location(&fixture), &["abc"]].concat(), &[]);
    assert_exit(&output, EXIT_USAGE);
    assert!(output.stdout.is_empty());
    assert!(assert_plain_error(&output), "the key's form is the hint");

    assert!(remove_data_files(dir.path())? > 0);
    let unloadable = run(&[&["get"][..], &location(&fixture), &["1"]].concat(), &[]);
    assert_exit(&unloadable, EXIT_FAILURE);
    let bad_key = run(&[&["get"][..], &location(&fixture), &["abc"]].concat(), &[]);
    assert_exit(&bad_key, EXIT_USAGE);
    assert!(bad_key.stdout.is_empty());
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
    assert!(assert_plain_error(&unnamed), "the tables are the hint");

    let unknown = run(&["get", "--config", &config, "--table", "c", &key], &[]);
    assert_exit(&unknown, EXIT_USAGE);
    assert!(unknown.stdout.is_empty());
    assert!(assert_plain_error(&unknown), "the tables are the hint");

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

/// Flags that describe a located table, no target at all, and `--table`
/// beside `--location`, which opens one table rather than selecting among
/// configured ones, are each a usage error before anything is opened.
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

    let selected = run(
        &[&["info"][..], &location(&fixture), &["--table", "kv_flags"]].concat(),
        &[],
    );
    assert_exit(&selected, EXIT_USAGE);
    assert!(selected.stdout.is_empty());
    assert!(
        assert_plain_error(&selected),
        "the choice between the two is the hint"
    );
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
    assert_eq!(lines(&located.stdout)[0]["table"], fixture.ident().name());

    let config = write_file(dir.path(), "env.toml", &toml_table(TABLE_A, &fixture))?;
    let bare = run(&["info"], &[(ENV_CONFIG, &config)]);
    assert_exit(&bare, EXIT_OK);
    assert_eq!(lines(&bare.stdout)[0]["table"], TABLE_A);
    Ok(())
}

/// With no `--config` and `ICETRAY_CONFIG` unset or empty, every command,
/// `serve` and each one-shot, has no configuration: a usage error, exit 64,
/// before anything is opened. A path that names no file is the file's
/// failure, exit 1, whether the flag or the variable named it.
#[test]
fn test_every_command_treats_an_empty_config_variable_as_unset() -> TestResult {
    let dir = tempfile::tempdir()?;
    let missing = dir.path().join("missing.toml");
    let missing = missing.to_str().expect("a utf-8 temporary path");
    let commands: [&[&str]; 4] = [&["serve"], &["doctor"], &["info"], &["get", "1"]];
    for args in commands {
        for env in [&[][..], &[(ENV_CONFIG, "")]] {
            let output = run(args, env);
            assert_exit(&output, EXIT_USAGE);
            assert!(output.stdout.is_empty(), "{args:?} {env:?}");
            assert_plain_error(&output);
        }
        let from_env = run(args, &[(ENV_CONFIG, missing)]);
        assert_exit(&from_env, EXIT_FAILURE);
        assert_plain_error(&from_env);
        let from_flag = run(&[args, &["--config", missing]].concat(), &[]);
        assert_exit(&from_flag, EXIT_FAILURE);
        assert_plain_error(&from_flag);
    }
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

/// A file that is not TOML is the file's failure, exit 1, reported as one
/// plain error line like every other: the parser's place and message, not
/// its excerpt of the file over several lines.
#[test]
fn test_a_toml_syntax_error_is_one_error_line() -> TestResult {
    let dir = tempfile::tempdir()?;
    let config = write_file(dir.path(), "broken.toml", "[server]\nlisten = \n")?;
    let output = run(&["info", "--config", &config], &[]);
    assert_exit(&output, EXIT_FAILURE);
    assert!(output.stdout.is_empty(), "{}", stderr(&output));
    assert!(!assert_plain_error(&output), "{}", stderr(&output));
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

/// A `--rewrite` rule that is not `FROM=TO` with both halves set is a
/// usage error, refused before anything is opened with exit 64; the same
/// rule in the file is the file's fault, exit 1.
#[tokio::test]
async fn test_an_unusable_rewrite_is_usage_on_the_command_line_and_failure_in_the_file()
-> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(dir.path(), FixtureSpec::small("kv_empty", KeyType::Long)).await?;
    for rule in [
        "=file:///elsewhere/",
        "file:///elsewhere/=",
        "file:///elsewhere/",
    ] {
        let output = run(
            &[&["info"][..], &location(&fixture), &["--rewrite", rule]].concat(),
            &[],
        );
        assert_exit(&output, EXIT_USAGE);
        assert!(output.stdout.is_empty(), "{rule}: {}", stderr(&output));
    }
    let text = format!(
        "[[storage.rewrite]]\nfrom = \"\"\nto = \"file:///elsewhere/\"\n\n{}",
        toml_table(TABLE_A, &fixture)
    );
    let config = write_file(dir.path(), "empty-from.toml", &text)?;
    let output = run(&["info", "--config", &config], &[]);
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
    assert_eq!(line["kind"], "lookup");
    drop(reader);
    let output = child.wait_with_output()?;
    assert_exit(&output, EXIT_OK);
    Ok(())
}

/// A failure reported to a stderr whose reader is gone ends with the
/// failure's own exit code: the usage code for a run with no target, the
/// failure code for a location that cannot be read. The pipe's read end is
/// closed before the binary starts, so every write to stderr fails.
#[test]
fn test_a_closed_stderr_keeps_the_failure_s_exit_code() -> TestResult {
    let dir = tempfile::tempdir()?;
    let missing = dir.path().join("missing").join("kv");
    let missing = missing.to_str().expect("a utf-8 temporary path");
    let cases: [(&[&str], u8); 2] = [
        (&["info"], EXIT_USAGE),
        (&["info", "--location", missing], EXIT_FAILURE),
    ];
    for (args, expected) in cases {
        let (reader, writer) = std::io::pipe()?;
        drop(reader);
        let status = Command::new(BIN)
            .args(args)
            .env_remove(ENV_CONFIG)
            .stdout(Stdio::null())
            .stderr(writer)
            .status()?;
        assert_eq!(status.code(), Some(i32::from(expected)), "{args:?}");
    }
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

/// A configuration with no table, `[[tables]]` absent or an empty list, is
/// the file's fault: every one-shot refuses it with exit 1 before anything
/// is opened, not with the usage code.
#[tokio::test]
async fn test_a_configuration_with_no_table_exits_1() -> TestResult {
    let dir = tempfile::tempdir()?;
    let configs = [
        ("absent.toml", "[server]\npoll_interval = \"30s\"\n"),
        ("empty.json", "{\"tables\": []}"),
    ];
    for (name, text) in configs {
        let config = write_file(dir.path(), name, text)?;
        for args in [
            &["doctor", "--config", &config][..],
            &["info", "--config", &config],
            &["get", "--config", &config, "1"],
        ] {
            let output = run(args, &[]);
            assert_exit(&output, EXIT_FAILURE);
            assert!(output.stdout.is_empty(), "{args:?}: {}", stderr(&output));
            assert_plain_error(&output);
        }
    }
    Ok(())
}

/// `serve` refuses to start on a configuration with no table, exit 1,
/// rather than report ready while serving nothing. The refusal comes before
/// logging starts, so it is plain text on stderr and the log stream,
/// stdout, carries nothing.
#[tokio::test(flavor = "multi_thread")]
async fn test_serve_exits_1_on_a_configuration_with_no_table() -> TestResult {
    let dir = tempfile::tempdir()?;
    let config = serde_json::json!({
        "server": {"listen": "127.0.0.1:0", "metrics_listen": "127.0.0.1:0"},
        "tables": []
    });
    let path = write_file(dir.path(), "serve.json", &config.to_string())?;
    // Spawned and waited on within a bound rather than run to its end, so
    // a daemon that wrongly starts serving fails the test instead of
    // holding it open.
    let mut daemon = Daemon(
        Command::new(BIN)
            .args(["serve", "--config", &path])
            .env_remove(ENV_CONFIG)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?,
    );
    let status = wait_within(&mut daemon.0, READY_WAIT).await?;
    assert_eq!(status.code(), Some(i32::from(EXIT_FAILURE)));
    let mut output = Output {
        status,
        stdout: Vec::new(),
        stderr: Vec::new(),
    };
    if let Some(mut stdout) = daemon.0.stdout.take() {
        std::io::Read::read_to_end(&mut stdout, &mut output.stdout)?;
    }
    if let Some(mut stderr) = daemon.0.stderr.take() {
        std::io::Read::read_to_end(&mut stderr, &mut output.stderr)?;
    }
    assert!(output.stdout.is_empty(), "{}", stderr(&output));
    assert_plain_error(&output);
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

    send(&daemon, "-TERM")?;
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

/// A daemon serving the configuration at `config`, spawned and waited on
/// until ready: the daemon, the reader of its log, kept open so the
/// daemon's log writes never meet a closed pipe, and its metrics address.
async fn serve_ready(
    config: &str,
) -> Result<(Daemon, BufReader<ChildStdout>, SocketAddr), BoxError> {
    let mut daemon = Daemon(
        Command::new(BIN)
            .args(["serve", "--config", config])
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
    Ok((daemon, reader, metrics))
}

/// Sends `signal` (`-HUP`, `-TERM`) to `daemon` through `kill`.
fn send(daemon: &Daemon, signal: &str) -> TestResult {
    let sent = Command::new("kill")
        .arg(signal)
        .arg(daemon.0.id().to_string())
        .status()?;
    assert!(sent.success(), "kill {signal}");
    Ok(())
}

/// Waits for the scrape at `metrics` to count a reload of `result`, whose
/// series reads 0 from the start, and returns that scrape.
async fn reload_counted(metrics: SocketAddr, result: ReloadResult) -> Result<String, BoxError> {
    let label = result.to_string();
    poll_until(READY_WAIT, "a reload counted", async || {
        let (_, exposition) = http_get(metrics, METRICS_PATH).await?;
        let reloads = series_value(&exposition, CONFIG_RELOADS_TOTAL, &[(LABEL_RESULT, &label)]);
        Ok(reloads
            .is_some_and(|count| count > 0.0)
            .then_some(exposition))
    })
    .await
}

/// Ends `daemon` with SIGTERM and asserts its drain exits 0.
async fn drain(mut daemon: Daemon) -> TestResult {
    send(&daemon, "-TERM")?;
    let status = wait_within(&mut daemon.0, SHUTDOWN_WAIT).await?;
    assert_eq!(status.code(), Some(i32::from(EXIT_OK)));
    Ok(())
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
    let (mut daemon, _log, metrics) = serve_ready(&config).await?;

    std::fs::write(
        &config,
        json_config_polling_never(&[(TABLE_A, &first), (TABLE_B, &second)]),
    )?;
    send(&daemon, "-HUP")?;
    let exposition = reload_counted(metrics, ReloadResult::Applied).await?;
    assert_eq!(
        series_value(
            &exposition,
            CONFIG_RELOADS_TOTAL,
            &[(LABEL_RESULT, &ReloadResult::Applied.to_string())]
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
    drain(daemon).await
}

/// A reload to a file with no table is refused like any file that does
/// not validate: the table serving serves on, readiness stays, and
/// `config_pending` reads 1 while the file differs from what is applied.
#[tokio::test(flavor = "multi_thread")]
async fn test_serve_refuses_a_reload_to_a_file_with_no_table() -> TestResult {
    let dir = tempfile::tempdir()?;
    let fixture = local_fixture(dir.path(), FixtureSpec::small("kv_kept", KeyType::Long)).await?;
    let text = json_config_polling_never(&[(TABLE_A, &fixture)]);
    let config = write_file(dir.path(), "serve.json", &text)?;
    let (daemon, _log, metrics) = serve_ready(&config).await?;

    let mut empty: Json = serde_json::from_str(&text)?;
    empty["tables"] = Json::Array(Vec::new());
    std::fs::write(&config, empty.to_string())?;
    send(&daemon, "-HUP")?;
    let exposition = reload_counted(metrics, ReloadResult::Refused).await?;
    assert_eq!(
        series_value(&exposition, CONFIG_PENDING, &[]).map(f64::to_bits),
        Some(1.0_f64.to_bits())
    );
    assert_eq!(
        series_value(
            &exposition,
            TABLE_STATE,
            &[(LABEL_TABLE, TABLE_A), (LABEL_STATE, &serving_state())]
        )
        .map(f64::to_bits),
        Some(1.0_f64.to_bits()),
        "the table serving serves on"
    );
    let (status, _) = http_get(metrics, READY_PATH).await?;
    assert_eq!(status, StatusCode::OK);
    drain(daemon).await
}
