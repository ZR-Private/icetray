//! The truth-file check, a program and not a test: it replays a truth file
//! (`icetray_testkit::truth`) against a running server over gRPC and says
//! whether every answer was the one the file expects. Each pass sends every
//! entry, as one `Get` per key or as `MultiGet`s of `--batch` consecutive
//! keys, with `--concurrency` calls in flight, and compares each answer with
//! `truth::compare`. It reports per pass the count of each outcome, the
//! first `MISMATCH_SAMPLE` mismatching keys and failed calls, and the call
//! latency's percentiles and maximum, split into present and absent keys
//! for `Get`; and for the whole run the object-store GETs per lookup the
//! server's `/metrics` counted between a scrape before the first pass and
//! one after the last. The GET count is the server's across every table,
//! so it is this table's only while nothing else is asked of the server.
//! Results print as a table and are written as JSON to `--out`. The exit
//! code is 0 only when every answer matched and no call failed. Run with
//! `cargo run -p icetray-testkit --example check --release -- --help`.

use std::io::BufReader;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use clap::{Parser, ValueEnum};
use http_body_util::{BodyExt as _, Empty};
use hyper::header::HOST;
use hyper::{Request, StatusCode, Uri};
use hyper_util::rt::TokioIo;
use icetray::listen::METRICS_PATH;
use icetray_core::config::ServerConfig;
use icetray_core::{approx, metric};
use icetray_proto::v1::icetray_client::IcetrayClient;
use icetray_proto::v1::{
    self, ColumnRead, GetRequest, KeyType, MultiGetRequest, Presence, TableInfoRequest,
};
use icetray_testkit::TestkitError;
use icetray_testkit::latency::{Latencies, P50, P90, P99, micros};
use icetray_testkit::truth::{self, Entry, Outcome};
use icetray_testkit::verify::series_sum;
use serde_json::{Value as Json, json};
use tonic::transport::Channel;

// ---------------------------------------------------------------------------
// The command line.

/// Keys per `MultiGet` when `--batch` is not given: sixty, a page of
/// results a caller shows at once.
const DEFAULT_BATCH: u32 = 60;

/// Calls in flight when `--concurrency` is not given: enough to keep a
/// server's workers busy without queueing behind its in-flight cap.
const DEFAULT_CONCURRENCY: u32 = 8;

/// Mismatching keys, and failed calls, a pass reports by name; the rest
/// are counted. Enough to see a pattern, few enough to read.
const MISMATCH_SAMPLE: usize = 20;

/// How each pass asks for the truth file's keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Mode {
    /// One `Get` per key.
    Get,
    /// One `MultiGet` per `--batch` consecutive keys.
    MultiGet,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::MultiGet => "multi-get",
        }
    }
}

/// The server's gRPC listener as a URI, on the default configuration.
fn default_endpoint() -> String {
    format!("http://{}", ServerConfig::default().listen)
}

/// The server's metrics listener as a URI, on the default configuration.
fn default_metrics() -> String {
    format!("http://{}", ServerConfig::default().metrics_listen)
}

/// The check's command line.
#[derive(Parser, Debug)]
#[command(about = "Replays a truth file against a running icetray and compares every answer")]
struct Cli {
    /// The server's gRPC endpoint.
    #[arg(long, default_value_t = default_endpoint())]
    endpoint: String,
    /// The server's metrics listener, whose `/metrics` is scraped before
    /// and after the run.
    #[arg(long, default_value_t = default_metrics())]
    metrics: String,
    /// The served table to ask; its key type must be string.
    #[arg(long)]
    table: String,
    /// The value column whose bytes the truth file holds; every lookup
    /// names it alone.
    #[arg(long)]
    column: String,
    /// The truth file: JSON Lines of `key` and `value_base64`.
    #[arg(long)]
    truth: PathBuf,
    /// One `Get` per key, or `MultiGet`s of `--batch` keys.
    #[arg(long, value_enum, default_value_t = Mode::Get)]
    mode: Mode,
    /// Keys per `MultiGet`.
    #[arg(long, default_value_t = DEFAULT_BATCH, value_parser = clap::value_parser!(u32).range(1..))]
    batch: u32,
    /// Calls in flight at once.
    #[arg(long, default_value_t = DEFAULT_CONCURRENCY, value_parser = clap::value_parser!(u32).range(1..))]
    concurrency: u32,
    /// Times the whole file is replayed.
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u32).range(1..))]
    passes: u32,
    /// Where the JSON results go.
    #[arg(long, default_value = "target/check/results.json")]
    out: PathBuf,
}

// ---------------------------------------------------------------------------
// Errors.

/// Why the check could not run to its verdict. A wrong answer or a failed
/// lookup call is part of the verdict, not one of these.
#[derive(Debug, thiserror::Error)]
enum CheckError {
    /// The truth file could not be read or parsed.
    #[error("{0}")]
    Truth(#[from] TestkitError),
    /// A file could not be opened or written, or a socket connected.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// The gRPC endpoint could not be reached.
    #[error("grpc transport: {0}")]
    Transport(#[from] tonic::transport::Error),
    /// The table could not be described, so nothing was asked.
    #[error("describing the table: {0}")]
    TableInfo(#[from] tonic::Status),
    /// The table's keys are not strings, which is all a truth file holds.
    #[error("table {table} has key type {key_type:?}; a truth file holds string keys")]
    KeyType {
        /// The table asked for.
        table: String,
        /// The key type the server reported.
        key_type: KeyType,
    },
    /// The metrics location is not a URI with a host and a port.
    #[error("metrics location {location} needs a host and a port")]
    MetricsLocation {
        /// The location given.
        location: String,
    },
    /// The endpoint or the metrics location does not parse as a URI.
    #[error("uri: {0}")]
    Uri(#[from] hyper::http::uri::InvalidUri),
    /// The scrape request could not be built.
    #[error("metrics request: {0}")]
    MetricsRequest(#[from] hyper::http::Error),
    /// The scrape failed on the wire.
    #[error("metrics scrape: {0}")]
    MetricsHttp(#[from] hyper::Error),
    /// The scrape answered other than 200.
    #[error("metrics scrape answered {status}")]
    MetricsStatus {
        /// The status it answered.
        status: StatusCode,
    },
    /// The scrape's body is not text.
    #[error("metrics body: {0}")]
    MetricsBody(#[from] std::string::FromUtf8Error),
    /// The results could not be written as JSON.
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    /// A pass's task panicked or was cancelled.
    #[error("task: {0}")]
    Join(#[from] tokio::task::JoinError),
}

// ---------------------------------------------------------------------------
// The metrics.

/// The two counters a run is priced by, as one scrape reads them.
#[derive(Clone, Copy, Debug)]
struct Counters {
    /// Lookup GETs across every table.
    object_gets: f64,
    /// Lookups of the checked table, every result.
    lookups: f64,
}

/// `GET /metrics` at `location` over HTTP/1, and the two counters in it.
async fn scrape(location: &str, table: &str) -> Result<Counters, CheckError> {
    let uri: Uri = location.parse()?;
    let (Some(host), Some(port)) = (uri.host(), uri.port_u16()) else {
        return Err(CheckError::MetricsLocation {
            location: location.to_string(),
        });
    };
    let stream = tokio::net::TcpStream::connect((host, port)).await?;
    let (mut sender, connection) =
        hyper::client::conn::http1::handshake::<_, Empty<Bytes>>(TokioIo::new(stream)).await?;
    // intentional: the connection serves this one request; an error after
    // the response was read changes nothing the check reports.
    tokio::spawn(connection);
    let request = Request::builder()
        .uri(METRICS_PATH)
        .header(HOST, format!("{host}:{port}"))
        .body(Empty::new())?;
    let response = sender.send_request(request).await?;
    let status = response.status();
    if status != StatusCode::OK {
        return Err(CheckError::MetricsStatus { status });
    }
    let body = String::from_utf8(response.into_body().collect().await?.to_bytes().to_vec())?;
    // Lookup GETs only: load GETs of a refresh during the run are left out.
    let lookup = metric::ReadKind::Lookup.to_string();
    Ok(Counters {
        object_gets: series_sum(
            &body,
            metric::OBJECT_GETS_TOTAL,
            &[(metric::LABEL_KIND, &lookup)],
        ),
        lookups: series_sum(
            &body,
            metric::LOOKUPS_TOTAL,
            &[(metric::LABEL_TABLE, table)],
        ),
    })
}

// ---------------------------------------------------------------------------
// Calls.

/// Why a call's answer could not be compared.
#[derive(Debug)]
enum Failure {
    /// The call failed with a gRPC status.
    Status(tonic::Status),
    /// A key answered with a presence that is neither present nor absent,
    /// one the server never sends.
    Presence {
        /// The key.
        key: String,
        /// The presence answered, or its raw number when it is not one
        /// this client knows.
        presence: Result<Presence, i32>,
    },
    /// A present key answered other than one value of bytes: a null value,
    /// which a truth file cannot expect, or another number of values than
    /// the one column asked.
    Values {
        /// The key.
        key: String,
        /// The values answered.
        answered: usize,
        /// Whether the first of them is null.
        null: bool,
    },
    /// A `MultiGet` answered a different number of results than it asked.
    Results {
        /// Keys asked.
        asked: usize,
        /// Results answered.
        answered: usize,
    },
}

impl Failure {
    fn describe(&self) -> String {
        match self {
            Self::Status(status) => format!("{:?}: {}", status.code(), status.message()),
            Self::Presence { key, presence } => match presence {
                Ok(presence) => format!("{key} answered {}", presence.as_str_name()),
                Err(raw) => format!("{key} answered unknown presence {raw}"),
            },
            Self::Values {
                key,
                answered,
                null,
            } => format!("{key} answered {answered} values, the first null: {null}"),
            Self::Results { asked, answered } => {
                format!("asked {asked} keys, answered {answered}")
            }
        }
    }
}

/// One answered key as the check reads it: `Some` bytes for present with
/// its one value, `None` for absent, or why it is neither.
fn answered(
    key: &str,
    presence: i32,
    values: Vec<v1::ColumnValue>,
) -> Result<Option<Bytes>, Failure> {
    match Presence::try_from(presence) {
        Ok(Presence::Present) => {
            let null = values
                .first()
                .is_some_and(|value| !matches!(value.kind, Some(v1::column_value::Kind::Value(_))));
            let answered = values.len();
            match values.into_iter().next().and_then(|value| value.kind) {
                Some(v1::column_value::Kind::Value(bytes)) if answered == 1 => Ok(Some(bytes)),
                Some(v1::column_value::Kind::Value(_) | v1::column_value::Kind::NullValue(_))
                | None => Err(Failure::Values {
                    key: key.to_string(),
                    answered,
                    null,
                }),
            }
        }
        Ok(Presence::Absent) => Ok(None),
        Ok(other @ Presence::Unspecified) => Err(Failure::Presence {
            key: key.to_string(),
            presence: Ok(other),
        }),
        Err(_) => Err(Failure::Presence {
            key: key.to_string(),
            presence: Err(presence),
        }),
    }
}

/// A truth-file key on the wire.
fn wire_key(entry: &Entry) -> v1::Key {
    v1::Key {
        key: Some(v1::key::Key::Str(entry.key.clone())),
    }
}

/// One timed call over the entries `unit` names, compared entry by entry.
struct Call {
    unit: Range<usize>,
    elapsed: Duration,
    outcomes: Result<Vec<Outcome>, Failure>,
}

/// Sends the entries of `unit` as one call of `mode`, timing it, and
/// compares each answer with its entry.
async fn call(
    client: &mut IcetrayClient<Channel>,
    (table, column): (&str, &str),
    mode: Mode,
    entries: &[Entry],
    unit: Range<usize>,
) -> Call {
    let asked = &entries[unit.clone()];
    let started = Instant::now();
    let answers = match mode {
        Mode::Get => get(client, (table, column), &asked[0])
            .await
            .map(|answer| vec![answer]),
        Mode::MultiGet => multi_get(client, (table, column), asked).await,
    };
    let elapsed = started.elapsed();
    let outcomes = answers.map(|answers| {
        asked
            .iter()
            .zip(&answers)
            .map(|(entry, answer)| truth::compare(entry.expected.as_deref(), answer.as_deref()))
            .collect()
    });
    Call {
        unit,
        elapsed,
        outcomes,
    }
}

async fn get(
    client: &mut IcetrayClient<Channel>,
    (table, column): (&str, &str),
    entry: &Entry,
) -> Result<Option<Bytes>, Failure> {
    let response = client
        .get(GetRequest {
            table: table.to_string(),
            key: Some(wire_key(entry)),
            columns: vec![ColumnRead {
                name: column.to_string(),
                fields: Vec::new(),
            }],
        })
        .await
        .map_err(Failure::Status)?
        .into_inner();
    answered(&entry.key, response.presence, response.values)
}

async fn multi_get(
    client: &mut IcetrayClient<Channel>,
    (table, column): (&str, &str),
    asked: &[Entry],
) -> Result<Vec<Option<Bytes>>, Failure> {
    let response = client
        .multi_get(MultiGetRequest {
            table: table.to_string(),
            keys: asked.iter().map(wire_key).collect(),
            columns: vec![ColumnRead {
                name: column.to_string(),
                fields: Vec::new(),
            }],
        })
        .await
        .map_err(Failure::Status)?
        .into_inner();
    if response.results.len() != asked.len() {
        return Err(Failure::Results {
            asked: asked.len(),
            answered: response.results.len(),
        });
    }
    asked
        .iter()
        .zip(response.results)
        .map(|(entry, hit)| answered(&entry.key, hit.presence, hit.values))
        .collect()
}

// ---------------------------------------------------------------------------
// A pass.

/// The units a pass sends, in file order: one entry each for `Get`, runs
/// of `batch` consecutive entries for `MultiGet`, the last one shorter.
fn units(entries: usize, mode: Mode, batch: usize) -> Vec<Range<usize>> {
    let size = match mode {
        Mode::Get => 1,
        Mode::MultiGet => batch,
    };
    (0..entries)
        .step_by(size)
        .map(|start| start..(start + size).min(entries))
        .collect()
}

/// Every entry once, over `concurrency` tasks that each send units `task`,
/// `task + concurrency`, ... one call at a time, so `concurrency` calls
/// are in flight; every call, in no particular order, and the pass's wall
/// time.
async fn pass(
    client: &IcetrayClient<Channel>,
    (table, column): (&Arc<str>, &Arc<str>),
    mode: Mode,
    entries: &Arc<Vec<Entry>>,
    units: &Arc<Vec<Range<usize>>>,
    concurrency: usize,
) -> Result<(Vec<Call>, Duration), CheckError> {
    let started = Instant::now();
    let tasks: Vec<_> = (0..concurrency)
        .map(|task| {
            let mut client = client.clone();
            let (table, column, entries, units) = (
                Arc::clone(table),
                Arc::clone(column),
                Arc::clone(entries),
                Arc::clone(units),
            );
            tokio::spawn(async move {
                let mut calls = Vec::new();
                for unit in units.iter().skip(task).step_by(concurrency) {
                    calls.push(
                        call(&mut client, (&table, &column), mode, &entries, unit.clone()).await,
                    );
                }
                calls
            })
        })
        .collect();
    let mut calls = Vec::with_capacity(units.len());
    for task in tasks {
        calls.extend(task.await?);
    }
    Ok((calls, started.elapsed()))
}

// ---------------------------------------------------------------------------
// Reporting.

/// The latencies of one class of a pass's calls.
struct ClassLatencies {
    class: &'static str,
    latencies: Latencies,
}

impl ClassLatencies {
    fn of(class: &'static str, samples: Vec<Duration>) -> Self {
        Self {
            class,
            latencies: Latencies::of(samples),
        }
    }

    fn to_json(&self) -> Json {
        let latencies = &self.latencies;
        json!({
            "class": self.class,
            "calls": latencies.len(),
            "p50_us": micros(latencies.percentile(P50)),
            "p90_us": micros(latencies.percentile(P90)),
            "p99_us": micros(latencies.percentile(P99)),
            "max_us": micros(latencies.max()),
        })
    }
}

/// Keys of a pass by outcome, failed keys among them.
#[derive(Clone, Copy, Debug, Default)]
struct Tally {
    matched: usize,
    wrong_value: usize,
    missing_value: usize,
    unexpected_value: usize,
    /// Keys whose call failed, so they were never compared.
    failed: usize,
}

impl Tally {
    fn add(&mut self, outcome: Outcome) {
        match outcome {
            Outcome::Match => self.matched += 1,
            Outcome::WrongValue { .. } => self.wrong_value += 1,
            Outcome::MissingValue => self.missing_value += 1,
            Outcome::UnexpectedValue => self.unexpected_value += 1,
        }
    }

    fn mismatched(self) -> usize {
        self.wrong_value + self.missing_value + self.unexpected_value
    }

    fn to_json(self) -> Json {
        json!({
            "match": self.matched,
            "wrong_value": self.wrong_value,
            "missing_value": self.missing_value,
            "unexpected_value": self.unexpected_value,
            "failed_keys": self.failed,
        })
    }
}

/// An outcome as the report names it.
fn outcome_text(outcome: Outcome) -> String {
    match outcome {
        Outcome::Match => "match".to_string(),
        Outcome::WrongValue {
            expected_len,
            actual_len,
        } => format!("wrong value ({expected_len} bytes expected, {actual_len} answered)"),
        Outcome::MissingValue => "missing value (expected present, answered absent)".to_string(),
        Outcome::UnexpectedValue => {
            "unexpected value (expected absent, answered present)".to_string()
        }
    }
}

/// One pass, summarized.
struct PassReport {
    pass: u32,
    wall: Duration,
    tally: Tally,
    /// The first `MISMATCH_SAMPLE` mismatching keys in file order, with
    /// their outcomes.
    mismatches: Vec<(String, Outcome)>,
    /// Failed calls: the first key of each, the keys it carried, and why,
    /// the first `MISMATCH_SAMPLE` in file order.
    failures: Vec<(String, usize, Failure)>,
    failed_calls: usize,
    latencies: Vec<ClassLatencies>,
}

impl PassReport {
    fn of(pass: u32, mode: Mode, entries: &[Entry], mut calls: Vec<Call>, wall: Duration) -> Self {
        calls.sort_unstable_by_key(|call| call.unit.start);
        let mut tally = Tally::default();
        let mut mismatches = Vec::new();
        let mut failures = Vec::new();
        let mut failed_calls = 0;
        let (mut all, mut present, mut absent) = (Vec::new(), Vec::new(), Vec::new());
        for call in calls {
            all.push(call.elapsed);
            if mode == Mode::Get {
                match entries[call.unit.start].expected {
                    Some(_) => present.push(call.elapsed),
                    None => absent.push(call.elapsed),
                }
            }
            match call.outcomes {
                Ok(outcomes) => {
                    for (index, outcome) in call.unit.zip(outcomes) {
                        tally.add(outcome);
                        if outcome != Outcome::Match && mismatches.len() < MISMATCH_SAMPLE {
                            mismatches.push((entries[index].key.clone(), outcome));
                        }
                    }
                }
                Err(failure) => {
                    failed_calls += 1;
                    tally.failed += call.unit.len();
                    if failures.len() < MISMATCH_SAMPLE {
                        let first = entries[call.unit.start].key.clone();
                        failures.push((first, call.unit.len(), failure));
                    }
                }
            }
        }
        let mut latencies = vec![ClassLatencies::of("all", all)];
        if mode == Mode::Get {
            latencies.push(ClassLatencies::of("present", present));
            latencies.push(ClassLatencies::of("absent", absent));
        }
        Self {
            pass,
            wall,
            tally,
            mismatches,
            failures,
            failed_calls,
            latencies,
        }
    }

    fn clean(&self) -> bool {
        self.tally.mismatched() == 0 && self.failed_calls == 0
    }

    fn print(&self) {
        let tally = self.tally;
        println!(
            "pass {}: {:.2?}; {} match, {} wrong value, {} missing value, {} unexpected value, {} failed calls ({} keys)",
            self.pass,
            self.wall,
            tally.matched,
            tally.wrong_value,
            tally.missing_value,
            tally.unexpected_value,
            self.failed_calls,
            tally.failed,
        );
        println!(
            "  {:<8} {:>8} {:>10} {:>10} {:>10} {:>10}",
            "class", "calls", "p50", "p90", "p99", "max"
        );
        for class in &self.latencies {
            println!(
                "  {:<8} {:>8} {:>10.2?} {:>10.2?} {:>10.2?} {:>10.2?}",
                class.class,
                class.latencies.len(),
                class.latencies.percentile(P50),
                class.latencies.percentile(P90),
                class.latencies.percentile(P99),
                class.latencies.max(),
            );
        }
        for (key, outcome) in &self.mismatches {
            println!("  mismatch {key}: {}", outcome_text(*outcome));
        }
        for (key, keys, failure) in &self.failures {
            println!(
                "  failed call at {key} ({keys} keys): {}",
                failure.describe()
            );
        }
    }

    fn to_json(&self) -> Json {
        json!({
            "pass": self.pass,
            "wall_us": micros(self.wall),
            "outcomes": self.tally.to_json(),
            "failed_calls": self.failed_calls,
            "latency": self.latencies.iter().map(ClassLatencies::to_json).collect::<Vec<_>>(),
            "mismatches": self.mismatches.iter().map(|(key, outcome)| json!({
                "key": key,
                "outcome": outcome_text(*outcome),
            })).collect::<Vec<_>>(),
            "failures": self.failures.iter().map(|(key, keys, failure)| json!({
                "first_key": key,
                "keys": keys,
                "failure": failure.describe(),
            })).collect::<Vec<_>>(),
        })
    }
}

/// Writes `report` to `path`, creating its directory.
fn write_report(path: &Path, report: &Json) -> Result<(), CheckError> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::write(path, serde_json::to_vec_pretty(report)?)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// The program.

#[tokio::main]
async fn main() -> Result<ExitCode, CheckError> {
    let cli = Cli::parse();
    let entries = truth::parse(BufReader::new(std::fs::File::open(&cli.truth)?))?;
    let present = entries
        .iter()
        .filter(|entry| entry.expected.is_some())
        .count();
    let channel = Channel::from_shared(cli.endpoint.clone())?
        .connect()
        .await?;
    let mut client = IcetrayClient::new(channel).max_decoding_message_size(usize::MAX);
    let info = client
        .table_info(TableInfoRequest {
            table: cli.table.clone(),
        })
        .await?
        .into_inner();
    // A key type this client does not know is not string either.
    let key_type = KeyType::try_from(info.key_type).unwrap_or(KeyType::Unspecified);
    if key_type != KeyType::String {
        return Err(CheckError::KeyType {
            table: cli.table,
            key_type,
        });
    }
    println!(
        "{}: snapshot {}, {} entries ({present} present, {} absent), {} x {}, {} in flight",
        cli.table,
        info.snapshot_id,
        entries.len(),
        entries.len() - present,
        cli.passes,
        cli.mode.name(),
        cli.concurrency,
    );

    let before = scrape(&cli.metrics, &cli.table).await?;
    let table: Arc<str> = Arc::from(cli.table.as_str());
    let column: Arc<str> = Arc::from(cli.column.as_str());
    let units = Arc::new(units(entries.len(), cli.mode, approx::length(cli.batch)));
    let entries = Arc::new(entries);
    let concurrency = approx::length(cli.concurrency);
    let mut passes = Vec::new();
    for pass_number in 1..=cli.passes {
        let (calls, wall) = pass(
            &client,
            (&table, &column),
            cli.mode,
            &entries,
            &units,
            concurrency,
        )
        .await?;
        let report = PassReport::of(pass_number, cli.mode, &entries, calls, wall);
        report.print();
        passes.push(report);
    }
    let after = scrape(&cli.metrics, &cli.table).await?;
    let object_gets = after.object_gets - before.object_gets;
    let lookups = after.lookups - before.lookups;
    // No lookups counted leaves the ratio undefined, not zero.
    let gets_per_lookup = (lookups > 0.0).then(|| object_gets / lookups);
    match gets_per_lookup {
        Some(ratio) => println!(
            "object GETs per lookup: {ratio:.3} ({object_gets} GETs over {lookups} lookups)"
        ),
        None => println!("object GETs per lookup: no lookups counted ({object_gets} GETs)"),
    }

    let clean = passes.iter().all(PassReport::clean);
    let report = json!({
        "table": cli.table,
        "snapshot_id": info.snapshot_id,
        "truth": cli.truth.display().to_string(),
        "entries": entries.len(),
        "present": present,
        "mode": cli.mode.name(),
        "batch": cli.batch,
        "concurrency": cli.concurrency,
        "passes": passes.iter().map(PassReport::to_json).collect::<Vec<_>>(),
        "object_gets": object_gets,
        "lookups": lookups,
        "object_gets_per_lookup": gets_per_lookup,
        "clean": clean,
    });
    write_report(&cli.out, &report)?;
    println!("results: {}", cli.out.display());
    if clean {
        println!("every answer matched");
        Ok(ExitCode::SUCCESS)
    } else {
        println!("mismatches or failed calls: see above");
        Ok(ExitCode::FAILURE)
    }
}
