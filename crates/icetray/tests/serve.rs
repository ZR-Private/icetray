//! The daemon in-process over the in-process S3 server (RFD 014, P tier):
//! health and readiness before and after the first loads, the wire contract
//! of RFD 010 with its error mapping, request counting, refresh within the
//! poll interval, the drain, the start-or-fail rule of RFD 005, and the
//! configuration reload of RFD 018.

use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use hyper::StatusCode;
use icetray::listen::{METRICS_PATH, NOT_READY_BODY, READY_BODY, READY_PATH};
use icetray::reload::{ConfigSource, Digest, Reading, Reload, ReloadResult};
use icetray::serve::{OVERALL_SERVICE, PeriodicTask};
use icetray::telemetry::{self, Call, CodeLabel};
use icetray::{ServeError, ServeOptions, Serving};
use icetray_core::approx;
use icetray_core::config::{
    Config, LocalConfig, Residency, ServerConfig, SourceConfig, TableConfig, ValueFormat,
};
use icetray_core::key::UUID_BYTES;
use icetray_core::metric::{self, Kind, LookupResult};
use icetray_core::{
    ColumnRead, ErrorKind, Generation, IoStats, Key, KeyType, MAX_FIELD_NUMBER, Row, Storage,
    TableState,
};
use icetray_proto::v1::icetray_client::IcetrayClient;
use icetray_proto::v1::icetray_server::SERVICE_NAME;
use icetray_proto::v1::{
    self, GetRequest, GetResponse, MultiGetRequest, MultiGetResponse, TableInfoRequest,
    TableInfoResponse,
};
use icetray_testkit::fixture::{NAMESPACE, SAMPLE, SECOND_VALUE_COLUMN, SMALL_BUCKETS, SMALL_IDS};
use icetray_testkit::protobuf;
use icetray_testkit::verify::{expected_cold_gets, series_sum, series_value};
use icetray_testkit::{
    DeleteForm, Dictionary, Fixture, FixtureSpec, Layout, MemoryCatalog, Nulls, S3Fixture,
    ValueKind, memory_catalog_s3, write_fixture,
};
use tokio::task::JoinHandle;
use tonic::transport::Channel;
use tonic::{Code, Status};
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;

mod common;

use common::{
    BoxError, NULL_EVERY, NULL_ID, PRESENT_ID, SHUTDOWN_WAIT, health_status, http_get, poll_until,
};
use icetray_testkit::layout::VALUE_COLUMN;

/// What every test returns.
type TestResult = Result<(), BoxError>;

/// The fixture server's bucket and the warehouse prefix inside it.
const BUCKET: &str = "icetray-serve";
const WAREHOUSE: &str = "warehouse";

/// The name of the served table in the one-table tests.
const TABLE: &str = "kv";

/// The name of the uuid-keyed table beside `TABLE`.
const UUID_TABLE: &str = "kv_uuid";

/// A name no table is served under.
const NOBODY: &str = "nobody";

/// A table no other test serves, so its counters read this test's alone.
const FRESH_TABLE: &str = "kv_fresh";

/// A table no other test serves, so its gauges are this test's alone.
const SCRAPED_TABLE: &str = "kv_scraped";

/// A disk table no other test serves, so its page-cache gauges are this
/// test's alone.
const SAMPLED_TABLE: &str = "kv_sampled";

/// How long the page-cache test waits for the first sample: the start's
/// sample runs as the tables serve, so this is room for a loaded machine.
const FIRST_SAMPLE_WAIT: Duration = Duration::from_secs(10);

/// A table served with deletes beside `TABLE`.
const DELETED_TABLE: &str = "kv_deleted";

/// Loopback with an OS-assigned port, for both listeners.
const LOOPBACK_ANY_PORT: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);

/// The poll interval the served tables refresh at: short, so a refresh
/// test finishes in well under a second.
const TEST_POLL_INTERVAL: Duration = Duration::from_millis(200);

/// How many polls a refresh test waits for the table to advance.
const REFRESH_POLLS: u32 = 10;

/// How long a refresh test waits for the table to advance.
const REFRESH_WAIT: Duration = TEST_POLL_INTERVAL.saturating_mul(REFRESH_POLLS);

/// How long the first-load test holds that the failed run stays up once
/// the failure is confirmed.
const STAYS_UP_FOR: Duration = Duration::from_millis(100);

/// How many `STAYS_UP_FOR` intervals the failed first load's grace spans
/// in the test: the one checked, and the rest as room for the poll that
/// confirms the failure and for a loaded machine.
const GRACE_INTERVALS: u32 = 5;

/// The grace a failed first load stays on the health service in the test.
const FAILURE_GRACE: Duration = STAYS_UP_FOR.saturating_mul(GRACE_INTERVALS);

/// How many `SAMPLE` ids the cold-cost test looks up; they share no page,
/// so the oracle prices them at one GET each.
const COLD_IDS: usize = 3;

/// How many `SAMPLE` ids the batch tests send beside the absent key.
const BATCH_IDS: usize = 4;

/// How many `Get`s the counting test issues.
const COUNTED_GETS: u64 = 3;

/// How many ids the refresh test appends.
const APPENDED_IDS: u64 = 16;

/// The ids the refresh test appends: above every id the small fixture
/// wrote, so the new files overlap nothing.
const NEW_IDS: std::ops::Range<u64> = SMALL_IDS.end..SMALL_IDS.end + APPENDED_IDS;

/// The file prefix of the refresh test's append.
const APPEND_PREFIX: &str = "build2";

/// The scheme every S3 location under the fixture bucket carries.
const S3_SCHEME: &str = "s3://";

/// The `Get` deadline the deadline tests configure: short, so a call over a
/// held store ends quickly.
const SHORT_DEADLINE: Duration = Duration::from_millis(300);

/// The `Get` deadline of the test that sets its `MultiGet` deadline to
/// `SHORT_DEADLINE`: longer, since a page fetch ends at the `Get` deadline,
/// so a `MultiGet` over a held store is seen to end at its own only when
/// its own comes first.
const LONGER_DEADLINE: Duration = SHORT_DEADLINE.saturating_mul(2);

/// A client deadline shorter than every deadline the server is configured
/// with in the test that sends it.
const CLIENT_DEADLINE: Duration = Duration::from_millis(200);

/// How long past its deadline a call may take to be answered: the timer's
/// wake-up and one loopback round trip, with room for a loaded machine.
const DEADLINE_MARGIN: Duration = Duration::from_millis(500);

/// The in-flight cap the admission tests configure: two, so one `Get` and
/// one `MultiGet` fill it.
const IN_FLIGHT_CAP: usize = 2;

/// How soon a refused call is answered: one loopback round trip, far
/// inside `SHORT_DEADLINE`, so the refusal is seen not to wait for a slot.
const REFUSED_WITHIN: Duration = Duration::from_millis(100);

/// How soon a shutdown during the first loads ends the run: the drain of a
/// daemon that never served, with room for a loaded machine, and far inside
/// the refresh deadline the held load would otherwise wait out.
const FIRST_LOAD_SHUTDOWN_WITHIN: Duration = Duration::from_secs(1);

/// The drain deadline the overrun test configures: short, and far inside
/// the default `get_deadline`, so the held call outlasts the drain.
const SHORT_DRAIN: Duration = Duration::from_millis(300);

/// The `metrics` recorder is process-global, so `requests_total` is one
/// counter for every test in this binary. Every test that issues an RPC
/// holds this lock, so the counting test's deltas are exact.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// A health `Watch` being collected: every status the stream sent, once
/// the server has ended it.
type Watching = JoinHandle<Result<Vec<ServingStatus>, BoxError>>;

/// A running daemon over fixtures in the in-process S3 server, with the
/// clients to drive it, the catalog the fixtures were written through, and
/// the storage counters to price it.
struct Stack {
    s3: S3Fixture,
    catalog: Arc<MemoryCatalog>,
    fixtures: Vec<Fixture>,
    serving: Serving,
    client: IcetrayClient<Channel>,
    health: HealthClient<Channel>,
    stats: Arc<IoStats>,
    /// The local directory `Setup::with_local_dir` made, kept for as long
    /// as the daemon serves from it.
    _local: Option<tempfile::TempDir>,
}

/// What `Stack::start_with` hands a test before the daemon starts: the
/// written fixtures, the configuration serving each from its pinned
/// metadata file at `Residency::Full` and not strict, and the default
/// options. The configuration is built directly, so nothing validates it
/// before the load does, and a test reaches what `Config::parse` refuses.
struct Setup {
    fixtures: Vec<Fixture>,
    config: Config,
    options: ServeOptions,
    /// Holds the in-process store before the daemon starts, so the first
    /// loads wait on it until it is released.
    hold_store: bool,
    /// The local directory the configuration names, when a test gave it
    /// one.
    local: Option<tempfile::TempDir>,
}

impl Setup {
    /// Gives the configuration a fresh local directory with no page cache
    /// disk level and serves every table from it in the Disk tier.
    fn with_local_dir(&mut self) -> Result<(), BoxError> {
        let dir = tempfile::tempdir()?;
        self.config.storage.local = Some(LocalConfig {
            dir: dir.path().to_path_buf(),
            index_budget_bytes: None,
            page_cache_disk_bytes: 0,
        });
        for table in &mut self.config.tables {
            table.residency = Residency::Disk;
        }
        self.local = Some(dir);
        Ok(())
    }
}

/// A configuration over `s3` serving `tables` with both listeners on
/// loopback at OS-assigned ports and the test poll interval.
fn config_over(s3: &S3Fixture, tables: Vec<TableConfig>) -> Config {
    Config {
        server: ServerConfig {
            listen: LOOPBACK_ANY_PORT,
            metrics_listen: LOOPBACK_ANY_PORT,
            poll_interval: TEST_POLL_INTERVAL,
            ..ServerConfig::default()
        },
        storage: s3.storage_config(),
        tables,
    }
}

impl Stack {
    /// Writes one fixture per spec and serves each from its pinned metadata
    /// file, not yet loaded.
    async fn start(specs: Vec<FixtureSpec>) -> Result<Self, BoxError> {
        Self::start_with(specs, async |_| Ok(())).await
    }

    /// `start`, with `configure` run over the `Setup` between the fixture
    /// writes and the daemon's start; the loads run behind the returned
    /// stack.
    async fn start_with(
        specs: Vec<FixtureSpec>,
        configure: impl AsyncFnOnce(&mut Setup) -> Result<(), BoxError>,
    ) -> Result<Self, BoxError> {
        let s3 = S3Fixture::start(BUCKET).await?;
        let catalog = memory_catalog_s3(&s3, WAREHOUSE).await?;
        let mut fixtures = Vec::with_capacity(specs.len());
        for spec in specs {
            fixtures.push(write_fixture(catalog.as_ref(), NAMESPACE, spec).await?);
        }
        let tables = fixtures
            .iter()
            .map(|fixture| fixture.config(Residency::Full, false))
            .collect();
        let mut setup = Setup {
            fixtures,
            config: config_over(&s3, tables),
            options: ServeOptions::default(),
            hold_store: false,
            local: None,
        };
        configure(&mut setup).await?;
        let Setup {
            fixtures,
            config,
            options,
            hold_store,
            local,
        } = setup;
        if hold_store {
            s3.hold();
        }
        let recorder = telemetry::install()?;
        let storage = Arc::new(Storage::new(s3.storage_config()));
        let stats = storage.stats();
        let serving = Serving::start_over(config, storage, recorder, options).await?;
        let channel = Channel::from_shared(format!("http://{}", serving.listen_addr()))?
            .connect()
            .await?;
        Ok(Self {
            s3,
            catalog,
            fixtures,
            serving,
            client: IcetrayClient::new(channel.clone()),
            health: HealthClient::new(channel),
            stats,
            _local: local,
        })
    }

    /// `start` with one small long-keyed fixture named `TABLE`, loaded.
    async fn ready_small() -> Result<Self, BoxError> {
        let mut stack = Self::start(vec![FixtureSpec::small(TABLE, KeyType::Long)]).await?;
        stack.serving.ready().await;
        Ok(stack)
    }

    /// The health status of `service`.
    async fn health_status(&mut self, service: &str) -> Result<ServingStatus, BoxError> {
        health_status(&mut self.health, service).await
    }

    /// Opens a `Watch` of `service` and collects, in a task, every status
    /// it sends until the server ends the stream. The watch is registered
    /// with the health service when this returns.
    async fn watch_health(&mut self, service: &str) -> Result<Watching, BoxError> {
        let mut stream = self
            .health
            .watch(HealthCheckRequest {
                service: service.to_string(),
            })
            .await?
            .into_inner();
        Ok(tokio::spawn(async move {
            let mut statuses = Vec::new();
            while let Some(response) = stream.message().await? {
                statuses.push(ServingStatus::try_from(response.status)?);
            }
            Ok(statuses)
        }))
    }

    /// One `Get` of `key` on `table` naming `VALUE_COLUMN`.
    async fn get(&mut self, table: &str, key: Option<v1::Key>) -> Result<GetResponse, Status> {
        self.get_naming(table, key, &[VALUE_COLUMN]).await
    }

    /// One `Get` of `key` on `table` naming `columns`, each read whole.
    async fn get_naming(
        &mut self,
        table: &str,
        key: Option<v1::Key>,
        columns: &[&str],
    ) -> Result<GetResponse, Status> {
        self.get_reading(table, key, wire_columns(columns)).await
    }

    /// One `Get` of `key` on `table` with the column reads `columns`.
    async fn get_reading(
        &mut self,
        table: &str,
        key: Option<v1::Key>,
        columns: Vec<v1::ColumnRead>,
    ) -> Result<GetResponse, Status> {
        self.client
            .get(GetRequest {
                table: table.to_string(),
                key,
                columns,
            })
            .await
            .map(tonic::Response::into_inner)
    }

    /// One `MultiGet` of `keys` on `table` naming `VALUE_COLUMN`.
    async fn multi_get(&mut self, table: &str, keys: &[Key]) -> Result<MultiGetResponse, Status> {
        self.multi_get_naming(table, keys, &[VALUE_COLUMN]).await
    }

    /// One `MultiGet` of `keys` on `table` naming `columns`, each read
    /// whole.
    async fn multi_get_naming(
        &mut self,
        table: &str,
        keys: &[Key],
        columns: &[&str],
    ) -> Result<MultiGetResponse, Status> {
        self.multi_get_reading(table, keys, wire_columns(columns))
            .await
    }

    /// One `MultiGet` of `keys` on `table` with the column reads `columns`.
    async fn multi_get_reading(
        &mut self,
        table: &str,
        keys: &[Key],
        columns: Vec<v1::ColumnRead>,
    ) -> Result<MultiGetResponse, Status> {
        self.client
            .multi_get(MultiGetRequest {
                table: table.to_string(),
                keys: keys.iter().map(wire_key).collect(),
                columns,
            })
            .await
            .map(tonic::Response::into_inner)
    }

    /// One `TableInfo` of `table`.
    async fn table_info(&mut self, table: &str) -> Result<TableInfoResponse, Status> {
        self.client
            .table_info(TableInfoRequest {
                table: table.to_string(),
            })
            .await
            .map(tonic::Response::into_inner)
    }

    /// The generation the first served table holds.
    fn generation(&self) -> Arc<Generation> {
        self.serving.opened().tables()[0]
            .current()
            .expect("the table was loaded")
    }

    /// `GET path` on the metrics listener.
    async fn http_get(&self, path: &str) -> Result<(StatusCode, String), BoxError> {
        http_get(self.serving.metrics_addr(), path).await
    }

    /// One scrape of `/metrics`, which answers 200.
    async fn scrape(&self) -> Result<String, BoxError> {
        let (status, exposition) = self.http_get(METRICS_PATH).await?;
        assert_eq!(status, StatusCode::OK);
        Ok(exposition)
    }
}

/// `columns` as a request carries them.
fn wire_columns(columns: &[&str]) -> Vec<v1::ColumnRead> {
    columns
        .iter()
        .map(|&column| v1::ColumnRead {
            name: column.to_string(),
            fields: Vec::new(),
        })
        .collect()
}

/// The bytes of the one value `values` carries, `None` for a null.
///
/// Panics on any number of values but one, or a value with no variant.
fn one_wire_value(values: &[v1::ColumnValue]) -> Option<Vec<u8>> {
    let [value] = values else {
        panic!("one value, got {values:?}");
    };
    wire_value(value)
}

/// The bytes `value` carries, `None` for a null.
///
/// Panics on a value with no variant.
fn wire_value(value: &v1::ColumnValue) -> Option<Vec<u8>> {
    match &value.kind {
        Some(v1::column_value::Kind::Value(bytes)) => Some(bytes.to_vec()),
        Some(v1::column_value::Kind::NullValue(_)) => None,
        None => panic!("a value with no variant"),
    }
}

/// `key` as the wire carries it.
fn wire_key(key: &Key) -> v1::Key {
    let variant = match key {
        Key::Long(value) => v1::key::Key::Long(*value),
        Key::Str(text) => v1::key::Key::Str(text.clone()),
        Key::Bytes(bytes) => v1::key::Key::Bin(bytes.clone()),
        Key::Uuid(bytes) => v1::key::Key::Bin(bytes.to_vec()),
    };
    v1::Key { key: Some(variant) }
}

/// The `requests_total` count of `call` answering `code` in `exposition`:
/// zero before the first such answer, when the series is not yet exposed.
fn calls(exposition: &str, call: Call, code: Code) -> f64 {
    series_value(
        exposition,
        metric::REQUESTS_TOTAL,
        &[
            (metric::LABEL_CALL, &call.to_string()),
            (metric::LABEL_CODE, &CodeLabel(code).to_string()),
        ],
    )
    .unwrap_or(0.0)
}

/// Asserts that every metric `exposition` types has a `# HELP` line, and
/// that it types some.
fn assert_every_metric_described(exposition: &str) {
    let named = |prefix: &str| -> Vec<String> {
        exposition
            .lines()
            .filter_map(|line| line.strip_prefix(prefix)?.split_whitespace().next())
            .map(str::to_string)
            .collect()
    };
    let typed = named("# TYPE ");
    let described = named("# HELP ");
    assert!(!typed.is_empty(), "{exposition}");
    for name in &typed {
        assert!(
            described.contains(name),
            "{name} has no HELP in {exposition}"
        );
    }
}

/// Whether `exposition` exposes the `requests_total` series of `call`
/// answering `code`.
fn calls_exposed(exposition: &str, call: Call, code: Code) -> bool {
    series_value(
        exposition,
        metric::REQUESTS_TOTAL,
        &[
            (metric::LABEL_CALL, &call.to_string()),
            (metric::LABEL_CODE, &CodeLabel(code).to_string()),
        ],
    )
    .is_some()
}

/// Asserts that `after` counts `expected` more `call`s answering `code`
/// than `before`. A counter's integer is exact in the float the exposition
/// carries it as, so the difference compares exactly.
fn assert_counted(before: &str, after: &str, call: Call, code: Code, expected: u64) {
    let added = calls(after, call, code) - calls(before, call, code);
    assert_eq!(
        added.to_bits(),
        approx::gauge(expected).to_bits(),
        "{call} {code:?}: {added} counted, {expected} expected"
    );
}

/// The value of the unlabeled series `name` in `exposition`, which must
/// expose it.
fn gauge(exposition: &str, name: &str) -> f64 {
    series_value(exposition, name, &[]).unwrap_or_else(|| panic!("{name} in {exposition}"))
}

/// Asserts that `exposition` reads `admitted` calls in flight of `limit`
/// slots.
fn assert_in_flight(exposition: &str, admitted: usize, limit: usize) {
    for (name, expected) in [
        (metric::REQUESTS_IN_FLIGHT, admitted),
        (metric::MAX_IN_FLIGHT, limit),
    ] {
        assert_eq!(
            gauge(exposition, name).to_bits(),
            approx::gauge_len(expected).to_bits(),
            "{name}"
        );
    }
}

/// `call`'s outcome beside how long it took.
async fn timed<T>(call: impl Future<Output = T>) -> (T, Duration) {
    let started = tokio::time::Instant::now();
    let outcome = call.await;
    (outcome, started.elapsed())
}

/// Asserts that a call answered `code` no sooner than `deadline` and within
/// `DEADLINE_MARGIN` of it.
fn assert_ended_at<T: std::fmt::Debug>(
    outcome: Result<T, Status>,
    elapsed: Duration,
    deadline: Duration,
    code: Code,
) {
    assert_eq!(code_of(outcome), code);
    assert!(
        elapsed >= deadline && elapsed < deadline + DEADLINE_MARGIN,
        "answered after {elapsed:?}, deadline {deadline:?}"
    );
}

/// The code of a failed call.
fn code_of<T: std::fmt::Debug>(result: Result<T, Status>) -> Code {
    result.expect_err("the call fails").code()
}

/// The local file of an object under the fixture server's root.
fn object_file(s3: &S3Fixture, location: &str) -> PathBuf {
    let under_bucket = location
        .strip_prefix(S3_SCHEME)
        .expect("fixture objects live under s3://");
    s3.root().join(under_bucket)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_health_is_not_serving_until_every_table_serves() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::start_with(
        vec![FixtureSpec::small(TABLE, KeyType::Long)],
        async |setup| {
            setup.hold_store = true;
            Ok(())
        },
    )
    .await?;
    assert_eq!(
        stack.health_status(OVERALL_SERVICE).await?,
        ServingStatus::NotServing,
        "the listener accepts before the load completes"
    );
    let (status, body) = stack.http_get(READY_PATH).await?;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body, NOT_READY_BODY);

    stack.s3.release();
    stack.serving.ready().await;
    assert_eq!(
        stack.health_status(OVERALL_SERVICE).await?,
        ServingStatus::Serving
    );
    assert_eq!(
        stack.health_status(SERVICE_NAME).await?,
        ServingStatus::Serving
    );
    let (status, body) = stack.http_get(READY_PATH).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, READY_BODY);
    let (status, _) = stack.http_get("/elsewhere").await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_info_describes_the_served_snapshot() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::ready_small().await?;
    let generation = stack.generation();
    let info = stack.table_info(TABLE).await?;
    assert_eq!(info.table, TABLE);
    assert_eq!(info.buckets, SMALL_BUCKETS);
    assert_eq!(info.data_files, u64::from(SMALL_BUCKETS));
    assert_eq!(info.records, SMALL_IDS.end - SMALL_IDS.start);
    assert_eq!(info.key_type, i32::from(v1::KeyType::Long));
    assert_eq!(info.snapshot_id, generation.snapshot_id.0);
    assert_eq!(info.index_bytes, approx::count(generation.index_bytes));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_costs_one_get_cold_and_none_warm_or_absent() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::ready_small().await?;
    let generation = stack.generation();
    let keys = stack.fixtures[0].keys_at(&SAMPLE[..COLD_IDS]);
    let expected = expected_cold_gets(&generation, &keys, &[VALUE_COLUMN])?;
    assert_eq!(expected, approx::count(COLD_IDS), "the ids share no page");

    let before = stack.stats.gets();
    for key in &keys {
        let response = stack.get(TABLE, Some(wire_key(key))).await?;
        assert_eq!(response.presence, i32::from(v1::Presence::Present), "{key}");
        assert_eq!(
            one_wire_value(&response.values),
            stack.fixtures[0].value(key),
            "{key}"
        );
        assert_eq!(response.snapshot_id, generation.snapshot_id.0);
    }
    assert_eq!(
        stack.stats.gets() - before,
        expected,
        "one GET per cold hit"
    );

    let before = stack.stats.gets();
    for key in &keys {
        let response = stack.get(TABLE, Some(wire_key(key))).await?;
        assert_eq!(response.presence, i32::from(v1::Presence::Present));
    }
    assert_eq!(stack.stats.gets(), before, "a warm hit costs nothing");

    let absent = stack.fixtures[0].absent_near(SAMPLE[0]);
    let before = stack.stats.gets();
    let response = stack.get(TABLE, Some(wire_key(&absent))).await?;
    assert_eq!(response.presence, i32::from(v1::Presence::Absent));
    assert!(response.values.is_empty());
    assert_eq!(response.snapshot_id, generation.snapshot_id.0);
    assert_eq!(stack.stats.gets(), before, "a miss costs nothing");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_serves_a_null_value_as_present_with_null() -> TestResult {
    let _serial = SERIAL.lock().await;
    let spec = FixtureSpec {
        nulls: Nulls::EveryNth(NULL_EVERY),
        ..FixtureSpec::small(TABLE, KeyType::Long)
    };
    let mut stack = Stack::start(vec![spec]).await?;
    stack.serving.ready().await;
    let null_key = stack.fixtures[0].key(NULL_ID);
    assert_eq!(stack.fixtures[0].value(&null_key), None);
    let present_key = stack.fixtures[0].key(PRESENT_ID);
    assert!(stack.fixtures[0].value(&present_key).is_some());

    let before = stack.stats.gets();
    let null = stack.get(TABLE, Some(wire_key(&null_key))).await?;
    assert_eq!(null.presence, i32::from(v1::Presence::Present));
    assert_eq!(one_wire_value(&null.values), None, "the value is null");
    assert_eq!(stack.stats.gets() - before, 1, "a null costs its page cold");

    let present = stack.get(TABLE, Some(wire_key(&present_key))).await?;
    assert_eq!(present.presence, i32::from(v1::Presence::Present));
    assert_eq!(
        one_wire_value(&present.values),
        stack.fixtures[0].value(&present_key)
    );

    let absent = stack
        .get(
            TABLE,
            Some(wire_key(&stack.fixtures[0].absent_near(NULL_ID))),
        )
        .await?;
    assert_eq!(absent.presence, i32::from(v1::Presence::Absent));
    assert!(absent.values.is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_get_answers_in_order_from_one_snapshot() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::ready_small().await?;
    let generation = stack.generation();
    let present = stack.fixtures[0].keys_at(&SAMPLE[..BATCH_IDS]);
    let absent = stack.fixtures[0].absent_near(SAMPLE[0]);
    let mut keys = present.clone();
    keys.push(absent);
    let expected = expected_cold_gets(&generation, &present, &[VALUE_COLUMN])?;

    let before = stack.stats.gets();
    let response = stack.multi_get(TABLE, &keys).await?;
    assert_eq!(stack.stats.gets() - before, expected);
    assert_eq!(response.snapshot_id, generation.snapshot_id.0);
    assert_eq!(response.results.len(), keys.len());
    for (key, hit) in present.iter().zip(&response.results) {
        assert_eq!(hit.presence, i32::from(v1::Presence::Present), "{key}");
        assert_eq!(
            one_wire_value(&hit.values),
            stack.fixtures[0].value(key),
            "{key}"
        );
    }
    let last = &response.results[keys.len() - 1];
    assert_eq!(last.presence, i32::from(v1::Presence::Absent));
    assert!(last.values.is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_maps_a_missing_or_mismatched_key_to_invalid_argument() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::start(vec![
        FixtureSpec::small(TABLE, KeyType::Long),
        FixtureSpec::small(UUID_TABLE, KeyType::Uuid),
    ])
    .await?;
    stack.serving.ready().await;

    let scraped = stack.scrape().await?;
    let before = stack.stats.gets();
    assert_eq!(code_of(stack.get(TABLE, None).await), Code::InvalidArgument);
    assert_eq!(
        code_of(stack.get(TABLE, Some(v1::Key { key: None })).await),
        Code::InvalidArgument
    );
    let str_key = v1::Key {
        key: Some(v1::key::Key::Str("nope".to_string())),
    };
    assert_eq!(
        code_of(stack.get(TABLE, Some(str_key)).await),
        Code::InvalidArgument
    );
    let short = v1::Key {
        key: Some(v1::key::Key::Bin(vec![0u8; UUID_BYTES - 1])),
    };
    assert_eq!(
        code_of(stack.get(UUID_TABLE, Some(short)).await),
        Code::InvalidArgument
    );
    assert_eq!(stack.stats.gets(), before, "a refused key issues no GET");
    let refused_keys = 4;
    assert_counted(
        &scraped,
        &stack.scrape().await?,
        Call::Get,
        Code::InvalidArgument,
        refused_keys,
    );

    let uuid_key = stack.fixtures[1].key(SAMPLE[1]);
    let Key::Uuid(bytes) = &uuid_key else {
        panic!("{uuid_key} is a uuid");
    };
    assert_eq!(bytes.len(), UUID_BYTES);
    let response = stack.get(UUID_TABLE, Some(wire_key(&uuid_key))).await?;
    assert_eq!(response.presence, i32::from(v1::Presence::Present));
    assert_eq!(
        one_wire_value(&response.values),
        stack.fixtures[1].value(&uuid_key)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_unknown_table_is_not_found() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::ready_small().await?;
    let key = stack.fixtures[0].key(SAMPLE[0]);
    let before = stack.scrape().await?;
    assert_eq!(code_of(stack.table_info(NOBODY).await), Code::NotFound);
    assert_eq!(
        code_of(stack.get(NOBODY, Some(wire_key(&key))).await),
        Code::NotFound
    );
    assert_eq!(
        code_of(stack.multi_get(NOBODY, std::slice::from_ref(&key)).await),
        Code::NotFound
    );
    let after = stack.scrape().await?;
    for call in [Call::Get, Call::MultiGet, Call::TableInfo] {
        assert_counted(&before, &after, call, Code::NotFound, 1);
        assert_counted(&before, &after, call, Code::Ok, 0);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_storage_failure_is_unavailable_and_fails_a_batch_whole() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::ready_small().await?;
    let generation = stack.generation();
    let sample = stack.fixtures[0].keys_at(&SAMPLE);
    let plan = generation.plan(&sample);
    let (doomed, _) = plan[0].as_ref().expect("a fixture key has a value page");
    let doomed_path = doomed.entry.path.clone();
    let doomed_key = sample[0].clone();
    let others: Vec<Key> = sample
        .iter()
        .zip(&plan)
        .filter_map(|(key, planned)| {
            let (file, _) = planned.as_ref()?;
            (file.entry.path != doomed_path).then(|| key.clone())
        })
        .take(BATCH_IDS)
        .collect();
    assert_eq!(others.len(), BATCH_IDS, "the sample spans several files");
    std::fs::remove_file(object_file(&stack.s3, &doomed_path))?;

    let before = stack.scrape().await?;
    assert_eq!(
        code_of(stack.get(TABLE, Some(wire_key(&doomed_key))).await),
        Code::Unavailable
    );
    let mut mixed = others.clone();
    mixed.push(doomed_key);
    assert_eq!(
        code_of(stack.multi_get(TABLE, &mixed).await),
        Code::Unavailable,
        "one failed read fails the batch"
    );
    let after = stack.scrape().await?;
    assert_counted(&before, &after, Call::Get, Code::Unavailable, 1);
    assert_counted(&before, &after, Call::MultiGet, Code::Unavailable, 1);
    let answered = stack.multi_get(TABLE, &others).await?;
    assert_eq!(answered.results.len(), others.len());
    for (key, hit) in others.iter().zip(&answered.results) {
        assert_eq!(hit.presence, i32::from(v1::Presence::Present), "{key}");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_requests_total_counts_every_call() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::ready_small().await?;
    let before = stack.scrape().await?;

    let key = stack.fixtures[0].key(SAMPLE[0]);
    for _ in 0..COUNTED_GETS {
        stack.get(TABLE, Some(wire_key(&key))).await?;
    }
    stack.multi_get(TABLE, std::slice::from_ref(&key)).await?;
    stack.table_info(TABLE).await?;

    let after = stack.scrape().await?;
    assert_every_metric_described(&after);
    assert_counted(&before, &after, Call::Get, Code::Ok, COUNTED_GETS);
    assert_counted(&before, &after, Call::MultiGet, Code::Ok, 1);
    assert_counted(&before, &after, Call::TableInfo, Code::Ok, 1);
    let lookups = |exposition: &str| {
        series_sum(
            exposition,
            metric::LOOKUPS_TOTAL,
            &[(metric::LABEL_TABLE, TABLE)],
        )
    };
    let looked_up = lookups(&after) - lookups(&before);
    assert_eq!(
        looked_up.to_bits(),
        approx::gauge(COUNTED_GETS + 1).to_bits(),
        "every lookup answered before the scrape is in it: {looked_up}"
    );
    // On the Full tier each lookup takes its value page from the page
    // cache once, a hit or a fetch.
    let pages_served = |exposition: &str| {
        series_sum(exposition, metric::PAGE_CACHE_HITS_TOTAL, &[])
            + series_sum(exposition, metric::PAGE_CACHE_FETCHES_TOTAL, &[])
    };
    let served = pages_served(&after) - pages_served(&before);
    assert_eq!(
        served.to_bits(),
        approx::gauge(COUNTED_GETS + 1).to_bits(),
        "every page hit before the scrape is in it: {served}"
    );
    let rss = series_value(&after, metric::PROCESS_RSS_BYTES, &[])
        .expect("the scrape samples the resident set size");
    assert!(rss > 0.0, "a running process holds memory: {rss}");
    let age = series_value(
        &after,
        metric::GENERATION_AGE_SECONDS,
        &[(metric::LABEL_TABLE, TABLE)],
    )
    .expect("the serving generation's age is sampled");
    assert!(
        age >= 0.0,
        "the snapshot was committed before the scrape: {age}"
    );
    Ok(())
}

/// The scrape samples each table's oldest applying delete's age beside its
/// generation's: 0 on a table with no deletes; on one whose deletes were
/// committed in two snapshots, the later served, above 0 and no less than
/// the generation's age, since the oldest delete's snapshot is no newer
/// than the one served.
#[tokio::test(flavor = "multi_thread")]
async fn test_scrape_samples_the_oldest_delete_age_beside_the_generation_age() -> TestResult {
    /// The ids deleted, the first in one snapshot, the second in the next.
    const DELETED_IDS: [u64; 2] = [SAMPLE[0], SAMPLE[1]];
    let _serial = SERIAL.lock().await;
    let specs = vec![
        FixtureSpec::small(TABLE, KeyType::Long),
        FixtureSpec::small(DELETED_TABLE, KeyType::Long),
    ];
    let mut stack = Stack::start_with(specs, async |setup| {
        let fixture = &mut setup.fixtures[1];
        fixture
            .delete(&DELETED_IDS[..1], DeleteForm::ScopedPositions, "first")
            .await?;
        fixture
            .delete(&DELETED_IDS[1..], DeleteForm::ScopedPositions, "second")
            .await?;
        setup.config.tables[1] = fixture.config(Residency::Full, false);
        Ok(())
    })
    .await?;
    stack.serving.ready().await;
    let scrape = stack.scrape().await?;
    let gauge = |name: &str, table: &str| {
        series_value(&scrape, name, &[(metric::LABEL_TABLE, table)])
            .unwrap_or_else(|| panic!("{name} of {table} is sampled"))
    };
    assert_eq!(
        gauge(metric::TABLE_OLDEST_DELETE_AGE_SECONDS, TABLE).to_bits(),
        approx::gauge(0).to_bits(),
        "no delete applies"
    );
    let oldest = gauge(metric::TABLE_OLDEST_DELETE_AGE_SECONDS, DELETED_TABLE);
    let generation = gauge(metric::GENERATION_AGE_SECONDS, DELETED_TABLE);
    assert!(
        oldest > 0.0,
        "the deletes were committed before the scrape: {oldest}"
    );
    assert!(
        oldest >= generation,
        "the oldest delete is no newer than the served snapshot: {oldest} < {generation}"
    );
    assert_eq!(
        gauge(metric::TABLE_DELETED_ROWS, DELETED_TABLE).to_bits(),
        approx::gauge_len(DELETED_IDS.len()).to_bits()
    );
    Ok(())
}

/// A request body that is no gRPC message: its first byte, the
/// compression flag, is neither 0 nor 1, so tonic answers `INTERNAL`
/// before any handler runs, as `curl --http2-prior-knowledge` sending it
/// is answered.
const MALFORMED_BODY: &[u8] = b"\xff\xff\xff";

/// A request tonic answers before its handler runs, a body that is no gRPC
/// message, is counted under the status the client received, and a valid
/// request beside it under `OK`: each exactly once.
#[tokio::test(flavor = "multi_thread")]
async fn test_requests_total_counts_a_request_answered_before_its_handler() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::ready_small().await?;
    let before = stack.scrape().await?;

    let mut channel = Channel::from_shared(format!("http://{}", stack.serving.listen_addr()))?
        .connect()
        .await?;
    let malformed = hyper::Request::builder()
        .method(hyper::Method::POST)
        .uri(format!("/{SERVICE_NAME}/{}", Call::Get.method()))
        .header(hyper::header::CONTENT_TYPE, "application/grpc")
        .header(hyper::header::TE, "trailers")
        .body(tonic::body::Body::new(http_body_util::Full::new(
            bytes::Bytes::from_static(MALFORMED_BODY),
        )))?;
    std::future::poll_fn(|cx| tower::Service::poll_ready(&mut channel, cx)).await?;
    let response = tower::Service::call(&mut channel, malformed).await?;
    let answered = response
        .headers()
        .get("grpc-status")
        .map(|code| Code::from_bytes(code.as_bytes()));
    assert_eq!(answered, Some(Code::Internal), "{:?}", response.headers());

    let key = stack.fixtures[0].key(SAMPLE[0]);
    stack.get(TABLE, Some(wire_key(&key))).await?;

    let after = stack.scrape().await?;
    assert_counted(&before, &after, Call::Get, Code::Internal, 1);
    assert_counted(&before, &after, Call::Get, Code::Ok, 1);
    let total = |exposition: &str| series_sum(exposition, metric::REQUESTS_TOTAL, &[]);
    assert_eq!(
        (total(&after) - total(&before)).to_bits(),
        2.0_f64.to_bits(),
        "nothing else is counted"
    );
    Ok(())
}

/// Every series an alert keys on is exposed once the server serves, before
/// any call, swap, or reload: a new table's counters at zero, each call's
/// `OK` count, each reload result, the page cache's counters and bytes,
/// nothing in flight of the default slots, nothing pending, and the table
/// neither degraded, held, nor below its level, at the optimal level it
/// declares (RFD 020). The process-wide counters other tests also count
/// are checked for presence.
#[tokio::test(flavor = "multi_thread")]
async fn test_alerting_series_are_exposed_from_the_start() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::start_with(
        vec![FixtureSpec::small(FRESH_TABLE, KeyType::Long)],
        async |setup| {
            setup.options.reload = Some(Versions::new(setup.config.clone()).reload());
            Ok(())
        },
    )
    .await?;
    stack.serving.ready().await;
    let scrape = stack.scrape().await?;

    let table = (metric::LABEL_TABLE, FRESH_TABLE);
    for result in LookupResult::ALL {
        let result = result.to_string();
        let labels = [table, (metric::LABEL_RESULT, result.as_str())];
        assert_eq!(
            series_value(&scrape, metric::LOOKUPS_TOTAL, &labels),
            Some(0.0),
            "{result}"
        );
    }
    assert_eq!(
        series_value(&scrape, metric::GENERATION_SWAPS_TOTAL, &[table]),
        Some(0.0)
    );
    // The state label's values are spelled out: `StateName` is private to
    // icetray-core.
    for state in ["degraded", "held"] {
        assert_eq!(
            series_value(
                &scrape,
                metric::TABLE_STATE,
                &[table, (metric::LABEL_STATE, state)]
            ),
            Some(0.0),
            "{state}"
        );
    }
    for name in [
        metric::TABLE_BELOW_LEVEL,
        metric::TABLE_LEVEL,
        metric::TABLE_DECLARED_LEVEL,
    ] {
        assert_eq!(series_value(&scrape, name, &[table]), Some(0.0), "{name}");
    }
    for call in Call::ALL {
        assert!(calls_exposed(&scrape, call, Code::Ok), "{call} in {scrape}");
    }
    for result in ReloadResult::ALL {
        let result = result.to_string();
        let labels = [(metric::LABEL_RESULT, result.as_str())];
        assert!(
            series_value(&scrape, metric::CONFIG_RELOADS_TOTAL, &labels).is_some(),
            "{result} in {scrape}"
        );
    }
    for name in [
        metric::PAGE_CACHE_FETCHES_TOTAL,
        metric::PAGE_CACHE_HITS_TOTAL,
        metric::PAGE_CACHE_BYTES,
    ] {
        gauge(&scrape, name);
    }
    assert_in_flight(&scrape, 0, ServerConfig::default().max_in_flight);
    assert_eq!(
        series_value(&scrape, metric::CONFIG_PENDING, &[]),
        Some(0.0)
    );
    Ok(())
}

/// Two scrapes with nothing between them expose the same gauges: every
/// series of a described gauge the first exposed, the second exposes too.
/// The recorder drops a gauge no scrape wrote since the scrape before, so a
/// gauge set once on an event instead of on the scrape path fails here. The
/// server serves a disk table whose page cache it has sampled and follows
/// its configuration, so the first scrape holds every described gauge, and
/// the test checks that it does: a gauge this server does not expose would
/// pass the comparison unheld.
#[tokio::test(flavor = "multi_thread")]
async fn test_scrape_keeps_every_gauge_the_scrape_before_exposed() -> TestResult {
    let mut stack = Stack::start_with(
        vec![FixtureSpec::small(SCRAPED_TABLE, KeyType::Long)],
        async |setup| {
            setup.with_local_dir()?;
            setup.options.reload = Some(Versions::new(setup.config.clone()).reload());
            Ok(())
        },
    )
    .await?;
    stack.serving.ready().await;
    stack.serving.opened().sample_page_cache().await;
    let first = stack.scrape().await?;
    let second = stack.scrape().await?;

    let first_series = gauge_series(&first, SCRAPED_TABLE);
    for (name, kind, _) in metric::DESCRIPTIONS {
        if kind == Kind::Gauge {
            assert!(
                first_series.iter().any(|series| series.name == name),
                "{name} in {first}"
            );
        }
    }
    let second_series = gauge_series(&second, SCRAPED_TABLE);
    let dropped: Vec<_> = first_series.difference(&second_series).collect();
    assert!(
        dropped.is_empty(),
        "gauges the second scrape dropped: {dropped:?}"
    );
    Ok(())
}

/// The daemon samples a disk table's chunk files as soon as the tables
/// serve, with no call or scrape asking: the resident fraction is exposed
/// without waiting out `PAGE_CACHE_SAMPLE_INTERVAL`, and a table that has
/// just written its chunk files has them whole in the page cache, at its
/// layout's level.
#[tokio::test(flavor = "multi_thread")]
async fn test_page_cache_is_sampled_as_the_tables_serve() -> TestResult {
    let mut stack = Stack::start_with(
        vec![FixtureSpec::small(SAMPLED_TABLE, KeyType::Long)],
        async |setup| setup.with_local_dir(),
    )
    .await?;
    stack.serving.ready().await;
    let labels = [(metric::LABEL_TABLE, SAMPLED_TABLE)];
    let fraction = poll_until(
        FIRST_SAMPLE_WAIT,
        "the first page cache sample",
        async || {
            let exposition = stack.scrape().await?;
            Ok(series_value(
                &exposition,
                metric::TABLE_CHUNK_RESIDENT_FRACTION,
                &labels,
            ))
        },
    )
    .await?;
    assert_eq!(fraction.to_bits(), 1.0_f64.to_bits());
    let exposition = stack.scrape().await?;
    assert_eq!(
        series_value(&exposition, metric::TABLE_BELOW_LEVEL, &labels).map(f64::to_bits),
        Some(0.0_f64.to_bits())
    );
    Ok(())
}

/// One sample line's series: its metric name and its label text.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Series<'a> {
    name: &'a str,
    labels: &'a str,
}

/// The series of every gauge `metric::DESCRIPTIONS` names in `exposition`,
/// leaving out those labeled with a table other than `table`, which belong
/// to another test's server.
fn gauge_series<'a>(exposition: &'a str, table: &str) -> BTreeSet<Series<'a>> {
    let gauges: Vec<&str> = metric::DESCRIPTIONS
        .iter()
        .filter(|(_, kind, _)| *kind == Kind::Gauge)
        .map(|(name, ..)| *name)
        .collect();
    exposition
        .lines()
        .filter(|line| !line.starts_with('#'))
        .filter_map(|line| {
            let (series, _value) = line.rsplit_once(' ')?;
            let (name, labels) = series.split_once('{').map_or((series, ""), |(name, rest)| {
                (name, rest.trim_end_matches('}'))
            });
            Some(Series { name, labels })
        })
        .filter(|series| gauges.contains(&series.name))
        .filter(|series| {
            series.labels.split(',').all(|pair| {
                pair.split_once('=').is_none_or(|(label, quoted)| {
                    label != metric::LABEL_TABLE || quoted.trim_matches('"') == table
                })
            })
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_follows_the_table_within_the_poll_interval() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::start_with(
        vec![FixtureSpec::small(TABLE, KeyType::Long)],
        async |setup| {
            // Followed from the table root through its version hint, so an
            // append after the start is what the poll finds.
            setup.fixtures[0].write_hint().await?;
            setup.config.tables[0].source = setup.fixtures[0].root_source();
            Ok(())
        },
    )
    .await?;
    stack.serving.ready().await;
    let first = stack.table_info(TABLE).await?.snapshot_id;

    stack.fixtures[0]
        .append(stack.catalog.as_ref(), NEW_IDS, APPEND_PREFIX)
        .await?;
    stack.fixtures[0].write_hint().await?;
    let advanced = poll_until(REFRESH_WAIT, "the table advancing", async || {
        let info = stack.table_info(TABLE).await?;
        Ok((info.snapshot_id != first).then_some(info))
    })
    .await?;
    assert_eq!(
        advanced.snapshot_id,
        stack.fixtures[0]
            .metadata()
            .current_snapshot_id()
            .expect("the append committed a snapshot")
    );

    let new_key = stack.fixtures[0].key(NEW_IDS.start);
    let response = stack.get(TABLE, Some(wire_key(&new_key))).await?;
    assert_eq!(response.presence, i32::from(v1::Presence::Present));
    assert_eq!(
        one_wire_value(&response.values),
        stack.fixtures[0].value(&new_key)
    );
    assert_eq!(response.snapshot_id, advanced.snapshot_id);

    let (status, scrape) = stack.http_get(METRICS_PATH).await?;
    assert_eq!(status, StatusCode::OK);
    let sequence_number = stack.fixtures[0]
        .metadata()
        .current_snapshot()
        .expect("the append committed a snapshot")
        .sequence_number();
    assert_eq!(
        series_value(
            &scrape,
            metric::GENERATION_SEQUENCE_NUMBER,
            &[(metric::LABEL_TABLE, TABLE)]
        )
        .map(f64::to_bits),
        Some(approx::signed_gauge(sequence_number).to_bits()),
        "the served snapshot's sequence number"
    );
    let swapped_at = series_value(
        &scrape,
        metric::GENERATION_SWAPPED_AT_SECONDS,
        &[(metric::LABEL_TABLE, TABLE)],
    )
    .expect("a swap sets the swap time");
    assert!(swapped_at > 0.0, "{swapped_at}");
    assert!(labeled_with(&scrape, metric::LABEL_TABLE) > 0);
    assert_eq!(
        labeled_with(&scrape, SNAPSHOT_ID_LABEL),
        0,
        "a series per snapshot would grow with every swap"
    );
    Ok(())
}

/// The label no metric carries: the snapshot id is in the logs and the
/// table-info reply, and a series per snapshot would grow with every swap.
const SNAPSHOT_ID_LABEL: &str = "snapshot_id";

/// The sample lines of a Prometheus text exposition whose labels include
/// `label`.
fn labeled_with(exposition: &str, label: &str) -> usize {
    exposition
        .lines()
        .filter(|line| !line.starts_with('#'))
        .filter_map(|line| line.split_once('{')?.1.split_once('}'))
        .filter(|(labels, _)| {
            labels
                .split(',')
                .any(|pair| pair.split_once('=').is_some_and(|(name, _)| name == label))
        })
        .count()
}

/// A `Get` in flight when `shutdown` is asked for is answered, and the
/// drain ends `Ok`. The store is held until the call has reached it, so the
/// call is open on the server when the drain starts; a call still open when
/// the drain deadline passes is `test_drain_stops_waiting_at_its_deadline`.
#[tokio::test(flavor = "multi_thread")]
async fn test_shutdown_drains_and_returns_ok() -> TestResult {
    let _serial = SERIAL.lock().await;
    let stack = Stack::ready_small().await?;
    let key = stack.fixtures[0].key(SAMPLE[0]);
    stack.s3.hold();
    let mut client = stack.client.clone();
    let in_flight = tokio::spawn(async move {
        client
            .get(GetRequest {
                table: TABLE.to_string(),
                key: Some(wire_key(&key)),
                columns: wire_columns(&[VALUE_COLUMN]),
            })
            .await
    });
    poll_until(SHUTDOWN_WAIT, "the call reaching the store", async || {
        Ok((stack.s3.held() == 1).then_some(()))
    })
    .await?;
    stack.serving.shutdown();
    stack.s3.release();
    let response = in_flight.await??.into_inner();
    assert_eq!(response.presence, i32::from(v1::Presence::Present));
    tokio::time::timeout(SHUTDOWN_WAIT, stack.serving.join()).await??;
    Ok(())
}

/// A client watching health holds a stream open, and tonic's graceful
/// shutdown waits for every connection to close; the drain sends
/// `NOT_SERVING` on both health names and ends their watches, so the run
/// returns.
#[tokio::test(flavor = "multi_thread")]
async fn test_shutdown_ends_health_watches_so_the_drain_finishes() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::ready_small().await?;
    let watches = [
        stack.watch_health(OVERALL_SERVICE).await?,
        stack.watch_health(SERVICE_NAME).await?,
    ];
    stack.serving.shutdown();
    tokio::time::timeout(SHUTDOWN_WAIT, stack.serving.join()).await??;
    for watch in watches {
        let statuses = tokio::time::timeout(SHUTDOWN_WAIT, watch).await???;
        assert_eq!(statuses.first(), Some(&ServingStatus::Serving));
        assert_eq!(statuses.last(), Some(&ServingStatus::NotServing));
    }
    Ok(())
}

/// A failed start ends the run on its own once the grace is over, and the
/// watches opened during the grace end with it rather than hold the drain.
#[tokio::test(flavor = "multi_thread")]
async fn test_failed_start_ends_health_watches_so_the_run_ends() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::start_with(
        vec![FixtureSpec::small(TABLE, KeyType::Long)],
        async |setup| {
            setup.config.tables[0].residency = Residency::Disk;
            setup.options.start_failure_grace = REFRESH_WAIT;
            Ok(())
        },
    )
    .await?;
    let watches = [
        stack.watch_health(OVERALL_SERVICE).await?,
        stack.watch_health(SERVICE_NAME).await?,
    ];
    let outcome = tokio::time::timeout(SHUTDOWN_WAIT, stack.serving.join()).await?;
    assert!(
        matches!(outcome, Err(ServeError::FirstLoad { .. })),
        "expected a first-load failure, got {outcome:?}"
    );
    for watch in watches {
        let statuses = tokio::time::timeout(SHUTDOWN_WAIT, watch).await???;
        assert!(
            statuses
                .iter()
                .all(|status| *status == ServingStatus::NotServing),
            "{statuses:?}"
        );
        assert!(!statuses.is_empty());
    }
    Ok(())
}

/// A first load that fails leaves the run up through the grace with the
/// failure on the health service, then ends it with `FirstLoad`. The check
/// starts once the table is `Degraded`, which the load sets in the poll
/// that ends the loads and starts the grace, and holds that the run stays
/// up for `STAYS_UP_FOR`: an interval, since "stays up" is the claim, and
/// short, a fifth of the grace, so the rest is room for the poll's delay.
#[tokio::test(flavor = "multi_thread")]
async fn test_first_load_failure_stays_visible_then_fails() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::start_with(
        vec![FixtureSpec::small(TABLE, KeyType::Long)],
        async |setup| {
            // Disk residency with no `storage.local`: the configuration is
            // built here rather than parsed, so the violation `Config::parse`
            // would refuse is met at the first load.
            setup.config.tables[0].residency = Residency::Disk;
            setup.options.start_failure_grace = FAILURE_GRACE;
            Ok(())
        },
    )
    .await?;
    let table = Arc::clone(&stack.serving.opened().tables()[0]);
    poll_until(REFRESH_WAIT, "the first load failing", async || {
        Ok(matches!(table.state(), TableState::Degraded { .. }).then_some(()))
    })
    .await?;

    assert!(
        tokio::time::timeout(STAYS_UP_FOR, stack.serving.stopped())
            .await
            .is_err(),
        "the run stays up through the grace"
    );
    assert_eq!(
        stack.health_status(OVERALL_SERVICE).await?,
        ServingStatus::NotServing,
        "the failure stays visible through the grace"
    );
    assert_eq!(
        stack.health_status(SERVICE_NAME).await?,
        ServingStatus::NotServing
    );
    stack.serving.ready().await;
    let outcome = tokio::time::timeout(SHUTDOWN_WAIT, stack.serving.join()).await?;
    let Err(ServeError::FirstLoad { failures }) = outcome else {
        panic!("expected a first-load failure, got {outcome:?}");
    };
    assert_eq!(failures, vec![(TABLE.to_string(), ErrorKind::Config)]);
    Ok(())
}

/// A periodic task that panics ends the run with the task named, so the
/// process exits non-zero instead of answering `/ready` 200 from tables
/// nothing refreshes. A poll interval of zero is what `Config::parse`
/// refuses as `PollIntervalTooShort`, and what tokio's interval refuses with a
/// panic at the first tick of the refresh and upkeep tasks; the
/// configuration is built directly, so the panic is reached. Both tasks
/// panic at once, and whichever the run joins first is the one named.
#[tokio::test(flavor = "multi_thread")]
async fn test_periodic_task_panic_ends_the_run_naming_the_task() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::start_with(
        vec![FixtureSpec::small(TABLE, KeyType::Long)],
        async |setup| {
            setup.config.server.poll_interval = Duration::ZERO;
            Ok(())
        },
    )
    .await?;
    stack.serving.ready().await;
    let outcome = tokio::time::timeout(SHUTDOWN_WAIT, stack.serving.join()).await?;
    let Err(ServeError::Periodic { task, source }) = outcome else {
        panic!("expected a periodic task failure, got {outcome:?}");
    };
    assert!(source.is_panic(), "{source}");
    match task {
        PeriodicTask::Refresh { table } => assert_eq!(table, TABLE),
        PeriodicTask::Upkeep => {}
        PeriodicTask::PageCacheSample => {
            panic!("the page cache sample runs on its own interval, not the poll interval")
        }
    }
    Ok(())
}

/// A shutdown asked for once the first loads have failed cuts the grace
/// short: a failed start whose grace is `SHUTDOWN_WAIT` long reports its
/// failure well inside the grace instead of sleeping it out. The shutdown
/// waits for the table to be `Degraded`, which the load sets on the run's
/// task in the poll that ends the loads, so the loads' outcome is in hand
/// before the shutdown is asked for.
#[tokio::test(flavor = "multi_thread")]
async fn test_shutdown_during_the_grace_ends_the_run_without_waiting() -> TestResult {
    let _serial = SERIAL.lock().await;
    let stack = Stack::start_with(
        vec![FixtureSpec::small(TABLE, KeyType::Long)],
        async |setup| {
            setup.config.tables[0].residency = Residency::Disk;
            setup.options.start_failure_grace = SHUTDOWN_WAIT;
            Ok(())
        },
    )
    .await?;
    let table = Arc::clone(&stack.serving.opened().tables()[0]);
    poll_until(REFRESH_WAIT, "the first load failing", async || {
        Ok(matches!(table.state(), TableState::Degraded { .. }).then_some(()))
    })
    .await?;
    stack.serving.shutdown();
    let outcome = tokio::time::timeout(REFRESH_WAIT, stack.serving.join()).await?;
    let Err(ServeError::FirstLoad { failures }) = outcome else {
        panic!("expected a first-load failure, got {outcome:?}");
    };
    assert_eq!(failures.len(), 1);
    Ok(())
}

/// A shutdown asked for while a first load waits on a store that has
/// stopped answering ends the load and the run at once, `Ok`, instead of
/// waiting for the load to reach its refresh deadline.
#[tokio::test(flavor = "multi_thread")]
async fn test_shutdown_during_the_first_loads_ends_them() -> TestResult {
    let _serial = SERIAL.lock().await;
    assert!(ServerConfig::default().refresh_deadline > SHUTDOWN_WAIT);
    let stack = Stack::start_with(
        vec![FixtureSpec::small(TABLE, KeyType::Long)],
        async |setup| {
            setup.hold_store = true;
            Ok(())
        },
    )
    .await?;
    poll_until(
        SHUTDOWN_WAIT,
        "the first load reaching the store",
        async || Ok((stack.s3.held() > 0).then_some(())),
    )
    .await?;

    stack.serving.shutdown();
    let (outcome, elapsed) =
        tokio::time::timeout(SHUTDOWN_WAIT, timed(stack.serving.join())).await?;
    outcome?;
    assert!(
        elapsed < FIRST_LOAD_SHUTDOWN_WITHIN,
        "the run ended {elapsed:?} after the shutdown"
    );
    Ok(())
}

/// Over a store that has stopped answering, a `Get` ends at the configured
/// `get_deadline` and a `MultiGet` at a shorter `multi_get_deadline`, each
/// with `DEADLINE_EXCEEDED`, instead of waiting on the store.
#[tokio::test(flavor = "multi_thread")]
async fn test_get_and_multi_get_end_at_their_deadlines_over_a_held_store() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::start_with(
        vec![FixtureSpec::small(TABLE, KeyType::Long)],
        async |setup| {
            setup.config.server.get_deadline = LONGER_DEADLINE;
            setup.config.server.multi_get_deadline = SHORT_DEADLINE;
            Ok(())
        },
    )
    .await?;
    stack.serving.ready().await;
    let keys = stack.fixtures[0].keys_at(&SAMPLE[..BATCH_IDS]);
    stack.s3.hold();

    let (outcome, elapsed) = tokio::time::timeout(
        SHUTDOWN_WAIT,
        timed(stack.get(TABLE, Some(wire_key(&keys[0])))),
    )
    .await?;
    assert_ended_at(outcome, elapsed, LONGER_DEADLINE, Code::DeadlineExceeded);

    let (outcome, elapsed) =
        tokio::time::timeout(SHUTDOWN_WAIT, timed(stack.multi_get(TABLE, &keys[1..]))).await?;
    assert_ended_at(outcome, elapsed, SHORT_DEADLINE, Code::DeadlineExceeded);
    assert!(stack.s3.held() > 0, "the calls reached the held store");
    Ok(())
}

/// A `Get` past its deadline takes its page fetch with it: over a held
/// store the call ends `DEADLINE_EXCEEDED` at `get_deadline`, and the GET
/// behind it is given up within `DEADLINE_MARGIN` of that instead of
/// waiting on the store under the S3 client's own bounds. A `MultiGet`'s
/// page fetches have the same deadline, so under a longer
/// `multi_get_deadline` the batch ends at `get_deadline` too. Nothing from
/// an abandoned fetch is cached: once the store answers again, the same key
/// runs a fetch of its own.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_get_past_its_deadline_ends_its_page_fetch() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::start_with(
        vec![FixtureSpec::small(TABLE, KeyType::Long)],
        async |setup| {
            setup.config.server.get_deadline = SHORT_DEADLINE;
            Ok(())
        },
    )
    .await?;
    assert!(ServerConfig::default().multi_get_deadline > SHORT_DEADLINE + DEADLINE_MARGIN);
    stack.serving.ready().await;
    let keys = stack.fixtures[0].keys_at(&SAMPLE[..BATCH_IDS]);
    let cache = stack.serving.opened().cache.clone();
    let fetched = cache.fetches();
    stack.s3.hold();

    let (outcome, elapsed) = tokio::time::timeout(
        SHUTDOWN_WAIT,
        timed(stack.get(TABLE, Some(wire_key(&keys[0])))),
    )
    .await?;
    assert_ended_at(outcome, elapsed, SHORT_DEADLINE, Code::DeadlineExceeded);
    assert_eq!(stack.s3.held(), 1, "the call reached the held store");
    assert_eq!(cache.fetches(), fetched + 1);
    poll_until(
        DEADLINE_MARGIN,
        "the page GET ending with the call",
        async || Ok((stack.s3.waiting() == 0).then_some(())),
    )
    .await?;

    let (outcome, elapsed) =
        tokio::time::timeout(SHUTDOWN_WAIT, timed(stack.multi_get(TABLE, &keys[1..]))).await?;
    assert_ended_at(outcome, elapsed, SHORT_DEADLINE, Code::DeadlineExceeded);
    poll_until(
        DEADLINE_MARGIN,
        "the batch's page GETs ending with it",
        async || Ok((stack.s3.waiting() == 0).then_some(())),
    )
    .await?;

    stack.s3.release();
    let fetched = cache.fetches();
    let response = stack.get(TABLE, Some(wire_key(&keys[0]))).await?;
    assert_eq!(response.presence, i32::from(v1::Presence::Present));
    assert_eq!(
        cache.fetches(),
        fetched + 1,
        "the abandoned fetch cached nothing"
    );
    Ok(())
}

/// With every slot taken by calls held on a stalled store, one more `Get`
/// or `MultiGet` is refused at once with `RESOURCE_EXHAUSTED` rather than
/// queued; the held calls end at their deadline and give their slots back,
/// so the next call is admitted.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_call_beyond_max_in_flight_is_refused_at_once() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::start_with(
        vec![FixtureSpec::small(TABLE, KeyType::Long)],
        async |setup| {
            setup.config.server.max_in_flight = IN_FLIGHT_CAP;
            setup.config.server.get_deadline = SHORT_DEADLINE;
            setup.config.server.multi_get_deadline = SHORT_DEADLINE;
            Ok(())
        },
    )
    .await?;
    stack.serving.ready().await;
    let keys = stack.fixtures[0].keys_at(&SAMPLE[..BATCH_IDS]);
    stack.s3.hold();

    let mut client = stack.client.clone();
    let held_get = tokio::spawn({
        let request = GetRequest {
            table: TABLE.to_string(),
            key: Some(wire_key(&keys[0])),
            columns: wire_columns(&[VALUE_COLUMN]),
        };
        async move { client.get(request).await.map(|_| ()) }
    });
    let mut client = stack.client.clone();
    let held_batch = tokio::spawn({
        let request = MultiGetRequest {
            table: TABLE.to_string(),
            keys: vec![wire_key(&keys[1])],
            columns: wire_columns(&[VALUE_COLUMN]),
        };
        async move { client.multi_get(request).await.map(|_| ()) }
    });
    poll_until(SHUTDOWN_WAIT, "both calls reaching the store", async || {
        Ok((stack.s3.held() == IN_FLIGHT_CAP).then_some(()))
    })
    .await?;

    let before = stack.scrape().await?;
    assert_in_flight(&before, IN_FLIGHT_CAP, IN_FLIGHT_CAP);
    let (outcome, elapsed) = timed(stack.get(TABLE, Some(wire_key(&keys[2])))).await;
    assert_eq!(code_of(outcome), Code::ResourceExhausted);
    assert!(elapsed < REFUSED_WITHIN, "refused after {elapsed:?}");
    let (outcome, elapsed) = timed(stack.multi_get(TABLE, &keys[2..])).await;
    assert_eq!(code_of(outcome), Code::ResourceExhausted);
    assert!(elapsed < REFUSED_WITHIN, "refused after {elapsed:?}");
    assert_eq!(
        stack.s3.held(),
        IN_FLIGHT_CAP,
        "a refused call reads nothing"
    );

    for held in [held_get, held_batch] {
        let outcome = tokio::time::timeout(SHUTDOWN_WAIT, held).await??;
        assert_eq!(code_of(outcome), Code::DeadlineExceeded);
    }
    let after = stack.scrape().await?;
    for call in [Call::Get, Call::MultiGet] {
        assert_counted(&before, &after, call, Code::ResourceExhausted, 1);
        assert_counted(&before, &after, call, Code::DeadlineExceeded, 1);
    }
    assert_in_flight(&after, 0, IN_FLIGHT_CAP);
    stack.s3.release();
    let response = stack.get(TABLE, Some(wire_key(&keys[2]))).await?;
    assert_eq!(response.presence, i32::from(v1::Presence::Present));
    assert_in_flight(&stack.scrape().await?, 0, IN_FLIGHT_CAP);
    Ok(())
}

/// A call that ends at its client's deadline gives its slot back then, not
/// at the server's longer deadline: tonic's server layer drops the call at
/// the client's `grpc-timeout`, and the client reports the expiry as its
/// own, `CANCELLED`. With one slot, a second call made just after the first
/// ended reaches the store instead of being refused.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_client_deadline_frees_the_slot_it_held() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::start_with(
        vec![FixtureSpec::small(TABLE, KeyType::Long)],
        async |setup| {
            setup.config.server.max_in_flight = 1;
            Ok(())
        },
    )
    .await?;
    assert!(ServerConfig::default().get_deadline > CLIENT_DEADLINE + DEADLINE_MARGIN);
    stack.serving.ready().await;
    let keys = stack.fixtures[0].keys_at(&SAMPLE[..BATCH_IDS]);
    stack.s3.hold();
    let before = stack.scrape().await?;

    let timed_out = 2;
    for (sent, key) in keys[..timed_out].iter().enumerate() {
        let mut get = tonic::Request::new(GetRequest {
            table: TABLE.to_string(),
            key: Some(wire_key(key)),
            columns: wire_columns(&[VALUE_COLUMN]),
        });
        get.set_timeout(CLIENT_DEADLINE);
        let (outcome, elapsed) =
            tokio::time::timeout(SHUTDOWN_WAIT, timed(stack.client.get(get))).await?;
        assert_eq!(code_of(outcome), Code::Cancelled, "call {sent}");
        assert!(elapsed < CLIENT_DEADLINE + DEADLINE_MARGIN, "call {sent}");
        assert_eq!(stack.s3.held(), sent + 1, "call {sent} reached the store");
        // The server's own timer on the client's deadline fires a loopback
        // hop after the client's. It drops the call, and the slot and the
        // page GET with it, so once the store sees the GET given up, the
        // slot is back.
        poll_until(
            SHUTDOWN_WAIT,
            "the call's page GET ending with it",
            async || Ok((stack.s3.waiting() == 0).then_some(())),
        )
        .await?;
    }
    // A call tonic dropped at its client's deadline was answered
    // `CANCELLED` by tonic, and counts so.
    assert_counted(
        &before,
        &stack.scrape().await?,
        Call::Get,
        Code::Cancelled,
        approx::count(timed_out),
    );
    Ok(())
}

/// A `MultiGet` of `max_multi_get_values` values is answered, one column of
/// each key or each key alone; one more value is `INVALID_ARGUMENT` before
/// any lookup, so it issues no GET.
#[tokio::test(flavor = "multi_thread")]
async fn test_multi_get_over_max_values_is_invalid_argument_before_any_lookup() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::start_with(
        vec![FixtureSpec::small(TABLE, KeyType::Long)],
        async |setup| {
            setup.config.server.max_multi_get_values = BATCH_IDS;
            Ok(())
        },
    )
    .await?;
    stack.serving.ready().await;
    let keys = stack.fixtures[0].keys_at(&SAMPLE[..=BATCH_IDS]);

    let before = stack.stats.gets();
    assert_eq!(
        code_of(stack.multi_get(TABLE, &keys).await),
        Code::InvalidArgument
    );
    assert_eq!(stack.stats.gets(), before, "a refused batch issues no GET");

    let at_limit = &keys[..BATCH_IDS];
    let response = stack.multi_get(TABLE, at_limit).await?;
    assert_eq!(response.results.len(), BATCH_IDS);
    for hit in &response.results {
        assert_eq!(hit.presence, i32::from(v1::Presence::Present));
    }
    let presence_only = stack.multi_get_naming(TABLE, at_limit, &[]).await?;
    assert_eq!(
        presence_only.results.len(),
        BATCH_IDS,
        "a presence-only key counts one value"
    );
    let before = stack.stats.gets();
    assert_eq!(
        code_of(
            stack
                .multi_get_naming(TABLE, &keys[..=BATCH_IDS], &[])
                .await
        ),
        Code::InvalidArgument
    );
    assert_eq!(stack.stats.gets(), before, "a refused batch issues no GET");
    Ok(())
}

/// A column the table does not serve, and a column named twice, are the
/// caller's faults on `Get` and `MultiGet` alike, refused before any read;
/// a presence-only call reads no value page.
#[tokio::test(flavor = "multi_thread")]
async fn test_unknown_or_repeated_column_is_invalid_argument_and_presence_only_reads_nothing()
-> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::ready_small().await?;
    let key = stack.fixtures[0].key(SAMPLE[0]);
    let before = stack.stats.gets();
    for columns in [&["unserved"][..], &[VALUE_COLUMN, VALUE_COLUMN]] {
        assert_eq!(
            code_of(stack.get_naming(TABLE, Some(wire_key(&key)), columns).await),
            Code::InvalidArgument,
            "{columns:?}"
        );
        assert_eq!(
            code_of(
                stack
                    .multi_get_naming(TABLE, std::slice::from_ref(&key), columns)
                    .await
            ),
            Code::InvalidArgument,
            "{columns:?}"
        );
    }
    let present = stack.get_naming(TABLE, Some(wire_key(&key)), &[]).await?;
    assert_eq!(present.presence, i32::from(v1::Presence::Present));
    assert!(present.values.is_empty(), "presence alone carries no value");
    let absent = stack.fixtures[0].absent_near(SAMPLE[0]);
    let absent = stack
        .get_naming(TABLE, Some(wire_key(&absent)), &[])
        .await?;
    assert_eq!(absent.presence, i32::from(v1::Presence::Absent));
    assert_eq!(stack.stats.gets(), before, "no value page is read");
    Ok(())
}

/// A table serving two value columns, `VALUE_COLUMN` holding values of
/// `kind` and declared `format`, beside `SECOND_VALUE_COLUMN`, opaque.
async fn two_column_stack(kind: ValueKind, format: ValueFormat) -> Result<Stack, BoxError> {
    let spec = FixtureSpec {
        second_value: true,
        value_kind: kind,
        ..FixtureSpec::small(TABLE, KeyType::Long)
    };
    let mut stack = Stack::start_with(vec![spec], async |setup| {
        setup.config.tables[0] =
            setup.fixtures[0].config_declaring(Residency::Full, VALUE_COLUMN, format);
        Ok(())
    })
    .await?;
    stack.serving.ready().await;
    Ok(stack)
}

/// `TableInfo` lists the value columns a lookup may name, in name order,
/// each with its type and, for a `fixed` column alone, a width.
#[tokio::test(flavor = "multi_thread")]
async fn test_table_info_lists_the_value_columns() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = two_column_stack(ValueKind::Random, ValueFormat::Opaque).await?;
    let info = stack.table_info(TABLE).await?;
    let binary = |name: &str| v1::ValueColumn {
        name: name.to_string(),
        value_type: i32::from(v1::ValueType::Binary),
        width: 0,
        format: i32::from(v1::ValueFormat::Opaque),
    };
    assert_eq!(
        info.value_columns,
        [binary(SECOND_VALUE_COLUMN), binary(VALUE_COLUMN)],
        "in name order"
    );
    Ok(())
}

/// `Get` and `MultiGet` naming two columns carry one value per column, in
/// the order the request names them, over the wire.
#[tokio::test(flavor = "multi_thread")]
async fn test_get_and_multi_get_carry_two_columns_in_request_order() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = two_column_stack(ValueKind::Random, ValueFormat::Opaque).await?;
    let fixture = &stack.fixtures[0];
    let keys = fixture.keys_at(&SAMPLE);
    let expected = |key: &Key, columns: &[&str]| -> Vec<Option<Vec<u8>>> {
        columns
            .iter()
            .map(|&column| match column {
                VALUE_COLUMN => fixture.value(key),
                _ => fixture.second_value(key),
            })
            .collect()
    };
    let orders = [
        [VALUE_COLUMN, SECOND_VALUE_COLUMN],
        [SECOND_VALUE_COLUMN, VALUE_COLUMN],
    ];
    let wanted: Vec<_> = orders
        .iter()
        .map(|order| expected(&keys[0], order))
        .collect();
    assert_ne!(wanted[0], wanted[1], "the two orders differ");
    let batches: Vec<Vec<Vec<Option<Vec<u8>>>>> = orders
        .iter()
        .map(|order| keys.iter().map(|key| expected(key, order)).collect())
        .collect();
    for (order, (single, batch)) in orders.iter().zip(wanted.iter().zip(&batches)) {
        let got = stack
            .get_naming(TABLE, Some(wire_key(&keys[0])), order)
            .await?;
        assert_eq!(got.presence, i32::from(v1::Presence::Present));
        let values: Vec<_> = got.values.iter().map(wire_value).collect();
        assert_eq!(&values, single, "{order:?}");
        let answered = stack.multi_get_naming(TABLE, &keys, order).await?;
        let values: Vec<Vec<_>> = answered
            .results
            .iter()
            .map(|hit| hit.values.iter().map(wire_value).collect())
            .collect();
        assert_eq!(&values, batch, "{order:?}");
    }
    Ok(())
}

/// The column read of `name` keeping `fields`.
fn wire_read(name: &str, fields: &[u32]) -> v1::ColumnRead {
    v1::ColumnRead {
        name: name.to_string(),
        fields: fields.to_vec(),
    }
}

/// The value-bytes counters of `column` of `TABLE` in `exposition`: read,
/// then returned.
fn value_bytes(exposition: &str, column: &str) -> (f64, f64) {
    let labels = [(metric::LABEL_TABLE, TABLE), (metric::LABEL_COLUMN, column)];
    (
        series_sum(exposition, metric::VALUE_BYTES_READ_TOTAL, &labels),
        series_sum(exposition, metric::VALUE_BYTES_RETURNED_TOTAL, &labels),
    )
}

/// The bytes of `values`, nulls as none, as the value-bytes counters count
/// them.
fn length_of(values: &[Option<Vec<u8>>]) -> f64 {
    let bytes: usize = values.iter().flatten().map(Vec::len).sum();
    approx::gauge_len(bytes)
}

/// `TableInfo` reports each column's format, and `Get` and `MultiGet`
/// trim a protobuf column to the fields a read names while an opaque one
/// beside it is read whole; the value-bytes counters count what each
/// column read and returned (RFD 024).
#[tokio::test(flavor = "multi_thread")]
async fn test_get_and_multi_get_trim_a_protobuf_column_and_count_its_bytes() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = two_column_stack(ValueKind::Protobuf, ValueFormat::Protobuf).await?;
    let info = stack.table_info(TABLE).await?;
    let formats: Vec<(String, i32)> = info
        .value_columns
        .iter()
        .map(|column| (column.name.clone(), column.format))
        .collect();
    assert_eq!(
        formats,
        [
            (
                SECOND_VALUE_COLUMN.to_string(),
                i32::from(v1::ValueFormat::Opaque)
            ),
            (
                VALUE_COLUMN.to_string(),
                i32::from(v1::ValueFormat::Protobuf)
            ),
        ]
    );
    let fixture = &stack.fixtures[0];
    let keys = fixture.keys_at(&SAMPLE);
    let fields = [protobuf::ID_FIELD, protobuf::SCORES_FIELD];
    let reads = [
        ColumnRead {
            name: VALUE_COLUMN,
            fields: &fields,
        },
        ColumnRead::whole(SECOND_VALUE_COLUMN),
    ];
    let wire_reads = || {
        vec![
            wire_read(VALUE_COLUMN, &fields),
            wire_read(SECOND_VALUE_COLUMN, &[]),
        ]
    };
    let wanted: Vec<Vec<Option<Vec<u8>>>> = keys
        .iter()
        .map(|key| {
            let Row::Present(values) = fixture.expected_reads(key, &reads) else {
                unreachable!("a sampled key is present");
            };
            values
                .into_iter()
                .map(|value| value.map(|bytes| bytes.to_vec()))
                .collect()
        })
        .collect();
    let whole: Vec<Option<Vec<u8>>> = keys.iter().map(|key| fixture.value(key)).collect();
    let before = stack.scrape().await?;
    for (key, expected) in keys.iter().zip(&wanted) {
        let got = stack
            .get_reading(TABLE, Some(wire_key(key)), wire_reads())
            .await?;
        let values: Vec<_> = got.values.iter().map(wire_value).collect();
        assert_eq!(&values, expected, "{key}");
    }
    let answered = stack.multi_get_reading(TABLE, &keys, wire_reads()).await?;
    let batch: Vec<Vec<_>> = answered
        .results
        .iter()
        .map(|hit| hit.values.iter().map(wire_value).collect())
        .collect();
    assert_eq!(batch, wanted);
    let after = stack.scrape().await?;
    let looked_up_twice = 2.0;
    let trimmed: Vec<Option<Vec<u8>>> = wanted.iter().map(|values| values[0].clone()).collect();
    let detail: Vec<Option<Vec<u8>>> = wanted.iter().map(|values| values[1].clone()).collect();
    assert!(
        length_of(&trimmed) < length_of(&whole),
        "the projection saves bytes"
    );
    let delta = |column| {
        let (read_after, returned_after) = value_bytes(&after, column);
        let (read_before, returned_before) = value_bytes(&before, column);
        (read_after - read_before, returned_after - returned_before)
    };
    assert_eq!(
        delta(VALUE_COLUMN),
        (
            looked_up_twice * length_of(&whole),
            looked_up_twice * length_of(&trimmed)
        )
    );
    assert_eq!(
        delta(SECOND_VALUE_COLUMN),
        (
            looked_up_twice * length_of(&detail),
            looked_up_twice * length_of(&detail)
        ),
        "a whole read counts the same in both"
    );
    Ok(())
}

/// Every field list the column's format or the protobuf range does not
/// take is `INVALID_ARGUMENT` on `Get` and `MultiGet`, before any read.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_field_list_the_column_does_not_take_is_invalid_argument() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = two_column_stack(ValueKind::Protobuf, ValueFormat::Protobuf).await?;
    let key = stack.fixtures[0].key(SAMPLE[0]);
    let before = stack.stats.gets();
    let cases = [
        wire_read(SECOND_VALUE_COLUMN, &[protobuf::ID_FIELD]),
        wire_read(VALUE_COLUMN, &[0]),
        wire_read(VALUE_COLUMN, &[MAX_FIELD_NUMBER + 1]),
        wire_read(VALUE_COLUMN, &[protobuf::ID_FIELD, protobuf::ID_FIELD]),
    ];
    for read in cases {
        assert_eq!(
            code_of(
                stack
                    .get_reading(TABLE, Some(wire_key(&key)), vec![read.clone()])
                    .await
            ),
            Code::InvalidArgument,
            "{read:?}"
        );
        assert_eq!(
            code_of(
                stack
                    .multi_get_reading(TABLE, std::slice::from_ref(&key), vec![read.clone()])
                    .await
            ),
            Code::InvalidArgument,
            "{read:?}"
        );
    }
    assert_eq!(
        stack.stats.gets(),
        before,
        "a refused request reads nothing"
    );
    Ok(())
}

/// A value of a protobuf column that is not protobuf fails a `Get` and a
/// `MultiGet` holding its key as `FAILED_PRECONDITION`, a `MultiGet`
/// whole beside a key that answers, counted under `projection_errors_total`
/// and under no value-bytes counter, and is served whole to a read naming
/// no fields.
#[tokio::test(flavor = "multi_thread")]
async fn test_a_malformed_value_is_failed_precondition_and_counted() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = two_column_stack(ValueKind::Random, ValueFormat::Protobuf).await?;
    let fixture = &stack.fixtures[0];
    let (malformed, _) = protobuf::malformed_key(fixture)
        .expect("some random value starts with an undefined wire type");
    let malformed_value = fixture.value(&malformed);
    let absent = fixture.absent_near(SAMPLE[0]);
    let errors = |exposition: &str| {
        series_sum(
            exposition,
            metric::PROJECTION_ERRORS_TOTAL,
            &[
                (metric::LABEL_TABLE, TABLE),
                (metric::LABEL_COLUMN, VALUE_COLUMN),
            ],
        )
    };
    let before = stack.scrape().await?;
    let trimmed = || vec![wire_read(VALUE_COLUMN, &[protobuf::ID_FIELD])];
    assert_eq!(
        code_of(
            stack
                .get_reading(TABLE, Some(wire_key(&malformed)), trimmed())
                .await
        ),
        Code::FailedPrecondition
    );
    assert_eq!(
        code_of(
            stack
                .multi_get_reading(TABLE, std::slice::from_ref(&malformed), trimmed())
                .await
        ),
        Code::FailedPrecondition
    );
    let failed = stack.scrape().await?;
    assert_eq!(
        value_bytes(&failed, VALUE_COLUMN),
        value_bytes(&before, VALUE_COLUMN),
        "a failed lookup counts no bytes"
    );
    assert_eq!(
        code_of(
            stack
                .multi_get_reading(TABLE, &[absent, malformed.clone()], trimmed())
                .await
        ),
        Code::FailedPrecondition,
        "a batch fails whole beside a key that answers"
    );
    let whole = stack
        .get_naming(TABLE, Some(wire_key(&malformed)), &[VALUE_COLUMN])
        .await?;
    assert_eq!(one_wire_value(&whole.values), malformed_value);
    let after = stack.scrape().await?;
    let failed_lookups: usize = 3;
    assert_eq!(
        (errors(&after) - errors(&before)).to_bits(),
        approx::gauge_len(failed_lookups).to_bits()
    );
    Ok(())
}

/// A call held on a stalled store when the shutdown arrives keeps its
/// connection open past the drain deadline; the drain stops waiting at the
/// deadline and the run ends with `DrainOverran` instead of waiting out the
/// call. The page cache is closed on that path too, which a memory-only
/// cache gives no way to observe.
#[tokio::test(flavor = "multi_thread")]
async fn test_drain_stops_waiting_at_its_deadline() -> TestResult {
    let _serial = SERIAL.lock().await;
    let stack = Stack::start_with(
        vec![FixtureSpec::small(TABLE, KeyType::Long)],
        async |setup| {
            setup.config.server.drain_deadline = SHORT_DRAIN;
            Ok(())
        },
    )
    .await?;
    assert!(ServerConfig::default().get_deadline > SHORT_DRAIN + DEADLINE_MARGIN);
    let mut serving = stack.serving;
    serving.ready().await;
    let key = stack.fixtures[0].key(SAMPLE[0]);
    stack.s3.hold();
    let mut client = stack.client.clone();
    let in_flight = tokio::spawn(async move {
        client
            .get(GetRequest {
                table: TABLE.to_string(),
                key: Some(wire_key(&key)),
                columns: wire_columns(&[VALUE_COLUMN]),
            })
            .await
    });
    poll_until(SHUTDOWN_WAIT, "the call reaching the store", async || {
        Ok((stack.s3.held() == 1).then_some(()))
    })
    .await?;

    serving.shutdown();
    let (outcome, elapsed) = tokio::time::timeout(SHUTDOWN_WAIT, timed(serving.join())).await?;
    let Err(ServeError::DrainOverran { after }) = outcome else {
        panic!("expected the drain to overrun, got {outcome:?}");
    };
    assert_eq!(after, SHORT_DRAIN);
    assert!(
        elapsed >= SHORT_DRAIN && elapsed < SHORT_DRAIN + DEADLINE_MARGIN,
        "the run ended after {elapsed:?}, drain deadline {SHORT_DRAIN:?}"
    );
    assert!(
        !in_flight.is_finished(),
        "the drain did not wait out the call"
    );
    Ok(())
}

/// A configuration a reload test changes under the running daemon, read as
/// the daemon reads its file: each version under a digest of its own.
struct Versions {
    current: std::sync::Mutex<(u64, Config)>,
    /// Reads the daemon made.
    reads: std::sync::atomic::AtomicU64,
}

impl Versions {
    /// Version 0, `config`.
    fn new(config: Config) -> Arc<Self> {
        Arc::new(Self {
            current: std::sync::Mutex::new((0, config)),
            reads: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// What the daemon follows these versions through, from version 0.
    fn reload(self: &Arc<Self>) -> Reload {
        Reload {
            source: Arc::clone(self) as Arc<dyn ConfigSource>,
            started_from: Self::digest(0),
        }
    }

    /// Makes `config` the next version.
    fn set(&self, config: Config) {
        let mut current = self.current.lock().expect("no test panics holding it");
        *current = (current.0 + 1, config);
    }

    /// The configuration as it stands.
    fn config(&self) -> Config {
        self.current
            .lock()
            .expect("no test panics holding it")
            .1
            .clone()
    }

    /// Reads the daemon made so far.
    fn reads(&self) -> u64 {
        self.reads.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn digest(version: u64) -> Digest {
        Digest::of(&version.to_le_bytes())
    }
}

impl ConfigSource for Versions {
    fn read(&self) -> std::io::Result<Reading> {
        self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let (version, config) = self
            .current
            .lock()
            .expect("no test panics holding it")
            .clone();
        Ok(Reading {
            digest: Self::digest(version),
            config: Ok(config),
        })
    }
}

/// A stack over two small fixtures, `TABLE` and `UUID_TABLE`, serving
/// `TABLE` alone and following `Versions` from there, loaded.
async fn following() -> Result<(Stack, Arc<Versions>), BoxError> {
    following_over(vec![
        FixtureSpec::small(TABLE, KeyType::Long),
        FixtureSpec::small(UUID_TABLE, KeyType::Uuid),
    ])
    .await
}

/// A stack over `specs`, serving the first alone and following `Versions`
/// from there, loaded.
async fn following_over(specs: Vec<FixtureSpec>) -> Result<(Stack, Arc<Versions>), BoxError> {
    let mut followed = None;
    let mut stack = Stack::start_with(specs, async |setup| {
        setup.config.tables.truncate(1);
        let versions = Versions::new(setup.config.clone());
        setup.options.reload = Some(versions.reload());
        followed = Some(versions);
        Ok(())
    })
    .await?;
    stack.serving.ready().await;
    let versions = followed.ok_or("the setup ran")?;
    Ok((stack, versions))
}

impl Stack {
    /// `UUID_TABLE`'s configuration as `start_with` would serve it.
    fn uuid_table(&self) -> TableConfig {
        self.fixtures[1].config(Residency::Full, false)
    }

    /// `config_reloads_total` of `result` in a scrape now; zero before the
    /// first, when the series is not yet exposed.
    async fn reloads(&self, result: ReloadResult) -> Result<f64, BoxError> {
        let (_, scrape) = self.http_get(METRICS_PATH).await?;
        Ok(series_value(
            &scrape,
            metric::CONFIG_RELOADS_TOTAL,
            &[(metric::LABEL_RESULT, &result.to_string())],
        )
        .unwrap_or(0.0))
    }

    /// `config_pending` in a scrape now.
    async fn pending(&self) -> Result<Option<f64>, BoxError> {
        let (_, scrape) = self.http_get(METRICS_PATH).await?;
        Ok(series_value(&scrape, metric::CONFIG_PENDING, &[]))
    }

    /// Waits for `config_reloads_total` of `result` to pass `before`.
    async fn reloaded(&self, result: ReloadResult, before: f64) -> Result<(), BoxError> {
        poll_until(REFRESH_WAIT, "a reload counted", async || {
            Ok((self.reloads(result).await? > before).then_some(()))
        })
        .await
    }
}

/// Polls the retry test waits for after a refusal: two whose attempts it
/// checks, and the one that shows they ended.
const POLLS_OVER_A_REFUSAL: u64 = 3;

/// A table no fixture holds: its first load fails.
fn missing_table(fixture: &Fixture) -> TableConfig {
    TableConfig {
        name: "missing".to_string(),
        source: SourceConfig::Static {
            location: format!("{S3_SCHEME}{BUCKET}/{WAREHOUSE}/missing/metadata/v1.metadata.json"),
        },
        ..fixture.config(Residency::Full, false)
    }
}

/// A reload that adds one table and removes another swaps both at once:
/// the added table answers, the removed one is `NOT_FOUND`, readiness
/// stays, and nothing is left pending.
#[tokio::test(flavor = "multi_thread")]
async fn test_reload_adds_and_removes_tables_on_request() -> TestResult {
    let _serial = SERIAL.lock().await;
    let (mut stack, versions) = following().await?;
    let before = stack.reloads(ReloadResult::Applied).await?;
    let mut next = versions.config();
    next.tables = vec![stack.uuid_table()];
    versions.set(next);
    stack.serving.reload();
    stack.reloaded(ReloadResult::Applied, before).await?;

    let uuid_key = stack.fixtures[1].key(SAMPLE[0]);
    let response = stack.get(UUID_TABLE, Some(wire_key(&uuid_key))).await?;
    assert_eq!(response.presence, i32::from(v1::Presence::Present));
    assert_eq!(code_of(stack.table_info(TABLE).await), Code::NotFound);
    let (status, _) = stack.http_get(READY_PATH).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(stack.pending().await?, Some(0.0));

    // The removed table's gauges leave the exposition rather than read
    // their last values; its counters stay, and the added table's gauges
    // are there.
    let removed = (metric::LABEL_TABLE, TABLE);
    let scrape = poll_until(
        REFRESH_WAIT,
        "the removed table's gauges dropped",
        async || {
            let scrape = stack.scrape().await?;
            let left = TABLE_GAUGES
                .iter()
                .any(|name| series_value(&scrape, name, &[removed]).is_some());
            Ok((!left).then_some(scrape))
        },
    )
    .await?;
    assert!(series_value(&scrape, metric::LOOKUPS_TOTAL, &[removed]).is_some());
    for name in TABLE_GAUGES {
        let added = [(metric::LABEL_TABLE, UUID_TABLE)];
        assert!(series_value(&scrape, name, &added).is_some(), "{name}");
    }
    Ok(())
}

/// Every gauge a serving table exports under its name.
const TABLE_GAUGES: [&str; 17] = [
    metric::TABLE_STATE,
    metric::INDEX_BYTES,
    metric::FINDINGS,
    metric::GENERATION_SEQUENCE_NUMBER,
    metric::GENERATION_SWAPPED_AT_SECONDS,
    metric::GENERATION_AGE_SECONDS,
    metric::TABLE_BELOW_LEVEL,
    metric::TABLE_LEVEL,
    metric::TABLE_DECLARED_LEVEL,
    metric::TABLE_GETS_PER_HIT,
    metric::TABLE_GETS_PER_MISS,
    metric::TABLE_FILES_PROBED_PER_LOOKUP,
    metric::TABLE_DELETED_ROWS,
    metric::TABLE_DELETED_FRACTION,
    metric::TABLE_DELETE_FILES,
    metric::TABLE_DELETE_STATE_BYTES,
    metric::TABLE_OLDEST_DELETE_AGE_SECONDS,
];

/// A changed file is found on the poll, with no request.
#[tokio::test(flavor = "multi_thread")]
async fn test_reload_follows_the_file_on_the_poll() -> TestResult {
    let _serial = SERIAL.lock().await;
    let (mut stack, versions) = following().await?;
    let mut next = versions.config();
    next.tables.push(stack.uuid_table());
    versions.set(next);
    poll_until(REFRESH_WAIT, "the added table serving", async || {
        Ok(stack.table_info(UUID_TABLE).await.ok())
    })
    .await?;
    stack.table_info(TABLE).await?;
    Ok(())
}

/// A change outside the tables refuses the whole reload: the table it
/// would have added is not served, and `config_pending` reads 1.
#[tokio::test(flavor = "multi_thread")]
async fn test_reload_refuses_a_change_outside_the_tables() -> TestResult {
    let _serial = SERIAL.lock().await;
    let (mut stack, versions) = following().await?;
    let before = stack.reloads(ReloadResult::Refused).await?;
    let mut next = versions.config();
    next.tables.push(stack.uuid_table());
    next.server.max_in_flight += 1;
    versions.set(next);
    stack.serving.reload();
    stack.reloaded(ReloadResult::Refused, before).await?;

    assert_eq!(code_of(stack.table_info(UUID_TABLE).await), Code::NotFound);
    stack.table_info(TABLE).await?;
    assert_eq!(stack.pending().await?, Some(1.0));
    Ok(())
}

/// A table that fails its load refuses every change the reload carries,
/// and a poll does not try the refused file again; a request does.
#[tokio::test(flavor = "multi_thread")]
async fn test_reload_refused_for_a_failed_load_is_retried_on_request_only() -> TestResult {
    let _serial = SERIAL.lock().await;
    let (mut stack, versions) = following().await?;
    let before = stack.reloads(ReloadResult::Refused).await?;
    let mut next = versions.config();
    next.tables.push(stack.uuid_table());
    next.tables.push(missing_table(&stack.fixtures[0]));
    versions.set(next);
    stack.serving.reload();
    stack.reloaded(ReloadResult::Refused, before).await?;
    assert_eq!(code_of(stack.table_info(UUID_TABLE).await), Code::NotFound);
    stack.table_info(TABLE).await?;

    // A reader's attempts run one after another, so once three more polls
    // have read, the attempts of the first two have ended.
    let refused = stack.reloads(ReloadResult::Refused).await?;
    let reads = versions.reads();
    poll_until(REFRESH_WAIT, "three more polls", async || {
        Ok((versions.reads() >= reads + POLLS_OVER_A_REFUSAL).then_some(()))
    })
    .await?;
    assert_eq!(
        stack.reloads(ReloadResult::Refused).await?.to_bits(),
        refused.to_bits(),
        "the polls pass the refused file over"
    );
    stack.serving.reload();
    stack.reloaded(ReloadResult::Refused, refused).await?;
    Ok(())
}

/// A table whose configuration changed is refreshed as the new
/// configuration says: the old table, pinned to its metadata file, is
/// replaced by one following the table root, which the poll then advances.
/// The old table's refresh, stopped by the reload, does not end the run.
#[tokio::test(flavor = "multi_thread")]
async fn test_reload_refreshes_a_changed_table_under_its_new_configuration() -> TestResult {
    let _serial = SERIAL.lock().await;
    let (mut stack, versions) = following().await?;
    let first = stack.table_info(TABLE).await?.snapshot_id;
    let before = stack.reloads(ReloadResult::Applied).await?;
    stack.fixtures[0].write_hint().await?;
    let mut next = versions.config();
    next.tables[0].source = stack.fixtures[0].root_source();
    versions.set(next);
    stack.serving.reload();
    stack.reloaded(ReloadResult::Applied, before).await?;
    assert_eq!(
        stack.table_info(TABLE).await?.snapshot_id,
        first,
        "the changed table starts from the snapshot it served"
    );

    stack.fixtures[0]
        .append(stack.catalog.as_ref(), NEW_IDS, APPEND_PREFIX)
        .await?;
    stack.fixtures[0].write_hint().await?;
    poll_until(REFRESH_WAIT, "the changed table advancing", async || {
        let info = stack.table_info(TABLE).await?;
        Ok((info.snapshot_id != first).then_some(()))
    })
    .await?;
    let (status, _) = stack.http_get(READY_PATH).await?;
    assert_eq!(status, StatusCode::OK);
    Ok(())
}

/// A reload that changes only a table's `strict` applies to the table
/// serving in place, with no load: the same table serves the same
/// generation under the new entry, and the reload is counted applied.
#[tokio::test(flavor = "multi_thread")]
async fn test_reload_applies_a_strict_change_in_place() -> TestResult {
    let _serial = SERIAL.lock().await;
    let (mut stack, versions) = following().await?;
    let table = Arc::clone(&stack.serving.opened().tables()[0]);
    let generation = stack.generation();
    let before = stack.reloads(ReloadResult::Applied).await?;
    let mut next = versions.config();
    assert!(!next.tables[0].strict);
    next.tables[0].strict = true;
    versions.set(next);
    stack.serving.reload();
    stack.reloaded(ReloadResult::Applied, before).await?;

    let after = Arc::clone(&stack.serving.opened().tables()[0]);
    assert!(
        Arc::ptr_eq(&after, &table),
        "the table serving is retuned, not replaced"
    );
    assert!(Arc::ptr_eq(&stack.generation(), &generation));
    assert!(after.config().strict);
    stack.table_info(TABLE).await?;
    Ok(())
}

/// A reload that would make a table strict while its serving snapshot is
/// below its level is refused and counted refused, as a start under it
/// would fail: the table serves on under its entry, and `config_pending`
/// reads 1.
#[tokio::test(flavor = "multi_thread")]
async fn test_reload_refuses_a_strict_change_the_serving_snapshot_is_below() -> TestResult {
    let _serial = SERIAL.lock().await;
    let balanced = FixtureSpec {
        layout: Layout {
            dictionary: Dictionary::Value,
            ..Layout::contract()
        },
        ..FixtureSpec::small(TABLE, KeyType::Long)
    };
    let (mut stack, versions) = following_over(vec![balanced]).await?;
    let table = Arc::clone(&stack.serving.opened().tables()[0]);
    let before = stack.reloads(ReloadResult::Refused).await?;
    let mut next = versions.config();
    assert!(!next.tables[0].strict);
    next.tables[0].strict = true;
    versions.set(next);
    stack.serving.reload();
    stack.reloaded(ReloadResult::Refused, before).await?;

    assert!(Arc::ptr_eq(&stack.serving.opened().tables()[0], &table));
    assert!(!table.config().strict, "the refused entry is not taken");
    assert_eq!(stack.pending().await?, Some(1.0));
    stack.table_info(TABLE).await?;
    Ok(())
}
