//! The daemon in-process over the in-process S3 server (RFD 014, P tier):
//! health and readiness before and after the first loads, the wire contract
//! of RFD 010 with its error mapping, request counting, refresh within the
//! poll interval, the drain, and the start-or-fail rule of RFD 005.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use hyper::StatusCode;
use icetray::listen::{METRICS_PATH, NOT_READY_BODY, READY_BODY, READY_PATH};
use icetray::serve::{OVERALL_SERVICE, PeriodicTask};
use icetray::{ServeError, ServeOptions, Serving, telemetry};
use icetray_core::approx;
use icetray_core::config::{Config, Residency, ServerConfig, TableConfig};
use icetray_core::key::UUID_BYTES;
use icetray_core::{ErrorKind, Generation, IoStats, Key, KeyType, Storage, TableState, metric};
use icetray_proto::v1::icetray_client::IcetrayClient;
use icetray_proto::v1::icetray_server::SERVICE_NAME;
use icetray_proto::v1::{
    self, GetRequest, GetResponse, MultiGetRequest, MultiGetResponse, TableInfoRequest,
    TableInfoResponse,
};
use icetray_testkit::fixture::{NAMESPACE, SAMPLE, SMALL_BUCKETS, SMALL_IDS};
use icetray_testkit::verify::{expected_cold_gets, series_sum, series_value};
use icetray_testkit::{
    Fixture, FixtureSpec, MemoryCatalog, Nulls, S3Fixture, memory_catalog_s3, write_fixture,
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
        };
        configure(&mut setup).await?;
        let Setup {
            fixtures,
            config,
            options,
            hold_store,
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

    /// One `Get` of `key` on `table`.
    async fn get(&mut self, table: &str, key: Option<v1::Key>) -> Result<GetResponse, Status> {
        self.client
            .get(GetRequest {
                table: table.to_string(),
                key,
            })
            .await
            .map(tonic::Response::into_inner)
    }

    /// One `MultiGet` of `keys` on `table`.
    async fn multi_get(&mut self, table: &str, keys: &[Key]) -> Result<MultiGetResponse, Status> {
        self.client
            .multi_get(MultiGetRequest {
                table: table.to_string(),
                keys: keys.iter().map(wire_key).collect(),
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
        self.serving.opened().tables[0]
            .current()
            .expect("the table was loaded")
    }

    /// `GET path` on the metrics listener.
    async fn http_get(&self, path: &str) -> Result<(StatusCode, String), BoxError> {
        http_get(self.serving.metrics_addr(), path).await
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

/// The `requests_total` count of `call` in `exposition`: zero before the
/// first call, when the series is not yet exposed.
fn calls(exposition: &str, call: telemetry::Call) -> f64 {
    series_value(
        exposition,
        metric::REQUESTS_TOTAL,
        &[(metric::LABEL_CALL, &call.to_string())],
    )
    .unwrap_or(0.0)
}

/// Asserts that `after` counts `expected` more `call`s than `before`. A
/// counter's integer is exact in the float the exposition carries it as,
/// so the difference compares exactly.
fn assert_counted(before: &str, after: &str, call: telemetry::Call, expected: u64) {
    let added = calls(after, call) - calls(before, call);
    assert_eq!(
        added.to_bits(),
        approx::gauge(expected).to_bits(),
        "{call}: {added} counted, {expected} expected"
    );
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
    let expected = expected_cold_gets(&generation, &keys)?;
    assert_eq!(expected, approx::count(COLD_IDS), "the ids share no page");

    let before = stack.stats.gets();
    for key in &keys {
        let response = stack.get(TABLE, Some(wire_key(key))).await?;
        assert_eq!(response.presence, i32::from(v1::Presence::Present), "{key}");
        assert_eq!(
            Some(response.value.to_vec()),
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
    assert!(response.value.is_empty());
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
    assert_eq!(null.presence, i32::from(v1::Presence::Null));
    assert!(null.value.is_empty());
    assert_eq!(stack.stats.gets() - before, 1, "a null costs its page cold");

    let present = stack.get(TABLE, Some(wire_key(&present_key))).await?;
    assert_eq!(present.presence, i32::from(v1::Presence::Present));
    assert_eq!(
        Some(present.value.to_vec()),
        stack.fixtures[0].value(&present_key)
    );

    let absent = stack
        .get(
            TABLE,
            Some(wire_key(&stack.fixtures[0].absent_near(NULL_ID))),
        )
        .await?;
    assert_eq!(absent.presence, i32::from(v1::Presence::Absent));
    assert!(absent.value.is_empty());
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
    let expected = expected_cold_gets(&generation, &present)?;

    let before = stack.stats.gets();
    let response = stack.multi_get(TABLE, &keys).await?;
    assert_eq!(stack.stats.gets() - before, expected);
    assert_eq!(response.snapshot_id, generation.snapshot_id.0);
    assert_eq!(response.results.len(), keys.len());
    for (key, hit) in present.iter().zip(&response.results) {
        assert_eq!(hit.presence, i32::from(v1::Presence::Present), "{key}");
        assert_eq!(
            Some(hit.value.to_vec()),
            stack.fixtures[0].value(key),
            "{key}"
        );
    }
    let last = &response.results[keys.len() - 1];
    assert_eq!(last.presence, i32::from(v1::Presence::Absent));
    assert!(last.value.is_empty());
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

    let uuid_key = stack.fixtures[1].key(SAMPLE[1]);
    let Key::Uuid(bytes) = &uuid_key else {
        panic!("{uuid_key} is a uuid");
    };
    assert_eq!(bytes.len(), UUID_BYTES);
    let response = stack.get(UUID_TABLE, Some(wire_key(&uuid_key))).await?;
    assert_eq!(response.presence, i32::from(v1::Presence::Present));
    assert_eq!(
        Some(response.value.to_vec()),
        stack.fixtures[1].value(&uuid_key)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_unknown_table_is_not_found() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::ready_small().await?;
    let key = stack.fixtures[0].key(SAMPLE[0]);
    assert_eq!(code_of(stack.table_info(NOBODY).await), Code::NotFound);
    assert_eq!(
        code_of(stack.get(NOBODY, Some(wire_key(&key))).await),
        Code::NotFound
    );
    assert_eq!(
        code_of(stack.multi_get(NOBODY, std::slice::from_ref(&key)).await),
        Code::NotFound
    );
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
    let (status, before) = stack.http_get(METRICS_PATH).await?;
    assert_eq!(status, StatusCode::OK);

    let key = stack.fixtures[0].key(SAMPLE[0]);
    for _ in 0..COUNTED_GETS {
        stack.get(TABLE, Some(wire_key(&key))).await?;
    }
    stack.multi_get(TABLE, std::slice::from_ref(&key)).await?;
    stack.table_info(TABLE).await?;

    let (status, after) = stack.http_get(METRICS_PATH).await?;
    assert_eq!(status, StatusCode::OK);
    assert_counted(&before, &after, telemetry::Call::Get, COUNTED_GETS);
    assert_counted(&before, &after, telemetry::Call::MultiGet, 1);
    assert_counted(&before, &after, telemetry::Call::TableInfo, 1);
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
    let rss = series_value(&after, metric::PROCESS_RSS_BYTES, &[])
        .expect("the sampler ran after the first loads");
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
        Some(response.value.to_vec()),
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
    let table = Arc::clone(&stack.serving.opened().tables[0]);
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
/// panic at the first tick of the refresh and sampler tasks; the
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
        PeriodicTask::Sampler => {}
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
    let table = Arc::clone(&stack.serving.opened().tables[0]);
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
        };
        async move { client.get(request).await.map(|_| ()) }
    });
    let mut client = stack.client.clone();
    let held_batch = tokio::spawn({
        let request = MultiGetRequest {
            table: TABLE.to_string(),
            keys: vec![wire_key(&keys[1])],
        };
        async move { client.multi_get(request).await.map(|_| ()) }
    });
    poll_until(SHUTDOWN_WAIT, "both calls reaching the store", async || {
        Ok((stack.s3.held() == IN_FLIGHT_CAP).then_some(()))
    })
    .await?;

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
    stack.s3.release();
    let response = stack.get(TABLE, Some(wire_key(&keys[2]))).await?;
    assert_eq!(response.presence, i32::from(v1::Presence::Present));
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

    for (sent, key) in keys[..2].iter().enumerate() {
        let mut get = tonic::Request::new(GetRequest {
            table: TABLE.to_string(),
            key: Some(wire_key(key)),
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
    Ok(())
}

/// A `MultiGet` of `max_multi_get_keys` keys is answered; one more key is
/// `INVALID_ARGUMENT` before any lookup, so it issues no GET.
#[tokio::test(flavor = "multi_thread")]
async fn test_multi_get_over_max_keys_is_invalid_argument_before_any_lookup() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::start_with(
        vec![FixtureSpec::small(TABLE, KeyType::Long)],
        async |setup| {
            setup.config.server.max_multi_get_keys = BATCH_IDS;
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
