//! The daemon in-process over the in-process S3 server (RFD 014, P tier):
//! health and readiness before and after the first loads, the wire contract
//! of RFD 010 with its error mapping, request counting, refresh within the
//! poll interval, the drain, and the start-or-fail rule of RFD 005.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use hyper::StatusCode;
use icetray::listen::{METRICS_PATH, READY_PATH};
use icetray::serve::OVERALL_SERVICE;
use icetray::{ServeError, ServeOptions, Serving, telemetry};
use icetray_core::config::{Config, Residency, ServerConfig, TableConfig};
use icetray_core::key::UUID_BYTES;
use icetray_core::{ErrorKind, Generation, IoStats, Key, KeyType, RefusalReason, Storage, metric};
use icetray_proto::v1::icetray_client::IcetrayClient;
use icetray_proto::v1::icetray_server::SERVICE_NAME;
use icetray_proto::v1::{
    self, GetRequest, GetResponse, MultiGetRequest, MultiGetResponse, TableInfoRequest,
    TableInfoResponse,
};
use icetray_testkit::fixture::{NAMESPACE, SAMPLE, SMALL_BUCKETS, SMALL_IDS};
use icetray_testkit::verify::{expected_cold_gets, series_value};
use icetray_testkit::{Fixture, FixtureSpec, Nulls, S3Fixture, memory_catalog_s3, write_fixture};
use tonic::transport::Channel;
use tonic::{Code, Status};
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;

mod common;

use common::{BoxError, NULL_EVERY, NULL_ID, POLL_STEP, PRESENT_ID, SHUTDOWN_WAIT, http_get};

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

/// When the first-load test checks health during the grace.
const HALF_GRACE: Duration = Duration::from_millis(150);

/// The grace a failed first load stays on the health service in the test:
/// twice the check point, so the check falls halfway through.
const FAILURE_GRACE: Duration = HALF_GRACE.saturating_mul(2);

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

/// The ids of the fixture the health test loads: five times the small
/// fixture, so the load comfortably outlasts the first health round trip
/// and the readiness probe; the S3 fixture has no hook to hold a load open.
const HEALTH_RACE_IDS: std::ops::Range<u64> = 0..100_000;

/// The file prefix of the refresh test's append.
const APPEND_PREFIX: &str = "build2";

/// The scheme every S3 location under the fixture bucket carries.
const S3_SCHEME: &str = "s3://";

/// How long the drain test lets a spawned request reach the connection
/// before it asks for the drain: the client's h2 refuses to open a stream
/// once any GOAWAY has arrived, so a request must be on the wire, or
/// already answered, before the server starts closing.
const DISPATCH_DELAY: Duration = Duration::from_millis(20);

/// The `metrics` recorder is process-global, so `requests_total` is one
/// counter for every test in this binary. Every test that issues an RPC
/// holds this lock, so the counting test's deltas are exact.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// A running daemon over fixtures in the in-process S3 server, with the
/// clients to drive it and the storage counters to price it.
struct Stack {
    s3: S3Fixture,
    fixtures: Vec<Fixture>,
    serving: Serving,
    client: IcetrayClient<Channel>,
    health: HealthClient<Channel>,
    stats: Arc<IoStats>,
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
        Self::serve(s3, fixtures, tables, ServeOptions::default()).await
    }

    /// Serves `tables` over `s3`, whose objects `fixtures` wrote, and
    /// connects the clients; the loads run behind the returned stack.
    async fn serve(
        s3: S3Fixture,
        fixtures: Vec<Fixture>,
        tables: Vec<TableConfig>,
        options: ServeOptions,
    ) -> Result<Self, BoxError> {
        let config = config_over(&s3, tables);
        let storage = Arc::new(Storage::new(s3.storage_config()));
        let stats = storage.stats();
        let serving = Serving::start_over(config, storage, telemetry::install()?, options).await?;
        let channel = Channel::from_shared(format!("http://{}", serving.listen_addr()))?
            .connect()
            .await?;
        Ok(Self {
            s3,
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
        let response = self
            .health
            .check(HealthCheckRequest {
                service: service.to_string(),
            })
            .await?
            .into_inner();
        Ok(ServingStatus::try_from(response.status)?)
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
fn calls(exposition: &str, call: telemetry::Call) -> u64 {
    series_value(
        exposition,
        metric::REQUESTS_TOTAL,
        &[(metric::LABEL_CALL, &call.to_string())],
    )
    .unwrap_or(0)
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
    let spec = FixtureSpec {
        ids: HEALTH_RACE_IDS,
        ..FixtureSpec::small(TABLE, KeyType::Long)
    };
    let mut stack = Stack::start(vec![spec]).await?;
    assert_eq!(
        stack.health_status(OVERALL_SERVICE).await?,
        ServingStatus::NotServing,
        "the listener accepts before the load completes"
    );
    let (status, body) = stack.http_get(READY_PATH).await?;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body, "not ready");

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
    assert_eq!(body, "ready");
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
    assert_eq!(info.index_bytes, u64::try_from(generation.index_bytes)?);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_costs_one_get_cold_and_none_warm_or_absent() -> TestResult {
    let _serial = SERIAL.lock().await;
    let mut stack = Stack::ready_small().await?;
    let generation = stack.generation();
    let keys = stack.fixtures[0].keys_at(&SAMPLE[..COLD_IDS]);
    let expected = expected_cold_gets(&generation, &keys)?;
    assert_eq!(expected, u64::try_from(COLD_IDS)?, "the ids share no page");

    let before = stack.stats.gets();
    for key in &keys {
        let response = stack.get(TABLE, Some(wire_key(key))).await?;
        assert_eq!(response.presence, i32::from(v1::Presence::Present), "{key}");
        assert_eq!(Some(response.value), stack.fixtures[0].value(key), "{key}");
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
    assert_eq!(Some(present.value), stack.fixtures[0].value(&present_key));

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
            Some(hit.value.clone()),
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
    assert_eq!(Some(response.value), stack.fixtures[1].value(&uuid_key));
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
    assert_eq!(
        calls(&after, telemetry::Call::Get),
        calls(&before, telemetry::Call::Get) + COUNTED_GETS
    );
    assert_eq!(
        calls(&after, telemetry::Call::MultiGet),
        calls(&before, telemetry::Call::MultiGet) + 1
    );
    assert_eq!(
        calls(&after, telemetry::Call::TableInfo),
        calls(&before, telemetry::Call::TableInfo) + 1
    );
    assert!(
        after
            .lines()
            .any(|line| line.starts_with(metric::PROCESS_RSS_BYTES)),
        "the sampler ran after the first loads"
    );
    let age_series = format!(
        "{}{{{}=\"{TABLE}\"}}",
        metric::GENERATION_AGE_SECONDS,
        metric::LABEL_TABLE
    );
    assert!(
        after.lines().any(|line| line.starts_with(&age_series)),
        "{age_series} is sampled"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_follows_the_table_within_the_poll_interval() -> TestResult {
    let _serial = SERIAL.lock().await;
    let s3 = S3Fixture::start(BUCKET).await?;
    let catalog = memory_catalog_s3(&s3, WAREHOUSE).await?;
    let fixture = write_fixture(
        catalog.as_ref(),
        NAMESPACE,
        FixtureSpec::small(TABLE, KeyType::Long),
    )
    .await?;
    fixture.write_hint().await?;
    let followed = TableConfig {
        source: fixture.root_source(),
        ..fixture.config(Residency::Full, false)
    };
    let mut stack =
        Stack::serve(s3, vec![fixture], vec![followed], ServeOptions::default()).await?;
    stack.serving.ready().await;
    let first = stack.table_info(TABLE).await?.snapshot_id;

    stack.fixtures[0]
        .append(catalog.as_ref(), NEW_IDS, APPEND_PREFIX)
        .await?;
    stack.fixtures[0].write_hint().await?;
    let deadline = tokio::time::Instant::now() + REFRESH_WAIT;
    let advanced = loop {
        let info = stack.table_info(TABLE).await?;
        if info.snapshot_id != first {
            break info;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the table did not advance within {REFRESH_WAIT:?}"
        );
        tokio::time::sleep(POLL_STEP).await;
    };
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
    assert_eq!(Some(response.value), stack.fixtures[0].value(&new_key));
    assert_eq!(response.snapshot_id, advanced.snapshot_id);
    Ok(())
}

/// A `Get` issued before `shutdown` and awaited after it completes, and the
/// drain ends `Ok`. The cold `Get` over loopback S3 is answered within the
/// dispatch delay, so what the drain does with a stream still open when the
/// shutdown arrives is not held under test here: the S3 fixture has no hook
/// to stall a read, which a proof of that would need.
#[tokio::test(flavor = "multi_thread")]
async fn test_shutdown_drains_and_returns_ok() -> TestResult {
    let _serial = SERIAL.lock().await;
    let stack = Stack::ready_small().await?;
    let key = stack.fixtures[0].key(SAMPLE[0]);
    let mut client = stack.client.clone();
    let in_flight = tokio::spawn(async move {
        client
            .get(GetRequest {
                table: TABLE.to_string(),
                key: Some(wire_key(&key)),
            })
            .await
    });
    tokio::time::sleep(DISPATCH_DELAY).await;
    stack.serving.shutdown();
    let response = in_flight.await??.into_inner();
    assert_eq!(response.presence, i32::from(v1::Presence::Present));
    tokio::time::timeout(SHUTDOWN_WAIT, stack.serving.join()).await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_first_load_failure_stays_visible_then_fails() -> TestResult {
    let _serial = SERIAL.lock().await;
    let s3 = S3Fixture::start(BUCKET).await?;
    let catalog = memory_catalog_s3(&s3, WAREHOUSE).await?;
    let fixture = write_fixture(
        catalog.as_ref(),
        NAMESPACE,
        FixtureSpec::small(TABLE, KeyType::Long),
    )
    .await?;
    // Disk residency with no `storage.local`: refused as `NoLocalDir` at
    // load. The configuration is built directly, so nothing validates it
    // before the load does.
    let disk = fixture.config(Residency::Disk, false);
    let options = ServeOptions {
        start_failure_grace: FAILURE_GRACE,
    };
    let mut stack = Stack::serve(s3, vec![fixture], vec![disk], options).await?;

    assert_eq!(
        stack.health_status(OVERALL_SERVICE).await?,
        ServingStatus::NotServing
    );
    tokio::time::sleep(HALF_GRACE).await;
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
    assert_eq!(
        failures,
        vec![(
            TABLE.to_string(),
            ErrorKind::Refused(RefusalReason::NoLocalDir)
        )]
    );
    Ok(())
}

/// A shutdown asked for while the first loads run is honoured as soon as
/// they end: a failed start whose grace is `SHUTDOWN_WAIT` long reports its
/// failure well inside the grace instead of sleeping it out.
#[tokio::test(flavor = "multi_thread")]
async fn test_shutdown_during_the_grace_ends_the_run_without_waiting() -> TestResult {
    let _serial = SERIAL.lock().await;
    let s3 = S3Fixture::start(BUCKET).await?;
    let catalog = memory_catalog_s3(&s3, WAREHOUSE).await?;
    let fixture = write_fixture(
        catalog.as_ref(),
        NAMESPACE,
        FixtureSpec::small(TABLE, KeyType::Long),
    )
    .await?;
    let disk = fixture.config(Residency::Disk, false);
    let options = ServeOptions {
        start_failure_grace: SHUTDOWN_WAIT,
    };
    let stack = Stack::serve(s3, vec![fixture], vec![disk], options).await?;
    stack.serving.shutdown();
    let outcome = tokio::time::timeout(REFRESH_WAIT, stack.serving.join()).await?;
    let Err(ServeError::FirstLoad { failures }) = outcome else {
        panic!("expected a first-load failure, got {outcome:?}");
    };
    assert_eq!(failures.len(), 1);
    Ok(())
}
