//! What both integration test binaries need: a plain HTTP/1 `GET` against
//! the metrics and readiness listener, one health check, one deadline poll,
//! the error type the tests plumb with `?`, and the constants both drive
//! their fixtures and their waits with.

use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt as _, Empty};
use hyper::header::HOST;
use hyper::{Request, StatusCode};
use hyper_util::rt::TokioIo;
use icetray_core::contract::PAGE_ROWS;
use icetray_testkit::fixture::SAMPLE;
use tonic::transport::Channel;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;

/// Any failure a test can meet; the tests assert on typed outcomes and use
/// `?` for the plumbing.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// How often a test polls while waiting for readiness, a refresh, or an exit.
pub const POLL_STEP: Duration = Duration::from_millis(50);

/// How long a drain or a failed start may take before the test fails.
pub const SHUTDOWN_WAIT: Duration = Duration::from_secs(10);

/// Every id that is a multiple of this has a null value in the nulls
/// fixture: the contract's page row count, so with ids written in order a
/// null value opens every page after the first and the `get` null path is
/// exercised at a page edge.
pub const NULL_EVERY: u64 = PAGE_ROWS.unsigned_abs();

/// The `SAMPLE` id the nulls fixture holds with a null value: a multiple of
/// `NULL_EVERY`.
pub const NULL_ID: u64 = SAMPLE[4];

/// A `SAMPLE` id the nulls fixture holds with a value: not a multiple of
/// `NULL_EVERY`.
pub const PRESENT_ID: u64 = SAMPLE[2];

const _: () = assert!(NULL_ID.is_multiple_of(NULL_EVERY));
const _: () = assert!(!PRESENT_ID.is_multiple_of(NULL_EVERY));

/// `GET path` at `addr` over HTTP/1: the status and the body.
pub async fn http_get(addr: SocketAddr, path: &str) -> Result<(StatusCode, String), BoxError> {
    let stream = tokio::net::TcpStream::connect(addr).await?;
    let (mut sender, connection) =
        hyper::client::conn::http1::handshake::<_, Empty<Bytes>>(TokioIo::new(stream)).await?;
    // intentional: the connection outlives one request only; an error after
    // the response was read is nobody's failure.
    tokio::spawn(connection);
    let request = Request::builder()
        .uri(path)
        .header(HOST, addr.to_string())
        .body(Empty::new())?;
    let response = sender.send_request(request).await?;
    let status = response.status();
    let body = response.into_body().collect().await?.to_bytes();
    Ok((status, String::from_utf8(body.to_vec())?))
}

/// The status the health service answers for `service` over `client`.
pub async fn health_status(
    client: &mut HealthClient<Channel>,
    service: &str,
) -> Result<ServingStatus, BoxError> {
    let response = client
        .check(HealthCheckRequest {
            service: service.to_string(),
        })
        .await?
        .into_inner();
    Ok(ServingStatus::try_from(response.status)?)
}

/// Polls `probe` every `POLL_STEP` until it yields `Some`, which is the
/// result, or `wait` has passed, which fails the test naming `what` did not
/// happen. A probe's failure fails the test at once.
pub async fn poll_until<T>(
    wait: Duration,
    what: &str,
    mut probe: impl AsyncFnMut() -> Result<Option<T>, BoxError>,
) -> Result<T, BoxError> {
    let deadline = tokio::time::Instant::now() + wait;
    loop {
        if let Some(found) = probe().await? {
            return Ok(found);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!("{what} did not happen within {wait:?}").into());
        }
        tokio::time::sleep(POLL_STEP).await;
    }
}
