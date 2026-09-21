//! What both integration test binaries need: a plain HTTP/1 `GET` against
//! the metrics and readiness listener, the error type the tests plumb with
//! `?`, and the constants both drive their fixtures and their waits with.

use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt as _, Empty};
use hyper::header::HOST;
use hyper::{Request, StatusCode};
use hyper_util::rt::TokioIo;

/// Any failure a test can meet; the tests assert on typed outcomes and use
/// `?` for the plumbing.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// The namespace every fixture is written in.
pub const NAMESPACE: &str = "ns";

/// How often a test polls while waiting for readiness, a refresh, or an exit.
pub const POLL_STEP: Duration = Duration::from_millis(50);

/// How long a drain or a failed start may take before the test fails.
pub const SHUTDOWN_WAIT: Duration = Duration::from_secs(10);

/// Every id that is a multiple of this has a null value in the nulls fixture.
pub const NULL_EVERY: u64 = 1_000;

/// A `SAMPLE` id that is a multiple of `NULL_EVERY`, and one that is not.
pub const NULL_ID: u64 = 5_000;
pub const PRESENT_ID: u64 = 2_134;

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
