//! The metrics and readiness listener (RFD 012): one HTTP/1 accept loop of
//! the binary's own, serving `GET /metrics` from the Prometheus recorder
//! and `GET /ready` from the readiness watch. Readiness is a fact about the
//! first loads: true once every table served a generation, and it stays
//! true (RFD 005: a later refresh failure keeps the previous generation).

use std::convert::Infallible;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::header::CONTENT_TYPE;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinSet;

use crate::telemetry::Scrape;

/// The path the Prometheus exposition is served on.
pub const METRICS_PATH: &str = "/metrics";

/// The path readiness is served on: 200 when every table serves, 503 before.
pub const READY_PATH: &str = "/ready";

/// The content type of the Prometheus text exposition format, version 0.0.4.
pub const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// The body of a 200 on `READY_PATH`.
pub const READY_BODY: &str = "ready";

/// The body of a 503 on `READY_PATH`.
pub const NOT_READY_BODY: &str = "not ready";

/// How long the accept loop pauses after `accept` fails before trying again:
/// long enough that a descriptor shortage does not spin a worker, short
/// enough that a readiness probe at the usual period still finds the
/// listener answering.
const ACCEPT_RETRY_DELAY: Duration = Duration::from_millis(100);

/// Serves `/metrics` (through `Scrape::render`) and `/ready` on
/// `listener` until `shutdown` becomes true, then aborts the open connections and returns. Only the shutdown
/// ends the loop: an `accept` that fails (a connection aborted before it was
/// accepted, a descriptor shortage) is logged and retried after
/// `ACCEPT_RETRY_DELAY`, since a readiness probe that finds the port closed
/// would restart a replica whose gRPC service is healthy (RFD 012). Every
/// connection is its own task; finished ones are joined as new ones arrive,
/// so the set does not grow with the connection count.
pub async fn serve_http(
    listener: TcpListener,
    scrape: Scrape,
    ready: watch::Receiver<bool>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut connections = JoinSet::new();
    loop {
        let accepted = tokio::select! {
            accepted = listener.accept() => accepted,
            // A closed watch means the daemon is gone, which ends the loop too.
            _ = shutdown.wait_for(|stop| *stop) => break,
        };
        let stream = match accepted {
            Ok((stream, _peer)) => stream,
            Err(error) => {
                tracing::warn!(%error, "metrics listener failed to accept a connection");
                tokio::time::sleep(ACCEPT_RETRY_DELAY).await;
                continue;
            }
        };
        while let Some(joined) = connections.try_join_next() {
            if let Err(error) = joined {
                tracing::warn!(%error, "metrics connection task failed");
            }
        }
        let scrape = scrape.clone();
        let ready = ready.clone();
        connections.spawn(async move {
            let service = service_fn(move |request| {
                std::future::ready(Ok::<_, Infallible>(respond(&request, &scrape, &ready)))
            });
            if let Err(error) = http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await
            {
                // A scraper closing its connection ends it with an error that
                // is nobody's failure.
                tracing::debug!(%error, "metrics connection ended");
            }
        });
    }
    connections.shutdown().await;
}

/// One response: the exposition, the readiness, or 404.
fn respond(
    request: &Request<Incoming>,
    scrape: &Scrape,
    ready: &watch::Receiver<bool>,
) -> Response<Full<Bytes>> {
    let response = match request.uri().path() {
        METRICS_PATH => Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, PROMETHEUS_CONTENT_TYPE)
            .body(Full::from(scrape.render())),
        READY_PATH => {
            let (status, body) = if *ready.borrow() {
                (StatusCode::OK, READY_BODY)
            } else {
                (StatusCode::SERVICE_UNAVAILABLE, NOT_READY_BODY)
            };
            Response::builder()
                .status(status)
                .body(Full::new(Bytes::from_static(body.as_bytes())))
        }
        _ => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Full::default()),
    };
    response.expect("a status code and one constant header form a valid response")
}
