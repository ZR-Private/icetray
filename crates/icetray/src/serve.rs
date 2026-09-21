//! The daemon (RFD 012, RFD 005): both listeners bound and the health
//! service registered before any table loads, every table loaded
//! concurrently, readiness and `SERVING` once every table serves, one
//! refresh task per table and one sampler on the poll interval, and a drain
//! on shutdown that finishes in-flight calls and flushes the page cache's
//! disk level. A first-load failure keeps health `NOT_SERVING` for a grace
//! period, so an orchestrator's probe sees it, then ends the run.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use icetray_core::config::Config;
use icetray_core::open::open_with;
use icetray_core::{ErrorKind, Opened, Storage, Table};
use icetray_proto::v1::icetray_server::IcetrayServer;
use metrics_exporter_prometheus::PrometheusHandle;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::MissedTickBehavior;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;
use tonic_health::ServingStatus;
use tonic_health::server::HealthReporter;

use crate::error::ServeError;
use crate::listen::serve_http;
use crate::service::Service;
use crate::telemetry;

/// How long a first-load failure stays visible on the health service before
/// the process exits: long enough for an orchestrator's probe, at the usual
/// ten-second period, to see `NOT_SERVING` beside the logged reason (RFD
/// 012), so the restart it then sees has a cause on record.
pub const START_FAILURE_GRACE: Duration = Duration::from_secs(10);

/// The gRPC health protocol's name for the server as a whole.
pub const OVERALL_SERVICE: &str = "";

/// What a caller of `Serving::start` chooses; the binary takes the defaults
/// and tests shorten the grace.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ServeOptions {
    /// How long a first-load failure stays on the health service before the
    /// run ends.
    pub start_failure_grace: Duration,
}

impl Default for ServeOptions {
    fn default() -> Self {
        Self {
            start_failure_grace: START_FAILURE_GRACE,
        }
    }
}

/// A running daemon: the addresses it bound, what it opened, and the
/// handles to wait on it and to stop it.
pub struct Serving {
    listen: SocketAddr,
    metrics: SocketAddr,
    opened: Arc<Opened>,
    ready: watch::Receiver<bool>,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<Result<(), ServeError>>,
}

impl Serving {
    /// Binds both listeners, registers health, opens every table, starts
    /// the gRPC and HTTP servers, then spawns the run that loads, refreshes,
    /// and drains. Storage comes from the SDK's default chain (RFD 003).
    pub async fn start(
        config: Config,
        handle: PrometheusHandle,
        options: ServeOptions,
    ) -> Result<Self, ServeError> {
        let storage = Arc::new(Storage::with_sdk_defaults(config.storage.clone()).await);
        Self::start_over(config, storage, handle, options).await
    }

    /// `start` over a storage the caller built, so a test registers its own
    /// stores and never touches the SDK chain. Returns once both listeners
    /// accept and the health service answers `NOT_SERVING`; the loads run
    /// behind it (RFD 012).
    pub async fn start_over(
        config: Config,
        storage: Arc<Storage>,
        handle: PrometheusHandle,
        options: ServeOptions,
    ) -> Result<Self, ServeError> {
        let grpc_listener = bind(config.server.listen).await?;
        let metrics_listener = bind(config.server.metrics_listen).await?;
        let listen = local_addr(&grpc_listener, config.server.listen)?;
        let metrics = local_addr(&metrics_listener, config.server.metrics_listen)?;

        let (health, health_service) = tonic_health::server::health_reporter();
        health.set_not_serving::<IcetrayServer<Service>>().await;
        health
            .set_service_status(OVERALL_SERVICE, ServingStatus::NotServing)
            .await;

        let opened = Arc::new(open_with(&config, None, storage).await?);
        let service = Service::new(&opened);
        let (shutdown, _stop) = watch::channel(false);
        let (ready_tx, ready) = watch::channel(false);

        let grpc = tokio::spawn(
            Server::builder()
                .add_service(health_service)
                .add_service(IcetrayServer::new(service))
                .serve_with_incoming_shutdown(
                    TcpIncoming::from(grpc_listener),
                    stop(shutdown.subscribe()),
                ),
        );
        let http = tokio::spawn(serve_http(
            metrics_listener,
            handle.clone(),
            ready.clone(),
            shutdown.subscribe(),
        ));
        tracing::info!(
            grpc = %listen,
            metrics = %metrics,
            tables = opened.tables.len(),
            "listening"
        );

        let run = Run {
            opened: Arc::clone(&opened),
            health,
            handle,
            options,
            poll_interval: config.server.poll_interval,
            shutdown: shutdown.clone(),
            ready: ready_tx,
            grpc,
            http,
        };
        let task = tokio::spawn(run.run());
        Ok(Self {
            listen,
            metrics,
            opened,
            ready,
            shutdown,
            task,
        })
    }

    /// The address the gRPC listener bound.
    pub fn listen_addr(&self) -> SocketAddr {
        self.listen
    }

    /// The address the metrics and readiness listener bound.
    pub fn metrics_addr(&self) -> SocketAddr {
        self.metrics
    }

    /// The storage, cache, local directory, and tables the daemon serves.
    pub fn opened(&self) -> &Arc<Opened> {
        &self.opened
    }

    /// Completes once every table serves a generation, or at once when the
    /// run has already ended, which a first-load failure does.
    pub async fn ready(&mut self) {
        let mut stopped = self.shutdown.subscribe();
        tokio::select! {
            _ = self.ready.wait_for(|ready| *ready) => {}
            _ = stopped.wait_for(|stop| *stop) => {}
        }
    }

    /// Completes when the run is ending: a caller asked for `shutdown`, or
    /// the run failed its first loads and signalled it itself.
    pub async fn stopped(&self) {
        stop(self.shutdown.subscribe()).await;
    }

    /// Asks the run to drain: the servers stop accepting, in-flight calls
    /// finish, the refresh tasks end, and the page cache's disk level is
    /// flushed. `join` reports how it went.
    pub fn shutdown(&self) {
        self.shutdown.send_modify(|stop| *stop = true);
    }

    /// Waits for the run to end: `Ok` after a drain, `FirstLoad` when a table
    /// failed its first load, or the failure that ended it.
    pub async fn join(self) -> Result<(), ServeError> {
        self.task.await?
    }
}

/// Binds `addr`, naming it in the failure.
async fn bind(addr: SocketAddr) -> Result<TcpListener, ServeError> {
    TcpListener::bind(addr)
        .await
        .map_err(|source| ServeError::Bind { addr, source })
}

/// The address `listener` bound, which differs from `addr` when `addr`'s
/// port is 0.
fn local_addr(listener: &TcpListener, addr: SocketAddr) -> Result<SocketAddr, ServeError> {
    listener
        .local_addr()
        .map_err(|source| ServeError::Bind { addr, source })
}

/// Completes when `shutdown` becomes true. A closed channel completes it
/// too: the sender lives in the `Serving` and its run, and when both are
/// gone whatever awaits this has nothing left to serve.
async fn stop(mut shutdown: watch::Receiver<bool>) {
    // intentional: a closed channel is the daemon gone, which ends the wait
    // the same way a shutdown does.
    let _ = shutdown.wait_for(|stop| *stop).await;
}

/// The run behind a `Serving`: the loads, the readiness flip, the periodic
/// tasks, and the drain.
struct Run {
    opened: Arc<Opened>,
    health: HealthReporter,
    handle: PrometheusHandle,
    options: ServeOptions,
    poll_interval: Duration,
    shutdown: watch::Sender<bool>,
    ready: watch::Sender<bool>,
    grpc: JoinHandle<Result<(), tonic::transport::Error>>,
    http: JoinHandle<()>,
}

impl Run {
    /// The loads, then either the failed start (the grace, the drain, and
    /// `FirstLoad`) or the serving run until shutdown. A shutdown asked for
    /// while the tables load is honoured as soon as the loads end: the grace
    /// is cut short, and a run whose loads succeeded drains without ever
    /// flipping to `SERVING`, since the servers stopped accepting when the
    /// shutdown was asked for.
    async fn run(self) -> Result<(), ServeError> {
        let failures = load_all(&self.opened).await;
        if !failures.is_empty() {
            tokio::select! {
                () = tokio::time::sleep(self.options.start_failure_grace) => {}
                () = stop(self.shutdown.subscribe()) => {}
            }
            self.shutdown.send_modify(|stop| *stop = true);
            if let Err(error) = self.drain().await {
                tracing::error!(%error, "drain after a failed start");
            }
            return Err(ServeError::FirstLoad { failures });
        }
        if *self.shutdown.borrow() {
            return self.drain().await;
        }

        retain(&self.opened).await;
        telemetry::sample(&self.opened, &self.handle);
        self.health.set_serving::<IcetrayServer<Service>>().await;
        self.health
            .set_service_status(OVERALL_SERVICE, ServingStatus::Serving)
            .await;
        self.ready.send_modify(|ready| *ready = true);

        let mut periodic = JoinSet::new();
        for table in &self.opened.tables {
            periodic.spawn(refresh_loop(
                Arc::clone(table),
                Arc::clone(&self.opened),
                self.poll_interval,
            ));
        }
        periodic.spawn(sample_loop(
            Arc::clone(&self.opened),
            self.handle.clone(),
            self.poll_interval,
        ));

        stop(self.shutdown.subscribe()).await;
        periodic.shutdown().await;
        self.drain().await
    }

    /// Waits for the servers to finish their in-flight work and closes the
    /// page cache, so the disk level is flushed before the process exits
    /// (RFD 017).
    async fn drain(self) -> Result<(), ServeError> {
        let Self {
            grpc, http, opened, ..
        } = self;
        grpc.await??;
        http.await?;
        opened.cache.close().await?;
        Ok(())
    }
}

/// Loads every table concurrently and logs each outcome; returns the
/// tables that failed with the kind of their failure.
async fn load_all(opened: &Opened) -> Vec<(String, ErrorKind)> {
    let outcomes = join_all(opened.tables.iter().map(|table| table.load())).await;
    let mut failures = Vec::new();
    for (table, outcome) in opened.tables.iter().zip(outcomes) {
        match outcome {
            Ok(()) => log_serving(table),
            Err(error) => {
                tracing::error!(
                    table = table.name(),
                    reason = %error.kind(),
                    %error,
                    "table failed to load"
                );
                failures.push((table.name().to_string(), error.kind()));
            }
        }
    }
    failures
}

/// One `info` event for the generation `table` serves.
fn log_serving(table: &Table) {
    if let Some(generation) = table.current() {
        tracing::info!(
            table = table.name(),
            snapshot_id = %generation.snapshot_id,
            files = generation.files.len(),
            records = generation.records,
            residency = %generation.residency,
            index_bytes = generation.index_bytes,
            "table serving"
        );
    }
}

/// Deletes the chunk files no table serves any more and logs what it freed;
/// a failure here leaves the daemon serving and is logged.
async fn retain(opened: &Opened) {
    match opened.retain().await {
        Ok(freed) if freed > 0 => tracing::info!(freed_bytes = freed, "chunk files retained"),
        Ok(_) => {}
        Err(error) => {
            tracing::warn!(reason = %error.kind(), %error, "chunk file retention failed");
        }
    }
}

/// An interval of `period` whose first tick is one period from now, not at
/// once, and which delays rather than bursts after a missed tick.
fn ticker(period: Duration) -> tokio::time::Interval {
    let start = tokio::time::Instant::now() + period;
    let mut interval = tokio::time::interval_at(start, period);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    interval
}

/// Polls `table`'s ref every `poll_interval` (RFD 005): a swap is logged and
/// followed by retention; a failure is logged with the snapshot still
/// served, which core keeps serving and counts under `refresh_errors_total`.
/// The poll's outcome is bound before anything else awaits, so the finished
/// poll future is dropped first and the loop stays `Send`.
async fn refresh_loop(table: Arc<Table>, opened: Arc<Opened>, poll_interval: Duration) {
    let mut ticker = ticker(poll_interval);
    loop {
        ticker.tick().await;
        let outcome = table.refresh().await;
        if report_refresh(&table, outcome) {
            retain(&opened).await;
        }
    }
}

/// Logs one poll's outcome; true when a new generation was swapped in.
fn report_refresh(table: &Table, outcome: icetray_core::Result<bool>) -> bool {
    match outcome {
        Ok(true) => {
            if let Some(generation) = table.current() {
                tracing::info!(
                    table = table.name(),
                    snapshot_id = %generation.snapshot_id,
                    "table advanced"
                );
            }
            true
        }
        Ok(false) => false,
        Err(error) => {
            if let Some(generation) = table.current() {
                tracing::warn!(
                    table = table.name(),
                    reason = %error.kind(),
                    %error,
                    snapshot_id = %generation.snapshot_id,
                    "refresh failed"
                );
            } else {
                tracing::warn!(
                    table = table.name(),
                    reason = %error.kind(),
                    %error,
                    "refresh failed"
                );
            }
            false
        }
    }
}

/// Samples the binary's gauges every `poll_interval`.
async fn sample_loop(opened: Arc<Opened>, handle: PrometheusHandle, poll_interval: Duration) {
    let mut ticker = ticker(poll_interval);
    loop {
        ticker.tick().await;
        telemetry::sample(&opened, &handle);
    }
}
