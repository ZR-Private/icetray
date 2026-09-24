//! The daemon (RFD 012, RFD 005): both listeners bound and the health
//! service registered before any table loads, every table loaded
//! concurrently, readiness and `SERVING` once every table serves, one
//! refresh task per table and one sampler on the poll interval, and a drain
//! on shutdown that ends health watches, finishes in-flight calls, and
//! flushes the page cache's disk level. A first-load failure keeps health
//! `NOT_SERVING` for a grace period, so an orchestrator's probe sees it,
//! then ends the run; a periodic task that panics ends the run at once,
//! with the task named.

use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use icetray_core::config::{Config, ServerConfig};
use icetray_core::open::open_with;
use icetray_core::{ErrorKind, Opened, Storage, Table, open};
use icetray_proto::v1::icetray_server::{IcetrayServer, SERVICE_NAME};
use metrics_exporter_prometheus::PrometheusHandle;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::{Id, JoinError, JoinHandle, JoinSet};
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

/// Every name the health service answers for: the server as a whole and
/// the `Icetray` service, which change status together.
const HEALTH_NAMES: [&str; 2] = [OVERALL_SERVICE, SERVICE_NAME];

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

/// Both listeners, bound before any table is opened, so a port that is
/// taken is reported before a local directory or a cache is touched.
struct Listeners {
    grpc: TcpListener,
    metrics: TcpListener,
    /// The address the gRPC listener bound.
    listen: SocketAddr,
    /// The address the metrics and readiness listener bound.
    metrics_addr: SocketAddr,
}

impl Listeners {
    /// Binds `server`'s two addresses, naming the one that fails.
    async fn bind(server: &ServerConfig) -> Result<Self, ServeError> {
        let grpc = bind(server.listen).await?;
        let metrics = bind(server.metrics_listen).await?;
        let listen = local_addr(&grpc, server.listen)?;
        let metrics_addr = local_addr(&metrics, server.metrics_listen)?;
        Ok(Self {
            grpc,
            metrics,
            listen,
            metrics_addr,
        })
    }
}

impl Serving {
    /// Binds both listeners, opens every table through `open` (storage from
    /// the SDK's default chain, RFD 003), registers health, starts the gRPC
    /// and HTTP servers, then spawns the run that loads, refreshes, and
    /// drains. Returns once both listeners accept and the health service
    /// answers `NOT_SERVING`; the loads run behind it (RFD 012).
    pub async fn start(
        config: Config,
        handle: PrometheusHandle,
        options: ServeOptions,
    ) -> Result<Self, ServeError> {
        let listeners = Listeners::bind(&config.server).await?;
        let opened = open(&config, None).await?;
        Ok(Self::start_serving(config, listeners, opened, handle, options).await)
    }

    /// `start` over a storage the caller built, through `open_with`, so a
    /// test registers its own stores and never touches the SDK chain.
    pub async fn start_over(
        config: Config,
        storage: Arc<Storage>,
        handle: PrometheusHandle,
        options: ServeOptions,
    ) -> Result<Self, ServeError> {
        let listeners = Listeners::bind(&config.server).await?;
        let opened = open_with(&config, None, storage).await?;
        Ok(Self::start_serving(config, listeners, opened, handle, options).await)
    }

    /// Registers health as `NOT_SERVING`, starts the servers on `listeners`
    /// over `opened`, logs `listening`, and spawns the run.
    async fn start_serving(
        config: Config,
        listeners: Listeners,
        opened: Opened,
        handle: PrometheusHandle,
        options: ServeOptions,
    ) -> Self {
        let (health, health_service) = tonic_health::server::health_reporter();
        set_health(&health, ServingStatus::NotServing).await;

        let opened = Arc::new(opened);
        let service = Service::new(&opened);
        let (shutdown, _stop) = watch::channel(false);
        let (ready_tx, ready) = watch::channel(false);

        let grpc = tokio::spawn(
            Server::builder()
                .add_service(health_service)
                .add_service(IcetrayServer::new(service))
                .serve_with_incoming_shutdown(
                    TcpIncoming::from(listeners.grpc),
                    stop(shutdown.subscribe()),
                ),
        );
        let http = tokio::spawn(serve_http(
            listeners.metrics,
            handle.clone(),
            ready.clone(),
            shutdown.subscribe(),
        ));
        tracing::info!(
            grpc = %listeners.listen,
            metrics = %listeners.metrics_addr,
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
        Self {
            listen: listeners.listen,
            metrics: listeners.metrics_addr,
            opened,
            ready,
            shutdown,
            task,
        }
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

/// A task the run keeps on the poll interval, named so the failure that
/// ends one says which. Display gives the name the log and `ServeError`
/// carry.
#[derive(Debug, PartialEq, Eq)]
pub enum PeriodicTask {
    /// The refresh of one table's ref.
    Refresh {
        /// The table's configured name.
        table: String,
    },
    /// The sampler of the binary's gauges.
    Sampler,
}

impl fmt::Display for PeriodicTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Refresh { table } => write!(f, "refresh of table {table}"),
            Self::Sampler => f.write_str("sampler"),
        }
    }
}

/// The run's periodic tasks, each under its name. Every loop runs until it
/// is aborted, so a task that ends on its own ends by a `JoinError`: a
/// panic inside it.
struct Periodic {
    tasks: JoinSet<Infallible>,
    names: HashMap<Id, PeriodicTask>,
}

impl Periodic {
    fn new() -> Self {
        Self {
            tasks: JoinSet::new(),
            names: HashMap::new(),
        }
    }

    /// Spawns `future` as `task`.
    fn spawn(
        &mut self,
        task: PeriodicTask,
        future: impl Future<Output = Infallible> + Send + 'static,
    ) {
        let id = self.tasks.spawn(future).id();
        self.names.insert(id, task);
    }

    /// Completes when a task ends, naming it. The sampler is in the set
    /// until `shutdown`, so there is always a task to wait on.
    async fn ended(&mut self) -> (PeriodicTask, JoinError) {
        let outcome = self
            .tasks
            .join_next()
            .await
            .expect("the sampler is in the set until shutdown");
        match outcome {
            Ok(never) => match never {},
            Err(error) => {
                let task = self
                    .names
                    .remove(&error.id())
                    .expect("every task in the set was spawned under a name");
                (task, error)
            }
        }
    }

    /// Aborts every task and waits for them to end.
    async fn shutdown(&mut self) {
        self.tasks.shutdown().await;
    }
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
    /// shutdown was asked for. A periodic task that ends before the shutdown
    /// ends the run as `Periodic`, so the process exits non-zero instead of
    /// serving on with nothing refreshing.
    async fn run(self) -> Result<(), ServeError> {
        let failures = load_all(&self.opened).await;
        if !failures.is_empty() {
            tokio::select! {
                () = tokio::time::sleep(self.options.start_failure_grace) => {}
                () = stop(self.shutdown.subscribe()) => {}
            }
            return self.fail(ServeError::FirstLoad { failures }).await;
        }
        if *self.shutdown.borrow() {
            return self.drain().await;
        }

        retain(&self.opened).await;
        telemetry::sample(&self.opened, &self.handle);
        set_health(&self.health, ServingStatus::Serving).await;
        self.ready.send_modify(|ready| *ready = true);

        let mut periodic = Periodic::new();
        for table in &self.opened.tables {
            periodic.spawn(
                PeriodicTask::Refresh {
                    table: table.name().to_string(),
                },
                refresh_loop(
                    Arc::clone(table),
                    Arc::clone(&self.opened),
                    self.poll_interval,
                ),
            );
        }
        periodic.spawn(
            PeriodicTask::Sampler,
            sample_loop(
                Arc::clone(&self.opened),
                self.handle.clone(),
                self.poll_interval,
            ),
        );

        let ended = tokio::select! {
            () = stop(self.shutdown.subscribe()) => None,
            ended = periodic.ended() => Some(ended),
        };
        periodic.shutdown().await;
        let Some((task, source)) = ended else {
            return self.drain().await;
        };
        tracing::error!(%task, %source, "periodic task ended");
        self.fail(ServeError::Periodic { task, source }).await
    }

    /// Ends the run with `error`: the shutdown is signalled, so the servers
    /// and the readiness listener stop, and the drain's own failure is
    /// logged, since `error` is the outcome that names the cause.
    async fn fail(self, error: ServeError) -> Result<(), ServeError> {
        self.shutdown.send_modify(|stop| *stop = true);
        if let Err(drain) = self.drain().await {
            tracing::error!(error = %drain, "drain after a failed run");
        }
        Err(error)
    }

    /// Ends every health watch, waits for the servers to finish their
    /// in-flight work, and closes the page cache, so the disk level is
    /// flushed before the process exits (RFD 017). tonic's graceful shutdown
    /// waits for every connection to close, and a `Watch` stream the health
    /// service still feeds would hold its connection open without end.
    async fn drain(self) -> Result<(), ServeError> {
        let Self {
            mut health,
            grpc,
            http,
            opened,
            ..
        } = self;
        withdraw_health(&mut health).await;
        grpc.await??;
        http.await?;
        opened.cache.close().await?;
        Ok(())
    }
}

/// Sets every health name to `status`, which every watch of it is sent.
async fn set_health(health: &HealthReporter, status: ServingStatus) {
    for name in HEALTH_NAMES {
        health.set_service_status(name, status).await;
    }
}

/// Sets every health name `NOT_SERVING`, then removes it. Removing a name
/// drops its status sender, so every `Watch` stream on it ends after the
/// `NOT_SERVING` it has not yet sent; a check or watch that arrives later
/// is answered `NOT_FOUND` at once instead of opening a stream.
async fn withdraw_health(health: &mut HealthReporter) {
    set_health(health, ServingStatus::NotServing).await;
    for name in HEALTH_NAMES {
        health.clear_service_status(name).await;
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
async fn refresh_loop(
    table: Arc<Table>,
    opened: Arc<Opened>,
    poll_interval: Duration,
) -> Infallible {
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
async fn sample_loop(
    opened: Arc<Opened>,
    handle: PrometheusHandle,
    poll_interval: Duration,
) -> Infallible {
    let mut ticker = ticker(poll_interval);
    loop {
        ticker.tick().await;
        telemetry::sample(&opened, &handle);
    }
}
