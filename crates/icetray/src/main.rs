//! The one binary. `serve` is the daemon: the gRPC service, the table
//! lifecycle, the poll loop, and the twelve-factor surface of RFD 012.
//! `doctor`, `get`, and `info` are one-shot subcommands that print JSON
//! lines. This file parses the command line, raises the open-file limit,
//! builds the runtime, and turns the library's typed outcomes into exit
//! codes; everything else is in the library.

use std::io::{self, Write as _};
use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;

use clap::Parser as _;
use icetray::cli::{Cli, Command, ConfigPath, Target};
use icetray::error::{CliError, EXIT_FAILURE, EXIT_OK, EXIT_USAGE, render};
use icetray::logging::{self, LogSink};
use icetray::reload::{ConfigFile, Reload};
use icetray::serve::{ServeOptions, Serving};
use icetray::{oneshot, telemetry};
use icetray_core::config::Config;
use icetray_core::descriptors;
use tokio::signal::unix::{Signal, SignalKind, signal};
use tracing::Level;

/// jemalloc, with background purging turned on at start, so memory freed
/// after a load (shed key chunks, dropped generations) returns to the
/// operating system.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() -> ExitCode {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => {
            // intentional: a print that fails has lost stderr, and there is
            // nowhere left to report it.
            let _ = error.print();
            let code = if error.use_stderr() {
                EXIT_USAGE
            } else {
                EXIT_OK
            };
            return ExitCode::from(code);
        }
    };
    let raised = descriptors::raise();
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            write_stderr(&render(
                &format_args!("cannot start the runtime: {error}"),
                None,
            ));
            return ExitCode::from(EXIT_FAILURE);
        }
    };
    runtime.block_on(run(cli, raised))
}

/// Dispatches the parsed command line; `raised` is the open-file limit
/// `descriptors::raise` left in force, or why it could not.
async fn run(cli: Cli, raised: io::Result<u64>) -> ExitCode {
    match cli.command {
        Command::Serve { config } => serve(config.as_deref(), raised).await,
        Command::Doctor {
            target,
            table,
            files,
        } => {
            run_oneshot(&target, table, raised, |config, table| async move {
                oneshot::doctor(&config, table.as_deref(), files).await
            })
            .await
        }
        Command::Get {
            target,
            table,
            keys,
        } => {
            run_oneshot(&target, table, raised, |config, table| async move {
                oneshot::get(&config, table.as_deref(), &keys)
                    .await
                    .map(|()| ExitCode::from(EXIT_OK))
            })
            .await
        }
        Command::Info { target, table } => {
            run_oneshot(&target, table, raised, |config, table| async move {
                oneshot::info(&config, table.as_deref())
                    .await
                    .map(|()| ExitCode::from(EXIT_OK))
            })
            .await
        }
    }
}

/// One one-shot subcommand: its configuration prepared from `target` for a
/// run that selects `table`, `subcommand` run over both, and the outcome
/// finished into the exit code.
async fn run_oneshot<Fut>(
    target: &Target,
    table: Option<String>,
    raised: io::Result<u64>,
    subcommand: impl FnOnce(Config, Option<String>) -> Fut,
) -> ExitCode
where
    Fut: Future<Output = Result<ExitCode, CliError>>,
{
    match prepare(target, table.as_deref(), raised) {
        Ok(config) => finish(subcommand(config, table).await),
        Err(code) => code,
    }
}

/// Reports `error` on stderr as plain text with its hint, the form of a
/// one-shot's every failure and of `serve`'s before its logging starts, and
/// gives its exit code.
fn report(error: &CliError) -> ExitCode {
    write_stderr(&render(error, error.hint().as_ref()));
    error.exit_code()
}

/// Writes `text` to stderr. A write that fails is ignored, so the exit
/// code stays the failure's own rather than a panic's.
fn write_stderr(text: &str) {
    // intentional: a write that fails has lost stderr, and there is
    // nowhere left to report it.
    let _ = io::stderr().lock().write_all(text.as_bytes());
}

/// The one-shot's configuration for a run that selects `table`, with
/// logging to stderr at `warn` under its `server.log`, since a one-shot's
/// stdout is data, and the open-file limit `raised` logged at `debug`. A
/// target that cannot be used is reported with its exit code.
fn prepare(
    target: &Target,
    table: Option<&str>,
    raised: io::Result<u64>,
) -> Result<Config, ExitCode> {
    let config = target.config(table).map_err(|error| report(&error))?;
    logging::init(&config.server.log, LogSink::Stderr, Level::WARN);
    log_descriptor_limit(raised, &config, Level::DEBUG);
    Ok(config)
}

/// Logs the open-file limit `raised` left in force, at `level` (`info` for
/// `serve`, `debug` for a one-shot), or at `warn` why it could not be
/// raised, and warns, with both numbers, when the limit is below the
/// descriptors `config`'s disk level and a load of each of its tables at
/// once need (`descriptors::needed_by`).
fn log_descriptor_limit(raised: io::Result<u64>, config: &Config, level: Level) {
    let limit = match raised {
        Ok(limit) => {
            if level == Level::INFO {
                tracing::info!(limit, "open-file limit");
            } else {
                tracing::debug!(limit, "open-file limit");
            }
            limit
        }
        Err(error) => {
            let limit = descriptors::limit();
            tracing::warn!(%error, limit, "cannot raise the open-file limit");
            limit
        }
    };
    let needed = descriptors::needed_by(config);
    if limit < needed {
        tracing::warn!(
            limit,
            needed,
            "the open-file limit is below the descriptors the page cache's disk level and loads need"
        );
    }
}

/// A one-shot's outcome as the exit code: a failure is reported as plain
/// text, as before logging started, so a mistake reads the same however
/// far the run got; warnings logged on the way stay log events. A closed
/// stdout ends the run quietly with exit 0.
fn finish(outcome: Result<ExitCode, CliError>) -> ExitCode {
    match outcome {
        Ok(code) => code,
        Err(CliError::BrokenPipe) => CliError::BrokenPipe.exit_code(),
        Err(error) => report(&error),
    }
}

/// The daemon's outcome as the exit code: a failure is logged, since once
/// its logging starts the daemon reports through its log stream (RFD 012).
fn finish_logged(outcome: Result<ExitCode, CliError>) -> ExitCode {
    outcome.unwrap_or_else(|error| {
        tracing::error!(%error, "run failed");
        error.exit_code()
    })
}

/// `serve`: the configuration from `--config`, given as `flag`, or from
/// `ICETRAY_CONFIG`, logging to stdout at `info` (twelve-factor: the log
/// stream is stdout), the open-file limit `raised` logged, jemalloc's
/// background purging, then the daemon following the file (RFD 018), whose
/// failure is logged.
async fn serve(flag: Option<&Path>, raised: io::Result<u64>) -> ExitCode {
    let Some(file) = ConfigPath::resolve(flag) else {
        return report(&CliError::NoConfig);
    };
    let reading = match file.read() {
        Ok(reading) => reading,
        Err(error) => return report(&error),
    };
    let config = match reading.config {
        Ok(config) => config,
        Err(rejected) => return report(&CliError::from(rejected)),
    };
    logging::init(&config.server.log, LogSink::Stdout, Level::INFO);
    log_descriptor_limit(raised, &config, Level::INFO);
    if let Err(error) = tikv_jemalloc_ctl::background_thread::write(true) {
        // Background purging is a Linux feature of jemalloc, so only there
        // does its absence mean something is wrong; elsewhere freed memory
        // returns to the operating system on the allocator's own schedule.
        if cfg!(target_os = "linux") {
            tracing::warn!(%error, "jemalloc background purging is unavailable");
        } else {
            tracing::debug!(%error, "jemalloc background purging is unavailable");
        }
    }
    let reload = Reload {
        source: Arc::new(ConfigFile::new(file)),
        started_from: reading.digest,
    };
    finish_logged(daemon(config, reload).await)
}

/// The metrics recorder, the daemon following `reload`, a reload on
/// SIGHUP, and a drain on SIGTERM or SIGINT. `EXIT_OK` after a drain; a
/// failure to start or a failed run, which a first-load failure is on its
/// own (RFD 005), and a drain that passed its deadline, are
/// `CliError::Serve`.
async fn daemon(config: Config, reload: Reload) -> Result<ExitCode, CliError> {
    let handle = telemetry::install()?;
    let hangup = signal(SignalKind::hangup());
    let options = ServeOptions {
        reload: Some(reload),
        ..ServeOptions::default()
    };
    let serving = Serving::start(config, handle, options).await?;
    tokio::select! {
        () = signaled() => tracing::info!("draining"),
        () = serving.stopped() => {}
        never = hangups(hangup, &serving) => match never {},
    }
    serving.shutdown();
    serving.join().await?;
    Ok(ExitCode::from(EXIT_OK))
}

/// Asks `serving` to reload on every SIGHUP `hangup` receives (RFD 018).
/// The listener is registered before the daemon starts, replacing the
/// signal's default action, which would end the process; a SIGHUP before
/// that, while the file is read and logging set up, still ends it. A signal the
/// runtime cannot listen for, or a listener that stops receiving, is
/// logged, and a change to the file is then applied on the poll alone.
async fn hangups(hangup: std::io::Result<Signal>, serving: &Serving) -> std::convert::Infallible {
    match hangup {
        Ok(mut hangup) => {
            while hangup.recv().await.is_some() {
                tracing::info!("reload requested");
                serving.reload();
            }
            tracing::warn!("SIGHUP listener ended");
        }
        Err(error) => tracing::warn!(%error, "cannot listen for SIGHUP"),
    }
    std::future::pending().await
}

/// Completes on SIGTERM or SIGINT. A signal the runtime cannot listen for
/// is logged, and the daemon then waits for the other.
async fn signaled() {
    let terminate = async {
        match signal(SignalKind::terminate()) {
            Ok(mut terminate) => {
                terminate.recv().await;
            }
            Err(error) => {
                tracing::warn!(%error, "cannot listen for SIGTERM");
                std::future::pending::<()>().await;
            }
        }
    };
    let interrupt = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::warn!(%error, "cannot listen for SIGINT");
            std::future::pending::<()>().await;
        }
    };
    tokio::select! {
        () = terminate => {}
        () = interrupt => {}
    }
}
