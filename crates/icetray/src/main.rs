//! The one binary. `serve` is the daemon: the gRPC service, the table
//! lifecycle, the poll loop, and the twelve-factor surface of RFD 012.
//! `doctor`, `get`, and `info` are one-shot subcommands that print JSON
//! lines. This file parses the command line, builds the runtime, and turns
//! the library's typed outcomes into exit codes; everything else is in the
//! library.

use std::path::Path;
use std::process::ExitCode;

use clap::Parser as _;
use icetray::cli::{Cli, Command, Target};
use icetray::error::{CliError, EXIT_FAILURE, EXIT_OK, EXIT_USAGE};
use icetray::logging::{self, LogSink};
use icetray::serve::{ServeOptions, Serving};
use icetray::{oneshot, telemetry};
use icetray_core::config::Config;
use tokio::signal::unix::{SignalKind, signal};
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
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("cannot start the runtime: {error}");
            return ExitCode::from(EXIT_FAILURE);
        }
    };
    runtime.block_on(run(cli))
}

/// Dispatches the parsed command line.
async fn run(cli: Cli) -> ExitCode {
    match cli.command {
        Command::Serve { config } => serve(&config).await,
        Command::Doctor {
            target,
            table,
            files,
        } => {
            run_oneshot(&target, |config| async move {
                oneshot::doctor(&config, table.as_deref(), files).await
            })
            .await
        }
        Command::Get {
            target,
            table,
            keys,
        } => {
            run_oneshot(&target, |config| async move {
                oneshot::get(&config, table.as_deref(), &keys)
                    .await
                    .map(|()| ExitCode::from(EXIT_OK))
            })
            .await
        }
        Command::Info { target, table } => {
            run_oneshot(&target, |config| async move {
                oneshot::info(&config, table.as_deref())
                    .await
                    .map(|()| ExitCode::from(EXIT_OK))
            })
            .await
        }
    }
}

/// One one-shot subcommand: its configuration prepared from `target`,
/// `subcommand` run over it, and the outcome finished into the exit code.
async fn run_oneshot<Fut>(target: &Target, subcommand: impl FnOnce(Config) -> Fut) -> ExitCode
where
    Fut: Future<Output = Result<ExitCode, CliError>>,
{
    match prepare(target) {
        Ok(config) => finish(subcommand(config).await),
        Err(code) => code,
    }
}

/// Reports `error` on stderr, where a failure before logging exists goes,
/// and gives its exit code.
fn report(error: &CliError) -> ExitCode {
    eprintln!("{error}");
    error.exit_code()
}

/// The one-shot's configuration, with logging to stderr at `warn` under its
/// `server.log`, since a one-shot's stdout is data. A target that cannot be
/// used is reported with its exit code.
fn prepare(target: &Target) -> Result<Config, ExitCode> {
    let config = target.config().map_err(|error| report(&error))?;
    logging::init(&config.server.log, LogSink::Stderr, Level::WARN);
    Ok(config)
}

/// A run's outcome, a one-shot's or `serve`'s, as the exit code: a failure
/// is logged, except a closed stdout, which ends the run quietly with exit 0.
fn finish(outcome: Result<ExitCode, CliError>) -> ExitCode {
    match outcome {
        Ok(code) => code,
        Err(CliError::BrokenPipe) => CliError::BrokenPipe.exit_code(),
        Err(error) => {
            tracing::error!(%error, "run failed");
            error.exit_code()
        }
    }
}

/// `serve`: the configuration from `path`, logging to stdout at `info`
/// (twelve-factor: the log stream is stdout), jemalloc's background
/// purging, then the daemon, whose outcome is finished into the exit code
/// as a one-shot's is.
async fn serve(path: &Path) -> ExitCode {
    let config = match Config::from_path(path) {
        Ok(config) => config,
        Err(error) => return report(&CliError::Core(error)),
    };
    logging::init(&config.server.log, LogSink::Stdout, Level::INFO);
    if let Err(error) = tikv_jemalloc_ctl::background_thread::write(true) {
        // Background purging is a Linux feature of jemalloc; elsewhere freed
        // memory returns to the operating system on the allocator's own
        // schedule.
        tracing::warn!(%error, "jemalloc background purging is unavailable");
    }
    finish(daemon(config).await)
}

/// The metrics recorder, the daemon, and a drain on SIGTERM or SIGINT.
/// `EXIT_OK` after a drain; a failure to start or a failed run, which a
/// first-load failure is on its own (RFD 005), and a drain that passed its
/// deadline, are `CliError::Serve`.
async fn daemon(config: Config) -> Result<ExitCode, CliError> {
    let handle = telemetry::install()?;
    let serving = Serving::start(config, handle, ServeOptions::default()).await?;
    tokio::select! {
        () = signalled() => tracing::info!("draining"),
        () = serving.stopped() => {}
    }
    serving.shutdown();
    serving.join().await?;
    Ok(ExitCode::from(EXIT_OK))
}

/// Completes on SIGTERM or SIGINT. A signal the runtime cannot listen for
/// is logged, and the daemon then waits for the other.
async fn signalled() {
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
