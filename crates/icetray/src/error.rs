//! The binary's failures and the exit codes they become. `CliError` is every
//! failure a subcommand can end with, `serve`'s included; `ServeError` every
//! failure of the daemon once its configuration and logging are in place,
//! which reaches the exit code as `CliError::Serve`; `KeyTextFault` why
//! command-line text is not a key; `RewriteTextFault` why `--rewrite` text
//! is not a rule; `WireKeyFault` why a wire key is not a key; `CallFault`
//! why the service ended a call on one of its own bounds.
//! `CliError::exit_code` is the one exhaustive match from a subcommand's
//! failure to an exit code; the two failures before any subcommand runs, a
//! command line clap refuses and a runtime that cannot start, are mapped in
//! `main`. The service maps faults to status codes by variant, so nothing
//! decides on a message.

use std::net::SocketAddr;
use std::process::ExitCode;
use std::time::Duration;

use icetray_core::key::UUID_BYTES;
use icetray_core::{ErrorKind, KeyType};

use crate::cli::ENV_CONFIG;
use crate::logging::StandardField;
use crate::serve::PeriodicTask;
use crate::service::WireVariant;

/// The run did what was asked: the doctor found no cost, every lookup was
/// answered, the daemon drained.
pub const EXIT_OK: u8 = 0;

/// The run failed: a load or read error, a table the doctor could not load,
/// a first-load failure, a listener that would not bind.
pub const EXIT_FAILURE: u8 = 1;

/// The doctor found a cost finding (RFD 016): the table serves, at a price a
/// build pipeline gates on.
pub const EXIT_INEFFICIENT: u8 = 2;

/// The command line or the target could not be used: clap's own errors, no
/// target, no table matched, a table name required, a key that does not
/// parse, an unknown log field or two under one name. `sysexits.h`'s `EX_USAGE`,
/// chosen so that 2 means one thing; clap's default usage code of 2 is never
/// used.
pub const EXIT_USAGE: u8 = 64;

/// Why command-line text is not a key of the table's type. The messages
/// carry the text, since the shell may have changed what the person typed.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum KeyTextFault {
    /// The table's keys are longs and the text is not a signed 64-bit integer.
    #[error("key {text} is not a long")]
    NotALong {
        /// The text as given.
        text: String,
    },
    /// A binary key's hex has an odd number of digits, so a byte is half given.
    #[error("binary key {text} has an odd number of hex digits")]
    OddHexLength {
        /// The text as given.
        text: String,
    },
    /// A binary key's text is not hex.
    #[error("binary key {text} is not hex")]
    NotHex {
        /// The text as given.
        text: String,
    },
    /// The table's keys are uuids and the text is not one.
    #[error("key {text} is not a uuid")]
    NotAUuid {
        /// The text as given.
        text: String,
    },
}

/// Why `--rewrite` text is not a rule. clap reports it as a value
/// validation failure, so the run ends with `EXIT_USAGE` before anything is
/// opened; the messages carry the text as given.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum RewriteTextFault {
    /// No `=` separates `FROM` from `TO`.
    #[error("rewrite {text} is not FROM=TO")]
    NoSeparator {
        /// The text as given.
        text: String,
    },
    /// `FROM` is empty, and an empty prefix would rewrite every location.
    #[error("rewrite {text} has an empty FROM, which would rewrite every location")]
    EmptyPrefix {
        /// The text as given.
        text: String,
    },
}

/// Why a key from the wire is not a key of the table's type (RFD 010).
/// Every variant is the caller's fault, `INVALID_ARGUMENT` on the wire.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum WireKeyFault {
    /// The request carries no key, or a `Key` with no variant set.
    #[error("key is required")]
    Missing,
    /// The variant is one the table's key type does not take.
    #[error("table keys are {expected}, got a {got} key")]
    VariantMismatch {
        /// The table's key type.
        expected: KeyType,
        /// The variant the wire carried.
        got: WireVariant,
    },
    /// A `bin` key on a uuid table whose length is not a uuid's.
    #[error("a uuid key is {} bytes, got {bytes}", UUID_BYTES)]
    UuidLength {
        /// The length the wire carried.
        bytes: usize,
    },
}

/// Why the service ended a call on one of its own bounds (RFD 013) rather
/// than on its lookup's outcome. Each variant has its own status code.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum CallFault {
    /// The call ran past its deadline: the server's for the method, or the
    /// client's when that was shorter. `DEADLINE_EXCEEDED` on the wire.
    #[error("the call did not finish within {after:?}")]
    DeadlineExceeded {
        /// The deadline that passed.
        after: Duration,
    },
    /// Every in-flight slot was taken when the call arrived, so it was
    /// refused rather than queued. `RESOURCE_EXHAUSTED` on the wire.
    #[error("{limit} calls are already in flight")]
    Overloaded {
        /// `server.max_in_flight`.
        limit: usize,
    },
    /// A `MultiGet` carried more keys than one batch may, refused before
    /// any lookup. The caller's fault, so `INVALID_ARGUMENT` on the wire.
    #[error("a MultiGet carries at most {limit} keys, got {keys}")]
    TooManyKeys {
        /// The keys the batch carried.
        keys: usize,
        /// `server.max_multi_get_keys`.
        limit: usize,
    },
}

/// Why `serve` ended other than by a drain.
#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    /// A listener could not bind its configured address.
    #[error("cannot bind {addr}: {source}")]
    Bind {
        /// The configured address.
        addr: SocketAddr,
        /// The operating system's refusal.
        source: std::io::Error,
    },
    /// A failure inside `icetray-core`: opening the tables or closing the cache.
    #[error(transparent)]
    Core(#[from] icetray_core::Error),
    /// The Prometheus recorder could not be built or installed.
    #[error("metrics recorder: {0}")]
    Recorder(#[from] metrics_exporter_prometheus::BuildError),
    /// The gRPC server failed.
    #[error("grpc server: {0}")]
    Transport(#[from] tonic::transport::Error),
    /// A table failed its first load, so the process refuses to start (RFD
    /// 005). Every failed table is named with the kind of its failure; the
    /// errors themselves were logged as they happened.
    #[error("first load failed: {}", failure_list(failures))]
    FirstLoad {
        /// Each failed table's name and the kind of its failure.
        failures: Vec<(String, ErrorKind)>,
    },
    /// The run task itself panicked or was cancelled.
    #[error("task: {0}")]
    Join(#[from] tokio::task::JoinError),
    /// A periodic task panicked before the shutdown aborted it; nothing else
    /// ends one. The run ends rather than serve on: the tables would
    /// otherwise stop following their refs, or the gauges stop sampling,
    /// while `/ready` kept answering 200.
    #[error("periodic task {task} ended: {source}")]
    Periodic {
        /// Which task ended.
        task: PeriodicTask,
        /// How it ended, carrying the panic's message.
        source: tokio::task::JoinError,
    },
}

/// The failed tables of a `FirstLoad` as `name (kind)`, comma-separated.
fn failure_list(failures: &[(String, ErrorKind)]) -> String {
    failures
        .iter()
        .map(|(table, kind)| format!("{table} ({kind})"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Every failure a subcommand can end with: a one-shot's, and `serve`'s,
/// whose daemon failures are wrapped as `Serve`, so every subcommand's exit
/// code comes from `exit_code`.
#[derive(Debug, thiserror::Error)]
pub enum CliError {
    /// A failure inside `icetray-core`: a load or a read.
    #[error(transparent)]
    Core(#[from] icetray_core::Error),
    /// Neither `--config` nor `--location` was given and `ICETRAY_CONFIG` is
    /// unset, so there is nothing to open.
    #[error("pass --config or --location, or set {ENV_CONFIG}")]
    NoTarget,
    /// The configuration names no table, or none by the name asked for.
    #[error("{}", no_table_matched(name.as_deref()))]
    NoTableMatched {
        /// The name asked for with `--table`, when one was.
        name: Option<String>,
    },
    /// A `get` with no `--table` while more than one table is configured.
    #[error("several tables are configured, pass --table with one of: {}", tables.join(", "))]
    TableNameRequired {
        /// The configured names, in configuration order.
        tables: Vec<String>,
    },
    /// Command-line text that is not a key of the table's type.
    #[error(transparent)]
    Key(#[from] KeyTextFault),
    /// A key of `log.fields` that names no standard field.
    #[error(
        "log field {name} is not a standard field; expected one of {}",
        StandardField::names()
    )]
    LogField {
        /// The key as configured.
        name: String,
    },
    /// Two standard fields of `log.fields` would be emitted under one name,
    /// so every line would lose one of them.
    #[error("log field name {name} is emitted for two standard fields; each needs its own")]
    LogFieldCollision {
        /// The emitted name both fields map to.
        name: String,
    },
    /// Writing a line failed for a reason other than a closed pipe.
    #[error("cannot write output: {0}")]
    Io(#[from] std::io::Error),
    /// A line could not be serialized.
    #[error("cannot serialize output: {0}")]
    Json(#[from] serde_json::Error),
    /// A reader closed stdout (`head`, `jq -e`): the run ends with nothing
    /// more printed and exit 0, since the reader got what it asked for.
    #[error("stdout was closed by its reader")]
    BrokenPipe,
    /// `serve` failed.
    #[error(transparent)]
    Serve(#[from] ServeError),
}

/// `no table matched`, naming the `--table` filter when there was one.
fn no_table_matched(name: Option<&str>) -> String {
    match name {
        Some(name) => format!("no table named {name} is configured"),
        None => "no table is configured".to_string(),
    }
}

impl CliError {
    /// The exit code the process ends with for this failure.
    pub fn exit_code(&self) -> ExitCode {
        let code = match self {
            Self::Core(_) | Self::Io(_) | Self::Json(_) | Self::Serve(_) => EXIT_FAILURE,
            Self::NoTarget
            | Self::NoTableMatched { .. }
            | Self::TableNameRequired { .. }
            | Self::Key(_)
            | Self::LogField { .. }
            | Self::LogFieldCollision { .. } => EXIT_USAGE,
            Self::BrokenPipe => EXIT_OK,
        };
        ExitCode::from(code)
    }
}

#[cfg(test)]
mod tests {
    use icetray_core::config::ServerConfig;

    use super::*;

    /// A table name for the errors that carry one.
    const TABLE: &str = "kv";

    fn core_error() -> icetray_core::Error {
        icetray_core::Error::NotReady {
            table: TABLE.to_string(),
        }
    }

    /// `Bind` on the default gRPC address.
    fn serve_error() -> ServeError {
        ServeError::Bind {
            addr: ServerConfig::default().listen,
            source: std::io::Error::other("sample"),
        }
    }

    #[test]
    fn test_exit_code_failure_variants() {
        let cases = [
            CliError::Core(core_error()),
            CliError::Io(std::io::Error::other("sample")),
            CliError::Json(serde_json::from_str::<u8>("sample").unwrap_err()),
            CliError::Serve(serve_error()),
        ];
        for error in cases {
            assert_eq!(error.exit_code(), ExitCode::from(EXIT_FAILURE), "{error}");
        }
    }

    #[test]
    fn test_exit_code_usage_variants() {
        let cases = [
            CliError::NoTarget,
            CliError::NoTableMatched { name: None },
            CliError::NoTableMatched {
                name: Some(TABLE.to_string()),
            },
            CliError::TableNameRequired {
                tables: vec!["a".to_string(), "b".to_string()],
            },
            CliError::Key(KeyTextFault::NotALong {
                text: "abc".to_string(),
            }),
            CliError::LogField {
                name: "severity".to_string(),
            },
            CliError::LogFieldCollision {
                name: "level".to_string(),
            },
        ];
        for error in cases {
            assert_eq!(error.exit_code(), ExitCode::from(EXIT_USAGE), "{error}");
        }
    }

    #[test]
    fn test_exit_code_broken_pipe_is_ok() {
        assert_eq!(CliError::BrokenPipe.exit_code(), ExitCode::from(EXIT_OK));
    }

    #[test]
    fn test_exit_codes_are_distinct() {
        let codes = [EXIT_OK, EXIT_FAILURE, EXIT_INEFFICIENT, EXIT_USAGE];
        for (i, a) in codes.iter().enumerate() {
            for b in &codes[i + 1..] {
                assert_ne!(a, b);
            }
        }
    }
}
