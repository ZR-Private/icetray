//! The binary's failures and the exit codes they become. `CliError` is every
//! failure a one-shot subcommand can end with; `ServeError` every failure
//! `serve` can end with; `KeyTextFault` why command-line text is not a key;
//! `WireKeyFault` why a wire key is not one. `CliError::exit_code` is the
//! one exhaustive match from a failure to an exit code, and the service maps
//! faults to status codes by variant, so nothing decides on a message.

use std::net::SocketAddr;
use std::process::ExitCode;

use icetray_core::key::UUID_BYTES;
use icetray_core::{ErrorKind, KeyType};

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
/// table matched, a table name required, a key that does not parse, an
/// unknown log field or two under one name. `sysexits.h`'s `EX_USAGE`,
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
    /// A periodic task ended before the shutdown, which only a panic inside
    /// it does. The run ends rather than serve on: the tables would
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

/// Every failure a one-shot subcommand can end with, and the failures of
/// `serve` wrapped, so `main` has one exit-code mapping.
#[derive(Debug, thiserror::Error)]
pub enum CliError {
    /// A failure inside `icetray-core`: a load or a read.
    #[error(transparent)]
    Core(#[from] icetray_core::Error),
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
    #[error("log field {name} is not a standard field; expected time, level, message, or target")]
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
            Self::NoTableMatched { .. }
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
    use std::net::{IpAddr, Ipv4Addr};

    use super::*;

    /// A table name for the errors that carry one.
    const TABLE: &str = "kv";

    /// An address for `Bind`.
    const ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7433);

    fn core_error() -> icetray_core::Error {
        icetray_core::Error::NotReady {
            table: TABLE.to_string(),
        }
    }

    fn serve_error() -> ServeError {
        ServeError::Bind {
            addr: ADDR,
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

    /// `sysexits.h`'s `EX_USAGE`, the value `EXIT_USAGE` is fixed at.
    const EX_USAGE: u8 = 64;

    /// RFD 016's exit code for a table that serves at a cost.
    const RFD_016_INEFFICIENT: u8 = 2;

    #[test]
    fn test_exit_codes_are_distinct_and_usage_is_ex_usage() {
        let codes = [EXIT_OK, EXIT_FAILURE, EXIT_INEFFICIENT, EXIT_USAGE];
        for (i, a) in codes.iter().enumerate() {
            for b in &codes[i + 1..] {
                assert_ne!(a, b);
            }
        }
        assert_eq!(EXIT_OK, 0);
        assert_eq!(EXIT_INEFFICIENT, RFD_016_INEFFICIENT);
        assert_eq!(EXIT_USAGE, EX_USAGE);
    }
}
