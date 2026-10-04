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
//! `main`. `render` is the plain text a failure is reported in, with the
//! `Hint` `CliError::hint` gives. The service maps faults to status codes
//! by variant, so nothing decides on a message.

use std::fmt;
use std::net::SocketAddr;
use std::process::ExitCode;
use std::time::Duration;

use icetray_core::key::UUID_BYTES;
use icetray_core::{ErrorKind, KeyType};

use crate::cli::{ConfigPath, ENV_CONFIG};
use crate::serve::PeriodicTask;
use crate::service::WireVariant;

/// The run did what was asked: the doctor found nothing inefficient, every
/// lookup was answered, the daemon drained.
pub const EXIT_OK: u8 = 0;

/// The run failed: a configuration that cannot be read, parsed, or
/// validated, one that lists no table included, a load or read error, a
/// table the doctor could not load or refused, a first-load failure, a
/// listener that would not bind, a drain that passed its deadline.
pub const EXIT_FAILURE: u8 = 1;

/// The doctor found an inefficient finding (RFD 016): the table serves,
/// inefficiently, which a build pipeline gates on.
pub const EXIT_INEFFICIENT: u8 = 2;

/// The command line or the target could not be used: clap's own errors, no
/// configuration and no target, a `--table` name no table is configured under, a table name
/// required, a key that does not parse. `sysexits.h`'s `EX_USAGE`, chosen so that 2 means one thing;
/// clap's default usage code of 2 is never used.
pub const EXIT_USAGE: u8 = 64;

/// What starts a failure's line on stderr: clap's own prefix, so a usage
/// error clap reports and one icetray reports read alike.
pub const ERROR_PREFIX: &str = "error: ";

/// What starts the line after a failure that says how to fix it.
pub const HINT_PREFIX: &str = "  hint: ";

/// A failure as the plain text a command ends with on stderr: one line
/// `error: <message>`, and with `hint` one more, `  hint: <hint>`. A
/// one-shot reports every failure so, and `serve` every failure before its
/// logging starts; stdout carries data alone.
pub fn render(message: &dyn fmt::Display, hint: Option<&Hint>) -> String {
    match hint {
        Some(hint) => format!("{ERROR_PREFIX}{message}\n{HINT_PREFIX}{hint}\n"),
        None => format!("{ERROR_PREFIX}{message}\n"),
    }
}

/// How to fix a failure, where the fix is plain from the failure alone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Hint {
    /// Name one of the configured tables.
    PassTable {
        /// The configured names, in configuration order.
        tables: Vec<String>,
    },
    /// Write the key in the form its table's key type takes.
    KeyForm {
        /// The table's key type.
        key_type: KeyType,
    },
}

impl fmt::Display for Hint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PassTable { tables } => {
                write!(f, "pass --table with one of: {}", tables.join(", "))
            }
            Self::KeyForm { key_type } => f.write_str(match key_type {
                KeyType::Long => "a long key is its digits, with a leading - when negative",
                KeyType::String => "a string key is its text as given",
                KeyType::Binary => "a binary key is two hex digits per byte, with or without 0x",
                KeyType::Uuid => "a uuid key is hyphenated or 32 hex digits",
            }),
        }
    }
}

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

impl KeyTextFault {
    /// The key type the text did not parse as.
    pub fn key_type(&self) -> KeyType {
        match self {
            Self::NotALong { .. } => KeyType::Long,
            Self::OddHexLength { .. } | Self::NotHex { .. } => KeyType::Binary,
            Self::NotAUuid { .. } => KeyType::Uuid,
        }
    }
}

/// Why `--rewrite` text is not a rule. clap reports it as a value
/// validation failure with the flag and the text, so the run ends with
/// `EXIT_USAGE` before anything is opened. The two empty halves are the
/// faults `RewriteRule::fault` finds in a file's rule, named here by the
/// halves of the flag's `FROM=TO`.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum RewriteTextFault {
    /// No `=` separates `FROM` from `TO`.
    #[error("no = separates FROM from TO")]
    NoSeparator,
    /// `FROM` is empty, so the rule would rewrite every location.
    #[error("FROM is empty")]
    EmptyFrom,
    /// `TO` is empty, so every location it matches would become relative.
    #[error("TO is empty")]
    EmptyTo,
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
    /// The call ran past the server's deadline for the method.
    /// `DEADLINE_EXCEEDED` on the wire.
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

/// Why `serve` ended other than by a drain that finished in time.
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
    /// The drain waited `server.drain_deadline` for open calls and stopped
    /// waiting; the page cache was closed, and a call still open ends with
    /// the process, its caller answered by a closed connection.
    #[error("drain did not finish within {after:?}, calls were still open")]
    DrainOverran {
        /// The deadline that passed.
        after: Duration,
    },
    /// The run task itself panicked or was canceled.
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
    /// A one-shot was given neither `--config` nor `--location`, and
    /// `ICETRAY_CONFIG` is unset or empty, so there is nothing to open.
    #[error("no configuration: pass --config or --location, or set {ENV_CONFIG}")]
    NoTarget,
    /// `serve` was given no `--config`, and `ICETRAY_CONFIG` is unset or
    /// empty.
    #[error("no configuration: pass --config or set {ENV_CONFIG}")]
    NoConfig,
    /// The configuration file cannot be read. Its origin is named, since a
    /// path from the environment is not on the command line the person is
    /// looking at.
    #[error(
        "cannot read configuration file {} (from {}): {source}",
        file.path.display(),
        file.origin
    )]
    ConfigUnreadable {
        /// The file and where its path came from.
        file: ConfigPath,
        /// The operating system's refusal.
        source: std::io::Error,
    },
    /// No table is configured under the name asked for with `--table`. A
    /// configuration with no table at all is `ConfigViolation::NoTables`,
    /// the file's fault rather than the command line's.
    #[error("no table named {name} is configured")]
    NoTableMatched {
        /// The name asked for with `--table`.
        name: String,
        /// The configured names, in configuration order.
        tables: Vec<String>,
    },
    /// A `get` with no `--table` while more than one table is configured.
    #[error("several tables are configured and get reads one")]
    TableNameRequired {
        /// The configured names, in configuration order.
        tables: Vec<String>,
    },
    /// Command-line text that is not a key of the table's type.
    #[error(transparent)]
    Key(#[from] KeyTextFault),
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

impl CliError {
    /// How to fix this failure, when the failure alone makes it plain.
    pub fn hint(&self) -> Option<Hint> {
        match self {
            Self::NoTableMatched { tables, .. } | Self::TableNameRequired { tables } => {
                Some(Hint::PassTable {
                    tables: tables.clone(),
                })
            }
            Self::Key(fault) => Some(Hint::KeyForm {
                key_type: fault.key_type(),
            }),
            Self::Core(_)
            | Self::NoTarget
            | Self::NoConfig
            | Self::ConfigUnreadable { .. }
            | Self::Io(_)
            | Self::Json(_)
            | Self::BrokenPipe
            | Self::Serve(_) => None,
        }
    }

    /// The exit code the process ends with for this failure.
    pub fn exit_code(&self) -> ExitCode {
        let code = match self {
            Self::Core(_)
            | Self::ConfigUnreadable { .. }
            | Self::Io(_)
            | Self::Json(_)
            | Self::Serve(_) => EXIT_FAILURE,
            Self::NoTarget
            | Self::NoConfig
            | Self::NoTableMatched { .. }
            | Self::TableNameRequired { .. }
            | Self::Key(_) => EXIT_USAGE,
            Self::BrokenPipe => EXIT_OK,
        };
        ExitCode::from(code)
    }
}

#[cfg(test)]
mod tests {
    use icetray_core::config::ServerConfig;

    use super::*;
    use crate::cli::PathOrigin;

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
            CliError::Serve(ServeError::DrainOverran {
                after: ServerConfig::default().drain_deadline,
            }),
            CliError::ConfigUnreadable {
                file: ConfigPath {
                    path: "icetray.toml".into(),
                    origin: PathOrigin::Env,
                },
                source: std::io::Error::other("sample"),
            },
        ];
        for error in cases {
            assert_eq!(error.exit_code(), ExitCode::from(EXIT_FAILURE), "{error}");
        }
    }

    #[test]
    fn test_exit_code_usage_variants() {
        let cases = [
            CliError::NoTarget,
            CliError::NoConfig,
            CliError::NoTableMatched {
                name: TABLE.to_string(),
                tables: vec!["a".to_string()],
            },
            CliError::TableNameRequired {
                tables: vec!["a".to_string(), "b".to_string()],
            },
            CliError::Key(KeyTextFault::NotALong {
                text: "abc".to_string(),
            }),
        ];
        for error in cases {
            assert_eq!(error.exit_code(), ExitCode::from(EXIT_USAGE), "{error}");
        }
    }

    #[test]
    fn test_hint_names_the_tables_to_choose_from() {
        let tables = vec!["a".to_string(), "b".to_string()];
        let pass_table = Some(Hint::PassTable {
            tables: tables.clone(),
        });
        let unmatched = CliError::NoTableMatched {
            name: TABLE.to_string(),
            tables: tables.clone(),
        };
        assert_eq!(unmatched.hint(), pass_table);
        assert_eq!(CliError::TableNameRequired { tables }.hint(), pass_table);
        assert_eq!(CliError::NoTarget.hint(), None);
    }

    #[test]
    fn test_hint_gives_the_form_of_the_key_type_the_text_missed() {
        let text = || "abc".to_string();
        let cases = [
            (KeyTextFault::NotALong { text: text() }, KeyType::Long),
            (KeyTextFault::OddHexLength { text: text() }, KeyType::Binary),
            (KeyTextFault::NotHex { text: text() }, KeyType::Binary),
            (KeyTextFault::NotAUuid { text: text() }, KeyType::Uuid),
        ];
        for (fault, key_type) in cases {
            assert_eq!(
                CliError::Key(fault).hint(),
                Some(Hint::KeyForm { key_type })
            );
        }
    }

    #[test]
    fn test_render_is_an_error_line_and_a_hint_line_when_there_is_a_hint() {
        let error = CliError::TableNameRequired {
            tables: vec!["a".to_string(), "b".to_string()],
        };
        let with_hint = render(&error, error.hint().as_ref());
        let lines: Vec<&str> = with_hint.lines().collect();
        assert_eq!(lines.len(), 2, "{with_hint}");
        assert!(lines[0].starts_with(ERROR_PREFIX), "{with_hint}");
        assert!(lines[1].starts_with(HINT_PREFIX), "{with_hint}");
        assert!(with_hint.ends_with('\n'));

        let bare = render(&CliError::NoTarget, None);
        assert_eq!(bare.lines().count(), 1, "{bare}");
        assert!(bare.starts_with(ERROR_PREFIX), "{bare}");
        assert!(bare.ends_with('\n'));
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
