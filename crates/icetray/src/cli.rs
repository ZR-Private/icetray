//! The command line: `serve` takes only the configuration path, from a flag
//! or the environment (RFD 012); `doctor`, `get`, and `info` share one
//! target vocabulary, a configuration file or one table located directly,
//! and open tables the same way `serve` does.

use std::path::PathBuf;

use clap::builder::{PossibleValuesParser, TypedValueParser};
use clap::{Args, Parser, Subcommand};
use icetray_core::config::{
    Config, DEFAULT_RESIDENCY, Residency, RewriteRule, ServerConfig, SourceConfig, StorageConfig,
    TableConfig,
};

use crate::error::{CliError, RewriteTextFault};

/// The environment variable that names the configuration file when no flag
/// does (RFD 012), so a pod passes no flags at all. `serve` reads it through
/// clap, which prints it in the flag's help; a one-shot's `Target` reads it
/// itself, since clap cannot tell an environment value from a flag and would
/// count an exported variable as `--config` against `--location`.
pub const ENV_CONFIG: &str = "ICETRAY_CONFIG";

/// The name a `--location` table is opened and printed under.
pub const LOCATION_TABLE_NAME: &str = "table";

/// The key column of a `--location` table when `--key-column` says nothing.
pub const DEFAULT_KEY_COLUMN: &str = "key";

/// The value column of a `--location` table when `--value-column` says nothing.
pub const DEFAULT_VALUE_COLUMN: &str = "value";

/// The ref of a `--location` table when `--ref` says nothing: Iceberg's main
/// branch, the value of `iceberg::spec::MAIN_BRANCH`, which this crate does
/// not depend on.
pub const DEFAULT_REF: &str = "main";

/// The residencies a `--location` table can run. A one-shot is one load and
/// exit, and no flag supplies the local directory a disk tier needs, so
/// `disk` is not offered; the doctor under `full` already reports each
/// tier's predicted bytes.
const LOCATED_RESIDENCIES: [Residency; 2] = [Residency::Full, Residency::Bloom];

/// What separates the two halves of a `--rewrite FROM=TO` rule; the first
/// occurrence, since a location may itself carry one.
const REWRITE_SEPARATOR: char = '=';

/// Serve get-by-key lookups over Apache Iceberg tables, or inspect and
/// query them.
#[derive(Parser, Debug)]
#[command(name = "icetray", version)]
pub struct Cli {
    /// What to run.
    #[command(subcommand)]
    pub command: Command,
}

/// The subcommands.
#[derive(Subcommand, Debug)]
pub enum Command {
    /// Run the daemon: the gRPC service, health, metrics, and readiness.
    Serve {
        /// The configuration file, TOML or JSON; also read from `ICETRAY_CONFIG`.
        #[arg(long, short, env = ENV_CONFIG)]
        config: PathBuf,
    },
    /// Diagnose tables: serviceability, inefficient findings with their
    /// penalties, and advice. Exits 2 when a table is inefficient, 1 when
    /// one cannot be loaded or is refused.
    Doctor {
        /// Where the tables come from.
        #[command(flatten)]
        target: Target,
        /// Only the table with this configured name.
        #[arg(long)]
        table: Option<String>,
        /// Also print one line per data file.
        #[arg(long)]
        files: bool,
    },
    /// Look keys up in one table and print one line per key.
    Get {
        /// Where the tables come from.
        #[command(flatten)]
        target: Target,
        /// The table to read; needed when more than one is configured.
        #[arg(long)]
        table: Option<String>,
        /// Keys in the form icetray prints them: a long as its digits (a
        /// negative one too), a string as its text, a binary key as hex with
        /// or without 0x, a uuid hyphenated or as 32 hex digits.
        #[arg(required = true, allow_negative_numbers = true)]
        keys: Vec<String>,
    },
    /// Describe the served snapshot of each table.
    Info {
        /// Where the tables come from.
        #[command(flatten)]
        target: Target,
        /// Only the table with this configured name.
        #[arg(long)]
        table: Option<String>,
    },
}

/// Where the tables come from: a configuration file listing them, or one
/// table located directly. At most one of the two; with neither, `config`
/// reads the file `ICETRAY_CONFIG` names, and a `--location` run never
/// reads the variable. The column, ref, and residency flags describe the
/// located table, so they both require `--location` and conflict with
/// `--config`: clap waives a requirement whose argument conflicts with a
/// present one, so `requires` alone would let `--config` and `--key-column`
/// pass together, and the conflict alone would let `--key-column` pass with
/// no target and go unread under the variable.
#[derive(Args, Debug)]
pub struct Target {
    /// The configuration file listing the tables, TOML or JSON. With
    /// neither this nor --location, the file `ICETRAY_CONFIG` names.
    #[arg(long, short)]
    pub config: Option<PathBuf>,
    /// A table root or a metadata file to open without a configuration
    /// file, under the name `table`; `ICETRAY_CONFIG` is ignored.
    #[arg(long, conflicts_with = "config")]
    pub location: Option<String>,
    /// A location prefix rewrite FROM=TO applied before any read; repeatable,
    /// and ahead of the file's rules, so it wins for the same prefix.
    #[arg(long, value_parser = parse_rewrite)]
    pub rewrite: Vec<RewriteRule>,
    /// The key column of the located table (default: key).
    #[arg(long, requires = "location", conflicts_with = "config")]
    pub key_column: Option<String>,
    /// The value column of the located table (default: value).
    #[arg(long, requires = "location", conflicts_with = "config")]
    pub value_column: Option<String>,
    /// The ref of the located table (default: main).
    #[arg(long, requires = "location", conflicts_with = "config")]
    pub r#ref: Option<String>,
    /// Where the located table keeps its key chunks (default: full). Disk
    /// is not offered: a one-shot loads once and exits, and the doctor
    /// under full reports every tier's predicted bytes.
    #[arg(
        long,
        requires = "location",
        conflicts_with = "config",
        value_parser = located_residency()
    )]
    pub residency: Option<Residency>,
}

/// `--residency`'s parser: the name of one of `LOCATED_RESIDENCIES`, read
/// through `Residency`'s `FromStr`. Clap refuses any other word, `disk`
/// included, as a usage error that lists the accepted names.
fn located_residency() -> impl TypedValueParser<Value = Residency> {
    PossibleValuesParser::new(LOCATED_RESIDENCIES.map(Residency::name)).map(|name| {
        name.parse()
            .expect("a possible value is the name of a residency")
    })
}

/// A `--rewrite` rule from `FROM=TO`, split at the first `=`. The rule's
/// content is checked with the rest of the configuration by
/// `Target::config`, so an empty `FROM` is the same violation here as in a
/// file.
fn parse_rewrite(text: &str) -> Result<RewriteRule, RewriteTextFault> {
    let (from, to) =
        text.split_once(REWRITE_SEPARATOR)
            .ok_or_else(|| RewriteTextFault::NoSeparator {
                text: text.to_string(),
            })?;
    Ok(RewriteRule {
        from: from.to_string(),
        to: to.to_string(),
    })
}

impl Target {
    /// The configuration the target denotes: the file parsed (TOML or JSON
    /// by content, RFD 011), named by `--config` or, with no target flag,
    /// by `ICETRAY_CONFIG`; or one table synthesized from the location with
    /// server and storage defaults, never strict, since no configuration
    /// asks for it and a located table is opened to be inspected. The
    /// `--rewrite` rules go ahead of the file's in `storage.rewrite`, and
    /// storage applies the first rule that matches, so a command-line rule
    /// takes precedence over the file's for the same prefix. Only then is
    /// the whole validated, so the command-line rules and a located table
    /// are held to every rule `Config::parse` checks: an empty `--rewrite`
    /// prefix is the same violation a file would be. No flag and no variable is `NoTarget`.
    pub fn config(&self) -> Result<Config, CliError> {
        self.config_with_env(std::env::var_os(ENV_CONFIG).map(PathBuf::from))
    }

    /// `config` with the variable's value passed in, so a test needs no
    /// process environment.
    fn config_with_env(&self, env_path: Option<PathBuf>) -> Result<Config, CliError> {
        let mut config = match (&self.config, &self.location, env_path) {
            (Some(path), _, _) => Config::from_path_unvalidated(path)?,
            (None, Some(location), _) => self.location_config(location.clone()),
            (None, None, Some(path)) => Config::from_path_unvalidated(&path)?,
            (None, None, None) => return Err(CliError::NoTarget),
        };
        let from_file = std::mem::take(&mut config.storage.rewrite);
        config.storage.rewrite = self.rewrite.iter().cloned().chain(from_file).collect();
        Ok(config.validated()?)
    }

    /// The configuration of one table at `location` under this target's
    /// column, ref, and residency flags or their defaults.
    fn location_config(&self, location: String) -> Config {
        Config {
            server: ServerConfig::default(),
            storage: StorageConfig::default(),
            tables: vec![TableConfig {
                name: LOCATION_TABLE_NAME.to_string(),
                source: SourceConfig::Static { location },
                r#ref: self
                    .r#ref
                    .clone()
                    .unwrap_or_else(|| DEFAULT_REF.to_string()),
                key_column: self
                    .key_column
                    .clone()
                    .unwrap_or_else(|| DEFAULT_KEY_COLUMN.to_string()),
                value_column: self
                    .value_column
                    .clone()
                    .unwrap_or_else(|| DEFAULT_VALUE_COLUMN.to_string()),
                strict: false,
                residency: self.residency.unwrap_or(DEFAULT_RESIDENCY),
                resident_budget_bytes: None,
            }],
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::error::ErrorKind;
    use icetray_core::config::{ConfigError, ConfigViolation};

    use super::*;

    /// A table root for the location tests.
    const LOCATION: &str = "s3://warehouse/db/kv";

    /// The halves of the sample rewrite rule.
    const REWRITE_FROM: &str = "s3://old/";
    const REWRITE_TO: &str = "s3://new/";

    /// Parses `args` as if typed after the binary's name.
    fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
        Cli::try_parse_from(std::iter::once("icetray").chain(args.iter().copied()))
    }

    /// The target of a parsed one-shot.
    fn target(cli: Cli) -> Target {
        match cli.command {
            Command::Doctor { target, .. }
            | Command::Get { target, .. }
            | Command::Info { target, .. } => target,
            Command::Serve { .. } => panic!("a one-shot was parsed"),
        }
    }

    #[test]
    fn test_config_synthesizes_the_location_table_with_defaults() {
        let rewrite = format!("{REWRITE_FROM}={REWRITE_TO}");
        let cli = parse(&["info", "--location", LOCATION, "--rewrite", &rewrite]).unwrap();
        let config = target(cli).config().unwrap();
        assert_eq!(config.server, ServerConfig::default());
        assert_eq!(
            config.storage,
            StorageConfig {
                rewrite: vec![RewriteRule {
                    from: REWRITE_FROM.to_string(),
                    to: REWRITE_TO.to_string(),
                }],
                ..StorageConfig::default()
            }
        );
        assert_eq!(
            config.tables,
            vec![TableConfig {
                name: LOCATION_TABLE_NAME.to_string(),
                source: SourceConfig::Static {
                    location: LOCATION.to_string(),
                },
                r#ref: DEFAULT_REF.to_string(),
                key_column: DEFAULT_KEY_COLUMN.to_string(),
                value_column: DEFAULT_VALUE_COLUMN.to_string(),
                strict: false,
                residency: Residency::Full,
                resident_budget_bytes: None,
            }]
        );
    }

    #[test]
    fn test_config_location_flags_override_the_defaults_and_stay_lenient() {
        let cli = parse(&[
            "doctor",
            "--location",
            LOCATION,
            "--key-column",
            "id",
            "--value-column",
            "payload",
            "--ref",
            "serving",
            "--residency",
            "bloom",
        ])
        .unwrap();
        let config = target(cli).config().unwrap();
        let table = &config.tables[0];
        assert_eq!(table.key_column, "id");
        assert_eq!(table.value_column, "payload");
        assert_eq!(table.r#ref, "serving");
        assert_eq!(table.residency, Residency::Bloom);
        assert!(!table.strict);
    }

    #[test]
    fn test_try_parse_rejects_location_flags_without_location() {
        for flag in ["--key-column", "--value-column", "--ref"] {
            let under_config = parse(&["info", "--config", "icetray.toml", flag, "x"]).unwrap_err();
            assert_eq!(
                under_config.kind(),
                ErrorKind::ArgumentConflict,
                "{flag}: {under_config}"
            );
            let alone = parse(&["info", flag, "x"]).unwrap_err();
            assert_eq!(
                alone.kind(),
                ErrorKind::MissingRequiredArgument,
                "{flag}: {alone}"
            );
        }
        let error =
            parse(&["info", "--config", "icetray.toml", "--residency", "bloom"]).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::ArgumentConflict, "{error}");
    }

    /// The smallest configuration a file can hold, for the tests that read
    /// one through the environment.
    const TOML_ONE_TABLE: &str = r#"
[[tables]]
name = "kv"
source = { type = "static", location = "s3://warehouse/db/kv" }
ref = "main"
key_column = "key"
value_column = "value"
"#;

    #[test]
    fn test_config_with_env_reads_the_variable_only_with_no_target_flag() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        std::io::Write::write_all(&mut file, TOML_ONE_TABLE.as_bytes()).unwrap();
        let missing = file.path().with_extension("missing");

        let bare = target(parse(&["info"]).unwrap());
        let from_env = bare
            .config_with_env(Some(file.path().to_path_buf()))
            .unwrap();
        assert_eq!(from_env.tables[0].name, "kv");
        let unset = bare.config_with_env(None).unwrap_err();
        assert!(matches!(unset, CliError::NoTarget), "{unset:?}");

        let located = target(parse(&["info", "--location", LOCATION]).unwrap());
        let ignoring_env = located.config_with_env(Some(missing.clone())).unwrap();
        assert_eq!(ignoring_env.tables[0].name, LOCATION_TABLE_NAME);

        let flagged = target(parse(&["info", "--config", file.path().to_str().unwrap()]).unwrap());
        let from_flag = flagged.config_with_env(Some(missing)).unwrap();
        assert_eq!(from_flag.tables[0].name, "kv");
    }

    #[test]
    fn test_try_parse_rejects_both_targets() {
        let both =
            parse(&["info", "--config", "icetray.toml", "--location", LOCATION]).unwrap_err();
        assert_eq!(both.kind(), ErrorKind::ArgumentConflict, "{both}");
        let get_without_keys = parse(&["get", "--location", LOCATION]).unwrap_err();
        assert_eq!(
            get_without_keys.kind(),
            ErrorKind::MissingRequiredArgument,
            "{get_without_keys}"
        );
    }

    #[test]
    fn test_try_parse_offers_a_located_table_full_and_bloom_only() {
        for residency in [Residency::Full, Residency::Bloom] {
            let cli = parse(&[
                "info",
                "--location",
                LOCATION,
                "--residency",
                &residency.to_string(),
            ])
            .unwrap();
            assert_eq!(target(cli).residency, Some(residency));
        }
        for refused in [&Residency::Disk.to_string(), "memory"] {
            let error =
                parse(&["info", "--location", LOCATION, "--residency", refused]).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::InvalidValue, "{refused}: {error}");
        }
    }

    /// A negative long key, as `Key`'s `Display` prints one.
    const NEGATIVE_KEY: &str = "-5";

    #[test]
    fn test_try_parse_get_accepts_a_negative_long_key() {
        for args in [
            ["get", "--location", LOCATION, NEGATIVE_KEY, "7"],
            ["get", "--location", LOCATION, "--", NEGATIVE_KEY],
        ] {
            let cli = parse(&args).unwrap();
            let Command::Get { keys, .. } = cli.command else {
                panic!("{args:?} is a get");
            };
            assert_eq!(keys[0], NEGATIVE_KEY, "{args:?}");
        }
    }

    #[test]
    fn test_try_parse_help_and_version_go_to_stdout() {
        assert!(!parse(&["--help"]).unwrap_err().use_stderr());
        assert!(!parse(&["--version"]).unwrap_err().use_stderr());
    }

    #[test]
    fn test_try_parse_serve_takes_only_the_config() {
        let cli = parse(&["serve", "--config", "icetray.toml"]).unwrap();
        assert!(
            matches!(cli.command, Command::Serve { config } if config.as_path() == std::path::Path::new("icetray.toml"))
        );
        let error =
            parse(&["serve", "--config", "icetray.toml", "--location", LOCATION]).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::UnknownArgument, "{error}");
    }

    #[test]
    fn test_parse_rewrite_splits_at_the_first_separator() {
        assert_eq!(
            parse_rewrite("s3://a/=s3://b/?x=1"),
            Ok(RewriteRule {
                from: "s3://a/".to_string(),
                to: "s3://b/?x=1".to_string(),
            })
        );
        assert_eq!(
            parse_rewrite("no-separator"),
            Err(RewriteTextFault::NoSeparator {
                text: "no-separator".to_string(),
            })
        );
    }

    #[test]
    fn test_config_puts_command_line_rewrites_ahead_of_the_file_s() {
        let file_to = "s3://from-the-file/";
        let text = format!(
            "{TOML_ONE_TABLE}\n[[storage.rewrite]]\nfrom = \"{REWRITE_FROM}\"\nto = \"{file_to}\"\n"
        );
        let mut file = tempfile::NamedTempFile::new().unwrap();
        std::io::Write::write_all(&mut file, text.as_bytes()).unwrap();
        let rewrite = format!("{REWRITE_FROM}={REWRITE_TO}");
        let path = file.path().to_str().unwrap();
        let cli = parse(&["info", "--config", path, "--rewrite", &rewrite]).unwrap();
        let config = target(cli).config().unwrap();
        assert_eq!(
            config.storage.rewrite,
            vec![
                RewriteRule {
                    from: REWRITE_FROM.to_string(),
                    to: REWRITE_TO.to_string(),
                },
                RewriteRule {
                    from: REWRITE_FROM.to_string(),
                    to: file_to.to_string(),
                },
            ]
        );
    }

    #[test]
    fn test_config_refuses_an_empty_rewrite_prefix_as_the_file_does() {
        let text = format!("={REWRITE_TO}");
        let cli = parse(&["info", "--location", LOCATION, "--rewrite", &text]).unwrap();
        let error = target(cli).config().unwrap_err();
        assert!(
            matches!(
                error,
                CliError::Core(icetray_core::Error::Config(ConfigError::Invalid(
                    ConfigViolation::EmptyRewritePrefix
                )))
            ),
            "{error:?}"
        );
    }
}
