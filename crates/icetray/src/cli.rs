//! The command line: `serve` takes only the configuration path, from a flag
//! or the environment (RFD 012); `doctor`, `get`, and `info` share one
//! target vocabulary, a configuration file or one table located directly,
//! and open tables the same way `serve` does.

use std::path::PathBuf;

use clap::{ArgGroup, Args, Parser, Subcommand};
use icetray_core::config::{
    Config, DEFAULT_RESIDENCY, Residency, RewriteRule, ServerConfig, SourceConfig, StorageConfig,
    TableConfig,
};

use crate::error::{CliError, RewriteTextFault};

/// The environment variable that names the configuration file when the
/// flag does not (RFD 012), so a pod passes no flags at all.
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

/// What separates the two halves of a `--rewrite FROM=TO` rule; the first
/// occurrence, since a location may itself carry one.
const REWRITE_SEPARATOR: char = '=';

/// The clap group that requires one of `--config` and `--location`, so a
/// run with neither is a usage error before anything is opened.
const TARGET_GROUP: &str = "target";

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
    /// Diagnose tables: serviceability, cost findings with their penalties,
    /// and advice. Exits 2 when a table serves at a cost, 1 when one cannot
    /// be loaded.
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
/// table located directly. Exactly one of the two; the column, ref, and
/// residency flags describe the located table and are refused with a file.
/// They conflict with `--config` rather than requiring `--location`: clap
/// waives a requirement whose argument conflicts with a present one, so a
/// `requires` alone would let `--config` and `--key-column` pass together.
#[derive(Args, Debug)]
#[command(group(ArgGroup::new(TARGET_GROUP).required(true).args(["config", "location"])))]
pub struct Target {
    /// The configuration file listing the tables, TOML or JSON; also read
    /// from `ICETRAY_CONFIG`.
    #[arg(long, short, env = ENV_CONFIG)]
    pub config: Option<PathBuf>,
    /// A table root or a metadata file to open without a configuration
    /// file, under the name `table`.
    #[arg(long, conflicts_with = "config")]
    pub location: Option<String>,
    /// A location prefix rewrite FROM=TO applied before any read; repeatable.
    #[arg(long, value_parser = parse_rewrite)]
    pub rewrite: Vec<RewriteRule>,
    /// The key column of the located table (default: key).
    #[arg(long, conflicts_with = "config")]
    pub key_column: Option<String>,
    /// The value column of the located table (default: value).
    #[arg(long, conflicts_with = "config")]
    pub value_column: Option<String>,
    /// The ref of the located table (default: main).
    #[arg(long, conflicts_with = "config")]
    pub r#ref: Option<String>,
    /// Where the located table keeps its key chunks: full, disk, or bloom
    /// (default: full).
    #[arg(long, conflicts_with = "config")]
    pub residency: Option<Residency>,
}

/// A `--rewrite` rule from `FROM=TO`, split at the first `=`. An empty
/// `FROM` is refused here, the one guard for a rule typed on the command
/// line: `Config::parse` refuses the same in a file, and the rules a target
/// appends never pass through it.
fn parse_rewrite(text: &str) -> Result<RewriteRule, RewriteTextFault> {
    let (from, to) =
        text.split_once(REWRITE_SEPARATOR)
            .ok_or_else(|| RewriteTextFault::NoSeparator {
                text: text.to_string(),
            })?;
    if from.is_empty() {
        return Err(RewriteTextFault::EmptyPrefix {
            text: text.to_string(),
        });
    }
    Ok(RewriteRule {
        from: from.to_string(),
        to: to.to_string(),
    })
}

impl Target {
    /// The configuration the target denotes: the file parsed (TOML or JSON
    /// by content, RFD 011), or one table synthesized from the location
    /// with server and storage defaults, never strict, since the one-shots
    /// exist to report what `strict` would refuse. The `--rewrite` rules
    /// are appended to `storage.rewrite` in both cases. Neither target is
    /// `NoTableMatched`, the state clap's group prevents.
    pub fn config(&self) -> Result<Config, CliError> {
        let mut config = match (&self.config, &self.location) {
            (Some(path), _) => Config::from_path(path)?,
            (None, Some(location)) => self.location_config(location.clone()),
            (None, None) => return Err(CliError::NoTableMatched { name: None }),
        };
        config.storage.rewrite.extend(self.rewrite.iter().cloned());
        Ok(config)
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
            parse(&["info", "--config", "icetray.toml", "--residency", "disk"]).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::ArgumentConflict, "{error}");
    }

    #[test]
    fn test_try_parse_rejects_neither_target_and_both_targets() {
        let neither = parse(&["info"]).unwrap_err();
        assert_eq!(
            neither.kind(),
            ErrorKind::MissingRequiredArgument,
            "{neither}"
        );
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
    fn test_try_parse_rejects_an_unknown_residency() {
        let error = parse(&["info", "--location", LOCATION, "--residency", "memory"]).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::ValueValidation, "{error}");
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
    fn test_parse_rewrite_refuses_an_empty_prefix() {
        let text = format!("={REWRITE_TO}");
        assert_eq!(
            parse_rewrite(&text),
            Err(RewriteTextFault::EmptyPrefix { text: text.clone() })
        );
        let error = parse(&["info", "--location", LOCATION, "--rewrite", &text]).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::ValueValidation, "{error}");
    }
}
