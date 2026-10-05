//! The command line: `serve` takes only the configuration path, from a flag
//! or the environment (RFD 012); `doctor`, `get`, and `info` share one
//! target vocabulary, a configuration file or one table located directly,
//! and open tables the same way `serve` does. Every command finds and reads
//! its configuration file through `ConfigPath`.

use std::ffi::OsString;
use std::fmt;
use std::path::{Component, MAIN_SEPARATOR, Path, PathBuf};

use clap::builder::{NonEmptyStringValueParser, PossibleValuesParser, Styles, TypedValueParser};
use clap::{Args, Parser, Subcommand};
use icetray_core::catalog::{is_relative_path, table_root_name};
use icetray_core::config::{
    Config, DEFAULT_RESIDENCY, Residency, RewriteFault, RewriteRule, ServerConfig, SourceConfig,
    StorageConfig, TableConfig,
};

use crate::error::{
    CliError, EXIT_FAILURE, EXIT_INEFFICIENT, EXIT_OK, EXIT_USAGE, RewriteTextFault,
};
use crate::reload::{ConfigFile, ConfigSource as _, Reading};

/// The environment variable that names the configuration file when no flag
/// does (RFD 012), so a pod passes no flags at all. Every command reads it
/// through `ConfigPath::resolve` rather than through clap: clap cannot tell
/// an environment value from a flag, so it would count an exported variable
/// as `--config` against `--location`, and it would take an empty value as
/// a path.
pub const ENV_CONFIG: &str = "ICETRAY_CONFIG";

/// The name a `--location` table is opened and printed under when its
/// table root has no last component to name it after
/// (`catalog::table_root_name`), as for a bucket alone.
pub const LOCATION_FALLBACK_TABLE_NAME: &str = "table";

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

/// The help heading of the flags that choose the tables, on every command.
const HEADING_TABLES: &str = "Table selection";

/// The help heading of the flags that describe a `--location` table.
const HEADING_LOCATED: &str = "Located table (with --location)";

/// The help heading of the flags that add to what a command prints.
const HEADING_OUTPUT: &str = "Output";

/// What `--config` is, the same words on every command.
const CONFIG_HELP: &str = "The configuration file, TOML or JSON";

/// `--config`'s help: `CONFIG_HELP` with `ENV_CONFIG` named in the form
/// clap's env feature gives, which is off (see `ENV_CONFIG`). Clap writes a
/// spec like `[default: ..]` after the help on the same line under `-h`.
fn config_help() -> String {
    format!("{CONFIG_HELP} [env: {ENV_CONFIG}]")
}

/// `config_help` for `--help`, where clap puts a spec in its own paragraph.
fn config_long_help() -> String {
    format!("{CONFIG_HELP}\n\n[env: {ENV_CONFIG}]")
}

/// The root command's footer: the environment it reads, its exit statuses
/// from the `EXIT_*` codes, and examples, under headings styled as clap
/// styles its own; clap drops the styling when it does not print in color.
fn footer() -> String {
    let heading = *Styles::default().get_header();
    format!(
        "{heading}Environment:{heading:#}
  {ENV_CONFIG}  The configuration file when no --config or --location is given
  RUST_LOG        Log filter, such as debug (default: info for serve, else warn;
                  other crates warn)

{heading}Exit status:{heading:#}
  {EXIT_OK:<3} Success; an absent key is an answer
  {EXIT_FAILURE:<3} Failure, or a table refused
  {EXIT_INEFFICIENT:<3} The doctor found an inefficient table it did not refuse
  {EXIT_USAGE:<3} Usage error

{heading}Examples:{heading:#}
  icetray get --location s3://warehouse/db/kv 42 43
  icetray doctor --config icetray.toml
  icetray serve --config icetray.toml"
    )
}

/// Serve get-by-key lookups over Apache Iceberg tables, or inspect and
/// query them.
#[derive(Parser, Debug)]
#[command(name = "icetray", version, after_help = footer())]
pub struct Cli {
    /// What to run.
    #[command(subcommand)]
    pub command: Command,
}

/// The subcommands. Each one's doc comment is its help: the first paragraph
/// the summary `-h` and the command list show, the whole of it what
/// `--help` shows.
#[derive(Subcommand, Debug)]
pub enum Command {
    /// Run the gRPC server.
    ///
    /// Serves lookups and gRPC health for the tables the configuration file
    /// lists, with metrics and readiness over HTTP, and logs JSON lines to
    /// stdout. Reloads the file when it changes and on SIGHUP; drains and
    /// exits on SIGTERM or SIGINT.
    Serve {
        /// The configuration file.
        #[arg(
            long,
            short,
            value_name = "PATH",
            help = config_help(),
            long_help = config_long_help(),
            help_heading = HEADING_TABLES
        )]
        config: Option<PathBuf>,
    },
    /// Check tables against the table contract.
    ///
    /// Loads each table and prints a JSON table line with its summary, a
    /// finding line for each inefficient finding or advice, and a refusal
    /// line for a table that cannot be served. A table from a configuration
    /// file is judged under its strict setting, as serve judges it.
    ///
    /// Exits 0 when no table is inefficient, 2 when one is inefficient and
    /// not refused, and 1 when one cannot be loaded or is refused.
    Doctor {
        /// Where the tables come from.
        #[command(flatten)]
        target: Target,
        /// Only the table with this configured name.
        #[arg(long, short, value_name = "NAME", help_heading = HEADING_TABLES)]
        table: Option<String>,
        /// Also print one file line per data file.
        #[arg(long, help_heading = HEADING_OUTPUT)]
        files: bool,
    },
    /// Look up keys in one table.
    ///
    /// Prints one JSON lookup line per key, in the order given, saying
    /// whether the key is present, null, or absent. An absent key is an
    /// answer, not a failure: the exit status is 0.
    Get {
        /// Where the tables come from.
        #[command(flatten)]
        target: Target,
        /// The table to read; needed when more than one is configured.
        #[arg(long, short, value_name = "NAME", help_heading = HEADING_TABLES)]
        table: Option<String>,
        /// Keys to look up, each as text of the table's key type.
        ///
        /// A long is its digits, with a leading - when negative. A string
        /// is its text as given. A binary key is hex, two digits per byte
        /// in either case, with or without the 0x a lookup line prints. A
        /// uuid is hyphenated or 32 hex digits. A lookup line's key is in
        /// this form. A key that starts with - and is not a number goes
        /// after --.
        #[arg(value_name = "KEY", required = true, allow_negative_numbers = true)]
        keys: Vec<String>,
    },
    /// Describe each table's current snapshot.
    ///
    /// Loads each table and prints one JSON table line per table with its
    /// snapshot, size, serviceability, and each residency's resident bytes.
    Info {
        /// Where the tables come from.
        #[command(flatten)]
        target: Target,
        /// Only the table with this configured name.
        #[arg(long, short, value_name = "NAME", help_heading = HEADING_TABLES)]
        table: Option<String>,
    },
}

/// Where the tables come from: a configuration file listing them, or one
/// table located directly. At most one of the two; with neither, `config`
/// reads the file `ICETRAY_CONFIG` names, and a `--location` run never
/// reads the variable. An empty `--location`, which as a path would be the
/// filesystem root, is refused by clap as a usage error. The column, ref,
/// and residency flags describe the
/// located table, so they both require `--location` and conflict with
/// `--config`: clap waives a requirement whose argument conflicts with a
/// present one, so `requires` alone would let `--config` and `--key-column`
/// pass together, and the conflict alone would let `--key-column` pass with
/// no target and go unread under the variable. Their defaults are clap's,
/// and a defaulted argument counts as absent to both checks, so the
/// defaults stand under `--config` without tripping either. Each field's
/// doc comment is its help.
#[derive(Args, Debug)]
pub struct Target {
    /// The configuration file listing the tables.
    #[arg(
        long,
        short,
        value_name = "PATH",
        help = config_help(),
        long_help = config_long_help(),
        help_heading = HEADING_TABLES
    )]
    pub config: Option<PathBuf>,
    /// A table root or metadata file to open as one table.
    ///
    /// A URI, or a local path, which may be relative to the working
    /// directory. The table is named after the table root's last directory,
    /// and no configuration file is read, not even one the environment
    /// names.
    #[arg(
        long,
        value_name = "URI",
        value_parser = NonEmptyStringValueParser::new(),
        conflicts_with = "config",
        help_heading = HEADING_TABLES
    )]
    pub location: Option<String>,
    /// Rewrite the location prefix FROM to TO before reading; repeatable.
    ///
    /// A rule given here goes ahead of the configuration file's, so it wins
    /// for the same prefix. A TO that is a local path may be relative to
    /// the working directory.
    #[arg(
        long,
        value_name = "FROM=TO",
        value_parser = parse_rewrite,
        help_heading = HEADING_TABLES
    )]
    pub rewrite: Vec<RewriteRule>,
    /// The key column of the located table.
    #[arg(
        long,
        value_name = "COLUMN",
        requires = "location",
        conflicts_with = "config",
        default_value = DEFAULT_KEY_COLUMN,
        help_heading = HEADING_LOCATED
    )]
    pub key_column: String,
    /// The value column of the located table.
    #[arg(
        long,
        value_name = "COLUMN",
        requires = "location",
        conflicts_with = "config",
        default_value = DEFAULT_VALUE_COLUMN,
        help_heading = HEADING_LOCATED
    )]
    pub value_column: String,
    /// The branch or tag of the located table to read.
    #[arg(
        long,
        value_name = "REF",
        requires = "location",
        conflicts_with = "config",
        default_value = DEFAULT_REF,
        help_heading = HEADING_LOCATED
    )]
    pub r#ref: String,
    /// Where the located table keeps its key chunks.
    ///
    /// Disk residency needs a local directory and is available through a
    /// configuration file.
    #[arg(
        long,
        value_name = "RESIDENCY",
        requires = "location",
        conflicts_with = "config",
        value_parser = located_residency(),
        default_value_t = DEFAULT_RESIDENCY,
        help_heading = HEADING_LOCATED
    )]
    pub residency: Residency,
}

/// Where a configuration file's path came from, which a message about the
/// file names: a mistyped variable is otherwise hard to tell from a
/// mistyped flag. Display gives the flag or the variable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PathOrigin {
    /// `--config`.
    Flag,
    /// `ICETRAY_CONFIG`.
    Env,
}

impl fmt::Display for PathOrigin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Flag => "--config",
            Self::Env => ENV_CONFIG,
        })
    }
}

/// A configuration file's path and where it came from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigPath {
    /// The file.
    pub path: PathBuf,
    /// The flag or the variable that named it.
    pub origin: PathOrigin,
}

impl ConfigPath {
    /// The file `--config` names, given as `flag`, or else the one
    /// `ICETRAY_CONFIG` names in the process's environment; `None` with
    /// neither. An empty variable is unset, as a shell's `ICETRAY_CONFIG=`
    /// means.
    pub fn resolve(flag: Option<&Path>) -> Option<Self> {
        Self::resolve_with_env(flag, std::env::var_os(ENV_CONFIG))
    }

    /// `resolve` with the variable's value passed in, so a test needs no
    /// process environment.
    fn resolve_with_env(flag: Option<&Path>, env: Option<OsString>) -> Option<Self> {
        match (flag, env) {
            (Some(path), _) => Some(Self {
                path: path.to_path_buf(),
                origin: PathOrigin::Flag,
            }),
            (None, Some(path)) if !path.is_empty() => Some(Self {
                path: PathBuf::from(path),
                origin: PathOrigin::Env,
            }),
            (None, Some(_) | None) => None,
        }
    }

    /// The file read once, as `serve` reads it at start and on every
    /// reload: its digest and what its bytes parse and validate to. A file
    /// that cannot be read is `CliError::ConfigUnreadable`, naming the path
    /// and its origin; bytes that make no configuration are a
    /// `ConfigRejected` in the reading, naming both as well.
    pub fn read(&self) -> Result<Reading, CliError> {
        ConfigFile::new(self.clone())
            .read()
            .map_err(|source| CliError::ConfigUnreadable {
                file: self.clone(),
                source,
            })
    }
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

/// A `--rewrite` rule from `FROM=TO`, split at the first `=` and checked by
/// `RewriteRule::fault`, the check a file's rules go through, so a rule
/// the file would refuse is refused here as a usage error.
fn parse_rewrite(text: &str) -> Result<RewriteRule, RewriteTextFault> {
    let (from, to) = text
        .split_once(REWRITE_SEPARATOR)
        .ok_or(RewriteTextFault::NoSeparator)?;
    let rule = RewriteRule {
        from: from.to_string(),
        to: to.to_string(),
    };
    match rule.fault() {
        None => Ok(rule),
        Some(RewriteFault::EmptyFrom) => Err(RewriteTextFault::EmptyFrom),
        Some(RewriteFault::EmptyTo) => Err(RewriteTextFault::EmptyTo),
    }
}

/// `location` made absolute against the working directory when it is a
/// relative local path (`catalog::is_relative_path`), with its `.` and `..`
/// components resolved by name, since storage refuses either in a path, and
/// a trailing separator kept, since a `--rewrite` TO is a prefix the rest
/// of a location follows; any other location as given. The working
/// directory is read only for a relative path: one that cannot be read is
/// `CliError::WorkingDirectory`, one whose path is not UTF-8
/// `CliError::WorkingDirectoryNotUtf8`.
fn absolute_location(location: &str) -> Result<String, CliError> {
    if !is_relative_path(location) {
        return Ok(location.to_string());
    }
    let cwd = std::env::current_dir().map_err(|source| CliError::WorkingDirectory {
        location: location.to_string(),
        source,
    })?;
    let mut path = PathBuf::new();
    for component in cwd.join(location).components() {
        match component {
            Component::ParentDir => {
                path.pop();
            }
            Component::CurDir => {}
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
                path.push(component);
            }
        }
    }
    let mut absolute =
        path.into_os_string()
            .into_string()
            .map_err(|_| CliError::WorkingDirectoryNotUtf8 {
                location: location.to_string(),
                path: cwd,
            })?;
    if location.ends_with(MAIN_SEPARATOR) && !absolute.ends_with(MAIN_SEPARATOR) {
        absolute.push(MAIN_SEPARATOR);
    }
    Ok(absolute)
}

impl Target {
    /// The configuration the target denotes: the file parsed and validated
    /// (TOML or JSON by content, RFD 011), named by `--config` or, with no
    /// target flag, by `ICETRAY_CONFIG`; or one table synthesized from the
    /// location with server and storage defaults, never strict, since no
    /// configuration asks for it and a located table is opened to be
    /// inspected, and validated as a file is. The `--rewrite` rules, which
    /// `parse_rewrite` already held to the file's rule check, then go ahead
    /// of the file's in `storage.rewrite`, and storage applies the first
    /// rule that matches, so a command-line rule takes precedence over the
    /// file's for the same prefix. A violation names a file rule by its
    /// index in the file. No flag and no variable, or an empty one, is
    /// `NoTarget`. A run that selects a table by name, `table`, selects
    /// among configured tables, so with `--location` it is
    /// `TableWithLocation`. A `--location` and a `--rewrite` TO that are
    /// relative local paths are made absolute first (`absolute_location`);
    /// the file's own locations are left as written. A `--location` that,
    /// once the `--rewrite` rules apply, names no store the process builds
    /// is `CliError::Location`, a usage error, before the synthesized table
    /// is validated, so the run never calls it a configuration's fault.
    pub fn config(&self, table: Option<&str>) -> Result<Config, CliError> {
        self.config_with_env(table, std::env::var_os(ENV_CONFIG))
    }

    /// `config` with the variable's value passed in, so a test needs no
    /// process environment.
    fn config_with_env(
        &self,
        table: Option<&str>,
        env: Option<OsString>,
    ) -> Result<Config, CliError> {
        if table.is_some() && self.location.is_some() {
            return Err(CliError::TableWithLocation);
        }
        let given = self
            .rewrite
            .iter()
            .map(|rule| {
                Ok(RewriteRule {
                    from: rule.from.clone(),
                    to: absolute_location(&rule.to)?,
                })
            })
            .collect::<Result<Vec<_>, CliError>>()?;
        let Some(location) = &self.location else {
            let mut config = ConfigPath::resolve_with_env(self.config.as_deref(), env)
                .ok_or(CliError::NoTarget)?
                .read()?
                .config?;
            let from_file = std::mem::take(&mut config.storage.rewrite);
            config.storage.rewrite = given.into_iter().chain(from_file).collect();
            return Ok(config);
        };
        let storage = StorageConfig {
            rewrite: given,
            ..StorageConfig::default()
        };
        let location = absolute_location(location)?;
        if let Some(error) = storage.location_error(&location) {
            return Err(CliError::Location(error));
        }
        Ok(self.location_config(location, storage).validated()?)
    }

    /// The configuration of one table at `location` over `storage`, under
    /// this target's column, ref, and residency flags or their defaults,
    /// named after its table root.
    fn location_config(&self, location: String, storage: StorageConfig) -> Config {
        Config {
            server: ServerConfig::default(),
            storage,
            tables: vec![TableConfig {
                name: table_root_name(&location)
                    .unwrap_or_else(|| LOCATION_FALLBACK_TABLE_NAME.to_string()),
                source: SourceConfig::Static { location },
                r#ref: self.r#ref.clone(),
                key_column: self.key_column.clone(),
                value_column: self.value_column.clone(),
                strict: false,
                residency: self.residency,
                resident_budget_bytes: None,
            }],
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory as _;
    use clap::error::ErrorKind;
    use icetray_core::config::{ConfigError, ConfigViolation};
    use icetray_core::{LocationError, LocationFault};

    use super::*;
    use crate::error::ConfigRejected;

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
        let config = target(cli).config(None).unwrap();
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
                name: "kv".to_string(),
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
        let config = target(cli).config(None).unwrap();
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
        let alone = parse(&["info", "--residency", "bloom"]).unwrap_err();
        assert_eq!(alone.kind(), ErrorKind::MissingRequiredArgument, "{alone}");
    }

    #[test]
    fn test_try_parse_located_defaults_trip_no_conflict_or_requirement() {
        for args in [
            &["info"][..],
            &["info", "--config", "icetray.toml"],
            &["doctor", "--config", "icetray.toml"],
            &["get", "--config", "icetray.toml", "7"],
            &["info", "--location", LOCATION],
        ] {
            let parsed = target(parse(args).unwrap());
            assert_eq!(parsed.key_column, DEFAULT_KEY_COLUMN, "{args:?}");
            assert_eq!(parsed.value_column, DEFAULT_VALUE_COLUMN, "{args:?}");
            assert_eq!(parsed.r#ref, DEFAULT_REF, "{args:?}");
            assert_eq!(parsed.residency, DEFAULT_RESIDENCY, "{args:?}");
        }
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
            .config_with_env(None, Some(file.path().as_os_str().to_owned()))
            .unwrap();
        assert_eq!(from_env.tables[0].name, "kv");
        for unset in [None, Some(OsString::new())] {
            let error = bare.config_with_env(None, unset.clone()).unwrap_err();
            assert!(matches!(error, CliError::NoTarget), "{unset:?}: {error:?}");
        }
        let unreadable = bare
            .config_with_env(None, Some(missing.as_os_str().to_owned()))
            .unwrap_err();
        assert!(
            matches!(
                &unreadable,
                CliError::ConfigUnreadable { file, .. }
                    if file.origin == PathOrigin::Env && file.path == missing
            ),
            "{unreadable:?}"
        );

        let located = target(parse(&["info", "--location", LOCATION]).unwrap());
        let ignoring_env = located
            .config_with_env(None, Some(missing.as_os_str().to_owned()))
            .unwrap();
        assert_eq!(ignoring_env.tables[0].name, "kv");

        let flagged = target(parse(&["info", "--config", file.path().to_str().unwrap()]).unwrap());
        let from_flag = flagged
            .config_with_env(None, Some(missing.as_os_str().to_owned()))
            .unwrap();
        assert_eq!(from_flag.tables[0].name, "kv");
    }

    /// A file that parses and breaks a rule is rejected naming the file
    /// and where its path came from, as an unreadable one is.
    #[test]
    fn test_config_with_env_rejects_an_invalid_file_naming_it() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        std::io::Write::write_all(&mut file, b"[server]\n").unwrap();
        let bare = target(parse(&["info"]).unwrap());
        let rejected = bare
            .config_with_env(None, Some(file.path().as_os_str().to_owned()))
            .unwrap_err();
        assert!(
            matches!(
                &rejected,
                CliError::ConfigRejected(ConfigRejected {
                    file: named,
                    source: ConfigError::Invalid(ConfigViolation::NoTables),
                }) if named.origin == PathOrigin::Env && named.path == file.path()
            ),
            "{rejected:?}"
        );
    }

    #[test]
    fn test_config_names_a_located_table_after_its_root_or_the_fallback() {
        for (location, name) in [
            ("s3://warehouse/db/kv/metadata/v3.metadata.json", "kv"),
            ("s3://warehouse", LOCATION_FALLBACK_TABLE_NAME),
        ] {
            let cli = parse(&["info", "--location", location]).unwrap();
            let config = target(cli).config(None).unwrap();
            assert_eq!(config.tables[0].name, name, "{location}");
        }
    }

    /// A `--location` no built store serves is refused as the command
    /// line's fault, with the location as the rules rewrote it, and a
    /// `--rewrite` that moves it to a store makes it open; the same
    /// location in a file is the file's violation at its setting.
    #[test]
    fn test_config_refuses_a_location_no_store_serves() {
        let unserved = "gs://b/db/kv";
        let located = target(parse(&["info", "--location", unserved]).unwrap());
        let error = located.config_with_env(None, None).unwrap_err();
        assert!(
            matches!(
                &error,
                CliError::Location(LocationError { location, fault: LocationFault::NoStore })
                    if location == unserved
            ),
            "{error:?}"
        );
        let rewritten = target(
            parse(&[
                "info",
                "--location",
                unserved,
                "--rewrite",
                "gs://b/=s3://b/",
            ])
            .unwrap(),
        );
        let config = rewritten.config_with_env(None, None).unwrap();
        assert_eq!(config.tables[0].name, "kv");

        let mut file = tempfile::NamedTempFile::new().unwrap();
        let text = TOML_ONE_TABLE.replace(LOCATION, unserved);
        std::io::Write::write_all(&mut file, text.as_bytes()).unwrap();
        let bare = target(parse(&["info"]).unwrap());
        let rejected = bare
            .config_with_env(None, Some(file.path().as_os_str().to_owned()))
            .unwrap_err();
        assert!(
            matches!(
                &rejected,
                CliError::ConfigRejected(ConfigRejected {
                    source: ConfigError::Invalid(ConfigViolation::TableLocation { index: 0, .. }),
                    ..
                })
            ),
            "{rejected:?}"
        );
    }

    /// `file:` before a relative path is refused as the command line's
    /// fault rather than resolved, and the table is named after the root
    /// the path means all the same.
    #[test]
    fn test_config_refuses_a_file_uri_with_a_relative_path() {
        let located = target(parse(&["info", "--location", "file:db/kv"]).unwrap());
        let error = located.config_with_env(None, None).unwrap_err();
        assert!(
            matches!(
                &error,
                CliError::Location(LocationError { location, fault: LocationFault::FileRelative })
                    if location == "file:db/kv"
            ),
            "{error:?}"
        );
        assert_eq!(table_root_name("file:db/kv").as_deref(), Some("kv"));
    }

    #[test]
    fn test_config_refuses_a_table_name_with_a_location() {
        let located = target(parse(&["info", "--location", LOCATION]).unwrap());
        let error = located.config_with_env(Some("kv"), None).unwrap_err();
        assert!(matches!(error, CliError::TableWithLocation), "{error:?}");
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
            assert_eq!(target(cli).residency, residency);
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
    fn test_debug_assert_accepts_the_definition() {
        Cli::command().debug_assert();
    }

    #[test]
    fn test_try_parse_takes_t_for_table_on_every_one_shot() {
        for args in [
            &["doctor", "-t", "kv"][..],
            &["get", "-t", "kv", "7"],
            &["info", "-t", "kv"],
        ] {
            let table = match parse(args).unwrap().command {
                Command::Doctor { table, .. }
                | Command::Get { table, .. }
                | Command::Info { table, .. } => table,
                Command::Serve { .. } => panic!("{args:?} is a one-shot"),
            };
            assert_eq!(table.as_deref(), Some("kv"), "{args:?}");
        }
        let error = parse(&["serve", "-t", "kv"]).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::UnknownArgument, "{error}");
    }

    #[test]
    fn test_try_parse_help_and_version_go_to_stdout() {
        assert!(!parse(&["--help"]).unwrap_err().use_stderr());
        assert!(!parse(&["--version"]).unwrap_err().use_stderr());
    }

    #[test]
    fn test_resolve_with_env_prefers_the_flag_and_treats_an_empty_variable_as_unset() {
        let flag = Path::new("flag.toml");
        let env = || Some(OsString::from("env.toml"));
        assert_eq!(
            ConfigPath::resolve_with_env(Some(flag), env()),
            Some(ConfigPath {
                path: flag.to_path_buf(),
                origin: PathOrigin::Flag,
            })
        );
        assert_eq!(
            ConfigPath::resolve_with_env(None, env()),
            Some(ConfigPath {
                path: PathBuf::from("env.toml"),
                origin: PathOrigin::Env,
            })
        );
        assert_eq!(
            ConfigPath::resolve_with_env(None, Some(OsString::new())),
            None
        );
        assert_eq!(ConfigPath::resolve_with_env(None, None), None);
    }

    #[test]
    fn test_try_parse_serve_takes_only_the_config() {
        let cli = parse(&["serve", "--config", "icetray.toml"]).unwrap();
        assert!(
            matches!(cli.command, Command::Serve { config: Some(config) } if config.as_path() == Path::new("icetray.toml"))
        );
        let bare = parse(&["serve"]).unwrap();
        assert!(matches!(bare.command, Command::Serve { config: None }));
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
    }

    #[test]
    fn test_parse_rewrite_refuses_a_rule_the_file_would_refuse() {
        let cases = [
            ("no-separator", RewriteTextFault::NoSeparator),
            ("=s3://b/", RewriteTextFault::EmptyFrom),
            ("s3://a/=", RewriteTextFault::EmptyTo),
            ("=", RewriteTextFault::EmptyFrom),
        ];
        for (text, fault) in cases {
            assert_eq!(parse_rewrite(text), Err(fault), "{text:?}");
        }
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
        let config = target(cli).config(None).unwrap();
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

    /// The working directory joined with `relative`, as a location.
    fn under_cwd(relative: &str) -> String {
        let cwd = std::env::current_dir().unwrap();
        format!("{}{MAIN_SEPARATOR}{relative}", cwd.to_str().unwrap())
    }

    /// A relative `--location` and a relative `--rewrite` TO are made
    /// absolute against the working directory, `.` and `..` resolved by
    /// name and the TO's trailing separator kept; the table is named after
    /// the resolved root, and a URI FROM is left alone.
    #[test]
    fn test_config_makes_relative_command_line_paths_absolute() {
        let cli = parse(&[
            "info",
            "--location",
            "./warehouse/../warehouse/db/kv",
            "--rewrite",
            "s3://warehouse/=./mirror/",
        ])
        .unwrap();
        let config = target(cli).config(None).unwrap();
        assert_eq!(config.tables[0].name, "kv");
        assert_eq!(
            config.tables[0].source,
            SourceConfig::Static {
                location: under_cwd("warehouse/db/kv"),
            }
        );
        assert_eq!(
            config.storage.rewrite,
            vec![RewriteRule {
                from: "s3://warehouse/".to_string(),
                to: under_cwd("mirror/"),
            }]
        );
    }

    /// A URI and an absolute path are taken as given; only a relative
    /// local path is resolved.
    #[test]
    fn test_absolute_location_leaves_uris_and_absolute_paths_alone() {
        for location in [LOCATION, "file:///data/kv", "file:/data/kv", "/data/../kv"] {
            assert_eq!(absolute_location(location).unwrap(), location);
        }
        assert_eq!(absolute_location("..").unwrap(), {
            let cwd = std::env::current_dir().unwrap();
            cwd.parent().unwrap().to_str().unwrap().to_string()
        });
    }

    /// A configuration file's relative location is not resolved against
    /// the working directory but refused as the file's violation at its
    /// setting: a file reads the same from any directory.
    #[test]
    fn test_config_refuses_a_file_s_relative_location() {
        let relative = "db/kv";
        let text = TOML_ONE_TABLE.replace(LOCATION, relative);
        let mut file = tempfile::NamedTempFile::new().unwrap();
        std::io::Write::write_all(&mut file, text.as_bytes()).unwrap();
        let path = file.path().to_str().unwrap();
        let rejected = target(parse(&["info", "--config", path]).unwrap())
            .config(None)
            .unwrap_err();
        assert!(
            matches!(
                &rejected,
                CliError::ConfigRejected(ConfigRejected {
                    source: ConfigError::Invalid(ConfigViolation::TableLocation {
                        index: 0,
                        source: LocationError {
                            location,
                            fault: LocationFault::NotAbsolute,
                        },
                    }),
                    ..
                }) if location == relative
            ),
            "{rejected:?}"
        );
    }

    #[test]
    fn test_try_parse_refuses_an_empty_location_as_usage() {
        let error = parse(&["info", "--location", ""]).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidValue, "{error}");
    }

    #[test]
    fn test_try_parse_refuses_an_unusable_rewrite_as_usage() {
        for text in ["no-separator", "=s3://b/", "s3://a/="] {
            let error = parse(&["info", "--location", LOCATION, "--rewrite", text]).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::ValueValidation, "{text}: {error}");
        }
    }
}
