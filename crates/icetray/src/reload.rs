//! Configuration reload (RFD 018): the configuration read again on the poll
//! interval and at once on request (SIGHUP, in the binary), its bytes'
//! digest compared with the digest of the configuration applied, and a
//! changed configuration's tables applied through `Opened::apply`, all or
//! nothing. A reload refuses a change to any setting outside the tables,
//! since the server builds those once at start. Each reload that read a
//! changed file is logged with its digest and counted under
//! `config_reloads_total` by result, and `config_pending` reads 1 while the
//! file differs from what the server applied.

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use icetray_core::config::{Config, ConfigError};
use icetray_core::{Applied, Opened, Refused, Table, metric};
use sha2::{Digest as _, Sha256};
use tokio::sync::Notify;

use crate::serve::{retain, ticker};

/// Bytes in a configuration digest: SHA-256's output.
pub const DIGEST_BYTES: usize = 32;

/// The SHA-256 digest of a configuration's bytes, what a reload compares to
/// decide whether the configuration changed. Display gives lowercase hex,
/// as the log carries it.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Digest([u8; DIGEST_BYTES]);

impl Digest {
    /// The digest of `bytes`.
    pub fn of(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).into())
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

/// One read of the configuration: the digest of the bytes read and what
/// they parse and validate to.
pub struct Reading {
    /// The digest of the bytes read.
    pub digest: Digest,
    /// The configuration the bytes hold, or why they hold none.
    pub config: icetray_core::Result<Config>,
}

/// Where a running server reads its configuration again. `ConfigFile` is
/// the binary's; a test supplies its own.
pub trait ConfigSource: Send + Sync {
    /// The configuration as it stands now; `Err` when no bytes could be
    /// read. Called on the blocking pool.
    fn read(&self) -> io::Result<Reading>;
}

/// The configuration file `serve` was started from.
#[derive(Clone, Debug)]
pub struct ConfigFile {
    path: PathBuf,
}

impl ConfigFile {
    /// The file at `path`.
    pub fn new(path: &Path) -> Self {
        Self {
            path: path.to_path_buf(),
        }
    }
}

impl ConfigSource for ConfigFile {
    /// Reads the whole file, digests its bytes, and parses them as
    /// `Config::parse` does; bytes that are not UTF-8 are
    /// `ConfigError::Read`, as `Config::from_path` reports them.
    fn read(&self) -> io::Result<Reading> {
        let bytes = std::fs::read(&self.path)?;
        let digest = Digest::of(&bytes);
        let config = std::str::from_utf8(&bytes)
            .map_err(|error| {
                icetray_core::Error::Config(ConfigError::Read(io::Error::new(
                    io::ErrorKind::InvalidData,
                    error,
                )))
            })
            .and_then(Config::parse);
        Ok(Reading { digest, config })
    }
}

/// What a `Serving` follows its configuration through: where it reads it
/// again, and the digest of the configuration it started from.
#[derive(Clone)]
pub struct Reload {
    /// Where the configuration is read again.
    pub source: Arc<dyn ConfigSource>,
    /// The digest of the configuration the server starts from.
    pub started_from: Digest,
}

impl fmt::Debug for Reload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reload")
            .field("started_from", &self.started_from)
            .finish_non_exhaustive()
    }
}

/// Why a reload changed nothing. Display gives the log's `reason`.
#[derive(Debug)]
pub enum ReloadRefusal {
    /// The bytes do not parse or validate.
    Invalid(icetray_core::Error),
    /// Settings outside the tables changed, by their names in the file.
    Restart(Vec<&'static str>),
    /// A table failed to open or load.
    Tables(Refused),
}

impl fmt::Display for ReloadRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(error) => write!(f, "the configuration is invalid: {error}"),
            Self::Restart(settings) => write!(
                f,
                "{} changed, which only a restart applies",
                settings.join(", ")
            ),
            Self::Tables(refused) => {
                let names: Vec<&str> = refused
                    .failures
                    .iter()
                    .map(|(name, _)| name.as_str())
                    .collect();
                write!(f, "tables failed to load: {}", names.join(", "))
            }
        }
    }
}

/// A reload's outcome, the `result` label of `config_reloads_total`.
/// Display gives the label value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReloadResult {
    /// The configuration's tables were swapped in.
    Applied,
    /// Nothing changed.
    Refused,
}

impl fmt::Display for ReloadResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Applied => "applied",
            Self::Refused => "refused",
        })
    }
}

/// The run's side of a reload: the configuration applied, its digest, the
/// last digest refused, and the two triggers.
pub(crate) struct Reloader {
    source: Arc<dyn ConfigSource>,
    /// The configuration applied: the one started from, with the tables of
    /// the last reload applied.
    running: Config,
    /// The digest of `running`'s bytes.
    applied: Digest,
    /// The digest of the last configuration refused. A poll does not try
    /// it again; a request does, since a table's load can fail for a
    /// reason that has since passed.
    refused: Option<Digest>,
    /// Whether the last read failed, so a file that stays unreadable is
    /// logged once and not at every poll.
    unreadable: bool,
    ticker: tokio::time::Interval,
    requested: Arc<Notify>,
}

impl Reloader {
    /// Follows `reload` from `running`, reading on every `poll_interval`
    /// from one interval from now and whenever `requested` is notified.
    pub(crate) fn new(
        reload: Reload,
        running: Config,
        poll_interval: Duration,
        requested: Arc<Notify>,
    ) -> Self {
        metrics::gauge!(metric::CONFIG_PENDING).set(0.0);
        Self {
            source: reload.source,
            running,
            applied: reload.started_from,
            refused: None,
            unreadable: false,
            ticker: ticker(poll_interval),
            requested,
        }
    }

    /// Waits for polls and requests until one reload applies, then returns
    /// what it changed. A read that finds the configuration applied, or on
    /// a poll the one last refused, does nothing further; a refused reload
    /// is logged and counted, and the wait goes on. A read that fails is
    /// no reload: it is logged when the file becomes unreadable, and
    /// neither counted nor reflected in `config_pending`, since whether the
    /// file differs is unknown. Dropping the future
    /// drops a reload in flight, which swaps nothing in.
    pub(crate) async fn next(&mut self, opened: &Opened) -> Applied {
        loop {
            let requested = tokio::select! {
                _ = self.ticker.tick() => false,
                () = self.requested.notified() => true,
            };
            if let Some(applied) = self.attempt(opened, requested).await {
                return applied;
            }
        }
    }

    /// One read, and the reload it calls for.
    async fn attempt(&mut self, opened: &Opened, requested: bool) -> Option<Applied> {
        let source = Arc::clone(&self.source);
        let read = match tokio::task::spawn_blocking(move || source.read()).await {
            Ok(read) => read,
            Err(join) => std::panic::resume_unwind(join.into_panic()),
        };
        let reading = match read {
            Ok(reading) => reading,
            Err(error) => {
                if !self.unreadable {
                    tracing::warn!(%error, "configuration cannot be read");
                }
                self.unreadable = true;
                return None;
            }
        };
        self.unreadable = false;
        let digest = reading.digest;
        if digest == self.applied {
            metrics::gauge!(metric::CONFIG_PENDING).set(0.0);
            return None;
        }
        metrics::gauge!(metric::CONFIG_PENDING).set(1.0);
        if !requested && self.refused == Some(digest) {
            return None;
        }
        match self.reload(opened, reading).await {
            Ok(applied) => Some(applied),
            Err(refusal) => {
                self.refuse(digest, &refusal);
                if matches!(refusal, ReloadRefusal::Tables(_)) {
                    // The tables that did load wrote chunk files no table
                    // serves; the next swap's retention would free them
                    // only then.
                    retain(opened).await;
                }
                None
            }
        }
    }

    /// Applies `reading`'s tables when nothing else in it changed.
    async fn reload(
        &mut self,
        opened: &Opened,
        reading: Reading,
    ) -> Result<Applied, ReloadRefusal> {
        let config = reading.config.map_err(ReloadRefusal::Invalid)?;
        let restart = self.running.restart_settings(&config);
        if !restart.is_empty() {
            return Err(ReloadRefusal::Restart(restart));
        }
        let applied = opened
            .apply(&config.tables)
            .await
            .map_err(ReloadRefusal::Tables)?;
        let (added, changed, removed) = changes(&applied);
        tracing::info!(
            digest = %reading.digest,
            ?added,
            ?changed,
            ?removed,
            kept = applied.kept,
            "configuration reloaded"
        );
        count(ReloadResult::Applied);
        metrics::gauge!(metric::CONFIG_PENDING).set(0.0);
        self.running = config;
        self.applied = reading.digest;
        self.refused = None;
        Ok(applied)
    }

    /// Logs and counts `refusal` of the configuration digested as `digest`,
    /// and remembers the digest.
    fn refuse(&mut self, digest: Digest, refusal: &ReloadRefusal) {
        if let ReloadRefusal::Tables(refused) = refusal {
            for (table, error) in &refused.failures {
                tracing::warn!(
                    table = table.as_str(),
                    reason = %error.kind(),
                    %error,
                    "table failed to load for a reload"
                );
            }
        }
        tracing::warn!(%digest, reason = %refusal, "configuration reload refused");
        self.refused = Some(digest);
        count(ReloadResult::Refused);
        metrics::gauge!(metric::CONFIG_PENDING).set(1.0);
    }
}

/// Counts one reload of `result` under `config_reloads_total`.
fn count(result: ReloadResult) {
    metrics::counter!(
        metric::CONFIG_RELOADS_TOTAL,
        metric::LABEL_RESULT => result.to_string()
    )
    .increment(1);
}

/// The names `applied` added, changed, and removed: a name both started
/// and stopped is a changed table.
fn changes(applied: &Applied) -> (Vec<&str>, Vec<&str>, Vec<&str>) {
    let started = names(&applied.started);
    let stopped = names(&applied.stopped);
    let added = started
        .iter()
        .copied()
        .filter(|name| !stopped.contains(name))
        .collect();
    let changed = started
        .iter()
        .copied()
        .filter(|name| stopped.contains(name))
        .collect();
    let removed = stopped
        .iter()
        .copied()
        .filter(|name| !started.contains(name))
        .collect();
    (added, changed, removed)
}

/// The names of `tables`, in order.
fn names(tables: &[Arc<Table>]) -> Vec<&str> {
    tables.iter().map(|table| table.name()).collect()
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use super::*;

    /// A configuration `Config::parse` accepts: one static table.
    const ONE_TABLE: &str = r#"
[[tables]]
name = "kv"
ref = "main"
key_column = "id"
value_column = "payload"
[tables.source]
type = "static"
location = "s3://warehouse/kv"
"#;

    fn file_with(bytes: &[u8]) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(bytes).unwrap();
        file
    }

    #[test]
    fn test_config_file_reads_the_digest_and_the_configuration() {
        let file = file_with(ONE_TABLE.as_bytes());
        let reading = ConfigFile::new(file.path()).read().unwrap();
        assert_eq!(reading.digest, Digest::of(ONE_TABLE.as_bytes()));
        let config = reading.config.unwrap();
        assert_eq!(config.tables[0].name, "kv");
    }

    #[test]
    fn test_config_file_digests_bytes_that_do_not_parse() {
        let bytes = b"[[tables]\n";
        let file = file_with(bytes);
        let reading = ConfigFile::new(file.path()).read().unwrap();
        assert_eq!(reading.digest, Digest::of(bytes));
        assert!(reading.config.is_err());
    }

    #[test]
    fn test_config_file_reads_bytes_that_are_not_utf8_as_a_read_error() {
        let file = file_with(&[0xff, 0xfe]);
        let reading = ConfigFile::new(file.path()).read().unwrap();
        assert!(matches!(
            reading.config,
            Err(icetray_core::Error::Config(ConfigError::Read(_)))
        ));
    }

    #[test]
    fn test_config_file_that_is_gone_is_an_io_error() {
        let dir = tempfile::tempdir().unwrap();
        let missing = ConfigFile::new(&dir.path().join("missing.toml"));
        assert!(missing.read().is_err());
    }

    #[test]
    fn test_fmt_digest_is_lowercase_hex() {
        let digest = Digest::of(b"");
        assert_eq!(
            digest.to_string(),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn test_fmt_reload_result_label_values() {
        assert_eq!(ReloadResult::Applied.to_string(), "applied");
        assert_eq!(ReloadResult::Refused.to_string(), "refused");
    }

    #[test]
    fn test_fmt_restart_refusal_names_the_settings() {
        let refusal = ReloadRefusal::Restart(vec!["server.listen", "storage.hedge"]);
        assert_eq!(
            refusal.to_string(),
            "server.listen, storage.hedge changed, which only a restart applies"
        );
    }
}
