//! JSON logs (RFD 012): one object per event, the standard fields under the
//! names a deployer's `LogConfig.fields` maps them to, every other event
//! field under its own name, and the configured static fields on every
//! line. The binary's own event formatter, so the subscriber's built-in
//! formats stay out; spans are not formatted, since the binary opens none.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fmt;

use icetray_core::config::{LogConfig, StandardField};
use icetray_core::time;
use serde::Serialize;
use serde::ser::{SerializeMap, Serializer};
use serde_json::Value;
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::writer::BoxMakeWriter;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt as _;

/// The field `tracing` records an event's message under.
const MESSAGE_FIELD: &str = "message";

/// The name each standard field is emitted under: the mapped name from
/// `LogConfig.fields`, or the field's own name. The names are distinct for
/// a configuration `Config::validated` accepted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FieldNames {
    time: String,
    level: String,
    message: String,
    target: String,
}

impl FieldNames {
    /// The names `config` emits the standard fields under.
    pub fn from_config(config: &LogConfig) -> Self {
        let name = |field| config.emitted_name(field).to_string();
        Self {
            time: name(StandardField::Time),
            level: name(StandardField::Level),
            message: name(StandardField::Message),
            target: name(StandardField::Target),
        }
    }
}

/// The event formatter: one JSON object per event, keys in this order: the
/// time, the level, the target, the message, every other event field under
/// its own name, then the static fields. Keys are unique: a field the event
/// carries under a standard field's emitted name is written once, with the
/// event's value; a static field never displaces an event's field.
#[derive(Clone, Debug)]
pub struct JsonEvents {
    names: FieldNames,
    static_fields: BTreeMap<String, String>,
}

impl JsonEvents {
    /// The formatter `config` describes.
    pub fn new(config: &LogConfig) -> Self {
        Self {
            names: FieldNames::from_config(config),
            static_fields: config.static_fields.clone(),
        }
    }
}

/// The level's lowercase name.
fn level_name(level: Level) -> &'static str {
    match level {
        Level::TRACE => "trace",
        Level::DEBUG => "debug",
        Level::INFO => "info",
        Level::WARN => "warn",
        Level::ERROR => "error",
    }
}

/// One line under construction: keys in insertion order, each present once.
#[derive(Default)]
struct Line(Vec<(String, Value)>);

impl Line {
    /// Sets `key` to `value`, replacing an earlier value in place.
    fn set(&mut self, key: &str, value: Value) {
        match self.0.iter_mut().find(|(existing, _)| existing == key) {
            Some((_, slot)) => *slot = value,
            None => self.0.push((key.to_string(), value)),
        }
    }

    /// Sets `key` to `value` unless the line already carries `key`.
    fn set_if_absent(&mut self, key: &str, value: Value) {
        if !self.0.iter().any(|(existing, _)| existing == key) {
            self.0.push((key.to_string(), value));
        }
    }
}

impl Serialize for Line {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (key, value) in &self.0 {
            map.serialize_entry(key, value)?;
        }
        map.end()
    }
}

/// Collects an event's fields: the message as text, every other field as a
/// JSON value. A `fmt::Arguments` value (the message, and every `%` field)
/// arrives through `record_debug`, whose `Debug` form is the text itself.
#[derive(Default)]
struct Fields {
    message: Option<String>,
    others: Vec<(String, Value)>,
}

impl Fields {
    fn put(&mut self, field: &Field, value: Value) {
        if field.name() == MESSAGE_FIELD {
            self.message = Some(match value {
                Value::String(text) => text,
                other @ (Value::Null
                | Value::Bool(_)
                | Value::Number(_)
                | Value::Array(_)
                | Value::Object(_)) => other.to_string(),
            });
        } else {
            self.others.push((field.name().to_string(), value));
        }
    }
}

impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.put(field, Value::String(format!("{value:?}")));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.put(field, Value::String(value.to_string()));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.put(field, Value::from(value));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.put(field, Value::from(value));
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.put(field, Value::from(value));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.put(field, Value::Bool(value));
    }
}

impl<S, N> FormatEvent<S, N> for JsonEvents
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        _ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let mut fields = Fields::default();
        event.record(&mut fields);
        let metadata = event.metadata();
        let mut line = Line::default();
        line.set(
            &self.names.time,
            Value::String(time::log_timestamp(time::now())),
        );
        line.set(
            &self.names.level,
            Value::String(level_name(*metadata.level()).to_string()),
        );
        line.set(
            &self.names.target,
            Value::String(metadata.target().to_string()),
        );
        line.set(
            &self.names.message,
            Value::String(fields.message.unwrap_or_default()),
        );
        for (name, value) in fields.others {
            line.set(&name, value);
        }
        for (name, value) in &self.static_fields {
            line.set_if_absent(name, Value::String(value.clone()));
        }
        let json = serde_json::to_string(&line).map_err(|_| fmt::Error)?;
        writer.write_str(&json)?;
        writer.write_char('\n')
    }
}

/// Where the log stream goes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogSink {
    /// The daemon's stream: stdout, the twelve-factor log stream (RFD 012).
    Stdout,
    /// The one-shots' stream: stderr, since their stdout is data.
    Stderr,
}

/// The crates whose events the default filter passes at the subcommand's
/// own level: the binary and the library under it.
const OWN_TARGETS: [&str; 2] = ["icetray", "icetray_core"];

/// The level every other crate's events pass at by default. Below it a
/// dependency narrates its own workings (foyer's engine choices at start,
/// the SDK's client construction), which an operator has no use for unless
/// they ask through `RUST_LOG`.
const DEPENDENCY_LEVEL: Level = Level::WARN;

/// Targets that pass only errors by default: the AWS SDK's profile-file
/// normalizer, which warns once for every section or key of `~/.aws/config`
/// it ignores, and a laptop's file holds profiles for other tools that are
/// no fault of icetray's. The SDK's warning that a file named by
/// `AWS_CONFIG_FILE` or `AWS_SHARED_CREDENTIALS_FILE` is missing or
/// unreadable comes from `aws_runtime::env_config::source`, and passes.
const QUIET_TARGETS: [&str; 1] = ["aws_runtime::env_config::normalize"];

/// The filter with `RUST_LOG` unset or invalid: icetray's own events at
/// `level`, every other crate's at `DEPENDENCY_LEVEL`, and
/// `QUIET_TARGETS` at `error`.
fn default_filter(level: Level) -> EnvFilter {
    let own = OWN_TARGETS.iter().map(|target| format!("{target}={level}"));
    let quiet = QUIET_TARGETS
        .iter()
        .map(|target| format!("{target}={}", Level::ERROR));
    let directives: Vec<String> = std::iter::once(DEPENDENCY_LEVEL.to_string())
        .chain(own)
        .chain(quiet)
        .collect();
    EnvFilter::new(directives.join(","))
}

/// The filter for `RUST_LOG`'s value `env`: its directives when the value
/// is set, holds one, and parses, replacing the default filter whole; else
/// `default_filter` of `level`. A value of nothing but whitespace is unset,
/// as a shell's `RUST_LOG=` means: `EnvFilter` would read it as no
/// directive at all, which passes no event, errors included.
fn filter_from(env: Option<OsString>, level: Level) -> EnvFilter {
    env.and_then(|value| value.into_string().ok())
        .filter(|value| !value.trim().is_empty())
        // intentional: a value that does not parse gets the default filter,
        // as an unset one does, so a typo never silences the errors.
        .and_then(|value| EnvFilter::try_new(value).ok())
        .unwrap_or_else(|| default_filter(level))
}

/// Installs the process's subscriber: the filter `RUST_LOG` gives
/// (`filter_from`), with `default` as icetray's own level when the
/// variable gives none; JSON events through `JsonEvents` to `sink`,
/// and an event the sink cannot take is dropped. A subscriber already
/// installed keeps its place and is told so through itself; the binary
/// installs once, so that is a test's situation.
pub fn init(config: &LogConfig, sink: LogSink, default: Level) {
    let format = JsonEvents::new(config);
    let filter = filter_from(std::env::var_os(EnvFilter::DEFAULT_ENV), default);
    let writer = match sink {
        LogSink::Stdout => BoxMakeWriter::new(std::io::stdout),
        LogSink::Stderr => BoxMakeWriter::new(std::io::stderr),
    };
    // A write the sink refuses is dropped: reporting it would go to stderr,
    // which for a one-shot is the sink, and a print to a closed stderr
    // panics.
    let layer = tracing_subscriber::fmt::layer()
        .event_format(format)
        .with_writer(writer)
        .log_internal_errors(false);
    if let Err(error) = tracing_subscriber::registry()
        .with(filter)
        .with(layer)
        .try_init()
    {
        tracing::warn!(%error, "logging was already initialized; keeping the installed subscriber");
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::{Arc, Mutex, PoisonError};

    use tracing_subscriber::fmt::MakeWriter;

    use super::*;
    use crate::output::KeyOrder;

    /// A buffer the subscriber under test writes into, shared with the test.
    #[derive(Clone, Default)]
    struct Buffer(Arc<Mutex<Vec<u8>>>);

    impl Buffer {
        /// Every complete line written so far, as written.
        fn raw_lines(&self) -> Vec<String> {
            let bytes = self.0.lock().unwrap_or_else(PoisonError::into_inner);
            String::from_utf8(bytes.clone())
                .unwrap()
                .lines()
                .map(str::to_string)
                .collect()
        }
    }

    /// The keys of `line` in the order written, duplicates included.
    fn keys(line: &str) -> Vec<String> {
        let KeyOrder(keys) = serde_json::from_str(line).unwrap();
        keys
    }

    impl<'a> MakeWriter<'a> for Buffer {
        type Writer = Buffer;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    impl io::Write for Buffer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// Runs `emit` under a subscriber formatting through `config`, and
    /// returns the lines it wrote, as written.
    fn capture_raw(config: &LogConfig, emit: impl FnOnce()) -> Vec<String> {
        let buffer = Buffer::default();
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .event_format(JsonEvents::new(config))
                .with_writer(buffer.clone()),
        );
        tracing::subscriber::with_default(subscriber, emit);
        buffer.raw_lines()
    }

    /// `capture_raw` with every line parsed.
    fn capture(config: &LogConfig, emit: impl FnOnce()) -> Vec<Value> {
        capture_raw(config, emit)
            .iter()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    fn renamed_config() -> LogConfig {
        LogConfig {
            fields: BTreeMap::from([
                (StandardField::Time, "ts".to_string()),
                (StandardField::Level, "lvl".to_string()),
                (StandardField::Message, "msg".to_string()),
            ]),
            static_fields: BTreeMap::from([("service".to_string(), "icetray".to_string())]),
        }
    }

    /// An integer field of the sample event, so a number is emitted as a
    /// JSON number, not quoted.
    const COUNT: u64 = 3;

    /// A float field of the sample event, so a fraction keeps its point.
    const RATIO: f64 = 0.5;

    #[test]
    fn test_from_config_maps_named_fields_and_defaults_the_rest() {
        let names = FieldNames::from_config(&renamed_config());
        assert_eq!(
            names,
            FieldNames {
                time: "ts".to_string(),
                level: "lvl".to_string(),
                message: "msg".to_string(),
                target: "target".to_string(),
            }
        );
        assert_eq!(
            FieldNames::from_config(&LogConfig::default()),
            FieldNames {
                time: "time".to_string(),
                level: "level".to_string(),
                message: "message".to_string(),
                target: "target".to_string(),
            }
        );
    }

    #[test]
    fn test_format_event_renames_fields_and_adds_static_ones() {
        let raw = capture_raw(&renamed_config(), || {
            tracing::info!(count = COUNT, "listening on {}", "addr");
        });
        assert_eq!(raw.len(), 1);
        assert_eq!(
            keys(&raw[0]),
            ["ts", "lvl", "target", "msg", "count", "service"]
        );
        let lines = capture(&renamed_config(), || {
            tracing::info!(count = COUNT, "listening on {}", "addr");
        });
        let line = lines[0].as_object().unwrap();
        assert!(line.contains_key("ts"));
        assert_eq!(line["lvl"], Value::String("info".to_string()));
        assert_eq!(line["msg"], Value::String("listening on addr".to_string()));
        assert_eq!(line["service"], Value::String("icetray".to_string()));
        assert_eq!(line["count"], Value::from(COUNT));
        assert!(line.contains_key("target"));
        for default in ["time", "level", "message"] {
            assert!(!line.contains_key(default), "{default}");
        }
        let time = line["ts"].as_str().unwrap();
        assert!(time.ends_with('Z'), "{time}");
    }

    #[test]
    fn test_format_event_defaults_keep_the_standard_names_in_order() {
        let raw = capture_raw(&LogConfig::default(), || {
            tracing::warn!(flag = true, ratio = RATIO, name = "x", "plain");
        });
        assert_eq!(raw.len(), 1);
        assert_eq!(
            keys(&raw[0]),
            [
                "time", "level", "target", "message", "flag", "ratio", "name"
            ]
        );
        let line: Value = serde_json::from_str(&raw[0]).unwrap();
        let line = line.as_object().unwrap();
        assert_eq!(line["level"], Value::String("warn".to_string()));
        assert_eq!(line["message"], Value::String("plain".to_string()));
        assert_eq!(line["flag"], Value::Bool(true));
        assert_eq!(line["ratio"], Value::from(RATIO));
        assert_eq!(line["name"], Value::String("x".to_string()));
        assert_eq!(line["target"], Value::String(module_path!().to_string()));
    }

    #[test]
    fn test_format_event_display_and_debug_fields_are_text() {
        let error = io::Error::other("sample");
        let lines = capture(&LogConfig::default(), || {
            tracing::error!(%error, code = ?Some(1), "failed");
        });
        let line = lines[0].as_object().unwrap();
        assert_eq!(line["error"], Value::String(error.to_string()));
        assert_eq!(line["code"], Value::String(format!("{:?}", Some(1))));
    }

    #[test]
    fn test_format_event_one_object_per_line_with_every_level_lowercase() {
        let lines = capture(&LogConfig::default(), || {
            tracing::trace!("a");
            tracing::debug!("b");
            tracing::info!("c");
            tracing::warn!("d");
            tracing::error!("e");
        });
        let levels: Vec<&str> = lines
            .iter()
            .map(|line| line["level"].as_str().unwrap())
            .collect();
        assert_eq!(levels, ["trace", "debug", "info", "warn", "error"]);
    }

    #[test]
    fn test_format_event_field_under_a_standard_name_is_written_once() {
        let raw = capture_raw(&renamed_config(), || {
            tracing::info!(msg = "from the event", service = "other", "the message");
        });
        let keys = keys(&raw[0]);
        let mut distinct = keys.clone();
        distinct.sort();
        distinct.dedup();
        assert_eq!(distinct.len(), keys.len(), "{keys:?}");
        assert_eq!(keys.iter().filter(|key| *key == "msg").count(), 1);
        assert_eq!(keys.iter().filter(|key| *key == "service").count(), 1);
        let line: Value = serde_json::from_str(&raw[0]).unwrap();
        assert_eq!(line["msg"], Value::String("from the event".to_string()));
        assert_eq!(line["service"], Value::String("other".to_string()));
    }

    /// The messages of the events `emit` wrote under `default_filter` of
    /// `level`, in order.
    fn passed_by_default(level: Level, emit: impl FnOnce()) -> Vec<String> {
        passed(default_filter(level), emit)
    }

    /// The messages of the events `emit` wrote under `filter`, in order.
    fn passed(filter: EnvFilter, emit: impl FnOnce()) -> Vec<String> {
        let buffer = Buffer::default();
        let subscriber = tracing_subscriber::registry().with(filter).with(
            tracing_subscriber::fmt::layer()
                .event_format(JsonEvents::new(&LogConfig::default()))
                .with_writer(buffer.clone()),
        );
        tracing::subscriber::with_default(subscriber, emit);
        buffer
            .raw_lines()
            .iter()
            .map(|line| {
                let line: Value = serde_json::from_str(line).unwrap();
                line["message"].as_str().unwrap().to_string()
            })
            .collect()
    }

    #[test]
    fn test_default_filter_passes_own_events_at_the_level_and_dependencies_at_warn() {
        let passed = passed_by_default(Level::INFO, || {
            tracing::info!(target: "icetray", "own info");
            tracing::debug!(target: "icetray", "own debug");
            tracing::info!(target: "icetray_core::table", "core info");
            tracing::info!(target: "foyer_storage::engine", "dependency info");
            tracing::warn!(target: "foyer_storage::engine", "dependency warn");
            tracing::warn!(target: "aws_runtime::env_config::normalize", "profile warn");
            tracing::error!(target: "aws_runtime::env_config::normalize", "profile error");
            tracing::warn!(target: "aws_runtime::env_config::source", "file warn");
            tracing::warn!(target: "aws_config::meta::credentials::chain", "chain warn");
        });
        assert_eq!(
            passed,
            [
                "own info",
                "core info",
                "dependency warn",
                "profile error",
                "file warn",
                "chain warn"
            ]
        );
    }

    /// An unset, empty, blank, or unparsable `RUST_LOG` gives the default
    /// filter, which passes an error; a value with a directive replaces it.
    #[test]
    fn test_filter_from_treats_an_empty_variable_as_unset() {
        let emit = || {
            tracing::error!(target: "icetray", "own error");
            tracing::debug!(target: "icetray", "own debug");
        };
        for env in [None, Some(""), Some("  "), Some("icetray=nonsense")] {
            let filter = filter_from(env.map(OsString::from), Level::WARN);
            assert_eq!(passed(filter, emit), ["own error"], "{env:?}");
        }
        let filter = filter_from(Some(OsString::from("debug")), Level::WARN);
        assert_eq!(passed(filter, emit), ["own error", "own debug"]);
    }

    #[test]
    fn test_default_filter_at_warn_holds_own_info_back() {
        let passed = passed_by_default(Level::WARN, || {
            tracing::info!(target: "icetray_core::table", "core info");
            tracing::warn!(target: "icetray_core::table", "core warn");
        });
        assert_eq!(passed, ["core warn"]);
    }

    #[test]
    fn test_format_event_without_a_message_writes_an_empty_one() {
        let lines = capture(&LogConfig::default(), || {
            tracing::info!(count = COUNT);
        });
        let line = lines[0].as_object().unwrap();
        assert_eq!(line["message"], Value::String(String::new()));
        assert_eq!(line["count"], Value::from(COUNT));
    }
}
