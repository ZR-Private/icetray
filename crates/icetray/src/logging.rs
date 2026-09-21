//! JSON logs (RFD 012): one object per event, the standard fields under the
//! names a deployer's `LogConfig.fields` maps them to, every other event
//! field under its own name, and the configured static fields on every
//! line. The binary's own event formatter, so the subscriber's built-in
//! formats stay out; spans are not formatted, since the binary opens none.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use icetray_core::config::LogConfig;
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

use crate::error::CliError;

/// The field `tracing` records an event's message under.
const MESSAGE_FIELD: &str = "message";

/// The standard fields of every event, each renamable through
/// `LogConfig.fields`. `FromStr` reads the lowercase names the file uses,
/// and `Display` prints them, which are also the default emitted names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StandardField {
    /// When the event happened, RFC 3339 in UTC.
    Time,
    /// The event's level, lowercase.
    Level,
    /// The event's message.
    Message,
    /// The module the event came from.
    Target,
}

impl StandardField {
    /// Every standard field, for the parser.
    const ALL: [Self; 4] = [Self::Time, Self::Level, Self::Message, Self::Target];
}

impl fmt::Display for StandardField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Time => "time",
            Self::Level => "level",
            Self::Message => "message",
            Self::Target => "target",
        };
        f.write_str(name)
    }
}

impl FromStr for StandardField {
    type Err = CliError;

    /// Exactly the lowercase names `Display` prints; anything else is
    /// `CliError::LogField`, so a misspelled key in the file is refused at
    /// startup rather than silently ignored.
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|field| field.to_string() == text)
            .ok_or_else(|| CliError::LogField {
                name: text.to_string(),
            })
    }
}

/// The name each standard field is emitted under: the mapped name from
/// `LogConfig.fields`, or the field's own name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FieldNames {
    time: String,
    level: String,
    message: String,
    target: String,
}

impl FieldNames {
    /// The names `config.fields` maps to, the defaults for the rest; a key
    /// that names no standard field is `CliError::LogField`, and two fields
    /// emitted under one name are `CliError::LogFieldCollision`, since a
    /// line would silently lose one of them.
    pub fn from_config(config: &LogConfig) -> Result<Self, CliError> {
        let mut names = Self::default();
        for (key, emitted) in &config.fields {
            let slot = match key.parse::<StandardField>()? {
                StandardField::Time => &mut names.time,
                StandardField::Level => &mut names.level,
                StandardField::Message => &mut names.message,
                StandardField::Target => &mut names.target,
            };
            slot.clone_from(emitted);
        }
        let emitted = [&names.time, &names.level, &names.message, &names.target];
        for (at, name) in emitted.iter().enumerate() {
            if emitted[..at].contains(name) {
                return Err(CliError::LogFieldCollision {
                    name: (*name).clone(),
                });
            }
        }
        Ok(names)
    }
}

impl Default for FieldNames {
    fn default() -> Self {
        Self {
            time: StandardField::Time.to_string(),
            level: StandardField::Level.to_string(),
            message: StandardField::Message.to_string(),
            target: StandardField::Target.to_string(),
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
    pub fn new(config: &LogConfig) -> Result<Self, CliError> {
        Ok(Self {
            names: FieldNames::from_config(config)?,
            static_fields: config.static_fields.clone(),
        })
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

/// Installs the process's subscriber: `RUST_LOG` when set and valid, else
/// `default` as the level; JSON events through `JsonEvents` to `sink`. A
/// `log.fields` key that names no standard field is `CliError::LogField`.
/// A subscriber already installed keeps its place and is told so through
/// itself; the binary installs once, so that is a test's situation.
pub fn init(config: &LogConfig, sink: LogSink, default: Level) -> Result<(), CliError> {
    let format = JsonEvents::new(config)?;
    // intentional: an unset RUST_LOG, the common case, shares this Err with an
    // invalid one, and the default level applies to both.
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default.to_string()));
    let writer = match sink {
        LogSink::Stdout => BoxMakeWriter::new(std::io::stdout),
        LogSink::Stderr => BoxMakeWriter::new(std::io::stderr),
    };
    let layer = tracing_subscriber::fmt::layer()
        .event_format(format)
        .with_writer(writer);
    if let Err(error) = tracing_subscriber::registry()
        .with(filter)
        .with(layer)
        .try_init()
    {
        tracing::warn!(%error, "logging was already initialised; keeping the installed subscriber");
    }
    Ok(())
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
                .event_format(JsonEvents::new(config).unwrap())
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
                ("time".to_string(), "ts".to_string()),
                ("level".to_string(), "lvl".to_string()),
                ("message".to_string(), "msg".to_string()),
            ]),
            static_fields: BTreeMap::from([("service".to_string(), "icetray".to_string())]),
        }
    }

    /// The structured field values the sample event carries.
    const COUNT: u64 = 3;
    const RATIO: f64 = 0.5;

    #[test]
    fn test_from_str_standard_field_round_trips_display() {
        for field in StandardField::ALL {
            assert_eq!(field.to_string().parse::<StandardField>().unwrap(), field);
        }
    }

    #[test]
    fn test_from_str_standard_field_refuses_another_word() {
        let error = "severity".parse::<StandardField>().unwrap_err();
        assert!(matches!(error, CliError::LogField { name } if name == "severity"));
    }

    #[test]
    fn test_from_config_maps_named_fields_and_defaults_the_rest() {
        let names = FieldNames::from_config(&renamed_config()).unwrap();
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
            FieldNames::from_config(&LogConfig::default()).unwrap(),
            FieldNames::default()
        );
    }

    #[test]
    fn test_from_config_refuses_an_unknown_field() {
        let config = LogConfig {
            fields: BTreeMap::from([("severity".to_string(), "lvl".to_string())]),
            static_fields: BTreeMap::new(),
        };
        assert!(matches!(
            FieldNames::from_config(&config),
            Err(CliError::LogField { name }) if name == "severity"
        ));
        assert!(JsonEvents::new(&config).is_err());
    }

    #[test]
    fn test_from_config_refuses_two_fields_with_one_emitted_name() {
        let onto_a_default = LogConfig {
            fields: BTreeMap::from([("message".to_string(), "level".to_string())]),
            static_fields: BTreeMap::new(),
        };
        assert!(matches!(
            FieldNames::from_config(&onto_a_default),
            Err(CliError::LogFieldCollision { name }) if name == "level"
        ));
        let two_onto_one = LogConfig {
            fields: BTreeMap::from([
                ("time".to_string(), "x".to_string()),
                ("level".to_string(), "x".to_string()),
            ]),
            static_fields: BTreeMap::new(),
        };
        assert!(matches!(
            FieldNames::from_config(&two_onto_one),
            Err(CliError::LogFieldCollision { name }) if name == "x"
        ));
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
