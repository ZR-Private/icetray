//! JSON logs (RFD 012): one object per event, the standard fields under the
//! names a deployer's `LogConfig.fields` maps them to, every other event
//! field under its own name, and the configured static fields on every
//! line. The binary's own event formatter, so the subscriber's built-in
//! formats stay out; spans are not formatted, since the binary opens none.

use std::collections::BTreeMap;
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

/// Installs the process's subscriber: `RUST_LOG` when set and valid, else
/// `default` as the level; JSON events through `JsonEvents` to `sink`.
/// A subscriber already installed keeps its place and is told so through
/// itself; the binary installs once, so that is a test's situation.
pub fn init(config: &LogConfig, sink: LogSink, default: Level) {
    let format = JsonEvents::new(config);
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
