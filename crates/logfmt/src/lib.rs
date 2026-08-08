//! Structured-JSON `tracing` log formatter shared across Crabka services.
//!
//! Every Crabka service installs this formatter on its stdout `fmt` layer, so
//! container log collectors ingest each line as fields and not as
//! ANSI-coloured human text. Those services are the broker, the gateway, the
//! operator, and the schema-registry. The output is shaped for Google Cloud
//! Logging on GKE. That agent parses stdout JSON and recognises a few special
//! fields:
//!
//! - `severity`: mapped from the `tracing` level, where `WARN` becomes
//!   `WARNING` and `TRACE` becomes `DEBUG`. It sets the log entry's
//!   `LogSeverity`.
//! - `message`: the event message, flattened to the top level, so it becomes
//!   the entry's summary line.
//! - `timestamp`: RFC3339 UTC, recognised as the entry timestamp.
//!
//! The formatter emits everything else at the top level too, that is the event
//! `target` and all the event fields. A line looks like this:
//!
//! ```json
//! {"timestamp":"2026-06-13T05:55:09.951788Z","severity":"INFO","target":"crabka_broker::network::dispatch","message":"connection opened","listener":"PLAIN","sasl":false}
//! ```
//!
//! The JSON formatter never writes ANSI escape codes, so logs stay clean in
//! non-TTY environments. The default `tracing_subscriber` `fmt` layer emits
//! ANSI colours even when stdout is not a terminal. This crate avoids that
//! bug.

#![forbid(unsafe_code)]

use std::fmt;

use serde_json::{Map, Value};
use tracing::{
    Event, Level, Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::{
    EnvFilter, Layer, Registry,
    fmt::{
        FmtContext, FormatEvent, FormatFields, MakeWriter,
        format::Writer,
        time::{FormatTime, SystemTime},
    },
    registry::LookupSpan,
};

/// Map a `tracing` [`Level`] to a Google Cloud Logging `LogSeverity` string.
///
/// `WARN` becomes `WARNING`, which is Cloud Logging's spelling. `TRACE` floors
/// to `DEBUG`, because Cloud Logging has no finer level. See
/// <https://cloud.google.com/logging/docs/reference/v2/rest/v2/LogEntry#LogSeverity>.
fn severity(level: Level) -> &'static str {
    match level {
        Level::ERROR => "ERROR",
        Level::WARN => "WARNING",
        Level::INFO => "INFO",
        Level::DEBUG | Level::TRACE => "DEBUG",
    }
}

/// Collects `tracing` event fields into a JSON object. It keeps the primitive
/// types and turns everything else into a string with `Debug`.
#[derive(Default)]
struct JsonVisitor {
    fields: Map<String, Value>,
}

impl JsonVisitor {
    fn insert(&mut self, field: &Field, value: Value) {
        self.fields.insert(field.name().to_owned(), value);
    }
}

impl Visit for JsonVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.insert(field, Value::from(value));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.insert(field, Value::from(value));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.insert(field, Value::from(value));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.insert(field, Value::from(value));
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.insert(field, Value::from(value));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        // The event message is recorded here as a `fmt::Arguments`, whose
        // `Debug` impl is its `Display` output — so `message` is a clean string,
        // not a quoted one.
        self.insert(field, Value::from(format!("{value:?}")));
    }
}

/// `tracing_subscriber` event formatter that emits one Cloud Logging-friendly
/// JSON object per line. See the crate-level docs for the output shape.
#[derive(Debug, Clone, Copy, Default)]
pub struct CloudLogging;

impl<S, N> FormatEvent<S, N> for CloudLogging
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
        let meta = event.metadata();
        let mut obj = Map::new();

        // Reuse `tracing_subscriber`'s default timer so the timestamp format
        // (RFC3339 UTC, microsecond precision) matches the rest of the
        // ecosystem exactly.
        let mut timestamp = String::new();
        {
            let mut tw = Writer::new(&mut timestamp);
            SystemTime.format_time(&mut tw)?;
        }
        obj.insert("timestamp".to_owned(), Value::from(timestamp));
        obj.insert("severity".to_owned(), Value::from(severity(*meta.level())));
        obj.insert("target".to_owned(), Value::from(meta.target()));

        // Flatten event fields (including `message`) to the top level. `entry`
        // keeps the reserved keys above from being clobbered by a same-named
        // user field.
        let mut visitor = JsonVisitor::default();
        event.record(&mut visitor);
        for (key, value) in visitor.fields {
            obj.entry(key).or_insert(value);
        }

        let line = serde_json::to_string(&Value::Object(obj)).map_err(|_| fmt::Error)?;
        writeln!(writer, "{line}")
    }
}

/// Build a stdout `fmt` layer that emits [`CloudLogging`] JSON, filtered by
/// `filter`.
///
/// `make_writer` is the sink. Production code passes `std::io::stdout`, and
/// tests pass a capturing buffer.
///
/// The function returns a boxed layer over a [`Registry`], so call sites
/// compose it with `tracing_subscriber::registry().with(...)`.
#[must_use]
pub fn layer<W>(filter: EnvFilter, make_writer: W) -> Box<dyn Layer<Registry> + Send + Sync>
where
    W: for<'writer> MakeWriter<'writer> + Send + Sync + 'static,
{
    tracing_subscriber::fmt::layer()
        .event_format(CloudLogging)
        .with_writer(make_writer)
        .with_filter(filter)
        .boxed()
}

#[cfg(test)]
mod tests {
    use std::{
        io::Write,
        sync::{Arc, Mutex},
    };

    use assert2::check;
    use tracing_subscriber::layer::SubscriberExt as _;

    use super::*;

    #[derive(Clone)]
    struct Buf(Arc<Mutex<Vec<u8>>>);

    impl Write for Buf {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for Buf {
        type Writer = Buf;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// Capture the JSON emitted for a single event by `emit`.
    fn capture(emit: impl FnOnce()) -> serde_json::Value {
        let buf = Arc::new(Mutex::new(Vec::new()));
        let subscriber =
            tracing_subscriber::registry().with(layer(EnvFilter::new("trace"), Buf(buf.clone())));
        tracing::subscriber::with_default(subscriber, emit);
        let out = String::from_utf8(buf.lock().unwrap().clone()).expect("utf8 log output");
        // The crux of the fix: captured logs carry no ANSI escape sequences.
        assert2::assert!(!out.contains('\u{1b}'));
        let line = out.lines().next().expect("a log line");
        serde_json::from_str(line).expect("each log line is valid JSON")
    }

    #[test]
    fn emits_cloud_logging_json() {
        let v = capture(|| {
            tracing::info!(
                listener = "PLAIN",
                sasl = false,
                port = 9092,
                "connection opened"
            );
        });
        // Field types are preserved, not stringified.
        for (field, want) in [
            ("severity", serde_json::json!("INFO")),
            ("message", serde_json::json!("connection opened")),
            ("listener", serde_json::json!("PLAIN")),
            ("sasl", serde_json::json!(false)),
            ("port", serde_json::json!(9092)),
        ] {
            assert2::assert!(v[field] == want);
        }
        check!(v["target"].is_string());
        check!(v["timestamp"].as_str().is_some_and(|t| t.ends_with('Z')));
        // `level` is replaced by Cloud Logging's `severity`.
        check!(v.get("level").is_none());
    }

    #[test]
    fn maps_warn_to_warning_severity() {
        let v = capture(|| tracing::warn!("disk almost full"));
        assert2::assert!(&v["severity"] == &serde_json::json!("WARNING"));
        assert2::assert!(&v["message"] == &serde_json::json!("disk almost full"));
    }

    #[test]
    fn maps_trace_to_debug_severity() {
        let v = capture(|| tracing::trace!("fine-grained detail"));
        assert2::assert!(v["severity"] == "DEBUG");
    }

    #[test]
    fn preserves_u64_and_f64_field_types() {
        // The other record_* visitor paths are covered by emits_cloud_logging_json
        // (str/bool/i64/debug); u64 and f64 have their own visitor methods, so
        // exercise them too and confirm the values survive as native JSON types.
        let v = capture(|| {
            tracing::info!(offset = 42_u64, ratio = 0.5_f64, "metrics");
        });
        assert2::assert!(&v["offset"] == &serde_json::json!(42_u64));
        assert2::assert!(&v["ratio"] == &serde_json::json!(0.5));
    }
}
