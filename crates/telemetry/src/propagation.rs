//! W3C Trace Context propagation over Kafka record headers.
//!
//! [`crate::init`] installs a global `TraceContextPropagator`, so a producer can
//! serialise the current span's `traceparent`/`tracestate` into record headers
//! (via [`current_trace_headers`]) and a consumer can rebuild that context (via
//! [`extract_context`] / [`set_remote_parent`]) to make its own span a child of
//! the producer's — stitching one distributed trace across services through the
//! Kafka WAL / topics.
//!
//! The helpers work in terms of `(key, value)` string/byte pairs so this crate
//! stays independent of the concrete `crabka_client_{producer,consumer}` header
//! types; callers convert to/from their own `Header` at the edge.

use std::collections::HashMap;

use opentelemetry::Context;
use opentelemetry::propagation::{Extractor, Injector};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

/// The canonical W3C trace-context header keys. Producers should attach these
/// (lower-case) keys to Kafka record headers.
pub const TRACEPARENT: &str = "traceparent";
pub const TRACESTATE: &str = "tracestate";

/// Collects injected key/value pairs into a `Vec` for the caller to convert
/// into its own header type (`crabka_client_producer::Header`, …).
struct VecInjector<'a>(&'a mut Vec<(String, String)>);

impl Injector for VecInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        self.0.push((key.to_owned(), value));
    }
}

/// Reads W3C headers back out of a borrowed key→value map.
struct MapExtractor<'a>(&'a HashMap<String, String>);

impl Extractor for MapExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(String::as_str).collect()
    }
}

/// W3C `traceparent` (and `tracestate` when present) for the **current**
/// `tracing` span, as `(key, value)` pairs ready to attach to Kafka record
/// headers. Returns an empty `Vec` when there is no active span, the span is
/// not sampled/recorded, or OTLP is disabled — so it is always safe to call.
///
/// ```no_run
/// let span = tracing::info_span!("produce_order");
/// let _g = span.enter();
/// let carriers = crabka_telemetry::propagation::current_trace_headers();
/// // convert each (key, value) into your producer's Header and attach it
/// ```
#[must_use]
pub fn current_trace_headers() -> Vec<(String, String)> {
    let cx = tracing::Span::current().context();
    let mut out = Vec::new();
    opentelemetry::global::get_text_map_propagator(|prop| {
        prop.inject_context(&cx, &mut VecInjector(&mut out));
    });
    out
}

/// Rebuild an OpenTelemetry [`Context`] from Kafka record headers (each a key
/// plus a raw byte value). Non-UTF-8 values are skipped. Use the result as the
/// remote parent of a consumer-side span (see [`set_remote_parent`]).
#[must_use]
pub fn extract_context<'a, I, V>(headers: I) -> Context
where
    I: IntoIterator<Item = (&'a str, V)>,
    V: AsRef<[u8]>,
{
    let map: HashMap<String, String> = headers
        .into_iter()
        .filter_map(|(k, v)| {
            std::str::from_utf8(v.as_ref())
                .ok()
                .map(|s| (k.to_owned(), s.to_owned()))
        })
        .collect();
    opentelemetry::global::get_text_map_propagator(|prop| prop.extract(&MapExtractor(&map)))
}

/// Make `span` a child of the trace carried in `headers` (the producer's
/// context) so consumer-side work appears in the same distributed trace. A
/// no-op when the headers carry no valid trace context.
pub fn set_remote_parent<'a, I, V>(span: &tracing::Span, headers: I)
where
    I: IntoIterator<Item = (&'a str, V)>,
    V: AsRef<[u8]>,
{
    // `set_parent` returns the previous `Context`; we don't need it.
    let _ = span.set_parent(extract_context(headers));
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use opentelemetry::trace::{TraceContextExt as _, TraceId};

    #[test]
    fn extract_context_roundtrips_w3c_traceparent() {
        // The global propagator is a no-op until one is installed; install the
        // W3C propagator (idempotent for our purposes) so extraction works even
        // when `init()` has not run in this test process.
        opentelemetry::global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );

        let traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        let cx = extract_context([(TRACEPARENT, traceparent.as_bytes())]);
        let sc = cx.span().span_context().clone();

        assert!(sc.is_valid());
        assert!(sc.trace_id() == TraceId::from_hex("0af7651916cd43dd8448eb211c80319c").unwrap());
        assert!(sc.is_sampled());
    }

    #[test]
    fn extract_context_skips_non_utf8_and_missing() {
        opentelemetry::global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );
        // Invalid UTF-8 header value is dropped, leaving no valid context.
        let cx = extract_context([(TRACEPARENT, [0xff, 0xfe].as_slice())]);
        assert!(!cx.span().span_context().is_valid());
    }
}
