//! W3C Trace Context propagation over Kafka record headers.
//!
//! The telemetry initialisation of the service, `crabka_telemetry::init`,
//! installs a global `TraceContextPropagator`. A producer can then serialise
//! the current span's `traceparent` and `tracestate` into record headers with
//! [`current_trace_headers`]. A consumer can rebuild that context with
//! [`extract_context`] and [`set_remote_parent`], and make its own span a child
//! of the producer's span. One distributed trace then covers all the services
//! that the Kafka WAL and topics connect.
//!
//! The helpers work with `(key, value)` string and byte pairs, so this crate
//! stays independent of the concrete `crabka_client_{producer,consumer}` header
//! types. Callers convert to and from their own `Header` at the edge.

use std::collections::HashMap;

use opentelemetry::{
    Context,
    propagation::{Extractor, Injector},
};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

/// The canonical W3C trace-context header keys.
///
/// A producer should attach these keys, in lower case, to Kafka record headers.
pub const TRACEPARENT: &str = "traceparent";
pub const TRACESTATE: &str = "tracestate";

/// Collects injected key and value pairs into a `Vec`.
///
/// The caller converts each pair into its own header type, such as
/// `crabka_client_producer::Header`.
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

/// The W3C trace-context headers for the **current** `tracing` span.
///
/// Returns the `traceparent`, and the `tracestate` when the span has one, as
/// `(key, value)` pairs for Kafka record headers. Returns an empty `Vec` when
/// there is no active span, when the span is not sampled or recorded, or when
/// OTLP is disabled. It is always safe to call.
///
/// ```no_run
/// let span = tracing::info_span!("produce_order");
/// let _g = span.enter();
/// let carriers = crabka_trace_context::current_trace_headers();
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

/// Rebuild an OpenTelemetry [`Context`] from Kafka record headers.
///
/// Each header is a key plus a raw byte value. This function skips a non-UTF-8
/// value. Use the result as the remote parent of a consumer-side span: see
/// [`set_remote_parent`].
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

/// Make `span` a child of the trace that `headers` carries.
///
/// The headers hold the producer's context, so consumer-side work then appears
/// in the same distributed trace. This function does nothing when the headers
/// carry no valid trace context.
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

    use opentelemetry::trace::{TraceContextExt as _, TraceId};

    use super::*;

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

        assert2::assert!(sc.is_valid());
        assert2::assert!(
            sc.trace_id() == TraceId::from_hex("0af7651916cd43dd8448eb211c80319c").unwrap()
        );
        assert2::assert!(sc.is_sampled());
    }

    #[test]
    fn extract_context_skips_non_utf8_and_missing() {
        opentelemetry::global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );
        // Invalid UTF-8 header value is dropped, leaving no valid context.
        let cx = extract_context([(TRACEPARENT, [0xff, 0xfe].as_slice())]);
        assert2::assert!(!cx.span().span_context().is_valid());
    }

    /// Run `f` under a subscriber wired to a real `OTel` tracer with
    /// `AlwaysOn` sampling and no exporter.
    ///
    /// Spans created inside then carry a valid, *sampled* `OTel` context. The
    /// inject and re-parent helpers do nothing observable without it.
    fn with_otel_subscriber(f: impl FnOnce()) {
        use opentelemetry::trace::TracerProvider as _;
        use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
        use tracing_subscriber::prelude::*;

        opentelemetry::global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );
        let provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::AlwaysOn)
            .build();
        let tracer = provider.tracer("propagation-test");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
        tracing::subscriber::with_default(subscriber, f);
    }

    #[test]
    fn vec_injector_collects_key_value_pairs() {
        // The Injector must actually record each (key, value); a no-op set()
        // would silently drop the trace headers.
        let mut out = Vec::new();
        VecInjector(&mut out).set(TRACEPARENT, "abc".to_owned());
        assert2::assert!(out == vec![(TRACEPARENT.to_owned(), "abc".to_owned())]);
    }

    #[test]
    fn map_extractor_reads_values_and_lists_keys() {
        // get() returns the stored value (None when absent); keys() must list the
        // real header keys, not an empty/placeholder set.
        let map: HashMap<String, String> = [(TRACEPARENT.to_owned(), "v".to_owned())]
            .into_iter()
            .collect();
        let ex = MapExtractor(&map);
        assert2::assert!(ex.get(TRACEPARENT) == Some("v"));
        assert2::assert!(ex.get("absent").is_none());
        assert2::assert!(ex.keys() == vec![TRACEPARENT]);
    }

    #[test]
    fn current_trace_headers_emits_traceparent_for_active_span() {
        with_otel_subscriber(|| {
            let span = tracing::info_span!("producer");
            let _g = span.enter();

            let headers = current_trace_headers();
            let tp = headers.iter().find(|(k, _)| k == TRACEPARENT);
            assert2::assert!(tp.is_some());

            // The injected value carries *this* span's trace id and the sampled
            // flag — pinning both the key and the content so a wrong-key or
            // placeholder-value mutant is caught.
            let (_, value) = tp.unwrap();
            let trace_id = tracing::Span::current()
                .context()
                .span()
                .span_context()
                .trace_id();
            assert2::assert!(value.contains(&trace_id.to_string()));
            assert2::assert!(value.ends_with("-01"));
        });
    }

    #[test]
    fn current_trace_headers_empty_without_active_span() {
        // No subscriber ⇒ no OTel context ⇒ nothing to inject. Safe to call.
        assert2::assert!(current_trace_headers().is_empty());
    }

    #[test]
    fn set_remote_parent_reparents_span_into_header_trace() {
        with_otel_subscriber(|| {
            let traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
            let span = tracing::info_span!("consumer");
            set_remote_parent(&span, [(TRACEPARENT, traceparent.as_bytes())]);

            // The span now belongs to the producer's trace (shares its trace id).
            let sc = span.context().span().span_context().clone();
            assert2::assert!(
                sc.trace_id() == TraceId::from_hex("0af7651916cd43dd8448eb211c80319c").unwrap()
            );
        });
    }
}
