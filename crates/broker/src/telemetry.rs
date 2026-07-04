//! Broker tracing + OTLP distributed-tracing pipeline.
//!
//! The broker always installs a structured-JSON `tracing_subscriber` `fmt`
//! layer (stdout, gated by the usual `RUST_LOG` `EnvFilter`) so GKE / Cloud
//! Logging ingests fields rather than ANSI text. When OTLP export is
//! configured via the environment, a second `tracing-opentelemetry` layer
//! is attached that converts `tracing` spans into OpenTelemetry spans and
//! batch-exports them over OTLP to a collector (gRPC `:4317` or
//! HTTP/protobuf `:4318`).
//!
//! ## Enabling
//!
//! OTLP is **off by default** — a broker with no OTLP environment behaves
//! byte-for-byte as before. It turns on when any endpoint is set
//! (`CRABKA_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`,
//! `OTEL_EXPORTER_OTLP_ENDPOINT`) or `CRABKA_OTLP_ENABLED=true`, and is
//! force-disabled by `OTEL_SDK_DISABLED=true`. The follow-up operator
//! the operator surfaces these knobs through `Kafka.spec` and injects the env on
//! the broker pods.
//!
//! ## Request spans
//!
//! Per-request spans are emitted under the dedicated
//! [`REQUEST_TARGET`] target at `DEBUG`, so they cost nothing (a disabled
//! level check) on a broker without OTLP, and the stdout `fmt` layer never
//! prints them. Only the OTLP layer enables that target (via the
//! `otel_default_filter` passed to [`init`]).

use std::net::SocketAddr;

// Re-export the generic OTLP pipeline from crabka-telemetry.
pub use crabka_telemetry::{OtlpConfig, OtlpProtocol, TelemetryError, TelemetryGuard, init};

/// `tracing` target carrying per-request server spans. Kept off the `fmt`
/// layer's default filter so request spans only materialise for OTLP.
pub const REQUEST_TARGET: &str = "crabka_broker::request";

/// Build the per-request server span. Disabled (zero-cost) unless the OTLP
/// layer has enabled [`REQUEST_TARGET`] at `DEBUG`. The span name is set to
/// the Kafka API name via the `otel.name` field that `tracing-opentelemetry`
/// recognises; attribute names follow OpenTelemetry semantic conventions.
#[must_use]
pub fn request_span(
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    client_id: Option<&str>,
    peer: &SocketAddr,
) -> tracing::Span {
    let span = tracing::debug_span!(
        target: REQUEST_TARGET,
        "kafka.request",
        otel.kind = "server",
        otel.name = tracing::field::Empty,
        messaging.system = "kafka",
        kafka.api_key = api_key,
        kafka.api_version = api_version,
        kafka.correlation_id = correlation_id,
        messaging.kafka.client_id = client_id.unwrap_or(""),
        network.peer.address = %peer,
    );
    span.record("otel.name", api_name(api_key));
    span
}

/// Map a Kafka request `api_key` to its canonical protocol name, used as
/// the `OTel` span name. The name is sourced from the generated
/// [`crabka_protocol::ApiKey`] registry (whose variant names are the
/// canonical Kafka request names), so it stays in sync with the schemas.
/// Keys outside the registry render as `"Unknown"` so a span is still
/// emitted.
#[must_use]
pub fn api_name(api_key: i16) -> &'static str {
    crabka_protocol::ApiKey::from_i16(api_key).map_or("Unknown", Into::into)
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    #[test]
    fn api_name_known_and_unknown() {
        for (api_key, want) in [
            (0, "Produce"),
            (1, "Fetch"),
            (18, "ApiVersions"),
            (51, "AlterUserScramCredentials"),
            (30, "CreateAcls"),
            (9999, "Unknown"),
        ] {
            assert!(api_name(api_key) == want, "api key {api_key}");
        }
    }

    #[test]
    fn request_span_records_otel_name() {
        use std::sync::{Arc, Mutex};

        use tracing::{
            field::{Field, Visit},
            span::{Attributes, Record},
        };
        use tracing_subscriber::{Layer, layer::Context, prelude::*};

        #[derive(Default)]
        struct Captured {
            name: Option<String>,
            api_key: Option<i64>,
            kind: Option<String>,
        }
        // `otel.kind` / `kafka.api_key` arrive at span creation; `otel.name`
        // is set afterwards via `Span::record`, so capture both callbacks.
        struct V<'a>(&'a mut Captured);
        impl Visit for V<'_> {
            fn record_debug(&mut self, _f: &Field, _v: &dyn std::fmt::Debug) {}
            fn record_str(&mut self, f: &Field, v: &str) {
                match f.name() {
                    "otel.name" => self.0.name = Some(v.to_owned()),
                    "otel.kind" => self.0.kind = Some(v.to_owned()),
                    _ => {}
                }
            }
            fn record_i64(&mut self, f: &Field, v: i64) {
                if f.name() == "kafka.api_key" {
                    self.0.api_key = Some(v);
                }
            }
        }
        struct Cap(Arc<Mutex<Captured>>);
        impl<S: tracing::Subscriber> Layer<S> for Cap {
            fn on_new_span(&self, attrs: &Attributes<'_>, _id: &tracing::Id, _ctx: Context<'_, S>) {
                attrs.record(&mut V(&mut self.0.lock().unwrap()));
            }
            fn on_record(&self, _id: &tracing::Id, values: &Record<'_>, _ctx: Context<'_, S>) {
                values.record(&mut V(&mut self.0.lock().unwrap()));
            }
        }

        let captured = Arc::new(Mutex::new(Captured::default()));
        let subscriber = tracing_subscriber::registry().with(
            Cap(captured.clone()).with_filter(tracing_subscriber::filter::LevelFilter::DEBUG),
        );
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        tracing::subscriber::with_default(subscriber, || {
            let _span = request_span(0, 9, 42, Some("my-client"), &peer);
        });

        let g = captured.lock().unwrap();
        check!(g.name.as_deref() == Some("Produce"));
        check!(g.kind.as_deref() == Some("server"));
        check!(g.api_key == Some(0));
    }
}
