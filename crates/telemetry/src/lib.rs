//! Generic OTLP distributed-tracing pipeline for Crabka services.
//!
//! The consuming service always installs a structured-JSON `tracing_subscriber`
//! `fmt` layer (stdout, gated by the usual `RUST_LOG` `EnvFilter`) so container
//! log collectors (GKE / Cloud Logging, Loki, …) ingest each line as fields
//! rather than ANSI-coloured human text. When OTLP export is
//! configured via the environment, a second `tracing-opentelemetry` layer is
//! attached that converts `tracing` spans into OpenTelemetry spans and
//! batch-exports them over OTLP to a collector (gRPC `:4317` or
//! HTTP/protobuf `:4318`).
//!
//! ## Enabling
//!
//! OTLP is **off by default** — a service with no OTLP environment behaves
//! byte-for-byte as before. It turns on when any endpoint is set
//! (`CRABKA_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`,
//! `OTEL_EXPORTER_OTLP_ENDPOINT`) or `CRABKA_OTLP_ENABLED=true`, and is
//! force-disabled by `OTEL_SDK_DISABLED=true`.
//!
//! ## Resolve OTLP settings without touching the environment
//!
//! ```rust
//! use crabka_telemetry::{OtlpConfig, OtlpProtocol};
//!
//! let cfg = OtlpConfig::from_env(
//!     |key| (key == "CRABKA_OTLP_ENABLED").then(|| "true".to_string()),
//!     "broker-1",
//!     "0.1.0",
//!     "crabka-broker",
//! )
//! .unwrap();
//!
//! assert_eq!(cfg.protocol, OtlpProtocol::Grpc);
//! assert_eq!(cfg.endpoint, "http://localhost:4317");
//! ```

#![forbid(unsafe_code)]

pub mod profiling;
pub mod propagation;

use std::time::Duration;

use opentelemetry::{
    Context, KeyValue,
    trace::{
        Span as _, SpanBuilder, SpanContext, SpanId, TraceContextExt as _, TraceFlags, TraceId,
        TraceState, TracerProvider as _,
    },
};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{LogExporter, Protocol, SpanExporter, WithExportConfig};
use opentelemetry_sdk::{
    Resource,
    logs::SdkLoggerProvider,
    propagation::TraceContextPropagator,
    trace::{Sampler, SdkTracerProvider},
};
use tracing_subscriber::{
    EnvFilter, Layer as _, layer::SubscriberExt as _, util::SubscriberInitExt as _,
};

/// Errors building the OTLP pipeline. Carries the underlying exporter
/// build failure so a misconfigured endpoint surfaces a clear message
/// rather than a silent no-export.
#[derive(Debug, thiserror::Error)]
pub enum TelemetryError {
    #[error("failed to build OTLP span exporter: {0}")]
    Exporter(#[from] opentelemetry_otlp::ExporterBuildError),
}

/// OTLP transport. Mirrors the `OTEL_EXPORTER_OTLP_PROTOCOL` spec values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OtlpProtocol {
    /// gRPC (OTLP/gRPC), default collector port `4317`.
    Grpc,
    /// HTTP with protobuf payloads (OTLP/HTTP), default collector port `4318`.
    HttpProtobuf,
}

impl OtlpProtocol {
    /// Parse an `OTEL_EXPORTER_OTLP_PROTOCOL`-style value. Unknown /
    /// unsupported values fall back to gRPC (the SDK's transport default).
    #[must_use]
    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "http/protobuf" | "http" | "httpbinary" | "http-protobuf" => Self::HttpProtobuf,
            _ => Self::Grpc,
        }
    }

    #[must_use]
    pub fn default_endpoint(self) -> &'static str {
        match self {
            Self::Grpc => "http://localhost:4317",
            Self::HttpProtobuf => "http://localhost:4318",
        }
    }
}

/// Resolved OTLP configuration. Built by [`OtlpConfig::from_env`]; `None`
/// from that constructor means OTLP is disabled and no exporter is built.
#[derive(Debug, Clone)]
pub struct OtlpConfig {
    pub endpoint: String,
    pub protocol: OtlpProtocol,
    /// Head sampling ratio in `[0.0, 1.0]`, wrapped in a parent-based
    /// sampler so child spans honour an upstream sampling decision.
    pub sample_ratio: f64,
    pub service_name: String,
    pub service_version: String,
    pub service_instance_id: String,
    pub timeout: Duration,
    pub heartbeat_interval: Option<Duration>,
}

/// Truthy parse for `*_ENABLED` / `*_DISABLED` style env values.
fn env_truthy(v: &str) -> bool {
    matches!(
        v.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

impl OtlpConfig {
    /// Resolve OTLP config from the environment. `get` is the env lookup
    /// (injected so this is a pure, testable function);
    /// `service_instance_id` is the service instance id (e.g. broker node id),
    /// `service_version` the crate version, and `default_service_name` is the
    /// fallback used when `OTEL_SERVICE_NAME` is not set.
    ///
    /// Returns `None` when OTLP is disabled — either nothing turned it on
    /// or `OTEL_SDK_DISABLED` turned it off.
    #[must_use]
    pub fn from_env(
        get: impl Fn(&str) -> Option<String>,
        service_instance_id: &str,
        service_version: &str,
        default_service_name: &str,
    ) -> Option<Self> {
        if get("OTEL_SDK_DISABLED").as_deref().is_some_and(env_truthy) {
            return None;
        }

        let endpoint_override = get("CRABKA_OTLP_ENDPOINT")
            .or_else(|| get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"))
            .or_else(|| get("OTEL_EXPORTER_OTLP_ENDPOINT"))
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty());

        let explicitly_enabled = get("CRABKA_OTLP_ENABLED")
            .as_deref()
            .is_some_and(env_truthy);

        // Off unless something opts in.
        if endpoint_override.is_none() && !explicitly_enabled {
            return None;
        }

        let protocol = get("CRABKA_OTLP_PROTOCOL")
            .or_else(|| get("OTEL_EXPORTER_OTLP_PROTOCOL"))
            .map_or(OtlpProtocol::Grpc, |s| OtlpProtocol::parse(&s));

        let endpoint = endpoint_override.unwrap_or_else(|| protocol.default_endpoint().to_owned());

        let sample_ratio = get("CRABKA_OTLP_SAMPLE_RATIO")
            .or_else(|| get("OTEL_TRACES_SAMPLER_ARG"))
            .and_then(|s| s.trim().parse::<f64>().ok())
            .map_or(1.0, |r| r.clamp(0.0, 1.0));

        let service_name = get("OTEL_SERVICE_NAME")
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| default_service_name.to_owned());

        let timeout = get("CRABKA_OTLP_TIMEOUT_SECS")
            .or_else(|| get("OTEL_EXPORTER_OTLP_TIMEOUT_SECS"))
            .and_then(|s| s.trim().parse::<u64>().ok())
            .map_or(Duration::from_secs(10), Duration::from_secs);

        let heartbeat_interval = get("CRABKA_OTLP_HEARTBEAT_INTERVAL_SECS")
            .and_then(|s| s.trim().parse::<u64>().ok())
            .and_then(|secs| (secs > 0).then(|| Duration::from_secs(secs)));

        Some(Self {
            endpoint,
            protocol,
            sample_ratio,
            service_name,
            service_version: service_version.to_owned(),
            service_instance_id: service_instance_id.to_owned(),
            timeout,
            heartbeat_interval,
        })
    }

    fn build_exporter(&self) -> Result<SpanExporter, TelemetryError> {
        let builder = SpanExporter::builder();
        let exporter = match self.protocol {
            OtlpProtocol::Grpc => builder
                .with_tonic()
                .with_endpoint(self.endpoint.clone())
                .with_timeout(self.timeout)
                .build()?,
            OtlpProtocol::HttpProtobuf => builder
                .with_http()
                .with_protocol(Protocol::HttpBinary)
                .with_endpoint(self.endpoint.clone())
                .with_timeout(self.timeout)
                .build()?,
        };
        Ok(exporter)
    }

    /// OTLP **log** exporter, mirroring [`Self::build_exporter`] (spans). Lets
    /// services ship their `tracing` logs over OTLP to the logs pipeline instead
    /// of relying on container-stdout tailing.
    fn build_log_exporter(&self) -> Result<LogExporter, TelemetryError> {
        let builder = LogExporter::builder();
        let exporter = match self.protocol {
            OtlpProtocol::Grpc => builder
                .with_tonic()
                .with_endpoint(self.endpoint.clone())
                .with_timeout(self.timeout)
                .build()?,
            OtlpProtocol::HttpProtobuf => builder
                .with_http()
                .with_protocol(Protocol::HttpBinary)
                .with_endpoint(self.endpoint.clone())
                .with_timeout(self.timeout)
                .build()?,
        };
        Ok(exporter)
    }

    fn resource(&self) -> Resource {
        Resource::builder()
            .with_service_name(self.service_name.clone())
            .with_attributes([
                KeyValue::new("service.version", self.service_version.clone()),
                KeyValue::new("service.instance.id", self.service_instance_id.clone()),
            ])
            .build()
    }

    fn sampler(&self) -> Sampler {
        Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(self.sample_ratio)))
    }
}

/// Per-layer filter for the OTLP layer. Overridable with `CRABKA_OTLP_FILTER`
/// for operators who want finer control; falls back to `default`.
fn otel_filter(default: &str, get: impl Fn(&str) -> Option<String>) -> EnvFilter {
    get("CRABKA_OTLP_FILTER")
        .and_then(|s| EnvFilter::try_new(s).ok())
        .unwrap_or_else(|| EnvFilter::new(default))
}

/// Owns the OTLP `SdkTracerProvider` so spans are flushed on shutdown.
/// Dropping also flushes (the provider shuts down when its last clone
/// drops), but call [`TelemetryGuard::shutdown`] explicitly before exit so
/// the final batch is delivered before the process ends.
#[must_use = "hold the guard for the process lifetime and call shutdown() before exit"]
pub struct TelemetryGuard {
    provider: Option<SdkTracerProvider>,
    logger_provider: Option<SdkLoggerProvider>,
    heartbeat_task: Option<tokio::task::JoinHandle<()>>,
}

impl TelemetryGuard {
    /// Flush and shut down the OTLP exporters (traces + logs). No-op when OTLP
    /// is disabled.
    pub fn shutdown(self) {
        if let Some(task) = self.heartbeat_task {
            task.abort();
        }
        if let Some(provider) = self.provider
            && let Err(e) = provider.shutdown()
        {
            tracing::warn!(error = %e, "OTLP tracer provider shutdown error");
        }
        if let Some(logger_provider) = self.logger_provider
            && let Err(e) = logger_provider.shutdown()
        {
            tracing::warn!(error = %e, "OTLP logger provider shutdown error");
        }
    }
}

/// Install the global `tracing` subscriber: a stdout JSON `fmt` layer plus,
/// when `otlp` is `Some`, a batch OTLP export layer.
///
/// - `fmt_default_filter`: the `fmt` layer's filter when `RUST_LOG` is unset.
/// - `otel_default_filter`: the OTLP layer's filter when `CRABKA_OTLP_FILTER`
///   is unset.
/// - `tracer_name`: the name passed to `TracerProvider::tracer(...)`.
///
/// Must be called exactly once, from within the tokio runtime (the
/// gRPC exporter captures the current runtime handle).
pub fn init(
    otlp: Option<OtlpConfig>,
    fmt_default_filter: &str,
    otel_default_filter: &str,
    tracer_name: &str,
) -> Result<TelemetryGuard, TelemetryError> {
    let fmt_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(fmt_default_filter));
    // Structured Cloud Logging-friendly JSON to stdout (see `crabka_logfmt`),
    // so GKE / Cloud Logging ingests fields rather than ANSI-coloured text.
    let fmt_layer = crabka_logfmt::layer(fmt_filter, std::io::stdout);

    let Some(cfg) = otlp else {
        tracing_subscriber::registry().with(fmt_layer).init();
        return Ok(TelemetryGuard {
            provider: None,
            logger_provider: None,
            heartbeat_task: None,
        });
    };

    let exporter = cfg.build_exporter()?;
    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(cfg.resource())
        .with_sampler(cfg.sampler())
        .build();
    let tracer = provider.tracer(tracer_name.to_owned());

    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
    opentelemetry::global::set_tracer_provider(provider.clone());

    let otel_layer = tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_location(false)
        .with_filter(otel_filter(otel_default_filter, |k| std::env::var(k).ok()));

    // OTLP LOGS: a `tracing` → OTLP-log-record bridge so services ship their
    // logs over OTLP (alloy:4317 → logs-distributor) — Docker-stdout tailing
    // doesn't stream on Docker Desktop. Quiet the OTLP/transport stack on THIS
    // layer so the exporter's own events can't feed back into more log records.
    let log_exporter = cfg.build_log_exporter()?;
    let logger_provider = SdkLoggerProvider::builder()
        .with_batch_exporter(log_exporter)
        .with_resource(cfg.resource())
        .build();
    let logs_default = format!(
        "{otel_default_filter},opentelemetry=warn,opentelemetry_sdk=warn,opentelemetry_otlp=warn,hyper_util=warn,tonic=warn,h2=warn,tower=warn"
    );
    let otel_logs_layer = OpenTelemetryTracingBridge::new(&logger_provider)
        .with_filter(otel_filter(&logs_default, |k| std::env::var(k).ok()));

    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(otel_layer)
        .with(otel_logs_layer)
        .init();

    let heartbeat_task = cfg.heartbeat_interval.map(|interval| {
        spawn_heartbeat_task(
            provider.clone(),
            cfg.service_name.clone(),
            cfg.service_instance_id.clone(),
            interval,
        )
    });

    tracing::info!(
        endpoint = %cfg.endpoint,
        protocol = ?cfg.protocol,
        sample_ratio = cfg.sample_ratio,
        heartbeat_interval_secs = cfg.heartbeat_interval.map(|d| d.as_secs()),
        "OTLP distributed tracing + logs enabled"
    );

    Ok(TelemetryGuard {
        provider: Some(provider),
        logger_provider: Some(logger_provider),
        heartbeat_task,
    })
}

fn spawn_heartbeat_task(
    provider: SdkTracerProvider,
    service_name: String,
    service_instance_id: String,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let tracer = provider.tracer("crabka-telemetry-heartbeat");
        let mut sequence = 0_i64;
        loop {
            emit_heartbeat_span(&tracer, &service_name, &service_instance_id, sequence);
            sequence = sequence.saturating_add(1);
            tokio::time::sleep(interval).await;
        }
    })
}

fn heartbeat_parent_context(sequence: i64) -> Context {
    let trace_sequence = u128::try_from(sequence).unwrap_or(0);
    let span_sequence = u64::try_from(sequence).unwrap_or(0);
    let trace_id =
        TraceId::from(0x6372_6162_6b61_5f68_6561_7274_0000_0001_u128.wrapping_add(trace_sequence));
    let span_id = SpanId::from(0x6865_6172_7400_0001_u64.wrapping_add(span_sequence));
    Context::new().with_remote_span_context(SpanContext::new(
        trace_id,
        span_id,
        TraceFlags::SAMPLED,
        true,
        TraceState::default(),
    ))
}

fn emit_heartbeat_span<T: opentelemetry::trace::Tracer>(
    tracer: &T,
    service_name: &str,
    service_instance_id: &str,
    sequence: i64,
) {
    let mut span = tracer.build_with_context(
        SpanBuilder::from_name("crabka.telemetry.heartbeat").with_attributes([
            KeyValue::new("crabka.telemetry.signal", "heartbeat"),
            KeyValue::new("crabka.telemetry.service_name", service_name.to_owned()),
            KeyValue::new(
                "crabka.telemetry.service_instance_id",
                service_instance_id.to_owned(),
            ),
            KeyValue::new("crabka.telemetry.sequence", sequence),
        ]),
        &heartbeat_parent_context(sequence),
    );
    span.end();
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering::SeqCst},
    };

    use assert2::{assert, check};
    use opentelemetry_sdk::error::OTelSdkResult;

    use super::*;

    #[derive(Clone, Debug)]
    struct ShutdownCountingSpanExporter {
        calls: Arc<AtomicUsize>,
    }

    impl opentelemetry_sdk::trace::SpanExporter for ShutdownCountingSpanExporter {
        fn export(
            &self,
            _batch: Vec<opentelemetry_sdk::trace::SpanData>,
        ) -> impl std::future::Future<Output = OTelSdkResult> + Send {
            std::future::ready(Ok(()))
        }

        fn shutdown_with_timeout(&self, _timeout: std::time::Duration) -> OTelSdkResult {
            self.calls.fetch_add(1, SeqCst);
            Ok(())
        }
    }

    #[derive(Clone, Debug, Default)]
    struct RecordingSpanExporter {
        spans: Arc<Mutex<Vec<opentelemetry_sdk::trace::SpanData>>>,
    }

    impl RecordingSpanExporter {
        fn spans(&self) -> Vec<opentelemetry_sdk::trace::SpanData> {
            self.spans.lock().expect("recorded spans lock").clone()
        }
    }

    impl opentelemetry_sdk::trace::SpanExporter for RecordingSpanExporter {
        fn export(
            &self,
            batch: Vec<opentelemetry_sdk::trace::SpanData>,
        ) -> impl std::future::Future<Output = OTelSdkResult> + Send {
            self.spans
                .lock()
                .expect("recorded spans lock")
                .extend(batch);
            std::future::ready(Ok(()))
        }
    }

    fn env_from<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |k: &str| {
            pairs
                .iter()
                .find(|(key, _)| *key == k)
                .map(|(_, v)| (*v).to_owned())
        }
    }

    #[test]
    fn disabled_when_no_env() {
        let cfg = OtlpConfig::from_env(env_from(&[]), "1", "0.1.1", "crabka-broker");
        assert!(cfg.is_none());
    }

    #[test]
    fn env_truthy_rejects_falsy_values() {
        for value in ["", "0", "false", "no", "off", " definitely "] {
            assert!(!env_truthy(value), "expected {value:?} to be falsy");
        }
    }

    #[test]
    fn falsy_env_flags_do_not_enable_or_disable_otlp() {
        let cfg = OtlpConfig::from_env(
            env_from(&[("CRABKA_OTLP_ENABLED", "false")]),
            "1",
            "0.1.1",
            "crabka-broker",
        );
        assert!(cfg.is_none());

        let cfg = OtlpConfig::from_env(
            env_from(&[
                ("CRABKA_OTLP_ENDPOINT", "http://collector:4317"),
                ("OTEL_SDK_DISABLED", "off"),
            ]),
            "1",
            "0.1.1",
            "crabka-broker",
        );
        assert!(cfg.is_some());
    }

    #[test]
    fn enabled_by_crabka_endpoint() {
        let cfg = OtlpConfig::from_env(
            env_from(&[("CRABKA_OTLP_ENDPOINT", "http://collector:4317")]),
            "7",
            "0.1.1",
            "crabka-broker",
        )
        .expect("enabled");
        assert_eq!(cfg.endpoint.as_str(), "http://collector:4317");
        assert_eq!(cfg.protocol, OtlpProtocol::Grpc);
        assert_eq!(cfg.sample_ratio, 1.0);
        assert_eq!(cfg.service_name.as_str(), "crabka-broker");
        assert_eq!(cfg.service_instance_id.as_str(), "7");
        assert_eq!(cfg.service_version.as_str(), "0.1.1");
    }

    #[test]
    fn enabled_flag_uses_protocol_default_endpoint() {
        let cfg = OtlpConfig::from_env(
            env_from(&[
                ("CRABKA_OTLP_ENABLED", "true"),
                ("CRABKA_OTLP_PROTOCOL", "http/protobuf"),
            ]),
            "1",
            "0.1.1",
            "crabka-broker",
        )
        .expect("enabled");
        assert_eq!(cfg.protocol, OtlpProtocol::HttpProtobuf);
        assert_eq!(cfg.endpoint.as_str(), "http://localhost:4318");
    }

    #[test]
    fn grpc_is_the_default_protocol() {
        let cfg = OtlpConfig::from_env(
            env_from(&[("CRABKA_OTLP_ENABLED", "1")]),
            "1",
            "0.1.1",
            "crabka-broker",
        )
        .expect("enabled");
        assert_eq!(cfg.protocol, OtlpProtocol::Grpc);
        assert_eq!(cfg.endpoint.as_str(), "http://localhost:4317");
    }

    #[test]
    fn sdk_disabled_overrides_endpoint() {
        let cfg = OtlpConfig::from_env(
            env_from(&[
                ("CRABKA_OTLP_ENDPOINT", "http://collector:4317"),
                ("OTEL_SDK_DISABLED", "true"),
            ]),
            "1",
            "0.1.1",
            "crabka-broker",
        );
        assert!(cfg.is_none());
    }

    #[test]
    fn endpoint_precedence_and_standard_vars() {
        // Standard OTLP env (no CRABKA_ override) still enables export.
        let cfg = OtlpConfig::from_env(
            env_from(&[("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel:4317")]),
            "1",
            "0.1.1",
            "crabka-broker",
        )
        .expect("enabled");
        assert_eq!(cfg.endpoint, "http://otel:4317");

        // Traces-specific endpoint wins over the generic one.
        let cfg = OtlpConfig::from_env(
            env_from(&[
                ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://generic:4317"),
                ("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://traces:4317"),
            ]),
            "1",
            "0.1.1",
            "crabka-broker",
        )
        .expect("enabled");
        assert_eq!(cfg.endpoint, "http://traces:4317");

        // CRABKA override wins over everything.
        let cfg = OtlpConfig::from_env(
            env_from(&[
                ("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://traces:4317"),
                ("CRABKA_OTLP_ENDPOINT", "http://crabka:4317"),
            ]),
            "1",
            "0.1.1",
            "crabka-broker",
        )
        .expect("enabled");
        assert_eq!(cfg.endpoint, "http://crabka:4317");
    }

    #[test]
    fn sample_ratio_parsed_and_clamped() {
        let cfg = OtlpConfig::from_env(
            env_from(&[
                ("CRABKA_OTLP_ENDPOINT", "http://c:4317"),
                ("CRABKA_OTLP_SAMPLE_RATIO", "0.25"),
            ]),
            "1",
            "0.1.1",
            "crabka-broker",
        )
        .expect("enabled");
        assert_eq!(cfg.sample_ratio, 0.25);

        // Out-of-range clamps to [0,1].
        let cfg = OtlpConfig::from_env(
            env_from(&[
                ("CRABKA_OTLP_ENABLED", "true"),
                ("CRABKA_OTLP_SAMPLE_RATIO", "9.0"),
            ]),
            "1",
            "0.1.1",
            "crabka-broker",
        )
        .expect("enabled");
        assert_eq!(cfg.sample_ratio, 1.0);
    }

    #[test]
    fn heartbeat_interval_is_disabled_by_default_and_ignores_zero() {
        let cfg = OtlpConfig::from_env(
            env_from(&[("CRABKA_OTLP_ENDPOINT", "http://c:4317")]),
            "1",
            "0.1.1",
            "crabka-broker",
        )
        .expect("enabled");
        assert!(cfg.heartbeat_interval.is_none());

        let cfg = OtlpConfig::from_env(
            env_from(&[
                ("CRABKA_OTLP_ENDPOINT", "http://c:4317"),
                ("CRABKA_OTLP_HEARTBEAT_INTERVAL_SECS", "0"),
            ]),
            "1",
            "0.1.1",
            "crabka-broker",
        )
        .expect("enabled");
        assert!(cfg.heartbeat_interval.is_none());
    }

    #[test]
    fn heartbeat_interval_parses_from_env() {
        let cfg = OtlpConfig::from_env(
            env_from(&[
                ("CRABKA_OTLP_ENDPOINT", "http://c:4317"),
                ("CRABKA_OTLP_HEARTBEAT_INTERVAL_SECS", "15"),
            ]),
            "1",
            "0.1.1",
            "crabka-broker",
        )
        .expect("enabled");
        assert_eq!(cfg.heartbeat_interval, Some(Duration::from_secs(15)));
    }

    #[test]
    fn heartbeat_span_is_sampled_when_ratio_sampler_would_drop_roots() {
        use opentelemetry::trace::TraceFlags;
        use opentelemetry_sdk::trace::SimpleSpanProcessor;

        let exporter = RecordingSpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
                0.0,
            ))))
            .with_span_processor(SimpleSpanProcessor::new(exporter.clone()))
            .build();
        let tracer = provider.tracer("crabka-telemetry-test");

        emit_heartbeat_span(&tracer, "crabka-broker", "broker-1", 7);
        provider.force_flush().expect("flush spans");

        let spans = exporter.spans();
        assert_eq!(spans.len(), 1);
        let span = &spans[0];
        assert_eq!(span.name, "crabka.telemetry.heartbeat");
        check!(span.parent_span_is_remote);
        assert_eq!(span.span_context.trace_flags(), TraceFlags::SAMPLED);
        check!(
            span.attributes
                .iter()
                .any(|attr| attr.key.as_str() == "crabka.telemetry.sequence"
                    && attr.value == opentelemetry::Value::I64(7))
        );
    }

    #[test]
    fn service_name_and_timeout_overrides() {
        let cfg = OtlpConfig::from_env(
            env_from(&[
                ("CRABKA_OTLP_ENDPOINT", "http://c:4317"),
                ("OTEL_SERVICE_NAME", "my-kafka"),
                ("CRABKA_OTLP_TIMEOUT_SECS", "3"),
            ]),
            "9",
            "0.1.1",
            "crabka-broker",
        )
        .expect("enabled");
        assert_eq!(cfg.service_name.as_str(), "my-kafka");
        assert_eq!(cfg.timeout, Duration::from_secs(3));
    }

    #[test]
    fn protocol_parse_variants() {
        for (input, want) in [
            ("grpc", OtlpProtocol::Grpc),
            ("http/protobuf", OtlpProtocol::HttpProtobuf),
            ("HTTP", OtlpProtocol::HttpProtobuf),
            ("nonsense", OtlpProtocol::Grpc),
        ] {
            assert_eq!(OtlpProtocol::parse(input), want, "case {input:?}");
        }
    }

    #[test]
    fn otel_filter_uses_override_or_default() {
        let filter = otel_filter("warn", env_from(&[]));
        assert_eq!(filter.to_string(), "warn");

        let filter = otel_filter(
            "warn",
            env_from(&[("CRABKA_OTLP_FILTER", "crabka_telemetry=debug")]),
        );
        assert_eq!(filter.to_string(), "crabka_telemetry=debug");

        let filter = otel_filter("warn", env_from(&[("CRABKA_OTLP_FILTER", "[")]));
        assert_eq!(filter.to_string(), "warn");
    }

    #[test]
    fn telemetry_guard_shutdown_flushes_held_provider_clone() {
        let calls = Arc::new(AtomicUsize::new(0));
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(ShutdownCountingSpanExporter {
                calls: Arc::clone(&calls),
            })
            .build();
        let held_clone = provider.clone();

        TelemetryGuard {
            provider: Some(provider),
            logger_provider: None,
            heartbeat_task: None,
        }
        .shutdown();

        assert_eq!(calls.load(SeqCst), 1);
        drop(held_clone);
    }
}
