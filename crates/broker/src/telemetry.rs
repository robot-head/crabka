//! Broker tracing + OTLP distributed-tracing pipeline (slice 42).
//!
//! The broker always installs a `tracing_subscriber` `fmt` layer (stdout,
//! gated by the usual `RUST_LOG` `EnvFilter`). When OTLP export is
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
//! slice surfaces these knobs through `Kafka.spec` and injects the env on
//! the broker pods.
//!
//! ## Request spans
//!
//! Per-request spans are emitted under the dedicated
//! [`REQUEST_TARGET`] target at `DEBUG`, so they cost nothing (a disabled
//! level check) on a broker without OTLP, and the stdout `fmt` layer never
//! prints them. Only the OTLP layer enables that target (see
//! [`OtlpConfig::otel_filter`]).

use std::net::SocketAddr;
use std::time::Duration;

use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{Protocol, SpanExporter, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;
use tracing_subscriber::{EnvFilter, Layer as _};

/// `tracing` target carrying per-request server spans. Kept off the `fmt`
/// layer's default filter so request spans only materialise for OTLP.
pub const REQUEST_TARGET: &str = "crabka_broker::request";

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
    fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "http/protobuf" | "http" | "httpbinary" | "http-protobuf" => Self::HttpProtobuf,
            _ => Self::Grpc,
        }
    }

    fn default_endpoint(self) -> &'static str {
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
    /// `service_instance_id` is the broker id and `service_version` the
    /// crate version, both supplied by the caller.
    ///
    /// Returns `None` when OTLP is disabled — either nothing turned it on
    /// or `OTEL_SDK_DISABLED` turned it off.
    #[must_use]
    pub fn from_env(
        get: impl Fn(&str) -> Option<String>,
        service_instance_id: &str,
        service_version: &str,
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
            .unwrap_or_else(|| "crabka-broker".to_owned());

        let timeout = get("CRABKA_OTLP_TIMEOUT_SECS")
            .or_else(|| get("OTEL_EXPORTER_OTLP_TIMEOUT_SECS"))
            .and_then(|s| s.trim().parse::<u64>().ok())
            .map_or(Duration::from_secs(10), Duration::from_secs);

        Some(Self {
            endpoint,
            protocol,
            sample_ratio,
            service_name,
            service_version: service_version.to_owned(),
            service_instance_id: service_instance_id.to_owned(),
            timeout,
        })
    }

    /// Per-layer filter for the OTLP layer: capture everything the `fmt`
    /// layer sees at `INFO` plus the per-request `DEBUG` spans. Overridable
    /// with `CRABKA_OTLP_FILTER` for operators who want finer control.
    fn otel_filter(get: impl Fn(&str) -> Option<String>) -> EnvFilter {
        get("CRABKA_OTLP_FILTER")
            .and_then(|s| EnvFilter::try_new(s).ok())
            .unwrap_or_else(|| {
                EnvFilter::new(format!("info,{REQUEST_TARGET}=debug,crabka_log=info"))
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

/// Owns the OTLP `SdkTracerProvider` so spans are flushed on shutdown.
/// Dropping also flushes (the provider shuts down when its last clone
/// drops), but call [`TelemetryGuard::shutdown`] explicitly before exit so
/// the final batch is delivered before the process ends.
#[must_use = "hold the guard for the process lifetime and call shutdown() before exit"]
pub struct TelemetryGuard {
    provider: Option<SdkTracerProvider>,
}

impl TelemetryGuard {
    /// Flush and shut down the OTLP exporter. No-op when OTLP is disabled.
    pub fn shutdown(self) {
        if let Some(provider) = self.provider
            && let Err(e) = provider.shutdown()
        {
            tracing::warn!(error = %e, "OTLP tracer provider shutdown error");
        }
    }
}

/// Install the global `tracing` subscriber: a stdout `fmt` layer plus,
/// when `otlp` is `Some`, a batch OTLP export layer.
///
/// `default_filter` is the `fmt` layer's filter when `RUST_LOG` is unset.
/// Must be called exactly once, from within the tokio runtime (the
/// gRPC exporter captures the current runtime handle).
pub fn init(
    otlp: Option<OtlpConfig>,
    default_filter: &str,
) -> Result<TelemetryGuard, TelemetryError> {
    let fmt_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter));
    let fmt_layer = tracing_subscriber::fmt::layer().with_filter(fmt_filter);

    let Some(cfg) = otlp else {
        tracing_subscriber::registry().with(fmt_layer).init();
        return Ok(TelemetryGuard { provider: None });
    };

    let exporter = cfg.build_exporter()?;
    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(cfg.resource())
        .with_sampler(cfg.sampler())
        .build();
    let tracer = provider.tracer("crabka-broker");

    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
    opentelemetry::global::set_tracer_provider(provider.clone());

    let otel_layer = tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_location(false)
        .with_filter(OtlpConfig::otel_filter(|k| std::env::var(k).ok()));

    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(otel_layer)
        .init();

    tracing::info!(
        endpoint = %cfg.endpoint,
        protocol = ?cfg.protocol,
        sample_ratio = cfg.sample_ratio,
        "OTLP distributed tracing enabled"
    );

    Ok(TelemetryGuard {
        provider: Some(provider),
    })
}

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
/// the `OTel` span name. Unknown keys (newer than what the broker handles)
/// render as `"Unknown"` so a span is still emitted.
#[must_use]
pub fn api_name(api_key: i16) -> &'static str {
    match api_key {
        0 => "Produce",
        1 => "Fetch",
        2 => "ListOffsets",
        3 => "Metadata",
        8 => "OffsetCommit",
        9 => "OffsetFetch",
        10 => "FindCoordinator",
        11 => "JoinGroup",
        12 => "Heartbeat",
        13 => "LeaveGroup",
        14 => "SyncGroup",
        15 => "DescribeGroups",
        16 => "ListGroups",
        17 => "SaslHandshake",
        18 => "ApiVersions",
        19 => "CreateTopics",
        20 => "DeleteTopics",
        21 => "DeleteRecords",
        22 => "InitProducerId",
        23 => "OffsetForLeaderEpoch",
        24 => "AddPartitionsToTxn",
        25 => "AddOffsetsToTxn",
        26 => "EndTxn",
        28 => "TxnOffsetCommit",
        32 => "DescribeConfigs",
        33 => "AlterConfigs",
        36 => "SaslAuthenticate",
        37 => "CreatePartitions",
        42 => "DeleteGroups",
        44 => "IncrementalAlterConfigs",
        47 => "OffsetDelete",
        50 => "DescribeUserScramCredentials",
        51 => "AlterUserScramCredentials",
        55 => "DescribeQuorum",
        56 => "AlterPartition",
        57 => "UpdateFeatures",
        60 => "DescribeCluster",
        _ => api_name_admin(api_key),
    }
}

/// Second arm of [`api_name`] — ACL / quota / reassignment / leader-election
/// admin keys. Split out to keep the primary `match` under clippy's
/// arm-count lints.
fn api_name_admin(api_key: i16) -> &'static str {
    match api_key {
        29 => "DescribeAcls",
        30 => "CreateAcls",
        31 => "DeleteAcls",
        43 => "ElectLeaders",
        45 => "AlterPartitionReassignments",
        46 => "ListPartitionReassignments",
        48 => "DescribeClientQuotas",
        49 => "AlterClientQuotas",
        _ => "Unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let cfg = OtlpConfig::from_env(env_from(&[]), "1", "0.1.1");
        assert!(cfg.is_none());
    }

    #[test]
    fn enabled_by_crabka_endpoint() {
        let cfg = OtlpConfig::from_env(
            env_from(&[("CRABKA_OTLP_ENDPOINT", "http://collector:4317")]),
            "7",
            "0.1.1",
        )
        .expect("enabled");
        assert_eq!(cfg.endpoint, "http://collector:4317");
        assert_eq!(cfg.protocol, OtlpProtocol::Grpc);
        assert!((cfg.sample_ratio - 1.0).abs() < f64::EPSILON);
        assert_eq!(cfg.service_name, "crabka-broker");
        assert_eq!(cfg.service_instance_id, "7");
        assert_eq!(cfg.service_version, "0.1.1");
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
        )
        .expect("enabled");
        assert_eq!(cfg.protocol, OtlpProtocol::HttpProtobuf);
        assert_eq!(cfg.endpoint, "http://localhost:4318");
    }

    #[test]
    fn grpc_is_the_default_protocol() {
        let cfg = OtlpConfig::from_env(env_from(&[("CRABKA_OTLP_ENABLED", "1")]), "1", "0.1.1")
            .expect("enabled");
        assert_eq!(cfg.protocol, OtlpProtocol::Grpc);
        assert_eq!(cfg.endpoint, "http://localhost:4317");
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
        )
        .expect("enabled");
        assert!((cfg.sample_ratio - 0.25).abs() < f64::EPSILON);

        // Out-of-range clamps to [0,1].
        let cfg = OtlpConfig::from_env(
            env_from(&[
                ("CRABKA_OTLP_ENABLED", "true"),
                ("CRABKA_OTLP_SAMPLE_RATIO", "9.0"),
            ]),
            "1",
            "0.1.1",
        )
        .expect("enabled");
        assert!((cfg.sample_ratio - 1.0).abs() < f64::EPSILON);
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
        )
        .expect("enabled");
        assert_eq!(cfg.service_name, "my-kafka");
        assert_eq!(cfg.timeout, Duration::from_secs(3));
    }

    #[test]
    fn protocol_parse_variants() {
        assert_eq!(OtlpProtocol::parse("grpc"), OtlpProtocol::Grpc);
        assert_eq!(
            OtlpProtocol::parse("http/protobuf"),
            OtlpProtocol::HttpProtobuf
        );
        assert_eq!(OtlpProtocol::parse("HTTP"), OtlpProtocol::HttpProtobuf);
        assert_eq!(OtlpProtocol::parse("nonsense"), OtlpProtocol::Grpc);
    }

    #[test]
    fn api_name_known_and_unknown() {
        assert_eq!(api_name(0), "Produce");
        assert_eq!(api_name(1), "Fetch");
        assert_eq!(api_name(18), "ApiVersions");
        assert_eq!(api_name(51), "AlterUserScramCredentials");
        assert_eq!(api_name(30), "CreateAcls");
        assert_eq!(api_name(9999), "Unknown");
    }

    #[test]
    fn request_span_records_otel_name() {
        use std::sync::{Arc, Mutex};
        use tracing::field::{Field, Visit};
        use tracing::span::{Attributes, Record};
        use tracing_subscriber::Layer;
        use tracing_subscriber::layer::Context;
        use tracing_subscriber::prelude::*;

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
        assert_eq!(g.name.as_deref(), Some("Produce"));
        assert_eq!(g.kind.as_deref(), Some("server"));
        assert_eq!(g.api_key, Some(0));
    }
}
