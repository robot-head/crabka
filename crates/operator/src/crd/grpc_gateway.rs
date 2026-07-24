//! `KafkaGrpcGateway` CRD. Represents a deployment of the
//! `crabka-grpc-gateway` binary that the operator manages — it produces
//! a Deployment, Service, serving-cert Secret, config Secret, and a
//! child `KafkaUser` for the gateway's broker-mTLS client identity.
//! The parent Kafka cluster is discovered from the `crabka.io/cluster`
//! label (same convention as `KafkaTopic` / `KafkaUser`).

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::ResourceRequirements;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[kube(
    group = "crabka.io",
    version = "v1alpha1",
    kind = "KafkaGrpcGateway",
    plural = "kafkagrpcgateways",
    singular = "kafkagrpcgateway",
    shortname = "kgg",
    namespaced,
    status = "KafkaGrpcGatewayStatus",
    derive = "PartialEq"
)]
#[serde(rename_all = "camelCase")]
pub struct KafkaGrpcGatewaySpec {
    /// Number of gateway replicas. Defaults to 1 when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub replicas: Option<i32>,

    /// Container image override. When absent the operator uses its
    /// `--default-gateway-image` flag.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,

    /// CPU / memory resource requests and limits for the gateway container.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceRequirements>,

    /// Configuration for the deduplication topic that backs idempotent
    /// produce (exactly-once delivery). When absent, controller defaults apply.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dedup: Option<DedupSpec>,

    /// Internal membership / owner-routing topic.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub membership_topic: Option<String>,

    /// Gateway runtime policy overrides.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tuning: Option<GatewayTuning>,

    /// Schema Registry integration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_registry: Option<GatewaySchemaRegistrySpec>,

    /// Kubernetes readiness and liveness probe timing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health_checks: Option<GatewayHealthChecks>,

    /// TLS serving configuration. When absent, TLS defaults apply
    /// (`clientAuth: required`, `validityDays: 365`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<GatewayTlsSpec>,

    /// Authorization configuration. When absent, simple ACL-based authz
    /// is used (mode = `simple`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authz: Option<GatewayAuthzSpec>,

    /// Inbound HTTP-webhook endpoints. Each entry creates one
    /// authenticated ingress route that produces records to
    /// `targetTopic`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub webhooks: Vec<InboundWebhookSpec>,

    /// Outbound webhook subscriptions. Each entry reads from
    /// `sourceTopics` and HTTP-POSTs records to `targetUrl`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outbound_subscriptions: Vec<OutboundSubscriptionSpec>,

    /// Explicit SSRF allowlist for outbound HTTP targets. The controller
    /// derives entries automatically from each subscription's
    /// `targetUrl`; use this field to add extra allowed hosts.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_targets: Vec<AllowedTargetSpec>,

    /// OpenTelemetry / observability configuration. When absent,
    /// the gateway exports no telemetry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub telemetry: Option<TelemetrySpec>,
}

/// Configuration for the per-gateway deduplication Kafka topic.
/// The topic is created by the operator; the gateway uses transactional
/// produce against it to provide exactly-once delivery.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DedupSpec {
    /// Kafka topic used to store dedup state. Defaults to
    /// `<gateway-name>-dedup` when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,

    /// Number of partitions for the dedup topic. Default 8.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1, max = 2_147_483_647))]
    pub partitions: Option<u32>,

    /// Dedup window in milliseconds. Records with the same idempotency
    /// key within this window are dropped. Default `86_400_000` (24 h).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub window_ms: Option<i64>,

    /// Prefix for transactional producer IDs. Defaults to the gateway
    /// name. The full `transactional.id` is `<prefix>-<partition>`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub txn_id_prefix: Option<String>,

    /// Consumer group used to divide dedup ownership between replicas.
    /// Defaults to a value derived from the gateway name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ownership_group: Option<String>,
}

/// Runtime policy passed to the gateway process.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GatewayTuning {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub internal_topic_replication_factor: Option<i16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub internal_topic_allow_replication_fallback: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub internal_topic_create_timeout_ms: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub internal_topic_segment_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 0, max = 10_000))]
    pub internal_topic_min_cleanable_dirty_ratio_basis_points: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub consumer_poll_timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub ownership_warmup_empty_polls: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub readiness_poll_interval_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub produce_max_body_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub forward_max_body_bytes: Option<u64>,
}

/// Schema Registry settings for structured records.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GatewaySchemaRegistrySpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub latest_cache_ttl_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frame_raw: Option<bool>,
}

/// Kubernetes readiness and liveness probe timing.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GatewayHealthChecks {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 0))]
    pub readiness_initial_delay_seconds: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub readiness_period_seconds: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 0))]
    pub liveness_initial_delay_seconds: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub liveness_period_seconds: Option<i32>,
}

/// TLS serving configuration for the gateway's gRPC / webhook / metrics
/// endpoints. The serving cert is issued by the operator from the
/// cluster CA.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GatewayTlsSpec {
    /// How the gateway authenticates inbound clients.
    /// One of `disabled`, `optional`, `required`. Default `required`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_auth: Option<String>,

    /// Serving-cert lifetime in days. Default 365.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub validity_days: Option<u32>,

    /// Cert hot-reload poll interval in seconds. Default 30.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub reload_interval_secs: Option<u64>,
}

/// Authorization configuration for the gateway.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GatewayAuthzSpec {
    /// Authorization mode. One of `off` or `simple`. Default `simple`
    /// (ACL-based, reading `KafkaUser` ACLs from the broker).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,

    /// Principal strings (e.g. `User:admin`) that bypass all ACL checks.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub super_users: Vec<String>,

    /// How often the gateway refreshes its ACL cache from the broker,
    /// in seconds. Default 60.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub acl_refresh_secs: Option<u64>,

    /// Bearer-token authentication configuration. When absent,
    /// bearer auth is disabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bearer: Option<GatewayBearerSpec>,
}

/// Bearer-token authentication for the gateway.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GatewayBearerSpec {
    /// Bearer auth mode. One of `off` or `unsecured`. Default `off`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,

    /// JWT claim used as the Kafka principal. Default `sub`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal_claim: Option<String>,

    /// Allowable clock skew for bearer-token timestamps in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 0))]
    pub allowable_clock_skew_ms: Option<i64>,
}

/// One inbound HTTP-webhook endpoint. Records a produce call against
/// `targetTopic` for every verified POST to `/<name>`.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct InboundWebhookSpec {
    /// Unique name for this webhook route. Used as the URL path segment.
    pub name: String,

    /// Kafka topic that records are produced to.
    pub target_topic: String,

    /// Kafka principal for ACL checks on produce to `targetTopic`.
    /// Defaults to the gateway service account principal when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal: Option<String>,

    /// HTTP header carrying the HMAC signature. E.g.
    /// `X-Hub-Signature-256`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature_header: Option<String>,

    /// Signature encoding. One of `hex` or `base64`. Default `hex`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature_encoding: Option<String>,

    /// Prefix to strip from the signature header value before
    /// verifying. E.g. `sha256=`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature_prefix: Option<String>,

    /// HTTP header carrying the request timestamp. Used with
    /// `timestampToleranceSecs` to reject replayed requests.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp_header: Option<String>,

    /// Maximum age of a request timestamp in seconds before it is
    /// rejected as a replay. Default 300 (5 minutes).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 0))]
    pub timestamp_tolerance_secs: Option<i64>,

    /// How to derive the idempotency key for deduplication. E.g.
    /// `header:X-Idempotency-Key` or `body_hash`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_source: Option<String>,

    /// How to derive the Kafka record key from the request. E.g.
    /// `header:X-Record-Key` or `body_path:.id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_source: Option<String>,

    /// Maximum accepted request body size in bytes. Default 1 MiB.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub max_body_bytes: Option<u64>,

    /// Optional Schema Registry subject for structured request bodies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_subject: Option<String>,

    /// Structured payload format: `avro`, `json`, or `protobuf`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_format: Option<String>,

    /// Kubernetes Secret key reference for the HMAC signing secret.
    /// The controller resolves this at render time and injects the
    /// raw secret value into the config Secret — it is never stored
    /// in the CRD.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<SecretKeyRef>,
}

/// One outbound webhook subscription. Reads from `sourceTopics` and
/// HTTP-POSTs each record to `targetUrl`.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct OutboundSubscriptionSpec {
    /// Unique name for this subscription. Used as the consumer group id
    /// suffix.
    pub name: String,

    /// Kafka topics to consume records from.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_topics: Vec<String>,

    /// HTTP endpoint to POST records to.
    pub target_url: String,

    /// Kafka topic for records that exhausted all delivery attempts.
    /// When absent, failed records are logged and discarded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dead_letter_topic: Option<String>,

    /// Maximum number of delivery attempts before moving the record to
    /// the dead-letter topic. Default 5.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub max_attempts: Option<u32>,

    /// Initial backoff in milliseconds for exponential retry. Default 500.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub base_backoff_ms: Option<u64>,

    /// Maximum backoff cap in milliseconds. Default 30000.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub max_backoff_ms: Option<u64>,

    /// HTTP request timeout in milliseconds. Default 10000.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub request_timeout_ms: Option<u64>,

    /// Consumer group override for this subscription.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_id: Option<String>,

    /// Decode Schema Registry framed values to JSON before delivery.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decode_to_json: Option<bool>,

    /// CEL expression evaluated against the record to decide whether to
    /// deliver it. An absent or empty filter delivers all records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,

    /// Static HTTP headers appended to every delivery request.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, String>,

    /// Secret key reference for the outbound request HMAC signing secret.
    /// Resolved by the controller at render time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signing_secret_ref: Option<SecretKeyRef>,
}

/// One entry in the gateway's SSRF allowlist. An outbound HTTP request
/// is allowed only when `scheme` and `host` match.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AllowedTargetSpec {
    /// URL scheme. One of `http` or `https`.
    pub scheme: String,

    /// Hostname (and optional port) of the allowed target.
    pub host: String,
}

/// Reference to a key within a Kubernetes Secret in the same namespace.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SecretKeyRef {
    /// Name of the Kubernetes Secret.
    pub name: String,

    /// Key within the Secret's `data` map.
    pub key: String,
}

/// OpenTelemetry observability configuration.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TelemetrySpec {
    /// OTLP exporter endpoint URL. E.g.
    /// `http://otel-collector.observability.svc:4317`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub otlp_endpoint: Option<String>,

    /// OTLP exporter protocol. One of `grpc` or `http`. Default `grpc`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub otlp_protocol: Option<String>,

    /// Fraction of traces to sample in the range `[0.0, 1.0]`.
    /// Default 1.0 (sample everything).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 0.0, max = 1.0))]
    pub sample_ratio: Option<f64>,
}

/// Status reported back by the controller onto each `KafkaGrpcGateway`.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaGrpcGatewayStatus {
    /// Standard Kubernetes-style condition list. Surfaces
    /// `Ready`, `KafkaVersionValid`, `CertReady`, `Degraded`.
    #[serde(default)]
    pub conditions: Vec<crate::crd::KafkaCondition>,

    /// `metadata.generation` of the last successfully-reconciled spec.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    /// Number of gateway replicas currently reporting Ready.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready_replicas: Option<i32>,
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use kube::CustomResourceExt as _;

    use super::*;

    #[test]
    fn crd_metadata_is_correct() {
        let crd = KafkaGrpcGateway::crd();
        check!(crd.spec.group == "crabka.io");
        check!(crd.spec.names.kind == "KafkaGrpcGateway");
        check!(crd.spec.names.plural == "kafkagrpcgateways");
        check!(
            crd.spec
                .names
                .short_names
                .as_ref()
                .is_some_and(|v| v.contains(&"kgg".to_string())),
            "expected shortname `kgg`",
        );
        check!(crd.spec.versions.len() == 1);
        check!(crd.spec.versions[0].name == "v1alpha1");
    }

    #[test]
    fn minimal_spec_parses() {
        let json = r"{}";
        let spec: KafkaGrpcGatewaySpec = serde_json::from_str(json).unwrap();
        assert!(
            spec == KafkaGrpcGatewaySpec {
                replicas: None,
                image: None,
                resources: None,
                dedup: None,
                membership_topic: None,
                tuning: None,
                schema_registry: None,
                health_checks: None,
                tls: None,
                authz: None,
                webhooks: vec![],
                outbound_subscriptions: vec![],
                allowed_targets: vec![],
                telemetry: None,
            }
        );
    }

    #[test]
    fn spec_round_trips_through_json() {
        let gw = KafkaGrpcGateway::new(
            "my-gateway",
            KafkaGrpcGatewaySpec {
                replicas: Some(2),
                image: Some("ghcr.io/robot-head/crabka-grpc-gateway:latest".into()),
                resources: None,
                dedup: Some(DedupSpec {
                    topic: Some("my-gateway-dedup".into()),
                    partitions: Some(16),
                    window_ms: Some(86_400_000),
                    txn_id_prefix: Some("gw".into()),
                    ownership_group: None,
                }),
                membership_topic: None,
                tuning: None,
                schema_registry: None,
                health_checks: Some(GatewayHealthChecks {
                    readiness_initial_delay_seconds: Some(3),
                    readiness_period_seconds: Some(6),
                    liveness_initial_delay_seconds: Some(11),
                    liveness_period_seconds: Some(12),
                }),
                tls: Some(GatewayTlsSpec {
                    client_auth: Some("required".into()),
                    validity_days: Some(365),
                    reload_interval_secs: None,
                }),
                authz: Some(GatewayAuthzSpec {
                    mode: Some("simple".into()),
                    super_users: vec!["User:admin".into()],
                    acl_refresh_secs: Some(60),
                    bearer: Some(GatewayBearerSpec {
                        mode: Some("off".into()),
                        principal_claim: None,
                        allowable_clock_skew_ms: None,
                    }),
                }),
                webhooks: vec![InboundWebhookSpec {
                    name: "orders".into(),
                    target_topic: "raw-orders".into(),
                    principal: Some("User:webhook-producer".into()),
                    signature_header: Some("X-Hub-Signature-256".into()),
                    signature_encoding: Some("hex".into()),
                    signature_prefix: Some("sha256=".into()),
                    timestamp_header: None,
                    timestamp_tolerance_secs: Some(300),
                    idempotency_source: Some("header:X-Idempotency-Key".into()),
                    key_source: None,
                    max_body_bytes: Some(1_048_576),
                    schema_subject: None,
                    schema_format: None,
                    secret_ref: Some(SecretKeyRef {
                        name: "orders-webhook-secret".into(),
                        key: "hmac-key".into(),
                    }),
                }],
                outbound_subscriptions: vec![OutboundSubscriptionSpec {
                    name: "processed-orders".into(),
                    source_topics: vec!["processed-orders".into()],
                    target_url: "https://example.com/hook".into(),
                    dead_letter_topic: Some("failed-deliveries".into()),
                    max_attempts: Some(5),
                    base_backoff_ms: Some(1000),
                    max_backoff_ms: Some(30_000),
                    request_timeout_ms: Some(10_000),
                    group_id: None,
                    decode_to_json: None,
                    filter: None,
                    headers: BTreeMap::from([(
                        "Authorization".to_string(),
                        "Bearer token".to_string(),
                    )]),
                    signing_secret_ref: None,
                }],
                allowed_targets: vec![AllowedTargetSpec {
                    scheme: "https".into(),
                    host: "example.com".into(),
                }],
                telemetry: Some(TelemetrySpec {
                    otlp_endpoint: Some("http://otel:4317".into()),
                    otlp_protocol: Some("grpc".into()),
                    sample_ratio: Some(1.0),
                }),
            },
        );
        let json = serde_json::to_string(&gw).unwrap();
        for want in [
            "\"replicas\":2",
            "\"targetTopic\":\"raw-orders\"",
            "\"targetUrl\":\"https://example.com/hook\"",
            "\"otlpEndpoint\":\"http://otel:4317\"",
        ] {
            assert!(json.contains(want), "case {want:?}; got: {json}");
        }
        let back: KafkaGrpcGateway = serde_json::from_str(&json).unwrap();
        assert!(back.spec == gw.spec);
    }

    #[test]
    fn spec_omits_empty_optional_fields() {
        let spec = KafkaGrpcGatewaySpec {
            replicas: None,
            image: None,
            resources: None,
            dedup: None,
            membership_topic: None,
            tuning: None,
            schema_registry: None,
            health_checks: None,
            tls: None,
            authz: None,
            webhooks: vec![],
            outbound_subscriptions: vec![],
            allowed_targets: vec![],
            telemetry: None,
        };
        let j = serde_json::to_string(&spec).unwrap();
        for absent in [
            "replicas",
            "image",
            "webhooks",
            "outboundSubscriptions",
            "telemetry",
        ] {
            assert!(!j.contains(absent), "case {absent:?}; got: {j}");
        }
    }

    #[test]
    fn status_omits_optional_fields_when_unset() {
        let status = KafkaGrpcGatewayStatus::default();
        let j = serde_json::to_string(&status).unwrap();
        assert!(!j.contains("observedGeneration"), "got: {j}");
        assert!(!j.contains("readyReplicas"), "got: {j}");
    }

    #[test]
    fn outbound_subscription_headers_round_trip() {
        let sub = OutboundSubscriptionSpec {
            name: "sub1".into(),
            source_topics: vec!["topic-a".into()],
            target_url: "https://example.com/hook".into(),
            dead_letter_topic: None,
            max_attempts: None,
            base_backoff_ms: None,
            max_backoff_ms: None,
            request_timeout_ms: None,
            group_id: None,
            decode_to_json: None,
            filter: None,
            headers: BTreeMap::from([
                ("X-Tenant".to_string(), "acme".to_string()),
                ("Content-Type".to_string(), "application/json".to_string()),
            ]),
            signing_secret_ref: None,
        };
        let j = serde_json::to_string(&sub).unwrap();
        assert!(j.contains("\"X-Tenant\":\"acme\""), "got: {j}");
        let back: OutboundSubscriptionSpec = serde_json::from_str(&j).unwrap();
        assert!(back == sub);
    }

    #[test]
    fn secret_key_ref_round_trips() {
        let r = SecretKeyRef {
            name: "my-secret".into(),
            key: "hmac-key".into(),
        };
        let j = serde_json::to_string(&r).unwrap();
        assert!(j == r#"{"name":"my-secret","key":"hmac-key"}"#, "got: {j}");
        let back: SecretKeyRef = serde_json::from_str(&j).unwrap();
        assert!(back == r);
    }
}
