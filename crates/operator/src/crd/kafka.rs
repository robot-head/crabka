use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Crabka cluster spec. Slice 20: spec carries only the version label;
/// broker pods are described by sibling `KafkaNodePool`s labeled
/// `crabka.io/cluster=<this name>`.
#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[kube(
    group = "crabka.io",
    version = "v1alpha1",
    kind = "Kafka",
    plural = "kafkas",
    singular = "kafka",
    shortname = "kk",
    namespaced,
    status = "KafkaStatus",
    derive = "PartialEq"
)]
#[serde(rename_all = "camelCase")]
pub struct KafkaSpec {
    /// Crabka version label, propagated to all pool pods via the
    /// `app.kubernetes.io/version` label.
    pub kafka_version: String,
    /// `KRaft` metadata version (the runtime analog of
    /// `inter.broker.protocol.version`). When unset, tracks
    /// `kafkaVersion`'s `major.minor`; when set, pins the metadata version
    /// for the safe two-step upgrade. Validated against `kafkaVersion` and
    /// the finalized `status.metadataVersion` (slice 28) — an invalid value
    /// surfaces `KafkaVersionValid=False` and blocks the roll.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_version: Option<String>,
    /// Opaque broker properties (`server.properties`-style key/value
    /// pairs). Slice 25 passes these through to the broker's
    /// `[server_properties]` TOML table; the broker currently treats
    /// them as inert. Changes propagate through the slice-21 config
    /// hash.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<std::collections::BTreeMap<String, String>>,
    /// Slice 25: named listeners. Empty (or absent) synthesizes a
    /// single internal `PLAIN` listener on port 9092 (slice 19/20
    /// compatibility default).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub listeners: Vec<crate::crd::Listener>,
    /// Slice 25: name of the listener used for inter-broker traffic.
    /// When `None`, the operator picks the first `internal` listener;
    /// when `listeners` is empty, the synthesized default `"PLAIN"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inter_broker_listener_name: Option<String>,
    /// Slice 40: Prometheus scrape configuration. When `None`, brokers do
    /// not bind `/metrics` and no `PodMonitor` / `ServiceMonitor` is
    /// rendered. When `Some`, the broker `StatefulSet` gains a `metrics`
    /// container port (TCP 9404) and the resources requested by
    /// `pod_monitor` / `service_monitor` are SSA-applied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics_config: Option<crate::crd::MetricsConfig>,
    /// Slice 23: opt-in `NetworkPolicy` generation. When `None`, no
    /// `NetworkPolicy` is generated. When `Some` (even `{}`), the operator
    /// renders a cluster-level `NetworkPolicy` gating ingress to broker /
    /// controller pods.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network_policy: Option<crate::crd::NetworkPolicySpec>,
    /// Slice 30: per-cluster CA used for inter-broker mTLS + broker certs.
    /// Absent → fully-defaulted `CertificateAuthority` (operator-generated,
    /// 365/30 days).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_ca: Option<crate::crd::CertificateAuthority>,
    /// Slice 30: per-cluster CA used to sign `KafkaUser` TLS certs (slice
    /// 37). Absent → fully-defaulted `CertificateAuthority`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clients_ca: Option<crate::crd::CertificateAuthority>,
    /// Slice 41: broker log configuration. When `None`, brokers use their
    /// built-in default `RUST_LOG` filter. When `Some`, the operator
    /// composes (inline) or reads (external) a `tracing` env-filter string,
    /// renders it into the broker `ConfigMap` (`rust.log` key), wires it
    /// into each broker pod's `RUST_LOG` env, and rolls the cluster on
    /// change via the slice-21 config hash.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logging: Option<crate::crd::Logging>,
    /// Slice 51b: delegation-token master HMAC key source. When `None`,
    /// the broker rejects all KIP-48 delegation-token RPCs with err 61
    /// `DELEGATION_TOKEN_AUTH_DISABLED`. When `Some`, the operator
    /// injects `CRABKA_DELEGATION_TOKEN_SECRET_KEY` into each broker
    /// pod via a `valueFrom.secretKeyRef`, baking the key into the
    /// rendered `StatefulSet` so the slice-21 SSA reconcile doesn't
    /// race with out-of-band `kubectl set env` patches.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delegation_token: Option<DelegationTokenConfig>,
    /// Slice 53: cluster-level authorizer selection. When `None`, the
    /// broker uses the default `AllowAll` authorizer (no ACL checks).
    /// When `Some`, the operator renders the `[authorization]` TOML
    /// section so the broker builds the matching `Arc<dyn Authorizer>`
    /// (`SimpleAclAuthorizer` for `type: simple`, `OpaAuthorizer` for
    /// `type: opa`). With `simple` or `opa` selected, the operator's
    /// inter-broker principal MUST appear in `super_users` (no implicit
    /// `ANONYMOUS` allow); operators opt in explicitly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorization: Option<Authorization>,
}

/// Slice 51b: master-HMAC-key source for KIP-48 delegation tokens.
///
/// The operator wires the referenced Secret key as the broker pod's
/// `CRABKA_DELEGATION_TOKEN_SECRET_KEY` env var (env wins over TOML in
/// slice 51's broker config layer). Required for delegation-token
/// `KafkaUser` support (slice 51b). If unset on the parent `Kafka`,
/// the broker rejects all delegation-token RPCs with err 61
/// `DELEGATION_TOKEN_AUTH_DISABLED`.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DelegationTokenConfig {
    /// Reference to a Kubernetes `Secret` (same namespace as the
    /// `Kafka` CR) whose `data.<key>` value is the broker's master HMAC
    /// key for KIP-48 delegation tokens.
    pub secret_key_ref: SecretKeyRef,
}

/// Slice 51b: minimal namespaced Secret-key reference (name + optional
/// data-map key, defaulting to `secret-key`).
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SecretKeyRef {
    /// Secret name in the same namespace as the `Kafka` CR.
    pub name: String,
    /// Key within the Secret's `data`. Defaults to `secret-key`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
}

/// Slice 53: cluster-level authorizer selection on `Kafka.spec.authorization`.
///
/// Tagged on `type` to pick the broker-side `Arc<dyn Authorizer>` impl.
/// `None` on the parent spec means `AllowAll` (no `[authorization]` TOML
/// section is rendered, the broker uses `AllowAllAuthorizer`). When set,
/// the operator's inter-broker principal MUST be in `super_users` — there
/// is no implicit ANONYMOUS allow.
///
/// The `schema_with` workaround avoids a kube-rs 3.x `StructuralSchemaRewriter`
/// panic when `oneOf` branches share a `type` discriminator with differing
/// `enum` values — same pattern as `Authentication` in `user.rs` and
/// `ListenerAuthentication` in `listener.rs`.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
#[schemars(schema_with = "authorization_schema")]
pub enum Authorization {
    #[serde(rename = "simple")]
    Simple(SimpleAuthorization),
    #[serde(rename = "opa")]
    Opa(OpaAuthorization),
}

/// Slice 53: `type: simple` config for `Kafka.spec.authorization`. Drives the
/// broker's `SimpleAclAuthorizer`. Distinct from the per-user
/// `crate::crd::user::SimpleAuthorization` (which carries ACL rules for one
/// `KafkaUser`): this one is cluster-wide and only carries the super-user
/// bypass list. ACLs themselves are owned by `KafkaUser` CRs / `CreateAcls`.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SimpleAuthorization {
    /// Principal strings (e.g. `"User:admin"`, `"ANONYMOUS"`) that
    /// bypass ACL checks. Empty = no super-users.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub super_users: Vec<String>,
}

/// Slice 53: `type: opa` config for `Kafka.spec.authorization`. Drives the
/// broker's `OpaAuthorizer` — an HTTP-backed authorizer with an LRU+TTL
/// decision cache. No `derive(Default)` because `url` has no sensible default.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct OpaAuthorization {
    /// OPA decision endpoint URL — must include the data-API path, e.g.
    /// `http://opa:8181/v1/data/kafka/authz/allow`.
    pub url: String,
    /// Permit the operation on any OPA error (timeout, 5xx, parse).
    /// Default false (fail-closed).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allow_on_error: Option<bool>,
    /// Initial capacity of the broker's LRU decision cache. Broker
    /// default applies when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 0))]
    pub initial_cache_capacity: Option<u32>,
    /// Hard upper bound on the LRU decision cache. Broker default
    /// applies when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub maximum_cache_size: Option<u32>,
    /// Per-entry TTL (ms). Broker default applies when unset.
    /// Minimum 1000 ms — sub-second TTLs defeat the cache.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1000))]
    pub expire_after_ms: Option<i64>,
    /// Principal strings that bypass OPA entirely. The broker's
    /// internal calls (replication etc.) use `ANONYMOUS` by default,
    /// which MUST be a super-user for inter-broker traffic to work
    /// when `type: opa` is selected. Empty = no super-users (OPA
    /// decides every request).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub super_users: Vec<String>,
}

fn authorization_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "type": "object",
        "required": ["type"],
        "properties": {
            "type": {
                "type": "string",
                "enum": ["simple", "opa"],
            },
            "superUsers": {
                "type": "array",
                "items": { "type": "string" },
            },
            // OPA-only sibling properties.
            "url": { "type": "string" },
            "allowOnError": { "type": "boolean" },
            "initialCacheCapacity": { "type": "integer", "minimum": 0 },
            "maximumCacheSize": { "type": "integer", "minimum": 1 },
            "expireAfterMs": { "type": "integer", "minimum": 1000 },
        },
    })
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaStatus {
    /// Standard Kubernetes-style condition list. Surfaces
    /// `Ready`, `ListenersValid`, `ListenersReady`.
    #[serde(default)]
    pub conditions: Vec<KafkaCondition>,
    /// Mirrors `StatefulSet.status.replicas`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replicas: Option<i32>,
    /// Mirrors `StatefulSet.status.readyReplicas`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready_replicas: Option<i32>,
    /// Slice 25: per-listener resolved addresses. Populated once
    /// `ListenersReady=True`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub listeners: Vec<crate::crd::ListenerStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_ca: Option<crate::crd::CertificateAuthorityStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clients_ca: Option<crate::crd::CertificateAuthorityStatus>,
    /// Slice 28: echo of `spec.kafkaVersion`, for observability.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kafka_version: Option<String>,
    /// Slice 28: the operator-finalized metadata version. Advances only
    /// when version validation passes; drives the downgrade-window check on
    /// the next reconcile.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_version: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaCondition {
    /// e.g. `Ready`.
    #[serde(rename = "type")]
    pub type_: String,
    /// `True`, `False`, or `Unknown`.
    pub status: String,
    /// CamelCase machine reason.
    pub reason: String,
    /// Human-readable message.
    pub message: String,
    /// RFC3339 timestamp.
    pub last_transition_time: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use kube::CustomResourceExt as _;

    #[test]
    fn crd_metadata_is_correct() {
        let crd = Kafka::crd();
        assert_eq!(crd.spec.group, "crabka.io");
        assert_eq!(crd.spec.names.kind, "Kafka");
        assert_eq!(crd.spec.names.plural, "kafkas");
        assert_eq!(crd.spec.versions.len(), 1);
        assert_eq!(crd.spec.versions[0].name, "v1alpha1");
    }

    #[test]
    fn round_trips_through_json() {
        let k = Kafka::new(
            "demo",
            KafkaSpec {
                kafka_version: "0.1.1".into(),
                metadata_version: None,
                config: None,
                listeners: vec![],
                inter_broker_listener_name: None,
                metrics_config: None,
                network_policy: None,
                cluster_ca: None,
                clients_ca: None,
                logging: None,
                delegation_token: None,
                authorization: None,
            },
        );
        let json = serde_json::to_string(&k).unwrap();
        assert!(
            json.contains("\"kafkaVersion\""),
            "expected camelCase wire shape, got: {json}"
        );
        let back: Kafka = serde_json::from_str(&json).unwrap();
        assert_eq!(back.spec, k.spec);
    }

    #[test]
    fn spec_omits_metrics_config_when_none() {
        let k = Kafka::new(
            "demo",
            KafkaSpec {
                kafka_version: "0.1.1".into(),
                metadata_version: None,
                config: None,
                listeners: vec![],
                inter_broker_listener_name: None,
                metrics_config: None,
                network_policy: None,
                cluster_ca: None,
                clients_ca: None,
                logging: None,
                delegation_token: None,
                authorization: None,
            },
        );
        let j = serde_json::to_string(&k.spec).unwrap();
        assert!(!j.contains("metricsConfig"), "got: {j}");
    }

    #[test]
    fn spec_carries_metrics_config_pod_monitor() {
        use crate::crd::{MetricsConfig, PodMonitorSpec};
        let json = r#"{"kafkaVersion":"0.1.1","metricsConfig":{"podMonitor":{"interval":"30s"}}}"#;
        let spec: KafkaSpec = serde_json::from_str(json).unwrap();
        let cfg: MetricsConfig = spec.metrics_config.expect("metricsConfig present");
        let pm: PodMonitorSpec = cfg.pod_monitor.expect("podMonitor present");
        assert_eq!(pm.interval.as_deref(), Some("30s"));
    }

    #[test]
    fn spec_only_carries_kafka_version() {
        let json = r#"{"kafkaVersion":"0.1.1"}"#;
        let spec: KafkaSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.kafka_version, "0.1.1");
        assert!(spec.config.is_none());
    }

    #[test]
    fn spec_carries_config() {
        let json = r#"{"kafkaVersion":"0.1.1","config":{"log.retention.hours":"24"}}"#;
        let spec: KafkaSpec = serde_json::from_str(json).unwrap();
        let cfg = spec.config.expect("config present");
        assert_eq!(
            cfg.get("log.retention.hours").map(String::as_str),
            Some("24")
        );
    }

    #[test]
    fn spec_carries_listeners() {
        use crate::crd::ListenerType;

        let json = r#"{
            "kafkaVersion":"0.1.1",
            "listeners":[{"name":"PLAIN","port":9092,"type":"internal","tls":false}],
            "interBrokerListenerName":"PLAIN"
        }"#;
        let spec: KafkaSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.listeners.len(), 1);
        assert_eq!(spec.listeners[0].name, "PLAIN");
        assert_eq!(spec.listeners[0].type_, ListenerType::Internal);
        assert_eq!(spec.inter_broker_listener_name.as_deref(), Some("PLAIN"));
    }

    #[test]
    fn spec_defaults_listeners_to_empty() {
        let json = r#"{"kafkaVersion":"0.1.1"}"#;
        let spec: KafkaSpec = serde_json::from_str(json).unwrap();
        assert!(spec.listeners.is_empty());
        assert!(spec.inter_broker_listener_name.is_none());
    }

    #[test]
    fn status_carries_listener_status() {
        use crate::crd::{ListenerAddress, ListenerStatus, ListenerType};

        let status = KafkaStatus {
            conditions: vec![],
            replicas: Some(1),
            ready_replicas: Some(1),
            listeners: vec![ListenerStatus {
                name: "PLAIN".into(),
                type_: ListenerType::Internal,
                bootstrap_servers: "demo-broker-headless.default.svc.cluster.local:9092".into(),
                addresses: vec![ListenerAddress {
                    host: "demo-broker-headless.default.svc.cluster.local".into(),
                    port: 9092,
                }],
            }],
            cluster_ca: None,
            clients_ca: None,
            kafka_version: None,
            metadata_version: None,
        };
        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("\"bootstrapServers\""), "got: {json}");
        let back: KafkaStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(back, status);
    }

    #[test]
    fn spec_carries_metadata_version() {
        let json = r#"{"kafkaVersion":"3.7.0","metadataVersion":"3.6"}"#;
        let spec: KafkaSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.metadata_version.as_deref(), Some("3.6"));
    }

    #[test]
    fn spec_omits_metadata_version_when_none() {
        let k = Kafka::new(
            "demo",
            KafkaSpec {
                kafka_version: "3.7.0".into(),
                metadata_version: None,
                config: None,
                listeners: vec![],
                inter_broker_listener_name: None,
                metrics_config: None,
                network_policy: None,
                cluster_ca: None,
                clients_ca: None,
                logging: None,
                delegation_token: None,
                authorization: None,
            },
        );
        let j = serde_json::to_string(&k.spec).unwrap();
        assert!(!j.contains("metadataVersion"), "got: {j}");
    }

    #[test]
    fn status_carries_version_fields() {
        let status = KafkaStatus {
            kafka_version: Some("3.7.0".into()),
            metadata_version: Some("3.7".into()),
            ..Default::default()
        };
        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("\"metadataVersion\":\"3.7\""), "got: {json}");
        let back: KafkaStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(back, status);
    }

    #[test]
    fn spec_omits_network_policy_when_none() {
        let k = Kafka::new(
            "demo",
            KafkaSpec {
                kafka_version: "0.1.1".into(),
                metadata_version: None,
                config: None,
                listeners: vec![],
                inter_broker_listener_name: None,
                metrics_config: None,
                network_policy: None,
                cluster_ca: None,
                clients_ca: None,
                logging: None,
                delegation_token: None,
                authorization: None,
            },
        );
        let j = serde_json::to_string(&k.spec).unwrap();
        assert!(!j.contains("networkPolicy"), "got: {j}");
    }

    #[test]
    fn spec_carries_network_policy_when_set() {
        let json = r#"{"kafkaVersion":"0.1.1","networkPolicy":{}}"#;
        let spec: KafkaSpec = serde_json::from_str(json).unwrap();
        assert!(spec.network_policy.is_some(), "networkPolicy parsed");
    }

    #[test]
    fn spec_omits_logging_when_none() {
        let json = r#"{"kafkaVersion":"0.1.1"}"#;
        let spec: KafkaSpec = serde_json::from_str(json).unwrap();
        assert!(spec.logging.is_none());
        let j = serde_json::to_string(&spec).unwrap();
        assert!(!j.contains("logging"), "got: {j}");
    }

    #[test]
    fn spec_carries_inline_logging() {
        use crate::crd::LoggingType;
        let json = r#"{"kafkaVersion":"0.1.1","logging":{"loggers":{"root":"info","crabka_broker":"debug"}}}"#;
        let spec: KafkaSpec = serde_json::from_str(json).unwrap();
        let lg = spec.logging.expect("logging present");
        assert_eq!(lg.r#type, LoggingType::Inline);
        assert_eq!(
            lg.loggers.get("crabka_broker").map(String::as_str),
            Some("debug")
        );
    }

    #[test]
    fn kafka_spec_parses_without_ca_fields() {
        let v: KafkaSpec = serde_json::from_value(serde_json::json!({
            "kafkaVersion": "3.7.0",
        }))
        .expect("parse minimal spec");
        assert!(v.cluster_ca.is_none());
        assert!(v.clients_ca.is_none());
    }

    #[test]
    fn spec_omits_delegation_token_when_none() {
        let json = r#"{"kafkaVersion":"0.1.1"}"#;
        let spec: KafkaSpec = serde_json::from_str(json).unwrap();
        assert!(spec.delegation_token.is_none());
        let j = serde_json::to_string(&spec).unwrap();
        assert!(!j.contains("delegationToken"), "got: {j}");
    }

    #[test]
    fn spec_carries_delegation_token_with_default_key() {
        let json = r#"{
            "kafkaVersion":"0.1.1",
            "delegationToken":{"secretKeyRef":{"name":"dt-master"}}
        }"#;
        let spec: KafkaSpec = serde_json::from_str(json).unwrap();
        let dt = spec.delegation_token.expect("delegationToken present");
        assert_eq!(dt.secret_key_ref.name, "dt-master");
        assert!(dt.secret_key_ref.key.is_none());
    }

    #[test]
    fn spec_carries_delegation_token_with_explicit_key() {
        let json = r#"{
            "kafkaVersion":"0.1.1",
            "delegationToken":{"secretKeyRef":{"name":"dt-master","key":"hmac"}}
        }"#;
        let spec: KafkaSpec = serde_json::from_str(json).unwrap();
        let dt = spec.delegation_token.expect("delegationToken present");
        assert_eq!(dt.secret_key_ref.name, "dt-master");
        assert_eq!(dt.secret_key_ref.key.as_deref(), Some("hmac"));
    }

    #[test]
    fn kafka_spec_parses_with_ca_fields() {
        let v: KafkaSpec = serde_json::from_value(serde_json::json!({
            "kafkaVersion": "3.7.0",
            "clusterCa": { "validityDays": 30 },
            "clientsCa": { "generateCertificateAuthority": false },
        }))
        .expect("parse with CAs");
        assert_eq!(v.cluster_ca.as_ref().unwrap().validity_days, 30);
        assert!(
            !v.clients_ca
                .as_ref()
                .unwrap()
                .generate_certificate_authority
        );
    }

    // Slice 53: `Kafka.spec.authorization` round-trip tests.
    //
    // Pin the wire shape of the slice-53 authorizer-selection CRD
    // alongside its sibling enums on `KafkaSpec`. Mirrors the slice-51b
    // `delegationToken` round-trip pattern: deserialize Strimzi-shape
    // YAML, assert the typed Rust value, then re-serialize and assert
    // optional fields are omitted (so the rendered TOML stays minimal
    // and the broker's `[authorization]` parser doesn't trip on
    // explicit-null vs absent).

    #[test]
    fn simple_authorization_round_trip() {
        let yaml = r"
kafkaVersion: 0.1.1
authorization:
  type: simple
  superUsers:
    - User:admin
    - ANONYMOUS
";
        let spec: KafkaSpec = serde_yaml::from_str(yaml).expect("yaml must parse");
        let Some(Authorization::Simple(simple)) = spec.authorization.clone() else {
            panic!("expected Simple variant, got {:?}", spec.authorization);
        };
        assert_eq!(
            simple.super_users,
            vec!["User:admin".to_string(), "ANONYMOUS".to_string()]
        );

        // JSON round-trip pins the camelCase wire shape (`superUsers`,
        // `type: "simple"`).
        let json = serde_json::to_string(&spec).unwrap();
        assert!(json.contains("\"type\":\"simple\""), "got: {json}");
        assert!(
            json.contains("\"superUsers\":[\"User:admin\",\"ANONYMOUS\"]"),
            "got: {json}"
        );
        let back: KafkaSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(back, spec);
    }

    #[test]
    fn opa_authorization_round_trip_full_fields() {
        let yaml = r"
kafkaVersion: 0.1.1
authorization:
  type: opa
  url: http://opa.opa.svc:8181/v1/data/kafka/authz/allow
  allowOnError: true
  initialCacheCapacity: 1000
  maximumCacheSize: 50000
  expireAfterMs: 60000
  superUsers:
    - User:admin
    - ANONYMOUS
";
        let spec: KafkaSpec = serde_yaml::from_str(yaml).expect("yaml must parse");
        let Some(Authorization::Opa(opa)) = spec.authorization.clone() else {
            panic!("expected Opa variant, got {:?}", spec.authorization);
        };
        assert_eq!(opa.url, "http://opa.opa.svc:8181/v1/data/kafka/authz/allow");
        assert_eq!(opa.allow_on_error, Some(true));
        assert_eq!(opa.initial_cache_capacity, Some(1000));
        assert_eq!(opa.maximum_cache_size, Some(50_000));
        assert_eq!(opa.expire_after_ms, Some(60_000));
        assert_eq!(
            opa.super_users,
            vec!["User:admin".to_string(), "ANONYMOUS".to_string()]
        );

        let json = serde_json::to_string(&spec).unwrap();
        assert!(json.contains("\"type\":\"opa\""), "got: {json}");
        // Every numeric knob must round-trip in camelCase form.
        assert!(json.contains("\"allowOnError\":true"), "got: {json}");
        assert!(
            json.contains("\"initialCacheCapacity\":1000"),
            "got: {json}"
        );
        assert!(json.contains("\"maximumCacheSize\":50000"), "got: {json}");
        assert!(json.contains("\"expireAfterMs\":60000"), "got: {json}");
        let back: KafkaSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(back, spec);
    }

    #[test]
    fn opa_authorization_minimal_omits_optional_fields() {
        // Only `url` is required on the `opa` variant; every other
        // field is `Option<...>` / `Vec<...>` and must be skipped on
        // serialize when `None`/empty so the rendered TOML and the
        // resulting hash are minimal.
        let yaml = r"
kafkaVersion: 0.1.1
authorization:
  type: opa
  url: http://opa.opa.svc:8181/v1/data/kafka/authz/allow
";
        let spec: KafkaSpec = serde_yaml::from_str(yaml).expect("yaml must parse");
        let Some(Authorization::Opa(opa)) = spec.authorization.clone() else {
            panic!("expected Opa variant, got {:?}", spec.authorization);
        };
        assert_eq!(opa.url, "http://opa.opa.svc:8181/v1/data/kafka/authz/allow");
        assert_eq!(opa.allow_on_error, None);
        assert_eq!(opa.initial_cache_capacity, None);
        assert_eq!(opa.maximum_cache_size, None);
        assert_eq!(opa.expire_after_ms, None);
        assert!(opa.super_users.is_empty());

        let json = serde_json::to_string(&spec).unwrap();
        for absent in [
            "allowOnError",
            "initialCacheCapacity",
            "maximumCacheSize",
            "expireAfterMs",
            "superUsers",
        ] {
            assert!(
                !json.contains(absent),
                "{absent} must be omitted when None/empty; got: {json}"
            );
        }
        let back: KafkaSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(back, spec);
    }
}
