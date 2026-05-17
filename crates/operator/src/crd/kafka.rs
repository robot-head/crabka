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
                config: None,
                listeners: vec![],
                inter_broker_listener_name: None,
                metrics_config: None,
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
                config: None,
                listeners: vec![],
                inter_broker_listener_name: None,
                metrics_config: None,
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
        };
        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("\"bootstrapServers\""), "got: {json}");
        let back: KafkaStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(back, status);
    }
}
