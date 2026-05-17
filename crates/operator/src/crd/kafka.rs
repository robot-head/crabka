use k8s_openapi::api::core::v1::ResourceRequirements;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Crabka cluster spec.
///
/// Slice 19 introduces `replicas`, `image`, and `resources` alongside the
/// `kafka_version` field; multi-broker support arrives in slice 20 via
/// `KafkaNodePool`.
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
    /// Crabka version label (informational; image tag governs the actual
    /// binary version).
    pub kafka_version: String,

    /// Number of broker replicas. Slice 19 supports `1` only;
    /// `KafkaNodePool` (slice 20) generalizes this.
    #[serde(default = "default_replicas")]
    #[schemars(range(min = 1, max = 1))]
    pub replicas: i32,

    /// Container image. Reconciler falls back to the operator's
    /// `--default-broker-image` flag if absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,

    /// Resource requests / limits applied to the broker container.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceRequirements>,
}

const fn default_replicas() -> i32 {
    1
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaStatus {
    /// Standard Kubernetes-style condition list.
    #[serde(default)]
    pub conditions: Vec<KafkaCondition>,
    /// Mirrors `StatefulSet.status.replicas`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replicas: Option<i32>,
    /// Mirrors `StatefulSet.status.readyReplicas`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready_replicas: Option<i32>,
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
                replicas: 1,
                image: None,
                resources: None,
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
    fn spec_defaults_replicas_to_one() {
        let json = r#"{"kafkaVersion":"0.1.1"}"#;
        let spec: KafkaSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.replicas, 1);
        assert!(spec.image.is_none());
        assert!(spec.resources.is_none());
    }
}
