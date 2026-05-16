use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Crabka cluster spec.
///
/// Slice 17 ships a placeholder with only `kafka_version`; the real
/// schema lands in slice 18.
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
    /// Crabka version to deploy (semver). Required.
    pub kafka_version: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaStatus {
    /// Standard Kubernetes-style condition list.
    #[serde(default)]
    pub conditions: Vec<KafkaCondition>,
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
            },
        );
        let json = serde_json::to_string(&k).unwrap();
        let back: Kafka = serde_json::from_str(&json).unwrap();
        assert_eq!(back.spec, k.spec);
    }
}
