//! Slice 35: `KafkaTopic` CRD. Strimzi-shaped; unidirectional
//! reconciliation (CRD wins).

use std::collections::BTreeMap;

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[kube(
    group = "crabka.io",
    version = "v1alpha1",
    kind = "KafkaTopic",
    plural = "kafkatopics",
    singular = "kafkatopic",
    shortname = "kt",
    namespaced,
    status = "KafkaTopicStatus",
    derive = "PartialEq"
)]
#[serde(rename_all = "camelCase")]
pub struct KafkaTopicSpec {
    /// Optional override for the Kafka topic name. Defaults to
    /// `metadata.name`. Validated at reconcile time against Kafka's
    /// rules (length ≤ 249, chars `[A-Za-z0-9._-]`, not `.` or `..`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_name: Option<String>,

    /// Number of partitions. Increases honored via `CreatePartitions`;
    /// decreases rejected with `ImmutableFieldChanged`.
    #[schemars(range(min = 1, max = 1_000_000))]
    pub partitions: i32,

    /// Replication factor. Changes rejected with
    /// `ImmutableFieldChanged` until partition reassignment lands
    /// (slice 43+).
    #[schemars(range(min = 1, max = 1_000))]
    pub replicas: i32,

    /// Opaque topic-level config (`retention.ms`, `cleanup.policy`,
    /// etc.). Reconciled via `IncrementalAlterConfigs` SET/DELETE diff
    /// against the cluster's current dynamic-topic overrides.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<BTreeMap<String, String>>,

    /// When `true`, CRD delete still removes the finalizer but skips
    /// the `DeleteTopics` call so the Kafka topic survives. Default
    /// `false`.
    #[serde(default)]
    pub preserve_topic: bool,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaTopicStatus {
    /// Standard Kubernetes-style condition list. Surfaces `Ready`.
    #[serde(default)]
    pub conditions: Vec<crate::crd::KafkaCondition>,

    /// `metadata.generation` of the last successfully-reconciled spec
    /// (i.e. last time we wrote `Ready=True reason=Ready`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    /// Effective topic name (defaulted if `spec.topicName` unset).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_name: Option<String>,

    /// Cluster-assigned topic UUID, populated once the topic exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_id: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use kube::CustomResourceExt as _;

    #[test]
    fn crd_metadata_is_correct() {
        let crd = KafkaTopic::crd();
        assert_eq!(crd.spec.group, "crabka.io");
        assert_eq!(crd.spec.names.kind, "KafkaTopic");
        assert_eq!(crd.spec.names.plural, "kafkatopics");
        assert!(
            crd.spec
                .names
                .short_names
                .as_ref()
                .is_some_and(|v| v.contains(&"kt".to_string())),
            "expected shortname `kt`",
        );
        assert_eq!(crd.spec.versions.len(), 1);
        assert_eq!(crd.spec.versions[0].name, "v1alpha1");
    }

    #[test]
    fn spec_round_trips_through_json() {
        let kt = KafkaTopic::new(
            "demo-topic",
            KafkaTopicSpec {
                topic_name: Some("Demo.Topic".into()),
                partitions: 3,
                replicas: 2,
                config: Some(BTreeMap::from([(
                    "retention.ms".to_string(),
                    "60000".to_string(),
                )])),
                preserve_topic: true,
            },
        );
        let json = serde_json::to_string(&kt).unwrap();
        assert!(json.contains("\"topicName\":\"Demo.Topic\""), "got: {json}");
        assert!(json.contains("\"partitions\":3"), "got: {json}");
        assert!(json.contains("\"preserveTopic\":true"), "got: {json}");
        let back: KafkaTopic = serde_json::from_str(&json).unwrap();
        assert_eq!(back.spec, kt.spec);
    }

    #[test]
    fn spec_omits_optional_fields_when_default() {
        let kt = KafkaTopic::new(
            "demo",
            KafkaTopicSpec {
                topic_name: None,
                partitions: 1,
                replicas: 1,
                config: None,
                preserve_topic: false,
            },
        );
        let j = serde_json::to_string(&kt.spec).unwrap();
        assert!(!j.contains("topicName"), "got: {j}");
        assert!(!j.contains("config"), "got: {j}");
        // `preserveTopic` is a plain bool — serde emits it.
        assert!(j.contains("\"preserveTopic\":false"), "got: {j}");
    }

    #[test]
    fn status_topic_id_omitted_when_none() {
        let status = KafkaTopicStatus {
            conditions: vec![],
            observed_generation: Some(1),
            topic_name: Some("foo".into()),
            topic_id: None,
        };
        let j = serde_json::to_string(&status).unwrap();
        assert!(!j.contains("topicId"), "got: {j}");
        assert!(j.contains("\"observedGeneration\":1"), "got: {j}");
    }

    #[test]
    fn minimum_required_spec_parses() {
        let json = r#"{"partitions":1,"replicas":1}"#;
        let spec: KafkaTopicSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.partitions, 1);
        assert_eq!(spec.replicas, 1);
        assert!(spec.topic_name.is_none());
        assert!(spec.config.is_none());
        assert!(!spec.preserve_topic);
    }
}
