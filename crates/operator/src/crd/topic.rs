//! `KafkaTopic` CRD.
//!
//! The CRD is Strimzi-shaped. Reconciliation is unidirectional: the CRD wins.

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
    /// `metadata.name`. The operator validates it at reconcile time against
    /// Kafka's rules: length ≤ 249, characters `[A-Za-z0-9._-]`, and not `.`
    /// or `..`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_name: Option<String>,

    /// Number of partitions. The operator applies increases through
    /// `CreatePartitions`. The operator rejects decreases with
    /// `ImmutableFieldChanged`.
    #[schemars(range(min = 1, max = 1_000_000))]
    pub partitions: i32,

    /// Replication factor. The operator rejects changes with
    /// `ImmutableFieldChanged` until partition reassignment lands.
    #[schemars(range(min = 1, max = 1_000))]
    pub replicas: i32,

    /// Opaque topic-level config, such as `retention.ms` and
    /// `cleanup.policy`. The operator reconciles it with an
    /// `IncrementalAlterConfigs` SET and DELETE diff against the cluster's
    /// current dynamic-topic overrides.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<BTreeMap<String, String>>,

    /// When `true`, a CRD delete still removes the finalizer but skips the
    /// `DeleteTopics` call, so the Kafka topic survives. Default: `false`.
    #[serde(default)]
    pub preserve_topic: bool,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaTopicStatus {
    /// Standard Kubernetes-style condition list. It shows `Ready`.
    #[serde(default)]
    pub conditions: Vec<crate::crd::KafkaCondition>,

    /// `metadata.generation` of the last successfully-reconciled spec. That
    /// is the last time the operator wrote `Ready=True reason=Ready`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    /// Effective topic name. The operator supplies a default if
    /// `spec.topicName` is unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_name: Option<String>,

    /// Cluster-assigned topic UUID. The operator fills it in once the topic
    /// exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_id: Option<String>,
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use kube::CustomResourceExt as _;

    use super::*;

    #[test]
    fn crd_metadata_is_correct() {
        let crd = KafkaTopic::crd();
        check!(crd.spec.group == "crabka.io");
        check!(crd.spec.names.kind == "KafkaTopic");
        check!(crd.spec.names.plural == "kafkatopics");
        check!(
            crd.spec
                .names
                .short_names
                .as_ref()
                .is_some_and(|v| v.contains(&"kt".to_string())),
            "expected shortname `kt`",
        );
        check!(crd.spec.versions.len() == 1);
        check!(crd.spec.versions[0].name == "v1alpha1");
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
        for want in [
            "\"topicName\":\"Demo.Topic\"",
            "\"partitions\":3",
            "\"preserveTopic\":true",
        ] {
            assert!(json.contains(want), "case {want:?}; got: {json}");
        }
        let back: KafkaTopic = serde_json::from_str(&json).unwrap();
        assert!(back.spec == kt.spec);
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
        check!(!j.contains("topicName"), "got: {j}");
        check!(!j.contains("config"), "got: {j}");
        // `preserveTopic` is a plain bool — serde emits it.
        check!(j.contains("\"preserveTopic\":false"), "got: {j}");
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
        assert!(
            spec == KafkaTopicSpec {
                topic_name: None,
                partitions: 1,
                replicas: 1,
                config: None,
                preserve_topic: false,
            }
        );
    }
}
