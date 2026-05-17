use k8s_openapi::api::core::v1::ResourceRequirements;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A pool of nodes (pods) that share role + image + resources.
/// One `StatefulSet` per pool; pods are addressed via the shared
/// headless `Service` owned by the parent `Kafka`.
#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[kube(
    group = "crabka.io",
    version = "v1alpha1",
    kind = "KafkaNodePool",
    plural = "kafkanodepools",
    singular = "kafkanodepool",
    shortname = "knp",
    namespaced,
    status = "KafkaNodePoolStatus",
    derive = "PartialEq"
)]
#[serde(rename_all = "camelCase")]
pub struct KafkaNodePoolSpec {
    /// Roles each node in this pool fulfills. Slice 20 supports only
    /// the union `{Controller, Broker}`.
    pub roles: Vec<NodeRole>,

    /// Number of pods. Slice 20 validation: must equal 1.
    #[serde(default = "default_replicas")]
    #[schemars(range(min = 1, max = 1))]
    pub replicas: i32,

    /// First node id. Pod ordinal `i` -> `node_id = nodeIdStart + i`.
    #[schemars(range(min = 0, max = 999_999))]
    pub node_id_start: i32,

    /// Container image. Falls back to operator `--default-broker-image`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,

    /// Broker container resources.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceRequirements>,

    /// Optional pod-level customization applied to every pod in this pool.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template: Option<PodTemplate>,
}

const fn default_replicas() -> i32 {
    1
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash)]
pub enum NodeRole {
    Controller,
    Broker,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PodTemplate {
    /// Extra labels / annotations on the pod template.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<MetadataTemplate>,
    /// Forwarded to `PodSpec.affinity`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub affinity: Option<k8s_openapi::api::core::v1::Affinity>,
    /// Forwarded to `PodSpec.tolerations`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tolerations: Vec<k8s_openapi::api::core::v1::Toleration>,
    /// Forwarded to `PodSpec.nodeSelector`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_selector: Option<std::collections::BTreeMap<String, String>>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MetadataTemplate {
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub labels: std::collections::BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub annotations: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaNodePoolStatus {
    /// Standard Kubernetes-style condition list.
    #[serde(default)]
    pub conditions: Vec<crate::crd::kafka::KafkaCondition>,
    /// Mirrors `StatefulSet.status.replicas`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replicas: Option<i32>,
    /// Mirrors `StatefulSet.status.readyReplicas`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready_replicas: Option<i32>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use kube::CustomResourceExt as _;

    #[test]
    fn crd_metadata_is_correct() {
        let crd = KafkaNodePool::crd();
        assert_eq!(crd.spec.group, "crabka.io");
        assert_eq!(crd.spec.names.kind, "KafkaNodePool");
        assert_eq!(crd.spec.names.plural, "kafkanodepools");
        assert!(
            crd.spec
                .names
                .short_names
                .as_ref()
                .is_some_and(|v| v.contains(&"knp".to_string())),
            "expected shortname `knp`, got {:?}",
            crd.spec.names.short_names
        );
        assert_eq!(crd.spec.versions.len(), 1);
        assert_eq!(crd.spec.versions[0].name, "v1alpha1");
    }

    #[test]
    fn round_trips_through_json() {
        let pool = KafkaNodePool::new(
            "brokers",
            KafkaNodePoolSpec {
                roles: vec![NodeRole::Controller, NodeRole::Broker],
                replicas: 1,
                node_id_start: 0,
                image: None,
                resources: None,
                template: None,
            },
        );
        let json = serde_json::to_string(&pool).unwrap();
        assert!(
            json.contains("\"nodeIdStart\""),
            "expected camelCase wire shape, got: {json}"
        );
        assert!(
            json.contains("\"Controller\""),
            "roles serialized in UpperCamelCase, got: {json}"
        );
        let back: KafkaNodePool = serde_json::from_str(&json).unwrap();
        assert_eq!(back.spec, pool.spec);
    }

    #[test]
    fn spec_defaults_replicas_to_one() {
        let json = r#"{"roles":["Controller","Broker"],"nodeIdStart":0}"#;
        let spec: KafkaNodePoolSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.replicas, 1);
        assert!(spec.image.is_none());
        assert!(spec.resources.is_none());
    }

    #[test]
    fn pod_template_round_trips_through_json() {
        use k8s_openapi::api::core::v1::{
            Affinity, NodeAffinity, NodeSelector, NodeSelectorTerm, Toleration,
        };

        let mut labels = std::collections::BTreeMap::new();
        labels.insert("team".into(), "platform".into());

        let template = PodTemplate {
            metadata: Some(MetadataTemplate {
                labels: labels.clone(),
                annotations: std::collections::BTreeMap::new(),
            }),
            affinity: Some(Affinity {
                node_affinity: Some(NodeAffinity {
                    required_during_scheduling_ignored_during_execution: Some(NodeSelector {
                        node_selector_terms: vec![NodeSelectorTerm::default()],
                    }),
                    preferred_during_scheduling_ignored_during_execution: None,
                }),
                ..Default::default()
            }),
            tolerations: vec![Toleration {
                key: Some("dedicated".into()),
                operator: Some("Exists".into()),
                effect: Some("NoSchedule".into()),
                ..Default::default()
            }],
            node_selector: Some({
                let mut m = std::collections::BTreeMap::new();
                m.insert("kubernetes.io/os".into(), "linux".into());
                m
            }),
        };
        let pool = KafkaNodePool::new(
            "brokers",
            KafkaNodePoolSpec {
                roles: vec![NodeRole::Controller, NodeRole::Broker],
                replicas: 1,
                node_id_start: 0,
                image: None,
                resources: None,
                template: Some(template),
            },
        );

        let json = serde_json::to_string(&pool).unwrap();
        assert!(json.contains("\"team\":\"platform\""), "labels: {json}");
        assert!(json.contains("\"dedicated\""), "tolerations: {json}");
        assert!(json.contains("\"nodeSelector\""), "node_selector: {json}");
        let back: KafkaNodePool = serde_json::from_str(&json).unwrap();
        assert_eq!(back.spec, pool.spec);
    }
}
