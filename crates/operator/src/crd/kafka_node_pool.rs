use crabka_units::ByteSize;
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
    /// Roles each node in this pool fulfills. Only
    /// the union `{Controller, Broker}` is supported.
    pub roles: Vec<NodeRole>,

    /// Number of pods. Validation: must equal 1.
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

    /// Kafka client request-dispatch queue capacity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub client_dispatch_queue_capacity: Option<usize>,

    /// Maximum accepted Kafka client frame size.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crabka_units::serde_units::human::option_byte_size"
    )]
    #[schemars(with = "Option<String>")]
    pub client_frame_max: Option<ByteSize>,

    /// Optional pod-level customization applied to every pod in this pool.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template: Option<PodTemplate>,

    /// Storage configuration. `None` (field absent) → emptyDir (the
    /// default). See [`Storage`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage: Option<Storage>,
}

impl KafkaNodePoolSpec {
    pub(crate) fn client_resource_policy(
        &self,
    ) -> Result<
        (
            Option<crabka_client_core::ConnectionDispatchQueueCapacity>,
            Option<crabka_client_core::ClientFrameMax>,
        ),
        String,
    > {
        let queue = self
            .client_dispatch_queue_capacity
            .map(crabka_client_core::ConnectionDispatchQueueCapacity::new)
            .transpose()
            .map_err(|error| format!("spec.clientDispatchQueueCapacity: {error}"))?;
        let frame = self
            .client_frame_max
            .map(crabka_client_core::ClientFrameMax::try_from)
            .transpose()
            .map_err(|error| format!("spec.clientFrameMax: {error}"))?;
        Ok((queue, frame))
    }
}

const fn default_replicas() -> i32 {
    1
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash)]
pub enum NodeRole {
    Controller,
    Broker,
}

/// Storage configuration for the pool's pods. Three variants:
/// - `Ephemeral` (or field absent) — `emptyDir` volume, no PVC.
///   Suitable for dev clusters.
/// - `PersistentClaim` — single PVC per pod via the `StatefulSet`'s
///   `volumeClaimTemplates`. Production-shaped.
/// - `Jbod` — multiple PVCs per pod, one per JBOD disk. The
///   broker spreads partition data across every disk. The
///   lowest-`id` volume is the primary metadata disk.
///
/// The wire shape is flat (Strimzi-shaped): `type` is the discriminator
/// and each variant's fields (`PersistentClaim`: `size`, `class`,
/// `deleteClaim`; `Jbod`: `volumes`, `deleteClaim`) are siblings of
/// `type`. The custom `schema_with` hand-rolls a structural schema
/// because kube-rs 3.x's `StructuralSchemaRewriter` panics when `oneOf`
/// branches share a `type` property with differing enum values (the
/// default schemars output for tagged-union enums).
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(tag = "type")]
#[schemars(schema_with = "storage_schema")]
pub enum Storage {
    Ephemeral,
    PersistentClaim(PersistentClaimSpec),
    Jbod(JbodSpec),
}

/// Hand-rolled structural schema for `Storage`. See the doc comment on
/// [`Storage`] for why this is necessary. The schema validates only
/// the discriminator (`type ∈ {Ephemeral, PersistentClaim, Jbod}`) and
/// the per-variant field types; cross-variant constraints (e.g.
/// "`size` must be present when `type=PersistentClaim`") are enforced
/// by the operator at reconcile time, not by the apiserver.
fn storage_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "type": "object",
        "required": ["type"],
        "properties": {
            "type": {
                "type": "string",
                "enum": ["Ephemeral", "PersistentClaim", "Jbod"],
            },
            "size": { "type": "string" },
            "class": { "type": "string" },
            "deleteClaim": { "type": "boolean" },
            "volumes": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["id", "size"],
                    "properties": {
                        "id": { "type": "integer", "format": "int32" },
                        "size": { "type": "string" },
                        "class": { "type": "string" },
                    },
                },
            },
        },
    })
}

/// `PersistentClaim` configuration. Mirrors Strimzi's
/// `KafkaNodePool.spec.storage` flat shape.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PersistentClaimSpec {
    /// K8s `Quantity` (e.g., `"10Gi"`, `"500Mi"`). Validated at
    /// reconcile time.
    pub size: String,
    /// Storage class name. `None` = cluster default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub class: Option<String>,
    /// `true` → `persistentVolumeClaimRetentionPolicy.whenDeleted: Delete`.
    /// Default `false` (Retain) is the safe option.
    #[serde(default)]
    pub delete_claim: bool,
}

/// `Jbod` configuration: a set of persistent disks, one PVC
/// per disk. The broker spreads partition data across all of them
/// (JBOD / KIP-113). The lowest-`id` volume is the primary metadata
/// disk and keeps the PVC name `data` / mount
/// `/var/lib/crabka/data`; every other volume `id = N` is mounted at
/// `/var/lib/crabka/data-{N}` (PVC `data-{N}`) and handed to the broker
/// via `CRABKA_EXTRA_LOG_DIRS`.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct JbodSpec {
    /// One persistent volume per JBOD disk. Must be non-empty with unique
    /// ids (validated at reconcile time).
    pub volumes: Vec<JbodVolume>,
    /// `true` → `persistentVolumeClaimRetentionPolicy.whenDeleted: Delete`
    /// for *every* JBOD PVC. A `StatefulSet`'s retention policy is
    /// set-wide — K8s offers no per-`volumeClaimTemplate` retention — so
    /// this single flag covers all disks. Default `false` (Retain).
    #[serde(default)]
    pub delete_claim: bool,
}

/// One JBOD disk. `id` is a stable per-disk identifier; the lowest id is
/// the primary metadata disk.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct JbodVolume {
    /// Stable disk id (>= 0). Drives the PVC name / mount path for
    /// non-primary disks (`data-{id}` / `/var/lib/crabka/data-{id}`).
    pub id: i32,
    /// K8s `Quantity` (e.g., `"100Gi"`). Validated at reconcile time.
    pub size: String,
    /// Storage class name. `None` = cluster default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub class: Option<String>,
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
    use assert2::{assert, check};
    use kube::CustomResourceExt as _;

    use super::*;

    #[test]
    fn crd_metadata_is_correct() {
        let crd = KafkaNodePool::crd();
        check!(crd.spec.group == "crabka.io");
        check!(crd.spec.names.kind == "KafkaNodePool");
        check!(crd.spec.names.plural == "kafkanodepools");
        check!(
            crd.spec
                .names
                .short_names
                .as_ref()
                .is_some_and(|v| v.contains(&"knp".to_string())),
            "expected shortname `knp`, got {:?}",
            crd.spec.names.short_names
        );
        check!(crd.spec.versions.len() == 1);
        check!(crd.spec.versions[0].name == "v1alpha1");
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
                client_dispatch_queue_capacity: None,
                client_frame_max: None,
                template: None,
                storage: None,
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
        assert!(back.spec == pool.spec);
    }

    #[test]
    fn spec_defaults_replicas_to_one() {
        let json = r#"{"roles":["Controller","Broker"],"nodeIdStart":0}"#;
        let spec: KafkaNodePoolSpec = serde_json::from_str(json).unwrap();
        assert!(
            spec == KafkaNodePoolSpec {
                roles: vec![NodeRole::Controller, NodeRole::Broker],
                replicas: 1,
                node_id_start: 0,
                image: None,
                resources: None,
                client_dispatch_queue_capacity: None,
                client_frame_max: None,
                template: None,
                storage: None,
            }
        );
    }

    #[test]
    fn client_policy_round_trips_has_schema_and_validates() {
        let json = r#"{
            "roles":["Controller","Broker"],
            "nodeIdStart":0,
            "clientDispatchQueueCapacity":7,
            "clientFrameMax":"32KiB"
        }"#;
        let spec: KafkaNodePoolSpec = serde_json::from_str(json).unwrap();
        let (queue, frame) = spec
            .client_resource_policy()
            .expect("valid broker client policy");
        assert!(queue.expect("queue").get() == 7);
        assert!(frame.expect("frame").size() == crabka_units::kibibytes(32));
        assert!(
            serde_json::from_str::<KafkaNodePoolSpec>(&serde_json::to_string(&spec).unwrap())
                .unwrap()
                == spec
        );

        let crd = serde_json::to_value(KafkaNodePool::crd()).expect("serialize CRD");
        let properties = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
            ["properties"];
        assert!(properties["clientDispatchQueueCapacity"]["minimum"].as_f64() == Some(1.0));
        assert!(properties["clientFrameMax"]["type"] == "string");

        for (json, path) in [
            (
                r#"{"roles":["Broker"],"nodeIdStart":0,"clientDispatchQueueCapacity":0}"#,
                "spec.clientDispatchQueueCapacity",
            ),
            (
                r#"{"roles":["Broker"],"nodeIdStart":0,"clientFrameMax":"0B"}"#,
                "spec.clientFrameMax",
            ),
            (
                r#"{"roles":["Broker"],"nodeIdStart":0,"clientFrameMax":"1.5B"}"#,
                "spec.clientFrameMax",
            ),
            (
                r#"{"roles":["Broker"],"nodeIdStart":0,"clientFrameMax":"101MiB"}"#,
                "spec.clientFrameMax",
            ),
        ] {
            let spec: KafkaNodePoolSpec = serde_json::from_str(json).unwrap();
            let error = spec
                .client_resource_policy()
                .expect_err("invalid broker client policy");
            assert!(error.contains(path), "got: {error}");
        }
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
                client_dispatch_queue_capacity: None,
                client_frame_max: None,
                template: Some(template),
                storage: None,
            },
        );

        let json = serde_json::to_string(&pool).unwrap();
        for want in ["\"team\":\"platform\"", "\"dedicated\"", "\"nodeSelector\""] {
            assert!(json.contains(want), "case {want:?}; got: {json}");
        }
        let back: KafkaNodePool = serde_json::from_str(&json).unwrap();
        assert!(back.spec == pool.spec);
    }

    #[test]
    fn storage_ephemeral_round_trips_through_json() {
        let pool = KafkaNodePool::new(
            "brokers",
            KafkaNodePoolSpec {
                roles: vec![NodeRole::Controller, NodeRole::Broker],
                replicas: 1,
                node_id_start: 0,
                image: None,
                resources: None,
                client_dispatch_queue_capacity: None,
                client_frame_max: None,
                template: None,
                storage: Some(Storage::Ephemeral),
            },
        );
        let json = serde_json::to_string(&pool).unwrap();
        assert!(
            json.contains("\"storage\":{\"type\":\"Ephemeral\"}"),
            "expected flat tagged shape, got: {json}"
        );
        let back: KafkaNodePool = serde_json::from_str(&json).unwrap();
        assert!(back.spec == pool.spec);
    }

    #[test]
    fn storage_persistent_claim_round_trips_through_json() {
        let pool = KafkaNodePool::new(
            "brokers",
            KafkaNodePoolSpec {
                roles: vec![NodeRole::Controller, NodeRole::Broker],
                replicas: 1,
                node_id_start: 0,
                image: None,
                resources: None,
                client_dispatch_queue_capacity: None,
                client_frame_max: None,
                template: None,
                storage: Some(Storage::PersistentClaim(PersistentClaimSpec {
                    size: "10Gi".into(),
                    class: Some("fast-ssd".into()),
                    delete_claim: true,
                })),
            },
        );
        let json = serde_json::to_string(&pool).unwrap();
        for want in [
            "\"type\":\"PersistentClaim\"",
            "\"size\":\"10Gi\"",
            "\"class\":\"fast-ssd\"",
            "\"deleteClaim\":true",
        ] {
            assert!(json.contains(want), "case {want:?}; got: {json}");
        }
        let back: KafkaNodePool = serde_json::from_str(&json).unwrap();
        assert!(back.spec == pool.spec);
    }

    #[test]
    fn spec_defaults_storage_to_none() {
        let json = r#"{"roles":["Controller","Broker"],"nodeIdStart":0}"#;
        let spec: KafkaNodePoolSpec = serde_json::from_str(json).unwrap();
        assert!(spec.storage.is_none());
    }

    #[test]
    fn storage_jbod_round_trips_through_json() {
        let pool = KafkaNodePool::new(
            "brokers",
            KafkaNodePoolSpec {
                roles: vec![NodeRole::Controller, NodeRole::Broker],
                replicas: 1,
                node_id_start: 0,
                image: None,
                resources: None,
                client_dispatch_queue_capacity: None,
                client_frame_max: None,
                template: None,
                storage: Some(Storage::Jbod(JbodSpec {
                    volumes: vec![
                        JbodVolume {
                            id: 0,
                            size: "10Gi".into(),
                            class: None,
                        },
                        JbodVolume {
                            id: 1,
                            size: "20Gi".into(),
                            class: Some("fast-ssd".into()),
                        },
                    ],
                    delete_claim: true,
                })),
            },
        );
        let json = serde_json::to_string(&pool).unwrap();
        for want in [
            "\"type\":\"Jbod\"",
            "\"volumes\":[",
            "\"id\":0",
            "\"size\":\"20Gi\"",
            "\"class\":\"fast-ssd\"",
            "\"deleteClaim\":true",
        ] {
            assert!(json.contains(want), "case {want:?}; got: {json}");
        }
        let back: KafkaNodePool = serde_json::from_str(&json).unwrap();
        assert!(back.spec == pool.spec);
    }

    #[test]
    fn storage_jbod_deserializes_flat_wire_shape() {
        let json = r#"{
            "roles":["Controller","Broker"],
            "nodeIdStart":0,
            "storage":{
                "type":"Jbod",
                "volumes":[{"id":0,"size":"1Gi"},{"id":1,"size":"1Gi"}]
            }
        }"#;
        let spec: KafkaNodePoolSpec = serde_json::from_str(json).unwrap();
        match spec.storage {
            Some(Storage::Jbod(j)) => {
                // deleteClaim defaults to false.
                assert!(
                    j == JbodSpec {
                        volumes: vec![
                            JbodVolume {
                                id: 0,
                                size: "1Gi".to_string(),
                                class: None,
                            },
                            JbodVolume {
                                id: 1,
                                size: "1Gi".to_string(),
                                class: None,
                            },
                        ],
                        delete_claim: false,
                    }
                );
            }
            other => panic!("expected Jbod, got {other:?}"),
        }
    }
}
