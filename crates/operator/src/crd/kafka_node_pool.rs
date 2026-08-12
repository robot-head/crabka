use crabka_units::ByteSize;
use k8s_openapi::api::core::v1::ResourceRequirements;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A pool of nodes, which are pods, that share the role, the image, and
/// the resources.
///
/// There is one `StatefulSet` for each pool. Clients address the pods
/// through the shared headless `Service` that the parent `Kafka` owns.
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
    /// Roles that each node in this pool has. The list must contain
    /// `Controller`, `Broker`, or both, without duplicates.
    #[schemars(length(min = 1, max = 2))]
    pub roles: Vec<NodeRole>,

    /// Number of pods. Each ordinal gets one consecutive node id.
    #[serde(default = "default_replicas")]
    #[schemars(range(min = 1))]
    pub replicas: i32,

    /// First node id. Pod ordinal `i` -> `node_id = nodeIdStart + i`.
    #[schemars(range(min = 0, max = 999_999))]
    pub node_id_start: i32,

    /// Container image. The operator uses `--default-broker-image` when
    /// this field is absent.
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

    /// Optional pod-level customization for every pod in this pool.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template: Option<PodTemplate>,

    /// Storage configuration. `None`, which means that the field is
    /// absent, gives an emptyDir. That is the default. See [`Storage`].
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

/// Storage configuration for the pods of the pool.
///
/// There are three variants:
/// - `Ephemeral`, which is also the value when the field is absent. It
///   gives an `emptyDir` volume and no PVC. Use it for dev clusters.
/// - `PersistentClaim`. It gives one PVC for each pod through the
///   `volumeClaimTemplates` of the `StatefulSet`. Use it in production.
/// - `Jbod`. It gives more than one PVC for each pod, one for each JBOD
///   disk. The broker spreads the partition data across every disk. The
///   volume with the lowest `id` is the primary metadata disk.
///
/// The wire shape is flat, in the Strimzi form. `type` is the
/// discriminator, and the fields of each variant are siblings of `type`.
/// `PersistentClaim` has `size`, `class`, and `deleteClaim`. `Jbod` has
/// `volumes` and `deleteClaim`. The custom `schema_with` writes a
/// structural schema by hand, because the `StructuralSchemaRewriter` of
/// kube-rs 3.x panics when two `oneOf` branches share a `type` property
/// with different enum values. That is the default schemars output for
/// tagged-union enums.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(tag = "type")]
#[schemars(schema_with = "storage_schema")]
pub enum Storage {
    Ephemeral,
    PersistentClaim(PersistentClaimSpec),
    Jbod(JbodSpec),
}

/// Structural schema for `Storage`, written by hand.
///
/// The doc comment on [`Storage`] gives the reason for this. The schema
/// validates only the discriminator, where `type` is one of `Ephemeral`,
/// `PersistentClaim`, and `Jbod`, and the field types of each variant. The
/// operator enforces the cross-variant constraints at reconcile time, and
/// the apiserver does not. One such constraint is that `size` must be
/// present when `type=PersistentClaim`.
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

/// `PersistentClaim` configuration.
///
/// It follows the flat shape of `KafkaNodePool.spec.storage` in Strimzi.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PersistentClaimSpec {
    /// K8s `Quantity`, for example `"10Gi"` or `"500Mi"`. The operator
    /// validates it at reconcile time.
    pub size: String,
    /// Storage class name. `None` means the cluster default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub class: Option<String>,
    /// `true` gives
    /// `persistentVolumeClaimRetentionPolicy.whenDeleted: Delete`. The
    /// default `false`, which is Retain, is the safe option.
    #[serde(default)]
    pub delete_claim: bool,
}

/// `Jbod` configuration: a set of persistent disks with one PVC for each
/// disk.
///
/// The broker spreads the partition data across all of them. This is JBOD
/// from KIP-113. The volume with the lowest `id` is the primary metadata
/// disk. It keeps the PVC name `data` and the mount
/// `/var/lib/crabka/data`. Every other volume with `id = N` has the PVC
/// `data-{N}` and the mount `/var/lib/crabka/data-{N}`, and the operator
/// gives it to the broker in `CRABKA_EXTRA_LOG_DIRS`.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct JbodSpec {
    /// One persistent volume for each JBOD disk. The list must be
    /// non-empty and the ids must be unique. The operator validates this
    /// at reconcile time.
    pub volumes: Vec<JbodVolume>,
    /// `true` gives
    /// `persistentVolumeClaimRetentionPolicy.whenDeleted: Delete` for
    /// *every* JBOD PVC. The retention policy of a `StatefulSet` applies
    /// to the whole set, because K8s has no retention setting for one
    /// `volumeClaimTemplate`. This one flag therefore covers all disks.
    /// The default is `false`, which is Retain.
    #[serde(default)]
    pub delete_claim: bool,
}

/// One JBOD disk.
///
/// `id` is a stable identifier for the disk. The disk with the lowest id
/// is the primary metadata disk.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct JbodVolume {
    /// Stable disk id, 0 or more. For a non-primary disk it gives the PVC
    /// name `data-{id}` and the mount path
    /// `/var/lib/crabka/data-{id}`.
    pub id: i32,
    /// K8s `Quantity`, for example `"100Gi"`. The operator validates it
    /// at reconcile time.
    pub size: String,
    /// Storage class name. `None` = cluster default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub class: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PodTemplate {
    /// Extra labels and annotations on the pod template.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<MetadataTemplate>,
    /// The operator copies this to `PodSpec.affinity`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub affinity: Option<k8s_openapi::api::core::v1::Affinity>,
    /// The operator copies this to `PodSpec.tolerations`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tolerations: Vec<k8s_openapi::api::core::v1::Toleration>,
    /// The operator copies this to `PodSpec.nodeSelector`.
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
    /// The same value as `StatefulSet.status.replicas`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replicas: Option<i32>,
    /// The same value as `StatefulSet.status.readyReplicas`.
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
