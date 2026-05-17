//! Kafka CRD reconciler.
//!
//! Materializes a single-broker `KRaft` mixed-mode cluster out of a `Kafka`
//! resource: headless `Service`, `ConfigMap`, cluster-id `Secret`, and a
//! `StatefulSet` running `crabka-broker`. Status conditions reflect the
//! `StatefulSet` rollout state (`Available` / `NoBrokersReady` /
//! `PartiallyReady`) and the `replicas` / `readyReplicas` mirrors are
//! projected from the live `StatefulSet`.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt as _;
use k8s_openapi::ByteString;
use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::{ConfigMap, ResourceRequirements, Secret, Service};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use kube::Resource;
use kube::ResourceExt as _;
use kube::api::{Api, Patch, PatchParams, PostParams};
use kube::runtime::controller::{Action, Controller};
use kube::runtime::watcher;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::json;

use crate::context::Context;
use crate::crd::{Kafka, KafkaCondition, KafkaStatus};

const FIELD_MANAGER: &str = "crabka-operator";

pub(crate) const BROKER_PORT: i32 = 9092;
pub(crate) const APP_LABEL: &str = "crabka-broker";
pub(crate) const DEFAULT_BROKER_IMAGE: &str = concat!(
    "ghcr.io/robot-head/crabka-broker:",
    env!("CARGO_PKG_VERSION")
);

#[derive(Debug, thiserror::Error)]
pub enum ReconcileError {
    #[error("kube error: {0}")]
    Kube(#[from] kube::Error),
    #[error("Kafka resource missing uid (not yet admitted)")]
    MissingUid,
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("spec.replicas={0} is unsupported in slice 19 (only 1 allowed)")]
    UnsupportedReplicas(i32),
    #[error("cluster-id secret malformed: {0}")]
    MalformedSecret(String),
}

/// Run the `Kafka` controller forever. Returns only on irrecoverable
/// stream error (the kube-rs `Controller` re-establishes watches on
/// recoverable errors internally).
pub async fn run(ctx: Context) -> anyhow::Result<()> {
    let api: Api<Kafka> = Api::all(ctx.client.clone());
    let sts_api: Api<StatefulSet> = Api::all(ctx.client.clone());
    Controller::new(api, watcher::Config::default())
        .owns(sts_api, watcher::Config::default())
        .run(reconcile, error_policy, Arc::new(ctx))
        .for_each(|res| async move {
            match res {
                Ok((obj, _)) => tracing::debug!(?obj, "reconciled"),
                Err(e) => tracing::warn!(error = %e, "reconcile error"),
            }
        })
        .await;
    Ok(())
}

pub async fn reconcile(obj: Arc<Kafka>, ctx: Arc<Context>) -> Result<Action, ReconcileError> {
    let ns = obj.namespace().unwrap_or_else(|| "default".into());
    let name = obj.name_any();
    tracing::info!(%ns, %name, "reconciling Kafka");

    let kafka_api: Api<Kafka> = Api::namespaced(ctx.client.clone(), &ns);

    // Reject unsupported replica counts up-front; surface as a status
    // condition and stop reconciling until the spec changes.
    if obj.spec.replicas != 1 {
        let bad = obj.spec.replicas;
        let status = KafkaStatus {
            conditions: vec![condition(
                "Ready",
                "False",
                "UnsupportedReplicaCount",
                &format!("spec.replicas must be 1 in slice 19, got {bad}"),
            )],
            replicas: None,
            ready_replicas: None,
        };
        patch_status(&kafka_api, &name, status).await?;
        return Ok(Action::await_change());
    }

    // 1. Service
    let svc = render_service(&obj)?;
    let svc_api: Api<Service> = Api::namespaced(ctx.client.clone(), &ns);
    let svc_name = format!("{name}-broker-headless");
    apply_object(&svc_api, &svc_name, &svc).await?;

    // 2. ConfigMap
    let cm = render_configmap(&obj)?;
    let cm_api: Api<ConfigMap> = Api::namespaced(ctx.client.clone(), &ns);
    let cm_name = format!("{name}-broker-config");
    apply_object(&cm_api, &cm_name, &cm).await?;

    // 3. Secret with if-not-exists semantics. We never overwrite an
    //    existing cluster-id Secret: lose any race by re-reading.
    let secret_api: Api<Secret> = Api::namespaced(ctx.client.clone(), &ns);
    let secret_name = format!("{name}-cluster-id");
    let cluster_id = if let Some(existing) = secret_api.get_opt(&secret_name).await? {
        uuid_from_secret(&existing)?
    } else {
        let new_id = uuid::Uuid::new_v4();
        let s = render_secret(&obj, new_id)?;
        match secret_api.create(&PostParams::default(), &s).await {
            Ok(_) => new_id,
            Err(kube::Error::Api(e)) if e.code == 409 => {
                let fetched = secret_api.get(&secret_name).await?;
                uuid_from_secret(&fetched)?
            }
            Err(e) => return Err(e.into()),
        }
    };
    // cluster_id is injected into the StatefulSet via a secretKeyRef env
    // var; we don't need to use the value directly here. Bind to `_`
    // explicitly so clippy doesn't complain about the let binding.
    let _ = cluster_id;

    // 4. StatefulSet
    let image = obj
        .spec
        .image
        .clone()
        .or_else(|| ctx.config.default_broker_image.clone())
        .unwrap_or_else(|| DEFAULT_BROKER_IMAGE.into());
    let sts = render_statefulset(&obj, &image)?;
    let sts_api: Api<StatefulSet> = Api::namespaced(ctx.client.clone(), &ns);
    let sts_name = format!("{name}-broker");
    apply_object(&sts_api, &sts_name, &sts).await?;

    // 5. Reflect status from the live StatefulSet.
    let live = sts_api.get_opt(&sts_name).await?;
    let (replicas, ready_replicas, reason, message) =
        derive_status(live.as_ref(), obj.spec.replicas);
    let ready_status = if reason == "Available" {
        "True"
    } else {
        "False"
    };
    let status = KafkaStatus {
        conditions: vec![condition("Ready", ready_status, reason, &message)],
        replicas,
        ready_replicas,
    };
    patch_status(&kafka_api, &name, status).await?;

    Ok(Action::requeue(Duration::from_secs(30)))
}

pub fn error_policy(_obj: Arc<Kafka>, err: &ReconcileError, _ctx: Arc<Context>) -> Action {
    tracing::warn!(error = %err, "reconcile error, requeueing");
    Action::requeue(Duration::from_secs(15))
}

/// Build a Kubernetes-style condition with `lastTransitionTime` set to
/// now (RFC3339, second precision, with `Z`).
fn condition(type_: &str, status: &str, reason: &str, message: &str) -> KafkaCondition {
    KafkaCondition {
        type_: type_.into(),
        status: status.into(),
        reason: reason.into(),
        message: message.into(),
        last_transition_time: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
    }
}

/// Server-side apply a typed object. Field manager is `crabka-operator`,
/// force-takeover is on so we wrest fields back from the slice-17 manager
/// if any happen to linger. Object shape is stable across reconciles
/// because renderers are pure functions of the owner.
async fn apply_object<K>(api: &Api<K>, name: &str, obj: &K) -> Result<(), ReconcileError>
where
    K: Resource + Clone + Serialize + DeserializeOwned + Debug,
{
    let params = PatchParams {
        field_manager: Some(FIELD_MANAGER.into()),
        force: true,
        ..Default::default()
    };
    api.patch(name, &params, &Patch::Apply(obj)).await?;
    Ok(())
}

/// Merge-patch the Kafka status subresource. Uses `Patch::Merge` so we
/// only overwrite the fields we set rather than replacing the whole
/// status (which would conflict with any future status writers).
async fn patch_status(
    api: &Api<Kafka>,
    name: &str,
    status: KafkaStatus,
) -> Result<(), ReconcileError> {
    let patch = json!({ "status": status });
    let params = PatchParams {
        field_manager: Some(FIELD_MANAGER.into()),
        ..Default::default()
    };
    api.patch_status(name, &params, &Patch::Merge(&patch))
        .await?;
    Ok(())
}

/// Read `data.clusterId` from a Secret, decode the bytes as UTF-8, and
/// parse the hyphenated UUID. Returns `MalformedSecret` rather than
/// panicking if the Secret was hand-edited or otherwise unparsable —
/// the operator should not crash on bad operator input.
fn uuid_from_secret(secret: &Secret) -> Result<uuid::Uuid, ReconcileError> {
    let data = secret
        .data
        .as_ref()
        .ok_or_else(|| ReconcileError::MalformedSecret("Secret.data is empty".into()))?;
    let bytes = &data
        .get("clusterId")
        .ok_or_else(|| ReconcileError::MalformedSecret("missing clusterId key".into()))?
        .0;
    let s = std::str::from_utf8(bytes)
        .map_err(|e| ReconcileError::MalformedSecret(format!("clusterId not UTF-8: {e}")))?;
    uuid::Uuid::parse_str(s)
        .map_err(|e| ReconcileError::MalformedSecret(format!("clusterId not a UUID: {e}")))
}

/// Pure helper deriving the status fields from the live `StatefulSet`.
/// Returns `(replicas, readyReplicas, reason, message)`. The caller maps
/// `reason == "Available"` to `Ready=True`, anything else to `Ready=False`.
pub(crate) fn derive_status(
    live: Option<&StatefulSet>,
    desired_replicas: i32,
) -> (Option<i32>, Option<i32>, &'static str, String) {
    let (replicas, ready_replicas) = live
        .and_then(|s| s.status.as_ref())
        .map_or((None, None), |st| (Some(st.replicas), st.ready_replicas));

    let ready_count = ready_replicas.unwrap_or(0);
    if ready_count == desired_replicas {
        (
            replicas,
            ready_replicas,
            "Available",
            format!("{desired_replicas} broker(s) ready"),
        )
    } else if ready_count == 0 {
        (
            replicas,
            ready_replicas,
            "NoBrokersReady",
            format!("0/{desired_replicas} brokers ready"),
        )
    } else {
        (
            replicas,
            ready_replicas,
            "PartiallyReady",
            format!("{ready_count}/{desired_replicas} brokers ready"),
        )
    }
}

pub(crate) fn common_labels(owner_name: &str, kafka_version: &str) -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    m.insert("app.kubernetes.io/name".into(), APP_LABEL.into());
    m.insert("app.kubernetes.io/instance".into(), owner_name.into());
    m.insert("app.kubernetes.io/version".into(), kafka_version.into());
    m.insert(
        "app.kubernetes.io/managed-by".into(),
        "crabka-operator".into(),
    );
    m
}

pub(crate) fn owner_ref(owner: &Kafka) -> Result<OwnerReference, ReconcileError> {
    let uid = owner
        .metadata
        .uid
        .as_deref()
        .ok_or(ReconcileError::MissingUid)?;
    Ok(OwnerReference {
        api_version: "crabka.io/v1alpha1".into(),
        kind: "Kafka".into(),
        name: owner.metadata.name.clone().unwrap_or_default(),
        uid: uid.to_string(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    })
}

pub(crate) fn default_resources() -> ResourceRequirements {
    let mut requests = BTreeMap::new();
    requests.insert("cpu".into(), Quantity("100m".into()));
    requests.insert("memory".into(), Quantity("256Mi".into()));
    let mut limits = BTreeMap::new();
    limits.insert("cpu".into(), Quantity("1000m".into()));
    limits.insert("memory".into(), Quantity("1Gi".into()));
    ResourceRequirements {
        requests: Some(requests),
        limits: Some(limits),
        ..Default::default()
    }
}

pub(crate) fn render_service(owner: &Kafka) -> Result<Service, ReconcileError> {
    let name = owner.metadata.name.clone().unwrap_or_default();
    let labels = common_labels(&name, &owner.spec.kafka_version);
    let mut selector: BTreeMap<String, String> = BTreeMap::new();
    selector.insert("app.kubernetes.io/name".into(), APP_LABEL.into());
    selector.insert("app.kubernetes.io/instance".into(), name.clone());

    let svc: Service = serde_json::from_value(json!({
        "metadata": {
            "name": format!("{name}-broker-headless"),
            "namespace": owner.metadata.namespace.clone(),
            "labels": labels,
            "ownerReferences": [owner_ref(owner)?],
        },
        "spec": {
            "clusterIP": "None",
            "selector": selector,
            "ports": [{
                "name": "kafka-internal",
                "port": BROKER_PORT,
                "protocol": "TCP",
                "targetPort": BROKER_PORT,
            }],
        }
    }))?;
    Ok(svc)
}

pub(crate) fn render_configmap(owner: &Kafka) -> Result<ConfigMap, ReconcileError> {
    let name = owner.metadata.name.clone().unwrap_or_default();
    let labels = common_labels(&name, &owner.spec.kafka_version);

    let mut data = BTreeMap::new();
    data.insert(
        "broker.env".to_string(),
        format!("CRABKA_LISTEN_ADDR=0.0.0.0:{BROKER_PORT}\n"),
    );

    Ok(ConfigMap {
        metadata: ObjectMeta {
            name: Some(format!("{name}-broker-config")),
            namespace: owner.metadata.namespace.clone(),
            labels: Some(labels),
            owner_references: Some(vec![owner_ref(owner)?]),
            ..Default::default()
        },
        data: Some(data),
        ..Default::default()
    })
}

pub(crate) fn render_secret(
    owner: &Kafka,
    cluster_id: uuid::Uuid,
) -> Result<Secret, ReconcileError> {
    let name = owner.metadata.name.clone().unwrap_or_default();
    let labels = common_labels(&name, &owner.spec.kafka_version);

    let mut data = BTreeMap::new();
    data.insert(
        "clusterId".to_string(),
        ByteString(cluster_id.to_string().into_bytes()),
    );

    Ok(Secret {
        metadata: ObjectMeta {
            name: Some(format!("{name}-cluster-id")),
            namespace: owner.metadata.namespace.clone(),
            labels: Some(labels),
            owner_references: Some(vec![owner_ref(owner)?]),
            ..Default::default()
        },
        type_: Some("Opaque".into()),
        data: Some(data),
        ..Default::default()
    })
}

pub(crate) fn render_statefulset(
    owner: &Kafka,
    broker_image: &str,
) -> Result<StatefulSet, ReconcileError> {
    let name = owner.metadata.name.clone().unwrap_or_default();
    let namespace = owner.metadata.namespace.clone().unwrap_or_default();
    let labels = common_labels(&name, &owner.spec.kafka_version);
    let mut selector: BTreeMap<String, String> = BTreeMap::new();
    selector.insert("app.kubernetes.io/name".into(), APP_LABEL.into());
    selector.insert("app.kubernetes.io/instance".into(), name.clone());

    let resources = owner
        .spec
        .resources
        .clone()
        .unwrap_or_else(default_resources);

    let secret_name = format!("{name}-cluster-id");
    let service_name = format!("{name}-broker-headless");
    let sts_name = format!("{name}-broker");
    let advertised =
        format!("$(POD_NAME).{service_name}.$(POD_NAMESPACE).svc.cluster.local:{BROKER_PORT}");
    let init_script = "set -eu\nif [ ! -f /var/lib/crabka/data/.formatted ]; then\n  /usr/bin/crabka format --log-dir /var/lib/crabka/data --cluster-id \"$CRABKA_CLUSTER_ID\"\n  touch /var/lib/crabka/data/.formatted\nfi\n";

    let sts: StatefulSet = serde_json::from_value(json!({
        "metadata": {
            "name": sts_name,
            "namespace": namespace,
            "labels": labels,
            "ownerReferences": [owner_ref(owner)?],
        },
        "spec": {
            "serviceName": service_name,
            "replicas": 1,
            "podManagementPolicy": "Parallel",
            "selector": { "matchLabels": selector },
            "template": {
                "metadata": { "labels": labels },
                "spec": {
                    "securityContext": {
                        "runAsNonRoot": true,
                        "runAsUser": 65532,
                        "fsGroup": 65532,
                        "seccompProfile": { "type": "RuntimeDefault" }
                    },
                    "initContainers": [{
                        "name": "format",
                        "image": broker_image,
                        "command": ["/bin/sh", "-c"],
                        "args": [init_script],
                        "env": [{
                            "name": "CRABKA_CLUSTER_ID",
                            "valueFrom": { "secretKeyRef": { "name": secret_name, "key": "clusterId" } }
                        }],
                        "volumeMounts": [{ "name": "data", "mountPath": "/var/lib/crabka/data" }],
                        "securityContext": {
                            "allowPrivilegeEscalation": false,
                            "readOnlyRootFilesystem": true,
                            "capabilities": { "drop": ["ALL"] }
                        }
                    }],
                    "containers": [{
                        "name": "broker",
                        "image": broker_image,
                        "command": ["/usr/bin/crabka-broker"],
                        "args": [
                            "--listen-addr=0.0.0.0:9092",
                            "--log-dir=/var/lib/crabka/data",
                            "--broker-id=0"
                        ],
                        "env": [
                            { "name": "POD_NAME", "valueFrom": { "fieldRef": { "fieldPath": "metadata.name" } } },
                            { "name": "POD_NAMESPACE", "valueFrom": { "fieldRef": { "fieldPath": "metadata.namespace" } } },
                            { "name": "CRABKA_CLUSTER_ID", "valueFrom": { "secretKeyRef": { "name": secret_name, "key": "clusterId" } } },
                            { "name": "CRABKA_ADVERTISED_LISTENER", "value": advertised }
                        ],
                        "ports": [{ "containerPort": BROKER_PORT, "name": "kafka-internal", "protocol": "TCP" }],
                        "readinessProbe": {
                            "tcpSocket": { "port": BROKER_PORT },
                            "initialDelaySeconds": 2,
                            "periodSeconds": 5
                        },
                        "livenessProbe": {
                            "tcpSocket": { "port": BROKER_PORT },
                            "initialDelaySeconds": 30,
                            "periodSeconds": 10
                        },
                        "resources": resources,
                        "volumeMounts": [{ "name": "data", "mountPath": "/var/lib/crabka/data" }],
                        "securityContext": {
                            "allowPrivilegeEscalation": false,
                            "readOnlyRootFilesystem": true,
                            "capabilities": { "drop": ["ALL"] }
                        }
                    }],
                    "volumes": [{ "name": "data", "emptyDir": {} }]
                }
            }
        }
    }))?;
    Ok(sts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::KafkaSpec;
    use base64::Engine as _;

    fn fixture(name: &str, replicas: i32) -> Kafka {
        let mut k = Kafka::new(
            name,
            KafkaSpec {
                kafka_version: "0.1.1".into(),
                replicas,
                image: None,
                resources: None,
            },
        );
        k.metadata.namespace = Some("default".into());
        k.metadata.uid = Some("u-1".into());
        k
    }

    fn assert_owner_ref(refs: Option<&Vec<OwnerReference>>) {
        let refs = refs.expect("owner references must be set");
        assert_eq!(refs.len(), 1);
        let r = &refs[0];
        assert_eq!(r.api_version, "crabka.io/v1alpha1");
        assert_eq!(r.kind, "Kafka");
        assert_eq!(r.name, "demo");
        assert_eq!(r.uid, "u-1");
        assert_eq!(r.controller, Some(true));
        assert_eq!(r.block_owner_deletion, Some(true));
    }

    #[test]
    fn render_service_clusterip_none_owner_ref_set() {
        let k = fixture("demo", 1);
        let svc = render_service(&k).unwrap();
        assert_eq!(svc.metadata.name.as_deref(), Some("demo-broker-headless"));
        assert_eq!(svc.metadata.namespace.as_deref(), Some("default"));
        let spec = svc.spec.expect("service spec");
        assert_eq!(spec.cluster_ip.as_deref(), Some("None"));
        let ports = spec.ports.expect("ports");
        assert_eq!(ports.len(), 1);
        assert_eq!(ports[0].port, BROKER_PORT);
        assert_eq!(ports[0].name.as_deref(), Some("kafka-internal"));
        assert_owner_ref(svc.metadata.owner_references.as_ref());
    }

    #[test]
    fn render_configmap_carries_owner_ref() {
        let k = fixture("demo", 1);
        let cm = render_configmap(&k).unwrap();
        assert_eq!(cm.metadata.name.as_deref(), Some("demo-broker-config"));
        assert_eq!(cm.metadata.namespace.as_deref(), Some("default"));
        let data = cm.data.expect("configmap data");
        let env = data.get("broker.env").expect("broker.env key");
        assert!(env.contains("CRABKA_LISTEN_ADDR=0.0.0.0:9092"));
        assert_owner_ref(cm.metadata.owner_references.as_ref());
    }

    #[test]
    fn render_secret_data_is_base64_uuid() {
        let k = fixture("demo", 1);
        let id = uuid::Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();
        let secret = render_secret(&k, id).unwrap();
        assert_eq!(secret.metadata.name.as_deref(), Some("demo-cluster-id"));
        assert_eq!(secret.type_.as_deref(), Some("Opaque"));
        let data = secret.data.as_ref().expect("secret data");
        let bytes = &data.get("clusterId").expect("clusterId key").0;
        assert_eq!(
            std::str::from_utf8(bytes).unwrap(),
            id.to_string(),
            "in-memory bytes are the raw UUID string; ByteString serializes them as base64"
        );

        let serialized = serde_json::to_value(&secret).unwrap();
        let b64 = serialized["data"]["clusterId"]
            .as_str()
            .expect("data.clusterId is a base64 string on the wire");
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .expect("valid base64");
        assert_eq!(std::str::from_utf8(&decoded).unwrap(), id.to_string());
        assert_owner_ref(secret.metadata.owner_references.as_ref());
    }

    fn broker_container(sts: &StatefulSet) -> &k8s_openapi::api::core::v1::Container {
        let spec = sts.spec.as_ref().expect("sts spec");
        let pod = spec.template.spec.as_ref().expect("pod spec");
        pod.containers
            .iter()
            .find(|c| c.name == "broker")
            .expect("broker container")
    }

    #[test]
    fn render_statefulset_default_image() {
        let k = fixture("demo", 1);
        let sts = render_statefulset(&k, DEFAULT_BROKER_IMAGE).unwrap();
        assert_eq!(sts.metadata.name.as_deref(), Some("demo-broker"));
        let spec = sts.spec.as_ref().expect("sts spec");
        assert_eq!(spec.service_name.as_deref(), Some("demo-broker-headless"));
        assert_eq!(spec.pod_management_policy.as_deref(), Some("Parallel"));
        assert_eq!(spec.replicas, Some(1));
        let pod = spec.template.spec.as_ref().expect("pod spec");
        assert_eq!(pod.init_containers.as_ref().expect("init").len(), 1);
        assert_eq!(pod.containers.len(), 1);
        assert_eq!(
            broker_container(&sts).image.as_deref(),
            Some(DEFAULT_BROKER_IMAGE)
        );
        assert_owner_ref(sts.metadata.owner_references.as_ref());
    }

    #[test]
    fn render_statefulset_user_image_override() {
        let k = fixture("demo", 1);
        let img = "registry.example.com/custom-broker:9.9.9";
        let sts = render_statefulset(&k, img).unwrap();
        assert_eq!(broker_container(&sts).image.as_deref(), Some(img));
        let pod = sts.spec.unwrap().template.spec.unwrap();
        let init = &pod.init_containers.unwrap()[0];
        assert_eq!(init.image.as_deref(), Some(img));
    }

    #[test]
    fn render_statefulset_resources_default() {
        let k = fixture("demo", 1);
        let sts = render_statefulset(&k, DEFAULT_BROKER_IMAGE).unwrap();
        let resources = broker_container(&sts)
            .resources
            .as_ref()
            .expect("resources");
        let requests = resources.requests.as_ref().expect("requests");
        assert_eq!(requests.get("cpu"), Some(&Quantity("100m".into())));
        assert_eq!(requests.get("memory"), Some(&Quantity("256Mi".into())));
        let limits = resources.limits.as_ref().expect("limits");
        assert_eq!(limits.get("cpu"), Some(&Quantity("1000m".into())));
        assert_eq!(limits.get("memory"), Some(&Quantity("1Gi".into())));
    }

    #[test]
    fn render_statefulset_resources_user_override() {
        let mut k = fixture("demo", 1);
        let mut requests = BTreeMap::new();
        requests.insert("cpu".into(), Quantity("250m".into()));
        requests.insert("memory".into(), Quantity("512Mi".into()));
        let mut limits = BTreeMap::new();
        limits.insert("cpu".into(), Quantity("2000m".into()));
        limits.insert("memory".into(), Quantity("4Gi".into()));
        k.spec.resources = Some(ResourceRequirements {
            requests: Some(requests),
            limits: Some(limits),
            ..Default::default()
        });
        let sts = render_statefulset(&k, DEFAULT_BROKER_IMAGE).unwrap();
        let resources = broker_container(&sts)
            .resources
            .as_ref()
            .expect("resources");
        assert_eq!(
            resources.requests.as_ref().unwrap().get("cpu"),
            Some(&Quantity("250m".into()))
        );
        assert_eq!(
            resources.limits.as_ref().unwrap().get("memory"),
            Some(&Quantity("4Gi".into()))
        );
    }

    #[test]
    fn render_statefulset_includes_cluster_id_env_from_secret() {
        let k = fixture("demo", 1);
        let sts = render_statefulset(&k, DEFAULT_BROKER_IMAGE).unwrap();
        let c = broker_container(&sts);
        let env = c.env.as_ref().expect("env");
        let names: Vec<&str> = env.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"POD_NAME"));
        assert!(names.contains(&"POD_NAMESPACE"));
        assert!(names.contains(&"CRABKA_CLUSTER_ID"));
        assert!(names.contains(&"CRABKA_ADVERTISED_LISTENER"));

        let cluster = env
            .iter()
            .find(|e| e.name == "CRABKA_CLUSTER_ID")
            .expect("CRABKA_CLUSTER_ID");
        let src = cluster
            .value_from
            .as_ref()
            .expect("CRABKA_CLUSTER_ID uses valueFrom");
        let key_ref = src.secret_key_ref.as_ref().expect("secretKeyRef");
        assert_eq!(key_ref.name, "demo-cluster-id");
        assert_eq!(key_ref.key, "clusterId");

        let adv = env
            .iter()
            .find(|e| e.name == "CRABKA_ADVERTISED_LISTENER")
            .expect("advertised listener env");
        let val = adv.value.as_deref().expect("inline value");
        assert!(
            val.contains("$(POD_NAME).demo-broker-headless.$(POD_NAMESPACE).svc.cluster.local"),
            "unexpected advertised listener template: {val}"
        );
    }

    #[test]
    fn derive_status_handles_all_rollout_states() {
        use k8s_openapi::api::apps::v1::StatefulSetStatus;

        fn sts_with(replicas: i32, ready: Option<i32>) -> StatefulSet {
            StatefulSet {
                status: Some(StatefulSetStatus {
                    replicas,
                    ready_replicas: ready,
                    ..Default::default()
                }),
                ..Default::default()
            }
        }

        // No live StatefulSet — NoBrokersReady, no replica mirrors.
        let (r, rr, reason, msg) = derive_status(None, 1);
        assert_eq!(r, None);
        assert_eq!(rr, None);
        assert_eq!(reason, "NoBrokersReady");
        assert!(msg.contains("0/1"));

        // Live STS with replicas=1 but no readyReplicas yet ⇒
        // NoBrokersReady, replicas mirror set.
        let sts = sts_with(1, None);
        let (r, rr, reason, _msg) = derive_status(Some(&sts), 1);
        assert_eq!(r, Some(1));
        assert_eq!(rr, None);
        assert_eq!(reason, "NoBrokersReady");

        // Ready: readyReplicas == desired.
        let sts = sts_with(1, Some(1));
        let (_, rr, reason, _) = derive_status(Some(&sts), 1);
        assert_eq!(rr, Some(1));
        assert_eq!(reason, "Available");

        // Partial — desired=3 but only 2 ready. (Slice 19 only allows
        // replicas=1; the helper is generic so slice 20 will exercise
        // this branch.)
        let sts = sts_with(3, Some(2));
        let (_, _, reason, msg) = derive_status(Some(&sts), 3);
        assert_eq!(reason, "PartiallyReady");
        assert!(msg.contains("2/3"));
    }
}
