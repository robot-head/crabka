//! Shared helpers across the `Kafka` and `KafkaNodePool` reconcilers.
//!
//! The `Kafka`-owned cluster-level objects (`Service`, `ConfigMap`,
//! cluster-id `Secret`) live here because both reconcilers need to refer
//! to their names (the pool reconciler reads the Secret; the parent
//! reconciler renders+applies them). The status-derivation helper, the
//! SSA / merge-patch wrappers, and the labels / owner-ref helpers are
//! shared verbatim.

use std::collections::BTreeMap;
use std::fmt::Debug;

use k8s_openapi::ByteString;
use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::{ConfigMap, Secret, Service};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use kube::Resource;
use kube::api::{Api, Patch, PatchParams, PostParams};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::json;
use uuid::Uuid;

use crate::crd::{Kafka, KafkaCondition};

pub(crate) const FIELD_MANAGER: &str = "crabka-operator";

pub(crate) const BROKER_PORT: i32 = 9092;
pub(crate) const APP_LABEL: &str = "crabka-broker";
pub(crate) const DEFAULT_BROKER_IMAGE: &str = concat!(
    "ghcr.io/robot-head/crabka-broker:",
    env!("CARGO_PKG_VERSION")
);

/// Reconcile-error surface shared by both reconcilers.
#[derive(Debug, thiserror::Error)]
pub enum ReconcileError {
    #[error("kube error: {0}")]
    Kube(#[from] kube::Error),
    #[error("resource missing uid (not yet admitted)")]
    MissingUid,
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("spec.replicas={0} is unsupported (only 1 allowed in slice 20)")]
    UnsupportedReplicas(i32),
    #[error("cluster-id secret malformed: {0}")]
    MalformedSecret(String),
}

/// Build a Kubernetes-style condition with `lastTransitionTime` set to
/// now (RFC3339, second precision, with `Z`).
pub(crate) fn condition(type_: &str, status: &str, reason: &str, message: &str) -> KafkaCondition {
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
pub(crate) async fn apply_object<K>(api: &Api<K>, name: &str, obj: &K) -> Result<(), ReconcileError>
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

/// Merge-patch the status subresource of a CR. Uses `Patch::Merge` so we
/// only overwrite the fields we set rather than replacing the whole
/// status (which would conflict with any future status writers). Generic
/// over the parent resource `K` and status payload `S`.
pub(crate) async fn patch_status<K, S>(
    api: &Api<K>,
    name: &str,
    status: S,
) -> Result<(), ReconcileError>
where
    K: Resource + Clone + Serialize + DeserializeOwned + Debug,
    <K as Resource>::DynamicType: Default,
    S: Serialize,
{
    let patch = json!({ "status": status });
    let params = PatchParams {
        field_manager: Some(FIELD_MANAGER.into()),
        ..Default::default()
    };
    api.patch_status(name, &params, &Patch::Merge(&patch))
        .await?;
    Ok(())
}

/// Common labels for objects owned by a `Kafka`. When the object belongs
/// to a specific pool (i.e. pod-level labels on a `StatefulSet`), pass
/// `Some(<pool name>)`; cluster-level objects (`Service` / `ConfigMap` /
/// `Secret`) pass `None`.
pub(crate) fn common_labels(
    kafka_name: &str,
    kafka_version: &str,
    pool: Option<&str>,
) -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    m.insert("app.kubernetes.io/name".into(), APP_LABEL.into());
    m.insert("app.kubernetes.io/instance".into(), kafka_name.into());
    m.insert("app.kubernetes.io/version".into(), kafka_version.into());
    m.insert(
        "app.kubernetes.io/managed-by".into(),
        "crabka-operator".into(),
    );
    if let Some(p) = pool {
        m.insert("crabka.io/pool".into(), p.into());
    }
    m
}

/// Generic owner-reference builder. Works for any CR (`Kafka`,
/// `KafkaNodePool`) whose `DynamicType = ()`. Reads `apiVersion` and
/// `kind` from the trait, name from the metadata.
pub(crate) fn owner_ref<T>(obj: &T) -> Result<OwnerReference, ReconcileError>
where
    T: Resource<DynamicType = ()>,
{
    let uid = obj
        .meta()
        .uid
        .as_deref()
        .ok_or(ReconcileError::MissingUid)?;
    Ok(OwnerReference {
        api_version: T::api_version(&()).to_string(),
        kind: T::kind(&()).to_string(),
        name: obj.meta().name.clone().unwrap_or_default(),
        uid: uid.to_string(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    })
}

/// Render the cluster-level headless `Service`. Owner-ref'd to the
/// parent `Kafka`. Selector matches every pool's pods via the shared
/// `app.kubernetes.io/instance` + `app.kubernetes.io/name` labels.
pub(crate) fn render_service(owner: &Kafka) -> Result<Service, ReconcileError> {
    let name = owner.meta().name.clone().unwrap_or_default();
    let labels = common_labels(&name, &owner.spec.kafka_version, None);
    let mut selector: BTreeMap<String, String> = BTreeMap::new();
    selector.insert("app.kubernetes.io/name".into(), APP_LABEL.into());
    selector.insert("app.kubernetes.io/instance".into(), name.clone());

    let svc: Service = serde_json::from_value(json!({
        "metadata": {
            "name": format!("{name}-broker-headless"),
            "namespace": owner.meta().namespace.clone(),
            "labels": labels,
            "ownerReferences": [owner_ref::<Kafka>(owner)?],
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

/// Render the cluster-level `ConfigMap`. Owner-ref'd to the parent
/// `Kafka`.
pub(crate) fn render_configmap(owner: &Kafka) -> Result<ConfigMap, ReconcileError> {
    let name = owner.meta().name.clone().unwrap_or_default();
    let labels = common_labels(&name, &owner.spec.kafka_version, None);

    let mut data = BTreeMap::new();
    data.insert(
        "broker.env".to_string(),
        format!("CRABKA_LISTEN_ADDR=0.0.0.0:{BROKER_PORT}\n"),
    );

    Ok(ConfigMap {
        metadata: ObjectMeta {
            name: Some(format!("{name}-broker-config")),
            namespace: owner.meta().namespace.clone(),
            labels: Some(labels),
            owner_references: Some(vec![owner_ref::<Kafka>(owner)?]),
            ..Default::default()
        },
        data: Some(data),
        ..Default::default()
    })
}

/// Render the cluster-id `Secret`. Owner-ref'd to the parent `Kafka`.
/// The `clusterId` value is the canonical hyphenated UUID encoded as
/// UTF-8 bytes (k8s wraps with base64 on the wire).
pub(crate) fn render_secret(owner: &Kafka, cluster_id: Uuid) -> Result<Secret, ReconcileError> {
    let name = owner.meta().name.clone().unwrap_or_default();
    let labels = common_labels(&name, &owner.spec.kafka_version, None);

    let mut data = BTreeMap::new();
    data.insert(
        "clusterId".to_string(),
        ByteString(cluster_id.to_string().into_bytes()),
    );

    Ok(Secret {
        metadata: ObjectMeta {
            name: Some(format!("{name}-cluster-id")),
            namespace: owner.meta().namespace.clone(),
            labels: Some(labels),
            owner_references: Some(vec![owner_ref::<Kafka>(owner)?]),
            ..Default::default()
        },
        type_: Some("Opaque".into()),
        data: Some(data),
        ..Default::default()
    })
}

/// Read `data.clusterId` from a Secret, decode the bytes as UTF-8, and
/// parse the hyphenated UUID. Returns `MalformedSecret` rather than
/// panicking if the Secret was hand-edited or otherwise unparsable —
/// the operator should not crash on bad operator input.
pub(crate) fn uuid_from_secret(secret: &Secret) -> Result<Uuid, ReconcileError> {
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
    Uuid::parse_str(s)
        .map_err(|e| ReconcileError::MalformedSecret(format!("clusterId not a UUID: {e}")))
}

/// Get-or-create the cluster-id Secret. Returns the parsed UUID.
///
/// The Secret is created with `Patch::Apply` semantics-equivalent
/// `POST` (i.e. a plain create) so that an existing Secret is never
/// overwritten — the cluster id is a one-shot value that must never
/// change. If the Secret already exists, we read its `clusterId` back.
pub(crate) async fn ensure_cluster_id_secret(
    secret_api: &Api<Secret>,
    parent: &Kafka,
) -> Result<Uuid, ReconcileError> {
    let name = parent.meta().name.clone().unwrap_or_default();
    let secret_name = format!("{name}-cluster-id");
    if let Some(existing) = secret_api.get_opt(&secret_name).await? {
        return uuid_from_secret(&existing);
    }
    let id = Uuid::new_v4();
    let secret = render_secret(parent, id)?;
    secret_api.create(&PostParams::default(), &secret).await?;
    Ok(id)
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
