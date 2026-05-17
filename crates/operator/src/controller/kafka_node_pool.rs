//! `KafkaNodePool` reconciler.
//!
//! A `KafkaNodePool` describes a group of broker pods that share role,
//! image, and resources. The reconciler renders one `StatefulSet` per
//! pool, owner-ref'd to the pool itself, scheduled into the shared
//! headless `Service` owned by the parent `Kafka` (looked up via the
//! `crabka.io/cluster` label).
//!
//! Slice 20 constraints: pools must be mixed `{Controller, Broker}`,
//! `replicas` must equal 1, and `nodeIdStart` must lie in `0..=999_999`.
//! Validation errors surface as a `Ready=False` condition without
//! attempting any further reconcile.

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt as _;
use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::{PersistentVolumeClaim, ResourceRequirements};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::api::Api;
use kube::runtime::controller::{Action, Controller};
use kube::runtime::watcher;
use kube::{Resource, ResourceExt as _};
use serde_json::json;

use crate::context::Context;
use crate::controller::common::{
    self, APP_LABEL, BROKER_PORT, DEFAULT_BROKER_IMAGE, ReconcileError, apply_object,
    common_labels, condition, derive_status, owner_ref,
};
use crate::crd::{
    Kafka, KafkaCondition, KafkaNodePool, KafkaNodePoolStatus, NodeRole, PersistentClaimSpec,
    Storage,
};

/// Validation errors for a `KafkaNodePool`. Each variant maps to a
/// distinct condition reason; the operator surfaces the variant as
/// `Ready=False` and does not attempt further reconcile until the spec
/// is corrected.
#[derive(Debug, thiserror::Error)]
pub enum PoolValidationError {
    #[error("spec.roles must equal {{Controller, Broker}}; got {0:?}")]
    RolesNotMixed(Vec<NodeRole>),
    #[error("spec.replicas={0} is unsupported in slice 20 (only 1 allowed)")]
    ReplicasNotOne(i32),
    #[error("spec.nodeIdStart={0} is out of range 0..=999999")]
    NodeIdOutOfRange(i32),
    #[error("metadata.labels.\"crabka.io/cluster\" missing")]
    MissingClusterLabel,
    #[error("spec.storage.size={0:?} is not a valid positive Quantity ({1})")]
    StorageSizeInvalid(String, &'static str),
    #[error("spec.storage.type changed from {from} to {to}: immutable")]
    StorageTypeChanged {
        from: &'static str,
        to: &'static str,
    },
    #[error("spec.storage.class changed from {from:?} to {to:?}: immutable")]
    StorageClassChanged {
        from: Option<String>,
        to: Option<String>,
    },
    #[error("spec.storage.size decrease from {current} to {desired}: shrink not allowed")]
    StorageShrinkNotAllowed { current: String, desired: String },
}

/// Validate a `KafkaNodePool` spec against slice-20 invariants.
pub(crate) fn validate(pool: &KafkaNodePool) -> Result<(), PoolValidationError> {
    let roles: HashSet<NodeRole> = pool.spec.roles.iter().copied().collect();
    let expected: HashSet<NodeRole> = [NodeRole::Controller, NodeRole::Broker]
        .into_iter()
        .collect();
    if roles != expected {
        return Err(PoolValidationError::RolesNotMixed(pool.spec.roles.clone()));
    }
    if pool.spec.replicas != 1 {
        return Err(PoolValidationError::ReplicasNotOne(pool.spec.replicas));
    }
    if !(0..=999_999).contains(&pool.spec.node_id_start) {
        return Err(PoolValidationError::NodeIdOutOfRange(
            pool.spec.node_id_start,
        ));
    }
    if let Some(Storage::PersistentClaim(pc)) = pool.spec.storage.as_ref() {
        common::parse_quantity(&pc.size)
            .map_err(|why| PoolValidationError::StorageSizeInvalid(pc.size.clone(), why))?;
    }
    Ok(())
}

// Init script: derive ORDINAL from $HOSTNAME (StatefulSet pods are
// named `<sts>-<ordinal>`), compute NODE_ID = NODE_ID_START + ORDINAL,
// run `crabka format` if `.formatted` is missing, then persist the
// node id to `.node-id` for the main container.
//
// `.node-id` is written *after* `crabka format` because `format`
// refuses to overwrite a non-empty `log_dir`. Writing it inside the
// freshly-formatted directory keeps the data dir empty when the
// formatter runs, while still leaving the file in place for the
// broker container.
const INIT_SCRIPT: &str = "set -eu\n\
ORDINAL=\"${HOSTNAME##*-}\"\n\
NODE_ID=$((NODE_ID_START + ORDINAL))\n\
mkdir -p /var/lib/crabka/data\n\
if [ ! -f /var/lib/crabka/data/.formatted ]; then\n\
  /usr/bin/crabka format --log-dir /var/lib/crabka/data --cluster-id \"$CRABKA_CLUSTER_ID\"\n\
  touch /var/lib/crabka/data/.formatted\n\
fi\n\
printf '%s' \"$NODE_ID\" > /var/lib/crabka/data/.node-id\n";

// Main script: read the persisted node id and exec the broker.
const MAIN_SCRIPT: &str = "set -eu\n\
exec /usr/bin/crabka-broker \\\n  --listen-addr=0.0.0.0:9092 \\\n  --log-dir=/var/lib/crabka/data \\\n  --broker-id=\"$(cat /var/lib/crabka/data/.node-id)\"\n";

fn render_init_container(
    broker_image: &str,
    secret_name: &str,
    node_id_start: i32,
) -> serde_json::Value {
    json!({
        "name": "format",
        "image": broker_image,
        "command": ["/bin/sh", "-c"],
        "args": [INIT_SCRIPT],
        "env": [
            { "name": "NODE_ID_START", "value": node_id_start.to_string() },
            { "name": "CRABKA_CLUSTER_ID", "valueFrom": { "secretKeyRef": { "name": secret_name, "key": "clusterId" } } }
        ],
        "volumeMounts": [{ "name": "data", "mountPath": "/var/lib/crabka/data" }],
        "securityContext": {
            "allowPrivilegeEscalation": false,
            "readOnlyRootFilesystem": true,
            "capabilities": { "drop": ["ALL"] }
        }
    })
}

fn render_broker_container(
    broker_image: &str,
    secret_name: &str,
    advertised: &str,
    resources: &ResourceRequirements,
) -> serde_json::Value {
    json!({
        "name": "broker",
        "image": broker_image,
        "command": ["/bin/sh", "-c"],
        "args": [MAIN_SCRIPT],
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
    })
}

/// Build the `StatefulSet`'s pod-volume entry and (optionally) its
/// `volumeClaimTemplates` based on the pool's `Storage` setting.
/// Returns `(pod_volumes_json, volume_claim_templates_json_or_none)`.
///
/// For `PersistentClaim` the returned `volumes` array is empty (no
/// `data` entry): the `StatefulSet` controller mounts the PVC into the
/// pod under the same name as the template automatically, so an
/// explicit pod-volume entry would conflict.
fn render_storage(
    storage: Option<&Storage>,
    pod_labels: &BTreeMap<String, String>,
) -> (serde_json::Value, Option<serde_json::Value>) {
    match storage {
        None | Some(Storage::Ephemeral) => {
            let volumes = json!([{ "name": "data", "emptyDir": {} }]);
            (volumes, None)
        }
        Some(Storage::PersistentClaim(pc)) => {
            let mut template = json!({
                "metadata": {
                    "name": "data",
                    "labels": pod_labels,
                },
                "spec": {
                    "accessModes": ["ReadWriteOnce"],
                    "resources": {
                        "requests": { "storage": pc.size }
                    }
                }
            });
            if let Some(class) = pc.class.as_ref() {
                template["spec"]["storageClassName"] = serde_json::Value::String(class.clone());
            }
            (json!([]), Some(template))
        }
    }
}

/// Build the `StatefulSet`'s `persistentVolumeClaimRetentionPolicy`
/// block when storage is `PersistentClaim`. Returns `None` for
/// `Ephemeral` (no PVCs to retain).
fn render_pvc_retention_policy(storage: Option<&Storage>) -> Option<serde_json::Value> {
    match storage {
        Some(Storage::PersistentClaim(pc)) => Some(json!({
            "whenDeleted": if pc.delete_claim { "Delete" } else { "Retain" },
            "whenScaled": "Retain",
        })),
        _ => None,
    }
}

/// Overwrite `pod_spec`'s `volumes` field with the rendered
/// `pod_volumes`. The `pod_spec` template already carries an emptyDir
/// `data` entry from the inline `json!` block; this replaces it so the
/// `PersistentClaim` path doesn't double-declare `data`.
fn pod_spec_with_data_volume(
    mut pod_spec: serde_json::Value,
    pod_volumes: serde_json::Value,
) -> serde_json::Value {
    pod_spec["volumes"] = pod_volumes;
    pod_spec
}

/// Render the `StatefulSet` for a pool. Naming: `<parent>-<pool>`,
/// served from the parent's shared headless `Service`
/// `<parent>-broker-headless`. Owner-ref points to the pool, not the
/// parent — `kubectl delete knp <pool>` deletes the `StatefulSet`
/// directly.
pub(crate) fn render_statefulset(
    parent: &Kafka,
    pool: &KafkaNodePool,
    broker_image: &str,
) -> Result<StatefulSet, ReconcileError> {
    let parent_name = parent.meta().name.clone().unwrap_or_default();
    let pool_name = pool.meta().name.clone().unwrap_or_default();
    let namespace = pool.meta().namespace.clone().unwrap_or_default();

    let labels = common_labels(&parent_name, &parent.spec.kafka_version, Some(&pool_name));
    // Pod selector must NOT include the version label (it would force
    // re-creation of the StatefulSet on every version bump) but it MUST
    // pin to the parent cluster + this specific pool so we don't capture
    // sibling pools' pods.
    let mut selector: BTreeMap<String, String> = BTreeMap::new();
    selector.insert("app.kubernetes.io/name".into(), APP_LABEL.into());
    selector.insert("app.kubernetes.io/instance".into(), parent_name.clone());
    selector.insert("crabka.io/pool".into(), pool_name.clone());

    let resources = pool
        .spec
        .resources
        .clone()
        .unwrap_or_else(default_resources);

    let secret_name = format!("{parent_name}-cluster-id");
    let service_name = format!("{parent_name}-broker-headless");
    let sts_name = format!("{parent_name}-{pool_name}");
    let advertised =
        format!("$(POD_NAME).{service_name}.$(POD_NAMESPACE).svc.cluster.local:{BROKER_PORT}");

    let init = render_init_container(broker_image, &secret_name, pool.spec.node_id_start);
    let main = render_broker_container(broker_image, &secret_name, &advertised, &resources);

    // Merge user-provided pod metadata under operator-owned labels.
    // Operator labels win collisions; user labels fill in the rest.
    let mut pod_labels = labels.clone();
    let mut pod_annotations: BTreeMap<String, String> = BTreeMap::new();
    if let Some(meta) = pool
        .spec
        .template
        .as_ref()
        .and_then(|t| t.metadata.as_ref())
    {
        for (k, v) in &meta.labels {
            pod_labels.entry(k.clone()).or_insert_with(|| v.clone());
        }
        for (k, v) in &meta.annotations {
            pod_annotations.insert(k.clone(), v.clone());
        }
    }

    // Operator-owned annotation: propagate `crabka.io/config-hash` from
    // the pool's metadata label (set by the Kafka reconciler) into the
    // pod-template annotation. Placed after the user-annotation merge so
    // the operator wins on a same-key collision — the hash is the
    // mechanism that triggers a rolling restart on config drift.
    if let Some(hash) = pool
        .metadata
        .labels
        .as_ref()
        .and_then(|l| l.get("crabka.io/config-hash"))
    {
        pod_annotations.insert("crabka.io/config-hash".into(), hash.clone());
    }

    let mut template_meta = json!({ "labels": pod_labels });
    if !pod_annotations.is_empty() {
        template_meta["annotations"] = serde_json::to_value(&pod_annotations)?;
    }

    let mut pod_spec = json!({
        "securityContext": {
            "runAsNonRoot": true,
            "runAsUser": 65532,
            "fsGroup": 65532,
            "seccompProfile": { "type": "RuntimeDefault" }
        },
        "initContainers": [init],
        "containers": [main],
        "volumes": [{ "name": "data", "emptyDir": {} }],
    });
    if let Some(tpl) = pool.spec.template.as_ref() {
        if let Some(affinity) = tpl.affinity.as_ref() {
            pod_spec["affinity"] = serde_json::to_value(affinity)?;
        }
        if !tpl.tolerations.is_empty() {
            pod_spec["tolerations"] = serde_json::to_value(&tpl.tolerations)?;
        }
        if let Some(ns) = tpl.node_selector.as_ref()
            && !ns.is_empty()
        {
            pod_spec["nodeSelector"] = serde_json::to_value(ns)?;
        }
    }

    let (pod_volumes, volume_claim_templates) =
        render_storage(pool.spec.storage.as_ref(), &pod_labels);
    let retention_policy = render_pvc_retention_policy(pool.spec.storage.as_ref());

    let mut sts_spec = json!({
        "serviceName": service_name,
        "replicas": pool.spec.replicas,
        "podManagementPolicy": "Parallel",
        "selector": { "matchLabels": selector },
        "template": {
            "metadata": template_meta,
            "spec": pod_spec_with_data_volume(pod_spec, pod_volumes),
        }
    });
    if let Some(vct) = volume_claim_templates {
        sts_spec["volumeClaimTemplates"] = json!([vct]);
    }
    if let Some(policy) = retention_policy {
        sts_spec["persistentVolumeClaimRetentionPolicy"] = policy;
    }

    let sts: StatefulSet = serde_json::from_value(json!({
        "metadata": {
            "name": sts_name,
            "namespace": namespace,
            "labels": labels,
            "ownerReferences": [owner_ref::<KafkaNodePool>(pool)?],
        },
        "spec": sts_spec,
    }))?;
    Ok(sts)
}

fn default_resources() -> ResourceRequirements {
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

/// Monotonic validation of `spec.storage` against the live
/// `StatefulSet`'s observed `volumeClaimTemplates`. Returns `Ok(())`
/// when no live `StatefulSet` exists (first reconcile — any spec is
/// acceptable) or when desired and observed agree on the immutable
/// fields.
///
/// Rejections (all map to `Ready=False, reason=StorageImmutable`):
/// - `Ephemeral ↔ PersistentClaim` switch.
/// - `class` change.
/// - `size` decrease.
///
/// `delete_claim` is *not* checked: the spec (§ 4) intentionally
/// classifies it as mutable, and the PVC template doesn't reflect it
/// (the retention policy lives on the `StatefulSet` spec). The
/// reconstructed observed `Storage` therefore hardcodes
/// `delete_claim: false` and this comparison never inspects that
/// field, so a flip from `false → true` (or vice versa) passes
/// through to the SSA-apply path.
fn validate_storage_change(
    desired: Option<&Storage>,
    observed_template: Option<&PersistentVolumeClaim>,
) -> Result<(), PoolValidationError> {
    let observed = observed_template.map(observed_storage_from_pvc_template);
    let desired_kind = storage_kind(desired);
    let observed_kind = observed.as_ref().map(|s| storage_kind(Some(s)));

    let Some(observed_kind) = observed_kind else {
        return Ok(());
    };

    if desired_kind != observed_kind {
        return Err(PoolValidationError::StorageTypeChanged {
            from: observed_kind,
            to: desired_kind,
        });
    }

    let (Some(Storage::PersistentClaim(desired_pc)), Some(Storage::PersistentClaim(observed_pc))) =
        (desired, observed.as_ref())
    else {
        return Ok(());
    };

    if desired_pc.class != observed_pc.class {
        return Err(PoolValidationError::StorageClassChanged {
            from: observed_pc.class.clone(),
            to: desired_pc.class.clone(),
        });
    }

    let observed_bytes = common::parse_quantity(&observed_pc.size).unwrap_or(0);
    let desired_bytes = common::parse_quantity(&desired_pc.size).unwrap_or(0);
    if desired_bytes < observed_bytes {
        return Err(PoolValidationError::StorageShrinkNotAllowed {
            current: observed_pc.size.clone(),
            desired: desired_pc.size.clone(),
        });
    }

    Ok(())
}

/// Reconstruct a `Storage` value from the live `StatefulSet`'s `data`
/// `volumeClaimTemplate`. `delete_claim` is set to `false` because the
/// retention policy is not reflected in the PVC template; the
/// monotonic validator never inspects this field (see
/// [`validate_storage_change`]).
fn observed_storage_from_pvc_template(pvc: &PersistentVolumeClaim) -> Storage {
    let Some(spec) = pvc.spec.as_ref() else {
        return Storage::Ephemeral;
    };
    let size = spec
        .resources
        .as_ref()
        .and_then(|r| r.requests.as_ref())
        .and_then(|m| m.get("storage"))
        .map(|q| q.0.clone())
        .unwrap_or_default();
    let class = spec.storage_class_name.clone();
    Storage::PersistentClaim(PersistentClaimSpec {
        size,
        class,
        delete_claim: false,
    })
}

fn storage_kind(s: Option<&Storage>) -> &'static str {
    match s {
        None | Some(Storage::Ephemeral) => "Ephemeral",
        Some(Storage::PersistentClaim(_)) => "PersistentClaim",
    }
}

/// Map a `PoolValidationError` to a `Ready=False` condition with a
/// distinct `reason`. Reason strings are the contract that admins
/// (and the e2e tests) match on.
fn condition_for_validation_error(err: &PoolValidationError) -> KafkaCondition {
    let (reason, message) = match err {
        PoolValidationError::RolesNotMixed(roles) => (
            "RolesNotMixed",
            format!("spec.roles must equal {{Controller, Broker}}; got {roles:?}"),
        ),
        PoolValidationError::ReplicasNotOne(n) => (
            "UnsupportedReplicaCount",
            format!("spec.replicas={n} is unsupported in slice 20 (only 1 allowed)"),
        ),
        PoolValidationError::NodeIdOutOfRange(n) => (
            "NodeIdOutOfRange",
            format!("spec.nodeIdStart={n} is out of range 0..=999999"),
        ),
        PoolValidationError::MissingClusterLabel => (
            "MissingClusterLabel",
            "metadata.labels.\"crabka.io/cluster\" missing".to_string(),
        ),
        PoolValidationError::StorageSizeInvalid(value, why) => (
            "StorageSizeInvalid",
            format!("spec.storage.size={value:?} ({why})"),
        ),
        PoolValidationError::StorageTypeChanged { from, to } => (
            "StorageImmutable",
            format!("spec.storage.type changed from {from} to {to}"),
        ),
        PoolValidationError::StorageClassChanged { from, to } => (
            "StorageImmutable",
            format!("spec.storage.class changed from {from:?} to {to:?}"),
        ),
        PoolValidationError::StorageShrinkNotAllowed { current, desired } => (
            "StorageImmutable",
            format!("spec.storage.size {current} -> {desired} (shrink rejected)"),
        ),
    };
    condition("Ready", "False", reason, &message)
}

/// Wrap `common::patch_status` with the pool-specific status shape.
async fn patch_status_for_pool(
    pool_api: &Api<KafkaNodePool>,
    name: &str,
    cond: KafkaCondition,
) -> Result<(), ReconcileError> {
    let status = KafkaNodePoolStatus {
        conditions: vec![cond],
        replicas: None,
        ready_replicas: None,
    };
    common::patch_status::<KafkaNodePool, KafkaNodePoolStatus>(pool_api, name, status).await
}

/// Run the `KafkaNodePool` controller forever. Returns only on
/// irrecoverable stream error.
pub async fn run(ctx: Context) -> anyhow::Result<()> {
    let api: Api<KafkaNodePool> = Api::all(ctx.client.clone());
    let sts_api: Api<StatefulSet> = Api::all(ctx.client.clone());
    Controller::new(api, watcher::Config::default())
        .owns(sts_api, watcher::Config::default())
        .run(reconcile, error_policy, Arc::new(ctx))
        .for_each(|res| async move {
            match res {
                Ok((obj, _)) => tracing::debug!(?obj, "pool reconciled"),
                Err(e) => tracing::warn!(error = %e, "pool reconcile error"),
            }
        })
        .await;
    Ok(())
}

pub async fn reconcile(
    pool: Arc<KafkaNodePool>,
    ctx: Arc<Context>,
) -> Result<Action, ReconcileError> {
    let ns = pool.namespace().unwrap_or_else(|| "default".into());
    let name = pool.name_any();

    let pool_api: Api<KafkaNodePool> = Api::namespaced(ctx.client.clone(), &ns);

    // 1. Validate. On failure, patch a Ready=False condition and stop.
    if let Err(e) = validate(&pool) {
        let cond = condition_for_validation_error(&e);
        patch_status_for_pool(&pool_api, &name, cond).await?;
        return Ok(Action::await_change());
    }

    // 2. Look up the parent Kafka via the `crabka.io/cluster` label.
    let Some(kafka_name) = pool
        .meta()
        .labels
        .as_ref()
        .and_then(|l| l.get("crabka.io/cluster").cloned())
    else {
        let cond = condition(
            "Ready",
            "False",
            "MissingClusterLabel",
            "metadata.labels.\"crabka.io/cluster\" is required to link a pool to its parent Kafka",
        );
        patch_status_for_pool(&pool_api, &name, cond).await?;
        return Ok(Action::await_change());
    };

    let kafka_api: Api<Kafka> = Api::namespaced(ctx.client.clone(), &ns);
    let Some(parent) = kafka_api.get_opt(&kafka_name).await? else {
        let cond = condition(
            "Ready",
            "False",
            "ParentNotFound",
            &format!("Kafka '{kafka_name}' not found in namespace '{ns}'"),
        );
        patch_status_for_pool(&pool_api, &name, cond).await?;
        return Ok(Action::requeue(Duration::from_secs(30)));
    };

    // 3. Resolve broker image: spec override > operator default > built-in.
    let image = pool
        .spec
        .image
        .clone()
        .or_else(|| ctx.config.default_broker_image.clone())
        .unwrap_or_else(|| DEFAULT_BROKER_IMAGE.into());

    // 4. Pre-apply GET: capture the live StatefulSet (or None on first
    //    reconcile) so the monotonic-storage validator can compare
    //    desired spec.storage against the existing volumeClaimTemplates.
    let sts_api: Api<StatefulSet> = Api::namespaced(ctx.client.clone(), &ns);
    let sts_name = format!("{kafka_name}-{name}");
    let observed_sts = sts_api.get_opt(&sts_name).await?;
    let observed_pvc_template = observed_sts
        .as_ref()
        .and_then(|s| s.spec.as_ref())
        .and_then(|spec| spec.volume_claim_templates.as_ref())
        .and_then(|templates| {
            templates
                .iter()
                .find(|t| t.metadata.name.as_deref() == Some("data"))
        });

    if let Err(e) = validate_storage_change(pool.spec.storage.as_ref(), observed_pvc_template) {
        let cond = condition_for_validation_error(&e);
        patch_status_for_pool(&pool_api, &name, cond).await?;
        return Ok(Action::await_change());
    }

    // 5. Render + apply the StatefulSet.
    let sts = render_statefulset(&parent, &pool, &image)?;
    apply_object(&sts_api, &sts_name, &sts).await?;

    // 6. Read back live state and patch status.
    let live = sts_api.get_opt(&sts_name).await?;
    let (replicas, ready_replicas, reason, message) =
        derive_status(live.as_ref(), pool.spec.replicas);
    let status_value = if reason == "Available" {
        "True"
    } else {
        "False"
    };
    let status = KafkaNodePoolStatus {
        conditions: vec![condition("Ready", status_value, reason, &message)],
        replicas,
        ready_replicas,
    };
    common::patch_status::<KafkaNodePool, KafkaNodePoolStatus>(&pool_api, &name, status).await?;

    Ok(Action::requeue(Duration::from_secs(30)))
}

pub fn error_policy(_obj: Arc<KafkaNodePool>, err: &ReconcileError, _ctx: Arc<Context>) -> Action {
    tracing::warn!(error = %err, "pool reconcile error, requeueing");
    Action::requeue(Duration::from_secs(15))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{
        KafkaNodePoolSpec, KafkaSpec, MetadataTemplate, PersistentClaimSpec, PodTemplate, Storage,
    };
    use std::collections::BTreeMap;

    fn parent_fixture(name: &str) -> Kafka {
        let mut k = Kafka::new(
            name,
            KafkaSpec {
                kafka_version: "0.1.1".into(),
                config: None,
            },
        );
        k.metadata.namespace = Some("default".into());
        k.metadata.uid = Some("parent-u".into());
        k
    }

    fn pool_fixture(name: &str, parent: &str, replicas: i32) -> KafkaNodePool {
        let mut p = KafkaNodePool::new(
            name,
            KafkaNodePoolSpec {
                roles: vec![NodeRole::Controller, NodeRole::Broker],
                replicas,
                node_id_start: 0,
                image: None,
                resources: None,
                template: None,
                storage: None,
            },
        );
        p.metadata.namespace = Some("default".into());
        p.metadata.uid = Some("pool-u".into());
        let mut labels = BTreeMap::new();
        labels.insert("crabka.io/cluster".into(), parent.to_string());
        p.metadata.labels = Some(labels);
        p
    }

    #[test]
    fn render_statefulset_name_is_kafka_dash_pool() {
        let parent = parent_fixture("demo");
        let pool = pool_fixture("brokers", "demo", 1);
        let sts = render_statefulset(&parent, &pool, DEFAULT_BROKER_IMAGE).unwrap();
        assert_eq!(sts.metadata.name.as_deref(), Some("demo-brokers"));
    }

    #[test]
    fn render_statefulset_service_name_is_shared_headless() {
        let parent = parent_fixture("demo");
        let pool = pool_fixture("brokers", "demo", 1);
        let sts = render_statefulset(&parent, &pool, DEFAULT_BROKER_IMAGE).unwrap();
        let spec = sts.spec.expect("sts spec");
        assert_eq!(spec.service_name.as_deref(), Some("demo-broker-headless"));
    }

    #[test]
    fn render_statefulset_pod_labels_include_kafka_instance_and_pool_name() {
        let parent = parent_fixture("demo");
        let pool = pool_fixture("brokers", "demo", 1);
        let sts = render_statefulset(&parent, &pool, DEFAULT_BROKER_IMAGE).unwrap();
        let spec = sts.spec.expect("sts spec");
        let pod_labels = spec
            .template
            .metadata
            .as_ref()
            .and_then(|m| m.labels.as_ref())
            .expect("pod template labels");
        assert_eq!(
            pod_labels
                .get("app.kubernetes.io/instance")
                .map(String::as_str),
            Some("demo")
        );
        assert_eq!(
            pod_labels.get("crabka.io/pool").map(String::as_str),
            Some("brokers")
        );
    }

    #[test]
    fn render_statefulset_init_script_uses_nodeidstart() {
        let parent = parent_fixture("demo");
        let mut pool = pool_fixture("brokers", "demo", 1);
        pool.spec.node_id_start = 42;
        let sts = render_statefulset(&parent, &pool, DEFAULT_BROKER_IMAGE).unwrap();
        let pod = sts.spec.unwrap().template.spec.unwrap();
        let init = &pod.init_containers.expect("init containers")[0];

        // The literal env entry should carry the rendered start id.
        let env = init.env.as_ref().expect("init env");
        let node_id_start = env
            .iter()
            .find(|e| e.name == "NODE_ID_START")
            .expect("NODE_ID_START env");
        assert_eq!(node_id_start.value.as_deref(), Some("42"));

        // The shell script should combine NODE_ID_START + the pod ordinal.
        let args = init.args.as_ref().expect("init args");
        let script = args.iter().find(|s| s.contains("NODE_ID_START"));
        let script = script.expect("init script references NODE_ID_START");
        assert!(
            script.contains("NODE_ID_START + ORDINAL"),
            "expected the init script to compute NODE_ID = NODE_ID_START + ORDINAL, got: {script}"
        );
        // Regression: `crabka format` refuses to run when the log_dir
        // is non-empty. The init script must therefore write `.node-id`
        // *after* the format step, not before — otherwise the first
        // boot of an empty PVC fails with
        // "refusing to overwrite non-empty log_dir".
        let format_pos = script
            .find("crabka format")
            .expect("init script must invoke `crabka format`");
        let node_id_write_pos = script
            .find(".node-id")
            .expect("init script must write .node-id");
        assert!(
            node_id_write_pos > format_pos,
            "init script must write .node-id AFTER crabka format. \
             Otherwise `crabka format` refuses to overwrite a non-empty \
             log_dir on the first boot of an empty PVC. \
             format at byte {format_pos}, .node-id at byte {node_id_write_pos}",
        );
    }

    #[test]
    fn validate_rejects_replicas_two() {
        let pool = pool_fixture("brokers", "demo", 2);
        let err = validate(&pool).unwrap_err();
        assert!(
            matches!(err, PoolValidationError::ReplicasNotOne(2)),
            "expected ReplicasNotOne(2), got {err:?}"
        );
    }

    #[test]
    fn validate_rejects_controller_only_roles() {
        let mut pool = pool_fixture("brokers", "demo", 1);
        pool.spec.roles = vec![NodeRole::Controller];
        let err = validate(&pool).unwrap_err();
        assert!(
            matches!(err, PoolValidationError::RolesNotMixed(_)),
            "expected RolesNotMixed, got {err:?}"
        );
    }

    #[test]
    fn validate_rejects_broker_only_roles() {
        let mut pool = pool_fixture("brokers", "demo", 1);
        pool.spec.roles = vec![NodeRole::Broker];
        let err = validate(&pool).unwrap_err();
        assert!(
            matches!(err, PoolValidationError::RolesNotMixed(_)),
            "expected RolesNotMixed, got {err:?}"
        );
    }

    #[test]
    fn validate_rejects_negative_nodeidstart() {
        let mut pool = pool_fixture("brokers", "demo", 1);
        pool.spec.node_id_start = -1;
        let err = validate(&pool).unwrap_err();
        assert!(
            matches!(err, PoolValidationError::NodeIdOutOfRange(-1)),
            "expected NodeIdOutOfRange(-1), got {err:?}"
        );
    }

    fn pool_with_template(template: PodTemplate) -> KafkaNodePool {
        let mut pool = pool_fixture("brokers", "demo", 1);
        pool.spec.template = Some(template);
        pool
    }

    #[test]
    fn render_statefulset_template_labels_merge_under_operator_labels() {
        let mut user_labels = BTreeMap::new();
        user_labels.insert("team".into(), "platform".into());
        user_labels.insert("app.kubernetes.io/name".into(), "hijack".into());

        let pool = pool_with_template(PodTemplate {
            metadata: Some(MetadataTemplate {
                labels: user_labels,
                annotations: BTreeMap::new(),
            }),
            ..Default::default()
        });
        let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
        let pod_labels = sts.spec.unwrap().template.metadata.unwrap().labels.unwrap();
        assert_eq!(pod_labels.get("team").map(String::as_str), Some("platform"));
        // operator-managed name MUST win
        assert_eq!(
            pod_labels.get("app.kubernetes.io/name").map(String::as_str),
            Some(APP_LABEL)
        );
    }

    #[test]
    fn render_statefulset_template_annotations_apply() {
        let mut annos = BTreeMap::new();
        annos.insert("crabka.io/test-anno".into(), "yes".into());
        let pool = pool_with_template(PodTemplate {
            metadata: Some(MetadataTemplate {
                labels: BTreeMap::new(),
                annotations: annos,
            }),
            ..Default::default()
        });
        let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
        let anno = sts
            .spec
            .unwrap()
            .template
            .metadata
            .unwrap()
            .annotations
            .unwrap();
        assert_eq!(
            anno.get("crabka.io/test-anno").map(String::as_str),
            Some("yes")
        );
    }

    #[test]
    fn render_statefulset_affinity_passes_through() {
        use k8s_openapi::api::core::v1::{Affinity, NodeAffinity, NodeSelector, NodeSelectorTerm};
        let affinity = Affinity {
            node_affinity: Some(NodeAffinity {
                required_during_scheduling_ignored_during_execution: Some(NodeSelector {
                    node_selector_terms: vec![NodeSelectorTerm::default()],
                }),
                preferred_during_scheduling_ignored_during_execution: None,
            }),
            ..Default::default()
        };
        let pool = pool_with_template(PodTemplate {
            affinity: Some(affinity.clone()),
            ..Default::default()
        });
        let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
        let rendered = sts.spec.unwrap().template.spec.unwrap().affinity;
        assert_eq!(rendered, Some(affinity));
    }

    #[test]
    fn render_statefulset_tolerations_passes_through() {
        use k8s_openapi::api::core::v1::Toleration;
        let tol = Toleration {
            key: Some("dedicated".into()),
            operator: Some("Exists".into()),
            effect: Some("NoSchedule".into()),
            ..Default::default()
        };
        let pool = pool_with_template(PodTemplate {
            tolerations: vec![tol.clone()],
            ..Default::default()
        });
        let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
        let tols = sts
            .spec
            .unwrap()
            .template
            .spec
            .unwrap()
            .tolerations
            .unwrap();
        assert_eq!(tols, vec![tol]);
    }

    #[test]
    fn render_statefulset_node_selector_passes_through() {
        let mut ns = BTreeMap::new();
        ns.insert("disktype".into(), "ssd".into());
        let pool = pool_with_template(PodTemplate {
            node_selector: Some(ns.clone()),
            ..Default::default()
        });
        let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
        let rendered = sts
            .spec
            .unwrap()
            .template
            .spec
            .unwrap()
            .node_selector
            .unwrap();
        assert_eq!(rendered.get("disktype").map(String::as_str), Some("ssd"));
    }

    #[test]
    fn render_statefulset_no_template_no_extra_fields() {
        let pool = pool_fixture("brokers", "demo", 1);
        let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
        let spec = sts.spec.unwrap().template.spec.unwrap();
        assert!(spec.affinity.is_none());
        assert!(spec.tolerations.is_none() || spec.tolerations.as_ref().unwrap().is_empty());
        assert!(spec.node_selector.is_none() || spec.node_selector.as_ref().unwrap().is_empty());
    }

    #[test]
    fn render_statefulset_propagates_config_hash_from_label() {
        let mut pool = pool_fixture("brokers", "demo", 1);
        pool.metadata
            .labels
            .get_or_insert_with(BTreeMap::new)
            .insert("crabka.io/config-hash".into(), "abc123".into());
        let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
        let anno = sts
            .spec
            .unwrap()
            .template
            .metadata
            .unwrap()
            .annotations
            .unwrap();
        assert_eq!(
            anno.get("crabka.io/config-hash").map(String::as_str),
            Some("abc123")
        );
    }

    #[test]
    fn render_statefulset_no_config_hash_when_label_absent() {
        let pool = pool_fixture("brokers", "demo", 1);
        let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
        // Annotation map may be None or just lack our key — both fine.
        if let Some(anno) = sts.spec.unwrap().template.metadata.unwrap().annotations {
            assert!(!anno.contains_key("crabka.io/config-hash"));
        }
    }

    #[test]
    fn render_statefulset_emptydir_when_storage_none() {
        let pool = pool_fixture("brokers", "demo", 1);
        let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
        let spec = sts.spec.unwrap();
        assert!(
            spec.volume_claim_templates.is_none()
                || spec.volume_claim_templates.as_ref().unwrap().is_empty()
        );
        let volumes = spec.template.spec.unwrap().volumes.unwrap();
        let data_vol = volumes
            .iter()
            .find(|v| v.name == "data")
            .expect("data volume present");
        assert!(
            data_vol.empty_dir.is_some(),
            "data volume must be emptyDir; got {data_vol:?}"
        );
    }

    #[test]
    fn render_statefulset_emptydir_when_storage_ephemeral() {
        let mut pool = pool_fixture("brokers", "demo", 1);
        pool.spec.storage = Some(Storage::Ephemeral);
        let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
        let spec = sts.spec.unwrap();
        assert!(
            spec.volume_claim_templates.is_none()
                || spec.volume_claim_templates.as_ref().unwrap().is_empty()
        );
        let volumes = spec.template.spec.unwrap().volumes.unwrap();
        let data_vol = volumes.iter().find(|v| v.name == "data").unwrap();
        assert!(data_vol.empty_dir.is_some());
    }

    #[test]
    fn render_statefulset_volume_claim_template_when_persistent() {
        let mut pool = pool_fixture("brokers", "demo", 1);
        pool.spec.storage = Some(Storage::PersistentClaim(PersistentClaimSpec {
            size: "10Gi".into(),
            class: Some("fast-ssd".into()),
            delete_claim: false,
        }));
        let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
        let spec = sts.spec.unwrap();
        let volumes = spec.template.spec.as_ref().unwrap().volumes.as_ref();
        if let Some(vols) = volumes {
            assert!(
                vols.iter()
                    .all(|v| v.name != "data" || v.empty_dir.is_none()),
                "expected no emptyDir for data; got {vols:?}"
            );
        }
        let vct = spec.volume_claim_templates.unwrap();
        assert_eq!(vct.len(), 1);
        let data_pvc = &vct[0];
        assert_eq!(data_pvc.metadata.name.as_deref(), Some("data"));
        let pvc_spec = data_pvc.spec.as_ref().unwrap();
        assert_eq!(
            pvc_spec.access_modes.as_deref(),
            Some(["ReadWriteOnce".to_string()].as_slice())
        );
        let req = pvc_spec
            .resources
            .as_ref()
            .unwrap()
            .requests
            .as_ref()
            .unwrap();
        assert_eq!(req.get("storage").map(|q| q.0.as_str()), Some("10Gi"));
        assert_eq!(pvc_spec.storage_class_name.as_deref(), Some("fast-ssd"));
    }

    #[test]
    fn render_statefulset_no_storage_class_when_class_absent() {
        let mut pool = pool_fixture("brokers", "demo", 1);
        pool.spec.storage = Some(Storage::PersistentClaim(PersistentClaimSpec {
            size: "1Gi".into(),
            class: None,
            delete_claim: false,
        }));
        let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
        let pvc_spec = sts.spec.unwrap().volume_claim_templates.unwrap()[0]
            .spec
            .clone()
            .unwrap();
        assert!(
            pvc_spec.storage_class_name.is_none(),
            "must omit storageClassName when class is None"
        );
    }

    #[test]
    fn render_statefulset_pvc_labels_inherit_pod_labels() {
        let mut pool = pool_fixture("brokers", "demo", 1);
        pool.spec.storage = Some(Storage::PersistentClaim(PersistentClaimSpec {
            size: "1Gi".into(),
            class: None,
            delete_claim: false,
        }));
        let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
        let labels = sts.spec.unwrap().volume_claim_templates.unwrap()[0]
            .metadata
            .labels
            .clone()
            .expect("PVC has labels");
        assert_eq!(
            labels.get("app.kubernetes.io/instance").map(String::as_str),
            Some("demo")
        );
        assert_eq!(
            labels.get("crabka.io/pool").map(String::as_str),
            Some("brokers")
        );
    }

    #[test]
    fn render_statefulset_retention_policy_delete_when_delete_claim_true() {
        let mut pool = pool_fixture("brokers", "demo", 1);
        pool.spec.storage = Some(Storage::PersistentClaim(PersistentClaimSpec {
            size: "1Gi".into(),
            class: None,
            delete_claim: true,
        }));
        let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
        let policy = sts
            .spec
            .unwrap()
            .persistent_volume_claim_retention_policy
            .unwrap();
        assert_eq!(policy.when_deleted.as_deref(), Some("Delete"));
        assert_eq!(policy.when_scaled.as_deref(), Some("Retain"));
    }

    #[test]
    fn render_statefulset_retention_policy_retain_when_delete_claim_false() {
        let mut pool = pool_fixture("brokers", "demo", 1);
        pool.spec.storage = Some(Storage::PersistentClaim(PersistentClaimSpec {
            size: "1Gi".into(),
            class: None,
            delete_claim: false,
        }));
        let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
        let policy = sts
            .spec
            .unwrap()
            .persistent_volume_claim_retention_policy
            .unwrap();
        assert_eq!(policy.when_deleted.as_deref(), Some("Retain"));
        assert_eq!(policy.when_scaled.as_deref(), Some("Retain"));
    }

    #[test]
    fn render_statefulset_no_retention_policy_when_ephemeral() {
        let pool = pool_fixture("brokers", "demo", 1);
        let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
        assert!(
            sts.spec
                .unwrap()
                .persistent_volume_claim_retention_policy
                .is_none()
        );
    }

    fn pvc_template(size: &str, class: Option<&str>) -> PersistentVolumeClaim {
        let mut map = std::collections::BTreeMap::new();
        map.insert(
            "storage".to_string(),
            k8s_openapi::apimachinery::pkg::api::resource::Quantity(size.into()),
        );
        PersistentVolumeClaim {
            metadata: kube::core::ObjectMeta {
                name: Some("data".into()),
                ..Default::default()
            },
            spec: Some(k8s_openapi::api::core::v1::PersistentVolumeClaimSpec {
                access_modes: Some(vec!["ReadWriteOnce".into()]),
                resources: Some(k8s_openapi::api::core::v1::VolumeResourceRequirements {
                    requests: Some(map),
                    ..Default::default()
                }),
                storage_class_name: class.map(String::from),
                ..Default::default()
            }),
            status: None,
        }
    }

    fn pc(size: &str, class: Option<&str>) -> Storage {
        Storage::PersistentClaim(PersistentClaimSpec {
            size: size.into(),
            class: class.map(String::from),
            delete_claim: false,
        })
    }

    #[test]
    fn validate_storage_change_first_reconcile_accepts_any() {
        assert!(validate_storage_change(None, None).is_ok());
        assert!(validate_storage_change(Some(&Storage::Ephemeral), None).is_ok());
        assert!(validate_storage_change(Some(&pc("10Gi", None)), None).is_ok());
    }

    #[test]
    fn validate_storage_change_rejects_type_switch() {
        let observed = pvc_template("10Gi", None);
        let err = validate_storage_change(Some(&Storage::Ephemeral), Some(&observed)).unwrap_err();
        assert!(matches!(
            err,
            PoolValidationError::StorageTypeChanged { .. }
        ));
    }

    #[test]
    fn validate_storage_change_rejects_class_change() {
        let observed = pvc_template("10Gi", Some("class-a"));
        let err = validate_storage_change(Some(&pc("10Gi", Some("class-b"))), Some(&observed))
            .unwrap_err();
        assert!(matches!(
            err,
            PoolValidationError::StorageClassChanged { .. }
        ));
    }

    #[test]
    fn validate_storage_change_rejects_shrink() {
        let observed = pvc_template("10Gi", None);
        let err = validate_storage_change(Some(&pc("5Gi", None)), Some(&observed)).unwrap_err();
        assert!(matches!(
            err,
            PoolValidationError::StorageShrinkNotAllowed { .. }
        ));
    }

    #[test]
    fn validate_storage_change_allows_grow() {
        let observed = pvc_template("10Gi", None);
        assert!(validate_storage_change(Some(&pc("20Gi", None)), Some(&observed)).is_ok());
    }

    #[test]
    fn validate_storage_change_allows_delete_claim_flip() {
        let observed = pvc_template("10Gi", None);
        let mut desired = pc("10Gi", None);
        if let Storage::PersistentClaim(ref mut p) = desired {
            p.delete_claim = true;
        }
        assert!(validate_storage_change(Some(&desired), Some(&observed)).is_ok());
    }

    #[test]
    fn validate_static_rejects_unparseable_size() {
        let mut pool = pool_fixture("brokers", "demo", 1);
        pool.spec.storage = Some(pc("banana", None));
        let err = validate(&pool).unwrap_err();
        assert!(matches!(err, PoolValidationError::StorageSizeInvalid(_, _)));
    }
}
