//! Kafka CRD reconciler.
//!
//! Slice 20: `Kafka` is a parent/coordinator. It owns the cluster-level
//! `Service`, `ConfigMap`, and cluster-id `Secret`. Broker
//! `StatefulSet`s live on sibling `KafkaNodePool`s (one per pool, owned
//! by the pool). The `Kafka` reconciler aggregates per-pool status and
//! surfaces a cluster-level `Ready` condition.
//!
//! Per-pool status is rolled up by summing `replicas` and
//! `readyReplicas` across every `KafkaNodePool` labeled
//! `crabka.io/cluster=<this name>`. The `Ready` condition follows the
//! rule:
//! - no pools           -> `Ready=False`, reason `NoNodePools`
//! - all ready          -> `Ready=True`,  reason `Available`
//! - otherwise          -> `Ready=False`, reason `PartiallyReady`

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt as _;
use k8s_openapi::api::core::v1::{ConfigMap, Secret, Service};
use kube::api::{Api, ListParams, Patch, PatchParams};
use kube::runtime::controller::{Action, Controller};
use kube::runtime::reflector::ObjectRef;
use kube::runtime::watcher;
use kube::{Resource, ResourceExt as _};
use serde_json::json;

use crate::context::Context;
use crate::controller::common::{
    self, FIELD_MANAGER, ReconcileError, apply_object, condition, ensure_cluster_id_secret,
    owner_ref, patch_status, render_service,
};
use crate::crd::{Kafka, KafkaNodePool, KafkaStatus};

/// Rolled-up view of a cluster's pools. Computed by
/// `aggregate_pool_status` and consumed by `rollup_condition`.
pub(crate) struct ClusterRollup {
    pub replicas: i32,
    pub ready_replicas: i32,
    pub pool_count: usize,
}

/// Sum `replicas` and `readyReplicas` across every pool, counting how
/// many pools we saw. A pool with no status yet contributes zero to
/// both totals but still increments `pool_count` — so a freshly-created
/// pool surfaces as `PartiallyReady` rather than `NoNodePools`.
pub(crate) fn aggregate_pool_status<'a>(
    pools: impl IntoIterator<Item = &'a KafkaNodePool>,
) -> ClusterRollup {
    let mut r = ClusterRollup {
        replicas: 0,
        ready_replicas: 0,
        pool_count: 0,
    };
    for pool in pools {
        r.pool_count += 1;
        let s = pool.status.as_ref();
        r.replicas += s.and_then(|s| s.replicas).unwrap_or(0);
        r.ready_replicas += s.and_then(|s| s.ready_replicas).unwrap_or(0);
    }
    r
}

/// Translate a rollup into `(rolling, reason, message)` for the cluster
/// `Rolling` condition. `Rolling=True` is surfaced whenever at least one
/// pool exists and not all brokers have reached Ready — covers both
/// initial bring-up and config-drift-triggered restarts (which we can't
/// distinguish from the rollup alone).
pub(crate) fn rolling_condition_from_rollup(
    rollup: &ClusterRollup,
) -> (bool, &'static str, String) {
    if rollup.pool_count > 0 && rollup.ready_replicas < rollup.replicas {
        (
            true,
            "RollingUpdate",
            format!(
                "{}/{} brokers ready (roll in progress)",
                rollup.ready_replicas, rollup.replicas
            ),
        )
    } else {
        (
            false,
            "Stable",
            "all brokers on current revision".to_string(),
        )
    }
}

/// Translate a rollup into `(ready, reason, message)` for the cluster
/// `Ready` condition. The three branches are the contract that admins
/// (and the e2e tests) match on.
pub(crate) fn rollup_condition(rollup: &ClusterRollup) -> (bool, &'static str, String) {
    if rollup.pool_count == 0 {
        (
            false,
            "NoNodePools",
            "no KafkaNodePool with label crabka.io/cluster=<name>".into(),
        )
    } else if rollup.ready_replicas == rollup.replicas && rollup.replicas > 0 {
        (
            true,
            "Available",
            format!(
                "{}/{} brokers ready across {} pool(s)",
                rollup.ready_replicas, rollup.replicas, rollup.pool_count
            ),
        )
    } else {
        (
            false,
            "PartiallyReady",
            format!(
                "{}/{} brokers ready",
                rollup.ready_replicas, rollup.replicas
            ),
        )
    }
}

/// Run the `Kafka` controller forever. Returns only on irrecoverable
/// stream error (the kube-rs `Controller` re-establishes watches on
/// recoverable errors internally).
///
/// Watches `KafkaNodePool` so a pool status change wakes its parent's
/// reconcile. The mapper resolves the parent name via the
/// `crabka.io/cluster` label and the namespace from the pool itself.
pub async fn run(ctx: Context) -> anyhow::Result<()> {
    let api: Api<Kafka> = Api::all(ctx.client.clone());
    let pools: Api<KafkaNodePool> = Api::all(ctx.client.clone());
    Controller::new(api, watcher::Config::default())
        .watches(pools, watcher::Config::default(), |pool| {
            let ns = pool.meta().namespace.clone();
            let kafka_name = pool
                .meta()
                .labels
                .as_ref()
                .and_then(|l| l.get("crabka.io/cluster").cloned());
            match (kafka_name, ns) {
                (Some(name), Some(ns)) => Some(ObjectRef::<Kafka>::new(&name).within(&ns)),
                _ => None,
            }
            .into_iter()
        })
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

    // 1. Cluster-level Service + ConfigMap via SSA. Names are derived
    //    inside the renderers; we mirror them here for the api.patch
    //    target.
    let svc_api: Api<Service> = Api::namespaced(ctx.client.clone(), &ns);
    let svc = render_service(&obj)?;
    apply_object(&svc_api, &svc_name(&name), &svc).await?;

    // Slice 25/7 transitional: ConfigMap is rendered with empty per-broker
    // data until Task 25/9 wires the full reconcile flow. The CM keys
    // disappear from the cluster on first reconcile; pods can't start until
    // Task 25/9 populates the per-broker TOML.
    let cm_api: Api<ConfigMap> = Api::namespaced(ctx.client.clone(), &ns);
    let cm = common::render_configmap(
        &obj,
        &[],                                // listeners — empty until Task 25/9 wires reconcile
        &std::collections::BTreeMap::new(), // addresses_per_broker — empty until Task 25/9
        "PLAIN", // inter_broker_listener_name — placeholder until Task 25/9
    )?;
    apply_object(&cm_api, &cm_name(&name), &cm).await?;

    // Compute the content hash. Task 25/8 will update this to use
    // combined_config_hash with listener intent; for now we hash the
    // raw spec.config string as before so the rolling-restart label
    // continues to function.
    let broker_props = obj.spec.config.as_ref().map_or_else(String::new, |cfg| {
        // inline: sorted key=value lines (same output as the removed
        // serialize_broker_properties helper, kept here until Task 25/8)
        cfg.iter().fold(String::new(), |mut s, (k, v)| {
            s.push_str(k);
            s.push('=');
            s.push_str(v);
            s.push('\n');
            s
        })
    });
    let cfg_hash = common::config_hash(&broker_props);

    // 2. Cluster-id Secret: one-shot create-if-missing. The pool
    //    reconciler reads this secret to inject CRABKA_CLUSTER_ID into
    //    broker pods.
    let secret_api: Api<Secret> = Api::namespaced(ctx.client.clone(), &ns);
    let _cluster_id = ensure_cluster_id_secret(&secret_api, &obj).await?;

    // 3. List sibling pools by label.
    let pool_api: Api<KafkaNodePool> = Api::namespaced(ctx.client.clone(), &ns);
    let lp = ListParams::default().labels(&format!("crabka.io/cluster={name}"));
    let pools = pool_api.list(&lp).await?;

    // 3b. Adopt sibling pools: patch each pool's `metadata.ownerReferences`
    //     to include this Kafka as the controlling owner, and stamp the
    //     `crabka.io/config-hash` label so the pool reconciler can
    //     propagate it into the broker pod template (which forces a
    //     StatefulSet rolling restart on drift). Idempotent — Kubernetes
    //     server-side resolves the patch to a no-op when the fields
    //     already match. The owner-ref drives Kubernetes' built-in GC.
    adopt_pools(&pool_api, &obj, pools.iter(), &cfg_hash).await?;

    // 4. Aggregate + patch our own status. Surface both a `Ready`
    //    condition (existing slice-20 contract) and a `Rolling`
    //    condition (slice 21) so admins can distinguish "broker down"
    //    from "rolling restart in progress".
    let rollup = aggregate_pool_status(pools.iter());
    let (ready, reason, message) = rollup_condition(&rollup);
    let (rolling, rolling_reason, rolling_message) = rolling_condition_from_rollup(&rollup);
    let status = KafkaStatus {
        conditions: vec![
            condition(
                "Ready",
                if ready { "True" } else { "False" },
                reason,
                &message,
            ),
            condition(
                "Rolling",
                if rolling { "True" } else { "False" },
                rolling_reason,
                &rolling_message,
            ),
        ],
        replicas: Some(rollup.replicas),
        ready_replicas: Some(rollup.ready_replicas),
        listeners: vec![],
    };
    let kafka_api: Api<Kafka> = Api::namespaced(ctx.client.clone(), &ns);
    patch_status::<Kafka, KafkaStatus>(&kafka_api, &name, status).await?;

    Ok(Action::requeue(Duration::from_secs(30)))
}

/// For every pool labeled `crabka.io/cluster=<this Kafka>`, patch
/// `metadata.ownerReferences` so the Kafka is the controlling owner
/// AND `metadata.labels["crabka.io/config-hash"]` so the pool reconciler
/// observes config drift. Uses a server-side apply with the operator's
/// field manager so the patch wins over any out-of-band manual edits.
async fn adopt_pools<'a>(
    pool_api: &Api<KafkaNodePool>,
    parent: &Kafka,
    pools: impl IntoIterator<Item = &'a KafkaNodePool>,
    config_hash: &str,
) -> Result<(), ReconcileError> {
    let owner = owner_ref::<Kafka>(parent)?;
    let params = PatchParams {
        field_manager: Some(FIELD_MANAGER.into()),
        force: true,
        ..Default::default()
    };
    // SSA needs apiVersion + kind on the patch payload. The patch
    // *target* is a KafkaNodePool, so the payload's apiVersion/kind
    // match the pool, not the parent Kafka.
    let patch_body = json!({
        "apiVersion": KafkaNodePool::api_version(&()),
        "kind": KafkaNodePool::kind(&()),
        "metadata": {
            "ownerReferences": [owner],
            "labels": { "crabka.io/config-hash": config_hash },
        }
    });
    for pool in pools {
        let pool_name = pool.name_any();
        pool_api
            .patch(&pool_name, &params, &Patch::Apply(&patch_body))
            .await?;
    }
    Ok(())
}

pub fn error_policy(_obj: Arc<Kafka>, err: &ReconcileError, _ctx: Arc<Context>) -> Action {
    tracing::warn!(error = %err, "reconcile error, requeueing");
    Action::requeue(Duration::from_secs(15))
}

fn svc_name(kafka: &str) -> String {
    format!("{kafka}-broker-headless")
}

fn cm_name(kafka: &str) -> String {
    format!("{kafka}-broker-config")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{KafkaNodePoolSpec, KafkaNodePoolStatus, NodeRole};

    fn pool_with_status(name: &str, replicas: i32, ready: i32) -> KafkaNodePool {
        let mut p = KafkaNodePool::new(
            name,
            KafkaNodePoolSpec {
                roles: vec![NodeRole::Controller, NodeRole::Broker],
                replicas: 1,
                node_id_start: 0,
                image: None,
                resources: None,
                template: None,
                storage: None,
            },
        );
        p.status = Some(KafkaNodePoolStatus {
            conditions: vec![],
            replicas: Some(replicas),
            ready_replicas: Some(ready),
        });
        p
    }

    #[test]
    fn aggregate_status_no_pools_is_no_node_pools() {
        let r = aggregate_pool_status(std::iter::empty::<&KafkaNodePool>());
        let (ready, reason, _) = rollup_condition(&r);
        assert!(!ready);
        assert_eq!(reason, "NoNodePools");
    }

    #[test]
    fn aggregate_status_partial_pool_is_partially_ready() {
        let p = pool_with_status("brokers", 3, 1);
        let r = aggregate_pool_status([&p]);
        let (ready, reason, _) = rollup_condition(&r);
        assert!(!ready);
        assert_eq!(reason, "PartiallyReady");
    }

    #[test]
    fn aggregate_status_all_ready_pools_is_available() {
        let p = pool_with_status("brokers", 1, 1);
        let r = aggregate_pool_status([&p]);
        let (ready, reason, _) = rollup_condition(&r);
        assert!(ready);
        assert_eq!(reason, "Available");
    }

    #[test]
    fn rolling_condition_when_pool_partial() {
        let r = ClusterRollup {
            replicas: 3,
            ready_replicas: 1,
            pool_count: 1,
        };
        let (rolling, reason, _) = rolling_condition_from_rollup(&r);
        assert!(rolling);
        assert_eq!(reason, "RollingUpdate");
    }

    #[test]
    fn rolling_condition_when_pool_stable() {
        let r = ClusterRollup {
            replicas: 1,
            ready_replicas: 1,
            pool_count: 1,
        };
        let (rolling, reason, _) = rolling_condition_from_rollup(&r);
        assert!(!rolling);
        assert_eq!(reason, "Stable");
    }
}
