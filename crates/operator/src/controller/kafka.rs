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
use kube::api::{Api, ListParams};
use kube::runtime::controller::{Action, Controller};
use kube::runtime::reflector::ObjectRef;
use kube::runtime::watcher;
use kube::{Resource, ResourceExt as _};

use crate::context::Context;
use crate::controller::common::{
    ReconcileError, apply_object, condition, ensure_cluster_id_secret, patch_status,
    render_configmap, render_service,
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

    let cm_api: Api<ConfigMap> = Api::namespaced(ctx.client.clone(), &ns);
    let cm = render_configmap(&obj)?;
    apply_object(&cm_api, &cm_name(&name), &cm).await?;

    // 2. Cluster-id Secret: one-shot create-if-missing. The pool
    //    reconciler reads this secret to inject CRABKA_CLUSTER_ID into
    //    broker pods.
    let secret_api: Api<Secret> = Api::namespaced(ctx.client.clone(), &ns);
    let _cluster_id = ensure_cluster_id_secret(&secret_api, &obj).await?;

    // 3. List sibling pools by label.
    let pool_api: Api<KafkaNodePool> = Api::namespaced(ctx.client.clone(), &ns);
    let lp = ListParams::default().labels(&format!("crabka.io/cluster={name}"));
    let pools = pool_api.list(&lp).await?;

    // 4. Aggregate + patch our own status.
    let rollup = aggregate_pool_status(pools.iter());
    let (ready, reason, message) = rollup_condition(&rollup);
    let status = KafkaStatus {
        conditions: vec![condition(
            "Ready",
            if ready { "True" } else { "False" },
            reason,
            &message,
        )],
        replicas: Some(rollup.replicas),
        ready_replicas: Some(rollup.ready_replicas),
    };
    let kafka_api: Api<Kafka> = Api::namespaced(ctx.client.clone(), &ns);
    patch_status::<Kafka, KafkaStatus>(&kafka_api, &name, status).await?;

    Ok(Action::requeue(Duration::from_secs(30)))
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
}
