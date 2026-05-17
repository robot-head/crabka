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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt as _;
use k8s_openapi::api::core::v1::{ConfigMap, Node, Pod, Secret, Service};
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
use crate::controller::listeners::{
    self, AdvertisedAddress, compute_advertised, effective_inter_broker_listener_name,
    render_bootstrap_service, render_broker_service, synthesized_default_listener,
    validate_listeners,
};
use crate::crd::{
    Kafka, KafkaNodePool, KafkaStatus, Listener, ListenerAddress, ListenerStatus, ListenerType,
};

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
///
/// Also watches `Node` (cluster-scoped) so that `ExternalIP` changes
/// (relevant for `NodePort` listeners) eventually trigger a reconcile.
/// The mapper returns empty — the periodic requeue (30 s) picks up the
/// change rather than enqueuing every Kafka on every Node event.
pub async fn run(ctx: Context) -> anyhow::Result<()> {
    let api: Api<Kafka> = Api::all(ctx.client.clone());
    let pools: Api<KafkaNodePool> = Api::all(ctx.client.clone());
    let nodes: Api<Node> = Api::all(ctx.client.clone());
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
        // Slice 25/11: Node changes (e.g. ExternalIP added/removed) may
        // invalidate a Kafka's advertised-listener TOML for NodePort
        // listeners. We return empty here and rely on the periodic requeue
        // to pick up the change, avoiding a flood of reconciles on large
        // clusters where every node churn fires 100 events.
        .watches(nodes, watcher::Config::default(), |_node| {
            Vec::<ObjectRef<Kafka>>::new().into_iter()
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

/// Identifier triple for one broker, derived from a `KafkaNodePool`.
/// `pod_fqdn` is the stable in-cluster DNS name (cluster headless
/// Service subdomain) — same string whether the pod is scheduled or
/// not, so internal listeners can advertise it before any pod exists.
#[derive(Debug, Clone)]
pub(crate) struct BrokerInfo {
    pub broker_id: i32,
    pub pod_name: String,
    pub pod_fqdn: String,
}

/// Walk pools (in caller-provided order) and emit one `BrokerInfo` per
/// pool. Slice-20 enforces `replicas == 1`, so each pool maps to
/// exactly one broker whose id is the pool's `nodeIdStart`.
pub(crate) fn enumerate_brokers(
    cluster_name: &str,
    namespace: &str,
    pools: &[KafkaNodePool],
) -> Vec<BrokerInfo> {
    let svc = format!("{cluster_name}-broker-headless");
    let mut out = Vec::with_capacity(pools.len());
    let mut sorted: Vec<&KafkaNodePool> = pools.iter().collect();
    sorted.sort_by_key(|p| p.name_any());
    for pool in sorted {
        let pool_name = pool.name_any();
        let pod_name = format!("{cluster_name}-{pool_name}-0");
        let pod_fqdn = format!("{pod_name}.{svc}.{namespace}.svc.cluster.local");
        out.push(BrokerInfo {
            broker_id: pool.spec.node_id_start,
            pod_name,
            pod_fqdn,
        });
    }
    out
}

/// Build the per-listener `ListenerStatus` entries. Internal listeners
/// surface the headless Service FQDN; external listeners pull a
/// bootstrap host:port from the apiserver-returned bootstrap Service.
/// Returns only entries whose addresses successfully resolved — a
/// listener still pending external infra is omitted.
pub(crate) fn build_listener_status(
    effective_listeners: &[Listener],
    addresses_per_broker: &BTreeMap<i32, BTreeMap<String, AdvertisedAddress>>,
    bootstrap_services: &HashMap<String, Service>,
    nodes: &HashMap<String, Node>,
    cluster_name: &str,
    namespace: &str,
) -> Vec<ListenerStatus> {
    let mut out = Vec::new();
    for l in effective_listeners {
        let mut addresses: Vec<ListenerAddress> = addresses_per_broker
            .values()
            .filter_map(|m| m.get(&l.name))
            .map(|a| ListenerAddress {
                host: a.host.clone(),
                port: a.port,
            })
            .collect();
        addresses.sort_by(|a, b| a.host.cmp(&b.host).then(a.port.cmp(&b.port)));

        let bootstrap =
            resolve_bootstrap_servers(l, bootstrap_services, nodes, cluster_name, namespace);
        if let Some(bootstrap_servers) = bootstrap {
            out.push(ListenerStatus {
                name: l.name.clone(),
                type_: l.type_,
                bootstrap_servers,
                addresses,
            });
        }
    }
    out
}

/// Inner helper for [`build_listener_status`]. Splits the per-listener
/// bootstrap-address derivation out so the body can `?`-chain through
/// the Option-returning apiserver lookups.
fn resolve_bootstrap_servers(
    listener: &Listener,
    bootstrap_services: &HashMap<String, Service>,
    nodes: &HashMap<String, Node>,
    cluster_name: &str,
    namespace: &str,
) -> Option<String> {
    match listener.type_ {
        ListenerType::Internal => Some(format!(
            "{cluster_name}-broker-headless.{namespace}.svc.cluster.local:{}",
            listener.port
        )),
        ListenerType::Nodeport => {
            let svc_name = format!("{cluster_name}-{}-bootstrap", listener.name);
            let svc = bootstrap_services.get(&svc_name)?;
            let node_port = svc
                .spec
                .as_ref()
                .and_then(|s| s.ports.as_ref())
                .and_then(|ps| ps.first())
                .and_then(|p| p.node_port)?;
            // Pick any node address — prefer ExternalIP, fall back to
            // InternalIP. Clients re-resolve via the per-broker list.
            let host = nodes.values().find_map(|n| {
                let addrs = n.status.as_ref().and_then(|s| s.addresses.as_ref())?;
                addrs
                    .iter()
                    .find(|a| a.type_ == "ExternalIP")
                    .or_else(|| addrs.iter().find(|a| a.type_ == "InternalIP"))
                    .map(|a| a.address.clone())
            })?;
            Some(format!("{host}:{node_port}"))
        }
        ListenerType::Loadbalancer => {
            let svc_name = format!("{cluster_name}-{}-bootstrap", listener.name);
            let svc = bootstrap_services.get(&svc_name)?;
            let ingress = svc
                .status
                .as_ref()
                .and_then(|st| st.load_balancer.as_ref())
                .and_then(|lb| lb.ingress.as_ref())
                .and_then(|ig| ig.first())?;
            let host = ingress.hostname.clone().or_else(|| ingress.ip.clone())?;
            Some(format!("{host}:{}", listener.port))
        }
        ListenerType::Ingress | ListenerType::Route => None,
    }
}

/// Apply both the bootstrap and per-broker Services for each external
/// (`nodeport` / `loadbalancer`) listener. Internal listeners don't
/// need any Service objects beyond the cluster-wide headless one.
async fn apply_external_services(
    svc_api: &Api<Service>,
    owner: &Kafka,
    cluster_name: &str,
    effective_listeners: &[Listener],
    brokers: &[BrokerInfo],
) -> Result<(), ReconcileError> {
    for l in effective_listeners
        .iter()
        .filter(|l| matches!(l.type_, ListenerType::Nodeport | ListenerType::Loadbalancer))
    {
        let bs = render_bootstrap_service(owner, l)?;
        let bs_name = format!("{cluster_name}-{}-bootstrap", l.name);
        apply_object(svc_api, &bs_name, &bs).await?;
        for b in brokers {
            let per = render_broker_service(owner, l, b.broker_id, &b.pod_name)?;
            let per_name = format!("{cluster_name}-{}-{}", l.name, b.broker_id);
            apply_object(svc_api, &per_name, &per).await?;
        }
    }
    Ok(())
}

/// Read back the cluster Nodes, broker Pods, and per-listener Services
/// the operator just applied. Returned as three `HashMap`s plus the
/// pod-by-name lookup the address resolver needs.
///
/// Returning `Ok(default)` rather than failing on any individual GET's
/// 404 is intentional: a Pod that hasn't been created yet is not an
/// error — it surfaces as `PodNotScheduled` in `compute_advertised`.
#[allow(clippy::type_complexity)]
async fn read_external_state(
    ctx: &Context,
    svc_api: &Api<Service>,
    namespace: &str,
    cluster_name: &str,
    effective_listeners: &[Listener],
    brokers: &[BrokerInfo],
) -> Result<
    (
        HashMap<String, Node>,
        HashMap<String, Pod>,
        HashMap<String, Service>,
        HashMap<(String, i32), Service>,
    ),
    ReconcileError,
> {
    let node_api: Api<Node> = Api::all(ctx.client.clone());
    let mut nodes = HashMap::new();
    for n in node_api.list(&ListParams::default()).await?.items {
        if let Some(nname) = n.metadata.name.clone() {
            nodes.insert(nname, n);
        }
    }

    let pod_api: Api<Pod> = Api::namespaced(ctx.client.clone(), namespace);
    let pod_lp =
        ListParams::default().labels(&format!("app.kubernetes.io/instance={cluster_name}"));
    let mut pods_by_name = HashMap::new();
    for p in pod_api.list(&pod_lp).await?.items {
        if let Some(pname) = p.metadata.name.clone() {
            pods_by_name.insert(pname, p);
        }
    }

    let mut bootstrap_services = HashMap::new();
    let mut broker_services = HashMap::new();
    for l in effective_listeners
        .iter()
        .filter(|l| matches!(l.type_, ListenerType::Nodeport | ListenerType::Loadbalancer))
    {
        let bs_name = format!("{cluster_name}-{}-bootstrap", l.name);
        if let Some(bs) = svc_api.get_opt(&bs_name).await? {
            bootstrap_services.insert(bs_name, bs);
        }
        for b in brokers {
            let per_name = format!("{cluster_name}-{}-{}", l.name, b.broker_id);
            if let Some(s) = svc_api.get_opt(&per_name).await? {
                broker_services.insert((l.name.clone(), b.broker_id), s);
            }
        }
    }
    Ok((nodes, pods_by_name, bootstrap_services, broker_services))
}

/// For each (broker, listener) pair, resolve the advertised host:port
/// via [`compute_advertised`]. Short-circuits on the first
/// `AdvertisedError` so the caller can surface a single
/// `PendingExternalAddresses` reason rather than a flapping list.
fn resolve_addresses_per_broker(
    effective_listeners: &[Listener],
    brokers: &[BrokerInfo],
    pods_by_name: &HashMap<String, Pod>,
    nodes: &HashMap<String, Node>,
    broker_services: &HashMap<(String, i32), Service>,
) -> Result<BTreeMap<i32, BTreeMap<String, AdvertisedAddress>>, listeners::AdvertisedError> {
    let mut out: BTreeMap<i32, BTreeMap<String, AdvertisedAddress>> = BTreeMap::new();
    for b in brokers {
        let mut listener_map: BTreeMap<String, AdvertisedAddress> = BTreeMap::new();
        for l in effective_listeners {
            let pod_node = pods_by_name
                .get(&b.pod_name)
                .and_then(|p| p.spec.as_ref())
                .and_then(|s| s.node_name.as_deref());
            let svc_ref = broker_services.get(&(l.name.clone(), b.broker_id));
            let addr = compute_advertised(l, b.broker_id, &b.pod_fqdn, pod_node, nodes, svc_ref)?;
            listener_map.insert(l.name.clone(), addr);
        }
        out.insert(b.broker_id, listener_map);
    }
    Ok(out)
}

#[allow(clippy::too_many_lines)] // linear pipeline; the three branches (invalid / pending / ready) need direct condition + status binding
pub async fn reconcile(obj: Arc<Kafka>, ctx: Arc<Context>) -> Result<Action, ReconcileError> {
    let ns = obj.namespace().unwrap_or_else(|| "default".into());
    let name = obj.name_any();
    tracing::info!(%ns, %name, "reconciling Kafka");

    // 1. Cluster-level headless Service via SSA. Selectors target every
    //    broker pod; rendered identically whether listeners validate or
    //    not so admins can `kubectl get svc` immediately.
    let svc_api: Api<Service> = Api::namespaced(ctx.client.clone(), &ns);
    let svc = render_service(&obj)?;
    apply_object(&svc_api, &svc_name(&name), &svc).await?;

    // 2. Validate `spec.listeners`. On failure we still apply the
    //    headless Service (done) and ensure the cluster-id Secret + adopt
    //    pools below, but skip per-broker Service rendering and write
    //    an empty per-broker ConfigMap so existing TOML keys reflect
    //    "no broker should boot". The spec describes this as
    //    "existing objects are not deleted; surface the error and wait."
    let validation = validate_listeners(
        &obj.spec.listeners,
        obj.spec.inter_broker_listener_name.as_deref(),
    );

    // Effective listeners: synthesize the slice-19/20 default when
    // `spec.listeners` is empty.
    let effective_listeners: Vec<Listener> = if obj.spec.listeners.is_empty() {
        vec![synthesized_default_listener()]
    } else {
        obj.spec.listeners.clone()
    };
    let inter_broker_name = effective_inter_broker_listener_name(
        &obj.spec.listeners,
        obj.spec.inter_broker_listener_name.as_deref(),
    );

    let cm_api: Api<ConfigMap> = Api::namespaced(ctx.client.clone(), &ns);

    // Helper to render+apply a ConfigMap with the supplied address map.
    // Reused on the validation-fail / pending-external paths (empty map)
    // and the happy path (full map).
    let apply_cm = async |listeners_for_cm: &[Listener],
                          addresses: &BTreeMap<i32, BTreeMap<String, AdvertisedAddress>>|
           -> Result<(), ReconcileError> {
        let cm = common::render_configmap(&obj, listeners_for_cm, addresses, &inter_broker_name)?;
        apply_object(&cm_api, &cm_name(&name), &cm).await?;
        Ok(())
    };

    let cfg_hash = common::combined_config_hash(&obj.spec);

    let secret_api: Api<Secret> = Api::namespaced(ctx.client.clone(), &ns);
    let _cluster_id = ensure_cluster_id_secret(&secret_api, &obj).await?;

    let pool_api: Api<KafkaNodePool> = Api::namespaced(ctx.client.clone(), &ns);
    let lp = ListParams::default().labels(&format!("crabka.io/cluster={name}"));
    let pools = pool_api.list(&lp).await?;

    // If validation failed, write an empty ConfigMap (no broker TOML
    // keys) before adopting pools / patching status. This keeps every
    // owned object present but unconfigured.
    let listener_status: Vec<ListenerStatus>;
    let (listeners_valid_cond, listeners_ready_cond);
    if let Err(e) = validation {
        apply_cm(&[], &BTreeMap::new()).await?;
        adopt_pools(&pool_api, &obj, pools.iter(), &cfg_hash).await?;
        listener_status = vec![];
        listeners_valid_cond = condition("ListenersValid", "False", e.reason(), &e.message());
        listeners_ready_cond =
            condition("ListenersReady", "False", "ListenersInvalid", &e.message());
    } else {
        // Enumerate brokers from sibling pools. Empty pool list ->
        // empty broker list -> ConfigMap with no per-broker TOML keys,
        // but listeners are still "valid" (just no consumers yet).
        let pool_items: Vec<KafkaNodePool> = pools.items.clone();
        let brokers = enumerate_brokers(&name, &ns, &pool_items);

        // Optimization: when `spec.listeners` is empty, the synthesized
        // default is internal-only — `compute_advertised` for internal
        // listeners only needs `pod_fqdn`, computable from `BrokerInfo`
        // alone, so we can skip the Pod/Node list and per-broker
        // Service rendering entirely. This preserves the slice-24
        // request sequence exactly.
        let has_external = effective_listeners
            .iter()
            .any(|l| matches!(l.type_, ListenerType::Nodeport | ListenerType::Loadbalancer));

        let (nodes, pods_by_name, bootstrap_services, broker_services) = if has_external {
            apply_external_services(&svc_api, &obj, &name, &effective_listeners, &brokers).await?;
            read_external_state(&ctx, &svc_api, &ns, &name, &effective_listeners, &brokers).await?
        } else {
            (
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
            )
        };

        match resolve_addresses_per_broker(
            &effective_listeners,
            &brokers,
            &pods_by_name,
            &nodes,
            &broker_services,
        ) {
            Err(err) => {
                // Render an empty ConfigMap (no broker TOML keys) so
                // brokers don't try to boot with stale data; report
                // ListenersReady=False with the actionable message.
                apply_cm(&[], &BTreeMap::new()).await?;
                adopt_pools(&pool_api, &obj, pools.iter(), &cfg_hash).await?;
                listener_status = vec![];
                listeners_valid_cond =
                    condition("ListenersValid", "True", "Valid", "listeners validated");
                listeners_ready_cond = condition(
                    "ListenersReady",
                    "False",
                    "PendingExternalAddresses",
                    &err.message(),
                );
            }
            Ok(addresses_per_broker) => {
                apply_cm(&effective_listeners, &addresses_per_broker).await?;
                adopt_pools(&pool_api, &obj, pools.iter(), &cfg_hash).await?;
                listener_status = build_listener_status(
                    &effective_listeners,
                    &addresses_per_broker,
                    &bootstrap_services,
                    &nodes,
                    &name,
                    &ns,
                );
                listeners_valid_cond =
                    condition("ListenersValid", "True", "Valid", "listeners validated");
                let msg = format!("{} listener(s) ready", effective_listeners.len());
                listeners_ready_cond = condition("ListenersReady", "True", "Ready", &msg);
            }
        }
    }

    // Aggregate + patch our own status.
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
            listeners_valid_cond,
            listeners_ready_cond,
        ],
        replicas: Some(rollup.replicas),
        ready_replicas: Some(rollup.ready_replicas),
        listeners: listener_status,
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
