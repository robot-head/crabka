//! Kafka CRD reconciler.
//!
//! `Kafka` is a parent/coordinator. It owns the cluster-level
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

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};

use futures::StreamExt as _;
use k8s_openapi::{
    ByteString,
    api::{
        core::v1::{ConfigMap, Node, Pod, Secret, Service},
        networking::v1::Ingress,
    },
    apimachinery::pkg::apis::meta::v1::ObjectMeta,
};
use kube::{
    Resource, ResourceExt as _,
    api::{Api, ListParams, Patch, PatchParams},
    runtime::{
        controller::{Action, Controller},
        reflector::ObjectRef,
        watcher,
    },
};
use serde_json::json;

use crate::{
    context::Context,
    controller::{
        cluster_ca,
        common::{
            self, FIELD_MANAGER, ReconcileError, apply_dynamic, apply_object, condition,
            ensure_cluster_id_secret, owner_ref, patch_status, render_service,
        },
        listeners::{
            self, AdvertisedAddress, INGRESS_PORT, compute_advertised,
            effective_inter_broker_listener_name, ingress_bootstrap_host, render_bootstrap_ingress,
            render_bootstrap_route, render_bootstrap_service, render_broker_ingress,
            render_broker_route, render_broker_service, synthesized_default_listener,
            validate_listeners,
        },
        logging, network_policy,
    },
    crd::{
        Kafka, KafkaCondition, KafkaNodePool, KafkaStatus, Listener, ListenerAddress,
        ListenerAuthentication, ListenerAuthenticationOAuth, ListenerStatus, ListenerType,
    },
    ids::{ReadyReplicaCount, ReplicaCount},
};

/// Rolled-up view of a cluster's pools. Computed by
/// `aggregate_pool_status` and consumed by `rollup_condition`.
pub(crate) struct ClusterRollup {
    pub replicas: ReplicaCount,
    pub ready_replicas: ReadyReplicaCount,
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
        replicas: ReplicaCount(0),
        ready_replicas: ReadyReplicaCount(0),
        pool_count: 0,
    };
    for pool in pools {
        r.pool_count += 1;
        let s = pool.status.as_ref();
        r.replicas += ReplicaCount(s.and_then(|s| s.replicas).unwrap_or(0));
        r.ready_replicas += ReadyReplicaCount(s.and_then(|s| s.ready_replicas).unwrap_or(0));
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
    if rollup.pool_count > 0 && rollup.ready_replicas < rollup.replicas.0 {
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
    } else if rollup.ready_replicas == rollup.replicas.0 && rollup.replicas > 0 {
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
        // Node changes (e.g. ExternalIP added/removed) may
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
/// pool. The operator enforces `replicas == 1`, so each pool maps to
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
        ListenerType::Ingress | ListenerType::Route => {
            // The bootstrap hostname comes from config; clients reach it on the
            // ingress controller / router port (443).
            let host = ingress_bootstrap_host(listener)?;
            Some(format!("{host}:{INGRESS_PORT}"))
        }
    }
}

/// Apply the bootstrap and per-broker objects for each external listener.
/// Internal listeners need no objects beyond the cluster-wide headless Service.
///
/// - `nodeport` / `loadbalancer`: a `NodePort` / `LoadBalancer` Service each.
/// - `ingress` / `route`: a `ClusterIP` backend Service each, plus an `Ingress`
///   (typed) or `OpenShift` `Route` (dynamic) routing the configured hostname to
///   that backend over TLS passthrough.
#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(cluster = %cluster_name, namespace = %namespace, listeners = effective_listeners.len(), brokers = brokers.len()),
    err,
)]
async fn apply_external_services(
    ctx: &Context,
    svc_api: &Api<Service>,
    owner: &Kafka,
    namespace: &str,
    cluster_name: &str,
    effective_listeners: &[Listener],
    brokers: &[BrokerInfo],
) -> Result<(), ReconcileError> {
    let ingress_api: Api<Ingress> = Api::namespaced(ctx.client.clone(), namespace);
    for l in effective_listeners
        .iter()
        .filter(|l| l.type_ != ListenerType::Internal)
    {
        // Backend Services for every external listener type.
        let bs = render_bootstrap_service(owner, l)?;
        let bs_name = format!("{cluster_name}-{}-bootstrap", l.name);
        apply_object(svc_api, &bs_name, &bs).await?;
        for b in brokers {
            let per = render_broker_service(owner, l, b.broker_id, &b.pod_name)?;
            let per_name = format!("{cluster_name}-{}-{}", l.name, b.broker_id);
            apply_object(svc_api, &per_name, &per).await?;
        }

        // Ingress / Route objects layered on top of the ClusterIP backends.
        match l.type_ {
            ListenerType::Ingress => {
                if let Some(host) = ingress_bootstrap_host(l) {
                    let ing = render_bootstrap_ingress(owner, l, &host)?;
                    apply_object(&ingress_api, &bs_name, &ing).await?;
                }
                for b in brokers {
                    if let Some(host) = listeners::ingress_broker_host(l, b.broker_id) {
                        let ing = render_broker_ingress(owner, l, b.broker_id, &host)?;
                        let per_name = format!("{cluster_name}-{}-{}", l.name, b.broker_id);
                        apply_object(&ingress_api, &per_name, &ing).await?;
                    }
                }
            }
            ListenerType::Route => {
                if let Some(host) = ingress_bootstrap_host(l) {
                    let body = render_bootstrap_route(owner, l, &host)?;
                    apply_route(ctx, namespace, &bs_name, &body).await?;
                }
                for b in brokers {
                    if let Some(host) = listeners::ingress_broker_host(l, b.broker_id) {
                        let body = render_broker_route(owner, l, b.broker_id, &host)?;
                        let per_name = format!("{cluster_name}-{}-{}", l.name, b.broker_id);
                        apply_route(ctx, namespace, &per_name, &body).await?;
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// Return the single canonical OAuth listener config (if any).
/// `validate_listeners` already rejects divergent per-listener OAuth
/// configs, so the first OAuth listener's config is the canonical one
/// for the whole cluster.
fn canonical_oauth_config(listeners: &[Listener]) -> Option<ListenerAuthenticationOAuth> {
    listeners.iter().find_map(|l| match &l.authentication {
        Some(ListenerAuthentication::OAuth(cfg)) => Some(cfg.clone()),
        _ => None,
    })
}

/// Compute the managed OAUTHBEARER trust Secret's name from
/// the parent Kafka CR's listeners. Returns `Some(name)` when at least
/// one OAuth listener has a non-empty `tls_trusted_certificates`, else
/// `None`. The naming is deterministic
/// (`{kafka}-oauth-jwks-trust`) so both `kafka.rs::reconcile_kafka`
/// (which actually upserts the Secret via [`reconcile_oauth_jwks_trust`])
/// and `kafka_node_pool.rs::reconcile` (which mounts the Secret into
/// the broker pod) can derive the same name independently without
/// re-doing the bundle assembly.
pub(crate) fn oauth_jwks_trust_secret_name(kafka: &Kafka) -> Option<String> {
    let canonical = canonical_oauth_config(&kafka.spec.listeners)?;
    if canonical.tls_trusted_certificates.is_empty() {
        return None;
    }
    Some(format!("{}-oauth-jwks-trust", kafka.name_any()))
}

/// Describes the source Secret the operator mounts into
/// broker pods for OAUTHBEARER introspection client-secret. Returned
/// by [`reconcile_oauth_introspection_secret`] (validating, async,
/// runs in `reconcile_kafka`) and re-derived deterministically from
/// the parent Kafka CR via [`oauth_introspection_secret_mount`] (pure,
/// sync, used by the pool reconciler to know what to mount without
/// re-fetching from the apiserver).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OauthIntrospectionMount {
    pub secret_name: String,
    pub key: String,
}

/// Derives the OAUTHBEARER introspection client-secret
/// mount from the parent Kafka CR's listeners. `Some` when at least
/// one OAuth listener has `accessTokenIsJwt: false` and a
/// `clientSecret` ref; `None` when no OAuth listener uses
/// introspection mode (or no OAuth listener at all). Pure: derives
/// the same `OauthIntrospectionMount` the pool reconciler will mount
/// without consulting the apiserver — the source Secret's name + key
/// live on the CR.
pub(crate) fn oauth_introspection_secret_mount(kafka: &Kafka) -> Option<OauthIntrospectionMount> {
    let canonical = canonical_oauth_config(&kafka.spec.listeners)?;
    if canonical.access_token_is_jwt {
        return None;
    }
    let cs = canonical.client_secret.as_ref()?;
    Some(OauthIntrospectionMount {
        secret_name: cs.secret_name.clone(),
        key: cs.key.clone(),
    })
}

/// In-pod mount info for the GSSAPI keytab. `key` is the user's source
/// key; mounted via projected items to a fixed path so the broker reads
/// `/etc/crabka/gssapi-keytab/keytab` regardless of key name.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct GssapiKeytabMount {
    pub secret_name: String,
    pub key: String,
}

/// The keytab Secret ref from the (first) GSSAPI listener, or `None` when
/// no listener is `type: gssapi`. Validation guarantees all GSSAPI
/// listeners agree, so the first is canonical.
pub(crate) fn gssapi_keytab_mount(kafka: &Kafka) -> Option<GssapiKeytabMount> {
    kafka
        .spec
        .listeners
        .iter()
        .find_map(|l| match &l.authentication {
            Some(ListenerAuthentication::Gssapi(c)) => Some(GssapiKeytabMount {
                secret_name: c.keytab_secret_ref.secret_name.clone(),
                key: c.keytab_secret_ref.key.clone(),
            }),
            _ => None,
        })
}

/// krb5.conf Secret ref, when `spec.krb5ConfSecretRef` is set.
pub(crate) fn krb5_conf_mount(kafka: &Kafka) -> Option<(String, String)> {
    kafka
        .spec
        .krb5_conf_secret_ref
        .as_ref()
        .map(|r| (r.secret_name.clone(), r.key.clone()))
}

/// Build the managed oauth-jwks-trust Secret from the
/// canonical OAuth config's `tls_trusted_certificates`. Returns the
/// Secret's name (so the `StatefulSet` can mount it), or `None` when no
/// managed Secret is needed (no OAuth listener, or no trust certs
/// configured).
#[tracing::instrument(level = "debug", skip_all, fields(kafka = %kafka.name_any()), err)]
async fn reconcile_oauth_jwks_trust(
    secret_api: &Api<Secret>,
    kafka: &Kafka,
    canonical: Option<&ListenerAuthenticationOAuth>,
) -> Result<Option<String>, ReconcileError> {
    let Some(canonical) = canonical else {
        return Ok(None);
    };
    if canonical.tls_trusted_certificates.is_empty() {
        return Ok(None);
    }
    let mut bundle = Vec::<u8>::new();
    for entry in &canonical.tls_trusted_certificates {
        let src = secret_api
            .get_opt(&entry.secret_name)
            .await?
            .ok_or_else(|| ReconcileError::MissingOauthTrustSecret(entry.secret_name.clone()))?;
        let key_bytes = src
            .data
            .as_ref()
            .and_then(|d| d.get(&entry.certificate))
            .ok_or_else(|| ReconcileError::MissingOauthTrustKey {
                secret: entry.secret_name.clone(),
                key: entry.certificate.clone(),
            })?;
        if key_bytes.0.is_empty() {
            return Err(ReconcileError::EmptyOauthTrustValue {
                secret: entry.secret_name.clone(),
                key: entry.certificate.clone(),
            });
        }
        if !bundle.is_empty() && !bundle.ends_with(b"\n") {
            bundle.push(b'\n');
        }
        bundle.extend_from_slice(&key_bytes.0);
    }
    let managed_name = format!("{}-oauth-jwks-trust", kafka.name_any());
    upsert_oauth_trust_secret(secret_api, kafka, &managed_name, bundle).await?;
    Ok(Some(managed_name))
}

/// Server-side apply the managed `{kafka}-oauth-jwks-trust`
/// Secret with the concatenated PEM bundle under key `ca.crt`. Owner-
/// ref'd to the parent `Kafka` so deleting the CR cascades the Secret.
#[tracing::instrument(
    level = "info",
    skip_all,
    fields(kafka = %kafka.name_any(), secret = %managed_name, bytes = bundle.len()),
    err,
)]
async fn upsert_oauth_trust_secret(
    secret_api: &Api<Secret>,
    kafka: &Kafka,
    managed_name: &str,
    bundle: Vec<u8>,
) -> Result<(), ReconcileError> {
    let labels = common::common_labels(&kafka.name_any(), &kafka.spec.kafka_version, None);
    let mut data = BTreeMap::new();
    data.insert("ca.crt".to_string(), ByteString(bundle));
    let secret = Secret {
        metadata: ObjectMeta {
            name: Some(managed_name.to_string()),
            namespace: kafka.meta().namespace.clone(),
            labels: Some(labels),
            owner_references: Some(vec![owner_ref::<Kafka>(kafka)?]),
            ..Default::default()
        },
        type_: Some("Opaque".into()),
        data: Some(data),
        ..Default::default()
    };
    apply_object(secret_api, managed_name, &secret).await
}

/// Validate the OAUTHBEARER introspection client-secret
/// Secret + key exist (no managed-Secret upsert — the pod template
/// mounts the source Secret directly via projected items). Returns
/// the mount info for the `StatefulSet` renderer, or `None` when
/// introspection is not configured (JWT mode or no oauth listener).
///
/// The `_kafka` arg mirrors the sibling's signature for
/// call-site symmetry but is unused here: there is no managed Secret
/// to owner-ref — the source Secret stays user-owned.
async fn reconcile_oauth_introspection_secret(
    secret_api: &Api<Secret>,
    _kafka: &Kafka,
    canonical: Option<&ListenerAuthenticationOAuth>,
) -> Result<Option<OauthIntrospectionMount>, ReconcileError> {
    let Some(c) = canonical else {
        return Ok(None);
    };
    if c.access_token_is_jwt {
        return Ok(None);
    }
    let cs = c.client_secret.as_ref().ok_or_else(|| {
        ReconcileError::InvalidListenerOauthAccessTokenIsJwt(
            "introspection mode requires clientSecret".into(),
        )
    })?;
    let src = secret_api
        .get_opt(&cs.secret_name)
        .await?
        .ok_or_else(|| ReconcileError::MissingOauthIntrospectionSecret(cs.secret_name.clone()))?;
    let val = src
        .data
        .as_ref()
        .and_then(|d| d.get(&cs.key))
        .ok_or_else(|| ReconcileError::MissingOauthIntrospectionKey {
            secret: cs.secret_name.clone(),
            key: cs.key.clone(),
        })?;
    if val.0.is_empty() {
        return Err(ReconcileError::EmptyOauthIntrospectionValue {
            secret: cs.secret_name.clone(),
            key: cs.key.clone(),
        });
    }
    Ok(Some(OauthIntrospectionMount {
        secret_name: cs.secret_name.clone(),
        key: cs.key.clone(),
    }))
}

/// Apply one `OpenShift` `Route` via the dynamic-object path.
async fn apply_route(
    ctx: &Context,
    namespace: &str,
    name: &str,
    body: &serde_json::Value,
) -> Result<(), ReconcileError> {
    apply_dynamic(
        &ctx.client,
        namespace,
        "route.openshift.io/v1",
        "Route",
        "routes",
        name,
        body,
    )
    .await
}

/// Read back the cluster Nodes, broker Pods, and per-listener Services
/// the operator just applied. Returned as three `HashMap`s plus the
/// pod-by-name lookup the address resolver needs.
///
/// Returning `Ok(default)` rather than failing on any individual GET's
/// 404 is intentional: a Pod that hasn't been created yet is not an
/// error — it surfaces as `PodNotScheduled` in `compute_advertised`.
#[allow(clippy::type_complexity)]
#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(cluster = %cluster_name, namespace = %namespace, brokers = brokers.len()),
    err,
)]
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
    // Node + Pod state is only needed to resolve NodePort advertised hosts (the
    // node's external IP) / LoadBalancer scheduling. ingress/route advertised
    // hosts come from config, so an ingress/route-only cluster issues no
    // Node/Pod LISTs.
    let needs_node_pod = effective_listeners
        .iter()
        .any(|l| matches!(l.type_, ListenerType::Nodeport | ListenerType::Loadbalancer));

    let mut nodes = HashMap::new();
    let mut pods_by_name = HashMap::new();
    if needs_node_pod {
        let node_api: Api<Node> = Api::all(ctx.client.clone());
        for n in node_api.list(&ListParams::default()).await?.items {
            if let Some(nname) = n.metadata.name.clone() {
                nodes.insert(nname, n);
            }
        }

        let pod_api: Api<Pod> = Api::namespaced(ctx.client.clone(), namespace);
        let pod_lp =
            ListParams::default().labels(&format!("app.kubernetes.io/instance={cluster_name}"));
        for p in pod_api.list(&pod_lp).await?.items {
            if let Some(pname) = p.metadata.name.clone() {
                pods_by_name.insert(pname, p);
            }
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

/// Read-modify-write the `Kafka` status conditions: fetch the current
/// status, replace any existing condition with the same `type_` as
/// `new_cond`, push `new_cond`, and patch. Preserves all other status
/// fields (replicas, `cluster_ca`, etc.) so a BYO-CA early-return does
/// not wipe conditions that were written by a previous reconcile pass.
#[tracing::instrument(
    level = "info",
    skip_all,
    fields(name = %name, condition = %new_cond.type_, status = %new_cond.status, reason = %new_cond.reason),
    err,
)]
async fn patch_status_with_condition(
    kafka_api: &Api<Kafka>,
    name: &str,
    new_cond: KafkaCondition,
) -> Result<(), ReconcileError> {
    let current = kafka_api.get_status(name).await?.status.unwrap_or_default();
    let mut conditions: Vec<KafkaCondition> = current
        .conditions
        .into_iter()
        .filter(|c| c.type_ != new_cond.type_)
        .collect();
    conditions.push(new_cond);
    let status = KafkaStatus {
        conditions,
        ..current
    };
    patch_status::<Kafka, KafkaStatus>(kafka_api, name, status).await
}

/// Reconcile entry point. Thin wrapper that times the pass, records the
/// `reconciliations_total{kind,result}` counter + `reconcile_duration_seconds`
/// histogram, and delegates to [`reconcile_inner`]. Kept separate so the
/// per-outcome metric classification (ok / error) lives in one place and the
/// long linear inner body's many early-return sites don't each need to record.
#[tracing::instrument(
    skip_all,
    fields(
        kind = "Kafka",
        namespace = %obj.namespace().unwrap_or_else(|| "default".into()),
        name = %obj.name_any(),
        generation = ?obj.meta().generation,
    )
)]
pub async fn reconcile(obj: Arc<Kafka>, ctx: Arc<Context>) -> Result<Action, ReconcileError> {
    common::record_reconcile(&ctx, "Kafka", Box::pin(reconcile_inner(obj, ctx.clone()))).await
}

#[allow(clippy::too_many_lines)] // linear pipeline; the three branches (invalid / pending / ready) need direct condition + status binding
async fn reconcile_inner(obj: Arc<Kafka>, ctx: Arc<Context>) -> Result<Action, ReconcileError> {
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

    // Effective listeners: synthesize the default when
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

    // Emit a Warning event for each SCRAM listener that lacks transport TLS.
    for msg in listeners::weak_auth_warnings(&effective_listeners) {
        emit_weak_auth_event(&ctx.client, &ns, &obj, &msg)
            .await
            .ok();
    }

    let cm_api: Api<ConfigMap> = Api::namespaced(ctx.client.clone(), &ns);

    let secret_api: Api<Secret> = Api::namespaced(ctx.client.clone(), &ns);
    let _cluster_id = ensure_cluster_id_secret(&secret_api, &obj).await?;

    // KIP-405: shape-validate `spec.tieredStorage` before
    // any ConfigMap render — a mis-paired discriminator (`type=S3` with no
    // `s3` block, or an S3 spec missing `bucket`/`region`) would otherwise
    // produce broker TOML the broker rejects at boot. Failing here keeps
    // the broker pods on the previously-valid generation.
    //
    // Surface the failure as a `TieredStorageReady=False`
    // condition on `Kafka.status.conditions[]` (matching the OAuth-
    // validation pattern) so operators see *why* their spec was rejected
    // instead of having to read controller logs. The happy path emits
    // `TieredStorageReady=True` so a transition from invalid → valid
    // clears the condition.
    let kafka_api_for_ts: Api<Kafka> = Api::namespaced(ctx.client.clone(), &ns);
    if let Some(ts) = &obj.spec.tiered_storage {
        match ts.validate() {
            Ok(()) => {
                let cond = condition(
                    "TieredStorageReady",
                    "True",
                    "Validated",
                    "tieredStorage spec is well-formed",
                );
                patch_status_with_condition(&kafka_api_for_ts, &name, cond).await?;
            }
            Err(why) => {
                let cond = condition(
                    "TieredStorageReady",
                    "False",
                    "TieredStorageInvalid",
                    &format!("tieredStorage: {why}"),
                );
                patch_status_with_condition(&kafka_api_for_ts, &name, cond).await?;
                return Err(ReconcileError::TieredStorageInvalid(why));
            }
        }
    }

    // Validate `spec.tracing` before rendering pods. Same
    // pattern as the tiered-storage `TieredStorageReady` condition:
    // emit a `TracingReady` condition on validation
    // pass/fail so operators see *why* their OTLP spec was rejected
    // instead of having to read controller logs.
    if let Some(tr) = &obj.spec.tracing {
        match tr.validate() {
            Ok(()) => {
                let cond = condition(
                    "TracingReady",
                    "True",
                    "Validated",
                    "tracing spec is well-formed",
                );
                patch_status_with_condition(&kafka_api_for_ts, &name, cond).await?;
            }
            Err(why) => {
                let cond = condition(
                    "TracingReady",
                    "False",
                    "TracingInvalid",
                    &format!("tracing: {why}"),
                );
                patch_status_with_condition(&kafka_api_for_ts, &name, cond).await?;
                return Err(ReconcileError::TracingInvalid(why));
            }
        }
    }

    // Evaluate the declared versions against the operator-
    // finalized metadata version (read from the watched object's status —
    // no extra API request). On a failure we surface KafkaVersionValid=
    // False, do not inject the new metadata version, and do not advance the
    // config hash or the finalized version — "surface the error and wait".
    let finalized_metadata = obj
        .status
        .as_ref()
        .and_then(|s| s.metadata_version.as_deref());
    let version_outcome = crate::version::evaluate(
        &obj.spec.kafka_version,
        obj.spec.metadata_version.as_deref(),
        finalized_metadata,
    );
    let (version_cond, resolved_metadata): (KafkaCondition, Option<String>) = match &version_outcome
    {
        crate::version::VersionOutcome::Valid { resolved_metadata } => (
            condition(
                "KafkaVersionValid",
                "True",
                "Valid",
                &format!(
                    "kafkaVersion {} metadata.version {resolved_metadata}",
                    obj.spec.kafka_version
                ),
            ),
            Some(resolved_metadata.clone()),
        ),
        crate::version::VersionOutcome::Invalid { reason, message } => (
            condition("KafkaVersionValid", "False", reason.as_str(), message),
            None,
        ),
    };
    // Only an explicit, valid pin enters the config hash (a defaulted
    // metadata version rolls via the pod-template image change instead,
    // and including it would break the empty-hash collapse).
    let explicit_pin: Option<&str> = if resolved_metadata.is_some() {
        obj.spec.metadata_version.as_deref()
    } else {
        None
    };

    // Resolve spec.logging into a RUST_LOG env-filter (inline
    // composed in-process, external read from a user ConfigMap). A transient
    // API error propagates and requeues; a user error (bad level, missing
    // ConfigMap/key) surfaces LoggingReady=False without rolling and leaves
    // the broker on its built-in default filter.
    let logging_outcome = logging::resolve_logging(&ctx, &obj, &ns).await?;
    let logging_filter = logging_outcome.filter().map(str::to_string);
    let logging_condition = logging::condition_for(&logging_outcome);

    // Pool list — needed up front for the CA rotation convergence
    // check (whether the previous rotation step's roll has finished), and
    // reused below for status rollup + owner-ref adoption.
    let pool_api: Api<KafkaNodePool> = Api::namespaced(ctx.client.clone(), &ns);
    let lp = ListParams::default().labels(&format!("crabka.io/cluster={name}"));
    let pools = pool_api.list(&lp).await?;
    let rollout_converged = pools_converged(pools.iter());

    // Reconcile both CAs with rotation. The cluster CA drives the
    // staged key-replacement machine + the config-hash; the clients CA only
    // creates / same-key-renews (its truststore is hot-reloaded). force-* and
    // CronJob `ca-renew-after` annotations target the cluster CA. On
    // BYO-missing, surface a False condition and requeue.
    let cr_anns = obj.meta().annotations.clone().unwrap_or_default();
    let force_renew = cr_anns.contains_key(cluster_ca::ANN_FORCE_RENEW)
        || cr_anns.contains_key(cluster_ca::ANN_RENEW_AFTER);
    let force_replace_key = cr_anns.contains_key(cluster_ca::ANN_FORCE_REPLACE_KEY);
    let now = time::OffsetDateTime::now_utc();

    let (cluster_ca_outcome, clients_ca_outcome, cluster_ca_cond, clients_ca_cond) = {
        let cluster_result = cluster_ca::reconcile_ca(
            &secret_api,
            &obj,
            cluster_ca::WhichCa::Cluster,
            force_renew,
            force_replace_key,
            rollout_converged,
            now,
        )
        .await;
        let cluster_outcome = match cluster_result {
            Ok(o) => o,
            Err(ReconcileError::ByoCaMissing { ref which }) => {
                let cond = condition(
                    which,
                    "False",
                    "ByoCaMissing",
                    "spec.clusterCa.generateCertificateAuthority=false but the CA Secret pair is absent",
                );
                let kafka_api: Api<Kafka> = Api::namespaced(ctx.client.clone(), &ns);
                patch_status_with_condition(&kafka_api, &name, cond).await?;
                return Ok(Action::requeue(Duration::from_mins(1)));
            }
            Err(e) => return Err(e),
        };
        // Clients CA never enters the staged machine and takes no force flags.
        let clients_result = cluster_ca::reconcile_ca(
            &secret_api,
            &obj,
            cluster_ca::WhichCa::Clients,
            false,
            false,
            true,
            now,
        )
        .await;
        let clients_outcome = match clients_result {
            Ok(o) => o,
            Err(ReconcileError::ByoCaMissing { ref which }) => {
                let cond = condition(
                    which,
                    "False",
                    "ByoCaMissing",
                    "spec.clientsCa.generateCertificateAuthority=false but the CA Secret pair is absent",
                );
                let kafka_api: Api<Kafka> = Api::namespaced(ctx.client.clone(), &ns);
                patch_status_with_condition(&kafka_api, &name, cond).await?;
                return Ok(Action::requeue(Duration::from_mins(1)));
            }
            Err(e) => return Err(e),
        };
        let cc = condition(
            "ClusterCaReady",
            "True",
            "CaReady",
            "cluster CA Secret pair present and parseable",
        );
        let clic = condition(
            "ClientsCaReady",
            "True",
            "CaReady",
            "clients CA Secret pair present and parseable",
        );
        (cluster_outcome, clients_outcome, cc, clic)
    };

    // Strip the one-shot rotation-trigger annotations once consumed (force
    // renew/replace + CronJob nudge are all acted on this pass).
    let strip_keys: Vec<&str> = [
        cluster_ca::ANN_FORCE_RENEW,
        cluster_ca::ANN_FORCE_REPLACE_KEY,
        cluster_ca::ANN_RENEW_AFTER,
    ]
    .into_iter()
    .filter(|k| cr_anns.contains_key(*k))
    .collect();
    if !strip_keys.is_empty() {
        let kafka_api: Api<Kafka> = Api::namespaced(ctx.client.clone(), &ns);
        strip_annotations(&kafka_api, &name, &strip_keys).await?;
    }

    // A forced rotation the operator can't honor (BYO / clients-CA key
    // replace) surfaces a Warning Event; the condition explains it too.
    if cluster_ca_outcome.refused.is_some() {
        emit_ca_rotation_refused_event(
            &ctx.client,
            &ns,
            &obj,
            &cluster_ca_outcome.rotation_message,
        )
        .await
        .ok();
    }

    let ca_rotation_cond = condition(
        "CaRotation",
        if cluster_ca_outcome.rotation_in_progress {
            "True"
        } else {
            "False"
        },
        cluster_ca_outcome.rotation_reason,
        &cluster_ca_outcome.rotation_message,
    );

    // The config-hash covers the cluster-CA *trust bundle* (not just
    // the signing cert), so adding / promoting / pruning a trust anchor rolls
    // the cluster, while same-key leaf renewal (hot-reload) does not.
    let cfg_hash = common::combined_config_hash(
        &obj.spec,
        Some(&cluster_ca_outcome.trust_bundle_pem),
        explicit_pin,
        logging_filter.as_deref(),
    );

    // If validation failed, leave the existing ConfigMap untouched —
    // per the spec, "existing objects are not deleted; surface the
    // error and wait." Stripping `broker-{id}.toml` keys would crash
    // a previously-healthy cluster on the next pod restart. The pool
    // is still adopted so the config-hash annotation reflects the
    // (invalid) intent, but no roll fires until the user fixes the spec.
    let listener_status: Vec<ListenerStatus>;
    let (listeners_valid_cond, listeners_ready_cond);
    let mut lb_pending: Vec<(i32, String)> = Vec::new();
    if let Err(e) = validation {
        adopt_pools(&pool_api, &obj, pools.iter(), &cfg_hash).await?;
        listener_status = vec![];
        listeners_valid_cond = condition("ListenersValid", "False", e.reason(), &e.message());
        listeners_ready_cond =
            condition("ListenersReady", "False", "ListenersInvalid", &e.message());
    } else {
        // Assemble the OAUTHBEARER JWKS TLS trust bundle (if any).
        // Failures here surface as Ready=False and short-circuit before
        // any per-broker objects are rendered (an OAuth listener with a
        // broken trust spec is not safe to bring brokers up against).
        // The managed Secret's name (derived deterministically from the
        // parent name) is recomputed by the pool reconciler via
        // [`oauth_jwks_trust_secret_name`] when rendering the
        // `StatefulSet`'s pod template, so it doesn't need to be
        // threaded out of this function — calling
        // `reconcile_oauth_jwks_trust` here is purely for its
        // upsert-the-Secret side effect.
        let oauth_canonical = canonical_oauth_config(&effective_listeners);
        match reconcile_oauth_jwks_trust(&secret_api, &obj, oauth_canonical.as_ref()).await {
            Ok(_) => {}
            Err(
                e @ (ReconcileError::MissingOauthTrustSecret(_)
                | ReconcileError::MissingOauthTrustKey { .. }
                | ReconcileError::EmptyOauthTrustValue { .. }),
            ) => {
                let reason = match &e {
                    ReconcileError::MissingOauthTrustSecret(_) => "MissingOauthTrustSecret",
                    ReconcileError::MissingOauthTrustKey { .. } => "MissingOauthTrustKey",
                    ReconcileError::EmptyOauthTrustValue { .. } => "EmptyOauthTrustValue",
                    _ => unreachable!(),
                };
                let cond = condition("Ready", "False", reason, &e.to_string());
                let kafka_api: Api<Kafka> = Api::namespaced(ctx.client.clone(), &ns);
                patch_status_with_condition(&kafka_api, &name, cond).await?;
                return Ok(Action::requeue(Duration::from_secs(30)));
            }
            Err(e) => return Err(e),
        }

        // Validate the OAUTHBEARER introspection client-secret
        // Secret (when introspection is configured). The pod template
        // derives the same mount independently via
        // `oauth_introspection_secret_mount`, so we don't need to thread
        // the value through here — calling
        // `reconcile_oauth_introspection_secret` is purely for its
        // validate-Secret-exists side effect.
        match reconcile_oauth_introspection_secret(&secret_api, &obj, oauth_canonical.as_ref())
            .await
        {
            Ok(_) => {}
            Err(
                e @ (ReconcileError::InvalidListenerOauthAccessTokenIsJwt(_)
                | ReconcileError::MissingOauthIntrospectionSecret(_)
                | ReconcileError::MissingOauthIntrospectionKey { .. }
                | ReconcileError::EmptyOauthIntrospectionValue { .. }),
            ) => {
                let reason = match &e {
                    ReconcileError::InvalidListenerOauthAccessTokenIsJwt(_) => {
                        "InvalidListenerOauthAccessTokenIsJwt"
                    }
                    ReconcileError::MissingOauthIntrospectionSecret(_) => {
                        "MissingOauthIntrospectionSecret"
                    }
                    ReconcileError::MissingOauthIntrospectionKey { .. } => {
                        "MissingOauthIntrospectionKey"
                    }
                    ReconcileError::EmptyOauthIntrospectionValue { .. } => {
                        "EmptyOauthIntrospectionValue"
                    }
                    _ => unreachable!(),
                };
                let cond = condition("Ready", "False", reason, &e.to_string());
                let kafka_api: Api<Kafka> = Api::namespaced(ctx.client.clone(), &ns);
                patch_status_with_condition(&kafka_api, &name, cond).await?;
                return Ok(Action::requeue(Duration::from_secs(30)));
            }
            Err(e) => return Err(e),
        }

        // Validate the GSSAPI keytab Secret (when a `type: gssapi`
        // listener is configured) and the optional `spec.krb5ConfSecretRef`
        // Secret. The pod template derives the same mounts independently
        // via `gssapi_keytab_mount` / `krb5_conf_mount`, so we don't thread
        // anything out — these checks are purely for the
        // validate-Secret-exists side effect, mirroring the OAuth
        // introspection check above.
        let gssapi_secret_check: Result<(), ReconcileError> = async {
            if let Some(m) = gssapi_keytab_mount(&obj) {
                let secret = secret_api.get_opt(&m.secret_name).await?.ok_or_else(|| {
                    ReconcileError::MissingGssapiKeytabSecret(m.secret_name.clone())
                })?;
                let has_key = secret.data.as_ref().is_some_and(|d| d.contains_key(&m.key))
                    || secret
                        .string_data
                        .as_ref()
                        .is_some_and(|d| d.contains_key(&m.key));
                if !has_key {
                    return Err(ReconcileError::MissingGssapiKeytabKey {
                        secret: m.secret_name,
                        key: m.key,
                    });
                }
            }
            if let Some((secret_name, key)) = krb5_conf_mount(&obj) {
                let secret = secret_api
                    .get_opt(&secret_name)
                    .await?
                    .ok_or_else(|| ReconcileError::MissingKrb5ConfSecret(secret_name.clone()))?;
                let has_key = secret.data.as_ref().is_some_and(|d| d.contains_key(&key))
                    || secret
                        .string_data
                        .as_ref()
                        .is_some_and(|d| d.contains_key(&key));
                if !has_key {
                    return Err(ReconcileError::MissingKrb5ConfKey {
                        secret: secret_name,
                        key,
                    });
                }
            }
            Ok(())
        }
        .await;
        match gssapi_secret_check {
            Ok(()) => {}
            Err(
                e @ (ReconcileError::MissingGssapiKeytabSecret(_)
                | ReconcileError::MissingGssapiKeytabKey { .. }
                | ReconcileError::MissingKrb5ConfSecret(_)
                | ReconcileError::MissingKrb5ConfKey { .. }),
            ) => {
                let reason = match &e {
                    ReconcileError::MissingGssapiKeytabSecret(_) => "MissingGssapiKeytabSecret",
                    ReconcileError::MissingGssapiKeytabKey { .. } => "MissingGssapiKeytabKey",
                    ReconcileError::MissingKrb5ConfSecret(_) => "MissingKrb5ConfSecret",
                    ReconcileError::MissingKrb5ConfKey { .. } => "MissingKrb5ConfKey",
                    _ => unreachable!(),
                };
                let cond = condition("Ready", "False", reason, &e.to_string());
                let kafka_api: Api<Kafka> = Api::namespaced(ctx.client.clone(), &ns);
                patch_status_with_condition(&kafka_api, &name, cond).await?;
                return Ok(Action::requeue(Duration::from_secs(30)));
            }
            Err(e) => return Err(e),
        }

        // Inter-broker Kerberos: when the resolved inter-broker listener
        // uses GSSAPI, `spec.interBrokerKerberos` must be present (the
        // broker needs initiate-side credentials). Surface a failure the
        // same way `validate_listeners` does — `ListenersValid=False` with
        // the `ValidationError`'s `reason()`/`message()`.
        if let Err(e) = listeners::validate_inter_broker_gssapi(
            &effective_listeners,
            &inter_broker_name,
            obj.spec.inter_broker_kerberos.is_some(),
        ) {
            let cond = condition("ListenersValid", "False", e.reason(), &e.message());
            let kafka_api: Api<Kafka> = Api::namespaced(ctx.client.clone(), &ns);
            patch_status_with_condition(&kafka_api, &name, cond).await?;
            return Ok(Action::requeue(Duration::from_secs(30)));
        }

        // Enumerate brokers from sibling pools. Empty pool list ->
        // empty broker list -> ConfigMap with no per-broker TOML keys,
        // but listeners are still "valid" (just no consumers yet).
        let pool_items: Vec<KafkaNodePool> = pools.items.clone();
        let brokers = enumerate_brokers(&name, &ns, &pool_items);

        // Observe external listener addresses for SAN extension.
        let broker_ids: Vec<i32> = brokers.iter().map(|b| b.broker_id).collect();
        let observed = listeners::observe_listener_addresses(
            &ctx,
            &ns,
            &name,
            &effective_listeners,
            &broker_ids,
        )
        .await?;

        // Brokers whose LB ingress isn't ready yet are skipped; a status condition will surface this.
        let extra_sans_per_broker: BTreeMap<i32, Vec<crabka_security::ca::SubjectAltName>> =
            brokers
                .iter()
                .filter_map(|b| {
                    match listeners::compute_extra_sans(
                        b.broker_id,
                        &effective_listeners,
                        &observed,
                    ) {
                        Ok(sans) => Some((b.broker_id, sans)),
                        Err(listeners::SanComputationError::SansNotReady {
                            broker_id,
                            listener,
                        }) => {
                            tracing::warn!(
                                broker_id,
                                %listener,
                                "LB ingress not ready; skipping cert SAN extension for this broker"
                            );
                            lb_pending.push((broker_id, listener));
                            None
                        }
                    }
                })
                .collect();

        // Issue per-broker leaf certs into the broker-keystore Secret.
        let keystore_requests: Vec<cluster_ca::BrokerCertRequest> = brokers
            .iter()
            .map(|b| {
                let id = b.broker_id;
                let cn = b.pod_name.clone();
                let sans = vec![
                    crabka_security::ca::SubjectAltName::Dns(b.pod_fqdn.clone()),
                    crabka_security::ca::SubjectAltName::Dns(b.pod_name.clone()),
                    crabka_security::ca::SubjectAltName::Dns(format!(
                        "{name}-broker-headless.{ns}.svc.cluster.local"
                    )),
                    crabka_security::ca::SubjectAltName::Ip(std::net::IpAddr::V4(
                        std::net::Ipv4Addr::LOCALHOST,
                    )),
                ];
                let extra = extra_sans_per_broker.get(&id).cloned().unwrap_or_default();
                cluster_ca::BrokerCertRequest {
                    broker_id: id,
                    cn,
                    sans,
                    extra_sans: extra,
                }
            })
            .collect();
        // Keystore status fields (issued/reused/pruned) are reserved
        // for a future status surface; ignored for now.
        cluster_ca::ensure_broker_keystore(
            &secret_api,
            &obj,
            &keystore_requests,
            &cluster_ca_outcome.signing_material,
            cluster_ca_outcome.force_reissue_leafs,
        )
        .await?;

        // Build per-broker TLS render map (paths inside the
        // mounted broker-tls volume).
        let tls_per_broker: std::collections::BTreeMap<i32, listeners::BrokerTlsRender> = brokers
            .iter()
            .map(|b| {
                let id = b.broker_id;
                (
                    id,
                    listeners::BrokerTlsRender {
                        controller_listener_protocol: "Ssl".into(),
                        cert_path: format!("/etc/crabka/broker-tls/{id}.crt"),
                        key_path: format!("/etc/crabka/broker-tls/{id}.key"),
                        client_ca_path: "/etc/crabka/cluster-ca/ca.crt".into(),
                        client_auth: "Required".into(),
                        trust_roots_path: "/etc/crabka/cluster-ca/ca.crt".into(),
                    },
                )
            })
            .collect();

        let clients_ca_path: Option<&str> = if effective_listeners
            .iter()
            .any(|l| matches!(l.authentication, Some(ListenerAuthentication::Tls)))
        {
            Some("/etc/crabka/clients-ca/ca.crt")
        } else {
            None
        };

        // Helper to render+apply a ConfigMap with the supplied address map.
        // Defined here (inside the validation-ok branch) so it can capture
        // `tls_per_broker` and `clients_ca_path`. On the
        // validation-fail path there is no `apply_cm` call.
        let apply_cm = async |listeners_for_cm: &[Listener],
                              addresses: &BTreeMap<i32, BTreeMap<String, AdvertisedAddress>>|
               -> Result<(), ReconcileError> {
            let cm = common::render_configmap(
                &obj,
                listeners_for_cm,
                addresses,
                &inter_broker_name,
                Some(&tls_per_broker),
                clients_ca_path,
                logging_filter.as_deref(),
            )?;
            apply_object(&cm_api, &cm_name(&name), &cm).await?;
            Ok(())
        };

        // Optimization: when every effective listener is internal (e.g. the
        // synthesized default), `compute_advertised` only needs
        // `pod_fqdn` (from `BrokerInfo`), so we skip per-broker object
        // rendering and the Pod/Node/Service reads entirely. This preserves
        // the internal-only request sequence exactly.
        let has_external = effective_listeners
            .iter()
            .any(|l| l.type_ != ListenerType::Internal);

        let (nodes, pods_by_name, bootstrap_services, broker_services) = if has_external {
            apply_external_services(
                &ctx,
                &svc_api,
                &obj,
                &ns,
                &name,
                &effective_listeners,
                &brokers,
            )
            .await?;
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
                // Pending external addresses (cold-start LB provisioning,
                // node not scheduled yet). Leave the existing ConfigMap
                // untouched — pods that are already running should not
                // be disturbed; cold-start pods sit pending the CM,
                // which arrives once the apiserver populates the
                // Service status on a subsequent reconcile.
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

    // Metrics resources: surface a MetricsReady condition regardless of
    // whether spec.metricsConfig is set. MutuallyExclusive and
    // PrometheusOperatorCrdsMissing are reported via the condition only —
    // the reconcile continues so the rest of the status patch lands.
    let metrics_outcome =
        crate::controller::metrics::reconcile_metrics(&ctx, &obj, &name, &ns).await;
    let metrics_condition = match &metrics_outcome {
        None => condition(
            "MetricsReady",
            "False",
            "Disabled",
            "spec.metricsConfig is not set",
        ),
        Some(Ok(())) => condition(
            "MetricsReady",
            "True",
            "Available",
            "metrics resources reconciled",
        ),
        Some(Err(ReconcileError::MetricsMutuallyExclusive)) => condition(
            "MetricsReady",
            "False",
            "MutuallyExclusive",
            "podMonitor and serviceMonitor are mutually exclusive",
        ),
        Some(Err(ReconcileError::PrometheusOperatorCrdsMissing)) => condition(
            "MetricsReady",
            "False",
            "PrometheusOperatorCrdsMissing",
            "monitoring.coreos.com/v1 is not served by the API server",
        ),
        Some(Err(_)) => condition("MetricsReady", "False", "Error", "metrics reconcile failed"),
    };

    // NetworkPolicy reconcile (opt-in via spec.networkPolicy).
    // Inter-broker port: the listener whose name matches the effective
    // inter-broker name. Falls back to the synthesized default's BROKER_PORT
    // (defensive only; effective_listeners is always non-empty).
    let inter_broker_port = effective_listeners
        .iter()
        .find(|l| l.name == inter_broker_name)
        .map_or(common::BROKER_PORT, |l| l.port);

    let np_outcome = network_policy::reconcile_network_policy(
        &ctx,
        &obj,
        &name,
        &ns,
        &effective_listeners,
        inter_broker_port,
    )
    .await;
    let np_condition = match &np_outcome {
        None => condition(
            "NetworkPolicyReady",
            "False",
            "Disabled",
            "spec.networkPolicy is not set",
        ),
        Some(Ok(())) => condition(
            "NetworkPolicyReady",
            "True",
            "Available",
            "network policy reconciled",
        ),
        Some(Err(_)) => condition(
            "NetworkPolicyReady",
            "False",
            "Error",
            "network policy reconcile failed",
        ),
    };

    // Aggregate + patch our own status.
    let rollup = aggregate_pool_status(pools.iter());
    // Surface the observed node-pool count for this cluster as a gauge. This is
    // a coarse ownership/liveness signal (how many pools the parent reconciler
    // saw this pass), complementing the per-pool `KafkaNodePool` reconciles.
    ctx.metrics.set_managed_resources(
        "KafkaNodePool",
        i64::try_from(rollup.pool_count).unwrap_or(i64::MAX),
    );
    let (ready, reason, message) = rollup_condition(&rollup);
    let (rolling, rolling_reason, rolling_message) = rolling_condition_from_rollup(&rollup);
    let mut conditions = vec![
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
        metrics_condition,
        np_condition,
        cluster_ca_cond,
        clients_ca_cond,
        ca_rotation_cond,
        version_cond,
        logging_condition,
    ];
    let has_lb_tls_listener = effective_listeners
        .iter()
        .any(|l| l.type_ == ListenerType::Loadbalancer && l.tls);
    if has_lb_tls_listener {
        if lb_pending.is_empty() {
            conditions.push(condition(
                "WaitingForLoadBalancerIp",
                "False",
                "LoadBalancerReady",
                "all broker LB ingress addresses assigned",
            ));
        } else {
            let detail: Vec<String> = lb_pending
                .iter()
                .map(|(id, l)| format!("broker {id} listener '{l}'"))
                .collect();
            conditions.push(condition(
                "WaitingForLoadBalancerIp",
                "True",
                "LoadBalancerPending",
                &format!("LB ingress not ready for: {}", detail.join(", ")),
            ));
        }
    }
    let status = KafkaStatus {
        conditions,
        replicas: Some(rollup.replicas.0),
        ready_replicas: Some(rollup.ready_replicas.0),
        listeners: listener_status,
        cluster_ca: Some(crate::crd::CertificateAuthorityStatus {
            not_after: cluster_ca_outcome.not_after.clone(),
            generated: cluster_ca_outcome.generated,
            cert_generation: cluster_ca_outcome.cert_generation,
            key_generation: cluster_ca_outcome.key_generation,
            rotation_phase: Some(cluster_ca_outcome.phase.as_str().to_string()),
            trust_anchors: Some(cluster_ca_outcome.trust_anchors),
        }),
        clients_ca: Some(crate::crd::CertificateAuthorityStatus {
            not_after: clients_ca_outcome.not_after.clone(),
            generated: clients_ca_outcome.generated,
            cert_generation: clients_ca_outcome.cert_generation,
            key_generation: clients_ca_outcome.key_generation,
            rotation_phase: Some(clients_ca_outcome.phase.as_str().to_string()),
            trust_anchors: Some(clients_ca_outcome.trust_anchors),
        }),
        kafka_version: Some(obj.spec.kafka_version.clone()),
        // Advance the finalized metadata version when valid; hold the
        // previous value on a validation failure.
        metadata_version: resolved_metadata
            .clone()
            .or_else(|| finalized_metadata.map(str::to_string)),
    };
    let kafka_api: Api<Kafka> = Api::namespaced(ctx.client.clone(), &ns);
    patch_status::<Kafka, KafkaStatus>(&kafka_api, &name, status).await?;

    // Propagate any non-condition-mapped metrics error after the status
    // patch — we want admins to see the condition update before bouncing.
    if let Some(Err(e)) = metrics_outcome
        && !matches!(
            e,
            ReconcileError::MetricsMutuallyExclusive
                | ReconcileError::PrometheusOperatorCrdsMissing
        )
    {
        return Err(e);
    }

    if let Some(Err(e)) = np_outcome {
        return Err(e);
    }

    Ok(Action::requeue(Duration::from_secs(30)))
}

/// For every pool labeled `crabka.io/cluster=<this Kafka>`, patch
/// `metadata.ownerReferences` so the Kafka is the controlling owner AND
/// `metadata.labels["crabka.io/config-hash"]` so the pool reconciler
/// observes config drift. Uses a server-side apply with the operator's
/// field manager so the patch wins over any out-of-band manual edits.
///
/// The per-pool hash is planned by [`common::plan_rollout`] so an
/// established multi-pool cluster rolls one node at a time (ordered by
/// `(node_id_start, name)`, gated on each pool reaching Ready) rather than
/// rolling every pool at once. Initial bring-up still applies the hash to
/// every pool in parallel — a `KRaft` controller quorum needs all controllers
/// up together. The owner-ref is applied to every pool every reconcile
/// regardless, so the request count is unchanged.
#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(parent = %parent.name_any(), config_hash = %config_hash),
    err,
)]
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

    // Order pools deterministically and capture each one's observed hash +
    // readiness so the rollout planner can gate advancement.
    let mut ordered: Vec<&KafkaNodePool> = pools.into_iter().collect();
    ordered.sort_by(|a, b| {
        a.spec
            .node_id_start
            .cmp(&b.spec.node_id_start)
            .then_with(|| a.name_any().cmp(&b.name_any()))
    });
    let states: Vec<common::PoolRolloutState> = ordered
        .iter()
        .map(|p| common::PoolRolloutState {
            name: p.name_any(),
            current_hash: p
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get("crabka.io/config-hash").cloned()),
            ready: p
                .status
                .as_ref()
                .and_then(|s| s.ready_replicas)
                .unwrap_or(0)
                >= 1,
        })
        .collect();
    let plan = common::plan_rollout(&states, config_hash);

    for (pool_name, target_hash) in plan {
        // SSA needs apiVersion + kind on the patch payload. The patch
        // *target* is a KafkaNodePool, so the payload's apiVersion/kind
        // match the pool, not the parent Kafka.
        let patch_body = json!({
            "apiVersion": KafkaNodePool::api_version(&()),
            "kind": KafkaNodePool::kind(&()),
            "metadata": {
                "ownerReferences": [owner],
                "labels": { "crabka.io/config-hash": target_hash },
            }
        });
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

/// Emit a `Warning` Kubernetes Event on the `Kafka` object for a listener
/// that carries SCRAM authentication without transport TLS. Fire-and-forget
/// (the caller `.ok()`-s the result) — a transient API error should not
/// block the rest of the reconcile.
async fn emit_weak_auth_event(
    client: &kube::Client,
    namespace: &str,
    kafka: &Kafka,
    message: &str,
) -> Result<(), ReconcileError> {
    crate::controller::cluster_ca::emit_event(
        client,
        namespace,
        kafka,
        "Warning",
        "WeakAuth",
        message,
        "crabka-listener-auth-",
        "ListenerValidation",
        "crabka-operator/listener-auth-check",
    )
    .await
}

/// Warning Event when a forced CA rotation can't be honored (BYO CA
/// or clients-CA key replacement). Fire-and-forget at the call site.
async fn emit_ca_rotation_refused_event(
    client: &kube::Client,
    namespace: &str,
    kafka: &Kafka,
    message: &str,
) -> Result<(), ReconcileError> {
    crate::controller::cluster_ca::emit_event(
        client,
        namespace,
        kafka,
        "Warning",
        "CaRotationRefused",
        message,
        "crabka-ca-rotation-",
        "CaRotation",
        "crabka-operator/ca-rotation",
    )
    .await
}

/// "No roll in flight" — every pool carries the same (non-empty)
/// `crabka.io/config-hash` label and every pool's broker is Ready. The CA
/// rotation state machine advances a staged phase only when this holds, so
/// trust distribution finishes before the new key is promoted (and promotion
/// finishes before the old anchor is pruned). Empty pool list ⇒ converged.
pub(crate) fn pools_converged<'a>(pools: impl IntoIterator<Item = &'a KafkaNodePool>) -> bool {
    let mut hashes = std::collections::BTreeSet::new();
    let mut all_ready = true;
    let mut any = false;
    for p in pools {
        any = true;
        let h = p
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("crabka.io/config-hash").cloned());
        hashes.insert(h);
        if p.status
            .as_ref()
            .and_then(|s| s.ready_replicas)
            .unwrap_or(0)
            < 1
        {
            all_ready = false;
        }
    }
    !any || (hashes.len() == 1 && !hashes.contains(&None) && all_ready)
}

/// Remove one-shot rotation-trigger annotations from the `Kafka` CR
/// (JSON Merge Patch with `null` values deletes the keys).
#[tracing::instrument(level = "debug", skip_all, fields(name = %name, keys = keys.len()), err)]
async fn strip_annotations(
    kafka_api: &Api<Kafka>,
    name: &str,
    keys: &[&str],
) -> Result<(), ReconcileError> {
    let mut ann = serde_json::Map::new();
    for k in keys {
        ann.insert((*k).to_string(), serde_json::Value::Null);
    }
    let patch = json!({ "metadata": { "annotations": serde_json::Value::Object(ann) } });
    kafka_api
        .patch(name, &PatchParams::default(), &Patch::Merge(&patch))
        .await?;
    Ok(())
}

fn svc_name(kafka: &str) -> String {
    format!("{kafka}-broker-headless")
}

fn cm_name(kafka: &str) -> String {
    format!("{kafka}-broker-config")
}

#[cfg(test)]
mod tests {
    use assert2::assert;

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
    fn aggregate_status_rollup_cases() {
        for (name, pools, expected) in [
            ("no node pools", vec![], (false, "NoNodePools")),
            (
                "partially ready pool",
                vec![pool_with_status("brokers", 3, 1)],
                (false, "PartiallyReady"),
            ),
            (
                "all pools ready",
                vec![pool_with_status("brokers", 1, 1)],
                (true, "Available"),
            ),
            (
                "pool with zero replicas",
                vec![pool_with_status("brokers", 0, 0)],
                (false, "PartiallyReady"),
            ),
        ] {
            let r = aggregate_pool_status(pools.iter());
            let (ready, reason, _) = rollup_condition(&r);
            assert_eq!((ready, reason), expected, "case {name}");
        }
    }

    #[test]
    fn rolling_condition_cases() {
        for (name, replicas, ready_replicas, pool_count, expected) in [
            ("partial pool", 3, 1, 1, (true, "RollingUpdate")),
            ("stable pool", 1, 1, 1, (false, "Stable")),
            ("zero pools boundary", 3, 1, 0, (false, "Stable")),
        ] {
            let r = ClusterRollup {
                replicas: ReplicaCount(replicas),
                ready_replicas: ReadyReplicaCount(ready_replicas),
                pool_count,
            };
            let (rolling, reason, _) = rolling_condition_from_rollup(&r);
            assert_eq!((rolling, reason), expected, "case {name}");
        }
    }

    // Pure helper — picks the first OAuth listener as canonical.
    // The reconcile-level no-op cases (no OAuth listener / empty
    // tls_trusted_certificates) are exercised through this helper plus the
    // length check; the network-touching paths are covered by the
    // integration tests.

    fn listener_with_auth(name: &str, auth: Option<ListenerAuthentication>) -> Listener {
        Listener {
            name: name.into(),
            port: 9092,
            type_: ListenerType::Internal,
            tls: true,
            authentication: auth,
            configuration: None,
            network_policy_peers: None,
        }
    }

    fn sample_oauth_cfg(
        certs: Vec<crate::crd::TlsTrustedCertificate>,
    ) -> ListenerAuthenticationOAuth {
        ListenerAuthenticationOAuth {
            valid_issuer_uri: "https://iss.example/".into(),
            jwks_endpoint_uri: Some("https://iss.example/jwks".into()),
            valid_audience: None,
            user_name_claim: None,
            custom_claim_check: None,
            jwks_refresh_seconds: None,
            max_clock_skew_seconds: None,
            enable_oauth_bearer: true,
            tls_trusted_certificates: certs,
            access_token_is_jwt: true,
            introspection_endpoint_uri: None,
            user_info_endpoint_uri: None,
            client_id: None,
            client_secret: None,
            introspection_http_timeout_seconds: None,
            max_seconds_without_reauthentication: None,
            valid_token_type: None,
            fallback_user_name_claim: None,
            fallback_user_name_prefix: None,
            groups_claim: None,
            groups_claim_delimiter: None,
            jwks_min_refresh_pause_seconds: None,
            jwks_expiry_seconds: None,
            jwks_ignore_key_use: None,
        }
    }

    fn sample_oauth_cfg_introspection(secret_name: &str, key: &str) -> ListenerAuthenticationOAuth {
        ListenerAuthenticationOAuth {
            valid_issuer_uri: "https://iss.example/".into(),
            jwks_endpoint_uri: None,
            valid_audience: None,
            user_name_claim: None,
            custom_claim_check: None,
            jwks_refresh_seconds: None,
            max_clock_skew_seconds: None,
            enable_oauth_bearer: true,
            tls_trusted_certificates: vec![],
            access_token_is_jwt: false,
            introspection_endpoint_uri: Some("https://iss.example/introspect".into()),
            user_info_endpoint_uri: None,
            client_id: Some("broker-client".into()),
            client_secret: Some(crate::crd::OauthClientSecretRef {
                secret_name: secret_name.into(),
                key: key.into(),
            }),
            introspection_http_timeout_seconds: None,
            max_seconds_without_reauthentication: None,
            valid_token_type: None,
            fallback_user_name_claim: None,
            fallback_user_name_prefix: None,
            groups_claim: None,
            groups_claim_delimiter: None,
            jwks_min_refresh_pause_seconds: None,
            jwks_expiry_seconds: None,
            jwks_ignore_key_use: None,
        }
    }

    fn kafka_with_listeners(listeners: Vec<Listener>) -> Kafka {
        use crate::crd::KafkaSpec;
        let mut k = Kafka::new(
            "c1",
            KafkaSpec {
                kafka_version: "3.7.0".into(),
                metadata_version: None,
                config: None,
                listeners,
                inter_broker_listener_name: None,
                metrics_config: None,
                network_policy: None,
                cluster_ca: None,
                clients_ca: None,
                logging: None,
                delegation_token: None,
                authorization: None,
                tiered_storage: None,
                inter_broker_kerberos: None,
                krb5_conf_secret_ref: None,
                tracing: None,
            },
        );
        k.metadata.namespace = Some("ns".into());
        k
    }

    #[test]
    fn canonical_oauth_config_cases() {
        let oauth = sample_oauth_cfg(vec![]);
        for (name, listeners, expected) in [
            (
                "no OAuth listener",
                vec![
                    listener_with_auth("plain", None),
                    listener_with_auth("scram", Some(ListenerAuthentication::ScramSha512)),
                ],
                None,
            ),
            (
                "first OAuth listener",
                vec![
                    listener_with_auth("plain", None),
                    listener_with_auth("oauth", Some(ListenerAuthentication::OAuth(oauth.clone()))),
                ],
                Some(oauth.clone()),
            ),
            (
                "OAuth listener with empty trust certificates",
                vec![listener_with_auth(
                    "oauth",
                    Some(ListenerAuthentication::OAuth(oauth.clone())),
                )],
                Some(oauth.clone()),
            ),
        ] {
            assert_eq!(canonical_oauth_config(&listeners), expected, "case {name}");
        }
    }

    // Pure helper — derives the introspection client-secret
    // mount from the CR's listeners. The async, apiserver-touching
    // `reconcile_oauth_introspection_secret` path is covered by the
    // integration tests.

    #[test]
    fn oauth_introspection_secret_mount_absence_cases() {
        let no_oauth = kafka_with_listeners(vec![
            listener_with_auth("plain", None),
            listener_with_auth("scram", Some(ListenerAuthentication::ScramSha512)),
        ]);
        let cfg = sample_oauth_cfg(vec![]);
        let jwt_mode = kafka_with_listeners(vec![listener_with_auth(
            "oauth",
            Some(ListenerAuthentication::OAuth(cfg)),
        )]);
        let mut cfg = sample_oauth_cfg_introspection("oauth-cs", "client-secret");
        cfg.client_secret = None;
        let introspection_without_secret = kafka_with_listeners(vec![listener_with_auth(
            "oauth",
            Some(ListenerAuthentication::OAuth(cfg)),
        )]);
        for (name, kafka) in [
            ("no OAuth listener", no_oauth),
            ("JWT mode", jwt_mode),
            (
                "introspection mode without client secret",
                introspection_without_secret,
            ),
        ] {
            assert_eq!(
                oauth_introspection_secret_mount(&kafka),
                None,
                "case {name}"
            );
        }
    }

    #[test]
    fn oauth_introspection_secret_mount_returns_some_for_introspection_config() {
        let cfg = sample_oauth_cfg_introspection("oauth-cs", "client-secret");
        let kafka = kafka_with_listeners(vec![listener_with_auth(
            "oauth",
            Some(ListenerAuthentication::OAuth(cfg)),
        )]);
        assert_eq!(
            oauth_introspection_secret_mount(&kafka),
            Some(OauthIntrospectionMount {
                secret_name: "oauth-cs".to_string(),
                key: "client-secret".to_string(),
            })
        );
    }

    #[test]
    fn gssapi_keytab_mount_extracted_from_listener() {
        let g = crate::crd::ListenerAuthenticationGssapi {
            keytab_secret_ref: crate::crd::KeytabSecretRef {
                secret_name: "kt".into(),
                key: "krb5.keytab".into(),
            },
            service_name: None,
            principal_to_local_rules: vec!["DEFAULT".into()],
            realm: None,
            kdc: None,
        };
        let k = kafka_with_listeners(vec![listener_with_auth(
            "gss",
            Some(ListenerAuthentication::Gssapi(g)),
        )]);
        assert_eq!(
            gssapi_keytab_mount(&k),
            Some(GssapiKeytabMount {
                secret_name: "kt".to_string(),
                key: "krb5.keytab".to_string(),
            })
        );
    }

    #[test]
    fn no_keytab_mount_without_gssapi_listener() {
        let k = kafka_with_listeners(vec![listener_with_auth("plain", None)]);
        assert!(gssapi_keytab_mount(&k).is_none());
    }

    #[test]
    fn krb5_conf_mount_extracted_from_spec() {
        let mut k = kafka_with_listeners(vec![listener_with_auth("plain", None)]);
        assert!(krb5_conf_mount(&k).is_none());
        k.spec.krb5_conf_secret_ref = Some(crate::crd::Krb5ConfSecretRef {
            secret_name: "krb5".into(),
            key: "krb5.conf".into(),
        });
        assert_eq!(
            krb5_conf_mount(&k),
            Some(("krb5".to_string(), "krb5.conf".to_string()))
        );
    }
}
