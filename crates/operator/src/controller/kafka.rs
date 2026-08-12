//! Kafka CRD reconciler.
//!
//! `Kafka` is a parent and a coordinator. It owns the cluster-level
//! `Service`, the `ConfigMap`, and the cluster-id `Secret`. Broker
//! `StatefulSet`s live on the sibling `KafkaNodePool`s, one for each pool,
//! and the pool owns them. The `Kafka` reconciler collects the per-pool
//! status and reports a cluster-level `Ready` condition.
//!
//! The reconciler rolls the per-pool status up. It sums `replicas` and
//! `readyReplicas` across every `KafkaNodePool` with the label
//! `crabka.io/cluster=<this name>`. The `Ready` condition follows this
//! rule:
//! - no pools           -> `Ready=False`, reason `NoNodePools`
//! - all ready          -> `Ready=True`,  reason `Available`
//! - otherwise          -> `Ready=False`, reason `PartiallyReady`

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use crabka_units::{Time, secs};
use futures::StreamExt as _;
use k8s_openapi::{
    ByteString,
    api::{
        apps::v1::StatefulSet,
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
        kafka_node_pool,
        listeners::{
            self, AdvertisedAddress, INGRESS_PORT, compute_advertised,
            effective_inter_broker_listener_name, ingress_bootstrap_host, render_bootstrap_ingress,
            render_bootstrap_route, render_bootstrap_service, render_broker_ingress,
            render_broker_route, render_broker_service, synthesized_default_listener,
            validate_listeners,
        },
        logging, network_policy, user_tls,
    },
    crd::{
        Kafka, KafkaCondition, KafkaNodePool, KafkaStatus, KafkaUser, Listener, ListenerAddress,
        ListenerAuthentication, ListenerAuthenticationOAuth, ListenerStatus, ListenerType,
    },
    ids::{ReadyReplicaCount, ReplicaCount},
};

/// Rolled-up view of the pools of one cluster.
///
/// `aggregate_pool_status` computes it and `rollup_condition` reads it.
pub(crate) struct ClusterRollup {
    pub replicas: ReplicaCount,
    pub ready_replicas: ReadyReplicaCount,
    pub pool_count: usize,
}

/// Sums `replicas` and `readyReplicas` across every pool and counts the
/// pools.
///
/// A pool with no status yet adds zero to both totals, but it still
/// increments `pool_count`. A new pool therefore shows as
/// `PartiallyReady` and not as `NoNodePools`.
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

/// Translates a rollup into `(rolling, reason, message)` for the cluster
/// `Rolling` condition.
///
/// The operator reports `Rolling=True` when at least one pool exists and
/// not all brokers have reached Ready. This covers the initial bring-up
/// and the restarts that config drift triggers. The rollup alone cannot
/// separate the two.
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

/// Translates a rollup into `(ready, reason, message)` for the cluster
/// `Ready` condition.
///
/// The three branches are the contract that admins and the e2e tests
/// match on.
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

/// Runs the `Kafka` controller forever. It returns only on an
/// irrecoverable stream error. The kube-rs `Controller` re-establishes
/// the watches on recoverable errors by itself.
///
/// The controller watches `KafkaNodePool`, so that a pool status change
/// wakes the reconcile of its parent. The mapper reads the parent name
/// from the `crabka.io/cluster` label and the namespace from the pool
/// itself.
///
/// The controller also watches `Node`, which is cluster-scoped, so that a
/// change of an `ExternalIP` triggers a reconcile. `ExternalIP` matters
/// for `NodePort` listeners. The mapper returns an empty result. The
/// periodic requeue every 30 s picks the change up, and the controller
/// does not enqueue every Kafka on every Node event.
/// # Errors
/// Returns an error when cluster state cannot be loaded, the proposed plan is invalid, or a broker, Kubernetes, or persistence operation fails.
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

/// Identity and roles for one node, derived from a `KafkaNodePool` ordinal.
///
/// `pod_fqdn` is the stable in-cluster DNS name in the subdomain of the
/// cluster headless Service. The string is the same whether the pod is
/// scheduled or not, so internal listeners can advertise it before any pod
/// exists.
#[derive(Debug, Clone)]
pub(crate) struct NodeInfo {
    pub broker_id: i32,
    pub pod_name: String,
    pub pod_fqdn: String,
    pub roles: Vec<crate::crd::NodeRole>,
}

impl NodeInfo {
    fn is_broker(&self) -> bool {
        self.roles.contains(&crate::crd::NodeRole::Broker)
    }
}

struct NodeInventory {
    all: Vec<NodeInfo>,
    brokers: Vec<NodeInfo>,
    roles: BTreeMap<i32, Vec<crate::crd::NodeRole>>,
    broker_ids: Vec<i32>,
}

/// Enumerate every pool ordinal as a stable node id and pod DNS name.
pub(crate) fn enumerate_nodes(
    cluster_name: &str,
    namespace: &str,
    pools: &[KafkaNodePool],
) -> Vec<NodeInfo> {
    let svc = format!("{cluster_name}-broker-headless");
    let mut out = Vec::new();
    let mut sorted: Vec<&KafkaNodePool> = pools.iter().collect();
    sorted.sort_by_key(|p| p.name_any());
    for pool in sorted {
        let pool_name = pool.name_any();
        for ordinal in 0..pool.spec.replicas {
            let Some(broker_id) = pool.spec.node_id_start.checked_add(ordinal) else {
                continue;
            };
            let pod_name = format!("{cluster_name}-{pool_name}-{ordinal}");
            let pod_fqdn = format!("{pod_name}.{svc}.{namespace}.svc.cluster.local");
            out.push(NodeInfo {
                broker_id,
                pod_name,
                pod_fqdn,
                roles: pool.spec.roles.clone(),
            });
        }
    }
    out
}

fn inventory_nodes(cluster_name: &str, namespace: &str, pools: &[KafkaNodePool]) -> NodeInventory {
    let all = enumerate_nodes(cluster_name, namespace, pools);
    let brokers: Vec<NodeInfo> = all
        .iter()
        .filter(|node| node.is_broker())
        .cloned()
        .collect();
    let roles = all
        .iter()
        .map(|node| (node.broker_id, node.roles.clone()))
        .collect();
    let broker_ids = brokers.iter().map(|node| node.broker_id).collect();
    NodeInventory {
        all,
        brokers,
        roles,
        broker_ids,
    }
}

fn controller_tls_per_node(nodes: &[NodeInfo]) -> BTreeMap<i32, listeners::BrokerTlsRender> {
    nodes
        .iter()
        .map(|node| {
            let id = node.broker_id;
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
        .collect()
}

/// Builds the per-listener `ListenerStatus` entries.
///
/// Internal listeners report the FQDN of the headless Service. External
/// listeners take a bootstrap host and port from the bootstrap Service
/// that the apiserver returned. This function returns only the entries
/// whose addresses resolved. It omits a listener that still waits for
/// external infrastructure.
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

/// Inner helper for [`build_listener_status`].
///
/// This function holds the per-listener bootstrap-address derivation, so
/// that the body can `?`-chain through the apiserver lookups that return
/// an `Option`.
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

/// Applies the bootstrap objects and the per-broker objects for each
/// external listener.
///
/// Internal listeners need no objects other than the cluster-wide headless
/// Service.
///
/// - `nodeport` and `loadbalancer`: one `NodePort` or `LoadBalancer`
///   Service each.
/// - `ingress` and `route`: one `ClusterIP` backend Service each, and a
///   typed `Ingress` or a dynamic `OpenShift` `Route`. The `Ingress` or
///   `Route` routes the configured hostname to that backend over TLS
///   passthrough.
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
    brokers: &[NodeInfo],
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

/// Returns the single canonical OAuth listener configuration, if there is
/// one.
///
/// `validate_listeners` already rejects per-listener OAuth configurations
/// that differ from each other. The configuration of the first OAuth
/// listener is therefore the canonical one for the whole cluster.
fn canonical_oauth_config(listeners: &[Listener]) -> Option<ListenerAuthenticationOAuth> {
    listeners.iter().find_map(|l| match &l.authentication {
        Some(ListenerAuthentication::OAuth(cfg)) => Some((**cfg).clone()),
        _ => None,
    })
}

/// Computes the name of the managed OAUTHBEARER trust Secret from the
/// listeners of the parent Kafka CR.
///
/// This function returns `Some(name)` when at least one OAuth listener has
/// a non-empty `tls_trusted_certificates`. If not, it returns `None`. The
/// name is deterministic and is always `{kafka}-oauth-jwks-trust`. Two
/// call sites can therefore derive the same name on their own, and neither
/// has to assemble the bundle again. `kafka.rs::reconcile_kafka` upserts
/// the Secret with [`reconcile_oauth_jwks_trust`], and
/// `kafka_node_pool.rs::reconcile` mounts the Secret into the broker pod.
pub(crate) fn oauth_jwks_trust_secret_name(kafka: &Kafka) -> Option<String> {
    let canonical = canonical_oauth_config(&kafka.spec.listeners)?;
    if canonical.tls_trusted_certificates.is_empty() {
        return None;
    }
    Some(format!("{}-oauth-jwks-trust", kafka.name_any()))
}

/// Describes the source Secret that the operator mounts into broker pods
/// for the OAUTHBEARER introspection client-secret.
///
/// [`reconcile_oauth_introspection_secret`] returns it. That function
/// validates, is async, and runs in `reconcile_kafka`.
/// [`oauth_introspection_secret_mount`] derives the same value again from
/// the parent Kafka CR. That function is pure and synchronous, and the
/// pool reconciler uses it to learn what to mount without another fetch
/// from the apiserver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OauthIntrospectionMount {
    pub secret_name: String,
    pub key: String,
}

/// Derives the OAUTHBEARER introspection client-secret mount from the
/// listeners of the parent Kafka CR.
///
/// The result is `Some` when at least one OAuth listener has
/// `accessTokenIsJwt: false` and a `clientSecret` ref. The result is
/// `None` when no OAuth listener uses introspection mode, and also when
/// there is no OAuth listener at all. This function is pure. It derives
/// the same `OauthIntrospectionMount` that the pool reconciler mounts, and
/// it does not read the apiserver. The name and the key of the source
/// Secret live on the CR.
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

/// In-pod mount information for the GSSAPI keytab.
///
/// `key` is the source key of the user. The operator mounts it with
/// projected items to a fixed path, so that the broker reads
/// `/etc/crabka/gssapi-keytab/keytab` whatever the key name is.
pub(crate) struct GssapiKeytabMount {
    pub secret_name: String,
    pub key: String,
}

/// Returns the keytab Secret ref from the first GSSAPI listener, or
/// `None` when no listener is `type: gssapi`.
///
/// Validation guarantees that all GSSAPI listeners agree, so the first one
/// is canonical.
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

/// Builds the managed oauth-jwks-trust Secret from the
/// `tls_trusted_certificates` of the canonical OAuth configuration.
///
/// This function returns the name of the Secret, so that the
/// `StatefulSet` can mount it. It returns `None` when no managed Secret is
/// necessary, which happens when there is no OAuth listener and when no
/// trust certs are configured.
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

/// Applies the managed `{kafka}-oauth-jwks-trust` Secret from the server
/// side, with the concatenated PEM bundle under the key `ca.crt`.
///
/// The Secret has an owner reference to the parent `Kafka`, so a delete of
/// the CR also deletes the Secret.
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

/// Validates that the Secret and the key of the OAUTHBEARER introspection
/// client-secret exist.
///
/// This function upserts no managed Secret. The pod template mounts the
/// source Secret directly with projected items. The function returns the
/// mount information for the `StatefulSet` renderer, or `None` when
/// introspection is not configured. That happens in JWT mode and when
/// there is no oauth listener.
///
/// The `_kafka` argument gives this function the same signature as its
/// sibling, for symmetry at the call sites. This function does not use it,
/// because there is no managed Secret to give an owner reference to. The
/// source Secret stays the property of the user.
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

/// Applies one `OpenShift` `Route` through the dynamic-object path.
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

/// Reads back the cluster Nodes, the broker Pods, and the per-listener
/// Services that the operator just applied.
///
/// The result holds three `HashMap`s and the pod-by-name lookup that the
/// address resolver needs.
///
/// A 404 from one GET gives `Ok(default)` and not an error, and this is on
/// purpose. A Pod that does not exist yet is not an error. It shows as
/// `PodNotScheduled` in `compute_advertised`.
type ExternalState = (
    HashMap<String, Node>,
    HashMap<String, Pod>,
    HashMap<String, Service>,
    HashMap<(String, i32), Service>,
);

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
    brokers: &[NodeInfo],
) -> Result<ExternalState, ReconcileError> {
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

/// Resolves the advertised host and port for each broker and listener
/// pair with [`compute_advertised`].
///
/// This function stops at the first `AdvertisedError`, so that the caller
/// can report one `PendingExternalAddresses` reason instead of a list that
/// changes on every pass.
fn resolve_addresses_per_broker(
    effective_listeners: &[Listener],
    inter_broker_listener_name: &str,
    brokers: &[NodeInfo],
    pods_by_name: &HashMap<String, Pod>,
    nodes: &HashMap<String, Node>,
    broker_services: &HashMap<(String, i32), Service>,
) -> Result<BTreeMap<i32, BTreeMap<String, AdvertisedAddress>>, listeners::AdvertisedError> {
    let mut out: BTreeMap<i32, BTreeMap<String, AdvertisedAddress>> = BTreeMap::new();
    for b in brokers {
        let mut listener_map: BTreeMap<String, AdvertisedAddress> = BTreeMap::new();
        for l in effective_listeners
            .iter()
            .filter(|listener| b.is_broker() || listener.name == inter_broker_listener_name)
        {
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

/// Reads, modifies, and writes the `Kafka` status conditions.
///
/// This function fetches the current status, removes any condition with
/// the same `type_` as `new_cond`, pushes `new_cond`, and patches. It
/// keeps all other status fields, such as `replicas` and `cluster_ca`. A
/// BYO-CA early return therefore does not delete the conditions that an
/// earlier reconcile pass wrote.
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

/// Reconcile entry point.
///
/// This thin wrapper times the pass, records the
/// `reconciliations_total{kind,result}` counter and the
/// `reconcile_duration_seconds` histogram, then calls the internal
/// `reconcile_inner` operation. It is a separate function so that the
/// per-outcome metric classification, ok or error, lives in one place. The
/// many early-return sites in the long inner body then do not each have to
/// record it.
#[tracing::instrument(
    skip_all,
    fields(
        kind = "Kafka",
        namespace = %obj.namespace().unwrap_or_else(|| "default".into()),
        name = %obj.name_any(),
        generation = ?obj.meta().generation,
    )
)]
/// # Errors
/// Returns an error when cluster state cannot be loaded, the proposed plan is invalid, or a broker, Kubernetes, or persistence operation fails.
pub async fn reconcile(obj: Arc<Kafka>, ctx: Arc<Context>) -> Result<Action, ReconcileError> {
    common::record_reconcile(&ctx, "Kafka", Box::pin(reconcile_inner(obj, ctx.clone()))).await
}

struct CaPhaseInput<'a> {
    obj: &'a Kafka,
    ctx: &'a Context,
    namespace: &'a str,
    name: &'a str,
    secret_api: &'a Api<Secret>,
    rollout_converged: bool,
    explicit_pin: Option<&'a str>,
    logging_filter: Option<&'a str>,
}

struct CaArtifacts {
    cluster: cluster_ca::CaReconcileOutcome,
    clients: cluster_ca::CaReconcileOutcome,
    cluster_condition: KafkaCondition,
    clients_condition: KafkaCondition,
    rotation_condition: KafkaCondition,
    config_hash: String,
}

enum CaPhaseResult {
    Ready(Box<CaArtifacts>),
    Done(Action),
}

struct ListenerPhaseInput<'a> {
    obj: &'a Kafka,
    ctx: &'a Context,
    namespace: &'a str,
    name: &'a str,
    effective_listeners: &'a [Listener],
    inter_broker_name: &'a str,
    logging_filter: Option<&'a str>,
    service_api: &'a Api<Service>,
    config_map_api: &'a Api<ConfigMap>,
    secret_api: &'a Api<Secret>,
    pool_api: &'a Api<KafkaNodePool>,
    pools: &'a [KafkaNodePool],
    config_hash: &'a str,
    cluster_ca: &'a cluster_ca::CaReconcileOutcome,
}

struct ListenerArtifacts {
    status: Vec<ListenerStatus>,
    valid_condition: KafkaCondition,
    ready_condition: KafkaCondition,
    load_balancer_pending: Vec<(i32, String)>,
}

enum ListenerPhaseResult {
    Ready(Box<ListenerArtifacts>),
    Done(Action),
}

struct FinalizeKafkaInput<'a> {
    obj: &'a Kafka,
    ctx: &'a Context,
    namespace: &'a str,
    name: &'a str,
    effective_listeners: &'a [Listener],
    inter_broker_name: &'a str,
    pools: &'a [KafkaNodePool],
    listener_status: Vec<ListenerStatus>,
    listeners_valid_condition: KafkaCondition,
    listeners_ready_condition: KafkaCondition,
    load_balancer_pending: Vec<(i32, String)>,
    cluster_ca: &'a cluster_ca::CaReconcileOutcome,
    clients_ca: &'a cluster_ca::CaReconcileOutcome,
    cluster_ca_condition: KafkaCondition,
    clients_ca_condition: KafkaCondition,
    ca_rotation_condition: KafkaCondition,
    version_condition: KafkaCondition,
    logging_condition: KafkaCondition,
    resolved_metadata: Option<String>,
    finalized_metadata: Option<&'a str>,
}

// linear pipeline; the three branches (invalid / pending / ready) need direct condition + status binding
async fn reconcile_cas(input: CaPhaseInput<'_>) -> Result<CaPhaseResult, ReconcileError> {
    let CaPhaseInput {
        obj,
        ctx,
        namespace: ns,
        name,
        secret_api,
        rollout_converged,
        explicit_pin,
        logging_filter,
    } = input;
    // Reconcile both CAs with rotation. The cluster CA drives the
    // staged key-replacement machine + the config-hash. The clients CA uses
    // the same trust-first machine, then reissues every managed TLS user before
    // its old trust anchor can be pruned. On BYO-missing, surface a False
    // condition and requeue.
    let cr_anns = obj.meta().annotations.clone().unwrap_or_default();
    let force_renew = cr_anns.contains_key(cluster_ca::ANN_FORCE_RENEW)
        || cr_anns.contains_key(cluster_ca::ANN_RENEW_AFTER);
    let force_replace_key = cr_anns.contains_key(cluster_ca::ANN_FORCE_REPLACE_KEY);
    let force_replace_clients_key = cr_anns.contains_key(cluster_ca::ANN_FORCE_REPLACE_CLIENTS_KEY);
    let now = time::OffsetDateTime::now_utc();

    let (cluster_ca_outcome, clients_ca_outcome, cluster_ca_cond, clients_ca_cond) = {
        let cluster_result = cluster_ca::reconcile_ca(
            secret_api,
            obj,
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
                let kafka_api: Api<Kafka> = Api::namespaced(ctx.client.clone(), ns);
                patch_status_with_condition(&kafka_api, name, cond).await?;
                return Ok(CaPhaseResult::Done(common::requeue(
                    ctx.config.controller_drift_requeue,
                )));
            }
            Err(e) => return Err(e),
        };
        let clients_result = cluster_ca::reconcile_ca(
            secret_api,
            obj,
            cluster_ca::WhichCa::Clients,
            false,
            force_replace_clients_key,
            rollout_converged,
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
                let kafka_api: Api<Kafka> = Api::namespaced(ctx.client.clone(), ns);
                patch_status_with_condition(&kafka_api, name, cond).await?;
                return Ok(CaPhaseResult::Done(common::requeue(
                    ctx.config.controller_drift_requeue,
                )));
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

    // Generation counters make convergence retryable after a partial failure:
    // once this CA has ever rotated, each Kafka reconcile cheaply verifies all
    // managed TLS-user Secrets. A prune may patch the CA Secret successfully
    // and then fail on one user; the next pass is still required to repair it
    // even though the one-pass `pruned_old_trust` signal is gone.
    let sync_tls_user_secrets = clients_ca_outcome.leaf_transition.requires_reissue()
        || clients_ca_outcome.leaf_transition.pruned_old_trust()
        || clients_ca_outcome.cert_generation.0 > 0
        || clients_ca_outcome.key_generation.0 > 0;
    if sync_tls_user_secrets {
        let user_api: Api<KafkaUser> = Api::namespaced(ctx.client.clone(), ns);
        let users = user_api
            .list(&ListParams::default().labels(&format!("crabka.io/cluster={name}")))
            .await?;
        let issued = user_tls::reissue_tls_user_cert_secrets(
            secret_api,
            &users.items,
            &clients_ca_outcome.signing_material,
            &clients_ca_outcome.trust_bundle_pem,
        )
        .await?;
        if clients_ca_outcome.leaf_transition.requires_reissue() {
            cluster_ca::mark_leafs_reissued(secret_api, name, clients_ca_outcome.key_generation)
                .await?;
        }
        tracing::info!(issued, "clients CA user Secret convergence complete");
    }

    // Strip the one-shot rotation-trigger annotations once consumed (force
    // renew/replace + CronJob nudge are all acted on this pass).
    let strip_keys: Vec<&str> = [
        cluster_ca::ANN_FORCE_RENEW,
        cluster_ca::ANN_FORCE_REPLACE_KEY,
        cluster_ca::ANN_FORCE_REPLACE_CLIENTS_KEY,
        cluster_ca::ANN_RENEW_AFTER,
    ]
    .into_iter()
    .filter(|k| cr_anns.contains_key(*k))
    .collect();
    if !strip_keys.is_empty() {
        let kafka_api: Api<Kafka> = Api::namespaced(ctx.client.clone(), ns);
        strip_annotations(&kafka_api, name, &strip_keys).await?;
    }

    // A forced rotation the operator can't honor (BYO) surfaces a Warning
    // Event; the condition explains it too.
    if cluster_ca_outcome.refused.is_some() {
        emit_ca_rotation_refused_event(&ctx.client, ns, obj, &cluster_ca_outcome.rotation_message)
            .await
            .ok();
    }
    if clients_ca_outcome.refused.is_some() {
        emit_ca_rotation_refused_event(&ctx.client, ns, obj, &clients_ca_outcome.rotation_message)
            .await
            .ok();
    }

    let cluster_rotation_visible = cluster_ca_outcome.rotation_in_progress
        || cluster_ca_outcome.refused.is_some()
        || cluster_ca_outcome.rotation_message != "no rotation in progress";
    let clients_rotation_visible = clients_ca_outcome.rotation_in_progress
        || clients_ca_outcome.refused.is_some()
        || clients_ca_outcome.rotation_message != "no rotation in progress";
    let rotation_outcome = if !cluster_rotation_visible && clients_rotation_visible {
        &clients_ca_outcome
    } else {
        &cluster_ca_outcome
    };
    let ca_rotation_cond = condition(
        "CaRotation",
        if rotation_outcome.rotation_in_progress {
            "True"
        } else {
            "False"
        },
        rotation_outcome.rotation_reason,
        &rotation_outcome.rotation_message,
    );

    // During clients-CA key replacement, include that trust bundle in the
    // roll gate before any user certificate switches to the new signing key.
    // Idle clients-CA renewals remain hot-reload-only.
    let ca_trust = if clients_ca_outcome.phase == cluster_ca::CaPhase::Idle {
        cluster_ca_outcome.trust_bundle_pem.clone()
    } else {
        format!(
            "{}\x1E{}",
            cluster_ca_outcome.trust_bundle_pem, clients_ca_outcome.trust_bundle_pem
        )
    };
    let cfg_hash =
        common::combined_config_hash(&obj.spec, Some(&ca_trust), explicit_pin, logging_filter);

    Ok(CaPhaseResult::Ready(Box::new(CaArtifacts {
        cluster: cluster_ca_outcome,
        clients: clients_ca_outcome,
        cluster_condition: cluster_ca_cond,
        clients_condition: clients_ca_cond,
        rotation_condition: ca_rotation_cond,
        config_hash: cfg_hash,
    })))
}

struct ListenerDependencyInput<'a> {
    obj: &'a Kafka,
    ctx: &'a Context,
    namespace: &'a str,
    name: &'a str,
    listeners: &'a [Listener],
    inter_broker_name: &'a str,
    secret_api: &'a Api<Secret>,
}

async fn validate_listener_dependencies(
    input: ListenerDependencyInput<'_>,
) -> Result<Option<Action>, ReconcileError> {
    let ListenerDependencyInput {
        obj,
        ctx,
        namespace: ns,
        name,
        listeners: effective_listeners,
        inter_broker_name,
        secret_api,
    } = input;
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
    let oauth_canonical = canonical_oauth_config(effective_listeners);
    match reconcile_oauth_jwks_trust(secret_api, obj, oauth_canonical.as_ref()).await {
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
            let kafka_api: Api<Kafka> = Api::namespaced(ctx.client.clone(), ns);
            patch_status_with_condition(&kafka_api, name, cond).await?;
            return Ok(Some(common::requeue(
                ctx.config.controller_dependency_requeue,
            )));
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
    match reconcile_oauth_introspection_secret(secret_api, obj, oauth_canonical.as_ref()).await {
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
            let kafka_api: Api<Kafka> = Api::namespaced(ctx.client.clone(), ns);
            patch_status_with_condition(&kafka_api, name, cond).await?;
            return Ok(Some(common::requeue(
                ctx.config.controller_dependency_requeue,
            )));
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
        if let Some(m) = gssapi_keytab_mount(obj) {
            let secret = secret_api
                .get_opt(&m.secret_name)
                .await?
                .ok_or_else(|| ReconcileError::MissingGssapiKeytabSecret(m.secret_name.clone()))?;
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
        if let Some((secret_name, key)) = krb5_conf_mount(obj) {
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
            let kafka_api: Api<Kafka> = Api::namespaced(ctx.client.clone(), ns);
            patch_status_with_condition(&kafka_api, name, cond).await?;
            return Ok(Some(common::requeue(
                ctx.config.controller_dependency_requeue,
            )));
        }
        Err(e) => return Err(e),
    }

    // Inter-broker Kerberos: when the resolved inter-broker listener
    // uses GSSAPI, `spec.interBrokerKerberos` must be present (the
    // broker needs initiate-side credentials). Surface a failure the
    // same way `validate_listeners` does — `ListenersValid=False` with
    // the `ValidationError`'s `reason()`/`message()`.
    if let Err(e) = listeners::validate_inter_broker_gssapi(
        effective_listeners,
        inter_broker_name,
        obj.spec.inter_broker_kerberos.is_some(),
    ) {
        let cond = condition("ListenersValid", "False", e.reason(), &e.message());
        let kafka_api: Api<Kafka> = Api::namespaced(ctx.client.clone(), ns);
        patch_status_with_condition(&kafka_api, name, cond).await?;
        return Ok(Some(common::requeue(
            ctx.config.controller_dependency_requeue,
        )));
    }

    Ok(None)
}

async fn reconcile_listener_resources(
    input: ListenerPhaseInput<'_>,
    validation: Result<(), listeners::ValidationError>,
) -> Result<ListenerPhaseResult, ReconcileError> {
    if let Err(e) = validation {
        adopt_pools(
            input.pool_api,
            input.obj,
            input.pools.iter(),
            input.config_hash,
        )
        .await?;
        return Ok(ListenerPhaseResult::Ready(Box::new(ListenerArtifacts {
            status: vec![],
            valid_condition: condition("ListenersValid", "False", e.reason(), &e.message()),
            ready_condition: condition("ListenersReady", "False", "ListenersInvalid", &e.message()),
            load_balancer_pending: vec![],
        })));
    }

    if let Some(action) = validate_listener_dependencies(ListenerDependencyInput {
        obj: input.obj,
        ctx: input.ctx,
        namespace: input.namespace,
        name: input.name,
        listeners: input.effective_listeners,
        inter_broker_name: input.inter_broker_name,
        secret_api: input.secret_api,
    })
    .await?
    {
        return Ok(ListenerPhaseResult::Done(action));
    }

    reconcile_valid_listener_resources(input)
        .await
        .map(|artifacts| ListenerPhaseResult::Ready(Box::new(artifacts)))
}

struct ListenerTlsArtifacts {
    inventory: NodeInventory,
    per_node: BTreeMap<i32, listeners::BrokerTlsRender>,
    clients_ca_path: Option<&'static str>,
    load_balancer_pending: Vec<(i32, String)>,
}

async fn prepare_listener_tls(
    input: &ListenerPhaseInput<'_>,
) -> Result<ListenerTlsArtifacts, ReconcileError> {
    let inventory = inventory_nodes(input.name, input.namespace, input.pools);
    let observed = listeners::observe_listener_addresses(
        input.ctx,
        input.namespace,
        input.name,
        input.effective_listeners,
        &inventory.broker_ids,
    )
    .await?;
    let mut load_balancer_pending = Vec::new();
    let extra_sans: BTreeMap<i32, Vec<crabka_security::ca::SubjectAltName>> = inventory
        .brokers
        .iter()
        .filter_map(|broker| {
            match listeners::compute_extra_sans(
                broker.broker_id,
                input.effective_listeners,
                &observed,
            ) {
                Ok(sans) => Some((broker.broker_id, sans)),
                Err(listeners::SanComputationError::SansNotReady {
                    broker_id,
                    listener,
                }) => {
                    tracing::warn!(
                        broker_id,
                        %listener,
                        "LB ingress not ready; skipping cert SAN extension for this broker"
                    );
                    load_balancer_pending.push((broker_id, listener));
                    None
                }
            }
        })
        .collect();
    let requests = inventory
        .all
        .iter()
        .map(|node| cluster_ca::BrokerCertRequest {
            broker_id: node.broker_id,
            cn: node.pod_name.clone(),
            sans: vec![
                crabka_security::ca::SubjectAltName::Dns(node.pod_fqdn.clone()),
                crabka_security::ca::SubjectAltName::Dns(node.pod_name.clone()),
                crabka_security::ca::SubjectAltName::Dns(format!(
                    "{}-broker-headless.{}.svc.cluster.local",
                    input.name, input.namespace
                )),
                crabka_security::ca::SubjectAltName::Ip(std::net::IpAddr::V4(
                    std::net::Ipv4Addr::LOCALHOST,
                )),
            ],
            extra_sans: extra_sans.get(&node.broker_id).cloned().unwrap_or_default(),
        })
        .collect::<Vec<_>>();
    cluster_ca::ensure_broker_keystore(
        input.secret_api,
        input.obj,
        &requests,
        &input.cluster_ca.signing_material,
        input.cluster_ca.leaf_transition.requires_reissue(),
    )
    .await?;
    let clients_ca_path = input
        .effective_listeners
        .iter()
        .any(|listener| matches!(listener.authentication, Some(ListenerAuthentication::Tls)))
        .then_some("/etc/crabka/clients-ca/ca.crt");
    Ok(ListenerTlsArtifacts {
        per_node: controller_tls_per_node(&inventory.all),
        inventory,
        clients_ca_path,
        load_balancer_pending,
    })
}

async fn load_external_listener_state(
    input: &ListenerPhaseInput<'_>,
    inventory: &NodeInventory,
) -> Result<ExternalState, ReconcileError> {
    if !input
        .effective_listeners
        .iter()
        .any(|listener| listener.type_ != ListenerType::Internal)
    {
        return Ok(Default::default());
    }
    apply_external_services(
        input.ctx,
        input.service_api,
        input.obj,
        input.namespace,
        input.name,
        input.effective_listeners,
        &inventory.brokers,
    )
    .await?;
    read_external_state(
        input.ctx,
        input.service_api,
        input.namespace,
        input.name,
        input.effective_listeners,
        &inventory.brokers,
    )
    .await
}

async fn reconcile_valid_listener_resources(
    input: ListenerPhaseInput<'_>,
) -> Result<ListenerArtifacts, ReconcileError> {
    let tls = prepare_listener_tls(&input).await?;
    let (nodes, pods, bootstrap_services, broker_services) =
        load_external_listener_state(&input, &tls.inventory).await?;
    let resolved = resolve_addresses_per_broker(
        input.effective_listeners,
        input.inter_broker_name,
        &tls.inventory.all,
        &pods,
        &nodes,
        &broker_services,
    );
    let valid = condition("ListenersValid", "True", "Valid", "listeners validated");
    let (status, ready) = match resolved {
        Err(err) => (
            vec![],
            condition(
                "ListenersReady",
                "False",
                "PendingExternalAddresses",
                &err.message(),
            ),
        ),
        Ok(addresses) => {
            let cm = common::render_configmap(
                input.obj,
                input.effective_listeners,
                (&addresses, &tls.inventory.roles),
                input.inter_broker_name,
                Some(&tls.per_node),
                tls.clients_ca_path,
                input.logging_filter,
            )?;
            apply_object(input.config_map_api, &cm_name(input.name), &cm).await?;
            let broker_addresses = addresses
                .into_iter()
                .filter(|(id, _)| tls.inventory.broker_ids.contains(id))
                .collect();
            let status = build_listener_status(
                input.effective_listeners,
                &broker_addresses,
                &bootstrap_services,
                &nodes,
                input.name,
                input.namespace,
            );
            let message = format!("{} listener(s) ready", input.effective_listeners.len());
            (
                status,
                condition("ListenersReady", "True", "Ready", &message),
            )
        }
    };
    adopt_pools(
        input.pool_api,
        input.obj,
        input.pools.iter(),
        input.config_hash,
    )
    .await?;
    Ok(ListenerArtifacts {
        status,
        valid_condition: valid,
        ready_condition: ready,
        load_balancer_pending: tls.load_balancer_pending,
    })
}

async fn finalize_kafka(input: FinalizeKafkaInput<'_>) -> Result<Action, ReconcileError> {
    let FinalizeKafkaInput {
        obj,
        ctx,
        namespace: ns,
        name,
        effective_listeners,
        inter_broker_name,
        pools,
        listener_status,
        listeners_valid_condition: listeners_valid_cond,
        listeners_ready_condition: listeners_ready_cond,
        load_balancer_pending: lb_pending,
        cluster_ca: cluster_ca_outcome,
        clients_ca: clients_ca_outcome,
        cluster_ca_condition: cluster_ca_cond,
        clients_ca_condition: clients_ca_cond,
        ca_rotation_condition: ca_rotation_cond,
        version_condition: version_cond,
        logging_condition,
        resolved_metadata,
        finalized_metadata,
    } = input;
    // Metrics resources: surface a MetricsReady condition regardless of
    // whether spec.metricsConfig is set. MutuallyExclusive and
    // PrometheusOperatorCrdsMissing are reported via the condition only —
    // the reconcile continues so the rest of the status patch lands.
    let metrics_outcome = crate::controller::metrics::reconcile_metrics(ctx, obj, name, ns).await;
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
        ctx,
        obj,
        name,
        ns,
        effective_listeners,
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
    let kafka_api: Api<Kafka> = Api::namespaced(ctx.client.clone(), ns);
    patch_status::<Kafka, KafkaStatus>(&kafka_api, name, status).await?;

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

    Ok(common::requeue(ctx.config.controller_dependency_requeue))
}

async fn validate_kafka_runtime(
    obj: &Kafka,
    ctx: &Context,
    namespace: &str,
    name: &str,
) -> Result<(), ReconcileError> {
    let kafka_api: Api<Kafka> = Api::namespaced(ctx.client.clone(), namespace);
    if let Some(tuning) = &obj.spec.broker_tuning
        && let Err(why) = tuning.validate()
    {
        let cond = condition("KafkaConfigValid", "False", "KafkaConfigInvalid", &why);
        patch_status_with_condition(&kafka_api, name, cond).await?;
        return Err(ReconcileError::KafkaConfigInvalid(why));
    }
    if let Some(tiered_storage) = &obj.spec.tiered_storage {
        let (condition, error) = match tiered_storage.validate() {
            Ok(()) => (
                condition(
                    "TieredStorageReady",
                    "True",
                    "Validated",
                    "tieredStorage spec is well-formed",
                ),
                None,
            ),
            Err(why) => (
                condition(
                    "TieredStorageReady",
                    "False",
                    "TieredStorageInvalid",
                    &format!("tieredStorage: {why}"),
                ),
                Some(why),
            ),
        };
        patch_status_with_condition(&kafka_api, name, condition).await?;
        if let Some(why) = error {
            return Err(ReconcileError::TieredStorageInvalid(why));
        }
    }
    if let Some(tracing) = &obj.spec.tracing {
        let (condition, error) = match tracing.validate() {
            Ok(()) => (
                condition(
                    "TracingReady",
                    "True",
                    "Validated",
                    "tracing spec is well-formed",
                ),
                None,
            ),
            Err(why) => (
                condition(
                    "TracingReady",
                    "False",
                    "TracingInvalid",
                    &format!("tracing: {why}"),
                ),
                Some(why),
            ),
        };
        patch_status_with_condition(&kafka_api, name, condition).await?;
        if let Some(why) = error {
            return Err(ReconcileError::TracingInvalid(why));
        }
    }
    Ok(())
}

fn evaluate_kafka_version(obj: &Kafka) -> (KafkaCondition, Option<String>) {
    let finalized = obj
        .status
        .as_ref()
        .and_then(|status| status.metadata_version.as_deref());
    match crate::version::evaluate(
        &obj.spec.kafka_version,
        obj.spec.metadata_version.as_deref(),
        finalized,
    ) {
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
            Some(resolved_metadata),
        ),
        crate::version::VersionOutcome::Invalid { reason, message } => (
            condition("KafkaVersionValid", "False", reason.as_str(), &message),
            None,
        ),
    }
}

fn metadata_version_level(version: &str) -> Option<i16> {
    crabka_metadata::metadata_version::from_version_string(version)
        .map(crabka_metadata::metadata_version::MetadataVersion::feature_level)
}

async fn reconcile_metadata_version(
    ctx: &Context,
    namespace: &str,
    name: &str,
    port: i32,
    resolved: Option<&str>,
    finalized: Option<&str>,
    timeout: Time,
) -> Result<Option<String>, KafkaCondition> {
    let Some(resolved) = resolved else {
        return Ok(None);
    };
    let Some(finalized) = finalized else {
        return Ok(Some(resolved.to_string()));
    };
    let Some(target_level) = metadata_version_level(resolved) else {
        return Ok(None);
    };
    let Some(finalized_level) = metadata_version_level(finalized) else {
        return Ok(None);
    };
    if target_level == finalized_level {
        return Ok(Some(resolved.to_string()));
    }

    let bootstrap = format!("{name}-broker-headless.{namespace}.svc.cluster.local:{port}");
    let admin = ctx
        .admin_client_for(name, &bootstrap)
        .await
        .map_err(|error| {
            condition(
                "KafkaVersionValid",
                "False",
                "MetadataVersionUpdateFailed",
                &format!("UpdateFeatures connection failed: {error}"),
            )
        })?;
    let mut admin = admin.lock().await;
    admin
        .update_metadata_version(target_level, target_level < finalized_level, timeout)
        .await
        .map_err(|error| {
            condition(
                "KafkaVersionValid",
                "False",
                "MetadataVersionUpdateFailed",
                &format!("UpdateFeatures rejected metadata.version {resolved}: {error}"),
            )
        })?;
    Ok(Some(resolved.to_string()))
}

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
    validate_kafka_runtime(&obj, &ctx, &ns, &name).await?;
    let finalized_metadata = obj
        .status
        .as_ref()
        .and_then(|s| s.metadata_version.as_deref());
    let (version_cond, resolved_metadata) = evaluate_kafka_version(&obj);
    let inter_broker_port = effective_listeners
        .iter()
        .find(|listener| listener.name == inter_broker_name)
        .map_or(common::BROKER_PORT, |listener| listener.port);
    let (version_cond, resolved_metadata) = match reconcile_metadata_version(
        &ctx,
        &ns,
        &name,
        inter_broker_port,
        resolved_metadata.as_deref(),
        finalized_metadata,
        secs(30),
    )
    .await
    {
        Ok(resolved) => (version_cond, resolved),
        Err(condition) => (condition, None),
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
    let sts_api: Api<StatefulSet> = Api::namespaced(ctx.client.clone(), &ns);
    let statefulsets = sts_api
        .list(&ListParams::default().labels(&format!(
            "app.kubernetes.io/instance={name},app.kubernetes.io/name={}",
            common::APP_LABEL
        )))
        .await?;
    let topology_pod_api: Api<Pod> = Api::namespaced(ctx.client.clone(), &ns);
    let topology_pods = topology_pod_api
        .list(&ListParams::default().labels(&format!(
            "app.kubernetes.io/instance={name},app.kubernetes.io/name={}",
            common::APP_LABEL
        )))
        .await?;
    if let Err(error) =
        kafka_node_pool::validate_topology(&pools.items, &statefulsets.items, &topology_pods.items)
    {
        let kafka_api: Api<Kafka> = Api::namespaced(ctx.client.clone(), &ns);
        patch_status_with_condition(
            &kafka_api,
            &name,
            condition(
                "NodePoolTopologyValid",
                "False",
                "InvalidNodePoolTopology",
                &error.to_string(),
            ),
        )
        .await?;
        return Ok(common::requeue(ctx.config.controller_dependency_requeue));
    }
    let rollout_converged = pools_converged(pools.iter(), statefulsets.iter());

    let CaArtifacts {
        cluster: cluster_ca_outcome,
        clients: clients_ca_outcome,
        cluster_condition: cluster_ca_cond,
        clients_condition: clients_ca_cond,
        rotation_condition: ca_rotation_cond,
        config_hash: cfg_hash,
    } = match reconcile_cas(CaPhaseInput {
        obj: &obj,
        ctx: &ctx,
        namespace: &ns,
        name: &name,
        secret_api: &secret_api,
        rollout_converged,
        explicit_pin,
        logging_filter: logging_filter.as_deref(),
    })
    .await?
    {
        CaPhaseResult::Ready(artifacts) => *artifacts,
        CaPhaseResult::Done(action) => return Ok(action),
    };

    let ListenerArtifacts {
        status: listener_status,
        valid_condition: listeners_valid_cond,
        ready_condition: listeners_ready_cond,
        load_balancer_pending: lb_pending,
    } = match reconcile_listener_resources(
        ListenerPhaseInput {
            obj: &obj,
            ctx: &ctx,
            namespace: &ns,
            name: &name,
            effective_listeners: &effective_listeners,
            inter_broker_name: &inter_broker_name,
            logging_filter: logging_filter.as_deref(),
            service_api: &svc_api,
            config_map_api: &cm_api,
            secret_api: &secret_api,
            pool_api: &pool_api,
            pools: &pools.items,
            config_hash: &cfg_hash,
            cluster_ca: &cluster_ca_outcome,
        },
        validation,
    )
    .await?
    {
        ListenerPhaseResult::Ready(artifacts) => *artifacts,
        ListenerPhaseResult::Done(action) => return Ok(action),
    };

    finalize_kafka(FinalizeKafkaInput {
        obj: &obj,
        ctx: &ctx,
        namespace: &ns,
        name: &name,
        effective_listeners: &effective_listeners,
        inter_broker_name: &inter_broker_name,
        pools: &pools.items,
        listener_status,
        listeners_valid_condition: listeners_valid_cond,
        listeners_ready_condition: listeners_ready_cond,
        load_balancer_pending: lb_pending,
        cluster_ca: &cluster_ca_outcome,
        clients_ca: &clients_ca_outcome,
        cluster_ca_condition: cluster_ca_cond,
        clients_ca_condition: clients_ca_cond,
        ca_rotation_condition: ca_rotation_cond,
        version_condition: version_cond,
        logging_condition,
        resolved_metadata,
        finalized_metadata,
    })
    .await
}

/// Patches every pool with the label `crabka.io/cluster=<this Kafka>`.
///
/// The patch sets `metadata.ownerReferences`, so that the Kafka is the
/// controlling owner, AND
/// `metadata.labels["crabka.io/config-hash"]`, so that the pool
/// reconciler sees config drift. This function uses a server-side apply
/// with the field manager of the operator, so the patch wins over any
/// manual edit from outside.
///
/// [`common::plan_rollout`] plans the per-pool hash. An established
/// multi-pool cluster therefore rolls one node at a time, in the order
/// `(node_id_start, name)`, and each pool must reach Ready before the next
/// one rolls. The initial bring-up still applies the hash to every pool in
/// parallel, because a `KRaft` controller quorum needs all controllers up
/// together. Every reconcile applies the owner reference to every pool in
/// both cases, so the request count does not change.
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
                >= p.spec.replicas,
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

pub fn error_policy(_obj: Arc<Kafka>, err: &ReconcileError, ctx: Arc<Context>) -> Action {
    tracing::warn!(error = %err, "reconcile error, requeueing");
    common::error_requeue(ctx)
}

/// Emits a `Warning` Kubernetes Event on the `Kafka` object for a listener
/// that carries SCRAM authentication without transport TLS.
///
/// The caller calls `.ok()` on the result and does not wait for it. A
/// transient API error should not block the rest of the reconcile.
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
        crate::controller::cluster_ca::EventDetails {
            type_: "Warning",
            reason: "WeakAuth",
            message,
            generate_name: "crabka-listener-auth-",
            action: "ListenerValidation",
            reporting_component: "crabka-operator/listener-auth-check",
        },
    )
    .await
}

/// Emits a Warning Event when the operator cannot do a forced CA
/// rotation.
///
/// BYO CAs are immutable, so a force annotation is rejected. The call site
/// does not wait for the Event result.
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
        crate::controller::cluster_ca::EventDetails {
            type_: "Warning",
            reason: "CaRotationRefused",
            message,
            generate_name: "crabka-ca-rotation-",
            action: "CaRotation",
            reporting_component: "crabka-operator/ca-rotation",
        },
    )
    .await
}

/// Reports whether no roll is in flight.
///
/// No roll is in flight when every pool carries the same non-empty
/// `crabka.io/config-hash` label and its live `StatefulSet` has observed that
/// hash, completed its current revision, and made every desired replica both
/// updated and Ready.
/// The CA rotation state machine advances a staged phase only in this
/// condition. Trust distribution therefore finishes before the operator
/// promotes the new key, and the promotion finishes before the operator
/// prunes the old anchor. An empty pool list counts as converged.
pub(crate) fn pools_converged<'a, 'b>(
    pools: impl IntoIterator<Item = &'a KafkaNodePool>,
    statefulsets: impl IntoIterator<Item = &'b StatefulSet>,
) -> bool {
    let mut statefulsets_by_name: HashMap<String, &StatefulSet> = HashMap::new();
    for statefulset in statefulsets {
        if statefulsets_by_name
            .insert(statefulset.name_any(), statefulset)
            .is_some()
        {
            return false;
        }
    }
    let mut hashes = std::collections::BTreeSet::new();
    let mut any = false;
    for p in pools {
        any = true;
        let h = p
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("crabka.io/config-hash").cloned());
        hashes.insert(h.clone());
        let Some(hash) = h else { return false };
        if hash.is_empty() {
            return false;
        }
        let cluster = p
            .metadata
            .labels
            .as_ref()
            .and_then(|labels| labels.get("crabka.io/cluster"));
        let Some(cluster) = cluster else { return false };
        let pool_name = p.name_any();
        let Some(statefulset) = statefulsets_by_name.get(&format!("{cluster}-{pool_name}")) else {
            return false;
        };
        let statefulset_labels = statefulset.metadata.labels.as_ref();
        if statefulset_labels
            .and_then(|labels| labels.get("app.kubernetes.io/instance"))
            .map(String::as_str)
            != Some(cluster.as_str())
            || statefulset_labels
                .and_then(|labels| labels.get("app.kubernetes.io/name"))
                .map(String::as_str)
                != Some(common::APP_LABEL)
            || statefulset_labels
                .and_then(|labels| labels.get("crabka.io/pool"))
                .map(String::as_str)
                != Some(pool_name.as_str())
        {
            return false;
        }
        let Some(spec) = statefulset.spec.as_ref() else {
            return false;
        };
        let Some(status) = statefulset.status.as_ref() else {
            return false;
        };
        let template_hash = spec
            .template
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.annotations.as_ref())
            .and_then(|annotations| annotations.get("crabka.io/config-hash"));
        let desired = p.spec.replicas;
        let Some(generation) = statefulset.metadata.generation else {
            return false;
        };
        if template_hash != Some(&hash)
            || spec.replicas != Some(desired)
            || status.observed_generation != Some(generation)
            || status.current_revision.is_none()
            || status.current_revision != status.update_revision
            || status.updated_replicas.unwrap_or_default() != desired
            || status.ready_replicas.unwrap_or_default() != desired
        {
            return false;
        }
    }
    !any || hashes.len() == 1
}

/// Removes the one-shot rotation-trigger annotations from the `Kafka` CR.
///
/// A JSON Merge Patch with `null` values deletes the keys.
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
                replicas,
                node_id_start: 0,
                image: None,
                resources: None,
                client_dispatch_queue_capacity: None,
                client_frame_max: None,
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
    fn enumerate_nodes_expands_ordinals_with_roles_and_dns() {
        let mut controllers = pool_with_status("controllers", 0, 0);
        controllers.spec.roles = vec![NodeRole::Controller];
        controllers.spec.replicas = 3;
        controllers.spec.node_id_start = 10;
        let mut brokers = pool_with_status("brokers", 0, 0);
        brokers.spec.roles = vec![NodeRole::Broker];
        brokers.spec.replicas = 2;
        brokers.spec.node_id_start = 20;

        let nodes = enumerate_nodes("demo", "ns", &[controllers, brokers]);
        let actual: Vec<_> = nodes
            .iter()
            .map(|node| {
                (
                    node.broker_id,
                    node.pod_name.as_str(),
                    node.pod_fqdn.as_str(),
                    node.roles.as_slice(),
                )
            })
            .collect();
        assert!(
            actual
                == vec![
                    (
                        20,
                        "demo-brokers-0",
                        "demo-brokers-0.demo-broker-headless.ns.svc.cluster.local",
                        &[NodeRole::Broker][..],
                    ),
                    (
                        21,
                        "demo-brokers-1",
                        "demo-brokers-1.demo-broker-headless.ns.svc.cluster.local",
                        &[NodeRole::Broker][..],
                    ),
                    (
                        10,
                        "demo-controllers-0",
                        "demo-controllers-0.demo-broker-headless.ns.svc.cluster.local",
                        &[NodeRole::Controller][..],
                    ),
                    (
                        11,
                        "demo-controllers-1",
                        "demo-controllers-1.demo-broker-headless.ns.svc.cluster.local",
                        &[NodeRole::Controller][..],
                    ),
                    (
                        12,
                        "demo-controllers-2",
                        "demo-controllers-2.demo-broker-headless.ns.svc.cluster.local",
                        &[NodeRole::Controller][..],
                    ),
                ]
        );
    }

    #[test]
    fn aggregate_status_no_pools_is_no_node_pools() {
        let r = aggregate_pool_status(std::iter::empty::<&KafkaNodePool>());
        let (ready, reason, _) = rollup_condition(&r);
        assert!(!ready);
        assert!(reason == "NoNodePools");
    }

    #[test]
    fn aggregate_status_partial_pool_is_partially_ready() {
        let p = pool_with_status("brokers", 3, 1);
        let r = aggregate_pool_status([&p]);
        let (ready, reason, _) = rollup_condition(&r);
        assert!(!ready);
        assert!(reason == "PartiallyReady");
    }

    #[test]
    fn aggregate_status_all_ready_pools_is_available() {
        let p = pool_with_status("brokers", 1, 1);
        let r = aggregate_pool_status([&p]);
        let (ready, reason, _) = rollup_condition(&r);
        assert!(ready);
        assert!(reason == "Available");
    }

    #[test]
    fn ca_rollout_gate_rejects_stale_ready_revision_then_accepts_convergence() {
        let mut p = pool_with_status("controllers", 3, 3);
        p.metadata.labels = Some(BTreeMap::from([
            ("crabka.io/config-hash".into(), "new-hash".into()),
            ("crabka.io/cluster".into(), "demo".into()),
        ]));
        let mut statefulset = StatefulSet::default();
        statefulset.metadata.name = Some("demo-controllers".into());
        statefulset.metadata.generation = Some(2);
        statefulset.metadata.labels = Some(BTreeMap::from([
            ("app.kubernetes.io/instance".into(), "demo".into()),
            ("app.kubernetes.io/name".into(), common::APP_LABEL.into()),
            ("crabka.io/pool".into(), "controllers".into()),
        ]));
        statefulset.spec = Some(
            serde_json::from_value(json!({
                "serviceName": "demo-broker-headless",
                "replicas": 3,
                "selector": { "matchLabels": {} },
                "template": {
                    "metadata": { "annotations": { "crabka.io/config-hash": "new-hash" } },
                    "spec": { "containers": [] }
                }
            }))
            .unwrap(),
        );
        statefulset.status = Some(
            serde_json::from_value(json!({
                "replicas": 3,
                "readyReplicas": 3,
                "updatedReplicas": 3,
                "observedGeneration": 1,
                "currentRevision": "revision-2",
                "updateRevision": "revision-2"
            }))
            .unwrap(),
        );

        assert!(!pools_converged([&p], []));
        assert!(!pools_converged([&p], [&statefulset, &statefulset]));

        // Ready counts alone are insufficient until the controller has
        // observed the latest StatefulSet generation.
        assert!(!pools_converged([&p], [&statefulset]));

        statefulset.status.as_mut().unwrap().observed_generation = Some(2);
        statefulset.status.as_mut().unwrap().current_revision = Some("revision-1".into());
        // A fully Ready old revision must not advance CA promotion/pruning.
        assert!(!pools_converged([&p], [&statefulset]));

        statefulset.status.as_mut().unwrap().current_revision = Some("revision-2".into());
        assert!(pools_converged([&p], [&statefulset]));

        statefulset.spec.as_mut().unwrap().replicas = Some(2);
        assert!(!pools_converged([&p], [&statefulset]));
        statefulset.spec.as_mut().unwrap().replicas = Some(3);

        p.metadata
            .labels
            .as_mut()
            .unwrap()
            .insert("crabka.io/config-hash".into(), String::new());
        assert!(!pools_converged([&p], [&statefulset]));
    }

    #[test]
    fn rolling_condition_when_pool_partial() {
        let r = ClusterRollup {
            replicas: ReplicaCount(3),
            ready_replicas: ReadyReplicaCount(1),
            pool_count: 1,
        };
        let (rolling, reason, _) = rolling_condition_from_rollup(&r);
        assert!(rolling);
        assert!(reason == "RollingUpdate");
    }

    #[test]
    fn rolling_condition_when_pool_stable() {
        let r = ClusterRollup {
            replicas: ReplicaCount(1),
            ready_replicas: ReadyReplicaCount(1),
            pool_count: 1,
        };
        let (rolling, reason, _) = rolling_condition_from_rollup(&r);
        assert!(!rolling);
        assert!(reason == "Stable");
    }

    // Boundary: with zero pools the cluster is never "rolling", even when the
    // (defaulted) ready/replica totals disagree. Pins `pool_count > 0` so a
    // `>=` mutant (which would treat pool_count==0 as rolling) fails here.
    #[test]
    fn rolling_condition_zero_pools_is_stable() {
        let r = ClusterRollup {
            replicas: ReplicaCount(3),
            ready_replicas: ReadyReplicaCount(1),
            pool_count: 0,
        };
        let (rolling, reason, _) = rolling_condition_from_rollup(&r);
        assert!(!rolling);
        assert!(reason == "Stable");
    }

    // Boundary: a pool that exists but reports zero replicas (ready==replicas==0)
    // is PartiallyReady, not Available. Pins `replicas > 0` so a `>=` mutant
    // (which would call an all-zero cluster "Available") fails here.
    #[test]
    fn rollup_condition_zero_replicas_is_partially_ready() {
        let r = ClusterRollup {
            replicas: ReplicaCount(0),
            ready_replicas: ReadyReplicaCount(0),
            pool_count: 1,
        };
        let (ready, reason, _) = rollup_condition(&r);
        assert!(!ready);
        assert!(reason == "PartiallyReady");
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
                broker_tuning: None,
                gres_registry: None,
            },
        );
        k.metadata.namespace = Some("ns".into());
        k
    }

    #[test]
    fn canonical_oauth_config_none_when_no_oauth_listener() {
        let ls = vec![
            listener_with_auth("plain", None),
            listener_with_auth("scram", Some(ListenerAuthentication::ScramSha512)),
        ];
        assert!(canonical_oauth_config(&ls).is_none());
    }

    #[test]
    fn canonical_oauth_config_picks_first_oauth() {
        let cfg = sample_oauth_cfg(vec![]);
        let ls = vec![
            listener_with_auth("plain", None),
            listener_with_auth(
                "oauth",
                Some(ListenerAuthentication::OAuth(Box::new(cfg.clone()))),
            ),
        ];
        assert!(canonical_oauth_config(&ls) == Some(cfg));
    }

    #[test]
    fn canonical_oauth_config_with_empty_trust_certs_is_some_but_empty() {
        // The reconcile-level no-op check is
        //   `canonical.tls_trusted_certificates.is_empty()` — guard that the
        // helper still returns Some so the no-op branch is reached.
        let cfg = sample_oauth_cfg(vec![]);
        let ls = vec![listener_with_auth(
            "oauth",
            Some(ListenerAuthentication::OAuth(Box::new(cfg))),
        )];
        let got = canonical_oauth_config(&ls).expect("OAuth listener present");
        assert!(got.tls_trusted_certificates.is_empty());
    }

    // Pure helper — derives the introspection client-secret
    // mount from the CR's listeners. The async, apiserver-touching
    // `reconcile_oauth_introspection_secret` path is covered by the
    // integration tests.

    #[test]
    fn oauth_introspection_secret_mount_returns_none_when_no_oauth_listener() {
        let kafka = kafka_with_listeners(vec![
            listener_with_auth("plain", None),
            listener_with_auth("scram", Some(ListenerAuthentication::ScramSha512)),
        ]);
        assert!(oauth_introspection_secret_mount(&kafka).is_none());
    }

    #[test]
    fn oauth_introspection_secret_mount_returns_none_when_access_token_is_jwt_true() {
        // sample_oauth_cfg defaults to access_token_is_jwt = true (JWT mode).
        let cfg = sample_oauth_cfg(vec![]);
        let kafka = kafka_with_listeners(vec![listener_with_auth(
            "oauth",
            Some(ListenerAuthentication::OAuth(Box::new(cfg))),
        )]);
        assert!(oauth_introspection_secret_mount(&kafka).is_none());
    }

    #[test]
    fn oauth_introspection_secret_mount_returns_none_when_client_secret_absent_introspection_mode()
    {
        // Introspection mode but clientSecret omitted (would fail
        // validation, but the helper should still handle it gracefully —
        // the pool reconciler must not panic on an invalid-but-applied CR).
        let mut cfg = sample_oauth_cfg_introspection("oauth-cs", "client-secret");
        cfg.client_secret = None;
        let kafka = kafka_with_listeners(vec![listener_with_auth(
            "oauth",
            Some(ListenerAuthentication::OAuth(Box::new(cfg))),
        )]);
        assert!(oauth_introspection_secret_mount(&kafka).is_none());
    }

    #[test]
    fn oauth_introspection_secret_mount_returns_some_for_introspection_config() {
        let cfg = sample_oauth_cfg_introspection("oauth-cs", "client-secret");
        let kafka = kafka_with_listeners(vec![listener_with_auth(
            "oauth",
            Some(ListenerAuthentication::OAuth(Box::new(cfg))),
        )]);
        let mount = oauth_introspection_secret_mount(&kafka).expect("mount derived");
        assert!(mount.secret_name == "oauth-cs");
        assert!(mount.key == "client-secret");
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
            max_time_skew: None,
        };
        let k = kafka_with_listeners(vec![listener_with_auth(
            "gss",
            Some(ListenerAuthentication::Gssapi(g)),
        )]);
        let m = gssapi_keytab_mount(&k).expect("keytab mount present");
        assert!(m.secret_name == "kt");
        assert!(m.key == "krb5.keytab");
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
        let (secret_name, key) = krb5_conf_mount(&k).expect("krb5.conf mount present");
        assert!(secret_name == "krb5");
        assert!(key == "krb5.conf");
    }
}
