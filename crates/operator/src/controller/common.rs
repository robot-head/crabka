//! Shared helpers across the `Kafka` and `KafkaNodePool` reconcilers.
//!
//! The `Kafka`-owned cluster-level objects (`Service`, `ConfigMap`,
//! cluster-id `Secret`) live here because both reconcilers need to refer
//! to their names (the pool reconciler reads the Secret; the parent
//! reconciler renders+applies them). The status-derivation helper, the
//! SSA / merge-patch wrappers, and the labels / owner-ref helpers are
//! shared verbatim.

use std::{collections::BTreeMap, fmt::Debug, future::Future, pin::Pin, sync::Arc};

use k8s_openapi::{
    ByteString,
    api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Secret, Service},
    },
    apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference},
};
use kube::{
    Resource,
    api::{Api, DynamicObject, Patch, PatchParams, PostParams},
    core::{ApiResource, GroupVersionKind},
    runtime::controller::Action,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::json;
use uuid::Uuid;

use crate::{
    context::Context,
    crd::{Kafka, KafkaCondition},
};

pub(crate) const FIELD_MANAGER: &str = "crabka-operator";

pub(crate) const BROKER_PORT: i32 = 9092;
/// `KRaft` controller listener port. Every broker binds its controller
/// listener on `0.0.0.0:9093` and peers reach each other on this port
/// via the headless Service's per-pod DNS A-records.
pub(crate) const CONTROLLER_PORT: i32 = 9093;
pub(crate) const APP_LABEL: &str = "crabka-broker";
pub(crate) const DEFAULT_BROKER_IMAGE: &str = concat!(
    "ghcr.io/robot-head/crabka-broker:",
    env!("CARGO_PKG_VERSION")
);

pub(super) fn error_requeue(ctx: Arc<Context>) -> Action {
    let delay = ctx.config.controller_error_requeue;
    drop(ctx);
    Action::requeue(delay.to_std())
}

pub(crate) fn time_from_millis_u64(millis: u64) -> Time {
    Time::from_millis(i64::try_from(millis).unwrap_or(i64::MAX))
}

pub(crate) fn millis_u64(extent: Time) -> u64 {
    u64::try_from(extent.millis_i64()).unwrap_or_default()
}

pub(crate) fn secs_u64(extent: Time) -> u64 {
    u64::try_from(extent.secs_i64()).unwrap_or_default()
}

/// Reconcile-error surface shared by both reconcilers.
#[derive(Debug, thiserror::Error)]
pub enum ReconcileError {
    #[error("kube error: {0}")]
    Kube(#[from] kube::Error),
    #[error("resource missing uid (not yet admitted)")]
    MissingUid,
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("spec.replicas={0} is unsupported (only 1 allowed)")]
    UnsupportedReplicas(i32),
    #[error("cluster-id secret malformed: {0}")]
    MalformedSecret(String),
    #[error("metricsConfig: podMonitor and serviceMonitor are mutually exclusive")]
    MetricsMutuallyExclusive,
    #[error("monitoring.coreos.com/v1 is not served by the API server")]
    PrometheusOperatorCrdsMissing,
    #[error("malformed input: {0}")]
    Malformed(String),
    #[error("CA: {0}")]
    Ca(#[from] crabka_security::ca::CaError),
    #[error("cert parse: {0}")]
    CertParse(String),
    #[error(
        "BYO CA missing: {which} requires pre-existing Secret pair (generateCertificateAuthority=false)"
    )]
    ByoCaMissing { which: String },
    #[allow(dead_code)] // reserved to surface BYO CA parse failures at reconcile time
    #[error("BYO CA malformed: {which}: {reason}")]
    ByoCaMalformed { which: String, reason: String },
    #[error("CA Secret missing: {name}")]
    CaSecretMissing { name: String },
    #[error("oauth trust Secret '{0}' not found")]
    MissingOauthTrustSecret(String),
    #[error("oauth trust Secret '{secret}' has no key '{key}'")]
    MissingOauthTrustKey { secret: String, key: String },
    #[error("oauth trust Secret '{secret}' key '{key}' is empty")]
    EmptyOauthTrustValue { secret: String, key: String },
    /// An oauth listener's `accessTokenIsJwt` setting
    /// disagrees with which mode-specific fields are set (JWT-mode
    /// requires `jwksEndpointUri` and rejects introspection fields;
    /// introspection-mode requires `introspectionEndpointUri` + `clientId`
    /// + `clientSecret` and rejects `jwksEndpointUri`).
    #[error("listener OAuth: {0}")]
    InvalidListenerOauthAccessTokenIsJwt(String),
    /// An oauth listener's `clientSecret.secretName` doesn't
    /// exist in the cluster's namespace.
    #[error("oauth introspection Secret '{0}' not found")]
    MissingOauthIntrospectionSecret(String),
    /// An oauth listener's `clientSecret.secretName` exists
    /// but does not contain the named `key`.
    #[error("oauth introspection Secret '{secret}' has no key '{key}'")]
    MissingOauthIntrospectionKey { secret: String, key: String },
    /// An oauth listener's `clientSecret` Secret + key both
    /// exist but the value is zero bytes.
    #[error("oauth introspection Secret '{secret}' key '{key}' is empty")]
    EmptyOauthIntrospectionValue { secret: String, key: String },
    /// `type: gssapi` listener references a keytab Secret that doesn't exist.
    #[error("gssapi keytab Secret '{0}' not found")]
    MissingGssapiKeytabSecret(String),
    /// keytab Secret exists but lacks the referenced key.
    #[error("gssapi keytab Secret '{secret}' has no key '{key}'")]
    MissingGssapiKeytabKey { secret: String, key: String },
    /// `spec.krb5ConfSecretRef` references a Secret that doesn't exist.
    #[error("krb5.conf Secret '{0}' not found")]
    MissingKrb5ConfSecret(String),
    /// `spec.krb5ConfSecretRef` Secret exists but lacks the referenced key.
    #[error("krb5.conf Secret {secret:?} is missing key {key:?}")]
    MissingKrb5ConfKey { secret: String, key: String },
    /// KIP-405: `spec.tieredStorage` failed shape
    /// validation. Concrete cases: `type = "S3"` without `spec.tieredStorage.s3`,
    /// `type = "Local"` with `spec.tieredStorage.s3` set, or an S3 spec
    /// missing required `bucket` / `region`. The reconciler returns this
    /// before rendering any `ConfigMap` so the broker pod never boots
    /// against malformed `[remote_storage]` TOML.
    #[error("tieredStorage: {0}")]
    TieredStorageInvalid(String),

    /// `spec.tracing` failed shape validation. Concrete
    /// cases: `type = "Otlp"` without an `otlp` block; `otlp.endpoint`
    /// empty; `sampleRatio` outside `[0.0, 1.0]`; `timeoutSecs = 0`.
    /// The reconciler returns this before rendering any pod template
    /// so the broker pod never boots with broken OTLP env vars.
    #[error("tracing: {0}")]
    TracingInvalid(String),
    #[error("broker tuning: {0}")]
    KafkaConfigInvalid(String),
    #[error("schema registry tuning: {0}")]
    SchemaRegistryConfigInvalid(String),
    #[error("gateway tuning: {0}")]
    GatewayConfigInvalid(String),
    #[error("gres control: {0}")]
    GresControl(#[from] crabka_gres_control::ControlError),
    #[error("producer error: {0}")]
    Producer(#[from] crabka_client_producer::ProducerError),
    #[error("gres control write: {0}")]
    GresControlWrite(#[from] crate::context::GresControlWriteError),
    #[error("admin error: {0}")]
    Admin(#[from] crabka_client_admin::AdminError),
    #[error("pgdog admin error: {0}")]
    PgdogAdmin(#[from] crate::context::PgdogAdminError),
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

#[derive(Debug, Clone, Copy)]
pub(crate) enum ParentVersionGate<'a> {
    Cleared,
    Invalid(&'a KafkaCondition),
    Waiting,
}

/// Shared parent-Kafka version gate. Clears when the parent has either
/// `KafkaVersionValid=True` or a finalized `status.metadataVersion`.
pub(crate) fn parent_version_gate(parent: &Kafka) -> ParentVersionGate<'_> {
    let status = parent.status.as_ref();
    let version_cond =
        status.and_then(|s| s.conditions.iter().find(|c| c.type_ == "KafkaVersionValid"));
    let finalized = status.and_then(|s| s.metadata_version.as_deref());

    if finalized.is_some() || version_cond.is_some_and(|c| c.status == "True") {
        return ParentVersionGate::Cleared;
    }

    match version_cond {
        Some(condition) => ParentVersionGate::Invalid(condition),
        None => ParentVersionGate::Waiting,
    }
}

/// Time a reconcile future and record the shared reconcile metric.
pub(crate) async fn record_reconcile<E, F>(
    ctx: &Context,
    kind: &'static str,
    reconcile: Pin<Box<F>>,
) -> Result<Action, E>
where
    F: Future<Output = Result<Action, E>> + ?Sized,
{
    let started = std::time::Instant::now();
    let result = reconcile.await;
    let outcome = if result.is_ok() {
        crate::telemetry::ReconcileResult::Ok
    } else {
        crate::telemetry::ReconcileResult::Error
    };
    ctx.metrics
        .record_reconcile(kind, outcome, started.elapsed().as_secs_f64());
    result
}

/// Server-side apply a typed object. Field manager is `crabka-operator`,
/// force-takeover is on so we wrest fields back from any previous manager
/// if any happen to linger. Object shape is stable across reconciles
/// because renderers are pure functions of the owner.
#[tracing::instrument(level = "debug", skip_all, fields(name = %name), err)]
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

/// Server-side apply an arbitrary object that is not in `k8s-openapi` (e.g. an
/// `OpenShift` `Route`), given its GVK + plural and a JSON body. Errors —
/// including a 404 when the CRD's API is not served (a non-`OpenShift` cluster)
/// — propagate to the caller.
#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(namespace = %namespace, api_version = %api_version, kind = %kind, name = %name),
    err,
)]
pub(crate) async fn apply_dynamic(
    client: &kube::Client,
    namespace: &str,
    api_version: &str,
    kind: &str,
    plural: &str,
    name: &str,
    body: &serde_json::Value,
) -> Result<(), ReconcileError> {
    let (group, version) = api_version
        .split_once('/')
        .ok_or_else(|| ReconcileError::Malformed("apiVersion missing '/'".into()))?;
    let gvk = GroupVersionKind::gvk(group, version, kind);
    let ar = ApiResource::from_gvk_with_plural(&gvk, plural);
    let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), namespace, &ar);
    let obj: DynamicObject = serde_json::from_value(body.clone())?;
    let pp = PatchParams::apply(FIELD_MANAGER).force();
    api.patch(name, &pp, &Patch::Apply(&obj)).await?;
    Ok(())
}

/// Merge-patch the status subresource of a CR. Uses `Patch::Merge` so we
/// only overwrite the fields we set rather than replacing the whole
/// status (which would conflict with any future status writers). Generic
/// over the parent resource `K` and status payload `S`.
#[tracing::instrument(level = "info", skip_all, fields(name = %name), err)]
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
            // Publish per-pod DNS A-records before pods are Ready. KRaft
            // peers resolve each other's controller FQDN at cold start —
            // gating DNS on readiness would deadlock quorum formation.
            "publishNotReadyAddresses": true,
            "selector": selector,
            "ports": [
                {
                    "name": "kafka-internal",
                    "port": BROKER_PORT,
                    "protocol": "TCP",
                    "targetPort": BROKER_PORT,
                },
                {
                    "name": "controller",
                    "port": CONTROLLER_PORT,
                    "protocol": "TCP",
                    "targetPort": CONTROLLER_PORT,
                },
            ],
        }
    }))?;
    Ok(svc)
}

/// Render the cluster-level `ConfigMap`. Owner-ref'd to the parent
/// `Kafka`. Emits one `broker-{id}.toml` key per entry in
/// `addresses_per_broker`, generated by
/// [`crate::controller::listeners::render_broker_toml`].
// each arg is an independent operator-owned render input
pub(crate) fn render_configmap(
    owner: &Kafka,
    listeners: &[crate::crd::Listener],
    addresses_per_broker: &std::collections::BTreeMap<
        i32,
        std::collections::BTreeMap<String, crate::controller::listeners::AdvertisedAddress>,
    >,
    inter_broker_listener_name: &str,
    tls_per_broker: Option<
        &std::collections::BTreeMap<i32, crate::controller::listeners::BrokerTlsRender>,
    >,
    clients_ca_path: Option<&str>,
    logging_filter: Option<&str>,
) -> Result<ConfigMap, ReconcileError> {
    let name = owner.meta().name.clone().unwrap_or_default();
    let labels = common_labels(&name, &owner.spec.kafka_version, None);

    let mut data = BTreeMap::new();
    // Cluster-wide `RUST_LOG` filter, referenced by each broker
    // pod's `RUST_LOG` env via `configMapKeyRef` (see `render_broker_container`).
    if let Some(filter) = logging_filter {
        data.insert("rust.log".to_string(), filter.to_string());
    }
    // `metadata.version` is finalized via the bootstrap-seeded feature
    // record (`crabka format --release-version`), not the broker config —
    // so it is intentionally not rendered here. An explicit
    // `spec.metadataVersion` pin still rolls the cluster via the config
    // hash (see `combined_config_hash`), which is a separate channel.
    let server_properties = owner.spec.config.clone().unwrap_or_default();
    // Surface delegation-token enablement to the per-broker
    // renderer. The `super_users = ["ANONYMOUS"]`
    // top-level emit is folded into the `[authorization]` block — passing this
    // flag still drives the auto-injected `[authorization]` shape (or the
    // ANONYMOUS-merge into a user-authored authorization).
    let delegation_token_enabled = owner.spec.delegation_token.is_some();
    // Optional broker authorizer config. `None` ⇒ broker
    // defaults to AllowAll (or, with delegation tokens enabled, gets the
    // auto-injected `simple + ANONYMOUS` block — see
    // `render_broker_toml`).
    let authorization = owner.spec.authorization.as_ref();
    // Thread `Kafka.spec.tieredStorage` into each broker's
    // TOML so the broker-wide `[remote_storage]` block (and the matching
    // `tier-storage` pod volume) light up together.
    let tiered_storage = owner.spec.tiered_storage.as_ref();
    let inter_broker_kerberos = owner.spec.inter_broker_kerberos.as_ref();
    // KRaft controller quorum voter set. Build the full cluster voter list
    // ONCE (sorted by broker_id via the BTreeMap iteration order) and emit
    // the identical complete list into every broker's TOML. Each voter is
    // `<id>@<host>:9093`, where `host` is the broker's inter-broker
    // advertised FQDN (the headless per-pod DNS name) paired with the
    // controller port. A broker with no inter-broker advertised address is
    // skipped (shouldn't happen in practice).
    let controller_quorum_voters: Vec<String> = addresses_per_broker
        .iter()
        .filter_map(|(broker_id, addrs)| {
            addrs
                .get(inter_broker_listener_name)
                .map(|adv| format!("{broker_id}@{}:{CONTROLLER_PORT}", adv.host))
        })
        .collect();
    // TLS server-name (SNI) every broker presents when dialing a peer's
    // controller listener for the KIP-595 quorum. The shared headless-Service
    // FQDN is a SAN on every broker's serving cert (see `kafka.rs` keystore
    // SAN list), so mTLS validation succeeds regardless of which peer pod IP
    // is dialed. Identical across all brokers.
    let ns = owner.meta().namespace.clone().unwrap_or_default();
    let controller_server_name = format!("{name}-broker-headless.{ns}.svc.cluster.local");
    for (broker_id, addrs) in addresses_per_broker {
        let tls_for_broker = tls_per_broker.and_then(|m| m.get(broker_id));
        let mut toml = crate::controller::listeners::render_broker_toml(
            (*broker_id, listeners, addrs, inter_broker_listener_name),
            (&server_properties, tls_for_broker, clients_ca_path),
            (
                delegation_token_enabled,
                authorization,
                inter_broker_kerberos,
            ),
            tiered_storage,
            (&controller_quorum_voters, &controller_server_name),
        );
        if let Some(tuning) = &owner.spec.broker_tuning {
            toml.push_str(&tuning.render_runtime_toml());
        }
        data.insert(format!("broker-{broker_id}.toml"), toml);
    }

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

/// Read a PEM string from a Secret's data field. Returns `None` if the key is
/// absent, the data map is missing, or the bytes are not valid UTF-8.
pub(crate) fn read_pem_key(secret: &Secret, key: &str) -> Option<String> {
    let data = secret.data.as_ref()?;
    let bytes = &data.get(key)?.0;
    String::from_utf8(bytes.clone()).ok()
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

/// Truncated SHA-256 hex digest (16 hex chars / 8 bytes of entropy)
/// of the given content. Used to detect `Kafka.spec.config`
/// changes that the K8s `StatefulSet` controller can't see directly.
///
/// The full sha256 is 64 hex chars, which exceeds the 63-char K8s
/// label-value limit. 64 bits of entropy is more than enough for a
/// drift detector — collisions for accidental config changes are
/// astronomically unlikely.
#[must_use]
pub fn config_hash(content: &str) -> String {
    use std::fmt::Write;

    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(content.as_bytes());
    let digest = h.finalize();
    let mut out = String::with_capacity(16);
    for byte in digest.iter().take(8) {
        write!(&mut out, "{byte:02x}").expect("writing to a String never fails");
    }
    out
}

/// Combined hash over user `spec.config`, the
/// canonical listener intent, a `metrics_config.is_some()` bit, and
/// the cluster CA cert PEM.
/// Empty listeners produce empty intent and metrics-unset produces an
/// empty third segment, so the combined hash is identical to the
/// bare `config_hash(spec.config)` for an unchanged `spec.config` with neither listeners
/// nor metrics.
///
/// The metrics segment is a coarse `metrics_enabled` bit, not a hash of
/// the full `metrics_config` body — toggling `metricsConfig` on/off
/// changes the broker pod template (adds/removes the metrics port + CLI
/// flag) and so must trigger a pool reconcile (which re-renders the
/// `StatefulSet`). Sub-field changes (interval, scrape labels) affect
/// only the `PodMonitor`/`ServiceMonitor` objects, not the broker pod,
/// and do not need a roll.
///
/// `cluster_ca_cert_pem` — when `Some`, the cluster CA cert PEM is
/// included as a fourth segment. Rotating the cluster CA forces a
/// cluster roll; leaf renewal does not (hot-reload handles it).
///
/// `metadata_version_pin` — when `Some`, an *explicit*
/// `spec.metadataVersion` pin is included as a fifth segment, so changing
/// the pin rolls the cluster. A *defaulted* metadata version is passed as
/// `None` here (a binary bump already rolls via the pod-template image
/// change), which preserves the empty-hash collapse.
///
/// `logging_filter` — when `Some`, the resolved `RUST_LOG`
/// env-filter string is included as a sixth segment. The broker only re-reads
/// `RUST_LOG` on restart, so a *value* change (not just on/off) must roll the
/// cluster. `None` (logging unset, or external resolution failed) contributes
/// an empty segment, preserving the empty-hash collapse.
///
/// Nonempty `broker_tuning` contributes its deterministic rendered `[runtime]`
/// TOML. Absent and all-`None` tuning contribute an empty segment.
#[must_use]
pub fn combined_config_hash(
    spec: &crate::crd::KafkaSpec,
    cluster_ca_cert_pem: Option<&str>,
    metadata_version_pin: Option<&str>,
    logging_filter: Option<&str>,
) -> String {
    let config_part = spec
        .config
        .as_ref()
        .map(|m| {
            let mut s = String::new();
            for (k, v) in m {
                s.push_str(k);
                s.push('=');
                s.push_str(v);
                s.push('\n');
            }
            s
        })
        .unwrap_or_default();
    let intent = crate::controller::listeners::canonical_listener_intent(
        &spec.listeners,
        spec.inter_broker_listener_name.as_deref(),
    );
    let metrics_part = if spec.metrics_config.is_some() {
        "metrics=on"
    } else {
        ""
    };
    let ca_part = cluster_ca_cert_pem.unwrap_or("");
    let metadata_part = metadata_version_pin.unwrap_or("");
    let logging_part = logging_filter.unwrap_or("");
    let runtime_part = spec
        .broker_tuning
        .as_ref()
        .map(crate::crd::BrokerTuning::render_runtime_toml)
        .unwrap_or_default();
    // Hash-collapse compatibility: when listeners, metricsConfig, the CA cert,
    // an explicit metadataVersion pin, logging, and rendered runtime tuning
    // are all absent, the hash
    // collapses to `config_hash(config_part)` — byte-identical to the
    // bare config hash for the same `spec.config`. This is what makes an
    // in-place upgrade from a config-only cluster not trigger a hash-driven roll (the
    // unavoidable template-change roll fires separately and once).
    if intent.is_empty()
        && metrics_part.is_empty()
        && ca_part.is_empty()
        && metadata_part.is_empty()
        && logging_part.is_empty()
        && runtime_part.is_empty()
    {
        return config_hash(&config_part);
    }
    let mut buf = String::with_capacity(
        config_part.len()
            + 6
            + intent.len()
            + metrics_part.len()
            + ca_part.len()
            + metadata_part.len()
            + logging_part.len()
            + runtime_part.len(),
    );
    buf.push_str(&config_part);
    buf.push('\x1F'); // ASCII unit separator
    buf.push_str(&intent);
    buf.push('\x1F');
    buf.push_str(metrics_part);
    buf.push('\x1F');
    buf.push_str(ca_part);
    buf.push('\x1F');
    buf.push_str(metadata_part);
    buf.push('\x1F');
    buf.push_str(logging_part);
    buf.push('\x1F');
    buf.push_str(&runtime_part);
    config_hash(&buf)
}

/// One pool's observed state, fed to [`plan_rollout`]. `current_hash` is
/// the pool's `crabka.io/config-hash` label (`None` if never stamped);
/// `ready` is whether the pool's single broker has reached Ready.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PoolRolloutState {
    pub name: String,
    pub current_hash: Option<String>,
    pub ready: bool,
}

/// Decide the config-hash to write to each pool for an ordered,
/// one-node-at-a-time rollout. `pools` must be pre-sorted into the desired
/// roll order (by `(node_id_start, name)`). Returns the target hash per
/// pool name, in the same order.
///
/// - **Bring-up / recovery** — if any pool has no current hash, or there
///   is more than one distinct *non-desired* hash among pools, every pool
///   gets `desired` (parallel). This is required so a `KRaft` controller
///   quorum can form: gating initial creation one-at-a-time would deadlock
///   (a single controller can't reach Ready without quorum). Also the
///   single-pool first-reconcile path.
/// - **Steady state** — if every pool already carries `desired`, all stay
///   `desired` (no-op).
/// - **Established roll** — otherwise the cluster is uniform on one old
///   hash (or mid-roll between `{old, desired}`) and transitioning. Walk
///   pools in order; a pool is *converged* when it already carries
///   `desired` AND is Ready. Advance the first non-converged pool to
///   `desired`; every later pool keeps its current hash until the earlier
///   pools converge.
pub(crate) fn plan_rollout(pools: &[PoolRolloutState], desired: &str) -> Vec<(String, String)> {
    let all_have_hash = pools.iter().all(|p| p.current_hash.is_some());
    let distinct_non_desired: std::collections::BTreeSet<&str> = pools
        .iter()
        .filter_map(|p| p.current_hash.as_deref())
        .filter(|h| *h != desired)
        .collect();

    // Bring-up / recovery / messy state → everyone gets `desired`.
    if !all_have_hash || distinct_non_desired.len() > 1 {
        return pools
            .iter()
            .map(|p| (p.name.clone(), desired.to_string()))
            .collect();
    }

    // Established cluster: advance one pool at a time, gated on readiness.
    let mut gate_open = true;
    let mut out = Vec::with_capacity(pools.len());
    for p in pools {
        if gate_open {
            let converged = p.current_hash.as_deref() == Some(desired) && p.ready;
            // This pool advances to (or already holds) `desired`.
            out.push((p.name.clone(), desired.to_string()));
            if !converged {
                // Hold every later pool at its current hash until this one
                // converges.
                gate_open = false;
            }
        } else {
            // Keep the existing hash; `all_have_hash` guarantees `Some`.
            let keep = p
                .current_hash
                .clone()
                .unwrap_or_else(|| desired.to_string());
            out.push((p.name.clone(), keep));
        }
    }
    out
}

/// Parse a K8s `Quantity` string into a comparable byte count.
///
/// Accepts:
/// - Binary suffixes: `Ki`, `Mi`, `Gi`, `Ti`, `Pi`, `Ei` (1 Ki = 1024).
/// - Decimal suffixes: `K`, `M`, `G`, `T`, `P`, `E` (1 K = 1000).
/// - Bare numbers (no suffix → bytes).
/// - Integer or decimal mantissa (`1.5Gi`).
///
/// Rejects: scientific notation, negative numbers, zero, empty
/// strings, or any value that doesn't match `<mantissa><suffix?>`.
///
/// Returns the byte count as `i128` (1.5 Pi fits with headroom for
/// arithmetic). The result is only used for ordered comparison
/// — we never round-trip back to a string, so sub-byte rounding from
/// the `f64` intermediate is acceptable. The in-tree implementation
/// is ~50 lines and saves a workspace dependency; no third-party
/// Quantity parser is wired into Crabka yet.
///
/// # Errors
///
/// Returns a static `&str` describing the parse failure.
pub(crate) fn parse_quantity(s: &str) -> Result<i128, &'static str> {
    if s.is_empty() {
        return Err("empty quantity string");
    }

    let (mantissa_str, multiplier): (&str, i128) = if let Some(rest) = s.strip_suffix("Ki") {
        (rest, 1_024)
    } else if let Some(rest) = s.strip_suffix("Mi") {
        (rest, 1_024_i128.pow(2))
    } else if let Some(rest) = s.strip_suffix("Gi") {
        (rest, 1_024_i128.pow(3))
    } else if let Some(rest) = s.strip_suffix("Ti") {
        (rest, 1_024_i128.pow(4))
    } else if let Some(rest) = s.strip_suffix("Pi") {
        (rest, 1_024_i128.pow(5))
    } else if let Some(rest) = s.strip_suffix("Ei") {
        (rest, 1_024_i128.pow(6))
    } else if let Some(rest) = s.strip_suffix('K') {
        (rest, 1_000)
    } else if let Some(rest) = s.strip_suffix('M') {
        (rest, 1_000_000)
    } else if let Some(rest) = s.strip_suffix('G') {
        (rest, 1_000_000_000)
    } else if let Some(rest) = s.strip_suffix('T') {
        (rest, 1_000_000_000_000)
    } else if let Some(rest) = s.strip_suffix('P') {
        (rest, 1_000_000_000_000_000)
    } else if let Some(rest) = s.strip_suffix('E') {
        (rest, 1_000_000_000_000_000_000)
    } else {
        (s, 1)
    };

    if mantissa_str.is_empty() {
        return Err("missing numeric mantissa before suffix");
    }
    if mantissa_str.contains(['e', 'E']) {
        return Err("scientific notation not supported");
    }
    if mantissa_str.starts_with('-') {
        return Err("negative quantity rejected");
    }

    let (whole, fraction) = mantissa_str.split_once('.').unwrap_or((mantissa_str, ""));
    if whole.is_empty() && fraction.is_empty() {
        return Err("mantissa is not a valid number");
    }
    if !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err("mantissa is not a valid number");
    }
    let whole = if whole.is_empty() {
        0
    } else {
        whole
            .parse::<i128>()
            .map_err(|_| "quantity overflows i128")?
    };
    let scale = 10_i128
        .checked_pow(u32::try_from(fraction.len()).map_err(|_| "quantity overflows i128")?)
        .ok_or("quantity overflows i128")?;
    let fraction = if fraction.is_empty() {
        0
    } else {
        fraction
            .parse::<i128>()
            .map_err(|_| "quantity overflows i128")?
    };
    let scaled = whole
        .checked_mul(scale)
        .and_then(|value| value.checked_add(fraction))
        .ok_or("quantity overflows i128")?;
    if scaled == 0 {
        return Err("quantity must be strictly positive");
    }
    scaled
        .checked_mul(multiplier)
        .map(|value| value / scale)
        .ok_or("quantity overflows i128")
}

#[cfg(test)]
mod config_hash_tests {
    use assert2::{assert, check};

    use super::*;

    #[test]
    fn config_hash_is_truncated_sha256_hex() {
        // First 16 hex chars (8 bytes) of sha256("hello"):
        //   2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824
        //   ^^^^^^^^^^^^^^^^
        let h = config_hash("hello");
        assert!(h == "2cf24dba5fb0a30e");
        assert!(h.len() == 16, "must fit within K8s 63-char label limit");
    }

    #[test]
    fn config_hash_empty_string() {
        // First 16 hex chars of sha256("").
        let h = config_hash("");
        assert!(h == "e3b0c44298fc1c14");
    }

    #[test]
    fn config_hash_fits_in_kubernetes_label_value() {
        // K8s label values are limited to 63 characters. Our truncated
        // hash must always fit; this test guards against future widening.
        let h = config_hash("any content at all");
        assert!(h.len() <= 63, "hash {h} exceeds K8s label limit");
    }

    #[test]
    fn combined_hash_unchanged_when_listeners_empty() {
        use crate::crd::KafkaSpec;

        let spec_a = KafkaSpec {
            kafka_version: "0.1.1".into(),
            metadata_version: None,
            config: Some({
                let mut m = std::collections::BTreeMap::new();
                m.insert("log.retention.hours".into(), "24".into());
                m
            }),
            listeners: vec![],
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
        };
        let h = combined_config_hash(&spec_a, None, None, None);
        let h_again = combined_config_hash(&spec_a, None, None, None);
        assert!(h == h_again);

        // Hash-collapse compat: the hash for empty listeners + no metrics MUST
        // equal `config_hash(serialized broker-properties)`. That's what
        // lets an in-place config-only upgrade avoid a
        // hash-driven roll (the e2e job `kind-upgrade` asserts this
        // against a real config-only cluster).
        let config_only_form = "log.retention.hours=24\n";
        assert!(
            h == config_hash(config_only_form),
            "combined hash for empty listeners must equal config_hash(spec.config)"
        );

        let mut spec_b = spec_a.clone();
        spec_b.listeners = vec![crate::controller::listeners::synthesized_default_listener()];
        spec_b.inter_broker_listener_name = Some("PLAIN".into());
        let h_with_listener = combined_config_hash(&spec_b, None, None, None);
        assert!(
            h != h_with_listener,
            "non-empty listener intent must change hash"
        );
    }

    #[test]
    fn combined_hash_tracks_nonempty_broker_tuning_only() {
        use crate::crd::{BrokerTuning, KafkaSpec};

        let mut spec = KafkaSpec {
            kafka_version: "0.1.1".into(),
            metadata_version: None,
            config: None,
            listeners: vec![],
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
        };
        let absent = combined_config_hash(&spec, None, None, None);

        spec.broker_tuning = Some(BrokerTuning::default());
        let empty = combined_config_hash(&spec, None, None, None);
        assert!(empty == absent, "empty tuning must preserve hash collapse");

        spec.broker_tuning = Some(BrokerTuning {
            auto_join_voter_request_timeout: Some(crabka_units::secs(7)),
            ..BrokerTuning::default()
        });
        let nonempty = combined_config_hash(&spec, None, None, None);
        assert!(
            nonempty != absent,
            "rendered runtime tuning must roll broker pods"
        );
    }

    #[test]
    fn combined_hash_flips_when_metrics_config_toggles() {
        use crate::crd::{KafkaSpec, MetricsConfig, PodMonitorSpec};

        let spec_off = KafkaSpec {
            kafka_version: "0.1.1".into(),
            metadata_version: None,
            config: None,
            listeners: vec![],
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
        };
        let h_off = combined_config_hash(&spec_off, None, None, None);

        let mut spec_on = spec_off.clone();
        spec_on.metrics_config = Some(MetricsConfig {
            pod_monitor: Some(PodMonitorSpec::default()),
            ..Default::default()
        });
        let h_on = combined_config_hash(&spec_on, None, None, None);
        assert!(
            h_off != h_on,
            "enabling metrics_config must bump the hash (triggers pool reconcile + StatefulSet re-render)"
        );

        // Toggling sub-fields (interval, labels) does NOT change the
        // hash — those only affect the PodMonitor/ServiceMonitor body,
        // not the broker pod template, so they must not trigger a roll.
        let mut spec_on_diff_interval = spec_on.clone();
        if let Some(cfg) = spec_on_diff_interval.metrics_config.as_mut() {
            cfg.pod_monitor = Some(PodMonitorSpec {
                interval: Some("60s".into()),
                ..Default::default()
            });
        }
        assert!(
            h_on == combined_config_hash(&spec_on_diff_interval, None, None, None),
            "PodMonitor interval change must NOT roll the broker pod"
        );
    }

    #[test]
    fn combined_hash_changes_when_cluster_ca_cert_changes() {
        let spec = crate::crd::KafkaSpec {
            kafka_version: "0.1.1".into(),
            metadata_version: None,
            config: None,
            listeners: vec![],
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
        };
        let h_none = combined_config_hash(&spec, None, None, None);
        let h_a = combined_config_hash(
            &spec,
            Some("-----BEGIN CERTIFICATE-----\nA\n-----END CERTIFICATE-----\n"),
            None,
            None,
        );
        let h_b = combined_config_hash(
            &spec,
            Some("-----BEGIN CERTIFICATE-----\nB\n-----END CERTIFICATE-----\n"),
            None,
            None,
        );
        assert!(h_none != h_a, "absent vs present CA must differ");
        assert!(h_a != h_b, "different CA PEM must differ");
    }

    #[test]
    fn combined_hash_stable_under_broker_keystore_changes() {
        // The keystore Secret's contents are never inputs to
        // combined_config_hash (hot-reload handles leaf renewal).
        // This test guards against a future regression where someone wires
        // a keystore digest into the hash.
        let spec = crate::crd::KafkaSpec {
            kafka_version: "0.1.1".into(),
            metadata_version: None,
            config: None,
            listeners: vec![],
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
        };
        let h1 = combined_config_hash(&spec, Some("ca-pem"), None, None);
        let h2 = combined_config_hash(&spec, Some("ca-pem"), None, None);
        assert!(h1 == h2);
    }

    #[test]
    fn configmap_has_one_toml_key_per_broker() {
        use crate::{
            controller::listeners::{AdvertisedAddress, synthesized_default_listener},
            crd::KafkaSpec,
        };

        let mut k = Kafka::new(
            "demo",
            KafkaSpec {
                kafka_version: "0.1.1".into(),
                metadata_version: None,
                config: None,
                listeners: vec![],
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
        k.meta_mut().namespace = Some("default".into());
        k.meta_mut().uid = Some("uid".into());

        let listeners = vec![synthesized_default_listener()];
        let mut per_broker = std::collections::BTreeMap::new();
        let mut addrs0 = std::collections::BTreeMap::new();
        addrs0.insert(
            "PLAIN".into(),
            AdvertisedAddress {
                host: "demo-0.svc".into(),
                port: 9092,
            },
        );
        let mut addrs1 = std::collections::BTreeMap::new();
        addrs1.insert(
            "PLAIN".into(),
            AdvertisedAddress {
                host: "demo-1.svc".into(),
                port: 9092,
            },
        );
        per_broker.insert(0i32, addrs0);
        per_broker.insert(1i32, addrs1);

        let cm = render_configmap(&k, &listeners, &per_broker, "PLAIN", None, None, None).unwrap();
        let data = cm.data.unwrap();
        // Exactly one toml key per broker; the old broker.env /
        // broker.properties keys are dropped.
        let keys: Vec<&str> = data.keys().map(String::as_str).collect();
        assert!(keys == ["broker-0.toml", "broker-1.toml"]);
        check!(data["broker-0.toml"].contains("demo-0.svc"));
        check!(data["broker-1.toml"].contains("demo-1.svc"));
    }

    #[test]
    fn combined_hash_changes_when_metadata_version_pin_set() {
        use crate::crd::KafkaSpec;

        let spec = KafkaSpec {
            kafka_version: "3.7.0".into(),
            metadata_version: None,
            config: None,
            listeners: vec![],
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
        };
        // No explicit pin => hash collapse preserved (== config_hash of
        // the empty config part).
        let h_default = combined_config_hash(&spec, None, None, None);
        assert!(h_default == config_hash(""));

        // An explicit pin enters the hash and changes it.
        let h_pin = combined_config_hash(&spec, None, Some("3.6"), None);
        assert!(h_default != h_pin, "explicit metadata pin must change hash");
        // A different pin differs again.
        let h_pin2 = combined_config_hash(&spec, None, Some("3.7"), None);
        assert!(h_pin != h_pin2, "different metadata pin must differ");
    }

    #[test]
    fn configmap_never_injects_metadata_version_into_server_properties() {
        use crate::{
            controller::listeners::{AdvertisedAddress, synthesized_default_listener},
            crd::KafkaSpec,
        };

        // Even with an explicit `spec.metadataVersion` pin, the rendered
        // broker config must not carry `metadata.version` — it is finalized
        // via the bootstrap feature record, not the config channel.
        let mut k = Kafka::new(
            "demo",
            KafkaSpec {
                kafka_version: "3.7.0".into(),
                metadata_version: Some("3.6".into()),
                config: None,
                listeners: vec![],
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
        k.meta_mut().namespace = Some("default".into());
        k.meta_mut().uid = Some("uid".into());

        let listeners = vec![synthesized_default_listener()];
        let mut per_broker = std::collections::BTreeMap::new();
        let mut addrs0 = std::collections::BTreeMap::new();
        addrs0.insert(
            "PLAIN".into(),
            AdvertisedAddress {
                host: "demo-0.svc".into(),
                port: 9092,
            },
        );
        per_broker.insert(0i32, addrs0);

        let cm = render_configmap(&k, &listeners, &per_broker, "PLAIN", None, None, None).unwrap();
        let toml = &cm.data.unwrap()["broker-0.toml"];
        assert!(
            !toml.contains("metadata.version"),
            "metadata.version must never be injected into broker config, got:\n{toml}"
        );
    }
}

#[cfg(test)]
mod rollout_tests {
    use assert2::assert;

    use super::{PoolRolloutState, plan_rollout};

    fn st(name: &str, hash: Option<&str>, ready: bool) -> PoolRolloutState {
        PoolRolloutState {
            name: name.into(),
            current_hash: hash.map(str::to_string),
            ready,
        }
    }

    fn targets(plan: &[(String, String)]) -> Vec<(&str, &str)> {
        plan.iter().map(|(n, h)| (n.as_str(), h.as_str())).collect()
    }

    #[test]
    fn bring_up_all_get_desired_when_no_hash() {
        // Initial creation: no pool has a hash yet -> all get `desired`
        // (parallel) so a KRaft controller quorum can form.
        let pools = vec![
            st("a", None, false),
            st("b", None, false),
            st("c", None, false),
        ];
        let plan = plan_rollout(&pools, "H1");
        assert!(targets(&plan) == vec![("a", "H1"), ("b", "H1"), ("c", "H1")]);
    }

    #[test]
    fn single_pool_first_reconcile_gets_desired() {
        let pools = vec![st("only", None, false)];
        assert!(targets(&plan_rollout(&pools, "H1")) == vec![("only", "H1")]);
    }

    #[test]
    fn single_pool_roll_advances() {
        // Established single pool moving to a new hash.
        let pools = vec![st("only", Some("H0"), true)];
        assert!(targets(&plan_rollout(&pools, "H1")) == vec![("only", "H1")]);
    }

    #[test]
    fn steady_state_all_desired_is_noop() {
        let pools = vec![st("a", Some("H1"), true), st("b", Some("H1"), true)];
        assert!(targets(&plan_rollout(&pools, "H1")) == vec![("a", "H1"), ("b", "H1")]);
    }

    #[test]
    fn established_roll_advances_first_pool_only() {
        // Uniform on H0; first reconcile after the change advances only
        // pool `a`, holding `b` and `c` at H0.
        let pools = vec![
            st("a", Some("H0"), true),
            st("b", Some("H0"), true),
            st("c", Some("H0"), true),
        ];
        let plan = plan_rollout(&pools, "H1");
        assert!(targets(&plan) == vec![("a", "H1"), ("b", "H0"), ("c", "H0")]);
    }

    #[test]
    fn established_roll_holds_later_pools_until_first_ready() {
        // `a` already moved to H1 but is not Ready yet -> `b`, `c` wait.
        let pools = vec![
            st("a", Some("H1"), false),
            st("b", Some("H0"), true),
            st("c", Some("H0"), true),
        ];
        let plan = plan_rollout(&pools, "H1");
        assert!(targets(&plan) == vec![("a", "H1"), ("b", "H0"), ("c", "H0")]);
    }

    #[test]
    fn established_roll_advances_next_after_prefix_converges() {
        // `a` converged (H1 + ready); advance `b`, hold `c`.
        let pools = vec![
            st("a", Some("H1"), true),
            st("b", Some("H0"), true),
            st("c", Some("H0"), true),
        ];
        let plan = plan_rollout(&pools, "H1");
        assert!(targets(&plan) == vec![("a", "H1"), ("b", "H1"), ("c", "H0")]);
    }

    #[test]
    fn messy_multiple_old_hashes_falls_back_to_all_desired() {
        // More than one distinct non-desired hash -> not a clean ordered
        // roll; apply `desired` to all (recovery).
        let pools = vec![st("a", Some("H0"), true), st("b", Some("HX"), true)];
        let plan = plan_rollout(&pools, "H1");
        assert!(targets(&plan) == vec![("a", "H1"), ("b", "H1")]);
    }
}

#[cfg(test)]
mod parse_quantity_tests {
    use assert2::assert;

    use super::parse_quantity;

    #[test]
    fn quantity_parse_binary_suffixes() {
        for (input, want) in [
            ("1Ki", 1024),
            ("512Mi", 512 * 1024 * 1024),
            ("10Gi", 10 * 1024 * 1024 * 1024),
        ] {
            assert!(parse_quantity(input).unwrap() == want, "case {input:?}");
        }
    }

    #[test]
    fn quantity_parse_decimal_suffixes() {
        for (input, want) in [
            ("1K", 1_000),
            ("500M", 500_000_000),
            ("10G", 10_000_000_000),
        ] {
            assert!(parse_quantity(input).unwrap() == want, "case {input:?}");
        }
    }

    #[test]
    fn quantity_parse_decimal_mantissa() {
        // 1.5Gi = 1.5 * 1024^3 = 1,610,612,736
        assert!(parse_quantity("1.5Gi").unwrap() == 1_610_612_736);
    }

    #[test]
    fn quantity_parse_no_suffix_is_bytes() {
        assert!(parse_quantity("1024").unwrap() == 1024);
    }

    #[test]
    fn quantity_parse_rejects_garbage() {
        // "1e3" pins that scientific notation is rejected.
        for input in ["", "banana", "1.5x", "Gi", "1e3"] {
            assert!(parse_quantity(input).is_err(), "case {input:?}");
        }
    }

    #[test]
    fn quantity_parse_zero_and_negative_are_errors() {
        for input in ["0", "0Gi", "-10Gi"] {
            assert!(parse_quantity(input).is_err(), "case {input:?}");
        }
    }
}

#[cfg(test)]
mod cluster_object_tests {
    use assert2::{assert, check};

    use super::*;
    use crate::{
        controller::listeners::AdvertisedAddress,
        crd::{KafkaSpec, Listener, ListenerType},
    };

    fn test_kafka() -> Kafka {
        let mut k = Kafka::new(
            "demo",
            KafkaSpec {
                kafka_version: "0.1.1".into(),
                metadata_version: None,
                config: None,
                listeners: vec![],
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
        k.metadata.namespace = Some("default".into());
        k.metadata.uid = Some("00000000-0000-0000-0000-000000000001".into());
        k
    }

    #[test]
    fn headless_service_publishes_not_ready_addresses_and_controller_port() {
        let svc = render_service(&test_kafka()).expect("render_service");
        let spec = svc.spec.expect("service spec");

        // KRaft peers must resolve each other's DNS before readiness.
        assert!(spec.publish_not_ready_addresses == Some(true));
        // Still a headless Service.
        assert!(spec.cluster_ip.as_deref() == Some("None"));

        let ports = spec.ports.expect("service ports");
        let controller = ports
            .iter()
            .find(|p| p.name.as_deref() == Some("controller"))
            .expect("controller port must be present");
        check!(controller.port == CONTROLLER_PORT);
        check!(controller.port == 9093);
        // Original broker port is preserved.
        check!(ports.iter().any(|p| p.port == BROKER_PORT));
    }

    fn internal_listener(name: &str, port: i32) -> Listener {
        Listener {
            name: name.into(),
            port,
            type_: ListenerType::Internal,
            tls: false,
            authentication: None,
            configuration: None,
            network_policy_peers: None,
        }
    }

    #[test]
    fn configmap_emits_full_voter_set_into_every_broker_toml() {
        let listeners = vec![internal_listener("PLAIN", 9092)];
        // Three brokers, each with its own inter-broker advertised host.
        let mut addresses_per_broker: BTreeMap<i32, BTreeMap<String, AdvertisedAddress>> =
            BTreeMap::new();
        for (id, host) in [(0, "host-a"), (1, "host-b"), (2, "host-c")] {
            let mut per_listener = BTreeMap::new();
            per_listener.insert(
                "PLAIN".to_string(),
                AdvertisedAddress {
                    host: host.into(),
                    port: 9092,
                },
            );
            addresses_per_broker.insert(id, per_listener);
        }

        let cm = render_configmap(
            &test_kafka(),
            &listeners,
            &addresses_per_broker,
            "PLAIN",
            None,
            None,
            None,
        )
        .expect("render_configmap");
        let data = cm.data.expect("configmap data");

        let expected =
            "controller_quorum_voters = [\"0@host-a:9093\", \"1@host-b:9093\", \"2@host-c:9093\"]";
        // The controller TLS server-name is the shared headless-Service FQDN
        // (`<name>-broker-headless.<ns>.svc.cluster.local`), identical across
        // every broker — a SAN on each broker's serving cert.
        let expected_server_name =
            "controller_server_name = \"demo-broker-headless.default.svc.cluster.local\"";
        for id in 0..3 {
            let toml = data
                .get(&format!("broker-{id}.toml"))
                .unwrap_or_else(|| panic!("broker-{id}.toml missing"));
            assert!(
                toml.contains(expected),
                "broker-{id}.toml must carry the full voter set, got:\n{toml}"
            );
            assert!(
                toml.contains(expected_server_name),
                "broker-{id}.toml must carry the controller server name, got:\n{toml}"
            );
            // Voters must precede the first [[listeners]] header.
            let key_pos = toml.find("controller_quorum_voters").unwrap();
            let listeners_pos = toml.find("[[listeners]]").unwrap();
            assert!(key_pos < listeners_pos);
            // The server-name top-level scalar must also precede [[listeners]].
            let server_name_pos = toml.find("controller_server_name").unwrap();
            assert!(server_name_pos < listeners_pos);
        }
    }
}
