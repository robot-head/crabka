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
    #[allow(dead_code)] // reserved for T8/T10: surface BYO CA parse failures at reconcile time
    #[error("BYO CA malformed: {which}: {reason}")]
    ByoCaMalformed { which: String, reason: String },
    #[error("CA Secret missing: {name}")]
    CaSecretMissing { name: String },
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
/// `Kafka`. Emits one `broker-{id}.toml` key per entry in
/// `addresses_per_broker`, generated by
/// [`crate::controller::listeners::render_broker_toml`].
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
    clients_ca_paths_per_broker: &std::collections::BTreeMap<i32, String>,
) -> Result<ConfigMap, ReconcileError> {
    let name = owner.meta().name.clone().unwrap_or_default();
    let labels = common_labels(&name, &owner.spec.kafka_version, None);

    let mut data = BTreeMap::new();
    let server_properties = owner.spec.config.clone().unwrap_or_default();
    for (broker_id, addrs) in addresses_per_broker {
        let tls_for_broker = tls_per_broker.and_then(|m| m.get(broker_id));
        let toml = crate::controller::listeners::render_broker_toml(
            *broker_id,
            listeners,
            addrs,
            inter_broker_listener_name,
            &server_properties,
            tls_for_broker,
            clients_ca_paths_per_broker,
        );
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
/// of the given content. Used by slice 21 to detect `Kafka.spec.config`
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

/// Slice 25 (+ slice 40): combined hash over user `spec.config`, the
/// canonical listener intent, a `metrics_config.is_some()` bit, and
/// (slice 30) the cluster CA cert PEM.
/// Empty listeners produce empty intent and metrics-unset produces an
/// empty third segment, so the combined hash is identical to the
/// slice-24 hash for an unchanged `spec.config` with neither listeners
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
/// cluster roll; leaf renewal does not (slice 33 hot-reload handles it).
#[must_use]
pub fn combined_config_hash(
    spec: &crate::crd::KafkaSpec,
    cluster_ca_cert_pem: Option<&str>,
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
    // Slice-24 compatibility: when neither listeners nor metricsConfig
    // are set, the hash collapses to `config_hash(config_part)` —
    // byte-identical to the slice-24 hash for the same `spec.config`.
    // This is what makes an in-place upgrade from slice 24 not trigger
    // a hash-driven roll (the unavoidable template-change roll fires
    // separately and once).
    if intent.is_empty() && metrics_part.is_empty() && ca_part.is_empty() {
        return config_hash(&config_part);
    }
    let mut buf = String::with_capacity(
        config_part.len() + 3 + intent.len() + metrics_part.len() + ca_part.len(),
    );
    buf.push_str(&config_part);
    buf.push('\x1F'); // ASCII unit separator
    buf.push_str(&intent);
    buf.push('\x1F');
    buf.push_str(metrics_part);
    buf.push('\x1F');
    buf.push_str(ca_part);
    config_hash(&buf)
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
/// arithmetic). Slice 24 only uses the result for ordered comparison
/// — we never round-trip back to a string, so sub-byte rounding from
/// the `f64` intermediate is acceptable. The in-tree implementation
/// is ~50 lines and saves a workspace dependency; no third-party
/// Quantity parser is wired into Crabka yet.
///
/// # Errors
///
/// Returns a static `&str` describing the parse failure.
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
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

    let mantissa: f64 = mantissa_str
        .parse()
        .map_err(|_| "mantissa is not a valid number")?;
    if !mantissa.is_finite() {
        return Err("mantissa is not finite");
    }
    if mantissa <= 0.0 {
        return Err("quantity must be strictly positive");
    }

    let bytes = mantissa * multiplier as f64;
    if bytes > i128::MAX as f64 {
        return Err("quantity overflows i128");
    }
    Ok(bytes as i128)
}

#[cfg(test)]
mod config_hash_tests {
    use super::*;

    #[test]
    fn config_hash_is_truncated_sha256_hex() {
        // First 16 hex chars (8 bytes) of sha256("hello"):
        //   2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824
        //   ^^^^^^^^^^^^^^^^
        let h = config_hash("hello");
        assert_eq!(h, "2cf24dba5fb0a30e");
        assert_eq!(h.len(), 16, "must fit within K8s 63-char label limit");
    }

    #[test]
    fn config_hash_empty_string() {
        // First 16 hex chars of sha256("").
        let h = config_hash("");
        assert_eq!(h, "e3b0c44298fc1c14");
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
        };
        let h = combined_config_hash(&spec_a, None);
        let h_again = combined_config_hash(&spec_a, None);
        assert_eq!(h, h_again);

        // Slice-24 compat: the hash for empty listeners + no metrics MUST
        // equal `config_hash(serialized broker-properties)`. That's what
        // lets an in-place slice-24 -> slice-25 upgrade avoid a
        // hash-driven roll (the e2e job `kind-upgrade` asserts this
        // against a real slice-24 cluster).
        let slice24_form = "log.retention.hours=24\n";
        assert_eq!(
            h,
            config_hash(slice24_form),
            "combined hash for empty listeners must equal slice-24 config_hash(spec.config)"
        );

        let mut spec_b = spec_a.clone();
        spec_b.listeners = vec![crate::controller::listeners::synthesized_default_listener()];
        spec_b.inter_broker_listener_name = Some("PLAIN".into());
        let h_with_listener = combined_config_hash(&spec_b, None);
        assert_ne!(
            h, h_with_listener,
            "non-empty listener intent must change hash"
        );
    }

    #[test]
    fn combined_hash_flips_when_metrics_config_toggles() {
        use crate::crd::{KafkaSpec, MetricsConfig, PodMonitorSpec};

        let spec_off = KafkaSpec {
            kafka_version: "0.1.1".into(),
            config: None,
            listeners: vec![],
            inter_broker_listener_name: None,
            metrics_config: None,
            network_policy: None,
            cluster_ca: None,
            clients_ca: None,
        };
        let h_off = combined_config_hash(&spec_off, None);

        let mut spec_on = spec_off.clone();
        spec_on.metrics_config = Some(MetricsConfig {
            pod_monitor: Some(PodMonitorSpec::default()),
            ..Default::default()
        });
        let h_on = combined_config_hash(&spec_on, None);
        assert_ne!(
            h_off, h_on,
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
        assert_eq!(
            h_on,
            combined_config_hash(&spec_on_diff_interval, None),
            "PodMonitor interval change must NOT roll the broker pod",
        );
    }

    #[test]
    fn combined_hash_changes_when_cluster_ca_cert_changes() {
        let spec = crate::crd::KafkaSpec {
            kafka_version: "0.1.1".into(),
            config: None,
            listeners: vec![],
            inter_broker_listener_name: None,
            metrics_config: None,
            network_policy: None,
            cluster_ca: None,
            clients_ca: None,
        };
        let h_none = combined_config_hash(&spec, None);
        let h_a = combined_config_hash(
            &spec,
            Some("-----BEGIN CERTIFICATE-----\nA\n-----END CERTIFICATE-----\n"),
        );
        let h_b = combined_config_hash(
            &spec,
            Some("-----BEGIN CERTIFICATE-----\nB\n-----END CERTIFICATE-----\n"),
        );
        assert_ne!(h_none, h_a, "absent vs present CA must differ");
        assert_ne!(h_a, h_b, "different CA PEM must differ");
    }

    #[test]
    fn combined_hash_stable_under_broker_keystore_changes() {
        // The keystore Secret's contents are never inputs to
        // combined_config_hash (slice 33 hot-reload handles leaf renewal).
        // This test guards against a future regression where someone wires
        // a keystore digest into the hash.
        let spec = crate::crd::KafkaSpec {
            kafka_version: "0.1.1".into(),
            config: None,
            listeners: vec![],
            inter_broker_listener_name: None,
            metrics_config: None,
            network_policy: None,
            cluster_ca: None,
            clients_ca: None,
        };
        let h1 = combined_config_hash(&spec, Some("ca-pem"));
        let h2 = combined_config_hash(&spec, Some("ca-pem"));
        assert_eq!(h1, h2);
    }

    #[test]
    fn configmap_has_one_toml_key_per_broker() {
        use crate::controller::listeners::{AdvertisedAddress, synthesized_default_listener};
        use crate::crd::KafkaSpec;

        let mut k = Kafka::new(
            "demo",
            KafkaSpec {
                kafka_version: "0.1.1".into(),
                config: None,
                listeners: vec![],
                inter_broker_listener_name: None,
                metrics_config: None,
                network_policy: None,
                cluster_ca: None,
                clients_ca: None,
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

        let cm = render_configmap(
            &k,
            &listeners,
            &per_broker,
            "PLAIN",
            None,
            &std::collections::BTreeMap::new(),
        )
        .unwrap();
        let data = cm.data.unwrap();
        assert!(data.contains_key("broker-0.toml"));
        assert!(data.contains_key("broker-1.toml"));
        assert!(data["broker-0.toml"].contains("demo-0.svc"));
        assert!(data["broker-1.toml"].contains("demo-1.svc"));
        // Slice 25 drops the old broker.env / broker.properties keys.
        assert!(!data.contains_key("broker.env"));
        assert!(!data.contains_key("broker.properties"));
    }
}

#[cfg(test)]
mod parse_quantity_tests {
    use super::parse_quantity;

    #[test]
    fn quantity_parse_binary_suffixes() {
        assert_eq!(parse_quantity("1Ki").unwrap(), 1024);
        assert_eq!(parse_quantity("512Mi").unwrap(), 512 * 1024 * 1024);
        assert_eq!(parse_quantity("10Gi").unwrap(), 10 * 1024 * 1024 * 1024);
    }

    #[test]
    fn quantity_parse_decimal_suffixes() {
        assert_eq!(parse_quantity("1K").unwrap(), 1_000);
        assert_eq!(parse_quantity("500M").unwrap(), 500_000_000);
        assert_eq!(parse_quantity("10G").unwrap(), 10_000_000_000);
    }

    #[test]
    fn quantity_parse_decimal_mantissa() {
        // 1.5Gi = 1.5 * 1024^3 = 1,610,612,736
        assert_eq!(parse_quantity("1.5Gi").unwrap(), 1_610_612_736);
    }

    #[test]
    fn quantity_parse_no_suffix_is_bytes() {
        assert_eq!(parse_quantity("1024").unwrap(), 1024);
    }

    #[test]
    fn quantity_parse_rejects_garbage() {
        assert!(parse_quantity("").is_err());
        assert!(parse_quantity("banana").is_err());
        assert!(parse_quantity("1.5x").is_err());
        assert!(parse_quantity("Gi").is_err());
        // No scientific notation:
        assert!(parse_quantity("1e3").is_err());
    }

    #[test]
    fn quantity_parse_zero_and_negative_are_errors() {
        assert!(parse_quantity("0").is_err());
        assert!(parse_quantity("0Gi").is_err());
        assert!(parse_quantity("-10Gi").is_err());
    }
}
