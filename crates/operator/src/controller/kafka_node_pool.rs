//! `KafkaNodePool` reconciler.
//!
//! A `KafkaNodePool` describes a group of broker pods that share role,
//! image, and resources. The reconciler renders one `StatefulSet` per
//! pool, owner-ref'd to the pool itself, scheduled into the shared
//! headless `Service` owned by the parent `Kafka` (looked up via the
//! `crabka.io/cluster` label).
//!
//! Constraints: pools must be mixed `{Controller, Broker}`,
//! `replicas` must equal 1, and `nodeIdStart` must lie in `0..=999_999`.
//! Validation errors surface as a `Ready=False` condition without
//! attempting any further reconcile.

use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
    time::Duration,
};

use futures::StreamExt as _;
use k8s_openapi::{
    api::{
        apps::v1::StatefulSet,
        core::v1::{PersistentVolumeClaim, ResourceRequirements},
    },
    apimachinery::pkg::api::resource::Quantity,
};
use kube::{
    Resource, ResourceExt as _,
    api::Api,
    runtime::{
        controller::{Action, Controller},
        watcher,
    },
};
use serde_json::json;

use crate::{
    context::Context,
    controller::common::{
        self, APP_LABEL, BROKER_PORT, DEFAULT_BROKER_IMAGE, ReconcileError, apply_object,
        common_labels, condition, derive_status, owner_ref, parent_version_gate,
    },
    crd::{
        JbodVolume, Kafka, KafkaCondition, KafkaNodePool, KafkaNodePoolStatus, NodeRole, Storage,
    },
};

/// Container port the broker binds for Prometheus `/metrics`
/// when `Kafka.spec.metricsConfig` is `Some`. Kept here next to
/// `BROKER_PORT` so the pod-template renderer has both numbers in one
/// place; referenced by `controller::metrics` to build the
/// `PodMonitor` / `ServiceMonitor` endpoints.
pub(crate) const METRICS_PORT: i32 = 9404;

/// Validation errors for a `KafkaNodePool`. Each variant maps to a
/// distinct condition reason; the operator surfaces the variant as
/// `Ready=False` and does not attempt further reconcile until the spec
/// is corrected.
#[derive(Debug, thiserror::Error)]
pub enum PoolValidationError {
    #[error("spec.roles must equal {{Controller, Broker}}; got {0:?}")]
    RolesNotMixed(Vec<NodeRole>),
    #[error("spec.replicas={0} is unsupported (only 1 allowed)")]
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
    #[error(
        "spec.storage.volumes must list at least 2 disks for Jbod (use PersistentClaim for one disk)"
    )]
    JbodNeedsTwoVolumes(usize),
    #[error("spec.storage.volumes has a duplicate id {0}")]
    JbodDuplicateVolumeId(i32),
    #[error("spec.storage.volumes set changed: adding/removing JBOD disks is not yet supported")]
    JbodVolumesImmutable,
}

/// Validate a `KafkaNodePool` spec against its invariants.
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
    match pool.spec.storage.as_ref() {
        Some(Storage::PersistentClaim(pc)) => {
            common::parse_quantity(&pc.size)
                .map_err(|why| PoolValidationError::StorageSizeInvalid(pc.size.clone(), why))?;
        }
        Some(Storage::Jbod(j)) => {
            // A single-disk "JBOD" is just a PersistentClaim and would render
            // an identical one-PVC StatefulSet, making the storage kind
            // ambiguous on re-reconcile. Require >= 2 disks; the count is the
            // observed-kind discriminator (see `observed_storage_kind`).
            if j.volumes.len() < 2 {
                return Err(PoolValidationError::JbodNeedsTwoVolumes(j.volumes.len()));
            }
            let mut seen = HashSet::new();
            for v in &j.volumes {
                if !seen.insert(v.id) {
                    return Err(PoolValidationError::JbodDuplicateVolumeId(v.id));
                }
                common::parse_quantity(&v.size)
                    .map_err(|why| PoolValidationError::StorageSizeInvalid(v.size.clone(), why))?;
            }
        }
        None | Some(Storage::Ephemeral) => {}
    }
    Ok(())
}

/// JBOD volumes sorted ascending by id. Empty for non-JBOD storage.
/// Sorting makes the rendered `StatefulSet` deterministic regardless of
/// the order disks are listed in the spec.
fn jbod_volumes_sorted(storage: Option<&Storage>) -> Vec<JbodVolume> {
    match storage {
        Some(Storage::Jbod(j)) => {
            let mut v = j.volumes.clone();
            v.sort_by_key(|vol| vol.id);
            v
        }
        _ => Vec::new(),
    }
}

/// PVC-template name + pod mount path for one JBOD disk. The primary
/// (lowest-id) disk reuses the `data` / `/var/lib/crabka/data`
/// so the metadata raft log, the init container, and the cluster-level
/// broker TOML (`log_dir = "/var/lib/crabka/data"`) are all unchanged.
/// Every other disk `id = N` lives at `data-{N}` / `/var/lib/crabka/data-{N}`.
fn jbod_mount(volume_id: i32, is_primary: bool) -> (String, String) {
    if is_primary {
        ("data".to_string(), "/var/lib/crabka/data".to_string())
    } else {
        (
            format!("data-{volume_id}"),
            format!("/var/lib/crabka/data-{volume_id}"),
        )
    }
}

/// `(name, mount_path)` for every non-primary JBOD disk, sorted by id.
/// Empty for non-JBOD storage. These become the broker container's extra
/// `volumeMounts` and the `CRABKA_EXTRA_LOG_DIRS` env value.
fn jbod_extra_mounts(storage: Option<&Storage>) -> Vec<(String, String)> {
    jbod_volumes_sorted(storage)
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != 0)
        .map(|(_, v)| jbod_mount(v.id, false))
        .collect()
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
rm -rf /var/lib/crabka/data/lost+found\n\
if [ ! -f /var/lib/crabka/data/.formatted ]; then\n\
  /usr/bin/crabka format --log-dir /var/lib/crabka/data --cluster-id \"$CRABKA_CLUSTER_ID\" --release-version \"$CRABKA_METADATA_VERSION\"\n\
  touch /var/lib/crabka/data/.formatted\n\
fi\n\
printf '%s' \"$NODE_ID\" > /var/lib/crabka/data/.node-id\n";

// Main script (zero-metrics variant). Retained as a const so the
// `build_main_script_cases` test gives a
// loud failure if the upgrade-stability contract breaks.
//
// Copies the per-broker TOML from the ConfigMap volume into a writable
// tmpfs path (the root FS is read-only), then execs the broker with
// `--config-file` so it picks up advertised listeners and all other
// per-broker config from the rendered TOML. `/run/crabka` is backed by
// an emptyDir volume (see `render_storage`) so it's writable even with
// `readOnlyRootFilesystem: true`.
const MAIN_SCRIPT: &str = "set -eu\n\
NODE_ID=\"$(cat /var/lib/crabka/data/.node-id)\"\n\
cp /etc/crabka/config/broker-${NODE_ID}.toml /run/crabka/broker.toml\n\
exec /usr/bin/crabka-broker \\\n  --config-file=/run/crabka/broker.toml \\\n  --broker-id=\"${NODE_ID}\"\n";

/// Build the broker container's main shell script. The disabled variant
/// returns `MAIN_SCRIPT` byte-for-byte so a cluster with
/// `metrics_config: None` produces a byte-identical pod template (the
/// pod-template-hash stays put, so no broker pod rolls). The enabled
/// variant appends `--metrics-listen-addr=0.0.0.0:9404` so the broker
/// binds its Prometheus endpoint.
///
/// The enabled variant is a separate string literal (no `format!`) so a
/// test failure shows the full expected text inline rather than a
/// templated fragment.
fn build_main_script(metrics_enabled: bool) -> String {
    if !metrics_enabled {
        return MAIN_SCRIPT.to_string();
    }
    // NB: the enabled-variant body intentionally duplicates the disabled
    // one. See the `build_main_script_cases` test — keeping the literals
    // separate is the upgrade-stability
    // contract. Don't refactor to a `format!`.
    "set -eu\n\
     NODE_ID=\"$(cat /var/lib/crabka/data/.node-id)\"\n\
     cp /etc/crabka/config/broker-${NODE_ID}.toml /run/crabka/broker.toml\n\
     exec /usr/bin/crabka-broker \\\n  \
       --config-file=/run/crabka/broker.toml \\\n  \
       --broker-id=\"${NODE_ID}\" \\\n  \
       --metrics-listen-addr=0.0.0.0:9404\n"
        .to_string()
}

fn render_init_container(
    broker_image: &str,
    secret_name: &str,
    node_id_start: i32,
    metadata_version: &str,
) -> serde_json::Value {
    json!({
        "name": "format",
        "image": broker_image,
        "command": ["/bin/sh", "-c"],
        "args": [INIT_SCRIPT],
        "env": [
            { "name": "NODE_ID_START", "value": node_id_start.to_string() },
            { "name": "CRABKA_CLUSTER_ID", "valueFrom": { "secretKeyRef": { "name": secret_name, "key": "clusterId" } } },
            { "name": "CRABKA_METADATA_VERSION", "value": metadata_version.to_string() }
        ],
        "volumeMounts": [{ "name": "data", "mountPath": "/var/lib/crabka/data" }],
        "securityContext": {
            "allowPrivilegeEscalation": false,
            "readOnlyRootFilesystem": true,
            "capabilities": { "drop": ["ALL"] }
        }
    })
}

#[allow(clippy::too_many_arguments)] // pure render helper: each arg names one independent feature toggle, struct-ifying buys nothing
#[allow(clippy::fn_params_excessive_bools)] // each bool is an independent presence toggle (metrics / logging / gssapi keytab / krb5.conf)
#[allow(clippy::too_many_lines)] // linear: per-feature env / mount segments are independent
fn render_broker_container(
    broker_image: &str,
    secret_name: &str,
    cm_name: &str,
    resources: &ResourceRequirements,
    metrics_enabled: bool,
    logging_enabled: bool,
    jbod_extra_mounts: &[(String, String)],
    oauth_jwks_trust_mount: Option<&str>,
    oauth_introspection_mount_path: Option<&str>,
    gssapi_keytab: bool,
    krb5_conf: bool,
    delegation_token: Option<&crate::crd::kafka::DelegationTokenConfig>,
    tiered_storage: Option<&crate::crd::kafka::TieredStorage>,
    tracing: Option<&crate::crd::kafka::Tracing>,
) -> serde_json::Value {
    use crate::crd::kafka::TieredStorageType;
    // Local pulls a writable emptyDir mount; S3 pulls credential env vars.
    let tier_storage_local = matches!(
        tiered_storage.map(|t| t.kind),
        Some(TieredStorageType::Local)
    );
    let mut ports = vec![json!({
        "containerPort": BROKER_PORT, "name": "kafka-internal", "protocol": "TCP"
    })];
    if metrics_enabled {
        ports.push(json!({
            "containerPort": METRICS_PORT, "name": "metrics", "protocol": "TCP"
        }));
    }
    let mut env = vec![
        json!({ "name": "POD_NAME", "valueFrom": { "fieldRef": { "fieldPath": "metadata.name" } } }),
        json!({ "name": "POD_NAMESPACE", "valueFrom": { "fieldRef": { "fieldPath": "metadata.namespace" } } }),
        json!({ "name": "CRABKA_CLUSTER_ID", "valueFrom": { "secretKeyRef": { "name": secret_name, "key": "clusterId" } } }),
    ];
    // When spec.logging is set, point RUST_LOG at the broker
    // ConfigMap's `rust.log` key (rendered by `common::render_configmap`).
    // `optional: true` keeps the pod bootable if the key is briefly absent
    // (e.g. external-ConfigMap resolution pending) — the broker then falls
    // back to its built-in default filter. The env entry is gated on
    // `logging_enabled` so logging-off clusters keep a byte-identical pod
    // template (no spurious roll).
    if logging_enabled {
        env.push(json!({
            "name": "RUST_LOG",
            "valueFrom": {
                "configMapKeyRef": { "name": cm_name, "key": "rust.log", "optional": true }
            }
        }));
    }
    // When storage is JBOD, tell the broker about the extra data
    // disks via `CRABKA_EXTRA_LOG_DIRS` (the broker reads this env, splits on
    // commas, and spreads partitions across `[log_dir] + extras`). The
    // primary disk stays the broker's `log_dir` (`/var/lib/crabka/data`), so
    // it's excluded here. The env is omitted entirely for non-JBOD pools.
    if !jbod_extra_mounts.is_empty() {
        let value = jbod_extra_mounts
            .iter()
            .map(|(_, path)| path.as_str())
            .collect::<Vec<_>>()
            .join(",");
        env.push(json!({ "name": "CRABKA_EXTRA_LOG_DIRS", "value": value }));
    }
    // When `Kafka.spec.delegationToken` is set, source the
    // broker's master HMAC key from the referenced Secret via
    // `valueFrom.secretKeyRef`. Baking the env entry into the
    // operator-rendered pod template removes the
    // `kubectl set env` race: every SSA reconcile re-asserts the env
    // entry, so it can't drift from beneath the broker. The broker's
    // config layer reads `CRABKA_DELEGATION_TOKEN_SECRET_KEY`
    // (env wins over TOML) and flips the four delegation-token RPCs
    // from `DELEGATION_TOKEN_AUTH_DISABLED` (err 61) to live. Omitted
    // entirely when `delegation_token` is `None`.
    if let Some(dt) = delegation_token {
        let key = dt.secret_key_ref.key.as_deref().unwrap_or("secret-key");
        env.push(json!({
            "name": "CRABKA_DELEGATION_TOKEN_SECRET_KEY",
            "valueFrom": {
                "secretKeyRef": {
                    "name": dt.secret_key_ref.name,
                    "key": key,
                }
            }
        }));
    }
    // KIP-405: S3 credentials from operator-CRD →
    // broker pod via the standard AWS env vars. `object_store`'s
    // `AmazonS3Builder` resolves these through the AWS credential chain
    // when the broker TOML omits explicit `access_key_id` /
    // `secret_access_key` (which it always does on the operator path —
    // the operator never copies the secret value into the TOML).
    // When `credentials` is absent on the S3 spec, the env entries are
    // omitted and the broker pod inherits whatever IRSA / instance-
    // profile auth the cluster already has.
    if let Some(s3) = tiered_storage
        .filter(|t| matches!(t.kind, TieredStorageType::S3))
        .and_then(|t| t.s3.as_ref())
        && let Some(creds) = &s3.credentials
    {
        let ak_key = creds
            .access_key_id
            .key
            .as_deref()
            .unwrap_or("access-key-id");
        let sk_key = creds
            .secret_access_key
            .key
            .as_deref()
            .unwrap_or("secret-access-key");
        env.push(json!({
            "name": "AWS_ACCESS_KEY_ID",
            "valueFrom": {
                "secretKeyRef": {
                    "name": creds.access_key_id.name,
                    "key": ak_key,
                }
            }
        }));
        env.push(json!({
            "name": "AWS_SECRET_ACCESS_KEY",
            "valueFrom": {
                "secretKeyRef": {
                    "name": creds.secret_access_key.name,
                    "key": sk_key,
                }
            }
        }));
    }
    // Cluster-wide tracing → broker pod env vars. The broker's
    // `TelemetryConfig::from_env` reads these and installs the OTLP
    // tracer at startup. Omitted entirely when `tracing` is `None` so
    // the rendered pod template stays byte-identical to the established
    // shape for non-tracing clusters.
    if let Some(t) = tracing
        && let crate::crd::kafka::TracingType::Otlp = t.kind
        && let Some(otlp) = t.otlp.as_ref()
    {
        env.push(json!({ "name": "CRABKA_OTLP_ENABLED", "value": "true" }));
        env.push(json!({ "name": "CRABKA_OTLP_ENDPOINT", "value": otlp.endpoint }));
        if let Some(p) = otlp.protocol {
            env.push(json!({
                "name": "CRABKA_OTLP_PROTOCOL",
                "value": p.as_env_value(),
            }));
        }
        if let Some(r) = otlp.sample_ratio {
            env.push(json!({
                "name": "CRABKA_OTLP_SAMPLE_RATIO",
                "value": r.to_string(),
            }));
        }
        if let Some(name) = otlp.service_name.as_deref() {
            env.push(json!({ "name": "OTEL_SERVICE_NAME", "value": name }));
        }
        if let Some(t) = otlp.timeout_secs {
            env.push(json!({
                "name": "CRABKA_OTLP_TIMEOUT_SECS",
                "value": t.to_string(),
            }));
        }
    }
    let main_script = build_main_script(metrics_enabled);
    let mut volume_mounts = vec![
        json!({ "name": "data", "mountPath": "/var/lib/crabka/data" }),
        json!({ "name": "broker-config", "mountPath": "/etc/crabka/config", "readOnly": true }),
        json!({ "name": "broker-runtime", "mountPath": "/run/crabka" }),
        json!({ "name": "cluster-ca-cert", "mountPath": "/etc/crabka/cluster-ca", "readOnly": true }),
        json!({ "name": "broker-tls", "mountPath": "/etc/crabka/broker-tls", "readOnly": true }),
        json!({ "name": "clients-ca-cert", "mountPath": "/etc/crabka/clients-ca", "readOnly": true }),
    ];
    for (name, path) in jbod_extra_mounts {
        volume_mounts.push(json!({ "name": name, "mountPath": path }));
    }
    // When the parent Kafka has an OAuth listener with
    // `tls_trusted_certificates`, mount the managed
    // `{kafka}-oauth-jwks-trust` Secret at
    // `/etc/crabka/oauth-jwks-trust` so the broker can read
    // `ca.crt` for JWKS-endpoint TLS verification (the broker's
    // generated TOML's `idp_tls_trust` points at this path).
    if let Some(mount_path) = oauth_jwks_trust_mount {
        volume_mounts.push(json!({
            "name": "oauth-jwks-trust",
            "mountPath": mount_path,
            "readOnly": true,
        }));
    }
    // When the parent Kafka has an OAuth listener configured
    // for introspection mode (`accessTokenIsJwt: false` + `clientSecret`),
    // mount the source Secret directly at
    // `/etc/crabka/oauth-introspection` so the broker can read the
    // introspection-endpoint Basic-Auth client secret from
    // `<mount>/client-secret` (matching the broker TOML render).
    if let Some(mount_path) = oauth_introspection_mount_path {
        volume_mounts.push(json!({
            "name": "oauth-introspection-secret",
            "mountPath": mount_path,
            "readOnly": true,
        }));
    }
    // SASL/GSSAPI: mount the service keytab at the fixed directory
    // `/etc/crabka/gssapi-keytab` (projected item lands at
    // `keytab`, so the broker reads `GSSAPI_KEYTAB_PATH`).
    if gssapi_keytab {
        volume_mounts.push(json!({
            "name": "gssapi-keytab",
            "mountPath": crate::controller::listeners::GSSAPI_KEYTAB_DIR,
            "readOnly": true,
        }));
    }
    // Optional krb5.conf: mount at `/etc/crabka/krb5/krb5.conf` and
    // point the Kerberos libraries at it via `KRB5_CONFIG`.
    if krb5_conf {
        volume_mounts.push(json!({
            "name": "krb5-conf",
            "mountPath": "/etc/crabka/krb5",
            "readOnly": true,
        }));
        env.push(json!({ "name": "KRB5_CONFIG", "value": "/etc/crabka/krb5/krb5.conf" }));
    }
    // KIP-405: mount the `tier-storage` emptyDir
    // read-write at the broker's `remote_log_storage_dir` (matches
    // `[remote_storage].storage_dir` in the rendered TOML). Local-only
    // — the S3 backend writes through `object_store` directly to the
    // bucket and needs no pod-local scratch space. Omitted entirely
    // when tiered storage is off (or S3), so non-Local clusters keep
    // a byte-identical pod template.
    if tier_storage_local {
        volume_mounts.push(json!({
            "name": "tier-storage",
            "mountPath": crate::controller::listeners::TIER_STORAGE_PATH,
        }));
    }
    // KIP-405: GCS with an explicit service-account JSON key. Unlike S3
    // (env-var credentials), GCS credentials are a FILE and `object_store`'s
    // GCS builder reads the path directly, so the operator mounts the
    // referenced Secret read-only at `GCS_CREDENTIALS_DIR` (the key.json
    // projection is set up by `render_storage`); the broker TOML's
    // `service_account_path` points at `<dir>/key.json`. Keyless Workload
    // Identity / ADC (credentials absent) mounts nothing — the pod resolves
    // credentials from its bound KSA via the metadata server.
    if tiered_storage
        .filter(|t| matches!(t.kind, TieredStorageType::Gcs))
        .and_then(|t| t.gcs.as_ref())
        .is_some_and(|g| g.credentials.is_some())
    {
        volume_mounts.push(json!({
            "name": "gcs-credentials",
            "mountPath": crate::controller::listeners::GCS_CREDENTIALS_DIR,
            "readOnly": true,
        }));
    }
    json!({
        "name": "broker",
        "image": broker_image,
        "command": ["/bin/sh", "-c"],
        "args": [main_script],
        "env": env,
        "ports": ports,
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
        // A controller-voter that is restarted uncleanly (e.g. a deleted pod)
        // can take tens of seconds to rejoin the KRaft quorum before it opens
        // the data port. The startupProbe grants that grace and suppresses the
        // liveness probe until the broker is actually serving on BROKER_PORT,
        // so a slow rejoin is never SIGKILLed mid-rejoin into a crash loop
        // (which would also keep flapping the broker and block leader failover).
        "startupProbe": {
            "tcpSocket": { "port": BROKER_PORT },
            "periodSeconds": 5,
            "failureThreshold": 30
        },
        "resources": resources,
        "volumeMounts": volume_mounts,
        "securityContext": {
            "allowPrivilegeEscalation": false,
            "readOnlyRootFilesystem": true,
            "capabilities": { "drop": ["ALL"] }
        }
    })
}

/// Build one `volumeClaimTemplate` for a single PVC: `accessModes`,
/// requested `size`, optional `storageClassName`, and inherited pod
/// labels (so the GC selector matches the bound PVC).
fn pvc_template(
    name: &str,
    size: &str,
    class: Option<&str>,
    pod_labels: &BTreeMap<String, String>,
) -> serde_json::Value {
    let mut template = json!({
        "metadata": {
            "name": name,
            "labels": pod_labels,
        },
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {
                "requests": { "storage": size }
            }
        }
    });
    if let Some(class) = class {
        template["spec"]["storageClassName"] = serde_json::Value::String(class.to_string());
    }
    template
}

/// Build the `StatefulSet`'s pod-volume entries and its
/// `volumeClaimTemplates` based on the pool's `Storage` setting.
/// Returns `(pod_volumes_json, volume_claim_templates)`. An empty
/// templates vec means "no PVCs" (the `Ephemeral` path).
///
/// The returned `volumes` array always includes the `broker-config`
/// `ConfigMap` volume (unconditional). For `PersistentClaim` / `Jbod` the
/// `data` (and `data-{id}`) volume entries are omitted: the `StatefulSet`
/// controller mounts each PVC into the pod under the template name
/// automatically, so an explicit pod-volume entry would conflict.
#[allow(clippy::too_many_lines)] // each branch + secret mount is independent
#[allow(clippy::too_many_arguments)] // pure render helper: each arg names one independent secret-mount / storage toggle
fn render_storage(
    storage: Option<&Storage>,
    pod_labels: &BTreeMap<String, String>,
    parent_name: &str,
    oauth_jwks_trust_secret: Option<&str>,
    oauth_introspection_mount: Option<&crate::controller::kafka::OauthIntrospectionMount>,
    gssapi_keytab: Option<&crate::controller::kafka::GssapiKeytabMount>,
    krb5_conf: Option<(&str, &str)>,
    tier_storage_local: bool,
    tier_storage_persistence: Option<&crate::crd::kafka::TieredStoragePersistence>,
    gcs_credentials: Option<&crate::crd::kafka::GcsCredentials>,
) -> (serde_json::Value, Vec<serde_json::Value>) {
    let broker_config_vol = json!({
        "name": "broker-config",
        "configMap": { "name": format!("{parent_name}-broker-config") }
    });
    // Writable emptyDir for the init script's `/run/crabka/broker.toml`
    // assembly. The container runs `readOnlyRootFilesystem: true`, so
    // `mkdir /run/crabka` would fail without an explicit mount.
    let runtime_vol = json!({ "name": "broker-runtime", "emptyDir": {} });
    // CA + broker keystore Secrets mounted read-only into broker pods.
    let cluster_ca_cert_vol = json!({
        "name": "cluster-ca-cert",
        "secret": {
            "secretName": format!("{parent_name}-cluster-ca-cert"),
            "defaultMode": 0o400_i32,
        }
    });
    let broker_tls_vol = json!({
        "name": "broker-tls",
        "secret": {
            "secretName": format!("{parent_name}-kafka-brokers"),
            "defaultMode": 0o400_i32,
        }
    });
    let clients_ca_cert_vol = json!({
        "name": "clients-ca-cert",
        "secret": {
            "secretName": format!("{parent_name}-clients-ca-cert"),
            "defaultMode": 0o400_i32,
        }
    });
    let (mut volumes, mut templates) = match storage {
        None | Some(Storage::Ephemeral) => {
            let volumes = json!([
                { "name": "data", "emptyDir": {} },
                broker_config_vol,
                runtime_vol,
                cluster_ca_cert_vol,
                broker_tls_vol,
                clients_ca_cert_vol,
            ]);
            (volumes, Vec::new())
        }
        Some(Storage::PersistentClaim(pc)) => {
            let template = pvc_template("data", &pc.size, pc.class.as_deref(), pod_labels);
            (
                json!([
                    broker_config_vol,
                    runtime_vol,
                    cluster_ca_cert_vol,
                    broker_tls_vol,
                    clients_ca_cert_vol,
                ]),
                vec![template],
            )
        }
        Some(Storage::Jbod(_)) => {
            // One PVC template per disk: the lowest-id disk is `data`
            // (primary / metadata), the rest are `data-{id}`.
            let jbod_vols = jbod_volumes_sorted(storage);
            let templates = jbod_vols
                .iter()
                .enumerate()
                .map(|(i, v)| {
                    let (name, _) = jbod_mount(v.id, i == 0);
                    pvc_template(&name, &v.size, v.class.as_deref(), pod_labels)
                })
                .collect();
            (
                json!([
                    broker_config_vol,
                    runtime_vol,
                    cluster_ca_cert_vol,
                    broker_tls_vol,
                    clients_ca_cert_vol,
                ]),
                templates,
            )
        }
    };
    // Append the managed `{kafka}-oauth-jwks-trust` Secret as
    // a read-only pod volume when an OAuth listener carries
    // `tls_trusted_certificates`. The matching volumeMount is appended
    // by `render_broker_container`. Same `defaultMode` (0o400) as the
    // cluster-CA / broker-TLS / clients-CA Secret volumes above.
    if let Some(secret_name) = oauth_jwks_trust_secret {
        volumes
            .as_array_mut()
            .expect("render_storage built `volumes` via json!([...])")
            .push(json!({
                "name": "oauth-jwks-trust",
                "secret": {
                    "secretName": secret_name,
                    "defaultMode": 0o400_i32,
                }
            }));
    }
    // Append the user-owned source Secret as a read-only pod
    // volume when an OAuth listener is configured for introspection mode.
    // The projected `items` mapping pins the user's source key to a
    // fixed in-pod path (`client-secret`) so the broker always reads
    // `/etc/crabka/oauth-introspection/client-secret` regardless of
    // what the user named their key. Same 0o400 mode as the other
    // Secret volumes above.
    if let Some(mount) = oauth_introspection_mount {
        volumes
            .as_array_mut()
            .expect("render_storage built `volumes` via json!([...])")
            .push(json!({
                "name": "oauth-introspection-secret",
                "secret": {
                    "secretName": mount.secret_name,
                    "items": [{ "key": mount.key, "path": "client-secret" }],
                    "defaultMode": 0o400_i32,
                }
            }));
    }
    // SASL/GSSAPI: append the user-owned keytab Secret as a read-only
    // pod volume, pinning the user's source key to the fixed in-pod
    // path `keytab` (so the broker reads `GSSAPI_KEYTAB_PATH` =
    // `/etc/crabka/gssapi-keytab/keytab` regardless of key name).
    if let Some(m) = gssapi_keytab {
        volumes
            .as_array_mut()
            .expect("render_storage built `volumes` via json!([...])")
            .push(json!({
                "name": "gssapi-keytab",
                "secret": {
                    "secretName": m.secret_name,
                    "items": [{ "key": m.key, "path": "keytab" }],
                    "defaultMode": 0o400_i32,
                }
            }));
    }
    // Optional krb5.conf: append the user-owned Secret as a read-only
    // pod volume, pinning the user's key to `krb5.conf` so the broker
    // reads `/etc/crabka/krb5/krb5.conf` (matching `KRB5_CONFIG`).
    if let Some((secret_name, key)) = krb5_conf {
        volumes
            .as_array_mut()
            .expect("render_storage built `volumes` via json!([...])")
            .push(json!({
                "name": "krb5-conf",
                "secret": {
                    "secretName": secret_name,
                    "items": [{ "key": key, "path": "krb5.conf" }],
                    "defaultMode": 0o400_i32,
                }
            }));
    }
    // KIP-405: append a writable `tier-storage` volume
    // when the parent `Kafka.spec.tieredStorage.type == Local`. When
    // `spec.tieredStorage.persistence` is set,
    // render a `volumeClaimTemplate` named `tier-storage` instead of
    // an `emptyDir`, and let the StatefulSet controller mount the
    // bound PVC into each pod automatically (so the explicit
    // pod-volume entry must NOT be added in the PVC case, exactly as
    // for the data PVC). S3 adds no pod-local volume.
    if tier_storage_local {
        if let Some(p) = tier_storage_persistence {
            templates.push(pvc_template(
                "tier-storage",
                &p.size,
                p.class.as_deref(),
                pod_labels,
            ));
        } else {
            volumes
                .as_array_mut()
                .expect("render_storage built `volumes` via json!([...])")
                .push(json!({
                    "name": "tier-storage",
                    "emptyDir": {}
                }));
        }
    }
    // KIP-405: GCS with an explicit service-account JSON key. Append the
    // user-owned source Secret as a read-only pod volume, pinning the
    // user's source key to the fixed in-pod path `key.json` so the broker
    // reads `<GCS_CREDENTIALS_DIR>/key.json` (matching the rendered
    // `service_account_path`) regardless of how the user named their key.
    // Same 0o400 mode as the other Secret volumes. Omitted entirely for
    // keyless Workload Identity / ADC (`credentials` absent).
    if let Some(creds) = gcs_credentials {
        let key = creds
            .service_account_key
            .key
            .as_deref()
            .unwrap_or("secret-key");
        volumes
            .as_array_mut()
            .expect("render_storage built `volumes` via json!([...])")
            .push(json!({
                "name": "gcs-credentials",
                "secret": {
                    "secretName": creds.service_account_key.name,
                    "items": [{
                        "key": key,
                        "path": crate::controller::listeners::GCS_CREDENTIALS_FILE,
                    }],
                    "defaultMode": 0o400_i32,
                }
            }));
    }
    (volumes, templates)
}

/// Build the `StatefulSet`'s `persistentVolumeClaimRetentionPolicy`
/// block when any PVC is in play. Returns `None` only when neither
/// the pool's data storage nor the tier-storage cache is a PVC.
///
/// A `StatefulSet`'s retention policy applies set-wide to every
/// `volumeClaimTemplate`. Validation upstream ensures that
/// when both data and tier PVCs exist, their `delete_claim` flags
/// match — so we can pick the pool's value when present and the tier
/// value otherwise.
fn render_pvc_retention_policy(
    storage: Option<&Storage>,
    tier_persistence: Option<&crate::crd::kafka::TieredStoragePersistence>,
) -> Option<serde_json::Value> {
    let delete_claim = match storage {
        Some(Storage::PersistentClaim(pc)) => pc.delete_claim,
        Some(Storage::Jbod(j)) => j.delete_claim,
        _ => {
            let p = tier_persistence?;
            p.delete_claim
        }
    };
    Some(json!({
        "whenDeleted": if delete_claim { "Delete" } else { "Retain" },
        "whenScaled": "Retain",
    }))
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
#[allow(clippy::too_many_lines)] // linear render pipeline: pod template + storage + per-feature wiring
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

    // Resolve the metadata version to seed into `crabka format
    // --release-version`. Priority: finalized status > operator-pinned spec
    // > kafka_version (MAX default). The kafka reconciler writes the finalized
    // value to `status.metadataVersion` once version validation passes; on a
    // first-ever reconcile (status absent) we fall back to the spec pin, then
    // to the kafka_version so the formatter always receives a concrete level.
    let chosen = parent
        .status
        .as_ref()
        .and_then(|s| s.metadata_version.as_deref())
        .or(parent.spec.metadata_version.as_deref())
        .unwrap_or(&parent.spec.kafka_version);
    // Normalize to major.minor — `crabka format --release-version` resolves
    // a short form (e.g. "3.7"); a 3-part version string would not resolve.
    let normalized = crate::version::KafkaVersion::parse(chosen)
        .map_or_else(|_| chosen.to_string(), |v| v.short());
    // `crabka format --release-version` hard-rejects a value outside the
    // broker's supported metadata.version table, which would crash-loop the
    // init container. The broker always supports [MIN, MAX] regardless of the
    // kafka_version compat label, so clamp an unsupported/out-of-range value
    // to MAX. (`evaluate` still surfaces the misconfig as KafkaVersionValid=False.)
    let resolved_metadata_version =
        if crabka_metadata::metadata_version::from_version_string(&normalized).is_some() {
            normalized
        } else {
            crabka_metadata::metadata_version::from_feature_level(
                crabka_metadata::metadata_version::METADATA_VERSION_MAX,
            )
            .expect("MAX level is in the table")
            .short()
            .to_string()
        };
    let init = render_init_container(
        broker_image,
        &secret_name,
        pool.spec.node_id_start,
        &resolved_metadata_version,
    );
    let metrics_enabled = parent.spec.metrics_config.is_some();
    let logging_enabled = parent.spec.logging.is_some();
    let cm_name = format!("{parent_name}-broker-config");
    let jbod_extra = jbod_extra_mounts(pool.spec.storage.as_ref());
    // Derive the managed `{kafka}-oauth-jwks-trust` Secret
    // name from the parent Kafka CR's listeners. `Some` iff at least
    // one OAuth listener has non-empty `tls_trusted_certificates` —
    // i.e. iff `kafka.rs::reconcile_kafka` actually upserted the
    // Secret. The naming is shared with `kafka.rs` via the
    // [`controller::kafka::oauth_jwks_trust_secret_name`] helper so
    // both sides stay in lockstep without re-doing the bundle
    // assembly here.
    let oauth_jwks_trust_secret = crate::controller::kafka::oauth_jwks_trust_secret_name(parent);
    // The mount path is a stable contract with the
    // broker (it reads the trust bundle from
    // `/etc/crabka/oauth-jwks-trust/ca.crt`), and matches the
    // `idp_tls_trust` TOML key rendered by the listener reconciler.
    let oauth_jwks_trust_mount = oauth_jwks_trust_secret
        .as_deref()
        .map(|_| "/etc/crabka/oauth-jwks-trust");
    // Derive the OAUTHBEARER introspection client-secret
    // mount info from the parent CR's listeners (mirrors the
    // jwks-trust derivation above). `Some` iff at least one OAuth
    // listener uses `accessTokenIsJwt: false` with a `clientSecret`
    // ref. The mount path is a stable contract with the TOML render —
    // the broker reads `<mount>/client-secret` regardless of the
    // user's source key name.
    let oauth_introspection_mount =
        crate::controller::kafka::oauth_introspection_secret_mount(parent);
    let oauth_introspection_mount_path = oauth_introspection_mount
        .as_ref()
        .map(|_| "/etc/crabka/oauth-introspection");
    // SASL/GSSAPI: the keytab Secret ref from the (first) `type: gssapi`
    // listener, and the optional `spec.krb5ConfSecretRef`. Derived the
    // same way the introspection mount is — the pool reconciler mounts
    // the user-owned source Secrets directly via projected items.
    let gssapi_keytab_mount = crate::controller::kafka::gssapi_keytab_mount(parent);
    let krb5_conf_mount = crate::controller::kafka::krb5_conf_mount(parent);
    // KIP-405: cluster-wide tier-storage selector.
    // `Local` adds a writable `tier-storage` emptyDir + matching
    // volumeMount at `TIER_STORAGE_PATH`. `S3` adds no pod volume — the
    // broker writes through `object_store` directly to the bucket — but
    // wires the configured `Secret` keys onto the pod as
    // `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env vars.
    let tiered_storage = parent.spec.tiered_storage.as_ref();
    let tier_storage_local = matches!(
        tiered_storage.map(|t| t.kind),
        Some(crate::crd::kafka::TieredStorageType::Local)
    );
    let main = render_broker_container(
        broker_image,
        &secret_name,
        &cm_name,
        &resources,
        metrics_enabled,
        logging_enabled,
        &jbod_extra,
        oauth_jwks_trust_mount,
        oauth_introspection_mount_path,
        gssapi_keytab_mount.is_some(),
        krb5_conf_mount.is_some(),
        parent.spec.delegation_token.as_ref(),
        tiered_storage,
        parent.spec.tracing.as_ref(),
    );

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

    let tier_storage_persistence = tiered_storage.and_then(|t| t.persistence.as_ref());

    // K8s StatefulSets have a single set-wide PVC retention
    // policy; per-template overrides don't exist. When the pool has both
    // a data PVC and a tier PVC, their `delete_claim` flags must match
    // (otherwise we'd silently pick one and lose data in a way the user
    // didn't intend). Pool-Ephemeral skips this check — there's no data
    // PVC to collide with.
    if let Some(tp) = tier_storage_persistence {
        let pool_data_delete_claim = match pool.spec.storage.as_ref() {
            Some(Storage::PersistentClaim(pc)) => Some(pc.delete_claim),
            Some(Storage::Jbod(j)) => Some(j.delete_claim),
            _ => None,
        };
        if let Some(dc) = pool_data_delete_claim
            && dc != tp.delete_claim
        {
            return Err(ReconcileError::TieredStorageInvalid(format!(
                "tiered storage persistence.deleteClaim={} but pool '{}' storage.deleteClaim={}; \
                 K8s StatefulSets have a single set-wide PVC retention policy — these must match",
                tp.delete_claim, pool_name, dc,
            )));
        }
    }

    let (pod_volumes, volume_claim_templates) = render_storage(
        pool.spec.storage.as_ref(),
        &pod_labels,
        &parent_name,
        oauth_jwks_trust_secret.as_deref(),
        oauth_introspection_mount.as_ref(),
        gssapi_keytab_mount.as_ref(),
        krb5_conf_mount
            .as_ref()
            .map(|(s, k)| (s.as_str(), k.as_str())),
        tier_storage_local,
        tier_storage_persistence,
        tiered_storage
            .filter(|t| matches!(t.kind, crate::crd::kafka::TieredStorageType::Gcs))
            .and_then(|t| t.gcs.as_ref())
            .and_then(|g| g.credentials.as_ref()),
    );
    let retention_policy =
        render_pvc_retention_policy(pool.spec.storage.as_ref(), tier_storage_persistence);

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
    if !volume_claim_templates.is_empty() {
        sts_spec["volumeClaimTemplates"] = serde_json::Value::Array(volume_claim_templates);
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
/// `StatefulSet`'s observed `volumeClaimTemplates`. `observed` is
/// `None` when no live `StatefulSet` exists yet (first reconcile — any
/// spec is acceptable) and `Some(templates)` (possibly empty, for an
/// `Ephemeral` pool) otherwise.
///
/// The observed storage *kind* is derived from the template count:
/// `0 → Ephemeral`, `1 → PersistentClaim`, `>= 2 → Jbod` (JBOD always
/// has >= 2 disks — see [`validate`]). Rejections (all map to
/// `Ready=False, reason=StorageImmutable`):
/// - storage-type switch (`Ephemeral` / `PersistentClaim` / `Jbod`).
/// - `class` change on any matched disk.
/// - `size` decrease on any matched disk.
/// - JBOD disk-set change (adding/removing disks, deferred).
///
/// `delete_claim` is *not* checked: it only affects the `StatefulSet`'s
/// retention-policy field (mutable), not the PVC templates this compares.
fn validate_storage_change(
    desired: Option<&Storage>,
    observed: Option<&[PersistentVolumeClaim]>,
) -> Result<(), PoolValidationError> {
    let Some(observed) = observed else {
        return Ok(());
    };

    let desired_kind = storage_kind(desired);
    let observed_kind = observed_storage_kind(observed);
    if desired_kind != observed_kind {
        return Err(PoolValidationError::StorageTypeChanged {
            from: observed_kind,
            to: desired_kind,
        });
    }

    match desired {
        Some(Storage::PersistentClaim(desired_pc)) => {
            // observed is exactly one `data` template (kind matched above).
            let (observed_size, observed_class) = size_class_from_pvc(&observed[0]);
            check_class_and_shrink(
                &desired_pc.size,
                desired_pc.class.as_deref(),
                &observed_size,
                observed_class.as_deref(),
            )
        }
        Some(Storage::Jbod(desired_jbod)) => validate_jbod_change(desired_jbod, observed),
        // Ephemeral / absent: nothing else to compare.
        None | Some(Storage::Ephemeral) => Ok(()),
    }
}

/// Derive the observed storage kind from the live `StatefulSet`'s
/// `volumeClaimTemplates` count. JBOD is required to have >= 2 disks
/// (see [`validate`]), so the count is an unambiguous discriminator.
fn observed_storage_kind(templates: &[PersistentVolumeClaim]) -> &'static str {
    match templates.len() {
        0 => "Ephemeral",
        1 => "PersistentClaim",
        _ => "Jbod",
    }
}

/// Extract `(size, class)` from a `volumeClaimTemplate`. A missing spec /
/// request yields an empty size (compares as 0 bytes).
fn size_class_from_pvc(pvc: &PersistentVolumeClaim) -> (String, Option<String>) {
    let Some(spec) = pvc.spec.as_ref() else {
        return (String::new(), None);
    };
    let size = spec
        .resources
        .as_ref()
        .and_then(|r| r.requests.as_ref())
        .and_then(|m| m.get("storage"))
        .map(|q| q.0.clone())
        .unwrap_or_default();
    (size, spec.storage_class_name.clone())
}

/// Reject a `class` change or `size` decrease on one matched disk.
fn check_class_and_shrink(
    desired_size: &str,
    desired_class: Option<&str>,
    observed_size: &str,
    observed_class: Option<&str>,
) -> Result<(), PoolValidationError> {
    if desired_class != observed_class {
        return Err(PoolValidationError::StorageClassChanged {
            from: observed_class.map(String::from),
            to: desired_class.map(String::from),
        });
    }
    let observed_bytes = common::parse_quantity(observed_size).unwrap_or(0);
    let desired_bytes = common::parse_quantity(desired_size).unwrap_or(0);
    if desired_bytes < observed_bytes {
        return Err(PoolValidationError::StorageShrinkNotAllowed {
            current: observed_size.to_string(),
            desired: desired_size.to_string(),
        });
    }
    Ok(())
}

/// JBOD-vs-JBOD monotonic check. Disks are matched by identity: the
/// `data` template ↔ the desired primary (lowest id), and each
/// `data-{N}` template ↔ desired disk id `N`. The non-primary id set
/// must be identical (adding/removing disks — and reassigning the
/// primary — is deferred), and each matched disk's `class`/`size` obey
/// [`check_class_and_shrink`].
fn validate_jbod_change(
    desired: &crate::crd::JbodSpec,
    observed: &[PersistentVolumeClaim],
) -> Result<(), PoolValidationError> {
    use std::collections::{BTreeMap, BTreeSet};

    let mut observed_primary: Option<(String, Option<String>)> = None;
    let mut observed_extra: BTreeMap<i32, (String, Option<String>)> = BTreeMap::new();
    for t in observed {
        let name = t.metadata.name.as_deref().unwrap_or_default();
        if name == "data" {
            observed_primary = Some(size_class_from_pvc(t));
        } else if let Some(n) = name
            .strip_prefix("data-")
            .and_then(|s| s.parse::<i32>().ok())
        {
            observed_extra.insert(n, size_class_from_pvc(t));
        }
    }

    let mut volumes = desired.volumes.clone();
    volumes.sort_by_key(|v| v.id);
    let Some((primary, extras)) = volumes.split_first() else {
        return Ok(()); // empty desired is caught by static validation
    };

    let desired_extra_ids: BTreeSet<i32> = extras.iter().map(|v| v.id).collect();
    let observed_extra_ids: BTreeSet<i32> = observed_extra.keys().copied().collect();
    if desired_extra_ids != observed_extra_ids {
        return Err(PoolValidationError::JbodVolumesImmutable);
    }

    if let Some((obs_size, obs_class)) = observed_primary.as_ref() {
        check_class_and_shrink(
            &primary.size,
            primary.class.as_deref(),
            obs_size,
            obs_class.as_deref(),
        )?;
    }
    for v in extras {
        if let Some((obs_size, obs_class)) = observed_extra.get(&v.id) {
            check_class_and_shrink(&v.size, v.class.as_deref(), obs_size, obs_class.as_deref())?;
        }
    }
    Ok(())
}

fn storage_kind(s: Option<&Storage>) -> &'static str {
    match s {
        None | Some(Storage::Ephemeral) => "Ephemeral",
        Some(Storage::PersistentClaim(_)) => "PersistentClaim",
        Some(Storage::Jbod(_)) => "Jbod",
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
            format!("spec.replicas={n} is unsupported (only 1 allowed)"),
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
        PoolValidationError::JbodNeedsTwoVolumes(n) => (
            "JbodNeedsTwoVolumes",
            format!(
                "spec.storage.volumes has {n} disk(s); Jbod needs >= 2 (use PersistentClaim for one)"
            ),
        ),
        PoolValidationError::JbodDuplicateVolumeId(id) => (
            "JbodDuplicateVolumeId",
            format!("spec.storage.volumes has a duplicate id {id}"),
        ),
        PoolValidationError::JbodVolumesImmutable => (
            "StorageImmutable",
            "spec.storage.volumes set changed: adding/removing JBOD disks is not yet supported"
                .to_string(),
        ),
    };
    condition("Ready", "False", reason, &message)
}

/// Wrap `common::patch_status` with the pool-specific status shape.
#[tracing::instrument(
    level = "info",
    skip_all,
    fields(pool = %name, condition = %cond.type_, status = %cond.status, reason = %cond.reason),
    err,
)]
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

/// Whether the parent `Kafka`'s version model has cleared this pool to
/// render (and therefore format) broker pods.
///
/// Version validation lives in the *Kafka* controller ([`kafka::reconcile`]
/// → [`crate::version::evaluate`]), which publishes the verdict as the
/// parent's `KafkaVersionValid` condition and finalizes the resolved value
/// in `status.metadataVersion`. The pool reconciler owns no version logic;
/// it only reads that already-fetched parent status (no extra API request,
/// mirroring [`common::plan_rollout`]'s status-from-the-watched-object
/// posture) and refuses to format pods until the model has cleared —
/// otherwise an invalid `spec.kafkaVersion` on a brand-new cluster would
/// bring the brokers up at an unvalidated version instead of surfacing the
/// error and waiting.
#[derive(Debug)]
enum VersionGate {
    /// The parent's version model is valid (or already finalized): render
    /// the `StatefulSet` as normal.
    Cleared,
    /// The parent's version model has not cleared: refrain from rendering
    /// and surface `cond` (a `Ready=False`) on the pool.
    Blocked(KafkaCondition),
}

/// Decide whether `parent`'s version model clears this pool to format pods.
///
/// Clears when EITHER the parent carries `KafkaVersionValid=True`, OR a
/// finalized `status.metadataVersion` is present. The finalized-version
/// fallback is deliberate: a value there means a prior reconcile already
/// validated the model and formatted the pods, so a *later* spec edit that
/// flips `KafkaVersionValid=False` must not tear a running cluster down —
/// the Kafka controller holds the previous finalized version and simply
/// declines to advance it (see `kafka.rs` status patch).
fn version_gate(parent: &Kafka) -> VersionGate {
    // Not cleared. Distinguish "the parent declared the version invalid"
    // from "the parent hasn't published a verdict yet" so admins can tell
    // a misconfiguration from a transient ordering gap.
    let cond = match parent_version_gate(parent) {
        common::ParentVersionGate::Cleared => return VersionGate::Cleared,
        common::ParentVersionGate::Invalid(c) => condition(
            "Ready",
            "False",
            "KafkaVersionInvalid",
            &format!(
                "refusing to format brokers: parent Kafka '{}' KafkaVersionValid={} ({}): {}",
                parent.name_any(),
                c.status,
                c.reason,
                c.message
            ),
        ),
        common::ParentVersionGate::Waiting => condition(
            "Ready",
            "False",
            "WaitingForVersionValidation",
            &format!(
                "waiting for parent Kafka '{}' to publish a KafkaVersionValid verdict before formatting brokers",
                parent.name_any()
            ),
        ),
    };
    VersionGate::Blocked(cond)
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

/// Reconcile entry point. Times the pass and records the reconcile
/// counter/histogram, then delegates to [`reconcile_inner`].
#[tracing::instrument(
    level = "info",
    skip_all,
    fields(
        kind = "KafkaNodePool",
        namespace = %pool.namespace().unwrap_or_else(|| "default".into()),
        name = %pool.name_any(),
        generation = ?pool.meta().generation,
    )
)]
pub async fn reconcile(
    pool: Arc<KafkaNodePool>,
    ctx: Arc<Context>,
) -> Result<Action, ReconcileError> {
    common::record_reconcile(
        &ctx,
        "KafkaNodePool",
        Box::pin(reconcile_inner(pool, ctx.clone())),
    )
    .await
}

async fn reconcile_inner(
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

    // Gate on the parent's version model. Version validation lives in
    //     the Kafka controller; until it has declared the version valid
    //     (KafkaVersionValid=True) or finalized a metadata version, refrain
    //     from formatting/creating broker pods. This surfaces an invalid
    //     spec.kafkaVersion as a clear CR condition and waits, rather than
    //     bringing a cluster up at an unvalidated version. The requeue +
    //     the Kafka controller's adopt-pools label patch re-trigger this
    //     reconcile once the parent publishes its verdict.
    if let VersionGate::Blocked(cond) = version_gate(&parent) {
        patch_status_for_pool(&pool_api, &name, cond).await?;
        return Ok(Action::requeue(Duration::from_secs(30)));
    }

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
    // `None` = no live StatefulSet (first reconcile). `Some(templates)`
    // (possibly empty, for an Ephemeral pool) = the live STS's PVC
    // templates, which `validate_storage_change` reads to derive the
    // observed storage kind + per-disk size/class.
    let observed_pvc_templates: Option<Vec<PersistentVolumeClaim>> =
        observed_sts.as_ref().map(|s| {
            s.spec
                .as_ref()
                .and_then(|spec| spec.volume_claim_templates.clone())
                .unwrap_or_default()
        });

    if let Err(e) = validate_storage_change(
        pool.spec.storage.as_ref(),
        observed_pvc_templates.as_deref(),
    ) {
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
    use std::collections::BTreeMap;

    use assert2::{assert, check};

    use super::*;
    use crate::crd::{
        KafkaNodePoolSpec, KafkaSpec, MetadataTemplate, PersistentClaimSpec, PodTemplate, Storage,
    };

    fn parent_fixture(name: &str) -> Kafka {
        let mut k = Kafka::new(
            name,
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
        assert!(sts.metadata.name.as_deref() == Some("demo-brokers"));
    }

    #[test]
    fn render_statefulset_service_name_is_shared_headless() {
        let parent = parent_fixture("demo");
        let pool = pool_fixture("brokers", "demo", 1);
        let sts = render_statefulset(&parent, &pool, DEFAULT_BROKER_IMAGE).unwrap();
        let spec = sts.spec.expect("sts spec");
        assert!(spec.service_name.as_deref() == Some("demo-broker-headless"));
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
            (
                pod_labels
                    .get("app.kubernetes.io/instance")
                    .map(String::as_str),
                pod_labels.get("crabka.io/pool").map(String::as_str),
            ),
            (Some("demo"), Some("brokers"))
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
        assert!(node_id_start.value.as_deref() == Some("42"));

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
    fn render_statefulset_broker_has_startup_probe_for_slow_rejoin() {
        // A controller-voter restarted uncleanly can take tens of seconds to
        // rejoin the KRaft quorum before it opens the data port. Without a
        // startupProbe the liveness probe SIGKILLs it mid-rejoin into a crash
        // loop (which also keeps it flapping so leader failover never fires).
        let parent = parent_fixture("demo");
        let pool = pool_fixture("brokers", "demo", 1);
        let sts = render_statefulset(&parent, &pool, DEFAULT_BROKER_IMAGE).unwrap();
        let pod = sts.spec.unwrap().template.spec.unwrap();
        let broker = pod
            .containers
            .iter()
            .find(|c| c.name == "broker")
            .expect("broker container");
        let startup = broker.startup_probe.as_ref().expect(
            "broker must have a startupProbe so a slow rejoin isn't crash-looped by liveness",
        );
        assert!(
            startup.tcp_socket.is_some(),
            "startupProbe should gate on the data port being open"
        );
        // Generous failure budget so a legitimately slow rejoin completes.
        assert!(
            startup.failure_threshold.unwrap_or(0) >= 12,
            "startupProbe needs a generous failureThreshold for a slow KRaft rejoin, got {:?}",
            startup.failure_threshold
        );
    }

    #[test]
    fn init_script_passes_release_version() {
        assert!(
            INIT_SCRIPT.contains("--release-version \"$CRABKA_METADATA_VERSION\""),
            "init script must pass the resolved metadata.version to crabka format"
        );
    }

    #[test]
    fn init_container_wires_metadata_version_env() {
        let c = render_init_container("img:tag", "sec", 0, "4.0");
        let env = c["env"].as_array().expect("env array");
        let mv = env
            .iter()
            .find(|e| e["name"] == "CRABKA_METADATA_VERSION")
            .expect("CRABKA_METADATA_VERSION env present");
        assert!(mv["value"] == "4.0");
    }

    #[test]
    fn statefulset_init_normalizes_metadata_version_to_short() {
        // kafka_version "3.7.1", no spec/status metadata_version -> "3.7" reaches the init env.
        let parent = parent_fixture("demo"); // spec.kafka_version = "0.1.1", no metadata_version
        // Use a parent with a real 3-part kafka_version to exercise the normalization path.
        let mut parent37 = parent.clone();
        parent37.spec.kafka_version = "3.7.1".into();
        let pool = pool_fixture("brokers", "demo", 1);
        let sts = render_statefulset(&parent37, &pool, DEFAULT_BROKER_IMAGE).unwrap();
        let pod = sts.spec.unwrap().template.spec.unwrap();
        let init = &pod.init_containers.expect("init containers")[0];
        let env = init.env.as_ref().expect("init env");
        let mv = env
            .iter()
            .find(|e| e.name == "CRABKA_METADATA_VERSION")
            .expect("CRABKA_METADATA_VERSION env present");
        assert!(
            mv.value.as_deref() == Some("3.7"),
            "init container must receive short major.minor form, not the 3-part kafka_version"
        );
    }

    #[test]
    fn statefulset_init_clamps_out_of_range_version_to_max() {
        // kafka_version "4.1.0" normalises to "4.1", which is NOT yet in the
        // broker's supported metadata.version table. Without clamping,
        // `crabka format --release-version 4.1` would exit non-zero and
        // crash-loop the init container. The clamp must silently fall back to
        // the broker's MAX short form ("4.0") so the pod can boot.
        let mut parent = parent_fixture("demo");
        parent.spec.kafka_version = "4.1.0".into();
        // No spec.metadata_version pin, no status.metadataVersion.
        let pool = pool_fixture("brokers", "demo", 1);
        let sts = render_statefulset(&parent, &pool, DEFAULT_BROKER_IMAGE).unwrap();
        let pod = sts.spec.unwrap().template.spec.unwrap();
        let init = &pod.init_containers.expect("init containers")[0];
        let env = init.env.as_ref().expect("init env");
        let mv = env
            .iter()
            .find(|e| e.name == "CRABKA_METADATA_VERSION")
            .expect("CRABKA_METADATA_VERSION env present");
        let max_short = crabka_metadata::metadata_version::from_feature_level(
            crabka_metadata::metadata_version::METADATA_VERSION_MAX,
        )
        .unwrap()
        .short();
        assert!(
            mv.value.as_deref() == Some(max_short),
            "out-of-range kafka_version must clamp to MAX short form ({max_short}), \
             not the unsupported \"4.1\""
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
    fn validate_rejects_single_role_cases() {
        for (name, role) in [
            ("controller only", NodeRole::Controller),
            ("broker only", NodeRole::Broker),
        ] {
            let mut pool = pool_fixture("brokers", "demo", 1);
            pool.spec.roles = vec![role];
            let err = validate(&pool).unwrap_err();
            assert!(
                matches!(err, PoolValidationError::RolesNotMixed(_)),
                "case {name}: expected RolesNotMixed, got {err:?}"
            );
        }
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
        assert_eq!(
            (
                pod_labels.get("team").map(String::as_str),
                pod_labels.get("app.kubernetes.io/name").map(String::as_str),
            ),
            (Some("platform"), Some(APP_LABEL))
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
        assert!(anno.get("crabka.io/test-anno").map(String::as_str) == Some("yes"));
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
        assert!(rendered == Some(affinity));
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
        assert!(tols == vec![tol]);
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
        assert!(rendered.get("disktype").map(String::as_str) == Some("ssd"));
    }

    #[test]
    fn render_statefulset_no_template_no_extra_fields() {
        let pool = pool_fixture("brokers", "demo", 1);
        let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
        let spec = sts.spec.unwrap().template.spec.unwrap();
        assert_eq!(
            (
                spec.affinity,
                spec.tolerations.unwrap_or_default(),
                spec.node_selector.unwrap_or_default(),
            ),
            (None, vec![], BTreeMap::new())
        );
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
        assert!(anno.get("crabka.io/config-hash").map(String::as_str) == Some("abc123"));
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
    fn render_statefulset_emptydir_storage_cases() {
        for (name, storage) in [
            ("storage omitted", None),
            ("explicit ephemeral storage", Some(Storage::Ephemeral)),
        ] {
            let mut pool = pool_fixture("brokers", "demo", 1);
            pool.spec.storage = storage;
            let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
            let spec = sts.spec.unwrap();
            let claims = spec.volume_claim_templates.unwrap_or_default();
            let volumes = spec.template.spec.unwrap().volumes.unwrap();
            let data_vol = volumes
                .iter()
                .find(|v| v.name == "data")
                .expect("data volume present");
            assert_eq!(
                (claims, data_vol.empty_dir.is_some()),
                (vec![], true),
                "case {name}"
            );
        }
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
        let data_pvc = &vct[0];
        let pvc_spec = data_pvc.spec.as_ref().unwrap();
        let req = pvc_spec
            .resources
            .as_ref()
            .unwrap()
            .requests
            .as_ref()
            .unwrap();
        assert_eq!(
            (
                vct.len(),
                data_pvc.metadata.name.as_deref(),
                pvc_spec.access_modes.as_deref(),
                req.get("storage").map(|q| q.0.as_str()),
                pvc_spec.storage_class_name.as_deref(),
            ),
            (
                1,
                Some("data"),
                Some(["ReadWriteOnce".to_string()].as_slice()),
                Some("10Gi"),
                Some("fast-ssd"),
            )
        );
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
            (
                labels.get("app.kubernetes.io/instance").map(String::as_str),
                labels.get("crabka.io/pool").map(String::as_str),
            ),
            (Some("demo"), Some("brokers"))
        );
    }

    #[test]
    fn render_statefulset_retention_policy_cases() {
        for (name, delete_claim, when_deleted) in [
            ("delete claim", true, "Delete"),
            ("retain claim", false, "Retain"),
        ] {
            let mut pool = pool_fixture("brokers", "demo", 1);
            pool.spec.storage = Some(Storage::PersistentClaim(PersistentClaimSpec {
                size: "1Gi".into(),
                class: None,
                delete_claim,
            }));
            let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
            let policy = sts
                .spec
                .unwrap()
                .persistent_volume_claim_retention_policy
                .unwrap();
            assert_eq!(
                policy,
                k8s_openapi::api::apps::v1::StatefulSetPersistentVolumeClaimRetentionPolicy {
                    when_deleted: Some(when_deleted.to_string()),
                    when_scaled: Some("Retain".to_string()),
                },
                "case {name}"
            );
        }
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
        let ephemeral = Storage::Ephemeral;
        let persistent = pc("10Gi", None);
        for (case, desired) in [
            ("none", None),
            ("ephemeral", Some(&ephemeral)),
            ("persistent-claim", Some(&persistent)),
        ] {
            assert!(
                validate_storage_change(desired, None).is_ok(),
                "case {case}"
            );
        }
    }

    #[test]
    fn validate_storage_change_rejects_type_switch() {
        let observed = pvc_template("10Gi", None);
        let err = validate_storage_change(
            Some(&Storage::Ephemeral),
            Some(std::slice::from_ref(&observed)),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            PoolValidationError::StorageTypeChanged { .. }
        ));
    }

    #[test]
    fn validate_storage_change_rejects_class_change() {
        let observed = pvc_template("10Gi", Some("class-a"));
        let err = validate_storage_change(
            Some(&pc("10Gi", Some("class-b"))),
            Some(std::slice::from_ref(&observed)),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            PoolValidationError::StorageClassChanged { .. }
        ));
    }

    #[test]
    fn validate_storage_change_rejects_shrink() {
        let observed = pvc_template("10Gi", None);
        let err = validate_storage_change(
            Some(&pc("5Gi", None)),
            Some(std::slice::from_ref(&observed)),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            PoolValidationError::StorageShrinkNotAllowed { .. }
        ));
    }

    #[test]
    fn validate_storage_change_allows_grow() {
        let observed = pvc_template("10Gi", None);
        assert!(
            validate_storage_change(
                Some(&pc("20Gi", None)),
                Some(std::slice::from_ref(&observed))
            )
            .is_ok()
        );
    }

    #[test]
    fn validate_storage_change_allows_delete_claim_flip() {
        let observed = pvc_template("10Gi", None);
        let mut desired = pc("10Gi", None);
        if let Storage::PersistentClaim(ref mut p) = desired {
            p.delete_claim = true;
        }
        assert!(
            validate_storage_change(Some(&desired), Some(std::slice::from_ref(&observed))).is_ok()
        );
    }

    #[test]
    fn validate_static_rejects_unparseable_size() {
        let mut pool = pool_fixture("brokers", "demo", 1);
        pool.spec.storage = Some(pc("banana", None));
        let err = validate(&pool).unwrap_err();
        assert!(matches!(err, PoolValidationError::StorageSizeInvalid(_, _)));
    }

    // --- JBOD -------------------------------------------------------------

    fn pvc_template_named(name: &str, size: &str, class: Option<&str>) -> PersistentVolumeClaim {
        let mut t = pvc_template(size, class);
        t.metadata.name = Some(name.into());
        t
    }

    fn jbod(volumes: &[(i32, &str, Option<&str>)], delete_claim: bool) -> Storage {
        Storage::Jbod(crate::crd::JbodSpec {
            volumes: volumes
                .iter()
                .map(|(id, size, class)| JbodVolume {
                    id: *id,
                    size: (*size).into(),
                    class: class.map(String::from),
                })
                .collect(),
            delete_claim,
        })
    }

    fn jbod_pool(volumes: &[(i32, &str, Option<&str>)], delete_claim: bool) -> KafkaNodePool {
        let mut pool = pool_fixture("brokers", "demo", 1);
        pool.spec.storage = Some(jbod(volumes, delete_claim));
        pool
    }

    #[test]
    fn validate_rejects_jbod_single_volume() {
        let pool = jbod_pool(&[(0, "1Gi", None)], false);
        let err = validate(&pool).unwrap_err();
        assert!(matches!(err, PoolValidationError::JbodNeedsTwoVolumes(1)));
    }

    #[test]
    fn validate_rejects_jbod_duplicate_ids() {
        let pool = jbod_pool(&[(0, "1Gi", None), (0, "2Gi", None)], false);
        let err = validate(&pool).unwrap_err();
        assert!(matches!(err, PoolValidationError::JbodDuplicateVolumeId(0)));
    }

    #[test]
    fn validate_rejects_jbod_bad_size() {
        let pool = jbod_pool(&[(0, "1Gi", None), (1, "banana", None)], false);
        let err = validate(&pool).unwrap_err();
        assert!(matches!(err, PoolValidationError::StorageSizeInvalid(_, _)));
    }

    #[test]
    fn validate_accepts_valid_jbod() {
        let pool = jbod_pool(&[(0, "1Gi", None), (1, "2Gi", Some("fast"))], true);
        assert!(validate(&pool).is_ok());
    }

    #[test]
    fn render_statefulset_jbod_renders_one_pvc_per_volume() {
        let pool = jbod_pool(&[(0, "10Gi", None), (1, "20Gi", Some("fast-ssd"))], false);
        let parent = parent_fixture("demo");
        let expected_labels = common_labels("demo", &parent.spec.kafka_version, Some("brokers"));
        let sts = render_statefulset(&parent, &pool, "img:1").unwrap();
        let vct = sts.spec.unwrap().volume_claim_templates.unwrap();
        let expected_pvc = |name, size, class| {
            let mut pvc = super::pvc_template(name, size, class, &expected_labels);
            pvc["apiVersion"] = serde_json::json!("v1");
            pvc["kind"] = serde_json::json!("PersistentVolumeClaim");
            pvc
        };
        assert_eq!(
            serde_json::to_value(vct).unwrap(),
            serde_json::Value::Array(vec![
                expected_pvc("data", "10Gi", None),
                expected_pvc("data-1", "20Gi", Some("fast-ssd")),
            ])
        );
    }

    #[test]
    fn render_statefulset_jbod_sorts_volumes_by_id() {
        // Disks listed out of order must render deterministically (sorted).
        let pool = jbod_pool(
            &[(2, "1Gi", None), (0, "1Gi", None), (1, "1Gi", None)],
            false,
        );
        let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
        let vct = sts.spec.unwrap().volume_claim_templates.unwrap();
        let names: Vec<&str> = vct
            .iter()
            .map(|t| t.metadata.name.as_deref().unwrap())
            .collect();
        assert!(names == vec!["data", "data-1", "data-2"]);
    }

    #[test]
    fn render_statefulset_jbod_sets_extra_log_dirs_env() {
        let pool = jbod_pool(
            &[(0, "1Gi", None), (1, "1Gi", None), (2, "1Gi", None)],
            false,
        );
        let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
        let env = sts.spec.unwrap().template.spec.unwrap().containers[0]
            .env
            .clone()
            .unwrap();
        let extra = env
            .iter()
            .find(|e| e.name == "CRABKA_EXTRA_LOG_DIRS")
            .expect("CRABKA_EXTRA_LOG_DIRS env present for JBOD");
        // Primary (`/var/lib/crabka/data`) excluded; extras sorted by id.
        assert!(extra.value.as_deref() == Some("/var/lib/crabka/data-1,/var/lib/crabka/data-2"));
    }

    #[test]
    fn render_statefulset_jbod_mounts_extra_volumes() {
        let pool = jbod_pool(&[(0, "1Gi", None), (1, "1Gi", None)], false);
        let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
        let mounts = sts.spec.unwrap().template.spec.unwrap().containers[0]
            .volume_mounts
            .clone()
            .unwrap();
        let by_name: Vec<(&str, &str)> = mounts
            .iter()
            .filter(|mount| mount.name == "data" || mount.name.starts_with("data-"))
            .map(|m| (m.name.as_str(), m.mount_path.as_str()))
            .collect();
        assert_eq!(
            by_name,
            vec![
                ("data", "/var/lib/crabka/data"),
                ("data-1", "/var/lib/crabka/data-1"),
            ]
        );
    }

    #[test]
    fn render_statefulset_jbod_no_extra_log_dirs_env_for_non_jbod() {
        // Regression: PersistentClaim / Ephemeral pools must NOT gain the
        // env (keeps their pod template byte-identical).
        let mut pool = pool_fixture("brokers", "demo", 1);
        pool.spec.storage = Some(pc("1Gi", None));
        let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
        let env = sts.spec.unwrap().template.spec.unwrap().containers[0]
            .env
            .clone()
            .unwrap();
        assert!(env.iter().all(|e| e.name != "CRABKA_EXTRA_LOG_DIRS"));
    }

    #[test]
    fn render_statefulset_jbod_retention_policy_delete_when_delete_claim_true() {
        let pool = jbod_pool(&[(0, "1Gi", None), (1, "1Gi", None)], true);
        let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
        let policy = sts
            .spec
            .unwrap()
            .persistent_volume_claim_retention_policy
            .unwrap();
        assert_eq!(
            (
                policy.when_deleted.as_deref(),
                policy.when_scaled.as_deref()
            ),
            (Some("Delete"), Some("Retain"))
        );
    }

    #[test]
    fn render_statefulset_jbod_pvc_labels_inherit_pod_labels() {
        let pool = jbod_pool(&[(0, "1Gi", None), (1, "1Gi", None)], false);
        let sts = render_statefulset(&parent_fixture("demo"), &pool, "img:1").unwrap();
        let vct = sts.spec.unwrap().volume_claim_templates.unwrap();
        for t in &vct {
            let labels = t.metadata.labels.clone().expect("PVC labels");
            assert!(
                labels.get("app.kubernetes.io/instance").map(String::as_str) == Some("demo"),
                "every JBOD PVC inherits the GC instance label"
            );
        }
    }

    /// Build observed JBOD templates (`data` + `data-{id}`) for the
    /// monotonic-change tests.
    fn jbod_observed(volumes: &[(&str, &str, Option<&str>)]) -> Vec<PersistentVolumeClaim> {
        volumes
            .iter()
            .map(|(name, size, class)| pvc_template_named(name, size, *class))
            .collect()
    }

    #[test]
    fn validate_storage_change_rejects_switch_into_jbod() {
        // observed = single PersistentClaim template; desired = JBOD.
        let observed = jbod_observed(&[("data", "10Gi", None)]);
        let desired = jbod(&[(0, "10Gi", None), (1, "10Gi", None)], false);
        let err = validate_storage_change(Some(&desired), Some(&observed)).unwrap_err();
        assert!(matches!(
            err,
            PoolValidationError::StorageTypeChanged { .. }
        ));
    }

    #[test]
    fn validate_storage_change_rejects_switch_out_of_jbod() {
        // observed = JBOD (2 templates); desired = PersistentClaim.
        let observed = jbod_observed(&[("data", "10Gi", None), ("data-1", "10Gi", None)]);
        let err = validate_storage_change(Some(&pc("10Gi", None)), Some(&observed)).unwrap_err();
        assert!(matches!(
            err,
            PoolValidationError::StorageTypeChanged { .. }
        ));
    }

    #[test]
    fn validate_storage_change_jbod_success_cases() {
        for (name, observed, desired) in [
            (
                "grow volumes",
                jbod_observed(&[("data", "10Gi", None), ("data-1", "10Gi", None)]),
                jbod(&[(0, "20Gi", None), (1, "30Gi", None)], false),
            ),
            (
                "unchanged volumes",
                jbod_observed(&[("data", "10Gi", None), ("data-1", "20Gi", Some("fast"))]),
                jbod(&[(0, "10Gi", None), (1, "20Gi", Some("fast"))], false),
            ),
        ] {
            let result = validate_storage_change(Some(&desired), Some(&observed));
            assert!(result.is_ok(), "case {name}: {result:?}");
        }
    }

    #[test]
    fn validate_storage_change_jbod_rejects_shrink() {
        let observed = jbod_observed(&[("data", "10Gi", None), ("data-1", "10Gi", None)]);
        let desired = jbod(&[(0, "10Gi", None), (1, "5Gi", None)], false);
        let err = validate_storage_change(Some(&desired), Some(&observed)).unwrap_err();
        assert!(matches!(
            err,
            PoolValidationError::StorageShrinkNotAllowed { .. }
        ));
    }

    #[test]
    fn validate_storage_change_jbod_rejects_class_change() {
        let observed = jbod_observed(&[("data", "10Gi", None), ("data-1", "10Gi", Some("a"))]);
        let desired = jbod(&[(0, "10Gi", None), (1, "10Gi", Some("b"))], false);
        let err = validate_storage_change(Some(&desired), Some(&observed)).unwrap_err();
        assert!(matches!(
            err,
            PoolValidationError::StorageClassChanged { .. }
        ));
    }

    #[test]
    fn validate_storage_change_jbod_rejects_adding_disk() {
        let observed = jbod_observed(&[("data", "10Gi", None), ("data-1", "10Gi", None)]);
        let desired = jbod(
            &[(0, "10Gi", None), (1, "10Gi", None), (2, "10Gi", None)],
            false,
        );
        let err = validate_storage_change(Some(&desired), Some(&observed)).unwrap_err();
        assert!(matches!(err, PoolValidationError::JbodVolumesImmutable));
    }

    #[test]
    fn validate_storage_change_jbod_rejects_removing_disk() {
        let observed = jbod_observed(&[
            ("data", "10Gi", None),
            ("data-1", "10Gi", None),
            ("data-2", "10Gi", None),
        ]);
        let desired = jbod(&[(0, "10Gi", None), (1, "10Gi", None)], false);
        let err = validate_storage_change(Some(&desired), Some(&observed)).unwrap_err();
        assert!(matches!(err, PoolValidationError::JbodVolumesImmutable));
    }

    #[test]
    fn build_main_script_cases() {
        let metrics_script = concat!(
            "set -eu\n",
            "NODE_ID=\"$(cat /var/lib/crabka/data/.node-id)\"\n",
            "cp /etc/crabka/config/broker-${NODE_ID}.toml /run/crabka/broker.toml\n",
            "exec /usr/bin/crabka-broker \\\n",
            "  --config-file=/run/crabka/broker.toml \\\n",
            "  --broker-id=\"${NODE_ID}\" \\\n",
            "  --metrics-listen-addr=0.0.0.0:9404\n",
        );
        for (name, enabled, expected) in [
            ("metrics disabled", false, MAIN_SCRIPT),
            ("metrics enabled", true, metrics_script),
        ] {
            assert_eq!(build_main_script(enabled), expected, "case {name}");
        }
    }

    #[test]
    fn render_statefulset_mounts_cluster_ca_and_broker_tls_secrets() {
        let parent = parent_fixture("mycluster");
        let pool = pool_fixture("brokers", "mycluster", 1);
        let ss = render_statefulset(&parent, &pool, DEFAULT_BROKER_IMAGE).expect("render");
        let pod_spec = ss.spec.unwrap().template.spec.unwrap();
        let mounts: Vec<&str> = pod_spec.containers[0]
            .volume_mounts
            .as_ref()
            .unwrap()
            .iter()
            .map(|m| m.mount_path.as_str())
            .collect();
        for path in [
            "/etc/crabka/cluster-ca",
            "/etc/crabka/broker-tls",
            "/etc/crabka/clients-ca",
        ] {
            assert!(mounts.contains(&path), "missing {path}; got {mounts:?}");
        }
    }

    #[test]
    fn render_statefulset_mounts_gssapi_keytab() {
        let mut parent = parent_fixture("kerb");
        parent.spec.listeners = vec![crate::crd::Listener {
            name: "gss".into(),
            port: 9092,
            type_: crate::crd::ListenerType::Internal,
            tls: true,
            authentication: Some(crate::crd::ListenerAuthentication::Gssapi(
                crate::crd::ListenerAuthenticationGssapi {
                    keytab_secret_ref: crate::crd::KeytabSecretRef {
                        secret_name: "broker-keytab".into(),
                        key: "krb5.keytab".into(),
                    },
                    service_name: None,
                    principal_to_local_rules: vec![],
                    realm: None,
                    kdc: None,
                },
            )),
            configuration: None,
            network_policy_peers: None,
        }];
        let pool = pool_fixture("brokers", "kerb", 1);
        let ss = render_statefulset(&parent, &pool, DEFAULT_BROKER_IMAGE).expect("render");
        let pod_spec = ss.spec.unwrap().template.spec.unwrap();

        let mount = pod_spec.containers[0]
            .volume_mounts
            .as_ref()
            .unwrap()
            .iter()
            .find(|mount| mount.name == "gssapi-keytab")
            .expect("gssapi-keytab mount present");
        let volumes = pod_spec.volumes.unwrap_or_default();
        let volume = volumes
            .iter()
            .find(|v| v.name == "gssapi-keytab")
            .expect("gssapi-keytab volume present");
        assert_eq!(
            (mount, volume),
            (
                &k8s_openapi::api::core::v1::VolumeMount {
                    mount_path: "/etc/crabka/gssapi-keytab".into(),
                    name: "gssapi-keytab".into(),
                    read_only: Some(true),
                    ..Default::default()
                },
                &k8s_openapi::api::core::v1::Volume {
                    name: "gssapi-keytab".into(),
                    secret: Some(k8s_openapi::api::core::v1::SecretVolumeSource {
                        default_mode: Some(0o400),
                        items: Some(vec![k8s_openapi::api::core::v1::KeyToPath {
                            key: "krb5.keytab".into(),
                            mode: None,
                            path: "keytab".into(),
                        }]),
                        secret_name: Some("broker-keytab".into()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
        );
    }

    #[test]
    fn render_statefulset_mounts_krb5_conf_and_sets_env() {
        let mut parent = parent_fixture("kerb");
        parent.spec.krb5_conf_secret_ref = Some(crate::crd::Krb5ConfSecretRef {
            secret_name: "krb5-conf".into(),
            key: "config".into(),
        });
        let pool = pool_fixture("brokers", "kerb", 1);
        let ss = render_statefulset(&parent, &pool, DEFAULT_BROKER_IMAGE).expect("render");
        let pod_spec = ss.spec.unwrap().template.spec.unwrap();

        let mount = pod_spec.containers[0]
            .volume_mounts
            .as_ref()
            .unwrap()
            .iter()
            .find(|mount| mount.name == "krb5-conf")
            .expect("krb5-conf mount present");

        // KRB5_CONFIG env points at the projected krb5.conf file.
        let env = pod_spec.containers[0].env.as_ref().expect("env present");
        let krb5_config = env
            .iter()
            .find(|e| e.name == "KRB5_CONFIG")
            .expect("KRB5_CONFIG env present");
        let volumes = pod_spec.volumes.unwrap_or_default();
        let volume = volumes
            .iter()
            .find(|v| v.name == "krb5-conf")
            .expect("krb5-conf volume present");
        assert_eq!(
            (mount, krb5_config, volume),
            (
                &k8s_openapi::api::core::v1::VolumeMount {
                    mount_path: "/etc/crabka/krb5".into(),
                    name: "krb5-conf".into(),
                    read_only: Some(true),
                    ..Default::default()
                },
                &k8s_openapi::api::core::v1::EnvVar {
                    name: "KRB5_CONFIG".into(),
                    value: Some("/etc/crabka/krb5/krb5.conf".into()),
                    value_from: None,
                },
                &k8s_openapi::api::core::v1::Volume {
                    name: "krb5-conf".into(),
                    secret: Some(k8s_openapi::api::core::v1::SecretVolumeSource {
                        default_mode: Some(0o400),
                        items: Some(vec![k8s_openapi::api::core::v1::KeyToPath {
                            key: "config".into(),
                            mode: None,
                            path: "krb5.conf".into(),
                        }]),
                        secret_name: Some("krb5-conf".into()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
        );
    }

    #[test]
    fn render_statefulset_volume_secret_names_match_cluster() {
        let parent = parent_fixture("mycluster");
        let pool = pool_fixture("brokers", "mycluster", 1);
        let cluster = parent.metadata.name.clone().unwrap();
        let ss = render_statefulset(&parent, &pool, DEFAULT_BROKER_IMAGE).expect("render");
        let volumes = ss
            .spec
            .unwrap()
            .template
            .spec
            .unwrap()
            .volumes
            .unwrap_or_default();
        let names: Vec<String> = volumes
            .iter()
            .filter_map(|v| v.secret.as_ref().and_then(|s| s.secret_name.clone()))
            .collect();
        for suffix in ["cluster-ca-cert", "kafka-brokers", "clients-ca-cert"] {
            let want = format!("{cluster}-{suffix}");
            assert!(names.contains(&want), "missing {want}; got {names:?}");
        }
    }

    #[test]
    fn render_statefulset_metrics_port_cases() {
        use crate::crd::{MetricsConfig, PodMonitorSpec};

        let broker_port = k8s_openapi::api::core::v1::ContainerPort {
            container_port: BROKER_PORT,
            name: Some("kafka-internal".into()),
            protocol: Some("TCP".into()),
            ..Default::default()
        };
        let metrics_port = k8s_openapi::api::core::v1::ContainerPort {
            container_port: METRICS_PORT,
            name: Some("metrics".into()),
            protocol: Some("TCP".into()),
            ..Default::default()
        };

        for (name, config, expected) in [
            ("metrics disabled", None, vec![broker_port.clone()]),
            (
                "metrics enabled",
                Some(MetricsConfig {
                    pod_monitor: Some(PodMonitorSpec::default()),
                    ..Default::default()
                }),
                vec![broker_port.clone(), metrics_port],
            ),
        ] {
            let mut parent = parent_fixture("demo");
            parent.spec.metrics_config = config;
            let pool = pool_fixture("brokers", "demo", 1);
            let sts = render_statefulset(&parent, &pool, "img:latest").unwrap();
            let actual = sts.spec.unwrap().template.spec.unwrap().containers[0]
                .ports
                .clone()
                .unwrap();
            assert_eq!(actual, expected, "case {name}");
        }
    }

    #[test]
    fn render_statefulset_logging_cases() {
        use crate::crd::Logging;

        let expected_rust_log = k8s_openapi::api::core::v1::EnvVar {
            name: "RUST_LOG".into(),
            value: None,
            value_from: Some(k8s_openapi::api::core::v1::EnvVarSource {
                config_map_key_ref: Some(k8s_openapi::api::core::v1::ConfigMapKeySelector {
                    key: "rust.log".into(),
                    name: "demo-broker-config".into(),
                    optional: Some(true),
                }),
                ..Default::default()
            }),
        };

        for (name, logging, expected) in [
            ("logging disabled", None, None),
            (
                "logging enabled",
                Some(Logging::default()),
                Some(expected_rust_log),
            ),
        ] {
            let mut parent = parent_fixture("demo");
            parent.spec.logging = logging;
            let pool = pool_fixture("brokers", "demo", 1);
            let sts = render_statefulset(&parent, &pool, "img:latest").unwrap();
            let env = sts.spec.unwrap().template.spec.unwrap().containers[0]
                .env
                .clone()
                .unwrap();
            let actual = env.into_iter().find(|entry| entry.name == "RUST_LOG");
            assert_eq!(actual, expected, "case {name}");
        }
    }

    #[test]
    fn render_statefulset_dt_master_key_env_cases() {
        use crate::crd::kafka::{DelegationTokenConfig, SecretKeyRef};

        let expected_env = |key: &str| {
            Some(k8s_openapi::api::core::v1::EnvVar {
                name: "CRABKA_DELEGATION_TOKEN_SECRET_KEY".into(),
                value: None,
                value_from: Some(k8s_openapi::api::core::v1::EnvVarSource {
                    secret_key_ref: Some(k8s_openapi::api::core::v1::SecretKeySelector {
                        key: key.into(),
                        name: "dt-master".into(),
                        optional: None,
                    }),
                    ..Default::default()
                }),
            })
        };
        for (name, configured_key, expected) in [
            ("delegation token unset", None, None),
            ("default key", Some(None), expected_env("secret-key")),
            ("explicit key", Some(Some("hmac")), expected_env("hmac")),
        ] {
            let mut parent = parent_fixture("demo");
            parent.spec.delegation_token = configured_key.map(|key| DelegationTokenConfig {
                secret_key_ref: SecretKeyRef {
                    name: "dt-master".into(),
                    key: key.map(str::to_string),
                },
            });
            let pool = pool_fixture("brokers", "demo", 1);
            let sts = render_statefulset(&parent, &pool, "img:latest").unwrap();
            let env = sts.spec.unwrap().template.spec.unwrap().containers[0]
                .env
                .clone()
                .unwrap();
            let actual = env
                .into_iter()
                .find(|entry| entry.name == "CRABKA_DELEGATION_TOKEN_SECRET_KEY");
            assert_eq!(actual, expected, "case {name}");
        }
    }

    /// Build a Kafka CR with one OAuth listener whose
    /// `tls_trusted_certificates` contains one entry — exercises the
    /// `Some(...)` branch of [`oauth_jwks_trust_secret_name`] from
    /// inside the pool reconcile's render path.
    fn parent_with_oauth_trust(name: &str) -> Kafka {
        use crate::crd::{
            Listener, ListenerAuthentication, ListenerAuthenticationOAuth, ListenerType,
            TlsTrustedCertificate,
        };
        let mut k = parent_fixture(name);
        k.spec.listeners = vec![Listener {
            name: "oauth".into(),
            port: 9094,
            type_: ListenerType::Internal,
            tls: true,
            authentication: Some(ListenerAuthentication::OAuth(ListenerAuthenticationOAuth {
                valid_issuer_uri: "https://iss.example/".into(),
                jwks_endpoint_uri: Some("https://iss.example/jwks".into()),
                valid_audience: None,
                user_name_claim: None,
                custom_claim_check: None,
                jwks_refresh_seconds: None,
                max_clock_skew_seconds: None,
                enable_oauth_bearer: true,
                tls_trusted_certificates: vec![TlsTrustedCertificate {
                    secret_name: "my-idp-ca".into(),
                    certificate: "ca.crt".into(),
                }],
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
            })),
            configuration: None,
            network_policy_peers: None,
        }];
        k
    }

    #[test]
    fn render_statefulset_mounts_oauth_jwks_trust_secret_when_some() {
        let parent = parent_with_oauth_trust("demo");
        let pool = pool_fixture("brokers", "demo", 1);
        let ss = render_statefulset(&parent, &pool, DEFAULT_BROKER_IMAGE).expect("render");
        let pod_spec = ss.spec.unwrap().template.spec.unwrap();

        // VolumeMount on the broker container points at the canonical
        // path the broker reads (`/etc/crabka/oauth-jwks-trust`).
        let mount = pod_spec.containers[0]
            .volume_mounts
            .as_ref()
            .unwrap()
            .iter()
            .find(|m| m.name == "oauth-jwks-trust")
            .expect("oauth-jwks-trust mount present");

        // Pod volume sources the managed `{kafka}-oauth-jwks-trust`
        // Secret with the same 0o400 mode as the other CA volumes.
        let volume = pod_spec
            .volumes
            .as_ref()
            .unwrap()
            .iter()
            .find(|v| v.name == "oauth-jwks-trust")
            .expect("oauth-jwks-trust volume present");
        assert_eq!(
            (mount, volume),
            (
                &k8s_openapi::api::core::v1::VolumeMount {
                    mount_path: "/etc/crabka/oauth-jwks-trust".into(),
                    name: "oauth-jwks-trust".into(),
                    read_only: Some(true),
                    ..Default::default()
                },
                &k8s_openapi::api::core::v1::Volume {
                    name: "oauth-jwks-trust".into(),
                    secret: Some(k8s_openapi::api::core::v1::SecretVolumeSource {
                        default_mode: Some(0o400),
                        secret_name: Some("demo-oauth-jwks-trust".into()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
        );
    }

    #[test]
    fn render_statefulset_oauth_secret_absence_cases() {
        use crate::crd::{
            Listener, ListenerAuthentication, ListenerAuthenticationOAuth, ListenerType,
        };
        let no_listener = parent_fixture("demo");
        let mut empty_certificates = parent_fixture("demo");
        empty_certificates.spec.listeners = vec![Listener {
            name: "oauth".into(),
            port: 9094,
            type_: ListenerType::Internal,
            tls: true,
            authentication: Some(ListenerAuthentication::OAuth(ListenerAuthenticationOAuth {
                valid_issuer_uri: "https://iss.example/".into(),
                jwks_endpoint_uri: Some("https://iss.example/jwks".into()),
                valid_audience: None,
                user_name_claim: None,
                custom_claim_check: None,
                jwks_refresh_seconds: None,
                max_clock_skew_seconds: None,
                enable_oauth_bearer: true,
                tls_trusted_certificates: vec![],
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
            })),
            configuration: None,
            network_policy_peers: None,
        }];

        for (name, parent, resource_name) in [
            ("no OAuth listener", no_listener, "oauth-jwks-trust"),
            (
                "OAuth listener with empty certificates",
                empty_certificates,
                "oauth-jwks-trust",
            ),
            (
                "JWT-mode OAuth has no introspection secret",
                parent_with_oauth_trust("demo"),
                "oauth-introspection-secret",
            ),
        ] {
            let pool = pool_fixture("brokers", "demo", 1);
            let ss = render_statefulset(&parent, &pool, DEFAULT_BROKER_IMAGE).expect("render");
            let pod_spec = ss.spec.unwrap().template.spec.unwrap();
            let mount_present = pod_spec.containers[0]
                .volume_mounts
                .as_ref()
                .unwrap()
                .iter()
                .any(|mount| mount.name == resource_name);
            let volume_present = pod_spec
                .volumes
                .as_ref()
                .unwrap()
                .iter()
                .any(|volume| volume.name == resource_name);
            assert_eq!(
                (mount_present, volume_present),
                (false, false),
                "case {name}"
            );
        }
    }

    /// When the parent Kafka CR has an OAuth listener
    /// configured for introspection mode (`accessTokenIsJwt: false` +
    /// `clientSecret`), the rendered `StatefulSet` must:
    /// - expose the user's source Secret as a pod volume with a
    ///   projected `items` mapping that pins the user's key to the
    ///   fixed in-pod filename `client-secret`;
    /// - mount that volume on the broker container at the canonical
    ///   path `/etc/crabka/oauth-introspection` (matching the broker TOML
    ///   render).
    #[test]
    fn render_statefulset_mounts_oauth_introspection_secret_when_introspection_mode() {
        use crate::crd::{
            Listener, ListenerAuthentication, ListenerAuthenticationOAuth, ListenerType,
            OauthClientSecretRef,
        };
        let mut parent = parent_fixture("demo");
        parent.spec.listeners = vec![Listener {
            name: "oauth".into(),
            port: 9094,
            type_: ListenerType::Internal,
            tls: true,
            authentication: Some(ListenerAuthentication::OAuth(ListenerAuthenticationOAuth {
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
                client_id: Some("kafka-broker".into()),
                client_secret: Some(OauthClientSecretRef {
                    secret_name: "my-oauth-secret".into(),
                    key: "my-key".into(),
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
            })),
            configuration: None,
            network_policy_peers: None,
        }];
        let pool = pool_fixture("brokers", "demo", 1);
        let ss = render_statefulset(&parent, &pool, DEFAULT_BROKER_IMAGE).expect("render");
        let pod_spec = ss.spec.unwrap().template.spec.unwrap();

        // VolumeMount on the broker container points at the canonical
        // path the broker TOML render uses (`/etc/crabka/oauth-introspection`).
        let mount = pod_spec.containers[0]
            .volume_mounts
            .as_ref()
            .unwrap()
            .iter()
            .find(|m| m.name == "oauth-introspection-secret")
            .expect("oauth-introspection-secret mount present");

        // Pod volume sources the user-owned Secret directly with a
        // projected items mapping (user's key -> fixed path
        // `client-secret`) and the same 0o400 mode as the other
        // Secret volumes.
        let volume = pod_spec
            .volumes
            .as_ref()
            .unwrap()
            .iter()
            .find(|v| v.name == "oauth-introspection-secret")
            .expect("oauth-introspection-secret volume present");
        assert_eq!(
            (mount, volume),
            (
                &k8s_openapi::api::core::v1::VolumeMount {
                    mount_path: "/etc/crabka/oauth-introspection".into(),
                    name: "oauth-introspection-secret".into(),
                    read_only: Some(true),
                    ..Default::default()
                },
                &k8s_openapi::api::core::v1::Volume {
                    name: "oauth-introspection-secret".into(),
                    secret: Some(k8s_openapi::api::core::v1::SecretVolumeSource {
                        default_mode: Some(0o400),
                        items: Some(vec![k8s_openapi::api::core::v1::KeyToPath {
                            key: "my-key".into(),
                            mode: None,
                            path: "client-secret".into(),
                        }]),
                        secret_name: Some("my-oauth-secret".into()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
        );
    }

    // ── tieredStorage volume + mount tests ───────────────────────────

    fn parent_with_tiered_storage(name: &str) -> Kafka {
        let mut k = parent_fixture(name);
        k.spec.tiered_storage = Some(crate::crd::kafka::TieredStorage {
            kind: crate::crd::kafka::TieredStorageType::Local,
            s3: None,
            gcs: None,
            metadata_manager: None,
            persistence: None,
        });
        k
    }

    fn parent_with_s3_tiered_storage(name: &str, with_creds: bool) -> Kafka {
        let mut k = parent_fixture(name);
        let credentials = with_creds.then(|| crate::crd::kafka::S3Credentials {
            access_key_id: crate::crd::kafka::SecretKeyRef {
                name: "crabka-s3-creds".into(),
                key: Some("access-key-id".into()),
            },
            secret_access_key: crate::crd::kafka::SecretKeyRef {
                name: "crabka-s3-creds".into(),
                key: Some("secret-access-key".into()),
            },
        });
        k.spec.tiered_storage = Some(crate::crd::kafka::TieredStorage {
            kind: crate::crd::kafka::TieredStorageType::S3,
            s3: Some(crate::crd::kafka::S3StorageSpec {
                bucket: "crabka-tier".into(),
                region: "us-east-1".into(),
                credentials,
                ..Default::default()
            }),
            gcs: None,
            metadata_manager: None,
            persistence: None,
        });
        k
    }

    #[test]
    fn pod_template_mounts_tier_storage_emptydir_when_tiered_set() {
        let parent = parent_with_tiered_storage("demo");
        let pool = pool_fixture("brokers", "demo", 1);
        let sts = render_statefulset(&parent, &pool, DEFAULT_BROKER_IMAGE).unwrap();
        let pod_spec = sts.spec.as_ref().unwrap().template.spec.as_ref().unwrap();
        // Volume present with the tier-storage emptyDir shape.
        let volumes = pod_spec.volumes.as_ref().expect("pod volumes");
        let tier = volumes
            .iter()
            .find(|v| v.name == "tier-storage")
            .expect("tier-storage volume present");
        assert!(
            tier.empty_dir.is_some(),
            "tier-storage must be an emptyDir, got: {tier:?}"
        );

        // Broker container has the matching mount at the canonical path.
        let broker = pod_spec
            .containers
            .iter()
            .find(|c| c.name == "broker")
            .expect("broker container");
        let mount = broker
            .volume_mounts
            .as_ref()
            .expect("broker volumeMounts")
            .iter()
            .find(|m| m.name == "tier-storage")
            .expect("tier-storage mount present");
        assert_eq!(
            (mount.mount_path.as_str(), mount.read_only.unwrap_or(false)),
            (crate::controller::listeners::TIER_STORAGE_PATH, false)
        );
    }

    // ── S3 tiered storage env + volume gating ────────────────────────

    /// S3 backend with credentials must inject AWS env vars from the
    /// referenced Secret via `valueFrom.secretKeyRef`. The literal env
    /// value must be empty so the secret never lands in the pod spec
    /// JSON (same guarantee as the delegation-token wiring).
    #[test]
    fn pod_template_injects_aws_credentials_env_from_secret_when_s3() {
        let parent = parent_with_s3_tiered_storage("demo", true);
        let pool = pool_fixture("brokers", "demo", 1);
        let sts = render_statefulset(&parent, &pool, DEFAULT_BROKER_IMAGE).unwrap();
        let pod_spec = sts.spec.as_ref().unwrap().template.spec.as_ref().unwrap();
        let broker = pod_spec
            .containers
            .iter()
            .find(|c| c.name == "broker")
            .expect("broker container");
        let env = broker.env.as_ref().expect("env present");

        let ak = env
            .iter()
            .find(|e| e.name == "AWS_ACCESS_KEY_ID")
            .expect("AWS_ACCESS_KEY_ID env present");
        let ak_ref = ak
            .value_from
            .as_ref()
            .and_then(|v| v.secret_key_ref.as_ref())
            .expect("secretKeyRef present");

        let sk = env
            .iter()
            .find(|e| e.name == "AWS_SECRET_ACCESS_KEY")
            .expect("AWS_SECRET_ACCESS_KEY env present");
        let sk_ref = sk
            .value_from
            .as_ref()
            .and_then(|v| v.secret_key_ref.as_ref())
            .expect("secretKeyRef present");
        assert_eq!(
            (ak.value.as_deref(), sk.value.as_deref(), ak_ref, sk_ref),
            (
                None,
                None,
                &k8s_openapi::api::core::v1::SecretKeySelector {
                    name: "crabka-s3-creds".to_string(),
                    key: "access-key-id".to_string(),
                    optional: None,
                },
                &k8s_openapi::api::core::v1::SecretKeySelector {
                    name: "crabka-s3-creds".to_string(),
                    key: "secret-access-key".to_string(),
                    optional: None,
                },
            )
        );
    }

    /// S3 backend without `credentials` must produce a byte-identical
    /// env list to the no-tier baseline modulo any other tier-storage
    /// signal — the broker pod inherits IRSA / instance-profile auth
    /// from the cluster, and the operator must not inject placeholder
    /// AWS env entries.
    #[test]
    fn pod_template_omits_aws_credentials_env_when_s3_credentials_absent() {
        let parent = parent_with_s3_tiered_storage("demo", false);
        let pool = pool_fixture("brokers", "demo", 1);
        let sts = render_statefulset(&parent, &pool, DEFAULT_BROKER_IMAGE).unwrap();
        let pod_spec = sts.spec.as_ref().unwrap().template.spec.as_ref().unwrap();
        let broker = pod_spec
            .containers
            .iter()
            .find(|c| c.name == "broker")
            .expect("broker container");
        let env = broker.env.as_ref().expect("env present");
        assert!(
            env.iter()
                .all(|e| e.name != "AWS_ACCESS_KEY_ID" && e.name != "AWS_SECRET_ACCESS_KEY"),
            "credentialless S3 must not inject AWS env, got: {env:?}",
        );
    }

    // ── GCS tiered storage volume + mount gating ─────────────────────

    fn parent_with_gcs_tiered_storage(name: &str, with_creds: bool) -> Kafka {
        let mut k = parent_fixture(name);
        let credentials = with_creds.then(|| crate::crd::kafka::GcsCredentials {
            service_account_key: crate::crd::kafka::SecretKeyRef {
                name: "crabka-gcs-creds".into(),
                key: Some("key.json".into()),
            },
        });
        k.spec.tiered_storage = Some(crate::crd::kafka::TieredStorage {
            kind: crate::crd::kafka::TieredStorageType::Gcs,
            s3: None,
            gcs: Some(crate::crd::kafka::GcsStorageSpec {
                bucket: "crabka-tier".into(),
                credentials,
                ..Default::default()
            }),
            metadata_manager: None,
            persistence: None,
        });
        k
    }

    /// GCS with an explicit service-account key Secret must mount that
    /// Secret read-only as a FILE at `GCS_CREDENTIALS_DIR`, projecting the
    /// referenced key to `key.json`. No AWS-style env vars are added.
    #[test]
    fn pod_template_mounts_gcs_credentials_file_when_creds_set() {
        let parent = parent_with_gcs_tiered_storage("demo", true);
        let pool = pool_fixture("brokers", "demo", 1);
        let sts = render_statefulset(&parent, &pool, DEFAULT_BROKER_IMAGE).unwrap();
        let pod_spec = sts.spec.as_ref().unwrap().template.spec.as_ref().unwrap();

        // Pod volume present, sourced from the Secret with a key.json projection.
        let vol = pod_spec
            .volumes
            .as_ref()
            .expect("pod volumes")
            .iter()
            .find(|v| v.name == "gcs-credentials")
            .expect("gcs-credentials volume present");
        let secret = vol
            .secret
            .as_ref()
            .expect("gcs-credentials is a Secret volume");
        let items = secret.items.as_ref().expect("projected items");
        assert_eq!(
            (secret.secret_name.as_deref(), items.as_slice()),
            (
                Some("crabka-gcs-creds"),
                [k8s_openapi::api::core::v1::KeyToPath {
                    key: "key.json".into(),
                    mode: None,
                    path: "key.json".into(),
                }]
                .as_slice(),
            )
        );

        // Read-only mount at the canonical credentials dir.
        let broker = pod_spec
            .containers
            .iter()
            .find(|c| c.name == "broker")
            .expect("broker container");
        let mount = broker
            .volume_mounts
            .as_ref()
            .expect("broker volumeMounts")
            .iter()
            .find(|m| m.name == "gcs-credentials")
            .expect("gcs-credentials mount present");
        assert_eq!(
            (mount.mount_path.as_str(), mount.read_only),
            (
                crate::controller::listeners::GCS_CREDENTIALS_DIR,
                Some(true)
            ),
            "must use the canonical read-only mount"
        );

        // GCS must NOT inject AWS-style env vars, and must NOT mount the
        // Local tier-storage scratch volume.
        let env = broker.env.as_ref().expect("env present");
        assert!(
            env.iter()
                .all(|e| e.name != "AWS_ACCESS_KEY_ID" && e.name != "AWS_SECRET_ACCESS_KEY"),
            "GCS must not inject AWS env, got: {env:?}",
        );
        assert!(
            pod_spec
                .volumes
                .as_ref()
                .unwrap()
                .iter()
                .all(|v| v.name != "tier-storage"),
            "GCS must not allocate the Local tier-storage emptyDir",
        );
    }

    #[test]
    fn pod_template_omits_inapplicable_storage_resources() {
        for (name, parent, resource_name) in [
            (
                "S3 omits local tier storage",
                parent_with_s3_tiered_storage("demo", true),
                "tier-storage",
            ),
            (
                "cluster without tiering omits local tier storage",
                parent_fixture("demo"),
                "tier-storage",
            ),
            (
                "keyless GCS omits credentials",
                parent_with_gcs_tiered_storage("demo", false),
                "gcs-credentials",
            ),
        ] {
            let pool = pool_fixture("brokers", "demo", 1);
            let sts = render_statefulset(&parent, &pool, DEFAULT_BROKER_IMAGE).unwrap();
            let pod_spec = sts.spec.as_ref().unwrap().template.spec.as_ref().unwrap();
            let broker = pod_spec
                .containers
                .iter()
                .find(|container| container.name == "broker")
                .expect("broker container");
            assert_eq!(
                (
                    pod_spec
                        .volumes
                        .as_ref()
                        .unwrap()
                        .iter()
                        .any(|volume| volume.name == resource_name),
                    broker.volume_mounts.as_ref().is_some_and(|mounts| mounts
                        .iter()
                        .any(|mount| mount.name == resource_name)),
                ),
                (false, false),
                "case {name}"
            );
        }
    }

    // ── tier-storage PVC tests ───────────────────────────────────────

    fn parent_with_tier_storage_pvc(name: &str, size: &str, class: Option<&str>) -> Kafka {
        let mut k = parent_fixture(name);
        k.spec.tiered_storage = Some(crate::crd::kafka::TieredStorage {
            kind: crate::crd::kafka::TieredStorageType::Local,
            s3: None,
            gcs: None,
            metadata_manager: None,
            persistence: Some(crate::crd::kafka::TieredStoragePersistence {
                size: size.into(),
                class: class.map(str::to_string),
                delete_claim: false,
            }),
        });
        k
    }

    #[test]
    fn pod_template_emits_pvc_template_when_tier_persistence_set() {
        let parent = parent_with_tier_storage_pvc("demo", "50Gi", Some("fast-ssd"));
        let pool = pool_fixture("brokers", "demo", 1);
        let sts = render_statefulset(&parent, &pool, DEFAULT_BROKER_IMAGE).unwrap();
        // Pod-level volume entry must NOT be added — the StatefulSet
        // controller mounts the bound PVC automatically.
        let pod_spec = sts.spec.as_ref().unwrap().template.spec.as_ref().unwrap();
        assert!(
            pod_spec
                .volumes
                .as_ref()
                .unwrap()
                .iter()
                .all(|v| v.name != "tier-storage"),
            "explicit pod-volume `tier-storage` must not exist when PVC-backed"
        );
        // The volumeClaimTemplate is present with the configured size + class.
        let tmpls = sts
            .spec
            .as_ref()
            .unwrap()
            .volume_claim_templates
            .as_ref()
            .expect("volumeClaimTemplates");
        let tier = tmpls
            .iter()
            .find(|t| t.metadata.name.as_deref() == Some("tier-storage"))
            .expect("tier-storage volumeClaimTemplate");
        let spec = tier.spec.as_ref().expect("template spec");
        let req = spec
            .resources
            .as_ref()
            .and_then(|r| r.requests.as_ref())
            .expect("resources.requests");
        assert_eq!(
            (
                req.get("storage").map(|q| q.0.as_str()),
                spec.storage_class_name.as_deref(),
            ),
            (Some("50Gi"), Some("fast-ssd"))
        );
        // Mount inside the broker container still lands at the canonical path.
        let broker = pod_spec
            .containers
            .iter()
            .find(|c| c.name == "broker")
            .expect("broker container");
        let mount = broker
            .volume_mounts
            .as_ref()
            .unwrap()
            .iter()
            .find(|m| m.name == "tier-storage")
            .expect("tier-storage mount");
        assert!(mount.mount_path == crate::controller::listeners::TIER_STORAGE_PATH);
    }

    #[test]
    fn pod_template_pvc_template_omits_storage_class_when_unset() {
        let parent = parent_with_tier_storage_pvc("demo", "25Gi", None);
        let pool = pool_fixture("brokers", "demo", 1);
        let sts = render_statefulset(&parent, &pool, DEFAULT_BROKER_IMAGE).unwrap();
        let tier = sts
            .spec
            .as_ref()
            .unwrap()
            .volume_claim_templates
            .as_ref()
            .unwrap()
            .iter()
            .find(|t| t.metadata.name.as_deref() == Some("tier-storage"))
            .expect("tier-storage volumeClaimTemplate");
        assert!(
            tier.spec.as_ref().unwrap().storage_class_name.is_none(),
            "storageClassName must be omitted when class is None"
        );
    }

    #[test]
    fn tier_persistence_delete_claim_mismatch_fails_validation() {
        use crate::crd::{
            kafka::{TieredStorage, TieredStoragePersistence, TieredStorageType},
            kafka_node_pool::{PersistentClaimSpec, Storage},
        };

        let mut parent = parent_fixture("demo");
        parent.spec.tiered_storage = Some(TieredStorage {
            kind: TieredStorageType::Local,
            s3: None,
            gcs: None,
            metadata_manager: None,
            persistence: Some(TieredStoragePersistence {
                size: "20Gi".into(),
                class: None,
                delete_claim: true,
            }),
        });
        let mut pool = pool_fixture("brokers", "demo", 1);
        pool.spec.storage = Some(Storage::PersistentClaim(PersistentClaimSpec {
            size: "100Gi".into(),
            class: None,
            delete_claim: false,
        }));

        let err = render_statefulset(&parent, &pool, DEFAULT_BROKER_IMAGE)
            .expect_err("must reject deleteClaim mismatch");
        let msg = format!("{err:?}");
        assert!(msg.contains("TieredStorageInvalid"), "got: {msg}");
        assert!(msg.contains("deleteClaim"), "got: {msg}");
    }

    #[test]
    fn tier_persistence_delete_claim_matching_pool_passes() {
        use crate::crd::{
            kafka::{TieredStorage, TieredStoragePersistence, TieredStorageType},
            kafka_node_pool::{PersistentClaimSpec, Storage},
        };

        let mut parent = parent_fixture("demo");
        parent.spec.tiered_storage = Some(TieredStorage {
            kind: TieredStorageType::Local,
            s3: None,
            gcs: None,
            metadata_manager: None,
            persistence: Some(TieredStoragePersistence {
                size: "20Gi".into(),
                class: None,
                delete_claim: false,
            }),
        });
        let mut pool = pool_fixture("brokers", "demo", 1);
        pool.spec.storage = Some(Storage::PersistentClaim(PersistentClaimSpec {
            size: "100Gi".into(),
            class: None,
            delete_claim: false,
        }));

        // Should succeed; we only care that it doesn't error.
        let _sts = render_statefulset(&parent, &pool, DEFAULT_BROKER_IMAGE)
            .expect("matching deleteClaim must pass");
    }

    #[test]
    fn tier_persistence_with_ephemeral_pool_storage_passes() {
        use crate::crd::kafka::{TieredStorage, TieredStoragePersistence, TieredStorageType};

        let mut parent = parent_fixture("demo");
        parent.spec.tiered_storage = Some(TieredStorage {
            kind: TieredStorageType::Local,
            s3: None,
            gcs: None,
            metadata_manager: None,
            persistence: Some(TieredStoragePersistence {
                size: "20Gi".into(),
                class: None,
                delete_claim: true,
            }),
        });
        // pool.spec.storage stays None (ephemeral); no data PVC to collide with
        let pool = pool_fixture("brokers", "demo", 1);

        let _sts = render_statefulset(&parent, &pool, DEFAULT_BROKER_IMAGE)
            .expect("ephemeral pool + tier persistence must pass regardless of tier deleteClaim");
    }

    #[test]
    fn ephemeral_pool_with_tier_persistence_emits_retention_policy() {
        use crate::crd::kafka::{TieredStorage, TieredStoragePersistence, TieredStorageType};

        let mut parent = parent_fixture("demo");
        parent.spec.tiered_storage = Some(TieredStorage {
            kind: TieredStorageType::Local,
            s3: None,
            gcs: None,
            metadata_manager: None,
            persistence: Some(TieredStoragePersistence {
                size: "20Gi".into(),
                class: None,
                delete_claim: true,
            }),
        });
        // pool.spec.storage = None  → ephemeral; no data PVC
        let pool = pool_fixture("brokers", "demo", 1);
        let sts = render_statefulset(&parent, &pool, DEFAULT_BROKER_IMAGE).unwrap();
        let policy = sts
            .spec
            .as_ref()
            .unwrap()
            .persistent_volume_claim_retention_policy
            .as_ref()
            .expect("policy must exist when tier PVC is present");
        assert_eq!(
            (
                policy.when_deleted.as_deref(),
                policy.when_scaled.as_deref()
            ),
            (Some("Delete"), Some("Retain")),
            "delete_claim=true retention policy"
        );
    }

    #[test]
    fn ephemeral_pool_without_tier_persistence_emits_no_retention_policy() {
        let parent = parent_fixture("demo");
        let pool = pool_fixture("brokers", "demo", 1);
        let sts = render_statefulset(&parent, &pool, DEFAULT_BROKER_IMAGE).unwrap();
        assert!(
            sts.spec
                .as_ref()
                .unwrap()
                .persistent_volume_claim_retention_policy
                .is_none(),
            "no PVCs => no retention policy"
        );
    }

    // ── tracing env-var rendering ────────────────────────────────────

    fn parent_with_tracing(name: &str, otlp: crate::crd::kafka::OtlpTracing) -> Kafka {
        let mut k = parent_fixture(name);
        k.spec.tracing = Some(crate::crd::kafka::Tracing {
            kind: crate::crd::kafka::TracingType::Otlp,
            otlp: Some(otlp),
        });
        k
    }

    #[test]
    fn pod_template_otlp_env_cases() {
        use k8s_openapi::api::core::v1::EnvVar;

        use crate::crd::kafka::{OtlpProtocol, OtlpTracing};

        let env = |name: &str, value: &str| EnvVar {
            name: name.into(),
            value: Some(value.into()),
            value_from: None,
        };
        for (name, tracing, expected) in [
            ("tracing disabled", None, vec![]),
            (
                "required fields only",
                Some(OtlpTracing {
                    endpoint: "http://otel:4317".into(),
                    protocol: None,
                    sample_ratio: None,
                    service_name: None,
                    timeout_secs: None,
                }),
                vec![
                    env("CRABKA_OTLP_ENABLED", "true"),
                    env("CRABKA_OTLP_ENDPOINT", "http://otel:4317"),
                ],
            ),
            (
                "all fields",
                Some(OtlpTracing {
                    endpoint: "http://otel:4317".into(),
                    protocol: Some(OtlpProtocol::HttpProtobuf),
                    sample_ratio: Some(0.25),
                    service_name: Some("svc".into()),
                    timeout_secs: Some(7),
                }),
                vec![
                    env("CRABKA_OTLP_ENABLED", "true"),
                    env("CRABKA_OTLP_ENDPOINT", "http://otel:4317"),
                    env("CRABKA_OTLP_PROTOCOL", "http/protobuf"),
                    env("CRABKA_OTLP_SAMPLE_RATIO", "0.25"),
                    env("OTEL_SERVICE_NAME", "svc"),
                    env("CRABKA_OTLP_TIMEOUT_SECS", "7"),
                ],
            ),
        ] {
            let parent = tracing
                .map(|otlp| parent_with_tracing("demo", otlp))
                .unwrap_or_else(|| parent_fixture("demo"));
            let pool = pool_fixture("brokers", "demo", 1);
            let sts = render_statefulset(&parent, &pool, DEFAULT_BROKER_IMAGE).unwrap();
            let actual = sts
                .spec
                .unwrap()
                .template
                .spec
                .unwrap()
                .containers
                .into_iter()
                .find(|container| container.name == "broker")
                .expect("broker container")
                .env
                .unwrap()
                .into_iter()
                .filter(|entry| {
                    entry.name.starts_with("CRABKA_OTLP_") || entry.name == "OTEL_SERVICE_NAME"
                })
                .collect::<Vec<_>>();
            assert_eq!(actual, expected, "case {name}");
        }
    }

    // --- Version gate: the pool reconciler must not format/create broker
    // pods until the parent Kafka's version model has cleared. The decision is the pure
    // `version_gate`; the reconciler just acts on it. ---

    /// Attach a `KafkaVersionValid` condition + finalized metadata version
    /// to a parent fixture's status, mirroring what `kafka.rs` writes.
    fn parent_with_version_status(
        name: &str,
        version_valid: Option<bool>,
        finalized_metadata: Option<&str>,
    ) -> Kafka {
        let mut parent = parent_fixture(name);
        let mut conditions = Vec::new();
        if let Some(valid) = version_valid {
            let (status, reason, message) = if valid {
                ("True", "Valid", "kafkaVersion 3.7.0 metadata.version 3.7")
            } else {
                (
                    "False",
                    "InvalidVersion",
                    "spec.kafkaVersion \"99.9\" is not a valid version",
                )
            };
            conditions.push(condition("KafkaVersionValid", status, reason, message));
        }
        parent.status = Some(crate::crd::KafkaStatus {
            conditions,
            metadata_version: finalized_metadata.map(str::to_string),
            ..Default::default()
        });
        parent
    }

    #[test]
    fn version_gate_blocked_cases() {
        let missing_status = parent_fixture("demo");
        assert!(missing_status.status.is_none(), "fixture precondition");
        for (name, parent, expected, message_fragment) in [
            (
                "invalid finalized version",
                parent_with_version_status("demo", Some(false), None),
                ("Ready", "False", "KafkaVersionInvalid"),
                Some("KafkaVersionValid=False"),
            ),
            (
                "version status not published",
                missing_status,
                ("Ready", "False", "WaitingForVersionValidation"),
                None,
            ),
        ] {
            match version_gate(&parent) {
                VersionGate::Blocked(cond) => {
                    assert_eq!(
                        (
                            cond.type_.as_str(),
                            cond.status.as_str(),
                            cond.reason.as_str()
                        ),
                        expected,
                        "case {name}"
                    );
                    if let Some(fragment) = message_fragment {
                        check!(
                            cond.message.contains(fragment),
                            "case {name}: {}",
                            cond.message
                        );
                    }
                }
                VersionGate::Cleared => panic!("case {name}: version gate must block"),
            }
        }
    }

    #[test]
    fn version_gate_clears_when_kafkaversionvalid_true() {
        // Valid version: gate clears AND the StatefulSet renders as today.
        let parent = parent_with_version_status("demo", Some(true), Some("3.7"));
        assert!(
            matches!(version_gate(&parent), VersionGate::Cleared),
            "a valid parent version must clear the gate"
        );
        let pool = pool_fixture("brokers", "demo", 1);
        let sts = render_statefulset(&parent, &pool, DEFAULT_BROKER_IMAGE)
            .expect("pods are created as today when the version is valid");
        assert!(sts.metadata.name.as_deref() == Some("demo-brokers"));
    }

    #[test]
    fn version_gate_clears_when_metadata_version_finalized() {
        // An already-running cluster carries a finalized status.metadataVersion
        // even if a later spec edit flips KafkaVersionValid=False. We must not
        // tear the cluster down — the finalized version means a prior reconcile
        // formatted the pods at a known-good version.
        let parent = parent_with_version_status("demo", Some(false), Some("3.7"));
        assert!(
            matches!(version_gate(&parent), VersionGate::Cleared),
            "a finalized metadata version keeps a running cluster's pods"
        );
    }
}
