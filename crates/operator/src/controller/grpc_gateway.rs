//! `KafkaGrpcGateway` reconciler.
//!
//! Reconciles one `KafkaGrpcGateway` CR into the full runtime surface for a
//! `crabka-grpc-gateway` Deployment:
//!
//! - a **Deployment** (the gateway pods) with the downward-API env, the gateway
//!   CLI flags, and the five TLS / config volume mounts from the Design;
//! - a **Service** (`ClusterIP`, port 9500) selecting the gateway pods;
//! - an operator-issued **serving cert** Secret (`<gw>-serving`), signed by the
//!   parent's **cluster CA** (so peers / clients that trust the cluster CA
//!   accept the gateway's Connect / webhook / metrics TLS);
//! - a rendered **config Secret** (`<gw>-config`) holding `webhooks.toml` +
//!   `outbound.toml`, with HMAC secrets resolved from same-namespace Secrets at
//!   render time (never persisted in the CR);
//! - a child **`KafkaUser`** (`<gw>-broker`, `authentication: tls`) so the
//!   existing `KafkaUser` reconciler issues the gateway's **clients-CA-signed**
//!   broker-mTLS client cert + provisions its ACLs.
//!
//! The parent `Kafka` is discovered from the `crabka.io/cluster` label (the same
//! convention as `KafkaTopic` / `KafkaUser`). The reconcile is gated on the
//! parent's version model (mirrors `kafka_node_pool::version_gate`) so the
//! gateway never deploys against an unvalidated cluster.
//!
//! **Trust topology** (the load-bearing detail — see the P9 Design): the serving
//! cert is **cluster-CA-signed** (issued here), while the broker-client cert is
//! **clients-CA-signed** (delegated to the child `KafkaUser`). Signing the
//! broker-client cert with the cluster CA would be rejected by the broker's
//! `client_ca_path`; the child-KafkaUser path sidesteps that by construction.

use std::{collections::BTreeMap, sync::Arc};

use crabka_security::ca::{SubjectAltName, issue_broker_cert};
use crabka_units::{
    ByteSize, Ratio, Time,
    convert::{ByteSizeExt, RatioExt as _, TimeExt},
    fmt::Human as _,
    hours, millis, minutes, secs,
};
use futures::StreamExt as _;
use k8s_openapi::{
    ByteString,
    api::{
        apps::v1::Deployment,
        core::v1::{Secret, Service},
    },
    apimachinery::pkg::apis::meta::v1::ObjectMeta,
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
use time::OffsetDateTime;

use crate::{
    context::Context,
    controller::{
        cluster_ca::{cluster_ca_cert_name, cluster_ca_key_name, renew_if_expiring},
        common::{
            self, ReconcileError, apply_object, condition, millis_u64, owner_ref,
            parent_version_gate, patch_status, read_pem_key, secs_u64,
        },
    },
    crd::{
        Kafka, KafkaCondition, KafkaGrpcGateway, KafkaGrpcGatewayStatus,
        user::{
            AclOp, AclPermission, AclResource, AclResourceKind, AclRule, Authentication,
            Authorization, KafkaUser, KafkaUserSpec, SimpleAuthorization, TlsAuth,
        },
    },
};

/// Container / Service port the gateway binds for Connect-RPC + health +
/// webhooks + metrics. Matches the gateway binary's `--listen-addr` default
/// (`0.0.0.0:9500`).
const GATEWAY_PORT: i32 = 9500;

/// Default replica count when `spec.replicas` is absent.
const DEFAULT_REPLICAS: i32 = 1;

/// Default serving-cert validity (days) when `spec.tls.validityDays` is absent.
const DEFAULT_VALIDITY_DAYS: u32 = 365;

/// Built-in gateway image, used when neither `spec.image` nor the operator's
/// `--default-gateway-image` is set.
const DEFAULT_GATEWAY_IMAGE: &str = concat!(
    "ghcr.io/robot-head/crabka-grpc-gateway:",
    env!("CARGO_PKG_VERSION")
);

// In-pod mount paths (Design §"Deployment mount set"). The gateway CLI flags
// point at these.
const SERVING_DIR: &str = "/etc/crabka-gw/serving";
const BROKER_CLIENT_DIR: &str = "/etc/crabka-gw/broker-client";
const CLUSTER_CA_DIR: &str = "/etc/crabka-gw/cluster-ca";
const CLIENTS_CA_DIR: &str = "/etc/crabka-gw/clients-ca";
const CONFIG_DIR: &str = "/etc/crabka-gw/config";

// ---------------------------------------------------------------------------
// Naming helpers
// ---------------------------------------------------------------------------

/// The operator-issued serving-cert Secret: `<gw>-serving`.
/// Dedup window the gateway assumes when the CR leaves `window` unset.
const DEFAULT_DEDUP_WINDOW: Time = hours(24);

/// ACL refresh cadence the gateway assumes when `aclRefresh` is unset.
const DEFAULT_ACL_REFRESH: Time = minutes(1);

/// Outbound-subscription retry backoff bounds the gateway assumes when the CR
/// leaves them unset; only used to check `maxBackoffMs >= baseBackoffMs`.
const DEFAULT_BASE_BACKOFF: Time = millis(500);
const DEFAULT_MAX_BACKOFF: Time = secs(30);

fn serving_secret_name(gw_name: &str) -> String {
    format!("{gw_name}-serving")
}

/// The child `KafkaUser` (and therefore its issued client-cert Secret):
/// `<gw>-broker`. The `KafkaUser` reconciler names the Secret after the user.
fn broker_user_name(gw_name: &str) -> String {
    format!("{gw_name}-broker")
}

/// The rendered config Secret: `<gw>-config`.
fn config_secret_name(gw_name: &str) -> String {
    format!("{gw_name}-config")
}

/// Common labels for the gateway's owned objects. Mirrors
/// [`common::common_labels`] but with the gateway's own `app` value and
/// `instance = parent Kafka name` (so the objects group under the cluster).
/// These labels are also the Deployment pod-selector + Service selector.
fn gateway_labels(parent_name: &str, gw_name: &str) -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    m.insert(
        "app.kubernetes.io/name".into(),
        "crabka-grpc-gateway".into(),
    );
    m.insert("app.kubernetes.io/instance".into(), parent_name.into());
    m.insert(
        "app.kubernetes.io/managed-by".into(),
        "crabka-operator".into(),
    );
    m.insert("crabka.io/gateway".into(), gw_name.into());
    m
}

// ---------------------------------------------------------------------------
// Pure render helpers
// ---------------------------------------------------------------------------

/// Build a downward-API env entry (`valueFrom.fieldRef`).
fn field_ref_env(name: &str, field_path: &str) -> serde_json::Value {
    json!({ "name": name, "valueFrom": { "fieldRef": { "fieldPath": field_path } } })
}

/// Build a literal env entry.
fn value_env(name: &str, value: impl Into<String>) -> serde_json::Value {
    json!({ "name": name, "value": value.into() })
}

/// Render the gateway Deployment. Pure function of the CR + resolved inputs
/// (image, broker bootstrap, broker SNI). Owner-ref → the gateway CR.
///
/// The pod template carries:
/// - the gateway container (image, the `CRABKA_GATEWAY_*` env per Design
///   §"Config rendering", including `$(POD_NAME)` / `$(POD_IP)` downward-API
///   refs), the broker-TLS + serving-TLS CLI flags pointing at the mounted
///   paths, the webhook/outbound config paths, and telemetry env;
/// - the five volumes + mounts from the Design mount-set table;
/// - `/healthz` + `/readyz` httpGet probes on the gateway port;
/// - `containerPort` 9500.
// linear render pipeline: env + flags + mounts are independent segments
fn deployment(
    gw: &KafkaGrpcGateway,
    parent_name: &str,
    image: &str,
    bootstrap: &str,
    broker_sni: &str,
) -> Result<Deployment, ReconcileError> {
    let gw_name = gw.name_any();
    let namespace = gw.meta().namespace.clone().unwrap_or_default();
    let labels = gateway_labels(parent_name, &gw_name);
    let replicas = gw.spec.replicas.unwrap_or(DEFAULT_REPLICAS);

    let args = gateway_args(gw, &gw_name, bootstrap, broker_sni);

    // Env: downward-API client-id + advertised addr, plus telemetry.
    let mut env = vec![
        field_ref_env("POD_NAME", "metadata.name"),
        field_ref_env("POD_IP", "status.podIP"),
        field_ref_env("POD_NAMESPACE", "metadata.namespace"),
        // `client.id` = the pod name (distinct per replica).
        value_env("CRABKA_GATEWAY_CLIENT_ID", "$(POD_NAME)"),
    ];
    if let Some(t) = gw.spec.telemetry.as_ref() {
        if let Some(ep) = t.otlp_endpoint.as_deref() {
            env.push(value_env("CRABKA_OTLP_ENABLED", "true"));
            env.push(value_env("CRABKA_OTLP_ENDPOINT", ep));
        }
        if let Some(p) = t.otlp_protocol.as_deref() {
            // The gateway's telemetry reads `OTEL_EXPORTER_OTLP_PROTOCOL`-style
            // values; map the CRD's `grpc`/`http` onto them.
            let proto = if p == "http" { "http/protobuf" } else { "grpc" };
            env.push(value_env("CRABKA_OTLP_PROTOCOL", proto));
        }
        if let Some(r) = t.sample_ratio {
            env.push(value_env("CRABKA_OTLP_SAMPLE_RATIO", r.to_string()));
        }
    }

    let volumes = json!([
        {
            "name": "serving",
            "secret": { "secretName": serving_secret_name(&gw_name), "defaultMode": 0o400_i32 }
        },
        {
            "name": "broker-client",
            "secret": { "secretName": broker_user_name(&gw_name), "defaultMode": 0o400_i32 }
        },
        {
            "name": "cluster-ca",
            "secret": { "secretName": cluster_ca_cert_name(parent_name), "defaultMode": 0o400_i32 }
        },
        {
            "name": "clients-ca",
            "secret": { "secretName": format!("{parent_name}-clients-ca-cert"), "defaultMode": 0o400_i32 }
        },
        {
            "name": "config",
            "secret": { "secretName": config_secret_name(&gw_name), "defaultMode": 0o400_i32 }
        },
    ]);

    let volume_mounts = json!([
        { "name": "serving", "mountPath": SERVING_DIR, "readOnly": true },
        { "name": "broker-client", "mountPath": BROKER_CLIENT_DIR, "readOnly": true },
        { "name": "cluster-ca", "mountPath": CLUSTER_CA_DIR, "readOnly": true },
        { "name": "clients-ca", "mountPath": CLIENTS_CA_DIR, "readOnly": true },
        { "name": "config", "mountPath": CONFIG_DIR, "readOnly": true },
    ]);

    let resources = gw
        .spec
        .resources
        .clone()
        .map(serde_json::to_value)
        .transpose()?
        .unwrap_or_else(|| {
            json!({
                "requests": { "cpu": "100m", "memory": "128Mi" },
                "limits": { "cpu": "1000m", "memory": "512Mi" }
            })
        });
    let health = gw.spec.health_checks.as_ref();
    let readiness_initial_delay_seconds = health
        .and_then(|checks| checks.readiness_initial_delay_seconds)
        .unwrap_or(2);
    let readiness_period_seconds = health
        .and_then(|checks| checks.readiness_period_seconds)
        .unwrap_or(5);
    let liveness_initial_delay_seconds = health
        .and_then(|checks| checks.liveness_initial_delay_seconds)
        .unwrap_or(10);
    let liveness_period_seconds = health
        .and_then(|checks| checks.liveness_period_seconds)
        .unwrap_or(10);

    let container = json!({
        "name": "gateway",
        "image": image,
        "args": args,
        "env": env,
        "ports": [{ "containerPort": GATEWAY_PORT, "name": "grpc", "protocol": "TCP" }],
        "resources": resources,
        "volumeMounts": volume_mounts,
        "readinessProbe": {
            "httpGet": { "path": "/readyz", "port": GATEWAY_PORT },
            "initialDelaySeconds": readiness_initial_delay_seconds,
            "periodSeconds": readiness_period_seconds
        },
        "livenessProbe": {
            "httpGet": { "path": "/healthz", "port": GATEWAY_PORT },
            "initialDelaySeconds": liveness_initial_delay_seconds,
            "periodSeconds": liveness_period_seconds
        },
        "securityContext": {
            "allowPrivilegeEscalation": false,
            "readOnlyRootFilesystem": true,
            "capabilities": { "drop": ["ALL"] }
        }
    });

    let dep: Deployment = serde_json::from_value(json!({
        "metadata": {
            "name": gw_name,
            "namespace": namespace,
            "labels": labels,
            "ownerReferences": [owner_ref::<KafkaGrpcGateway>(gw)?],
        },
        "spec": {
            "replicas": replicas,
            "selector": { "matchLabels": labels },
            "template": {
                "metadata": { "labels": labels },
                "spec": {
                    "securityContext": {
                        "runAsNonRoot": true,
                        "runAsUser": 65532,
                        "fsGroup": 65532,
                        "seccompProfile": { "type": "RuntimeDefault" }
                    },
                    "containers": [container],
                    "volumes": volumes,
                }
            }
        }
    }))?;
    Ok(dep)
}

fn gateway_args(
    gateway: &KafkaGrpcGateway,
    gateway_name: &str,
    bootstrap: &str,
    broker_sni: &str,
) -> Vec<String> {
    let authz = gateway.spec.authz.as_ref();
    let bearer = authz.and_then(|value| value.bearer.as_ref());
    let mut args = vec![
        format!("--bootstrap-servers={bootstrap}"),
        format!("--listen-addr=0.0.0.0:{GATEWAY_PORT}"),
        format!("--advertised-addr=$(POD_IP):{GATEWAY_PORT}"),
        format!(
            "--dedup-topic={}",
            gateway
                .spec
                .dedup
                .as_ref()
                .and_then(|value| value.topic.clone())
                .unwrap_or_else(|| format!("{gateway_name}-dedup"))
        ),
        format!(
            "--dedup-partitions={}",
            gateway
                .spec
                .dedup
                .as_ref()
                .and_then(|value| value.partitions)
                .unwrap_or(8)
        ),
        format!(
            "--dedup-window={}",
            gateway
                .spec
                .dedup
                .as_ref()
                .and_then(|value| value.window)
                .unwrap_or(DEFAULT_DEDUP_WINDOW)
                .human()
        ),
        format!(
            "--dedup-txn-id-prefix={}",
            gateway
                .spec
                .dedup
                .as_ref()
                .and_then(|value| value.txn_id_prefix.as_deref())
                .unwrap_or(gateway_name)
        ),
        format!(
            "--dedup-ownership-group={}",
            gateway
                .spec
                .dedup
                .as_ref()
                .and_then(|value| value.ownership_group.as_deref())
                .map_or_else(|| format!("{gateway_name}-dedup-owners"), ToOwned::to_owned)
        ),
        format!("--tls-cert={SERVING_DIR}/tls.crt"),
        format!("--tls-key={SERVING_DIR}/tls.key"),
        format!("--tls-client-ca={CLIENTS_CA_DIR}/ca.crt"),
        format!("--tls-trust-roots={CLUSTER_CA_DIR}/ca.crt"),
        format!(
            "--tls-client-auth={}",
            gateway
                .spec
                .tls
                .as_ref()
                .and_then(|value| value.client_auth.as_deref())
                .unwrap_or("required")
        ),
        format!("--broker-tls-cert={BROKER_CLIENT_DIR}/user.crt"),
        format!("--broker-tls-key={BROKER_CLIENT_DIR}/user.key"),
        format!("--broker-tls-ca={CLUSTER_CA_DIR}/ca.crt"),
        format!("--broker-tls-server-name={broker_sni}"),
        format!(
            "--authz={}",
            authz
                .and_then(|value| value.mode.as_deref())
                .unwrap_or("simple")
        ),
        format!(
            "--authz-super-users={}",
            authz
                .map(|value| value.super_users.join(","))
                .unwrap_or_default()
        ),
        format!(
            "--acl-refresh-interval={}",
            authz
                .and_then(|value| value.acl_refresh)
                .unwrap_or(DEFAULT_ACL_REFRESH)
                .human()
        ),
        format!(
            "--bearer={}",
            bearer
                .and_then(|value| value.mode.as_deref())
                .unwrap_or("off")
        ),
        format!(
            "--bearer-principal-claim={}",
            bearer
                .and_then(|value| value.principal_claim.as_deref())
                .unwrap_or("sub")
        ),
        format!("--webhooks-config={CONFIG_DIR}/webhooks.toml"),
        format!("--outbound-webhooks-config={CONFIG_DIR}/outbound.toml"),
    ];
    if let Some(value) = gateway.spec.membership_topic.as_deref() {
        args.push(format!("--membership-topic={value}"));
    }
    if let Some(value) = gateway
        .spec
        .tls
        .as_ref()
        .and_then(|tls| tls.reload_interval)
    {
        args.push(format!("--tls-reload-interval={}", value.human()));
    }
    if let Some(value) = bearer.and_then(|bearer| bearer.allowable_clock_skew) {
        args.push(format!("--bearer-allowable-clock-skew={}", value.human()));
    }
    if let Some(tuning) = &gateway.spec.tuning {
        // Two arms: one for the fields that are still bare numbers, one that
        // renders a quantity back into the raw unit the gateway's flag expects.
        macro_rules! push {
            ($field:ident) => {
                if let Some(value) = tuning.$field {
                    args.push(format!(
                        "--{}={value}",
                        stringify!($field).replace('_', "-")
                    ));
                }
            };
            (quantity $field:ident) => {
                if let Some(value) = tuning.$field {
                    args.push(format!(
                        "--{}={}",
                        stringify!($field).replace('_', "-"),
                        value.human()
                    ));
                }
            };
        }
        push!(internal_topic_replication_factor);
        push!(internal_topic_allow_replication_fallback);
        push!(quantity internal_topic_create_timeout);
        push!(quantity internal_topic_segment);
        if let Some(value) = tuning.internal_topic_min_cleanable_dirty_ratio {
            args.push(format!(
                "--internal-topic-min-cleanable-dirty-ratio={}",
                value.human()
            ));
        }
        push!(quantity consumer_poll_timeout);
        push!(ownership_warmup_empty_polls);
        push!(quantity readiness_poll_interval);
        push!(quantity produce_max_body);
        push!(quantity forward_max_body);
        if let Some(value) = tuning.client_dispatch_queue_capacity {
            args.push(format!("--client-dispatch-queue-capacity={value}"));
        }
        if let Some(value) = tuning.client_frame_max {
            args.push(format!("--client-frame-max={}B", value.bytes_u64()));
        }
    }
    if let Some(registry) = &gateway.spec.schema_registry {
        if let Some(value) = registry.url.as_deref() {
            args.push(format!("--schema-registry-url={value}"));
        }
        if let Some(value) = registry.latest_cache_ttl {
            args.push(format!(
                "--schema-registry-latest-cache-ttl={}",
                value.human()
            ));
        }
        if let Some(value) = registry.frame_raw {
            args.push(format!("--schema-registry-frame-raw={value}"));
        }
    }
    args.sort_unstable();
    args
}

/// Render the gateway Service: `ClusterIP`, port 9500, selector = the gateway
/// labels, owner-ref → the gateway CR.
fn service(gw: &KafkaGrpcGateway, parent_name: &str) -> Result<Service, ReconcileError> {
    let gw_name = gw.name_any();
    let labels = gateway_labels(parent_name, &gw_name);
    let svc: Service = serde_json::from_value(json!({
        "metadata": {
            "name": gw_name,
            "namespace": gw.meta().namespace.clone(),
            "labels": labels,
            "ownerReferences": [owner_ref::<KafkaGrpcGateway>(gw)?],
        },
        "spec": {
            "type": "ClusterIP",
            "selector": labels,
            "ports": [{
                "name": "grpc",
                "port": GATEWAY_PORT,
                "protocol": "TCP",
                "targetPort": GATEWAY_PORT,
            }],
        }
    }))?;
    Ok(svc)
}

/// Render the `<gw>-config` Secret holding `webhooks.toml` + `outbound.toml`.
///
/// HMAC secrets are passed in already resolved (keyed by webhook / subscription
/// name) — the controller resolves the `secretRef`s from same-namespace Secrets
/// before calling this, so no secret material is ever read from the CR.
///
/// The TOML is serialized into the **exact** gateway schemas
/// (`crabka_grpc_gateway::webhook_config::WebhooksFile` /
/// `outbound_config::OutboundFile`) so it round-trips through the gateway's
/// loader. `allowed_targets` is the union of each subscription's `targetUrl`
/// host and any explicit `spec.allowedTargets`.
fn config_secret(
    gw: &KafkaGrpcGateway,
    resolved_webhook_secrets: &BTreeMap<String, String>,
    resolved_outbound_secrets: &BTreeMap<String, String>,
) -> Result<Secret, ReconcileError> {
    let gw_name = gw.name_any();
    let webhooks_toml = render_webhooks_toml(gw, resolved_webhook_secrets)?;
    let outbound_toml = render_outbound_toml(gw, resolved_outbound_secrets)?;

    let mut data: BTreeMap<String, ByteString> = BTreeMap::new();
    data.insert(
        "webhooks.toml".into(),
        ByteString(webhooks_toml.into_bytes()),
    );
    data.insert(
        "outbound.toml".into(),
        ByteString(outbound_toml.into_bytes()),
    );

    Ok(Secret {
        metadata: ObjectMeta {
            name: Some(config_secret_name(&gw_name)),
            namespace: gw.meta().namespace.clone(),
            labels: Some(gateway_labels(
                gw.meta()
                    .labels
                    .as_ref()
                    .and_then(|l| l.get("crabka.io/cluster"))
                    .map_or(gw_name.as_str(), String::as_str),
                &gw_name,
            )),
            owner_references: Some(vec![owner_ref::<KafkaGrpcGateway>(gw)?]),
            ..Default::default()
        },
        type_: Some("Opaque".into()),
        data: Some(data),
        ..Default::default()
    })
}

/// Serialize `spec.webhooks` into the gateway's `WebhooksFile` TOML.
fn render_webhooks_toml(
    gw: &KafkaGrpcGateway,
    resolved: &BTreeMap<String, String>,
) -> Result<String, ReconcileError> {
    // Build the wire shape with `serde_json::Value` then hand it to the `toml`
    // serializer. The keys match `webhook_config::WebhookEndpoint` exactly
    // (snake_case, no `rename`).
    let endpoints: Vec<serde_json::Value> = gw
        .spec
        .webhooks
        .iter()
        .map(|w| {
            let mut e = serde_json::Map::new();
            e.insert("name".into(), json!(w.name));
            e.insert("target_topic".into(), json!(w.target_topic));
            if let Some(v) = &w.principal {
                e.insert("principal".into(), json!(v));
            }
            if let Some(secret) = resolved.get(&w.name) {
                e.insert("secret".into(), json!(secret));
            }
            if let Some(v) = &w.signature_header {
                e.insert("signature_header".into(), json!(v));
            }
            if let Some(v) = &w.signature_encoding {
                e.insert("signature_encoding".into(), json!(v));
            }
            if let Some(v) = &w.signature_prefix {
                e.insert("signature_prefix".into(), json!(v));
            }
            if let Some(v) = &w.timestamp_header {
                e.insert("timestamp_header".into(), json!(v));
            }
            if let Some(v) = w.timestamp_tolerance {
                e.insert("timestamp_tolerance".into(), json!(v.human().to_string()));
            }
            if let Some(v) = &w.idempotency_source {
                e.insert("idempotency_source".into(), json!(v));
            }
            if let Some(v) = &w.key_source {
                e.insert("key_source".into(), json!(v));
            }
            if let Some(v) = w.max_body {
                e.insert("max_body".into(), json!(v.human().to_string()));
            }
            if let Some(v) = &w.schema_subject {
                e.insert("schema_subject".into(), json!(v));
            }
            if let Some(v) = &w.schema_format {
                e.insert("schema_format".into(), json!(v));
            }
            serde_json::Value::Object(e)
        })
        .collect();
    let doc = json!({ "endpoints": endpoints });
    toml::to_string(&doc).map_err(|e| ReconcileError::Malformed(format!("webhooks.toml: {e}")))
}

/// Serialize `spec.outboundSubscriptions` + derived `allowed_targets` into the
/// gateway's `OutboundFile` TOML.
fn render_outbound_toml(
    gw: &KafkaGrpcGateway,
    resolved: &BTreeMap<String, String>,
) -> Result<String, ReconcileError> {
    let subscriptions: Vec<serde_json::Value> = gw
        .spec
        .outbound_subscriptions
        .iter()
        .map(|s| {
            let mut e = serde_json::Map::new();
            e.insert("name".into(), json!(s.name));
            e.insert("source_topics".into(), json!(s.source_topics));
            e.insert("target_url".into(), json!(s.target_url));
            if let Some(secret) = resolved.get(&s.name) {
                e.insert("signing_secret".into(), json!(secret));
            }
            if let Some(v) = &s.dead_letter_topic {
                e.insert("dead_letter_topic".into(), json!(v));
            }
            if let Some(v) = s.max_attempts {
                e.insert("max_attempts".into(), json!(v));
            }
            if let Some(v) = s.base_backoff {
                e.insert("base_backoff".into(), json!(v.human().to_string()));
            }
            if let Some(v) = s.max_backoff {
                e.insert("max_backoff".into(), json!(v.human().to_string()));
            }
            if let Some(v) = s.request_timeout {
                e.insert("request_timeout".into(), json!(v.human().to_string()));
            }
            if let Some(v) = &s.group_id {
                e.insert("group_id".into(), json!(v));
            }
            if let Some(v) = s.decode_to_json {
                e.insert("decode_to_json".into(), json!(v));
            }
            if let Some(v) = &s.filter {
                e.insert("filter".into(), json!(v));
            }
            if !s.headers.is_empty() {
                e.insert("headers".into(), json!(s.headers));
            }
            serde_json::Value::Object(e)
        })
        .collect();

    let allowed = derive_allowed_targets(gw);

    let doc = json!({
        "subscriptions": subscriptions,
        "allowed_targets": allowed,
    });
    toml::to_string(&doc).map_err(|e| ReconcileError::Malformed(format!("outbound.toml: {e}")))
}

/// Build the SSRF allow-list: every subscription `targetUrl`'s `(scheme, host)`
/// plus any explicit `spec.allowedTargets`. Deduped, order-stable.
fn derive_allowed_targets(gw: &KafkaGrpcGateway) -> Vec<serde_json::Value> {
    let mut out: Vec<(String, String)> = Vec::new();
    let mut push = |scheme: String, host: String| {
        if !out.iter().any(|(s, h)| *s == scheme && *h == host) {
            out.push((scheme, host));
        }
    };
    for s in &gw.spec.outbound_subscriptions {
        if let Ok(url) = reqwest::Url::parse(&s.target_url)
            && let Some(host) = url.host_str()
        {
            push(url.scheme().to_ascii_lowercase(), host.to_string());
        }
    }
    for a in &gw.spec.allowed_targets {
        push(a.scheme.clone(), a.host.clone());
    }
    out.into_iter()
        .map(|(scheme, host)| json!({ "scheme": scheme, "host": host }))
        .collect()
}

/// Render the child `KafkaUser` (`<gw>-broker`): `authentication: tls`, broad
/// ALLOW ACLs (all operations on `Topic:*`, `Group:*`, `TransactionalId:*`, and
/// `Cluster`), `crabka.io/cluster` label, owner-ref → the gateway CR.
///
/// The existing `KafkaUser` reconciler issues a **clients-CA-signed** client cert
/// (Secret `<gw>-broker`, keys `user.crt` / `user.key` / `ca.crt`) and
/// provisions the ACLs — exactly the gateway's broker-mTLS identity.
fn child_kafkauser(gw: &KafkaGrpcGateway, parent_name: &str) -> Result<KafkaUser, ReconcileError> {
    let gw_name = gw.name_any();
    let user_name = broker_user_name(&gw_name);

    let acls = vec![
        broad_acl(AclResourceKind::Topic, "*"),
        broad_acl(AclResourceKind::Group, "*"),
        broad_acl(AclResourceKind::TransactionalId, "*"),
        broad_acl(AclResourceKind::Cluster, "kafka-cluster"),
    ];

    let mut user = KafkaUser::new(
        &user_name,
        KafkaUserSpec {
            authentication: Authentication::Tls(TlsAuth::default()),
            authorization: Some(Authorization::Simple(SimpleAuthorization { acls })),
            quotas: None,
        },
    );
    user.metadata.namespace.clone_from(&gw.meta().namespace);
    let mut labels = BTreeMap::new();
    labels.insert("crabka.io/cluster".into(), parent_name.to_string());
    labels.insert("crabka.io/gateway".into(), gw_name.clone());
    user.metadata.labels = Some(labels);
    user.metadata.owner_references = Some(vec![owner_ref::<KafkaGrpcGateway>(gw)?]);
    Ok(user)
}

/// One `All`-operation ALLOW rule on a `literal` resource named `name`.
fn broad_acl(kind: AclResourceKind, name: &str) -> AclRule {
    AclRule {
        resource: AclResource {
            kind,
            name: name.into(),
            pattern_type: crate::crd::user::AclPatternType::Literal,
        },
        operations: vec![AclOp::All],
        host: "*".into(),
        permission: AclPermission::Allow,
    }
}

// ---------------------------------------------------------------------------
// Serving cert
// ---------------------------------------------------------------------------

/// Ensure the `<gw>-serving` Secret holds a current cluster-CA-signed serving
/// cert for the gateway Service DNS.
///
/// Loads the parent's cluster CA (`<parent>-cluster-ca` key `ca.key` +
/// `<parent>-cluster-ca-cert` key `ca.crt`), issues a serving cert via
/// [`issue_broker_cert`] with `base_sans` = the Service DNS names, and SSA's the
/// Opaque Secret (`tls.crt` / `tls.key`), owner-ref → the gateway. Re-issues
/// when the stored cert is within its renewal window.
#[tracing::instrument(
    level = "info",
    skip_all,
    fields(gateway = %gw.name_any(), namespace = %namespace, parent = %parent_name),
    err,
)]
async fn ensure_serving_cert(
    secret_api: &Api<Secret>,
    gw: &KafkaGrpcGateway,
    namespace: &str,
    parent_name: &str,
) -> Result<(), ReconcileError> {
    let gw_name = gw.name_any();
    let secret_name = serving_secret_name(&gw_name);

    // If a current cert exists and is comfortably in the future, no-op.
    let now = OffsetDateTime::now_utc();
    if let Some(existing) = secret_api.get_opt(&secret_name).await?
        && let Some(cert_pem) = read_pem_key(&existing, "tls.crt")
        && !renew_if_expiring(&cert_pem, crabka_units::days(30), now).unwrap_or(true)
    {
        return Ok(());
    }

    // Load the cluster CA key + cert.
    let key_name = cluster_ca_key_name(parent_name);
    let cert_name = cluster_ca_cert_name(parent_name);
    let key_secret =
        secret_api
            .get_opt(&key_name)
            .await?
            .ok_or_else(|| ReconcileError::CaSecretMissing {
                name: key_name.clone(),
            })?;
    let cert_secret =
        secret_api
            .get_opt(&cert_name)
            .await?
            .ok_or_else(|| ReconcileError::CaSecretMissing {
                name: cert_name.clone(),
            })?;
    let ca_key_pem = read_pem_key(&key_secret, "ca.key")
        .ok_or_else(|| ReconcileError::CertParse(format!("{key_name} ca.key unreadable")))?;
    let ca_cert_pem = read_pem_key(&cert_secret, "ca.crt")
        .ok_or_else(|| ReconcileError::CertParse(format!("{cert_name} ca.crt unreadable")))?;

    let validity = gw
        .spec
        .tls
        .as_ref()
        .and_then(|t| t.validity_days)
        .unwrap_or(DEFAULT_VALIDITY_DAYS);

    let base_sans = vec![
        SubjectAltName::Dns(format!("{gw_name}.{namespace}.svc")),
        SubjectAltName::Dns(format!("{gw_name}.{namespace}.svc.cluster.local")),
        SubjectAltName::Dns(gw_name.clone()),
    ];

    let leaf = issue_broker_cert(
        &ca_cert_pem,
        &ca_key_pem,
        &gw_name,
        &base_sans,
        &[],
        validity,
    )?;

    let mut data: BTreeMap<String, ByteString> = BTreeMap::new();
    data.insert("tls.crt".into(), ByteString(leaf.cert_pem.into_bytes()));
    data.insert("tls.key".into(), ByteString(leaf.key_pem.into_bytes()));

    let secret = Secret {
        metadata: ObjectMeta {
            name: Some(secret_name.clone()),
            namespace: Some(namespace.to_string()),
            labels: Some(gateway_labels(parent_name, &gw_name)),
            owner_references: Some(vec![owner_ref::<KafkaGrpcGateway>(gw)?]),
            ..Default::default()
        },
        type_: Some("Opaque".into()),
        data: Some(data),
        ..Default::default()
    };
    apply_object(secret_api, &secret_name, &secret).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// SecretRef resolution
// ---------------------------------------------------------------------------

/// Resolve a `(secret-name, key)` reference from a same-namespace Secret,
/// returning the value as a UTF-8 string. `Secret.data` is base64-decoded by
/// kube-rs into `ByteString`, so we just read the bytes.
async fn resolve_secret_ref(
    secret_api: &Api<Secret>,
    secret_name: &str,
    key: &str,
) -> Result<String, ReconcileError> {
    let secret = secret_api.get_opt(secret_name).await?.ok_or_else(|| {
        ReconcileError::Malformed(format!("secretRef Secret '{secret_name}' not found"))
    })?;
    let bytes = secret
        .data
        .as_ref()
        .and_then(|d| d.get(key))
        .map(|b| b.0.clone())
        .ok_or_else(|| {
            ReconcileError::Malformed(format!(
                "secretRef Secret '{secret_name}' has no key '{key}'"
            ))
        })?;
    String::from_utf8(bytes).map_err(|e| {
        ReconcileError::Malformed(format!("secretRef Secret '{secret_name}' key '{key}': {e}"))
    })
}

/// Resolve every webhook `secretRef` + outbound `signingSecretRef` from
/// same-namespace Secrets. Returns `(webhook name → secret, subscription name →
/// secret)`.
async fn resolve_all_secret_refs(
    secret_api: &Api<Secret>,
    gw: &KafkaGrpcGateway,
) -> Result<(BTreeMap<String, String>, BTreeMap<String, String>), ReconcileError> {
    let mut webhook_secrets = BTreeMap::new();
    for w in &gw.spec.webhooks {
        if let Some(r) = &w.secret_ref {
            let value = resolve_secret_ref(secret_api, &r.name, &r.key).await?;
            webhook_secrets.insert(w.name.clone(), value);
        }
    }
    let mut outbound_secrets = BTreeMap::new();
    for s in &gw.spec.outbound_subscriptions {
        if let Some(r) = &s.signing_secret_ref {
            let value = resolve_secret_ref(secret_api, &r.name, &r.key).await?;
            outbound_secrets.insert(s.name.clone(), value);
        }
    }
    Ok((webhook_secrets, outbound_secrets))
}

// ---------------------------------------------------------------------------
// Version gate (copied from kafka_node_pool::version_gate)
// ---------------------------------------------------------------------------

/// Whether the parent's version model clears the gateway to deploy. Same logic
/// as `kafka_node_pool::version_gate`: cleared when the parent carries
/// `KafkaVersionValid=True` OR a finalized `status.metadataVersion`.
fn version_gate(parent: &Kafka) -> Option<KafkaCondition> {
    let cond = match parent_version_gate(parent) {
        common::ParentVersionGate::Cleared => return None,
        common::ParentVersionGate::Invalid(c) => condition(
            "Ready",
            "False",
            "KafkaVersionInvalid",
            &format!(
                "refusing to deploy gateway: parent Kafka '{}' KafkaVersionValid={} ({}): {}",
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
                "waiting for parent Kafka '{}' to publish a KafkaVersionValid verdict before deploying the gateway",
                parent.name_any()
            ),
        ),
    };
    Some(cond)
}

// ---------------------------------------------------------------------------
// Broker endpoint resolution
// ---------------------------------------------------------------------------

/// Resolve the broker `(bootstrap, sni)` the gateway should dial.
///
/// The gateway authenticates to the broker with its **clients-CA-signed**
/// client cert (issued by the child `KafkaUser`) and is recognised as
/// `User:CN=<gw>-broker`. That only works against a broker listener that is
/// (a) **internal** (in-cluster headless DNS), (b) **TLS** (the transport the
/// gateway's `--broker-tls-*` flags speak), and (c) **`authentication: tls`**
/// (mTLS — the broker maps the client cert subject to a Kafka principal). A
/// plaintext `PLAIN` listener on 9092 is none of these, so the gateway must
/// never be pointed at it.
///
/// The predicate (internal + `tls` + `authentication == tls`) lives on
/// `spec.listeners`; the resolved in-cluster `host:port` lives on
/// `status.listeners[name].bootstrap_servers` (populated once
/// `ListenersReady`). We correlate the two by listener `name`. The SNI is the
/// broker serving-cert SAN at that listener — the shared headless-svc DNS
/// `<kafka>-broker-headless.<ns>.svc.cluster.local` (see the broker keystore
/// SANs in `controller::kafka`), which is stable across every internal
/// listener regardless of port.
///
/// Returns `None` when no internal TLS+mTLS listener exists *or* its bootstrap
/// has not yet resolved into `status.listeners` — the caller surfaces that as a
/// `Ready=False reason=NoTlsListener` degraded condition and renders no
/// Deployment.
fn resolve_broker_endpoint(parent: &Kafka, namespace: &str) -> Option<(String, String)> {
    let status = parent.status.as_ref()?;
    // The eligible listener: internal, TLS transport, mTLS client auth.
    let listener = parent.spec.listeners.iter().find(|l| {
        l.type_ == crate::crd::ListenerType::Internal
            && l.tls
            && matches!(
                l.authentication,
                Some(crate::crd::ListenerAuthentication::Tls)
            )
    })?;
    // Its resolved in-cluster bootstrap host:port from status.listeners.
    let bootstrap = status
        .listeners
        .iter()
        .find(|s| s.name == listener.name)
        .map(|s| s.bootstrap_servers.clone())?;
    // SNI = the broker serving-cert SAN at the headless Service (matches the
    // SANs minted in `controller::kafka`'s broker keystore).
    let sni = format!(
        "{}-broker-headless.{namespace}.svc.cluster.local",
        parent.name_any()
    );
    Some((bootstrap, sni))
}

// ---------------------------------------------------------------------------
// Status helpers
// ---------------------------------------------------------------------------

fn validate_internal_topic_dirty_ratio(value: Option<Ratio>) -> Result<(), String> {
    if value.is_some_and(|value| {
        !value.as_f64().is_finite()
            || value < <Ratio as crabka_units::convert::RatioExt>::ZERO
            || value > <Ratio as crabka_units::convert::RatioExt>::ONE
    }) {
        return Err(
            "spec.tuning.internalTopicMinCleanableDirtyRatio: must be between 0% and 100%".into(),
        );
    }
    Ok(())
}

fn validate_protocol_millis_i32(value: Option<Time>, path: &str) -> Result<(), String> {
    let Some(value) = value else {
        return Ok(());
    };
    let millis = value.millis_i64();
    if !value.secs_f64().is_finite() || Time::from_millis(millis) != value {
        return Err(format!(
            "{path}: must be a positive whole number of milliseconds within 1..=i32::MAX"
        ));
    }
    let millis = i32::try_from(millis).map_err(|_| {
        format!("{path}: must be a positive whole number of milliseconds within 1..=i32::MAX")
    })?;
    refined_type::rule::GreaterI32::<0>::new(millis)
        .map(|_| ())
        .map_err(|_| {
            format!("{path}: must be a positive whole number of milliseconds within 1..=i32::MAX")
        })
}

// f64 represents every integer below 2^53; at this value adjacent inputs can
// collapse before validation sees the UOM quantity.
const FIRST_AMBIGUOUS_F64_MILLIS: i64 = 9_007_199_254_740_992;

fn validate_protocol_millis_i64(value: Option<Time>, path: &str) -> Result<(), String> {
    let Some(value) = value else {
        return Ok(());
    };
    let millis = value.millis_i64();
    if millis >= FIRST_AMBIGUOUS_F64_MILLIS {
        return Err(format!(
            "{path}: must be below {FIRST_AMBIGUOUS_F64_MILLIS}ms because UOM quantities use f64"
        ));
    }
    if !value.secs_f64().is_finite() || Time::from_millis(millis) != value {
        return Err(format!(
            "{path}: must be a positive whole number of milliseconds within 1..=i64::MAX"
        ));
    }
    refined_type::rule::GreaterI64::<0>::new(millis)
        .map(|_| ())
        .map_err(|_| {
            format!("{path}: must be a positive whole number of milliseconds within 1..=i64::MAX")
        })
}

fn validate_config(spec: &crate::crd::grpc_gateway::KafkaGrpcGatewaySpec) -> Result<(), String> {
    macro_rules! validate {
        ($value:expr, $rule:ty, $path:literal) => {
            if let Some(value) = $value {
                <$rule>::new(value).map_err(|error| format!("{}: {error}", $path))?;
            }
        };
    }
    macro_rules! nonempty {
        ($value:expr, $path:literal) => {
            if let Some(value) = $value {
                refined_type::rule::NonEmptyString::new(value.to_owned())
                    .map_err(|error| format!("{}: {error}", $path))?;
            }
        };
    }

    validate!(
        spec.replicas,
        refined_type::rule::GreaterI32<0>,
        "spec.replicas"
    );
    nonempty!(spec.image.as_deref(), "spec.image");
    nonempty!(spec.membership_topic.as_deref(), "spec.membershipTopic");

    if let Some(tuning) = &spec.tuning {
        tuning
            .client_dispatch_queue_capacity
            .map(crabka_client_core::ConnectionDispatchQueueCapacity::new)
            .transpose()
            .map_err(|error| format!("spec.tuning.clientDispatchQueueCapacity: {error}"))?;
        tuning
            .client_frame_max
            .map(crabka_client_core::ClientFrameMax::try_from)
            .transpose()
            .map_err(|error| format!("spec.tuning.clientFrameMax: {error}"))?;
        validate!(
            tuning.internal_topic_replication_factor,
            refined_type::rule::GreaterI16<0>,
            "spec.tuning.internalTopicReplicationFactor"
        );
        validate_protocol_millis_i32(
            tuning.internal_topic_create_timeout,
            "spec.tuning.internalTopicCreateTimeout",
        )?;
        validate_protocol_millis_i64(
            tuning.internal_topic_segment,
            "spec.tuning.internalTopicSegment",
        )?;
        validate_internal_topic_dirty_ratio(tuning.internal_topic_min_cleanable_dirty_ratio)?;
        validate!(
            tuning.consumer_poll_timeout.map(millis_u64),
            refined_type::rule::GreaterU64<0>,
            "spec.tuning.consumerPollTimeout"
        );
        validate!(
            tuning.ownership_warmup_empty_polls,
            refined_type::rule::GreaterU32<0>,
            "spec.tuning.ownershipWarmupEmptyPolls"
        );
        validate!(
            tuning.readiness_poll_interval.map(millis_u64),
            refined_type::rule::GreaterU64<0>,
            "spec.tuning.readinessPollInterval"
        );
        validate_produce_max_body(tuning.produce_max_body)?;
        validate_forward_max_body(tuning.forward_max_body)?;
    }
    if let Some(registry) = &spec.schema_registry {
        nonempty!(registry.url.as_deref(), "spec.schemaRegistry.url");
        if let Some(url) = registry.url.as_deref() {
            reqwest::Url::parse(url)
                .map_err(|error| format!("spec.schemaRegistry.url: {error}"))?;
        }
        validate!(
            registry.latest_cache_ttl.map(millis_u64),
            refined_type::rule::GreaterU64<0>,
            "spec.schemaRegistry.latestCacheTtl"
        );
    }
    validate_health_checks(spec.health_checks.as_ref())?;
    if let Some(dedup) = &spec.dedup {
        nonempty!(dedup.topic.as_deref(), "spec.dedup.topic");
        validate!(
            dedup.partitions,
            refined_type::rule::MinMaxU32<1, 2_147_483_647>,
            "spec.dedup.partitions"
        );
        validate_protocol_millis_i64(dedup.window, "spec.dedup.window")?;
        nonempty!(dedup.txn_id_prefix.as_deref(), "spec.dedup.txnIdPrefix");
        nonempty!(
            dedup.ownership_group.as_deref(),
            "spec.dedup.ownershipGroup"
        );
    }
    if let Some(tls) = &spec.tls {
        if let Some(mode) = tls.client_auth.as_deref()
            && !matches!(mode, "disabled" | "optional" | "required")
        {
            return Err("spec.tls.clientAuth must be disabled, optional, or required".into());
        }
        validate!(
            tls.validity_days,
            refined_type::rule::GreaterU32<0>,
            "spec.tls.validityDays"
        );
        validate!(
            tls.reload_interval.map(secs_u64),
            refined_type::rule::GreaterU64<0>,
            "spec.tls.reloadInterval"
        );
    }
    if let Some(authz) = &spec.authz {
        if let Some(mode) = authz.mode.as_deref()
            && !matches!(mode, "off" | "simple")
        {
            return Err("spec.authz.mode must be off or simple".into());
        }
        validate!(
            authz.acl_refresh.map(secs_u64),
            refined_type::rule::GreaterU64<0>,
            "spec.authz.aclRefresh"
        );
        for user in &authz.super_users {
            refined_type::rule::NonEmptyString::new(user.clone())
                .map_err(|error| format!("spec.authz.superUsers: {error}"))?;
        }
        if let Some(bearer) = &authz.bearer {
            if let Some(mode) = bearer.mode.as_deref()
                && !matches!(mode, "off" | "unsecured")
            {
                return Err("spec.authz.bearer.mode must be off or unsecured".into());
            }
            nonempty!(
                bearer.principal_claim.as_deref(),
                "spec.authz.bearer.principalClaim"
            );
            validate!(
                bearer.allowable_clock_skew.map(TimeExt::millis_i64),
                refined_type::rule::GreaterEqualI64<0>,
                "spec.authz.bearer.allowableClockSkew"
            );
        }
    }
    for webhook in &spec.webhooks {
        refined_type::rule::NonEmptyString::new(webhook.name.clone())
            .map_err(|error| format!("spec.webhooks.name: {error}"))?;
        refined_type::rule::NonEmptyString::new(webhook.target_topic.clone())
            .map_err(|error| format!("spec.webhooks.targetTopic: {error}"))?;
        if let Some(encoding) = webhook.signature_encoding.as_deref()
            && !matches!(encoding, "hex" | "base64")
        {
            return Err("spec.webhooks.signatureEncoding must be hex or base64".into());
        }
        if webhook.secret_ref.is_some() != webhook.signature_header.is_some() {
            return Err("spec.webhooks.secretRef and signatureHeader must be set together".into());
        }
        if let Some(value) = webhook.idempotency_source.as_deref() {
            validate_webhook_source(value, "spec.webhooks.idempotencySource")?;
        }
        if let Some(value) = webhook.key_source.as_deref() {
            validate_webhook_source(value, "spec.webhooks.keySource")?;
        }
        validate!(
            webhook.timestamp_tolerance.map(TimeExt::secs_i64),
            refined_type::rule::GreaterEqualI64<0>,
            "spec.webhooks.timestampTolerance"
        );
        validate!(
            webhook.max_body.map(ByteSizeExt::bytes_u64),
            refined_type::rule::GreaterU64<0>,
            "spec.webhooks.maxBody"
        );
        nonempty!(
            webhook.schema_subject.as_deref(),
            "spec.webhooks.schemaSubject"
        );
        if let Some(format) = webhook.schema_format.as_deref()
            && !matches!(format, "avro" | "json" | "protobuf")
        {
            return Err("spec.webhooks.schemaFormat must be avro, json, or protobuf".into());
        }
    }
    validate_outbound_config(spec)?;
    if let Some(telemetry) = &spec.telemetry {
        nonempty!(
            telemetry.otlp_endpoint.as_deref(),
            "spec.telemetry.otlpEndpoint"
        );
        if let Some(protocol) = telemetry.otlp_protocol.as_deref()
            && !matches!(protocol, "grpc" | "http")
        {
            return Err("spec.telemetry.otlpProtocol must be grpc or http".into());
        }
        if let Some(ratio) = telemetry.sample_ratio
            && (!ratio.is_finite() || !(0.0..=1.0).contains(&ratio))
        {
            return Err("spec.telemetry.sampleRatio must be finite and between 0 and 1".into());
        }
    }
    Ok(())
}

fn validate_produce_max_body(value: Option<ByteSize>) -> Result<(), String> {
    if let Some(value) = value.map(ByteSizeExt::bytes_u64) {
        refined_type::rule::GreaterU64::<0>::new(value)
            .map_err(|error| format!("spec.tuning.produceMaxBody: {error}"))?;
    }
    Ok(())
}

fn validate_forward_max_body(value: Option<ByteSize>) -> Result<(), String> {
    if let Some(value) = value.map(ByteSizeExt::bytes_u64) {
        refined_type::rule::GreaterU64::<0>::new(value)
            .map_err(|error| format!("spec.tuning.forwardMaxBody: {error}"))?;
    }
    Ok(())
}

fn validate_health_checks(
    health: Option<&crate::crd::grpc_gateway::GatewayHealthChecks>,
) -> Result<(), String> {
    let Some(health) = health else {
        return Ok(());
    };
    for (value, path, permits_zero) in [
        (
            health.readiness_initial_delay_seconds,
            "spec.healthChecks.readinessInitialDelaySeconds",
            true,
        ),
        (
            health.readiness_period_seconds,
            "spec.healthChecks.readinessPeriodSeconds",
            false,
        ),
        (
            health.liveness_initial_delay_seconds,
            "spec.healthChecks.livenessInitialDelaySeconds",
            true,
        ),
        (
            health.liveness_period_seconds,
            "spec.healthChecks.livenessPeriodSeconds",
            false,
        ),
    ] {
        if let Some(value) = value {
            if permits_zero {
                refined_type::rule::GreaterEqualI32::<0>::new(value)
                    .map_err(|error| format!("{path}: {error}"))?;
            } else {
                refined_type::rule::GreaterI32::<0>::new(value)
                    .map_err(|error| format!("{path}: {error}"))?;
            }
        }
    }
    Ok(())
}

fn validate_webhook_source(value: &str, path: &str) -> Result<(), String> {
    if value.strip_prefix("header:").is_some() {
        return Ok(());
    }
    let Some(json_path) = value.strip_prefix("json:") else {
        return Err(format!("{path}: must start with 'header:' or 'json:'"));
    };
    jsonpath_rust::parser::parse_json_path(json_path)
        .map(|_| ())
        .map_err(|error| format!("{path}: invalid JSONPath {json_path:?}: {error}"))
}

fn validate_outbound_config(
    spec: &crate::crd::grpc_gateway::KafkaGrpcGatewaySpec,
) -> Result<(), String> {
    macro_rules! validate {
        ($value:expr, $rule:ty, $path:literal) => {
            if let Some(value) = $value {
                <$rule>::new(value).map_err(|error| format!("{}: {error}", $path))?;
            }
        };
    }

    for subscription in &spec.outbound_subscriptions {
        refined_type::rule::NonEmptyString::new(subscription.name.clone())
            .map_err(|error| format!("spec.outboundSubscriptions.name: {error}"))?;
        refined_type::rule::NonEmptyString::new(subscription.target_url.clone())
            .map_err(|error| format!("spec.outboundSubscriptions.targetUrl: {error}"))?;
        let url = reqwest::Url::parse(&subscription.target_url).map_err(|error| {
            format!("spec.outboundSubscriptions.targetUrl: invalid URL: {error}")
        })?;
        url.host_str()
            .ok_or_else(|| "spec.outboundSubscriptions.targetUrl: URL has no host".to_string())?;
        for topic in &subscription.source_topics {
            refined_type::rule::NonEmptyString::new(topic.clone())
                .map_err(|error| format!("spec.outboundSubscriptions.sourceTopics: {error}"))?;
        }
        validate!(
            subscription.max_attempts,
            refined_type::rule::GreaterU32<0>,
            "spec.outboundSubscriptions.maxAttempts"
        );
        validate!(
            subscription.base_backoff.map(millis_u64),
            refined_type::rule::GreaterU64<0>,
            "spec.outboundSubscriptions.baseBackoff"
        );
        validate!(
            subscription.max_backoff.map(millis_u64),
            refined_type::rule::GreaterU64<0>,
            "spec.outboundSubscriptions.maxBackoff"
        );
        validate!(
            subscription.request_timeout.map(millis_u64),
            refined_type::rule::GreaterU64<0>,
            "spec.outboundSubscriptions.requestTimeout"
        );
        if let Some(group_id) = &subscription.group_id {
            refined_type::rule::NonEmptyString::new(group_id.clone())
                .map_err(|error| format!("spec.outboundSubscriptions.groupId: {error}"))?;
        }
        if subscription.max_backoff.unwrap_or(DEFAULT_MAX_BACKOFF)
            < subscription.base_backoff.unwrap_or(DEFAULT_BASE_BACKOFF)
        {
            return Err(
                "spec.outboundSubscriptions.maxBackoff must be at least baseBackoff".into(),
            );
        }
        if let Some(filter) = subscription.filter.as_deref() {
            let Some(json_path) = filter.strip_prefix("json:") else {
                return Err("spec.outboundSubscriptions.filter must start with 'json:'".into());
            };
            jsonpath_rust::parser::parse_json_path(json_path).map_err(|error| {
                format!("spec.outboundSubscriptions.filter: invalid JSONPath: {error}")
            })?;
        }
    }
    for target in &spec.allowed_targets {
        if !matches!(target.scheme.as_str(), "http" | "https") {
            return Err("spec.allowedTargets.scheme must be http or https".into());
        }
        refined_type::rule::NonEmptyString::new(target.host.clone())
            .map_err(|error| format!("spec.allowedTargets.host: {error}"))?;
    }
    Ok(())
}

/// Patch a single-condition status onto the gateway (preserving the
/// observed-generation echo).
#[tracing::instrument(level = "info", skip_all, fields(name = %name, conditions = conditions.len()), err)]
async fn patch_conditions(
    gw_api: &Api<KafkaGrpcGateway>,
    name: &str,
    observed_generation: Option<i64>,
    conditions: Vec<KafkaCondition>,
) -> Result<(), ReconcileError> {
    let status = KafkaGrpcGatewayStatus {
        conditions,
        observed_generation,
        ready_replicas: None,
    };
    patch_status::<KafkaGrpcGateway, KafkaGrpcGatewayStatus>(gw_api, name, status).await
}

// ---------------------------------------------------------------------------
// Reconcile
// ---------------------------------------------------------------------------

/// Reconcile one `KafkaGrpcGateway`. Orchestrates the Design §"Controller flow":
/// label → parent, version gate, child `KafkaUser` + its client Secret, serving
/// cert, config Secret (secretRefs resolved), Deployment + Service, status.
/// Reconcile entry point. Times the pass and records the reconcile
/// counter/histogram, then delegates to the internal `reconcile_inner` operation.
#[tracing::instrument(
    level = "info",
    skip_all,
    fields(
        kind = "KafkaGrpcGateway",
        namespace = %gw.namespace().unwrap_or_else(|| "default".into()),
        name = %gw.name_any(),
        generation = ?gw.meta().generation,
    )
)]
/// # Errors
/// Returns an error when cluster state cannot be loaded, the proposed plan is invalid, or a broker, Kubernetes, or persistence operation fails.
pub async fn reconcile(
    gw: Arc<KafkaGrpcGateway>,
    ctx: Arc<Context>,
) -> Result<Action, ReconcileError> {
    common::record_reconcile(
        &ctx,
        "KafkaGrpcGateway",
        Box::pin(reconcile_inner(gw, ctx.clone())),
    )
    .await
}

// linear 7-step controller flow; each step is independent
async fn reconcile_inner(
    gw: Arc<KafkaGrpcGateway>,
    ctx: Arc<Context>,
) -> Result<Action, ReconcileError> {
    let ns = gw.namespace().unwrap_or_else(|| "default".into());
    let name = gw.name_any();
    let observed_generation = gw.meta().generation;

    let gw_api: Api<KafkaGrpcGateway> = Api::namespaced(ctx.client.clone(), &ns);
    let secret_api: Api<Secret> = Api::namespaced(ctx.client.clone(), &ns);

    if let Err(why) = validate_config(&gw.spec) {
        let ready = condition("Ready", "False", "GatewayConfigInvalid", &why);
        patch_conditions(&gw_api, &name, observed_generation, vec![ready]).await?;
        return Err(ReconcileError::GatewayConfigInvalid(why));
    }

    // (1) Parse the `crabka.io/cluster` label → fetch the parent Kafka.
    let Some(parent_name) = gw
        .meta()
        .labels
        .as_ref()
        .and_then(|l| l.get("crabka.io/cluster").cloned())
    else {
        let cond = condition(
            "Ready",
            "False",
            "MissingClusterLabel",
            "metadata.labels.\"crabka.io/cluster\" is required to link a gateway to its parent Kafka",
        );
        patch_conditions(&gw_api, &name, observed_generation, vec![cond]).await?;
        return Ok(common::requeue(ctx.config.controller_dependency_requeue));
    };

    let kafka_api: Api<Kafka> = Api::namespaced(ctx.client.clone(), &ns);
    let Some(parent) = kafka_api.get_opt(&parent_name).await? else {
        let cond = condition(
            "Ready",
            "False",
            "ParentNotFound",
            &format!("Kafka '{parent_name}' not found in namespace '{ns}'"),
        );
        patch_conditions(&gw_api, &name, observed_generation, vec![cond]).await?;
        return Ok(common::requeue(ctx.config.controller_dependency_requeue));
    };

    // (2) Version gate (copy of the pool's logic).
    if let Some(cond) = version_gate(&parent) {
        let version_valid = condition("KafkaVersionValid", "False", &cond.reason, &cond.message);
        patch_conditions(
            &gw_api,
            &name,
            observed_generation,
            vec![cond, version_valid],
        )
        .await?;
        return Ok(common::requeue(ctx.config.controller_dependency_requeue));
    }

    // (3) SSA the child KafkaUser; GET its issued Secret. If absent, the
    //     KafkaUser reconciler hasn't issued the cert yet — wait.
    let user = child_kafkauser(&gw, &parent_name)?;
    let user_name = broker_user_name(&name);
    let user_api: Api<KafkaUser> = Api::namespaced(ctx.client.clone(), &ns);
    apply_object(&user_api, &user_name, &user).await?;

    if secret_api.get_opt(&user_name).await?.is_none() {
        let cond = condition(
            "CertReady",
            "False",
            "WaitingForBrokerCert",
            &format!(
                "child KafkaUser '{user_name}' has not yet been issued its broker-client cert Secret"
            ),
        );
        let ready = condition(
            "Ready",
            "False",
            "WaitingForBrokerCert",
            "waiting for the gateway's broker-mTLS client cert",
        );
        patch_conditions(&gw_api, &name, observed_generation, vec![ready, cond]).await?;
        return Ok(common::requeue(ctx.config.controller_error_requeue));
    }

    // (4) Issue / renew the serving cert.
    ensure_serving_cert(&secret_api, &gw, &ns, &parent_name).await?;

    // (5) Resolve secretRefs + render the config Secret.
    let (webhook_secrets, outbound_secrets) = resolve_all_secret_refs(&secret_api, &gw).await?;
    let cfg = config_secret(&gw, &webhook_secrets, &outbound_secrets)?;
    apply_object(&secret_api, &config_secret_name(&name), &cfg).await?;

    // (6) Resolve the broker bootstrap + SNI from the parent's TLS+mTLS
    //     internal listener. Full-mTLS to the broker is the P9 design: the
    //     gateway's clients-CA client cert authenticates it as a Kafka
    //     principal, which only works against a `tls` + `authentication: tls`
    //     listener. If no such listener exists (or its bootstrap hasn't
    //     resolved into status yet), refuse to render the Deployment — pointing
    //     the gateway's forced TLS handshake at a plaintext listener would fail
    //     every broker connection.
    let Some((bootstrap, broker_sni)) = resolve_broker_endpoint(&parent, &ns) else {
        let ready = condition(
            "Ready",
            "False",
            "NoTlsListener",
            &format!(
                "no internal TLS listener with authentication=tls found on Kafka '{parent_name}'; the gateway requires mTLS to the broker"
            ),
        );
        let degraded = condition(
            "Degraded",
            "True",
            "NoTlsListener",
            &format!(
                "Kafka '{parent_name}' exposes no internal TLS+mTLS listener for the gateway to dial"
            ),
        );
        patch_conditions(&gw_api, &name, observed_generation, vec![ready, degraded]).await?;
        return Ok(common::requeue(ctx.config.controller_dependency_requeue));
    };

    // Image: spec override > operator default (--default-gateway-image) > built-in default.
    let image = gw
        .spec
        .image
        .clone()
        .or_else(|| ctx.config.default_gateway_image.clone())
        .unwrap_or_else(|| DEFAULT_GATEWAY_IMAGE.into());

    let dep = deployment(&gw, &parent_name, &image, &bootstrap, &broker_sni)?;
    let dep_api: Api<Deployment> = Api::namespaced(ctx.client.clone(), &ns);
    apply_object(&dep_api, &name, &dep).await?;

    let svc = service(&gw, &parent_name)?;
    let svc_api: Api<Service> = Api::namespaced(ctx.client.clone(), &ns);
    apply_object(&svc_api, &name, &svc).await?;

    // (7) Read the Deployment back; derive Ready from readyReplicas == replicas.
    let live = dep_api.get_opt(&name).await?;
    let desired_replicas = gw.spec.replicas.unwrap_or(DEFAULT_REPLICAS);
    let ready_replicas = live
        .as_ref()
        .and_then(|d| d.status.as_ref())
        .and_then(|s| s.ready_replicas);
    let ready = ready_replicas.unwrap_or(0) == desired_replicas;
    let (status_val, reason, message) = if ready {
        (
            "True",
            "Available",
            format!("{desired_replicas} gateway replica(s) ready"),
        )
    } else {
        (
            "False",
            "Progressing",
            format!(
                "{}/{desired_replicas} gateway replica(s) ready",
                ready_replicas.unwrap_or(0)
            ),
        )
    };

    let status = KafkaGrpcGatewayStatus {
        conditions: vec![
            condition("Ready", status_val, reason, &message),
            condition(
                "CertReady",
                "True",
                "Issued",
                "serving + broker-client certs present",
            ),
        ],
        observed_generation,
        ready_replicas,
    };
    patch_status::<KafkaGrpcGateway, KafkaGrpcGatewayStatus>(&gw_api, &name, status).await?;

    Ok(common::requeue(ctx.config.controller_dependency_requeue))
}

/// Requeue on transient error.
pub fn error_policy(
    _obj: Arc<KafkaGrpcGateway>,
    err: &ReconcileError,
    ctx: Arc<Context>,
) -> Action {
    tracing::warn!(error = %err, "gateway reconcile error, requeueing");
    common::error_requeue(ctx)
}

/// Run the `KafkaGrpcGateway` controller forever. Owns the Deployment, Service,
/// the serving + config Secrets, and the child `KafkaUser`; watches the parent
/// `Kafka` so a version-validity flip re-triggers gateways.
/// # Errors
/// Returns an error when cluster state cannot be loaded, the proposed plan is invalid, or a broker, Kubernetes, or persistence operation fails.
pub async fn run(ctx: Context) -> anyhow::Result<()> {
    let api: Api<KafkaGrpcGateway> = Api::all(ctx.client.clone());
    let deployments: Api<Deployment> = Api::all(ctx.client.clone());
    let services: Api<Service> = Api::all(ctx.client.clone());
    let secrets: Api<Secret> = Api::all(ctx.client.clone());
    let kafkausers: Api<KafkaUser> = Api::all(ctx.client.clone());
    let kafkas: Api<Kafka> = Api::all(ctx.client.clone());

    Controller::new(api, watcher::Config::default())
        .owns(deployments, watcher::Config::default())
        .owns(services, watcher::Config::default())
        .owns(secrets, watcher::Config::default())
        .owns(kafkausers, watcher::Config::default())
        .watches(kafkas, watcher::Config::default(), |kafka| {
            // Re-reconcile every gateway in the parent's namespace when the
            // parent Kafka changes (a version-validity flip must propagate).
            // The mapper yields object refs; we map the Kafka to nothing
            // specific (gateways are linked by label, not owner-ref), so rely
            // on the periodic requeue + owned-object events for convergence.
            let _ = kafka;
            std::iter::empty::<kube::runtime::reflector::ObjectRef<KafkaGrpcGateway>>()
        })
        .run(reconcile, error_policy, Arc::new(ctx))
        .for_each(|res| async move {
            match res {
                Ok((obj, _)) => tracing::debug!(?obj, "gateway reconciled"),
                Err(e) => tracing::warn!(error = %e, "gateway reconcile error"),
            }
        })
        .await;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use crabka_units::{millis, secs};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;

    use super::*;
    use crate::crd::grpc_gateway::{
        AllowedTargetSpec, DedupSpec, GatewayAuthzSpec, GatewayBearerSpec, GatewayHealthChecks,
        GatewaySchemaRegistrySpec, GatewayTlsSpec, GatewayTuning, InboundWebhookSpec,
        KafkaGrpcGatewaySpec, OutboundSubscriptionSpec, SecretKeyRef, TelemetrySpec,
    };

    fn empty_spec() -> KafkaGrpcGatewaySpec {
        KafkaGrpcGatewaySpec {
            replicas: None,
            image: None,
            resources: None,
            dedup: None,
            membership_topic: None,
            tuning: None,
            schema_registry: None,
            health_checks: None,
            tls: None,
            authz: None,
            webhooks: vec![],
            outbound_subscriptions: vec![],
            allowed_targets: vec![],
            telemetry: None,
        }
    }

    fn gateway_fixture(name: &str, parent: &str) -> KafkaGrpcGateway {
        let mut gw = KafkaGrpcGateway::new(name, empty_spec());
        gw.metadata.namespace = Some("default".into());
        gw.metadata.uid = Some("gw-uid".into());
        let mut labels = BTreeMap::new();
        labels.insert("crabka.io/cluster".into(), parent.to_string());
        gw.metadata.labels = Some(labels);
        gw
    }

    #[test]
    fn deployment_name_and_owner_ref() {
        let gw = gateway_fixture("gw", "demo");
        let dep = deployment(
            &gw,
            "demo",
            "img:1",
            "demo-broker-headless.default.svc.cluster.local:9092",
            "demo-broker-headless.default.svc.cluster.local",
        )
        .unwrap();
        assert!(dep.metadata.name.as_deref() == Some("gw"));
        let owner = &dep.metadata.owner_references.as_ref().unwrap()[0];
        let expected = OwnerReference {
            api_version: "crabka.io/v1alpha1".into(),
            block_owner_deletion: Some(true),
            controller: Some(true),
            kind: "KafkaGrpcGateway".into(),
            name: "gw".into(),
            uid: "gw-uid".into(),
        };
        assert!(*owner == expected);
    }

    #[test]
    fn deployment_has_five_volume_mounts() {
        let gw = gateway_fixture("gw", "demo");
        let dep = deployment(&gw, "demo", "img:1", "boot:9092", "sni").unwrap();
        let pod = dep.spec.unwrap().template.spec.unwrap();
        let container = &pod.containers[0];
        let mounts = container.volume_mounts.as_ref().expect("volume mounts");
        let names: std::collections::BTreeSet<&str> =
            mounts.iter().map(|m| m.name.as_str()).collect();
        for want in [
            "serving",
            "broker-client",
            "cluster-ca",
            "clients-ca",
            "config",
        ] {
            assert!(names.contains(want), "missing mount {want}; got {names:?}");
        }
        assert!(
            mounts.len() == 5,
            "expected exactly 5 mounts, got {}",
            mounts.len()
        );

        // The five backing Secret volumes must exist too.
        let vols = pod.volumes.as_ref().expect("volumes");
        let secret_names: std::collections::BTreeSet<String> = vols
            .iter()
            .filter_map(|v| v.secret.as_ref().and_then(|s| s.secret_name.clone()))
            .collect();
        for want in [
            "gw-serving",
            "gw-broker",
            "demo-cluster-ca-cert",
            "demo-clients-ca-cert",
            "gw-config",
        ] {
            assert!(
                secret_names.contains(want),
                "missing backing Secret volume {want}; got {secret_names:?}"
            );
        }
    }

    #[test]
    fn deployment_has_broker_tls_args_pointing_at_mounts() {
        let gw = gateway_fixture("gw", "demo");
        let dep = deployment(
            &gw,
            "demo",
            "img:1",
            "demo-broker-headless.default.svc.cluster.local:9092",
            "demo-broker-headless.default.svc.cluster.local",
        )
        .unwrap();
        let pod = dep.spec.unwrap().template.spec.unwrap();
        let args = pod.containers[0].args.as_ref().expect("args");
        let joined = args.join(" ");
        for want in [
            "--broker-tls-cert=/etc/crabka-gw/broker-client/user.crt",
            "--broker-tls-key=/etc/crabka-gw/broker-client/user.key",
            "--broker-tls-ca=/etc/crabka-gw/cluster-ca/ca.crt",
            "--broker-tls-server-name=demo-broker-headless.default.svc.cluster.local",
            // Serving-side flags reference the right CA bundles.
            "--tls-client-ca=/etc/crabka-gw/clients-ca/ca.crt",
            "--tls-trust-roots=/etc/crabka-gw/cluster-ca/ca.crt",
            "--tls-cert=/etc/crabka-gw/serving/tls.crt",
            "--bootstrap-servers=demo-broker-headless",
        ] {
            assert!(joined.contains(want), "missing {want}; args: {joined}");
        }
    }

    #[test]
    fn deployment_advertised_addr_uses_pod_ip_field_ref() {
        let gw = gateway_fixture("gw", "demo");
        let dep = deployment(&gw, "demo", "img:1", "boot:9092", "sni").unwrap();
        let pod = dep.spec.unwrap().template.spec.unwrap();
        let container = &pod.containers[0];

        // The advertised-addr arg uses the `$(POD_IP)` shell-expansion form.
        let args = container.args.as_ref().expect("args");
        assert!(
            args.iter().any(|a| a == "--advertised-addr=$(POD_IP):9500"),
            "args: {args:?}"
        );

        // POD_IP is sourced from the downward API `status.podIP`.
        let env = container.env.as_ref().expect("env");
        let pod_ip = env.iter().find(|e| e.name == "POD_IP").expect("POD_IP env");
        let fr = pod_ip
            .value_from
            .as_ref()
            .and_then(|v| v.field_ref.as_ref())
            .expect("POD_IP fieldRef");
        assert!(fr.field_path == "status.podIP");

        // client-id is the pod name.
        let client_id = env
            .iter()
            .find(|e| e.name == "CRABKA_GATEWAY_CLIENT_ID")
            .expect("client id env");
        assert!(client_id.value.as_deref() == Some("$(POD_NAME)"));
    }

    #[test]
    fn deployment_default_replicas_is_one() {
        let gw = gateway_fixture("gw", "demo");
        let dep = deployment(&gw, "demo", "img:1", "boot:9092", "sni").unwrap();
        assert!(dep.spec.unwrap().replicas == Some(1));
    }

    #[test]
    fn deployment_honors_explicit_replicas() {
        let mut gw = gateway_fixture("gw", "demo");
        gw.spec.replicas = Some(3);
        let dep = deployment(&gw, "demo", "img:1", "boot:9092", "sni").unwrap();
        assert!(dep.spec.unwrap().replicas == Some(3));
    }

    #[test]
    fn deployment_probes_on_gateway_port() {
        let gw = gateway_fixture("gw", "demo");
        let dep = deployment(&gw, "demo", "img:1", "boot:9092", "sni").unwrap();
        let container = dep
            .spec
            .unwrap()
            .template
            .spec
            .unwrap()
            .containers
            .remove(0);
        let readiness = container.readiness_probe.expect("readiness probe");
        let get = readiness.http_get.expect("httpGet readiness");
        assert!(get.path.as_deref() == Some("/readyz"));
        let liveness = container.liveness_probe.expect("liveness probe");
        let get = liveness.http_get.expect("httpGet liveness");
        assert!(get.path.as_deref() == Some("/healthz"));
        // containerPort 9500.
        let ports = container.ports.expect("ports");
        assert!(ports.iter().any(|p| p.container_port == 9500));
    }

    #[test]
    fn deployment_probe_timing_uses_health_checks() {
        let mut gw = gateway_fixture("gw", "demo");
        gw.spec.health_checks = Some(GatewayHealthChecks {
            readiness_initial_delay_seconds: Some(3),
            readiness_period_seconds: Some(6),
            liveness_initial_delay_seconds: Some(11),
            liveness_period_seconds: Some(12),
        });
        let mut container = deployment(&gw, "demo", "img:1", "boot:9092", "sni")
            .unwrap()
            .spec
            .unwrap()
            .template
            .spec
            .unwrap()
            .containers
            .remove(0);
        let readiness = container.readiness_probe.take().unwrap();
        let liveness = container.liveness_probe.take().unwrap();
        assert!(readiness.initial_delay_seconds == Some(3));
        assert!(readiness.period_seconds == Some(6));
        assert!(liveness.initial_delay_seconds == Some(11));
        assert!(liveness.period_seconds == Some(12));
    }

    #[test]
    fn deployment_telemetry_env_present_when_configured() {
        let mut gw = gateway_fixture("gw", "demo");
        gw.spec.telemetry = Some(TelemetrySpec {
            otlp_endpoint: Some("http://otel:4317".into()),
            otlp_protocol: Some("http".into()),
            sample_ratio: Some(0.5),
        });
        let dep = deployment(&gw, "demo", "img:1", "boot:9092", "sni").unwrap();
        let env = dep
            .spec
            .unwrap()
            .template
            .spec
            .unwrap()
            .containers
            .remove(0)
            .env
            .unwrap();
        let by_name = |n: &str| {
            env.iter()
                .find(|e| e.name == n)
                .and_then(|e| e.value.clone())
        };
        for (name, want) in [
            ("CRABKA_OTLP_ENABLED", "true"),
            ("CRABKA_OTLP_ENDPOINT", "http://otel:4317"),
            ("CRABKA_OTLP_PROTOCOL", "http/protobuf"),
            ("CRABKA_OTLP_SAMPLE_RATIO", "0.5"),
        ] {
            assert!(by_name(name).as_deref() == Some(want), "env {name}");
        }
    }

    #[test]
    fn service_selector_matches_labels_and_port() {
        let gw = gateway_fixture("gw", "demo");
        let svc = service(&gw, "demo").unwrap();
        let spec = svc.spec.expect("svc spec");
        assert!(spec.type_.as_deref() == Some("ClusterIP"));
        let selector = spec.selector.expect("selector");
        let labels = gateway_labels("demo", "gw");
        assert!(
            selector == labels,
            "selector {selector:?} != labels {labels:?}"
        );
        let port = &spec.ports.expect("ports")[0];
        assert!(port.port == 9500);
        // Owner-ref present.
        assert!(svc.metadata.owner_references.as_ref().unwrap()[0].name == "gw");
    }

    #[test]
    fn child_kafkauser_is_tls_with_broad_acls() {
        let gw = gateway_fixture("gw", "demo");
        let user = child_kafkauser(&gw, "demo").unwrap();
        check!(user.metadata.name.as_deref() == Some("gw-broker"));
        // TLS auth.
        assert!(matches!(user.spec.authentication, Authentication::Tls(_)));
        // crabka.io/cluster label links it to the parent.
        check!(
            user.metadata
                .labels
                .as_ref()
                .and_then(|l| l.get("crabka.io/cluster"))
                .map(String::as_str)
                == Some("demo")
        );
        // Owner-ref → the gateway CR.
        check!(user.metadata.owner_references.as_ref().unwrap()[0].kind == "KafkaGrpcGateway");
        // Broad ALLOW ACLs over Topic/Group/TransactionalId/Cluster.
        let Some(Authorization::Simple(authz)) = &user.spec.authorization else {
            panic!("expected simple authorization");
        };
        let kinds: Vec<AclResourceKind> = authz.acls.iter().map(|a| a.resource.kind).collect();
        for want in [
            AclResourceKind::Topic,
            AclResourceKind::Group,
            AclResourceKind::TransactionalId,
            AclResourceKind::Cluster,
        ] {
            assert!(kinds.contains(&want), "missing ACL kind {want:?}");
        }
        for rule in &authz.acls {
            assert!(rule.permission == AclPermission::Allow);
            assert!(rule.operations.contains(&AclOp::All));
        }
    }

    #[test]
    fn config_secret_renders_webhooks_and_outbound_toml() {
        let mut gw = gateway_fixture("gw", "demo");
        gw.spec.webhooks = vec![InboundWebhookSpec {
            name: "orders".into(),
            target_topic: "raw-orders".into(),
            principal: Some("User:webhook".into()),
            signature_header: Some("X-Hub-Signature-256".into()),
            signature_encoding: None,
            signature_prefix: Some("sha256=".into()),
            timestamp_header: None,
            timestamp_tolerance: None,
            idempotency_source: Some("header:X-Idempotency-Key".into()),
            key_source: None,
            max_body: None,
            schema_subject: None,
            schema_format: None,
            secret_ref: Some(SecretKeyRef {
                name: "orders-secret".into(),
                key: "hmac".into(),
            }),
        }];
        gw.spec.outbound_subscriptions = vec![OutboundSubscriptionSpec {
            name: "processed".into(),
            source_topics: vec!["processed-orders".into()],
            target_url: "https://hooks.example.com/deliver".into(),
            dead_letter_topic: Some("dlq".into()),
            max_attempts: Some(5),
            base_backoff: None,
            max_backoff: None,
            request_timeout: None,
            group_id: None,
            decode_to_json: None,
            filter: Some("json:$.type".into()),
            headers: BTreeMap::from([("X-Tenant".to_string(), "acme".to_string())]),
            signing_secret_ref: Some(SecretKeyRef {
                name: "sign-secret".into(),
                key: "hmac".into(),
            }),
        }];

        let mut webhook_secrets = BTreeMap::new();
        webhook_secrets.insert("orders".to_string(), "WEBHOOK-HMAC".to_string());
        let mut outbound_secrets = BTreeMap::new();
        outbound_secrets.insert("processed".to_string(), "SIGN-HMAC".to_string());

        let secret = config_secret(&gw, &webhook_secrets, &outbound_secrets).unwrap();
        assert!(secret.metadata.name.as_deref() == Some("gw-config"));
        let data = secret.data.unwrap();

        let webhooks_toml = String::from_utf8(data["webhooks.toml"].0.clone()).unwrap();
        for want in [
            "name = \"orders\"",
            "target_topic = \"raw-orders\"",
            // The resolved HMAC secret was injected (never present in the CR).
            "secret = \"WEBHOOK-HMAC\"",
            "signature_prefix = \"sha256=\"",
        ] {
            assert!(
                webhooks_toml.contains(want),
                "missing {want} in {webhooks_toml}"
            );
        }

        let outbound_toml = String::from_utf8(data["outbound.toml"].0.clone()).unwrap();
        for want in [
            "name = \"processed\"",
            "target_url = \"https://hooks.example.com/deliver\"",
            "signing_secret = \"SIGN-HMAC\"",
            // allowed_targets derived from the target_url host.
            "hooks.example.com",
        ] {
            assert!(
                outbound_toml.contains(want),
                "missing {want} in {outbound_toml}"
            );
        }

        // The owner-ref points at the gateway.
        assert!(secret.metadata.owner_references.as_ref().unwrap()[0].name == "gw");
    }

    #[test]
    fn config_secret_round_trips_through_gateway_parsers_shape() {
        // The TOML we emit must parse as valid TOML with the expected table
        // structure. We assert structural keys (not a full gateway-crate
        // round-trip, which would require adding it as a dev-dependency).
        let mut gw = gateway_fixture("gw", "demo");
        gw.spec.outbound_subscriptions = vec![OutboundSubscriptionSpec {
            name: "s".into(),
            source_topics: vec!["t".into()],
            target_url: "https://h.example.com/x".into(),
            dead_letter_topic: None,
            max_attempts: None,
            base_backoff: None,
            max_backoff: None,
            request_timeout: None,
            group_id: None,
            decode_to_json: None,
            filter: None,
            headers: BTreeMap::new(),
            signing_secret_ref: None,
        }];
        let secret = config_secret(&gw, &BTreeMap::new(), &BTreeMap::new()).unwrap();
        let data = secret.data.unwrap();
        let outbound_toml = String::from_utf8(data["outbound.toml"].0.clone()).unwrap();
        let parsed: toml::Value = toml::from_str(&outbound_toml).expect("valid TOML");
        assert!(parsed.get("subscriptions").is_some());
        assert!(parsed.get("allowed_targets").is_some());
        let subs = parsed["subscriptions"].as_array().unwrap();
        assert!(subs[0]["name"].as_str() == Some("s"));
        let allowed = parsed["allowed_targets"].as_array().unwrap();
        assert!(allowed[0]["host"].as_str() == Some("h.example.com"));
        assert!(allowed[0]["scheme"].as_str() == Some("https"));
    }

    #[test]
    fn derive_allowed_targets_unions_explicit_and_subscription_hosts() {
        let mut gw = gateway_fixture("gw", "demo");
        gw.spec.outbound_subscriptions = vec![OutboundSubscriptionSpec {
            name: "s".into(),
            source_topics: vec!["t".into()],
            target_url: "https://a.example.com:8443/x".into(),
            dead_letter_topic: None,
            max_attempts: None,
            base_backoff: None,
            max_backoff: None,
            request_timeout: None,
            group_id: None,
            decode_to_json: None,
            filter: None,
            headers: BTreeMap::new(),
            signing_secret_ref: None,
        }];
        gw.spec.allowed_targets = vec![AllowedTargetSpec {
            scheme: "https".into(),
            host: "b.example.com".into(),
        }];
        let allowed = derive_allowed_targets(&gw);
        let hosts: std::collections::BTreeSet<String> = allowed
            .iter()
            .map(|v| v["host"].as_str().unwrap().to_string())
            .collect();
        // Port stripped from the subscription host.
        assert!(hosts.contains("a.example.com"), "{hosts:?}");
        assert!(hosts.contains("b.example.com"), "{hosts:?}");
    }

    #[test]
    fn version_gate_blocks_when_not_validated() {
        let mut parent = Kafka::new(
            "demo",
            crate::crd::KafkaSpec {
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
        parent.metadata.namespace = Some("default".into());
        // No status at all → blocked with WaitingForVersionValidation.
        let cond = version_gate(&parent).expect("blocked");
        assert!(cond.reason == "WaitingForVersionValidation");

        // KafkaVersionValid=True clears the gate.
        parent.status = Some(crate::crd::KafkaStatus {
            conditions: vec![condition("KafkaVersionValid", "True", "Valid", "ok")],
            ..Default::default()
        });
        assert!(version_gate(&parent).is_none());
    }

    #[test]
    fn deployment_authz_and_dedup_flags() {
        let mut gw = gateway_fixture("gw", "demo");
        gw.spec.authz = Some(GatewayAuthzSpec {
            mode: Some("simple".into()),
            super_users: vec!["User:admin".into(), "User:ops".into()],
            acl_refresh: Some(secs(42)),
            bearer: Some(GatewayBearerSpec {
                mode: Some("unsecured".into()),
                principal_claim: Some("email".into()),
                allowable_clock_skew: None,
            }),
        });
        gw.spec.dedup = Some(DedupSpec {
            topic: Some("gw-dedup".into()),
            partitions: Some(16),
            window: Some(millis(123)),
            txn_id_prefix: Some("pfx".into()),
            ownership_group: None,
        });
        gw.spec.tls = Some(GatewayTlsSpec {
            client_auth: Some("optional".into()),
            validity_days: Some(90),
            reload_interval: None,
        });
        let dep = deployment(&gw, "demo", "img:1", "boot:9092", "sni").unwrap();
        let args = dep
            .spec
            .unwrap()
            .template
            .spec
            .unwrap()
            .containers
            .remove(0)
            .args
            .unwrap();
        let joined = args.join(" ");
        for want in [
            "--authz=simple",
            "--authz-super-users=User:admin,User:ops",
            "--acl-refresh-interval=42s",
            "--bearer=unsecured",
            "--bearer-principal-claim=email",
            "--dedup-topic=gw-dedup",
            "--dedup-partitions=16",
            "--dedup-window=123ms",
            "--dedup-txn-id-prefix=pfx",
            "--tls-client-auth=optional",
        ] {
            assert!(joined.contains(want), "missing {want}; args: {joined}");
        }
    }

    #[test]
    fn omitted_runtime_fields_preserve_operator_defaults() {
        let gw = gateway_fixture("gw", "demo");
        let args = gateway_args(&gw, "gw", "boot:9092", "sni");
        for want in [
            "--dedup-window=1d",
            "--acl-refresh-interval=1m",
            "--dedup-ownership-group=gw-dedup-owners",
        ] {
            assert!(
                args.iter().any(|arg| arg == want),
                "missing {want}: {args:?}"
            );
        }
    }

    #[test]
    fn configured_client_policy_renders_once() {
        let mut gw = gateway_fixture("gw", "demo");
        gw.spec.tuning = Some(
            serde_json::from_value(serde_json::json!({
                "clientDispatchQueueCapacity": 7,
                "clientFrameMax": "32KiB"
            }))
            .unwrap(),
        );
        let args = gateway_args(&gw, "gw", "boot:9092", "sni");
        check!(
            args.iter()
                .filter(|arg| *arg == "--client-dispatch-queue-capacity=7")
                .count()
                == 1
        );
        check!(
            args.iter()
                .filter(|arg| *arg == "--client-frame-max=32768B")
                .count()
                == 1
        );

        let omitted = gateway_args(&gateway_fixture("gw", "demo"), "gw", "boot:9092", "sni");
        check!(
            omitted.iter().all(|arg| {
                !arg.starts_with("--client-dispatch-queue-capacity=")
                    && !arg.starts_with("--client-frame-max=")
            }),
            "got: {omitted:?}"
        );
    }

    #[test]
    fn client_policy_rejects_invalid_boundaries() {
        for (tuning, path) in [
            (
                serde_json::json!({"clientDispatchQueueCapacity": 0}),
                "spec.tuning.clientDispatchQueueCapacity",
            ),
            (
                serde_json::json!({"clientFrameMax": "0B"}),
                "spec.tuning.clientFrameMax",
            ),
            (
                serde_json::json!({"clientFrameMax": "1.5B"}),
                "spec.tuning.clientFrameMax",
            ),
            (
                serde_json::json!({"clientFrameMax": "101MiB"}),
                "spec.tuning.clientFrameMax",
            ),
        ] {
            let mut gw = gateway_fixture("gw", "demo");
            gw.spec.tuning = Some(serde_json::from_value(tuning).unwrap());
            let error = validate_config(&gw.spec).expect_err("reject invalid policy");
            assert!(error.contains(path), "got: {error}");
        }
    }

    #[test]
    fn runtime_values_render_to_flags_and_existing_toml_paths() {
        let mut gw = gateway_fixture("gw", "demo");
        gw.spec.membership_topic = Some("members-custom".into());
        gw.spec.tuning = Some(GatewayTuning {
            client_dispatch_queue_capacity: None,
            client_frame_max: None,
            internal_topic_replication_factor: Some(2),
            internal_topic_allow_replication_fallback: Some(false),
            internal_topic_create_timeout: Some(millis(7_001)),
            internal_topic_segment: Some(millis(22_001)),
            internal_topic_min_cleanable_dirty_ratio: Some(crabka_units::fraction(0.025)),
            consumer_poll_timeout: Some(millis(501)),
            ownership_warmup_empty_polls: Some(3),
            readiness_poll_interval: Some(millis(251)),
            produce_max_body: Some(crabka_units::bytes(3_145_728)),
            forward_max_body: Some(crabka_units::bytes(3_145_727)),
        });
        gw.spec.schema_registry = Some(GatewaySchemaRegistrySpec {
            url: Some("http://registry:8081".into()),
            latest_cache_ttl: Some(millis(5_001)),
            frame_raw: Some(true),
        });
        gw.spec.dedup = Some(DedupSpec {
            ownership_group: Some("owners-custom".into()),
            ..Default::default()
        });
        gw.spec.tls = Some(GatewayTlsSpec {
            reload_interval: Some(secs(31)),
            ..Default::default()
        });
        gw.spec.authz = Some(GatewayAuthzSpec {
            bearer: Some(GatewayBearerSpec {
                allowable_clock_skew: Some(millis(31_001)),
                ..Default::default()
            }),
            ..Default::default()
        });
        gw.spec.webhooks = vec![InboundWebhookSpec {
            name: "orders".into(),
            target_topic: "orders".into(),
            principal: None,
            signature_header: None,
            signature_encoding: None,
            signature_prefix: None,
            timestamp_header: None,
            timestamp_tolerance: None,
            idempotency_source: None,
            key_source: None,
            max_body: None,
            schema_subject: Some("orders-value".into()),
            schema_format: Some("avro".into()),
            secret_ref: None,
        }];
        gw.spec.outbound_subscriptions = vec![OutboundSubscriptionSpec {
            name: "deliver".into(),
            source_topics: vec!["orders".into()],
            target_url: "https://example.com/hook".into(),
            dead_letter_topic: None,
            max_attempts: None,
            base_backoff: None,
            max_backoff: None,
            request_timeout: None,
            group_id: Some("deliver-custom".into()),
            decode_to_json: Some(true),
            filter: None,
            headers: BTreeMap::new(),
            signing_secret_ref: None,
        }];

        let args = gateway_args(&gw, "gw", "boot:9092", "sni");
        for want in [
            "--membership-topic=members-custom",
            "--internal-topic-replication-factor=2",
            "--internal-topic-allow-replication-fallback=false",
            "--internal-topic-create-timeout=7.001s",
            "--internal-topic-segment=22.001s",
            "--internal-topic-min-cleanable-dirty-ratio=2.5%",
            "--consumer-poll-timeout=501ms",
            "--ownership-warmup-empty-polls=3",
            "--readiness-poll-interval=251ms",
            "--produce-max-body=3MiB",
            "--forward-max-body=3145727B",
            "--schema-registry-url=http://registry:8081",
            "--schema-registry-latest-cache-ttl=5.001s",
            "--schema-registry-frame-raw=true",
            "--dedup-ownership-group=owners-custom",
            "--tls-reload-interval=31s",
            "--bearer-allowable-clock-skew=31.001s",
        ] {
            assert!(
                args.iter().any(|arg| arg == want),
                "missing {want}: {args:?}"
            );
        }
        assert!(
            args.windows(2).all(|pair| pair[0] <= pair[1]),
            "args must remain sorted: {args:?}"
        );

        let secret = config_secret(&gw, &BTreeMap::new(), &BTreeMap::new()).unwrap();
        let data = secret.data.unwrap();
        let webhooks = String::from_utf8(data["webhooks.toml"].0.clone()).unwrap();
        assert!(webhooks.contains("schema_subject = \"orders-value\""));
        assert!(webhooks.contains("schema_format = \"avro\""));
        let outbound = String::from_utf8(data["outbound.toml"].0.clone()).unwrap();
        assert!(outbound.contains("group_id = \"deliver-custom\""));
        assert!(outbound.contains("decode_to_json = true"));
    }

    /// The gateway's own CLI parsers require an explicit unit and reject a bare
    /// number, so every dimensioned argument the operator emits has to read back
    /// as the exact quantity that went in.
    #[test]
    fn dimensioned_args_round_trip_through_the_unit_parsers() {
        let mut gw = gateway_fixture("gw", "demo");
        gw.spec.dedup = Some(crate::crd::grpc_gateway::DedupSpec {
            window: Some(hours(1)),
            ..Default::default()
        });
        gw.spec.tuning = Some(crate::crd::grpc_gateway::GatewayTuning {
            internal_topic_create_timeout: Some(secs(10)),
            internal_topic_segment: Some(minutes(1)),
            internal_topic_min_cleanable_dirty_ratio: Some(crabka_units::percent(1)),
            consumer_poll_timeout: Some(millis(500)),
            readiness_poll_interval: Some(millis(250)),
            produce_max_body: Some(crabka_units::mebibytes(2)),
            forward_max_body: Some(crabka_units::mebibytes(2)),
            ..Default::default()
        });

        let args = gateway_args(&gw, "gw", "boot:9092", "sni");
        let value_of = |flag: &str| -> String {
            args.iter()
                .find_map(|arg| arg.strip_prefix(&format!("{flag}=")))
                .unwrap_or_else(|| panic!("missing {flag} in {args:?}"))
                .to_string()
        };

        for (flag, want) in [
            ("--dedup-window", hours(1)),
            ("--internal-topic-create-timeout", secs(10)),
            ("--internal-topic-segment", minutes(1)),
            ("--consumer-poll-timeout", millis(500)),
            ("--readiness-poll-interval", millis(250)),
        ] {
            let raw = value_of(flag);
            check!(
                crabka_units::parse::time(&raw) == Ok(want),
                "case {flag} = {raw}"
            );
        }
        for (flag, want) in [
            ("--produce-max-body", crabka_units::mebibytes(2)),
            ("--forward-max-body", crabka_units::mebibytes(2)),
        ] {
            let raw = value_of(flag);
            check!(
                crabka_units::parse::byte_size(&raw) == Ok(want),
                "case {flag} = {raw}"
            );
        }
        let raw = value_of("--internal-topic-min-cleanable-dirty-ratio");
        check!(
            crabka_units::parse::ratio(&raw) == Ok(crabka_units::percent(1)),
            "case dirty ratio = {raw}"
        );
    }

    #[test]
    fn runtime_validation_rejects_scalar_domain_and_relation_errors() {
        let cases = [
            serde_json::json!({"replicas": 0}),
            serde_json::json!({"tuning": {"internalTopicReplicationFactor": 0}}),
            serde_json::json!({"tuning": {"internalTopicMinCleanableDirtyRatio": "100.01%"}}),
            serde_json::json!({"schemaRegistry": {"latestCacheTtl": "0s"}}),
            serde_json::json!({"tuning": {"produceMaxBody": "0B"}}),
            serde_json::json!({"tuning": {"forwardMaxBody": "0B"}}),
            serde_json::json!({"healthChecks": {"readinessInitialDelaySeconds": -1}}),
            serde_json::json!({"healthChecks": {"livenessPeriodSeconds": 0}}),
            serde_json::json!({"dedup": {"partitions": 0}}),
            serde_json::json!({"tls": {"clientAuth": "sometimes"}}),
            serde_json::json!({"authz": {"mode": "maybe"}}),
            serde_json::json!({"outboundSubscriptions": [{
                "name": "s", "sourceTopics": ["t"], "targetUrl": "https://example.com",
                "baseBackoff": "2ms", "maxBackoff": "1ms"
            }]}),
        ];
        for value in cases {
            let spec: KafkaGrpcGatewaySpec = serde_json::from_value(value.clone()).unwrap();
            assert!(validate_config(&spec).is_err(), "accepted invalid {value}");
        }
        let mut spec = empty_spec();
        spec.telemetry = Some(TelemetrySpec {
            sample_ratio: Some(f64::NAN),
            ..Default::default()
        });
        assert!(validate_config(&spec).is_err());
    }

    #[test]
    fn protocol_runtime_validation_rejects_lossy_lowering() {
        for value in [
            serde_json::json!({"dedup": {"window": "0.5ms"}}),
            serde_json::json!({"tuning": {"internalTopicSegment": "0.5ms"}}),
            serde_json::json!({"tuning": {"internalTopicCreateTimeout": "0.5ms"}}),
            serde_json::json!({
                "tuning": {"internalTopicCreateTimeout": "2147483648ms"}
            }),
            serde_json::json!({"dedup": {"window": "9223372036854775808ms"}}),
            serde_json::json!({
                "tuning": {"internalTopicSegment": "9223372036854775808ms"}
            }),
            serde_json::json!({"dedup": {"window": "9007199254740992.5ms"}}),
            serde_json::json!({
                "tuning": {"internalTopicSegment": "9007199254740992.5ms"}
            }),
            serde_json::json!({"dedup": {"window": "9007199254740993ms"}}),
            serde_json::json!({
                "tuning": {"internalTopicSegment": "9007199254740993ms"}
            }),
            serde_json::json!({"dedup": {"window": "9007199254740992ms"}}),
            serde_json::json!({
                "tuning": {"internalTopicSegment": "9007199254740992ms"}
            }),
        ] {
            let spec: KafkaGrpcGatewaySpec = serde_json::from_value(value.clone()).unwrap();
            assert!(validate_config(&spec).is_err(), "accepted invalid {value}");
        }

        for value in [
            serde_json::json!({}),
            serde_json::json!({
                "dedup": {"window": "9007199254740991ms"},
                "tuning": {
                    "internalTopicSegment": "9007199254740991ms",
                    "internalTopicCreateTimeout": "2147483647ms"
                }
            }),
        ] {
            let spec: KafkaGrpcGatewaySpec = serde_json::from_value(value.clone()).unwrap();
            assert!(validate_config(&spec).is_ok(), "rejected valid {value}");
        }

        let ambiguous: KafkaGrpcGatewaySpec = serde_json::from_value(serde_json::json!({
            "dedup": {"window": "9007199254740992ms"}
        }))
        .unwrap();
        assert!(
            validate_config(&ambiguous)
                .expect_err("ambiguous UOM quantity must be rejected")
                .contains("below 9007199254740992ms because UOM quantities use f64")
        );
    }

    // ── resolve_broker_endpoint ───────────────────────────────

    use crate::crd::{
        KafkaSpec, KafkaStatus, Listener, ListenerAuthentication, ListenerStatus, ListenerType,
    };

    /// Build a parent `Kafka` named `demo` in `default` with the given
    /// `spec.listeners` + `status.listeners`.
    fn parent_with_listeners(
        spec_listeners: Vec<Listener>,
        status_listeners: Vec<ListenerStatus>,
    ) -> Kafka {
        let mut parent = Kafka::new(
            "demo",
            KafkaSpec {
                kafka_version: "0.1.1".into(),
                metadata_version: None,
                config: None,
                listeners: spec_listeners,
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
        parent.metadata.namespace = Some("default".into());
        parent.status = Some(KafkaStatus {
            listeners: status_listeners,
            ..Default::default()
        });
        parent
    }

    fn tls_mtls_listener(name: &str, port: i32) -> Listener {
        Listener {
            name: name.into(),
            port,
            type_: ListenerType::Internal,
            tls: true,
            authentication: Some(ListenerAuthentication::Tls),
            configuration: None,
            network_policy_peers: None,
        }
    }

    fn plain_listener(name: &str, port: i32) -> Listener {
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

    fn internal_status(name: &str, port: i32) -> ListenerStatus {
        ListenerStatus {
            name: name.into(),
            type_: ListenerType::Internal,
            bootstrap_servers: format!("demo-broker-headless.default.svc.cluster.local:{port}"),
            addresses: vec![],
        }
    }

    #[test]
    fn resolve_broker_endpoint_picks_tls_mtls_internal_listener() {
        // PLAIN/9092 (plaintext) + secured/9093 (tls + mtls). Must pick the
        // secured one's bootstrap, NOT the plaintext :9092.
        let parent = parent_with_listeners(
            vec![
                plain_listener("PLAIN", 9092),
                tls_mtls_listener("secured", 9093),
            ],
            vec![
                internal_status("PLAIN", 9092),
                internal_status("secured", 9093),
            ],
        );
        let (bootstrap, sni) = resolve_broker_endpoint(&parent, "default").expect("resolved");
        assert!(
            bootstrap == "demo-broker-headless.default.svc.cluster.local:9093",
            "must resolve the secured listener bootstrap, got {bootstrap}"
        );
        assert!(
            sni == "demo-broker-headless.default.svc.cluster.local",
            "SNI must be the headless-svc SAN, got {sni}"
        );
    }

    #[test]
    fn resolve_broker_endpoint_none_without_tls_listener() {
        // Only a plaintext PLAIN listener → no eligible endpoint.
        let parent = parent_with_listeners(
            vec![plain_listener("PLAIN", 9092)],
            vec![internal_status("PLAIN", 9092)],
        );
        assert!(resolve_broker_endpoint(&parent, "default").is_none());
    }

    #[test]
    fn resolve_broker_endpoint_none_when_tls_listener_has_no_auth() {
        // A TLS listener with no `authentication` is anonymous-over-TLS, not
        // mTLS — the gateway's client cert wouldn't authenticate it, so it is
        // not eligible.
        let anon_tls = Listener {
            authentication: None,
            ..tls_mtls_listener("anon", 9093)
        };
        let parent = parent_with_listeners(vec![anon_tls], vec![internal_status("anon", 9093)]);
        assert!(resolve_broker_endpoint(&parent, "default").is_none());
    }

    #[test]
    fn resolve_broker_endpoint_none_when_bootstrap_not_in_status() {
        // Eligible spec listener, but its bootstrap hasn't resolved into
        // status.listeners yet (ListenersReady not reached) → None.
        let parent = parent_with_listeners(vec![tls_mtls_listener("secured", 9093)], vec![]);
        assert!(resolve_broker_endpoint(&parent, "default").is_none());
    }
}
