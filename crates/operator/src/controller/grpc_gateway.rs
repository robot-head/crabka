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

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use crabka_security::ca::{SubjectAltName, issue_broker_cert};
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
            self, ReconcileError, apply_object, condition, owner_ref, parent_version_gate,
            patch_status, read_pem_key,
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
#[allow(clippy::too_many_lines)] // linear render pipeline: env + flags + mounts are independent segments
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

    // Mounted-file paths the gateway CLI flags reference.
    let serving_cert = format!("{SERVING_DIR}/tls.crt");
    let serving_key = format!("{SERVING_DIR}/tls.key");
    let broker_cert = format!("{BROKER_CLIENT_DIR}/user.crt");
    let broker_key = format!("{BROKER_CLIENT_DIR}/user.key");
    let cluster_ca_crt = format!("{CLUSTER_CA_DIR}/ca.crt");
    let clients_ca_crt = format!("{CLIENTS_CA_DIR}/ca.crt");
    let webhooks_toml = format!("{CONFIG_DIR}/webhooks.toml");
    let outbound_toml = format!("{CONFIG_DIR}/outbound.toml");

    // CLI args. The advertised address is `$(POD_IP):9500` via the downward
    // API (the shell `$(VAR)` form is expanded by the container runtime from
    // the env vars declared below).
    let advertised_addr = format!("$(POD_IP):{GATEWAY_PORT}");
    let client_auth = gw
        .spec
        .tls
        .as_ref()
        .and_then(|t| t.client_auth.clone())
        .unwrap_or_else(|| "required".into());
    let authz_mode = gw
        .spec
        .authz
        .as_ref()
        .and_then(|a| a.mode.clone())
        .unwrap_or_else(|| "simple".into());
    let super_users = gw
        .spec
        .authz
        .as_ref()
        .map(|a| a.super_users.join(","))
        .unwrap_or_default();
    let acl_refresh = gw
        .spec
        .authz
        .as_ref()
        .and_then(|a| a.acl_refresh_secs)
        .unwrap_or(60);
    let bearer_mode = gw
        .spec
        .authz
        .as_ref()
        .and_then(|a| a.bearer.as_ref())
        .and_then(|b| b.mode.clone())
        .unwrap_or_else(|| "off".into());
    let bearer_claim = gw
        .spec
        .authz
        .as_ref()
        .and_then(|a| a.bearer.as_ref())
        .and_then(|b| b.principal_claim.clone())
        .unwrap_or_else(|| "sub".into());

    let mut args = vec![
        format!("--bootstrap-servers={bootstrap}"),
        format!("--listen-addr=0.0.0.0:{GATEWAY_PORT}"),
        format!("--advertised-addr={advertised_addr}"),
        // dedup
        format!(
            "--dedup-topic={}",
            gw.spec
                .dedup
                .as_ref()
                .and_then(|d| d.topic.clone())
                .unwrap_or_else(|| format!("{gw_name}-dedup"))
        ),
        format!(
            "--dedup-partitions={}",
            gw.spec
                .dedup
                .as_ref()
                .and_then(|d| d.partitions)
                .unwrap_or(8)
        ),
        format!(
            "--dedup-window-ms={}",
            gw.spec
                .dedup
                .as_ref()
                .and_then(|d| d.window_ms)
                .unwrap_or(86_400_000)
        ),
        format!(
            "--dedup-txn-id-prefix={}",
            gw.spec
                .dedup
                .as_ref()
                .and_then(|d| d.txn_id_prefix.clone())
                .unwrap_or_else(|| gw_name.clone())
        ),
        // serving TLS (cluster-CA-signed cert; clients-CA verifies inbound mTLS;
        // cluster-CA trust roots verify peer-gateway serving certs)
        format!("--tls-cert={serving_cert}"),
        format!("--tls-key={serving_key}"),
        format!("--tls-client-ca={clients_ca_crt}"),
        format!("--tls-trust-roots={cluster_ca_crt}"),
        format!("--tls-client-auth={client_auth}"),
        // broker mTLS (clients-CA-signed client cert from the child KafkaUser;
        // cluster-CA verifies the broker's serving cert; SNI matches the
        // broker serving-cert SAN = headless-svc DNS)
        format!("--broker-tls-cert={broker_cert}"),
        format!("--broker-tls-key={broker_key}"),
        format!("--broker-tls-ca={cluster_ca_crt}"),
        format!("--broker-tls-server-name={broker_sni}"),
        // authz
        format!("--authz={authz_mode}"),
        format!("--authz-super-users={super_users}"),
        format!("--acl-refresh-secs={acl_refresh}"),
        // bearer
        format!("--bearer={bearer_mode}"),
        format!("--bearer-principal-claim={bearer_claim}"),
        // config files
        format!("--webhooks-config={webhooks_toml}"),
        format!("--outbound-webhooks-config={outbound_toml}"),
    ];
    // Sort the flags so the rendered pod template is byte-stable across
    // reconciles (clap flags are order-independent).
    args.sort_unstable();

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
            "initialDelaySeconds": 2,
            "periodSeconds": 5
        },
        "livenessProbe": {
            "httpGet": { "path": "/healthz", "port": GATEWAY_PORT },
            "initialDelaySeconds": 10,
            "periodSeconds": 10
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
            if let Some(v) = w.timestamp_tolerance_secs {
                e.insert("timestamp_tolerance_secs".into(), json!(v));
            }
            if let Some(v) = &w.idempotency_source {
                e.insert("idempotency_source".into(), json!(v));
            }
            if let Some(v) = &w.key_source {
                e.insert("key_source".into(), json!(v));
            }
            if let Some(v) = w.max_body_bytes {
                e.insert("max_body_bytes".into(), json!(v));
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
            if let Some(v) = s.base_backoff_ms {
                e.insert("base_backoff_ms".into(), json!(v));
            }
            if let Some(v) = s.max_backoff_ms {
                e.insert("max_backoff_ms".into(), json!(v));
            }
            if let Some(v) = s.request_timeout_ms {
                e.insert("request_timeout_ms".into(), json!(v));
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
        if let Ok(url) = reqwest_url_parse(&s.target_url) {
            push(url.0, url.1);
        }
    }
    for a in &gw.spec.allowed_targets {
        push(a.scheme.clone(), a.host.clone());
    }
    out.into_iter()
        .map(|(scheme, host)| json!({ "scheme": scheme, "host": host }))
        .collect()
}

/// Minimal `scheme`/`host` extraction from a target URL, without pulling in the
/// `reqwest`/`url` crate (not an operator dependency). Returns
/// `(scheme, host)`; the host excludes any port (the gateway's SSRF check
/// matches host only).
fn reqwest_url_parse(target: &str) -> Result<(String, String), ()> {
    let (scheme, rest) = target.split_once("://").ok_or(())?;
    if scheme.is_empty() {
        return Err(());
    }
    // Strip path/query, then any userinfo, then any port.
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    let host_port = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    let host = host_port.rsplit_once(':').map_or(host_port, |(h, _)| h);
    if host.is_empty() {
        return Err(());
    }
    Ok((scheme.to_ascii_lowercase(), host.to_string()))
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
        && !renew_if_expiring(&cert_pem, 30, now).unwrap_or(true)
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
/// counter/histogram, then delegates to [`reconcile_inner`].
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

#[allow(clippy::too_many_lines)] // linear 7-step controller flow; each step is independent
async fn reconcile_inner(
    gw: Arc<KafkaGrpcGateway>,
    ctx: Arc<Context>,
) -> Result<Action, ReconcileError> {
    let ns = gw.namespace().unwrap_or_else(|| "default".into());
    let name = gw.name_any();
    let observed_generation = gw.meta().generation;

    let gw_api: Api<KafkaGrpcGateway> = Api::namespaced(ctx.client.clone(), &ns);
    let secret_api: Api<Secret> = Api::namespaced(ctx.client.clone(), &ns);

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
        return Ok(Action::requeue(Duration::from_secs(30)));
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
        return Ok(Action::requeue(Duration::from_secs(30)));
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
        return Ok(Action::requeue(Duration::from_secs(30)));
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
        return Ok(Action::requeue(Duration::from_secs(15)));
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
        return Ok(Action::requeue(Duration::from_secs(30)));
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

    Ok(Action::requeue(Duration::from_secs(30)))
}

/// Requeue on transient error.
pub fn error_policy(
    _obj: Arc<KafkaGrpcGateway>,
    err: &ReconcileError,
    _ctx: Arc<Context>,
) -> Action {
    tracing::warn!(error = %err, "gateway reconcile error, requeueing");
    Action::requeue(Duration::from_secs(15))
}

/// Run the `KafkaGrpcGateway` controller forever. Owns the Deployment, Service,
/// the serving + config Secrets, and the child `KafkaUser`; watches the parent
/// `Kafka` so a version-validity flip re-triggers gateways.
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
    use assert2::assert;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;

    use super::*;
    use crate::crd::grpc_gateway::{
        AllowedTargetSpec, DedupSpec, GatewayAuthzSpec, GatewayBearerSpec, GatewayTlsSpec,
        InboundWebhookSpec, KafkaGrpcGatewaySpec, OutboundSubscriptionSpec, SecretKeyRef,
        TelemetrySpec,
    };

    fn empty_spec() -> KafkaGrpcGatewaySpec {
        KafkaGrpcGatewaySpec {
            replicas: None,
            image: None,
            resources: None,
            dedup: None,
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
        assert_eq!(dep.metadata.name, Some("gw".into()));
        assert_eq!(
            dep.metadata.owner_references.unwrap_or_default(),
            vec![OwnerReference {
                api_version: "crabka.io/v1alpha1".into(),
                block_owner_deletion: Some(true),
                controller: Some(true),
                kind: "KafkaGrpcGateway".into(),
                name: "gw".into(),
                uid: "gw-uid".into(),
            }]
        );
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
        assert_eq!(
            names,
            std::collections::BTreeSet::from([
                "broker-client",
                "clients-ca",
                "cluster-ca",
                "config",
                "serving",
            ])
        );

        // The five backing Secret volumes must exist too.
        let vols = pod.volumes.as_ref().expect("volumes");
        let secret_names: std::collections::BTreeSet<String> = vols
            .iter()
            .filter_map(|v| v.secret.as_ref().and_then(|s| s.secret_name.clone()))
            .collect();
        assert_eq!(
            secret_names,
            std::collections::BTreeSet::from([
                "demo-clients-ca-cert".to_string(),
                "demo-cluster-ca-cert".to_string(),
                "gw-broker".to_string(),
                "gw-config".to_string(),
                "gw-serving".to_string(),
            ])
        );
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

        // POD_IP is sourced from the downward API `status.podIP`.
        let env = container.env.as_ref().expect("env");
        let pod_ip = env.iter().find(|e| e.name == "POD_IP").expect("POD_IP env");
        let fr = pod_ip
            .value_from
            .as_ref()
            .and_then(|v| v.field_ref.as_ref())
            .expect("POD_IP fieldRef");

        // client-id is the pod name.
        let client_id = env
            .iter()
            .find(|e| e.name == "CRABKA_GATEWAY_CLIENT_ID")
            .expect("client id env");
        assert!(
            args.iter()
                .any(|arg| arg == "--advertised-addr=$(POD_IP):9500"),
            "args: {args:?}"
        );
        assert_eq!(fr.field_path.as_str(), "status.podIP", "args: {args:?}");
        assert_eq!(
            client_id.value.as_deref(),
            Some("$(POD_NAME)"),
            "args: {args:?}"
        );
    }

    #[test]
    fn deployment_uses_default_or_explicit_replicas() {
        for (name, configured, expected) in [
            ("default replica count", None, 1),
            ("explicit replica count", Some(3), 3),
        ] {
            let mut gw = gateway_fixture("gw", "demo");
            gw.spec.replicas = configured;
            let dep = deployment(&gw, "demo", "img:1", "boot:9092", "sni").unwrap();
            assert_eq!(dep.spec.unwrap().replicas, Some(expected), "case {name}");
        }
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
        let readiness_get = readiness.http_get.expect("httpGet readiness");
        let liveness = container.liveness_probe.expect("liveness probe");
        let liveness_get = liveness.http_get.expect("httpGet liveness");
        // containerPort 9500.
        let ports = container.ports.expect("ports");
        assert_eq!(readiness_get.path.as_deref(), Some("/readyz"));
        assert_eq!(liveness_get.path.as_deref(), Some("/healthz"));
        assert!(ports.iter().any(|p| p.container_port == 9500));
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
        let selector = spec.selector.expect("selector");
        let labels = gateway_labels("demo", "gw");
        let port = &spec.ports.expect("ports")[0];
        assert_eq!(spec.type_.as_deref(), Some("ClusterIP"));
        assert_eq!(selector, labels);
        assert_eq!(port.port, 9500);
        assert_eq!(
            svc.metadata.owner_references.as_ref().unwrap()[0]
                .name
                .as_str(),
            "gw"
        );
    }

    #[test]
    fn child_kafkauser_is_tls_with_broad_acls() {
        let gw = gateway_fixture("gw", "demo");
        let user = child_kafkauser(&gw, "demo").unwrap();
        let expected_labels = BTreeMap::from([
            ("crabka.io/cluster".to_string(), "demo".to_string()),
            ("crabka.io/gateway".to_string(), "gw".to_string()),
        ]);
        assert_eq!(user.metadata.name, Some("gw-broker".into()));
        assert_eq!(user.metadata.namespace, Some("default".into()));
        assert_eq!(user.metadata.labels, Some(expected_labels));
        assert_eq!(
            user.metadata.owner_references,
            Some(vec![OwnerReference {
                api_version: "crabka.io/v1alpha1".into(),
                block_owner_deletion: Some(true),
                controller: Some(true),
                kind: "KafkaGrpcGateway".into(),
                name: "gw".into(),
                uid: "gw-uid".into(),
            }])
        );
        assert_eq!(
            user.spec,
            KafkaUserSpec {
                authentication: Authentication::Tls(TlsAuth::default()),
                authorization: Some(Authorization::Simple(SimpleAuthorization {
                    acls: vec![
                        broad_acl(AclResourceKind::Topic, "*"),
                        broad_acl(AclResourceKind::Group, "*"),
                        broad_acl(AclResourceKind::TransactionalId, "*"),
                        broad_acl(AclResourceKind::Cluster, "kafka-cluster"),
                    ],
                })),
                quotas: None,
            }
        );
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
            timestamp_tolerance_secs: None,
            idempotency_source: Some("header:X-Idempotency-Key".into()),
            key_source: None,
            max_body_bytes: None,
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
            base_backoff_ms: None,
            max_backoff_ms: None,
            request_timeout_ms: None,
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
        assert_eq!(secret.metadata.name.as_deref(), Some("gw-config"));
        assert_eq!(
            secret.metadata.owner_references.as_deref(),
            Some(
                [OwnerReference {
                    api_version: "crabka.io/v1alpha1".into(),
                    block_owner_deletion: Some(true),
                    controller: Some(true),
                    kind: "KafkaGrpcGateway".into(),
                    name: "gw".into(),
                    uid: "gw-uid".into(),
                }]
                .as_slice()
            )
        );
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
            base_backoff_ms: None,
            max_backoff_ms: None,
            request_timeout_ms: None,
            filter: None,
            headers: BTreeMap::new(),
            signing_secret_ref: None,
        }];
        let secret = config_secret(&gw, &BTreeMap::new(), &BTreeMap::new()).unwrap();
        let data = secret.data.unwrap();
        let outbound_toml = String::from_utf8(data["outbound.toml"].0.clone()).unwrap();
        let parsed: toml::Value = toml::from_str(&outbound_toml).expect("valid TOML");
        let subs = parsed["subscriptions"].as_array().unwrap();
        let allowed = parsed["allowed_targets"].as_array().unwrap();
        assert_eq!(subs[0]["name"].as_str(), Some("s"));
        assert_eq!(allowed[0]["host"].as_str(), Some("h.example.com"));
        assert_eq!(allowed[0]["scheme"].as_str(), Some("https"));
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
            base_backoff_ms: None,
            max_backoff_ms: None,
            request_timeout_ms: None,
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
        assert_eq!(
            hosts,
            std::collections::BTreeSet::from([
                "a.example.com".to_string(),
                "b.example.com".to_string(),
            ])
        );
    }

    #[test]
    fn reqwest_url_parse_strips_port_and_path() {
        for (input, scheme, host) in [
            (
                "https://h.example.com:8443/a/b?x=1",
                "https",
                "h.example.com",
            ),
            ("http://h.example.com", "http", "h.example.com"),
            ("https://user@h.example.com/x", "https", "h.example.com"),
        ] {
            assert!(
                reqwest_url_parse(input) == Ok((scheme.into(), host.into())),
                "case {input}"
            );
        }
        assert!(reqwest_url_parse("not-a-url").is_err());
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
            acl_refresh_secs: Some(42),
            bearer: Some(GatewayBearerSpec {
                mode: Some("unsecured".into()),
                principal_claim: Some("email".into()),
            }),
        });
        gw.spec.dedup = Some(DedupSpec {
            topic: Some("gw-dedup".into()),
            partitions: Some(16),
            window_ms: Some(123),
            txn_id_prefix: Some("pfx".into()),
        });
        gw.spec.tls = Some(GatewayTlsSpec {
            client_auth: Some("optional".into()),
            validity_days: Some(90),
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
            "--acl-refresh-secs=42",
            "--bearer=unsecured",
            "--bearer-principal-claim=email",
            "--dedup-topic=gw-dedup",
            "--dedup-partitions=16",
            "--dedup-window-ms=123",
            "--dedup-txn-id-prefix=pfx",
            "--tls-client-auth=optional",
        ] {
            assert!(joined.contains(want), "missing {want}; args: {joined}");
        }
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
        assert_eq!(
            bootstrap.as_str(),
            "demo-broker-headless.default.svc.cluster.local:9093"
        );
        assert_eq!(
            sni.as_str(),
            "demo-broker-headless.default.svc.cluster.local"
        );
    }

    #[test]
    fn resolve_broker_endpoint_ineligible_cases() {
        let without_tls = parent_with_listeners(
            vec![plain_listener("PLAIN", 9092)],
            vec![internal_status("PLAIN", 9092)],
        );
        let anon_tls = Listener {
            authentication: None,
            ..tls_mtls_listener("anon", 9093)
        };
        let anonymous_tls =
            parent_with_listeners(vec![anon_tls], vec![internal_status("anon", 9093)]);
        let unresolved_bootstrap =
            parent_with_listeners(vec![tls_mtls_listener("secured", 9093)], vec![]);
        for (name, parent) in [
            ("no TLS listener", without_tls),
            ("anonymous TLS listener", anonymous_tls),
            ("bootstrap absent from status", unresolved_bootstrap),
        ] {
            assert_eq!(
                resolve_broker_endpoint(&parent, "default"),
                None,
                "case {name}"
            );
        }
    }
}
