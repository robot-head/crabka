//! `Gres` fleet reconciler. Renders the `PgDog` front door from live tenants.

use std::{collections::BTreeMap, fmt::Write as _, sync::Arc, time::Duration};

use crabka_gres_control::{
    PgdogGeneral, PgdogRenderInput, PgdogUser, TenantEndpoint, TenantName, TenantState,
    render_pgdog_toml, render_users_toml,
};
use futures::StreamExt as _;
use k8s_openapi::{
    ByteString,
    api::{
        apps::v1::Deployment,
        core::v1::{Pod, Secret, Service},
    },
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
use sha2::{Digest as _, Sha256};

use crate::{
    context::{Context, PgdogExpectedRoute, PgdogReloadRequest},
    controller::common::{self, FIELD_MANAGER, ReconcileError, apply_object, condition, owner_ref},
    crd::{Gres, GresTenant},
};

const APP_NAME: &str = "crabka-pgdog";
const DEFAULT_IMAGE: &str = "ghcr.io/pgdogdev/pgdog:0.1.47";
const ACTIVATOR_APP_NAME: &str = "crabka-gres-activator";
const DEFAULT_ACTIVATOR_IMAGE: &str = concat!(
    "ghcr.io/robot-head/crabka-gres-activator:",
    env!("CARGO_PKG_VERSION")
);
const ACTIVATOR_PORT: i32 = 6543;
const ACTIVATOR_PORT_U16: u16 = 6543;
const RELOAD_RETRY_LIMIT: usize = 3;
const RELOAD_REQUEUE: Duration = Duration::from_secs(15);

/// Run the controller forever.
pub async fn run(ctx: Context) -> anyhow::Result<()> {
    let gres_api: Api<Gres> = Api::all(ctx.client.clone());
    let tenant_api: Api<GresTenant> = Api::all(ctx.client.clone());
    Controller::new(gres_api, watcher::Config::default())
        .watches(tenant_api, watcher::Config::default(), |tenant| {
            tenant_to_gres_refs(&tenant).into_iter()
        })
        .run(reconcile, error_policy, Arc::new(ctx))
        .for_each(|res| async move {
            match res {
                Ok((obj, _)) => tracing::debug!(?obj, "gres fleet reconciled"),
                Err(err) => tracing::warn!(error = %err, "gres fleet reconcile error"),
            }
        })
        .await;
    Ok(())
}

pub fn error_policy(_obj: Arc<Gres>, err: &ReconcileError, _ctx: Arc<Context>) -> Action {
    tracing::warn!(error = %err, "gres fleet reconcile error, requeueing");
    Action::requeue(Duration::from_secs(15))
}

#[tracing::instrument(level = "info", skip_all, fields(kind = "Gres", name = %obj.name_any()))]
pub async fn reconcile(obj: Arc<Gres>, ctx: Arc<Context>) -> Result<Action, ReconcileError> {
    common::record_reconcile(&ctx, "Gres", Box::pin(reconcile_inner(obj, ctx.clone()))).await
}

async fn reconcile_inner(obj: Arc<Gres>, ctx: Arc<Context>) -> Result<Action, ReconcileError> {
    let ns = obj.namespace().unwrap_or_else(|| "default".into());
    let name = obj.name_any();
    let gres_api: Api<Gres> = Api::namespaced(ctx.client.clone(), &ns);
    let tenant_api: Api<GresTenant> = Api::namespaced(ctx.client.clone(), &ns);
    let tenants = tenant_api.list(&ListParams::default()).await?;
    let endpoints: Vec<_> = tenants
        .items
        .iter()
        .filter(|tenant| tenant.spec.gres == name)
        .filter_map(tenant_endpoint)
        .collect();
    let users = endpoints
        .iter()
        .filter_map(|endpoint| {
            tenants
                .items
                .iter()
                .find(|tenant| tenant.name_any() == endpoint.name)
                .map(|tenant| PgdogUser {
                    name: tenant.spec.user.clone(),
                    database: endpoint.name.clone(),
                    password: None,
                })
        })
        .collect();

    let render_input = PgdogRenderInput {
        tenants: &endpoints,
        activator: Some((activator_service_host(&name, &ns), ACTIVATOR_PORT_U16)),
        general: PgdogGeneral {
            listen_port: u16::try_from(obj.spec.pgdog.listen_port).unwrap_or(6_432),
            tls_cert_path: obj
                .spec
                .pgdog
                .tls_secret_ref
                .as_ref()
                .map(|_| "/etc/pgdog/tls/tls.crt".into()),
            tls_key_path: obj
                .spec
                .pgdog
                .tls_secret_ref
                .as_ref()
                .map(|_| "/etc/pgdog/tls/tls.key".into()),
            users,
            ..Default::default()
        },
    };
    let pgdog_toml = render_pgdog_toml(&render_input)?;
    let users_toml = render_users_toml(&render_input)?;
    let hash = config_hash(&pgdog_toml, &users_toml);

    let secret_api: Api<Secret> = Api::namespaced(ctx.client.clone(), &ns);
    apply_object(
        &secret_api,
        &config_secret_name(&name),
        &render_config_secret(&obj, &pgdog_toml, &users_toml, &hash)?,
    )
    .await?;
    let service_api: Api<Service> = Api::namespaced(ctx.client.clone(), &ns);
    apply_object(
        &service_api,
        &activator_service_name(&name),
        &render_activator_service(&obj)?,
    )
    .await?;
    apply_object(&service_api, &service_name(&name), &render_service(&obj)?).await?;
    let deployment_api: Api<Deployment> = Api::namespaced(ctx.client.clone(), &ns);
    let image = pgdog_image(&obj, &ctx);
    let activator_image = activator_image(&ctx);
    apply_object(
        &deployment_api,
        &activator_deployment_name(&name),
        &render_activator_deployment(&obj, &activator_image)?,
    )
    .await?;
    apply_object(
        &deployment_api,
        &deployment_name(&name),
        &render_deployment(&obj, &image, &hash)?,
    )
    .await?;

    if obj
        .status
        .as_ref()
        .and_then(|status| status.confirmed_pgdog_config_hash.as_deref())
        != Some(hash.as_str())
    {
        let verified = verify_pgdog_reload(&obj, &ctx, &endpoints).await?;
        if !verified {
            tracing::warn!(gres = %name, config_hash = %hash, "pgdog admin view is stale after reload attempts");
            return Ok(Action::requeue(RELOAD_REQUEUE));
        }
    }

    let balancer_status = balancer_status(&obj);
    patch_status(&gres_api, &name, &obj, &hash, &balancer_status).await?;
    Ok(Action::requeue(Duration::from_mins(1)))
}

#[must_use]
pub fn tenant_to_gres_refs(tenant: &GresTenant) -> Vec<ObjectRef<Gres>> {
    let mut reference = ObjectRef::new(&tenant.spec.gres);
    if let Some(namespace) = tenant.namespace() {
        reference = reference.within(&namespace);
    }
    vec![reference]
}

fn pgdog_image(obj: &Gres, ctx: &Context) -> String {
    obj.spec
        .pgdog
        .image
        .clone()
        .or_else(|| ctx.config.default_pgdog_image.clone())
        .unwrap_or_else(|| DEFAULT_IMAGE.to_string())
}

fn activator_image(ctx: &Context) -> String {
    ctx.config
        .default_gres_activator_image
        .clone()
        .unwrap_or_else(|| DEFAULT_ACTIVATOR_IMAGE.to_string())
}

async fn verify_pgdog_reload(
    obj: &Gres,
    ctx: &Context,
    endpoints: &[TenantEndpoint],
) -> Result<bool, ReconcileError> {
    let ns = obj.namespace().unwrap_or_else(|| "default".into());
    let password = pgdog_admin_password(obj, ctx, &ns).await?;
    let tls_ca_pem = pgdog_tls_ca(obj, ctx, &ns).await?;
    let port = u16::try_from(obj.spec.pgdog.listen_port).map_err(|err| {
        ReconcileError::Malformed(format!("PgDog listenPort is outside u16 range: {err}"))
    })?;
    let expected_routes = endpoints
        .iter()
        .map(|endpoint| {
            let (host, port) = if endpoint.state == TenantState::Active {
                (endpoint.backend_host.clone(), endpoint.backend_port)
            } else {
                (
                    activator_service_host(&obj.name_any(), &ns),
                    ACTIVATOR_PORT_U16,
                )
            };
            PgdogExpectedRoute {
                database: endpoint.name.clone(),
                host,
                port,
            }
        })
        .collect::<Vec<_>>();
    let service_host = format!("{}.{}.svc.cluster.local", service_name(&obj.name_any()), ns);
    let connect_addrs = if obj.spec.pgdog.replicas > 1 {
        let pods: Api<Pod> = Api::namespaced(ctx.client.clone(), &ns);
        let selector = selector_labels(obj)
            .into_iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join(",");
        let hosts = pods
            .list(&ListParams::default().labels(&selector))
            .await?
            .items
            .into_iter()
            .filter_map(|pod| {
                pod.status
                    .and_then(|status| status.pod_ip)
                    .and_then(|ip| ip.parse::<std::net::IpAddr>().ok())
                    .map(Some)
            })
            .collect::<Vec<_>>();
        if hosts.len() != obj.spec.pgdog.replicas as usize {
            return Ok(false);
        }
        hosts
    } else {
        vec![None]
    };
    let requests = connect_addrs
        .into_iter()
        .map(|connect_addr| PgdogReloadRequest {
            host: service_host.clone(),
            connect_addr,
            port,
            password: password.clone(),
            expected_routes: expected_routes.clone(),
            maintenance_mode: obj.spec.pgdog.replicas > 1,
            tls_ca_pem: tls_ca_pem.clone(),
        })
        .collect::<Vec<_>>();

    for attempt in 1..=RELOAD_RETRY_LIMIT {
        if ctx
            .pgdog_admin
            .reload_and_database_views_match(&requests)
            .await?
        {
            return Ok(true);
        }
        if attempt < RELOAD_RETRY_LIMIT {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    Ok(false)
}

async fn pgdog_tls_ca(
    obj: &Gres,
    ctx: &Context,
    namespace: &str,
) -> Result<Option<Vec<u8>>, ReconcileError> {
    let Some(secret_ref) = &obj.spec.pgdog.tls_secret_ref else {
        return Ok(None);
    };
    let secret_api: Api<Secret> = Api::namespaced(ctx.client.clone(), namespace);
    let secret = secret_api.get(&secret_ref.name).await?;
    let data = secret.data.ok_or_else(|| {
        ReconcileError::MalformedSecret(format!(
            "PgDog TLS Secret {:?} has no data",
            secret_ref.name
        ))
    })?;
    let certificate = data
        .get("ca.crt")
        .or_else(|| data.get("tls.crt"))
        .ok_or_else(|| {
            ReconcileError::MalformedSecret(format!(
                "PgDog TLS Secret {:?} must contain ca.crt or tls.crt",
                secret_ref.name
            ))
        })?;
    Ok(Some(certificate.0.clone()))
}

async fn pgdog_admin_password(
    obj: &Gres,
    ctx: &Context,
    namespace: &str,
) -> Result<String, ReconcileError> {
    let secret_api: Api<Secret> = Api::namespaced(ctx.client.clone(), namespace);
    let secret_name = &obj.spec.pgdog.admin_secret_ref.name;
    let secret = secret_api.get(secret_name).await?;
    let data = secret.data.ok_or_else(|| {
        ReconcileError::MalformedSecret(format!("PgDog admin Secret {secret_name:?} has no data"))
    })?;
    let key = &obj.spec.pgdog.admin_secret_ref.key;
    let value = data.get(key).ok_or_else(|| {
        ReconcileError::MalformedSecret(format!(
            "PgDog admin Secret {secret_name:?} missing key {key:?}"
        ))
    })?;
    String::from_utf8(value.0.clone()).map_err(|err| {
        ReconcileError::MalformedSecret(format!(
            "PgDog admin Secret {secret_name:?} key {key:?} is not UTF-8: {err}"
        ))
    })
}

#[must_use]
pub fn tenant_endpoint(tenant: &GresTenant) -> Option<TenantEndpoint> {
    if has_multiple_spec_ranges(tenant) || is_multi_range_unsupported(tenant) {
        return None;
    }

    let name = tenant.name_any();
    TenantName::try_from(name.as_str()).ok()?;
    let state = tenant_lifecycle_state(tenant);
    Some(TenantEndpoint {
        name: name.clone(),
        backend_host: format!(
            "{}-gres.{}.svc.cluster.local",
            name,
            tenant.namespace().unwrap_or_else(|| "default".into())
        ),
        backend_port: 5432,
        state,
        pooler_mode: None,
    })
}

fn has_multiple_spec_ranges(tenant: &GresTenant) -> bool {
    tenant.spec.ranges.len() > 1
}

fn is_multi_range_unsupported(tenant: &GresTenant) -> bool {
    tenant.status.as_ref().is_some_and(|status| {
        status.conditions.iter().any(|condition| {
            condition.type_ == "Ready"
                && condition.status == "False"
                && condition.reason == "MultiRangeUnsupported"
        })
    })
}

fn tenant_lifecycle_state(tenant: &GresTenant) -> TenantState {
    if let Some(phase) = tenant
        .status
        .as_ref()
        .and_then(|status| status.lifecycle_phase.as_deref())
    {
        return match phase {
            "suspended" => TenantState::Suspended,
            "resume_requested" => TenantState::ResumeRequested,
            _ => TenantState::Active,
        };
    }

    if tenant.spec.suspended.unwrap_or(false) {
        return TenantState::Suspended;
    }

    TenantState::Active
}

fn config_hash(pgdog_toml: &str, users_toml: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(pgdog_toml.as_bytes());
    hasher.update(b"\0");
    hasher.update(users_toml.as_bytes());
    hasher
        .finalize()
        .iter()
        .fold(String::with_capacity(64), |mut out, byte| {
            write!(&mut out, "{byte:02x}").expect("writing to String cannot fail");
            out
        })
}

fn config_secret_name(name: &str) -> String {
    format!("{name}-pgdog-config")
}
fn service_name(name: &str) -> String {
    format!("{name}-pgdog")
}
fn deployment_name(name: &str) -> String {
    format!("{name}-pgdog")
}

fn activator_service_name(name: &str) -> String {
    format!("{name}-gres-activator")
}

fn activator_deployment_name(name: &str) -> String {
    format!("{name}-gres-activator")
}

fn activator_service_host(name: &str, namespace: &str) -> String {
    format!(
        "{}.{}.svc.cluster.local",
        activator_service_name(name),
        namespace
    )
}

fn selector_labels(obj: &Gres) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("app.kubernetes.io/name".into(), APP_NAME.into()),
        ("app.kubernetes.io/instance".into(), obj.name_any()),
        ("app.kubernetes.io/component".into(), "pgdog".into()),
    ])
}

fn meta_labels(obj: &Gres) -> BTreeMap<String, String> {
    let mut labels = selector_labels(obj);
    labels.insert(
        "app.kubernetes.io/managed-by".into(),
        "crabka-operator".into(),
    );
    labels
}

fn render_config_secret(
    obj: &Gres,
    pgdog_toml: &str,
    users_toml: &str,
    hash: &str,
) -> Result<Secret, ReconcileError> {
    Ok(serde_json::from_value(json!({
        "metadata": {
            "name": config_secret_name(&obj.name_any()),
            "namespace": obj.namespace(),
            "labels": meta_labels(obj),
            "annotations": { "crabka.io/pgdog-config-hash": hash },
            "ownerReferences": [owner_ref::<Gres>(obj)?],
        },
        "type": "Opaque",
        "data": {
            "pgdog.toml": ByteString(pgdog_toml.as_bytes().to_vec()),
            "users.toml": ByteString(users_toml.as_bytes().to_vec()),
        }
    }))?)
}

fn render_service(obj: &Gres) -> Result<Service, ReconcileError> {
    Ok(serde_json::from_value(json!({
        "metadata": { "name": service_name(&obj.name_any()), "namespace": obj.namespace(), "labels": meta_labels(obj), "ownerReferences": [owner_ref::<Gres>(obj)?] },
        "spec": { "type": "ClusterIP", "selector": selector_labels(obj), "ports": [{ "name": "postgres", "port": obj.spec.pgdog.listen_port, "targetPort": obj.spec.pgdog.listen_port, "protocol": "TCP" }] }
    }))?)
}

fn render_activator_service(obj: &Gres) -> Result<Service, ReconcileError> {
    Ok(serde_json::from_value(json!({
        "metadata": { "name": activator_service_name(&obj.name_any()), "namespace": obj.namespace(), "labels": activator_meta_labels(obj), "ownerReferences": [owner_ref::<Gres>(obj)?] },
        "spec": { "type": "ClusterIP", "selector": activator_selector_labels(obj), "ports": [{ "name": "postgres", "port": ACTIVATOR_PORT, "targetPort": ACTIVATOR_PORT, "protocol": "TCP" }] }
    }))?)
}

fn render_deployment(obj: &Gres, image: &str, hash: &str) -> Result<Deployment, ReconcileError> {
    let selector = selector_labels(obj);
    let name = obj.name_any();
    let mut volumes =
        vec![json!({ "name": "config", "secret": { "secretName": config_secret_name(&name) } })];
    let mut mounts = vec![json!({ "name": "config", "mountPath": "/etc/pgdog", "readOnly": true })];
    if let Some(tls) = &obj.spec.pgdog.tls_secret_ref {
        volumes.push(json!({ "name": "tls", "secret": { "secretName": tls.name } }));
        mounts.push(json!({ "name": "tls", "mountPath": "/etc/pgdog/tls", "readOnly": true }));
    }
    Ok(serde_json::from_value(json!({
        "metadata": { "name": deployment_name(&name), "namespace": obj.namespace(), "labels": meta_labels(obj), "ownerReferences": [owner_ref::<Gres>(obj)?] },
        "spec": {
            "replicas": obj.spec.pgdog.replicas,
            "selector": { "matchLabels": selector },
            "template": {
                "metadata": { "labels": selector, "annotations": { "crabka.io/pgdog-config-hash": hash } },
                "spec": {
                    "securityContext": { "runAsNonRoot": true, "runAsUser": 65532, "fsGroup": 65532 },
                    "volumes": volumes,
                    "containers": [{
                        "name": "pgdog",
                        "image": image,
                        "args": ["--config", "/etc/pgdog/pgdog.toml", "--users", "/etc/pgdog/users.toml"],
                        "env": [{
                            "name": "PGDOG_ADMIN_PASSWORD",
                            "valueFrom": { "secretKeyRef": {
                                "name": obj.spec.pgdog.admin_secret_ref.name,
                                "key": obj.spec.pgdog.admin_secret_ref.key
                            }}
                        }],
                        "ports": [{ "name": "postgres", "containerPort": obj.spec.pgdog.listen_port, "protocol": "TCP" }],
                        "volumeMounts": mounts,
                        "readinessProbe": { "tcpSocket": { "port": obj.spec.pgdog.listen_port }, "periodSeconds": 5 }
                    }]
                }
            }
        }
    }))?)
}

fn render_activator_deployment(obj: &Gres, image: &str) -> Result<Deployment, ReconcileError> {
    let selector = activator_selector_labels(obj);
    let name = obj.name_any();
    Ok(serde_json::from_value(json!({
        "metadata": { "name": activator_deployment_name(&name), "namespace": obj.namespace(), "labels": activator_meta_labels(obj), "ownerReferences": [owner_ref::<Gres>(obj)?] },
        "spec": {
            "replicas": obj.spec.pgdog.replicas.max(1),
            "selector": { "matchLabels": selector },
            "template": {
                "metadata": { "labels": selector },
                "spec": {
                    "securityContext": { "runAsNonRoot": true, "runAsUser": 65532, "fsGroup": 65532 },
                    "containers": [{
                        "name": "gres-activator",
                        "image": image,
                        "args": ["--listen", format!("0.0.0.0:{ACTIVATOR_PORT}")],
                        "ports": [{ "name": "postgres", "containerPort": ACTIVATOR_PORT, "protocol": "TCP" }],
                        "readinessProbe": { "tcpSocket": { "port": ACTIVATOR_PORT }, "periodSeconds": 5 }
                    }]
                }
            }
        }
    }))?)
}

fn activator_selector_labels(obj: &Gres) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("app.kubernetes.io/name".into(), ACTIVATOR_APP_NAME.into()),
        ("app.kubernetes.io/instance".into(), obj.name_any()),
        (
            "app.kubernetes.io/component".into(),
            "gres-activator".into(),
        ),
    ])
}

fn activator_meta_labels(obj: &Gres) -> BTreeMap<String, String> {
    let mut labels = activator_selector_labels(obj);
    labels.insert(
        "app.kubernetes.io/managed-by".into(),
        "crabka-operator".into(),
    );
    labels
}

async fn patch_status(
    api: &Api<Gres>,
    name: &str,
    obj: &Gres,
    hash: &str,
    balancer_status: &crate::crd::GresBalancerStatus,
) -> Result<(), ReconcileError> {
    let service_url = format!(
        "postgres://{}.{}.svc.cluster.local:{}",
        service_name(name),
        obj.namespace().unwrap_or_else(|| "default".into()),
        obj.spec.pgdog.listen_port
    );
    let body = json!({
        "status": {
            "conditions": [condition("Ready", "True", "Ready", "PgDog config applied; rollout hash updated")],
            "observedGeneration": obj.meta().generation,
            "confirmedPgdogConfigHash": hash,
            "serviceUrl": service_url,
            "balancer": balancer_status,
        }
    });
    api.patch_status(
        name,
        &PatchParams {
            field_manager: Some(FIELD_MANAGER.into()),
            ..Default::default()
        },
        &Patch::Merge(&body),
    )
    .await?;
    Ok(())
}

#[must_use]
pub fn balancer_status(obj: &Gres) -> crate::crd::GresBalancerStatus {
    let Some(spec) = &obj.spec.balancer else {
        return disabled_balancer_status(
            "balancer planning disabled; spec.balancer is not set",
            false,
        );
    };

    if !spec.enabled {
        return disabled_balancer_status(
            "balancer planning disabled by spec.balancer.enabled=false",
            spec.registry_layout.transactional_registry_protocol,
        );
    }

    let (enabled_goals, disabled_goals) = balancer_goal_names(&spec.goals);
    let operations = spec
        .plan_snapshot
        .as_ref()
        .map_or_else(Vec::new, |snapshot| snapshot.operations.clone());
    let planned_operations = operations.len();
    let planned_operation_kinds = operation_kinds(&operations);
    let transactional_registry_protocol_available =
        spec.registry_layout.transactional_registry_protocol;
    let executable_operations = 0;
    let executable_operation_kinds = Vec::new();
    let unsupported_operations = planned_operations - executable_operations;
    let unsupported_operation_kinds = planned_operation_kinds.clone();
    let message = balancer_message(
        planned_operations,
        executable_operations,
        transactional_registry_protocol_available,
    );

    crate::crd::GresBalancerStatus {
        enabled: true,
        dry_run_only: true,
        transactional_registry_protocol_available,
        enabled_goals,
        disabled_goals,
        planned_operations,
        planned_operation_kinds,
        executable_operations,
        executable_operation_kinds,
        unsupported_operations,
        unsupported_operation_kinds,
        mutation_disabled_reason: mutation_disabled_reason(
            transactional_registry_protocol_available,
        )
        .to_string(),
        message,
    }
}

fn disabled_balancer_status(
    message: &str,
    transactional_registry_protocol_available: bool,
) -> crate::crd::GresBalancerStatus {
    crate::crd::GresBalancerStatus {
        enabled: false,
        dry_run_only: true,
        transactional_registry_protocol_available,
        enabled_goals: Vec::new(),
        disabled_goals: all_balancer_goal_names(),
        planned_operations: 0,
        planned_operation_kinds: Vec::new(),
        executable_operations: 0,
        executable_operation_kinds: Vec::new(),
        unsupported_operations: 0,
        unsupported_operation_kinds: Vec::new(),
        mutation_disabled_reason: mutation_disabled_reason(
            transactional_registry_protocol_available,
        )
        .to_string(),
        message: message.to_string(),
    }
}

const fn mutation_disabled_reason(transactional_registry_protocol_available: bool) -> &'static str {
    if transactional_registry_protocol_available {
        "Kafka transactional registry protocol is available, but physical Move, Split, and Merge orchestration is unavailable; all balancer mutations remain disabled pending checkpoint, copy, catch-up, and cutover"
    } else {
        "Kafka transactional registry protocol is not configured or available; all balancer mutations remain disabled"
    }
}

fn operation_kinds(operations: &[crate::crd::GresBalancerOperationKind]) -> Vec<String> {
    let mut kinds = Vec::new();
    for operation in operations {
        let name = operation.as_str();
        if !kinds.iter().any(|kind| kind == name) {
            kinds.push(name.to_string());
        }
    }
    kinds
}

fn balancer_message(
    planned_operations: usize,
    executable_operations: usize,
    transactional_registry_protocol_available: bool,
) -> String {
    if planned_operations == 0 {
        return if transactional_registry_protocol_available {
            "Kafka transactional registry protocol is configured; no dry-run planner snapshot is available; the operator does not execute mutations".to_string()
        } else {
            "no dry-run planner snapshot is available; Kafka transactional registry protocol is not configured; mutations remain disabled".to_string()
        };
    }

    if !transactional_registry_protocol_available {
        return format!(
            "dry-run planner snapshot reports {planned_operations} operation(s); Kafka transactional registry protocol is not configured; mutations remain disabled"
        );
    }

    format!(
        "dry-run planner snapshot reports {planned_operations} operation(s); Kafka transactional registry protocol is available, but {executable_operations} operations are executable because physical checkpoint, copy, catch-up, and cutover orchestration is unavailable"
    )
}

fn balancer_goal_names(goals: &crate::crd::GresBalancerGoals) -> (Vec<String>, Vec<String>) {
    let mut enabled = Vec::new();
    let mut disabled = Vec::new();
    for (goal, name) in [
        (
            crate::crd::GresBalancerGoal::CoLocationIntegrity,
            "co_location_integrity",
        ),
        (crate::crd::GresBalancerGoal::RangeLimit, "range_limit"),
        (crate::crd::GresBalancerGoal::RangeSize, "range_size"),
        (crate::crd::GresBalancerGoal::LoadSkew, "load_skew"),
        (
            crate::crd::GresBalancerGoal::AutoShardConversion,
            "auto_shard_conversion",
        ),
    ] {
        if goals.disabled_goals.contains(&goal) {
            disabled.push(name.to_string());
        } else {
            enabled.push(name.to_string());
        }
    }
    (enabled, disabled)
}

fn all_balancer_goal_names() -> Vec<String> {
    vec![
        "co_location_integrity".to_string(),
        "range_limit".to_string(),
        "range_size".to_string(),
        "load_skew".to_string(),
        "auto_shard_conversion".to_string(),
    ]
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::crd::{GresSpec, PgdogSpec, SecretKeyRef};

    fn gres() -> Gres {
        let mut obj = Gres::new(
            "fleet",
            GresSpec {
                kafka_cluster: "demo".into(),
                pgdog: PgdogSpec {
                    image: None,
                    replicas: 1,
                    listen_port: 6432,
                    tls_secret_ref: None,
                    admin_secret_ref: SecretKeyRef {
                        name: "admin".into(),
                        key: "password".into(),
                    },
                },
                defaults: None,
                balancer: None,
            },
        );
        obj.metadata.namespace = Some("ns".into());
        obj.metadata.uid = Some("uid".into());
        obj
    }

    #[test]
    fn config_secret_contains_rendered_pgdog_files_and_hash() {
        let obj = gres();
        let secret = render_config_secret(&obj, "[general]\n", "[[users]]\n", "abc").unwrap();
        assert!(secret.data.as_ref().unwrap().contains_key("pgdog.toml"));
        assert!(secret.data.as_ref().unwrap().contains_key("users.toml"));
        assert!(secret.metadata.annotations.unwrap()["crabka.io/pgdog-config-hash"] == "abc");
    }

    #[test]
    fn config_hash_changes_when_rendered_config_changes() {
        assert!(config_hash("a", "b") != config_hash("a", "c"));
    }

    #[test]
    fn balancer_status_without_registry_protocol_marks_all_operations_unsupported() {
        let mut obj = gres();
        obj.spec.balancer = Some(crate::crd::GresBalancerSpec {
            enabled: true,
            goals: crate::crd::GresBalancerGoals::default(),
            thresholds: crate::crd::GresBalancerThresholds::default(),
            registry_layout: crate::crd::GresBalancerRegistryLayout::default(),
            plan_snapshot: Some(crate::crd::GresBalancerPlanSnapshot {
                operations: vec![
                    crate::crd::GresBalancerOperationKind::Split,
                    crate::crd::GresBalancerOperationKind::Move,
                    crate::crd::GresBalancerOperationKind::Merge,
                    crate::crd::GresBalancerOperationKind::ConvertToSharded,
                ],
            }),
        });

        let status = balancer_status(&obj);

        assert!(status.dry_run_only);
        assert!(status.planned_operations == 4);
        assert!(status.executable_operations == 0);
        assert!(status.executable_operation_kinds.is_empty());
        assert!(status.unsupported_operations == 4);
        assert!(status.unsupported_operation_kinds == status.planned_operation_kinds);
    }

    #[test]
    fn balancer_status_reports_protocol_without_claiming_physical_operations_executable() {
        let mut obj = gres();
        obj.spec.balancer = Some(crate::crd::GresBalancerSpec {
            enabled: true,
            goals: crate::crd::GresBalancerGoals::default(),
            thresholds: crate::crd::GresBalancerThresholds::default(),
            registry_layout: crate::crd::GresBalancerRegistryLayout {
                transactional_registry_protocol: true,
            },
            plan_snapshot: Some(crate::crd::GresBalancerPlanSnapshot {
                operations: vec![
                    crate::crd::GresBalancerOperationKind::Split,
                    crate::crd::GresBalancerOperationKind::ConvertToSharded,
                    crate::crd::GresBalancerOperationKind::Move,
                    crate::crd::GresBalancerOperationKind::Merge,
                    crate::crd::GresBalancerOperationKind::Split,
                ],
            }),
        });

        let status = balancer_status(&obj);

        assert!(status.transactional_registry_protocol_available);
        assert!(status.planned_operations == 5);
        assert!(status.executable_operations == 0);
        assert!(status.executable_operation_kinds.is_empty());
        assert!(status.unsupported_operations == 5);
        assert!(status.unsupported_operation_kinds == status.planned_operation_kinds);
        assert!(
            status
                .message
                .contains("checkpoint, copy, catch-up, and cutover")
        );
        assert!(
            status
                .mutation_disabled_reason
                .contains("checkpoint, copy, catch-up, and cutover")
        );
    }
}
