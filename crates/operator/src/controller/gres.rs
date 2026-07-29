//! `Gres` fleet reconciler. Renders the `PgDog` front door from live tenants.

use std::{collections::BTreeMap, fmt::Write as _, sync::Arc};

use crabka_gres_control::{
    PgdogGeneral, PgdogRenderInput, PgdogTimeouts, PgdogUser, TenantEndpoint, TenantName,
    TenantState, render_pgdog_toml, render_users_toml,
};
use crabka_units::{Time, convert::TimeExt as _, fmt::Human as _};
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
    controller::{
        common::{
            self, FIELD_MANAGER, ReconcileError, apply_object, condition, millis_u64, owner_ref,
            time_from_millis_u64,
        },
        gres_tenant::COMPUTE_PORT,
        topic::internal_listener_bootstrap,
    },
    crd::{Gres, GresTenant, Kafka, gres::EffectivePgdogPolicy},
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
const DEFAULT_ACTIVATOR_REGISTRY_POLL: Time = crabka_units::millis(250);
const DEFAULT_ACTIVATOR_COLD_START_TIMEOUT: Time = crabka_units::secs(30);
const DEFAULT_ACTIVATOR_READINESS_PERIOD_SECONDS: i32 = 5;

/// Run the controller forever.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
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

pub fn error_policy(_obj: Arc<Gres>, err: &ReconcileError, ctx: Arc<Context>) -> Action {
    tracing::warn!(error = %err, "gres fleet reconcile error, requeueing");
    common::error_requeue(ctx)
}

#[tracing::instrument(level = "info", skip_all, fields(kind = "Gres", name = %obj.name_any()))]
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub async fn reconcile(obj: Arc<Gres>, ctx: Arc<Context>) -> Result<Action, ReconcileError> {
    common::record_reconcile(&ctx, "Gres", Box::pin(reconcile_inner(obj, ctx.clone()))).await
}

async fn reconcile_inner(obj: Arc<Gres>, ctx: Arc<Context>) -> Result<Action, ReconcileError> {
    let pgdog_policy = obj
        .spec
        .pgdog
        .effective_policy()
        .map_err(ReconcileError::Malformed)?;
    validate_activator_config(&obj.spec)?;
    let activator_cold_start_timeout = obj
        .spec
        .activator
        .as_ref()
        .and_then(|activator| activator.cold_start_timeout)
        .unwrap_or(DEFAULT_ACTIVATOR_COLD_START_TIMEOUT);
    let cold_start_ceiling = PgdogTimeouts::cold_start_ceiling_for_attempt_timeout(
        activator_cold_start_timeout,
        pgdog_policy.connect_attempts,
    )
    .map_err(|error| {
        ReconcileError::Malformed(format!("spec.activator.coldStartTimeout: {error}"))
    })?;
    let ns = obj.namespace().unwrap_or_else(|| "default".into());
    let name = obj.name_any();
    let kafka_api: Api<Kafka> = Api::namespaced(ctx.client.clone(), &ns);
    let kafka = kafka_api
        .get_opt(&obj.spec.kafka_cluster)
        .await?
        .ok_or_else(|| {
            ReconcileError::Malformed(format!(
                "referenced Kafka {} does not exist",
                obj.spec.kafka_cluster
            ))
        })?;
    let registry_policy = kafka
        .spec
        .gres_registry
        .as_ref()
        .map_or_else(
            || Ok(crabka_gres_control::RegistryPolicy::default()),
            crate::crd::GresRegistrySpec::policy,
        )
        .map_err(ReconcileError::Malformed)?;
    let bootstrap = internal_listener_bootstrap(&kafka)
        .unwrap_or_else(|| format!("{}-plain-bootstrap.{ns}.svc:9092", obj.spec.kafka_cluster));
    let gres_api: Api<Gres> = Api::namespaced(ctx.client.clone(), &ns);
    let tenant_api: Api<GresTenant> = Api::namespaced(ctx.client.clone(), &ns);
    let tenants = tenant_api.list(&ListParams::default()).await?;
    let secret_api: Api<Secret> = Api::namespaced(ctx.client.clone(), &ns);
    let endpoints: Vec<_> = tenants
        .items
        .iter()
        .filter(|tenant| tenant.spec.gres == name)
        .filter_map(tenant_endpoint)
        .collect();
    let mut users = Vec::with_capacity(endpoints.len());
    for endpoint in &endpoints {
        let Some(tenant) = tenants
            .items
            .iter()
            .find(|tenant| tenant.name_any() == endpoint.name)
        else {
            continue;
        };
        // PgDog 0.1.47 cannot bootstrap a passwordless passthrough pool to an
        // activator: it pauses the pool before the frontend password exchange.
        // Scope the credential fallback to non-Active routes; active routes
        // retain normal passthrough authentication.
        let grace_deadline = tenant
            .status
            .as_ref()
            .and_then(|status| status.pgdog_credential_grace_until_unix_ms);
        let now = current_unix_millis();
        let needs_bootstrap_credential = needs_bootstrap_credential(
            endpoint.state,
            grace_deadline,
            now,
            time_from_millis_u64(pgdog_policy.direct_bootstrap_grace.into_value()),
        );
        let password = if needs_bootstrap_credential {
            let reference = &tenant.spec.password_secret_ref;
            let secret = secret_api.get(&reference.name).await?;
            let bytes = secret
                .data
                .as_ref()
                .and_then(|data| data.get(&reference.key))
                .ok_or_else(|| {
                    ReconcileError::MalformedSecret(format!(
                        "tenant password Secret {:?} missing key {:?}",
                        reference.name, reference.key
                    ))
                })?;
            Some(String::from_utf8(bytes.0.clone()).map_err(|error| {
                ReconcileError::MalformedSecret(format!(
                    "tenant password Secret {:?} key {:?} is not UTF-8: {error}",
                    reference.name, reference.key
                ))
            })?)
        } else {
            None
        };
        users.push(PgdogUser {
            name: tenant.spec.user.clone(),
            database: endpoint.name.clone(),
            password,
        });
    }

    let (pgdog_toml, users_toml) = render_pgdog_files(
        &obj,
        &pgdog_policy,
        &tenants.items,
        &endpoints,
        cold_start_ceiling,
        users,
    )?;
    let hash = config_hash(&pgdog_toml, &users_toml);
    let rollout_hash = config_hash(&pgdog_toml, "");

    apply_object(
        &secret_api,
        &config_secret_name(&name),
        &render_config_secret(&obj, &pgdog_toml, &users_toml, &hash, &rollout_hash)?,
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
    let activator_image = activator_image(&obj, &ctx);
    apply_object(
        &deployment_api,
        &activator_deployment_name(&name),
        &render_activator_deployment(&obj, &bootstrap, &activator_image, &registry_policy)?,
    )
    .await?;
    apply_object(
        &deployment_api,
        &deployment_name(&name),
        &render_deployment(&obj, &image, &rollout_hash, &pgdog_policy)?,
    )
    .await?;

    if obj
        .status
        .as_ref()
        .and_then(|status| status.confirmed_pgdog_config_hash.as_deref())
        != Some(hash.as_str())
    {
        let admin_timeout = ctx.config.pgdog_admin_timeout.to_std();
        let verified = tokio::time::timeout(
            admin_timeout,
            verify_pgdog_reload(&obj, &ctx, &endpoints, &rollout_hash),
        )
        .await
        .map_err(|_| {
            ReconcileError::PgdogAdmin(crate::context::PgdogAdminError::Fleet(format!(
                "admin reload verification exceeded {admin_timeout:?}"
            )))
        })??;
        if !verified {
            tracing::warn!(gres = %name, config_hash = %hash, "pgdog admin view is stale after reload attempts");
            return Ok(Action::requeue(ctx.config.pgdog_reload_requeue.to_std()));
        }
    }

    let balancer_status = balancer_status(&obj);
    patch_status(&gres_api, &name, &obj, &hash, &balancer_status).await?;
    let now = current_unix_millis();
    let requeue = pgdog_transition_requeue_for_tenants(
        &tenants.items,
        &name,
        now,
        time_from_millis_u64(pgdog_policy.direct_bootstrap_grace.into_value()),
        ctx.config.pgdog_transition_poll,
    );
    Ok(Action::requeue(requeue.to_std()))
}

fn validate_activator_config(spec: &crate::crd::GresSpec) -> Result<(), ReconcileError> {
    macro_rules! validate {
        ($value:expr, $rule:ty, $path:literal) => {
            if let Some(value) = $value {
                <$rule>::new(value)
                    .map_err(|error| ReconcileError::Malformed(format!("{}: {error}", $path)))?;
            }
        };
    }

    if let Some(activator) = &spec.activator {
        if let Some(image) = &activator.image {
            refined_type::rule::NonEmptyString::new(image.clone()).map_err(|error| {
                ReconcileError::Malformed(format!("spec.activator.image: {error}"))
            })?;
        }
        validate!(
            activator.replicas,
            refined_type::rule::GreaterI32<0>,
            "spec.activator.replicas"
        );
        for (value, path) in [
            (activator.registry_poll, "spec.activator.registryPoll"),
            (
                activator.cold_start_timeout,
                "spec.activator.coldStartTimeout",
            ),
        ] {
            if value
                .is_some_and(|value| !value.secs_f64().is_finite() || value <= Time::from_secs(0))
            {
                return Err(ReconcileError::Malformed(format!(
                    "{path}: must be finite and positive"
                )));
            }
        }
        validate!(
            activator.readiness_probe_period_seconds,
            refined_type::rule::GreaterI32<0>,
            "spec.activator.readinessProbePeriodSeconds"
        );
    }
    Ok(())
}

/// Next requeue that lands on a credential-grace boundary, or `fallback` when
/// every boundary is already behind `now`.
///
/// The deadlines and `now` are epoch-millisecond instants; `direct_bootstrap_grace`
/// and the result are extents, so the grace window narrows back to raw
/// milliseconds only for the instant arithmetic.
fn next_pgdog_transition_requeue(
    grace_deadlines: impl Iterator<Item = u64>,
    now: u64,
    direct_bootstrap_grace: Time,
    fallback: Time,
) -> Time {
    let grace_ms = millis_u64(direct_bootstrap_grace);
    grace_deadlines
        .flat_map(|deadline| [deadline, deadline.saturating_add(grace_ms)])
        .filter(|deadline| *deadline > now)
        .map(|deadline| time_from_millis_u64(deadline.saturating_sub(now).max(1)))
        .reduce(Time::min)
        .map_or(fallback, |boundary| boundary.min(fallback))
}

fn pgdog_transition_requeue_for_tenants(
    tenants: &[GresTenant],
    gres_name: &str,
    now: u64,
    direct_bootstrap_grace: Time,
    fallback: Time,
) -> Time {
    next_pgdog_transition_requeue(
        tenants
            .iter()
            .filter(|tenant| tenant.spec.gres == gres_name)
            .filter_map(|tenant| {
                tenant
                    .status
                    .as_ref()
                    .and_then(|status| status.pgdog_credential_grace_until_unix_ms)
            }),
        now,
        direct_bootstrap_grace,
        fallback,
    )
}

fn needs_bootstrap_credential(
    state: TenantState,
    grace_deadline: Option<u64>,
    now: u64,
    direct_bootstrap_grace: Time,
) -> bool {
    let grace_ms = millis_u64(direct_bootstrap_grace);
    state != TenantState::Active
        || grace_deadline.is_some_and(|deadline| now < deadline.saturating_add(grace_ms))
}

fn uses_suspension_idle_timeout(
    tenants: &[GresTenant],
    gres_name: &str,
    defaults: Option<&crate::crd::TenantDefaults>,
) -> bool {
    tenants
        .iter()
        .filter(|tenant| tenant.spec.gres == gres_name)
        .any(|tenant| {
            tenant
                .spec
                .overrides
                .as_ref()
                .and_then(|overrides| overrides.idle_seconds)
                .or_else(|| defaults.and_then(|defaults| defaults.idle_seconds))
                .is_some_and(|idle_seconds| idle_seconds > 0)
        })
}

fn render_pgdog_files(
    obj: &Gres,
    policy: &EffectivePgdogPolicy,
    tenants: &[GresTenant],
    endpoints: &[TenantEndpoint],
    cold_start_ceiling: Time,
    users: Vec<PgdogUser>,
) -> Result<(String, String), ReconcileError> {
    let tls = obj.spec.pgdog.tls_secret_ref.as_ref();
    let input = PgdogRenderInput {
        tenants: endpoints,
        activator: Some((
            activator_service_host(
                &obj.name_any(),
                &obj.namespace().unwrap_or_else(|| "default".into()),
            ),
            ACTIVATOR_PORT_U16,
        )),
        general: PgdogGeneral {
            listen_port: policy.listen_port,
            tls_cert_path: tls.map(|_| "/etc/pgdog/tls/tls.crt".into()),
            tls_key_path: tls.map(|_| "/etc/pgdog/tls/tls.key".into()),
            tls_client_ca_path: tls.map(|_| "/etc/pgdog/tls/ca.crt".into()),
            pooler_mode: policy.pooler_mode,
            connect_attempts: policy.connect_attempts,
            idle_timeout: if uses_suspension_idle_timeout(
                tenants,
                &obj.name_any(),
                obj.spec.defaults.as_ref(),
            ) {
                time_from_millis_u64(policy.suspension_idle_timeout.into_value())
            } else {
                time_from_millis_u64(policy.idle_timeout.into_value())
            },
            server_lifetime: time_from_millis_u64(policy.server_lifetime.into_value()),
            cold_start_ceiling,
            users,
            ..Default::default()
        },
    };
    Ok((render_pgdog_toml(&input)?, render_users_toml(&input)?))
}

fn current_unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
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

fn activator_image(obj: &Gres, ctx: &Context) -> String {
    obj.spec
        .activator
        .as_ref()
        .and_then(|activator| activator.image.clone())
        .or_else(|| ctx.config.default_gres_activator_image.clone())
        .unwrap_or_else(|| DEFAULT_ACTIVATOR_IMAGE.to_string())
}

async fn verify_pgdog_reload(
    obj: &Gres,
    ctx: &Context,
    endpoints: &[TenantEndpoint],
    expected_rollout_hash: &str,
) -> Result<bool, ReconcileError> {
    let ns = obj.namespace().unwrap_or_else(|| "default".into());
    if endpoints
        .iter()
        .all(|endpoint| endpoint.state != TenantState::Active)
    {
        // An idle activator route has no backend pool to inspect. Forcing PgDog
        // to materialize one (for example with RECONNECT) would itself wake the
        // tenant. The completed hash-addressed Deployment rollout is the
        // non-invasive proof that every replica mounted this exact config.
        let deployments: Api<Deployment> = Api::namespaced(ctx.client.clone(), &ns);
        let deployment = deployments.get(&deployment_name(&obj.name_any())).await?;
        return Ok(deployment_has_applied_hash(
            &deployment,
            expected_rollout_hash,
        ));
    }
    let password = pgdog_admin_password(obj, ctx, &ns).await?;
    let tls_material = pgdog_tls_material(obj, ctx, &ns).await?;
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
        if hosts.len() != usize::try_from(obj.spec.pgdog.replicas).unwrap_or_default() {
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
            tls_ca_pem: tls_material.as_ref().map(|material| material.0.clone()),
            tls_client_identity_pem: tls_material
                .as_ref()
                .map(|material| (material.1.clone(), material.2.clone())),
        })
        .collect::<Vec<_>>();

    let reload_attempts = ctx.config.pgdog_reload_attempts.into_value();
    for attempt in 1..=reload_attempts {
        if ctx
            .pgdog_admin
            .reload_and_database_views_match(&requests)
            .await?
        {
            return Ok(true);
        }
        if attempt < reload_attempts {
            tokio::time::sleep(ctx.config.pgdog_reload_backoff.to_std()).await;
        }
    }
    Ok(false)
}

fn deployment_has_applied_hash(deployment: &Deployment, expected_hash: &str) -> bool {
    let desired = deployment
        .spec
        .as_ref()
        .and_then(|spec| spec.replicas)
        .unwrap_or(1);
    let generation = deployment.metadata.generation.unwrap_or_default();
    let Some(status) = deployment.status.as_ref() else {
        return false;
    };
    deployment
        .spec
        .as_ref()
        .and_then(|spec| spec.template.metadata.as_ref())
        .and_then(|metadata| metadata.annotations.as_ref())
        .and_then(|annotations| annotations.get("crabka.io/pgdog-config-hash"))
        .is_some_and(|hash| hash == expected_hash)
        && status.observed_generation.unwrap_or_default() >= generation
        && status.updated_replicas.unwrap_or_default() == desired
        && status.available_replicas.unwrap_or_default() == desired
}

async fn pgdog_tls_material(
    obj: &Gres,
    ctx: &Context,
    namespace: &str,
) -> Result<Option<(Vec<u8>, Vec<u8>, Vec<u8>)>, ReconcileError> {
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
    let ca = data
        .get("ca.crt")
        .or_else(|| data.get("tls.crt"))
        .ok_or_else(|| {
            ReconcileError::MalformedSecret(format!(
                "PgDog TLS Secret {:?} must contain ca.crt or tls.crt",
                secret_ref.name
            ))
        })?;
    let client_certificate = data.get("client.crt").ok_or_else(|| {
        ReconcileError::MalformedSecret(format!(
            "PgDog TLS Secret {:?} must contain client.crt for admin mTLS",
            secret_ref.name
        ))
    })?;
    let client_key = data.get("client.key").ok_or_else(|| {
        ReconcileError::MalformedSecret(format!(
            "PgDog TLS Secret {:?} must contain client.key for admin mTLS",
            secret_ref.name
        ))
    })?;
    Ok(Some((
        ca.0.clone(),
        client_certificate.0.clone(),
        client_key.0.clone(),
    )))
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
    tenant_endpoint_at(tenant, current_unix_millis())
}

fn tenant_endpoint_at(tenant: &GresTenant, now_unix_millis: u64) -> Option<TenantEndpoint> {
    if has_multiple_spec_ranges(tenant) || is_multi_range_unsupported(tenant) {
        return None;
    }

    let name = tenant.name_any();
    TenantName::try_from(name.as_str()).ok()?;
    let state = tenant_lifecycle_state(tenant);
    // Keep the wake path byte-for-byte identical to the suspended PgDog
    // configuration. The held frontend session must continue through the
    // activator while compute recovers; the ordinary reconcile after this
    // bounded grace period performs the lazy flip to direct compute.
    let state = if state == TenantState::Active
        && tenant
            .status
            .as_ref()
            .and_then(|status| status.pgdog_credential_grace_until_unix_ms)
            .is_some_and(|deadline| now_unix_millis < deadline)
    {
        TenantState::ResumeRequested
    } else {
        state
    };
    Some(TenantEndpoint {
        name: name.clone(),
        backend_host: format!(
            "{}-gres.{}.svc.cluster.local",
            name,
            tenant.namespace().unwrap_or_else(|| "default".into())
        ),
        backend_port: u16::try_from(COMPUTE_PORT).expect("compute listener port fits in u16"),
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
    rollout_hash: &str,
) -> Result<Secret, ReconcileError> {
    Ok(serde_json::from_value(json!({
        "metadata": {
            "name": config_secret_name(&obj.name_any()),
            "namespace": obj.namespace(),
            "labels": meta_labels(obj),
            "annotations": {
                "crabka.io/pgdog-config-hash": hash,
                "crabka.io/pgdog-rollout-hash": rollout_hash
            },
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

fn render_deployment(
    obj: &Gres,
    image: &str,
    hash: &str,
    pgdog_policy: &EffectivePgdogPolicy,
) -> Result<Deployment, ReconcileError> {
    let selector = selector_labels(obj);
    let name = obj.name_any();
    let mut volumes = vec![
        json!({ "name": "config", "secret": { "secretName": config_secret_name(&name), "defaultMode": 256 } }),
    ];
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
                        "command": ["/usr/local/bin/pgdog"],
                        "args": ["--config", "/etc/pgdog/pgdog.toml", "--users", "/etc/pgdog/users.toml", "run"],
                        "env": [{
                            "name": "PGDOG_ADMIN_PASSWORD",
                            "valueFrom": { "secretKeyRef": {
                                "name": obj.spec.pgdog.admin_secret_ref.name,
                                "key": obj.spec.pgdog.admin_secret_ref.key
                            }}
                        }],
                        "ports": [{ "name": "postgres", "containerPort": obj.spec.pgdog.listen_port, "protocol": "TCP" }],
                        "volumeMounts": mounts,
                        "readinessProbe": { "tcpSocket": { "port": obj.spec.pgdog.listen_port }, "periodSeconds": pgdog_policy.readiness_probe_period_seconds }
                    }]
                }
            }
        }
    }))?)
}

fn render_activator_deployment(
    obj: &Gres,
    bootstrap: &str,
    image: &str,
    registry_policy: &crabka_gres_control::RegistryPolicy,
) -> Result<Deployment, ReconcileError> {
    let selector = activator_selector_labels(obj);
    let name = obj.name_any();
    let activator = obj.spec.activator.as_ref();
    let replicas = activator
        .and_then(|activator| activator.replicas)
        .unwrap_or_else(|| obj.spec.pgdog.replicas.max(1));
    let registry_poll = activator
        .and_then(|activator| activator.registry_poll)
        .unwrap_or(DEFAULT_ACTIVATOR_REGISTRY_POLL);
    let cold_start_timeout = activator
        .and_then(|activator| activator.cold_start_timeout)
        .unwrap_or(DEFAULT_ACTIVATOR_COLD_START_TIMEOUT);
    let readiness_period_seconds = activator
        .and_then(|activator| activator.readiness_probe_period_seconds)
        .unwrap_or(DEFAULT_ACTIVATOR_READINESS_PERIOD_SECONDS);
    Ok(serde_json::from_value(json!({
        "metadata": { "name": activator_deployment_name(&name), "namespace": obj.namespace(), "labels": activator_meta_labels(obj), "ownerReferences": [owner_ref::<Gres>(obj)?] },
        "spec": {
            "replicas": replicas,
            "selector": { "matchLabels": selector },
            "template": {
                "metadata": { "labels": selector },
                "spec": {
                    "securityContext": { "runAsNonRoot": true, "runAsUser": 65532, "fsGroup": 65532 },
                    "containers": [{
                        "name": "gres-activator",
                        "image": image,
                        "args": [
                            "--listen", format!("0.0.0.0:{ACTIVATOR_PORT}"),
                            "--bootstrap", bootstrap,
                            "--registry-poll", registry_poll.human().to_string(),
                            "--cold-start-timeout", cold_start_timeout.human().to_string(),
                            "--registry-replication-factor", registry_policy.replication_factor().to_string(),
                            "--registry-topic-create-timeout", registry_policy.topic_create_timeout().human().to_string(),
                            "--registry-reader-retry-backoff", registry_policy.reader_retry_backoff().human().to_string(),
                            "--registry-fetch-max-wait", registry_policy.fetch_max_wait().human().to_string(),
                            "--registry-fetch-partition-max", registry_policy.fetch_partition_max().human().to_string(),
                            "--registry-producer-dns-timeout", Time::from_std(registry_policy.producer_dns_timeout().duration()).human().to_string(),
                            "--registry-reader-admin-dns-timeout", Time::from_std(registry_policy.reader_admin_dns_timeout().duration()).human().to_string(),
                            "--backend-endpoint-template", format!("{{tenant}}-gres.{namespace}.svc:{COMPUTE_PORT}", namespace = obj.namespace().unwrap_or_else(|| "default".into()))
                        ],
                        "ports": [{ "name": "postgres", "containerPort": ACTIVATOR_PORT, "protocol": "TCP" }],
                        "readinessProbe": { "tcpSocket": { "port": ACTIVATOR_PORT }, "periodSeconds": readiness_period_seconds }
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
    use crabka_units::{millis, secs};

    use super::*;
    use crate::crd::{
        GresActivatorSpec, GresSpec, GresTenantSpec, GresTenantStatus, PgdogSpec, SecretKeyRef,
    };

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
                    pooler_mode: None,
                    connect_attempts: None,
                    idle_timeout: None,
                    suspension_idle_timeout: None,
                    server_lifetime: None,
                    readiness_probe_period_seconds: None,
                    direct_bootstrap_grace: None,
                },
                activator: None,
                compute: None,
                defaults: None,
                balancer: None,
            },
        );
        obj.metadata.namespace = Some("ns".into());
        obj.metadata.uid = Some("uid".into());
        obj
    }

    fn tenant_with_phase(phase: &str, grace_until: Option<u64>) -> GresTenant {
        let mut tenant = GresTenant::new(
            "tenant-a",
            GresTenantSpec {
                gres: "fleet".into(),
                image: None,
                user: "alice".into(),
                password_secret_ref: SecretKeyRef {
                    name: "tenant-a-password".into(),
                    key: "password".into(),
                },
                suspended: None,
                resources: None,
                ranges: Vec::new(),
                overrides: None,
            },
        );
        tenant.metadata.namespace = Some("ns".into());
        tenant.status = Some(GresTenantStatus {
            lifecycle_phase: Some(phase.into()),
            pgdog_credential_grace_until_unix_ms: grace_until,
            ..Default::default()
        });
        tenant
    }

    #[test]
    fn resumed_active_grace_keeps_suspended_pgdog_route_hash_unchanged() {
        let suspended = tenant_endpoint_at(&tenant_with_phase("suspended", None), 1_000).unwrap();
        let resumed =
            tenant_endpoint_at(&tenant_with_phase("active", Some(31_000)), 1_000).unwrap();

        let activator = Some((
            "fleet-gres-activator.ns.svc.cluster.local".to_string(),
            ACTIVATOR_PORT_U16,
        ));
        let general = PgdogGeneral::default();
        let suspended_toml = render_pgdog_toml(&PgdogRenderInput {
            tenants: std::slice::from_ref(&suspended),
            activator: activator.clone(),
            general: general.clone(),
        })
        .unwrap();
        let resumed_toml = render_pgdog_toml(&PgdogRenderInput {
            tenants: std::slice::from_ref(&resumed),
            activator,
            general,
        })
        .unwrap();
        let users = "[[users]]\nname = \"alice\"\npassword = \"secret\"\n";
        assert_eq!(
            config_hash(&suspended_toml, users),
            config_hash(&resumed_toml, users)
        );
    }

    #[test]
    fn tenant_endpoint_states_use_the_range_zero_sql_listener() {
        let cases = [
            ("suspended", None, TenantState::Suspended),
            ("resume_requested", None, TenantState::ResumeRequested),
            ("active", Some(31_000), TenantState::ResumeRequested),
            ("active", Some(1_000), TenantState::Active),
        ];

        for (phase, grace_until, state) in cases {
            assert_eq!(
                tenant_endpoint_at(&tenant_with_phase(phase, grace_until), 1_000),
                Some(TenantEndpoint {
                    name: "tenant-a".into(),
                    backend_host: "tenant-a-gres.ns.svc.cluster.local".into(),
                    backend_port: 5432,
                    state,
                    pooler_mode: None,
                })
            );
        }
    }

    #[test]
    fn config_secret_contains_rendered_pgdog_files_and_hash() {
        let obj = gres();
        let secret =
            render_config_secret(&obj, "[general]\n", "[[users]]\n", "abc", "route").unwrap();
        assert!(secret.data.as_ref().unwrap().contains_key("pgdog.toml"));
        assert!(secret.data.as_ref().unwrap().contains_key("users.toml"));
        assert!(secret.metadata.annotations.unwrap()["crabka.io/pgdog-config-hash"] == "abc");
    }

    #[test]
    fn pgdog_workload_invokes_the_pinned_image_binary_and_run_subcommand() {
        let obj = gres();
        let policy = obj.spec.pgdog.effective_policy().expect("PgDog policy");
        let deployment = render_deployment(&obj, "ghcr.io/pgdogdev/pgdog:0.1.47", "hash", &policy)
            .expect("render PgDog deployment");
        let container = &deployment
            .spec
            .expect("deployment spec")
            .template
            .spec
            .expect("pod spec")
            .containers[0];
        assert!(
            container
                .command
                .as_ref()
                .is_some_and(|command| command == &["/usr/local/bin/pgdog"])
        );
        assert!(
            container
                .args
                .as_ref()
                .is_some_and(|args| args.last().map(String::as_str) == Some("run"))
        );
    }

    #[test]
    fn config_hash_changes_when_rendered_config_changes() {
        assert!(config_hash("a", "b") != config_hash("a", "c"));
    }

    #[test]
    fn activator_workload_is_wired_to_registry_and_range_zero_sql_listener() {
        let deployment = render_activator_deployment(
            &gres(),
            "registry.demo.svc:9092",
            "crabka-gres-activator:e2e",
            &crabka_gres_control::RegistryPolicy::default(),
        )
        .expect("render activator deployment");
        let args = deployment
            .spec
            .expect("spec")
            .template
            .spec
            .expect("pod spec")
            .containers[0]
            .args
            .clone()
            .expect("activator args");

        assert_eq!(
            args,
            [
                "--listen",
                "0.0.0.0:6543",
                "--bootstrap",
                "registry.demo.svc:9092",
                "--registry-poll",
                "250ms",
                "--cold-start-timeout",
                "30s",
                "--registry-replication-factor",
                "1",
                "--registry-topic-create-timeout",
                "15s",
                "--registry-reader-retry-backoff",
                "250ms",
                "--registry-fetch-max-wait",
                "500ms",
                "--registry-fetch-partition-max",
                "1MiB",
                "--registry-producer-dns-timeout",
                "10s",
                "--registry-reader-admin-dns-timeout",
                "10s",
                "--backend-endpoint-template",
                "{tenant}-gres.ns.svc:5432",
            ]
        );
    }

    #[test]
    fn activator_workload_renders_custom_policy() {
        let mut obj = gres();
        obj.spec.activator = Some(GresActivatorSpec {
            image: Some("example.test/activator:v2".into()),
            replicas: Some(4),
            registry_poll: Some(crabka_units::millis(600)),
            cold_start_timeout: Some(crabka_units::secs(40)),
            readiness_probe_period_seconds: Some(9),
        });

        let policy = crabka_gres_control::RegistryPolicy::new(
            2,
            crabka_units::millis(15_001),
            crabka_units::millis(251),
            crabka_units::millis(501),
            crabka_units::bytes(1_048_577),
        )
        .expect("policy")
        .with_producer_dns_timeout(crabka_units::millis(37))
        .expect("DNS timeout")
        .with_reader_admin_dns_timeout(crabka_units::millis(37))
        .expect("reader/admin DNS timeout");
        let deployment =
            render_activator_deployment(&obj, "registry.demo.svc:9092", "activator:test", &policy)
                .expect("render activator deployment");
        let spec = deployment.spec.expect("deployment spec");
        let container = &spec.template.spec.expect("pod spec").containers[0];

        assert!(spec.replicas == Some(4));
        assert!(
            container.args.as_ref().expect("activator args")
                == &[
                    "--listen",
                    "0.0.0.0:6543",
                    "--bootstrap",
                    "registry.demo.svc:9092",
                    "--registry-poll",
                    "600ms",
                    "--cold-start-timeout",
                    "40s",
                    "--registry-replication-factor",
                    "2",
                    "--registry-topic-create-timeout",
                    "15.001s",
                    "--registry-reader-retry-backoff",
                    "251ms",
                    "--registry-fetch-max-wait",
                    "501ms",
                    "--registry-fetch-partition-max",
                    "1048577B",
                    "--registry-producer-dns-timeout",
                    "37ms",
                    "--registry-reader-admin-dns-timeout",
                    "37ms",
                    "--backend-endpoint-template",
                    "{tenant}-gres.ns.svc:5432",
                ]
        );
        let args = container.args.as_ref().expect("activator args");
        assert!(
            args.iter()
                .filter(|arg| arg.as_str() == "--registry-producer-dns-timeout")
                .count()
                == 1
        );
        assert!(
            args.iter()
                .filter(|arg| { arg.as_str() == "--registry-reader-admin-dns-timeout" })
                .count()
                == 1
        );
        assert!(
            container
                .readiness_probe
                .as_ref()
                .and_then(|probe| probe.period_seconds)
                == Some(9)
        );
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

    #[test]
    fn pgdog_requeue_keeps_bootstrap_end_after_grace_has_elapsed() {
        let grace_deadline = 10_000;
        let now = grace_deadline + 1_000;

        assert!(
            next_pgdog_transition_requeue(
                [grace_deadline].into_iter(),
                now,
                millis(7_000),
                millis(60_000)
            ) == secs(6)
        );
    }

    #[test]
    fn pgdog_requeue_ignores_unrelated_fleet_deadlines() {
        let matching = tenant_with_phase("active", Some(11_000));
        let mut unrelated = tenant_with_phase("active", Some(2_000));
        unrelated.spec.gres = "other".into();

        assert!(
            pgdog_transition_requeue_for_tenants(
                &[matching, unrelated],
                "fleet",
                1_000,
                millis(7_000),
                millis(60_000),
            ) == secs(10)
        );
    }

    #[test]
    fn pgdog_requeue_uses_configured_fallback_without_future_deadlines() {
        assert!(
            next_pgdog_transition_requeue([].into_iter(), 1_000, millis(7_000), millis(1_234))
                == millis(1_234)
        );
    }

    #[test]
    fn pgdog_requeue_polls_before_a_later_grace_boundary() {
        assert!(
            next_pgdog_transition_requeue(
                [601_000].into_iter(),
                1_000,
                millis(7_000),
                millis(1_000)
            ) == secs(1)
        );
    }

    #[test]
    fn configured_pgdog_grace_drives_credential_retention_and_expiry() {
        let deadline = Some(10_000);

        assert!(needs_bootstrap_credential(
            TenantState::Active,
            deadline,
            16_999,
            millis(7_000)
        ));
        assert!(!needs_bootstrap_credential(
            TenantState::Active,
            deadline,
            17_000,
            millis(7_000)
        ));
        assert!(needs_bootstrap_credential(
            TenantState::Suspended,
            None,
            17_000,
            millis(7_000)
        ));
    }
}
