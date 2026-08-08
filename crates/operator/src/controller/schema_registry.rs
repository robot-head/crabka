//! `SchemaRegistry` reconciler.
//!
//! This reconciler renders a stateless Deployment, a headless Service, and a
//! `ClusterIP` Service for the `crabka-schema-registry` binary. The
//! `crabka.io/cluster` label associates them with a managed `Kafka`.

use std::{collections::BTreeMap, sync::Arc};

use crabka_units::{
    ByteSize, Time,
    convert::{ByteSizeExt as _, TimeExt as _},
    fmt::Human as _,
    secs,
};
use futures::StreamExt as _;
use k8s_openapi::api::{
    apps::v1::Deployment,
    core::v1::{Secret, Service},
};
use kube::{
    Resource, ResourceExt as _,
    api::{Api, DynamicObject, Patch, PatchParams},
    core::{ApiResource, GroupVersionKind},
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
        common::{
            self, FIELD_MANAGER, ReconcileError, apply_object, condition, owner_ref, patch_status,
        },
        topic::internal_listener_bootstrap,
    },
    crd::{
        BearerMode, CertManagerIssuerRef, Kafka, SchemaRegistry, SchemaRegistrySpec,
        SchemaRegistryStatus, TlsClientAuth,
    },
};

const APP_NAME: &str = "crabka-schema-registry";
const SR_PORT: i32 = 8081;
const DEFAULT_ELECTION_SESSION_TIMEOUT: Time = secs(10);
const DEFAULT_ELECTION_REBALANCE_TIMEOUT: Time = secs(30);
const DEFAULT_ELECTION_HEARTBEAT_INTERVAL: Time = secs(3);
const DEFAULT_IMAGE: &str = concat!(
    "ghcr.io/robot-head/crabka-schema-registry:",
    env!("CARGO_PKG_VERSION")
);

/// # Errors
///
/// Returns an error when cluster state cannot be loaded, the proposed plan is invalid, or a broker, Kubernetes, or persistence operation fails.
pub async fn run(ctx: Context) -> anyhow::Result<()> {
    let sr_api: Api<SchemaRegistry> = Api::all(ctx.client.clone());
    let kafka_api: Api<Kafka> = Api::all(ctx.client.clone());
    Controller::new(sr_api, watcher::Config::default())
        .watches(kafka_api, watcher::Config::default(), |_kafka| {
            Vec::<ObjectRef<SchemaRegistry>>::new().into_iter()
        })
        .run(reconcile, error_policy, Arc::new(ctx))
        .for_each(|res| async move {
            match res {
                Ok((obj, _)) => tracing::debug!(?obj, "schemaregistry reconciled"),
                Err(e) => tracing::warn!(error = %e, "schemaregistry reconcile error"),
            }
        })
        .await;
    Ok(())
}

pub fn error_policy(_obj: Arc<SchemaRegistry>, err: &ReconcileError, ctx: Arc<Context>) -> Action {
    tracing::warn!(error = %err, "schemaregistry reconcile error, requeueing");
    common::error_requeue(ctx)
}

/// Reconcile entry point. This function times the pass, records the reconcile
/// counter and histogram, and then delegates to the internal `reconcile_inner`
/// operation.
#[tracing::instrument(
    level = "info",
    skip_all,
    fields(
        kind = "SchemaRegistry",
        namespace = %obj.namespace().unwrap_or_else(|| "default".into()),
        name = %obj.name_any(),
        generation = ?obj.meta().generation,
    )
)]
/// # Errors
///
/// Returns an error when cluster state cannot be loaded, the proposed plan is invalid, or a broker, Kubernetes, or persistence operation fails.
pub async fn reconcile(
    obj: Arc<SchemaRegistry>,
    ctx: Arc<Context>,
) -> Result<Action, ReconcileError> {
    common::record_reconcile(
        &ctx,
        "SchemaRegistry",
        Box::pin(reconcile_inner(obj, ctx.clone())),
    )
    .await
}

async fn reconcile_inner(
    obj: Arc<SchemaRegistry>,
    ctx: Arc<Context>,
) -> Result<Action, ReconcileError> {
    let ns = obj.namespace().unwrap_or_else(|| "default".into());
    let name = obj.name_any();
    let sr_api: Api<SchemaRegistry> = Api::namespaced(ctx.client.clone(), &ns);

    if let Err(why) = validate_config(&obj.spec) {
        set_status(
            &sr_api,
            &name,
            &obj,
            "SchemaRegistryConfigInvalid",
            &why,
            None,
            None,
        )
        .await?;
        return Err(ReconcileError::SchemaRegistryConfigInvalid(why));
    }

    // 1. Cluster label (unless an explicit bootstrap override is set)
    let cluster = obj
        .meta()
        .labels
        .as_ref()
        .and_then(|l| l.get("crabka.io/cluster").cloned());

    // 2. Resolve bootstrap: spec override wins, else derive from the Kafka.
    let bootstrap = if let Some(b) = obj.spec.bootstrap_servers.clone() {
        Some(b)
    } else {
        let Some(cluster) = cluster.clone() else {
            set_status(
                &sr_api,
                &name,
                &obj,
                "MissingClusterLabel",
                "set metadata.labels[\"crabka.io/cluster\"] or spec.bootstrapServers",
                None,
                None,
            )
            .await?;
            return Ok(common::requeue(ctx.config.controller_drift_requeue));
        };
        let kafka_api: Api<Kafka> = Api::namespaced(ctx.client.clone(), &ns);
        let kafka = kafka_api.get_opt(&cluster).await?;
        kafka.as_ref().and_then(internal_listener_bootstrap)
    };
    let Some(bootstrap) = bootstrap else {
        set_status(
            &sr_api,
            &name,
            &obj,
            "KafkaNotReady",
            "referenced Kafka is not Ready or has no internal listener",
            None,
            None,
        )
        .await?;
        return Ok(common::requeue(ctx.config.controller_dependency_requeue));
    };

    // 3. Resolve TLS secret name (handles issuerRef path + mutual-exclusion).
    let tls_secret_name: Option<String> = if let Some(tls) = &obj.spec.tls {
        match (&tls.secret_name, &tls.issuer_ref) {
            (Some(_), Some(_)) => {
                set_status(
                    &sr_api,
                    &name,
                    &obj,
                    "InvalidSpec",
                    "spec.tls.secretName and spec.tls.issuerRef are mutually exclusive",
                    None,
                    None,
                )
                .await?;
                return Ok(common::requeue(ctx.config.controller_dependency_requeue));
            }
            (None, None) => {
                set_status(
                    &sr_api,
                    &name,
                    &obj,
                    "InvalidSpec",
                    "spec.tls must set either secretName or issuerRef",
                    None,
                    None,
                )
                .await?;
                return Ok(common::requeue(ctx.config.controller_dependency_requeue));
            }
            (Some(sn), None) => Some(sn.clone()),
            (None, Some(issuer)) => {
                let cert_secret = format!("{name}-sr-tls");
                apply_certificate_cr(&ctx.client, &ns, &name, &cert_secret, issuer, &obj).await?;
                // Gate: wait for cert-manager to provision the Secret.
                let secret_api: Api<Secret> = Api::namespaced(ctx.client.clone(), &ns);
                if secret_api.get_opt(&cert_secret).await?.is_none() {
                    set_status(
                        &sr_api,
                        &name,
                        &obj,
                        "WaitingForCert",
                        &format!("waiting for cert-manager to provision Secret {cert_secret}"),
                        None,
                        None,
                    )
                    .await?;
                    return Ok(common::requeue(ctx.config.controller_certificate_requeue));
                }
                Some(cert_secret)
            }
        }
    } else {
        None
    };

    // 4. Render + apply children (Deployment + 2 Services).
    let svc_api: Api<Service> = Api::namespaced(ctx.client.clone(), &ns);
    let dep_api: Api<Deployment> = Api::namespaced(ctx.client.clone(), &ns);

    let headless = render_headless_service(&obj)?;
    apply_object(&svc_api, &headless_name(&name), &headless).await?;
    let clusterip = render_clusterip_service(&obj)?;
    apply_object(&svc_api, &service_name(&name), &clusterip).await?;
    let image = obj
        .spec
        .image
        .clone()
        .or_else(|| ctx.config.default_schema_registry_image.clone())
        .unwrap_or_else(|| DEFAULT_IMAGE.to_string());
    let deployment = render_deployment(&obj, &bootstrap, &image, tls_secret_name.as_deref())?;
    apply_object(&dep_api, &deployment_name(&name), &deployment).await?;

    // 5. Status from the live Deployment.
    let live = dep_api.get_opt(&deployment_name(&name)).await?;
    let (replicas, ready) = live
        .as_ref()
        .and_then(|d| d.status.as_ref())
        .map_or((None, None), |s| (s.replicas, s.ready_replicas));
    let desired = obj.spec.replicas;
    let scheme = if tls_secret_name.is_some() {
        "https"
    } else {
        "http"
    };
    let url = format!(
        "{}://{}.{}.svc.cluster.local:{SR_PORT}",
        scheme,
        service_name(&name),
        ns
    );
    if ready.unwrap_or(0) >= desired {
        set_status(
            &sr_api,
            &name,
            &obj,
            "Available",
            &format!("{desired} replica(s) ready"),
            Some((replicas, ready)),
            Some(url),
        )
        .await?;
    } else {
        set_status(
            &sr_api,
            &name,
            &obj,
            "Progressing",
            &format!("{}/{desired} replica(s) ready", ready.unwrap_or(0)),
            Some((replicas, ready)),
            Some(url),
        )
        .await?;
    }
    Ok(common::requeue(ctx.config.controller_drift_requeue))
}

fn deployment_name(n: &str) -> String {
    format!("{n}-sr")
}
fn service_name(n: &str) -> String {
    format!("{n}-sr")
}
fn headless_name(n: &str) -> String {
    format!("{n}-sr-headless")
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

fn validate_protocol_bytes_i32(value: Option<ByteSize>, path: &str) -> Result<(), String> {
    let Some(value) = value else {
        return Ok(());
    };
    let bytes = value.bytes_u64();
    if !value.bytes_f64().is_finite() || ByteSize::from_bytes(bytes) != value {
        return Err(format!(
            "{path}: must be a positive whole number of bytes within 1..=i32::MAX"
        ));
    }
    let bytes = i32::try_from(bytes).map_err(|_| {
        format!("{path}: must be a positive whole number of bytes within 1..=i32::MAX")
    })?;
    refined_type::rule::GreaterI32::<0>::new(bytes)
        .map(|_| ())
        .map_err(|_| {
            format!("{path}: must be a positive whole number of bytes within 1..=i32::MAX")
        })
}

fn validate_config(spec: &SchemaRegistrySpec) -> Result<(), String> {
    macro_rules! validate {
        ($value:expr, $rule:ty, $path:literal) => {
            if let Some(value) = $value {
                <$rule>::new(value).map_err(|error| format!("{}: {error}", $path))?;
            }
        };
    }

    validate!(
        spec.schemas_topic_replication_factor,
        refined_type::rule::GreaterI32<0>,
        "spec.schemasTopicReplicationFactor"
    );
    if let Some(client_id) = &spec.client_id {
        refined_type::rule::NonEmptyString::new(client_id.clone())
            .map_err(|error| format!("spec.clientId: {error}"))?;
    }
    if let Some(runtime) = &spec.runtime {
        runtime
            .client_dispatch_queue_capacity
            .map(crabka_client_core::ConnectionDispatchQueueCapacity::new)
            .transpose()
            .map_err(|error| format!("spec.runtime.clientDispatchQueueCapacity: {error}"))?;
        runtime
            .client_frame_max
            .map(crabka_client_core::ClientFrameMax::try_from)
            .transpose()
            .map_err(|error| format!("spec.runtime.clientFrameMax: {error}"))?;
        validate_protocol_millis_i32(
            runtime.store_reader_fetch_max_wait,
            "spec.runtime.storeReaderFetchMaxWait",
        )?;
        validate_protocol_bytes_i32(
            runtime.store_reader_fetch_max,
            "spec.runtime.storeReaderFetchMax",
        )?;
        validate_protocol_millis_i32(
            runtime.schemas_topic_create_timeout,
            "spec.runtime.schemasTopicCreateTimeout",
        )?;
        for (value, path) in [
            (
                runtime.election_session_timeout,
                "spec.runtime.electionSessionTimeout",
            ),
            (
                runtime.election_rebalance_timeout,
                "spec.runtime.electionRebalanceTimeout",
            ),
            (
                runtime.election_heartbeat_interval,
                "spec.runtime.electionHeartbeatInterval",
            ),
            (
                runtime.election_reconnect_backoff,
                "spec.runtime.electionReconnectBackoff",
            ),
            (
                runtime.store_reader_retry_backoff,
                "spec.runtime.storeReaderRetryBackoff",
            ),
        ] {
            if value.is_some_and(|value| {
                !value.secs_f64().is_finite()
                    || value <= <Time as crabka_units::convert::TimeExt>::ZERO
            }) {
                return Err(format!("{path}: must be positive"));
            }
        }
        if runtime.forward_max_body.is_some_and(|value| {
            !value.bytes_f64().is_finite()
                || value <= <ByteSize as crabka_units::convert::ByteSizeExt>::ZERO
        }) {
            return Err("spec.runtime.forwardMaxBody: must be positive".into());
        }

        let session = runtime
            .election_session_timeout
            .unwrap_or(DEFAULT_ELECTION_SESSION_TIMEOUT);
        let rebalance = runtime
            .election_rebalance_timeout
            .unwrap_or(DEFAULT_ELECTION_REBALANCE_TIMEOUT);
        let heartbeat = runtime
            .election_heartbeat_interval
            .unwrap_or(DEFAULT_ELECTION_HEARTBEAT_INTERVAL);
        if heartbeat >= session {
            return Err(
                "spec.runtime.electionHeartbeatInterval must be below electionSessionTimeout"
                    .into(),
            );
        }
        if session > rebalance {
            return Err(
                "spec.runtime.electionSessionTimeout must not exceed electionRebalanceTimeout"
                    .into(),
            );
        }
        if let Some(level) = &runtime.default_compatibility_level
            && !matches!(
                level.as_str(),
                "NONE"
                    | "BACKWARD"
                    | "BACKWARD_TRANSITIVE"
                    | "FORWARD"
                    | "FORWARD_TRANSITIVE"
                    | "FULL"
                    | "FULL_TRANSITIVE"
            )
        {
            return Err("spec.runtime.defaultCompatibilityLevel is invalid".into());
        }
        if let Some(mode) = &runtime.default_mode
            && !matches!(mode.as_str(), "READWRITE" | "READONLY" | "IMPORT")
        {
            return Err("spec.runtime.defaultMode is invalid".into());
        }
    }
    if let Some(health) = &spec.health_checks {
        validate!(
            health.readiness_initial_delay_seconds,
            refined_type::rule::GreaterEqualI32<0>,
            "spec.healthChecks.readinessInitialDelaySeconds"
        );
        validate!(
            health.readiness_period_seconds,
            refined_type::rule::GreaterI32<0>,
            "spec.healthChecks.readinessPeriodSeconds"
        );
        validate!(
            health.liveness_initial_delay_seconds,
            refined_type::rule::GreaterEqualI32<0>,
            "spec.healthChecks.livenessInitialDelaySeconds"
        );
        validate!(
            health.liveness_period_seconds,
            refined_type::rule::GreaterI32<0>,
            "spec.healthChecks.livenessPeriodSeconds"
        );
    }
    if let Some(refresh) = spec
        .authentication
        .as_ref()
        .and_then(|authn| authn.bearer.as_ref())
        .and_then(|bearer| bearer.jwks_refresh)
        && (!refresh.secs_f64().is_finite()
            || refresh <= <Time as crabka_units::convert::TimeExt>::ZERO)
    {
        return Err("spec.authentication.bearer.jwksRefresh: must be positive".into());
    }
    if let Some(refresh) = spec
        .authorization
        .as_ref()
        .and_then(|authz| authz.acl_refresh)
        && (!refresh.secs_f64().is_finite()
            || refresh <= <Time as crabka_units::convert::TimeExt>::ZERO)
    {
        return Err("spec.authorization.aclRefresh: must be positive".into());
    }
    Ok(())
}

/// Stable label set for the Deployment `selector.matchLabels`, the pod
/// template labels, and BOTH Services' `spec.selector`. Deployment selectors
/// are immutable, so this map must NOT carry the version label. A value that
/// churns there would make the Deployment un-updatable, and a mismatch between
/// the selector and the template would keep pods from becoming Ready.
fn selector_labels(obj: &SchemaRegistry) -> BTreeMap<String, String> {
    let instance = obj.name_any();
    let mut m = BTreeMap::new();
    m.insert("app.kubernetes.io/name".into(), APP_NAME.into());
    m.insert("app.kubernetes.io/instance".into(), instance);
    m.insert(
        "app.kubernetes.io/component".into(),
        "schema-registry".into(),
    );
    m
}

/// Metadata labels for the rendered objects: the stable selector labels plus
/// the version and managed-by metadata labels. The operator uses them only for
/// `metadata.labels`, never for selectors.
fn meta_labels(obj: &SchemaRegistry) -> BTreeMap<String, String> {
    let mut m = selector_labels(obj);
    m.insert("app.kubernetes.io/version".into(), "0.1.1".into());
    m.insert(
        "app.kubernetes.io/managed-by".into(),
        "crabka-operator".into(),
    );
    m
}

fn render_headless_service(obj: &SchemaRegistry) -> Result<Service, ReconcileError> {
    let name = obj.name_any();
    let svc = serde_json::from_value(json!({
        "metadata": {
            "name": headless_name(&name),
            "namespace": obj.meta().namespace.clone(),
            "labels": meta_labels(obj),
            "ownerReferences": [owner_ref::<SchemaRegistry>(obj)?],
        },
        "spec": {
            "clusterIP": "None",
            "selector": selector_labels(obj),
            "ports": [{ "name": "rest", "port": SR_PORT, "protocol": "TCP", "targetPort": SR_PORT }],
        }
    }))?;
    Ok(svc)
}

fn render_clusterip_service(obj: &SchemaRegistry) -> Result<Service, ReconcileError> {
    let name = obj.name_any();
    let svc = serde_json::from_value(json!({
        "metadata": {
            "name": service_name(&name),
            "namespace": obj.meta().namespace.clone(),
            "labels": meta_labels(obj),
            "ownerReferences": [owner_ref::<SchemaRegistry>(obj)?],
        },
        "spec": {
            "type": "ClusterIP",
            "selector": selector_labels(obj),
            "ports": [{ "name": "rest", "port": SR_PORT, "protocol": "TCP", "targetPort": SR_PORT }],
        }
    }))?;
    Ok(svc)
}

fn render_deployment(
    obj: &SchemaRegistry,
    bootstrap: &str,
    image: &str,
    tls_secret: Option<&str>,
) -> Result<Deployment, ReconcileError> {
    let name = obj.name_any();
    let ns = obj
        .meta()
        .namespace
        .clone()
        .unwrap_or_else(|| "default".into());
    let selector = selector_labels(obj);
    let (args, volumes, mounts, extra_env) = build_args_and_mounts(obj, bootstrap, tls_secret);
    let scheme = if tls_secret.is_some() {
        "https"
    } else {
        "http"
    };
    let advertised = format!(
        "{}://$(POD_NAME).{}.{}.svc.cluster.local:{SR_PORT}",
        scheme,
        headless_name(&name),
        ns
    );
    let health = obj.spec.health_checks.as_ref();
    let readiness_initial_delay_seconds = health
        .and_then(|checks| checks.readiness_initial_delay_seconds)
        .unwrap_or(2);
    let readiness_period_seconds = health
        .and_then(|checks| checks.readiness_period_seconds)
        .unwrap_or(5);
    let liveness_initial_delay_seconds = health
        .and_then(|checks| checks.liveness_initial_delay_seconds)
        .unwrap_or(5);
    let liveness_period_seconds = health
        .and_then(|checks| checks.liveness_period_seconds)
        .unwrap_or(10);
    let mut env = vec![
        json!({ "name": "POD_NAME", "valueFrom": { "fieldRef": { "fieldPath": "metadata.name" } } }),
        json!({ "name": "SCHEMA_REGISTRY_ADVERTISED_URL", "value": advertised }),
    ];
    env.extend(extra_env);
    let dep = serde_json::from_value(json!({
        "metadata": {
            "name": deployment_name(&name),
            "namespace": obj.meta().namespace.clone(),
            "labels": meta_labels(obj),
            "ownerReferences": [owner_ref::<SchemaRegistry>(obj)?],
        },
        "spec": {
            "replicas": obj.spec.replicas,
            "selector": { "matchLabels": selector },
            "template": {
                "metadata": { "labels": selector },
                "spec": {
                    "securityContext": { "runAsNonRoot": true, "runAsUser": 65532, "fsGroup": 65532 },
                    "volumes": volumes,
                    "containers": [{
                        "name": "schema-registry",
                        "image": image,
                        "args": args,
                        "env": env,
                        "ports": [{ "name": "rest", "containerPort": SR_PORT, "protocol": "TCP" }],
                        "volumeMounts": mounts,
                        "readinessProbe": {
                            "tcpSocket": { "port": SR_PORT },
                            "initialDelaySeconds": readiness_initial_delay_seconds,
                            "periodSeconds": readiness_period_seconds,
                        },
                        "livenessProbe": {
                            "tcpSocket": { "port": SR_PORT },
                            "initialDelaySeconds": liveness_initial_delay_seconds,
                            "periodSeconds": liveness_period_seconds,
                        },
                        "resources": obj.spec.resources.clone().unwrap_or_default(),
                    }],
                }
            }
        }
    }))?;
    Ok(dep)
}

/// Build the container args and the Secret volumes and mounts from the spec.
/// Non-secret config becomes args. Credentials become mounted Secret files
/// that path args reference. Returns `args`, `volumes`, `volumeMounts`, and
/// `extra_env` as JSON values.
fn build_args_and_mounts(
    obj: &SchemaRegistry,
    bootstrap: &str,
    tls_secret: Option<&str>,
) -> (
    Vec<String>,
    Vec<serde_json::Value>,
    Vec<serde_json::Value>,
    Vec<serde_json::Value>,
) {
    let s = &obj.spec;
    // The SR binary has no subcommand — args are flags only. (apko's
    // `cmd: run` default is replaced by the container `args` set here.)
    let mut a: Vec<String> = Vec::new();
    a.push(format!("--bootstrap-servers={bootstrap}"));
    a.push(format!("--listen-addr=0.0.0.0:{SR_PORT}"));
    if let Some(t) = &s.schemas_topic {
        a.push(format!("--schemas-topic={t}"));
    }
    if let Some(rf) = s.schemas_topic_replication_factor {
        a.push(format!("--schemas-topic-rf={rf}"));
    }
    if let Some(g) = &s.group_id {
        a.push(format!("--group-id={g}"));
    }
    if let Some(runtime) = &s.runtime {
        macro_rules! push_runtime {
            ($field:ident) => {
                if let Some(value) = &runtime.$field {
                    a.push(format!(
                        "--{}={value}",
                        stringify!($field).replace('_', "-")
                    ));
                }
            };
            (quantity $field:ident) => {
                if let Some(value) = runtime.$field {
                    a.push(format!(
                        "--{}={}",
                        stringify!($field).replace('_', "-"),
                        value.human()
                    ));
                }
            };
        }
        push_runtime!(quantity election_session_timeout);
        push_runtime!(quantity election_rebalance_timeout);
        push_runtime!(quantity election_heartbeat_interval);
        push_runtime!(quantity election_reconnect_backoff);
        push_runtime!(quantity store_reader_retry_backoff);
        push_runtime!(quantity store_reader_fetch_max_wait);
        push_runtime!(quantity store_reader_fetch_max);
        push_runtime!(quantity schemas_topic_create_timeout);
        push_runtime!(quantity forward_max_body);
        push_runtime!(default_compatibility_level);
        push_runtime!(default_mode);
        if let Some(value) = runtime.client_dispatch_queue_capacity {
            a.push(format!("--client-dispatch-queue-capacity={value}"));
        }
        if let Some(value) = runtime.client_frame_max {
            a.push(format!("--client-frame-max={}B", value.bytes_u64()));
        }
    }
    if let Some(client_id) = &s.client_id {
        a.push(format!("--client-id={client_id}"));
    }

    let mut volumes = Vec::new();
    let mut mounts = Vec::new();
    let mut extra_env: Vec<serde_json::Value> = Vec::new();

    // Server TLS — use the resolved tls_secret param instead of tls.secret_name
    if let Some(tls) = &s.tls {
        if let Some(sn) = tls_secret {
            a.push("--tls-cert=/etc/sr/tls/tls.crt".into());
            a.push("--tls-key=/etc/sr/tls/tls.key".into());
            volumes.push(json!({ "name": "tls", "secret": { "secretName": sn } }));
            mounts.push(json!({ "name": "tls", "mountPath": "/etc/sr/tls", "readOnly": true }));
        }
        // client_auth and client_ca still read from tls struct (unchanged)
        let mode = match tls.client_auth.unwrap_or(TlsClientAuth::Disabled) {
            TlsClientAuth::Disabled => "disabled",
            TlsClientAuth::Optional => "optional",
            TlsClientAuth::Required => "required",
        };
        a.push(format!("--tls-client-auth={mode}"));
        if let Some(ca) = &tls.client_ca_secret_name {
            a.push("--tls-client-ca=/etc/sr/client-ca/ca.crt".into());
            volumes.push(json!({ "name": "client-ca", "secret": { "secretName": ca } }));
            mounts.push(
                json!({ "name": "client-ca", "mountPath": "/etc/sr/client-ca", "readOnly": true }),
            );
        }
    }

    // Authentication
    if let Some(authn) = &s.authentication {
        if authn.require_auth {
            a.push("--require-auth".into());
        }
        if let Some(r) = &authn.realm {
            a.push(format!("--realm={r}"));
        }
        if let Some(b) = &authn.basic {
            let key = b.users_secret_key.clone().unwrap_or_else(|| "users".into());
            a.push("--basic-auth-file=/etc/sr/basic/users".into());
            volumes.push(json!({ "name": "basic", "secret": {
                "secretName": b.users_secret_name,
                "items": [{ "key": key, "path": "users" }]
            }}));
            mounts.push(json!({ "name": "basic", "mountPath": "/etc/sr/basic", "readOnly": true }));
        }
        if let Some(bearer) = &authn.bearer {
            match bearer.mode {
                BearerMode::Unsecured => {
                    a.push("--bearer=unsecured".into());
                    if let Some(pc) = &bearer.principal_claim {
                        a.push(format!("--bearer-principal-claim={pc}"));
                    }
                }
                BearerMode::Jwks => {
                    a.push("--bearer=jwks".into());
                    if let Some(uri) = &bearer.jwks_endpoint_uri {
                        a.push(format!("--bearer-jwks-endpoint-uri={uri}"));
                    }
                    if let Some(iss) = &bearer.jwks_valid_issuer {
                        a.push(format!("--bearer-jwks-valid-issuer={iss}"));
                    }
                    if let Some(aud) = &bearer.jwks_expected_audience {
                        a.push(format!("--bearer-jwks-expected-audience={aud}"));
                    }
                    if let Some(pc) = bearer
                        .jwks_principal_claim
                        .as_ref()
                        .or(bearer.principal_claim.as_ref())
                    {
                        a.push(format!("--bearer-jwks-principal-claim={pc}"));
                    }
                    if let Some(refresh) = bearer.jwks_refresh {
                        a.push(format!("--bearer-jwks-refresh={}", refresh.human()));
                    }
                    if let Some(ca_sn) = &bearer.jwks_tls_secret_name {
                        a.push("--bearer-jwks-ca=/etc/sr/jwks-ca/ca.crt".into());
                        volumes
                            .push(json!({ "name": "jwks-ca", "secret": { "secretName": ca_sn } }));
                        mounts.push(
                            json!({ "name": "jwks-ca", "mountPath": "/etc/sr/jwks-ca", "readOnly": true }),
                        );
                    }
                }
            }
        }
    }

    // Authorization
    if let Some(az) = &s.authorization {
        if az.enabled {
            a.push("--authz".into());
        }
        for u in &az.super_users {
            a.push(format!("--super-user={u}"));
        }
        if let Some(refresh) = az.acl_refresh {
            a.push(format!("--acl-refresh={}", refresh.human()));
        }
    }

    // SR → broker kafka-client security
    if let Some(kc) = &s.kafka_client {
        let proto = kc.security_protocol.as_deref().unwrap_or("PLAINTEXT");
        a.push(format!("--kafka-security-protocol={proto}"));
        if let Some(sasl) = &kc.sasl {
            a.push(format!("--kafka-sasl-mechanism={}", sasl.mechanism));
            extra_env.push(json!({
                "name": "SCHEMA_REGISTRY_KAFKA_SASL_USERNAME",
                "valueFrom": { "secretKeyRef": { "name": sasl.secret_ref, "key": "username" } }
            }));
            extra_env.push(json!({
                "name": "SCHEMA_REGISTRY_KAFKA_SASL_PASSWORD",
                "valueFrom": { "secretKeyRef": { "name": sasl.secret_ref, "key": "password" } }
            }));
        }
        if let Some(tls) = &kc.tls {
            if tls.ca_secret_name.is_some() {
                a.push("--kafka-tls-ca=/etc/sr/kafka-tls/ca.crt".into());
            }
            if let Some(sn) = &tls.server_name_override {
                a.push(format!("--kafka-tls-server-name={sn}"));
            }
            if let Some(ca_sn) = &tls.ca_secret_name {
                volumes.push(json!({ "name": "kafka-tls", "secret": { "secretName": ca_sn } }));
                mounts.push(
                    json!({ "name": "kafka-tls", "mountPath": "/etc/sr/kafka-tls", "readOnly": true }),
                );
            }
        }
    }

    (a, volumes, mounts, extra_env)
}

/// SSA-apply a `cert-manager.io/v1` `Certificate` CR with [`DynamicObject`].
/// This avoids a dependency on a cert-manager Rust crate, and it is the same
/// pattern as `metrics.rs`.
///
/// The DNS SANs are the per-pod headless DNS and the `ClusterIP` service DNS.
#[tracing::instrument(
    level = "info",
    skip_all,
    fields(name = %name, namespace = %ns, cert_secret = %cert_secret_name),
    err,
)]
async fn apply_certificate_cr(
    client: &kube::Client,
    ns: &str,
    name: &str,
    cert_secret_name: &str,
    issuer: &CertManagerIssuerRef,
    owner: &SchemaRegistry,
) -> Result<(), ReconcileError> {
    let kind = issuer.kind.as_deref().unwrap_or("Issuer");
    let group = issuer.group.as_deref().unwrap_or("cert-manager.io");
    let cert_name = format!("{name}-sr");
    let body = serde_json::json!({
        "apiVersion": "cert-manager.io/v1",
        "kind": "Certificate",
        "metadata": {
            "name": cert_name,
            "namespace": ns,
            "ownerReferences": [owner_ref::<SchemaRegistry>(owner)?],
        },
        "spec": {
            "secretName": cert_secret_name,
            "issuerRef": {
                "name": issuer.name,
                "kind": kind,
                "group": group,
            },
            "dnsNames": [
                format!("*.{}.{}.svc.cluster.local", headless_name(name), ns),
                format!("{}.{}.svc.cluster.local", service_name(name), ns),
            ],
        }
    });
    let gvk = GroupVersionKind::gvk("cert-manager.io", "v1", "Certificate");
    let ar = ApiResource::from_gvk_with_plural(&gvk, "certificates");
    let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), ns, &ar);
    let obj: DynamicObject = serde_json::from_value(body)?;
    let pp = PatchParams::apply(FIELD_MANAGER).force();
    match api.patch(&cert_name, &pp, &Patch::Apply(&obj)).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(status)) if status.code == 404 => Err(ReconcileError::Malformed(
            "cert-manager Certificate CRD not installed".into(),
        )),
        Err(e) => Err(e.into()),
    }
}

/// Patch status with one rolled-up `Ready` condition and a `KafkaReady`
/// condition. `reason == "Available"` means Ready=True.
#[tracing::instrument(level = "info", skip_all, fields(name = %name, reason = %reason), err)]
async fn set_status(
    api: &Api<SchemaRegistry>,
    name: &str,
    obj: &SchemaRegistry,
    reason: &str,
    message: &str,
    counts: Option<(Option<i32>, Option<i32>)>,
    url: Option<String>,
) -> Result<(), ReconcileError> {
    let kafka_ok = !matches!(reason, "MissingClusterLabel" | "KafkaNotReady");
    let ready = if reason == "Available" {
        "True"
    } else {
        "False"
    };
    let (replicas, ready_replicas) = counts.unwrap_or((None, None));
    let observed_generation = if ready == "True" {
        obj.meta().generation
    } else {
        obj.status.as_ref().and_then(|s| s.observed_generation)
    };
    let status = SchemaRegistryStatus {
        conditions: vec![
            condition(
                "KafkaReady",
                if kafka_ok { "True" } else { "False" },
                reason,
                message,
            ),
            condition("Ready", ready, reason, message),
        ],
        observed_generation,
        replicas,
        ready_replicas,
        url,
    };
    patch_status(api, name, status).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configured_client_policy_renders_once() {
        let mut registry = SchemaRegistry::new(
            "registry",
            SchemaRegistrySpec {
                replicas: 1,
                image: None,
                bootstrap_servers: None,
                schemas_topic: None,
                schemas_topic_replication_factor: None,
                group_id: None,
                runtime: None,
                client_id: None,
                health_checks: None,
                kafka_client: None,
                tls: None,
                authentication: None,
                authorization: None,
                resources: None,
            },
        );
        registry.spec.runtime = Some(crate::crd::SchemaRegistryRuntime {
            client_dispatch_queue_capacity: Some(7),
            client_frame_max: Some(crabka_units::kibibytes(32)),
            ..crate::crd::SchemaRegistryRuntime::default()
        });

        let (args, _, _, _) = build_args_and_mounts(&registry, "boot:9092", None);
        assert_eq!(
            args.iter()
                .filter(|arg| *arg == "--client-dispatch-queue-capacity=7")
                .count(),
            1
        );
        assert_eq!(
            args.iter()
                .filter(|arg| *arg == "--client-frame-max=32768B")
                .count(),
            1
        );

        registry.spec.runtime = None;
        let (omitted, _, _, _) = build_args_and_mounts(&registry, "boot:9092", None);
        assert!(
            omitted.iter().all(|arg| {
                !arg.starts_with("--client-dispatch-queue-capacity=")
                    && !arg.starts_with("--client-frame-max=")
            }),
            "got: {omitted:?}"
        );
    }

    #[test]
    fn client_policy_validation_rejects_invalid_boundaries() {
        for (runtime, path) in [
            (
                serde_json::json!({"clientDispatchQueueCapacity": 0}),
                "spec.runtime.clientDispatchQueueCapacity",
            ),
            (
                serde_json::json!({"clientFrameMax": "0B"}),
                "spec.runtime.clientFrameMax",
            ),
            (
                serde_json::json!({"clientFrameMax": "1.5B"}),
                "spec.runtime.clientFrameMax",
            ),
            (
                serde_json::json!({"clientFrameMax": "101MiB"}),
                "spec.runtime.clientFrameMax",
            ),
        ] {
            let spec: SchemaRegistrySpec = serde_json::from_value(serde_json::json!({
                "replicas": 1,
                "runtime": runtime,
            }))
            .unwrap();
            let error = validate_config(&spec).expect_err("reject invalid policy");
            assert!(error.contains(path), "got: {error}");
        }
    }

    #[test]
    fn protocol_runtime_validation_rejects_lossy_lowering() {
        for value in [
            serde_json::json!({
                "replicas": 1,
                "runtime": {"storeReaderFetchMaxWait": "0.5ms"}
            }),
            serde_json::json!({
                "replicas": 1,
                "runtime": {"storeReaderFetchMax": "0.5B"}
            }),
            serde_json::json!({
                "replicas": 1,
                "runtime": {"schemasTopicCreateTimeout": "0.5ms"}
            }),
            serde_json::json!({
                "replicas": 1,
                "runtime": {"storeReaderFetchMaxWait": "2147483648ms"}
            }),
            serde_json::json!({
                "replicas": 1,
                "runtime": {"storeReaderFetchMax": "2147483648B"}
            }),
            serde_json::json!({
                "replicas": 1,
                "runtime": {"schemasTopicCreateTimeout": "2147483648ms"}
            }),
        ] {
            let spec: SchemaRegistrySpec = serde_json::from_value(value.clone()).unwrap();
            assert!(validate_config(&spec).is_err(), "accepted invalid {value}");
        }

        for value in [
            serde_json::json!({"replicas": 1}),
            serde_json::json!({
                "replicas": 1,
                "runtime": {
                    "storeReaderFetchMaxWait": "2147483647ms",
                    "storeReaderFetchMax": "2147483647B",
                    "schemasTopicCreateTimeout": "2147483647ms"
                }
            }),
        ] {
            let spec: SchemaRegistrySpec = serde_json::from_value(value.clone()).unwrap();
            assert!(validate_config(&spec).is_ok(), "rejected valid {value}");
        }
    }
}
