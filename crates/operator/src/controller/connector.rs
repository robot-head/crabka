//! Reconciler for one `PostgreSQL` `CDC` worker per `KafkaConnector`.

use std::{collections::BTreeMap, sync::Arc};

use futures::StreamExt as _;
use k8s_openapi::api::{apps::v1::Deployment, core::v1::Secret};
use kube::{
    Resource, ResourceExt as _,
    api::{Api, Patch, PatchParams},
    runtime::{
        controller::{Action, Controller},
        watcher,
    },
};
use serde_json::json;
use sha2::{Digest as _, Sha256};

use crate::{
    context::Context,
    controller::{
        cluster_ca::cluster_ca_cert_name,
        common::{self, ReconcileError, apply_object, condition, owner_ref, patch_status},
    },
    crd::{
        AclOp, AclPatternType, AclPermission, AclResource, AclResourceKind, AclRule,
        Authentication, Kafka, KafkaConnector, KafkaConnectorSpec, KafkaConnectorStatus, KafkaUser,
        KafkaUserSpec,
        user::{Authorization, SimpleAuthorization, TlsAuth},
    },
};

const APP_NAME: &str = "crabka-connect-worker";
const HEALTH_PORT: i32 = 8080;
const HEALTH_ADDR: &str = "0.0.0.0:8080";
const DEFAULT_IMAGE: &str = concat!(
    "ghcr.io/robot-head/crabka-connect-worker:",
    env!("CARGO_PKG_VERSION")
);
const BROKER_CLIENT_DIR: &str = "/etc/crabka/broker-client";
const BROKER_CA_DIR: &str = "/etc/crabka/broker-ca";

/// Run the connector controller until its watch stream closes.
///
/// # Errors
///
/// Returns an error when Kubernetes watch setup or streaming fails.
pub async fn run(ctx: Context) -> anyhow::Result<()> {
    let connectors: Api<KafkaConnector> = Api::all(ctx.client.clone());
    let deployments: Api<Deployment> = Api::all(ctx.client.clone());
    let users: Api<KafkaUser> = Api::all(ctx.client.clone());

    Controller::new(connectors, watcher::Config::default())
        .owns(deployments, watcher::Config::default())
        .owns(users, watcher::Config::default())
        .run(reconcile, error_policy, Arc::new(ctx))
        .for_each(|result| async move {
            match result {
                Ok((object, _)) => tracing::debug!(?object, "connector reconciled"),
                Err(error) => tracing::warn!(%error, "connector reconcile error"),
            }
        })
        .await;
    Ok(())
}

pub fn error_policy(
    _obj: Arc<KafkaConnector>,
    error: &ReconcileError,
    ctx: Arc<Context>,
) -> Action {
    tracing::warn!(%error, "connector reconcile error, requeueing");
    common::error_requeue(ctx)
}

/// Reconcile a connector and its worker Deployment.
///
/// # Errors
///
/// Returns an error for Kubernetes API and rendering failures.
pub async fn reconcile(
    obj: Arc<KafkaConnector>,
    ctx: Arc<Context>,
) -> Result<Action, ReconcileError> {
    common::record_reconcile(
        &ctx,
        "KafkaConnector",
        Box::pin(reconcile_inner(obj, ctx.clone())),
    )
    .await
}

async fn reconcile_inner(
    obj: Arc<KafkaConnector>,
    ctx: Arc<Context>,
) -> Result<Action, ReconcileError> {
    let namespace = obj.namespace().unwrap_or_else(|| "default".into());
    let name = obj.name_any();
    let connector_api: Api<KafkaConnector> = Api::namespaced(ctx.client.clone(), &namespace);

    if let Err(message) = validate_spec(&obj.spec) {
        patch_state(
            &connector_api,
            &obj,
            ("InvalidSpec", &message, true, None, None, false),
        )
        .await?;
        return Ok(common::requeue(ctx.config.controller_invalid_requeue));
    }

    let deployment_api: Api<Deployment> = Api::namespaced(ctx.client.clone(), &namespace);
    if obj.spec.paused {
        let live = deployment_api.get_opt(&name).await?;
        let owned = live.as_ref().is_some_and(|deployment| {
            deployment
                .metadata
                .owner_references
                .as_ref()
                .is_some_and(|owners| {
                    owners.iter().any(|owner| {
                        owner.kind == "KafkaConnector"
                            && obj.meta().uid.as_ref() == Some(&owner.uid)
                    })
                })
        });
        let needs_scale_down = owned
            && live
                .as_ref()
                .and_then(|deployment| deployment.spec.as_ref())
                .and_then(|spec| spec.replicas)
                != Some(0);
        if needs_scale_down {
            deployment_api
                .patch(
                    &name,
                    &PatchParams::default(),
                    &Patch::Merge(&json!({ "spec": { "replicas": 0 } })),
                )
                .await?;
        }
        let ready = live
            .as_ref()
            .and_then(|deployment| deployment.status.as_ref())
            .and_then(|status| status.ready_replicas)
            .unwrap_or(0);
        patch_state(
            &connector_api,
            &obj,
            (
                "Paused",
                "connector is paused",
                false,
                Some(0),
                Some(ready),
                true,
            ),
        )
        .await?;
        return Ok(common::requeue(ctx.config.controller_drift_requeue));
    }

    let ResolvedInputs::Ready {
        cluster_name,
        bootstrap,
        broker_server_name,
        config_hash,
    } = resolve_inputs(&obj, &ctx, &connector_api, &namespace).await?
    else {
        return Ok(common::requeue(ctx.config.controller_dependency_requeue));
    };

    let image = obj
        .spec
        .image
        .as_deref()
        .or(ctx.config.default_connector_image.as_deref())
        .unwrap_or(DEFAULT_IMAGE);
    let deployment = render_deployment(
        &obj,
        &cluster_name,
        image,
        &bootstrap,
        &broker_server_name,
        &config_hash,
    )?;
    apply_object(&deployment_api, &name, &deployment).await?;

    let live = deployment_api.get_opt(&name).await?;
    let desired = 1;
    let ready = live
        .as_ref()
        .and_then(|deployment| deployment.status.as_ref())
        .and_then(|status| status.ready_replicas)
        .unwrap_or(0);
    if ready >= desired {
        patch_state(
            &connector_api,
            &obj,
            (
                "Available",
                "connector worker is ready",
                false,
                Some(desired),
                Some(ready),
                true,
            ),
        )
        .await?;
    } else {
        patch_state(
            &connector_api,
            &obj,
            (
                "Progressing",
                "waiting for connector worker readiness",
                false,
                Some(desired),
                Some(ready),
                true,
            ),
        )
        .await?;
    }
    Ok(common::requeue(ctx.config.controller_drift_requeue))
}

enum ResolvedInputs {
    Ready {
        cluster_name: String,
        bootstrap: String,
        broker_server_name: String,
        config_hash: String,
    },
    Requeue,
}

async fn resolve_inputs(
    obj: &KafkaConnector,
    ctx: &Context,
    connector_api: &Api<KafkaConnector>,
    namespace: &str,
) -> Result<ResolvedInputs, ReconcileError> {
    let Some(cluster_name) = obj
        .meta()
        .labels
        .as_ref()
        .and_then(|labels| labels.get("crabka.io/cluster"))
        .cloned()
    else {
        patch_unready(
            connector_api,
            obj,
            "MissingClusterLabel",
            "metadata.labels[\"crabka.io/cluster\"] is required",
            true,
        )
        .await?;
        return Ok(ResolvedInputs::Requeue);
    };
    let secret_api: Api<Secret> = Api::namespaced(ctx.client.clone(), namespace);
    let Some(database_secret) = secret_api.get_opt(&obj.spec.database_url.name).await? else {
        patch_unready(
            connector_api,
            obj,
            "DatabaseSecretNotFound",
            &format!("Secret '{}' not found", obj.spec.database_url.name),
            true,
        )
        .await?;
        return Ok(ResolvedInputs::Requeue);
    };
    let Some(database_url) = database_secret
        .data
        .as_ref()
        .and_then(|data| data.get(&obj.spec.database_url.key))
        .filter(|value| !value.0.is_empty())
    else {
        patch_unready(
            connector_api,
            obj,
            "DatabaseSecretInvalid",
            &format!(
                "Secret '{}' has no non-empty '{}' key",
                obj.spec.database_url.name, obj.spec.database_url.key
            ),
            true,
        )
        .await?;
        return Ok(ResolvedInputs::Requeue);
    };
    let kafka_api: Api<Kafka> = Api::namespaced(ctx.client.clone(), namespace);
    let Some(parent) = kafka_api.get_opt(&cluster_name).await? else {
        patch_unready(
            connector_api,
            obj,
            "ParentNotFound",
            &format!("Kafka '{cluster_name}' not found"),
            true,
        )
        .await?;
        return Ok(ResolvedInputs::Requeue);
    };
    let Some((bootstrap, broker_server_name)) = resolve_broker_endpoint(&parent, namespace) else {
        patch_unready(
            connector_api,
            obj,
            "KafkaNotReady",
            "referenced Kafka is not Ready or has no internal TLS listener with TLS authentication",
            false,
        )
        .await?;
        return Ok(ResolvedInputs::Requeue);
    };
    let user_name = broker_user_name(&obj.name_any());
    let user_api: Api<KafkaUser> = Api::namespaced(ctx.client.clone(), namespace);
    apply_object(
        &user_api,
        &user_name,
        &child_kafka_user(obj, &cluster_name)?,
    )
    .await?;
    let Some(client_secret) = secret_api.get_opt(&user_name).await? else {
        patch_unready(
            connector_api,
            obj,
            "WaitingForBrokerCredentials",
            &format!("waiting for KafkaUser '{user_name}' credentials"),
            false,
        )
        .await?;
        return Ok(ResolvedInputs::Requeue);
    };
    let ca_secret_name = cluster_ca_cert_name(&cluster_name);
    let ca_secret = secret_api.get_opt(&ca_secret_name).await?;
    if !secret_has_keys(&client_secret, &["user.crt", "user.key"])
        || !ca_secret
            .as_ref()
            .is_some_and(|secret| secret_has_keys(secret, &["ca.crt"]))
    {
        patch_unready(
            connector_api,
            obj,
            "WaitingForBrokerCredentials",
            "broker client or CA credentials are not ready",
            false,
        )
        .await?;
        return Ok(ResolvedInputs::Requeue);
    }
    let Some(ca_secret) = ca_secret.as_ref() else {
        return Ok(ResolvedInputs::Requeue);
    };
    Ok(ResolvedInputs::Ready {
        cluster_name,
        bootstrap,
        broker_server_name,
        config_hash: secret_hash(&[
            database_url.0.as_slice(),
            secret_value(&client_secret, "user.crt"),
            secret_value(&client_secret, "user.key"),
            secret_value(ca_secret, "ca.crt"),
        ]),
    })
}

async fn patch_unready(
    api: &Api<KafkaConnector>,
    obj: &KafkaConnector,
    reason: &str,
    message: &str,
    failed: bool,
) -> Result<(), ReconcileError> {
    patch_state(api, obj, (reason, message, failed, None, None, false)).await
}

fn validate_spec(spec: &KafkaConnectorSpec) -> Result<(), String> {
    for (path, value) in [
        ("spec.databaseUrl.name", spec.database_url.name.as_str()),
        ("spec.databaseUrl.key", spec.database_url.key.as_str()),
        ("spec.slot", spec.slot.as_str()),
        ("spec.publication", spec.publication.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(format!("{path} must not be empty"));
        }
    }
    if spec.tables.is_empty() {
        return Err("spec.tables must contain at least one table".into());
    }
    if spec.tables.iter().any(|table| table.trim().is_empty()) {
        return Err("spec.tables entries must not be empty".into());
    }
    if spec
        .schema
        .as_ref()
        .is_some_and(|value| value.trim().is_empty())
    {
        return Err("spec.schema must not be empty when set".into());
    }
    if spec
        .topic_prefix
        .as_ref()
        .is_some_and(|value| value.trim().is_empty())
    {
        return Err("spec.topicPrefix must not be empty when set".into());
    }
    if spec
        .image
        .as_ref()
        .is_some_and(|value| value.trim().is_empty())
    {
        return Err("spec.image must not be empty when set".into());
    }
    Ok(())
}

fn resolve_broker_endpoint(parent: &Kafka, namespace: &str) -> Option<(String, String)> {
    let status = parent.status.as_ref()?;
    if !status
        .conditions
        .iter()
        .any(|condition| condition.type_ == "Ready" && condition.status == "True")
    {
        return None;
    }
    let listener = parent.spec.listeners.iter().find(|listener| {
        listener.type_ == crate::crd::ListenerType::Internal
            && listener.tls
            && matches!(
                listener.authentication,
                Some(crate::crd::ListenerAuthentication::Tls)
            )
    })?;
    let bootstrap = status
        .listeners
        .iter()
        .find(|candidate| candidate.name == listener.name)?
        .bootstrap_servers
        .clone();
    if bootstrap.is_empty() {
        return None;
    }
    Some((
        bootstrap,
        format!(
            "{}-broker-headless.{namespace}.svc.cluster.local",
            parent.name_any()
        ),
    ))
}

fn broker_user_name(connector_name: &str) -> String {
    format!("{connector_name}-broker")
}

fn child_kafka_user(
    connector: &KafkaConnector,
    cluster_name: &str,
) -> Result<KafkaUser, ReconcileError> {
    let connector_name = connector.name_any();
    let mut user = KafkaUser::new(
        &broker_user_name(&connector_name),
        KafkaUserSpec {
            authentication: Authentication::Tls(TlsAuth::default()),
            authorization: Some(Authorization::Simple(SimpleAuthorization {
                acls: [
                    AclResourceKind::Topic,
                    AclResourceKind::Group,
                    AclResourceKind::Cluster,
                ]
                .into_iter()
                .map(|kind| AclRule {
                    resource: AclResource {
                        kind,
                        name: if kind == AclResourceKind::Cluster {
                            "kafka-cluster".into()
                        } else {
                            "*".into()
                        },
                        pattern_type: AclPatternType::Literal,
                    },
                    operations: vec![AclOp::All],
                    host: "*".into(),
                    permission: AclPermission::Allow,
                })
                .collect(),
            })),
            quotas: None,
        },
    );
    user.metadata
        .namespace
        .clone_from(&connector.metadata.namespace);
    user.metadata.labels = Some(BTreeMap::from([
        ("crabka.io/cluster".into(), cluster_name.into()),
        ("crabka.io/connector".into(), connector_name),
    ]));
    user.metadata.owner_references = Some(vec![owner_ref::<KafkaConnector>(connector)?]);
    Ok(user)
}

fn render_deployment(
    connector: &KafkaConnector,
    cluster_name: &str,
    image: &str,
    bootstrap: &str,
    broker_server_name: &str,
    config_hash: &str,
) -> Result<Deployment, ReconcileError> {
    let name = connector.name_any();
    let labels: BTreeMap<String, String> = BTreeMap::from([
        ("app.kubernetes.io/name".into(), APP_NAME.into()),
        ("app.kubernetes.io/instance".into(), name.clone()),
        (
            "app.kubernetes.io/managed-by".into(),
            "crabka-operator".into(),
        ),
        ("crabka.io/cluster".into(), cluster_name.into()),
    ]);
    let mut env = vec![
        value_env("CRABKA_CONNECTOR_ID", &name),
        value_env("CRABKA_KAFKA_BOOTSTRAP", bootstrap),
        json!({
            "name": "CRABKA_POSTGRES_URL",
            "valueFrom": { "secretKeyRef": {
                "name": connector.spec.database_url.name,
                "key": connector.spec.database_url.key,
            }}
        }),
        value_env("CRABKA_POSTGRES_SLOT", &connector.spec.slot),
        value_env("CRABKA_POSTGRES_PUBLICATION", &connector.spec.publication),
        value_env(
            "CRABKA_POSTGRES_SCHEMA",
            connector.spec.schema.as_deref().unwrap_or("public"),
        ),
        value_env("CRABKA_POSTGRES_TABLES", connector.spec.tables.join(",")),
        value_env(
            "CRABKA_TOPIC_PREFIX",
            connector.spec.topic_prefix.as_deref().unwrap_or("db"),
        ),
        value_env("CRABKA_CONNECT_HEALTH_LISTEN", HEALTH_ADDR),
        value_env("CRABKA_BROKER_PROTOCOL", "SSL"),
        value_env(
            "CRABKA_BROKER_CERT_PATH",
            format!("{BROKER_CLIENT_DIR}/user.crt"),
        ),
        value_env(
            "CRABKA_BROKER_KEY_PATH",
            format!("{BROKER_CLIENT_DIR}/user.key"),
        ),
        value_env("CRABKA_BROKER_CA_PATH", format!("{BROKER_CA_DIR}/ca.crt")),
        value_env("CRABKA_BROKER_SERVER_NAME", broker_server_name),
    ];
    if let Some(runtime) = &connector.spec.runtime {
        if let Some(value) = runtime.max_batch {
            env.push(value_env("CRABKA_CONNECT_BATCH_SIZE", value.to_string()));
        }
        if let Some(value) = runtime.commit_interval_ms {
            env.push(value_env(
                "CRABKA_CONNECT_COMMIT_INTERVAL_MS",
                value.to_string(),
            ));
        }
        if let Some(value) = runtime.poll_backoff_ms {
            env.push(value_env(
                "CRABKA_CONNECT_POLL_BACKOFF_MS",
                value.to_string(),
            ));
        }
    }
    let resources = connector.spec.resources.clone().unwrap_or_default();
    let deployment = serde_json::from_value(json!({
        "metadata": {
            "name": name,
            "namespace": connector.metadata.namespace,
            "labels": labels,
            "ownerReferences": [owner_ref::<KafkaConnector>(connector)?],
        },
        "spec": {
            "replicas": i32::from(!connector.spec.paused),
            "selector": { "matchLabels": labels },
            "template": {
                "metadata": {
                    "labels": labels,
                    "annotations": { "crabka.io/config-hash": config_hash },
                },
                "spec": {
                    "securityContext": {
                        "runAsNonRoot": true,
                        "runAsUser": 65532,
                        "fsGroup": 65532,
                        "seccompProfile": { "type": "RuntimeDefault" },
                    },
                    "containers": [{
                        "name": "connector",
                        "image": image,
                        "env": env,
                        "ports": [{ "name": "health", "containerPort": HEALTH_PORT }],
                        "resources": resources,
                        "readinessProbe": { "httpGet": { "path": "/ready", "port": HEALTH_PORT } },
                        "livenessProbe": { "httpGet": { "path": "/live", "port": HEALTH_PORT } },
                        "securityContext": {
                            "allowPrivilegeEscalation": false,
                            "readOnlyRootFilesystem": true,
                            "capabilities": { "drop": ["ALL"] },
                        },
                        "volumeMounts": [
                            { "name": "broker-client", "mountPath": BROKER_CLIENT_DIR, "readOnly": true },
                            { "name": "broker-ca", "mountPath": BROKER_CA_DIR, "readOnly": true },
                        ],
                    }],
                    "volumes": [
                        { "name": "broker-client", "secret": { "secretName": broker_user_name(&name), "defaultMode": 256 } },
                        { "name": "broker-ca", "secret": { "secretName": cluster_ca_cert_name(cluster_name), "defaultMode": 256 } },
                    ],
                },
            },
        },
    }))?;
    Ok(deployment)
}

fn value_env(name: &str, value: impl Into<String>) -> serde_json::Value {
    json!({ "name": name, "value": value.into() })
}

fn secret_has_keys(secret: &Secret, keys: &[&str]) -> bool {
    secret.data.as_ref().is_some_and(|data| {
        keys.iter()
            .all(|key| data.get(*key).is_some_and(|value| !value.0.is_empty()))
    })
}

fn secret_value<'a>(secret: &'a Secret, key: &str) -> &'a [u8] {
    secret
        .data
        .as_ref()
        .and_then(|data| data.get(key))
        .map_or(&[], |value| value.0.as_slice())
}

fn secret_hash(parts: &[&[u8]]) -> String {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part);
    }
    hex::encode(hasher.finalize())
}

async fn patch_state(
    api: &Api<KafkaConnector>,
    obj: &KafkaConnector,
    state: (&str, &str, bool, Option<i32>, Option<i32>, bool),
) -> Result<(), ReconcileError> {
    let (reason, message, failed, replicas, ready_replicas, observed) = state;
    let paused = obj.spec.paused && reason == "Paused";
    let ready = reason == "Available";
    let status = KafkaConnectorStatus {
        conditions: vec![
            condition(
                "Ready",
                if ready { "True" } else { "False" },
                reason,
                message,
            ),
            condition(
                "Paused",
                if paused { "True" } else { "False" },
                reason,
                message,
            ),
            condition(
                "Failed",
                if failed { "True" } else { "False" },
                reason,
                message,
            ),
        ],
        observed_generation: observed.then_some(obj.meta().generation).flatten(),
        replicas,
        ready_replicas,
    };
    patch_status::<KafkaConnector, KafkaConnectorStatus>(api, &obj.name_any(), status).await
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::crd::{ConnectorRuntime, ConnectorSecretKeyRef, ConnectorType};

    fn fixture(paused: bool) -> KafkaConnector {
        let mut connector = KafkaConnector::new(
            "orders",
            KafkaConnectorSpec {
                type_: ConnectorType::PostgresSource,
                paused,
                image: None,
                resources: None,
                database_url: ConnectorSecretKeyRef {
                    name: "database".into(),
                    key: "url".into(),
                },
                slot: "orders_crabka".into(),
                publication: "crabka_connect".into(),
                schema: Some("public".into()),
                tables: vec!["orders".into(), "customers".into()],
                topic_prefix: Some("db".into()),
                runtime: Some(ConnectorRuntime {
                    max_batch: Some(100),
                    commit_interval_ms: Some(1_000),
                    poll_backoff_ms: Some(50),
                }),
            },
        );
        connector.metadata.namespace = Some("default".into());
        connector.metadata.uid = Some("uid".into());
        connector
    }

    #[test]
    fn deployment_renders_worker_contract_and_pause() {
        for (paused, replicas) in [(false, 1), (true, 0)] {
            let deployment = render_deployment(
                &fixture(paused),
                "demo",
                "worker:latest",
                "demo:9093",
                "demo-broker-headless.default.svc.cluster.local",
                "hash",
            )
            .unwrap();
            let spec = deployment.spec.unwrap();
            check!(spec.replicas == Some(replicas), "paused={paused}");
            let container = &spec.template.spec.unwrap().containers[0];
            check!(
                container
                    .readiness_probe
                    .as_ref()
                    .unwrap()
                    .http_get
                    .as_ref()
                    .unwrap()
                    .path
                    == Some("/ready".into())
            );
            check!(
                container
                    .liveness_probe
                    .as_ref()
                    .unwrap()
                    .http_get
                    .as_ref()
                    .unwrap()
                    .path
                    == Some("/live".into())
            );
            let env = container.env.as_ref().unwrap();
            for (name, value) in [
                ("CRABKA_CONNECTOR_ID", "orders"),
                ("CRABKA_KAFKA_BOOTSTRAP", "demo:9093"),
                ("CRABKA_POSTGRES_TABLES", "orders,customers"),
                ("CRABKA_CONNECT_BATCH_SIZE", "100"),
                ("CRABKA_CONNECT_COMMIT_INTERVAL_MS", "1000"),
                ("CRABKA_CONNECT_POLL_BACKOFF_MS", "50"),
                ("CRABKA_BROKER_PROTOCOL", "SSL"),
            ] {
                assert!(
                    env.iter()
                        .any(|item| item.name == name && item.value.as_deref() == Some(value)),
                    "missing {name}={value}"
                );
            }
            let database_url = env
                .iter()
                .find(|item| item.name == "CRABKA_POSTGRES_URL")
                .unwrap();
            let reference = database_url
                .value_from
                .as_ref()
                .unwrap()
                .secret_key_ref
                .as_ref()
                .unwrap();
            check!(reference.name == "database");
            check!(reference.key == "url");
        }
    }

    #[test]
    fn invalid_specs_are_rejected_before_rendering() {
        let mut spec = fixture(false).spec;
        spec.tables.clear();
        assert!(validate_spec(&spec).is_err());
        spec.tables.push("orders".into());
        spec.database_url.key.clear();
        assert!(validate_spec(&spec).is_err());
    }

    #[test]
    fn child_user_is_owned_tls_identity() {
        let connector = fixture(false);
        let user = child_kafka_user(&connector, "demo").unwrap();
        check!(user.name_any() == "orders-broker");
        assert!(matches!(user.spec.authentication, Authentication::Tls(_)));
        check!(user.metadata.owner_references.unwrap()[0].kind == "KafkaConnector");
    }
}
