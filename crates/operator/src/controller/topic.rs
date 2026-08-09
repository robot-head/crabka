//! `KafkaTopic` reconciler. It works in one direction, and the CRD wins.
//!
//! The reconciler watches `KafkaTopic` as its primary resource. It watches
//! `Kafka` as a secondary resource, so that a cluster that becomes Ready
//! wakes the topic reconciles that wait for it. The reconciler diffs the
//! spec against the live cluster and applies the difference with
//! `crabka_client_admin::AdminClient`.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use crabka_client_admin::{
    AdminClientLike, CreatePartitionsOp, CreateTopicSpec, IncrementalAlterOp,
};
use futures::StreamExt as _;
use kube::{
    Resource, ResourceExt as _,
    api::{Api, Patch, PatchParams},
    runtime::{
        controller::{Action, Controller},
        reflector::ObjectRef,
        watcher,
    },
};
use serde_json::json;

use crate::{
    context::Context,
    controller::common::{self, FIELD_MANAGER, ReconcileError, condition},
    crd::{Kafka, KafkaTopic},
};

const FINALIZER: &str = "crabka.io/topic-finalizer";

/// Runs the controller forever.
/// # Errors
/// Returns an error when cluster state cannot be loaded, the proposed plan is invalid, or a broker, Kubernetes, or persistence operation fails.
pub async fn run(ctx: Context) -> anyhow::Result<()> {
    let topic_api: Api<KafkaTopic> = Api::all(ctx.client.clone());
    let kafka_api: Api<Kafka> = Api::all(ctx.client.clone());
    Controller::new(topic_api, watcher::Config::default())
        // Kafka watch wakes the reconcile loop on cluster status changes.
        // We return empty here (rather than listing matching topics) so
        // the mapper stays sync-safe — listing would require an async
        // call that the kube-rs `mapper` signature doesn't allow. The
        // 60-second periodic requeue on each `KafkaTopic` catches the
        // transition (matches how `kafka.rs` handles its Node watch).
        .watches(kafka_api, watcher::Config::default(), |_kafka| {
            Vec::<ObjectRef<KafkaTopic>>::new().into_iter()
        })
        .run(reconcile, error_policy, Arc::new(ctx))
        .for_each(|res| async move {
            match res {
                Ok((obj, _)) => tracing::debug!(?obj, "topic reconciled"),
                Err(e) => tracing::warn!(error = %e, "topic reconcile error"),
            }
        })
        .await;
    Ok(())
}

pub fn error_policy(_obj: Arc<KafkaTopic>, err: &ReconcileError, ctx: Arc<Context>) -> Action {
    tracing::warn!(error = %err, "topic reconcile error, requeueing");
    common::error_requeue(ctx)
}

/// Reconcile entry point. It times the pass, records the reconcile
/// counter and histogram, then calls the internal `reconcile_inner`
/// operation.
#[tracing::instrument(
    level = "info",
    skip_all,
    fields(
        kind = "KafkaTopic",
        namespace = %obj.namespace().unwrap_or_else(|| "default".into()),
        name = %obj.name_any(),
        generation = ?obj.meta().generation,
    )
)]
/// # Errors
/// Returns an error when cluster state cannot be loaded, the proposed plan is invalid, or a broker, Kubernetes, or persistence operation fails.
pub async fn reconcile(obj: Arc<KafkaTopic>, ctx: Arc<Context>) -> Result<Action, ReconcileError> {
    common::record_reconcile(
        &ctx,
        "KafkaTopic",
        Box::pin(reconcile_inner(obj, ctx.clone())),
    )
    .await
}

enum TopicPreparation {
    Ready {
        cluster: String,
        bootstrap: String,
        topic_name: String,
    },
    Done(Action),
}

async fn create_topic(
    admin: &mut dyn AdminClientLike,
    ctx: &Context,
    cluster: &str,
    topic_api: &Api<KafkaTopic>,
    resource_name: &str,
    obj: &KafkaTopic,
    topic_name: &str,
) -> Result<Action, ReconcileError> {
    let outcome = match admin
        .create_topics(
            &[CreateTopicSpec {
                name: topic_name.to_string(),
                partitions: obj.spec.partitions,
                replicas: obj.spec.replicas,
                configs: obj.spec.config.clone().unwrap_or_default(),
            }],
            ctx.config.topic_mutation_timeout,
        )
        .await
    {
        Ok(mut outcomes) => outcomes.pop().expect("one spec produces one outcome"),
        Err(error) => {
            tracing::warn!(%error, "CreateTopics transport failure");
            if matches!(error, crabka_client_admin::AdminError::Transport(_)) {
                ctx.drop_admin_client(cluster).await;
            }
            return Ok(common::requeue(ctx.config.controller_error_requeue));
        }
    };
    if let Some(error) = outcome.error {
        patch_status(
            topic_api,
            resource_name,
            obj,
            (
                "False",
                "BrokerError",
                &format!("CreateTopics: {} ({})", error.name, error.code),
            ),
            None,
            false,
        )
        .await?;
        return Ok(common::requeue(ctx.config.controller_error_requeue));
    }
    patch_status(
        topic_api,
        resource_name,
        obj,
        ("True", "Ready", "topic created"),
        outcome.topic_id.map(|id| id.to_string()),
        true,
    )
    .await?;
    Ok(common::requeue(ctx.config.controller_drift_requeue))
}

async fn prepare_topic(
    obj: &KafkaTopic,
    ctx: &Context,
    topic_api: &Api<KafkaTopic>,
    namespace: &str,
    name: &str,
) -> Result<TopicPreparation, ReconcileError> {
    let Some(cluster) = obj
        .meta()
        .labels
        .as_ref()
        .and_then(|labels| labels.get("crabka.io/cluster").cloned())
    else {
        patch_status(
            topic_api,
            name,
            obj,
            (
                "False",
                "MissingClusterLabel",
                "metadata.labels[\"crabka.io/cluster\"] is required",
            ),
            None,
            false,
        )
        .await?;
        return Ok(TopicPreparation::Done(common::requeue(
            ctx.config.controller_drift_requeue,
        )));
    };
    let topic_name = obj.spec.topic_name.clone().unwrap_or_else(|| name.into());
    if let Err(message) = validate_kafka_topic_name(&topic_name) {
        patch_status(
            topic_api,
            name,
            obj,
            ("False", "InvalidTopicName", &message),
            None,
            false,
        )
        .await?;
        return Ok(TopicPreparation::Done(common::requeue(
            ctx.config.controller_invalid_requeue,
        )));
    }
    let kafka_api: Api<Kafka> = Api::namespaced(ctx.client.clone(), namespace);
    let bootstrap = kafka_api
        .get_opt(&cluster)
        .await?
        .as_ref()
        .and_then(internal_listener_bootstrap);
    let Some(bootstrap) = bootstrap else {
        patch_status(
            topic_api,
            name,
            obj,
            (
                "False",
                "ClusterNotReady",
                &format!("Kafka/{cluster} not Ready or no internal listener"),
            ),
            None,
            false,
        )
        .await?;
        return Ok(TopicPreparation::Done(common::requeue(
            ctx.config.controller_dependency_requeue,
        )));
    };
    if obj.meta().deletion_timestamp.is_some() {
        if !obj.spec.preserve_topic
            && let Ok(client) = ctx.admin_client_for(&cluster, &bootstrap).await
        {
            let mut admin = client.lock().await;
            if let Err(error) = admin
                .delete_topics(&[&topic_name], ctx.config.topic_mutation_timeout)
                .await
            {
                tracing::warn!(%error, %topic_name, "DeleteTopics failed during finalizer");
            }
        }
        remove_finalizer(topic_api, name).await?;
        return Ok(TopicPreparation::Done(Action::await_change()));
    }
    if !has_finalizer(obj) {
        add_finalizer(topic_api, name).await?;
        return Ok(TopicPreparation::Done(Action::requeue(Duration::ZERO)));
    }
    Ok(TopicPreparation::Ready {
        cluster,
        bootstrap,
        topic_name,
    })
}

// linear pipeline; extraction hurts more than helps
async fn reconcile_inner(
    obj: Arc<KafkaTopic>,
    ctx: Arc<Context>,
) -> Result<Action, ReconcileError> {
    let ns = obj.namespace().unwrap_or_else(|| "default".into());
    let name = obj.name_any();
    let topic_api: Api<KafkaTopic> = Api::namespaced(ctx.client.clone(), &ns);
    let (cluster, bootstrap, topic_name) =
        match prepare_topic(&obj, &ctx, &topic_api, &ns, &name).await? {
            TopicPreparation::Ready {
                cluster,
                bootstrap,
                topic_name,
            } => (cluster, bootstrap, topic_name),
            TopicPreparation::Done(action) => return Ok(action),
        };

    // 6. Connect and fetch current state
    let admin_handle = match ctx.admin_client_for(&cluster, &bootstrap).await {
        Ok(h) => h,
        Err(e) => {
            tracing::warn!(error = %e, %cluster, "AdminClient connect failed");
            return Ok(common::requeue(ctx.config.controller_error_requeue));
        }
    };
    let mut admin = admin_handle.lock().await;

    let md = match admin.metadata(&[&topic_name]).await {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!(error = %e, %topic_name, "Metadata failed");
            let is_transport = matches!(e, crabka_client_admin::AdminError::Transport(_));
            drop(admin);
            if is_transport {
                ctx.drop_admin_client(&cluster).await;
            }
            return Ok(common::requeue(ctx.config.controller_error_requeue));
        }
    };
    let current = md.topics.iter().find(|t| t.name == topic_name);

    // 7. Diff and apply
    let current = match current {
        Some(t) if t.error.is_none() => Some(t.clone()),
        _ => None,
    };
    match current {
        None => {
            create_topic(
                &mut *admin,
                &ctx,
                &cluster,
                &topic_api,
                &name,
                &obj,
                &topic_name,
            )
            .await
        }
        Some(cur) => {
            // Immutable fields
            if cur.replication_factor != obj.spec.replicas {
                patch_status(
                    &topic_api,
                    &name,
                    &obj,
                    (
                        "False",
                        "ImmutableFieldChanged",
                        "spec.replicas change requires partition reassignment",
                    ),
                    cur.topic_id.map(|u| u.to_string()),
                    false,
                )
                .await?;
                return Ok(common::requeue(ctx.config.controller_invalid_requeue));
            }
            if cur.partition_count > obj.spec.partitions {
                patch_status(
                    &topic_api,
                    &name,
                    &obj,
                    (
                        "False",
                        "ImmutableFieldChanged",
                        "spec.partitions decrease is not supported by Kafka",
                    ),
                    cur.topic_id.map(|u| u.to_string()),
                    false,
                )
                .await?;
                return Ok(common::requeue(ctx.config.controller_invalid_requeue));
            }

            // Partition increase
            if cur.partition_count < obj.spec.partitions {
                let outcomes = admin
                    .create_partitions(
                        &[CreatePartitionsOp {
                            name: topic_name.clone(),
                            new_total_count: obj.spec.partitions,
                        }],
                        ctx.config.topic_mutation_timeout,
                    )
                    .await;
                match outcomes {
                    Ok(mut v) => {
                        let o = v.pop().expect("one op → one outcome");
                        if let Some(err) = o.error {
                            patch_status(
                                &topic_api,
                                &name,
                                &obj,
                                (
                                    "False",
                                    "BrokerError",
                                    &format!("CreatePartitions: {} ({})", err.name, err.code),
                                ),
                                cur.topic_id.map(|u| u.to_string()),
                                false,
                            )
                            .await?;
                            return Ok(common::requeue(ctx.config.controller_error_requeue));
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "CreatePartitions transport failure");
                        let is_transport =
                            matches!(e, crabka_client_admin::AdminError::Transport(_));
                        drop(admin);
                        if is_transport {
                            ctx.drop_admin_client(&cluster).await;
                        }
                        return Ok(common::requeue(ctx.config.controller_error_requeue));
                    }
                }
            }

            // Config diff
            let desired = obj.spec.config.clone().unwrap_or_default();
            let overrides = match admin.describe_configs(&[&topic_name]).await {
                Ok(v) => v
                    .into_iter()
                    .next()
                    .map(|o| o.overrides)
                    .unwrap_or_default(),
                Err(e) => {
                    tracing::warn!(error = %e, "DescribeConfigs failed");
                    let is_transport = matches!(e, crabka_client_admin::AdminError::Transport(_));
                    drop(admin);
                    if is_transport {
                        ctx.drop_admin_client(&cluster).await;
                    }
                    return Ok(common::requeue(ctx.config.controller_error_requeue));
                }
            };
            let ops = diff_configs(&overrides, &desired, &topic_name);
            if !ops.is_empty() {
                match admin.incremental_alter_configs(&ops).await {
                    Ok(outcomes) => {
                        if let Some(err) = outcomes.into_iter().find_map(|o| o.error) {
                            patch_status(
                                &topic_api,
                                &name,
                                &obj,
                                (
                                    "False",
                                    "BrokerError",
                                    &format!(
                                        "IncrementalAlterConfigs: {} ({})",
                                        err.name, err.code
                                    ),
                                ),
                                cur.topic_id.map(|u| u.to_string()),
                                false,
                            )
                            .await?;
                            return Ok(common::requeue(ctx.config.controller_error_requeue));
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "IncrementalAlterConfigs failure");
                        let is_transport =
                            matches!(e, crabka_client_admin::AdminError::Transport(_));
                        drop(admin);
                        if is_transport {
                            ctx.drop_admin_client(&cluster).await;
                        }
                        return Ok(common::requeue(ctx.config.controller_error_requeue));
                    }
                }
            }

            patch_status(
                &topic_api,
                &name,
                &obj,
                ("True", "Ready", "topic in sync"),
                cur.topic_id.map(|u| u.to_string()),
                true,
            )
            .await?;
            Ok(common::requeue(ctx.config.controller_drift_requeue))
        }
    }
}

/// Diffs `desired` against the `current` overrides and produces a `Vec`
/// of `IncrementalAlterOps`.
///
/// This is a pure function. The tests below cover it.
pub(crate) fn diff_configs(
    current: &BTreeMap<String, String>,
    desired: &BTreeMap<String, String>,
    topic: &str,
) -> Vec<IncrementalAlterOp> {
    let mut ops = Vec::new();
    for (k, v) in desired {
        if current.get(k) != Some(v) {
            ops.push(IncrementalAlterOp::Set {
                topic: topic.to_string(),
                key: k.clone(),
                value: v.clone(),
            });
        }
    }
    for k in current.keys() {
        if !desired.contains_key(k) {
            ops.push(IncrementalAlterOp::Delete {
                topic: topic.to_string(),
                key: k.clone(),
            });
        }
    }
    ops
}

/// Returns the bootstrap address from
/// `Kafka.status.listeners[<inter_broker>]`.
///
/// The result is `None` when
/// `Kafka.status.conditions[Ready].status != "True"`.
pub(crate) fn internal_listener_bootstrap(kafka: &Kafka) -> Option<String> {
    let ready_true = kafka
        .status
        .as_ref()
        .and_then(|s| s.conditions.iter().find(|c| c.type_ == "Ready"))
        .is_some_and(|c| c.status == "True");
    if !ready_true {
        return None;
    }
    let inter_broker = kafka
        .spec
        .inter_broker_listener_name
        .as_deref()
        .unwrap_or("PLAIN");
    let listeners = &kafka.status.as_ref()?.listeners;
    listeners
        .iter()
        .find(|l| l.name == inter_broker)
        .map(|l| l.bootstrap_servers.clone())
        .filter(|s| !s.is_empty())
}

/// Validates a Kafka topic name. It follows `Topic.validate` of the JVM
/// client.
pub(crate) fn validate_kafka_topic_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("topic name is empty".into());
    }
    if name.len() > 249 {
        return Err(format!("topic name length {} exceeds 249", name.len()));
    }
    if name == "." || name == ".." {
        return Err("topic name cannot be \".\" or \"..\"".into());
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-')
    {
        return Err(format!("topic name {name:?} contains invalid characters"));
    }
    Ok(())
}

fn has_finalizer(obj: &KafkaTopic) -> bool {
    obj.meta()
        .finalizers
        .as_ref()
        .is_some_and(|f| f.iter().any(|s| s == FINALIZER))
}

#[tracing::instrument(level = "debug", skip_all, fields(topic = %name), err)]
async fn add_finalizer(api: &Api<KafkaTopic>, name: &str) -> Result<(), ReconcileError> {
    let patch = json!({ "metadata": { "finalizers": [FINALIZER] } });
    let params = PatchParams {
        field_manager: Some(FIELD_MANAGER.into()),
        ..Default::default()
    };
    api.patch(name, &params, &Patch::Merge(&patch)).await?;
    Ok(())
}

#[tracing::instrument(level = "info", skip_all, fields(topic = %name), err)]
async fn remove_finalizer(api: &Api<KafkaTopic>, name: &str) -> Result<(), ReconcileError> {
    let patch = json!({ "metadata": { "finalizers": [] } });
    let params = PatchParams {
        field_manager: Some(FIELD_MANAGER.into()),
        ..Default::default()
    };
    api.patch(name, &params, &Patch::Merge(&patch)).await?;
    Ok(())
}

/// Builds the status and patches it.
///
/// With `advance_generation = true`, this function writes
/// `observedGeneration` to the current generation. The caller sets that
/// flag only when the resource lands on a True or Ready state.
// pure status helper; arity reflects the condition contract
#[tracing::instrument(
    level = "info",
    skip_all,
    fields(topic = %name, condition = ?condition_state),
    err,
)]
async fn patch_status(
    api: &Api<KafkaTopic>,
    name: &str,
    obj: &KafkaTopic,
    condition_state: (&str, &str, &str),
    topic_id: Option<String>,
    advance_generation: bool,
) -> Result<(), ReconcileError> {
    let (status, reason, message) = condition_state;
    let topic_name = obj
        .spec
        .topic_name
        .clone()
        .unwrap_or_else(|| name.to_string());
    let conditions = vec![condition("Ready", status, reason, message)];
    let observed_generation = if advance_generation {
        obj.meta().generation
    } else {
        obj.status.as_ref().and_then(|s| s.observed_generation)
    };

    let body = json!({
        "status": {
            "conditions": conditions,
            "observedGeneration": observed_generation,
            "topicName": topic_name,
            "topicId": topic_id,
        }
    });
    let params = PatchParams {
        field_manager: Some(FIELD_MANAGER.into()),
        ..Default::default()
    };
    api.patch_status(name, &params, &Patch::Merge(&body))
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::crd::{KafkaCondition, KafkaSpec, KafkaStatus, ListenerStatus, ListenerType};

    fn kafka_ready(name: &str, namespace: &str, listener_port: i32) -> Kafka {
        let mut k = Kafka::new(
            name,
            KafkaSpec {
                kafka_version: "0.1.1".into(),
                metadata_version: None,
                config: None,
                listeners: vec![],
                inter_broker_listener_name: Some("PLAIN".into()),
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
        k.metadata.namespace = Some(namespace.into());
        k.status = Some(KafkaStatus {
            conditions: vec![KafkaCondition {
                type_: "Ready".into(),
                status: "True".into(),
                reason: "Available".into(),
                message: String::new(),
                last_transition_time: "2026-05-18T00:00:00Z".into(),
            }],
            replicas: Some(1),
            ready_replicas: Some(1),
            listeners: vec![ListenerStatus {
                name: "PLAIN".into(),
                type_: ListenerType::Internal,
                bootstrap_servers: format!(
                    "{name}-broker-headless.{namespace}.svc.cluster.local:{listener_port}"
                ),
                addresses: vec![],
            }],
            cluster_ca: None,
            clients_ca: None,
            kafka_version: None,
            metadata_version: None,
        });
        k
    }

    #[test]
    fn validate_topic_name_accepts_typical() {
        assert!(validate_kafka_topic_name("demo-topic").is_ok());
        assert!(validate_kafka_topic_name("My.Topic_1").is_ok());
    }

    #[test]
    fn validate_topic_name_rejects_empty() {
        assert!(validate_kafka_topic_name("").is_err());
    }

    #[test]
    fn validate_topic_name_rejects_dot_and_dotdot() {
        assert!(validate_kafka_topic_name(".").is_err());
        assert!(validate_kafka_topic_name("..").is_err());
    }

    #[test]
    fn validate_topic_name_rejects_too_long() {
        let n = "a".repeat(250);
        assert!(validate_kafka_topic_name(&n).is_err());
    }

    #[test]
    fn validate_topic_name_rejects_invalid_chars() {
        for name in ["has space", "has/slash", "has@at"] {
            assert!(validate_kafka_topic_name(name).is_err(), "case {name:?}");
        }
    }

    #[test]
    fn diff_configs_set_adds_missing_key() {
        let current = BTreeMap::new();
        let desired = BTreeMap::from([("retention.ms".to_string(), "60000".to_string())]);
        let ops = diff_configs(&current, &desired, "foo");
        assert!(ops.len() == 1);
        assert!(matches!(&ops[0], IncrementalAlterOp::Set { key, value, .. }
            if key == "retention.ms" && value == "60000"));
    }

    #[test]
    fn diff_configs_set_updates_changed_value() {
        let current = BTreeMap::from([("retention.ms".to_string(), "30000".to_string())]);
        let desired = BTreeMap::from([("retention.ms".to_string(), "60000".to_string())]);
        let ops = diff_configs(&current, &desired, "foo");
        assert!(ops.len() == 1);
        assert!(matches!(&ops[0], IncrementalAlterOp::Set { value, .. } if value == "60000"));
    }

    #[test]
    fn diff_configs_delete_removes_extra_key() {
        let current = BTreeMap::from([("cleanup.policy".to_string(), "delete".to_string())]);
        let desired = BTreeMap::new();
        let ops = diff_configs(&current, &desired, "foo");
        assert!(ops.len() == 1);
        assert!(
            matches!(&ops[0], IncrementalAlterOp::Delete { key, .. } if key == "cleanup.policy")
        );
    }

    #[test]
    fn diff_configs_noop_when_matching() {
        let m = BTreeMap::from([("retention.ms".to_string(), "60000".to_string())]);
        assert!(diff_configs(&m, &m, "foo").is_empty());
    }

    #[test]
    fn diff_configs_combines_set_and_delete() {
        let current = BTreeMap::from([
            ("retention.ms".to_string(), "30000".to_string()),
            ("cleanup.policy".to_string(), "delete".to_string()),
        ]);
        let desired = BTreeMap::from([
            ("retention.ms".to_string(), "60000".to_string()),
            ("segment.bytes".to_string(), "1048576".to_string()),
        ]);
        let ops = diff_configs(&current, &desired, "foo");
        assert!(
            ops.len() == 3,
            "expected SET(retention.ms), SET(segment.bytes), DELETE(cleanup.policy)"
        );
    }

    #[test]
    fn internal_listener_bootstrap_returns_listener_when_ready() {
        let k = kafka_ready("demo", "default", 9092);
        assert!(
            internal_listener_bootstrap(&k).as_deref()
                == Some("demo-broker-headless.default.svc.cluster.local:9092")
        );
    }

    #[test]
    fn internal_listener_bootstrap_returns_none_when_not_ready() {
        let mut k = kafka_ready("demo", "default", 9092);
        if let Some(s) = k.status.as_mut() {
            s.conditions[0].status = "False".into();
        }
        assert!(internal_listener_bootstrap(&k).is_none());
    }
}
