//! `GresTenant` reconciler.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use crabka_client_admin::{
    AclEntry, AclEntryFilter, AclOperation, CreateTopicSpec, DEFAULT_SCRAM_ITERATIONS, PatternType,
    PermissionType, ResourceType, ScramDeletion, ScramUpsertion,
};
use crabka_gres_control::{
    DEFAULT_CHECKPOINT_BYTES, DEFAULT_CHECKPOINT_FRAMES, RangeBoundary, RangeLayoutEntry, SqlUser,
    TenantId, TenantName, TenantRecord, TenantState, tenant_config_topic,
};
use crabka_security::{
    ca::{SubjectAltName, generate_cluster_ca, issue_broker_cert},
    scram::PgScramVerifier,
};
use futures::StreamExt as _;
use k8s_openapi::{
    ByteString,
    api::{
        apps::v1::Deployment,
        core::v1::{Secret, Service},
        networking::v1::{
            NetworkPolicy, NetworkPolicyIngressRule, NetworkPolicyPeer, NetworkPolicyPort,
            NetworkPolicySpec,
        },
    },
    apimachinery::pkg::{apis::meta::v1::LabelSelector, util::intstr::IntOrString},
};
use kube::{
    Resource, ResourceExt as _,
    api::{Api, DeleteParams, Patch, PatchParams},
    runtime::{
        controller::{Action, Controller},
        reflector::ObjectRef,
        watcher,
    },
};
use serde_json::json;
use sha2::{Digest as _, Sha256};

use crate::{
    context::Context,
    controller::{
        common::{self, FIELD_MANAGER, ReconcileError, apply_object, condition, owner_ref},
        gres_split_operation::{
            MtlsRangeMutationClient, active_operations, reconcile_activated_cutover,
            reconcile_one_rpc_phase, successors_may_be_deployed, verify_target_topology_ready,
        },
        topic::internal_listener_bootstrap,
        user::{diff_acls, entry_to_exact_filter},
    },
    crd::{
        Gres, GresTenant, GresTenantRangeKey, GresTenantRangeSpec, Kafka, SecretKeyRef,
        TenantDefaults, gres::EffectiveGresComputePolicy,
    },
};

const FINALIZER: &str = "crabka.io/gres-tenant-finalizer";
const APP_NAME: &str = "crabka-gres";
const DEFAULT_IMAGE: &str = concat!("ghcr.io/robot-head/crabka-gres:", env!("CARGO_PKG_VERSION"));
pub(super) const COMPUTE_PORT: i32 = 5432;
const RANGE_PORT: i32 = 7432;
const RANGE_TLS_DIR: &str = "/etc/crabka/range-tls";
const RANGE_TLS_IDENTITY_ANNOTATION: &str = "crabka.io/range-tls-identity";
const RANGE_TLS_HASH_ANNOTATION: &str = "crabka.io/range-tls-hash";

/// Run the controller forever.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub async fn run(ctx: Context) -> anyhow::Result<()> {
    let tenant_api: Api<GresTenant> = Api::all(ctx.client.clone());
    let gres_api: Api<Gres> = Api::all(ctx.client.clone());
    let kafka_api: Api<Kafka> = Api::all(ctx.client.clone());
    Controller::new(tenant_api, watcher::Config::default())
        .watches(gres_api, watcher::Config::default(), |_gres| {
            Vec::<ObjectRef<GresTenant>>::new().into_iter()
        })
        .watches(kafka_api, watcher::Config::default(), |_kafka| {
            Vec::<ObjectRef<GresTenant>>::new().into_iter()
        })
        .run(reconcile, error_policy, Arc::new(ctx))
        .for_each(|res| async move {
            match res {
                Ok((obj, _)) => tracing::debug!(?obj, "gres tenant reconciled"),
                Err(err) => tracing::warn!(error = %err, "gres tenant reconcile error"),
            }
        })
        .await;
    Ok(())
}

pub fn error_policy(_obj: Arc<GresTenant>, err: &ReconcileError, ctx: Arc<Context>) -> Action {
    tracing::warn!(error = %err, "gres tenant reconcile error, requeueing");
    common::error_requeue(ctx)
}

#[tracing::instrument(level = "info", skip_all, fields(kind = "GresTenant", name = %obj.name_any()))]
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub async fn reconcile(obj: Arc<GresTenant>, ctx: Arc<Context>) -> Result<Action, ReconcileError> {
    common::record_reconcile(
        &ctx,
        "GresTenant",
        Box::pin(reconcile_inner(obj, ctx.clone())),
    )
    .await
}

struct ReadyTenant {
    namespace: String,
    name: String,
    tenant_api: Api<GresTenant>,
    tenant_name: TenantName,
    cluster: String,
    bootstrap: String,
    policy: crabka_gres_control::RegistryPolicy,
    defaults: EffectiveDefaults,
    compute_image: String,
    compute_policy: EffectiveGresComputePolicy,
    direct_bootstrap_grace_ms: u64,
    kafka_sasl: bool,
}

enum TenantPreparation {
    Ready(Box<ReadyTenant>),
    Requeue(Action),
}

fn effective_compute_image(
    obj: &GresTenant,
    config: &crate::config::OperatorConfig,
) -> Result<String, ReconcileError> {
    let (image, path) = if let Some(image) = &obj.spec.image {
        (image.clone(), "spec.image")
    } else if let Some(image) = &config.default_gres_image {
        (image.clone(), "DEFAULT_GRES_IMAGE")
    } else {
        (DEFAULT_IMAGE.to_owned(), "compiled default Gres image")
    };
    refined_type::rule::NonEmptyString::new(image)
        .map(refined_type::Refined::into_value)
        .map_err(|error| ReconcileError::Malformed(format!("{path}: {error}")))
}

async fn prepare_tenant(
    obj: &GresTenant,
    ctx: &Context,
) -> Result<TenantPreparation, ReconcileError> {
    let namespace = obj.namespace().unwrap_or_else(|| "default".into());
    let name = obj.name_any();
    let tenant_api: Api<GresTenant> = Api::namespaced(ctx.client.clone(), &namespace);
    let tenant_name = match TenantName::try_from(name.as_str()) {
        Ok(name) => name,
        Err(error) => {
            patch_status(
                &tenant_api,
                &name,
                obj,
                &TenantStatusUpdate {
                    status: "False",
                    reason: "InvalidName",
                    message: &error.to_string(),
                    registry_version: None,
                    lifecycle_phase: preserved_lifecycle_state(obj),
                    advance_generation: false,
                    direct_bootstrap_grace_ms: None,
                },
            )
            .await?;
            return Ok(TenantPreparation::Requeue(Action::requeue(
                Duration::from_mins(5),
            )));
        }
    };
    let gres_api: Api<Gres> = Api::namespaced(ctx.client.clone(), &namespace);
    let Some(gres) = gres_api.get_opt(&obj.spec.gres).await? else {
        patch_status(
            &tenant_api,
            &name,
            obj,
            &TenantStatusUpdate {
                status: "False",
                reason: "GresNotFound",
                message: "referenced Gres does not exist",
                registry_version: None,
                lifecycle_phase: preserved_lifecycle_state(obj),
                advance_generation: false,
                direct_bootstrap_grace_ms: None,
            },
        )
        .await?;
        return Ok(TenantPreparation::Requeue(Action::requeue(
            Duration::from_secs(30),
        )));
    };
    let pgdog_policy = gres
        .spec
        .pgdog
        .effective_policy()
        .map_err(ReconcileError::Malformed)?;
    let compute_policy = gres
        .spec
        .compute
        .as_ref()
        .map_or_else(
            || crate::crd::gres::GresComputeSpec::default().effective_policy(),
            crate::crd::gres::GresComputeSpec::effective_policy,
        )
        .map_err(ReconcileError::Malformed)?;
    let compute_image = effective_compute_image(obj, &ctx.config)?;
    let cluster = gres.spec.kafka_cluster.clone();
    let kafka_api: Api<Kafka> = Api::namespaced(ctx.client.clone(), &namespace);
    let Some(kafka) = kafka_api.get_opt(&cluster).await? else {
        patch_cluster_not_ready(&tenant_api, &name, obj).await?;
        return Ok(TenantPreparation::Requeue(Action::requeue(
            Duration::from_secs(30),
        )));
    };
    let Some(bootstrap) = internal_listener_bootstrap(&kafka) else {
        patch_cluster_not_ready(&tenant_api, &name, obj).await?;
        return Ok(TenantPreparation::Requeue(Action::requeue(
            Duration::from_secs(30),
        )));
    };
    let policy = kafka
        .spec
        .gres_registry
        .as_ref()
        .map_or_else(
            || Ok(crabka_gres_control::RegistryPolicy::default()),
            crate::crd::GresRegistrySpec::policy,
        )
        .map_err(|error| ReconcileError::Malformed(format!("spec.gresRegistry: {error}")))?;
    if obj.meta().deletion_timestamp.is_some() {
        cleanup_tenant(
            ctx,
            &namespace,
            &cluster,
            &bootstrap,
            &policy,
            &tenant_name,
            &name,
        )
        .await;
        remove_finalizer(&tenant_api, &name).await?;
        return Ok(TenantPreparation::Requeue(Action::await_change()));
    }
    if !has_finalizer(obj) {
        add_finalizer(&tenant_api, &name).await?;
        return Ok(TenantPreparation::Requeue(Action::requeue(Duration::ZERO)));
    }
    Ok(TenantPreparation::Ready(Box::new(ReadyTenant {
        namespace,
        name,
        tenant_api,
        tenant_name,
        cluster,
        bootstrap,
        policy,
        defaults: effective_defaults(gres.spec.defaults.as_ref(), obj.spec.overrides.as_ref()),
        compute_image,
        compute_policy,
        direct_bootstrap_grace_ms: pgdog_policy.direct_bootstrap_grace.into_value(),
        kafka_sasl: kafka_internal_listener_requires_sasl(&kafka),
    })))
}

async fn patch_cluster_not_ready(
    tenant_api: &Api<GresTenant>,
    name: &str,
    obj: &GresTenant,
) -> Result<(), ReconcileError> {
    patch_status(
        tenant_api,
        name,
        obj,
        &TenantStatusUpdate {
            status: "False",
            reason: "ClusterNotReady",
            message: "referenced Kafka is not Ready or has no internal listener",
            registry_version: None,
            lifecycle_phase: preserved_lifecycle_state(obj),
            advance_generation: false,
            direct_bootstrap_grace_ms: None,
        },
    )
    .await
}

async fn reconcile_inner(
    obj: Arc<GresTenant>,
    ctx: Arc<Context>,
) -> Result<Action, ReconcileError> {
    let ready = match Box::pin(prepare_tenant(&obj, &ctx)).await? {
        TenantPreparation::Ready(ready) => ready,
        TenantPreparation::Requeue(action) => return Ok(action),
    };
    let ReadyTenant {
        namespace: ns,
        name,
        tenant_api,
        tenant_name,
        cluster,
        bootstrap,
        policy,
        defaults,
        compute_image,
        compute_policy,
        direct_bootstrap_grace_ms,
        kafka_sasl,
    } = *ready;
    let (wal_topic, cfg_topic) = (wal_topic(&tenant_name), tenant_config_topic(&tenant_name));
    let spec_ranges = effective_ranges(&obj.spec.ranges)?;
    let control = Context::gres_control_for(&ctx, &ns, &cluster, &bootstrap, &policy).await?;
    let current_record = control.get_tenant(&tenant_name).await?;
    let split_operations = active_operations(control.list_split_operations(&tenant_name).await?);
    let active_split = split_operations.first().cloned();
    let tenant_ranges =
        reconcile_tenant_ranges(current_record.as_ref(), &spec_ranges, active_split.as_ref());
    let lifecycle_state = current_record
        .as_ref()
        .map_or_else(|| requested_state(&obj), |record| record.state);
    let registry_version = current_record.as_ref().map(|record| record.record_version);
    let range_control_enabled = tenant_ranges.len() > 1 || active_split.is_some();
    let range_tls_hash = if range_control_enabled {
        Some(reconcile_range_tls_secret(&ctx, &ns, &obj).await?)
    } else {
        None
    };
    let reconcile_result: Result<Action, ReconcileError> = async {
        let admin_handle = ctx.admin_client_for(&cluster, &bootstrap).await?;
        let mut admin = admin_handle.lock().await;
        let password = provision_tenant_resources(
            &mut admin,
            &TenantResourceConfig {
                ctx: &ctx,
                namespace: &ns,
                obj: &obj,
                name: &name,
                tenant_name: &tenant_name,
                tenant_ranges: &tenant_ranges,
                config_topic: &cfg_topic,
                defaults,
                current_record: current_record.as_ref(),
                lifecycle_state,
            },
        )
        .await?;
        let record_version = match current_record.as_ref() {
            None => 1,
            Some(record) => record.record_version.checked_add(1).ok_or_else(|| {
                ReconcileError::Malformed("tenant registry record version overflowed".to_string())
            })?,
        };
        let mut record = build_tenant_record(
            &obj,
            &tenant_name,
            &password,
            record_version,
            &defaults,
            current_record.as_ref(),
            &tenant_ranges,
        )?;
        let range_parking_progress = park_retiring_ranges(
            &control,
            &mut admin,
            &tenant_name,
            &mut record,
            current_record.as_ref(),
        )
        .await?;
        let parking_progress = if matches!(
            lifecycle_state,
            TenantState::Parking | TenantState::Suspended
        ) {
            park_suspended_tenant_wal(
                &control,
                &mut admin,
                &tenant_ranges,
                &tenant_name,
                &mut record,
                current_record
                    .as_ref()
                    .map(|current| current.record_version),
            )
            .await?
        } else {
            ParkingProgress::NotNeeded
        };
        if parking_progress == ParkingProgress::DeletionPending
            || range_parking_progress == ParkingProgress::DeletionPending
        {
            drop(admin);
            let _ready = reconcile_compute_deployments(
                &ctx,
                &ns,
                &obj,
                &ComputeDeploymentConfig {
                    ranges: &tenant_ranges,
                    bootstrap: &bootstrap,
                    wal_topic: &wal_topic,
                    config_topic: &cfg_topic,
                    policy: &policy,
                    image: &compute_image,
                    compute_policy,
                    lifecycle_state: record.state,
                    kafka_sasl,
                    range_control_enabled,
                    range_tls_hash: range_tls_hash.as_deref(),
                },
            )
            .await?;
            return Ok(lifecycle_requeue(&compute_policy));
        }
        if matches!(
            lifecycle_state,
            TenantState::Parking | TenantState::Suspended
        ) {
            record.record_version = record_version;
            record.ensure_valid()?;
        }
        drop(admin);
        if parking_progress == ParkingProgress::Complete
            || range_parking_progress == ParkingProgress::Complete
        {
            // The fenced parking intent was committed before deleting any WAL.
            // Do not issue a second replacement against the stale pre-park view.
        } else if active_split.is_some() {
            if let Some(current) = current_record.as_ref() {
                record = current.clone();
            }
        } else if tenant_record_changed(current_record.as_ref(), &record) {
            record = control
                .replace_tenant_if_version(
                    &record,
                    current_record
                        .as_ref()
                        .map(|current| current.record_version),
                )
                .await?;
        } else if let Some(current) = current_record.as_ref() {
            record = current.clone();
        }
        reconcile_compute_and_status(&ComputeStatusConfig {
            ctx: &ctx,
            namespace: &ns,
            obj: &obj,
            name: &name,
            tenant_api: &tenant_api,
            control: &control,
            active_split: active_split.as_ref(),
            tenant_ranges: &tenant_ranges,
            bootstrap: &bootstrap,
            wal_topic: &wal_topic,
            config_topic: &cfg_topic,
            policy: &policy,
            image: &compute_image,
            compute_policy,
            record: &record,
            direct_bootstrap_grace_ms,
            kafka_sasl,
            range_control_enabled,
            range_tls_hash: range_tls_hash.as_deref(),
        })
        .await
    }
    .await;
    match reconcile_result {
        Ok(action) => Ok(action),
        Err(error) => {
            patch_reconcile_failed(
                &tenant_api,
                &name,
                &obj,
                &error,
                registry_version,
                lifecycle_state,
                direct_bootstrap_grace_ms,
            )
            .await?;
            Err(error)
        }
    }
}

fn reconcile_tenant_ranges(
    current_record: Option<&TenantRecord>,
    spec_ranges: &[GresTenantRangeSpec],
    active_split: Option<&crabka_gres_control::SplitOperationRecord>,
) -> Vec<GresTenantRangeSpec> {
    active_split
        .filter(|operation| successors_may_be_deployed(operation))
        .and_then(|operation| operation.plan.as_ref())
        .map_or_else(
            || reconcile_ranges(current_record, spec_ranges),
            |plan| {
                plan.target_layout
                    .iter()
                    .map(|range| GresTenantRangeSpec {
                        range_id: range.range_id,
                        end_key: range.end_key.map(range_key_from_boundary),
                    })
                    .collect()
            },
        )
}

async fn patch_reconcile_failed(
    tenant_api: &Api<GresTenant>,
    name: &str,
    obj: &GresTenant,
    error: &ReconcileError,
    registry_version: Option<u64>,
    lifecycle_phase: TenantState,
    direct_bootstrap_grace_ms: u64,
) -> Result<(), ReconcileError> {
    patch_status(
        tenant_api,
        name,
        obj,
        &TenantStatusUpdate {
            status: "False",
            reason: "ReconcileFailed",
            message: &error.to_string(),
            registry_version,
            lifecycle_phase,
            advance_generation: false,
            direct_bootstrap_grace_ms: Some(direct_bootstrap_grace_ms),
        },
    )
    .await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParkingProgress {
    NotNeeded,
    DeletionPending,
    Complete,
}

struct TenantResourceConfig<'a> {
    ctx: &'a Context,
    namespace: &'a str,
    obj: &'a GresTenant,
    name: &'a str,
    tenant_name: &'a TenantName,
    tenant_ranges: &'a [GresTenantRangeSpec],
    config_topic: &'a str,
    defaults: EffectiveDefaults,
    current_record: Option<&'a TenantRecord>,
    lifecycle_state: TenantState,
}

async fn provision_tenant_resources(
    admin: &mut tokio::sync::MutexGuard<'_, dyn crabka_client_admin::AdminClientLike + Send>,
    config: &TenantResourceConfig<'_>,
) -> Result<String, ReconcileError> {
    let mut topic_specs = vec![CreateTopicSpec {
        name: config.config_topic.to_owned(),
        partitions: 1,
        replicas: config.defaults.wal_replication,
        configs: BTreeMap::from([("cleanup.policy".to_string(), "compact".to_string())]),
    }];
    if matches!(
        config.lifecycle_state,
        TenantState::Active | TenantState::ResumeRequested
    ) {
        let wal_generation = config
            .current_record
            .map_or(0, |record| record.wal_generation);
        topic_specs.extend(config.tenant_ranges.iter().map(|range| CreateTopicSpec {
            name: wal_topic_for_generation(config.tenant_name, range.range_id, wal_generation),
            partitions: 1,
            replicas: config.defaults.wal_replication,
            configs: BTreeMap::new(),
        }));
    }
    let missing = missing_topics(admin, &topic_specs).await?;
    if !missing.is_empty() {
        admin.create_topics(&missing, 30_000).await?;
    }

    let password = read_password_secret(
        config.ctx,
        config.namespace,
        &config.obj.spec.password_secret_ref,
    )
    .await?;
    let kafka_username = tenant_kafka_username(config.tenant_name);
    admin
        .alter_user_scram_credentials_sha512(
            &[ScramUpsertion {
                username: kafka_username.clone(),
                password: password.clone(),
                iterations: DEFAULT_SCRAM_ITERATIONS,
            }],
            &[],
        )
        .await?;

    let principal = format!("User:{kafka_username}");
    let desired_acls = tenant_acls(&principal, config.tenant_name)
        .into_iter()
        .collect();
    let current_acls = admin
        .describe_acls(&AclEntryFilter {
            principal: Some(principal),
            ..Default::default()
        })
        .await?
        .into_iter()
        .collect();
    let (additions, deletions) = diff_acls(&current_acls, &desired_acls);
    if !additions.is_empty() {
        admin.create_acls(&additions).await?;
    }
    if !deletions.is_empty() {
        let filters: Vec<_> = deletions.iter().map(entry_to_exact_filter).collect();
        admin.delete_acls(&filters).await?;
    }

    // Publish stable Service DNS before writing those names into the range registry.
    let svc_api: Api<Service> = Api::namespaced(config.ctx.client.clone(), config.namespace);
    cleanup_obsolete_range_resources(
        config.ctx,
        config.namespace,
        config.obj,
        config.current_record,
        config.tenant_ranges,
    )
    .await?;
    if config.tenant_ranges.len() > 1 {
        apply_object(
            &svc_api,
            &front_door_service_name(config.name),
            &render_service(config.obj)?,
        )
        .await?;
    }
    for range in config.tenant_ranges {
        apply_object(
            &svc_api,
            &range_service_name(config.name, range.range_id),
            &render_range_service(config.obj, range.range_id)?,
        )
        .await?;
    }
    Ok(password)
}

struct ComputeStatusConfig<'a> {
    ctx: &'a Context,
    namespace: &'a str,
    obj: &'a GresTenant,
    name: &'a str,
    tenant_api: &'a Api<GresTenant>,
    control: &'a crate::context::GresControlHandle,
    active_split: Option<&'a crabka_gres_control::SplitOperationRecord>,
    tenant_ranges: &'a [GresTenantRangeSpec],
    bootstrap: &'a str,
    wal_topic: &'a str,
    config_topic: &'a str,
    policy: &'a crabka_gres_control::RegistryPolicy,
    image: &'a str,
    compute_policy: EffectiveGresComputePolicy,
    record: &'a TenantRecord,
    direct_bootstrap_grace_ms: u64,
    kafka_sasl: bool,
    range_control_enabled: bool,
    range_tls_hash: Option<&'a str>,
}

async fn reconcile_compute_and_status(
    config: &ComputeStatusConfig<'_>,
) -> Result<Action, ReconcileError> {
    let deployments_ready = reconcile_compute_deployments(
        config.ctx,
        config.namespace,
        config.obj,
        &ComputeDeploymentConfig {
            ranges: config.tenant_ranges,
            bootstrap: config.bootstrap,
            wal_topic: config.wal_topic,
            config_topic: config.config_topic,
            policy: config.policy,
            image: config.image,
            compute_policy: config.compute_policy,
            lifecycle_state: config.record.state,
            kafka_sasl: config.kafka_sasl,
            range_control_enabled: config.range_control_enabled,
            range_tls_hash: config.range_tls_hash,
        },
    )
    .await?;
    if deployments_ready && let Some(operation) = config.active_split {
        let mutation_client =
            operator_control_mutation_client(config.ctx, config.namespace, config.obj).await?;
        if operation.phase == crabka_gres_control::SplitOperationPhase::Activated {
            verify_target_topology_ready(&mutation_client, operation)
                .await
                .map_err(|error| ReconcileError::Malformed(error.to_string()))?;
            reconcile_activated_cutover(config.control, operation)
                .await
                .map_err(|error| ReconcileError::Malformed(error.to_string()))?;
        } else {
            reconcile_one_rpc_phase(config.control, &mutation_client, operation)
                .await
                .map_err(|error| ReconcileError::Malformed(error.to_string()))?;
        }
        return Ok(lifecycle_requeue(&config.compute_policy));
    }

    let policy_api: Api<NetworkPolicy> =
        Api::namespaced(config.ctx.client.clone(), config.namespace);
    apply_object(
        &policy_api,
        &network_policy_name(config.name),
        &render_range_compute_network_policy(config.obj)?,
    )
    .await?;
    let (status, reason, message, endpoint_ready) = if deployments_ready {
        ("True", "Ready", "tenant in sync", true)
    } else {
        (
            "False",
            "ComputeProgressing",
            "waiting for all range compute Deployments to become available",
            false,
        )
    };
    patch_status(
        config.tenant_api,
        config.name,
        config.obj,
        &TenantStatusUpdate {
            status,
            reason,
            message,
            registry_version: Some(config.record.record_version),
            lifecycle_phase: config.record.state,
            advance_generation: endpoint_ready,
            direct_bootstrap_grace_ms: Some(config.direct_bootstrap_grace_ms),
        },
    )
    .await?;
    Ok(lifecycle_requeue(&config.compute_policy))
}

fn lifecycle_requeue(policy: &EffectiveGresComputePolicy) -> Action {
    Action::requeue(Duration::from_millis(
        policy.lifecycle_requeue_ms.into_value(),
    ))
}

async fn reconcile_range_tls_secret(
    ctx: &Context,
    namespace: &str,
    obj: &GresTenant,
) -> Result<String, ReconcileError> {
    let api: Api<Secret> = Api::namespaced(ctx.client.clone(), namespace);
    let name = range_tls_secret_name(&obj.name_any());
    let existing = api.get_opt(&name).await?;
    let (secret, hash) = render_range_tls_secret(obj, existing.as_ref())?;
    apply_object(&api, &name, &secret).await?;
    Ok(hash)
}

async fn operator_control_mutation_client(
    ctx: &Context,
    namespace: &str,
    obj: &GresTenant,
) -> Result<MtlsRangeMutationClient, ReconcileError> {
    let api: Api<Secret> = Api::namespaced(ctx.client.clone(), namespace);
    let range_secret = api.get(&range_tls_secret_name(&obj.name_any())).await?;
    let operator_name = operator_control_tls_secret_name(&obj.name_any());
    let existing = api.get_opt(&operator_name).await?;
    let secret = render_operator_control_tls_secret(obj, &range_secret, existing.as_ref())?;
    apply_object(&api, &operator_name, &secret).await?;
    let data = secret.data.as_ref().ok_or_else(|| {
        ReconcileError::Malformed("operator control TLS secret has no data".into())
    })?;
    let bytes = |key: &str| {
        data.get(key)
            .map(|value| value.0.as_slice())
            .ok_or_else(|| {
                ReconcileError::Malformed(format!("operator control TLS secret is missing {key}"))
            })
    };
    let client = crabka_gres_ranges::FramedTcpClient::with_tls_pem(
        bytes("tls.crt")?,
        bytes("tls.key")?,
        bytes("ca.crt")?,
        format!("{}.range.internal", obj.name_any()),
    )
    .map_err(|error| ReconcileError::Malformed(format!("operator control TLS: {error}")))?;
    Ok(MtlsRangeMutationClient::new(client))
}

struct ComputeDeploymentConfig<'a> {
    ranges: &'a [GresTenantRangeSpec],
    bootstrap: &'a str,
    wal_topic: &'a str,
    config_topic: &'a str,
    policy: &'a crabka_gres_control::RegistryPolicy,
    image: &'a str,
    compute_policy: EffectiveGresComputePolicy,
    lifecycle_state: TenantState,
    kafka_sasl: bool,
    range_control_enabled: bool,
    range_tls_hash: Option<&'a str>,
}

async fn reconcile_compute_deployments(
    ctx: &Context,
    namespace: &str,
    obj: &GresTenant,
    config: &ComputeDeploymentConfig<'_>,
) -> Result<bool, ReconcileError> {
    let dep_api: Api<Deployment> = Api::namespaced(ctx.client.clone(), namespace);
    let tenant_name = obj.name_any();
    let mut all_ready = true;
    for range in config.ranges {
        let deployment = render_deployment(
            obj,
            range,
            &DeploymentRenderConfig {
                all_ranges: config.ranges,
                image: config.image,
                readiness_probe_period_seconds: config
                    .compute_policy
                    .readiness_probe_period_seconds,
                bootstrap: config.bootstrap,
                wal_topic: config.wal_topic,
                config_topic: config.config_topic,
                policy: config.policy,
                compute_policy: config.compute_policy,
                replicas: compute_replicas(config.lifecycle_state),
                operator_config: &ctx.config,
                kafka_sasl: config.kafka_sasl,
                range_control_enabled: config.range_control_enabled,
                range_tls_hash: config.range_tls_hash,
            },
        )?;
        apply_object(
            &dep_api,
            &deployment_name(&tenant_name, range.range_id),
            &deployment,
        )
        .await?;
        let observed = dep_api
            .get(&deployment_name(&tenant_name, range.range_id))
            .await?;
        all_ready &= deployment_is_ready(&observed);
    }
    Ok(all_ready)
}

fn deployment_is_ready(deployment: &Deployment) -> bool {
    let desired = deployment
        .spec
        .as_ref()
        .and_then(|spec| spec.replicas)
        .unwrap_or(1);
    let generation = deployment.metadata.generation.unwrap_or_default();
    deployment.status.as_ref().is_some_and(|status| {
        status.observed_generation.unwrap_or_default() >= generation
            && status.available_replicas.unwrap_or_default() >= desired
    })
}

async fn park_suspended_tenant_wal(
    control: &crate::context::GresControlHandle,
    admin: &mut tokio::sync::MutexGuard<'_, dyn crabka_client_admin::AdminClientLike + Send>,
    ranges: &[GresTenantRangeSpec],
    tenant: &TenantName,
    record: &mut TenantRecord,
    expected_record_version: Option<u64>,
) -> Result<ParkingProgress, ReconcileError> {
    let generation_to_park = if record.state == TenantState::Parking {
        record.wal_generation.saturating_sub(1)
    } else {
        record.wal_generation
    };
    let mut ranges_to_park = Vec::with_capacity(ranges.len());
    for range in ranges {
        let topic = wal_topic_for_generation(tenant, range.range_id, generation_to_park);
        if topic_exists(&mut **admin, &topic).await? {
            ranges_to_park.push(range.range_id);
        }
    }
    let parking_record_version = if record.state == TenantState::Suspended {
        if ranges_to_park.is_empty() {
            return Ok(ParkingProgress::NotNeeded);
        }
        control.validate_final_checkpoint_manifest(record).await?;

        let next_generation = record.wal_generation.checked_add(1).ok_or_else(|| {
            ReconcileError::Malformed(format!(
                "WAL generation overflow for suspended tenant {}",
                record.name
            ))
        })?;
        for range in &mut record.ranges {
            range.wal_generation = range.wal_generation.max(next_generation);
        }
        record.wal_generation = next_generation;
        record.state = TenantState::Parking;
        record.ensure_valid()?;
        *record = control
            .replace_tenant_if_version(record, expected_record_version)
            .await?;
        record.record_version
    } else {
        expected_record_version.ok_or_else(|| {
            ReconcileError::Malformed(format!("parking tenant {tenant} has no registry version"))
        })?
    };

    let latest = control.get_tenant(tenant).await?;
    let Some(latest) = latest else {
        return Err(ReconcileError::Malformed(format!(
            "parking intent for tenant {tenant} disappeared before WAL deletion"
        )));
    };
    if latest.state != TenantState::Parking || latest.record_version != parking_record_version {
        return Err(ReconcileError::Malformed(format!(
            "parking intent for tenant {tenant} changed before WAL deletion"
        )));
    }
    *record = latest;

    if !ranges_to_park.is_empty() {
        delete_wal_topics(&mut **admin, tenant, &ranges_to_park, generation_to_park).await?;
    }
    if wal_topics_remain(admin, tenant, ranges, generation_to_park).await? {
        return Ok(ParkingProgress::DeletionPending);
    }
    record.state = TenantState::Suspended;
    record.record_version = parking_record_version.checked_add(1).ok_or_else(|| {
        ReconcileError::Malformed(format!(
            "tenant registry record version overflowed for {tenant}"
        ))
    })?;
    record.ensure_valid()?;
    *record = control
        .replace_tenant_if_version(record, Some(parking_record_version))
        .await?;
    Ok(ParkingProgress::Complete)
}

async fn park_retiring_ranges(
    control: &crate::context::GresControlHandle,
    admin: &mut tokio::sync::MutexGuard<'_, dyn crabka_client_admin::AdminClientLike + Send>,
    tenant: &TenantName,
    record: &mut TenantRecord,
    current: Option<&TenantRecord>,
) -> Result<ParkingProgress, ReconcileError> {
    if let Some(current) = current.filter(|current| {
        current.range_retirements.iter().any(|retirement| {
            retirement.phase == crabka_gres_control::RangeRetirementPhase::Parking
        })
    }) {
        *record = current.clone();
    }
    let split_retirements = record
        .range_retirements
        .iter()
        .filter(|retirement| retirement.phase == crabka_gres_control::RangeRetirementPhase::Parking)
        .map(|retirement| {
            (
                retirement.operation_id.clone(),
                retirement.source_range_id,
                retirement.source_generation,
            )
        })
        .collect::<Vec<_>>();
    for (operation_id, range_id, generation) in split_retirements {
        if record.state != TenantState::Active {
            return Err(ReconcileError::Malformed(
                "split predecessor retirement requires an active tenant".into(),
            ));
        }
        let topic = wal_topic_for_generation(tenant, range_id, generation);
        if topic_exists(&mut **admin, &topic).await? {
            delete_wal_topics(&mut **admin, tenant, &[range_id], generation).await?;
        }
        if topic_exists(&mut **admin, &topic).await? {
            return Ok(ParkingProgress::DeletionPending);
        }
        let expected = record.record_version;
        let parked =
            record
                .clone()
                .confirm_split_predecessor_parked(&operation_id, range_id, generation)?;
        *record = control
            .replace_tenant_if_version(&parked, Some(expected))
            .await?;
    }
    if let Some(current) = current.filter(|current| {
        current
            .ranges
            .iter()
            .any(|range| range.lifecycle == crabka_gres_control::RangeLifecycle::Parking)
    }) {
        *record = current.clone();
    }
    let retiring = record
        .ranges
        .iter()
        .filter(|range| range.lifecycle == crabka_gres_control::RangeLifecycle::Parking)
        .map(|range| {
            let retirement = range
                .retirement
                .as_ref()
                .expect("validated parking metadata");
            (
                range.range_id,
                retirement.operation_id.clone(),
                retirement.retiring_generation,
            )
        })
        .collect::<Vec<_>>();
    if retiring.is_empty() {
        return Ok(ParkingProgress::NotNeeded);
    }
    if record.state != TenantState::Active {
        return Err(ReconcileError::Malformed(
            "range-scoped parking requires an active tenant".into(),
        ));
    }
    for (range_id, operation_id, generation) in retiring {
        let topic = wal_topic_for_generation(tenant, range_id, generation);
        if topic_exists(&mut **admin, &topic).await? {
            delete_wal_topics(&mut **admin, tenant, &[range_id], generation).await?;
        }
        if topic_exists(&mut **admin, &topic).await? {
            return Ok(ParkingProgress::DeletionPending);
        }
        let expected = record.record_version;
        let parked = record
            .clone()
            .confirm_range_parked(range_id, &operation_id, generation)?;
        *record = control
            .replace_tenant_if_version(&parked, Some(expected))
            .await?;
    }
    Ok(ParkingProgress::Complete)
}

/// Reconcile at most one durable split/move predecessor WAL retirement without Kubernetes.
///
/// The registry sidecar is the authority: deletion is replay-safe, and the sidecar advances to
/// `Parked` only after metadata confirms the exact generation topic is absent.
#[async_trait::async_trait]
pub trait RangeRetirementAdmin: Send {
    async fn metadata(
        &mut self,
        topics: &[&str],
    ) -> Result<crabka_client_admin::TopicMetadata, crabka_client_admin::AdminError>;

    async fn delete_topics(
        &mut self,
        names: &[&str],
        timeout_ms: i32,
    ) -> Result<Vec<crabka_client_admin::DeleteTopicOutcome>, crabka_client_admin::AdminError>;
}

#[async_trait::async_trait]
impl<T> RangeRetirementAdmin for T
where
    T: crabka_client_admin::AdminClientLike + Send + ?Sized,
{
    async fn metadata(
        &mut self,
        topics: &[&str],
    ) -> Result<crabka_client_admin::TopicMetadata, crabka_client_admin::AdminError> {
        crabka_client_admin::AdminClientLike::metadata(self, topics).await
    }

    async fn delete_topics(
        &mut self,
        names: &[&str],
        timeout_ms: i32,
    ) -> Result<Vec<crabka_client_admin::DeleteTopicOutcome>, crabka_client_admin::AdminError> {
        crabka_client_admin::AdminClientLike::delete_topics(self, names, timeout_ms).await
    }
}

/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub async fn reconcile_one_retiring_range_wal(
    control: &crate::context::GresControlHandle,
    admin: &mut (dyn RangeRetirementAdmin + Send),
    tenant: &TenantName,
) -> Result<bool, ReconcileError> {
    let Some(record) = control.get_tenant(tenant).await? else {
        return Err(ReconcileError::Malformed(format!(
            "retiring tenant {tenant} disappeared"
        )));
    };
    let Some(retirement) = record
        .range_retirements
        .iter()
        .find(|retirement| retirement.phase == crabka_gres_control::RangeRetirementPhase::Parking)
    else {
        return Ok(true);
    };
    let operation_id = retirement.operation_id.clone();
    let range_id = retirement.source_range_id;
    let generation = retirement.source_generation;
    let topic = wal_topic_for_generation(tenant, range_id, generation);
    if topic_exists(admin, &topic).await? {
        delete_wal_topics(admin, tenant, &[range_id], generation).await?;
    }
    if topic_exists(admin, &topic).await? {
        return Ok(false);
    }
    let expected_version = record.record_version;
    let parked = record.confirm_split_predecessor_parked(&operation_id, range_id, generation)?;
    control
        .replace_tenant_if_version(&parked, Some(expected_version))
        .await?;
    Ok(true)
}

async fn wal_topics_remain(
    admin: &mut tokio::sync::MutexGuard<'_, dyn crabka_client_admin::AdminClientLike + Send>,
    tenant: &TenantName,
    ranges: &[GresTenantRangeSpec],
    generation: u64,
) -> Result<bool, ReconcileError> {
    for range in ranges {
        if topic_exists(
            &mut **admin,
            &wal_topic_for_generation(tenant, range.range_id, generation),
        )
        .await?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn delete_wal_topics<A>(
    admin: &mut A,
    tenant: &TenantName,
    ranges: &[u32],
    generation: u64,
) -> Result<(), ReconcileError>
where
    A: RangeRetirementAdmin + Send + ?Sized,
{
    for range_id in ranges {
        let topic = wal_topic_for_generation(tenant, *range_id, generation);
        let outcomes = admin.delete_topics(&[topic.as_str()], 30_000).await?;
        for outcome in outcomes {
            if let Some(error) = outcome.error {
                return Err(ReconcileError::Malformed(format!(
                    "failed to delete parked WAL topic {}: {} ({})",
                    outcome.name, error.name, error.code
                )));
            }
        }
    }
    Ok(())
}

async fn topic_exists<A>(admin: &mut A, topic: &str) -> Result<bool, ReconcileError>
where
    A: RangeRetirementAdmin + Send + ?Sized,
{
    let metadata = admin.metadata(&[topic]).await?;
    Ok(metadata
        .topics
        .iter()
        .find(|entry| entry.name == topic)
        .is_some_and(|entry| entry.error.is_none()))
}

async fn cleanup_tenant(
    ctx: &Context,
    namespace: &str,
    kafka_name: &str,
    bootstrap: &str,
    policy: &crabka_gres_control::RegistryPolicy,
    tenant: &TenantName,
    _tenant_name: &str,
) {
    if let Ok(control) = ctx
        .gres_control_for(namespace, kafka_name, bootstrap, policy)
        .await
        && let Err(err) = control.delete_tenant(tenant).await
    {
        tracing::warn!(error = %err, tenant = %tenant, "gres tenant tombstone write failed");
    }
    let Ok(admin_handle) = ctx.admin_client_for(kafka_name, bootstrap).await else {
        return;
    };
    let mut admin = admin_handle.lock().await;
    if let Err(err) = admin
        .alter_user_scram_credentials_sha512(
            &[],
            &[ScramDeletion {
                username: tenant_kafka_username(tenant),
            }],
        )
        .await
    {
        tracing::warn!(error = %err, username = %tenant_kafka_username(tenant), "gres tenant SCRAM delete failed");
    }
    if let Err(err) = admin
        .delete_acls(&[AclEntryFilter {
            principal: Some(format!("User:{}", tenant_kafka_username(tenant))),
            ..Default::default()
        }])
        .await
    {
        tracing::warn!(error = %err, username = %tenant_kafka_username(tenant), "gres tenant ACL delete failed");
    }
}

#[derive(Debug, Clone, Copy)]
struct EffectiveDefaults {
    wal_replication: i32,
    checkpoint_frames: Option<u64>,
    checkpoint_bytes: Option<u64>,
    suspend_max_checkpoint_bytes: Option<u64>,
    idle_seconds: Option<u64>,
}

fn effective_defaults(
    base: Option<&TenantDefaults>,
    override_: Option<&TenantDefaults>,
) -> EffectiveDefaults {
    EffectiveDefaults {
        wal_replication: override_
            .and_then(|d| d.wal_replication)
            .or_else(|| base.and_then(|d| d.wal_replication))
            .unwrap_or(1),
        checkpoint_frames: override_
            .and_then(|d| d.checkpoint_frames)
            .or_else(|| base.and_then(|d| d.checkpoint_frames))
            .or(Some(DEFAULT_CHECKPOINT_FRAMES)),
        checkpoint_bytes: override_
            .and_then(|d| d.checkpoint_bytes)
            .or_else(|| base.and_then(|d| d.checkpoint_bytes))
            .or(Some(DEFAULT_CHECKPOINT_BYTES)),
        suspend_max_checkpoint_bytes: override_
            .and_then(|d| d.suspend_max_checkpoint_bytes)
            .or_else(|| base.and_then(|d| d.suspend_max_checkpoint_bytes)),
        idle_seconds: override_
            .and_then(|d| d.idle_seconds)
            .or_else(|| base.and_then(|d| d.idle_seconds)),
    }
}

async fn missing_topics(
    admin: &mut tokio::sync::MutexGuard<'_, dyn crabka_client_admin::AdminClientLike + Send>,
    specs: &[CreateTopicSpec],
) -> Result<Vec<CreateTopicSpec>, ReconcileError> {
    let names: Vec<_> = specs.iter().map(|spec| spec.name.as_str()).collect();
    let metadata = admin.metadata(&names).await?;
    Ok(specs
        .iter()
        .filter(|spec| {
            metadata
                .topics
                .iter()
                .find(|topic| topic.name == spec.name)
                .is_none_or(|topic| topic.error.is_some())
        })
        .cloned()
        .collect())
}

async fn read_password_secret(
    ctx: &Context,
    ns: &str,
    secret: &SecretKeyRef,
) -> Result<String, ReconcileError> {
    let api: Api<Secret> = Api::namespaced(ctx.client.clone(), ns);
    let secret_obj = api.get(&secret.name).await?;
    let Some(data) = secret_obj.data else {
        return Err(ReconcileError::Malformed(format!(
            "Secret {} has no data",
            secret.name
        )));
    };
    let Some(value) = data.get(&secret.key) else {
        return Err(ReconcileError::Malformed(format!(
            "Secret {} missing key {}",
            secret.name, secret.key
        )));
    };
    let password = std::str::from_utf8(&value.0).map_err(|err| {
        ReconcileError::Malformed(format!(
            "Secret {} key {} is not UTF-8: {err}",
            secret.name, secret.key
        ))
    })?;
    if password.is_empty() {
        return Err(ReconcileError::Malformed(format!(
            "Secret {} key {} is empty",
            secret.name, secret.key
        )));
    }
    Ok(password.to_string())
}

fn build_tenant_record(
    obj: &GresTenant,
    tenant_name: &TenantName,
    password: &str,
    record_version: u64,
    defaults: &EffectiveDefaults,
    current: Option<&TenantRecord>,
    desired_ranges: &[GresTenantRangeSpec],
) -> Result<TenantRecord, ReconcileError> {
    let verifier = current
        .filter(|record| verifier_matches_password(&record.scram_verifier, password))
        .map(|record| record.scram_verifier.clone())
        .unwrap_or(
            PgScramVerifier::generate(password, DEFAULT_SCRAM_ITERATIONS as u32)
                .map_err(|err| ReconcileError::Malformed(err.to_string()))?
                .to_string(),
        );
    let state = current.map_or_else(|| requested_state(obj), |record| record.state);
    let mut record = TenantRecord::new(
        record_version,
        TenantId::try_from(tenant_name.as_str())?,
        tenant_name.clone(),
        state,
        SqlUser::try_from(obj.spec.user.as_str())?,
        verifier,
        defaults.wal_replication,
    )?;
    record.checkpoint_frames = defaults.checkpoint_frames;
    record.checkpoint_bytes = defaults.checkpoint_bytes;
    record.suspend_max_checkpoint_bytes = defaults.suspend_max_checkpoint_bytes;
    record.idle_seconds = defaults.idle_seconds;
    if let Some(current) = current {
        record.wal_generation = current.wal_generation;
        record.endpoint.clone_from(&current.endpoint);
        record.bucket_prefix.clone_from(&current.bucket_prefix);
        record.hash_placements.clone_from(&current.hash_placements);
        record
            .range_retirements
            .clone_from(&current.range_retirements);
        record
            .final_checkpoint
            .clone_from(&current.final_checkpoint);
    }
    let mut layout = range_layout_for_ranges(obj, desired_ranges);
    if let Some(current) = current {
        for range in &mut layout {
            if let Some(current_range) = current
                .ranges
                .iter()
                .find(|item| item.range_id == range.range_id)
            {
                range.wal_generation = current_range.wal_generation;
                range.lifecycle = current_range.lifecycle;
                range.retirement.clone_from(&current_range.retirement);
            }
        }
    }
    record = record.with_range_layout(layout)?;
    record.record_version = record_version;
    record.ensure_valid()?;
    Ok(record)
}

fn reconcile_ranges(
    current: Option<&TenantRecord>,
    spec_ranges: &[GresTenantRangeSpec],
) -> Vec<GresTenantRangeSpec> {
    let Some(current) = current else {
        return spec_ranges.to_vec();
    };
    if current.ranges.is_empty() {
        return spec_ranges.to_vec();
    }
    current
        .ranges
        .iter()
        .map(|range| GresTenantRangeSpec {
            range_id: range.range_id,
            end_key: range.end_key.map(range_key_from_boundary),
        })
        .collect()
}

fn range_key_from_boundary(boundary: RangeBoundary) -> GresTenantRangeKey {
    GresTenantRangeKey {
        table_id: boundary.table_id,
        bucket: boundary.bucket,
        rowid: boundary.rowid,
    }
}

fn boundary_from_range_key(key: GresTenantRangeKey) -> RangeBoundary {
    match key.bucket {
        Some(bucket) => RangeBoundary::hash(key.table_id, bucket, key.rowid),
        None => RangeBoundary::new(key.table_id, key.rowid),
    }
}

fn requested_state(obj: &GresTenant) -> TenantState {
    if obj.spec.suspended.unwrap_or(false) {
        return TenantState::Suspended;
    }

    TenantState::Active
}

fn verifier_matches_password(verifier: &str, password: &str) -> bool {
    let Ok(parsed) = PgScramVerifier::parse(verifier) else {
        return false;
    };
    let Ok(candidate) =
        PgScramVerifier::generate_with_salt(password, parsed.iterations, parsed.salt)
    else {
        return false;
    };
    candidate.to_string() == verifier
}

fn tenant_record_changed(current: Option<&TenantRecord>, desired: &TenantRecord) -> bool {
    let Some(current) = current else {
        return true;
    };
    let mut desired_at_current_version = desired.clone();
    desired_at_current_version.record_version = current.record_version;
    desired_at_current_version != *current
}

fn compute_replicas(state: TenantState) -> i32 {
    match state {
        TenantState::Active | TenantState::ResumeRequested => 1,
        TenantState::Parking | TenantState::Suspended => 0,
    }
}

fn tenant_acls(principal: &str, tenant: &TenantName) -> Vec<AclEntry> {
    let wal_prefix = format!("__gres_wal.{tenant}");
    let cfg = tenant_config_topic(tenant);
    let txn = format!("__gres.{tenant}");
    let literal_resources = [
        (
            ResourceType::Topic,
            wal_prefix,
            [
                AclOperation::Read,
                AclOperation::Write,
                AclOperation::Create,
                AclOperation::Delete,
                AclOperation::Describe,
            ]
            .as_slice(),
        ),
        (
            ResourceType::Topic,
            cfg,
            [
                AclOperation::Read,
                AclOperation::Write,
                AclOperation::Create,
                AclOperation::Delete,
                AclOperation::Describe,
            ]
            .as_slice(),
        ),
    ];
    let mut acls: Vec<_> = literal_resources
        .into_iter()
        .flat_map(|(resource_type, resource_name, ops)| {
            ops.iter().copied().map(move |operation| AclEntry {
                resource_type,
                resource_name: resource_name.clone(),
                pattern_type: if resource_name.starts_with("__gres_wal.") {
                    PatternType::Prefixed
                } else {
                    PatternType::Literal
                },
                principal: principal.to_string(),
                host: "*".to_string(),
                operation,
                permission_type: PermissionType::Allow,
            })
        })
        .collect();
    acls.extend(
        [AclOperation::Write, AclOperation::Describe]
            .into_iter()
            .map(|operation| AclEntry {
                resource_type: ResourceType::TransactionalId,
                resource_name: txn.clone(),
                pattern_type: PatternType::Prefixed,
                principal: principal.to_string(),
                host: "*".to_string(),
                operation,
                permission_type: PermissionType::Allow,
            }),
    );
    for operation in [AclOperation::Create, AclOperation::IdempotentWrite] {
        acls.push(AclEntry {
            resource_type: ResourceType::Cluster,
            resource_name: "kafka-cluster".to_string(),
            pattern_type: PatternType::Literal,
            principal: principal.to_string(),
            host: "*".to_string(),
            operation,
            permission_type: PermissionType::Allow,
        });
    }
    acls
}

fn tenant_kafka_username(tenant: &TenantName) -> String {
    format!("gres-{tenant}")
}

fn wal_topic(tenant: &TenantName) -> String {
    wal_topic_for_range(tenant, 0)
}

fn wal_topic_for_range(tenant: &TenantName, range_id: u32) -> String {
    format!("__gres_wal.{tenant}.r{range_id}")
}

fn wal_topic_for_generation(tenant: &TenantName, range_id: u32, generation: u64) -> String {
    let base = wal_topic_for_range(tenant, range_id);
    if generation == 0 {
        base
    } else {
        format!("{base}.g{generation:010}")
    }
}

fn front_door_service_name(name: &str) -> String {
    format!("{name}-gres-pg")
}
fn range_service_name(name: &str, range_id: u32) -> String {
    deployment_name(name, range_id)
}

#[derive(Debug, PartialEq, Eq)]
struct ObsoleteRangeResources {
    deployments: Vec<String>,
    services: Vec<String>,
}

fn obsolete_range_resources(
    tenant: &str,
    previous: &[u32],
    desired: &[u32],
) -> ObsoleteRangeResources {
    let withdrawn: Vec<_> = previous
        .iter()
        .copied()
        .filter(|range_id| !desired.contains(range_id))
        .collect();
    let deployments = withdrawn
        .iter()
        .map(|range_id| deployment_name(tenant, *range_id))
        .collect();
    let mut services: Vec<_> = withdrawn
        .iter()
        .map(|range_id| range_service_name(tenant, *range_id))
        .collect();
    if previous.len() > 1 && desired.len() <= 1 {
        services.push(front_door_service_name(tenant));
    }
    ObsoleteRangeResources {
        deployments,
        services,
    }
}

async fn cleanup_obsolete_range_resources(
    ctx: &Context,
    namespace: &str,
    tenant: &GresTenant,
    current: Option<&TenantRecord>,
    desired: &[GresTenantRangeSpec],
) -> Result<(), ReconcileError> {
    let Some(current) = current else {
        return Ok(());
    };
    let previous: Vec<_> = current.ranges.iter().map(|range| range.range_id).collect();
    let desired: Vec<_> = desired.iter().map(|range| range.range_id).collect();
    let obsolete = obsolete_range_resources(&tenant.name_any(), &previous, &desired);
    let deployment_api: Api<Deployment> = Api::namespaced(ctx.client.clone(), namespace);
    let service_api: Api<Service> = Api::namespaced(ctx.client.clone(), namespace);
    for name in obsolete.deployments {
        if let Some(object) = deployment_api.get_opt(&name).await?
            && is_managed_by_tenant(&object.metadata, tenant)
        {
            deployment_api
                .delete(&name, &DeleteParams::default())
                .await?;
        }
    }
    for name in obsolete.services {
        if let Some(object) = service_api.get_opt(&name).await?
            && is_managed_by_tenant(&object.metadata, tenant)
        {
            service_api.delete(&name, &DeleteParams::default()).await?;
        }
    }
    Ok(())
}

fn is_managed_by_tenant(
    metadata: &k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta,
    tenant: &GresTenant,
) -> bool {
    let tenant_uid = tenant.metadata.uid.as_deref();
    metadata.owner_references.as_ref().is_some_and(|owners| {
        owners.iter().any(|owner| {
            owner.controller == Some(true)
                && Some(owner.uid.as_str()) == tenant_uid
                && owner.kind == GresTenant::kind(&())
        })
    }) && metadata.labels.as_ref().is_some_and(|labels| {
        labels.get("app.kubernetes.io/name").map(String::as_str) == Some(APP_NAME)
            && labels.get("app.kubernetes.io/instance").map(String::as_str)
                == Some(tenant.name_any().as_str())
    })
}
fn deployment_name(name: &str, range_id: u32) -> String {
    if range_id == 0 {
        return format!("{name}-gres");
    }
    format!("{name}-gres-r{range_id}")
}

fn network_policy_name(name: &str) -> String {
    format!("{name}-gres-range-policy")
}

fn range_tls_secret_name(name: &str) -> String {
    format!("{name}-gres-range-tls")
}

fn operator_control_tls_secret_name(name: &str) -> String {
    format!("{name}-gres-operator-control-tls")
}

fn range_tls_identity(obj: &GresTenant) -> String {
    format!("{}|{}.range.internal", obj.name_any(), obj.name_any())
}

fn range_tls_data_hash(data: &BTreeMap<String, ByteString>) -> String {
    let mut digest = Sha256::new();
    for key in ["ca.crt", "tls.crt", "tls.key"] {
        digest.update(key.as_bytes());
        if let Some(value) = data.get(key) {
            digest.update(&value.0);
        }
    }
    hex::encode(digest.finalize())
}

fn existing_range_tls_is_current(existing: &Secret, identity: &str) -> bool {
    let identity_matches = existing
        .metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get(RANGE_TLS_IDENTITY_ANNOTATION))
        .is_some_and(|value| value == identity);
    let Some(data) = existing.data.as_ref() else {
        return false;
    };
    if !identity_matches
        || !["ca.crt", "ca.key", "tls.crt", "tls.key"]
            .iter()
            .all(|key| data.contains_key(*key))
    {
        return false;
    }
    let Ok(cert_pem) = std::str::from_utf8(&data["tls.crt"].0) else {
        return false;
    };
    crate::controller::cluster_ca::cert_not_after(cert_pem).is_ok_and(|not_after| {
        not_after > time::OffsetDateTime::now_utc() + time::Duration::days(30)
    })
}

fn render_range_tls_secret(
    obj: &GresTenant,
    existing: Option<&Secret>,
) -> Result<(Secret, String), ReconcileError> {
    let name = obj.name_any();
    let identity = range_tls_identity(obj);
    let data = if let Some(existing) =
        existing.filter(|secret| existing_range_tls_is_current(secret, &identity))
    {
        existing.data.clone().expect("validated existing TLS data")
    } else {
        let ca = generate_cluster_ca(&format!("{name}-range-ca"), 365).map_err(|error| {
            ReconcileError::Malformed(format!("range TLS CA generation failed: {error}"))
        })?;
        let leaf = issue_broker_cert(
            &ca.cert_pem,
            &ca.key_pem,
            &format!("{name}-range"),
            &[SubjectAltName::Dns(format!("{name}.range.internal"))],
            &[],
            90,
        )
        .map_err(|error| {
            ReconcileError::Malformed(format!("range TLS leaf issuance failed: {error}"))
        })?;
        BTreeMap::from([
            ("ca.crt".into(), ByteString(ca.cert_pem.into_bytes())),
            ("ca.key".into(), ByteString(ca.key_pem.into_bytes())),
            ("tls.crt".into(), ByteString(leaf.cert_pem.into_bytes())),
            ("tls.key".into(), ByteString(leaf.key_pem.into_bytes())),
        ])
    };
    let hash = range_tls_data_hash(&data);
    let secret = Secret {
        metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some(range_tls_secret_name(&name)),
            namespace: obj.namespace(),
            labels: Some(meta_labels(obj)),
            annotations: Some(BTreeMap::from([
                (RANGE_TLS_IDENTITY_ANNOTATION.into(), identity),
                (RANGE_TLS_HASH_ANNOTATION.into(), hash.clone()),
            ])),
            owner_references: Some(vec![owner_ref::<GresTenant>(obj)?]),
            ..Default::default()
        },
        data: Some(data),
        type_: Some("kubernetes.io/tls".into()),
        ..Default::default()
    };
    Ok((secret, hash))
}

fn render_operator_control_tls_secret(
    obj: &GresTenant,
    range_secret: &Secret,
    existing: Option<&Secret>,
) -> Result<Secret, ReconcileError> {
    let name = obj.name_any();
    let range_data = range_secret.data.as_ref().ok_or_else(|| {
        ReconcileError::Malformed("range TLS secret has no certificate data".into())
    })?;
    let bytes = |key: &str| {
        range_data
            .get(key)
            .map(|value| value.0.as_slice())
            .ok_or_else(|| ReconcileError::Malformed(format!("range TLS secret is missing {key}")))
    };
    let ca_cert = std::str::from_utf8(bytes("ca.crt")?)
        .map_err(|error| ReconcileError::Malformed(error.to_string()))?;
    let ca_key = std::str::from_utf8(bytes("ca.key")?)
        .map_err(|error| ReconcileError::Malformed(error.to_string()))?;
    let range_hash = range_secret
        .metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get(RANGE_TLS_HASH_ANNOTATION))
        .cloned()
        .ok_or_else(|| {
            ReconcileError::Malformed("range TLS secret has no CA identity hash".into())
        })?;
    let operator_identity = format!("CN={name}-operator");
    if let Some(existing) = existing
        .filter(|secret| operator_control_tls_is_current(secret, &range_hash, &operator_identity))
    {
        return Ok(existing.clone());
    }
    let leaf = issue_broker_cert(
        ca_cert,
        ca_key,
        &format!("{name}-operator"),
        &[SubjectAltName::Dns(format!(
            "{name}-operator.range.internal"
        ))],
        &[],
        90,
    )
    .map_err(|error| {
        ReconcileError::Malformed(format!("operator control TLS issuance failed: {error}"))
    })?;
    Ok(Secret {
        metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some(operator_control_tls_secret_name(&name)),
            namespace: obj.namespace(),
            labels: Some(meta_labels(obj)),
            annotations: Some(BTreeMap::from([
                (RANGE_TLS_HASH_ANNOTATION.into(), range_hash),
                (RANGE_TLS_IDENTITY_ANNOTATION.into(), operator_identity),
            ])),
            owner_references: Some(vec![owner_ref::<GresTenant>(obj)?]),
            ..Default::default()
        },
        data: Some(BTreeMap::from([
            ("ca.crt".into(), ByteString(bytes("ca.crt")?.to_vec())),
            ("tls.crt".into(), ByteString(leaf.cert_pem.into_bytes())),
            ("tls.key".into(), ByteString(leaf.key_pem.into_bytes())),
        ])),
        type_: Some("kubernetes.io/tls".into()),
        ..Default::default()
    })
}

fn operator_control_tls_is_current(secret: &Secret, range_hash: &str, identity: &str) -> bool {
    let annotations = secret.metadata.annotations.as_ref();
    if annotations
        .and_then(|values| values.get(RANGE_TLS_HASH_ANNOTATION))
        .map(String::as_str)
        != Some(range_hash)
        || annotations
            .and_then(|values| values.get(RANGE_TLS_IDENTITY_ANNOTATION))
            .map(String::as_str)
            != Some(identity)
    {
        return false;
    }
    let Some(data) = secret.data.as_ref() else {
        return false;
    };
    let Some(cert) = data.get("tls.crt") else {
        return false;
    };
    if !data.contains_key("tls.key") || !data.contains_key("ca.crt") {
        return false;
    }
    let Ok(cert_pem) = std::str::from_utf8(&cert.0) else {
        return false;
    };
    let Ok((_, pem)) = x509_parser::pem::parse_x509_pem(cert_pem.as_bytes()) else {
        return false;
    };
    crabka_security::extract_principal_from_cert(&pem.contents)
        .is_some_and(|principal| principal == identity)
        && crate::controller::cluster_ca::cert_not_after(cert_pem).is_ok_and(|not_after| {
            not_after > time::OffsetDateTime::now_utc() + time::Duration::days(30)
        })
}

fn selector_labels(obj: &GresTenant) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("app.kubernetes.io/name".into(), APP_NAME.into()),
        ("app.kubernetes.io/instance".into(), obj.name_any()),
        ("app.kubernetes.io/component".into(), "gres-tenant".into()),
    ])
}

fn range_labels(obj: &GresTenant, range_id: u32) -> BTreeMap<String, String> {
    let mut labels = selector_labels(obj);
    labels.insert("crabka.io/gres-range".into(), format!("r{range_id}"));
    labels
}

fn effective_ranges(
    ranges: &[GresTenantRangeSpec],
) -> Result<Vec<GresTenantRangeSpec>, ReconcileError> {
    if ranges.is_empty() {
        return Ok(vec![GresTenantRangeSpec {
            range_id: 0,
            end_key: None,
        }]);
    }
    validate_ranges(ranges)?;
    Ok(ranges.to_vec())
}

fn validate_ranges(ranges: &[GresTenantRangeSpec]) -> Result<(), ReconcileError> {
    let mut seen = std::collections::BTreeSet::new();
    let mut previous_end = None;
    for (index, range) in ranges.iter().enumerate() {
        if !seen.insert(range.range_id) {
            return Err(ReconcileError::Malformed(
                "GresTenant.spec.ranges rangeId values must be unique".into(),
            ));
        }
        if range.range_id != u32::try_from(index).unwrap_or(u32::MAX) {
            return Err(ReconcileError::Malformed(
                "GresTenant.spec.ranges rangeId values must be contiguous from 0".into(),
            ));
        }
        if range.end_key.is_none() && index + 1 != ranges.len() {
            return Err(ReconcileError::Malformed(
                "only the final GresTenant.spec.ranges entry may omit endKey".into(),
            ));
        }
        if let Some(current) = range.end_key {
            if let Some(previous) = previous_end
                && current <= previous
            {
                return Err(ReconcileError::Malformed(
                    "GresTenant.spec.ranges endKey values must increase".into(),
                ));
            }
            previous_end = Some(current);
        }
    }
    Ok(())
}

fn range_layout_for_ranges(
    obj: &GresTenant,
    ranges: &[GresTenantRangeSpec],
) -> Vec<RangeLayoutEntry> {
    ranges
        .iter()
        .map(|range| RangeLayoutEntry {
            range_id: range.range_id,
            end_key: range.end_key.map(boundary_from_range_key),
            endpoint: format!(
                "{}.{}.svc.cluster.local:{RANGE_PORT}",
                range_service_name(&obj.name_any(), range.range_id),
                obj.namespace().unwrap_or_else(|| "default".into())
            ),
            wal_generation: 0,
            lifecycle: crabka_gres_control::RangeLifecycle::default(),
            retirement: None,
        })
        .collect()
}

fn meta_labels(obj: &GresTenant) -> BTreeMap<String, String> {
    let mut labels = selector_labels(obj);
    labels.insert(
        "app.kubernetes.io/managed-by".into(),
        "crabka-operator".into(),
    );
    labels
}

fn render_service(obj: &GresTenant) -> Result<Service, ReconcileError> {
    let name = obj.name_any();
    Ok(serde_json::from_value(json!({
        "metadata": { "name": front_door_service_name(&name), "namespace": obj.namespace(), "labels": meta_labels(obj), "ownerReferences": [owner_ref::<GresTenant>(obj)?] },
        "spec": { "type": "ClusterIP", "selector": selector_labels(obj), "ports": [{ "name": "postgres", "port": COMPUTE_PORT, "targetPort": COMPUTE_PORT, "protocol": "TCP" }] }
    }))?)
}

fn render_range_service(obj: &GresTenant, range_id: u32) -> Result<Service, ReconcileError> {
    let name = obj.name_any();
    let mut ports = Vec::with_capacity(if range_id == 0 { 2 } else { 1 });
    if range_id == 0 {
        ports.push(json!({ "name": "postgres", "port": COMPUTE_PORT, "targetPort": COMPUTE_PORT, "protocol": "TCP" }));
    }
    ports.push(
        json!({ "name": "range", "port": RANGE_PORT, "targetPort": RANGE_PORT, "protocol": "TCP" }),
    );
    Ok(serde_json::from_value(json!({
        "metadata": { "name": range_service_name(&name, range_id), "namespace": obj.namespace(), "labels": meta_labels(obj), "ownerReferences": [owner_ref::<GresTenant>(obj)?] },
        "spec": { "type": "ClusterIP", "selector": range_labels(obj, range_id), "ports": ports }
    }))?)
}

struct DeploymentRenderConfig<'a> {
    all_ranges: &'a [GresTenantRangeSpec],
    image: &'a str,
    readiness_probe_period_seconds: i32,
    bootstrap: &'a str,
    wal_topic: &'a str,
    config_topic: &'a str,
    policy: &'a crabka_gres_control::RegistryPolicy,
    compute_policy: EffectiveGresComputePolicy,
    replicas: i32,
    operator_config: &'a crate::config::OperatorConfig,
    kafka_sasl: bool,
    range_control_enabled: bool,
    range_tls_hash: Option<&'a str>,
}

fn render_deployment(
    obj: &GresTenant,
    range: &GresTenantRangeSpec,
    config: &DeploymentRenderConfig<'_>,
) -> Result<Deployment, ReconcileError> {
    let name = obj.name_any();
    let selector = range_labels(obj, range.range_id);
    let host_ranges = host_ranges_arg(range.range_id);
    let ranges = ranges_arg(config.all_ranges);
    let compute_policy = config.compute_policy;
    let mut args = vec![
        "--listen".to_owned(),
        format!("0.0.0.0:{COMPUTE_PORT}"),
        "--substrate-bootstrap".to_owned(),
        config.bootstrap.to_owned(),
        "--tenant".to_owned(),
        name.clone(),
        "--registry-replication-factor".to_owned(),
        config.policy.replication_factor().to_string(),
        "--registry-topic-create-timeout-ms".to_owned(),
        config.policy.topic_create_timeout_ms().to_string(),
        "--registry-reader-retry-backoff-ms".to_owned(),
        config.policy.reader_retry_backoff().as_millis().to_string(),
        "--registry-fetch-max-wait-ms".to_owned(),
        config.policy.fetch_max_wait_ms().to_string(),
        "--registry-fetch-partition-max-bytes".to_owned(),
        config.policy.fetch_partition_max_bytes().to_string(),
        "--wal-recovery-fetch-max-wait-ms".to_owned(),
        compute_policy
            .wal_recovery_fetch_max_wait_ms
            .into_value()
            .to_string(),
        "--wal-recovery-fetch-partition-max-bytes".to_owned(),
        compute_policy
            .wal_recovery_fetch_partition_max_bytes
            .into_value()
            .to_string(),
        "--wal-recovery-fetch-response-max-bytes".to_owned(),
        compute_policy
            .wal_recovery_fetch_response_max_bytes
            .into_value()
            .to_string(),
        "--wal-recovery-empty-fetch-retries".to_owned(),
        compute_policy
            .wal_recovery_empty_fetch_retries
            .into_value()
            .to_string(),
        "--wal-recovery-dns-timeout-ms".to_owned(),
        u64::to_string(&compute_policy.wal_recovery_dns_timeout_ms.into_value()),
        "--wal-recovery-connect-timeout-ms".to_owned(),
        compute_policy
            .wal_recovery_connect_timeout_ms
            .into_value()
            .to_string(),
        "--wal-recovery-request-timeout-ms".to_owned(),
        compute_policy
            .wal_recovery_request_timeout_ms
            .into_value()
            .to_string(),
        "--wal-topic-replication-factor".to_owned(),
        compute_policy
            .wal_topic_replication_factor
            .into_value()
            .to_string(),
        "--wal-topic-ensure-timeout-ms".to_owned(),
        compute_policy
            .wal_topic_ensure_timeout_ms
            .into_value()
            .to_string(),
        "--wal-admin-connect-timeout-ms".to_owned(),
        compute_policy
            .wal_admin_connect_timeout_ms
            .into_value()
            .to_string(),
        "--wal-admin-request-timeout-ms".to_owned(),
        compute_policy
            .wal_admin_request_timeout_ms
            .into_value()
            .to_string(),
    ];
    args.extend(wal_producer_args(&compute_policy));
    if config.range_control_enabled {
        args.extend([
            "--ranges".to_owned(),
            ranges.clone(),
            "--host-ranges".to_owned(),
            host_ranges.clone(),
            "--range-listen".to_owned(),
            format!("0.0.0.0:{RANGE_PORT}"),
            "--range-tls-cert".to_owned(),
            format!("{RANGE_TLS_DIR}/tls.crt"),
            "--range-tls-key".to_owned(),
            format!("{RANGE_TLS_DIR}/tls.key"),
            "--range-tls-ca".to_owned(),
            format!("{RANGE_TLS_DIR}/ca.crt"),
            "--range-tls-server-name".to_owned(),
            format!("{name}.range.internal"),
            "--range-allowed-principal".to_owned(),
            format!("CN={name}-range"),
            "--operator-control-principal".to_owned(),
            format!("CN={name}-operator"),
            "--range0-follower-poll-interval-ms".to_owned(),
            compute_policy
                .range0_follower_poll_interval_ms
                .into_value()
                .to_string(),
        ]);
    }
    let checkpoint_runtime_args = checkpoint_runtime_args(config.operator_config)?;
    if !checkpoint_runtime_args.is_empty() {
        args.extend([
            "--checkpoint-part-bytes".to_owned(),
            compute_policy
                .checkpoint_part_bytes
                .into_value()
                .to_string(),
            "--checkpoint-retain".to_owned(),
            compute_policy.checkpoint_retain.into_value().to_string(),
            "--checkpoint-delete-records-timeout-ms".to_owned(),
            compute_policy
                .checkpoint_delete_records_timeout_ms
                .into_value()
                .to_string(),
            "--checkpoint-poll-interval-ms".to_owned(),
            compute_policy
                .checkpoint_poll_interval_ms
                .into_value()
                .to_string(),
            "--idle-suspend-poll-interval-ms".to_owned(),
            compute_policy
                .idle_suspend_poll_interval_ms
                .into_value()
                .to_string(),
        ]);
        args.extend(checkpoint_runtime_args);
    }
    let mut env = vec![
        json!({ "name": "KAFKA_BOOTSTRAP_SERVERS", "value": config.bootstrap }),
        json!({ "name": "GRES_TENANT", "value": name }),
        json!({ "name": "GRES_WAL_TOPIC", "value": config.wal_topic }),
        json!({ "name": "GRES_CONFIG_TOPIC", "value": config.config_topic }),
        json!({ "name": "GRES_RANGES", "value": ranges }),
        json!({ "name": "GRES_HOST_RANGES", "value": host_ranges }),
    ];
    if config.kafka_sasl {
        env.push(json!({ "name": "GRES_KAFKA_USERNAME", "value": format!("gres-{name}") }));
        env.push(json!({ "name": "GRES_KAFKA_PASSWORD", "valueFrom": { "secretKeyRef": { "name": obj.spec.password_secret_ref.name, "key": obj.spec.password_secret_ref.key } } }));
    }
    if let Some(access_key) = &config.operator_config.gres_checkpoint_access_key_id {
        env.push(json!({ "name": "AWS_ACCESS_KEY_ID", "value": access_key }));
    }
    if let Some(secret_key) = &config.operator_config.gres_checkpoint_secret_access_key {
        env.push(json!({ "name": "AWS_SECRET_ACCESS_KEY", "value": secret_key }));
    }
    let mut ports =
        vec![json!({ "name": "postgres", "containerPort": COMPUTE_PORT, "protocol": "TCP" })];
    let (range_tls_mounts, range_tls_volumes) = if config.range_control_enabled {
        ports.push(json!({ "name": "range", "containerPort": RANGE_PORT, "protocol": "TCP" }));
        (
            vec![json!({ "name": "range-tls", "mountPath": RANGE_TLS_DIR, "readOnly": true })],
            vec![json!({ "name": "range-tls", "secret": {
                    "secretName": format!("{name}-gres-range-tls"),
                    "items": [
                        { "key": "ca.crt", "path": "ca.crt" },
                        { "key": "tls.crt", "path": "tls.crt" },
                        { "key": "tls.key", "path": "tls.key" }
                    ]
                } })],
        )
    } else {
        (Vec::new(), Vec::new())
    };
    let pod_annotations = config
        .range_tls_hash
        .map(|hash| BTreeMap::from([(RANGE_TLS_HASH_ANNOTATION.to_owned(), hash.to_owned())]));
    Ok(serde_json::from_value(json!({
        "metadata": { "name": deployment_name(&name, range.range_id), "namespace": obj.namespace(), "labels": meta_labels(obj), "ownerReferences": [owner_ref::<GresTenant>(obj)?] },
        "spec": {
            "replicas": config.replicas,
            "selector": { "matchLabels": selector },
            "template": {
                "metadata": { "labels": selector, "annotations": pod_annotations },
                "spec": {
                    "securityContext": { "runAsNonRoot": true, "runAsUser": 65532, "fsGroup": 65532 },
                    "containers": [{
                        "name": "gres",
                        "image": config.image,
                        "args": args,
                        "env": env,
                        "ports": ports,
                        "volumeMounts": range_tls_mounts,
                        "readinessProbe": { "tcpSocket": { "port": COMPUTE_PORT }, "periodSeconds": config.readiness_probe_period_seconds },
                        "resources": obj.spec.resources.clone().unwrap_or_default()
                    }],
                    "volumes": range_tls_volumes
                }
            }
        }
    }))?)
}

fn wal_producer_flush_args(policy: crabka_client_producer::ProducerFlushTimeout) -> [String; 2] {
    [
        "--wal-producer-flush-timeout-ms".to_owned(),
        policy.milliseconds().to_string(),
    ]
}

fn wal_producer_dns_args(timeout: crabka_client_core::ClientDnsTimeout) -> [String; 2] {
    [
        "--wal-producer-dns-timeout-ms".to_owned(),
        timeout.milliseconds().to_string(),
    ]
}

fn wal_producer_args(policy: &EffectiveGresComputePolicy) -> Vec<String> {
    let mut args = Vec::with_capacity(24);
    args.extend(wal_producer_flush_args(policy.wal_producer_flush_timeout));
    args.extend(wal_producer_dns_args(policy.wal_producer_dns_timeout));
    let retry = policy.wal_producer_retry_policy;
    args.extend([
        "--wal-producer-request-timeout-ms".to_owned(),
        retry.request_timeout().as_millis().to_string(),
        "--wal-producer-retries".to_owned(),
        retry.retries().to_string(),
        "--wal-producer-retry-backoff-ms".to_owned(),
        retry.retry_backoff().as_millis().to_string(),
        "--wal-producer-routing-retry-budget-ms".to_owned(),
        retry.routing_retry_budget().as_millis().to_string(),
        "--wal-producer-init-retry-timeout-ms".to_owned(),
        retry.init_retry_timeout().as_millis().to_string(),
        "--wal-producer-init-max-backoff-ms".to_owned(),
        retry.init_max_backoff().as_millis().to_string(),
        "--wal-producer-transaction-timeout-ms".to_owned(),
        retry.transaction_timeout().as_millis().to_string(),
    ]);
    args.extend(wal_producer_throughput_args(
        policy.wal_producer_throughput_policy,
    ));
    args
}

fn wal_producer_throughput_args(
    policy: crabka_client_producer::ProducerThroughputPolicy,
) -> [String; 6] {
    [
        "--wal-producer-compression".to_owned(),
        policy.compression().to_string(),
        "--wal-producer-linger-ms".to_owned(),
        policy.linger_ms().to_string(),
        "--wal-producer-batch-bytes".to_owned(),
        policy.batch_bytes().to_string(),
    ]
}

fn kafka_internal_listener_requires_sasl(kafka: &Kafka) -> bool {
    !kafka
        .spec
        .inter_broker_listener_name
        .as_deref()
        .unwrap_or("PLAIN")
        .eq_ignore_ascii_case("PLAIN")
}

fn checkpoint_runtime_args(
    config: &crate::config::OperatorConfig,
) -> Result<Vec<String>, ReconcileError> {
    let Some(store) = config.gres_checkpoint_store else {
        return Ok(Vec::new());
    };
    let mut args = vec![
        "--checkpoint-store".to_owned(),
        match store {
            crate::config::GresCheckpointStoreKind::S3 => "s3".to_owned(),
            crate::config::GresCheckpointStoreKind::Gcs => "gcs".to_owned(),
        },
    ];
    let bucket = config.gres_checkpoint_bucket.as_ref().ok_or_else(|| {
        ReconcileError::Malformed("GRES_CHECKPOINT_BUCKET is required".to_owned())
    })?;
    args.extend(["--checkpoint-bucket".to_owned(), bucket.clone()]);
    if let Some(region) = &config.gres_checkpoint_region {
        args.extend(["--checkpoint-region".to_owned(), region.clone()]);
    }
    if let Some(endpoint) = &config.gres_checkpoint_endpoint {
        args.extend(["--checkpoint-endpoint".to_owned(), endpoint.clone()]);
    }
    if config.gres_checkpoint_allow_http {
        args.push("--checkpoint-allow-http".to_owned());
    }
    Ok(args)
}

fn ranges_arg(ranges: &[GresTenantRangeSpec]) -> String {
    let mut starts = vec![GresTenantRangeKey {
        table_id: 0,
        bucket: None,
        rowid: 0,
    }];
    starts.extend(ranges.iter().filter_map(|range| range.end_key));
    starts
        .iter()
        .map(|key| format!("{}:{}", key.table_id, key.rowid))
        .collect::<Vec<_>>()
        .join(",")
}

fn host_ranges_arg(range_id: u32) -> String {
    format!("r{range_id}")
}

fn render_range_compute_network_policy(obj: &GresTenant) -> Result<NetworkPolicy, ReconcileError> {
    let name = obj.name_any();
    let pod_selector = LabelSelector {
        match_labels: Some(selector_labels(obj)),
        match_expressions: None,
    };
    let same_tenant_peer = NetworkPolicyPeer {
        pod_selector: Some(pod_selector.clone()),
        namespace_selector: None,
        ip_block: None,
    };
    let fleet_peer = |component: &str, app_name: &str| NetworkPolicyPeer {
        pod_selector: Some(LabelSelector {
            match_labels: Some(BTreeMap::from([
                ("app.kubernetes.io/name".into(), app_name.into()),
                ("app.kubernetes.io/component".into(), component.into()),
                ("app.kubernetes.io/instance".into(), obj.spec.gres.clone()),
            ])),
            match_expressions: None,
        }),
        namespace_selector: None,
        ip_block: None,
    };
    Ok(NetworkPolicy {
        metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some(network_policy_name(&name)),
            namespace: obj.namespace(),
            labels: Some(meta_labels(obj)),
            owner_references: Some(vec![owner_ref::<GresTenant>(obj)?]),
            ..Default::default()
        },
        spec: Some(NetworkPolicySpec {
            pod_selector: Some(pod_selector),
            policy_types: Some(vec!["Ingress".into()]),
            ingress: Some(vec![
                NetworkPolicyIngressRule {
                    from: Some(vec![same_tenant_peer]),
                    ports: Some(vec![NetworkPolicyPort {
                        protocol: Some("TCP".into()),
                        port: Some(IntOrString::Int(RANGE_PORT)),
                        end_port: None,
                    }]),
                },
                NetworkPolicyIngressRule {
                    from: Some(vec![
                        fleet_peer("pgdog", "crabka-pgdog"),
                        fleet_peer("gres-activator", "crabka-gres-activator"),
                    ]),
                    ports: Some(vec![NetworkPolicyPort {
                        protocol: Some("TCP".into()),
                        port: Some(IntOrString::Int(COMPUTE_PORT)),
                        end_port: None,
                    }]),
                },
            ]),
            egress: None,
        }),
    })
}

fn has_finalizer(obj: &GresTenant) -> bool {
    obj.meta()
        .finalizers
        .as_ref()
        .is_some_and(|f| f.iter().any(|item| item == FINALIZER))
}

async fn add_finalizer(api: &Api<GresTenant>, name: &str) -> Result<(), ReconcileError> {
    api.patch(
        name,
        &PatchParams {
            field_manager: Some(FIELD_MANAGER.into()),
            ..Default::default()
        },
        &Patch::Merge(&json!({ "metadata": { "finalizers": [FINALIZER] } })),
    )
    .await?;
    Ok(())
}

async fn remove_finalizer(api: &Api<GresTenant>, name: &str) -> Result<(), ReconcileError> {
    api.patch(
        name,
        &PatchParams {
            field_manager: Some(FIELD_MANAGER.into()),
            ..Default::default()
        },
        &Patch::Merge(&json!({ "metadata": { "finalizers": [] } })),
    )
    .await?;
    Ok(())
}

struct TenantStatusUpdate<'a> {
    status: &'a str,
    reason: &'a str,
    message: &'a str,
    registry_version: Option<u64>,
    lifecycle_phase: TenantState,
    advance_generation: bool,
    direct_bootstrap_grace_ms: Option<u64>,
}

async fn patch_status(
    api: &Api<GresTenant>,
    name: &str,
    obj: &GresTenant,
    update: &TenantStatusUpdate<'_>,
) -> Result<(), ReconcileError> {
    let observed_generation = if update.advance_generation {
        obj.meta().generation
    } else {
        obj.status.as_ref().and_then(|s| s.observed_generation)
    };
    let tenant = TenantName::try_from(name).ok();
    let previous_phase = obj
        .status
        .as_ref()
        .and_then(|status| status.lifecycle_phase.as_deref());
    let existing_grace = obj
        .status
        .as_ref()
        .and_then(|status| status.pgdog_credential_grace_until_unix_ms);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| ReconcileError::Malformed(format!("system clock: {error}")))?
        .as_millis();
    let pgdog_grace = pgdog_grace_deadline(
        previous_phase,
        existing_grace,
        update.lifecycle_phase,
        u64::try_from(now).unwrap_or(u64::MAX),
        update.direct_bootstrap_grace_ms,
    );
    let body = json!({
        "status": {
            "conditions": [condition("Ready", update.status, update.reason, update.message)],
            "observedGeneration": observed_generation,
            "ready": update.status == "True",
            "walTopic": tenant.as_ref().map(wal_topic),
            "registryVersion": update.registry_version.or_else(|| obj.status.as_ref().and_then(|s| s.registry_version)),
            "lifecyclePhase": update.lifecycle_phase.to_string(),
            "pgdogCredentialGraceUntilUnixMs": pgdog_grace,
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

fn pgdog_grace_deadline(
    previous_phase: Option<&str>,
    existing_grace: Option<u64>,
    lifecycle_phase: TenantState,
    now: u64,
    direct_bootstrap_grace_ms: Option<u64>,
) -> Option<u64> {
    if lifecycle_phase != TenantState::Active {
        return None;
    }
    if previous_phase == Some("active") && existing_grace.is_some() {
        return existing_grace;
    }
    direct_bootstrap_grace_ms.map(|grace| now.saturating_add(grace))
}

fn preserved_lifecycle_state(obj: &GresTenant) -> TenantState {
    match obj
        .status
        .as_ref()
        .and_then(|status| status.lifecycle_phase.as_deref())
    {
        Some("suspended") => TenantState::Suspended,
        Some("parking") => TenantState::Parking,
        Some("resume_requested") => TenantState::ResumeRequested,
        Some("active") => TenantState::Active,
        _ => requested_state(obj),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use clap::Parser as _;

    use super::*;

    #[test]
    fn configured_pgdog_grace_drives_active_transition_deadline() {
        assert!(
            pgdog_grace_deadline(
                Some("suspended"),
                None,
                TenantState::Active,
                10_000,
                Some(7_000)
            ) == Some(17_000)
        );
        assert!(
            pgdog_grace_deadline(
                Some("suspended"),
                None,
                TenantState::Active,
                u64::MAX - 1,
                Some(7_000)
            ) == Some(u64::MAX)
        );
        assert!(
            pgdog_grace_deadline(
                Some("active"),
                Some(12_000),
                TenantState::Active,
                20_000,
                Some(7_000)
            ) == Some(12_000)
        );
        assert!(
            pgdog_grace_deadline(
                Some("active"),
                None,
                TenantState::Active,
                20_000,
                Some(7_000)
            ) == Some(27_000)
        );
        assert!(
            pgdog_grace_deadline(
                Some("active"),
                None,
                TenantState::Active,
                u64::MAX - 1,
                Some(7_000)
            ) == Some(u64::MAX)
        );
        assert!(
            pgdog_grace_deadline(Some("active"), None, TenantState::Active, 20_000, None).is_none()
        );
    }
    use crate::crd::{GresTenantSpec, SecretKeyRef};

    fn fixture_password() -> String {
        std::process::id().to_string()
    }

    #[derive(clap::Parser)]
    struct ConfigArgs {
        #[command(flatten)]
        config: crate::config::OperatorConfig,
    }

    fn tenant() -> GresTenant {
        GresTenant::new(
            "tenant-a",
            GresTenantSpec {
                gres: "fleet".into(),
                image: None,
                user: "alice".into(),
                password_secret_ref: SecretKeyRef {
                    name: "pw".into(),
                    key: "password".into(),
                },
                suspended: Some(false),
                resources: None,
                ranges: Vec::new(),
                overrides: None,
            },
        )
    }

    fn render_test_deployment(
        obj: &GresTenant,
        range: &GresTenantRangeSpec,
        all_ranges: &[GresTenantRangeSpec],
        kafka_sasl: bool,
        range_control_enabled: bool,
        range_tls_hash: Option<&str>,
    ) -> Deployment {
        let operator_config = ConfigArgs::parse_from(["operator"]).config;
        let compute_policy = crate::crd::gres::GresComputeSpec::default()
            .effective_policy()
            .expect("compute policy");
        let wal_topic = format!("__gres_wal.tenant-a.r{}", range.range_id);
        render_deployment(
            obj,
            range,
            &DeploymentRenderConfig {
                all_ranges,
                image: "image",
                readiness_probe_period_seconds: 5,
                bootstrap: "k:9092",
                wal_topic: &wal_topic,
                config_topic: "__gres_cfg.tenant-a",
                policy: &crabka_gres_control::RegistryPolicy::default(),
                compute_policy,
                replicas: 1,
                operator_config: &operator_config,
                kafka_sasl,
                range_control_enabled,
                range_tls_hash,
            },
        )
        .expect("render deployment")
    }

    #[test]
    fn checkpoint_thresholds_use_override_then_fleet_then_compiled_fallback() {
        let defaults = |frames, bytes| TenantDefaults {
            wal_replication: None,
            checkpoint_frames: frames,
            checkpoint_bytes: bytes,
            suspend_max_checkpoint_bytes: None,
            idle_seconds: None,
        };
        for (base, override_, expected_frames, expected_bytes) in [
            (
                None,
                None,
                DEFAULT_CHECKPOINT_FRAMES,
                DEFAULT_CHECKPOINT_BYTES,
            ),
            (
                Some(defaults(Some(11), None)),
                None,
                11,
                DEFAULT_CHECKPOINT_BYTES,
            ),
            (
                Some(defaults(None, Some(12))),
                None,
                DEFAULT_CHECKPOINT_FRAMES,
                12,
            ),
            (
                Some(defaults(Some(11), Some(12))),
                Some(defaults(Some(21), None)),
                21,
                12,
            ),
            (
                Some(defaults(Some(11), Some(12))),
                Some(defaults(None, Some(22))),
                11,
                22,
            ),
        ] {
            let effective = effective_defaults(base.as_ref(), override_.as_ref());
            assert!(effective.checkpoint_frames == Some(expected_frames));
            assert!(effective.checkpoint_bytes == Some(expected_bytes));
        }
    }

    #[test]
    fn tenant_acls_do_not_grant_global_registry_read() {
        let tenant_name = TenantName::try_from("tenant-a").unwrap();
        let acls = tenant_acls("User:gres-tenant-a", &tenant_name);
        assert!(
            acls.iter()
                .any(|acl| acl.resource_name == "__gres_wal.tenant-a"
                    && acl.pattern_type == PatternType::Prefixed)
        );
        assert!(
            acls.iter()
                .any(|acl| acl.resource_name == "__gres_cfg.tenant-a")
        );
        assert!(!acls.iter().any(|acl| acl.resource_name
            == crabka_gres_control::TENANT_REGISTRY_TOPIC
            && acl.operation == AclOperation::Read));
        assert!(
            acls.iter()
                .any(|acl| acl.resource_type == ResourceType::TransactionalId
                    && acl.resource_name == "__gres.tenant-a"
                    && acl.pattern_type == PatternType::Prefixed)
        );
        assert!(
            acls.iter()
                .any(|acl| acl.resource_name == "__gres_wal.tenant-a"
                    && acl.operation == AclOperation::Delete)
        );
    }

    #[test]
    fn compute_network_policy_admits_front_door_and_activator() {
        let mut obj = tenant();
        obj.metadata.namespace = Some("ns".into());
        obj.metadata.uid = Some("uid".into());
        let rendered = serde_json::to_string(
            &render_range_compute_network_policy(&obj).expect("render network policy"),
        )
        .unwrap();

        assert!(rendered.contains("crabka-pgdog"));
        assert!(rendered.contains("crabka-gres-activator"));
        assert!(rendered.contains("fleet"));
    }

    #[test]
    fn tenant_record_hashes_password_without_plaintext() {
        let obj = tenant();
        let password = fixture_password();
        let defaults = EffectiveDefaults {
            wal_replication: 1,
            checkpoint_frames: Some(37),
            checkpoint_bytes: None,
            suspend_max_checkpoint_bytes: None,
            idle_seconds: None,
        };
        let record = build_tenant_record(
            &obj,
            &TenantName::try_from("tenant-a").unwrap(),
            &password,
            1,
            &defaults,
            None,
            &[GresTenantRangeSpec {
                range_id: 0,
                end_key: None,
            }],
        )
        .unwrap();
        assert!(record.scram_verifier.starts_with("SCRAM-SHA-256$"));
        assert!(!record.scram_verifier.contains(&password));
        assert!(record.checkpoint_frames == Some(37));
    }

    #[test]
    fn kafka_deployment_credentials_follow_sasl_mode() {
        let mut obj = tenant();
        obj.metadata.namespace = Some("ns".into());
        obj.metadata.uid = Some("uid".into());
        let ranges = [GresTenantRangeSpec {
            range_id: 0,
            end_key: None,
        }];

        for kafka_sasl in [false, true] {
            let deployment =
                render_test_deployment(&obj, &ranges[0], &ranges, kafka_sasl, false, None);
            let json = serde_json::to_string(&deployment).unwrap();
            assert_eq!(json.contains("secretKeyRef"), kafka_sasl);
            assert_eq!(json.contains("GRES_KAFKA_PASSWORD"), kafka_sasl);
            assert!(!json.contains("hunter2"));
            let args = deployment.spec.unwrap().template.spec.unwrap().containers[0]
                .args
                .clone()
                .unwrap();
            assert!(!args.iter().any(|arg| arg == "--ranges"));
        }
    }

    #[test]
    fn compute_workload_without_checkpoint_store_omits_checkpoint_policy() {
        let mut obj = tenant();
        obj.metadata.namespace = Some("ns".into());
        obj.metadata.uid = Some("uid".into());
        let ranges = [GresTenantRangeSpec {
            range_id: 0,
            end_key: None,
        }];
        let deployment = render_test_deployment(&obj, &ranges[0], &ranges, false, false, None);
        let args = deployment
            .spec
            .unwrap()
            .template
            .spec
            .unwrap()
            .containers
            .into_iter()
            .next()
            .expect("container")
            .args
            .expect("args");

        for absent in [
            "--checkpoint-part-bytes",
            "--checkpoint-retain",
            "--checkpoint-delete-records-timeout-ms",
            "--checkpoint-poll-interval-ms",
            "--idle-suspend-poll-interval-ms",
        ] {
            assert!(!args.iter().any(|arg| arg == absent), "got: {args:?}");
        }
        assert!(
            !args
                .iter()
                .any(|arg| arg == "--range0-follower-poll-interval-ms")
        );
        assert!(
            args.windows(14).any(|window| {
                window
                    == [
                        "--wal-recovery-fetch-max-wait-ms",
                        "100",
                        "--wal-recovery-fetch-partition-max-bytes",
                        "1048576",
                        "--wal-recovery-fetch-response-max-bytes",
                        "52428800",
                        "--wal-recovery-empty-fetch-retries",
                        "100",
                        "--wal-recovery-dns-timeout-ms",
                        "10000",
                        "--wal-recovery-connect-timeout-ms",
                        "10000",
                        "--wal-recovery-request-timeout-ms",
                        "30000",
                    ]
            }),
            "got: {args:?}"
        );
    }

    #[test]
    fn compute_wal_recovery_args_are_exact_in_single_and_multi_range_modes() {
        let mut obj = tenant();
        obj.metadata.namespace = Some("ns".into());
        obj.metadata.uid = Some("uid".into());
        let ranges = [GresTenantRangeSpec {
            range_id: 0,
            end_key: None,
        }];
        let operator_config = ConfigArgs::parse_from(["operator"]).config;
        for (spec, expected) in [
            (
                crate::crd::gres::GresComputeSpec::default(),
                [
                    "100", "1048576", "52428800", "100", "10000", "10000", "30000",
                ],
            ),
            (
                crate::crd::gres::GresComputeSpec {
                    wal_recovery_fetch_max_wait_ms: Some(11),
                    wal_recovery_fetch_partition_max_bytes: Some(22),
                    wal_recovery_fetch_response_max_bytes: Some(33),
                    wal_recovery_empty_fetch_retries: Some(44),
                    wal_recovery_dns_timeout_ms: Some(77),
                    wal_recovery_connect_timeout_ms: Some(55),
                    wal_recovery_request_timeout_ms: Some(66),
                    ..crate::crd::gres::GresComputeSpec::default()
                },
                ["11", "22", "33", "44", "77", "55", "66"],
            ),
        ] {
            let compute_policy = spec.effective_policy().expect("compute policy");
            for range_control_enabled in [false, true] {
                let deployment = render_deployment(
                    &obj,
                    &ranges[0],
                    &DeploymentRenderConfig {
                        all_ranges: &ranges,
                        image: "image",
                        readiness_probe_period_seconds: 5,
                        bootstrap: "k:9092",
                        wal_topic: "__gres_wal.tenant-a.r0",
                        config_topic: "__gres_cfg.tenant-a",
                        policy: &crabka_gres_control::RegistryPolicy::default(),
                        compute_policy,
                        replicas: 1,
                        operator_config: &operator_config,
                        kafka_sasl: false,
                        range_control_enabled,
                        range_tls_hash: None,
                    },
                )
                .expect("render deployment");
                let args = deployment.spec.unwrap().template.spec.unwrap().containers[0]
                    .args
                    .clone()
                    .unwrap();
                let expected = [
                    "--wal-recovery-fetch-max-wait-ms",
                    expected[0],
                    "--wal-recovery-fetch-partition-max-bytes",
                    expected[1],
                    "--wal-recovery-fetch-response-max-bytes",
                    expected[2],
                    "--wal-recovery-empty-fetch-retries",
                    expected[3],
                    "--wal-recovery-dns-timeout-ms",
                    expected[4],
                    "--wal-recovery-connect-timeout-ms",
                    expected[5],
                    "--wal-recovery-request-timeout-ms",
                    expected[6],
                ];
                assert!(
                    args.windows(expected.len())
                        .any(|window| window == expected),
                    "got: {args:?}"
                );
            }
        }
    }

    #[test]
    fn compute_wal_producer_args_are_exact_in_single_and_multi_range_modes() {
        let mut obj = tenant();
        obj.metadata.namespace = Some("ns".into());
        obj.metadata.uid = Some("uid".into());
        let ranges = [GresTenantRangeSpec {
            range_id: 0,
            end_key: None,
        }];
        let operator_config = ConfigArgs::parse_from(["operator"]).config;
        for (spec, expected) in [
            (
                crate::crd::gres::GresComputeSpec::default(),
                [
                    "30000",
                    "2147483647",
                    "100",
                    "30000",
                    "30000",
                    "1000",
                    "60000",
                ],
            ),
            (
                crate::crd::gres::GresComputeSpec {
                    wal_producer_request_timeout_ms: Some(11),
                    wal_producer_retries: Some(12),
                    wal_producer_retry_backoff_ms: Some(13),
                    wal_producer_routing_retry_budget_ms: Some(14),
                    wal_producer_init_retry_timeout_ms: Some(15),
                    wal_producer_init_max_backoff_ms: Some(16),
                    wal_producer_transaction_timeout_ms: Some(17),
                    ..crate::crd::gres::GresComputeSpec::default()
                },
                ["11", "12", "13", "14", "15", "16", "17"],
            ),
        ] {
            let compute_policy = spec.effective_policy().expect("compute policy");
            for range_control_enabled in [false, true] {
                let deployment = render_deployment(
                    &obj,
                    &ranges[0],
                    &DeploymentRenderConfig {
                        all_ranges: &ranges,
                        image: "image",
                        readiness_probe_period_seconds: 5,
                        bootstrap: "k:9092",
                        wal_topic: "__gres_wal.tenant-a.r0",
                        config_topic: "__gres_cfg.tenant-a",
                        policy: &crabka_gres_control::RegistryPolicy::default(),
                        compute_policy,
                        replicas: 1,
                        operator_config: &operator_config,
                        kafka_sasl: false,
                        range_control_enabled,
                        range_tls_hash: None,
                    },
                )
                .expect("render deployment");
                let args = deployment.spec.unwrap().template.spec.unwrap().containers[0]
                    .args
                    .clone()
                    .unwrap();
                let expected = [
                    "--wal-producer-request-timeout-ms",
                    expected[0],
                    "--wal-producer-retries",
                    expected[1],
                    "--wal-producer-retry-backoff-ms",
                    expected[2],
                    "--wal-producer-routing-retry-budget-ms",
                    expected[3],
                    "--wal-producer-init-retry-timeout-ms",
                    expected[4],
                    "--wal-producer-init-max-backoff-ms",
                    expected[5],
                    "--wal-producer-transaction-timeout-ms",
                    expected[6],
                ];
                assert!(
                    args.windows(expected.len())
                        .any(|window| window == expected),
                    "got: {args:?}"
                );
            }
        }
    }

    #[test]
    fn wal_producer_flush_timeout_is_exact_once_in_single_and_two_range_deployments() {
        let mut obj = tenant();
        obj.metadata.namespace = Some("ns".into());
        obj.metadata.uid = Some("uid".into());
        let ranges = [
            GresTenantRangeSpec {
                range_id: 0,
                end_key: Some(GresTenantRangeKey {
                    table_id: 10,
                    bucket: None,
                    rowid: 0,
                }),
            },
            GresTenantRangeSpec {
                range_id: 1,
                end_key: None,
            },
        ];
        let operator_config = ConfigArgs::parse_from(["operator"]).config;
        let compute_policy = crate::crd::gres::GresComputeSpec {
            wal_producer_flush_timeout_ms: Some(12_345),
            ..crate::crd::gres::GresComputeSpec::default()
        }
        .effective_policy()
        .expect("compute policy");

        for (range_control_enabled, active_ranges) in [(false, &ranges[..1]), (true, &ranges[..])] {
            for range in active_ranges {
                let wal_topic = format!("__gres_wal.tenant-a.r{}", range.range_id);
                let deployment = render_deployment(
                    &obj,
                    range,
                    &DeploymentRenderConfig {
                        all_ranges: active_ranges,
                        image: "image",
                        readiness_probe_period_seconds: 5,
                        bootstrap: "k:9092",
                        wal_topic: &wal_topic,
                        config_topic: "__gres_cfg.tenant-a",
                        policy: &crabka_gres_control::RegistryPolicy::default(),
                        compute_policy,
                        replicas: 1,
                        operator_config: &operator_config,
                        kafka_sasl: false,
                        range_control_enabled,
                        range_tls_hash: None,
                    },
                )
                .expect("render deployment");
                let args = deployment.spec.unwrap().template.spec.unwrap().containers[0]
                    .args
                    .clone()
                    .unwrap();
                let pair = ["--wal-producer-flush-timeout-ms", "12345"];
                assert!(
                    args.windows(2).filter(|window| *window == pair).count() == 1,
                    "expected {pair:?} exactly once, got: {args:?}"
                );
            }
        }
    }

    #[test]
    fn wal_producer_dns_timeout_is_exact_once_in_single_and_two_range_deployments() {
        let mut obj = tenant();
        obj.metadata.namespace = Some("ns".into());
        obj.metadata.uid = Some("uid".into());
        let ranges = [
            GresTenantRangeSpec {
                range_id: 0,
                end_key: Some(GresTenantRangeKey {
                    table_id: 10,
                    bucket: None,
                    rowid: 0,
                }),
            },
            GresTenantRangeSpec {
                range_id: 1,
                end_key: None,
            },
        ];
        let operator_config = ConfigArgs::parse_from(["operator"]).config;

        for (spec, pair) in [
            (
                crate::crd::gres::GresComputeSpec::default(),
                ["--wal-producer-dns-timeout-ms", "10000"],
            ),
            (
                crate::crd::gres::GresComputeSpec {
                    wal_producer_dns_timeout_ms: Some(37),
                    ..crate::crd::gres::GresComputeSpec::default()
                },
                ["--wal-producer-dns-timeout-ms", "37"],
            ),
        ] {
            let compute_policy = spec.effective_policy().expect("compute policy");
            for (range_control_enabled, active_ranges) in
                [(false, &ranges[..1]), (true, &ranges[..])]
            {
                for range in active_ranges {
                    let wal_topic = format!("__gres_wal.tenant-a.r{}", range.range_id);
                    let deployment = render_deployment(
                        &obj,
                        range,
                        &DeploymentRenderConfig {
                            all_ranges: active_ranges,
                            image: "image",
                            readiness_probe_period_seconds: 5,
                            bootstrap: "k:9092",
                            wal_topic: &wal_topic,
                            config_topic: "__gres_cfg.tenant-a",
                            policy: &crabka_gres_control::RegistryPolicy::default(),
                            compute_policy,
                            replicas: 1,
                            operator_config: &operator_config,
                            kafka_sasl: false,
                            range_control_enabled,
                            range_tls_hash: None,
                        },
                    )
                    .expect("render deployment");
                    let args = deployment.spec.unwrap().template.spec.unwrap().containers[0]
                        .args
                        .clone()
                        .unwrap();
                    assert!(
                        args.windows(2).filter(|window| *window == pair).count() == 1,
                        "got: {args:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn compute_wal_producer_throughput_args_are_exact_once_in_single_and_multi_range_modes() {
        let mut obj = tenant();
        obj.metadata.namespace = Some("ns".into());
        obj.metadata.uid = Some("uid".into());
        let ranges = [
            GresTenantRangeSpec {
                range_id: 0,
                end_key: Some(GresTenantRangeKey {
                    table_id: 10,
                    bucket: None,
                    rowid: 0,
                }),
            },
            GresTenantRangeSpec {
                range_id: 1,
                end_key: None,
            },
        ];
        let operator_config = ConfigArgs::parse_from(["operator"]).config;
        for (spec, expected) in [
            (
                crate::crd::gres::GresComputeSpec::default(),
                ["none", "0", "16384"],
            ),
            (
                crate::crd::gres::GresComputeSpec {
                    wal_producer_compression: Some(crate::crd::gres::WalProducerCompression::Zstd),
                    wal_producer_linger_ms: Some(18),
                    wal_producer_batch_bytes: Some(19),
                    ..crate::crd::gres::GresComputeSpec::default()
                },
                ["zstd", "18", "19"],
            ),
        ] {
            let compute_policy = spec.effective_policy().expect("compute policy");
            for (range_control_enabled, active_ranges) in
                [(false, &ranges[..1]), (true, &ranges[..])]
            {
                for range in active_ranges {
                    let wal_topic = format!("__gres_wal.tenant-a.r{}", range.range_id);
                    let deployment = render_deployment(
                        &obj,
                        range,
                        &DeploymentRenderConfig {
                            all_ranges: active_ranges,
                            image: "image",
                            readiness_probe_period_seconds: 5,
                            bootstrap: "k:9092",
                            wal_topic: &wal_topic,
                            config_topic: "__gres_cfg.tenant-a",
                            policy: &crabka_gres_control::RegistryPolicy::default(),
                            compute_policy,
                            replicas: 1,
                            operator_config: &operator_config,
                            kafka_sasl: false,
                            range_control_enabled,
                            range_tls_hash: None,
                        },
                    )
                    .expect("render deployment");
                    let args = deployment.spec.unwrap().template.spec.unwrap().containers[0]
                        .args
                        .clone()
                        .unwrap();
                    for pair in [
                        ["--wal-producer-compression", expected[0]],
                        ["--wal-producer-linger-ms", expected[1]],
                        ["--wal-producer-batch-bytes", expected[2]],
                    ] {
                        assert!(
                            args.windows(2).filter(|window| *window == pair).count() == 1,
                            "expected {pair:?} exactly once, got: {args:?}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn compute_wal_admin_args_are_exact_in_single_and_multi_range_modes() {
        let mut obj = tenant();
        obj.metadata.namespace = Some("ns".into());
        obj.metadata.uid = Some("uid".into());
        let ranges = [GresTenantRangeSpec {
            range_id: 0,
            end_key: None,
        }];
        let operator_config = ConfigArgs::parse_from(["operator"]).config;
        for (spec, expected) in [
            (
                crate::crd::gres::GresComputeSpec::default(),
                ["1", "30000", "5000", "30000"],
            ),
            (
                crate::crd::gres::GresComputeSpec {
                    wal_topic_replication_factor: Some(11),
                    wal_topic_ensure_timeout_ms: Some(22),
                    wal_admin_connect_timeout_ms: Some(33),
                    wal_admin_request_timeout_ms: Some(44),
                    ..crate::crd::gres::GresComputeSpec::default()
                },
                ["11", "22", "33", "44"],
            ),
        ] {
            let compute_policy = spec.effective_policy().expect("compute policy");
            for range_control_enabled in [false, true] {
                let deployment = render_deployment(
                    &obj,
                    &ranges[0],
                    &DeploymentRenderConfig {
                        all_ranges: &ranges,
                        image: "image",
                        readiness_probe_period_seconds: 5,
                        bootstrap: "k:9092",
                        wal_topic: "__gres_wal.tenant-a.r0",
                        config_topic: "__gres_cfg.tenant-a",
                        policy: &crabka_gres_control::RegistryPolicy::default(),
                        compute_policy,
                        replicas: 1,
                        operator_config: &operator_config,
                        kafka_sasl: false,
                        range_control_enabled,
                        range_tls_hash: None,
                    },
                )
                .expect("render deployment");
                let args = deployment.spec.unwrap().template.spec.unwrap().containers[0]
                    .args
                    .clone()
                    .unwrap();
                let expected = [
                    "--wal-topic-replication-factor",
                    expected[0],
                    "--wal-topic-ensure-timeout-ms",
                    expected[1],
                    "--wal-admin-connect-timeout-ms",
                    expected[2],
                    "--wal-admin-request-timeout-ms",
                    expected[3],
                ];
                assert!(
                    args.windows(expected.len())
                        .any(|window| window == expected),
                    "got: {args:?}"
                );
            }
        }
    }

    #[test]
    fn compute_workload_renders_custom_policy() {
        let mut obj = tenant();
        obj.metadata.namespace = Some("ns".into());
        obj.metadata.uid = Some("uid".into());
        let ranges = [
            GresTenantRangeSpec {
                range_id: 0,
                end_key: Some(GresTenantRangeKey {
                    table_id: 10,
                    bucket: None,
                    rowid: 0,
                }),
            },
            GresTenantRangeSpec {
                range_id: 1,
                end_key: None,
            },
        ];
        let mut operator_config = ConfigArgs::parse_from(["operator"]).config;
        operator_config.gres_checkpoint_store = Some(crate::config::GresCheckpointStoreKind::S3);
        operator_config.gres_checkpoint_bucket = Some("checkpoints".to_owned());
        let compute_policy = crate::crd::gres::GresComputeSpec {
            checkpoint_part_bytes: Some(8_388_608),
            checkpoint_retain: Some(4),
            checkpoint_delete_records_timeout_ms: Some(12_345),
            checkpoint_poll_interval_ms: Some(2_345),
            idle_suspend_poll_interval_ms: Some(3_456),
            range0_follower_poll_interval_ms: Some(5_678),
            lifecycle_requeue_ms: Some(4_567),
            ..crate::crd::gres::GresComputeSpec::default()
        }
        .effective_policy()
        .expect("compute policy");
        let policy = crabka_gres_control::RegistryPolicy::new(2, 15_001, 251, 501, 1_048_577)
            .expect("policy");
        let deployment = render_deployment(
            &obj,
            &ranges[0],
            &DeploymentRenderConfig {
                all_ranges: &ranges,
                image: "tenant-image",
                readiness_probe_period_seconds: 7,
                bootstrap: "k:9092",
                wal_topic: "__gres_wal.tenant-a.r0",
                config_topic: "__gres_cfg.tenant-a",
                policy: &policy,
                compute_policy,
                replicas: 1,
                operator_config: &operator_config,
                kafka_sasl: false,
                range_control_enabled: true,
                range_tls_hash: None,
            },
        )
        .expect("render deployment");
        let args = deployment
            .spec
            .as_ref()
            .expect("spec")
            .template
            .spec
            .as_ref()
            .expect("pod spec")
            .containers[0]
            .args
            .as_ref()
            .expect("args");

        for pair in [
            ["--registry-replication-factor", "2"],
            ["--registry-topic-create-timeout-ms", "15001"],
            ["--registry-reader-retry-backoff-ms", "251"],
            ["--registry-fetch-max-wait-ms", "501"],
            ["--registry-fetch-partition-max-bytes", "1048577"],
            ["--checkpoint-part-bytes", "8388608"],
            ["--checkpoint-retain", "4"],
            ["--checkpoint-delete-records-timeout-ms", "12345"],
            ["--checkpoint-poll-interval-ms", "2345"],
            ["--idle-suspend-poll-interval-ms", "3456"],
            ["--range0-follower-poll-interval-ms", "5678"],
        ] {
            assert!(
                args.windows(2).any(|window| window == pair),
                "missing {pair:?}: {args:?}"
            );
        }
        for absent in [
            "--checkpoint-frames",
            "--checkpoint-bytes",
            "--lifecycle-requeue-ms",
        ] {
            assert!(!args.iter().any(|arg| arg == absent), "got: {args:?}");
        }
        assert!(
            args.iter()
                .filter(|arg| {
                    [
                        "--checkpoint-part-bytes",
                        "--checkpoint-retain",
                        "--checkpoint-delete-records-timeout-ms",
                        "--checkpoint-poll-interval-ms",
                        "--idle-suspend-poll-interval-ms",
                    ]
                    .contains(&arg.as_str())
                })
                .count()
                == 5
        );
        assert!(
            lifecycle_requeue(&compute_policy) == Action::requeue(Duration::from_millis(4_567))
        );
        let readiness = deployment
            .spec
            .as_ref()
            .expect("deployment spec")
            .template
            .spec
            .as_ref()
            .expect("pod spec")
            .containers[0]
            .readiness_probe
            .as_ref()
            .expect("readiness probe");
        assert!(readiness.period_seconds == Some(7));
        assert!(
            deployment
                .spec
                .as_ref()
                .expect("deployment spec")
                .template
                .spec
                .as_ref()
                .expect("pod spec")
                .containers[0]
                .image
                .as_deref()
                == Some("tenant-image")
        );
    }

    #[tokio::test]
    async fn invalid_compute_policy_is_rejected_before_kafka_or_resource_io() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        use tower::service_fn;

        let requests = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&requests);
        let client = kube::Client::new(
            service_fn(move |_request| {
                let observed = Arc::clone(&observed);
                async move {
                    observed.fetch_add(1, Ordering::SeqCst);
                    let body = serde_json::to_vec(&json!({
                        "apiVersion": "crabka.io/v1alpha1",
                        "kind": "Gres",
                        "metadata": {"name": "fleet", "namespace": "default"},
                        "spec": {
                            "kafkaCluster": "demo",
                            "pgdog": {
                                "replicas": 1,
                                "listenPort": 6432,
                                "adminSecretRef": {"name": "admin", "key": "password"}
                            },
                            "compute": {"checkpointPartBytes": 7}
                        }
                    }))
                    .expect("serialize Gres");
                    Ok::<_, std::convert::Infallible>(
                        http::Response::builder()
                            .status(200)
                            .header(http::header::CONTENT_TYPE, "application/json")
                            .body(kube::client::Body::from(body))
                            .expect("response"),
                    )
                }
            }),
            "default",
        );
        let (registry, metrics) = crate::telemetry::new_registry_with_metrics();
        let ctx = Context::new(
            client,
            ConfigArgs::parse_from(["operator"]).config,
            Arc::new(tokio::sync::Mutex::new(registry)),
            metrics,
        );

        let Err(error) = Box::pin(prepare_tenant(&tenant(), &ctx)).await else {
            panic!("invalid compute policy must fail");
        };

        assert!(
            error
                .to_string()
                .contains("spec.compute.checkpointPartBytes"),
            "got: {error}"
        );
        assert!(requests.load(Ordering::SeqCst) == 1);
    }

    #[test]
    fn compute_image_precedence_is_tenant_then_operator_then_compiled_default() {
        let mut obj = tenant();
        let mut operator_config = ConfigArgs::parse_from(["operator"]).config;
        operator_config.default_gres_image = Some("operator-image".into());

        obj.spec.image = Some("tenant-image".into());
        assert!(
            effective_compute_image(&obj, &operator_config).expect("tenant image")
                == "tenant-image"
        );
        obj.spec.image = None;
        assert!(
            effective_compute_image(&obj, &operator_config).expect("operator image")
                == "operator-image"
        );
        operator_config.default_gres_image = None;
        assert!(
            effective_compute_image(&obj, &operator_config).expect("compiled image")
                == DEFAULT_IMAGE
        );
    }

    #[test]
    fn empty_effective_compute_image_is_rejected_without_fallback() {
        let mut obj = tenant();
        let operator_config = ConfigArgs::parse_from(["operator"]).config;
        obj.spec.image = Some(String::new());

        let error =
            effective_compute_image(&obj, &operator_config).expect_err("empty image must fail");
        assert!(error.to_string().contains("spec.image"), "got: {error}");
    }

    #[test]
    fn active_first_split_enables_control_listener_without_changing_hosted_ranges() {
        let mut obj = tenant();
        obj.metadata.namespace = Some("ns".into());
        obj.metadata.uid = Some("uid".into());
        let ranges = [GresTenantRangeSpec {
            range_id: 0,
            end_key: None,
        }];
        let deployment = render_test_deployment(
            &obj,
            &ranges[0],
            &ranges,
            false,
            true,
            Some("range-tls-hash"),
        );
        let container = &deployment
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap()
            .containers[0];
        let args = container.args.as_ref().unwrap();
        assert!(args.windows(2).any(|pair| pair == ["--host-ranges", "r0"]));
        assert!(args.iter().any(|arg| arg == "--range-listen"));
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--operator-control-principal", "CN=tenant-a-operator"])
        );
        assert!(
            container
                .volume_mounts
                .as_ref()
                .unwrap()
                .iter()
                .any(|mount| mount.name == "range-tls")
        );
    }

    #[test]
    fn each_multi_range_deployment_hosts_only_its_own_range() {
        let mut obj = tenant();
        obj.metadata.namespace = Some("ns".into());
        obj.metadata.uid = Some("uid".into());
        let ranges = [
            GresTenantRangeSpec {
                range_id: 0,
                end_key: Some(GresTenantRangeKey {
                    table_id: 10,
                    bucket: None,
                    rowid: 0,
                }),
            },
            GresTenantRangeSpec {
                range_id: 1,
                end_key: None,
            },
        ];
        let deployment = render_test_deployment(
            &obj,
            &ranges[1],
            &ranges,
            false,
            true,
            Some("range-tls-hash"),
        );
        let args = deployment
            .spec
            .expect("spec")
            .template
            .spec
            .expect("pod")
            .containers[0]
            .args
            .clone()
            .expect("args");
        assert!(args.windows(2).any(|pair| pair == ["--host-ranges", "r1"]));
        assert!(!args.windows(2).any(|pair| pair == ["--host-ranges", "r0"]));
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--range-listen", "0.0.0.0:7432"])
        );
        assert!(
            args.windows(2)
                .any(|pair| { pair == ["--range-allowed-principal", "CN=tenant-a-range"] })
        );
    }

    #[test]
    fn hash_range_key_serde_and_registry_conversion_preserve_bucket_zero() {
        let boundary = RangeBoundary::hash(7, 0, 11);
        let key = range_key_from_boundary(boundary);
        assert_eq!(
            key,
            GresTenantRangeKey {
                table_id: 7,
                bucket: Some(0),
                rowid: 11
            }
        );
        assert_eq!(boundary_from_range_key(key), boundary);
        assert_eq!(
            serde_json::to_value(key).unwrap(),
            serde_json::json!({"tableId": 7, "bucket": 0, "rowid": 11})
        );
        assert_eq!(
            serde_json::to_value(GresTenantRangeKey {
                table_id: 7,
                bucket: None,
                rowid: 11
            })
            .unwrap(),
            serde_json::json!({"tableId": 7, "rowid": 11})
        );
    }

    #[test]
    fn range_services_expose_their_stable_listener_sets() {
        let mut obj = tenant();
        obj.metadata.namespace = Some("ns".into());
        obj.metadata.uid = Some("uid".into());

        let cases = [
            (
                0,
                "tenant-a-gres",
                vec![
                    json!({ "name": "postgres", "port": COMPUTE_PORT, "targetPort": COMPUTE_PORT, "protocol": "TCP" }),
                    json!({ "name": "range", "port": RANGE_PORT, "targetPort": RANGE_PORT, "protocol": "TCP" }),
                ],
            ),
            (
                1,
                "tenant-a-gres-r1",
                vec![
                    json!({ "name": "range", "port": RANGE_PORT, "targetPort": RANGE_PORT, "protocol": "TCP" }),
                ],
            ),
        ];

        for (range_id, name, ports) in cases {
            let expected = serde_json::from_value(json!({
                "metadata": {
                    "name": name,
                    "namespace": "ns",
                    "labels": meta_labels(&obj),
                    "ownerReferences": [owner_ref::<GresTenant>(&obj).expect("owner reference")],
                },
                "spec": {
                    "type": "ClusterIP",
                    "selector": range_labels(&obj, range_id),
                    "ports": ports,
                },
            }))
            .expect("expected range Service");

            assert_eq!(
                render_range_service(&obj, range_id).expect("render range Service"),
                expected
            );
        }
    }

    #[test]
    fn range_tls_secret_is_preserved_and_rotates_on_identity_drift() {
        let mut obj = tenant();
        obj.metadata.namespace = Some("ns".into());
        obj.metadata.uid = Some("uid".into());
        let (first, first_hash) = render_range_tls_secret(&obj, None).expect("issue TLS secret");
        let data = first.data.as_ref().expect("TLS data");
        for key in ["ca.crt", "ca.key", "tls.crt", "tls.key"] {
            assert!(data.contains_key(key), "missing {key}");
        }
        let (preserved, preserved_hash) =
            render_range_tls_secret(&obj, Some(&first)).expect("preserve valid identity");
        assert_eq!(first_hash, preserved_hash);
        assert_eq!(first.data, preserved.data);

        let mut drifted = first.clone();
        drifted
            .metadata
            .annotations
            .as_mut()
            .expect("annotations")
            .insert(
                RANGE_TLS_IDENTITY_ANNOTATION.into(),
                "wrong identity".into(),
            );
        let (_rotated, rotated_hash) =
            render_range_tls_secret(&obj, Some(&drifted)).expect("rotate drifted identity");
        assert_ne!(first_hash, rotated_hash);
    }

    #[test]
    fn operator_control_identity_is_distinct_preserved_and_ca_fenced() {
        let mut obj = tenant();
        obj.metadata.namespace = Some("ns".into());
        obj.metadata.uid = Some("uid".into());
        let (range, _) = render_range_tls_secret(&obj, None).unwrap();
        let first = render_operator_control_tls_secret(&obj, &range, None).unwrap();
        assert!(operator_control_tls_is_current(
            &first,
            range.metadata.annotations.as_ref().unwrap()[RANGE_TLS_HASH_ANNOTATION].as_str(),
            "CN=tenant-a-operator"
        ));
        assert!(!operator_control_tls_is_current(
            &first,
            range.metadata.annotations.as_ref().unwrap()[RANGE_TLS_HASH_ANNOTATION].as_str(),
            "CN=tenant-a-range"
        ));
        let preserved = render_operator_control_tls_secret(&obj, &range, Some(&first)).unwrap();
        assert_eq!(first.data, preserved.data);

        let (rotated_range, _) = render_range_tls_secret(&obj, None).unwrap();
        let rotated =
            render_operator_control_tls_secret(&obj, &rotated_range, Some(&first)).unwrap();
        assert_ne!(first.data, rotated.data);
    }

    #[test]
    fn deployment_readiness_requires_observed_generation_and_available_replicas() {
        let mut deployment = Deployment::default();
        deployment.metadata.generation = Some(4);
        deployment.spec = Some(serde_json::from_value(json!({
            "selector": { "matchLabels": { "app": "gres" } },
            "template": { "metadata": { "labels": { "app": "gres" } }, "spec": { "containers": [{ "name": "gres", "image": "gres" }] } },
            "replicas": 1
        })).expect("deployment spec"));
        deployment.status = Some(
            serde_json::from_value(json!({
                "observedGeneration": 3,
                "availableReplicas": 1
            }))
            .expect("deployment status"),
        );
        assert!(!deployment_is_ready(&deployment));

        deployment
            .status
            .as_mut()
            .expect("status")
            .observed_generation = Some(4);
        assert!(deployment_is_ready(&deployment));
        deployment
            .status
            .as_mut()
            .expect("status")
            .available_replicas = Some(0);
        assert!(!deployment_is_ready(&deployment));
    }

    #[test]
    fn layout_shrink_identifies_withdrawn_ranges_and_multi_range_front_door() {
        let previous = [0, 1, 2];
        let desired = [0];

        let obsolete = obsolete_range_resources("tenant-a", &previous, &desired);

        assert_eq!(
            obsolete,
            ObsoleteRangeResources {
                deployments: vec!["tenant-a-gres-r1".into(), "tenant-a-gres-r2".into()],
                services: vec![
                    "tenant-a-gres-r1".into(),
                    "tenant-a-gres-r2".into(),
                    "tenant-a-gres-pg".into(),
                ],
            }
        );
    }

    #[test]
    fn obsolete_cleanup_requires_matching_controller_owner_and_managed_labels() {
        let mut tenant = tenant();
        tenant.metadata.uid = Some("tenant-uid".into());
        let mut service = render_range_service(&tenant, 1).expect("managed service");

        assert!(is_managed_by_tenant(&service.metadata, &tenant));

        service
            .metadata
            .labels
            .as_mut()
            .unwrap()
            .insert("app.kubernetes.io/instance".into(), "another-tenant".into());
        assert!(!is_managed_by_tenant(&service.metadata, &tenant));
    }

    #[test]
    fn s3_checkpoint_runtime_args_match_operator_manifest_verifier() {
        let mut config = ConfigArgs::parse_from(["operator"]).config;
        config.gres_checkpoint_store = Some(crate::config::GresCheckpointStoreKind::S3);
        config.gres_checkpoint_bucket = Some("gres-checkpoints".into());
        config.gres_checkpoint_region = Some("us-east-1".into());
        config.gres_checkpoint_endpoint = Some("http://minio.default.svc:9000".into());
        config.gres_checkpoint_allow_http = true;
        config.gres_checkpoint_access_key_id = Some("minio".into());
        config.gres_checkpoint_secret_access_key = Some("secret".into());

        let args = checkpoint_runtime_args(&config).expect("checkpoint runtime args");
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--checkpoint-store", "s3"])
        );
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--checkpoint-bucket", "gres-checkpoints"])
        );
        assert!(args.contains(&"--checkpoint-allow-http".to_string()));
        assert!(!args.iter().any(|arg| arg == "--checkpoint-frames"));
        assert!(!args.iter().any(|arg| arg == "secret"));
    }
}
