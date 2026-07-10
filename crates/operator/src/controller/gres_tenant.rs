//! `GresTenant` reconciler.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use crabka_client_admin::{
    AclEntry, AclEntryFilter, AclOperation, CreateTopicSpec, DEFAULT_SCRAM_ITERATIONS, PatternType,
    PermissionType, ResourceType, ScramDeletion, ScramUpsertion,
};
use crabka_gres_control::{
    RangeBoundary, RangeLayoutEntry, SqlUser, TENANT_REGISTRY_TOPIC, TenantId, TenantName,
    TenantRecord, TenantState, tenant_config_topic,
};
use crabka_security::scram::PgScramVerifier;
use futures::StreamExt as _;
use k8s_openapi::{
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
    controller::{
        common::{self, FIELD_MANAGER, ReconcileError, apply_object, condition, owner_ref},
        topic::internal_listener_bootstrap,
        user::{diff_acls, entry_to_exact_filter},
    },
    crd::{
        Gres, GresTenant, GresTenantRangeKey, GresTenantRangeSpec, Kafka, SecretKeyRef,
        TenantDefaults,
    },
};

const FINALIZER: &str = "crabka.io/gres-tenant-finalizer";
const APP_NAME: &str = "crabka-gres";
const DEFAULT_IMAGE: &str = concat!("ghcr.io/robot-head/crabka-gres:", env!("CARGO_PKG_VERSION"));
const COMPUTE_PORT: i32 = 5432;
const LIFECYCLE_REQUEUE: Duration = Duration::from_secs(5);
const UNSUPPORTED_TOPOLOGY_REQUEUE: Duration = Duration::from_mins(5);
const MULTI_RANGE_UNSUPPORTED_REASON: &str = "MultiRangeUnsupported";
const MULTI_RANGE_UNSUPPORTED_MESSAGE: &str = "multi-range tenant placement is unavailable: it would start multiple computes that each host r0 and write the r0 WAL; wait for remote range-0 replication and a fencing barrier";

/// Run the controller forever.
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

pub fn error_policy(_obj: Arc<GresTenant>, err: &ReconcileError, _ctx: Arc<Context>) -> Action {
    tracing::warn!(error = %err, "gres tenant reconcile error, requeueing");
    Action::requeue(Duration::from_secs(15))
}

#[tracing::instrument(level = "info", skip_all, fields(kind = "GresTenant", name = %obj.name_any()))]
pub async fn reconcile(obj: Arc<GresTenant>, ctx: Arc<Context>) -> Result<Action, ReconcileError> {
    common::record_reconcile(
        &ctx,
        "GresTenant",
        Box::pin(reconcile_inner(obj, ctx.clone())),
    )
    .await
}

#[allow(clippy::too_many_lines)]
async fn reconcile_inner(
    obj: Arc<GresTenant>,
    ctx: Arc<Context>,
) -> Result<Action, ReconcileError> {
    let ns = obj.namespace().unwrap_or_else(|| "default".into());
    let name = obj.name_any();
    let tenant_api: Api<GresTenant> = Api::namespaced(ctx.client.clone(), &ns);

    let tenant_name = match TenantName::try_from(name.as_str()) {
        Ok(name) => name,
        Err(err) => {
            patch_status(
                &tenant_api,
                &name,
                &obj,
                "False",
                "InvalidName",
                &err.to_string(),
                None,
                preserved_lifecycle_state(&obj),
                false,
            )
            .await?;
            return Ok(Action::requeue(Duration::from_mins(5)));
        }
    };

    if has_multiple_ranges(&obj.spec.ranges) {
        patch_unsupported_multi_range_status(&tenant_api, &name, &obj, None).await?;
        return Ok(Action::requeue(UNSUPPORTED_TOPOLOGY_REQUEUE));
    }

    let has_sticky_unsupported_status = is_multi_range_unsupported(&obj);

    let gres_api: Api<Gres> = Api::namespaced(ctx.client.clone(), &ns);
    let Some(gres) = gres_api.get_opt(&obj.spec.gres).await? else {
        if !has_sticky_unsupported_status {
            patch_status(
                &tenant_api,
                &name,
                &obj,
                "False",
                "GresNotFound",
                "referenced Gres does not exist",
                None,
                preserved_lifecycle_state(&obj),
                false,
            )
            .await?;
        }
        return Ok(Action::requeue(Duration::from_secs(30)));
    };
    let kafka_api: Api<Kafka> = Api::namespaced(ctx.client.clone(), &ns);
    let cluster = gres.spec.kafka_cluster.clone();
    let kafka = kafka_api.get_opt(&cluster).await?;
    let Some(bootstrap) = kafka.as_ref().and_then(internal_listener_bootstrap) else {
        if !has_sticky_unsupported_status {
            patch_status(
                &tenant_api,
                &name,
                &obj,
                "False",
                "ClusterNotReady",
                "referenced Kafka is not Ready or has no internal listener",
                None,
                preserved_lifecycle_state(&obj),
                false,
            )
            .await?;
        }
        return Ok(Action::requeue(Duration::from_secs(30)));
    };

    if obj.meta().deletion_timestamp.is_some() {
        cleanup_tenant(&ctx, &cluster, &bootstrap, &tenant_name, &name).await;
        remove_finalizer(&tenant_api, &name).await?;
        return Ok(Action::await_change());
    }

    if !has_finalizer(&obj) {
        add_finalizer(&tenant_api, &name).await?;
        return Ok(Action::requeue(Duration::ZERO));
    }

    let defaults = effective_defaults(gres.spec.defaults.as_ref(), obj.spec.overrides.as_ref());
    let wal_topic = wal_topic(&tenant_name);
    let spec_ranges = effective_ranges(&obj.spec.ranges)?;
    let cfg_topic = tenant_config_topic(&tenant_name);
    let control = ctx.gres_control_for(&cluster, &bootstrap).await?;
    let current_record = control.get_tenant(&tenant_name).await?;
    let tenant_ranges = reconcile_ranges(current_record.as_ref(), &spec_ranges);
    if has_multiple_ranges(&tenant_ranges) {
        patch_unsupported_multi_range_status(&tenant_api, &name, &obj, current_record.as_ref())
            .await?;
        return Ok(Action::requeue(UNSUPPORTED_TOPOLOGY_REQUEUE));
    }
    let lifecycle_state = current_record
        .as_ref()
        .map_or_else(|| requested_state(&obj), |record| record.state);
    let registry_version = current_record.as_ref().map(|record| record.record_version);
    let reconcile_result: Result<Action, ReconcileError> = async {
        let admin_handle = ctx.admin_client_for(&cluster, &bootstrap).await?;
        let mut admin = admin_handle.lock().await;
        if lifecycle_state == TenantState::ResumeRequested
            && wal_topics_remain(&mut admin, &tenant_name, &tenant_ranges).await?
        {
            drop(admin);
            reconcile_compute_deployments(
                &ctx,
                &ns,
                &obj,
                &tenant_ranges,
                &bootstrap,
                &wal_topic,
                &cfg_topic,
                TenantState::Parking,
            )
            .await?;
            return Ok(Action::requeue(LIFECYCLE_REQUEUE));
        }
        let mut topic_specs = vec![
            CreateTopicSpec {
                name: cfg_topic.clone(),
                partitions: 1,
                replicas: defaults.wal_replication,
                configs: BTreeMap::from([("cleanup.policy".to_string(), "compact".to_string())]),
            },
            CreateTopicSpec {
                name: TENANT_REGISTRY_TOPIC.to_string(),
                partitions: 1,
                replicas: defaults.wal_replication,
                configs: BTreeMap::from([("cleanup.policy".to_string(), "compact".to_string())]),
            },
        ];
        if matches!(
            lifecycle_state,
            TenantState::Active | TenantState::ResumeRequested
        ) {
            topic_specs.extend(tenant_ranges.iter().map(|range| CreateTopicSpec {
                name: wal_topic_for_range(&tenant_name, range.range_id),
                partitions: 1,
                replicas: defaults.wal_replication,
                configs: BTreeMap::new(),
            }));
        }
        let missing = missing_topics(&mut admin, &topic_specs).await?;
        if !missing.is_empty() {
            admin.create_topics(&missing, 30_000).await?;
        }

        let password = read_password_secret(&ctx, &ns, &obj.spec.password_secret_ref).await?;
        let kafka_username = tenant_kafka_username(&tenant_name);
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
        let desired_acls: std::collections::BTreeSet<_> =
            tenant_acls(&principal, &tenant_name).into_iter().collect();
        let current_acls: std::collections::BTreeSet<_> = admin
            .describe_acls(&AclEntryFilter {
                principal: Some(principal.clone()),
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
        if parking_progress == ParkingProgress::DeletionPending {
            drop(admin);
            reconcile_compute_deployments(
                &ctx,
                &ns,
                &obj,
                &tenant_ranges,
                &bootstrap,
                &wal_topic,
                &cfg_topic,
                record.state,
            )
            .await?;
            return Ok(Action::requeue(LIFECYCLE_REQUEUE));
        }
        if matches!(
            lifecycle_state,
            TenantState::Parking | TenantState::Suspended
        ) {
            record.record_version = record_version;
            record.ensure_valid()?;
        }
        drop(admin);

        if parking_progress == ParkingProgress::Complete {
            // The fenced parking intent was committed before deleting any WAL.
            // Do not issue a second replacement against the stale pre-park view.
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

        let svc_api: Api<Service> = Api::namespaced(ctx.client.clone(), &ns);
        let policy_api: Api<NetworkPolicy> = Api::namespaced(ctx.client.clone(), &ns);
        apply_object(&svc_api, &service_name(&name), &render_service(&obj)?).await?;
        reconcile_compute_deployments(
            &ctx,
            &ns,
            &obj,
            &tenant_ranges,
            &bootstrap,
            &wal_topic,
            &cfg_topic,
            record.state,
        )
        .await?;
        apply_object(
            &policy_api,
            &network_policy_name(&name),
            &render_range_compute_network_policy(&obj)?,
        )
        .await?;

        patch_status(
            &tenant_api,
            &name,
            &obj,
            "True",
            "Ready",
            "tenant in sync",
            Some(record.record_version),
            record.state,
            true,
        )
        .await?;
        Ok(Action::requeue(LIFECYCLE_REQUEUE))
    }
    .await;

    match reconcile_result {
        Ok(action) => Ok(action),
        Err(error) => {
            patch_status(
                &tenant_api,
                &name,
                &obj,
                "False",
                "ReconcileFailed",
                &error.to_string(),
                registry_version,
                lifecycle_state,
                false,
            )
            .await?;
            Err(error)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParkingProgress {
    NotNeeded,
    DeletionPending,
    Complete,
}

#[allow(clippy::too_many_arguments)]
async fn reconcile_compute_deployments(
    ctx: &Context,
    namespace: &str,
    obj: &GresTenant,
    ranges: &[GresTenantRangeSpec],
    bootstrap: &str,
    wal_topic: &str,
    config_topic: &str,
    lifecycle_state: TenantState,
) -> Result<(), ReconcileError> {
    let dep_api: Api<Deployment> = Api::namespaced(ctx.client.clone(), namespace);
    let image = ctx
        .config
        .default_gres_image
        .clone()
        .unwrap_or_else(|| DEFAULT_IMAGE.to_string());
    let tenant_name = obj.name_any();
    for range in ranges {
        let deployment = render_deployment(
            obj,
            range,
            ranges,
            &image,
            bootstrap,
            wal_topic,
            config_topic,
            compute_replicas(lifecycle_state),
        )?;
        apply_object(
            &dep_api,
            &deployment_name(&tenant_name, range.range_id),
            &deployment,
        )
        .await?;
    }
    Ok(())
}

async fn park_suspended_tenant_wal(
    control: &crate::context::GresControlHandle,
    admin: &mut tokio::sync::MutexGuard<'_, dyn crabka_client_admin::AdminClientLike + Send>,
    ranges: &[GresTenantRangeSpec],
    tenant: &TenantName,
    record: &mut TenantRecord,
    expected_record_version: Option<u64>,
) -> Result<ParkingProgress, ReconcileError> {
    let mut ranges_to_park = Vec::with_capacity(ranges.len());
    for range in ranges {
        let topic = wal_topic_for_range(tenant, range.range_id);
        if topic_exists(admin, &topic).await? {
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
        delete_wal_topics(admin, tenant, &ranges_to_park).await?;
    }
    if wal_topics_remain(admin, tenant, ranges).await? {
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

async fn wal_topics_remain(
    admin: &mut tokio::sync::MutexGuard<'_, dyn crabka_client_admin::AdminClientLike + Send>,
    tenant: &TenantName,
    ranges: &[GresTenantRangeSpec],
) -> Result<bool, ReconcileError> {
    for range in ranges {
        if topic_exists(admin, &wal_topic_for_range(tenant, range.range_id)).await? {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn delete_wal_topics(
    admin: &mut tokio::sync::MutexGuard<'_, dyn crabka_client_admin::AdminClientLike + Send>,
    tenant: &TenantName,
    ranges: &[u32],
) -> Result<(), ReconcileError> {
    for range_id in ranges {
        let topic = wal_topic_for_range(tenant, *range_id);
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

async fn topic_exists(
    admin: &mut tokio::sync::MutexGuard<'_, dyn crabka_client_admin::AdminClientLike + Send>,
    topic: &str,
) -> Result<bool, ReconcileError> {
    let metadata = admin.metadata(&[topic]).await?;
    Ok(metadata
        .topics
        .iter()
        .find(|entry| entry.name == topic)
        .is_some_and(|entry| entry.error.is_none()))
}

async fn cleanup_tenant(
    ctx: &Context,
    cluster: &str,
    bootstrap: &str,
    tenant: &TenantName,
    _tenant_name: &str,
) {
    if let Ok(control) = ctx.gres_control_for(cluster, bootstrap).await
        && let Err(err) = control.delete_tenant(tenant).await
    {
        tracing::warn!(error = %err, tenant = %tenant, "gres tenant tombstone write failed");
    }
    let Ok(admin_handle) = ctx.admin_client_for(cluster, bootstrap).await else {
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
            .or_else(|| base.and_then(|d| d.checkpoint_frames)),
        checkpoint_bytes: override_
            .and_then(|d| d.checkpoint_bytes)
            .or_else(|| base.and_then(|d| d.checkpoint_bytes)),
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
    record.idle_seconds = defaults.idle_seconds;
    if let Some(current) = current {
        record.wal_generation = current.wal_generation;
        record.endpoint.clone_from(&current.endpoint);
        record.bucket_prefix.clone_from(&current.bucket_prefix);
        record.hash_placements.clone_from(&current.hash_placements);
        record.suspend_max_checkpoint_bytes = current.suspend_max_checkpoint_bytes;
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
        rowid: boundary.rowid,
    }
}

fn boundary_from_range_key(key: GresTenantRangeKey) -> RangeBoundary {
    RangeBoundary::new(key.table_id, key.rowid)
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

fn has_multiple_ranges(ranges: &[GresTenantRangeSpec]) -> bool {
    ranges.len() > 1
}

fn is_multi_range_unsupported(obj: &GresTenant) -> bool {
    obj.status.as_ref().is_some_and(|status| {
        status.conditions.iter().any(|condition| {
            condition.type_ == "Ready"
                && condition.status == "False"
                && condition.reason == MULTI_RANGE_UNSUPPORTED_REASON
        })
    })
}

async fn patch_unsupported_multi_range_status(
    api: &Api<GresTenant>,
    name: &str,
    obj: &GresTenant,
    current_record: Option<&TenantRecord>,
) -> Result<(), ReconcileError> {
    patch_status(
        api,
        name,
        obj,
        "False",
        MULTI_RANGE_UNSUPPORTED_REASON,
        MULTI_RANGE_UNSUPPORTED_MESSAGE,
        current_record.map(|record| record.record_version),
        current_record.map_or_else(|| preserved_lifecycle_state(obj), |record| record.state),
        false,
    )
    .await
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

fn service_name(name: &str) -> String {
    format!("{name}-gres")
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
                "{}.{}.svc.cluster.local:{COMPUTE_PORT}",
                deployment_name(&obj.name_any(), range.range_id),
                obj.namespace().unwrap_or_else(|| "default".into())
            ),
            wal_generation: 0,
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
        "metadata": { "name": service_name(&name), "namespace": obj.namespace(), "labels": meta_labels(obj), "ownerReferences": [owner_ref::<GresTenant>(obj)?] },
        "spec": { "type": "ClusterIP", "selector": selector_labels(obj), "ports": [{ "name": "postgres", "port": COMPUTE_PORT, "targetPort": COMPUTE_PORT, "protocol": "TCP" }] }
    }))?)
}

#[allow(clippy::too_many_arguments)]
fn render_deployment(
    obj: &GresTenant,
    range: &GresTenantRangeSpec,
    all_ranges: &[GresTenantRangeSpec],
    image: &str,
    bootstrap: &str,
    wal_topic: &str,
    cfg_topic: &str,
    replicas: i32,
) -> Result<Deployment, ReconcileError> {
    let name = obj.name_any();
    let selector = range_labels(obj, range.range_id);
    let host_ranges = host_ranges_arg();
    let ranges = ranges_arg(all_ranges);
    Ok(serde_json::from_value(json!({
        "metadata": { "name": deployment_name(&name, range.range_id), "namespace": obj.namespace(), "labels": meta_labels(obj), "ownerReferences": [owner_ref::<GresTenant>(obj)?] },
        "spec": {
            "replicas": replicas,
            "selector": { "matchLabels": selector },
            "template": {
                "metadata": { "labels": selector },
                "spec": {
                    "securityContext": { "runAsNonRoot": true, "runAsUser": 65532, "fsGroup": 65532 },
                    "containers": [{
                        "name": "gres",
                        "image": image,
                        "args": [
                            "--listen", format!("0.0.0.0:{COMPUTE_PORT}"),
                            "--substrate-bootstrap", bootstrap,
                            "--tenant", name,
                            "--ranges", ranges,
                            "--host-ranges", host_ranges
                        ],
                        "env": [
                            { "name": "KAFKA_BOOTSTRAP_SERVERS", "value": bootstrap },
                            { "name": "GRES_TENANT", "value": name },
                            { "name": "GRES_WAL_TOPIC", "value": wal_topic },
                            { "name": "GRES_CONFIG_TOPIC", "value": cfg_topic },
                            { "name": "GRES_RANGES", "value": ranges },
                            { "name": "GRES_HOST_RANGES", "value": host_ranges },
                            { "name": "GRES_KAFKA_USERNAME", "value": format!("gres-{name}") },
                            { "name": "GRES_KAFKA_PASSWORD", "valueFrom": { "secretKeyRef": { "name": obj.spec.password_secret_ref.name, "key": obj.spec.password_secret_ref.key } } }
                        ],
                        "ports": [{ "name": "postgres", "containerPort": COMPUTE_PORT, "protocol": "TCP" }],
                        "readinessProbe": { "tcpSocket": { "port": COMPUTE_PORT }, "periodSeconds": 5 },
                        "resources": obj.spec.resources.clone().unwrap_or_default()
                    }]
                }
            }
        }
    }))?)
}

fn ranges_arg(ranges: &[GresTenantRangeSpec]) -> String {
    let mut starts = vec![GresTenantRangeKey {
        table_id: 0,
        rowid: 0,
    }];
    starts.extend(ranges.iter().filter_map(|range| range.end_key));
    starts
        .iter()
        .map(|key| format!("{}:{}", key.table_id, key.rowid))
        .collect::<Vec<_>>()
        .join(",")
}

fn host_ranges_arg() -> String {
    "r0".to_string()
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
            ingress: Some(vec![NetworkPolicyIngressRule {
                from: Some(vec![same_tenant_peer]),
                ports: Some(vec![NetworkPolicyPort {
                    protocol: Some("TCP".into()),
                    port: Some(IntOrString::Int(COMPUTE_PORT)),
                    end_port: None,
                }]),
            }]),
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

#[allow(clippy::too_many_arguments)]
async fn patch_status(
    api: &Api<GresTenant>,
    name: &str,
    obj: &GresTenant,
    status: &str,
    reason: &str,
    message: &str,
    registry_version: Option<u64>,
    lifecycle_phase: TenantState,
    advance_generation: bool,
) -> Result<(), ReconcileError> {
    let observed_generation = if advance_generation {
        obj.meta().generation
    } else {
        obj.status.as_ref().and_then(|s| s.observed_generation)
    };
    let tenant = TenantName::try_from(name).ok();
    let body = json!({
        "status": {
            "conditions": [condition("Ready", status, reason, message)],
            "observedGeneration": observed_generation,
            "ready": status == "True",
            "walTopic": tenant.as_ref().map(wal_topic),
            "registryVersion": registry_version.or_else(|| obj.status.as_ref().and_then(|s| s.registry_version)),
            "lifecyclePhase": lifecycle_phase.to_string(),
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

    use super::*;
    use crate::crd::{GresTenantSpec, SecretKeyRef};

    fn tenant() -> GresTenant {
        GresTenant::new(
            "tenant-a",
            GresTenantSpec {
                gres: "fleet".into(),
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
        assert!(
            !acls
                .iter()
                .any(|acl| acl.resource_name == TENANT_REGISTRY_TOPIC
                    && acl.operation == AclOperation::Read)
        );
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
    fn tenant_record_hashes_password_without_plaintext() {
        let obj = tenant();
        let defaults = EffectiveDefaults {
            wal_replication: 1,
            checkpoint_frames: None,
            checkpoint_bytes: None,
            idle_seconds: None,
        };
        let record = build_tenant_record(
            &obj,
            &TenantName::try_from("tenant-a").unwrap(),
            "hunter2",
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
        assert!(!record.scram_verifier.contains("hunter2"));
    }

    #[test]
    fn deployment_references_password_secret_without_plaintext() {
        let mut obj = tenant();
        obj.metadata.namespace = Some("ns".into());
        obj.metadata.uid = Some("uid".into());
        let deployment = render_deployment(
            &obj,
            &GresTenantRangeSpec {
                range_id: 0,
                end_key: None,
            },
            &[GresTenantRangeSpec {
                range_id: 0,
                end_key: None,
            }],
            "image",
            "k:9092",
            "__gres_wal.tenant-a.r0",
            "__gres_cfg.tenant-a",
            1,
        )
        .unwrap();
        let json = serde_json::to_string(&deployment).unwrap();
        assert!(json.contains("secretKeyRef"));
        assert!(!json.contains("hunter2"));
    }
}
