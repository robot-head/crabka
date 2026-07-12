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
use crabka_security::ca::{SubjectAltName, generate_cluster_ca, issue_broker_cert};
use crabka_security::scram::PgScramVerifier;
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

use crate::controller::gres_split_operation::{
    MtlsRangeMutationClient, active_operations, reconcile_activated_cutover,
    reconcile_one_rpc_phase, successors_may_be_deployed, verify_target_topology_ready,
};
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
const RANGE_PORT: i32 = 7432;
const RANGE_TLS_DIR: &str = "/etc/crabka/range-tls";
const RANGE_TLS_IDENTITY_ANNOTATION: &str = "crabka.io/range-tls-identity";
const RANGE_TLS_HASH_ANNOTATION: &str = "crabka.io/range-tls-hash";
const LIFECYCLE_REQUEUE: Duration = Duration::from_secs(5);

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

    let gres_api: Api<Gres> = Api::namespaced(ctx.client.clone(), &ns);
    let Some(gres) = gres_api.get_opt(&obj.spec.gres).await? else {
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
        return Ok(Action::requeue(Duration::from_secs(30)));
    };
    let kafka_api: Api<Kafka> = Api::namespaced(ctx.client.clone(), &ns);
    let cluster = gres.spec.kafka_cluster.clone();
    let kafka = kafka_api.get_opt(&cluster).await?;
    let Some(bootstrap) = kafka.as_ref().and_then(internal_listener_bootstrap) else {
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
    let split_operations = active_operations(control.list_split_operations(&tenant_name).await?);
    let active_split = split_operations.first().cloned();
    let tenant_ranges = active_split
        .as_ref()
        .filter(|operation| successors_may_be_deployed(operation))
        .and_then(|operation| operation.plan.as_ref())
        .map_or_else(
            || reconcile_ranges(current_record.as_ref(), &spec_ranges),
            |plan| {
                plan.target_layout
                    .iter()
                    .map(|range| GresTenantRangeSpec {
                        range_id: range.range_id,
                        end_key: range.end_key.map(range_key_from_boundary),
                    })
                    .collect()
            },
        );
    let lifecycle_state = current_record
        .as_ref()
        .map_or_else(|| requested_state(&obj), |record| record.state);
    let registry_version = current_record.as_ref().map(|record| record.record_version);
    let kafka_sasl = kafka
        .as_ref()
        .is_some_and(kafka_internal_listener_requires_sasl);
    let range_control_enabled = tenant_ranges.len() > 1 || active_split.is_some();
    let range_tls_hash = if range_control_enabled {
        Some(reconcile_range_tls_secret(&ctx, &ns, &obj).await?)
    } else {
        None
    };
    let reconcile_result: Result<Action, ReconcileError> = async {
        let admin_handle = ctx.admin_client_for(&cluster, &bootstrap).await?;
        let mut admin = admin_handle.lock().await;
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
            let wal_generation = current_record
                .as_ref()
                .map_or(0, |record| record.wal_generation);
            topic_specs.extend(tenant_ranges.iter().map(|range| CreateTopicSpec {
                name: wal_topic_for_generation(&tenant_name, range.range_id, wal_generation),
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

        // Publish stable Service DNS before writing those names into the range
        // registry. Deployments may still be progressing, but clients never
        // observe a registry endpoint whose Kubernetes object is absent.
        let svc_api: Api<Service> = Api::namespaced(ctx.client.clone(), &ns);
        cleanup_obsolete_range_resources(&ctx, &ns, &obj, current_record.as_ref(), &tenant_ranges)
            .await?;
        if tenant_ranges.len() > 1 {
            apply_object(
                &svc_api,
                &front_door_service_name(&name),
                &render_service(&obj)?,
            )
            .await?;
        }
        for range in &tenant_ranges {
            apply_object(
                &svc_api,
                &range_service_name(&name, range.range_id),
                &render_range_service(&obj, range.range_id)?,
            )
            .await?;
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
                &tenant_ranges,
                &bootstrap,
                &wal_topic,
                &cfg_topic,
                record.state,
                kafka_sasl,
                range_control_enabled,
                range_tls_hash.as_deref(),
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

        let policy_api: Api<NetworkPolicy> = Api::namespaced(ctx.client.clone(), &ns);
        let deployments_ready = reconcile_compute_deployments(
            &ctx,
            &ns,
            &obj,
            &tenant_ranges,
            &bootstrap,
            &wal_topic,
            &cfg_topic,
            record.state,
            kafka_sasl,
            range_control_enabled,
            range_tls_hash.as_deref(),
        )
        .await?;
        if deployments_ready && let Some(operation) = active_split.as_ref() {
            let mutation_client = operator_control_mutation_client(&ctx, &ns, &obj).await?;
            if operation.phase == crabka_gres_control::SplitOperationPhase::Activated {
                verify_target_topology_ready(&mutation_client, operation)
                    .await
                    .map_err(|error| ReconcileError::Malformed(error.to_string()))?;
                reconcile_activated_cutover(&control, operation)
                    .await
                    .map_err(|error| ReconcileError::Malformed(error.to_string()))?;
            } else {
                reconcile_one_rpc_phase(&control, &mutation_client, operation)
                    .await
                    .map_err(|error| ReconcileError::Malformed(error.to_string()))?;
            }
            return Ok(Action::requeue(LIFECYCLE_REQUEUE));
        }
        apply_object(
            &policy_api,
            &network_policy_name(&name),
            &render_range_compute_network_policy(&obj)?,
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
            &tenant_api,
            &name,
            &obj,
            status,
            reason,
            message,
            Some(record.record_version),
            record.state,
            endpoint_ready,
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
    kafka_sasl: bool,
    range_control_enabled: bool,
    range_tls_hash: Option<&str>,
) -> Result<bool, ReconcileError> {
    let dep_api: Api<Deployment> = Api::namespaced(ctx.client.clone(), namespace);
    let image = ctx
        .config
        .default_gres_image
        .clone()
        .unwrap_or_else(|| DEFAULT_IMAGE.to_string());
    let tenant_name = obj.name_any();
    let mut all_ready = true;
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
            &ctx.config,
            kafka_sasl,
            range_control_enabled,
            range_tls_hash,
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
pub async fn reconcile_one_retiring_range_wal(
    control: &crate::context::GresControlHandle,
    admin: &mut (dyn crabka_client_admin::AdminClientLike + Send),
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

async fn delete_wal_topics(
    admin: &mut (dyn crabka_client_admin::AdminClientLike + Send),
    tenant: &TenantName,
    ranges: &[u32],
    generation: u64,
) -> Result<(), ReconcileError> {
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

async fn topic_exists(
    admin: &mut (dyn crabka_client_admin::AdminClientLike + Send),
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
            .or_else(|| base.and_then(|d| d.checkpoint_frames)),
        checkpoint_bytes: override_
            .and_then(|d| d.checkpoint_bytes)
            .or_else(|| base.and_then(|d| d.checkpoint_bytes)),
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
            lifecycle: Default::default(),
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
    Ok(serde_json::from_value(json!({
        "metadata": { "name": range_service_name(&name, range_id), "namespace": obj.namespace(), "labels": meta_labels(obj), "ownerReferences": [owner_ref::<GresTenant>(obj)?] },
        "spec": { "type": "ClusterIP", "selector": range_labels(obj, range_id), "ports": [{ "name": "range", "port": RANGE_PORT, "targetPort": RANGE_PORT, "protocol": "TCP" }] }
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
    operator_config: &crate::config::OperatorConfig,
    kafka_sasl: bool,
    range_control_enabled: bool,
    range_tls_hash: Option<&str>,
) -> Result<Deployment, ReconcileError> {
    let name = obj.name_any();
    let selector = range_labels(obj, range.range_id);
    let host_ranges = host_ranges_arg(range.range_id);
    let ranges = ranges_arg(all_ranges);
    let mut args = vec![
        "--listen".to_owned(),
        format!("0.0.0.0:{COMPUTE_PORT}"),
        "--substrate-bootstrap".to_owned(),
        bootstrap.to_owned(),
        "--tenant".to_owned(),
        name.clone(),
    ];
    if range_control_enabled {
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
        ]);
    }
    args.extend(checkpoint_runtime_args(operator_config)?);
    let mut env = vec![
        json!({ "name": "KAFKA_BOOTSTRAP_SERVERS", "value": bootstrap }),
        json!({ "name": "GRES_TENANT", "value": name }),
        json!({ "name": "GRES_WAL_TOPIC", "value": wal_topic }),
        json!({ "name": "GRES_CONFIG_TOPIC", "value": cfg_topic }),
        json!({ "name": "GRES_RANGES", "value": ranges }),
        json!({ "name": "GRES_HOST_RANGES", "value": host_ranges }),
    ];
    if kafka_sasl {
        env.push(json!({ "name": "GRES_KAFKA_USERNAME", "value": format!("gres-{name}") }));
        env.push(json!({ "name": "GRES_KAFKA_PASSWORD", "valueFrom": { "secretKeyRef": { "name": obj.spec.password_secret_ref.name, "key": obj.spec.password_secret_ref.key } } }));
    }
    if let Some(access_key) = &operator_config.gres_checkpoint_access_key_id {
        env.push(json!({ "name": "AWS_ACCESS_KEY_ID", "value": access_key }));
    }
    if let Some(secret_key) = &operator_config.gres_checkpoint_secret_access_key {
        env.push(json!({ "name": "AWS_SECRET_ACCESS_KEY", "value": secret_key }));
    }
    let mut ports =
        vec![json!({ "name": "postgres", "containerPort": COMPUTE_PORT, "protocol": "TCP" })];
    let (range_tls_mounts, range_tls_volumes) = if range_control_enabled {
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
    let pod_annotations = range_tls_hash
        .map(|hash| BTreeMap::from([(RANGE_TLS_HASH_ANNOTATION.to_owned(), hash.to_owned())]));
    Ok(serde_json::from_value(json!({
        "metadata": { "name": deployment_name(&name, range.range_id), "namespace": obj.namespace(), "labels": meta_labels(obj), "ownerReferences": [owner_ref::<GresTenant>(obj)?] },
        "spec": {
            "replicas": replicas,
            "selector": { "matchLabels": selector },
            "template": {
                "metadata": { "labels": selector, "annotations": pod_annotations },
                "spec": {
                    "securityContext": { "runAsNonRoot": true, "runAsUser": 65532, "fsGroup": 65532 },
                    "containers": [{
                        "name": "gres",
                        "image": image,
                        "args": args,
                        "env": env,
                        "ports": ports,
                        "volumeMounts": range_tls_mounts,
                        "readinessProbe": { "tcpSocket": { "port": COMPUTE_PORT }, "periodSeconds": 5 },
                        "resources": obj.spec.resources.clone().unwrap_or_default()
                    }],
                    "volumes": range_tls_volumes
                }
            }
        }
    }))?)
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
    // A final checkpoint must be possible even when no periodic threshold was
    // selected; this threshold also keeps the live lifecycle gate compact.
    args.extend(["--checkpoint-frames".to_owned(), "1".to_owned()]);
    Ok(args)
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
    const PGDOG_ACTIVE_GRACE_MS: u64 = 4_000;
    let observed_generation = if advance_generation {
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
    let pgdog_grace = if lifecycle_phase == TenantState::Active {
        if previous_phase == Some("active") {
            existing_grace
        } else {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|error| ReconcileError::Malformed(format!("system clock: {error}")))?
                .as_millis();
            Some(
                u64::try_from(now)
                    .unwrap_or(u64::MAX)
                    .saturating_add(PGDOG_ACTIVE_GRACE_MS),
            )
        }
    } else {
        None
    };
    let body = json!({
        "status": {
            "conditions": [condition("Ready", status, reason, message)],
            "observedGeneration": observed_generation,
            "ready": status == "True",
            "walTopic": tenant.as_ref().map(wal_topic),
            "registryVersion": registry_version.or_else(|| obj.status.as_ref().and_then(|s| s.registry_version)),
            "lifecyclePhase": lifecycle_phase.to_string(),
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
    use crate::crd::{GresTenantSpec, SecretKeyRef};

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
        let defaults = EffectiveDefaults {
            wal_replication: 1,
            checkpoint_frames: None,
            checkpoint_bytes: None,
            suspend_max_checkpoint_bytes: None,
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
    fn plaintext_kafka_deployment_omits_sasl_credentials() {
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
            &ConfigArgs::parse_from(["operator"]).config,
            false,
            false,
            None,
        )
        .unwrap();
        let json = serde_json::to_string(&deployment).unwrap();
        assert!(!json.contains("secretKeyRef"));
        assert!(!json.contains("GRES_KAFKA_PASSWORD"));
        let args = deployment.spec.unwrap().template.spec.unwrap().containers[0]
            .args
            .clone()
            .unwrap();
        assert!(!args.iter().any(|arg| arg == "--ranges"));
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
        let deployment = render_deployment(
            &obj,
            &ranges[0],
            &ranges,
            "image",
            "k:9092",
            "__gres_wal.tenant-a.r0",
            "__gres_cfg.tenant-a",
            1,
            &ConfigArgs::parse_from(["operator"]).config,
            false,
            true,
            Some("range-tls-hash"),
        )
        .unwrap();
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
    fn sasl_kafka_deployment_references_password_secret_without_plaintext() {
        let mut obj = tenant();
        obj.metadata.namespace = Some("ns".into());
        obj.metadata.uid = Some("uid".into());
        let ranges = [GresTenantRangeSpec {
            range_id: 0,
            end_key: None,
        }];
        let deployment = render_deployment(
            &obj,
            &ranges[0],
            &ranges,
            "image",
            "k:9092",
            "__gres_wal.tenant-a.r0",
            "__gres_cfg.tenant-a",
            1,
            &ConfigArgs::parse_from(["operator"]).config,
            true,
            false,
            None,
        )
        .unwrap();
        let json = serde_json::to_string(&deployment).unwrap();
        assert!(json.contains("GRES_KAFKA_PASSWORD"));
        assert!(json.contains("secretKeyRef"));
        assert!(!json.contains("hunter2"));
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
                    rowid: 0,
                }),
            },
            GresTenantRangeSpec {
                range_id: 1,
                end_key: None,
            },
        ];
        let deployment = render_deployment(
            &obj,
            &ranges[1],
            &ranges,
            "image",
            "k:9092",
            "__gres_wal.tenant-a.r1",
            "__gres_cfg.tenant-a",
            1,
            &ConfigArgs::parse_from(["operator"]).config,
            false,
            true,
            Some("range-tls-hash"),
        )
        .expect("render r1 deployment");
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
    fn range_service_has_stable_registry_name_and_selects_only_its_range() {
        let mut obj = tenant();
        obj.metadata.namespace = Some("ns".into());
        obj.metadata.uid = Some("uid".into());

        let service = render_range_service(&obj, 1).expect("render r1 service");

        assert_eq!(service.metadata.name.as_deref(), Some("tenant-a-gres-r1"));
        let spec = service.spec.expect("service spec");
        assert_eq!(
            spec.selector
                .expect("selector")
                .get("crabka.io/gres-range")
                .map(String::as_str),
            Some("r1")
        );
        let port = &spec.ports.expect("ports")[0];
        assert_eq!(port.name.as_deref(), Some("range"));
        assert_eq!(port.port, RANGE_PORT);
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
        assert!(!args.iter().any(|arg| arg == "secret"));
    }
}
