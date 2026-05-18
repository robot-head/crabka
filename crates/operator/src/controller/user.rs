//! Slice 36: `KafkaUser` reconciler — unidirectional (CRD wins).
//!
//! Provisions SCRAM-SHA-512 credentials via `AlterUserScramCredentials`
//! and keeps the ACL set in sync via `CreateAcls` / `DeleteAcls`. The
//! generated password lives in a Kubernetes Secret owner-referenced to
//! the `KafkaUser`, so cluster delete cascades automatically.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use crabka_client_admin::{
    AclEntry, AclEntryFilter, AclOperation, AdminError, DEFAULT_SCRAM_ITERATIONS, PatternType,
    PermissionType, ResourceType, ScramDeletion, ScramUpsertion,
};
use futures::StreamExt as _;
use k8s_openapi::ByteString;
use k8s_openapi::api::core::v1::Secret;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use kube::api::{Api, Patch, PatchParams};
use kube::runtime::controller::{Action, Controller};
use kube::runtime::reflector::ObjectRef;
use kube::runtime::watcher;
use kube::{Resource, ResourceExt as _};
use ring::rand::{SecureRandom, SystemRandom};
use serde_json::json;

use crate::context::Context;
use crate::controller::common::{FIELD_MANAGER, ReconcileError, condition};
use crate::controller::topic::internal_listener_bootstrap;
use crate::crd::{
    AclOp, AclPatternType, AclPermission, AclResourceKind, Authentication, Authorization, Kafka,
    KafkaUser,
};

const FINALIZER: &str = "crabka.io/user-finalizer";

/// Run the controller forever.
pub async fn run(ctx: Context) -> anyhow::Result<()> {
    let user_api: Api<KafkaUser> = Api::all(ctx.client.clone());
    let kafka_api: Api<Kafka> = Api::all(ctx.client.clone());
    Controller::new(user_api, watcher::Config::default())
        .watches(kafka_api, watcher::Config::default(), |_kafka| {
            // Empty mapper — periodic requeue picks up the transition.
            // Same posture as `topic.rs`.
            Vec::<ObjectRef<KafkaUser>>::new().into_iter()
        })
        .run(reconcile, error_policy, Arc::new(ctx))
        .for_each(|res| async move {
            match res {
                Ok((obj, _)) => tracing::debug!(?obj, "user reconciled"),
                Err(e) => tracing::warn!(error = %e, "user reconcile error"),
            }
        })
        .await;
    Ok(())
}

pub fn error_policy(_obj: Arc<KafkaUser>, err: &ReconcileError, _ctx: Arc<Context>) -> Action {
    tracing::warn!(error = %err, "user reconcile error, requeueing");
    Action::requeue(Duration::from_secs(15))
}

#[allow(clippy::too_many_lines)] // linear pipeline; extraction hurts more than helps
pub async fn reconcile(obj: Arc<KafkaUser>, ctx: Arc<Context>) -> Result<Action, ReconcileError> {
    let ns = obj.namespace().unwrap_or_else(|| "default".into());
    let name = obj.name_any();
    let user_api: Api<KafkaUser> = Api::namespaced(ctx.client.clone(), &ns);
    // Failure paths below preserve whatever `quotasInSync` was already
    // published — they haven't touched broker state, so the prior value
    // is still the truth.
    let prior_quotas_in_sync = obj.status.as_ref().is_some_and(|s| s.quotas_in_sync);

    // 1. Cluster label
    let cluster = obj
        .meta()
        .labels
        .as_ref()
        .and_then(|l| l.get("crabka.io/cluster").cloned());
    let Some(cluster) = cluster else {
        patch_status(
            &user_api,
            &name,
            &obj,
            "False",
            "MissingClusterLabel",
            "metadata.labels[\"crabka.io/cluster\"] is required",
            false,
            false,
            prior_quotas_in_sync,
        )
        .await?;
        return Ok(Action::requeue(Duration::from_mins(1)));
    };

    // 2. Validate spec
    if let Err(msg) = validate_spec(&obj.spec) {
        patch_status(
            &user_api,
            &name,
            &obj,
            "False",
            "InvalidSpec",
            &msg,
            false,
            false,
            prior_quotas_in_sync,
        )
        .await?;
        return Ok(Action::requeue(Duration::from_mins(5)));
    }

    // 3. Look up the Kafka + bootstrap
    let kafka_api: Api<Kafka> = Api::namespaced(ctx.client.clone(), &ns);
    let kafka = kafka_api.get_opt(&cluster).await?;
    let bootstrap = kafka.as_ref().and_then(internal_listener_bootstrap);
    let Some(bootstrap) = bootstrap else {
        patch_status(
            &user_api,
            &name,
            &obj,
            "False",
            "ClusterNotReady",
            &format!("Kafka/{cluster} not Ready or no internal listener"),
            false,
            false,
            prior_quotas_in_sync,
        )
        .await?;
        return Ok(Action::requeue(Duration::from_secs(30)));
    };

    let principal = principal_for(&name);

    // 4. Finalizer / delete path
    if obj.meta().deletion_timestamp.is_some() {
        // Best-effort cleanup; errors are logged but don't block finalizer removal.
        if let Ok(client) = ctx.admin_client_for(&cluster, &bootstrap).await {
            let mut admin = client.lock().await;
            match admin
                .alter_user_scram_credentials_sha512(
                    &[],
                    &[ScramDeletion {
                        username: name.clone(),
                    }],
                )
                .await
            {
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, %name, "scram delete during finalizer failed"),
            }
            let filter = AclEntryFilter {
                principal: Some(principal.clone()),
                ..Default::default()
            };
            match admin.delete_acls(&[filter]).await {
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, %name, "acl delete during finalizer failed"),
            }
            // Quotas: best-effort tombstone every key the broker
            // currently has for this user. Same posture as the SCRAM
            // and ACL paths above — log on error, never block the
            // finalizer.
            match admin.describe_user_quotas(&name).await {
                Ok(cur) if !cur.is_empty() => {
                    let ops: Vec<_> = cur
                        .into_keys()
                        .map(|key| crabka_client_admin::QuotaOp::Remove { key })
                        .collect();
                    if let Err(e) = admin.alter_user_quotas(&name, &ops, false).await {
                        tracing::warn!(error = %e, %name, "quota delete during finalizer failed");
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(error = %e, %name, "quota describe during finalizer failed");
                }
            }
        }
        remove_finalizer(&user_api, &name).await?;
        return Ok(Action::await_change());
    }

    // 5. Ensure finalizer
    if !has_finalizer(&obj) {
        add_finalizer(&user_api, &name).await?;
        return Ok(Action::requeue(Duration::ZERO));
    }

    // 6. Ensure password Secret (returns the raw password)
    let secret_api: Api<Secret> = Api::namespaced(ctx.client.clone(), &ns);
    let password_len = match &obj.spec.authentication {
        Authentication::ScramSha512(s) => s.password_length.unwrap_or(32),
    };
    let password = match ensure_password_secret(&secret_api, &obj, password_len).await {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(error = %e, %name, "ensure_password_secret failed");
            return Ok(Action::requeue(Duration::from_secs(15)));
        }
    };

    let iterations = match &obj.spec.authentication {
        Authentication::ScramSha512(s) => s.iterations.unwrap_or(DEFAULT_SCRAM_ITERATIONS),
    };

    // 7. Connect + upsert SCRAM
    let admin_handle = match ctx.admin_client_for(&cluster, &bootstrap).await {
        Ok(h) => h,
        Err(e) => {
            tracing::warn!(error = %e, %cluster, "AdminClient connect failed");
            return Ok(Action::requeue(Duration::from_secs(15)));
        }
    };
    let mut admin = admin_handle.lock().await;

    let outcomes = admin
        .alter_user_scram_credentials_sha512(
            &[ScramUpsertion {
                username: name.clone(),
                password,
                iterations,
            }],
            &[],
        )
        .await;
    let outcomes = match outcomes {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, "AlterUserScramCredentials transport failure");
            let is_transport = matches!(e, AdminError::Transport(_));
            drop(admin);
            if is_transport {
                ctx.drop_admin_client(&cluster).await;
            }
            return Ok(Action::requeue(Duration::from_secs(15)));
        }
    };
    if let Some(err) = outcomes.into_iter().find_map(|o| o.error) {
        patch_status(
            &user_api,
            &name,
            &obj,
            "False",
            "BrokerError",
            &format!("AlterUserScramCredentials: {} ({})", err.name, err.code),
            false,
            false,
            prior_quotas_in_sync,
        )
        .await?;
        return Ok(Action::requeue(Duration::from_secs(15)));
    }

    // 8. Reconcile ACLs
    let desired: BTreeSet<AclEntry> = expand_spec_acls(obj.spec.authorization.as_ref(), &principal)
        .into_iter()
        .collect();
    let filter = AclEntryFilter {
        principal: Some(principal.clone()),
        ..Default::default()
    };
    let current_vec = match admin.describe_acls(&filter).await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, "DescribeAcls failure");
            let is_transport = matches!(e, AdminError::Transport(_));
            drop(admin);
            if is_transport {
                ctx.drop_admin_client(&cluster).await;
            }
            return Ok(Action::requeue(Duration::from_secs(15)));
        }
    };
    let current: BTreeSet<AclEntry> = current_vec.into_iter().collect();
    let (additions, deletions) = diff_acls(&current, &desired);

    if !additions.is_empty()
        && let Err(e) = apply_create_acls(&mut admin, &additions).await
    {
        let is_transport = matches!(e, AdminError::Transport(_));
        drop(admin);
        if is_transport {
            ctx.drop_admin_client(&cluster).await;
        }
        return user_broker_error(&user_api, &name, &obj, e, "CreateAcls").await;
    }
    if !deletions.is_empty()
        && let Err(e) = apply_delete_acls(&mut admin, &deletions).await
    {
        let is_transport = matches!(e, AdminError::Transport(_));
        drop(admin);
        if is_transport {
            ctx.drop_admin_client(&cluster).await;
        }
        return user_broker_error(&user_api, &name, &obj, e, "DeleteAcls").await;
    }

    // 9. Reconcile quotas (slice 38). `spec.quotas == None` leaves the
    // broker's quota state untouched — a deliberate opt-in posture
    // matching Strimzi (the quotas section being absent ≠ "wipe all
    // quotas"). To clear quotas via the CRD, set `quotas: {}` (empty
    // object) — that desired-state map is empty and the diff
    // tombstones whatever the broker has.
    let quotas_in_sync = if let Some(spec_quotas) = obj.spec.quotas.as_ref() {
        let desired = spec_quotas.to_quota_map();
        let current = match admin.describe_user_quotas(&name).await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(error = %e, "DescribeClientQuotas failure");
                let is_transport = matches!(e, AdminError::Transport(_));
                drop(admin);
                if is_transport {
                    ctx.drop_admin_client(&cluster).await;
                }
                return Ok(Action::requeue(Duration::from_secs(15)));
            }
        };
        let ops = crabka_client_admin::diff_user_quotas(&current, &desired);
        if !ops.is_empty()
            && let Err(e) = apply_alter_user_quotas(&mut admin, &name, &ops).await
        {
            let is_transport = matches!(e, AdminError::Transport(_));
            drop(admin);
            if is_transport {
                ctx.drop_admin_client(&cluster).await;
            }
            return user_broker_error(&user_api, &name, &obj, e, "AlterClientQuotas").await;
        }
        true
    } else {
        false
    };

    patch_status(
        &user_api,
        &name,
        &obj,
        "True",
        "Ready",
        "user in sync",
        true,
        true,
        quotas_in_sync,
    )
    .await?;
    Ok(Action::requeue(Duration::from_mins(1)))
}

async fn apply_alter_user_quotas(
    admin: &mut tokio::sync::MutexGuard<'_, dyn crabka_client_admin::AdminClientLike + Send>,
    username: &str,
    ops: &[crabka_client_admin::QuotaOp],
) -> Result<(), AdminError> {
    if let Some(err) = admin.alter_user_quotas(username, ops, false).await? {
        return Err(AdminError::Broker {
            api: "AlterClientQuotas",
            code: err.code,
            name: err.name,
            message: err.message,
        });
    }
    Ok(())
}

async fn apply_create_acls(
    admin: &mut tokio::sync::MutexGuard<'_, dyn crabka_client_admin::AdminClientLike + Send>,
    additions: &[AclEntry],
) -> Result<(), AdminError> {
    let outcomes = admin.create_acls(additions).await?;
    if let Some(err) = outcomes.into_iter().find_map(|o| o.error) {
        return Err(AdminError::Broker {
            api: "CreateAcls",
            code: err.code,
            name: err.name,
            message: err.message,
        });
    }
    Ok(())
}

async fn apply_delete_acls(
    admin: &mut tokio::sync::MutexGuard<'_, dyn crabka_client_admin::AdminClientLike + Send>,
    deletions: &[AclEntry],
) -> Result<(), AdminError> {
    let filters: Vec<AclEntryFilter> = deletions.iter().map(entry_to_exact_filter).collect();
    let outcomes = admin.delete_acls(&filters).await?;
    if let Some(err) = outcomes.into_iter().find_map(|o| o.error) {
        return Err(AdminError::Broker {
            api: "DeleteAcls",
            code: err.code,
            name: err.name,
            message: err.message,
        });
    }
    Ok(())
}

async fn user_broker_error(
    api: &Api<KafkaUser>,
    name: &str,
    obj: &KafkaUser,
    err: AdminError,
    op: &str,
) -> Result<Action, ReconcileError> {
    let detail = match err {
        AdminError::Broker { code, name, .. } => format!("{op}: {name} ({code})"),
        other => format!("{op}: {other}"),
    };
    let prior_qis = obj.status.as_ref().is_some_and(|s| s.quotas_in_sync);
    patch_status(
        api,
        name,
        obj,
        "False",
        "BrokerError",
        &detail,
        false,
        false,
        prior_qis,
    )
    .await?;
    Ok(Action::requeue(Duration::from_secs(15)))
}

/// Build an `AclEntryFilter` that matches exactly one `AclEntry`.
/// Used to scope `DeleteAcls` to a single tuple at a time.
pub(crate) fn entry_to_exact_filter(e: &AclEntry) -> AclEntryFilter {
    AclEntryFilter {
        resource_type: Some(e.resource_type),
        resource_name: Some(e.resource_name.clone()),
        pattern_type: Some(e.pattern_type),
        principal: Some(e.principal.clone()),
        host: Some(e.host.clone()),
        operation: Some(e.operation),
        permission_type: Some(e.permission_type),
    }
}

/// Compose Kafka principal name from a CRD username.
pub(crate) fn principal_for(name: &str) -> String {
    format!("User:{name}")
}

/// Expand the CRD's `acls` list (one rule may carry many operations)
/// into the per-tuple `AclEntry` set the broker stores. Empty when
/// `authorization` is absent.
pub(crate) fn expand_spec_acls(auth: Option<&Authorization>, principal: &str) -> Vec<AclEntry> {
    let Some(Authorization::Simple(simple)) = auth else {
        return Vec::new();
    };
    let mut out = Vec::with_capacity(simple.acls.len());
    for rule in &simple.acls {
        for op in &rule.operations {
            out.push(AclEntry {
                resource_type: resource_kind_to_admin(rule.resource.kind),
                resource_name: rule.resource.name.clone(),
                pattern_type: pattern_to_admin(rule.resource.pattern_type),
                principal: principal.to_string(),
                host: rule.host.clone(),
                operation: op_to_admin(*op),
                permission_type: permission_to_admin(rule.permission),
            });
        }
    }
    out
}

/// Pure: split `(desired, current)` into `(additions, deletions)`.
/// `additions` are entries in `desired` but not in `current`; `deletions`
/// are entries in `current` but not in `desired`.
pub(crate) fn diff_acls(
    current: &BTreeSet<AclEntry>,
    desired: &BTreeSet<AclEntry>,
) -> (Vec<AclEntry>, Vec<AclEntry>) {
    let additions: Vec<AclEntry> = desired.difference(current).cloned().collect();
    let deletions: Vec<AclEntry> = current.difference(desired).cloned().collect();
    (additions, deletions)
}

fn validate_spec(spec: &crate::crd::KafkaUserSpec) -> Result<(), String> {
    if let Some(Authorization::Simple(simple)) = &spec.authorization {
        for (i, rule) in simple.acls.iter().enumerate() {
            if rule.operations.is_empty() {
                return Err(format!("acls[{i}].operations is empty"));
            }
            if rule.resource.name.is_empty() {
                return Err(format!("acls[{i}].resource.name is empty"));
            }
        }
    }
    Ok(())
}

fn resource_kind_to_admin(k: AclResourceKind) -> ResourceType {
    match k {
        AclResourceKind::Topic => ResourceType::Topic,
        AclResourceKind::Group => ResourceType::Group,
        AclResourceKind::Cluster => ResourceType::Cluster,
        AclResourceKind::TransactionalId => ResourceType::TransactionalId,
    }
}

fn pattern_to_admin(p: AclPatternType) -> PatternType {
    match p {
        AclPatternType::Literal => PatternType::Literal,
        AclPatternType::Prefixed => PatternType::Prefixed,
    }
}

fn permission_to_admin(p: AclPermission) -> PermissionType {
    match p {
        AclPermission::Allow => PermissionType::Allow,
        AclPermission::Deny => PermissionType::Deny,
    }
}

fn op_to_admin(op: AclOp) -> AclOperation {
    match op {
        AclOp::All => AclOperation::All,
        AclOp::Read => AclOperation::Read,
        AclOp::Write => AclOperation::Write,
        AclOp::Create => AclOperation::Create,
        AclOp::Delete => AclOperation::Delete,
        AclOp::Alter => AclOperation::Alter,
        AclOp::Describe => AclOperation::Describe,
        AclOp::ClusterAction => AclOperation::ClusterAction,
        AclOp::DescribeConfigs => AclOperation::DescribeConfigs,
        AclOp::AlterConfigs => AclOperation::AlterConfigs,
        AclOp::IdempotentWrite => AclOperation::IdempotentWrite,
    }
}

// --- Secret management ---------------------------------------------------

/// Get-or-create the password Secret. If a Secret already exists with a
/// `password` key, reuse it (the SCRAM credential is regenerated each
/// reconcile from the same plaintext so the operator-managed password
/// is stable). On first reconcile the operator allocates
/// `password_len_bytes` of cryptographically-random data and stores its
/// URL-safe base64 encoding.
async fn ensure_password_secret(
    api: &Api<Secret>,
    obj: &KafkaUser,
    password_len_bytes: u16,
) -> Result<String, ReconcileError> {
    let name = obj.name_any();
    if let Some(existing) = api.get_opt(&name).await?
        && let Some(p) = read_password(&existing)
    {
        return Ok(p);
    }
    let password = random_password(password_len_bytes);
    let secret = render_password_secret(obj, &password)?;
    let params = PatchParams {
        field_manager: Some(FIELD_MANAGER.into()),
        force: true,
        ..Default::default()
    };
    api.patch(&name, &params, &Patch::Apply(&secret)).await?;
    Ok(password)
}

fn read_password(secret: &Secret) -> Option<String> {
    let data = secret.data.as_ref()?;
    let bs = data.get("password")?;
    std::str::from_utf8(&bs.0).ok().map(str::to_string)
}

fn random_password(len_bytes: u16) -> String {
    let mut buf = vec![0u8; len_bytes as usize];
    SystemRandom::new()
        .fill(&mut buf)
        .expect("system RNG must succeed");
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(buf)
}

/// Render the user-credential Secret. Owner-ref'd to the `KafkaUser`
/// so cluster delete cascades. Keys:
///   - `password` (raw plaintext, used by `KafkaUser` admin RPCs and
///     pasted into application configuration)
///   - `sasl.jaas.config` (ready-to-paste JAAS line for JVM clients)
fn render_password_secret(obj: &KafkaUser, password: &str) -> Result<Secret, ReconcileError> {
    let name = obj.name_any();
    let jaas = format!(
        "org.apache.kafka.common.security.scram.ScramLoginModule required \
         username=\"{name}\" password=\"{password}\";"
    );
    let mut labels: BTreeMap<String, String> = BTreeMap::new();
    labels.insert("app.kubernetes.io/name".into(), "crabka-broker".into());
    labels.insert(
        "app.kubernetes.io/managed-by".into(),
        "crabka-operator".into(),
    );
    if let Some(cluster) = obj
        .meta()
        .labels
        .as_ref()
        .and_then(|l| l.get("crabka.io/cluster"))
    {
        labels.insert("crabka.io/cluster".into(), cluster.clone());
    }
    labels.insert("crabka.io/user".into(), name.clone());

    let mut data: BTreeMap<String, ByteString> = BTreeMap::new();
    data.insert("password".into(), ByteString(password.as_bytes().to_vec()));
    data.insert("sasl.jaas.config".into(), ByteString(jaas.into_bytes()));

    Ok(Secret {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: obj.meta().namespace.clone(),
            labels: Some(labels),
            owner_references: Some(vec![user_owner_ref(obj)?]),
            ..Default::default()
        },
        type_: Some("Opaque".into()),
        data: Some(data),
        ..Default::default()
    })
}

fn user_owner_ref(obj: &KafkaUser) -> Result<OwnerReference, ReconcileError> {
    let uid = obj
        .meta()
        .uid
        .as_deref()
        .ok_or(ReconcileError::MissingUid)?;
    Ok(OwnerReference {
        api_version: <KafkaUser as Resource>::api_version(&()).to_string(),
        kind: <KafkaUser as Resource>::kind(&()).to_string(),
        name: obj.name_any(),
        uid: uid.to_string(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    })
}

// --- Finalizer + status helpers -----------------------------------------

fn has_finalizer(obj: &KafkaUser) -> bool {
    obj.meta()
        .finalizers
        .as_ref()
        .is_some_and(|f| f.iter().any(|s| s == FINALIZER))
}

async fn add_finalizer(api: &Api<KafkaUser>, name: &str) -> Result<(), ReconcileError> {
    let patch = json!({ "metadata": { "finalizers": [FINALIZER] } });
    let params = PatchParams {
        field_manager: Some(FIELD_MANAGER.into()),
        ..Default::default()
    };
    api.patch(name, &params, &Patch::Merge(&patch)).await?;
    Ok(())
}

async fn remove_finalizer(api: &Api<KafkaUser>, name: &str) -> Result<(), ReconcileError> {
    let patch = json!({ "metadata": { "finalizers": [] } });
    let params = PatchParams {
        field_manager: Some(FIELD_MANAGER.into()),
        ..Default::default()
    };
    api.patch(name, &params, &Patch::Merge(&patch)).await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn patch_status(
    api: &Api<KafkaUser>,
    name: &str,
    obj: &KafkaUser,
    status: &str,
    reason: &str,
    message: &str,
    scram_sha512: bool,
    advance_generation: bool,
    quotas_in_sync: bool,
) -> Result<(), ReconcileError> {
    let conditions = vec![condition("Ready", status, reason, message)];
    let observed_generation = if advance_generation {
        obj.meta().generation
    } else {
        obj.status.as_ref().and_then(|s| s.observed_generation)
    };
    // Once the credential is provisioned, the secret name == metadata.name.
    let secret_name = if scram_sha512 {
        Some(name.to_string())
    } else {
        obj.status.as_ref().and_then(|s| s.secret.clone())
    };
    let body = json!({
        "status": {
            "conditions": conditions,
            "observedGeneration": observed_generation,
            "username": name,
            "secret": secret_name,
            "scramSha512": scram_sha512 || obj.status.as_ref().is_some_and(|s| s.scram_sha512),
            "quotasInSync": quotas_in_sync,
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
    use super::*;
    use crate::crd::{
        AclOp, AclPatternType, AclPermission, AclResource, AclResourceKind, AclRule,
        SimpleAuthorization,
    };

    fn rule(kind: AclResourceKind, name: &str, ops: &[AclOp]) -> AclRule {
        AclRule {
            resource: AclResource {
                kind,
                name: name.into(),
                pattern_type: AclPatternType::Literal,
            },
            operations: ops.to_vec(),
            host: "*".into(),
            permission: AclPermission::Allow,
        }
    }

    #[test]
    fn principal_uses_user_prefix() {
        assert_eq!(principal_for("alice"), "User:alice");
    }

    #[test]
    fn expand_spec_acls_with_no_authorization_is_empty() {
        assert!(expand_spec_acls(None, "User:alice").is_empty());
    }

    #[test]
    fn expand_spec_acls_one_rule_one_op() {
        let auth = Authorization::Simple(SimpleAuthorization {
            acls: vec![rule(AclResourceKind::Topic, "orders", &[AclOp::Read])],
        });
        let entries = expand_spec_acls(Some(&auth), "User:alice");
        assert_eq!(entries.len(), 1);
        let e = &entries[0];
        assert_eq!(e.resource_type, ResourceType::Topic);
        assert_eq!(e.resource_name, "orders");
        assert_eq!(e.principal, "User:alice");
        assert_eq!(e.operation, AclOperation::Read);
        assert_eq!(e.permission_type, PermissionType::Allow);
    }

    #[test]
    fn expand_spec_acls_one_rule_many_ops_fans_out() {
        let auth = Authorization::Simple(SimpleAuthorization {
            acls: vec![rule(
                AclResourceKind::Topic,
                "orders",
                &[AclOp::Read, AclOp::Describe, AclOp::Write],
            )],
        });
        let entries = expand_spec_acls(Some(&auth), "User:alice");
        assert_eq!(entries.len(), 3);
        let ops: Vec<_> = entries.iter().map(|e| e.operation).collect();
        assert!(ops.contains(&AclOperation::Read));
        assert!(ops.contains(&AclOperation::Describe));
        assert!(ops.contains(&AclOperation::Write));
    }

    #[test]
    fn diff_acls_additions_and_deletions() {
        let mut current = BTreeSet::new();
        let keep = AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: "keep".into(),
            pattern_type: PatternType::Literal,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: AclOperation::Read,
            permission_type: PermissionType::Allow,
        };
        let drop = AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: "drop".into(),
            pattern_type: PatternType::Literal,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: AclOperation::Read,
            permission_type: PermissionType::Allow,
        };
        current.insert(keep.clone());
        current.insert(drop.clone());

        let mut desired = BTreeSet::new();
        let add = AclEntry {
            resource_type: ResourceType::Group,
            resource_name: "g".into(),
            pattern_type: PatternType::Literal,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: AclOperation::Read,
            permission_type: PermissionType::Allow,
        };
        desired.insert(keep.clone());
        desired.insert(add.clone());

        let (adds, dels) = diff_acls(&current, &desired);
        assert_eq!(adds, vec![add]);
        assert_eq!(dels, vec![drop]);
    }

    #[test]
    fn diff_acls_noop_when_matching() {
        let mut s = BTreeSet::new();
        let e = AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: "x".into(),
            pattern_type: PatternType::Literal,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: AclOperation::Read,
            permission_type: PermissionType::Allow,
        };
        s.insert(e);
        let (adds, dels) = diff_acls(&s, &s);
        assert!(adds.is_empty());
        assert!(dels.is_empty());
    }

    #[test]
    fn validate_spec_rejects_empty_operations() {
        let spec = crate::crd::KafkaUserSpec {
            authentication: Authentication::ScramSha512(crate::crd::ScramSha512Auth::default()),
            authorization: Some(Authorization::Simple(SimpleAuthorization {
                acls: vec![AclRule {
                    resource: AclResource {
                        kind: AclResourceKind::Topic,
                        name: "x".into(),
                        pattern_type: AclPatternType::Literal,
                    },
                    operations: vec![],
                    host: "*".into(),
                    permission: AclPermission::Allow,
                }],
            })),
            quotas: None,
        };
        let err = validate_spec(&spec).unwrap_err();
        assert!(err.contains("operations is empty"), "got: {err}");
    }

    #[test]
    fn validate_spec_rejects_empty_resource_name() {
        let spec = crate::crd::KafkaUserSpec {
            authentication: Authentication::ScramSha512(crate::crd::ScramSha512Auth::default()),
            authorization: Some(Authorization::Simple(SimpleAuthorization {
                acls: vec![rule(AclResourceKind::Topic, "", &[AclOp::Read])],
            })),
            quotas: None,
        };
        let err = validate_spec(&spec).unwrap_err();
        assert!(err.contains("resource.name is empty"), "got: {err}");
    }

    #[test]
    fn entry_to_exact_filter_populates_every_axis() {
        let e = AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: "orders".into(),
            pattern_type: PatternType::Literal,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: AclOperation::Read,
            permission_type: PermissionType::Allow,
        };
        let f = entry_to_exact_filter(&e);
        assert_eq!(f.resource_type, Some(ResourceType::Topic));
        assert_eq!(f.resource_name.as_deref(), Some("orders"));
        assert_eq!(f.pattern_type, Some(PatternType::Literal));
        assert_eq!(f.principal.as_deref(), Some("User:alice"));
        assert_eq!(f.host.as_deref(), Some("*"));
        assert_eq!(f.operation, Some(AclOperation::Read));
        assert_eq!(f.permission_type, Some(PermissionType::Allow));
    }

    #[test]
    fn random_password_is_base64_and_uniform_length() {
        let p1 = random_password(32);
        let p2 = random_password(32);
        assert_eq!(p1.len(), p2.len()); // base64 of 32 random bytes = 43 chars
        assert_ne!(p1, p2);
        assert!(
            p1.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
            "got: {p1}"
        );
    }
}
