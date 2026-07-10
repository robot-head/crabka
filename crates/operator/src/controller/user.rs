//! `KafkaUser` reconciler — unidirectional (CRD wins).
//!
//! Provisions SCRAM-SHA-512 credentials via `AlterUserScramCredentials`
//! and keeps the ACL set in sync via `CreateAcls` / `DeleteAcls`. The
//! generated password lives in a Kubernetes Secret owner-referenced to
//! the `KafkaUser`, so cluster delete cascades automatically.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use base64::Engine as _;
use crabka_client_admin::{
    AclEntry, AclEntryFilter, AclOperation, AdminError, DEFAULT_SCRAM_ITERATIONS, PatternType,
    PermissionType, ResourceType, ScramDeletion, ScramUpsertion,
};
use futures::StreamExt as _;
use k8s_openapi::{
    ByteString,
    api::core::v1::Secret,
    apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference},
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
use ring::rand::{SecureRandom, SystemRandom};
use serde_json::json;

use crate::{
    context::Context,
    controller::{
        common::{self, FIELD_MANAGER, ReconcileError, condition},
        topic::internal_listener_bootstrap,
        user_delegation_token::{self, KubeKafkaUserStatusWriter, KubeSecretWriter},
        user_tls,
    },
    crd::{
        AclOp, AclPatternType, AclPermission, AclResourceKind, Authentication, Kafka, KafkaUser,
        KafkaUserAuthorization as Authorization,
    },
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

/// Reconcile entry point. Times the pass and records the reconcile
/// counter/histogram, then delegates to [`reconcile_inner`].
#[tracing::instrument(
    level = "info",
    skip_all,
    fields(
        kind = "KafkaUser",
        namespace = %obj.namespace().unwrap_or_else(|| "default".into()),
        name = %obj.name_any(),
        generation = ?obj.meta().generation,
    )
)]
pub async fn reconcile(obj: Arc<KafkaUser>, ctx: Arc<Context>) -> Result<Action, ReconcileError> {
    common::record_reconcile(
        &ctx,
        "KafkaUser",
        Box::pin(reconcile_inner(obj, ctx.clone())),
    )
    .await
}

#[allow(clippy::too_many_lines)] // linear pipeline; extraction hurts more than helps
async fn reconcile_inner(obj: Arc<KafkaUser>, ctx: Arc<Context>) -> Result<Action, ReconcileError> {
    let ns = obj.namespace().unwrap_or_else(|| "default".into());
    let name = obj.name_any();
    let user_api: Api<KafkaUser> = Api::namespaced(ctx.client.clone(), &ns);
    // Failure paths below preserve whatever `quotasInSync` / `tls*` was
    // already published — they haven't touched broker state, so the
    // prior value is still the truth.
    let prior_quotas_in_sync = obj.status.as_ref().is_some_and(|s| s.quotas_in_sync);
    let prior_tls = obj.status.as_ref().is_some_and(|s| s.tls);
    let prior_external = obj.status.as_ref().is_some_and(|s| s.external);
    let prior_tls_not_after = obj
        .status
        .as_ref()
        .and_then(|s| s.tls_cert_not_after.clone());
    let prior_tls_principal = obj.status.as_ref().and_then(|s| s.tls_principal.clone());

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
            StatusPatch {
                obj: &obj,
                status: "False",
                reason: "MissingClusterLabel",
                message: "metadata.labels[\"crabka.io/cluster\"] is required",
                scram_sha512: false,
                scram_sha256: false,
                tls: prior_tls,
                external: prior_external,
                tls_cert_not_after: prior_tls_not_after.clone(),
                tls_principal: prior_tls_principal.clone(),
                advance_generation: false,
                quotas_in_sync: prior_quotas_in_sync,
            },
        )
        .await?;
        return Ok(Action::requeue(Duration::from_mins(1)));
    };

    // 2. Validate spec
    if let Err(msg) = validate_spec(&obj.spec) {
        patch_status(
            &user_api,
            &name,
            StatusPatch {
                obj: &obj,
                status: "False",
                reason: "InvalidSpec",
                message: &msg,
                scram_sha512: false,
                scram_sha256: false,
                tls: prior_tls,
                external: prior_external,
                tls_cert_not_after: prior_tls_not_after.clone(),
                tls_principal: prior_tls_principal.clone(),
                advance_generation: false,
                quotas_in_sync: prior_quotas_in_sync,
            },
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
            StatusPatch {
                obj: &obj,
                status: "False",
                reason: "ClusterNotReady",
                message: &format!("Kafka/{cluster} not Ready or no internal listener"),
                scram_sha512: false,
                scram_sha256: false,
                tls: prior_tls,
                external: prior_external,
                tls_cert_not_after: prior_tls_not_after.clone(),
                tls_principal: prior_tls_principal.clone(),
                advance_generation: false,
                quotas_in_sync: prior_quotas_in_sync,
            },
        )
        .await?;
        return Ok(Action::requeue(Duration::from_secs(30)));
    };

    let principal = principal_for(&name, &obj.spec.authentication);
    // For TLS users the broker stores quotas keyed by the cert DN
    // (i.e. `principal` without the `User:` prefix).  For SCRAM users
    // this is just `name`.
    let quota_username: String = principal
        .strip_prefix("User:")
        .unwrap_or(&principal)
        .to_string();

    // 4. Finalizer / delete path
    if obj.meta().deletion_timestamp.is_some() {
        // Best-effort cleanup; errors are logged but don't block finalizer removal.
        if let Ok(client) = ctx.admin_client_for(&cluster, &bootstrap).await {
            let mut admin = client.lock().await;
            // SCRAM-SHA-256 + SCRAM-SHA-512 both reach
            // `alter_user_scram_credentials_*` for the finalizer
            // deletion — the broker stores credentials per mechanism,
            // so we tear down whichever mechanism this user used.
            let scram_finalizer = match &obj.spec.authentication {
                Authentication::ScramSha512(_) => Some(true),
                Authentication::ScramSha256(_) => Some(false),
                _ => None,
            };
            if let Some(is_sha512) = scram_finalizer {
                let deletion = ScramDeletion {
                    username: name.clone(),
                };
                let result = if is_sha512 {
                    admin
                        .alter_user_scram_credentials_sha512(&[], &[deletion])
                        .await
                } else {
                    admin
                        .alter_user_scram_credentials_sha256(&[], &[deletion])
                        .await
                };
                if let Err(e) = result {
                    tracing::warn!(error = %e, %name, "scram delete during finalizer failed");
                }
            }
            // Delegation-token finalizer — expire every token
            // owned by `User:<name>` via `ExpireDelegationToken` (period
            // -1 = immediate tombstone). Best-effort: errors are logged
            // and never block finalizer removal.
            //
            // The `expire_owned_tokens` helper Describe-lists tokens owned
            // by `User:<name>` then calls `ExpireDelegationToken` for each.
            // It takes a `&dyn DelegationTokenAdmin` — the
            // `AdminClientHandle` itself implements that trait (see
            // `user_delegation_token.rs`), but we already hold the mutex
            // guard `admin` here, so we drop it briefly so the trait impl
            // can re-acquire.
            if matches!(&obj.spec.authentication, Authentication::DelegationToken(_)) {
                drop(admin);
                if let Err(e) = user_delegation_token::expire_owned_tokens(&name, &client).await {
                    tracing::warn!(
                        error = %e,
                        %name,
                        "delegation-token finalizer expire failed",
                    );
                }
                // Re-acquire for the ACL + quota tombstones below.
                admin = client.lock().await;
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
            // finalizer.  Keyed by DN-minus-`User:` for TLS users.
            match admin.describe_user_quotas(&quota_username).await {
                Ok(cur) if !cur.is_empty() => {
                    let ops: Vec<_> = cur
                        .into_keys()
                        .map(|key| crabka_client_admin::QuotaOp::Remove { key })
                        .collect();
                    if let Err(e) = admin.alter_user_quotas(&quota_username, &ops, false).await {
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

    // 6. Provision credentials per auth type.
    //
    // SCRAM arm: ensure password Secret, then issue
    // `AlterUserScramCredentials` upsert.
    // TLS arm: ensure clients-CA, then issue (or reuse) the per-user
    // cert Secret. No broker call needed — the broker learns the user
    // from the certificate at mTLS handshake time.
    let secret_api: Api<Secret> = Api::namespaced(ctx.client.clone(), &ns);
    let tls_not_after: Option<String> = match &obj.spec.authentication {
        Authentication::ScramSha512(_) | Authentication::ScramSha256(_) => {
            // SCRAM-SHA-256 + SCRAM-SHA-512 share the
            // password-secret + admin-client + status-patch flow; the
            // only difference is which `_sha256` / `_sha512` admin
            // method we call. Pluck the per-variant knobs here.
            let (password_len, iterations, is_sha512) = match &obj.spec.authentication {
                Authentication::ScramSha512(s) => (
                    s.password_length.unwrap_or(32),
                    s.iterations.unwrap_or(DEFAULT_SCRAM_ITERATIONS),
                    true,
                ),
                Authentication::ScramSha256(s) => (
                    s.password_length.unwrap_or(32),
                    s.iterations.unwrap_or(DEFAULT_SCRAM_ITERATIONS),
                    false,
                ),
                _ => unreachable!(),
            };
            let password = match ensure_password_secret(&secret_api, &obj, password_len).await {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(error = %e, %name, "ensure_password_secret failed");
                    return Ok(Action::requeue(Duration::from_secs(15)));
                }
            };

            // 7. Connect + upsert SCRAM (within the SCRAM arm). The
            // admin connection is opened again below for ACL + quota
            // work; the brief duplication is intentional and keeps
            // each arm self-contained.
            let admin_handle = match ctx.admin_client_for(&cluster, &bootstrap).await {
                Ok(h) => h,
                Err(e) => {
                    tracing::warn!(error = %e, %cluster, "AdminClient connect failed");
                    return Ok(Action::requeue(Duration::from_secs(15)));
                }
            };
            let mut admin = admin_handle.lock().await;
            let upsertions = [ScramUpsertion {
                username: name.clone(),
                password,
                iterations,
            }];
            let outcomes = if is_sha512 {
                admin
                    .alter_user_scram_credentials_sha512(&upsertions, &[])
                    .await
            } else {
                admin
                    .alter_user_scram_credentials_sha256(&upsertions, &[])
                    .await
            };
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
                drop(admin);
                patch_status(
                    &user_api,
                    &name,
                    StatusPatch {
                        obj: &obj,
                        status: "False",
                        reason: "BrokerError",
                        message: &format!("AlterUserScramCredentials: {} ({})", err.name, err.code),
                        scram_sha512: false,
                        scram_sha256: false,
                        tls: prior_tls,
                        external: prior_external,
                        tls_cert_not_after: prior_tls_not_after.clone(),
                        tls_principal: prior_tls_principal.clone(),
                        advance_generation: false,
                        quotas_in_sync: prior_quotas_in_sync,
                    },
                )
                .await?;
                return Ok(Action::requeue(Duration::from_secs(15)));
            }
            None
        }
        Authentication::Tls(tls_auth) => {
            let kafka_ref = kafka
                .as_ref()
                .expect("bootstrap presence implies Kafka resource is Some");
            let ca_outcome = match crate::controller::cluster_ca::reconcile_ca(
                &secret_api,
                kafka_ref,
                crate::controller::cluster_ca::WhichCa::Clients,
                false,
                false,
                true,
                time::OffsetDateTime::now_utc(),
            )
            .await
            {
                Ok(o) => o,
                Err(e) => {
                    tracing::warn!(error = %e, %cluster, "clients CA reconcile failed");
                    return Ok(Action::requeue(Duration::from_secs(15)));
                }
            };
            // Sign user certs with the clients CA's active signer (the
            // first block of the trust bundle, paired with the active key).
            let ca = ca_outcome.signing_material;
            let cert_status =
                match user_tls::ensure_user_cert_secret(&secret_api, &obj, &ca, tls_auth).await {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!(error = %e, %name, "ensure_user_cert_secret failed");
                        return Ok(Action::requeue(Duration::from_secs(15)));
                    }
                };
            if cert_status.issued_new {
                tracing::info!(
                    %name,
                    not_after = %cert_status.not_after,
                    "issued new client cert",
                );
            }
            Some(cert_status.not_after)
        }
        // Credential-less user: the operator does not create a Secret
        // or issue a cert. ACL + quota reconciliation below is
        // principal-driven and Just Works for this arm.
        Authentication::TlsExternal => None,
        // Dispatch the delegation-token reconcile.
        //
        // The token lifecycle (Describe → decide → Create/Renew/NoOp/Cycle
        // → Secret + status patch) lives in
        // `controller::user_delegation_token`. The `AdminClientHandle`
        // now implements `DelegationTokenAdmin` directly (see the
        // `impl` block in `user_delegation_token.rs`), so we hand the
        // handle in as `&dyn DelegationTokenAdmin`.
        //
        // The module's `reconcile` returns an `Action` on its own —
        // computed from the live token's `expiry_timestamp_ms` minus
        // the spec's `renew_before_expiry_ms` — so this arm bypasses
        // the trailing ACL/quota reconciliation block by returning
        // early. (ACLs + quotas for delegation-token users are reached
        // on the next requeue: `reconcile` here returns just the
        // credential-side `Action`; the ACL/quota side would otherwise
        // need a second admin-client lock under the held mutex.)
        Authentication::DelegationToken(dt) => {
            let admin_handle = match ctx.admin_client_for(&cluster, &bootstrap).await {
                Ok(h) => h,
                Err(e) => {
                    tracing::warn!(error = %e, %cluster, "AdminClient connect failed");
                    return Ok(Action::requeue(Duration::from_secs(15)));
                }
            };
            let secret_writer = KubeSecretWriter {
                api: secret_api.clone(),
            };
            let user_writer = KubeKafkaUserStatusWriter {
                api: user_api.clone(),
            };
            let now_ms = chrono::Utc::now().timestamp_millis();
            let out = user_delegation_token::reconcile(
                &obj,
                dt,
                &admin_handle,
                &secret_writer,
                &user_writer,
                now_ms,
            )
            .await?;
            // ACL + quota reconciliation for delegation-token users
            // happens in a follow-up reconcile pass — the token
            // module's `reconcile` already wrote the credential Secret
            // and patched status, and the operator's per-user requeue
            // will pick up ACL drift on the next pass. Return early
            // with the renew-driven `Action`.
            return Ok(out.action);
        }
    };

    // Open admin for ACL + quota reconciliation (steps 8 + 9). Common
    // to both auth arms.
    let admin_handle = match ctx.admin_client_for(&cluster, &bootstrap).await {
        Ok(h) => h,
        Err(e) => {
            tracing::warn!(error = %e, %cluster, "AdminClient connect failed");
            return Ok(Action::requeue(Duration::from_secs(15)));
        }
    };
    let mut admin = admin_handle.lock().await;

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

    // 9. Reconcile quotas. `spec.quotas == None` leaves the
    // broker's quota state untouched — a deliberate opt-in posture
    // matching Strimzi (the quotas section being absent ≠ "wipe all
    // quotas"). To clear quotas via the CRD, set `quotas: {}` (empty
    // object) — that desired-state map is empty and the diff
    // tombstones whatever the broker has.
    let quotas_in_sync = if let Some(spec_quotas) = obj.spec.quotas.as_ref() {
        let desired = spec_quotas.to_quota_map();
        let current = match admin.describe_user_quotas(&quota_username).await {
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
            && let Err(e) = apply_alter_user_quotas(&mut admin, &quota_username, &ops).await
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

    let is_scram_sha512 = matches!(&obj.spec.authentication, Authentication::ScramSha512(_));
    let is_scram_sha256 = matches!(&obj.spec.authentication, Authentication::ScramSha256(_));
    let is_tls = matches!(&obj.spec.authentication, Authentication::Tls(_));
    let is_external = matches!(&obj.spec.authentication, Authentication::TlsExternal);
    patch_status(
        &user_api,
        &name,
        StatusPatch {
            obj: &obj,
            status: "True",
            reason: "Ready",
            message: "user in sync",
            scram_sha512: is_scram_sha512,
            scram_sha256: is_scram_sha256,
            tls: is_tls && tls_not_after.is_some(),
            external: is_external,
            tls_cert_not_after: tls_not_after.clone(),
            // `tls_principal` is the principal the operator pinned in ACLs.
            // For TLS users that's `User:CN=<name>`; for `tls-external`
            // users it's `User:<name>`. SCRAM users skip this — `username`
            // already carries the same string for them.
            tls_principal: if is_tls || is_external {
                Some(principal.clone())
            } else {
                prior_tls_principal.clone()
            },
            advance_generation: true,
            quotas_in_sync,
        },
    )
    .await?;
    // TLS certs live ~365d by default with a 30d renewal window — once
    // a minute requeue is overkill. SCRAM stays at the existing
    // cadence (password rotation is operator-driven, not time-driven,
    // but the per-minute requeue covers external drift in ACLs /
    // quotas).
    // `tls-external` users have no operator-owned credential to rotate,
    // but ACLs + quotas can drift externally — keep the per-minute
    // requeue to detect that (same cadence as SCRAM, different reason).
    // Delegation-token users do not reach this match — that
    // arm returns its renew-driven `Action` earlier (see the
    // `Authentication::DelegationToken(dt)` arm above).
    #[allow(clippy::match_same_arms)]
    let requeue = match &obj.spec.authentication {
        Authentication::ScramSha512(_) | Authentication::ScramSha256(_) => Duration::from_mins(1),
        Authentication::Tls(_) => Duration::from_hours(6),
        Authentication::TlsExternal => Duration::from_mins(1),
        Authentication::DelegationToken(_) => unreachable!(
            "delegation-token arm returns early after user_delegation_token::reconcile",
        ),
    };
    Ok(Action::requeue(requeue))
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

#[tracing::instrument(level = "info", skip_all, fields(additions = additions.len()), err)]
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

#[tracing::instrument(level = "info", skip_all, fields(deletions = deletions.len()), err)]
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
    let prior_tls = obj.status.as_ref().is_some_and(|s| s.tls);
    let prior_external = obj.status.as_ref().is_some_and(|s| s.external);
    let prior_tls_not_after = obj
        .status
        .as_ref()
        .and_then(|s| s.tls_cert_not_after.clone());
    let prior_tls_principal = obj.status.as_ref().and_then(|s| s.tls_principal.clone());
    patch_status(
        api,
        name,
        StatusPatch {
            obj,
            status: "False",
            reason: "BrokerError",
            message: &detail,
            scram_sha512: false,
            scram_sha256: false,
            tls: prior_tls,
            external: prior_external,
            tls_cert_not_after: prior_tls_not_after,
            tls_principal: prior_tls_principal,
            advance_generation: false,
            quotas_in_sync: prior_qis,
        },
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

/// Kafka principal for the user. SCRAM and `tls-external` users use
/// bare `User:<name>`; TLS users use `User:CN=<name>` (matches the
/// cert's Subject DN, which is what the broker's
/// `extract_principal_from_cert` returns at runtime).
#[allow(clippy::match_same_arms)]
pub(crate) fn principal_for(name: &str, auth: &Authentication) -> String {
    match auth {
        Authentication::ScramSha512(_) | Authentication::ScramSha256(_) => format!("User:{name}"),
        Authentication::Tls(_) => user_tls::tls_principal(name),
        // `tls-external` credentials are managed out-of-band (OIDC for
        // OAUTHBEARER, or an external CA whose certs carry the bare
        // metadata.name as the principal). Same string shape as SCRAM
        // but different rationale — kept as a distinct arm.
        Authentication::TlsExternal => format!("User:{name}"),
        // Delegation tokens carry the owner principal
        // (`User:<metadata.name>`); ACLs continue to be authored
        // against the owner, not the token-id.
        Authentication::DelegationToken(_) => format!("User:{name}"),
    }
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
#[tracing::instrument(level = "info", skip_all, fields(user = %obj.name_any()), err)]
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

#[tracing::instrument(level = "debug", skip_all, fields(user = %name), err)]
async fn add_finalizer(api: &Api<KafkaUser>, name: &str) -> Result<(), ReconcileError> {
    let patch = json!({ "metadata": { "finalizers": [FINALIZER] } });
    let params = PatchParams {
        field_manager: Some(FIELD_MANAGER.into()),
        ..Default::default()
    };
    api.patch(name, &params, &Patch::Merge(&patch)).await?;
    Ok(())
}

#[tracing::instrument(level = "info", skip_all, fields(user = %name), err)]
async fn remove_finalizer(api: &Api<KafkaUser>, name: &str) -> Result<(), ReconcileError> {
    let patch = json!({ "metadata": { "finalizers": [] } });
    let params = PatchParams {
        field_manager: Some(FIELD_MANAGER.into()),
        ..Default::default()
    };
    api.patch(name, &params, &Patch::Merge(&patch)).await?;
    Ok(())
}

// Five bools = "status flags" — each is an independent axis of the
// reconcile outcome. A state machine here would just rename the
// problem; keep the flat shape.
#[allow(clippy::struct_excessive_bools)]
struct StatusPatch<'a> {
    obj: &'a KafkaUser,
    status: &'a str,
    reason: &'a str,
    message: &'a str,
    scram_sha512: bool,
    /// `true` iff SCRAM-SHA-256 credentials are
    /// provisioned. Mirrors `scram_sha512`.
    scram_sha256: bool,
    tls: bool,
    /// True iff this is a credential-less (`tls-external`) user that
    /// has reached Ready. Sticky in `patch_status`.
    external: bool,
    tls_cert_not_after: Option<String>,
    tls_principal: Option<String>,
    advance_generation: bool,
    quotas_in_sync: bool,
}

#[tracing::instrument(level = "info", skip_all, fields(user = %name, status = %p.status, reason = %p.reason), err)]
async fn patch_status(
    api: &Api<KafkaUser>,
    name: &str,
    p: StatusPatch<'_>,
) -> Result<(), ReconcileError> {
    let conditions = vec![condition("Ready", p.status, p.reason, p.message)];
    let observed_generation = if p.advance_generation {
        p.obj.meta().generation
    } else {
        p.obj.status.as_ref().and_then(|s| s.observed_generation)
    };
    // Once any credential (SCRAM password or TLS cert) is provisioned,
    // the secret name == metadata.name.
    let secret_name = if p.scram_sha512 || p.scram_sha256 || p.tls {
        Some(name.to_string())
    } else {
        p.obj.status.as_ref().and_then(|s| s.secret.clone())
    };
    let body = json!({
        "status": {
            "conditions": conditions,
            "observedGeneration": observed_generation,
            "username": name,
            "secret": secret_name,
            "scramSha512": p.scram_sha512 || p.obj.status.as_ref().is_some_and(|s| s.scram_sha512),
            "scramSha256": p.scram_sha256 || p.obj.status.as_ref().is_some_and(|s| s.scram_sha256),
            "tls": p.tls || p.obj.status.as_ref().is_some_and(|s| s.tls),
            "external": p.external || p.obj.status.as_ref().is_some_and(|s| s.external),
            "tlsCertNotAfter": p.tls_cert_not_after,
            "tlsPrincipal": p.tls_principal,
            "quotasInSync": p.quotas_in_sync,
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
    use assert2::{assert, check};

    use super::*;
    use crate::crd::{
        AclOp, AclPatternType, AclPermission, AclResource, AclResourceKind, AclRule,
        KafkaUserSimpleAuthorization as SimpleAuthorization,
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
    fn principal_cases() {
        for (name, authentication, expected) in [
            (
                "SCRAM user",
                Authentication::ScramSha512(crate::crd::ScramSha512Auth::default()),
                "User:alice",
            ),
            (
                "TLS certificate user",
                Authentication::Tls(crate::crd::user::TlsAuth::default()),
                "User:CN=alice",
            ),
        ] {
            assert_eq!(
                principal_for("alice", &authentication),
                expected,
                "case {name}"
            );
        }
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
        assert!(
            entries
                == vec![AclEntry {
                    resource_type: ResourceType::Topic,
                    resource_name: "orders".into(),
                    pattern_type: PatternType::Literal,
                    principal: "User:alice".into(),
                    host: "*".into(),
                    operation: AclOperation::Read,
                    permission_type: PermissionType::Allow,
                }]
        );
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
        let ops: Vec<_> = entries.iter().map(|e| e.operation).collect();
        assert_eq!(
            ops,
            [
                AclOperation::Read,
                AclOperation::Describe,
                AclOperation::Write
            ]
        );
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
        assert_eq!((adds, dels), (vec![add], vec![drop]));
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
        assert_eq!((adds, dels), (vec![], vec![]));
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
        assert!(
            f == AclEntryFilter {
                resource_type: Some(ResourceType::Topic),
                resource_name: Some("orders".into()),
                pattern_type: Some(PatternType::Literal),
                principal: Some("User:alice".into()),
                host: Some("*".into()),
                operation: Some(AclOperation::Read),
                permission_type: Some(PermissionType::Allow),
            }
        );
    }

    #[test]
    fn random_password_is_base64_and_uniform_length() {
        let p1 = random_password(32);
        let p2 = random_password(32);
        check!(p1.len() == p2.len()); // base64 of 32 random bytes = 43 chars
        check!(p1 != p2);
        check!(
            p1.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
            "got: {p1}"
        );
    }

    #[test]
    fn principal_for_tls_external_uses_bare_name() {
        // `tls-external` users share SCRAM's bare-name principal shape:
        // credentials are managed out-of-band but the operator still
        // pins ACLs / quotas under `User:<metadata.name>`.
        assert!(principal_for("alice", &Authentication::TlsExternal) == "User:alice");
    }

    #[test]
    fn validate_spec_accepts_tls_external_with_no_authorization_and_no_quotas() {
        let spec = crate::crd::KafkaUserSpec {
            authentication: Authentication::TlsExternal,
            authorization: None,
            quotas: None,
        };
        assert!(validate_spec(&spec).is_ok());
    }

    #[test]
    fn validate_spec_accepts_tls_external_with_acls_and_quotas() {
        let spec = crate::crd::KafkaUserSpec {
            authentication: Authentication::TlsExternal,
            authorization: Some(Authorization::Simple(SimpleAuthorization {
                acls: vec![rule(AclResourceKind::Topic, "orders", &[AclOp::Read])],
            })),
            quotas: Some(crate::crd::KafkaUserQuotas {
                producer_byte_rate: Some(1_048_576),
                ..Default::default()
            }),
        };
        assert!(validate_spec(&spec).is_ok());
    }
}
