//! Reconcile arm for
//! `KafkaUser.spec.authentication.type: delegation-token`.
//!
//! This arm talks to the admin API of the cluster to keep one delegation
//! token for each user. The operator is a super-user and uses KIP-48
//! act-as. `User:<KafkaUser.metadata.name>` owns the token. The arm writes
//! the token credentials into a Kubernetes Secret, and it writes the token
//! lifetime and the condition into `KafkaUserStatus`.
//!
//! # Decisions
//!
//! `decide` is a pure function over `(spec, existing token, now)`. It
//! returns one of four `ReconcileDecision` arms. The reconcile loop
//! dispatches on the arm:
//!
//! - `Create`: there is no matching token. Call `CreateDelegationToken`
//!   with act-as.
//! - `NoOp`: the token is healthy and far from its expiry horizon.
//! - `Renew`: the token is inside `renew_before_expiry`. Call
//!   `RenewDelegationToken`.
//! - `Cycle`: the renewer set differs from the spec. Expire the old token
//!   and create a new one. `Renew` in KIP-48 cannot change the renewer
//!   set, so the operator must tombstone the old token.
//!
//! # I/O isolation
//!
//! The admin client surface is the `DelegationTokenAdmin` trait. The
//! Kubernetes I/O surface is the pair of traits `SecretWriter` and
//! `KafkaUserStatusWriter`. Unit tests substitute simple in-memory mocks.
//! Production wires the `kube::Api<Secret>` and `kube::Api<KafkaUser>`
//! adapters.

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use base64::Engine as _;
use crabka_client_admin::AdminError;
use crabka_metadata::DelegationToken;
use crabka_security::KafkaPrincipal;
use crabka_units::{Time, convert::TimeExt as _, hours};
use k8s_openapi::{
    ByteString,
    api::core::v1::Secret,
    apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference},
};
use kube::{
    Resource,
    api::{Api, Patch, PatchParams},
    runtime::controller::Action,
};
use serde_json::json;

use crate::{
    config::OperatorConfig,
    controller::common::{FIELD_MANAGER, ReconcileError, condition},
    crd::{DelegationTokenAuth, KafkaCondition, KafkaUser},
};

/// Default renewal lead time when the spec omits it.
pub(crate) const DEFAULT_RENEW_BEFORE_EXPIRY: Time = hours(24);

/// Broker error codes that the §2.5 table of the spec names.
const CODE_INVALID_REQUEST: i16 = 42;
const CODE_DELEGATION_TOKEN_AUTH_DISABLED: i16 = 61;
const CODE_DELEGATION_TOKEN_REQUEST_NOT_ALLOWED: i16 = 64;
const CODE_DELEGATION_TOKEN_AUTHORIZATION_FAILED: i16 = 65;

/// Output of [`decide`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReconcileDecision {
    /// There is no matching token. Issue a new one.
    Create,
    /// The token exists and is far from its expiry horizon. Do nothing.
    NoOp,
    /// The token exists, but `expiry_ts - now <= renew_before_expiry`.
    /// Renew it.
    Renew,
    /// The renewer set differs from the spec. Cycle the token: expire the
    /// old one and create a new one.
    Cycle,
}

/// Makes a pure decision over `(spec, existing token, now_ms)`.
///
/// The production reconcile loop and the unit tests both use it.
pub(crate) fn decide(
    auth: &DelegationTokenAuth,
    existing: Option<&DelegationToken>,
    now_ms: i64,
) -> ReconcileDecision {
    let Some(token) = existing else {
        return ReconcileDecision::Create;
    };
    let expected: BTreeSet<String> = auth.renewers.iter().cloned().collect();
    let actual: BTreeSet<String> = token
        .renewers
        .iter()
        .map(KafkaPrincipal::to_string)
        .collect();
    if expected != actual {
        return ReconcileDecision::Cycle;
    }
    let renew_before = auth
        .renew_before_expiry
        .unwrap_or(DEFAULT_RENEW_BEFORE_EXPIRY);
    if Time::from_millis(token.expiry_timestamp_ms - now_ms) <= renew_before {
        ReconcileDecision::Renew
    } else {
        ReconcileDecision::NoOp
    }
}

/// Admin-client surface that [`reconcile`] uses.
///
/// The production implementation lives in `crabka-client-admin`, task O3.
/// It proxies onto the 4 delegation-token methods of `AdminClient`. Unit
/// tests substitute an in-memory mock.
#[async_trait]
pub(crate) trait DelegationTokenAdmin: Send + Sync {
    /// Mints a token with KIP-48 act-as.
    ///
    /// The operator is a super-user. `owner_principal_name`, which is a
    /// `User:` principal, owns the new token. `renewers` is the list of
    /// `"User:<name>"` principal strings. A `max_lifetime` of `None` gives
    /// the broker value `delegation.token.max.lifetime.ms`.
    async fn create_delegation_token_as_owner(
        &self,
        owner_principal_name: &str,
        renewers: &[String],
        max_lifetime: Option<Time>,
    ) -> Result<DelegationToken, AdminError>;

    /// Extends the `expiry_timestamp_ms` of the token.
    ///
    /// The broker clamps the new value at `max_timestamp_ms`. A lifetime
    /// of `-1` gives the default renew period of the broker.
    async fn renew_delegation_token(&self, hmac: &[u8]) -> Result<DelegationToken, AdminError>;

    /// Tombstones the token immediately. A period of `-1` means
    /// expire-now.
    async fn expire_delegation_token(&self, hmac: &[u8]) -> Result<(), AdminError>;

    /// Runs `DescribeDelegationToken` for one owner principal string,
    /// for example `"User:alice"`.
    ///
    /// An empty vec means that there are no matching tokens.
    async fn describe_delegation_tokens_owned_by(
        &self,
        owner_principal: &str,
    ) -> Result<Vec<DelegationToken>, AdminError>;
}

/// Writes the token credentials of the user.
///
/// The production implementation wraps a server-side apply on
/// `kube::Api<Secret>`. Unit tests substitute an in-memory
/// `Mutex<Option<Secret>>`.
#[async_trait]
pub(crate) trait SecretWriter: Send + Sync {
    async fn apply(&self, secret: &Secret) -> Result<(), ReconcileError>;
}

/// Patches the status subresource of a `KafkaUser`.
///
/// The production implementation wraps a merge patch on
/// `kube::Api<KafkaUser>`. Unit tests record the JSON body.
#[async_trait]
pub(crate) trait KafkaUserStatusWriter: Send + Sync {
    async fn patch_status(&self, name: &str, body: serde_json::Value)
    -> Result<(), ReconcileError>;
}

/// Production `SecretWriter` over `kube::Api<Secret>`.
pub(crate) struct KubeSecretWriter {
    pub api: Api<Secret>,
}

#[async_trait]
impl SecretWriter for KubeSecretWriter {
    async fn apply(&self, secret: &Secret) -> Result<(), ReconcileError> {
        let name = secret.metadata.name.clone().unwrap_or_default();
        let params = PatchParams {
            field_manager: Some(FIELD_MANAGER.into()),
            force: true,
            ..Default::default()
        };
        self.api
            .patch(&name, &params, &Patch::Apply(secret))
            .await?;
        Ok(())
    }
}

/// Production `KafkaUserStatusWriter` over `kube::Api<KafkaUser>`.
pub(crate) struct KubeKafkaUserStatusWriter {
    pub api: Api<KafkaUser>,
}

#[async_trait]
impl KafkaUserStatusWriter for KubeKafkaUserStatusWriter {
    async fn patch_status(
        &self,
        name: &str,
        body: serde_json::Value,
    ) -> Result<(), ReconcileError> {
        let params = PatchParams {
            field_manager: Some(FIELD_MANAGER.into()),
            ..Default::default()
        };
        self.api
            .patch_status(name, &params, &Patch::Merge(&body))
            .await?;
        Ok(())
    }
}

/// Outcome of a single reconcile pass.
#[derive(Debug, Clone)]
pub(crate) struct ReconcileOutcome {
    pub action: Action,
    /// Token conditions carried into shared ACL and quota reconciliation.
    /// They reflect the last pending state when this pass changed credentials,
    /// or the stable Ready state for a healthy no-op. `None` means token
    /// reconciliation failed and the caller must return the supplied action.
    pub conditions: Option<Vec<KafkaCondition>>,
    /// Token-lifecycle deadline before the controller applies its shorter
    /// external-drift polling interval. Present only on success.
    pub token_requeue: Option<Time>,
    /// Whether this pass already published aggregate readiness as pending.
    /// The shared access phase uses this to avoid a redundant pending patch
    /// while still assigning a fresh transition time to the final Ready state.
    pub pending_published: bool,
}

/// Top-level reconcile entry point.
///
/// This function holds no concrete type with a single implementation. The
/// caller passes the admin client and the I/O writers in as trait
/// objects.
#[tracing::instrument(
    level = "info",
    skip_all,
    fields(user = %obj.metadata.name.clone().unwrap_or_default()),
    err,
)]
pub(crate) async fn reconcile(
    obj: &KafkaUser,
    auth: &DelegationTokenAuth,
    admin: &dyn DelegationTokenAdmin,
    secrets: &dyn SecretWriter,
    users: &dyn KafkaUserStatusWriter,
    now_ms: i64,
    config: &OperatorConfig,
) -> Result<ReconcileOutcome, ReconcileError> {
    let name = obj.metadata.name.clone().unwrap_or_default();
    let owner_principal = format!("User:{name}");

    // 1. Describe — find any token already owned by this user.
    let existing = match admin
        .describe_delegation_tokens_owned_by(&owner_principal)
        .await
    {
        Ok(v) => v,
        Err(e) => {
            let prior_conditions = existing_token_conditions(obj);
            return on_admin_error(
                &name,
                e,
                "DescribeDelegationToken",
                users,
                config,
                prior_conditions.as_deref(),
                prior_conditions.is_some(),
            )
            .await;
        }
    };
    // Prefer the persisted token_id from status; else fall back to the
    // first match (one-token-per-user is the operator's contract).
    let preferred_id = obj
        .status
        .as_ref()
        .and_then(|s| s.delegation_token_id.as_deref());
    let matching = preferred_id
        .and_then(|id| existing.iter().find(|t| t.token_id == id).cloned())
        .or_else(|| existing.first().cloned());

    let decision = decide(auth, matching.as_ref(), now_ms);
    let mut pending_conditions = None;

    // 2. Drive the decision. Each arm yields the live token + its
    //    expiry-driven requeue cadence.
    let (token, requeue): (DelegationToken, Time) = match decision {
        ReconcileDecision::Create => {
            let prior = obj
                .status
                .as_ref()
                .map_or(&[][..], |status| status.conditions.as_slice());
            pending_conditions = Some(publish_token_pending(&name, users, prior, false).await?);
            match issue_new_token(&name, auth, admin).await {
                Ok(t) => {
                    let r = compute_requeue(
                        &t,
                        auth,
                        now_ms,
                        config.delegation_token_min_requeue,
                        config.delegation_token_max_requeue,
                    );
                    (t, r)
                }
                Err(e) => {
                    return on_admin_error(
                        &name,
                        e,
                        "CreateDelegationToken",
                        users,
                        config,
                        pending_conditions.as_deref(),
                        false,
                    )
                    .await;
                }
            }
        }
        ReconcileDecision::NoOp => {
            let t = matching.expect("NoOp implies existing token");
            let r = compute_requeue(
                &t,
                auth,
                now_ms,
                config.delegation_token_min_requeue,
                config.delegation_token_max_requeue,
            );
            (t, r)
        }
        ReconcileDecision::Renew => {
            let existing_token = matching.expect("Renew implies existing token");
            match admin.renew_delegation_token(&existing_token.hmac).await {
                Ok(renewed) => {
                    let r = compute_requeue(
                        &renewed,
                        auth,
                        now_ms,
                        config.delegation_token_min_requeue,
                        config.delegation_token_max_requeue,
                    );
                    (renewed, r)
                }
                Err(e) => {
                    let live_conditions =
                        conditions_with_history(obj, &existing_token, auth, now_ms);
                    return on_admin_error(
                        &name,
                        e,
                        "RenewDelegationToken",
                        users,
                        config,
                        Some(&live_conditions),
                        true,
                    )
                    .await;
                }
            }
        }
        ReconcileDecision::Cycle => {
            let existing_token = matching.expect("Cycle implies existing token");
            // Expire the old, then create the new. KIP-48 has no
            // in-place renewer-set mutation API, so we must cycle.
            let live_conditions = conditions_with_history(obj, &existing_token, auth, now_ms);
            pending_conditions =
                Some(publish_token_pending(&name, users, &live_conditions, true).await?);
            if let Err(e) = admin.expire_delegation_token(&existing_token.hmac).await {
                return on_admin_error(
                    &name,
                    e,
                    "ExpireDelegationToken",
                    users,
                    config,
                    Some(&live_conditions),
                    true,
                )
                .await;
            }
            match issue_new_token(&name, auth, admin).await {
                Ok(t) => {
                    let r = compute_requeue(
                        &t,
                        auth,
                        now_ms,
                        config.delegation_token_min_requeue,
                        config.delegation_token_max_requeue,
                    );
                    (t, r)
                }
                Err(e) => {
                    return on_admin_error(
                        &name,
                        e,
                        "CreateDelegationToken",
                        users,
                        config,
                        pending_conditions.as_deref(),
                        false,
                    )
                    .await;
                }
            }
        }
    };

    let mut conditions = conditions_with_history(obj, &token, auth, now_ms);
    if pending_conditions.is_none() && !status_has_current_ready_token(obj, &token) {
        pending_conditions = Some(publish_token_pending(&name, users, &conditions, true).await?);
    }
    if let Some(published) = pending_conditions.as_ref() {
        conditions = inherit_published_ready(&conditions, published);
    }

    // 3. Persist the token identity before exposing its Secret. A cycle may
    // already have expired the previous token, so status must identify the
    // replacement even if the Secret write fails. Include pending conditions
    // atomically whenever this pass changed credentials.
    let identity_body = build_token_identity_patch(&token, pending_conditions.as_deref());
    users.patch_status(&name, identity_body).await?;

    // 4. Write the Secret with the live token credentials. Readiness has
    // already moved to pending whenever this token is new to the status.
    let secret = build_secret(obj, &token)?;
    if let Err(error) = secrets.apply(&secret).await {
        let failed = replace_ready_condition(
            pending_conditions.as_deref().unwrap_or(&conditions),
            "False",
            "SecretWriteFailed",
            &error.to_string(),
        );
        users
            .patch_status(&name, build_failure_status_patch(&failed))
            .await?;
        return Err(error);
    }

    Ok(ReconcileOutcome {
        action: Action::requeue(requeue.to_std()),
        conditions: Some(conditions),
        token_requeue: Some(requeue),
        pending_published: pending_conditions.is_some(),
    })
}

/// Runs the `Create` arm and issues a new token with act-as.
///
/// When the spec omits the lifetime field, the token gets the broker
/// ceiling.
async fn issue_new_token(
    name: &str,
    auth: &DelegationTokenAuth,
    admin: &dyn DelegationTokenAdmin,
) -> Result<DelegationToken, AdminError> {
    admin
        .create_delegation_token_as_owner(name, &auth.renewers, auth.max_lifetime)
        .await
}

/// Builds the per-user Secret that §2.3 of the spec defines.
///
/// The Secret has four keys. `token-id` holds the token id. `hmac` holds
/// the raw bytes. `password` holds the base64 form of the hmac, which the
/// user can paste directly. `sasl.jaas.config` holds the JAAS line that a
/// JVM client can put into `--producer.config`.
pub(crate) fn build_secret(
    obj: &KafkaUser,
    token: &DelegationToken,
) -> Result<Secret, ReconcileError> {
    let name = obj.metadata.name.clone().unwrap_or_default();
    let mut labels: BTreeMap<String, String> = BTreeMap::new();
    labels.insert("app.kubernetes.io/name".into(), "crabka-broker".into());
    labels.insert(
        "app.kubernetes.io/managed-by".into(),
        "crabka-operator".into(),
    );
    if let Some(cluster) = obj
        .metadata
        .labels
        .as_ref()
        .and_then(|l| l.get("crabka.io/cluster"))
    {
        labels.insert("crabka.io/cluster".into(), cluster.clone());
    }
    labels.insert("crabka.io/user".into(), name.clone());

    let data = build_secret_data(token);

    Ok(Secret {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: obj.metadata.namespace.clone(),
            labels: Some(labels),
            owner_references: Some(vec![user_owner_ref(obj)?]),
            ..Default::default()
        },
        type_: Some("Opaque".into()),
        data: Some(data),
        ..Default::default()
    })
}

/// Pure helper that builds the `data` map of the Secret.
pub(crate) fn build_secret_data(token: &DelegationToken) -> BTreeMap<String, ByteString> {
    let hmac_b64 = base64::engine::general_purpose::STANDARD.encode(&token.hmac);
    // KIP-48 client JAAS: SCRAM-SHA-256 mechanism, `tokenauth="true"`
    // flag tells the client to treat the password as a base64-of-HMAC.
    let jaas = format!(
        "org.apache.kafka.common.security.scram.ScramLoginModule required \
         username=\"{}\" password=\"{}\" tokenauth=\"true\";",
        token.token_id, hmac_b64,
    );

    let mut data = BTreeMap::new();
    data.insert(
        "token-id".into(),
        ByteString(token.token_id.clone().into_bytes()),
    );
    data.insert("hmac".into(), ByteString(token.hmac.clone()));
    data.insert("password".into(), ByteString(hmac_b64.clone().into_bytes()));
    data.insert("sasl.jaas.config".into(), ByteString(jaas.into_bytes()));
    data
}

fn user_owner_ref(obj: &KafkaUser) -> Result<OwnerReference, ReconcileError> {
    let uid = obj
        .metadata
        .uid
        .as_deref()
        .ok_or(ReconcileError::MissingUid)?;
    let name = obj.metadata.name.clone().unwrap_or_default();
    Ok(OwnerReference {
        api_version: <KafkaUser as Resource>::api_version(&()).to_string(),
        kind: <KafkaUser as Resource>::kind(&()).to_string(),
        name,
        uid: uid.to_string(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    })
}

/// Waits until about `renew_before_expiry` before the expiry, clamped to
/// the configured minimum and maximum requeue extents.
///
/// `expiry_timestamp_ms` and `now_ms` are instants. Their difference,
/// less the renewal lead time, is the extent.
pub(crate) fn compute_requeue(
    token: &DelegationToken,
    auth: &DelegationTokenAuth,
    now_ms: i64,
    min_requeue: Time,
    max_requeue: Time,
) -> Time {
    let renew_before = auth
        .renew_before_expiry
        .unwrap_or(DEFAULT_RENEW_BEFORE_EXPIRY);
    let until_expiry = Time::from_millis(token.expiry_timestamp_ms - now_ms);
    (until_expiry - renew_before)
        .max(min_requeue)
        .min(max_requeue)
}

/// Computes the `Ready`, `TokenIssued`, and `TokenExpiring` conditions.
///
/// - `TokenIssued = True` on the success path with reason `Issued`.
/// - `TokenIssued = False` on the error path with the reason from §2.5 of
///   the spec.
/// - `Ready = True` in the returned success conditions with reason
///   `TokenReady`. The token reconciler persists the same conditions with
///   `Ready=False/AccessPending`; its caller publishes the final Ready value
///   only after ACL and quota reconciliation succeeds.
/// - `Ready = False` on the error path with the same reason as
///   `TokenIssued`. `kubectl describe kafkauser` then shows the correlated
///   cause, and the reader does not have to compare two conditions.
/// - `TokenExpiring = True` when `expiry_ts - now < renew_before * 2`, and
///   `False` if not. The reason is `WithinRenewalHorizon` or `Healthy`.
pub(crate) fn compute_conditions(
    token: &DelegationToken,
    auth: &DelegationTokenAuth,
    now_ms: i64,
    issued_ok: bool,
    issued_reason: Option<(&str, &str)>,
) -> Vec<KafkaCondition> {
    let renew_before = auth
        .renew_before_expiry
        .unwrap_or(DEFAULT_RENEW_BEFORE_EXPIRY);
    let mut out = Vec::with_capacity(3);
    if issued_ok {
        out.push(condition(
            "Ready",
            "True",
            "TokenReady",
            "delegation token, Secret, ACLs, and quotas in sync",
        ));
        out.push(condition(
            "TokenIssued",
            "True",
            "Issued",
            "delegation token in sync",
        ));
    } else {
        let (reason, msg) = issued_reason.unwrap_or(("IssueFailed", "issue failed"));
        out.push(condition("Ready", "False", reason, msg));
        out.push(condition("TokenIssued", "False", reason, msg));
    }

    let expiring =
        Time::from_millis(token.expiry_timestamp_ms - now_ms) < renew_before + renew_before;
    if expiring {
        out.push(condition(
            "TokenExpiring",
            "True",
            "WithinRenewalHorizon",
            "expiry < 2× renewBeforeExpiry from now",
        ));
    } else {
        out.push(condition(
            "TokenExpiring",
            "False",
            "Healthy",
            "expiry comfortably outside renewal horizon",
        ));
    }
    out
}

/// Replaces the aggregate `Ready` condition while retaining token-specific
/// conditions and their transition timestamps.
pub(crate) fn replace_ready_condition(
    conditions: &[KafkaCondition],
    status: &str,
    reason: &str,
    message: &str,
) -> Vec<KafkaCondition> {
    replace_condition(conditions, "Ready", status, reason, message)
}

fn replace_condition(
    conditions: &[KafkaCondition],
    type_: &str,
    status: &str,
    reason: &str,
    message: &str,
) -> Vec<KafkaCondition> {
    let mut updated = conditions.to_vec();
    let mut replacement = condition(type_, status, reason, message);
    if let Some(existing) = updated.iter_mut().find(|item| item.type_ == type_) {
        if existing.status == replacement.status {
            replacement
                .last_transition_time
                .clone_from(&existing.last_transition_time);
        }
        *existing = replacement;
    } else if type_ == "Ready" {
        updated.insert(0, replacement);
    } else {
        updated.push(replacement);
    }
    updated
}

/// Replaces `Ready` after a known false-to-true transition. Unlike
/// [`replace_ready_condition`], this always assigns a new transition time.
pub(crate) fn transition_ready_condition(
    conditions: &[KafkaCondition],
    status: &str,
    reason: &str,
    message: &str,
) -> Vec<KafkaCondition> {
    let mut updated = conditions.to_vec();
    let ready = condition("Ready", status, reason, message);
    if let Some(existing) = updated.iter_mut().find(|item| item.type_ == "Ready") {
        *existing = ready;
    } else {
        updated.insert(0, ready);
    }
    updated
}

fn conditions_with_history(
    obj: &KafkaUser,
    token: &DelegationToken,
    auth: &DelegationTokenAuth,
    now_ms: i64,
) -> Vec<KafkaCondition> {
    let mut conditions = compute_conditions(token, auth, now_ms, true, None);
    let Some(status) = obj.status.as_ref() else {
        return conditions;
    };
    for current in &mut conditions {
        if current.type_ == "Ready" {
            if let Some(previous) = status
                .conditions
                .iter()
                .find(|previous| previous.type_ == "Ready")
            {
                *current = previous.clone();
            }
            continue;
        }
        if let Some(previous) = status
            .conditions
            .iter()
            .find(|previous| previous.type_ == current.type_ && previous.status == current.status)
        {
            current.last_transition_time = previous.last_transition_time.clone();
        }
    }
    conditions
}

fn inherit_published_ready(
    conditions: &[KafkaCondition],
    published: &[KafkaCondition],
) -> Vec<KafkaCondition> {
    let Some(ready) = published.iter().find(|item| item.type_ == "Ready") else {
        return conditions.to_vec();
    };
    let mut updated = conditions.to_vec();
    if let Some(current) = updated.iter_mut().find(|item| item.type_ == "Ready") {
        *current = ready.clone();
    } else {
        updated.insert(0, ready.clone());
    }
    updated
}

fn status_has_current_ready_token(obj: &KafkaUser, token: &DelegationToken) -> bool {
    let Some(status) = obj.status.as_ref() else {
        return false;
    };
    status.delegation_token_id.as_deref() == Some(token.token_id.as_str())
        && status.delegation_token_expiry_timestamp_ms == Some(token.expiry_timestamp_ms)
        && status.delegation_token_max_timestamp_ms == Some(token.max_timestamp_ms)
        && status.observed_generation == obj.metadata.generation
        && status.conditions.iter().any(|item| {
            item.type_ == "Ready" && item.status == "True" && item.reason == "TokenReady"
        })
        && status
            .conditions
            .iter()
            .any(|item| item.type_ == "TokenIssued" && item.status == "True")
}

async fn publish_token_pending(
    name: &str,
    users: &dyn KafkaUserStatusWriter,
    prior_conditions: &[KafkaCondition],
    token_live: bool,
) -> Result<Vec<KafkaCondition>, ReconcileError> {
    let mut conditions = replace_ready_condition(
        prior_conditions,
        "False",
        "TokenPending",
        "delegation-token credential and access reconciliation pending",
    );
    if !token_live {
        conditions = replace_condition(
            &conditions,
            "TokenIssued",
            "False",
            "TokenPending",
            "delegation token not yet issued",
        );
        conditions.retain(|item| item.type_ != "TokenExpiring");
    }
    users
        .patch_status(name, build_failure_status_patch(&conditions))
        .await?;
    Ok(conditions)
}

/// Returns the last token-specific conditions only when status proves a token
/// was previously issued. This lets transient Describe/connect failures mark
/// aggregate readiness false without claiming that the persisted token and
/// Secret disappeared.
pub(crate) fn existing_token_conditions(obj: &KafkaUser) -> Option<Vec<KafkaCondition>> {
    let status = obj.status.as_ref()?;
    status.delegation_token_id.as_ref()?;
    if !status
        .conditions
        .iter()
        .any(|item| item.type_ == "TokenIssued" && item.status == "True")
    {
        return None;
    }
    Some(status.conditions.clone())
}

/// Builds a narrow merge patch for live token identity. Conditions are
/// deliberately omitted so a healthy no-op reconcile does not flap Ready.
pub(crate) fn build_token_identity_patch(
    token: &DelegationToken,
    conditions: Option<&[KafkaCondition]>,
) -> serde_json::Value {
    let mut status = json!({
        "username": token.owner.name,
        "secret": token.owner.name,
        "delegationTokenId": token.token_id,
        "delegationTokenExpiryTimestampMs": token.expiry_timestamp_ms,
        "delegationTokenMaxTimestampMs": token.max_timestamp_ms,
    });
    if let Some(conditions) = conditions {
        status["conditions"] = json!(conditions);
    }
    json!({ "status": status })
}

/// Builds the merge-patch JSON body for a failure path.
///
/// The body patches only conditions; token identity fields remain intact when
/// a transient failure occurs after issuance.
pub(crate) fn build_failure_status_patch(conditions: &[KafkaCondition]) -> serde_json::Value {
    json!({
        "status": {
            "conditions": conditions,
        }
    })
}

/// Maps an `AdminError` to an `(Action, status patch)` pair, as §2.5 of
/// the spec defines.
///
/// - `0` is unreachable, because the success path does not call here.
/// - `42` `INVALID_REQUEST`        → `InvalidSpec`, with a long requeue and no automatic retry.
/// - `61` `AUTH_DISABLED`          → `BrokerAuthDisabled`, 5m backoff.
/// - `64` `REQUEST_NOT_ALLOWED`    → `OperatorTokenAuthed`, 5m backoff.
/// - `65` `AUTHORIZATION_FAILED`   → `OperatorNotSuperUser`, 5m backoff.
/// - other broker error            → `BrokerError`, 5m backoff.
/// - transport / connect / protocol → `Transport`, 5m backoff.
pub(crate) async fn on_admin_error(
    name: &str,
    err: AdminError,
    op: &'static str,
    users: &dyn KafkaUserStatusWriter,
    config: &OperatorConfig,
    prior_conditions: Option<&[KafkaCondition]>,
    token_live: bool,
) -> Result<ReconcileOutcome, ReconcileError> {
    let (reason, message, requeue): (&'static str, String, Time) = match &err {
        AdminError::Broker { code, .. } if *code == CODE_INVALID_REQUEST => (
            "InvalidSpec",
            format!("{op}: INVALID_REQUEST (42)"),
            config.delegation_token_invalid_requeue,
        ),
        AdminError::Broker { code, .. } if *code == CODE_DELEGATION_TOKEN_AUTH_DISABLED => (
            "BrokerAuthDisabled",
            format!("{op}: DELEGATION_TOKEN_AUTH_DISABLED (61)"),
            config.delegation_token_transient_backoff,
        ),
        AdminError::Broker { code, .. } if *code == CODE_DELEGATION_TOKEN_REQUEST_NOT_ALLOWED => (
            "OperatorTokenAuthed",
            format!("{op}: DELEGATION_TOKEN_REQUEST_NOT_ALLOWED (64)"),
            config.delegation_token_transient_backoff,
        ),
        AdminError::Broker { code, .. } if *code == CODE_DELEGATION_TOKEN_AUTHORIZATION_FAILED => (
            "OperatorNotSuperUser",
            format!("{op}: DELEGATION_TOKEN_AUTHORIZATION_FAILED (65)"),
            config.delegation_token_transient_backoff,
        ),
        AdminError::Broker {
            code,
            name: code_name,
            ..
        } => (
            "BrokerError",
            format!("{op}: {code_name} ({code})"),
            config.delegation_token_transient_backoff,
        ),
        other => (
            "Transport",
            format!("{op}: {other}"),
            config.delegation_token_transient_backoff,
        ),
    };

    let mut conds = replace_ready_condition(
        prior_conditions.unwrap_or_default(),
        "False",
        reason,
        &message,
    );
    // A transient failure must not claim that a previously issued token
    // vanished. Keep its issue and expiry-horizon conditions intact.
    if !token_live {
        conds = replace_condition(&conds, "TokenIssued", "False", reason, &message);
        conds.retain(|item| item.type_ != "TokenExpiring");
    }
    let body = build_failure_status_patch(&conds);
    users.patch_status(name, body).await?;
    Ok(ReconcileOutcome {
        action: Action::requeue(requeue.to_std()),
        conditions: None,
        token_requeue: None,
        pending_published: false,
    })
}

/// Expires the tokens that the user owns.
///
/// The finalizer arm in user.rs calls this function on a delete. The call
/// is best-effort. The caller logs an error, and an error never blocks the
/// removal of the finalizer.
pub(crate) async fn expire_owned_tokens(
    name: &str,
    admin: &dyn DelegationTokenAdmin,
) -> Result<(), AdminError> {
    let owner_principal = format!("User:{name}");
    let tokens = admin
        .describe_delegation_tokens_owned_by(&owner_principal)
        .await?;
    for t in tokens {
        admin.expire_delegation_token(&t.hmac).await?;
    }
    Ok(())
}

/// Production `DelegationTokenAdmin` implementation over the
/// `AdminClientHandle` of the operator.
///
/// `AdminClientHandle` is an `Arc<Mutex<dyn AdminClientLike + Send>>`.
/// Each method locks the inner mutex and calls one of the four
/// delegation-token methods of `AdminClientLike`.
///
/// The trait surface uses `&self`, so each call takes the mutex. The
/// SCRAM, ACL, and quota arms in `user.rs` also lock the same handle on
/// each admin RPC.
#[async_trait]
impl DelegationTokenAdmin for crate::context::AdminClientHandle {
    async fn create_delegation_token_as_owner(
        &self,
        owner_principal_name: &str,
        renewers: &[String],
        max_lifetime: Option<Time>,
    ) -> Result<DelegationToken, AdminError> {
        let mut admin = self.lock().await;
        admin
            .create_delegation_token_as_owner(owner_principal_name, renewers, max_lifetime)
            .await
    }

    async fn renew_delegation_token(&self, hmac: &[u8]) -> Result<DelegationToken, AdminError> {
        let mut admin = self.lock().await;
        admin.renew_delegation_token(hmac).await
    }

    async fn expire_delegation_token(&self, hmac: &[u8]) -> Result<(), AdminError> {
        let mut admin = self.lock().await;
        admin.expire_delegation_token(hmac).await
    }

    async fn describe_delegation_tokens_owned_by(
        &self,
        owner_principal: &str,
    ) -> Result<Vec<DelegationToken>, AdminError> {
        let mut admin = self.lock().await;
        admin
            .describe_delegation_tokens_owned_by(owner_principal)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;

    use assert2::{assert, check};
    use clap::Parser;
    use crabka_units::minutes;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    use super::*;
    use crate::{
        config::OperatorConfig,
        crd::{Authentication, KafkaUserSpec, KafkaUserStatus},
    };

    #[derive(Parser)]
    struct TestArgs {
        #[command(flatten)]
        config: OperatorConfig,
    }

    fn config() -> OperatorConfig {
        TestArgs::parse_from(["test"]).config
    }

    fn kp(t: &str, n: &str) -> KafkaPrincipal {
        KafkaPrincipal {
            principal_type: t.into(),
            name: n.into(),
        }
    }

    fn token_with(expiry: i64, renewers: Vec<KafkaPrincipal>) -> DelegationToken {
        DelegationToken {
            token_id: "t1".into(),
            owner: kp("User", "alice"),
            hmac: vec![0xAB; 32],
            issue_timestamp_ms: 0,
            expiry_timestamp_ms: expiry,
            max_timestamp_ms: expiry + 1_000_000,
            renewers,
        }
    }

    fn auth(renewers: Vec<&str>, renew_before: Option<Time>) -> DelegationTokenAuth {
        DelegationTokenAuth {
            renewers: renewers.into_iter().map(str::to_string).collect(),
            max_lifetime: None,
            renew_before_expiry: renew_before,
        }
    }

    // --- decide() unit tests ----------------------------------------------

    #[test]
    fn decide_create_when_no_token_exists() {
        assert!(decide(&auth(vec![], None), None, 0) == ReconcileDecision::Create);
    }

    #[test]
    fn decide_noop_when_expiry_far_from_now() {
        let t = token_with(1_000_000_000, vec![]);
        // Default 24h before-expiry; token expires far in future.
        assert!(decide(&auth(vec![], None), Some(&t), 0) == ReconcileDecision::NoOp);
    }

    #[test]
    fn decide_renew_when_inside_renew_threshold() {
        let t = token_with(1000, vec![]);
        // renew_before = 5000 > (1000 - 0). Renew.
        assert!(
            decide(
                &auth(vec![], Some(crabka_units::millis(5_000))),
                Some(&t),
                0
            ) == ReconcileDecision::Renew
        );
    }

    #[test]
    fn decide_cycle_when_renewers_diverge() {
        let t = token_with(1_000_000_000, vec![kp("User", "bob")]);
        // Spec adds carol.
        assert!(
            decide(&auth(vec!["User:bob", "User:carol"], None), Some(&t), 0)
                == ReconcileDecision::Cycle
        );
    }

    #[test]
    fn decide_renew_when_default_threshold_just_met() {
        // Token expires in exactly 24h; default renew_before = 24h. Renew
        // (boundary is inclusive: <= triggers).
        let t = token_with(24 * 60 * 60 * 1_000, vec![]);
        assert!(decide(&auth(vec![], None), Some(&t), 0) == ReconcileDecision::Renew);
    }

    // --- Mock admin client + writers --------------------------------------

    /// One recorded call against the mock admin client.
    ///
    /// Tests assert on this to confirm that the reconciler called the
    /// correct RPC with the correct shape. The type is not `Eq`, because
    /// `Create` carries a `Time`, and the `f64` storage of `Time` is only
    /// `PartialEq`.
    #[derive(Debug, Clone, PartialEq)]
    enum MockCall {
        Create {
            owner: String,
            renewers: Vec<String>,
            max_lifetime: Option<Time>,
        },
        Renew {
            hmac: Vec<u8>,
        },
        Expire {
            hmac: Vec<u8>,
        },
        Describe {
            owner: String,
        },
    }

    #[derive(Default)]
    struct MockDelegationTokenAdmin {
        tokens: StdMutex<Vec<DelegationToken>>,
        calls: StdMutex<Vec<MockCall>>,
        /// When set, the next `create` extends `expiry_timestamp_ms` by
        /// this delta over `now`. The tests set the issue timestamp to
        /// control it.
        create_expiry_offset_ms: i64,
        /// When set, the next `renew` adds this delta to the expiry of
        /// the existing token, with a cap at `max_timestamp_ms`.
        renew_delta_ms: i64,
        /// Optional canned error code that every RPC returns.
        force_broker_error: Option<i16>,
    }

    impl MockDelegationTokenAdmin {
        fn new() -> Self {
            Self {
                create_expiry_offset_ms: 7 * 24 * 60 * 60 * 1_000, // 7d
                renew_delta_ms: 24 * 60 * 60 * 1_000,              // +1d
                ..Default::default()
            }
        }

        fn calls(&self) -> Vec<MockCall> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl DelegationTokenAdmin for MockDelegationTokenAdmin {
        async fn create_delegation_token_as_owner(
            &self,
            owner_principal_name: &str,
            renewers: &[String],
            max_lifetime: Option<Time>,
        ) -> Result<DelegationToken, AdminError> {
            self.calls.lock().unwrap().push(MockCall::Create {
                owner: owner_principal_name.into(),
                renewers: renewers.to_vec(),
                max_lifetime,
            });
            if let Some(code) = self.force_broker_error {
                return Err(AdminError::Broker {
                    api: "CreateDelegationToken",
                    code,
                    name: "FORCED",
                    message: None,
                });
            }
            let now: i64 = 1_000_000; // arbitrary mock "now"
            let token = DelegationToken {
                token_id: format!("tok-{}", self.tokens.lock().unwrap().len()),
                owner: kp("User", owner_principal_name),
                hmac: vec![0xCD; 32],
                issue_timestamp_ms: now,
                expiry_timestamp_ms: now + self.create_expiry_offset_ms,
                max_timestamp_ms: now + self.create_expiry_offset_ms + 1_000_000,
                renewers: renewers.iter().filter_map(|s| s.parse().ok()).collect(),
            };
            self.tokens.lock().unwrap().push(token.clone());
            Ok(token)
        }

        async fn renew_delegation_token(&self, hmac: &[u8]) -> Result<DelegationToken, AdminError> {
            self.calls.lock().unwrap().push(MockCall::Renew {
                hmac: hmac.to_vec(),
            });
            if let Some(code) = self.force_broker_error {
                return Err(AdminError::Broker {
                    api: "RenewDelegationToken",
                    code,
                    name: "FORCED",
                    message: None,
                });
            }
            let mut guard = self.tokens.lock().unwrap();
            let pos = guard
                .iter()
                .position(|t| t.hmac == hmac)
                .ok_or(AdminError::Protocol("hmac not found".into()))?;
            let max = guard[pos].max_timestamp_ms;
            guard[pos].expiry_timestamp_ms =
                (guard[pos].expiry_timestamp_ms + self.renew_delta_ms).min(max);
            Ok(guard[pos].clone())
        }

        async fn expire_delegation_token(&self, hmac: &[u8]) -> Result<(), AdminError> {
            self.calls.lock().unwrap().push(MockCall::Expire {
                hmac: hmac.to_vec(),
            });
            if let Some(code) = self.force_broker_error {
                return Err(AdminError::Broker {
                    api: "ExpireDelegationToken",
                    code,
                    name: "FORCED",
                    message: None,
                });
            }
            let mut guard = self.tokens.lock().unwrap();
            guard.retain(|t| t.hmac != hmac);
            Ok(())
        }

        async fn describe_delegation_tokens_owned_by(
            &self,
            owner_principal: &str,
        ) -> Result<Vec<DelegationToken>, AdminError> {
            self.calls.lock().unwrap().push(MockCall::Describe {
                owner: owner_principal.into(),
            });
            if let Some(code) = self.force_broker_error {
                return Err(AdminError::Broker {
                    api: "DescribeDelegationToken",
                    code,
                    name: "FORCED",
                    message: None,
                });
            }
            let want: KafkaPrincipal = owner_principal
                .parse()
                .map_err(|e: String| AdminError::Protocol(e))?;
            let guard = self.tokens.lock().unwrap();
            Ok(guard.iter().filter(|t| t.owner == want).cloned().collect())
        }
    }

    #[derive(Default)]
    struct RecordingSecretWriter {
        applied: StdMutex<Vec<Secret>>,
    }

    #[async_trait]
    impl SecretWriter for RecordingSecretWriter {
        async fn apply(&self, secret: &Secret) -> Result<(), ReconcileError> {
            self.applied.lock().unwrap().push(secret.clone());
            Ok(())
        }
    }

    struct FailingSecretWriter;

    #[async_trait]
    impl SecretWriter for FailingSecretWriter {
        async fn apply(&self, _secret: &Secret) -> Result<(), ReconcileError> {
            Err(ReconcileError::Malformed(
                "forced Secret write failure".into(),
            ))
        }
    }

    #[derive(Default)]
    struct RecordingStatusWriter {
        patches: StdMutex<Vec<(String, serde_json::Value)>>,
    }

    #[async_trait]
    impl KafkaUserStatusWriter for RecordingStatusWriter {
        async fn patch_status(
            &self,
            name: &str,
            body: serde_json::Value,
        ) -> Result<(), ReconcileError> {
            self.patches.lock().unwrap().push((name.into(), body));
            Ok(())
        }
    }

    fn user(name: &str, auth: DelegationTokenAuth) -> KafkaUser {
        KafkaUser {
            metadata: ObjectMeta {
                name: Some(name.into()),
                namespace: Some("kafka".into()),
                uid: Some("00000000-0000-0000-0000-000000000001".into()),
                ..Default::default()
            },
            spec: KafkaUserSpec {
                authentication: Authentication::DelegationToken(auth),
                authorization: None,
                quotas: None,
            },
            status: None,
        }
    }

    // --- reconcile-level tests --------------------------------------------

    #[tokio::test]
    async fn reconcile_creates_token_writes_secret_when_no_token_exists() {
        let admin = MockDelegationTokenAdmin::new();
        let secrets = RecordingSecretWriter::default();
        let users = RecordingStatusWriter::default();

        let auth_cfg = DelegationTokenAuth {
            renewers: vec!["User:bob".into()],
            max_lifetime: Some(hours(24)),
            renew_before_expiry: None,
        };
        let obj = user("alice", auth_cfg.clone());
        let config = config();

        let out = reconcile(&obj, &auth_cfg, &admin, &secrets, &users, 0, &config)
            .await
            .expect("reconcile should succeed");
        // Action is requeue (we don't inspect the exact duration; that's
        // compute_requeue's job, separately covered).
        let _ = &out.action;

        // Admin calls: Describe (empty result) → Create.
        let calls = admin.calls();
        assert!(
            calls
                == vec![
                    MockCall::Describe {
                        owner: "User:alice".into(),
                    },
                    MockCall::Create {
                        owner: "alice".into(),
                        renewers: vec!["User:bob".into()],
                        max_lifetime: Some(hours(24)),
                    },
                ],
            "expected Describe+Create, got: {calls:?}",
        );

        // Secret applied with the expected keys.
        let applied = secrets.applied.lock().unwrap();
        assert!(applied.len() == 1);
        let data = applied[0].data.as_ref().expect("data set");
        for key in ["token-id", "hmac", "password", "sasl.jaas.config"] {
            assert!(data.contains_key(key), "missing key {key:?}");
        }
        let jaas = std::str::from_utf8(&data["sasl.jaas.config"].0).unwrap();
        assert!(jaas.contains("tokenauth=\"true\""), "jaas: {jaas}");
        assert!(jaas.contains("ScramLoginModule"), "jaas: {jaas}");

        // Readiness moves to pending before token creation; the second patch
        // persists identity without overwriting those conditions.
        let patches = users.patches.lock().unwrap();
        assert!(patches.len() == 2);
        let pending = patches[0].1["status"]["conditions"]
            .as_array()
            .expect("pending conditions");
        assert!(pending.iter().any(|condition| condition["type"] == "Ready"
            && condition["status"] == "False"
            && condition["reason"] == "TokenPending"));
        let (name, body) = &patches[1];
        assert!(name == "alice");
        let status = body.get("status").unwrap();
        assert!(status.get("delegationTokenId").is_some());
        assert!(status.get("delegationTokenExpiryTimestampMs").is_some());
        assert!(
            status["conditions"]
                .as_array()
                .expect("identity pending conditions")
                .iter()
                .any(|condition| condition["type"] == "Ready"
                    && condition["status"] == "False"
                    && condition["reason"] == "TokenPending")
        );
        let ready_conditions = out
            .conditions
            .as_ref()
            .expect("successful token reconcile returns final conditions");
        assert!(
            ready_conditions
                .iter()
                .any(|condition| condition.type_ == "TokenIssued" && condition.status == "True")
        );
        assert!(
            ready_conditions
                .iter()
                .any(|condition| condition.type_ == "Ready"
                    && condition.status == "False"
                    && condition.reason == "TokenPending")
        );
    }

    #[tokio::test]
    async fn reconcile_renews_token_when_inside_horizon() {
        let admin = MockDelegationTokenAdmin::new();
        let secrets = RecordingSecretWriter::default();
        let users = RecordingStatusWriter::default();

        // Pre-seed an existing token expiring in 10s.
        let existing = DelegationToken {
            token_id: "preexisting".into(),
            owner: kp("User", "alice"),
            hmac: vec![0xEE; 32],
            issue_timestamp_ms: 0,
            expiry_timestamp_ms: 10_000,
            max_timestamp_ms: 1_000_000_000,
            renewers: vec![],
        };
        admin.tokens.lock().unwrap().push(existing.clone());

        // Spec's renew_before is 60s — token (10s away) is inside.
        let auth_cfg = DelegationTokenAuth {
            renewers: vec![],
            max_lifetime: None,
            renew_before_expiry: Some(minutes(1)),
        };
        let obj = user("alice", auth_cfg.clone());
        let config = config();

        let _ = reconcile(&obj, &auth_cfg, &admin, &secrets, &users, 0, &config)
            .await
            .unwrap();

        let calls = admin.calls();
        assert!(
            calls
                == vec![
                    MockCall::Describe {
                        owner: "User:alice".into(),
                    },
                    MockCall::Renew {
                        hmac: vec![0xEE; 32],
                    },
                ],
            "expected Describe+Renew on the existing hmac, got: {calls:?}",
        );

        // Secret plus pending and identity status patches.
        check!(secrets.applied.lock().unwrap().len() == 1);
        check!(users.patches.lock().unwrap().len() == 2);
    }

    #[tokio::test]
    async fn healthy_noop_preserves_condition_transition_times() {
        let admin = MockDelegationTokenAdmin::new();
        let token = token_with(1_000_000_000, vec![]);
        admin.tokens.lock().unwrap().push(token.clone());
        let secrets = RecordingSecretWriter::default();
        let users = RecordingStatusWriter::default();
        let auth_cfg = DelegationTokenAuth::default();
        let mut obj = user("alice", auth_cfg.clone());
        obj.status = Some(KafkaUserStatus {
            observed_generation: obj.metadata.generation,
            delegation_token_id: Some(token.token_id.clone()),
            delegation_token_expiry_timestamp_ms: Some(token.expiry_timestamp_ms),
            delegation_token_max_timestamp_ms: Some(token.max_timestamp_ms),
            conditions: vec![
                KafkaCondition {
                    type_: "Ready".into(),
                    status: "True".into(),
                    reason: "TokenReady".into(),
                    message: "delegation token, Secret, ACLs, and quotas in sync".into(),
                    last_transition_time: "2026-01-01T00:00:00Z".into(),
                },
                KafkaCondition {
                    type_: "TokenIssued".into(),
                    status: "True".into(),
                    reason: "Issued".into(),
                    message: "delegation token in sync".into(),
                    last_transition_time: "2026-01-02T00:00:00Z".into(),
                },
                KafkaCondition {
                    type_: "TokenExpiring".into(),
                    status: "False".into(),
                    reason: "Healthy".into(),
                    message: "expiry comfortably outside renewal horizon".into(),
                    last_transition_time: "2026-01-03T00:00:00Z".into(),
                },
            ],
            ..Default::default()
        });

        let out = reconcile(&obj, &auth_cfg, &admin, &secrets, &users, 0, &config())
            .await
            .unwrap();

        assert!(!out.pending_published);
        assert!(users.patches.lock().unwrap().len() == 1);
        let conditions = out.conditions.expect("successful no-op conditions");
        assert!(conditions[0].last_transition_time == "2026-01-01T00:00:00Z");
        assert!(conditions[1].last_transition_time == "2026-01-02T00:00:00Z");
        assert!(conditions[2].last_transition_time == "2026-01-03T00:00:00Z");
    }

    #[tokio::test]
    async fn cycle_marks_not_ready_before_secret_failure() {
        let admin = MockDelegationTokenAdmin::new();
        let old = token_with(1_000_000_000, vec![kp("User", "old-renewer")]);
        admin.tokens.lock().unwrap().push(old.clone());
        let users = RecordingStatusWriter::default();
        let auth_cfg = auth(vec!["User:new-renewer"], None);
        let mut obj = user("alice", auth_cfg.clone());
        obj.status = Some(KafkaUserStatus {
            delegation_token_id: Some(old.token_id.clone()),
            delegation_token_expiry_timestamp_ms: Some(old.expiry_timestamp_ms),
            delegation_token_max_timestamp_ms: Some(old.max_timestamp_ms),
            conditions: vec![
                condition("Ready", "True", "TokenReady", "previously reconciled"),
                condition("TokenIssued", "True", "Issued", "token exists"),
                condition("TokenExpiring", "False", "Healthy", "token healthy"),
            ],
            ..Default::default()
        });

        let error = reconcile(
            &obj,
            &auth_cfg,
            &admin,
            &FailingSecretWriter,
            &users,
            0,
            &config(),
        )
        .await
        .expect_err("forced Secret failure must escape");
        assert!(error.to_string().contains("forced Secret write failure"));

        let calls = admin.calls();
        assert!(matches!(
            calls.as_slice(),
            [
                MockCall::Describe { .. },
                MockCall::Expire { .. },
                MockCall::Create { .. },
            ]
        ));
        let patches = users.patches.lock().unwrap();
        assert!(patches.len() == 3);
        let pending_conditions = patches[0].1["status"]["conditions"]
            .as_array()
            .expect("pending conditions");
        assert!(
            pending_conditions
                .iter()
                .any(|condition| condition["type"] == "Ready"
                    && condition["status"] == "False"
                    && condition["reason"] == "TokenPending")
        );
        assert!(
            pending_conditions.iter().any(
                |condition| condition["type"] == "TokenIssued" && condition["status"] == "True"
            )
        );
        let identity = &patches[1].1["status"];
        let replacement_id = identity["delegationTokenId"]
            .as_str()
            .expect("replacement token id");
        assert!(replacement_id != old.token_id);
        assert!(
            identity["conditions"]
                .as_array()
                .expect("identity pending conditions")
                .iter()
                .any(|condition| condition["type"] == "Ready" && condition["status"] == "False")
        );
        let failure_conditions = patches[2].1["status"]["conditions"]
            .as_array()
            .expect("Secret failure conditions");
        assert!(
            failure_conditions
                .iter()
                .any(|condition| condition["type"] == "Ready"
                    && condition["status"] == "False"
                    && condition["reason"] == "SecretWriteFailed")
        );
        assert!(
            failure_conditions.iter().any(
                |condition| condition["type"] == "TokenIssued" && condition["status"] == "True"
            )
        );
    }

    // --- failure-path coverage (§2.5) ------------------------------------

    #[tokio::test]
    async fn reconcile_maps_authorization_failed_to_operator_not_super_user() {
        let mut admin = MockDelegationTokenAdmin::new();
        admin.force_broker_error = Some(CODE_DELEGATION_TOKEN_AUTHORIZATION_FAILED);
        let secrets = RecordingSecretWriter::default();
        let users = RecordingStatusWriter::default();
        let auth_cfg = DelegationTokenAuth::default();
        let obj = user("alice", auth_cfg.clone());
        let mut config = config();
        config.delegation_token_transient_backoff = crabka_units::millis(1_234);

        let out = reconcile(&obj, &auth_cfg, &admin, &secrets, &users, 0, &config)
            .await
            .unwrap();
        assert!(out.action == Action::requeue(core::time::Duration::from_millis(1_234)));

        // No Secret was applied — failure path skips the write.
        assert!(secrets.applied.lock().unwrap().is_empty());

        let patches = users.patches.lock().unwrap();
        assert!(patches.len() == 1);
        let conds = patches[0].1["status"]["conditions"].as_array().unwrap();
        let issued = conds
            .iter()
            .find(|c| c["type"] == "TokenIssued")
            .expect("TokenIssued present");
        assert!(issued["status"] == "False");
        assert!(issued["reason"] == "OperatorNotSuperUser");
        // Ready mirrors TokenIssued on the failure path — same reason
        // string so `kubectl describe` shows the correlated cause.
        let ready = conds
            .iter()
            .find(|c| c["type"] == "Ready")
            .expect("Ready present");
        assert!(ready["status"] == "False");
        assert!(ready["reason"] == "OperatorNotSuperUser");
    }

    #[tokio::test]
    async fn describe_failure_preserves_existing_token_conditions() {
        let mut admin = MockDelegationTokenAdmin::new();
        admin.force_broker_error = Some(CODE_DELEGATION_TOKEN_AUTHORIZATION_FAILED);
        let secrets = RecordingSecretWriter::default();
        let users = RecordingStatusWriter::default();
        let auth_cfg = DelegationTokenAuth::default();
        let mut obj = user("alice", auth_cfg.clone());
        obj.status = Some(KafkaUserStatus {
            delegation_token_id: Some("persisted-token".into()),
            conditions: vec![
                condition("Ready", "True", "TokenReady", "previously reconciled"),
                condition("TokenIssued", "True", "Issued", "token exists"),
                condition("TokenExpiring", "False", "Healthy", "token healthy"),
            ],
            ..Default::default()
        });

        reconcile(&obj, &auth_cfg, &admin, &secrets, &users, 0, &config())
            .await
            .unwrap();

        let patches = users.patches.lock().unwrap();
        let conditions = patches[0].1["status"]["conditions"]
            .as_array()
            .expect("conditions array");
        assert!(conditions.iter().any(|item| item["type"] == "Ready"
            && item["status"] == "False"
            && item["reason"] == "OperatorNotSuperUser"));
        assert!(
            conditions
                .iter()
                .any(|item| item["type"] == "TokenIssued" && item["status"] == "True")
        );
        assert!(
            conditions
                .iter()
                .any(|item| item["type"] == "TokenExpiring"),
            "transient lifecycle failure must retain token horizon status",
        );
    }

    #[tokio::test]
    async fn reconcile_maps_invalid_request_to_invalid_spec() {
        let mut admin = MockDelegationTokenAdmin::new();
        admin.force_broker_error = Some(CODE_INVALID_REQUEST);
        let secrets = RecordingSecretWriter::default();
        let users = RecordingStatusWriter::default();
        let auth_cfg = DelegationTokenAuth::default();
        let obj = user("alice", auth_cfg.clone());
        let mut config = config();
        config.delegation_token_invalid_requeue = crabka_units::millis(2_345);

        let out = reconcile(&obj, &auth_cfg, &admin, &secrets, &users, 0, &config)
            .await
            .unwrap();
        assert!(out.action == Action::requeue(core::time::Duration::from_millis(2_345)));

        let patches = users.patches.lock().unwrap();
        let conds = patches[0].1["status"]["conditions"].as_array().unwrap();
        let issued = conds.iter().find(|c| c["type"] == "TokenIssued").unwrap();
        assert!(issued["status"] == "False");
        assert!(issued["reason"] == "InvalidSpec");
        let ready = conds.iter().find(|c| c["type"] == "Ready").unwrap();
        assert!(ready["status"] == "False");
        assert!(ready["reason"] == "InvalidSpec");
    }

    // --- helpers ----------------------------------------------------------

    #[test]
    fn build_secret_data_emits_all_four_keys() {
        let t = token_with(0, vec![]);
        let data = build_secret_data(&t);
        assert!(data.len() == 4);
        // password is base64(hmac), bytes of the b64 string.
        let want_b64 = base64::engine::general_purpose::STANDARD.encode(&t.hmac);
        for (key, want) in [
            ("token-id", t.token_id.as_bytes().to_vec()),
            ("hmac", t.hmac.clone()),
            ("password", want_b64.as_bytes().to_vec()),
        ] {
            assert!(data[key].0 == want, "key {key:?}");
        }
        let jaas = std::str::from_utf8(&data["sasl.jaas.config"].0).unwrap();
        for want in [t.token_id.as_str(), want_b64.as_str(), "tokenauth=\"true\""] {
            assert!(jaas.contains(want), "jaas missing {want:?}: {jaas}");
        }
    }

    #[test]
    fn compute_requeue_clamps_to_one_minute_minimum() {
        // Token expires "now" with renew_before unset — without a clamp
        // we'd compute a zero extent and hot-loop the reconciler.
        let t = token_with(0, vec![]);
        let r = compute_requeue(&t, &auth(vec![], None), 0, crabka_units::secs(7), hours(24));
        assert!(r == crabka_units::secs(7));
    }

    #[test]
    fn compute_requeue_clamps_to_24h_maximum() {
        // Token expires in a year; without clamp we'd requeue weeks out.
        let t = token_with(365 * 24 * 60 * 60 * 1_000, vec![]);
        let r = compute_requeue(&t, &auth(vec![], None), 0, minutes(1), hours(3));
        assert!(r == hours(3));
    }

    #[test]
    fn compute_requeue_lands_between_the_clamps() {
        // Expiry 3h out, renewal lead 1h -> re-check in 2h.
        let t = token_with(3 * 60 * 60 * 1_000, vec![]);
        let r = compute_requeue(&t, &auth(vec![], Some(hours(1))), 0, minutes(1), hours(24));
        assert!(r == hours(2));
    }

    #[test]
    fn compute_conditions_token_expiring_true_when_close_to_horizon() {
        // expiry - now (1500) < renew_before (1000) * 2 → TokenExpiring=True
        let t = token_with(1500, vec![]);
        let conds = compute_conditions(
            &t,
            &auth(vec![], Some(crabka_units::secs(1))),
            0,
            true,
            None,
        );
        let expiring = conds.iter().find(|c| c.type_ == "TokenExpiring").unwrap();
        assert!(expiring.status == "True");
        assert!(expiring.reason == "WithinRenewalHorizon");
        // Success path also emits Ready=True/TokenReady (spec §2.4).
        let ready = conds.iter().find(|c| c.type_ == "Ready").unwrap();
        assert!(ready.status == "True");
        assert!(ready.reason == "TokenReady");
    }

    #[test]
    fn compute_conditions_token_expiring_false_when_far() {
        // expiry - now (5000) > renew_before (1000) * 2 → TokenExpiring=False
        let t = token_with(5000, vec![]);
        let conds = compute_conditions(
            &t,
            &auth(vec![], Some(crabka_units::secs(1))),
            0,
            true,
            None,
        );
        let expiring = conds.iter().find(|c| c.type_ == "TokenExpiring").unwrap();
        assert!(expiring.status == "False");
        assert!(expiring.reason == "Healthy");
        let ready = conds.iter().find(|c| c.type_ == "Ready").unwrap();
        assert!(ready.status == "True");
        assert!(ready.reason == "TokenReady");
    }
}
