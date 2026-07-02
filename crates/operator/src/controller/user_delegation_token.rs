//! Reconcile arm for
//! `KafkaUser.spec.authentication.type: delegation-token`.
//!
//! Talks to the cluster's admin API (the operator is a super-user and
//! uses KIP-48 act-as) to maintain one delegation token per user, owned
//! by `User:<KafkaUser.metadata.name>`. Persists the token credentials
//! into a Kubernetes Secret + reflects token lifetime + condition into
//! `KafkaUserStatus`.
//!
//! # Decisions
//!
//! `decide` is a pure function over `(spec, existing token, now)` and
//! returns one of four `ReconcileDecision` arms. The reconcile loop
//! dispatches per arm:
//!
//! - `Create` — no matching token; call `CreateDelegationToken` (act-as).
//! - `NoOp`   — token healthy and well within its expiry horizon.
//! - `Renew`  — token within `renew_before_expiry_ms`; call `RenewDelegationToken`.
//! - `Cycle`  — renewer set diverged from spec; expire + create (KIP-48's
//!   `Renew` cannot mutate the renewer set, so the old token must be
//!   tombstoned).
//!
//! # I/O isolation
//!
//! The admin client surface is the `DelegationTokenAdmin` trait. The
//! Kubernetes I/O surface is the `SecretWriter` + `KafkaUserStatusWriter`
//! trait pair. Unit tests substitute trivial in-memory mocks; production
//! wires `kube::Api<Secret>` / `kube::Api<KafkaUser>` adapters.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine as _;
use crabka_client_admin::AdminError;
use crabka_metadata::DelegationToken;
use crabka_security::KafkaPrincipal;
use k8s_openapi::ByteString;
use k8s_openapi::api::core::v1::Secret;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use kube::Resource;
use kube::api::{Api, Patch, PatchParams};
use kube::runtime::controller::Action;
use serde_json::json;

use crate::controller::common::{FIELD_MANAGER, ReconcileError, condition};
use crate::crd::{DelegationTokenAuth, KafkaCondition, KafkaUser};

/// Default `renew_before_expiry_ms` when the spec omits it (24h).
pub(crate) const DEFAULT_RENEW_BEFORE_EXPIRY_MS: i64 = 24 * 60 * 60 * 1_000;

/// Transient broker errors trigger a 5-minute requeue backoff per spec
/// §2.5 (`AUTH_DISABLED`, `AUTHORIZATION_FAILED`, `REQUEST_NOT_ALLOWED`).
const TRANSIENT_BACKOFF: Duration = Duration::from_mins(5);

/// Broker error codes the spec §2.5 table calls out.
const CODE_INVALID_REQUEST: i16 = 42;
const CODE_DELEGATION_TOKEN_AUTH_DISABLED: i16 = 61;
const CODE_DELEGATION_TOKEN_REQUEST_NOT_ALLOWED: i16 = 64;
const CODE_DELEGATION_TOKEN_AUTHORIZATION_FAILED: i16 = 65;

/// Output of [`decide`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReconcileDecision {
    /// No matching token; issue a fresh one.
    Create,
    /// Token exists and is well within its expiry horizon. No-op.
    NoOp,
    /// Token exists but `expiry_ts - now <= renew_before_expiry_ms` — renew.
    Renew,
    /// Renewer set has diverged from spec; cycle (expire old + create new).
    Cycle,
}

/// Pure decision over `(spec, existing token, now_ms)`. Used by both the
/// production reconcile loop and the unit tests.
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
        .renew_before_expiry_ms
        .unwrap_or(DEFAULT_RENEW_BEFORE_EXPIRY_MS);
    if token.expiry_timestamp_ms - now_ms <= renew_before {
        ReconcileDecision::Renew
    } else {
        ReconcileDecision::NoOp
    }
}

/// Admin-client surface used by [`reconcile`]. The production impl lives
/// in `crabka-client-admin` (task O3) and proxies onto `AdminClient`'s
/// 4 delegation-token methods. Unit tests substitute an in-memory mock.
#[async_trait]
pub(crate) trait DelegationTokenAdmin: Send + Sync {
    /// KIP-48 act-as: the operator (a super-user) mints a token owned by
    /// `owner_principal_name` (a `User:` principal). `renewers` is the
    /// list of `"User:<name>"` principal strings. `max_lifetime_ms` of
    /// `-1` defers to the broker's `delegation.token.max.lifetime.ms`.
    async fn create_delegation_token_as_owner(
        &self,
        owner_principal_name: &str,
        renewers: &[String],
        max_lifetime_ms: i64,
    ) -> Result<DelegationToken, AdminError>;

    /// Extend the token's `expiry_timestamp_ms` (clamped by
    /// `max_timestamp_ms` on the broker side). `-1` lifetime defers to
    /// the broker's default renew period.
    async fn renew_delegation_token(&self, hmac: &[u8]) -> Result<DelegationToken, AdminError>;

    /// Immediate tombstone of the token. `-1` period = expire-now.
    async fn expire_delegation_token(&self, hmac: &[u8]) -> Result<(), AdminError>;

    /// `DescribeDelegationToken` filtered to a single owner principal
    /// string (e.g. `"User:alice"`). Empty vec = no matching tokens.
    async fn describe_delegation_tokens_owned_by(
        &self,
        owner_principal: &str,
    ) -> Result<Vec<DelegationToken>, AdminError>;
}

/// Persist the user's token credentials. Production impl wraps
/// `kube::Api<Secret>` server-side-apply; unit tests substitute an
/// in-memory `Mutex<Option<Secret>>`.
#[async_trait]
pub(crate) trait SecretWriter: Send + Sync {
    async fn apply(&self, secret: &Secret) -> Result<(), ReconcileError>;
}

/// Patch the status subresource of a `KafkaUser`. Production impl wraps
/// `kube::Api<KafkaUser>` merge-patch; unit tests record the JSON body.
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
}

/// Top-level reconcile entrypoint. Pure of any single-impl concrete
/// type — admin client + I/O writers are passed in as trait objects.
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
) -> Result<ReconcileOutcome, ReconcileError> {
    let name = obj.metadata.name.clone().unwrap_or_default();
    let owner_principal = format!("User:{name}");

    // 1. Describe — find any token already owned by this user.
    let existing = match admin
        .describe_delegation_tokens_owned_by(&owner_principal)
        .await
    {
        Ok(v) => v,
        Err(e) => return on_admin_error(obj, &name, e, "DescribeDelegationToken", users).await,
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

    // 2. Drive the decision. Each arm yields the live token + its
    //    expiry-driven requeue cadence.
    let (token, requeue): (DelegationToken, Duration) = match decision {
        ReconcileDecision::Create => match issue_new_token(&name, auth, admin).await {
            Ok(t) => {
                let r = compute_requeue(&t, auth, now_ms);
                (t, r)
            }
            Err(e) => {
                return on_admin_error(obj, &name, e, "CreateDelegationToken", users).await;
            }
        },
        ReconcileDecision::NoOp => {
            let t = matching.expect("NoOp implies existing token");
            let r = compute_requeue(&t, auth, now_ms);
            (t, r)
        }
        ReconcileDecision::Renew => {
            let existing_token = matching.expect("Renew implies existing token");
            match admin.renew_delegation_token(&existing_token.hmac).await {
                Ok(renewed) => {
                    let r = compute_requeue(&renewed, auth, now_ms);
                    (renewed, r)
                }
                Err(e) => {
                    return on_admin_error(obj, &name, e, "RenewDelegationToken", users).await;
                }
            }
        }
        ReconcileDecision::Cycle => {
            let existing_token = matching.expect("Cycle implies existing token");
            // Expire the old, then create the new. KIP-48 has no
            // in-place renewer-set mutation API, so we must cycle.
            if let Err(e) = admin.expire_delegation_token(&existing_token.hmac).await {
                return on_admin_error(obj, &name, e, "ExpireDelegationToken", users).await;
            }
            match issue_new_token(&name, auth, admin).await {
                Ok(t) => {
                    let r = compute_requeue(&t, auth, now_ms);
                    (t, r)
                }
                Err(e) => {
                    return on_admin_error(obj, &name, e, "CreateDelegationToken", users).await;
                }
            }
        }
    };

    // 3. Write the Secret with the live token credentials.
    let secret = build_secret(obj, &token)?;
    secrets.apply(&secret).await?;

    // 4. Patch status: token IDs + conditions.
    let conds = compute_conditions(&token, auth, now_ms, /*issued_ok=*/ true, None);
    let body = build_status_patch(&token, &conds);
    users.patch_status(&name, body).await?;

    Ok(ReconcileOutcome {
        action: Action::requeue(requeue),
    })
}

/// `Create` arm: issue a new token via act-as. `max_lifetime_ms` defaults
/// to `-1` (broker ceiling) when the spec omits the field.
async fn issue_new_token(
    name: &str,
    auth: &DelegationTokenAuth,
    admin: &dyn DelegationTokenAdmin,
) -> Result<DelegationToken, AdminError> {
    let max_lifetime_ms = auth.max_lifetime_ms.unwrap_or(-1);
    admin
        .create_delegation_token_as_owner(name, &auth.renewers, max_lifetime_ms)
        .await
}

/// Build the per-user Secret per spec §2.3. Keys: `token-id`, `hmac`
/// (raw bytes), `password` (base64-of-hmac, ready-to-paste), and
/// `sasl.jaas.config` (the JAAS line a JVM client can drop into
/// `--producer.config`).
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

/// Pure helper for the Secret's `data` map.
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

/// Wait until ~`renew_before_expiry_ms` before expiry. Clamped:
/// - at most 24h (the operator wants to re-check at least once a day),
/// - at least 1m (prevent a hot loop if the broker hands back an
///   already-stale `expiry_ts`).
pub(crate) fn compute_requeue(
    token: &DelegationToken,
    auth: &DelegationTokenAuth,
    now_ms: i64,
) -> Duration {
    let renew_before = auth
        .renew_before_expiry_ms
        .unwrap_or(DEFAULT_RENEW_BEFORE_EXPIRY_MS);
    let until_renew_ms = (token.expiry_timestamp_ms - now_ms - renew_before).max(0);
    let clamped_ms = until_renew_ms.clamp(60 * 1_000, 24 * 60 * 60 * 1_000); // [1m, 24h]
    Duration::from_millis(clamped_ms.cast_unsigned())
}

/// Compute the `Ready` + `TokenIssued` + `TokenExpiring` conditions.
///
/// - `TokenIssued = True` on the success path with reason `Issued`.
/// - `TokenIssued = False` on the error path with the spec §2.5 reason.
/// - `Ready = True` on the success path with reason `TokenReady`. The
///   reconcile loop only calls this after the Secret has been written,
///   so `Ready = True` is the spec §2.4 aggregator (`TokenIssued=True
///   AND Secret exists`).
/// - `Ready = False` on the error path with the same reason as
///   `TokenIssued`, so `kubectl describe kafkauser` surfaces the
///   correlated cause without cross-referencing two conditions.
/// - `TokenExpiring = True` when `expiry_ts - now < renew_before * 2`,
///   else `False`. Reason: `WithinRenewalHorizon` / `Healthy`.
pub(crate) fn compute_conditions(
    token: &DelegationToken,
    auth: &DelegationTokenAuth,
    now_ms: i64,
    issued_ok: bool,
    issued_reason: Option<(&str, &str)>,
) -> Vec<KafkaCondition> {
    let renew_before = auth
        .renew_before_expiry_ms
        .unwrap_or(DEFAULT_RENEW_BEFORE_EXPIRY_MS);
    let mut out = Vec::with_capacity(3);
    if issued_ok {
        out.push(condition(
            "Ready",
            "True",
            "TokenReady",
            "delegation token issued and Secret in sync",
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

    let expiring = token.expiry_timestamp_ms - now_ms < renew_before * 2;
    if expiring {
        out.push(condition(
            "TokenExpiring",
            "True",
            "WithinRenewalHorizon",
            "expiry < 2× renewBeforeExpiryMs from now",
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

/// Build the merge-patch JSON body for `KafkaUserStatus` after a
/// successful reconcile. Only sets the delegation-token specific fields
/// plus the two new conditions; existing status fields (managed by the
/// shared SCRAM/TLS path) are not touched.
pub(crate) fn build_status_patch(
    token: &DelegationToken,
    conditions: &[KafkaCondition],
) -> serde_json::Value {
    json!({
        "status": {
            "conditions": conditions,
            "username": token.owner.name,
            "secret": token.owner.name,
            "delegationTokenId": token.token_id,
            "delegationTokenExpiryTimestampMs": token.expiry_timestamp_ms,
            "delegationTokenMaxTimestampMs": token.max_timestamp_ms,
        }
    })
}

/// Build the merge-patch JSON body for a failure path. Patches only
/// the `TokenIssued = False` condition (no token fields to surface).
pub(crate) fn build_failure_status_patch(conditions: &[KafkaCondition]) -> serde_json::Value {
    json!({
        "status": {
            "conditions": conditions,
        }
    })
}

/// Map an `AdminError` to (Action, status patch) per spec §2.5.
///
/// - `0` is unreachable (success path doesn't call here).
/// - `42` `INVALID_REQUEST`        → `InvalidSpec`, no automatic retry (long requeue).
/// - `61` `AUTH_DISABLED`          → `BrokerAuthDisabled`, 5m backoff.
/// - `64` `REQUEST_NOT_ALLOWED`    → `OperatorTokenAuthed`, 5m backoff.
/// - `65` `AUTHORIZATION_FAILED`   → `OperatorNotSuperUser`, 5m backoff.
/// - other broker error            → `BrokerError`, 5m backoff.
/// - transport / connect / protocol → `Transport`, 5m backoff.
async fn on_admin_error(
    _obj: &KafkaUser,
    name: &str,
    err: AdminError,
    op: &'static str,
    users: &dyn KafkaUserStatusWriter,
) -> Result<ReconcileOutcome, ReconcileError> {
    let (reason, message, requeue): (&'static str, String, Duration) = match &err {
        AdminError::Broker { code, .. } if *code == CODE_INVALID_REQUEST => (
            "InvalidSpec",
            format!("{op}: INVALID_REQUEST (42)"),
            // No automatic recovery — long requeue so a human notices.
            Duration::from_hours(1),
        ),
        AdminError::Broker { code, .. } if *code == CODE_DELEGATION_TOKEN_AUTH_DISABLED => (
            "BrokerAuthDisabled",
            format!("{op}: DELEGATION_TOKEN_AUTH_DISABLED (61)"),
            TRANSIENT_BACKOFF,
        ),
        AdminError::Broker { code, .. } if *code == CODE_DELEGATION_TOKEN_REQUEST_NOT_ALLOWED => (
            "OperatorTokenAuthed",
            format!("{op}: DELEGATION_TOKEN_REQUEST_NOT_ALLOWED (64)"),
            TRANSIENT_BACKOFF,
        ),
        AdminError::Broker { code, .. } if *code == CODE_DELEGATION_TOKEN_AUTHORIZATION_FAILED => (
            "OperatorNotSuperUser",
            format!("{op}: DELEGATION_TOKEN_AUTHORIZATION_FAILED (65)"),
            TRANSIENT_BACKOFF,
        ),
        AdminError::Broker {
            code,
            name: code_name,
            ..
        } => (
            "BrokerError",
            format!("{op}: {code_name} ({code})"),
            TRANSIENT_BACKOFF,
        ),
        other => ("Transport", format!("{op}: {other}"), TRANSIENT_BACKOFF),
    };

    // Both `Ready` and `TokenIssued` flip to `False` with the same reason —
    // the aggregator and the underlying cause share a root, so they should
    // share a story in `kubectl describe`.
    let conds = vec![
        condition("Ready", "False", reason, &message),
        condition("TokenIssued", "False", reason, &message),
    ];
    let body = build_failure_status_patch(&conds);
    users.patch_status(name, body).await?;
    Ok(ReconcileOutcome {
        action: Action::requeue(requeue),
    })
}

/// `expire_owned_tokens` is called from the user.rs finalizer arm on
/// deletion. Best-effort: errors are logged by the caller and never
/// block finalizer removal.
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

/// Production `DelegationTokenAdmin` impl over the operator's
/// `AdminClientHandle` (an `Arc<Mutex<dyn AdminClientLike + Send>>`).
/// Each method locks the inner mutex and delegates to the four
/// `AdminClientLike` delegation-token methods.
///
/// The trait surface uses `&self`, so the mutex is taken per call —
/// matching the SCRAM/ACL/quota arms in `user.rs` which also lock the
/// same handle on each admin RPC.
#[async_trait]
impl DelegationTokenAdmin for crate::context::AdminClientHandle {
    async fn create_delegation_token_as_owner(
        &self,
        owner_principal_name: &str,
        renewers: &[String],
        max_lifetime_ms: i64,
    ) -> Result<DelegationToken, AdminError> {
        let mut admin = self.lock().await;
        admin
            .create_delegation_token_as_owner(owner_principal_name, renewers, max_lifetime_ms)
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
    use super::*;
    use assert2::assert;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use std::sync::Mutex as StdMutex;

    use crate::crd::{Authentication, KafkaUserSpec};

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

    fn auth(renewers: Vec<&str>, renew_before: Option<i64>) -> DelegationTokenAuth {
        DelegationTokenAuth {
            renewers: renewers.into_iter().map(str::to_string).collect(),
            max_lifetime_ms: None,
            renew_before_expiry_ms: renew_before,
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
        assert!(decide(&auth(vec![], Some(5000)), Some(&t), 0) == ReconcileDecision::Renew);
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

    /// Recorded call against the mock admin client. Tests assert on
    /// this to verify the reconciler called the right RPC with the
    /// right shape.
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum MockCall {
        Create {
            owner: String,
            renewers: Vec<String>,
            max_lifetime_ms: i64,
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
        /// If set, the next `create` extends `expiry_timestamp_ms` by
        /// this delta over `now` (proxied via tests setting issue ts).
        create_expiry_offset_ms: i64,
        /// If set, the next `renew` adds this delta to the existing
        /// token's expiry (capped at `max_timestamp_ms`).
        renew_delta_ms: i64,
        /// Optional canned error code returned from every RPC.
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
            max_lifetime_ms: i64,
        ) -> Result<DelegationToken, AdminError> {
            self.calls.lock().unwrap().push(MockCall::Create {
                owner: owner_principal_name.into(),
                renewers: renewers.to_vec(),
                max_lifetime_ms,
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
            max_lifetime_ms: Some(86_400_000),
            renew_before_expiry_ms: None,
        };
        let obj = user("alice", auth_cfg.clone());

        let out = reconcile(&obj, &auth_cfg, &admin, &secrets, &users, 0)
            .await
            .expect("reconcile should succeed");
        // Action is requeue (we don't inspect the exact duration; that's
        // compute_requeue's job, separately covered).
        let _ = out.action;

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
                        max_lifetime_ms: 86_400_000,
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

        // Status patch carries delegationTokenId + TokenIssued condition.
        let patches = users.patches.lock().unwrap();
        assert!(patches.len() == 1);
        let (name, body) = &patches[0];
        assert!(name == "alice");
        let status = body.get("status").unwrap();
        assert!(status.get("delegationTokenId").is_some());
        assert!(status.get("delegationTokenExpiryTimestampMs").is_some());
        let conds = status.get("conditions").unwrap().as_array().unwrap();
        assert!(
            conds
                .iter()
                .any(|c| c["type"] == "TokenIssued" && c["status"] == "True"),
            "missing TokenIssued=True: {conds:?}",
        );
        // Ready=True is the spec §2.4 aggregator (TokenIssued AND Secret
        // exists) — required for the kind e2e's
        // `kubectl wait --for=condition=Ready` to ever return.
        assert!(
            conds.iter().any(|c| c["type"] == "Ready"
                && c["status"] == "True"
                && c["reason"] == "TokenReady"),
            "missing Ready=True/TokenReady: {conds:?}",
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
            max_lifetime_ms: None,
            renew_before_expiry_ms: Some(60_000),
        };
        let obj = user("alice", auth_cfg.clone());

        let _ = reconcile(&obj, &auth_cfg, &admin, &secrets, &users, 0)
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

        // Secret + status both patched.
        assert!(secrets.applied.lock().unwrap().len() == 1);
        assert!(users.patches.lock().unwrap().len() == 1);
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

        let _ = reconcile(&obj, &auth_cfg, &admin, &secrets, &users, 0)
            .await
            .unwrap();

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
    async fn reconcile_maps_invalid_request_to_invalid_spec() {
        let mut admin = MockDelegationTokenAdmin::new();
        admin.force_broker_error = Some(CODE_INVALID_REQUEST);
        let secrets = RecordingSecretWriter::default();
        let users = RecordingStatusWriter::default();
        let auth_cfg = DelegationTokenAuth::default();
        let obj = user("alice", auth_cfg.clone());

        let _ = reconcile(&obj, &auth_cfg, &admin, &secrets, &users, 0)
            .await
            .unwrap();

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
        // we'd compute Duration::ZERO and hot-loop the reconciler.
        let t = token_with(0, vec![]);
        let r = compute_requeue(&t, &auth(vec![], None), 0);
        assert!(r >= Duration::from_mins(1));
    }

    #[test]
    fn compute_requeue_clamps_to_24h_maximum() {
        // Token expires in a year; without clamp we'd requeue weeks out.
        let t = token_with(365 * 24 * 60 * 60 * 1_000, vec![]);
        let r = compute_requeue(&t, &auth(vec![], None), 0);
        assert!(r <= Duration::from_hours(24));
    }

    #[test]
    fn compute_conditions_token_expiring_true_when_close_to_horizon() {
        // expiry - now (1500) < renew_before (1000) * 2 → TokenExpiring=True
        let t = token_with(1500, vec![]);
        let conds = compute_conditions(&t, &auth(vec![], Some(1000)), 0, true, None);
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
        let conds = compute_conditions(&t, &auth(vec![], Some(1000)), 0, true, None);
        let expiring = conds.iter().find(|c| c.type_ == "TokenExpiring").unwrap();
        assert!(expiring.status == "False");
        assert!(expiring.reason == "Healthy");
        let ready = conds.iter().find(|c| c.type_ == "Ready").unwrap();
        assert!(ready.status == "True");
        assert!(ready.reason == "TokenReady");
    }
}
