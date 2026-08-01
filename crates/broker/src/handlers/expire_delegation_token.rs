//! KIP-48: `ExpireDelegationToken` (`api_key` 40).
//!
//! Per spec §1.4: caller must be SASL-authenticated; the request's
//! `hmac` selects an existing token; only the owner, a `renewers`
//! entry, or a configured super-user may expire it (else
//! `DELEGATION_TOKEN_AUTHORIZATION_FAILED`). The super-user bypass
//! matches Kafka's `DelegationTokenManager.isAuthorizedToOperateOnToken`
//! (via `SecurityUtils.isAuthorized`) and is what the operator
//! relies on for cleaning up tokens it minted via act-as on behalf of
//! `KafkaUser` principals.
//!
//! Decision on `expiry_time_period_ms`:
//!   - `< 0`  → append `V1DeleteDelegationToken` tombstone; respond with
//!     a past-sentinel `expiry_timestamp_ms = now - 1` per KIP-48.
//!   - `== 0` → set expiry to `now` (record-replace).
//!   - `> 0`  → set expiry to `now + period`, clamped to the token's
//!     `max_timestamp_ms` (record-replace).

use std::{collections::HashSet, hash::BuildHasher};

use crabka_metadata::{
    DelegationToken, DelegationTokenRecord, DeleteDelegationTokenRecord, MetadataRecord,
};
use crabka_protocol::owned::{
    expire_delegation_token_request::ExpireDelegationTokenRequest,
    expire_delegation_token_response::ExpireDelegationTokenResponse,
};
use crabka_security::SecretBytes;

use crate::{network::auth::ConnectionAuth, time_util::now_ms};

#[tracing::instrument(
    name = "handle_expire_delegation_token",
    level = "info",
    skip_all,
    fields(api = "ExpireDelegationToken")
)]
pub(crate) async fn handle<S: BuildHasher>(
    req: &ExpireDelegationTokenRequest,
    auth: &ConnectionAuth,
    secret_key: Option<&SecretBytes>,
    controller: &dyn crate::metadata_source::MetadataSource,
    super_users: &HashSet<String, S>,
) -> ExpireDelegationTokenResponse {
    if secret_key.is_none() {
        return err_response(crate::codes::DELEGATION_TOKEN_AUTH_DISABLED);
    }
    let ConnectionAuth::Authenticated { principal, .. } = auth else {
        return err_response(crate::codes::INVALID_REQUEST);
    };
    let caller = principal.to_kafka();

    let image = controller.current_image();
    // KIP-48/KIP-778: KRaft delegation tokens require metadata.version >= 3.6-IV2.
    if crate::features::require_feature(
        &image,
        crate::features::METADATA_VERSION,
        crabka_metadata::metadata_version::DELEGATION_TOKEN_MIN_LEVEL,
    )
    .is_err()
    {
        return err_response(crate::codes::UNSUPPORTED_VERSION);
    }
    let Some(token) = image.delegation_token_by_hmac(req.hmac.as_ref()).cloned() else {
        return err_response(crate::codes::DELEGATION_TOKEN_NOT_FOUND);
    };

    // KIP-48: super-users bypass the owner/renewer gate. See the renew
    // handler module docs for the operator-flow rationale.
    let is_super_user = super_users.contains(&principal.name);
    if !is_super_user && token.owner != caller && !token.renewers.contains(&caller) {
        return err_response(crate::codes::DELEGATION_TOKEN_AUTHORIZATION_FAILED);
    }

    let new_expiry = if req.expiry_time_period_ms < 0 {
        // KIP-48: negative period deletes the token immediately. The
        // response carries a past-sentinel timestamp.
        if let Err(e) = controller
            .submit_change(vec![MetadataRecord::V1DeleteDelegationToken(
                DeleteDelegationTokenRecord {
                    token_id: token.token_id.clone(),
                },
            )])
            .await
        {
            tracing::warn!(error = %e, "ExpireDelegationToken: tombstone submit_change failed");
            return err_response(crate::codes::INVALID_REQUEST);
        }
        now_ms() - 1
    } else {
        let now = now_ms();
        let candidate = if req.expiry_time_period_ms == 0 {
            now
        } else {
            now + req.expiry_time_period_ms
        };
        let new_expiry = candidate.min(token.max_timestamp_ms);
        let record = DelegationTokenRecord {
            expiry_timestamp_ms: new_expiry,
            ..token_to_record(&token)
        };
        if let Err(e) = controller
            .submit_change(vec![MetadataRecord::V1DelegationToken(record)])
            .await
        {
            tracing::warn!(error = %e, "ExpireDelegationToken: update submit_change failed");
            return err_response(crate::codes::INVALID_REQUEST);
        }
        new_expiry
    };

    ExpireDelegationTokenResponse {
        error_code: 0,
        expiry_timestamp_ms: new_expiry,
        ..Default::default()
    }
}

fn err_response(code: i16) -> ExpireDelegationTokenResponse {
    ExpireDelegationTokenResponse {
        error_code: code,
        ..Default::default()
    }
}

/// Project an image-level [`DelegationToken`] back into a
/// [`DelegationTokenRecord`] so a partial update (only `expiry_*`
/// changing) can be expressed via struct-update syntax.
fn token_to_record(t: &DelegationToken) -> DelegationTokenRecord {
    DelegationTokenRecord {
        token_id: t.token_id.clone(),
        owner: t.owner.clone(),
        hmac: t.hmac.clone(),
        issue_timestamp_ms: t.issue_timestamp_ms,
        expiry_timestamp_ms: t.expiry_timestamp_ms,
        max_timestamp_ms: t.max_timestamp_ms,
        renewers: t.renewers.clone(),
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc, time::Duration};

    use assert2::assert;
    use crabka_raft::ControllerHandle;
    use crabka_security::{AuthMethod, KafkaPrincipal, Principal, SaslMechanism};
    use tempfile::TempDir;

    use super::*;

    /// Helper: empty super-users set for the pre-existing tests, which
    /// all exercise the owner/renewer path.
    fn empty_super_users() -> HashSet<String> {
        HashSet::new()
    }

    /// Helper: super-users set containing the given names (for the new
    /// super-user-bypass tests).
    fn super_users_with(names: &[&str]) -> HashSet<String> {
        names.iter().map(|s| (*s).to_string()).collect()
    }

    async fn test_controller(log_dir: std::path::PathBuf) -> Arc<ControllerHandle> {
        let cfg = crabka_raft::ControllerConfig {
            election_timeout: crabka_units::millis(200),
            heartbeat_interval: Some(crabka_units::millis(50)),
            client_id: "test".into(),
            ..crabka_raft::ControllerConfig::for_tests(crabka_raft::NodeId(1), log_dir)
        };
        let handle = Arc::new(crabka_raft::Controller::start(cfg).await.unwrap());
        let mut rx = handle.watch_leader();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while rx.borrow().is_none() {
            assert!(std::time::Instant::now() < deadline, "no leader in 5s");
            let _ = tokio::time::timeout(Duration::from_millis(100), rx.changed()).await;
        }
        handle
    }

    fn authed(name: &str) -> ConnectionAuth {
        ConnectionAuth::Authenticated {
            principal: Principal {
                name: name.into(),
                auth_method: AuthMethod::SaslScramSha256,
                groups: vec![],
            },
            mechanism: SaslMechanism::ScramSha256,
            expires_at_ms: None,
            authenticated_via_token: false,
        }
    }

    fn kp(name: &str) -> KafkaPrincipal {
        KafkaPrincipal {
            principal_type: "User".into(),
            name: name.into(),
        }
    }

    async fn seed_token(
        target: (&ControllerHandle, &str),
        hmac: Vec<u8>,
        owner: KafkaPrincipal,
        renewers: Vec<KafkaPrincipal>,
        issue_ms: i64,
        expiry_ms: i64,
        max_ms: i64,
    ) {
        let (controller, token_id) = target;
        let rec = DelegationTokenRecord {
            token_id: token_id.into(),
            owner,
            hmac,
            issue_timestamp_ms: issue_ms,
            expiry_timestamp_ms: expiry_ms,
            max_timestamp_ms: max_ms,
            renewers,
        };
        controller
            .submit_change(vec![MetadataRecord::V1DelegationToken(rec)])
            .await
            .expect("seed token");
    }

    #[tokio::test]
    async fn returns_auth_disabled_when_no_secret_key() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let req = ExpireDelegationTokenRequest::default();
        let resp = handle(
            &req,
            &authed("alice"),
            None,
            &*controller,
            &empty_super_users(),
        )
        .await;
        assert!(resp.error_code == crate::codes::DELEGATION_TOKEN_AUTH_DISABLED);
        controller.cancel().await;
    }

    #[tokio::test]
    async fn future_expiry_period_updates_token() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());
        let hmac = vec![0xAA; 32];
        let now = now_ms();
        seed_token(
            (&controller, "tok-1"),
            hmac.clone(),
            kp("alice"),
            vec![],
            now - 1_000,
            now + 60_000,
            now + 7 * 24 * 60 * 60 * 1_000,
        )
        .await;

        let req = ExpireDelegationTokenRequest {
            hmac: hmac.into(),
            expiry_time_period_ms: 30_000,
            ..Default::default()
        };
        let resp = handle(
            &req,
            &authed("alice"),
            Some(&secret),
            &*controller,
            &empty_super_users(),
        )
        .await;
        assert!(resp.error_code == 0);
        let target = now_ms() + 30_000;
        let slop = 60_000;
        assert!(
            (resp.expiry_timestamp_ms - target).abs() < slop,
            "expiry {} far from {target}",
            resp.expiry_timestamp_ms
        );
        let img = controller.current_image();
        let stored = img.delegation_token_by_id("tok-1").expect("present");
        assert!(stored.expiry_timestamp_ms == resp.expiry_timestamp_ms);
        controller.cancel().await;
    }

    #[tokio::test]
    async fn negative_period_immediately_tombstones() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());
        let hmac = vec![0xBB; 32];
        let now = now_ms();
        seed_token(
            (&controller, "tok-2"),
            hmac.clone(),
            kp("alice"),
            vec![],
            now - 1_000,
            now + 60_000,
            now + 7 * 24 * 60 * 60 * 1_000,
        )
        .await;

        let req = ExpireDelegationTokenRequest {
            hmac: hmac.into(),
            expiry_time_period_ms: -1,
            ..Default::default()
        };
        let resp = handle(
            &req,
            &authed("alice"),
            Some(&secret),
            &*controller,
            &empty_super_users(),
        )
        .await;
        assert!(resp.error_code == 0);
        // Past sentinel: should be <= now.
        assert!(resp.expiry_timestamp_ms <= now_ms());
        // Token removed from image.
        let img = controller.current_image();
        assert!(img.delegation_token_by_id("tok-2").is_none());
        controller.cancel().await;
    }

    #[tokio::test]
    async fn unauthorized_caller_returns_authorization_failed() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());
        let hmac = vec![0xCC; 32];
        let now = now_ms();
        seed_token(
            (&controller, "tok-3"),
            hmac.clone(),
            kp("alice"),
            vec![kp("bob")],
            now - 1_000,
            now + 60_000,
            now + 7 * 24 * 60 * 60 * 1_000,
        )
        .await;

        let req = ExpireDelegationTokenRequest {
            hmac: hmac.into(),
            expiry_time_period_ms: 1_000,
            ..Default::default()
        };
        let resp = handle(
            &req,
            &authed("eve"),
            Some(&secret),
            &*controller,
            &empty_super_users(),
        )
        .await;
        assert!(resp.error_code == crate::codes::DELEGATION_TOKEN_AUTHORIZATION_FAILED);
        // Token unchanged.
        let img = controller.current_image();
        let stored = img.delegation_token_by_id("tok-3").expect("present");
        assert!(stored.expiry_timestamp_ms == now + 60_000);
        controller.cancel().await;
    }

    /// A super-user caller may expire a token they
    /// neither own nor are listed as a renewer on. Mirrors Kafka's
    /// `DelegationTokenManager.isAuthorizedToOperateOnToken` and is the
    /// load-bearing gate for the operator's finalizer — on
    /// `KafkaUser` delete, the operator tombstones the act-as-minted
    /// token by calling `ExpireDelegationToken` with period = -1.
    #[tokio::test]
    async fn super_user_can_expire_any_token() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());
        let hmac = vec![0xDD; 32];
        let now = now_ms();
        seed_token(
            (&controller, "tok-super"),
            hmac.clone(),
            kp("alice"),
            vec![],
            now - 1_000,
            now + 60_000,
            now + 7 * 24 * 60 * 60 * 1_000,
        )
        .await;

        // Period = -1 → tombstone path; same code path the operator's
        // finalizer hits.
        let req = ExpireDelegationTokenRequest {
            hmac: hmac.into(),
            expiry_time_period_ms: -1,
            ..Default::default()
        };
        let resp = handle(
            &req,
            &authed("admin"),
            Some(&secret),
            &*controller,
            &super_users_with(&["admin"]),
        )
        .await;
        assert!(
            resp.error_code == 0,
            "super-user must be able to expire any token regardless of owner/renewers"
        );
        // Past-sentinel + tombstoned.
        assert!(resp.expiry_timestamp_ms <= now_ms());
        let img = controller.current_image();
        assert!(img.delegation_token_by_id("tok-super").is_none());
        controller.cancel().await;
    }

    /// A non-super-user caller who is also not the
    /// owner and not a listed renewer must still be rejected with
    /// `DELEGATION_TOKEN_AUTHORIZATION_FAILED`. Guards against
    /// accidentally widening the bypass beyond `super_users`.
    #[tokio::test]
    async fn non_super_user_non_owner_non_renewer_still_rejected() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());
        let hmac = vec![0xEE; 32];
        let now = now_ms();
        seed_token(
            (&controller, "tok-eve"),
            hmac.clone(),
            kp("alice"),
            vec![kp("bob")],
            now - 1_000,
            now + 60_000,
            now + 7 * 24 * 60 * 60 * 1_000,
        )
        .await;

        let req = ExpireDelegationTokenRequest {
            hmac: hmac.into(),
            expiry_time_period_ms: 1_000,
            ..Default::default()
        };
        // `eve` is not in the super-users set (only `admin` is) and is
        // neither owner nor renewer — must still get the authz error.
        let resp = handle(
            &req,
            &authed("eve"),
            Some(&secret),
            &*controller,
            &super_users_with(&["admin"]),
        )
        .await;
        assert!(resp.error_code == crate::codes::DELEGATION_TOKEN_AUTHORIZATION_FAILED);
        // Token unchanged.
        let img = controller.current_image();
        let stored = img.delegation_token_by_id("tok-eve").expect("present");
        assert!(stored.expiry_timestamp_ms == now + 60_000);
        controller.cancel().await;
    }
}
