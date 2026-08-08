//! KIP-48: `RenewDelegationToken` (`api_key` 39).
//!
//! Per spec §1.3, the caller must be SASL-authenticated. The request's
//! `hmac` selects an existing token by HMAC bytes. Only the owner, a
//! `renewers` entry, or a configured super-user may extend it. The handler
//! clamps the new expiry to the token's `max_timestamp_ms`. It then appends a
//! fresh `V1DelegationToken` record with the same `token_id`. The image
//! semantics are replace.
//!
//! The super-user bypass matches Kafka's `DelegationTokenManager.
//! isAuthorizedToOperateOnToken`, which goes through
//! `SecurityUtils.isAuthorized`. The operator relies on this bypass. The
//! operator is a super-user that mints tokens on behalf of `KafkaUser`
//! principals with act-as, then must be able to renew or expire them even
//! though it is neither the owner nor a listed renewer.

use std::{collections::HashSet, hash::BuildHasher};

use crabka_metadata::{DelegationTokenRecord, MetadataRecord};
use crabka_protocol::owned::{
    renew_delegation_token_request::RenewDelegationTokenRequest,
    renew_delegation_token_response::RenewDelegationTokenResponse,
};
use crabka_security::SecretBytes;

use crate::{network::auth::ConnectionAuth, time_util::now_ms};

#[tracing::instrument(
    name = "handle_renew_delegation_token",
    level = "info",
    skip_all,
    fields(api = "RenewDelegationToken")
)]
pub(crate) async fn handle<S: BuildHasher>(
    req: &RenewDelegationTokenRequest,
    auth: &ConnectionAuth,
    secret_key: Option<&SecretBytes>,
    default_renew_period_ms: i64,
    controller: &dyn crate::metadata_source::MetadataSource,
    super_users: &HashSet<String, S>,
) -> RenewDelegationTokenResponse {
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

    // KIP-48: super-users bypass the owner/renewer gate. The
    // operator-driven issuance flow depends on this — the operator is
    // a super-user that mints tokens via act-as for other principals,
    // so it is neither the owner nor a listed renewer when it later
    // needs to renew them ahead of expiry.
    let is_super_user = super_users.contains(&principal.name);
    if !is_super_user && token.owner != caller && !token.renewers.contains(&caller) {
        return err_response(crate::codes::DELEGATION_TOKEN_OWNER_MISMATCH);
    }

    let now = now_ms();
    let renew_period_ms = if req.renew_period_ms == -1 {
        default_renew_period_ms
    } else {
        req.renew_period_ms
    };
    let new_expiry = (now + renew_period_ms).min(token.max_timestamp_ms);

    let record = DelegationTokenRecord {
        token_id: token.token_id.clone(),
        owner: token.owner.clone(),
        hmac: token.hmac.clone(),
        issue_timestamp_ms: token.issue_timestamp_ms,
        expiry_timestamp_ms: new_expiry,
        max_timestamp_ms: token.max_timestamp_ms,
        renewers: token.renewers.clone(),
    };
    if let Err(e) = controller
        .submit_change(vec![MetadataRecord::V1DelegationToken(record)])
        .await
    {
        tracing::warn!(error = %e, "RenewDelegationToken: submit_change failed");
        return err_response(crate::codes::INVALID_REQUEST);
    }

    RenewDelegationTokenResponse {
        error_code: 0,
        expiry_timestamp_ms: new_expiry,
        ..Default::default()
    }
}

fn err_response(code: i16) -> RenewDelegationTokenResponse {
    RenewDelegationTokenResponse {
        error_code: code,
        ..Default::default()
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

    /// Helper: empty super-users set for the tests that all exercise the
    /// owner/renewer path.
    fn empty_super_users() -> HashSet<String> {
        HashSet::new()
    }

    /// Helper: super-users set containing the given names, for the
    /// super-user-bypass tests.
    fn super_users_with(names: &[&str]) -> HashSet<String> {
        names.iter().map(|s| (*s).to_string()).collect()
    }

    /// Spin up a single-voter `Controller` for tests, wait for leader.
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
        // Auth-disabled gate fires before anything else (no controller needed).
        let req = RenewDelegationTokenRequest::default();
        let auth = authed("alice");
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let resp = handle(&req, &auth, None, 1_000, &*controller, &empty_super_users()).await;
        assert!(resp.error_code == crate::codes::DELEGATION_TOKEN_AUTH_DISABLED);
        controller.cancel().await;
    }

    #[tokio::test]
    async fn success_as_owner_extends_expiry() {
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

        let req = RenewDelegationTokenRequest {
            hmac: hmac.into(),
            renew_period_ms: 3_600_000, // +1h
            ..Default::default()
        };
        let resp = handle(
            &req,
            &authed("alice"),
            Some(&secret),
            1_000,
            &*controller,
            &empty_super_users(),
        )
        .await;
        assert!(resp.error_code == 0);
        // expiry should be roughly now + 1h (within a small slop window).
        let slop = 60_000;
        let target = now_ms() + 3_600_000;
        assert!(
            (resp.expiry_timestamp_ms - target).abs() < slop,
            "expiry {} far from {target}",
            resp.expiry_timestamp_ms
        );
        // Persisted in image.
        let img = controller.current_image();
        let stored = img.delegation_token_by_id("tok-1").expect("present");
        assert!(stored.expiry_timestamp_ms == resp.expiry_timestamp_ms);
        controller.cancel().await;
    }

    #[tokio::test]
    async fn minus_one_renew_period_uses_default_period() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());
        let hmac = vec![0xA1; 32];
        let now = now_ms();
        seed_token(
            (&controller, "tok-default-renew"),
            hmac.clone(),
            kp("alice"),
            vec![],
            now - 1_000,
            now + 60_000,
            now + 7 * 24 * 60 * 60 * 1_000,
        )
        .await;

        let default_period = 120_000;
        let req = RenewDelegationTokenRequest {
            hmac: hmac.into(),
            renew_period_ms: -1,
            ..Default::default()
        };
        let resp = handle(
            &req,
            &authed("alice"),
            Some(&secret),
            default_period,
            &*controller,
            &empty_super_users(),
        )
        .await;

        assert!(resp.error_code == 0);
        let target = now_ms() + default_period;
        assert!(
            (resp.expiry_timestamp_ms - target).abs() < 60_000,
            "expiry {} far from {target}",
            resp.expiry_timestamp_ms
        );
        controller.cancel().await;
    }

    #[tokio::test]
    async fn success_as_renewer_extends_expiry() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());
        let hmac = vec![0xBB; 32];
        let now = now_ms();
        seed_token(
            (&controller, "tok-2"),
            hmac.clone(),
            kp("alice"),
            vec![kp("bob")],
            now - 1_000,
            now + 60_000,
            now + 7 * 24 * 60 * 60 * 1_000,
        )
        .await;

        let req = RenewDelegationTokenRequest {
            hmac: hmac.into(),
            renew_period_ms: 60_000, // +1m
            ..Default::default()
        };
        let resp = handle(
            &req,
            &authed("bob"),
            Some(&secret),
            1_000,
            &*controller,
            &empty_super_users(),
        )
        .await;
        assert!(resp.error_code == 0);
        assert!(resp.expiry_timestamp_ms > now + 30_000);
        controller.cancel().await;
    }

    #[tokio::test]
    async fn unknown_hmac_returns_not_found() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());
        let req = RenewDelegationTokenRequest {
            hmac: vec![0xFF; 32].into(),
            renew_period_ms: 60_000,
            ..Default::default()
        };
        let resp = handle(
            &req,
            &authed("alice"),
            Some(&secret),
            1_000,
            &*controller,
            &empty_super_users(),
        )
        .await;
        assert!(resp.error_code == crate::codes::DELEGATION_TOKEN_NOT_FOUND);
        controller.cancel().await;
    }

    #[tokio::test]
    async fn non_owner_non_renewer_returns_owner_mismatch() {
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

        let req = RenewDelegationTokenRequest {
            hmac: hmac.into(),
            renew_period_ms: 60_000,
            ..Default::default()
        };
        let resp = handle(
            &req,
            &authed("eve"),
            Some(&secret),
            1_000,
            &*controller,
            &empty_super_users(),
        )
        .await;
        assert!(resp.error_code == crate::codes::DELEGATION_TOKEN_OWNER_MISMATCH);
        controller.cancel().await;
    }

    /// A super-user caller may renew a token they
    /// neither own nor are listed as a renewer on. This mirrors Kafka's
    /// `DelegationTokenManager.isAuthorizedToOperateOnToken` and is the
    /// load-bearing gate for the operator flow. The operator
    /// is a super-user that act-as-mints tokens on behalf of `KafkaUser`
    /// principals, then must be able to renew them before expiry.
    #[tokio::test]
    async fn super_user_can_renew_any_token() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());
        let hmac = vec![0xDD; 32];
        let now = now_ms();
        // Token owned by `alice`, no renewers — operator (`admin`) is
        // neither, but it IS in `super_users`, so renew must succeed.
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

        let req = RenewDelegationTokenRequest {
            hmac: hmac.into(),
            renew_period_ms: 3_600_000, // +1h
            ..Default::default()
        };
        let resp = handle(
            &req,
            &authed("admin"),
            Some(&secret),
            1_000,
            &*controller,
            &super_users_with(&["admin"]),
        )
        .await;
        assert!(
            resp.error_code == 0,
            "super-user must be able to renew any token regardless of owner/renewers"
        );
        // Persisted in image with the new expiry.
        let img = controller.current_image();
        let stored = img.delegation_token_by_id("tok-super").expect("present");
        assert!(stored.expiry_timestamp_ms == resp.expiry_timestamp_ms);
        controller.cancel().await;
    }

    /// The handler must still reject a non-super-user caller who is also not
    /// the owner and not a listed renewer, with
    /// `DELEGATION_TOKEN_OWNER_MISMATCH`. This test guards against accidentally
    /// widening the bypass beyond `super_users`.
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

        let req = RenewDelegationTokenRequest {
            hmac: hmac.into(),
            renew_period_ms: 60_000,
            ..Default::default()
        };
        // `eve` is not in the super-users set (only `admin` is) and is
        // neither owner nor renewer — must still get the mismatch error.
        let resp = handle(
            &req,
            &authed("eve"),
            Some(&secret),
            1_000,
            &*controller,
            &super_users_with(&["admin"]),
        )
        .await;
        assert!(resp.error_code == crate::codes::DELEGATION_TOKEN_OWNER_MISMATCH);
        controller.cancel().await;
    }
}
