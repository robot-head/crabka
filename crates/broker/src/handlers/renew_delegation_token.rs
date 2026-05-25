//! Slice 51 (KIP-48): `RenewDelegationToken` (`api_key` 39).
//!
//! Per spec §1.3: caller must be SASL-authenticated; the request's
//! `hmac` selects an existing token by HMAC bytes; only the owner or a
//! `renewers` entry may extend it; the new expiry is clamped to the
//! token's `max_timestamp_ms`; a fresh `V1DelegationToken` record is
//! appended with the same `token_id` (image semantics: replace).

use crabka_metadata::{DelegationTokenRecord, MetadataRecord};
use crabka_protocol::owned::renew_delegation_token_request::RenewDelegationTokenRequest;
use crabka_protocol::owned::renew_delegation_token_response::RenewDelegationTokenResponse;
use crabka_raft::ControllerHandle;
use crabka_security::SecretBytes;

use crate::network::auth::ConnectionAuth;
use crate::time_util::now_ms;

pub(crate) async fn handle(
    req: &RenewDelegationTokenRequest,
    auth: &ConnectionAuth,
    secret_key: Option<&SecretBytes>,
    default_renew_period_ms: i64,
    controller: &ControllerHandle,
) -> RenewDelegationTokenResponse {
    if secret_key.is_none() {
        return err_response(crate::codes::DELEGATION_TOKEN_AUTH_DISABLED);
    }
    let ConnectionAuth::Authenticated { principal, .. } = auth else {
        return err_response(crate::codes::INVALID_REQUEST);
    };
    let caller = principal.to_kafka();

    let image = controller.current_image();
    let Some(token) = image.delegation_token_by_hmac(req.hmac.as_ref()).cloned() else {
        return err_response(crate::codes::DELEGATION_TOKEN_NOT_FOUND);
    };

    if token.owner != caller && !token.renewers.contains(&caller) {
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
    use super::*;
    use crabka_security::{AuthMethod, KafkaPrincipal, Principal, SaslMechanism};
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;

    /// Spin up a single-voter `Controller` for tests, wait for leader.
    async fn test_controller(log_dir: std::path::PathBuf) -> Arc<ControllerHandle> {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let cfg = crabka_raft::ControllerConfig {
            node_id: 1,
            voters: vec![(1, addr)],
            controller_listen_addr: addr,
            log_dir,
            election_timeout: Duration::from_millis(200),
            heartbeat_interval: Duration::from_millis(50),
            client_id: "test".into(),
            bootstrap_mode: crabka_raft::BootstrapMode::Bootstrap,
            cluster_id: None,
            dialer: None,
            handshake: None,
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
        controller: &ControllerHandle,
        token_id: &str,
        hmac: Vec<u8>,
        owner: KafkaPrincipal,
        renewers: Vec<KafkaPrincipal>,
        issue_ms: i64,
        expiry_ms: i64,
        max_ms: i64,
    ) {
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
        let resp = handle(&req, &auth, None, 1_000, &controller).await;
        assert_eq!(
            resp.error_code,
            crate::codes::DELEGATION_TOKEN_AUTH_DISABLED
        );
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
            &controller,
            "tok-1",
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
        let resp = handle(&req, &authed("alice"), Some(&secret), 1_000, &controller).await;
        assert_eq!(resp.error_code, 0);
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
        assert_eq!(stored.expiry_timestamp_ms, resp.expiry_timestamp_ms);
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
            &controller,
            "tok-2",
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
        let resp = handle(&req, &authed("bob"), Some(&secret), 1_000, &controller).await;
        assert_eq!(resp.error_code, 0);
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
        let resp = handle(&req, &authed("alice"), Some(&secret), 1_000, &controller).await;
        assert_eq!(resp.error_code, crate::codes::DELEGATION_TOKEN_NOT_FOUND);
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
            &controller,
            "tok-3",
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
        let resp = handle(&req, &authed("eve"), Some(&secret), 1_000, &controller).await;
        assert_eq!(
            resp.error_code,
            crate::codes::DELEGATION_TOKEN_OWNER_MISMATCH
        );
        controller.cancel().await;
    }
}
