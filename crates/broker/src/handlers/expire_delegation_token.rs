//! Slice 51 (KIP-48): `ExpireDelegationToken` (`api_key` 40).
//!
//! Per spec §1.4: caller must be SASL-authenticated; the request's
//! `hmac` selects an existing token; only the owner or a `renewers`
//! entry may expire it (else `DELEGATION_TOKEN_AUTHORIZATION_FAILED`).
//! Decision on `expiry_time_period_ms`:
//!   - `< 0`  → append `V1DeleteDelegationToken` tombstone; respond with
//!              a past-sentinel `expiry_timestamp_ms = now - 1` per KIP-48.
//!   - `== 0` → set expiry to `now` (record-replace).
//!   - `> 0`  → set expiry to `now + period`, clamped to the token's
//!              `max_timestamp_ms` (record-replace).

use crabka_metadata::{
    DelegationToken, DelegationTokenRecord, DeleteDelegationTokenRecord, MetadataRecord,
};
use crabka_protocol::owned::expire_delegation_token_request::ExpireDelegationTokenRequest;
use crabka_protocol::owned::expire_delegation_token_response::ExpireDelegationTokenResponse;
use crabka_raft::ControllerHandle;
use crabka_security::SecretBytes;

use crate::network::auth::ConnectionAuth;
use crate::time_util::now_ms;

pub(crate) async fn handle(
    req: &ExpireDelegationTokenRequest,
    auth: &ConnectionAuth,
    secret_key: Option<&SecretBytes>,
    controller: &ControllerHandle,
) -> ExpireDelegationTokenResponse {
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
    use super::*;
    use crabka_security::{AuthMethod, KafkaPrincipal, Principal, SaslMechanism};
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;

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
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let req = ExpireDelegationTokenRequest::default();
        let resp = handle(&req, &authed("alice"), None, &controller).await;
        assert_eq!(
            resp.error_code,
            crate::codes::DELEGATION_TOKEN_AUTH_DISABLED
        );
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

        let req = ExpireDelegationTokenRequest {
            hmac: hmac.into(),
            expiry_time_period_ms: 30_000,
            ..Default::default()
        };
        let resp = handle(&req, &authed("alice"), Some(&secret), &controller).await;
        assert_eq!(resp.error_code, 0);
        let target = now_ms() + 30_000;
        let slop = 60_000;
        assert!(
            (resp.expiry_timestamp_ms - target).abs() < slop,
            "expiry {} far from {target}",
            resp.expiry_timestamp_ms
        );
        let img = controller.current_image();
        let stored = img.delegation_token_by_id("tok-1").expect("present");
        assert_eq!(stored.expiry_timestamp_ms, resp.expiry_timestamp_ms);
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
            &controller,
            "tok-2",
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
        let resp = handle(&req, &authed("alice"), Some(&secret), &controller).await;
        assert_eq!(resp.error_code, 0);
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

        let req = ExpireDelegationTokenRequest {
            hmac: hmac.into(),
            expiry_time_period_ms: 1_000,
            ..Default::default()
        };
        let resp = handle(&req, &authed("eve"), Some(&secret), &controller).await;
        assert_eq!(
            resp.error_code,
            crate::codes::DELEGATION_TOKEN_AUTHORIZATION_FAILED
        );
        // Token unchanged.
        let img = controller.current_image();
        let stored = img.delegation_token_by_id("tok-3").expect("present");
        assert_eq!(stored.expiry_timestamp_ms, now + 60_000);
        controller.cancel().await;
    }
}
