//! Slice 51 (KIP-48): `CreateDelegationToken` (`api_key` 38).
//!
//! Per spec §1.2: caller must be SASL-authenticated and NOT itself
//! authenticated via a delegation token (KIP-48 forbids
//! token-creating-token chains). The owner is always the calling
//! principal (we don't support the privileged "act-as" form via wire
//! `owner_principal_type/name`). The HMAC-SHA-256 of `(secret_key,
//! token_id)` becomes the token's "password equivalent" — clients
//! re-authenticate with the hex `token_id` as the SCRAM username and
//! the HMAC bytes as the password.

use crabka_metadata::{DelegationTokenRecord, MetadataRecord};
use crabka_protocol::owned::create_delegation_token_request::CreateDelegationTokenRequest;
use crabka_protocol::owned::create_delegation_token_response::CreateDelegationTokenResponse;
use crabka_raft::ControllerHandle;
use crabka_security::{KafkaPrincipal, Principal, SecretBytes};

use crate::network::auth::ConnectionAuth;

pub(crate) async fn handle(
    req: &CreateDelegationTokenRequest,
    auth: &ConnectionAuth,
    secret_key: Option<&SecretBytes>,
    max_lifetime_ms: i64,
    controller: &ControllerHandle,
) -> CreateDelegationTokenResponse {
    let Some(secret_key) = secret_key else {
        return err_response(crate::codes::DELEGATION_TOKEN_AUTH_DISABLED);
    };

    let ConnectionAuth::Authenticated {
        principal,
        authenticated_via_token,
        ..
    } = auth
    else {
        return err_response(crate::codes::INVALID_REQUEST);
    };
    if *authenticated_via_token {
        // KIP-48: a delegation-token-authed caller cannot create more
        // delegation tokens (no token-creating-token chains).
        return err_response(crate::codes::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED);
    }

    // KIP-48: the owner is the requester. The wire
    // `owner_principal_type/name` fields exist for the privileged
    // "act-as" path, which Crabka doesn't support (any non-self request
    // would be a no-op since we'd reject it).
    let owner = principal_to_kafka(principal);

    // Validate + clamp `max_lifetime_ms`. `-1` defers to the broker
    // ceiling; a positive value is clamped to the ceiling; anything
    // else is invalid (zero or non-`-1` negatives).
    let chosen_lifetime = match req.max_lifetime_ms {
        -1 => max_lifetime_ms,
        n if n > 0 => n.min(max_lifetime_ms),
        _ => return err_response(crate::codes::INVALID_REQUEST),
    };

    let now = chrono::Utc::now().timestamp_millis();
    let token_id = uuid::Uuid::new_v4().to_string();
    let hmac = crabka_security::compute_token_hmac(secret_key.as_bytes(), &token_id);

    let renewers: Vec<KafkaPrincipal> = req
        .renewers
        .iter()
        .map(|r| KafkaPrincipal {
            principal_type: r.principal_type.clone(),
            name: r.principal_name.clone(),
        })
        .collect();

    let expiry_ms = now + chosen_lifetime;
    let record = DelegationTokenRecord {
        token_id: token_id.clone(),
        owner: owner.clone(),
        hmac: hmac.clone(),
        issue_timestamp_ms: now,
        expiry_timestamp_ms: expiry_ms,
        max_timestamp_ms: expiry_ms,
        renewers,
    };

    if let Err(e) = controller
        .submit_change(vec![MetadataRecord::V1DelegationToken(record)])
        .await
    {
        tracing::warn!(error = %e, "CreateDelegationToken: submit_change failed");
        return err_response(crate::codes::INVALID_REQUEST);
    }

    CreateDelegationTokenResponse {
        error_code: 0,
        principal_type: owner.principal_type.clone(),
        principal_name: owner.name.clone(),
        token_requester_principal_type: owner.principal_type,
        token_requester_principal_name: owner.name,
        issue_timestamp_ms: now,
        expiry_timestamp_ms: expiry_ms,
        max_timestamp_ms: expiry_ms,
        token_id,
        hmac: bytes::Bytes::from(hmac),
        throttle_time_ms: 0,
        ..Default::default()
    }
}

fn err_response(code: i16) -> CreateDelegationTokenResponse {
    CreateDelegationTokenResponse {
        error_code: code,
        ..Default::default()
    }
}

/// Maps a runtime session [`Principal`] (auth-method + name) onto the
/// Kafka wire-level [`KafkaPrincipal`] (`principalType:name`). All
/// authenticated callers ride under `principal_type = "User"`, matching
/// Kafka's `DefaultKafkaPrincipalBuilder`.
fn principal_to_kafka(p: &Principal) -> KafkaPrincipal {
    KafkaPrincipal {
        principal_type: "User".to_string(),
        name: p.name.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabka_security::{AuthMethod, SaslMechanism};
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

    fn authed_with_token(name: &str, via_token: bool) -> ConnectionAuth {
        ConnectionAuth::Authenticated {
            principal: Principal {
                name: name.into(),
                auth_method: AuthMethod::SaslScramSha256,
                groups: vec![],
            },
            mechanism: SaslMechanism::ScramSha256,
            expires_at_ms: None,
            authenticated_via_token: via_token,
        }
    }

    fn authed(name: &str) -> ConnectionAuth {
        authed_with_token(name, false)
    }

    #[tokio::test]
    async fn returns_auth_disabled_when_no_secret_key() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let req = CreateDelegationTokenRequest::default();
        let auth = authed("alice");
        let resp = handle(&req, &auth, None, 1_000, &controller).await;
        assert_eq!(
            resp.error_code,
            crate::codes::DELEGATION_TOKEN_AUTH_DISABLED
        );
        controller.cancel().await;
    }

    #[tokio::test]
    async fn success_returns_token_id_and_hmac() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"master-key".to_vec());
        let req = CreateDelegationTokenRequest {
            max_lifetime_ms: -1,
            ..Default::default()
        };
        let resp = handle(&req, &authed("alice"), Some(&secret), 60_000, &controller).await;
        assert_eq!(resp.error_code, 0);
        assert_eq!(resp.principal_type, "User");
        assert_eq!(resp.principal_name, "alice");
        assert!(!resp.token_id.is_empty(), "token_id should be non-empty");
        // HMAC-SHA-256 output is 32 bytes; the response carries them raw.
        assert_eq!(resp.hmac.len(), 32);
        // Persisted in image with the same hmac + owner.
        let img = controller.current_image();
        let stored = img
            .delegation_token_by_id(&resp.token_id)
            .expect("token in image");
        assert_eq!(stored.hmac.as_slice(), &resp.hmac[..]);
        assert_eq!(stored.owner.principal_type, "User");
        assert_eq!(stored.owner.name, "alice");
        controller.cancel().await;
    }

    #[tokio::test]
    async fn token_authenticated_caller_is_rejected_with_request_not_allowed() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());
        let req = CreateDelegationTokenRequest {
            max_lifetime_ms: -1,
            ..Default::default()
        };
        let resp = handle(
            &req,
            &authed_with_token("alice", true),
            Some(&secret),
            60_000,
            &controller,
        )
        .await;
        assert_eq!(
            resp.error_code,
            crate::codes::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED
        );
        controller.cancel().await;
    }

    #[tokio::test]
    async fn max_lifetime_is_clamped_to_config_ceiling() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());
        // Caller requests 1 hour; broker ceiling is 5 minutes.
        let ceiling_ms = 5 * 60 * 1_000;
        let req = CreateDelegationTokenRequest {
            max_lifetime_ms: 60 * 60 * 1_000,
            ..Default::default()
        };
        let resp = handle(
            &req,
            &authed("alice"),
            Some(&secret),
            ceiling_ms,
            &controller,
        )
        .await;
        assert_eq!(resp.error_code, 0);
        let lifetime = resp.expiry_timestamp_ms - resp.issue_timestamp_ms;
        assert_eq!(lifetime, ceiling_ms);
        assert_eq!(resp.max_timestamp_ms, resp.expiry_timestamp_ms);
        controller.cancel().await;
    }

    #[tokio::test]
    async fn invalid_lifetime_returns_invalid_request() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());
        // Zero is invalid (only `-1` selects the default).
        let req = CreateDelegationTokenRequest {
            max_lifetime_ms: 0,
            ..Default::default()
        };
        let resp = handle(&req, &authed("alice"), Some(&secret), 60_000, &controller).await;
        assert_eq!(resp.error_code, crate::codes::INVALID_REQUEST);
        controller.cancel().await;
    }
}
