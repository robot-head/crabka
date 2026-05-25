//! Slice 51 (KIP-48): `DescribeDelegationToken` (`api_key` 41).
//!
//! Per spec §1.5: SASL-authenticated callers can list tokens visible
//! to them. Filtering rules:
//!   - Token-authed callers see only their own owned tokens (regardless
//!     of any owner filter — KIP-48 isolation).
//!   - With an explicit non-empty `owners` filter: tokens whose owner
//!     matches one of the entries AND that the caller can see (owner
//!     or listed renewer).
//!   - With no `owners` filter (or an empty/null one): every token
//!     where the caller is owner or a renewer.
//!
//! ACL-based visibility (the `Describe` operation on `TOKEN:<owner>`)
//! is T9's job; this handler currently only honours owner+renewer
//! visibility.

use crabka_protocol::owned::describe_delegation_token_request::DescribeDelegationTokenRequest;
use crabka_protocol::owned::describe_delegation_token_response::{
    DescribeDelegationTokenResponse, DescribedDelegationToken, DescribedDelegationTokenRenewer,
};
use crabka_raft::ControllerHandle;
use crabka_security::{KafkaPrincipal, SecretBytes};

use crate::network::auth::ConnectionAuth;

// `async` matches the call-site shape used by every other
// `crate::handlers::*::handle` and lets T9 add ACL lookups without
// changing the signature; today the body is purely synchronous.
#[allow(clippy::unused_async)]
pub(crate) async fn handle(
    req: &DescribeDelegationTokenRequest,
    auth: &ConnectionAuth,
    secret_key: Option<&SecretBytes>,
    controller: &ControllerHandle,
) -> DescribeDelegationTokenResponse {
    if secret_key.is_none() {
        return err_response(crate::codes::DELEGATION_TOKEN_AUTH_DISABLED);
    }
    let ConnectionAuth::Authenticated {
        principal,
        authenticated_via_token,
        ..
    } = auth
    else {
        return err_response(crate::codes::INVALID_REQUEST);
    };
    let caller = principal.to_kafka();

    let image = controller.current_image();

    // Optional owner filter: present + non-empty selects tokens whose
    // owner matches one of the entries. A missing or empty list means
    // "no filter".
    let candidate_owners: Option<Vec<KafkaPrincipal>> = match &req.owners {
        Some(list) if !list.is_empty() => Some(
            list.iter()
                .map(|o| KafkaPrincipal {
                    principal_type: o.principal_type.clone(),
                    name: o.principal_name.clone(),
                })
                .collect(),
        ),
        _ => None,
    };

    // Build the visible-token set per the rules above.
    // TODO (T9): extend visibility with ACL-based `Describe` permission
    // on `TOKEN:<owner_name>` so cluster admins can see tokens they
    // neither own nor renew.
    let tokens: Vec<crabka_metadata::DelegationToken> = if *authenticated_via_token {
        // KIP-48: a token-authed caller is restricted to tokens they
        // own. The wire owner filter is intentionally ignored.
        image
            .delegation_tokens_by_owner(&caller)
            .into_iter()
            .cloned()
            .collect()
    } else if let Some(owners) = candidate_owners {
        image
            .all_delegation_tokens()
            .filter(|t| {
                owners.contains(&t.owner) && (t.owner == caller || t.renewers.contains(&caller))
            })
            .cloned()
            .collect()
    } else {
        image
            .delegation_tokens_visible_to(&caller)
            .into_iter()
            .cloned()
            .collect()
    };

    DescribeDelegationTokenResponse {
        error_code: 0,
        tokens: tokens.into_iter().map(describe_token).collect(),
        throttle_time_ms: 0,
        ..Default::default()
    }
}

fn describe_token(t: crabka_metadata::DelegationToken) -> DescribedDelegationToken {
    DescribedDelegationToken {
        principal_type: t.owner.principal_type.clone(),
        principal_name: t.owner.name.clone(),
        // KIP-48 token-requester = owner; we don't support the
        // privileged "act-as" path so these are always equal.
        token_requester_principal_type: t.owner.principal_type,
        token_requester_principal_name: t.owner.name,
        issue_timestamp: t.issue_timestamp_ms,
        expiry_timestamp: t.expiry_timestamp_ms,
        max_timestamp: t.max_timestamp_ms,
        token_id: t.token_id,
        hmac: bytes::Bytes::from(t.hmac),
        renewers: t
            .renewers
            .into_iter()
            .map(|r| DescribedDelegationTokenRenewer {
                principal_type: r.principal_type,
                principal_name: r.name,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

fn err_response(code: i16) -> DescribeDelegationTokenResponse {
    DescribeDelegationTokenResponse {
        error_code: code,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabka_metadata::{DelegationTokenRecord, MetadataRecord};
    use crabka_protocol::owned::describe_delegation_token_request::DescribeDelegationTokenOwner;
    use crabka_security::{AuthMethod, Principal, SaslMechanism};
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

    fn kp(name: &str) -> KafkaPrincipal {
        KafkaPrincipal {
            principal_type: "User".into(),
            name: name.into(),
        }
    }

    async fn seed_token(
        controller: &ControllerHandle,
        token_id: &str,
        owner: KafkaPrincipal,
        renewers: Vec<KafkaPrincipal>,
    ) {
        let rec = DelegationTokenRecord {
            token_id: token_id.into(),
            owner,
            hmac: vec![0u8; 32],
            issue_timestamp_ms: 1_000,
            expiry_timestamp_ms: 2_000,
            max_timestamp_ms: 3_000,
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
        let req = DescribeDelegationTokenRequest::default();
        let resp = handle(&req, &authed("alice"), None, &controller).await;
        assert_eq!(
            resp.error_code,
            crate::codes::DELEGATION_TOKEN_AUTH_DISABLED
        );
        controller.cancel().await;
    }

    #[tokio::test]
    async fn empty_filter_returns_all_tokens_visible_to_caller() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());
        // alice owns t-a; bob owns t-b; alice is a renewer on t-b.
        seed_token(&controller, "t-a", kp("alice"), vec![]).await;
        seed_token(&controller, "t-b", kp("bob"), vec![kp("alice")]).await;
        // carol owns an unrelated token — alice should not see it.
        seed_token(&controller, "t-c", kp("carol"), vec![]).await;

        let req = DescribeDelegationTokenRequest::default();
        let resp = handle(&req, &authed("alice"), Some(&secret), &controller).await;
        assert_eq!(resp.error_code, 0);
        let ids: std::collections::HashSet<&str> =
            resp.tokens.iter().map(|t| t.token_id.as_str()).collect();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains("t-a"));
        assert!(ids.contains("t-b"));
        controller.cancel().await;
    }

    #[tokio::test]
    async fn owner_filter_intersects_with_visibility() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());
        seed_token(&controller, "t-a", kp("alice"), vec![]).await;
        // bob's token: alice is a renewer.
        seed_token(&controller, "t-b", kp("bob"), vec![kp("alice")]).await;
        // carol's token: alice has no relationship.
        seed_token(&controller, "t-c", kp("carol"), vec![]).await;

        // Ask for tokens owned by either bob or carol. alice can see
        // t-b (renewer) but not t-c (no relationship).
        let req = DescribeDelegationTokenRequest {
            owners: Some(vec![
                DescribeDelegationTokenOwner {
                    principal_type: "User".into(),
                    principal_name: "bob".into(),
                    ..Default::default()
                },
                DescribeDelegationTokenOwner {
                    principal_type: "User".into(),
                    principal_name: "carol".into(),
                    ..Default::default()
                },
            ]),
            ..Default::default()
        };
        let resp = handle(&req, &authed("alice"), Some(&secret), &controller).await;
        assert_eq!(resp.error_code, 0);
        let ids: std::collections::HashSet<&str> =
            resp.tokens.iter().map(|t| t.token_id.as_str()).collect();
        assert_eq!(ids.len(), 1);
        assert!(ids.contains("t-b"));
        controller.cancel().await;
    }

    #[tokio::test]
    async fn token_authed_caller_sees_only_own_owned_tokens() {
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());
        // alice owns t-a; bob owns t-b; alice is a renewer on t-b.
        seed_token(&controller, "t-a", kp("alice"), vec![]).await;
        seed_token(&controller, "t-b", kp("bob"), vec![kp("alice")]).await;

        // Wire owner filter asks for bob's tokens — but a token-authed
        // alice is restricted to her own owned set regardless.
        let req = DescribeDelegationTokenRequest {
            owners: Some(vec![DescribeDelegationTokenOwner {
                principal_type: "User".into(),
                principal_name: "bob".into(),
                ..Default::default()
            }]),
            ..Default::default()
        };
        let resp = handle(
            &req,
            &authed_with_token("alice", true),
            Some(&secret),
            &controller,
        )
        .await;
        assert_eq!(resp.error_code, 0);
        let ids: std::collections::HashSet<&str> =
            resp.tokens.iter().map(|t| t.token_id.as_str()).collect();
        assert_eq!(ids.len(), 1);
        assert!(ids.contains("t-a"));
        controller.cancel().await;
    }
}
