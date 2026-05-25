//! Slice 51 (KIP-48): `DescribeDelegationToken` (`api_key` 41).
//!
//! Per spec §1.5: SASL-authenticated callers can list tokens visible
//! to them. Filtering rules:
//!   - Token-authed callers see only their own owned tokens (regardless
//!     of any owner filter — KIP-48 isolation; the ACL extension below
//!     does NOT apply to token-authed callers).
//!   - With an explicit non-empty `owners` filter: tokens whose owner
//!     matches one of the entries AND that the caller can see (owner
//!     or listed renewer, OR Describe-ACL on `TOKEN:<owner>`).
//!   - With no `owners` filter (or an empty/null one): every token
//!     where the caller is owner / listed renewer / holds the
//!     `Describe` ACL on `TOKEN:<owner>`.
//!
//! ACL-based visibility (spec §5.3): for any token whose owner has
//! been granted `Describe` on `TOKEN:<owner_principal_string>` to the
//! calling principal, include it in the visible set even if the caller
//! is not owner or renewer.

use std::net::SocketAddr;

use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::owned::describe_delegation_token_request::DescribeDelegationTokenRequest;
use crabka_protocol::owned::describe_delegation_token_response::{
    DescribeDelegationTokenResponse, DescribedDelegationToken, DescribedDelegationTokenRenewer,
};
use crabka_raft::ControllerHandle;
use crabka_security::{KafkaPrincipal, SecretBytes};

use crate::authorizer::{AuthorizationRequest, AuthorizationResult, authorize};
use crate::network::auth::ConnectionAuth;

// `async` matches the call-site shape used by every other
// `crate::handlers::*::handle`; today the body is purely synchronous.
#[allow(clippy::unused_async)]
pub(crate) async fn handle<S: std::hash::BuildHasher>(
    req: &DescribeDelegationTokenRequest,
    auth: &ConnectionAuth,
    secret_key: Option<&SecretBytes>,
    controller: &ControllerHandle,
    peer: &SocketAddr,
    super_users: &std::collections::HashSet<String, S>,
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
    let tokens: Vec<crabka_metadata::DelegationToken> = if *authenticated_via_token {
        // KIP-48: a token-authed caller is restricted to tokens they
        // own. The wire owner filter is intentionally ignored, and the
        // ACL extension below does NOT apply to token-authed callers.
        image
            .delegation_tokens_by_owner(&caller)
            .into_iter()
            .cloned()
            .collect()
    } else {
        // Step 1: tokens visible via owner / renewer (and the optional
        // owner filter, if present).
        let base: Vec<&crabka_metadata::DelegationToken> = if let Some(owners) = &candidate_owners {
            image
                .all_delegation_tokens()
                .filter(|t| {
                    owners.contains(&t.owner) && (t.owner == caller || t.renewers.contains(&caller))
                })
                .collect()
        } else {
            image.delegation_tokens_visible_to(&caller)
        };

        // Step 2 (spec §5.3): extend with tokens whose owner has
        // granted `Describe` on `TOKEN:<owner_principal_string>` to
        // the calling principal. Apply the same owner filter if one
        // was supplied so the filter remains authoritative.
        //
        // Skip the extension when no super-users and no ACLs are
        // configured — `authorize()` returns Allow unconditionally
        // in that world (a pre-existing compatibility shim) and would
        // otherwise surface every token to every caller. Delete this
        // gate together with the shim itself when slice 53/54 lands.
        let acl_extra: Vec<&crabka_metadata::DelegationToken> =
            if acl_authorization_is_active(&image, super_users) {
                image
                    .all_delegation_tokens()
                    .filter(|t| match &candidate_owners {
                        Some(owners) => owners.contains(&t.owner),
                        None => true,
                    })
                    .filter(|t| {
                        let resource = t.owner.to_string();
                        authorize(
                            &image,
                            super_users,
                            &AuthorizationRequest {
                                principal,
                                host: peer,
                                resource_type: ResourceType::DelegationToken,
                                resource_name: &resource,
                                operation: AclOperation::Describe,
                            },
                        ) == AuthorizationResult::Allow
                    })
                    .collect()
            } else {
                Vec::new()
            };

        // Merge + dedup by token_id. Order is unspecified (matches the
        // existing `delegation_tokens_*` accessor contracts).
        let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
        let mut merged: Vec<crabka_metadata::DelegationToken> = Vec::new();
        for t in base.into_iter().chain(acl_extra) {
            if seen.insert(t.token_id.as_str()) {
                merged.push(t.clone());
            }
        }
        merged
    };

    DescribeDelegationTokenResponse {
        error_code: 0,
        tokens: tokens.into_iter().map(describe_token).collect(),
        throttle_time_ms: 0,
        ..Default::default()
    }
}

/// True when the broker has anything that would make
/// [`crate::authorizer::authorize`] perform real ACL checks. The
/// existing authorizer treats "no super-users AND no ACL entries" as
/// "allow everything" (a pre-slice-13 compat shim); in that mode the
/// delegation-token Describe-via-ACL extension must be suppressed or
/// every caller would see every token. Delete this helper together
/// with that shim once slice 53/54 lands a real authorizer.
fn acl_authorization_is_active<S: std::hash::BuildHasher>(
    image: &crabka_metadata::MetadataImage,
    super_users: &std::collections::HashSet<String, S>,
) -> bool {
    !super_users.is_empty() || image.all_acls().next().is_some()
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

    fn peer() -> SocketAddr {
        "127.0.0.1:0".parse().unwrap()
    }

    fn no_super() -> std::collections::HashSet<String> {
        std::collections::HashSet::new()
    }

    async fn seed_acl(controller: &ControllerHandle, entry: crabka_metadata::AclEntry) {
        controller
            .submit_change(vec![MetadataRecord::V1AccessControlEntry(entry)])
            .await
            .expect("seed acl");
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
        let resp = handle(
            &req,
            &authed("alice"),
            None,
            &controller,
            &peer(),
            &no_super(),
        )
        .await;
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
        let resp = handle(
            &req,
            &authed("alice"),
            Some(&secret),
            &controller,
            &peer(),
            &no_super(),
        )
        .await;
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
        let resp = handle(
            &req,
            &authed("alice"),
            Some(&secret),
            &controller,
            &peer(),
            &no_super(),
        )
        .await;
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
            &peer(),
            &no_super(),
        )
        .await;
        assert_eq!(resp.error_code, 0);
        let ids: std::collections::HashSet<&str> =
            resp.tokens.iter().map(|t| t.token_id.as_str()).collect();
        assert_eq!(ids.len(), 1);
        assert!(ids.contains("t-a"));
        controller.cancel().await;
    }

    /// Slice 51 T9 / spec §5.3: a caller who is neither owner nor a
    /// listed renewer can still see a token when granted `Describe` on
    /// `TOKEN:<owner_principal_string>`. Token-authed callers do NOT
    /// pick this extension up — covered by
    /// `token_authed_caller_acl_extension_does_not_apply` below.
    #[tokio::test]
    async fn describe_grants_visibility_via_token_acl() {
        use crabka_metadata::{AclEntry, AclOperation, PatternType, PermissionType, ResourceType};
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());
        // alice owns t-a; bob has no owner/renewer relationship to it.
        seed_token(&controller, "t-a", kp("alice"), vec![]).await;
        // Grant bob `Describe` on `TOKEN:User:alice`.
        seed_acl(
            &controller,
            AclEntry {
                resource_type: ResourceType::DelegationToken,
                resource_name: "User:alice".into(),
                pattern_type: PatternType::Literal,
                principal: "User:bob".into(),
                host: "*".into(),
                operation: AclOperation::Describe,
                permission_type: PermissionType::Allow,
            },
        )
        .await;

        // bob queries with an empty filter; the ACL extension should
        // surface alice's token.
        let req = DescribeDelegationTokenRequest::default();
        let resp = handle(
            &req,
            &authed("bob"),
            Some(&secret),
            &controller,
            &peer(),
            &no_super(),
        )
        .await;
        assert_eq!(resp.error_code, 0);
        let ids: std::collections::HashSet<&str> =
            resp.tokens.iter().map(|t| t.token_id.as_str()).collect();
        assert!(
            ids.contains("t-a"),
            "expected ACL Describe on TOKEN:User:alice to make t-a visible to bob; got {ids:?}"
        );
        controller.cancel().await;
    }

    /// Token-authenticated callers stay restricted to their own owned
    /// tokens even when an ACL would otherwise extend visibility.
    #[tokio::test]
    async fn token_authed_caller_acl_extension_does_not_apply() {
        use crabka_metadata::{AclEntry, AclOperation, PatternType, PermissionType, ResourceType};
        let dir = TempDir::new().unwrap();
        let controller = test_controller(dir.path().into()).await;
        let secret = SecretBytes::new(b"k".to_vec());
        seed_token(&controller, "t-a", kp("alice"), vec![]).await;
        seed_acl(
            &controller,
            AclEntry {
                resource_type: ResourceType::DelegationToken,
                resource_name: "User:alice".into(),
                pattern_type: PatternType::Literal,
                principal: "User:bob".into(),
                host: "*".into(),
                operation: AclOperation::Describe,
                permission_type: PermissionType::Allow,
            },
        )
        .await;

        let req = DescribeDelegationTokenRequest::default();
        let resp = handle(
            &req,
            &authed_with_token("bob", true),
            Some(&secret),
            &controller,
            &peer(),
            &no_super(),
        )
        .await;
        assert_eq!(resp.error_code, 0);
        // bob owns nothing — ACL extension MUST NOT surface alice's t-a.
        assert!(
            resp.tokens.is_empty(),
            "token-authed bob must not see alice's token via ACL; got {:?}",
            resp.tokens.iter().map(|t| &t.token_id).collect::<Vec<_>>()
        );
        controller.cancel().await;
    }
}
