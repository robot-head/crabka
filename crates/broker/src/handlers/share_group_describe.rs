//! `ShareGroupDescribe` (`api_key` 77) — KIP-932. Returns one
//! `DescribedGroup` per requested `group_id`, rendered from the share
//! actor's `Describe` view.
//!
//! Intercepted inline in `network::dispatch` (not `build_table`) so the
//! handler receives the per-connection principal + peer `SocketAddr` for the
//! per-group `Describe` ACL gate.

use bytes::{Bytes, BytesMut};
use tokio::sync::oneshot;

use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::owned::share_group_describe_request::ShareGroupDescribeRequest;
use crabka_protocol::owned::share_group_describe_response::{
    DescribedGroup, ShareGroupDescribeResponse,
};
use crabka_protocol::{Decode, Encode};

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes;
use crate::coordinator::unified::share::actor::ShareGroupActorMessage;
use crate::error::BrokerError;

#[tracing::instrument(
    name = "handle_share_group_describe",
    level = "info",
    skip_all,
    fields(api = "ShareGroupDescribe", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = ShareGroupDescribeRequest::decode(&mut cur, version)?;

    let image = broker.controller.current_image();
    let share_enabled = broker.config.share_group.enable;
    let ng_opt = Some(broker.group_coordinator.clone());

    let mut groups: Vec<DescribedGroup> = Vec::with_capacity(req.group_ids.len());
    for gid in &req.group_ids {
        // ── ACL preamble ────────────────────────────────────
        // Per-group `Describe` check. On Deny → per-group
        // `error_code = GROUP_AUTHORIZATION_FAILED (30)`.
        let acl_req = AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: ResourceType::Group,
            resource_name: gid.as_str(),
            operation: AclOperation::Describe,
        };
        if broker.config.authorizer.authorize(&*image, &acl_req) == AuthorizationResult::Deny {
            groups.push(DescribedGroup {
                group_id: gid.clone(),
                error_code: codes::GROUP_AUTHORIZATION_FAILED,
                ..Default::default()
            });
            continue;
        }

        if !share_enabled {
            groups.push(DescribedGroup {
                group_id: gid.clone(),
                error_code: codes::GROUP_ID_NOT_FOUND,
                ..Default::default()
            });
            continue;
        }

        let Some(handle) = ng_opt.as_ref().and_then(|ng| ng.find_share(gid)) else {
            groups.push(DescribedGroup {
                group_id: gid.clone(),
                error_code: codes::GROUP_ID_NOT_FOUND,
                ..Default::default()
            });
            continue;
        };

        let (tx, rx) = oneshot::channel();
        if handle
            .tx
            .send(ShareGroupActorMessage::Describe { reply: tx })
            .await
            .is_err()
        {
            groups.push(DescribedGroup {
                group_id: gid.clone(),
                error_code: codes::COORDINATOR_LOAD_IN_PROGRESS,
                ..Default::default()
            });
            continue;
        }
        match rx.await {
            Ok(view) => groups.push(view.into_described_group()),
            Err(_) => groups.push(DescribedGroup {
                group_id: gid.clone(),
                error_code: codes::UNKNOWN_SERVER_ERROR,
                ..Default::default()
            }),
        }
    }

    let resp = ShareGroupDescribeResponse {
        groups,
        throttle_time_ms: 0,
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use crabka_protocol::owned::share_group_describe_response;
    use crabka_security::{AuthMethod, Principal};
    use std::net::SocketAddr;
    use std::sync::Arc;

    use crate::authorizer::{AuthorizationRequest, Authorizer};
    use crate::config::BrokerConfig;

    #[derive(Debug)]
    struct DenyAll;

    impl Authorizer for DenyAll {
        fn authorize(
            &self,
            _source: &dyn crabka_authz::AclSource,
            _req: &AuthorizationRequest<'_>,
        ) -> AuthorizationResult {
            AuthorizationResult::Deny
        }
    }

    fn request(group_ids: &[&str]) -> ShareGroupDescribeRequest {
        ShareGroupDescribeRequest {
            group_ids: group_ids.iter().map(|g| (*g).to_string()).collect(),
            include_authorized_operations: false,
            ..Default::default()
        }
    }

    fn encode_request(req: &ShareGroupDescribeRequest) -> Bytes {
        let version = share_group_describe_response::MAX_VERSION;
        let mut buf = BytesMut::with_capacity(req.encoded_len(version));
        req.encode(&mut buf, version).expect("encode request");
        buf.freeze()
    }

    fn decode_response(bytes: &Bytes) -> ShareGroupDescribeResponse {
        let version = share_group_describe_response::MAX_VERSION;
        let mut cur: &[u8] = bytes.as_ref();
        let resp = ShareGroupDescribeResponse::decode(&mut cur, version).expect("decode response");
        assert!(cur.is_empty(), "response decoder consumed all bytes");
        resp
    }

    fn test_context<'a>(
        principal: &'a Principal,
        peer: &'a SocketAddr,
    ) -> crate::handlers::RequestContext<'a> {
        crate::handlers::RequestContext {
            principal,
            peer,
            client_id: "admin-client",
            sendfile_capable: false,
            connection_listener_name: "PLAINTEXT",
        }
    }

    async fn start_broker(
        authorizer: Arc<dyn Authorizer>,
        share_enabled: bool,
    ) -> (crate::broker::BrokerHandle, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
        cfg.authorizer = authorizer;
        cfg.share_group.enable = share_enabled;
        let handle = Broker::start(cfg).await.expect("start broker");
        (handle, dir)
    }

    fn principal() -> Principal {
        Principal {
            name: "alice".into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        }
    }

    #[tokio::test]
    async fn handle_denied_groups_preserve_group_ids_and_error_codes() {
        let version = share_group_describe_response::MAX_VERSION;
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll), true).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let req_bytes = encode_request(&request(&["g1", "g2"]));

        let resp = handle(&broker, version, 1, &req_bytes, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&resp);

        assert!(resp.throttle_time_ms == 0);
        assert!(resp.groups.len() == 2, "{resp:?}");
        assert!(resp.groups[0].group_id == "g1");
        assert!(resp.groups[0].error_code == codes::GROUP_AUTHORIZATION_FAILED);
        assert!(resp.groups[1].group_id == "g2");
        assert!(resp.groups[1].error_code == codes::GROUP_AUTHORIZATION_FAILED);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_disabled_feature_wins_even_when_share_actor_exists() {
        let version = share_group_describe_response::MAX_VERSION;
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer), false).await;
        let broker = broker_handle.broker_arc_for_test();
        broker.group_coordinator.mark_share("g1");
        let _actor = broker.group_coordinator.get_or_create_share("g1");
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let req_bytes = encode_request(&request(&["g1"]));

        let resp = handle(&broker, version, 1, &req_bytes, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&resp);

        assert!(resp.throttle_time_ms == 0);
        assert!(resp.groups.len() == 1, "{resp:?}");
        assert!(resp.groups[0].group_id == "g1");
        assert!(resp.groups[0].error_code == codes::GROUP_ID_NOT_FOUND);
        assert!(resp.groups[0].members.is_empty());
        broker_handle.shutdown().await;
    }
}
