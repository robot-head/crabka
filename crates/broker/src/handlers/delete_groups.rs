//! `DeleteGroups` (`api_key=42`). Drops empty groups from the in-memory
//! registry. Non-empty groups are rejected with `NON_EMPTY_GROUP`.

use bytes::{Bytes, BytesMut};

use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::owned::delete_groups_request::DeleteGroupsRequest;
use crabka_protocol::owned::delete_groups_response::{DeletableGroupResult, DeleteGroupsResponse};
use crabka_protocol::{Decode, Encode};

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes;
use crate::coordinator::DeleteGroupError;
use crate::error::BrokerError;

#[tracing::instrument(
    name = "handle_delete_groups",
    level = "info",
    skip_all,
    fields(api = "DeleteGroups", version, req_bytes = req_bytes.len()),
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
    let req = DeleteGroupsRequest::decode(&mut cur, version)?;

    let image = broker.controller.current_image();

    let mut results: Vec<DeletableGroupResult> = Vec::with_capacity(req.groups_names.len());
    for gid in req.groups_names {
        // ── ACL preamble ────────────────────────────────────
        // Per-group `Delete` check. On Deny → per-group
        // `error_code = GROUP_AUTHORIZATION_FAILED (30)`.
        let acl_req = AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: ResourceType::Group,
            resource_name: gid.as_str(),
            operation: AclOperation::Delete,
        };
        if broker.config.authorizer.authorize(&*image, &acl_req) == AuthorizationResult::Deny {
            results.push(DeletableGroupResult {
                group_id: gid,
                error_code: codes::GROUP_AUTHORIZATION_FAILED,
                ..Default::default()
            });
            continue;
        }

        let error_code = match broker.group_coordinator.delete_group(&gid).await {
            Ok(()) => codes::NONE,
            Err(DeleteGroupError::NotFound) => codes::GROUP_ID_NOT_FOUND,
            Err(DeleteGroupError::NonEmpty) => codes::NON_EMPTY_GROUP,
            Err(DeleteGroupError::Internal) => codes::UNKNOWN_SERVER_ERROR,
        };
        results.push(DeletableGroupResult {
            group_id: gid,
            error_code,
            ..Default::default()
        });
    }

    let resp = DeleteGroupsResponse {
        results,
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
    use crabka_protocol::Decode;
    use crabka_security::{AuthMethod, Principal};
    use std::net::SocketAddr;
    use std::sync::Arc;

    use crate::authorizer::{AuthorizationRequest, Authorizer};
    use crate::config::BrokerConfig;

    const VERSION: i16 = 2;

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

    fn request(groups: &[&str]) -> DeleteGroupsRequest {
        DeleteGroupsRequest {
            groups_names: groups.iter().map(|g| (*g).into()).collect(),
            ..Default::default()
        }
    }

    fn encode_request(req: &DeleteGroupsRequest) -> Bytes {
        let mut buf = BytesMut::with_capacity(req.encoded_len(VERSION));
        req.encode(&mut buf, VERSION).expect("encode request");
        buf.freeze()
    }

    fn decode_response(bytes: Bytes) -> DeleteGroupsResponse {
        let mut cur: &[u8] = bytes.as_ref();
        let resp = DeleteGroupsResponse::decode(&mut cur, VERSION).expect("decode response");
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

    fn principal(name: &str) -> Principal {
        Principal {
            name: name.into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        }
    }

    fn peer() -> SocketAddr {
        "127.0.0.1:9092".parse().unwrap()
    }

    async fn start_broker(
        authorizer: Arc<dyn Authorizer>,
    ) -> (crate::broker::BrokerHandle, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
        cfg.audit_enabled = false;
        cfg.authorizer = authorizer;
        let handle = Broker::start(cfg).await.expect("start broker");
        (handle, dir)
    }

    async fn drive(
        broker: &Broker,
        req: &DeleteGroupsRequest,
        principal: &Principal,
        peer: &SocketAddr,
    ) -> DeleteGroupsResponse {
        let ctx = test_context(principal, peer);
        let req_bytes = encode_request(req);
        let bytes = handle(broker, VERSION, 123, &req_bytes, &ctx)
            .await
            .expect("handle");
        decode_response(bytes)
    }

    #[tokio::test]
    async fn handle_denies_delete_for_each_group() {
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("alice");
        let peer = peer();
        let req = request(&["group-a", "group-b"]);

        let resp = drive(&broker, &req, &p, &peer).await;

        assert!(resp.throttle_time_ms == 0);
        assert!(resp.results.len() == 2);
        assert!(resp.results[0].group_id == "group-a");
        assert!(resp.results[0].error_code == codes::GROUP_AUTHORIZATION_FAILED);
        assert!(resp.results[1].group_id == "group-b");
        assert!(resp.results[1].error_code == codes::GROUP_AUTHORIZATION_FAILED);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_allowed_missing_group_returns_not_found() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("admin");
        let peer = peer();
        let req = request(&["missing"]);

        let resp = drive(&broker, &req, &p, &peer).await;

        assert!(resp.throttle_time_ms == 0);
        assert!(resp.results.len() == 1);
        assert!(resp.results[0].group_id == "missing");
        assert!(resp.results[0].error_code == codes::GROUP_ID_NOT_FOUND);
        broker_handle.shutdown().await;
    }
}
