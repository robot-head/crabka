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
    use crabka_security::Principal;
    use std::net::SocketAddr;
    use std::sync::Arc;

    use crate::test_support::{DenyAll, peer, principal};

    const VERSION: i16 = 2;

    fn request(groups: &[&str]) -> DeleteGroupsRequest {
        DeleteGroupsRequest {
            groups_names: groups.iter().map(|g| (*g).into()).collect(),
            ..Default::default()
        }
    }

    crate::test_support::wire_helpers!(
        DeleteGroupsRequest,
        DeleteGroupsResponse,
        version = VERSION,
        client_id = "admin-client"
    );

    use crate::test_support::start_broker_with_authorizer_no_audit as start_broker;

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
        decode_response(&bytes)
    }

    #[tokio::test]
    async fn handle_denies_delete_for_each_group() {
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("alice");
        let peer = peer();
        let req = request(&["group-a", "group-b"]);

        let resp = drive(&broker, &req, &p, &peer).await;

        let expected = DeleteGroupsResponse {
            throttle_time_ms: 0,
            results: vec![
                DeletableGroupResult {
                    group_id: "group-a".to_string(),
                    error_code: codes::GROUP_AUTHORIZATION_FAILED,
                    unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![]),
                },
                DeletableGroupResult {
                    group_id: "group-b".to_string(),
                    error_code: codes::GROUP_AUTHORIZATION_FAILED,
                    unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![]),
                },
            ],
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(resp == expected);
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

        let expected = DeleteGroupsResponse {
            throttle_time_ms: 0,
            results: vec![DeletableGroupResult {
                group_id: "missing".to_string(),
                error_code: codes::GROUP_ID_NOT_FOUND,
                unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![]),
            }],
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }
}
