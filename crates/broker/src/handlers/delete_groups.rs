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
        throttle_time_ms: 0,
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}
