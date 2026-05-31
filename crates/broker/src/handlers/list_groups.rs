//! `ListGroups` (`api_key=16`). Returns every known group from
//! `GroupManager::list_groups`. The optional `states_filter` (v4+) is
//! honored; the optional `types_filter` (v5+) is ignored — there
//! are no group types beyond "consumer".

use bytes::{Bytes, BytesMut};

use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::owned::list_groups_request::ListGroupsRequest;
use crabka_protocol::owned::list_groups_response::{ListGroupsResponse, ListedGroup};
use crabka_protocol::{Decode, Encode};

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes;
use crate::coordinator::unified::classic_state::GroupState;
use crate::error::BrokerError;

pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = ListGroupsRequest::decode(&mut cur, version)?;
    let snapshots = broker.group_coordinator.list_groups().await;

    let image = broker.controller.current_image();

    // `states_filter` is a plain `Vec<String>` (not Option); it is empty
    // when no filter was requested (v0-v3 always decode it as empty).
    let filter_active = !req.states_filter.is_empty();

    let mut groups: Vec<ListedGroup> = Vec::with_capacity(snapshots.len());
    for s in snapshots {
        // ── ACL preamble ────────────────────────────────────
        // Per-group `Describe` check. On Deny the group is silently
        // omitted from the response (no per-group error_code). With the
        // default `AllowAllAuthorizer` every group passes; with
        // `SimpleAclAuthorizer` the super-user bypass plus matching
        // Describe ACLs let groups through; with `OpaAuthorizer` the
        // policy decides per group.
        let acl_req = AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: ResourceType::Group,
            resource_name: s.group_id.as_str(),
            operation: AclOperation::Describe,
        };
        if broker.config.authorizer.authorize(&image, &acl_req) == AuthorizationResult::Deny {
            continue;
        }

        let state_str = state_to_str(s.state);
        if filter_active && !req.states_filter.iter().any(|v| v == state_str) {
            continue;
        }
        groups.push(ListedGroup {
            group_id: s.group_id,
            protocol_type: s.protocol_type.unwrap_or_else(|| "consumer".into()),
            group_state: state_str.into(),
            ..Default::default()
        });
    }

    let resp = ListGroupsResponse {
        error_code: codes::NONE,
        groups,
        throttle_time_ms: 0,
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

fn state_to_str(s: GroupState) -> &'static str {
    match s {
        GroupState::Empty => "Empty",
        GroupState::PreparingRebalance => "PreparingRebalance",
        GroupState::CompletingRebalance => "CompletingRebalance",
        GroupState::Stable => "Stable",
        GroupState::Dead => "Dead",
    }
}
