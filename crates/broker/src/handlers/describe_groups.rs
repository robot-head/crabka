//! `DescribeGroups` (`api_key=15`). One entry per requested `group_id`.
//! Members include their current assignment bytes; the `protocol_type` is
//! reported from the group's stored value (defaulting to "consumer").
//!
//! KIP-430: when `include_authorized_operations` is set on the request,
//! each Allow row carries a bitfield of the group operations the
//! principal may perform; rows that auth-fail or aren't found stay at
//! the `i32::MIN` "not present" sentinel.

use bytes::{Bytes, BytesMut};

use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::owned::describe_groups_request::DescribeGroupsRequest;
use crabka_protocol::owned::describe_groups_response::{
    DescribeGroupsResponse, DescribedGroup, DescribedGroupMember,
};
use crabka_protocol::{Decode, Encode};

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes;
use crate::coordinator::unified::classic_state::GroupState;
use crate::error::BrokerError;
use crate::handlers::authorized_operations::authorized_operations_bits;

pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = DescribeGroupsRequest::decode(&mut cur, version)?;

    let image = broker.controller.current_image();

    let mut groups: Vec<DescribedGroup> = Vec::with_capacity(req.groups.len());
    for gid in req.groups {
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
        if broker.config.authorizer.authorize(&image, &acl_req) == AuthorizationResult::Deny {
            groups.push(DescribedGroup {
                group_id: gid,
                error_code: codes::GROUP_AUTHORIZATION_FAILED,
                ..Default::default()
            });
            continue;
        }

        let Some(snap) = broker.group_coordinator.describe_group(&gid).await else {
            groups.push(DescribedGroup {
                group_id: gid,
                error_code: codes::GROUP_ID_NOT_FOUND,
                ..Default::default()
            });
            continue;
        };
        let state_str = state_to_str(snap.state);
        let members = snap
            .members
            .into_iter()
            .map(|m| DescribedGroupMember {
                member_id: m.member_id,
                client_id: m.client_id,
                client_host: m.client_host,
                // MemberSnapshot.assignment is Vec<u8>; wire type is Bytes.
                member_assignment: m.assignment.into(),
                ..Default::default()
            })
            .collect();
        // KIP-430: bitfield of group operations alice@host is authorized
        // for, when the request opted in. Otherwise the wire-default
        // `i32::MIN` "not present" sentinel is preserved.
        let authorized = if req.include_authorized_operations {
            authorized_operations_bits(
                broker.config.authorizer.as_ref(),
                &image,
                ctx.principal,
                ctx.peer,
                ResourceType::Group,
                snap.group_id.as_str(),
            )
        } else {
            i32::MIN
        };
        groups.push(DescribedGroup {
            group_id: snap.group_id,
            protocol_type: snap.protocol_type.unwrap_or_else(|| "consumer".into()),
            protocol_data: String::new(),
            group_state: state_str.into(),
            error_code: codes::NONE,
            members,
            authorized_operations: authorized,
            ..Default::default()
        });
    }

    let resp = DescribeGroupsResponse {
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
