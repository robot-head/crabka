//! `DescribeGroups` (`api_key=15`). One entry per requested `group_id`.
//! Members include their current assignment bytes; the `protocol_type` is
//! reported from the group's stored value (defaulting to "consumer").

use std::net::SocketAddr;

use bytes::{Bytes, BytesMut};

use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::owned::describe_groups_request::DescribeGroupsRequest;
use crabka_protocol::owned::describe_groups_response::{
    DescribeGroupsResponse, DescribedGroup, DescribedGroupMember,
};
use crabka_protocol::{Decode, Encode};
use crabka_security::Principal;

use crate::authorizer::{AuthorizationRequest, AuthorizationResult, authorize};
use crate::broker::Broker;
use crate::codes;
use crate::coordinator::group::GroupState;
use crate::error::BrokerError;

pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    principal: &Principal,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = DescribeGroupsRequest::decode(&mut cur, version)?;

    let image = broker.controller.current_image();

    let mut groups: Vec<DescribedGroup> = Vec::with_capacity(req.groups.len());
    for gid in req.groups {
        // ── slice-13 ACL preamble ────────────────────────────────────
        // Per-group `Describe` check. On Deny → per-group
        // `error_code = GROUP_AUTHORIZATION_FAILED (30)`.
        let acl_req = AuthorizationRequest {
            principal,
            host: peer,
            resource_type: ResourceType::Group,
            resource_name: gid.as_str(),
            operation: AclOperation::Describe,
        };
        if authorize(&image, &broker.config.super_users, &acl_req) == AuthorizationResult::Deny {
            groups.push(DescribedGroup {
                group_id: gid,
                error_code: codes::GROUP_AUTHORIZATION_FAILED,
                ..Default::default()
            });
            continue;
        }

        let Some(snap) = broker.group_manager.describe_group(&gid).await else {
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
        groups.push(DescribedGroup {
            group_id: snap.group_id,
            protocol_type: snap.protocol_type.unwrap_or_else(|| "consumer".into()),
            protocol_data: String::new(),
            group_state: state_str.into(),
            error_code: codes::NONE,
            members,
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
