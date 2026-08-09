//! `DescribeGroups` (`api_key=15`). The response holds one entry per requested
//! `group_id`.
//!
//! Each member carries its `JoinGroup` protocol metadata (`member_metadata`)
//! and its current assignment bytes. The group reports its selected protocol
//! name (`protocol_data`) and its stored `protocol_type`. That type is `""`
//! for a typeless or dead group, which matches Kafka.
//!
//! KIP-430: when the request sets `include_authorized_operations`, each Allow
//! row carries a bitfield of the group operations that the principal may
//! perform. A row that fails the auth check, and a row for a group that does
//! not exist, keeps the `i32::MIN` "not present" sentinel.

use bytes::Bytes;
use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::{
    Decode,
    owned::{
        describe_groups_request::DescribeGroupsRequest,
        describe_groups_response::{DescribeGroupsResponse, DescribedGroup, DescribedGroupMember},
    },
};
use tokio::sync::oneshot;

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
    coordinator::unified::{
        GroupType, classic_state::GroupState, streams::actor::StreamsGroupActorMessage,
    },
    error::BrokerError,
    handlers::authorized_operations::authorized_operations_bits,
};

#[tracing::instrument(
    name = "handle_describe_groups",
    level = "info",
    skip_all,
    fields(api = "DescribeGroups", version, req_bytes = req_bytes.len()),
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
        if broker.config.authorizer.authorize(&*image, &acl_req) == AuthorizationResult::Deny {
            groups.push(DescribedGroup {
                group_id: gid,
                error_code: codes::GROUP_AUTHORIZATION_FAILED,
                ..Default::default()
            });
            continue;
        }
        if let Some(error_code) = crate::handlers::group_coordinator_error(broker, &gid) {
            groups.push(DescribedGroup {
                group_id: gid,
                error_code,
                ..Default::default()
            });
            continue;
        }

        // KIP-1071: a Streams-locked group's offset home is a drained classic
        // actor; describing it via the classic projection would mislabel it.
        // Report its streams identity (full task detail lives in
        // StreamsGroupDescribe, api 89). Exact protocol_type/state is matched
        // empirically (spec §7.4); the firm contract is "not classic/consumer".
        if broker.group_coordinator.group_type(&gid) == Some(GroupType::Streams)
            && let Some(handle) = broker.group_coordinator.find_streams(&gid)
        {
            let (tx, rx) = oneshot::channel();
            if handle
                .tx
                .send(StreamsGroupActorMessage::Describe { reply: tx })
                .await
                .is_ok()
                && let Ok(view) = rx.await
            {
                groups.push(DescribedGroup {
                    group_id: gid,
                    protocol_type: "streams".into(),
                    group_state: view.group_state,
                    error_code: codes::NONE,
                    ..Default::default()
                });
                continue;
            }
            // Streams-locked but no live streams actor (e.g. just downgraded) →
            // fall through to the classic describe path below.
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
                // MemberSnapshot.{protocol_metadata,assignment} are Vec<u8>;
                // wire type is Bytes.
                member_metadata: m.protocol_metadata.into(),
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
            // Kafka returns "" for a typeless/dead group; real consumer
            // groups already carry Some("consumer").
            protocol_type: snap.protocol_type.clone().unwrap_or_default(),
            // Selected protocol NAME (e.g. "range"); "" for an empty group.
            protocol_data: snap.protocol_name.clone().unwrap_or_default(),
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
    crate::handlers::encode_response(&resp, version)
}

fn state_to_str(s: GroupState) -> &'static str {
    match s {
        GroupState::Empty => "Empty",
        GroupState::PreparingRebalance => "PreparingRebalance",
        GroupState::CompletingRebalance => "CompletingRebalance",
        GroupState::Stable => "Stable",
    }
}
