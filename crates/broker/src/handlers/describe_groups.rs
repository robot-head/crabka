//! `DescribeGroups` (`api_key=15`). One entry per requested `group_id`.
//! Members include their current assignment bytes; the `protocol_type` is
//! reported from the group's stored value (defaulting to "consumer").

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::describe_groups_request::DescribeGroupsRequest;
use crabka_protocol::owned::describe_groups_response::{
    DescribedGroup, DescribedGroupMember, DescribeGroupsResponse,
};
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::coordinator::group::GroupState;
use crate::error::BrokerError;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let group_manager = broker.group_manager.clone();

    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = DescribeGroupsRequest::decode(&mut cur, version)?;

        let mut groups: Vec<DescribedGroup> = Vec::with_capacity(req.groups.len());
        for gid in req.groups {
            let Some(snap) = group_manager.describe_group(&gid).await else {
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
    })
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
