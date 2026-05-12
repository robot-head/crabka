//! `LeaveGroup` (`api_key=13`). Removes one or more members and transitions
//! the group to `PreparingRebalance` if it still has members.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::leave_group_request::LeaveGroupRequest;
use crabka_protocol::owned::leave_group_response::{LeaveGroupResponse, MemberResponse};
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
        let req = LeaveGroupRequest::decode(&mut cur, version)?;

        let Some(handle) = group_manager.find(&req.group_id) else {
            // No such group; respond OK but no member responses.
            let resp = LeaveGroupResponse {
                error_code: codes::NONE,
                throttle_time_ms: 0,
                members: vec![],
                ..Default::default()
            };
            return encode(version, &resp);
        };

        // v0-v2 uses the single `member_id`; v3+ uses the `members` list of
        // (member_id, group_instance_id). Build a unified `Vec<String>`.
        let to_remove: Vec<String> = if req.member_id.is_empty() {
            req.members.iter().map(|m| m.member_id.clone()).collect()
        } else {
            vec![req.member_id.clone()]
        };

        let mut member_responses: Vec<MemberResponse> = Vec::with_capacity(to_remove.len());
        {
            let mut g = handle.state.lock().await;
            for mid in &to_remove {
                let code = if g.members.contains_key(mid) {
                    g.remove_member(mid);
                    codes::NONE
                } else {
                    codes::UNKNOWN_MEMBER_ID
                };
                member_responses.push(MemberResponse {
                    member_id: mid.clone(),
                    group_instance_id: None,
                    error_code: code,
                    ..Default::default()
                });
            }
            // If group still has members and was Stable, kick a new rebalance.
            if !g.members.is_empty() && matches!(g.state, GroupState::Stable) {
                g.state = GroupState::PreparingRebalance;
                g.rebalance_deadline = Some(
                    std::time::Instant::now()
                        + g.members
                            .values()
                            .map(|m| m.rebalance_timeout)
                            .max()
                            .unwrap_or(std::time::Duration::from_mins(1)),
                );
            }
        }
        // Wake any JoinGroup handlers that are parked on this group; they
        // need to observe the membership change.
        handle.join_complete.notify_waiters();

        let resp = LeaveGroupResponse {
            error_code: codes::NONE,
            throttle_time_ms: 0,
            members: member_responses,
            ..Default::default()
        };
        encode(version, &resp)
    })
}

fn encode(version: i16, resp: &LeaveGroupResponse) -> Result<Bytes, BrokerError> {
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}
