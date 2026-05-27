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

/// Normalised `(member_id, group_instance_id)` pair from either the
/// v0–v2 single-member shape or the v3+ `MemberIdentity` list.
struct MemberIdentityIn {
    member_id: String,
    group_instance_id: Option<String>,
}

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

        // v0-v2 uses the single `member_id`; v3+ uses the `members` list
        // of `MemberIdentity { member_id, group_instance_id, reason }`.
        // Normalize both shapes into a single `Vec<MemberIdentityIn>`.
        let inputs: Vec<MemberIdentityIn> = if version >= 3 {
            req.members
                .iter()
                .map(|m| MemberIdentityIn {
                    member_id: m.member_id.clone(),
                    group_instance_id: m.group_instance_id.clone(),
                })
                .collect()
        } else {
            vec![MemberIdentityIn {
                member_id: req.member_id.clone(),
                group_instance_id: None,
            }]
        };

        let mut member_responses: Vec<MemberResponse> = Vec::with_capacity(inputs.len());
        let mut any_removed = false;
        {
            let mut g = handle.state.lock().await;
            for ident in &inputs {
                // KIP-345 resolution rules:
                // - instance_id set + member_id empty → look up via static
                //   index. Missing → UNKNOWN_MEMBER_ID.
                // - instance_id set + member_id set → look up via static
                //   index. Mismatch → FENCED_INSTANCE_ID.
                // - instance_id None → look up by member_id directly.
                let (resolved_id, code): (Option<String>, i16) =
                    match (ident.group_instance_id.as_deref(), ident.member_id.as_str()) {
                        (Some(iid), "") => match g.current_member_id_for_instance(iid) {
                            Some(pinned) => (Some(pinned.to_string()), codes::NONE),
                            None => (None, codes::UNKNOWN_MEMBER_ID),
                        },
                        (Some(iid), mid) => match g.current_member_id_for_instance(iid) {
                            Some(pinned) if pinned == mid => {
                                (Some(pinned.to_string()), codes::NONE)
                            }
                            Some(_) => (None, codes::FENCED_INSTANCE_ID),
                            None => (None, codes::UNKNOWN_MEMBER_ID),
                        },
                        (None, mid) => {
                            if g.members.contains_key(mid) {
                                (Some(mid.to_string()), codes::NONE)
                            } else {
                                (None, codes::UNKNOWN_MEMBER_ID)
                            }
                        }
                    };

                if let Some(id) = resolved_id {
                    g.remove_member(&id);
                    any_removed = true;
                }
                member_responses.push(MemberResponse {
                    member_id: ident.member_id.clone(),
                    group_instance_id: ident.group_instance_id.clone(),
                    error_code: code,
                    ..Default::default()
                });
            }
            // KIP-345: LeaveGroup is an *intentional* removal — even for
            // static members, it triggers a rebalance if the group is
            // still Stable with surviving members. (The suppression rule
            // only applies to session-timeout-driven removal.)
            if any_removed && !g.members.is_empty() && matches!(g.state, GroupState::Stable) {
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
