//! `SyncGroup` (`api_key=14`). The leader supplies assignment bytes per
//! member; non-leaders block until the leader's call arrives, then
//! receive their own assignment.

use std::collections::HashMap;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::sync_group_request::SyncGroupRequest;
use crabka_protocol::owned::sync_group_response::SyncGroupResponse;
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::coordinator::group::GroupState;
use crate::error::BrokerError;

const FOLLOWER_WAIT: Duration = Duration::from_secs(30);

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
        let req = SyncGroupRequest::decode(&mut cur, version)?;

        let Some(handle) = group_manager.find(&req.group_id) else {
            return encode_err(version, codes::UNKNOWN_MEMBER_ID);
        };

        // 1. Validate (member, generation) and check whether we're the leader.
        let is_leader = {
            let g = handle.state.lock().await;
            if !g.members.contains_key(&req.member_id) {
                return encode_err(version, codes::UNKNOWN_MEMBER_ID);
            }
            if g.generation_id != req.generation_id {
                return encode_err(version, codes::ILLEGAL_GENERATION);
            }
            g.leader_id.as_deref() == Some(&req.member_id)
        };

        if is_leader {
            // 2a. Leader supplies assignments → install + wake waiters.
            let assignments: HashMap<String, Bytes> = req
                .assignments
                .iter()
                .map(|a| (a.member_id.clone(), a.assignment.clone()))
                .collect();
            tracing::info!(
                req_assignments_count = req.assignments.len(),
                assignment_keys = ?assignments.keys().collect::<Vec<_>>(),
                assignment_lens = ?assignments.values().map(bytes::Bytes::len).collect::<Vec<_>>(),
                self_member_id = %req.member_id,
                "SyncGroup leader installing assignments"
            );
            {
                let mut g = handle.state.lock().await;
                g.install_assignments(assignments);
                tracing::info!(
                    member_keys = ?g.members.keys().collect::<Vec<_>>(),
                    member_has_assignment = ?g.members.iter().map(|(id, m)| (id.clone(), m.assignment.is_some())).collect::<Vec<_>>(),
                    "SyncGroup post-install member state"
                );
            }
            handle.sync_complete.notify_waiters();
        } else {
            // 2b. Follower blocks until the leader's SyncGroup arrives.
            let _ = tokio::time::timeout(FOLLOWER_WAIT, handle.sync_complete.notified()).await;
        }

        // 3. Read back this member's assignment.
        let (state_is_stable, assignment) = {
            let g = handle.state.lock().await;
            let stable = matches!(g.state, GroupState::Stable);
            let asn = g
                .members
                .get(&req.member_id)
                .and_then(|m| m.assignment.clone())
                .unwrap_or_default();
            (stable, asn)
        };
        if !state_is_stable {
            return encode_err(version, codes::REBALANCE_IN_PROGRESS);
        }

        let resp = SyncGroupResponse {
            error_code: codes::NONE,
            assignment,
            throttle_time_ms: 0,
            ..Default::default()
        };
        encode(version, &resp)
    })
}

fn encode_err(version: i16, code: i16) -> Result<Bytes, BrokerError> {
    let resp = SyncGroupResponse {
        error_code: code,
        assignment: Bytes::new(),
        throttle_time_ms: 0,
        ..Default::default()
    };
    encode(version, &resp)
}

fn encode(version: i16, resp: &SyncGroupResponse) -> Result<Bytes, BrokerError> {
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}
