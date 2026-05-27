//! `SyncGroup` (`api_key=14`). The leader supplies assignment bytes per
//! member; non-leaders block until the leader's call arrives, then
//! receive their own assignment.
//!
//! KIP-559 (v5+): the response carries `protocol_type` + `protocol_name`
//! so an L7 proxy can route the call without remembering the prior
//! `JoinGroup` exchange. The codegen drops both fields on v < 5, so
//! emitting them on every path is harmless on older versions.

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
            return encode_err(version, codes::UNKNOWN_MEMBER_ID, None, None);
        };

        // 1. Validate (member, generation) and check whether we're the leader.
        //    Snapshot the group's negotiated (protocol_type, protocol_name)
        //    so KIP-559 can echo them on every response below — including
        //    the error paths.
        let (is_leader, protocol_type, protocol_name) = {
            let g = handle.state.lock().await;
            // KIP-345 fence: instance id pinned elsewhere → reject.
            if req.group_instance_id.as_deref().is_some_and(|iid| {
                g.current_member_id_for_instance(iid)
                    .is_none_or(|pinned| pinned != req.member_id)
            }) {
                return encode_err(
                    version,
                    codes::FENCED_INSTANCE_ID,
                    g.protocol_type.clone(),
                    g.protocol_name.clone(),
                );
            }
            if !g.members.contains_key(&req.member_id) {
                return encode_err(
                    version,
                    codes::UNKNOWN_MEMBER_ID,
                    g.protocol_type.clone(),
                    g.protocol_name.clone(),
                );
            }
            if g.generation_id != req.generation_id {
                return encode_err(
                    version,
                    codes::ILLEGAL_GENERATION,
                    g.protocol_type.clone(),
                    g.protocol_name.clone(),
                );
            }
            (
                g.leader_id.as_deref() == Some(&req.member_id),
                g.protocol_type.clone(),
                g.protocol_name.clone(),
            )
        };

        if is_leader {
            // 2a. Leader supplies assignments → install + wake waiters.
            let assignments: HashMap<String, Bytes> = req
                .assignments
                .iter()
                .map(|a| (a.member_id.clone(), a.assignment.clone()))
                .collect();
            {
                let mut g = handle.state.lock().await;
                g.install_assignments(assignments);
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
            return encode_err(
                version,
                codes::REBALANCE_IN_PROGRESS,
                protocol_type,
                protocol_name,
            );
        }

        // KIP-559: the wire fields are nullable on v5+. Echo whatever
        // the JoinGroup that preceded this SyncGroup recorded; null
        // (None) is the schema-level default for the rare path where a
        // group reaches SyncGroup without a recorded protocol (the
        // member-existence checks above already gate this, but the
        // null default is defensive).
        let resp = SyncGroupResponse {
            error_code: codes::NONE,
            assignment,
            throttle_time_ms: 0,
            protocol_type,
            protocol_name,
            ..Default::default()
        };
        encode(version, &resp)
    })
}

fn encode_err(
    version: i16,
    code: i16,
    protocol_type: Option<String>,
    protocol_name: Option<String>,
) -> Result<Bytes, BrokerError> {
    let resp = SyncGroupResponse {
        error_code: code,
        assignment: Bytes::new(),
        throttle_time_ms: 0,
        protocol_type,
        protocol_name,
        ..Default::default()
    };
    encode(version, &resp)
}

fn encode(version: i16, resp: &SyncGroupResponse) -> Result<Bytes, BrokerError> {
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}
