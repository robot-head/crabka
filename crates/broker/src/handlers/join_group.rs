//! `JoinGroup` (`api_key=11`). Blocks for up to `rebalance_timeout_ms`
//! waiting for the group to transition out of `PreparingRebalance`.

use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;
use uuid::Uuid;

use crabka_protocol::owned::join_group_request::JoinGroupRequest;
use crabka_protocol::owned::join_group_response::{JoinGroupResponse, JoinGroupResponseMember};
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::coordinator::group::{GroupState, Member};
use crate::error::BrokerError;

const SUPPORTED_PROTOCOL: &str = "range";
const DEFAULT_SESSION_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_REBALANCE_TIMEOUT_MS: u64 = 60_000;

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
        let req = JoinGroupRequest::decode(&mut cur, version)?;

        // 1. Reject proposals that don't include `range`. (For the MVP we
        //    only negotiate `range`; we don't run a real protocol-set
        //    intersection.)
        let proposes_range = req.protocols.iter().any(|p| p.name == SUPPORTED_PROTOCOL);
        if !proposes_range {
            return encode(
                version,
                &JoinGroupResponse {
                    error_code: codes::INCONSISTENT_GROUP_PROTOCOL,
                    member_id: req.member_id,
                    ..Default::default()
                },
            );
        }

        // 2. Empty member_id on first join → broker generates one (KIP-394).
        if req.member_id.is_empty() {
            let new_id = format!("crabka-{}", Uuid::new_v4());
            return encode(
                version,
                &JoinGroupResponse {
                    error_code: codes::MEMBER_ID_REQUIRED,
                    member_id: new_id,
                    ..Default::default()
                },
            );
        }

        let handle = group_manager.get_or_create(&req.group_id);

        // 3. Add member, transition to PreparingRebalance, set deadline.
        let protocol_md = req
            .protocols
            .iter()
            .find(|p| p.name == SUPPORTED_PROTOCOL)
            .map(|p| p.metadata.clone())
            .unwrap_or_default();
        let session_timeout = Duration::from_millis(
            u64::try_from(req.session_timeout_ms).unwrap_or(DEFAULT_SESSION_TIMEOUT_MS),
        );
        let rebalance_timeout = Duration::from_millis(
            u64::try_from(req.rebalance_timeout_ms).unwrap_or(DEFAULT_REBALANCE_TIMEOUT_MS),
        );
        {
            let mut g = handle.state.lock().await;
            g.protocol_type = Some(req.protocol_type.clone());
            g.add_member(Member::new(
                req.member_id.clone(),
                String::new(), // client_id not threaded through the body; header-level only
                String::new(), // client_host; unused in MVP
                session_timeout,
                rebalance_timeout,
                protocol_md,
            ));
            if g.rebalance_deadline.is_none() {
                g.rebalance_deadline = Some(std::time::Instant::now() + rebalance_timeout);
            }
        }

        // 4. Wait on the per-group join-complete notify, with a deadline.
        let _ = tokio::time::timeout(rebalance_timeout, handle.join_complete.notified()).await;

        // 5. Complete the rebalance if we're the one who fell out of the
        //    wait first. (Multiple JoinGroup handlers race; whoever wins
        //    transitions the state under the mutex.)
        {
            let mut g = handle.state.lock().await;
            if matches!(g.state, GroupState::PreparingRebalance) && !g.members.is_empty() {
                g.complete_rebalance(SUPPORTED_PROTOCOL);
                handle.join_complete.notify_waiters();
            }
        }

        // 6. Build the response from the post-rebalance state.
        let resp = {
            let g = handle.state.lock().await;
            let is_leader = g.leader_id.as_deref() == Some(&req.member_id);
            let members: Vec<JoinGroupResponseMember> = if is_leader {
                g.members
                    .values()
                    .map(|m| JoinGroupResponseMember {
                        member_id: m.member_id.clone(),
                        metadata: m.protocol_metadata.clone(),
                        ..Default::default()
                    })
                    .collect()
            } else {
                Vec::new()
            };
            JoinGroupResponse {
                error_code: codes::NONE,
                generation_id: g.generation_id,
                protocol_type: g.protocol_type.clone(),
                protocol_name: g.protocol_name.clone(),
                leader: g.leader_id.clone().unwrap_or_default(),
                member_id: req.member_id,
                members,
                throttle_time_ms: 0,
                ..Default::default()
            }
        };

        encode(version, &resp)
    })
}

fn encode(version: i16, resp: &JoinGroupResponse) -> Result<Bytes, BrokerError> {
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}
