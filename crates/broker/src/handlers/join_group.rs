//! `JoinGroup` (`api_key=11`). Blocks for up to `rebalance_timeout_ms`
//! waiting for the group to transition out of `PreparingRebalance`.

use std::net::SocketAddr;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use uuid::Uuid;

use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::owned::join_group_request::JoinGroupRequest;
use crabka_protocol::owned::join_group_response::{JoinGroupResponse, JoinGroupResponseMember};
use crabka_protocol::{Decode, Encode};
use crabka_security::Principal;

use crate::authorizer::{AuthorizationRequest, AuthorizationResult, authorize};
use crate::broker::Broker;
use crate::codes;
use crate::coordinator::group::{GroupState, Member};
use crate::error::BrokerError;

const SUPPORTED_PROTOCOL: &str = "range";
const DEFAULT_SESSION_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_REBALANCE_TIMEOUT_MS: u64 = 60_000;
/// Mirror of Apache Kafka's `group.initial.rebalance.delay.ms` default.
/// Used as the `JoinGroup` wait so a single-member group completes quickly
/// instead of holding the full client-supplied `rebalance_timeout_ms`.
const INITIAL_REBALANCE_DELAY: Duration = Duration::from_secs(3);

#[allow(clippy::too_many_lines)] // rebalance state machine + ACL preamble; splitting hurts readability
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    principal: &Principal,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = JoinGroupRequest::decode(&mut cur, version)?;

    // ── slice-13 ACL preamble ────────────────────────────────────────────
    // `Read` on `Group(group_id)`. On Deny → whole-response
    // `error_code = GROUP_AUTHORIZATION_FAILED (30)`.
    {
        let image = broker.controller.current_image();
        let acl_req = AuthorizationRequest {
            principal,
            host: peer,
            resource_type: ResourceType::Group,
            resource_name: req.group_id.as_str(),
            operation: AclOperation::Read,
        };
        if authorize(&image, &broker.config.super_users, &acl_req) == AuthorizationResult::Deny {
            return encode(
                version,
                &JoinGroupResponse {
                    error_code: codes::GROUP_AUTHORIZATION_FAILED,
                    ..Default::default()
                },
            );
        }
    }

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

    let handle = broker.group_manager.get_or_create(&req.group_id);

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

    // 4. Wait on the per-group join-complete notify.
    //
    // Real Kafka uses `group.initial.rebalance.delay.ms` (default 3 s)
    // for the WAIT, not `rebalance_timeout_ms` (which is the cap on
    // total rebalance time; the JVM consumer sends 5 minutes there).
    // Use the SHORTER of `rebalance_timeout` and the initial-rebalance
    // delay so multi-member rebalances still batch new joins but a
    // single member completes quickly.
    let wait = rebalance_timeout.min(INITIAL_REBALANCE_DELAY);
    let _ = tokio::time::timeout(wait, handle.join_complete.notified()).await;

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
}

fn encode(version: i16, resp: &JoinGroupResponse) -> Result<Bytes, BrokerError> {
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}
