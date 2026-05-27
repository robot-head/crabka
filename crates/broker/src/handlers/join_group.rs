//! `JoinGroup` (`api_key=11`). Blocks for up to `rebalance_timeout_ms`
//! waiting for the group to transition out of `PreparingRebalance`.

use std::time::Duration;

use bytes::{Bytes, BytesMut};
use uuid::Uuid;

use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::owned::join_group_request::JoinGroupRequest;
use crabka_protocol::owned::join_group_response::{JoinGroupResponse, JoinGroupResponseMember};
use crabka_protocol::{Decode, Encode};

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes;
use crate::coordinator::group::{AddMemberOutcome, GroupState, Member};
use crate::error::BrokerError;

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
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = JoinGroupRequest::decode(&mut cur, version)?;

    // ── slice-13 ACL preamble ────────────────────────────────────────────
    // `Read` on `Group(group_id)`. On Deny → whole-response
    // `error_code = GROUP_AUTHORIZATION_FAILED (30)`.
    {
        let image = broker.controller.current_image();
        let acl_req = AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: ResourceType::Group,
            resource_name: req.group_id.as_str(),
            operation: AclOperation::Read,
        };
        if broker.config.authorizer.authorize(&image, &acl_req) == AuthorizationResult::Deny {
            return encode(
                version,
                &JoinGroupResponse {
                    error_code: codes::GROUP_AUTHORIZATION_FAILED,
                    ..Default::default()
                },
            );
        }
    }

    // 1. Empty member_id on first join → broker generates one (KIP-394).
    //    KIP-345: for static members, derive the bootstrap id from the
    //    instance id so debug logs are readable, and check the
    //    static-members index first — if the instance id is already
    //    pinned, the broker can skip the bootstrap dance and use the
    //    existing slot's member_id.
    if req.member_id.is_empty() {
        if let Some(instance_id) = req.group_instance_id.as_deref() {
            // If the static member already has a slot, hand its current
            // `member_id` back so the client can immediately re-Join with
            // it. (Kafka's bootstrap dance still requires one round-trip,
            // but the assigned id is stable across reconnects.)
            if let Some(existing) = broker.group_manager.find(&req.group_id) {
                let g = existing.state.lock().await;
                if let Some(mid) = g.current_member_id_for_instance(instance_id) {
                    let id = mid.to_string();
                    drop(g);
                    return encode(
                        version,
                        &JoinGroupResponse {
                            error_code: codes::MEMBER_ID_REQUIRED,
                            member_id: id,
                            ..Default::default()
                        },
                    );
                }
            }
            let new_id = format!("{instance_id}-{}", Uuid::new_v4());
            return encode(
                version,
                &JoinGroupResponse {
                    error_code: codes::MEMBER_ID_REQUIRED,
                    member_id: new_id,
                    ..Default::default()
                },
            );
        }
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

    // 2. If the group already exists with a different `protocol_type`,
    //    reject. New groups (no existing handle) accept any type.
    //    KIP-559: echo the group's recorded protocol_type/name on the
    //    error response so L7 proxies stay parseable.
    if let Some(existing) = broker.group_manager.find(&req.group_id) {
        let g = existing.state.lock().await;
        if let Some(existing_type) = g.protocol_type.as_deref()
            && existing_type != req.protocol_type
        {
            let (ptype, pname) = (g.protocol_type.clone(), g.protocol_name.clone());
            drop(g);
            return encode(
                version,
                &JoinGroupResponse {
                    error_code: codes::INCONSISTENT_GROUP_PROTOCOL,
                    member_id: req.member_id,
                    protocol_type: ptype,
                    protocol_name: pname,
                    ..Default::default()
                },
            );
        }
    }

    let handle = broker.group_manager.get_or_create(&req.group_id);

    // 3. KIP-345 fence check. If the request carries a `group_instance_id`
    //    that's already pinned to a *different* live member id, reject
    //    with `FENCED_INSTANCE_ID` — another client owns this slot.
    if let Some(instance_id) = req.group_instance_id.as_deref() {
        let g = handle.state.lock().await;
        if let Some(pinned) = g.current_member_id_for_instance(instance_id)
            && pinned != req.member_id
        {
            let (ptype, pname) = (g.protocol_type.clone(), g.protocol_name.clone());
            drop(g);
            return encode(
                version,
                &JoinGroupResponse {
                    error_code: codes::FENCED_INSTANCE_ID,
                    member_id: req.member_id,
                    protocol_type: ptype,
                    protocol_name: pname,
                    ..Default::default()
                },
            );
        }
    }

    // 4. Add member. The outcome distinguishes a true join (rebalance
    //    required) from a static-rejoin into a `Stable` group (skip the
    //    rebalance and return the cached assignment immediately).
    let protocols: Vec<(String, bytes::Bytes)> = req
        .protocols
        .iter()
        .map(|p| (p.name.clone(), p.metadata.clone()))
        .collect();
    let session_timeout = Duration::from_millis(
        u64::try_from(req.session_timeout_ms).unwrap_or(DEFAULT_SESSION_TIMEOUT_MS),
    );
    let rebalance_timeout = Duration::from_millis(
        u64::try_from(req.rebalance_timeout_ms).unwrap_or(DEFAULT_REBALANCE_TIMEOUT_MS),
    );
    let static_rejoin_to_stable;
    {
        let mut g = handle.state.lock().await;
        g.protocol_type = Some(req.protocol_type.clone());
        let pre_state = g.state;
        let outcome = g.add_member(
            Member::new(
                req.member_id.clone(),
                String::new(), // client_id not threaded through the body; header-level only
                String::new(), // client_host; unused in MVP
                session_timeout,
                rebalance_timeout,
                protocols,
            )
            .with_instance_id(req.group_instance_id.clone()),
        );
        static_rejoin_to_stable = matches!(outcome, AddMemberOutcome::StaticRejoin { .. })
            && matches!(pre_state, GroupState::Stable);
        if !static_rejoin_to_stable && g.rebalance_deadline.is_none() {
            g.rebalance_deadline = Some(std::time::Instant::now() + rebalance_timeout);
        }
    }

    // 5. KIP-345 headline path: a static rejoin into a `Stable` group
    //    skips the rebalance wait entirely. Build the response from the
    //    preserved group state — the new session reclaims the cached
    //    assignment and the generation_id does NOT advance.
    if static_rejoin_to_stable {
        let g = handle.state.lock().await;
        let is_leader = g.leader_id.as_deref() == Some(&req.member_id);
        let members: Vec<JoinGroupResponseMember> = if is_leader {
            g.members
                .values()
                .map(|m| JoinGroupResponseMember {
                    member_id: m.member_id.clone(),
                    group_instance_id: m.group_instance_id.clone(),
                    metadata: m.protocol_metadata.clone(),
                    ..Default::default()
                })
                .collect()
        } else {
            Vec::new()
        };
        let resp = JoinGroupResponse {
            error_code: codes::NONE,
            generation_id: g.generation_id,
            protocol_type: g.protocol_type.clone(),
            protocol_name: g.protocol_name.clone(),
            leader: g.leader_id.clone().unwrap_or_default(),
            member_id: req.member_id,
            members,
            throttle_time_ms: 0,
            ..Default::default()
        };
        return encode(version, &resp);
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
    //    transitions the state under the mutex.) Run the vote rule
    //    over the proposed protocol sets; empty intersection surfaces
    //    `INCONSISTENT_GROUP_PROTOCOL` to this member.
    {
        let mut g = handle.state.lock().await;
        if matches!(g.state, GroupState::PreparingRebalance) && !g.members.is_empty() {
            if let Some(chosen) = crate::coordinator::group::select_protocol(&g.members) {
                g.resolve_selected_protocol_metadata(&chosen);
                g.complete_rebalance(chosen);
            } else {
                // KIP-559: echo the group's recorded protocol_type even on
                // this no-intersection error path.
                let ptype = g.protocol_type.clone();
                drop(g);
                handle.join_complete.notify_waiters();
                return encode(
                    version,
                    &JoinGroupResponse {
                        error_code: codes::INCONSISTENT_GROUP_PROTOCOL,
                        member_id: req.member_id,
                        protocol_type: ptype,
                        ..Default::default()
                    },
                );
            }
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
                    group_instance_id: m.group_instance_id.clone(),
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
