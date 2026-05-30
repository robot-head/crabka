//! Classic-protocol Join/Sync/Heartbeat/Leave/offset-validate logic against
//! [`ClassicState`]. Pure transitions that return a *disposition* the actor
//! turns into either an immediate reply or a parked `oneshot`. Ported verbatim
//! from the old `handlers/{join_group,sync_group,heartbeat,leave_group}.rs`
//! and `offset_commit::validate`; the actor (`super::actor`) owns the
//! park/wake plumbing and the rebalance-deadline timer that drives completion.

use std::time::{Duration, Instant};

use bytes::Bytes;
use uuid::Uuid;

use crabka_protocol::owned::heartbeat_request::HeartbeatRequest;
use crabka_protocol::owned::join_group_request::JoinGroupRequest;
use crabka_protocol::owned::leave_group_request::LeaveGroupRequest;
use crabka_protocol::owned::leave_group_response::MemberResponse;
use crabka_protocol::owned::sync_group_request::SyncGroupRequest;

use crate::codes;

use super::actor::{JoinResult, JoinResultMember, SyncResult};
use super::classic_state::{
    AddMemberOutcome, Group as ClassicState, GroupState, Member, select_protocol,
};

const DEFAULT_SESSION_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_REBALANCE_TIMEOUT_MS: u64 = 60_000;
/// Mirror of Apache Kafka's `group.initial.rebalance.delay.ms` default. Used
/// as the rebalance-completion deadline so a single-member group completes
/// quickly instead of holding the full client-supplied `rebalance_timeout_ms`.
pub(super) const INITIAL_REBALANCE_DELAY: Duration = Duration::from_secs(3);

// ── JoinGroup ───────────────────────────────────────────────────────────────

/// What the actor should do with a `ClassicJoin`.
pub(super) enum JoinAction {
    /// Reply right away (fast paths: `MEMBER_ID_REQUIRED`, validation errors,
    /// static-rejoin into a `Stable` group).
    Immediate(JoinResult),
    /// Park the reply; `state.rebalance_deadline` has been set (the actor
    /// completes the rebalance when it fires).
    Park,
    /// Every still-live member has joined this round and the group has
    /// rebalanced before (`generation_id > 0`): complete the rebalance now and
    /// drain all parked joiners (mirrors the old `wake_other_joiners` path).
    CompleteNow,
}

/// Port of `handlers/join_group.rs` steps 1–6, operating on `ClassicState`.
pub(super) fn handle_join(
    state: &mut ClassicState,
    req: &JoinGroupRequest,
    client_host: &str,
) -> JoinAction {
    // 1. Empty member_id on first join → broker generates one (KIP-394).
    //    KIP-345: derive the bootstrap id from the instance id, and return the
    //    existing slot's member_id if the instance is already pinned.
    if req.member_id.is_empty() {
        let member_id = if let Some(instance_id) = req.group_instance_id.as_deref() {
            match state.current_member_id_for_instance(instance_id) {
                Some(mid) => mid.to_string(),
                None => format!("{instance_id}-{}", Uuid::new_v4()),
            }
        } else {
            format!("crabka-{}", Uuid::new_v4())
        };
        return JoinAction::Immediate(JoinResult {
            error_code: codes::MEMBER_ID_REQUIRED,
            member_id,
            ..JoinResult::default()
        });
    }

    // 2. protocol_type mismatch on an existing group → INCONSISTENT (KIP-559 echo).
    if let Some(existing_type) = state.protocol_type.as_deref()
        && existing_type != req.protocol_type
    {
        return JoinAction::Immediate(JoinResult {
            error_code: codes::INCONSISTENT_GROUP_PROTOCOL,
            member_id: req.member_id.clone(),
            protocol_type: state.protocol_type.clone(),
            protocol_name: state.protocol_name.clone(),
            ..JoinResult::default()
        });
    }

    // 3. KIP-345 fence: instance id pinned to a different live member id.
    if let Some(instance_id) = req.group_instance_id.as_deref()
        && let Some(pinned) = state.current_member_id_for_instance(instance_id)
        && pinned != req.member_id
    {
        return JoinAction::Immediate(JoinResult {
            error_code: codes::FENCED_INSTANCE_ID,
            member_id: req.member_id.clone(),
            protocol_type: state.protocol_type.clone(),
            protocol_name: state.protocol_name.clone(),
            ..JoinResult::default()
        });
    }

    // 4. Add member.
    let protocols: Vec<(String, Bytes)> = req
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
    state.protocol_type = Some(req.protocol_type.clone());
    let pre_state = state.state;
    let outcome = state.add_member(
        Member::new(
            req.member_id.clone(),
            String::new(), // client_id is header-level only
            client_host.to_string(),
            session_timeout,
            rebalance_timeout,
            protocols,
        )
        .with_instance_id(req.group_instance_id.clone()),
    );
    let static_rejoin_to_stable = matches!(outcome, AddMemberOutcome::StaticRejoin { .. })
        && matches!(pre_state, GroupState::Stable);
    // Open the rebalance window: the deadline drives completion in the actor,
    // anchored at the first join. Use the SHORTER of the client's
    // rebalance_timeout and INITIAL_REBALANCE_DELAY (the effective wait the
    // old per-handler `tokio::time::timeout` used).
    if !static_rejoin_to_stable && state.rebalance_deadline.is_none() {
        state.rebalance_deadline =
            Some(Instant::now() + rebalance_timeout.min(INITIAL_REBALANCE_DELAY));
    }

    // 5. Static rejoin into a `Stable` group: skip the rebalance entirely.
    if static_rejoin_to_stable {
        return JoinAction::Immediate(build_join_result(state, &req.member_id));
    }

    // 6. Early-complete once every still-live member has rejoined this round —
    //    but only after the first rebalance (gen > 0); the very first rebalance
    //    burns the full INITIAL_REBALANCE_DELAY (Kafka's batching semantic).
    let complete_now = state.generation_id > 0
        && matches!(state.state, GroupState::PreparingRebalance)
        && state.all_members_joined_this_round();
    if complete_now {
        JoinAction::CompleteNow
    } else {
        JoinAction::Park
    }
}

/// Build a successful `JoinResult` from post-rebalance state (leader gets the
/// member list; followers get an empty list).
pub(super) fn build_join_result(state: &ClassicState, member_id: &str) -> JoinResult {
    let is_leader = state.leader_id.as_deref() == Some(member_id);
    let members = if is_leader {
        state
            .members
            .values()
            .map(|m| JoinResultMember {
                member_id: m.member_id.clone(),
                group_instance_id: m.group_instance_id.clone(),
                metadata: m.protocol_metadata.clone(),
            })
            .collect()
    } else {
        Vec::new()
    };
    JoinResult {
        error_code: codes::NONE,
        generation_id: state.generation_id,
        protocol_type: state.protocol_type.clone(),
        protocol_name: state.protocol_name.clone(),
        leader: state.leader_id.clone().unwrap_or_default(),
        member_id: member_id.to_string(),
        members,
    }
}

/// Run the rebalance-completion vote. `Ok(())` if the round completed (or there
/// was nothing to complete), `Err(())` if the protocol intersection was empty
/// (`INCONSISTENT_GROUP_PROTOCOL`). Mirrors `join_group.rs` block 5.
pub(super) fn try_complete(state: &mut ClassicState) -> Result<(), ()> {
    if matches!(state.state, GroupState::PreparingRebalance) && !state.members.is_empty() {
        if let Some(chosen) = select_protocol(&state.members) {
            state.resolve_selected_protocol_metadata(&chosen);
            state.complete_rebalance(chosen);
            Ok(())
        } else {
            Err(())
        }
    } else {
        Ok(())
    }
}

// ── SyncGroup ─────────────────────────────────────────────────────────────

/// What the actor should do with a `ClassicSync`.
pub(super) enum SyncAction {
    /// Reply right away (validation error, or a follower while `Stable`).
    Immediate(SyncResult),
    /// Park the follower until the leader's `SyncGroup` installs assignments.
    Park,
    /// The leader installed assignments: reply this result to the leader and
    /// drain the parked followers.
    LeaderInstalled(SyncResult),
}

/// Port of `handlers/sync_group.rs`, operating on `ClassicState`.
pub(super) fn handle_sync(state: &mut ClassicState, req: &SyncGroupRequest) -> SyncAction {
    let protocol_type = state.protocol_type.clone();
    let protocol_name = state.protocol_name.clone();

    // KIP-345 fence.
    if req.group_instance_id.as_deref().is_some_and(|iid| {
        state
            .current_member_id_for_instance(iid)
            .is_none_or(|pinned| pinned != req.member_id)
    }) {
        return SyncAction::Immediate(sync_err(
            codes::FENCED_INSTANCE_ID,
            protocol_type,
            protocol_name,
        ));
    }
    if !state.members.contains_key(&req.member_id) {
        return SyncAction::Immediate(sync_err(
            codes::UNKNOWN_MEMBER_ID,
            protocol_type,
            protocol_name,
        ));
    }
    if state.generation_id != req.generation_id {
        return SyncAction::Immediate(sync_err(
            codes::ILLEGAL_GENERATION,
            protocol_type,
            protocol_name,
        ));
    }

    let is_leader = state.leader_id.as_deref() == Some(&req.member_id);
    if is_leader {
        let assignments = req
            .assignments
            .iter()
            .map(|a| (a.member_id.clone(), a.assignment.clone()))
            .collect();
        state.install_assignments(assignments);
        SyncAction::LeaderInstalled(read_sync_result(
            state,
            &req.member_id,
            protocol_type,
            protocol_name,
        ))
    } else if matches!(state.state, GroupState::Stable) {
        SyncAction::Immediate(read_sync_result(
            state,
            &req.member_id,
            protocol_type,
            protocol_name,
        ))
    } else {
        SyncAction::Park
    }
}

/// Read back one member's installed assignment. Mirrors `sync_group.rs` step 3:
/// `REBALANCE_IN_PROGRESS` if the group is not `Stable`.
pub(super) fn read_sync_result(
    state: &ClassicState,
    member_id: &str,
    protocol_type: Option<String>,
    protocol_name: Option<String>,
) -> SyncResult {
    if !matches!(state.state, GroupState::Stable) {
        return sync_err(codes::REBALANCE_IN_PROGRESS, protocol_type, protocol_name);
    }
    let assignment = state
        .members
        .get(member_id)
        .and_then(|m| m.assignment.clone())
        .unwrap_or_default();
    SyncResult {
        error_code: codes::NONE,
        assignment,
        protocol_type,
        protocol_name,
    }
}

fn sync_err(code: i16, protocol_type: Option<String>, protocol_name: Option<String>) -> SyncResult {
    SyncResult {
        error_code: code,
        assignment: Bytes::new(),
        protocol_type,
        protocol_name,
    }
}

// ── Heartbeat ───────────────────────────────────────────────────────────────

/// Port of `handlers/heartbeat.rs`. Returns the error code; refreshes
/// `last_heartbeat` on success.
pub(super) fn handle_heartbeat(state: &mut ClassicState, req: &HeartbeatRequest) -> i16 {
    let instance_fenced = req.group_instance_id.as_deref().is_some_and(|iid| {
        state
            .current_member_id_for_instance(iid)
            .is_none_or(|pinned| pinned != req.member_id)
    });
    if instance_fenced {
        codes::FENCED_INSTANCE_ID
    } else if !state.members.contains_key(&req.member_id) {
        codes::UNKNOWN_MEMBER_ID
    } else if state.generation_id != req.generation_id {
        codes::ILLEGAL_GENERATION
    } else if !matches!(state.state, GroupState::Stable) {
        codes::REBALANCE_IN_PROGRESS
    } else {
        state
            .members
            .get_mut(&req.member_id)
            .expect("contains_key checked above")
            .last_heartbeat = Instant::now();
        codes::NONE
    }
}

// ── LeaveGroup ──────────────────────────────────────────────────────────────

struct MemberIdentityIn {
    member_id: String,
    group_instance_id: Option<String>,
}

/// Port of `handlers/leave_group.rs`. Removes the resolved members and, if the
/// group was `Stable` with survivors, reopens a rebalance (sets the deadline).
/// Returns the per-member responses.
pub(super) fn handle_leave(
    state: &mut ClassicState,
    req: &LeaveGroupRequest,
    version: i16,
) -> Vec<MemberResponse> {
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
    for ident in &inputs {
        let (resolved_id, code): (Option<String>, i16) =
            match (ident.group_instance_id.as_deref(), ident.member_id.as_str()) {
                (Some(iid), "") => match state.current_member_id_for_instance(iid) {
                    Some(pinned) => (Some(pinned.to_string()), codes::NONE),
                    None => (None, codes::UNKNOWN_MEMBER_ID),
                },
                (Some(iid), mid) => match state.current_member_id_for_instance(iid) {
                    Some(pinned) if pinned == mid => (Some(pinned.to_string()), codes::NONE),
                    Some(_) => (None, codes::FENCED_INSTANCE_ID),
                    None => (None, codes::UNKNOWN_MEMBER_ID),
                },
                (None, mid) => {
                    if state.members.contains_key(mid) {
                        (Some(mid.to_string()), codes::NONE)
                    } else {
                        (None, codes::UNKNOWN_MEMBER_ID)
                    }
                }
            };
        if let Some(id) = resolved_id {
            state.remove_member(&id);
            any_removed = true;
        }
        member_responses.push(MemberResponse {
            member_id: ident.member_id.clone(),
            group_instance_id: ident.group_instance_id.clone(),
            error_code: code,
            ..Default::default()
        });
    }
    if any_removed && !state.members.is_empty() && matches!(state.state, GroupState::Stable) {
        state.state = GroupState::PreparingRebalance;
        state.rebalance_deadline = Some(
            Instant::now()
                + state
                    .members
                    .values()
                    .map(|m| m.rebalance_timeout)
                    .max()
                    .unwrap_or(Duration::from_mins(1)),
        );
    }
    member_responses
}

// ── OffsetCommit validation ───────────────────────────────────────────────

/// Port of `offset_commit::validate`. Returns `Some(code)` to reject.
pub(super) fn validate_commit(
    state: &ClassicState,
    member_id: &str,
    group_instance_id: Option<&str>,
    generation_id: i32,
) -> Option<i16> {
    if member_id.is_empty() && group_instance_id.is_none() {
        return None; // simple consumer
    }
    if let Some(iid) = group_instance_id {
        match state.current_member_id_for_instance(iid) {
            None => return Some(codes::UNKNOWN_MEMBER_ID),
            Some(pinned) => {
                if !member_id.is_empty() && pinned != member_id {
                    return Some(codes::FENCED_INSTANCE_ID);
                }
            }
        }
    } else if !state.members.contains_key(member_id) {
        return Some(codes::UNKNOWN_MEMBER_ID);
    }
    if state.generation_id != generation_id {
        return Some(codes::ILLEGAL_GENERATION);
    }
    None
}
