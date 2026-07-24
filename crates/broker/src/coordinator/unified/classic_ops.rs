//! Classic-protocol Join/Sync/Heartbeat/Leave/offset-validate logic against
//! [`ClassicState`]. Pure transitions that return a *disposition* the actor
//! turns into either an immediate reply or a parked `oneshot`. Ported verbatim
//! from the old `handlers/{join_group,sync_group,heartbeat,leave_group}.rs`
//! and `offset_commit::validate`; the actor (`super::actor`) owns the
//! park/wake plumbing and the rebalance-deadline timer that drives completion.

use std::time::{Duration, Instant};

use bytes::Bytes;
use crabka_protocol::owned::{
    heartbeat_request::HeartbeatRequest, join_group_request::JoinGroupRequest,
    leave_group_request::LeaveGroupRequest, leave_group_response::MemberResponse,
    sync_group_request::SyncGroupRequest,
};
use uuid::Uuid;

use super::{
    actor::{JoinResult, JoinResultMember, SyncResult},
    classic_state::{
        AddMemberOutcome, ClassicGroup as ClassicState, GroupState, Member, select_protocol,
    },
};
use crate::codes;

const DEFAULT_SESSION_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_REBALANCE_TIMEOUT_MS: u64 = 60_000;
// ── JoinGroup ───────────────────────────────────────────────────────────────

/// What the actor should do with a `ClassicJoin`.
pub(super) enum JoinAction {
    /// Reply right away (fast paths: `MEMBER_ID_REQUIRED`, validation errors,
    /// static-rejoin into a `Stable` group).
    Immediate(JoinResult),
    /// Park the reply; `state.rebalance_deadline` has been set (the actor
    /// completes the rebalance when it fires).
    Park,
    /// Every still-live member has joined this round and the round was
    /// triggered by a membership change in a group that still had members
    /// (not a start-from-`Empty` herd): complete the rebalance now and drain
    /// all parked joiners (mirrors the old `wake_other_joiners` path).
    CompleteNow,
}

/// Port of `handlers/join_group.rs` steps 1–6, operating on `ClassicState`.
pub(super) fn handle_join(
    state: &mut ClassicState,
    req: &JoinGroupRequest,
    client_host: &str,
    initial_rebalance_delay: Duration,
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

    // 2b. KIP-345 fence: a known member_id must keep a consistent
    // group.instance.id. A join that reuses an existing member's id with a
    // different instance nature — e.g. a dynamic rejoin of a static member, or a
    // switch between instances — would otherwise overwrite the member in
    // `add_member` and orphan its `static_members` index entry, permanently
    // pinning the old instance id (a non-conforming client could lock a slot
    // forever). Apache Kafka validates the registered instance against the
    // request and fences the mismatch.
    if let Some(existing) = state.members.get(&req.member_id)
        && existing.group_instance_id.as_deref() != req.group_instance_id.as_deref()
    {
        return JoinAction::Immediate(JoinResult {
            error_code: codes::FENCED_INSTANCE_ID,
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
    // rebalance_timeout and the configured initial delay (the effective wait the
    // old per-handler `tokio::time::timeout` used).
    if !static_rejoin_to_stable && state.rebalance_deadline.is_none() {
        state.rebalance_deadline =
            Some(Instant::now() + rebalance_timeout.min(initial_rebalance_delay));
    }

    // 5. Static rejoin into a `Stable` group: skip the rebalance entirely.
    if static_rejoin_to_stable {
        return JoinAction::Immediate(build_join_result(state, &req.member_id));
    }

    // 6. Early-complete once every still-live member has rejoined this round —
    //    but only for a rebalance triggered by a membership change in a group
    //    that still had members. A round that opened from `Empty` (a fresh
    //    group, or one whose members all left — e.g. after a warm-up consumer
    //    joins and leaves) burns the full initial delay so a herd of
    //    consumers starting together batches into one generation, mirroring
    //    Kafka's `InitialDelayedJoin`. Eager-completing a from-`Empty` round
    //    strands the first joiner in a solo generation and forces an immediate
    //    re-rebalance when the next member arrives, which thrashes
    //    produce+fetch under concurrent load.
    let complete_now = !state.rebalance_from_empty
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
                member_id: m.id.clone(),
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
        // A member left a live group: this is a membership-change rebalance,
        // not a start-from-empty herd, so the survivors eager-complete.
        state.rebalance_from_empty = false;
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

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use crabka_protocol::owned::{
        join_group_request::JoinGroupRequestProtocol, leave_group_request::MemberIdentity,
        sync_group_request::SyncGroupRequestAssignment,
    };

    use super::*;

    fn handle_join(
        state: &mut ClassicState,
        req: &JoinGroupRequest,
        client_host: &str,
    ) -> JoinAction {
        super::handle_join(state, req, client_host, Duration::from_secs(3))
    }

    fn join_req(member_id: &str, instance: Option<&str>) -> JoinGroupRequest {
        JoinGroupRequest {
            group_id: "g".into(),
            session_timeout_ms: 30_000,
            rebalance_timeout_ms: 60_000,
            member_id: member_id.into(),
            group_instance_id: instance.map(String::from),
            protocol_type: "consumer".into(),
            protocols: vec![JoinGroupRequestProtocol {
                name: "range".into(),
                metadata: Bytes::from_static(b"meta"),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    // ── handle_join ─────────────────────────────────────────────────────────

    #[test]
    fn join_empty_member_id_dynamic_returns_member_id_required() {
        let mut g = ClassicState::new("g");
        let action = handle_join(&mut g, &join_req("", None), "h");
        match action {
            JoinAction::Immediate(r) => {
                assert!(r.error_code == codes::MEMBER_ID_REQUIRED);
                assert!(r.member_id.starts_with("crabka-"));
            }
            _ => panic!("expected Immediate MEMBER_ID_REQUIRED"),
        }
    }

    #[test]
    fn join_empty_member_id_static_derives_from_instance() {
        let mut g = ClassicState::new("g");
        let action = handle_join(&mut g, &join_req("", Some("inst-a")), "h");
        match action {
            JoinAction::Immediate(r) => {
                assert!(r.error_code == codes::MEMBER_ID_REQUIRED);
                assert!(r.member_id.starts_with("inst-a-"));
            }
            _ => panic!("expected Immediate MEMBER_ID_REQUIRED"),
        }
    }

    #[test]
    fn join_protocol_type_mismatch_is_inconsistent() {
        let mut g = ClassicState::new("g");
        g.protocol_type = Some("connect".into());
        let action = handle_join(&mut g, &join_req("m1", None), "h");
        match action {
            JoinAction::Immediate(r) => assert!(r.error_code == codes::INCONSISTENT_GROUP_PROTOCOL),
            _ => panic!("expected Immediate INCONSISTENT_GROUP_PROTOCOL"),
        }
    }

    #[test]
    fn join_fenced_instance_id() {
        let mut g = ClassicState::new("g");
        // Pin inst-a to m1 via a first join.
        let _ = handle_join(&mut g, &join_req("m1", Some("inst-a")), "h");
        // A different member id claiming the same instance is fenced.
        let action = handle_join(&mut g, &join_req("m2", Some("inst-a")), "h");
        match action {
            JoinAction::Immediate(r) => assert!(r.error_code == codes::FENCED_INSTANCE_ID),
            _ => panic!("expected Immediate FENCED_INSTANCE_ID"),
        }
    }

    #[test]
    fn join_new_member_parks_and_opens_deadline() {
        let mut g = ClassicState::new("g");
        let action = handle_join(&mut g, &join_req("m1", None), "h");
        assert!(matches!(action, JoinAction::Park));
        check!(g.rebalance_deadline.is_some());
        check!(g.state == GroupState::PreparingRebalance);
    }

    #[test]
    fn join_uses_configured_initial_rebalance_delay() {
        let mut g = ClassicState::new("g");
        let before = Instant::now();
        let action = super::handle_join(
            &mut g,
            &join_req("m1", None),
            "h",
            Duration::from_millis(17),
        );
        let after = Instant::now();

        assert!(matches!(action, JoinAction::Park));
        let deadline = g.rebalance_deadline.expect("rebalance deadline");
        assert!(deadline >= before + Duration::from_millis(17));
        assert!(deadline <= after + Duration::from_millis(17));
    }

    #[test]
    fn join_static_rejoin_to_stable_is_immediate_success() {
        let mut g = ClassicState::new("g");
        let _ = handle_join(&mut g, &join_req("m1", Some("inst-a")), "h");
        g.complete_rebalance("range");
        let mut a = std::collections::HashMap::new();
        a.insert("m1".to_string(), Bytes::from_static(b"asn"));
        g.install_assignments(a);
        assert!(g.state == GroupState::Stable);
        // Rejoin the same instance with the pinned member_id (KIP-345: a
        // restarted static client re-supplies the id it got back from the
        // MEMBER_ID_REQUIRED round-trip) → static rejoin into Stable skips the
        // rebalance and returns the cached assignment.
        let action = handle_join(&mut g, &join_req("m1", Some("inst-a")), "h");
        match action {
            JoinAction::Immediate(r) => {
                assert!(r.error_code == codes::NONE);
                assert!(r.generation_id == g.generation_id);
            }
            _ => panic!("expected Immediate success (static rejoin)"),
        }
    }

    #[test]
    fn join_all_members_rejoined_completes_now() {
        let mut g = ClassicState::new("g");
        let _ = handle_join(&mut g, &join_req("m1", None), "h");
        g.complete_rebalance("range"); // gen 1, Stable-ish path
        g.state = GroupState::Stable;
        // m2 joins → Preparing, only m2 joined this round → Park.
        assert!(matches!(
            handle_join(&mut g, &join_req("m2", None), "h"),
            JoinAction::Park
        ));
        // m1 rejoins → all members joined this round, from a live (Stable)
        // group → CompleteNow.
        assert!(matches!(
            handle_join(&mut g, &join_req("m1", None), "h"),
            JoinAction::CompleteNow
        ));
    }

    #[test]
    fn join_into_reemptied_group_parks_to_batch_herd() {
        // A group that has rebalanced before (gen > 0) and then went `Empty`
        // — e.g. a warm-up consumer joined and left — must NOT eager-complete
        // a solo generation when the first new member rejoins. It parks for
        // the configured initial-delay window so a herd of consumers starting
        // together batches into one generation. Regression test for the
        // produce+consume throughput collapse triggered by re-joining a group
        // a prior consumer had already used.
        let mut g = ClassicState::new("g");
        let _ = handle_join(&mut g, &join_req("warmup", None), "h");
        g.complete_rebalance("range"); // gen 1
        g.remove_member("warmup"); // group is now Empty, generation still 1
        check!(g.state == GroupState::Empty);
        check!(g.generation_id == 1);
        // First real member rejoins the re-emptied group: Park (batch the
        // herd), NOT CompleteNow, even though the group has rebalanced before.
        assert!(matches!(
            handle_join(&mut g, &join_req("m1", None), "h"),
            JoinAction::Park
        ));
        check!(g.rebalance_from_empty);
    }

    #[test]
    fn build_join_result_leader_lists_members_follower_empty() {
        let mut g = ClassicState::new("g");
        let _ = handle_join(&mut g, &join_req("m1", None), "h");
        let _ = handle_join(&mut g, &join_req("m2", None), "h");
        assert!(try_complete(&mut g).is_ok());
        let leader = g.leader_id.clone().unwrap();
        let follower = if leader == "m1" { "m2" } else { "m1" };
        assert!(!build_join_result(&g, &leader).members.is_empty());
        assert!(build_join_result(&g, follower).members.is_empty());
    }

    #[test]
    fn try_complete_empty_intersection_is_err() {
        let mut g = ClassicState::new("g");
        let mut a = join_req("m1", None);
        a.protocols = vec![JoinGroupRequestProtocol {
            name: "range".into(),
            metadata: Bytes::new(),
            ..Default::default()
        }];
        let _ = handle_join(&mut g, &a, "h");
        let mut b = join_req("m2", None);
        b.protocols = vec![JoinGroupRequestProtocol {
            name: "cooperative-sticky".into(),
            metadata: Bytes::new(),
            ..Default::default()
        }];
        let _ = handle_join(&mut g, &b, "h");
        assert!(try_complete(&mut g).is_err());
    }

    // ── handle_sync ─────────────────────────────────────────────────────────

    fn stable_two_member_group() -> ClassicState {
        let mut g = ClassicState::new("g");
        let _ = handle_join(&mut g, &join_req("m1", None), "h");
        let _ = handle_join(&mut g, &join_req("m2", None), "h");
        try_complete(&mut g).unwrap(); // leader = m1 (min id), CompletingRebalance
        g
    }

    fn sync_req(member_id: &str, generation: i32) -> SyncGroupRequest {
        SyncGroupRequest {
            group_id: "g".into(),
            generation_id: generation,
            member_id: member_id.into(),
            ..Default::default()
        }
    }

    #[test]
    fn sync_unknown_member_and_wrong_generation() {
        let mut g = stable_two_member_group();
        let cur_gen = g.generation_id;
        match handle_sync(&mut g, &sync_req("ghost", cur_gen)) {
            SyncAction::Immediate(r) => assert!(r.error_code == codes::UNKNOWN_MEMBER_ID),
            _ => panic!("expected UNKNOWN_MEMBER_ID"),
        }
        match handle_sync(&mut g, &sync_req("m1", cur_gen + 9)) {
            SyncAction::Immediate(r) => assert!(r.error_code == codes::ILLEGAL_GENERATION),
            _ => panic!("expected ILLEGAL_GENERATION"),
        }
    }

    #[test]
    fn sync_leader_installs_follower_parks_then_reads() {
        let mut g = stable_two_member_group();
        let cur_gen = g.generation_id;
        let leader = g.leader_id.clone().unwrap();
        let follower = if leader == "m1" { "m2" } else { "m1" };
        // Follower before the leader syncs → Park (not yet Stable).
        assert!(matches!(
            handle_sync(&mut g, &sync_req(follower, cur_gen)),
            SyncAction::Park
        ));
        // Leader installs assignments.
        let mut req = sync_req(&leader, cur_gen);
        req.assignments = vec![
            SyncGroupRequestAssignment {
                member_id: leader.clone(),
                assignment: Bytes::from_static(b"L"),
                ..Default::default()
            },
            SyncGroupRequestAssignment {
                member_id: follower.into(),
                assignment: Bytes::from_static(b"F"),
                ..Default::default()
            },
        ];
        match handle_sync(&mut g, &req) {
            SyncAction::LeaderInstalled(r) => {
                assert!(r.error_code == codes::NONE);
                assert!(r.assignment == Bytes::from_static(b"L"));
            }
            _ => panic!("expected LeaderInstalled"),
        }
        assert!(g.state == GroupState::Stable);
        // Now the follower (re-sync) reads its assignment immediately.
        match handle_sync(&mut g, &sync_req(follower, cur_gen)) {
            SyncAction::Immediate(r) => assert!(r.assignment == Bytes::from_static(b"F")),
            _ => panic!("expected Immediate follower assignment"),
        }
    }

    #[test]
    fn read_sync_result_rebalance_in_progress_when_not_stable() {
        let mut g = stable_two_member_group(); // CompletingRebalance, not Stable
        let r = read_sync_result(&g, "m1", None, None);
        assert!(r.error_code == codes::REBALANCE_IN_PROGRESS);
        // Drive to Stable, then it returns NONE.
        let leader = g.leader_id.clone().unwrap();
        let cur_gen = g.generation_id;
        let mut req = sync_req(&leader, cur_gen);
        req.assignments = vec![SyncGroupRequestAssignment {
            member_id: leader.clone(),
            assignment: Bytes::new(),
            ..Default::default()
        }];
        let _ = handle_sync(&mut g, &req);
        let r = read_sync_result(&g, &leader, None, None);
        assert!(r.error_code == codes::NONE);
    }

    // ── handle_heartbeat ─────────────────────────────────────────────────────

    #[test]
    fn heartbeat_codes_cover_all_branches() {
        let mut g = stable_two_member_group();
        let hb = |member: &str, gen_id: i32| HeartbeatRequest {
            group_id: "g".into(),
            generation_id: gen_id,
            member_id: member.into(),
            ..Default::default()
        };
        let cur_gen = g.generation_id;
        // Not Stable yet → REBALANCE_IN_PROGRESS.
        assert!(handle_heartbeat(&mut g, &hb("m1", cur_gen)) == codes::REBALANCE_IN_PROGRESS);
        g.state = GroupState::Stable;
        for (member, gen_id, want) in [
            ("ghost", cur_gen, codes::UNKNOWN_MEMBER_ID),
            ("m1", cur_gen + 9, codes::ILLEGAL_GENERATION),
            ("m1", cur_gen, codes::NONE),
        ] {
            assert!(handle_heartbeat(&mut g, &hb(member, gen_id)) == want);
        }
    }

    // ── handle_leave ──────────────────────────────────────────────────────────

    #[test]
    fn leave_v2_single_member_removed() {
        let mut g = stable_two_member_group();
        g.state = GroupState::Stable;
        let req = LeaveGroupRequest {
            group_id: "g".into(),
            member_id: "m1".into(),
            ..Default::default()
        };
        let out = handle_leave(&mut g, &req, 2);
        assert!(out.len() == 1);
        check!(out[0].error_code == codes::NONE);
        check!(!g.members.contains_key("m1"));
        // Surviving member + was Stable → reopened a rebalance.
        check!(g.state == GroupState::PreparingRebalance);
    }

    #[test]
    fn leave_v3_list_with_instance_resolution_and_unknown() {
        let mut g = ClassicState::new("g");
        let _ = handle_join(&mut g, &join_req("m1", Some("inst-a")), "h");
        let req = LeaveGroupRequest {
            group_id: "g".into(),
            members: vec![
                MemberIdentity {
                    member_id: String::new(),
                    group_instance_id: Some("inst-a".into()),
                    ..Default::default()
                },
                MemberIdentity {
                    member_id: "ghost".into(),
                    group_instance_id: None,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let out = handle_leave(&mut g, &req, 3);
        assert!(out.len() == 2);
        check!(out[0].error_code == codes::NONE); // resolved via instance index
        check!(out[1].error_code == codes::UNKNOWN_MEMBER_ID);
        check!(!g.members.contains_key("m1"));
    }

    // ── validate_commit ───────────────────────────────────────────────────────

    #[test]
    fn validate_commit_branches() {
        let mut g = stable_two_member_group();
        g.state = GroupState::Stable;
        for (member, instance, gen_id, want) in [
            // Simple consumer (no member, no instance) → allowed.
            ("", None, -1, None),
            // Unknown member → UNKNOWN_MEMBER_ID.
            (
                "ghost",
                None,
                g.generation_id,
                Some(codes::UNKNOWN_MEMBER_ID),
            ),
            // Wrong generation → ILLEGAL_GENERATION.
            (
                "m1",
                None,
                g.generation_id + 9,
                Some(codes::ILLEGAL_GENERATION),
            ),
            // Correct → allowed.
            ("m1", None, g.generation_id, None),
            // Instance set but unknown → UNKNOWN_MEMBER_ID.
            (
                "",
                Some("nope"),
                g.generation_id,
                Some(codes::UNKNOWN_MEMBER_ID),
            ),
        ] {
            assert!(validate_commit(&g, member, instance, gen_id) == want);
        }
    }
}
