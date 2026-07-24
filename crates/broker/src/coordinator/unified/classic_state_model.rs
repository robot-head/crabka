//! Exhaustive stateright enumeration of the classic consumer-group membership
//! state machine (KIP-345/62), wrapping the real [`super::ClassicGroup`]. Drives the
//! real `add_member` / `remove_member` / `complete_rebalance` /
//! `install_assignments` / `expire_dead_members` + the handler's KIP-345 fence
//! pre-check (`current_member_id_for_instance`) under every interleaving of join
//! (dynamic / static / fenced), heartbeat, leave, rebalance completion, sync,
//! and session-timeout expiry. Asserts static-index coherence, single-owner,
//! and static-never-expired invariants. See the design spec at
//! `docs/superpowers/specs/2026-06-14-crabka-classic-group-fencing-model-design.md`.

use std::{
    hash::{Hash, Hasher},
    sync::OnceLock,
    time::{Duration, Instant},
};

use bytes::Bytes;
use stateright::{Checker, Model, Property};

use super::{ClassicGroup, GroupState, Member};

// Exhaustiveness is bounded on UNIQUE states (memory-proportional); the BFS's
// generated count runs several times the unique count here (high branching:
// every idle member has join/leave/heartbeat actions). `TARGET_STATE_COUNT` is
// the truncation ceiling set high so the BFS runs to completion (the 2-minute
// timeout + 3 GB host watchdog are the real runaway guards —
// `[[feedback_bound_model_checkers]]`); `state_count() < TARGET` then certifies
// the run was exhaustive.
const TARGET_STATE_COUNT: usize = 8_000_000;
const MAX_UNIQUE_STATES: usize = 500_000; // wide ~362k unique; margin for determinism
const MAX_DEPTH: usize = 80;
const CHECK_TIMEOUT: Duration = Duration::from_mins(2);

fn epoch() -> Instant {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    *EPOCH.get_or_init(Instant::now)
}
const UNIT: Duration = Duration::from_secs(1);
const SESSION: Duration = Duration::from_secs(2); // 2 * UNIT

fn at(clock: i64) -> Instant {
    epoch() + UNIT * u32::try_from(clock.max(0)).unwrap_or(0)
}

fn mk_member(mid: &str, iid: Option<&str>, clock: i64) -> Member {
    let mut m = Member::new(
        mid,
        "c",
        "h",
        SESSION,
        Duration::from_secs(10),
        vec![("range".to_string(), Bytes::new())],
    )
    .with_instance_id(iid.map(str::to_string));
    m.last_heartbeat = at(clock);
    m
}

/// Every index entry points at a live member carrying the matching instance id,
/// and every static member has a matching index entry (bidirectional mirror).
fn index_coherent(g: &ClassicGroup) -> bool {
    for (iid, mid) in &g.static_members {
        match g.members.get(mid) {
            Some(m) if m.group_instance_id.as_deref() == Some(iid.as_str()) => {}
            _ => return false,
        }
    }
    for (mid, m) in &g.members {
        if let Some(iid) = &m.group_instance_id
            && g.static_members.get(iid).map(String::as_str) != Some(mid.as_str())
        {
            return false;
        }
    }
    true
}

/// No two live members share a `group.instance.id` (no fencing-bypass).
fn single_owner(g: &ClassicGroup) -> bool {
    let mut seen = std::collections::HashSet::new();
    for m in g.members.values() {
        if let Some(iid) = &m.group_instance_id
            && !seen.insert(iid.clone())
        {
            return false;
        }
    }
    true
}

struct ClassicModel {
    members: Vec<&'static str>,
    instances: Vec<&'static str>,
    max_clock: i64,
}

/// Model state: a real `ClassicGroup` plus the logical clock. `Hash`/`Eq` are manual
/// over a canonical projection because `ClassicGroup` holds `HashMap`s.
#[derive(Clone, Debug)]
struct GrpState {
    g: ClassicGroup,
    clock: i64,
}

// NOTE: `generation_id` is deliberately EXCLUDED from the fingerprint. It is a
// monotonic counter bumped on every rebalance and read by no transition, so
// including it would make the rebalance cycle (join→complete→sync→…) an
// unbounded state generator (the DPM-A1 monotonic-counter lesson). States that
// differ only in generation are behaviorally equivalent for every invariant.
type Proj = (
    GroupState,
    Option<String>,
    Option<String>,
    bool,
    i64,
    Vec<(String, Option<String>, bool, Instant)>,
    Vec<(String, String)>,
    Vec<String>,
);

impl GrpState {
    fn proj(&self) -> Proj {
        let mut members: Vec<(String, Option<String>, bool, Instant)> = self
            .g
            .members
            .iter()
            .map(|(id, m)| {
                (
                    id.clone(),
                    m.group_instance_id.clone(),
                    m.assignment.is_some(),
                    m.last_heartbeat,
                )
            })
            .collect();
        members.sort();
        let mut idx: Vec<(String, String)> = self
            .g
            .static_members
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        idx.sort();
        let mut joined: Vec<String> = self.g.joined_this_round.iter().cloned().collect();
        joined.sort();
        (
            self.g.state,
            self.g.leader_id.clone(),
            self.g.protocol_name.clone(),
            self.g.rebalance_from_empty,
            self.clock,
            members,
            idx,
            joined,
        )
    }
}

impl PartialEq for GrpState {
    fn eq(&self, other: &Self) -> bool {
        self.proj() == other.proj()
    }
}
impl Eq for GrpState {}
impl Hash for GrpState {
    fn hash<H: Hasher>(&self, h: &mut H) {
        self.proj().hash(h);
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum Act {
    JoinDynamic(&'static str),
    JoinStatic(&'static str, &'static str), // (instance_id, member_id)
    Heartbeat(&'static str),
    Leave(&'static str),
    CompleteRebalance,
    Sync,
    ExpireTick,
}

impl Model for ClassicModel {
    type State = GrpState;
    type Action = Act;

    fn init_states(&self) -> Vec<Self::State> {
        vec![GrpState {
            g: ClassicGroup::new("g"),
            clock: 0,
        }]
    }

    fn actions(&self, s: &Self::State, actions: &mut Vec<Self::Action>) {
        for &mid in &self.members {
            actions.push(Act::JoinDynamic(mid));
            actions.push(Act::Heartbeat(mid));
            actions.push(Act::Leave(mid));
            for &iid in &self.instances {
                actions.push(Act::JoinStatic(iid, mid));
            }
        }
        if matches!(s.g.state, GroupState::PreparingRebalance) && !s.g.members.is_empty() {
            actions.push(Act::CompleteRebalance);
        }
        if matches!(s.g.state, GroupState::CompletingRebalance) {
            actions.push(Act::Sync);
        }
        if s.clock < self.max_clock {
            actions.push(Act::ExpireTick);
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = last.clone();
        match action {
            Act::JoinDynamic(mid) => {
                // Handler guard (classic_ops step 2b): a known member_id with a
                // different instance nature is fenced — here, a dynamic rejoin of
                // a member currently pinned to an instance.
                if s.g
                    .members
                    .get(mid)
                    .is_some_and(|m| m.group_instance_id.is_some())
                {
                    return None;
                }
                s.g.add_member(mk_member(mid, None, s.clock));
            }
            Act::JoinStatic(iid, mid) => {
                // Handler step 2b: a known member_id must keep a consistent
                // instance id (else the overwrite orphans the static index).
                if s.g
                    .members
                    .get(mid)
                    .is_some_and(|m| m.group_instance_id.as_deref() != Some(iid))
                {
                    return None;
                }
                // Handler step 3: instance id pinned to a different live member.
                if let Some(pinned) = s.g.current_member_id_for_instance(iid)
                    && pinned != mid
                {
                    return None;
                }
                s.g.add_member(mk_member(mid, Some(iid), s.clock));
            }
            Act::Heartbeat(mid) => {
                let m = s.g.members.get_mut(mid)?;
                m.last_heartbeat = at(s.clock);
            }
            Act::Leave(mid) => {
                if !s.g.members.contains_key(mid) {
                    return None;
                }
                s.g.remove_member(mid);
                // Mirror handle_leave: a member leaving a live (Stable) group
                // triggers a membership-change rebalance. (leader_id is NOT reset
                // here — it is best-effort, overwritten by the next
                // complete_rebalance; a stale leader is recovered via the
                // rebalance timeout, so it is not a safety invariant.)
                if !s.g.members.is_empty() && matches!(s.g.state, GroupState::Stable) {
                    s.g.state = GroupState::PreparingRebalance;
                    s.g.rebalance_from_empty = false;
                }
            }
            Act::CompleteRebalance => {
                if !matches!(s.g.state, GroupState::PreparingRebalance) || s.g.members.is_empty() {
                    return None;
                }
                s.g.complete_rebalance("range");
            }
            Act::Sync => {
                if !matches!(s.g.state, GroupState::CompletingRebalance) {
                    return None;
                }
                let assignments =
                    s.g.members
                        .keys()
                        .map(|id| (id.clone(), Bytes::from_static(b"a")))
                        .collect();
                s.g.install_assignments(assignments);
            }
            Act::ExpireTick => {
                s.clock += 1;
                let dropped = s.g.expire_dead_members(at(s.clock), Duration::from_secs(3));
                for id in &dropped {
                    assert!(
                        !last.g.members.get(id).is_some_and(Member::is_static),
                        "static member {id} was expired"
                    );
                }
            }
        }
        assert!(index_coherent(&s.g), "index coherence violated: {:?}", s.g);
        assert!(single_owner(&s.g), "single-owner violated: {:?}", s.g);
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("index_coherence", |_, s: &GrpState| index_coherent(&s.g)),
            Property::always("single_owner_per_instance", |_, s: &GrpState| {
                single_owner(&s.g)
            }),
            Property::always("joined_subset", |_, s: &GrpState| {
                s.g.joined_this_round
                    .iter()
                    .all(|id| s.g.members.contains_key(id))
            }),
            Property::always("empty_iff_empty_state", |_, s: &GrpState| {
                s.g.members.is_empty() == matches!(s.g.state, GroupState::Empty)
            }),
            Property::sometimes("reached_stable", |_, s: &GrpState| {
                matches!(s.g.state, GroupState::Stable)
            }),
            Property::sometimes("instance_pinned", |_, s: &GrpState| {
                !s.g.static_members.is_empty()
            }),
            Property::sometimes("two_members", |_, s: &GrpState| s.g.members.len() >= 2),
            Property::sometimes("generation_bumped", |_, s: &GrpState| {
                s.g.generation_id >= 1
            }),
        ]
    }

    fn within_boundary(&self, s: &Self::State) -> bool {
        s.clock <= self.max_clock && s.g.members.len() <= self.members.len()
    }
}

fn run(model: ClassicModel, label: &str) {
    let checker = model
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(TARGET_STATE_COUNT)
        .timeout(CHECK_TIMEOUT)
        .spawn_bfs()
        .join();
    eprintln!(
        "[{label}] unique_states={} generated={} max_depth={}",
        checker.unique_state_count(),
        checker.state_count(),
        checker.max_depth()
    );
    assert!(checker.max_depth() < MAX_DEPTH, "[{label}] depth cap hit");
    assert!(
        checker.state_count() < TARGET_STATE_COUNT,
        "[{label}] truncated at the state-count target — not exhaustive"
    );
    assert!(
        checker.unique_state_count() < MAX_UNIQUE_STATES,
        "[{label}] unique-state bound exceeded ({})",
        checker.unique_state_count()
    );
    checker.assert_properties();
}

#[test]
fn classic_basic() {
    run(
        ClassicModel {
            members: vec!["a", "b"],
            instances: vec!["x"],
            max_clock: 4,
        },
        "classic_basic",
    );
}

#[test]
fn classic_wide() {
    run(
        ClassicModel {
            members: vec!["a", "b", "c"],
            instances: vec!["x", "y"],
            max_clock: 5,
        },
        "classic_wide",
    );
}

#[cfg(test)]
mod fuzz {
    use std::time::Duration;

    use bytes::Bytes;
    use proptest::prelude::*;

    use super::{
        super::{ClassicGroup, GroupState},
        at, index_coherent, mk_member, single_owner,
    };

    #[derive(Clone, Debug)]
    enum Op {
        JoinDynamic(u8),
        JoinStatic(u8, u8),
        Heartbeat(u8),
        Leave(u8),
        Complete,
        Sync,
        Expire,
    }

    fn op_strategy() -> impl Strategy<Value = Op> {
        prop_oneof![
            (0u8..3).prop_map(Op::JoinDynamic),
            (0u8..2, 0u8..3).prop_map(|(i, m)| Op::JoinStatic(i, m)),
            (0u8..3).prop_map(Op::Heartbeat),
            (0u8..3).prop_map(Op::Leave),
            Just(Op::Complete),
            Just(Op::Sync),
            Just(Op::Expire),
        ]
    }

    proptest! {
        /// Large-N random op sequences over a real `ClassicGroup` (mirroring the handler
        /// fence guards + leave retrigger): the membership invariants hold after
        /// every step.
        #[test]
        fn classic_invariants_hold(ops in proptest::collection::vec(op_strategy(), 0..300)) {
            let mids = ["a", "b", "c"];
            let iids = ["x", "y"];
            let mut g = ClassicGroup::new("g");
            let mut clock: i64 = 0;
            for op in ops {
                match op {
                    Op::JoinDynamic(m) => {
                        let mid = mids[m as usize];
                        // Handler step 2b: a known member_id pinned to an instance
                        // can't rejoin as dynamic.
                        if g
                            .members
                            .get(mid)
                            .is_none_or(|mm| mm.group_instance_id.is_none())
                        {
                            g.add_member(mk_member(mid, None, clock));
                        }
                    }
                    Op::JoinStatic(i, m) => {
                        let iid = iids[i as usize];
                        let mid = mids[m as usize];
                        let member_mismatch = g
                            .members
                            .get(mid)
                            .is_some_and(|mm| mm.group_instance_id.as_deref() != Some(iid));
                        let fenced = g.current_member_id_for_instance(iid).is_some_and(|p| p != mid);
                        if !(member_mismatch || fenced) {
                            g.add_member(mk_member(mid, Some(iid), clock));
                        }
                    }
                    Op::Heartbeat(m) => {
                        if let Some(mm) = g.members.get_mut(mids[m as usize]) {
                            mm.last_heartbeat = at(clock);
                        }
                    }
                    Op::Leave(m) => {
                        g.remove_member(mids[m as usize]);
                        if !g.members.is_empty() && matches!(g.state, GroupState::Stable) {
                            g.state = GroupState::PreparingRebalance;
                            g.rebalance_from_empty = false;
                        }
                    }
                    Op::Complete => {
                        if matches!(g.state, GroupState::PreparingRebalance) && !g.members.is_empty()
                        {
                            g.complete_rebalance("range");
                        }
                    }
                    Op::Sync => {
                        if matches!(g.state, GroupState::CompletingRebalance) {
                            let a = g
                                .members
                                .keys()
                                .map(|k| (k.clone(), Bytes::from_static(b"a")))
                                .collect();
                            g.install_assignments(a);
                        }
                    }
                    Op::Expire => {
                        clock += 1;
                        let static_before: std::collections::HashSet<String> = g
                            .members
                            .iter()
                            .filter(|(_, m)| m.is_static())
                            .map(|(id, _)| id.clone())
                            .collect();
                        let dropped =
                            g.expire_dead_members(at(clock), Duration::from_secs(3));
                        for id in &dropped {
                            prop_assert!(!static_before.contains(id), "static member was expired");
                        }
                    }
                }
                prop_assert!(index_coherent(&g), "index coherence");
                prop_assert!(single_owner(&g), "single owner");
                prop_assert!(
                    g.joined_this_round.iter().all(|id| g.members.contains_key(id)),
                    "joined subset"
                );
                prop_assert_eq!(
                    g.members.is_empty(),
                    matches!(g.state, GroupState::Empty),
                    "empty iff Empty"
                );
            }
        }
    }
}
