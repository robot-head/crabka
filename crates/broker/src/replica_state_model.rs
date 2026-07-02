//! Exhaustive stateright model of the pure leader-side replication core
//! (`ReplicaState`).
//!
//! The model state holds the REAL `ReplicaState` and drives the production
//! `install_isr` / `update_follower_leo` / `recompute_hw_for_leader_append`;
//! the BFS checker explores every interleaving of leader append, follower
//! fetch, and ISR shrink/expand, asserting the partition-replication safety
//! invariants never break — above all no-committed-data-loss. Design:
//! `docs/superpowers/specs/2026-06-13-crabka-isr-replica-state-model-design.md`.
//!
//! Memory safety: stateright BFS keeps every visited unique state resident, so
//! each run is fenced with `within_boundary` + `target_state_count` + `timeout`
//! and MUST be executed under the host memory watchdog while bounds are tuned.

use std::{
    collections::HashSet,
    hash::{Hash, Hasher},
    time::{Duration, Instant},
};

use crabka_raft::NodeId;
use stateright::{Checker, Model, Property};

use super::ReplicaState;

/// Hard backstop on generated states — bounds host memory even if
/// `within_boundary` is looser than intended.
const MAX_STATES: usize = 200_000;
/// Depth backstop; must exceed each config's reachable-graph diameter.
const MAX_DEPTH: usize = 80;
/// Wall-clock backstop.
const CHECK_TIMEOUT: Duration = Duration::from_mins(2);

/// Bounded model config (held here, not in the fingerprinted state).
struct IsrModel {
    /// Constant injected `now` — the model does not model wall-clock time
    /// (ISR shrink/expand is an explicit action, not a time-based decision).
    t0: Instant,
    /// `replicas[0]` is the fixed leader; the rest are followers.
    replicas: Vec<NodeId>,
    /// Leader-LEO / follower-LEO cap.
    max_offset: i64,
    /// When set, followers may report a LEO above `leader_leo` (clamp test).
    test_overshoot: bool,
}

impl IsrModel {
    fn safety(max_offset: i64) -> Self {
        Self {
            t0: Instant::now(),
            replicas: vec![1, 2, 3],
            max_offset,
            test_overshoot: false,
        }
    }

    fn overshoot(max_offset: i64) -> Self {
        Self {
            t0: Instant::now(),
            replicas: vec![1, 2, 3],
            max_offset,
            test_overshoot: true,
        }
    }

    fn leader(&self) -> NodeId {
        self.replicas[0]
    }

    fn followers(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.replicas[1..].iter().copied()
    }
}

/// The fingerprinted model state: the REAL core + the leader's own LEO.
#[derive(Clone, Debug)]
struct IsrState {
    rs: ReplicaState,
    leader_leo: i64,
}

impl IsrState {
    /// Normalized, timestamp-free projection used for Eq/Hash: the real state
    /// holds non-`Hash` `HashMap`/`HashSet` and non-deterministic timestamps,
    /// neither of which is safety-relevant here.
    fn project(&self) -> (Vec<NodeId>, Vec<(NodeId, i64)>, i64, i32, i64) {
        let mut isr: Vec<NodeId> = self.rs.isr.iter().copied().collect();
        isr.sort_unstable();
        let mut pf: Vec<(NodeId, i64)> = self
            .rs
            .per_follower
            .iter()
            .map(|(k, v)| (*k, v.leo))
            .collect();
        pf.sort_unstable();
        (
            isr,
            pf,
            self.rs.hw,
            self.rs.current_leader_epoch,
            self.leader_leo,
        )
    }
}

impl PartialEq for IsrState {
    fn eq(&self, other: &Self) -> bool {
        self.project() == other.project()
    }
}
impl Eq for IsrState {}
impl Hash for IsrState {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.project().hash(state);
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum IsrAction {
    /// Leader appends one record (`leader_leo` += 1) and recomputes HW.
    LeaderAppend,
    /// A follower reports `leo` via fetch.
    FollowerFetch { follower: NodeId, leo: i64 },
    /// The controller installs a new committed ISR.
    InstallIsr { isr: Vec<NodeId> },
}

impl Model for IsrModel {
    type State = IsrState;
    type Action = IsrAction;

    fn init_states(&self) -> Vec<Self::State> {
        // Fresh leader: full replica set in the ISR, followers seeded at 0.
        let mut rs = ReplicaState::new();
        rs.install_isr(&self.replicas, &self.replicas, self.leader(), self.t0);
        vec![IsrState { rs, leader_leo: 0 }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        let leader = self.leader();

        if state.leader_leo < self.max_offset {
            actions.push(IsrAction::LeaderAppend);
        }

        // Follower fetches: advance by one or jump to the leader's LEO. Targets
        // are monotonic (never below the follower's current LEO) — a real
        // follower's reported LEO never regresses, which is what keeps HW
        // monotone. `test_overshoot` additionally probes the defensive clamp.
        for f in self.followers() {
            let cur = state.rs.per_follower.get(&f).map_or(0, |s| s.leo);
            let mut targets: Vec<i64> = Vec::new();
            if cur < state.leader_leo {
                targets.push(cur + 1);
                targets.push(state.leader_leo);
            }
            if self.test_overshoot {
                targets.push(state.leader_leo + 1);
            }
            targets.sort_unstable();
            targets.dedup();
            for leo in targets {
                actions.push(IsrAction::FollowerFetch { follower: f, leo });
            }
        }

        // ISR changes: every subset of replicas that contains the leader and
        // differs from the current ISR. Expansion only admits caught-up
        // followers (per_follower.leo >= hw) — the controller's real rule;
        // without it the model would report a false data-loss violation.
        let cur_isr: HashSet<NodeId> = state.rs.isr.clone();
        let follower_vec: Vec<NodeId> = self.followers().collect();
        for mask in 0u32..(1u32 << follower_vec.len()) {
            let mut isr: Vec<NodeId> = vec![leader];
            for (i, &f) in follower_vec.iter().enumerate() {
                if mask & (1 << i) != 0 {
                    isr.push(f);
                }
            }
            let isr_set: HashSet<NodeId> = isr.iter().copied().collect();
            if isr_set == cur_isr {
                continue;
            }
            let expansion_ok = isr
                .iter()
                .filter(|&&n| n != leader && !cur_isr.contains(&n))
                .all(|f| state.rs.per_follower.get(f).map_or(0, |s| s.leo) >= state.rs.hw);
            if !expansion_ok {
                continue;
            }
            isr.sort_unstable();
            actions.push(IsrAction::InstallIsr { isr });
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = last.clone();
        match action {
            IsrAction::LeaderAppend => {
                if state.leader_leo >= self.max_offset {
                    return None;
                }
                state.leader_leo += 1;
                state.rs.recompute_hw_for_leader_append(state.leader_leo);
            }
            IsrAction::FollowerFetch { follower, leo } => {
                state
                    .rs
                    .update_follower_leo(follower, leo, state.leader_leo, self.t0);
            }
            IsrAction::InstallIsr { isr } => {
                state
                    .rs
                    .install_isr(&isr, &self.replicas, self.leader(), self.t0);
            }
        }
        // Transition invariant (kept out of the fingerprinted state): the
        // high-watermark never regresses.
        assert!(
            state.rs.hw >= last.rs.hw,
            "HWM regressed: {} -> {}",
            last.rs.hw,
            state.rs.hw
        );
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("hw_within_leader", |_, s: &IsrState| {
                s.rs.hw <= s.leader_leo
            }),
            // No-committed-data-loss: every ISR member holds every committed
            // record. A missing per_follower entry for an ISR member counts as a
            // violation (compute_hw skips entryless members).
            Property::always("no_data_loss", |m: &IsrModel, s: &IsrState| {
                let leader = m.leader();
                s.rs.isr
                    .iter()
                    .filter(|&&f| f != leader)
                    .all(|f| s.rs.per_follower.get(f).is_some_and(|st| st.leo >= s.rs.hw))
            }),
            Property::always("leo_clamped", |_, s: &IsrState| {
                s.rs.per_follower.values().all(|st| st.leo <= s.leader_leo)
            }),
            Property::always("hw_nonneg", |_, s: &IsrState| s.rs.hw >= 0),
            Property::always("leader_in_isr", |m: &IsrModel, s: &IsrState| {
                s.rs.isr.contains(&m.leader())
            }),
            Property::sometimes("can_advance_hw", |_, s: &IsrState| s.rs.hw > 0),
            Property::sometimes("can_reach_leader_leo", |_, s: &IsrState| {
                s.leader_leo > 0 && s.rs.hw == s.leader_leo
            }),
            Property::sometimes("can_pin_below_leader", |_, s: &IsrState| {
                s.rs.hw > 0 && s.rs.hw < s.leader_leo
            }),
            Property::sometimes("can_shrink_isr", |m: &IsrModel, s: &IsrState| {
                let leader = m.leader();
                m.replicas.iter().any(|&r| {
                    r != leader && !s.rs.isr.contains(&r) && s.rs.per_follower.contains_key(&r)
                })
            }),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.leader_leo <= self.max_offset
            && state.rs.hw <= self.max_offset
            && state
                .rs
                .per_follower
                .values()
                .all(|s| s.leo <= self.max_offset)
    }
}

/// Run one bounded config to completion; assert it was exhaustive (not
/// cap/depth-truncated) and that all properties hold.
fn run(model: IsrModel, label: &str) {
    let checker = model
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(MAX_STATES)
        .timeout(CHECK_TIMEOUT)
        .spawn_bfs()
        .join();
    eprintln!(
        "[{label}] unique_states={} generated={} max_depth={}",
        checker.unique_state_count(),
        checker.state_count(),
        checker.max_depth()
    );
    assert!(
        checker.max_depth() < MAX_DEPTH,
        "[{label}] hit depth cap {MAX_DEPTH}: depth-truncated, not exhaustive"
    );
    assert!(
        checker.state_count() < MAX_STATES,
        "[{label}] hit state cap {MAX_STATES}: truncated, not exhaustive"
    );
    checker.assert_properties();
}

#[test]
fn isr_safety() {
    run(IsrModel::safety(3), "isr_safety");
}

#[test]
fn isr_overshoot() {
    run(IsrModel::overshoot(3), "isr_overshoot");
}
