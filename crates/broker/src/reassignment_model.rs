//! Exhaustive stateright model of the pure KIP-455 reassignment-completion core
//! (`reassign_one`).
//!
//! The model state holds a single partition's reassignment; `next_state` drives
//! the real `reassign_one`; the BFS checker explores every interleaving of
//! replica catch-up, broker liveness, and completion ticks, asserting the
//! reassignment-safety invariants — above all that the replica set never
//! switches off the leader. Design:
//! `docs/superpowers/specs/2026-06-13-crabka-reassignment-model-design.md`.
//!
//! Memory safety: stateright BFS keeps every visited unique state resident, so
//! each run is fenced with `within_boundary` + `target_state_count` + `timeout`
//! and MUST be executed under the host memory watchdog while bounds are tuned.

use std::{
    collections::{BTreeSet, HashSet},
    time::Duration,
};

use crabka_metadata::PartitionRecord;
use crabka_raft::NodeId;
use stateright::{Checker, Model, Property};

use super::reassign_one;

const MAX_STATES: usize = 200_000;
const MAX_DEPTH: usize = 80;
const CHECK_TIMEOUT: Duration = Duration::from_mins(2);

/// Bounded config for the reassignment model (held here, not in the state).
struct ReassignModel {
    replicas: Vec<NodeId>,
    adding: Vec<NodeId>,
    removing: Vec<NodeId>,
    initial_isr: Vec<NodeId>,
    leader: NodeId,
    max_epoch: i32,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct ReassignState {
    replicas: Vec<NodeId>,
    isr: Vec<NodeId>, // canonical replica order
    adding: Vec<NodeId>,
    removing: Vec<NodeId>,
    leader: NodeId,
    leader_epoch: i32,
    alive: BTreeSet<NodeId>,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum ReassignAction {
    AdmitToIsr(NodeId),
    Die(NodeId),
    Revive(NodeId),
    ReassignStep,
}

impl ReassignModel {
    fn basic() -> Self {
        Self {
            replicas: vec![1, 2, 3],
            adding: vec![3],
            removing: vec![2],
            initial_isr: vec![1, 2],
            leader: 1, // not removed → no handoff
            max_epoch: 10,
        }
    }

    fn leader_handoff() -> Self {
        Self {
            replicas: vec![1, 2, 3],
            adding: vec![3],
            removing: vec![2],
            initial_isr: vec![1, 2],
            leader: 2, // in `removing` → handoff required before completion
            max_epoch: 10,
        }
    }

    fn wide() -> Self {
        Self {
            replicas: vec![1, 2, 3, 4, 5],
            adding: vec![4, 5],
            removing: vec![1, 2],
            initial_isr: vec![1, 2, 3],
            leader: 1, // in `removing` → handoff required
            max_epoch: 10,
        }
    }
}

fn in_flight(s: &ReassignState) -> bool {
    !s.adding.is_empty() || !s.removing.is_empty()
}

/// The target replica set the reassignment converges to: replicas − removing.
fn target_of(s: &ReassignState) -> Vec<NodeId> {
    s.replicas
        .iter()
        .filter(|r| !s.removing.contains(r))
        .copied()
        .collect()
}

/// Build a `PartitionRecord` from the model state to drive the real
/// `reassign_one`. `directories` is irrelevant to the safety properties.
fn pr_of(s: &ReassignState) -> PartitionRecord {
    PartitionRecord {
        topic: "t".to_string(),
        partition: 0,
        leader: s.leader,
        replicas: s.replicas.clone(),
        isr: s.isr.clone(),
        leader_epoch: s.leader_epoch,
        adding_replicas: s.adding.clone(),
        removing_replicas: s.removing.clone(),
        directories: vec![],
        partition_epoch: 0,
    }
}

/// Verify a `reassign_one` decision against the pre-state. These are the
/// safety-critical invariants; they hold per-decision under any ordering.
fn assert_step(pre: &ReassignState, next: &PartitionRecord) {
    assert!(
        next.leader_epoch >= pre.leader_epoch,
        "leader_epoch regressed: {} -> {}",
        pre.leader_epoch,
        next.leader_epoch
    );
    assert!(
        pre.adding.iter().all(|n| pre.isr.contains(n)),
        "decision emitted before adding caught up: adding={:?} isr={:?}",
        pre.adding,
        pre.isr
    );
    let target = target_of(pre);
    if next.leader != pre.leader {
        // Handoff.
        assert!(
            pre.isr.contains(&next.leader),
            "handoff to non-ISR {}",
            next.leader
        );
        assert!(
            target.contains(&next.leader),
            "handoff to non-target {}",
            next.leader
        );
        assert!(
            pre.alive.contains(&next.leader),
            "handoff to dead {}",
            next.leader
        );
        assert!(
            !pre.removing.contains(&next.leader),
            "handoff to a removing replica {}",
            next.leader
        );
        assert!(
            next.replicas == pre.replicas,
            "handoff changed the replica set"
        );
        assert!(next.adding_replicas == pre.adding, "handoff changed adding");
        assert!(
            next.removing_replicas == pre.removing,
            "handoff changed removing"
        );
        assert!(
            next.leader_epoch == pre.leader_epoch + 1,
            "handoff did not bump leader_epoch by exactly 1"
        );
    } else if next.adding_replicas.is_empty() && next.removing_replicas.is_empty() {
        // Completion.
        assert!(
            next.replicas.contains(&next.leader),
            "completion switched the replica set off the leader {}: replicas={:?}",
            next.leader,
            next.replicas
        );
        assert!(
            next.replicas == target,
            "completion replicas != target: {:?} vs {:?}",
            next.replicas,
            target
        );
        assert!(
            next.isr.iter().all(|n| next.replicas.contains(n)),
            "completion ISR not a subset of replicas"
        );
        assert!(
            next.leader_epoch == pre.leader_epoch,
            "completion bumped leader_epoch"
        );
    } else {
        panic!("unexpected reassign_one decision shape: {next:?} from {pre:?}");
    }
}

impl Model for ReassignModel {
    type State = ReassignState;
    type Action = ReassignAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![ReassignState {
            replicas: self.replicas.clone(),
            isr: self.initial_isr.clone(),
            adding: self.adding.clone(),
            removing: self.removing.clone(),
            leader: self.leader,
            leader_epoch: 0,
            alive: self.replicas.iter().copied().collect(),
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        // AdmitToIsr: any replica not yet in ISR (models a catch-up + admit).
        for &r in &state.replicas {
            if !state.isr.contains(&r) {
                actions.push(ReassignAction::AdmitToIsr(r));
            }
        }
        // Die / Revive over the replica set (keep >= 1 alive).
        if state.alive.len() > 1 {
            for &r in &state.replicas {
                if state.alive.contains(&r) {
                    actions.push(ReassignAction::Die(r));
                }
            }
        }
        for &r in &state.replicas {
            if !state.alive.contains(&r) {
                actions.push(ReassignAction::Revive(r));
            }
        }
        // ReassignStep when in flight, under the epoch cap.
        if in_flight(state) && state.leader_epoch < self.max_epoch {
            actions.push(ReassignAction::ReassignStep);
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = last.clone();
        match action {
            ReassignAction::AdmitToIsr(n) => {
                if state.isr.contains(&n) || !state.replicas.contains(&n) {
                    return None;
                }
                // Rebuild ISR in canonical replica order (keeps the space small).
                state.isr = state
                    .replicas
                    .iter()
                    .copied()
                    .filter(|r| state.isr.contains(r) || *r == n)
                    .collect();
            }
            ReassignAction::Die(n) => {
                if last.alive.len() <= 1 || !state.alive.remove(&n) {
                    return None;
                }
            }
            ReassignAction::Revive(n) => {
                if !state.alive.insert(n) {
                    return None;
                }
            }
            ReassignAction::ReassignStep => {
                if !in_flight(&state) {
                    return None;
                }
                let pr = pr_of(&state);
                let alive: HashSet<NodeId> = state.alive.iter().copied().collect();
                match reassign_one(&pr, &alive) {
                    Some(next) => {
                        assert_step(last, &next);
                        state.leader = next.leader;
                        state.isr = next.isr;
                        state.adding = next.adding_replicas;
                        state.removing = next.removing_replicas;
                        state.replicas = next.replicas;
                        state.leader_epoch = next.leader_epoch;
                    }
                    None => return None,
                }
            }
        }
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("isr_subset_replicas", |_, s: &ReassignState| {
                s.isr.iter().all(|n| s.replicas.contains(n))
            }),
            Property::always("leader_in_replicas", |_, s: &ReassignState| {
                s.replicas.contains(&s.leader)
            }),
            Property::always("leader_in_isr", |_, s: &ReassignState| {
                s.isr.contains(&s.leader)
            }),
            Property::always("adding_subset_replicas", |_, s: &ReassignState| {
                s.adding.iter().all(|n| s.replicas.contains(n))
            }),
            Property::always("removing_subset_replicas", |_, s: &ReassignState| {
                s.removing.iter().all(|n| s.replicas.contains(n))
            }),
            Property::sometimes("can_complete", |_, s: &ReassignState| {
                s.adding.is_empty() && s.removing.is_empty()
            }),
            // Config-conditional so it is not vacuously unsatisfiable in the
            // basic config (where no handoff happens).
            Property::sometimes("can_handoff", |m: &ReassignModel, s: &ReassignState| {
                !m.removing.contains(&m.leader) || s.leader != m.leader
            }),
            Property::sometimes("can_wait", |_, s: &ReassignState| {
                in_flight(s) && s.adding.iter().any(|n| !s.isr.contains(n))
            }),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.leader_epoch <= self.max_epoch
    }
}

fn run(model: ReassignModel, label: &str) {
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
fn reassign_basic() {
    // Leader not removed: catch-up then completion to the target replica set.
    run(ReassignModel::basic(), "reassign_basic");
}

#[test]
fn reassign_leader_handoff() {
    // Leader in `removing`: catch-up, leader handoff, then completion.
    run(ReassignModel::leader_handoff(), "reassign_leader_handoff");
}

#[test]
fn reassign_wide() {
    // 5 replicas, add 2 + remove 2, leader removed → handoff then completion.
    run(ReassignModel::wide(), "reassign_wide");
}
