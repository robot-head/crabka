//! Exhaustive stateright models of the controller leader-failover decision
//! (`failover_one`) and the KIP-966 winner selection (Task 3). See
//! `docs/superpowers/specs/2026-06-13-crabka-failover-recovery-model-design.md`.
//!
//! Memory safety: stateright BFS keeps every visited unique state resident, so
//! each run is fenced with `within_boundary` + `target_state_count` + `timeout`
//! and MUST be executed under the host memory watchdog while bounds are tuned.

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    time::Duration,
};

use crabka_metadata::PartitionRecord;
use crabka_raft::NodeId;
use stateright::{Checker, Model, Property};

use super::{FailoverDecision, failover_one};
use crate::{
    config_keys::RecoveryStrategy,
    unclean_recovery::{ReplicaLogInfo, has_newer_leader, select_best_replica},
};

const MAX_STATES: usize = 200_000;
const MAX_DEPTH: usize = 80;
const CHECK_TIMEOUT: Duration = Duration::from_mins(2);

// ============================ FailoverModel ============================

/// Bounded config for the failover-scan model.
struct FailoverModel {
    replicas: Vec<NodeId>, // replicas[0] is the fixed initial leader
    strategy: RecoveryStrategy,
    unclean_enabled: bool,
    max_epoch: i32,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct FailoverState {
    leader: NodeId,
    isr: Vec<NodeId>,      // order significant (clean election picks isr.first())
    replicas: Vec<NodeId>, // fixed; order significant (KIP-841 picks replicas order)
    leader_epoch: i32,
    alive: BTreeSet<NodeId>,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum FailoverAction {
    Die(NodeId),
    Revive(NodeId),
    Failover(NodeId),
}

impl FailoverModel {
    fn config(strategy: RecoveryStrategy, unclean_enabled: bool) -> Self {
        Self {
            replicas: vec![
                crabka_audit::NodeId(1),
                crabka_audit::NodeId(2),
                crabka_audit::NodeId(3),
            ],
            strategy,
            unclean_enabled,
            max_epoch: 6,
        }
    }
}

/// Build a minimal `PartitionRecord` from the model state to drive the real
/// `failover_one`. The fields `failover_one` ignores are dummied.
fn pr_of(s: &FailoverState) -> PartitionRecord {
    PartitionRecord {
        topic: "t".to_string(),
        partition: 0,
        leader: s.leader,
        replicas: s.replicas.clone(),
        isr: s.isr.clone(),
        leader_epoch: crabka_metadata::LeaderEpoch(s.leader_epoch),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 0,
    }
}

/// Verify a `failover_one` decision against the pre-failover state. These are
/// the safety-critical invariants; they hold per-decision under any ordering.
fn assert_decision(pre: &FailoverState, dead: NodeId, d: &FailoverDecision, unclean_enabled: bool) {
    match d {
        FailoverDecision::Elect {
            leader,
            isr,
            unclean,
        } => {
            assert2::assert!(*leader != dead);
            assert2::assert!(pre.alive.contains(leader));
            assert2::assert!(isr.contains(leader));
            if *unclean {
                assert2::assert!(unclean_enabled);
            } else {
                // Clean election: the new leader was in the pre-failover ISR, so
                // it holds every committed record. No data loss.
                assert2::assert!(pre.isr.contains(leader));
            }
        }
        FailoverDecision::ShrinkIsr { isr } => {
            assert2::assert!(isr.iter().all(|n| pre.isr.contains(n)));
            assert2::assert!(isr.len() < pre.isr.len());
        }
        FailoverDecision::Recover(s) => {
            assert2::assert!(*s != RecoveryStrategy::None);
            assert2::assert!(pre.leader == dead);
        }
        FailoverDecision::Unavailable | FailoverDecision::NoChange => {}
    }
}

impl Model for FailoverModel {
    type State = FailoverState;
    type Action = FailoverAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![FailoverState {
            leader: self.replicas[0],
            isr: self.replicas.clone(),
            replicas: self.replicas.clone(),
            leader_epoch: 0,
            alive: self.replicas.iter().copied().collect(),
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        // Die: any alive broker, keeping >= 1 alive.
        if state.alive.len() > 1 {
            for &r in &self.replicas {
                if state.alive.contains(&r) {
                    actions.push(FailoverAction::Die(r));
                }
            }
        }
        // Revive: any dead broker.
        for &r in &self.replicas {
            if !state.alive.contains(&r) {
                actions.push(FailoverAction::Revive(r));
            }
        }
        // Failover: any dead broker (the real scan's filter is replicas-or-isr;
        // all model brokers are replicas), under the epoch cap.
        if state.leader_epoch < self.max_epoch {
            for &r in &self.replicas {
                if !state.alive.contains(&r) {
                    actions.push(FailoverAction::Failover(r));
                }
            }
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = last.clone();
        match action {
            FailoverAction::Die(n) => {
                if last.alive.len() <= 1 || !state.alive.remove(&n) {
                    return None;
                }
            }
            FailoverAction::Revive(n) => {
                if !state.alive.insert(n) {
                    return None;
                }
            }
            FailoverAction::Failover(dead) => {
                if state.alive.contains(&dead) {
                    return None;
                }
                let pr = pr_of(&state);
                let alive: HashSet<NodeId> = state.alive.iter().copied().collect();
                let decision = failover_one(&pr, dead, &alive, self.strategy, self.unclean_enabled);
                assert_decision(&state, dead, &decision, self.unclean_enabled);
                match decision {
                    FailoverDecision::Elect { leader, isr, .. } => {
                        state.leader = leader;
                        state.isr = isr;
                        state.leader_epoch += 1;
                    }
                    FailoverDecision::ShrinkIsr { isr } => {
                        state.isr = isr;
                    }
                    FailoverDecision::Recover(_)
                    | FailoverDecision::Unavailable
                    | FailoverDecision::NoChange => return None,
                }
            }
        }
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("isr_subset_replicas", |_, s: &FailoverState| {
                s.isr.iter().all(|n| s.replicas.contains(n))
            }),
            Property::always("leader_in_replicas", |_, s: &FailoverState| {
                s.replicas.contains(&s.leader)
            }),
            Property::sometimes("can_elect", |_, s: &FailoverState| s.leader_epoch > 0),
            Property::sometimes("can_singleton_isr", |_, s: &FailoverState| s.isr.len() == 1),
            Property::sometimes("can_lose_isr_member", |_, s: &FailoverState| {
                s.isr.iter().any(|n| !s.alive.contains(n))
            }),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.leader_epoch <= self.max_epoch
    }
}

fn run_failover(model: FailoverModel, label: &str) {
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
    assert2::assert!(checker.max_depth() < MAX_DEPTH);
    assert2::assert!(checker.state_count() < MAX_STATES);
    checker.assert_properties();
}

#[test]
fn failover_safe() {
    // unclean disabled: a clean election (or unavailability) is the only path;
    // the decision asserts guarantee no out-of-ISR election ever happens.
    run_failover(
        FailoverModel::config(RecoveryStrategy::None, false),
        "failover_safe",
    );
}

#[test]
fn failover_unclean() {
    // KIP-841: out-of-ISR election permitted when ISR is empty.
    run_failover(
        FailoverModel::config(RecoveryStrategy::None, true),
        "failover_unclean",
    );
}

#[test]
fn failover_recover() {
    // KIP-966: empty-ISR leader death defers to offset-aware recovery.
    run_failover(
        FailoverModel::config(RecoveryStrategy::Balanced, false),
        "failover_recover",
    );
}

// ============================ RecoveryModel ============================

/// One replica's reported log state (a hashable mirror of `ReplicaLogInfo`,
/// which isn't `Hash`).
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct ReplicaLog {
    last_written_leader_epoch: i32,
    log_end_offset: i64,
    current_leader_epoch: i32,
}

/// Bounded config for the KIP-966 winner-selection model.
struct RecoveryModel {
    replicas: Vec<NodeId>,
    max_epoch: i32,
    max_leo: i64,
    known_leader_epoch: i32,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct RecoveryState {
    responses: BTreeMap<NodeId, ReplicaLog>,
    known_leader_epoch: i32,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum RecoveryAction {
    AddResponse {
        node: NodeId,
        last_written_epoch: i32,
        leo: i64,
        current_epoch: i32,
    },
}

impl RecoveryModel {
    fn offset_recovery() -> Self {
        Self {
            replicas: vec![
                crabka_audit::NodeId(1),
                crabka_audit::NodeId(2),
                crabka_audit::NodeId(3),
            ],
            max_epoch: 2,
            max_leo: 2,
            known_leader_epoch: 1,
        }
    }
}

/// Project the gathered responses into the real wire-decoupled type.
fn infos_of(s: &RecoveryState) -> Vec<ReplicaLogInfo> {
    s.responses
        .iter()
        .map(|(id, l)| ReplicaLogInfo {
            broker_id: *id,
            last_written_leader_epoch: l.last_written_leader_epoch,
            log_end_offset: l.log_end_offset,
            current_leader_epoch: l.current_leader_epoch,
        })
        .collect()
}

impl Model for RecoveryModel {
    type State = RecoveryState;
    type Action = RecoveryAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![RecoveryState {
            responses: BTreeMap::new(),
            known_leader_epoch: self.known_leader_epoch,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        // Each replica reports at most one log state; fan out over the bounded
        // (epoch, leo, current_epoch) domain. current_epoch ranges one past the
        // known epoch so has_newer_leader is reachable both ways.
        for &node in &self.replicas {
            if state.responses.contains_key(&node) {
                continue;
            }
            for last_written_epoch in 0..=self.max_epoch {
                for leo in 0..=self.max_leo {
                    for current_epoch in self.known_leader_epoch..=(self.known_leader_epoch + 1) {
                        actions.push(RecoveryAction::AddResponse {
                            node,
                            last_written_epoch,
                            leo,
                            current_epoch,
                        });
                    }
                }
            }
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = last.clone();
        match action {
            RecoveryAction::AddResponse {
                node,
                last_written_epoch,
                leo,
                current_epoch,
            } => {
                if state.responses.contains_key(&node) {
                    return None;
                }
                state.responses.insert(
                    node,
                    ReplicaLog {
                        last_written_leader_epoch: last_written_epoch,
                        log_end_offset: leo,
                        current_leader_epoch: current_epoch,
                    },
                );
            }
        }
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // The real select_best_replica returns the true maximum by
            // (last_written_leader_epoch, log_end_offset, then lowest broker_id).
            Property::always("select_best_is_max", |_, s: &RecoveryState| {
                let infos = infos_of(s);
                match select_best_replica(&infos) {
                    None => infos.is_empty(),
                    Some(w) => {
                        let win = infos
                            .iter()
                            .find(|i| i.broker_id == w)
                            .expect("winner is among the inputs");
                        infos.iter().all(|i| {
                            (win.last_written_leader_epoch, win.log_end_offset)
                                .cmp(&(i.last_written_leader_epoch, i.log_end_offset))
                                .then(i.broker_id.cmp(&win.broker_id)) // lower id wins
                                != std::cmp::Ordering::Less
                        })
                    }
                }
            }),
            // The real has_newer_leader matches its specification.
            Property::always("has_newer_leader_matches", |_, s: &RecoveryState| {
                let infos = infos_of(s);
                has_newer_leader(&infos, s.known_leader_epoch)
                    == infos
                        .iter()
                        .any(|i| i.current_leader_epoch > s.known_leader_epoch)
            }),
            Property::sometimes("can_pick_winner", |_, s: &RecoveryState| {
                !s.responses.is_empty()
            }),
            Property::sometimes("can_detect_newer", |_, s: &RecoveryState| {
                s.responses
                    .values()
                    .any(|l| l.current_leader_epoch > s.known_leader_epoch)
            }),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.responses.len() <= self.replicas.len()
            && state.responses.values().all(|l| {
                l.last_written_leader_epoch <= self.max_epoch && l.log_end_offset <= self.max_leo
            })
    }
}

fn run_recovery(model: RecoveryModel, label: &str) {
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
    assert2::assert!(checker.max_depth() < MAX_DEPTH);
    assert2::assert!(checker.state_count() < MAX_STATES);
    checker.assert_properties();
}

#[test]
fn offset_recovery() {
    run_recovery(RecoveryModel::offset_recovery(), "offset_recovery");
}
