//! Exhaustive stateright models of the controller leader-failover decision
//! (`failover_one`) and the KIP-966 winner selection (Task 3). See
//! `docs/superpowers/specs/2026-06-13-crabka-failover-recovery-model-design.md`.
//!
//! Memory safety: stateright BFS keeps every visited unique state resident, so
//! each run is fenced with `within_boundary` + `target_state_count` + `timeout`
//! and MUST be executed under the host memory watchdog while bounds are tuned.

use std::collections::{BTreeSet, HashSet};
use std::time::Duration;

use crabka_metadata::PartitionRecord;
use crabka_raft::NodeId;
use stateright::{Checker, Model, Property};

use super::{FailoverDecision, failover_one};
use crate::config_keys::RecoveryStrategy;

const MAX_STATES: usize = 200_000;
const MAX_DEPTH: usize = 80;
const CHECK_TIMEOUT: Duration = Duration::from_secs(120);

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
            replicas: vec![1, 2, 3],
            strategy,
            unclean_enabled,
            max_epoch: 4,
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
        leader_epoch: s.leader_epoch,
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
            assert!(*leader != dead, "elected the dead broker {dead}");
            assert!(
                pre.alive.contains(leader),
                "elected leader {leader} not alive"
            );
            assert!(
                isr.contains(leader),
                "elected leader {leader} not in new ISR {isr:?}"
            );
            if *unclean {
                assert!(unclean_enabled, "unclean election without unclean_enabled");
            } else {
                // Clean election: the new leader was in the pre-failover ISR, so
                // it holds every committed record. No data loss.
                assert!(
                    pre.isr.contains(leader),
                    "clean election picked {leader} not in pre-failover ISR {:?} (data loss!)",
                    pre.isr
                );
            }
        }
        FailoverDecision::ShrinkIsr { isr } => {
            assert!(
                isr.iter().all(|n| pre.isr.contains(n)),
                "shrink introduced a non-member: {isr:?} vs {:?}",
                pre.isr
            );
            assert!(isr.len() < pre.isr.len(), "ShrinkIsr did not shrink");
        }
        FailoverDecision::Recover(s) => {
            assert!(*s != RecoveryStrategy::None, "Recover with strategy None");
            assert!(
                pre.leader == dead,
                "Recover when the dead broker was not leader"
            );
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
