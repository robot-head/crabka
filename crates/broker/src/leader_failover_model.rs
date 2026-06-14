//! Exhaustive stateright models of the controller leader-failover decision
//! (`failover_one`) and the KIP-966 winner selection
//! (`crate::unclean_recovery::select_best_replica` / `has_newer_leader`). See
//! `docs/superpowers/specs/2026-06-13-crabka-failover-recovery-model-design.md`.
//!
//! The full models are added in Tasks 2-3. This file starts as a wiring smoke
//! test.
//!
//! Memory safety: stateright BFS keeps every visited unique state resident, so
//! each checker run is fenced with `within_boundary` + `target_state_count` +
//! `timeout` and MUST be run under the host memory watchdog while bounds are
//! tuned.

use std::collections::HashSet;

use crabka_metadata::PartitionRecord;

use super::{FailoverDecision, failover_one};
use crate::config_keys::RecoveryStrategy;

#[test]
fn failover_one_clean_election_smoke() {
    // Leader 1 dies with {2,3} alive in the ISR → clean elect of 2.
    let pr = PartitionRecord {
        topic: "t".to_string(),
        partition: 0,
        leader: 1,
        replicas: vec![1, 2, 3],
        isr: vec![1, 2, 3],
        leader_epoch: 0,
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 0,
    };
    let alive: HashSet<u64> = [2, 3].into_iter().collect();
    let d = failover_one(&pr, 1, &alive, RecoveryStrategy::None, false);
    assert_eq!(
        d,
        FailoverDecision::Elect {
            leader: 2,
            isr: vec![2, 3],
            unclean: false
        }
    );
}
