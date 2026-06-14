//! Exhaustive stateright model of the pure KIP-455 reassignment-completion core
//! (`reassign_one`). See
//! `docs/superpowers/specs/2026-06-13-crabka-reassignment-model-design.md`.
//!
//! The full model is added in Task 2. This file starts as a wiring smoke test.
//!
//! Memory safety: stateright BFS keeps every visited unique state resident, so
//! each checker run is fenced with `within_boundary` + `target_state_count` +
//! `timeout` and MUST be run under the host memory watchdog while bounds are
//! tuned.

use std::collections::HashSet;

use crabka_metadata::PartitionRecord;

use super::reassign_one;

#[test]
fn reassign_one_completes_smoke() {
    // replicas=[1,2,3] adding=[3] removing=[2], all in ISR, leader=1 → completes
    // to target [1,3].
    let pr = PartitionRecord {
        topic: "t".to_string(),
        partition: 0,
        leader: 1,
        replicas: vec![1, 2, 3],
        isr: vec![1, 2, 3],
        leader_epoch: 5,
        adding_replicas: vec![3],
        removing_replicas: vec![2],
        directories: vec![],
        partition_epoch: 0,
    };
    let alive: HashSet<u64> = [1, 2, 3].into_iter().collect();
    let next = reassign_one(&pr, &alive).expect("a completion update");
    assert_eq!(next.replicas, vec![1, 3]);
    assert_eq!(next.isr, vec![1, 3]);
    assert!(next.adding_replicas.is_empty());
    assert!(next.removing_replicas.is_empty());
    assert_eq!(next.leader, 1);
}
