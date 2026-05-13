//! Per-partition replica progress tracking, lives on the partition leader.
//!
//! `ReplicaState` records each follower's last-fetched offset (= the
//! follower's persisted LEO from the leader's perspective) and caches
//! the High Watermark = min LEO over the ISR. Slice 10a uses a static
//! ISR (= `replicas` from the metadata image); slice 10b will replace
//! the static install with controller-driven ISR mutation.
//!
//! See `docs/superpowers/specs/2026-05-12-crabka-bulletproof-eos-10a-design.md`.

#![allow(dead_code)] // wired in later batches (slice 10a phase B+)

use std::collections::{HashMap, HashSet};

use crabka_raft::NodeId;

#[derive(Debug, Clone)]
pub(crate) struct ReplicaState {
    pub(crate) isr: HashSet<NodeId>,
    pub(crate) follower_leo: HashMap<NodeId, i64>,
    pub(crate) hw: i64,
}

impl ReplicaState {
    pub(crate) fn new() -> Self {
        Self {
            isr: HashSet::new(),
            follower_leo: HashMap::new(),
            hw: 0,
        }
    }

    /// Install (or reinstall) the ISR membership and seed non-leader
    /// `follower_leo` entries to 0. Idempotent: re-installing the same
    /// `(replicas, leader)` preserves existing follower progress.
    pub(crate) fn install_isr(&mut self, replicas: &[NodeId], leader: NodeId) {
        self.isr = replicas.iter().copied().collect();
        for &r in replicas {
            if r != leader {
                self.follower_leo.entry(r).or_insert(0);
            }
        }
        let isr = self.isr.clone();
        self.follower_leo.retain(|k, _| isr.contains(k));
    }

    pub(crate) fn update_follower_leo(
        &mut self,
        follower: NodeId,
        follower_leo: i64,
        leader_leo: i64,
    ) -> i64 {
        if !self.isr.contains(&follower) {
            return self.recompute_hw_for_leader_append(leader_leo);
        }
        let clamped = follower_leo.min(leader_leo);
        self.follower_leo.insert(follower, clamped);
        self.hw = self.compute_hw(leader_leo);
        self.hw
    }

    pub(crate) fn recompute_hw_for_leader_append(&mut self, leader_leo: i64) -> i64 {
        self.hw = self.compute_hw(leader_leo);
        self.hw
    }

    fn compute_hw(&self, leader_leo: i64) -> i64 {
        if self.isr.is_empty() {
            return leader_leo;
        }
        let mut min_leo = leader_leo;
        for follower in &self.isr {
            if let Some(&leo) = self.follower_leo.get(follower)
                && leo < min_leo
            {
                min_leo = leo;
            }
        }
        min_leo
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh() -> ReplicaState {
        ReplicaState::new()
    }

    #[test]
    fn new_state_has_zero_hw_and_empty_membership() {
        let s = fresh();
        assert_eq!(s.hw, 0);
        assert!(s.isr.is_empty());
        assert!(s.follower_leo.is_empty());
    }

    #[test]
    fn install_isr_seeds_non_leader_followers_at_zero() {
        let mut s = fresh();
        s.install_isr(&[1, 2, 3], 1);
        assert_eq!(s.isr, [1, 2, 3].into_iter().collect());
        assert_eq!(s.follower_leo.get(&2), Some(&0));
        assert_eq!(s.follower_leo.get(&3), Some(&0));
        assert!(!s.follower_leo.contains_key(&1));
    }

    #[test]
    fn install_isr_idempotent_preserves_follower_progress() {
        let mut s = fresh();
        s.install_isr(&[1, 2, 3], 1);
        s.update_follower_leo(2, 50, 100);
        s.update_follower_leo(3, 75, 100);
        s.install_isr(&[1, 2, 3], 1);
        assert_eq!(s.follower_leo.get(&2), Some(&50));
        assert_eq!(s.follower_leo.get(&3), Some(&75));
    }

    #[test]
    fn install_isr_drops_stale_follower_leo_for_removed_replicas() {
        let mut s = fresh();
        s.install_isr(&[1, 2, 3], 1);
        s.update_follower_leo(3, 75, 100);
        s.install_isr(&[1, 2], 1);
        assert!(!s.follower_leo.contains_key(&3));
    }

    #[test]
    fn hw_advances_when_trailing_follower_catches_up() {
        let mut s = fresh();
        s.install_isr(&[1, 2, 3], 1);
        let hw1 = s.update_follower_leo(2, 50, 100);
        assert_eq!(hw1, 0);
        let hw2 = s.update_follower_leo(3, 75, 100);
        assert_eq!(hw2, 50);
        let hw3 = s.update_follower_leo(2, 80, 100);
        assert_eq!(hw3, 75);
    }

    #[test]
    fn hw_pins_at_slowest_isr_follower() {
        let mut s = fresh();
        s.install_isr(&[1, 2, 3], 1);
        s.update_follower_leo(2, 100, 100);
        s.update_follower_leo(3, 30, 100);
        assert_eq!(s.hw, 30);
    }

    #[test]
    fn non_isr_follower_leo_update_uses_leader_path() {
        let mut s = fresh();
        s.install_isr(&[1, 2], 1);
        // Node 3 is not in ISR. Its report falls through to
        // recompute_hw_for_leader_append. follower_leo[2] = 0 from
        // install, so HW = min(100, 0) = 0.
        let hw = s.update_follower_leo(3, 999, 100);
        assert_eq!(hw, 0);
        assert_eq!(s.hw, 0);
    }

    #[test]
    fn single_replica_isr_hw_equals_leader_leo() {
        let mut s = fresh();
        s.install_isr(&[1], 1);
        let hw = s.recompute_hw_for_leader_append(42);
        assert_eq!(hw, 42);
    }

    #[test]
    fn follower_overshoot_clamps_to_leader_leo() {
        let mut s = fresh();
        s.install_isr(&[1, 2], 1);
        let hw = s.update_follower_leo(2, 200, 100);
        assert_eq!(hw, 100);
        assert_eq!(s.follower_leo.get(&2), Some(&100));
    }

    #[test]
    fn empty_isr_hw_equals_leader_leo() {
        let mut s = fresh();
        let hw = s.recompute_hw_for_leader_append(50);
        assert_eq!(hw, 50);
    }
}
