//! Per-partition replica progress tracking, lives on the partition leader.
//!
//! `ReplicaState` records each follower's last-fetched offset (= the
//! follower's persisted LEO from the leader's perspective) and caches
//! the High Watermark = min LEO over the ISR. ISR-lag
//! tracking via `FollowerStats` (`last_fetch`, `last_caught_up`) lets
//! the `isr_maintenance` task can shrink/expand the ISR.

#![allow(dead_code)] // wired in by the ISR-maintenance path

use std::{
    collections::{HashMap, HashSet},
    time::Instant,
};

use crabka_log::Offset;
use crabka_raft::NodeId;

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct FollowerStats {
    pub(crate) leo: Offset,
    pub(crate) last_fetch: Instant,
    pub(crate) last_caught_up: Instant,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ReplicaState {
    pub(crate) isr: HashSet<NodeId>,
    pub(crate) per_follower: HashMap<NodeId, FollowerStats>,
    pub(crate) hw: Offset,
    pub(crate) current_leader_epoch: i32,
}

impl ReplicaState {
    pub(crate) fn new() -> Self {
        Self {
            isr: HashSet::new(),
            per_follower: HashMap::new(),
            hw: Offset(0),
            current_leader_epoch: 0,
        }
    }

    /// Install (or reinstall) the ISR membership and seed non-leader
    /// `per_follower` entries to zero. Idempotent: re-installing the same
    /// `(isr, replicas, leader)` preserves existing follower progress.
    ///
    /// `isr` is the committed in-sync set; `replicas` is the full replica
    /// assignment. `per_follower` is keyed by the **replica set** (minus
    /// the leader), not the ISR: a replica that has been shrunk out of the
    /// ISR — or hasn't yet rejoined after a restart — is still catching up
    /// via follower-fetch, and its fetch-driven `last_caught_up` is exactly
    /// what `isr_maintenance` reads to expand it back in. Keying retention
    /// on the ISR instead would discard that progress on every
    /// metadata-image reconcile and starve ISR re-admission under image
    /// churn. Only nodes no longer in the replica set (e.g. removed by a
    /// reassignment) are dropped.
    pub(crate) fn install_isr(
        &mut self,
        isr: &[NodeId],
        replicas: &[NodeId],
        leader: NodeId,
        now: Instant,
    ) {
        self.isr = isr.iter().copied().collect();
        // Seed only ISR members: seeding a non-ISR replica with
        // `last_caught_up = now` would let `isr_maintenance` falsely
        // re-admit a replica that has not actually fetched up to the LEO.
        for &r in isr {
            if r != leader {
                self.per_follower.entry(r).or_insert(FollowerStats {
                    leo: Offset(0),
                    last_fetch: now,
                    last_caught_up: now,
                });
            }
        }
        let keep: HashSet<NodeId> = replicas.iter().copied().collect();
        self.per_follower.retain(|k, _| keep.contains(k));
    }

    pub(crate) fn update_follower_leo(
        &mut self,
        follower: NodeId,
        follower_leo: Offset,
        leader_leo: Offset,
        now: Instant,
    ) -> Offset {
        if !self.isr.contains(&follower) {
            // Track stats so isr_maintenance can expand back when caught up.
            let stats = self.per_follower.entry(follower).or_insert(FollowerStats {
                leo: Offset(0),
                last_fetch: now,
                last_caught_up: now,
            });
            stats.last_fetch = now;
            stats.leo = follower_leo.min(leader_leo);
            if stats.leo >= leader_leo {
                stats.last_caught_up = now;
            }
            return self.recompute_hw_for_leader_append(leader_leo);
        }
        let clamped = follower_leo.min(leader_leo);
        let stats = self.per_follower.entry(follower).or_insert(FollowerStats {
            leo: Offset(0),
            last_fetch: now,
            last_caught_up: now,
        });
        stats.leo = clamped;
        stats.last_fetch = now;
        if clamped >= leader_leo {
            stats.last_caught_up = now;
        }
        self.hw = self.compute_hw(leader_leo);
        self.hw
    }

    pub(crate) fn recompute_hw_for_leader_append(&mut self, leader_leo: Offset) -> Offset {
        self.hw = self.compute_hw(leader_leo);
        self.hw
    }

    fn compute_hw(&self, leader_leo: Offset) -> Offset {
        if self.isr.is_empty() {
            return leader_leo;
        }
        let mut min_leo = leader_leo;
        for follower in &self.isr {
            if let Some(stats) = self.per_follower.get(follower)
                && stats.leo < min_leo
            {
                min_leo = stats.leo;
            }
        }
        min_leo
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use assert2::assert;

    use super::*;

    /// Shorthand for wrapping a raw offset in the test asserts below.
    fn o(v: i64) -> Offset {
        Offset(v)
    }

    fn fresh() -> ReplicaState {
        ReplicaState::new()
    }

    fn now() -> Instant {
        Instant::now()
    }

    #[test]
    fn new_state_has_zero_hw_and_empty_membership() {
        let s = fresh();
        let expected = ReplicaState {
            isr: HashSet::new(),
            per_follower: HashMap::new(),
            hw: Offset(0),
            current_leader_epoch: 0,
        };
        assert!(s == expected);
    }

    #[test]
    fn install_isr_seeds_non_leader_followers_at_zero() {
        let mut s = fresh();
        let t = now();
        s.install_isr(&[1, 2, 3], &[1, 2, 3], 1, t);
        let seeded = FollowerStats {
            leo: Offset(0),
            last_fetch: t,
            last_caught_up: t,
        };
        // Only the non-leader followers (2 and 3) are seeded; the leader (1)
        // gets no per_follower entry.
        let expected = ReplicaState {
            isr: [1, 2, 3].into_iter().collect(),
            per_follower: [(2, seeded), (3, seeded)].into_iter().collect(),
            hw: Offset(0),
            current_leader_epoch: 0,
        };
        assert!(s == expected);
    }

    #[test]
    fn install_isr_idempotent_preserves_follower_progress() {
        let mut s = fresh();
        s.install_isr(&[1, 2, 3], &[1, 2, 3], 1, now());
        s.update_follower_leo(2, o(50), o(100), now());
        s.update_follower_leo(3, o(75), o(100), now());
        s.install_isr(&[1, 2, 3], &[1, 2, 3], 1, now());
        assert!(s.per_follower.get(&2).map(|f| f.leo) == Some(o(50)));
        assert!(s.per_follower.get(&3).map(|f| f.leo) == Some(o(75)));
    }

    #[test]
    fn install_isr_drops_stale_follower_leo_for_removed_replicas() {
        // Node 3 leaves the *replica set* entirely (e.g. reassignment) →
        // its progress entry is dropped.
        let mut s = fresh();
        s.install_isr(&[1, 2, 3], &[1, 2, 3], 1, now());
        s.update_follower_leo(3, o(75), o(100), now());
        s.install_isr(&[1, 2], &[1, 2], 1, now());
        assert!(!s.per_follower.contains_key(&3));
    }

    #[test]
    fn install_isr_keeps_catching_up_replica_shrunk_from_isr() {
        // Node 3 is shrunk out of the ISR but stays a replica (it's
        // catching back up). Its fetch-driven progress must survive an
        // ISR reinstall so isr_maintenance can later expand it back in.
        let mut s = fresh();
        s.install_isr(&[1, 2, 3], &[1, 2, 3], 1, now());
        s.update_follower_leo(3, o(75), o(100), now());
        // Committed ISR shrinks to {1,2}; replica set is still {1,2,3}.
        s.install_isr(&[1, 2], &[1, 2, 3], 1, now());
        assert!(
            s.per_follower.contains_key(&3),
            "a replica catching up toward ISR re-admission must keep its progress"
        );
        assert!(s.per_follower.get(&3).map(|f| f.leo) == Some(o(75)));
    }

    #[test]
    fn hw_advances_when_trailing_follower_catches_up() {
        let mut s = fresh();
        s.install_isr(&[1, 2, 3], &[1, 2, 3], 1, now());
        // Ordered steps over shared state: (follower, follower_leo, expected_hw).
        let steps = [(2, 50, 0), (3, 75, 50), (2, 80, 75)];
        for (follower, leo, expected_hw) in steps {
            let hw = s.update_follower_leo(follower, o(leo), o(100), now());
            assert!(hw == o(expected_hw), "step: follower {follower} leo {leo}");
        }
    }

    #[test]
    fn hw_pins_at_slowest_isr_follower() {
        let mut s = fresh();
        s.install_isr(&[1, 2, 3], &[1, 2, 3], 1, now());
        s.update_follower_leo(2, o(100), o(100), now());
        s.update_follower_leo(3, o(30), o(100), now());
        assert!(s.hw == o(30));
    }

    #[test]
    fn non_isr_follower_leo_update_uses_leader_path() {
        let mut s = fresh();
        s.install_isr(&[1, 2], &[1, 2], 1, now());
        // Node 3 is not in ISR. Its progress is tracked for possible
        // re-admission, but it is excluded from HW; per_follower[2] = 0 from
        // install, so HW = min(100, 0) = 0.
        let hw = s.update_follower_leo(3, o(999), o(100), now());
        assert!(hw == o(0));
        assert!(s.hw == o(0));
    }

    #[test]
    fn single_replica_isr_hw_equals_leader_leo() {
        let mut s = fresh();
        s.install_isr(&[1], &[1], 1, now());
        let hw = s.recompute_hw_for_leader_append(o(42));
        assert!(hw == o(42));
    }

    #[test]
    fn follower_overshoot_clamps_to_leader_leo() {
        let mut s = fresh();
        s.install_isr(&[1, 2], &[1, 2], 1, now());
        let hw = s.update_follower_leo(2, o(200), o(100), now());
        assert!(hw == o(100));
        assert!(s.per_follower.get(&2).map(|f| f.leo) == Some(o(100)));
    }

    #[test]
    fn empty_isr_hw_equals_leader_leo() {
        let mut s = fresh();
        let hw = s.recompute_hw_for_leader_append(o(50));
        assert!(hw == o(50));
    }

    #[test]
    fn update_follower_leo_advances_last_fetch_time() {
        // Deterministic: pass explicit ordered instants instead of sleeping.
        let mut s = fresh();
        let t0 = Instant::now();
        s.install_isr(&[1, 2], &[1, 2], 1, t0);
        let t_install = s.per_follower.get(&2).unwrap().last_fetch;
        let t1 = t0 + Duration::from_millis(10);
        s.update_follower_leo(2, o(5), o(10), t1);
        let t_after = s.per_follower.get(&2).unwrap().last_fetch;
        assert!(t_after > t_install);
    }

    #[test]
    fn last_caught_up_set_when_leo_reaches_leader_leo() {
        // Deterministic: pass explicit ordered instants instead of sleeping.
        let mut s = fresh();
        let t0 = Instant::now();
        s.install_isr(&[1, 2], &[1, 2], 1, t0);
        let t1 = t0 + Duration::from_millis(10);
        s.update_follower_leo(2, o(5), o(10), t1);
        let lag = s.per_follower.get(&2).unwrap().last_caught_up;
        let lag_fetch = s.per_follower.get(&2).map(|f| f.last_fetch).unwrap();
        // Not yet caught up — last_caught_up is the install time (t0), which is
        // strictly before the most recent fetch time (t1).
        assert!(lag <= lag_fetch);
        let t2 = t1 + Duration::from_millis(10);
        s.update_follower_leo(2, o(10), o(10), t2);
        let lag2 = s.per_follower.get(&2).unwrap().last_caught_up;
        assert!(lag2 > lag);
    }

    #[test]
    fn non_isr_follower_refreshes_last_caught_up_only_when_caught_up() {
        let mut s = fresh();
        let t0 = Instant::now();
        s.install_isr(&[1, 2, 3], &[1, 2, 3], 1, t0);

        let t_caught_in_isr = t0 + Duration::from_millis(10);
        s.update_follower_leo(3, o(10), o(10), t_caught_in_isr);
        assert!(s.per_follower.get(&3).unwrap().last_caught_up == t_caught_in_isr);

        s.install_isr(&[1, 2], &[1, 2, 3], 1, t_caught_in_isr);
        let t_lagging = t_caught_in_isr + Duration::from_millis(10);
        s.update_follower_leo(3, o(9), o(10), t_lagging);
        let lagging = s.per_follower.get(&3).unwrap();
        assert!(lagging.last_fetch == t_lagging);
        assert!(lagging.last_caught_up == t_caught_in_isr);

        let t_caught_out_of_isr = t_lagging + Duration::from_millis(10);
        s.update_follower_leo(3, o(10), o(10), t_caught_out_of_isr);
        assert!(s.per_follower.get(&3).unwrap().last_caught_up == t_caught_out_of_isr);
    }
}

#[cfg(test)]
#[path = "replica_state_model.rs"]
mod replica_state_model;
