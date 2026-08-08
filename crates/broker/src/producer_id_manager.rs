//! Allocates `(producer_id, producer_epoch)` pairs.
//!
//! This is the single-broker MVP: the id space is a single monotonic
//! counter. Transactions will revisit this when transactional ids arrive.

use std::sync::atomic::{AtomicI16, AtomicI64, Ordering};

use crabka_log::ProducerId;
use dashmap::DashMap;

/// Lowest pid handed out. This mirrors Apache Kafka's `0` initial range.
/// Crabka starts above the legacy non-idempotent sentinel of `-1`.
const PID_BASE: i64 = 1000;

#[derive(Debug)]
pub struct ProducerIdManager {
    next_pid: AtomicI64,
    epochs: DashMap<ProducerId, AtomicI16>,
}

impl Default for ProducerIdManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ProducerIdManager {
    #[must_use]
    pub fn new() -> Self {
        Self {
            next_pid: AtomicI64::new(PID_BASE),
            epochs: DashMap::new(),
        }
    }

    /// Allocate a fresh `(producer_id, producer_epoch=0)`.
    pub fn allocate(&self) -> (ProducerId, i16) {
        let pid = ProducerId(self.next_pid.fetch_add(1, Ordering::Relaxed));
        self.epochs.insert(pid, AtomicI16::new(0));
        (pid, 0)
    }

    /// Bump the epoch for an existing pid. Transactional producers call it
    /// when they re-initialise under the same `transactional_id`. Returns
    /// the new epoch.
    ///
    /// Transactional producers call it on `InitProducerId` re-init.
    #[allow(dead_code)]
    pub fn bump_epoch(&self, pid: ProducerId) -> Option<i16> {
        self.epochs
            .get(&pid)
            .map(|e| e.value().fetch_add(1, Ordering::Relaxed) + 1)
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn allocate_returns_monotonic_pids_starting_at_base() {
        let m = ProducerIdManager::new();
        for want_pid in [PID_BASE, PID_BASE + 1, PID_BASE + 2] {
            assert!(m.allocate() == (ProducerId(want_pid), 0));
        }
    }

    #[test]
    fn bump_epoch_increments() {
        let m = ProducerIdManager::new();
        let (pid, _) = m.allocate();
        for (bump_pid, want) in [(pid, Some(1)), (pid, Some(2)), (ProducerId(9999), None)] {
            assert!(m.bump_epoch(bump_pid) == want, "pid {bump_pid}");
        }
    }
}
