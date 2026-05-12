//! Allocates `(producer_id, producer_epoch)` pairs. Single-broker MVP:
//! the id space is a single monotonic counter. Slice 9 (transactions)
//! will revisit when transactional ids enter the picture.

use std::sync::atomic::{AtomicI16, AtomicI64, Ordering};

use dashmap::DashMap;

/// Lowest pid handed out. Mirrors Apache Kafka's `0` initial range
/// (we start above the legacy non-idempotent sentinel of `-1`).
const PID_BASE: i64 = 1000;

#[derive(Debug)]
#[allow(dead_code)] // fields used via methods; handler wiring lands in Tasks 7-8
pub struct ProducerIdManager {
    next_pid: AtomicI64,
    epochs: DashMap<i64, AtomicI16>,
}

impl Default for ProducerIdManager {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(dead_code)] // allocate + bump_epoch wired in Tasks 7-8
impl ProducerIdManager {
    #[must_use]
    pub fn new() -> Self {
        Self {
            next_pid: AtomicI64::new(PID_BASE),
            epochs: DashMap::new(),
        }
    }

    /// Allocate a fresh `(producer_id, producer_epoch=0)`.
    pub fn allocate(&self) -> (i64, i16) {
        let pid = self.next_pid.fetch_add(1, Ordering::Relaxed);
        self.epochs.insert(pid, AtomicI16::new(0));
        (pid, 0)
    }

    /// Bump the epoch for an existing pid. Used by transactional producers
    /// re-initialising under the same `transactional_id`. Returns the new
    /// epoch.
    pub fn bump_epoch(&self, pid: i64) -> Option<i16> {
        self.epochs
            .get(&pid)
            .map(|e| e.value().fetch_add(1, Ordering::Relaxed) + 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocate_returns_monotonic_pids_starting_at_base() {
        let m = ProducerIdManager::new();
        assert_eq!(m.allocate(), (PID_BASE, 0));
        assert_eq!(m.allocate(), (PID_BASE + 1, 0));
        assert_eq!(m.allocate(), (PID_BASE + 2, 0));
    }

    #[test]
    fn bump_epoch_increments() {
        let m = ProducerIdManager::new();
        let (pid, _) = m.allocate();
        assert_eq!(m.bump_epoch(pid), Some(1));
        assert_eq!(m.bump_epoch(pid), Some(2));
        assert_eq!(m.bump_epoch(9999), None);
    }
}
