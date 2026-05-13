//! Controller-side liveness tracking for KIP-500 broker heartbeats.
//!
//! `ControllerLivenessState` tracks the last-seen timestamp for every
//! registered broker and drives a periodic liveness ticker that emits
//! `LivenessTransition` events when a broker goes dead or comes alive.

// Items are consumed by the BrokerHeartbeat handler (Task 13) and the
// liveness ticker spawn (Task 14). Allow dead_code until those land.
#![allow(dead_code)]

use std::collections::HashMap;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

/// Per-broker liveness state as seen by the controller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BrokerLivenessState {
    /// Broker has sent a heartbeat within the timeout window.
    Alive,
    /// No heartbeat received within the timeout window.
    Dead,
}

/// An edge transition emitted by [`ControllerLivenessState::tick`] or
/// [`ControllerLivenessState::record_heartbeat`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LivenessTransition {
    /// Broker was `Dead`; this heartbeat revived it.
    DeadToAlive(u64),
    /// Broker crossed the deadline; marked `Dead`.
    AliveToDead(u64),
}

struct BrokerEntry {
    last_heartbeat: Instant,
    state: BrokerLivenessState,
}

/// Controller-side heartbeat registry.
///
/// One instance lives on the `Broker` struct. Handlers call
/// [`record_heartbeat`](Self::record_heartbeat) on every incoming
/// `BrokerHeartbeat` RPC; the liveness ticker calls [`tick`](Self::tick)
/// every second to expire stale entries.
pub(crate) struct ControllerLivenessState {
    timeout: Duration,
    brokers: Mutex<HashMap<u64, BrokerEntry>>,
}

impl ControllerLivenessState {
    /// Create a new registry with the given heartbeat timeout.
    pub(crate) fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            brokers: Mutex::new(HashMap::new()),
        }
    }

    /// Record a heartbeat from `broker_id`. Returns `Some(DeadToAlive)`
    /// if this heartbeat revives a previously-dead broker, `None` if the
    /// broker was already alive (or is new).
    pub(crate) async fn record_heartbeat(&self, broker_id: u64) -> Option<LivenessTransition> {
        let mut map = self.brokers.lock().await;
        let now = Instant::now();
        let entry = map.entry(broker_id).or_insert(BrokerEntry {
            last_heartbeat: now,
            state: BrokerLivenessState::Alive,
        });
        let prev = entry.state;
        entry.last_heartbeat = now;
        entry.state = BrokerLivenessState::Alive;
        if prev == BrokerLivenessState::Dead {
            Some(LivenessTransition::DeadToAlive(broker_id))
        } else {
            None
        }
    }

    /// Scan all registered brokers and mark those that have not sent a
    /// heartbeat within `timeout` as `Dead`. Returns the list of
    /// transitions that occurred this tick.
    pub(crate) async fn tick(&self) -> Vec<LivenessTransition> {
        let mut map = self.brokers.lock().await;
        let now = Instant::now();
        let mut transitions = Vec::new();
        for (&id, entry) in map.iter_mut() {
            if entry.state == BrokerLivenessState::Alive
                && now.duration_since(entry.last_heartbeat) > self.timeout
            {
                entry.state = BrokerLivenessState::Dead;
                transitions.push(LivenessTransition::AliveToDead(id));
            }
        }
        transitions
    }

    /// Return the current liveness state for `broker_id`, or `None` if
    /// the broker has never sent a heartbeat.
    pub(crate) async fn state(&self, broker_id: u64) -> Option<BrokerLivenessState> {
        let map = self.brokers.lock().await;
        map.get(&broker_id).map(|e| e.state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn new_broker_starts_alive_after_first_heartbeat() {
        let liveness = ControllerLivenessState::new(Duration::from_secs(10));
        let transition = liveness.record_heartbeat(1).await;
        assert_eq!(transition, None); // first heartbeat: not a revival
        assert_eq!(liveness.state(1).await, Some(BrokerLivenessState::Alive));
    }

    #[tokio::test]
    async fn tick_marks_expired_broker_dead() {
        // Use a very short timeout so we can expire without sleeping.
        let liveness = ControllerLivenessState::new(Duration::from_nanos(1));
        liveness.record_heartbeat(2).await;
        // Spin until at least 1ns has elapsed (nearly instant on any OS).
        std::thread::sleep(Duration::from_millis(1));
        let transitions = liveness.tick().await;
        assert_eq!(transitions, vec![LivenessTransition::AliveToDead(2)]);
        assert_eq!(liveness.state(2).await, Some(BrokerLivenessState::Dead));
    }

    #[tokio::test]
    async fn heartbeat_after_dead_emits_revival() {
        let liveness = ControllerLivenessState::new(Duration::from_nanos(1));
        liveness.record_heartbeat(3).await;
        std::thread::sleep(Duration::from_millis(1));
        let _ = liveness.tick().await; // broker 3 → Dead
        let transition = liveness.record_heartbeat(3).await;
        assert_eq!(transition, Some(LivenessTransition::DeadToAlive(3)));
        assert_eq!(liveness.state(3).await, Some(BrokerLivenessState::Alive));
    }

    #[tokio::test]
    async fn tick_does_not_expire_recently_heartbeated_broker() {
        let liveness = ControllerLivenessState::new(Duration::from_mins(1));
        liveness.record_heartbeat(4).await;
        let transitions = liveness.tick().await;
        assert!(
            transitions.is_empty(),
            "broker 4 should not expire with 60s timeout"
        );
        assert_eq!(liveness.state(4).await, Some(BrokerLivenessState::Alive));
    }
}
