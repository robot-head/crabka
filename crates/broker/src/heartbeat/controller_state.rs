//! Controller-side liveness tracking for KIP-500 broker heartbeats.
//!
//! `ControllerLivenessState` tracks the last-seen timestamp for every
//! registered broker and drives a periodic liveness ticker that emits
//! `LivenessTransition` events when a broker goes dead or comes alive.

// Items are consumed by the BrokerHeartbeat handler and the liveness
// ticker spawn. Allow dead_code until those land.
#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
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
    /// Brokers that signaled `want_shut_down=true` on a recent
    /// heartbeat. The controller tries to move leadership away from
    /// these brokers and returns `should_shut_down=true` once every
    /// partition has been re-led.
    wants_shutdown: Mutex<HashSet<u64>>,
}

impl ControllerLivenessState {
    /// Create a new registry with the given heartbeat timeout.
    pub(crate) fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            brokers: Mutex::new(HashMap::new()),
            wants_shutdown: Mutex::new(HashSet::new()),
        }
    }

    /// Record whether `broker_id` is currently asking to shut down.
    /// `true` adds to the set; `false` removes (covers a broker that
    /// retracts the request, though in practice the controller only
    /// clears state when the broker is observed dead).
    pub(crate) async fn set_wants_shutdown(&self, broker_id: u64, want: bool) {
        let mut set = self.wants_shutdown.lock().await;
        if want {
            set.insert(broker_id);
        } else {
            set.remove(&broker_id);
        }
    }

    /// Returns `true` if `broker_id` is currently in the wants-shutdown
    /// set.
    pub(crate) async fn wants_shutdown(&self, broker_id: u64) -> bool {
        self.wants_shutdown.lock().await.contains(&broker_id)
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

    /// Return `true` if `broker_id` is currently `Alive` (has sent a
    /// heartbeat within the timeout window). Returns `false` for unknown
    /// brokers and for brokers whose heartbeat has expired.
    pub(crate) async fn is_alive(&self, broker_id: u64) -> bool {
        matches!(
            self.state(broker_id).await,
            Some(BrokerLivenessState::Alive)
        )
    }

    /// Snapshot the set of currently-`Alive` broker ids under a single
    /// lock acquisition. Equivalent to calling [`is_alive`](Self::is_alive)
    /// for every broker, but the cluster-wide maintenance loops (failover,
    /// rebalance, metrics) take the `brokers` lock once and then do
    /// synchronous set-membership checks instead of one `.await` lock per
    /// partition. Unknown brokers are absent from the set (so membership
    /// `false` == not alive), matching `is_alive`'s predicate exactly.
    pub(crate) async fn alive_snapshot(&self) -> HashSet<u64> {
        let map = self.brokers.lock().await;
        map.iter()
            .filter(|(_, e)| e.state == BrokerLivenessState::Alive)
            .map(|(&id, _)| id)
            .collect()
    }

    /// Seed the liveness registry with the given broker ids. Each id that
    /// is not already present is inserted as `Alive` with
    /// `last_heartbeat = now`. This is called when this broker becomes the
    /// raft leader so that peers which stop heartbeating (because they are
    /// dead) will be detected by [`tick`](Self::tick) after `timeout` ms
    /// even if they never sent a heartbeat to this specific node before.
    ///
    /// Ids already in the registry are left untouched (their existing
    /// `last_heartbeat` and `state` are preserved).
    pub(crate) async fn seed_brokers(&self, broker_ids: impl IntoIterator<Item = u64>) {
        let mut map = self.brokers.lock().await;
        let now = Instant::now();
        for id in broker_ids {
            map.entry(id).or_insert(BrokerEntry {
                last_heartbeat: now,
                state: BrokerLivenessState::Alive,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use std::time::Duration;

    #[tokio::test]
    async fn new_broker_starts_alive_after_first_heartbeat() {
        let liveness = ControllerLivenessState::new(Duration::from_secs(10));
        let transition = liveness.record_heartbeat(1).await;
        assert!(transition == None); // first heartbeat: not a revival
        assert!(liveness.state(1).await == Some(BrokerLivenessState::Alive));
    }

    #[tokio::test]
    async fn tick_marks_expired_broker_dead() {
        // Use a very short timeout so we can expire without sleeping.
        let liveness = ControllerLivenessState::new(Duration::from_nanos(1));
        liveness.record_heartbeat(2).await;
        // Spin until at least 1ns has elapsed (nearly instant on any OS).
        std::thread::sleep(Duration::from_millis(1));
        let transitions = liveness.tick().await;
        assert!(transitions == vec![LivenessTransition::AliveToDead(2)]);
        assert!(liveness.state(2).await == Some(BrokerLivenessState::Dead));
    }

    #[tokio::test]
    async fn heartbeat_after_dead_emits_revival() {
        let liveness = ControllerLivenessState::new(Duration::from_nanos(1));
        liveness.record_heartbeat(3).await;
        std::thread::sleep(Duration::from_millis(1));
        let _ = liveness.tick().await; // broker 3 → Dead
        let transition = liveness.record_heartbeat(3).await;
        assert!(transition == Some(LivenessTransition::DeadToAlive(3)));
        assert!(liveness.state(3).await == Some(BrokerLivenessState::Alive));
    }

    #[tokio::test]
    async fn wants_shutdown_set_and_unset() {
        let liveness = ControllerLivenessState::new(Duration::from_secs(10));
        assert!(!liveness.wants_shutdown(5).await);
        liveness.set_wants_shutdown(5, true).await;
        assert!(liveness.wants_shutdown(5).await);
        liveness.set_wants_shutdown(5, false).await;
        assert!(!liveness.wants_shutdown(5).await);
    }

    #[tokio::test]
    async fn wants_shutdown_is_per_broker() {
        let liveness = ControllerLivenessState::new(Duration::from_secs(10));
        liveness.set_wants_shutdown(1, true).await;
        liveness.set_wants_shutdown(2, true).await;
        assert!(liveness.wants_shutdown(1).await);
        assert!(liveness.wants_shutdown(2).await);
        assert!(!liveness.wants_shutdown(3).await);
        liveness.set_wants_shutdown(1, false).await;
        assert!(!liveness.wants_shutdown(1).await);
        assert!(liveness.wants_shutdown(2).await);
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
        assert!(liveness.state(4).await == Some(BrokerLivenessState::Alive));
    }
}
