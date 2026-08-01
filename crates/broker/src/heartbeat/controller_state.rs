//! Controller-side liveness tracking for KIP-500 broker heartbeats.
//!
//! `ControllerLivenessState` tracks the last-seen timestamp for every
//! registered broker and drives a periodic liveness ticker that emits
//! `LivenessTransition` events when a broker goes dead or comes alive.

// Items are consumed by the BrokerHeartbeat handler and the liveness
// ticker spawn. Allow dead_code until those land.
#![allow(dead_code)]

use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

use crabka_units::{Time, convert::TimeExt as _};
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

/// Monotonic time source for liveness tracking.
///
/// Production uses the real clock. Tests use a controllable clock so the
/// liveness windows are driven by explicit advances instead of wall-clock
/// `std::thread::sleep` — sleeps flake when the CI runner is loaded, because the
/// gap between a re-seed and the next `tick` can exceed a short timeout and
/// spuriously mark a broker dead.
enum Clock {
    Real,
    #[cfg(test)]
    Test(std::sync::Arc<TestClockInner>),
}

impl Clock {
    fn now(&self) -> Instant {
        match self {
            Clock::Real => Instant::now(),
            #[cfg(test)]
            Clock::Test(inner) => {
                inner.base
                    + Duration::from_nanos(
                        inner
                            .offset_nanos
                            .load(std::sync::atomic::Ordering::Relaxed),
                    )
            }
        }
    }
}

#[cfg(test)]
struct TestClockInner {
    base: Instant,
    offset_nanos: std::sync::atomic::AtomicU64,
}

/// Test handle for the controllable [`Clock`]. Shares its inner state with the
/// `Clock::Test` handed to [`ControllerLivenessState::with_clock`], so `advance`
/// is observed by the liveness state under test.
#[cfg(test)]
struct TestClock(std::sync::Arc<TestClockInner>);

#[cfg(test)]
impl TestClock {
    fn new() -> Self {
        Self(std::sync::Arc::new(TestClockInner {
            base: Instant::now(),
            // Start an hour in so `seed_brokers_expiring_after`'s `checked_sub`
            // never underflows the monotonic clock in short-lived test processes.
            offset_nanos: std::sync::atomic::AtomicU64::new(3_600_000_000_000),
        }))
    }

    fn advance(&self, by: Duration) {
        self.0.offset_nanos.fetch_add(
            u64::try_from(by.as_nanos()).expect("advance fits u64 nanos"),
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    fn clock(&self) -> Clock {
        Clock::Test(self.0.clone())
    }
}

/// Controller-side heartbeat registry.
///
/// One instance lives on the `Broker` struct. Handlers call
/// [`record_heartbeat`](Self::record_heartbeat) on every incoming
/// `BrokerHeartbeat` RPC; the liveness ticker calls [`tick`](Self::tick)
/// every second to expire stale entries.
pub(crate) struct ControllerLivenessState {
    timeout: Duration,
    clock: Clock,
    brokers: Mutex<HashMap<u64, BrokerEntry>>,
    /// Brokers that signaled `want_shut_down=true` on a recent
    /// heartbeat. The controller tries to move leadership away from
    /// these brokers and returns `should_shut_down=true` once every
    /// partition has been re-led.
    wants_shutdown: Mutex<HashSet<u64>>,
}

impl ControllerLivenessState {
    /// Create a new registry with the given heartbeat timeout.
    pub(crate) fn new(timeout: Time) -> Self {
        Self {
            timeout: timeout.to_std(),
            clock: Clock::Real,
            brokers: Mutex::new(HashMap::new()),
            wants_shutdown: Mutex::new(HashSet::new()),
        }
    }

    /// Construct with a test-controlled [`Clock`] so liveness windows are driven
    /// by explicit `advance` calls instead of wall-clock sleeps.
    #[cfg(test)]
    fn with_clock(timeout: Duration, clock: Clock) -> Self {
        Self {
            timeout,
            clock,
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
        let now = self.clock.now();
        let entry = map.entry(broker_id).or_insert(BrokerEntry {
            last_heartbeat: now,
            state: BrokerLivenessState::Alive,
        });
        let prev = entry.state;
        entry.last_heartbeat = now;
        entry.state = BrokerLivenessState::Alive;
        if prev == BrokerLivenessState::Dead {
            tracing::info!(
                broker_id,
                "broker liveness: DEAD -> ALIVE (heartbeat resumed)"
            );
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
        let now = self.clock.now();
        let mut transitions = Vec::new();
        for (&id, entry) in map.iter_mut() {
            if entry.state == BrokerLivenessState::Alive
                && now.duration_since(entry.last_heartbeat) > self.timeout
            {
                entry.state = BrokerLivenessState::Dead;
                tracing::warn!(
                    broker_id = id,
                    since_last_heartbeat_ms =
                        u64::try_from(now.duration_since(entry.last_heartbeat).as_millis())
                            .unwrap_or(u64::MAX),
                    timeout_ms = u64::try_from(self.timeout.as_millis()).unwrap_or(u64::MAX),
                    "broker liveness: ALIVE -> DEAD (heartbeat session timeout) — triggers partition-leader failover"
                );
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

    /// Seed the liveness registry with the given broker ids as `Alive` with
    /// `last_heartbeat = now`. This is called when this broker becomes the
    /// raft leader so that live peers get a full timeout window to redirect
    /// their heartbeat loop at the new controller, while dead peers are still
    /// detected by [`tick`](Self::tick) after `timeout` ms.
    pub(crate) async fn seed_brokers(&self, broker_ids: impl IntoIterator<Item = u64>) {
        let mut map = self.brokers.lock().await;
        let now = self.clock.now();
        for id in broker_ids {
            map.entry(id)
                .and_modify(|entry| {
                    entry.last_heartbeat = now;
                    entry.state = BrokerLivenessState::Alive;
                })
                .or_insert(BrokerEntry {
                    last_heartbeat: now,
                    state: BrokerLivenessState::Alive,
                });
        }
    }

    /// Seed brokers as alive, but with only `grace` remaining before timeout.
    /// Used when this node becomes controller leader: live peers should heartbeat
    /// quickly, while a broker that died with the previous leader should not get
    /// a full fresh session timeout.
    pub(crate) async fn seed_brokers_expiring_after(
        &self,
        broker_ids: impl IntoIterator<Item = u64>,
        grace: Duration,
    ) {
        let mut map = self.brokers.lock().await;
        let now = self.clock.now();
        let backdated = self.timeout.saturating_sub(grace);
        let last_heartbeat = now.checked_sub(backdated).unwrap_or(now);
        for id in broker_ids {
            map.entry(id).or_insert(BrokerEntry {
                last_heartbeat,
                state: BrokerLivenessState::Alive,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::assert;

    use super::*;

    #[tokio::test]
    async fn new_broker_starts_alive_after_first_heartbeat() {
        let liveness = ControllerLivenessState::new(crabka_units::secs(10));
        let transition = liveness.record_heartbeat(1).await;
        assert!(transition == None); // first heartbeat: not a revival
        assert!(liveness.state(1).await == Some(BrokerLivenessState::Alive));
    }

    #[tokio::test]
    async fn tick_marks_expired_broker_dead() {
        let clock = TestClock::new();
        let liveness =
            ControllerLivenessState::with_clock(Duration::from_millis(10), clock.clock());
        liveness.record_heartbeat(2).await;
        // Advance past the timeout deterministically (no wall-clock sleep).
        clock.advance(Duration::from_millis(11));
        let transitions = liveness.tick().await;
        assert!(transitions == vec![LivenessTransition::AliveToDead(2)]);
        assert!(liveness.state(2).await == Some(BrokerLivenessState::Dead));
    }

    #[tokio::test]
    async fn heartbeat_after_dead_emits_revival() {
        let clock = TestClock::new();
        let liveness =
            ControllerLivenessState::with_clock(Duration::from_millis(10), clock.clock());
        liveness.record_heartbeat(3).await;
        clock.advance(Duration::from_millis(11));
        let _ = liveness.tick().await; // broker 3 → Dead
        let transition = liveness.record_heartbeat(3).await;
        assert!(transition == Some(LivenessTransition::DeadToAlive(3)));
        assert!(liveness.state(3).await == Some(BrokerLivenessState::Alive));
    }

    #[tokio::test]
    async fn expiring_seed_times_out_without_fresh_heartbeat() {
        let clock = TestClock::new();
        let liveness =
            ControllerLivenessState::with_clock(Duration::from_millis(10), clock.clock());
        liveness
            .seed_brokers_expiring_after([7], Duration::from_nanos(1))
            .await;
        // Only ~1ns of grace was granted; any advance past it expires the seed.
        clock.advance(Duration::from_millis(1));

        let transitions = liveness.tick().await;

        assert!(transitions == vec![LivenessTransition::AliveToDead(7)]);
    }

    #[tokio::test]
    async fn normal_seed_gives_brokers_full_timeout_window() {
        let clock = TestClock::new();
        let liveness =
            ControllerLivenessState::with_clock(Duration::from_millis(50), clock.clock());
        liveness.seed_brokers([7]).await;
        // Well within the 50ms window — deterministically still alive.
        clock.advance(Duration::from_millis(1));

        let transitions = liveness.tick().await;

        assert!(transitions.is_empty());
        assert!(liveness.state(7).await == Some(BrokerLivenessState::Alive));
    }

    #[tokio::test]
    async fn normal_seed_refreshes_existing_entries() {
        let clock = TestClock::new();
        let liveness =
            ControllerLivenessState::with_clock(Duration::from_millis(10), clock.clock());
        liveness.record_heartbeat(7).await;
        // Let the original heartbeat go stale relative to the 10ms window...
        clock.advance(Duration::from_millis(20));

        // ...a normal re-seed must REFRESH the existing entry to a full window,
        liveness.seed_brokers([7]).await;
        // so 1ms later it is nowhere near expiry. Were the refresh missing, the
        // entry would be ~21ms stale here and `tick` would mark it dead — which
        // is exactly the regression this test guards.
        clock.advance(Duration::from_millis(1));
        let transitions = liveness.tick().await;

        assert!(transitions.is_empty());
        assert!(liveness.state(7).await == Some(BrokerLivenessState::Alive));
    }

    #[tokio::test]
    async fn expiring_seed_stays_alive_after_fresh_heartbeat() {
        let liveness = ControllerLivenessState::new(crabka_units::minutes(1));
        liveness
            .seed_brokers_expiring_after([7], Duration::from_nanos(1))
            .await;
        liveness.record_heartbeat(7).await;

        let transitions = liveness.tick().await;

        assert!(transitions.is_empty());
        assert!(liveness.state(7).await == Some(BrokerLivenessState::Alive));
    }

    #[tokio::test]
    async fn wants_shutdown_set_and_unset() {
        let liveness = ControllerLivenessState::new(crabka_units::secs(10));
        assert!(!liveness.wants_shutdown(5).await);
        liveness.set_wants_shutdown(5, true).await;
        assert!(liveness.wants_shutdown(5).await);
        liveness.set_wants_shutdown(5, false).await;
        assert!(!liveness.wants_shutdown(5).await);
    }

    #[tokio::test]
    async fn wants_shutdown_is_per_broker() {
        let liveness = ControllerLivenessState::new(crabka_units::secs(10));
        liveness.set_wants_shutdown(1, true).await;
        liveness.set_wants_shutdown(2, true).await;
        for (broker, want) in [(1, true), (2, true), (3, false)] {
            assert!(
                liveness.wants_shutdown(broker).await == want,
                "broker {broker}"
            );
        }
        liveness.set_wants_shutdown(1, false).await;
        assert!(!liveness.wants_shutdown(1).await);
        assert!(liveness.wants_shutdown(2).await);
    }

    #[tokio::test]
    async fn tick_does_not_expire_recently_heartbeated_broker() {
        let liveness = ControllerLivenessState::new(crabka_units::minutes(1));
        liveness.record_heartbeat(4).await;
        let transitions = liveness.tick().await;
        assert!(
            transitions.is_empty(),
            "broker 4 should not expire with 60s timeout"
        );
        assert!(liveness.state(4).await == Some(BrokerLivenessState::Alive));
    }
}
