//! Controller-side liveness tracking for KIP-500 broker heartbeats.
//!
//! `ControllerLivenessState` tracks the last-seen timestamp for every
//! registered broker and drives a periodic liveness ticker that emits
//! `LivenessTransition` events when a broker goes dead or comes alive.

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
/// [`ControllerLivenessState::record_fenced_heartbeat`].
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
    fenced: bool,
}

/// Monotonic time source for liveness tracking.
///
/// Production uses the real clock. Tests use a controllable clock, so
/// explicit advances drive the liveness windows instead of wall-clock
/// `std::thread::sleep`. Sleeps flake when the CI runner is loaded. The gap
/// between a re-seed and the next `tick` can then exceed a short timeout and
/// mark a broker dead by mistake.
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

/// Test handle for the controllable [`Clock`]. It shares its inner state with
/// the `Clock::Test` handed to [`ControllerLivenessState::with_clock`], so the
/// liveness state under test observes every `advance`. Tests in other modules
/// reach it through [`ControllerLivenessState::with_test_clock`].
#[cfg(test)]
pub(crate) struct TestClock(std::sync::Arc<TestClockInner>);

#[cfg(test)]
impl TestClock {
    pub(crate) fn new() -> Self {
        Self(std::sync::Arc::new(TestClockInner {
            base: Instant::now(),
            offset_nanos: std::sync::atomic::AtomicU64::new(0),
        }))
    }

    pub(crate) fn advance(&self, by: Duration) {
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
/// [`record_fenced_heartbeat`](Self::record_fenced_heartbeat) on every incoming
/// `BrokerHeartbeat` RPC. The liveness ticker calls [`tick`](Self::tick)
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

    /// Construct with a [`TestClock`]. Tests outside this module use it to
    /// drive a broker to `Dead` through [`tick`](Self::tick) without a
    /// wall-clock sleep.
    #[cfg(test)]
    pub(crate) fn with_test_clock(timeout: Duration, clock: &TestClock) -> Self {
        Self::with_clock(timeout, clock.clock())
    }

    /// Record whether `broker_id` is currently asking to shut down.
    /// `true` adds to the set. `false` removes from the set, which
    /// covers a broker that retracts the request. In practice the
    /// controller only clears state when it observes the broker dead.
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
    #[cfg(test)]
    async fn wants_shutdown(&self, broker_id: u64) -> bool {
        self.wants_shutdown.lock().await.contains(&broker_id)
    }

    /// Record a heartbeat from `broker_id`. A broker that has no existing
    /// session starts fenced until the handler confirms metadata catch-up.
    pub(crate) async fn record_fenced_heartbeat(
        &self,
        broker_id: u64,
    ) -> Option<LivenessTransition> {
        self.record_heartbeat_inner(broker_id, true).await
    }

    #[cfg(test)]
    pub(crate) async fn record_heartbeat(&self, broker_id: u64) -> Option<LivenessTransition> {
        self.record_heartbeat_inner(broker_id, false).await
    }

    async fn record_heartbeat_inner(
        &self,
        broker_id: u64,
        initially_fenced: bool,
    ) -> Option<LivenessTransition> {
        let mut map = self.brokers.lock().await;
        let now = self.clock.now();
        let entry = map.entry(broker_id).or_insert(BrokerEntry {
            last_heartbeat: now,
            state: BrokerLivenessState::Alive,
            fenced: initially_fenced,
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
    #[cfg(test)]
    async fn state(&self, broker_id: u64) -> Option<BrokerLivenessState> {
        let map = self.brokers.lock().await;
        map.get(&broker_id).map(|e| e.state)
    }

    /// Return `true` if `broker_id` is currently `Alive` (has sent a
    /// heartbeat within the timeout window). Returns `false` for unknown
    /// brokers and for brokers whose heartbeat has expired.
    pub(crate) async fn is_alive(&self, broker_id: u64) -> bool {
        self.brokers
            .lock()
            .await
            .get(&broker_id)
            .is_some_and(|entry| entry.state == BrokerLivenessState::Alive && !entry.fenced)
    }

    /// Apply the broker's fencing request. A broker can only unfence after it
    /// has caught up through its registration record. Returns the resulting
    /// fenced state.
    pub(crate) async fn apply_fencing(
        &self,
        broker_id: u64,
        want_fence: bool,
        is_caught_up: bool,
    ) -> bool {
        let mut map = self.brokers.lock().await;
        let Some(entry) = map.get_mut(&broker_id) else {
            return true;
        };
        if want_fence {
            entry.fenced = true;
        } else if is_caught_up {
            entry.fenced = false;
        }
        entry.fenced
    }

    /// Snapshot the set of currently-`Alive` broker ids under a single
    /// lock acquisition. This is equivalent to calling
    /// [`is_alive`](Self::is_alive) for every broker. But the cluster-wide
    /// maintenance loops for failover, rebalance, and metrics take the
    /// `brokers` lock once and then do synchronous set-membership checks.
    /// They do not take one `.await` lock per partition. Unknown brokers
    /// are absent from the set, so membership `false` means not alive.
    /// This matches `is_alive`'s predicate exactly.
    pub(crate) async fn alive_snapshot(&self) -> HashSet<u64> {
        let map = self.brokers.lock().await;
        map.iter()
            .filter(|(_, e)| e.state == BrokerLivenessState::Alive && !e.fenced)
            .map(|(&id, _)| id)
            .collect()
    }

    /// Snapshot brokers that are known to be unavailable for new replica
    /// assignments. Unknown brokers are deliberately omitted: immediately
    /// after a controller election, registrations may be visible before the
    /// liveness registry has been seeded.
    pub(crate) async fn unavailable_snapshot(&self) -> HashSet<u64> {
        let map = self.brokers.lock().await;
        map.iter()
            .filter(|(_, entry)| entry.state == BrokerLivenessState::Dead || entry.fenced)
            .map(|(&id, _)| id)
            .collect()
    }

    /// Snapshot the brokers whose heartbeat session has expired. Only the
    /// `Dead` state qualifies. A fenced but alive broker is not in the set,
    /// and neither is an unknown broker. The liveness ticker's sweep uses it
    /// to find dead brokers that still lead a partition or still sit in an
    /// ISR.
    pub(crate) async fn dead_snapshot(&self) -> HashSet<u64> {
        let map = self.brokers.lock().await;
        map.iter()
            .filter(|(_, entry)| entry.state == BrokerLivenessState::Dead)
            .map(|(&id, _)| id)
            .collect()
    }

    /// Start a session for every broker in `broker_ids` that the registry does
    /// not know yet. Each new entry starts `Alive` with `last_heartbeat =
    /// now`, so the broker gets one full timeout window to send its first
    /// heartbeat. It also starts fenced, as a first heartbeat would leave it:
    /// a broker that has not yet proved metadata catch-up must not be elected
    /// or receive replicas, and only [`apply_fencing`](Self::apply_fencing)
    /// with `is_caught_up` lifts the fence. Known entries keep their state,
    /// their fence, and their death clock.
    ///
    /// The controller leader calls this on every liveness tick with the
    /// brokers registered in the metadata image. Without it the registry
    /// only knows brokers that heartbeated this controller or that a
    /// leadership change seeded. A broker that registers and dies before its
    /// first heartbeat reaches this controller would then never expire, and
    /// the partitions it leads would never fail over.
    pub(crate) async fn track_registered(&self, broker_ids: impl IntoIterator<Item = u64>) {
        let mut map = self.brokers.lock().await;
        let now = self.clock.now();
        for id in broker_ids {
            map.entry(id).or_insert(BrokerEntry {
                last_heartbeat: now,
                state: BrokerLivenessState::Alive,
                fenced: true,
            });
        }
    }

    /// Seed the liveness registry with the given broker ids as `Alive` with
    /// `last_heartbeat = now`. The broker calls this when it becomes the raft
    /// leader. Live peers then get a full timeout window to redirect their
    /// heartbeat loop at the new controller. [`tick`](Self::tick) still
    /// detects dead peers after `timeout` ms.
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
                    fenced: false,
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
    async fn fencing_removes_alive_broker_from_eligible_snapshot() {
        let liveness = ControllerLivenessState::new(crabka_units::secs(10));
        liveness.record_heartbeat(3).await;

        assert!(liveness.apply_fencing(3, true, true).await);
        assert!(!liveness.is_alive(3).await);
        assert!(!liveness.alive_snapshot().await.contains(&3));
        assert!(liveness.unavailable_snapshot().await.contains(&3));
    }

    #[tokio::test]
    async fn unavailable_snapshot_includes_dead_but_not_unknown_brokers() {
        let clock = TestClock::new();
        let liveness =
            ControllerLivenessState::with_clock(Duration::from_millis(10), clock.clock());
        liveness.record_heartbeat(2).await;
        clock.advance(Duration::from_millis(11));
        let _ = liveness.tick().await;

        let unavailable = liveness.unavailable_snapshot().await;
        assert!(unavailable.contains(&2));
        assert!(!unavailable.contains(&99));
    }

    #[tokio::test]
    async fn dead_snapshot_holds_expired_brokers_only() {
        let clock = TestClock::new();
        let liveness = ControllerLivenessState::with_test_clock(Duration::from_millis(10), &clock);
        liveness.record_heartbeat(1).await;
        liveness.record_heartbeat(2).await;
        clock.advance(Duration::from_millis(11));
        // Broker 2 heartbeats again inside the new window. Broker 1 does not.
        // Broker 3 is alive but fenced.
        liveness.record_heartbeat(2).await;
        liveness.record_fenced_heartbeat(3).await;
        let transitions = liveness.tick().await;
        assert!(transitions == vec![LivenessTransition::AliveToDead(1)]);

        let dead = liveness.dead_snapshot().await;
        assert!(dead == [1].into_iter().collect());
        // The fenced broker is unavailable but not dead.
        assert!(liveness.unavailable_snapshot().await.contains(&3));
        assert!(!dead.contains(&99));

        // A revival heartbeat empties the set.
        liveness.record_heartbeat(1).await;
        assert!(liveness.dead_snapshot().await.is_empty());
    }

    #[tokio::test]
    async fn track_registered_adds_unknown_brokers_and_keeps_known_state() {
        let clock = TestClock::new();
        let liveness = ControllerLivenessState::with_test_clock(Duration::from_millis(10), &clock);
        // Broker 1 is known and expires.
        liveness.record_heartbeat(1).await;
        clock.advance(Duration::from_millis(11));
        assert!(liveness.tick().await == vec![LivenessTransition::AliveToDead(1)]);

        // Broker 1 keeps its dead state. Broker 2 starts a fresh session that
        // is fenced until it proves catch-up: not dead, not electable, and
        // unavailable for new replicas.
        liveness.track_registered([1, 2]).await;
        assert!(liveness.dead_snapshot().await == [1].into_iter().collect());
        assert!(!liveness.is_alive(2).await);
        assert!(liveness.unavailable_snapshot().await.contains(&2));

        // A caught-up heartbeat lifts the fence and only then makes it alive.
        liveness.record_fenced_heartbeat(2).await;
        assert!(!liveness.apply_fencing(2, false, true).await);
        assert!(liveness.is_alive(2).await);

        // Broker 3 is discovered and never heartbeats. It expires one full
        // window later, while broker 2 keeps heartbeating.
        liveness.track_registered([3]).await;
        clock.advance(Duration::from_millis(11));
        liveness.record_fenced_heartbeat(2).await;
        assert!(liveness.tick().await == vec![LivenessTransition::AliveToDead(3)]);
        assert!(liveness.dead_snapshot().await == [1, 3].into_iter().collect());
    }

    #[tokio::test]
    async fn track_registered_does_not_refresh_a_stale_session() {
        let clock = TestClock::new();
        let liveness = ControllerLivenessState::with_test_clock(Duration::from_millis(10), &clock);
        liveness.record_heartbeat(5).await;
        clock.advance(Duration::from_millis(9));

        // Unlike `seed_brokers`, a track call must not extend the window.
        liveness.track_registered([5]).await;
        clock.advance(Duration::from_millis(2));

        assert!(liveness.tick().await == vec![LivenessTransition::AliveToDead(5)]);
    }

    #[tokio::test]
    async fn seed_moves_dead_broker_out_of_dead_snapshot() {
        let clock = TestClock::new();
        let liveness = ControllerLivenessState::with_test_clock(Duration::from_millis(10), &clock);
        liveness.record_heartbeat(4).await;
        clock.advance(Duration::from_millis(11));
        let _ = liveness.tick().await;
        assert!(liveness.dead_snapshot().await.contains(&4));

        liveness.seed_brokers([4]).await;

        assert!(liveness.dead_snapshot().await.is_empty());
    }

    #[tokio::test]
    async fn broker_only_unfences_after_metadata_catch_up() {
        let liveness = ControllerLivenessState::new(crabka_units::secs(10));
        liveness.record_fenced_heartbeat(3).await;

        assert!(liveness.apply_fencing(3, false, false).await);
        assert!(!liveness.is_alive(3).await);
        assert!(!liveness.apply_fencing(3, false, true).await);
        assert!(liveness.is_alive(3).await);
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
