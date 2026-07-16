//! Range-0 monotone timestamp oracle with stride-ahead durability.

use std::{
    num::NonZeroU64,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use crabka_pgkv::{Kv, WriteOp};
use tokio::{sync::Mutex, time::Instant};

use crate::tso::stats::TsoOracleStats;

/// Range-0 key carrying the durable inclusive timestamp horizon.
pub const MAX_TS_KEY: &[u8] = b"/0/meta/max_ts";

/// Logical timestamp granted by the range-0 oracle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TsoTimestamp(u64);

impl TsoTimestamp {
    /// First valid transaction timestamp.
    pub const FIRST: Self = Self(1);

    /// Build a timestamp from a non-zero raw value.
    #[must_use]
    pub const fn new(raw: NonZeroU64) -> Self {
        Self(raw.get())
    }

    /// Return the raw wire/storage value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    fn from_persisted_next(raw: u64) -> Result<Self, TsoError> {
        let next = raw.checked_add(1).ok_or(TsoError::TimestampOverflow)?;
        NonZeroU64::new(next)
            .map(Self::new)
            .ok_or(TsoError::TimestampOverflow)
    }
}

impl From<TsoTimestamp> for u64 {
    fn from(value: TsoTimestamp) -> Self {
        value.get()
    }
}

/// Contiguous grant returned by the timestamp oracle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GrantLease {
    /// First timestamp in the lease.
    pub first_ts: TsoTimestamp,
    /// Number of contiguous timestamps granted.
    pub count: NonZeroU64,
}

impl GrantLease {
    /// Build a parsed non-empty lease.
    #[must_use]
    pub const fn new(first_ts: TsoTimestamp, count: NonZeroU64) -> Self {
        Self { first_ts, count }
    }

    /// Last timestamp in the lease.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn last_ts(self) -> Result<TsoTimestamp, TsoError> {
        let last = self
            .first_ts
            .get()
            .checked_add(self.count.get() - 1)
            .ok_or(TsoError::TimestampOverflow)?;
        NonZeroU64::new(last)
            .map(TsoTimestamp::new)
            .ok_or(TsoError::TimestampOverflow)
    }
}

/// Durable range-0 append seam for `max_ts` horizon bumps.
#[async_trait::async_trait]
pub trait TsoHorizonCommitter: Send + Sync {
    /// Persist the new inclusive `max_ts` horizon through range 0 if `epoch` is
    /// still the live writer epoch.
    async fn persist_max_ts_for_epoch(
        &self,
        epoch: i16,
        max_ts: TsoTimestamp,
    ) -> Result<(), TsoError>;
}

/// Epoch-liveness heartbeat seam for fencing stale range-0 writers.
#[async_trait::async_trait]
pub trait EpochHeartbeat: Send + Sync {
    /// Verify that `epoch` is still live before the oracle grants a batch that
    /// does not need to advance the durable horizon.
    async fn heartbeat(&self, epoch: i16) -> Result<HeartbeatVerdict, TsoError>;
}

/// Result of a range-0 epoch heartbeat.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeartbeatVerdict {
    /// The epoch remains the active writer epoch.
    Live,
    /// A successor fenced this oracle's epoch.
    Fenced,
}

/// State touched only on the slow path, guarded by the slow-path mutex.
struct SlowState {
    has_granted: bool,
}

trait TsoClock: Send + Sync {
    fn now_ms(&self) -> u64;
}

/// Monotone clock backed by [`tokio::time::Instant`], so paused-time tests
/// advance it together with the timer wheel; in production it is identical
/// to the standard monotonic clock.
struct SystemTsoClock(Instant);

impl TsoClock for SystemTsoClock {
    fn now_ms(&self) -> u64 {
        u64::try_from(self.0.elapsed().as_millis()).unwrap_or(u64::MAX)
    }
}

const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(10);

/// Range-0 timestamp oracle.
///
/// Within-stride grants are served lock-free: `next_ts`, `durable_max_ts`,
/// and `certified_until_ms` are atomics read on the hot path, and only
/// horizon advancement and certificate renewal serialize through the
/// slow-path mutex.
///
/// Ordering: `durable_max_ts` and `certified_until_ms` are stored with
/// `Release` only after the epoch-gated persist or heartbeat succeeded and
/// loaded with `Acquire`, so a fast path that observes a horizon or
/// certificate also observes the liveness check that produced it. `next_ts`
/// is a pure reservation counter — no other data is published through it —
/// so `AcqRel` on its compare-exchange is already stronger than it needs.
pub struct TsoOracle<C, H> {
    committer: C,
    heartbeat: H,
    epoch: i16,
    stride: NonZeroU64,
    heartbeat_interval: Duration,
    clock: Arc<dyn TsoClock>,
    ready: AtomicBool,
    ready_at_ms: u64,
    next_ts: AtomicU64,
    durable_max_ts: AtomicU64,
    certified_until_ms: AtomicU64,
    slow: Mutex<SlowState>,
    stats: Arc<TsoOracleStats>,
}

impl<C, H> TsoOracle<C, H>
where
    C: TsoHorizonCommitter,
    H: EpochHeartbeat,
{
    /// Recover an oracle from an already-replayed durable horizon.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn recover(
        committer: C,
        heartbeat: H,
        epoch: i16,
        stride: NonZeroU64,
        persisted_max_ts: u64,
    ) -> Result<Self, TsoError> {
        Self::recover_with_clock(
            committer,
            heartbeat,
            epoch,
            stride,
            persisted_max_ts,
            DEFAULT_HEARTBEAT_INTERVAL,
            Arc::new(SystemTsoClock(Instant::now())),
        )
    }

    /// Recover with an explicit liveness-certificate interval for deterministic tests.
    #[doc(hidden)]
    pub fn recover_with_heartbeat_interval(
        committer: C,
        heartbeat: H,
        epoch: i16,
        stride: NonZeroU64,
        persisted_max_ts: u64,
        heartbeat_interval: Duration,
    ) -> Result<Self, TsoError> {
        Self::recover_with_clock(
            committer,
            heartbeat,
            epoch,
            stride,
            persisted_max_ts,
            heartbeat_interval,
            Arc::new(SystemTsoClock(Instant::now())),
        )
    }

    fn recover_with_clock(
        committer: C,
        heartbeat: H,
        epoch: i16,
        stride: NonZeroU64,
        persisted_max_ts: u64,
        heartbeat_interval: Duration,
        clock: Arc<dyn TsoClock>,
    ) -> Result<Self, TsoError> {
        // A zero horizon proves no predecessor ever granted (every grant
        // persists a stride first), so no successor grace period is needed.
        let ready_at_ms = if persisted_max_ts == 0 {
            0
        } else {
            clock
                .now_ms()
                .saturating_add(duration_ms(heartbeat_interval))
        };
        Ok(Self {
            committer,
            heartbeat,
            epoch,
            stride,
            heartbeat_interval,
            clock,
            ready: AtomicBool::new(ready_at_ms == 0),
            ready_at_ms,
            next_ts: AtomicU64::new(TsoTimestamp::from_persisted_next(persisted_max_ts)?.get()),
            durable_max_ts: AtomicU64::new(persisted_max_ts),
            certified_until_ms: AtomicU64::new(0),
            slow: Mutex::new(SlowState { has_granted: false }),
            stats: Arc::default(),
        })
    }

    /// Record grant activity into `stats` so an external poller can observe
    /// this oracle.
    #[must_use]
    pub fn with_stats(mut self, stats: Arc<TsoOracleStats>) -> Self {
        self.stats = stats;
        self
    }

    /// Return the stats handle recording this oracle's grant activity.
    #[must_use]
    pub fn stats(&self) -> Arc<TsoOracleStats> {
        Arc::clone(&self.stats)
    }

    /// Grant a non-empty contiguous timestamp lease.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn grant(&self, count: NonZeroU64) -> Result<GrantLease, TsoError> {
        self.wait_until_ready().await;
        if self.clock.now_ms() >= self.certified_until_ms.load(Ordering::Acquire) {
            self.renew_certificate().await?;
        }
        let (first, last) = self.reserve(count)?;
        if last > self.durable_max_ts.load(Ordering::Acquire) {
            self.advance_horizon(first, last).await?;
        }
        let first_ts =
            TsoTimestamp::new(NonZeroU64::new(first).ok_or(TsoError::TimestampOverflow)?);
        self.stats.record_grant(count.get());
        Ok(GrantLease::new(first_ts, count))
    }

    /// Wait out the successor grace period before the first grants.
    ///
    /// Invariant: a fenced-but-alive predecessor keeps serving within-stride
    /// grants from memory until its liveness certificate lapses, and that
    /// certificate was anchored before the fence — so it expires no later
    /// than one `heartbeat_interval` past the fence, which itself precedes
    /// this successor's recovery. Waiting one full interval after recovery
    /// therefore outlasts every possible predecessor certificate: this
    /// oracle acknowledges no grant while the predecessor may still serve,
    /// so no granted read timestamp can precede a commit acknowledged before
    /// the grant. Assumes `heartbeat_interval` is identical across writer
    /// generations (today it is a compile-time constant).
    async fn wait_until_ready(&self) {
        if self.ready.load(Ordering::Acquire) {
            return;
        }
        loop {
            let now_ms = self.clock.now_ms();
            if now_ms >= self.ready_at_ms {
                break;
            }
            tokio::time::sleep(Duration::from_millis(self.ready_at_ms - now_ms)).await;
        }
        self.ready.store(true, Ordering::Release);
    }

    /// Reserve `count` contiguous timestamps from the shared counter.
    ///
    /// Uses a compare-exchange loop over `fetch_add` so overflow fails the
    /// grant instead of wrapping the counter.
    fn reserve(&self, count: NonZeroU64) -> Result<(u64, u64), TsoError> {
        let mut first = self.next_ts.load(Ordering::Acquire);
        loop {
            let last = first
                .checked_add(count.get() - 1)
                .ok_or(TsoError::TimestampOverflow)?;
            let next = last.checked_add(1).ok_or(TsoError::TimestampOverflow)?;
            match self.next_ts.compare_exchange_weak(
                first,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok((first, last)),
                Err(observed) => first = observed,
            }
        }
    }

    /// Renew the epoch-liveness certificate under the slow-path mutex.
    async fn renew_certificate(&self) -> Result<(), TsoError> {
        let slow = self.slow.lock().await;
        // Re-check under the lock: one renewal serves every concurrent waiter.
        if self.clock.now_ms() < self.certified_until_ms.load(Ordering::Acquire) {
            return Ok(());
        }
        // The first grant always persists a stride and heartbeats right
        // after; renewing here as well would heartbeat that grant twice.
        if !slow.has_granted {
            return Ok(());
        }
        // Anchor the certificate before the heartbeat round-trip so it never
        // extends past `heartbeat_interval` after the liveness check began.
        let now_ms = self.clock.now_ms();
        self.ensure_epoch_live().await?;
        self.certified_until_ms.store(
            now_ms.saturating_add(duration_ms(self.heartbeat_interval)),
            Ordering::Release,
        );
        Ok(())
    }

    /// Persist a stride-advanced horizon covering the reserved range, then
    /// publish the horizon and a fresh certificate.
    ///
    /// On failure the reserved timestamps are burned, never granted: gaps
    /// are harmless, monotonicity is the requirement.
    async fn advance_horizon(&self, first: u64, last: u64) -> Result<(), TsoError> {
        self.stats.record_horizon_wait();
        let mut slow = self.slow.lock().await;
        // Another waiter may have advanced the horizon past this reservation.
        if last <= self.durable_max_ts.load(Ordering::Acquire) {
            return Ok(());
        }
        let stride_last = first
            .checked_add(self.stride.get() - 1)
            .ok_or(TsoError::TimestampOverflow)?;
        let new_horizon = last.max(stride_last);
        let persisted =
            TsoTimestamp::new(NonZeroU64::new(new_horizon).ok_or(TsoError::TimestampOverflow)?);
        // Anchor the certificate before the epoch-gated persist round-trip.
        let now_ms = self.clock.now_ms();
        self.committer
            .persist_max_ts_for_epoch(self.epoch, persisted)
            .await?;
        self.stats.record_horizon_persist();
        if !slow.has_granted {
            self.ensure_epoch_live().await?;
            slow.has_granted = true;
        }
        self.durable_max_ts.store(new_horizon, Ordering::Release);
        self.certified_until_ms.store(
            now_ms.saturating_add(duration_ms(self.heartbeat_interval)),
            Ordering::Release,
        );
        Ok(())
    }

    async fn ensure_epoch_live(&self) -> Result<(), TsoError> {
        self.stats.record_heartbeat();
        match self.heartbeat.heartbeat(self.epoch).await? {
            HeartbeatVerdict::Live => Ok(()),
            HeartbeatVerdict::Fenced => Err(TsoError::FencedEpoch { epoch: self.epoch }),
        }
    }
}

/// In-memory range-0 horizon committer and epoch gate for deterministic tests.
#[derive(Clone)]
pub struct MemoryTsoHorizon {
    store: Arc<dyn Kv>,
    live_epoch: Arc<Mutex<i16>>,
}

impl MemoryTsoHorizon {
    /// Build a memory horizon over an already-open range-0 store.
    #[must_use]
    pub fn new(store: Arc<dyn Kv>, epoch: i16) -> Self {
        Self {
            store,
            live_epoch: Arc::new(Mutex::new(epoch)),
        }
    }

    /// Load the durable inclusive `max_ts` horizon.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn load_max_ts(&self) -> Result<u64, TsoError> {
        self.store
            .get(MAX_TS_KEY)?
            .as_deref()
            .map_or(Ok(0), decode_u64)
    }

    /// Fence old oracle instances by making `epoch` the live writer epoch.
    pub async fn set_live_epoch(&self, epoch: i16) {
        *self.live_epoch.lock().await = epoch;
    }
}

#[async_trait::async_trait]
impl TsoHorizonCommitter for MemoryTsoHorizon {
    async fn persist_max_ts_for_epoch(
        &self,
        epoch: i16,
        max_ts: TsoTimestamp,
    ) -> Result<(), TsoError> {
        let live_epoch = self.live_epoch.lock().await;
        if *live_epoch != epoch {
            return Err(TsoError::FencedEpoch { epoch });
        }
        self.store.write_batch(&[WriteOp::Put {
            key: MAX_TS_KEY.to_vec(),
            value: max_ts.get().to_be_bytes().to_vec(),
        }])?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl EpochHeartbeat for MemoryTsoHorizon {
    async fn heartbeat(&self, epoch: i16) -> Result<HeartbeatVerdict, TsoError> {
        if *self.live_epoch.lock().await == epoch {
            return Ok(HeartbeatVerdict::Live);
        }

        Ok(HeartbeatVerdict::Fenced)
    }
}

/// Timestamp-oracle failure.
#[derive(Debug, thiserror::Error)]
pub enum TsoError {
    /// The caller requested an empty grant.
    #[error("timestamp grant count must be greater than zero")]
    EmptyGrant,
    /// Timestamp arithmetic overflowed.
    #[error("timestamp oracle overflow")]
    TimestampOverflow,
    /// The oracle epoch was fenced by a successor.
    #[error("timestamp oracle epoch {epoch} was fenced")]
    FencedEpoch { epoch: i16 },
    /// Range-0 durable storage failed.
    #[error(transparent)]
    Kv(#[from] crabka_pgkv::KvError),
    /// The range-0 horizon bytes were malformed.
    #[error("malformed timestamp horizon: {0}")]
    CorruptHorizon(String),
    /// The remote oracle rejected or failed the request.
    #[error("timestamp oracle rpc failed: {0}")]
    Rpc(String),
}

pub(crate) fn parse_count(count: u64) -> Result<NonZeroU64, TsoError> {
    NonZeroU64::new(count).ok_or(TsoError::EmptyGrant)
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn decode_u64(bytes: &[u8]) -> Result<u64, TsoError> {
    let array: [u8; 8] = bytes.try_into().map_err(|_| {
        TsoError::CorruptHorizon(format!("expected 8 bytes, found {}", bytes.len()))
    })?;
    Ok(u64::from_be_bytes(array))
}

#[cfg(test)]
mod tests {
    use std::{
        num::NonZeroU64,
        sync::atomic::{AtomicU64, AtomicUsize, Ordering},
        time::Duration,
    };

    use assert2::assert;
    use crabka_pgkv::MemKv;

    use super::*;

    fn nonzero(value: u64) -> NonZeroU64 {
        NonZeroU64::new(value).expect("test count is non-zero")
    }

    /// Grant with a hang bound: any regression that turns a grant into an
    /// unbounded wait must fail the suite promptly rather than stalling it
    /// past the mutation-testing budget.
    async fn grant_within<C, H>(
        oracle: &TsoOracle<C, H>,
        count: NonZeroU64,
    ) -> Result<GrantLease, TsoError>
    where
        C: TsoHorizonCommitter,
        H: EpochHeartbeat,
    {
        tokio::time::timeout(Duration::from_secs(5), oracle.grant(count))
            .await
            .expect("grant must complete promptly")
    }

    #[derive(Clone)]
    struct CountingHeartbeat {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl EpochHeartbeat for CountingHeartbeat {
        async fn heartbeat(&self, _epoch: i16) -> Result<HeartbeatVerdict, TsoError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(HeartbeatVerdict::Live)
        }
    }

    struct ManualClock(AtomicU64);

    impl TsoClock for ManualClock {
        fn now_ms(&self) -> u64 {
            self.0.load(Ordering::SeqCst)
        }
    }

    #[tokio::test]
    async fn epoch_certificate_heartbeats_first_and_at_interval_expiry() {
        let store = Arc::new(MemKv::default());
        let horizon = MemoryTsoHorizon::new(store, 3);
        let calls = Arc::new(AtomicUsize::new(0));
        let heartbeat = CountingHeartbeat {
            calls: Arc::clone(&calls),
        };
        let clock = Arc::new(ManualClock(AtomicU64::new(1)));
        let oracle = TsoOracle::recover_with_clock(
            horizon,
            heartbeat,
            3,
            nonzero(100),
            100,
            Duration::from_millis(10),
            clock.clone(),
        )
        .expect("recover");

        // Advance strictly past the successor grace period (ready at 1 + 10):
        // an exactly-at-boundary clock would let a regressed readiness
        // comparison spin on zero-duration sleeps instead of failing fast.
        clock.0.store(12, Ordering::SeqCst);
        grant_within(&oracle, nonzero(1)).await.expect("first");
        grant_within(&oracle, nonzero(1)).await.expect("certified");
        assert!(calls.load(Ordering::SeqCst) == 1);
        // Strictly past the certificate (12 + 10): at the exact boundary a
        // flipped renewal recheck is indistinguishable from the real one.
        clock.0.store(23, Ordering::SeqCst);
        grant_within(&oracle, nonzero(1)).await.expect("renewed");
        assert!(calls.load(Ordering::SeqCst) == 2);
    }

    #[tokio::test]
    async fn grant_activity_is_recorded_in_oracle_stats() {
        use crate::tso::stats::TsoOracleStatsSnapshot;

        let store = Arc::new(MemKv::default());
        let horizon = MemoryTsoHorizon::new(store, 3);
        let clock = Arc::new(ManualClock(AtomicU64::new(1)));
        let stats = Arc::new(TsoOracleStats::default());
        let oracle = TsoOracle::recover_with_clock(
            horizon.clone(),
            horizon.clone(),
            3,
            nonzero(10),
            0,
            Duration::from_millis(10),
            clock.clone(),
        )
        .expect("recover")
        .with_stats(Arc::clone(&stats));

        // First grant persists one stride and issues the first-grant
        // heartbeat; the second is served within stride and certificate.
        grant_within(&oracle, nonzero(3)).await.expect("first");
        grant_within(&oracle, nonzero(2)).await.expect("second");
        assert!(
            stats.snapshot()
                == TsoOracleStatsSnapshot {
                    grants_served: 2,
                    timestamps_granted: 5,
                    horizon_waits: 1,
                    horizon_persists: 1,
                    heartbeats: 1,
                }
        );

        // Certificate expiry (strictly past 1 + 10) renews via one more
        // heartbeat; the grant stays within the persisted stride.
        clock.0.store(12, Ordering::SeqCst);
        grant_within(&oracle, nonzero(1)).await.expect("renewed");
        assert!(
            stats.snapshot()
                == TsoOracleStatsSnapshot {
                    grants_served: 3,
                    timestamps_granted: 6,
                    horizon_waits: 1,
                    horizon_persists: 1,
                    heartbeats: 2,
                }
        );
        assert!(oracle.stats().snapshot() == stats.snapshot());
    }

    #[tokio::test]
    async fn boundary_grant_at_durable_horizon_stays_on_the_fast_path() {
        use crate::tso::stats::TsoOracleStatsSnapshot;

        let store = Arc::new(MemKv::default());
        let horizon = MemoryTsoHorizon::new(store, 3);
        let stats = Arc::new(TsoOracleStats::default());
        let oracle = TsoOracle::recover(horizon.clone(), horizon.clone(), 3, nonzero(4), 0)
            .expect("recover")
            .with_stats(Arc::clone(&stats));

        // First grant persists the stride to 4; the second consumes exactly
        // up to the durable horizon and must be served lock-free.
        grant_within(&oracle, nonzero(2)).await.expect("first");
        grant_within(&oracle, nonzero(2)).await.expect("boundary");

        assert!(
            stats.snapshot()
                == TsoOracleStatsSnapshot {
                    grants_served: 2,
                    timestamps_granted: 4,
                    horizon_waits: 1,
                    horizon_persists: 1,
                    heartbeats: 1,
                }
        );
    }

    #[tokio::test]
    async fn grace_wait_sleeps_only_the_remaining_interval() {
        let store = Arc::new(MemKv::default());
        let horizon = MemoryTsoHorizon::new(store, 2);
        let oracle = TsoOracle::recover_with_heartbeat_interval(
            horizon.clone(),
            horizon.clone(),
            2,
            nonzero(8),
            4,
            Duration::from_millis(400),
        )
        .expect("recover successor");

        // Enter the grace window late: the wait must sleep only the remaining
        // ~100ms, not a duration derived from the absolute clock reading.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let first = tokio::time::timeout(Duration::from_millis(350), oracle.grant(nonzero(1)))
            .await
            .expect("grace wait must sleep only the remainder")
            .expect("grant");

        assert!(first == GrantLease::new(TsoTimestamp::new(nonzero(5)), nonzero(1)));
    }

    #[tokio::test]
    async fn epoch_certificate_detects_fence_at_expiry() {
        let store = Arc::new(MemKv::default());
        let horizon = MemoryTsoHorizon::new(store, 3);
        let clock = Arc::new(ManualClock(AtomicU64::new(1)));
        let oracle = TsoOracle::recover_with_clock(
            horizon.clone(),
            horizon.clone(),
            3,
            nonzero(100),
            0,
            Duration::from_millis(10),
            clock.clone(),
        )
        .expect("recover");
        grant_within(&oracle, nonzero(1)).await.expect("first");
        horizon.set_live_epoch(4).await;
        grant_within(&oracle, nonzero(1))
            .await
            .expect("within certificate");
        clock.0.store(11, Ordering::SeqCst);
        assert!(matches!(
            grant_within(&oracle, nonzero(1)).await,
            Err(TsoError::FencedEpoch { epoch: 3 })
        ));
        assert!(matches!(
            grant_within(&oracle, nonzero(1)).await,
            Err(TsoError::FencedEpoch { epoch: 3 })
        ));
    }

    #[tokio::test]
    async fn oracle_serves_grants_from_memory_below_durable_stride() {
        let store = Arc::new(MemKv::default());
        let horizon = MemoryTsoHorizon::new(store, 3);
        let oracle = TsoOracle::recover(horizon.clone(), horizon.clone(), 3, nonzero(10), 0)
            .expect("recover");

        let first = grant_within(&oracle, nonzero(3))
            .await
            .expect("first grant");
        let second = grant_within(&oracle, nonzero(3))
            .await
            .expect("second grant");

        assert!(first == GrantLease::new(TsoTimestamp::FIRST, nonzero(3)));
        assert!(second == GrantLease::new(TsoTimestamp::new(nonzero(4)), nonzero(3)));
        assert!(horizon.load_max_ts().expect("horizon") == 10);
    }

    #[tokio::test]
    async fn crash_recovery_resumes_after_durable_stride() {
        let store = Arc::new(MemKv::default());
        let horizon = MemoryTsoHorizon::new(store, 5);
        let oracle = TsoOracle::recover(horizon.clone(), horizon.clone(), 5, nonzero(8), 0)
            .expect("recover");

        let before_crash = grant_within(&oracle, nonzero(2)).await.expect("grant");
        let recovered = TsoOracle::recover(
            horizon.clone(),
            horizon.clone(),
            5,
            nonzero(8),
            horizon.load_max_ts().expect("horizon"),
        )
        .expect("recover successor");
        let after_crash = grant_within(&recovered, nonzero(1)).await.expect("grant");

        assert!(before_crash.last_ts().expect("last") < after_crash.first_ts);
    }

    #[tokio::test]
    async fn fenced_oracle_refuses_next_grant() {
        let store = Arc::new(MemKv::default());
        let horizon = MemoryTsoHorizon::new(store, 7);
        let oracle = TsoOracle::recover(horizon.clone(), horizon.clone(), 7, nonzero(4), 0)
            .expect("recover");

        horizon.set_live_epoch(8).await;
        let error = grant_within(&oracle, nonzero(1)).await.expect_err("fenced");

        assert!(matches!(error, TsoError::FencedEpoch { epoch: 7 }));
    }

    #[tokio::test]
    async fn horizon_persist_rejects_epoch_fenced_after_initial_liveness() {
        let store = Arc::new(MemKv::default());
        let horizon = MemoryTsoHorizon::new(store, 1);
        let racing_horizon = FenceBeforePersist {
            inner: horizon.clone(),
        };
        let stale = TsoOracle::recover(racing_horizon.clone(), horizon.clone(), 1, nonzero(4), 0)
            .expect("recover stale");

        let error = grant_within(&stale, nonzero(1))
            .await
            .expect_err("fenced persist");

        assert!(matches!(error, TsoError::FencedEpoch { epoch: 1 }));
        assert!(horizon.load_max_ts().expect("horizon") == 0);

        let successor = TsoOracle::recover(
            horizon.clone(),
            horizon.clone(),
            2,
            nonzero(4),
            horizon.load_max_ts().expect("horizon"),
        )
        .expect("recover successor");
        let grant = grant_within(&successor, nonzero(1))
            .await
            .expect("successor grant");

        assert!(grant == GrantLease::new(TsoTimestamp::FIRST, nonzero(1)));
    }

    #[derive(Clone)]
    struct FenceBeforePersist {
        inner: MemoryTsoHorizon,
    }

    #[async_trait::async_trait]
    impl TsoHorizonCommitter for FenceBeforePersist {
        async fn persist_max_ts_for_epoch(
            &self,
            epoch: i16,
            max_ts: TsoTimestamp,
        ) -> Result<(), TsoError> {
            self.inner.set_live_epoch(epoch + 1).await;
            self.inner.persist_max_ts_for_epoch(epoch, max_ts).await
        }
    }

    #[derive(Clone)]
    struct CountingCommitter {
        inner: MemoryTsoHorizon,
        persists: Arc<AtomicU64>,
    }

    #[async_trait::async_trait]
    impl TsoHorizonCommitter for CountingCommitter {
        async fn persist_max_ts_for_epoch(
            &self,
            epoch: i16,
            max_ts: TsoTimestamp,
        ) -> Result<(), TsoError> {
            self.persists.fetch_add(1, Ordering::SeqCst);
            self.inner.persist_max_ts_for_epoch(epoch, max_ts).await
        }
    }

    #[tokio::test]
    async fn successor_first_grant_waits_out_predecessor_certificate() {
        let store = Arc::new(MemKv::default());
        let horizon = MemoryTsoHorizon::new(store, 2);
        // The grace period anchors at recovery, so measure from before it.
        let started = tokio::time::Instant::now();
        let oracle = TsoOracle::recover_with_heartbeat_interval(
            horizon.clone(),
            horizon.clone(),
            2,
            nonzero(8),
            40,
            Duration::from_millis(50),
        )
        .expect("recover successor");

        let first = grant_within(&oracle, nonzero(1))
            .await
            .expect("first grant");

        assert!(started.elapsed() >= Duration::from_millis(50));
        assert!(first == GrantLease::new(TsoTimestamp::new(nonzero(41)), nonzero(1)));

        // After the grace period the hot path costs no further waiting: the
        // second grant completes well inside another certificate interval.
        let second = tokio::time::timeout(Duration::from_millis(25), oracle.grant(nonzero(1)))
            .await
            .expect("no further grace wait")
            .expect("second grant");
        assert!(second == GrantLease::new(TsoTimestamp::new(nonzero(42)), nonzero(1)));
    }

    #[tokio::test]
    async fn fresh_oracle_first_grant_needs_no_grace_period() {
        let store = Arc::new(MemKv::default());
        let horizon = MemoryTsoHorizon::new(store, 2);
        let oracle = TsoOracle::recover_with_heartbeat_interval(
            horizon.clone(),
            horizon.clone(),
            2,
            nonzero(8),
            0,
            Duration::from_secs(1),
        )
        .expect("recover fresh");

        // A grace wait would sleep one full second; a fresh oracle grants
        // far inside a fraction of that.
        let first = tokio::time::timeout(Duration::from_millis(250), oracle.grant(nonzero(1)))
            .await
            .expect("no grace wait")
            .expect("first grant");

        assert!(first == GrantLease::new(TsoTimestamp::FIRST, nonzero(1)));
    }

    #[tokio::test]
    async fn successor_clock_already_past_grace_grants_without_waiting() {
        let store = Arc::new(MemKv::default());
        let horizon = MemoryTsoHorizon::new(store, 6);
        let clock = Arc::new(ManualClock(AtomicU64::new(5)));
        let oracle = TsoOracle::recover_with_clock(
            horizon.clone(),
            horizon.clone(),
            6,
            nonzero(8),
            16,
            Duration::from_secs(1),
            clock.clone(),
        )
        .expect("recover successor");

        // Push the manual clock strictly past ready_at_ms = 5 + 1000 before
        // the first grant; a grace wait would then hang on the frozen clock
        // forever, and the strictly-past value makes a regressed readiness
        // comparison underflow immediately instead of spinning.
        clock.0.store(1_205, Ordering::SeqCst);
        let first = tokio::time::timeout(Duration::from_millis(250), oracle.grant(nonzero(1)))
            .await
            .expect("no grace wait")
            .expect("grant");

        assert!(first == GrantLease::new(TsoTimestamp::new(nonzero(17)), nonzero(1)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_grants_stay_disjoint_contiguous_and_durably_covered() {
        const TASKS: u64 = 8;
        const GRANTS_PER_TASK: u64 = 200;
        const STRIDE: u64 = 64;

        let store = Arc::new(MemKv::default());
        let horizon = MemoryTsoHorizon::new(store, 9);
        let persists = Arc::new(AtomicU64::new(0));
        let committer = CountingCommitter {
            inner: horizon.clone(),
            persists: Arc::clone(&persists),
        };
        let oracle = Arc::new(
            TsoOracle::recover(committer, horizon.clone(), 9, nonzero(STRIDE), 0).expect("recover"),
        );

        let mut workers = Vec::new();
        for task in 0..TASKS {
            let oracle = Arc::clone(&oracle);
            workers.push(tokio::spawn(async move {
                let mut leases = Vec::new();
                for grant in 0..GRANTS_PER_TASK {
                    let count = nonzero((task + grant) % 5 + 1);
                    leases.push(oracle.grant(count).await.expect("grant"));
                }
                leases
            }));
        }
        let mut ranges = Vec::new();
        for worker in workers {
            for lease in worker.await.expect("worker") {
                ranges.push((lease.first_ts.get(), lease.last_ts().expect("last").get()));
            }
        }
        ranges.sort_unstable();

        // Pairwise disjoint and an exact contiguous cover of [1, total].
        let mut expected_next = 1;
        for &(first, last) in &ranges {
            assert!(first == expected_next);
            assert!(last >= first);
            expected_next = last + 1;
        }
        let total_granted = expected_next - 1;
        assert!(horizon.load_max_ts().expect("horizon") >= total_granted);

        // Within-stride grants skipped durability: persist calls stay near
        // ceil(total / stride) and far below the number of grants.
        let persist_calls = persists.load(Ordering::SeqCst);
        let ideal = total_granted.div_ceil(STRIDE);
        assert!(persist_calls <= 2 * ideal);
        assert!(persist_calls * 10 < TASKS * GRANTS_PER_TASK);
    }
}
