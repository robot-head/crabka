//! Range-0 monotone timestamp oracle with stride-ahead durability.
//!
//! One oracle serves both timestamp domains: the dense logical counter of
//! `LogicalTso` mode, and the wall-anchored packed-HLC stamps of `Hlc` mode.
//! The durable machinery is mode-independent. It persists `max_ts` a stride
//! ahead of the grants, so a restarted range 0 never re-mints below granted
//! stamps, and it supplies the successor grace period, the liveness
//! certificates, and the epoch fencing. It is mode-independent because both
//! domains are plain `u64`s reserved in contiguous runs. Only the choice of a
//! run's first stamp differs.
//!
//! In the wall-anchored mode the stride is expressed in the packed domain, as
//! whole milliseconds of headroom, so wall time bounds the persist rate rather
//! than grant volume. The dense logical counter has no such intrinsic bound: a
//! fixed count stride would persist once per that many grants, so its
//! durable-write rate would climb with load. The logical counter therefore paces
//! its stride against wall time instead. It widens the stride under grant
//! pressure and narrows it when idle, which holds the persist rate near a
//! handful per second at any grant volume. See [`TsoOracle::persist_stride`].

use std::{
    num::NonZeroU64,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use crabka_pgexec::{HybridLogicalClock, WallClock};
use crabka_pgkv::{Kv, WriteOp};
use crabka_units::{Time, convert::TimeExt as _, millis};
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
    /// The current horizon stride, in logical mode only.
    ///
    /// The oracle widens it under grant pressure and narrows it when idle, so
    /// wall time keeps the durable persist rate bounded. See
    /// [`TsoOracle::advance_horizon`]. The field starts at the configured base
    /// stride. The wall-anchored arm does not use it, because its packed stride
    /// is already wall-bounded.
    persist_stride: u64,
    /// Monotone-clock reading, in milliseconds, of the last horizon persist, or
    /// `u64::MAX` before the first persist. The oracle uses it to measure the
    /// interval between persists.
    last_persist_ms: u64,
}

/// How the oracle carves each contiguous reservation out of the timestamp
/// space.
///
/// Both arms share every other oracle obligation: the successor grace period,
/// the liveness certificates, the stride-ahead horizon persistence, and the
/// epoch fencing. That is why the wall-anchored variant lives inside
/// [`TsoOracle`] rather than beside it. Only the choice of a run's first stamp
/// differs.
enum GrantReservation {
    /// Dense logical counter, used by `LogicalTso` mode. Each run starts
    /// immediately after the previous one, so timestamps are small consecutive
    /// integers.
    Logical(AtomicU64),
    /// Wall-anchored Hybrid Logical Clock over the packed stamp domain, used by
    /// `Hlc` mode. Each run starts at the current wall reading when the wall has
    /// moved past the last stamp. Otherwise the run packs densely behind the
    /// previous run. Logical-counter overflow carries into the physical field by
    /// plain integer arithmetic. That gives bounded drift ahead of the wall, the
    /// same budget a stalled wall clock already spends.
    WallAnchored {
        clock: HybridLogicalClock,
        wall: Arc<dyn WallClock>,
    },
}

impl GrantReservation {
    /// Reserve `count` contiguous timestamps and return `(first, last)`.
    fn reserve(&self, count: NonZeroU64) -> Result<(u64, u64), TsoError> {
        match self {
            // Compare-exchange over `fetch_add` so overflow fails the grant
            // instead of wrapping the counter.
            Self::Logical(next_ts) => {
                let mut first = next_ts.load(Ordering::Acquire);
                loop {
                    let last = first
                        .checked_add(count.get() - 1)
                        .ok_or(TsoError::TimestampOverflow)?;
                    let next = last.checked_add(1).ok_or(TsoError::TimestampOverflow)?;
                    match next_ts.compare_exchange_weak(
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
            Self::WallAnchored { clock, wall } => {
                let first = clock
                    .allocate_batch(count, wall.now_ms())
                    .ok_or(TsoError::TimestampOverflow)?;
                // The reservation overflow-checked `first + count - 1` itself.
                let last = first
                    .checked_add(count.get() - 1)
                    .ok_or(TsoError::TimestampOverflow)?;
                Ok((first, last))
            }
        }
    }
}

trait TsoClock: Send + Sync {
    fn now_ms(&self) -> u64;
}

/// Monotone clock backed by [`tokio::time::Instant`], so paused-time tests
/// advance it together with the timer wheel. In production it is identical to
/// the standard monotonic clock.
struct SystemTsoClock(Instant);

impl TsoClock for SystemTsoClock {
    fn now_ms(&self) -> u64 {
        u64::try_from(self.0.elapsed().as_millis()).unwrap_or(u64::MAX)
    }
}

/// Target minimum wall interval between logical-mode horizon persists.
///
/// The dense logical counter advances with grant volume, so a fixed-count stride
/// would persist more frequently as load rises. That is one synchronous durable
/// range-0 write on the serialized grant path every `stride` timestamps. When
/// persists arrive faster than this interval, the oracle widens the stride,
/// which halves the persist rate, until the persists space out to about this
/// cadence. A busy logical oracle therefore persists a handful of times per
/// second at any grant volume. That is the same wall-time bound the packed
/// stride gives the wall-anchored arm, and a comparable cadence.
const LOGICAL_MIN_PERSIST_INTERVAL: Time = millis(100);

/// Ceiling on the widened logical stride.
///
/// It caps how far ahead the durable horizon runs, and therefore how many
/// otherwise-unused timestamps a crash burns. That count is trivial for the
/// dense `u64` counter, whose space this never meaningfully dents.
const LOGICAL_MAX_PERSIST_STRIDE: u64 = 1 << 24;

/// Recovery-time parameters shared by both reservation modes.
#[derive(Clone, Copy)]
struct RecoverySettings {
    epoch: i16,
    stride: NonZeroU64,
    persisted_max_ts: u64,
    heartbeat_interval: Time,
    logical_min_persist_interval: Time,
    logical_max_persist_stride: u64,
}

/// Range-0 timestamp oracle.
///
/// The oracle serves within-stride grants lock-free. The reservation state,
/// `durable_max_ts`, and `certified_until_ms` are atomics that the hot path
/// reads. Only horizon advancement and certificate renewal serialize through the
/// slow-path mutex.
///
/// The memory ordering is this. The oracle stores `durable_max_ts` and
/// `certified_until_ms` with `Release`, and only after the epoch-gated persist
/// or heartbeat succeeded, and it loads them with `Acquire`. A fast path that
/// observes a horizon or a certificate therefore also observes the liveness
/// check that produced it. The reservation state is a pure counter and clock,
/// and no other data is published through it, so `AcqRel` on its
/// compare-exchange is already stronger than it needs to be.
///
/// The [`GrantReservation`] arm decides the timestamp *domain*, which is either
/// dense logical integers or wall-anchored packed HLC stamps. Everything durable
/// and everything related to fencing is identical across both arms.
pub struct TsoOracle<C, H> {
    committer: C,
    heartbeat: H,
    epoch: i16,
    stride: NonZeroU64,
    heartbeat_interval: Time,
    logical_min_persist_interval: Time,
    logical_max_persist_stride: u64,
    clock: Arc<dyn TsoClock>,
    ready: AtomicBool,
    ready_at_ms: u64,
    reservation: GrantReservation,
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
        Self::recover_with_policy(
            committer,
            heartbeat,
            epoch,
            stride,
            persisted_max_ts,
            &crate::RangeRuntimePolicy::default(),
        )
    }

    /// Recover a logical oracle using explicit runtime policy.
    /// # Errors
    /// Returns an error when the durable horizon is invalid.
    pub fn recover_with_policy(
        committer: C,
        heartbeat: H,
        epoch: i16,
        stride: NonZeroU64,
        persisted_max_ts: u64,
        policy: &crate::RangeRuntimePolicy,
    ) -> Result<Self, TsoError> {
        Self::recover_with_clock_and_policy(
            committer,
            heartbeat,
            RecoverySettings {
                epoch,
                stride,
                persisted_max_ts,
                heartbeat_interval: policy.tso_heartbeat_interval,
                logical_min_persist_interval: policy.logical_min_persist_interval,
                logical_max_persist_stride: policy.logical_max_persist_stride.get(),
            },
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
        heartbeat_interval: Time,
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
        heartbeat_interval: Time,
        clock: Arc<dyn TsoClock>,
    ) -> Result<Self, TsoError> {
        Self::recover_with_clock_and_policy(
            committer,
            heartbeat,
            RecoverySettings {
                epoch,
                stride,
                persisted_max_ts,
                heartbeat_interval,
                logical_min_persist_interval: LOGICAL_MIN_PERSIST_INTERVAL,
                logical_max_persist_stride: LOGICAL_MAX_PERSIST_STRIDE,
            },
            clock,
        )
    }

    fn recover_with_clock_and_policy(
        committer: C,
        heartbeat: H,
        settings: RecoverySettings,
        clock: Arc<dyn TsoClock>,
    ) -> Result<Self, TsoError> {
        let persisted_max_ts = settings.persisted_max_ts;
        let reservation = GrantReservation::Logical(AtomicU64::new(
            TsoTimestamp::from_persisted_next(persisted_max_ts)?.get(),
        ));
        Ok(Self::assemble(
            committer,
            heartbeat,
            settings,
            clock,
            reservation,
        ))
    }

    /// Recover a wall-anchored HLC oracle from an already-replayed durable
    /// horizon.
    ///
    /// The reservation clock starts at `persisted_max_ts`. The first grant
    /// therefore strictly dominates everything any predecessor oracle could have
    /// granted, and by monotonicity so does every later grant. This holds even
    /// when `wall` reads behind the predecessor's wall clock. `stride` is in the
    /// packed stamp domain. See the horizon-persistence notes on [`TsoOracle`].
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn recover_hlc(
        committer: C,
        heartbeat: H,
        epoch: i16,
        stride: NonZeroU64,
        persisted_max_ts: u64,
        wall: Arc<dyn WallClock>,
    ) -> Result<Self, TsoError> {
        Self::recover_hlc_with_policy(
            committer,
            heartbeat,
            epoch,
            stride,
            persisted_max_ts,
            wall,
            &crate::RangeRuntimePolicy::default(),
        )
    }

    /// Recover an HLC oracle using explicit runtime policy.
    /// # Errors
    /// Returns an error when the durable horizon is invalid.
    pub fn recover_hlc_with_policy(
        committer: C,
        heartbeat: H,
        epoch: i16,
        stride: NonZeroU64,
        persisted_max_ts: u64,
        wall: Arc<dyn WallClock>,
        policy: &crate::RangeRuntimePolicy,
    ) -> Result<Self, TsoError> {
        let settings = RecoverySettings {
            epoch,
            stride,
            persisted_max_ts,
            heartbeat_interval: policy.tso_heartbeat_interval,
            logical_min_persist_interval: policy.logical_min_persist_interval,
            logical_max_persist_stride: policy.logical_max_persist_stride.get(),
        };
        Ok(Self::recover_hlc_with_clock(
            committer,
            heartbeat,
            settings,
            wall,
            Arc::new(SystemTsoClock(Instant::now())),
        ))
    }

    fn recover_hlc_with_clock(
        committer: C,
        heartbeat: H,
        settings: RecoverySettings,
        wall: Arc<dyn WallClock>,
        clock: Arc<dyn TsoClock>,
    ) -> Self {
        let reservation = GrantReservation::WallAnchored {
            clock: HybridLogicalClock::seeded_at(settings.persisted_max_ts),
            wall,
        };
        Self::assemble(committer, heartbeat, settings, clock, reservation)
    }

    fn assemble(
        committer: C,
        heartbeat: H,
        settings: RecoverySettings,
        clock: Arc<dyn TsoClock>,
        reservation: GrantReservation,
    ) -> Self {
        let RecoverySettings {
            epoch,
            stride,
            persisted_max_ts,
            heartbeat_interval,
            logical_min_persist_interval,
            logical_max_persist_stride,
        } = settings;
        // A zero horizon proves no predecessor ever granted (every grant
        // persists a stride first), so no successor grace period is needed.
        let ready_at_ms = if persisted_max_ts == 0 {
            0
        } else {
            clock
                .now_ms()
                .saturating_add(interval_ms(heartbeat_interval))
        };
        Self {
            committer,
            heartbeat,
            epoch,
            stride,
            heartbeat_interval,
            logical_min_persist_interval,
            logical_max_persist_stride,
            clock,
            ready: AtomicBool::new(ready_at_ms == 0),
            ready_at_ms,
            reservation,
            durable_max_ts: AtomicU64::new(persisted_max_ts),
            certified_until_ms: AtomicU64::new(0),
            slow: Mutex::new(SlowState {
                has_granted: false,
                persist_stride: stride.get(),
                last_persist_ms: u64::MAX,
            }),
            stats: Arc::default(),
        }
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
    /// The invariant is this. A fenced but live predecessor keeps serving
    /// within-stride grants from memory until its liveness certificate lapses.
    /// That certificate was anchored before the fence, so it expires no later
    /// than one `heartbeat_interval` past the fence, and the fence itself
    /// precedes this successor's recovery. A wait of one full interval after
    /// recovery therefore outlasts every possible predecessor certificate. This
    /// oracle acknowledges no grant while the predecessor can still serve, so no
    /// granted read timestamp can precede a commit acknowledged before the
    /// grant. This assumes `heartbeat_interval` is identical across writer
    /// generations. Today it is a compile-time constant.
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

    /// Reserve `count` contiguous timestamps from the shared reservation state.
    fn reserve(&self, count: NonZeroU64) -> Result<(u64, u64), TsoError> {
        self.reservation.reserve(count)
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
            now_ms.saturating_add(interval_ms(self.heartbeat_interval)),
            Ordering::Release,
        );
        Ok(())
    }

    /// Persist a stride-advanced horizon covering the reserved range, then
    /// publish the horizon and a fresh certificate.
    ///
    /// On a failure this method burns the reserved timestamps and never grants
    /// them. Gaps are harmless. Monotonicity is the requirement.
    async fn advance_horizon(&self, first: u64, last: u64) -> Result<(), TsoError> {
        self.stats.record_horizon_wait();
        let mut slow = self.slow.lock().await;
        // Another waiter may have advanced the horizon past this reservation.
        if last <= self.durable_max_ts.load(Ordering::Acquire) {
            return Ok(());
        }
        // Anchor the certificate before the epoch-gated persist round-trip, and
        // reuse the same reading to pace the logical stride against wall time.
        let now_ms = self.clock.now_ms();
        let stride = self.persist_stride(&mut slow, now_ms);
        let stride_last = first
            .checked_add(stride - 1)
            .ok_or(TsoError::TimestampOverflow)?;
        let new_horizon = last.max(stride_last);
        let persisted =
            TsoTimestamp::new(NonZeroU64::new(new_horizon).ok_or(TsoError::TimestampOverflow)?);
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
            now_ms.saturating_add(interval_ms(self.heartbeat_interval)),
            Ordering::Release,
        );
        Ok(())
    }

    /// Choose the horizon stride for this persist, and pace the logical arm
    /// against wall time.
    ///
    /// The wall-anchored arm keeps its fixed packed stride. That stride is
    /// already whole milliseconds of wall headroom, so wall time bounds its
    /// persist cadence. The dense logical arm advances with grant volume, so a
    /// fixed count stride would persist more often as load rises and would flood
    /// the serialized grant path with synchronous durable writes.
    ///
    /// This method paces the logical arm against wall time instead. It widens
    /// the stride, which halves the persist rate, whenever persists arrive
    /// closer together than [`LOGICAL_MIN_PERSIST_INTERVAL`]. It narrows the
    /// stride back toward the base after the persists space out. The persist
    /// rate therefore settles at a handful per second under any grant volume,
    /// and a light or idle oracle keeps a tight horizon. The caller holds the
    /// slow-path mutex.
    fn persist_stride(&self, slow: &mut SlowState, now_ms: u64) -> u64 {
        match &self.reservation {
            GrantReservation::WallAnchored { .. } => self.stride.get(),
            GrantReservation::Logical(_) => {
                let target_ms = interval_ms(self.logical_min_persist_interval);
                let elapsed = now_ms.saturating_sub(slow.last_persist_ms);
                if slow.last_persist_ms != u64::MAX && elapsed < target_ms {
                    slow.persist_stride = slow
                        .persist_stride
                        .saturating_mul(2)
                        .min(self.logical_max_persist_stride);
                } else if elapsed > target_ms.saturating_mul(4) {
                    slow.persist_stride = (slow.persist_stride / 2).max(self.stride.get());
                }
                slow.last_persist_ms = now_ms;
                slow.persist_stride
            }
        }
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

/// An interval in whole milliseconds, for comparison against the oracle's own
/// monotone millisecond clock.
///
/// This function rounds to nearest. The result is internal only: it never
/// reaches a wire field, a durable record, or any external system. Nearest is
/// therefore the honest reading of the configured extent.
fn interval_ms(interval: Time) -> u64 {
    u64::try_from(interval.millis_i64()).unwrap_or(u64::MAX)
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
    use crabka_units::secs;

    use super::*;

    fn nonzero(value: u64) -> NonZeroU64 {
        NonZeroU64::new(value).expect("test count is non-zero")
    }

    /// Grant with a hang bound. Any regression that turns a grant into an
    /// unbounded wait must fail the suite quickly, and must not stall it past
    /// the mutation-testing budget.
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
            millis(10),
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
            millis(10),
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
            millis(400),
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
            millis(10),
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
    async fn logical_persist_cadence_stays_bounded_under_sustained_grants() {
        // The dense logical counter advances with grant volume, so a fixed
        // count stride would persist once per `STRIDE` timestamps. A pinned
        // wall clock models the worst case: grants pour in with no wall time
        // passing between persists. Without pacing this run would perform
        // `GRANTS` = 4096 synchronous durable writes; the adaptive stride
        // widens on each too-soon persist, holding the count logarithmic.
        const STRIDE: u64 = 8;
        const GRANTS: u64 = 4096; // GRANTS * STRIDE timestamps consumed

        let store = Arc::new(MemKv::default());
        let horizon = MemoryTsoHorizon::new(store, 3);
        let persists = Arc::new(AtomicU64::new(0));
        let committer = CountingCommitter {
            inner: horizon.clone(),
            persists: Arc::clone(&persists),
        };
        // Clock pinned at 1 (past the absent grace period) and never advanced,
        // so every persist sees a zero interval and must widen the stride.
        let clock = Arc::new(ManualClock(AtomicU64::new(1)));
        let oracle = TsoOracle::recover_with_clock(
            committer,
            horizon.clone(),
            3,
            nonzero(STRIDE),
            0,
            millis(10),
            clock,
        )
        .expect("recover");

        for _ in 0..GRANTS {
            grant_within(&oracle, nonzero(STRIDE)).await.expect("grant");
        }

        // Geometric widening bounds this to ~log2(GRANTS) plus a small
        // constant, versus GRANTS = 4096 without pacing.
        let persist_count = persists.load(Ordering::SeqCst);
        assert!(
            persist_count <= 20,
            "logical persists should stay logarithmic, got {persist_count}"
        );
        // Durability is preserved — only the cadence changed. The horizon still
        // dominates every granted timestamp.
        assert!(horizon.load_max_ts().expect("horizon") >= GRANTS * STRIDE);
    }

    #[tokio::test]
    async fn persist_stride_widens_under_pressure_and_narrows_when_idle() {
        let store = Arc::new(MemKv::default());
        let horizon = MemoryTsoHorizon::new(store, 1);
        let clock = Arc::new(ManualClock(AtomicU64::new(0)));
        let oracle = TsoOracle::recover_with_clock(
            horizon.clone(),
            horizon.clone(),
            1,
            nonzero(1024),
            0,
            millis(10),
            clock,
        )
        .expect("recover");

        let mut slow = oracle.slow.lock().await;
        // The first persist has no prior interval to measure, so the stride
        // stays at the base.
        assert!(oracle.persist_stride(&mut slow, 1_000) == 1_024);
        // Persists arriving within the target interval keep doubling the
        // stride, halving the persist rate each time.
        assert!(oracle.persist_stride(&mut slow, 1_000) == 2_048);
        assert!(oracle.persist_stride(&mut slow, 1_000) == 4_096);
        let target_ms = interval_ms(LOGICAL_MIN_PERSIST_INTERVAL);
        let last_persist = 1_000 + target_ms - 1;
        assert!(oracle.persist_stride(&mut slow, last_persist) == 8_192);
        // A gap well past the target interval narrows the stride back toward
        // the base, so an idle oracle keeps a tight horizon.
        let idle_at = last_persist + target_ms * 4 + 1;
        assert!(oracle.persist_stride(&mut slow, idle_at) == 4_096);
    }

    #[tokio::test]
    async fn persist_stride_leaves_the_wall_anchored_stride_fixed() {
        let store = Arc::new(MemKv::default());
        let horizon = MemoryTsoHorizon::new(store, 1);
        let wall = Arc::new(crabka_pgexec::ManualWallClock::new(1_000));
        let clock = Arc::new(ManualClock(AtomicU64::new(0)));
        let oracle = TsoOracle::recover_hlc_with_clock(
            horizon.clone(),
            horizon.clone(),
            hlc_settings(1, 128, 0),
            Arc::clone(&wall) as Arc<dyn WallClock>,
            clock,
        );
        let fixed = crabka_pgexec::hlc::pack(128, 0);

        let mut slow = oracle.slow.lock().await;
        // The wall-anchored arm's packed stride is already whole milliseconds
        // of wall headroom, so pacing never touches it however fast persists
        // arrive.
        assert!(oracle.persist_stride(&mut slow, 1_000) == fixed);
        assert!(oracle.persist_stride(&mut slow, 1_000) == fixed);
        assert!(oracle.persist_stride(&mut slow, 1_000) == fixed);
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
            millis(50),
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
            secs(1),
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
            secs(1),
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

    fn hlc_settings(epoch: i16, stride_ms: u64, persisted_max_ts: u64) -> RecoverySettings {
        RecoverySettings {
            epoch,
            stride: nonzero(crabka_pgexec::hlc::pack(stride_ms, 0)),
            persisted_max_ts,
            heartbeat_interval: millis(10),
            logical_min_persist_interval: LOGICAL_MIN_PERSIST_INTERVAL,
            logical_max_persist_stride: LOGICAL_MAX_PERSIST_STRIDE,
        }
    }

    #[tokio::test]
    async fn hlc_grants_are_wall_anchored_and_persist_packed_strides() {
        use crabka_pgexec::hlc::pack;

        let store = Arc::new(MemKv::default());
        let horizon = MemoryTsoHorizon::new(store, 3);
        let persists = Arc::new(AtomicU64::new(0));
        let committer = CountingCommitter {
            inner: horizon.clone(),
            persists: Arc::clone(&persists),
        };
        let wall = Arc::new(crabka_pgexec::ManualWallClock::new(1_000));
        let clock = Arc::new(ManualClock(AtomicU64::new(1)));
        let oracle = TsoOracle::recover_hlc_with_clock(
            committer,
            horizon.clone(),
            hlc_settings(3, 128, 0),
            Arc::clone(&wall) as Arc<dyn WallClock>,
            clock,
        );

        // The first grant anchors at the wall reading and persists one packed
        // stride (128 ms of headroom) ahead of it.
        let first = grant_within(&oracle, nonzero(3)).await.expect("first");
        assert!(first == GrantLease::new(TsoTimestamp::new(nonzero(pack(1_000, 0))), nonzero(3)));
        assert!(horizon.load_max_ts().expect("horizon") == pack(1_000, 0) + pack(128, 0) - 1);
        assert!(persists.load(Ordering::SeqCst) == 1);

        // A stalled wall packs the next grant densely behind the first, still
        // under the persisted stride: no new persist.
        let second = grant_within(&oracle, nonzero(2)).await.expect("second");
        assert!(second == GrantLease::new(TsoTimestamp::new(nonzero(pack(1_000, 3))), nonzero(2)));
        assert!(persists.load(Ordering::SeqCst) == 1);

        // An advancing wall re-anchors the run at the new reading; 50 ms of
        // movement stays inside the 128 ms stride, so still no persist.
        wall.set(1_050);
        let third = grant_within(&oracle, nonzero(1)).await.expect("third");
        assert!(third == GrantLease::new(TsoTimestamp::new(nonzero(pack(1_050, 0))), nonzero(1)));
        assert!(persists.load(Ordering::SeqCst) == 1);

        // Crossing the persisted stride advances the horizon exactly once more.
        wall.set(1_200);
        let fourth = grant_within(&oracle, nonzero(1)).await.expect("fourth");
        assert!(fourth == GrantLease::new(TsoTimestamp::new(nonzero(pack(1_200, 0))), nonzero(1)));
        assert!(persists.load(Ordering::SeqCst) == 2);
        assert!(horizon.load_max_ts().expect("horizon") == pack(1_200, 0) + pack(128, 0) - 1);
    }

    #[tokio::test]
    async fn hlc_restart_dominates_predecessor_grants_despite_wall_regression() {
        use crabka_pgexec::hlc::unpack;

        let store = Arc::new(MemKv::default());
        let horizon = MemoryTsoHorizon::new(store, 5);
        let predecessor_wall = Arc::new(crabka_pgexec::ManualWallClock::new(2_000));
        let predecessor_clock = Arc::new(ManualClock(AtomicU64::new(1)));
        let predecessor = TsoOracle::recover_hlc_with_clock(
            horizon.clone(),
            horizon.clone(),
            hlc_settings(5, 64, 0),
            predecessor_wall as Arc<dyn WallClock>,
            predecessor_clock,
        );
        let before_crash = grant_within(&predecessor, nonzero(10))
            .await
            .expect("grant");
        assert!(unpack(before_crash.first_ts.get()).physical_ms == 2_000);
        drop(predecessor);

        // The successor's wall clock reads far BEHIND the predecessor's, so
        // only horizon seeding — not wall luck — can provide monotonicity.
        let persisted = horizon.load_max_ts().expect("horizon");
        let successor_wall = Arc::new(crabka_pgexec::ManualWallClock::new(10));
        let successor_clock = Arc::new(ManualClock(AtomicU64::new(1)));
        let successor = TsoOracle::recover_hlc_with_clock(
            horizon.clone(),
            horizon.clone(),
            hlc_settings(5, 64, persisted),
            successor_wall as Arc<dyn WallClock>,
            Arc::clone(&successor_clock) as Arc<dyn TsoClock>,
        );
        // Step the manual clock strictly past the successor grace period.
        successor_clock.0.store(12, Ordering::SeqCst);
        let after_crash = grant_within(&successor, nonzero(1)).await.expect("grant");

        assert!(after_crash.first_ts > before_crash.last_ts().expect("last"));
        // The stride persistence guarantees the persisted horizon dominates
        // every predecessor grant; the successor continues right above it.
        assert!(persisted >= before_crash.last_ts().expect("last").get());
        assert!(after_crash.first_ts.get() == persisted + 1);
        assert!(unpack(after_crash.first_ts.get()).physical_ms >= 2_000);
    }

    #[tokio::test]
    async fn hlc_oracle_refuses_grants_once_fenced() {
        let store = Arc::new(MemKv::default());
        let horizon = MemoryTsoHorizon::new(store, 7);
        let wall = Arc::new(crabka_pgexec::ManualWallClock::new(500));
        let oracle = TsoOracle::recover_hlc(
            horizon.clone(),
            horizon.clone(),
            7,
            nonzero(crabka_pgexec::hlc::pack(128, 0)),
            0,
            wall as Arc<dyn WallClock>,
        )
        .expect("recover");

        horizon.set_live_epoch(8).await;
        let error = grant_within(&oracle, nonzero(1)).await.expect_err("fenced");

        assert!(matches!(error, TsoError::FencedEpoch { epoch: 7 }));
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
