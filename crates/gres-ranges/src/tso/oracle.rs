//! Range-0 monotone timestamp oracle with stride-ahead durability.

use std::{
    num::NonZeroU64,
    sync::Arc,
    time::{Duration, Instant},
};

use crabka_pgkv::{Kv, WriteOp};
use tokio::sync::Mutex;

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

#[derive(Debug, Clone, Copy)]
struct OracleState {
    next_ts: TsoTimestamp,
    durable_max_ts: u64,
    certified_until_ms: u64,
    has_granted: bool,
}

trait TsoClock: Send + Sync {
    fn now_ms(&self) -> u64;
}

struct SystemTsoClock(Instant);

impl TsoClock for SystemTsoClock {
    fn now_ms(&self) -> u64 {
        u64::try_from(self.0.elapsed().as_millis()).unwrap_or(u64::MAX)
    }
}

const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(10);

/// Range-0 timestamp oracle.
pub struct TsoOracle<C, H> {
    committer: C,
    heartbeat: H,
    epoch: i16,
    stride: NonZeroU64,
    heartbeat_interval: Duration,
    clock: Arc<dyn TsoClock>,
    state: Mutex<OracleState>,
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
        Ok(Self {
            committer,
            heartbeat,
            epoch,
            stride,
            heartbeat_interval,
            clock,
            state: Mutex::new(OracleState {
                next_ts: TsoTimestamp::from_persisted_next(persisted_max_ts)?,
                durable_max_ts: persisted_max_ts,
                certified_until_ms: 0,
                has_granted: false,
            }),
        })
    }

    /// Grant a non-empty contiguous timestamp lease.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn grant(&self, count: NonZeroU64) -> Result<GrantLease, TsoError> {
        let mut state = self.state.lock().await;
        let first_ts = state.next_ts;
        let requested_last = checked_last(first_ts, count)?;
        let now_ms = self.clock.now_ms();
        if requested_last > state.durable_max_ts {
            let stride_last = checked_last(first_ts, self.stride)?;
            let new_horizon = requested_last.max(stride_last);
            let persisted =
                TsoTimestamp::new(NonZeroU64::new(new_horizon).ok_or(TsoError::TimestampOverflow)?);
            self.committer
                .persist_max_ts_for_epoch(self.epoch, persisted)
                .await?;
            if !state.has_granted {
                self.ensure_epoch_live().await?;
            }
            state.durable_max_ts = new_horizon;
            state.certified_until_ms = now_ms.saturating_add(
                u64::try_from(self.heartbeat_interval.as_millis()).unwrap_or(u64::MAX),
            );
        } else if now_ms >= state.certified_until_ms {
            self.ensure_epoch_live().await?;
            state.certified_until_ms = now_ms.saturating_add(
                u64::try_from(self.heartbeat_interval.as_millis()).unwrap_or(u64::MAX),
            );
        }

        state.next_ts = TsoTimestamp::from_persisted_next(requested_last)?;
        state.has_granted = true;
        Ok(GrantLease::new(first_ts, count))
    }

    async fn ensure_epoch_live(&self) -> Result<(), TsoError> {
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

fn checked_last(first_ts: TsoTimestamp, count: NonZeroU64) -> Result<u64, TsoError> {
    first_ts
        .get()
        .checked_add(count.get() - 1)
        .ok_or(TsoError::TimestampOverflow)
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

        oracle.grant(nonzero(1)).await.expect("first");
        oracle.grant(nonzero(1)).await.expect("certified");
        assert!(calls.load(Ordering::SeqCst) == 1);
        clock.0.store(11, Ordering::SeqCst);
        oracle.grant(nonzero(1)).await.expect("renewed");
        assert!(calls.load(Ordering::SeqCst) == 2);
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
        oracle.grant(nonzero(1)).await.expect("first");
        horizon.set_live_epoch(4).await;
        oracle.grant(nonzero(1)).await.expect("within certificate");
        clock.0.store(11, Ordering::SeqCst);
        assert!(matches!(
            oracle.grant(nonzero(1)).await,
            Err(TsoError::FencedEpoch { epoch: 3 })
        ));
        assert!(matches!(
            oracle.grant(nonzero(1)).await,
            Err(TsoError::FencedEpoch { epoch: 3 })
        ));
    }

    #[tokio::test]
    async fn oracle_serves_grants_from_memory_below_durable_stride() {
        let store = Arc::new(MemKv::default());
        let horizon = MemoryTsoHorizon::new(store, 3);
        let oracle = TsoOracle::recover(horizon.clone(), horizon.clone(), 3, nonzero(10), 0)
            .expect("recover");

        let first = oracle.grant(nonzero(3)).await.expect("first grant");
        let second = oracle.grant(nonzero(3)).await.expect("second grant");

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

        let before_crash = oracle.grant(nonzero(2)).await.expect("grant");
        let recovered = TsoOracle::recover(
            horizon.clone(),
            horizon.clone(),
            5,
            nonzero(8),
            horizon.load_max_ts().expect("horizon"),
        )
        .expect("recover successor");
        let after_crash = recovered.grant(nonzero(1)).await.expect("grant");

        assert!(before_crash.last_ts().expect("last") < after_crash.first_ts);
    }

    #[tokio::test]
    async fn fenced_oracle_refuses_next_grant() {
        let store = Arc::new(MemKv::default());
        let horizon = MemoryTsoHorizon::new(store, 7);
        let oracle = TsoOracle::recover(horizon.clone(), horizon.clone(), 7, nonzero(4), 0)
            .expect("recover");

        horizon.set_live_epoch(8).await;
        let error = oracle.grant(nonzero(1)).await.expect_err("fenced");

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

        let error = stale.grant(nonzero(1)).await.expect_err("fenced persist");

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
        let grant = successor.grant(nonzero(1)).await.expect("successor grant");

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
}
