//! [`TimestampSource`] backed by a node-local Hybrid Logical Clock.
//!
//! This is the distributed-mode ([`Hlc`]) implementation of the timestamp seam:
//! every allocation is minted from the local [`HybridLogicalClock`] with no RPC
//! to a central authority. Wall-clock time is *injected* through the
//! [`WallClock`] trait — a [`SystemWallClock`] in production, a
//! [`ManualWallClock`] under test — mirroring the `TsoClock` injection the
//! range-0 oracle uses for deterministic timing.
//!
//! The seam's durable-horizon fencing survives: the `_after` variants fold the
//! horizon into the clock (an `observe`) before minting, so every stamp strictly
//! exceeds the durable horizon by construction rather than being checked and
//! rejected after the fact. [`HlcTimestampSource::seeded_from_horizon`] is the
//! promotion constructor — a solo tenant's persisted `LogicalTso` horizon (a
//! packed stamp with physical component zero) is folded in so the first
//! distributed stamp dominates it.
//!
//! [`Hlc`]: crate::hlc

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    hlc::{HybridLogicalClock, pack},
    timestamp_txn::{
        CommitTimestamp, ReadTimestamp, TimestampSource, TimestampSourceError,
        TimestampTransactionId,
    },
};

/// Injected wall-clock source, in milliseconds since the Unix epoch.
///
/// The clock rules never read `SystemTime` directly so allocation is
/// deterministic under test; production wires [`SystemWallClock`].
pub trait WallClock: Send + Sync {
    /// Current wall-clock reading in milliseconds since the Unix epoch.
    fn now_ms(&self) -> u64;
}

/// Production [`WallClock`] reading the system clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemWallClock;

impl WallClock for SystemWallClock {
    fn now_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| {
                u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
            })
    }
}

/// Manually driven [`WallClock`] for deterministic tests.
#[derive(Debug, Default)]
pub struct ManualWallClock(AtomicU64);

impl ManualWallClock {
    /// Create a manual clock reading `now_ms`.
    #[must_use]
    pub fn new(now_ms: u64) -> Self {
        Self(AtomicU64::new(now_ms))
    }

    /// Set the reading returned by subsequent [`WallClock::now_ms`] calls.
    pub fn set(&self, now_ms: u64) {
        self.0.store(now_ms, Ordering::Release);
    }
}

impl WallClock for ManualWallClock {
    fn now_ms(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }
}

/// Node-local Hybrid Logical Clock exposed as a [`TimestampSource`].
///
/// Allocation draws straight off the local [`HybridLogicalClock`]; `observe`
/// folds a remote stamp under the HLC receive rule; `uncertainty_window`
/// reports the configured `max_offset` (in the packed timestamp domain) that the
/// multi-node read path will later size its restart window against. A
/// single-source deployment — one `HlcTimestampSource` fanned to every engine —
/// is correct on its own: there is one timestamp authority, so no cross-node
/// skew and no read-restart. Multi-node stamping (folding participant HLCs at
/// the cross-range commit site) and uncertainty-driven read-restart are the
/// documented follow-up.
pub struct HlcTimestampSource {
    clock: HybridLogicalClock,
    wall: Arc<dyn WallClock>,
    /// Maximum clock offset in the packed timestamp domain (physical ms shifted
    /// into the high bits). Zero means an empty uncertainty window.
    max_offset: u64,
    /// This node's dense per-tenant index, stamped into every transaction
    /// identity this source originates so two nodes minting the same `start_ts`
    /// stay distinct. `0` collapses to single-source behavior.
    node_id: u16,
}

impl std::fmt::Debug for HlcTimestampSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HlcTimestampSource")
            .field("clock", &self.clock)
            .field("max_offset", &self.max_offset)
            .field("node_id", &self.node_id)
            .finish_non_exhaustive()
    }
}

impl HlcTimestampSource {
    /// Build a fresh source over `wall` with a `max_offset_ms` uncertainty bound,
    /// minting on node `node_id`.
    #[must_use]
    pub fn new(wall: Arc<dyn WallClock>, max_offset_ms: u64, node_id: u16) -> Self {
        Self {
            clock: HybridLogicalClock::new(),
            wall,
            max_offset: pack(max_offset_ms, 0),
            node_id,
        }
    }

    /// Build a source whose clock is seeded so no stamp it mints can fall at or
    /// below `horizon`, minting on node `node_id`.
    ///
    /// This is the promotion constructor: `horizon` is the fenced solo tenant's
    /// persisted `LogicalTso` horizon (a packed stamp with physical component
    /// zero). Seeding folds it in so the first — and, by monotonicity, every —
    /// distributed stamp strictly dominates it.
    #[must_use]
    pub fn seeded_from_horizon(
        horizon: u64,
        wall: Arc<dyn WallClock>,
        max_offset_ms: u64,
        node_id: u16,
    ) -> Self {
        Self {
            clock: HybridLogicalClock::seeded_at(horizon),
            wall,
            max_offset: pack(max_offset_ms, 0),
            node_id,
        }
    }

    /// Mint a fresh stamp under the HLC local/send rule.
    fn mint(&self) -> u64 {
        self.clock.now(self.wall.now_ms())
    }

    /// Fold `floor` into the clock and mint a stamp strictly greater than it.
    fn mint_after(&self, floor: u64) -> u64 {
        self.clock.observe(floor, self.wall.now_ms())
    }
}

#[async_trait::async_trait]
impl TimestampSource for HlcTimestampSource {
    async fn allocate_read_timestamp(&self) -> Result<ReadTimestamp, TimestampSourceError> {
        ReadTimestamp::new(self.mint()).map_err(Into::into)
    }

    async fn allocate_transaction_id(
        &self,
    ) -> Result<TimestampTransactionId, TimestampSourceError> {
        TimestampTransactionId::new(self.mint()).map_err(Into::into)
    }

    async fn allocate_commit_after(
        &self,
        start_ts: TimestampTransactionId,
    ) -> Result<CommitTimestamp, TimestampSourceError> {
        // Fold the start timestamp, then mint: the HLC receive rule guarantees
        // the result strictly dominates `start_ts`.
        let commit = self.mint_after(start_ts.get());
        CommitTimestamp::after_start(start_ts, commit).map_err(Into::into)
    }

    async fn allocate_read_timestamp_after(
        &self,
        durable_horizon: u64,
    ) -> Result<ReadTimestamp, TimestampSourceError> {
        ReadTimestamp::new(self.mint_after(durable_horizon)).map_err(Into::into)
    }

    async fn allocate_transaction_id_after(
        &self,
        durable_horizon: u64,
    ) -> Result<TimestampTransactionId, TimestampSourceError> {
        TimestampTransactionId::new(self.mint_after(durable_horizon)).map_err(Into::into)
    }

    async fn allocate_commit_after_durable(
        &self,
        start_ts: TimestampTransactionId,
        durable_horizon: u64,
    ) -> Result<CommitTimestamp, TimestampSourceError> {
        let floor = durable_horizon.max(start_ts.get());
        let commit = self.mint_after(floor);
        CommitTimestamp::after_start(start_ts, commit).map_err(Into::into)
    }

    fn observe(&self, observed_ts: u64) {
        let _ = self.clock.observe(observed_ts, self.wall.now_ms());
    }

    fn uncertainty_window(&self) -> u64 {
        self.max_offset
    }

    fn node_id(&self) -> u16 {
        self.node_id
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::hlc::{LOGICAL_BITS, MAX_LOGICAL, unpack};

    fn source_at(wall_ms: u64, max_offset_ms: u64) -> (HlcTimestampSource, Arc<ManualWallClock>) {
        let wall = Arc::new(ManualWallClock::new(wall_ms));
        let source =
            HlcTimestampSource::new(Arc::clone(&wall) as Arc<dyn WallClock>, max_offset_ms, 0);
        (source, wall)
    }

    #[test]
    fn node_id_is_reported_from_construction() {
        let wall = Arc::new(ManualWallClock::new(100));
        let source = HlcTimestampSource::new(wall as Arc<dyn WallClock>, 0, 42);
        assert!(TimestampSource::node_id(&source) == 42);
    }

    #[tokio::test]
    async fn allocations_are_monotonic_even_when_wall_stalls_and_regresses() {
        let (source, wall) = source_at(100, 0);
        let mut previous = 0;
        for wall_ms in [100, 100, 50, 101, 101, 10] {
            wall.set(wall_ms);
            let read = source.allocate_read_timestamp().await.expect("read").get();
            assert!(read > previous);
            previous = read;
            let start = source.allocate_transaction_id().await.expect("start").get();
            assert!(start > previous);
            previous = start;
        }
    }

    #[tokio::test]
    async fn commit_strictly_follows_start_within_the_same_millisecond() {
        let (source, _wall) = source_at(7, 0);
        let start = source
            .allocate_transaction_id()
            .await
            .expect("start timestamp");
        let commit = source
            .allocate_commit_after(start)
            .await
            .expect("commit timestamp");
        assert!(commit.get() > start.get());
    }

    #[tokio::test]
    async fn observe_folds_a_remote_stamp_so_the_next_allocation_exceeds_it() {
        let (source, _wall) = source_at(5, 0);
        // A remote stamp from a node whose physical clock is far ahead.
        let remote = pack(1_000, 3);
        source.observe(remote);
        let next = source.allocate_read_timestamp().await.expect("read").get();
        assert!(next > remote);
    }

    #[tokio::test]
    async fn after_variants_fence_the_durable_horizon() {
        let (source, _wall) = source_at(5, 0);
        // A horizon well above the local wall clock must still be exceeded.
        let horizon = pack(9_000, 11);
        let read = source
            .allocate_read_timestamp_after(horizon)
            .await
            .expect("read after")
            .get();
        assert!(read > horizon);
        let start = source
            .allocate_transaction_id_after(horizon)
            .await
            .expect("start after");
        assert!(start.get() > horizon);
        let commit = source
            .allocate_commit_after_durable(start, horizon)
            .await
            .expect("commit after durable");
        assert!(commit.get() > horizon);
        assert!(commit.get() > start.get());
    }

    #[tokio::test]
    async fn every_post_seed_stamp_exceeds_the_seed_horizon() {
        // A persisted LogicalTso horizon is a packed stamp with physical zero.
        let horizon = pack(0, MAX_LOGICAL);
        let wall = Arc::new(ManualWallClock::new(0));
        let source = HlcTimestampSource::seeded_from_horizon(
            horizon,
            Arc::clone(&wall) as Arc<dyn WallClock>,
            250,
            0,
        );
        // Drive wall time backwards and forwards; no stamp may reach the horizon.
        for wall_ms in [0, 0, 1, 0, 2, 1] {
            wall.set(wall_ms);
            let read = source.allocate_read_timestamp().await.expect("read").get();
            assert!(read > horizon);
            let start = source.allocate_transaction_id().await.expect("start").get();
            assert!(start > horizon);
        }
    }

    #[tokio::test]
    async fn uncertainty_window_reports_the_configured_offset_in_the_packed_domain() {
        let (empty, _wall) = source_at(5, 0);
        assert!(empty.uncertainty_window() == 0);

        let (offset, _wall) = source_at(5, 250);
        assert!(offset.uncertainty_window() == 250_u64 << LOGICAL_BITS);
        // The window is a whole-millisecond band: its logical component is zero.
        assert!(unpack(offset.uncertainty_window()).logical == 0);
    }
}
