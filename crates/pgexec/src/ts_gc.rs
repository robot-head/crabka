//! Timestamp-domain dead-version reclamation for sharded tables.
//!
//! The xid path reclaims dead versions through write-path chain pruning (on
//! every engine kind) and the engine-level vacuum (single-range local
//! engines), but both cover only xid tuples. Timestamp-stamped versions had
//! no reclamation at all: they accumulated forever, and every update of a
//! hot row paid O(total updates) to rescan its chain.
//!
//! [`TsVersionGc`] closes that gap with write-path opportunistic pruning:
//! when a timestamp transaction resolves a row to a committed or deleted
//! version, the same commit batch deletes the row's versions that are dead
//! below the reclaim floor ([`crabka_pgmvcc::gc::ts_dead_version_indices`]).
//! Because the deletes ride the ordinary [`crate::commit::Committer`] batch,
//! they replicate through the WAL and replay deterministically — recovery and
//! followers apply exactly the leader's reclamation.
//!
//! The reclaim floor is the timestamp-domain sibling of the xid path's
//! [`crabka_pgmvcc::gc::GcHorizon`] snapshot pins, reusing that machinery
//! directly (a second `GcHorizon` instance whose pinned values are read
//! timestamps):
//!
//! - every served timestamp read pins its `read_ts` for the duration of the
//!   scan, so the floor can never pass a read in progress on this engine;
//! - the floor candidate is the range's published closed timestamp — the
//!   watermark every served read has reconciled against — so reclamation
//!   only ever runs below timestamps the range's readers have moved past;
//! - the floor is published durably (in the same batch as the deletes) under
//!   [`TS_GC_FLOOR_KEY`], and every read pin and prewrite admission first
//!   folds the durable value in and refuses timestamps below it. A read or
//!   write that lost the race (allocated long ago, arriving after
//!   reclamation passed it) fails with a retryable serialization error
//!   instead of silently observing pruned history.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crabka_pgkv::{Kv, WriteOp};

use crate::{error::ExecError, local_sequence::LocalSequence, timestamp_txn::TimestampWrite};

/// Durable per-range reclaim floor: reads and prewrites below this timestamp
/// may observe pruned history and must be refused. Written atomically with
/// the prune deletes it covers.
pub(crate) const TS_GC_FLOOR_KEY: &[u8] = b"\0\0\0\0meta/ts_gc_floor";

/// Most dead versions one statement reclaims per written row, bounding the
/// write path's opportunistic reclamation work per statement; a longer
/// backlog amortizes over subsequent statements.
pub(crate) const TS_PRUNE_ROW_VERSION_CAP: usize = 64;

/// Wall-clock lag applied to the reclaim-floor candidate: reclamation only
/// runs below the closed timestamp as it stood at least this long ago. Read
/// timestamps are allocated near the present, so a read would have to be
/// delayed by more than this lag (plus be superseded meanwhile) before it can
/// be refused — cross-gateway clock skew and RPC latency sit far below it.
/// The trade-off is bounded retained garbage: at most `lag x update rate`
/// dead versions per hot row, instead of the unbounded chains of no
/// reclamation at all.
const TS_PRUNE_FLOOR_LAG: Duration = Duration::from_secs(5);

/// Minimum interval between reclamation-telemetry log lines
/// ([`TsVersionGc::log_engagement`]).
const TS_GC_LOG_EVERY: Duration = Duration::from_secs(1);

/// Reclamation telemetry accumulated between rate-limited log emissions.
#[derive(Default)]
struct EngagementLog {
    /// When the previous line was emitted; `None` before the first.
    last_emitted: Option<Instant>,
    /// Commit batches that ran pruning since the previous line.
    batches: u64,
    /// Dead versions deleted since the previous line.
    pruned: u64,
}

/// Shared timestamp-version GC state for one engine (one range): the
/// timestamp-domain pin registry plus the closed-timestamp floor source.
/// Shared across `clone_handle` handles and sessions like the local sequence.
pub struct TsVersionGc {
    /// Timestamp-domain pin registry and reclaim floor. Values pinned here
    /// are read timestamps, not xids.
    floor: Arc<crabka_pgmvcc::gc::GcHorizon>,
    /// The range's local sequence; its published closed timestamp is the
    /// reclaim-floor candidate.
    local_sequence: Arc<LocalSequence>,
    /// Closed-timestamp samples `(taken, closed_ts)` backing the lagged floor
    /// candidate ([`TS_PRUNE_FLOOR_LAG`]), newest at the back.
    closed_samples: Mutex<VecDeque<(Instant, u64)>>,
    /// The active floor lag in milliseconds (defaults to
    /// [`TS_PRUNE_FLOOR_LAG`]); tests shrink it to observe reclamation
    /// without waiting out the production lag.
    floor_lag_millis: std::sync::atomic::AtomicU64,
    /// Rate-limited reclamation telemetry (see [`Self::log_engagement`]).
    engagement: Mutex<EngagementLog>,
}

impl TsVersionGc {
    /// GC state seeded with an empty pin registry and a zero floor; the
    /// durable floor is folded in on every admission check.
    #[must_use]
    pub fn new(local_sequence: Arc<LocalSequence>) -> Self {
        Self {
            floor: Arc::new(crabka_pgmvcc::gc::GcHorizon::new()),
            local_sequence,
            closed_samples: Mutex::new(VecDeque::new()),
            floor_lag_millis: std::sync::atomic::AtomicU64::new(
                u64::try_from(TS_PRUNE_FLOOR_LAG.as_millis()).unwrap_or(u64::MAX),
            ),
            engagement: Mutex::new(EngagementLog::default()),
        }
    }

    /// Override the reclaim-floor lag. A testing seam: shrinking the lag lets
    /// tests observe reclamation without waiting out the production window.
    pub fn set_floor_lag(&self, lag: Duration) {
        self.floor_lag_millis.store(
            u64::try_from(lag.as_millis()).unwrap_or(u64::MAX),
            std::sync::atomic::Ordering::SeqCst,
        );
    }

    /// Record a locally allocated commit timestamp as closed, on engines
    /// where nothing else publishes closure (the single-store autocommit path
    /// has no range gateway reconciling reads). Folding the commit into the
    /// local sequence first upholds the closed-timestamp contract: the
    /// sequence reserves strictly above the commit before the watermark is
    /// published, so no later single-shard allocation can land at or below
    /// it.
    pub(crate) fn observe_committed(&self, commit_ts: crate::timestamp_txn::CommitTimestamp) {
        self.local_sequence.observe(commit_ts.get());
        // Only fails at timestamp-domain exhaustion; the watermark is
        // monotone, so a stale publish is a no-op.
        let _ = self
            .local_sequence
            .publish_closed_timestamp(commit_ts.get());
    }

    /// The lagged reclaim-floor candidate: the newest closed-timestamp sample
    /// at least [`TS_PRUNE_FLOOR_LAG`] old (zero until one exists). Samples
    /// are taken on each call, so the candidate trails the closed timestamp
    /// by the lag under steady write load.
    fn lagged_closed_timestamp(&self) -> u64 {
        let lag = Duration::from_millis(
            self.floor_lag_millis
                .load(std::sync::atomic::Ordering::SeqCst),
        );
        let now = Instant::now();
        let mut samples = self.closed_samples.lock().expect("closed samples");
        samples.push_back((now, self.local_sequence.closed_timestamp()));
        // Keep exactly one sample older than the lag window: the newest such
        // sample is the candidate, and anything older is superseded by it.
        while samples.len() >= 2 && now.duration_since(samples[1].0) >= lag {
            samples.pop_front();
        }
        let (taken, closed) = samples[0];
        if now.duration_since(taken) >= lag {
            closed
        } else {
            0
        }
    }

    /// Read the durable reclaim floor from `kv` and fold it into the
    /// in-memory floor, returning the folded value. Reading it fresh on
    /// every admission keeps followers and newly promoted leaders correct
    /// without cache invalidation: the durable key rides the same replicated
    /// batch as the deletes it covers.
    fn observed_floor(&self, kv: &dyn Kv) -> Result<u64, ExecError> {
        self.floor.observe_reclaim_floor(durable_reclaim_floor(kv)?);
        Ok(self.floor.reclaim_floor())
    }

    /// Pin `read_ts` for the duration of a timestamp read served against
    /// `kv`, refusing reads below the reclaim floor (their history may be
    /// pruned). Hold the returned pin across the scan: while it lives, no
    /// reclamation on this engine passes `read_ts`.
    ///
    /// # Errors
    ///
    /// Returns [`ExecError::SerializationFailure`] when `read_ts` is below
    /// the reclaim floor (retry with a fresh read timestamp), or a store
    /// error when the durable floor cannot be read.
    pub fn pin_read(
        &self,
        kv: &dyn Kv,
        read_ts: crate::timestamp_txn::ReadTimestamp,
    ) -> Result<crabka_pgmvcc::gc::SnapshotPin, ExecError> {
        self.observed_floor(kv)?;
        self.floor
            .pin_above(read_ts.get())
            .map_err(|_| ExecError::SerializationFailure)
    }

    /// Build the opportunistic prune ops that ride a commit batch resolving
    /// `writes`: for each written row, delete the timestamp versions that
    /// are dead below the reclaim floor, and publish the floor durably in
    /// the same batch. Work per row is bounded by
    /// [`TS_PRUNE_ROW_VERSION_CAP`].
    ///
    /// The floor is the closed timestamp bounded by every active read pin
    /// (see [`crabka_pgmvcc::gc::GcHorizon::raise_reclaim_floor`]), so no
    /// read served on this engine — nor any read admitted later against the
    /// published floor — can miss a pruned version. Decisions read the
    /// durable pre-batch state: the batch's own resolution target is still
    /// an intent there and intents are never dead, so a batch never deletes
    /// a key it also rewrites.
    ///
    /// # Errors
    ///
    /// Returns a store error when the chain scan or durable-floor read fails.
    pub(crate) fn prune_batch_ops(
        &self,
        kv: &dyn Kv,
        writes: &[TimestampWrite],
    ) -> Result<Vec<WriteOp>, ExecError> {
        self.observed_floor(kv)?;
        let floor = self
            .floor
            .raise_reclaim_floor(self.lagged_closed_timestamp());
        let mut ops = Vec::new();
        for write in writes {
            let prefix = match write.bucket {
                Some(bucket) => crabka_pgkv::key::hash_row_key(write.table_id, bucket, write.rowid),
                None => crabka_pgkv::key::row_key(write.table_id, write.rowid),
            };
            let mut keys = Vec::new();
            let mut states = Vec::new();
            for (key, value) in kv.scan_prefix(&prefix)? {
                // Only decodable timestamp tuples participate; anything else
                // under the prefix is left untouched.
                if let Ok(version) = crabka_pgmvcc::version::decode_ts_tuple(&value) {
                    keys.push(key);
                    states.push(version.state);
                }
            }
            for index in
                crabka_pgmvcc::gc::ts_dead_version_indices(&states, floor, TS_PRUNE_ROW_VERSION_CAP)
            {
                ops.push(WriteOp::Delete {
                    key: keys[index].clone(),
                });
            }
        }
        self.log_engagement(floor, u64::try_from(ops.len()).unwrap_or(u64::MAX));
        if !ops.is_empty() {
            // The published floor must cover every pruned version's superseding
            // commit; `floor` does (the rule only kills versions superseded at
            // or below it), and publishing rides the same atomic batch as the
            // deletes so no applied state has the deletes without the floor.
            ops.push(WriteOp::Put {
                key: TS_GC_FLOOR_KEY.to_vec(),
                value: floor.to_be_bytes().to_vec(),
            });
        }
        Ok(ops)
    }

    /// Emit rate-limited reclamation telemetry: at most one debug line per
    /// [`TS_GC_LOG_EVERY`], carrying the current floor and the batch/deletion
    /// counts accumulated since the previous line. A live node logging a zero
    /// floor with growing `batches` shows pruning is wired but closure is not
    /// advancing; a non-zero `pruned` confirms end-to-end engagement.
    fn log_engagement(&self, floor: u64, pruned: u64) {
        let mut log = self.engagement.lock().expect("engagement log");
        log.batches += 1;
        log.pruned += pruned;
        let now = Instant::now();
        let due = log
            .last_emitted
            .is_none_or(|last| now.duration_since(last) >= TS_GC_LOG_EVERY);
        if !due {
            return;
        }
        tracing::debug!(
            floor,
            pruned = log.pruned,
            batches = log.batches,
            "ts_version_gc_engagement"
        );
        log.last_emitted = Some(now);
        log.batches = 0;
        log.pruned = 0;
    }
}

/// The durable reclaim floor published on `kv`, or zero when none has been
/// published yet. Prewrite admission reads this directly so every prewrite —
/// including ones built by bare participants without in-memory GC state — is
/// fenced against reclaimed history.
///
/// # Errors
///
/// Returns a store error when the floor key cannot be read.
pub(crate) fn durable_reclaim_floor(kv: &dyn Kv) -> Result<u64, crabka_pgkv::KvError> {
    Ok(kv
        .get(TS_GC_FLOOR_KEY)?
        .and_then(|bytes| <[u8; 8]>::try_from(bytes).ok())
        .map_or(0, u64::from_be_bytes))
}
