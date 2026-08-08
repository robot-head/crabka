//! Timestamp-domain dead-version reclamation for sharded tables.
//!
//! The xid path reclaims dead versions through write-path chain pruning on
//! every engine kind, and through the engine-level vacuum on single-range
//! local engines. Both cover only xid tuples. Timestamp-stamped versions had
//! no reclamation at all. They accumulated forever, and every update of a
//! hot row paid O(total updates) to rescan its chain.
//!
//! [`TsVersionGc`] closes that gap with write-path opportunistic pruning.
//! When a timestamp transaction resolves a row to a committed or deleted
//! version, the same commit batch deletes the row's versions that are dead
//! below the reclaim floor. See
//! [`crabka_pgmvcc::gc::ts_dead_version_indices`]. The deletes ride the
//! ordinary [`crate::commit::Committer`] batch, so they replicate through the
//! WAL and replay deterministically. Recovery and followers apply exactly the
//! leader's reclamation.
//!
//! The reclaim floor is the timestamp-domain sibling of the xid path's
//! [`crabka_pgmvcc::gc::GcHorizon`] snapshot pins. It reuses that machinery
//! directly, as a second `GcHorizon` instance whose pinned values are read
//! timestamps:
//!
//! - every served timestamp read pins its `read_ts` for the duration of the
//!   scan, so the floor can never pass a read in progress on this engine;
//! - the floor candidate is the range's published closed timestamp, which is
//!   the watermark every served read has reconciled against, so reclamation
//!   only ever runs below timestamps the range's readers have moved past;
//! - the code publishes the floor durably under [`TS_GC_FLOOR_KEY`], in the
//!   same batch as the deletes. Every read pin and prewrite admission first
//!   folds the durable value in and refuses timestamps below it. A read or
//!   write that lost the race was allocated long ago and arrives after
//!   reclamation passed it. It fails with a retryable serialization error and
//!   does not silently observe pruned history.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Instant,
};

use crabka_pgkv::{Kv, WriteOp};
use crabka_units::{
    Time,
    convert::{StdDurationExt as _, TimeExt as _},
    secs,
};

use crate::{error::ExecError, local_sequence::LocalSequence, timestamp_txn::TimestampWrite};

/// Durable per-range reclaim floor. Reads and prewrites below this timestamp
/// may observe pruned history and must be refused. The code writes it
/// atomically with the prune deletes it covers.
pub(crate) const TS_GC_FLOOR_KEY: &[u8] = b"\0\0\0\0meta/ts_gc_floor";

/// Most dead versions one statement reclaims per written row. This bounds the
/// write path's opportunistic reclamation work per statement. A longer
/// backlog amortizes over later statements.
pub(crate) const TS_PRUNE_ROW_VERSION_CAP: usize = 64;

/// Wall-clock lag applied to the reclaim-floor candidate. Reclamation only
/// runs below the closed timestamp as it stood at least this long ago. The
/// engine allocates read timestamps near the present, so a read must be
/// delayed by more than this lag, and be superseded in the meantime, before
/// it can be refused. Cross-gateway clock skew and RPC latency sit far below
/// the lag. The trade-off is bounded retained garbage: at most
/// `lag x update rate` dead versions per hot row, instead of the unbounded
/// chains of no reclamation at all.
pub(crate) const TS_PRUNE_FLOOR_LAG: Time = secs(5);

/// Minimum interval between reclamation-telemetry log lines
/// ([`TsVersionGc::log_engagement`]).
const TS_GC_LOG_EVERY: Time = secs(1);

/// `lag` in whole milliseconds, the representation `floor_lag_millis` stores.
/// A negative lag is meaningless and clamps to zero. A lag beyond `i64::MAX`
/// milliseconds saturates there, which is already longer than any run.
fn floor_lag_millis(lag: Time) -> u64 {
    u64::try_from(lag.millis_i64()).unwrap_or(0)
}

/// Reclamation telemetry accumulated between rate-limited log emissions.
#[derive(Default)]
struct EngagementLog {
    /// When the previous line was emitted. `None` before the first.
    last_emitted: Option<Instant>,
    /// Commit batches that ran pruning since the previous line.
    batches: u64,
    /// Dead versions deleted since the previous line.
    pruned: u64,
}

/// Shared timestamp-version GC state for one engine, which is one range. It
/// holds the timestamp-domain pin registry and the closed-timestamp floor
/// source. It is shared across `clone_handle` handles and sessions like the
/// local sequence.
pub struct TsVersionGc {
    /// Timestamp-domain pin registry and reclaim floor. Values pinned here
    /// are read timestamps, not xids.
    floor: Arc<crabka_pgmvcc::gc::GcHorizon>,
    /// The range's local sequence. Its published closed timestamp is the
    /// reclaim-floor candidate.
    local_sequence: Arc<LocalSequence>,
    /// Closed-timestamp samples `(taken, closed_ts)` that back the lagged
    /// floor candidate, [`TS_PRUNE_FLOOR_LAG`]. The newest sample is at the
    /// back.
    closed_samples: Mutex<VecDeque<(Instant, u64)>>,
    /// The active floor lag. Default: [`TS_PRUNE_FLOOR_LAG`]. Tests shrink it
    /// to observe reclamation without a wait for the production lag. It is
    /// held as raw milliseconds because an atomic cannot carry a quantity.
    /// `floor_lag` restores the dimension.
    floor_lag_millis: std::sync::atomic::AtomicU64,
    prune_row_version_cap: usize,
    /// Rate-limited reclamation telemetry (see [`Self::log_engagement`]).
    engagement: Mutex<EngagementLog>,
}

impl TsVersionGc {
    /// GC state seeded with an empty pin registry and a zero floor. Every
    /// admission check folds the durable floor in.
    #[must_use]
    pub fn new(local_sequence: Arc<LocalSequence>) -> Self {
        Self::with_policy(local_sequence, TS_PRUNE_ROW_VERSION_CAP, TS_PRUNE_FLOOR_LAG)
    }

    #[must_use]
    pub fn with_policy(
        local_sequence: Arc<LocalSequence>,
        prune_row_version_cap: usize,
        floor_lag: Time,
    ) -> Self {
        Self {
            floor: Arc::new(crabka_pgmvcc::gc::GcHorizon::new()),
            local_sequence,
            closed_samples: Mutex::new(VecDeque::new()),
            floor_lag_millis: std::sync::atomic::AtomicU64::new(floor_lag_millis(floor_lag)),
            prune_row_version_cap,
            engagement: Mutex::new(EngagementLog::default()),
        }
    }

    /// Override the reclaim-floor lag. This is a testing seam. A shorter lag
    /// lets tests observe reclamation without a wait for the production window.
    pub fn set_floor_lag(&self, lag: Time) {
        self.floor_lag_millis
            .store(floor_lag_millis(lag), std::sync::atomic::Ordering::SeqCst);
    }

    /// The active reclaim-floor lag, re-dimensioned from the atomic's raw
    /// milliseconds.
    fn floor_lag(&self) -> Time {
        let millis = self
            .floor_lag_millis
            .load(std::sync::atomic::Ordering::SeqCst);
        Time::from_millis(i64::try_from(millis).unwrap_or(i64::MAX))
    }

    /// Record a locally allocated commit timestamp as closed, on engines
    /// where nothing else publishes closure.
    ///
    /// The single-store autocommit path has no range gateway that reconciles
    /// reads. The fold of the commit into the local sequence comes first, and
    /// that upholds the closed-timestamp contract. The sequence reserves
    /// strictly above the commit before it publishes the watermark, so no
    /// later single-shard allocation can land at or below it.
    pub(crate) fn observe_committed(&self, commit_ts: crate::timestamp_txn::CommitTimestamp) {
        self.local_sequence.observe(commit_ts.get());
        // Only fails at timestamp-domain exhaustion; the watermark is
        // monotone, so a stale publish is a no-op.
        let _ = self
            .local_sequence
            .publish_closed_timestamp(commit_ts.get());
    }

    /// The lagged reclaim-floor candidate, which is the newest
    /// closed-timestamp sample at least [`TS_PRUNE_FLOOR_LAG`] old. It is zero
    /// until one exists. This method takes a sample on each call, so under
    /// steady write load the candidate trails the closed timestamp by the lag.
    fn lagged_closed_timestamp(&self) -> u64 {
        let lag = self.floor_lag();
        let now = Instant::now();
        let mut samples = self.closed_samples.lock().expect("closed samples");
        samples.push_back((now, self.local_sequence.closed_timestamp()));
        // Keep exactly one sample older than the lag window: the newest such
        // sample is the candidate, and anything older is superseded by it.
        while samples.len() >= 2 && now.duration_since(samples[1].0).as_time() >= lag {
            samples.pop_front();
        }
        let (taken, closed) = samples[0];
        if now.duration_since(taken).as_time() >= lag {
            closed
        } else {
            0
        }
    }

    /// Read the durable reclaim floor from `kv`, fold it into the in-memory
    /// floor, and return the folded value.
    ///
    /// A fresh read on every admission keeps followers and newly promoted
    /// leaders correct without cache invalidation. The durable key rides the
    /// same replicated batch as the deletes it covers.
    fn observed_floor(&self, kv: &dyn Kv) -> Result<u64, ExecError> {
        self.floor.observe_reclaim_floor(durable_reclaim_floor(kv)?);
        Ok(self.floor.reclaim_floor())
    }

    /// Pin `read_ts` for the duration of a timestamp read served against `kv`.
    ///
    /// This method refuses reads below the reclaim floor, because their
    /// history may be pruned. Hold the returned pin across the scan. While the
    /// pin lives, no reclamation on this engine passes `read_ts`.
    ///
    /// # Errors
    ///
    /// Returns [`ExecError::SerializationFailure`] when `read_ts` is below
    /// the reclaim floor. Retry with a fresh read timestamp. Returns a store
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

    /// Build the opportunistic prune ops that ride a commit batch which
    /// resolves `writes`.
    ///
    /// For each written row, this method deletes the timestamp versions that
    /// are dead below the reclaim floor, and it publishes the floor durably in
    /// the same batch. [`TS_PRUNE_ROW_VERSION_CAP`] bounds the work per row.
    ///
    /// The floor is the closed timestamp bounded by every active read pin. See
    /// [`crabka_pgmvcc::gc::GcHorizon::raise_reclaim_floor`]. No read served
    /// on this engine can miss a pruned version, and no read admitted later
    /// against the published floor can miss one either. The decisions read the
    /// durable pre-batch state. The batch's own resolution target is still an
    /// intent there, and intents are never dead, so a batch never deletes a
    /// key it also rewrites.
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
            for index in crabka_pgmvcc::gc::ts_dead_version_indices(
                &states,
                floor,
                self.prune_row_version_cap,
            ) {
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

    /// Emit rate-limited reclamation telemetry.
    ///
    /// The method emits at most one debug line per [`TS_GC_LOG_EVERY`]. The
    /// line carries the current floor and the batch and deletion counts
    /// accumulated since the previous line. A live node that logs a zero floor
    /// with a growing `batches` count shows that pruning is wired but closure
    /// does not advance. A non-zero `pruned` count confirms end-to-end
    /// engagement.
    fn log_engagement(&self, floor: u64, pruned: u64) {
        let mut log = self.engagement.lock().expect("engagement log");
        log.batches += 1;
        log.pruned += pruned;
        let now = Instant::now();
        let due = log
            .last_emitted
            .is_none_or(|last| now.duration_since(last).as_time() >= TS_GC_LOG_EVERY);
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
/// published yet. Prewrite admission reads this directly, so every prewrite is
/// fenced against reclaimed history. This includes prewrites built by bare
/// participants without in-memory GC state.
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
