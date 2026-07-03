//! A single partition's runtime handle. Owned by the partition registry
//! inside `Broker`. The handle gives any task:
//!
//! - read access to the partition's [`Log`] via `Arc<Mutex<Log>>`
//! - write access via a `mpsc::Sender<WriterMessage>` (a single writer task
//!   drains the channel; see `partition_writer.rs`)
//! - a [`Notify`] that fires after every successful append, used by
//!   long-poll Fetch to wake when new data arrives.

// Fields (`log`, `writer_tx`, `append_notify`) are consumed by the Produce
// + Fetch handlers landing in Tasks 15-16; keep this allow until then.
#![allow(dead_code)]

use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicI32, AtomicU64, Ordering},
    },
};

use arc_swap::ArcSwap;
use crabka_log::{AbortedTxn, Log, Offset, ReadOutput, VerbatimBatch};
use crabka_protocol::records::RecordBatch;
use tokio::{
    sync::{Notify, mpsc, oneshot},
    task::JoinHandle,
};

// `std::sync::Mutex` is kept for `log` (sync hot-path callers);
// `replica_state` uses `tokio::sync::Mutex` to avoid blocking worker threads.
use crate::error::BrokerError;
use crate::replica_state::ReplicaState;

/// Absolute record offset within a partition's log (base offset, log end
/// offset, high watermark, truncation points, …). Alias only — clarifies
/// which `i64`s in signatures are offsets vs. timestamps or counts.
pub type LogOffset = i64;

/// The records to append for a single produce job — either the producer's
/// verbatim wire bytes (zero-copy passthrough fast path) or a fully owned,
/// decoded [`RecordBatch`] (the fallback path used when passthrough is
/// unsafe: recompression, legacy up-conversion, control batches, etc.).
///
/// The `Owned` arm is a complete fallback, so the whole verbatim
/// passthrough feature is trivially revertible — "always construct
/// `Owned`" restores the previous behavior.
#[derive(Debug)]
pub enum ProduceData {
    /// Append the producer's exact wire bytes, patching only `base_offset`
    /// + `partition_leader_epoch`. No decode/re-encode/recompress/CRC.
    Verbatim(VerbatimBatch),
    /// Decode + re-encode the owned batch on append (the original path).
    /// The writer mutates `base_offset` before append.
    Owned(RecordBatch),
}

/// Produce-path message sent from the Produce handler to the partition's
/// writer task. The writer assigns `base_offset` (overwriting whatever the
/// handler put there) and replies with the assigned value.
#[derive(Debug)]
pub struct ProduceJob {
    /// The records to append (verbatim passthrough or owned fallback).
    pub data: ProduceData,
    /// Oneshot for the writer to report success (base offset assigned)
    /// or failure back to the handler.
    pub ack: oneshot::Sender<Result<LogOffset, BrokerError>>,
}

/// All message kinds the partition's writer task accepts.
///
/// The writer task is single-consumer over a single `mpsc::Sender`; using
/// an enum here keeps replication appends ordered with produce appends.
#[derive(Debug)]
pub enum WriterMessage {
    /// Append a batch, assigning `base_offset` from the log. Used by the
    /// `Produce` handler.
    Produce(ProduceJob),
    /// Append a batch at the caller-supplied offset (already assigned by
    /// the partition's leader). Used by the per-(topic, partition)
    /// replicator on a follower broker.
    Replicate {
        batch: RecordBatch,
        ack: oneshot::Sender<Result<(), BrokerError>>,
    },
    /// Truncate the log so no records at offset `>= offset` remain. Used
    /// by the replicator's `OFFSET_OUT_OF_RANGE` recovery path.
    Truncate {
        offset: LogOffset,
        ack: oneshot::Sender<Result<(), BrokerError>>,
    },
    /// Drop every segment and recreate the active segment at `new_base`.
    /// Used by the replicator's `OFFSET_OUT_OF_RANGE` recovery path when
    /// the follower has fallen behind the leader's `log_start` — the
    /// follower must move its own `log_start` *forward* past records it
    /// never saw, which `Truncate` can't do.
    ResetTo {
        new_base: LogOffset,
        ack: oneshot::Sender<Result<(), BrokerError>>,
    },
    /// Atomically swap the partition's `LogConfig`. The writer task
    /// serializes this with appends so no in-flight `RecordBatch` sees a
    /// half-applied config. Sent by
    /// `ReplicatorSupervisor::reconcile` whenever a `V1TopicConfig`
    /// record changes the topic's overrides.
    SetLogConfig {
        config: crabka_log::LogConfig,
        ack: tokio::sync::oneshot::Sender<()>,
    },
    /// Run one compaction pass. The writer actor serializes this with
    /// appends to preserve the single-writer invariant on `Log`.
    Compact {
        ack: tokio::sync::oneshot::Sender<Result<(), BrokerError>>,
    },
    /// Trim from the start of the log: drop sealed segments whose last
    /// offset is `< new_start`, advance `log_start_offset` if `new_start`
    /// falls inside the active segment. Returns the resulting
    /// `log_start_offset` (which may be less than `new_start` when
    /// `new_start` falls between segment boundaries — Kafka semantics).
    /// Used by the `DeleteRecords` handler.
    TrimToOffset {
        new_start: LogOffset,
        ack: tokio::sync::oneshot::Sender<Result<LogOffset, BrokerError>>,
    },
    /// Test-only: shift the in-memory `log_start_offset` without
    /// physically truncating segments. Simulates retention-driven
    /// truncation for the `out_of_range_truncates_and_recovers`
    /// replication integration test.
    #[cfg(any(test, feature = "test-helpers"))]
    TestSetLogStart {
        new_start: LogOffset,
        ack: oneshot::Sender<Result<(), BrokerError>>,
    },
    /// Atomically swap the partition's `Log` to a future log that has
    /// fully caught up. Sent by the KIP-113 move task in
    /// `future_log.rs` once `future_log.LEO == current_log.LEO`. The
    /// writer re-checks the invariant under its own lock, then:
    /// 1. drops the current `Log`,
    /// 2. `fs::rename`s `future_path` → `target_partition_path`,
    /// 3. removes the source partition directory,
    /// 4. re-opens `Log` at `target_partition_path` and stores it,
    /// 5. updates `Partition.log_dir` to `target_log_dir`.
    ///
    /// If the future log fell behind during the request hop, returns
    /// `Ok(SwapOutcome::NotCaughtUp)` so the caller can loop once more.
    SwapFutureLog {
        target_log_dir: PathBuf,
        future_log: Arc<Mutex<Log>>,
        future_path: PathBuf,
        target_partition_path: PathBuf,
        ack: oneshot::Sender<Result<SwapOutcome, BrokerError>>,
    },
}

/// Result of a [`WriterMessage::SwapFutureLog`] handling cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwapOutcome {
    /// The swap succeeded; the partition is now serving from the
    /// target log dir and the source dir has been removed.
    Swapped,
    /// The future log was behind the current log when the writer
    /// re-checked. The caller should resume replication and retry.
    NotCaughtUp,
}

/// Returned by `await_hw_at_least` when the deadline elapses before
/// the High Watermark reaches the target offset.
#[derive(Debug)]
pub struct HwTimeout;

/// Runtime handle for a single partition.
///
/// Cheap to clone — `log`, `writer_tx`, `append_notify` are all `Arc`-ish
/// and the writer handle isn't cloned (`Arc<JoinHandle<()>>` wraps it).
#[derive(Clone)]
// `partition_id` mirrors Kafka's wire naming and is the conventional term
// used throughout the broker; renaming to `id` would shadow `Partition`'s
// own identity at every call site.
#[allow(clippy::struct_field_names)]
pub struct Partition {
    pub topic: String,
    pub partition_id: i32,
    /// Parent `log.dir` currently owning the partition (the parent of
    /// `log.lock().dir()` — i.e. the configured directory, not the
    /// `<topic>-<partition>` subdirectory). Updated by
    /// [`WriterMessage::SwapFutureLog`] as the last step of a KIP-113
    /// move. `ArcSwap` so readers (e.g. `DescribeLogDirs`,
    /// `AlterReplicaLogDirs` validation) see the swap atomically
    /// without taking the `log` mutex.
    pub log_dir: Arc<ArcSwap<PathBuf>>,
    pub log: Arc<Mutex<Log>>,
    pub writer_tx: mpsc::Sender<WriterMessage>,
    pub append_notify: Arc<Notify>,
    pub(crate) replica_state: Arc<tokio::sync::Mutex<ReplicaState>>,
    pub hw_advance_notify: Arc<Notify>,
    /// Current leader's `NodeId` from the metadata image. Atomic for
    /// lock-free reads in the Produce/Fetch hot paths.
    pub current_leader: Arc<AtomicU64>,
    /// Current `leader_epoch` from the metadata image. Stamped on every
    /// appended batch; validated on every follower Fetch.
    pub current_leader_epoch: Arc<AtomicI32>,
    /// Held so the writer task is reaped when every `Partition` handle is
    /// dropped. Not accessed after construction.
    #[allow(clippy::pub_underscore_fields)]
    pub _writer_handle: Arc<JoinHandle<()>>,
}

impl Partition {
    /// Next offset the underlying [`Log`] will assign. Cheap: takes the
    /// `Arc<Mutex<Log>>` briefly. Replicators call this before each Fetch
    /// to compute `fetch_offset`.
    ///
    /// Returns 0 if the log mutex is poisoned (i.e. the writer task
    /// panicked). The caller treats that as "not making progress" and the
    /// writer-died path eventually surfaces a clearer error.
    #[must_use]
    pub fn log_end_offset(&self) -> LogOffset {
        match self.log.lock() {
            Ok(g) => g.log_end_offset().0,
            Err(_) => 0,
        }
    }

    /// Last Stable Offset: the highest offset at or before which all records
    /// in all in-flight transactions have been resolved (committed or aborted).
    /// Cheap: takes the `Arc<Mutex<Log>>` briefly.
    ///
    /// Returns 0 if the log mutex is poisoned (i.e. the writer task
    /// panicked). The caller treats that as "not making progress" and the
    /// writer-died path eventually surfaces a clearer error.
    #[must_use]
    pub fn lso(&self) -> LogOffset {
        match self.log.lock() {
            Ok(g) => g.lso().0,
            Err(_) => 0,
        }
    }

    /// Push `overrides` (already-validated; see `config_keys`) through the
    /// writer actor so the partition's `Log` picks up the new
    /// `retention.ms` / `retention.bytes` / `segment.bytes` on the next
    /// retention/roll tick. Idempotent: pushing the same map twice is a
    /// cheap noop. Called by `ReplicatorSupervisor::reconcile` every time
    /// the metadata image changes.
    ///
    /// # Errors
    ///
    /// Returns `BrokerError::Replication` if the writer is dead or the
    /// ack is dropped.
    pub(crate) async fn apply_log_config_overrides(
        &self,
        overrides: &std::collections::BTreeMap<String, String>,
    ) -> Result<(), BrokerError> {
        let merged =
            crate::config_keys::apply_to_log_config(overrides, &crabka_log::LogConfig::default());
        let (ack_tx, ack_rx) = oneshot::channel();
        self.writer_tx
            .send(WriterMessage::SetLogConfig {
                config: merged,
                ack: ack_tx,
            })
            .await
            .map_err(|_| BrokerError::Replication("partition writer dead".into()))?;
        ack_rx
            .await
            .map_err(|_| BrokerError::Replication("ack dropped".into()))?;
        Ok(())
    }

    /// Append a leader-assigned batch to the local log, preserving its
    /// `base_offset`. Used by the per-partition replicator on a follower
    /// broker. Sends the batch through the writer task so it stays
    /// ordered with produce appends (which, on a follower, will be
    /// rejected by the produce handler anyway, but the channel ordering
    /// is still part of the invariant).
    pub async fn replicate_batch(&self, batch: RecordBatch) -> Result<(), BrokerError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.writer_tx
            .send(WriterMessage::Replicate { batch, ack: ack_tx })
            .await
            .map_err(|_| BrokerError::Replication("partition writer dead".into()))?;
        ack_rx
            .await
            .map_err(|_| BrokerError::Replication("ack dropped".into()))?
    }

    /// Truncate the log to `offset`, dropping all records at offsets
    /// `>= offset`. Used by the replicator's `OFFSET_OUT_OF_RANGE`
    /// recovery path and the KIP-320 in-band `diverging_epoch` truncation
    /// path (which passes the leader's epoch boundary, not just 0).
    pub async fn truncate_to(&self, offset: LogOffset) -> Result<(), BrokerError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.writer_tx
            .send(WriterMessage::Truncate {
                offset,
                ack: ack_tx,
            })
            .await
            .map_err(|_| BrokerError::Replication("partition writer dead".into()))?;
        ack_rx
            .await
            .map_err(|_| BrokerError::Replication("ack dropped".into()))?
    }

    /// Drop every segment and recreate the active segment at `new_base`.
    /// Goes through the writer task so it stays ordered with appends.
    pub async fn reset_to(&self, new_base: LogOffset) -> Result<(), BrokerError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.writer_tx
            .send(WriterMessage::ResetTo {
                new_base,
                ack: ack_tx,
            })
            .await
            .map_err(|_| BrokerError::Replication("partition writer dead".into()))?;
        ack_rx
            .await
            .map_err(|_| BrokerError::Replication("ack dropped".into()))?
    }

    /// Send a trim request through the writer actor. Returns the resulting
    /// `log_start_offset`. Used by the `DeleteRecords` handler.
    ///
    /// # Errors
    ///
    /// Returns `BrokerError` if the writer is dead, the ack is dropped,
    /// or the underlying `Log::trim_to_offset` fails (negative offset).
    pub async fn trim_to_offset(&self, new_start: LogOffset) -> Result<LogOffset, BrokerError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.writer_tx
            .send(WriterMessage::TrimToOffset {
                new_start,
                ack: ack_tx,
            })
            .await
            .map_err(|_| BrokerError::Replication("partition writer dead".into()))?;
        ack_rx
            .await
            .map_err(|_| BrokerError::Replication("ack dropped".into()))?
    }

    /// Send a `WriterMessage::Compact` to the partition's writer
    /// actor and await the ack. Used by the broker-wide [`Cleaner`]
    /// ticker.
    pub async fn compact_log(&self) -> Result<(), BrokerError> {
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        self.writer_tx
            .send(WriterMessage::Compact { ack: ack_tx })
            .await
            .map_err(|_| BrokerError::Replication("partition writer dead".into()))?;
        ack_rx
            .await
            .map_err(|_| BrokerError::Replication("compact ack dropped".into()))?
    }

    /// First absolute offset still present in the underlying [`Log`].
    /// Cheap: takes the `Arc<Mutex<Log>>` briefly.
    ///
    /// Returns 0 if the log mutex is poisoned (i.e. the writer task panicked).
    /// Used by `TxnCoordinator::recover` to seed the replay scan offset.
    #[must_use]
    pub(crate) fn log_start_offset(&self) -> LogOffset {
        match self.log.lock() {
            Ok(g) => g.log_start_offset().0,
            Err(_) => 0,
        }
    }

    /// Return aborted transactions from the active segment's `.txnindex`
    /// whose offset range overlaps `[start, end)`.
    ///
    /// Locks the `Arc<Mutex<Log>>` briefly. Returns an empty `Vec` if
    /// the mutex is poisoned.
    #[must_use]
    pub fn aborted_in_range(&self, start: LogOffset, end: LogOffset) -> Vec<AbortedTxn> {
        match self.log.lock() {
            Ok(g) => g.aborted_in_range(Offset(start), Offset(end)),
            Err(_) => Vec::new(),
        }
    }

    /// Read batches from the underlying [`Log`] starting at `offset`,
    /// returning up to `max_bytes` of data.
    ///
    /// Locks the `Arc<Mutex<Log>>` for the duration of the read. Used by
    /// `TxnCoordinator::recover` to replay `__transaction_state` records.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Log`] if the underlying [`Log::read`] fails
    /// (e.g. `offset < log_start_offset()`).
    pub(crate) fn read_log(
        &self,
        offset: LogOffset,
        max_bytes: usize,
    ) -> Result<ReadOutput, BrokerError> {
        self.log
            .lock()
            .map_err(|_| BrokerError::Txn("log mutex poisoned".into()))?
            .read(Offset(offset), max_bytes)
            .map_err(BrokerError::from)
    }

    /// Append `batch` to the local log at the next assigned offset, going
    /// through the partition's writer task so the append is ordered with
    /// all other produce appends. Returns the assigned `base_offset`.
    ///
    /// Used by `TxnCoordinator::put` to persist `__transaction_state` records.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Txn`] if the writer task is dead or the ack
    /// channel closes before the writer replies.
    pub(crate) async fn produce_batch(&self, batch: RecordBatch) -> Result<LogOffset, BrokerError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.writer_tx
            .send(WriterMessage::Produce(ProduceJob {
                data: ProduceData::Owned(batch),
                ack: ack_tx,
            }))
            .await
            .map_err(|_| BrokerError::Txn("partition writer dead".into()))?;
        ack_rx
            .await
            .map_err(|_| BrokerError::Txn("ack dropped".into()))?
    }

    /// Cached High Watermark. Awaits `replica_state` cooperatively so it
    /// doesn't block tokio worker threads.
    #[must_use]
    pub async fn high_watermark(&self) -> LogOffset {
        self.replica_state.lock().await.hw
    }

    /// KIP-392: record the high watermark the leader reported in a follower
    /// Fetch response, so consumer reads served from this follower are bounded
    /// correctly. Clamps to the local log end (never expose records we have not
    /// replicated yet) and only advances `hw` (HW is monotonic). Fires
    /// `hw_advance_notify` when it advances so a consumer parked at the old HW
    /// wakes.
    pub async fn set_follower_hw(&self, reported_hw: LogOffset) {
        let log_end = self.log_end_offset();
        let new_hw = reported_hw.min(log_end);
        let advanced = {
            let mut st = self.replica_state.lock().await;
            if new_hw > st.hw {
                st.hw = new_hw;
                true
            } else {
                false
            }
        };
        if advanced {
            self.hw_advance_notify.notify_waiters();
        }
    }

    /// Install (or reinstall) the ISR membership and seed non-leader
    /// follower entries to 0. Called by the replicator supervisor
    /// when this broker materializes a partition where it's the leader.
    /// Idempotent: re-installing the same `(isr, replicas, leader)`
    /// preserves existing follower progress.
    ///
    /// `isr` is the committed in-sync set; `replicas` is the full replica
    /// assignment. Follower-progress tracking is keyed on `replicas` so a
    /// replica catching up toward ISR re-admission keeps its progress
    /// across reconciles — see [`crate::replica_state::ReplicaState::install_isr`].
    ///
    /// Recomputes HW under the new ISR and fires `hw_advance_notify`
    /// if HW advanced — necessary because an `AlterPartition` shrink
    /// may have just dropped a lagging follower, and the surviving
    /// followers' LEOs (which were already being updated via fetch)
    /// can now satisfy a previously-blocked acks=-1 produce without
    /// waiting for the next fetch round.
    pub async fn install_isr(
        &self,
        isr: &[crabka_raft::NodeId],
        replicas: &[crabka_raft::NodeId],
        leader: crabka_raft::NodeId,
    ) {
        let leader_leo = self.log_end_offset();
        let mut st = self.replica_state.lock().await;
        let prev_hw = st.hw;
        st.install_isr(isr, replicas, leader, std::time::Instant::now());
        let new_hw = st.recompute_hw_for_leader_append(leader_leo);
        drop(st);
        if new_hw > prev_hw {
            self.hw_advance_notify.notify_waiters();
        }
    }

    /// Apply a leader change observed via the metadata image. Updates
    /// the cached `current_leader` + `current_leader_epoch`. If the
    /// leader or epoch actually changed, clears per-follower stats
    /// (stale under the new leader's view). On idempotent re-installs
    /// (same leader + epoch) per-follower progress is preserved — the
    /// supervisor calls this on every reconcile and unconditional
    /// clearing would reset follower LEOs each time, dropping HW back
    /// to 0 and blocking acks=-1 producers until followers re-fetch.
    /// Fires `hw_advance_notify` so waiting Produce gates can re-check.
    pub async fn install_leader_change(&self, new_leader: u64, new_epoch: i32) {
        let prev_leader = self.current_leader.swap(new_leader, Ordering::AcqRel);
        let prev_epoch = self.current_leader_epoch.swap(new_epoch, Ordering::AcqRel);
        let leader_changed = prev_leader != new_leader || prev_epoch != new_epoch;
        let mut st = self.replica_state.lock().await;
        if leader_changed {
            // Diagnostic: every broker hosting this partition logs the
            // leader/epoch transition it observes in committed metadata. Logged
            // on ALL replicas, so the full leadership sequence survives even
            // when the controller-leader pod that drove the change is killed —
            // used to trace failover leadership churn / flip-flop.
            tracing::info!(
                topic = %self.topic,
                partition = self.partition_id,
                prev_leader,
                new_leader,
                prev_epoch,
                new_epoch,
                "partition leadership changed (observed in committed metadata)"
            );
            st.per_follower.clear();
        }
        st.current_leader_epoch = new_epoch;
        drop(st);
        self.hw_advance_notify.notify_waiters();
    }

    /// Wait until `replica_state.hw >= target_offset` or `deadline`
    /// elapses. Used by the Produce handler for `acks == -1` to gate
    /// the response on full replication.
    ///
    /// # Errors
    ///
    /// Returns `Err(HwTimeout)` if the deadline elapses before the HW
    /// advances. Returns `Ok(())` on the first re-check that satisfies
    /// the target.
    pub async fn await_hw_at_least(
        &self,
        target_offset: LogOffset,
        deadline: std::time::Instant,
    ) -> Result<(), HwTimeout> {
        loop {
            if self.high_watermark().await >= target_offset {
                return Ok(());
            }
            // Subscribe to the notify BEFORE re-reading HW so we don't
            // miss an advance that happens between read and await.
            let waiter = self.hw_advance_notify.notified();
            tokio::pin!(waiter);
            if self.high_watermark().await >= target_offset {
                return Ok(());
            }
            tokio::select! {
                () = &mut waiter => {},
                () = tokio::time::sleep_until(deadline.into()) => {
                    // Diagnostic: an acks=all produce gave up waiting for the HW
                    // to reach its appended offset. Dump the leader-side replica
                    // state so a failover stall (HW stuck because the ISR can't
                    // be satisfied) is observable — this path was previously
                    // silent. Cheap: only fires on a (rare) produce timeout.
                    let leader_leo = self.log_end_offset();
                    let st = self.replica_state.lock().await;
                    let mut isr: Vec<crabka_raft::NodeId> = st.isr.iter().copied().collect();
                    isr.sort_unstable();
                    let followers: Vec<(crabka_raft::NodeId, i64)> =
                        st.per_follower.iter().map(|(k, v)| (*k, v.leo)).collect();
                    tracing::warn!(
                        target_offset,
                        hw = st.hw,
                        leader_leo,
                        leader_epoch = st.current_leader_epoch,
                        ?isr,
                        ?followers,
                        "await_hw_at_least: acks=all produce timed out; HW below target offset"
                    );
                    return Err(HwTimeout);
                }
            }
        }
    }

    /// Test-only: directly set the partition's `current_leader_epoch`
    /// without going through the supervisor's metadata-image-driven path.
    /// Used by `tests/leader_epoch.rs` to simulate split-brain by forcing
    /// an epoch bump mid-Produce.
    #[cfg(any(test, feature = "test-helpers"))]
    pub fn test_set_leader_epoch(&self, epoch: i32) {
        self.current_leader_epoch
            .store(epoch, std::sync::atomic::Ordering::Release);
    }

    /// Test-only: shift the partition's in-memory `log_start_offset` to
    /// `new_start`. Goes through the writer task to maintain the
    /// single-writer invariant on the underlying `Log`.
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn test_set_log_start(&self, new_start: LogOffset) -> Result<(), BrokerError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.writer_tx
            .send(WriterMessage::TestSetLogStart {
                new_start,
                ack: ack_tx,
            })
            .await
            .map_err(|_| BrokerError::Replication("partition writer dead".into()))?;
        ack_rx
            .await
            .map_err(|_| BrokerError::Replication("ack dropped".into()))?
    }
}

impl std::fmt::Debug for Partition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Deliberately does NOT include `log` — formatting a `Mutex<Log>`
        // would block on the mutex and dump internal segment state into
        // tracing output.
        f.debug_struct("Partition")
            .field("topic", &self.topic)
            .field("partition_id", &self.partition_id)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI32, AtomicU64};

    use assert2::{assert, check};
    use crabka_log::LogConfig;
    use tempfile::tempdir;

    use super::*;

    fn test_partition(hw_advance_notify: Arc<Notify>) -> (Partition, tempfile::TempDir) {
        let dir = tempdir().expect("tempdir");
        let log = Log::open(dir.path(), LogConfig::default()).expect("open log");
        let (tx, _rx) = mpsc::channel::<WriterMessage>(1);
        let writer = tokio::spawn(async {});
        let p = Partition {
            topic: "t".into(),
            partition_id: 0,
            log_dir: Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
            log: Arc::new(Mutex::new(log)),
            writer_tx: tx,
            append_notify: Arc::new(Notify::new()),
            replica_state: Arc::new(tokio::sync::Mutex::new(
                crate::replica_state::ReplicaState::new(),
            )),
            hw_advance_notify,
            current_leader: Arc::new(AtomicU64::new(0)),
            current_leader_epoch: Arc::new(AtomicI32::new(0)),
            _writer_handle: Arc::new(writer),
        };
        (p, dir)
    }

    fn test_partition_with_writer() -> (Partition, tempfile::TempDir) {
        let dir = tempdir().expect("tempdir");
        let log = Arc::new(Mutex::new(
            Log::open(dir.path(), LogConfig::default()).expect("open log"),
        ));
        let log_dir = Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf()));
        let (tx, rx) = mpsc::channel::<WriterMessage>(8);
        let append_notify = Arc::new(Notify::new());
        let replica_state = Arc::new(tokio::sync::Mutex::new(
            crate::replica_state::ReplicaState::new(),
        ));
        let hw_advance_notify = Arc::new(Notify::new());
        let writer = tokio::spawn(crate::partition_writer::run(
            "t".to_string(),
            0,
            log.clone(),
            log_dir.clone(),
            rx,
            append_notify.clone(),
            replica_state.clone(),
            hw_advance_notify.clone(),
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(crate::producer_state::ProducerState::new()),
        ));
        let p = Partition {
            topic: "t".into(),
            partition_id: 0,
            log_dir,
            log,
            writer_tx: tx,
            append_notify,
            replica_state,
            hw_advance_notify,
            current_leader: Arc::new(AtomicU64::new(0)),
            current_leader_epoch: Arc::new(AtomicI32::new(0)),
            _writer_handle: Arc::new(writer),
        };
        (p, dir)
    }

    fn append_records(p: &Partition, count: i32) {
        use crabka_protocol::records::{Attributes, Record, RecordBatch};

        let mut batch = RecordBatch {
            base_offset: 0,
            partition_leader_epoch: -1,
            attributes: Attributes::default(),
            last_offset_delta: count - 1,
            base_timestamp: 1_700_000_000,
            max_timestamp: 1_700_000_000,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: (0..count)
                .map(|i| Record {
                    attributes: 0,
                    offset_delta: i,
                    timestamp_delta: 0,
                    key: None,
                    value: Some(bytes::Bytes::from_static(b"v")),
                    headers: vec![],
                })
                .collect(),
        };
        p.log
            .lock()
            .expect("log mutex")
            .append(&mut batch)
            .expect("append");
    }

    #[test]
    fn partition_is_clone_and_send() {
        // Compile-time check.
        fn assert_send<T: Send>() {}
        fn assert_clone<T: Clone>() {}
        assert_send::<Partition>();
        assert_clone::<Partition>();
    }

    #[tokio::test]
    async fn debug_does_not_dump_log() {
        let dir = tempdir().expect("tempdir");
        let log = Log::open(dir.path(), LogConfig::default()).expect("open log");
        let (tx, _rx) = mpsc::channel::<WriterMessage>(1);
        let writer = tokio::spawn(async {});
        let p = Partition {
            topic: "t".into(),
            partition_id: 0,
            log_dir: Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
            log: Arc::new(Mutex::new(log)),
            writer_tx: tx,
            append_notify: Arc::new(Notify::new()),
            replica_state: Arc::new(tokio::sync::Mutex::new(
                crate::replica_state::ReplicaState::new(),
            )),
            hw_advance_notify: Arc::new(Notify::new()),
            current_leader: Arc::new(AtomicU64::new(0)),
            current_leader_epoch: Arc::new(AtomicI32::new(0)),
            _writer_handle: Arc::new(writer),
        };
        let s = format!("{p:?}");
        // topic/partition_id appear; the mutex/log internals must NOT appear
        // in Debug output.
        let cases = [
            ("topic", true),
            ("partition_id", true),
            ("Mutex", false),
            ("segments", false),
        ];
        for (needle, expected) in cases {
            assert!(s.contains(needle) == expected, "needle {needle:?} in {s:?}");
        }
    }

    #[tokio::test]
    async fn high_watermark_reads_cached_value() {
        let dir = tempdir().expect("tempdir");
        let log = Log::open(dir.path(), LogConfig::default()).expect("open log");
        let (tx, _rx) = mpsc::channel::<WriterMessage>(1);
        let writer = tokio::spawn(async {});
        let replica_state = Arc::new(tokio::sync::Mutex::new(
            crate::replica_state::ReplicaState::new(),
        ));
        {
            let mut st = replica_state.lock().await;
            st.hw = 42;
        }
        let p = Partition {
            topic: "t".into(),
            partition_id: 0,
            log_dir: Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
            log: Arc::new(Mutex::new(log)),
            writer_tx: tx,
            append_notify: Arc::new(Notify::new()),
            replica_state,
            hw_advance_notify: Arc::new(Notify::new()),
            current_leader: Arc::new(AtomicU64::new(0)),
            current_leader_epoch: Arc::new(AtomicI32::new(0)),
            _writer_handle: Arc::new(writer),
        };
        assert!(p.high_watermark().await == 42);
    }

    #[tokio::test]
    async fn install_isr_populates_replica_state() {
        let dir = tempdir().expect("tempdir");
        let log = Log::open(dir.path(), LogConfig::default()).expect("open log");
        let (tx, _rx) = mpsc::channel::<WriterMessage>(1);
        let writer = tokio::spawn(async {});
        let p = Partition {
            topic: "t".into(),
            partition_id: 0,
            log_dir: Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
            log: Arc::new(Mutex::new(log)),
            writer_tx: tx,
            append_notify: Arc::new(Notify::new()),
            replica_state: Arc::new(tokio::sync::Mutex::new(
                crate::replica_state::ReplicaState::new(),
            )),
            hw_advance_notify: Arc::new(Notify::new()),
            current_leader: Arc::new(AtomicU64::new(0)),
            current_leader_epoch: Arc::new(AtomicI32::new(0)),
            _writer_handle: Arc::new(writer),
        };
        p.install_isr(&[1, 2, 3], &[1, 2, 3], 1).await;
        let st = p.replica_state.lock().await;
        check!(st.isr == [1, 2, 3].into_iter().collect());
        check!(st.per_follower.get(&2).map(|f| f.leo) == Some(0));
    }

    #[tokio::test]
    async fn install_isr_notifies_when_high_watermark_advances() {
        let hw_advance_notify = Arc::new(Notify::new());
        let (p, _td) = test_partition(hw_advance_notify.clone());
        append_records(&p, 3);
        assert!(p.high_watermark().await == 0);

        let waiter = hw_advance_notify.notified();
        tokio::pin!(waiter);
        assert!(
            futures_util::poll!(&mut waiter).is_pending(),
            "waiter registers on first poll"
        );

        p.install_isr(&[1], &[1], 1).await;

        assert!(p.high_watermark().await == 3);
        assert!(
            futures_util::poll!(&mut waiter).is_ready(),
            "notify should fire when ISR install advances HW"
        );
    }

    #[tokio::test]
    async fn install_isr_same_high_watermark_does_not_notify() {
        let hw_advance_notify = Arc::new(Notify::new());
        let (p, _td) = test_partition(hw_advance_notify.clone());
        append_records(&p, 2);
        p.install_isr(&[1], &[1], 1).await;
        assert!(p.high_watermark().await == 2);

        let waiter = hw_advance_notify.notified();
        tokio::pin!(waiter);
        assert!(
            futures_util::poll!(&mut waiter).is_pending(),
            "waiter registers on first poll"
        );

        p.install_isr(&[1], &[1], 1).await;

        assert!(p.high_watermark().await == 2);
        assert!(
            futures_util::poll!(&mut waiter).is_pending(),
            "unchanged HW must not wake waiters"
        );
    }

    #[tokio::test]
    async fn install_leader_change_clears_followers() {
        // (new_leader, new_epoch, seeded_follower_leo):
        // first case changes only the leader, second only the epoch —
        // either change alone must clear follower state.
        let cases = [(2u64, 0i32, 11i64), (0, 9, 17)];
        for (leader, epoch, seeded_leo) in cases {
            let (p, _td) = test_partition(Arc::new(Notify::new()));
            p.install_isr(&[1, 2], &[1, 2], 1).await;
            {
                let mut st = p.replica_state.lock().await;
                st.per_follower.get_mut(&2).expect("follower").leo = seeded_leo;
            }

            p.install_leader_change(leader, epoch).await;

            assert!(
                p.current_leader.load(Ordering::Acquire) == leader,
                "case ({leader}, {epoch})"
            );
            assert!(
                p.current_leader_epoch.load(Ordering::Acquire) == epoch,
                "case ({leader}, {epoch})"
            );
            let st = p.replica_state.lock().await;
            assert!(st.per_follower.is_empty(), "case ({leader}, {epoch})");
            assert!(st.current_leader_epoch == epoch, "case ({leader}, {epoch})");
        }
    }

    #[tokio::test]
    async fn test_set_leader_epoch_updates_cached_epoch() {
        let (p, _td) = test_partition(Arc::new(Notify::new()));

        p.test_set_leader_epoch(6);

        assert!(p.current_leader_epoch.load(Ordering::Acquire) == 6);
    }

    #[tokio::test]
    async fn test_set_log_start_updates_log_start_through_writer() {
        let (p, _td) = test_partition_with_writer();

        p.test_set_log_start(5).await.expect("set log start");

        assert!(p.log_start_offset() == 5);
    }

    #[tokio::test]
    async fn await_hw_returns_immediately_if_already_satisfied() {
        let dir = tempdir().expect("tempdir");
        let log = Log::open(dir.path(), LogConfig::default()).expect("open log");
        let (tx, _rx) = mpsc::channel::<WriterMessage>(1);
        let writer = tokio::spawn(async {});
        let replica_state = Arc::new(tokio::sync::Mutex::new(
            crate::replica_state::ReplicaState::new(),
        ));
        {
            let mut st = replica_state.lock().await;
            st.hw = 100;
        }
        let p = Partition {
            topic: "t".into(),
            partition_id: 0,
            log_dir: Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
            log: Arc::new(Mutex::new(log)),
            writer_tx: tx,
            append_notify: Arc::new(Notify::new()),
            replica_state,
            hw_advance_notify: Arc::new(Notify::new()),
            current_leader: Arc::new(AtomicU64::new(0)),
            current_leader_epoch: Arc::new(AtomicI32::new(0)),
            _writer_handle: Arc::new(writer),
        };
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        p.await_hw_at_least(50, deadline).await.expect("immediate");
    }

    #[tokio::test]
    async fn await_hw_returns_timeout_when_unreached() {
        let dir = tempdir().expect("tempdir");
        let log = Log::open(dir.path(), LogConfig::default()).expect("open log");
        let (tx, _rx) = mpsc::channel::<WriterMessage>(1);
        let writer = tokio::spawn(async {});
        let p = Partition {
            topic: "t".into(),
            partition_id: 0,
            log_dir: Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
            log: Arc::new(Mutex::new(log)),
            writer_tx: tx,
            append_notify: Arc::new(Notify::new()),
            replica_state: Arc::new(tokio::sync::Mutex::new(
                crate::replica_state::ReplicaState::new(),
            )),
            hw_advance_notify: Arc::new(Notify::new()),
            current_leader: Arc::new(AtomicU64::new(0)),
            current_leader_epoch: Arc::new(AtomicI32::new(0)),
            _writer_handle: Arc::new(writer),
        };
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(50);
        let result = p.await_hw_at_least(100, deadline).await;
        assert!(matches!(result, Err(crate::partition::HwTimeout)));
    }

    #[tokio::test]
    async fn set_follower_hw_clamps_advances_and_notifies() {
        use crabka_protocol::records::{Attributes, Record, RecordBatch};

        let dir = tempdir().expect("tempdir");
        let log = Log::open(dir.path(), LogConfig::default()).expect("open log");
        let (tx, _rx) = mpsc::channel::<WriterMessage>(1);
        let writer = tokio::spawn(async {});
        let hw_advance_notify = Arc::new(Notify::new());
        let p = Partition {
            topic: "t".into(),
            partition_id: 0,
            log_dir: Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
            log: Arc::new(Mutex::new(log)),
            writer_tx: tx,
            append_notify: Arc::new(Notify::new()),
            replica_state: Arc::new(tokio::sync::Mutex::new(
                crate::replica_state::ReplicaState::new(),
            )),
            hw_advance_notify: hw_advance_notify.clone(),
            current_leader: Arc::new(AtomicU64::new(0)),
            current_leader_epoch: Arc::new(AtomicI32::new(0)),
            _writer_handle: Arc::new(writer),
        };

        // Append a 3-record batch so log_end_offset() == 3.
        let mut batch = RecordBatch {
            base_offset: 0,
            partition_leader_epoch: -1,
            attributes: Attributes::default(),
            last_offset_delta: 2,
            base_timestamp: 1_700_000_000,
            max_timestamp: 1_700_000_000,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: (0..3)
                .map(|i| Record {
                    attributes: 0,
                    offset_delta: i,
                    timestamp_delta: 0,
                    key: None,
                    value: Some(bytes::Bytes::from_static(b"v")),
                    headers: vec![],
                })
                .collect(),
        };
        p.log
            .lock()
            .expect("log mutex")
            .append(&mut batch)
            .expect("append");
        assert!(p.log_end_offset() == 3);

        // reported_hw below log_end: stored verbatim, notify fires.
        // A `Notified` future does not register with the `Notify` until it is
        // first polled, and `notify_waiters()` only wakes already-registered
        // waiters — so poll once (Pending) to register BEFORE advancing HW.
        let waiter = hw_advance_notify.notified();
        tokio::pin!(waiter);
        assert!(
            futures_util::poll!(&mut waiter).is_pending(),
            "waiter registers on first poll"
        );
        p.set_follower_hw(2).await;
        assert!(p.high_watermark().await == 2);
        assert!(
            futures_util::poll!(&mut waiter).is_ready(),
            "notify should fire when HW advances"
        );

        // reported_hw above log_end: clamped to log_end (3).
        p.set_follower_hw(100).await;
        assert!(p.high_watermark().await == 3);

        // reported_hw below current HW: no regression.
        p.set_follower_hw(1).await;
        assert!(p.high_watermark().await == 3);
    }

    #[tokio::test]
    async fn set_follower_hw_same_high_watermark_does_not_notify() {
        let hw_advance_notify = Arc::new(Notify::new());
        let (p, _td) = test_partition(hw_advance_notify.clone());
        assert!(p.high_watermark().await == 0);

        let waiter = hw_advance_notify.notified();
        tokio::pin!(waiter);
        assert!(
            futures_util::poll!(&mut waiter).is_pending(),
            "waiter registers on first poll"
        );

        p.set_follower_hw(0).await;

        assert!(p.high_watermark().await == 0);
        assert!(
            futures_util::poll!(&mut waiter).is_pending(),
            "unchanged HW must not wake waiters"
        );
    }

    #[tokio::test]
    async fn await_hw_wakes_on_advance() {
        let dir = tempdir().expect("tempdir");
        let log = Log::open(dir.path(), LogConfig::default()).expect("open log");
        let (tx, _rx) = mpsc::channel::<WriterMessage>(1);
        let writer = tokio::spawn(async {});
        let replica_state = Arc::new(tokio::sync::Mutex::new(
            crate::replica_state::ReplicaState::new(),
        ));
        let hw_advance_notify = Arc::new(Notify::new());
        let p = Partition {
            topic: "t".into(),
            partition_id: 0,
            log_dir: Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
            log: Arc::new(Mutex::new(log)),
            writer_tx: tx,
            append_notify: Arc::new(Notify::new()),
            replica_state: replica_state.clone(),
            hw_advance_notify: hw_advance_notify.clone(),
            current_leader: Arc::new(AtomicU64::new(0)),
            current_leader_epoch: Arc::new(AtomicI32::new(0)),
            _writer_handle: Arc::new(writer),
        };
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            replica_state.lock().await.hw = 100;
            hw_advance_notify.notify_waiters();
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        p.await_hw_at_least(50, deadline)
            .await
            .expect("woke on advance");
    }
}
