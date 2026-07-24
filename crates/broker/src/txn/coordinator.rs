//! Per-broker `TxnCoordinator`. Owns the in-memory state map of every
//! `transactional_id` whose `__transaction_state` partition this broker
//! hosts as leader. Persists every state change as a record in the
//! corresponding `__transaction_state` partition. Recovers state on
//! `Broker::start` by replaying those partitions.

// `is_coordinator_for`, `get`, and a couple of admin helpers are consumed by
// the transaction wire handlers. Remove this attribute once those land.
#![allow(dead_code)]

use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use crabka_ids::PartitionIndex;
use crabka_log::{Offset, ProducerId};
use crabka_metadata::MetadataImage;
use crabka_protocol::records::{Record, RecordBatch};
use dashmap::DashMap;
use tokio::sync::{Mutex, RwLock};
use tracing::{info, warn};

use crate::{
    coordinator::unified::classic_state::OffsetEntry,
    error::BrokerError,
    partition_registry::PartitionRegistry,
    txn::{
        bootstrap,
        handlers::end_txn::next_producer_identity,
        partitioner::partition_for_tid,
        state::{TxnEntry, TxnState},
        two_pc::should_abort_idle_txn,
        version::TxnVersion,
    },
};

/// A consumer-group committed-offset key: `(topic, partition)`.
pub(crate) type OffsetKey = (String, i32);

/// Buffered transactional offsets for one producer, grouped by consumer
/// `group_id`. A producer may fold offset commits for several groups into a
/// single transaction (each `TxnOffsetCommit` carries its own `group_id`), so
/// the buffer keys by group inside one producer's pending set.
pub(crate) type PendingTxnOffsets =
    std::collections::HashMap<String, Vec<(OffsetKey, OffsetEntry)>>;

/// Live-dependency seam for the KIP-939 idle-transaction reaper.
///
/// `sweep_expired`'s orchestration (the per-tid three-phase abort dance) is
/// pure decision logic wrapped around four irreducible side effects: a
/// coordinator-ownership check, two compare-and-swap-style persisted
/// transitions (`Ongoing → PrepareAbort`, then `PrepareAbort → CompleteAbort`),
/// the abort-marker fan-out, and producer-identity allocation. Each of those
/// touches a live `__transaction_state` partition, partition leaders, or the
/// producer-id allocator. Pulling them behind this trait lets the orchestration
/// be unit-tested against a [`mockall`] mock — every method returns
/// already-extracted plain data (snapshots), so the decisions consuming them
/// are killable by a mock. The live adapter is [`TxnCoordinator`] itself.
///
/// The two `*_transition` methods perform the entry mutation **atomically under
/// the per-tid lock** (so a concurrent `EndTxn`/`InitProducerId` is not
/// clobbered) and return the resulting persisted snapshot, or `None` when the
/// guard failed (lost race / no longer present). The pure helpers
/// [`apply_prepare_abort`] / [`apply_complete_abort`] / [`complete_abort_guard_ok`]
/// compute the transitions; the backend only owns the CAS + persistence, which
/// is the irreducible part.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub(crate) trait ReaperBackend: Send + Sync {
    /// Is this broker the transaction coordinator for `tid` right now?
    async fn is_coordinator_for(&self, tid: &str) -> bool;

    /// Atomically, under `tid`'s entry lock: if the entry should be aborted as
    /// an idle transaction at `now_ms`, transition `Ongoing → PrepareAbort`,
    /// persist it, and return the persisted snapshot. Returns `None` when the
    /// entry is absent, must not be reaped, or persistence failed.
    async fn prepare_abort(&self, tid: &str, now_ms: i64, txnv: TxnVersion) -> Option<TxnEntry>;

    /// Fan out abort markers for `entry` to the partition leaders this broker
    /// hosts (remote leaders are logged + skipped).
    async fn dispatch_abort_markers(&self, entry: &TxnEntry);

    /// Atomically, under `tid`'s entry lock: re-validate that the current entry
    /// still matches the `prepared` snapshot this reaper wrote (same identity +
    /// still `PrepareAbort`), and if so transition to `CompleteAbort` — bumping
    /// producer identity per KIP-890 at `now_ms` — persist it, and return the
    /// persisted snapshot. Returns `None` when the entry advanced underneath us
    /// or persistence failed.
    async fn complete_abort(
        &self,
        prepared: &TxnEntry,
        now_ms: i64,
        txnv: TxnVersion,
    ) -> Option<TxnEntry>;
}

/// The `Ongoing → PrepareAbort` mutation for an idle-reaped entry: flip the
/// state and stamp `last_update_ms`. Pure so the transition is unit-killable
/// independently of persistence.
fn apply_prepare_abort(entry: &mut TxnEntry, now_ms: i64) {
    entry.state = TxnState::PrepareAbort;
    entry.last_update_ms = now_ms;
}

/// The `PrepareAbort → CompleteAbort` mutation, given the freshly-allocated
/// `(producer_id, producer_epoch)` from the KIP-890 identity bump. Records the
/// prior id as `prev_producer_id` only when a roll actually happened (a fresh
/// pid was allocated). Pure so the transition is unit-killable.
fn apply_complete_abort(entry: &mut TxnEntry, new_pid: ProducerId, new_epoch: i16, now_ms: i64) {
    if new_pid != entry.producer_id {
        entry.prev_producer_id = entry.producer_id;
    }
    entry.state = TxnState::CompleteAbort;
    entry.producer_id = new_pid;
    entry.producer_epoch = new_epoch;
    entry.last_update_ms = now_ms;
}

/// Does the current re-acquired `entry` still match the `prepared` snapshot this
/// reaper wrote in Phase 1, so it is safe to finalise to `CompleteAbort`? Guards
/// against a concurrent `EndTxn`/`InitProducerId` having advanced the entry.
/// Pure so the guard is unit-killable.
fn complete_abort_guard_ok(entry: &TxnEntry, prepared: &TxnEntry) -> bool {
    entry.producer_id == prepared.producer_id
        && entry.producer_epoch == prepared.producer_epoch
        && entry.state == TxnState::PrepareAbort
}

/// The reaper orchestration loop, generic over the [`ReaperBackend`] seam so it
/// is unit-testable against a mock. For each candidate tid it runs the
/// three-phase abort: ownership check → `prepare_abort` (CAS) → marker fan-out →
/// `complete_abort` (CAS). Returns the tids it finalised, in iteration order.
async fn sweep_with_backend<B: ReaperBackend + ?Sized>(
    backend: &B,
    candidates: Vec<String>,
    now_ms: i64,
    txnv: TxnVersion,
) -> Vec<String> {
    let mut aborted = Vec::new();
    for tid in candidates {
        // Only reap transactions this broker currently coordinates: a
        // partition we used to lead may have moved, leaving stale state.
        if !backend.is_coordinator_for(&tid).await {
            continue;
        }

        // Phase 1: decide + Ongoing → PrepareAbort, persisted under the lock.
        let Some(prepared) = backend.prepare_abort(&tid, now_ms, txnv).await else {
            continue;
        };

        // Phase 2: fan out abort markers to local partition leaders.
        backend.dispatch_abort_markers(&prepared).await;

        // Phase 3: PrepareAbort → CompleteAbort, re-validating identity + state
        // under the lock so a concurrent EndTxn / InitProducerId is not
        // clobbered.
        if backend
            .complete_abort(&prepared, now_ms, txnv)
            .await
            .is_some()
        {
            info!(tid, "txn reaper: aborted timed-out transaction");
            aborted.push(tid);
        }
    }
    aborted
}

/// Per-broker transaction coordinator. Constructed in `Broker::start`
/// and shared via `Arc` with the transaction wire handlers.
pub(crate) struct TxnCoordinator {
    pub(crate) node_id: crabka_metadata::NodeId,
    pub(crate) partitions: Arc<PartitionRegistry>,
    pub(crate) producer_ids: Arc<crate::producer_id_manager::ProducerIdManager>,
    num_partitions: i32,
    /// Live in-memory state: `transactional_id` → locked `TxnEntry`.
    state: DashMap<String, Arc<Mutex<TxnEntry>>>,
    /// Set of `__transaction_state` partition indices this broker leads.
    leader_partitions: RwLock<HashSet<PartitionIndex>>,
    /// Reverse lookup: `producer_id` → `transactional_id`. Used by the
    /// Produce handler to verify transactional batches (KIP-1319 v2).
    pid_to_tid: DashMap<ProducerId, String>,
    /// KIP-447 transactional consumer offsets buffered per `producer_id`,
    /// pending the transaction's COMMIT/ABORT marker. `TxnOffsetCommit`
    /// appends the offset records to `__consumer_offsets` (held under the LSO)
    /// AND records them here; on COMMIT (`EndTxn` with `committed=true`) the
    /// buffer is drained and materialized into the owning group's in-memory
    /// `committed_offsets` (the map `OffsetFetch` reads), matching Kafka's
    /// "visible only after the commit marker" semantics. On ABORT the buffer
    /// is dropped without applying. Keyed by `producer_id` because that is the
    /// identity `EndTxn` finalizes on; the value groups offsets by the
    /// `group_id` each `TxnOffsetCommit` named.
    pending_txn_offsets: DashMap<ProducerId, PendingTxnOffsets>,
}

impl TxnCoordinator {
    pub(crate) fn new(
        node_id: crabka_metadata::NodeId,
        partitions: Arc<PartitionRegistry>,
        producer_ids: Arc<crate::producer_id_manager::ProducerIdManager>,
        num_partitions: i32,
    ) -> Self {
        Self {
            node_id,
            partitions,
            producer_ids,
            num_partitions,
            state: DashMap::new(),
            leader_partitions: RwLock::new(HashSet::new()),
            pid_to_tid: DashMap::new(),
            pending_txn_offsets: DashMap::new(),
        }
    }

    /// Buffer a `TxnOffsetCommit`'s offsets for `producer_id` under `group_id`,
    /// pending the transaction's commit marker. Called from the
    /// `TxnOffsetCommit` handler after the offset records are appended to
    /// `__consumer_offsets`. Multiple commits for the same `(producer_id,
    /// group_id)` within one transaction accumulate (later entries for the same
    /// `(topic, partition)` are applied last-writer-wins at materialization, the
    /// same as a non-transactional re-commit).
    pub(crate) fn buffer_txn_offsets(
        &self,
        producer_id: ProducerId,
        group_id: &str,
        entries: Vec<(OffsetKey, OffsetEntry)>,
    ) {
        if entries.is_empty() {
            return;
        }
        self.pending_txn_offsets
            .entry(producer_id)
            .or_default()
            .entry(group_id.to_string())
            .or_default()
            .extend(entries);
    }

    /// Remove and return all buffered transactional offsets for `producer_id`
    /// (grouped by `group_id`). Used by `EndTxn`: on COMMIT the returned offsets
    /// are materialized into each group's `committed_offsets`; on ABORT this is
    /// still called so the buffer is dropped, and the result discarded. Returns
    /// an empty map if the producer buffered no transactional offsets.
    pub(crate) fn take_txn_offsets(&self, producer_id: ProducerId) -> PendingTxnOffsets {
        self.pending_txn_offsets
            .remove(&producer_id)
            .map(|(_, v)| v)
            .unwrap_or_default()
    }

    /// Recompute which `__transaction_state` partitions this broker leads
    /// from the current `MetadataImage`. Called from `recover` and also
    /// on every metadata change.
    pub(crate) async fn refresh_leader_partitions(&self, image: &MetadataImage) {
        let mut set = HashSet::new();
        for p in image.partitions_of(bootstrap::TOPIC) {
            if p.leader == self.node_id {
                set.insert(PartitionIndex(p.partition));
            }
        }
        *self.leader_partitions.write().await = set;
    }

    /// Returns the `__transaction_state` partition index responsible for `tid`.
    pub(crate) fn partition_for(&self, tid: &str) -> PartitionIndex {
        PartitionIndex(partition_for_tid(tid, self.num_partitions))
    }

    /// Returns `true` if this broker is the transaction coordinator for `tid`.
    pub(crate) async fn is_coordinator_for(&self, tid: &str) -> bool {
        let p = self.partition_for(tid);
        self.leader_partitions.read().await.contains(&p)
    }

    /// Retrieve the locked `TxnEntry` for `tid`, or `None` if unknown.
    pub(crate) fn get(&self, tid: &str) -> Option<Arc<Mutex<TxnEntry>>> {
        self.state.get(tid).map(|e| e.value().clone())
    }

    /// Reverse lookup: given a `producer_id`, return the `transactional_id`
    /// it was registered under, or `None` if the pid is unknown.
    pub(crate) fn tid_for_pid(&self, pid: ProducerId) -> Option<String> {
        self.pid_to_tid.get(&pid).map(|e| e.value().clone())
    }

    /// Evict the stale `prev_producer_id -> tid` mapping after a KIP-890
    /// epoch-overflow roll. When the producer epoch is exhausted the `EndTxn`
    /// completion path allocates a new `producer_id` and records the prior id
    /// as `entry.prev_producer_id` (see `next_producer_identity`); without this
    /// the old id's mapping would leak one entry per roll. Idempotent: a no-op
    /// once the old id is gone, and skipped for entries that never rolled
    /// (`prev == -1`). pids are globally unique, so the prior id only ever
    /// mapped to this tid — removing it can't affect another transaction.
    fn evict_rolled_pid(pid_to_tid: &DashMap<ProducerId, String>, entry: &TxnEntry) {
        if entry.prev_producer_id >= 0 && entry.prev_producer_id != entry.producer_id {
            pid_to_tid.remove(&entry.prev_producer_id);
        }
    }

    /// Snapshot every locally-coordinated `TxnEntry`. Used by the KIP-664
    /// admin handlers (`ListTransactions`, `DescribeTransactions`) to
    /// expose the in-memory txn-state map. Each entry is locked + cloned
    /// in turn so the snapshot is internally consistent per-tid but not
    /// across the entire batch — acceptable for an admin introspection
    /// API (Apache Kafka's JVM coordinator has the same property).
    pub(crate) async fn snapshot(&self) -> Vec<TxnEntry> {
        // Collect the `Arc<Mutex<_>>` handles first so we don't hold the
        // DashMap shard locks while taking the inner async mutex.
        let handles: Vec<Arc<Mutex<TxnEntry>>> =
            self.state.iter().map(|e| e.value().clone()).collect();
        let mut out = Vec::with_capacity(handles.len());
        for h in handles {
            let entry = h.lock().await;
            out.push(entry.clone());
        }
        out
    }

    /// Persist `entry` to the corresponding `__transaction_state` partition
    /// log, then update the in-memory map. The batch is appended via the
    /// partition's writer task (ordered with all other produce appends).
    ///
    /// `txnv` is the finalized `transaction.version` resolved from the live
    /// metadata image at the caller; it selects the byte-exact Kafka
    /// `TransactionLogValue` format (v0 for `TV_0`, v1 for `TV >= 1`).
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Txn`] if the partition is not locally held
    /// or the append fails.
    #[tracing::instrument(
        name = "txn_coordinator_put",
        level = "debug",
        skip_all,
        fields(tid = %entry.transactional_id, producer_id = entry.producer_id.0),
        err,
    )]
    pub(crate) async fn put(
        &self,
        entry: TxnEntry,
        txnv: crate::txn::version::TxnVersion,
    ) -> Result<(), BrokerError> {
        let tid = entry.transactional_id.clone();
        let p = self.partition_for(&tid);
        let part = self
            .partitions
            .get(bootstrap::TOPIC, p)
            .ok_or_else(|| BrokerError::Txn(format!("__transaction_state-{p} not local")))?;

        // Byte-exact Kafka TransactionLogKey(v0) + TransactionLogValue(v0/v1).
        let key = crate::txn::log_record::encode_key(&tid);
        let value = crate::txn::log_record::encode_value(&entry, txnv.flexible_records());

        let mut batch = RecordBatch::default();
        batch.records.push(Record {
            offset_delta: 0,
            key: Some(Bytes::from(key)),
            value: Some(Bytes::from(value)),
            ..Default::default()
        });
        batch.last_offset_delta = 0;

        part.produce_batch(batch).await?;

        Self::evict_rolled_pid(&self.pid_to_tid, &entry);
        self.pid_to_tid
            .insert(entry.producer_id, entry.transactional_id.clone());
        self.state.insert(tid, Arc::new(Mutex::new(entry)));
        Ok(())
    }

    /// KIP-939 idle-transaction reaper: abort every locally-coordinated,
    /// non-2PC, `Ongoing` transaction whose timeout has elapsed at `now_ms`.
    ///
    /// 2PC transactions (`txn_timeout_ms == NO_TIMEOUT_MS`) are skipped — their
    /// external transaction manager owns the commit/abort decision, and Kafka
    /// must never unilaterally abort a prepared 2PC transaction. The decision is
    /// delegated to [`should_abort_idle_txn`], the exhaustively model-checked
    /// core (see [`crate::txn::two_pc_model`]).
    ///
    /// Each abort runs the same two-step transition + marker fan-out as an
    /// `EndTxn(committed=false)` and bumps the producer epoch on completion (at
    /// `TV_2`) so the timed-out producer is fenced. Marker fan-out is local-only
    /// (remote partitions are logged + skipped, mirroring the `InitProducerId`
    /// abort-on-stale-Ongoing path); a concurrent caller that changed the entry
    /// out from under us aborts this reap of that tid (re-validated before the
    /// Complete write). Returns the tids it finalized.
    // cargo-mutants: I/O orchestration over live DashMap/partition state
    #[cfg_attr(test, mutants::skip)]
    #[tracing::instrument(
        name = "txn_coordinator_sweep_expired",
        level = "debug",
        skip_all,
        fields(now_ms)
    )]
    pub(crate) async fn sweep_expired(&self, now_ms: i64, txnv: TxnVersion) -> Vec<String> {
        // Snapshot the candidate tids first so we don't hold DashMap shard locks
        // across the async abort work; the orchestration then drives the live
        // `ReaperBackend` (this coordinator), which re-acquires each entry's lock
        // per phase.
        let candidates: Vec<String> = self.state.iter().map(|e| e.key().clone()).collect();
        sweep_with_backend(self, candidates, now_ms, txnv).await
    }

    /// Replay every locally-led `__transaction_state` partition into the
    /// in-memory state map. Called from `Broker::start`.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] if reading a partition's log fails with an
    /// error other than reading past the end (which is treated as a normal
    /// "partition is empty" condition).
    // The `base_offset + last_offset_delta + 1` next-batch offset advance is
    // only reachable by replaying real committed `__transaction_state` batches
    // from an on-disk `Log`; there is no pure seam over the read loop, so the
    // arithmetic is exercised by the live recovery / differential suite.
    #[cfg_attr(test, mutants::skip)]
    #[tracing::instrument(name = "txn_coordinator_recover", level = "info", skip_all, err)]
    pub(crate) async fn recover(&self, image: &MetadataImage) -> Result<(), BrokerError> {
        self.refresh_leader_partitions(image).await;

        let local_partitions: Vec<PartitionIndex> = self
            .leader_partitions
            .read()
            .await
            .iter()
            .copied()
            .collect();

        for p in local_partitions {
            let Some(part) = self.partitions.get(bootstrap::TOPIC, p) else {
                // Partition is not yet open locally (no log dir / not yet created).
                continue;
            };

            let mut offset = part.log_start_offset();
            loop {
                let out = match part.read_log(offset, 1 << 20) {
                    Ok(o) => o,
                    // OffsetTooLow can happen when the partition just opened
                    // with no data written yet (log_start == log_end == 0
                    // but the log returns empty in that case). Treat any
                    // read error as "nothing to replay here" to be safe.
                    Err(e) => {
                        warn!(
                            partition = p.get(),
                            error = %e,
                            "read error during __transaction_state recovery; skipping partition"
                        );
                        break;
                    }
                };

                if out.batches.is_empty() {
                    break;
                }

                for batch in &out.batches {
                    for rec in &batch.records {
                        let Some(key_bytes) = rec.key.as_ref() else {
                            warn!(
                                partition = p.get(),
                                "__transaction_state record missing key; skipping"
                            );
                            continue;
                        };
                        let tid = match crate::txn::log_record::decode_key(key_bytes) {
                            Ok(t) => t,
                            Err(e) => {
                                warn!(
                                    partition = p.get(),
                                    error = %e,
                                    "invalid TransactionLogKey in __transaction_state; skipping"
                                );
                                continue;
                            }
                        };
                        let Some(value_bytes) = rec.value.as_ref() else {
                            // Tombstone (null value) deletes txn state for this tid.
                            self.state.remove(&tid);
                            continue;
                        };
                        let entry = match crate::txn::log_record::decode_value(value_bytes, tid) {
                            Ok(e) => e,
                            Err(e) => {
                                warn!(
                                    partition = p.get(),
                                    error = %e,
                                    "invalid TransactionLogValue in __transaction_state; skipping"
                                );
                                continue;
                            }
                        };
                        self.pid_to_tid
                            .insert(entry.producer_id, entry.transactional_id.clone());
                        self.state
                            .insert(entry.transactional_id.clone(), Arc::new(Mutex::new(entry)));
                    }
                    offset = Offset(batch.base_offset + i64::from(batch.last_offset_delta) + 1);
                }
            }
        }

        info!(
            tids_loaded = self.state.len(),
            "TxnCoordinator recovery complete"
        );
        Ok(())
    }
}

/// Live adapter: the real reaper side effects against the in-memory state map,
/// the `__transaction_state` partition log, partition leaders, and the
/// producer-id allocator. Only the irreducible IO lives here; every decision is
/// the pure helper / orchestration logic above.
#[async_trait]
impl ReaperBackend for TxnCoordinator {
    // cargo-mutants: thin adapter over inherent method / live lock state
    #[cfg_attr(test, mutants::skip)]
    async fn is_coordinator_for(&self, tid: &str) -> bool {
        let p = self.partition_for(tid);
        self.leader_partitions.read().await.contains(&p)
    }

    // cargo-mutants: I/O over live entry locks + raft persistence
    #[cfg_attr(test, mutants::skip)]
    async fn prepare_abort(&self, tid: &str, now_ms: i64, txnv: TxnVersion) -> Option<TxnEntry> {
        let handle = self.get(tid)?;
        let prepared = {
            let mut entry = handle.lock().await;
            if !should_abort_idle_txn(entry.state, entry.txn_timeout_ms, entry.start_ms, now_ms) {
                return None;
            }
            apply_prepare_abort(&mut entry, now_ms);
            entry.clone()
        };
        if let Err(e) = self.put(prepared.clone(), txnv).await {
            warn!(tid, error = %e, "txn reaper: failed to persist PrepareAbort; skipping");
            return None;
        }
        Some(prepared)
    }

    // cargo-mutants: writes abort markers to live partition logs
    #[cfg_attr(test, mutants::skip)]
    async fn dispatch_abort_markers(&self, entry: &TxnEntry) {
        use crate::txn::marker::{MarkerType, build_marker_batch};
        for tp in &entry.partitions {
            let Some(part) = self.partitions.get(&tp.topic, tp.partition) else {
                warn!(
                    topic = %tp.topic,
                    partition = tp.partition.get(),
                    "txn reaper: partition not locally led; abort marker needs inter-broker \
                     WriteTxnMarkers (not yet wired), skipping"
                );
                continue;
            };
            let marker = build_marker_batch(
                entry.producer_id,
                entry.producer_epoch,
                part.log_end_offset(),
                MarkerType::Abort,
            );
            if let Err(e) = part.produce_batch(marker).await {
                warn!(
                    topic = %tp.topic,
                    partition = tp.partition.get(),
                    error = %e,
                    "txn reaper: failed to write abort marker"
                );
            }
        }
    }

    // cargo-mutants: I/O over live entry locks + raft persistence
    #[cfg_attr(test, mutants::skip)]
    async fn complete_abort(
        &self,
        prepared: &TxnEntry,
        now_ms: i64,
        txnv: TxnVersion,
    ) -> Option<TxnEntry> {
        let tid = prepared.transactional_id.as_str();
        let handle = self.get(tid)?;
        let complete = {
            let mut entry = handle.lock().await;
            if !complete_abort_guard_ok(&entry, prepared) {
                // Someone advanced the entry underneath us; don't finalize.
                return None;
            }
            let (new_pid, new_epoch) = next_producer_identity(
                txnv,
                entry.producer_id,
                entry.producer_epoch,
                &self.producer_ids,
            );
            apply_complete_abort(&mut entry, new_pid, new_epoch, now_ms);
            entry.clone()
        };
        if let Err(e) = self.put(complete, txnv).await {
            warn!(tid, error = %e, "txn reaper: failed to persist CompleteAbort; skipping");
            return None;
        }
        Some(prepared.clone())
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    fn test_coordinator() -> TxnCoordinator {
        test_coordinator_with_partitions(50)
    }

    fn test_coordinator_with_partitions(num_partitions: i32) -> TxnCoordinator {
        TxnCoordinator::new(
            crabka_metadata::NodeId(1),
            Arc::new(PartitionRegistry::new()),
            Arc::new(crate::producer_id_manager::ProducerIdManager::new()),
            num_partitions,
        )
    }

    #[test]
    fn partition_for_maps_tid_via_murmur2_over_num_partitions() {
        // Canonical JVM murmur2 vectors (see `partitioner` tests) with N=50.
        // Pins the real mapping so a
        // constant `PartitionIndex(0)` (the Default) is caught: none of these
        // hash to 0.
        let coordinator = test_coordinator();
        check!(coordinator.partition_for("my-tid") == PartitionIndex(43));
        check!(coordinator.partition_for("producer-1") == PartitionIndex(45));
        check!(coordinator.partition_for("tx-orders-prod") == PartitionIndex(26));
    }

    #[test]
    fn nondefault_partition_count_changes_coordinator_routing() {
        let coordinator = test_coordinator_with_partitions(7);
        check!(
            coordinator.partition_for("my-tid") == PartitionIndex(partition_for_tid("my-tid", 7))
        );
    }

    fn entry(pid: i64, prev: i64) -> TxnEntry {
        let mut e = TxnEntry::new_empty("tid-a".into(), ProducerId(pid), 0, 60_000, 0);
        e.prev_producer_id = ProducerId(prev);
        e
    }

    #[test]
    fn evict_rolled_pid_drops_only_the_prior_id_on_a_roll() {
        let map: DashMap<ProducerId, String> = DashMap::new();
        map.insert(ProducerId(1000), "tid-a".into()); // the pre-roll mapping

        // A roll: new pid 2000, prev = 1000. The stale 1000 mapping is evicted;
        // put then inserts 2000 (mirrored here).
        TxnCoordinator::evict_rolled_pid(&map, &entry(2000, 1000));
        map.insert(ProducerId(2000), "tid-a".into());

        assert!(
            map.get(&ProducerId(1000)).is_none(),
            "stale pre-roll pid must be evicted"
        );
        check!(map.get(&ProducerId(2000)).map(|e| e.value().clone()) == Some("tid-a".into()));
    }

    #[test]
    fn evict_rolled_pid_is_noop_without_a_roll() {
        let map: DashMap<ProducerId, String> = DashMap::new();
        map.insert(ProducerId(1000), "tid-a".into());
        // Never rolled: prev == -1 → nothing evicted.
        TxnCoordinator::evict_rolled_pid(&map, &entry(1000, -1));
        assert!(map.get(&ProducerId(1000)).is_some());
        // prev == current (defensive): nothing evicted.
        TxnCoordinator::evict_rolled_pid(&map, &entry(1000, 1000));
        assert!(map.get(&ProducerId(1000)).is_some());
    }

    #[test]
    fn evict_rolled_pid_is_idempotent_after_the_id_is_gone() {
        let map: DashMap<ProducerId, String> = DashMap::new();
        map.insert(ProducerId(2000), "tid-a".into());
        // prev=1000 already absent → repeated evictions are harmless no-ops.
        TxnCoordinator::evict_rolled_pid(&map, &entry(2000, 1000));
        TxnCoordinator::evict_rolled_pid(&map, &entry(2000, 1000));
        assert!(map.get(&ProducerId(1000)).is_none());
        assert!(map.get(&ProducerId(2000)).is_some());
    }

    // ── Pure transition / guard helpers ───────────────────────────────────

    #[test]
    fn apply_prepare_abort_flips_state_and_stamps_time() {
        let mut e = entry(1000, -1);
        e.state = TxnState::Ongoing;
        e.last_update_ms = 1;
        apply_prepare_abort(&mut e, 999);
        check!(e.state == TxnState::PrepareAbort);
        check!(e.last_update_ms == 999);
    }

    #[test]
    fn apply_complete_abort_records_prev_only_on_a_pid_roll() {
        // No roll: same pid, epoch bumped → prev untouched.
        let mut e = entry(1000, -1);
        e.state = TxnState::PrepareAbort;
        e.producer_epoch = 4;
        apply_complete_abort(&mut e, ProducerId(1000), 5, 42);
        check!(e.state == TxnState::CompleteAbort);
        check!(e.producer_id == 1000);
        check!(e.producer_epoch == 5);
        check!(e.prev_producer_id == -1, "no roll must not set prev");
        check!(e.last_update_ms == 42);

        // Roll: fresh pid at epoch 0 → prior pid recorded as prev.
        let mut rolled = entry(1000, -1);
        rolled.state = TxnState::PrepareAbort;
        apply_complete_abort(&mut rolled, ProducerId(2000), 0, 43);
        check!(rolled.producer_id == 2000);
        check!(rolled.producer_epoch == 0);
        check!(
            rolled.prev_producer_id == 1000,
            "roll must record prior pid"
        );
    }

    #[test]
    fn complete_abort_guard_rejects_identity_or_state_drift() {
        let mut prepared = entry(1000, -1);
        prepared.producer_epoch = 7;
        prepared.state = TxnState::PrepareAbort;

        // Exact match → ok.
        let mut current = prepared.clone();
        assert!(complete_abort_guard_ok(&current, &prepared));

        // pid changed → reject.
        current = prepared.clone();
        current.producer_id = ProducerId(9999);
        assert!(!complete_abort_guard_ok(&current, &prepared));

        // epoch changed → reject.
        current = prepared.clone();
        current.producer_epoch = 8;
        assert!(!complete_abort_guard_ok(&current, &prepared));

        // state advanced past PrepareAbort → reject.
        current = prepared.clone();
        current.state = TxnState::CompleteAbort;
        assert!(!complete_abort_guard_ok(&current, &prepared));
    }

    // ── Orchestration loop, driven against a mock backend ─────────────────

    fn prepared_entry(tid: &str, pid: i64, epoch: i16) -> TxnEntry {
        let mut e = TxnEntry::new_empty(tid.to_owned(), ProducerId(pid), epoch, 60_000, 0);
        e.state = TxnState::PrepareAbort;
        e
    }

    #[tokio::test]
    async fn sweep_runs_full_three_phase_abort_for_an_expired_tid() {
        let mut backend = MockReaperBackend::new();
        backend
            .expect_is_coordinator_for()
            .withf(|t| t == "tid-a")
            .returning(|_| true);
        backend
            .expect_prepare_abort()
            .times(1)
            .returning(|t, _, _| Some(prepared_entry(t, 1000, 3)));
        backend
            .expect_dispatch_abort_markers()
            .times(1)
            .withf(|e| e.transactional_id == "tid-a" && e.state == TxnState::PrepareAbort)
            .returning(|_| ());
        backend
            .expect_complete_abort()
            .times(1)
            .withf(|e, _, _| e.transactional_id == "tid-a")
            .returning(|e, _, _| Some(e.clone()));

        let out = sweep_with_backend(
            &backend,
            vec!["tid-a".to_owned()],
            1_000,
            TxnVersion::Verified,
        )
        .await;
        check!(out == vec!["tid-a".to_owned()]);
    }

    #[tokio::test]
    async fn sweep_skips_tids_this_broker_does_not_coordinate() {
        let mut backend = MockReaperBackend::new();
        backend.expect_is_coordinator_for().returning(|_| false);
        // No prepare / dispatch / complete must be reached.
        backend.expect_prepare_abort().never();
        backend.expect_dispatch_abort_markers().never();
        backend.expect_complete_abort().never();

        let out = sweep_with_backend(
            &backend,
            vec!["tid-a".to_owned()],
            1_000,
            TxnVersion::Verified,
        )
        .await;
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn sweep_skips_tid_when_prepare_declines_and_does_not_dispatch() {
        let mut backend = MockReaperBackend::new();
        backend.expect_is_coordinator_for().returning(|_| true);
        // Not idle / persistence failed → None.
        backend
            .expect_prepare_abort()
            .times(1)
            .returning(|_, _, _| None);
        backend.expect_dispatch_abort_markers().never();
        backend.expect_complete_abort().never();

        let out = sweep_with_backend(
            &backend,
            vec!["tid-a".to_owned()],
            1_000,
            TxnVersion::Verified,
        )
        .await;
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn sweep_does_not_report_tid_when_complete_loses_the_race() {
        let mut backend = MockReaperBackend::new();
        backend.expect_is_coordinator_for().returning(|_| true);
        backend
            .expect_prepare_abort()
            .returning(|t, _, _| Some(prepared_entry(t, 1000, 3)));
        // Markers still fan out (Phase 2 ran)...
        backend
            .expect_dispatch_abort_markers()
            .times(1)
            .returning(|_| ());
        // ...but Phase 3 lost the race → not finalized, not reported.
        backend
            .expect_complete_abort()
            .times(1)
            .returning(|_, _, _| None);

        let out = sweep_with_backend(
            &backend,
            vec!["tid-a".to_owned()],
            1_000,
            TxnVersion::Verified,
        )
        .await;
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn sweep_aborts_each_expired_tid_independently() {
        let mut backend = MockReaperBackend::new();
        // tid-a coordinated + expired; tid-b not coordinated.
        backend
            .expect_is_coordinator_for()
            .returning(|t| t == "tid-a");
        backend
            .expect_prepare_abort()
            .withf(|t, _, _| t == "tid-a")
            .returning(|t, _, _| Some(prepared_entry(t, 1000, 3)));
        backend.expect_dispatch_abort_markers().returning(|_| ());
        backend
            .expect_complete_abort()
            .returning(|e, _, _| Some(e.clone()));

        let out = sweep_with_backend(
            &backend,
            vec!["tid-a".to_owned(), "tid-b".to_owned()],
            1_000,
            TxnVersion::Verified,
        )
        .await;
        check!(out == vec!["tid-a".to_owned()]);
    }
}
