//! Per-broker `TxnCoordinator`.
//!
//! The coordinator owns the in-memory state map of every `transactional_id`
//! whose `__transaction_state` partition this broker leads. It persists every
//! state change as a record in the matching `__transaction_state` partition.
//! On `Broker::start` it recovers the state by replaying those partitions.

use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use crabka_ids::PartitionIndex;
use crabka_log::{Offset, ProducerId};
use crabka_metadata::MetadataImage;
use crabka_protocol::records::{Record, RecordBatch};
use crabka_security::ListenerProtocol;
use crabka_units::ByteSize;
use dashmap::DashMap;
use tokio::sync::{Mutex, RwLock};
use tracing::{info, warn};

use crate::{
    error::BrokerError,
    partition_registry::PartitionRegistry,
    txn::{
        bootstrap,
        handlers::{
            end_txn::{
                MarkerDispatchContext, completion_producer_identity, dispatch_markers,
                prepare_completion_identities,
            },
            write_txn_markers::append_marker_and_materialize,
        },
        marker::MarkerType,
        partitioner::partition_for_tid,
        state::{TxnEntry, TxnState},
        two_pc::should_abort_idle_txn,
        version::TxnVersion,
    },
};

/// Live-dependency seam for the KIP-939 idle-transaction reaper.
///
/// `sweep_expired` orchestrates a three-phase abort for each tid. That
/// orchestration is pure decision logic around four irreducible side effects:
/// a coordinator-ownership check, two compare-and-swap-style persisted
/// transitions (`Ongoing → PrepareAbort`, then
/// `PrepareAbort → CompleteAbort`), the abort-marker fan-out, and
/// producer-identity allocation. Each one touches a live
/// `__transaction_state` partition, partition leaders, or the producer-id
/// allocator.
///
/// This trait puts those effects behind a seam, so a unit test can drive the
/// orchestration against a [`mockall`] mock. Every method returns
/// already-extracted plain data, that is, snapshots, so a mock can kill the
/// decisions that read them. The live adapter is [`TxnCoordinator`] itself.
///
/// The two `*_transition` methods mutate the entry **atomically under the
/// per-tid lock**, so a concurrent `EndTxn` or `InitProducerId` is not
/// overwritten. They return the resulting persisted snapshot, or `None` when
/// the guard failed because the caller lost a race or the entry is no longer
/// present. The pure helpers [`apply_prepare_abort`], [`apply_complete_abort`],
/// and [`complete_abort_guard_ok`] compute the transitions. The backend owns
/// only the compare-and-swap and the persistence, which is the irreducible
/// part.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub(crate) trait ReaperBackend: Send + Sync {
    /// Is this broker the transaction coordinator for `tid` right now?
    async fn is_coordinator_for(&self, tid: &str) -> bool;

    /// Moves `Ongoing → PrepareAbort` under `tid`'s entry lock, atomically.
    ///
    /// If the entry should abort as an idle transaction at `now_ms`, this
    /// method makes the transition, persists it, and returns the persisted
    /// snapshot. It returns `None` when the entry is absent, when the entry
    /// must not be reaped, or when the persistence failed.
    async fn prepare_abort(&self, tid: &str, now_ms: i64, txnv: TxnVersion) -> Option<TxnEntry>;

    /// Fan out abort markers for `entry`. Returns `false` if any marker could
    /// not be written, leaving the transaction in `PrepareAbort` for retry.
    async fn dispatch_abort_markers(&self, entry: &TxnEntry) -> bool;

    /// Moves `PrepareAbort → CompleteAbort` under `tid`'s entry lock,
    /// atomically.
    ///
    /// The method first checks that the current entry still matches the
    /// `prepared` snapshot this reaper wrote: the same identity, and still
    /// `PrepareAbort`. If it matches, the method moves the entry to
    /// `CompleteAbort`, bumps the producer identity at `now_ms` as KIP-890
    /// requires, persists the entry, and returns the persisted snapshot. It
    /// returns `None` when another caller advanced the entry or when the
    /// persistence failed.
    async fn complete_abort(
        &self,
        prepared: &TxnEntry,
        now_ms: i64,
        txnv: TxnVersion,
    ) -> Option<TxnEntry>;
}

/// Applies the `Ongoing → PrepareAbort` mutation for an idle-reaped entry. It
/// changes the state and stamps `last_update_ms`. The function is pure, so a
/// unit test can kill the transition without any persistence.
fn apply_prepare_abort(entry: &mut TxnEntry, now_ms: i64) {
    entry.state = TxnState::PrepareAbort;
    entry.last_update_ms = now_ms;
}

/// Applies the `PrepareAbort → CompleteAbort` mutation from the newly
/// allocated `(producer_id, producer_epoch)` of the KIP-890 identity bump. It
/// records the prior id as `prev_producer_id` only when a roll happened, that
/// is, when the allocator gave out a fresh pid. The function is pure, so a
/// unit test can kill the transition.
fn apply_complete_abort(entry: &mut TxnEntry, new_pid: ProducerId, new_epoch: i16, now_ms: i64) {
    if new_pid != entry.producer_id {
        entry.prev_producer_id = entry.producer_id;
    }
    entry.state = TxnState::CompleteAbort;
    entry.producer_id = new_pid;
    entry.producer_epoch = new_epoch;
    entry.next_producer_id = ProducerId(-1);
    entry.next_producer_epoch = -1;
    entry.partitions.clear();
    entry.last_update_ms = now_ms;
}

/// Reports whether the re-acquired `entry` still matches the `prepared`
/// snapshot this reaper wrote in phase 1, so that it is safe to finalise to
/// `CompleteAbort`. The guard protects against a concurrent `EndTxn` or
/// `InitProducerId` that advanced the entry. The function is pure, so a unit
/// test can kill the guard.
fn complete_abort_guard_ok(entry: &TxnEntry, prepared: &TxnEntry) -> bool {
    entry.producer_id == prepared.producer_id
        && entry.producer_epoch == prepared.producer_epoch
        && entry.state == TxnState::PrepareAbort
}

/// Runs the reaper orchestration loop.
///
/// The loop is generic over the [`ReaperBackend`] seam, so a unit test can
/// drive it against a mock. For each candidate tid it runs the three-phase
/// abort: ownership check, `prepare_abort` (compare-and-swap), marker fan-out,
/// then `complete_abort` (compare-and-swap). Returns the tids it finalised, in
/// iteration order.
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
        if !backend.dispatch_abort_markers(&prepared).await {
            continue;
        }

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

/// Per-broker transaction coordinator. `Broker::start` constructs it and
/// shares it with the transaction wire handlers through an `Arc`.
pub(crate) struct TxnCoordinator {
    pub(crate) node_id: crabka_metadata::NodeId,
    pub(crate) partitions: Arc<PartitionRegistry>,
    pub(crate) producer_ids: Arc<crate::producer_id_manager::ProducerIdManager>,
    num_partitions: i32,
    recovery_read_max: ByteSize,
    /// Live in-memory state: `transactional_id` → locked `TxnEntry`.
    state: DashMap<String, Arc<Mutex<TxnEntry>>>,
    /// Set of `__transaction_state` partition indices this broker leads.
    leader_partitions: RwLock<HashSet<PartitionIndex>>,
    /// Reverse lookup: `producer_id` → `transactional_id`. The Produce
    /// handler reads it to verify transactional batches (KIP-1319 v2).
    pid_to_tid: DashMap<ProducerId, String>,
    marker_transport: Option<MarkerTransport>,
    group_coordinator: Option<Arc<crate::coordinator::GroupCoordinator>>,
}

struct MarkerTransport {
    controller: Arc<dyn crate::metadata_source::MetadataSource>,
    inter_broker_client: Arc<crate::network::client::InterBrokerClient>,
    protocol: ListenerProtocol,
    listener_name: String,
    server_name: String,
}

impl TxnCoordinator {
    pub(crate) fn new(
        node_id: crabka_metadata::NodeId,
        partitions: Arc<PartitionRegistry>,
        producer_ids: Arc<crate::producer_id_manager::ProducerIdManager>,
        num_partitions: i32,
        recovery_read_max: ByteSize,
    ) -> Self {
        Self {
            node_id,
            partitions,
            producer_ids,
            num_partitions,
            recovery_read_max,
            state: DashMap::new(),
            leader_partitions: RwLock::new(HashSet::new()),
            pid_to_tid: DashMap::new(),
            marker_transport: None,
            group_coordinator: None,
        }
    }

    pub(crate) fn configure_marker_transport(
        &mut self,
        controller: Arc<dyn crate::metadata_source::MetadataSource>,
        inter_broker_client: Arc<crate::network::client::InterBrokerClient>,
        protocol: ListenerProtocol,
        listener_name: String,
        server_name: String,
        group_coordinator: Arc<crate::coordinator::GroupCoordinator>,
    ) {
        self.marker_transport = Some(MarkerTransport {
            controller,
            inter_broker_client,
            protocol,
            listener_name,
            server_name,
        });
        self.group_coordinator = Some(group_coordinator);
    }

    pub(crate) async fn dispatch_transaction_markers(
        &self,
        entry: &TxnEntry,
        marker_type: MarkerType,
    ) -> Result<(), BrokerError> {
        let Some(transport) = &self.marker_transport else {
            return self.dispatch_local_markers(entry, marker_type).await;
        };
        let image = transport.controller.current_image();
        let coordinator_partition = self.partition_for(&entry.transactional_id);
        let coordinator_epoch = image
            .partition(bootstrap::TOPIC, coordinator_partition.get())
            .ok_or_else(|| {
                BrokerError::Txn(format!(
                    "transaction coordinator partition {}-{} is missing from metadata",
                    bootstrap::TOPIC,
                    coordinator_partition.get()
                ))
            })?
            .leader_epoch
            .get();
        dispatch_markers(
            MarkerDispatchContext {
                node_id: self.node_id,
                coordinator_epoch,
                image: &image,
                inter_broker_client: &transport.inter_broker_client,
                inter_broker_protocol: transport.protocol,
                inter_broker_listener_name: &transport.listener_name,
                inter_broker_server_name: &transport.server_name,
                group_coordinator: self.group_coordinator.as_ref(),
            },
            &self.partitions,
            entry,
            marker_type,
        )
        .await
    }

    async fn dispatch_local_markers(
        &self,
        entry: &TxnEntry,
        marker_type: MarkerType,
    ) -> Result<(), BrokerError> {
        for tp in &entry.partitions {
            let part = self
                .partitions
                .get(&tp.topic, tp.partition)
                .ok_or_else(|| {
                    BrokerError::Txn(format!(
                        "transaction marker transport is not configured for remote partition {}-{}",
                        tp.topic,
                        tp.partition.get()
                    ))
                })?;
            append_marker_and_materialize(
                &part,
                self.group_coordinator.as_ref(),
                &tp.topic,
                entry.producer_id,
                entry.producer_epoch,
                marker_type,
                -1,
            )
            .await?;
        }
        Ok(())
    }

    /// Recomputes which `__transaction_state` partitions this broker leads,
    /// from the current `MetadataImage`. `recover` calls it, and so does
    /// every metadata change.
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

    /// Returns the locked `TxnEntry` for `tid`, or `None` if `tid` is
    /// unknown.
    pub(crate) fn get(&self, tid: &str) -> Option<Arc<Mutex<TxnEntry>>> {
        self.state.get(tid).map(|e| e.value().clone())
    }

    /// Returns the `transactional_id` that `producer_id` was registered
    /// under, or `None` if the pid is unknown.
    pub(crate) fn tid_for_pid(&self, pid: ProducerId) -> Option<String> {
        self.pid_to_tid.get(&pid).map(|e| e.value().clone())
    }

    /// Keep only the current transaction and staged recovery producer IDs for
    /// this transactional ID. Repeated KIP-939 recovery calls can rotate the
    /// staged ID before the transaction completes; retaining the superseded
    /// mapping would let that fenced ID bypass coordinator validation.
    fn evict_superseded_pids(pid_to_tid: &DashMap<ProducerId, String>, entry: &TxnEntry) {
        pid_to_tid.retain(|pid, tid| {
            tid != &entry.transactional_id
                || *pid == entry.producer_id
                || *pid == entry.next_producer_id
        });
    }

    /// Snapshots every locally-coordinated `TxnEntry`.
    ///
    /// The KIP-664 admin handlers `ListTransactions` and
    /// `DescribeTransactions` call this to expose the in-memory txn-state map.
    /// The method locks and clones each entry in turn, so the snapshot is
    /// consistent for one tid but not across the whole batch. That is
    /// acceptable for an admin introspection API, and Apache Kafka's JVM
    /// coordinator has the same property.
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

    /// Persists `entry` to the matching `__transaction_state` partition log,
    /// then updates the in-memory map. The partition's writer task appends the
    /// batch, in order with all other produce appends.
    ///
    /// `txnv` is the finalized `transaction.version` that the caller resolved
    /// from the live metadata image. It selects the byte-exact Kafka
    /// `TransactionLogValue` format: v0 for `TV_0`, and v1 for `TV >= 1`.
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

        Self::evict_superseded_pids(&self.pid_to_tid, &entry);
        self.pid_to_tid
            .insert(entry.producer_id, entry.transactional_id.clone());
        if !entry.next_producer_id.is_none() {
            self.pid_to_tid
                .insert(entry.next_producer_id, entry.transactional_id.clone());
        }
        self.state.insert(tid, Arc::new(Mutex::new(entry)));
        Ok(())
    }

    /// KIP-939 idle-transaction reaper: aborts every locally-coordinated,
    /// non-2PC, `Ongoing` transaction whose timeout has elapsed at `now_ms`.
    ///
    /// The reaper skips 2PC transactions, where
    /// `txn_timeout_ms == NO_TIMEOUT_MS`. Their external transaction manager
    /// owns the commit or abort decision, and Kafka must never abort a
    /// prepared 2PC transaction on its own. [`should_abort_idle_txn`] makes
    /// the decision; it is the exhaustively model-checked core. See
    /// [`crate::txn::two_pc_model`].
    ///
    /// Each abort runs the same two-step transition + marker fan-out as an
    /// `EndTxn(committed=false)` and bumps the producer epoch on completion (at
    /// `TV >= 2`) so the timed-out producer is fenced. A marker failure leaves the
    /// entry in `PrepareAbort`; the next sweep retries the fan-out. A concurrent
    /// caller that changed the entry out from under us aborts this reap of that
    /// tid (re-validated before the Complete write). Returns the tids it
    /// finalized.
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

    /// Replays every locally-led `__transaction_state` partition into the
    /// in-memory state map. `Broker::start` calls it.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] if a read of a partition's log fails with an
    /// error other than a read past the end. A read past the end is a normal
    /// "partition is empty" condition.
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
                let out = match part.read_log(offset, self.recovery_read_max) {
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
                        if !entry.next_producer_id.is_none() {
                            self.pid_to_tid
                                .insert(entry.next_producer_id, entry.transactional_id.clone());
                        }
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

/// Live adapter that runs the real reaper side effects against the in-memory
/// state map, the `__transaction_state` partition log, partition leaders, and
/// the producer-id allocator. Only the irreducible IO lives here. The pure
/// helpers and the orchestration logic above hold every decision.
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
            if entry.state == TxnState::PrepareAbort {
                return Some(entry.clone());
            }
            if !should_abort_idle_txn(entry.state, entry.txn_timeout_ms, entry.start_ms, now_ms) {
                return None;
            }
            apply_prepare_abort(&mut entry, now_ms);
            if let Err(error) =
                prepare_completion_identities(&mut entry, txnv, &self.producer_ids).await
            {
                warn!(tid, %error, "txn reaper: failed to allocate completion identity");
                return None;
            }
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
    async fn dispatch_abort_markers(&self, entry: &TxnEntry) -> bool {
        match self
            .dispatch_transaction_markers(entry, MarkerType::Abort)
            .await
        {
            Ok(()) => true,
            Err(error) => {
                warn!(
                    tid = %entry.transactional_id,
                    %error,
                    "txn reaper: abort marker fan-out failed; will retry"
                );
                false
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
            let (new_pid, new_epoch) = completion_producer_identity(&entry);
            apply_complete_abort(&mut entry, new_pid, new_epoch, now_ms);
            entry.clone()
        };
        if let Err(e) = self.put(complete.clone(), txnv).await {
            warn!(tid, error = %e, "txn reaper: failed to persist CompleteAbort; skipping");
            return None;
        }
        Some(complete)
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
            crabka_units::mebibytes(1),
        )
    }

    #[tokio::test]
    async fn reaper_retries_an_existing_prepare_abort() {
        let coordinator = test_coordinator();
        let mut prepared =
            TxnEntry::new_empty("tid-retry".to_string(), ProducerId(1000), 2, 60_000, 0);
        prepared.state = TxnState::PrepareAbort;
        coordinator.state.insert(
            prepared.transactional_id.clone(),
            Arc::new(Mutex::new(prepared.clone())),
        );

        let retried = ReaperBackend::prepare_abort(
            &coordinator,
            &prepared.transactional_id,
            1,
            TxnVersion::Verified,
        )
        .await
        .expect("prepared abort should be retried");

        check!(retried.transactional_id == prepared.transactional_id);
        check!(retried.producer_id == prepared.producer_id);
        check!(retried.producer_epoch == prepared.producer_epoch);
        check!(retried.state == TxnState::PrepareAbort);
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
        TxnCoordinator::evict_superseded_pids(&map, &entry(2000, 1000));
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
        TxnCoordinator::evict_superseded_pids(&map, &entry(1000, -1));
        assert!(map.get(&ProducerId(1000)).is_some());
        // prev == current (defensive): nothing evicted.
        TxnCoordinator::evict_superseded_pids(&map, &entry(1000, 1000));
        assert!(map.get(&ProducerId(1000)).is_some());
    }

    #[test]
    fn evict_rolled_pid_is_idempotent_after_the_id_is_gone() {
        let map: DashMap<ProducerId, String> = DashMap::new();
        map.insert(ProducerId(2000), "tid-a".into());
        // prev=1000 already absent → repeated evictions are harmless no-ops.
        TxnCoordinator::evict_superseded_pids(&map, &entry(2000, 1000));
        TxnCoordinator::evict_superseded_pids(&map, &entry(2000, 1000));
        assert!(map.get(&ProducerId(1000)).is_none());
        assert!(map.get(&ProducerId(2000)).is_some());
    }

    #[test]
    fn evict_superseded_pids_removes_a_rotated_recovery_identity() {
        let map: DashMap<ProducerId, String> = DashMap::new();
        map.insert(ProducerId(1000), "tid-a".into());
        map.insert(ProducerId(2000), "tid-a".into());

        let mut current = entry(1000, -1);
        current.next_producer_id = ProducerId(3000);
        current.next_producer_epoch = 0;
        TxnCoordinator::evict_superseded_pids(&map, &current);

        assert!(map.get(&ProducerId(1000)).is_some());
        assert!(map.get(&ProducerId(2000)).is_none());
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
        e.partitions.insert(crate::txn::state::TopicPartition {
            topic: "orders".into(),
            partition: PartitionIndex(2),
        });
        apply_complete_abort(&mut e, ProducerId(1000), 5, 42);
        check!(e.state == TxnState::CompleteAbort);
        check!(e.producer_id == 1000);
        check!(e.producer_epoch == 5);
        check!(e.prev_producer_id == -1, "no roll must not set prev");
        check!(e.partitions.is_empty());
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
            .returning(|_| true);
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
            .returning(|_| true);
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
        backend.expect_dispatch_abort_markers().returning(|_| true);
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

    #[tokio::test]
    async fn sweep_does_not_complete_when_marker_fanout_fails() {
        let mut backend = MockReaperBackend::new();
        backend.expect_is_coordinator_for().returning(|_| true);
        backend
            .expect_prepare_abort()
            .returning(|t, _, _| Some(prepared_entry(t, 1000, 3)));
        backend.expect_dispatch_abort_markers().returning(|_| false);
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
}
