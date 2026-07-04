use std::{collections::HashMap, sync::Arc};

use tokio::sync::Mutex as TokioMutex;

use crate::{
    error::StreamsClientError,
    membership::StreamsAssignment,
    runtime::{
        eos::{ProcessingGuarantee, StreamsGroupMeta, TransactionalProducer},
        io::{BeginTxnGate, OffsetStore, RecordFetcher, RecordProducer},
        iq::{IqRequest, answer_iq},
        task::{StreamTask, TaskRole},
    },
    topology::BuiltTopology,
};

pub(crate) struct StreamThread {
    tasks: HashMap<(String, i32), StreamTask>,
    /// Shared fetcher reference kept for restore (replaying changelog on task creation).
    fetcher: Arc<dyn RecordFetcher>,
    /// Storage backend to use when instantiating new task graphs.
    backend: crate::store::backend::StoreBackend,
    /// Application ID passed to `instantiate` for changelog-name derivation and
    /// backend path construction.
    application_id: String,
    /// The shared, fully-replicated global stores for this app. Built + bootstrapped
    /// once from the topology's global store factories (on the first assignment that
    /// has work), then lent by `Arc` clone into every task's graph so a
    /// stream-globaltable join reads the same global state. Empty (default) when the
    /// topology declares no `GlobalKTable`.
    globals: crate::runtime::global::GlobalStateManager,
    /// Whether `globals` has been built + bootstrapped yet. Guards the one-time
    /// lazy build at the top of `apply_assignment`.
    globals_ready: bool,
    /// Per-`(global topic, partition)` next-offset, seeded by the bootstrap read and
    /// advanced by each `poll_all` live-update pass. Empty when the topology declares
    /// no `GlobalKTable`.
    global_offsets: std::collections::HashMap<(String, i32), i64>,
    /// Wall-clock source driving wall-clock punctuation between polls. Defaults to
    /// `SystemClock`; tests inject a `ManualClock` via `with_clock` for determinism.
    clock: Arc<dyn crate::runtime::clock::Clock>,
    /// Delivery guarantee for this thread. Set by `apply_assignment`; defaults to
    /// at-least-once until the first assignment arrives.
    guarantee: ProcessingGuarantee,
    /// The EOS-v2 transactional producer (the same object the tasks `send`
    /// through, viewed as a `TransactionalProducer`). `None` under at-least-once.
    txn: Option<Arc<dyn TransactionalProducer>>,
    /// Whether `init_transactions` has run (one-time, on the first EOS assignment).
    initialized: bool,
    /// Whether a transaction is currently open (`begin_transaction` called, not yet
    /// committed/aborted). Drives the begin-on-first-poll / commit barrier.
    in_txn: bool,
    /// Record-cache budget (JVM `statestore.cache.max.bytes`) threaded into each
    /// task graph at `instantiate`. `0` disables caching.
    cache_max_bytes: i64,
}

impl StreamThread {
    pub fn new(
        fetcher: Arc<dyn RecordFetcher>,
        backend: crate::store::backend::StoreBackend,
        application_id: String,
        cache_max_bytes: i64,
    ) -> Self {
        Self {
            tasks: HashMap::new(),
            fetcher,
            backend,
            application_id,
            cache_max_bytes,
            globals: crate::runtime::global::GlobalStateManager::default(),
            globals_ready: false,
            global_offsets: std::collections::HashMap::new(),
            clock: Arc::new(crate::runtime::clock::SystemClock),
            guarantee: ProcessingGuarantee::AtLeastOnce,
            txn: None,
            initialized: false,
            in_txn: false,
        }
    }

    /// Test-only: swap in a deterministic clock (e.g. `ManualClock`) so wall-clock
    /// punctuation can be driven without real time passing.
    #[cfg(test)]
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn crate::runtime::clock::Clock>) -> Self {
        self.clock = clock;
        self
    }

    #[cfg(test)]
    #[must_use]
    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }

    /// Test-only: typed read from a named KV store on the task at `key`.
    #[cfg(test)]
    async fn task_store_get_i64(
        &mut self,
        task: &(String, i32),
        store: &str,
        k: &String,
    ) -> Option<i64> {
        self.tasks.get_mut(task)?.store_get_i64(store, k).await
    }

    /// Test-only: whether the task at `key` has pending (uncommitted) offsets.
    #[cfg(test)]
    fn task_has_pending(&self, task: &(String, i32)) -> bool {
        self.tasks
            .get(task)
            .is_some_and(|t| !t.pending_offsets().is_empty())
    }

    /// Reconcile tasks to `assignment`. Reconciles active, standby, and warmup tasks.
    ///
    /// `guarantee` + `txn` configure the EOS commit path: under
    /// [`ProcessingGuarantee::ExactlyOnceV2`] the same producer object is also
    /// passed as `txn` (a [`TransactionalProducer`] view), and the first EOS
    /// assignment runs `init_transactions` once (fencing any zombie).
    #[allow(clippy::too_many_lines)]
    #[tracing::instrument(
        name = "streams.thread.apply_assignment",
        level = "info",
        skip_all,
        fields(
            application_id = %self.application_id,
            guarantee = ?guarantee,
            active = assignment.active.len(),
            standby = assignment.standby.len(),
            warmup = assignment.warmup.len(),
        ),
        err,
    )]
    pub async fn apply_assignment(
        &mut self,
        assignment: &StreamsAssignment,
        topology: &BuiltTopology,
        producer: &Arc<dyn RecordProducer>,
        store: &Arc<dyn OffsetStore>,
        guarantee: ProcessingGuarantee,
        txn: Option<Arc<dyn TransactionalProducer>>,
    ) -> Result<(), StreamsClientError> {
        self.guarantee = guarantee;
        self.txn = txn;
        // One-time `init_transactions` on the first EOS assignment (bumps the
        // producer epoch, fencing a zombie of the same transactional id).
        if self.guarantee == ProcessingGuarantee::ExactlyOnceV2 && !self.initialized {
            let txn = self
                .txn
                .as_ref()
                .expect("EOS requires a transactional producer");
            txn.init_transactions().await?;
            self.initialized = true;
        }

        // Lazily build + bootstrap the shared global store(s) exactly once, before
        // any task processes. Kafka blocks task start until the global store is
        // ready, so we drain every partition of each global source topic here.
        if !self.globals_ready {
            let factories = topology.global_store_factories();
            if !factories.is_empty() {
                self.globals = crate::runtime::global::GlobalStateManager::build(
                    factories,
                    topology.global_store_topics(),
                    &self.backend,
                    &self.application_id,
                )
                .await;
                // Bootstrap from all partitions BEFORE any task processes
                // (bootstrap-before-process is the required behavior); the returned
                // resume offsets seed the live-update poll in `poll_all`.
                self.global_offsets = self.globals.bootstrap(&*self.fetcher).await?;
            }
            self.globals_ready = true;
        }

        // Desired (subtopology_id, partition) -> (TaskRole, &TaskAssignment).
        let mut desired: HashMap<(String, i32), (TaskRole, &crate::membership::TaskAssignment)> =
            HashMap::new();
        for ta in &assignment.active {
            for &p in &ta.partitions {
                desired.insert((ta.subtopology_id.clone(), p), (TaskRole::Active, ta));
            }
        }
        for ta in &assignment.standby {
            for &p in &ta.partitions {
                desired.insert((ta.subtopology_id.clone(), p), (TaskRole::Standby, ta));
            }
        }
        for ta in &assignment.warmup {
            for &p in &ta.partitions {
                desired.insert((ta.subtopology_id.clone(), p), (TaskRole::Warmup, ta));
            }
        }

        // Drop removed: close processors, commit, then drop.
        let to_remove: Vec<(String, i32)> = self
            .tasks
            .keys()
            .filter(|k| !desired.contains_key(*k))
            .cloned()
            .collect();
        for k in to_remove {
            if let Some(mut t) = self.tasks.remove(&k).filter(|t| t.role == TaskRole::Active) {
                t.close_processors().await;
                t.commit().await?;
            }
        }

        // Transition existing tasks whose role has changed.
        for (key, &(desired_role, _ta)) in &desired {
            if let Some(task) = self.tasks.get_mut(key).filter(|t| t.role != desired_role) {
                let old_role = task.role;
                if desired_role == TaskRole::Active {
                    // Promotion: catch up remaining restore, seek source partitions, and init processors.
                    task.restore(&*self.fetcher).await?;
                    task.seek_to_start().await?;
                    task.init().await?;
                } else if old_role == TaskRole::Active {
                    // Demotion: close processors and commit offsets.
                    task.close_processors().await;
                    task.commit().await?;
                }
                task.role = desired_role;
            }
        }

        // Add new.
        for (key, &(desired_role, ta)) in &desired {
            if self.tasks.contains_key(key) {
                continue;
            }
            let mut graph = topology
                .instantiate(&self.backend, &self.application_id, self.cache_max_bytes)
                .await
                .map_err(|e| StreamsClientError::Runtime(e.to_string()))?;
            // Lend the shared, bootstrapped global manager to this task's graph so a
            // stream-globaltable join reads the same fully-replicated global state.
            graph.globals = self.globals.clone();
            let sources: Vec<crate::membership::TopicPartition> = ta
                .source_topic_partitions
                .iter()
                .filter(|tp| tp.partition == key.1)
                .cloned()
                .collect();
            let mut task = StreamTask::new(
                key.0.clone(),
                graph,
                sources,
                Arc::clone(producer),
                Arc::clone(store),
                desired_role,
                self.guarantee,
            );
            if desired_role == TaskRole::Active {
                // Seek positions to committed offsets (or earliest) BEFORE restore so
                // that normal processing knows where to start after restore completes.
                task.seek_to_start().await?;
                // Restore state stores from changelog, then initialise processors.
                task.restore(&*self.fetcher).await?;
                task.init().await?;
            }
            self.tasks.insert(key.clone(), task);
        }
        Ok(())
    }

    /// Abort the in-flight txn and roll back every task to the last committed
    /// state (rewind source offsets, wipe stores, re-restore from the committed
    /// changelog). Called on any error during an EOS process/commit cycle.
    //
    // All EOS-cycle errors are treated as retryable abort+rollback here; the
    // fenced-fatal distinction (a `ProducerFenced` must shut the thread down, not
    // retry) is a follow-up.
    #[tracing::instrument(
        name = "streams.thread.abort_and_rollback",
        level = "info",
        skip_all,
        fields(tasks = self.tasks.len()),
        err,
    )]
    async fn abort_and_rollback(&mut self) -> Result<(), StreamsClientError> {
        if let Some(txn) = self.txn.as_ref() {
            let _ = txn.abort_transaction().await;
        }
        self.in_txn = false;
        let fetcher = Arc::clone(&self.fetcher);
        for task in self.tasks.values_mut() {
            task.rollback(&*fetcher).await?;
        }
        Ok(())
    }

    #[tracing::instrument(
        name = "streams.thread.poll_all",
        level = "debug",
        skip_all,
        fields(guarantee = ?self.guarantee, tasks = self.tasks.len()),
        err,
    )]
    pub async fn poll_all(
        &mut self,
        fetcher: &dyn RecordFetcher,
        tracker: &Arc<TokioMutex<crate::membership::TaskOffsetTracker>>,
    ) -> Result<(), StreamsClientError> {
        // Apply any new global-topic records to the shared global store(s) before
        // processing, so stream-globaltable joins see live updates (Kafka keeps the
        // global store current after the initial bootstrap). No-op without globals.
        if !self.global_offsets.is_empty() {
            self.globals
                .poll_once(fetcher, &mut self.global_offsets)
                .await?;
        }
        match self.guarantee {
            ProcessingGuarantee::AtLeastOnce => {
                for task in self.tasks.values_mut() {
                    task.process_once(fetcher, None).await?;
                }
            }
            ProcessingGuarantee::ExactlyOnceV2 => {
                // EOS: the transaction is opened lazily, on the FIRST produced
                // record of the interval (via the begin-gate handed to each
                // task). An interval that fetches no records opens no
                // transaction, so `commit_all` is a no-op (no empty-txn churn on
                // an idle app). Idempotent across polls within a commit interval
                // — `in_txn` stays set until `commit_all`. Any error mid-begin or
                // mid-process aborts the txn and rolls every task back to the last
                // commit; the cycle is then re-begun on the next poll (so
                // `poll_all` returns Ok).
                let res = self.eos_begin_and_process(fetcher).await;
                if res.is_err() {
                    self.abort_and_rollback().await?;
                    return Ok(());
                }
            }
        }
        // Wall-clock punctuation tick (independent of the delivery guarantee): read
        // the clock once, then fire every task's due WALL_CLOCK_TIME punctuators.
        // Forwarded records go through each task's producer — under EOS they join
        // the interval's open transaction (committed by the next `commit_all`).
        let now = self.clock.now_ms();
        for task in self.tasks.values_mut() {
            if task.role == TaskRole::Active {
                task.punctuate_wall_clock(now).await?;
            }
        }

        // Update task offsets in the shared tracker.
        let mut task_offsets = std::collections::HashMap::new();
        let mut task_end_offsets = std::collections::HashMap::new();
        for (key, task) in &mut self.tasks {
            let (curr, end) = task.compute_changelog_offsets().await?;
            task_offsets.insert(key.clone(), curr);
            task_end_offsets.insert(key.clone(), end);
        }
        {
            let mut lock = tracker.lock().await;
            lock.task_offsets = task_offsets;
            lock.task_end_offsets = task_end_offsets;
        }

        Ok(())
    }

    /// EOS begin-on-first-record + per-task process, captured so `poll_all` can
    /// turn any `Err` into an abort + rollback.
    ///
    /// The transaction is NOT begun up front. Instead each task is handed an
    /// [`EosBeginGate`] that begins the transaction lazily, right before the
    /// task's first produced record of the interval. If no task fetches any
    /// records the gate is never tripped, so the interval opens no transaction
    /// (and `commit_all` becomes a no-op) — matching the JVM's "gate on records
    /// processed since last commit" behaviour.
    #[tracing::instrument(
        name = "streams.thread.eos_begin_and_process",
        level = "debug",
        skip_all,
        fields(tasks = self.tasks.len(), in_txn = self.in_txn),
        err,
    )]
    async fn eos_begin_and_process(
        &mut self,
        fetcher: &dyn RecordFetcher,
    ) -> Result<(), StreamsClientError> {
        let txn = Arc::clone(self.txn.as_ref().expect("EOS txn producer"));
        let mut gate = EosBeginGate {
            txn,
            begun: self.in_txn,
        };
        let res: Result<(), StreamsClientError> = async {
            for task in self.tasks.values_mut() {
                if task.role == TaskRole::Active {
                    task.process_once(fetcher, Some(&mut gate)).await?;
                } else {
                    task.restore_step(fetcher).await?;
                }
            }
            Ok(())
        }
        .await;
        // Reflect any lazily-opened transaction back onto the thread so
        // `commit_all` / `abort_and_rollback` see it, even on the error path.
        self.in_txn = gate.begun;
        res
    }

    /// EOS commit barrier: fold every task's pending source offsets into a single
    /// `send_offsets_to_transaction`, then `commit_transaction`. Captured so
    /// `commit_all` can turn any `Err` into an abort + rollback. Does NOT clear
    /// pending (the caller does that only on success).
    #[tracing::instrument(
        name = "streams.thread.eos_send_offsets_and_commit",
        level = "debug",
        skip_all,
        fields(tasks = self.tasks.len()),
        err,
    )]
    async fn eos_send_offsets_and_commit(
        &mut self,
        meta: Option<&StreamsGroupMeta>,
    ) -> Result<(), StreamsClientError> {
        let txn = self.txn.as_ref().expect("EOS txn producer");
        let mut offsets = Vec::new();
        for task in self.tasks.values() {
            offsets.extend(task.pending_offsets());
        }
        let meta = meta.expect("EOS commit requires group metadata");
        txn.send_offsets_to_transaction(&offsets, meta).await?;
        txn.commit_transaction().await?;
        Ok(())
    }

    /// Commit advanced offsets.
    ///
    /// At-least-once: per-task `flush` + offset commit (`meta` ignored).
    /// Exactly-once-v2: fold every task's pending source offsets into a single
    /// `send_offsets_to_transaction`, then `commit_transaction` atomically, and
    /// clear the tasks' pending offsets. Requires `meta` (the streams group
    /// metadata). A no-op when no transaction is open (nothing produced since the
    /// last commit).
    #[tracing::instrument(
        name = "streams.thread.commit_all",
        level = "info",
        skip_all,
        fields(guarantee = ?self.guarantee, tasks = self.tasks.len(), in_txn = self.in_txn),
        err,
    )]
    pub async fn commit_all(
        &mut self,
        meta: Option<&StreamsGroupMeta>,
    ) -> Result<(), StreamsClientError> {
        match self.guarantee {
            ProcessingGuarantee::AtLeastOnce => {
                for task in self.tasks.values_mut() {
                    task.commit().await?;
                }
            }
            ProcessingGuarantee::ExactlyOnceV2 => {
                if !self.in_txn {
                    return Ok(()); // nothing produced since last commit
                }
                // Flush each task's record caches BEFORE sending offsets +
                // committing the transaction, so the deduped `Change`s + their
                // changelog records are produced into the SAME open transaction
                // that `eos_send_offsets_and_commit` then commits (mirroring the
                // ALOS `task.commit()` flush-then-commit ordering). The flush
                // sends via each task's producer, which under EOS is the thread's
                // txn producer — so the records join this interval's transaction.
                // A flush failure aborts + rolls back like any other commit-path
                // error (so the cycle is retried on the next interval).
                for task in self.tasks.values_mut() {
                    if task.flush_caches().await.is_err() {
                        self.abort_and_rollback().await?;
                        return Ok(());
                    }
                }
                // Capture the txn-commit sequence so any error (e.g. a failed
                // `commit_transaction`) aborts the txn and rolls every task back to
                // the last committed state. The cycle is then retried on the next
                // interval (so `commit_all` returns Ok after a clean rollback).
                let res = self.eos_send_offsets_and_commit(meta).await;
                if res.is_err() {
                    self.abort_and_rollback().await?;
                    return Ok(());
                }
                for task in self.tasks.values_mut() {
                    task.clear_pending();
                }
                self.in_txn = false;
            }
        }
        Ok(())
    }

    /// Serve one interactive query against this thread's local tasks. Composite
    /// across every task whose registry hosts the named store.
    ///
    /// Takes `&mut self` (not `&self`): the query borrows `&dyn IqQueryable`
    /// views out of the task graphs and holds them across `answer_iq`'s awaits.
    /// A `&self` body would capture `&StreamThread` across the await, requiring
    /// `StreamThread: Sync` — but the graph holds `Box<dyn StateStore>` /
    /// `Box<dyn ErasedNode>` which are `Send` but not `Sync`, so the supervisor's
    /// spawned future would not be `Send`. `&mut self` only needs `Send`.
    #[tracing::instrument(
        name = "streams.thread.serve_iq",
        level = "debug",
        skip_all,
        fields(store = %req.store, kind = ?req.kind, tasks = self.tasks.len()),
    )]
    pub(crate) async fn serve_iq(&mut self, req: IqRequest) {
        let matching: Vec<&dyn crate::store::iq::IqQueryable> = self
            .tasks
            .values()
            .filter_map(|t| t.registry().iq_get(&req.store))
            .collect();
        let result = answer_iq(
            matching,
            req.kind,
            &req.op,
            &req.store,
            !self.tasks.is_empty(),
        )
        .await;
        let _ = req.reply.send(result);
    }

    /// Serve one `IQv2` query: per-partition (no merge). Filters tasks by the
    /// requested partition set, applies the active-only and position-bound
    /// gates, and tags each store's typed result with its partition + position.
    #[tracing::instrument(
        name = "streams.thread.serve_iq2",
        level = "debug",
        skip_all,
        fields(store = %req.store, kind = ?req.kind, tasks = self.tasks.len()),
    )]
    pub(crate) async fn serve_iq2(&mut self, req: crate::runtime::iqv2::dispatch::Iq2Request) {
        use crate::{
            runtime::{
                iqv2::{
                    dispatch::Iq2Outcome,
                    request::{PartitionSel, PositionBound},
                    result::FailureReason,
                },
                task::TaskRole,
            },
            store::iq::Iq2Failure,
        };

        let had_tasks = !self.tasks.is_empty();
        let mut per_partition = Vec::new();

        // Phase 1 (sync): gate every task and collect the runnable store views
        // before any await. The `tasks.values()` iterator and `&StreamTask` are
        // not `Send` (the graph holds `Send`-but-not-`Sync` erased nodes), so
        // the iterator must be fully drained — and dropped — before the first
        // `iq2_execute().await`. The store view (`&dyn IqQueryable`) and
        // `Position` *are* `Send`, so they may cross the await in phase 2.
        let mut runnable: Vec<(
            i32,
            crate::runtime::iqv2::request::Position,
            &dyn crate::store::iq::IqQueryable,
        )> = Vec::new();
        for t in self.tasks.values() {
            let partition = t.partition;
            if let PartitionSel::Set(set) = &req.partitions
                && !set.contains(&partition)
            {
                continue;
            }
            let Some(store) = t.registry().iq_get(&req.store) else {
                continue;
            };
            let pos = t.position();
            if store.kind() != req.kind {
                per_partition.push((partition, pos, Err(FailureReason::NotPresent)));
                continue;
            }
            if req.require_active && t.role != TaskRole::Active {
                per_partition.push((partition, pos, Err(FailureReason::NotActive)));
                continue;
            }
            if let PositionBound::At(bound) = &req.bound
                && !pos.dominates(bound)
            {
                per_partition.push((partition, pos, Err(FailureReason::NotUpToBound)));
                continue;
            }
            runnable.push((partition, pos, store));
        }

        // Phase 2 (async): execute the collected queries. The iterator above is
        // dropped, so only `Send` data crosses each await.
        for (partition, pos, store) in runnable {
            let outcome = match store.iq2_execute(&req.query).await {
                Ok(boxed) => Ok(boxed),
                Err(Iq2Failure::UnknownQueryType) => Err(FailureReason::UnknownQueryType),
                Err(Iq2Failure::KeyTypeMismatch) => Err(FailureReason::StoreException),
            };
            per_partition.push((partition, pos, outcome));
        }
        let _ = req.reply.send(Iq2Outcome {
            per_partition,
            had_tasks,
        });
    }

    /// Commit + drop all tasks (on Fenced / shutdown).
    ///
    /// Under EOS, an open transaction is aborted (best-effort) rather than
    /// committed — a fence/shutdown mid-cycle must not leak a half-written txn.
    #[tracing::instrument(
        name = "streams.thread.close_all",
        level = "info",
        skip_all,
        fields(guarantee = ?self.guarantee, tasks = self.tasks.len(), in_txn = self.in_txn),
        err,
    )]
    pub async fn close_all(
        &mut self,
        meta: Option<&StreamsGroupMeta>,
    ) -> Result<(), StreamsClientError> {
        match self.guarantee {
            ProcessingGuarantee::AtLeastOnce => {
                self.commit_all(meta).await?;
            }
            ProcessingGuarantee::ExactlyOnceV2 => {
                // Abort any in-flight txn (best-effort) — a fence/shutdown mid-cycle
                // must not leak a half-written transaction. (Clean rollback = T4.)
                if self.in_txn {
                    if let Some(t) = &self.txn {
                        let _ = t.abort_transaction().await;
                    }
                    self.in_txn = false;
                }
            }
        }
        self.tasks.clear();
        Ok(())
    }
}

/// Lazy begin-transaction gate handed to each task's `process_once` under
/// EOS-v2. The first task to produce a record this interval calls
/// [`BeginTxnGate::ensure_begun`], which begins the transaction exactly once;
/// subsequent calls (further records / partitions / tasks) are no-ops. When no
/// task produces anything the gate is never tripped and no transaction opens.
struct EosBeginGate {
    txn: Arc<dyn TransactionalProducer>,
    /// Whether a transaction is currently open. Seeded from the thread's
    /// `in_txn` (so a re-poll within an already-open interval doesn't re-begin)
    /// and read back into it after processing.
    begun: bool,
}

#[async_trait::async_trait]
impl BeginTxnGate for EosBeginGate {
    async fn ensure_begun(&mut self) -> Result<(), StreamsClientError> {
        if !self.begun {
            self.txn.begin_transaction().await?;
            self.begun = true;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex as StdMutex},
    };

    use assert2::check;

    use super::*;
    use crate::{
        membership::{StreamsAssignment, TaskAssignment, TaskOffsetTracker, TopicPartition},
        processor::{
            api::{Processor, ProcessorContext},
            record::Record,
            serde::{I64Serde, StringSerde},
        },
        runtime::io::{
            FetchBatch, FetchedRec, IsolationLevel, OffsetStore, RecordFetcher, RecordProducer,
        },
        topology::{NodeHandle, Topology},
    };

    // ─── stateless Upper processor ────────────────────────────────────────────

    struct Upper;
    #[async_trait::async_trait]
    impl Processor<String, String, String, String> for Upper {
        async fn process(
            &mut self,
            ctx: &mut ProcessorContext<'_, '_, String, String>,
            r: Record<String, String>,
        ) {
            ctx.forward(Record::new(r.key, r.value.to_uppercase(), r.timestamp));
        }
    }

    fn built() -> crate::topology::BuiltTopology {
        let mut t = Topology::new();
        let src: NodeHandle<String, String> = t.add_source("src", ["in"]);
        let up = t.add_processor("up", || Upper, [&src]);
        t.add_sink("out", "out", [&up]);
        t.build("app").unwrap()
    }

    // ─── stateful Counter processor ───────────────────────────────────────────

    struct Counter;
    #[async_trait::async_trait]
    impl Processor<String, String, String, i64> for Counter {
        async fn process(
            &mut self,
            ctx: &mut ProcessorContext<'_, '_, String, i64>,
            r: Record<String, String>,
        ) {
            let n = {
                let store = ctx.get_state_store::<String, i64>("counts").unwrap();
                let n = store.get(&r.value).await.unwrap_or(0) + 1;
                store.put(r.value.clone(), n).await;
                n
            };
            ctx.forward(Record::new(Some(r.value), n, r.timestamp));
        }
    }

    fn stateful_built() -> crate::topology::BuiltTopology {
        let mut t = Topology::new();
        let src: NodeHandle<String, String> = t.add_source("src", ["in"]);
        let c = t.add_processor("c", || Counter, [&src]);
        t.add_state_store("counts", StringSerde, I64Serde, [c.name()]);
        t.add_sink("out", "out", [&c]);
        t.build("app").unwrap()
    }

    // ─── wall-clock punctuator scheduler ──────────────────────────────────────

    struct EmitTs;
    #[async_trait::async_trait]
    impl crate::processor::punctuation::Punctuator<String, i64> for EmitTs {
        async fn punctuate(&mut self, ctx: &mut ProcessorContext<'_, '_, String, i64>, ts: i64) {
            ctx.forward(Record::new(None, ts, ts));
        }
    }

    /// Schedules a `WALL_CLOCK_TIME` punctuator (interval 100ms) in `init`; no-op on
    /// records (so any sink output is from the wall-clock punctuator).
    struct WallClockScheduler;
    #[async_trait::async_trait]
    impl Processor<String, String, String, i64> for WallClockScheduler {
        async fn init(&mut self, ctx: &mut ProcessorContext<'_, '_, String, i64>) {
            ctx.schedule(
                std::time::Duration::from_millis(100),
                crate::processor::punctuation::PunctuationType::WallClockTime,
                EmitTs,
            );
        }
        async fn process(
            &mut self,
            _ctx: &mut ProcessorContext<'_, '_, String, i64>,
            _r: Record<String, String>,
        ) {
        }
    }

    fn wall_clock_built() -> crate::topology::BuiltTopology {
        let mut t = Topology::new();
        let src: NodeHandle<String, String> = t.add_source("src", ["in"]);
        let p = t.add_processor("p", || WallClockScheduler, [&src]);
        t.add_sink("out", "out", [&p]);
        t.build("app").unwrap()
    }

    // ─── fakes ────────────────────────────────────────────────────────────────

    /// Returns one batch at its scripted (topic, partition, offset), then empty.
    struct ScriptedFetcher {
        scripts: StdMutex<HashMap<(String, i32, i64), FetchBatch>>,
    }

    impl ScriptedFetcher {
        fn new(scripts: Vec<((String, i32, i64), FetchBatch)>) -> Self {
            Self {
                scripts: StdMutex::new(scripts.into_iter().collect()),
            }
        }
    }

    #[async_trait::async_trait]
    impl RecordFetcher for ScriptedFetcher {
        async fn fetch(
            &self,
            t: &str,
            p: i32,
            o: i64,
            _isolation: IsolationLevel,
        ) -> Result<FetchBatch, crate::StreamsClientError> {
            Ok(self
                .scripts
                .lock()
                .unwrap()
                .remove(&(t.to_string(), p, o))
                .unwrap_or_default())
        }
    }

    struct OneShot {
        batch: StdMutex<Option<FetchBatch>>,
    }

    #[async_trait::async_trait]
    impl RecordFetcher for OneShot {
        async fn fetch(
            &self,
            _t: &str,
            _p: i32,
            _o: i64,
            _isolation: IsolationLevel,
        ) -> Result<FetchBatch, crate::StreamsClientError> {
            Ok(self.batch.lock().unwrap().take().unwrap_or_default())
        }
    }

    type SentRecord = (
        String,
        Option<i32>,
        Option<bytes::Bytes>,
        Option<bytes::Bytes>,
    );

    #[derive(Default)]
    struct CollectProducer {
        /// (topic, partition, key, value)
        sent: StdMutex<Vec<SentRecord>>,
        flushes: StdMutex<u32>,
    }

    #[async_trait::async_trait]
    impl RecordProducer for CollectProducer {
        async fn send(
            &self,
            topic: &str,
            partition: Option<i32>,
            k: Option<bytes::Bytes>,
            v: Option<bytes::Bytes>,
        ) -> Result<(), crate::StreamsClientError> {
            self.sent
                .lock()
                .unwrap()
                .push((topic.to_string(), partition, k, v));
            Ok(())
        }

        async fn flush(&self) -> Result<(), crate::StreamsClientError> {
            *self.flushes.lock().unwrap() += 1;
            Ok(())
        }
    }

    #[derive(Default)]
    struct MemStore {
        committed: StdMutex<HashMap<(String, i32), i64>>,
    }

    #[async_trait::async_trait]
    impl OffsetStore for MemStore {
        async fn committed(
            &self,
            t: &str,
            p: i32,
        ) -> Result<Option<i64>, crate::StreamsClientError> {
            Ok(self
                .committed
                .lock()
                .unwrap()
                .get(&(t.to_string(), p))
                .copied())
        }

        async fn earliest(&self, _t: &str, _p: i32) -> Result<i64, crate::StreamsClientError> {
            Ok(0)
        }

        async fn latest(&self, _t: &str, _p: i32) -> Result<i64, crate::StreamsClientError> {
            Ok(0)
        }

        async fn commit(
            &self,
            offs: &[(String, i32, i64)],
        ) -> Result<(), crate::StreamsClientError> {
            let mut m = self.committed.lock().unwrap();
            for (t, p, o) in offs {
                m.insert((t.clone(), *p), *o);
            }
            Ok(())
        }
    }

    fn assignment() -> StreamsAssignment {
        StreamsAssignment {
            active: vec![TaskAssignment {
                subtopology_id: "0".into(),
                partitions: vec![0],
                source_topic_partitions: vec![TopicPartition {
                    topic: "in".into(),
                    partition: 0,
                }],
            }],
            standby: vec![],
            warmup: vec![],
        }
    }

    fn empty_fetcher() -> Arc<dyn RecordFetcher> {
        // Returns empty for all fetches; used when restore has nothing to replay.
        Arc::new(ScriptedFetcher::new(vec![])) as Arc<dyn RecordFetcher>
    }

    /// Dispatch one `Iq2Request` (built from the supplied reply sender) through
    /// `serve_iq2` and return the assembled `Iq2Outcome`.
    async fn serve_iq2_outcome(
        thread: &mut StreamThread,
        build: impl FnOnce(
            tokio::sync::oneshot::Sender<crate::runtime::iqv2::dispatch::Iq2Outcome>,
        ) -> crate::runtime::iqv2::dispatch::Iq2Request,
    ) -> crate::runtime::iqv2::dispatch::Iq2Outcome {
        let (reply, rx) = tokio::sync::oneshot::channel();
        thread.serve_iq2(build(reply)).await;
        rx.await.unwrap()
    }

    /// One `(subtopology "0", partition)` task over source topic `in`.
    fn task_for(partition: i32) -> TaskAssignment {
        TaskAssignment {
            subtopology_id: "0".into(),
            partitions: vec![partition],
            source_topic_partitions: vec![TopicPartition {
                topic: "in".into(),
                partition,
            }],
        }
    }

    // ─── tests ────────────────────────────────────────────────────────────────

    /// `poll_all` must fire due `WALL_CLOCK_TIME` punctuators between polls, driven
    /// by the injected `Clock`. We use a `ManualClock` over a shared atomic so we
    /// can advance wall time deterministically:
    ///   - `init` schedules the punctuator at base `wall_clock`=0 → next fire = 100.
    ///   - clock=0: first `poll_all` → now=0 < 100, no fire.
    ///   - advance clock to 150: second `poll_all` → now=150 >= 100, fires ONCE,
    ///     emitting value = now = 150 to the "out" sink.
    #[tokio::test]
    async fn poll_all_fires_wall_clock_punctuation_via_manual_clock() {
        use std::sync::atomic::AtomicI64;

        let producer_c = Arc::new(CollectProducer::default());
        let store_c = Arc::new(MemStore::default());
        let producer: Arc<dyn RecordProducer> = Arc::clone(&producer_c) as _;
        let store: Arc<dyn OffsetStore> = Arc::clone(&store_c) as _;
        let built = wall_clock_built();
        let tracker = Arc::new(TokioMutex::new(TaskOffsetTracker::default()));

        let now = Arc::new(AtomicI64::new(0));
        let clock: Arc<dyn crate::runtime::clock::Clock> =
            Arc::new(crate::runtime::clock::ManualClock(Arc::clone(&now)));
        let mut thread = StreamThread::new(
            empty_fetcher(),
            crate::store::backend::StoreBackend::InMemory,
            "app".into(),
            0,
        )
        .with_clock(clock);
        thread
            .apply_assignment(
                &assignment(),
                &built,
                &producer,
                &store,
                crate::runtime::eos::ProcessingGuarantee::AtLeastOnce,
                None,
            )
            .await
            .unwrap();
        check!(thread.task_count() == 1);

        // clock=0 → no record source, no wall-clock fire yet (now=0 < next=100).
        thread.poll_all(&*empty_fetcher(), &tracker).await.unwrap();
        check!(
            !producer_c
                .sent
                .lock()
                .unwrap()
                .iter()
                .any(|(t, _p, _k, _v)| t == "out"),
            "no wall-clock punctuation should fire before the interval elapses"
        );

        // Advance wall time past one interval; the next poll must fire the
        // punctuator (value = now = 150) and produce it to "out".
        now.store(150, std::sync::atomic::Ordering::SeqCst);
        thread.poll_all(&*empty_fetcher(), &tracker).await.unwrap();
        check!(
            producer_c
                .sent
                .lock()
                .unwrap()
                .iter()
                .any(|(t, _p, _k, v)| t == "out"
                    && v.as_deref() == Some(150i64.to_be_bytes().as_ref())),
            "wall-clock punctuator must fire from poll_all once the ManualClock passes the interval, emitting value=150"
        );
    }

    #[tokio::test]
    async fn apply_assignment_creates_task_polls_commits() {
        let producer_c = Arc::new(CollectProducer::default());
        let store_c = Arc::new(MemStore::default());
        let producer: Arc<dyn RecordProducer> = Arc::clone(&producer_c) as _;
        let store: Arc<dyn OffsetStore> = Arc::clone(&store_c) as _;
        let built = built();
        let mut thread = StreamThread::new(
            empty_fetcher(),
            crate::store::backend::StoreBackend::InMemory,
            "app".into(),
            0,
        );
        thread
            .apply_assignment(
                &assignment(),
                &built,
                &producer,
                &store,
                ProcessingGuarantee::AtLeastOnce,
                None,
            )
            .await
            .unwrap();
        check!(thread.task_count() == 1);

        let tracker = Arc::new(TokioMutex::new(TaskOffsetTracker::default()));
        let fetcher = OneShot {
            batch: StdMutex::new(Some(FetchBatch {
                records: vec![FetchedRec {
                    offset: 0,
                    key: Some("k".into()),
                    value: Some("hi".into()),
                    timestamp: -1,
                }],
            })),
        };
        thread.poll_all(&fetcher, &tracker).await.unwrap();
        thread.commit_all(None).await.unwrap();
        check!(
            producer_c
                .sent
                .lock()
                .unwrap()
                .iter()
                .any(|(t, _p, _k, v)| t == "out" && v.as_deref() == Some(b"HI".as_ref()))
        );
        check!(
            store_c
                .committed
                .lock()
                .unwrap()
                .get(&("in".to_string(), 0))
                == Some(&1)
        );

        // empty assignment → task removed (close_processors + committed on the way out)
        thread
            .apply_assignment(
                &StreamsAssignment::default(),
                &built,
                &producer,
                &store,
                ProcessingGuarantee::AtLeastOnce,
                None,
            )
            .await
            .unwrap();
        check!(thread.task_count() == 0);
    }

    /// Verify that `apply_assignment` replays changelog records into the task's
    /// store during restore, so that the first `process_once` continues from the
    /// restored count rather than from zero.
    #[tokio::test]
    async fn stateful_apply_assignment_restores_store_from_changelog() {
        // Changelog: key="a", value=i64 BE 7 at offset 0 on "app-counts-changelog".
        let cl_key = bytes::Bytes::copy_from_slice(b"a");
        let cl_val = bytes::Bytes::copy_from_slice(&7i64.to_be_bytes());
        let restore_fetcher: Arc<dyn RecordFetcher> = Arc::new(ScriptedFetcher::new(vec![(
            ("app-counts-changelog".to_string(), 0, 0),
            FetchBatch {
                records: vec![FetchedRec {
                    offset: 0,
                    key: Some(cl_key),
                    value: Some(cl_val),
                    timestamp: -1,
                }],
            },
        )]));

        let producer_c = Arc::new(CollectProducer::default());
        let store_c = Arc::new(MemStore::default());
        let producer: Arc<dyn RecordProducer> = Arc::clone(&producer_c) as _;
        let store: Arc<dyn OffsetStore> = Arc::clone(&store_c) as _;
        let built = stateful_built();

        let mut thread = StreamThread::new(
            Arc::clone(&restore_fetcher),
            crate::store::backend::StoreBackend::InMemory,
            "app".into(),
            0,
        );
        thread
            .apply_assignment(
                &assignment(),
                &built,
                &producer,
                &store,
                ProcessingGuarantee::AtLeastOnce,
                None,
            )
            .await
            .unwrap();
        check!(thread.task_count() == 1);

        // Now process one "a" record.  Restored count is 7, so output must be 8.
        let tracker = Arc::new(TokioMutex::new(TaskOffsetTracker::default()));
        let process_fetcher = ScriptedFetcher::new(vec![(
            ("in".to_string(), 0, 0),
            FetchBatch {
                records: vec![FetchedRec {
                    offset: 0,
                    key: None,
                    value: Some("a".into()),
                    timestamp: -1,
                }],
            },
        )]);
        thread.poll_all(&process_fetcher, &tracker).await.unwrap();
        thread.commit_all(None).await.unwrap();

        let sent = producer_c.sent.lock().unwrap();
        check!(
            sent.iter()
                .any(|(t, _p, _k, v)| t == "out"
                    && v.as_deref() == Some(8i64.to_be_bytes().as_ref())),
            "after restore with N=7, processing 'a' must emit count = 8"
        );
    }

    /// `serve_iq` must resolve a `KvGet` against the live, restored task store:
    /// after restoring `counts` with `a=7` from the changelog (same setup as
    /// `stateful_apply_assignment_restores_store_from_changelog`), a `KvGet` for
    /// "a" returns the i64-BE bytes for 7. A thread with no tasks (rebalancing)
    /// returns `RebalanceInProgress`.
    #[tokio::test]
    async fn serve_iq_reads_restored_kv_store() {
        use crate::{
            processor::serde::{I64Serde, Serde, StringSerde},
            runtime::iq::{IqError, IqOp, IqPayload, IqRequest},
            store::iq::StoreKind,
        };

        // --- build a thread + restore `counts` with a=7 (copied from
        //     stateful_apply_assignment_restores_store_from_changelog) ---
        // Changelog: key="a", value=i64 BE 7 at offset 0 on "app-counts-changelog".
        let cl_key = bytes::Bytes::copy_from_slice(b"a");
        let cl_val = bytes::Bytes::copy_from_slice(&7i64.to_be_bytes());
        let restore_fetcher: Arc<dyn RecordFetcher> = Arc::new(ScriptedFetcher::new(vec![(
            ("app-counts-changelog".to_string(), 0, 0),
            FetchBatch {
                records: vec![FetchedRec {
                    offset: 0,
                    key: Some(cl_key),
                    value: Some(cl_val),
                    timestamp: -1,
                }],
            },
        )]));

        let producer_c = Arc::new(CollectProducer::default());
        let store_c = Arc::new(MemStore::default());
        let producer: Arc<dyn RecordProducer> = Arc::clone(&producer_c) as _;
        let store: Arc<dyn OffsetStore> = Arc::clone(&store_c) as _;
        let built = stateful_built();

        let mut thread = StreamThread::new(
            Arc::clone(&restore_fetcher),
            crate::store::backend::StoreBackend::InMemory,
            "app".into(),
            0,
        );
        thread
            .apply_assignment(
                &assignment(),
                &built,
                &producer,
                &store,
                crate::runtime::eos::ProcessingGuarantee::AtLeastOnce,
                None,
            )
            .await
            .unwrap();
        check!(thread.task_count() == 1);

        // happy path: get "a" -> 7
        let (reply, rx) = tokio::sync::oneshot::channel();
        thread
            .serve_iq(IqRequest {
                store: "counts".into(),
                kind: StoreKind::KeyValue,
                op: IqOp::KvGet {
                    key: StringSerde.serialize("t", &"a".to_string()),
                },
                reply,
            })
            .await;
        assert_eq!(
            rx.await.unwrap().unwrap(),
            IqPayload::Value(Some(I64Serde.serialize("t", &7_i64)))
        );

        // empty thread (no tasks) -> RebalanceInProgress
        let mut empty = StreamThread::new(
            empty_fetcher(),
            crate::store::backend::StoreBackend::InMemory,
            "app".into(),
            0,
        );
        let (reply2, rx2) = tokio::sync::oneshot::channel();
        empty
            .serve_iq(IqRequest {
                store: "counts".into(),
                kind: StoreKind::KeyValue,
                op: IqOp::KvGet {
                    key: StringSerde.serialize("t", &"a".to_string()),
                },
                reply: reply2,
            })
            .await;
        assert!(matches!(
            rx2.await.unwrap(),
            Err(IqError::RebalanceInProgress)
        ));
    }

    /// `serve_iq2` per-partition gating: an active task over the `counts`
    /// `KeyValue` store yields a `Success` (empty store → `Ok(Box<None>)`),
    /// while the partition-set, active-only, and position-bound gates each
    /// suppress or fail the matching partition.
    #[tokio::test]
    async fn serve_iq2_gates_partition_active_and_bound() {
        use crate::{
            runtime::iqv2::{
                dispatch::Iq2Request,
                request::{PartitionSel, Position, PositionBound},
                result::FailureReason,
            },
            store::iq::{Iq2Query, StoreKind},
        };

        // active(p0) + standby(p1) over the stateful `counts` KeyValue store.
        // Neither task is fed records, so the store is empty.
        let producer_c = Arc::new(CollectProducer::default());
        let store_c = Arc::new(MemStore::default());
        let producer: Arc<dyn RecordProducer> = Arc::clone(&producer_c) as _;
        let store: Arc<dyn OffsetStore> = Arc::clone(&store_c) as _;
        let built = stateful_built();
        let assignment = StreamsAssignment {
            active: vec![task_for(0)],
            standby: vec![task_for(1)],
            warmup: vec![],
        };

        let mut thread = StreamThread::new(
            empty_fetcher(),
            crate::store::backend::StoreBackend::InMemory,
            "app".into(),
            0,
        );
        thread
            .apply_assignment(
                &assignment,
                &built,
                &producer,
                &store,
                ProcessingGuarantee::AtLeastOnce,
                None,
            )
            .await
            .unwrap();
        check!(thread.task_count() == 2);

        // Build a base KeyQuery request; callers tweak the gate fields.
        let req = |partitions, bound, require_active, reply| Iq2Request {
            store: "counts".into(),
            kind: StoreKind::KeyValue,
            query: Iq2Query::Key {
                key: Box::new("k".to_string()),
            },
            partitions,
            bound,
            require_active,
            reply,
        };
        let find = |out: &crate::runtime::iqv2::dispatch::Iq2Outcome, want: i32| {
            out.per_partition
                .iter()
                .find(|(p, _, _)| *p == want)
                .map(|(_, _, r)| r.is_ok())
        };

        // (1) Happy path, all partitions: empty active store → Success(None).
        let out = serve_iq2_outcome(&mut thread, |reply| {
            req(PartitionSel::All, PositionBound::Unbounded, false, reply)
        })
        .await;
        let (_, _, r) = out
            .per_partition
            .iter()
            .find(|(p, _, _)| *p == 0)
            .expect("partition 0 present");
        let boxed = r.as_ref().expect("partition 0 is a Success");
        let downcast = boxed.downcast_ref::<Option<i64>>().expect("Option<i64>");
        assert_eq!(*downcast, None, "empty store yields None");

        // (2) Partition-set gate: a set excluding p0 omits it entirely.
        let set1 = || PartitionSel::Set([1].into_iter().collect());
        let out = serve_iq2_outcome(&mut thread, |reply| {
            req(set1(), PositionBound::Unbounded, false, reply)
        })
        .await;
        assert_eq!(
            find(&out, 0),
            None,
            "p0 excluded from the set must not appear"
        );
        assert!(find(&out, 1).is_some(), "p1 (in the set) must appear");

        // (3) Active-only gate against the standby (p1) task → NotActive.
        let out = serve_iq2_outcome(&mut thread, |reply| {
            req(set1(), PositionBound::Unbounded, true, reply)
        })
        .await;
        let (_, _, r) = out.per_partition.iter().find(|(p, _, _)| *p == 1).unwrap();
        assert_eq!(r.as_ref().err(), Some(&FailureReason::NotActive));

        // (4) Position-bound gate: a bound ahead of the (empty) p0 position →
        // NotUpToBound (p0 has never advanced).
        let ahead = Position(
            [("in".to_string(), [(0, 100)].into_iter().collect())]
                .into_iter()
                .collect(),
        );
        let out = serve_iq2_outcome(&mut thread, |reply| {
            req(
                PartitionSel::Set([0].into_iter().collect()),
                PositionBound::At(ahead),
                false,
                reply,
            )
        })
        .await;
        let (_, _, r) = out.per_partition.iter().find(|(p, _, _)| *p == 0).unwrap();
        assert_eq!(r.as_ref().err(), Some(&FailureReason::NotUpToBound));
    }

    /// End-to-end of the real runtime global-store path: `StreamThread` builds +
    /// bootstraps the shared `GlobalStateManager` from the broker BEFORE any task
    /// processes, then a stream-globaltable join reads the bootstrapped value.
    ///
    /// Topology: a `GlobalKTable` over topic "global" (store "g-store"), and a
    /// stream "in" that joins it with `key_mapper = |_k, v| v.clone()` (lookup key
    /// = the record value) and `joiner = |sv, gv| sv + gv`. The global store is
    /// seeded on the broker at (global, 0, 0) = ("gk", "GV"); the stream record is
    /// (key "k", value "gk"). The derived lookup key "gk" hits "GV", so the join
    /// emits key "k", value "gkGV". Proves bootstrap-before-process wires the
    /// shared manager into the task graph in the real runtime.
    #[tokio::test]
    async fn global_apply_assignment_bootstraps_store_before_join() {
        use crate::dsl::{GlobalKTable, StreamsBuilder};

        // Build the global-table join topology via the DSL.
        let b = StreamsBuilder::new();
        let g: GlobalKTable<String, String> = b.global_table::<String, String>("global", "g-store");
        b.stream::<String, String>(["in"])
            .join_global(
                &g,
                |_k: &String, v: &String| v.clone(),
                |sv: &String, gv: &String| format!("{sv}{gv}"),
            )
            .to("out");
        drop(g);
        let built = b.build("app").unwrap();

        // Bootstrap fetcher: serves the single global record at (global, 0, 0), then
        // empty. The default `partitions("global")` is vec![0], which matches the
        // single-partition global topic. The "in" restore replays nothing (no state
        // store on the stream subtopology).
        let boot_fetcher: Arc<dyn RecordFetcher> = Arc::new(ScriptedFetcher::new(vec![(
            ("global".to_string(), 0, 0),
            FetchBatch {
                records: vec![FetchedRec {
                    offset: 0,
                    key: Some("gk".into()),
                    value: Some("GV".into()),
                    timestamp: -1,
                }],
            },
        )]));

        let producer_c = Arc::new(CollectProducer::default());
        let store_c = Arc::new(MemStore::default());
        let producer: Arc<dyn RecordProducer> = Arc::clone(&producer_c) as _;
        let store: Arc<dyn OffsetStore> = Arc::clone(&store_c) as _;

        let mut thread = StreamThread::new(
            Arc::clone(&boot_fetcher),
            crate::store::backend::StoreBackend::InMemory,
            "app".into(),
            0,
        );

        // The global-table topology emits the stream subtopology as id "1".
        let assignment = StreamsAssignment {
            active: vec![TaskAssignment {
                subtopology_id: "1".into(),
                partitions: vec![0],
                source_topic_partitions: vec![TopicPartition {
                    topic: "in".into(),
                    partition: 0,
                }],
            }],
            standby: vec![],
            warmup: vec![],
        };
        thread
            .apply_assignment(
                &assignment,
                &built,
                &producer,
                &store,
                ProcessingGuarantee::AtLeastOnce,
                None,
            )
            .await
            .unwrap();
        check!(thread.task_count() == 1);

        // Now process one stream record (key "k", value "gk"). The key-mapper derives
        // lookup key "gk", which the bootstrapped global store resolves to "GV", so
        // the join emits key "k", value "gk" + "GV" = "gkGV".
        let tracker = Arc::new(TokioMutex::new(TaskOffsetTracker::default()));
        let process_fetcher = ScriptedFetcher::new(vec![(
            ("in".to_string(), 0, 0),
            FetchBatch {
                records: vec![FetchedRec {
                    offset: 0,
                    key: Some("k".into()),
                    value: Some("gk".into()),
                    timestamp: -1,
                }],
            },
        )]);
        thread.poll_all(&process_fetcher, &tracker).await.unwrap();
        thread.commit_all(None).await.unwrap();

        let sent = producer_c.sent.lock().unwrap();
        check!(
            sent.iter().any(|(t, _p, k, v)| t == "out"
                && k.as_deref() == Some(b"k".as_ref())
                && v.as_deref() == Some(b"gkGV".as_ref())),
            "join must see the bootstrapped global value: ('out', key 'k', value 'gkGV')"
        );
    }

    #[tokio::test]
    async fn reconciles_active_standby_warmup_roles_and_transitions() {
        let producer_c = Arc::new(CollectProducer::default());
        let store_c = Arc::new(MemStore::default());
        let producer: Arc<dyn RecordProducer> = Arc::clone(&producer_c) as _;
        let store: Arc<dyn OffsetStore> = Arc::clone(&store_c) as _;
        let built = built();

        let mut thread = StreamThread::new(
            empty_fetcher(),
            crate::store::backend::StoreBackend::InMemory,
            "app".into(),
            0,
        );

        // 1. Initial assignment:
        // Subtopology 0 Partition 0 -> Active
        // Subtopology 0 Partition 1 -> Standby
        // Subtopology 0 Partition 2 -> Warmup
        let assignment1 = StreamsAssignment {
            active: vec![TaskAssignment {
                subtopology_id: "0".into(),
                partitions: vec![0],
                source_topic_partitions: vec![TopicPartition {
                    topic: "in".into(),
                    partition: 0,
                }],
            }],
            standby: vec![TaskAssignment {
                subtopology_id: "0".into(),
                partitions: vec![1],
                source_topic_partitions: vec![TopicPartition {
                    topic: "in".into(),
                    partition: 1,
                }],
            }],
            warmup: vec![TaskAssignment {
                subtopology_id: "0".into(),
                partitions: vec![2],
                source_topic_partitions: vec![TopicPartition {
                    topic: "in".into(),
                    partition: 2,
                }],
            }],
        };

        thread
            .apply_assignment(
                &assignment1,
                &built,
                &producer,
                &store,
                ProcessingGuarantee::AtLeastOnce,
                None,
            )
            .await
            .unwrap();
        check!(thread.task_count() == 3);

        check!(thread.tasks.get(&("0".to_string(), 0)).map(|t| t.role) == Some(TaskRole::Active));
        check!(thread.tasks.get(&("0".to_string(), 1)).map(|t| t.role) == Some(TaskRole::Standby));
        check!(thread.tasks.get(&("0".to_string(), 2)).map(|t| t.role) == Some(TaskRole::Warmup));

        // 2. Updated assignment:
        // Subtopology 0 Partition 0 -> Standby (Demoted)
        // Subtopology 0 Partition 1 -> removed
        // Subtopology 0 Partition 2 -> Active (Promoted)
        // Subtopology 0 Partition 3 -> Warmup (New)
        let assignment2 = StreamsAssignment {
            active: vec![TaskAssignment {
                subtopology_id: "0".into(),
                partitions: vec![2],
                source_topic_partitions: vec![TopicPartition {
                    topic: "in".into(),
                    partition: 2,
                }],
            }],
            standby: vec![TaskAssignment {
                subtopology_id: "0".into(),
                partitions: vec![0],
                source_topic_partitions: vec![TopicPartition {
                    topic: "in".into(),
                    partition: 0,
                }],
            }],
            warmup: vec![TaskAssignment {
                subtopology_id: "0".into(),
                partitions: vec![3],
                source_topic_partitions: vec![TopicPartition {
                    topic: "in".into(),
                    partition: 3,
                }],
            }],
        };

        thread
            .apply_assignment(
                &assignment2,
                &built,
                &producer,
                &store,
                ProcessingGuarantee::AtLeastOnce,
                None,
            )
            .await
            .unwrap();
        check!(thread.task_count() == 3);

        check!(thread.tasks.get(&("0".to_string(), 0)).map(|t| t.role) == Some(TaskRole::Standby));
        check!(!thread.tasks.contains_key(&("0".to_string(), 1)));
        check!(thread.tasks.get(&("0".to_string(), 2)).map(|t| t.role) == Some(TaskRole::Active));
        check!(thread.tasks.get(&("0".to_string(), 3)).map(|t| t.role) == Some(TaskRole::Warmup));
    }

    /// EOS-v2 happy path: the thread runs the full transactional commit lifecycle
    /// over a stateless `source → up → sink` topology. The single
    /// `MockTransactionalProducer` is shared as BOTH the task `RecordProducer`
    /// (for the sink `send`) AND the thread's `TransactionalProducer`, so the
    /// recorded call sequence is the cross-product of both views.
    ///
    /// Expected sequence: `Init` (`apply_assignment`), `Begin` (first `poll_all`),
    /// `Send` (the sink emit during process), then `SendOffsets` + `Commit`
    /// (`commit_all`). The sink record must also be logged in `.sent`.
    #[tokio::test]
    async fn eos_happy_path_runs_begin_send_offsets_commit() {
        use crate::runtime::eos::mock::{MockTransactionalProducer, Step};

        // One mock object, two trait-object views (same Arc).
        let mock = Arc::new(MockTransactionalProducer::default());
        let producer: Arc<dyn RecordProducer> = Arc::clone(&mock) as _;
        let txn: Arc<dyn TransactionalProducer> = Arc::clone(&mock) as _;
        let store: Arc<dyn OffsetStore> = Arc::new(MemStore::default());

        let built = built();
        let mut thread = StreamThread::new(
            empty_fetcher(),
            crate::store::backend::StoreBackend::InMemory,
            "app".into(),
            0,
        );
        // EOS assignment: passes the txn producer and ExactlyOnceV2.
        thread
            .apply_assignment(
                &assignment(),
                &built,
                &producer,
                &store,
                ProcessingGuarantee::ExactlyOnceV2,
                Some(Arc::clone(&txn)),
            )
            .await
            .unwrap();
        check!(thread.task_count() == 1);

        // One input batch → the sink emits one uppercased record.
        let fetcher = OneShot {
            batch: StdMutex::new(Some(FetchBatch {
                records: vec![FetchedRec {
                    offset: 0,
                    key: Some("k".into()),
                    value: Some("hi".into()),
                    timestamp: -1,
                }],
            })),
        };
        let tracker = Arc::new(TokioMutex::new(TaskOffsetTracker::default()));
        thread.poll_all(&fetcher, &tracker).await.unwrap();

        let meta = crate::runtime::eos::StreamsGroupMeta {
            group_id: "app".into(),
            generation_id: 3,
            member_id: "m".into(),
            group_instance_id: None,
        };
        thread.commit_all(Some(&meta)).await.unwrap();

        // The full transactional lifecycle, in order.
        check!(
            *mock.calls.lock().unwrap()
                == vec![
                    Step::Init,
                    Step::Begin,
                    Step::Send,
                    Step::SendOffsets,
                    Step::Commit,
                ]
        );
        // The sink record was produced through the transactional producer.
        check!(
            mock.sent
                .lock()
                .unwrap()
                .iter()
                .any(|(t, _p, _k, v)| t == "out" && v.as_deref() == Some(b"HI".as_ref()))
        );
    }

    /// EOS-v2 idle interval: when a `poll_all` fetches NO records, the runtime
    /// must NOT begin a transaction, and the following `commit_all` must be a
    /// no-op — no `Begin`, no `Send`, no `SendOffsets`, no `Commit`. (Regression
    /// guard for the empty-transaction churn the begin-on-first-record gate
    /// fixes: the old eager begin opened + committed an empty txn every interval
    /// on an idle app.) Only `Init` (from `apply_assignment`) is recorded.
    #[tokio::test]
    async fn eos_idle_interval_opens_no_transaction() {
        use crate::runtime::eos::mock::{MockTransactionalProducer, Step};

        let mock = Arc::new(MockTransactionalProducer::default());
        let producer: Arc<dyn RecordProducer> = Arc::clone(&mock) as _;
        let txn: Arc<dyn TransactionalProducer> = Arc::clone(&mock) as _;
        let store: Arc<dyn OffsetStore> = Arc::new(MemStore::default());

        let built = built();
        let mut thread = StreamThread::new(
            empty_fetcher(),
            crate::store::backend::StoreBackend::InMemory,
            "app".into(),
            0,
        );
        thread
            .apply_assignment(
                &assignment(),
                &built,
                &producer,
                &store,
                ProcessingGuarantee::ExactlyOnceV2,
                Some(Arc::clone(&txn)),
            )
            .await
            .unwrap();
        check!(thread.task_count() == 1);

        // Idle interval: the fetcher returns empty for every fetch.
        let idle_fetcher = empty_fetcher();
        let tracker = Arc::new(TokioMutex::new(TaskOffsetTracker::default()));
        thread.poll_all(&*idle_fetcher, &tracker).await.unwrap();

        let meta = crate::runtime::eos::StreamsGroupMeta {
            group_id: "app".into(),
            generation_id: 3,
            member_id: "m".into(),
            group_instance_id: None,
        };
        thread.commit_all(Some(&meta)).await.unwrap();

        // Only the one-time Init ran — no transaction was opened or committed.
        check!(
            *mock.calls.lock().unwrap() == vec![Step::Init],
            "idle interval must open no transaction; got {:?}",
            *mock.calls.lock().unwrap()
        );
        check!(mock.sent.lock().unwrap().is_empty());
    }

    /// EOS-v2 abort + rollback: a `commit_transaction` failure mid-cycle must
    /// abort the txn and roll every task back to its last committed state —
    /// rewinding source offsets, wiping the stores, and re-restoring from the
    /// (here empty) changelog. A subsequent successful cycle then reprocesses the
    /// re-fetched batch without double-counting.
    ///
    /// Topology: stateful `source → counter (counts store) → sink`. The fetcher
    /// returns the SAME "a" record for `("in", 0, 0)` on every fetch (so the
    /// rewound cycle re-reads it) and an empty changelog (so re-restore yields an
    /// empty store).
    #[tokio::test]
    async fn eos_commit_failure_aborts_and_rolls_back() {
        use crate::runtime::eos::mock::{MockTransactionalProducer, Step};

        /// A fetcher that ALWAYS returns the "a" record at `("in", 0, 0)`
        /// regardless of how many times it's fetched (it never consumes the
        /// script), and an empty changelog. Re-fetching after a rewind re-reads
        /// the same input — proving the rollback rewound the source offset.
        struct ReplayFetcher;
        #[async_trait::async_trait]
        impl RecordFetcher for ReplayFetcher {
            async fn fetch(
                &self,
                t: &str,
                p: i32,
                o: i64,
                _isolation: IsolationLevel,
            ) -> Result<FetchBatch, crate::StreamsClientError> {
                if t == "in" && p == 0 && o == 0 {
                    Ok(FetchBatch {
                        records: vec![FetchedRec {
                            offset: 0,
                            key: None,
                            value: Some("a".into()),
                            timestamp: -1,
                        }],
                    })
                } else {
                    Ok(FetchBatch::default())
                }
            }
        }

        // One mock object, two trait-object views (same Arc). Fail the FIRST commit.
        let mock = Arc::new(MockTransactionalProducer {
            fail_at: StdMutex::new(Some(Step::Commit)),
            ..Default::default()
        });
        let producer: Arc<dyn RecordProducer> = Arc::clone(&mock) as _;
        let txn: Arc<dyn TransactionalProducer> = Arc::clone(&mock) as _;
        let store: Arc<dyn OffsetStore> = Arc::new(MemStore::default());

        let built = stateful_built();
        let replay: Arc<dyn RecordFetcher> = Arc::new(ReplayFetcher);
        let mut thread = StreamThread::new(
            Arc::clone(&replay),
            crate::store::backend::StoreBackend::InMemory,
            "app".into(),
            0,
        );
        thread
            .apply_assignment(
                &assignment(),
                &built,
                &producer,
                &store,
                ProcessingGuarantee::ExactlyOnceV2,
                Some(Arc::clone(&txn)),
            )
            .await
            .unwrap();
        check!(thread.task_count() == 1);

        let meta = crate::runtime::eos::StreamsGroupMeta {
            group_id: "app".into(),
            generation_id: 3,
            member_id: "m".into(),
            group_instance_id: None,
        };
        let key = ("0".to_string(), 0);
        let tracker = Arc::new(TokioMutex::new(TaskOffsetTracker::default()));

        // ── Cycle 1: begin + process (count "a" → 1, store dirty), then commit
        //    FAILS → abort + rollback. ──────────────────────────────────────────
        thread.poll_all(&*replay, &tracker).await.unwrap();
        // The dirty count is in the store before commit.
        check!(
            thread
                .task_store_get_i64(&key, "counts", &"a".to_string())
                .await
                == Some(1)
        );
        // Commit fails internally → abort + rollback, but commit_all returns Ok
        // (the cycle is rolled back; the next interval re-begins).
        thread.commit_all(Some(&meta)).await.unwrap();

        // The mock recorded the abort.
        check!(mock.calls.lock().unwrap().contains(&Step::Abort));
        // Pending offsets were cleared by the rollback.
        check!(!thread.task_has_pending(&key));
        // The store was rolled back: re-restored from the empty changelog, so the
        // dirty count is gone.
        check!(
            thread
                .task_store_get_i64(&key, "counts", &"a".to_string())
                .await
                == None,
            "store must be rolled back to the (empty) committed changelog state"
        );

        // ── Cycle 2: fail_at is now None → the re-fetched "a" batch reprocesses
        //    and yields count = 1 (NOT double-counted to 2). ────────────────────
        thread.poll_all(&*replay, &tracker).await.unwrap();
        thread.commit_all(Some(&meta)).await.unwrap();
        check!(
            thread
                .task_store_get_i64(&key, "counts", &"a".to_string())
                .await
                == Some(1),
            "after rollback + reprocess the count must be 1, not double-counted"
        );
        // The second commit succeeded.
        check!(
            mock.calls
                .lock()
                .unwrap()
                .iter()
                .filter(|s| **s == Step::Commit)
                .count()
                == 2
        );
    }

    // ─── Bug A: EOS commit flushes record caches into the transaction ─────────

    /// A `Change<i64>` serde so a cached-materialized topology can wire a
    /// `Change<i64>` sink. Encodes the `new` side (8 bytes BE) so the sink (and,
    /// crucially, the flush-forwarded deduped change) has bytes to emit.
    #[derive(Clone)]
    struct ChangeI64Serde;
    impl crate::processor::serde::Serde<crate::dsl::processors::change::Change<i64>>
        for ChangeI64Serde
    {
        fn serialize(
            &self,
            _topic: &str,
            v: &crate::dsl::processors::change::Change<i64>,
        ) -> bytes::Bytes {
            bytes::Bytes::copy_from_slice(&v.new.unwrap_or(0).to_be_bytes())
        }
        fn deserialize(
            &self,
            _topic: &str,
            _bytes: &[u8],
        ) -> Result<crate::dsl::processors::change::Change<i64>, crate::processor::serde::SerdeError>
        {
            unreachable!("Change<i64> sink is never deserialized in this test")
        }
    }

    /// A materializing count processor that uses the real `TupleForwarder`
    /// suppression seam: when its "counts" store is cached it does NOT forward
    /// immediately (the cache flush forwards the deduped change), mirroring the
    /// DSL aggregate processors. Used to prove that the EOS commit path flushes
    /// the cache so the deduped change + changelog reach the transaction.
    struct SuppressingCounter {
        forwarder: crate::dsl::processors::tuple_forwarder::TupleForwarder,
    }
    #[async_trait::async_trait]
    impl Processor<String, String, String, crate::dsl::processors::change::Change<i64>>
        for SuppressingCounter
    {
        async fn init(
            &mut self,
            ctx: &mut ProcessorContext<'_, '_, String, crate::dsl::processors::change::Change<i64>>,
        ) {
            self.forwarder = crate::dsl::processors::tuple_forwarder::TupleForwarder::resolve(
                ctx.store_is_cached("counts"),
            );
        }

        async fn process(
            &mut self,
            ctx: &mut ProcessorContext<'_, '_, String, crate::dsl::processors::change::Change<i64>>,
            r: Record<String, String>,
        ) {
            let rc = ctx.record_context().clone();
            let (old, new) = {
                let s = ctx.get_state_store::<String, i64>("counts").unwrap();
                s.set_record_context(rc);
                let old = s.get(&r.value).await;
                let new = old.unwrap_or(0) + 1;
                s.put(r.value.clone(), new).await;
                (old, new)
            };
            self.forwarder
                .maybe_forward(ctx, r.value, old, new, r.timestamp);
        }
    }

    /// `source → SuppressingCounter(materializes "counts", cache-marked) → Change<i64> sink`.
    fn cached_counting_built() -> crate::topology::BuiltTopology {
        use crate::dsl::processors::{change::Change, tuple_forwarder::TupleForwarder};
        let mut t = Topology::new();
        let src: NodeHandle<String, String> = t.add_source("src", ["in"]);
        let c = t.add_processor(
            "c",
            || SuppressingCounter {
                forwarder: TupleForwarder::default(),
            },
            [&src],
        );
        t.add_state_store("counts", StringSerde, I64Serde, [c.name()]);
        t.add_sink_explicit::<String, Change<i64>, _, _, _, _>(
            "out",
            "out",
            [&c],
            crate::processor::serde::Produced::with(StringSerde, ChangeI64Serde),
        );
        t.mark_store_caching("counts", true);
        t.build("app").unwrap()
    }

    /// EOS-v2 + a CACHED materialized store: a record buffered in the cache (its
    /// immediate forward suppressed) must have its deduped `Change` + changelog
    /// produced INTO the transaction at commit — the `commit_all` EOS branch must
    /// flush caches before `send_offsets`/`commit`. Regression guard for the bug
    /// where the EOS commit never flushed caches (dropping cached output).
    #[tokio::test]
    async fn eos_commit_flushes_record_caches_into_transaction() {
        use crate::runtime::eos::mock::{MockTransactionalProducer, Step};

        let mock = Arc::new(MockTransactionalProducer::default());
        let producer: Arc<dyn RecordProducer> = Arc::clone(&mock) as _;
        let txn: Arc<dyn TransactionalProducer> = Arc::clone(&mock) as _;
        let store: Arc<dyn OffsetStore> = Arc::new(MemStore::default());

        let built = cached_counting_built();
        // cache_max_bytes > 0 so "counts" is actually wrapped in a cache.
        let mut thread = StreamThread::new(
            empty_fetcher(),
            crate::store::backend::StoreBackend::InMemory,
            "app".into(),
            1024,
        );
        thread
            .apply_assignment(
                &assignment(),
                &built,
                &producer,
                &store,
                ProcessingGuarantee::ExactlyOnceV2,
                Some(Arc::clone(&txn)),
            )
            .await
            .unwrap();
        check!(thread.task_count() == 1);

        // Two records for the SAME key "a": both buffer in the cache, suppressed.
        let fetcher = ScriptedFetcher::new(vec![(
            ("in".to_string(), 0, 0),
            FetchBatch {
                records: vec![
                    FetchedRec {
                        offset: 0,
                        key: None,
                        value: Some("a".into()),
                        timestamp: -1,
                    },
                    FetchedRec {
                        offset: 1,
                        key: None,
                        value: Some("a".into()),
                        timestamp: -1,
                    },
                ],
            },
        )]);
        let tracker = Arc::new(TokioMutex::new(TaskOffsetTracker::default()));
        thread.poll_all(&fetcher, &tracker).await.unwrap();

        // Suppressed: nothing emitted to the "out" sink during processing yet
        // (the changelog/sink records are deferred to the cache flush at commit).
        check!(
            !mock
                .sent
                .lock()
                .unwrap()
                .iter()
                .any(|(t, _p, _k, _v)| t == "out"),
            "cached materialization must suppress the immediate sink forward"
        );

        let meta = crate::runtime::eos::StreamsGroupMeta {
            group_id: "app".into(),
            generation_id: 3,
            member_id: "m".into(),
            group_instance_id: None,
        };
        thread.commit_all(Some(&meta)).await.unwrap();

        // The cache flush at commit produced the deduped change into the txn: the
        // "out" sink got exactly ONE record (count = 2, the latest) AND the
        // "counts" changelog got exactly one entry.
        let sent = mock.sent.lock().unwrap();
        let out: Vec<_> = sent.iter().filter(|(t, ..)| t == "out").collect();
        check!(out.len() == 1, "exactly one deduped sink record");
        check!(
            out[0].3.as_deref() == Some([0, 0, 0, 0, 0, 0, 0, 2].as_ref()),
            "deduped sink value is the latest count (2)"
        );
        check!(
            sent.iter().any(|(t, ..)| t.contains("counts")),
            "cached store changelog must be produced into the transaction"
        );
        drop(sent);

        // The flush-produced Send happened BEFORE SendOffsets + Commit, so the
        // records are part of the committed transaction (not after it).
        let calls = mock.calls.lock().unwrap();
        let first_send = calls.iter().position(|s| *s == Step::Send);
        let send_offsets = calls.iter().position(|s| *s == Step::SendOffsets);
        let commit = calls.iter().position(|s| *s == Step::Commit);
        check!(
            first_send.is_some(),
            "the flush must produce at least one Send"
        );
        check!(
            first_send < send_offsets,
            "flush Send must precede SendOffsets"
        );
        check!(send_offsets < commit, "SendOffsets must precede Commit");
    }
}
