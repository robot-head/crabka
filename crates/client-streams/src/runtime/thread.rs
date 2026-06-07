use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex as TokioMutex;

use crate::error::StreamsClientError;
use crate::membership::StreamsAssignment;
use crate::runtime::io::{OffsetStore, RecordFetcher, RecordProducer};
use crate::runtime::iq::{IqRequest, answer_iq};
use crate::runtime::task::{StreamTask, TaskRole};
use crate::topology::BuiltTopology;

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
}

impl StreamThread {
    pub fn new(
        fetcher: Arc<dyn RecordFetcher>,
        backend: crate::store::backend::StoreBackend,
        application_id: String,
    ) -> Self {
        Self {
            tasks: HashMap::new(),
            fetcher,
            backend,
            application_id,
            globals: crate::runtime::global::GlobalStateManager::default(),
            globals_ready: false,
            global_offsets: std::collections::HashMap::new(),
            clock: Arc::new(crate::runtime::clock::SystemClock),
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

    /// Reconcile tasks to `assignment`. Reconciles active, standby, and warmup tasks.
    pub async fn apply_assignment(
        &mut self,
        assignment: &StreamsAssignment,
        topology: &BuiltTopology,
        producer: &Arc<dyn RecordProducer>,
        store: &Arc<dyn OffsetStore>,
    ) -> Result<(), StreamsClientError> {
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
                .instantiate(&self.backend, &self.application_id)
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
        for task in self.tasks.values_mut() {
            if task.role == TaskRole::Active {
                task.process_once(fetcher).await?;
            } else {
                task.restore_step(fetcher).await?;
            }
        }
        // Wall-clock punctuation tick: read the clock once, then fire every task's
        // due WALL_CLOCK_TIME punctuators. Forwarded records are produced through
        // each task's own producer (same plumbing as `process_once`).
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

    pub async fn commit_all(&mut self) -> Result<(), StreamsClientError> {
        for task in self.tasks.values_mut() {
            task.commit().await?;
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

    /// Commit + drop all tasks (on Fenced / shutdown).
    pub async fn close_all(&mut self) -> Result<(), StreamsClientError> {
        self.commit_all().await?;
        self.tasks.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::membership::{StreamsAssignment, TaskAssignment, TaskOffsetTracker, TopicPartition};
    use crate::processor::api::{Processor, ProcessorContext};
    use crate::processor::record::Record;
    use crate::processor::serde::{Consumed, I64Serde, Produced, StringSerde};
    use crate::runtime::io::{FetchBatch, FetchedRec, OffsetStore, RecordFetcher, RecordProducer};
    use crate::topology::Topology;
    use assert2::check;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::Mutex as StdMutex;

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
        let src = t.add_source("src", ["in"], Consumed::with(StringSerde, StringSerde));
        let up = t.add_processor("up", || Upper, [&src]);
        t.add_sink(
            "out",
            "out",
            [&up],
            Produced::with(StringSerde, StringSerde),
        );
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
        let src = t.add_source("src", ["in"], Consumed::with(StringSerde, StringSerde));
        let c = t.add_processor("c", || Counter, [&src]);
        t.add_state_store("counts", StringSerde, I64Serde, [c.name()]);
        t.add_sink("out", "out", [&c], Produced::with(StringSerde, I64Serde));
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
        let src = t.add_source("src", ["in"], Consumed::with(StringSerde, StringSerde));
        let p = t.add_processor("p", || WallClockScheduler, [&src]);
        t.add_sink("out", "out", [&p], Produced::with(StringSerde, I64Serde));
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
        )
        .with_clock(clock);
        thread
            .apply_assignment(&assignment(), &built, &producer, &store)
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
        );
        thread
            .apply_assignment(&assignment(), &built, &producer, &store)
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
        thread.commit_all().await.unwrap();
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
            .apply_assignment(&StreamsAssignment::default(), &built, &producer, &store)
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
        );
        thread
            .apply_assignment(&assignment(), &built, &producer, &store)
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
        thread.commit_all().await.unwrap();

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
        use crate::processor::serde::{I64Serde, Serde, StringSerde};
        use crate::runtime::iq::{IqError, IqOp, IqPayload, IqRequest};
        use crate::store::iq::StoreKind;

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
        );
        thread
            .apply_assignment(&assignment(), &built, &producer, &store)
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
                    key: StringSerde.serialize(&"a".to_string()),
                },
                reply,
            })
            .await;
        assert_eq!(
            rx.await.unwrap().unwrap(),
            IqPayload::Value(Some(I64Serde.serialize(&7_i64)))
        );

        // empty thread (no tasks) -> RebalanceInProgress
        let mut empty = StreamThread::new(
            empty_fetcher(),
            crate::store::backend::StoreBackend::InMemory,
            "app".into(),
        );
        let (reply2, rx2) = tokio::sync::oneshot::channel();
        empty
            .serve_iq(IqRequest {
                store: "counts".into(),
                kind: StoreKind::KeyValue,
                op: IqOp::KvGet {
                    key: StringSerde.serialize(&"a".to_string()),
                },
                reply: reply2,
            })
            .await;
        assert!(matches!(
            rx2.await.unwrap(),
            Err(IqError::RebalanceInProgress)
        ));
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
        use crate::dsl::{GlobalKTable, Materialized, StreamsBuilder};

        // Build the global-table join topology via the DSL.
        let b = StreamsBuilder::new();
        let g: GlobalKTable<String, String> = b.global_table(
            "global",
            Consumed::with(StringSerde, StringSerde),
            Materialized::with(StringSerde, StringSerde).as_store("g-store"),
        );
        b.stream(["in"], Consumed::with(StringSerde, StringSerde))
            .join_global(
                &g,
                |_k: &String, v: &String| v.clone(),
                |sv: &String, gv: &String| format!("{sv}{gv}"),
            )
            .to("out", Produced::with(StringSerde, StringSerde));
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
            .apply_assignment(&assignment, &built, &producer, &store)
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
        thread.commit_all().await.unwrap();

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
            .apply_assignment(&assignment1, &built, &producer, &store)
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
            .apply_assignment(&assignment2, &built, &producer, &store)
            .await
            .unwrap();
        check!(thread.task_count() == 3);

        check!(thread.tasks.get(&("0".to_string(), 0)).map(|t| t.role) == Some(TaskRole::Standby));
        check!(!thread.tasks.contains_key(&("0".to_string(), 1)));
        check!(thread.tasks.get(&("0".to_string(), 2)).map(|t| t.role) == Some(TaskRole::Active));
        check!(thread.tasks.get(&("0".to_string(), 3)).map(|t| t.role) == Some(TaskRole::Warmup));
    }
}
