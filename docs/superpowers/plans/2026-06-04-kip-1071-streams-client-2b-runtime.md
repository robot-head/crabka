# KIP-1071 Streams Client #2b — Broker-Backed Runtime — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A managed `KafkaStreams` runtime that joins a streams group (via #1 membership), fetches its assigned active-task source partitions, runs records through the #2a processor graph, produces to sink/repartition topics, and commits source offsets at **at-least-once**, reacting to rebalances.

**Architecture:** I/O is dependency-injected behind three traits (`RecordFetcher`, `RecordProducer`, `OffsetStore`) so `StreamTask`/`StreamThread` are pure orchestration, unit-testable with fakes (no broker). A single `StreamThread` owns one `StreamTask` per assigned active task `(subtopology_id, partition)`; each task holds a `BuiltTopology::instantiate()` graph and is fed only its assigned source-topic-partitions (so multi-subtopology/repartition topologies work — each subtopology is a separate active task fed its own topics). `KafkaStreams` wires the real broker-backed I/O impls + #1 membership.

**Tech Stack:** Rust 2024, tokio, `async-trait` (already a dep), `bytes`. Reuses `crabka-client-core` (`fetch_partition`/`Client`), `crabka-client-producer` (`Producer`), and `crabka-protocol` (OffsetCommit/OffsetFetch/ListOffsets). Builds on merged #2a.

**Spec:** `docs/superpowers/specs/2026-06-03-kip-1071-streams-client-2-execution-design.md` §5 (this is Phase 2b; 2a is merged).

**Branch:** `claude/streams-2b-runtime` (off `main` @ 0667730c, which has #2a).

---

## Scope of 2b

**In:** `RecordFetcher`/`RecordProducer`/`OffsetStore` traits + real broker impls; `StreamTask` (process batch → produce → commit, at-least-once); `StreamThread` (poll/commit loop + rebalance task add/remove); `KafkaStreams` handle (`start`/`close`/`state`); end-to-end broker integration test.

**Deferred:** per-subtopology graph *trimming* (each task instantiates the whole-topology graph but is only fed its own source partitions — correct, just memory-heavier; trim is an optimization for later); state stores (#3); EOS/transactions (#7); standby/warmup processing (#5); record-timestamp extraction from fetched batches (set to `-1` for now — client-core `fetch_partition` doesn't expose it; matters for windowed/stateful #3); cross-partition stream-time sync.

## File structure

```
crates/client-streams/src/
  runtime/                  NEW
    mod.rs                  re-exports (KafkaStreams, KafkaStreamsState, RuntimeError)
    io.rs                   RecordFetcher / RecordProducer / OffsetStore traits + FetchedRec/StartPosition
    task.rs                 StreamTask: graph + per-partition offsets; process_batch(); commit()
    thread.rs               StreamThread: task map; poll/commit loop; apply_assignment() add/remove
    io_broker.rs            real impls: BrokerFetcher, BrokerProducer, BrokerOffsetStore
    app.rs                  KafkaStreams handle (membership + thread + real I/O); start/close/state
  error.rs                  MODIFY: add RuntimeError variants (or reuse StreamsClientError)
  lib.rs                    MODIFY: pub mod runtime; re-exports
  tests/runtime_integration.rs  NEW: in-process broker end-to-end
```

## Reference signatures (verbatim — verified)

**#2a/#1 seams (this crate, `pub(crate)` reachable from `runtime/`):**
- `crate::topology::BuiltTopology::instantiate(&self) -> Result<crate::processor::graph::Graph, crate::processor::erased::ProcessorError>`; `.list_source_topics() -> Vec<String>`; `.to_wire() -> WireTopology`; `.application_id() -> &str`.
- `crate::processor::graph::Graph::pipe(&mut self, topic: &str, key: Option<&[u8]>, value: &[u8], timestamp: i64) -> Result<(), ProcessorError>`; `.take_output(&mut self) -> Vec<OutputRecord>`.
- `crate::processor::erased::OutputRecord { topic: String, key: Option<Bytes>, value: Option<Bytes>, timestamp: i64 }` (`pub(crate)`).
- `crate::membership::{StreamsMembership, StreamsEvent, StreamsAssignment, TaskAssignment, TopicPartition}`. `StreamsMembership::builder().bootstrap(s).group_id(s).topology(BuiltTopology).process_id(opt).instance_id(opt).rebalance_timeout(Duration).build().await -> Result<StreamsMembership, StreamsClientError>`; `.next_event().await -> Result<StreamsEvent, StreamsClientError>`; `.close().await`. `StreamsEvent::{Assigned(StreamsAssignment), NotReady(Vec<StreamsStatus>), Fenced}`. `TaskAssignment { subtopology_id: String, partitions: Vec<i32>, source_topic_partitions: Vec<TopicPartition> }`. `TopicPartition { topic: String, partition: i32 }`.

**Reuse (other crates):**
- `crabka_client_core::{Client, ClientError, Connection, ConnectionOptions, FetchedRecord, fetch_partition}`. `fetch_partition(conn: &Connection, topic: &str, topic_id: WireUuid, partition: i32, fetch_offset: i64, max_wait_ms: i32, partition_max_bytes: i32) -> Result<Vec<FetchedRecord>, ClientError>`; `FetchedRecord { offset: i64, key: Option<Bytes>, value: Option<Bytes> }` (NO timestamp). Leader routing: `Client::send(FetchRequest)` to bootstrap works for a single-broker test; for correctness use `client.refresh_metadata().await?` then `client.broker(leader_id).send(fetch_req)` (see `client-consumer/src/poll.rs`). For #2b, simplest correct path: build a `FetchRequest` and `client.send(...)` (single-broker test broker) OR reuse `fetch_partition` with a `Connection` to the bootstrap. Pick the in-process-broker-friendly path; the test broker is single-node.
- `crabka_client_producer::{Producer, ProducerRecord, Acks}`. `Producer::builder().bootstrap(s).enable_idempotence(true).acks(Acks::All).build().await -> Result<Producer, ProducerError>`. `producer.send(ProducerRecord { topic, partition: None, key: Option<Bytes>, value: Option<Bytes>, ..Default::default() }).await -> oneshot::Receiver<Result<RecordMetadata, ProducerError>>`. `producer.flush().await -> Result<(), ProducerError>`. (partition None → producer default partitioner.)
- OffsetCommit: `crabka_protocol::owned::offset_commit_request::{OffsetCommitRequest, OffsetCommitRequestTopic, OffsetCommitRequestPartition}`; send via `client.send(OffsetCommitRequest { group_id, generation_id_or_member_epoch: -1, member_id: String::new(), topics, ..Default::default() })`. Partition: `OffsetCommitRequestPartition { partition_index, committed_offset, committed_leader_epoch: -1, committed_metadata: Some(String::new()), ..Default::default() }`.
- OffsetFetch: `crabka_protocol::owned::offset_fetch_request::{OffsetFetchRequest, OffsetFetchRequestTopic}`; `client.send(OffsetFetchRequest { group_id, topics: Some(vec![OffsetFetchRequestTopic { name, partition_indexes, ..Default::default() }]), ..Default::default() })`; response `committed_offset == -1` means none.
- ListOffsets (auto.offset.reset = earliest): `crabka_protocol::owned::list_offsets_request::{ListOffsetsRequest, ListOffsetsTopic, ListOffsetsPartition}`; `client.send(ListOffsetsRequest { replica_id: -1, topics: vec![ListOffsetsTopic { name, partitions: vec![ListOffsetsPartition { partition_index, timestamp: -2 /* EARLIEST */, ..Default::default() }], ..Default::default() }], ..Default::default() })`; `resp.topics[0].partitions[0].offset` is the earliest offset.

**Decisions (from spec §8):** one shared `Producer` across tasks; no record headers in #2b; `auto.offset.reset = earliest` (JVM Streams default for app source topics); record timestamp `-1` (fetcher limitation, documented).

---

## Task 1: I/O traits

**Files:** Create `crates/client-streams/src/runtime/io.rs`, `crates/client-streams/src/runtime/mod.rs`; modify `lib.rs`.

- [ ] **Step 1: scaffold runtime module.** `runtime/mod.rs`:
```rust
//! Broker-backed execution runtime (sub-project #2b).
pub mod io;
mod task;
mod thread;
mod io_broker;
mod app;

pub use app::{KafkaStreams, KafkaStreamsState};
pub use io::{FetchedRec, FetchBatch, RecordFetcher, RecordProducer, OffsetStore};
```
Add `pub mod runtime;` + `pub use runtime::{KafkaStreams, KafkaStreamsState};` to `lib.rs`. (Create empty stub `task.rs`/`thread.rs`/`io_broker.rs`/`app.rs` with a `//! stub` line so the module compiles; later tasks fill them. For now, comment out the `pub use app::...` until app.rs exists, OR stub `app.rs` with `pub struct KafkaStreams; pub enum KafkaStreamsState { Created }`.)

- [ ] **Step 2: failing test** — append to `io.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::check;

    #[test]
    fn fetch_batch_next_offset_advances_past_last() {
        let b = FetchBatch { records: vec![
            FetchedRec { offset: 5, key: None, value: Some(bytes::Bytes::from_static(b"a")), timestamp: -1 },
            FetchedRec { offset: 6, key: None, value: Some(bytes::Bytes::from_static(b"b")), timestamp: -1 },
        ]};
        check!(b.next_offset(0) == 7);
        let empty = FetchBatch { records: vec![] };
        check!(empty.next_offset(9) == 9);
    }
}
```

- [ ] **Step 3: implement** `io.rs` (above tests):
```rust
//! Dependency-injected I/O the runtime depends on: fetching source records,
//! producing sink records, and committing/fetching offsets. Real broker impls
//! live in `io_broker.rs`; fakes in tests make `StreamTask`/`StreamThread`
//! deterministically testable without a broker.

use bytes::Bytes;

use crate::error::StreamsClientError;

/// A fetched source record (timestamp is `-1` when the fetcher can't surface it).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedRec {
    pub offset: i64,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub timestamp: i64,
}

/// A batch of consecutive records from one partition.
#[derive(Debug, Clone, Default)]
pub struct FetchBatch {
    pub records: Vec<FetchedRec>,
}

impl FetchBatch {
    /// The offset to fetch next: one past the last record, or `current` if empty.
    #[must_use]
    pub fn next_offset(&self, current: i64) -> i64 {
        self.records.last().map_or(current, |r| r.offset + 1)
    }
}

#[async_trait::async_trait]
pub trait RecordFetcher: Send + Sync + 'static {
    /// Fetch records for `(topic, partition)` starting at `offset`. An empty
    /// batch means nothing new yet.
    async fn fetch(&self, topic: &str, partition: i32, offset: i64) -> Result<FetchBatch, StreamsClientError>;
}

#[async_trait::async_trait]
pub trait RecordProducer: Send + Sync + 'static {
    /// Enqueue a record to `topic` (producer default partitioner).
    async fn send(&self, topic: &str, key: Option<Bytes>, value: Option<Bytes>) -> Result<(), StreamsClientError>;
    /// Block until all enqueued records are acknowledged (durability barrier).
    async fn flush(&self) -> Result<(), StreamsClientError>;
}

#[async_trait::async_trait]
pub trait OffsetStore: Send + Sync + 'static {
    /// Committed offset for `(topic, partition)`, or `None` if never committed.
    async fn committed(&self, topic: &str, partition: i32) -> Result<Option<i64>, StreamsClientError>;
    /// The earliest available offset (auto.offset.reset = earliest).
    async fn earliest(&self, topic: &str, partition: i32) -> Result<i64, StreamsClientError>;
    /// Commit `(topic, partition, offset)` triples for the streams group.
    async fn commit(&self, offsets: &[(String, i32, i64)]) -> Result<(), StreamsClientError>;
}
```
(Add `RuntimeError` to `error.rs` if you prefer a dedicated error; reusing `StreamsClientError` is simpler — add a `#[error("runtime: {0}")] Runtime(String)` variant to it for produce/commit failures.)

- [ ] **Step 4: run/clippy/commit.** `cargo test -p crabka-client-streams --lib runtime::io` PASS; clippy clean; fmt; commit `feat(streams-client): runtime I/O traits`.

---

## Task 2: StreamTask

**Files:** Modify `crates/client-streams/src/runtime/task.rs`.

A `StreamTask` owns one subtopology+partition's graph and the fetch offset per assigned source partition. `process_once` fetches each assigned partition, pipes records through the graph, produces sink outputs, and tracks offsets. `commit` flushes the producer then commits offsets (at-least-once).

- [ ] **Step 1: failing test** — `task.rs` test module with a **fake fetcher/producer/store**:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::processor::api::{Processor, ProcessorContext};
    use crate::processor::record::Record;
    use crate::processor::serde::StringSerde;
    use crate::runtime::io::{FetchBatch, FetchedRec, OffsetStore, RecordFetcher, RecordProducer};
    use crate::topology::Topology;
    use crate::membership::TopicPartition;
    use assert2::check;
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;

    struct Upper;
    impl Processor<String, String, String, String> for Upper {
        fn process(&mut self, ctx: &mut ProcessorContext<String, String>, r: Record<String, String>) {
            ctx.forward(Record::new(r.key, r.value.to_uppercase(), r.timestamp));
        }
    }
    fn built() -> crate::topology::BuiltTopology {
        let mut t = Topology::new();
        t.add_source("src", ["in"], StringSerde, StringSerde);
        t.add_processor("up", || Box::new(Upper), ["src"]);
        t.add_sink("out", "out", ["up"], StringSerde, StringSerde);
        t.build("app").unwrap()
    }

    // One-shot fetcher: returns the scripted batch once per partition, then empty.
    struct OneShot { batch: StdMutex<Option<FetchBatch>> }
    #[async_trait::async_trait]
    impl RecordFetcher for OneShot {
        async fn fetch(&self, _t: &str, _p: i32, _o: i64) -> Result<FetchBatch, crate::StreamsClientError> {
            Ok(self.batch.lock().unwrap().take().unwrap_or_default())
        }
    }
    #[derive(Default)] struct CollectProducer { sent: StdMutex<Vec<(String, Option<bytes::Bytes>)>>, flushes: StdMutex<u32> }
    #[async_trait::async_trait]
    impl RecordProducer for CollectProducer {
        async fn send(&self, topic: &str, _k: Option<bytes::Bytes>, v: Option<bytes::Bytes>) -> Result<(), crate::StreamsClientError> {
            self.sent.lock().unwrap().push((topic.to_string(), v)); Ok(())
        }
        async fn flush(&self) -> Result<(), crate::StreamsClientError> { *self.flushes.lock().unwrap() += 1; Ok(()) }
    }
    #[derive(Default)] struct MemStore { committed: StdMutex<HashMap<(String,i32), i64>> }
    #[async_trait::async_trait]
    impl OffsetStore for MemStore {
        async fn committed(&self, t: &str, p: i32) -> Result<Option<i64>, crate::StreamsClientError> { Ok(self.committed.lock().unwrap().get(&(t.to_string(), p)).copied()) }
        async fn earliest(&self, _t: &str, _p: i32) -> Result<i64, crate::StreamsClientError> { Ok(0) }
        async fn commit(&self, offs: &[(String,i32,i64)]) -> Result<(), crate::StreamsClientError> {
            let mut m = self.committed.lock().unwrap(); for (t,p,o) in offs { m.insert((t.clone(), *p), *o); } Ok(())
        }
    }

    #[tokio::test]
    async fn processes_batch_produces_and_commits() {
        let producer = std::sync::Arc::new(CollectProducer::default());
        let store = std::sync::Arc::new(MemStore::default());
        let fetcher = OneShot { batch: StdMutex::new(Some(FetchBatch { records: vec![
            FetchedRec { offset: 0, key: Some("k".into()), value: Some("hi".into()), timestamp: -1 },
        ]})) };
        let mut task = StreamTask::new(
            "0".into(),
            built().instantiate().unwrap(),
            vec![TopicPartition { topic: "in".into(), partition: 0 }],
            std::sync::Arc::clone(&producer) as std::sync::Arc<dyn RecordProducer>,
            std::sync::Arc::clone(&store) as std::sync::Arc<dyn OffsetStore>,
        );
        task.seek_to_start(&*store).await.unwrap();   // no committed → earliest (0)
        task.process_once(&fetcher).await.unwrap();    // fetch+pipe+produce
        task.commit().await.unwrap();                  // flush + commit
        check!(producer.sent.lock().unwrap().iter().any(|(t, v)| t == "out" && v.as_deref() == Some(b"HI".as_ref())));
        check!(*producer.flushes.lock().unwrap() >= 1);
        check!(store.committed.lock().unwrap().get(&("in".to_string(), 0)) == Some(&1)); // next offset after offset 0
    }
}
```

- [ ] **Step 2: run → FAIL.** `cargo test -p crabka-client-streams --lib runtime::task`

- [ ] **Step 3: implement** `task.rs`:
```rust
//! A `StreamTask` = one active task `(subtopology_id, partition)`. Owns the
//! instantiated graph + fetch offsets for its assigned source partitions.
//! At-least-once: produce → flush → commit.

use std::collections::HashMap;
use std::sync::Arc;

use crate::error::StreamsClientError;
use crate::membership::TopicPartition;
use crate::processor::graph::Graph;
use crate::runtime::io::{OffsetStore, RecordFetcher, RecordProducer};

pub(crate) struct StreamTask {
    subtopology_id: String,
    graph: Graph,
    /// Assigned source partitions → next fetch offset.
    positions: HashMap<(String, i32), i64>,
    /// Offsets advanced since the last commit (to commit on `commit()`).
    pending: HashMap<(String, i32), i64>,
    producer: Arc<dyn RecordProducer>,
    store: Arc<dyn OffsetStore>,
}

impl StreamTask {
    pub fn new(
        subtopology_id: String,
        graph: Graph,
        sources: Vec<TopicPartition>,
        producer: Arc<dyn RecordProducer>,
        store: Arc<dyn OffsetStore>,
    ) -> Self {
        let positions = sources.into_iter().map(|tp| ((tp.topic, tp.partition), 0)).collect();
        Self { subtopology_id, graph, positions, pending: HashMap::new(), producer, store }
    }

    #[must_use]
    pub fn subtopology_id(&self) -> &str { &self.subtopology_id }

    /// Seek each assigned partition to its committed offset, or `earliest` if
    /// none (auto.offset.reset = earliest).
    pub async fn seek_to_start(&self, store: &dyn OffsetStore) -> Result<(), StreamsClientError> {
        // (positions is set per-partition; this fills the starting offsets)
        let mut starts = HashMap::new();
        for (topic, partition) in self.positions.keys() {
            let start = match store.committed(topic, *partition).await? {
                Some(o) => o,
                None => store.earliest(topic, *partition).await?,
            };
            starts.insert((topic.clone(), *partition), start);
        }
        // SAFETY: interior mutate — make `positions` a field we can set. Simplest:
        // take &mut self instead of &self. Change the signature to &mut self and
        // assign self.positions = starts.
        // (Implementer: make `seek_to_start(&mut self, store)` and `self.positions = starts;`)
        let _ = starts;
        Ok(())
    }

    /// Fetch one batch from each assigned partition, pipe records through the
    /// graph, produce sink outputs, and advance offsets.
    pub async fn process_once(&mut self, fetcher: &dyn RecordFetcher) -> Result<(), StreamsClientError> {
        let keys: Vec<(String, i32)> = self.positions.keys().cloned().collect();
        for (topic, partition) in keys {
            let offset = self.positions[&(topic.clone(), partition)];
            let batch = fetcher.fetch(&topic, partition, offset).await?;
            for rec in &batch.records {
                self.graph
                    .pipe(&topic, rec.key.as_deref(), rec.value.as_deref().unwrap_or(&[]), rec.timestamp)
                    .map_err(|e| StreamsClientError::Runtime(e.to_string()))?;
                for out in self.graph.take_output() {
                    self.producer.send(&out.topic, out.key, out.value).await?;
                }
            }
            let next = batch.next_offset(offset);
            self.positions.insert((topic.clone(), partition), next);
            self.pending.insert((topic, partition), next);
        }
        Ok(())
    }

    /// At-least-once commit: flush the producer (durably send sink/repartition
    /// records) THEN commit the advanced source offsets.
    pub async fn commit(&mut self) -> Result<(), StreamsClientError> {
        self.producer.flush().await?;
        if self.pending.is_empty() {
            return Ok(());
        }
        let offsets: Vec<(String, i32, i64)> =
            self.pending.iter().map(|((t, p), o)| (t.clone(), *p, *o)).collect();
        self.store.commit(&offsets).await?;
        self.pending.clear();
        Ok(())
    }
}
```
NOTE for implementer: change `seek_to_start` to `&mut self` and assign `self.positions = starts;` (the inline comment marks where). The test calls `task.seek_to_start(&*store)` — adjust to `&mut` (the test has `let mut task`). Also add the `Runtime(String)` variant to `StreamsClientError` in `error.rs` if not present.

- [ ] **Step 4: run → PASS; clippy; fmt; commit** `feat(streams-client): StreamTask (process/produce/commit, at-least-once)`.

---

## Task 3: StreamThread

**Files:** Modify `crates/client-streams/src/runtime/thread.rs`.

Owns a map of `(subtopology_id, partition) -> StreamTask`. `apply_assignment` diffs the active set: create added tasks (seek to start), drop removed (flush+commit first). `run` polls all tasks and commits on an interval, until a shutdown signal.

- [ ] **Step 1: failing test** — `thread.rs` tests reuse the fakes from task.rs's tests (duplicate the fake structs in this module, or move them to a shared `#[cfg(test)]` helper module `runtime/test_support.rs`). Test that `apply_assignment` creates a task per active TaskAssignment and `poll_all` processes them:
```rust
    #[tokio::test]
    async fn apply_assignment_creates_tasks_and_polls() {
        // build topology, producer/store/fetcher fakes (as in task.rs tests)
        // assignment with one active task subtopology "0", partition 0, source ("in",0)
        // thread.apply_assignment(&assignment, &built, &producer, &store).await.unwrap();
        // check task count == 1
        // thread.poll_all(&fetcher).await.unwrap(); thread.commit_all().await.unwrap();
        // assert producer received the "out" record + store committed ("in",0)=1
    }
```
(Write the full test mirroring task.rs's fakes + a `StreamsAssignment { active: vec![TaskAssignment { subtopology_id: "0".into(), partitions: vec![0], source_topic_partitions: vec![TopicPartition{topic:"in".into(),partition:0}] }], standby: vec![], warmup: vec![] }`.)

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement** `thread.rs`:
```rust
//! Owns the active `StreamTask`s and drives the poll/commit loop. Reacts to
//! assignment changes from the membership.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::error::StreamsClientError;
use crate::membership::StreamsAssignment;
use crate::runtime::io::{OffsetStore, RecordFetcher, RecordProducer};
use crate::runtime::task::StreamTask;
use crate::topology::BuiltTopology;

pub(crate) struct StreamThread {
    tasks: HashMap<(String, i32), StreamTask>,
}

impl StreamThread {
    pub fn new() -> Self { Self { tasks: HashMap::new() } }

    #[must_use]
    pub fn task_count(&self) -> usize { self.tasks.len() }

    /// Reconcile the active task set to `assignment.active`: drop removed tasks
    /// (flush+commit), create added (seek to start). standby/warmup ignored.
    pub async fn apply_assignment(
        &mut self,
        assignment: &StreamsAssignment,
        topology: &BuiltTopology,
        producer: &Arc<dyn RecordProducer>,
        store: &Arc<dyn OffsetStore>,
    ) -> Result<(), StreamsClientError> {
        // Desired key set.
        let mut desired: HashMap<(String, i32), &crate::membership::TaskAssignment> = HashMap::new();
        for ta in &assignment.active {
            for &p in &ta.partitions {
                desired.insert((ta.subtopology_id.clone(), p), ta);
            }
        }
        // Drop removed (commit first).
        let to_remove: Vec<(String, i32)> = self.tasks.keys().filter(|k| !desired.contains_key(*k)).cloned().collect();
        for k in to_remove {
            if let Some(mut t) = self.tasks.remove(&k) {
                t.commit().await?;
            }
        }
        // Add new.
        for (key, ta) in desired {
            if self.tasks.contains_key(&key) {
                continue;
            }
            let graph = topology.instantiate().map_err(|e| StreamsClientError::Runtime(e.to_string()))?;
            // This task's source partitions are the assignment's source_topic_partitions
            // filtered to this partition index (key.1).
            let sources: Vec<crate::membership::TopicPartition> = ta
                .source_topic_partitions
                .iter()
                .filter(|tp| tp.partition == key.1)
                .cloned()
                .collect();
            let mut task = StreamTask::new(key.0.clone(), graph, sources, Arc::clone(producer), Arc::clone(store));
            task.seek_to_start(store).await?;
            self.tasks.insert(key, task);
        }
        Ok(())
    }

    pub async fn poll_all(&mut self, fetcher: &dyn RecordFetcher) -> Result<(), StreamsClientError> {
        for task in self.tasks.values_mut() {
            task.process_once(fetcher).await?;
        }
        Ok(())
    }

    pub async fn commit_all(&mut self) -> Result<(), StreamsClientError> {
        for task in self.tasks.values_mut() {
            task.commit().await?;
        }
        Ok(())
    }

    /// Flush+commit all and clear (on Fenced / shutdown).
    pub async fn close_all(&mut self) -> Result<(), StreamsClientError> {
        self.commit_all().await?;
        self.tasks.clear();
        Ok(())
    }
}
```
NOTE: `seek_to_start` is `&mut self` (Task 2) — call `task.seek_to_start(store).await?` where `store: &Arc<dyn OffsetStore>` (deref to `&dyn`). Adjust signatures so this compiles (pass `store.as_ref()` / `&**store`).

- [ ] **Step 4: run → PASS; clippy; fmt; commit** `feat(streams-client): StreamThread (assignment reconcile + poll/commit)`.

---

## Task 4: Real broker I/O impls

**Files:** Modify `crates/client-streams/src/runtime/io_broker.rs`. Modify `Cargo.toml` (add `crabka-client-producer` dep).

Implement the three traits against the broker. `BrokerFetcher` wraps a `Client` (fetch via FetchRequest to the broker, resolving topic ids from metadata); `BrokerProducer` wraps `crabka_client_producer::Producer`; `BrokerOffsetStore` wraps a `Client` + the group id (OffsetCommit/OffsetFetch/ListOffsets).

- [ ] **Step 1:** Add to `crates/client-streams/Cargo.toml` `[dependencies]`: `crabka-client-producer = { version = "0.2", path = "../client-producer" }`.

- [ ] **Step 2: implement** `io_broker.rs` (no unit test here — exercised by the Task 6 integration test; verify via build + clippy). Use the reference signatures above:
  - `BrokerFetcher { client: Client, max_wait_ms: i32, max_bytes: i32 }` impl `RecordFetcher::fetch`: build a `crabka_protocol::owned::fetch_request::FetchRequest` for `(topic, partition, offset)` and `client.send(...)`, decode the returned record batches into `FetchedRec` (offset, key, value, timestamp from the batch if available else `-1`), OR reuse `crabka_client_core::fetch_partition` with a `Connection` + the topic id from `client.refresh_metadata()`. Map `ClientError` → `StreamsClientError::Transport`. (Read `client-consumer/src/poll.rs` for the exact Fetch decode if building the request directly; reusing `fetch_partition` is fewer moving parts — resolve topic_id via metadata once and cache it.)
  - `BrokerProducer { inner: crabka_client_producer::Producer }` impl: `send` → `self.inner.send(ProducerRecord { topic: topic.into(), partition: None, key, value, ..Default::default() }).await;` (drop the returned receiver — at-least-once relies on `flush`); `flush` → `self.inner.flush().await.map_err(|e| StreamsClientError::Runtime(e.to_string()))`.
  - `BrokerOffsetStore { client: Client, group_id: String }` impl: `committed` → OffsetFetch (return None when `committed_offset < 0`); `earliest` → ListOffsets timestamp `-2`; `commit` → OffsetCommit with `generation_id_or_member_epoch: -1, member_id: ""`.

  Provide `pub(crate) async fn build(bootstrap: &str, group_id: &str, client_id: &str) -> Result<(BrokerFetcher, Arc<BrokerProducer>, Arc<BrokerOffsetStore>), StreamsClientError>` that constructs the `Client`(s) + `Producer` (one shared producer).

- [ ] **Step 3:** `cargo build -p crabka-client-streams` + `cargo clippy -p crabka-client-streams --all-targets -- -D warnings` clean. fmt. Commit `feat(streams-client): broker-backed runtime I/O impls`.

---

## Task 5: KafkaStreams handle

**Files:** Modify `crates/client-streams/src/runtime/app.rs`.

Owns `StreamsMembership` + `StreamThread` + the real I/O. `start()` spawns a supervisor task that: joins is already done by membership builder; loops `membership.next_event()`; on `Assigned` → `thread.apply_assignment(...)`; on `Fenced` → `thread.close_all()`; on `NotReady` → idle; and on each loop tick also `thread.poll_all(&fetcher)` + periodic `commit_all()`. `close()` cancels + `thread.close_all()` + `membership.close()`.

- [ ] **Step 1: implement** `app.rs` (no isolated unit test — covered by Task 6; verify build+clippy):
```rust
//! `KafkaStreams` — the managed runtime handle. Owns membership + a StreamThread.

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::error::StreamsClientError;
use crate::membership::{StreamsEvent, StreamsMembership};
use crate::runtime::io::{OffsetStore, RecordFetcher, RecordProducer};
use crate::runtime::io_broker;
use crate::runtime::thread::StreamThread;
use crate::topology::BuiltTopology;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KafkaStreamsState { Created, Running, Closed }

pub struct KafkaStreams {
    shutdown: CancellationToken,
    handle: Option<JoinHandle<()>>,
    membership_member_id: String,
    state: KafkaStreamsState,
}

#[bon::bon]
impl KafkaStreams {
    #[builder(start_fn = builder, finish_fn = build)]
    pub async fn start(
        #[builder(into)] bootstrap: String,
        #[builder(into)] application_id: String,   // == group id
        topology: BuiltTopology,
        #[builder(default = Duration::from_millis(200))] poll_interval: Duration,
        #[builder(default = Duration::from_secs(5))] commit_interval: Duration,
    ) -> Result<Self, StreamsClientError> {
        // 1. Build broker I/O (one shared producer).
        let (fetcher, producer, store) = io_broker::build(&bootstrap, &application_id, &application_id).await?;
        let fetcher: Arc<dyn RecordFetcher> = Arc::new(fetcher);
        let producer: Arc<dyn RecordProducer> = producer;
        let store: Arc<dyn OffsetStore> = store;

        // 2. Join the streams group (membership owns the heartbeat).
        let mut membership = StreamsMembership::builder()
            .bootstrap(&bootstrap)
            .group_id(&application_id)
            .topology(topology.clone_for_membership()) // see note
            .build()
            .await?;
        let member_id = membership.member_id().to_string();

        // 3. Supervisor loop.
        let topology = Arc::new(topology);
        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        let handle = tokio::spawn(async move {
            let mut thread = StreamThread::new();
            let mut ticker = tokio::time::interval(poll_interval);
            let mut commit_ticker = tokio::time::interval(commit_interval);
            loop {
                tokio::select! {
                    () = sd.cancelled() => { let _ = thread.close_all().await; let _ = membership.close().await; break; }
                    ev = membership.next_event() => match ev {
                        Ok(StreamsEvent::Assigned(a)) => { let _ = thread.apply_assignment(&a, &topology, &producer, &store).await; }
                        Ok(StreamsEvent::Fenced) => { let _ = thread.close_all().await; }
                        Ok(StreamsEvent::NotReady(_)) => {}
                        Err(_) => break,
                    },
                    _ = ticker.tick() => { let _ = thread.poll_all(&*fetcher).await; }
                    _ = commit_ticker.tick() => { let _ = thread.commit_all().await; }
                }
            }
        });

        Ok(Self { shutdown, handle: Some(handle), membership_member_id: member_id, state: KafkaStreamsState::Running })
    }
}

impl KafkaStreams {
    #[must_use]
    pub fn member_id(&self) -> &str { &self.membership_member_id }
    #[must_use]
    pub fn state(&self) -> KafkaStreamsState { self.state }

    pub async fn close(&mut self) -> Result<(), StreamsClientError> {
        self.shutdown.cancel();
        if let Some(h) = self.handle.take() { let _ = h.await; }
        self.state = KafkaStreamsState::Closed;
        Ok(())
    }
}
```
NOTES for implementer:
- `BuiltTopology` is NOT `Clone` (it holds closures). The supervisor needs the topology to `instantiate()` per task, and membership needs `to_wire()`. Resolve by: build the wire topology + pass the `BuiltTopology` into the supervisor via `Arc`, and have `StreamsMembership::builder().topology(...)` take what it needs. Since membership wraps the topology in `Arc` internally and only calls `to_wire()` at join, **change membership to accept `Arc<BuiltTopology>`** OR have `KafkaStreams` wrap the single `BuiltTopology` in `Arc` and share it between the membership join and the supervisor. Simplest: membership's builder already moves the topology; instead, KafkaStreams holds `Arc<BuiltTopology>`, calls `built.to_wire()` to get the wire topology for the join, and the supervisor uses the same `Arc` for `instantiate()`. Adjust `StreamsMembership` to accept the wire `Topology` directly OR an `Arc<BuiltTopology>` (check `membership/client.rs` — it currently takes `BuiltTopology` by value and `Arc`s it; change to `Arc<BuiltTopology>` so it can be shared). Delete the fictional `clone_for_membership()` — it doesn't exist; wire the `Arc` sharing instead.
- The `select!` polls/commits on tickers AND drains membership events; `next_event` being `&mut membership` and the supervisor owning membership is fine (membership moved into the task).

- [ ] **Step 2:** build + clippy clean; fmt; commit `feat(streams-client): KafkaStreams managed runtime handle`.

---

## Task 6: In-process broker integration test

**Files:** Create `crates/client-streams/tests/runtime_integration.rs`.

Reuse the #1 broker-boot + `finalize_streams_version` + `create_topic` helpers (copy from `crates/broker/tests/streams_groups.rs` / the existing `tests/integration.rs`). End-to-end: create input+output topics, produce input records (via a `crabka_client_producer::Producer` in the test), start a `KafkaStreams` upper-casing app, assert the upper-cased records appear on the output topic, then close.

- [ ] **Step 1: write the test** (`#![cfg(not(target_os = "windows"))]`, `#[tokio::test(flavor = "multi_thread", worker_threads = 2)]`):
  - Boot broker; `finalize_streams_version`; `create_topic("stream-in", 1)`, `create_topic("stream-out", 1)`.
  - Produce 3 records to `stream-in` (keys/values) via a test `Producer`.
  - Build the upper-case topology (`add_source("src",["stream-in"],StringSerde,StringSerde)`, `add_processor("up", || Box::new(Upper), ["src"])`, `add_sink("out","stream-out",["up"],StringSerde,StringSerde)`, `build("stream-app")`).
  - `KafkaStreams::builder().bootstrap(addr).application_id("stream-app").topology(built).build().await?`.
  - Consume from `stream-out` (via `crabka_client_core::fetch_partition` or a test consumer) with a timeout (~20s); assert the 3 upper-cased values appear.
  - Optionally assert committed offsets on `stream-app` advanced (OffsetFetch).
  - `streams.close().await?`; `broker.shutdown().await`.
- If the app doesn't converge/produce within the timeout, debug (don't weaken): confirm `finalize_streams_version`, topic creation, and that `Assigned` fired (the app needs an active task for `stream-in`). If a real bug surfaces (e.g. records fetched but not produced), report it.

- [ ] **Step 2:** `cargo test -p crabka-client-streams --test runtime_integration -- --nocapture` → PASS. Add dev-deps to `Cargo.toml` if needed (`crabka-client-producer` for the test producer — already a normal dep after Task 4). Commit `test(streams-client): end-to-end KafkaStreams broker integration`.

---

## Task 7: Docs + final verification

**Files:** Modify `crates/client-streams/src/lib.rs`.

- [ ] **Step 1:** Add a `## Running an app (KafkaStreams)` doc section to `lib.rs` (a `no_run` example: build a topology, `KafkaStreams::builder()...build().await?`, then `close()`). Mark `no_run` (needs a broker).
- [ ] **Step 2: full verification.** `cargo test -p crabka-client-streams` (all: unit incl runtime::{io,task,thread} + #2a + membership integration + runtime integration + golden frame + doctests); `cargo fmt -p crabka-client-streams -- --check`; `cargo clippy -p crabka-client-streams --all-targets -- -D warnings`; `cargo build --workspace`.
- [ ] **Step 3: commit** `docs(streams-client): KafkaStreams runtime example + 2b verification`.

---

## Self-review

**Spec §5 coverage:**
- §5.1 StreamTask (process → produce → commit, at-least-once flush-before-commit) → Task 2. ✓
- §5.2 StreamThread (single tokio task, poll/commit, fetch per assigned partition) → Tasks 3, 5. ✓
- §5.3 rebalance (Assigned diff add/remove, Fenced close-all, NotReady idle; standby/warmup ignored) → Tasks 3, 5. ✓
- §5.4 KafkaStreams (membership + thread + shared producer; start/close/state) → Task 5. ✓
- §5 at-least-once + offset seek (committed else earliest) → Tasks 2, 4. ✓
- Repartition (uniform per-task; downstream subtopology consumes the internal topic) → handled: each active task is fed only its assigned source partitions, so a repartition subtopology is a separate task fed the repartition topic. ✓
- Testing: DI fakes (unit) + in-process broker (integration) → Tasks 2, 3, 6. ✓

**Deferred (noted in scope):** per-subtopology graph trimming (whole-graph-per-task is correct but memory-heavier), record timestamp (-1), state stores/EOS/standby/IQ.

**Placeholder notes:** Task 2 flags the `seek_to_start` `&self`→`&mut self` change inline; Task 5 explicitly deletes the fictional `clone_for_membership()` and specifies the real `Arc<BuiltTopology>` sharing (requires a small `membership/client.rs` change to accept `Arc<BuiltTopology>` — call it out as part of Task 5). Task 4 references existing reuse signatures rather than re-deriving them. These are flagged, not silent.

**Type consistency:** `FetchedRec`/`FetchBatch`/`RecordFetcher`/`RecordProducer`/`OffsetStore` (T1) are used identically in T2/T3/T4. `StreamTask::new(subtopology_id, Graph, Vec<TopicPartition>, Arc<dyn RecordProducer>, Arc<dyn OffsetStore>)` (T2) matches the call in T3. `StreamThread::{apply_assignment, poll_all, commit_all, close_all}` (T3) match T5's supervisor calls. `KafkaStreams::builder().bootstrap().application_id().topology().build()` (T5) matches T6's usage. ✓

**Known risk:** the `BuiltTopology` non-Clone + `Arc` sharing between membership-join and the supervisor (Task 5) needs a small `membership/client.rs` signature change (`Arc<BuiltTopology>`); flagged. The real Fetch decode (Task 4) is the riskiest I/O glue — the DI design means it's the ONLY broker-coupled code, exercised by Task 6.
