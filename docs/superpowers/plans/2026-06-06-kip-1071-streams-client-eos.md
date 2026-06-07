# Exactly-once (EOS v2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run the streams runtime exactly-once under `processing.guarantee=exactly_once_v2` — produce sink + changelog records and commit source offsets in one Kafka transaction per `StreamThread` per commit interval, with abort + state rollback on failure.

**Architecture:** EOS v2 / KIP-447: one transactional producer per thread (the producer is already a shared `Arc` across the thread's tasks). The thread drives `begin → produce (sink + changelog) → send_offsets_to_transaction → commit`; on any error it aborts, rewinds source offsets, and rolls back stores by wiping + re-restoring from the committed changelog (read_committed). The at-least-once path is the default and is untouched.

**Tech Stack:** Rust, `async_trait`, `bon` builder; the existing `RecordFetcher`/`RecordProducer`/`OffsetStore` DI traits; the native `crabka-client-producer` transactional API + `crabka-client-consumer::ConsumerGroupMetadata`; the Crabka broker's txn coordinator (`crates/broker/src/txn/`).

**Branch:** `streams-eos` (off `origin/main`). Worktree `/Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl`. Commit `-c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com"`; never `git config`; never edit the main repo; subagents assert the branch + use `git -C`.

**Spec:** `docs/superpowers/specs/2026-06-06-kip-1071-streams-client-eos-design.md`

---

## File structure

- **Create** `crates/client-streams/src/runtime/eos.rs` — `ProcessingGuarantee`, `StreamsGroupMeta`, `TransactionalProducer` trait, `transactional_id(...)`, `MockTransactionalProducer` (test).
- **Modify** `crates/client-streams/src/runtime/mod.rs` — `pub(crate) mod eos;` + re-export `ProcessingGuarantee`.
- **Modify** `crates/client-streams/src/runtime/io.rs` — `IsolationLevel` enum + `RecordFetcher::fetch` gains it.
- **Modify** `crates/client-streams/src/runtime/io_broker.rs` — `BrokerTransactionalProducer` + `build_eos`; `BrokerFetcher` honors isolation.
- **Modify** `crates/client-streams/src/runtime/task.rs` — `pending_offsets`/`clear_pending`/`rollback`; restore honors isolation.
- **Modify** `crates/client-streams/src/runtime/thread.rs` — `ProcessingGuarantee` + EOS txn lifecycle in `poll_all`/`commit_all`/abort.
- **Modify** `crates/client-streams/src/runtime/app.rs` — `processing_guarantee` builder field; EOS wiring + `group_metadata` per commit.
- **Modify** `crates/client-streams/src/membership/client.rs` — `group_metadata()` accessor.
- **Modify** `crates/client-streams/src/store/api.rs` (+ impls) — `clear()` on the store trait (for rollback).
- **Modify** `crates/client-streams/src/lib.rs` — re-export `ProcessingGuarantee` + docs.
- **Create** `crates/client-streams/tests/eos_broker.rs` — single-broker EOS integration test.

---

## Task 1: `ProcessingGuarantee` + `TransactionalProducer` trait + mock + config

**Files:**
- Create: `crates/client-streams/src/runtime/eos.rs`
- Modify: `crates/client-streams/src/runtime/mod.rs`, `crates/client-streams/src/runtime/app.rs`, `crates/client-streams/src/lib.rs`

- [ ] **Step 1: `runtime/eos.rs`.** Read `runtime/io.rs` for the `RecordProducer` trait shape + `StreamsClientError` usage.

```rust
//! Exactly-once (EOS v2 / KIP-447) primitives: the processing-guarantee config,
//! the transactional-producer I/O seam, and the streams group metadata used by
//! `send_offsets_to_transaction`. Wired into the runtime in T2/T3.
use async_trait::async_trait;
use bytes::Bytes;

use crate::error::StreamsClientError;

/// Delivery guarantee for the runtime (`processing.guarantee`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProcessingGuarantee {
    /// Produce-then-commit; a crash mid-cycle may replay (the default).
    #[default]
    AtLeastOnce,
    /// Transactional: produce + offset-commit atomically (KIP-447).
    ExactlyOnceV2,
}

/// Streams group metadata for `send_offsets_to_transaction` (maps to the native
/// `crabka_client_consumer::ConsumerGroupMetadata`).
#[derive(Debug, Clone)]
pub struct StreamsGroupMeta {
    pub group_id: String,
    /// The member epoch (next-gen "generation").
    pub generation_id: i32,
    pub member_id: String,
    pub group_instance_id: Option<String>,
}

/// EOS-v2 transactional producer seam (DI'd; `BrokerTransactionalProducer` in T2,
/// `MockTransactionalProducer` for tests). Also impls `RecordProducer` for `send`.
#[async_trait]
pub trait TransactionalProducer: crate::runtime::io::RecordProducer {
    async fn init_transactions(&self) -> Result<(), StreamsClientError>;
    async fn begin_transaction(&self) -> Result<(), StreamsClientError>;
    async fn send_offsets_to_transaction(
        &self,
        offsets: &[(String, i32, i64)],
        group_meta: &StreamsGroupMeta,
    ) -> Result<(), StreamsClientError>;
    async fn commit_transaction(&self) -> Result<(), StreamsClientError>;
    async fn abort_transaction(&self) -> Result<(), StreamsClientError>;
}

/// KIP-447 transactional id: stable per (application, thread) so a restart fences
/// a zombie via the producer-epoch bump in `init_transactions`.
#[must_use]
pub fn transactional_id(application_id: &str, thread_idx: usize) -> String {
    format!("{application_id}-{thread_idx}")
}

#[cfg(test)]
pub(crate) mod mock {
    use super::*;
    use std::sync::Mutex;

    /// Records the call sequence; can be told to fail at a chosen call so the
    /// abort/rollback path is testable. `Step` names each txn-control call.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Step { Init, Begin, Send, SendOffsets, Commit, Abort }

    #[derive(Default)]
    pub struct MockTransactionalProducer {
        pub calls: Mutex<Vec<Step>>,
        pub sent: Mutex<Vec<(String, Option<i32>, Option<Bytes>, Option<Bytes>)>>,
        /// If set, the first time this Step is reached it returns an error.
        pub fail_at: Mutex<Option<Step>>,
    }
    impl MockTransactionalProducer {
        fn record(&self, s: Step) -> Result<(), StreamsClientError> {
            self.calls.lock().unwrap().push(s);
            let mut f = self.fail_at.lock().unwrap();
            if *f == Some(s) { *f = None; return Err(StreamsClientError::Runtime(format!("mock fail at {s:?}"))); }
            Ok(())
        }
    }
    #[async_trait]
    impl crate::runtime::io::RecordProducer for MockTransactionalProducer {
        async fn send(&self, topic: &str, partition: Option<i32>, key: Option<Bytes>, value: Option<Bytes>)
            -> Result<(), StreamsClientError> {
            self.sent.lock().unwrap().push((topic.to_string(), partition, key, value));
            self.record(Step::Send)
        }
        async fn flush(&self) -> Result<(), StreamsClientError> { Ok(()) }
    }
    #[async_trait]
    impl TransactionalProducer for MockTransactionalProducer {
        async fn init_transactions(&self) -> Result<(), StreamsClientError> { self.record(Step::Init) }
        async fn begin_transaction(&self) -> Result<(), StreamsClientError> { self.record(Step::Begin) }
        async fn send_offsets_to_transaction(&self, _o: &[(String, i32, i64)], _m: &StreamsGroupMeta)
            -> Result<(), StreamsClientError> { self.record(Step::SendOffsets) }
        async fn commit_transaction(&self) -> Result<(), StreamsClientError> { self.record(Step::Commit) }
        async fn abort_transaction(&self) -> Result<(), StreamsClientError> { self.record(Step::Abort) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::check;

    #[test]
    fn transactional_id_is_stable_per_thread() {
        check!(transactional_id("word-count", 0) == "word-count-0");
        check!(transactional_id("word-count", 1) == "word-count-1");
    }

    #[tokio::test]
    async fn mock_records_calls_and_can_fail() {
        use mock::{MockTransactionalProducer, Step};
        use crate::runtime::io::RecordProducer;
        let p = MockTransactionalProducer { fail_at: std::sync::Mutex::new(Some(Step::Commit)), ..Default::default() };
        p.begin_transaction().await.unwrap();
        p.send("out", None, None, None).await.unwrap();
        check!(p.commit_transaction().await.is_err());          // fails once at Commit
        check!(*p.calls.lock().unwrap() == vec![Step::Begin, Step::Send, Step::Commit]);
    }
}
```

- [ ] **Step 2: register + config.** `runtime/mod.rs`: `pub(crate) mod eos;`. `lib.rs`: `pub use runtime::eos::ProcessingGuarantee;` (next to `KafkaStreams` re-export). `app.rs`: add `#[builder(default)] processing_guarantee: ProcessingGuarantee` to the `start` builder args (default `AtLeastOnce`); thread it no further yet (T3 consumes it) — add `#[allow(unused_variables)]` or `let _ = processing_guarantee;` with a `// consumed in T3` note so it compiles cleanly.

- [ ] **Step 3: verify + commit.** `cargo test -p crabka-client-streams --lib eos` (2 pass); `cargo test -p crabka-client-streams`; `cargo clippy -p crabka-client-streams --all-targets -- -D warnings`; `cargo fmt`. Commit `feat(streams): EOS config + TransactionalProducer seam + transactional.id + mock`.

---

## Task 2: `BrokerTransactionalProducer` + `build_eos`

**Files:**
- Modify: `crates/client-streams/src/runtime/io_broker.rs`

- [ ] **Step 1: read** `io_broker.rs` `BrokerProducer` (lines ~119–190: `send`/`flush` over `crabka_client_producer::Producer`) + `build` (~426–490: builds `Producer::builder()...`). Read `crates/client-producer/src/producer.rs` for `init_transactions`/`begin_transaction`/`send_offsets_to_transaction(offsets: impl IntoIterator<Item=((String,i32),i64)>, &ConsumerGroupMetadata)`/`commit_transaction`/`abort_transaction`, and `crates/client-producer/src/builder.rs` `transactional_id(Option<String>)`. `ConsumerGroupMetadata` is `crabka_client_consumer::group_metadata::ConsumerGroupMetadata { group_id, generation_id: i32, member_id, group_instance_id: Option<String> }`.

- [ ] **Step 2: `BrokerTransactionalProducer`** in `io_broker.rs`:
```rust
use crate::runtime::eos::{StreamsGroupMeta, TransactionalProducer};

/// A transactional [`RecordProducer`]/[`TransactionalProducer`] backed by a real
/// Kafka txn producer (EOS v2). Like `BrokerProducer` but the durability barrier
/// is `commit_transaction`, not `flush`.
pub(crate) struct BrokerTransactionalProducer {
    inner: Producer, // built with transactional_id
    pending: Mutex<Vec<oneshot::Receiver<Result<RecordMetadata, ProducerError>>>>,
}

#[async_trait::async_trait]
impl RecordProducer for BrokerTransactionalProducer {
    async fn send(&self, topic: &str, partition: Option<i32>, key: Option<Bytes>, value: Option<Bytes>)
        -> Result<(), StreamsClientError> {
        // Mirror BrokerProducer::send: enqueue ProducerRecord, stash the ack receiver.
        let rx = self.inner.send(ProducerRecord { /* topic, partition, key, value … as BrokerProducer */ }).await
            .map_err(|e| StreamsClientError::Runtime(e.to_string()))?;
        self.pending.lock().await.push(rx);
        Ok(())
    }
    async fn flush(&self) -> Result<(), StreamsClientError> {
        // Drain pending acks (BrokerProducer::flush body). Under EOS the txn commit
        // is the real barrier, but draining keeps the pending vec bounded.
        // … copy BrokerProducer::flush …
        Ok(())
    }
}

#[async_trait::async_trait]
impl TransactionalProducer for BrokerTransactionalProducer {
    async fn init_transactions(&self) -> Result<(), StreamsClientError> {
        self.inner.init_transactions().await.map_err(|e| StreamsClientError::Runtime(e.to_string()))
    }
    async fn begin_transaction(&self) -> Result<(), StreamsClientError> {
        self.inner.begin_transaction().await.map_err(|e| StreamsClientError::Runtime(e.to_string()))
    }
    async fn send_offsets_to_transaction(&self, offsets: &[(String, i32, i64)], m: &StreamsGroupMeta)
        -> Result<(), StreamsClientError> {
        let meta = crabka_client_consumer::group_metadata::ConsumerGroupMetadata {
            group_id: m.group_id.clone(), generation_id: m.generation_id,
            member_id: m.member_id.clone(), group_instance_id: m.group_instance_id.clone(),
        };
        let off = offsets.iter().map(|(t, p, o)| ((t.clone(), *p), *o));
        self.inner.send_offsets_to_transaction(off, &meta).await
            .map_err(|e| StreamsClientError::Runtime(e.to_string()))
    }
    async fn commit_transaction(&self) -> Result<(), StreamsClientError> {
        self.inner.commit_transaction().await.map_err(|e| StreamsClientError::Runtime(e.to_string()))
    }
    async fn abort_transaction(&self) -> Result<(), StreamsClientError> {
        self.inner.abort_transaction().await.map_err(|e| StreamsClientError::Runtime(e.to_string()))
    }
}
```
ADJUST the `send`/`flush` bodies to MATCH `BrokerProducer`'s exact `ProducerRecord` construction + ack-draining (copy them verbatim — same `inner: Producer`). Add the `crabka-client-consumer` dependency to `crates/client-streams/Cargo.toml` if not present (check `Cargo.toml` first).

- [ ] **Step 3: `build_eos`** next to `build` in `io_broker.rs`:
```rust
/// Build broker I/O for EOS: a transactional producer (with `transactional_id`),
/// a fetcher, and an offset store (for reads / committed-offset lookups).
pub(crate) async fn build_eos(
    bootstrap: &str, group_id: &str, client_id: &str, transactional_id: &str,
) -> Result<(BrokerFetcher, Arc<BrokerTransactionalProducer>, Arc<BrokerOffsetStore>), StreamsClientError> {
    // … same as `build`, but Producer::builder()…transactional_id(Some(transactional_id.into()))…build()
    //   and wrap in BrokerTransactionalProducer. Return it as the producer.
}
```
Copy `build`'s fetcher + offset-store construction; only the producer differs (transactional id + `BrokerTransactionalProducer`).

- [ ] **Step 4: verify + commit.** `cargo build -p crabka-client-streams`; `cargo clippy -p crabka-client-streams --all-targets -- -D warnings` (the new types are unused until T3 → add `#[allow(dead_code)]` with `// consumed by the EOS commit path in T3` on `BrokerTransactionalProducer`/`build_eos`); `cargo test -p crabka-client-streams`; fmt. Commit `feat(streams): BrokerTransactionalProducer + build_eos over the native txn producer`.

---

## Task 3: thread-level EOS commit lifecycle (happy path)

**Files:**
- Modify: `crates/client-streams/src/runtime/task.rs`, `crates/client-streams/src/runtime/thread.rs`, `crates/client-streams/src/membership/client.rs`, `crates/client-streams/src/runtime/app.rs`

- [ ] **Step 1: task offset accessors** (`task.rs`). The thread (not the task) drives the EOS commit, so expose the task's advanced offsets without committing:
```rust
/// The source offsets advanced since the last commit (for the thread's txn).
pub fn pending_offsets(&self) -> Vec<(String, i32, i64)> {
    self.pending.iter().map(|((t, p), o)| (t.clone(), *p, *o)).collect()
}
/// Clear pending after the thread's txn commit succeeds.
pub fn clear_pending(&mut self) { self.pending.clear(); }
```

- [ ] **Step 2: membership group metadata** (`membership/client.rs`). Read how `member_epoch` is stored (`Arc<Mutex<...>>`) + `member_id()`. Add:
```rust
/// The streams group metadata for EOS `send_offsets_to_transaction`.
pub fn group_metadata(&self) -> crate::runtime::eos::StreamsGroupMeta {
    crate::runtime::eos::StreamsGroupMeta {
        group_id: self.group_id.clone(),       // confirm field name; else thread application_id
        generation_id: i32::try_from(*self.member_epoch.lock().unwrap()).unwrap_or(0),
        member_id: self.member_id.clone(),
        group_instance_id: None,
    }
}
```
ADJUST to the real field names/locks (`group_id` may be named differently; `member_epoch` lock type). Read the struct first.

- [ ] **Step 3: thread EOS lifecycle** (`thread.rs`). Add to `StreamThread`: `guarantee: ProcessingGuarantee` (default `AtLeastOnce`), `txn: Option<Arc<dyn TransactionalProducer>>`, `in_txn: bool`, `initialized: bool`. `apply_assignment` gains the guarantee + (for EOS) the txn producer; on first EOS assignment call `txn.init_transactions()` once (`initialized`). `poll_all` (EOS): if `!self.in_txn { txn.begin_transaction().await?; self.in_txn = true; }` then process tasks. `commit_all` gains `meta: Option<&StreamsGroupMeta>`:
```rust
pub async fn commit_all(&mut self, meta: Option<&StreamsGroupMeta>) -> Result<(), StreamsClientError> {
    match self.guarantee {
        ProcessingGuarantee::AtLeastOnce => {
            for task in self.tasks.values_mut() { task.commit().await?; }
        }
        ProcessingGuarantee::ExactlyOnceV2 => {
            if !self.in_txn { return Ok(()); }       // nothing produced since last commit
            let txn = self.txn.as_ref().expect("EOS txn producer");
            let mut offsets = Vec::new();
            for task in self.tasks.values() { offsets.extend(task.pending_offsets()); }
            let meta = meta.expect("EOS commit requires group metadata");
            txn.send_offsets_to_transaction(&offsets, meta).await?;
            txn.commit_transaction().await?;
            for task in self.tasks.values_mut() { task.clear_pending(); }
            self.in_txn = false;
        }
    }
    Ok(())
}
```
Update `apply_assignment`'s signature to accept the guarantee + an `Option<Arc<dyn TransactionalProducer>>` (or a small `RuntimeIo` struct). Keep the producer passed to tasks as `Arc<dyn RecordProducer>` — for EOS pass the `BrokerTransactionalProducer` coerced to `Arc<dyn RecordProducer>` (it impls both), AND keep an `Arc<dyn TransactionalProducer>` clone on the thread for control calls.

- [ ] **Step 4: app wiring** (`app.rs`). When `processing_guarantee == ExactlyOnceV2`: build via `io_broker::build_eos(&bootstrap, &application_id, &application_id, &eos::transactional_id(&application_id, 0))`; keep both the `Arc<dyn RecordProducer>` (for tasks) and `Arc<dyn TransactionalProducer>` (for the thread). Pass the guarantee to `StreamThread`. In the `commit.tick()` branch, build `let meta = (guarantee == EOS).then(|| membership.group_metadata());` and call `thread.commit_all(meta.as_ref()).await`. (ALO: `thread.commit_all(None)`.)

- [ ] **Step 5: happy-path unit test** (`thread.rs` `#[cfg(test)]`): with the `MockTransactionalProducer` + a `MockFetcher` returning one batch for a stateless `source→proc→sink` topology, run an EOS `apply_assignment` + `poll_all` + `commit_all(Some(&meta))`; assert the mock's call sequence is `[Init, Begin, Send(sink…), SendOffsets, Commit]` and the sink record was sent. Reuse the existing `thread.rs` test harness; build the thread in EOS mode.

- [ ] **Step 6: verify + commit.** `cargo test -p crabka-client-streams --lib runtime`; full suite; clippy; fmt. Existing ALO runtime tests stay green (`commit_all(None)`). Commit `feat(streams): thread-level EOS commit lifecycle (begin/produce/send_offsets/commit)`.

---

## Task 4: abort + offset rewind + store rollback

**Files:**
- Modify: `crates/client-streams/src/store/api.rs` (+ store impls), `crates/client-streams/src/processor/graph.rs`, `crates/client-streams/src/runtime/task.rs`, `crates/client-streams/src/runtime/thread.rs`

- [ ] **Step 1: store `clear()`** — add to the `StateStore`/byte-store trait in `store/api.rs` an `async fn clear(&mut self)` that empties the store (in-memory: clear the map; Turso: `DELETE FROM`). Implement for each backend (InMemory + Turso byte stores). Add `Graph::clear_stores()` in `graph.rs`:
```rust
pub async fn clear_stores(&mut self) {
    for name in self.stores.names() {
        if let Some(s) = self.stores.get_mut(&name) { s.clear().await; }
    }
}
```

- [ ] **Step 2: `StreamTask::rollback`** (`task.rs`):
```rust
/// Roll back to the last committed state after a txn abort: rewind source
/// positions to committed offsets, wipe stores, and re-restore from the
/// committed changelog.
pub async fn rollback(&mut self, fetcher: &dyn RecordFetcher) -> Result<(), StreamsClientError> {
    self.pending.clear();
    self.seek_to_start().await?;          // positions ← committed (or earliest)
    self.graph.clear_stores().await;
    self.restore(fetcher).await?;         // replay committed changelog (read_committed in T5)
    Ok(())
}
```

- [ ] **Step 3: thread abort path** (`thread.rs`). Wrap the EOS `poll_all` + `commit_all` so any `Err` triggers abort + rollback. Add a helper:
```rust
async fn abort_and_rollback(&mut self, fetcher: &dyn RecordFetcher) -> Result<(), StreamsClientError> {
    if let Some(txn) = self.txn.as_ref() { let _ = txn.abort_transaction().await; }
    self.in_txn = false;
    for task in self.tasks.values_mut() { task.rollback(fetcher).await?; }
    Ok(())
}
```
In `app.rs`'s supervisor (or in `poll_all`/`commit_all`), on an EOS error call `thread.abort_and_rollback(&*fetcher).await`. Cleanest: make `poll_all`/`commit_all` (EOS) call `abort_and_rollback` internally on their own `Err` and return `Ok(())` (logged), so the supervisor's existing `if let Err(e) = ...` logging still works and the next interval re-begins. (Producer-fenced errors are fatal — match the error and propagate instead of rolling back; a `StreamsClientError::Runtime` containing the fenced marker → return Err. Keep it simple: treat all as retryable abort for this slice, note fenced-fatal as a follow-up if the native producer surfaces a distinct fenced error.)

- [ ] **Step 4: abort/rollback unit test** (`thread.rs`): `MockTransactionalProducer { fail_at: Some(Step::Commit) }` + a stateful `Counter` topology + a `MockFetcher` that returns the SAME batch again after rollback. Run `poll_all` + `commit_all` (commit fails → abort + rollback) → assert the mock recorded `Abort`, each task's `pending` is empty, and a subsequent successful cycle yields the correct (non-double-counted) store value. Assert `seek_to_start` rewound positions (the mock `OffsetStore.committed` returns the pre-txn offset).

- [ ] **Step 5: verify + commit.** `cargo test -p crabka-client-streams`; clippy; fmt. Commit `feat(streams): EOS abort + offset rewind + store rollback (re-restore)`.

---

## Task 5: read_committed changelog restore

**Files:**
- Modify: `crates/client-streams/src/runtime/io.rs`, `crates/client-streams/src/runtime/io_broker.rs`, `crates/client-streams/src/runtime/task.rs` (+ test fetchers)

- [ ] **Step 1: `IsolationLevel`** (`io.rs`):
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IsolationLevel { #[default] ReadUncommitted, ReadCommitted }
```
Change `RecordFetcher::fetch` to take `isolation: IsolationLevel`:
```rust
async fn fetch(&self, topic: &str, partition: i32, offset: i64, isolation: IsolationLevel)
    -> Result<FetchBatch, StreamsClientError>;
```

- [ ] **Step 2: update impls + call sites.** `BrokerFetcher::fetch` (io_broker.rs) passes the isolation level into the Kafka `Fetch` request (read how it builds the fetch; set `isolation_level` 0/1). `task.rs` call sites: `process_once` uses `ReadUncommitted`; `restore()` uses `ReadCommitted` when `self.guarantee == ExactlyOnceV2` else `ReadUncommitted` — thread a `guarantee: ProcessingGuarantee` field onto `StreamTask` (set in `StreamTask::new`, passed from the thread). Update ALL `MockFetcher`/test fetchers across `thread.rs`/`task.rs`/`app.rs` tests to the new signature (ignore the `isolation` param — `cargo build --tests` pins them).

- [ ] **Step 3: test** (`task.rs`): a `MockFetcher` that returns DIFFERENT batches for `ReadUncommitted` vs `ReadCommitted` on the changelog topic; assert an EOS task's `restore()` reads the `ReadCommitted` batch (excludes the "aborted" record), and an ALO task reads `ReadUncommitted`.

- [ ] **Step 4: verify + commit.** `cargo test -p crabka-client-streams`; clippy; fmt. Commit `feat(streams): read_committed changelog restore under EOS`.

---

## Task 6: broker integration test (single 127.0.0.1 broker)

**Files:**
- Create: `crates/client-streams/tests/eos_broker.rs`
- Modify: `.github/workflows/ci.yml` (add the new test to the client-streams-integration llvm-cov `--test` list)

- [ ] **Step 1: read** an existing broker integration test (e.g. `crates/client-streams/tests/*broker*.rs` from #2b/#3) for the in-process / 127.0.0.1 broker harness (how it starts a Crabka broker, creates topics, finalizes `streams.version`, and runs a `KafkaStreams` app). Reuse that harness.

- [ ] **Step 2: EOS e2e test** `eos_broker.rs`: stand up a single broker; create input/output topics (rf=1, 1 partition); produce a few input records; run a stateful word-count `KafkaStreams` app with `.processing_guarantee(ProcessingGuarantee::ExactlyOnceV2)`; then:
  - assert a **`read_committed`** consumer on the output topic sees exactly the committed results (no duplicates / no aborted data);
  - assert the committed source offsets (via an `OffsetFetch` / the broker) advanced to the end of the input (atomic with the output);
  - **restart** the app (new `KafkaStreams` instance, same application.id) and assert it resumes from the committed changelog with the correct store state (no double-count).
  Follow the multi-broker-produce-readiness gate from memory: wait on the TARGET broker's local state, not image convergence. Single broker (rf=1) avoids the inter-broker replication limitation.

- [ ] **Step 3: CI wiring.** Add `eos_broker` to the crate's `client-streams-integration` llvm-cov `--test` list in `ci.yml` (per the codecov-per-crate memory — a new `tests/<x>.rs` not added there reports 0% patch). Mark the test `#[ignore]` if it requires a broker binary the unit CI lacks, OR gate it behind the integration job (match how the existing broker integration tests are gated — read `ci.yml`).

- [ ] **Step 4: verify + commit.** Run the test locally if a broker is available (`cargo test -p crabka-client-streams --test eos_broker -- --ignored` or the integration harness); otherwise verify it compiles (`cargo test -p crabka-client-streams --test eos_broker --no-run`) and rely on CI. clippy; fmt. Commit `test(streams): EOS broker integration (atomic output+offsets, read_committed, restart-resume)`.

---

## Task 7: docs + final verification

**Files:**
- Modify: `crates/client-streams/src/lib.rs`

- [ ] **Step 1: docs.** Add a `## Exactly-once (EOS v2)` section to `lib.rs` crate docs: `processing.guarantee` config (`at_least_once` default vs `exactly_once_v2`); one transaction per thread; atomic output + offset commit via `send_offsets_to_transaction`; abort → rewind + store rollback; the `KafkaStreams::builder().processing_guarantee(ProcessingGuarantee::ExactlyOnceV2)` usage. Mirror the existing `## Running an app` prose; a `no_run` doctest showing the builder with the guarantee set.

- [ ] **Step 2: final verify.** `cargo test -p crabka-client-streams` + `--doc`; `cargo clippy -p crabka-client-streams --all-targets -- -D warnings`; `cargo fmt --check`; `cargo test -p crabka-client-streams --test dsl_golden_frame` (**17 goldens unchanged** — EOS adds no wire). Commit `test(streams): EOS docs + final verification`.

---

## Done criteria
- `KafkaStreams::builder().processing_guarantee(ExactlyOnceV2)` runs the runtime transactionally: per-thread `begin → produce sink+changelog → send_offsets_to_transaction → commit`; on failure `abort` + rewind offsets + roll back stores from the committed changelog (read_committed).
- ALO is the default and behaviorally unchanged.
- Broker integration test proves atomic output+offsets, `read_committed` visibility, and restart-resume without double-count; mock unit tests prove the commit ordering + abort/rollback.
- 17 topology goldens byte-identical (no wire footprint); full suite + doctests + clippy `--all-targets -D warnings` + fmt green.

## Notes for the implementer
- **One producer per thread** (EOS v2 / KIP-447): the producer is already a shared `Arc`; the thread owns the txn boundaries, tasks just `send`. Do NOT create a producer per task.
- **Offsets under EOS go through the producer** (`send_offsets_to_transaction` → TxnOffsetCommit), NOT `OffsetStore.commit`. `OffsetStore` is still used for `committed`/`earliest` lookups (seek + rewind).
- **Rollback = wipe + re-restore** from the committed changelog; reuse `restore()` after `clear_stores()` + `seek_to_start()`.
- **The txn spans poll ticks**: `begin` lazily on the first `poll_all` after a commit; `commit` at the `commit.tick()`. `in_txn` guards a no-op commit when nothing was produced.
- **CLAUDE.md greenfield:** `processing.guarantee` is a real Kafka config (not a compat shim); supporting both ALO and EOS is Kafka-faithful, not a default-off feature flag. The ALO path is the existing code, unchanged.
- **Don't touch the wire / goldens** — EOS is transaction-level, invisible to the KIP-1071 topology.
