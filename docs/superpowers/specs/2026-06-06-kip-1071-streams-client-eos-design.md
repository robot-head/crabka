# KIP-1071 streams client — exactly-once (EOS v2)

**Status:** design approved (brainstorm)
**Builds on:** #2b runtime (`StreamThread`/`StreamTask`, the `RecordFetcher`/`RecordProducer`/`OffsetStore` I/O traits + `io_broker` impls), #3 state stores + changelog restore, #1 membership. Branches from `main` (independent of the open punctuation PR #421; both touch `runtime/task.rs`+`thread.rs`, so rebase when one lands).
**Ground truth:** Apache Kafka 4.1 `processing.guarantee=exactly_once_v2` (KIP-447). The Crabka broker's transaction coordinator (`crates/broker/src/txn/`) and the native `crabka-client-producer` transactional API already exist; this slice wires the streams runtime onto them.

## 1. Goal

Make the streams runtime **exactly-once** under `processing.guarantee=exactly_once_v2`: each commit cycle produces sink + changelog records and commits source offsets inside a single Kafka transaction, so a crash mid-cycle neither double-emits output nor double-counts state. The existing at-least-once (ALO) path stays the default.

## 2. Scope

### In scope
1. **`processing.guarantee` config** on the `KafkaStreams` builder: `AtLeastOnce` (default, today's path) | `ExactlyOnceV2`.
2. **One transaction per `StreamThread`** (KIP-447): a single transactional producer shared across the thread's tasks; the thread drives the txn boundaries.
3. **`TransactionalProducer` I/O seam** (DI trait) + a `BrokerTransactionalProducer` impl over the native producer + a mock for unit tests.
4. **EOS commit lifecycle**: `begin` → produce sink + changelog in-txn → `send_offsets_to_transaction(source offsets, group meta)` → `commit`.
5. **Abort + rollback**: on any error in a cycle, `abort`, rewind each task's source positions to last committed, and roll back state stores by wiping + re-restoring from the **committed** changelog.
6. **read_committed changelog restore** so restore/rollback excludes aborted changelog writes.
7. **Broker integration test** (single 127.0.0.1 broker) proving atomic output+offsets and `read_committed` visibility, + mock-producer abort unit tests.

### Non-goals (deferred)
- **EOS v1 / `exactly_once_beta`** — removed in Kafka 4.x; v2 only.
- **Multi-instance `processId` fencing** — a stable per-thread `transactional.id` (`<application.id>-<threadIdx>`) only; a persisted process UUID for cross-instance zombie fencing is a follow-up.
- **Standby / warmup task EOS**, **producer-per-task (v1 model)**, **`TopologyTestDriver` transactions** (TTD stays ALO — it is broker-free).
- Wire/topology changes — none; EOS is producer/consumer-transaction-level, orthogonal to the KIP-1071 rebalance protocol. No goldens.

## 3. Architecture

```
processing.guarantee = exactly_once_v2
        │
        ▼
KafkaStreams app  → io_broker::build_eos(bootstrap, application_id, transactional_id)
        │                 → BrokerTransactionalProducer (native txn producer, ONE per thread)
        ▼
StreamThread (owns the txn)                       one transaction per commit interval:
  begin_transaction()
    for task in tasks: task.process_once()  ──►  producer.send(sink, key-hash)
                                                 producer.send(changelog, pinned partition)
  producer.send_offsets_to_transaction(            ── all tasks' advanced source offsets
        all source offsets, group_meta)            ── group_meta from StreamsMembership
  commit_transaction()
        │  on ANY error in the cycle:
        ▼
  abort_transaction(); for task: rewind positions to committed + rollback stores (re-restore)
```

The producer is already a single `Arc` shared across a thread's tasks, so EOS v2's one-producer-per-thread maps directly: tasks `send` into the shared txn producer; the **thread** owns `begin`/`send_offsets`/`commit`/`abort`. In ALO mode the per-task `commit` (`flush` + `OffsetStore.commit`) is unchanged.

### 3.1 Config + transactional.id + group metadata

- `ProcessingGuarantee { AtLeastOnce, ExactlyOnceV2 }`; `KafkaStreams::builder().processing_guarantee(..)`, default `AtLeastOnce`.
- `transactional.id = "<application.id>-<threadIdx>"` (single-thread client → `<application.id>-0`). Stable across restarts → `init_transactions()` bumps the producer epoch and fences a zombie instance with the same id.
- `send_offsets_to_transaction` needs a `ConsumerGroupMetadata { group_id: application.id, generation: member_epoch, member_id, group_instance_id: None }`, built from the `StreamsMembership` (it exposes `member_id()` + `member_epoch`). KIP-447 uses this group metadata (not per-partition txn ids) for fencing, enabling one producer per thread.

### 3.2 The `TransactionalProducer` seam

```rust
#[async_trait]
pub trait TransactionalProducer: Send + Sync + 'static {
    async fn init_transactions(&self) -> Result<(), StreamsClientError>;
    async fn begin_transaction(&self) -> Result<(), StreamsClientError>;
    async fn send(&self, topic: &str, partition: Option<i32>, key: Option<Bytes>, value: Option<Bytes>)
        -> Result<(), StreamsClientError>;
    async fn send_offsets_to_transaction(
        &self, offsets: &[(String, i32, i64)], group_meta: &StreamsGroupMeta,
    ) -> Result<(), StreamsClientError>;
    async fn commit_transaction(&self) -> Result<(), StreamsClientError>;
    async fn abort_transaction(&self) -> Result<(), StreamsClientError>;
}
```
- `BrokerTransactionalProducer` wraps `crabka_client_producer::Producer` built with `transactional_id(Some(..))`, delegating to its `init_transactions`/`begin_transaction`/`send`/`send_offsets_to_transaction(offsets, &ConsumerGroupMetadata)`/`commit_transaction`/`abort_transaction`.
- A `MockTransactionalProducer` (records the call sequence + a configurable failure point) drives the abort unit tests.
- The task's `producer: Arc<dyn RecordProducer>` is unchanged for `send` (EOS producer also impls `RecordProducer`'s `send`); the thread additionally holds the `Arc<dyn TransactionalProducer>` for the txn-control calls. (`BrokerTransactionalProducer` impls both traits over the one native producer; `flush` in EOS mode is a no-op — `commit_transaction` is the durability barrier.)

### 3.3 Commit lifecycle (EOS, per thread per interval)

The thread runs a `CommitStrategy`:
- **ALO** (default): each task `process_once` (send sink+changelog), then per-task `commit` (`flush` + `OffsetStore.commit`) — exactly today's behavior.
- **EOS**:
  1. Once after assignment: `init_transactions()`.
  2. Per interval: `begin_transaction()`.
  3. `for task in tasks: task.process_once(fetcher)` — sink + changelog produced via the shared txn producer; each task accumulates its advanced source offsets in `pending`.
  4. `send_offsets_to_transaction(union of all tasks' pending offsets, group_meta)` — replaces `OffsetStore.commit`; the source offsets land in `__consumer_offsets` atomically with the txn.
  5. `commit_transaction()`. Clear each task's `pending`.
  6. On ANY `Err` in steps 2–5: `abort_transaction()` then **rollback** (§3.4), then continue (next interval re-begins). A producer-fenced error (zombie) surfaces as a fatal `StreamsClientError` → the app rejoins/-shuts down.

### 3.4 Abort + rollback

On abort the txn's produced records (sink + changelog) and offsets are discarded by the broker. The in-memory state stores still hold the **dirty** writes from the aborted cycle, so each stateful task must roll back:
1. **Rewind source offsets**: set each task's `positions` (and clear `pending`) back to the last committed offset (`OffsetStore.committed`, or the txn's last `send_offsets` baseline) so the next cycle re-reads the aborted input.
2. **Roll back stores**: wipe each task's stores and **re-restore from the committed changelog** via the existing `StreamTask::restore` path (read_committed, §3.5). This rebuilds store state to exactly the last committed point. (Reuses `restore()`; the only addition is a `rollback()` that clears stores first.)

### 3.5 read_committed changelog restore

For restore/rollback to reflect only committed state, the changelog **fetch** must use `read_committed` (skip aborted records, stop at the last stable offset). The `RecordFetcher` gains an isolation level (or a `fetch_committed` variant) used by `restore()` under EOS. Under ALO, restore stays read_uncommitted (unchanged).

## 4. Components & boundaries

| Unit | Responsibility | Depends on |
|---|---|---|
| `runtime/eos.rs` (new) | `ProcessingGuarantee`, `TransactionalProducer` trait, `StreamsGroupMeta`, transactional.id derivation | io traits |
| `runtime/io_broker.rs` | `BrokerTransactionalProducer` (+ `build_eos`) | native producer |
| `runtime/task.rs` | accumulate `pending` offsets (unchanged send path); `rollback()` (wipe + restore) | graph, restore |
| `runtime/thread.rs` | `CommitStrategy` (ALO vs EOS); EOS txn lifecycle across tasks; group_meta | task, eos |
| `runtime/app.rs` | `processing_guarantee` builder field; wire EOS producer + group_meta | thread, membership |
| `runtime/io.rs` | `RecordFetcher` isolation level for read_committed restore | — |
| `tests/` | broker integration (EOS app, atomic output+offsets, read_committed) + mock abort unit | all |

## 5. Testing

- **Unit (mock txn producer):** EOS commit calls `begin → send(sink) → send(changelog) → send_offsets → commit` in order; a forced failure at each step triggers `abort` + a task `rollback` (assert stores re-restored, positions rewound, `pending` cleared). ALO path unchanged.
- **Broker integration (single 127.0.0.1 broker):** run a stateful EOS app (e.g. word-count) end-to-end; assert (a) a `read_committed` consumer sees only committed output, (b) committed source offsets match the produced output (atomicity), (c) restart re-restores state from the committed changelog and resumes without double-counting. Mirrors the #2b/#3 broker integration tests. Gated on the broker's txn coordinator (already present).
- **Restart-rollback:** simulate an abort (mock fetcher returning a batch, mock producer failing commit once) → assert the next cycle reprocesses the same input and the store value is correct (no double-count).

## 6. Error handling

- **Producer fenced** (zombie, epoch bumped by a newer instance): fatal `StreamsClientError` → the app stops the thread / rejoins; do NOT retry the txn.
- **Transient commit error**: `abort` + rollback + retry next interval (the input is re-read).
- **`init_transactions` failure** at startup: surfaced as a fatal error (the app can't run EOS without it).
- **Mixed guarantee misconfig**: `processing.guarantee=exactly_once_v2` with no `transactional.id` derivable is impossible (it's derived from `application.id`), so no misconfig path.

## 7. Slice decomposition (phased, one PR)

- **T1** — `ProcessingGuarantee` config + `processing_guarantee` builder field + transactional.id derivation + `TransactionalProducer` trait + `StreamsGroupMeta` + `MockTransactionalProducer` (unit-tested).
- **T2** — `BrokerTransactionalProducer` over the native txn producer + `io_broker::build_eos`.
- **T3** — thread-level EOS commit lifecycle (happy path: begin → process → send_offsets → commit; `CommitStrategy` ALO/EOS split) + group_meta threading; mock-producer happy-path unit test.
- **T4** — abort + offset rewind + `StreamTask::rollback` (wipe + re-restore); mock-producer abort/rollback unit tests.
- **T5** — `RecordFetcher` read_committed isolation for EOS restore.
- **T6** — broker integration test (single 127.0.0.1 broker): atomic output+offsets, read_committed visibility, restart-resume.
- **T7** — docs (`lib.rs` EOS prose + `processing.guarantee` example) + final verification.

## 8. Open questions resolved

- **Producer-per-thread vs per-task:** per-thread (EOS v2 / KIP-447), matching the already-shared producer Arc.
- **Offset commit channel under EOS:** `send_offsets_to_transaction` (TxnOffsetCommit to the txn coordinator), NOT `OffsetStore.commit`.
- **Store rollback mechanism:** wipe + re-restore from the committed changelog (reuse `restore()`), under read_committed.
