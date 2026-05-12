# Slice 9: Transactions — design

## Summary

Kafka transactions for Crabka — KIP-98 plus the full KIP-1319 v2
("Transactions Server-Side Defense") protocol. After this slice a
JVM `kafka-console-producer --transactional-id <tid>` interleaves
committed + aborted record batches against Crabka, and
`kafka-console-consumer --isolation-level read_committed` reads only
the committed records. Consume-process-produce loops via
`TxnOffsetCommit` work end-to-end.

Slice 8's deferrals — high-watermark tracking, `acks=all` blocking,
controller-driven leader-election on broker failure, KIP-101 leader-
epoch — remain deferred. **EOS is therefore "soft" in slice 9**: the
transactional state machine and control markers are correct, but a
partition-leader crash mid-transaction can lose records the producer
believed were durably committed. Bulletproof EOS lands when those
slice-8 follow-ups ship.

## Non-goals

- **High-watermark / `acks=all` blocking durability.** Slice 8
  follow-up.
- **Controller-driven leader election on partition failure.** Slice 8
  follow-up.
- **KIP-101 leader-epoch + KIP-279 truncation safety.** Slice 8
  follow-up.
- **`ListTransactions` / `DescribeTransactions` admin RPCs.** Slice 10
  (Admin API + tooling).
- **Transaction-aware log compaction** (dropping records past their
  abort marker during compaction). Slice 12-ish (compaction is its
  own follow-up).
- **Cross-broker producer leader routing in the Rust producer.** Same
  slice-6 follow-up that slice 8 already documents as deferred.
- **Cross-cluster transactional mirror-maker.** Separate product
  surface; out of meta-spec.
- **Static-membership / KIP-345 interaction with `TxnOffsetCommit`.**
  Slice 13 follow-up (share groups).

## Crate layout

| Crate | Status | Responsibility |
|---|---|---|
| `crabka-broker` | modified + new modules | Transaction coordinator, `__transaction_state` bootstrap, 6 new wire handlers + extensions, control markers, LSO tracking, per-segment aborted-txn index, Fetch `isolation_level=read_committed` filtering. |
| `crabka-client-producer` | modified | bon-builder gains `transactional_id` + `transaction_timeout`. `Producer` gains `init_transactions` / `begin_transaction` / `commit_transaction` / `abort_transaction` / `send_offsets_to_transaction`. Tags transactional records and drives the v2 protocol. |
| `crabka-client-consumer` | modified, minor | bon-builder gains `isolation_level: IsolationLevel`. Threads it into Fetch requests. Default is `ReadUncommitted` (preserves slice-5 behavior). |
| `crabka-log` | modified, minor | Per-segment `.txnindex` reader + writer. `Log::append` reads `is_transactional`/`is_control` attribute bits on incoming batches and updates the index + LSO. |
| `crabka-metadata` | unchanged | Coordinator state lives in `__transaction_state` (a regular replicated topic), NOT in the openraft metadata image — same pattern as slice-5's `__consumer_offsets`. |

## Architecture

```
   producer client                                broker (partition leader)
   ─────────────────                              ─────────────────────────
   init_transactions(tid)
        │
        ▼  FindCoordinator(tid, TXN)
       ┌──────────────────────────────────────────┐
       │  hash(tid) % 50 → __transaction_state-p │
       │  return broker hosting that partition's  │
       │  leader                                  │
       └──────────────────────────────────────────┘
        │
        ▼  InitProducerId(tid, txn_timeout)
   ┌──────────────────────────────────────────────────────────────────┐
   │  TxnCoordinator @ broker N                                       │
   │   · allocate or look up TxnEntry by tid                          │
   │   · bump epoch (KIP-1319 v2 bumps on every Init, not just fence) │
   │   · if prior entry was Ongoing → write PrepareAbort + dispatch    │
   │     abort markers, then respond                                  │
   │   · persist TxnEntry to __transaction_state                      │
   │   · return (producer_id, producer_epoch)                         │
   └──────────────────────────────────────────────────────────────────┘
        │
        ▼  begin_transaction() — client-side state bump

        ▼  send(record) — tags batch is_transactional=true, (pid, epoch)
   ┌──────────────────────────────────────────────────────────────────┐
   │  partition leader's Produce handler                              │
   │   · verify (pid, epoch) against the local TxnCoordinator cache   │
   │     (or via inter-broker fetch on cache miss)                     │
   │   · KIP-1319 v2: on first transactional Produce per partition,    │
   │     auto-AddPartitionsToTxn to the txn coordinator                │
   │   · append RecordBatch with is_transactional=true                │
   └──────────────────────────────────────────────────────────────────┘
        │
        ▼  commit_transaction()  →  EndTxn(commit=true)
   ┌──────────────────────────────────────────────────────────────────┐
   │  TxnCoordinator                                                  │
   │   · transition Ongoing → PrepareCommit                            │
   │   · persist to __transaction_state                                │
   │   · for each partition in entry.partitions:                       │
   │       send WriteTxnMarkers(commit) inter-broker                  │
   │   · for each group in entry.offset_commit_groups:                 │
   │       send WriteTxnMarkers(commit) to __consumer_offsets leader  │
   │   · await all marker acks                                         │
   │   · transition PrepareCommit → CompleteCommit; persist            │
   │   · respond Ok to producer                                        │
   └──────────────────────────────────────────────────────────────────┘
        │
        ▼  partition leader writes a control RecordBatch
        │  (is_control=true, type=COMMIT, version=0) at next offset.
        │  Advances Partition::lso() past the marker.

   consumer client
   ─────────────────
   Fetch(isolation_level=read_committed)
        │
        ▼
   ┌──────────────────────────────────────────────────────────────────┐
   │  partition leader's Fetch handler                                │
   │   · clamp response window to [start, lso())                       │
   │   · consult per-segment .txnindex to skip aborted batches         │
   │   · response carries last_stable_offset = lso()                   │
   └──────────────────────────────────────────────────────────────────┘
```

## Components

### Wire api keys (KIP-1319 v2 versions)

| api_key | Handler | Status | Notes |
|---|---|---|---|
| 10 | `FindCoordinator` v6+ | modified | Existing slice-5 handler; extend with `key_type=TRANSACTION (1)`. Slice 5 only handled `key_type=GROUP (0)`. |
| 22 | `InitProducerId` v5+ | modified | Slice-6 stub currently rejects transactional ids with `TRANSACTIONAL_ID_AUTHORIZATION_FAILED (67)`. Replace with real routing to `TxnCoordinator`. |
| 24 | `AddPartitionsToTxn` v5+ | **new** | Producer registers partitions about to be written to. Under v2 the broker can auto-call this on first transactional Produce. |
| 25 | `AddOffsetCommitsToTxn` v4+ | **new** | Producer registers a consumer group whose offsets it intends to commit transactionally. |
| 26 | `EndTxn` v5+ | **new** | Commit or abort the in-flight transaction. |
| 27 | `WriteTxnMarkers` v1+ | **new** | Inter-broker RPC: the coordinator pushes control markers to every involved partition leader. |
| 28 | `TxnOffsetCommit` v4+ | **new** | Like `OffsetCommit` but within a transaction; goes through the group coordinator + ties into the txn coordinator via `AddOffsetCommitsToTxn`. |

### `txn::coordinator::TxnCoordinator`

```rust
pub(crate) struct TxnCoordinator {
    node_id: NodeId,
    log_dir: PathBuf,
    partitions: Arc<DashMap<(String, i32), Arc<Partition>>>,
    controller: Arc<ControllerHandle>,
    // Per-tid in-memory state, keyed by transactional_id.
    state: DashMap<String, Arc<Mutex<TxnEntry>>>,
    // __transaction_state partitions this broker hosts as leader.
    leader_partitions: Arc<RwLock<HashSet<i32>>>,
}
```

The coordinator is per-broker; it serves `(transactional_id)`s whose
hash maps to a `__transaction_state` partition this broker hosts as
leader. Requests for other tids return `NOT_COORDINATOR (16)` with the
correct broker id, so clients re-issue `FindCoordinator`.

### `txn::state::TxnEntry` + `TxnState`

```rust
#[derive(Debug, Clone)]
pub struct TxnEntry {
    pub transactional_id: String,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub state: TxnState,
    pub txn_timeout_ms: i32,
    pub partitions: HashSet<TopicPartition>,
    pub offset_commit_groups: HashSet<String>,
    pub last_update_ms: i64,
    pub start_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnState {
    Empty,
    Ongoing,
    PrepareCommit,
    PrepareAbort,
    CompleteCommit,
    CompleteAbort,
    Dead,
}
```

Encoded into `__transaction_state` records via a versioned binary
format (`bincode` via `serde-wincode`'s `SerdeCompat`, matching the
slice-7 metadata-record approach). Each tid update is one record:
key = `tid` bytes, value = encoded `TxnEntry`.

### `__transaction_state` topic

- 50 partitions (Apache Kafka default).
- `replication_factor = min(3, broker_count)` — same policy slice-5
  applied to `__consumer_offsets`.
- Created lazily on first `FindCoordinator(key_type=TRANSACTION)`
  request (`txn::bootstrap` module; mirrors slice-5).
- Partition key for a given tid: `murmur2(tid) % 50` — Apache Kafka's
  `Utils.abs(murmur2(...)) % numPartitions` convention. Matches the
  JVM client.
- State recovery: on `Broker::start`, replay every locally-led
  partition's log to rebuild the in-memory `state` map.

### Control markers (`txn::marker`)

A control RecordBatch carries `is_control=true` and
`is_transactional=true` in its attributes. The single inner record
has:

- Key bytes: `(version: i16 = 0, type: i16)` — type=0 for ABORT,
  type=1 for COMMIT.
- Value bytes: empty.

The batch's `producer_id`/`producer_epoch` reflect the producer
whose transaction this marker terminates.

### `Partition::lso()` + per-segment `.txnindex`

- `Partition::lso()` returns the offset of the first record belonging
  to an unfinished transaction. Initially equals `log_start_offset`.
  Advances when a marker is appended.
- Per-segment `.txnindex` file: flat array of fixed-width records
  `(start_offset: i64, last_offset: i64, producer_id: i64)` per
  aborted transaction. Byte-compatible with Apache Kafka's format so
  `kafka-dump-log --offsets-decoder` can dump it.
- `Log::append` (already exists from slice 4) extended: if the batch
  is `is_control=true`, parse the inner record and either advance
  LSO (commit) or write a `.txnindex` entry (abort) + advance LSO.

### Fetch with `isolation_level=read_committed`

`crates/broker/src/handlers/fetch.rs` gains one branch on
`req.isolation_level`:

- `0` (`read_uncommitted`, default): existing slice-8 path; serve
  batches up to `log_end_offset`.
- `1` (`read_committed`): clamp the response window to `[fetch_offset,
  lso())`. Consult the per-segment `.txnindex` to mark aborted batches
  for filtering (the response can include batches the client must
  skip; the wire field `aborted_transactions` carries the
  `.txnindex` entries within the response window).

Response carries `last_stable_offset` so clients can detect lag.

### Producer client API

```rust
let producer = Producer::builder()
    .bootstrap("localhost:9092")
    .transactional_id("my-tid")             // NEW
    .transaction_timeout(Duration::from_secs(60))  // NEW
    .build()
    .await?;

producer.init_transactions().await?;
producer.begin_transaction()?;

producer.send(ProducerRecord {
    topic: "a".into(),
    value: Some("hello".into()),
    ..Default::default()
}).await;

producer.send_offsets_to_transaction(
    [(("topic-in".to_string(), 0), 42i64)],
    "my-consumer-group",
).await?;

producer.commit_transaction().await?;
// OR:
producer.abort_transaction().await?;
```

Producer state machine:

```
Uninitialized
   │  init_transactions()
   ▼
InitTransactionsCalled
   │  (synchronously)
   ▼
ReadyForTxn
   │  begin_transaction()
   ▼
InTransaction
   │  commit_transaction() / abort_transaction()
   ▼
CommittingOrAborting
   │  (broker EndTxn returns Ok)
   ▼
ReadyForTxn ─→ (back to begin_transaction)
```

Out-of-order calls return `ProducerError::InvalidTransactionState(reason)`.

**Non-transactional producers (slice-6 callers) are unaffected.** A
producer built without `transactional_id` rejects all transactional
methods with `ProducerError::NotTransactional`. The slice-6
idempotent-producer tests pass byte-for-byte.

### Consumer client extension

```rust
let consumer = Consumer::builder()
    .bootstrap("localhost:9092")
    .group_id("g1")
    .subscribe(["my-topic"])
    .isolation_level(IsolationLevel::ReadCommitted)  // NEW; default ReadUncommitted
    .build().await?;
```

One new builder field; threaded into the `Fetch` request body. No
client-side filtering — the broker does it.

## Data flow

### Happy path: commit a transaction with 2 partitions

```
producer.init_transactions()  →  FindCoordinator(tid, TXN) → broker N
                              →  InitProducerId(tid, txn_timeout) to broker N
                                  · TxnCoordinator allocates pid=1000, epoch=0
                                  · writes TxnEntry { state=Empty } to __transaction_state-p
                                  · returns (1000, 0)

producer.begin_transaction()  →  client-side: state = InTransaction

producer.send(record1)        →  Produce(is_transactional=true) to broker hosting partition A
                                  · KIP-1319 v2 auto-AddPartitionsToTxn to broker N
                                  · broker N: state Empty → Ongoing, persists, returns Ok
                                  · partition leader appends RecordBatch

producer.send(record2)        →  Produce to broker hosting partition B
                                  · auto-AddPartitionsToTxn again
                                  · broker N: Ongoing, adds B to entry.partitions
                                  · partition leader appends

producer.commit_transaction() →  EndTxn(tid, pid=1000, epoch=0, committed=true)
                                  · broker N transitions Ongoing → PrepareCommit
                                  · sends WriteTxnMarkers(commit) to A's leader
                                  · sends WriteTxnMarkers(commit) to B's leader
                                  · awaits both
                                  · transitions PrepareCommit → CompleteCommit
                                  · responds Ok
```

### Fenced-producer path

```
producer-A.init_transactions()  →  pid=1000, epoch=0
producer-A.begin_transaction()
producer-A.send(...)             →  partition appends as (pid=1000, epoch=0)

producer-B.init_transactions()  →  TxnCoordinator finds existing entry, bumps epoch
                                  · prior entry was Ongoing → writes PrepareAbort marker
                                  · dispatches abort markers to producer-A's partitions
                                  · transitions to CompleteAbort
                                  · allocates pid=1000, epoch=1; new TxnEntry Empty
                                  · returns (1000, 1) to producer-B

producer-A.commit_transaction() →  EndTxn(pid=1000, epoch=0)
                                  · TxnCoordinator: stored epoch is 1, requested 0
                                  · returns INVALID_PRODUCER_EPOCH (53)
producer-A surfaces ProducerError::ProducerFenced
```

### Consume-process-produce loop with TxnOffsetCommit

```
consumer.poll() → records from input-topic
... process ...
producer.begin_transaction()
producer.send(transformed_record)                        →  to output-topic
producer.send_offsets_to_transaction(input_offsets, g)   →  AddOffsetCommitsToTxn(tid, pid, epoch, g)
                                                              · TxnCoordinator: state Ongoing, adds g
                                                          →  TxnOffsetCommit(tid, pid, epoch, g, offsets)
                                                              · group coordinator writes offset records
                                                                tagged with (pid, epoch) to __consumer_offsets
producer.commit_transaction()                            →  EndTxn(commit=true)
                                                              · WriteTxnMarkers(commit) to output-topic partitions
                                                              · WriteTxnMarkers(commit) to __consumer_offsets partition
```

## Errors

### New wire codes

```rust
pub const INVALID_TXN_STATE: i16 = 24;
pub const INVALID_TXN_TIMEOUT: i16 = 48;
pub const CONCURRENT_TRANSACTIONS: i16 = 49;
pub const TRANSACTION_COORDINATOR_FENCED: i16 = 50;
pub const STALE_MEMBER_EPOCH: i16 = 82;
```

(Codes `47 INVALID_PRODUCER_ID_MAPPING`, `53 INVALID_PRODUCER_EPOCH`,
`67 TRANSACTIONAL_ID_AUTHORIZATION_FAILED` already exist from slice
6/7.)

### New `ProducerError` variants

```rust
NotTransactional,                       // method called on non-transactional producer
InvalidTransactionState(&'static str),  // out-of-order method call
TransactionAborted,                     // committing a txn the broker aborted
ProducerFenced,                         // already exists from slice 6; also post-init
ConcurrentTransactions,                 // begin_transaction while another in-flight
```

### `BrokerError::Txn(String)`

Diagnostic logging only — never reaches the wire. Clients see the
standard wire codes above.

### Resolution policy

- `NOT_COORDINATOR (16)` on any txn RPC → producer re-issues
  `FindCoordinator` and retries.
- `INVALID_PRODUCER_EPOCH (53)` → producer surfaces
  `ProducerError::ProducerFenced` and stops. Caller must
  `init_transactions` again (with the same tid) to recover.
- `CONCURRENT_TRANSACTIONS (49)` → producer retries with 100 ms
  backoff up to 5 attempts (rare; happens during epoch handoff).
- `INVALID_TXN_STATE (24)` → caller bug; surface as
  `InvalidTransactionState`. No retry.
- `STALE_MEMBER_EPOCH (82)` on `TxnOffsetCommit` → producer re-fetches
  the consumer-group's member epoch and retries.

## Observability

- Tracing spans on every txn RPC: `txn.init_producer_id`,
  `txn.add_partitions`, `txn.add_offset_commits`, `txn.end_txn`,
  `txn.write_markers`.
- Structured events:
  - `txn.state_transition` — old, new, tid, pid, epoch.
  - `txn.marker_written` — partition, type (commit/abort), offset.
  - `txn.lso_advanced` — partition, old_lso, new_lso.
  - `txn.fenced` — at WARN.
- Per-partition `txn.in_flight` metric: count of unfinished
  transactions with records past LSO.

## Testing

### Layer 1 — unit tests

- `txn::state::tests::state_machine_transitions` — every valid
  transition + every rejected invalid transition.
- `txn::coordinator::tests::murmur2_partition_assignment` — verify
  tid → partition matches a known JVM-computed table (`hash("my-tid")
  % 50 == 12`, etc.).
- `txn::marker::tests::commit_marker_byte_compat` — encoded marker
  bytes match an Apache Kafka–generated fixture (captured via
  `kafka-dump-log` on a JVM-produced control batch).
- `txn::marker::tests::abort_marker_byte_compat` — same for abort.
- Producer `tests::out_of_order_methods_return_invalid_state` — call
  `commit_transaction` without `begin_transaction` first → error.
- Producer `tests::epoch_bump_per_init_kip1319` — two
  `init_transactions` calls bump producer epoch.

### Layer 2 — in-process integration (`crates/broker/tests/transactions.rs`)

Single-broker (single-voter quorum), gated
`#[cfg(not(target_os = "windows"))]` per the slice-7/slice-8 cadence.

- `commit_then_read_committed_sees_records`: begin → send 3 →
  commit → `read_committed` Fetch sees all 3.
- `abort_then_read_committed_skips_records`: same → abort →
  `read_committed` Fetch sees nothing; `read_uncommitted` sees all 3.
- `interleaved_commit_and_abort`: produce + commit (3), produce +
  abort (2), produce + commit (4) → `read_committed` sees 7;
  `read_uncommitted` sees 9.
- `fenced_producer_cannot_commit`: producer A inits + begins.
  Producer B re-inits same tid. Producer A commits → `ProducerFenced`.
- `send_offsets_to_transaction_atomic_with_records`: consume-
  process-produce; commit succeeds → offset commits visible. Same
  again with abort → offsets unchanged.

### Layer 3 — JVM acceptance (`crates/broker/tests/jvm_acceptance.rs::transactional_console_producer_eos`)

`#[ignore = "requires Docker"]`. 3-broker Crabka cluster on ports
9792/9892/9992 + 9793/9893/9993 (offset 600 from slice-7 + 300 from
slice-8 to dodge TIME_WAIT).

1. `kafka-topics --create --topic <T> --partitions 1
   --replication-factor 1`. Let `__transaction_state` auto-create on
   first FindCoordinator.
2. Run a `kafka-verifiable-producer --transactional-id eos-tid
   --transaction-duration-ms 100 --num-records 6` (sends 6 records
   inside committed txns); follow with a second invocation that
   aborts (`--abort-transactions`).
3. If `kafka-verifiable-producer` doesn't expose the right knobs,
   fall back to a small Java snippet in a Docker container that
   uses the `KafkaProducer.beginTransaction` /
   `commitTransaction` / `abortTransaction` API directly.
4. `kafka-console-consumer --isolation-level read_committed
   --max-messages 6` → asserts exactly the 6 committed records,
   none of the aborted ones.
5. `kafka-console-consumer --isolation-level read_uncommitted
   --max-messages 8` → asserts all 8 records (6 committed + 2
   aborted) appear in stream.

## Acceptance gate

Slice 9 is shippable when:

1. `cargo test --workspace -- --include-ignored` passes locally and on
   CI's Linux + macOS runners.
2. `cargo clippy --workspace --all-targets -- -D warnings` clean.
3. `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps` clean.
4. `transactions.rs` integration tests (5 scenarios) green.
5. `jvm_acceptance.rs::transactional_console_producer_eos` green on
   CI's Docker-enabled Ubuntu runner.
6. All slice-1..8 tests still pass (non-transactional producer
   untouched; existing `Producer::builder().bootstrap(...).build()`
   call sites compile + behave identically).

## Risks and mitigations

- **`__transaction_state` consistency under leader change.** The
  coordinator stores per-tid state in this topic; a failover before a
  marker write completes leaves a follower with stale state.
  *Mitigation:* slice-9 always reads-then-writes within the same
  coordinator's lifetime; a leader failover restarts the txn (producer
  sees `CONCURRENT_TRANSACTIONS` and retries via `init_transactions`).
  True hardening requires slice-8's leader-election follow-up.
- **`WriteTxnMarkers` partial failure across partitions.** EndTxn
  dispatches markers to N partitions; if one fails, the txn is
  half-marked. *Mitigation:* coordinator retries marker writes
  indefinitely with backoff; if a partition is permanently
  unreachable, the txn stays in `PrepareCommit`/`PrepareAbort`. A
  future slice adds an operator escape hatch.
- **KIP-1319 v2 producer-epoch-per-transaction protocol fidelity.**
  The KIP's epoch-bump-on-every-init behavior is subtle.
  *Mitigation:* Layer-1 unit tests against a hand-curated
  state-transition table; Layer-3 acceptance against the JVM client
  verifies wire-compat with real Apache Kafka logic.
- **`.txnindex` byte-compat.** The format isn't formally specified;
  we reverse-engineer from the JVM's on-disk shape. *Mitigation:* the
  JVM acceptance test indirectly verifies — if our `.txnindex` is
  wrong, aborted records leak through to `kafka-console-consumer
  --isolation-level read_committed`.

## Next step after this spec

Invoke `superpowers:writing-plans` once committed and approved.
