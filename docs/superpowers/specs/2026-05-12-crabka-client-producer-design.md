# `crabka-client-producer` (slice 6) design

**Status:** draft — slice 6 of the Crabka meta-spec.
**Depends on:** slice 1 (`crabka-protocol`), slice 2 (`crabka-client-core`), slice 3 (`crabka-log`), slice 4 (`crabka-broker`). All shipped to `main`.
**Tracks the meta-spec at:** [`2026-05-10-crabka-rust-rewrite-design.md`](2026-05-10-crabka-rust-rewrite-design.md).

## Goal

Ship a full-idempotent Rust Kafka producer + the broker-side support that backs it. End artifact: a `crabka-client-producer` crate that writes records to a Crabka broker; a JVM `kafka-console-consumer --partition 0 --from-beginning` reads them back. The broker grows real `InitProducerId` handling plus a per-(producer_id, partition) last-sequence tracker that dedupes retries and fences old-epoch writes — the standard idempotent-producer contract.

## In scope

Two crates change:

- **`crabka-broker`** gains a `ProducerIdManager` + per-partition `ProducerState` and a real `InitProducerId` handler. The slice-4 `Produce` handler is extended to read `(producer_id, producer_epoch, base_sequence)` off each record batch and run the standard dedup / out-of-order / epoch-fence checks before appending.
- **`crabka-client-producer`** is a new crate: a high-level `Producer` built on top of `crabka-client-core`, with a `bon`-generated builder, a single sender task draining per-partition accumulators, and pluggable compression.

### Wire surface (added or extended)

| API key | Name              | Notes |
|--------:|-------------------|-------|
| 22      | InitProducerId    | Real impl; returns `(producer_id, producer_epoch)`. Rejects non-empty `transactional_id` with `TRANSACTIONAL_ID_AUTHORIZATION_FAILED`. |
| 0       | Produce           | Extended (slice 4). Reads `(pid, epoch, base_seq)` from each batch; consults `ProducerState`; emits `OUT_OF_ORDER_SEQUENCE_NUMBER` (45), `DUPLICATE_SEQUENCE_NUMBER` (46), or `INVALID_PRODUCER_EPOCH` (90) where appropriate. |

KIP-360 (producer-id-recovery) wire points (`TxnOffsetCommit`, `AddPartitionsToTxn`, etc.) stay `UNSUPPORTED_VERSION` — they're slice 9 (transactions).

### Producer API surface

A subscribe-style builder via [`bon`]:

```rust
let producer = Producer::builder()
    .bootstrap("localhost:9092")
    .client_id("my-app")
    .compression(Compression::Lz4)
    .enable_idempotence(true)              // default
    .acks(Acks::All)                       // required when idempotence is on
    .linger(Duration::from_millis(5))
    .batch_size(16 * 1024)                 // 16 KiB
    .request_timeout(Duration::from_secs(30))
    .build()
    .await?;

let metadata = producer.send(ProducerRecord {
    topic: "my-topic".into(),
    key: Some(Bytes::from("k")),
    value: Some(Bytes::from("v")),
    ..Default::default()
}).await?;

producer.flush().await?;
producer.close().await?;
```

The slice also retrofits the two existing crate builders to `bon`:

- `crabka-client-core::Client::builder` (slice 2) — public API unchanged (`Client::builder().bootstrap(...).client_id(...).build().await?`).
- `crabka-client-consumer::Consumer::builder` (slice 5) — public API unchanged.

The retrofits drop hand-written `ClientBuilder` / `ConsumerBuilder` types in favor of `bon`-generated builders. Existing tests should compile as-is; if any call-site uses an internal constructor, it gets a mechanical fix-up.

### Compression

The producer accepts `Compression::{None, Gzip, Snappy, Lz4, Zstd}`. Codecs live in `crabka-compression` (shipped in slice 1); the producer maps each variant to the `compression_type` bits on the v2 `RecordBatch` header and calls the codec on the batch body before framing. The default is `Compression::None` to match Kafka's defaults.

### Partitioner

Built-in `UniformStickyPartitioner` (Java's 3.0+ default):

- `key` is `Some(...)` → `partition = murmur2(key) % num_partitions`.
- `key` is `None` → use the current sticky partition for this topic; only rotate when the current accumulator drains (the partition's in-flight queue empties).

`ProducerRecord::partition` overrides the partitioner when set.

### Acks

`Acks::{Zero, One, All}`. Wire values 0, 1, -1. When `enable_idempotence = true`, the builder rejects anything other than `Acks::All` (Kafka's standard rule).

### Idempotence

- **Producer side**: every batch carries `(producer_id, producer_epoch, base_sequence)`. `base_sequence` is a 32-bit per-partition counter that monotonically increases by `batch.record_count` after each successful send. On a network failure, the sender re-frames the SAME `RecordBatch` (same bytes, same `base_sequence`) so the broker's dedup catches it.
- **Broker side**: in-memory per-(topic, partition) state. For each `(producer_id, partition)`:
  ```rust
  struct ProducerEntry { epoch: i16, last_sequence: i32, last_offset: i64, last_timestamp: i64 }
  ```
  Updated under the partition's existing lock after a successful append.
- **No persistence** in slice 6. Broker restart resets all `ProducerState`. The producer reacts to subsequent `OUT_OF_ORDER_SEQUENCE_NUMBER` by terminating (see Error handling); user must build a new producer. Real Kafka stores producer snapshots inside the partition log; deferred (slice 9 area).

## Architecture

### Crate layout

```
crates/broker/                                  # additions to slice-4 crate
└── src/
    ├── producer_id_manager.rs                  # NEW — pid allocation + epoch tracking
    ├── producer_state.rs                       # NEW — per-(topic, partition) ProducerEntry map
    ├── codes.rs                                # MODIFIED — add 5 codes
    ├── error.rs                                # MODIFIED — add ProducerEpochFenced
    ├── broker.rs                               # MODIFIED — wire ProducerIdManager
    └── handlers/
        ├── init_producer_id.rs                 # NEW — replaces slice-4 stub
        ├── produce.rs                          # MODIFIED — dedup + fence checks
        └── mod.rs                              # MODIFIED — register InitProducerId

crates/client-producer/                          # NEW crate
├── Cargo.toml
└── src/
    ├── lib.rs                                  # public API
    ├── producer.rs                             # Producer struct + close()
    ├── builder.rs                              # #[bon::builder] on Producer::start
    ├── record.rs                               # ProducerRecord, RecordMetadata, Header
    ├── partitioner.rs                          # UniformStickyPartitioner
    ├── accumulator.rs                          # per-partition InProgressBatch queue
    ├── sender.rs                               # spawned task: drains accumulators
    ├── compression.rs                          # Compression enum + map to RecordBatch bits
    └── error.rs                                # ProducerError

crates/client-core/src/                          # retrofitted to bon
└── client.rs                                   # MODIFIED — Client::builder via #[bon::builder]

crates/client-consumer/src/                      # retrofitted to bon
└── builder.rs                                  # MODIFIED — Consumer::builder via #[bon::builder]
```

The workspace `Cargo.toml` adds `bon = "3"` to `[workspace.dependencies]`.

### Components

#### Broker side

- **`ProducerIdManager`** — `{ next_pid: AtomicI64 (start 1000), epochs: DashMap<i64, AtomicI16> }`. Methods: `allocate() -> (pid, epoch=0)`; `bump_epoch(pid) -> epoch` (for transactional producer fencing — exported but unused in slice 6). Held inside `Broker` as a regular field (no Arc<Mutex> — internal state is already concurrency-safe).

- **`ProducerState`** — `Arc<DashMap<(String, i32), Arc<Mutex<PartitionProducerState>>>>`. `PartitionProducerState { entries: HashMap<i64, ProducerEntry> }`. Held by `Broker` (shared with handlers). The Produce handler holds the per-partition lock for the dedup-check-then-append window.

- **`init_producer_id` handler** — decodes `InitProducerIdRequest`. If `transactional_id` is empty/null, allocate a fresh `(pid, epoch=0)` and return it. If non-empty, reject with `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` (slice 9 will replace this).

- **`produce` handler (modified)** — per `(topic, partition)`:
  ```rust
  match producer_state.check(producer_id, producer_epoch, base_sequence, last_offset_delta) {
      Decision::Append => send ProduceJob; on ack: producer_state.commit(pid, base_seq, base_offset, last_ts)
      Decision::Duplicate { last_offset } => respond NONE error_code, base_offset = last_offset
      Decision::OutOfOrder => respond OUT_OF_ORDER_SEQUENCE_NUMBER (45)
      Decision::Fenced => respond INVALID_PRODUCER_EPOCH (90)
  }
  ```
  For batches without a producer_id (the slice-4 fast path is preserved when `producer_id == -1`), skip the producer-state machinery entirely — non-idempotent producers still work.

#### Producer client

- **`Producer`** — public type. Owns: `client: Client`, `producer_id: i64`, `producer_epoch: i16`, `metadata_cache: Arc<RwLock<HashMap<String, TopicMetadata>>>` (partition counts per topic), `accumulators: DashMap<(String, i32), Arc<Mutex<Accumulator>>>`, `partitioner: UniformStickyPartitioner`, `sender_handle: JoinHandle<()>`, `sender_shutdown: CancellationToken`, `next_seq: DashMap<(String, i32), i32>` (per-partition base-sequence counter), `state: AtomicProducerState` (`Active | Fenced | Closed`).

- **`Producer::send(record) -> impl Future<Output = Result<RecordMetadata, ProducerError>>`** — returns a `oneshot` future immediately; the actual send happens in the background.

- **`Producer::flush() -> Result<(), ProducerError>`** — signals the sender to drain all accumulators; awaits per-partition `Notify`s that fire when in-flight queues empty.

- **`Producer::close()`** — `flush()`, then cancel the sender, then drop the inner client.

- **`ProducerBuilder`** — `#[bon::builder]` on an async `Producer::start` constructor. Fields:
  ```rust
  bootstrap: String,
  client_id: String,                // default "crabka-producer"
  compression: Compression,         // default None
  enable_idempotence: bool,         // default true
  acks: Acks,                       // default One (overridden to All when idempotence on)
  linger: Duration,                 // default 0
  batch_size: usize,                // default 16384
  request_timeout: Duration,        // default 30s
  max_in_flight_per_connection: usize, // default 5
  max_block: Duration,              // default 60s
  retries: i32,                     // default i32::MAX
  retry_backoff: Duration,          // default 100ms
  ```
  `.build()` runs ApiVersions, fetches Metadata for the bootstrap topic list (empty by default → no preload), conditionally calls `InitProducerId` when `enable_idempotence = true`, then spawns the sender task and returns the `Producer`. Idempotence + `acks=Zero` is a build-time error (`ProducerError::InvalidConfig`).

- **`Accumulator`** — per-(topic, partition). State: `VecDeque<InProgressBatch>` plus `current_batch: Option<InProgressBatch>`. Each `InProgressBatch` contains the raw record bytes, the per-record metadata (oneshot tx + offset_delta), `base_sequence` (assigned at send-time), and the current uncompressed size. `try_append(record) -> AppendResult` returns `Appended(oneshot_rx)`, `BatchFull`, or `Backpressure` (when `max_block` exceeded).

- **`UniformStickyPartitioner`** — `Mutex<HashMap<String, i32>>` of sticky partition per topic. `pick(topic, key, num_partitions, in_flight_for_current_sticky) -> partition_id`. Rotates to a new sticky when the current partition's in-flight queue drains AND a new batch was triggered (mirrors Java's behavior).

- **`Sender`** — a single `tokio::spawn`'d task. Loop:
  1. `tokio::select!` on `shutdown.cancelled()` vs. a `wake_rx: mpsc::Receiver<()>` vs. a `linger_interval`.
  2. For each `(topic, partition)` in `accumulators` with a ready batch (linger expired OR batch full OR `flush_pending`):
     - Pop the in-progress batch.
     - Fill in `base_sequence = next_seq[(topic, partition)]` (and store before send).
     - Compress the batch body via the configured codec; populate the v2 `RecordBatch` header bits.
     - Frame a `ProduceRequest { acks, timeout_ms, topic_data }`.
     - `client.send(...) → ProduceResponse`.
     - For each `(topic, partition)` response:
       - `error_code == 0` → resolve each record's oneshot with `RecordMetadata { topic, partition, offset = base_offset + offset_delta, timestamp }`.
       - `error_code == DUPLICATE_SEQUENCE_NUMBER (46)` → resolve as `Ok(RecordMetadata { offset = response.base_offset, ... })` for every record in the batch.
       - `error_code == OUT_OF_ORDER (45)` OR `INVALID_PRODUCER_EPOCH (53)` → `state = Fenced`; resolve every queued record's oneshot with `Err(FencedProducer)`; future `send()` returns `Err(Closed)`.
       - Retryable codes (`NOT_LEADER_OR_FOLLOWER`, `LEADER_NOT_AVAILABLE`, network errors) → re-enqueue the batch with the same `base_sequence` (dedup guarantees correctness); back off `retry_backoff`. Stop after `retries` attempts → resolve every record with `Err(Client(...))`.

- **`Compression`** — enum + per-variant `compress(&[u8]) -> Result<Vec<u8>, CompressionError>` that delegates to `crabka-compression`.

### Retrofits (existing crates)

**`crabka-client-core`**: replace `pub struct ClientBuilder { ... }` + `impl ClientBuilder { ... }` with `#[bon::builder]` on an async `Client::start` constructor. `Client::builder()` keeps the same call sites. Drop the now-dead `ClientBuilder` type.

**`crabka-client-consumer`**: same treatment for `ConsumerBuilder` on `Consumer::start`. The `.subscribe(&[&str])` call becomes a `bon`-style setter that accepts `Vec<String>` or `&[&str]` (via `impl Into<Vec<String>>` via `bon`'s `#[builder(into)]` attribute).

## Data flow

### Producer::send (happy path)

```
[caller]   producer.send(ProducerRecord { topic, key, value, .. })
   │
   ▼
[partitioner]   pick(topic, key, num_partitions) → partition_id
   │
   ▼
[accumulator]   per-(topic, partition) lock:
                  - if current batch + record > batch.size: seal current; start new
                  - append (key, value, headers, timestamp) to current batch
                  - record (offset_delta = batch.record_count - 1)
                  - allocate a oneshot<Result<RecordMetadata, ProducerError>>
                  - return the oneshot Rx
   │
   ▼
[caller]   .await on the oneshot (suspends here)

(sender wakes via linger timer or batch-size signal)

[sender]   for each partition with a ready batch:
              - base_sequence ← next_seq[(topic, partition)]
              - next_seq[(topic, partition)] += batch.record_count
              - compress + frame ProduceRequest
              - client.send → ProduceResponse
              - per partition response code:
                  NONE   → resolve oneshots with offsets
                  46     → resolve oneshots with cached offsets (duplicate succeeds)
                  45/53  → fence producer; FencedProducer
                  retryable → re-enqueue same batch; back off
                  other  → Server(code) for each record
```

### Broker-side idempotent Produce dispatch

```
[connection task]   decode ProduceRequest
   │
   ▼
[per-(topic, partition) loop]
   - decode the RecordBatch
   - read (producer_id, producer_epoch, base_sequence, last_offset_delta) from header
   - if producer_id < 0: slice-4 path — append unconditionally
   - else: producer_state.check_and_reserve(pid, epoch, base_seq, last_offset_delta):
       Decision::Append { reservation } → send ProduceJob; on ack:
                                              producer_state.commit(reservation, base_offset, last_ts)
       Decision::Duplicate { last_offset } → respond NONE, base_offset = last_offset
       Decision::OutOfOrder               → respond 45
       Decision::Fenced                    → respond 53
```

The `check_and_reserve` / `commit` split keeps the per-partition mutex held only for the brief reservation window; the actual `Log::append` happens outside the lock. If the writer fails, the reservation is discarded.

### Startup interaction with slice 4 / 5

- `Broker::start` adds the `ProducerIdManager` + `ProducerState` to its state struct.
- Slice-4 partitions, slice-5 group coordinator, slice-6 producer state all coexist in the same `Broker` struct. No persistence yet; restart resets producer state (idempotence-wise) but not committed offsets or topic metadata.

## Error handling

### Wire codes (new in `crates/broker/src/codes.rs`)

| Code | Name                                    | Where |
|-----:|-----------------------------------------|-------|
| 45   | OUT_OF_ORDER_SEQUENCE_NUMBER            | Produce dedup check fails: `base_sequence != last_seq + 1`. |
| 46   | DUPLICATE_SEQUENCE_NUMBER               | Produce sees a previously-committed `base_sequence` — reply with cached `base_offset`. |
| 47   | INVALID_PRODUCER_ID_MAPPING             | Reserved (slice 9). |
| 53   | INVALID_PRODUCER_EPOCH                  | Lower-epoch producer fenced by newer instance. |
| 67   | TRANSACTIONAL_ID_AUTHORIZATION_FAILED   | `InitProducerId` carries a `transactional_id` that we don't support. |

### Internal `BrokerError` (one new variant)

```rust
#[error("producer epoch fenced: pid={producer_id} got {requested}, current {current}")]
ProducerEpochFenced { producer_id: i64, current: i16, requested: i16 },
```

Maps to `INVALID_PRODUCER_EPOCH` (53) at the handler boundary.

### `ProducerError`

```rust
#[non_exhaustive]
pub enum ProducerError {
    #[error("client: {0}")] Client(#[from] crabka_client_core::ClientError),
    #[error("protocol: {0}")] Protocol(#[from] crabka_protocol::ProtocolError),
    #[error("broker error_code {0}")] Server(i16),
    #[error("fenced by newer producer instance")] FencedProducer,
    #[error("invalid config: {0}")] InvalidConfig(&'static str),
    #[error("batch too large: {batch_size} > max")] BatchTooLarge { batch_size: usize },
    #[error("record too large: {record_size} > max_request_size")] RecordTooLarge { record_size: usize },
    #[error("send buffer full (max_block exceeded)")] BufferFull,
    #[error("producer closed")] Closed,
}
```

### Resolution rules at `send().await`

- `Server(46)` (duplicate): success. Resolve with the broker-echoed `base_offset`. Caller can't tell a retry from a fresh write.
- `Server(45)` / `Server(53)` (fenced): TERMINAL. Every pending record resolves `Err(FencedProducer)`; subsequent `send()` returns `Closed`. User builds a new producer to recover.
- `Server(*)` (other non-zero): individual oneshot resolves `Err(Server(code))`; producer stays alive.
- Network failure / `ClientError`: sender retries up to `retries` with `retry_backoff`. On exhaustion: every record in the batch resolves `Err(Client(...))`.

### Retries + idempotence

Idempotent producer: retries are SAFE. Sender re-frames the EXACT same `RecordBatch` (same bytes, same `base_sequence`). Broker's dedup catches the retry and replies `DUPLICATE_SEQUENCE_NUMBER`; we surface as success.

Non-idempotent producer (`enable_idempotence = false`): retries are NOT safe; the producer is allowed to retry but duplicates may actually duplicate. The builder forces `max_in_flight_per_connection = 1` when `enable_idempotence = false` AND `retries > 0` (Java's safety rule).

### Panic / supervisor handling

Sender task is `tokio::spawn`'d under a `JoinHandle`. On panic: every queued oneshot resolves `Err(Closed)`; subsequent `send()` returns `Closed`. User builds a new producer.

## Testing

### Unit tests

**Producer client** (`crates/client-producer/tests/unit.rs`):

- Partitioner: parameterized table for `UniformStickyPartitioner` — null-key sticky; explicit-key hash determinism.
- Accumulator: `try_append` rolls over at `batch.size`; oneshots resolve in record order when a batch's `(base_offset, base_sequence)` is filled.
- Compression round-trip: one test per codec (`gzip`, `snappy`, `lz4`, `zstd`) — encode → compress → decode → match.
- Builder validation: `enable_idempotence = true` + `acks = Zero` → `Err(InvalidConfig)` at `.build()`.
- `MockBroker`-driven (reuse slice 2's mock): send → ProduceRequest wire shape captured; injected response triggers correct oneshot resolution.

**Broker** (`crates/broker/tests/unit.rs` extensions):

- `producer_id_manager` table-driven: fresh allocations are monotonically increasing; `epoch` starts at 0.
- `producer_state` parameterized: `(epoch, base_seq) → expected Decision`. Covers ok / duplicate / out-of-order / fenced cases.
- `InitProducerId` handler: empty `transactional_id` → success; non-empty → `TRANSACTIONAL_ID_AUTHORIZATION_FAILED`.
- Produce handler dedup: same `(pid, base_seq)` twice → second one returns `DUPLICATE_SEQUENCE_NUMBER` with the first's offset.

### Integration tests

`crates/client-producer/tests/integration.rs` (new; in-process broker; no Docker):

- `idempotent_produce_then_consume`: send 1000 records, consume via `crabka-client-consumer`, assert all 1000 present, in order, no duplicates.
- `duplicate_send_resolves_as_success`: force a transport retry (white-box) on the same batch; both `await`s return `Ok` with the same offset.
- `compression_round_trip`: one test per codec; consume the produced records back via the in-process consumer.
- `out_of_order_fence_terminates_producer`: white-box inject an out-of-order sequence into the sender; assert subsequent `send()`s return `Closed`.
- `non_idempotent_acks_zero_fire_and_forget`: builder accepts `acks = Zero` with `enable_idempotence = false`; `send()` returns success after batch is enqueued.

### JVM acceptance

`crates/broker/tests/jvm_acceptance.rs` adds one test:

- `rust_producer_to_console_consumer`: build a `crabka-client-producer` on the host pointed at the host broker; send 3 records; `docker run --rm --add-host=host.docker.internal:host-gateway mirror.gcr.io/confluentinc/cp-kafka:6.1.1 kafka-console-consumer --bootstrap-server host.docker.internal:9092 --topic crabka-rust-producer-itest --partition 0 --from-beginning --max-messages 3`; assert all 3 records appear. Joins the existing `broker-jvm-acceptance` job.

### Out of scope for testing

Transactions, multi-broker producer redirection on `NOT_LEADER_OR_FOLLOWER`, partition-leader-failover dedup, kill-9 mid-batch durability, performance benchmarks under load.

## Out of scope (explicit non-goals)

- **Transactions** — slice 9.
- **Persisted producer-state snapshots** — slice 6 keeps producer sequences in memory. Broker restart = sequence reset.
- **Cross-partition atomicity** — `ProduceRequest` may carry multiple `(topic, partition)` entries but each gets independent dedup. Transactions cover the atomic case.
- **Schema registry / serde glue** — `key` and `value` are `Bytes`. No Avro / Protobuf / JSON Schema.
- **Custom partitioner trait** — sticky+hash only; users can override per-record via `ProducerRecord::partition`.
- **Producer interceptors** (Java `ProducerInterceptor`) — out.
- **Quota / throttle handling** — `throttle_time_ms` is logged but not honored.
- **`max.in.flight.requests.per.connection > 1` with non-idempotent retries** — automatically capped at 1 when idempotence is off and retries > 0.
- **Sender thread pool** — one task drains all partitions.
- **Sender metrics** — `tracing` only; no per-record-rate gauges.
- **`crabka-producer` binary CLI** — slice 10.

## Acceptance gate

The slice is done when, in CI:

1. `cargo fmt --all -- --check` clean.
2. `cargo clippy --workspace --all-targets -- -D warnings` clean.
3. `cargo test -p crabka-broker`, `cargo test -p crabka-client-producer` pass.
4. `cargo test --workspace --include-ignored` no regressions in slices 1-5.
5. `broker-jvm-acceptance` job green AND includes `rust_producer_to_console_consumer`.
6. `cargo doc -p crabka-broker --no-deps` and `cargo doc -p crabka-client-producer --no-deps` build without warnings.
7. Public API of `crabka-client-producer`: `Producer`, `ProducerRecord`, `RecordMetadata`, `Header`, `Compression`, `Acks`, `ProducerError`. Builder via `Producer::builder()`. No transactional API surface.
8. Retrofitted `Client::builder()` and `Consumer::builder()` still compile and tests still pass.

## Reference

Meta-spec: [`2026-05-10-crabka-rust-rewrite-design.md`](2026-05-10-crabka-rust-rewrite-design.md).
Slice 4 spec: [`2026-05-11-crabka-broker-design.md`](2026-05-11-crabka-broker-design.md).
Slice 5 spec: [`2026-05-11-crabka-consumer-groups-design.md`](2026-05-11-crabka-consumer-groups-design.md).
[`bon`]: https://docs.rs/bon/
