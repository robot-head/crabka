# `crabka-broker` (slice 4) design

**Status:** draft (this is the spec for slice 4 of the Crabka meta-spec).
**Depends on:** `crabka-protocol` (slice 1) and `crabka-log` (slice 3), both shipped to `main`.
**Tracks the meta-spec at:** [`docs/superpowers/specs/2026-05-10-crabka-rust-rewrite-design.md`](2026-05-10-crabka-rust-rewrite-design.md).

## Goal

Ship a single-node `crabka-broker` binary that an unmodified JVM Kafka client can produce records to and consume from. End artifact: a `broker-jvm-acceptance` CI job in which `kafka-console-producer` pipes records into the broker and `kafka-console-consumer --partition 0 --from-beginning` reads them back, both running from the official Apache Kafka image via testcontainers, both connecting to a Rust `crabka-broker` process.

## In scope

A library crate (`crabka-broker`) plus a thin `crabka-broker` binary. The library exposes `Broker::start(config) -> BrokerHandle` so future integration tests and the conformance harness (slice 11 prep) can drive an in-process broker without spawning a child.

### Wire surface

Handler implementations for exactly these API keys; everything else replies with `UNSUPPORTED_VERSION` (35):

| API key | Name              | Why we need it                                                                   |
|--------:|-------------------|----------------------------------------------------------------------------------|
| 18      | ApiVersions       | First call from every Kafka client; without it nothing else negotiates.          |
| 3       | Metadata          | Drives topic + partition + leader discovery for both producer and consumer.      |
| 19      | CreateTopics      | `kafka-topics --create`.                                                         |
| 20      | DeleteTopics      | `kafka-topics --delete`, plus cleanup between test runs.                         |
| 0       | Produce           | `kafka-console-producer`.                                                        |
| 1       | Fetch             | `kafka-console-consumer`.                                                        |
| 2       | ListOffsets       | Consumer queries beginning / latest / by-timestamp before its first Fetch.       |
| 32      | DescribeConfigs   | `kafka-topics --describe` probes this.                                           |
| 10      | FindCoordinator   | Consumer always sends this; we stub-fail with `COORDINATOR_NOT_AVAILABLE` (15).  |

Topic provisioning is RPC-only — no auto-topic-creation on Produce, no static config file. Metadata mutation goes exclusively through CreateTopics / DeleteTopics.

### Concurrency model

`tokio::spawn` per accepted TCP connection. Within a connection, requests are decoded and dispatched sequentially (Kafka guarantees in-order responses per connection). Writes go through an mpsc channel to a single-writer-per-partition actor task that owns the `crabka_log::Log`. Reads use `Arc<Log>` directly via `&self` — concurrent reads are safe per slice 3's design.

Critical: connection tasks never block on log I/O. Produce sends a `ProduceJob` on the mpsc channel and `await`s a oneshot; Fetch calls `log.read` against an `Arc<Log>` shared with the writer.

## Architecture

### Crate layout

```
crates/broker/
├── Cargo.toml
├── src/
│   ├── lib.rs                 # public API: Broker, BrokerConfig, BrokerHandle, BrokerError
│   ├── bin/broker.rs          # the crabka-broker binary (clap CLI)
│   ├── config.rs              # BrokerConfig (listen_addr, log_dir, broker_id, advertised_listener)
│   ├── error.rs               # BrokerError
│   ├── metadata.rs            # in-memory metadata image
│   ├── partition.rs           # Partition handle (mpsc sender + Arc<Log>)
│   ├── partition_writer.rs    # spawned actor: rx ProduceJob, drives Log::append
│   ├── log_dir.rs             # <log_dir>/<topic>-<partition>/ helpers + startup scan
│   ├── network/
│   │   ├── mod.rs             # TcpListener + per-connection loop
│   │   ├── codec.rs           # LengthDelimitedCodec wired to Kafka framing
│   │   └── request_id.rs      # RequestHeader decode + dispatch
│   └── handlers/
│       ├── mod.rs             # api_key → handler routing table
│       ├── api_versions.rs
│       ├── metadata.rs
│       ├── create_topics.rs
│       ├── delete_topics.rs
│       ├── produce.rs
│       ├── fetch.rs
│       ├── list_offsets.rs
│       ├── describe_configs.rs
│       └── find_coordinator.rs
└── tests/
    ├── unit.rs                # per-handler tests with synthetic metadata
    ├── integration.rs         # spawn broker in-process; drive with crabka-client-core
    └── jvm_acceptance.rs      # testcontainers JVM clients → broker
```

### Components

- **`Broker`** (library entry point) — owns the network listener task, `Arc<RwLock<MetadataImage>>`, and a `DashMap<(String, i32), Arc<Partition>>` partition registry. Constructed via `Broker::start(config) -> BrokerHandle`. `BrokerHandle: Send + 'static`, exposes `.shutdown()`, `.listen_addr() -> SocketAddr`. Drops cleanly: listener stops accepting, in-flight requests drain, partition writers receive shutdown, log files fsync.
- **`MetadataImage`** — `topics: HashMap<String, TopicMeta>`. `TopicMeta { topic_id: Uuid, partitions: Vec<PartitionMeta> }`. `PartitionMeta { partition_id: i32, leader_broker_id: i32, replicas: Vec<i32>, isr: Vec<i32> }`. All leader / replica / ISR entries are this broker's id. No persistence across restarts — `log_dir` scan on startup rebuilds entries from the directory layout (`<topic>-<partition>` naming).
- **`Partition`** — `{ topic: String, partition_id: i32, log: Arc<crabka_log::Log>, writer_tx: mpsc::Sender<ProduceJob>, writer_handle: JoinHandle<()> }`. Lifecycle: created on CreateTopics → spawned writer → registered in the DashMap. Dropped on DeleteTopics → registry removal → writer mpsc closes → writer task drains and exits → log dir is rm'd.
- **`partition_writer`** — single task per partition. Loop: `recv` a `ProduceJob`, call `log.append(&mut batch)`, send the assigned base offset back on the oneshot. Sole owner of `&mut Log`. On `log.append` error, propagates the error back via oneshot — never panics out of the supervisor's reach.
- **`network::*`** — `TcpListener::bind(config.listen_addr)`, accept loop spawns one task per connection. The task wraps the stream in `LengthDelimitedCodec` (same big-endian i32 framing as `crabka-client-core`), parses each request via `RequestHeader::decode`, dispatches to the right handler, encodes the response via `Response::encode`. Connection-level errors (frame decode failure, peer disconnect) close the stream; broker stays up.
- **`handlers/*`** — one module per supported API key. Each implements:
    ```rust
    async fn handle(broker: &Broker, version: i16, req: Req) -> Result<Resp, BrokerError>
    ```
    Routing built at startup as a `HashMap<i16, fn pointer>`. Anything not in the table responds with `UNSUPPORTED_VERSION`.
- **`log_dir`** — path helpers (`<log_dir>/<topic>-<partition>/`) and the startup-scan routine that walks the directory and registers existing partitions.

## Data flow

### Produce

```
[JVM client]
   │  TCP write: 4-byte len + RequestHeader v2 + ProduceRequest v9
   ▼
[per-connection task]
   1. Read frame, decode RequestHeader, look up handler.
   2. Decode ProduceRequest.
   3. For each (topic, partition, RecordBatch) in the request:
        a. Resolve PartitionRef from registry. Unknown → UNKNOWN_TOPIC_OR_PARTITION (3).
        b. Send ProduceJob { batch, ack } on the partition's mpsc Sender.
           Channel-closed → NOT_LEADER_OR_FOLLOWER (6).
        c. Await ack (with the request's timeout_ms).
   4. Build ProduceResponse with per-partition results + assigned base offset.
   5. Encode + write frame back on the same TCP stream.

[partition_writer task]              (one per partition, owns the Log)
   loop:
     job = rx.recv().await;
     match log.append(&mut job.batch):
       Ok(base) => job.ack.send(Ok(base));
       Err(e)   => job.ack.send(Err(e));
```

### Fetch

```
1. Decode FetchRequest.
2. For each (topic, partition, fetch_offset, max_bytes):
     a. Resolve PartitionRef. Unknown → UNKNOWN_TOPIC_OR_PARTITION + empty.
     b. log.read(fetch_offset, max_bytes) via Arc<Log> with &self.
     c. Accumulate raw RecordBatch bytes for the response.
3. Build FetchResponse, encode, write.
```

If `total_bytes < min_bytes` and `max_wait_ms > 0`, the handler `tokio::select!`s on (a) `tokio::time::sleep(max_wait_ms)` and (b) per-partition `tokio::sync::Notify` that the writer signals on every successful append. Wakes early when new data arrives so `kafka-console-consumer` blocks cleanly instead of busy-polling.

### CreateTopics

```
1. Decode CreateTopicsRequest.
2. For each requested topic:
     a. Validate name + partition_count.
     b. Acquire write-lock on MetadataImage.
     c. Insert TopicMeta with random topic_id, partitions[0..N] each leader=self.
     d. For each partition:
          - mkdir <log_dir>/<topic>-<i>
          - Log::open it
          - spawn partition_writer
          - insert PartitionRef into the registry
     e. Release lock.
3. Reply with per-topic result.
```

### DeleteTopics

Mirror of CreateTopics: remove registry entries, drop the partition mpsc senders (which closes the writer's channel and triggers shutdown), `join` the writer handles, `rm -rf` the partition directories. In-flight Produce / Fetch against a deleting topic gets UNKNOWN_TOPIC_OR_PARTITION as soon as the registry entry is gone.

### Startup

Scan `log_dir`, find `<topic>-<partition>` directories, `Log::open` each, populate `MetadataImage` + partition registry, spawn partition writers, only then bind the TCP listener. New broker with empty `log_dir` starts with zero topics.

## Error handling

Two layers, distinct audiences.

### Wire-level error codes (client-visible)

Per-(topic, partition) `error_code: i16` fields are populated with the canonical Apache Kafka codes; JVM clients react to specific codes and will misbehave if we substitute. Codes the MVP must emit correctly:

| Code | Name                          | When |
|-----:|-------------------------------|------|
| 0    | NONE                          | Success. |
| 1    | UNKNOWN_SERVER_ERROR          | Internal `BrokerError` we didn't map. Includes `tracing::error!` on the broker side. |
| 3    | UNKNOWN_TOPIC_OR_PARTITION    | Topic / partition not in registry. |
| 6    | NOT_LEADER_OR_FOLLOWER        | Partition was alive at metadata-lookup time but its writer mpsc is now closed. |
| 7    | REQUEST_TIMED_OUT             | `timeout_ms` exceeded waiting for partition writer ack. |
| 15   | COORDINATOR_NOT_AVAILABLE     | FindCoordinator stub response. |
| 35   | UNSUPPORTED_VERSION           | API key + version combination not in our handler routing table. |
| 36   | TOPIC_ALREADY_EXISTS          | CreateTopics on an existing name. |
| 37   | INVALID_PARTITIONS            | CreateTopics with `partition_count <= 0`. |
| 41   | NOT_CONTROLLER                | (Reserved — admin clients sometimes route topic ops through "the controller". We're it.) |

### Internal `BrokerError`

```rust
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BrokerError {
    #[error("I/O: {0}")] Io(#[from] std::io::Error),
    #[error("log: {0}")] Log(#[from] crabka_log::LogError),
    #[error("protocol: {0}")] Protocol(#[from] crabka_protocol::ProtocolError),
    #[error("unsupported api_key={api_key} version={version}")]
    UnsupportedApi { api_key: i16, version: i16 },
    #[error("partition writer for {topic}-{partition} died")]
    PartitionWriterDied { topic: String, partition: i32 },
    #[error("shutting down")] Shutdown,
}
```

Conversion `BrokerError → wire-level code` happens at the handler boundary. Most internal errors map to `1` (UNKNOWN_SERVER_ERROR) with the underlying detail logged.

### Panic handling

Every `tokio::spawn` returns a `JoinHandle` that an internal supervisor task awaits. A connection task panic logs + closes the connection only — the broker process never exits. A partition-writer panic logs + the supervisor flips that partition's registry entry into a "writer dead" state (subsequent Produce returns NOT_LEADER_OR_FOLLOWER) and emits a high-priority `tracing::error!`.

## Configuration

`BrokerConfig`:

```rust
pub struct BrokerConfig {
    pub broker_id: i32,                  // default: 1
    pub listen_addr: SocketAddr,         // default: 127.0.0.1:9092
    pub advertised_listener: String,     // e.g. "localhost:9092" — what Metadata returns
    pub log_dir: PathBuf,                // default: ./crabka-data
    pub log_config: crabka_log::LogConfig,
    pub num_io_threads: usize,           // default: 0 = use tokio's default
}
```

The binary parses flags via `clap` (`--listen-addr`, `--log-dir`, `--broker-id`, `--advertised-listener`). No server.properties; no config file; flag-driven for the MVP.

## Testing

Three layers, each guarding a different failure mode.

### Unit tests (`tests/unit.rs`)

Per-handler tests with an in-process `Broker` + synthetic metadata. Verify success path + documented error paths for each handler: unknown topic, version negotiation, malformed body, write-to-deleted-topic. Run on every push. No Docker, no network.

### Integration tests (`tests/integration.rs`)

Spawn the `Broker` in-process; drive it with `crabka-client-core` (slice 2). One end-to-end scenario per request type:

- `connect_and_negotiate_versions`
- `create_then_describe_topic`
- `produce_then_fetch_records`
- `delete_topic_removes_partitions`
- `concurrent_producers_to_same_partition`

Catches Crabka-client ↔ Crabka-broker interop regressions on every push.

### JVM acceptance (`tests/jvm_acceptance.rs`, `#[ignore]`-gated, Linux-only)

testcontainers pulls `mirror.gcr.io/confluentinc/cp-kafka:6.1.1` solely for the bundled `kafka-console-producer` / `kafka-console-consumer` / `kafka-topics` binaries. The broker runs as a normal Rust process on the host; the JVM tools run inside the container and connect back to the broker via `host.docker.internal` (Mac/Windows) or `--network host` (Linux CI). Two scenarios:

1. `console_producer_round_trip` — `kafka-topics --create`, pipe stdin into `kafka-console-producer`, then `kafka-console-consumer --from-beginning --partition 0 --max-messages N`. Assert the consumer emits the same records.
2. `kafka_topics_describe_smokes_metadata` — `kafka-topics --describe` after a create, parse stdout, assert topic + partition counts.

Both run in CI under a new `broker-jvm-acceptance` job. Same `KNOWN_ISSUES.md` escape hatch as slice 3 if a test hits an environmental snag.

### Out of scope for testing

Chaos / partition fault injection, performance gates, fuzzing, multi-broker, network partition. Those land in later slices.

## Out of scope (explicit non-goals)

- **Replication, leader election, ISR** — slice 8. MVP is single-broker; all leader / ISR fields point at this broker.
- **KRaft / metadata quorum** — slice 7. Metadata image lives in process memory and is reconstructed from the directory layout on startup. No metadata records, no controller log.
- **Consumer groups, offset commits, coordinators** — slice 5. FindCoordinator stubs to `COORDINATOR_NOT_AVAILABLE`; consumers must use `--partition` to bypass groups.
- **Idempotent / transactional producers** — slices 6 + 9. InitProducerId, AddPartitionsToTxn, etc. respond with UNSUPPORTED_VERSION (35).
- **Auth, TLS, SASL, ACLs** — slice 11. PLAINTEXT only, no authorizer.
- **Tiered storage** — slice 12.
- **Log compaction** — explicitly deferred from slice 3. The broker ignores `cleanup.policy=compact` topic configs and treats every topic as plain delete-retention.
- **Quotas, throttling, request prioritization** — out.
- **Multi-listener / advertised-listener-per-protocol** — one TCP listener, one advertised endpoint.
- **Auto-topic-creation on Produce** — out. CreateTopics RPC is the only path.
- **Configurable fsync policy** (`flush.messages` / `flush.ms`) — ignored. `BrokerConfig.log_config.flush_on_append` is exposed for tests but defaults off.
- **JMX-compatible metrics** — out. Plain `tracing` spans + structured fields for the MVP. OpenTelemetry export ships when slice 11's observability work lands.

## Acceptance gate

The slice is done when, in CI:

1. `cargo fmt --all -- --check` clean.
2. `cargo clippy --workspace --all-targets -- -D warnings` clean.
3. `cargo test -p crabka-broker` passes (unit + integration).
4. `cargo test --workspace --include-ignored` is no worse than before (no regressions in other slices).
5. `broker-jvm-acceptance` job is green: both scenarios pass.
6. `cargo doc -p crabka-broker --no-deps` builds without warnings; every public type carries rustdoc.
7. Public API matches the spec: `Broker`, `BrokerHandle`, `BrokerConfig`, `BrokerError`.

## Reference

Meta-spec: [`2026-05-10-crabka-rust-rewrite-design.md`](2026-05-10-crabka-rust-rewrite-design.md) (slice 1 detail is the "Slice 1 detailed design" section there).
Slice 2 spec: [`2026-05-11-crabka-client-core-design.md`](2026-05-11-crabka-client-core-design.md).
Slice 3 spec: [`2026-05-11-crabka-log-design.md`](2026-05-11-crabka-log-design.md).
