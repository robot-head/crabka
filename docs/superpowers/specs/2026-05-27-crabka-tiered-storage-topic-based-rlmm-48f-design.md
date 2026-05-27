# Crabka tiered storage 48f — `TopicBasedRemoteLogMetadataManager` (design)

**Date:** 2026-05-27
**Status:** Slice design. Follows slice 48e (remote retention + partition
delete). Part of the KIP-405 umbrella
(`docs/superpowers/specs/2026-05-25-crabka-tiered-storage-roadmap-design.md`).

## Goal

Replace `InmemoryRemoteLogMetadataManager` as the broker's default
[`RemoteLogMetadataManager`] with a durable, multi-broker-shared
implementation that persists remote-segment lifecycle events in an
internal Kafka topic, **`__remote_log_metadata`**. After this slice,
remote-tier metadata survives a broker restart, is consistent across
brokers in a cluster, and no longer requires every broker to be the
sole writer of the partitions it leads.

`InmemoryRemoteLogMetadataManager` stays as the test fixture (every
48a-48e test runs against it) and is the default in single-broker /
ephemeral configurations; production multi-broker deployments will
select the topic-backed manager via the new config introduced here.

## What Kafka does

Apache Kafka's `TopicBasedRemoteLogMetadataManager` (TBRLMM) is the
production default. The key invariants we adopt unchanged:

- Internal topic `__remote_log_metadata`. 50 partitions by default,
  replication factor 3, `cleanup.policy=delete`,
  `retention.ms=-1` (infinite). No log compaction — events are an
  append-only event log, not a key-value snapshot.
- Each user `TopicIdPartition` deterministically hashes to **one**
  metadata-topic partition; every metadata event for that user
  partition lands there. The metadata topic's ordering within a single
  partition is the source of truth for that user partition's lifecycle
  ordering.
- A broker maintains an in-memory cache (the existing 48a
  `RemoteLogMetadataCache`) per user partition by **consuming the
  metadata topic** and replaying events into it. Writers publish
  events; readers learn about them through the same loop.
- `add_remote_log_segment_metadata` / `update_…` / `put_remote_partition_delete_metadata`
  are **read-your-writes**: each call publishes an event and waits
  until that event has come back through the consumer and been
  applied to the local cache, before returning.

This design follows that shape. Where Kafka's TBRLMM allows brokers
to consume a *subset* of metadata partitions (limited to those that
host their leader/follower assignments), 48f's first cut **consumes
all metadata-topic partitions on every broker**. Partition-set
assignment is an optimization deferred to a follow-up.

## Non-goals (deferred)

- Per-broker metadata-partition assignment / re-assignment on
  rebalance — every broker consumes all `__remote_log_metadata`
  partitions. Acceptable for small clusters; the all-partitions
  consumer load grows with cluster size.
- Snapshot / fast bootstrap — every broker startup re-reads the full
  metadata topic from offset 0. A snapshot file (`RemoteLogMetadataSnapshotFile`
  in Kafka) is a future optimization.
- Authentication on the internal metadata-client connection. The
  TBRLMM connects via plaintext loopback to its own broker (`localhost:<listener>`).
  TLS / SASL on the internal client is a follow-up that needs the
  broker to expose a trusted inter-broker endpoint with credentials
  the manager can use.
- Compaction of `__remote_log_metadata`. Eventually we want
  per-`TopicIdPartition` compaction (collapse to "current state"
  records) but for 48f the topic is unbounded.
- Custom `remote.log.metadata.manager.class.name` plug-in mechanism;
  we surface a closed enum `inmemory | topic` only.

## Crate layout

A new workspace member, **`crates/remote-storage-topic`**
(`crabka-remote-storage-topic`):

```
crates/remote-storage-topic/
├── Cargo.toml
└── src/
    ├── lib.rs
    ├── serde.rs       — wire format for metadata events
    ├── partitioning.rs — TopicIdPartition → metadata-topic partition
    ├── log.rs         — `MetadataEventLog` trait + in-process fixture
    ├── kafka_log.rs   — Kafka-backed `MetadataEventLog` (producer/consumer/admin)
    ├── manager.rs     — `TopicBasedRemoteLogMetadataManager` (the RLMM impl)
    └── config.rs      — `TopicRlmmConfig` (topic name, partitions, RF, bootstrap)
```

Dependencies:

- `crabka-remote-storage` — the SPI traits + data model + the
  `RemoteLogMetadataCache` (slice 48a exposes it as `pub` for re-use
  by this crate; the in-memory manager keeps wrapping it).
- `crabka-client-producer`, `crabka-client-consumer`,
  `crabka-client-admin`, `crabka-client-core` — the runtime
  publish/subscribe machinery and the topic-create call.
- `crabka-protocol` — record types (`RecordBatch`, `Record`) for
  the consumer side.
- `tokio` — the async runtime the producer/consumer require; the
  TBRLMM holds a `runtime::Handle` to bridge the sync RLMM SPI.
- `bytes`, `thiserror`, `tracing`, `uuid`, `async-trait`.

`crates/remote-storage`'s `RemoteLogMetadataCache` (today `pub(crate)`)
is promoted to `pub` so this crate can drive it directly. The cache
already enforces the full 48a lifecycle state machine; TBRLMM should
not re-implement it.

## `MetadataEventLog` — the publish/subscribe seam

Both for testability and to keep the manager free of Kafka client
internals, the manager talks to the topic through a small async
trait:

```rust
#[async_trait]
pub trait MetadataEventLog: Send + Sync {
    /// Number of metadata-topic partitions. Stable for the life of
    /// the log; the manager hashes user partitions into [0, count()).
    fn partition_count(&self) -> i32;

    /// Append `event` to `partition`; resolves to the assigned offset
    /// when the broker has acked the write.
    async fn publish(
        &self,
        partition: i32,
        event: Bytes,
    ) -> Result<i64, MetadataLogError>;

    /// Subscribe to every partition this log holds. Each item carries
    /// the source `(partition, offset)` and the raw event bytes. The
    /// stream starts at offset 0 of every partition and never ends
    /// until the receiver is dropped.
    fn subscribe(&self) -> BoxStream<'static, MetadataEventRecord>;

    /// Current high-water-mark per partition. Used at startup to
    /// compute the initial-catch-up target before the manager begins
    /// answering queries.
    async fn high_water_marks(&self) -> Result<Vec<i64>, MetadataLogError>;
}

pub struct MetadataEventRecord {
    pub partition: i32,
    pub offset: i64,
    pub payload: Bytes,
}
```

Two implementations ship in 48f:

- **`InProcessMetadataEventLog`** (`log.rs`) — a tokio-broadcast-channel
  fixture used by unit tests. Multiple TBRLMMs that share the same
  fixture see each other's writes, modeling the multi-broker case
  without bringing up a cluster.
- **`KafkaMetadataEventLog`** (`kafka_log.rs`) — the production
  implementation. Holds a `crabka_client_producer::Producer`, spawns
  per-partition `crabka_client_consumer::Consumer` poll loops, and
  uses `crabka_client_admin::AdminClient` to ensure the topic exists
  on startup with the configured partition count and replication
  factor.

## Wire format

A single versioned binary encoding shared by every event type. No
serde / JSON / protobuf dependency — hand-rolled to keep the
crate dependency-light and to make on-wire bytes obvious in tests:

```text
event := version u8 = 0
       | tag     u8 ∈ {0, 1, 2}
       | payload (tag-dependent)

string   := uvarint length + UTF-8 bytes
uuid     := 16 raw bytes (big-endian)
i32      := 4 bytes big-endian
i64      := 8 bytes big-endian
opt<T>   := 0u8 (None) | 1u8 + T
bytes    := uvarint length + raw bytes
btreemap<i32,i64> := uvarint length + (i32, i64) × length

topic_id_partition := uuid (topic_id) + string (topic) + i32 (partition)
remote_log_segment_id := topic_id_partition + uuid

tag = 0 — AddRemoteLogSegmentMetadata:
    remote_log_segment_id
    i64 start_offset
    i64 end_offset
    i64 max_timestamp_ms
    i32 broker_id
    i64 event_timestamp_ms
    i32 segment_size_in_bytes
    opt<bytes> custom_metadata
    u8         state         (0=Started, 1=Finished, 2=DeleteStarted, 3=DeleteFinished)
    btreemap<i32,i64> segment_leader_epochs

tag = 1 — UpdateRemoteLogSegmentMetadata:
    remote_log_segment_id
    i64        event_timestamp_ms
    opt<bytes> custom_metadata
    u8         state
    i32        broker_id

tag = 2 — RemotePartitionDeleteMetadata:
    topic_id_partition
    u8  state   (0=Marked, 1=Started, 2=Finished)
    i64 event_timestamp_ms
    i32 broker_id
```

Strings are UTF-8 (varint length) rather than fixed-width / nul-padded
so that topic names of any length encode unambiguously. Big-endian
ints match Kafka's protocol throughout the rest of Crabka.

Round-trip unit tests cover every variant; a small fuzz-style "encode
× decode is identity for every well-formed event" test runs in
`#[test]` (no harness).

## Partitioning

```rust
pub fn metadata_partition_for(
    tp: &TopicIdPartition,
    partition_count: i32,
) -> i32 {
    use std::hash::Hasher as _;
    let mut h = std::collections::hash_map::DefaultHasher::new();
    tp.topic_id.as_bytes().iter().for_each(|b| h.write_u8(*b));
    h.write_i32(tp.partition);
    let bucket = (h.finish() & i64::MAX as u64) as i64; // strip sign bit
    (bucket % partition_count as i64) as i32
}
```

Hash inputs are `topic_id` + `partition` only — matching
`TopicIdPartition::Hash` — so the bucket survives topic renames.
`DefaultHasher` is stable for the life of the cluster's binary;
upgrades that change the hasher would re-bucket every partition's
metadata, but the wire format is stable across all binaries that
read the same topic. For 48f we accept that constraint; a `siphash`
or `xxh64` fixed across releases is a follow-up.

## `TopicBasedRemoteLogMetadataManager`

```rust
pub struct TopicBasedRemoteLogMetadataManager {
    log: Arc<dyn MetadataEventLog>,
    inner: Arc<Mutex<HashMap<TopicIdPartition, RemoteLogMetadataCache>>>,
    /// Highest applied offset per metadata-topic partition.
    applied: Arc<Mutex<Vec<i64>>>,
    /// Broadcast tick on every newly-applied offset; publish-and-wait
    /// loops park on this until their offset is covered.
    applied_tx: tokio::sync::watch::Sender<()>,
    runtime: tokio::runtime::Handle,
    shutdown: CancellationToken,
}
```

### Startup

```rust
impl TopicBasedRemoteLogMetadataManager {
    pub async fn start(
        log: Arc<dyn MetadataEventLog>,
        runtime: tokio::runtime::Handle,
    ) -> Result<Arc<Self>, RemoteStorageError> {
        let target_hwms = log.high_water_marks().await?;
        let manager = Arc::new(Self { /* … */ });

        // Spawn the consumer-pump task; it owns the subscribe stream.
        let pump_handle = runtime.spawn(consumer_pump(manager.clone(), log.subscribe()));

        // Block until applied[p] >= target_hwms[p] for every p.
        manager.wait_for_offsets(&target_hwms).await?;
        Ok(manager)
    }
}
```

`consumer_pump`:

1. Pulls the next `MetadataEventRecord` from the subscribe stream.
2. Deserializes (drop + warn on decode error; never panic).
3. Routes to the per-partition `RemoteLogMetadataCache`'s
   `add` / `update` / `set_delete_state` (the 48a cache already enforces
   the state machine). State-machine errors on replay are logged at
   `warn` — they mean two writers raced or the topic was corrupted;
   we keep going.
4. Records the new high-water `applied[partition] = offset`, then
   `applied_tx.send_replace(())` to wake waiters.

### Sync RLMM impl

The TBRLMM holds a `runtime::Handle`. Each sync method becomes a
`handle.block_on(self.publish_and_wait(…))`:

```rust
impl RemoteLogMetadataManager for TopicBasedRemoteLogMetadataManager {
    fn add_remote_log_segment_metadata(
        &self,
        metadata: RemoteLogSegmentMetadata,
    ) -> Result<(), RemoteStorageError> {
        // Same starting-state precondition as the in-memory impl —
        // catch errors before paying the round-trip cost.
        if metadata.state() != RemoteLogSegmentState::CopySegmentStarted {
            return Err(RemoteStorageError::InvalidAdd { /* … */ });
        }
        let tp = metadata.remote_log_segment_id().topic_id_partition.clone();
        let part = metadata_partition_for(&tp, self.log.partition_count());
        let bytes = serde::encode_add(&metadata);
        self.runtime.block_on(async {
            let offset = self.log.publish(part, bytes).await?;
            self.wait_for_offsets_pointwise(part, offset).await
        })
    }

    // update_… and put_remote_partition_delete_metadata are the same
    // shape: encode, publish, await applied >= offset.

    fn remote_log_segment_metadata(
        &self,
        tp: &TopicIdPartition,
        leader_epoch: i32,
        offset: i64,
    ) -> Result<Option<RemoteLogSegmentMetadata>, RemoteStorageError> {
        // Pure local read against the cache.
        let g = self.inner.lock().unwrap();
        Ok(g.get(tp).and_then(|c| c.segment_for(leader_epoch, offset)))
    }
    // remaining read methods identical: local cache lookups.
}
```

`block_on` from a context that itself may be running on the same
runtime would deadlock; the broker already calls every RLMM method
through `spawn_blocking`, so the calling worker is on the blocking
thread pool and `block_on` is safe. Documented as a precondition on
the type.

### Read-your-writes wait

The producer's `send` resolves with the assigned `offset` on the
metadata partition. After publish, the call waits on a
`watch::Receiver` cloned from `applied_tx` and re-reads
`applied[part]` until it covers `offset`. With a single metadata
partition per user partition the published event will be the next
one the consumer sees (or close to it), so the wait is bounded;
liveness assumes the consumer pump is up.

A `MetadataLogError::PublishTimeout` propagates out; a stuck consumer
manifests as a `wait_for_offsets` future never completing, which the
broker's `spawn_blocking` worker eventually times out (the broker's
retention tick already runs with a budget).

## Topic provisioning

On `KafkaMetadataEventLog::start`:

1. Connect an `AdminClient` to the configured bootstrap servers.
2. `metadata(&["__remote_log_metadata"])`. If present, read its
   partition count and reuse it. If absent:
   `create_topics(&[CreateTopicSpec { name: "__remote_log_metadata", num_partitions, replication_factor, configs: [("cleanup.policy", "delete"), ("retention.ms", "-1")] }], 30_000)`.
3. If `metadata` reports a smaller partition count than the configured
   target, **do not** auto-create-partitions: log a warning and use
   the existing count. Re-bucketing on partition growth is a separate
   migration not in 48f scope.

Topic name is hard-coded; the partition count, replication factor,
and bootstrap servers come from `TopicRlmmConfig`.

## Config

New broker config fields, threaded through `broker.toml` →
`file_config.rs` → `config.rs` → `Broker::start`:

```toml
[remote_log_metadata]
manager        = "topic"   # "inmemory" (default) | "topic"
bootstrap      = "127.0.0.1:9092"
num_partitions = 50
replication    = 3
```

`manager = "inmemory"` keeps the 48a–48e fixture; `manager = "topic"`
selects TBRLMM. `bootstrap`, `num_partitions`, `replication` are
ignored when `manager = "inmemory"`. CLAUDE.md's no-feature-flag rule
applies to format / behavioral toggles; this is a runtime selection
of two real implementations and is fine.

`config_keys.rs` is **not** touched — these are broker-global
settings, not per-topic.

## Broker wiring

`crates/broker/src/broker.rs` `Broker::start` currently constructs:

```rust
let rlmm: Arc<dyn RemoteLogMetadataManager> =
    Arc::new(InmemoryRemoteLogMetadataManager::new());
```

48f replaces this with a small factory:

```rust
let rlmm: Arc<dyn RemoteLogMetadataManager> = match cfg.remote_log_metadata.manager {
    RlmmKind::InMemory => Arc::new(InmemoryRemoteLogMetadataManager::new()),
    RlmmKind::Topic => {
        let log = KafkaMetadataEventLog::start(&cfg.remote_log_metadata).await?;
        TopicBasedRemoteLogMetadataManager::start(Arc::new(log), Handle::current()).await?
    }
};
```

The `RemoteLogManager`, `RemoteReader`, and 48d/48e handlers continue
to hold this as `Arc<dyn RemoteLogMetadataManager>` — they are
implementation-agnostic and need no edits.

`Broker::start` already runs inside an async context; the TBRLMM
construction can `.await` directly. The RLMM trait stays synchronous;
the trait calls inside the broker tick go through `spawn_blocking`
exactly as today.

## Testing

`crates/remote-storage-topic` ships pure-logic unit tests using the
`InProcessMetadataEventLog` fixture:

- **Wire-format round-trip** for every event variant, including
  optional `custom_metadata` set/unset, multi-epoch segment-leader
  maps, and partitions with non-ASCII topic names.
- **Partitioning** is deterministic and only depends on
  `topic_id` + `partition` (topic name change does not move the
  bucket).
- **Single-broker round trip**: add → finish → query; finish-before-add
  is rejected; delete-segment lifecycle hides then drops.
- **Two TBRLMMs sharing one log** (multi-broker fixture): writes from
  manager A are visible to manager B's queries after each manager's
  consumer pump applies the event; both managers' caches converge.
- **Crash + rehydrate**: write a sequence into the in-process log,
  drop the manager, start a fresh one against the same log,
  `start()` returns only once the cache has the full sequence
  applied; queries against the fresh manager see everything.
- **Publish error**: a log whose `publish` returns `Err` surfaces as
  `RemoteStorageError::Io`; the cache is not mutated.

A separate integration test (in `crates/broker/tests/…` — but only
if it can stay self-contained, otherwise deferred) wires
`Broker::start` with `manager = "topic"` against a single-broker
loopback and exercises one copy-then-fetch cycle. The bar for 48f is
"unit tests prove the manager-level correctness"; broker-integration
coverage of TBRLMM piggybacks on 48b/c/d/e once they're rebuilt to
configurable RLMM.

## Acceptance gates

- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test -p crabka-remote-storage-topic`
- `cargo test --workspace` (no regressions; the existing
  remote-storage / broker tests still pass against the in-memory
  default)
- No CRD drift (operator surface for choosing the RLMM is a 48g
  follow-up).
