# Slice 18: Log compaction (cleanup.policy=compact) — Design

**Status:** Approved 2026-05-16.

**Goal:** Implement Kafka-compatible log compaction for topics configured with `cleanup.policy=compact`. End-to-end: per-topic config, a pure-function compaction engine, a per-broker background cleaner task that runs the engine, a broker integration test, and one JVM acceptance test. First slice of a multi-slice effort; intentionally narrow.

---

## 1. Scope

### In

- Per-topic `cleanup.policy` config key (`"compact"` or `"delete"`, default `"delete"`).
- `LogConfig.cleanup_policy: CleanupPolicy { Delete, Compact }`.
- Pure-function compaction primitives in `crates/log/src/compact.rs`:
  - `build_offset_map(segments) -> HashMap<Vec<u8>, i64>` (key → max absolute offset).
  - `rewrite_segments(segments, map) -> (new_log, new_index, new_timeindex)` writing temp files.
  - `atomic_swap(dir, ...)` finalizing the rewrite.
- `Log::compact(now)` API that runs the above over sealed (non-active) segments.
- `.swap`-orphan recovery in `Log::open`.
- Per-broker background cleaner task in `crates/broker/src/cleaner.rs`. Default interval 30 s. Leader-only. Dispatches through the partition writer actor (single-writer invariant preserved).
- One broker integration test (`crates/broker/tests/compaction.rs`).
- One JVM acceptance test in `jvm_acceptance.rs`.

### Out (deferred)

| Concern | Slice |
|---|---|
| Tombstone retention (`delete.retention.ms`) | 18b |
| Dirty-ratio + `min.compaction.lag.ms` + `max.compaction.lag.ms` | 18c |
| Transactional aborted-record skipping during compaction | 18d |
| `compact,delete` combined policy | 18e |
| KIP-204 cleaner I/O throttle | 18e |
| Reconfigure `__consumer_offsets` / `__transaction_state` to `compact` | follow-on |

### Semantics for slice 18

- **Null-key records:** dropped during compaction. Matches Kafka's `LogCleaner`.
- **Tombstones (null-value, non-null key):** kept naturally as the most-recent value for their key. Survive compaction passes indefinitely until slice 18b adds `delete.retention.ms`.
- **Trigger:** the cleaner ticks every 30 s; on each tick, every `cleanup.policy=compact` partition where this broker is leader runs a compaction pass. `Log::compact` is a no-op when there are <2 sealed segments (nothing to dedup).
- **Offset preservation:** compaction never renumbers. Output records retain their original absolute offsets (gaps are normal for compacted logs).
- **Active segment:** never touched.

---

## 2. Per-topic config

`crates/broker/src/config_keys.rs` gains:

```rust
pub(crate) const CLEANUP_POLICY: &str = "cleanup.policy";

// validate: must be exactly "delete" or "compact"
// (slice 18e adds "compact,delete")
fn validate_cleanup_policy(v: &str) -> Result<(), ConfigError> {
    match v {
        "delete" | "compact" => Ok(()),
        _ => Err(ConfigError::InvalidValue("cleanup.policy")),
    }
}

// apply_to gains one more arm that sets out.cleanup_policy
```

`crates/log/src/config.rs` gains:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CleanupPolicy {
    #[default]
    Delete,
    Compact,
}

pub struct LogConfig {
    // ... existing fields ...
    pub cleanup_policy: CleanupPolicy,
}
```

`Default` for `LogConfig` keeps `cleanup_policy = Delete` (matches Kafka).

The existing per-topic config pipeline (`Arc<RwLock<LogConfig>>` shared between broker and `Log`) carries the new field without further structural changes. `CreateTopics`, `AlterConfigs`, `IncrementalAlterConfigs` all flow through `apply_to`; they pick up the new key for free.

---

## 3. Compaction algorithm

Single sealed-segments-only pass per Log per cleaner tick.

### Step 1 — Build offset map (newest-wins)

Iterate sealed segments oldest → newest. For each record (decoded from each `RecordBatch`):

- If `key.is_none()` → skip (record is dropped in step 2).
- Else `map.insert(key.clone(), record.absolute_offset)` — later writes overwrite.

Result: `HashMap<Vec<u8>, i64>` mapping each key to its newest-seen absolute offset.

### Step 2 — Rewrite

Stream the same sealed segments oldest → newest into a new segment file at temp path `00...basek.log.swap`. For each record:

- Drop if `key.is_none()`.
- Drop if `map.get(key) != Some(this_record_offset)` (newer instance exists).
- Else: emit into the active output `RecordBatch`.

`RecordBatch` boundaries may be repacked freely (Kafka does this). The output's per-record offsets retain their **absolute** values from the source — this means the output batch's `base_offset` and `last_offset_delta` cover the surviving record range, and there can be gaps inside the batch. Kafka's RecordBatch v2 supports this (record offset deltas may skip values).

A fresh `.index.swap` and `.timeindex.swap` are written alongside, at the same `index.interval.bytes` cadence as the writer normally produces.

### Step 3 — Atomic swap

1. `fsync` all three `.swap` files.
2. Delete the originals: `.log` / `.index` / `.timeindex` for every consumed segment's `base_offset`.
3. Rename `.swap` → final names (`00...basek.log`, `.index`, `.timeindex`) — the new segment's `base_offset` equals the **lowest** input segment's `base_offset`.
4. `fsync` the directory.
5. Update `Log::segments` in memory: replace the consumed sealed-segment list with the single new sealed `Segment`.

### Recovery on `Log::open`

Two crash mid-points:

- **Originals + `.swap` both present** (crashed in step 2 or earlier): `.swap` files are partial — delete them; originals are authoritative.
- **Originals deleted, `.swap` present** (crashed in step 3): finish the rename. The `.swap` files are complete (we fsynced in step 1) so promoting them is safe.

Detection logic lives in `crates/log/src/recovery.rs`. Algorithm: on open, for every `.log.swap` found:
- If matching plain `.log` exists at the same base offset → delete the `.swap` triple.
- Else → rename the `.swap` triple to final names.

---

## 4. Cleaner task

New `crates/broker/src/cleaner.rs`, modeled after the existing `leader_rebalance.rs` ticker:

```rust
pub(crate) struct Cleaner {
    partitions: Arc<PartitionsRegistry>,
    interval: Duration,                          // default 30s
    shutdown: tokio::sync::watch::Receiver<bool>,
}

impl Cleaner {
    pub async fn run(self) {
        let mut ticker = tokio::time::interval(self.interval);
        loop {
            tokio::select! {
                _ = ticker.tick() => self.tick_all().await,
                _ = self.shutdown.changed() => break,
            }
        }
    }

    async fn tick_all(&self) {
        for partition in self.partitions.snapshot() {
            if !partition.is_leader() { continue; }
            if partition.log_config().cleanup_policy != CleanupPolicy::Compact { continue; }
            partition.request_compact().await;  // sends WriterMessage::Compact
        }
    }
}
```

`PartitionWriter` (the existing single-writer actor) gains a new `WriterMessage::Compact { reply }` variant. The actor handles it by calling `self.log.compact(now)` and replying. This preserves the single-writer invariant: compaction and `append` are serialized through the same actor.

`Broker::start` spawns the cleaner alongside `isr_maintenance` and `leader_rebalance`. Shutdown signal piggybacks on the existing per-broker watch.

### Leader-only rationale

Followers replicate from the leader. After a leader-side compaction, the follower's log diverges (it still has the pre-compaction superseded records). This is fine in slice 18 because:

- Compaction never removes records from the active segment.
- Compaction never renumbers offsets.
- Follower `Fetch` continues to receive new appends normally.
- On leader change, the new leader may have a different compaction state, but since compaction is monotonic ("newest wins") the union of two leaders' compacted views agrees on the latest value for each key.

A full reconciliation story (followers compact on demand, or copy compaction state via a new replication mode) lives in a later slice if/when divergence matters. For slice 18, the only consumer is the leader's read path, which serves compacted data; the follower's local logs grow until the broker becomes leader and starts compacting itself.

---

## 5. Testing

### Pure-function unit tests (`crates/log/src/compact.rs`)

- `build_offset_map_keeps_newest_offset_per_key`
- `build_offset_map_drops_null_key_records`
- `rewrite_drops_superseded_records`
- `rewrite_preserves_absolute_offsets`
- `rewrite_keeps_tombstone_as_latest_value`
- `rewrite_repacks_batch_boundaries`

### `Log`-level integration tests (`crates/log/src/log.rs`)

- `compact_dedupes_sealed_segments_keeps_active_intact`
- `compact_is_idempotent`
- `compact_atomic_swap_completes_partial_rename_on_open` (write `.swap` triples + missing originals, call `Log::open`, assert promoted)
- `compact_atomic_swap_discards_orphan_swap_on_open` (write `.swap` triples + intact originals, call `Log::open`, assert `.swap` discarded)

### Broker integration test (`crates/broker/tests/compaction.rs`, new)

`compaction_dedupes_via_native_client`:

1. Spawn a single broker (no SASL; reuse the simplest helper).
2. Create topic `compacted` with `cleanup.policy=compact` and `segment.bytes=1024` (force frequent rolls).
3. Produce 30 records: 10 each under keys `k1`, `k2`, `k3`, with monotonically increasing values.
4. Sleep > cleaner interval (override interval to 1 s for the test via a test-only config hook).
5. Force segment roll on the active segment if needed.
6. Sleep another tick.
7. Fetch from offset 0 to end: assert exactly 3 records, one per key, with the latest value.

### JVM acceptance test (`crates/broker/tests/jvm_acceptance.rs`, modified)

`jvm_kafka_console_consumer_sees_compacted_topic_end_to_end`:

1. Spin 3-broker cluster (existing fixture).
2. `kafka-topics --create --topic compacted --config cleanup.policy=compact --config segment.bytes=1024`.
3. `kafka-console-producer --property parse.key=true --property key.separator=:` piping `k1:v1\nk1:v2\nk2:v3\nk1:v4\nk3:v5\n`.
4. Sleep ~5 s for cleaner + segment roll.
5. `kafka-console-consumer --from-beginning --timeout-ms 5000`: assert output contains `v4`, `v3`, `v5` (one per key, the latest value), and that `v1`, `v2` are absent. Order matches the latest-write order of distinct keys.

---

## 6. File structure & task layout

```
crates/log/src/
├── compact.rs                                      # NEW — primitives + tests
├── config.rs                                       # MODIFIED — CleanupPolicy enum
├── log.rs                                          # MODIFIED — Log::compact() + tests
├── recovery.rs                                     # MODIFIED — .swap orphan detection
└── lib.rs                                          # MODIFIED — pub use CleanupPolicy
crates/broker/src/
├── cleaner.rs                                      # NEW — background ticker
├── broker.rs                                       # MODIFIED — spawn Cleaner
├── config_keys.rs                                  # MODIFIED — cleanup.policy validate + apply
└── partition.rs                                    # MODIFIED — WriterMessage::Compact
crates/broker/tests/
├── compaction.rs                                   # NEW — native-client test
└── jvm_acceptance.rs                               # MODIFIED — 1 new JVM test
```

Implementation plan target: **~10–12 tasks across 4 batches**.

- **Batch 1 (parallel):** T1 config wiring (config_keys.rs + log/config.rs); T2 pure-function primitives in compact.rs.
- **Batch 2 (parallel):** T3 `Log::compact` integration in log.rs; T4 `.swap` recovery in recovery.rs.
- **Batch 3 (sequential):** T5 `PartitionWriter::Compact` message + `Cleaner` task + broker spawn.
- **Batch 4 (parallel):** T6 broker integration test; T7 JVM acceptance.
