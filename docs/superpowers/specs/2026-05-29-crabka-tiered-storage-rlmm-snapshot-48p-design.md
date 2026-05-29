# Crabka tiered storage 48p — RLMM snapshot / fast-bootstrap (design)

**Date:** 2026-05-29
**Status:** Slice design. Depends on 48o (assign + seek). Closes a 48f
follow-up. Part of the KIP-405 umbrella
(`docs/superpowers/specs/2026-05-25-crabka-tiered-storage-roadmap-design.md`).

## Goal

Stop replaying the entire `__remote_log_metadata` topic from offset 0 on
every broker restart. Persist the in-memory metadata cache plus the
consumed offsets to local disk, and on restart load the snapshot and
resume the consumer from where it left off. Startup cost becomes bounded
by the events written since the last snapshot, not by total topic size.

## The gap

`TopicBasedRemoteLogMetadataManager::start`
(`crates/remote-storage-topic/src/manager.rs:67`) builds a fresh
`InmemoryRemoteLogMetadataManager` and replays from offset 0:

```rust
let inner = Arc::new(InmemoryRemoteLogMetadataManager::new());
let stream = log.subscribe();              // from 0 (48o: assignment @ 0)
let pump = runtime.spawn(pump_loop(stream, inner.clone(), ...));
manager.wait_for_targets(&target_hwms).await;   // catch up to HWM
```

Replay is O(all segments ever recorded). This mirrors Kafka's model
*before* its `RemoteLogMetadataSnapshotFile`; 48p adds the snapshot.

## Approach (local on-disk snapshot, Kafka's model)

### Snapshot contents & location

One snapshot per broker under the data dir, e.g.
`<data_dir>/remote-log-metadata/snapshot`. Contents:

- format version (u16);
- per-metadata-partition committed offset: `Vec<(partition: i32, offset:
  i64)>` — the highest offset applied into the cache for each partition;
- the full cache: every partition's `RemoteLogSegmentMetadata` (all
  states, terminal included) and `RemotePartitionDeleteMetadata`.

Reuse the `serde.rs` event codec for the per-segment encoding; wrap it in
a snapshot envelope (version + offsets + length-prefixed entries). Write
to a temp file and atomically rename, so a crash mid-write never yields a
torn snapshot.

### Cache export / import

`InmemoryRemoteLogMetadataManager` (`crates/remote-storage/src/inmemory.rs`)
gains:

```rust
/// Dump every partition's segment + partition-delete metadata for
/// snapshotting.
pub fn export(&self) -> RlmmCacheDump;

/// Seed the cache from a dump. Bypasses transition validation: dumped
/// states are already the result of valid transitions, so re-applying
/// them through add/update would wrongly reject terminal states.
pub fn import(&self, dump: RlmmCacheDump);
```

`import` populates the same epoch-indexed `RemoteLogMetadataCache` the live
path uses, so reads after load behave identically.

### Save cadence

A snapshotter task in `manager.rs`:

- writes on a configurable interval (`remote.log.metadata.snapshot.interval`,
  default e.g. 60s) when the cache has advanced since the last snapshot;
- writes once on graceful shutdown (hook into the existing
  `CancellationToken` shutdown path);
- captures the committed offsets from the pump's `applied` vector
  (`manager.rs`) together with the cache `export`, under a lock so the
  offset/cache pair is consistent.

### Resume on start

`start` becomes:

1. Load the snapshot if present; on success `inner.import(dump)` and take
   its per-partition committed offsets; on absence/corruption, fall back
   to empty cache + offsets all `-1` (full replay — same as today, so a
   bad snapshot is never fatal).
2. Build the 48o assignment as `PartitionStart { partition, start_offset:
   committed + 1 }` for each partition.
3. `wait_for_targets(&target_hwms)` against current HWMs as before — the
   pump now only needs to apply the delta from `committed + 1` to HWM.

## Files

- `crates/remote-storage-topic/src/snapshot.rs` (new) — envelope encode /
  decode, atomic write, load.
- `crates/remote-storage-topic/src/serde.rs` — reuse / extend for the
  envelope.
- `crates/remote-storage-topic/src/manager.rs` — load+resume in `start`,
  snapshotter task, shutdown flush.
- `crates/remote-storage/src/inmemory.rs` — `export` / `import` +
  `RlmmCacheDump`.
- Config: snapshot interval + directory (`KafkaRlmmConfig`).
- `crates/broker/src/broker.rs` — pass the data dir into the RLMM
  bootstrap (`bootstrap_topic_rlmm`).

## Testing

- Snapshot round-trip: `export` → encode → decode → `import` reproduces a
  byte-identical cache (compare `list_remote_log_segments` +
  partition-delete metadata across all partitions).
- Atomic write: a truncated/garbage snapshot file → `start` falls back to
  full replay, no panic.
- Resume: seed a topic, snapshot at offset N, restart → assert the 48o
  assignment starts at `N+1` per partition (no replay from 0) and the
  post-load cache equals the pre-restart cache.
- Shutdown flush writes a snapshot covering all applied events.

## Non-goals

- No `__remote_log_metadata` topic compaction (the rejected alternative).
- No cross-broker snapshot sharing — each broker snapshots its own cache.

## Dependencies & sequencing

Requires 48o (resume needs `PartitionStart.start_offset`). Sequenced after
48o; before 48q (48q reuses the resume-offset plumbing and both touch
`manager.rs`, so they are not parallel).

## Acceptance gates

- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test -p crabka-remote-storage-topic -p crabka-remote-storage`
- `cargo test --workspace` (no regressions)
- No CRD drift.
