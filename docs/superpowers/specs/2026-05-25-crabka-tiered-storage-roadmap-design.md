# Crabka tiered storage (KIP-405) — roadmap design

**Date:** 2026-05-25
**Status:** Roadmap design (umbrella); first sub-slice (48a) detailed below.
**Roadmap slot:** Operator roadmap Phase 8, slice 48 — "**Crabka core:**
Tiered storage (KIP-405). Large; likely splits into sub-slices when
planned. An operator-surfacing follow-up slice lands after the core work."

## Why this exists

[KIP-405](https://cwiki.apache.org/confluence/display/KAFKA/KIP-405%3A+Kafka+Tiered+Storage)
lets a broker offload older, sealed log segments to a remote object store
(S3, HDFS, GCS, …) and serve reads back from it, so a partition's
effective retention is decoupled from local disk capacity. It is one of
the largest single features in modern Kafka and cannot land in one PR.

This document is the umbrella: it defines the subsystems, breaks them
into individually-deliverable sub-slices, and details the **first**
sub-slice (48a) for immediate implementation. Each later sub-slice gets
its own per-slice design at planning time — only the scope envelope is
legislated here.

## What Kafka actually ships

KIP-405 is implemented across a few well-separated layers in Apache
Kafka. Crabka mirrors the same boundaries:

| Kafka module / class | Role |
|----------------------|------|
| `storage-api`: `RemoteStorageManager` (RSM) | Plugin SPI for copy / fetch / delete of segment data + indexes to/from the remote tier. |
| `storage-api`: `RemoteLogMetadataManager` (RLMM) | Plugin SPI for persisting *metadata* about remote segments (offset ranges, leader epochs, lifecycle state). |
| `storage-api`: `RemoteLogSegmentMetadata`, `RemoteLogSegmentId`, `RemoteLogSegmentState`, `RemotePartitionDeleteMetadata`, `LogSegmentData`, `RemoteStorageManager.IndexType` | The data model exchanged across both SPIs. |
| `LocalTieredStorage` (test fixture) | A filesystem-backed `RemoteStorageManager` Kafka uses to test the whole stack without a real object store. |
| `InmemoryRemoteLogMetadataManager` (test fixture) | A `HashMap`-backed RLMM, again for testing. Production default is `TopicBasedRemoteLogMetadataManager` (metadata in an internal `__remote_log_metadata` topic). |
| `core`: `RemoteLogManager` (RLM) | The broker orchestrator: a per-leader-partition task that copies eligible segments via RSM, records metadata via RLMM, enforces local- vs remote-retention, and serves remote reads on the fetch path. |

Crabka's existing storage layer that this builds on:

- `crabka_log::Log` — per-partition segmented log (`crates/log/src/log.rs`).
  `Log::tick()` already computes time/size retention; `Log::read()` walks
  sealed-then-active segments. `Segment` exposes `.log` / `.index` /
  `.timeindex` plus a per-partition `.leader-epoch-checkpoint` and per-segment
  `.txnindex`.
- Broker partition registry: `DashMap<(String,i32), Arc<Partition>>`
  (`crates/broker/src/broker.rs`); each `Partition` owns
  `Arc<Mutex<Log>>` (`crates/broker/src/partition.rs`).
- Fetch read site: `crates/broker/src/handlers/fetch.rs` →
  `log.read(fetch_offset, …)`.
- `ListOffsets`: `crates/broker/src/handlers/list_offsets.rs` returns
  `log.log_start_offset()` / `log.log_end_offset()`.
- Topic-config → `LogConfig` plumbing:
  `crates/broker/src/config_keys.rs::apply_to_log_config` pushed via the
  reconcile loop in `replicator_supervisor.rs`.

No tiered-storage code exists today (greenfield).

## Subsystem inventory

| Subsystem | What Crabka has to add |
|-----------|------------------------|
| **A. Storage SPI + data model** | `RemoteStorageManager` / `RemoteLogMetadataManager` traits, the metadata types, and the lifecycle state machines. Plus the two reference implementations (`LocalTieredStorage`, `InmemoryRemoteLogMetadataManager`) the rest of the stack is tested against. |
| **B. Copy path** | A per-leader-partition `RemoteLogManager` task that picks sealed segments below the active segment / recovery point, assembles `LogSegmentData`, calls `RSM::copy_log_segment_data`, and records `CopySegmentStarted` → `CopySegmentFinished` via RLMM. |
| **C. Local retention split** | `local.retention.ms` / `local.retention.bytes`: once a segment is safely in the remote tier, local copies become eligible for deletion independent of the (longer) total retention. Introduces `local-log-start-offset` distinct from `log-start-offset`. |
| **D. Remote read path** | Fetch below `local-log-start-offset` reads from the remote tier via RSM, using the remote offset/time indexes to position. `ListOffsets` EARLIEST/by-timestamp consults remote metadata. |
| **E. Remote retention + partition delete** | Total-retention eviction of remote segments (`DeleteSegmentStarted`/`Finished`); `RemotePartitionDeleteMetadata` lifecycle on topic delete. Leader-epoch-cache-driven eligibility. |
| **F. Config + topic surface** | Broker `remote.log.storage.system.enable`; per-topic `remote.storage.enable`, `local.retention.{ms,bytes}`; RSM/RLMM selection. Wire through `config_keys` / `LogConfig`. |
| **G. Operator surface** | `Kafka.spec` tiered-storage enablement + `KafkaTopic` `remote.storage.enable`; mount object-store credentials. (Operator roadmap follow-up; pairs after the core read/write path lands.) |

## Sub-slice plan

| Slice | Layer | Title | Notes |
|------:|-------|-------|-------|
| **48a** | A | **Storage SPI + data model + reference impls** | **This PR.** New `crates/remote-storage` crate: the two SPI traits, the full metadata model + lifecycle state machines, `LocalTieredStorage` (filesystem RSM) and `InmemoryRemoteLogMetadataManager` (incl. the epoch-indexed `RemoteLogMetadataCache`). Pure logic; no broker wiring, no config. Complete + unit-tested on its own. |
| 48b | F | Config + `LogConfig` tiered fields | `remote.storage.enable` (per-topic), `local.retention.ms`/`local.retention.bytes`, broker-global `remote.log.storage.system.enable`. Threads through `config_keys` + `LogConfig`. RSM/RLMM instances constructed at broker start (LocalTieredStorage default until a real object-store RSM lands). |
| 48c | B + C | Copy path + local retention | `RemoteLogManager` per-leader task copies eligible sealed segments; `Log` gains `local-log-start-offset`; local retention deletes copied segments; `Log::tick` learns the local/remote split. |
| 48d | D | Remote read path | Fetch below `local-log-start-offset` serves from remote via RSM + remote indexes; `ListOffsets` EARLIEST + by-timestamp consult RLMM. |
| 48e | E | Remote retention + partition delete | Total-retention eviction of remote segments; `RemotePartitionDeleteMetadata` lifecycle on `DeleteTopics`. |
| 48f | A (prod RLMM) | `TopicBasedRemoteLogMetadataManager` | Production RLMM backed by an internal `__remote_log_metadata` topic, replacing the in-memory default. (Optional / later — in-memory + a future object-store-native RLMM may suffice first.) |
| 48g | G | Operator surface | `Kafka` + `KafkaTopic` CRD fields; credential/secret mounting. Operator-roadmap follow-up. |

Sequencing: 48a → 48b → 48c → 48d → 48e, with 48f/48g as follow-ups.
48a is **standalone and useful**: the reference RSM + RLMM are exactly
what every later slice's tests run against, mirroring how Kafka's own
`LocalTieredStorage` / `InmemoryRemoteLogMetadataManager` underpin its
tiered-storage test suite.

## First sub-slice — 48a

### Goal

Land a `crates/remote-storage` workspace member (`crabka-remote-storage`)
that provides Kafka's `storage-api` surface, faithfully shaped, plus the
two reference implementations — all pure logic with no dependency on the
broker, the async runtime, or any config. Every type is a complete,
working, unit-tested component; only the broker *wiring* is deferred to
48b+.

### Deliverables

`crates/remote-storage/src/`:

- `error.rs` — `RemoteStorageError` (thiserror).
- `metadata.rs` — the data model:
  - `TopicIdPartition { topic_id: Uuid, topic: String, partition: i32 }`
    (equality/hash by `topic_id` + `partition`, matching Kafka).
  - `RemoteLogSegmentId { topic_id_partition, id: Uuid }`.
  - `RemoteLogSegmentState` { `CopySegmentStarted`, `CopySegmentFinished`,
    `DeleteSegmentStarted`, `DeleteSegmentFinished` } + `is_valid_transition`.
  - `RemoteLogSegmentMetadata` (start/end offset, broker id, max-timestamp,
    event-timestamp, size, `segment_leader_epochs: BTreeMap<i32,i64>`,
    `custom_metadata`, state) + constructor validation + `with_update`.
  - `RemoteLogSegmentMetadataUpdate`.
  - `RemotePartitionDeleteState` { `DeletePartitionMarked`,
    `DeletePartitionStarted`, `DeletePartitionFinished` } + transitions.
  - `RemotePartitionDeleteMetadata`.
  - `CustomMetadata(Vec<u8>)`.
- `storage_manager.rs` — `RemoteStorageManager` trait + `LogSegmentData`
  + `IndexType` { Offset, Timestamp, ProducerSnapshot, LeaderEpoch, Transaction }.
- `metadata_manager.rs` — `RemoteLogMetadataManager` trait.
- `cache.rs` — `RemoteLogMetadataCache`: per-partition state machine +
  per-epoch navigable offset→segment index; the core query logic
  (`remote_log_segment_metadata(epoch, offset)`,
  `highest_offset_for_epoch`).
- `inmemory.rs` — `InmemoryRemoteLogMetadataManager` (Mutex<HashMap<tp, cache>>).
- `local.rs` — `LocalTieredStorage` (filesystem RSM: copy/fetch/fetch_index/delete).
- `lib.rs` — module wiring + re-exports + crate docs.

### SPI shapes (sync, matching Kafka's blocking RSM/RLMM)

```rust
pub trait RemoteStorageManager {
    fn copy_log_segment_data(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        data: &LogSegmentData,
    ) -> Result<Option<CustomMetadata>, RemoteStorageError>;

    /// `start_position` inclusive; `end_position` inclusive when `Some`,
    /// else read to end of segment.
    fn fetch_log_segment(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        start_position: u32,
        end_position: Option<u32>,
    ) -> Result<Vec<u8>, RemoteStorageError>;

    fn fetch_index(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        index_type: IndexType,
    ) -> Result<Vec<u8>, RemoteStorageError>;

    fn delete_log_segment_data(
        &self,
        metadata: &RemoteLogSegmentMetadata,
    ) -> Result<(), RemoteStorageError>;
}
```

The broker wraps these in `spawn_blocking` in 48c/48d; keeping the SPI
synchronous matches Kafka (RSM/RLMM run on the RLM thread pool) and keeps
48a free of the async runtime.

### Lifecycle invariants enforced in 48a

- `add_remote_log_segment_metadata` requires state `CopySegmentStarted`.
- `update_remote_log_segment_metadata` requires the segment to exist and
  the transition to be valid (`CopySegmentStarted` →
  {`CopySegmentFinished`, `DeleteSegmentStarted`}; `CopySegmentFinished` →
  `DeleteSegmentStarted`; `DeleteSegmentStarted` → `DeleteSegmentFinished`).
  A no-op same-state update and any other jump are rejected.
- Only `CopySegmentFinished` segments are visible to offset/epoch queries.
- `remote_log_segment_metadata(tp, epoch, offset)` returns the finished
  segment covering `offset` within `epoch`'s contributed range.
- `highest_offset_for_epoch` is the max `end_offset` of finished segments
  carrying that epoch.

### Explicit non-goals for 48a

- No broker integration (no `RemoteLogManager`, no fetch/retention wiring).
- No config fields (`remote.storage.enable`, `local.retention.*`) — added
  in 48b where they are actually consumed, per the no-dead-config rule.
- No `TopicBasedRemoteLogMetadataManager` (48f).
- No real object-store RSM (S3/etc.) — `LocalTieredStorage` is the only
  RSM impl; production RSMs are later plugins behind the same trait.
- No serialization/wire format for remote metadata — 48a's metadata lives
  in process memory; on-disk/topic encoding is 48f's concern.

### Testing (48a)

Pure-logic unit tests, no cluster:

- `metadata`: state-transition matrix (valid + rejected); constructor
  validation (empty leader-epoch map rejected; `start_offset` consistency);
  `with_update` applies state + event timestamp + custom metadata.
- `cache` / `inmemory`: add→finish→query happy path; offset lookup across
  multiple segments + multiple epochs; `highest_offset_for_epoch`;
  unfinished segments invisible; delete lifecycle; add-with-wrong-state and
  invalid-transition errors; list ordering by start offset.
- `local`: round-trip copy → fetch full + partial byte ranges → fetch each
  index type → delete (idempotent); fetch-after-delete errors; isolation by
  `RemoteLogSegmentId`.

### Acceptance gates

- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test -p crabka-remote-storage`
- `cargo test --workspace` (no regressions)
- No CRD drift (no CRDs touched).
</content>
</invoke>
