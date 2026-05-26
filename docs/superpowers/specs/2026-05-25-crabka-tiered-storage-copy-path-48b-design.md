# Crabka tiered storage 48b — Copy path (design)

**Date:** 2026-05-25
**Status:** Slice design. Follows slice 48a (foundation crate
`crabka-remote-storage`). Part of the KIP-405 umbrella
(`docs/superpowers/specs/2026-05-25-crabka-tiered-storage-roadmap-design.md`).

## Goal

Wire the broker's first tiered-storage behavior: a background
`RemoteLogManager` task that, on the partition's leader, copies sealed log
segments of `remote.storage.enable=true` topics to the remote tier (via a
`RemoteStorageManager`) and records them in a `RemoteLogMetadataManager`.

This is the **copy path only**. Local-retention deletion of copied
segments (`local.retention.*` + `local-log-start-offset`) and the remote
read path on `Fetch` are deferred to 48c / 48d.

## Why copy-without-eviction is a complete slice

The copy subsystem is a self-contained vertical: a segment is durably
offloaded and tracked in remote metadata (`CopySegmentStarted` →
`CopySegmentFinished`), observable and testable on its own. Deciding
*when* a local copy is safe to delete is a separate, additive concern
(48c). No dead code: the per-topic `remote.storage.enable` Kafka config
and the broker-global enablement are both consumed by the task this slice
adds.

## Config (consumed in this slice)

- **Per-topic** `remote.storage.enable` (Kafka-standard topic config) →
  new `LogConfig.remote_storage_enable: bool` (default `false`, Kafka's
  default). Threaded through `config_keys::{validate_topic_config,
  is_recognized, apply_to_log_config}`.
- **Broker-global** `BrokerConfig.remote_log_storage_dir: Option<PathBuf>`.
  `Some(dir)` enables tiered storage and roots the `LocalTieredStorage`;
  `None` (default) leaves it off. Collapses Kafka's
  `remote.log.storage.system.enable` + RSM dir config into one knob —
  greenfield-OK. TOML: `[remote_storage] storage_dir = "..."`.

`LocalTieredStorage` is the only RSM and `InmemoryRemoteLogMetadataManager`
the only RLMM in 48b (a topic-backed prod RLMM + object-store RSM are
later slices). Both are constructed once at `Broker::start` when
`remote_log_storage_dir` is `Some`, shared across ticks via `Arc`.

## Log-crate surface (new)

`crabka_log` learns to describe its sealed segments without depending on
`crabka-remote-storage` (layering preserved):

```rust
pub struct SegmentExport {
    pub base_offset: i64,
    pub last_offset: i64,
    pub max_timestamp: i64,
    pub size_bytes: u64,
    pub log_path: PathBuf,
    pub offset_index_path: PathBuf,
    pub time_index_path: PathBuf,
    pub transaction_index_path: Option<PathBuf>, // present iff the .txnindex exists
    pub leader_epochs: Vec<(i32, i64)>,          // (epoch, start offset) covering [base,last]
}

impl Log { pub fn tierable_segments(&self) -> Vec<SegmentExport>; }
```

`leader_epochs` is computed from the per-partition leader-epoch checkpoint:
every epoch whose coverage `[start_e, start_{e+1})` intersects
`[base_offset, last_offset]`, with the recorded offset clamped to
`max(start_e, base_offset)`. May be empty when no epochs were ever
recorded (e.g. batches with `partition_leader_epoch < 0`); the broker then
falls back to the partition's current leader epoch.

Crabka writes no producer-id snapshot files, so
`LogSegmentData.producer_snapshot_index` becomes `Option<PathBuf>` (set
`None`); `LocalTieredStorage` skips it when absent. (Breaking change to the
48a crate — greenfield-OK, no external users.)

## RemoteLogManager (new broker module)

`crates/broker/src/remote_log_manager.rs`, modeled on `cleaner.rs`:

- `run(partitions, node_id, rsm, rlmm, cfg, shutdown)` — interval ticker;
  each tick snapshots the partition registry and calls
  `copy_partition` for every partition where this broker is leader and
  `log.config_snapshot().remote_storage_enable` is true.
- `copy_partition(partition, node_id, topic_id, leader_epoch, rsm, rlmm)`
  — the testable orchestration core:
  1. Snapshot `log.tierable_segments()` (under the log lock, then drop it).
  2. Build the set of already-known base offsets from
     `rlmm.list_remote_log_segments(tp)` (any state) and skip those.
  3. For each remaining sealed segment, in offset order:
     - `RemoteLogSegmentId { tp, Uuid::new_v4() }`.
     - `RemoteLogSegmentMetadata` (state `CopySegmentStarted`, offsets,
       size, max-ts, broker id, event ts = now, `segment_leader_epochs`
       from the export, falling back to `[(leader_epoch.max(0), base)]`
       when empty).
     - `rlmm.add(...)` (records Started).
     - `rsm.copy_log_segment_data(&md, &LogSegmentData{…})` inside
       `spawn_blocking` (RSM is blocking).
     - On success: `rlmm.update(CopySegmentFinished)`.
     - On failure: `rsm.delete_log_segment_data` (idempotent) +
       `rlmm.update(DeleteSegmentStarted)` + `update(DeleteSegmentFinished)`
       so the metadata is dropped and the segment retries next tick. Log a
       warning.

`topic_id` (Uuid) comes from `controller.current_image().topic(name)`;
the partition's current leader epoch from
`partition.current_leader_epoch`.

## Broker startup wiring

In `Broker::start`, when `config.remote_log_storage_dir` is `Some(dir)`:
construct `Arc<LocalTieredStorage>` + `Arc<InmemoryRemoteLogMetadataManager>`
and `tokio::spawn(remote_log_manager::run(...))` with a child shutdown
token — same pattern as the cleaner spawn.

## Testing

- **log crate:** `tierable_segments` excludes the active segment, returns
  correct paths + offsets + sizes, computes leader-epoch ranges across
  single/multi-epoch logs, and flags the `.txnindex` only when present.
- **config_keys:** `remote.storage.enable` validate (true/false/junk),
  `is_recognized`, `apply_to_log_config` round-trip.
- **remote_log_manager (core):** drive `copy_partition` against a **real**
  `Log` (rolled to several sealed segments) + `LocalTieredStorage` +
  `InmemoryRemoteLogMetadataManager`; assert each sealed segment is copied
  (data + indexes present in the remote store) and recorded
  `CopySegmentFinished`; re-running is idempotent (no duplicate copies);
  the active segment is never copied; a disabled topic copies nothing.
- **remote-storage:** update `LocalTieredStorage` tests for the optional
  producer-snapshot index.

## Out of scope (48c+)

Local-retention deletion + `local-log-start-offset`; remote read path on
`Fetch` / `ListOffsets`; remote-retention + partition delete;
topic-backed RLMM; object-store RSM; operator CRD surface; per-segment
copy throttling / parallelism (one segment at a time per tick).

## Acceptance gates

- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test -p crabka-log -p crabka-remote-storage -p crabka-broker`
- `cargo build --workspace`
- No CRD drift.
</content>
