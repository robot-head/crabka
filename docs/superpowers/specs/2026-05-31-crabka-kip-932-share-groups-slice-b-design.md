# KIP-932 Share Groups — Slice B (Share Coordinator / Persister) Design

**Date:** 2026-05-31
**Status:** Approved (design). Builds on Slice A (membership).
**KIP:** [KIP-932](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka) (+ KIP-1226 lag persistence: `DeliveryCompleteCount`).
**Target:** Apache Kafka 4.3.0.
**Slice A spec:** `docs/superpowers/specs/2026-05-30-crabka-kip-932-share-groups-design.md`

## Goal

A durable **share coordinator (persister)** that stores per-share-partition delivery
state in a new internal topic `__share_group_state` and serves the five
inter-broker persister RPCs (83–87). It actively snapshots and **prunes** the
state log, and the Slice-A membership coordinator **wires its lifecycle**:
`InitializeShareGroupState` when a share group first needs a (topic, partition),
`DeleteShareGroupState` on removal — tracked via `ShareGroupStatePartitionMetadata`
in `__consumer_offsets`. No share-partition leader and no `ShareFetch`/`ShareAcknowledge`
(those are Slice C); the persister is exercised directly via its RPCs and the
membership lifecycle.

## Background: the two coordinators

KIP-932 has two distinct server components (see Slice A background):

- **Share *group* coordinator** (membership) — built in Slice A on the unified
  `GroupCoordinator`; persists membership to `__consumer_offsets`.
- **Share *coordinator* (persister)** — *this slice*. A separate per-broker
  service, sharded across `__share_group_state` partitions, persisting
  per-`(group, topicId, partition)` delivery state. Structurally it mirrors the
  **transaction coordinator** (`crates/broker/src/txn/`), which is the template.

## What already exists (reused)

- Persister RPC wire codecs (apiKeys 83–87) — generated. Confirmed shapes:
  - **InitializeShareGroupState(83, v0):** `GroupId, Topics[]{TopicId, Partitions[]{Partition, StateEpoch, StartOffset}}`.
  - **ReadShareGroupState(84, v0):** `GroupId, Topics[]{TopicId, Partitions[]{Partition, LeaderEpoch}}`.
  - **WriteShareGroupState(85, v0–1):** `GroupId, Topics[]{TopicId, Partitions[]{Partition, StateEpoch, LeaderEpoch, StartOffset, DeliveryCompleteCount, StateBatches[]{FirstOffset, LastOffset, DeliveryState:int8, DeliveryCount:int16}}}`.
  - **DeleteShareGroupState(86, v0):** `GroupId, Topics[]{TopicId, Partitions[]{Partition}}`.
  - **ReadShareGroupStateSummary(87, v0–1):** request `{…Partition, LeaderEpoch}`; response `Results[]{TopicId, Partitions[]{Partition, ErrorCode, ErrorMessage, StateEpoch, LeaderEpoch, StartOffset, DeliveryCompleteCount}}`.
- `TxnCoordinator` (`crates/broker/src/txn/{coordinator,bootstrap,partitioner}.rs`) — the structural template: per-broker `DashMap` state, 50-partition internal topic via `ensure_topic` + round-robin replica assignment, `recover()` replaying leader partitions, FindCoordinator hashing key→partition→leader.
- `FindCoordinator` handler (`handlers/find_coordinator.rs`): `KEY_TYPE_GROUP=0`, `KEY_TYPE_TRANSACTION=1`; the TRANSACTION arm does `ensure_topic` → `partition_for_tid` → metadata partition leader → broker address. Template for SHARE.
- `FENCED_STATE_EPOCH(124)` error code (added in Slice A). `InterBrokerClient` (`network/client.rs`) for cross-broker calls. DeleteRecords / log-start-offset advance for pruning (KIP-204, implemented).
- Slice-A share group coordinator (`coordinator/unified/share/`) — gets the lifecycle hook.

## Non-goals (Slice B)

- No share-partition leader; no `ShareFetch(78)`/`ShareAcknowledge(79)`; no acquisition locks / delivery counts driven by real consumption (the persister stores whatever state batches it is handed). Those are Slice C.
- No admin offset RPCs (90–92) — Slice D.
- No native share consumer — Slice E.

---

## Components

### 1. Module: `crates/broker/src/share_coordinator/` (sibling to `txn/`)

```
share_coordinator/
├── mod.rs
├── bootstrap.rs        // ensure __share_group_state topic (50 partitions)
├── partitioner.rs      // hash(group:topicId:partition) % NUM_PARTITIONS
├── coordinator.rs      // ShareCoordinator (per-broker state machine)
├── state.rs            // SharePartitionState + StateBatch
├── persistence.rs      // ShareSnapshot/ShareUpdate key+value codecs
├── pruning.rs          // redundant-offset computation + log-start advance
└── handlers/
    ├── mod.rs
    ├── initialize_share_group_state.rs   // 83
    ├── read_share_group_state.rs         // 84
    ├── write_share_group_state.rs        // 85
    ├── delete_share_group_state.rs       // 86
    └── read_share_group_state_summary.rs // 87
```

### 2. `ShareCoordinator` (mirrors `TxnCoordinator`)

```rust
pub(crate) struct ShareCoordinator {
    node_id: NodeId,
    partitions: Arc<DashMap<(String, i32), Arc<Partition>>>,
    /// (group_id, topic_id, partition) -> locked per-share-partition state.
    state: DashMap<ShareStateKey, Arc<Mutex<SharePartitionState>>>,
    /// __share_group_state partition indices this broker leads.
    leader_partitions: RwLock<HashSet<i32>>,
    config: ShareCoordinatorConfig,
}
```

- Held on `Broker` as `Arc<ShareCoordinator>`; constructed + `recover(image)` in `Broker::start` (mirror txn).
- `recover()`: for each led `__share_group_state` partition, replay the log start→end, applying `ShareSnapshot` (replace) / `ShareUpdate` (merge) / tombstone (remove) into `state`, and record each key's latest-snapshot offset (for pruning).
- `refresh_leader_partitions(image)` on metadata change (mirror txn).

`ShareStateKey = (group_id: String, topic_id: Uuid, partition: i32)`.

`SharePartitionState`:
```rust
pub struct SharePartitionState {
    pub state_epoch: i32,
    pub leader_epoch: i32,
    pub start_offset: i64,            // SPSO
    pub delivery_complete_count: i32, // KIP-1226 lag (v1)
    pub state_batches: Vec<StateBatch>,
    // pruning bookkeeping (in-memory only):
    pub snapshot_epoch: i64,          // increments per snapshot written
    pub last_snapshot_offset: i64,    // log offset of this key's latest ShareSnapshot
    pub updates_since_snapshot: u32,
}
pub struct StateBatch { pub first_offset: i64, pub last_offset: i64,
                        pub delivery_state: i8, pub delivery_count: i16 }
```

### 3. `__share_group_state` topic (`bootstrap.rs`)

`TOPIC = "__share_group_state"`, `NUM_PARTITIONS = 50` (configurable), RF
`min(brokers, 3)`, `cleanup.policy=delete` (pruning is coordinator-driven, not
compaction). Created lazily via `ensure_topic(controller)` exactly like
`txn::bootstrap::ensure_topic` (TopicRecord + 50 PartitionRecords with
round-robin replicas in one `submit_change`, tolerating `TopicExists`).

### 4. Records (`persistence.rs`, hand-written codecs like `persistence_next_gen.rs`)

Two record types on `__share_group_state`, key carries a version discriminator:

- **ShareSnapshotKey/Value** (`KEY_SHARE_SNAPSHOT = 0`): key `{group_id, topic_id, partition}`; value `{snapshot_epoch: i64, state_epoch: i32, leader_epoch: i32, start_offset: i64, delivery_complete_count: i32, state_batches: Vec<StateBatch>}`.
- **ShareUpdateKey/Value** (`KEY_SHARE_UPDATE = 1`): same key; value `{snapshot_epoch: i64, leader_epoch: i32, start_offset: i64, delivery_complete_count: i32, state_batches: Vec<StateBatch>}` (a delta layered on the snapshot identified by `snapshot_epoch`).

Tombstone (value `None`) deletes the key's state.

### 5. Persister RPC handlers (83–87), plain `build_table` registration

All five route to the local `ShareCoordinator` for the led partition (inter-broker
callers reach the right broker via FindCoordinator SHARE). No per-connection ACL
(inter-broker cluster action, matching `write_txn_markers`). Each returns
`NOT_COORDINATOR`/`COORDINATOR_NOT_AVAILABLE` if this broker doesn't lead the
target `__share_group_state` partition, `COORDINATOR_LOAD_IN_PROGRESS` during recovery.

- **Initialize(83):** create state for each `(group, topic, partition)` at the
  given `StartOffset`/`StateEpoch` (idempotent: re-init at a higher `StateEpoch`
  replaces; lower/equal is fenced). Writes a `ShareSnapshot`.
- **Write(85):** validate `StateEpoch` (`FENCED_STATE_EPOCH` on mismatch) and
  `LeaderEpoch` (`FENCED_LEADER_EPOCH` if stale); merge the delta into in-memory
  state; write a `ShareUpdate` — or, every `snapshot_update_records_per_snapshot`
  updates, write a fresh `ShareSnapshot` instead and trigger pruning (§7). Returns
  per-partition error codes.
- **Read(84):** return full state (`StateEpoch, StartOffset, StateBatches`).
- **ReadSummary(87):** return `StartOffset, StateEpoch, LeaderEpoch, DeliveryCompleteCount` (no batches) — cheap leader bootstrap.
- **Delete(86):** write tombstone(s); remove from `state`.

### 6. FindCoordinator SHARE(2)

Add `KEY_TYPE_SHARE = 2`. Each key is `"groupId:topicId:partition"`; call
`share_coordinator::bootstrap::ensure_topic`, hash the key (mirror
`txn::partitioner`, applied to the composite string) to a `__share_group_state`
partition, look up that partition's leader in the metadata image, and return the
leader's broker address (preferring the advertised listener when leader == self).
Mirrors the TRANSACTION arm precisely.

### 7. Active pruning (`pruning.rs`) — *full pruning, per the chosen scope*

After writing a `ShareSnapshot` for a key, the prior records for that key are
superseded. Because the `__share_group_state` partition log interleaves many keys,
we prune by the **redundant offset**: for each led partition, `redundant_offset =
min over all live keys of (last_snapshot_offset)`. Every key still retains at
least its latest snapshot at or after `redundant_offset`, so advancing the log
start offset to `redundant_offset` is safe. Implementation:

- On each snapshot write, update the key's `last_snapshot_offset`, recompute the
  partition's `redundant_offset`, and if it advanced beyond the current log start,
  advance the partition log's start offset (the DeleteRecords / `delete_records`
  mechanism used by KIP-204 — reuse `Partition`'s log-start-advance API).
- Bound the work: only recompute/prune when a snapshot is written (not per update).
- A `#[allow]`-free, tested helper computes `redundant_offset` from the in-memory
  per-key `last_snapshot_offset` map for that partition.

### 8. Lifecycle wiring (membership coordinator → persister) — *per the chosen scope*

The Slice-A share group coordinator initializes/deletes share state:

- **On first use:** when the share actor reconciles and a subscribed `(topic,
  partition)` is not yet recorded in `ShareGroupStatePartitionMetadata`, it calls
  `InitializeShareGroupState` (StartOffset from `group.share.auto.offset.reset`,
  default earliest → 0/-2 sentinel; StateEpoch = group epoch or a dedicated state
  epoch) and, on success, persists a new **`ShareGroupStatePartitionMetadata`**
  record (key version 14) to `__consumer_offsets` recording the initialized
  partitions.
- **On removal / group delete:** call `DeleteShareGroupState` for the removed
  partitions and update/tombstone `ShareGroupStatePartitionMetadata`
  (`DeletingTopics` tracking).
- **Routing:** a `SharePersister` abstraction the actor calls. When this broker
  leads the target `__share_group_state` partition, it invokes the local
  `ShareCoordinator` directly; otherwise it issues the RPC via `InterBrokerClient`
  to the partition leader (located by the same hash → partition → leader logic as
  FindCoordinator SHARE). In single-broker setups (all tests here) it is always
  local. The call is best-effort/retryable from the actor; initialization failures
  leave the partition un-recorded so it is retried on the next reconcile (do not
  fail the heartbeat).

### 9. Config (`ShareCoordinatorConfig`)

`state_topic_num_partitions = 50`, `state_topic_replication_factor`,
`state_topic_min_isr`, `snapshot_update_records_per_snapshot` (default ~50). Added
to `BrokerConfig` (boxed, per the large_futures lesson) and defaulted in both
ctors.

## Error handling

- Not this broker's `__share_group_state` partition → `NOT_COORDINATOR`.
- Topic/partition not yet available → `COORDINATOR_NOT_AVAILABLE`; recovering → `COORDINATOR_LOAD_IN_PROGRESS`.
- `StateEpoch` mismatch on Write/Initialize → `FENCED_STATE_EPOCH`.
- Stale `LeaderEpoch` → `FENCED_LEADER_EPOCH` (reuse existing code).
- Lifecycle init failure in the actor → logged, partition left un-recorded for retry; heartbeat still succeeds.

## Testing

- **Unit:** ShareSnapshot/ShareUpdate codec round-trips (incl. tombstone, multi-batch, v1 `DeliveryCompleteCount`); state machine init→write→read→summary→delete; epoch fencing; snapshot-fold-every-N; `redundant_offset` computation and log-start advance; partitioner determinism.
- **Integration (`tests/share_state.rs`):** boot broker; `FindCoordinator(SHARE, "g:t:0")` returns this broker; Initialize → Write (several, crossing the snapshot threshold) → Read (full) → ReadSummary (matches) → Delete (Read returns empty/SPSO reset); restart-replay reconstructs state; pruning advances the log start offset after enough snapshots.
- **Lifecycle integration (extend `tests/share_groups.rs`):** a share group joining a topic causes `ShareGroupStatePartitionMetadata` to be recorded and the share state to be Initialized (observable via `ReadShareGroupStateSummary`); restart preserves it.

## Acceptance gate (Slice B)

1. `cargo fmt --check` clean.
2. `cargo clippy --workspace --all-targets -- -D warnings` clean (run the exact CI command; `--lib` is not sufficient — see [[feedback-clippy-all-targets-ci]]).
3. `cargo test --workspace` green.
4. No codegen drift.
5. `FindCoordinator SHARE(2)` resolves; persister RPCs 83–87 round-trip via integration test; restart-replay reconstructs state.
6. Snapshot folding + log-start pruning verified (the state log does not grow unboundedly across many writes).
7. A share group joining a topic Initializes share state and records `ShareGroupStatePartitionMetadata`; removal Deletes it.

## File-set sketch (batching)

- **Leaf/independent:** `share_coordinator/{persistence,state,partitioner}.rs` (codecs + state + hashing); `ShareCoordinatorConfig` on `BrokerConfig`. New `ShareGroupStatePartitionMetadata` codec (key v14) in `coordinator/unified/share/persistence.rs` + replay.
- **Coordinator core (depends on leaf):** `share_coordinator/{coordinator,bootstrap,pruning}.rs` + `Broker` wiring + `recover`.
- **Handlers (depends on core):** 5 RPC handlers + `build_table` registration; `FindCoordinator SHARE(2)`.
- **Lifecycle (depends on core + slice-A actor):** `SharePersister` abstraction + share-actor reconcile hook + `ShareGroupStatePartitionMetadata` tracking.
- **Tests:** `tests/share_state.rs` + extend `tests/share_groups.rs`.

`find_coordinator.rs`, `coordinator/unified/share/actor.rs`, and `broker.rs` are
shared-file edits — sequence those; the leaf modules are parallel-safe.
