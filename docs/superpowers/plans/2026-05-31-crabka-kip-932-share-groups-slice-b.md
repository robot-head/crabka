# KIP-932 Share Groups — Slice B (Share Coordinator / Persister) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** A durable share coordinator (persister) storing per-`(group, topicId, partition)` delivery state in `__share_group_state`, serving RPCs 83–87, with active snapshot-pruning, plus `FindCoordinator SHARE(2)` and group-coordinator lifecycle wiring (Initialize/Delete + `ShareGroupStatePartitionMetadata`).

**Architecture:** Mirror the transaction coordinator (`crates/broker/src/txn/`): a per-broker `ShareCoordinator` with `DashMap` state, a 50-partition internal topic created lazily, `recover()` replaying led partitions, records persisted to the partition log. Active pruning via `Partition::trim_to_offset`. Lifecycle calls routed through a `SharePersister` (local when this broker leads the target state partition, else inter-broker RPC).

**Tech Stack:** Rust 2024, tokio, `DashMap`, `bytes`, hand-written binary record codecs, `crabka-protocol` generated codecs, `assert2` tests, in-process `crabka_client_core::Client` harness.

**Spec:** `docs/superpowers/specs/2026-05-31-crabka-kip-932-share-groups-slice-b-design.md`

---

## Established facts (verbatim from research — do not re-derive)

- **Template:** `crates/broker/src/txn/{coordinator,bootstrap,partitioner}.rs`. `TxnCoordinator { node_id, partitions: Arc<PartitionRegistry>, state: DashMap<..>, leader_partitions: RwLock<HashSet<i32>>, .. }`; `new(node_id, partitions, producer_ids)`; `recover(&self, image: &MetadataImage)`; `refresh_leader_partitions(&self, image)` iterates `image.partitions_of(TOPIC)` and inserts `p.partition` where `p.leader == self.node_id`.
- **Partition API** (`crates/broker/src/partition.rs`): `log_start_offset()->i64`, `log_end_offset()->i64`, `read_log(offset:i64, max_bytes:usize)->Result<ReadOutput,BrokerError>`, `produce_batch(batch:RecordBatch)->Result<i64,BrokerError>` (async), **`trim_to_offset(new_start:i64)->Result<i64,BrokerError>`** (async; advances log start, returns new low watermark; what pruning uses). `PartitionRegistry::get(topic:&str, p:i32)->Option<Arc<Partition>>`.
- **MetadataImage**: `partitions_of(topic)->impl Iterator<&PartitionRecord>`, `partition(topic, idx)->Option<&PartitionRecord>`, `topic(name)`, `broker(node_id)->Option<&BrokerRegistrationRecord>`, `brokers()`.
- **Bootstrap template:** `txn::bootstrap::ensure_topic(controller: &Arc<dyn MetadataSource>)` — `TopicRecord` + 50 `PartitionRecord`s (round-robin replicas, RF=min(brokers,3)) in one `controller.submit_change(records)`, tolerating `MetadataError::TopicExists`.
- **Partitioner:** `txn::partitioner` has private `murmur2(&[u8])->u32` and `partition_for_tid(tid, num)->i32` (`murmur2(bytes).cast_signed().abs() % num`). Share keying is `(group, topicId, partition)` — build key string `"{group_id}:{topic_id}:{partition}"` and hash it the same way (copy `murmur2` into the share partitioner; it's small).
- **FindCoordinator** (`handlers/find_coordinator.rs`): `KEY_TYPE_GROUP=0`, `KEY_TYPE_TRANSACTION=1`. Handler extracts `broker_id=broker.config.broker_id`, `node_id=broker.config.node_id`, `advertised=broker.config.advertised_listener.clone()`, `controller=Arc::clone(&broker.controller)`, `keys` from `coordinator_keys` (or `[key]`). The TRANSACTION arm: `ensure_topic` → for each key `partition_for_tid` → `image.partition(TOPIC,p)` → `pr.leader` → `image.broker(leader)` → push `Coordinator { key, node_id, host, port, error_code }` (advertised listener when `leader==node_id`). `parse_host_port(&advertised)` helper exists. Registered `t.register(10, find_coordinator::handle)`.
- **InterBrokerClient** (`network/client.rs`): `connect_as_connection(host:&str, port:u16, listener_protocol:ListenerProtocol, server_name:&str, options:ConnectionOptions)->Result<Connection,InterBrokerError>`. `Connection::send<R: ProtocolRequest>(req)->Result<R::Response,ClientError>`, then `conn.close()`. Example: `txn/handlers/end_txn.rs:402-486` (endpoint resolution prefers the inter-broker listener). `Broker` has `inter_broker_client: Arc<InterBrokerClient>` and `inter_broker_listener_protocol: ListenerProtocol`.
- **Broker::start** constructs `txn_coordinator` (~line 1350) then `txn_coordinator.recover(&controller.current_image()).await`; struct field at ~line 51; moved into literal at ~line 2131. `Broker.controller: Arc<dyn MetadataSource>`, `partitions: Arc<PartitionRegistry>`.
- **HandlerFn** = `fn(&Broker, i16, i32, &[u8]) -> BoxFuture<'static, Result<Bytes, BrokerError>>`; `t.register(api_key, handle)`. Share-state handlers need no ACL ctx (inter-broker), so the 4-arg form fits (like `write_txn_markers`).
- **Error codes (all exist):** `COORDINATOR_LOAD_IN_PROGRESS=14`, `COORDINATOR_NOT_AVAILABLE=15`, `NOT_COORDINATOR=16`, `FENCED_LEADER_EPOCH=74`, `FENCED_STATE_EPOCH=124`.
- **Generated persister types** (import `crabka_protocol::owned::<snake>::*`; `Uuid = crabka_protocol::primitives::uuid::Uuid`; every struct has `unknown_tagged_fields`): see the spec for field lists. Request types impl `ProtocolRequest` with `type Response = ...Response`, `FLEXIBLE_MIN=0`. Key nested response type `StateBatch { first_offset:i64, last_offset:i64, delivery_state:i8, delivery_count:i16 }`.
- **Slice-A share persistence** (`coordinator/unified/share/persistence.rs`): key versions 9–13 used; `ShareGroupKey` enum + `encode_share_key`/`parse_share_key`; value codecs use `put_i16(0)` version preamble + `put_string`/`get_string`/`get_i16`/`get_i32` from `crate::coordinator::unified::persistence`. Top-level `Key` enum in `coordinator/unified/persistence.rs` routes `9..=13 => Key::Share(parse_share_key(..))` (MUST widen to `9..=14`). `bootstrap.rs::apply_share_record` dispatches each variant to `coord.replay_share_*`.
- **Slice-A share actor** (`coordinator/unified/share/actor.rs`): `reconcile(state, metadata)` is a SYNC fn called from async `handle_heartbeat`/`handle_session_tick`. Lifecycle RPCs must be issued from the async handler AFTER `reconcile` returns. Actor holds `metadata: Arc<dyn MetadataProvider>` (`snapshot()->ReconcileInput { topic_id_by_name, partitions_per_topic, partition_racks }`), `offsets_log`, `coordinator: Arc<GroupCoordinator>`. `ShareGroupActorHandle::spawn(group_id, config, metadata, offsets_log, coordinator)`.

## Design decisions (locked)

- **Share-state record key:** leading `i16` record-type version: `KEY_SHARE_SNAPSHOT=0`, `KEY_SHARE_UPDATE=1`; then `group_id` (string), `topic_id` (16 bytes), `partition` (i32). (Distinct namespace from the `__consumer_offsets` share keys 9–14, since this is a different topic.)
- **WriteShareGroupState merge:** validate epochs; advance `start_offset`; drop in-memory batches fully below the new `start_offset`; upsert the written `state_batches` by `first_offset` (sorted); update `delivery_complete_count`, `leader_epoch`. Persist a `ShareUpdate` carrying the written delta; every `snapshot_update_records_per_snapshot` updates, persist a full `ShareSnapshot` and prune. (Faithful enough for a store; leader-side coalescing fidelity is Slice C.)
- **FindCoordinator SHARE key format:** `"{group_id}:{topic_id}:{partition}"` (Kafka's share-coordinator key form), hashed with murmur2 % NUM_PARTITIONS.
- **`ShareGroupStatePartitionMetadata`** key version **14** in `__consumer_offsets` (greenfield; internal record).

## Task batching (sequential dispatch, git-safe; group by cohesion)

- **B-α (leaf, new files + additive):** Tasks 1–4 + 10.
- **B-core:** Tasks 5, 6, 7 (coordinator + pruning + Broker wiring).
- **B-handlers:** Tasks 8, 9 (RPC handlers + FindCoordinator SHARE).
- **B-lifecycle:** Task 11 (SharePersister + actor hook).
- **B-tests:** Task 12.

Every implementer: work in the worktree, `git -C <worktree>`, assert branch `claude/intelligent-bouman-224792` before commit, identity overrides, `cargo fmt --all` pre-commit, and verify with **`cargo clippy --workspace --all-targets -- -D warnings`** (NOT `--lib` — it misses test-module lints; this is the CI gate).

---

## Task 1: `ShareCoordinatorConfig`

**Files:** Create `crates/broker/src/share_coordinator/mod.rs` (`pub mod config;` + later modules), `crates/broker/src/share_coordinator/config.rs`; add `pub mod share_coordinator;` to `crates/broker/src/lib.rs` (or wherever top-level modules are declared — grep `pub mod txn;`); add boxed field to `BrokerConfig` (`crates/broker/src/config.rs`).

- [ ] **Step 1: failing test** in `config.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    #[test]
    fn defaults_match_kafka() {
        let c = ShareCoordinatorConfig::default();
        assert!(c.state_topic_num_partitions == 50);
        assert!(c.snapshot_update_records_per_snapshot == 50);
        assert!(c.state_topic_min_isr == 1);
    }
}
```
- [ ] **Step 2:** `cargo test -p crabka-broker share_coordinator::config` → FAIL.
- [ ] **Step 3:** implement:
```rust
//! KIP-932 share-coordinator (persister) configuration.
#[derive(Debug, Clone)]
pub struct ShareCoordinatorConfig {
    pub state_topic_num_partitions: i32,
    pub state_topic_replication_factor: i16,
    pub state_topic_min_isr: i32,
    pub snapshot_update_records_per_snapshot: u32,
}
impl Default for ShareCoordinatorConfig {
    fn default() -> Self {
        Self {
            state_topic_num_partitions: 50,
            state_topic_replication_factor: 3,
            state_topic_min_isr: 1,
            snapshot_update_records_per_snapshot: 50,
        }
    }
}
```
Create `share_coordinator/mod.rs` with `pub mod config;`. Add `pub mod share_coordinator;` next to `pub mod txn;`. Add to `BrokerConfig`: `pub share_coordinator: Box<crate::share_coordinator::config::ShareCoordinatorConfig>,` and default it `Box::new(Default::default())` in BOTH `for_tests` and the production default (grep the `share_group:` defaults from Slice A — co-locate).
- [ ] **Step 4:** test passes; `cargo build -p crabka-broker`.
- [ ] **Step 5:** commit `feat(kip-932): ShareCoordinatorConfig`.

## Task 2: Share-state record codecs

**Files:** Create `crates/broker/src/share_coordinator/persistence.rs`; declare `pub mod persistence;` in `share_coordinator/mod.rs`.

Mirror `coordinator/unified/persistence_next_gen.rs`. Reuse leaf helpers — import `use crate::coordinator::unified::persistence::{get_i16, get_i32, get_i64, get_string, put_string};` (confirm `get_i64`/`put_i64` exist; if not, use `buf.put_i64`/`get` via `bytes::Buf`). `Uuid` here = `uuid::Uuid` (16 bytes via `as_bytes()`/`from_bytes`).

- [ ] **Step 1: failing round-trip tests** covering: snapshot key + value, update key + value, tombstone-parse of key, multi-batch, and the v1 `delivery_complete_count` field.
- [ ] **Step 2:** run → FAIL.
- [ ] **Step 3:** implement:
```rust
use bytes::{Buf, BufMut, Bytes, BytesMut};
use uuid::Uuid;
use crate::error::BrokerError;
use crabka_protocol::error::ProtocolError;
use crate::coordinator::unified::persistence::{get_i16, get_i32, get_string, put_string};

pub const KEY_SHARE_SNAPSHOT: i16 = 0;
pub const KEY_SHARE_UPDATE: i16 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareStateKey { pub record_type: i16, pub group_id: String, pub topic_id: Uuid, pub partition: i32 }

pub fn encode_state_key(k: &ShareStateKey) -> Bytes {
    let mut b = BytesMut::new();
    b.put_i16(k.record_type);
    put_string(&mut b, &k.group_id);
    b.put_slice(k.topic_id.as_bytes());
    b.put_i32(k.partition);
    b.freeze()
}
pub fn parse_state_key(mut buf: &[u8]) -> Result<ShareStateKey, BrokerError> {
    let record_type = get_i16(&mut buf)?;
    if record_type != KEY_SHARE_SNAPSHOT && record_type != KEY_SHARE_UPDATE {
        return Err(BrokerError::Protocol(ProtocolError::InvalidValue("unknown share-state key type")));
    }
    let group_id = get_string(&mut buf)?;
    if buf.len() < 20 { return Err(BrokerError::Protocol(ProtocolError::InvalidValue("short share-state key"))); }
    let mut id = [0u8; 16]; id.copy_from_slice(&buf[..16]); buf.advance(16);
    let topic_id = Uuid::from_bytes(id);
    let partition = get_i32(&mut buf)?;
    Ok(ShareStateKey { record_type, group_id, topic_id, partition })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateBatch { pub first_offset: i64, pub last_offset: i64, pub delivery_state: i8, pub delivery_count: i16 }

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareSnapshotValue {
    pub snapshot_epoch: i64, pub state_epoch: i32, pub leader_epoch: i32,
    pub start_offset: i64, pub delivery_complete_count: i32, pub state_batches: Vec<StateBatch>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareUpdateValue {
    pub snapshot_epoch: i64, pub leader_epoch: i32,
    pub start_offset: i64, pub delivery_complete_count: i32, pub state_batches: Vec<StateBatch>,
}
```
Implement `encode`/`decode` for both values with a `put_i16(0)` version preamble, then fixed fields, then a length-prefixed (`put_i32(len)`) array of `StateBatch` (`i64,i64,i8,i16`). Add a private `put_batches`/`get_batches` helper. (Use `buf.put_i64`, `buf.get_i64`, `buf.put_i8`, `buf.get_i8`, `buf.put_i16`, `buf.get_i16` from `bytes::{BufMut,Buf}`.)
- [ ] **Step 4:** tests pass.
- [ ] **Step 5:** commit `feat(kip-932): share-state record codecs (ShareSnapshot/ShareUpdate)`.

## Task 3: `SharePartitionState` + partitioner

**Files:** Create `crates/broker/src/share_coordinator/state.rs` and `crates/broker/src/share_coordinator/partitioner.rs`; declare both in `mod.rs`.

- [ ] **Step 1: failing tests**: `state.rs` — an `apply_snapshot`/`apply_update`/`merge_write` unit test (write advances start_offset, drops sub-SPSO batches, upserts batches); `partitioner.rs` — `partition_for_share_key` determinism + range `[0,num)`.
- [ ] **Step 2:** run → FAIL.
- [ ] **Step 3:** implement `state.rs`:
```rust
use crate::share_coordinator::persistence::{ShareSnapshotValue, ShareUpdateValue, StateBatch};

#[derive(Debug, Clone, Default)]
pub struct SharePartitionState {
    pub state_epoch: i32, pub leader_epoch: i32, pub start_offset: i64,
    pub delivery_complete_count: i32, pub state_batches: Vec<StateBatch>,
    pub snapshot_epoch: i64, pub last_snapshot_offset: i64, pub updates_since_snapshot: u32,
}
impl SharePartitionState {
    pub fn apply_snapshot(&mut self, v: &ShareSnapshotValue) {
        self.snapshot_epoch = v.snapshot_epoch; self.state_epoch = v.state_epoch;
        self.leader_epoch = v.leader_epoch; self.start_offset = v.start_offset;
        self.delivery_complete_count = v.delivery_complete_count;
        self.state_batches = v.state_batches.clone(); self.updates_since_snapshot = 0;
    }
    pub fn apply_update(&mut self, v: &ShareUpdateValue) {
        self.leader_epoch = v.leader_epoch; self.merge_batches(v.start_offset, &v.state_batches);
        self.delivery_complete_count = v.delivery_complete_count; self.updates_since_snapshot += 1;
    }
    /// Advance SPSO, drop batches fully below it, upsert written batches by first_offset.
    pub fn merge_batches(&mut self, new_start: i64, written: &[StateBatch]) {
        if new_start > self.start_offset { self.start_offset = new_start; }
        self.state_batches.retain(|b| b.last_offset >= self.start_offset);
        for w in written {
            if w.last_offset < self.start_offset { continue; }
            self.state_batches.retain(|b| b.first_offset != w.first_offset);
            self.state_batches.push(w.clone());
        }
        self.state_batches.sort_by_key(|b| b.first_offset);
    }
    pub fn to_snapshot(&self) -> ShareSnapshotValue {
        ShareSnapshotValue { snapshot_epoch: self.snapshot_epoch + 1, state_epoch: self.state_epoch,
            leader_epoch: self.leader_epoch, start_offset: self.start_offset,
            delivery_complete_count: self.delivery_complete_count, state_batches: self.state_batches.clone() }
    }
}
```
`partitioner.rs`: copy `murmur2` from `txn/partitioner.rs` (private) + add:
```rust
pub fn partition_for_share_key(group_id: &str, topic_id: &uuid::Uuid, partition: i32, num: i32) -> i32 {
    let key = format!("{group_id}:{topic_id}:{partition}");
    let h = murmur2(key.as_bytes()).cast_signed();
    let abs = if h == i32::MIN { 0 } else { h.abs() };
    abs % num
}
```
- [ ] **Step 4:** tests pass.
- [ ] **Step 5:** commit `feat(kip-932): SharePartitionState + share-state partitioner`.

## Task 4: `__share_group_state` bootstrap

**Files:** Create `crates/broker/src/share_coordinator/bootstrap.rs`; declare in `mod.rs`.

- [ ] **Step 1:** copy `crates/broker/src/txn/bootstrap.rs` verbatim, changing `TOPIC = "__share_group_state"`, keeping `NUM_PARTITIONS = 50` (or sourcing from config — keep a const for the FindCoordinator path; the coordinator uses config), and RF `min(brokers,3)`. Map errors via `BrokerError` (use the same `BrokerError` variant txn used, e.g. a generic one — check `BrokerError::Txn` exists; if a `Share` variant is cleaner, add `BrokerError::Share(String)` to `crates/broker/src/error.rs`).
- [ ] **Step 2:** add a unit test that, given a fake controller/image with N brokers, `ensure_topic` submits a TopicRecord with 50 partitions (or assert idempotency when the topic already exists). If a fake `MetadataSource` is heavy, defer to the integration test and just `cargo build`.
- [ ] **Step 3:** `cargo build -p crabka-broker`.
- [ ] **Step 4:** commit `feat(kip-932): __share_group_state topic bootstrap`.

## Task 10: `ShareGroupStatePartitionMetadata` record (key v14)

**Files:** `crates/broker/src/coordinator/unified/share/persistence.rs`, `crates/broker/src/coordinator/unified/persistence.rs` (widen range), `crates/broker/src/coordinator/bootstrap.rs` (`apply_share_record` arm), `crates/broker/src/coordinator/unified/mod.rs` (`replay_share_state_partition_metadata`).

Tracks which `(topic_id, partition)` share-states a group has initialized (+ a `deleting` set).

- [ ] **Step 1: failing test**: round-trip the new key (v14) + value, and that `parse_share_key(14, ..)` returns the new variant.
- [ ] **Step 2:** run → FAIL.
- [ ] **Step 3:** add `pub const KEY_SHARE_GROUP_STATE_PARTITION_METADATA: i16 = 14;`, a `ShareGroupKey::StatePartitionMetadata { group_id }` variant (+ `encode_share_key`/`parse_share_key` arms), and:
```rust
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ShareGroupStatePartitionMetadataValue {
    pub initialized: Vec<(uuid::Uuid, Vec<i32>)>, // topic_id -> initialized partitions
    pub deleting: Vec<uuid::Uuid>,
}
```
with `encode`/`decode` (i16(0) preamble; length-prefixed arrays). Widen `coordinator/unified/persistence.rs` range `9..=13` → `9..=14`. Add `apply_share_record` arm calling `coord.mark_share(&group_id)` + `coord.replay_share_state_partition_metadata(&group_id, value)`. Add that replay method + storage to `GroupCoordinator` / the share seed (mirror existing `replay_share_*`), exposed so the lifecycle hook (Task 11) can read "already initialized" partitions.
- [ ] **Step 4:** tests pass; `cargo build`.
- [ ] **Step 5:** commit `feat(kip-932): ShareGroupStatePartitionMetadata record (key v14)`.

## Task 5: `ShareCoordinator` (core state machine)

**Files:** Create `crates/broker/src/share_coordinator/coordinator.rs`; declare in `mod.rs`. (Pruning lives in Task 6; leave a hook.)

- [ ] **Step 1: failing tests** (unit, using a real in-memory `Partition`? — if heavy, test the pure state transitions via the `state` map directly): `initialize_then_read`, `write_advances_spso_and_summary_matches`, `write_fences_stale_state_epoch`, `delete_removes_state`, `snapshot_fold_after_threshold`. Prefer testing the coordinator's pure methods that take `&self` and a partition handle; if a `Partition` is needed, construct one via the test harness used by `txn` coordinator tests (check `txn/coordinator.rs` tests for how they build a `Partition`).
- [ ] **Step 2:** run → FAIL.
- [ ] **Step 3:** implement, mirroring `TxnCoordinator`:
```rust
pub(crate) struct ShareCoordinator {
    pub(crate) node_id: crabka_metadata::NodeId,
    pub(crate) partitions: Arc<PartitionRegistry>,
    state: DashMap<ShareStateKey3, Arc<Mutex<SharePartitionState>>>, // key = (group, topic_id, partition)
    leader_partitions: RwLock<HashSet<i32>>,
    config: ShareCoordinatorConfig,
}
```
(define `ShareStateKey3 = (String, uuid::Uuid, i32)`). Methods:
- `new(node_id, partitions, config)`.
- `refresh_leader_partitions(&self, image)` — iterate `image.partitions_of(bootstrap::TOPIC)`, insert where `leader==node_id`.
- `recover(&self, image)` — like txn: for each led `__share_group_state` partition, replay log start→end; `parse_state_key` + decode value; `apply_snapshot`/`apply_update`; track `last_snapshot_offset` per key (the record's offset for snapshot records).
- `is_leader(&self, state_partition: i32) -> bool` (read `leader_partitions`).
- `state_partition_for(&self, group, topic_id, partition) -> i32` via `partition_for_share_key(.., self.config.state_topic_num_partitions)`.
- `initialize(&self, group, topic_id, partition, state_epoch, start_offset) -> Result<(), i16>` — fence if existing `state_epoch >= state_epoch`; create/replace in-memory; persist a `ShareSnapshot`; set `last_snapshot_offset`.
- `write(&self, group, topic_id, partition, state_epoch, leader_epoch, start_offset, delivery_complete_count, batches) -> Result<(), i16>` — fence `state_epoch` (`FENCED_STATE_EPOCH`) and stale `leader_epoch` (`FENCED_LEADER_EPOCH`); `apply_update`; persist `ShareUpdate`; if `updates_since_snapshot >= config.snapshot_update_records_per_snapshot`, persist `to_snapshot()` (bump snapshot_epoch), reset counter, set `last_snapshot_offset`, and call the pruning hook (Task 6).
- `read(&self, ..) -> Option<SharePartitionState>` (clone) ; `read_summary(&self, ..) -> Option<(state_epoch, leader_epoch, start_offset, delivery_complete_count)>`.
- `delete(&self, group, topic_id, partition) -> Result<(), i16>` — persist tombstone (snapshot key, value None) and remove from `state`.
- A private `persist_record(&self, state_partition: i32, key: ShareStateKey, value: Option<Bytes>) -> Result<i64, BrokerError>` building a `RecordBatch` (one `Record { key: Some(encode_state_key(&key)), value, .. }`) and calling `part.produce_batch(batch).await` (returns base offset). Use `self.partitions.get(bootstrap::TOPIC, state_partition)`.
- [ ] **Step 4:** tests pass; `cargo build`.
- [ ] **Step 5:** commit `feat(kip-932): ShareCoordinator state machine (init/write/read/summary/delete)`.

## Task 6: Active pruning

**Files:** Create `crates/broker/src/share_coordinator/pruning.rs`; wire into `coordinator.rs`'s snapshot path.

- [ ] **Step 1: failing test** for `redundant_offset(per_key_last_snapshot: &[i64]) -> Option<i64>` = `min` (None if empty) and a coordinator test asserting that after enough writes to cross the snapshot threshold, the partition's log start offset advances (via a real/stub Partition exposing `log_start_offset`).
- [ ] **Step 2:** run → FAIL.
- [ ] **Step 3:** implement `pruning.rs`:
```rust
/// Smallest last-snapshot offset across all live keys on a state partition.
/// Pruning the log below this is safe: every key retains its latest snapshot.
pub fn redundant_offset(per_key_last_snapshot: &[i64]) -> Option<i64> {
    per_key_last_snapshot.iter().copied().min()
}
```
In `coordinator.rs`, maintain a per-state-partition view of each key's `last_snapshot_offset` (e.g. group `state` keys by `state_partition_for(..)`), and after writing a snapshot compute `redundant_offset`; if it exceeds the partition's current `log_start_offset()`, call `part.trim_to_offset(redundant).await` (ignore/log errors; pruning is best-effort). Only run on snapshot writes, not per update.
- [ ] **Step 4:** tests pass.
- [ ] **Step 5:** commit `feat(kip-932): share-state log pruning (redundant-offset trim)`.

## Task 7: `Broker` wiring

**Files:** `crates/broker/src/broker.rs`.

- [ ] **Step 1:** Add `pub(crate) share_coordinator: Arc<crate::share_coordinator::coordinator::ShareCoordinator>,` to the `Broker` struct (after `txn_coordinator`). In `start`, right after the txn-coordinator block, construct it with `config.node_id`, `partitions.clone()`, `(*config.share_coordinator).clone()`, then `share_coordinator.recover(&controller.current_image()).await` (log on error like txn). Add `share_coordinator,` to the struct literal. Refresh leader partitions wherever txn's `refresh_leader_partitions` is called on metadata change (grep for `txn_coordinator.refresh_leader_partitions` and mirror).
- [ ] **Step 2:** `cargo build -p crabka-broker`; `cargo test -p crabka-broker` (no regressions).
- [ ] **Step 3:** commit `feat(kip-932): wire ShareCoordinator into Broker (construct + recover)`.

## Task 8: Persister RPC handlers (83–87)

**Files:** Create `crates/broker/src/share_coordinator/handlers/{mod,initialize,read,write,delete,read_summary}.rs`; register in `crates/broker/src/handlers/mod.rs`.

Each handler: decode the typed request; for each `(topic, partition)` compute `state_partition = broker.share_coordinator.state_partition_for(..)`; if `!broker.share_coordinator.is_leader(state_partition)` → per-partition `NOT_COORDINATOR`; else call the coordinator method and map `Result<_,i16>` to the per-partition `error_code`. Encode the typed response. Register `t.register(83..=87, ...)`.

- [ ] **Step 1: failing test** — a handler-level unit test is heavy (needs a Broker); rely on the Task 12 integration test. Instead add a tiny pure mapping test if useful, else proceed and let Task 12 gate.
- [ ] **Step 2:** implement the five handlers mirroring `write_txn_markers::handle`'s shape (4-arg `HandlerFn`, `Box::pin(async move {...})`, decode → coordinator → encode). Use the generated field names from the dossier (e.g. `WriteShareGroupStateRequest { group_id, topics: Vec<WriteStateData{topic_id, partitions: Vec<PartitionData{partition, state_epoch, leader_epoch, start_offset, delivery_complete_count, state_batches}}>} }`). Build responses with matching `results`/`partitions`/`error_code` shapes.
- [ ] **Step 3:** register in `handlers/mod.rs` (module decl + 5 `t.register`s). `cargo build`.
- [ ] **Step 4:** commit `feat(kip-932): ShareGroupState persister RPC handlers (83-87)`.

## Task 9: `FindCoordinator SHARE(2)`

**Files:** `crates/broker/src/handlers/find_coordinator.rs`.

- [ ] **Step 1:** add `const KEY_TYPE_SHARE: i8 = 2;` and a match arm mirroring `KEY_TYPE_TRANSACTION` but: call `crate::share_coordinator::bootstrap::ensure_topic(&controller)`; parse each key `"group:topicId:partition"` (split on ':'; on malformed key push `COORDINATOR_NOT_AVAILABLE`); compute the state partition via `crate::share_coordinator::partitioner::partition_for_share_key(group, &topic_uuid, partition, NUM_PARTITIONS)`; look up `image.partition("__share_group_state", p)` → leader → broker address (same as TXN arm). Use `share_coordinator::bootstrap::{TOPIC, NUM_PARTITIONS}`.
- [ ] **Step 2:** verified by the Task 12 integration test (`FindCoordinator(SHARE, "g:<uuid>:0")` returns this broker). `cargo build`.
- [ ] **Step 3:** commit `feat(kip-932): FindCoordinator SHARE(2) routing`.

## Task 11: Lifecycle wiring (`SharePersister` + actor hook)

**Files:** Create `crates/broker/src/share_coordinator/persister_client.rs` (the `SharePersister`); thread it into `coordinator/unified/share/actor.rs` (spawn signature + `handle_heartbeat`); `coordinator/unified/mod.rs` (pass it through `get_or_create_share`); `broker.rs` (build the `SharePersister` from `share_coordinator` + `inter_broker_client` and hand it to the `GroupCoordinator`).

`SharePersister` exposes `async fn initialize(&self, group, topic_id, partition, state_epoch, start_offset) -> Result<(), BrokerError>` and `async fn delete(&self, group, topic_id, partition)`. Implementation: compute the state partition; if this broker leads it, call the local `ShareCoordinator` directly; else resolve the leader from the metadata image and send `InitializeShareGroupStateRequest`/`DeleteShareGroupStateRequest` via `inter_broker_client.connect_as_connection(..).send(req)` (mirror `end_txn.rs`). In all tests here it's local (single broker).

- [ ] **Step 1: failing test** (extend `tests/share_groups.rs`): after a share group joins a topic with P partitions, `ReadShareGroupStateSummary` for those `(topic,partition)`s returns initialized state, and `ShareGroupStatePartitionMetadata` is recorded (survives restart).
- [ ] **Step 2:** run → FAIL.
- [ ] **Step 3:** in the share actor's `handle_heartbeat`, AFTER `reconcile` returns and the assignment is known, for each assigned `(topic_id, partition)` not already in the group's `ShareGroupStatePartitionMetadata`, call `share_persister.initialize(group, topic_id, partition, state_epoch, start_offset=0)` (best-effort; on success, record it in `ShareGroupStatePartitionMetadata` and persist that record via the offsets log). On group-empty/topic-removal path, call `delete` and update the metadata. Do NOT fail the heartbeat on persister error — log and retry next reconcile. Thread `share_persister: Arc<SharePersister>` through `ShareGroupActorHandle::spawn` and `GroupCoordinator::get_or_create_share`.
- [ ] **Step 4:** test passes; full `cargo test -p crabka-broker`.
- [ ] **Step 5:** commit `feat(kip-932): wire group-coordinator share-state lifecycle (Initialize/Delete)`.

## Task 12: Integration tests

**Files:** Create `crates/broker/tests/share_state.rs`; extend `crates/broker/tests/share_groups.rs` (done in Task 11 Step 1 if not already).

- [ ] **Step 1:** `tests/share_state.rs` (mirror `share_groups.rs` boot+client harness):
  - `find_coordinator_share_returns_broker`: `FindCoordinatorRequest { key_type: 2, coordinator_keys: vec!["g:<uuid>:0"], .. }` → one coordinator, `error_code == 0`, `node_id == this broker`.
  - `persister_round_trip`: `InitializeShareGroupState(g, t, 0, state_epoch=0, start_offset=0)` → ok; `WriteShareGroupState` with a couple `StateBatch`es and `start_offset` advance → ok; `ReadShareGroupState` returns those batches + start_offset; `ReadShareGroupStateSummary` returns matching `start_offset`/`state_epoch`/`delivery_complete_count`; `DeleteShareGroupState` → subsequent `ReadShareGroupState` returns empty/initial.
  - `write_fences_stale_state_epoch`: a `WriteShareGroupState` with a wrong `state_epoch` → per-partition `error_code == 124` (FENCED_STATE_EPOCH).
  - `state_survives_restart`: initialize+write, drop broker, restart same dir, `ReadShareGroupStateSummary` reflects recovered state.
  - `pruning_advances_log_start` (if feasible at integration level): write enough times to cross the snapshot threshold ×2 and assert the `__share_group_state` partition's log start advanced (may need a broker-internal accessor; if not exposable, cover this in the Task 6 unit test instead and note it here).
- [ ] **Step 2:** run → iterate until green. Fix real bugs in share code (separate commits).
- [ ] **Step 3: full gate:** `cargo fmt --all && cargo fmt --check`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo test --workspace`; `bash tools/regenerate.sh && git status --porcelain crates/protocol` empty.
- [ ] **Step 4:** commit `test(kip-932): share-coordinator persister + lifecycle integration tests`.

---

## Acceptance gate (Slice B)

1. `cargo fmt --check` clean. 2. `cargo clippy --workspace --all-targets -- -D warnings` clean. 3. `cargo test --workspace` green. 4. No codegen drift. 5. FindCoordinator SHARE(2) resolves; RPCs 83–87 round-trip; restart-replay reconstructs state. 6. Snapshot folding + log-start pruning verified. 7. Share group joining a topic Initializes state + records `ShareGroupStatePartitionMetadata`; removal Deletes it.

## Self-review

- **Spec coverage:** persister module (T1–7), RPCs (T8), FindCoordinator SHARE (T9), `ShareGroupStatePartitionMetadata` (T10), pruning (T6), lifecycle (T11), tests incl. restart + pruning + fencing (T12). All spec acceptance-gate items mapped.
- **Type consistency:** `ShareStateKey`/`ShareStateKey3`, `SharePartitionState`, `ShareSnapshotValue`/`ShareUpdateValue`/`StateBatch`, `ShareCoordinator`, `SharePersister`, `partition_for_share_key`, `KEY_SHARE_SNAPSHOT/UPDATE`, `KEY_SHARE_GROUP_STATE_PARTITION_METADATA=14` used consistently.
- **Confirm-at-build:** `get_i64`/`put_i64` helper availability (else use `bytes` directly); the exact `BrokerError` variant for share errors (reuse or add `Share`); how `txn` coordinator tests construct a `Partition` (for T5/T6 unit tests); the metadata-change refresh hook site for `refresh_leader_partitions`.
