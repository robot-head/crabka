# Slice 64a follow-up — KIP-848 persistence + JVM client gating Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire the next-gen `GroupActor` to persist KIP-848 records (3/5/6/7/8) to `__consumer_offsets` partition 0 on every state mutation, and advertise `group.version=1` in ApiVersions so kafka-clients 4.0 engages KIP-848 instead of falling back to classic.

**Architecture:** New `OffsetsLog` trait abstracts the partition-writer call. `ProductionOffsetsLog` wraps `Arc<Partition>` and replays the `WriterMessage::Produce(ProduceJob { batch, ack })` pattern from `handlers/offset_commit.rs::append_batch`. Actor mutations bundle their k3/k5/k6/k7/k8 records into a single `RecordBatch` and await `OffsetsLog::append` before replying. On `Err`, the actor exits and the coordinator's `get_or_create` respawns a fresh actor seeded from a coordinator-owned `seeds_cache`.

**Tech Stack:** Rust 1.95, tokio (mpsc + oneshot), bytes, dashmap, async-trait, existing `crabka-broker`/`crabka-protocol` codegen.

**Spec:** `docs/superpowers/specs/2026-05-28-crabka-kip-848-64a-followup-design.md`

---

## File map

**Create:**
- `crates/broker/src/coordinator/next_gen/offsets_log.rs` — `OffsetsLog` trait, `ProductionOffsetsLog`, `fake::InMemoryOffsetsLog`.

**Modify:**
- `crates/broker/src/coordinator/next_gen/mod.rs` — pass partitions into `NextGenCoordinator::new`, construct `ProductionOffsetsLog`, thread `Arc<dyn OffsetsLog>` into `get_or_create`/`GroupActorHandle::spawn`, add `seeds_cache: Arc<DashMap<String, GroupSeed>>`, add `update_cache` method.
- `crates/broker/src/coordinator/next_gen/group_actor.rs` — add `offsets_log` + `coordinator` fields, factor out a record-emit helper, wire writes into every mutation path in `handle_heartbeat` and the tick path; `actor_loop` exits on `OffsetsLog::append` Err.
- `crates/broker/src/coordinator/bootstrap.rs` — populate `seeds_cache` alongside `seeds` so respawned actors have a hydration source.
- `crates/broker/src/handlers/api_versions.rs` — advertise `group.version=1` in `supported_features` + `finalized_features` when `next_gen_consumer_group.next_gen_enabled()` is true.
- `crates/broker/src/broker.rs` — pass `partitions` to `NextGenCoordinator::new`.
- `crates/broker/tests/consumer_group_next_gen_persistence.rs` — drop both `#[ignore]` attributes.
- `crates/broker/tests/jvm_consumer_group_next_gen.rs` — drop all four `#[ignore]` attributes.
- `.github/workflows/ci.yml` — restore `--test jvm_consumer_group_next_gen` to the `broker-jvm-acceptance` job.
- `STATUS.md` — slice-64a-followup entry; drop the two follow-up bullets from slice 64a's out-of-scope list.

---

## Pre-flight

- [ ] **PF-1: Branch already created**

```bash
git rev-parse --abbrev-ref HEAD
```

Expected: `kip-848-64a-followup` (created during spec phase).

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p crabka-broker --lib coordinator::next_gen
```

Expected: all clean, 34 next_gen unit tests pass.

---

## Task 1 — OffsetsLog trait + production impl + fake

**Files:**
- Create: `crates/broker/src/coordinator/next_gen/offsets_log.rs`
- Modify: `crates/broker/src/coordinator/next_gen/mod.rs` — add `pub mod offsets_log;`

- [ ] **Step 1.1: Create `offsets_log.rs`**

```rust
//! `OffsetsLog` — abstraction for writing KIP-848 records to
//! `__consumer_offsets` partition 0. Production impl wraps the
//! partition's `writer_tx` mpsc and mirrors the pattern in
//! `handlers/offset_commit.rs::append_batch`.

use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use tokio::sync::oneshot;

use crabka_protocol::records::RecordBatch;

use crate::error::BrokerError;
use crate::partition::{Partition, ProduceJob, WriterMessage};

pub const OFFSETS_TOPIC: &str = "__consumer_offsets";
pub const OFFSETS_PARTITION: i32 = 0;

#[async_trait]
pub trait OffsetsLog: Send + Sync + std::fmt::Debug {
    async fn append(&self, batch: RecordBatch) -> Result<(), BrokerError>;
}

#[derive(Debug)]
pub struct ProductionOffsetsLog {
    partition: Arc<Partition>,
}

impl ProductionOffsetsLog {
    pub fn from_partitions(
        partitions: &Arc<DashMap<(String, i32), Arc<Partition>>>,
    ) -> Option<Self> {
        partitions
            .get(&(OFFSETS_TOPIC.to_string(), OFFSETS_PARTITION))
            .map(|e| Self {
                partition: e.value().clone(),
            })
    }
}

#[async_trait]
impl OffsetsLog for ProductionOffsetsLog {
    async fn append(&self, batch: RecordBatch) -> Result<(), BrokerError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        if self
            .partition
            .writer_tx
            .send(WriterMessage::Produce(ProduceJob { batch, ack: ack_tx }))
            .await
            .is_err()
        {
            return Err(BrokerError::Internal(
                "offsets partition writer dropped".into(),
            ));
        }
        match ack_rx.await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(BrokerError::Internal(
                "offsets writer ack channel closed".into(),
            )),
        }
    }
}

pub mod fake {
    use super::*;
    use tokio::sync::Mutex;

    #[derive(Debug, Default)]
    pub struct InMemoryOffsetsLog {
        pub appended: Mutex<Vec<RecordBatch>>,
        pub fail_next: std::sync::atomic::AtomicBool,
    }

    #[async_trait]
    impl OffsetsLog for InMemoryOffsetsLog {
        async fn append(&self, batch: RecordBatch) -> Result<(), BrokerError> {
            if self
                .fail_next
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(BrokerError::Internal("test-injected failure".into()));
            }
            self.appended.lock().await.push(batch);
            Ok(())
        }
    }

    impl InMemoryOffsetsLog {
        pub async fn batches(&self) -> Vec<RecordBatch> {
            self.appended.lock().await.clone()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fake_records_in_order() {
        let log = fake::InMemoryOffsetsLog::default();
        let b1 = RecordBatch::default();
        let mut b2 = RecordBatch::default();
        b2.max_timestamp = 42;
        log.append(b1.clone()).await.unwrap();
        log.append(b2.clone()).await.unwrap();
        let got = log.batches().await;
        assert_eq!(got.len(), 2);
        assert_eq!(got[1].max_timestamp, 42);
    }

    #[tokio::test]
    async fn fake_fails_when_armed() {
        let log = fake::InMemoryOffsetsLog::default();
        log.fail_next
            .store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(log.append(RecordBatch::default()).await.is_err());
        assert!(log.append(RecordBatch::default()).await.is_ok());
    }
}
```

- [ ] **Step 1.2: Module wiring**

In `crates/broker/src/coordinator/next_gen/mod.rs`, add `pub mod offsets_log;` near the other `pub mod ...` lines.

- [ ] **Step 1.3: Verify `BrokerError::Internal` accepts a `String`**

Read `crates/broker/src/error.rs` to confirm the variant. If `BrokerError::Internal` takes a `&'static str` or a `String`, adapt the literal calls (`"..."` vs `"...".into()`). If no `Internal` variant exists, use whatever the codebase already uses for generic "internal failure" (likely `BrokerError::Internal(String)` or `BrokerError::Other(String)`). Adjust both calls in `offsets_log.rs` accordingly.

- [ ] **Step 1.4: Build + test + commit**

```bash
cargo build -p crabka-broker
cargo test -p crabka-broker --lib coordinator::next_gen::offsets_log
git add crates/broker/src/coordinator/next_gen/
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "broker(coordinator/next_gen): OffsetsLog trait + production + fake"
```

Expected: 2 tests pass.

---

## Task 2 — Wire `OffsetsLog` + `seeds_cache` through NextGenCoordinator and GroupActorHandle

**Files:**
- Modify: `crates/broker/src/coordinator/next_gen/mod.rs`
- Modify: `crates/broker/src/coordinator/next_gen/group_actor.rs`
- Modify: `crates/broker/src/broker.rs`

- [ ] **Step 2.1: Extend `NextGenCoordinator` with `offsets_log` + `seeds_cache`**

In `crates/broker/src/coordinator/next_gen/mod.rs`:

1. Add imports near the top:

```rust
use offsets_log::OffsetsLog;
```

2. Replace the existing `NextGenCoordinator` struct definition with:

```rust
#[derive(Debug)]
pub struct NextGenCoordinator {
    pub config: Arc<NextGenConfig>,
    pub metadata: Arc<dyn MetadataProvider>,
    pub offsets_log: Arc<dyn OffsetsLog>,
    pub groups: Arc<DashMap<String, Arc<GroupActorHandle>>>,
    pub group_types: Arc<DashMap<String, GroupType>>,
    /// Bootstrap-time accumulator; drained by `finalize_bootstrap`.
    pub seeds: Arc<DashMap<String, GroupSeed>>,
    /// Last-known-good state per group, populated alongside every
    /// successful actor write. Used to seed a fresh actor when the
    /// previous instance crashed after a log-write failure.
    pub seeds_cache: Arc<DashMap<String, GroupSeed>>,
}
```

3. Replace `NextGenCoordinator::new` with:

```rust
impl NextGenCoordinator {
    pub fn new(
        config: NextGenConfig,
        metadata: Arc<dyn MetadataProvider>,
        offsets_log: Arc<dyn OffsetsLog>,
    ) -> Self {
        Self {
            config: Arc::new(config),
            metadata,
            offsets_log,
            groups: Arc::new(DashMap::new()),
            group_types: Arc::new(DashMap::new()),
            seeds: Arc::new(DashMap::new()),
            seeds_cache: Arc::new(DashMap::new()),
        }
    }
    // ... keep all other methods unchanged for now; we'll modify
    //     get_or_create in step 2.4.
}
```

4. Add a cache-update method on the same impl block:

```rust
    /// Replace the cached seed for `group_id` with `seed`. Called by the
    /// actor after every successful `OffsetsLog::append`.
    pub fn update_cache(&self, group_id: &str, seed: GroupSeed) {
        self.seeds_cache.insert(group_id.into(), seed);
    }

    /// Fetch the most recently cached seed for `group_id`, if any.
    pub fn cached_seed(&self, group_id: &str) -> Option<GroupSeed> {
        self.seeds_cache.get(group_id).map(|e| clone_seed(e.value()))
    }
```

5. Add a free function (since `GroupSeed` doesn't derive Clone — confirm before writing) at the bottom of `mod.rs`:

```rust
fn clone_seed(s: &GroupSeed) -> GroupSeed {
    GroupSeed {
        group_epoch: s.group_epoch,
        target_epoch: s.target_epoch,
        members: s.members.clone(),
        target_per_member: s.target_per_member.clone(),
        current_per_member: s.current_per_member.clone(),
    }
}
```

Alternatively: derive `Clone` on `GroupSeed`. Confirm by reading the existing struct — `MemberMetadataValue`, `TargetAssignmentMemberValue`, `CurrentMemberAssignmentValue` all derive `Clone` already, so deriving `Clone` on `GroupSeed` is the cleaner approach. Use:

```rust
#[derive(Debug, Default, Clone)]
pub struct GroupSeed {
    pub group_epoch: i32,
    pub target_epoch: i32,
    pub members: std::collections::HashMap<String, persistence::MemberMetadataValue>,
    pub target_per_member: std::collections::HashMap<String, persistence::TargetAssignmentMemberValue>,
    pub current_per_member: std::collections::HashMap<String, persistence::CurrentMemberAssignmentValue>,
}
```

Drop the `clone_seed` helper. `cached_seed` becomes:

```rust
    pub fn cached_seed(&self, group_id: &str) -> Option<GroupSeed> {
        self.seeds_cache.get(group_id).map(|e| e.value().clone())
    }
```

- [ ] **Step 2.2: Extend `GroupActorHandle::spawn` signature**

In `crates/broker/src/coordinator/next_gen/group_actor.rs`:

1. Add imports:

```rust
use super::offsets_log::OffsetsLog;
```

2. Replace `GroupActorHandle::spawn` with:

```rust
impl GroupActorHandle {
    pub fn spawn(
        group_id: String,
        config: Arc<NextGenConfig>,
        metadata_provider: Arc<dyn MetadataProvider>,
        offsets_log: Arc<dyn OffsetsLog>,
        coordinator: Arc<super::NextGenCoordinator>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(64);
        let task = tokio::spawn(actor_loop(
            group_id,
            config,
            metadata_provider,
            offsets_log,
            coordinator,
            rx,
        ));
        Self { tx, _task: task }
    }
}
```

3. Update `actor_loop` signature to accept the two new params:

```rust
async fn actor_loop(
    group_id: String,
    config: Arc<NextGenConfig>,
    metadata: Arc<dyn MetadataProvider>,
    offsets_log: Arc<dyn OffsetsLog>,
    coordinator: Arc<super::NextGenCoordinator>,
    mut rx: mpsc::Receiver<GroupActorMessage>,
) {
    // existing body unchanged for now — Tasks 4–7 wire writes into the body.
    let mut state = GroupState::new(group_id);
    let mut tick = tokio::time::interval(config.heartbeat_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            // ... unchanged ...
        }
    }
}
```

(Body unchanged in this task — Tasks 4–7 will modify the body.)

- [ ] **Step 2.3: Update `NextGenCoordinator::get_or_create` to pass the new args**

```rust
    pub fn get_or_create(self: &Arc<Self>, group_id: &str) -> Arc<GroupActorHandle> {
        if let Some(h) = self.groups.get(group_id) {
            // Dead-actor detection: if the mpsc sender is closed, the actor
            // has exited (typically after a log-write failure). Drop the
            // entry and fall through to spawn a fresh actor.
            if !h.value().tx.is_closed() {
                return h.value().clone();
            }
            drop(h);
            self.groups.remove(group_id);
        }
        let h = Arc::new(GroupActorHandle::spawn(
            group_id.into(),
            self.config.clone(),
            self.metadata.clone(),
            self.offsets_log.clone(),
            self.clone(),
        ));
        let inserted = self.groups
            .entry(group_id.into())
            .or_insert(h)
            .value()
            .clone();
        // Seed the new actor if we have cached state.
        if let Some(seed) = self.cached_seed(group_id) {
            let _ = inserted.tx.try_send(GroupActorMessage::Seed(seed));
        }
        inserted
    }
```

Note the receiver type changed from `&self` to `self: &Arc<Self>` so we can clone the `Arc` into the spawned actor. Every call site of `get_or_create` needs to be on an `Arc<NextGenCoordinator>` — verify with `grep -rn "next_gen()\." crates/broker/src/`. All current call sites already work with `Arc<NextGenCoordinator>` via `group_manager.next_gen()` returning `Option<&Arc<NextGenCoordinator>>`.

- [ ] **Step 2.4: Update `Broker::start` to pass `offsets_log` to `NextGenCoordinator::new`**

In `crates/broker/src/broker.rs`, find the existing `NextGenCoordinator::new(...)` call (around line 1037) and replace with:

```rust
        let offsets_log: std::sync::Arc<dyn crate::coordinator::next_gen::offsets_log::OffsetsLog> =
            match crate::coordinator::next_gen::offsets_log::ProductionOffsetsLog::from_partitions(&partitions) {
                Some(p) => std::sync::Arc::new(p),
                None => {
                    tracing::warn!(
                        "__consumer_offsets-0 not present at NextGenCoordinator construction; \
                         next-gen group state will be in-memory only until bootstrap completes"
                    );
                    std::sync::Arc::new(
                        crate::coordinator::next_gen::offsets_log::fake::InMemoryOffsetsLog::default(),
                    )
                }
            };
        let next_gen_coord = std::sync::Arc::new(
            crate::coordinator::next_gen::NextGenCoordinator::new(
                config.next_gen_consumer_group.clone(),
                std::sync::Arc::new(crate::coordinator::next_gen::ImageMetadataProvider {
                    controller: controller.clone(),
                }),
                offsets_log,
            ),
        );
        group_manager.set_next_gen(next_gen_coord);
```

The fallback to `InMemoryOffsetsLog` is the same shape the actor will use in tests; it produces no durability but lets the broker boot cleanly. In production, `__consumer_offsets-0` is created early enough that the real path is taken. Confirm by checking ordering: the partitions map is populated by `bootstrap.rs` which runs AFTER `Broker::start` constructs `partitions`. If `__consumer_offsets-0` registration happens before NextGenCoordinator construction (look for where `__consumer_offsets` is registered into `partitions`), the real path wins; otherwise the warn fires. Either way the broker boots.

- [ ] **Step 2.5: Fix all callers of `NextGenCoordinator::new` in tests**

`grep -rn "NextGenCoordinator::new" crates/` — adjust any test call sites to pass an `InMemoryOffsetsLog`. Likely zero call sites outside `Broker::start` exist (NextGenCoordinator is internal), but verify.

- [ ] **Step 2.6: Build + commit**

```bash
cargo build -p crabka-broker
git add crates/broker/src/coordinator/next_gen/ crates/broker/src/broker.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "broker(coordinator/next_gen): thread OffsetsLog + seeds_cache through NextGenCoordinator"
```

Expected: build green; tests still pass (no behavior change yet).

---

## Task 3 — Populate `seeds_cache` during bootstrap replay

**Files:**
- Modify: `crates/broker/src/coordinator/next_gen/mod.rs`

- [ ] **Step 3.1: Mirror writes into seeds_cache during the replay path**

The existing `replay_*` methods update `seeds`. Modify each so it also updates `seeds_cache` with the same value. Pattern: change every method like:

```rust
    pub fn replay_member_metadata(&self, group_id: &str, member_id: &str, v: persistence::MemberMetadataValue) {
        let mut seed = self.seeds.entry(group_id.into()).or_default();
        seed.members.insert(member_id.into(), v.clone());
        drop(seed);
        let mut cached = self.seeds_cache.entry(group_id.into()).or_default();
        cached.members.insert(member_id.into(), v);
    }
```

Apply the same dual-update to all five replay methods (`replay_group_metadata`, `replay_member_metadata`, `replay_target_assignment_metadata`, `replay_target_assignment_member`, `replay_current_member_assignment`).

The `drop(seed)` is required because both `seeds` and `seeds_cache` are `DashMap`s and dashmap's reference-mut would deadlock if held across the second `entry` call.

- [ ] **Step 3.2: Build + commit**

```bash
cargo build -p crabka-broker
git add crates/broker/src/coordinator/next_gen/mod.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "broker(coordinator/next_gen): mirror bootstrap replay into seeds_cache"
```

---

## Task 4 — Record-emission helper in group_actor

**Files:**
- Modify: `crates/broker/src/coordinator/next_gen/group_actor.rs`

We need a helper that, given a "what changed" set, produces a `RecordBatch` of the affected k3/k5/k6/k7/k8 records. Keep it data-driven: the helper takes references to the GroupState fields it should serialize and returns a `RecordBatch`.

- [ ] **Step 4.1: Add the encoder helper**

Append to `group_actor.rs`:

```rust
use bytes::Bytes;
use crabka_protocol::records::{Record, RecordBatch};

use super::persistence::{
    encode_key, AssignedTopicPartitions, CurrentMemberAssignmentValue, GroupMetadataValue,
    MemberAssignmentState, MemberMetadataValue, NextGenKey, TargetAssignmentMemberValue,
    TargetAssignmentMetadataValue,
};

#[derive(Debug, Default)]
pub(crate) struct PendingRecords {
    pub group_metadata: Option<GroupMetadataValue>,
    /// `Some(value)` writes the record; `None` writes a tombstone (null value).
    pub member_metadata: Vec<(String, Option<MemberMetadataValue>)>,
    pub target_metadata: Option<TargetAssignmentMetadataValue>,
    pub target_per_member: Vec<(String, Option<TargetAssignmentMemberValue>)>,
    pub current_per_member: Vec<(String, Option<CurrentMemberAssignmentValue>)>,
}

impl PendingRecords {
    pub fn is_empty(&self) -> bool {
        self.group_metadata.is_none()
            && self.member_metadata.is_empty()
            && self.target_metadata.is_none()
            && self.target_per_member.is_empty()
            && self.current_per_member.is_empty()
    }

    pub fn into_batch(self, group_id: &str, now_ms: i64) -> RecordBatch {
        let mut records: Vec<Record> = Vec::new();
        let mut push = |key: Bytes, value: Option<Bytes>| {
            let delta = i32::try_from(records.len()).expect("batch size fits i32");
            records.push(Record {
                offset_delta: delta,
                timestamp_delta: 0,
                key: Some(key),
                value,
                ..Default::default()
            });
        };

        if let Some(v) = self.group_metadata {
            push(
                encode_key(&NextGenKey::GroupMetadata {
                    group_id: group_id.into(),
                }),
                Some(v.encode()),
            );
        }
        for (member_id, v) in self.member_metadata {
            push(
                encode_key(&NextGenKey::MemberMetadata {
                    group_id: group_id.into(),
                    member_id,
                }),
                v.map(|x| x.encode()),
            );
        }
        if let Some(v) = self.target_metadata {
            push(
                encode_key(&NextGenKey::TargetAssignmentMetadata {
                    group_id: group_id.into(),
                }),
                Some(v.encode()),
            );
        }
        for (member_id, v) in self.target_per_member {
            push(
                encode_key(&NextGenKey::TargetAssignmentMember {
                    group_id: group_id.into(),
                    member_id,
                }),
                v.map(|x| x.encode()),
            );
        }
        for (member_id, v) in self.current_per_member {
            push(
                encode_key(&NextGenKey::CurrentMemberAssignment {
                    group_id: group_id.into(),
                    member_id,
                }),
                v.map(|x| x.encode()),
            );
        }

        let last_delta = i32::try_from(records.len().saturating_sub(1)).unwrap_or(0);
        RecordBatch {
            max_timestamp: now_ms,
            records,
            last_offset_delta: last_delta,
            ..RecordBatch::default()
        }
    }
}
```

- [ ] **Step 4.2: Add a function to snapshot a GroupState into a `GroupSeed`**

```rust
pub(crate) fn snapshot_seed(state: &GroupState) -> super::GroupSeed {
    use crate::coordinator::next_gen::persistence as p;
    let mut members = std::collections::HashMap::new();
    let mut target_per_member = std::collections::HashMap::new();
    let mut current_per_member = std::collections::HashMap::new();
    for (mid, m) in &state.members {
        let mm = p::MemberMetadataValue {
            instance_id: m.instance_id.clone(),
            rack_id: m.rack_id.clone(),
            client_id: m.client_id.clone(),
            client_host: m.client_host.clone(),
            subscribed_topic_names: m.subscribed_topic_names.iter().cloned().collect(),
            server_assignor: m.server_assignor.clone(),
            rebalance_timeout_ms: i32::try_from(m.rebalance_timeout.as_millis()).unwrap_or(60_000),
        };
        members.insert(mid.clone(), mm);

        let cur = p::CurrentMemberAssignmentValue {
            member_epoch: m.member_epoch,
            previous_member_epoch: m.previous_member_epoch,
            state: m.assignment_state,
            assigned_partitions: m
                .assigned_partitions
                .iter()
                .map(|(tid, parts)| p::AssignedTopicPartitions {
                    topic_id: *tid,
                    partitions: parts.clone(),
                })
                .collect(),
            partitions_pending_revocation: m
                .partitions_pending_revocation
                .iter()
                .map(|(tid, parts)| p::AssignedTopicPartitions {
                    topic_id: *tid,
                    partitions: parts.clone(),
                })
                .collect(),
        };
        current_per_member.insert(mid.clone(), cur);

        if let Some(target) = state.target.per_member.get(mid) {
            let tv = p::TargetAssignmentMemberValue {
                topic_partitions: target
                    .iter()
                    .map(|(tid, parts)| p::AssignedTopicPartitions {
                        topic_id: *tid,
                        partitions: parts.clone(),
                    })
                    .collect(),
            };
            target_per_member.insert(mid.clone(), tv);
        }
    }
    super::GroupSeed {
        group_epoch: state.group_epoch,
        target_epoch: state.target.epoch,
        members,
        target_per_member,
        current_per_member,
    }
}
```

- [ ] **Step 4.3: Unit test the helper**

Add to `group_actor.rs` (inside `#[cfg(test)] mod tests`):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_records_empty_yields_empty_batch() {
        let p = PendingRecords::default();
        let batch = p.into_batch("g", 0);
        assert!(batch.records.is_empty());
    }

    #[test]
    fn pending_records_offset_deltas_are_sequential() {
        let p = PendingRecords {
            group_metadata: Some(GroupMetadataValue { epoch: 1 }),
            member_metadata: vec![("m1".into(), Some(MemberMetadataValue {
                instance_id: None,
                rack_id: None,
                client_id: "c".into(),
                client_host: "h".into(),
                subscribed_topic_names: vec!["t".into()],
                server_assignor: None,
                rebalance_timeout_ms: 60_000,
            }))],
            target_metadata: Some(TargetAssignmentMetadataValue { assignment_epoch: 1 }),
            ..Default::default()
        };
        let batch = p.into_batch("g", 0);
        assert_eq!(batch.records.len(), 3);
        let deltas: Vec<i32> = batch.records.iter().map(|r| r.offset_delta).collect();
        assert_eq!(deltas, vec![0, 1, 2]);
        assert_eq!(batch.last_offset_delta, 2);
    }

    #[test]
    fn pending_records_tombstone_omits_value() {
        let p = PendingRecords {
            member_metadata: vec![("m1".into(), None)],
            ..Default::default()
        };
        let batch = p.into_batch("g", 0);
        assert_eq!(batch.records.len(), 1);
        assert!(batch.records[0].value.is_none());
    }
}
```

- [ ] **Step 4.4: Run + commit**

```bash
cargo test -p crabka-broker --lib coordinator::next_gen::group_actor
git add crates/broker/src/coordinator/next_gen/group_actor.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "broker(coordinator/next_gen): PendingRecords encoder + snapshot_seed helper"
```

Expected: 3 new tests pass.

---

## Task 5 — Wire persistence writes into `handle_heartbeat`

**Files:**
- Modify: `crates/broker/src/coordinator/next_gen/group_actor.rs`

We're changing `handle_heartbeat` from synchronous (`fn`) to async (`async fn`) so it can await `OffsetsLog::append`. We also pass `offsets_log` and `coordinator` references.

- [ ] **Step 5.1: Convert `handle_heartbeat` to async + thread the dependencies**

Replace the current `handle_heartbeat` signature with:

```rust
async fn handle_heartbeat(
    state: &mut GroupState,
    config: &NextGenConfig,
    metadata: &dyn MetadataProvider,
    offsets_log: &dyn OffsetsLog,
    coordinator: &super::NextGenCoordinator,
    req: &ConsumerGroupHeartbeatRequest,
    client_host: &str,
) -> Result<ConsumerGroupHeartbeatResponse, BrokerError> {
    let now = Instant::now();
    let now_ms = chrono_now_ms();

    // ─── Leave path ─────────────────────────────────────────────
    if req.member_epoch == -1 {
        let mut pending = PendingRecords::default();
        if let Some(m) = state.members.get(&req.member_id) {
            // Tombstone all records for this member.
            pending.member_metadata.push((req.member_id.clone(), None));
            pending
                .target_per_member
                .push((req.member_id.clone(), None));
            pending
                .current_per_member
                .push((req.member_id.clone(), None));
        }
        state.remove_member(&req.member_id);
        state.bump_epoch();
        pending.group_metadata = Some(GroupMetadataValue {
            epoch: state.group_epoch,
        });
        flush_pending(state, &pending, offsets_log, coordinator, now_ms).await?;
        return Ok(base_resp(0, req.member_epoch, config));
    }

    // ─── Validate assignor selection ──────────────────────────────
    if let Some(name) = req.server_assignor.as_deref() {
        if !config.assignor_enabled(name) {
            return Ok(error_resp(codes::UNSUPPORTED_ASSIGNOR, config));
        }
    }

    // ─── First-join path ─────────────────────────────────────────
    if req.member_epoch == 0 && req.member_id.is_empty() {
        let new_member_id = uuid::Uuid::new_v4().to_string();
        if let Some(iid) = req.instance_id.as_deref() {
            if let Some(existing) = state.current_member_for_instance(iid) {
                if state
                    .members
                    .get(existing)
                    .is_some_and(|m| m.member_epoch != 0)
                {
                    return Ok(error_resp(codes::UNRELEASED_INSTANCE_ID, config));
                }
            }
        }
        let m = build_member(&new_member_id, req, client_host, now);
        state.add_or_update_member(m);
        run_reconcile(state, config, metadata);
        state.advance_member_epoch(&new_member_id);
        let pending = snapshot_pending_after_change(state, &[new_member_id.clone()]);
        flush_pending(state, &pending, offsets_log, coordinator, now_ms).await?;
        return Ok(build_assignment_resp(state, &new_member_id, config));
    }

    // ─── Existing-member: validate epoch ─────────────────────────
    let cur_epoch = state
        .members
        .get(&req.member_id)
        .map(|m| m.member_epoch)
        .unwrap_or(-2);
    if cur_epoch == -2 {
        return Ok(error_resp(codes::UNKNOWN_MEMBER_ID, config));
    }
    if req.member_epoch < cur_epoch {
        return Ok(error_resp(codes::STALE_MEMBER_EPOCH, config));
    }
    if req.member_epoch > cur_epoch {
        return Ok(error_resp(codes::FENCED_MEMBER_EPOCH, config));
    }

    // ─── Steady-state: update last_seen / subscription / owned ───
    let mut subscription_changed = false;
    if let Some(m) = state.members.get_mut(&req.member_id) {
        m.last_seen = now;
        if let Some(ref names) = req.subscribed_topic_names {
            let set: std::collections::HashSet<String> = names.iter().cloned().collect();
            if set != m.subscribed_topic_names {
                m.subscribed_topic_names = set;
                state.dirty = true;
                subscription_changed = true;
            }
        }
        if let Some(ref tp) = req.topic_partitions {
            let owned: HashMap<Uuid, Vec<i32>> = tp
                .iter()
                .map(|t| (t.topic_id, t.partitions.clone()))
                .collect();
            m.assigned_partitions = owned;
            if m.partitions_pending_revocation.is_empty() {
                m.assignment_state = MemberAssignmentState::Stable;
            }
        }
    }
    let was_dirty = state.dirty;
    run_reconcile(state, config, metadata);
    let epoch_advanced = state.target.epoch > cur_epoch;
    if epoch_advanced {
        state.advance_member_epoch(&req.member_id);
    }
    let any_change = subscription_changed || was_dirty || epoch_advanced;
    if any_change {
        let pending = snapshot_pending_after_change(state, &[req.member_id.clone()]);
        flush_pending(state, &pending, offsets_log, coordinator, now_ms).await?;
    }
    Ok(build_assignment_resp(state, &req.member_id, config))
}
```

- [ ] **Step 5.2: Add the supporting helpers**

```rust
fn chrono_now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(0))
        .unwrap_or(0)
}

/// Build a `PendingRecords` set reflecting the state changes for the
/// listed `affected_members`. Always includes the current group epoch
/// and (if non-zero) target epoch.
fn snapshot_pending_after_change(
    state: &GroupState,
    affected_members: &[String],
) -> PendingRecords {
    use crate::coordinator::next_gen::persistence as p;
    let mut pending = PendingRecords::default();
    pending.group_metadata = Some(p::GroupMetadataValue {
        epoch: state.group_epoch,
    });
    if state.target.epoch > 0 {
        pending.target_metadata = Some(p::TargetAssignmentMetadataValue {
            assignment_epoch: state.target.epoch,
        });
    }
    for mid in affected_members {
        if let Some(m) = state.members.get(mid) {
            pending.member_metadata.push((
                mid.clone(),
                Some(p::MemberMetadataValue {
                    instance_id: m.instance_id.clone(),
                    rack_id: m.rack_id.clone(),
                    client_id: m.client_id.clone(),
                    client_host: m.client_host.clone(),
                    subscribed_topic_names: m.subscribed_topic_names.iter().cloned().collect(),
                    server_assignor: m.server_assignor.clone(),
                    rebalance_timeout_ms: i32::try_from(m.rebalance_timeout.as_millis())
                        .unwrap_or(60_000),
                }),
            ));
            pending.current_per_member.push((
                mid.clone(),
                Some(p::CurrentMemberAssignmentValue {
                    member_epoch: m.member_epoch,
                    previous_member_epoch: m.previous_member_epoch,
                    state: m.assignment_state,
                    assigned_partitions: m
                        .assigned_partitions
                        .iter()
                        .map(|(tid, parts)| p::AssignedTopicPartitions {
                            topic_id: *tid,
                            partitions: parts.clone(),
                        })
                        .collect(),
                    partitions_pending_revocation: m
                        .partitions_pending_revocation
                        .iter()
                        .map(|(tid, parts)| p::AssignedTopicPartitions {
                            topic_id: *tid,
                            partitions: parts.clone(),
                        })
                        .collect(),
                }),
            ));
            if let Some(target) = state.target.per_member.get(mid) {
                pending.target_per_member.push((
                    mid.clone(),
                    Some(p::TargetAssignmentMemberValue {
                        topic_partitions: target
                            .iter()
                            .map(|(tid, parts)| p::AssignedTopicPartitions {
                                topic_id: *tid,
                                partitions: parts.clone(),
                            })
                            .collect(),
                    }),
                ));
            }
        }
    }
    pending
}

async fn flush_pending(
    state: &GroupState,
    pending: &PendingRecords,
    offsets_log: &dyn OffsetsLog,
    coordinator: &super::NextGenCoordinator,
    now_ms: i64,
) -> Result<(), BrokerError> {
    if pending.is_empty() {
        return Ok(());
    }
    let batch = pending.clone_into_batch(&state.group_id, now_ms);
    offsets_log.append(batch).await?;
    coordinator.update_cache(&state.group_id, snapshot_seed(state));
    Ok(())
}
```

Note: `PendingRecords::into_batch` consumes `self`; we need a clone-based variant since `flush_pending` takes `&PendingRecords`. Add an alternate API on `PendingRecords`:

```rust
impl PendingRecords {
    pub fn clone_into_batch(&self, group_id: &str, now_ms: i64) -> RecordBatch {
        // Same body as into_batch but operates on references.
        // Simplest: clone self and call into_batch.
        self.clone().into_batch(group_id, now_ms)
    }
}

// And derive Clone:
#[derive(Debug, Default, Clone)]
pub(crate) struct PendingRecords { ... }
```

Update the derive above accordingly.

- [ ] **Step 5.3: Update `actor_loop` to await `handle_heartbeat` and exit on error**

In the `actor_loop` match arm for `GroupActorMessage::Heartbeat`:

```rust
                    GroupActorMessage::Heartbeat { request, client_host, reply } => {
                        match handle_heartbeat(
                            &mut state,
                            &config,
                            &*metadata,
                            &*offsets_log,
                            &coordinator,
                            &request,
                            &client_host,
                        )
                        .await
                        {
                            Ok(resp) => {
                                let _ = reply.send(resp);
                            }
                            Err(e) => {
                                tracing::warn!(
                                    group_id = %state.group_id,
                                    error = %e,
                                    "next-gen actor exiting after log-write failure",
                                );
                                let _ = reply.send(ConsumerGroupHeartbeatResponse {
                                    error_code: codes::COORDINATOR_LOAD_IN_PROGRESS,
                                    ..Default::default()
                                });
                                break;
                            }
                        }
                    }
```

- [ ] **Step 5.4: Build + commit**

```bash
cargo build -p crabka-broker
cargo test -p crabka-broker --lib coordinator::next_gen
git add crates/broker/src/coordinator/next_gen/group_actor.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "broker(coordinator/next_gen): persist mutations from handle_heartbeat to OffsetsLog"
```

Expected: build green; all 34 existing next_gen unit tests still pass (helpers don't change behavior of pure-state code paths).

---

## Task 6 — Persist on session-timeout eviction tick

**Files:**
- Modify: `crates/broker/src/coordinator/next_gen/group_actor.rs`

- [ ] **Step 6.1: Convert the tick arm to perform a write**

In `actor_loop`'s `tokio::select!`, replace the tick arm body:

```rust
            _ = tick.tick() => {
                let evicted = state.evict_expired(Instant::now(), config.session_timeout);
                if !evicted.is_empty() {
                    state.bump_epoch();
                    run_reconcile(&mut state, &config, &*metadata);
                    let mut pending = PendingRecords::default();
                    pending.group_metadata = Some(GroupMetadataValue {
                        epoch: state.group_epoch,
                    });
                    if state.target.epoch > 0 {
                        pending.target_metadata = Some(TargetAssignmentMetadataValue {
                            assignment_epoch: state.target.epoch,
                        });
                    }
                    for mid in &evicted {
                        pending.member_metadata.push((mid.clone(), None));
                        pending.target_per_member.push((mid.clone(), None));
                        pending.current_per_member.push((mid.clone(), None));
                    }
                    // Also include survivors whose target changed.
                    let now_ms = chrono_now_ms();
                    if let Err(e) = flush_pending(&state, &pending, &*offsets_log, &coordinator, now_ms).await {
                        tracing::warn!(
                            group_id = %state.group_id,
                            error = %e,
                            "next-gen actor exiting after tick log-write failure",
                        );
                        break;
                    }
                }
            }
```

- [ ] **Step 6.2: Build + commit**

```bash
cargo build -p crabka-broker
git add crates/broker/src/coordinator/next_gen/group_actor.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "broker(coordinator/next_gen): persist session-timeout evictions"
```

Expected: build green.

---

## Task 7 — Unit-test the persistence behavior

**Files:**
- Modify: `crates/broker/src/coordinator/next_gen/group_actor.rs` (test module)

- [ ] **Step 7.1: Add integration-style unit tests using the fake**

Add to the existing test module in `group_actor.rs`:

```rust
    use crate::coordinator::next_gen::config::NextGenConfig;
    use crate::coordinator::next_gen::offsets_log::fake::InMemoryOffsetsLog;
    use crate::coordinator::next_gen::reconciler::ReconcileInput;
    use crate::coordinator::next_gen::NextGenCoordinator;
    use std::sync::Arc;

    #[derive(Debug)]
    struct StaticMetadata {
        input: ReconcileInput,
    }
    impl MetadataProvider for StaticMetadata {
        fn snapshot(&self) -> ReconcileInput {
            self.input.clone()
        }
    }

    fn empty_metadata() -> Arc<dyn MetadataProvider> {
        Arc::new(StaticMetadata {
            input: ReconcileInput::default(),
        })
    }

    async fn make_actor() -> (
        Arc<NextGenCoordinator>,
        Arc<InMemoryOffsetsLog>,
        Arc<GroupActorHandle>,
    ) {
        let log = Arc::new(InMemoryOffsetsLog::default());
        let coord = Arc::new(NextGenCoordinator::new(
            NextGenConfig::default(),
            empty_metadata(),
            log.clone(),
        ));
        let handle = coord.get_or_create("g");
        (coord, log, handle)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn first_join_emits_one_batch() {
        let (_coord, log, handle) = make_actor().await;
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::Heartbeat {
                request: ConsumerGroupHeartbeatRequest {
                    group_id: "g".into(),
                    member_id: String::new(),
                    member_epoch: 0,
                    subscribed_topic_names: Some(vec!["t".into()]),
                    rebalance_timeout_ms: 60_000,
                    ..Default::default()
                },
                client_host: String::new(),
                reply: tx,
            })
            .await
            .unwrap();
        let resp = rx.await.unwrap();
        assert_eq!(resp.error_code, 0);
        let batches = log.batches().await;
        assert_eq!(batches.len(), 1, "first join should write exactly one batch");
        // At minimum: k3 (group metadata) + k5 (member metadata) + k8 (current).
        // k6/k7 may also be present if reconciliation produced a target.
        assert!(batches[0].records.len() >= 3);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unchanged_heartbeat_emits_no_batch() {
        let (_coord, log, handle) = make_actor().await;
        // First join.
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::Heartbeat {
                request: ConsumerGroupHeartbeatRequest {
                    group_id: "g".into(),
                    member_id: String::new(),
                    member_epoch: 0,
                    subscribed_topic_names: Some(vec!["t".into()]),
                    rebalance_timeout_ms: 60_000,
                    ..Default::default()
                },
                client_host: String::new(),
                reply: tx,
            })
            .await
            .unwrap();
        let resp1 = rx.await.unwrap();
        let mid = resp1.member_id.clone().unwrap();
        let batches_after_join = log.batches().await.len();

        // Same subscription, advanced epoch — no change.
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::Heartbeat {
                request: ConsumerGroupHeartbeatRequest {
                    group_id: "g".into(),
                    member_id: mid,
                    member_epoch: resp1.member_epoch,
                    subscribed_topic_names: Some(vec!["t".into()]),
                    rebalance_timeout_ms: 60_000,
                    ..Default::default()
                },
                client_host: String::new(),
                reply: tx,
            })
            .await
            .unwrap();
        let _ = rx.await.unwrap();
        let batches_after_steady = log.batches().await.len();
        assert_eq!(
            batches_after_steady, batches_after_join,
            "steady-state heartbeat should not write"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn leave_emits_tombstone_batch() {
        let (_coord, log, handle) = make_actor().await;
        // Join.
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::Heartbeat {
                request: ConsumerGroupHeartbeatRequest {
                    group_id: "g".into(),
                    member_id: String::new(),
                    member_epoch: 0,
                    subscribed_topic_names: Some(vec!["t".into()]),
                    rebalance_timeout_ms: 60_000,
                    ..Default::default()
                },
                client_host: String::new(),
                reply: tx,
            })
            .await
            .unwrap();
        let mid = rx.await.unwrap().member_id.unwrap();
        let pre_leave = log.batches().await.len();

        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::Heartbeat {
                request: ConsumerGroupHeartbeatRequest {
                    group_id: "g".into(),
                    member_id: mid,
                    member_epoch: -1,
                    ..Default::default()
                },
                client_host: String::new(),
                reply: tx,
            })
            .await
            .unwrap();
        let _ = rx.await.unwrap();
        let batches = log.batches().await;
        assert_eq!(batches.len(), pre_leave + 1);
        let leave_batch = &batches[batches.len() - 1];
        // Must contain at least one tombstone (None value).
        assert!(leave_batch.records.iter().any(|r| r.value.is_none()));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn actor_exits_on_append_error() {
        let (coord, log, handle) = make_actor().await;
        log.fail_next
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = handle
            .tx
            .send(GroupActorMessage::Heartbeat {
                request: ConsumerGroupHeartbeatRequest {
                    group_id: "g".into(),
                    member_id: String::new(),
                    member_epoch: 0,
                    subscribed_topic_names: Some(vec!["t".into()]),
                    rebalance_timeout_ms: 60_000,
                    ..Default::default()
                },
                client_host: String::new(),
                reply: tx,
            })
            .await;
        let resp = rx.await.unwrap();
        assert_eq!(resp.error_code, codes::COORDINATOR_LOAD_IN_PROGRESS);

        // Wait briefly for the actor to drain.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(handle.tx.is_closed(), "actor mpsc should be closed after exit");

        // get_or_create should respawn a fresh actor.
        let fresh = coord.get_or_create("g");
        assert!(!fresh.tx.is_closed());
    }
```

- [ ] **Step 7.2: Run + commit**

```bash
cargo test -p crabka-broker --lib coordinator::next_gen::group_actor
git add crates/broker/src/coordinator/next_gen/group_actor.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "test(broker): next-gen actor persistence + failure-recovery unit tests"
```

Expected: 4 new tests pass (plus the 3 from Task 4) = 7 new tests total.

---

## Task 8 — ApiVersions: advertise `group.version=1`

**Files:**
- Modify: `crates/broker/src/handlers/api_versions.rs`

- [ ] **Step 8.1: Populate `supported_features` + `finalized_features` when next-gen is on**

Find the line that constructs the success-path `ApiVersionsResponse` (where it currently uses `..Default::default()`). Replace the construction with:

```rust
        let next_gen_on = broker.config.next_gen_consumer_group.next_gen_enabled();
        let supported_features = if next_gen_on {
            vec![crabka_protocol::owned::api_versions_response::SupportedFeatureKey {
                name: "group.version".into(),
                min_version: 1,
                max_version: 1,
                ..Default::default()
            }]
        } else {
            vec![]
        };
        let finalized_features = if next_gen_on {
            vec![crabka_protocol::owned::api_versions_response::FinalizedFeatureKey {
                name: "group.version".into(),
                min_version_level: 1,
                max_version_level: 1,
                ..Default::default()
            }]
        } else {
            vec![]
        };
        let finalized_features_epoch: i64 = if next_gen_on { 0 } else { -1 };

        let resp = ApiVersionsResponse {
            error_code: codes::NONE,
            api_keys: supported_apis(),
            throttle_time_ms: 0,
            supported_features,
            finalized_features,
            finalized_features_epoch,
            ..Default::default()
        };
```

Don't touch the early-return for `INVALID_REQUEST`.

- [ ] **Step 8.2: Refresh any api_versions snapshot tests**

```bash
cargo test -p crabka-broker --lib handlers::api_versions 2>&1 | tail -20
```

If a snapshot fails, run once with `UPDATE_SNAPSHOTS=1` then re-run to confirm green:

```bash
UPDATE_SNAPSHOTS=1 cargo test -p crabka-broker --lib handlers::api_versions
cargo test -p crabka-broker --lib handlers::api_versions
```

- [ ] **Step 8.3: Build + commit**

```bash
cargo build -p crabka-broker
git add crates/broker/src/handlers/api_versions.rs crates/broker/src/handlers/api_versions/  # in case snapshots changed
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "broker(handlers): advertise group.version=1 in ApiVersions (KIP-584)"
```

Expected: api_versions tests green.

---

## Task 9 — Un-ignore the persistence integration tests

**Files:**
- Modify: `crates/broker/tests/consumer_group_next_gen_persistence.rs`

- [ ] **Step 9.1: Drop both `#[ignore]` attributes**

```bash
sed -i '' '/^#\[ignore = "next-gen persistence write not yet wired; tracked as 64a follow-up"\]/d' crates/broker/tests/consumer_group_next_gen_persistence.rs
grep -c "#\[ignore" crates/broker/tests/consumer_group_next_gen_persistence.rs
```

Expected: 0 remaining `#[ignore]` attributes.

- [ ] **Step 9.2: Run the persistence tests**

```bash
cargo test -p crabka-broker --test consumer_group_next_gen_persistence 2>&1 | tail -20
```

Expected: 2 tests pass.

- [ ] **Step 9.3: Commit**

```bash
git add crates/broker/tests/consumer_group_next_gen_persistence.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "test(broker): un-ignore KIP-848 persistence-replay tests"
```

---

## Task 10 — Un-ignore the JVM acceptance tests + restore CI

**Files:**
- Modify: `crates/broker/tests/jvm_consumer_group_next_gen.rs`
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 10.1: Drop all four `#[ignore]` attributes**

```bash
sed -i '' '/^#\[ignore = "next-gen client\/broker integration depth required; tracked as 64a follow-up"\]/d' crates/broker/tests/jvm_consumer_group_next_gen.rs
grep -c "#\[ignore" crates/broker/tests/jvm_consumer_group_next_gen.rs
```

Expected: 0 remaining `#[ignore]` attributes.

The plan author has confirmed these tests are correct in shape — only the `#[ignore]` lines are dropped. Don't touch the test bodies.

- [ ] **Step 10.2: Restore the JVM test binary to `cargo llvm-cov`**

In `.github/workflows/ci.yml`, find the `broker-jvm-acceptance` job's cargo invocation:

```yaml
          cargo llvm-cov -p crabka-broker \
            --test jvm_acceptance \
            --lcov --output-path coverage/broker-jvm-acceptance.lcov \
            -- --ignored --nocapture --test-threads=1
```

Replace with:

```yaml
          cargo llvm-cov -p crabka-broker \
            --test jvm_acceptance \
            --test jvm_consumer_group_next_gen \
            --lcov --output-path coverage/broker-jvm-acceptance.lcov \
            -- --ignored --nocapture --test-threads=1
```

But note: with `#[ignore]` dropped, the tests are no longer ignored. The `-- --ignored` flag in the workflow will *not* run them anymore (it only runs ignored tests). Options:

(A) Keep tests un-ignored and run BOTH the normal + ignored sweeps:
```yaml
          cargo llvm-cov --no-report -p crabka-broker --test jvm_consumer_group_next_gen
          cargo llvm-cov --no-report -p crabka-broker --test jvm_acceptance -- --ignored --nocapture --test-threads=1
          cargo llvm-cov report --lcov --output-path coverage/broker-jvm-acceptance.lcov
```

(B) Re-`#[ignore]` the JVM tests (keep them as opt-in) and trust the workflow's existing `-- --ignored` to pick them up. Same as before slice merged; just changed reason.

Take option **(A)** — the tests now genuinely require docker but no longer need `#[ignore]` as a "don't run in CI normally" marker, because they're explicitly named in the workflow's test list anyway. The persistence tests at step 9 ARE un-ignored and run in the normal `broker-integration` job (already covers `cargo llvm-cov --no-report -p crabka-broker --tests`), so they don't need CI changes.

Implementation: replace the broker-jvm-acceptance run block with:

```yaml
      - name: Broker JVM acceptance coverage
        run: |
          mkdir -p coverage
          cargo llvm-cov clean --workspace
          cargo llvm-cov --no-report -p crabka-broker --test jvm_consumer_group_next_gen -- --nocapture --test-threads=1
          cargo llvm-cov --no-report -p crabka-broker --test jvm_acceptance -- --ignored --nocapture --test-threads=1
          cargo llvm-cov report --lcov --output-path coverage/broker-jvm-acceptance.lcov
```

(The persistence test file runs in broker-integration; no extra CI plumbing needed.)

- [ ] **Step 10.3: Local smoke test — run the JVM tests locally if docker is available**

```bash
docker pull apache/kafka:4.0.0 2>&1 | tail -2
docker pull confluentinc/cp-kafka:7.5.0 2>&1 | tail -2
cargo test -p crabka-broker --test jvm_consumer_group_next_gen -- --nocapture --test-threads=1 2>&1 | tail -30
```

Expected: all 4 tests pass. If they don't pass, do NOT commit `-D warnings` and dig into the failure — the persistence + feature work is the root cause to verify. If they pass, commit.

- [ ] **Step 10.4: Commit**

```bash
git add crates/broker/tests/jvm_consumer_group_next_gen.rs .github/workflows/ci.yml
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "test(broker): un-ignore KIP-848 JVM acceptance + restore CI invocation"
```

Expected: 4 tests pass against `apache/kafka:4.0.0`.

---

## Final verification

- [ ] **F-1: Full workspace gate**

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

If clippy reports warnings on the new code, fix them. Common Crabka patterns:
- `clippy::manual_range_contains` → use `(a..b).contains(&x)`.
- `clippy::collapsible_if` → collapse.
- `clippy::needless_pass_by_value` → take reference.
- `clippy::map_unwrap_or` → use `.map_or()`.
- Long doc lines need backticks around identifiers.

Stage fmt/clippy fixes in a separate commit:

```bash
cargo fmt --all
git add -A
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "fmt+clippy: slice 64a-followup polish"
```

- [ ] **F-2: STATUS.md entry**

Append to `STATUS.md`:

```markdown
## Slice 64a follow-up — KIP-848 persistence + JVM-client gating (2026-05-28)

- New `coordinator/next_gen/offsets_log.rs` — `OffsetsLog` trait,
  `ProductionOffsetsLog` (wraps `Arc<Partition>`), `fake::InMemoryOffsetsLog`.
- `GroupActor` writes affected v3/v5/v6/v7/v8 records to `__consumer_offsets-0`
  as a single `RecordBatch` per mutation (join, leave, subscription change,
  reconciliation, session-timeout eviction). Writes happen before the
  heartbeat reply.
- On `OffsetsLog::append` failure, the actor exits and the next
  `NextGenCoordinator::get_or_create` call respawns a fresh actor seeded
  from a coordinator-owned `seeds_cache` populated by every successful write
  (mirrors the existing bootstrap-replay seed pipeline).
- `ApiVersions` now advertises `group.version=1` in both
  `supported_features` and `finalized_features` when next-gen is enabled —
  kafka-clients 4.0 needs this finalized feature (KIP-584) to engage
  KIP-848 instead of falling back to classic.
- Tests:
  - 7 new actor unit tests covering PendingRecords encoding, first-join
    write batching, unchanged-heartbeat no-op, leave-tombstone batching,
    actor-exit-on-write-failure.
  - 2 previously-ignored persistence-replay tests now passing.
  - 4 previously-ignored JVM-acceptance tests against `apache/kafka:4.0.0`
    now passing; restored to the `broker-jvm-acceptance` CI job.
- CI: `--test jvm_consumer_group_next_gen` re-added to the
  `broker-jvm-acceptance` cargo-llvm-cov invocation alongside
  `--test jvm_acceptance`.
```

Also drop the two "follow-up" bullets from slice 64a's out-of-scope list. Find this block under slice 64a in STATUS.md:

```markdown
- Out of scope (follow-up slices):
  - Actor → `__consumer_offsets` persistence write path (64a follow-up).
  - JVM-client integration depth — heartbeat-loop response shape,
    offset-fetch `member_epoch` plumbing, anything else needed to keep
    `apache/kafka:4.0.0` clients on the next-gen path end-to-end
    (64a follow-up; runs the four `jvm_kip848_*` tests today).
  - Rack-aware `UniformAssignor` (64b).
```

Delete the first two bullets so it reads:

```markdown
- Out of scope (follow-up slices):
  - Rack-aware `UniformAssignor` (64b).
  ...
```

(Keep the remaining bullets unchanged.)

- [ ] **F-3: Commit + push + PR**

```bash
git add STATUS.md
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "Slice 64a follow-up: STATUS.md entry + close 64a follow-up bullets"
git push -u origin kip-848-64a-followup
gh pr create --title "Slice 64a follow-up: KIP-848 persistence + JVM-client gating" --body "$(cat <<'EOF'
## Summary
- Per-group `GroupActor` now writes v3/v5/v6/v7/v8 records to `__consumer_offsets-0` on every state mutation, via a new `OffsetsLog` abstraction.
- ApiVersions advertises `group.version=1` (KIP-584) so kafka-clients 4.0 engages KIP-848 instead of falling back to classic.
- Closes the two follow-up gaps slice 64a left as `#[ignore]`d: 2 persistence-replay tests and 4 JVM-acceptance tests are now passing.

## Test plan
- [ ] `cargo test --workspace` — unit + integration green.
- [ ] `cargo test -p crabka-broker --test jvm_consumer_group_next_gen` — 4 JVM tests pass locally with `apache/kafka:4.0.0`.
- [ ] CI: broker-integration + broker-jvm-acceptance green.
- [ ] CI: codecov/patch within threshold.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Expected: PR opened against `main`.
