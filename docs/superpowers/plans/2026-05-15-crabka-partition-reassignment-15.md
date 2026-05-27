# Slice 15: Partition reassignment — Implementation Plan

## Implementation status

**Slice tracked in STATUS.md as:** `## Slice 15 — Partition reassignment (KIP-455) (2026-05-15)`

**Incomplete / deferred steps (out-of-scope follow-ups):**

- Known limitation: `kafka-reassign-partitions --verify` exits 1 because it unconditionally issues IncrementalAlterConfigs resource_type=4 (broker-scoped throttle config clear) which Crabka did not implement at slice 15 (closed by slice 15b)
- Out of scope: KIP-73 throttled replication (closed by slice 15b)
- KIP-113 log-dir reassignment (closed by slice 45 + slice 45 follow-up)
- KIP-841 force-elect

---

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement KIP-455 `AlterPartitionReassignments` (api_key 45) + `ListPartitionReassignments` (api_key 46) with a two-phase URP-aware state machine, cancellation, and leader handoff. JVM `kafka-reassign-partitions --execute|--verify` works end-to-end.

**Architecture:** `PartitionRecord` gains `adding_replicas` + `removing_replicas` fields. A pure-logic `process_one_partition` function in `handlers/alter_partition_reassignments.rs` produces the intermediate `PartitionRecord` for one alter row. A background task in `reassignment.rs` watches the metadata image; when `adding ⊆ isr`, atomically transitions to the target replica set (handing off leadership first if the current leader is in `removing_replicas`). Slice 10b's existing replicator transparently follows `adding_replicas` since `replicas = union(old, new)` during reassignment.

**Tech Stack:** Rust 1.95.0; reuses slice 14's `ControllerHandle::watch_image()`, slice 13's `authorize`, slice 10b's replicator + ISR maintenance. Wire types already generated at `crates/protocol/generated/{Alter,List}PartitionReassignments{Request,Response}.owned.rs`.

**Reference spec:** [`docs/superpowers/specs/2026-05-15-crabka-partition-reassignment-15-design.md`](../specs/2026-05-15-crabka-partition-reassignment-15-design.md).

**Working directory:** `C:\Users\Matt Stone\git\crabka`. Branch `feature/partition-reassignment-15` already created with spec committed at `ed95cec` (+ fix at `7f6f8b0`).

**Compat note:** Per `CLAUDE.md` at repo root, Crabka is greenfield/undeployed. **Do not** add `#[serde(default)]`, migration shims, or backwards-compat code when changing internal types like `PartitionRecord`. Wipe local data dirs when developing across the slice boundary.

---

## File structure

```
crates/metadata/src/
├── records.rs          # MODIFIED — PartitionRecord += adding_replicas, removing_replicas
└── image.rs (or wherever MetadataImage lives)
                        # MODIFIED — reassignments_in_flight() accessor

crates/broker/src/
├── codes.rs            # MODIFIED — INVALID_REPLICA_ASSIGNMENT, NO_REASSIGNMENT_IN_PROGRESS
├── reassignment.rs     # NEW      — ReassignmentController trait + run + compute_reassignment_progress + 8 unit tests
├── handlers/
│   ├── alter_partition_reassignments.rs   # NEW — api_key 45 handler + process_one_partition + 6 unit tests
│   ├── list_partition_reassignments.rs    # NEW — api_key 46 handler
│   ├── mod.rs                              # MODIFIED — register both modules
│   └── api_versions.rs                     # MODIFIED — supported_apis += 45, 46
├── network/dispatch.rs # MODIFIED — flex table + 2 intercept arms + 2 helpers
├── broker.rs           # MODIFIED — spawn reassignment task; ReassignmentControllerAdapter
└── lib.rs              # MODIFIED — pub mod reassignment

crates/broker/tests/
├── partition_reassignment.rs  # NEW — 4 broker integration tests
└── jvm_acceptance.rs          # MODIFIED — 1 new JVM test
```

12 tasks across 6 batches.

---

## Batch 1 — Metadata layer

### Task 1: `PartitionRecord` fields + `reassignments_in_flight` accessor

**Files:**
- Modify: `crates/metadata/src/records.rs`
- Modify: wherever `MetadataImage` lives (likely `crates/metadata/src/image.rs` — search with `rg "pub struct MetadataImage" crates/metadata/src/`)
- Test: same file's existing `#[cfg(test)] mod tests`

- [ ] **Step 1: Extend `PartitionRecord` in `records.rs`**

Locate `pub struct PartitionRecord` (around line 19 of `crates/metadata/src/records.rs`). Append the two new fields:

```rust
pub struct PartitionRecord {
    pub topic: String,
    pub partition: i32,
    pub leader: NodeId,
    pub replicas: Vec<NodeId>,
    pub isr: Vec<NodeId>,
    /// Per-partition leader epoch. Bumped on every leader change.
    /// Slice-10b adds this; older on-disk metadata is not migrated.
    pub leader_epoch: i32,
    /// Replicas being added in an in-flight reassignment. Empty when no
    /// reassignment in flight. KIP-455.
    pub adding_replicas: Vec<NodeId>,
    /// Replicas being removed in an in-flight reassignment. Empty when
    /// no reassignment in flight. KIP-455.
    pub removing_replicas: Vec<NodeId>,
}
```

Do **not** add `#[serde(default)]`. Per `CLAUDE.md`: greenfield project, change schemas freely.

- [ ] **Step 2: Update every `PartitionRecord` constructor in the workspace**

Search for `PartitionRecord {` (capital P struct-init):

```
rg "PartitionRecord \{" --type rust
```

Every constructor site needs the two new fields initialized (usually to `vec![]`). This may touch:
- `crates/broker/src/leader_election.rs` — slice 14 used `..pr.clone()` patterns that auto-cover, but explicit constructors need updating
- `crates/broker/src/handlers/create_topics.rs` (or wherever topic creation produces partition records)
- `crates/broker/tests/*` — test helpers
- `crates/broker/src/replicator_supervisor.rs` test fixtures
- The `MetadataRecord::V1Partition(PartitionRecord)` decoder — if it deserializes from raft log, serde will produce a missing-field error on old logs. That's expected; users wipe data dirs (CLAUDE.md).

Use `Edit` with `replace_all: false` for unique sites; if there are dozens, you can use `replace_all: true` only when the surrounding context is uniform.

- [ ] **Step 3: Add `MetadataImage::reassignments_in_flight()` accessor**

Find `impl MetadataImage` (likely in `crates/metadata/src/image.rs` — confirm with `rg "impl MetadataImage" crates/metadata/`). Add:

```rust
    /// All partitions where a reassignment is currently in flight
    /// (`adding_replicas` or `removing_replicas` non-empty).
    pub fn reassignments_in_flight(&self) -> impl Iterator<Item = &PartitionRecord> + '_ {
        self.topics()
            .flat_map(move |t| self.partitions_of(&t.name))
            .filter(|p| !p.adding_replicas.is_empty() || !p.removing_replicas.is_empty())
    }
```

Adjust the iterator-construction style to match local conventions (e.g., the file may use explicit `Box<dyn Iterator>` instead of `impl Iterator + '_`). Look at how `topics()` and `partitions_of` are themselves implemented.

- [ ] **Step 4: Add 4 unit tests for the new accessor**

Append to whatever `#[cfg(test)] mod tests` already exists in the image file (or `records.rs` if image.rs has no test block — pick the one with the existing `MetadataImage` tests):

```rust
    #[test]
    fn reassignments_in_flight_excludes_idle_partitions() {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "foo".into(),
            topic_id: uuid::Uuid::nil(),
            partitions: 1,
            replication_factor: 3,
        }));
        img.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "foo".into(),
            partition: 0,
            leader: 1,
            replicas: vec![1, 2, 3],
            isr: vec![1, 2, 3],
            leader_epoch: 0,
            adding_replicas: vec![],
            removing_replicas: vec![],
        }));
        assert_eq!(img.reassignments_in_flight().count(), 0);
    }

    #[test]
    fn reassignments_in_flight_returns_partitions_with_adding() {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "foo".into(),
            topic_id: uuid::Uuid::nil(),
            partitions: 1,
            replication_factor: 3,
        }));
        img.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "foo".into(),
            partition: 0,
            leader: 1,
            replicas: vec![1, 2, 3, 4],
            isr: vec![1, 2, 3],
            leader_epoch: 0,
            adding_replicas: vec![4],
            removing_replicas: vec![],
        }));
        let rows: Vec<_> = img.reassignments_in_flight().collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].adding_replicas, vec![4]);
    }

    #[test]
    fn reassignments_in_flight_returns_partitions_with_removing() {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "foo".into(),
            topic_id: uuid::Uuid::nil(),
            partitions: 1,
            replication_factor: 3,
        }));
        img.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "foo".into(),
            partition: 0,
            leader: 1,
            replicas: vec![1, 2, 3],
            isr: vec![1, 2, 3],
            leader_epoch: 0,
            adding_replicas: vec![],
            removing_replicas: vec![3],
        }));
        let rows: Vec<_> = img.reassignments_in_flight().collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].removing_replicas, vec![3]);
    }

    #[test]
    fn reassignments_in_flight_covers_multiple_topics() {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        for name in ["foo", "bar"] {
            img.apply(&MetadataRecord::V1Topic(TopicRecord {
                name: name.into(),
                topic_id: uuid::Uuid::nil(),
                partitions: 1,
                replication_factor: 3,
            }));
            img.apply(&MetadataRecord::V1Partition(PartitionRecord {
                topic: name.into(),
                partition: 0,
                leader: 1,
                replicas: vec![1, 2, 3, 4],
                isr: vec![1, 2, 3],
                leader_epoch: 0,
                adding_replicas: vec![4],
                removing_replicas: vec![],
            }));
        }
        assert_eq!(img.reassignments_in_flight().count(), 2);
    }
```

- [ ] **Step 5: Build + tests + lints**

```
cargo build --workspace
cargo test -p crabka-metadata
cargo test -p crabka-broker --lib
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: every `PartitionRecord` constructor site builds again; 4 new tests PASS. Workspace clippy clean (if a new constructor site clippy-fires on the now-larger struct literal — e.g., `clippy::too_many_lines` on a test fixture — fix inline).

- [ ] **Step 6: Commit**

```bash
git add crates/metadata/src/ crates/broker/src/ crates/broker/tests/ crates/raft/src/
git commit -m "$(cat <<'EOF'
feat(metadata): PartitionRecord += adding_replicas + removing_replicas

KIP-455 in-flight reassignment fields. Empty when no reassignment in
flight. MetadataImage::reassignments_in_flight() iterates partitions
with non-empty adding or removing — used by ListPartitionReassignments
handler and the background completion task.

Per CLAUDE.md (greenfield project), no serde(default) shim; pre-slice
raft logs require wiping the data dir.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: Wire error codes

**Files:**
- Modify: `crates/broker/src/codes.rs`

- [ ] **Step 1: Append two constants**

Locate the existing block of `pub const X: i16 = N;` declarations and append:

```rust
pub const INVALID_REPLICA_ASSIGNMENT: i16 = 39;
pub const NO_REASSIGNMENT_IN_PROGRESS: i16 = 85;
```

(`UNKNOWN_TOPIC_OR_PARTITION = 3`, `COORDINATOR_NOT_AVAILABLE = 15`, `CLUSTER_AUTHORIZATION_FAILED = 31`, `INVALID_REQUEST = 42`, `ELIGIBLE_LEADERS_NOT_AVAILABLE = 81` already exist from prior slices — verify before adding to avoid duplicates.)

- [ ] **Step 2: Build + lints**

```
cargo build -p crabka-broker
cargo fmt --check -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
```

Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add crates/broker/src/codes.rs
git commit -m "$(cat <<'EOF'
feat(broker): wire codes for partition reassignment

INVALID_REPLICA_ASSIGNMENT (39), NO_REASSIGNMENT_IN_PROGRESS (85)
for KIP-455 handlers.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Batch 2 — Algorithm

### Task 3: `process_one_partition` + 6 unit tests

**Files:**
- Create: `crates/broker/src/handlers/alter_partition_reassignments.rs`
- Modify: `crates/broker/src/handlers/mod.rs` (register module)

This task defines the pure algorithm. The wire handler that calls it is T5.

- [ ] **Step 1: Create the file with imports + the algorithm**

```rust
//! `AlterPartitionReassignments` (api_key 45, KIP-455).
//!
//! The wire handler lives here too (task 5). This task focuses on the
//! pure-logic `process_one_partition` helper that turns one alter row
//! into a `PartitionRecord` ready to submit, or a wire error code.

#![allow(dead_code)]

use crabka_metadata::{MetadataImage, PartitionRecord};
use crabka_raft::NodeId;

use crate::codes::{
    ELIGIBLE_LEADERS_NOT_AVAILABLE, INVALID_REPLICA_ASSIGNMENT, NO_REASSIGNMENT_IN_PROGRESS,
    UNKNOWN_TOPIC_OR_PARTITION,
};

/// Process one (topic, partition, target_opt) row from an
/// `AlterPartitionReassignments` request. Returns:
///   - `Ok(Some(PartitionRecord))` — submit this intermediate record
///   - `Ok(None)` — no-op (already at target, or empty alter)
///   - `Err((wire_code, message))` — reject this row
pub(crate) fn process_one_partition(
    image: &MetadataImage,
    topic: &str,
    partition: i32,
    target: Option<&[i32]>,
    allow_rf_change: bool,
) -> Result<Option<PartitionRecord>, (i16, String)> {
    let pr = image
        .partition(topic, partition)
        .ok_or((UNKNOWN_TOPIC_OR_PARTITION, "unknown partition".into()))?;

    match target {
        None => cancel_path(pr),
        Some(target_slice) => {
            validate_target(target_slice, image, allow_rf_change, pr)?;
            start_path(pr, target_slice)
        }
    }
}

fn validate_target(
    target: &[i32],
    image: &MetadataImage,
    allow_rf_change: bool,
    pr: &PartitionRecord,
) -> Result<(), (i16, String)> {
    if target.is_empty() {
        return Err((INVALID_REPLICA_ASSIGNMENT, "empty target".into()));
    }
    // Duplicates.
    let mut seen: std::collections::HashSet<i32> = std::collections::HashSet::new();
    for &n in target {
        if !seen.insert(n) {
            return Err((INVALID_REPLICA_ASSIGNMENT, format!("duplicate replica {n}")));
        }
    }
    // Every node id must be a registered broker.
    for &n in target {
        if !image.broker_exists(n as NodeId) {
            return Err((INVALID_REPLICA_ASSIGNMENT, format!("unknown broker {n}")));
        }
    }
    // RF-change check.
    if !allow_rf_change {
        let current_target_len = pr
            .replicas
            .iter()
            .filter(|n| !pr.removing_replicas.contains(n))
            .count();
        if target.len() != current_target_len {
            return Err((
                INVALID_REPLICA_ASSIGNMENT,
                format!(
                    "rf change disallowed: target len {} != current target len {}",
                    target.len(),
                    current_target_len,
                ),
            ));
        }
    }
    Ok(())
}

fn cancel_path(pr: &PartitionRecord) -> Result<Option<PartitionRecord>, (i16, String)> {
    if pr.adding_replicas.is_empty() && pr.removing_replicas.is_empty() {
        return Err((NO_REASSIGNMENT_IN_PROGRESS, "nothing to cancel".into()));
    }
    let reverted_replicas: Vec<NodeId> = pr
        .replicas
        .iter()
        .filter(|n| !pr.adding_replicas.contains(n))
        .copied()
        .collect();
    let reverted_isr: Vec<NodeId> = pr
        .isr
        .iter()
        .filter(|n| !pr.adding_replicas.contains(n))
        .copied()
        .collect();
    let (leader, epoch_bump) = if pr.adding_replicas.contains(&pr.leader) {
        // Leader was an adding replica; revert leadership.
        match reverted_replicas
            .iter()
            .find(|n| reverted_isr.contains(n))
        {
            Some(&n) => (n, 1),
            None => {
                return Err((
                    ELIGIBLE_LEADERS_NOT_AVAILABLE,
                    "no eligible leader after cancel".into(),
                ))
            }
        }
    } else {
        (pr.leader, 0)
    };
    Ok(Some(PartitionRecord {
        topic: pr.topic.clone(),
        partition: pr.partition,
        leader,
        replicas: reverted_replicas,
        isr: reverted_isr,
        leader_epoch: pr.leader_epoch + epoch_bump,
        adding_replicas: vec![],
        removing_replicas: vec![],
    }))
}

fn start_path(
    pr: &PartitionRecord,
    target: &[i32],
) -> Result<Option<PartitionRecord>, (i16, String)> {
    let target_set: Vec<NodeId> = target.iter().map(|&x| x as NodeId).collect();
    let current_target: Vec<NodeId> = pr
        .replicas
        .iter()
        .filter(|n| !pr.removing_replicas.contains(n))
        .copied()
        .collect();
    let old: Vec<NodeId> = current_target
        .iter()
        .filter(|n| !target_set.contains(n))
        .copied()
        .collect();
    let new: Vec<NodeId> = target_set
        .iter()
        .filter(|n| !current_target.contains(n))
        .copied()
        .collect();
    if old.is_empty() && new.is_empty() {
        return Ok(None); // already at target — no-op
    }
    // replicas = current_target ∪ target (current_target first, then new).
    let mut new_replicas = current_target.clone();
    for n in &new {
        new_replicas.push(*n);
    }
    Ok(Some(PartitionRecord {
        topic: pr.topic.clone(),
        partition: pr.partition,
        leader: pr.leader,
        replicas: new_replicas,
        isr: pr.isr.clone(),
        leader_epoch: pr.leader_epoch,
        adding_replicas: new,
        removing_replicas: old,
    }))
}
```

**Note on `image.broker_exists`:** if this accessor doesn't exist on `MetadataImage`, search for the equivalent (e.g., `image.brokers().any(|b| b.node_id == n)`). Slice 12b added broker-registration plumbing; the accessor pattern lives there.

- [ ] **Step 2: Register the module**

In `crates/broker/src/handlers/mod.rs`, add (alphabetically):

```rust
mod alter_partition_reassignments;
```

- [ ] **Step 3: Write 6 unit tests**

Append to the new file:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crabka_metadata::{BrokerRegistrationRecord, MetadataRecord, TopicRecord};
    use uuid::Uuid;

    fn img_with(replicas: &[NodeId], isr: &[NodeId], adding: &[NodeId], removing: &[NodeId], leader: NodeId) -> MetadataImage {
        let mut img = MetadataImage::new(Uuid::nil());
        // Register brokers 1..=6 so validate_target accepts target lists.
        for n in 1..=6 {
            img.apply(&MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
                node_id: n,
                ..Default::default()  // tweak per actual record field set
            }));
        }
        img.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "foo".into(),
            topic_id: Uuid::nil(),
            partitions: 1,
            replication_factor: replicas.len() as i16,
        }));
        img.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "foo".into(),
            partition: 0,
            leader,
            replicas: replicas.to_vec(),
            isr: isr.to_vec(),
            leader_epoch: 5,
            adding_replicas: adding.to_vec(),
            removing_replicas: removing.to_vec(),
        }));
        img
    }

    #[test]
    fn noop_when_already_at_target() {
        let img = img_with(&[1, 2, 3], &[1, 2, 3], &[], &[], 1);
        let res = process_one_partition(&img, "foo", 0, Some(&[1, 2, 3]), true).unwrap();
        assert!(res.is_none());
    }

    #[test]
    fn start_writes_union_replicas() {
        let img = img_with(&[1, 2, 3], &[1, 2, 3], &[], &[], 1);
        let res = process_one_partition(&img, "foo", 0, Some(&[1, 4]), true)
            .expect("ok")
            .expect("Some");
        assert_eq!(res.replicas, vec![1, 2, 3, 4]);
        assert_eq!(res.adding_replicas, vec![4]);
        assert_eq!(res.removing_replicas, vec![2, 3]);
        assert_eq!(res.leader, 1);
        assert_eq!(res.leader_epoch, 5); // unchanged on start
    }

    #[test]
    fn replaces_existing_in_flight_reassignment() {
        // Currently in flight: replicas=[1,2,3,4], adding=[4], removing=[2,3].
        // current_target = [1,4]. New alter target = [5,6].
        // Expected: replicas=[1,4,5,6], adding=[5,6], removing=[1,4].
        let img = img_with(&[1, 2, 3, 4], &[1, 2, 3], &[4], &[2, 3], 1);
        let res = process_one_partition(&img, "foo", 0, Some(&[5, 6]), true)
            .expect("ok")
            .expect("Some");
        assert_eq!(res.replicas, vec![1, 4, 5, 6]);
        assert_eq!(res.adding_replicas, vec![5, 6]);
        assert_eq!(res.removing_replicas, vec![1, 4]);
    }

    #[test]
    fn rf_change_rejected_when_disabled() {
        let img = img_with(&[1, 2, 3], &[1, 2, 3], &[], &[], 1);
        let err = process_one_partition(&img, "foo", 0, Some(&[1, 2]), false).unwrap_err();
        assert_eq!(err.0, INVALID_REPLICA_ASSIGNMENT);
    }

    #[test]
    fn rf_change_allowed_when_enabled() {
        let img = img_with(&[1, 2, 3], &[1, 2, 3], &[], &[], 1);
        let res = process_one_partition(&img, "foo", 0, Some(&[1, 2]), true)
            .expect("ok")
            .expect("Some");
        assert_eq!(res.removing_replicas, vec![3]);
    }

    #[test]
    fn cancel_with_leader_in_adding_reverts_leader() {
        // After a successful leader handoff during reassignment, leader=4 (an adding replica).
        // Cancel: leader should revert to whoever in reverted replicas ∩ isr.
        // replicas=[1,2,3,4], adding=[4], removing=[2,3], leader=4, isr=[1,4].
        let img = img_with(&[1, 2, 3, 4], &[1, 4], &[4], &[2, 3], 4);
        let res = process_one_partition(&img, "foo", 0, None, true)
            .expect("ok")
            .expect("Some");
        assert_eq!(res.replicas, vec![1, 2, 3]);
        assert_eq!(res.adding_replicas, Vec::<NodeId>::new());
        assert_eq!(res.removing_replicas, Vec::<NodeId>::new());
        assert_eq!(res.leader, 1);  // reverted replicas ∩ isr = [1]
        assert_eq!(res.leader_epoch, 6);  // bumped
    }

    #[test]
    fn empty_target_rejected() {
        let img = img_with(&[1, 2, 3], &[1, 2, 3], &[], &[], 1);
        let err = process_one_partition(&img, "foo", 0, Some(&[]), true).unwrap_err();
        assert_eq!(err.0, INVALID_REPLICA_ASSIGNMENT);
    }
}
```

**Note:** the `BrokerRegistrationRecord` literal in `img_with` assumes `Default` is derived. If it isn't (slice 12b's record has many fields — host, port, rack, etc.), fill in the minimal field set used by sibling tests. Look at how `crates/broker/tests/leader_election.rs` or `crates/raft/src/state_machine.rs` tests construct broker records. The test only cares that brokers 1..=6 exist in the image so `validate_target` accepts them.

- [ ] **Step 4: Build + tests + lints**

```
cargo test -p crabka-broker --lib alter_partition_reassignments
cargo fmt --check -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
```

Expected: 6 new tests PASS. Workspace clippy clean.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/handlers/alter_partition_reassignments.rs crates/broker/src/handlers/mod.rs
git commit -m "$(cat <<'EOF'
feat(broker): process_one_partition for AlterPartitionReassignments

Pure-logic helper that turns one alter row into an intermediate
PartitionRecord or a wire error code. Covers start, cancellation,
RF-change validation, leader-revert on cancel.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: `compute_reassignment_progress` + 8 unit tests

**Files:**
- Create: `crates/broker/src/reassignment.rs`
- Modify: `crates/broker/src/lib.rs` (add `pub mod reassignment`)

- [ ] **Step 1: Write the module**

```rust
//! KIP-455 reassignment-completion background task.
//!
//! Runs on the controller leader. Watches the metadata image; when a
//! reassignment's `adding_replicas` are all in ISR, atomically
//! transitions to the target replica set. If the current leader is in
//! `removing_replicas`, hands off leadership first to a target replica
//! in ISR.

#![allow(dead_code)]

use std::sync::Arc;

use async_trait::async_trait;
use crabka_metadata::{MetadataImage, MetadataRecord, PartitionRecord};
use crabka_raft::NodeId;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::heartbeat::controller_state::ControllerLivenessState;

/// Minimal trait for the controller surface this task needs. Lets unit
/// tests inject a mock without spinning up real raft.
#[async_trait]
pub(crate) trait ReassignmentController: Send + Sync {
    fn is_leader(&self) -> bool;
    fn current_image(&self) -> Arc<MetadataImage>;
    fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>>;
    async fn submit_change(&self, records: Vec<MetadataRecord>) -> Result<(), String>;
}

/// Background task entry point. Driven by image-apply events.
pub(crate) async fn run(
    controller: Arc<dyn ReassignmentController>,
    liveness: Arc<ControllerLivenessState>,
    shutdown: CancellationToken,
) {
    let mut watcher = controller.watch_image();
    loop {
        tokio::select! {
            _ = watcher.changed() => {},
            _ = shutdown.cancelled() => {
                info!("reassignment task shutting down");
                return;
            }
        }
        if !controller.is_leader() {
            debug!("reassignment tick skipped: not controller leader");
            continue;
        }
        let image = controller.current_image();
        let updates = compute_reassignment_progress(&image, &liveness).await;
        if !updates.is_empty() {
            info!(count = updates.len(), "reassignment: submitting completion updates");
            if let Err(e) = controller.submit_change(updates).await {
                warn!(error = %e, "reassignment: submit failed");
            }
        }
    }
}

/// Pure logic: scan every in-flight reassignment; produce completion
/// or leader-handoff records for those ready to advance.
pub(crate) async fn compute_reassignment_progress(
    image: &MetadataImage,
    liveness: &ControllerLivenessState,
) -> Vec<MetadataRecord> {
    let mut updates = Vec::new();
    for pr in image.reassignments_in_flight() {
        let target: Vec<NodeId> = pr
            .replicas
            .iter()
            .filter(|r| !pr.removing_replicas.contains(r))
            .copied()
            .collect();
        let adding_caught_up = pr.adding_replicas.iter().all(|n| pr.isr.contains(n));
        if !adding_caught_up {
            continue; // wait for replication
        }
        if pr.removing_replicas.contains(&pr.leader) {
            // Leader handoff phase. Find an eligible new leader in target ∩ isr that is alive.
            let mut new_leader: Option<NodeId> = None;
            for n in &target {
                if pr.isr.contains(n) && liveness.is_alive(*n).await {
                    new_leader = Some(*n);
                    break;
                }
            }
            if let Some(leader) = new_leader {
                updates.push(MetadataRecord::V1Partition(PartitionRecord {
                    topic: pr.topic.clone(),
                    partition: pr.partition,
                    leader,
                    leader_epoch: pr.leader_epoch + 1,
                    replicas: pr.replicas.clone(),
                    isr: pr.isr.clone(),
                    adding_replicas: pr.adding_replicas.clone(),
                    removing_replicas: pr.removing_replicas.clone(),
                }));
            }
            // Whether or not we found a leader, don't also try to complete this tick.
            continue;
        }
        // Completion phase.
        let new_isr: Vec<NodeId> = pr
            .isr
            .iter()
            .filter(|n| target.contains(n))
            .copied()
            .collect();
        updates.push(MetadataRecord::V1Partition(PartitionRecord {
            topic: pr.topic.clone(),
            partition: pr.partition,
            leader: pr.leader,
            leader_epoch: pr.leader_epoch, // unchanged: leader stays, only replica set changes
            replicas: target,
            isr: new_isr,
            adding_replicas: vec![],
            removing_replicas: vec![],
        }));
    }
    updates
}
```

**Note:** `is_alive` is `async` on `ControllerLivenessState` (Mutex-backed). The function is async because of that.

- [ ] **Step 2: Register the module**

In `crates/broker/src/lib.rs`, add (alphabetically alongside other `pub mod ...;`):

```rust
pub mod reassignment;
```

(`pub(crate)` is fine too if siblings use it — match local convention.)

- [ ] **Step 3: Write 8 unit tests**

Append to the new file:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crabka_metadata::{BrokerRegistrationRecord, MetadataImage, MetadataRecord, TopicRecord};
    use std::time::Duration;
    use uuid::Uuid;

    fn img(
        replicas: &[NodeId],
        isr: &[NodeId],
        adding: &[NodeId],
        removing: &[NodeId],
        leader: NodeId,
    ) -> Arc<MetadataImage> {
        let mut img = MetadataImage::new(Uuid::nil());
        for n in 1..=6 {
            img.apply(&MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
                node_id: n,
                ..Default::default()
            }));
        }
        img.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "foo".into(),
            topic_id: Uuid::nil(),
            partitions: 1,
            replication_factor: replicas.len() as i16,
        }));
        img.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "foo".into(),
            partition: 0,
            leader,
            replicas: replicas.to_vec(),
            isr: isr.to_vec(),
            leader_epoch: 5,
            adding_replicas: adding.to_vec(),
            removing_replicas: removing.to_vec(),
        }));
        Arc::new(img)
    }

    async fn liveness(alive: &[NodeId]) -> ControllerLivenessState {
        let l = ControllerLivenessState::new(Duration::from_secs(10));
        for n in alive {
            l.record_heartbeat(*n).await;
        }
        l
    }

    fn first_partition(rec: &MetadataRecord) -> &PartitionRecord {
        match rec {
            MetadataRecord::V1Partition(p) => p,
            _ => panic!("expected V1Partition"),
        }
    }

    #[tokio::test]
    async fn complete_when_adding_in_isr_writes_target() {
        let img = img(&[1, 2, 3], &[1, 2, 3], &[3], &[2], 1);
        let l = liveness(&[1, 2, 3]).await;
        let updates = compute_reassignment_progress(&img, &l).await;
        assert_eq!(updates.len(), 1);
        let pr = first_partition(&updates[0]);
        assert_eq!(pr.replicas, vec![1, 3]);
        assert_eq!(pr.adding_replicas, Vec::<NodeId>::new());
        assert_eq!(pr.removing_replicas, Vec::<NodeId>::new());
        assert_eq!(pr.isr, vec![1, 3]);
        assert_eq!(pr.leader, 1); // unchanged
        assert_eq!(pr.leader_epoch, 5); // unchanged (leader didn't change)
    }

    #[tokio::test]
    async fn wait_when_adding_not_in_isr() {
        let img = img(&[1, 2, 3], &[1, 2], &[3], &[2], 1);
        let l = liveness(&[1, 2, 3]).await;
        let updates = compute_reassignment_progress(&img, &l).await;
        assert!(updates.is_empty(), "should wait; got {:?}", updates);
    }

    #[tokio::test]
    async fn leader_handoff_when_leader_in_removing() {
        // leader=2, removing=[2]; new leader must come from target ∩ isr = {1,3} ∩ {1,2,3} = {1,3}.
        let img = img(&[1, 2, 3], &[1, 2, 3], &[3], &[2], 2);
        let l = liveness(&[1, 2, 3]).await;
        let updates = compute_reassignment_progress(&img, &l).await;
        assert_eq!(updates.len(), 1);
        let pr = first_partition(&updates[0]);
        assert!(pr.leader == 1 || pr.leader == 3, "leader was {}", pr.leader);
        assert_eq!(pr.leader_epoch, 6); // bumped
        // Replica set unchanged — completion happens next tick.
        assert_eq!(pr.adding_replicas, vec![3]);
        assert_eq!(pr.removing_replicas, vec![2]);
    }

    #[tokio::test]
    async fn leader_handoff_skipped_if_no_alive_target_replica() {
        // leader=2, removing=[2]; only target replicas {1,3} in isr but
        // none alive — wait.
        let img = img(&[1, 2, 3], &[1, 2, 3], &[3], &[2], 2);
        let l = liveness(&[2]).await; // only 2 alive
        let updates = compute_reassignment_progress(&img, &l).await;
        assert!(updates.is_empty());
    }

    #[tokio::test]
    async fn idle_partition_emits_no_update() {
        let img = img(&[1, 2, 3], &[1, 2, 3], &[], &[], 1);
        let l = liveness(&[1, 2, 3]).await;
        let updates = compute_reassignment_progress(&img, &l).await;
        assert!(updates.is_empty());
    }

    #[tokio::test]
    async fn multiple_partitions_handled_independently() {
        let mut img_inner = MetadataImage::new(Uuid::nil());
        for n in 1..=6 {
            img_inner.apply(&MetadataRecord::V1BrokerRegistration(
                BrokerRegistrationRecord {
                    node_id: n,
                    ..Default::default()
                },
            ));
        }
        for name in ["foo", "bar"] {
            img_inner.apply(&MetadataRecord::V1Topic(TopicRecord {
                name: name.into(),
                topic_id: Uuid::nil(),
                partitions: 1,
                replication_factor: 3,
            }));
            img_inner.apply(&MetadataRecord::V1Partition(PartitionRecord {
                topic: name.into(),
                partition: 0,
                leader: 1,
                replicas: vec![1, 2, 3],
                isr: vec![1, 2, 3],
                leader_epoch: 5,
                adding_replicas: vec![3],
                removing_replicas: vec![2],
            }));
        }
        let img = Arc::new(img_inner);
        let l = liveness(&[1, 2, 3]).await;
        let updates = compute_reassignment_progress(&img, &l).await;
        assert_eq!(updates.len(), 2);
    }

    #[tokio::test]
    async fn target_includes_only_replicas_minus_removing() {
        // adding=[4,5], removing=[1,2], replicas=[1,2,3,4,5].
        // target = [3,4,5]. isr ⊇ adding required; isr=[1,2,3,4,5].
        let img = img(&[1, 2, 3, 4, 5], &[1, 2, 3, 4, 5], &[4, 5], &[1, 2], 3);
        let l = liveness(&[1, 2, 3, 4, 5]).await;
        let updates = compute_reassignment_progress(&img, &l).await;
        assert_eq!(updates.len(), 1);
        let pr = first_partition(&updates[0]);
        assert_eq!(pr.replicas, vec![3, 4, 5]);
        assert_eq!(pr.isr, vec![3, 4, 5]);
    }

    #[tokio::test]
    async fn isr_intersection_when_some_targets_not_in_isr() {
        // adding=[4], removing=[2]; isr=[1,2,3,4]; target=[1,3,4].
        // new_isr = isr ∩ target = [1,3,4].
        let img = img(&[1, 2, 3, 4], &[1, 2, 3, 4], &[4], &[2], 1);
        let l = liveness(&[1, 2, 3, 4]).await;
        let updates = compute_reassignment_progress(&img, &l).await;
        assert_eq!(updates.len(), 1);
        let pr = first_partition(&updates[0]);
        assert_eq!(pr.isr, vec![1, 3, 4]);
    }
}
```

**Tweak as needed:** `BrokerRegistrationRecord { node_id, ..Default::default() }` assumes `Default` is derived — if it isn't, fill in the minimal field set used by sibling tests (e.g. `host: String::new(), port: 0`, etc.). Look at how slice 12b's tests construct broker records.

- [ ] **Step 4: Build + tests + lints**

```
cargo build -p crabka-broker
cargo test -p crabka-broker --lib reassignment
cargo fmt --check -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
```

Expected: 8 new tests PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/reassignment.rs crates/broker/src/lib.rs
git commit -m "$(cat <<'EOF'
feat(broker): compute_reassignment_progress + ReassignmentController trait

Background completion task pure logic. Iterates in-flight
reassignments; when adding ⊆ ISR, produces a completion record or a
leader-handoff record (when leader is in removing). 8 unit tests
cover wait/complete/handoff/idle paths plus multi-partition scope.
Spawning from Broker::start is task 8.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Batch 3 — Wire handlers + dispatch

### Task 5: `AlterPartitionReassignments` wire handler

**Files:**
- Modify: `crates/broker/src/handlers/alter_partition_reassignments.rs` (append the `handle` function alongside T3's `process_one_partition`)

- [ ] **Step 1: Append handler + helpers**

Append after the existing module-level functions (above the `#[cfg(test)] mod tests` block):

```rust
use std::collections::HashMap;
use std::net::SocketAddr;

use bytes::Bytes;
use crabka_metadata::ResourceType;
use crabka_protocol::owned::alter_partition_reassignments_request::AlterPartitionReassignmentsRequest;
use crabka_protocol::owned::alter_partition_reassignments_response::{
    AlterPartitionReassignmentsResponse, ReassignablePartitionResponse, ReassignableTopicResponse,
};
use crabka_protocol::Encode;
use crabka_security::Principal;

use crate::authorizer::{authorize, AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes::{CLUSTER_AUTHORIZATION_FAILED, COORDINATOR_NOT_AVAILABLE};

pub(crate) async fn handle(
    broker: &Broker,
    req: AlterPartitionReassignmentsRequest,
    principal: &Principal,
    peer: &SocketAddr,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    let image = broker.controller.current_image();
    // Whole-request Cluster Alter authorize.
    let allow = authorize(
        &image,
        &broker.config.super_users,
        &AuthorizationRequest {
            principal,
            host: peer,
            resource_type: ResourceType::Cluster,
            resource_name: "kafka-cluster",
            operation: crabka_metadata::AclOperation::Alter,
        },
    );
    if matches!(allow, AuthorizationResult::Deny) {
        return encode_whole_request_error(&req, CLUSTER_AUTHORIZATION_FAILED, "alter-reassignment denied", api_version);
    }

    let mut by_topic: HashMap<String, Vec<ReassignablePartitionResponse>> = HashMap::new();
    let mut to_submit: Vec<crabka_metadata::MetadataRecord> = Vec::new();
    for topic in &req.topics {
        let mut rows = Vec::with_capacity(topic.partitions.len());
        for p in &topic.partitions {
            let target_slice: Option<&[i32]> = p.replicas.as_deref();
            match process_one_partition(
                &image,
                &topic.name,
                p.partition_index,
                target_slice,
                req.allow_replication_factor_change,
            ) {
                Ok(Some(record)) => {
                    to_submit.push(crabka_metadata::MetadataRecord::V1Partition(record));
                    rows.push(ok_row(p.partition_index));
                }
                Ok(None) => rows.push(ok_row(p.partition_index)),
                Err((code, msg)) => rows.push(err_row(p.partition_index, code, msg)),
            }
        }
        by_topic.insert(topic.name.clone(), rows);
    }

    if !to_submit.is_empty() {
        if let Err(e) = broker.controller.submit_change(to_submit).await {
            tracing::warn!(error = %e, "alter-reassignment submit failed");
            for rows in by_topic.values_mut() {
                for r in rows.iter_mut() {
                    if r.error_code == 0 {
                        r.error_code = COORDINATOR_NOT_AVAILABLE;
                        r.error_message = Some(format!("submit failed: {e}"));
                    }
                }
            }
        }
    }

    let responses: Vec<ReassignableTopicResponse> = by_topic
        .into_iter()
        .map(|(name, partitions)| ReassignableTopicResponse {
            name,
            partitions,
            ..Default::default()
        })
        .collect();
    let resp = AlterPartitionReassignmentsResponse {
        throttle_time_ms: 0,
        allow_replication_factor_change: req.allow_replication_factor_change,
        error_code: 0,
        error_message: None,
        responses,
        ..Default::default()
    };
    encode_response(&resp, api_version)
}

fn ok_row(partition_index: i32) -> ReassignablePartitionResponse {
    ReassignablePartitionResponse {
        partition_index,
        error_code: 0,
        error_message: None,
        ..Default::default()
    }
}

fn err_row(partition_index: i32, code: i16, msg: String) -> ReassignablePartitionResponse {
    ReassignablePartitionResponse {
        partition_index,
        error_code: code,
        error_message: Some(msg),
        ..Default::default()
    }
}

fn encode_whole_request_error(
    req: &AlterPartitionReassignmentsRequest,
    code: i16,
    msg: &str,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    let responses: Vec<ReassignableTopicResponse> = req
        .topics
        .iter()
        .map(|t| ReassignableTopicResponse {
            name: t.name.clone(),
            partitions: t
                .partitions
                .iter()
                .map(|p| err_row(p.partition_index, code, msg.into()))
                .collect(),
            ..Default::default()
        })
        .collect();
    let resp = AlterPartitionReassignmentsResponse {
        throttle_time_ms: 0,
        allow_replication_factor_change: req.allow_replication_factor_change,
        error_code: 0,
        error_message: None,
        responses,
        ..Default::default()
    };
    encode_response(&resp, api_version)
}

fn encode_response<R: Encode>(resp: &R, api_version: i16) -> Result<Bytes, crate::error::BrokerError> {
    let mut body = Vec::new();
    resp.encode(&mut body, api_version)
        .map_err(|e| crate::error::BrokerError::Replication(format!("encode AlterPartitionReassignments: {e}")))?;
    Ok(Bytes::from(body))
}
```

- [ ] **Step 2: Remove `#![allow(dead_code)]` from the module header**

Now that `handle` is reachable (after T7 wires dispatch), the module-level allow is unnecessary. Replace `#![allow(dead_code)]` with no attribute, OR leave it until after T7 if clippy errors during the build.

- [ ] **Step 3: Build + lints**

```
cargo build -p crabka-broker
cargo fmt --check -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
```

May fail with "function `handle` is never used" until T7 wires dispatch — keep `#![allow(dead_code)]` for now and remove it in T7's commit.

- [ ] **Step 4: Commit**

```bash
git add crates/broker/src/handlers/alter_partition_reassignments.rs
git commit -m "$(cat <<'EOF'
feat(broker): AlterPartitionReassignments handler (api_key 45)

Cluster Alter gate; per-row process_one_partition + batched
submit_change. Matches slice-14 ElectLeaders shape (intercept arms +
helper come in task 7).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 6: `ListPartitionReassignments` wire handler

**Files:**
- Create: `crates/broker/src/handlers/list_partition_reassignments.rs`
- Modify: `crates/broker/src/handlers/mod.rs` (register module)

- [ ] **Step 1: Write the handler**

```rust
//! `ListPartitionReassignments` (api_key 46, KIP-455).

#![allow(dead_code)]

use std::net::SocketAddr;

use bytes::Bytes;
use crabka_metadata::{MetadataImage, PartitionRecord, ResourceType};
use crabka_protocol::owned::list_partition_reassignments_request::ListPartitionReassignmentsRequest;
use crabka_protocol::owned::list_partition_reassignments_response::{
    ListPartitionReassignmentsResponse, OngoingPartitionReassignment, OngoingTopicReassignment,
};
use crabka_protocol::Encode;
use crabka_security::Principal;

use crate::authorizer::{authorize, AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes::CLUSTER_AUTHORIZATION_FAILED;

pub(crate) async fn handle(
    broker: &Broker,
    req: ListPartitionReassignmentsRequest,
    principal: &Principal,
    peer: &SocketAddr,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    let image = broker.controller.current_image();
    let allow = authorize(
        &image,
        &broker.config.super_users,
        &AuthorizationRequest {
            principal,
            host: peer,
            resource_type: ResourceType::Cluster,
            resource_name: "kafka-cluster",
            operation: crabka_metadata::AclOperation::Describe,
        },
    );
    if matches!(allow, AuthorizationResult::Deny) {
        let resp = ListPartitionReassignmentsResponse {
            throttle_time_ms: 0,
            error_code: CLUSTER_AUTHORIZATION_FAILED,
            error_message: Some("list-reassignment denied".into()),
            topics: vec![],
            ..Default::default()
        };
        return encode_response(&resp, api_version);
    }

    let in_flight: Vec<&PartitionRecord> = match &req.topics {
        None => image.reassignments_in_flight().collect(),
        Some(filter) => {
            let mut acc = Vec::new();
            for t in filter {
                let want_all = t.partition_indexes.is_empty();
                for pr in image.partitions_of(&t.name) {
                    if pr.adding_replicas.is_empty() && pr.removing_replicas.is_empty() {
                        continue;
                    }
                    if want_all || t.partition_indexes.contains(&pr.partition) {
                        acc.push(pr);
                    }
                }
            }
            acc
        }
    };

    // Group by topic.
    let mut by_topic: std::collections::BTreeMap<String, Vec<OngoingPartitionReassignment>> =
        std::collections::BTreeMap::new();
    for pr in in_flight {
        by_topic.entry(pr.topic.clone()).or_default().push(OngoingPartitionReassignment {
            partition_index: pr.partition,
            replicas: pr.replicas.iter().map(|n| *n as i32).collect(),
            adding_replicas: pr.adding_replicas.iter().map(|n| *n as i32).collect(),
            removing_replicas: pr.removing_replicas.iter().map(|n| *n as i32).collect(),
            ..Default::default()
        });
    }
    let topics: Vec<OngoingTopicReassignment> = by_topic
        .into_iter()
        .map(|(name, partitions)| OngoingTopicReassignment {
            name,
            partitions,
            ..Default::default()
        })
        .collect();
    let resp = ListPartitionReassignmentsResponse {
        throttle_time_ms: 0,
        error_code: 0,
        error_message: None,
        topics,
        ..Default::default()
    };
    encode_response(&resp, api_version)
}

fn encode_response<R: Encode>(resp: &R, api_version: i16) -> Result<Bytes, crate::error::BrokerError> {
    let mut body = Vec::new();
    resp.encode(&mut body, api_version)
        .map_err(|e| crate::error::BrokerError::Replication(format!("encode ListPartitionReassignments: {e}")))?;
    Ok(Bytes::from(body))
}
```

- [ ] **Step 2: Register the module**

`crates/broker/src/handlers/mod.rs`:

```rust
mod list_partition_reassignments;
```

- [ ] **Step 3: Build + lints**

```
cargo build -p crabka-broker
cargo fmt --check -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
```

Expected: clean (with `#![allow(dead_code)]` until T7 wires dispatch).

- [ ] **Step 4: Commit**

```bash
git add crates/broker/src/handlers/list_partition_reassignments.rs crates/broker/src/handlers/mod.rs
git commit -m "$(cat <<'EOF'
feat(broker): ListPartitionReassignments handler (api_key 46)

Cluster Describe gate; returns in-flight reassignments from the
metadata image. Top-level error_code carries auth failure; partitions
list is empty on Deny.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 7: Dispatch + api_versions wiring

**Files:**
- Modify: `crates/broker/src/handlers/api_versions.rs`
- Modify: `crates/broker/src/network/dispatch.rs`
- Modify: `crates/broker/src/handlers/alter_partition_reassignments.rs` (remove `#![allow(dead_code)]`)
- Modify: `crates/broker/src/handlers/list_partition_reassignments.rs` (remove `#![allow(dead_code)]`)

- [ ] **Step 1: Add to `supported_apis`**

In `crates/broker/src/handlers/api_versions.rs`, append in api-key order (45 and 46 sit after 44):

```rust
v!(alter_partition_reassignments_request),
v!(list_partition_reassignments_request),
```

- [ ] **Step 2: Add to flexible-body table**

In `crates/broker/src/network/dispatch.rs::handler_body_flexible`, add arms:

```rust
45 => version >= crabka_protocol::owned::alter_partition_reassignments_request::FLEXIBLE_MIN,
46 => version >= crabka_protocol::owned::list_partition_reassignments_request::FLEXIBLE_MIN,
```

(Both `FLEXIBLE_MIN = 0` per the generated owned types, so both are always flex.)

- [ ] **Step 3: Add inline-intercept dispatch arms**

Slice 14's pattern (search for `handle_elect_leaders_frame` in `dispatch.rs`). Add two more sibling blocks in the per-connection loop after the existing ones:

```rust
if peek_api_key(&frame) == Some(45) {
    handle_alter_partition_reassignments_frame(
        broker, frame, api_version, correlation_id, client_id, auth, peer,
    ).await?;
    continue;
}
if peek_api_key(&frame) == Some(46) {
    handle_list_partition_reassignments_frame(
        broker, frame, api_version, correlation_id, client_id, auth, peer,
    ).await?;
    continue;
}
```

Match the **exact** parameter set that `handle_elect_leaders_frame` uses (slice 14 introduced it; argument names and types should be copied verbatim).

- [ ] **Step 4: Add the two helper functions**

Alongside `handle_elect_leaders_frame` (slice 14's helper), add:

```rust
async fn handle_alter_partition_reassignments_frame</* same generics as slice-14's helper */>(
    /* same params */
) -> Result<(), crate::error::BrokerError>
/* same where-clause */
{
    use crabka_protocol::owned::alter_partition_reassignments_request::AlterPartitionReassignmentsRequest;
    use crabka_protocol::Decode;
    let req = AlterPartitionReassignmentsRequest::decode(&mut frame.as_ref(), api_version)
        .map_err(|e| crate::error::BrokerError::Codec(e.to_string()))?;
    let principal = auth.principal();
    let response_bytes = crate::handlers::alter_partition_reassignments::handle(
        broker, req, principal, peer, api_version,
    ).await?;
    /* write_response — copy the framing call from handle_elect_leaders_frame verbatim */
    Ok(())
}

async fn handle_list_partition_reassignments_frame</* same generics */>(
    /* same params */
) -> Result<(), crate::error::BrokerError>
/* same where-clause */
{
    use crabka_protocol::owned::list_partition_reassignments_request::ListPartitionReassignmentsRequest;
    use crabka_protocol::Decode;
    let req = ListPartitionReassignmentsRequest::decode(&mut frame.as_ref(), api_version)
        .map_err(|e| crate::error::BrokerError::Codec(e.to_string()))?;
    let principal = auth.principal();
    let response_bytes = crate::handlers::list_partition_reassignments::handle(
        broker, req, principal, peer, api_version,
    ).await?;
    /* write_response */
    Ok(())
}
```

Copy slice 14's `handle_elect_leaders_frame` end-to-end and adapt the request/response types + `handle::*` path. **Do not invent** the response-write idiom — replicate slice 14's exactly.

- [ ] **Step 5: Remove `#![allow(dead_code)]` from both handler modules**

Both handlers are now reachable. Drop the module-level allow attribute.

- [ ] **Step 6: Build + tests + lints**

```
cargo build -p crabka-broker
cargo test -p crabka-broker --lib
cargo test -p crabka-broker --tests
cargo fmt --check -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
```

All existing tests pass. New tests come in T9/T10/T11.

- [ ] **Step 7: Commit**

```bash
git add crates/broker/src/handlers/api_versions.rs crates/broker/src/network/dispatch.rs \
        crates/broker/src/handlers/alter_partition_reassignments.rs \
        crates/broker/src/handlers/list_partition_reassignments.rs
git commit -m "$(cat <<'EOF'
feat(broker): wire AlterPartitionReassignments + ListPartitionReassignments dispatch

api_keys 45 + 46 registered in supported_apis + flexible-body table.
Inline-intercept dispatch arms match the slice-14 ElectLeaders pattern
(both handlers need &Principal + &SocketAddr).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Batch 4 — Background task wiring

### Task 8: `Broker::start` spawn + `ReassignmentControllerAdapter`

**Files:**
- Modify: `crates/broker/src/broker.rs`

- [ ] **Step 1: Add `ReassignmentControllerAdapter`**

Slice 14 added `ControllerAdapter` for `ControllerLike`. Adapt the same pattern for `ReassignmentController`:

```rust
struct ReassignmentControllerAdapter {
    handle: std::sync::Arc<crabka_raft::ControllerHandle>,
    node_id: crabka_raft::NodeId,
}

#[async_trait::async_trait]
impl crate::reassignment::ReassignmentController for ReassignmentControllerAdapter {
    fn is_leader(&self) -> bool {
        *self.handle.watch_leader().borrow() == Some(self.node_id)
    }
    fn current_image(&self) -> std::sync::Arc<crabka_metadata::MetadataImage> {
        self.handle.current_image()
    }
    fn watch_image(&self) -> tokio::sync::watch::Receiver<std::sync::Arc<crabka_metadata::MetadataImage>> {
        self.handle.watch_image()
    }
    async fn submit_change(
        &self,
        records: Vec<crabka_metadata::MetadataRecord>,
    ) -> Result<(), String> {
        self.handle.submit_change(records).await.map_err(|e| e.to_string())
    }
}
```

(Slice 14's `ControllerAdapter` uses `watch_leader().borrow() == Some(self.node_id)`. Same pattern.)

- [ ] **Step 2: Spawn the task in `Broker::start`**

Locate slice 14's `leader_rebalance` spawn block (search `rebalance_handle` or `leader_rebalance::run`). Add a sibling block:

```rust
        // Spawn reassignment-completion background task. The task itself
        // checks is_leader() per image apply — safe to run on every broker.
        // Always-on (no config gate): reassignment completion is a
        // correctness requirement, not an optional behavior.
        {
            let adapter: std::sync::Arc<dyn crate::reassignment::ReassignmentController> =
                std::sync::Arc::new(ReassignmentControllerAdapter {
                    handle: controller.clone(),
                    node_id: config.node_id,
                });
            let liveness_clone = liveness.clone();
            let shutdown_clone = supervisor_shutdown.child_token();
            tokio::spawn(crate::reassignment::run(adapter, liveness_clone, shutdown_clone));
        }
```

Match slice 14's variable names exactly (e.g. `supervisor_shutdown`, `liveness`, `controller`) — confirm by reading the `ControllerAdapter` spawn that slice 14 left at the same site.

- [ ] **Step 3: Build + tests + lints**

```
cargo build -p crabka-broker
cargo test -p crabka-broker --lib
cargo test -p crabka-broker --tests
cargo fmt --check -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
```

Expected: all existing tests pass. The task spawns silently on every broker; on followers it no-ops (`is_leader()` returns false).

- [ ] **Step 4: Commit**

```bash
git add crates/broker/src/broker.rs
git commit -m "$(cat <<'EOF'
feat(broker): spawn reassignment task from Broker::start

ReassignmentControllerAdapter wraps ControllerHandle to satisfy the
ReassignmentController trait. Always spawned; per-tick is_leader()
check makes it a no-op on followers.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Batch 5 — Broker integration tests

### Task 9: alter + complete + list + cancel tests

**Files:**
- Create: `crates/broker/tests/partition_reassignment.rs`

The plan reuses slice 14's 3-broker PLAINTEXT scaffolding (with metadata injection to drive deterministic state transitions). Read `crates/broker/tests/elect_leaders.rs` end-to-end first — it has the canonical helpers.

- [ ] **Step 1: File scaffold + SASL/PLAINTEXT helpers copied from slice 14**

Read `crates/broker/tests/elect_leaders.rs` and copy verbatim:
- `start_three_broker_plaintext_cluster` (or whatever it's named there) — 3-broker PLAINTEXT cluster startup
- `create_topic_plaintext(addr, topic, partitions, rf)` (or whatever slice 14 used to create rf=2 topics)
- `wait_partition_exists`, `wait_partition_leader`, `wait_isr_contains`
- `controller_leader_id(handle)`
- `partition_leader_for_test`, `partition_isr_for_test`, `partition_record_for_test` (the BrokerHandle accessors slice 14 added)

Top of new file:

```rust
//! Slice 15. Broker-side integration tests for AlterPartitionReassignments
//! and ListPartitionReassignments.

#![cfg(not(target_os = "windows"))]
#![allow(clippy::pedantic)] // clippy ICE workarounds noted in slice 14
```

- [ ] **Step 2: Helper — drive AlterPartitionReassignments via wire**

```rust
async fn drive_alter_reassignments(
    addr: std::net::SocketAddr,
    rows: Vec<(&str, i32, Option<Vec<i32>>)>,  // (topic, partition, target_or_none)
) -> Vec<(String, Vec<(i32, i16)>)> {
    use crabka_protocol::owned::alter_partition_reassignments_request::{
        AlterPartitionReassignmentsRequest, ReassignablePartition, ReassignableTopic,
    };
    use crabka_protocol::owned::alter_partition_reassignments_response::AlterPartitionReassignmentsResponse;
    use crabka_protocol::{Decode, Encode};

    // Group by topic.
    let mut by_topic: std::collections::BTreeMap<String, Vec<ReassignablePartition>> =
        std::collections::BTreeMap::new();
    for (topic, partition, target_opt) in rows {
        by_topic.entry(topic.to_string()).or_default().push(ReassignablePartition {
            partition_index: partition,
            replicas: target_opt,
            ..Default::default()
        });
    }
    let topics: Vec<ReassignableTopic> = by_topic.into_iter()
        .map(|(name, partitions)| ReassignableTopic { name, partitions, ..Default::default() })
        .collect();
    let req = AlterPartitionReassignmentsRequest {
        timeout_ms: 30_000,
        allow_replication_factor_change: true,
        topics,
        ..Default::default()
    };
    let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
    let mut body = Vec::new();
    req.encode(&mut body, 1).expect("encode");
    let response_bytes = round_trip(&mut stream, /*api_key*/ 45, /*api_version*/ 1, &body, /*flex*/ true).await;
    let resp = AlterPartitionReassignmentsResponse::decode(&mut response_bytes.as_ref(), 1).expect("decode");
    resp.responses.into_iter().map(|r| (
        r.name,
        r.partitions.into_iter().map(|p| (p.partition_index, p.error_code)).collect(),
    )).collect()
}
```

(`round_trip` is the PLAINTEXT request helper from slice 14's `elect_leaders.rs`; reuse it.)

- [ ] **Step 3: Helper — drive ListPartitionReassignments via wire**

```rust
async fn drive_list_reassignments(
    addr: std::net::SocketAddr,
    filter: Option<Vec<(&str, Vec<i32>)>>,
) -> Vec<(String, Vec<(i32, Vec<i32>, Vec<i32>, Vec<i32>)>)> {
    use crabka_protocol::owned::list_partition_reassignments_request::{
        ListPartitionReassignmentsRequest, ListPartitionReassignmentsTopics,
    };
    use crabka_protocol::owned::list_partition_reassignments_response::ListPartitionReassignmentsResponse;
    use crabka_protocol::{Decode, Encode};

    let topics_arg = filter.map(|list| {
        list.into_iter()
            .map(|(name, partition_indexes)| ListPartitionReassignmentsTopics {
                name: name.to_string(),
                partition_indexes,
                ..Default::default()
            })
            .collect()
    });
    let req = ListPartitionReassignmentsRequest {
        timeout_ms: 30_000,
        topics: topics_arg,
        ..Default::default()
    };
    let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
    let mut body = Vec::new();
    req.encode(&mut body, 0).expect("encode");
    let response_bytes = round_trip(&mut stream, 46, 0, &body, true).await;
    let resp = ListPartitionReassignmentsResponse::decode(&mut response_bytes.as_ref(), 0).expect("decode");
    resp.topics.into_iter().map(|t| (
        t.name,
        t.partitions.into_iter().map(|p| (p.partition_index, p.replicas, p.adding_replicas, p.removing_replicas)).collect(),
    )).collect()
}
```

- [ ] **Step 4: Test 1 — `alter_then_complete_via_isr_catchup`**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_then_complete_via_isr_catchup() {
    let (h1, h2, h3, _d1, _d2, _d3, addr1) = start_three_broker_plaintext_cluster().await;
    create_topic_plaintext(addr1, "foo", 1, /*rf=*/ 2).await;
    wait_partition_exists(&h1, "foo", 0).await;

    // Find which brokers are in `replicas` initially — choose target accordingly.
    let pr = h1.partition_record_for_test("foo", 0).await.expect("partition");
    let initial_replicas = pr.replicas.clone();
    assert_eq!(initial_replicas.len(), 2);
    // Pick the third broker (not in initial_replicas) as the new replica.
    let new_replica: i32 = (1..=3).find(|n| !initial_replicas.contains(&(*n as u64))).expect("free broker") as i32;
    let removing: i32 = *initial_replicas.last().unwrap() as i32;
    let staying: i32 = *initial_replicas.first().unwrap() as i32;
    let target = vec![staying, new_replica];

    // Send alter to controller leader (whichever broker leads raft).
    let raft_addr = controller_leader_addr(&[&h1, &h2, &h3]).await;
    let resp = drive_alter_reassignments(raft_addr, vec![("foo", 0, Some(target.clone()))]).await;
    assert_eq!(resp[0].1, vec![(0, 0)], "expected error_code=0");

    // Image should now show adding/removing.
    let pr_after_alter = h1.partition_record_for_test("foo", 0).await.expect("after alter");
    assert!(pr_after_alter.adding_replicas.contains(&(new_replica as u64)));
    assert!(pr_after_alter.removing_replicas.contains(&(removing as u64)));

    // Inject ISR including the new replica so the background task completes the reassignment.
    let injected = crabka_metadata::PartitionRecord {
        isr: vec![staying as u64, new_replica as u64, removing as u64],
        ..pr_after_alter.clone()
    };
    h1.submit_metadata_record_for_test(
        crabka_metadata::MetadataRecord::V1Partition(injected),
    ).await.expect("inject");

    // Within ~5s the background task should observe adding ⊆ isr and complete.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let pr = h1.partition_record_for_test("foo", 0).await.expect("partition");
        if pr.adding_replicas.is_empty() && pr.removing_replicas.is_empty() {
            assert_eq!(
                pr.replicas.iter().copied().collect::<std::collections::HashSet<u64>>(),
                target.iter().map(|n| *n as u64).collect::<std::collections::HashSet<u64>>(),
            );
            return;
        }
        if std::time::Instant::now() > deadline {
            panic!("reassignment did not complete; pr={:?}", pr);
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}
```

- [ ] **Step 5: Test 2 — `list_in_flight_returns_pending_rows`**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn list_in_flight_returns_pending_rows() {
    let (h1, h2, h3, _d1, _d2, _d3, addr) = start_three_broker_plaintext_cluster().await;
    create_topic_plaintext(addr, "foo", 1, 2).await;
    wait_partition_exists(&h1, "foo", 0).await;

    let pr = h1.partition_record_for_test("foo", 0).await.expect("partition");
    let new_replica: i32 = (1..=3).find(|n| !pr.replicas.contains(&(*n as u64))).expect("free") as i32;
    let staying: i32 = *pr.replicas.first().unwrap() as i32;
    let target = vec![staying, new_replica];

    let raft_addr = controller_leader_addr(&[&h1, &h2, &h3]).await;
    drive_alter_reassignments(raft_addr, vec![("foo", 0, Some(target))]).await;

    let listed = drive_list_reassignments(raft_addr, None).await;
    let foo = listed.iter().find(|(n, _)| n == "foo").expect("foo in list");
    assert_eq!(foo.1.len(), 1);
    assert_eq!(foo.1[0].0, 0);          // partition index
    assert_eq!(foo.1[0].2, vec![new_replica]);  // adding_replicas
}
```

- [ ] **Step 6: Test 3 — `cancel_via_null_replicas_reverts`**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_via_null_replicas_reverts() {
    let (h1, h2, h3, _d1, _d2, _d3, addr) = start_three_broker_plaintext_cluster().await;
    create_topic_plaintext(addr, "foo", 1, 2).await;
    wait_partition_exists(&h1, "foo", 0).await;

    let pr = h1.partition_record_for_test("foo", 0).await.expect("partition");
    let original_replicas = pr.replicas.clone();
    let new_replica: i32 = (1..=3).find(|n| !original_replicas.contains(&(*n as u64))).expect("free") as i32;
    let staying: i32 = *original_replicas.first().unwrap() as i32;
    let target = vec![staying, new_replica];

    let raft_addr = controller_leader_addr(&[&h1, &h2, &h3]).await;
    drive_alter_reassignments(raft_addr, vec![("foo", 0, Some(target))]).await;

    // Cancel: replicas = None.
    let resp = drive_alter_reassignments(raft_addr, vec![("foo", 0, None)]).await;
    assert_eq!(resp[0].1, vec![(0, 0)]);

    let pr_after_cancel = h1.partition_record_for_test("foo", 0).await.expect("partition");
    assert!(pr_after_cancel.adding_replicas.is_empty());
    assert!(pr_after_cancel.removing_replicas.is_empty());
    assert_eq!(pr_after_cancel.replicas, original_replicas);
}
```

- [ ] **Step 7: Run tests**

```
wsl bash -c "cd /mnt/c/Users/Matt\\ Stone/git/crabka && RUSTC_WRAPPER= cargo test -p crabka-broker --test partition_reassignment -- --nocapture --test-threads=1"
```

Expected: 3 tests PASS.

- [ ] **Step 8: Lints + commit**

```bash
cargo fmt --check -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/tests/partition_reassignment.rs
git commit -m "$(cat <<'EOF'
test(broker): partition_reassignment alter + complete + list + cancel

Three 3-broker PLAINTEXT integration tests exercising the
AlterPartitionReassignments and ListPartitionReassignments RPCs
end-to-end. Uses metadata injection (slice-14 idiom) to drive
deterministic ISR-catchup completion without depending on
inter-broker fetch timing.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 10: `non_super_user_denied`

**Files:**
- Modify: `crates/broker/tests/partition_reassignment.rs`

- [ ] **Step 1: Append test + SASL helpers**

Reuse slice 14 T9's SASL/PLAIN single-broker scaffolding. Copy `sasl_plain_authenticate`, `drive_alter_reassignments_sasl_plain`, `start_single_broker_sasl_plaintext_with_users` from `crates/broker/tests/elect_leaders.rs`.

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_super_user_denied() {
    let (handle, _dir, addr) = start_single_broker_sasl_plaintext_with_users(
        /*super_user=*/ "admin",
        &[("admin", "admin-secret"), ("alice", "alice-secret")],
    ).await;

    // Seed an unrelated ACL to disable slice-13's compat shim.
    let unrelated = crabka_metadata::MetadataRecord::V1AccessControlEntry(
        crabka_metadata::AclEntry {
            resource_type: crabka_metadata::ResourceType::Topic,
            resource_name: "__compat_shim_disable__".into(),
            pattern_type: crabka_metadata::PatternType::Literal,
            principal: "User:admin".into(),
            host: "*".into(),
            operation: crabka_metadata::AclOperation::Read,
            permission_type: crabka_metadata::PermissionType::Allow,
        },
    );
    handle.submit_metadata_record_for_test(unrelated).await.expect("seed ACL");

    create_topic_as_admin(addr, "foo", 1, 1).await;
    wait_partition_exists(&handle, "foo", 0).await;

    let resp = drive_alter_reassignments_sasl_plain(
        addr, "alice", "alice-secret",
        vec![("foo", 0, Some(vec![1]))],
    ).await;
    assert_eq!(resp[0].1, vec![(0, 31)], "expected CLUSTER_AUTHORIZATION_FAILED for unauth alice");
}
```

- [ ] **Step 2: Run test**

```
wsl bash -c "cd /mnt/c/Users/Matt\\ Stone/git/crabka && RUSTC_WRAPPER= cargo test -p crabka-broker --test partition_reassignment non_super_user_denied -- --nocapture --test-threads=1"
```

Expected: PASS.

- [ ] **Step 3: Lints + commit**

```bash
cargo fmt --check -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/tests/partition_reassignment.rs
git commit -m "$(cat <<'EOF'
test(broker): non_super_user_denied for AlterPartitionReassignments

Single-broker SASL/PLAIN; alice has PLAIN creds but no ACLs. One
unrelated ACL seeded to disable slice-13 compat shim. Expects
per-partition CLUSTER_AUTHORIZATION_FAILED (31).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Batch 6 — JVM acceptance + final

### Task 11: JVM `kafka-reassign-partitions` end-to-end

**Files:**
- Modify: `crates/broker/tests/jvm_acceptance.rs`

- [ ] **Step 1: Append the test**

Slice 14 T10 added `start_three_broker_sasl_plaintext_jvm_cluster`. Reuse.

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_kafka_reassign_partitions_end_to_end() {
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";
    const TOPIC: &str = "crabka-reassign-itest";

    let (h1, h2, h3, _d1, _d2, _d3, addr) =
        start_three_broker_sasl_plaintext_jvm_cluster(ADMIN, ADMIN_PASS).await;
    nc_check_connectivity();

    let admin_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    let admin_mount = admin_props.mount_str();

    // Create rf=2 topic.
    docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &["kafka-topics", "--create", "--if-not-exists",
          "--topic", TOPIC, "--partitions", "1", "--replication-factor", "2",
          "--bootstrap-server", BOOTSTRAP, "--command-config", "/client.properties"],
    );
    wait_jvm_partition_exists(&[&h1, &h2, &h3], TOPIC, 0).await;
    let pr = h1.partition_record_for_test(TOPIC, 0).await.expect("partition");
    let initial = pr.replicas.clone();
    let new_node: i32 = (1..=3).find(|n| !initial.contains(&(*n as u64))).expect("free") as i32;
    let staying: i32 = *initial.first().unwrap() as i32;

    // Write reassignment JSON: move to [staying, new_node].
    let json = format!(
        r#"{{"version":1,"partitions":[{{"topic":"{TOPIC}","partition":0,"replicas":[{},{}]}}]}}"#,
        staying, new_node,
    );
    let json_file = write_temp_file("reassignment.json", &json);
    let json_mount = format!("{}:/reassignment.json", json_file.host_path);

    // Execute reassignment.
    let out = std::process::Command::new("docker")
        .args([
            "run", "--rm",
            "-v", &admin_mount,
            "-v", &json_mount,
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE_TXN,
            "kafka-reassign-partitions",
            "--execute",
            "--reassignment-json-file", "/reassignment.json",
            "--bootstrap-server", BOOTSTRAP,
            "--command-config", "/client.properties",
        ])
        .output()
        .expect("spawn kafka-reassign-partitions --execute");
    assert!(out.status.success(), "execute failed: stderr={}", String::from_utf8_lossy(&out.stderr));

    // Inject ISR including new_node to allow the reassignment task to complete
    // (WSL2 inter-broker networking caveat; see slice-14 T10 docstring).
    let pr_after = h1.partition_record_for_test(TOPIC, 0).await.expect("after alter");
    let injected = crabka_metadata::PartitionRecord {
        isr: vec![staying as u64, new_node as u64, *pr_after.removing_replicas.first().unwrap()],
        ..pr_after.clone()
    };
    h1.submit_metadata_record_for_test(
        crabka_metadata::MetadataRecord::V1Partition(injected)
    ).await.expect("inject");

    // Wait for completion.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        let pr = h1.partition_record_for_test(TOPIC, 0).await.expect("partition");
        if pr.adding_replicas.is_empty() && pr.removing_replicas.is_empty() {
            assert_eq!(
                pr.replicas.iter().copied().collect::<std::collections::HashSet<u64>>(),
                [staying as u64, new_node as u64].into_iter().collect::<std::collections::HashSet<u64>>(),
            );
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("reassignment did not complete within 20s; pr={:?}", pr);
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    // --verify reports completion.
    let verify_out = std::process::Command::new("docker")
        .args([
            "run", "--rm",
            "-v", &admin_mount,
            "-v", &json_mount,
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE_TXN,
            "kafka-reassign-partitions",
            "--verify",
            "--reassignment-json-file", "/reassignment.json",
            "--bootstrap-server", BOOTSTRAP,
            "--command-config", "/client.properties",
        ])
        .output()
        .expect("spawn kafka-reassign-partitions --verify");
    assert!(verify_out.status.success(), "verify failed: stderr={}", String::from_utf8_lossy(&verify_out.stderr));
    let stdout = String::from_utf8_lossy(&verify_out.stdout);
    assert!(stdout.contains("completed successfully") || stdout.contains("is complete"),
            "verify stdout did not indicate success: {stdout}");
}
```

**Note on the `--admin.config` vs `--command-config` quirk** (slice 14 T10 learning): `kafka-reassign-partitions` does take `--command-config` (not `--admin.config`). Verify by reading the help output in cp-kafka:7.5 if unsure; either way, the test asserts on exit code.

**Helper `write_temp_file`:** if slice 14 has it, reuse. Otherwise, write `tempfile::NamedTempFile` + return host path. Slice 14's `write_client_props` is structurally similar.

- [ ] **Step 2: Run via WSL**

```
wsl bash -c "cd /mnt/c/Users/Matt\\ Stone/git/crabka && RUSTC_WRAPPER= cargo test -p crabka-broker --test jvm_acceptance jvm_kafka_reassign_partitions_end_to_end -- --ignored --nocapture --test-threads=1"
```

Expected: PASS in 30-90 seconds (Docker startup + cluster + 2× JVM tool invocations).

- [ ] **Step 3: Lints + commit**

```bash
cargo fmt --check -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/tests/jvm_acceptance.rs
git commit -m "$(cat <<'EOF'
test(jvm): kafka-reassign-partitions --execute + --verify

Three-broker SASL/PLAINTEXT cluster; create rf=2 topic; trigger
reassignment via the JVM admin CLI (cp-kafka:7.5.0); inject ISR to
unblock completion under WSL2's inter-broker networking limitation;
verify --verify reports completion.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 12: Sweep + docs + PR

**Files:**
- Modify: `README.md`
- Modify: `STATUS.md`

- [ ] **Step 1: Full local matrix**

```
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo build --workspace
cargo test --workspace --exclude crabka-client-core --exclude crabka-log --exclude crabka-broker
cargo test -p crabka-broker --lib
cargo test -p crabka-broker --tests
```

All green. (JVM acceptance tests stay `#[ignore]`-tagged.)

- [ ] **Step 2: WSL JVM acceptance (optional but recommended)**

```
wsl bash -c "cd /mnt/c/Users/Matt\\ Stone/git/crabka && RUSTC_WRAPPER= cargo test -p crabka-broker --test jvm_acceptance -- --ignored --nocapture --test-threads=1"
```

All green. If `jvm_inter_broker_*` (slice 12b) flake on WSL networking, document and rely on Linux CI.

- [ ] **Step 3: `README.md` — append slice 15 entry**

Under "Slices delivered" (find the list), append:

```markdown
- **Slice 15** — partition reassignment: `AlterPartitionReassignments` (api_key 45) and
  `ListPartitionReassignments` (api_key 46) per KIP-455 with the full two-phase
  state machine. `PartitionRecord` gains `adding_replicas` + `removing_replicas`.
  Background completion task on the controller leader watches the metadata image
  and, when `adding ⊆ ISR`, atomically transitions the partition to the target
  replica set (handing off leadership first if needed). JVM
  `kafka-reassign-partitions --execute|--verify` works end-to-end. Throttled
  replication (KIP-73) deferred to slice 15b.
```

- [ ] **Step 4: `STATUS.md` — append section**

```markdown
## Slice 15 — Partition reassignment (2026-05-15)

- Pure-logic `process_one_partition` in `crates/broker/src/handlers/alter_partition_reassignments.rs` turns one alter row into an intermediate `PartitionRecord` or a wire error code. Covers start, cancellation, RF-change validation, leader-revert on cancel; 6 unit tests.
- Pure-logic `compute_reassignment_progress` in `crates/broker/src/reassignment.rs` walks every in-flight reassignment, returns completion or leader-handoff records. 8 unit tests covering wait/complete/handoff/idle/multi-partition.
- New `handlers/list_partition_reassignments.rs` (api_key 46). Cluster Describe gate. Filter by topic (with empty `partition_indexes` meaning "all") or list everything in flight.
- `MetadataImage` gains `reassignments_in_flight()` + 4 unit tests. `PartitionRecord` gains `adding_replicas` and `removing_replicas` (greenfield project; no `#[serde(default)]` shim per `CLAUDE.md`).
- Inline-intercept dispatch arms for api_keys 45 + 46 follow the slice-13 ACL + slice-14 ElectLeaders pattern (both need `&Principal` + `&SocketAddr`).
- Background reassignment-completion task always spawned from `Broker::start`; per-tick `is_leader()` gate makes it a no-op on followers. Image-driven (not timer-driven) — wakes on every metadata apply.
- Replicator transparently handles `adding_replicas` because the existing supervisor iterates `replicas` (the union list during reassignment).
- 4 broker integration tests covering alter+complete, list, cancel, and the deny path. 1 JVM acceptance test drives `kafka-reassign-partitions --execute` + `--verify` against a 3-broker SASL/PLAINTEXT cluster (cp-kafka:7.5).
- Out of scope: KIP-73 throttled replication, KIP-113 log-dir reassignment, KIP-841 force-elect.
```

- [ ] **Step 5: Commit docs**

```bash
git add README.md STATUS.md
git commit -m "$(cat <<'EOF'
docs(slice-15): README + STATUS entry

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 6: Push + open PR**

```
git push -u origin feature/partition-reassignment-15
gh pr create --base main --head feature/partition-reassignment-15 \
  --title "Slice 15: Partition reassignment (KIP-455)" \
  --body "$(cat <<'EOF'
## Summary

KIP-455 partition reassignment with the full two-phase URP-aware state machine:

1. **`AlterPartitionReassignments` (api_key 45)** — start, replace-in-flight, or cancel (`replicas: null`). Honors `allow_replication_factor_change`. Cluster Alter authorize gate.
2. **`ListPartitionReassignments` (api_key 46)** — filter by topic or list everything in flight. Cluster Describe gate.
3. **Two-phase state machine** — `PartitionRecord` gains `adding_replicas` + `removing_replicas`. Background task on the controller leader observes ISR catch-up and atomically transitions to the target replica set. Hands off leadership first when the current leader is being removed.

JVM `kafka-reassign-partitions --execute|--verify` works end-to-end against a 3-broker SASL/PLAINTEXT cluster.

## Verified

- 18 new unit tests across `process_one_partition` (6), `compute_reassignment_progress` (8), `reassignments_in_flight` (4).
- 4 broker integration tests in \`tests/partition_reassignment.rs\`.
- 1 new JVM acceptance test driving \`kafka-reassign-partitions --execute\` and \`--verify\`.
- Workspace \`cargo fmt --check\`, \`cargo clippy --workspace --all-targets -- -D warnings\`, \`cargo test --workspace\` all green.

## Out of scope

KIP-73 throttled replication (deferred to slice 15b), KIP-113 log-dir reassignment, KIP-841 force-elect.

## Plan / spec

- Spec: \`docs/superpowers/specs/2026-05-15-crabka-partition-reassignment-15-design.md\`
- Plan: \`docs/superpowers/plans/2026-05-15-crabka-partition-reassignment-15.md\` (12 tasks across 6 batches)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 7: Confirm CI passes** — watch for new clippy lints on the workspace (slice 14 caught a few `doc_markdown` / `too_many_lines` on integration tests).

---

## Notes for the executing agent

1. **Branch:** all work on `feature/partition-reassignment-15`. Do NOT push to main.

2. **CLAUDE.md compatibility rule** is load-bearing for T1. Just add the fields. No `#[serde(default)]`, no migration helpers, no compatibility shims. Wipe local data dirs as needed during development.

3. **`replicas = union(old, new)` invariant** is what makes the replicator work without changes. The supervisor iterates `replicas`; new replicas in `adding_replicas` are also in `replicas`, so they start fetching automatically.

4. **`leader_epoch` bump policy:** only bumps on actual leader change (per Kafka semantics). T3's `start_path` does NOT bump epoch; T4's leader-handoff branch DOES; T4's completion-only branch DOES NOT.

5. **Metadata injection for tests** — slice 14 established this pattern (T8 `unclean_election_via_wire_picks_alive_replica`). Use it freely in T9/T10/T11 to avoid flake from inter-broker fetch timing.

6. **WSL2 `host.docker.internal` caveat** — same as slice 12b/14 JVM tests. CI handles it; locally rely on metadata injection.

7. **`#![allow(dead_code)]` lifecycle:** T3, T4, T5, T6 each add a module that isn't dispatched yet. Keep the module-level allow until T7 wires dispatch, then remove it.

8. **Don't generalize prematurely.** T9/T10 may be tempted to extract a `partition_reassignment_helpers.rs` shared with `elect_leaders.rs`. Don't — Rust integration tests can't share modules across files, and the copy is bounded.
