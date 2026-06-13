# KIP-1071 Streams↔Classic slice 2 (cold downgrade + admin type-awareness) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the **streams → classic** cold-downgrade direction (the mirror of slice 1's classic→streams upgrade) and make the generic admin handlers (`ListGroups`/`DescribeGroups`/`DeleteGroups`) consult the `group_type` lock so a converted group is no longer mislabeled `classic` or deleted out from under a live streams group.

**Architecture:** A pre-step in the classic `JoinGroup` handler converts a drained `Streams`-locked group to classic (tombstone k15–21, force the lock to `Classic`, drop the streams actor) or rejects it if live members remain. The tombstone batch is built directly from the streams key encoders. The three admin handlers consult `group_type()`: `ListGroups` excludes `Streams`-locked ids from the classic pass; `DeleteGroups` becomes type-aware (deletes a streams group through the streams path); `DescribeGroups` reports a streams group's identity instead of the classic-actor projection.

**Tech Stack:** Rust 2024, `crabka-broker`, tokio actors + mpsc, `OffsetsLog::append(RecordBatch)`, in-process broker test harness (`crates/broker/tests`).

**Spec:** `docs/superpowers/specs/2026-06-12-kip-1071-streams-classic-downgrade-design.md`

**Nightly rustfmt:** CI gates on `cargo +nightly fmt --all -- --check` (stable fmt gives false-clean — it ignores unstable `rustfmt.toml` options). Use nightly fmt in the gate.

---

## File / responsibility map

| File | Responsibility | Task |
|------|----------------|------|
| `crates/broker/src/coordinator/unified/streams/migration.rs` | `streams_records_tombstone_batch` + `DowngradeOutcome` | Task 1 |
| `crates/broker/src/handlers/list_groups.rs` | skip `Streams`-locked ids in the classic pass | Task 2 |
| `crates/broker/src/handlers/describe_groups.rs` | `Streams`-lock branch → report streams identity | Task 3 |
| `crates/broker/src/coordinator/unified/mod.rs` | `mark_classic_after_streams_downgrade` + `try_convert_streams_to_classic` | Task 4 |
| `crates/broker/src/coordinator/unified/mod.rs` (`delete_group`) + `crates/broker/src/coordinator/mod.rs` (`DeleteGroupError`) + `crates/broker/src/handlers/delete_groups.rs` | type-aware streams delete | Task 5 |
| `crates/broker/src/handlers/join_group.rs` | downgrade pre-step | Task 6 |
| `crates/broker/tests/streams_classic_downgrade.rs` (new) + `.github/workflows/ci.yml` | integration tests incl. restart-replay | Task 7 |
| (verification only) | nightly-fmt + clippy + build + regression | Task 8 |

## Execution notes (batches)

Tasks 4 and 5 BOTH edit `coordinator/unified/mod.rs` — they conflict and must run **sequentially** (4 before 5). Everything else parallelizes by disjoint file sets:

- **Batch A (parallel):** Task 1 (`migration.rs`), Task 2 (`list_groups.rs`), Task 3 (`describe_groups.rs`). No shared files.
- **Batch B (single):** Task 4 (`mod.rs` downgrade methods) — depends on Task 1.
- **Batch C (parallel):** Task 5 (`mod.rs` delete + `coordinator/mod.rs` + `delete_groups.rs`) and Task 6 (`join_group.rs`). Both depend on Task 4; disjoint files (Task 5's `mod.rs` edit is in a different region than Task 4's, and Task 4 is already committed).
- **Batch D (sequential):** Task 7 (integration tests) then Task 8 (verification). Depend on everything.

---

## Task 1: streams-records tombstone batch + downgrade outcome enum

**Files:**
- Modify: `crates/broker/src/coordinator/unified/streams/migration.rs`

The downgrade and the type-aware delete both need a batch that tombstones every streams record for a group. `PendingStreamsRecords`' group-level fields are `Option<Value>` (present-or-absent) and cannot express a group-level tombstone, so build the batch directly from the key encoders.

- [ ] **Step 1: Write the failing unit tests**

Append to the `#[cfg(test)] mod tests` at the bottom of `migration.rs`:

```rust
#[test]
fn streams_tombstone_batch_group_level_only() {
    let batch = streams_records_tombstone_batch("g", &[], 123);
    // k15 GroupMetadata, k17 Topology, k18 PartitionMetadata, k19 TargetAssignmentMetadata.
    assert_eq!(batch.records.len(), 4, "four group-level tombstones");
    assert_eq!(batch.max_timestamp, 123);
    assert_eq!(batch.last_offset_delta, 3);
    for r in &batch.records {
        assert!(r.key.is_some(), "every record carries a key");
        assert!(r.value.is_none(), "every record is a tombstone (null value)");
    }
    // The first record is the load-bearing k15 GroupMetadata tombstone.
    let k15 = batch.records[0].key.as_ref().unwrap();
    assert_eq!(&k15[..2], &15i16.to_be_bytes(), "k15 GroupMetadata key version");
}

#[test]
fn streams_tombstone_batch_includes_per_member_records() {
    let batch = streams_records_tombstone_batch("g", &["m1".to_string()], 1);
    // 4 group-level + k16/k20/k21 for m1 = 7.
    assert_eq!(batch.records.len(), 7, "group-level + 3 per-member tombstones");
    assert!(batch.records.iter().all(|r| r.value.is_none()));
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p crabka-broker --lib coordinator::unified::streams::migration::tests::streams_tombstone`
Expected: FAIL — `streams_records_tombstone_batch` does not exist (E0425).

- [ ] **Step 3: Add the enum and the function**

At the top of `migration.rs`, widen the imports (the file currently only imports `RecordBatch`):

```rust
use bytes::Bytes;
use crabka_protocol::records::{Record, RecordBatch};

use super::persistence::{
    encode_current_member_assignment_key, encode_group_metadata_key,
    encode_member_metadata_key, encode_partition_metadata_key,
    encode_target_assignment_member_key, encode_target_assignment_metadata_key,
    encode_topology_key,
};
```

Add the outcome enum next to the existing `ConvertOutcome`:

```rust
/// Result of inspecting a `group_id` for a streams→classic cold downgrade.
/// The mirror of [`ConvertOutcome`] for the opposite direction.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum DowngradeOutcome {
    /// Not a streams group — serve the classic `JoinGroup` normally.
    NotStreams,
    /// Was a drained streams group; converted in place to classic.
    Converted,
    /// Streams group has live members — online streams migration is unsupported.
    RejectLiveMembers,
}
```

Add the tombstone-batch builder:

```rust
/// Build the batch that tombstones every streams record for `group_id`, used by
/// the streams→classic downgrade and the type-aware streams delete. The
/// group-level keys — k15 `GroupMetadata`, k17 `Topology`, k18
/// `PartitionMetadata`, k19 `TargetAssignmentMetadata` — are tombstoned
/// unconditionally (a tombstone for a never-written key is a harmless replay
/// no-op, and k15's tombstone is load-bearing: a surviving k15 would resurrect
/// the group as streams). Each id in `member_ids` additionally tombstones its
/// k16/k20/k21; a drained group has none (members tombstone their own per-member
/// records on leave), so `member_ids` is typically empty.
///
/// Built directly from the key encoders rather than via `PendingStreamsRecords`,
/// whose group-level fields are `Option<Value>` (present-or-absent) with no way
/// to express a group-level null-value tombstone.
pub(crate) fn streams_records_tombstone_batch(
    group_id: &str,
    member_ids: &[String],
    now_ms: i64,
) -> RecordBatch {
    let mut keys: Vec<Bytes> = vec![
        encode_group_metadata_key(group_id),
        encode_topology_key(group_id),
        encode_partition_metadata_key(group_id),
        encode_target_assignment_metadata_key(group_id),
    ];
    for mid in member_ids {
        keys.push(encode_member_metadata_key(group_id, mid));
        keys.push(encode_target_assignment_member_key(group_id, mid));
        keys.push(encode_current_member_assignment_key(group_id, mid));
    }
    let records: Vec<Record> = keys
        .into_iter()
        .enumerate()
        .map(|(i, key)| Record {
            offset_delta: i32::try_from(i).expect("batch size fits i32"),
            timestamp_delta: 0,
            key: Some(key),
            value: None, // tombstone
            ..Default::default()
        })
        .collect();
    let last_offset_delta = i32::try_from(records.len().saturating_sub(1)).unwrap_or(0);
    RecordBatch {
        max_timestamp: now_ms,
        records,
        last_offset_delta,
        ..RecordBatch::default()
    }
}
```

> The `encode_*_key` functions are `pub` in `streams/persistence.rs` (lines 56–112). `Record`/`RecordBatch` field names (`offset_delta`, `timestamp_delta`, `key`, `value`, `max_timestamp`, `last_offset_delta`) are used verbatim by `PendingStreamsRecords::into_batch` (`persistence.rs:741-795`) — copy from there if a name differs.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p crabka-broker --lib coordinator::unified::streams::migration::tests`
Expected: PASS (existing slice-1 test + the two new ones).

- [ ] **Step 5: Commit**

```bash
git -C <worktree> add crates/broker/src/coordinator/unified/streams/migration.rs
git -C <worktree> commit -m "feat(broker): streams-records tombstone batch + DowngradeOutcome (KIP-1071 slice 2)"
```

---

## Task 2: ListGroups — exclude Streams-locked ids from the classic pass

**Files:**
- Modify: `crates/broker/src/handlers/list_groups.rs` (the classic-pass loop, ~line 68)

A converted group keeps a drained **classic-kind** offset-home actor in `groups`, so the classic snapshot pass (`list_groups()` → `ClassicInspect`) still enumerates it and, running first, wins the `emitted` dedup → mislabeled `classic`. Skip `Streams`-locked ids so the streams pass (already correct) is the sole emitter.

- [ ] **Step 1: Add the import**

At the top of `list_groups.rs` (with the other `use crate::...` lines):

```rust
use crate::coordinator::unified::GroupType;
```

- [ ] **Step 2: Skip Streams-locked ids in the classic pass**

In `handle`, the classic pass begins `for s in snapshots {` (~line 68). Insert as the FIRST statement inside the loop, before the `authorized(...)` check:

```rust
    for s in snapshots {
        // KIP-1071: a Streams-locked group keeps its drained classic-kind
        // offset-home actor in this classic snapshot. Report it via the streams
        // pass (group_type="streams"), never here as "classic".
        if broker.group_coordinator.group_type(&s.group_id) == Some(GroupType::Streams) {
            continue;
        }
        if !authorized(s.group_id.as_str()) {
            continue;
        }
        // ... unchanged ...
```

- [ ] **Step 3: Build to verify it compiles**

Run: `cargo build -p crabka-broker`
Expected: success. (Behavioral coverage is Task 7's integration test §5; a converted group lists once as `streams`.)

- [ ] **Step 4: Commit**

```bash
git -C <worktree> add crates/broker/src/handlers/list_groups.rs
git -C <worktree> commit -m "fix(broker): ListGroups reports a converted streams group as streams, not classic (KIP-1071)"
```

---

## Task 3: DescribeGroups — report a Streams-locked group's streams identity

**Files:**
- Modify: `crates/broker/src/handlers/describe_groups.rs`

For a `Streams`-locked group, `describe_group()` (api 15) finds the drained classic offset-home actor and projects it as a classic/consumer `Empty` group. Branch on the lock and report the streams identity instead. Exact wire shape is an empirical open item (spec §7.4); the firm requirement is "no longer reported as classic/consumer".

- [ ] **Step 1: Add imports**

At the top of `describe_groups.rs`:

```rust
use tokio::sync::oneshot;

use crate::coordinator::unified::GroupType;
use crate::coordinator::unified::streams::actor::StreamsGroupActorMessage;
```

- [ ] **Step 2: Branch on the type lock before the classic describe**

In `handle`, the per-group body currently goes straight to `let Some(snap) = broker.group_coordinator.describe_group(&gid).await else {...}` (~line 61). Insert this block immediately BEFORE that line (after the ACL preamble):

```rust
        // KIP-1071: a Streams-locked group's offset home is a drained classic
        // actor; describing it via the classic projection would mislabel it.
        // Report its streams identity (full task detail lives in
        // StreamsGroupDescribe, api 89). Exact protocol_type/state is matched
        // empirically (spec §7.4); the firm contract is "not classic/consumer".
        if broker.group_coordinator.group_type(&gid) == Some(GroupType::Streams) {
            if let Some(handle) = broker.group_coordinator.find_streams(&gid) {
                let (tx, rx) = oneshot::channel();
                if handle
                    .tx
                    .send(StreamsGroupActorMessage::Describe { reply: tx })
                    .await
                    .is_ok()
                    && let Ok(view) = rx.await
                {
                    groups.push(DescribedGroup {
                        group_id: gid,
                        protocol_type: "streams".into(),
                        group_state: view.group_state,
                        error_code: codes::NONE,
                        ..Default::default()
                    });
                    continue;
                }
            }
            // Streams-locked but no live streams actor (e.g. just downgraded) →
            // fall through to the classic describe path below.
        }

        let Some(snap) = broker.group_coordinator.describe_group(&gid).await else {
```

> `StreamsDescribeView.group_state` is a `String` (`streams/actor.rs:85`) and `DescribedGroup.group_state` is a `String`, so they map directly. `find_streams` is `mod.rs:415`.

- [ ] **Step 3: Build to verify it compiles**

Run: `cargo build -p crabka-broker`
Expected: success.

- [ ] **Step 4: Commit**

```bash
git -C <worktree> add crates/broker/src/handlers/describe_groups.rs
git -C <worktree> commit -m "fix(broker): DescribeGroups reports a Streams-locked group as streams (KIP-1071)"
```

---

## Task 4: coordinator downgrade — forced type flip + conversion method

**Files:**
- Modify: `crates/broker/src/coordinator/unified/mod.rs` (add a method near `mark_streams_after_upgrade` ~line 218 and `try_convert_classic_to_streams` ~line 427; add a `#[test]` in the existing `#[cfg(test)] mod tests`)

**Depends on Task 1** (`DowngradeOutcome`, `streams_records_tombstone_batch`).

- [ ] **Step 1: Write the failing unit test**

In the `#[cfg(test)] mod tests` of `mod.rs` (the same module that holds slice-1's `mark_streams_after_upgrade_forces_streams_over_classic` ~line 1167), add:

```rust
#[test]
fn mark_classic_after_streams_downgrade_forces_classic_over_streams() {
    let c = GroupCoordinator::for_tests();
    c.mark_streams("g");
    assert_eq!(c.group_type("g"), Some(GroupType::Streams));
    // mark_classic is first-mark-wins, so it must NOT override an existing lock:
    c.mark_classic("g");
    assert_eq!(c.group_type("g"), Some(GroupType::Streams));
    // The forced downgrade variant MUST override it:
    c.mark_classic_after_streams_downgrade("g");
    assert_eq!(c.group_type("g"), Some(GroupType::Classic));
}
```

> Use the same coordinator constructor the surrounding tests use (slice-1's mark test used `GroupCoordinator::for_tests()`; mirror whatever it actually calls). Keep the three assertions.

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p crabka-broker --lib coordinator::unified::tests::mark_classic_after_streams_downgrade`
Expected: FAIL — method does not exist (E0599).

- [ ] **Step 3: Add the forced type-flip method**

In `mod.rs`, immediately after `mark_streams_after_upgrade` (~line 222):

```rust
/// After an in-place streams→classic downgrade (KIP-1071), drop the streams
/// seed so a respawn does not re-hydrate the group as streams, and record it as
/// classic. Unlike [`Self::mark_classic`] (first-mark-wins via `or_insert`),
/// this FORCES the type to `Classic`, overriding any prior `Streams` lock — the
/// mirror of [`Self::mark_streams_after_upgrade`]. Drops the **streams** seeds
/// (`streams_seeds`/`streams_seeds_cache`), not the consumer `seeds` that
/// [`Self::mark_classic_after_downgrade`] drops.
pub fn mark_classic_after_streams_downgrade(&self, group_id: &str) {
    self.streams_seeds.remove(group_id);
    self.streams_seeds_cache.remove(group_id);
    self.group_types.insert(group_id.into(), GroupType::Classic);
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p crabka-broker --lib coordinator::unified::tests::mark_classic_after_streams_downgrade`
Expected: PASS.

- [ ] **Step 5: Add the conversion method**

In `mod.rs`, immediately after `try_convert_classic_to_streams` (~line 463), add the mirror method:

```rust
/// KIP-1071 cold downgrade: if `group_id` is a drained streams group, convert
/// it to a classic group in place — tombstone its streams records (k15–21),
/// force the type lock to `Classic`, and drop the streams actor. Committed
/// offsets (k0/k1) and the offset-home `groups` entry survive. Returns
/// `NotStreams` for non-streams groups (caller serves the classic `JoinGroup`
/// normally), `Converted` after a successful flip, or `RejectLiveMembers` when
/// the streams group still has live members (online streams migration is
/// unsupported in Kafka). The mirror of [`Self::try_convert_classic_to_streams`].
pub(crate) async fn try_convert_streams_to_classic(
    self: &Arc<Self>,
    group_id: &str,
    now_ms: i64,
) -> Result<streams::migration::DowngradeOutcome, crate::error::BrokerError> {
    use streams::actor::StreamsGroupActorMessage;
    use streams::migration::{DowngradeOutcome, streams_records_tombstone_batch};

    if self.group_type(group_id) != Some(GroupType::Streams) {
        return Ok(DowngradeOutcome::NotStreams);
    }

    // Inspect the live streams actor (if any) for remaining members.
    let mut member_ids: Vec<String> = Vec::new();
    if let Some(handle) = self.find_streams(group_id) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if handle
            .tx
            .send(StreamsGroupActorMessage::Describe { reply: tx })
            .await
            .is_ok()
            && let Ok(view) = rx.await
        {
            if !view.members.is_empty() {
                return Ok(DowngradeOutcome::RejectLiveMembers);
            }
            member_ids = view.members.into_iter().map(|m| m.member_id).collect();
        }
    }

    // Drained streams group → convert. Tombstone k15–21, flip the lock to
    // Classic, drop the streams actor. The offset-home `groups` entry stays.
    let batch = streams_records_tombstone_batch(group_id, &member_ids, now_ms);
    self.offsets_log.append(batch).await?;
    self.mark_classic_after_streams_downgrade(group_id);
    self.streams_groups.remove(group_id);
    Ok(DowngradeOutcome::Converted)
}
```

> `view.members` is `Vec<StreamsDescribeMember>` and each has `member_id` (`streams/actor.rs:96-100`, used in `streams_group_describe.rs:136-138`). A drained group reports an empty `members`, so `member_ids` is empty (group-level tombstones only). `offsets_log.append` is the same call slice-1 uses at `mod.rs:460`.

- [ ] **Step 6: Build to verify it compiles**

Run: `cargo build -p crabka-broker`
Expected: success. (End-to-end behavior is covered by Task 7.)

- [ ] **Step 7: Commit**

```bash
git -C <worktree> add crates/broker/src/coordinator/unified/mod.rs
git -C <worktree> commit -m "feat(broker): try_convert_streams_to_classic + forced downgrade type flip (KIP-1071 slice 2)"
```

---

## Task 5: type-aware DeleteGroups (full streams-aware delete)

**Files:**
- Modify: `crates/broker/src/coordinator/mod.rs` (`DeleteGroupError` enum — add `Internal`)
- Modify: `crates/broker/src/coordinator/unified/mod.rs` (`delete_group` branch + new `delete_streams_group`)
- Modify: `crates/broker/src/handlers/delete_groups.rs` (map `Internal`)

**Depends on Task 1** (`streams_records_tombstone_batch`). Runs after Task 4 (same `mod.rs`).

- [ ] **Step 1: Add the `Internal` error variant**

In `crates/broker/src/coordinator/mod.rs`, the `DeleteGroupError` enum (line 17) currently has `NotFound` and `NonEmpty`. Add:

```rust
pub enum DeleteGroupError {
    NotFound,
    NonEmpty,
    /// A durable side effect of the delete (e.g. tombstone append) failed.
    Internal,
}
```

- [ ] **Step 2: Map `Internal` in the handler**

In `crates/broker/src/handlers/delete_groups.rs`, the match at line 50 is exhaustive; add the arm:

```rust
        let error_code = match broker.group_coordinator.delete_group(&gid).await {
            Ok(()) => codes::NONE,
            Err(DeleteGroupError::NotFound) => codes::GROUP_ID_NOT_FOUND,
            Err(DeleteGroupError::NonEmpty) => codes::NON_EMPTY_GROUP,
            Err(DeleteGroupError::Internal) => codes::UNKNOWN_SERVER_ERROR,
        };
```

- [ ] **Step 3: Make `delete_group` type-aware and add `delete_streams_group`**

(The streams-delete behavior — empty deletes + tombstones, non-empty rejects, no offset-home removal for a live streams group — is integration-tested end-to-end in Task 7 Step 3, the same way slice 1 covered its conversion method via integration rather than a harness-heavy coordinator unit test.)

In `mod.rs`, change `delete_group` (line 543) to branch at the top, leaving the existing classic body intact:

```rust
pub async fn delete_group(&self, group_id: &str) -> Result<(), DeleteGroupError> {
    // KIP-1071: a Streams-locked group is deleted through the streams path —
    // never fall through to the classic path, which would remove the offset-home
    // `groups` entry out from under a live streams group.
    if self.group_type(group_id) == Some(GroupType::Streams) {
        return self.delete_streams_group(group_id).await;
    }
    let handle = self.find(group_id).ok_or(DeleteGroupError::NotFound)?;
    // ... existing classic body unchanged ...
}
```

Add the new method right after `delete_group`:

```rust
/// Delete a **streams** group (KIP-1071): `NonEmpty` if the streams actor still
/// has live members; `NotFound` if no streams actor exists for the id; else
/// tombstone its records (k15–21), drop the streams actor, and remove the
/// offset-home `groups` entry. `Internal` if the tombstone append fails.
async fn delete_streams_group(&self, group_id: &str) -> Result<(), DeleteGroupError> {
    let handle = self
        .find_streams(group_id)
        .ok_or(DeleteGroupError::NotFound)?;
    let (tx, rx) = oneshot::channel();
    handle
        .tx
        .send(streams::actor::StreamsGroupActorMessage::Describe { reply: tx })
        .await
        .map_err(|_| DeleteGroupError::NotFound)?;
    let view = rx.await.map_err(|_| DeleteGroupError::NotFound)?;
    if !view.members.is_empty() {
        return Err(DeleteGroupError::NonEmpty);
    }
    let member_ids: Vec<String> = view.members.into_iter().map(|m| m.member_id).collect();
    let batch = streams::migration::streams_records_tombstone_batch(
        group_id,
        &member_ids,
        crate::time_util::now_ms(),
    );
    self.offsets_log
        .append(batch)
        .await
        .map_err(|_| DeleteGroupError::Internal)?;
    self.streams_groups.remove(group_id);
    self.groups.remove(group_id);
    self.streams_seeds.remove(group_id);
    self.streams_seeds_cache.remove(group_id);
    Ok(())
}
```

> Per spec §4.6, the `group_types` lock is intentionally left in place (matching the classic `delete_group`, which never clears `group_types`); the §4.2 pre-steps degrade gracefully against a stale `Streams` lock with no live actor. `oneshot` is already imported in `mod.rs` (used by `list_groups`/`describe_group`). `now_ms` is `crate::time_util::now_ms` (used by the heartbeat handler).

- [ ] **Step 4: Build to verify it compiles**

Run: `cargo build -p crabka-broker`
Expected: success. (Behavioral coverage — empty-deletes-and-tombstones, non-empty-rejects, live-streams-group-not-removed — comes from Task 7 Step 3.)

- [ ] **Step 5: Commit**

```bash
git -C <worktree> add crates/broker/src/coordinator/mod.rs crates/broker/src/coordinator/unified/mod.rs crates/broker/src/handlers/delete_groups.rs
git -C <worktree> commit -m "feat(broker): type-aware DeleteGroups deletes streams groups via the streams path (KIP-1071 slice 2)"
```

---

## Task 6: wire the downgrade pre-step into JoinGroup

**Files:**
- Modify: `crates/broker/src/handlers/join_group.rs` (before `mark_classic`, ~line 64)

**Depends on Task 4** (`try_convert_streams_to_classic`). Disjoint from Task 5 → may run in parallel with it.

- [ ] **Step 1: Add the time import**

At the top of `join_group.rs` (with the other `use crate::...` lines):

```rust
use crate::time_util::now_ms;
```

- [ ] **Step 2: Insert the downgrade pre-step**

In `handle`, immediately BEFORE `broker.group_coordinator.mark_classic(&req.group_id);` (line 64), insert:

```rust
    // KIP-1071 cold downgrade: a classic JoinGroup for a drained streams group
    // converts it in place to a classic group; a streams group with live members
    // is rejected (online streams migration is unsupported). Non-streams group
    // ids pass through unchanged.
    match broker
        .group_coordinator
        .try_convert_streams_to_classic(&req.group_id, now_ms())
        .await
    {
        Ok(crate::coordinator::unified::streams::migration::DowngradeOutcome::RejectLiveMembers) => {
            return encode(
                version,
                &JoinGroupResponse {
                    error_code: codes::GROUP_ID_NOT_FOUND,
                    ..Default::default()
                },
            );
        }
        Ok(_) => {} // NotStreams | Converted → serve the classic JoinGroup below
        Err(e) => return Err(e),
    }

    broker.group_coordinator.mark_classic(&req.group_id);
```

> No streams feature-gate is needed: the pre-step is inert (`NotStreams`) for any non-`Streams` group, and converting away from streams is always valid. Rejection code `GROUP_ID_NOT_FOUND` mirrors slice 1 (spec §4.5); the slice-1 reject test already exercises the same code for the upgrade direction.

- [ ] **Step 3: Build to verify it compiles**

Run: `cargo build -p crabka-broker`
Expected: success.

- [ ] **Step 4: Commit**

```bash
git -C <worktree> add crates/broker/src/handlers/join_group.rs
git -C <worktree> commit -m "feat(broker): convert drained streams group to classic on JoinGroup (KIP-1071 slice 2)"
```

---

## Task 7: integration tests + CI coverage

**Files:**
- Create: `crates/broker/tests/streams_classic_downgrade.rs`
- Modify: `.github/workflows/ci.yml`

**Depends on Tasks 1–6.** Mirror the slice-1 harness in `crates/broker/tests/streams_classic_upgrade.rs` and the restart pattern in `crates/broker/tests/consumer_group_next_gen_persistence.rs`.

- [ ] **Step 1: Create the test file with the harness + downgrade tests**

Create `crates/broker/tests/streams_classic_downgrade.rs`. Copy the helper block from `streams_classic_upgrade.rs` (`boot`, `connect`, `create_topic`, `finalize_streams_version`, `topic_id_for`, `classic_join_sync`, `topology`, `first_join`, `follow_up`, `streams_join_and_converge`, and the error-code consts), then add a streams-leave helper and the tests:

```rust
#![allow(clippy::pedantic)]

//! KIP-1071 streams→classic cold downgrade + admin type-awareness integration
//! tests (slice 2). A drained streams group converts to classic on a classic
//! JoinGroup (offsets preserved); a streams group with a live member rejects it;
//! and the admin handlers (List/Describe/Delete) respect the type lock.

// (imports + helper block copied verbatim from streams_classic_upgrade.rs)
// (add `use crabka_broker::{BootstrapMode};` for the restart test)

/// Send a streams LeaveGroup (member_epoch -1) so the group drains.
async fn streams_leave(client: &Client, group: &str, member_id: &str) {
    let _ = client
        .send(StreamsGroupHeartbeatRequest {
            group_id: group.into(),
            member_id: member_id.into(),
            member_epoch: -1,
            ..Default::default()
        })
        .await
        .expect("streams leave heartbeat");
}

/// A drained streams group with a committed offset converts to classic on a
/// classic JoinGroup; the committed offset survives the flip.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drained_streams_group_downgrades_and_preserves_offsets() {
    let (broker, bootstrap, _dir) = boot().await;
    let streams_client = connect(&bootstrap).await;
    let classic_client = connect(&bootstrap).await;

    finalize_streams_version(&streams_client).await;
    create_topic(&streams_client, "in", 1).await;
    let topic_id = topic_id_for(&streams_client, "in").await;

    // ── Phase 1: form a streams group, commit offset 42, then leave. ──
    let (member_id, resp) =
        streams_join_and_converge(&streams_client, "g", topology("in"), 1, 15).await;
    assert!(resp.error_code == ERR_NONE, "streams converge: {resp:?}");
    assert!(
        broker.group_type_for_test("g")
            == Some(crabka_broker::coordinator::unified::GroupType::Streams),
        "precondition: group must be Streams before downgrade"
    );

    // Commit offset 42 for the live streams member (member_epoch from converge).
    let cr = streams_client
        .send(OffsetCommitRequest {
            group_id: "g".into(),
            generation_id_or_member_epoch: resp.member_epoch,
            member_id: member_id.clone(),
            topics: vec![OffsetCommitRequestTopic {
                name: "in".into(),
                topic_id,
                partitions: vec![OffsetCommitRequestPartition {
                    partition_index: 0,
                    committed_offset: 42,
                    committed_leader_epoch: 0,
                    committed_metadata: Some(String::new()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("OffsetCommit");
    assert!(
        cr.topics[0].partitions[0].error_code == ERR_NONE,
        "OffsetCommit for streams member failed: {cr:?}"
    );

    streams_leave(&streams_client, "g", &member_id).await;

    // ── Phase 2: classic JoinGroup for the same id → downgrade to classic. ──
    let (_cm, _gen) = classic_join_sync(&classic_client, "g").await;
    assert!(
        broker.group_type_for_test("g")
            == Some(crabka_broker::coordinator::unified::GroupType::Classic),
        "group_type must be Classic after downgrade, got {:?}",
        broker.group_type_for_test("g")
    );

    // ── Phase 3: committed offset survives the flip. ──
    let fr = classic_client
        .send(OffsetFetchRequest {
            groups: vec![OffsetFetchRequestGroup {
                group_id: "g".into(),
                topics: Some(vec![OffsetFetchRequestTopics {
                    name: "in".into(),
                    topic_id,
                    partition_indexes: vec![0],
                    ..Default::default()
                }]),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("OffsetFetch");
    let part = &fr.groups[0].topics[0].partitions[0];
    assert!(part.error_code == ERR_NONE, "OffsetFetch error: {part:?}");
    assert!(
        part.committed_offset == 42,
        "committed offset must survive classic↔streams downgrade, got {}",
        part.committed_offset
    );
}
```

> **Watch-item (streams OffsetCommit).** If `OffsetCommit` for a live streams member is rejected (member validation may differ from the classic path), commit via whatever path the existing streams offset tests use — grep `crates/broker/tests` for a streams-group `OffsetCommit` (e.g. `streams_groups.rs`) and mirror it. The assertions (downgrades to Classic + offset 42 survives) are the contract; the commit mechanism may need adjusting to the harness.

- [ ] **Step 2: Add the live-members rejection test**

```rust
/// A streams group with a LIVE member rejects a classic JoinGroup with
/// GROUP_ID_NOT_FOUND (69) and stays Streams-typed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streams_group_with_live_member_rejects_classic_join() {
    let (broker, bootstrap, _dir) = boot().await;
    let streams_client = connect(&bootstrap).await;
    let classic_client = connect(&bootstrap).await;

    finalize_streams_version(&streams_client).await;
    create_topic(&streams_client, "in2", 1).await;

    // Live streams member (converge, do NOT leave).
    let (_mid, resp) =
        streams_join_and_converge(&streams_client, "g2", topology("in2"), 1, 15).await;
    assert!(resp.error_code == ERR_NONE);
    assert!(
        broker.group_type_for_test("g2")
            == Some(crabka_broker::coordinator::unified::GroupType::Streams)
    );

    // Round-1 classic JoinGroup (empty member_id) must be rejected BEFORE the
    // MEMBER_ID_REQUIRED dance: the downgrade pre-step runs first.
    let r = tokio::time::timeout(
        Duration::from_secs(5),
        classic_client.send(join_request("g2", "")),
    )
    .await
    .expect("JoinGroup timeout")
    .expect("JoinGroup");
    assert!(
        r.error_code == ERR_GROUP_ID_NOT_FOUND,
        "classic join for streams group with live member must return \
         GROUP_ID_NOT_FOUND (69), got {}",
        r.error_code
    );
    assert!(
        broker.group_type_for_test("g2")
            == Some(crabka_broker::coordinator::unified::GroupType::Streams),
        "group_type must remain Streams after rejected downgrade"
    );
}
```

- [ ] **Step 3: Add the admin type-awareness tests**

Use the broker's test accessors. `ListGroups`/`DescribeGroups`/`DeleteGroups` are driven through the client. For a CONVERTED (classic→streams, slice-1) group:

```rust
/// After a classic→streams conversion (slice 1), the converted group is
/// reported as `streams` by ListGroups and is NOT deletable via the classic
/// path while the streams group has a live member.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn converted_group_admin_views_respect_type_lock() {
    let (broker, bootstrap, _dir) = boot().await;
    let classic_client = connect(&bootstrap).await;
    let streams_client = connect(&bootstrap).await;

    finalize_streams_version(&classic_client).await;
    create_topic(&classic_client, "in3", 1).await;

    // Drain a classic group, then upgrade it to streams via a heartbeat.
    let (cm, _gen) = classic_join_sync(&classic_client, "g3").await;
    // Leave so the classic group is drained.
    let _ = classic_client
        .send(LeaveGroupRequest {
            group_id: "g3".into(),
            member_id: cm.clone(),
            members: vec![MemberIdentity {
                member_id: cm.clone(),
                group_instance_id: None,
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("LeaveGroup");
    let (_sm, hb) =
        streams_join_and_converge(&streams_client, "g3", topology("in3"), 1, 15).await;
    assert!(hb.error_code == ERR_NONE);
    assert!(
        broker.group_type_for_test("g3")
            == Some(crabka_broker::coordinator::unified::GroupType::Streams)
    );

    // ListGroups: the converted group appears exactly once, as `streams`.
    let lg = classic_client
        .send(ListGroupsRequest::default())
        .await
        .expect("ListGroups");
    let rows: Vec<_> = lg.groups.iter().filter(|g| g.group_id == "g3").collect();
    assert!(rows.len() == 1, "g3 listed once, got {}", rows.len());
    assert!(
        rows[0].group_type.eq_ignore_ascii_case("streams"),
        "g3 must be typed streams, got {:?}",
        rows[0].group_type
    );

    // DeleteGroups via the classic path must NOT remove the live streams group's
    // offset home: with a live streams member it is NON_EMPTY_GROUP.
    let dg = classic_client
        .send(DeleteGroupsRequest {
            groups_names: vec!["g3".into()],
            ..Default::default()
        })
        .await
        .expect("DeleteGroups");
    assert!(
        dg.results[0].error_code == ERR_NON_EMPTY_GROUP,
        "delete of a live streams group must be NON_EMPTY_GROUP, got {}",
        dg.results[0].error_code
    );
    assert!(
        broker.group_type_for_test("g3")
            == Some(crabka_broker::coordinator::unified::GroupType::Streams),
        "the streams group must survive the rejected delete"
    );
}
```

> Add the request imports (`ListGroupsRequest`, `DeleteGroupsRequest`) and `const ERR_NON_EMPTY_GROUP: i16 = 24;` to the file. The `ListedGroup.group_type` field is the wire `group_type` string. If `streams_join_and_converge`'s converge leaves the streams member live, the delete is `NON_EMPTY_GROUP` as asserted; if you want to also assert the EMPTY-delete success path, `streams_leave` first, then `DeleteGroups` must return `NONE` and `group_type_for_test("g3")` must become `None`-or-unchanged with `find_streams` gone (assert via a follow-up `StreamsGroupDescribe` → `GROUP_ID_NOT_FOUND`).

- [ ] **Step 4: Add the restart-after-conversion replay test**

Mirror `consumer_group_next_gen_persistence.rs` (drop + `Broker::start(rejoin_config(dir))`):

```rust
fn rejoin_config(log_dir: std::path::PathBuf) -> BrokerConfig {
    let mut cfg = BrokerConfig::for_tests(log_dir);
    cfg.bootstrap_mode = BootstrapMode::Rejoin;
    cfg
}

/// A streams→classic downgrade survives a broker restart: after replay the
/// group is Classic with its committed offset intact.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn downgrade_survives_restart() {
    let dir = tempfile::TempDir::new().unwrap();
    let log_dir = dir.path().to_path_buf();
    let topic_id;
    {
        let broker = Broker::start(BrokerConfig::for_tests(log_dir.clone()))
            .await
            .unwrap();
        let bootstrap = broker.listen_addr().to_string();
        let sc = connect(&bootstrap).await;
        let cc = connect(&bootstrap).await;
        finalize_streams_version(&sc).await;
        create_topic(&sc, "in", 1).await;
        topic_id = topic_id_for(&sc, "in").await;

        let (mid, resp) = streams_join_and_converge(&sc, "g", topology("in"), 1, 15).await;
        // commit offset 42 (see watch-item in Step 1 if rejected)
        let _ = sc
            .send(OffsetCommitRequest {
                group_id: "g".into(),
                generation_id_or_member_epoch: resp.member_epoch,
                member_id: mid.clone(),
                topics: vec![OffsetCommitRequestTopic {
                    name: "in".into(),
                    topic_id,
                    partitions: vec![OffsetCommitRequestPartition {
                        partition_index: 0,
                        committed_offset: 42,
                        committed_metadata: Some(String::new()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .expect("OffsetCommit");
        streams_leave(&sc, "g", &mid).await;
        let _ = classic_join_sync(&cc, "g").await; // downgrade
        assert!(
            broker.group_type_for_test("g")
                == Some(crabka_broker::coordinator::unified::GroupType::Classic)
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
        broker.shutdown().await;
    }
    {
        let broker = Broker::start(rejoin_config(log_dir)).await.unwrap();
        let bootstrap = broker.listen_addr().to_string();
        let cc = connect(&bootstrap).await;
        // Replay must reconstruct g as Classic (k15 tombstoned), offset intact.
        assert!(
            broker.group_type_for_test("g")
                != Some(crabka_broker::coordinator::unified::GroupType::Streams),
            "group must not replay as Streams after downgrade"
        );
        let fr = cc
            .send(OffsetFetchRequest {
                groups: vec![OffsetFetchRequestGroup {
                    group_id: "g".into(),
                    topics: Some(vec![OffsetFetchRequestTopics {
                        name: "in".into(),
                        topic_id,
                        partition_indexes: vec![0],
                        ..Default::default()
                    }]),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .expect("OffsetFetch");
        assert!(
            fr.groups[0].topics[0].partitions[0].committed_offset == 42,
            "committed offset must survive downgrade + restart"
        );
    }
}
```

> **Watch-item (replay type assertion).** The firm contract is "does not replay as Streams" + "offset intact". Whether replay yields `Classic` exactly or `None` (no type lock persisted until the next classic write) depends on how the bootstrap replay seeds `group_types`; assert `!= Streams` (as written) and the offset, which holds either way. If `group_type_for_test` after replay is `Some(Classic)`, tighten the assertion to `== Classic`.

- [ ] **Step 5: Run the integration tests**

Run: `cargo test -p crabka-broker --test streams_classic_downgrade`
Expected: PASS (all tests). Debug against the real harness if a helper name/shape differs — the assertions (downgrades + offset preserved; live-members reject + no flip; admin type-awareness; replay) are the contract.

- [ ] **Step 6: Add the test to CI coverage**

In `.github/workflows/ci.yml`, find the broker crate's llvm-cov `--test streams_classic_upgrade` entry and add `--test streams_classic_downgrade` alongside it (same per-crate-integration convention).

- [ ] **Step 7: Commit**

```bash
git -C <worktree> add crates/broker/tests/streams_classic_downgrade.rs .github/workflows/ci.yml
git -C <worktree> commit -m "test(broker): streams→classic downgrade + admin type-awareness + restart replay (KIP-1071 slice 2)"
```

---

## Task 8: regression + verification gate

**Files:** none.

- [ ] **Step 1: Nightly format (CI gate)**

Run: `cargo +nightly fmt --all -- --check`
Expected: clean. If it fails, run `cargo +nightly fmt --all` and amend the relevant commit. (Stable `cargo fmt` gives a false-clean.)

- [ ] **Step 2: Clippy (CI gate = workspace --all-targets)**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: no warnings. (`touch` the edited broker files first if a stale-clean cache is suspected; check the real `$?`.)

- [ ] **Step 3: Broker tests (incl. regression)**

Run: `cargo test -p crabka-broker`
Expected: PASS — including the slice-1 `streams_classic_upgrade` suite, classic-group, consumer-migration, and streams-group suites (the JoinGroup pre-step is inert for non-Streams ids; the admin lock-checks are inert for non-Streams groups). Note: some broker integration tests are load-sensitive (`fk_join`, `share_consume`, EOS txn) — re-run an isolated flake before treating it as a regression.

- [ ] **Step 4: Workspace build**

Run: `cargo build --workspace`
Expected: success.

- [ ] **Step 5: Commit (only if fmt/clippy required a fixup)**

```bash
git -C <worktree> add -A
git -C <worktree> commit -m "chore(broker): fmt/clippy fixups for KIP-1071 streams downgrade slice 2"
```

---

## Self-review — spec coverage

- Spec §4.2 downgrade routing pre-step → Task 6; tests §6.3 (convert) + §6.4 (reject) → Task 7 Steps 1–2.
- Spec §4.3 conversion (tombstone k15–21, force flip, drop streams actor) → Task 4 (`try_convert_streams_to_classic`) + Task 1 (tombstone batch); type-flip unit → Task 4 Step 1.
- Spec §4.4 persistence (offsets untouched, k15–21 tombstoned) → Task 1 unit + Task 7 Step 1 (offset survives) + Step 4 (replay).
- Spec §4.5 live-members rejection → Task 6 (reject branch) + Task 7 Step 2.
- Spec §4.6 admin type-awareness: ListGroups → Task 2 + test Step 3; DeleteGroups (full streams-aware) → Task 5 + test Step 3; DescribeGroups → Task 3 + test Step 3.
- Spec §1.2 / §7a steady-state → Task 7 Step 4 (restart replay) + Step 3 (post-conversion admin).
- Spec §3 non-goals → no code (no online migration, no policy config, no new offset-tombstoning).
- Spec §7 open items (empirical) → flagged as watch-items in Tasks 4/6 (reject code), Task 3 (DescribeGroups shape), Task 7 (streams OffsetCommit path, replay type assertion).

## Risks / watch-items

1. **Streams `OffsetCommit` member validation** — a live streams member's `OffsetCommit` may validate differently from the classic path; if rejected, mirror the existing streams offset-commit test path (Task 7 Step 1 note). The offset-survives-downgrade contract is unchanged.
2. **`DescribeGroups` wire shape for a streams group** — `protocol_type="streams"` + streams `group_state` is the defensible default; confirm against `apache/kafka:4.2` (spec §7.4). Isolated to Task 3 + its assertion.
3. **Replay type lock after downgrade** — assert `!= Streams` + offset intact (holds regardless of whether replay seeds `Classic` or leaves the lock unset until the next classic write); tighten to `== Classic` if the harness yields it (Task 7 Step 4 note).
4. **Tasks 4 & 5 share `mod.rs`** — sequential (4 before 5); do not dispatch concurrently.
5. **Nightly fmt** — stable fmt passes locally but CI uses `cargo +nightly fmt`; always run nightly in the gate (Task 8).
