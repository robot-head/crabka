# KIP-903 Stale-epoch ISR Fencing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The controller fences replicas with a stale or absent broker epoch from the ISR on `AlterPartition`, returning `INELIGIBLE_REPLICA` (92), with broker epoch = the raft commit offset of the broker's registration record.

**Architecture:** Add `broker_epoch: i64` to `BrokerRegistrationRecord`. The controller leader assigns the epoch = the log offset the record commits at (once, at append, baked into the bytes); all other paths read it back. The partition leader stamps each ISR member's epoch (from its metadata image) onto `AlterPartition`; the controller compares against its own image and rejects mismatches.

**Tech Stack:** Rust 2024, tokio, the `crabka-metadata` / `crabka-raft` / `crabka-broker` workspace crates. Tests are `cargo test` (`#[tokio::test]` for async). Reference spec: `docs/superpowers/specs/2026-06-03-kip903-stale-epoch-isr-fencing-design.md`.

**Execution batching** (per CLAUDE.md — parallel where file sets are disjoint):
- **Batch 1:** Task 1 (foundation; touches many files, run alone).
- **Batch 2:** Tasks 2, 3, 4 (disjoint: `kraft_translate.rs`, `image.rs`, `codes.rs`).
- **Batch 3:** Tasks 5, 6, 7 (disjoint: `raft/kraft/controller.rs`, `isr_maintenance.rs`, `handlers/alter_partition.rs`).
- **Batch 4:** Task 8 (README flip + full-workspace verification; run alone, last).

All `cargo` commands run from the worktree root: `/Users/mattstone/git/crabka/.claude/worktrees/reverent-varahamihira-430254`.

---

## Batch 1

### Task 1: Add `broker_epoch` field + keep the workspace compiling

Adds the field with no behavior change. Every `BrokerRegistrationRecord { .. }` literal must gain the field (the struct has no `Default` derive). Real registration sites get `0` (the leader overwrites it at append in Task 5); test/bench literals get `0`.

**Files:**
- Modify: `crates/metadata/src/records.rs:50-62` (struct def)
- Modify (add `broker_epoch: 0,`): `crates/broker/src/broker.rs:1356`, `crates/broker/src/reassignment.rs:154,258`, `crates/broker/src/unclean_recovery.rs:594`, `crates/broker/src/txn/handlers/end_txn.rs:779,819`, `crates/broker/src/handlers/list_config_resources.rs:178`, `crates/broker/src/handlers/incremental_alter_configs.rs:434`, `crates/broker/src/handlers/alter_partition_reassignments.rs:341`, `crates/raft/src/snapshot.rs:285`, `crates/metadata/benches/image.rs:46`, `crates/metadata/src/image.rs:954,968,1423`, `crates/metadata/tests/evolution.rs:49`, `crates/metadata/src/records.rs:297,309`, `crates/metadata/src/kraft_translate.rs:908,1100,1112`

- [ ] **Step 1: Add the field**

In `crates/metadata/src/records.rs`, insert the field right after `node_id`:

```rust
pub struct BrokerRegistrationRecord {
    pub node_id: NodeId,
    /// KIP-903 broker epoch: the raft log offset at which this registration
    /// record committed. The controller leader assigns it at append time
    /// (`on_submit_change`); a freshly-built literal carries `0` until the
    /// leader stamps it. Used to fence stale replicas from the ISR on
    /// `AlterPartition`.
    pub broker_epoch: i64,
    /// Legacy single-listener host, used as inter-broker default and by
    /// pre-v9 `Metadata` responses. v9+ projects [`Self::endpoints`].
    pub host: String,
    pub port: u16,
    pub rack: Option<String>,
    /// Per-listener endpoints. Empty on records written before this
    /// field was added; populated from
    /// `BrokerConfig::effective_listeners()` for self-registration.
    pub endpoints: Vec<BrokerEndpoint>,
}
```

- [ ] **Step 2: Run the build to find every broken literal**

Run: `cargo build --workspace 2>&1 | grep -E "E0063|missing field" | head -40`
Expected: a list of `missing field \`broker_epoch\`` errors at the file:line sites listed above.

- [ ] **Step 3: Add `broker_epoch: 0,` to every broken literal**

For each site, add the line `broker_epoch: 0,` inside the struct literal (placement within the literal does not matter). The `kraft_translate.rs:908` site is `register_broker_from_kraft`'s output — also set `broker_epoch: 0` here for now; Task 2 replaces it with the real wire value. `evolution.rs:49` is a one-line literal: change to `BrokerRegistrationRecord { node_id, broker_epoch: 0, host, port, rack, endpoints: vec![] }`.

- [ ] **Step 4: Verify the workspace builds and tests still pass**

Run: `cargo build --workspace 2>&1 | tail -5`
Expected: builds clean (no `E0063`).
Run: `cargo test -p crabka-metadata 2>&1 | tail -15`
Expected: all pass (no behavior changed).

- [ ] **Step 5: Commit**

```bash
git add -A
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "KIP-903: add broker_epoch field to BrokerRegistrationRecord (unused)"
```

---

## Batch 2

### Task 2: Round-trip `broker_epoch` through the KRaft wire

**Files:**
- Modify: `crates/metadata/src/kraft_translate.rs` — `register_broker_to_kraft` (~681), `register_broker_from_kraft` (~908), module doc (~45-46), round-trip tests (~1099-1135)

- [ ] **Step 1: Update the existing round-trip tests to carry a non-zero epoch (failing test)**

In the `register_broker_no_endpoints_round_trips` test (~line 1099) set `broker_epoch: 42` on the input `BrokerRegistrationRecord`, and after the round-trip assert it survives. The test decodes back into a `MetadataRecord::V1BrokerRegistration(out)`; add:

```rust
assert!(out.broker_epoch == 42, "broker_epoch lost on round-trip: {}", out.broker_epoch);
```

Do the same (`broker_epoch: 7`, assert `== 7`) in `register_broker_with_endpoints_round_trips` (~line 1111). Set the field on the input literals (they currently have `broker_epoch: 0` from Task 1 — change to 42 / 7).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p crabka-metadata register_broker 2>&1 | tail -20`
Expected: FAIL — `broker_epoch lost on round-trip: 0` (translate still drops it).

- [ ] **Step 3: Wire the field through encode and decode**

In `register_broker_to_kraft` (the `Ok(RegisterBrokerRecord { .. })` at ~681), add:

```rust
        broker_epoch: b.broker_epoch,
```

In `register_broker_from_kraft` (the `Ok(BrokerRegistrationRecord { .. })` at ~908), replace the `broker_epoch: 0,` placeholder from Task 1 with:

```rust
        broker_epoch: b.broker_epoch,
```

Update the module doc comment at lines ~45-46 to drop `broker_epoch` from the "defaulted on encode and dropped on decode" list (leave `incarnation_id`, `features`, `fenced`):

```rust
//!   other KIP-631 extras (`incarnation_id`, `features`, `fenced`, …) are
//!   defaulted on encode and dropped on decode. `broker_epoch` IS carried
//!   (KIP-903 ISR fencing).
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p crabka-metadata register_broker 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/metadata/src/kraft_translate.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "KIP-903: round-trip broker_epoch through the KRaft RegisterBroker wire"
```

### Task 3: Image accessor for `broker_epoch`

**Files:**
- Modify: `crates/metadata/src/image.rs` (near `broker()` at ~264), test module

- [ ] **Step 1: Write the failing test**

Add to the `#[cfg(test)] mod tests` in `crates/metadata/src/image.rs`:

```rust
#[test]
fn broker_epoch_reads_back_registered_epoch() {
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    image.apply(&MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
        node_id: 5,
        broker_epoch: 99,
        host: "h".into(),
        port: 9092,
        rack: None,
        endpoints: vec![],
    }));
    assert!(image.broker_epoch(5) == Some(99));
    assert!(image.broker_epoch(404) == None);
}
```

(If `BrokerRegistrationRecord` / `MetadataRecord` are not already in the test module's scope, add `use super::*;` items as the neighbouring tests do.)

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p crabka-metadata broker_epoch_reads_back 2>&1 | tail -20`
Expected: FAIL — `no method named \`broker_epoch\``.

- [ ] **Step 3: Add the accessor**

Immediately after the existing `broker()` accessor (~264):

```rust
    /// KIP-903: the broker epoch (registration commit offset) for `node_id`,
    /// or `None` if the broker is not registered in this image.
    #[must_use]
    pub fn broker_epoch(&self, node_id: NodeId) -> Option<i64> {
        self.brokers.get(&node_id).map(|b| b.broker_epoch)
    }
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p crabka-metadata broker_epoch 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/metadata/src/image.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "KIP-903: add MetadataImage::broker_epoch accessor"
```

### Task 4: `INELIGIBLE_REPLICA` error code

**Files:**
- Modify: `crates/broker/src/codes.rs` (near `FENCED_LEADER_EPOCH` at ~199)

- [ ] **Step 1: Add the constant**

After `UNKNOWN_LEADER_EPOCH` (~203):

```rust
/// `INELIGIBLE_REPLICA` (92, KIP-903) — an `AlterPartition` proposed a new
/// ISR containing at least one ineligible replica: a broker not currently
/// registered, or one whose stamped broker epoch is stale relative to the
/// controller's registration epoch. The partition's ISR is left unchanged.
pub const INELIGIBLE_REPLICA: i16 = 92;
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo build -p crabka-broker 2>&1 | tail -5`
Expected: builds clean.

- [ ] **Step 3: Commit**

```bash
git add crates/broker/src/codes.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "KIP-903: add INELIGIBLE_REPLICA (92) error code"
```

---

## Batch 3

### Task 5: Controller assigns `broker_epoch` = commit offset

**Files:**
- Modify: `crates/raft/src/kraft/controller.rs` — `on_submit_change` (~1003-1080), test module (~2192+)

- [ ] **Step 1: Write the failing test**

Add to the `mod tests` in `crates/raft/src/kraft/controller.rs` (model on `submit_change_commits_on_single_voter_leader` ~2195). Build a single-voter leader, capture the log end offset, submit a broker registration, and assert the image epoch equals that offset; then re-register and assert it bumps:

```rust
#[tokio::test]
async fn broker_registration_epoch_equals_commit_offset() {
    use crabka_metadata::{BrokerRegistrationRecord, MetadataRecord};
    let (ctrl, _dir) = build(1, &[1]);
    ctrl.inject_event(Event::ElectionTimeout).await.unwrap();
    await_leader(&ctrl, Some(1)).await;

    let reg = |id: u64| {
        vec![MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
            node_id: id,
            broker_epoch: 0, // overwritten by the leader at append
            host: "h".into(),
            port: 9092,
            rack: None,
            endpoints: vec![],
        })]
    };

    let base1 = ctrl.quorum_state().await.unwrap().log_end_offset;
    ctrl.submit_change(reg(7)).await.expect("first registration");
    let e1 = ctrl.current_image().broker_epoch(7);
    assert!(e1 == Some(base1), "epoch {e1:?} != commit offset {base1}");

    let base2 = ctrl.quorum_state().await.unwrap().log_end_offset;
    ctrl.submit_change(reg(7)).await.expect("re-registration");
    let e2 = ctrl.current_image().broker_epoch(7);
    assert!(e2 == Some(base2), "re-reg epoch {e2:?} != offset {base2}");
    assert!(base2 > base1 && e2 > e1, "epoch must strictly increase");

    ctrl.shutdown().await;
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p crabka-raft broker_registration_epoch_equals_commit_offset 2>&1 | tail -25`
Expected: FAIL — `epoch Some(0) != commit offset N` (the leader does not stamp the epoch yet).

- [ ] **Step 3: Stamp the epoch in `on_submit_change`**

In `on_submit_change`, capture the assignment base immediately before the encode/validate loop (just before `let mut scratch = self.image.clone();`):

```rust
        // KIP-903: broker epoch = the offset this batch commits at. The i-th
        // value blob lands at `assign_base + i`; for a V1BrokerRegistration
        // (which fans out to exactly one blob) the offset delta equals the
        // number of blobs already allocated. Single-writer leader: the log end
        // offset now is the base `append` will return.
        let assign_base = self.log.log_end_offset();
```

Then inside the `for r in &records {` loop, replace the body's use of `r` so registration records are epoch-stamped before validate/encode/apply. The current loop starts:

```rust
        for r in &records {
            if let Err(e) = scratch.validate(r) {
```

Change the loop head to bind a possibly-rewritten `r`:

```rust
        for r in &records {
            // Stamp the registration epoch = its committed offset.
            let stamped;
            let r: &crabka_metadata::MetadataRecord = match r {
                crabka_metadata::MetadataRecord::V1BrokerRegistration(b) => {
                    let delta = i64::try_from(value_blobs.len()).unwrap_or(i64::MAX);
                    let mut b = b.clone();
                    b.broker_epoch = assign_base + delta;
                    stamped = crabka_metadata::MetadataRecord::V1BrokerRegistration(b);
                    &stamped
                }
                other => other,
            };
            if let Err(e) = scratch.validate(r) {
```

The rest of the loop body (`to_kraft_values(r, &scratch)`, `scratch.apply(r)`) now operates on the stamped record unchanged.

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p crabka-raft broker_registration_epoch_equals_commit_offset 2>&1 | tail -25`
Expected: PASS.

- [ ] **Step 5: Run the surrounding controller tests for no regressions**

Run: `cargo test -p crabka-raft kraft::controller 2>&1 | tail -15`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add crates/raft/src/kraft/controller.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "KIP-903: assign broker_epoch = registration commit offset at append"
```

### Task 6: Partition leader stamps real epochs on `AlterPartition`

**Files:**
- Modify: `crates/broker/src/isr_maintenance.rs` — `send_alter_partition` (~139-206)

- [ ] **Step 1: Stamp the epochs from the metadata image**

`send_alter_partition` already binds `let image = controller.current_image();` (used for topic-id lookup). Replace the `broker_epoch: -1` sentinels with image lookups.

Replace the top-level request field (currently `broker_epoch: -1,` at ~191) with the partition leader's own epoch:

```rust
        broker_id,
        // KIP-903: the partition leader stamps its own broker epoch and each
        // ISR member's epoch from the metadata image so the controller can
        // fence stale replicas. Unknown brokers fall back to -1 (skip-check).
        broker_epoch: image
            .broker_epoch(u64::try_from(broker_id).unwrap_or(0))
            .unwrap_or(-1),
```

Replace the `new_isr_with_epochs` construction (currently maps every entry to `broker_epoch: -1`) so each member carries its image epoch:

```rust
    let new_isr_with_epochs: Vec<BrokerState> = new_isr_i32
        .iter()
        .map(|&bid| BrokerState {
            broker_id: bid,
            broker_epoch: image
                .broker_epoch(u64::try_from(bid).unwrap_or(0))
                .unwrap_or(-1),
            ..Default::default()
        })
        .collect();
```

Delete the now-stale comment block above `new_isr_with_epochs` that says "Broker epochs are unknown at this call site so we send -1"; replace it with a one-line note that the epochs come from the image.

- [ ] **Step 2: Verify it builds**

Run: `cargo build -p crabka-broker 2>&1 | tail -5`
Expected: builds clean.

- [ ] **Step 3: Run the broker ISR/alter-partition tests**

Run: `cargo test -p crabka-broker isr 2>&1 | tail -15`
Expected: existing tests pass (the image is shared/fast in-process, so epochs match and no spurious fencing occurs).

- [ ] **Step 4: Commit**

```bash
git add crates/broker/src/isr_maintenance.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "KIP-903: partition leader stamps real broker epochs on AlterPartition"
```

### Task 7: Controller fences ineligible replicas

**Files:**
- Modify: `crates/broker/src/handlers/alter_partition.rs` — `handle_partition` (~110-208), add `#[cfg(test)] mod tests`

- [ ] **Step 1: Write the failing tests**

Add a test module at the end of `crates/broker/src/handlers/alter_partition.rs`. It drives `handle_partition` directly with a hand-built image:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crabka_metadata::{
        BrokerRegistrationRecord, MetadataImage, MetadataRecord, PartitionRecord, TopicRecord,
    };
    use crabka_protocol::owned::alter_partition_request::BrokerState;

    fn reg(node_id: u64, epoch: i64) -> MetadataRecord {
        MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
            node_id,
            broker_epoch: epoch,
            host: "h".into(),
            port: 9092,
            rack: None,
            endpoints: vec![],
        })
    }

    /// Image with topic "t" / partition 0, replicas [1,2,3], isr [1,2],
    /// leader 1 @ leader_epoch 5. Brokers registered per `epochs`.
    fn image_with(epochs: &[(u64, i64)]) -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(),
            topic_id: uuid::Uuid::nil(),
            partitions: 1,
            replication_factor: 3,
        }));
        image.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: 1,
            replicas: vec![1, 2, 3],
            isr: vec![1, 2],
            leader_epoch: 5,
            adding_replicas: vec![],
            removing_replicas: vec![],
        }));
        for &(id, ep) in epochs {
            image.apply(&reg(id, ep));
        }
        image
    }

    fn bs(broker_id: i32, broker_epoch: i64) -> BrokerState {
        BrokerState { broker_id, broker_epoch, ..Default::default() }
    }

    #[test]
    fn matching_epochs_succeed() {
        let image = image_with(&[(1, 10), (2, 20), (3, 30)]);
        let mut changes = Vec::new();
        // Expand ISR to [1,2,3] with the correct epochs.
        let isr = vec![bs(1, 10), bs(2, 20), bs(3, 30)];
        let resp = handle_partition(&image, Some("t"), 0, 5, &[], &isr, &mut changes);
        assert!(resp.error_code == codes::NONE, "got {}", resp.error_code);
        assert!(changes.len() == 1);
    }

    #[test]
    fn stale_epoch_is_ineligible() {
        let image = image_with(&[(1, 10), (2, 20), (3, 30)]);
        let mut changes = Vec::new();
        // Broker 3 stamped with a stale epoch (29 != image 30).
        let isr = vec![bs(1, 10), bs(2, 20), bs(3, 29)];
        let resp = handle_partition(&image, Some("t"), 0, 5, &[], &isr, &mut changes);
        assert!(resp.error_code == codes::INELIGIBLE_REPLICA, "got {}", resp.error_code);
        assert!(changes.is_empty());
    }

    #[test]
    fn unregistered_replica_is_ineligible() {
        // Broker 3 in `replicas` but never registered.
        let image = image_with(&[(1, 10), (2, 20)]);
        let mut changes = Vec::new();
        let isr = vec![bs(1, 10), bs(2, 20), bs(3, -1)];
        let resp = handle_partition(&image, Some("t"), 0, 5, &[], &isr, &mut changes);
        assert!(resp.error_code == codes::INELIGIBLE_REPLICA, "got {}", resp.error_code);
        assert!(changes.is_empty());
    }

    #[test]
    fn sentinel_epoch_skips_epoch_check() {
        // -1 epoch on a registered broker means "don't check epoch": eligible.
        let image = image_with(&[(1, 10), (2, 20), (3, 30)]);
        let mut changes = Vec::new();
        let isr = vec![bs(1, -1), bs(2, -1), bs(3, -1)];
        let resp = handle_partition(&image, Some("t"), 0, 5, &[], &isr, &mut changes);
        assert!(resp.error_code == codes::NONE, "got {}", resp.error_code);
        assert!(changes.len() == 1);
    }

    #[test]
    fn v2_no_epochs_path_unaffected() {
        // v2: new_isr populated, new_isr_with_epochs empty → no epoch fencing.
        let image = image_with(&[(1, 10), (2, 20)]);
        let mut changes = Vec::new();
        let resp = handle_partition(&image, Some("t"), 0, 5, &[1, 2, 3], &[], &mut changes);
        assert!(resp.error_code == codes::NONE, "got {}", resp.error_code);
        assert!(changes.len() == 1);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p crabka-broker alter_partition::tests 2>&1 | tail -30`
Expected: FAIL — `stale_epoch_is_ineligible` / `unregistered_replica_is_ineligible` return `NONE` (no fencing yet); the success cases may pass.

- [ ] **Step 3: Add the eligibility fence in `handle_partition`**

In `handle_partition`, after the subset-validation block (the `if !valid { return error_part(... INVALID_REQUEST ...) }` ending ~184) and before the `// Success: submit the ISR change.` comment, insert the KIP-903 fence. It walks `new_isr_with_epochs` (present only on v3); v2 requests pass through untouched:

```rust
    // KIP-903: fence ineligible replicas. A broker in the proposed ISR is
    // ineligible if it is not currently registered, or if its stamped broker
    // epoch is non-sentinel (-1) and disagrees with the controller's
    // registration epoch. Any ineligible replica fails the whole partition.
    for bstate in new_isr_with_epochs {
        let node = u64::try_from(bstate.broker_id).unwrap_or(u64::MAX);
        let registered = image.broker_epoch(node);
        let ineligible = registered.is_none()
            || (bstate.broker_epoch != -1 && registered != Some(bstate.broker_epoch));
        if ineligible {
            return error_part(
                partition_index,
                codes::INELIGIBLE_REPLICA,
                leader_i32,
                part_rec.leader_epoch,
                &current_isr_i32,
            );
        }
    }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p crabka-broker alter_partition::tests 2>&1 | tail -30`
Expected: all 5 PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/handlers/alter_partition.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "KIP-903: fence stale/unregistered replicas from the ISR (INELIGIBLE_REPLICA)"
```

---

## Batch 4

### Task 8: Flip the README KIP matrix + full-workspace verification

**Files:**
- Modify: `README.md:392` (KIP-903 row)

- [ ] **Step 1: Flip the matrix entry**

In `README.md`, change the KIP-903 row from `⚠️` to `✅`:

```markdown
| [KIP-903](https://cwiki.apache.org/confluence/display/KAFKA/KIP-903) | Fence replicas with stale broker epoch from the ISR | ✅ |
```

- [ ] **Step 2: Format check**

Run: `cargo fmt --all && cargo fmt --all --check`
Expected: no diff (clean). (Per repo convention, fmt must pass before push.)

- [ ] **Step 3: Clippy (all targets, deny warnings)**

Run: `cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -20`
Expected: no warnings/errors.

- [ ] **Step 4: Full test run for the touched crates**

Run: `cargo test -p crabka-metadata -p crabka-raft -p crabka-broker 2>&1 | tail -25`
Expected: all pass. (If a known-flaky test surfaces — see project memory on share-consume / ISR-expand churn — re-run it isolated before treating it as a regression.)

- [ ] **Step 5: Commit**

```bash
git add README.md
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "KIP-903: mark stale-epoch ISR fencing complete in the KIP matrix"
```

---

## Notes for the implementer

- **Greenfield, no back-compat** (CLAUDE.md): just change schemas/records; do not add `#[serde(default)]` or migration shims for the new field.
- **Kafka faithfulness:** `INELIGIBLE_REPLICA` is error code 92; the whole partition fails when any proposed ISR member is ineligible (Kafka does not partially apply). v2 `AlterPartition` (no per-replica epochs) must keep working unchanged.
- **Why epoch == offset is safe everywhere:** the offset is assigned once at the live append (Task 5) and baked into the committed record bytes; restart-replay and snapshot-install read it back via Task 2's wire round-trip — no apply-time position logic.
- **git:** identity is unset locally — always commit with the `-c user.name=... -c user.email=...` overrides shown above; never run `git config`.
