# KIP-1071 Classic → Streams Cold Upgrade (slice 1) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When a `StreamsGroupHeartbeat` arrives for a `group_id` currently held as a **drained classic group**, auto-convert it to a KIP-1071 streams group — preserving committed offsets and tombstoning the classic `GroupMetadata` (k2).

**Architecture:** A new pre-step in the `StreamsGroupHeartbeat` handler consults the coordinator's type lock. For a `Classic`-typed `group_id` with no live members, a new coordinator method `try_convert_classic_to_streams` appends a k2 tombstone (reusing the existing `PendingRecords::into_batch`), force-flips the type lock `Classic → Streams`, and removes the classic actor; then the heartbeat is served against the streams registry as usual. A `Classic` group with live members is rejected (online streams migration is unsupported in Kafka). Offsets (k0/k1) are protocol-agnostic and survive untouched.

**Tech Stack:** Rust 2024, `crabka-broker`, tokio actors + mpsc, `OffsetsLog::append(RecordBatch)`, in-process broker test harness (`crates/broker/tests/support`).

**Spec:** `docs/superpowers/specs/2026-06-12-kip-1071-streams-classic-upgrade-design.md`

**Nightly rustfmt:** CI gates on `cargo +nightly fmt --all -- --check` (stable fmt gives false-clean — it ignores unstable `rustfmt.toml` options). Use nightly fmt in the gate.

---

## File / responsibility map

| File | Responsibility | Task |
|------|----------------|------|
| `crates/broker/src/coordinator/unified/mod.rs` | `mark_streams_after_upgrade` (forced `Classic→Streams` + seed cleanup) | Task 2 |
| `crates/broker/src/coordinator/unified/streams/migration.rs` (new) | `classic_streams_tombstone_batch` helper + `try_convert_classic_to_streams` coordinator method | Task 3 |
| `crates/broker/src/coordinator/unified/streams/mod.rs` | `mod migration;` declaration | Task 3 |
| `crates/broker/src/handlers/streams_group_heartbeat.rs` | routing pre-step: convert / reject / passthrough | Task 4 |
| `crates/broker/tests/streams_classic_upgrade.rs` (new) | in-process integration tests | Task 4 |
| (verification only) | regression + nightly-fmt + clippy + build | Task 5 |

## Execution notes

Tasks are **sequential** (each builds on the prior) — dispatch one subagent at a time. Task 1 is a research probe; if its Docker step is infeasible on the host, adopt the provisional rejection code and proceed (downstream code references a single constant).

---

## Task 1: Empirically confirm the live-members rejection error code

**Files:** none (research → a findings note appended to the spec's §7).

The spec defers the exact wire error for the "live classic members present" rejection (online streams migration unsupported). Confirm it against the real broker before hard-coding.

- [ ] **Step 1: Attempt an empirical capture**

Run a single-broker `apache/kafka:4.2.0` (Docker), create a classic consumer group with a live member at `group_id=g`, then send a `StreamsGroupHeartbeat` for `group_id=g` (e.g. via `kafka-streams-groups.sh` describe/alter, or a minimal client) and capture the response error code.

```bash
docker run --rm -d --name k42 -p 9092:9092 apache/kafka:4.2.0
# ... create classic group 'g' with a live consumer, then drive a streams heartbeat at 'g'
# Capture the StreamsGroupHeartbeat error_code.
docker rm -f k42
```

Expected: a specific error code (candidates: `GROUP_ID_NOT_FOUND` (69), `INCONSISTENT_GROUP_PROTOCOL` (23), or a coordinator error).

- [ ] **Step 2: Record the decision**

If the capture succeeds, append a one-line note to `docs/superpowers/specs/2026-06-12-kip-1071-streams-classic-upgrade-design.md` §7.1 with the confirmed code and use it in Task 4. **If the capture is infeasible on this host, adopt the provisional code `GROUP_ID_NOT_FOUND` (code 69)** and append a note: "provisional — unverified against 4.2; revisit." Either way, the chosen value is referenced once, as `REJECT_CODE`, in Task 4.

- [ ] **Step 3: Commit the spec note**

```bash
git -C <worktree> add docs/superpowers/specs/2026-06-12-kip-1071-streams-classic-upgrade-design.md
git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "docs(broker): record streams-migration live-members rejection code (slice 1 §7.1)"
```

---

## Task 2: `mark_streams_after_upgrade` — forced type flip + seed cleanup

**Files:**
- Modify: `crates/broker/src/coordinator/unified/mod.rs` (add method near `mark_classic_after_downgrade` ~line 207 and `mark_streams` ~line 225; add a `#[cfg(test)]` test in the existing test module, or a new `#[test]`)

The existing `mark_streams` uses `or_insert` (first-write-wins) so it will NOT override a prior `Classic` lock. Conversion needs a FORCED override, mirroring `mark_classic_after_downgrade`.

- [ ] **Step 1: Write the failing test**

Add to the test module in `mod.rs` (find the existing `#[cfg(test)] mod tests` near the bottom; if none, add one):

```rust
#[test]
fn mark_streams_after_upgrade_forces_streams_over_classic() {
    let c = GroupCoordinator::for_tests();
    c.mark_classic("g");
    assert_eq!(c.group_type("g"), Some(GroupType::Classic));
    // or_insert mark_streams must NOT override an existing Classic lock:
    c.mark_streams("g");
    assert_eq!(c.group_type("g"), Some(GroupType::Classic));
    // The forced upgrade variant MUST override it:
    c.mark_streams_after_upgrade("g");
    assert_eq!(c.group_type("g"), Some(GroupType::Streams));
}
```

> If `GroupCoordinator::for_tests()` does not exist, use the same constructor the surrounding tests in `mod.rs` already use to build a coordinator (grep the test module for how it instantiates one). Keep the three assertions.

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p crabka-broker --lib coordinator::unified::tests::mark_streams_after_upgrade`
Expected: FAIL — `mark_streams_after_upgrade` does not exist (compile error E0599).

- [ ] **Step 3: Add the method**

In `mod.rs`, immediately after `mark_classic_after_downgrade` (the forced-`Classic` template):

```rust
/// After an in-place classic→streams upgrade (KIP-1071), drop the classic
/// seed so a respawn does not re-hydrate the group as classic, and record it
/// as streams. Unlike [`Self::mark_streams`] (first-mark-wins via `or_insert`),
/// this FORCES the type to `Streams`, overriding any prior `Classic` lock the
/// group carried while it was a classic group.
pub fn mark_streams_after_upgrade(&self, group_id: &str) {
    self.seeds.remove(group_id);
    self.seeds_cache.remove(group_id);
    self.group_types.insert(group_id.into(), GroupType::Streams);
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p crabka-broker --lib coordinator::unified::tests::mark_streams_after_upgrade`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git -C <worktree> add crates/broker/src/coordinator/unified/mod.rs
git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(broker): mark_streams_after_upgrade — forced Classic→Streams type flip (KIP-1071)"
```

---

## Task 3: conversion module — tombstone batch + `try_convert_classic_to_streams`

**Files:**
- Create: `crates/broker/src/coordinator/unified/streams/migration.rs`
- Modify: `crates/broker/src/coordinator/unified/streams/mod.rs` (add `mod migration;`)

### 3a — the tombstone batch helper (pure, unit-tested)

- [ ] **Step 1: Create the module with the helper + a failing unit test**

Create `crates/broker/src/coordinator/unified/streams/migration.rs`:

```rust
//! KIP-1071 classic→streams cold conversion. When a drained classic group
//! receives a `StreamsGroupHeartbeat`, the group is converted in place: the
//! classic `GroupMetadata` (k2) is tombstoned, the type lock is forced to
//! `Streams`, and the classic actor is dropped. Committed offsets (k0/k1) are
//! protocol-agnostic and are left untouched. Streams migration is COLD only
//! (Kafka does not support online streams migration), so there is no
//! hosted-classic-member translation here.

use crabka_protocol::records::RecordBatch;

use crate::coordinator::unified::actor::PendingRecords;

/// Build the single-record batch that tombstones the classic k2 `GroupMetadata`
/// for `group_id`. Reuses the consumer-migration `PendingRecords` encoder so the
/// tombstone key bytes are identical to the upgrade flip's.
pub(crate) fn classic_group_metadata_tombstone_batch(group_id: &str, now_ms: i64) -> RecordBatch {
    PendingRecords {
        classic_group_metadata_tombstone: true,
        ..Default::default()
    }
    .into_batch(group_id, now_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tombstone_batch_has_one_null_value_k2_record() {
        let batch = classic_group_metadata_tombstone_batch("g", 123);
        assert_eq!(batch.records.len(), 1, "exactly one record");
        let r = &batch.records[0];
        assert!(r.key.is_some(), "k2 GroupMetadata key present");
        assert!(r.value.is_none(), "tombstone = null value");
        // k2 key is version 2 + group_id; first two bytes are the i16 version 2.
        let key = r.key.as_ref().unwrap();
        assert_eq!(&key[..2], &2i16.to_be_bytes(), "classic GroupMetadata key version 2");
    }
}
```

Add to `crates/broker/src/coordinator/unified/streams/mod.rs` (with the other `mod` declarations):

```rust
mod migration;
```

> `PendingRecords` is `pub(crate)` in `unified/actor.rs`; the path `crate::coordinator::unified::actor::PendingRecords` resolves crate-internally. If `into_batch`/the `classic_group_metadata_tombstone` field are not visible, widen their visibility to `pub(crate)` (they are already `pub(crate)` per the actor module).

- [ ] **Step 2: Run the unit test to verify it fails, then passes**

Run: `cargo test -p crabka-broker --lib coordinator::unified::streams::migration::tests`
Expected: FAILS to compile until `mod migration;` is added and the module compiles, then PASSES. (If `RecordBatch.records`/`Record.key`/`.value` field names differ, correct them by reading `crabka_protocol::records::{Record, RecordBatch}` — they are used verbatim in `actor.rs:1376-1476`.)

- [ ] **Step 3: Commit**

```bash
git -C <worktree> add crates/broker/src/coordinator/unified/streams/migration.rs crates/broker/src/coordinator/unified/streams/mod.rs
git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(broker): classic GroupMetadata tombstone batch for streams upgrade (KIP-1071)"
```

### 3b — the conversion coordinator method

- [ ] **Step 4: Add `try_convert_classic_to_streams` to the coordinator**

This is an async method on `GroupCoordinator` (add it in `mod.rs`, or as a free `pub(crate) async fn` in `migration.rs` taking `&Arc<GroupCoordinator>` — prefer the method form on the coordinator for symmetry with the consumer migration). It returns an outcome enum so the handler can branch.

Add the outcome enum to `migration.rs`:

```rust
/// Result of inspecting a `group_id` for classic→streams conversion.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ConvertOutcome {
    /// Not a classic group (fresh streams group or already streams) — serve normally.
    NotClassic,
    /// Was a drained classic group; converted in place to streams.
    Converted,
    /// Classic group has live members — online streams migration is unsupported.
    RejectLiveMembers,
}
```

Add the method (on `GroupCoordinator`, in `mod.rs`, near `get_or_create_streams`):

```rust
/// KIP-1071 cold upgrade: if `group_id` is a drained classic group, convert it
/// to a streams group in place (tombstone the classic k2 GroupMetadata, force
/// the type lock to Streams, drop the classic actor). Committed offsets survive.
/// Returns `NotClassic` for non-classic groups (caller serves normally),
/// `Converted` after a successful flip, or `RejectLiveMembers` when live classic
/// members remain (online streams migration is unsupported in Kafka).
pub(crate) async fn try_convert_classic_to_streams(
    self: &Arc<Self>,
    group_id: &str,
    now_ms: i64,
) -> Result<ConvertOutcome, crate::error::BrokerError> {
    use crate::coordinator::unified::streams::migration::{
        classic_group_metadata_tombstone_batch, ConvertOutcome,
    };
    if self.group_type(group_id) != Some(GroupType::Classic) {
        return Ok(ConvertOutcome::NotClassic);
    }
    // Inspect the live classic actor (if any) for remaining members.
    if let Some(handle) = self.find(group_id) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if handle
            .tx
            .send(GroupActorMessage::ClassicInspect { reply: tx })
            .await
            .is_ok()
        {
            if let Ok(view) = rx.await {
                if !view.members.is_empty() {
                    return Ok(ConvertOutcome::RejectLiveMembers);
                }
            }
        }
    }
    // Drained classic group → convert. Tombstone k2, flip the lock, drop the actor.
    let batch = classic_group_metadata_tombstone_batch(group_id, now_ms);
    self.offsets_log.append(batch).await?;
    self.mark_streams_after_upgrade(group_id);
    self.groups.remove(group_id);
    Ok(ConvertOutcome::Converted)
}
```

> Adjust two things by reading the cited code: (1) `ClassicInspect`'s reply type `ClassicView` and its `members` field name — confirm the field that holds the live classic members (`actor.rs:93` + the `ClassicView` struct def) and use its real name (`view.members` assumed). (2) `now_ms` source — pass the broker's current wall-clock ms from the handler (see how `streams_group_heartbeat.rs` / `flush_pending` obtain `now_ms`).

- [ ] **Step 5: Build to verify it compiles**

Run: `cargo build -p crabka-broker`
Expected: success. (Behavioral coverage comes from Task 4's integration tests, which exercise this method end-to-end through the handler.)

- [ ] **Step 6: Commit**

```bash
git -C <worktree> add crates/broker/src/coordinator/unified/mod.rs crates/broker/src/coordinator/unified/streams/migration.rs
git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(broker): try_convert_classic_to_streams coordinator method (KIP-1071 cold upgrade)"
```

---

## Task 4: wire conversion into the handler + integration tests

**Files:**
- Modify: `crates/broker/src/handlers/streams_group_heartbeat.rs` (~lines 57-75)
- Create: `crates/broker/tests/streams_classic_upgrade.rs`
- Modify: `.github/workflows/ci.yml` (add `streams_classic_upgrade` to the broker crate's llvm-cov `--test` list — per the per-crate-integration coverage convention)

### 4a — handler pre-step

- [ ] **Step 1: Insert the conversion pre-step**

In `streams_group_heartbeat.rs`, BEFORE the existing `ng.mark_streams(&req.group_id); let handle = ng.get_or_create_streams(&req.group_id);` (lines 63-64), add:

```rust
// KIP-1071 cold upgrade: a StreamsGroupHeartbeat for a drained classic group
// converts it in place; a classic group with live members is rejected (online
// streams migration is unsupported). Non-classic group_ids pass through.
let now_ms = broker.now_ms(); // use the broker's wall-clock-ms accessor (see other handlers)
match ng.try_convert_classic_to_streams(&req.group_id, now_ms).await {
    Ok(crate::coordinator::unified::streams::migration::ConvertOutcome::RejectLiveMembers) => {
        return encode(version, &error(codes::REJECT_CODE)); // REJECT_CODE per Task 1
    }
    Ok(_) => {} // NotClassic | Converted → serve normally below
    Err(e) => return Err(e),
}
```

Replace `codes::REJECT_CODE` with the constant chosen in Task 1 (e.g. `codes::GROUP_ID_NOT_FOUND`). Confirm `broker.now_ms()` exists; if the handler already computes a timestamp, reuse it; otherwise use the same wall-clock source other handlers use (grep `now_ms` in `handlers/`).

- [ ] **Step 2: Build**

Run: `cargo build -p crabka-broker`
Expected: success.

### 4b — integration tests

- [ ] **Step 3: Write the failing integration tests**

Create `crates/broker/tests/streams_classic_upgrade.rs`. Use the existing helpers — boot/connect/create_topic/finalize_streams_version from the streams support module, OffsetCommit/OffsetFetch from the offsets support, and `first_join`/`join_and_converge` for the streams heartbeat. Read `crates/broker/tests/streams_groups.rs` and `crates/broker/tests/kip516_offsets.rs` for the exact helper signatures and imports, and mirror their harness.

```rust
//! KIP-1071 slice 1: classic→streams cold upgrade. A drained classic group with
//! committed offsets, on receiving a StreamsGroupHeartbeat, converts to a streams
//! group with offsets preserved and the classic GroupMetadata tombstoned.

// (imports mirror streams_groups.rs + kip516_offsets.rs)

/// A drained classic group (committed offsets, no live members) converts on the
/// first StreamsGroupHeartbeat: the group becomes a streams group, the committed
/// offset is still readable, and a streams assignment is produced.
#[tokio::test]
async fn drained_classic_group_upgrades_to_streams_preserving_offsets() {
    let (broker, bootstrap, _tmp) = boot().await;
    let client = connect(&bootstrap).await;
    finalize_streams_version(&client).await;
    create_topic(&client, "in", 1).await;

    // 1. Make 'g' a drained classic group with a committed offset: commit an
    //    offset for group 'g' (classic OffsetCommit path) without joining, then
    //    confirm the coordinator typed it Classic.
    commit_offset(&client, "g", "in", 0, 42).await; // helper from kip516_offsets.rs pattern
    assert_eq!(broker.coordinator().group_type("g"), Some(GroupType::Classic));

    // 2. Drive a StreamsGroupHeartbeat for 'g' → converts to streams.
    let (_member_id, resp) = join_and_converge(&client, "g", in_topology(), 1, 10).await;
    assert_eq!(resp.error_code, 0, "heartbeat served by the streams group");
    assert_eq!(broker.coordinator().group_type("g"), Some(GroupType::Streams));

    // 3. The committed offset survives the flip.
    let fetched = fetch_offset(&client, "g", "in", 0).await;
    assert_eq!(fetched, 42, "committed offset preserved across classic→streams");
}
```

> The exact group-creation path for a "classic group with offsets, no members" must produce `group_type == Classic`. If a bare `OffsetCommit` does not set the Classic type lock, instead have a classic consumer JoinGroup+SyncGroup then LeaveGroup (drain) so the group is `Empty` + Classic-typed with offsets — use the classic-consumer helpers in the broker tests (grep `JoinGroup` in `crates/broker/tests/`). Adjust the test to whichever path yields a drained, Classic-typed group; keep assertions 2 and 3 unchanged. `broker.coordinator()` / `in_topology()` / `commit_offset` / `fetch_offset` are thin wrappers you write over the existing helpers — name them to match what `streams_groups.rs` and `kip516_offsets.rs` already expose.

```rust
/// A classic group with a LIVE member rejects a StreamsGroupHeartbeat (online
/// streams migration is unsupported).
#[tokio::test]
async fn classic_group_with_live_member_rejects_streams_heartbeat() {
    let (broker, bootstrap, _tmp) = boot().await;
    let client = connect(&bootstrap).await;
    finalize_streams_version(&client).await;
    create_topic(&client, "in", 1).await;

    // A live classic member keeps the group non-drained.
    let _classic = join_classic_member(&client, "g", "in").await; // classic JoinGroup+SyncGroup, no leave
    assert_eq!(broker.coordinator().group_type("g"), Some(GroupType::Classic));

    // The streams heartbeat is rejected (REJECT_CODE from Task 1).
    let resp = send_first_streams_heartbeat(&client, "g", in_topology()).await;
    assert_eq!(resp.error_code, REJECT_CODE, "live classic members → reject");
    assert_eq!(broker.coordinator().group_type("g"), Some(GroupType::Classic), "no flip");
}
```

Replace `REJECT_CODE` with the Task 1 constant.

- [ ] **Step 4: Run the integration tests**

Run: `cargo test -p crabka-broker --test streams_classic_upgrade`
Expected: PASS (both tests). Debug against the real harness signatures if a helper name/shape differs — the assertions (converts + offset preserved + tombstone-via-type-flip; live-members reject + no flip) are the contract.

- [ ] **Step 5: Add the test to CI coverage**

In `.github/workflows/ci.yml`, add `streams_classic_upgrade` to the broker crate's llvm-cov `--test` list (find the existing broker `--test <name>` entries and append, matching the per-crate-integration convention).

- [ ] **Step 6: Commit**

```bash
git -C <worktree> add crates/broker/src/handlers/streams_group_heartbeat.rs crates/broker/tests/streams_classic_upgrade.rs .github/workflows/ci.yml
git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(broker): convert drained classic group to streams on StreamsGroupHeartbeat (KIP-1071 slice 1)"
```

---

## Task 5: regression + verification gate

**Files:** none.

- [ ] **Step 1: Nightly format (CI gate)**

Run: `cargo +nightly fmt --all -- --check`
Expected: clean. If it fails, run `cargo +nightly fmt --all` and amend the relevant commit. (Stable `cargo fmt` gives a false-clean — it ignores the unstable `rustfmt.toml` options nightly enforces.)

- [ ] **Step 2: Clippy (CI gate = workspace --all-targets)**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: no warnings. (`touch` the edited broker files first if a stale-clean cache is suspected; check the real `$?`.)

- [ ] **Step 3: Broker tests (incl. regression: existing classic/consumer/streams suites unchanged)**

Run: `cargo test -p crabka-broker`
Expected: PASS — including `jvm_consumer_group_next_gen` unit paths, classic-group, and streams-group suites (the handler pre-step is inert for non-classic group_ids). Note: some broker integration tests are load-sensitive (`fk_join`, `share_consume`, EOS txn) — re-run any isolated flake before treating it as a regression.

- [ ] **Step 4: Workspace build**

Run: `cargo build --workspace`
Expected: success.

- [ ] **Step 5: Commit (only if fmt/clippy required a fixup)**

```bash
git -C <worktree> add -A
git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "chore(broker): fmt/clippy fixups for KIP-1071 streams cold upgrade"
```

---

## Self-review — spec coverage

- Spec §4.1 routing pre-step → Task 4a; tests §6.3 (convert) + §6.4 (reject) → Task 4b.
- Spec §4.2 conversion (tombstone k2, force type flip, drop classic actor) → Task 3 (`try_convert_classic_to_streams` + tombstone batch); type-flip unit → Task 2.
- Spec §4.3 persistence (offsets untouched, k2 tombstoned) → Task 3a unit (tombstone shape) + Task 4b §6.3 (offset survives).
- Spec §4.4 live-members rejection → Task 1 (error code) + Task 4 (reject branch + test).
- Spec §6 testing → Tasks 2/3a (unit), 4b (integration §6.3/§6.4), 5 (regression §6.5).
- Spec §7 empirical items → Task 1 (rejection code; provisional fallback documented). §7.2 (no policy config) and §7.3 (trigger boundary) are assumptions baked into the design; flagged in the spec, not re-litigated here.

## Risks / watch-items

1. **`ClassicView.members` field name** — confirm against the `ClassicView` struct (the `ClassicInspect` reply); use the real field that lists live classic members.
2. **Classic-typed-group-with-offsets setup in the test** — a bare `OffsetCommit` may or may not set the `Classic` type lock; if not, drain a real classic member (JoinGroup+SyncGroup+LeaveGroup). The test must reach `group_type == Classic` with no live members before the streams heartbeat.
3. **`now_ms` source** — reuse the handler/broker wall-clock accessor; don't introduce a new clock.
4. **Rejection code** — provisional until Task 1's empirical probe; single constant, easy to correct.
5. **Nightly fmt** — stable fmt passes locally but CI uses `cargo +nightly fmt`; always run nightly in the gate.
