# Share + Group-Coordination Test De-flake — Implementation Plan (Phase 2)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add two test-only `BrokerHandle` awaiter families (share-state, group-state) and convert the 9 known-flaky broker share/group integration tests off fixed `sleep`-poll loops onto event-driven condition-waits.

**Architecture:** Mirror Phase 1: `#[cfg(any(test, feature = "test-helpers"))]` awaiters on `BrokerHandle` that poll **real subsystem state** at a ~25 ms cadence under a 30 s `tokio::time::timeout` safety-net (neither share-partition nor group-coordinator state has a Notify/watch, so poll-with-bounded-recheck is the pattern — exactly Phase 1's `wait_until_local_log_end_offset_eq`). Two independent batches: share-state (hooks + 4 `share_*` tests), then group-state (hooks + 5 group tests).

**Tech Stack:** Rust (edition 2024), `tokio` (`time`, `mpsc`, `oneshot`, `Mutex`), `dashmap`, `assert2`. Tests run with the `test-helpers` feature auto-enabled via the `crabka-broker-test-helpers` dev-dependency.

**Spec:** `docs/superpowers/specs/2026-06-13-crabka-share-group-deflake-design.md`

**Branch:** `claude/share-group-deflake`, stacked on the Phase 1 branch `claude/vigorous-johnson-51eec1` (reuses its `wait_until_partition_present` etc.).

---

## Grounding facts (verified)

- `BrokerHandle` holds `_broker: Arc<Broker>`. `Broker` has `pub(crate) group_coordinator: Arc<GroupCoordinator>`, `pub(crate) share_coordinator: Arc<ShareCoordinator>`, `pub(crate) share_partition_leaders: Arc<SharePartitionLeaderManager>` (`crates/broker/src/broker.rs:48-54`).
- Existing: `BrokerHandle::share_state_summary_for_test(group, topic_id, partition) -> Option<(i32 state_epoch, i32 leader_epoch, i64 start_offset, i32 dcc)>` (async, `broker.rs:433`) — reuse for SPSO/dcc/summary-present.
- `SharePartitionLeaderManager` (`manager.rs:28-41`): `leaders: DashMap<(String, Uuid, i32), Arc<Mutex<AcquisitionState>>>`; `pub(crate) async fn get_or_load(...)` returns the cell (loads from persister on miss).
- `AcquisitionState` (`state.rs:85`): `pub start_offset: i64`, `delivery_complete_count: i32` (private; a `#[cfg(test)]`-only accessor exists — NOT visible to integration tests), `batches: Vec<InFlightBatch>` (private; `InFlightBatch.state: RecordState`). `RecordState` = `Available|Acquired|Acknowledged|Archived`.
- `GroupCoordinator.groups: Arc<DashMap<String, Arc<GroupActorHandle>>>` (`mod.rs`). `GroupActorHandle.tx: mpsc::Sender<GroupActorMessage>` (`actor.rs:276`). `GroupActorMessage::Describe { reply: oneshot::Sender<DescribeView> }` (`actor.rs:69`). `DescribeView { group_id, group_epoch: i32, assignment_epoch: i32, members: Vec<DescribeMember> }`. `DescribeMember { member_id, instance_id, member_epoch: i32, client_id, client_host, subscribed_topic_names, assigned_partitions: HashMap<Uuid, Vec<i32>>, is_classic }` (`actor.rs:253-274`). `build_describe` populates it from `GroupState` (`actor.rs:1309`). The source `MemberState` has `assignment_state: MemberAssignmentState` (`Stable=0|UnreleasedPartitions=1|UnrevokedPartitions=2`, `persistence_next_gen.rs:320`) but `build_describe` does NOT project it — Task 2 adds it.

## File structure

- Modify `crates/broker/src/share_partition/state.rs` — add `count_acquired_batches` (test-helper-gated).
- Modify `crates/broker/src/share_partition/manager.rs` — add `peek_for_test` (non-loading cell lookup).
- Modify `crates/broker/src/coordinator/unified/actor.rs` — add `assignment_state` to `DescribeMember` + populate in `build_describe`.
- Modify `crates/broker/src/broker.rs` — share-state + group-state awaiters + `group_describe_for_test`.
- Modify the 9 test files under `crates/broker/tests/`.

## Site-type conversion rules (applied across all 9 files)

The survey classified ~64 sleep/poll sites. Convert by type:
- **share-state propagation** (SPSO advanced / summary present / dcc / acquired) → the Task-1 share awaiters.
- **group-state** (stable / epoch / member count / drained / active-tasks) → the Task-2 group awaiters.
- **partition materialization / produce-retry on UNKNOWN_TOPIC_OR_PARTITION|NOT_LEADER_OR_FOLLOWER** → reuse Phase 1 `wait_until_partition_present` then the produce; a bounded retry-on-retriable-error RPC loop may stay if it has no fixed-duration assumption.
- **pre-shutdown flush** → where the persisted condition is awaitable (the value was just written), replace the `sleep` with the matching share/group awaiter on that value *before* `broker.shutdown()`; keep only genuinely-opaque flushes.
- **lock-timeout redelivery/archive** → await the outcome (acquired-count returns / dcc increments / archived) via the share awaiters; the sweeper fires on its own configured `record_lock_duration`.
- **precise renew-timing** (`renew_extends_lock`, `no_renew_redelivers`) → KEEP as calibrated sleeps (derived from the configured `record_lock_duration`; proving a lock is *not* released before its deadline inherently requires waiting through it).
- **`consumer.poll()` API loops** → KEEP.

Preserve every assertion; only the waiting changes.

## Execution batches

- **Batch A:** Task 1 (share hooks), then Tasks 3–6 (convert the 4 `share_*` files — disjoint files, parallelizable after Task 1).
- **Batch B:** Task 2 (group hooks), then Tasks 7–11 (convert the 5 group files — disjoint, parallelizable after Task 2).

---

## Task 1: Share-state awaiters

**Files:**
- Modify: `crates/broker/src/share_partition/state.rs` (add accessor near the existing `delivery_complete_count`)
- Modify: `crates/broker/src/share_partition/manager.rs` (add `peek_for_test` near `get_or_load`)
- Modify: `crates/broker/src/broker.rs` (add awaiters near `share_state_summary_for_test`)

- [ ] **Step 1: Add `count_acquired_batches` to `AcquisitionState`**

In `crates/broker/src/share_partition/state.rs`, in `impl AcquisitionState`, add (note: `#[cfg(any(test, feature = "test-helpers"))]`, NOT `#[cfg(test)]`, so integration tests see it):

```rust
    /// Number of in-flight batches currently in `Acquired` state. Test-only.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    pub(crate) fn count_acquired_batches(&self) -> i32 {
        i32::try_from(
            self.batches
                .iter()
                .filter(|b| b.state == RecordState::Acquired)
                .count(),
        )
        .unwrap_or(i32::MAX)
    }
```

- [ ] **Step 2: Add a non-loading cell peek to the manager**

In `crates/broker/src/share_partition/manager.rs`, in `impl SharePartitionLeaderManager`, add (does NOT load from the persister — a pure read of the live cell, returns `None` if this partition isn't currently led/loaded here):

```rust
    /// Test-only: borrow the live acquisition cell without loading from the
    /// persister (returns `None` if not currently led/loaded on this node).
    #[cfg(any(test, feature = "test-helpers"))]
    pub(crate) fn peek_for_test(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Option<std::sync::Arc<tokio::sync::Mutex<AcquisitionState>>> {
        self.leaders
            .get(&(group.to_string(), topic_id, partition))
            .map(|c| c.value().clone())
    }
```

(If the file's `Mutex`/`Arc` imports differ, use the same paths `get_or_load` uses for `Arc<Mutex<AcquisitionState>>`.)

- [ ] **Step 3: Add the share-state awaiters to `BrokerHandle`**

In `crates/broker/src/broker.rs`, after `share_state_summary_for_test` (~line 443), add:

```rust
    /// Test-only: await until the persisted share-state summary exists for
    /// `(group, topic_id, partition)` (share-state initialized / recovered).
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub async fn wait_for_share_state_summary(&self, group: &str, topic_id: uuid::Uuid, partition: i32) {
        let res = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                if self.share_state_summary_for_test(group, topic_id, partition).await.is_some() {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        })
        .await;
        assert!(res.is_ok(), "share-state summary for {group}:{topic_id}:{partition} not present within 30s");
    }

    /// Test-only: await until the share-partition SPSO (start_offset) >= `min`.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub async fn wait_until_share_spso(&self, group: &str, topic_id: uuid::Uuid, partition: i32, min: i64) {
        let res = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                if let Some((_, _, spso, _)) = self.share_state_summary_for_test(group, topic_id, partition).await {
                    if spso >= min {
                        return;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        })
        .await;
        assert!(res.is_ok(), "share SPSO for {group}:{topic_id}:{partition} did not reach {min} within 30s");
    }

    /// Test-only: await until the share-partition delivery-complete count >= `min`.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub async fn wait_until_share_delivery_complete(&self, group: &str, topic_id: uuid::Uuid, partition: i32, min: i32) {
        let res = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                if let Some((_, _, _, dcc)) = self.share_state_summary_for_test(group, topic_id, partition).await {
                    if dcc >= min {
                        return;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        })
        .await;
        assert!(res.is_ok(), "share dcc for {group}:{topic_id}:{partition} did not reach {min} within 30s");
    }

    /// Test-only: await until the live share-partition has exactly `n` Acquired
    /// in-flight batches (e.g. after a ShareFetch acquires, or after lock-timeout
    /// redelivery returns records to Available — count drops back).
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub async fn wait_until_share_acquired_count(&self, group: &str, topic_id: uuid::Uuid, partition: i32, n: i32) {
        let res = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                if let Some(cell) = self._broker.share_partition_leaders.peek_for_test(group, topic_id, partition) {
                    let count = cell.lock().await.count_acquired_batches();
                    if count == n {
                        return;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        })
        .await;
        assert!(res.is_ok(), "share acquired-batch count for {group}:{topic_id}:{partition} did not reach {n} within 30s");
    }
```

- [ ] **Step 4: Verify it compiles**

Run: `cargo build -p crabka-broker --features test-helpers --lib`
Expected: PASS. If `share_state_summary_for_test`'s tuple order differs from `(state_epoch, leader_epoch, start_offset, dcc)`, adjust the destructures (it returns `Option<(i32, i32, i64, i32)>` where the `i64` is the SPSO). If `Mutex` is `tokio::sync::Mutex` confirm `.lock().await`; if it's `std::sync::Mutex`, use `.lock().unwrap()`.

- [ ] **Step 5: clippy + commit**

Run: `cargo clippy -p crabka-broker --features test-helpers --lib -- -D warnings` → clean.
```bash
cargo fmt -p crabka-broker
git add crates/broker/src/share_partition/state.rs crates/broker/src/share_partition/manager.rs crates/broker/src/broker.rs
git commit -m "feat(broker): add test-only share-state wait_* awaiters to BrokerHandle"
```

---

## Task 2: Group-state awaiters

**Files:**
- Modify: `crates/broker/src/coordinator/unified/actor.rs` (extend `DescribeMember` + `build_describe`)
- Modify: `crates/broker/src/broker.rs` (add `group_describe_for_test` + group awaiters)

- [ ] **Step 1: Project `assignment_state` into `DescribeMember`**

In `crates/broker/src/coordinator/unified/actor.rs`, add a field to `DescribeMember` (after `is_classic`):

```rust
    pub is_classic: bool,
    pub assignment_state: crate::coordinator::unified::persistence_next_gen::MemberAssignmentState,
```

In `build_describe`, populate it: add `assignment_state: m.assignment_state,` to the `DescribeMember { ... }` literal. (Use whatever path resolves to `MemberAssignmentState` within this module — it may already be in scope as `MemberAssignmentState`.) Ensure `MemberAssignmentState` derives `Clone, Copy, PartialEq, Eq` (it is a `#[repr(i32)]`-style enum; add the derives if missing so `DescribeMember`'s `Clone`/comparisons compile).

> If, while implementing Task 11 (`streams_classic_upgrade.rs`), you find the test needs the classic member's `generation_id` (not derivable from `member_epoch`/`is_classic`), also add `pub classic_generation: Option<i32>` to `DescribeMember` and populate it from `m.classic.as_ref().map(|c| c.generation_id)` (or the equivalent field on `ClassicMemberFacade`). Only add it if needed.

- [ ] **Step 2: Add `group_describe_for_test` + group awaiters to `BrokerHandle`**

In `crates/broker/src/broker.rs`, add (use the actual `DescribeView` / `GroupActorMessage` import paths — `crate::coordinator::unified::actor::{DescribeView, GroupActorMessage}` or re-exports):

```rust
    /// Test-only: describe a consumer/share/streams group via its actor.
    /// `None` if the group has no live actor.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub async fn group_describe_for_test(
        &self,
        group_id: &str,
    ) -> Option<crate::coordinator::unified::actor::DescribeView> {
        let handle = self._broker.group_coordinator.groups.get(group_id)?.value().clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(crate::coordinator::unified::actor::GroupActorMessage::Describe { reply: tx })
            .await
            .ok()?;
        rx.await.ok()
    }

    /// Test-only: await until the group is Stable — non-empty membership with
    /// every member fully reconciled (member_epoch == group_epoch AND
    /// assignment_state == Stable).
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub async fn wait_for_group_stable(&self, group_id: &str) {
        use crate::coordinator::unified::persistence_next_gen::MemberAssignmentState;
        let res = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                if let Some(v) = self.group_describe_for_test(group_id).await {
                    if !v.members.is_empty()
                        && v.members.iter().all(|m| {
                            m.member_epoch == v.group_epoch
                                && m.assignment_state == MemberAssignmentState::Stable
                        })
                    {
                        return;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(30)).await;
            }
        })
        .await;
        assert!(res.is_ok(), "group {group_id} did not reach Stable within 30s");
    }

    /// Test-only: await until the group epoch >= `min`.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub async fn wait_until_group_epoch(&self, group_id: &str, min: i32) {
        let res = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                if let Some(v) = self.group_describe_for_test(group_id).await {
                    if v.group_epoch >= min {
                        return;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(30)).await;
            }
        })
        .await;
        assert!(res.is_ok(), "group {group_id} epoch did not reach {min} within 30s");
    }

    /// Test-only: await until the group has exactly `n` members.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub async fn wait_until_group_member_count(&self, group_id: &str, n: usize) {
        let res = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                let count = self.group_describe_for_test(group_id).await.map_or(0, |v| v.members.len());
                if count == n {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(30)).await;
            }
        })
        .await;
        assert!(res.is_ok(), "group {group_id} member count did not reach {n} within 30s");
    }

    /// Test-only: await until the group is empty/drained (no members).
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub async fn wait_until_group_empty(&self, group_id: &str) {
        self.wait_until_group_member_count(group_id, 0).await;
    }

    /// Test-only: await until the group's assigned active-task partitions
    /// (summed across all members) >= `min`.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::used_underscore_binding)]
    pub async fn wait_until_group_active_partitions(&self, group_id: &str, min: usize) {
        let res = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                if let Some(v) = self.group_describe_for_test(group_id).await {
                    let total: usize = v
                        .members
                        .iter()
                        .map(|m| m.assigned_partitions.values().map(Vec::len).sum::<usize>())
                        .sum();
                    if total >= min {
                        return;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(30)).await;
            }
        })
        .await;
        assert!(res.is_ok(), "group {group_id} active partitions did not reach {min} within 30s");
    }
```

- [ ] **Step 3: Verify it compiles**

Run: `cargo build -p crabka-broker --features test-helpers --lib`
Expected: PASS. Fix import paths for `DescribeView`/`GroupActorMessage`/`MemberAssignmentState` if they differ. If `wait_until_group_member_count`'s `n: usize` triggers a sign mismatch with how tests count, keep `usize`.

- [ ] **Step 4: clippy + commit**

Run: `cargo clippy -p crabka-broker --features test-helpers --lib -- -D warnings` → clean.
```bash
cargo fmt -p crabka-broker
git add crates/broker/src/coordinator/unified/actor.rs crates/broker/src/broker.rs
git commit -m "feat(broker): add test-only group-state wait_* awaiters to BrokerHandle"
```

---

## Task 3: De-flake `share_state.rs`

**Files:** Modify `crates/broker/tests/share_state.rs`

Open the file and apply the site-type rules. Specific sites (from the survey; line numbers drift — match on content):

- [ ] **Step 1: `state_survives_restart` — recovered-SPSO poll loop** (the `for _ in 0..40 { read_summary; if !not_ready break; sleep(100ms) }` that ends asserting `start_offset == 7`). Replace the loop with awaiting the recovered SPSO, then read once:
```rust
    broker.wait_until_share_spso("g1", tid, 0, 7).await;
    let start_offset = read_summary(&client, "g1", tid, 0).await.start_offset;
    assert!(start_offset == 7, "recovered SPSO must be 7, got {start_offset}");
```
(`broker` is the in-process `BrokerHandle`; if the test only has a `Client`, get the handle from the test's broker binding — `share_groups.rs`/`share_consume.rs` already call `broker.share_state_summary_for_test`, so a `BrokerHandle` is in scope under the same name.)

- [ ] **Step 2: pre-shutdown flush `sleep(300ms)` before `broker.shutdown()`** (after `write_state(... 7 ...)`). Replace with awaiting the written SPSO is persisted before shutdown:
```rust
    broker.wait_until_share_spso("g1", tid, 0, 7).await;
    broker.shutdown().await;
```
- [ ] **Step 3: `initialize_ready` helper** — this is a retry-on-retriable-RPC-error loop (retries `InitializeShareGroupState` while `not_ready(code)`). It has a fixed 100 ms backoff but is condition-terminated (returns on ready). KEEP it (legitimate retry-on-retriable; not a fixed-duration guess) — do NOT convert. (If you prefer, the broker-readiness could be awaited first, but the RPC must still be retried; leaving the bounded retry is correct.)

- [ ] **Step 4: Build, run, stress**

```bash
cargo build -p crabka-broker --test share_state
cargo test -p crabka-broker --test share_state -- --test-threads=1
```
Then stress 10×: `1..10 | %{ cargo test -p crabka-broker --test share_state -q -- --test-threads=1 }` (PowerShell) → 0 flakes. Confirm no state-poll `sleep` remains except the `initialize_ready` retry backoff. `cargo clippy -p crabka-broker --test share_state -- -D warnings` clean.

- [ ] **Step 5: Commit**
```bash
git add crates/broker/tests/share_state.rs
git commit -m "test(broker): de-flake share_state.rs with share-state awaiters"
```

---

## Task 4: De-flake `share_groups.rs`

**Files:** Modify `crates/broker/tests/share_groups.rs`

- [ ] **Step 1: `lifecycle_initializes_share_state` — the `for _ in 0..3 { heartbeat; sleep(100ms) }` settle loop** before checking each partition's summary. Send one heartbeat, then await each partition's summary instead of the fixed 3× loop:
```rust
    let mut hb = heartbeat("g5", &mid, r.member_epoch);
    hb.subscribed_topic_names = Some(vec!["t5".into()]);
    let _ = client.send(hb).await.unwrap();
    for p in 0..3 {
        broker.wait_for_share_state_summary("g5", tid, p).await;
        let (_se, _le, start_offset, _dcc) =
            broker.share_state_summary_for_test("g5", tid, p).await.unwrap();
        assert!(start_offset == 0, "partition {p} initialized at start_offset 0, got {start_offset}");
    }
```
(If the lifecycle hook requires steady-state heartbeats to make progress, keep a single heartbeat before the awaits; the await replaces the timing guess. If one heartbeat proves insufficient in the stress runs, send heartbeats inside the wait — but try the single-heartbeat-then-await form first.)

- [ ] **Step 2: `lifecycle_metadata_survives_restart` — same `for _ in 0..3 { heartbeat; sleep }` + the `sleep(200ms)` before shutdown** (lines ~330-348). Convert the settle loop as in Step 1 (await `wait_for_share_state_summary` for both partitions), and replace the pre-shutdown `sleep(200ms)` with the awaits already establishing the partitions are initialized (they ran just above), so the bare `sleep` before `broker.shutdown()` can be dropped.

- [ ] **Step 3: post-restart re-join settle `sleep` (line ~379)** — after re-join, replace the fixed settle with awaiting the recovered partitions' summaries are still present (`wait_for_share_state_summary` for each, asserting non-reinitialized via the persisted start_offset).

- [ ] **Step 4: pre-shutdown flush `sleep(300ms)` (line ~225)** in `state_survives_restart` ("give the actor's async log-flush time to land in __consumer_offsets") — this one has no share-state summary to await (it's a group join, not a share write). If a group-state condition is awaitable (the member joined — `wait_until_group_member_count("...", 1)`), use it; otherwise KEEP as a documented flush sleep.

- [ ] **Step 5: Build, run, stress, commit** (as Task 3 Steps 4–5, `--test share_groups`).

---

## Task 5: De-flake `share_consume.rs`

**Files:** Modify `crates/broker/tests/share_consume.rs` (the big one — ~28 sleeps)

Apply the rules by site type (survey-classified):

- [ ] **Step 1: partition materialization (line ~104) + produce-retry (lines ~238, ~1085)** → `broker.wait_until_partition_present(topic, 0).await` before producing; keep the produce-retry-on-`UNKNOWN_TOPIC_OR_PARTITION`/`NOT_LEADER_OR_FOLLOWER` as a bounded retry (or precede it with `wait_until_partition_present`).

- [ ] **Step 2: `__share_group_state` partitions led (line ~158)** + **share-state initialization (line ~184)** + **lifecycle heartbeat settle (line ~279)** → `broker.wait_for_share_state_summary(group, tid, p).await` for the relevant partition(s) (replaces the `for p in 0..50 { has_partition }` and the `share_state_summary_for_test` poll loop and the heartbeat-settle).

- [ ] **Step 3: acquire sites — `fetch_until_acquired` retries (lines ~458, ~630, ~686, ~1144)** → these are client ShareFetch retry loops. Precede the first fetch with `broker.wait_for_share_state_summary(group, tid, p).await` (so the partition is loaded), then keep the fetch-retry (a fetch returning records is a legitimate client outcome). Where the test asserts a specific acquired count, you may additionally `broker.wait_until_share_acquired_count(group, tid, p, n).await` before the assertion.

- [ ] **Step 4: post-restart SPSO checks (lines ~545, ~630)** → `broker.wait_until_share_spso(group, tid, p, expected).await`.

- [ ] **Step 5: lock-timeout redelivery/archive (lines ~738, ~776, ~788)** → replace the fixed `sleep(record_lock_duration + margin)` with the OUTCOME await:
  - redelivery: `broker.wait_until_share_acquired_count(group, tid, p, 0).await` (lock expired → records returned to Available → acquired count drops), then re-fetch.
  - archive (delivery-limit): `broker.wait_until_share_delivery_complete(group, tid, p, expected_dcc).await` (poison archived → dcc advances).

- [ ] **Step 6: precise renew-timing (`renew_extends_lock` lines ~886/~900, `no_renew_redelivers` line ~933)** → KEEP the calibrated sleeps (they prove a lock is/ isn't released at a precise deadline relative to the configured `record_lock_duration`). Add a one-line comment marking them as intentional calibrated timing, not flaky waits.

- [ ] **Step 7: `read_committed` consumer.poll() loops (lines ~1006, ~1032)** → KEEP.

- [ ] **Step 8: pre-shutdown flush (line ~523)** → await the persisted dcc/SPSO that was just written (`wait_until_share_delivery_complete` / `wait_until_share_spso`) before `broker.shutdown()`; drop the bare sleep.

- [ ] **Step 9: Build, run, stress (10×), clippy, commit** (`--test share_consume`).

---

## Task 6: De-flake `share_admin_offsets.rs`

**Files:** Modify `crates/broker/tests/share_admin_offsets.rs` (~14 sleeps)

- [ ] **Step 1:** partition materialization (line ~109) + produce-retry (line ~221) → `wait_until_partition_present` + bounded produce-retry.
- [ ] **Step 2:** `__share_group_state` led (line ~154) + share-state init (line ~176) + lifecycle settle (line ~259) → `wait_for_share_state_summary`.
- [ ] **Step 3:** `describe_until` SPSO-persistence poll (line ~396) + first-fetch-after-Alter (line ~559) + SPSO after Alter → `wait_until_share_spso(group, tid, p, wanted)`.
- [ ] **Step 4:** `alter_resets_empty_group` Alter-retry (line ~502) → keep the bounded RPC retry (retriable error) OR precede with the relevant readiness await.
- [ ] **Step 5:** dcc pre-restart poll (line ~783) + post-restart dcc recovered (line ~815) → `wait_until_share_delivery_complete(group, tid, p, n)` / `wait_for_share_state_summary`.
- [ ] **Step 6:** `delete_rewrites_metadata` absence polls (lines ~905, ~944) → these assert a topic is ABSENT from describe output. Awaiting an absence is a negative; if there's a positive precondition (the delete RPC succeeded), await that, then assert absence once. If purely a "stays absent" check, keep a bounded poll. Use judgment; prefer awaiting the delete's positive effect.
- [ ] **Step 7:** pre-shutdown flush (line ~791) → await the persisted dcc (just written) before shutdown; drop the bare sleep.
- [ ] **Step 8:** Build, run, stress (10×), clippy, commit (`--test share_admin_offsets`).

---

## Task 7: De-flake `streams_groups.rs`

**Files:** Modify `crates/broker/tests/streams_groups.rs`

- [ ] **Step 1: active-task convergence sleep** (waits for member's active-task partitions to reach target in Stable) → `broker.wait_for_group_stable(group_id).await` then `broker.wait_until_group_active_partitions(group_id, want_active).await` (or just the latter if the test only checks partition count). Keep the subsequent assertion that reads active-tasks via the existing path.
- [ ] **Step 2: changelog auto-creation + assignment sleep** → if it waits on the internal changelog topic existing, `broker.wait_until_partition_present(changelog_topic, 0).await`; if on assignment, `wait_until_group_active_partitions`. Keep the "no MISSING_INTERNAL_TOPICS status" assertion.
- [ ] **Step 3:** Build, run, stress (10×), clippy, commit (`--test streams_groups`).

---

## Task 8: De-flake `streams_classic_downgrade.rs`

**Files:** Modify `crates/broker/tests/streams_classic_downgrade.rs`

- [ ] **Step 1: active-task convergence sleep** → `wait_for_group_stable` / `wait_until_group_active_partitions`.
- [ ] **Step 2: two "group drained after leave" sleeps** (waits for the coordinator to process a member leave → group Empty) → `broker.wait_until_group_empty(group_id).await`.
- [ ] **Step 3: pre-shutdown persisted-state sleep** → keep if opaque (no awaitable condition); otherwise await the relevant group/share condition.
- [ ] **Step 4:** Build, run, stress (10×), clippy, commit (`--test streams_classic_downgrade`).

---

## Task 9: De-flake `streams_classic_upgrade.rs`

**Files:** Modify `crates/broker/tests/streams_classic_upgrade.rs`

- [ ] **Step 1: active-task convergence sleep** → `wait_for_group_stable` / `wait_until_group_active_partitions`.
- [ ] **Step 2: classic-member registration sleep** (waits for a classic member to register with a generation) → `broker.wait_until_group_member_count(group_id, expected).await`, and if the test asserts on the classic member's `generation_id`, add `classic_generation` to `DescribeMember` (Task 2 Step 1 note) and await/assert via `group_describe_for_test`; otherwise assert presence via `is_classic` member in the describe view.
- [ ] **Step 3: group-settled-after-join sleep** → `wait_for_group_stable` (or `wait_until_group_member_count` + a "not Preparing" check via `member_epoch == group_epoch`).
- [ ] **Step 4:** Build, run, stress (10×), clippy, commit (`--test streams_classic_upgrade`).

---

## Task 10: De-flake `consumer_group_next_gen_persistence.rs`

**Files:** Modify `crates/broker/tests/consumer_group_next_gen_persistence.rs`

- [ ] **Step 1: two pre-shutdown "group state persisted before restart" sleeps** → these wait for the next-gen group state to flush to the log before a restart. If a positive group condition is awaitable (e.g. the member joined / committed offset visible), await it; the survey marked these keep-as-is (shutdown coordination, no condition exposed). Prefer awaiting `wait_until_group_member_count`/`wait_for_group_stable` for the join, then a minimal flush window only if genuinely needed. Document any retained sleep as a flush window.
- [ ] **Step 2:** Build, run, stress (10×), clippy, commit (`--test consumer_group_next_gen_persistence`).

---

## Task 11: De-flake `consumer_proactive_validation.rs`

**Files:** Modify `crates/broker/tests/consumer_proactive_validation.rs`

- [ ] **Step 1: metadata-image leader-epoch + partition sleeps** (2 sites) → reuse Phase 1 awaiters: `wait_until_partition_present` and (for the epoch advance) `wait_until_partition_leader_changed` or a metadata-image predicate via `wait_for_image`.
- [ ] **Step 2: consumer-assignment-published sleep** (member_epoch ≥ 1 AND consumer.assignment() non-empty) → `broker.wait_until_group_epoch(group_id, 1).await` (or `wait_for_group_stable`), then proceed; keep the `consumer.assignment()` assertion.
- [ ] **Step 3: two `consumer.poll()` validation loops** → KEEP (legitimate consumer API).
- [ ] **Step 4:** Build, run, stress (10×), clippy, commit (`--test consumer_proactive_validation`).

---

## Final verification (after all tasks)

- [ ] **Run all 9 converted test binaries**
```bash
cargo test -p crabka-broker --test share_state --test share_groups --test share_consume --test share_admin_offsets -- --test-threads=1
cargo test -p crabka-broker --test streams_groups --test streams_classic_downgrade --test streams_classic_upgrade --test consumer_group_next_gen_persistence --test consumer_proactive_validation -- --test-threads=1
```
Expected: all PASS.

- [ ] **Stress the share group (the known Windows flakes) 20×** (PowerShell):
```powershell
1..20 | ForEach-Object {
  cargo test -p crabka-broker --test share_consume --test share_admin_offsets --test share_state --test share_groups -q -- --test-threads=1
  if ($LASTEXITCODE -ne 0) { Write-Error "FLAKE on run $_"; break }
}
```
Expected: 20/20 PASS.

- [ ] **fmt + clippy**
```bash
cargo fmt -p crabka-broker
cargo clippy -p crabka-broker --features test-helpers --lib -- -D warnings
```
plus clippy on each converted test binary. Expected: clean.

- [ ] **Confirm no state-guessing sleeps remain**: `grep -n "tokio::time::sleep" crates/broker/tests/{share_state,share_groups,share_consume,share_admin_offsets,streams_groups,streams_classic_downgrade,streams_classic_upgrade,consumer_group_next_gen_persistence,consumer_proactive_validation}.rs` — every remaining `sleep` must be one of: a `consumer.poll()` timeout arg, a calibrated `record_lock_duration`-derived renew-timing sleep, a bounded retry-on-retriable-RPC backoff, or a documented opaque flush window. No "guess how long async state took" sleeps.

## Self-review notes (addressed)

- **Spec coverage:** share-state awaiters (T1) ✓; group-state awaiters + DescribeView extension (T2) ✓; lock-timeout outcome-await + calibrated renew-timing exceptions (T5 S5–S6) ✓; pre-shutdown flush → await-persisted-condition where possible (T3 S2, T5 S8, T6 S7) ✓; reuse Phase 1 image awaiters (T11 S1, T5 S1) ✓; consumer.poll() kept (T5 S7, T11 S3) ✓; all 9 files (T3–T11) ✓; stress verification on the Windows flakes ✓.
- **Type consistency:** awaiter names (`wait_for_share_state_summary`, `wait_until_share_spso`, `wait_until_share_delivery_complete`, `wait_until_share_acquired_count`, `group_describe_for_test`, `wait_for_group_stable`, `wait_until_group_epoch`, `wait_until_group_member_count`, `wait_until_group_empty`, `wait_until_group_active_partitions`) defined in T1/T2 and used identically in T3–T11. `count_acquired_batches` / `peek_for_test` defined T1, used by `wait_until_share_acquired_count`. `DescribeMember.assignment_state` defined T2, used by `wait_for_group_stable`.
- **Known judgment sites:** retry-on-retriable-RPC loops (`initialize_ready`, Alter-retry, produce-retry) and absence-polls (`delete_rewrites_metadata`) are flagged in-task as keep-or-await-precondition decisions for the implementer, since they are not fixed-duration flaky sleeps.
