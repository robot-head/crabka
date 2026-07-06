# Diskless WAL — Slice 6b Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let any WAL-group member serve a byte-exact diskless read: drive `ReplicaState.hw` from the quorum-committed watermark on *every* member, allow a non-leader member to answer a diskless fetch, and add a hot-tail in-memory cache as a latency fast-path.

**Architecture:** 6a replicates each batch (fsync) to every WAL-group member's local WAL-replica `Log`, so every member holds the committed tail. 6b (read-side only) sources `ReplicaState.hw` from 6a's `on_watermark_advance` on every member (today HW installs only on `leader == self`, `replicator_supervisor.rs:353-364`), relaxes the leader-only consumer-serve check for diskless, and adds a hot-tail cache in `do_read`. The write/ack path and offset assignment are untouched (leaderless writes are 6c).

**Tech Stack:** Rust 2024 (pinned stable 1.96.0), `tokio`, `bytes`, `assert2`, `cargo +nightly fmt`, `clippy::pedantic` (`unsafe_code = "forbid"`).

**Spec:** [`docs/superpowers/specs/2026-07-05-crabka-diskless-wal-slice6b-design.md`](../specs/2026-07-05-crabka-diskless-wal-slice6b-design.md).

**PREREQUISITES (unlanded):** Slices 1–5 + 6a. Consumes 6a's per-member WAL-replica log + `WalShardEngine::on_watermark_advance` and Slice-1's `recompute_hw_for_wal_durable`. Read-side only — depends on 6a, **not** 6c.

---

## Invariants

1. **HW from the *committed* watermark only.** Never source HW from a local optimistic offset — a member must never over-report HW (would serve un-committed data). Under-reporting (lagging observer) is safe.
2. **Correctness floor = local WAL-replica read.** The hot-tail cache is advisory; a miss/stale entry falls through to the authoritative `do_read`.
3. **Cache/network bytes take the `Raw` drain**, never sendfile.
4. **Write/ack path untouched** — 6b is read-side only (leaderless writes = 6c).
5. **Every task ends green** before its commit.

## Scope boundary

- **In scope:** HW-from-quorum-watermark on every member; non-leader serve for diskless; the hot-tail cache + commit-stream observer.
- **Deferred:** leaderless write path / produce-gate flip + client routing to non-leaders (6c / KIP-392); re-composed gate + Jepsen (6d); sendfile for cache bytes.

---

## File Structure

- **`crates/broker/src/replicator_supervisor.rs`** — install the quorum-watermark HW source on every diskless WAL-group member.
- **`crates/broker/src/handlers/fetch.rs`** — relax the leader-only consumer-serve check for diskless; add the hot-tail cache fast-path in `do_read`.
- **`crates/broker/src/diskless/hot_tail.rs`** (new) — the in-memory verbatim-batch cache + commit-stream observer.

---

## Task 1: Drive `ReplicaState.hw` from the quorum watermark on every member

**Files:**
- Modify: `crates/broker/src/replicator_supervisor.rs`

- [ ] **Step 1: Write the failing test**

In a 3-member diskless WAL group, quorum-commit an `acks=all` produce; assert **every** member's `ReplicaState.hw` equals the quorum-committed watermark (not just the leader's; non-leaders were zero pre-6b).

```rust
    #[tokio::test]
    async fn every_wal_member_hw_tracks_quorum_watermark() { /* ... */ }
```

- [ ] **Step 2: Run to verify it fails; implement**

In `crates/broker/src/replicator_supervisor.rs`, the HW/ISR machinery installs only where `part_record.leader == self.node_id` (`:353-364`). For a **diskless** partition, additionally drive `ReplicaState.hw` from the quorum-committed watermark on every WAL-group member: subscribe each member's `ReplicaState` to its `WalShardEngine::on_watermark_advance` (6a), calling `recompute_hw_for_wal_durable(watermark)` (Slice 1) and firing `hw_advance_notify` — regardless of `leader == self`. Keep the classic (`leader == self`) `install_isr` path unchanged for non-diskless topics. (`install_leader_change` at `:351` already runs on every broker, so the nominal leader is still tracked/reported.)

- [ ] **Step 3: Run to verify + commit**

Run → PASS.

```bash
git add crates/broker/src/replicator_supervisor.rs
git commit -m "feat(broker): source ReplicaState.hw from the quorum watermark on every WAL member"
```

---

## Task 2: Let a non-leader WAL-group member serve a diskless fetch

**Files:**
- Modify: `crates/broker/src/handlers/fetch.rs`

- [ ] **Step 1: Write the failing test**

A consumer fetch answered by a **non-leader** WAL-group member returns the same bytes and the same HW-bounded window as the leader would; a fetch beyond HW returns empty (not stale).

```rust
    #[tokio::test]
    async fn non_leader_member_serves_byte_exact_diskless_read() { /* ... */ }
```

- [ ] **Step 2: Run to verify it fails; implement**

In `crates/broker/src/handlers/fetch.rs`, the consumer-fetch path assumes the serving broker is the leader (grep for the leadership / preferred-replica / `NOT_LEADER_OR_FOLLOWER` check in the fetch handler). For a **diskless** partition whose local WAL-replica `Log` holds `[log_start, hw)`, allow this member to serve the read via the existing `do_read` (which reads the local WAL-replica log gated at `hw` — now correct per Task 1), without requiring `leader == self`. Non-diskless topics keep the existing leader/KIP-392 check. Byte-exactness and the visibility window (`compute_visibility_window`, `fetch.rs:1022-1042`) are unchanged.

- [ ] **Step 3: Run to verify + commit**

Run → PASS. Also run the classic fetch suite (non-diskless unaffected).

```bash
git add crates/broker/src/handlers/fetch.rs
git commit -m "feat(broker): allow any WAL-group member to serve a diskless fetch"
```

---

## Task 3: Hot-tail latency cache

**Files:**
- Create: `crates/broker/src/diskless/hot_tail.rs`; Modify: `crates/broker/src/handlers/fetch.rs`

- [ ] **Step 1: Write the failing tests**

`HotTailCache` (per `(topic_id, partition)` `BTreeMap<base_offset, Bytes>` of verbatim v2 batches, bounded ring) — `insert(base_offset, bytes)`, `get(fetch_offset) -> Option<Bytes>` (floor lookup to the batch covering `fetch_offset`). A `do_read` in `[flushed_frontier, hw)` that hits the cache returns bytes identical to the local WAL-replica read; a miss falls through and still returns correct bytes.

```rust
    #[test]
    fn hot_tail_cache_floor_lookup_and_bound() { /* ... */ }
    #[tokio::test]
    async fn cache_hit_matches_local_read_miss_falls_through() { /* ... */ }
```

- [ ] **Step 2: Run to verify they fail; implement**

Create `crates/broker/src/diskless/hot_tail.rs`: the bounded per-partition cache + a **commit-stream observer** that inserts verbatim batches as the member observes 6a's WAL commit stream (`on_watermark_advance` carries/notifies the newly-committed range; feed those batches in). In `fetch.rs` `do_read`, before the local disk read, if the partition is diskless and `fetch_offset` is in `[flushed_frontier, hw)`, try `cache.get(fetch_offset)` → on hit set `RecordsPayload::Raw(bytes)` (owned, **no sendfile**) and return; on miss, proceed to the authoritative local read. Key/validate entries against the local log's `end_offset`/epoch so a truncation can't serve stale bytes (miss → authoritative read).

- [ ] **Step 3: Run to verify + commit**

Run → PASS.

```bash
git add crates/broker/src/diskless/hot_tail.rs crates/broker/src/handlers/fetch.rs
git commit -m "feat(broker): hot-tail in-memory cache as a diskless fetch latency fast-path"
```

---

## Task 4: Final gate

- [ ] **Step 1:** `cargo +nightly fmt` then `--check` — no diff.
- [ ] **Step 2:** `cargo clippy --workspace --all-targets -- -D warnings` — no warnings.
- [ ] **Step 3:** `cargo nextest run -p crabka-broker` (or `cargo test`) — PASS, including the non-leader-serve + cache tests.
- [ ] **Step 4:** Commit any formatting.

---

## Self-Review

**1. Spec coverage:** HW from the quorum watermark on every member (Task 1); non-leader serve (Task 2); hot-tail cache latency path (Task 3). Deferred set (leaderless writes/routing 6c, gate+Jepsen 6d, sendfile-for-cache) untouched — Scope boundary. ✅

**2. Placeholder scan:** Task 3 is close to complete code (the cache + fast-path). Tasks 1-2 are handler edits that give the exact site (`replicator_supervisor.rs:353-364`; the fetch leadership check) and the exact reuse (`recompute_hw_for_wal_durable`, `do_read`, `compute_visibility_window`); the two "grep for the leadership check" pointers name the real code to locate. No `TBD`/`TODO`.

**3. Type consistency:** `recompute_hw_for_wal_durable(watermark)` (Slice 1) + `hw_advance_notify` are reused identically on every member (Task 1); `do_read`/`compute_visibility_window`/`RecordsPayload::Raw` (Slice 4) are the same types the cold path uses (Tasks 2-3); `HotTailCache::{insert,get}` (Task 3) match the observer + `do_read` call sites.

**4. Invariant check:** HW sourced only from the committed watermark, never over-reporting (Task 1); correctness floor = local WAL read, cache advisory (Task 3); cache bytes take the `Raw` drain (Task 3); write/ack path untouched (no produce edits). Each task green.

**5. Prerequisites flagged:** Slices 1-5 + 6a unlanded; read-side only (depends on 6a, not 6c) — stated in the header.
