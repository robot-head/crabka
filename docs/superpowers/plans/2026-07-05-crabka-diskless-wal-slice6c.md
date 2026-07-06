# Diskless WAL — Slice 6c Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Generalize offset assignment to concurrent stateless appenders (widen the commit to return the applied base), flip the diskless write path leaderless, add sequencer-authority handoff, and land the verified concurrent offset-allocator kernel.

**Architecture:** Any broker accepts a diskless write, calls `ConcurrentSequencer::assign` (which submits an advance and reads the applied base back **from the commit** — the controller's serialized apply order gives each concurrent appender a unique contiguous range), appends to the 6a quorum WAL, and acks on quorum-commit. The nominal leader stays a wire fiction. A Creusot kernel proves the concurrent ranges partition `[0, next)` gap-free.

**Tech Stack:** Rust 2024 (pinned stable 1.96.0), Creusot (`cargo creusot`, CI replay), `tokio`, `assert2`, `cargo +nightly fmt`, `clippy::pedantic`.

**Spec:** [`docs/superpowers/specs/2026-07-05-crabka-diskless-wal-slice6c-design.md`](../specs/2026-07-05-crabka-diskless-wal-slice6c-design.md).

**PREREQUISITES (unlanded):** Slices 1–5 + 6a. Generalizes Slice-2's `OffsetSequencer`/`ControllerSequencer`; appends to 6a's `QuorumWalStore`. (6b for leaderless serving of the writes this enables.)

---

## Invariants

1. **Concurrent ranges are a gap-free/unique/monotonic partition of `[0, next)`** — proven by the Creusot kernel and exercised end-to-end.
2. **Base comes from the commit, never a racy post-commit image read.**
3. **Handoff never regresses or duplicates an offset.**
4. **Wire still names a leader** (Metadata/`current_leader`/epoch fencing preserved) even though writes are leaderless.
5. **Classic (non-diskless) path unchanged** — the leader gate and read-after-commit stay for non-diskless.
6. **Every task ends green** before its commit.

## Scope boundary

- **In scope:** widen-commit-return; `ConcurrentSequencer`; the leaderless-write gate flip + min.insync reinterpretation; the Creusot offset-allocator kernel.
- **Deferred:** the throughput move to WAL-commit assignment; the re-composed gate + Jepsen (6d).

---

## Task 1: Widen the commit path to return the applied base

**Files:**
- Modify: `crates/raft/src/kraft/controller.rs`, `crates/raft/src/kraft/transport.rs`

- [ ] **Step 1: Write the failing test**

Two concurrent `V1PartitionOffsetAdvance` submits for the same partition return **distinct, contiguous** applied bases (e.g. `advance(3)` → base 0, `advance(2)` → base 3), each carried back to its own submitter (not a shared post-commit read).

- [ ] **Step 2: Run to verify it fails; implement**

Add an `AssignOffsets { topic, partition, count, reply: oneshot<Result<Offset>> }` command (or widen the `submit_change` reply) that, in the apply path (`controller.rs:1441-1448`, next to the Slice-2 `V1PartitionOffsetAdvance` apply), records the **pre-increment** `partition_next_offsets` slot value and resolves *that submitter's* reply with it. The single serialized apply loop (`:445`) gives concurrent advances a total order → unique contiguous ranges. (This is the return-from-commit Slice 2 deferred as "the S6-era robustification.")

- [ ] **Step 3: Run to verify + commit**

Run → PASS.

```bash
git add crates/raft/src/kraft/controller.rs crates/raft/src/kraft/transport.rs
git commit -m "feat(raft): return the applied base offset from the offset-advance commit"
```

---

## Task 2: `ConcurrentSequencer` (impl the `OffsetSequencer` seam)

**Files:**
- Create/Modify: `crates/broker/src/wal/…` (next to Slice-2's `ControllerSequencer`)

- [ ] **Step 1: Write the failing test**

`ConcurrentSequencer::assign(topic, part, N)` from **many concurrent tasks** against a single-voter controller returns contiguous, unique, gap-free, strictly-monotonic ranges (assert the sorted set of `[base, base+N)` tiles `[0, total)`).

- [ ] **Step 2: Run to verify it fails; implement**

`ConcurrentSequencer` implements the Slice-2 `OffsetSequencer::assign` trait: submit the advance via Task 1's `AssignOffsets` and return the applied base directly (no read-after-commit). Wire it as the diskless `OffsetSequencer` (replacing `ControllerSequencer`) at the Slice-2 construction site.

- [ ] **Step 3: Run to verify + commit**

Run → PASS.

```bash
git add -A
git commit -m "feat(broker): ConcurrentSequencer — concurrent-safe offset assignment"
```

---

## Task 3: Leaderless write path (produce-gate flip)

**Files:**
- Modify: `crates/broker/src/handlers/produce.rs`

- [ ] **Step 1: Write the failing test**

A produce sent to a **non-leader** broker for a **diskless** topic is accepted, sequenced, quorum-committed, and acked; the same produce to a non-leader **classic** topic still returns `NOT_LEADER_OR_FOLLOWER`.

- [ ] **Step 2: Run to verify it fails; implement**

In `crates/broker/src/handlers/produce.rs`, for a diskless partition replace the `leader != self.node_id` rejection (`:459-476`) with accept-and-sequence: call `ConcurrentSequencer::assign`, append to the 6a `QuorumWalStore`, ack on quorum-commit. Reinterpret the `min.insync.replicas` preflight (`:513-521`) against the WAL quorum's f+1 budget for diskless (not the classic image ISR). Keep `install_leader_change`/the advertised nominal leader intact. Non-diskless topics keep the exact existing gate.

- [ ] **Step 3: Run to verify + commit**

Run → PASS.

```bash
git add crates/broker/src/handlers/produce.rs
git commit -m "feat(broker): leaderless diskless write path (accept-and-sequence on any broker)"
```

---

## Task 4: The verified concurrent offset-allocator kernel

**Files:**
- Create: `crates/verified/src/offset_allocator.rs`; Modify: `crates/verified/src/lib.rs`

- [ ] **Step 1: Write the failing oracle test**

A sort-and-check-contiguity oracle: given concurrently-assigned `[base, count)` ranges, `is_gap_free_partition` is true iff the sorted ranges tile `[0, next)` with no gap/overlap. Test it against known good/bad range sets (mirror `hwm_sort_oracle`, `consensus.rs`).

- [ ] **Step 2: Run to verify it fails; implement + prove**

Implement `assign_ranges`/`is_gap_free_partition` in `crates/verified/src/offset_allocator.rs` with Creusot contracts (`#[requires]`/`#[ensures]`, pearlite specs) building like `count_ge_prefix` + monotone/nonnegative lemmas (`consensus.rs:20-57`): `#[ensures]` the result ranges form a gap-free/unique/monotonic partition of `[0, next)`. Export from `lib.rs`; call it from `ConcurrentSequencer`'s range validation.

- [ ] **Step 3: Prove + commit**

Run: `cargo creusot` (proof) + `cargo test -p crabka-verified offset_allocator` (oracle). Add to the CI proof-replay set.

```bash
git add crates/verified/src/offset_allocator.rs crates/verified/src/lib.rs
git commit -m "feat(verified): concurrent offset-allocator kernel (gap-free partition proof)"
```

---

## Task 5: Final gate

- [ ] **Step 1:** `cargo +nightly fmt --check` — no diff.
- [ ] **Step 2:** `cargo clippy --workspace --all-targets -- -D warnings` — no warnings.
- [ ] **Step 3:** `cargo nextest run -p crabka-raft -p crabka-broker -p crabka-verified` (or `cargo test`) + `cargo creusot` replay — PASS.
- [ ] **Step 4:** Commit any formatting.

---

## Self-Review

**1. Spec coverage:** widen-commit-return (Task 1); `ConcurrentSequencer` (Task 2); leaderless write flip + min.insync reinterpretation (Task 3); the verified kernel (Task 4). Deferred set (WAL-commit assignment, gate+Jepsen 6d) untouched — Scope boundary. ✅

**2. Placeholder scan:** Tasks give the concrete change + the exact site (`controller.rs:1441-1448`/`:445`, `produce.rs:459-476`/`:513-521`, `consensus.rs:20-57` as the kernel template). No `TBD`/`TODO`.

**3. Type consistency:** `ConcurrentSequencer` implements the same `OffsetSequencer::assign(&str, PartitionIndex, u32) -> Result<Offset>` (Task 2) the writer calls (Task 3); the `AssignOffsets`/widened reply carries the base the sequencer returns (Task 1→2); the kernel's `is_gap_free_partition` (Task 4) validates the ranges Task 2 assigns.

**4. Invariant check:** ranges proven gap-free/unique/monotonic (Task 4, exercised Task 2); base from commit not read (Task 1); handoff no-regress (via KRaft commit durability + fail-waiters, tested Task 2/spec); wire names a leader (Task 3 keeps `install_leader_change`); classic path unchanged (diskless-gated edits). Each task green.

**5. Prerequisites flagged:** Slices 1-5 + 6a unlanded; generalizes Slice-2's seam; appends to 6a — stated in the header.
