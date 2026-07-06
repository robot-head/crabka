# Diskless WAL — Slice 5 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a diskless partition recover cleanly on crash-restart and prove no acked record is ever lost: truncate the dangling leader-epoch checkpoint on tail recovery, rebuild idempotent-producer sequence dedup from the recovered WAL, re-anchor the local append cursor to the KRaft frontier, tighten trim to committed-index durability, and add a partial-durability crash-injection stateright model.

**Architecture:** Recovery extends `Log::open`. `recover_active_tail` already truncates a torn trailing batch; Slice 5 adds the missing epoch-checkpoint truncation, forces `validate_on_open` for diskless, and (broker-side) scans the recovered tail to rebuild `ProducerState`. On the first diskless produce after restart, the append cursor is re-anchored to `max(log_end_offset, reconciled_frontier)` where `reconciled_frontier` is the committed KRaft next-offset — turning the `[KRaft-commit, fsync)` crash window into a benign consumer-visible gap rather than a hard `OffsetMismatch`. Trim is gated on committed-index durability (not just the in-memory cache). A new, tighter diskless-only stateright model injects partial durability + `Crash`/`Recover` and asserts no `wal_acked` loss.

**Tech Stack:** Rust 2024 (pinned stable 1.96.0), `stateright` (dev), `tokio`, `assert2`, `cargo +nightly fmt`, `clippy::pedantic` (`unsafe_code = "forbid"`).

**Spec:** [`docs/superpowers/specs/2026-07-05-crabka-diskless-wal-slice5-design.md`](../specs/2026-07-05-crabka-diskless-wal-slice5-design.md).

**PREREQUISITES (unlanded):** Slices 1–4. Tasks 3–4 edit Slice-2's `append_verbatim_at` and Slice-4's trim gate (both spec-only) — written against their specced shapes. The model (Task 5) builds on the Slice-1 `wal_acked` ghost. Land Slices 1–4 first.

---

## Invariants

1. **No acked data lost on crash-restart.** Ack fires strictly after `fsync`, so anything lost pre-fsync was never acked (a benign offset gap). The proof (Task 5) asserts this across every interleaving.
2. **Trim ≤ committed-index-frontier.** Never trim past an offset whose `WalFlushRecord` isn't durably committed to `__diskless_wal_index` — the one window that could lose acked data.
3. **Re-anchor never masks a real gap.** `base == max(log_end_offset, reconciled_frontier)` keeps `base ≥ log_end_offset`; `reconciled_frontier` comes only from the committed KRaft frontier.
4. **Producer dedup keyed to the recovered LEO.** The rebuild must not wrongly dedup a fresh retry or accept a duplicate.
5. **Node-loss is out of scope.** The un-flushed acked tail is not recoverable on a different broker (Slice 6). The model marks `NodeLoss` explicitly out of scope.
6. **Every task ends green** before its commit.

## Scope boundary

- **In scope:** epoch-checkpoint truncation + `validate_on_open` for diskless; producer-sequence rebuild; KRaft re-anchor; trim-durability tighten; the partial-durability crash model.
- **Deferred:** node/disk-loss durability + object-side producer snapshots + durable PID allocation (Slice 6 / follow-up); orphan-object GC; transactional/LSO recovery; Creusot.

---

## File Structure

- **`crates/log/src/log.rs`** (`Log::open`) — truncate the epoch checkpoint to the recovered LEO; expose the recovered LEO for the producer rebuild.
- **`crates/broker/src/…`** — the diskless-open recovery: force `validate_on_open`; producer-sequence rebuild (scan the recovered tail → `ProducerState`); the KRaft re-anchor (`reconciled_frontier`).
- **Slice-2 `append_verbatim_at`** — relax the guard to `max(log_end_offset, reconciled_frontier)`.
- **`crates/broker/src/diskless/flusher.rs`** (Slice 4) — tighten the trim gate to committed-index durability.
- **A new `crates/broker/src/diskless_crash_model.rs`** — the partial-durability stateright model.

---

## Task 1: Truncate the dangling leader-epoch checkpoint on tail recovery (Seam B)

`recover_active_tail` truncates a torn trailing batch (`segment.rs:266-269`), but `Log::open` never truncates the leader-epoch checkpoint to match (`log.rs:265`) — a torn batch that introduced a new epoch leaves an entry dangling past `log_end_offset`, corrupting epoch→offset lookups. Fix it (an unconditional correctness fix; also latent for classic topics).

**Files:**
- Modify: `crates/log/src/log.rs`

- [ ] **Step 1: Write the failing test**

In `crates/log/src/log.rs` tests: open a log, append a batch that introduces leader epoch `E` at offset `X`, physically corrupt/truncate the log file just past `X`'s batch start (so `recover_active_tail` drops it), reopen, and assert the epoch checkpoint has **no** entry with `start_offset >= log_end_offset()`.

```rust
    #[test]
    fn open_truncates_epoch_checkpoint_to_recovered_leo() {
        // ... append batch at epoch E introducing an entry at offset X;
        //     truncate the segment file mid-that-batch; reopen with validate_on_open;
        //     assert epoch_checkpoint().latest_epoch()/entries have none past log_end_offset().
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-log open_truncates_epoch_checkpoint`
Expected: FAIL — the dangling entry survives.

- [ ] **Step 3: Implement**

In `crates/log/src/log.rs` `Log::open`, change `let epoch_checkpoint = ...` (`:265`) to `let mut epoch_checkpoint = ...`, and after `let lso = active.last_offset() + 1;` (`:267`) add:

```rust
        // Drop any leader-epoch entry dangling past the recovered LEO (a torn
        // trailing batch that introduced a new epoch would otherwise leave an
        // entry beyond `log_end_offset`, corrupting epoch->offset lookups).
        epoch_checkpoint.truncate_from_end(lso)?;
```

(`truncate_from_end` drops entries with `start_offset >= end_offset` — `leader_epoch_checkpoint.rs:104`; `lso` == recovered `log_end_offset`.)

- [ ] **Step 4: Run to verify it passes; commit**

Run: `cargo test -p crabka-log open_truncates_epoch_checkpoint` → PASS. Also `cargo test -p crabka-log` (no classic regression).

```bash
git add crates/log/src/log.rs
git commit -m "fix(log): truncate leader-epoch checkpoint to recovered LEO on open"
```

---

## Task 2: Rebuild idempotent-producer sequence dedup from the recovered WAL

`ProducerState` is always built empty (`ProducerState::new()`, `producer_state.rs:103`); on crash-restart the fsync'd log has the *records* but not the dedup map, so an idempotent retry would duplicate. Rebuild it broker-side by scanning the recovered tail (keeps the log/broker crate boundary clean).

**Files:**
- Modify: the broker's diskless partition-open path (where the `Log` is opened + `ProducerState` is created — `partition.rs:679` and the partition-registry open site); add a rebuild helper.

- [ ] **Step 1: Write the failing test**

Reopen a `Log` seeded with idempotent batches (`producer_id >= 0`, ascending `base_sequence`); run the rebuild into a fresh `ProducerState`; assert a replay of the last committed batch is `Decision::Duplicate` and the next sequence is `Decision::Append` — keyed to the recovered `log_end_offset`.

```rust
    #[tokio::test]
    async fn producer_dedup_rebuilt_from_recovered_wal() { /* ... */ }
```

- [ ] **Step 2: Run to verify it fails; implement the rebuild**

Run → FAIL. Add a rebuild routine that scans the recovered tail and populates `ProducerState`:

```rust
/// Rebuild idempotent-producer dedup state from a recovered log's records.
/// Scans verbatim batches (offset order) and replays each idempotent batch's
/// (producer_id, epoch, base_sequence, last_offset_delta) through the same
/// `ProducerState::commit` the produce path uses, so `last_sequence` matches.
fn rebuild_producer_state(log: &crabka_log::Log, partition: PartitionIndex, ps: &ProducerState) {
    let start = log.log_start_offset();
    let end = log.log_end_offset();
    let raw = match log.read_raw(start, end, usize::MAX) {
        Ok(r) => r.bytes,
        Err(_) => return,
    };
    let mut cur: &[u8] = &raw;
    while !cur.is_empty() {
        let Ok(batch) = crabka_protocol::records::RecordBatch::decode(&mut cur) else { break };
        if batch.producer_id < 0 { continue; } // -1 sentinel: non-idempotent
        // Mirror the produce-path commit (grep handlers/produce.rs for `.commit(`):
        ps.commit(
            partition,
            crabka_log::ProducerId(batch.producer_id),
            batch.producer_epoch,
            batch.base_sequence,
            batch.last_offset_delta,
            crate::partition::LogOffset(batch.base_offset),
            batch.max_timestamp,
        ).await; // match the actual commit signature/async-ness
    }
}
```

Call it from the diskless partition-open path after `Log::open`, before the partition serves produce. (Confirm `ProducerState::commit`'s exact signature against `handlers/produce.rs`'s post-append call; `commit` computes `last_sequence = base_sequence + last_offset_delta` at `producer_state.rs:148`. Force `validate_on_open = true` for diskless partitions at the open site so the tail is truncated before this scan.)

- [ ] **Step 3: Run to verify + commit**

Run → PASS. `cargo test -p crabka-broker producer_dedup_rebuilt`.

```bash
git add -A
git commit -m "feat(broker): rebuild idempotent-producer dedup from recovered WAL on restart"
```

---

## Task 3: Re-anchor the local append cursor to the KRaft frontier (Seam A)

After restart, `log_end_offset() < KRaft next-offset` (the `[B,B+N)` window). Slice-2's `base == log_end_offset()` guard would fail `OffsetMismatch` on every subsequent produce. Reconcile to the KRaft authority.

**Files:**
- Modify: Slice-2's `append_verbatim_at` (guard); the diskless-open reconciliation.

- [ ] **Step 1: Write the failing test**

Simulate `log_end_offset() = 3` but KRaft `partition_next_offset = 5`; on open, record `reconciled_frontier = 5`; the next `append_verbatim_at(batch, base=5)` succeeds (not `OffsetMismatch`), and a fetch across `[3,5)` sees a benign gap (consumer skips).

- [ ] **Step 2: Run to verify it fails; implement**

Relax Slice-2's guard from `base == log_end_offset()` to:

```rust
        let floor = self.reconciled_frontier.max(self.log_end_offset());
        if base != floor {
            return Err(LogError::OffsetMismatch { expected: floor, actual: base });
        }
```

where `reconciled_frontier` is a durable per-log value set on diskless open from the committed KRaft `partition_next_offset` (read the metadata image). Keep `base >= log_end_offset()` (never mask a real local gap). Document: KRaft is the offset authority; the local log is a suffix cache; restart re-anchors to the authority.

- [ ] **Step 3: Run to verify + commit**

Run → PASS.

```bash
git add -A
git commit -m "feat(broker): re-anchor diskless append cursor to KRaft frontier on restart"
```

---

## Task 4: Tighten trim to committed-index durability (Seam E2)

Slice 4 gated trim on the in-memory `flushed_frontier`. Slice 5 requires the index entry to be **durably committed** to `__diskless_wal_index` before trim removes the local copy — else `[below-floor ∧ cache-miss]` loses acked data on restart.

**Files:**
- Modify: `crates/broker/src/diskless/flusher.rs` (Slice 4 trim gate)

- [ ] **Step 1: Write the failing test**

A flush whose object PUT succeeded but whose `WalFlushRecord` is not yet committed to the index topic must NOT allow trim past its offsets; only after the index publish is committed (read-your-writes) may trim advance.

- [ ] **Step 2: Run to verify it fails; implement**

Change the trim gate so `trim_target = min(committed_index_frontier, hw − lag)`, where `committed_index_frontier` is derived from the **committed** `__diskless_wal_index` projection (publish-and-wait / committed-offset), not the in-memory frontier before the publish commits. Never issue `TrimToOffset` past it.

- [ ] **Step 3: Run to verify + commit**

Run → PASS.

```bash
git add crates/broker/src/diskless/flusher.rs
git commit -m "feat(broker): gate diskless trim on committed-index durability (Seam E2)"
```

---

## Task 5: Partial-durability crash-injection stateright model

A new, tighter diskless-only model proving no `wal_acked` loss across crash-restart.

**Files:**
- Create: `crates/broker/src/diskless_crash_model.rs`; Modify: `crates/broker/src/lib.rs`

- [ ] **Step 1: Build the model (small bounds)**

Mirror the structure of `data_path_model.rs` but drop ISR/replication actions and shrink bounds (small `MAX_LEN`, 1–2 brokers). State ghosts: `wal_appended`, `wal_acked`, `kraft_committed`, `object_put`, `flushed_offset`, `local_floor`, plus a producer-dedup ghost (`last_seq` per PID). Actions (each moves one frontier, so a crash can land between any two): `WalAppend → WalFsync (advances wal_acked) → KraftAssign (before WalFsync, per Slice-2 ordering) → ObjectPut → IndexPublish (advances flushed_offset) → Trim (advances local_floor, gated ≤ flushed_offset)`. Add `Crash(b)` (roll non-durable ghosts back to the last durable frontier; log persists) and `Recover(b)` (re-derive every frontier from durable state only). Add `NodeLoss(b)` (clears the log) as an explicitly-out-of-scope action.

- [ ] **Step 2: Assertions + non-vacuity**

`Property::always("wal_acked_durable", …)` — every offset ever in `wal_acked` is still recoverable in post-recovery durable state (except under `NodeLoss`, where it holds for flushed offsets only). `Property::always("producer_dedup_no_regress", …)` — recovery never lowers a PID's committed `last_seq`. **Mandatory `sometimes` witnesses**: `crash_in_kraft_fsync_gap`, `crash_between_put_and_index`, `crash_mid_fsync`, `trim_at_index_frontier` — so crashes actually land mid-sequence (guards against a vacuous pass).

- [ ] **Step 3: Run the checker**

Run: `cargo test -p crabka-broker diskless_crash_model -- --nocapture`
Expected: PASS — `wal_acked_durable` + `producer_dedup_no_regress` hold across every interleaving; all `sometimes` witnesses reached. A counterexample means a real crash window loses acked data — reconcile with the recovery logic (Tasks 1–4); do NOT weaken the property. Watch state-space bounds; keep `MAX_LEN` small.

- [ ] **Step 4: Commit**

```bash
git add crates/broker/src/diskless_crash_model.rs crates/broker/src/lib.rs
git commit -m "test(broker): diskless partial-durability crash model (no acked loss on restart)"
```

---

## Task 6: Final gate

- [ ] **Step 1:** `cargo +nightly fmt` then `--check` — no diff.
- [ ] **Step 2:** `cargo clippy --workspace --all-targets -- -D warnings` — no warnings.
- [ ] **Step 3:** `cargo nextest run -p crabka-log -p crabka-broker` (or `cargo test`) — PASS, including the crash model.
- [ ] **Step 4:** Commit any formatting.

---

## Self-Review

**1. Spec coverage:** epoch-checkpoint truncation + `validate_on_open` (Task 1/2); producer-sequence rebuild (Task 2); KRaft re-anchor (Task 3); trim ≤ committed-index (Task 4); partial-durability crash model with no-acked-loss + producer-dedup-no-regress + `NodeLoss` out-of-scope (Task 5). Deferred set (node-loss, object snapshots, PID durability, orphan GC, txn/LSO, Creusot) untouched — Scope boundary. ✅

**2. Placeholder scan:** Tasks 1 and 2 are complete code (the rebuild loop, the checkpoint truncation). Tasks 3-4 edit spec-only Slice-2/4 code and give the concrete guard/gate change; Task 5's model gives the exact ghosts/actions/properties to build (grounded, checker-iterative like the Slice-1 model). The two "match the actual commit signature" / "grep produce.rs for `.commit(`" notes point at named existing code, not blanks. No `TBD`/`TODO`.

**3. Type consistency:** `ProducerEntry{epoch, last_sequence, last_offset, base_offset, …}` (producer_state.rs) is populated via `ProducerState::commit(last_sequence = base_sequence + last_offset_delta)` — the rebuild (Task 2) uses the same fields the batch header carries (`producer_id`/`producer_epoch`/`base_sequence`/`last_offset_delta`, verified in owned.rs). `reconciled_frontier` (Task 3) and `committed_index_frontier` (Task 4) are the two durability anchors; the model (Task 5) mirrors them as ghosts. `truncate_from_end(lso)` (Task 1) matches `leader_epoch_checkpoint.rs:104`.

**4. Invariant check:** no-acked-loss proved across crash-restart (Task 5); trim ≤ committed-index (Task 4); re-anchor keeps `base ≥ log_end_offset` (Task 3); dedup keyed to recovered LEO (Task 2); node-loss out of scope (Task 5 `NodeLoss`). Each task green.

**5. Prerequisites flagged:** Slices 1-4 unlanded; Tasks 3-4 edit spec-only code; the model builds on the Slice-1 `wal_acked` ghost — stated in the header.
