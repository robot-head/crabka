# Diskless WAL — Slice 6a Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace single-node `LocalFsyncWal` with a `QuorumWalStore` behind the Slice-1 `WalStore` seam: a per-partition 2f+1 AZ-placed quorum WAL, `fsync`-before-ack, committed on f+1 fsync-acks, driven by the reused sans-IO `QuorumStateMachine` core, advancing the WAL-durable watermark via the verified `recompute_high_watermark`.

**Architecture:** Each diskless partition gets a WAL group: `QuorumStateMachine` (kraft-core, one per group) + a durable per-replica WAL log (reusing `LocalFsyncWal`/`Log`) implementing `LogView`. A new, leaner **per-shard async engine** drives the core (`on_event → Vec<Action>`, mirroring the deterministic `sim.rs` driver) and executes its actions (send wire, persist quorum state, arm timers, advance HWM). `QuorumWalStore::append_durable` replicates the verbatim batch to the group, returns once f+1 have fsync-acked, and advances the per-partition durable watermark that drives Slice-1's `recompute_hw_for_wal_durable`. Everything above the `WalStore` seam is untouched.

**Tech Stack:** Rust 2024 (pinned stable 1.96.0), `crabka-kraft-core` (sans-IO consensus), `crabka-voters`, `crabka-verified` (`recompute_high_watermark`), `tokio`, `stateright` (dev), `assert2`, `cargo +nightly fmt`, `clippy::pedantic` (`unsafe_code = "forbid"`).

**Spec:** [`docs/superpowers/specs/2026-07-05-crabka-diskless-wal-slice6a-design.md`](../specs/2026-07-05-crabka-diskless-wal-slice6a-design.md).

**PREREQUISITES (unlanded):** Slices 1–5. `QuorumWalStore` implements the Slice-1 `WalStore` trait and re-sources the Slice-1 WAL-durable HW. **This is the single largest build in the milestone** — the tasks below are components; land them in order.

---

## Invariants

1. **Acked ⇒ fsync'd on f+1 AZ-distributed disks.** Never ack before quorum-durable; survive f node/AZ losses and full-quorum power loss.
2. **The seam is unchanged.** `QuorumWalStore` implements `WalStore::append_durable`; `finalize_ack`/`await_hw_at_least`/offset assignment/fetch/flush are untouched — only the WalStore impl and the HW *source* change.
3. **Reuse the core, not the metadata engine.** Instantiate `QuorumStateMachine` per group; the watermark is the majority-th-largest via the verified `recompute_high_watermark` — do not re-derive it.
4. **The WAL retains only the un-flushed tail.** Per-replica WAL segments are trimmed on flush (Slice 3's `flushed_offset`), so replication is bounded by the flush window, not permanent N× storage.
5. **Single-writer preserved.** 6a changes durability only; leaderless writes (6b), the concurrent sequencer (6c), and the full gate + Jepsen (6d) are out.
6. **Every task ends green** before its commit.

## Scope boundary

- **In scope:** the per-partition quorum WAL group (core + per-replica durable log + engine), the `ShardId→engine` registry, shard-addressed routing, durable persist/reload, `QuorumWalStore`, AZ placement, and a first quorum-frontier proof delta.
- **Deferred:** leaderless serving (6b); concurrent sequencer (6c); re-composed gate + Jepsen (6d); shard-consolidation (many partitions → few groups); the RAM-quorum relaxed tier.

---

## File Structure

- **`crates/broker/src/wal/quorum/`** (new) — `mod.rs` (`QuorumWalStore`), `engine.rs` (per-shard async engine), `registry.rs` (`ShardId→engine`), `log_view.rs` (the shard log's `LogView`).
- **`crates/broker/src/wal/quorum/wire.rs`** — shard-addressed WAL RPC (populate the KIP-595 codec `topics[]`/`partitions[]` + a group discriminator).
- **`crates/kraft-core/src/core.rs`** — (if needed) a `from_durable`/reload constructor helper (else reuse `QuorumStateMachine::new(persisted_state)`).
- Reuse: `LocalFsyncWal` (Slice 1) per replica; `replica_selector.rs` (AZ); `crabka_verified::recompute_high_watermark`.

---

## Task 1: The shard WAL log + `LogView`

Each WAL group's durable log is a `Log` (reused) whose offset/epoch metadata the core reads through `LogView` (`crates/kraft-core/src/types.rs:40-48`).

**Files:**
- Create: `crates/broker/src/wal/quorum/log_view.rs`; `crates/broker/src/wal/quorum/mod.rs`

- [ ] **Step 1: Write the failing test**

A `ShardLog` wrapping a `Log` returns `end_offset`/`last_epoch`/`end_offset_for_epoch` consistent with appended batches.

```rust
    #[test]
    fn shard_log_view_reports_offset_and_epoch() {
        // append 3 records at epoch 2; assert end_offset==3, last_epoch==2,
        // end_offset_for_epoch(2) == Some(3), end_offset_for_epoch(1) == None/…
    }
```

- [ ] **Step 2: Run to verify it fails; implement**

Implement `ShardLog` (wraps the per-replica `Log` + `LocalFsyncWal` durable step) and `impl LogView for ShardLog` delegating to `Log::log_end_offset()` and `Log::epoch_checkpoint()` (mirroring how `KraftLog` implements `LogView` at `crates/raft/src/kraft/log.rs:136-168`, and the sim's `LogView` impl at `kraft-core/src/sim.rs:143-153`).

- [ ] **Step 3: Run to verify + commit**

Run: `cargo test -p crabka-broker shard_log_view` → PASS.

```bash
git add crates/broker/src/wal/quorum/
git commit -m "feat(broker): shard WAL log + LogView over the reused Log"
```

---

## Task 2: The per-shard async WAL engine (drives the core)

A leaner analog of `crates/raft/src/kraft/controller.rs`, but its "apply" is a durable-watermark advance (not a `MetadataImage` reduction). It drives `QuorumStateMachine::on_event(event, &shard_log, now) -> Vec<Action>` and executes the actions.

**Files:**
- Create: `crates/broker/src/wal/quorum/engine.rs`

- [ ] **Step 1: Write the failing test (3-replica in-process quorum)**

Stand up three `WalShardEngine`s (voter set {0,1,2}) wired by an in-process transport; append a batch on the leader; assert it commits (f+1=2 replicas fsync-ack) and the leader emits `AdvanceHighWatermark(3)` → the shard's durable watermark reaches 3; kill one replica (f=1) → a further append still commits; kill two → it does not.

```rust
    #[tokio::test]
    async fn quorum_commits_on_f_plus_1_and_survives_one_loss() { /* ... */ }
```

(Model the harness on `sim.rs`'s multi-node driver — `Cluster::run_until_stable`, `leader_append` at `sim.rs:425,469` — but async with a real fsync per replica.)

- [ ] **Step 2: Run to verify it fails; implement the engine**

Insert `crates/broker/src/wal/quorum/engine.rs`. The engine owns one `QuorumStateMachine` (`QuorumStateMachine::new(me, QuorumState{leader_epoch, voters}, election_timeout_ms)`) + a `ShardLog`, and a `tokio::select!` loop over {inbound wire events, timers, local append requests}. For each input, build an `Event` (`crates/kraft-core/src/event.rs`: `ReceiveVoteRequest`/`ReceiveFetch`/`FetchTimeout`/…), call `sm.on_event(event, &shard_log, now)`, and execute each returned `Action` (`crates/kraft-core/src/action.rs:12-47`):

```rust
    for action in sm.on_event(event, &self.shard_log, now) {
        match action {
            Action::SendVoteRequest { epoch, pre_vote } => self.wire.broadcast_vote(epoch, pre_vote).await,
            Action::ReplyVote { to, epoch, granted } => self.wire.reply_vote(to, epoch, granted).await,
            Action::SendBeginQuorumEpoch { epoch } => self.wire.begin_quorum(epoch).await,
            Action::SendFetch { leader_id } => self.wire.fetch_from(leader_id).await,
            Action::AppendLeaderChange { epoch } => self.shard_log.append_leader_change(epoch),
            Action::AdvanceHighWatermark(hw) => self.on_watermark_advance(hw), // <-- the durable watermark
            Action::TruncateTo(m) => self.shard_log.truncate_to(m),
            Action::PersistQuorumState => self.persist_quorum_state(sm.quorum_state()), // Task 4
            Action::ResetTimer { kind, deadline } => self.arm_timer(kind, deadline),
            Action::TransitionedTo(_) | Action::SendEndQuorumEpoch { .. } => { /* observe / forward */ }
        }
    }
```

The leader's watermark is computed by the core (via `crabka_verified::recompute_high_watermark`, `verified/src/consensus.rs:18`) from the followers' fetched offsets — the engine just persists the batch (fsync) and replies to Fetch with the tail bytes (a lean version of `serve_fetch_records`, but the followers only need durability, so replicate the verbatim WAL bytes). `on_watermark_advance(hw)` moves the per-partition WAL-durable watermark that `QuorumWalStore` reports.

- [ ] **Step 3: Run to verify + commit**

Run → PASS.

```bash
git add crates/broker/src/wal/quorum/engine.rs
git commit -m "feat(broker): per-shard WAL engine driving the sans-IO QuorumStateMachine"
```

---

## Task 3: `ShardId→engine` registry + shard-addressed wire routing

**Files:**
- Create: `crates/broker/src/wal/quorum/registry.rs`, `crates/broker/src/wal/quorum/wire.rs`

- [ ] **Step 1: Registry (failing test → implement)**

A `WalShardRegistry` (`DashMap<ShardId, Arc<WalShardEngine>>`) with `get_or_create(shard_id, voters)` and lookup. Test: two partitions map to their own groups; an inbound message routes to the right engine. `ShardId` = the partition (per-partition groups this slice); a `topic_id_partition → ShardId` map (identity for now; shard-consolidation later).

- [ ] **Step 2: Shard-addressed routing (failing test → implement)**

The KIP-595 codecs already carry `topics[]`/`partitions[]` (`crates/raft/src/transport.rs:452-456`), so populate those fields with the shard's `(topic_id, partition)` instead of the pinned `__cluster_metadata`/0 (`transport.rs:229-260`), and add a **group discriminator** to inbound dispatch (`server.rs:342-368` routes by `api_key` only today) so a WAL Fetch/Vote reaches the right registry engine. Test: an inbound WAL RPC for shard B is dispatched to engine B, not the metadata engine or engine A.

- [ ] **Step 3: Commit**

```bash
git add crates/broker/src/wal/quorum/registry.rs crates/broker/src/wal/quorum/wire.rs
git commit -m "feat(broker): WAL shard registry + shard-addressed KIP-595 routing"
```

---

## Task 4: Durable persist + reload on restart

**Files:**
- Modify: `crates/broker/src/wal/quorum/engine.rs`; (if needed) `crates/kraft-core/src/core.rs`

- [ ] **Step 1: Write the failing test**

A replica persists its `QuorumState` on every `Action::PersistQuorumState`; on restart it reloads that state + its durable WAL segment and rejoins the group without losing or double-counting any committed offset.

```rust
    #[tokio::test]
    async fn replica_reloads_durable_state_and_rejoins() { /* ... */ }
```

- [ ] **Step 2: Run to verify it fails; implement**

`persist_quorum_state(&QuorumState)` writes epoch/votedKey/leaderId durably (fsync). On engine open, load the persisted `QuorumState` and construct `QuorumStateMachine::new(me, loaded_state, election_timeout_ms)` — `new` already takes the state (`core.rs:40`), so reload is `new(persisted)`; the shard log is reopened via `Log::open` (Slice-5 recovery). Add a `from_durable` convenience on the engine (not the core). *If the crash model (Task 6) needs to reset a live machine, add a `QuorumStateMachine::reload(&mut self, state)` helper — flagged absent in grounding.*

- [ ] **Step 3: Commit**

Run → PASS.

```bash
git add -A
git commit -m "feat(broker): durable persist + reload of WAL-shard consensus state"
```

---

## Task 5: `QuorumWalStore` — the `WalStore` impl (composes everything)

**Files:**
- Create/Modify: `crates/broker/src/wal/quorum/mod.rs`

- [ ] **Step 1: Write the failing test**

A `QuorumWalStore` over a 3-replica group: `append_durable(batch)` returns only after f+1 replicas fsync; the returned durable watermark reaches the batch's last offset; a diskless `acks=all` produce driven through the Slice-1 writer + this store acks only post-quorum-commit; killing one replica still acks; killing two does not (availability loss, no silent acked-loss).

- [ ] **Step 2: Run to verify it fails; implement**

`impl WalStore for QuorumWalStore` (the Slice-1 trait): `append_durable(&self, batch)` routes to the partition's `WalShardEngine` (via the registry), submits the verbatim v2 batch as a local append request, and awaits the durable watermark reaching `base + last_offset_delta + 1`. Each replica's local append is the **Slice-1 `LocalFsyncWal` step** (`Log::append_verbatim` + `Segment::flush→sync_data`). Commit (f+1 fsync-acks) is signaled by the engine's `on_watermark_advance` (Task 2), which `QuorumWalStore` awaits (a `watch`/`Notify` on the per-partition durable watermark). Place the 2f+1 replicas across AZs via `replica_selector.rs` (`RackAware`) at group creation. Wire `QuorumWalStore` as the diskless partition's `WalStore` (replacing `LocalFsyncWal`) at the Slice-1 construction site.

- [ ] **Step 3: Run to verify + commit**

Run → PASS. Also run the Slice 1–4 diskless suites with `QuorumWalStore` swapped in — they stay green (the seam is unchanged).

```bash
git add crates/broker/src/wal/quorum/mod.rs
git commit -m "feat(broker): QuorumWalStore (fsync-quorum WalStore behind the Slice-1 seam)"
```

---

## Task 6: First proof delta — the quorum frontier

**Files:**
- Modify: the Slice-5 diskless crash model (`crates/broker/src/diskless_crash_model.rs`)

- [ ] **Step 1: Extend the model**

Replace the single-node `WalFsync` frontier with a quorum frontier: model per-WAL-node presence (mirror `DpState.log: [Vec<u8>; NB]`, `data_path_model.rs:58-68`); `wal_acked` advances to `o` only once `o` is present on `≥ ceil((N+1)/2)` nodes (the majority-th-largest — the shape `recompute_high_watermark` verifies). Make `NodeLoss(b)` **in-scope when bounded to a minority** of WAL nodes; assert `wal_acked_durable` holds under it (a surviving majority retains every acked offset; `Recover` re-derives it). Add a `sometimes("acked_unflushed_survives_minority_wal_loss", …)` witness — the state that flips Slice-5's out-of-scope note. Keep `NodeLoss` of a full quorum still out of scope (asserted for flushed offsets only).

- [ ] **Step 2: Run the checker**

Run: `cargo test -p crabka-broker diskless_crash_model -- --nocapture`
Expected: PASS — `wal_acked_durable` holds under minority WAL-node loss; the witness is reached. Keep `MAX_LEN`/broker count tiny (state explosion). The full re-composition (concurrent appenders + leader change + Jepsen) is **6d** — not here.

- [ ] **Step 3: Commit**

```bash
git add crates/broker/src/diskless_crash_model.rs
git commit -m "test(broker): quorum-frontier proof delta (minority WAL-node loss survives)"
```

---

## Task 7: Final gate

- [ ] **Step 1:** `cargo +nightly fmt` then `--check` — no diff.
- [ ] **Step 2:** `cargo clippy --workspace --all-targets -- -D warnings` — no warnings.
- [ ] **Step 3:** `cargo nextest run -p crabka-kraft-core -p crabka-broker` (or `cargo test`) — PASS, including the 3-replica quorum tests + the model.
- [ ] **Step 4:** Commit any formatting.

---

## Self-Review

**1. Spec coverage:** shard log + `LogView` (Task 1); per-shard engine driving the reused core (Task 2); registry + shard-addressed routing (Task 3); durable persist/reload (Task 4); `QuorumWalStore` behind the seam with f+1-fsync-commit + AZ placement (Task 5); quorum-frontier proof delta with minority `NodeLoss` in-scope (Task 6). Deferred set (leaderless 6b, sequencer 6c, gate+Jepsen 6d, shard-consolidation, RAM tier) untouched — Scope boundary. ✅

**2. Placeholder scan:** Tasks 1, 4, 5 are close to complete code (LogView delegation, reload-via-`new`, the `WalStore` impl + await-watermark). Tasks 2 and 3 are the large new components — they give the exact core contract (`on_event → Vec<Action>` with every `Action` arm mapped) and the exact templates to mirror (`sim.rs` driver, `KraftLog` LogView, `transport.rs`/`server.rs` routing). This is a slice-scale build; the plan specifies structure + the verified API surface, not thousands of lines of speculative engine code. No `TBD`/`TODO`.

**3. Type consistency:** `QuorumStateMachine::new(me, QuorumState{leader_epoch, voters}, election_timeout_ms)` + `on_event(Event, &dyn LogView, SimInstant) -> Vec<Action>` (kraft-core, verified) are used identically in Task 2; the `Action` arms match `action.rs:12-47`; `LogView`'s three methods (Task 1) match `types.rs:40-48`; `recompute_high_watermark` (Task 2/6) is called, not re-derived; `QuorumWalStore` implements the Slice-1 `WalStore::append_durable` (Task 5).

**4. Invariant check:** ack only on f+1 fsync (Task 5); seam unchanged (Task 5 — swap behind `WalStore`); reuse the core + verified HW (Tasks 2/6); WAL trimmed on flush so replication is bounded (Slice-3 `flushed_offset`, noted); single-writer preserved (no produce-gate change — that's 6b); minority-loss no-acked-loss proved (Task 6). Each task green.

**5. Prerequisites flagged:** Slices 1-5 unlanded; `QuorumWalStore` implements the spec-only Slice-1 `WalStore`; the largest build in the milestone — stated in the header.
