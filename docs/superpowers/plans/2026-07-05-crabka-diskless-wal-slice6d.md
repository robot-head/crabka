# Diskless WAL — Slice 6d Implementation Plan (the shipping gate)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Mechanically prove the composed diskless data path never loses an acked record — across concurrent appenders + minority WAL-node loss + sequencer handoff — via a re-composed stateright model, an extended KRaft linearizability model, and a greenfield Jepsen-style black-box harness. **Diskless does not ship until these are green.**

**Architecture:** 6d builds **no new data-path code** — it composes and adversarially verifies 6a–6c. Three verification legs: stateright (exhaustive tiny-model interleavings with partial-durability + `NodeLoss(minority)` in-scope), linearizability (concurrent `AppendVia` appenders, linearize at WAL-quorum-durable), and Jepsen (a real in-process 3-broker cluster under a fault nemesis, with a no-acked-loss ledger checker + a JVM byte-exact differential leg).

**Tech Stack:** Rust 2024 (pinned stable 1.96.0), `stateright`, Creusot (replay), the in-process `Broker::start` harness, `crabka-client-producer`/`consumer`, `assert2`, `cargo +nightly fmt`, `clippy::pedantic`.

**Spec:** [`docs/superpowers/specs/2026-07-05-crabka-diskless-wal-slice6d-design.md`](../specs/2026-07-05-crabka-diskless-wal-slice6d-design.md).

**PREREQUISITES (unlanded):** Slices 1–5 + 6a + 6b + 6c. The model extends the Slice-5 diskless crash model + the 6a quorum-frontier delta; the harness runs a 6a–6c cluster.

---

## Invariants

1. **No acked record ever lost** across every modeled interleaving and every Jepsen fault schedule — under concurrent appenders, minority WAL-node loss, and sequencer handoff.
2. **`NodeLoss(minority)` in-scope; full-quorum loss out of scope** (flushed-only), asserted explicitly.
3. **Non-vacuous proofs** — mandatory `sometimes` witnesses for every composed crash/loss/handoff.
4. **No new data-path code** — 6d is proofs + harness over 6a–6c.
5. **Deterministic faults** — seed the Jepsen schedule (no `Math.random`); tiny stateright bounds.
6. **Every task ends green** before its commit.

## Scope boundary

- **In scope:** the re-composed stateright gate; the linearizability extension; the Jepsen harness; the thin Creusot handoff lemma; the CI shipping-gate wiring.
- **Deferred:** full-quorum-loss durability (out of scope by design); throughput/perf gating.

---

## File Structure

- **The Slice-5 diskless crash model** — extended with the three deltas.
- **`crates/raft/tests/model/mod.rs`** — `AppendVia` per appender; linearization at WAL-quorum-durable.
- **`crates/integration-tests/tests/diskless_jepsen.rs`** (new) — the black-box harness.
- **`crates/verified/src/consensus.rs`** — the handoff-monotonicity lemma.
- **CI config** — the three legs as shipping-gate checks.

---

## Task 1: Re-compose the stateright gate (three deltas)

**Files:**
- Modify: the Slice-5 diskless crash model

- [ ] **Step 1: Add the deltas + assertions**

Extend the Slice-5 model: **(1)** WAL frontier = majority presence across N nodes (reuse `recompute_high_watermark`); `NodeLoss(b)` bounded to a **minority** is in-scope. **(2)** N concurrent appenders — `WalAppend → KraftAssign(concurrent) → WalFsync(quorum)`; `committed`/`wal_acked` advance by commit-order merge of concurrent ranges. **(3)** `SequencerHandoff` (may regress advertised HWM, never `wal_acked`). Carry three always-properties green: `committed_durable`, `wal_acked_durable` (under in-quorum `NodeLoss`), `offset_gap_free/unique`. Add `sometimes` witnesses: `nodeloss_minority_survives_unflushed`, `two_appenders_race_gap_free`, `handoff_no_wal_acked_regress`, `crash_between_put_and_index`. Keep bounds tiny (2 appenders, 2 appends, `max_epoch=2`, minimal WAL nodes).

- [ ] **Step 2: Run the checker**

Run: `cargo test -p crabka-broker diskless_crash_model -- --nocapture`
Expected: PASS — all three always-properties hold; all `sometimes` witnesses reached. A counterexample = a real composed loss window; reconcile with 6a–6c, do NOT weaken. Watch the state space (tiny bounds).

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "test(broker): re-composed diskless no-acked-loss gate (quorum + concurrency + handoff)"
```

---

## Task 2: Extend the KRaft linearizability model

**Files:**
- Modify: `crates/raft/tests/model/mod.rs`

- [ ] **Step 1: Failing test → implement**

Replace the single-leader-gated `ClientAppend` (`:172`, emits only when `leaders.len()==1` at `:566-576`) with `AppendVia(appender_id, value)` per live stateless appender (fresh `ClientId` each). Move the linearization point in `settle_committed` (`:467`) from HWM-passes-offset to **WAL-quorum-durable** (`KraftLogSpec::invoke` unchanged). Assert `linearizable` (`:682`) + `log_matching` (`:699`) + `gap_free`/`unique` hold with concurrent appenders.

- [ ] **Step 2: Run + commit**

Run: `cargo test -p crabka-raft --test kraft_model two_voters_append_via_linearizable -- --nocapture` → PASS (complete tiny config: 2 voters, 2 exchangeable appenders, 2 appends, 230,591 unique / 938,679 generated / depth 32).

```bash
git add crates/raft/tests/model/mod.rs
git commit -m "test(raft): linearizability model for concurrent stateless WAL appenders"
```

---

## Task 3: The Jepsen black-box harness

**Files:**
- Create: `crates/integration-tests/tests/diskless_jepsen.rs`

- [ ] **Step 1: Substrate + generator (failing test)**

Stand up a 3× `Broker::start(BrokerConfig::for_tests)` + quorum-WAL diskless cluster (extend the `BrokerHandle`/`listen_addr()` pattern from `producer_integration.rs`). A real `crabka-client-producer` produces `acks=all` and records an **acked-record ledger**; a `crabka-client-consumer` reads back.

- [ ] **Step 2: Nemesis (implement)**

Fault injectors matching the model actions, on a **seeded** schedule (no `Math.random`): kill-accepting-broker, kill-a-WAL-quorum-node-*within-quorum* (leave f+1 alive), force-a-PUT-failure (inject into the object store), trigger-a-KRaft-leader-change. In-process "kill" = drop the `BrokerHandle` (pattern from `durability.rs`/`leader_election.rs`).

- [ ] **Step 3: Checker (implement)**

After the fault schedule: use public `crabka-client-core` direct partition Fetch (not a classic consumer group whose coordinator may have been killed) to assert **every acked offset in the ledger is still consumable**; feed the acknowledged invocation/return history into `LinearizabilityTester`/`KafkaLogSpec`; run the Dockerized JVM console consumer against that same partition for a byte-exact comparison.

- [ ] **Step 4: Run + commit**

The live gate requires `ulimit -n 65536`; a 1024-FD soft limit can fail the three-broker runtime with `EMFILE` before the nemesis fires.

Run:

```bash
ulimit -n 65536
CARGO_INCREMENTAL=0 cargo test -p crabka-integration-tests \
  --test diskless_jepsen \
  three_broker_fault_schedule_preserves_the_acked_ledger \
  -- --ignored --nocapture
```

Observed PASS witness (2026-08-12): 8 `acks=all` records; 2 object PUT errors; exact WAL shard erased on node 3; controller 3→2; partition leader 3→1; Rust ledger exact; PUT retry materialized a WAL object; JVM bytes exact.

```bash
git add crates/integration-tests/tests/diskless_jepsen.rs
git commit -m "test(integration): diskless Jepsen harness (no-acked-loss under real faults)"
```

---

## Task 4: The handoff-monotonicity Creusot lemma

**Files:**
- Modify: `crates/verified/src/consensus.rs`

- [ ] **Step 1: Add + prove**

Add a thin lemma over the existing `recompute_high_watermark` contract (`#[ensures(result@ >= current_hwm@)]`, `consensus.rs`): the WAL-durability watermark **never regresses across a sequencer handoff** (the new authority's recomputed frontier is `>=` the committed one). Pure, small — no new kernel; reuse the 6c `assign_ranges`/`is_gap_free_partition` kernel unchanged.

- [ ] **Step 2: Prove + commit**

Run: `cargo creusot` (proof replay). Add to the CI proof set.

```bash
git add crates/verified/src/consensus.rs
git commit -m "feat(verified): handoff-monotonicity lemma for the WAL-durability watermark"
```

---

## Task 5: Shipping-gate CI wiring + final gate

**Files:**
- Modify: CI config (the three legs as required checks).

- [x] **Step 1:** Wire the three legs as **required** CI checks: the re-composed stateright model, the Creusot replay (incl. the 6c kernel + the handoff lemma), and the diskless Jepsen harness. The named live gate raises `nofile` to 65,536 before nextest. Document "diskless does not ship until these are green."
- [ ] **Step 2:** `cargo +nightly fmt --check` — no diff.
- [ ] **Step 3:** `cargo clippy --workspace --all-targets -- -D warnings` — no warnings.
- [ ] **Step 4:** `cargo nextest run -p crabka-broker -p crabka-raft -p crabka-integration-tests` + `cargo creusot` — PASS across all three legs.
- [ ] **Step 5:** Commit.

```bash
git add -A
git commit -m "ci: wire the diskless no-acked-loss shipping gate (stateright + Creusot + Jepsen)"
```

---

## Self-Review

**1. Spec coverage:** re-composed stateright gate with three deltas + `NodeLoss(minority)` in-scope + `sometimes` witnesses (Task 1); linearizability with `AppendVia` at WAL-quorum-durable (Task 2); the Jepsen harness (substrate/generator/nemesis/checker + JVM differential) (Task 3); the handoff-monotonicity lemma (Task 4); the shipping-gate CI wiring (Task 5). Deferred (full-quorum loss, throughput) untouched — Scope boundary. ✅

**2. Placeholder scan:** Tasks name the exact sites to extend (`raft/tests/model/mod.rs:172/:467/:566-576/:682/:699`, `consensus.rs` `recompute_high_watermark`, `producer_integration.rs`/`durability.rs`/`leader_election.rs` patterns) and the exact composed action set. No `TBD`/`TODO`. (Jepsen tasks are harness assembly from named in-tree parts — appropriate for a black-box test slice.)

**3. Type consistency:** the three deltas reuse the Slice-5 ghosts + `recompute_high_watermark` (Task 1); `AppendVia`/`KraftLogSpec` extend the existing `LinearizabilityTester` (Task 2); the Jepsen checker feeds the same `LinearizabilityTester`/`KraftLogSpec` + the `jvm_*` differential oracle (Task 3); the handoff lemma is over `recompute_high_watermark`'s existing contract (Task 4).

**4. Invariant check:** no-acked-loss across every interleaving/fault (Tasks 1,3); `NodeLoss(minority)` in-scope, full-quorum out (Task 1); non-vacuous via `sometimes` witnesses (Task 1); no new data-path code (all tasks are proofs/harness); seeded faults (Task 3). Each task green; Task 5 makes them the gate.

**5. Prerequisites flagged:** Slices 1-5 + 6a + 6b + 6c unlanded; 6d composes them — stated in the header.
