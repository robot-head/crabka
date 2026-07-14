# G8 Move Retirement-Phase SIGKILL Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a real-process Move retirement nemesis covering exact before-delete, after-delete, parked, and resuming crash windows with replay-safe deletion and end-to-end ACK safety evidence.

**Architecture:** Extend the existing topology process fixture with retirement kill points and exact predicates over the operation journal, tenant sidecar, Kafka metadata, and durable retire receipts. A test-only `AdminClientLike` wrapper delegates to real Kafka, counts and constrains deletion, and injects a one-shot post-delete error so the production retirement helper exposes the ambiguous AfterDelete window without production fault hooks.

**Tech Stack:** Rust, Tokio, `crabka_client_admin::AdminClientLike`, real Kafka process harness, JSON evidence, Bash/Python CI validation.

## Global Constraints

- Kill only the real Gres child after the exact phase predicate is true.
- Reconstruct registry, mutation, and admin clients after restart.
- AfterDelete performs one real exact predecessor deletion, returns a deterministic error before sidecar CAS, and never deletes again on replay.
- Preserve coordinator, successor, and unrelated sentinel topics; never recreate the predecessor.
- Derive timeout bounds from repeated live evidence before finalizing CI limits.
- Commit locally and obtain independent review; perform no remote Git operations.

---

### Task 1: Exact Retirement Predicates

**Files:**
- Modify: `crates/gres/tests/topology_process_nemesis.rs`

**Interfaces:**
- Consumes: `SplitOperationRecord`, `TenantRecord`, `RangeRetirementPhase`, and Kafka topic presence.
- Produces: four retirement `SourceKillPoint` variants and an exact predicate over journal, tenant, and topic state.

- [ ] **Step 1: Write failing predicate tests**

Add table-driven tests for the exact required state and near-misses: wrong phase, layout, version, sidecar phase, topic state, evidence, or Resuming retire receipt.

- [ ] **Step 2: Run tests and verify RED**

```bash
cargo test -p crabka-gres --test topology_process_nemesis retirement_kill_predicate -- --nocapture
```

Expected: compile failure because the retirement variants and predicate input are absent.

- [ ] **Step 3: Implement the minimal predicate model**

Add `RetirementPredicateState { predecessor_topic_present: bool, retire_receipt_durable: bool }`. Require exact target layout and target-era version for every phase, then respectively require `Parking/present`, `Parking/absent`, `Parked/absent`, and `Parked/absent + Resuming receipt`.

- [ ] **Step 4: Run the Step 2 command and verify GREEN**

Expected: all predicate cases pass.

- [ ] **Step 5: Commit**

```bash
git add crates/gres/tests/topology_process_nemesis.rs
git commit -m "test(gres): define move retirement kill predicates"
```

### Task 2: Counting Real-Admin Ambiguity Seam

**Files:**
- Modify: `crates/gres/tests/topology_process_nemesis.rs`

**Interfaces:**
- Consumes: `AdminClientLike`, exact predecessor topic, and shared delete-ledger state.
- Produces: a counting wrapper with a one-shot real-delete-then-error mode.

- [ ] **Step 1: Write failing wrapper tests**

Use a deterministic fake delegate to prove the exact topic is deleted once, unrelated deletion fails and is recorded, the injected error occurs after delegate deletion, and a fresh wrapper sharing counters does not increment them during metadata-only replay.

- [ ] **Step 2: Run tests and verify RED**

```bash
cargo test -p crabka-gres --test topology_process_nemesis counting_retirement_admin -- --nocapture
```

Expected: compile failure because the wrapper and ledger do not exist.

- [ ] **Step 3: Implement the wrapper minimally**

Delegate every admin method except `delete_topics`. Reject any request other than the exact predecessor topic, record the call, await the delegate, then return one deterministic admin error after a successful deletion when armed.

- [ ] **Step 4: Run the Step 2 command and verify GREEN**

Expected: counting, ordering, fresh-wrapper, and unrelated-topic cases pass.

- [ ] **Step 5: Commit**

```bash
git add crates/gres/tests/topology_process_nemesis.rs
git commit -m "test(gres): add retirement delete ambiguity seam"
```

### Task 3: Real-Process Driver and Evidence

**Files:**
- Modify: `crates/gres/tests/topology_process_nemesis.rs`
- Create: `docs/superpowers/evidence/2026-07-12-gres-g8-retirement-kill.md`

**Interfaces:**
- Consumes: exact predicates and counting wrapper.
- Produces: four executable retirement cases and complete JSON evidence.

- [ ] **Step 1: Add BeforeDelete with an impossible gap bound and observe RED**

```bash
CRABKA_G8_PROCESS_NEMESIS=1 CRABKA_G8_RETIREMENT_KILL_POINT=retiring_before_delete CRABKA_G8_KILL_EVIDENCE="$PWD/target/g8-topology-process-nemesis/retirement-before-delete.json" timeout 180s cargo test --locked -p crabka-gres --test topology_process_nemesis -- --exact real_process_move_source_phase_sigkill_with_exact_ack_ledger --nocapture
```

Expected: the case reaches and recovers from the window, then fails only at the intentionally impossible bound.

- [ ] **Step 2: Implement all four stepwise windows**

Drive `LayoutPublished -> Retiring`; pause before WAL reconciliation, after faulted real deletion, after normal Parking-to-Parked reconciliation, or after retire RPC advances to Resuming. At every boundary reload operation, tenant, and metadata before killing. Rebuild all clients and the wrapper delegate after restart while sharing only the delete ledger.

- [ ] **Step 3: Add exact final assertions**

Require ACK ledger equals database rows, recovered ACKs, ACKs across restart and Completed, exact r2/g1 owner, no r1 owner/topic/service, preserved r0/r2/sentinel topics, marker equality, Parked sidecar, Completed journal, exact delete count, zero unrelated deletes, and no orphan retirement.

- [ ] **Step 4: Collect repeated evidence and set bounds**

Run every phase at least three times, record operation duration and maximum ACK gap, and select bounds above observed maxima with a documented scheduling margin.

- [ ] **Step 5: Run all four phase commands separately**

Expected: each exits zero and writes complete evidence JSON.

- [ ] **Step 6: Commit**

```bash
git add crates/gres/tests/topology_process_nemesis.rs docs/superpowers/evidence/2026-07-12-gres-g8-retirement-kill.md
git commit -m "test(gres): cover move retirement phase recovery"
```

### Task 4: CI Validator and Review

**Files:**
- Create: `scripts/tests/gres-topology-process-retirement-nemesis-ci.sh`
- Modify: `docs/superpowers/evidence/2026-07-12-gres-g8-retirement-kill.md`

**Interfaces:**
- Consumes: retirement evidence JSON and measured bounds.
- Produces: one CI entry point running four isolated processes and exact validation.

- [ ] **Step 1: Write the validator before final evidence generation**

Require exact phase/layout/version/sidecar/topic state, receipt and marker evidence, distinct PIDs, recovered and cross-boundary ACKs, exact owner, predecessor absence, preserved topic set, single-delete semantics, zero unrelated deletes, cleanup, and phase-specific bounds.

- [ ] **Step 2: Verify validator RED on incomplete evidence**

Remove `unrelated_delete_attempts` from a copied JSON record and run the validator. Expected: nonzero assertion/key failure.

- [ ] **Step 3: Run the clean CI shard**

```bash
scripts/tests/gres-topology-process-retirement-nemesis-ci.sh
```

Expected: four process tests and the validator pass.

- [ ] **Step 4: Verify repository state**

```bash
cargo fmt --all -- --check
bash -n scripts/tests/gres-topology-process-retirement-nemesis-ci.sh
git diff --check
```

Expected: every command exits zero.

- [ ] **Step 5: Commit locally**

```bash
git add scripts/tests/gres-topology-process-retirement-nemesis-ci.sh docs/superpowers/evidence/2026-07-12-gres-g8-retirement-kill.md
git commit -m "ci(gres): validate move retirement recovery"
```

- [ ] **Step 6: Independent review**

Review exact predicates, real-delete ordering, shared-counter replay, topics, receipt/marker checks, ACK equality, cleanup, and bounds. Address findings with focused tests and a local follow-up commit. Do not fetch, rebase, push, or otherwise access Git remotes.
