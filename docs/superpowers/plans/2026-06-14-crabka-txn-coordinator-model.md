# Transaction-Coordinator (KIP-98/EOS) Safety Model — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an exhaustive `stateright` model of the transaction coordinator's per-`TransactionalId` state machine that proves a producer fenced during EndTxn's marker window can never complete, and no tid is ever both committed and aborted — by driving the real decision logic.

**Architecture:** Extract the EndTxn decision core into a pure `txn/decision.rs` (Phase-1 transition + Phase-3 completion, the latter wrapping the already-pure `validate_complete_reacquire`/`next_producer_identity`). A `#[cfg(test)]` descendant module drives those real functions, modeling the EndTxn Phase1→window→Phase3 split so concurrent `InitProducerId`/`AddPartitions` interleave in the window.

**Tech Stack:** Rust, `stateright` 0.31 (dev-dep, on main), the real `crabka-broker` txn coordinator (`crates/broker/src/txn/`).

**Spec:** `docs/superpowers/specs/2026-06-14-crabka-txn-coordinator-model-design.md`

**Verification discipline (MANDATORY):** every checker run is fenced with `within_boundary` + `target_state_count` + `timeout` and run under the host memory watchdog (kill >3 GB / >150 s) — see `[[feedback_bound_model_checkers]]`. CI fmt gate is **nightly**; clippy gate is `-D warnings`; timeouts use `Duration::from_mins`.

---

## File Structure

- `crates/broker/src/txn/decision.rs` — **create**: pure decision core (`decide_phase1_transition`, `decide_end_txn_completion`, the relocated `validate_complete_reacquire` / `next_producer_identity` / `ReacquireDecision`, and `CompletionDecision`). Owns the unit tests for those fns (relocated from `end_txn.rs`).
- `crates/broker/src/txn/handlers/end_txn.rs` — **modify**: Phase-1 and Phase-3 inline logic now calls `decision.rs`. Behavior-preserving.
- `crates/broker/src/txn/mod.rs` — **modify**: add `mod decision;`.
- `crates/broker/src/txn/decision_model.rs` — **create**: the stateright model, wired as a `#[cfg(test)] #[path = "decision_model.rs"] mod decision_model;` descendant of `decision.rs`.

---

## Task TCM-T1: Extract the EndTxn decision core + wire the model module

**Files:** create `txn/decision.rs`; modify `txn/handlers/end_txn.rs`, `txn/mod.rs`.

Behavior-preserving extraction. The existing `end_txn` tests are the regression gate.

- [ ] **Step 1: Add `mod decision;` to `txn/mod.rs`**

Add the line near the other `mod` declarations in `crates/broker/src/txn/mod.rs`:

```rust
mod decision;
```

- [ ] **Step 2: Create `txn/decision.rs` with the pure core**

Move `next_producer_identity`, `ReacquireDecision`, and `validate_complete_reacquire` out of `end_txn.rs` (verbatim bodies) into this new file as `pub(crate)`, and add the two new decision fns + the `CompletionDecision` enum:

```rust
//! Pure decision core of the transaction coordinator's EndTxn path, extracted
//! so the KIP-98/EOS state machine is independently model-checkable. No I/O.
//! See `decision_model.rs` and the design spec
//! `docs/superpowers/specs/2026-06-14-crabka-txn-coordinator-model-design.md`.

use crabka_protocol::codes;

use super::state::{TxnEntry, TxnState};
use super::version::TxnVersion;
use crate::producer_id_manager::ProducerIdManager;

/// Phase 1 of EndTxn: validate the `Ongoing → Prepare{Commit,Abort}` transition
/// and apply it to `entry`. Returns `(prepare, complete)` states on success, or
/// the Kafka error code to return. Pure; the caller persists `entry` afterwards.
pub(crate) fn decide_phase1_transition(
    entry: &mut TxnEntry,
    committed: bool,
) -> Result<(TxnState, TxnState), i16> {
    let prepare = if committed {
        TxnState::PrepareCommit
    } else {
        TxnState::PrepareAbort
    };
    let complete = if committed {
        TxnState::CompleteCommit
    } else {
        TxnState::CompleteAbort
    };
    if !entry.state.can_transition_to(prepare) {
        return Err(codes::INVALID_TXN_STATE);
    }
    entry.state = prepare;
    Ok((prepare, complete))
}

/// Phase 3 of EndTxn: after the marker fan-out, re-validate the re-acquired
/// `entry` and decide whether to finalise. Pure wrapper over
/// `validate_complete_reacquire` + `next_producer_identity`.
pub(crate) fn decide_end_txn_completion(
    entry: &TxnEntry,
    expected_pid: i64,
    expected_epoch: i16,
    prepare: TxnState,
    complete: TxnState,
    txnv: TxnVersion,
    ids: &ProducerIdManager,
) -> CompletionDecision {
    match validate_complete_reacquire(entry, expected_pid, expected_epoch, prepare, complete) {
        ReacquireDecision::Proceed => {
            let (pid, epoch) = next_producer_identity(txnv, entry.producer_id, entry.producer_epoch, ids);
            CompletionDecision::Proceed {
                next_state: complete,
                response_pid: pid,
                response_epoch: epoch,
            }
        }
        ReacquireDecision::AlreadyComplete => CompletionDecision::AlreadyComplete {
            response_pid: entry.producer_id,
            response_epoch: entry.producer_epoch,
        },
        ReacquireDecision::Reject(code) => CompletionDecision::Reject(code),
    }
}

/// Outcome of `decide_end_txn_completion`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompletionDecision {
    Proceed {
        next_state: TxnState,
        response_pid: i64,
        response_epoch: i16,
    },
    AlreadyComplete {
        response_pid: i64,
        response_epoch: i16,
    },
    Reject(i16),
}

// ── relocated verbatim from end_txn.rs (now pub(crate)) ──
// fn next_producer_identity(...)  — unchanged body
// enum ReacquireDecision { Proceed, AlreadyComplete, Reject(i16) }  — unchanged
// fn validate_complete_reacquire(...) -> ReacquireDecision  — unchanged body

#[cfg(test)]
#[path = "decision_model.rs"]
mod decision_model;
```

Relocate the three items (`next_producer_identity`, `ReacquireDecision`, `validate_complete_reacquire`) with their **exact current bodies** from `end_txn.rs:345-414`, changing `fn`→`pub(crate) fn` and `enum`→`pub(crate) enum`. Also relocate their unit tests (`end_txn.rs` ~631-793, the `next_producer_identity` / `validate_complete_reacquire` tests) into a `#[cfg(test)] mod tests` in `decision.rs`.

- [ ] **Step 3: Rewrite `end_txn.rs` Phase 1 to call `decide_phase1_transition`**

Replace the Phase-1 block (`end_txn.rs:96-123`, the `let prepare = …; let complete = …; let prepare_snap = { … can_transition_to … entry.state = prepare … }`) with:

```rust
    let marker_type = if req.committed { MarkerType::Commit } else { MarkerType::Abort };

    let (prepare, complete, prepare_snap): (TxnState, TxnState, TxnEntry) = {
        let mut entry = entry_mutex.lock().await;
        match super::super::decision::decide_phase1_transition(&mut entry, req.committed) {
            Ok((prepare, complete)) => {
                entry.last_update_ms = now_millis();
                (prepare, complete, entry.clone())
            }
            Err(code) => return encode_err(version, code),
        }
        // Lock dropped here.
    };
```

(Adjust the `super::super::decision` path to the correct relative path from `txn/handlers/end_txn.rs` to `txn/decision.rs`.)

- [ ] **Step 4: Rewrite `end_txn.rs` Phase 3 to call `decide_end_txn_completion`**

Replace the Phase-3 `match validate_complete_reacquire(...) { … }` block (`end_txn.rs:190-…`) so it calls `decide_end_txn_completion(&entry, req.producer_id, req.producer_epoch, prepare, complete, txnv, &ids)` and matches on `CompletionDecision::{Proceed, AlreadyComplete, Reject}`, preserving the exact existing behavior (Proceed → set `entry.state`, apply `response_pid`/`response_epoch`, persist; AlreadyComplete → `encode_ok(version, response_pid, response_epoch)`; Reject(code) → log + `encode_err(version, code)`). The `ids` handle is whatever `next_producer_identity` was already called with in the original Phase-3 code — thread the same `&ProducerIdManager` in.

- [ ] **Step 5: Create the model stub**

Create `crates/broker/src/txn/decision_model.rs`:

```rust
//! Exhaustive stateright model of the KIP-98/EOS EndTxn decision core — built
//! in TCM-T2. See docs/superpowers/specs/2026-06-14-crabka-txn-coordinator-model-design.md.
```

- [ ] **Step 6: Build + run the txn regression suite**

Run: `cargo test -p crabka-broker --lib txn`
Expected: all existing txn tests pass (the relocated `next_producer_identity` / `validate_complete_reacquire` tests now under `txn::decision::tests`, plus `end_txn`/`add_partitions` handler tests). This proves the extraction preserved behavior.

- [ ] **Step 7: fmt + clippy + commit**

```bash
cargo +nightly fmt -p crabka-broker
cargo +nightly fmt -p crabka-broker -- --check        # expect exit 0
cargo clippy -p crabka-broker --all-targets -- -D warnings   # expect exit 0
git add crates/broker/src/txn/decision.rs crates/broker/src/txn/handlers/end_txn.rs \
        crates/broker/src/txn/mod.rs crates/broker/src/txn/decision_model.rs
git commit -m "refactor(broker): extract pure EndTxn decision core + wire txn model module"
```

---

## Task TCM-T2: The model + properties + basic config

**Files:** modify `crates/broker/src/txn/decision_model.rs`.

The model drives the real `decide_phase1_transition` / `decide_end_txn_completion` (and the real `TxnState::can_transition_to`) over a single modeled topic. Single topic ⇒ a partition is just an `i32`; `TxnEntry.partitions` collapses to a `BTreeSet<i32>`. State is projected via the existing `TxnState::to_kafka_status() -> i8` (hashable; no `Hash` derive needed).

- [ ] **Step 1: Write the full model**

Replace `decision_model.rs` with the model. Core shape:

```rust
use std::collections::BTreeSet;
use std::time::Duration;

use crabka_protocol::codes;
use stateright::{Checker, Model, Property};

use super::super::state::{TxnEntry, TxnState};
use super::super::version::TxnVersion;
use super::{decide_end_txn_completion, decide_phase1_transition, CompletionDecision};
use crate::producer_id_manager::ProducerIdManager;

const MAX_STATES: usize = 200_000;
const MAX_DEPTH: usize = 60;
const CHECK_TIMEOUT: Duration = Duration::from_mins(2);
const MAX_EPOCH: i16 = 3;
const PID: i64 = 1000; // fixed base pid; epoch is the fencing dimension

struct TxnModel {
    partitions: Vec<i32>,
    max_epoch: i16,
}

/// In-flight EndTxn captured at Phase 1, awaiting Phase 3 (the marker window).
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct PendingEnd {
    expected_pid: i64,
    expected_epoch: i16,
    prepare: i8,   // TxnState::to_kafka_status()
    complete: i8,
    committed: bool,
}

/// The model state (one modeled tid). `Model::State = TxnProj` directly.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct TxnProj {
    pid: i64,
    epoch: i16,
    state: i8,                 // TxnState::to_kafka_status()
    partitions: Vec<i32>,      // sorted
    pending_end: Option<PendingEnd>,
    /// Terminal outcomes observed for THIS (pid,epoch): commit and/or abort.
    /// The no-commit-and-abort invariant asserts this never holds both.
    committed_done: bool,
    aborted_done: bool,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum TxnAction {
    InitProducerId,            // allocate/bump epoch; aborts an Ongoing txn
    AddPartition(i32),         // epoch+state-gated add (Ongoing)
    EndTxnPhase1(bool),        // committed? → Prepare; opens the window
    EndTxnPhase3,              // re-validate → Complete or fenced-reject
}
```

Then `impl Model for TxnModel`:
- `init_states`: one tid in `Empty` at `(PID, epoch 0)`, no pending, no terminal flags.
- `actions`: gate by `txn.epoch < max_epoch` for epoch-advancing actions. `InitProducerId` always (bumps epoch). `AddPartition(p)` for each `p ∈ partitions` when no pending and `can_transition_to(Ongoing)`. `EndTxnPhase1(true/false)` when no pending and state is `Ongoing`. `EndTxnPhase3` when `pending_end.is_some()`.
- `next_state`: reconstruct a real `TxnEntry` from `txn` (single topic; `partitions` → `HashSet<TopicPartition>`), then:
  - `InitProducerId`: if state `Ongoing`/`Prepare*`, model the abort (state → matching `CompleteAbort` / cleared) per the real init path, then bump `epoch` (`+1`, gated `< max_epoch`). This fences any `pending_end` (its `expected_epoch` no longer matches).
  - `AddPartition(p)`: drive real `can_transition_to(Ongoing)`; if ok, state → `Ongoing`, insert `p`.
  - `EndTxnPhase1(committed)`: call `decide_phase1_transition(&mut entry, committed)`; on `Ok((prepare, complete))` set `state`, capture `pending_end{expected_pid, expected_epoch, prepare, complete, committed}`.
  - `EndTxnPhase3`: call `decide_end_txn_completion(&entry, pending.expected_pid, pending.expected_epoch, prepare, complete, TxnVersion::Verified, &ProducerIdManager::new())`. On `Proceed{next_state, response_epoch, ..}` → set `state=next_state`, `epoch=response_epoch`, set `committed_done`/`aborted_done` from `pending.committed`; clear pending. On `AlreadyComplete` → clear pending. On `Reject(_)` → clear pending (do NOT finalize). Assert epoch-monotonicity (`next.epoch >= last.epoch`) here.
- `properties`:
  - `always("no_commit_and_abort", |_, s: &TxnProj| !(s.committed_done && s.aborted_done))` — HEADLINE.
  - `always("legal_dag", ...)` — every realized state transition satisfied `can_transition_to` (tracked via a `next_state` assert rather than stored history).
  - `always("fenced_cannot_complete", ...)` — if a `pending_end`'s `expected_epoch != s.epoch` (fenced mid-window), a subsequent `EndTxnPhase3` must NOT set a terminal flag (enforced by the `Reject` path; assert in `next_state`).
  - `sometimes("can_commit", |_, s: &TxnProj| s.committed_done)` and `sometimes("can_fence_midwindow", ...)` — non-vacuity (a producer is fenced while a pending EndTxn is open).
- `within_boundary`: `state.epoch <= max_epoch`.
- `run(model, label)`: the standard harness (`target_max_depth`/`target_state_count`/`timeout`, assert no cap truncation, `assert_properties`).

```rust
fn basic() -> TxnModel { TxnModel { partitions: vec![0, 1], max_epoch: MAX_EPOCH } }

#[test]
fn txn_basic() {
    run(TxnModel::basic(), "txn_basic");
}
```

- [ ] **Step 2: fmt + clippy** (`cargo +nightly fmt -p crabka-broker`; `cargo clippy -p crabka-broker --all-targets -- -D warnings`).

- [ ] **Step 3: Run `txn_basic` UNDER THE WATCHDOG**

Build the lib-test exe without running, then run only `txn_basic` through the host memory watchdog (the established `Invoke-GuardedExe`-style PowerShell: launch the exe directly, poll `WorkingSet64`, kill on >3 GB or >150 s). Capture the `[txn_basic] unique_states=… max_depth=…` line and the pass/fail.

- [ ] **Step 4: DECISION GATE — GREEN or RED**

- **GREEN:** the model proved the safety properties. Proceed to TCM-T3 GREEN path.
- **RED:** stateright prints a minimal action trace. Sanity-check it's a real gap (the fencing/idempotency logic genuinely admits a bad interleaving) vs a model-fidelity issue in how `InitProducerId`'s abort/bump is modeled. If real, it's a genuine EOS bug — capture the trace, mark `txn_basic` `#[ignore]` with the trace, commit, and report to the user before any coordinator fix (like the reconciliation slice's RED path).

- [ ] **Step 5: Commit**

```bash
cargo +nightly fmt -p crabka-broker -- --check
git add crates/broker/src/txn/decision_model.rs
git commit -m "test(broker): stateright model of KIP-98 EndTxn fencing + atomicity"
```

---

## Task TCM-T3: Scale-up + finalize (GREEN) / report (RED)

**Files:** modify `decision_model.rs`.

### GREEN path

- [ ] **Step G1: Add a wider config**

```rust
fn wide() -> TxnModel { TxnModel { partitions: vec![0, 1, 2], max_epoch: MAX_EPOCH } }

#[test]
fn txn_wide() {
    run(TxnModel::wide(), "txn_wide");
}
```

(If modeling two tids is tractable within the state cap, prefer that for the wide config — two independent tids sharing the pid space exercises cross-tid epoch independence. Reduce partitions if the count exceeds ~100k states.)

- [ ] **Step G2: Run `txn_wide` under the watchdog.** Keep it only if exhaustive (`state_count < MAX_STATES`, `max_depth < MAX_DEPTH`); record the state count in a comment.

- [ ] **Step G3: Confirm broader txn suite + finalize**

```
cargo test -p crabka-broker --lib txn::decision::tests   # relocated pure-fn tests
cargo test -p crabka-broker --lib txn -- --skip decision_model   # handler tests (model run guarded separately)
cargo +nightly fmt -p crabka-broker -- --check
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/src/txn/decision_model.rs
git commit -m "test(broker): add wide txn-coordinator config + final verification"
```

- [ ] **Step G4: Update memory** `project_stateright_testing_program.md` to record the txn-coordinator model as implemented (Workstream A now also covers KIP-98/EOS).

### RED path

- [ ] **Step R1: Minimize + document the counterexample**, add the minimal trace as a doc comment / `#[ignore]`d regression test.

- [ ] **Step R2: STOP and present to the user** — the trace, the confirmation it's a real EOS gap (a fenced producer completed, or a commit-and-abort occurred), and the fix options, before implementing any coordinator change (the fix would touch `end_txn.rs`'s re-validation — protocol-critical, needs review).

---

## Self-Review

**Spec coverage:**
- Refactor (extract `decide_phase1_transition` + `decide_end_txn_completion` wrapping the existing pure fns) → TCM-T1. ✓
- Wrap-real model driving the real decision fns → TCM-T2. ✓
- EndTxn Phase1→window→Phase3 split with concurrent `InitProducerId`/`AddPartition` → TCM-T2 actions + `pending_end`. ✓
- Properties: no-commit-and-abort (headline), legal-DAG, epoch-fencing/fenced-cannot-complete, idempotency (AlreadyComplete path), non-vacuity witnesses → TCM-T2 Step 1. ✓
- Bounding (1–2 tids, epoch 0–3, 2–3 partitions) + watchdog + caps → TCM-T2/T3. ✓
- Two-outcome (green finalize / red report) → TCM-T3. ✓
- Out-of-scope (marker I/O, offset materialization, timeout expiry, legacy txn version) → respected: model abstracts Phase 2 as the window, uses `TxnVersion::Verified` only, no timestamps. ✓

**Placeholder scan:** The relocation of `next_producer_identity`/`validate_complete_reacquire` (T1 Step 2) references their exact current source (`end_txn.rs:345-414`) rather than re-printing — this is a *move*, not new code, and the lines are cited; not a hidden TODO. The T2 model `next_state` window logic is specified action-by-action with the exact decision-fn calls; the per-action body is finalized at implementation (inherently iterative for a model, as with the reconciliation slice) and gated by the run.

**Type consistency:** `decide_phase1_transition(&mut TxnEntry, bool) -> Result<(TxnState,TxnState), i16>` and `decide_end_txn_completion(&TxnEntry, i64, i16, TxnState, TxnState, TxnVersion, &ProducerIdManager) -> CompletionDecision` match between T1 (definition) and T2 (call sites). `CompletionDecision` variants (`Proceed{next_state,response_pid,response_epoch}`, `AlreadyComplete{response_pid,response_epoch}`, `Reject(i16)`) are used consistently. State projection via `to_kafka_status() -> i8` (real, existing). `TxnVersion::Verified` + `ProducerIdManager::new()` match the real APIs (`version.rs:10`, `producer_id_manager.rs:27`).
