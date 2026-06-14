# Fetch HWM Visibility-Window Model Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract a pure `compute_visibility_window` from `do_read`, collapse the two duplicated response-HW/LSO computations into it, and prove the read-path clamp contract + KIP-227 monotonicity via exhaustive `stateright` + `proptest`.

**Architecture:** A pure, total decision fn over the partition watermarks + fetch params, called from `do_read` (single source of truth) and driven by a stateright model (advancing-watermark `Tick` actions + `Fetch` probes) and a proptest. Sequential pure-fn core → exhaustive-small stateright + large-N proptest, the same shape as the data-plane slices.

**Tech Stack:** Rust, `stateright` 0.31 + `proptest` (both already `crabka-broker` dev-deps).

**Spec:** `docs/superpowers/specs/2026-06-14-crabka-fetch-hwm-visibility-model-design.md`

**Verification discipline:** stateright runs are watchdog-guarded (3 GB / 150 s, `target`/`timeout` caps — `[[feedback_bound_model_checkers]]`); proptest is bounded sampling. `cargo +nightly fmt` per-crate (`[[reference_windows_fmt_path_length]]`); clippy `-D warnings`; backtick doc-comment code identifiers.

---

## File Structure

- `crates/broker/src/handlers/fetch.rs` — **modify**: add `VisibilityWindow` + `compute_visibility_window`; rewire `do_read`'s two response-field sites; wire the model module; add a `proptest` fuzz module.
- `crates/broker/src/handlers/fetch_visibility_model.rs` — **create**: stateright model (`#[cfg(test)]` descendant of `fetch`).

Batches: **B1** {Task FV-A} · **B2** {Task FV-B} (FV-B depends on FV-A's extracted fn; model + proptest both append to `fetch.rs`, so one implementer owns FV-B).

---

## Task FV-A: extract `compute_visibility_window` + de-dup `do_read`

**Files:** modify `crates/broker/src/handlers/fetch.rs`.

- [ ] **Step 1: Read the current `do_read`**

Read `crates/broker/src/handlers/fetch.rs:955-1135` — the `do_read` fn, its `ReadPlan` enum, the metadata-hold block (`:1000-1046`) that computes `upper_bound`/`effective_lso`/`limit_offset` + the OOR response-field block (`:1017-1024`), and the success/`NONE` response-field block (`:1115-1123`).

- [ ] **Step 2: Add `VisibilityWindow` + `compute_visibility_window`**

Insert above `do_read` (module level, `pub(crate)`):

```rust
/// The pure read-path visibility decision: given a partition's watermarks and a
/// fetch's parameters, what offsets may this fetch expose and what HW/LSO does
/// it report. Extracted from `do_read` so it is the single source of truth for
/// the response fields (previously computed in two places) and is exhaustively
/// + property-tested (see `fetch_visibility_model.rs`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct VisibilityWindow {
    /// `fetch_offset < log_start` — caller returns `OFFSET_OUT_OF_RANGE`.
    pub out_of_range: bool,
    /// `fetch_offset >= upper_bound` — nothing to read (no bytes).
    pub empty: bool,
    /// Exclusive upper offset the raw read may expose: `[fetch_offset, limit_offset)`.
    pub limit_offset: i64,
    /// read_committed aborted-txn scan ceiling (`lso.min(hw)` for a read_committed consumer).
    pub effective_lso: i64,
    /// Whether to populate `aborted_transactions` (read_committed consumer only).
    pub read_committed_aborts: bool,
    /// `out.high_watermark` to report.
    pub response_hw: i64,
    /// `out.last_stable_offset` to report.
    pub response_lso: i64,
}

/// Kafka invariants the caller upholds: `0 <= log_start <= hw <= log_end` and
/// `lso <= hw`; `read_committed` is only set for consumer fetches, so
/// `read_committed` implies `!is_follower`.
pub(crate) fn compute_visibility_window(
    is_follower: bool,
    read_committed: bool,
    log_start: i64,
    hw: i64,
    lso: i64,
    log_end: i64,
    fetch_offset: i64,
) -> VisibilityWindow {
    let upper_bound = if is_follower { log_end } else { hw };
    let effective_lso = if read_committed && !is_follower {
        lso.min(hw)
    } else {
        lso
    };
    let response_hw = if is_follower { log_end } else { hw };
    let response_lso = if read_committed && !is_follower {
        lso.min(hw)
    } else if is_follower {
        log_end
    } else {
        hw
    };
    let limit_offset = if is_follower {
        log_end
    } else if read_committed {
        effective_lso
    } else {
        hw
    };
    let out_of_range = fetch_offset < log_start;
    let empty = !out_of_range && fetch_offset >= upper_bound;
    VisibilityWindow {
        out_of_range,
        empty,
        limit_offset,
        effective_lso,
        read_committed_aborts: read_committed && !is_follower,
        response_hw,
        response_lso,
    }
}
```

- [ ] **Step 3: Rewire `do_read` to call it**

Replace the metadata-hold block + the OOR response-field block + the success response-field block so all three derive from one `compute_visibility_window` call. Inside the `let (log_start, log_end, lso, plan) = { … }` block, after reading `log_start`/`log_end`/`lso` under the lock, compute `let w = compute_visibility_window(is_follower_fetch, read_committed, log_start, hw, lso, log_end, fetch_offset);` and build the plan from `w`:
- `w.out_of_range` → set `out.error_code = codes::OFFSET_OUT_OF_RANGE; out.log_start_offset = log_start; out.high_watermark = w.response_hw; out.last_stable_offset = w.response_lso;` → `ReadPlan::OffsetOutOfRange`.
- else `w.empty` → `ReadPlan::Empty`; else `ReadPlan::Read { limit_offset: w.limit_offset, effective_lso: w.effective_lso, read_committed_aborts: w.read_committed_aborts }`.

After the read (the `out.error_code = codes::NONE;` block at `:1114`), replace the duplicated response-field assignment with:
```rust
    out.error_code = codes::NONE;
    out.high_watermark = w.response_hw;
    out.log_start_offset = log_start;
    out.last_stable_offset = w.response_lso;
```
(`w` must be in scope after the lock block — return it from the block alongside `(log_start, log_end, lso, plan)`, or recompute is unnecessary; prefer threading `w` out of the block.) Leave the `aborted_transactions` population (`:1125-1130`), the byte read, and everything else unchanged.

- [ ] **Step 4: Verify behavior-preserving**

Run: `cargo test -p crabka-broker --lib fetch` and `cargo test -p crabka-broker --test '*fetch*'` (the fetch handler unit + any fetch integration tests). Expected: all pass unchanged.

- [ ] **Step 5: fmt + clippy + commit**

`cargo +nightly fmt -p crabka-broker`; `cargo clippy -p crabka-broker --all-targets -- -D warnings`. Then:
```bash
git add crates/broker/src/handlers/fetch.rs
git commit -m "refactor(broker): extract compute_visibility_window, de-dup fetch response fields"
```

---

## Task FV-B: stateright model + proptest

**Files:** create `crates/broker/src/handlers/fetch_visibility_model.rs`; modify `crates/broker/src/handlers/fetch.rs` (module wiring + proptest module). **Depends on FV-A.**

- [ ] **Step 1: Wire the model module**

Append to `fetch.rs`:
```rust
#[cfg(test)]
#[path = "fetch_visibility_model.rs"]
mod fetch_visibility_model;
```

- [ ] **Step 2: Write the model**

Create `crates/broker/src/handlers/fetch_visibility_model.rs` (follow the `leader_epoch_model.rs` / `compact_model.rs` template for the stateright API + the watchdog-bounded `run`):

```rust
//! Exhaustive stateright enumeration of the fetch read-path visibility decision
//! (`super::compute_visibility_window`). State = the advancing partition
//! watermarks `{log_start, hw, lso, log_end}` (Kafka invariant
//! `0 <= log_start <= hw <= log_end`, `lso in [log_start, hw]`); `Tick` actions
//! advance them monotonically, `Fetch` probes drive the real decision. Asserts
//! the clamp contract (no-dirty-read / read_committed clamp / follower=LEO /
//! response consistency) per fetch and KIP-227 monotonicity across ticks. See
//! the design spec.

use std::time::Duration;

use stateright::{Checker, Model, Property};

use super::{VisibilityWindow, compute_visibility_window};

const MAX_STATES: usize = 200_000;
const MAX_DEPTH: usize = 40;
const CHECK_TIMEOUT: Duration = Duration::from_mins(2);

struct VisModel {
    max_offset: i64,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct VisState {
    log_start: i64,
    hw: i64,
    lso: i64,
    log_end: i64,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum VisAction {
    AdvanceLogEnd,
    AdvanceHw,
    AdvanceLso,
    AdvanceLogStart,
    /// (is_follower, read_committed, fetch_offset) — read_committed ⟹ !is_follower.
    Fetch(bool, bool, i64),
}

fn response_hw(is_follower: bool, hw: i64, log_end: i64) -> i64 {
    if is_follower { log_end } else { hw }
}
fn response_lso(is_follower: bool, read_committed: bool, hw: i64, lso: i64, log_end: i64) -> i64 {
    if read_committed && !is_follower {
        lso.min(hw)
    } else if is_follower {
        log_end
    } else {
        hw
    }
}

impl Model for VisModel {
    type State = VisState;
    type Action = VisAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![VisState { log_start: 0, hw: 0, lso: 0, log_end: 0 }]
    }

    fn actions(&self, s: &Self::State, actions: &mut Vec<Self::Action>) {
        // Advance watermarks, preserving 0<=log_start<=lso<=hw<=log_end<=max_offset.
        if s.log_end < self.max_offset {
            actions.push(VisAction::AdvanceLogEnd);
        }
        if s.hw < s.log_end {
            actions.push(VisAction::AdvanceHw);
        }
        if s.lso < s.hw {
            actions.push(VisAction::AdvanceLso);
        }
        if s.log_start < s.lso {
            actions.push(VisAction::AdvanceLogStart);
        }
        // Probe every fetch shape over a bounded fetch_offset window.
        for fo in 0..=(self.max_offset + 1) {
            actions.push(VisAction::Fetch(false, false, fo)); // consumer, read_uncommitted
            actions.push(VisAction::Fetch(false, true, fo)); // consumer, read_committed
            actions.push(VisAction::Fetch(true, false, fo)); // follower
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        match action {
            VisAction::AdvanceLogEnd => {
                let mut s = last.clone();
                s.log_end += 1;
                assert_monotonic(last, &s);
                Some(s)
            }
            VisAction::AdvanceHw => {
                let mut s = last.clone();
                s.hw += 1;
                assert_monotonic(last, &s);
                Some(s)
            }
            VisAction::AdvanceLso => {
                let mut s = last.clone();
                s.lso += 1;
                assert_monotonic(last, &s);
                Some(s)
            }
            VisAction::AdvanceLogStart => {
                let mut s = last.clone();
                s.log_start += 1;
                Some(s) // log_start advancing never lowers response_hw/lso
            }
            VisAction::Fetch(is_follower, read_committed, fetch_offset) => {
                let w = compute_visibility_window(
                    is_follower, read_committed,
                    last.log_start, last.hw, last.lso, last.log_end, fetch_offset,
                );
                assert_fetch_contract(last, is_follower, read_committed, fetch_offset, &w);
                None // probes never change state
            }
        }
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("watermarks_ordered", |_, s: &VisState| {
                0 <= s.log_start && s.log_start <= s.lso && s.lso <= s.hw && s.hw <= s.log_end
            }),
            // A read_committed clamp below HW is reachable (lso < hw).
            Property::sometimes("can_clamp_lso", |_, s: &VisState| s.lso < s.hw),
            // A follower can be served beyond HW (hw < log_end).
            Property::sometimes("follower_beyond_hw", |_, s: &VisState| s.hw < s.log_end),
            // OFFSET_OUT_OF_RANGE is reachable (log_start > 0 ⟹ a fetch_offset below it exists).
            Property::sometimes("can_out_of_range", |_, s: &VisState| s.log_start > 0),
        ]
    }

    fn within_boundary(&self, s: &Self::State) -> bool {
        s.log_end <= self.max_offset
    }
}

fn assert_monotonic(old: &VisState, new: &VisState) {
    for &if in &[false, true] {
        for &rc in &[false, true] {
            if rc && if {
                continue; // read_committed ⟹ !follower
            }
            assert!(
                response_hw(if, new.hw, new.log_end) >= response_hw(if, old.hw, old.log_end),
                "response_hw regressed on advance"
            );
            assert!(
                response_lso(if, rc, new.hw, new.lso, new.log_end)
                    >= response_lso(if, rc, old.hw, old.lso, old.log_end),
                "response_lso regressed on advance"
            );
        }
    }
}

fn assert_fetch_contract(
    s: &VisState,
    is_follower: bool,
    read_committed: bool,
    fetch_offset: i64,
    w: &VisibilityWindow,
) {
    // valid targets
    assert!(w.limit_offset >= 0 && w.response_hw >= 0 && w.response_lso >= 0);
    // out_of_range / empty correctness
    assert!(w.out_of_range == (fetch_offset < s.log_start));
    let upper = if is_follower { s.log_end } else { s.hw };
    if !w.out_of_range {
        assert!(w.empty == (fetch_offset >= upper));
    }
    // response single-source-of-truth contract
    assert!(w.response_hw == response_hw(is_follower, s.hw, s.log_end));
    assert!(w.response_lso == response_lso(is_follower, read_committed, s.hw, s.lso, s.log_end));
    if is_follower {
        // follower bound: serve up to LEO (>= hw)
        assert!(w.limit_offset == s.log_end && w.limit_offset >= s.hw);
    } else {
        // no-dirty-read: never expose beyond HW
        assert!(w.limit_offset <= s.hw, "consumer fetch exposed beyond HW");
        assert!(w.response_lso <= w.response_hw);
        if read_committed {
            assert!(w.effective_lso == s.lso.min(s.hw));
            assert!(w.limit_offset <= s.lso.min(s.hw));
        }
    }
}

fn run(model: VisModel, label: &str) {
    let checker = model
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(MAX_STATES)
        .timeout(CHECK_TIMEOUT)
        .spawn_bfs()
        .join();
    eprintln!(
        "[{label}] unique_states={} generated={} max_depth={}",
        checker.unique_state_count(),
        checker.state_count(),
        checker.max_depth()
    );
    assert!(checker.max_depth() < MAX_DEPTH, "[{label}] depth cap hit");
    assert!(checker.state_count() < MAX_STATES, "[{label}] state cap hit");
    checker.assert_properties();
}

#[test]
fn visibility_basic() {
    run(VisModel { max_offset: 4 }, "visibility_basic");
}

#[test]
fn visibility_wide() {
    run(VisModel { max_offset: 7 }, "visibility_wide");
}
```

- [ ] **Step 3: Write the proptest**

Append to `fetch.rs` a `#[cfg(test)] mod visibility_fuzz` (`proptest` is a dev-dep). Generate large-N valid watermark tuples + fetch params and assert the same contract + relational monotonicity:

```rust
#[cfg(test)]
mod visibility_fuzz {
    use proptest::prelude::*;
    use super::{compute_visibility_window, VisibilityWindow};

    // Build a valid (log_start <= lso <= hw <= log_end) tuple from sorted offsets.
    proptest! {
        #[test]
        fn visibility_contract_holds(
            a in 0i64..1_000_000, b in 0i64..1_000_000, c in 0i64..1_000_000, d in 0i64..1_000_000,
            fo in 0i64..1_000_000, is_follower in any::<bool>(), rc_raw in any::<bool>(),
        ) {
            let mut v = [a, b, c, d]; v.sort_unstable();
            let (log_start, lso, hw, log_end) = (v[0], v[1], v[2], v[3]);
            let read_committed = rc_raw && !is_follower; // precondition
            let w = compute_visibility_window(is_follower, read_committed, log_start, hw, lso, log_end, fo);
            prop_assert!(w.limit_offset >= 0 && w.response_hw >= 0 && w.response_lso >= 0);
            prop_assert_eq!(w.out_of_range, fo < log_start);
            if is_follower {
                prop_assert_eq!(w.limit_offset, log_end);
                prop_assert!(w.limit_offset >= hw);
            } else {
                prop_assert!(w.limit_offset <= hw, "no dirty read");
                prop_assert!(w.response_lso <= w.response_hw);
                if read_committed {
                    prop_assert_eq!(w.effective_lso, lso.min(hw));
                    prop_assert!(w.limit_offset <= lso.min(hw));
                }
            }
        }

        /// KIP-227 monotonicity: advancing hw/lso/log_end never lowers reported HW/LSO.
        #[test]
        fn response_monotonic(
            a in 0i64..100_000, da in 0i64..100_000, db in 0i64..100_000, dc in 0i64..100_000,
            is_follower in any::<bool>(), rc_raw in any::<bool>(),
        ) {
            let read_committed = rc_raw && !is_follower;
            let log_start = 0;
            let (hw, lso, log_end) = (a, a, a + da); // valid: lso=hw<=log_end
            let (hw2, lso2, log_end2) = (hw + db, lso + db, log_end + db + dc); // all advance
            let w1 = compute_visibility_window(is_follower, read_committed, log_start, hw, lso, log_end, 0);
            let w2 = compute_visibility_window(is_follower, read_committed, log_start, hw2, lso2, log_end2, 0);
            prop_assert!(w2.response_hw >= w1.response_hw);
            prop_assert!(w2.response_lso >= w1.response_lso);
        }
    }
}
```

- [ ] **Step 4: Build + run (controller runs the model under the watchdog)**

`cargo +nightly fmt -p crabka-broker`; `cargo clippy -p crabka-broker --all-targets -- -D warnings`; build the model exe `cargo test -p crabka-broker --lib fetch_visibility_model --no-run`; run `cargo test -p crabka-broker --lib visibility_fuzz` (proptest, bounded). The CONTROLLER runs `visibility_basic` + `visibility_wide` under the host memory watchdog (launch the exe, poll `WorkingSet64`, kill > 3 GB / > 150 s), confirms exhaustive (`state_count < MAX_STATES`, `max_depth < MAX_DEPTH`) + all asserts hold + witnesses satisfied, and scales `visibility_wide` up while exhaustive. If a config truncates, tune `max_offset` (or apply the unique-state-bound technique from the compaction model).

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/handlers/fetch.rs crates/broker/src/handlers/fetch_visibility_model.rs
git commit -m "test(broker): stateright model + proptest for fetch HWM visibility window"
```

---

## Self-Review

**Spec coverage:** extract `compute_visibility_window` + de-dup (FV-A) ✓; stateright model with advancing-watermark `Tick` + `Fetch` probes (FV-B Step 2) ✓; no-dirty-read / read_committed clamp / follower=LEO / response-consistency / OOR+empty asserts (FV-B `assert_fetch_contract`) ✓; KIP-227 monotonicity (`assert_monotonic` + proptest `response_monotonic`) ✓; non-vacuity witnesses ✓; proptest large-N (FV-B Step 3) ✓; watchdog discipline (FV-B Step 4) ✓.

**Placeholder scan:** `compute_visibility_window` is given in full (transcribed from `do_read`'s current logic); the model + proptest are complete code; bounds are tuned at the run step (FV-B Step 4) as in prior slices. No hidden TODOs.

**Type consistency:** `compute_visibility_window(bool,bool,i64,i64,i64,i64,i64) -> VisibilityWindow` and the `VisibilityWindow` fields (`out_of_range, empty, limit_offset, effective_lso, read_committed_aborts, response_hw, response_lso`) are used identically in FV-A (def + `do_read` rewire) and FV-B (model + proptest). The model's local `response_hw`/`response_lso` helpers mirror the fn's formulas exactly (the contract being asserted). ✓
