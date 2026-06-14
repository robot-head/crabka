# KIP-73 Token-Bucket Concurrency Model Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the confirmed lock-free over-grant/underflow race in `TokenBucket::try_consume` with a CAS loop, and prove it with a stateright shared-memory interleaving model (RED on the current algorithm → GREEN after the fix, with concurrent `set_rate`) + a proptest over the extracted pure `plan_consume`.

**Architecture:** Extract the pure arithmetic (`plan_consume`); rewrite `try_consume` as a `compare_exchange_weak` loop using it. A stateright model interleaves per-thread atomic steps over a shared `available` (modeled as `i64` so the buggy underflow is a catchable negative); a `cas` flag selects the buggy 3-step RMW (RED) vs the atomic CAS commit (GREEN). The headline invariant is `0 ≤ available ≤ max_rate`.

**Tech Stack:** Rust, `stateright` 0.31 + `proptest` (both `crabka-broker` dev-deps).

**Spec:** `docs/superpowers/specs/2026-06-14-crabka-token-bucket-quota-model-design.md`

**Verification discipline:** stateright runs are watchdog-guarded (3 GB / 150 s — `[[feedback_bound_model_checkers]]`); the model bounds exhaustiveness on unique-state count with a high truncation target (the compaction-model technique) so the BFS completes. proptest is bounded sampling. `cargo +nightly fmt` per-crate (`[[reference_windows_fmt_path_length]]`); clippy `-D warnings`; backtick doc-comment code identifiers.

---

## File Structure

- `crates/broker/src/throttle/bucket.rs` — **modify**: add `plan_consume`; rewrite `try_consume` as a CAS loop; wire the model module; add a `proptest` module.
- `crates/broker/src/throttle/bucket_model.rs` — **create**: stateright interleaving model (`#[cfg(test)]` descendant of `bucket`).

Batches: **B1** {Task TB-A} · **B2** {Task TB-B} (TB-B depends on TB-A's `plan_consume`; model + proptest both append to `bucket.rs`, so one owner).

---

## Task TB-A: extract `plan_consume` + CAS `try_consume`

**Files:** modify `crates/broker/src/throttle/bucket.rs`.

- [ ] **Step 1: Add `plan_consume`** (module level, `pub(crate)`):

```rust
/// Pure token-bucket consume arithmetic. Given the current `available`, the
/// `refill` claimed for this call, the `rate` cap, and `requested` bytes, return
/// `(grant, new_available)`:
///   `capped   = (available + refill).min(rate)`
///   `grant    = requested.min(capped)`
///   `new      = capped - grant`   (>= 0 by construction)
/// Used by both the real `try_consume` CAS loop and the model/proptest.
pub(crate) fn plan_consume(available: u64, refill: u64, rate: u64, requested: u64) -> (u64, u64) {
    let capped = available.saturating_add(refill).min(rate);
    let grant = requested.min(capped);
    (grant, capped - grant)
}
```

- [ ] **Step 2: Failing test for `plan_consume`**, then run to confirm it passes (the fn above already satisfies it):

```rust
#[test]
fn plan_consume_grants_and_caps() {
    assert!(plan_consume(100, 0, 1000, 50) == (50, 50));     // partial
    assert!(plan_consume(100, 0, 1000, 200) == (100, 0));    // drained
    assert!(plan_consume(900, 500, 1000, 200) == (200, 800)); // refill capped at rate
    assert!(plan_consume(0, 0, 1000, 100) == (0, 0));        // empty
    assert!(plan_consume(u64::MAX, u64::MAX, 1000, 1000) == (1000, 0)); // saturating + cap
}
```

- [ ] **Step 3: Rewrite `try_consume` as a CAS loop**

Replace the body after the `refill` computation:

```rust
pub fn try_consume(&self, requested: u64) -> u64 {
    let rate = self.rate_bytes_per_sec.load(Relaxed);
    if rate == 0 {
        return requested;
    }
    let now = now_nanos();
    let last = self.last_refill_nanos.swap(now, Relaxed);
    let elapsed = now.saturating_sub(last);
    let refill = ((u128::from(elapsed) * u128::from(rate)) / 1_000_000_000) as u64;
    // CAS loop: refill + consume must commit atomically against `available`, or a
    // concurrent caller (or a `set_rate` reset) can clobber the read-modify-write
    // and over-grant / underflow. The `last_refill` swap above already claimed
    // this call's elapsed gap atomically.
    loop {
        let cur = self.available.load(Relaxed);
        let (grant, new_avail) = plan_consume(cur, refill, rate, requested);
        if self
            .available
            .compare_exchange_weak(cur, new_avail, Relaxed, Relaxed)
            .is_ok()
        {
            return grant;
        }
    }
}
```

Remove the old `store` + `fetch_sub`. (`set_rate` is unchanged — the CAS retry absorbs its concurrent `available` reset.)

- [ ] **Step 4: Verify behavior-preserving**

Run: `cargo test -p crabka-broker --lib throttle::bucket` (the 6 existing single-threaded bucket tests + the new `plan_consume` test). Expected: all pass (single-threaded behavior is unchanged — one caller's CAS always succeeds first try).

- [ ] **Step 5: fmt + clippy + commit**

`cargo +nightly fmt -p crabka-broker`; `cargo clippy -p crabka-broker --lib -- -D warnings`. Then:
```bash
git add crates/broker/src/throttle/bucket.rs
git commit -m "fix(broker): make TokenBucket::try_consume atomic with a CAS loop (KIP-73)"
```

---

## Task TB-B: stateright interleaving model (RED→GREEN) + proptest

**Files:** create `crates/broker/src/throttle/bucket_model.rs`; modify `bucket.rs` (module wiring + proptest). **Depends on TB-A.**

- [ ] **Step 1: Wire the module** — append to `bucket.rs`:

```rust
#[cfg(test)]
#[path = "bucket_model.rs"]
mod bucket_model;
```

- [ ] **Step 2: Write the model** — create `crates/broker/src/throttle/bucket_model.rs`:

```rust
//! Exhaustive stateright shared-memory interleaving model of the KIP-73
//! `TokenBucket` concurrency. State = the shared `{rate, available, pending}`
//! atomics (modeled small; `available` is `i64` so the buggy underflow shows up
//! as a catchable negative) + a per-thread program counter for each in-flight
//! `try_consume` / `set_rate`. Actions interleave one atomic step at a time.
//!
//! A `cas` flag selects the algorithm: `false` reproduces the CURRENT buggy
//! read-modify-write (`Load` → `Store` → `Sub` as three interleavable steps,
//! where `Sub` can drive `available` negative); `true` models the fixed CAS
//! commit as one atomic read-compute-write step (the net effect of the
//! `compare_exchange_weak` loop). `set_rate` is three interleavable stores
//! (`rate`, `available`, reset `pending`) in both modes. The headline invariant
//! `0 <= available <= max_rate` is violated by the buggy path (RED witness) and
//! held by the CAS path even with concurrent `set_rate` (GREEN). See the spec.

use std::time::Duration;

use stateright::{Checker, Model, Property};

use super::plan_consume;

const TARGET_STATE_COUNT: usize = 4_000_000;
const MAX_UNIQUE_STATES: usize = 500_000;
const MAX_DEPTH: usize = 60;
const CHECK_TIMEOUT: Duration = Duration::from_mins(2);

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum Pc {
    Idle,
    // try_consume:
    Claimed { refill: i64, req: i64 },           // claimed refill; next: Load
    Loaded { refill: i64, req: i64, cur: i64 },  // read `available`; next: Store (buggy) / CommitCas (fixed)
    Stored { observed: i64, req: i64 },          // buggy only: stored capped avail; next: Sub
    // set_rate (three non-atomic stores):
    SetRate0 { new_rate: i64 },                  // next: store rate
    SetRate1 { new_rate: i64 },                  // next: store available
    SetRate2,                                    // next: reset pending
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct BucketState {
    rate: i64,
    available: i64,
    pending: i64,
    pcs: Vec<Pc>,
}

struct BucketModel {
    cas: bool,
    threads: usize,
    max_rate: i64,
    max_req: i64,
    max_pending: i64,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum Act {
    Tick,
    StartConsume(usize, i64), // (thread, requested)
    StartSetRate(usize, i64), // (thread, new_rate)
    Step(usize),              // advance thread's in-flight op by one atomic step
}

impl Model for BucketModel {
    type State = BucketState;
    type Action = Act;

    fn init_states(&self) -> Vec<Self::State> {
        vec![BucketState {
            rate: self.max_rate,
            available: self.max_rate,
            pending: 0,
            pcs: vec![Pc::Idle; self.threads],
        }]
    }

    fn actions(&self, s: &Self::State, actions: &mut Vec<Self::Action>) {
        if s.pending < self.max_pending {
            actions.push(Act::Tick);
        }
        for (t, pc) in s.pcs.iter().enumerate() {
            match pc {
                Pc::Idle => {
                    for req in 0..=self.max_req {
                        actions.push(Act::StartConsume(t, req));
                    }
                    for nr in [0, self.max_rate] {
                        actions.push(Act::StartSetRate(t, nr));
                    }
                }
                _ => actions.push(Act::Step(t)),
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = last.clone();
        match action {
            Act::Tick => {
                s.pending = (s.pending + 1).min(self.max_pending);
            }
            Act::StartConsume(t, req) => {
                // rate==0 fast path grants instantly without touching `available`.
                if s.rate == 0 {
                    return None; // irrelevant to the race; no state change
                }
                let refill = s.pending; // claim the elapsed gap (atomic swap)
                s.pending = 0;
                s.pcs[t] = Pc::Claimed { refill, req };
            }
            Act::StartSetRate(t, new_rate) => {
                s.pcs[t] = Pc::SetRate0 { new_rate };
            }
            Act::Step(t) => match s.pcs[t].clone() {
                Pc::Idle => return None,
                Pc::Claimed { refill, req } => {
                    if self.cas {
                        // Fixed: atomic read-compute-write (net effect of the CAS loop).
                        let (_grant, new) = plan_consume(
                            s.available.max(0) as u64,
                            refill as u64,
                            s.rate as u64,
                            req as u64,
                        );
                        s.available = new as i64;
                        s.pcs[t] = Pc::Idle;
                    } else {
                        s.pcs[t] = Pc::Loaded { refill, req, cur: s.available };
                    }
                }
                Pc::Loaded { refill, req, cur } => {
                    // Buggy: store the capped available (a plain store).
                    let capped = (cur + refill).min(s.rate);
                    s.available = capped;
                    s.pcs[t] = Pc::Stored { observed: capped, req };
                }
                Pc::Stored { observed, req } => {
                    // Buggy: fetch_sub(grant) on the CURRENT available — can go negative.
                    let grant = req.min(observed);
                    s.available -= grant;
                    s.pcs[t] = Pc::Idle;
                }
                Pc::SetRate0 { new_rate } => {
                    s.rate = new_rate;
                    s.pcs[t] = Pc::SetRate1 { new_rate };
                }
                Pc::SetRate1 { new_rate } => {
                    s.available = new_rate;
                    s.pcs[t] = Pc::SetRate2;
                }
                Pc::SetRate2 => {
                    s.pending = 0;
                    s.pcs[t] = Pc::Idle;
                }
            },
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // HEADLINE: available never underflows (< 0) nor exceeds the burst cap.
            // The buggy fetch_sub drives it negative (RED); the CAS path holds it.
            Property::always("available_in_range", |m: &BucketModel, s: &BucketState| {
                0 <= s.available && s.available <= m.max_rate
            }),
            // Non-vacuity: ≥2 threads in flight at once (real interleaving).
            Property::sometimes("concurrent_inflight", |_, s: &BucketState| {
                s.pcs.iter().filter(|p| **p != Pc::Idle).count() >= 2
            }),
            // Non-vacuity: a set_rate overlaps an in-flight consume.
            Property::sometimes("setrate_during_consume", |_, s: &BucketState| {
                let set = s
                    .pcs
                    .iter()
                    .any(|p| matches!(p, Pc::SetRate0 { .. } | Pc::SetRate1 { .. } | Pc::SetRate2));
                let con = s.pcs.iter().any(|p| {
                    matches!(p, Pc::Claimed { .. } | Pc::Loaded { .. } | Pc::Stored { .. })
                });
                set && con
            }),
            // Non-vacuity: the bucket gets fully drained.
            Property::sometimes("can_drain", |_, s: &BucketState| s.available == 0),
        ]
    }

    fn within_boundary(&self, s: &Self::State) -> bool {
        // available may transiently exceed max_rate only via a bug; keep the
        // explored space bounded on the legitimate range plus a small margin so a
        // counterexample is still discovered (not pruned away).
        s.available >= -(self.max_rate + self.max_req + 1)
            && s.available <= self.max_rate
            && s.pending <= self.max_pending
    }
}

fn green_run(model: BucketModel, label: &str) {
    let checker = model
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(TARGET_STATE_COUNT)
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
    assert!(checker.state_count() < TARGET_STATE_COUNT, "[{label}] truncated — not exhaustive");
    assert!(
        checker.unique_state_count() < MAX_UNIQUE_STATES,
        "[{label}] unique-state bound exceeded ({})",
        checker.unique_state_count()
    );
    checker.assert_properties();
}

#[test]
fn bucket_basic() {
    green_run(
        BucketModel { cas: true, threads: 2, max_rate: 2, max_req: 2, max_pending: 1 },
        "bucket_basic",
    );
}

#[test]
fn bucket_wide() {
    green_run(
        BucketModel { cas: true, threads: 2, max_rate: 3, max_req: 3, max_pending: 2 },
        "bucket_wide",
    );
}

/// RED witness: the CURRENT non-CAS algorithm violates `available_in_range`
/// (the over-grant/underflow race). We assert a counterexample is DISCOVERED.
#[test]
fn race_underflows_without_cas() {
    let checker = BucketModel { cas: false, threads: 2, max_rate: 1, max_req: 1, max_pending: 0 }
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(TARGET_STATE_COUNT)
        .timeout(CHECK_TIMEOUT)
        .spawn_bfs()
        .join();
    assert!(
        checker.discovery("available_in_range").is_some(),
        "expected the non-CAS try_consume to violate available_in_range (underflow/over-grant)"
    );
}
```

(`Checker::discovery(name)` returns `Some(path)` when an `always` property has a counterexample. If the exact stateright 0.31 API differs — e.g. `discoveries()` / `counterexample(name)` — adapt to the available method that returns the discovered counterexample for the named property.)

- [ ] **Step 3: Write the proptest** — append to `bucket.rs`:

```rust
#[cfg(test)]
mod plan_fuzz {
    use proptest::prelude::*;
    use super::plan_consume;

    proptest! {
        /// The pure arithmetic: grant within request + cap, new_available never
        /// underflows and never exceeds the rate cap.
        #[test]
        fn plan_consume_invariants(
            available in 0u64..=u64::MAX,
            refill in 0u64..=u64::MAX,
            rate in 0u64..1_000_000,
            requested in 0u64..=u64::MAX,
        ) {
            let (grant, new) = plan_consume(available, refill, rate, requested);
            let capped = available.saturating_add(refill).min(rate);
            prop_assert!(grant <= requested);
            prop_assert!(grant <= capped);
            prop_assert_eq!(new, capped - grant);
            prop_assert!(new <= rate, "burst cap");
            // (new is u64 and == capped - grant with grant <= capped, so no underflow.)
        }

        /// Sequential conservation: over a chain of consumes at a fixed rate with
        /// per-step refills, granted total never exceeds initial + Σ refills.
        #[test]
        fn sequential_conservation(
            rate in 1u64..10_000,
            ops in proptest::collection::vec((0u64..20_000, 0u64..20_000), 0..200usize),
        ) {
            let mut available = rate.min(rate); // start full
            let mut supplied = available;
            let mut granted: u64 = 0;
            for (refill, requested) in ops {
                let refill = refill.min(rate); // a single step never adds more than a burst
                supplied = supplied.saturating_add(((available.saturating_add(refill)).min(rate)).saturating_sub(available));
                let (g, new) = plan_consume(available, refill, rate, requested);
                granted = granted.saturating_add(g);
                available = new;
                prop_assert!(available <= rate);
            }
            prop_assert!(granted <= supplied, "granted {granted} exceeded supplied {supplied}");
        }
    }
}
```

(If the `sequential_conservation` accounting proves awkward, simplify to the core property: `available <= rate` after every step and `grant <= available_before + refill` per step — the `plan_consume_invariants` test already covers the per-call contract, which is the load-bearing one.)

- [ ] **Step 4: Build + run (controller runs the model under the watchdog)**

`cargo +nightly fmt -p crabka-broker`; `cargo clippy -p crabka-broker --all-targets -- -D warnings`; build `cargo test -p crabka-broker --lib bucket_model --no-run`; run `cargo test -p crabka-broker --lib plan_fuzz` (proptest). The CONTROLLER runs `bucket_basic` + `bucket_wide` + `race_underflows_without_cas` under the host memory watchdog (poll `WorkingSet64`, kill > 3 GB / > 150 s), confirming: the GREEN configs are exhaustive (`state_count < TARGET`, `unique < MAX_UNIQUE`, `max_depth < MAX_DEPTH`) with all asserts + witnesses holding, and the RED test discovers a counterexample. Tune bounds (threads/max_rate/max_req/max_pending, and `MAX_UNIQUE_STATES`) using the compaction-model techniques if a config truncates.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/throttle/bucket.rs crates/broker/src/throttle/bucket_model.rs
git commit -m "test(broker): stateright interleaving model + proptest for the token-bucket CAS fix"
```

---

## Self-Review

**Spec coverage:** `plan_consume` extraction + CAS `try_consume` (TB-A) ✓; shared-memory interleaving model with per-thread PCs (TB-B Step 2) ✓; `cas` flag RED (buggy 3-step RMW) / GREEN (atomic CAS step) ✓; concurrent `set_rate` three-store steps ✓; headline `0 <= available <= max_rate` + non-vacuity witnesses ✓; committed RED witness via `discovery` (TB-B `race_underflows_without_cas`) ✓; proptest over `plan_consume` + conservation (TB-B Step 3) ✓; watchdog + unique-state-bound discipline (TB-B Step 4) ✓.

**Placeholder scan:** `plan_consume` + `try_consume` given in full. The model is complete runnable code; the `discovery` API name and the bounds are flagged for tuning at the run step (TB-B Step 4) — the established pattern for every model slice, not hidden TODOs. The proptest's `sequential_conservation` has an explicit simpler fallback.

**Type consistency:** `plan_consume(u64,u64,u64,u64) -> (u64,u64)` used identically in TB-A (def + CAS loop), the model (`Act::Step` CAS branch), and the proptest. The model's `BucketState`/`Pc`/`Act` are internally consistent; `within_boundary` permits the small negative range so the underflow counterexample isn't pruned before discovery.
