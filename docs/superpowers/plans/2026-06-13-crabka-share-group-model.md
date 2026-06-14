# Share-group acquisition stateright model — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **OOM CAUTION — read before Task 2.** This plan runs a stateright BFS checker.
> stateright keeps every visited unique state resident in RAM; an under-bounded
> run OOM'd this user's machine once and forced a reboot
> (`memory/feedback_bound_model_checkers.md`). Therefore: **every checker run
> (Tasks 2–4) MUST go through the PowerShell memory watchdog defined in Task 2,
> and a subagent must NEVER run the checker unguarded** (a subagent uses the
> same host RAM). The recommended execution mode for this plan is **inline by
> the main agent**, exactly as the Phase-1 raft model was executed — see the
> Execution Handoff section. Build with `--no-run` first (unguarded; rustc is
> bounded), then run the test under the watchdog.

**Goal:** Exhaustively model-check the pure KIP-932 share-partition acquisition core (`AcquisitionState`) with stateright, asserting the share-group delivery-safety invariants hold under every interleaving of consumer operations, time advance, and leader-failover reload.

**Architecture:** A **wrap-real** stateright `Model` whose fingerprinted state embeds the *real* `AcquisitionState` and whose `next_state` drives the production `materialize`/`acquire`/`acknowledge`/`renew`/`expire_locks`/`to_persist_batches`/`load_from`. The model lives **inside the broker crate** as a `#[cfg(test)]` descendant module of `state` (`#[path="state_model.rs"] mod state_model;`), so it reads the private batch internals directly — zero new production observability surface; the only production change is adding `Clone/Eq/Hash` derives. Single-state invariants are `Property::always`; predecessor/durability invariants are imperative `assert!`s in `next_state` (kept out of the hashed state to avoid ghost-history explosion). A small finite logical clock makes lock-expiry-vs-renew exercisable while keeping `Instant`s hashable.

**Tech Stack:** Rust, `stateright = "=0.31.0"` (dev-dependency), `cargo test --lib`, PowerShell memory watchdog.

**Spec:** `docs/superpowers/specs/2026-06-13-crabka-share-group-model-design.md`

---

## File Structure

| File | Responsibility |
| --- | --- |
| `crates/broker/Cargo.toml` | (modify) add `stateright = { workspace = true }` to `[dev-dependencies]`. |
| `crates/broker/src/share_partition/state.rs` | (modify) add `Clone/PartialEq/Eq/Hash` derives to `AcquisitionState`, `PartialEq/Eq/Hash` to `InFlightBatch`, `Hash` to `RecordState` and `AckType`; declare `#[cfg(test)] #[path="state_model.rs"] mod state_model;`. |
| `crates/broker/src/share_partition/state_model.rs` | (create) the entire model: `ShareModel`/`ShareState`/`ShareAction`, observability + invariant helpers, the `Model` impl, the watchdog-friendly `run` harness, and the `#[test]` config functions. |

All model logic is one focused file; tasks build it incrementally and each leaves the crate compiling and the model (as built so far) checkable.

---

## Task 1: stateright dev-dep + hashable share types + module wiring

**Files:**
- Modify: `crates/broker/Cargo.toml:94` (the `[dev-dependencies]` block)
- Modify: `crates/broker/src/share_partition/state.rs:28` (`RecordState`), `:36` (`AckType`), `:68` (`InFlightBatch`), `:85` (`AcquisitionState`), and end-of-file (module declaration)
- Create: `crates/broker/src/share_partition/state_model.rs`

- [ ] **Step 1: Add the stateright dev-dependency**

In `crates/broker/Cargo.toml`, inside the existing `[dev-dependencies]` block (starts at line 94), add this line just after `assert2 = { workspace = true }`:

```toml
stateright = { workspace = true }
```

(The workspace already pins `stateright = "=0.31.0"` in the root `Cargo.toml` `[workspace.dependencies]`; the raft crate uses the identical `{ workspace = true }` form.)

- [ ] **Step 2: Add the derive additions in `state.rs`**

These are pure, behavior-preserving derive additions so the real types can live in a fingerprinted stateright state. Make exactly these four edits in `crates/broker/src/share_partition/state.rs`:

`RecordState` (line 28) — add `Hash`:
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RecordState {
```

`AckType` (line 36) — add `Hash` (it is embedded in the model's action enum, which derives `Hash`):
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AckType {
```

`InFlightBatch` (line 68) — add `PartialEq, Eq, Hash`:
```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct InFlightBatch {
```

`AcquisitionState` (line 85) — add `Clone, PartialEq, Eq, Hash`:
```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AcquisitionState {
```

- [ ] **Step 3: Declare the model module**

At the very end of `crates/broker/src/share_partition/state.rs` (after the existing `#[cfg(test)] mod tests { ... }` block), add:

```rust
#[cfg(test)]
#[path = "state_model.rs"]
mod state_model;
```

Because `state::state_model` is a *descendant* module of `state`, it can read the private `batches` field, the private `InFlightBatch` fields, and the private `delivery_complete_count` field directly — exactly how `mod tests` already accesses private internals. No new accessors or `pub` items are needed.

- [ ] **Step 4: Create the smoke-test model file**

Create `crates/broker/src/share_partition/state_model.rs` with just enough to prove the derives compile and the module is wired:

```rust
//! Exhaustive stateright model of the pure KIP-932 share-partition acquisition
//! core (`AcquisitionState`). See
//! `docs/superpowers/specs/2026-06-13-crabka-share-group-model-design.md`.
//!
//! The remaining model (types, `Model` impl, properties, configs) is added in
//! later tasks. This file starts as a derive/wiring smoke test.
//!
//! Memory safety: stateright BFS keeps every visited unique state resident, so
//! each checker run is fenced with `within_boundary` + `target_state_count` +
//! `timeout`, and MUST be run under the host memory watchdog while bounds are
//! being tuned (never unguarded).

use super::{AckType, AcquisitionState, RecordState};

#[test]
fn derives_compile() {
    // Build a small machine, exercise it, then prove Clone + Eq + Hash work
    // (these are what let the real machine live in a fingerprinted model state).
    use std::collections::HashSet;
    use std::time::{Duration, Instant};

    let mut s = AcquisitionState::new(0);
    s.materialize(2, 100);
    let _ = s.acquire("m0", 1, i32::MAX, Instant::now(), Duration::from_secs(1), 2);

    let clone = s.clone();
    assert_eq!(s, clone);

    let mut set: HashSet<AcquisitionState> = HashSet::new();
    set.insert(s);
    assert!(set.contains(&clone));

    // Touch the imported enums so the import is used and Hash is exercised.
    let mut codes: HashSet<(RecordState, AckType)> = HashSet::new();
    codes.insert((RecordState::Acquired, AckType::Accept));
    assert!(codes.contains(&(RecordState::Acquired, AckType::Accept)));
}
```

- [ ] **Step 5: Build (no run) and run the smoke test**

Build first (unguarded; this is a normal rustc build, not the checker):

```
cargo test -p crabka-broker --lib --no-run
```
Expected: compiles cleanly (the derives and the new module build).

Then run the smoke test:
```
cargo test -p crabka-broker --lib state_model::derives_compile -- --nocapture
```
Expected: `test ... derives_compile ... ok`.

- [ ] **Step 6: Confirm the existing `AcquisitionState` unit tests still pass**

The derives must not change behavior:
```
cargo test -p crabka-broker --lib share_partition::state::tests
```
Expected: all existing `state::tests::*` tests pass (e.g. `acquire_then_accept_advances_spso`, `renew_extends_lock_keeping_acquired`, …).

- [ ] **Step 7: Commit**

```
git add crates/broker/Cargo.toml crates/broker/src/share_partition/state.rs crates/broker/src/share_partition/state_model.rs
git commit -m "test(broker): stateright dev-dep + hashable share-partition types"
```

---

## Task 2: Core model (all ops except Reload) + full property suite + concurrency configs

**Files:**
- Modify (replace contents): `crates/broker/src/share_partition/state_model.rs`

This task replaces the smoke-test file with the complete model **minus** the `Reload` action (added in Task 3). It includes all state-level `always` properties, all transition-level asserts, all non-vacuity `sometimes` witnesses, the `within_boundary` bound, the watchdog-friendly `run` harness, and two `share_concurrency` configs.

- [ ] **Step 1: Write the complete model file**

Replace the entire contents of `crates/broker/src/share_partition/state_model.rs` with:

```rust
//! Exhaustive stateright model of the pure KIP-932 share-partition acquisition
//! core (`AcquisitionState`).
//!
//! The model state holds the REAL `AcquisitionState` and drives the production
//! `materialize` / `acquire` / `acknowledge` / `renew` / `expire_locks` /
//! `to_persist_batches` / `load_from`; the BFS checker explores every
//! interleaving of consumer operations, time advance, and (in the failover
//! config, Task 3) leader-reload, asserting the share-group delivery-safety
//! invariants never break. Design:
//! `docs/superpowers/specs/2026-06-13-crabka-share-group-model-design.md`.
//!
//! Memory safety: stateright BFS keeps every visited unique state resident, so
//! each run is fenced with `within_boundary` + `target_state_count` + `timeout`
//! and MUST be executed under the host memory watchdog while bounds are tuned
//! (never unguarded — a runaway space exhausts host RAM).

use std::time::{Duration, Instant};

use stateright::{Checker, Model, Property};

use super::{AckType, AcquisitionState, RecordState};

/// The single acquisition-lock duration used by the model. A lock taken at
/// logical time `clock` has deadline `t0 + LOCK*(clock + 1)`, so it expires once
/// the clock reaches `clock + 1`.
const LOCK: Duration = Duration::from_secs(1);

/// Hard backstop on generated states — bounds host memory even if
/// `within_boundary` is looser than intended. Set well above each config's true
/// bounded count so a real (exhaustive) run never truncates.
const MAX_STATES: usize = 200_000;
/// Depth backstop. Must exceed each config's reachable-graph diameter, or the
/// search is depth-truncated (incomplete) and the `run` harness fails loudly.
const MAX_DEPTH: usize = 80;
/// Wall-clock backstop.
const CHECK_TIMEOUT: Duration = Duration::from_secs(120);

/// Bounded model config (held here, not in the fingerprinted state).
struct ShareModel {
    /// Base instant; all `now` values are `t0 + LOCK*clock`. Captured once per
    /// run, so deadlines are drawn from a finite, hashable set.
    t0: Instant,
    /// Number of consumer members (named `m0`..`m{members-1}`).
    members: u8,
    /// High-watermark / window cap (records produced over a path).
    max_offset: i64,
    /// Logical-clock cap.
    max_tick: u8,
    /// Delivery-attempt limit before a record is archived as a poison pill.
    max_attempts: i16,
    /// Max records `materialize` pulls into the window at once.
    max_inflight: i32,
    /// Whether the leader-failover `Reload` action is generated (Task 3).
    allow_reload: bool,
}

/// The fingerprinted model state: the REAL machine plus the small finite clock
/// and produced-record high-watermark.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct ShareState {
    sm: AcquisitionState,
    clock: u8,
    hwm: i64,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum ShareAction {
    /// Append one record to the log (raise the produced high-watermark).
    Produce,
    /// Leader pulls produced-but-unmaterialized records into the window.
    Materialize,
    /// `member` acquires up to `max_records` Available records.
    Acquire { member: u8, max_records: i32 },
    /// `member` acknowledges `[first, last]` it holds.
    Acknowledge {
        member: u8,
        first: i64,
        last: i64,
        ack: AckType,
    },
    /// `member` renews (extends) the lock on `[first, last]` it holds.
    Renew { member: u8, first: i64, last: i64 },
    /// Sweep expired acquisition locks back to Available.
    ExpireLocks,
    /// Advance the logical clock by one lock-duration.
    Tick,
}

impl ShareModel {
    /// Concurrency config: full action set EXCEPT `Reload`. Bounds start small
    /// (proven memory-safe); Task 4 scales `max_offset` empirically.
    fn concurrency(max_offset: i64, max_inflight: i32) -> Self {
        Self {
            t0: Instant::now(),
            members: 2,
            max_offset,
            max_tick: 2,
            max_attempts: 2,
            max_inflight,
            allow_reload: false,
        }
    }

    fn now(&self, clock: u8) -> Instant {
        self.t0 + LOCK * u32::from(clock)
    }

    fn member_name(member: u8) -> String {
        format!("m{member}")
    }
}

// ---- observability helpers (descendant-module private access) --------------

/// Delivery state of `off`, if it currently lies in a batch.
fn offset_state(sm: &AcquisitionState, off: i64) -> Option<RecordState> {
    sm.batches
        .iter()
        .find(|b| b.first_offset <= off && off <= b.last_offset)
        .map(|b| b.state)
}

/// Delivery count of `off`, if it currently lies in a batch.
fn offset_dc(sm: &AcquisitionState, off: i64) -> Option<i16> {
    sm.batches
        .iter()
        .find(|b| b.first_offset <= off && off <= b.last_offset)
        .map(|b| b.delivery_count)
}

/// Maximal contiguous offset runs currently Acquired by `member`. Adjacent
/// same-owner batches with differing lock deadlines do not coalesce, so they are
/// stitched back into one run here (the whole run is ack/renew-able at once).
fn acquired_runs(sm: &AcquisitionState, member: &str) -> Vec<(i64, i64)> {
    let mut runs: Vec<(i64, i64)> = Vec::new();
    let mut cur: Option<(i64, i64)> = None;
    for b in &sm.batches {
        let mine = b.state == RecordState::Acquired && b.acquired_by.as_deref() == Some(member);
        match (mine, cur) {
            (true, Some((f, l))) if b.first_offset == l + 1 => cur = Some((f, b.last_offset)),
            (true, Some((f, l))) => {
                runs.push((f, l));
                cur = Some((b.first_offset, b.last_offset));
            }
            (true, None) => cur = Some((b.first_offset, b.last_offset)),
            (false, Some((f, l))) => {
                runs.push((f, l));
                cur = None;
            }
            (false, None) => {}
        }
    }
    if let Some((f, l)) = cur {
        runs.push((f, l));
    }
    runs
}

// ---- state-level invariants (Property::always predicates) ------------------

/// Batches are sorted, gap-free, non-overlapping, and exactly cover
/// `[start_offset, end_offset)`; `start_offset <= end_offset`.
fn window_integrity(sm: &AcquisitionState) -> bool {
    if sm.start_offset > sm.end_offset {
        return false;
    }
    if sm.batches.is_empty() {
        return sm.start_offset == sm.end_offset;
    }
    if sm.batches[0].first_offset != sm.start_offset {
        return false;
    }
    for w in sm.batches.windows(2) {
        if w[0].first_offset > w[0].last_offset || w[0].last_offset + 1 != w[1].first_offset {
            return false;
        }
    }
    let last = sm.batches.last().expect("non-empty checked above");
    last.first_offset <= last.last_offset && last.last_offset + 1 == sm.end_offset
}

/// Every Acquired batch carries exactly one owner. Combined with
/// `window_integrity`'s non-overlap, no offset is concurrently held by two
/// members — the headline share-group guarantee.
fn mutual_exclusion(sm: &AcquisitionState) -> bool {
    sm.batches
        .iter()
        .all(|b| b.state != RecordState::Acquired || b.acquired_by.is_some())
}

/// Lock bookkeeping matches the delivery state: Acquired ⇒ owner + deadline
/// present; every other state ⇒ neither present.
fn lock_consistency(sm: &AcquisitionState) -> bool {
    sm.batches.iter().all(|b| match b.state {
        RecordState::Acquired => b.acquired_by.is_some() && b.lock_deadline.is_some(),
        _ => b.acquired_by.is_none() && b.lock_deadline.is_none(),
    })
}

// ---- transition-level invariants (asserted in next_state) ------------------

/// Compare a parent machine to its child after one operation; panic on any
/// monotonicity / durability violation. Kept OUT of the fingerprinted state so
/// no path-history ghost can explode the space (Phase-1 OOM lesson).
fn assert_transition(parent: &AcquisitionState, child: &AcquisitionState) {
    assert!(
        child.start_offset >= parent.start_offset,
        "SPSO regressed: {} -> {}",
        parent.start_offset,
        child.start_offset
    );
    assert!(
        child.delivery_complete_count >= parent.delivery_complete_count,
        "delivery_complete_count regressed: {} -> {}",
        parent.delivery_complete_count,
        child.delivery_complete_count
    );
    // Per-offset delivery_count never regresses for offsets live in both.
    for off in child.start_offset..child.end_offset {
        if let (Some(pc), Some(cc)) = (offset_dc(parent, off), offset_dc(child, off)) {
            assert!(
                cc >= pc,
                "delivery_count regressed at offset {off}: {pc} -> {cc}"
            );
        }
    }
    // An Acknowledged offset is terminal: in the child it is still Acknowledged
    // or has dropped below the (non-decreasing) SPSO — never resurrected.
    for off in parent.start_offset..parent.end_offset {
        if offset_state(parent, off) == Some(RecordState::Acknowledged) {
            match offset_state(child, off) {
                None => assert!(
                    off < child.start_offset,
                    "acknowledged offset {off} vanished while still in window"
                ),
                Some(s) => assert!(
                    s == RecordState::Acknowledged,
                    "acknowledged offset {off} reverted to {s:?}"
                ),
            }
        }
    }
}

impl Model for ShareModel {
    type State = ShareState;
    type Action = ShareAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![ShareState {
            sm: AcquisitionState::new(0),
            clock: 0,
            hwm: 0,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        let has_available = state
            .sm
            .batches
            .iter()
            .any(|b| b.state == RecordState::Available);
        let has_acquired = state
            .sm
            .batches
            .iter()
            .any(|b| b.state == RecordState::Acquired);

        if state.hwm < self.max_offset {
            actions.push(ShareAction::Produce);
        }
        // Materialize only when there are produced-but-unmaterialized records and
        // no Available batch remains (the real `materialize` no-ops otherwise).
        if state.sm.end_offset < state.hwm && !has_available {
            actions.push(ShareAction::Materialize);
        }
        if has_available {
            for member in 0..self.members {
                actions.push(ShareAction::Acquire {
                    member,
                    max_records: 1,
                });
                actions.push(ShareAction::Acquire {
                    member,
                    max_records: i32::MAX,
                });
            }
        }
        // Data-dependent: ack/renew only over ranges a member actually holds.
        for member in 0..self.members {
            let name = Self::member_name(member);
            for (first, last) in acquired_runs(&state.sm, &name) {
                for ack in [AckType::Accept, AckType::Release, AckType::Reject] {
                    actions.push(ShareAction::Acknowledge {
                        member,
                        first,
                        last,
                        ack,
                    });
                }
                actions.push(ShareAction::Renew {
                    member,
                    first,
                    last,
                });
                // A split (first half) exercises partial-ack / partial-renew.
                if last > first {
                    let mid = first + (last - first) / 2;
                    for ack in [AckType::Accept, AckType::Release, AckType::Reject] {
                        actions.push(ShareAction::Acknowledge {
                            member,
                            first,
                            last: mid,
                            ack,
                        });
                    }
                    actions.push(ShareAction::Renew {
                        member,
                        first,
                        last: mid,
                    });
                }
            }
        }
        if has_acquired {
            actions.push(ShareAction::ExpireLocks);
        }
        if state.clock < self.max_tick {
            actions.push(ShareAction::Tick);
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = last.clone();
        match action {
            ShareAction::Produce => {
                if state.hwm >= self.max_offset {
                    return None;
                }
                state.hwm += 1;
            }
            ShareAction::Materialize => {
                let before = state.sm.end_offset;
                state.sm.materialize(state.hwm, self.max_inflight);
                if state.sm.end_offset == before {
                    return None; // no-op: nothing materialized
                }
            }
            ShareAction::Acquire {
                member,
                max_records,
            } => {
                let name = Self::member_name(member);
                let now = self.now(state.clock);
                state
                    .sm
                    .acquire(&name, max_records, i32::MAX, now, LOCK, self.max_attempts);
            }
            ShareAction::Acknowledge {
                member,
                first,
                last: hi,
                ack,
            } => {
                let name = Self::member_name(member);
                let now = self.now(state.clock);
                if state.sm.acknowledge(&name, first, hi, ack, now).is_err() {
                    return None; // inapplicable ack: no transition
                }
            }
            ShareAction::Renew {
                member,
                first,
                last: hi,
            } => {
                let name = Self::member_name(member);
                let now = self.now(state.clock);
                if state.sm.renew(&name, first, hi, now, LOCK).is_err() {
                    return None; // inapplicable renew: no transition
                }
            }
            ShareAction::ExpireLocks => {
                let now = self.now(state.clock);
                state.sm.expire_locks(now);
            }
            ShareAction::Tick => {
                if state.clock >= self.max_tick {
                    return None;
                }
                state.clock += 1;
            }
        }
        assert_transition(&last.sm, &state.sm);
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("window_integrity", |_, s: &ShareState| {
                window_integrity(&s.sm)
            }),
            Property::always("mutual_exclusion", |_, s: &ShareState| {
                mutual_exclusion(&s.sm)
            }),
            Property::always("lock_consistency", |_, s: &ShareState| {
                lock_consistency(&s.sm)
            }),
            Property::always(
                "delivery_count_bounded",
                |m: &ShareModel, s: &ShareState| {
                    s.sm.batches.iter().all(|b| b.delivery_count <= m.max_attempts)
                },
            ),
            Property::always("spso_in_range", |m: &ShareModel, s: &ShareState| {
                0 <= s.sm.start_offset
                    && s.sm.start_offset <= s.sm.end_offset
                    && s.sm.end_offset <= m.max_offset
            }),
            Property::sometimes("can_advance_spso", |_, s: &ShareState| {
                s.sm.start_offset > 0
            }),
            Property::sometimes("can_acknowledge", |_, s: &ShareState| {
                s.sm
                    .batches
                    .iter()
                    .any(|b| b.state == RecordState::Acknowledged)
            }),
            Property::sometimes("can_archive", |_, s: &ShareState| {
                s.sm
                    .batches
                    .iter()
                    .any(|b| b.state == RecordState::Archived)
            }),
            Property::sometimes("can_redeliver", |_, s: &ShareState| {
                s.sm.batches.iter().any(|b| b.delivery_count >= 2)
            }),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        // Bound ONLY the design-unbounded dimensions (so the space is finite);
        // do NOT bound delivery_count — its <= max_attempts boundedness is a
        // property we verify, so pruning it would mask a violation. The 12-batch
        // cap is a loose structural safety net (real max over a <=3 window is 3).
        state.clock <= self.max_tick
            && state.hwm <= self.max_offset
            && state.sm.end_offset <= self.max_offset
            && state.sm.batches.len() <= 12
    }
}

/// Run one bounded config to completion and assert it was exhaustive (not
/// truncated by a cap) and that all properties hold.
fn run(model: ShareModel, label: &str) {
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
    assert!(
        checker.max_depth() < MAX_DEPTH,
        "[{label}] hit depth cap {MAX_DEPTH}: search is depth-truncated, not exhaustive"
    );
    assert!(
        checker.state_count() < MAX_STATES,
        "[{label}] hit state cap {MAX_STATES}: search is truncated, not exhaustive"
    );
    checker.assert_properties();
}

#[test]
fn share_concurrency_inflight_full() {
    // max_inflight large enough to pull the whole window in one materialize.
    run(
        ShareModel::concurrency(2, 2),
        "share_concurrency_inflight_full",
    );
}

#[test]
fn share_concurrency_inflight_one() {
    // max_inflight = 1: exercises drain-then-rematerialize across Produce steps.
    run(
        ShareModel::concurrency(2, 1),
        "share_concurrency_inflight_one",
    );
}
```

- [ ] **Step 2: Build (no run)**

```
cargo test -p crabka-broker --lib --no-run
```
Expected: compiles cleanly. If `clippy`-style warnings about casts appear they are non-fatal; fix only true compile errors.

- [ ] **Step 3: Define the memory watchdog (run it once in the PowerShell session)**

Paste this function into the PowerShell session; reuse it for every checker run in this plan. It runs a cargo test and kills it if resident memory exceeds 3 GB or it runs longer than 150 s.

```powershell
function Invoke-GuardedModelTest {
    param([string]$TestFilter)
    $p = Start-Process -FilePath 'cargo' -ArgumentList @(
        'test', '-p', 'crabka-broker', '--lib', $TestFilter, '--', '--nocapture'
    ) -PassThru -NoNewWindow
    $limitBytes = 3GB
    $deadlineSec = 150
    $elapsed = 0.0
    while (-not $p.HasExited) {
        Start-Sleep -Milliseconds 600
        $elapsed += 0.6
        try { $rss = (Get-Process -Id $p.Id -ErrorAction Stop).WorkingSet64 } catch { break }
        if ($rss -gt $limitBytes) {
            Write-Host "WATCHDOG KILL: RSS $([math]::Round($rss/1GB,2)) GB > 3 GB"
            Stop-Process -Id $p.Id -Force; break
        }
        if ($elapsed -gt $deadlineSec) {
            Write-Host "WATCHDOG KILL: elapsed ${elapsed}s > ${deadlineSec}s"
            Stop-Process -Id $p.Id -Force; break
        }
    }
    $p | Wait-Process
    Write-Host "exit code: $($p.ExitCode)"
}
```

(The crate is pre-built by Step 2's `--no-run`, so the guarded run spends its time in the checker, not the build.)

- [ ] **Step 4: Run each concurrency config under the watchdog**

```powershell
Invoke-GuardedModelTest 'state_model::share_concurrency_inflight_full'
Invoke-GuardedModelTest 'state_model::share_concurrency_inflight_one'
```
Expected for each: the `[label] unique_states=… generated=… max_depth=…` line prints, **`unique_states` is small (low thousands)**, neither cap-assert fires, all `always`/`sometimes` properties pass, and `exit code: 0`. If `unique_states` is unexpectedly large (> ~50k) or the watchdog kills the run, STOP and tighten `within_boundary` / `max_offset` before continuing — do not scale up.

- [ ] **Step 5: Commit**

```
git add crates/broker/src/share_partition/state_model.rs
git commit -m "test(broker): stateright share-partition acquisition model (concurrency)"
```

---

## Task 3: Leader-failover `Reload` action + failover config

**Files:**
- Modify: `crates/broker/src/share_partition/state_model.rs`

- [ ] **Step 1: Add the `Reload` action variant**

In the `ShareAction` enum, add a final variant (after `Tick`):

```rust
    /// Advance the logical clock by one lock-duration.
    Tick,
    /// Leader failover: persist + reload (drops Acquired → Available, locks lost).
    Reload,
```

- [ ] **Step 2: Add a failover constructor**

In `impl ShareModel`, add after `concurrency`:

```rust
    /// Failover config: adds `Reload` over a small window; focuses the
    /// `acknowledged_is_terminal` durability invariant across crash-recovery.
    fn failover() -> Self {
        Self {
            t0: Instant::now(),
            members: 2,
            max_offset: 2,
            max_tick: 2,
            max_attempts: 2,
            max_inflight: 2,
            allow_reload: true,
        }
    }
```

- [ ] **Step 3: Offer `Reload` in `actions`**

In `actions`, just before the `if state.clock < self.max_tick` block, add:

```rust
        if self.allow_reload && state.sm.end_offset > state.sm.start_offset {
            actions.push(ShareAction::Reload);
        }
```

- [ ] **Step 4: Handle `Reload` in `next_state`**

In the `match action` of `next_state`, add this arm after the `ShareAction::Tick` arm:

```rust
            ShareAction::Reload => {
                let (start, dcc, batches) = state.sm.to_persist_batches();
                let mut fresh = AcquisitionState::new(start);
                fresh.load_from(
                    start,
                    state.sm.state_epoch,
                    state.sm.leader_epoch,
                    dcc,
                    &batches,
                );
                state.sm = fresh;
            }
```

(`to_persist_batches` maps transient `Acquired` to `Available(0-state)` retaining the delivery_count, and emits `Acknowledged`/`Archived` with terminal codes; `load_from` rehydrates them. The `assert_transition` call already at the end of `next_state` then verifies SPSO/dcc/per-offset-dc monotonicity and `acknowledged_is_terminal` across the reload.)

- [ ] **Step 5: Add the failover test**

After `share_concurrency_inflight_one`, add:

```rust
#[test]
fn share_failover() {
    run(ShareModel::failover(), "share_failover");
}
```

- [ ] **Step 6: Build, then run the failover config under the watchdog**

```
cargo test -p crabka-broker --lib --no-run
```
Expected: compiles cleanly.

```powershell
Invoke-GuardedModelTest 'state_model::share_failover'
```
Expected: `[share_failover] unique_states=… …` prints, `unique_states` small, no cap-assert fires, all properties pass, `exit code: 0`. In particular `acknowledged_is_terminal` (asserted in `next_state`) must not panic across any `Reload`.

- [ ] **Step 7: Commit**

```
git add crates/broker/src/share_partition/state_model.rs
git commit -m "test(broker): add leader-failover reload config to share-partition model"
```

---

## Task 4: Empirical bound scale-up + final verification

**Files:**
- Modify: `crates/broker/src/share_partition/state_model.rs` (only if scale-up is kept)

Goal: push coverage as wide as is provably memory-safe, confirm exhaustiveness, and leave a green, self-bounded test suite that is safe to run unguarded in CI.

- [ ] **Step 1: Record the baseline state counts**

From Tasks 2–3, note each config's reported `unique_states`. All three should be small (low thousands at `max_offset = 2`).

- [ ] **Step 2: Attempt a `max_offset = 3` scale-up on the concurrency configs**

Temporarily change the two concurrency tests to `max_offset = 3`:

```rust
#[test]
fn share_concurrency_inflight_full() {
    run(
        ShareModel::concurrency(3, 3),
        "share_concurrency_inflight_full",
    );
}

#[test]
fn share_concurrency_inflight_one() {
    run(
        ShareModel::concurrency(3, 1),
        "share_concurrency_inflight_one",
    );
}
```

Build, then run BOTH under the watchdog:
```
cargo test -p crabka-broker --lib --no-run
```
```powershell
Invoke-GuardedModelTest 'state_model::share_concurrency_inflight_full'
Invoke-GuardedModelTest 'state_model::share_concurrency_inflight_one'
```

**Decision rule:**
- If both complete with `unique_states < 100_000`, no cap-assert fires, the watchdog does not kill them, and all properties pass → **keep** `max_offset = 3` (wider coverage).
- If either reports `unique_states >= 100_000`, hits a cap, or is watchdog-killed → **revert** those two tests to `concurrency(2, …)` (from Task 2). Memory safety wins over coverage.

If `max_depth` approaches `MAX_DEPTH` (80) without being truncated, that's fine; if a cap-assert reports depth truncation, raise `MAX_DEPTH` to 120 and re-run. Leave `share_failover` at `max_offset = 2`.

- [ ] **Step 3: Final full run under the watchdog**

Run all model tests together (still guarded):
```powershell
Invoke-GuardedModelTest 'state_model::'
```
Expected: all of `share_concurrency_inflight_full`, `share_concurrency_inflight_one`, `share_failover` pass; every printed `unique_states` is below the kept threshold; `exit code: 0`.

- [ ] **Step 4: Confirm the broader broker `--lib` suite is unaffected**

The model tests share the lib test binary with the existing `AcquisitionState` unit tests; confirm nothing regressed:
```
cargo test -p crabka-broker --lib share_partition::state
```
Expected: `state::tests::*` and `state::state_model::*` all pass. (This run is also self-bounded by the in-test caps, so it is safe for CI to run unguarded.)

- [ ] **Step 5: Format**

```
cargo fmt -p crabka-broker
```
(Per project rule: `cargo fmt` before push; on deep Windows worktrees use `-p <crate>` to avoid the OS 206 path-length failure. CI gates on formatting.)

- [ ] **Step 6: Commit**

```
git add crates/broker/src/share_partition/state_model.rs
git commit -m "test(broker): tune share-partition model bounds + final verification"
```

(If Step 2 reverted to `max_offset = 2` with no other change, and `cargo fmt` produced no diff, there may be nothing to commit — in that case skip this commit.)

- [ ] **Step 7: Update the program memory note**

Update `C:\Users\Matt Stone\.claude\projects\C--Users-Matt-Stone-git-crabka\memory\project_stateright_testing_program.md` to record that the share-group `AcquisitionState` model is now implemented (Workstream A advanced past raft), and that the remaining model candidates are ISR/`ReplicaState`, dynamic-voters (KIP-853), reassignment (KIP-455), and unclean-recovery (KIP-966). (Memory edit only; not a git commit.)

---

## Self-Review (completed by plan author)

**1. Spec coverage:**
- Wrap-real `AcquisitionState` driving real methods → Task 2 `next_state`. ✅
- In-src `#[cfg(test)]` descendant module, zero observability surface → Task 1 Step 3 + helpers reading private fields directly. ✅
- Derives (`AcquisitionState`/`InFlightBatch`/`RecordState`) → Task 1 Step 2; **plus `AckType` Hash** (spec under-specified; reconciled in spec + added here). ✅
- Finite clock (`t0 + LOCK*clock`, `Tick`) → Task 2. ✅
- State-level `always` (window_integrity, mutual_exclusion, lock_consistency, delivery_count_bounded, spso_in_range) → Task 2 `properties`. ✅
- Transition asserts (spso/dcc/per-offset-dc monotonic, acknowledged_is_terminal) → Task 2 `assert_transition`. ✅
- Non-vacuity `sometimes` (advance_spso, acknowledge, archive, redeliver) → Task 2 `properties`. ✅
- `within_boundary` bounds design-unbounded dims only (not delivery_count) → Task 2. ✅
- `target_state_count` + `timeout` + cap-asserts + watchdog → Task 2 `run` + `Invoke-GuardedModelTest`. ✅
- Two configs (concurrency + failover) + max_inflight knob → Tasks 2–3 (3 tests). ✅
- Reload failover → Task 3. ✅
- Empirical scale-up / OOM discipline → Task 4. ✅
- `Produce`/`Materialize` split (refinement) → reconciled in spec; Task 2. ✅

**2. Placeholder scan:** No TBD/TODO; every code step shows complete code; every run step shows the exact command + expected output. ✅

**3. Type consistency:** `ShareState`/`ShareAction`/`ShareModel` field and variant names, method signatures (`acquire`/`acknowledge`/`renew`/`materialize`/`expire_locks`/`to_persist_batches`/`load_from`), and helper names (`offset_state`/`offset_dc`/`acquired_runs`/`window_integrity`/`mutual_exclusion`/`lock_consistency`/`assert_transition`) are consistent across Tasks 1–4 and match the real `AcquisitionState` API in `state.rs`. The action field `last` is bound as `hi` in `next_state` to avoid shadowing the `last: &Self::State` parameter. ✅
