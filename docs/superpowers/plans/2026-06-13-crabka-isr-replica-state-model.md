# ISR / replica-state stateright model — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **OOM CAUTION — read before Task 2.** This plan runs a stateright BFS checker.
> stateright keeps every visited unique state resident in RAM; an under-bounded
> run OOM'd this machine once (`memory/feedback_bound_model_checkers.md`).
> Therefore: **every checker run (Tasks 2–3) MUST go through the PowerShell
> memory watchdog, and a subagent must NEVER run the checker unguarded.** The
> recommended execution mode is **inline by the main agent**, as with the raft
> and share-group models. Build with `--no-run` first, then run under the
> watchdog.

**Goal:** Exhaustively model-check the pure leader-side replication core (`ReplicaState`) with stateright, proving no-committed-data-loss (`hw ≤ every ISR member's LEO`), HWM monotonicity, and overshoot clamping under every interleaving of leader append, follower fetch, and ISR shrink/expand.

**Architecture:** A **wrap-real** stateright `Model` whose state embeds the *real* `ReplicaState` and whose `next_state` drives the production `install_isr`/`update_follower_leo`/`recompute_hw_for_leader_append`. The model lives inside the broker crate as a `#[cfg(test)]` descendant module of `replica_state` (reads `pub(crate)` fields directly). The single safety prerequisite is making the core deterministic by injecting `now: Instant` (which also de-flakes two existing `thread::sleep` unit tests). The state is hashed over a normalized projection (the core holds non-`Hash` `HashMap`/`HashSet`); HWM monotonicity is an imperative `next_state` assert; the controller's "only admit caught-up replicas" rule is an `install_isr` action precondition.

**Tech Stack:** Rust, `stateright = "=0.31.0"` (already a broker dev-dep), `cargo test --lib`, PowerShell memory watchdog.

**Spec:** `docs/superpowers/specs/2026-06-13-crabka-isr-replica-state-model-design.md`

---

## File Structure

| File | Responsibility |
| --- | --- |
| `crates/broker/src/replica_state.rs` | (modify) inject `now: Instant` into `install_isr`/`update_follower_leo`; add `#[derive(Clone, Debug)]` to `ReplicaState`; update + de-flake the unit tests; declare `#[cfg(test)] #[path="replica_state_model.rs"] mod replica_state_model;`. |
| `crates/broker/src/partition.rs` | (modify) pass `std::time::Instant::now()` at the `install_isr` call (`:453`). |
| `crates/broker/src/handlers/fetch.rs` | (modify) pass `std::time::Instant::now()` at the `update_follower_leo` call (`:390`). |
| `crates/broker/src/partition_writer.rs` | (modify) pass `std::time::Instant::now()` at the test `install_isr` calls (`:632`, `:765`). |
| `crates/broker/src/replica_state_model.rs` | (create) the entire model: `IsrModel`/`IsrState`/`IsrAction`, the projection-based `Hash`/`Eq`, the `Model` impl, the watchdog-friendly `run` harness, and the `#[test]` configs. |

---

## Task 1: Inject `now` into `ReplicaState`, add derives + module wiring, de-flake unit tests

**Files:**
- Modify: `crates/broker/src/replica_state.rs` (core methods `:54`, `:73`; derive `:23`; test module `:130-279`; module decl at EOF)
- Modify: `crates/broker/src/partition.rs:453`
- Modify: `crates/broker/src/handlers/fetch.rs:390`
- Modify: `crates/broker/src/partition_writer.rs:632`, `:765`
- Create: `crates/broker/src/replica_state_model.rs` (smoke stub)

- [ ] **Step 1: Add `Clone, Debug` derive to `ReplicaState`**

In `crates/broker/src/replica_state.rs`, change the struct attribute (currently `ReplicaState` has no derive):

```rust
#[derive(Clone, Debug)]
pub(crate) struct ReplicaState {
    pub(crate) isr: HashSet<NodeId>,
    pub(crate) per_follower: HashMap<NodeId, FollowerStats>,
    pub(crate) hw: i64,
    pub(crate) current_leader_epoch: i32,
}
```

(`FollowerStats` already derives `Debug, Clone, Copy`. `Hash`/`Eq` are intentionally NOT derived — `HashMap`/`HashSet` aren't `Hash`; the model hand-implements them over a projection.)

- [ ] **Step 2: Inject `now` into `install_isr`**

Change the signature (add `now: Instant`) and delete the internal `let now = Instant::now();`:

```rust
    pub(crate) fn install_isr(
        &mut self,
        isr: &[NodeId],
        replicas: &[NodeId],
        leader: NodeId,
        now: Instant,
    ) {
        self.isr = isr.iter().copied().collect();
        // Seed only ISR members: seeding a non-ISR replica with
        // `last_caught_up = now` would let `isr_maintenance` falsely
        // re-admit a replica that has not actually fetched up to the LEO.
        for &r in isr {
            if r != leader {
                self.per_follower.entry(r).or_insert(FollowerStats {
                    leo: 0,
                    last_fetch: now,
                    last_caught_up: now,
                });
            }
        }
        let keep: HashSet<NodeId> = replicas.iter().copied().collect();
        self.per_follower.retain(|k, _| keep.contains(k));
    }
```

- [ ] **Step 3: Inject `now` into `update_follower_leo`**

Change the signature (add `now: Instant`) and delete the internal `let now = Instant::now();` (the body already uses `now`):

```rust
    pub(crate) fn update_follower_leo(
        &mut self,
        follower: NodeId,
        follower_leo: i64,
        leader_leo: i64,
        now: Instant,
    ) -> i64 {
        if !self.isr.contains(&follower) {
            // Track stats so isr_maintenance can expand back when caught up.
            let stats = self.per_follower.entry(follower).or_insert(FollowerStats {
                leo: 0,
                last_fetch: now,
                last_caught_up: now,
            });
            stats.last_fetch = now;
            stats.leo = follower_leo.min(leader_leo);
            if stats.leo >= leader_leo {
                stats.last_caught_up = now;
            }
            return self.recompute_hw_for_leader_append(leader_leo);
        }
        let clamped = follower_leo.min(leader_leo);
        let stats = self.per_follower.entry(follower).or_insert(FollowerStats {
            leo: 0,
            last_fetch: now,
            last_caught_up: now,
        });
        stats.leo = clamped;
        stats.last_fetch = now;
        if clamped >= leader_leo {
            stats.last_caught_up = now;
        }
        self.hw = self.compute_hw(leader_leo);
        self.hw
    }
```

(`recompute_hw_for_leader_append` and `compute_hw` are unchanged — no `Instant`.)

- [ ] **Step 4: Update the production callers**

`crates/broker/src/partition.rs:453` — inside `Partition::install_isr`, change:
```rust
        st.install_isr(isr, replicas, leader);
```
to:
```rust
        st.install_isr(isr, replicas, leader, std::time::Instant::now());
```

`crates/broker/src/handlers/fetch.rs:390` — change:
```rust
                    let new = st.update_follower_leo(
                        u64::try_from(effective_replica_id).unwrap_or(0),
                        fetch_offset,
                        leader_leo,
                    );
```
to:
```rust
                    let new = st.update_follower_leo(
                        u64::try_from(effective_replica_id).unwrap_or(0),
                        fetch_offset,
                        leader_leo,
                        std::time::Instant::now(),
                    );
```

- [ ] **Step 5: Update the `partition_writer.rs` test callers**

`crates/broker/src/partition_writer.rs:632` — change `st.install_isr(&[1], &[1], 1);` to `st.install_isr(&[1], &[1], 1, std::time::Instant::now());`

`crates/broker/src/partition_writer.rs:765` — change `st.install_isr(&[1, 2, 3], &[1, 2, 3], 1);` to `st.install_isr(&[1, 2, 3], &[1, 2, 3], 1, std::time::Instant::now());`

- [ ] **Step 6: Update the `replica_state.rs` unit tests + de-flake the two timing tests**

In the `#[cfg(test)] mod tests` block (`replica_state.rs:130`), add a `Duration`/`Instant` import and a `now()` helper right under `use super::*;`:

```rust
    use super::*;
    use assert2::assert;
    use std::time::{Duration, Instant};

    fn now() -> Instant {
        Instant::now()
    }
```

Then append `, now()` as the final argument to **every** `install_isr(...)` and `update_follower_leo(...)` call in the test module **except** the two timing tests rewritten below. Concretely, the affected calls are at lines (pre-edit) 150, 160, 161, 162, 163, 173, 174, 175, 185, 186, 188, 199, 200, 202, 204, 211, 212, 213, 220, 224, 232, 240, 241. (`recompute_hw_for_leader_append(...)` calls at 233 and 249 are NOT changed — that method has no `now`.) For example:
- `s.install_isr(&[1, 2, 3], &[1, 2, 3], 1);` → `s.install_isr(&[1, 2, 3], &[1, 2, 3], 1, now());`
- `s.update_follower_leo(2, 50, 100);` → `s.update_follower_leo(2, 50, 100, now());`

Replace the two timing tests in full (no more `thread::sleep` — they pass explicit ordered instants):

```rust
    #[test]
    fn update_follower_leo_advances_last_fetch_time() {
        let mut s = fresh();
        let t0 = Instant::now();
        s.install_isr(&[1, 2], &[1, 2], 1, t0);
        let t_install = s.per_follower.get(&2).unwrap().last_fetch;
        let t1 = t0 + Duration::from_millis(10);
        s.update_follower_leo(2, 5, 10, t1);
        let t_after = s.per_follower.get(&2).unwrap().last_fetch;
        assert!(t_after > t_install);
    }

    #[test]
    fn last_caught_up_set_when_leo_reaches_leader_leo() {
        let mut s = fresh();
        let t0 = Instant::now();
        s.install_isr(&[1, 2], &[1, 2], 1, t0);
        let t1 = t0 + Duration::from_millis(10);
        s.update_follower_leo(2, 5, 10, t1);
        let lag = s.per_follower.get(&2).unwrap().last_caught_up;
        let lag_fetch = s.per_follower.get(&2).map(|f| f.last_fetch).unwrap();
        // Not yet caught up — last_caught_up is the install time (t0), which is
        // strictly before the most recent fetch time (t1).
        assert!(lag <= lag_fetch);
        let t2 = t1 + Duration::from_millis(10);
        s.update_follower_leo(2, 10, 10, t2);
        let lag2 = s.per_follower.get(&2).unwrap().last_caught_up;
        assert!(lag2 > lag);
    }
```

- [ ] **Step 7: Declare the model module**

At the very end of `crates/broker/src/replica_state.rs` (after the `#[cfg(test)] mod tests { ... }` block):

```rust
#[cfg(test)]
#[path = "replica_state_model.rs"]
mod replica_state_model;
```

- [ ] **Step 8: Create the smoke-stub model file**

Create `crates/broker/src/replica_state_model.rs` (replaced wholesale in Task 2):

```rust
//! Exhaustive stateright model of the pure leader-side replication core
//! (`ReplicaState`). See
//! `docs/superpowers/specs/2026-06-13-crabka-isr-replica-state-model-design.md`.
//!
//! The full model (types, `Model` impl, properties, configs) is added in
//! Task 2. This file starts as a wiring smoke test.

use std::time::Instant;

use super::ReplicaState;

#[test]
fn core_wiring_smoke() {
    // Drive the real core with an injected `now`, proving the descendant module
    // can reach the pub(crate) surface + that `now` injection compiles.
    let t0 = Instant::now();
    let mut s = ReplicaState::new();
    s.install_isr(&[1, 2, 3], &[1, 2, 3], 1, t0);
    let hw = s.update_follower_leo(2, 5, 10, t0);
    assert_eq!(hw, 0); // follower 3 still at LEO 0 pins the HW
    let clone = s.clone();
    assert_eq!(clone.hw, s.hw);
}
```

- [ ] **Step 9: Build and run**

```
cargo test -p crabka-broker --lib --no-run
```
Expected: compiles cleanly (the `now` threading + derives + module build).

```
cargo test -p crabka-broker --lib replica_state -- --nocapture
```
Expected: all `replica_state::tests::*` unit tests pass (including the two de-flaked timing tests, now sleep-free) **and** `replica_state::replica_state_model::core_wiring_smoke` passes.

- [ ] **Step 10: Commit**

```
git add crates/broker/src/replica_state.rs crates/broker/src/partition.rs crates/broker/src/handlers/fetch.rs crates/broker/src/partition_writer.rs crates/broker/src/replica_state_model.rs
git commit -m "refactor(broker): inject now into ReplicaState + wire stateright model module"
```

---

## Task 2: The replica-state model + safety property suite + two configs

**Files:**
- Modify (replace contents): `crates/broker/src/replica_state_model.rs`

- [ ] **Step 1: Write the complete model file**

Replace the entire contents of `crates/broker/src/replica_state_model.rs` with:

```rust
//! Exhaustive stateright model of the pure leader-side replication core
//! (`ReplicaState`).
//!
//! The model state holds the REAL `ReplicaState` and drives the production
//! `install_isr` / `update_follower_leo` / `recompute_hw_for_leader_append`;
//! the BFS checker explores every interleaving of leader append, follower
//! fetch, and ISR shrink/expand, asserting the partition-replication safety
//! invariants never break — above all no-committed-data-loss. Design:
//! `docs/superpowers/specs/2026-06-13-crabka-isr-replica-state-model-design.md`.
//!
//! Memory safety: stateright BFS keeps every visited unique state resident, so
//! each run is fenced with `within_boundary` + `target_state_count` + `timeout`
//! and MUST be executed under the host memory watchdog while bounds are tuned.

use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant};

use crabka_raft::NodeId;
use stateright::{Checker, Model, Property};

use super::ReplicaState;

/// Hard backstop on generated states — bounds host memory even if
/// `within_boundary` is looser than intended.
const MAX_STATES: usize = 200_000;
/// Depth backstop; must exceed each config's reachable-graph diameter.
const MAX_DEPTH: usize = 80;
/// Wall-clock backstop.
const CHECK_TIMEOUT: Duration = Duration::from_secs(120);

/// Bounded model config (held here, not in the fingerprinted state).
struct IsrModel {
    /// Constant injected `now` — the model does not model wall-clock time
    /// (ISR shrink/expand is an explicit action, not a time-based decision).
    t0: Instant,
    /// `replicas[0]` is the fixed leader; the rest are followers.
    replicas: Vec<NodeId>,
    /// Leader-LEO / follower-LEO cap.
    max_offset: i64,
    /// When set, followers may report a LEO above `leader_leo` (clamp test).
    test_overshoot: bool,
}

impl IsrModel {
    fn safety(max_offset: i64) -> Self {
        Self {
            t0: Instant::now(),
            replicas: vec![1, 2, 3],
            max_offset,
            test_overshoot: false,
        }
    }

    fn overshoot(max_offset: i64) -> Self {
        Self {
            t0: Instant::now(),
            replicas: vec![1, 2, 3],
            max_offset,
            test_overshoot: true,
        }
    }

    fn leader(&self) -> NodeId {
        self.replicas[0]
    }

    fn followers(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.replicas[1..].iter().copied()
    }
}

/// The fingerprinted model state: the REAL core + the leader's own LEO.
#[derive(Clone, Debug)]
struct IsrState {
    rs: ReplicaState,
    leader_leo: i64,
}

impl IsrState {
    /// Normalized, timestamp-free projection used for Eq/Hash: the real state
    /// holds non-`Hash` `HashMap`/`HashSet` and non-deterministic timestamps,
    /// neither of which is safety-relevant here.
    fn project(&self) -> (Vec<NodeId>, Vec<(NodeId, i64)>, i64, i32, i64) {
        let mut isr: Vec<NodeId> = self.rs.isr.iter().copied().collect();
        isr.sort_unstable();
        let mut pf: Vec<(NodeId, i64)> = self
            .rs
            .per_follower
            .iter()
            .map(|(k, v)| (*k, v.leo))
            .collect();
        pf.sort_unstable();
        (
            isr,
            pf,
            self.rs.hw,
            self.rs.current_leader_epoch,
            self.leader_leo,
        )
    }
}

impl PartialEq for IsrState {
    fn eq(&self, other: &Self) -> bool {
        self.project() == other.project()
    }
}
impl Eq for IsrState {}
impl Hash for IsrState {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.project().hash(state);
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum IsrAction {
    /// Leader appends one record (leader_leo += 1) and recomputes HW.
    LeaderAppend,
    /// A follower reports `leo` via fetch.
    FollowerFetch { follower: NodeId, leo: i64 },
    /// The controller installs a new committed ISR.
    InstallIsr { isr: Vec<NodeId> },
}

impl Model for IsrModel {
    type State = IsrState;
    type Action = IsrAction;

    fn init_states(&self) -> Vec<Self::State> {
        // Fresh leader: full replica set in the ISR, followers seeded at 0.
        let mut rs = ReplicaState::new();
        rs.install_isr(&self.replicas, &self.replicas, self.leader(), self.t0);
        vec![IsrState { rs, leader_leo: 0 }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        let leader = self.leader();

        if state.leader_leo < self.max_offset {
            actions.push(IsrAction::LeaderAppend);
        }

        // Follower fetches: advance by one or jump to the leader's LEO. Targets
        // are monotonic (never below the follower's current LEO) — a real
        // follower's reported LEO never regresses, which is what keeps HW
        // monotone. `test_overshoot` additionally probes the defensive clamp.
        for f in self.followers() {
            let cur = state.rs.per_follower.get(&f).map(|s| s.leo).unwrap_or(0);
            let mut targets: Vec<i64> = Vec::new();
            if cur < state.leader_leo {
                targets.push(cur + 1);
                targets.push(state.leader_leo);
            }
            if self.test_overshoot {
                targets.push(state.leader_leo + 1);
            }
            targets.sort_unstable();
            targets.dedup();
            for leo in targets {
                actions.push(IsrAction::FollowerFetch { follower: f, leo });
            }
        }

        // ISR changes: every subset of replicas that contains the leader and
        // differs from the current ISR. Expansion only admits caught-up
        // followers (per_follower.leo >= hw) — the controller's real rule;
        // without it the model would report a false data-loss violation.
        let cur_isr: HashSet<NodeId> = state.rs.isr.clone();
        let follower_vec: Vec<NodeId> = self.followers().collect();
        for mask in 0u32..(1u32 << follower_vec.len()) {
            let mut isr: Vec<NodeId> = vec![leader];
            for (i, &f) in follower_vec.iter().enumerate() {
                if mask & (1 << i) != 0 {
                    isr.push(f);
                }
            }
            let isr_set: HashSet<NodeId> = isr.iter().copied().collect();
            if isr_set == cur_isr {
                continue;
            }
            let expansion_ok = isr
                .iter()
                .filter(|&&n| n != leader && !cur_isr.contains(&n))
                .all(|f| state.rs.per_follower.get(f).map(|s| s.leo).unwrap_or(0) >= state.rs.hw);
            if !expansion_ok {
                continue;
            }
            isr.sort_unstable();
            actions.push(IsrAction::InstallIsr { isr });
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = last.clone();
        match action {
            IsrAction::LeaderAppend => {
                if state.leader_leo >= self.max_offset {
                    return None;
                }
                state.leader_leo += 1;
                state.rs.recompute_hw_for_leader_append(state.leader_leo);
            }
            IsrAction::FollowerFetch { follower, leo } => {
                state
                    .rs
                    .update_follower_leo(follower, leo, state.leader_leo, self.t0);
            }
            IsrAction::InstallIsr { isr } => {
                state
                    .rs
                    .install_isr(&isr, &self.replicas, self.leader(), self.t0);
            }
        }
        // Transition invariant (kept out of the fingerprinted state): the
        // high-watermark never regresses.
        assert!(
            state.rs.hw >= last.rs.hw,
            "HWM regressed: {} -> {}",
            last.rs.hw,
            state.rs.hw
        );
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("hw_within_leader", |_, s: &IsrState| {
                s.rs.hw <= s.leader_leo
            }),
            // No-committed-data-loss: every ISR member holds every committed
            // record. A missing per_follower entry for an ISR member counts as a
            // violation (compute_hw skips entryless members).
            Property::always("no_data_loss", |m: &IsrModel, s: &IsrState| {
                let leader = m.leader();
                s.rs.isr.iter().filter(|&&f| f != leader).all(|f| {
                    s.rs.per_follower
                        .get(f)
                        .map(|st| st.leo >= s.rs.hw)
                        .unwrap_or(false)
                })
            }),
            Property::always("leo_clamped", |_, s: &IsrState| {
                s.rs.per_follower.values().all(|st| st.leo <= s.leader_leo)
            }),
            Property::always("hw_nonneg", |_, s: &IsrState| s.rs.hw >= 0),
            Property::always("leader_in_isr", |m: &IsrModel, s: &IsrState| {
                s.rs.isr.contains(&m.leader())
            }),
            Property::sometimes("can_advance_hw", |_, s: &IsrState| s.rs.hw > 0),
            Property::sometimes("can_reach_leader_leo", |_, s: &IsrState| {
                s.leader_leo > 0 && s.rs.hw == s.leader_leo
            }),
            Property::sometimes("can_pin_below_leader", |_, s: &IsrState| {
                s.rs.hw > 0 && s.rs.hw < s.leader_leo
            }),
            Property::sometimes("can_shrink_isr", |m: &IsrModel, s: &IsrState| {
                let leader = m.leader();
                m.replicas.iter().any(|&r| {
                    r != leader && !s.rs.isr.contains(&r) && s.rs.per_follower.contains_key(&r)
                })
            }),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.leader_leo <= self.max_offset
            && state.rs.hw <= self.max_offset
            && state
                .rs
                .per_follower
                .values()
                .all(|s| s.leo <= self.max_offset)
    }
}

/// Run one bounded config to completion; assert it was exhaustive (not
/// cap/depth-truncated) and that all properties hold.
fn run(model: IsrModel, label: &str) {
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
        "[{label}] hit depth cap {MAX_DEPTH}: depth-truncated, not exhaustive"
    );
    assert!(
        checker.state_count() < MAX_STATES,
        "[{label}] hit state cap {MAX_STATES}: truncated, not exhaustive"
    );
    checker.assert_properties();
}

#[test]
fn isr_safety() {
    run(IsrModel::safety(2), "isr_safety");
}

#[test]
fn isr_overshoot() {
    run(IsrModel::overshoot(2), "isr_overshoot");
}
```

- [ ] **Step 2: Build (no run)**

```
cargo test -p crabka-broker --lib --no-run
```
Expected: compiles cleanly.

- [ ] **Step 3: Define the memory watchdog (run once in the PowerShell session)**

```powershell
function Invoke-GuardedExe {
    param([string]$Exe, [string]$Filter)
    $out = New-TemporaryFile; $err = New-TemporaryFile
    $p = Start-Process -FilePath $Exe -ArgumentList @($Filter, '--nocapture', '--test-threads=1') `
        -PassThru -NoNewWindow -RedirectStandardOutput $out.FullName -RedirectStandardError $err.FullName
    $limit = 3GB; $deadline = 150; $elapsed = 0.0; $peak = 0
    while (-not $p.HasExited) {
        Start-Sleep -Milliseconds 600; $elapsed += 0.6
        try { $rss = (Get-Process -Id $p.Id -ErrorAction Stop).WorkingSet64 } catch { break }
        if ($rss -gt $peak) { $peak = $rss }
        if ($rss -gt $limit) { Write-Host "WATCHDOG KILL: RSS > 3GB"; Stop-Process -Id $p.Id -Force; break }
        if ($elapsed -gt $deadline) { Write-Host "WATCHDOG KILL: timeout"; Stop-Process -Id $p.Id -Force; break }
    }
    $p | Wait-Process
    Write-Host "--- [$Filter] exit=$($p.ExitCode) peakRSS=$([math]::Round($peak/1MB,1))MB ---"
    Get-Content $err.FullName; Get-Content $out.FullName
    Remove-Item $out.FullName, $err.FullName -ErrorAction SilentlyContinue
}
```

- [ ] **Step 4: Run both configs under the watchdog**

```powershell
$exe = (Get-ChildItem target\debug\deps\crabka_broker-*.exe | Sort-Object LastWriteTime -Descending | Select-Object -First 1).FullName
Invoke-GuardedExe $exe 'replica_state_model::isr_safety'
Invoke-GuardedExe $exe 'replica_state_model::isr_overshoot'
```
Expected for each: the `[label] unique_states=… …` line prints, `unique_states` is small (low thousands), no cap-assert fires, all `always`/`sometimes` properties pass, `exit code: 0`. If `unique_states` exceeds ~50k or the watchdog kills the run, STOP and tighten before continuing.

- [ ] **Step 5: Commit**

```
git add crates/broker/src/replica_state_model.rs
git commit -m "test(broker): stateright model of the ISR / replica-state core"
```

---

## Task 3: Empirical scale-up + final verification

**Files:**
- Modify: `crates/broker/src/replica_state_model.rs` (only if scale-up kept)

- [ ] **Step 1: Record the baseline counts** from Task 2 (both should be small, low thousands at `max_offset=2`).

- [ ] **Step 2: Attempt `max_offset = 3`**

Change both tests to `max_offset = 3`:
```rust
#[test]
fn isr_safety() {
    run(IsrModel::safety(3), "isr_safety");
}

#[test]
fn isr_overshoot() {
    run(IsrModel::overshoot(3), "isr_overshoot");
}
```

Build, then run both under the watchdog:
```
cargo test -p crabka-broker --lib --no-run
```
```powershell
$exe = (Get-ChildItem target\debug\deps\crabka_broker-*.exe | Sort-Object LastWriteTime -Descending | Select-Object -First 1).FullName
Invoke-GuardedExe $exe 'replica_state_model::isr_safety'
Invoke-GuardedExe $exe 'replica_state_model::isr_overshoot'
```

**Decision rule:** if both complete with `unique_states < 100_000`, no cap-assert fires, the watchdog does not kill them, and all properties pass → **keep** `max_offset = 3`. Otherwise **revert** to `max_offset = 2`. If `max_depth` approaches `MAX_DEPTH` (80) without truncating, fine; if a cap-assert reports depth truncation, raise `MAX_DEPTH` to 120 and re-run.

- [ ] **Step 3: Final guarded full run**

```powershell
$exe = (Get-ChildItem target\debug\deps\crabka_broker-*.exe | Sort-Object LastWriteTime -Descending | Select-Object -First 1).FullName
Invoke-GuardedExe $exe 'replica_state_model::'
```
Expected: both configs pass, every `unique_states` below the kept threshold, `exit code: 0`.

- [ ] **Step 4: Confirm the broader `replica_state` surface is unaffected**

```
cargo test -p crabka-broker --lib replica_state
```
Expected: all `replica_state::tests::*` (incl. the de-flaked timing tests) and `replica_state::replica_state_model::*` pass. (Self-bounded by the in-test caps — safe for CI to run unguarded.)

- [ ] **Step 5: Format**

```
cargo fmt -p crabka-broker
```

- [ ] **Step 6: Commit**

```
git add crates/broker/src/replica_state_model.rs
git commit -m "test(broker): tune ISR model bounds + final verification"
```
(If Step 2 reverted to `max_offset = 2` and `cargo fmt` produced no diff, there may be nothing to commit — skip this commit.)

- [ ] **Step 7: Update the program memory note**

Update `project_stateright_testing_program.md` to record that the ISR/`ReplicaState` model is implemented (Workstream A now covers raft consensus + share-group acquisition + ISR/HWM replication), leaving reassignment (KIP-455), unclean recovery (KIP-966), and dynamic voters (KIP-853, still blocked on the core) as remaining model candidates. (Memory edit only; not a git commit.)

---

## Self-Review (completed by plan author)

**1. Spec coverage:**
- Wrap-real `ReplicaState` driving real methods → Task 2 `next_state`. ✅
- `now` injection into `install_isr`/`update_follower_leo` + caller updates → Task 1 Steps 2–5. ✅
- De-flake the two `thread::sleep` unit tests → Task 1 Step 6. ✅
- `#[derive(Clone, Debug)]` on `ReplicaState` → Task 1 Step 1. ✅
- In-src `#[cfg(test)]` descendant module → Task 1 Step 7. ✅
- Projection-based `Hash`/`Eq` → Task 2 `IsrState::project` + impls. ✅
- Actions (LeaderAppend / FollowerFetch / InstallIsr) with monotonic-LEO + caught-up-expansion preconditions → Task 2 `actions`. ✅
- State-level `always` (hw_within_leader, no_data_loss, leo_clamped, hw_nonneg, leader_in_isr) → Task 2 `properties`. ✅
- Transition assert (hw_monotonic) → Task 2 `next_state`. ✅
- Non-vacuity `sometimes` (advance_hw, reach_leader_leo, pin_below_leader, shrink_isr) → Task 2 `properties`. ✅
- `within_boundary` design-unbounded dims only → Task 2. ✅
- target_state_count + timeout + cap-asserts + watchdog → Task 2 `run` + `Invoke-GuardedExe`. ✅
- Two configs (safety + overshoot) → Tasks 2–3. ✅
- Empirical scale-up / OOM discipline → Task 3. ✅
- Initial state (fresh leader, full ISR, seeded at 0) → Task 2 `init_states`. ✅

**2. Placeholder scan:** No TBD/TODO; every code step shows complete code; every run step shows the exact command + expected output. The Task 1 Step 6 mechanical test-call updates list the exact line numbers and the exact `, now()` argument with examples. ✅

**3. Type consistency:** `IsrModel`/`IsrState`/`IsrAction` field+variant names, the `now`-injected signatures (`install_isr(.., now: Instant)`, `update_follower_leo(.., now: Instant)`), `ReplicaState` field names (`isr`/`per_follower`/`hw`/`current_leader_epoch`), and `FollowerStats::leo` are consistent across Tasks 1–3 and match `replica_state.rs`. `NodeId = u64` (`crabka_raft::NodeId`). ✅
