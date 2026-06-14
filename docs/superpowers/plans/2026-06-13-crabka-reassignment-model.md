# Partition-reassignment (KIP-455) stateright model — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **OOM CAUTION (Tasks 2–3).** This plan runs a stateright BFS checker. stateright keeps every visited unique state resident; an under-bounded run OOM'd this machine once (`memory/feedback_bound_model_checkers.md`). Every checker run MUST go through the PowerShell memory watchdog, and a subagent must NEVER run the checker unguarded. Recommended execution mode: **inline by the main agent** (as with the raft / share / ISR / failover models). Build with `--no-run` first, then run under the watchdog.
>
> **FMT (Task 3).** CI's fmt gate is **nightly** (`cargo +nightly fmt -- --check`); stable `cargo fmt` silently skips the workspace's unstable rustfmt options. Always format with `cargo +nightly fmt -p crabka-broker` (`memory/reference_windows_fmt_path_length.md`).

**Goal:** Exhaustively model-check the pure KIP-455 reassignment-completion core (`reassign_one`, extracted from `compute_reassignment_progress`) with stateright, proving the replica set never switches off the leader (handoff-before-removal), the ISR stays a subset of the replica set, and the reassignment converges — under every interleaving of replica catch-up, broker liveness, and completion ticks.

**Architecture:** Extract the per-partition decision into a pure sync `reassign_one` (same pattern as `failover_one`), then an in-src `#[cfg(test)]` wrap-real stateright model driving the real `reassign_one` over a single partition's reassignment lifecycle. Safety is checked per-decision in `next_state` plus structural `always` invariants.

**Tech Stack:** Rust, `stateright = "=0.31.0"` (already a broker dev-dep on main), `cargo test --lib`, PowerShell memory watchdog, nightly `cargo fmt`.

**Spec:** `docs/superpowers/specs/2026-06-13-crabka-reassignment-model-design.md`

---

## File Structure

| File | Responsibility |
| --- | --- |
| `crates/broker/src/reassignment.rs` | (modify) add `reassign_one`; rewrite `compute_reassignment_progress` to call it; declare `#[cfg(test)] #[path="reassignment_model.rs"] mod reassignment_model;`. |
| `crates/broker/src/reassignment_model.rs` | (create) the model: `ReassignModel`/`ReassignState`/`ReassignAction`, the `Model` impl + `assert_step`, the watchdog-friendly `run` harness, and the `#[test]` configs. |

---

## Task 1: Extract `reassign_one` + rewrite `compute_reassignment_progress` + wire model module

**Files:**
- Modify: `crates/broker/src/reassignment.rs` (insert before `compute_reassignment_progress` at line ~92; rewrite its body lines ~98-160; module decl at EOF)
- Create: `crates/broker/src/reassignment_model.rs` (smoke stub)

- [ ] **Step 1: Add `reassign_one`**

In `crates/broker/src/reassignment.rs`, immediately **before** the doc comment
`/// Pure logic: scan every in-flight reassignment;` (the one above
`pub(crate) async fn compute_reassignment_progress`), insert:

```rust
/// The pure per-partition reassignment decision: given a partition's current
/// record and the alive set, return the next `PartitionRecord` (a leader
/// handoff or a completion), or `None` to wait. No I/O. Extracted from
/// `compute_reassignment_progress` so the policy is independently unit-testable
/// and model-checkable.
pub(crate) fn reassign_one(
    pr: &PartitionRecord,
    alive: &std::collections::HashSet<NodeId>,
) -> Option<PartitionRecord> {
    let target: Vec<NodeId> = pr
        .replicas
        .iter()
        .filter(|r| !pr.removing_replicas.contains(r))
        .copied()
        .collect();
    if !pr.adding_replicas.iter().all(|n| pr.isr.contains(n)) {
        return None; // wait for replication
    }
    if pr.removing_replicas.contains(&pr.leader) {
        // Leader-handoff phase: pick a new leader from target ∩ isr ∩ alive.
        let new_leader = *target
            .iter()
            .find(|n| pr.isr.contains(n) && alive.contains(n))?;
        return Some(PartitionRecord {
            topic: pr.topic.clone(),
            partition: pr.partition,
            leader: new_leader,
            leader_epoch: pr.leader_epoch + 1,
            replicas: pr.replicas.clone(),
            isr: pr.isr.clone(),
            adding_replicas: pr.adding_replicas.clone(),
            removing_replicas: pr.removing_replicas.clone(),
            directories: pr.directories.clone(),
            partition_epoch: pr.partition_epoch + 1,
        });
    }
    // Completion phase: switch to the target replica set.
    let new_isr: Vec<NodeId> = pr
        .isr
        .iter()
        .filter(|n| target.contains(n))
        .copied()
        .collect();
    let new_directories = remap_directories(&pr.replicas, &pr.directories, &target);
    Some(PartitionRecord {
        topic: pr.topic.clone(),
        partition: pr.partition,
        leader: pr.leader,
        leader_epoch: pr.leader_epoch, // unchanged: leader stays, only replica set changes
        replicas: target,
        isr: new_isr,
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: new_directories,
        partition_epoch: pr.partition_epoch + 1,
    })
}
```

- [ ] **Step 2: Rewrite `compute_reassignment_progress` to call `reassign_one`**

Replace the body of `compute_reassignment_progress` — currently:

```rust
    let mut updates = Vec::new();
    // Snapshot the alive set once (single lock) instead of taking the
    // liveness lock per target replica in the leader-handoff branch.
    let alive = liveness.alive_snapshot().await;
    for pr in image.reassignments_in_flight() {
        let target: Vec<NodeId> = pr
            .replicas
            .iter()
            .filter(|r| !pr.removing_replicas.contains(r))
            .copied()
            .collect();
        let adding_caught_up = pr.adding_replicas.iter().all(|n| pr.isr.contains(n));
        if !adding_caught_up {
            continue; // wait for replication
        }
        if pr.removing_replicas.contains(&pr.leader) {
            // Leader handoff phase. Find an eligible new leader in target ∩ isr that is alive.
            let mut new_leader: Option<NodeId> = None;
            for n in &target {
                if pr.isr.contains(n) && alive.contains(n) {
                    new_leader = Some(*n);
                    break;
                }
            }
            if let Some(leader) = new_leader {
                updates.push(MetadataRecord::V1Partition(PartitionRecord {
                    topic: pr.topic.clone(),
                    partition: pr.partition,
                    leader,
                    leader_epoch: pr.leader_epoch + 1,
                    replicas: pr.replicas.clone(),
                    isr: pr.isr.clone(),
                    adding_replicas: pr.adding_replicas.clone(),
                    removing_replicas: pr.removing_replicas.clone(),
                    directories: pr.directories.clone(),
                    partition_epoch: pr.partition_epoch + 1,
                }));
            }
            // Whether or not we found a leader, don't also try to complete this tick.
            continue;
        }
        // Completion phase.
        let new_isr: Vec<NodeId> = pr
            .isr
            .iter()
            .filter(|n| target.contains(n))
            .copied()
            .collect();
        let new_directories = remap_directories(&pr.replicas, &pr.directories, &target);
        updates.push(MetadataRecord::V1Partition(PartitionRecord {
            topic: pr.topic.clone(),
            partition: pr.partition,
            leader: pr.leader,
            leader_epoch: pr.leader_epoch, // unchanged: leader stays, only replica set changes
            replicas: target,
            isr: new_isr,
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: new_directories,
            partition_epoch: pr.partition_epoch + 1,
        }));
    }
    updates
```

with:

```rust
    let mut updates = Vec::new();
    // Snapshot the alive set once (single lock) instead of taking the
    // liveness lock per target replica in the leader-handoff branch.
    let alive = liveness.alive_snapshot().await;
    for pr in image.reassignments_in_flight() {
        if let Some(next) = reassign_one(pr, &alive) {
            updates.push(MetadataRecord::V1Partition(next));
        }
    }
    updates
```

- [ ] **Step 3: Verify the refactor is behavior-preserving**

```
cargo test -p crabka-broker --lib reassignment
```
Expected: all `reassignment::tests::*` pass (the 10 existing tests:
`complete_when_adding_in_isr_writes_target`, `leader_handoff_when_leader_in_removing`,
`wait_when_adding_not_in_isr`, `target_includes_only_replicas_minus_removing`, …),
confirming the extraction changed no behavior.

- [ ] **Step 4: Declare the model module + create the smoke stub**

At the very end of `crates/broker/src/reassignment.rs` (after the
`#[cfg(test)] mod tests { ... }` block):

```rust
#[cfg(test)]
#[path = "reassignment_model.rs"]
mod reassignment_model;
```

Create `crates/broker/src/reassignment_model.rs`:

```rust
//! Exhaustive stateright model of the pure KIP-455 reassignment-completion core
//! (`reassign_one`). See
//! `docs/superpowers/specs/2026-06-13-crabka-reassignment-model-design.md`.
//!
//! The full model is added in Task 2. This file starts as a wiring smoke test.
//!
//! Memory safety: stateright BFS keeps every visited unique state resident, so
//! each checker run is fenced with `within_boundary` + `target_state_count` +
//! `timeout` and MUST be run under the host memory watchdog while bounds are
//! tuned.

use std::collections::HashSet;

use crabka_metadata::PartitionRecord;

use super::reassign_one;

#[test]
fn reassign_one_completes_smoke() {
    // replicas=[1,2,3] adding=[3] removing=[2], all in ISR, leader=1 → completes
    // to target [1,3].
    let pr = PartitionRecord {
        topic: "t".to_string(),
        partition: 0,
        leader: 1,
        replicas: vec![1, 2, 3],
        isr: vec![1, 2, 3],
        leader_epoch: 5,
        adding_replicas: vec![3],
        removing_replicas: vec![2],
        directories: vec![],
        partition_epoch: 0,
    };
    let alive: HashSet<u64> = [1, 2, 3].into_iter().collect();
    let next = reassign_one(&pr, &alive).expect("a completion update");
    assert_eq!(next.replicas, vec![1, 3]);
    assert_eq!(next.isr, vec![1, 3]);
    assert!(next.adding_replicas.is_empty());
    assert!(next.removing_replicas.is_empty());
    assert_eq!(next.leader, 1);
}
```

- [ ] **Step 5: Build + run the smoke test**

```
cargo test -p crabka-broker --lib --no-run
```
Expected: compiles cleanly.

```
cargo test -p crabka-broker --lib reassignment_model::reassign_one_completes_smoke -- --nocapture
```
Expected: PASS.

- [ ] **Step 6: Commit**

```
git add crates/broker/src/reassignment.rs crates/broker/src/reassignment_model.rs
git commit -m "refactor(broker): extract pure reassign_one + wire reassignment model module"
```

---

## Task 2: The reassignment model + safety asserts + two configs

**Files:**
- Modify (replace contents): `crates/broker/src/reassignment_model.rs`

- [ ] **Step 1: Write the complete model file**

Replace the entire contents of `crates/broker/src/reassignment_model.rs` with:

```rust
//! Exhaustive stateright model of the pure KIP-455 reassignment-completion core
//! (`reassign_one`).
//!
//! The model state holds a single partition's reassignment; `next_state` drives
//! the real `reassign_one`; the BFS checker explores every interleaving of
//! replica catch-up, broker liveness, and completion ticks, asserting the
//! reassignment-safety invariants — above all that the replica set never
//! switches off the leader. Design:
//! `docs/superpowers/specs/2026-06-13-crabka-reassignment-model-design.md`.
//!
//! Memory safety: stateright BFS keeps every visited unique state resident, so
//! each run is fenced with `within_boundary` + `target_state_count` + `timeout`
//! and MUST be executed under the host memory watchdog while bounds are tuned.

use std::collections::{BTreeSet, HashSet};
use std::time::Duration;

use crabka_metadata::PartitionRecord;
use crabka_raft::NodeId;
use stateright::{Checker, Model, Property};

use super::reassign_one;

const MAX_STATES: usize = 200_000;
const MAX_DEPTH: usize = 80;
const CHECK_TIMEOUT: Duration = Duration::from_secs(120);

/// Bounded config for the reassignment model (held here, not in the state).
struct ReassignModel {
    replicas: Vec<NodeId>,
    adding: Vec<NodeId>,
    removing: Vec<NodeId>,
    initial_isr: Vec<NodeId>,
    leader: NodeId,
    max_epoch: i32,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct ReassignState {
    replicas: Vec<NodeId>,
    isr: Vec<NodeId>, // canonical replica order
    adding: Vec<NodeId>,
    removing: Vec<NodeId>,
    leader: NodeId,
    leader_epoch: i32,
    alive: BTreeSet<NodeId>,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum ReassignAction {
    AdmitToIsr(NodeId),
    Die(NodeId),
    Revive(NodeId),
    ReassignStep,
}

impl ReassignModel {
    fn basic() -> Self {
        Self {
            replicas: vec![1, 2, 3],
            adding: vec![3],
            removing: vec![2],
            initial_isr: vec![1, 2],
            leader: 1, // not removed → no handoff
            max_epoch: 10,
        }
    }

    fn leader_handoff() -> Self {
        Self {
            replicas: vec![1, 2, 3],
            adding: vec![3],
            removing: vec![2],
            initial_isr: vec![1, 2],
            leader: 2, // in `removing` → handoff required before completion
            max_epoch: 10,
        }
    }
}

fn in_flight(s: &ReassignState) -> bool {
    !s.adding.is_empty() || !s.removing.is_empty()
}

/// The target replica set the reassignment converges to: replicas − removing.
fn target_of(s: &ReassignState) -> Vec<NodeId> {
    s.replicas
        .iter()
        .filter(|r| !s.removing.contains(r))
        .copied()
        .collect()
}

/// Build a `PartitionRecord` from the model state to drive the real
/// `reassign_one`. `directories` is irrelevant to the safety properties.
fn pr_of(s: &ReassignState) -> PartitionRecord {
    PartitionRecord {
        topic: "t".to_string(),
        partition: 0,
        leader: s.leader,
        replicas: s.replicas.clone(),
        isr: s.isr.clone(),
        leader_epoch: s.leader_epoch,
        adding_replicas: s.adding.clone(),
        removing_replicas: s.removing.clone(),
        directories: vec![],
        partition_epoch: 0,
    }
}

/// Verify a `reassign_one` decision against the pre-state. These are the
/// safety-critical invariants; they hold per-decision under any ordering.
fn assert_step(pre: &ReassignState, next: &PartitionRecord) {
    assert!(
        next.leader_epoch >= pre.leader_epoch,
        "leader_epoch regressed: {} -> {}",
        pre.leader_epoch,
        next.leader_epoch
    );
    assert!(
        pre.adding.iter().all(|n| pre.isr.contains(n)),
        "decision emitted before adding caught up: adding={:?} isr={:?}",
        pre.adding,
        pre.isr
    );
    let target = target_of(pre);
    if next.leader != pre.leader {
        // Handoff.
        assert!(pre.isr.contains(&next.leader), "handoff to non-ISR {}", next.leader);
        assert!(target.contains(&next.leader), "handoff to non-target {}", next.leader);
        assert!(pre.alive.contains(&next.leader), "handoff to dead {}", next.leader);
        assert!(
            !pre.removing.contains(&next.leader),
            "handoff to a removing replica {}",
            next.leader
        );
        assert!(next.replicas == pre.replicas, "handoff changed the replica set");
        assert!(next.adding_replicas == pre.adding, "handoff changed adding");
        assert!(next.removing_replicas == pre.removing, "handoff changed removing");
        assert!(
            next.leader_epoch == pre.leader_epoch + 1,
            "handoff did not bump leader_epoch by exactly 1"
        );
    } else if next.adding_replicas.is_empty() && next.removing_replicas.is_empty() {
        // Completion.
        assert!(
            next.replicas.contains(&next.leader),
            "completion switched the replica set off the leader {}: replicas={:?}",
            next.leader,
            next.replicas
        );
        assert!(
            next.replicas == target,
            "completion replicas != target: {:?} vs {:?}",
            next.replicas,
            target
        );
        assert!(
            next.isr.iter().all(|n| next.replicas.contains(n)),
            "completion ISR not a subset of replicas"
        );
        assert!(
            next.leader_epoch == pre.leader_epoch,
            "completion bumped leader_epoch"
        );
    } else {
        panic!("unexpected reassign_one decision shape: {next:?} from {pre:?}");
    }
}

impl Model for ReassignModel {
    type State = ReassignState;
    type Action = ReassignAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![ReassignState {
            replicas: self.replicas.clone(),
            isr: self.initial_isr.clone(),
            adding: self.adding.clone(),
            removing: self.removing.clone(),
            leader: self.leader,
            leader_epoch: 0,
            alive: self.replicas.iter().copied().collect(),
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        // AdmitToIsr: any replica not yet in ISR (models a catch-up + admit).
        for &r in &state.replicas {
            if !state.isr.contains(&r) {
                actions.push(ReassignAction::AdmitToIsr(r));
            }
        }
        // Die / Revive over the replica set (keep >= 1 alive).
        if state.alive.len() > 1 {
            for &r in &state.replicas {
                if state.alive.contains(&r) {
                    actions.push(ReassignAction::Die(r));
                }
            }
        }
        for &r in &state.replicas {
            if !state.alive.contains(&r) {
                actions.push(ReassignAction::Revive(r));
            }
        }
        // ReassignStep when in flight, under the epoch cap.
        if in_flight(state) && state.leader_epoch < self.max_epoch {
            actions.push(ReassignAction::ReassignStep);
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = last.clone();
        match action {
            ReassignAction::AdmitToIsr(n) => {
                if state.isr.contains(&n) || !state.replicas.contains(&n) {
                    return None;
                }
                // Rebuild ISR in canonical replica order (keeps the space small).
                state.isr = state
                    .replicas
                    .iter()
                    .copied()
                    .filter(|r| state.isr.contains(r) || *r == n)
                    .collect();
            }
            ReassignAction::Die(n) => {
                if last.alive.len() <= 1 || !state.alive.remove(&n) {
                    return None;
                }
            }
            ReassignAction::Revive(n) => {
                if !state.alive.insert(n) {
                    return None;
                }
            }
            ReassignAction::ReassignStep => {
                if !in_flight(&state) {
                    return None;
                }
                let pr = pr_of(&state);
                let alive: HashSet<NodeId> = state.alive.iter().copied().collect();
                match reassign_one(&pr, &alive) {
                    Some(next) => {
                        assert_step(last, &next);
                        state.leader = next.leader;
                        state.isr = next.isr;
                        state.adding = next.adding_replicas;
                        state.removing = next.removing_replicas;
                        state.replicas = next.replicas;
                        state.leader_epoch = next.leader_epoch;
                    }
                    None => return None,
                }
            }
        }
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("isr_subset_replicas", |_, s: &ReassignState| {
                s.isr.iter().all(|n| s.replicas.contains(n))
            }),
            Property::always("leader_in_replicas", |_, s: &ReassignState| {
                s.replicas.contains(&s.leader)
            }),
            Property::always("leader_in_isr", |_, s: &ReassignState| s.isr.contains(&s.leader)),
            Property::always("adding_subset_replicas", |_, s: &ReassignState| {
                s.adding.iter().all(|n| s.replicas.contains(n))
            }),
            Property::always("removing_subset_replicas", |_, s: &ReassignState| {
                s.removing.iter().all(|n| s.replicas.contains(n))
            }),
            Property::sometimes("can_complete", |_, s: &ReassignState| {
                s.adding.is_empty() && s.removing.is_empty()
            }),
            // Config-conditional so it is not vacuously unsatisfiable in the
            // basic config (where no handoff happens).
            Property::sometimes("can_handoff", |m: &ReassignModel, s: &ReassignState| {
                !m.removing.contains(&m.leader) || s.leader != m.leader
            }),
            Property::sometimes("can_wait", |_, s: &ReassignState| {
                in_flight(s) && s.adding.iter().any(|n| !s.isr.contains(n))
            }),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.leader_epoch <= self.max_epoch
    }
}

fn run(model: ReassignModel, label: &str) {
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
fn reassign_basic() {
    // Leader not removed: catch-up then completion to the target replica set.
    run(ReassignModel::basic(), "reassign_basic");
}

#[test]
fn reassign_leader_handoff() {
    // Leader in `removing`: catch-up, leader handoff, then completion.
    run(ReassignModel::leader_handoff(), "reassign_leader_handoff");
}
```

- [ ] **Step 2: Build (no run)**

```
cargo test -p crabka-broker --lib --no-run
```
Expected: compiles cleanly.

- [ ] **Step 3: Define the memory watchdog (once per PowerShell session)**

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
Invoke-GuardedExe $exe 'reassignment_model::reassign_basic'
Invoke-GuardedExe $exe 'reassignment_model::reassign_leader_handoff'
```
Expected for each: the `[label] unique_states=… …` line prints, `unique_states`
is small (low hundreds), no cap-assert fires, all `always`/`sometimes`
properties pass, `assert_step` never panics, `exit code: 0`. If a count exceeds
~50k or the watchdog kills a run, STOP and tighten before continuing.

- [ ] **Step 5: Commit**

```
git add crates/broker/src/reassignment_model.rs
git commit -m "test(broker): stateright model of KIP-455 reassignment completion"
```

---

## Task 3: Empirical scale-up + final verification + nightly fmt

**Files:**
- Modify: `crates/broker/src/reassignment_model.rs` (only if scale-up kept)

- [ ] **Step 1: Record baseline counts** from Task 2 (both should be low hundreds).

- [ ] **Step 2: Attempt a wider config**

Add a third, larger config to exercise multi-add/multi-remove. After
`reassign_leader_handoff`'s `Self { ... }` in the `impl ReassignModel`, add:

```rust
    fn wide() -> Self {
        Self {
            replicas: vec![1, 2, 3, 4, 5],
            adding: vec![4, 5],
            removing: vec![1, 2],
            initial_isr: vec![1, 2, 3],
            leader: 1, // in `removing` → handoff required
            max_epoch: 10,
        }
    }
```

And add the test after `reassign_leader_handoff`:

```rust
#[test]
fn reassign_wide() {
    run(ReassignModel::wide(), "reassign_wide");
}
```

Build, then run all three under the watchdog:
```
cargo test -p crabka-broker --lib --no-run
```
```powershell
$exe = (Get-ChildItem target\debug\deps\crabka_broker-*.exe | Sort-Object LastWriteTime -Descending | Select-Object -First 1).FullName
Invoke-GuardedExe $exe 'reassignment_model::reassign_basic'
Invoke-GuardedExe $exe 'reassignment_model::reassign_leader_handoff'
Invoke-GuardedExe $exe 'reassignment_model::reassign_wide'
```

**Decision rule:** keep `reassign_wide` only if it completes with
`unique_states < 100_000`, no cap-assert fires, the watchdog doesn't kill it,
and all properties pass. Otherwise remove the `wide` config + test. If a
cap-assert reports depth truncation, raise `MAX_DEPTH` to 120 and re-run.

- [ ] **Step 3: Final guarded full run**

```powershell
$exe = (Get-ChildItem target\debug\deps\crabka_broker-*.exe | Sort-Object LastWriteTime -Descending | Select-Object -First 1).FullName
Invoke-GuardedExe $exe 'reassignment_model::'
```
Expected: all configs pass; every `unique_states` below the kept threshold.

- [ ] **Step 4: Confirm the broader `reassignment` surface is unaffected**

```
cargo test -p crabka-broker --lib reassignment
```
Expected: all `reassignment::tests::*` (10) and `reassignment::reassignment_model::*`
pass. (Self-bounded by the in-test caps — safe for CI unguarded.)

- [ ] **Step 5: Format with NIGHTLY**

```
cargo +nightly fmt -p crabka-broker
cargo +nightly fmt -p crabka-broker -- --check
```
Expected: `--check` exits 0. (CI's fmt gate is nightly; stable `cargo fmt`
silently skips the workspace's unstable rustfmt options — see the plan header.)

- [ ] **Step 6: Commit**

```
git add crates/broker/src/reassignment_model.rs
git commit -m "test(broker): tune reassignment model bounds + final verification"
```
(If Step 2 removed `wide` and nightly fmt produced no diff, there may be nothing
to commit — skip.)

- [ ] **Step 7: Update the program memory note**

Update `project_stateright_testing_program.md` to record the reassignment
(KIP-455) model as implemented (Workstream A now covers raft consensus +
share-group acquisition + ISR/HWM + failover + reassignment), leaving KIP-848
rebalance as the remaining model candidate (KIP-853 still blocked; KIP-966 ELR
unimplemented). Memory edit only.

---

## Self-Review (completed by plan author)

**1. Spec coverage:**
- Extract `reassign_one` + rewrite `compute_reassignment_progress` → Task 1 Steps 1-2. ✅
- Behavior-preserving (existing tests pass) → Task 1 Step 3. ✅
- In-src `#[cfg(test)]` descendant module → Task 1 Step 4. ✅
- `ReassignModel`/`ReassignState`/`ReassignAction` (AdmitToIsr/Die/Revive/ReassignStep) → Task 2. ✅
- `assert_step` (leader-in-target-on-completion, handoff invariants, epoch-monotonic, adding-caught-up) → Task 2. ✅
- State-level `always` (isr_subset_replicas, leader_in_replicas, leader_in_isr, adding/removing_subset_replicas) → Task 2 `properties`. ✅
- Non-vacuity `sometimes` (can_complete, can_handoff config-conditional, can_wait) → Task 2 `properties`. ✅
- `within_boundary` (leader_epoch ≤ max_epoch) → Task 2. ✅
- target_state_count + timeout + cap-asserts + watchdog → Task 2 `run` + `Invoke-GuardedExe`. ✅
- Two configs (basic + leader-handoff) → Task 2; optional wide → Task 3. ✅
- Empirical scale-up / OOM discipline → Task 3. ✅
- Nightly fmt → Task 3 Step 5. ✅

**2. Placeholder scan:** No TBD/TODO; every code step shows complete code; every run step shows the exact command + expected output. ✅

**3. Type consistency:** `reassign_one(pr, alive) -> Option<PartitionRecord>`, `ReassignModel`/`ReassignState`/`ReassignAction`, `pr_of`/`target_of`/`in_flight`/`assert_step`, and the `PartitionRecord` field names (`replicas`/`isr`/`adding_replicas`/`removing_replicas`/`leader`/`leader_epoch`) are consistent across Tasks 1–3 and match `reassignment.rs` / `crabka_metadata`. `NodeId = u64`. ✅
