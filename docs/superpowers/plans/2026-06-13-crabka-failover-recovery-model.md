# Failover / unclean-recovery stateright model — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **OOM CAUTION (Tasks 2–4).** This plan runs a stateright BFS checker. stateright keeps every visited unique state resident; an under-bounded run OOM'd this machine once (`memory/feedback_bound_model_checkers.md`). Every checker run MUST go through the PowerShell memory watchdog, and a subagent must NEVER run the checker unguarded. Recommended execution mode: **inline by the main agent** (as with the raft / share / ISR models). Build with `--no-run` first, then run under the watchdog.
>
> **FMT (Task 4).** CI's fmt gate is **nightly** (`cargo +nightly fmt -- --check`) and the workspace `rustfmt.toml` uses unstable options that **stable `cargo fmt` silently skips**. Always format with `cargo +nightly fmt -p crabka-broker` (per `memory/reference_windows_fmt_path_length.md`; `--all` overflows OS path limits in deep worktrees, `-p` is fine).

**Goal:** Exhaustively model-check the controller leader-failover + unclean-recovery decision logic with stateright, proving a clean election never loses committed data, an out-of-ISR (unclean) election happens only when enabled, and KIP-966 recovery elects the most-complete log.

**Architecture:** Extract the per-partition failover decision into a pure sync `failover_one` (shared by both `compute_failover_changes` and the offline-dir variant — deduping ~80 lines), then two in-src `#[cfg(test)]` wrap-real stateright models: `FailoverModel` drives the real `failover_one` under every broker-death ordering; `RecoveryModel` exhaustively feeds the already-pure `select_best_replica`/`has_newer_leader` every bounded response set. Safety is checked as transition-level asserts (per decision) plus structural `always` invariants.

**Tech Stack:** Rust, `stateright = "=0.31.0"` (already a broker dev-dep), `cargo test --lib`, PowerShell memory watchdog, nightly `cargo fmt`.

**Spec:** `docs/superpowers/specs/2026-06-13-crabka-failover-recovery-model-design.md`

---

## File Structure

| File | Responsibility |
| --- | --- |
| `crates/broker/src/leader_election.rs` | (modify) add `FailoverDecision` + `failover_one`; rewrite `compute_failover_changes` and `compute_offline_dir_failover_changes` to call it; declare `#[cfg(test)] #[path="leader_failover_model.rs"] mod leader_failover_model;`. |
| `crates/broker/src/leader_failover_model.rs` | (create) both models (`FailoverModel`, `RecoveryModel`) + the watchdog-friendly `run` harnesses + `#[test]` configs. |

`select_best_replica`/`has_newer_leader`/`ReplicaLogInfo` stay `pub(crate)` in
`unclean_recovery.rs`; the model imports them via `crate::unclean_recovery::…`.

---

## Task 1: Extract `failover_one` + rewrite both scans + wire model module

**Files:**
- Modify: `crates/broker/src/leader_election.rs` (add types after line 33; rewrite `:49-170` and `:186-295`; module decl at EOF)
- Create: `crates/broker/src/leader_failover_model.rs` (smoke stub)

- [ ] **Step 1: Add `FailoverDecision` + `failover_one`**

In `crates/broker/src/leader_election.rs`, immediately after the `FailoverPlan`
struct (ends line 33), add:

```rust
/// The pure per-partition failover decision shared by the dead-broker scan
/// (`compute_failover_changes`) and the offline-log-dir scan
/// (`compute_offline_dir_failover_changes`). No I/O: the callers handle
/// partition filtering, the alive snapshot, record construction, metrics, and
/// recovery enqueue. Extracted so the failover policy is independently
/// unit-testable and model-checkable, and so the two scans share one copy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FailoverDecision {
    /// Elect `leader` with `isr`; the caller bumps `leader_epoch + 1` and, when
    /// `unclean`, records the unclean-election metric.
    Elect {
        leader: NodeId,
        isr: Vec<NodeId>,
        unclean: bool,
    },
    /// Defer to the offset-aware Unclean Recovery Manager (KIP-966).
    Recover(RecoveryStrategy),
    /// Dead broker was a non-leader ISR member: shrink ISR (leader/epoch kept).
    ShrinkIsr { isr: Vec<NodeId> },
    /// Leader is dead, ISR empty, and no unclean path is permitted/available.
    Unavailable,
    /// Nothing to do for this partition.
    NoChange,
}

/// Decide the failover action for one partition. `alive` is the controller's
/// snapshot of live brokers; `strategy` / `unclean_enabled` are the topic's
/// resolved recovery policy.
pub(crate) fn failover_one(
    pr: &PartitionRecord,
    dead: NodeId,
    alive: &std::collections::HashSet<NodeId>,
    strategy: RecoveryStrategy,
    unclean_enabled: bool,
) -> FailoverDecision {
    // The ISR after dropping the dead broker AND any other non-alive member.
    let alive_isr: Vec<NodeId> = pr
        .isr
        .iter()
        .filter(|n| **n != dead && alive.contains(n))
        .copied()
        .collect();
    if pr.leader == dead {
        if let Some(&new_leader) = alive_isr.first() {
            // Clean: the new leader was in the ISR, so it holds every committed
            // record. No data loss.
            FailoverDecision::Elect {
                leader: new_leader,
                isr: alive_isr,
                unclean: false,
            }
        } else {
            match strategy {
                RecoveryStrategy::Balanced | RecoveryStrategy::Aggressive => {
                    FailoverDecision::Recover(strategy)
                }
                RecoveryStrategy::None if unclean_enabled => {
                    // KIP-841: ISR is dead but the operator opted into possible
                    // data loss. Elect the first alive replica, singleton ISR.
                    match pr.replicas.iter().find(|n| **n != dead && alive.contains(n)) {
                        Some(&new_leader) => FailoverDecision::Elect {
                            leader: new_leader,
                            isr: vec![new_leader],
                            unclean: true,
                        },
                        None => FailoverDecision::Unavailable,
                    }
                }
                RecoveryStrategy::None => FailoverDecision::Unavailable,
            }
        }
    } else if alive_isr.len() < pr.isr.len() {
        FailoverDecision::ShrinkIsr { isr: alive_isr }
    } else {
        FailoverDecision::NoChange
    }
}
```

- [ ] **Step 2: Rewrite `compute_failover_changes` to call `failover_one`**

Replace the entire body of `compute_failover_changes` (currently lines 49-170,
from `pub(crate) async fn compute_failover_changes(` through its closing `}`)
with:

```rust
pub(crate) async fn compute_failover_changes(
    image: &MetadataImage,
    dead: NodeId,
    liveness: &ControllerLivenessState,
    metrics: &crate::metrics::BrokerMetrics,
) -> FailoverPlan {
    let mut changes: Vec<MetadataRecord> = Vec::new();
    let mut recoveries: Vec<(String, i32, RecoveryStrategy)> = Vec::new();
    // Snapshot the alive set once (single lock) rather than per ISR/replica.
    let alive = liveness.alive_snapshot().await;
    for (_, pr) in image.all_partitions() {
        if !pr.replicas.contains(&dead) && !pr.isr.contains(&dead) {
            continue;
        }
        let strategy = resolve_recovery_strategy(image, &pr.topic);
        let unclean_enabled = unclean_election_enabled(image, &pr.topic);
        match failover_one(pr, dead, &alive, strategy, unclean_enabled) {
            FailoverDecision::Elect {
                leader,
                isr,
                unclean,
            } => {
                if unclean {
                    warn!(
                        topic = %pr.topic, partition = pr.partition, leader,
                        "unclean leader election: ISR empty, electing out-of-ISR replica (possible data loss)"
                    );
                    metrics.record_unclean_leader_election();
                }
                changes.push(MetadataRecord::V1Partition(PartitionRecord {
                    topic: pr.topic.clone(),
                    partition: pr.partition,
                    leader,
                    replicas: pr.replicas.clone(),
                    isr,
                    leader_epoch: pr.leader_epoch + 1,
                    adding_replicas: pr.adding_replicas.clone(),
                    removing_replicas: pr.removing_replicas.clone(),
                    directories: pr.directories.clone(),
                    partition_epoch: pr.partition_epoch + 1,
                }));
            }
            FailoverDecision::ShrinkIsr { isr } => {
                changes.push(MetadataRecord::V1Partition(PartitionRecord {
                    topic: pr.topic.clone(),
                    partition: pr.partition,
                    leader: pr.leader,
                    replicas: pr.replicas.clone(),
                    isr,
                    leader_epoch: pr.leader_epoch,
                    adding_replicas: pr.adding_replicas.clone(),
                    removing_replicas: pr.removing_replicas.clone(),
                    directories: pr.directories.clone(),
                    partition_epoch: pr.partition_epoch + 1,
                }));
            }
            FailoverDecision::Recover(strategy) => {
                recoveries.push((pr.topic.clone(), pr.partition, strategy));
            }
            FailoverDecision::Unavailable => {
                warn!(
                    topic = %pr.topic, partition = pr.partition,
                    "leader dead, no live ISR replica; partition unavailable"
                );
            }
            FailoverDecision::NoChange => {}
        }
    }
    FailoverPlan { changes, recoveries }
}
```

(Behavior-preserving. The two distinct "unavailable" warn strings in the
original — `None`+unclean-but-no-alive vs `None`+disabled — collapse to one log
line; both are no-ops, so cluster behavior is identical.)

- [ ] **Step 3: Rewrite `compute_offline_dir_failover_changes` to call `failover_one`**

Replace the entire body of `compute_offline_dir_failover_changes` (currently
lines 186-295) with:

```rust
#[allow(clippy::too_many_lines)]
pub(crate) async fn compute_offline_dir_failover_changes(
    image: &MetadataImage,
    broker: NodeId,
    offline_uuids: &std::collections::HashSet<uuid::Uuid>,
    liveness: &ControllerLivenessState,
    metrics: &crate::metrics::BrokerMetrics,
) -> FailoverPlan {
    let mut changes: Vec<MetadataRecord> = Vec::new();
    let mut recoveries: Vec<(String, i32, RecoveryStrategy)> = Vec::new();
    let alive = liveness.alive_snapshot().await;
    for (_, pr) in image.all_partitions() {
        let Some(slot) = pr.replicas.iter().position(|n| *n == broker) else {
            continue;
        };
        let on_offline = pr
            .directories
            .get(slot)
            .is_some_and(|d| offline_uuids.contains(d));
        if !on_offline {
            continue;
        }
        let strategy = resolve_recovery_strategy(image, &pr.topic);
        let unclean_enabled = unclean_election_enabled(image, &pr.topic);
        match failover_one(pr, broker, &alive, strategy, unclean_enabled) {
            FailoverDecision::Elect {
                leader,
                isr,
                unclean,
            } => {
                if unclean {
                    warn!(
                        topic = %pr.topic, partition = pr.partition, leader,
                        "offline-dir unclean leader election: ISR empty, electing out-of-ISR replica (possible data loss)"
                    );
                    metrics.record_unclean_leader_election();
                }
                changes.push(MetadataRecord::V1Partition(PartitionRecord {
                    topic: pr.topic.clone(),
                    partition: pr.partition,
                    leader,
                    replicas: pr.replicas.clone(),
                    isr,
                    leader_epoch: pr.leader_epoch + 1,
                    adding_replicas: pr.adding_replicas.clone(),
                    removing_replicas: pr.removing_replicas.clone(),
                    directories: pr.directories.clone(),
                    partition_epoch: pr.partition_epoch + 1,
                }));
            }
            FailoverDecision::ShrinkIsr { isr } => {
                changes.push(MetadataRecord::V1Partition(PartitionRecord {
                    topic: pr.topic.clone(),
                    partition: pr.partition,
                    leader: pr.leader,
                    replicas: pr.replicas.clone(),
                    isr,
                    leader_epoch: pr.leader_epoch,
                    adding_replicas: pr.adding_replicas.clone(),
                    removing_replicas: pr.removing_replicas.clone(),
                    directories: pr.directories.clone(),
                    partition_epoch: pr.partition_epoch + 1,
                }));
            }
            FailoverDecision::Recover(strategy) => {
                recoveries.push((pr.topic.clone(), pr.partition, strategy));
            }
            FailoverDecision::Unavailable => {
                warn!(
                    topic = %pr.topic, partition = pr.partition,
                    "offline dir on leader, no live ISR replica; partition unavailable"
                );
            }
            FailoverDecision::NoChange => {}
        }
    }
    FailoverPlan { changes, recoveries }
}
```

- [ ] **Step 4: Verify the refactor is behavior-preserving**

```
cargo test -p crabka-broker --lib leader_election
```
Expected: all `leader_election::tests::*` pass (the comprehensive failover +
operator-election suite), confirming the extraction changed no behavior.

- [ ] **Step 5: Declare the model module + create the smoke stub**

At the very end of `crates/broker/src/leader_election.rs` add:

```rust
#[cfg(test)]
#[path = "leader_failover_model.rs"]
mod leader_failover_model;
```

Create `crates/broker/src/leader_failover_model.rs`:

```rust
//! Exhaustive stateright models of the controller leader-failover decision
//! (`failover_one`) and the KIP-966 winner selection
//! (`crate::unclean_recovery::select_best_replica` / `has_newer_leader`). See
//! `docs/superpowers/specs/2026-06-13-crabka-failover-recovery-model-design.md`.
//!
//! The full models are added in Tasks 2-3. This file starts as a wiring smoke
//! test.
//!
//! Memory safety: stateright BFS keeps every visited unique state resident, so
//! each checker run is fenced with `within_boundary` + `target_state_count` +
//! `timeout` and MUST be run under the host memory watchdog while bounds are
//! tuned.

use std::collections::HashSet;

use crabka_metadata::PartitionRecord;

use super::{failover_one, FailoverDecision};
use crate::config_keys::RecoveryStrategy;

#[test]
fn failover_one_clean_election_smoke() {
    // Leader 1 dies with {2,3} alive in the ISR → clean elect of 2.
    let pr = PartitionRecord {
        topic: "t".to_string(),
        partition: 0,
        leader: 1,
        replicas: vec![1, 2, 3],
        isr: vec![1, 2, 3],
        leader_epoch: 0,
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 0,
    };
    let alive: HashSet<u64> = [2, 3].into_iter().collect();
    let d = failover_one(&pr, 1, &alive, RecoveryStrategy::None, false);
    assert_eq!(
        d,
        FailoverDecision::Elect {
            leader: 2,
            isr: vec![2, 3],
            unclean: false
        }
    );
}
```

- [ ] **Step 6: Build + run the smoke test**

```
cargo test -p crabka-broker --lib --no-run
```
Expected: compiles cleanly.

```
cargo test -p crabka-broker --lib leader_failover_model::failover_one_clean_election_smoke -- --nocapture
```
Expected: PASS.

- [ ] **Step 7: Commit**

```
git add crates/broker/src/leader_election.rs crates/broker/src/leader_failover_model.rs
git commit -m "refactor(broker): extract pure failover_one + wire failover model module"
```

---

## Task 2: `FailoverModel` — the failover scan + safety asserts + three configs

**Files:**
- Modify (replace contents): `crates/broker/src/leader_failover_model.rs`

- [ ] **Step 1: Write the `FailoverModel` (replace the whole file)**

Replace the entire contents of `crates/broker/src/leader_failover_model.rs` with:

```rust
//! Exhaustive stateright models of the controller leader-failover decision
//! (`failover_one`) and the KIP-966 winner selection (Task 3). See
//! `docs/superpowers/specs/2026-06-13-crabka-failover-recovery-model-design.md`.
//!
//! Memory safety: stateright BFS keeps every visited unique state resident, so
//! each run is fenced with `within_boundary` + `target_state_count` + `timeout`
//! and MUST be executed under the host memory watchdog while bounds are tuned.

use std::collections::{BTreeSet, HashSet};
use std::time::Duration;

use crabka_metadata::PartitionRecord;
use crabka_raft::NodeId;
use stateright::{Checker, Model, Property};

use super::{failover_one, FailoverDecision};
use crate::config_keys::RecoveryStrategy;

const MAX_STATES: usize = 200_000;
const MAX_DEPTH: usize = 80;
const CHECK_TIMEOUT: Duration = Duration::from_secs(120);

// ============================ FailoverModel ============================

/// Bounded config for the failover-scan model.
struct FailoverModel {
    replicas: Vec<NodeId>, // replicas[0] is the fixed initial leader
    strategy: RecoveryStrategy,
    unclean_enabled: bool,
    max_epoch: i32,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct FailoverState {
    leader: NodeId,
    isr: Vec<NodeId>,      // order significant (clean election picks isr.first())
    replicas: Vec<NodeId>, // fixed; order significant (KIP-841 picks replicas order)
    leader_epoch: i32,
    alive: BTreeSet<NodeId>,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum FailoverAction {
    Die(NodeId),
    Revive(NodeId),
    Failover(NodeId),
}

impl FailoverModel {
    fn config(strategy: RecoveryStrategy, unclean_enabled: bool) -> Self {
        Self {
            replicas: vec![1, 2, 3],
            strategy,
            unclean_enabled,
            max_epoch: 4,
        }
    }
}

/// Build a minimal `PartitionRecord` from the model state to drive the real
/// `failover_one`. The fields `failover_one` ignores are dummied.
fn pr_of(s: &FailoverState) -> PartitionRecord {
    PartitionRecord {
        topic: "t".to_string(),
        partition: 0,
        leader: s.leader,
        replicas: s.replicas.clone(),
        isr: s.isr.clone(),
        leader_epoch: s.leader_epoch,
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 0,
    }
}

/// Verify a `failover_one` decision against the pre-failover state. These are
/// the safety-critical invariants; they hold per-decision under any ordering.
fn assert_decision(pre: &FailoverState, dead: NodeId, d: &FailoverDecision, unclean_enabled: bool) {
    match d {
        FailoverDecision::Elect {
            leader,
            isr,
            unclean,
        } => {
            assert!(*leader != dead, "elected the dead broker {dead}");
            assert!(pre.alive.contains(leader), "elected leader {leader} not alive");
            assert!(isr.contains(leader), "elected leader {leader} not in new ISR {isr:?}");
            if *unclean {
                assert!(unclean_enabled, "unclean election without unclean_enabled");
            } else {
                // Clean election: the new leader was in the pre-failover ISR, so
                // it holds every committed record. No data loss.
                assert!(
                    pre.isr.contains(leader),
                    "clean election picked {leader} not in pre-failover ISR {:?} (data loss!)",
                    pre.isr
                );
            }
        }
        FailoverDecision::ShrinkIsr { isr } => {
            assert!(
                isr.iter().all(|n| pre.isr.contains(n)),
                "shrink introduced a non-member: {isr:?} vs {:?}",
                pre.isr
            );
            assert!(isr.len() < pre.isr.len(), "ShrinkIsr did not shrink");
        }
        FailoverDecision::Recover(s) => {
            assert!(*s != RecoveryStrategy::None, "Recover with strategy None");
            assert!(pre.leader == dead, "Recover when the dead broker was not leader");
        }
        FailoverDecision::Unavailable | FailoverDecision::NoChange => {}
    }
}

impl Model for FailoverModel {
    type State = FailoverState;
    type Action = FailoverAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![FailoverState {
            leader: self.replicas[0],
            isr: self.replicas.clone(),
            replicas: self.replicas.clone(),
            leader_epoch: 0,
            alive: self.replicas.iter().copied().collect(),
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        // Die: any alive broker, keeping >= 1 alive.
        if state.alive.len() > 1 {
            for &r in &self.replicas {
                if state.alive.contains(&r) {
                    actions.push(FailoverAction::Die(r));
                }
            }
        }
        // Revive: any dead broker.
        for &r in &self.replicas {
            if !state.alive.contains(&r) {
                actions.push(FailoverAction::Revive(r));
            }
        }
        // Failover: any dead broker (the real scan's filter is
        // replicas-or-isr; all model brokers are replicas), under the epoch cap.
        if state.leader_epoch < self.max_epoch {
            for &r in &self.replicas {
                if !state.alive.contains(&r) {
                    actions.push(FailoverAction::Failover(r));
                }
            }
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = last.clone();
        match action {
            FailoverAction::Die(n) => {
                if last.alive.len() <= 1 || !state.alive.remove(&n) {
                    return None;
                }
            }
            FailoverAction::Revive(n) => {
                if !state.alive.insert(n) {
                    return None;
                }
            }
            FailoverAction::Failover(dead) => {
                if state.alive.contains(&dead) {
                    return None;
                }
                let pr = pr_of(&state);
                let alive: HashSet<NodeId> = state.alive.iter().copied().collect();
                let decision =
                    failover_one(&pr, dead, &alive, self.strategy, self.unclean_enabled);
                assert_decision(&state, dead, &decision, self.unclean_enabled);
                match decision {
                    FailoverDecision::Elect { leader, isr, .. } => {
                        state.leader = leader;
                        state.isr = isr;
                        state.leader_epoch += 1;
                    }
                    FailoverDecision::ShrinkIsr { isr } => {
                        state.isr = isr;
                    }
                    FailoverDecision::Recover(_)
                    | FailoverDecision::Unavailable
                    | FailoverDecision::NoChange => return None,
                }
            }
        }
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("isr_subset_replicas", |_, s: &FailoverState| {
                s.isr.iter().all(|n| s.replicas.contains(n))
            }),
            Property::always("leader_in_replicas", |_, s: &FailoverState| {
                s.replicas.contains(&s.leader)
            }),
            Property::sometimes("can_elect", |_, s: &FailoverState| s.leader_epoch > 0),
            Property::sometimes("can_singleton_isr", |_, s: &FailoverState| s.isr.len() == 1),
            Property::sometimes("can_lose_isr_member", |_, s: &FailoverState| {
                s.isr.iter().any(|n| !s.alive.contains(n))
            }),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.leader_epoch <= self.max_epoch
    }
}

fn run_failover(model: FailoverModel, label: &str) {
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
fn failover_safe() {
    // unclean disabled: a clean election (or unavailability) is the only path;
    // the decision asserts guarantee no out-of-ISR election ever happens.
    run_failover(
        FailoverModel::config(RecoveryStrategy::None, false),
        "failover_safe",
    );
}

#[test]
fn failover_unclean() {
    // KIP-841: out-of-ISR election permitted when ISR is empty.
    run_failover(
        FailoverModel::config(RecoveryStrategy::None, true),
        "failover_unclean",
    );
}

#[test]
fn failover_recover() {
    // KIP-966: empty-ISR leader death defers to offset-aware recovery.
    run_failover(
        FailoverModel::config(RecoveryStrategy::Balanced, false),
        "failover_recover",
    );
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

- [ ] **Step 4: Run all three failover configs under the watchdog**

```powershell
$exe = (Get-ChildItem target\debug\deps\crabka_broker-*.exe | Sort-Object LastWriteTime -Descending | Select-Object -First 1).FullName
Invoke-GuardedExe $exe 'leader_failover_model::failover_safe'
Invoke-GuardedExe $exe 'leader_failover_model::failover_unclean'
Invoke-GuardedExe $exe 'leader_failover_model::failover_recover'
```
Expected for each: the `[label] unique_states=… …` line prints, `unique_states`
is small (low thousands), no cap-assert fires, all properties pass, `exit code:
0`, and (critically) `assert_decision` never panics — in particular
`failover_safe` never produces an unclean election. If a count exceeds ~50k or
the watchdog kills a run, STOP and tighten before continuing.

- [ ] **Step 5: Commit**

```
git add crates/broker/src/leader_failover_model.rs
git commit -m "test(broker): stateright model of the leader-failover scan"
```

---

## Task 3: `RecoveryModel` — KIP-966 winner selection

**Files:**
- Modify: `crates/broker/src/leader_failover_model.rs` (append the second model + config)

- [ ] **Step 1: Append the `RecoveryModel`**

Add these imports to the existing `use` block at the top of
`crates/broker/src/leader_failover_model.rs` (extend the existing lines):

```rust
use std::collections::BTreeMap;

use crate::unclean_recovery::{has_newer_leader, select_best_replica, ReplicaLogInfo};
```

Then append at the end of the file:

```rust
// ============================ RecoveryModel ============================

/// One replica's reported log state (a hashable mirror of `ReplicaLogInfo`,
/// which isn't `Hash`).
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct ReplicaLog {
    last_written_leader_epoch: i32,
    log_end_offset: i64,
    current_leader_epoch: i32,
}

/// Bounded config for the KIP-966 winner-selection model.
struct RecoveryModel {
    replicas: Vec<NodeId>,
    max_epoch: i32,
    max_leo: i64,
    known_leader_epoch: i32,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct RecoveryState {
    responses: BTreeMap<NodeId, ReplicaLog>,
    known_leader_epoch: i32,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum RecoveryAction {
    AddResponse {
        node: NodeId,
        last_written_epoch: i32,
        leo: i64,
        current_epoch: i32,
    },
}

impl RecoveryModel {
    fn offset_recovery() -> Self {
        Self {
            replicas: vec![1, 2, 3],
            max_epoch: 1,
            max_leo: 1,
            known_leader_epoch: 1,
        }
    }
}

/// Project the gathered responses into the real wire-decoupled type.
fn infos_of(s: &RecoveryState) -> Vec<ReplicaLogInfo> {
    s.responses
        .iter()
        .map(|(id, l)| ReplicaLogInfo {
            broker_id: *id,
            last_written_leader_epoch: l.last_written_leader_epoch,
            log_end_offset: l.log_end_offset,
            current_leader_epoch: l.current_leader_epoch,
        })
        .collect()
}

impl Model for RecoveryModel {
    type State = RecoveryState;
    type Action = RecoveryAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![RecoveryState {
            responses: BTreeMap::new(),
            known_leader_epoch: self.known_leader_epoch,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        // Each replica reports at most one log state; fan out over the bounded
        // (epoch, leo, current_epoch) domain. current_epoch ranges one past the
        // known epoch so has_newer_leader is reachable both ways.
        for &node in &self.replicas {
            if state.responses.contains_key(&node) {
                continue;
            }
            for last_written_epoch in 0..=self.max_epoch {
                for leo in 0..=self.max_leo {
                    for current_epoch in self.known_leader_epoch..=(self.known_leader_epoch + 1) {
                        actions.push(RecoveryAction::AddResponse {
                            node,
                            last_written_epoch,
                            leo,
                            current_epoch,
                        });
                    }
                }
            }
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = last.clone();
        match action {
            RecoveryAction::AddResponse {
                node,
                last_written_epoch,
                leo,
                current_epoch,
            } => {
                if state.responses.contains_key(&node) {
                    return None;
                }
                state.responses.insert(
                    node,
                    ReplicaLog {
                        last_written_leader_epoch: last_written_epoch,
                        log_end_offset: leo,
                        current_leader_epoch: current_epoch,
                    },
                );
            }
        }
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // The real select_best_replica returns the true maximum by
            // (last_written_leader_epoch, log_end_offset, then lowest broker_id).
            Property::always("select_best_is_max", |_, s: &RecoveryState| {
                let infos = infos_of(s);
                match select_best_replica(&infos) {
                    None => infos.is_empty(),
                    Some(w) => {
                        let win = infos
                            .iter()
                            .find(|i| i.broker_id == w)
                            .expect("winner is among the inputs");
                        infos.iter().all(|i| {
                            (win.last_written_leader_epoch, win.log_end_offset)
                                .cmp(&(i.last_written_leader_epoch, i.log_end_offset))
                                .then(i.broker_id.cmp(&win.broker_id)) // lower id wins
                                != std::cmp::Ordering::Less
                        })
                    }
                }
            }),
            // The real has_newer_leader matches its specification.
            Property::always("has_newer_leader_matches", |_, s: &RecoveryState| {
                let infos = infos_of(s);
                has_newer_leader(&infos, s.known_leader_epoch)
                    == infos
                        .iter()
                        .any(|i| i.current_leader_epoch > s.known_leader_epoch)
            }),
            Property::sometimes("can_pick_winner", |_, s: &RecoveryState| {
                !s.responses.is_empty()
            }),
            Property::sometimes("can_detect_newer", |_, s: &RecoveryState| {
                s.responses
                    .values()
                    .any(|l| l.current_leader_epoch > s.known_leader_epoch)
            }),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.responses.len() <= self.replicas.len()
            && state.responses.values().all(|l| {
                l.last_written_leader_epoch <= self.max_epoch && l.log_end_offset <= self.max_leo
            })
    }
}

fn run_recovery(model: RecoveryModel, label: &str) {
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
fn offset_recovery() {
    run_recovery(RecoveryModel::offset_recovery(), "offset_recovery");
}
```

- [ ] **Step 2: Build (no run)**

```
cargo test -p crabka-broker --lib --no-run
```
Expected: compiles cleanly.

- [ ] **Step 3: Run the recovery config under the watchdog**

```powershell
$exe = (Get-ChildItem target\debug\deps\crabka_broker-*.exe | Sort-Object LastWriteTime -Descending | Select-Object -First 1).FullName
Invoke-GuardedExe $exe 'leader_failover_model::offset_recovery'
```
Expected: `[offset_recovery] unique_states=… …` prints, `unique_states` small,
no cap-assert fires, `select_best_is_max` + `has_newer_leader_matches` hold over
every bounded response set, both `sometimes` witnesses fire, `exit code: 0`.

- [ ] **Step 4: Commit**

```
git add crates/broker/src/leader_failover_model.rs
git commit -m "test(broker): stateright model of KIP-966 unclean-recovery winner selection"
```

---

## Task 4: Empirical scale-up + final verification

**Files:**
- Modify: `crates/broker/src/leader_failover_model.rs` (only if scale-up kept)

- [ ] **Step 1: Record baseline counts** from Tasks 2–3 (all should be low thousands).

- [ ] **Step 2: Attempt a scale-up**

Raise `FailoverModel::config`'s `max_epoch` from `4` to `6`, and
`RecoveryModel::offset_recovery`'s `max_epoch`/`max_leo` from `1` to `2`. Build,
then run all four configs under the watchdog:

```
cargo test -p crabka-broker --lib --no-run
```
```powershell
$exe = (Get-ChildItem target\debug\deps\crabka_broker-*.exe | Sort-Object LastWriteTime -Descending | Select-Object -First 1).FullName
Invoke-GuardedExe $exe 'leader_failover_model::failover_safe'
Invoke-GuardedExe $exe 'leader_failover_model::failover_unclean'
Invoke-GuardedExe $exe 'leader_failover_model::failover_recover'
Invoke-GuardedExe $exe 'leader_failover_model::offset_recovery'
```

**Decision rule:** keep the larger bounds only if every config stays
`unique_states < 100_000`, no cap-assert fires, the watchdog doesn't kill any
run, and all properties pass. Otherwise revert that bound. If a cap-assert
reports depth truncation, raise `MAX_DEPTH` to 120 and re-run.

- [ ] **Step 3: Final guarded full run**

```powershell
$exe = (Get-ChildItem target\debug\deps\crabka_broker-*.exe | Sort-Object LastWriteTime -Descending | Select-Object -First 1).FullName
Invoke-GuardedExe $exe 'leader_failover_model::'
```
Expected: all four configs pass; every `unique_states` below the kept threshold.

- [ ] **Step 4: Confirm the broader `leader_election` surface is unaffected**

```
cargo test -p crabka-broker --lib leader_election
```
Expected: all `leader_election::tests::*` and
`leader_election::leader_failover_model::*` pass. (Self-bounded by the in-test
caps — safe for CI to run unguarded.)

- [ ] **Step 5: Format with NIGHTLY**

```
cargo +nightly fmt -p crabka-broker
cargo +nightly fmt -p crabka-broker -- --check
```
Expected: the `--check` exits 0. (CI's fmt gate is nightly; stable `cargo fmt`
silently skips the workspace's unstable rustfmt options and would fail CI — see
the plan header + `memory/reference_windows_fmt_path_length.md`.)

- [ ] **Step 6: Commit**

```
git add crates/broker/src/leader_failover_model.rs
git commit -m "test(broker): tune failover/recovery model bounds + final verification"
```
(If Step 2 reverted and nightly fmt produced no diff, there may be nothing to
commit — skip.)

- [ ] **Step 7: Update the program memory note**

Update `project_stateright_testing_program.md` to record the failover /
unclean-recovery model as implemented (Workstream A now covers raft consensus +
share-group acquisition + ISR/HWM + leader-change/failover safety), leaving
reassignment (KIP-455) and KIP-848 rebalance as remaining model candidates
(KIP-853 still blocked; KIP-966 ELR unimplemented). Memory edit only.

---

## Self-Review (completed by plan author)

**1. Spec coverage:**
- Extract `failover_one` + `FailoverDecision`, reuse in both scans → Task 1 Steps 1-3. ✅
- Behavior-preserving (existing tests pass) → Task 1 Step 4. ✅
- In-src `#[cfg(test)]` descendant module → Task 1 Step 5. ✅
- `FailoverModel` state/actions (Die/Revive/Failover, all orderings) → Task 2. ✅
- Decision-level asserts (clean-no-loss, unclean-only-when-enabled, elected-alive, leader-in-new-isr, shrink-only-removes, recover-requires-strategy) → Task 2 `assert_decision`. ✅
- State-level `always` (isr_subset_replicas, leader_in_replicas) → Task 2 `properties`. ✅
- Non-vacuity `sometimes` → Task 2 `properties`. ✅
- Three configs (safe / unclean / recover) → Task 2. ✅
- `RecoveryModel` verifying `select_best_replica` + `has_newer_leader` over every bounded response set → Task 3. ✅
- `offset_recovery` config → Task 3. ✅
- target_state_count + timeout + cap-asserts + watchdog → Tasks 2-3 `run_*` + `Invoke-GuardedExe`. ✅
- Empirical scale-up / OOM discipline → Task 4. ✅
- Nightly fmt → Task 4 Step 5. ✅

**2. Placeholder scan:** No TBD/TODO; every code step shows complete code; every run step shows exact command + expected output. ✅

**3. Type consistency:** `FailoverDecision` (Elect{leader,isr,unclean}/Recover/ShrinkIsr/Unavailable/NoChange), `failover_one(pr, dead, alive, strategy, unclean_enabled)`, `FailoverModel`/`FailoverState`/`FailoverAction`, `RecoveryModel`/`RecoveryState`/`RecoveryAction`/`ReplicaLog`, and the real `select_best_replica`/`has_newer_leader`/`ReplicaLogInfo`/`PartitionRecord`/`RecoveryStrategy` names are consistent across tasks and match `leader_election.rs` / `unclean_recovery.rs` / `config_keys.rs`. `NodeId = u64`. ✅
