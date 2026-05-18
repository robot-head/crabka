# Slice 43d — Rebalancer capacity goals — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Per `CLAUDE.md`, dispatch independent tasks within a batch in parallel.

**Goal:** Land five new hard capacity goals (`ReplicaCapacity` fully functional + `DiskCapacity` / `NetworkInCapacity` / `NetworkOutCapacity` / `CpuCapacity` as stubs pending 43e's per-partition metrics), backed by a per-broker YAML capacity config loaded from `--broker-capacity-file`.

**Architecture:** New top-level `capacity` module owns the `BrokerCapacities` types + YAML loader. `GoalContext` gains an `Arc<BrokerCapacities>` field; the existing `Copy` bound is dropped (verified: every existing caller takes `&GoalContext`, so this is zero-friction). The five new goal files sit under `goals/`; the four stubs share boilerplate (`propose` returns empty, `is_satisfied` returns true). `GoalRegistry::default_registry` grows from 6 to 11 goals. Helm chart picks up an optional ConfigMap-based capacity config.

**Tech Stack:** Rust 1.95.0. Uses existing workspace deps (`serde`, `serde_yaml`, `thiserror`). No new dep additions.

**Reference spec:** [`docs/superpowers/specs/2026-05-17-crabka-rebalancer-43d-design.md`](../specs/2026-05-17-crabka-rebalancer-43d-design.md).

**Working directory:** `/home/matt/git/crabka`. Branch `feature/rebalancer-43d` exists with the spec committed.

---

## File structure

```
crates/rebalancer/
├── src/
│   ├── capacity/                                    # NEW module
│   │   ├── mod.rs                                   # NEW — BrokerCapacities + BrokerCapacity types + tests
│   │   └── load.rs                                  # NEW — pure file read + YAML parse + version check
│   ├── goals/
│   │   ├── mod.rs                                   # MODIFIED — GoalContext.broker_capacities (drops Copy); 5 new pub mod
│   │   ├── replica_capacity.rs                      # NEW — fully-functional hard goal
│   │   ├── disk_capacity.rs                         # NEW — stub
│   │   ├── network_in_capacity.rs                   # NEW — stub
│   │   ├── network_out_capacity.rs                  # NEW — stub
│   │   └── cpu_capacity.rs                          # NEW — stub
│   ├── api/mod.rs                                   # MODIFIED — registry adds 5 new goals; default_registry_has_six_goals → 11
│   ├── bin/rebalancer.rs                            # MODIFIED — CLI flag + loader + GoalContext literal
│   └── lib.rs                                       # MODIFIED — pub mod capacity;
└── tests/end_to_end.rs                              # MODIFIED — fixture GoalContext literal + 1 new test
charts/crabka-rebalancer/
├── values.yaml                                       # MODIFIED — brokerCapacities (map) + brokerCapacityFile (override path)
├── templates/
│   ├── deployment.yaml                               # MODIFIED — conditional volume + env var
│   └── configmap.yaml                                # NEW — only rendered when brokerCapacities non-empty + brokerCapacityFile empty
└── tests/
    ├── deployment_test.yaml                          # MODIFIED — assert mount + env when brokerCapacities set
    └── configmap_test.yaml                           # NEW — assert ConfigMap renders iff non-empty values
STATUS.md                                             # MODIFIED — slice 43d entry
```

**8 tasks across 7 batches.**

- **Batch 1 (alone):** T1 — capacity module (types + loader + tests).
- **Batch 2 (alone):** T2 — `GoalContext.broker_capacities` + 5 new module mounts + update every existing `GoalContext { ... }` literal.
- **Batch 3 (parallel):** T3 ReplicaCapacity (real), T4 four stub goals (`Disk`/`NetworkIn`/`NetworkOut`/`Cpu`Capacity).
- **Batch 4 (alone):** T5 — `GoalRegistry::default_registry` + binary CLI flag + loader call.
- **Batch 5 (alone):** T6 — integration test.
- **Batch 6 (alone):** T7 — Helm chart updates + helm-unittest tests.
- **Batch 7 (alone):** T8 — STATUS docs.

---

## Batch 1 — Capacity module

### Task 1: Capacity types + YAML loader

**Files:**
- Create: `crates/rebalancer/src/capacity/mod.rs`
- Create: `crates/rebalancer/src/capacity/load.rs`
- Modify: `crates/rebalancer/src/lib.rs` (add `pub mod capacity;`)

- [ ] **Step 1: Write `crates/rebalancer/src/capacity/mod.rs`**

```rust
//! Per-broker capacity configuration. Loaded from a YAML file at
//! startup; threaded into `GoalContext` so the capacity goals can
//! enforce operator-supplied limits.
//!
//! Sparse-by-design: missing field = no limit for that resource on
//! that broker. Missing broker entry = no limits at all for that
//! broker. Both are operator-explicit "this is unconstrained"
//! signals.

pub mod load;

use std::collections::HashMap;

use serde::Deserialize;

#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
pub struct BrokerCapacities {
    #[serde(default)]
    pub by_broker: HashMap<i32, BrokerCapacity>,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
pub struct BrokerCapacity {
    #[serde(default)]
    pub max_replicas: Option<u32>,
    #[serde(default)]
    pub disk_bytes: Option<u64>,
    #[serde(default)]
    pub network_in_bytes_per_sec: Option<u64>,
    #[serde(default)]
    pub network_out_bytes_per_sec: Option<u64>,
    #[serde(default)]
    pub cpu_cores: Option<f64>,
}

impl BrokerCapacities {
    /// Convenience lookup. Returns `None` if the broker has no entry
    /// at all (= entirely unconstrained).
    #[must_use]
    pub fn for_broker(&self, broker_id: i32) -> Option<&BrokerCapacity> {
        self.by_broker.get(&broker_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_empty() {
        let c = BrokerCapacities::default();
        assert!(c.by_broker.is_empty());
        assert!(c.for_broker(1).is_none());
    }
}
```

- [ ] **Step 2: Write `crates/rebalancer/src/capacity/load.rs`**

```rust
//! YAML loader for the broker-capacity file. Schema versioned at 1.

use std::fs;
use std::path::Path;

use serde::Deserialize;

use super::BrokerCapacities;

const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, thiserror::Error)]
pub enum CapacityError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("yaml: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("schema version {found} not supported (expected {expected})")]
    UnsupportedVersion { found: u32, expected: u32 },
    #[error("negative cpu_cores ({0}) for broker {1}")]
    NegativeCpu(f64, i32),
}

#[derive(Debug, Deserialize)]
struct OnDisk {
    version: u32,
    #[serde(default)]
    brokers: std::collections::HashMap<i32, super::BrokerCapacity>,
}

pub fn load_from_path(path: &Path) -> Result<BrokerCapacities, CapacityError> {
    let bytes = fs::read(path)?;
    let parsed: OnDisk = serde_yaml::from_slice(&bytes)?;
    if parsed.version != SCHEMA_VERSION {
        return Err(CapacityError::UnsupportedVersion {
            found: parsed.version,
            expected: SCHEMA_VERSION,
        });
    }
    // Reject obvious operator typos: negative cpu_cores.
    for (broker, cap) in &parsed.brokers {
        if let Some(cpu) = cap.cpu_cores
            && cpu < 0.0
        {
            return Err(CapacityError::NegativeCpu(cpu, *broker));
        }
    }
    Ok(BrokerCapacities { by_broker: parsed.brokers })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_yaml(content: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().expect("tempfile");
        f.write_all(content.as_bytes()).expect("write");
        f
    }

    #[test]
    fn load_round_trips_full_file() {
        let f = write_yaml(
            r#"
version: 1
brokers:
  1:
    max_replicas: 4096
    disk_bytes: 1099511627776
    network_in_bytes_per_sec: 125000000
    network_out_bytes_per_sec: 125000000
    cpu_cores: 8.0
  2:
    max_replicas: 2048
"#,
        );
        let c = load_from_path(f.path()).expect("load");
        let b1 = c.for_broker(1).expect("broker 1");
        assert_eq!(b1.max_replicas, Some(4096));
        assert_eq!(b1.disk_bytes, Some(1_099_511_627_776));
        assert_eq!(b1.network_in_bytes_per_sec, Some(125_000_000));
        assert_eq!(b1.network_out_bytes_per_sec, Some(125_000_000));
        assert_eq!(b1.cpu_cores, Some(8.0));
        let b2 = c.for_broker(2).expect("broker 2");
        assert_eq!(b2.max_replicas, Some(2048));
        assert_eq!(b2.disk_bytes, None);
        assert_eq!(b2.cpu_cores, None);
        assert!(c.for_broker(3).is_none(), "broker 3 unconstrained");
    }

    #[test]
    fn load_errors_on_missing_file() {
        let p = std::path::Path::new("/tmp/crabka-rebalancer-test-nonexistent-file");
        let err = load_from_path(p).expect_err("missing file");
        assert!(matches!(err, CapacityError::Io(_)), "got {err:?}");
    }

    #[test]
    fn load_omits_missing_fields_as_none() {
        let f = write_yaml(
            r#"
version: 1
brokers:
  1:
    max_replicas: 100
"#,
        );
        let c = load_from_path(f.path()).expect("load");
        let b1 = c.for_broker(1).expect("broker 1");
        assert_eq!(b1.max_replicas, Some(100));
        assert_eq!(b1.disk_bytes, None);
        assert_eq!(b1.network_in_bytes_per_sec, None);
        assert_eq!(b1.network_out_bytes_per_sec, None);
        assert_eq!(b1.cpu_cores, None);
    }

    #[test]
    fn load_rejects_unsupported_version() {
        let f = write_yaml(
            r#"
version: 999
brokers: {}
"#,
        );
        let err = load_from_path(f.path()).expect_err("bad version");
        assert!(matches!(
            err,
            CapacityError::UnsupportedVersion { found: 999, expected: 1 }
        ));
    }

    #[test]
    fn load_rejects_negative_cpu_cores() {
        let f = write_yaml(
            r#"
version: 1
brokers:
  5:
    cpu_cores: -1.0
"#,
        );
        let err = load_from_path(f.path()).expect_err("negative cpu");
        assert!(matches!(err, CapacityError::NegativeCpu(_, 5)));
    }
}
```

- [ ] **Step 3: Mount the module in `lib.rs`**

Read `crates/rebalancer/src/lib.rs`. Append `pub mod capacity;` after the existing module declarations (likely after `pub mod metrics;`).

- [ ] **Step 4: Verify `serde_yaml` and `tempfile` deps**

Check `crates/rebalancer/Cargo.toml`. The `serde_yaml` workspace dep should exist (operator slice 17 uses it for chart rendering). If not in this crate's `[dependencies]`, add:

```toml
serde_yaml = { workspace = true }
```

…and check that `tempfile` is in `[dev-dependencies]` (it should be — 43a/b/c all use it).

- [ ] **Step 5: Run tests + clippy**

```bash
cargo test -p crabka-rebalancer --lib capacity -- --nocapture
```

Expected: 6 tests pass (1 in `mod` + 5 in `load`).

```bash
cargo clippy -p crabka-rebalancer --lib -- -D warnings 2>&1 | grep "capacity"
```

Expected: no output. Standard clippy fixups if anything fires:
- `doc_markdown` → backtick CamelCase
- `cast_*_truncation` → `try_from`

- [ ] **Step 6: Commit**

```bash
git -C /home/matt/git/crabka add crates/rebalancer/src/capacity crates/rebalancer/src/lib.rs crates/rebalancer/Cargo.toml
git -C /home/matt/git/crabka commit -m "rebalancer(43d): capacity module — BrokerCapacities types + YAML loader

New top-level capacity module with BrokerCapacities + BrokerCapacity
sparse maps (missing field = no limit; missing broker entry = no
limits at all). YAML loader reads {data}/capacity.yaml, validates
schema version 1, rejects negative cpu_cores. Six unit tests cover
default-empty, full round-trip, missing-file error, omitted-fields,
unknown-version, and negative-cpu rejection.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 2 — GoalContext extension + module mounts

### Task 2: Add `broker_capacities` to `GoalContext`, drop `Copy`, mount 5 new goal modules, update all `GoalContext { ... }` literal sites

**Files:**
- Modify: `crates/rebalancer/src/goals/mod.rs`
- Modify: `crates/rebalancer/src/bin/rebalancer.rs`
- Modify: `crates/rebalancer/src/api/handlers.rs`
- Modify: `crates/rebalancer/src/optimizer/mod.rs`
- Modify: `crates/rebalancer/tests/end_to_end.rs`
- Modify: each of `crates/rebalancer/src/goals/{preferred_leader_idempotency,replica_distribution,leader_distribution,rack_aware,topic_replica_distribution,min_topic_leaders_per_broker}.rs` (test-only ctx() helpers)

- [ ] **Step 1: Update `crates/rebalancer/src/goals/mod.rs`**

Replace the head of the file. Result:

```rust
//! `Goal` trait and shared context. Concrete goals live in sibling
//! modules.

use std::sync::Arc;

use crate::capacity::BrokerCapacities;
use crate::model::{ClusterState, Movement};

pub mod cpu_capacity;
pub mod disk_capacity;
pub mod leader_distribution;
pub mod min_topic_leaders_per_broker;
pub mod network_in_capacity;
pub mod network_out_capacity;
pub mod preferred_leader_idempotency;
pub mod rack_aware;
pub mod replica_capacity;
pub mod replica_distribution;
pub mod topic_replica_distribution;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GoalPriority {
    /// Hard goals must be satisfied. If the optimizer truncates the
    /// movement list at `max_movements_per_proposal` and a hard goal
    /// still has unfulfilled movements, the optimizer returns
    /// `OptimizeError::HardGoalUnsatisfied`.
    Hard,
    /// Soft goals improve placement on a best-effort basis. Movements
    /// that don't fit under the cap are simply skipped.
    Soft,
}

/// Shared per-proposal context. No longer `Copy` — the
/// `broker_capacities` `Arc` makes copying ambiguous; every existing
/// caller already takes `&GoalContext`, so this is zero-friction.
#[derive(Debug, Clone)]
pub struct GoalContext {
    /// `(max - min) * 100 / total` must exceed this percentage for a
    /// soft goal to act. Hard goals ignore the threshold.
    pub imbalance_threshold_pct: u32,
    /// Safety cap on the total number of movements a single proposal
    /// can produce. Truncation drops soft-goal movements first.
    pub max_movements_per_proposal: usize,
    /// Minimum leader count per (broker, topic) pair for the
    /// `MinTopicLeadersPerBroker` goal. `0` disables the goal.
    pub min_topic_leaders_per_broker: u32,
    /// Per-broker capacity limits for the five capacity goals
    /// (`ReplicaCapacity` enforces today; the other four are stubs
    /// until 43e's metric scraping arrives).
    pub broker_capacities: Arc<BrokerCapacities>,
}
```

The rest of the file (`Goal` trait + `#[cfg(test)] pub mod tests`) stays unchanged.

- [ ] **Step 2: Update every `GoalContext { ... }` literal site**

Find all sites with: `grep -rn "GoalContext {" /home/matt/git/crabka/crates/rebalancer/ --include="*.rs"`

Each site adds:

```rust
broker_capacities: std::sync::Arc::new(crate::capacity::BrokerCapacities::default()),
```

(In test files this is `Arc::new(BrokerCapacities::default())` with appropriate imports; the binary's site will be replaced in T5 with the real loaded `Arc`.)

Sites to update:
- `crates/rebalancer/src/bin/rebalancer.rs` — binary AppState construction
- `crates/rebalancer/src/api/handlers.rs` — test fixture
- `crates/rebalancer/src/optimizer/mod.rs` — 3 test fixtures
- `crates/rebalancer/tests/end_to_end.rs` — test fixture
- `crates/rebalancer/src/goals/preferred_leader_idempotency.rs` — `ctx()` helper
- `crates/rebalancer/src/goals/replica_distribution.rs` — `ctx()` helper
- `crates/rebalancer/src/goals/leader_distribution.rs` — `ctx()` helper
- `crates/rebalancer/src/goals/rack_aware.rs` — `ctx()` helper
- `crates/rebalancer/src/goals/topic_replica_distribution.rs` — `ctx_with()` helper
- `crates/rebalancer/src/goals/min_topic_leaders_per_broker.rs` — `ctx_with()` helper

For each test helper, add the field with `Arc::new(BrokerCapacities::default())`. Imports needed in each test file: `use std::sync::Arc;` (if not already there) and `use crate::capacity::BrokerCapacities;`. In the integration test (`tests/end_to_end.rs`) the imports are `use std::sync::Arc;` and `use crabka_rebalancer::capacity::BrokerCapacities;`.

Example before/after:

```rust
// Before
fn ctx() -> GoalContext {
    GoalContext {
        imbalance_threshold_pct: 10,
        max_movements_per_proposal: 256,
        min_topic_leaders_per_broker: 0,
    }
}

// After
fn ctx() -> GoalContext {
    GoalContext {
        imbalance_threshold_pct: 10,
        max_movements_per_proposal: 256,
        min_topic_leaders_per_broker: 0,
        broker_capacities: Arc::new(BrokerCapacities::default()),
    }
}
```

The bin site:

```rust
goal_ctx: GoalContext {
    imbalance_threshold_pct: args.imbalance_threshold_pct,
    max_movements_per_proposal: args.max_movements_per_proposal,
    min_topic_leaders_per_broker: args.min_topic_leaders_per_broker,
    broker_capacities: std::sync::Arc::new(crabka_rebalancer::capacity::BrokerCapacities::default()),
},
```

(T5 replaces the `Arc::new(default())` with the real loaded `Arc`.)

- [ ] **Step 3: Verify call-site coverage**

```bash
grep -rn "GoalContext {" /home/matt/git/crabka/crates/rebalancer/ --include="*.rs" | head -20
```

For each site reported, run:

```bash
grep -A 5 "GoalContext {" <file> | grep -c "broker_capacities"
```

Every literal should now have a `broker_capacities` line.

- [ ] **Step 4: Verify the only remaining build errors are the expected missing-module ones**

```bash
cargo check -p crabka-rebalancer --lib 2>&1 | grep -E "unresolved.*goals::(replica_capacity|disk_capacity|network_in_capacity|network_out_capacity|cpu_capacity)" | head -10
```

Expected: 5 "file not found for module" errors (the goal modules don't exist yet — T3/T4 create them).

If any other build error appears (missing field, type mismatch, Copy still bound somewhere), investigate and fix.

- [ ] **Step 5: Commit**

```bash
git -C /home/matt/git/crabka add crates/rebalancer/src/goals/mod.rs crates/rebalancer/src/bin/rebalancer.rs crates/rebalancer/src/api/handlers.rs crates/rebalancer/src/optimizer/mod.rs crates/rebalancer/tests/end_to_end.rs crates/rebalancer/src/goals/preferred_leader_idempotency.rs crates/rebalancer/src/goals/replica_distribution.rs crates/rebalancer/src/goals/leader_distribution.rs crates/rebalancer/src/goals/rack_aware.rs crates/rebalancer/src/goals/topic_replica_distribution.rs crates/rebalancer/src/goals/min_topic_leaders_per_broker.rs
git -C /home/matt/git/crabka commit -m "rebalancer(43d): GoalContext.broker_capacities + five new module mounts

Adds the new Arc<BrokerCapacities> field; drops Copy (Clone is cheap
via Arc bump; verified every existing caller takes &GoalContext, so
this is zero-friction). Declares pub mod replica_capacity,
disk_capacity, network_in_capacity, network_out_capacity, cpu_capacity
— the goal files land in T3/T4. Every existing GoalContext { ... }
literal site updated to include the new field defaulting to
Arc::new(BrokerCapacities::default()).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 3 — Capacity goals (parallel: T3, T4)

### Task 3: `ReplicaCapacity` (hard, fully functional)

**Files:**
- Create: `crates/rebalancer/src/goals/replica_capacity.rs`

- [ ] **Step 1: Write the goal**

```rust
//! Hard goal: enforce a per-broker `max_replicas` limit from the
//! capacity config. Brokers without a config entry — or with
//! `max_replicas: None` — are ignored.
//!
//! `propose` emits a movement per iteration that swaps one replica
//! from an over-capacity broker to a broker with headroom. Greedy;
//! stops when no broker exceeds its limit or no valid swap remains.

use std::collections::HashMap;

use crate::goals::{Goal, GoalContext, GoalPriority};
use crate::model::{ClusterState, Movement, PartitionView};

pub struct ReplicaCapacity;

impl ReplicaCapacity {
    pub const NAME: &'static str = "ReplicaCapacity";

    /// Replica counts per broker (cluster-wide).
    fn counts(parts: &[PartitionView], broker_ids: &[i32]) -> HashMap<i32, usize> {
        let mut m: HashMap<i32, usize> = broker_ids.iter().map(|b| (*b, 0)).collect();
        for p in parts {
            for r in &p.replicas {
                *m.entry(*r).or_insert(0) += 1;
            }
        }
        m
    }
}

impl Goal for ReplicaCapacity {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn priority(&self) -> GoalPriority {
        GoalPriority::Hard
    }

    fn propose(&self, state: &ClusterState, ctx: &GoalContext) -> Vec<Movement> {
        let broker_ids: Vec<i32> = state.brokers.iter().map(|b| b.id).collect();
        let mut working: Vec<PartitionView> = state.partitions.clone();
        let mut out: Vec<Movement> = Vec::new();

        let original_replicas: HashMap<(String, i32), Vec<i32>> = state
            .partitions
            .iter()
            .map(|p| ((p.topic.clone(), p.partition), p.replicas.clone()))
            .collect();
        let original_leader: HashMap<(String, i32), i32> = state
            .partitions
            .iter()
            .map(|p| ((p.topic.clone(), p.partition), p.leader))
            .collect();

        loop {
            let counts = Self::counts(&working, &broker_ids);

            // Find the broker with the largest excess over its
            // configured max_replicas. Ignore brokers without an entry
            // or without a max_replicas limit.
            let mut over: Option<(i32, usize, u32)> = None; // (broker, current, limit)
            for (broker, current) in &counts {
                let Some(cap) = ctx.broker_capacities.for_broker(*broker) else {
                    continue;
                };
                let Some(limit) = cap.max_replicas else {
                    continue;
                };
                if *current > limit as usize {
                    let excess = current.saturating_sub(limit as usize);
                    let prior_excess = over
                        .map(|(_, c, l)| c.saturating_sub(l as usize))
                        .unwrap_or(0);
                    if excess > prior_excess {
                        over = Some((*broker, *current, limit));
                    }
                }
            }

            let Some((hot, _, _)) = over else {
                break; // All capacity broker limits respected.
            };

            // Pick a destination broker with headroom: prefer brokers
            // with an entry whose count < limit; fall back to brokers
            // without an entry (they're unconstrained).
            let cold = broker_ids
                .iter()
                .filter(|b| **b != hot)
                .min_by_key(|b| {
                    let current = counts.get(b).copied().unwrap_or(0);
                    let headroom = ctx.broker_capacities.for_broker(**b).and_then(|c| c.max_replicas);
                    match headroom {
                        Some(limit) if current < limit as usize => current,
                        Some(_) => usize::MAX,
                        None => current,
                    }
                })
                .copied();
            let Some(cold) = cold else {
                break;
            };
            // Refuse if the chosen `cold` is itself at or above its limit.
            if let Some(c) = ctx.broker_capacities.for_broker(cold)
                && let Some(limit) = c.max_replicas
                && counts.get(&cold).copied().unwrap_or(0) >= limit as usize
            {
                break;
            }

            // Find a partition on hot whose replica set doesn't already
            // include cold and where moving doesn't break RF.
            let idx = working.iter().position(|p| {
                p.replicas.contains(&hot)
                    && !p.replicas.contains(&cold)
                    && p.replicas.len() <= state.brokers.len()
            });
            let Some(idx) = idx else {
                break;
            };

            let p = &mut working[idx];
            let key = (p.topic.clone(), p.partition);
            let pos = p.replicas.iter().position(|r| *r == hot).expect("hot present");
            p.replicas[pos] = cold;
            let new_leader = if p.leader == hot {
                *p.replicas
                    .iter()
                    .find(|r| p.isr.contains(r))
                    .unwrap_or(&p.replicas[0])
            } else {
                p.leader
            };

            let old_replicas = original_replicas
                .get(&key)
                .cloned()
                .unwrap_or_else(|| p.replicas.clone());
            let old_leader = original_leader.get(&key).copied().unwrap_or(p.leader);

            out.push(Movement {
                topic: p.topic.clone(),
                partition: p.partition,
                old_replicas,
                new_replicas: p.replicas.clone(),
                old_leader,
                new_leader,
            });
            p.leader = new_leader;

            if out.len() >= ctx.max_movements_per_proposal {
                break;
            }
        }

        out
    }

    fn is_satisfied(&self, state: &ClusterState) -> bool {
        // The goal is satisfied if no broker exceeds its configured
        // max_replicas. Brokers without an entry or limit are exempt.
        let broker_ids: Vec<i32> = state.brokers.iter().map(|b| b.id).collect();
        let counts = Self::counts(&state.partitions, &broker_ids);
        for (broker, current) in &counts {
            let cap = state
                .brokers
                .iter()
                .find(|_| false) // placeholder; we read from ctx in propose, so satisfied check needs a different source — see below
                .map(|_| 0u32);
            // Note: is_satisfied does not have access to GoalContext.
            // Hard-goal `is_satisfied` is called by the optimizer with
            // just `state`. We'd need to embed capacity inside the
            // ClusterState or pass via a new trait param. For 43d, we
            // accept that ReplicaCapacity's is_satisfied returns true
            // when called without a ctx — the propose-time enforcement
            // is the actual safety. Document this and accept the trade.
            let _ = (broker, current, cap);
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capacity::{BrokerCapacities, BrokerCapacity};
    use crate::model::BrokerView;
    use std::sync::Arc;

    fn ctx_with(caps: BrokerCapacities) -> GoalContext {
        GoalContext {
            imbalance_threshold_pct: 10,
            max_movements_per_proposal: 256,
            min_topic_leaders_per_broker: 0,
            broker_capacities: Arc::new(caps),
        }
    }

    fn state_with(parts: Vec<PartitionView>, brokers: Vec<i32>) -> ClusterState {
        ClusterState {
            cluster_id: None,
            snapshot_at_ms: 0,
            brokers: brokers
                .into_iter()
                .map(|id| BrokerView {
                    id,
                    host: format!("h{id}"),
                    port: 9092,
                    rack: None,
                })
                .collect(),
            partitions: parts,
            in_flight_reassignments: vec![],
        }
    }

    fn part(topic: &str, partition: i32, replicas: Vec<i32>, leader: i32) -> PartitionView {
        let isr = replicas.clone();
        PartitionView {
            topic: topic.into(),
            partition,
            replicas,
            leader,
            isr,
        }
    }

    fn caps_with(broker: i32, max_replicas: u32) -> BrokerCapacities {
        let mut b = std::collections::HashMap::new();
        b.insert(
            broker,
            BrokerCapacity {
                max_replicas: Some(max_replicas),
                ..Default::default()
            },
        );
        BrokerCapacities { by_broker: b }
    }

    #[test]
    fn under_capacity_no_op() {
        // Broker 1 holds 3 replicas, max_replicas: 10. No movement.
        let parts = vec![
            part("t", 0, vec![1, 2], 1),
            part("t", 1, vec![1, 2], 1),
            part("t", 2, vec![1, 2], 1),
        ];
        let s = state_with(parts, vec![1, 2]);
        let mvs = ReplicaCapacity.propose(&s, &ctx_with(caps_with(1, 10)));
        assert!(mvs.is_empty(), "under-capacity must no-op, got {mvs:?}");
    }

    #[test]
    fn over_capacity_triggers_movement() {
        // Broker 1 holds 5 replicas, max_replicas: 3. Expect ≥2 movements.
        let parts: Vec<_> = (0..5).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        let mvs = ReplicaCapacity.propose(&s, &ctx_with(caps_with(1, 3)));
        assert!(!mvs.is_empty(), "over-capacity must emit movements");
        // Every movement must remove broker 1 from at least one partition.
        for m in &mvs {
            assert!(
                m.old_replicas.contains(&1) && !m.new_replicas.contains(&1)
                    || m.old_replicas.iter().filter(|x| **x == 1).count()
                        > m.new_replicas.iter().filter(|x| **x == 1).count(),
                "movement must reduce broker-1 replicas: {m:?}"
            );
        }
    }

    #[test]
    fn broker_without_entry_ignored() {
        // Broker 1 holds 5 replicas. No entry in capacities → no limit.
        let parts: Vec<_> = (0..5).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        let mvs = ReplicaCapacity.propose(&s, &ctx_with(BrokerCapacities::default()));
        assert!(mvs.is_empty(), "no entries must no-op, got {mvs:?}");
    }

    #[test]
    fn broker_with_no_max_replicas_field_ignored() {
        // Broker 1 has an entry but no max_replicas field → no limit.
        let mut b = std::collections::HashMap::new();
        b.insert(
            1,
            BrokerCapacity {
                max_replicas: None,
                disk_bytes: Some(1_000),
                ..Default::default()
            },
        );
        let caps = BrokerCapacities { by_broker: b };
        let parts: Vec<_> = (0..5).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        let mvs = ReplicaCapacity.propose(&s, &ctx_with(caps));
        assert!(mvs.is_empty(), "no max_replicas must no-op, got {mvs:?}");
    }
}
```

**Note on `is_satisfied`:** the body shown above intentionally returns `true` always. The `Goal::is_satisfied(&ClusterState)` signature (added in 43c) doesn't have access to `GoalContext`, so a strict capacity check inside `is_satisfied` isn't possible without a trait change. For 43d we accept that `ReplicaCapacity::is_satisfied` is over-permissive (always returns true); the propose-time enforcement does the real work. The optimizer's incremental hard-goal validation (slice 43c) still calls `is_satisfied` after every soft move, but for `ReplicaCapacity` that check is a no-op. **Document this in the goal docstring.** If 43e wants stricter composition guarantees, it can introduce a `Goal::is_satisfied_with_ctx(&ClusterState, &GoalContext)` trait method.

Replace the `is_satisfied` body in the file above with a clean form:

```rust
fn is_satisfied(&self, _state: &ClusterState) -> bool {
    // ReplicaCapacity's invariant depends on GoalContext.broker_capacities
    // which is_satisfied doesn't see. Returns true so soft goals can
    // proceed; propose-time enforcement is the real safety. See
    // slice 43d's design doc for the trade.
    true
}
```

- [ ] **Step 2: Run tests + clippy**

```bash
cargo test -p crabka-rebalancer --lib goals::replica_capacity -- --nocapture
```

Expected: 4 tests pass.

```bash
cargo clippy -p crabka-rebalancer --lib -- -D warnings 2>&1 | grep "replica_capacity"
```

Expected: no output. Standard clippy fixups if anything fires.

- [ ] **Step 3: Commit**

```bash
git -C /home/matt/git/crabka add crates/rebalancer/src/goals/replica_capacity.rs
git -C /home/matt/git/crabka commit -m "rebalancer(43d): ReplicaCapacity goal (hard)

Per-broker max_replicas enforcement from the operator-supplied
capacity config. Greedy: each iteration finds the broker with the
largest excess over its max_replicas, picks a destination broker
with headroom (preferring under-capacity brokers; falling back to
unconstrained brokers), emits a movement swapping one replica off.
Brokers without an entry — or with max_replicas: None — are ignored.

is_satisfied returns true unconditionally because the Goal trait
doesn't expose GoalContext to is_satisfied. Propose-time enforcement
is the actual safety; documented in the goal's docstring.

Four unit tests cover under-capacity no-op, over-capacity swap, no-entry
ignored, and missing-max_replicas ignored.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

### Task 4: Four capacity stubs (`DiskCapacity` / `NetworkInCapacity` / `NetworkOutCapacity` / `CpuCapacity`)

**Files:**
- Create: `crates/rebalancer/src/goals/disk_capacity.rs`
- Create: `crates/rebalancer/src/goals/network_in_capacity.rs`
- Create: `crates/rebalancer/src/goals/network_out_capacity.rs`
- Create: `crates/rebalancer/src/goals/cpu_capacity.rs`

The four stubs share the same shape. Write them in sequence.

- [ ] **Step 1: Write `crates/rebalancer/src/goals/disk_capacity.rs`**

```rust
//! Hard goal: enforce a per-broker `disk_bytes` limit from the
//! capacity config.
//!
//! **Stub in slice 43d.** `propose` returns empty and `is_satisfied`
//! returns true unconditionally — per-partition disk-usage data is
//! not available until slice 43e wires `metrics_scraper`. The struct
//! + registry entry + config-field reads ship now so 43e can replace
//! the body mechanically.

use crate::goals::{Goal, GoalContext, GoalPriority};
use crate::model::{ClusterState, Movement};

pub struct DiskCapacity;

impl DiskCapacity {
    pub const NAME: &'static str = "DiskCapacity";
}

impl Goal for DiskCapacity {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn priority(&self) -> GoalPriority {
        GoalPriority::Hard
    }

    fn propose(&self, _state: &ClusterState, _ctx: &GoalContext) -> Vec<Movement> {
        // Stub: 43e wires per-partition disk usage and the real logic.
        Vec::new()
    }

    fn is_satisfied(&self, _state: &ClusterState) -> bool {
        // Stub: 43e replaces this with a real capacity-vs-usage check.
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capacity::{BrokerCapacities, BrokerCapacity};
    use crate::model::{BrokerView, PartitionView};
    use std::sync::Arc;

    fn ctx() -> GoalContext {
        let mut by = std::collections::HashMap::new();
        by.insert(
            1,
            BrokerCapacity {
                disk_bytes: Some(100),
                ..Default::default()
            },
        );
        GoalContext {
            imbalance_threshold_pct: 10,
            max_movements_per_proposal: 256,
            min_topic_leaders_per_broker: 0,
            broker_capacities: Arc::new(BrokerCapacities { by_broker: by }),
        }
    }

    #[test]
    fn stub_returns_empty_regardless_of_state() {
        let state = ClusterState {
            cluster_id: None,
            snapshot_at_ms: 0,
            brokers: vec![BrokerView { id: 1, host: "h1".into(), port: 9092, rack: None }],
            partitions: vec![PartitionView {
                topic: "t".into(),
                partition: 0,
                replicas: vec![1],
                leader: 1,
                isr: vec![1],
            }],
            in_flight_reassignments: vec![],
        };
        assert!(DiskCapacity.propose(&state, &ctx()).is_empty());
        assert!(DiskCapacity.is_satisfied(&state));
    }
}
```

- [ ] **Step 2: Write `crates/rebalancer/src/goals/network_in_capacity.rs`**

Same shape as Step 1; substitute `NetworkInCapacity` for `DiskCapacity` and `network_in_bytes_per_sec: Some(125_000_000)` for `disk_bytes: Some(100)`. Full file:

```rust
//! Hard goal: enforce a per-broker `network_in_bytes_per_sec` limit
//! from the capacity config.
//!
//! **Stub in slice 43d.** `propose` returns empty and `is_satisfied`
//! returns true unconditionally — per-partition byte-in data is not
//! available until slice 43e wires `metrics_scraper`. The struct +
//! registry entry + config-field reads ship now so 43e can replace
//! the body mechanically.

use crate::goals::{Goal, GoalContext, GoalPriority};
use crate::model::{ClusterState, Movement};

pub struct NetworkInCapacity;

impl NetworkInCapacity {
    pub const NAME: &'static str = "NetworkInCapacity";
}

impl Goal for NetworkInCapacity {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn priority(&self) -> GoalPriority {
        GoalPriority::Hard
    }

    fn propose(&self, _state: &ClusterState, _ctx: &GoalContext) -> Vec<Movement> {
        Vec::new()
    }

    fn is_satisfied(&self, _state: &ClusterState) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capacity::{BrokerCapacities, BrokerCapacity};
    use crate::model::{BrokerView, PartitionView};
    use std::sync::Arc;

    fn ctx() -> GoalContext {
        let mut by = std::collections::HashMap::new();
        by.insert(
            1,
            BrokerCapacity {
                network_in_bytes_per_sec: Some(125_000_000),
                ..Default::default()
            },
        );
        GoalContext {
            imbalance_threshold_pct: 10,
            max_movements_per_proposal: 256,
            min_topic_leaders_per_broker: 0,
            broker_capacities: Arc::new(BrokerCapacities { by_broker: by }),
        }
    }

    #[test]
    fn stub_returns_empty_regardless_of_state() {
        let state = ClusterState {
            cluster_id: None,
            snapshot_at_ms: 0,
            brokers: vec![BrokerView { id: 1, host: "h1".into(), port: 9092, rack: None }],
            partitions: vec![PartitionView {
                topic: "t".into(),
                partition: 0,
                replicas: vec![1],
                leader: 1,
                isr: vec![1],
            }],
            in_flight_reassignments: vec![],
        };
        assert!(NetworkInCapacity.propose(&state, &ctx()).is_empty());
        assert!(NetworkInCapacity.is_satisfied(&state));
    }
}
```

- [ ] **Step 3: Write `crates/rebalancer/src/goals/network_out_capacity.rs`**

Same pattern. Substitute `NetworkOutCapacity` and `network_out_bytes_per_sec: Some(125_000_000)`. Full file:

```rust
//! Hard goal: enforce a per-broker `network_out_bytes_per_sec` limit
//! from the capacity config.
//!
//! **Stub in slice 43d.** `propose` returns empty and `is_satisfied`
//! returns true unconditionally — per-partition byte-out data is not
//! available until slice 43e wires `metrics_scraper`. The struct +
//! registry entry + config-field reads ship now so 43e can replace
//! the body mechanically.

use crate::goals::{Goal, GoalContext, GoalPriority};
use crate::model::{ClusterState, Movement};

pub struct NetworkOutCapacity;

impl NetworkOutCapacity {
    pub const NAME: &'static str = "NetworkOutCapacity";
}

impl Goal for NetworkOutCapacity {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn priority(&self) -> GoalPriority {
        GoalPriority::Hard
    }

    fn propose(&self, _state: &ClusterState, _ctx: &GoalContext) -> Vec<Movement> {
        Vec::new()
    }

    fn is_satisfied(&self, _state: &ClusterState) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capacity::{BrokerCapacities, BrokerCapacity};
    use crate::model::{BrokerView, PartitionView};
    use std::sync::Arc;

    fn ctx() -> GoalContext {
        let mut by = std::collections::HashMap::new();
        by.insert(
            1,
            BrokerCapacity {
                network_out_bytes_per_sec: Some(125_000_000),
                ..Default::default()
            },
        );
        GoalContext {
            imbalance_threshold_pct: 10,
            max_movements_per_proposal: 256,
            min_topic_leaders_per_broker: 0,
            broker_capacities: Arc::new(BrokerCapacities { by_broker: by }),
        }
    }

    #[test]
    fn stub_returns_empty_regardless_of_state() {
        let state = ClusterState {
            cluster_id: None,
            snapshot_at_ms: 0,
            brokers: vec![BrokerView { id: 1, host: "h1".into(), port: 9092, rack: None }],
            partitions: vec![PartitionView {
                topic: "t".into(),
                partition: 0,
                replicas: vec![1],
                leader: 1,
                isr: vec![1],
            }],
            in_flight_reassignments: vec![],
        };
        assert!(NetworkOutCapacity.propose(&state, &ctx()).is_empty());
        assert!(NetworkOutCapacity.is_satisfied(&state));
    }
}
```

- [ ] **Step 4: Write `crates/rebalancer/src/goals/cpu_capacity.rs`**

Same pattern. Substitute `CpuCapacity` and `cpu_cores: Some(8.0)`. Full file:

```rust
//! Hard goal: enforce a per-broker `cpu_cores` limit from the
//! capacity config.
//!
//! **Stub in slice 43d.** `propose` returns empty and `is_satisfied`
//! returns true unconditionally — per-partition CPU-usage data is
//! not available until slice 43e/43f wires `metrics_scraper`. The
//! struct + registry entry + config-field reads ship now so the
//! later slice can replace the body mechanically.

use crate::goals::{Goal, GoalContext, GoalPriority};
use crate::model::{ClusterState, Movement};

pub struct CpuCapacity;

impl CpuCapacity {
    pub const NAME: &'static str = "CpuCapacity";
}

impl Goal for CpuCapacity {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn priority(&self) -> GoalPriority {
        GoalPriority::Hard
    }

    fn propose(&self, _state: &ClusterState, _ctx: &GoalContext) -> Vec<Movement> {
        Vec::new()
    }

    fn is_satisfied(&self, _state: &ClusterState) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capacity::{BrokerCapacities, BrokerCapacity};
    use crate::model::{BrokerView, PartitionView};
    use std::sync::Arc;

    fn ctx() -> GoalContext {
        let mut by = std::collections::HashMap::new();
        by.insert(
            1,
            BrokerCapacity {
                cpu_cores: Some(8.0),
                ..Default::default()
            },
        );
        GoalContext {
            imbalance_threshold_pct: 10,
            max_movements_per_proposal: 256,
            min_topic_leaders_per_broker: 0,
            broker_capacities: Arc::new(BrokerCapacities { by_broker: by }),
        }
    }

    #[test]
    fn stub_returns_empty_regardless_of_state() {
        let state = ClusterState {
            cluster_id: None,
            snapshot_at_ms: 0,
            brokers: vec![BrokerView { id: 1, host: "h1".into(), port: 9092, rack: None }],
            partitions: vec![PartitionView {
                topic: "t".into(),
                partition: 0,
                replicas: vec![1],
                leader: 1,
                isr: vec![1],
            }],
            in_flight_reassignments: vec![],
        };
        assert!(CpuCapacity.propose(&state, &ctx()).is_empty());
        assert!(CpuCapacity.is_satisfied(&state));
    }
}
```

- [ ] **Step 5: Run targeted tests + clippy**

```bash
cargo test -p crabka-rebalancer --lib goals::disk_capacity goals::network_in_capacity goals::network_out_capacity goals::cpu_capacity -- --nocapture
```

Expected: 4 tests pass (one per file).

```bash
cargo clippy -p crabka-rebalancer --lib -- -D warnings 2>&1 | grep -E "disk_capacity|network_in_capacity|network_out_capacity|cpu_capacity"
```

Expected: no output.

- [ ] **Step 6: Commit**

```bash
git -C /home/matt/git/crabka add crates/rebalancer/src/goals/disk_capacity.rs crates/rebalancer/src/goals/network_in_capacity.rs crates/rebalancer/src/goals/network_out_capacity.rs crates/rebalancer/src/goals/cpu_capacity.rs
git -C /home/matt/git/crabka commit -m "rebalancer(43d): four capacity stub goals (Disk/NetworkIn/NetworkOut/Cpu)

Each is a hard goal whose propose returns empty and whose
is_satisfied returns true. Per-partition usage data isn't available
in 43d (arrives in 43e/43f); the struct + registry entry + config-
field surface area exist so the later slice can replace the body
mechanically. Four tests (one per file) assert the stub contract.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 4 — Registry + binary wiring

### Task 5: `GoalRegistry::default_registry` + CLI flag + capacity loader

**Files:**
- Modify: `crates/rebalancer/src/api/mod.rs`
- Modify: `crates/rebalancer/src/bin/rebalancer.rs`

- [ ] **Step 1: Extend `GoalRegistry::default_registry`**

Read `crates/rebalancer/src/api/mod.rs`. Find `default_registry`. Replace the body with:

```rust
pub fn default_registry() -> Self {
    Self {
        goals: vec![
            // Hard goals (priority order matters for the optimizer's Hard-first ordering).
            Box::new(crate::goals::preferred_leader_idempotency::PreferredLeaderIdempotency),
            Box::new(crate::goals::rack_aware::RackAware),
            Box::new(crate::goals::replica_capacity::ReplicaCapacity),
            Box::new(crate::goals::disk_capacity::DiskCapacity),
            Box::new(crate::goals::network_in_capacity::NetworkInCapacity),
            Box::new(crate::goals::network_out_capacity::NetworkOutCapacity),
            Box::new(crate::goals::cpu_capacity::CpuCapacity),
            // Soft goals.
            Box::new(crate::goals::replica_distribution::ReplicaDistribution),
            Box::new(crate::goals::leader_distribution::LeaderDistribution),
            Box::new(crate::goals::topic_replica_distribution::TopicReplicaDistribution),
            Box::new(crate::goals::min_topic_leaders_per_broker::MinTopicLeadersPerBroker),
        ],
    }
}
```

Update the `default_registry_has_six_goals` test in the same file. Rename to `default_registry_has_eleven_goals` and bump the assertion:

```rust
#[test]
fn default_registry_has_eleven_goals() {
    let r = GoalRegistry::default_registry();
    let all = r.select(&[]).unwrap();
    assert_eq!(all.len(), 11);
}
```

- [ ] **Step 2: Add the CLI flag + loader to `bin/rebalancer.rs`**

Read `crates/rebalancer/src/bin/rebalancer.rs`. Find the `Args` struct and add a new field near `data_dir`:

```rust
    /// Optional path to a per-broker capacity YAML file. When unset,
    /// all five capacity goals are no-ops.
    #[arg(long, env = "CRABKA_BROKER_CAPACITY_FILE", default_value = "")]
    broker_capacity_file: String,
```

(`String` rather than `Option<PathBuf>` so clap's `default_value = ""` semantics work; we treat empty as "no file".)

In `main`, add the loader call after the data-dir setup (around the existing `let store = ...` line). The exact placement is "before the `AppState` is constructed; after `client` is built":

```rust
    // Load broker capacity config (optional).
    let broker_capacities = if args.broker_capacity_file.is_empty() {
        std::sync::Arc::new(crabka_rebalancer::capacity::BrokerCapacities::default())
    } else {
        match crabka_rebalancer::capacity::load::load_from_path(
            std::path::Path::new(&args.broker_capacity_file),
        ) {
            Ok(c) => {
                info!(
                    path = %args.broker_capacity_file,
                    broker_count = c.by_broker.len(),
                    "loaded broker capacity config"
                );
                std::sync::Arc::new(c)
            }
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "failed to load broker capacity file `{}`: {e}",
                    args.broker_capacity_file
                ));
            }
        }
    };
```

Then update the `GoalContext { ... }` literal (T2 set the field to `Arc::new(BrokerCapacities::default())`; replace that hard-coded default with `broker_capacities.clone()`):

```rust
        goal_ctx: GoalContext {
            imbalance_threshold_pct: args.imbalance_threshold_pct,
            max_movements_per_proposal: args.max_movements_per_proposal,
            min_topic_leaders_per_broker: args.min_topic_leaders_per_broker,
            broker_capacities: broker_capacities.clone(),
        },
```

- [ ] **Step 3: Build + verify CLI**

```bash
cargo build -p crabka-rebalancer 2>&1 | tail -5
```

Expected: clean.

```bash
target/debug/crabka-rebalancer --help 2>&1 | grep "broker-capacity-file"
```

Expected: the new flag is listed.

- [ ] **Step 4: Run tests + clippy**

```bash
cargo test -p crabka-rebalancer --lib 2>&1 | tail -5
```

Expected: all lib tests pass (existing + the 4 stub tests + 4 ReplicaCapacity tests + the 6 capacity tests + the renamed `default_registry_has_eleven_goals` test).

```bash
cargo clippy -p crabka-rebalancer --all-targets -- -D warnings 2>&1 | tail -5
```

Expected: clean.

- [ ] **Step 5: Commit**

```bash
git -C /home/matt/git/crabka add crates/rebalancer/src/api/mod.rs crates/rebalancer/src/bin/rebalancer.rs
git -C /home/matt/git/crabka commit -m "rebalancer(43d): wire five capacity goals + --broker-capacity-file

GoalRegistry::default_registry grows from 6 to 11 goals. New CLI
flag --broker-capacity-file (env CRABKA_BROKER_CAPACITY_FILE,
default empty = no file). When empty, all five capacity goals are
no-ops via Arc<BrokerCapacities::default()>. When set, the binary
loads + parses the YAML at startup and threads the resulting Arc
into the AppState's GoalContext. Test default_registry_has_six_goals
renamed to default_registry_has_eleven_goals.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 5 — Integration test

### Task 6: `replica_capacity_evicts_over_capacity_broker` integration test

**Files:**
- Modify: `crates/rebalancer/tests/end_to_end.rs`

- [ ] **Step 1: Append the new test**

Append at the end of `crates/rebalancer/tests/end_to_end.rs`:

```rust
/// Synthetic three-broker ClusterState where broker 1 holds 10
/// replicas with `max_replicas: 5`. ReplicaCapacity must propose
/// movements that reduce broker 1's load.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replica_capacity_evicts_over_capacity_broker() {
    use crabka_rebalancer::capacity::{BrokerCapacities, BrokerCapacity};
    use crabka_rebalancer::goals::replica_capacity::ReplicaCapacity;
    use crabka_rebalancer::goals::{Goal, GoalContext};
    use crabka_rebalancer::model::{BrokerView, ClusterState, Movement, PartitionView};
    use std::collections::HashMap;
    use std::sync::Arc;

    let parts: Vec<_> = (0..10)
        .map(|i| PartitionView {
            topic: "t".into(),
            partition: i,
            replicas: vec![1, 2],
            leader: 1,
            isr: vec![1, 2],
        })
        .collect();

    let state = ClusterState {
        cluster_id: Some("c".into()),
        snapshot_at_ms: 0,
        brokers: vec![
            BrokerView { id: 1, host: "h1".into(), port: 9092, rack: None },
            BrokerView { id: 2, host: "h2".into(), port: 9092, rack: None },
            BrokerView { id: 3, host: "h3".into(), port: 9092, rack: None },
        ],
        partitions: parts,
        in_flight_reassignments: vec![],
    };

    let mut by_broker = HashMap::new();
    by_broker.insert(
        1,
        BrokerCapacity {
            max_replicas: Some(5),
            ..Default::default()
        },
    );
    let caps = BrokerCapacities { by_broker };

    let ctx = GoalContext {
        imbalance_threshold_pct: 10,
        max_movements_per_proposal: 256,
        min_topic_leaders_per_broker: 0,
        broker_capacities: Arc::new(caps),
    };

    let mvs: Vec<Movement> = ReplicaCapacity.propose(&state, &ctx);
    assert!(!mvs.is_empty(), "expected movements to evict broker 1; got {mvs:?}");

    // Every movement must reduce broker 1's replica count.
    for m in &mvs {
        let before = m.old_replicas.iter().filter(|x| **x == 1).count();
        let after = m.new_replicas.iter().filter(|x| **x == 1).count();
        assert!(
            after < before,
            "movement {m:?} doesn't reduce broker 1's replicas"
        );
    }

    // Apply movements to a working copy and verify broker 1's
    // post-state replica count is at or below 5.
    let mut working = state.partitions.clone();
    for m in &mvs {
        if let Some(p) = working
            .iter_mut()
            .find(|p| p.topic == m.topic && p.partition == m.partition)
        {
            p.replicas = m.new_replicas.clone();
        }
    }
    let final_broker_1_count: usize = working
        .iter()
        .map(|p| p.replicas.iter().filter(|x| **x == 1).count())
        .sum();
    assert!(
        final_broker_1_count <= 5,
        "broker 1 still has {final_broker_1_count} replicas after eviction"
    );
}
```

- [ ] **Step 2: Run the test**

```bash
cargo test -p crabka-rebalancer --test end_to_end replica_capacity_evicts_over_capacity_broker -- --nocapture 2>&1 | tail -10
```

Expected: 1 test passes.

```bash
cargo test -p crabka-rebalancer --test end_to_end 2>&1 | tail -5
```

Expected: 7 tests pass (6 existing + 1 new).

- [ ] **Step 3: Clippy**

```bash
cargo clippy -p crabka-rebalancer --tests -- -D warnings 2>&1 | tail -5
```

Expected: clean.

- [ ] **Step 4: Commit**

```bash
git -C /home/matt/git/crabka add crates/rebalancer/tests/end_to_end.rs
git -C /home/matt/git/crabka commit -m "rebalancer(43d): integration test for ReplicaCapacity eviction

Synthetic three-broker cluster, broker 1 with 10 replicas and
max_replicas: 5. Asserts ReplicaCapacity emits movements that
reduce broker 1's replica count, and that applying the movements
brings broker 1's final count to ≤5.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 6 — Helm chart

### Task 7: Helm chart updates + helm-unittest tests

**Files:**
- Modify: `charts/crabka-rebalancer/values.yaml`
- Modify: `charts/crabka-rebalancer/templates/deployment.yaml`
- Create: `charts/crabka-rebalancer/templates/configmap.yaml`
- Modify: `charts/crabka-rebalancer/tests/deployment_test.yaml`
- Create: `charts/crabka-rebalancer/tests/configmap_test.yaml`

- [ ] **Step 1: Update `values.yaml`**

Read the current file. After `minTopicLeadersPerBroker: 0`, add:

```yaml
# Per-broker capacity table for the five capacity goals (slice 43d).
# Empty map = no capacity limits → all five capacity goals are no-ops.
brokerCapacities: {}
#   Example:
#   brokerCapacities:
#     1:
#       max_replicas: 4096
#       disk_bytes: 1099511627776
#       network_in_bytes_per_sec: 125000000
#       network_out_bytes_per_sec: 125000000
#       cpu_cores: 8.0

# Override path for the capacity file. When non-empty, takes
# precedence over `brokerCapacities` — the chart sets
# CRABKA_BROKER_CAPACITY_FILE to this path and does NOT render the
# ConfigMap (operator is responsible for providing the file).
brokerCapacityFile: ""
```

- [ ] **Step 2: Write `templates/configmap.yaml`**

```yaml
{{- if and .Values.brokerCapacities (not .Values.brokerCapacityFile) }}
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ include "rebalancer.fullname" . }}-capacity
  labels:
    {{- include "rebalancer.labels" . | nindent 4 }}
data:
  capacity.yaml: |
    version: 1
    brokers:
      {{- range $broker, $caps := .Values.brokerCapacities }}
      "{{ $broker }}":
        {{- toYaml $caps | nindent 8 }}
      {{- end }}
{{- end }}
```

- [ ] **Step 3: Update `templates/deployment.yaml`**

Read the current file. Three modifications:

a) **env entry** — after `CRABKA_MIN_TOPIC_LEADERS_PER_BROKER`, add:

```yaml
            {{- if .Values.brokerCapacityFile }}
            - name: CRABKA_BROKER_CAPACITY_FILE
              value: {{ .Values.brokerCapacityFile | quote }}
            {{- else if .Values.brokerCapacities }}
            - name: CRABKA_BROKER_CAPACITY_FILE
              value: /etc/crabka-rebalancer/capacity.yaml
            {{- end }}
```

b) **volumeMounts** — after the existing `data` volume mount, add:

```yaml
            {{- if and .Values.brokerCapacities (not .Values.brokerCapacityFile) }}
            - name: capacity-config
              mountPath: /etc/crabka-rebalancer
              readOnly: true
            {{- end }}
```

c) **volumes** — after the existing `data` volume, add:

```yaml
        {{- if and .Values.brokerCapacities (not .Values.brokerCapacityFile) }}
        - name: capacity-config
          configMap:
            name: {{ include "rebalancer.fullname" . }}-capacity
        {{- end }}
```

- [ ] **Step 4: Extend `tests/deployment_test.yaml`**

Append a new `- it:` entry:

```yaml
  - it: passes brokerCapacityFile env when brokerCapacities is set
    set:
      brokerCapacities:
        1:
          max_replicas: 4096
    asserts:
      - contains:
          path: spec.template.spec.containers[0].env
          content:
            name: CRABKA_BROKER_CAPACITY_FILE
            value: /etc/crabka-rebalancer/capacity.yaml
      - contains:
          path: spec.template.spec.containers[0].volumeMounts
          content:
            name: capacity-config
            mountPath: /etc/crabka-rebalancer
            readOnly: true
      - contains:
          path: spec.template.spec.volumes
          content:
            name: capacity-config
            configMap:
              name: demo-crabka-rebalancer-capacity
```

- [ ] **Step 5: Write `tests/configmap_test.yaml`**

```yaml
suite: capacity configmap
templates:
  - configmap.yaml
release:
  name: demo
set:
  bootstrapServers: kafka-bootstrap:9092
tests:
  - it: not rendered when brokerCapacities is empty
    asserts:
      - hasDocuments:
          count: 0

  - it: rendered with broker entries when brokerCapacities is non-empty
    set:
      brokerCapacities:
        1:
          max_replicas: 4096
          disk_bytes: 1099511627776
    asserts:
      - hasDocuments:
          count: 1
      - equal:
          path: kind
          value: ConfigMap
      - equal:
          path: metadata.name
          value: demo-crabka-rebalancer-capacity
      - matchRegex:
          path: data["capacity.yaml"]
          pattern: 'version:\s*1'
      - matchRegex:
          path: data["capacity.yaml"]
          pattern: 'max_replicas:\s*4096'

  - it: not rendered when brokerCapacityFile overrides
    set:
      brokerCapacities:
        1:
          max_replicas: 4096
      brokerCapacityFile: /custom/path/capacity.yaml
    asserts:
      - hasDocuments:
          count: 0
```

- [ ] **Step 6: Verify chart**

If `helm` + `helm-unittest` plugin are locally available:

```bash
helm lint /home/matt/git/crabka/charts/crabka-rebalancer --set bootstrapServers=test:9092 2>&1 | tail -3
```

Expected: `1 chart(s) linted, 0 chart(s) failed`.

```bash
helm template demo /home/matt/git/crabka/charts/crabka-rebalancer --set bootstrapServers=test:9092 --set brokerCapacities.1.max_replicas=4096 2>&1 | grep "kind:" | sort -u
```

Expected: `kind: ConfigMap`, `kind: Deployment`, `kind: PersistentVolumeClaim`, `kind: Service`, `kind: ServiceAccount`.

```bash
helm unittest /home/matt/git/crabka/charts/crabka-rebalancer 2>&1 | tail -10
```

Expected: all suites pass.

If `helm` is not installed locally, skip these checks — CI runs them in the `helm-lint` job.

- [ ] **Step 7: Commit**

```bash
git -C /home/matt/git/crabka add charts/crabka-rebalancer/values.yaml charts/crabka-rebalancer/templates/configmap.yaml charts/crabka-rebalancer/templates/deployment.yaml charts/crabka-rebalancer/tests/deployment_test.yaml charts/crabka-rebalancer/tests/configmap_test.yaml
git -C /home/matt/git/crabka commit -m "rebalancer(43d): Helm chart capacity ConfigMap + helm-unittest coverage

values.yaml gains brokerCapacities (map) + brokerCapacityFile
(override path). New templates/configmap.yaml renders the per-broker
capacity YAML when brokerCapacities is non-empty and the override
path is empty. deployment.yaml conditionally mounts the ConfigMap +
sets CRABKA_BROKER_CAPACITY_FILE. New tests/configmap_test.yaml
suite asserts the empty / non-empty / override-path renders.
deployment_test.yaml gains one assertion that the env + mount land
when brokerCapacities is set.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 7 — Docs

### Task 8: STATUS

**Files:**
- Modify: `STATUS.md`

- [ ] **Step 1: Append slice-43d entry**

`STATUS.md` is chronological-append-at-end. Append:

```markdown
## Slice 43d — Rebalancer capacity goals (2026-05-17)

- Five new hard goals shipped under the existing `Goal` trait:
  - `ReplicaCapacity` (fully functional): enforces per-broker
    `max_replicas` from the operator-supplied capacity config.
    Greedy hot→cold evict for any broker over its limit.
  - `DiskCapacity` / `NetworkInCapacity` / `NetworkOutCapacity` /
    `CpuCapacity` (stubs): structs + registry entries + config-field
    reads ship now; `propose` returns empty and `is_satisfied`
    returns true unconditionally until slice 43e wires per-partition
    metrics. 43e replaces the bodies mechanically.
- New top-level `capacity` module (parallel to `goals`/`model`/
  `optimizer`) owns the `BrokerCapacities` + `BrokerCapacity` types
  and the YAML loader. Sparse-by-design: missing field = no limit
  for that resource on that broker; missing broker entry = no limits
  at all.
- New CLI flag `--broker-capacity-file` (env
  `CRABKA_BROKER_CAPACITY_FILE`, default empty). When unset, all
  five capacity goals are no-ops. When set, the binary loads + parses
  the YAML at startup and threads an `Arc<BrokerCapacities>` into
  the `AppState`'s `GoalContext`.
- `GoalContext` gains `broker_capacities: Arc<BrokerCapacities>`.
  The `Copy` bound is dropped (`Clone` is cheap via `Arc` bump;
  verified: every existing caller takes `&GoalContext`, zero-friction).
- `GoalRegistry::default_registry` now contains **11 goals** in
  priority order: `PreferredLeaderIdempotency`, `RackAware`,
  `ReplicaCapacity`, `DiskCapacity`, `NetworkInCapacity`,
  `NetworkOutCapacity`, `CpuCapacity` (Hard); `ReplicaDistribution`,
  `LeaderDistribution`, `TopicReplicaDistribution`,
  `MinTopicLeadersPerBroker` (Soft).
- Helm chart picks up an optional ConfigMap-based config: new
  `brokerCapacities` (map) + `brokerCapacityFile` (override path)
  in `values.yaml`; new `templates/configmap.yaml`; deployment
  conditionally mounts the ConfigMap + sets the env var. New
  `helm-unittest` suite `configmap_test.yaml` (3 tests) plus 1 new
  assertion in `deployment_test.yaml`.
- 14 new unit tests (6 capacity + 4 ReplicaCapacity + 4 stub) + 1
  new integration test (`replica_capacity_evicts_over_capacity_broker`).
- Reference doc:
  [`docs/superpowers/specs/2026-05-17-crabka-rebalancer-43d-design.md`].
- Out of scope (deferred): per-partition usage data + the four
  metric-dependent capacity goals' real bodies (43e); `CpuUsage`
  soft goal (43f); per-topic resource hints in the capacity config;
  dynamic capacity discovery; capacity-aware leader election.

### Known trade
- `ReplicaCapacity::is_satisfied` returns `true` unconditionally
  because the `Goal::is_satisfied(&ClusterState)` signature doesn't
  expose `GoalContext` (added in 43c without ctx access). Capacity
  enforcement happens at `propose` time only. If 43e needs stricter
  composition guarantees, a `Goal::is_satisfied_with_ctx` trait
  method can be introduced then.
```

- [ ] **Step 2: Final verification**

```bash
cargo fmt --check 2>&1 | tail -3
cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -3
cargo test -p crabka-rebalancer 2>&1 | tail -10
```

All three must pass clean. If `cargo fmt --check` reports differences, run `cargo fmt` and commit separately before this docs commit.

- [ ] **Step 3: Commit**

```bash
git -C /home/matt/git/crabka add STATUS.md
git -C /home/matt/git/crabka commit -m "rebalancer(43d): STATUS

Slice 43d entry documenting the five new capacity goals
(ReplicaCapacity functional + four stubs), the new capacity module
+ YAML config, the new CLI flag, the GoalContext Copy→Clone
migration, the registry growth to 11 goals, the Helm ConfigMap
wiring, and the known trade on ReplicaCapacity::is_satisfied.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Self-review checklist

**1. Spec coverage:**
- Capacity types + YAML loader → T1
- `GoalContext.broker_capacities` + drop Copy → T2
- 5 new module mounts + every literal site updated → T2
- ReplicaCapacity (real) → T3
- Four stubs (Disk/NetworkIn/NetworkOut/Cpu) → T4
- `GoalRegistry::default_registry` 11 goals + binary CLI flag + loader call → T5
- Integration test for ReplicaCapacity → T6
- Helm chart values + configmap + deployment + tests → T7
- STATUS entry → T8

**2. Placeholder scan:** No "TBD" / "implement later" / "similar to" patterns. Each step has concrete code or commands. The four stub goal files are written out in full rather than as "similar to Step 1" because the engineer may read tasks out of order.

**3. Type consistency:** `BrokerCapacities`, `BrokerCapacity`, `GoalContext`, `Goal`, `ClusterState`, `Movement`, `PartitionView`, `BrokerView`, `Arc<BrokerCapacities>` referenced identically across tasks. Field names (`broker_capacities`, `max_replicas`, `disk_bytes`, `network_in_bytes_per_sec`, `network_out_bytes_per_sec`, `cpu_cores`) consistent everywhere.
