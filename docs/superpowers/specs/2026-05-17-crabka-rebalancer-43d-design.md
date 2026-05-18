# Crabka rebalancer — slice 43d — capacity goals (design)

**Date:** 2026-05-17
**Status:** Spec, ready for implementation plan
**Scope:** Five new hard goals — `ReplicaCapacity` (fully functional) plus `DiskCapacity` / `NetworkInCapacity` / `NetworkOutCapacity` / `CpuCapacity` (stubs pending 43e's per-partition metrics) — plus a per-broker capacity YAML config loaded from `--broker-capacity-file`.

## Goal

Land slice 43d: the rebalancer enforces operator-supplied per-broker capacity limits. `ReplicaCapacity` works today (the input is replica counts, available in every snapshot). The other four goals ship as type-level stubs — their `propose()` returns empty and their `is_satisfied()` returns true until 43e wires per-partition usage data.

## Out of scope (deferred)

- **Per-partition usage data.** Slice 43e ships `metrics_scraper` for per-partition byte counters; the four metric-dependent capacity goals stay no-op stubs until then.
- **`CpuUsage`** (soft goal, separate from `CpuCapacity`) — slice 43f.
- **Per-topic resource hints in the capacity config.** Defer to a future slice if operators ask before 43e ships.
- **Dynamic capacity discovery** (e.g., inferring disk from broker `/metrics`). All capacities are operator-supplied.
- **Capacity-aware leader election.** Capacity goals operate on replica placement; leader balance is the existing `LeaderDistribution` / `MinTopicLeadersPerBroker` story.

## Decisions captured during brainstorm

1. **Stub the four metric-dependent goals.** Ship all five goal types so the registry and CLI config infrastructure are in place; the four that need per-partition usage data (`Disk`/`NetworkIn`/`NetworkOut`/`Cpu`Capacity) return empty `Vec<Movement>` from `propose` and `true` from `is_satisfied` until 43e replaces the bodies.
2. **`--broker-capacity-file` is optional.** Default = no file = `BrokerCapacities` is empty = every broker has unlimited capacity = all five goals are no-ops. Operators opt in by passing the flag.
3. **`GoalContext` gains `broker_capacities: Arc<BrokerCapacities>`.** Drops the existing `Copy` bound on `GoalContext`; `Clone` is cheap (Arc bump).
4. **Per-broker entries are sparse.** Missing field = no limit for that resource on that broker. Missing broker entry = no limits at all for that broker. Both are operator-explicit "this is unconstrained" signals.
5. **Goal registry order:** the five new Hard goals slot between the existing Hard goals (`PreferredLeaderIdempotency`, `RackAware`) and the Soft goals. Order within Hard: `PreferredLeaderIdempotency`, `RackAware`, `ReplicaCapacity`, `DiskCapacity`, `NetworkInCapacity`, `NetworkOutCapacity`, `CpuCapacity`. The metric-dependent stubs are last in Hard order so when 43e fills them in, they run after `ReplicaCapacity` (replica count is the cheapest invariant to enforce).

## Component layout

```
crates/rebalancer/
├── src/
│   ├── capacity/
│   │   ├── mod.rs                                  # NEW — BrokerCapacities + BrokerCapacity types + tests
│   │   └── load.rs                                 # NEW — pure file-read + YAML parse + version check
│   ├── goals/
│   │   ├── mod.rs                                  # MODIFIED — GoalContext.broker_capacities (drops Copy); 5 new pub mod
│   │   ├── replica_capacity.rs                     # NEW — fully-functional hard goal
│   │   ├── disk_capacity.rs                        # NEW — stub
│   │   ├── network_in_capacity.rs                  # NEW — stub
│   │   ├── network_out_capacity.rs                 # NEW — stub
│   │   └── cpu_capacity.rs                         # NEW — stub
│   ├── api/mod.rs                                  # MODIFIED — GoalRegistry::default_registry adds 5 new goals
│   ├── bin/rebalancer.rs                           # MODIFIED — new CLI flag, loader call, GoalContext wiring
│   └── lib.rs                                      # MODIFIED — pub mod capacity;
├── tests/end_to_end.rs                             # MODIFIED — 1 new integration test
charts/crabka-rebalancer/
├── values.yaml                                      # MODIFIED — brokerCapacities (inline map) + optional brokerCapacityFile override
├── templates/
│   ├── deployment.yaml                              # MODIFIED — mount the capacity ConfigMap + set CRABKA_BROKER_CAPACITY_FILE
│   └── configmap.yaml                               # NEW — only rendered when .Values.brokerCapacities is non-empty
└── tests/
    ├── deployment_test.yaml                         # MODIFIED — assert mount + env when brokerCapacities set
    └── configmap_test.yaml                          # NEW — assert ConfigMap renders iff non-empty values
STATUS.md                                            # MODIFIED — slice 43d entry
```

The `capacity` module is a new top-level module (parallel to `goals` / `model` / `optimizer`) because:
- Parsing + types are distinct from goal logic.
- Slice 43e will reuse the same types for usage tables.
- The YAML loader has co-located unit tests and a natural home.

Five new goal files keep each goal in its own focused module — consistent with the 43c pattern. The four stubs are ~40 lines each (the same boilerplate: trait impl returning empty Vec, `is_satisfied` returning true, one no-op unit test).

## Capacity config

### File format

YAML at `--broker-capacity-file` (env `CRABKA_BROKER_CAPACITY_FILE`, optional — no default). Schema version 1:

```yaml
version: 1
brokers:
  1:
    max_replicas: 4096
    disk_bytes: 1099511627776          # 1 TiB
    network_in_bytes_per_sec: 125000000   # 1 Gbps
    network_out_bytes_per_sec: 125000000
    cpu_cores: 8.0
  2:
    max_replicas: 4096
    disk_bytes: 1099511627776
    # network/cpu omitted: those resources have no limit on broker 2
  # brokers not listed get no limits at all
```

### Rust types

```rust
// crates/rebalancer/src/capacity/mod.rs

use std::collections::HashMap;

#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct BrokerCapacities {
    pub by_broker: HashMap<i32, BrokerCapacity>,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct BrokerCapacity {
    pub max_replicas: Option<u32>,
    pub disk_bytes: Option<u64>,
    pub network_in_bytes_per_sec: Option<u64>,
    pub network_out_bytes_per_sec: Option<u64>,
    pub cpu_cores: Option<f64>,
}

// crates/rebalancer/src/capacity/load.rs
pub fn load_from_path(path: &std::path::Path) -> Result<BrokerCapacities, CapacityError> { ... }
```

`Default` impls give an empty `BrokerCapacities` (every broker uncapped) without forcing a file path.

### Wiring

`GoalContext` gains:

```rust
pub broker_capacities: std::sync::Arc<crate::capacity::BrokerCapacities>,
```

The binary entry constructs the `Arc`:

```rust
let capacities = if let Some(path) = args.broker_capacity_file {
    Arc::new(crate::capacity::load::load_from_path(&path)?)
} else {
    Arc::new(BrokerCapacities::default())
};
```

…and threads it into both the test fixture's and production's `GoalContext` literals.

Dropping `Copy` on `GoalContext` is a one-line change in `goals/mod.rs`; every call site that depended on implicit Copy needs an explicit `.clone()` (Arc bump — cheap).

## Goal semantics

### `ReplicaCapacity` (hard, fully functional)

**Invariant:** for every broker with a `max_replicas` entry in the config, the count of replicas hosted on that broker must not exceed `max_replicas`.

**Emit shape:** for each over-capacity broker, pick the partition with the highest replica count (deterministic by `(topic, partition)` ordering on ties) and emit a movement that swaps one replica from the over-capacity broker to an under-capacity broker. Repeat until no broker exceeds its `max_replicas` or no valid swap remains. Bounded by `ctx.max_movements_per_proposal`.

**Brokers without a config entry:** ignored — the goal has no opinion. Brokers with `max_replicas: None`: also ignored.

**`is_satisfied`:** returns `false` if any broker exceeds its `max_replicas`; `true` otherwise. Used by the optimizer's incremental hard-goal validation (slice 43c's footgun fix).

### `DiskCapacity` / `NetworkInCapacity` / `NetworkOutCapacity` / `CpuCapacity` (hard, stubs)

**`propose`:** returns empty `Vec<Movement>` unconditionally. No usage data is available without 43e's metric scraping.

**`is_satisfied`:** returns `true` unconditionally.

**Type definitions:** real struct + Goal impl + module file. Each goal reads its corresponding capacity field from `ctx.broker_capacities` even though it doesn't use it yet — the field access pattern is exercised so 43e's wire-up is mechanical.

**Unit test (per goal):** one test asserting `propose` returns empty regardless of capacity config + cluster state. Documents the stub contract.

## Helm chart updates

### `values.yaml` additions

```yaml
# Per-broker capacity table. Empty map = no capacity limits (all five
# capacity goals become no-ops).
brokerCapacities: {}
#   Example:
#   brokerCapacities:
#     1:
#       max_replicas: 4096
#       disk_bytes: 1099511627776
#       network_in_bytes_per_sec: 125000000
#       network_out_bytes_per_sec: 125000000
#       cpu_cores: 8.0

# Override path for the capacity file. Defaults to a path inside the
# pod where the chart's ConfigMap is mounted when `brokerCapacities`
# is non-empty.
brokerCapacityFile: ""
```

### `templates/configmap.yaml` (new)

```yaml
{{- if .Values.brokerCapacities }}
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

### `templates/deployment.yaml` (modified)

Add the env var + volume mount when `brokerCapacities` is non-empty:

```yaml
# In container.env:
{{- if .Values.brokerCapacities }}
- name: CRABKA_BROKER_CAPACITY_FILE
  value: /etc/crabka-rebalancer/capacity.yaml
{{- end }}

# In container.volumeMounts:
{{- if .Values.brokerCapacities }}
- name: capacity-config
  mountPath: /etc/crabka-rebalancer
  readOnly: true
{{- end }}

# In spec.template.spec.volumes:
{{- if .Values.brokerCapacities }}
- name: capacity-config
  configMap:
    name: {{ include "rebalancer.fullname" . }}-capacity
{{- end }}
```

**Precedence:** if `brokerCapacityFile` is set (non-empty string), it takes priority — the chart sets `CRABKA_BROKER_CAPACITY_FILE` to that path and does **not** render the ConfigMap (operator is responsible for providing the file via their own mechanism, e.g., an existing ConfigMap or a CSI driver). If `brokerCapacityFile` is empty and `brokerCapacities` is non-empty, the chart renders the ConfigMap and points the env var at the in-pod mount. If both are empty, neither is rendered and the capacity goals are no-ops.

## Testing

### Unit tests (in-crate `#[cfg(test)]`)

- **`capacity::tests`** (5 tests):
  - `default_is_empty`
  - `load_round_trips_full_file`
  - `load_errors_on_missing_file` — the loader returns an error; we don't auto-default. The "no file" path lives in the binary entry, which calls `BrokerCapacities::default()` when the CLI flag is absent.
  - `load_omits_missing_fields_as_none`
  - `load_rejects_unsupported_version`

- **`replica_capacity::tests`** (4 tests):
  - `under_capacity_no_op`
  - `over_capacity_triggers_movement` — broker 1 holds 10 replicas, `max_replicas: 5`; assert ≥1 movement off broker 1.
  - `broker_without_entry_ignored`
  - `is_satisfied_reflects_over_capacity`

- **Stub tests** (1 per stub × 4 = 4 tests): each asserts `propose` returns empty regardless of state + config; documents the stub contract.

### Integration test (1 in `tests/end_to_end.rs`)

- **`replica_capacity_evicts_over_capacity_broker`**: synthetic three-broker `ClusterState`, broker 1 holds 10 replicas, broker 1 capacity = 5; build `GoalContext` with the capacity, assert `ReplicaCapacity.propose` emits movements reducing broker 1's replica count.

### Helm unittest

- **`configmap_test.yaml`** (new suite, 2 tests):
  - With `brokerCapacities: {}`: ConfigMap is **not** rendered.
  - With one broker entry: ConfigMap renders + payload contains `version: 1` + `max_replicas:` field.

- **`deployment_test.yaml`** (extended, 1 new test):
  - With `brokerCapacities` set: `CRABKA_BROKER_CAPACITY_FILE` env var + capacity-config volume mount both present.

## Risks

- **`GoalContext` Copy → Clone migration churn.** Every existing call site that depends on Copy needs `.clone()`. The plan enumerates the call sites in T1 so the implementer can fix them in one pass. Grep-discoverable via `grep -rn "GoalContext " --include="*.rs"`.
- **YAML field naming consistency.** Rust struct fields are snake_case; serde's default Deserialize matches that, so `max_replicas` / `disk_bytes` etc. work directly. No `#[serde(rename = ...)]` needed unless we want camelCase YAML — sticking with snake_case keeps it idiomatic Rust + readable YAML.
- **`f64` for `cpu_cores`.** A capacity goal that compares CPU usage needs to handle NaN/infinity carefully. Since the field is operator-supplied and used by a stub in 43d, validation deferred to 43f when CPU usage actually drives a movement. The loader rejects negative values to catch obvious config typos.
- **`is_satisfied` semantics for the four stubs.** Returning `true` always means a soft goal could undo capacity invariants once 43e wires usage. The slice-43c composition guarantee assumes `is_satisfied` is conservative; the stubs are intentionally optimistic until 43e replaces them. Documented in the goal docstrings.

## Acceptance criteria

1. `cargo test -p crabka-rebalancer` — all existing tests pass + ~14 new unit tests + 1 new integration test.
2. `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings` clean.
3. `helm lint charts/crabka-rebalancer --set bootstrapServers=test:9092` clean.
4. `helm unittest charts/crabka-rebalancer` clean (existing 5 suites + new `configmap_test.yaml` suite).
5. `STATUS.md` gains a slice-43d entry.
6. `--broker-capacity-file <path>` accepted by the binary; absent → all five capacity goals are no-ops; present → `ReplicaCapacity` actively enforces, four stubs remain no-op pending 43e.

## File layout (summary)

```
crates/rebalancer/
├── src/
│   ├── capacity/                                    # NEW module
│   │   ├── mod.rs
│   │   └── load.rs
│   ├── goals/
│   │   ├── mod.rs                                   # MODIFIED
│   │   ├── replica_capacity.rs                      # NEW
│   │   ├── disk_capacity.rs                         # NEW (stub)
│   │   ├── network_in_capacity.rs                   # NEW (stub)
│   │   ├── network_out_capacity.rs                  # NEW (stub)
│   │   └── cpu_capacity.rs                          # NEW (stub)
│   ├── api/mod.rs                                   # MODIFIED — registry
│   ├── bin/rebalancer.rs                            # MODIFIED — CLI flag + loader
│   └── lib.rs                                       # MODIFIED — pub mod capacity;
└── tests/end_to_end.rs                              # MODIFIED — 1 new test
charts/crabka-rebalancer/
├── values.yaml                                       # MODIFIED
├── templates/configmap.yaml                          # NEW
├── templates/deployment.yaml                         # MODIFIED
└── tests/
    ├── deployment_test.yaml                          # MODIFIED
    └── configmap_test.yaml                           # NEW
STATUS.md                                             # MODIFIED
```
