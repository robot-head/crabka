# Gres Load-test Runtime Policy Implementation Plan

**Goal:** Configure the Gres load-test harness's remaining operational limits,
timeouts, retry policy, sampling, chaos-proxy behavior, and report selection
while preserving existing behavior.

**Architecture:** Resolve one validated `LoadtestRuntimePolicy` from flattened
CLI arguments backed by `CRABKA_GRES_LOADTEST_*` environment variables. Carry
it through internal and external runs to each runtime owner. Use UOM for every
dimensioned value and `refined_type`-validated newtypes for positive counts.

### Task 1: Policy and CLI boundary

- [x] Add policy defaults and invariant tests.
- [x] Add unit-bearing CLI/environment options to `run` and `compare`.
- [x] Make compare's HLC offset use the shared environment-backed boundary.
- [x] Run focused CLI and policy tests; commit.

### Task 2: Cluster and chaos runtime

- [x] Thread policy through process launch/kill/log drain and broker polling.
- [x] Thread policy through WAL-topic creation and diagnostic log-tail limits.
- [x] Thread policy through chaos-proxy burst and delay-queue behavior.
- [x] Preserve default public constructors for existing callers and tests.
- [x] Run focused cluster/proxy tests and strict Clippy; commit.

### Task 3: Workload and fault runtime

- [ ] Thread retry, connection, operation, startup, shutdown, reconnect,
  histogram, pacing, read-slice, and seed-batch policy through the workload.
- [ ] Thread the minimum flap period through fault validation/execution.
- [ ] Preserve scenario semantics and existing defaults.
- [ ] Run focused workload/fault tests and strict Clippy; commit.

### Task 4: Sampling and reports

- [ ] Thread the resource sampling interval through internal and external runs.
- [ ] Thread fault-window, timeline-cap, and deviation policy through rendering.
- [ ] Preserve default report-rendering entry points.
- [ ] Run focused runner/report tests and strict Clippy; commit.

### Task 5: Closure

- [ ] Run Gres load-test all-target tests and strict Clippy.
- [ ] Run workspace all-target check and strict Clippy.
- [ ] Run nightly formatting and diff hygiene.
- [ ] Update `docs/configuration-audit.md` and commit closure docs.
- [ ] Leave the repository-wide goal active.

Fixed values remain fixed where configurability has no useful meaning: tenant
and TLS identities, broker cluster ID, range/table and worker-ID layout,
protocol encodings, socket read chunk size, and the one-byte throttle arithmetic
safety floor.
