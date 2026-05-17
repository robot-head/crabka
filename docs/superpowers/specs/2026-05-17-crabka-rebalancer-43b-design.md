# Crabka rebalancer — slice 43b — execute path (design)

**Date:** 2026-05-17
**Status:** Spec, ready for implementation plan
**Scope:** Wire the rebalancer's `ExecuteProposal` Connect RPC to the cluster — KIP-455 `AlterPartitionReassignments`, KIP-73 throttle apply/clear, progress polling, on-disk persistence, restart recovery, cancel path, and a production Helm chart.

## Goal

Land slice 43b: the rebalancer transitions from advisor (43a) to executor. Operators can call `ExecuteProposal` and watch a proposal drive real partition reassignments through `AlterPartitionReassignments` under a `IncrementalAlterConfigs`-managed KIP-73 throttle, then resume cleanly if the rebalancer restarts mid-execution. Slice 43b also ships the production Helm chart at `charts/crabka-rebalancer/`, displacing 43a's placeholder.

## Out of scope (deferred to later slices)

- Multi-replica HA / leader election. Single-replica deployment only; cluster-wide locking and moving persistent state off local disk wait until later slices need them.
- Metric scraping for usage goals (slice 43e).
- Rack-aware / capacity / usage / CPU / anomaly goals (slices 43c–43g).
- Operator `KafkaRebalance` CRD (slice 44).
- Pause / step-through execution. Cancel is the only intervention.
- Adaptive throttle. Rate is static for the lifetime of one execution.
- Cross-cluster migration.

## Decisions captured during brainstorm

1. **Concurrency:** at most one execution at a time. `ExecuteProposal` returns `Code::FailedPrecondition` if another is in-flight.
2. **Cancel:** new `CancelExecution` RPC ships in 43b. Reverts pending reassignments (KIP-455 cancel) and clears the throttle.
3. **Restart recovery:** re-issue `AlterPartitionReassignments`. KIP-455 is idempotent against the same target replica set — completed movements no-op, pending ones continue. Matches Cruise Control's behavior.
4. **`CreateProposal` during execution:** runs normally, computed against the current (transition-state) snapshot. Only `ExecuteProposal` is gated on the in-flight slot.
5. **Throttle config:** per-proposal `throttle_bytes_per_sec` field on `ExecuteProposalRequest`, defaulting from a new CLI flag `--default-throttle-bytes-per-sec` (default 50_000_000, i.e. 50 MB/s per broker direction). Matches Cruise Control's typical default.

## Architecture

### New crate module

`crates/rebalancer/src/executor/` (new top-level module). Internal layout:

- `mod.rs` — `Executor` public struct, `Execution` state machine, `ExecutionHandle` (the `Arc<Mutex<Option<…>>>` slot stored on `AppState`).
- `phases.rs` — the four `Phase` enum variants and the per-phase action functions (`apply_throttle`, `submit`, `wait`, `clear_throttle`).
- `throttle.rs` — pure-logic `compute_throttle_targets(movements: &[Movement]) -> ThrottleTargets`. No I/O.
- `state.rs` — `Execution`'s persistent shape (`InFlightFile`), serde definitions, atomic-rename write helper.

The `executor` module consumes raw `crabka_protocol` request/response types via `Client::send`, mirroring the ingester pattern from 43a. No new typed wrappers in `crabka-client-core`.

### Execution state machine

Four phases. The current phase is persisted to `{data_dir}/in_flight.json` after every transition.

```
   ┌─────────────────────────────────────────────────────────┐
   │                                                         │
   │  ApplyThrottle ──► Submit ──► Wait ──► ClearThrottle    │
   │       │              │          │            │          │
   │       │              │          │            ▼          │
   │       └──────────────┴──────────┴───►   (Terminal:      │
   │                 (failure path)         Completed /      │
   │                                        Failed /         │
   │                                        Cancelled)       │
   │                                                         │
   └─────────────────────────────────────────────────────────┘
```

Failure semantics:
- If `ApplyThrottle` fails, we **do not** start `Submit` — proposal goes to `Failed` and `ClearThrottle` still runs to undo any partial set.
- If `Submit` fails (broker rejects e.g. `INVALID_REPLICA_ASSIGNMENT`), `ClearThrottle` runs and proposal goes to `Failed`.
- If `Wait` times out (per-execution deadline, default 30 minutes, configurable via `--execute-deadline-secs`), in-flight reassignments are cancelled (KIP-455 with `null` replicas), `ClearThrottle` runs, proposal goes to `Failed`.
- Cancel via `CancelExecution` from any phase: cancel in-flight reassignments → `ClearThrottle` → `Cancelled`.

`ClearThrottle` runs in **every** terminal path via a guard so the broker never gets stuck.

### State sharing

`AppState` gains:

```rust
pub struct AppState {
    pub snapshot: SharedSnapshot,
    pub store: Arc<ProposalStore>,
    pub goal_registry: GoalRegistry,
    pub goal_ctx: GoalContext,
    pub metrics: RebalancerMetrics,
    // new in 43b:
    pub client: Client,                                   // for the executor's admin calls
    pub data_dir: PathBuf,
    pub in_flight: Arc<Mutex<Option<ExecutionHandle>>>,   // None when idle; Some when executing
    pub default_throttle_bytes_per_sec: i64,
    pub poll_interval: Duration,
    pub execute_deadline: Duration,
}
```

`ExecutionHandle`:

```rust
pub struct ExecutionHandle {
    pub proposal_id: String,
    pub task: JoinHandle<()>,
    pub cancel: CancellationToken,
    pub started_at: Instant,
}
```

`ExecuteProposal` acquires the mutex, inserts a fresh `ExecutionHandle`, drops the lock, spawns the task. The task is responsible for clearing the slot on terminal — it calls `state.in_flight.lock().take()` from a `Drop`-guarded helper so a panic doesn't leak the slot.

`CancelExecution` acquires the mutex, signals `handle.cancel`. The task observes cancellation in its select loop, transitions through the cancel path, clears the slot.

## Persistence

Two JSON files under `--data-dir` (default `/var/lib/crabka-rebalancer`, mkdir on startup).

### `proposals.json` — full ProposalStore ring buffer

Schema (versioned):

```json
{
  "version": 1,
  "capacity": 20,
  "proposals": [
    {"id": "...", "status": "Completed", ...},
    ...
  ]
}
```

Written atomically (`{path}.tmp` + rename) after every mutation: `CreateProposal`, `ExecuteProposal` (status transitions), `CancelExecution`. Order in `proposals` is oldest-to-newest (matching the in-memory `VecDeque`).

`ProposalStore::insert` and the status-mutation paths take an `Arc<Persister>` that owns the on-disk file. The persister batches writes that arrive within a short coalesce window (default 10ms) to avoid serial fsync storms during a busy execution; on Drop or explicit `flush`, it always writes.

### `in_flight.json` — active-execution marker

Schema:

```json
{
  "version": 1,
  "proposal_id": "...",
  "phase": "Submit",
  "started_at_ms": 1715900000000,
  "throttle_bytes_per_sec": 50000000,
  "target_terminal_status": null
}
```

`phase` is one of `ApplyThrottle`, `Submit`, `Wait`, `ClearThrottle`. `target_terminal_status` is `null` while the execution is still in flight; it is set to one of `Completed` / `Failed` / `Cancelled` **before** transitioning into `ClearThrottle`, so a restart-during-clear knows which terminal state to commit to. `failure_reason` (when target = `Failed`) is also stamped at the same time.

Written at the start of `ApplyThrottle`, updated on every phase transition (and on `target_terminal_status` stamping), **deleted** when `ClearThrottle` finishes. Its existence is the recovery signal.

### Recovery flow on startup

1. Load `proposals.json` → populate the in-memory `ProposalStore`.
2. If `in_flight.json` exists:
   - Look up the referenced proposal in the store. If missing (corrupted state), log + delete `in_flight.json` + bail (proposal goes to `Failed` with `failure_reason = "recovery: in_flight proposal not in store"`).
   - Re-mark its status as `Executing` in memory (it would have been left at whatever phase the persist beat the crash).
   - Spawn an `Execution::resume(phase)` task.
3. `Execution::resume`:
   - `phase == ApplyThrottle` or `Submit` → re-run that phase. Idempotent in Kafka.
   - `phase == Wait` → re-enter the polling loop directly.
   - `phase == ClearThrottle` → re-run clear (idempotent DELETE), then commit `target_terminal_status` (always non-null in this case) to the proposal and delete `in_flight.json`.

## API surface

### `pb::ProposalStatus` enum (extended)

```proto
enum ProposalStatus {
  PROPOSAL_STATUS_UNSPECIFIED = 0;
  PROPOSAL_STATUS_COMPUTED = 1;
  PROPOSAL_STATUS_EXECUTING = 2;     // new
  PROPOSAL_STATUS_COMPLETED = 3;     // new
  PROPOSAL_STATUS_FAILED = 4;        // new
  PROPOSAL_STATUS_CANCELLED = 5;     // new
}
```

Internal `model::ProposalStatus` (Rust) mirrors the enum 1:1.

### `pb::Proposal` (new fields)

```proto
message Proposal {
  string id = 1;
  ProposalStatus status = 2;
  int64 created_at_ms = 3;
  repeated string goals_applied = 4;
  ProposalSummary summary = 5;
  repeated Movement movements = 6;
  // new in 43b:
  int64 started_at_ms = 7;
  int64 terminated_at_ms = 8;
  optional string failure_reason = 9;
  int64 throttle_bytes_per_sec = 10;
}
```

### `ExecuteProposal` (was a stub)

Request:

```proto
message ExecuteProposalRequest {
  string id = 1;
  optional int64 throttle_bytes_per_sec = 2;  // overrides --default-throttle-bytes-per-sec
}
```

Response: `{proposal}` — already transitioned to `Executing`, fields `started_at_ms` and `throttle_bytes_per_sec` populated.

Errors:
- `NotFound` — no proposal with that id
- `FailedPrecondition` — another execution in-flight, OR proposal in terminal state, OR proposal has zero movements
- `Internal` — could not persist `in_flight.json` (disk error)

`ExecuteProposal` is async: it returns immediately, and the operator polls `GetProposal` for progress.

### `CancelExecution` (new)

```proto
rpc CancelExecution (CancelExecutionRequest) returns (CancelExecutionResponse);

message CancelExecutionRequest {
  string id = 1;
}

message CancelExecutionResponse {
  Proposal proposal = 1;
}
```

Errors:
- `NotFound` — no execution in-flight
- `FailedPrecondition` — in-flight id doesn't match request id (defends against racing a stale operator UI)

Cancel signals the execution's `CancellationToken`; the task transitions through the cancel path and clears the in-flight slot. The response is the `Proposal` already transitioned to `Cancelled`.

### CLI flags

New on `crabka-rebalancer`:

- `--data-dir` (env `CRABKA_DATA_DIR`, default `/var/lib/crabka-rebalancer`) — already named in 43a's design but not wired; now wired.
- `--default-throttle-bytes-per-sec` (env `CRABKA_DEFAULT_THROTTLE_BYTES_PER_SEC`, default 50_000_000)
- `--execute-deadline-secs` (env `CRABKA_EXECUTE_DEADLINE_SECS`, default 1800 = 30 minutes)
- `--reassignment-poll-interval-secs` (env `CRABKA_REASSIGNMENT_POLL_INTERVAL_SECS`, default 5)
- `--reassignment-batch-size` (env `CRABKA_REASSIGNMENT_BATCH_SIZE`, default 200) — movements per `AlterPartitionReassignments` request

## Throttle strategy (KIP-73)

Four config keys, set via `IncrementalAlterConfigs`:

| Config | Resource type | Value |
|--------|---------------|-------|
| `leader.replication.throttled.rate` | BROKER (per broker id) | bytes/sec |
| `follower.replication.throttled.rate` | BROKER (per broker id) | bytes/sec |
| `leader.replication.throttled.replicas` | TOPIC | `partition:broker,partition:broker,...` |
| `follower.replication.throttled.replicas` | TOPIC | `partition:broker,partition:broker,...` |

Computation from `Proposal.movements`:

- **Affected leader brokers** = union of `old_replicas` across all movements (sources)
- **Affected follower brokers** = union of `new_replicas \ old_replicas` across all movements (true new replicas)
- **Affected topics** = unique topic names
- Per affected topic:
  - `leader.replication.throttled.replicas` = for each movement on this topic, append `{partition}:{r}` for each `r` in `old_replicas`
  - `follower.replication.throttled.replicas` = for each movement on this topic, append `{partition}:{r}` for each `r` in `new_replicas \ old_replicas`

`ApplyThrottle` issues one `IncrementalAlterConfigs` request with all four config families batched (SET op_type).

`ClearThrottle` issues one `IncrementalAlterConfigs` request that DELETEs the same four keys (op_type DELETE) on the same resources.

If we have to chunk for request-size reasons, the chunking key is "configs per resource"; we never split a logical apply across phases.

## Recovery / restart

Covered in [Persistence](#persistence). One additional note: the recovery path runs **before** the axum listener starts accepting connections (sequentially in `main`). That guarantees `GetProposal` reads after the snapshot loader land never see a brief "Executing → Computed → Executing" status flip caused by lazy resume.

## Helm chart

`charts/crabka-rebalancer/` ships:

- `Chart.yaml` — `appVersion` tracks the crate version
- `values.yaml` — `image.{repository,tag,pullPolicy}`, `bootstrapServers` (required, no default), `listenAddr` (default `0.0.0.0:9300`), `scrapeIntervalSecs`, `imbalanceThresholdPct`, `maxMovementsPerProposal`, `proposalRingBufferSize`, `throttle.defaultBytesPerSec`, `executeDeadlineSecs`, `reassignmentPollIntervalSecs`, `reassignmentBatchSize`, `persistence.size` (default `1Gi`), `persistence.storageClass`, `resources`, `nodeSelector`, `tolerations`, `affinity`
- `templates/_helpers.tpl` — name + selector boilerplate, matching the operator chart's conventions
- `templates/deployment.yaml` — `Deployment` with `replicas: 1`, `strategy: Recreate` (releases the RWO PVC before the next pod starts). Single container with env-var-bound CLI flags. Liveness probe on `/healthz`, readiness on `/readyz`.
- `templates/service.yaml` — ClusterIP exposing 9300
- `templates/serviceaccount.yaml` — empty ServiceAccount; no ClusterRole/RoleBinding. Rebalancer talks to Crabka over the wire protocol, not k8s API.
- `templates/persistentvolumeclaim.yaml` — RWO PVC mounted at `/var/lib/crabka-rebalancer`. `accessModes: [ReadWriteOnce]`.

**Chart tests** under `charts/crabka-rebalancer/tests/`:

- `deployment_test.yaml` — `replicas: 1`, `strategy.type: Recreate`, container env vars match values, probes wired to /healthz + /readyz, PVC mounted at `/var/lib/crabka-rebalancer`
- `required_values_test.yaml` — rendering fails if `bootstrapServers` unset
- `service_test.yaml` — `type: ClusterIP`, port `9300` → `9300`
- `pvc_test.yaml` — `accessModes: [ReadWriteOnce]`, size from values
- `rbac_test.yaml` — ServiceAccount present, no ClusterRole/RoleBinding generated

CI: extend the existing `helm-lint` job to:

1. `helm lint charts/crabka-rebalancer`
2. `helm template demo charts/crabka-rebalancer --set bootstrapServers=test:9092 > /tmp/rendered.yaml`
3. Grep assertions for required `kind:` resources (mirroring the operator chart's existing pattern).
4. Install the `helm-unittest` plugin (`helm plugin install https://github.com/helm-unittest/helm-unittest`) and run `helm unittest charts/crabka-rebalancer`.

The operator chart can adopt helm-unittest in a follow-up (out of scope for 43b).

## Testing

### Unit tests (in-crate `#[cfg(test)]`)

- `executor::tests` — drive the state machine with a mock `Client` (small trait the executor uses, with a `MockClient` impl in tests). Cover: happy path (Apply → Submit → Wait → Clear), failure mid-Submit (Failed terminal), Cancel during Wait, idempotent resume from each persisted phase.
- `executor::throttle::tests` — pure-logic test that, given a Vec<Movement>, computes the right `(broker, leader_rate)`, `(broker, follower_rate)`, `(topic, leader_replicas)`, `(topic, follower_replicas)` sets. No I/O.
- `model::store::tests` — extend with on-disk persistence round-trip (write → read → assert equal), atomic-rename behavior, schema version handling.
- `api::tests` — handler-level `ExecuteProposal` returns `FailedPrecondition` when (a) another in-flight, (b) proposal terminal, (c) proposal has zero movements. `CancelExecution` returns `NotFound` when no in-flight, `FailedPrecondition` on id mismatch.

### Integration tests (`tests/end_to_end.rs`, extended)

- `execute_proposal_settles_against_real_broker` — boot a single-broker Crabka, create three topics, deliberately stuff replicas onto broker 1 (since the test helper only spins one broker, the movements are recorded but the broker may reject them; if so, the test asserts the `Failed` path with a useful failure_reason). Multi-broker test deferred to whenever the broker crate's test helpers grow a `boot_cluster(n)`.
- `cancel_clears_throttle_and_reverts` — Execute (against a balanced cluster where Submit will be a no-op but ApplyThrottle still fires), Cancel immediately, assert status `Cancelled`, assert throttle configs are cleared via `IncrementalAlterConfigs` follow-up read.
- `restart_resumes_in_flight_plan` — Execute, kill the executor task (cancel its task handle directly), construct a fresh `AppState` pointed at the same data dir, assert recovery loads `in_flight.json` and the plan transitions to `Completed`.

### Connect HTTP smoke (`tests/connect_smoke.rs`, extended)

- Add an `ExecuteProposal` → poll `GetProposal` round-trip. Proves the new RPC is reachable over the JSON wire.

### Helm unittest

Five test files under `charts/crabka-rebalancer/tests/` listed above. Run by `helm unittest charts/crabka-rebalancer` in CI.

## Risks

- **KIP-73 throttle on dynamic-config-immutable brokers.** If the Crabka broker rejects dynamic config writes for the throttle keys (regression or unimplemented), `ApplyThrottle` fails the proposal at the first phase. Defensive: capture the broker's error response in `failure_reason` so operators can diagnose. Pre-check during implementation: verify slice 15b actually wires the four keys.
- **Resume race.** If the rebalancer restarts during the brief window between writing `in_flight.json` and issuing the AlterPartitionReassignments, `Execution::resume(Submit)` will issue the request. If it restarts in the *other* direction (the request was issued but the persist did not happen), resume will re-issue. Both are idempotent against KIP-455. We need to ensure persist happens **before** the network call for every phase transition (current design does this).
- **Mid-execution cluster topology change.** A broker death between `Submit` and `Wait` may leave a movement unable to complete. The deadline (`--execute-deadline-secs`) catches this; the proposal goes to `Failed` with `failure_reason = "deadline exceeded"`. Operator must manually re-create a proposal against the new topology.

## Acceptance criteria

1. `cargo test -p crabka-rebalancer` passes — existing 43a tests + the new executor/persistence/api tests + the three new e2e tests + the extended Connect smoke test.
2. `cargo fmt --check` and `cargo clippy --workspace --all-targets -- -D warnings` clean.
3. `helm lint charts/crabka-rebalancer` clean.
4. `helm unittest charts/crabka-rebalancer` clean (5 test files pass).
5. CI's `helm-lint` job is updated to lint + render-check + helm-unittest the new chart.
6. `STATUS.md` gains a slice-43b entry; README's "Cruise-Control-equivalent rebalancer (executor)" row flips from ❌ to ✅.
7. Manual smoke (called out in the plan, not a CI job): operator runs the binary against a real Crabka, invokes ExecuteProposal, observes successful settle. Documented as a one-liner in STATUS.

## File layout (summary of new/changed paths)

```
Cargo.toml                                                # no change (43b is rebalancer-only)
crates/rebalancer/
├── Cargo.toml                                            # MODIFIED — new deps if any (likely just serde for InFlightFile)
├── proto/crabka/rebalancer/v1/rebalancer.proto           # MODIFIED — extended ProposalStatus, new Proposal fields, CancelExecution RPC
├── src/
│   ├── lib.rs                                            # MODIFIED — mount executor module
│   ├── bin/rebalancer.rs                                 # MODIFIED — new CLI flags, executor wiring, recovery on startup
│   ├── executor/
│   │   ├── mod.rs                                        # NEW
│   │   ├── phases.rs                                     # NEW
│   │   ├── throttle.rs                                   # NEW
│   │   └── state.rs                                      # NEW
│   ├── api/
│   │   ├── handlers.rs                                   # MODIFIED — ExecuteProposal body, new CancelExecution handler
│   │   └── mod.rs                                        # MODIFIED — wire CancelExecution into the builder
│   ├── model/
│   │   ├── proposal.rs                                   # MODIFIED — new ProposalStatus variants + Proposal fields
│   │   └── store.rs                                      # MODIFIED — Persister + atomic write, status mutators
│   └── metrics.rs                                        # MODIFIED — new counters (executions_started_total, etc.)
└── tests/
    ├── end_to_end.rs                                     # MODIFIED — three new integration tests
    └── connect_smoke.rs                                  # MODIFIED — ExecuteProposal round-trip
charts/crabka-rebalancer/                                 # NEW (entire directory)
├── Chart.yaml
├── values.yaml
├── templates/
│   ├── _helpers.tpl
│   ├── deployment.yaml
│   ├── service.yaml
│   ├── serviceaccount.yaml
│   └── persistentvolumeclaim.yaml
└── tests/
    ├── deployment_test.yaml
    ├── required_values_test.yaml
    ├── service_test.yaml
    ├── pvc_test.yaml
    └── rbac_test.yaml
.github/workflows/ci.yml                                  # MODIFIED — helm-lint job adds helm-unittest install + run
README.md                                                 # MODIFIED — executor row → ✅
STATUS.md                                                 # MODIFIED — slice 43b entry
```
