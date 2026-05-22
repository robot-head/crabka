# Crabka Operator — `KafkaRebalance` CRD (slice 44) design

**Date:** 2026-05-22
**Status:** Implemented
**Slice:** 44 (operator roadmap Phase 7; rebalancer roadmap "operator-side follow-up")

## Goal

Surface the standalone `crabka-rebalancer` service (slices 43a–43g) through
a Kubernetes CRD so operators drive rebalances declaratively instead of by
hand-poking the Connect-RPC API. This closes Phase 7 of the operator
roadmap: the rebalancer service was fully built (advisor + executor +
topology/capacity/usage/CPU goals + anomaly detector) but had no operator
front-end.

This is **pure operator work** — no Crabka-core dependency. Every RPC the
controller needs (`CreateProposal`, `GetProposal`, `ExecuteProposal`,
`CancelExecution`) already exists on the rebalancer's Connect service.

## CRD shape (`crabka.io/v1alpha1`, kind `KafkaRebalance`, short `kr`)

Strimzi-shaped, trimmed to what the Crabka rebalancer's API actually
accepts:

```yaml
apiVersion: crabka.io/v1alpha1
kind: KafkaRebalance
metadata:
  name: my-rebalance
  namespace: kafka
  labels:
    crabka.io/cluster: my-cluster   # optional; used to derive the endpoint
spec:
  goals: ["RackAware", "ReplicaDistribution"]   # optional (default registry if unset)
  throttleBytesPerSec: 52428800                  # optional (KIP-73 execute throttle)
  endpoint: http://my-cluster-rebalancer.kafka.svc:9300  # optional override
```

- **`goals`** → `CreateProposal.goals`. Empty/unset = the rebalancer's full
  default goal registry, in priority order.
- **`throttleBytesPerSec`** → `ExecuteProposal.throttle_bytes_per_sec`.
- **`endpoint`** → the rebalancer's Connect base URL. When unset, the
  operator derives
  `http://<cluster>-rebalancer.<namespace>.svc.cluster.local:9300` from the
  `crabka.io/cluster` label. With neither, the CR goes `NotReady` /
  `MissingEndpoint`.

Strimzi's `mode` (`full` / `add-brokers` / `remove-brokers`) and `brokers`
fields are **out of scope** — the Crabka rebalancer's `CreateProposal` is
goal-driven against the live cluster snapshot and has no broker-scoped
mode parameter (yet). When the rebalancer grows broker-scoped modes a
follow-up slice adds the fields.

### Status

```yaml
status:
  conditions:
    - type: ProposalReady   # the active state lives in the condition type
      status: "True"
      reason: ProposalReady
      message: "proposal 4f3c… computed: 8 replica / 3 leader movements"
      lastTransitionTime: "2026-05-22T…Z"
  sessionId: 4f3c…          # rebalancer proposal id
  observedGeneration: 2
  optimizationResult:
    replicaMovements: 8
    leaderMovements: 3
    maxReplicasBefore: 12
    maxReplicasAfter: 9
    maxLeadersBefore: 6
    maxLeadersAfter: 4
    goals: ["RackAware", "ReplicaDistribution"]
```

The active **state is encoded as the condition `type`** (Strimzi
convention), one of: `ProposalReady`, `Rebalancing`, `Ready`, `NotReady`,
`Stopped` (plus an internal `New` before the first reconcile writes
status).

## State machine (annotation-driven, Strimzi-shaped)

The `crabka.io/rebalance` annotation carries one-shot commands —
`approve`, `refresh`, `stop` — consumed (deleted) once acted on.

```text
 (new) ──CreateProposal──▶ ProposalReady ──approve──▶ Rebalancing
                                │ refresh                   │ poll
                                ▼                    ┌───────┼─────────┐
                          (recompute)             Ready  NotReady  (stop→Stopped)
```

The decision core is a pure function
`decide(state, command, has_session) -> RebalanceAction` (fully unit-tested
in isolation); the reconcile fn only does I/O. Mapping:

| state \ command | (none) | approve | refresh | stop |
|-----------------|--------|---------|---------|------|
| New | CreateProposal | — | CreateProposal | — |
| ProposalReady | Idle | **Execute** | CreateProposal | Idle |
| Rebalancing | **Poll** | Idle | CreateProposal | **Cancel** |
| Ready/NotReady/Stopped | Idle | Idle | CreateProposal | Idle |

RPC results map back to states: `Computed → ProposalReady`, `Executing →
Rebalancing`, `Completed → Ready`, `Failed → NotReady`, `Cancelled →
Stopped`. `Rebalancing` requeues at 10s (poll cadence); other states at
5min (awaiting human action — the watch wakes the loop immediately when an
annotation lands).

The optimizer call never moves data; only `approve` (→ `ExecuteProposal`)
does. A human (or GitOps approval) stays in the loop.

## Connect-RPC client

The rebalancer speaks **Connect** (slice 43a) — unary `POST` to
`/crabka.rebalancer.v1.Rebalancer/<Method>` with a JSON or protobuf body.
The operator speaks the **JSON flavor** via a small `reqwest`-backed client
(`rebalancer_client::ConnectRebalancerClient`) and hand-rolled
serde DTOs, so the operator stays decoupled from the rebalancer's
prost/pbjson codegen.

Decode tolerates the proto3-JSON quirks pbjson emits: lowerCamelCase field
names, enums as their proto value name (`PROPOSAL_STATUS_COMPUTED`),
64-bit ints as strings, and default-valued fields omitted entirely. Connect
errors (non-2xx with `{code,message}`) map to `RebalancerError::Rpc`;
transport failures leave status untouched so the next reconcile retries
(the computed proposal isn't lost to a transient blip).

A `RebalancerClientLike` trait is the test seam, mirroring the existing
`AdminClientLike` pattern: production wraps `ConnectRebalancerClient`,
reconcile tests substitute `FakeRebalancerClient`. Both are cached per
endpoint on `Context`, evicted on transport error.

## Wiring

- `crd/rebalance.rs` — CRD types; `gen_crds.rs` emits
  `deploy/crds/crabka.io_kafkarebalances.yaml`.
- `controller/rebalance.rs` — reconciler + pure state machine.
- `rebalancer_client.rs` — Connect/JSON client + trait + decode.
- `context.rs` — per-endpoint client cache + test-injection seam.
- `run.rs` — controller spawned alongside the other four.
- Helm ClusterRole gains `kafkarebalances` + `kafkarebalances/status`.

## Tests

- **~44 lib unit tests:** CRD round-trip/defaults; Connect-JSON decode
  (enum names, numeric ordinals, nested `proposal`, omitted defaults,
  failure reason, Connect error → Rpc, int parsing); the full `decide`
  matrix; outcome mapping; `current_state`; `read_command`;
  `resolve_endpoint`.
- **7 reconcile integration tests** (`reconcile_rebalance.rs`) against a
  faked rebalancer + the FIFO mock kube transport: create→ProposalReady,
  approve→Rebalancing (+ throttle forwarded + annotation consumed),
  poll→Ready, stop→Stopped, poll-failure→NotReady, missing-endpoint, and
  transport-error-leaves-status-untouched.
- **1 end-to-end wire test** (`rebalance_e2e.rs`): the operator's *real*
  `ConnectRebalancerClient` driven over HTTP against the *real* rebalancer
  Connect router (served in-process against a real single-broker Crabka).
  Verifies `CreateProposal`/`GetProposal` round-trips, the
  `not_found` error on an unknown id, and the `failed_precondition` error
  on executing a zero-movement proposal. This is the wire-compatibility
  contract the unit tests can only assume.

## Out of scope / deferred

- `spec.mode` (`add-brokers` / `remove-brokers`) + `spec.brokers` — needs
  rebalancer-side support first.
- `DryRunProposal` surfacing (the rebalancer computes against the live
  snapshot; the proposal summary already serves the dry-run need).
- A finalizer that auto-cancels an in-flight execution on CR delete — the
  rebalancer's execution continues independently; the operator just stops
  tracking it. (Re-add if operators ask for delete-cancels-rebalance.)
- `auto-approval` annotation modes / scheduling.
- kind-e2e (deploy operator + rebalancer chart + drive a real
  `KafkaRebalance` to `Ready`) — a CI follow-up; operator-side wiring is
  covered by the in-process e2e wire test.
