# Slice 43a — Rebalancer foundation (design)

**Date:** 2026-05-17
**Status:** Design approved, ready for implementation plan
**Reference roadmap:** [`2026-05-17-crabka-rebalancer-roadmap-design.md`](2026-05-17-crabka-rebalancer-roadmap-design.md)

## Goal

Land a standalone `crabka-rebalancer` binary that connects to a Crabka cluster as an admin client, periodically snapshots cluster state, and exposes a Connect-RPC service for "what would balance this cluster?" proposals. **No execute path** in 43a — the executor and persistence land in slice 43b.

The slice ships the first three goals: replica-count balance (soft), leader-count balance (soft), and preferred-leader idempotency (hard).

## Non-goals

- No execute path. `ExecuteProposal` returns `Code::Unimplemented` with `"execute path lands in slice 43b"`. Slice 43b wires up `AlterPartitionReassignments` + KIP-73 throttle.
- No persistence. Proposals live in an in-memory ring buffer (default 20 most recent). Restarting the rebalancer drops them.
- No metric scraping. Goals operate on metadata + replica/leader counts only. `DryRunResponse.estimated_bytes_moved` is always 0 until slice 43e adds per-partition byte counters.
- No rack-aware / capacity / usage / anomaly logic — those land in 43c–43g.
- No operator integration. The `KafkaRebalance` CRD ships in operator slice 44 after 43b.
- No auth on the Connect endpoint. Mounted on a `NetworkPolicy`-gated internal port; slice 44 adds a token gate. The bind address defaults to `0.0.0.0:9300` and a non-Crabka-aware operator could expose it accidentally — flagged in the README's deployment section.

## Architecture

### Crate layout

```
crates/rebalancer/
├── Cargo.toml
├── build.rs                                  # connectrpc-axum-build codegen
├── proto/
│   └── crabka/rebalancer/v1/rebalancer.proto
├── src/
│   ├── lib.rs                                # public surface for tests
│   ├── bin/
│   │   └── rebalancer.rs                     # crabka-rebalancer binary (clap CLI)
│   ├── api/
│   │   ├── mod.rs                            # service impl + axum mount helpers
│   │   └── handlers.rs                       # one fn per RPC method
│   ├── ingest/
│   │   ├── mod.rs                            # Ingester + snapshot loop
│   │   └── admin_client.rs                   # thin wrapper over crabka_client_core::Client
│   ├── model/
│   │   ├── mod.rs                            # ClusterState, BrokerView, PartitionView
│   │   ├── proposal.rs                       # Proposal, Movement, ProposalSummary, ProposalStatus
│   │   └── store.rs                          # in-memory ProposalStore (UUID-keyed ring buffer)
│   ├── goals/
│   │   ├── mod.rs                            # Goal trait, GoalContext, GoalPriority
│   │   ├── preferred_leader_idempotency.rs   # hard
│   │   ├── replica_distribution.rs           # soft
│   │   └── leader_distribution.rs            # soft
│   ├── optimizer/
│   │   └── mod.rs                            # optimize(state, goals, ctx) -> Proposal
│   └── health.rs                             # /healthz /readyz /metrics
└── tests/
    └── end_to_end.rs                         # in-process broker + ingester + service
```

### Crate dependencies (additions to the workspace)

Workspace-level (`Cargo.toml`):
- `connectrpc = "0.4"`
- `connectrpc-axum = "0.1"`
- `connectrpc-axum-build = "0.1"`
- `prost = "0.13"` (matches `connectrpc`'s expected version when this lands)

`crates/rebalancer/Cargo.toml`:
- `crabka-client-core` (admin client)
- `crabka-protocol` (typed requests for Metadata / DescribeCluster / ListPartitionReassignments)
- `crabka-metadata` (`NodeId`)
- `axum` (operational endpoints) — already in workspace
- `prometheus-client` — already in workspace
- `arc-swap` — already in workspace
- `serde_json`, `tokio`, `tracing`, `clap`, `anyhow`, `uuid`, `thiserror` — already in workspace
- Dev: `tempfile`, `crabka-broker` with `test-helpers`, `tower`

### Process shape & CLI

One binary, `crabka-rebalancer`. Single-replica only in 43a. CLI flags (mirroring the operator binary, env-overridable):

```
--bootstrap-servers <host:port,host:port>   [env CRABKA_BOOTSTRAP_SERVERS]
--listen-addr 0.0.0.0:9300                  [env CRABKA_REBALANCER_LISTEN_ADDR]
--scrape-interval-secs 10                   [env CRABKA_SCRAPE_INTERVAL_SECS]
--imbalance-threshold-pct 10                [env CRABKA_IMBALANCE_THRESHOLD_PCT]
--max-movements-per-proposal 256            [env CRABKA_MAX_MOVEMENTS_PER_PROPOSAL]
--proposal-ring-buffer-size 20              [env CRABKA_PROPOSAL_RING_BUFFER_SIZE]
```

Operational endpoints (plain axum routes, not Connect):
- `GET /healthz` — 200 always
- `GET /readyz` — 200 after first successful state snapshot; 503 before
- `GET /metrics` — OpenMetrics text (own `prometheus-client` registry; metrics surface starts small: `crabka_rebalancer_snapshot_at_ms`, `crabka_rebalancer_snapshots_total`, `crabka_rebalancer_proposals_created_total`)

Connect endpoints mount under `/crabka.rebalancer.v1.Rebalancer/<MethodName>` (Connect's default path convention).

### Connect-RPC service

Stack: `connectrpc` 0.4 (protocol types) + `connectrpc-axum` 0.1 (axum integration) + `connectrpc-axum-build` 0.1 (build-time codegen of server stubs). `prost` for the generated messages.

Proto file (`crates/rebalancer/proto/crabka/rebalancer/v1/rebalancer.proto`):

```proto
syntax = "proto3";
package crabka.rebalancer.v1;

service Rebalancer {
  rpc GetState(GetStateRequest) returns (GetStateResponse);
  rpc CreateProposal(CreateProposalRequest) returns (Proposal);
  rpc DryRunProposal(DryRunProposalRequest) returns (DryRunResponse);
  rpc GetProposal(GetProposalRequest) returns (Proposal);
  rpc ListProposals(ListProposalsRequest) returns (ListProposalsResponse);
  rpc ExecuteProposal(ExecuteProposalRequest) returns (ExecuteProposalResponse);
}

message Broker {
  int32 id = 1; string host = 2; int32 port = 3; optional string rack = 4;
}

message Partition {
  int32 partition = 1; repeated int32 replicas = 2; int32 leader = 3; repeated int32 isr = 4;
}

message Topic { string name = 1; repeated Partition partitions = 2; }

message InFlightReassignment {
  string topic = 1; int32 partition = 2;
  repeated int32 adding_replicas = 3; repeated int32 removing_replicas = 4;
}

message GetStateRequest {}
message GetStateResponse {
  int64 snapshot_at_ms = 1;
  repeated Broker brokers = 2;
  repeated Topic topics = 3;
  repeated InFlightReassignment in_flight_reassignments = 4;
}

enum ProposalStatus {
  PROPOSAL_STATUS_UNSPECIFIED = 0;
  PROPOSAL_STATUS_COMPUTED = 1;
  // Reserved for slice 43b:
  // PROPOSAL_STATUS_EXECUTING = 2;
  // PROPOSAL_STATUS_COMPLETED = 3;
  // PROPOSAL_STATUS_FAILED = 4;
}

message Movement {
  string topic = 1; int32 partition = 2;
  repeated int32 old_replicas = 3; repeated int32 new_replicas = 4;
  int32 old_leader = 5; int32 new_leader = 6;
}

message ProposalSummary {
  int32 replica_movements = 1; int32 leader_movements = 2;
  int32 max_replicas_before = 3; int32 max_replicas_after = 4;
  int32 max_leaders_before = 5;  int32 max_leaders_after = 6;
}

message Proposal {
  string id = 1; ProposalStatus status = 2; int64 created_at_ms = 3;
  repeated string goals_applied = 4; ProposalSummary summary = 5;
  repeated Movement movements = 6;
}

message CreateProposalRequest { repeated string goals = 1; }   // empty = all
message DryRunProposalRequest { string id = 1; }
message DryRunResponse {
  string id = 1; ProposalSummary summary = 2;
  int64 estimated_bytes_moved = 3;                              // 0 in 43a
}
message GetProposalRequest { string id = 1; }
message ListProposalsRequest { int32 limit = 1; }               // 0 = default (20)
message ListProposalsResponse { repeated Proposal proposals = 1; }
message ExecuteProposalRequest { string id = 1; }
message ExecuteProposalResponse {}                              // empty in 43a
```

Method behaviors specific to 43a:

| RPC               | 43a behavior |
|-------------------|--------------|
| `GetState`        | Returns the current snapshot; `Code::Unavailable` until the first successful snapshot. |
| `CreateProposal`  | Runs the optimizer over the current snapshot; stores the result; returns it. Empty `goals` field = all goals; unknown goal names → `Code::InvalidArgument`. `Code::Unavailable` if no snapshot yet. |
| `DryRunProposal`  | Returns the stored proposal's summary with `estimated_bytes_moved = 0`. Idempotent. `Code::NotFound` for unknown id. |
| `GetProposal`     | Returns one stored proposal. `Code::NotFound` for unknown id. |
| `ListProposals`   | Most-recent-first, ring-buffer-bounded. `limit == 0` → 20; otherwise capped at `min(limit, ring_buffer_size)`. |
| `ExecuteProposal` | `Code::Unimplemented` with message `"execute path lands in slice 43b"`. |

Wire format: clients pick JSON (`Content-Type: application/json`) or protobuf (`application/proto`) per request — Connect handles content negotiation. `curl` + JSON workflows are supported out of the box.

### Optimizer + Goal trait

`Goal` trait — pure-logic, no I/O, deterministic given the input state.

```rust
pub trait Goal: Send + Sync {
    fn name(&self) -> &'static str;
    fn priority(&self) -> GoalPriority;
    fn propose(&self, state: &ClusterState, ctx: &GoalContext) -> Vec<Movement>;
}

pub enum GoalPriority { Hard, Soft }

pub struct GoalContext {
    pub imbalance_threshold_pct: u32,
    pub max_movements_per_proposal: usize,
}
```

Optimizer flow (`optimizer::optimize`):

1. Sort goals: Hard first, then Soft. Ties broken by registration order.
2. Apply each goal's `propose` against an in-memory mutable clone of `ClusterState`. Each `Movement` updates the clone *before* the next goal sees it — so soft goals see post-hard-goal counts.
3. Accumulate every `Movement` in a `Vec`; coalesce duplicates per `(topic, partition)` — last writer wins.
4. Truncate to `max_movements_per_proposal`. If a `Hard` goal still reports unfulfilled movements after the cap is hit, return `OptimizeError::HardGoalUnsatisfied`.
5. Compute `ProposalSummary` (before / after counts).
6. Return `Proposal { id: Uuid::new_v4().to_string(), status: Computed, ... }`.

Movement-validity invariants the optimizer enforces (any violation drops the movement):
- `new_replicas.len() == old_replicas.len()` (RF unchanged in 43a)
- `new_leader ∈ new_replicas`
- `new_replicas` has no duplicates
- All `new_replicas` ids exist in `state.brokers`

The three slice-43a goals:

1. **`PreferredLeaderIdempotency`** (Hard) — for every partition where `replicas[0]` is alive (broker present in `state.brokers`) and in ISR but isn't currently `leader`, emit a leader-swap movement (no replica-set change). Runs first so the soft goals see preferred-leader-as-leader counts.

2. **`ReplicaDistribution`** (Soft) — compute `replicas_per_broker`. If `(max - min) * 100 / total > imbalance_threshold_pct`, move replicas from the most-loaded broker to the least-loaded. Greedy: pick the most-loaded broker → pick one of its replicas whose partition currently lacks a replica on the least-loaded broker → swap. Repeat until threshold satisfied OR no valid swap remains.

3. **`LeaderDistribution`** (Soft) — compute `leaders_per_broker` over the *post-replica-balance* clone. Same imbalance heuristic; movements are leader-only (no replica change). For partitions where the new-leader candidate isn't already in the replica set, skip — leader-only movements can only target existing replicas.

### Cluster-state ingest

`Ingester` runs in a tokio task spawned by the binary entry point. Snapshot every `--scrape-interval-secs` (default 10).

```rust
pub struct Ingester {
    client: crabka_client_core::Client,
    interval: Duration,
    snapshot: Arc<ArcSwap<Option<ClusterState>>>,  // None until first success
    shutdown: CancellationToken,
}
```

Each tick calls `snapshot_once`:

1. `Metadata` v12, `allow_auto_topic_creation = false` → brokers + topics + partitions.
2. `DescribeCluster` v0 → cluster id (logged in 43a; pinned in proposals starting in 43b).
3. `ListPartitionReassignments` v0 with `topics = None` → in-flight rows.

Combine into one `ClusterState` with `snapshot_at_ms = now_ms()`. On error: log at warn, leave the prior snapshot in place, retry next tick. Fixed cadence with at-most-one-in-flight is simpler than backoff and adequate for the broker load (3 admin RPCs every 10s).

Snapshot storage: `ArcSwap<Option<ClusterState>>` — lock-free reads from RPC handlers, atomic swap on tick. `GetState` derefs and either returns the snapshot or maps `None` → `Code::Unavailable`.

**Behavior under in-flight reassignments:** if `in_flight_reassignments` is non-empty when `CreateProposal` runs, the proposal is computed against the *current* (transition-state) placement. The response includes the in-flight list in the prior `GetState` (operators can check themselves). Slice 43a does not gate proposal creation on in-flight reassignments; slice 43b adds that gate alongside the execute path.

### `ClusterState` data model

```rust
pub struct ClusterState {
    pub cluster_id: Option<String>,
    pub snapshot_at_ms: i64,
    pub brokers: Vec<BrokerView>,
    pub partitions: Vec<PartitionView>,
    pub in_flight_reassignments: Vec<InFlightReassignment>,
}
pub struct BrokerView { pub id: i32, pub host: String, pub port: i32, pub rack: Option<String> }
pub struct PartitionView {
    pub topic: String, pub partition: i32,
    pub replicas: Vec<i32>, pub leader: i32, pub isr: Vec<i32>,
}
pub struct InFlightReassignment {
    pub topic: String, pub partition: i32,
    pub adding: Vec<i32>, pub removing: Vec<i32>,
}
```

Partitions are flat, not grouped by topic — most goal logic iterates the full list anyway; per-topic grouping is computed lazily when the response is serialized.

## Test plan

### Unit tests

- `model::tests` — `ClusterState` round-trip into proto `GetStateResponse`; movement-validity helpers reject RF changes / duplicates / unknown broker ids / leader-not-in-replicas.
- `goals::preferred_leader_idempotency::tests` — preferred-already-leader is no-op; preferred-alive-and-in-ISR-but-not-leader triggers swap; preferred-dead is skipped; preferred-out-of-ISR is skipped.
- `goals::replica_distribution::tests` — balanced cluster → no movements; one-hot-broker case → expected number of moves; RF preserved; partitions with `replicas.len() == brokers.len()` are skipped.
- `goals::leader_distribution::tests` — leader-only moves (replicas unchanged); only moves to an existing replica; skip when no replica on the candidate broker.
- `optimizer::tests` — hard runs first; soft sees post-hard state; movement-cap truncation; duplicate-coalescing (last writer wins per `(topic, partition)`); `OptimizeError::HardGoalUnsatisfied` when truncation drops a hard movement.
- `ingest::tests` — `ArcSwap<Option<…>>` semantics: pre-first-tick reads return None; post-tick reads return the snapshot; error tick preserves prior snapshot.
- `api::tests` — handler-level tests: `Code::Unavailable` before first snapshot, `Code::NotFound` for unknown proposal id, `Code::Unimplemented` on `ExecuteProposal`, `Code::InvalidArgument` for unknown goal names. Use a hand-rolled in-memory `Ingester` fixture (no live broker).

### Integration test (`crates/rebalancer/tests/end_to_end.rs`)

1. Spin up a single-broker Crabka via `crabka_broker::Broker::start(BrokerConfig::for_tests(...))`.
2. Create 3 topics, 4 partitions each, RF=1.
3. Start an `Ingester` against the broker. Wait for `GetState` to return a non-None snapshot.
4. Call `CreateProposal` via the in-process service handler (not over HTTP — invoke the generated trait directly).
5. Assert `Proposal.status == Computed`, `goals_applied` lists all three, `movements` is empty (single broker — nothing to balance), `summary.max_replicas_before == 12 == max_replicas_after`.
6. Repeat with `CreateProposal { goals: ["ReplicaDistribution"] }` → same shape, `goals_applied` has one entry.

The "found movements" path needs multi-broker setup; that's a slice-43b integration test where execute is wired up. For 43a we prove the "balanced → empty movements" + "API plumbing works" paths.

### Connect protocol smoke test

Boot the binary in a separate test (`std::process::Command`), hit the live Connect endpoint over HTTP with `Content-Type: application/json`, parse the returned JSON, assert it deserializes into the proto-generated `GetStateResponse`. One test, no payloads to deep-compare — proves the axum mount + Connect handler glue works end-to-end.

### Not in 43a

- **No JVM acceptance test.** No JVM tool talks to a Cruise-Control-equivalent service. (Cruise Control's own REST API is the canonical one; a hand-rolled Java client against our proto would be required, and that's not worth the maintenance burden in 43a.)
- **No operator-side integration test.** That's operator slice 44.

## Risks & open questions

- **`connectrpc-axum-build` maturity.** Version 0.1 is recent; if the codegen has rough edges (broken proto features, panic on edge cases), we may need to pin to a specific commit or vendor the generator. Confirm during the first task's compile-the-skeleton step.
- **Connect protocol path conventions.** The default service path is `/crabka.rebalancer.v1.Rebalancer/<Method>`. If we'd rather mount under `/api/v1/rebalancer/...` for prettier URLs, that's an axum route prefix decision but it diverges from Connect's protocol expectations and breaks generated clients. Recommendation: stick with the default for now; future slices can add a translator layer if needed.
- **Default port `9300` collision.** Confirmed not used elsewhere in Crabka. JMX exporter conventions don't touch it.
- **`Code::Unavailable` semantics.** Connect uses gRPC's status codes; `UNAVAILABLE` is what gRPC-style retrying clients expect for "try again shortly". Matches our intent (pre-first-snapshot reads should be retried).

## Acceptance criteria

1. `cargo build -p crabka-rebalancer` produces a binary.
2. `crabka-rebalancer --bootstrap-servers <addr> --listen-addr 127.0.0.1:9300 &` starts and binds the port.
3. `curl -X POST -H 'Content-Type: application/json' http://127.0.0.1:9300/crabka.rebalancer.v1.Rebalancer/GetState -d '{}'` returns either `503 Code::Unavailable` (pre-first-snapshot) or a JSON `GetStateResponse`.
4. All unit + integration tests pass; `cargo clippy --workspace --all-targets -- -D warnings` clean; `cargo fmt --check` clean.
5. `README.md`'s `Replication & durability` table gains a row "Cruise-Control-equivalent rebalancer (advisor)" → ✅. The execute / topology / capacity / usage / anomaly variants stay as ❌ rows until the corresponding 43b–43g slices land.
6. `STATUS.md` gains a slice-43a entry documenting what shipped and the slice-43b → 43g follow-ups.
