# Crabka rebalancer — Cruise-Control-equivalent roadmap (design)

**Date:** 2026-05-17
**Status:** Roadmap design, ready for first-slice plan
**Scope:** Long-horizon roadmap toward full Cruise-Control feature parity, expressed as numbered sub-slices of slice 43. Identifies the first sub-slice (43a) to drive into an implementation plan.

## Goal

Deliver a Rust standalone service (`crabka-rebalancer`) that brings Crabka to feature parity with [Cruise Control](https://github.com/linkedin/cruise-control) for cluster-wide partition placement. The service ingests cluster state, runs a goal-seeking optimizer, and (on operator request) executes reassignments through Crabka's existing KIP-455 + KIP-73 plumbing.

This document is the long-form roadmap; each slice below is a single PR sized comparably to slices 1–67 in the existing Crabka history.

## Decisions captured during brainstorm

1. **Topology:** standalone `crabka-rebalancer` binary that talks to the cluster as a regular Kafka admin client. Mirrors Cruise Control's deployment model (separate process). Lets the rebalancer crash / upgrade independently of brokers.
2. **Goal coverage:** full Cruise-Control parity is the long-term target, decomposed into multiple sub-slices. Each goal family lands as its own slice.
3. **First cut (slice 43a):** MVP — REST API skeleton, periodic cluster-state ingest, two soft goals (replica count, leader count) + one hard goal (preferred-leader idempotency). No execute path yet; proposals are JSON.
4. **REST API base path:** `/api/v1/...` (versioned from day one so the operator can pin against a stable contract).
5. **State persistence:** local disk (JSON files in a configurable data dir) for early slices; revisit moving to a Crabka internal topic if multi-replica HA becomes a requirement.
6. **Operator integration:** `KafkaRebalance` CRD lands as a separate operator slice (44) after slice 43b ships the execute path.

## Architecture

### Process shape

One binary, `crabka-rebalancer`. Single-replica by default; the operator can deploy multiple replicas behind a `Lease`-based leader election once persistent state is moved off local disk (deferred). Cruise Control's split into "kafkacruisecontrol" + "metric reporter" is not mirrored — we use Crabka's slice-39 Prometheus endpoint as the metric source instead of shipping a separate JMX reporter.

### Crate layout

- New workspace member `crates/rebalancer/` producing the `crabka-rebalancer` binary and a small library surface for tests.
- Internal modules:
  - `ingest` — admin-client wrapper + periodic cluster-state snapshot (`Metadata`, `DescribeCluster`, `ListPartitionReassignments`).
  - `model` — pure-logic structs: `ClusterState`, `BrokerCapacity`, `MetricsWindow`, `Proposal`, `ReplicaMovement`.
  - `goals` — `Goal` trait + one module per goal (`replica_distribution`, `leader_distribution`, `rack_aware`, `disk_capacity`, …).
  - `optimizer` — runs a goal list against a `ClusterState`, returns a `Proposal`.
  - `executor` — drives `AlterPartitionReassignments` + KIP-73 throttle config writes against the cluster; polls progress.
  - `metrics_scraper` — scrapes broker `/metrics` (slice 39) for per-partition byte counters; maintains rolling windows.
  - `detector` — anomaly detector (slice 43g).
  - `api` — axum router for the REST surface.
- Same Cargo workspace as the broker. Same release cycle. `cargo test` runs rebalancer tests alongside broker tests.

### Naming & API surface

- Crate / binary / container image: `crabka-rebalancer`.
- Helm chart: `charts/crabka-rebalancer/`.
- REST API base: `/api/v1/`.
- Endpoints (full set across the roadmap):
  - `GET  /api/v1/state` — current cluster snapshot (broker list, partition placement, in-flight reassignments)
  - `POST /api/v1/proposals` — compute + return a proposal (no execute)
  - `POST /api/v1/proposals/{id}/dryrun` — compute movements + estimated cost without executing
  - `POST /api/v1/proposals/{id}/execute` — drive the proposal through KIP-455 with KIP-73 throttle
  - `GET  /api/v1/proposals/{id}` — proposal status (computed / executing / completed / failed)
  - `GET  /api/v1/proposals` — list recent proposals
  - `GET  /api/v1/anomalies` — anomaly history (slice 43g)
  - `GET  /healthz` `/readyz` `/metrics` — operational endpoints (mirrors operator's pattern)

### Deployment artifacts

- Helm chart at `charts/crabka-rebalancer/` for the service itself.
- `KafkaRebalance` CRD lands in the operator (slice 44); operator translates CRD specs into REST calls.

### Configuration

- CLI flags / env vars (mirror operator binary pattern):
  - `--bootstrap-servers` (admin-client connection)
  - `--listen-addr` (REST API bind address, default `0.0.0.0:9300`)
  - `--data-dir` (local persistence)
  - `--scrape-interval-secs` (cluster-state ingest cadence; default 10)
  - `--metrics-scrape-targets` (broker Prometheus endpoints; format `host:port,host:port,…`; later: discover via Metadata)
  - Goal-specific knobs as added by later slices (e.g. capacity limits via a config file)

## Phase / slice breakdown

### Phase A — Foundation

| Slice | Title | Summary |
|------:|-------|---------|
| 43a | Foundation: REST skeleton + replica/leader balance | Crate scaffold, REST API skeleton (state / propose / dryrun / status — **no execute**), periodic cluster-state ingest, optimizer + two soft goals (replica count, leader count) + one hard goal (preferred-leader idempotency). Proposals returned as JSON. Helm chart placeholder; full chart in 43b. |

### Phase B — Execution

| Slice | Title | Summary |
|------:|-------|---------|
| 43b | Execute path | Wire `executor` module to `AlterPartitionReassignments` (KIP-455) and `IncrementalAlterConfigs` for KIP-73 throttle apply / clear. Poll `ListPartitionReassignments` + image to surface progress in proposal status. Persist running-plan state to local disk (`{data_dir}/in_flight.json`). Ships the production Helm chart. |

### Phase C — Topology goals

| Slice | Title | Summary |
|------:|-------|---------|
| 43c | Topology goals | Hard: `RackAware` (reads `broker.rack` labels from `MetadataImage`). Soft: `TopicReplicaDistribution`, `MinTopicLeadersPerBroker`. |

### Phase D — Capacity (static limits)

| Slice | Title | Summary |
|------:|-------|---------|
| 43d | Capacity goals | Hard: `ReplicaCapacity`, `DiskCapacity`, `NetworkInCapacity`, `NetworkOutCapacity`, `CpuCapacity`. Adds a per-broker capacity config (YAML loaded from `--broker-capacity-file`); no metric scraping yet — capacities are static operator-supplied limits. |

### Phase E — Usage goals (live metrics)

| Slice | Title | Summary |
|------:|-------|---------|
| 43e | Usage goals + metric scraping | `metrics_scraper` module scrapes broker `/metrics` (slice 39) for per-partition bytes-in / bytes-out. Ring-buffer history at 5min / 1h / 12h windows. Soft goals: `DiskUsage`, `LeaderBytesIn`, `NetworkInUsage`, `NetworkOutUsage`. **Likely needs a slice-39 follow-up to expose per-partition byte counters that aren't on the slice-39 surface yet** — flagged as a Crabka-core sub-slice (`43e-core`) if so. |

### Phase F — CPU + remaining goals

| Slice | Title | Summary |
|------:|-------|---------|
| 43f | CPU usage + leftovers | `CpuUsage` goal — needs CPU metrics on the broker's `/metrics` endpoint (Crabka-core sub-slice `43f-core` if not already covered by 39 / 43e). Plus any leftover Cruise-Control goals (e.g. `PreferredLeaderElection` as a soft goal, beyond the slice-43a idempotency variant). |

### Phase G — Anomaly detection

| Slice | Title | Summary |
|------:|-------|---------|
| 43g | Anomaly detector | Background `detector` module that watches metric history + cluster state for anomalies (broker death, sustained under-replicated partitions, disk pressure, slow broker / outlier). Auto-triggers self-healing proposals via the existing optimizer path. Anomaly history persisted; surfaced via `GET /api/v1/anomalies`. Configurable per-anomaly mute windows. |

### Operator-side follow-up

| Slice | Title | Summary |
|------:|-------|---------|
| 44 | `KafkaRebalance` CRD | Operator translates `KafkaRebalance` specs into REST calls against the rebalancer service. Surfaces proposal status back through the CRD's `status` subresource. Lands after 43b. |

## Sequencing & dependencies

```
43a ─────► 43b ─────► 43c ─► 43d ─► 43e ─► 43f ─► 43g
                  │
                  └─► operator 44 (`KafkaRebalance` CRD)
```

- 43a is **standalone** — ships a useful "what would you do?" advisor before any execute path exists. Operators can run it in dry-run mode against a production cluster from day one.
- 43b unlocks both 43c–43g and the operator slice 44 in parallel.
- 43c / 43d / 43e / 43f / 43g are mostly independent and can ship in any order (or in parallel by separate engineers). Recommended order is topology-first → capacity → usage → anomaly because each later phase depends on more metrics infrastructure.
- Crabka-core sub-slices (`43e-core`, `43f-core`) may be needed to extend the slice-39 metrics surface — confirmed during the 43e / 43f design phase.

## Crabka-core dependency map

| Slice | Capability | Required by rebalancer phase |
|------:|------------|------------------------------|
| 14    | Preferred-leader election | 43a (idempotency goal) |
| 15    | KIP-455 `AlterPartitionReassignments` / `ListPartitionReassignments` | 43b (execute) |
| 15b   | KIP-73 throttled replication configs | 43b (execute throttle) |
| 39    | Prometheus `/metrics` endpoint | 43e (usage), 43f (CPU), 43g (anomaly detection) |
| 43e-core (TBD) | Per-partition byte counters on `/metrics` (if not already there) | 43e |
| 43f-core (TBD) | CPU usage metric on `/metrics` (if not already there) | 43f |

**Already shipped** — no Crabka-core work blocks 43a, 43b, 43c, 43d. The first four sub-slices can land back-to-back without dropping back into broker work.

## First sub-slice — 43a

The implementation plan that follows this design covers slice 43a only.

### Goal

Land a standalone `crabka-rebalancer` binary that:
- Connects to a Crabka cluster as an admin client.
- Periodically snapshots cluster state.
- Exposes a versioned REST API for "what should I do to balance this cluster?" — but **does not execute anything**.
- Computes proposals using two soft goals (replica count, leader count) plus a preferred-leader idempotency hard goal.

### Deliverables

- New workspace member `crates/rebalancer/` producing the `crabka-rebalancer` binary.
- Library modules: `ingest`, `model`, `goals` (`replica_distribution`, `leader_distribution`, `preferred_leader_idempotency`), `optimizer`, `api`.
- REST endpoints:
  - `GET /api/v1/state`
  - `POST /api/v1/proposals`
  - `POST /api/v1/proposals/{id}/dryrun`
  - `GET /api/v1/proposals/{id}`
  - `GET /api/v1/proposals`
  - `GET /healthz`, `GET /readyz`, `GET /metrics`
- Pure-logic unit tests for each goal + the optimizer.
- 1 integration test: spin up a single-broker Crabka, point the rebalancer at it, request a proposal, assert sane JSON shape.
- A minimal Helm chart at `charts/crabka-rebalancer/` (placeholder — slice 43b fleshes it out).

### Explicit non-goals for 43a

- No execute path. `POST .../execute` returns 501 until 43b.
- No persistence — proposals live in memory; restart loses them.
- No metric scraping. Goals operate on metadata + replica/leader counts only.
- No rack-aware / capacity / usage / anomaly logic — those land in 43c–43g.
- No operator integration — the operator slice 44 lands after 43b.

### Risks called out at this stage

- **Admin-client wire surface:** Crabka's `crabka-client-core` doesn't currently expose `ListPartitionReassignments` or `DescribeCluster` as typed methods. 43a may need to add thin wrappers in the client crate before the rebalancer can ingest state. Confirm during 43a brainstorm whether to extend `crabka-client-core` or hand-roll the wire calls inside `crates/rebalancer/`.
- **REST framework:** axum is already in the workspace (operator slice 17 + broker slice 39). Reuse it.
- **Default REST port:** `9300` (not yet used elsewhere in Crabka).
