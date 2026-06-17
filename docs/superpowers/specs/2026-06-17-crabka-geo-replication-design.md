# Crabka Geo-Replication (`crabka-replicator`) — Design

**Date:** 2026-06-17
**Status:** Design approved; Slice 1 ready for implementation planning
**Topic:** Cross-cluster, geo-distributed replication for Crabka with data-sovereignty controls — Crabka's MirrorMaker-2 equivalent plus a sovereignty/compliance layer MM2 does not have.

---

## 1. Summary

`crabka-replicator` is a new standalone service that replicates data between
geographically dispersed Crabka clusters. It delivers global event streaming,
cross-region availability, selective topic replication, consumer-group offset
translation, and automatic recovery from network/cluster failures — the
MirrorMaker-2 (MM2) feature set — and adds a **data-sovereignty policy layer**:
residency routing, field-level transforms (redact/mask/tokenize), and per-record
routing.

The service is built on the existing `crabka-connect` `ConnectorRuntime` and the
native `crabka-client-*` clients. It sits *above* the broker (like MM2 sits
outside Kafka) and touches no broker or wire-protocol code.

## 2. Goals / Non-goals

### Goals

- **All four topologies** from one composable primitive — a directional flow
  `(source → target)`:
  - Active/passive (DR), active/active (live multi-region), aggregation (N→1),
    fan-out (1→N). Topology is *emergent* from the set of declared flows; there
    is no separate "topology mode."
- **Selective replication** — per-flow topic and consumer-group selectors
  (include/exclude; exact / prefix / regex).
- **Consumer-group offset translation** so a failed-over consumer resumes in the
  target cluster without skipping or grossly reprocessing.
- **Automatic recovery** from replicator restarts, transient connectivity loss,
  and worker failure.
- **Data sovereignty**, three capabilities:
  1. **Residency routing** — hard allow/deny of a topic-flow by region/zone.
  2. **Field-level transforms** — redact / mask / tokenize fields before data
     crosses a border.
  3. **Record-level routing** — keep/drop individual records by a content
     predicate (per-record residency).
- **MM2 convention compatibility** — remote-topic naming and byte-compatible
  offset-sync / checkpoint / heartbeat internal-topic formats, so JVM
  `RemoteClusterUtils` and existing consumers interoperate for offset translation.
- **Configurable delivery per flow** — at-least-once (default) or exactly-once.

### Non-goals (v1)

- **No Kafka Connect worker/REST protocol or plugin-config model.** We adopt
  MM2's *conventions and byte formats*, not its Connect runtime. This is
  consistent with `crabka-connect`'s deliberate "no worker protocol,
  single-binary" design.
- **No audit-trail / right-to-erasure propagation and no region-scoped
  encryption / key residency** (the sovereignty capabilities deferred during
  design). Out of scope entirely for now.
- **No log/segment-level replication.** Replication is consume→produce, because
  sovereignty field-transforms and record-routing require materializing records.
- **No embedded-in-broker mode.** Cross-cluster replication stays external.

## 3. Constraints and deliberate decisions

- **Kafka/MM2 compatibility is the binding constraint.** Remote-topic naming,
  offset-sync/checkpoint/heartbeat record formats must be byte-compatible with
  JVM MM2 so `RemoteClusterUtils` and JVM consumers can translate offsets.
  Validated by differential golden-vector tests.
- **Schema-aware capabilities require schema-encoded topics.** Field transforms
  (#2) and record routing (#3) only work on topics whose records are encoded via
  Schema Registry (Avro / Protobuf / JSON-Schema). Opaque payloads can only get
  residency routing (#1). This is an inherent limitation, stated explicitly to
  users.
- **Residency is deny-wins**, mirroring Crabka's existing ACL authorizer
  semantics (deny-by-default; DENY beats ALLOW).
- **Fail-closed on undecodable records.** If a topic has a transform/route policy
  but a record cannot be decoded, the worker drops it and alerts rather than
  passing raw bytes through (which would leak the very PII the policy protects).
  Per-policy override to fail-open exists, but secure-by-default.
- **Offset translation is "at-or-before."** A failed-over consumer may reprocess
  a little but never skips un-consumed data — the same safety bias as Kafka.
- Greenfield project: no backwards-compatibility shims, no migration code. When a
  config/format changes during development, it just changes.

## 4. Architecture

```
                 ┌─────────────────────────────┐
   operator +    │  (optional control layer,    │
   GeoReplication│   or a plain config file)    │
       CRD       └──────────────┬──────────────┘
                                │ control
                                ▼
 ┌────────────┐   consume   ┌───────────────────────────┐   produce   ┌────────────┐
 │  Source    │────────────▶│   crabka-replicator        │────────────▶│  Target    │
 │  cluster   │             │                            │             │  cluster   │
 │  (region A)│             │  flow supervisor (control) │             │ (region B) │
 │            │             │  ├─ flow-worker A→B (data) │             │  internal: │
 │  orders    │             │  │   fetch→gate→[schema]   │             │  heartbeats│
 │  payments  │             │  │   →produce→offset-sync  │             │  A.check…  │
 │            │             │  │   + checkpoint/heartbeat│             │  offset-sy…│
 │            │             │  │   + loop-guard          │             │  A.orders  │
 └────────────┘             │  └─ …one worker per flow   │             └────────────┘
                            │  Schema Registry (only for │
                            │  transform/route topics)   │
                            └───────────────────────────┘
```

### Two planes

- **Control plane — the flow supervisor.** Loads replication config + sovereignty
  policy; resolves it against live cluster metadata (topic matches, region/zone
  allow/deny); spawns and supervises one flow-worker per directional flow;
  exposes health + Prometheus metrics (via `crabka-telemetry`); under the
  operator, writes CRD status.
- **Data plane — flow-workers.** One worker per `(source → target)` flow. Each
  owns a single consumer + producer pair and runs the staged data path. Active/
  active = two workers; aggregation = N workers into one target; fan-out = one
  source feeding N workers.

### State location

All replication state lives on the **target** cluster in MM2-convention internal
topics:

- `heartbeats`
- `<source>.checkpoints.internal` — translated consumer offsets
- `mm2-offset-syncs.<target>.internal` — source↔target offset mapping
- the replicator's own consumer read position

A replicator restart therefore recovers entirely from target-cluster state — no
local disk to lose.

### Crate structure

- New `crates/replicator` — engine library + binary, with an internal policy
  module.
- New `GeoReplication` CRD added to `crates/operator/src/crd/` (Slice 4).
- No changes to `crates/broker` or `crates/protocol`. This is purely an
  above-the-broker client application.

## 5. Flow-worker data path

Each batch is fetched once (reusing incremental fetch sessions), then each topic
takes a lane decided by its policy:

1. **Residency gate (every topic, fast path).** Allow/deny on
   `(topic × target region/zone)` resolved from policy, deny-wins. A denied
   topic-flow never produces a byte; the drop is surfaced as a metric/alert. This
   is the hard residency boundary.
2. **Fast lane (byte-passthrough).** Plain and residency-only topics are never
   decoded; the record batch travels as opaque bytes to produce. Preserves source
   bytes exactly (no recompression of compressed batches).
3. **Schema lane (engaged only when a topic has a transform/route policy):**
   `decode (Schema Registry) → redact/mask/tokenize fields → route predicate
   (keep/drop record) → encode`. The only path that materializes records.
4. **Produce.** Plain idempotent producer (at-least-once) or wrapped in the
   `connect` runtime's transactional gate (EOS) — per-flow choice.
5. **Offset-sync.** Records the source→target offset mapping at produce time;
   feeds checkpoint translation.

**Consequence:** record-routing (#3) drops records, so source/target offsets
diverge — which is exactly why offset *translation* (not copying) is mandatory,
and why translation stays "at-or-before."

The policy **is** the lane selector: a topic with no matching transform/route
policy is automatically on the fast lane.

## 6. Sovereignty policy model

Config is Crabka-native (not a Connect plugin config) with three parts —
**clusters**, **flows**, **policies**. Regions/compliance-zones are first-class;
policies reference *zones*, not raw cluster names, so they are portable.

```yaml
clusters:
  us-east: { bootstrap: "...", region: us, zones: [us] }
  eu-west: { bootstrap: "...", region: eu, zones: [eu, gdpr] }

flows:
  - from: us-east
    to:   eu-west
    topics:  { include: ["orders", "payments", "telemetry.*"], exclude: ["*.internal"] }
    groups:  { include: ["analytics-*"] }      # which consumer groups to checkpoint
    naming:  default                           # eu-west sees "us-east.orders" (or: identity)
    delivery: at-least-once                    # or: exactly-once

policies:
  - name: keep-pii-in-eu                       # (1) RESIDENCY — hard gate, fast path
    topics: ["customers", "kyc.*"]
    residency: { allow_zones: [gdpr] }         # block replication to any non-GDPR target

  - name: mask-on-export                       # (2) FIELD TRANSFORMS — schema lane
    topics: ["orders"]
    when:   { target_zone_not: [gdpr] }        # engage only when leaving the GDPR zone
    transforms:
      - { field: "$.customer.ssn",   action: drop }
      - { field: "$.customer.email", action: mask }
      - { field: "$.customer.id",    action: tokenize }

  - name: eu-residents-stay                     # (3) RECORD ROUTING — schema lane
    topics: ["events"]
    route:  { replicate_if: "$.user.region == flow.target.region" }
```

### Resolution rules

- **Selective replication** = the `topics`/`groups` include/exclude selectors per
  flow (regex / prefix / exact).
- **Residency = deny-wins.** A topic-flow violating any residency rule is
  hard-blocked at the gate.
- **Transforms compose in declared order.** Field paths are JSONPath-style
  against the decoded record value, key, or headers.
- **Fail-closed on undecodable records** (secure default; per-policy override).
- Policy is loaded by the control plane, resolved against metadata, and pushed to
  workers. Hot-reload on config change (operator reconcile or config-file watch).

## 7. Offset translation, heartbeats, loop prevention

### Loop prevention (active/active)

- `default` naming policy prefixes replicated topics with the source alias
  (`orders` → `us-east.orders`). Loop-guard: a worker's topic selector
  **excludes already-remote (prefixed) topics by default**, so a replicated topic
  is never bounced back. A cluster's own local topics still replicate outward.
- `identity` naming policy (active/passive, no rename) uses a **provenance
  header** stamped at produce time to recognize and skip origin-cluster data.
- No infinite loops in either mode.

### Offset translation (consumer failover)

The checkpoint task reads committed offsets for the configured groups on the
source, consults the offset-sync mapping, and writes translated checkpoints to
`<source>.checkpoints.internal` on the target — byte-compatible with MM2 so JVM
`RemoteClusterUtils` reads them. Translation is **"at-or-before"**: a failed-over
consumer never skips un-consumed data.

### Heartbeats

A heartbeat task emits to a `heartbeats` topic on the target at a fixed cadence.
Consumers/operators measure replication liveness and end-to-end lag; the control
plane distinguishes a wedged flow from a healthy-but-idle one.

## 8. Automatic recovery

Three layers, satisfying the "automatic recovery from network/cluster failures"
requirement:

1. **Position recovery** — read position lives in target-cluster state; a
   crashed/restarted replicator resumes exactly where it left off.
2. **Connectivity loss** — transient source/target unavailability triggers
   bounded exponential backoff + retry on the worker's clients (reusing
   `crabka-client-core` reconnection), not a crash. The flow self-heals when the
   link returns; the heartbeat gap makes the outage observable.
3. **Supervision** — the control plane restarts a dead worker with backoff; a
   persistently-failing worker is surfaced via health/metrics and (under the
   operator) CRD status.

**Backpressure & EOS boundary** carry over from the `connect` runtime: bounded
in-flight batches, and for EOS flows the produce + offset-sync commit are
bracketed in one transaction so a mid-flight crash leaves no partial/duplicated
output.

## 9. Operator / CRD surface (Slice 4)

A new `GeoReplication` CRD in `crates/operator/src/crd/`, following the existing
`KafkaRebalance` / `SchemaRegistry` CRD patterns. Its spec mirrors the config
model. The operator:

- Reconciles it into a Deployment of `crabka-replicator` pods + Service.
- Wires cluster credentials and Schema Registry auth from Secrets; reuses the
  existing CA/mTLS plumbing for cluster connections.
- Writes **status**: per-flow state (running / degraded / residency-blocked),
  heartbeat lag, records replicated / dropped / denied.

Direct (non-k8s) mode runs the same engine from a config file; the CRD is a
manager that renders that config.

## 10. Testing strategy

Several of these are **compliance gates**, not merely correctness checks.

1. **MM2 byte-format interop** — checkpoint / offset-sync / heartbeat records
   validated byte-for-byte against JVM golden vectors so `RemoteClusterUtils` and
   JVM consumers can translate offsets.
2. **Integration (two live Crabka clusters)** — selective replication (only
   included topics land), remote naming, active/active loop prevention (no
   bounce), offset-translation never-skips, auto-recovery.
3. **Sovereignty assertions (security-critical)** — denied topic-flows produce
   **zero bytes** at target; masked/dropped fields **never** appear at target;
   record-routing keeps only matching records; fail-closed on undecodable records.
4. **Failure/recovery** — kill+restart resumes from target state with no gap;
   severed link → backoff + self-heal; EOS-flow crash mid-transaction → no
   partial/duplicated output.
5. **Schema round-trips** — Avro / Protobuf / JSON-Schema decode→transform→encode,
   including schema-evolution edges.
6. **Model checking (follow-on, optional)** — offset-translation monotonicity
   (never-skip) and loop-termination are clean stateright properties, treated as
   a follow-on rather than v1 scope.

## 11. Build staging (slices)

The design above is the whole feature. Implementation is sliced; **only Slice 1
goes into the first implementation plan**. Slices 2–4 are documented follow-ons,
each its own spec → plan cycle.

- **Slice 1 — core replication engine (first plan).** `crates/replicator` crate +
  binary; flow supervisor; flow-worker; fast-lane passthrough; selective topics;
  remote naming + loop prevention; **residency routing (#1)**; offset translation
  + heartbeats (MM2 byte formats); at-least-once delivery; auto-recovery. Delivers
  all four topologies + residency + offset failover. (The `delivery` config field
  exists but accepts only `at-least-once` until Slice 3 lands EOS.)
- **Slice 2 — schema lane.** Field transforms (#2) + record routing (#3) + Schema
  Registry integration + fail-closed decode.
- **Slice 3 — EOS** delivery mode (per-flow transactional produce + atomic offset
  commit).
- **Slice 4 — operator `GeoReplication` CRD** + status surface.

## 12. Open questions / risks

- **Offset-sync granularity vs. accuracy.** MM2 emits offset-syncs periodically;
  too sparse coarsens translation, too dense costs throughput. Slice 1 should
  expose the sync interval and pick a sane default matching MM2.
- **Tokenize determinism.** `tokenize` must be deterministic per value if
  downstream joins on the token are expected; decide whether tokens are stable
  (keyed hash) or per-record random. Resolve in Slice 2.
- **Schema Registry topology.** Slice 2 must decide whether transforms resolve
  schemas against the source SR, the target SR, or both, and how subject naming
  maps across the remote-topic rename.
- **Large-fan aggregation hotspots.** N→1 aggregation concentrates produce load
  on one target; document partition/throughput expectations.
