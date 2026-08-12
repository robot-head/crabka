# Slices 20a + 20b: KRaft Role Separation & Per-Broker Metadata-Partition Assignment — Design

**Status:** Implemented 2026-08-11.

**Goal:** Reach Strimzi/Kafka parity for KRaft role separation. Support dedicated
**controller-only** and **broker-only** `KafkaNodePool`s (and multi-replica pools),
where controllers form the `__cluster_metadata` raft voter quorum and brokers
replicate that metadata as **true observers** — fetching the metadata log via the
Kafka `Fetch` protocol (KIP-595 semantics) rather than participating in openraft
membership. This is the single combined spec for the work the slice-20 design
deferred as 20a (multi-replica) and 20b (role separation), together with the
broker-side observer-fetch subsystem that makes broker-only nodes possible.

This document supersedes the deferral rows for slices 20a/20b in
`2026-05-17-crabka-operator-kafkanodepool-20-design.md`.

The shipped operator derives every node id and directory id from the pool ordinal.
Exactly one deterministic controller is formatted as the initial voter; later
controllers use the KIP-853 dynamic join path. Controller processes deliberately omit
the static voter list so an empty controller log cannot accidentally bootstrap a
second quorum. Broker-only observers receive the complete controller endpoint list.
Separated roles are passed as the broker process-role environment override, while a
combined pool keeps the broker's existing default for backwards-compatible
single-replica behavior.

---

## 1. Scope

### In

**Broker runtime (`crates/broker`, `crates/raft`, `crates/metadata`):**
- `process.roles` config on the broker: a node is a `controller`, a `broker`, or both.
- Controller-only nodes: raft **voter**, host **no** data partitions, never emit a
  `BrokerRegistration`, and are therefore absent from `Metadata`/`DescribeCluster`.
- Broker-only nodes: **true observers** of `__cluster_metadata`. They never join
  openraft membership; instead they run a metadata replica-fetcher that issues Kafka
  `Fetch` requests against `__cluster_metadata`-0 to the controller quorum and replays
  the returned records into a local `MetadataImage`.
- Combined nodes (`[controller, broker]`): today's behavior — voter + data + advertised.
- Controllers serve `__cluster_metadata`-0 over the standard `Fetch` path, sourced from
  the openraft `RaftLogStore`.
- A defined Kafka wire-format record schema for `__cluster_metadata` (the serialization
  bridge between the openraft log and the Fetch path).
- `DescribeQuorum` reports real voters (controllers) **and** observers (brokers).
- **Runtime quorum reconfiguration (KIP-853):** the controller voter set can grow and
  shrink at runtime — `AddRaftVoter` / `RemoveRaftVoter` / `UpdateRaftVoter` RPC handlers
  wired to openraft `add_learner` + `change_membership`, plus the broker-side
  promotion-from-learner flow. Not fixed at bootstrap.

**Operator (`crates/operator`):**
- `KafkaNodePool` validation relaxed: `roles` ∈ {`[Controller]`, `[Broker]`,
  `[Controller, Broker]`}; `replicas >= 1`; cross-pool node-id range uniqueness.
- Enumerate one node **per replica** (`node_id = nodeIdStart + ordinal`), partitioned
  into controllers vs brokers.
- Compute the voter set from controller-role nodes; render `process.roles`, the voter
  set, and the bootstrap/join mode into each node's TOML.
- Brokers render as observers (not in the voter set; pointed at the controller quorum).
- Rollout ordering: controller pods reach Ready (quorum forms) before broker pods start.
- Reconcile controller-pool scaling at runtime: scaling a controller pool's `replicas`
  adds/removes voters via the KIP-853 path (not just at bootstrap).

### Out (deferred)

| Concern | Where |
|---|---|
| Pod templates / affinity per role | already shipped (slice 20c) |
| Migration from ZK or combined→separated rebalance of an existing cluster | N/A (greenfield, per CLAUDE.md) |

### Non-goals / greenfield assumptions

Per CLAUDE.md: no backwards-compat shims. The `replicas == 1` and
`{Controller, Broker}`-only validation are simply removed and replaced. On-disk raft
log format and the `__cluster_metadata` record schema may change freely; wipe data dirs
during development.

---

## 2. Core model (Kafka/KRaft parity)

A node's `process.roles` determines its relationship to `__cluster_metadata`:

| Role set | Raft relationship | Data partitions | `BrokerRegistration` / advertised | `controller.quorum.voters` member |
|---|---|---|---|---|
| `controller` | **voter** | none | no | yes |
| `broker` | **observer** (Fetch) | yes | yes | no |
| `controller,broker` | **voter** | yes | yes | yes |

The voter set ≡ the set of controller-role nodes. This is exactly Kafka's
`controller.quorum.voters` semantics: brokers learn the controller endpoints, fetch
metadata from them, and are never voters.

---

## 3. Component A — Broker `process.roles`

### 3.1 Config

Add to `BrokerConfig` (`crates/broker/src/config.rs`):

```rust
pub roles: Vec<NodeRole>,   // NodeRole::{Controller, Broker}
```

- Parsed from TOML `process.roles = ["controller", "broker"]` (Kafka key name).
- Default `[Controller, Broker]` so every existing single-node test keeps working
  unchanged (no test churn).
- `validate()` additions: `roles` non-empty; only known variants; if `roles` omits
  `Broker`, the node must not be assigned data partitions; if it omits `Controller`,
  it must not appear in its own voter set.

`NodeRole` lives in the broker crate (not the operator crate) to avoid a dependency
inversion; the operator's `crd::kafka_node_pool::NodeRole` maps to it at render time.

### 3.2 Raft boot path (controllers)

Reuse the existing `Bootstrap`/`Join`/`Rejoin` enum
(`2026-05-14-crabka-bootstrap-then-join-design.md`). Controllers initialize/join as
**voters** exactly as today's multi-node quorum design intends. Broker-only nodes do
**not** call `Raft::new` for `__cluster_metadata` as a member at all — they run the
observer fetcher instead (§4).

### 3.3 Data-partition gating

A node without the `Broker` role:
- skips `log_dir::scan_all` partition recovery at startup,
- rejects any attempt to place a partition replica on it (defensive; the controller
  should never assign one — see §3.4),
- never emits a `BrokerRegistration` metadata record.

### 3.4 Broker registration / advertisement

Only `Broker`-role nodes register. Because controller-only nodes never write a
`V1BrokerRegistration` record, they:
- are excluded from the controller's set of assignable broker ids (replica placement,
  `CreateTopics`, reassignment),
- do not appear in `Metadata` or `DescribeCluster` responses.

This falls out of registration; no special-casing in the Metadata handler is required
beyond confirming it reads the broker registry (it already does).

---

## 4. Component B — True observer fetch (the load-bearing piece)

Broker-only nodes keep their `MetadataImage` current by **fetching** the
`__cluster_metadata` log, not by openraft replication. (Combined nodes are voters and
already get metadata through the raft state-machine apply path, so they do **not** run
the observer fetcher.)

### 4.1 Serving `__cluster_metadata` from controllers

The raft log is persisted in a crabka `Log` wrapped by `RaftLogStore`
(`crates/raft/src/log_store.rs`); committed entries are readable as an ordered
offset→entry stream via `read_range` (currently `pub(crate)` — to be exposed).

**Transport decision (refined during planning):** the observer fetches over the existing
**controller listener** (port 9093) via a new crabka RPC, *not* via the broker's
client-listener Kafka Fetch handler. Rationale: crabka's controller listener already
speaks a bespoke authenticated RPC protocol (`API_KEY_SUBMIT_CHANGE`, AppendEntries,
Vote) over `OutboundDialer` (TLS/SASL wired, slice 12), and `controller_quorum_voters`
already carries the controller-listener addresses. Layering Kafka Fetch onto a separate
client listener would require discovering controllers' client (9092) addresses and a new
config surface for no parity gain — the metadata-fetch transport is internal
crabka↔crabka and is not a surface JVM tools exercise (clients never fetch
`__cluster_metadata`; `kafka-metadata-quorum --describe` uses `DescribeQuorum`, §6).

- Expose `RaftLogStore::read_range` and stash an `Arc<RaftLogStore>` clone in
  `ControllerHandle` (the store is already an `Arc` used as the openraft storage adapter).
- Add a `ControllerHandle` method to read a committed offset range as Kafka-encoded
  records: `async fn metadata_records(&self, fetch_offset: u64, max_bytes: usize) ->
  MetadataFetchSlice` returning `{ records, log_start_offset, high_watermark }` where
  `high_watermark` = last *applied* (committed) index and `records` are produced by the
  §4.3 bridge. The fetch offset is the openraft log index (§4.4).
- Add a new controller-listener RPC `API_KEY_METADATA_FETCH` (mirroring the
  `API_KEY_SUBMIT_CHANGE` request/response pattern in `crates/raft/src/wire.rs` +
  dispatch in `crates/raft/src/server.rs`). The server handler calls `metadata_records`;
  only voters serve it, and a non-voter returns a leader hint so the observer can retarget
  the quorum.

### 4.2 Observer fetcher on the broker

Add a `MetadataObserver` (`crates/broker/src/metadata_observer.rs`) — a controller-RPC
client loop (mirroring `ControllerHandle::forward_submit_to`, *not* the Kafka replica
fetcher):
- targets the controller quorum from `controller_quorum_voters` (leader, with failover
  to other voters), issuing `API_KEY_METADATA_FETCH` RPCs through `OutboundDialer`,
- tracks its own fetch offset = next openraft log index to fetch,
- on each response, decodes the Kafka records into `MetadataRecord`s via the §4.3 bridge
  and feeds them through `MetadataImage::validate` + `MetadataImage::apply` (both already
  public and incremental/idempotent), publishing each new image via a
  `watch::Sender<Arc<MetadataImage>>` it owns.

Handlers read the current image through a `MetadataSource` abstraction backed by either
the `Controller`'s state-machine watch (voters) or the `MetadataObserver`'s watch
(broker-only nodes), so handler code is unchanged. The observer replaces, for broker-only
nodes, the role the openraft state-machine apply loop plays for voters — and broker-only
nodes therefore do **not** start a `Controller` at all (the raft-membership change Plan 1
deferred).

### 4.3 Serialization bridge (`__cluster_metadata` wire schema)

Today the raft log stores wincode-serialized openraft `Entry`s; Fetch serves Kafka
wire-format record batches. Define a stable Kafka-record encoding for `MetadataRecord`
(real KRaft has exactly such a schema — `ApiMessage` frames keyed by record type +
version). Concretely:
- a `to_kafka_record(&MetadataRecord) -> Record` / `from_kafka_record(&Record) ->
  Result<MetadataRecord>` pair in `crates/metadata`,
- the controller's Fetch path emits these; the observer decodes them.

This is the one genuinely new wire surface and the highest-risk area; it gets its own
plan phase and byte-exactness tests.

### 4.4 Offset alignment

The metadata Fetch offset is the openraft **log index** (entries are stored with
`base_offset == log_id.index`). `log_start_offset` follows raft log truncation/snapshot;
an observer that falls behind the start offset must reset from a snapshot of the current
image (v1: fetch from `log_start_offset` and rebuild, since snapshots are full images).

---

## 5. Component C — Operator role-aware node pools

### 5.1 CRD validation (`crates/operator/src/controller/kafka_node_pool.rs`)

- Replace `RolesNotMixed` check: accept `[Controller]`, `[Broker]`, or
  `[Controller, Broker]`; reject empty.
- Remove `ReplicasNotOne`: accept `replicas >= 1`.
- Add cross-pool validation at the `Kafka` reconciler level: node-id ranges
  `[nodeIdStart, nodeIdStart + replicas)` must not overlap across sibling pools.

### 5.2 Enumeration (`crates/operator/src/controller/kafka.rs`)

`enumerate_brokers` becomes node-level: for each pool, for each ordinal `i in
0..replicas`, emit a node `{ node_id: nodeIdStart + i, pod, fqdn, roles }`. Partition
the result into `voters` (role ⊇ Controller) and `observers` (role == [Broker] only).

### 5.3 TOML rendering (`crates/operator/src/controller/listeners.rs`, `common.rs`)

Per-node configuration gains:
- the effective `process.roles` (injected through the broker CLI environment override),
- `controller.quorum.voters` on broker-only observers = the controller nodes'
  `(node_id, controller_fqdn:9093)`,
- `bootstrap_servers` on every node for discovery,
- bootstrap mode: lowest-node-id controller → `Bootstrap`; other controllers → dynamic
  `Join`; brokers → not a member (observer; no bootstrap mode needed). Controller nodes
  omit `controller.quorum.voters`, because a static list would make a fresh joiner seed
  a second quorum. Restarts → `Rejoin` is decided broker-side from on-disk raft log
  presence (existing design).

### 5.4 Rollout ordering

The node-pool reconciler waits for every controller pool to reach its desired ready
replica count before it creates a broker-only pool — a broker cannot observe a quorum
that does not yet exist. Within controllers, keep the deterministic single-bootstrapper
ordering from the bootstrap-then-join design.

---

## 6. Component D — Admin / wire parity

- **`Metadata` / `DescribeCluster`:** controllers excluded (falls out of §3.4).
- **`DescribeQuorum`** (`crates/broker/src/handlers/describe_quorum.rs`): voters already
  come from openraft membership metrics; add observer tracking. The leader knows each
  observer's fetched offset from the Fetch requests it serves (§4.1), so extend
  `QuorumState` with `observers: Vec<(NodeId, u64)>` and populate the currently-hardcoded
  empty `observers` field so `kafka-metadata-quorum --describe` matches Kafka.

---

## 7. Component E — Runtime quorum reconfiguration (KIP-853)

The controller voter set is **not** frozen at bootstrap. Scaling a controller pool, or
replacing a failed controller, changes the voter set at runtime. openraft already
supports this dynamically (`crates/raft/src/controller.rs` exposes `add_learner` and
`change_membership`); this component wires those into the Kafka admin surface and the
operator.

### 7.1 Raft / broker side

- The two-phase Kafka promotion model maps onto openraft directly: a new controller
  first joins as a **learner** (`add_learner`), catches up to the leader's log, then is
  promoted to **voter** (`change_membership` adding its id). Removal is
  `change_membership` minus the id, then the node is decommissioned.
- A new controller boots in `Join` mode (it is in `process.roles=[controller]` and the
  rendered voter set, but does not `initialize`); the leader brings it in as a learner
  and promotes it once caught up. This reuses the bootstrap-then-join machinery rather
  than adding a parallel path.

### 7.2 Wire parity (KIP-853 RPCs)

Implement the controller-quorum admin RPCs so JVM tooling
(`kafka-metadata-quorum add-controller` / `remove-controller`, `Admin.addRaftVoter` /
`removeRaftVoter`) works against crabka:

- `AddRaftVoter`, `RemoveRaftVoter`, `UpdateRaftVoter` request/response handlers in the
  broker dispatch, served only by the quorum leader (others return
  `NOT_LEADER_OR_FOLLOWER` / forward).
- Exact API keys and versions are confirmed empirically against the latest cp-kafka image
  during planning (per CLAUDE.md), not assumed from the KIP text.
- Handlers translate to `add_learner` + `change_membership`, enforcing KIP-853 safety
  (one voter change at a time; new voter must catch up as a learner before promotion).

### 7.3 Operator side

- When a controller pool's `replicas` increases, the operator (after the new pod is
  Ready and observing) issues `AddRaftVoter` for the new node id; when it decreases, it
  issues `RemoveRaftVoter` **before** scaling the StatefulSet down, so the quorum never
  loses a voter it still counts on.
- The operator drives these changes itself (it already holds cluster credentials and
  reconciles pool state); it does not rely on an external admin invoking the CLI.
- Voter-set changes respect quorum-safety: the operator changes one voter at a time and
  waits for the membership change to commit before the next.

---

## 8. Data flow (end to end)

1. Operator renders TOML: 3 controller nodes (voters, one Bootstrap), N broker nodes
   (observers), each with `process.roles` + `controller.quorum.voters`.
2. Controllers come up first; bootstrap-then-join forms the `__cluster_metadata` quorum.
3. Broker nodes come up, register themselves (`BrokerRegistration` submitted to the
   quorum leader via the existing write-forwarding path), and start their
   `MetadataObserver` loop fetching `__cluster_metadata`-0.
4. A client `CreateTopics` against any broker → forwarded to the controller leader →
   committed to the raft log → controllers apply via state machine; brokers observe the
   new records via Fetch and update their images → partition replicas are assigned only
   to `Broker`-role nodes.
5. `kafka-metadata-quorum --describe` shows controllers as voters and brokers as
   observers with their fetch offsets.
6. Scaling the controller pool from 3 → 5 → operator adds the two new nodes as learners,
   waits for catch-up, then promotes them to voters one at a time; `--describe` reflects
   5 voters. Scaling back down removes voters before the pods are deleted.

---

## 9. Testing strategy

**Broker unit:**
- `process.roles` parse + validate matrix (empty, unknown, each combination).
- controller-only node hosts no partitions and emits no `BrokerRegistration`.
- broker-only node never joins openraft membership.

**Serialization (highest risk):**
- round-trip `MetadataRecord` ↔ Kafka record for every variant; byte-exactness golden
  vectors.

**Raft / integration:**
- 3 controllers + 2 brokers: quorum forms; brokers reach a consistent image via Fetch.
- kill a controller → quorum survives, brokers keep observing.
- kill a broker → no quorum impact; on restart it catches up from its last offset (and
  from `log_start_offset` if it fell behind).
- a `CreateTopics` on a broker propagates to all observers.

**Operator:**
- validation matrix (roles, replicas, cross-pool node-id overlap).
- enumeration with `replicas > 1`; voter-set computation; TOML render snapshot.
- rollout ordering: controllers Ready before brokers created.
- `DescribeQuorum` output includes observers.

**Runtime reconfiguration (KIP-853):**
- `AddRaftVoter`/`RemoveRaftVoter`/`UpdateRaftVoter` handler unit tests (leader-only,
  one-change-at-a-time enforcement, learner-catch-up-before-promotion).
- integration: scale controllers 3→5 and 5→3; quorum stays available throughout; a
  removed voter is gone from `--describe` before its pod is deleted.
- conformance (if feasible): JVM `kafka-metadata-quorum add-controller` /
  `remove-controller` against crabka.

**Conformance (if feasible):** JVM `kafka-metadata-quorum --describe` against a
controller; JVM client metadata round-trip through a broker-only node.

---

## 10. Risks & open questions

1. **Serialization bridge (§4.3)** is the riskiest surface — a new wire schema for
   `__cluster_metadata` records. Mitigation: dedicated plan phase, golden-vector tests,
   model on Kafka's `ApiMessage`/`metadata.json` record types.
2. **Observer offset vs raft snapshot/truncation (§4.4):** v1 rebuilds from
   `log_start_offset` when an observer falls behind; if snapshots compact aggressively
   this could be heavy. Acceptable for v1 (metadata volume is low).
3. **Fetch handler coupling:** special-casing `__cluster_metadata` in the data-plane
   Fetch handler adds a branch to a hot path. Keep it a cheap topic-name check at the top.
4. **Who tracks observer offsets for DescribeQuorum (§6):** the leader sees observer
   Fetches, but a follower controller serving `--describe` may not. v1: report observers
   only when the queried node is the leader (Kafka behavior is leader-authoritative here).
5. **Bootstrap ordering across pools (§5.4):** needs the rollout planner to understand
   roles, which it currently does not. Confirm this composes with the existing
   one-at-a-time config-hash gating.
6. **KIP-853 wire exactness (§7.2):** `AddRaftVoter`/`RemoveRaftVoter`/`UpdateRaftVoter`
   are newer, less-exercised APIs; their exact api keys/versions/field shapes must be
   verified empirically against cp-kafka rather than from the KIP text.
7. **Reconfig vs node failure (§7.3):** the operator must distinguish a deliberate
   scale-down (remove voter, then delete pod) from a transient pod crash (do **not**
   remove the voter). v1 keys off the `KafkaNodePool.spec.replicas` intent, not live pod
   health, to avoid removing a voter during a rolling restart.

---

## 11. References

- `2026-05-17-crabka-operator-kafkanodepool-20-design.md` — slice 20 (this is its
  deferred 20a + 20b).
- `2026-05-14-crabka-bootstrap-then-join-design.md` — `Bootstrap`/`Join`/`Rejoin`.
- `2026-05-14-crabka-raft-membership-design.md` — voter membership mechanics.
- `2026-05-12-crabka-metadata-quorum-design.md` — openraft-backed `__cluster_metadata`.
