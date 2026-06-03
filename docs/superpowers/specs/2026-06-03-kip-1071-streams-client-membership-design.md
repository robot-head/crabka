# KIP-1071 Streams Client — Sub-project #1: Membership client + byte-exact topology builder

**Date:** 2026-06-03
**Status:** Design approved, pending spec review
**Scope:** The first sub-project of the Crabka Streams client-runtime program.

## 1. Context and program decomposition

Crabka has a complete KIP-1071 **broker-side** Streams rebalance protocol
(`crates/broker/src/coordinator/unified/streams/`): topology ingestion, task
derivation, copartition validation, internal-topic creation, active/standby/
warmup assignment, and the heartbeat epoch dance (JVM-validated, commit
`b0874f62`). There is **no Streams client** yet.

A full Kafka Streams-equivalent runtime is far too large for a single spec, so
the program is decomposed into sub-projects, each with its own spec → plan →
implementation cycle:

| # | Sub-project | Delivers | Depends on |
|---|---|---|---|
| **1** | **Membership client + structural topology** (this spec) | `StreamsGroupHeartbeat` lifecycle + a byte-exact topology builder | broker (done) |
| 2 | Processor API + stateless execution engine | `Processor`/`ProcessorContext`/`forward`, the StreamTask loop, at-least-once | 1 |
| 3 | State stores + changelog backing | local KV/window/session stores, changelog produce/restore | 2 |
| 4 | Stateful DSL | KStream/KTable, aggregations, joins, windowing, repartitioning | 3 |
| 5 | Standby + warmup materialization | standby replication, warmup catch-up, `TaskOffsets` reporting | 3 |
| 6 | Interactive Queries | `UserEndpoint`, `PartitionsByUserEndpoint`, local+remote store query | 3 |
| 7 | EOS / transactional integration | exactly-once-v2 over the existing transactional producer | 2 |

This spec covers **sub-project #1** only. It is the foundation everything else
sits on and is independently JVM-interop-validatable against the done broker.

## 2. Goal and non-goals

### Goal

A new `crabka-client-streams` crate that lets a Rust application:

1. **Define a topology** with a Processor-API-style builder and serialize it to
   the `StreamsGroupHeartbeatRequest.Topology` wire shape **byte-for-byte
   identically to the JVM Kafka Streams 4.x client** (so a JVM app and a Crabka
   app can share one streams group).
2. **Join a streams group** via `StreamsGroupHeartbeat`, maintain membership
   with a background heartbeat loop, and **observe assigned active/standby/
   warmup tasks** plus the NotReady status lifecycle, with correct fencing/
   rejoin/leave behavior.

### Non-goals (deferred to later sub-projects)

- **No record processing.** Processors are *structural placeholders* in #1 — the
  builder models the node graph (names, edges, stores) but carries no executable
  logic. Execution is sub-project #2.
- **No state-store data path, no changelog produce/restore** (#3).
- **No DSL** (KStream/KTable + optimizer/auto-naming) — #1 targets the Processor
  API surface only, where byte-exactness is tractable. The DSL is #4 and builds
  on this machinery.
- **No Interactive Queries** (`UserEndpoint`/`PartitionsByUserEndpoint`) (#6).
- **No warmup offset reporting** — `TaskOffsets`/`TaskEndOffsets` sent as
  null/unchanged (#5).
- No rack-aware `ClientTags` assignment input (may be added trivially later).

## 3. Background: protocol surface and broker facts

**`StreamsGroupHeartbeat`** (apiKey 88, v0). The client sends a topology at
join (epoch 0), heartbeats, and receives assigned tasks
`(subtopology_id, partitions)` for active/standby/warmup roles plus a `Status`
list. Request schema:
`crates/protocol/schemas/StreamsGroupHeartbeatRequest.json`. Response:
`StreamsGroupHeartbeatResponse.json`. Owned/borrowed Rust types already
generated under `crates/protocol/.../streams_group_heartbeat_*`.

Verified broker facts (`coordinator/unified/streams/`):

- **Member ID is client-generated.** The broker accepts a client-supplied
  `member_id`, minting a UUID only if it is empty (`actor.rs:278`). The JVM
  Streams client generates its own UUID; we do the same and keep it for the
  process lifetime.
- **Topology epoch + subtopology IDs are entirely client-supplied.** The broker
  stores them verbatim (`topology.rs:to_stored_topology`) and only compares
  epochs for `STALE_TOPOLOGY` (`actor.rs:315`). **Determinism is purely the
  client's job.**
- **Epoch dance** mirrors the next-gen consumer / share-group:
  `member_epoch` 0=join, -1=leave, -2=static rejoin;
  `STALE_MEMBER_EPOCH (113)` / `FENCED_MEMBER_EPOCH (110)` /
  `UNKNOWN_MEMBER_ID (25)` → rejoin from scratch.

## 4. JVM interop contract (byte-exact derivation)

Source-grounded against apache/kafka `trunk`/4.1. The encoder treats these as
the single source of truth. (Files:
`clients/.../consumer/internals/StreamsGroupHeartbeatRequestManager.java`,
`StreamsRebalanceData.java`;
`streams/.../processor/internals/{StreamThread,InternalTopologyBuilder,TopologyMetadata,ProcessorStateManager}.java`.)

- **`Topology.Epoch` is always `0`** in JVM 4.x (`StreamsRebalanceData.topologyEpoch()`
  hard-codes `return 0`). Send `0`.
- **`SubtopologyId` is the node-group index as a decimal string** (`"0"`, `"1"`,
  …), *not* a hash (`StreamThread.initBrokerTopology` →
  `String.valueOf(nodeGroupId)`). Index assignment: `InternalTopologyBuilder.makeNodeGroups()`
  runs union-find over the node graph (predecessor edges + shared-state-store
  connections) in **node insertion order** (`nodeFactories` is a
  `LinkedHashMap`); the first node of a not-yet-seen union-find root mints the
  next integer id. **Groups whose source-topic set is empty are dropped but
  still consume an index** (emitted ids can be non-contiguous).
- **Serialization ordering** (`StreamsGroupHeartbeatRequestManager`):
  - `SourceTopics`, `RepartitionSinkTopics`: sorted **lexicographically**.
  - `RepartitionSourceTopics`, `StateChangelogTopics`: sorted **by name**;
    each `TopicInfo.topicConfigs` sorted **by key**.
  - The `Subtopologies` list: sorted **by `SubtopologyId` as a string** →
    `"0","1","10","11","2",…` (lexicographic, *not* numeric — gotcha).
  - `CopartitionGroups`: `int16` indices into the **sorted** `SourceTopics` /
    `RepartitionSourceTopics` arrays.
  - `SourceTopicRegex` (subtopology-level and copartition-level): **always
    empty** — the JVM resolves patterns to concrete topics client-side. #1
    requires concrete source topics.
- **Internal-topic naming** from `application.id`: changelog
  `<app>-<store>-changelog`; repartition `<app>-<name>-repartition`.
- **`TopicInfo.Partitions`**: **0** for changelog topics (always); for
  repartition-source topics, the enforced partition count if the topology pins
  one, else `0`.

**Byte-exactness is guaranteed for Processor-API topologies**, where node names
and insertion order are explicit and caller-controlled. DSL topologies
(auto-generated names + optimizer passes) are out of scope for #1.

## 5. Architecture: crate and modules

New crate **`crabka-client-streams`** (`crates/client-streams`), matching the
existing `client-consumer`/`-producer`/`-admin` family. Dependencies:
`crabka-client-core` (transport + `Client::send`), `crabka-protocol`
(`streams_group_heartbeat_*` owned types), `tokio`, `uuid`, `tracing`,
`thiserror`, `bon`. Dev-deps: `crabka-broker` with `test-helpers`, `assert2`,
`tempfile`, `tokio` test-util — mirroring `client-consumer`.

```
crates/client-streams/src/
  lib.rs                    crate docs + re-exports
  error.rs                  StreamsClientError
  topology/
    builder.rs              public Topology builder (Processor-API surface)
    node.rs                 internal node-factory model (Source/Processor/Sink, insertion order)
    grouping.rs             port of makeNodeGroups (quick-union, first-seen index, drop-empty-source)
    wire.rs                 built topology → wire Topology (sorting, naming, epoch=0, copartition indices)
    mod.rs
  membership/
    client.rs               public StreamsMembership handle + builder (join / next_event / close)
    coordinator.rs          background StreamsGroupHeartbeat loop (epoch dance, fencing → rejoin)
    assignment.rs           resolve (subtopology_id, partitions) → concrete topic-partitions
    status.rs               NotReady status-code mapping
    mod.rs
```

### Developer-facing API (illustrative; exact signatures pinned in the plan)

```rust
// 1. Build a topology (Processor-API structure; processors are structural
//    placeholders in #1 — record logic arrives in sub-project #2).
let mut topo = Topology::new();
topo.add_source("src", ["input-topic"]);
topo.add_processor("agg", ["src"]);            // name + predecessors
topo.add_state_store("store", ["agg"]);        // store + connected processors → changelog
topo.add_sink("out", "output-topic", ["agg"]);
let built = topo.build("my-application-id")?;  // byte-exact subtopologies + internal-topic names

// 2. Join the streams group and observe assignments.
let membership = StreamsMembership::builder()
    .bootstrap("localhost:9092")
    .group_id("my-group")
    .topology(built)
    .process_id(my_uuid)                  // optional; generated if absent
    .instance_id(Some("static-1"))        // optional static membership
    .rebalance_timeout(Duration::from_secs(30))
    .build().await?;                       // sends join HB (epoch 0 + topology), spawns heartbeat loop

loop {
    match membership.next_event().await? {
        StreamsEvent::Assigned(a)  => { /* a.active / a.standby / a.warmup + resolved topic-partitions */ }
        StreamsEvent::NotReady(s)  => { /* e.g. MissingSourceTopics(["input-topic"]) */ }
        StreamsEvent::Fenced       => { /* auto-rejoined; fresh assignment follows */ }
    }
}
membership.close().await?;  // leave (epoch -1)
```

`StreamsAssignment { active, standby, warmup }` /
`TaskAssignment { subtopology_id, partitions, source_topic_partitions }` is the
seam sub-project #2's execution engine consumes.

## 6. Topology determinism (the byte-exact core)

Three stages, a faithful port of the JVM:

**Stage 1 — Node model (`node.rs`).** Builder records nodes in an
**insertion-ordered** structure (`IndexMap` or `Vec` + name→index). Node kinds:
`Source { topics }`, `Processor { predecessors, connected_stores }`,
`Sink { topic, predecessor }`. State stores record their connected processors.
Insertion order is load-bearing and documented as significant (matching JVM PAPI
add-order).

**Stage 2 — Grouping (`grouping.rs`).** Quick-union over node names: unite a
processor with each predecessor and with every processor sharing a state store.
Iterate nodes in insertion order; first time a root is seen, mint the next
integer `nodeGroupId`. Build each group's topic sets; drop groups with an empty
source-topic set (but they still consumed an index → ids may be non-contiguous).
`SubtopologyId = nodeGroupId.to_string()`.

**Stage 3 — Wire serialization (`wire.rs`).** Encodes every ordering rule in
§4: `epoch = 0`; per-subtopology topic-array sorts; subtopologies sorted by id
**as a string**; copartition `int16` indices into sorted arrays; empty
`SourceTopicRegex`; internal-topic naming from `application_id`; `Partitions`
rules.

## 7. Membership lifecycle (`coordinator.rs`)

Mirrors `share/coordinator.rs` — the heartbeat *is* the join (no Join/Sync).

- **Join.** First heartbeat: `member_epoch = 0`, client-generated UUID
  `member_id`, full `Topology`, `process_id`, `rebalance_timeout_ms`,
  `instance_id` if static. Coordinator discovered via `FindCoordinator` (group
  key), reusing client-core/consumer machinery.
- **Steady-state.** Background tokio task at the broker's `HeartbeatIntervalMs`;
  sends current epoch; `Topology` null after epoch 0; owned task sets null when
  unchanged. Adopt the broker's returned epoch and assignment per response.
- **Reconciliation.** Owned-vs-target task sets. In #1 there is no real state to
  revoke/restore, so adoption is **instantaneous**: adopt the target, echo it
  back as owned on the next heartbeat, advance epoch. (Real revoke = flush/commit
  is #2.)
- **Fencing → rejoin.** Codes 113/110/25 → reset `member_epoch = 0`, resend
  topology, surface `StreamsEvent::Fenced`. Cold-coordinator codes 14/15/16 +
  disconnect → capped backoff retry (reuse the consumer's
  `with_coordinator_retry` shape).
- **Leave.** `close()` sends `member_epoch = -1`; static rejoin uses `-2`.

## 8. Assignment resolution (`assignment.rs`)

Broker `ActiveTasks`/`StandbyTasks`/`WarmupTasks` are
`TaskIds { subtopology_id, partitions }`. Using the built topology, map each
`(subtopology_id, partition)` → the concrete source `(topic, partition)` set the
task consumes → `TaskAssignment { subtopology_id, partitions,
source_topic_partitions }`. This is the seam #2 consumes.

## 9. Status and error handling

**Status (`status.rs`).** Response `Status` list → typed `StreamsStatus`:
`StaleTopology`, `MissingSourceTopics(detail)`,
`IncorrectlyPartitionedTopics(detail)`, `MissingInternalTopics(detail)`,
`ShutdownApplication`, `AssignmentDelayed`. Non-empty status ⇒ member stays
NotReady; keep heartbeating (the broker is creating internal topics) and surface
`StreamsEvent::NotReady(Vec<StreamsStatus>)` until it clears and an assignment
arrives.

**Errors (`error.rs`).** `StreamsClientError` (thiserror) maps the response
`ErrorCode` set: retriable coordinator codes handled internally;
`STREAMS_INVALID_TOPOLOGY` / `_EPOCH` / `_FENCED`, auth failures,
`GROUP_ID_NOT_FOUND` surfaced as fatal; fencing → auto-rejoin (not surfaced as
an error).

## 10. Concurrency shape

The `StreamsMembership` handle shares live epoch/assignment with the background
heartbeat task via `Arc<Mutex<…>>`; `next_event()` drains an `mpsc` fed by the
coordinator loop — the exact `ShareConsumer`/`ShareCoordinatorState` pattern.
`Client` is already `Clone + Send + Sync`.

## 11. Protocol wiring

Ensure the `StreamsGroupHeartbeat` `ProtocolRequest` (apiKey 88, v0) is
dispatchable via `Client::send` and present in client-side `ApiVersions`
negotiation. The generated owned/borrowed types exist; only the dispatch /
negotiation glue may be missing — a small, self-contained task to verify and add.

## 12. Testing strategy (verification gates)

1. **Unit** — `grouping.rs` (union-find, first-seen indexing, drop-empty-source,
   non-contiguous ids) and `wire.rs` (every sort rule, the string-sort gotcha,
   copartition indices, internal-topic naming, partitions rules).
2. **Golden-frame interop** — byte-compare `wire.rs` output against captured
   JVM 4.x Processor-API `StreamsGroupHeartbeat.Topology` frames checked into
   `testdata/`. The real interop gate; independent of broker leniency. Producing
   the golden frames (a JVM Streams PAPI app + captured frame) is its own plan
   task; doable on the Mac (single client RPC, no inter-broker replication).
3. **In-process integration** — spin a `crabka-broker` via `test-helpers`
   (dev-dep, like consumer/admin), enable the `streams.version` feature +
   config, join a streams group, and assert: member-id/epoch progression,
   NotReady→Ready as internal topics are created, `ActiveTasks` for a simple
   topology, and clean leave.
4. **Mixed JVM+Crabka group (flagged milestone)** — a JVM Streams PAPI app + the
   Crabka client in one group against the Crabka broker, asserting disjoint
   active-task assignments. The ultimate interop proof; heavier, can be a
   follow-on (single-client RPC works on the Mac; #1 processes no records so no
   inter-broker data replication is needed).

## 13. Open points to pin down in the plan

- **Reconciliation-ack semantics**: does the broker advance the member epoch on
  epoch alone, or require the client to echo owned tasks first? Verify against
  `coordinator/unified/streams/actor.rs` reconcile path; #1 uses adopt-and-echo
  as the safe model.
- **`application_id` vs `group_id`**: `application_id` defaults to `group_id` and
  drives internal-topic prefixes (matching Kafka Streams, where
  `application.id == group.id`).
- **Within-copartition-group index ordering**: the JVM iterates a `Set<String>`;
  if golden-frame comparison reveals a specific sequence, replicate it exactly.

## 14. Success criteria

- `cargo test -p crabka-client-streams` green, including the golden-frame
  byte-comparison and the in-process broker integration test.
- `cargo clippy --workspace --all-targets -- -D warnings` and
  `cargo fmt --check` clean.
- A documented example (in `lib.rs`) building a topology, joining a group, and
  observing an assignment against the Crabka broker.
