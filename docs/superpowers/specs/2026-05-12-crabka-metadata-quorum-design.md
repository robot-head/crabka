# Slice 7: Metadata quorum via openraft — design

## Summary

Replace slice-4's in-memory metadata with an [openraft][openraft]-backed
quorum across N Crabka brokers. Every broker is a voter; topic, partition,
and broker-registration records are agreed on through Raft and applied to
a shared `MetadataImage`. JVM Kafka clients continue to talk plain Kafka
TCP to any broker; behind the scenes the brokers stay consistent. Wire-
compatible KRaft is explicitly out of scope and gets its own future slice.

[openraft]: https://github.com/databendlabs/openraft

## Non-goals

- KRaft wire compatibility (Vote/BeginQuorumEpoch/EndQuorumEpoch api keys,
  KRaft Fetch piggyback). Mixed JVM+Crabka quorums therefore remain
  impossible until a later slice ports KRaft on top of openraft.
- Partition data replication (`replication.factor > 1` for user topics).
  That's slice 8.
- Snapshots / InstallSnapshot. The handler is a stub that rejects the RPC
  with `NotImplemented`; followers that fall too far behind have to be
  restarted in slice 7. A future slice 7-followup wires the real path.
- Dynamic voter membership changes (add/remove voter RPCs).
- Authentication on the controller listener.
- `crabka-cli` quorum inspection tooling (slice 10).

## Crate layout

Two new crates plus targeted changes to `crabka-broker`:

| Crate              | Status   | Responsibility                                                                                          |
|--------------------|----------|---------------------------------------------------------------------------------------------------------|
| `crabka-raft`      | **new**  | openraft adapters (`RaftLogStorage`, `RaftStateMachine`, `RaftNetworkFactory`) + the `Controller` type. |
| `crabka-metadata`  | **new**  | Versioned metadata record types + `MetadataImage` read snapshot.                                        |
| `crabka-broker`    | changed  | Quorum-backed metadata. Two listeners (client + controller). `CreateTopics`/`DeleteTopics` route through `Controller`. |
| `crabka-protocol`  | unchanged| Controller-private wire types live in `crabka-raft::wire`, not in the codegen schemas.                  |

## Architecture

```
                       client port (9092)              controller port (9093)
                            │                                │
              ┌─────────────┴───────────────┐    ┌───────────┴────────────┐
              │  slice-1..6 framing +       │    │  crabka-raft           │
              │  dispatch                   │    │  openraft RPCs         │
              │  → Produce/Fetch/Metadata   │    │  (api_keys 1000+)      │
              │    /InitProducerId/         │    │  AppendEntries/Vote/…  │
              │    CreateTopics/…           │    │                        │
              └──────┬──────────────────────┘    └────┬───────────────────┘
                     │ writes route via               │ openraft commits
                     ▼                                │ apply into …
              ┌──────────────────────────┐            │
              │   Controller handle      │◀───────────┘
              │   (submit_change → fut)  │
              └──────────┬───────────────┘
                         │  on commit
                         ▼
              ┌──────────────────────────┐
              │   MetadataImage (Arc)    │  ◀── Metadata handlers read here
              └──────────────────────────┘
```

**Two listeners per broker.** The client listener keeps the slice-1..6
dispatch shape unchanged. The new controller listener owns its own
dispatcher in `crabka-broker::network::controller_dispatch`; it accepts
only api_keys in `[1000, 1002]` and rejects everything else with
`INVALID_REQUEST`. Separation mirrors KRaft's
`controller.listener.names`/`inter.broker.listener.name` split and leaves
room for future auth/ACL divergence.

**Why `crabka-log` for the Raft log?** It's already the most battle-
tested code in the repo and uses the byte-format the JVM expects for the
metadata partition (`@metadata-0`). It positions us to switch to KRaft-
wire later without rewriting the log layer — only the RPC bodies change.

## Components

### `crabka-raft::Controller`

```rust
pub struct ControllerConfig {
    pub node_id: NodeId,                 // = broker_id
    pub voters: Vec<(NodeId, SocketAddr)>,
    pub controller_listen_addr: SocketAddr,
    pub log_dir: PathBuf,                // hosts @metadata-0
    pub election_timeout: Duration,      // default 1s
    pub heartbeat_interval: Duration,    // default 200ms
}

pub struct ControllerHandle {
    /// Submit a batch of metadata records. Future resolves when the records
    /// are committed AND applied to the local state machine.
    pub fn submit_change(
        &self,
        records: Vec<MetadataRecord>,
    ) -> impl Future<Output = Result<(), RaftError>> + Send;

    /// Read-only snapshot of the current image. Cheap (Arc clone).
    pub fn current_image(&self) -> Arc<MetadataImage>;

    /// Stream leader-id changes.
    pub fn watch_leader(&self) -> tokio::sync::watch::Receiver<Option<NodeId>>;

    pub async fn shutdown(self);
}

impl Controller {
    pub async fn start(config: ControllerConfig) -> Result<ControllerHandle, RaftError>;
}
```

`start` opens/recovers `@metadata-0` via `crabka-log`, replays existing
entries into a fresh `MetadataImage`, spawns the openraft node with the
static voter set, opens the controller listener, and returns the handle.

### `crabka-metadata::MetadataImage`

```rust
pub struct MetadataImage {
    inner: MetadataImageInner,           // private
}

pub enum MetadataRecord {
    V1Topic(TopicRecord),
    V1Partition(PartitionRecord),
    V1BrokerRegistration(BrokerRegistrationRecord),
    V1DeleteTopic(DeleteTopicRecord),
}

// Read API used by the broker's handlers:
impl MetadataImage {
    pub fn topics(&self) -> impl Iterator<Item = &TopicRecord>;
    pub fn topic(&self, name: &str) -> Option<&TopicRecord>;
    pub fn partition(&self, topic: &str, idx: i32) -> Option<&PartitionRecord>;
    pub fn broker(&self, node_id: NodeId) -> Option<&BrokerRegistrationRecord>;
    pub fn brokers(&self) -> impl Iterator<Item = &BrokerRegistrationRecord>;
    pub fn cluster_id(&self) -> Uuid;
}
```

`MetadataImage` is wrapped in an `Arc` inside `Controller` and atomically
swapped on each apply. Readers always see a consistent image.

`MetadataRecord` is a versioned enum. Future versions can add variants
without breaking older Raft logs because `bincode` skips unknown
discriminants gracefully if we encode each variant length-prefixed.

### `crabka-broker` changes

- New `BrokerConfig` fields:
  ```rust
  node_id: NodeId,
  controller_listen_addr: SocketAddr,
  controller_quorum_voters: Vec<(NodeId, SocketAddr)>,
  ```
- `Broker::start` calls `Controller::start(...)` before opening the
  client listener; bails if quorum start fails.
- Slice-4's in-memory `MetadataState` is deleted. Every handler that
  previously read it now reads from `controller.current_image()`.
- `CreateTopics` / `DeleteTopics` handlers build the appropriate
  `MetadataRecord`s and call `controller.submit_change(...)`. On
  `RaftError::NotLeader { current_leader }`, the handler routes the
  request to the leader's controller port (with retry) and returns the
  result. After 3 failed attempts or a leader churn, the JVM client sees
  `NOT_CONTROLLER = 41` with the best-known leader id.
- `BrokerRegistrationRecord` is submitted by each broker once on startup
  (after `Controller::start` confirms quorum membership). Brokers do
  NOT heartbeat in slice 7 — broker liveness is the next slice's
  concern.

## Wire protocol

Three new Crabka-private api keys, all v0, framed identically to existing
Kafka requests (`crabka-protocol`'s `length_prefixed` + `RequestHeader` v2,
flexible):

| api_key | Name                    | Notes                                              |
|---------|-------------------------|----------------------------------------------------|
| 1000    | `CrabkaAppendEntries`   | openraft `AppendEntries`                           |
| 1001    | `CrabkaVote`            | openraft `Vote`                                    |
| 1002    | `CrabkaInstallSnapshot` | Stub: returns `NotImplemented`. Reserves the slot. |

These do not flow through `crabka-protocol`'s codegen — they are hand-
written `Encode`/`Decode` impls in `crabka-raft::wire`.

### `CrabkaAppendEntriesRequest` v0

```
node_id            INT32      // sender (leader) node id
term               INT64
leader_id          INT32
prev_log_index     INT64
prev_log_term      INT64
leader_commit      INT64
entries            ARRAY of {
    log_index      INT64
    log_term       INT64
    payload_kind   INT8       // 0 = blank, 1 = normal, 2 = membership
    payload        BYTES      // bincode-serialized payload (see below)
}
```

### `CrabkaAppendEntriesResponse` v0

```
success            BOOLEAN
term               INT64
last_log_index     INT64
```

### `CrabkaVoteRequest` / `CrabkaVoteResponse` v0

Mirror openraft's `Vote` / `VoteResponse`: `term`, `candidate_id`,
`last_log_index`, `last_log_term` → `vote_granted`, `term`.

### Payload serialization

`entries.payload` is `bincode` v2 with fixed config (little-endian,
varint). For `payload_kind = 1`, the payload decodes to
`Vec<MetadataRecord>`. For `payload_kind = 2`, it decodes to a voter-set
struct (reserved for the deferred dynamic-membership work; encoded but
never received in slice 7). Splitting "what's in the log" from "what's
on the wire" via bincode keeps the wire stable as record schemas evolve.

## Data flow

### Happy-path topic creation

```
JVM client ──CreateTopicsRequest──▶ broker (any node)
                                       │
                                       │ pre-validate against current image
                                       │ build TopicRecord + PartitionRecord(s)
                                       ▼
                              Controller::submit_change(records)
                                       │  if not leader, forward to leader's
                                       │  controller port via cached Connection
                                       ▼
                            openraft.client_write(payload)
                                       │  replicate → commit (majority)
                                       ▼
                            state_machine.apply(records) → new MetadataImage
                                       │  resolves the submit_change future
                                       ▼
                            broker → CreateTopicsResponse (error_code = 0)
```

### Bootstrap

Every voter starts with the same static voter set. openraft handles
election. A 1-voter "cluster" is the new slice-1..6 single-node mode —
there is no non-quorum bypass.

### Leader change mid-write

`Controller::submit_change` snapshots the current leader from a `watch`,
re-routes on `NotLeader`. After 3 attempts or a leader churn during the
in-flight RPC, surface `RaftError::NotLeader { current_leader }` — the
broker maps this to `NOT_CONTROLLER` with the leader id in the response,
which Kafka clients understand and use to re-bootstrap.

## Error handling

### Error types

`crabka_raft::RaftError`:

```rust
#[non_exhaustive]
pub enum RaftError {
    Storage(crabka_log::LogError),
    Network(crabka_client_core::ClientError),
    Openraft(openraft::error::Fatal<NodeId>),
    NotLeader { current_leader: Option<NodeId> },
    LeaderUnknown,
    ChangeRejected(String),
    SerdeFailed(bincode::error::EncodeError),
    SerdeFailedDecode(bincode::error::DecodeError),
    Shutdown,
}
```

`crabka_metadata::MetadataError`:

```rust
#[non_exhaustive]
pub enum MetadataError {
    TopicExists(String),
    UnknownTopic(String),
    InvalidPartition { topic: String, partition: i32 },
    InvalidRecord(&'static str),
}
```

### Resolution policy

- **`NotLeader { current_leader: Some(id) }`** → `submit_change` opens a
  cached `Connection` to `id`'s controller port and re-issues the change.
  Up to 3 attempts with 100 ms backoff; after that, surface
  `NOT_CONTROLLER` to the client.
- **`LeaderUnknown`** (election in progress) → block up to 5 s on the
  leader-watch, then surface `NOT_CONTROLLER` with `leader_id = -1`.
- **Storage / Network errors from openraft's perspective** → openraft
  internally retries transient ones; what surfaces here is fatal and we
  return an error from `Broker::start` rather than running a half-broken
  node.
- **State-machine apply errors** → must be infallible. Pre-validation
  inside `submit_change` (against the current image) rejects bad records
  before they reach openraft. Concurrent submits can race past pre-
  validation; whichever record commits first wins, and the loser sees
  `TOPIC_ALREADY_EXISTS` on its next read.

No new wire codes. Clients only ever see `NOT_CONTROLLER` (41),
`TOPIC_ALREADY_EXISTS` (36), `UNKNOWN_TOPIC_OR_PARTITION` (3) — all of
which existed before slice 7.

## Observability

- Tracing spans, every async boundary that crosses an RPC: `raft.
  append_entries`, `raft.vote`, `raft.commit_index_advance`,
  `controller.submit_change`.
- Structured events emitted via `tracing` for: leader-id change, term
  bump, log gap repair, state-machine apply timing.
- Per-broker exported metrics (`tracing` events for now; OTLP later in
  slice 11): `raft.current_term`, `raft.role`, `raft.commit_index`,
  `raft.last_log_index`, `raft.leader_id`,
  `controller.submit_change.latency_ms`, `controller.queue_depth`.

## Testing

### Layer 1 — unit tests

- `crabka-raft`:
  - `RaftLogStore` round-trips (append, get-by-index, truncate-to,
    recover-from-empty).
  - Wire encode → decode → Eq with proptest generators.
  - `Controller::submit_change` pre-validation rejection paths via an
    in-process single-voter cluster.
  - Leader-forwarding via a mock `RaftNetwork`.
- `crabka-metadata`:
  - `MetadataRecord::V1*` bincode round-trips (proptest).
  - `MetadataImage::apply` invariants per record type.
  - DeleteTopic clears all matching partition entries.

### Layer 2 — multi-node integration in-process

`crates/broker/tests/quorum.rs`:

- `three_node_cluster_elects_leader` — exactly one leader within 5 s.
- `create_topic_on_any_node_propagates` — issue against each node;
  metadata appears on the other two within 1 s.
- `leader_kill_recovers` — kill the elected leader, new one within 5 s,
  metadata writes against the new leader succeed.
- `follower_forwards_create_topic` — explicit non-leader target succeeds.
- `concurrent_topic_creates_one_wins` — 10 brokers race the same topic
  name; exactly one succeeds, the rest get `TOPIC_ALREADY_EXISTS`.

### Layer 3 — JVM acceptance

`crates/broker/tests/jvm_acceptance.rs::three_node_jvm_round_trip`:

1. Boot 3-node Crabka cluster (client ports 9092/9192/9292, controller
   ports 9093/9193/9293).
2. `kafka-topics --create` via node 1.
3. `kafka-console-producer` writes 100 records via node 2.
4. `kafka-console-consumer` reads them back via node 3 — asserts 100
   visible.
5. Identify the Raft leader; shut it down.
6. Wait 5 s for re-election.
7. `kafka-topics --list` via a surviving node confirms the topic still
   exists.

`#[ignore = "requires Docker"]`; CI runs with `--include-ignored`.

### Layer 4 — proptest on metadata record evolution

`crates/metadata/tests/evolution.rs` — generate random `MetadataRecord`
trees, encode v1, decode v1, assert structural equality. Future record
versions extend this with "decode v2 → re-encode v1 round-trips for the
fields v1 understands" to keep schema evolution honest.

### Not tested in slice 7

- Snapshot transfer (handler is a stub).
- Partition data replication (slice 8).
- Voter membership change (deferred).

## Acceptance gate

Slice 7 is shippable when:

1. `cargo test --workspace -- --include-ignored` passes locally and on CI.
2. `cargo clippy --workspace --all-targets -- -D warnings` clean.
3. `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps` clean.
4. `three_node_jvm_round_trip` green on CI's Docker-enabled runner.
5. All `quorum.rs` integration tests green.
6. All existing slice-1..6 JVM acceptance tests pass against a single-
   voter Crabka cluster (the new single-node shape).
7. `Broker::start` with `controller.quorum.voters = 1@localhost:9093`
   works as the slice-4/5/6 single-node replacement — no flag, no opt-in.

## Risks

- **openraft + `crabka-log` impedance mismatch.** openraft's
  `RaftLogStorage` wants O(1) random reads by index; `crabka-log` is
  segment-based with offset indexes. Mitigation: openraft's hot path is
  sequential (follower replication); the random-read case is bounded to
  the log tip for conflict resolution, where the latest segment is
  always in memory.
- **State machine apply must not hold cross-await locks.** openraft
  serializes apply through its own runtime. Mitigation: `MetadataImage`
  is `Arc`-swapped; apply is synchronous + non-blocking.
- **Three-node JVM acceptance test wall time.** Could push CI past 5
  min. Mitigation: aggressive parallelism on Layer 2; the JVM test is
  one scenario, not a matrix.

## Next step after this spec

Invoke `superpowers:writing-plans` to produce a task-level
implementation plan for slice 7.
