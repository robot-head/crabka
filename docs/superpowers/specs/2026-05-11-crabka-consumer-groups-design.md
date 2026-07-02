# `crabka-consumer-groups` (slice 5) design

**Status:** draft — slice 5 of the Crabka meta-spec.
**Depends on:** slice 4 (`crabka-broker` single-node MVP), slice 2 (`crabka-client-core`), slice 3 (`crabka-log`). All shipped to `main`.
**Tracks the meta-spec at:** [`2026-05-10-crabka-rust-rewrite-design.md`](2026-05-10-crabka-rust-rewrite-design.md).

## Goal

Ship the classic Kafka group-coordinator protocol end-to-end. Acceptance: an unmodified JVM `kafka-console-consumer` (no `--partition`) subscribes through a group, receives records produced by a JVM `kafka-console-producer`, and its committed offsets survive a broker restart. The job runs from the same `mirror.gcr.io/confluentinc/cp-kafka:6.1.1` testcontainers image slice 4 uses.

## In scope

Two crates change:

- **`crabka-broker`** grows a `coordinator` subsystem and six new request handlers (`JoinGroup` / `SyncGroup` / `Heartbeat` / `LeaveGroup` / `OffsetCommit` / `OffsetFetch`). The existing `FindCoordinator` stub is replaced with a real impl.
- **`crabka-client-consumer`** is a new crate: a high-level subscribe-only `Consumer` built on top of slice 2's `crabka-client-core`.

### Wire surface (added handlers)

| API key | Name                | Notes |
|--------:|---------------------|-------|
| 10      | FindCoordinator     | Real impl; returns this broker as the coordinator for any `coordinator_keys`. |
| 11      | JoinGroup           | Blocks until rebalance completes or `rebalance_timeout_ms` elapses. |
| 12      | Heartbeat           | Validates `(generation_id, member_id)`; returns `REBALANCE_IN_PROGRESS`/`UNKNOWN_MEMBER_ID` where appropriate. |
| 13      | LeaveGroup          | Removes the member; non-empty groups transition to `PreparingRebalance`. |
| 14      | SyncGroup           | Leader supplies assignment; non-leaders block until the leader's sync arrives. |
| 8       | OffsetCommit        | Writes records to `__consumer_offsets-0` via the slice-4 partition writer. |
| 9       | OffsetFetch         | Reads from in-memory `Group.committed_offsets` (populated on startup from the topic). |

`ConsumerGroupHeartbeat` (api_key 68) and friends stay `UNSUPPORTED_VERSION`; KIP-848 is a later slice.

### Consumer API

`crabka-client-consumer` exposes a single `Consumer` with the standard builder/poll/commit shape:

```rust
let mut consumer = Consumer::builder("localhost:9092")
    .group_id("my-group")
    .client_id("my-app")
    .session_timeout(Duration::from_secs(45))
    .heartbeat_interval(Duration::from_secs(3))
    .subscribe(&["my-topic"])
    .build()
    .await?;

loop {
    let records = consumer.poll(Duration::from_millis(500)).await?;
    for r in records {
        process(r);
    }
    consumer.commit_sync().await?;
}

consumer.close().await;
```

No `assign()` — manual partition consumption uses `crabka-client-core` directly. No admin RPCs (`DescribeGroups` / `ListGroups`) — those land in slice 10.

### Partition assignor

Only `range` is implemented. The broker is assignor-agnostic (it just plumbs the negotiated `protocol_name` and `assignment_bytes` through the JoinGroup/SyncGroup flow); the consumer-side leader computes the assignment via a pure function in `crabka-client-consumer::assignor::range`. Members proposing only non-`range` assignors get `INCONSISTENT_GROUP_PROTOCOL`.

### `__consumer_offsets` persistence

A real internal topic with **1 partition** (single-broker MVP — coordinator is always us). Created on first `Broker::start` if not present. `Broker::start` replays the topic synchronously before binding the TCP listener, populating `GroupManager` with the saved group memberships, generations, and committed offsets.

Record formats mirror Apache Kafka:
- `OffsetCommitKey` (version 1) + `OffsetCommitValue` (version 3) per `(group_id, topic, partition)` commit
- `GroupMetadataKey` + `GroupMetadataValue` per group state snapshot, written at the end of every successful rebalance

Both keys are byte-compatible so a future Crabka broker can read a Kafka-produced `__consumer_offsets-0` and vice-versa, but this slice doesn't add tests for that direction. No compaction — the topic grows unbounded. Acceptable for the MVP; revisit when slice 3's log-compaction work lands.

## Architecture

### Crate layout

```
crates/broker/                                  # additions to the slice-4 crate
└── src/
    ├── coordinator/
    │   ├── mod.rs            # GroupManager, get_or_create, find, tick
    │   ├── group.rs          # Group state machine: GroupState, Member, methods
    │   ├── assignment.rs     # Assignment bytes container + per-member view
    │   ├── persistence.rs    # OffsetCommit + GroupMetadata record codecs
    │   └── bootstrap.rs      # __consumer_offsets create + replay on startup
    └── handlers/
        ├── join_group.rs
        ├── sync_group.rs
        ├── heartbeat.rs
        ├── leave_group.rs
        ├── offset_commit.rs
        └── offset_fetch.rs   # find_coordinator.rs is updated, not added

crates/client-consumer/                         # NEW crate
├── Cargo.toml
└── src/
    ├── lib.rs                # public API: Consumer, ConsumerBuilder, ConsumerError
    ├── builder.rs            # ConsumerBuilder + Consumer construction
    ├── consumer.rs           # Consumer struct + lifecycle
    ├── assignor/
    │   ├── mod.rs            # ProtocolName, ProtocolMetadata, MemberAssignment helpers
    │   └── range.rs          # the range assignor function + tests
    ├── heartbeat.rs          # spawned task issuing Heartbeat
    ├── poll.rs               # Consumer::poll (parallel FetchRequests across assigned partitions)
    ├── commit.rs             # commit_sync / commit_async
    └── error.rs              # ConsumerError
```

### Components

#### Broker side

- **`GroupManager`** — `groups: DashMap<String, Arc<tokio::sync::Mutex<Group>>>`, a `JoinHandle` for an expirations ticker, and a clone of the broker's partition registry (for `__consumer_offsets-0` access). Methods: `get_or_create(group_id) -> Arc<Mutex<Group>>`, `find(group_id) -> Option<Arc<Mutex<Group>>>`, `await_rebalance(group_id, member_id)` (used by `join_group`), `await_sync(group_id, member_id)` (used by `sync_group`).

- **`Group`** — state, members, generation, leader, committed offsets, rebalance deadline. State machine: `Empty | PreparingRebalance | CompletingRebalance | Stable | Dead`. Holds `tokio::sync::Notify` handles for rebalance + sync gates so handler tasks can park efficiently.

- **`Member`** — `{ member_id, client_id, host, session_timeout: Duration, last_heartbeat: Instant, subscribed_topics: Vec<String>, protocol_metadata: Bytes, assignment: Option<Bytes> }`.

- **`persistence::OffsetCommitKey`** etc. — `Encode` / `Decode` impls for the four `__consumer_offsets` record shapes. Standalone module to make differential testing against Apache Kafka straightforward.

- **`bootstrap::ensure_offsets_topic`** — invoked from `Broker::start` before any handler dispatch can occur. If the topic dir doesn't exist, mkdir + `Log::open` + register; if it does, scan from offset 0 to `log_end_offset` and replay each record into `GroupManager`.

- **Six handlers** — each follows the existing pattern in `crates/broker/src/handlers/`. The two blocking handlers (`join_group`, `sync_group`) use `tokio::sync::Notify` plus `tokio::time::timeout` to park efficiently.

- **Updated `find_coordinator.rs`** — returns this broker for any `coordinator_keys` (consumer or transaction). Drops the stub.

#### Consumer client

- **`Consumer`** — owns: a `crabka_client_core::Client`, the negotiated `(generation_id, member_id)`, the current `HashMap<(topic, partition), i64>` next-offsets, a per-partition `bytes_remaining` cursor, an mpsc receiver of `RebalanceNotice`s from the heartbeat task, and a `JoinHandle` for the heartbeat task.

- **`ConsumerBuilder`** — collects config; `.build()` runs the FindCoordinator → JoinGroup → SyncGroup handshake, derives the initial assignment, spawns the heartbeat task, and returns `Consumer`.

- **`assignor::range`** — pure function. Sorts members and partitions; assigns `range_size = partition_count.div_ceil(member_count)` contiguous partitions per member; trims for the last member.

- **`heartbeat::run`** — spawned task. `tokio::time::interval(heartbeat_interval)`. On error responses, sends `RebalanceNotice::{NeedRejoin | RejoinFromScratch}` on its `mpsc::Sender`. Exits when `consumer.close()` cancels its `CancellationToken`.

- **`poll`** — builds one `FetchRequest` covering all assigned partitions, sends it on the single connection, decodes the response, advances next-offsets per partition.

- **`commit`** — builds `OffsetCommitRequest` from the supplied offsets (or, when none are supplied to `commit_sync()`, from `next_offsets - 1` for every assigned partition).

## Data flow

### JoinGroup → SyncGroup handshake

The detailed dance from Section 3 of the brainstorming is captured here. Key gates:

1. JoinGroup arrives → `Group::add_member` adds it. If first member, generation += 1 and the group's `rebalance_deadline` is `now + max(session_timeout, rebalance_timeout)` from the joining member's view. The handler then `await`s a per-group `Notify` *with a deadline*.
2. When either (a) every expected member has joined or (b) the deadline elapses, the broker transitions to `CompletingRebalance`, elects the oldest member as leader, and `notify_waiters()`s.
3. Every waiting JoinGroup handler wakes, snapshots the current state, and returns the `JoinGroupResponse` (leader gets `members: full list`, others get `members: []`).
4. SyncGroup from the leader stores assignments per-member and `notify_waiters()`s the sync gate. SyncGroup from non-leaders parks until the leader arrives, then responds with that member's assignment.
5. Group transitions to `Stable`.

### Heartbeat loop

- Consumer's heartbeat task fires every `heartbeat_interval_ms`.
- Broker validates `generation_id` and `member_id`; updates `last_heartbeat`; responds 0 / 27 / 25.
- A separate broker-side `tokio::time::interval` runs `GroupManager::tick()` every 1 s (configurable). Per group, any member with `now - last_heartbeat > session_timeout` is dropped; the group transitions to `PreparingRebalance` if it still has members, or `Empty` if not.

### OffsetCommit

- Handler validates `(generation, member)`; encodes `OffsetCommitKey` + `OffsetCommitValue` records into a `RecordBatch`; sends a `ProduceJob` to `partitions[("__consumer_offsets", 0)].writer_tx`; awaits the oneshot ack.
- On success: update `Group.committed_offsets`; respond with per-(topic, partition) error_code = 0.
- On failure (writer dead, log error): respond with `UNKNOWN_SERVER_ERROR` and DON'T touch `committed_offsets`.

### Group state snapshot writes

After a successful rebalance (Sync completes), the handler builds a `GroupMetadataKey` + `GroupMetadataValue` record reflecting the new state and appends it to `__consumer_offsets-0` (via the same partition writer). If the write fails the group still transitions to `Stable` in memory; recovery is "lose this snapshot on restart and re-join". The next OffsetCommit or rebalance will write a fresh record.

### Startup replay

```
Broker::start
  log_dir::scan  →  if "__consumer_offsets-0" missing, create it (1 partition, 1 RF).
  Open the log; iterate every RecordBatch:
    for each record in batch:
      decode key → discriminate OffsetCommitKey vs GroupMetadataKey
      decode value
      mutate the in-memory image:
        OffsetCommitKey → Group.committed_offsets[(topic, partition)] = OffsetEntry { offset, metadata, leader_epoch, commit_timestamp }
        GroupMetadataKey → replace Group.members, generation, leader, protocol_name, state = Stable (or Empty if no members)
  Bind the TCP listener.
```

The replay is synchronous and runs in `Broker::start`'s task; it must complete before any client connection is accepted. For an MVP with bounded `__consumer_offsets` size this is fine; future work bounds replay time via log compaction or snapshotting.

## Error handling

### Wire-level codes (additions to `crates/broker/src/codes.rs`)

| Code | Name                          | Where |
|-----:|-------------------------------|-------|
| 22   | ILLEGAL_GENERATION            | `Heartbeat`, `OffsetCommit`, `SyncGroup` when generation_id mismatches. |
| 23   | INCONSISTENT_GROUP_PROTOCOL   | `JoinGroup` when proposed protocols share no member. |
| 25   | UNKNOWN_MEMBER_ID             | Member not found in group. |
| 27   | REBALANCE_IN_PROGRESS         | Heartbeat / OffsetCommit while group is `PreparingRebalance` or `CompletingRebalance`. |
| 79   | MEMBER_ID_REQUIRED            | First JoinGroup with empty `member_id`; broker generates one, returns it in the response, client retries. |

The pre-slice-5 codes (`COORDINATOR_NOT_AVAILABLE`, `NOT_COORDINATOR`, `UNKNOWN_SERVER_ERROR`, `UNSUPPORTED_VERSION`) stay in their existing roles.

### Internal `BrokerError`

Add variants `GroupInvalidState { group_id, state }`, `UnknownMember { group_id, member_id }`, `GenerationMismatch { group_id, current, requested }`. These map via the existing handler boundary into wire codes.

### `ConsumerError`

`crabka-client-consumer::ConsumerError` carries:
- `Client(crabka_client_core::ClientError)` for transport errors
- `RebalanceFailed(String)`
- `NotSubscribed`
- `CommitInvalid` (rebalance happened mid-poll)
- `CoordinatorUnavailable`

### Rebalance handling on the consumer

The heartbeat task feeds `RebalanceNotice`s into an `mpsc::Receiver<RebalanceNotice>` that `poll()` checks before each iteration. On `NeedRejoin`, `poll()` does JoinGroup → SyncGroup again with the same `member_id` (keeping group membership but acknowledging a new generation). On `RejoinFromScratch`, it starts over with `member_id = ""`. Either way, partition assignments may change; the new assignment supersedes any in-flight per-partition fetch.

### Panic / supervisor handling

Carries over from slice 4. Every spawned task (per-group expiration timer, consumer heartbeat task) is `tokio::spawn`'d under the broker/consumer's supervisor; panics are logged and don't take down the process. Consumer-side heartbeat panic → consumer becomes a dead-but-not-removed member until session timeout (~45 s default), at which point the broker drops it. The consumer's foreground notices via heartbeat-task channel close and surfaces a `CoordinatorUnavailable` error on the next `poll()`.

## Testing

### Unit tests

**Broker** (`crates/broker/tests/unit.rs` additions):

- Per-handler tests for all six new handlers + the real FindCoordinator (drives an in-process broker via `crabka-client-core::Client`, just like slice 4).
- `Group` state-machine table-driven tests: every valid transition, every invalid request → expected error code.
- `range` assignor: parameterized (1m/Np, 2m/Np, 3m/2t/different-Np) — same fixture pattern slice 2 used for codec tests.
- Startup replay: pre-seed `__consumer_offsets-0` log dir with synthetic records (using the persistence codec directly), `Broker::start`, assert in-memory `Group` state matches.
- Heartbeat session-timeout: a member that misses heartbeats > session_timeout is dropped; group transitions to `PreparingRebalance` if other members remain.

**Consumer** (`crates/client-consumer/tests/unit.rs`):

- Assignor in isolation (identical to broker-side assignor tests; lives in the consumer crate since the consumer leader computes the assignment).
- `MockBroker`-driven (reuse slice 2's mock) tests: subscribe → join → sync → poll → commit → close, with the mock asserting the right wire calls in order.
- Heartbeat task notifying poll() of REBALANCE_IN_PROGRESS.

### Integration tests

`crates/broker/tests/integration.rs` (additions) and `crates/client-consumer/tests/integration.rs` (new):

- End-to-end Rust → Rust:
  - Producer (`crabka-client-core`) writes records → Consumer (`crabka-client-consumer`) subscribes with `group_id="g"`, polls, gets the records, commits.
  - Two consumers join the same group on a 2-partition topic → each owns one partition.
  - Consumer commits offset 42 → broker restarts → new consumer in same group reads from offset 42.
  - Consumer dies (heartbeat task killed) → broker drops member after session_timeout → remaining consumers rebalance.

These run on every push, no Docker.

### JVM acceptance

`crates/broker/tests/jvm_acceptance.rs` adds one test:

- `console_consumer_with_group_round_trip`: `kafka-topics --create`, `kafka-console-producer` writes records, `kafka-console-consumer --from-beginning --topic <t>` (note: no `--partition`) subscribes via the default `console-consumer` group and reads records back. Same `docker run --add-host=host.docker.internal:host-gateway` pattern slice 4 established.

The new test joins the existing `broker-jvm-acceptance` CI job rather than getting its own — keeps CI lean.

### Out of scope for testing

- KIP-848 ConsumerGroupHeartbeat round-trip
- Cooperative-sticky rebalance partial-revocation
- Transactional offset commits (`TxnOffsetCommit`)
- Multi-broker coordinator failover
- Chaos / arbitrary disconnect injection

## Out of scope (explicit non-goals)

- **KIP-848 (next-gen consumer rebalance)** — `ConsumerGroupHeartbeat` (api_key 68) stays `UNSUPPORTED_VERSION`. Slice 5b material.
- **KIP-429 cooperative-sticky** — only eager (stop-the-world) range rebalance.
- **KIP-345 static membership** — `group.instance.id` is parsed but ignored.
- **Other assignors** — `roundrobin`, `sticky`, `cooperative-sticky` not implemented; coordinator rejects with `INCONSISTENT_GROUP_PROTOCOL` if the only proposals are these.
- **Transactional consumers** — `TxnOffsetCommit` (api_key 28) stays `UNSUPPORTED_VERSION`. Slice 9.
- **Multi-broker coordinator handoff** — single-node MVP; coordinator is always us. Slice 8.
- **`__consumer_offsets` log compaction** — slice 3 deferred compaction; this slice doesn't fix it. Retention disabled on the internal topic so we never lose state.
- **50-partition `__consumer_offsets`** — 1 partition (hash → always partition 0). Multi-partition fan-out is a slice-8 concern.
- **`DescribeGroups` / `ListGroups`** — admin RPCs, slice 10.
- **Auto-commit edge cases** (`commit-on-shutdown` without explicit `close()`, partial-commit-then-rebalance recovery) — only the happy path is in scope.
- **Consumer-group authorizer hooks** — slice 11.

## Acceptance gate

The slice is done when, in CI:

1. `cargo fmt --all -- --check` clean.
2. `cargo clippy --workspace --all-targets -- -D warnings` clean.
3. `cargo test -p crabka-broker` and `cargo test -p crabka-client-consumer` both pass.
4. `cargo test --workspace --include-ignored` is no worse than before (no regressions in slices 1-4).
5. `broker-jvm-acceptance` job stays green AND includes `console_consumer_with_group_round_trip`.
6. `cargo doc -p crabka-broker --no-deps` and `cargo doc -p crabka-client-consumer --no-deps` build without warnings; every public type carries rustdoc.
7. Public API of `crabka-client-consumer`: `Consumer`, `ConsumerBuilder`, `ConsumerRecord`, `ConsumerError`. No assign(), no admin RPCs.

## Reference

Meta-spec: [`2026-05-10-crabka-rust-rewrite-design.md`](2026-05-10-crabka-rust-rewrite-design.md).
Slice 4 spec: [`2026-05-11-crabka-broker-design.md`](2026-05-11-crabka-broker-design.md).
Slice 2 spec: [`2026-05-11-crabka-client-core-design.md`](2026-05-11-crabka-client-core-design.md).
Slice 3 spec: [`2026-05-11-crabka-log-design.md`](2026-05-11-crabka-log-design.md).
