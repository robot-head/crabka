# Slice 8: Replication — design

## Summary

Basic partition replication for Crabka. When `CreateTopics` runs with
`replication_factor=N`, the controller deterministically assigns N
replicas per partition via round-robin over registered brokers. Each
follower broker runs a per-partition replication task that continually
issues Kafka `Fetch` requests (api_key=1) to the leader with
`replica_id = self.node_id` set, appending received batches to its
local `crabka-log`. The on-disk log files on all replicas converge to
byte-equal contents.

This is the smallest slice that demonstrates multi-broker replication
end-to-end. ISR shrink/expand, high-watermark tracking, acks=all
blocking, AlterPartition RPC, controller-driven leader failover, and
cross-broker producer routing are each explicitly deferred — see
"Out of scope" below.

## Non-goals

- **ISR tracking.** `V1Partition.isr` stays equal to `V1Partition.replicas`
  for the whole partition lifetime. Lag detection + shrink/expand land
  in a slice-8 follow-up.
- **High-watermark / `acks=all` blocking.** `Fetch` for follower vs
  consumer takes the same code path in this slice (no HW filtering).
  Producer `acks=all` is effectively `acks=1` (local-leader ack only).
- **AlterPartition RPC** (KIP-497). Leader-to-controller ISR change
  proposals.
- **Leader election on broker failure.** A failed leader's partition
  stays unavailable. Recovery requires the broker to come back. The
  slice-7 metadata quorum *does* survive metadata-leader failure; this
  slice's deferral is only about **partition** leader failover.
- **Cross-broker Rust producer routing.** The slice-6 producer still
  uses its bootstrap connection for all Produce requests. JVM clients
  work because they natively follow `NOT_LEADER_FOR_PARTITION` hints
  in metadata responses; the slice-6 producer's retrofit is a separate
  follow-up.
- **KIP-101 leader-epoch + KIP-279 truncation safety.** Slice 8 does
  not propagate leader-epoch through the wire; followers truncate on
  `OFFSET_OUT_OF_RANGE` alone, not on epoch divergence.
- **Per-source-broker batched ReplicaFetcherThread.** Apache Kafka
  batches all per-(leader, follower) partitions into one Fetch round
  trip. Slice 8 uses one task per (topic, partition) per follower for
  simplicity. The optimization is a measured-need follow-up.
- **Rebalancing existing partitions when a new broker joins.** New
  brokers only appear in future `CreateTopics` assignments.

## Crate layout

No new crates. Everything lives in `crabka-broker`:

| Module | Status | Responsibility |
|---|---|---|
| `handlers/create_topics.rs` | modified | Pre-Raft step: read `controller.current_image().brokers()`, compute round-robin replica assignment per partition, build `V1Topic + V1Partition` records with `replicas` + `leader` baked in. |
| `handlers/fetch.rs` | modified | Branch on `replica_id`: `< 0` (consumer) → slice-4 path; `≥ 0` (follower) → serve from log without HW filtering (HW filtering is still a no-op in slice 8 either way). |
| `replicator.rs` | **new** | Per-partition replication task: open a `Connection` to the leader's advertised `host:port`, loop on `Fetch`, append received batches to local log via `crabka-log`. Handle `OFFSET_OUT_OF_RANGE` by truncating to 0 and re-fetching. |
| `replicator_supervisor.rs` | **new** | Subscribes to the controller's `watch_image()`. On each metadata apply, diffs the desired follower assignments against the running tasks: spawns new, cancels removed via per-task `CancellationToken`. |
| `broker.rs` | modified | Construct + spawn the supervisor in `Broker::start`. Cancels supervisor in `BrokerHandle::shutdown`. |
| `error.rs` | modified | Add `BrokerError::Replication(String)` for diagnostic logging. |

## Architecture

```
                  ┌────────────────────────────────────────┐
                  │  controller (openraft, slice 7)        │
                  │  V1Partition { topic, idx, leader,     │
                  │                replicas: [n1,n2,n3] }  │
                  └─────────────┬──────────────────────────┘
                                │ watch_image()
                                ▼
                ┌────────────────────────────────────┐
                │ replicator_supervisor              │
                │  diff(prev, current)               │
                │   → spawn(new)                     │
                │   → cancel(removed)                │
                └───────┬──────────────┬─────────────┘
                        │              │
                        ▼              ▼
              ┌──────────────────┐   ┌──────────────────┐
              │ replicator task  │   │ replicator task  │
              │ (topic-A, p=0)   │   │ (topic-B, p=2)   │
              │  Fetch loop →    │   │  Fetch loop →    │
              │  Connection      │   │  Connection      │
              │   to leader's    │   │   to leader's    │
              │   advertised     │   │   advertised     │
              │   port           │   │   port           │
              └────────┬─────────┘   └────────┬─────────┘
                       │                      │
                       │ Fetch(replica_id=node_id, …)
                       ▼                      ▼
                ┌─────────────────────────────────────┐
                │ leader broker's Fetch handler       │
                │  if replica_id >= 0:                │
                │    serve from log without HW filter │
                │  else (consumer):                   │
                │    serve up to HW (slice-4 path)    │
                └─────────────────────────────────────┘
```

**Two listeners stay the same** as slice 7 (client port + controller
port). Replication traffic uses the client port — it's standard Kafka
`Fetch`.

## Components

### `CreateTopics` handler — replica assignment

When the handler runs (on whichever broker received the request, then
forwarded to the Raft leader if it wasn't already there):

1. Read `controller.current_image().brokers()`. Sort ascending by
   `node_id` into `bs = [b0, b1, …, bk-1]`.
2. If `R > k`, return `INVALID_REPLICATION_FACTOR (38)`.
3. For each partition `p` in `0..P`:
   - `replicas = [bs[(p + i) % k] for i in 0..R]`
   - `leader = replicas[0]`
4. Build `V1Topic + V1Partition[P]` records (with the assignment
   baked in). Submit via `controller.submit_change(...)`.

Round-robin's per-partition phase offset means partition 0 leads on
`bs[0]`, partition 1 leads on `bs[1]`, and so on — preventing all
leaders from converging on one broker.

The assignment is **deterministic given the brokers set at submit
time**, which is safe in slice 8 because membership doesn't change at
runtime (dynamic membership is a slice-7 follow-up).

### `replicator_supervisor`

```rust
pub(crate) struct ReplicatorSupervisor {
    node_id: NodeId,
    controller: Arc<ControllerHandle>,
    partitions: Arc<DashMap<(String, i32), Arc<Partition>>>,
    log_dir: PathBuf,
    log_config: LogConfig,
    client_id: String,
    tasks: Arc<DashMap<(String, i32), CancellationToken>>,
    shutdown: CancellationToken,
}

impl ReplicatorSupervisor {
    pub(crate) fn spawn(...) -> JoinHandle<()>;
    async fn run(self);
    async fn reconcile(&self, image: &MetadataImage);
}
```

`run` loops on `controller.watch_image().changed()` and calls
`reconcile` on every metadata update. `reconcile`:

1. **Desired set:** every `(topic, partition)` where `self.node_id` is
   in `replicas` AND `leader != self.node_id`.
2. **Cancel removed:** for every running task whose key isn't in
   `desired`, cancel its `CancellationToken` and remove from `tasks`.
3. **Spawn new:** for every key in `desired` not already in `tasks`,
   resolve the leader's `host:port` from `image.broker(leader_node_id)`,
   create a new `CancellationToken`, and `tokio::spawn` a
   `replicator::run` future.

If a leader's `BrokerRegistrationRecord` isn't yet in the image (race
between `V1Partition` and `V1BrokerRegistration` apply order), the
supervisor logs at WARN and skips — the next apply triggers another
reconcile.

### `replicator::run` — per-partition fetch loop

```rust
pub(crate) struct Config {
    pub node_id: NodeId,
    pub topic: String,
    pub partition: i32,
    pub leader_addr: String,
    pub partitions: Arc<DashMap<(String, i32), Arc<Partition>>>,
    pub log_dir: PathBuf,
    pub log_config: LogConfig,
    pub client_id: String,
    pub shutdown: CancellationToken,
}

pub(crate) async fn run(cfg: Config);
```

Pseudocode:

```rust
let mut conn = connect_with_backoff(&cfg).await?;
let part = open_or_create_local_partition(&cfg)?;

loop {
    let fetch_offset = part.log_end_offset();
    let req = FetchRequest {
        replica_id: i32::from(cfg.node_id),
        max_wait_ms: 500,
        min_bytes: 1,
        max_bytes: 1 << 20,
        topics: vec![FetchTopic {
            topic: cfg.topic.clone(),
            partitions: vec![FetchPartition {
                partition: cfg.partition,
                fetch_offset,
                max_bytes: 1 << 20,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    tokio::select! {
        () = cfg.shutdown.cancelled() => return,
        resp = conn.send(req) => {
            match resp {
                Ok(r) => apply_response(r, &part, &cfg).await,
                Err(_) => conn = connect_with_backoff(&cfg).await?,
            }
        }
    }
}
```

**Error handling inside the loop:**

| Per-partition `error_code` | Action |
|---|---|
| 0 (`NONE`) | Append every returned batch to local log; loop. |
| 1 (`OFFSET_OUT_OF_RANGE`) | Truncate local log to 0; re-fetch from `fetch_offset=0` next round. Log at WARN. |
| 3 (`UNKNOWN_TOPIC_OR_PARTITION`) | Leader hasn't materialized this partition yet (CreateTopics-vs-replicator race). Sleep 100 ms; retry. |
| 6 (`NOT_LEADER_FOR_PARTITION`) | Stop the task; the next supervisor reconcile re-evaluates. Log at WARN. |
| Transport error | Reconnect with exponential backoff (capped at 5 s). Cancellation aborts the sleep. |

### Leader-side `Fetch` handler

The slice-4 handler at `handlers/fetch.rs` gains one branch:

```rust
let is_follower_fetch = req.replica_id >= 0;
// ... existing partition lookup, log.read() ...
if !is_follower_fetch {
    // Consumer: filter to HW (no-op in slice 8 — log_end_offset == HW
    // until slice-8-followup adds HW tracking).
}
// Encode response with the (filtered or unfiltered) batches.
```

In slice 8 the filter is a no-op for both forks. The branch is in
place for slice-8-followup.

## Data flow — happy path

```
JVM admin client → broker N1: kafka-topics --create
                                 --partitions 3 --replication-factor 3
                                       │
                       CreateTopics handler runs on N1:
                         1. brokers() = [N1, N2, N3]
                         2. P0: replicas=[N1,N2,N3], leader=N1
                            P1: replicas=[N2,N3,N1], leader=N2
                            P2: replicas=[N3,N1,N2], leader=N3
                         3. submit_change(V1Topic, V1Partition×3)
                                       │
                       Controller commits + applies on N1, N2, N3.
                                       │
                       N2 + N3 supervisors observe new
                       metadata. They each reconcile:
                         · N2's desired follower set: {(T,0), (T,2)}
                         · N3's desired follower set: {(T,0), (T,1)}
                       Two replicator tasks spawn on each.
                                       │
                       Each task loops Fetch against the
                       partition's leader broker. Records
                       produced to a partition replicate
                       to its followers within ~500 ms
                       (Fetch's max_wait_ms).
```

## Errors

`crabka-broker::BrokerError` gains:

```rust
    #[error("replication: {0}")]
    Replication(String),
```

This is used for diagnostic logging only and is never returned through
the wire to clients. Clients only see standard error codes (0, 1, 3,
6, 38) — no new wire surface.

## Observability

- `tracing::info_span!("replicator", topic, partition, leader_node_id)`
  wraps the per-partition fetch loop.
- Structured events (via `tracing`; OTLP export deferred to slice 11):
  - `replicator.started` — task spawned, reconnected, or recovered.
  - `replicator.appended` — `n_batches`, `n_records`, `new_log_end_offset`.
  - `replicator.out_of_range` — at WARN, with old and new
    `log_start_offset`.
  - `replicator.not_leader` — at WARN.
  - `replicator.stopped` — task cancelled by supervisor.
- Periodic per-partition metric (5-second cadence while active):
  `replicator.lag = leader_log_end_offset - follower_log_end_offset`.

## Testing

### Layer 1 — replica-assignment unit tests

`crates/broker/src/handlers/create_topics.rs::tests`:

- `round_robin_three_brokers_three_partitions_rf_three` — 3 brokers,
  3 partitions × rf=3 → each broker leads exactly one partition; each
  partition lists all 3 brokers as replicas.
- `round_robin_offset_per_partition` — partitions 0/1/2 lead on
  brokers 1/2/3 respectively.
- `replication_factor_too_high_rejects` — rf=5 with 3 brokers →
  `INVALID_REPLICATION_FACTOR` (38).
- `replication_factor_one_preserves_slice7_shape` — rf=1 keeps
  `replicas = [leader]` (single-broker case).

### Layer 2 — supervisor reconcile

`crates/broker/src/replicator_supervisor.rs::tests`:

- `reconcile_spawns_for_assigned_follower_partition`.
- `reconcile_cancels_removed`.
- `reconcile_idempotent_unchanged`.
- `reconcile_skips_self_leader`.

These use a mock supervisor that takes a `watch::Sender<Arc<MetadataImage>>`
directly, skipping real broker startup. The mock spawns a no-op
"replicator stub" that just records a `(topic, partition, leader)`
tuple — the tests assert on the recorded set.

### Layer 3 — in-process 3-node replication

`crates/broker/tests/replication.rs`:

- `replication_factor_three_propagates_to_all_followers` — 3-broker
  cluster, `partitions=1, rf=3`. Produce 20 records via a
  `crabka-client-producer` aimed at the partition leader. Poll all 3
  brokers' on-disk `Log::log_end_offset()` until they match (10 s
  deadline). Read records via each broker's local `crabka-log` API;
  assert byte-equal.

- `out_of_range_truncates_and_recovers` — same setup. Produce 50
  records. Cancel broker 3's supervisor (via a `#[cfg(test)]`
  accessor on `BrokerHandle`), simulate "fell behind past retention"
  by directly truncating broker 3's local partition log + advancing
  the leader's `log_start_offset` past it. Re-enable the supervisor.
  Assert broker 3 converges + emitted an `out_of_range` event.

Both tests are gated `#[cfg(not(target_os = "windows"))]`, matching
slice 7's `quorum.rs` rationale (openraft + hand-rolled wire timing on
the hosted Windows runner).

### Layer 4 — JVM acceptance

`crates/broker/tests/jvm_acceptance.rs::three_node_replication_byte_compare`
(`#[ignore = "requires Docker"]`):

1. 3-broker Crabka cluster on the slice-7 fixed-port pattern (client
   9192/9292/9392, controller 9193/9293/9393).
2. `kafka-topics --create --topic <T> --partitions 1
   --replication-factor 3 --bootstrap-server <node-1>`.
3. Wait for metadata to converge (poll `kafka-topics --describe`
   until `Leader: N`, `Replicas: 1,2,3`, `Isr: 1,2,3`).
4. `kafka-console-producer` writes 100 records via `<node-2>`. The
   JVM AdminClient transparently follows the partition leader.
5. Wait for follower replication to catch up — poll
   `kafka-topics --describe` lag information.
6. For each broker, copy its `<TOPIC>-0/00...000.log` file to a host
   temp path, then run `kafka-dump-log --files <copy>` via a Docker
   tool container; capture stdout.
7. Assert all three dumps produce identical record-by-record output.

The `dump-log` text output is human-readable per-record metadata +
key/value previews; comparing it is robust to minor encoding
artifacts that a raw byte-diff would catch erroneously.

## Acceptance gate

Slice 8 is shippable when:

1. `cargo test --workspace -- --include-ignored` passes locally and on
   Linux + macOS CI runners.
2. `cargo clippy --workspace --all-targets -- -D warnings` clean.
3. `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps` clean.
4. `replication.rs::replication_factor_three_propagates_to_all_followers`
   green.
5. `replication.rs::out_of_range_truncates_and_recovers` green.
6. `jvm_acceptance.rs::three_node_replication_byte_compare` green on
   CI's Docker-enabled Ubuntu runner.
7. All slice-1..7 tests still pass (including slice-7's `quorum.rs`
   retry wrapper).
8. Broker startup with `replication_factor=1` is byte-for-byte
   equivalent to slice 7 — no replication tasks spawn, no behavior
   change for single-broker deployments.

## Risks and mitigations

- **Per-partition fan-out.** 1000 partitions × 2 followers = 2000
  long-running tokio tasks. Tokio handles this fine, but per-task
  connections would be wasteful. *Mitigation:* the supervisor caches
  `Connection` per leader (`DashMap<NodeId, Arc<Connection>>`); each
  per-partition task shares the per-leader connection.
  Batched ReplicaFetcherThread (Apache Kafka's approach) is a slice
  follow-up optimization, not a slice-8 blocker.
- **Replicator races CreateTopics on the leader.** Between
  `V1Partition` apply and the leader broker materializing the on-disk
  partition, a follower's `Fetch` can hit
  `UNKNOWN_TOPIC_OR_PARTITION (3)`. *Mitigation:* retry on code 3
  with 100 ms backoff. Cleanly resolves once the leader's
  `CreateTopics` handler finishes on-disk materialization (typically
  < 100 ms after Raft commit).
- **`OFFSET_OUT_OF_RANGE` during normal operation.** Slice 8 doesn't
  implement retention-driven truncation, so this is rare in
  practice. *Mitigation:* standard Apache Kafka behavior — follower
  truncates to 0 and re-fetches. Tested in
  `out_of_range_truncates_and_recovers`.
- **Round-robin determinism vs broker churn.** `MetadataImage::brokers()`
  is keyed by `node_id` so it's deterministic. New brokers joining
  mid-slice-8 don't trigger rebalancing of *existing* partitions —
  they only appear in *future* `CreateTopics` assignments.
  Rebalancing is a slice follow-up.

## Next step after this spec

Invoke `superpowers:writing-plans` once the spec is committed and
approved.
