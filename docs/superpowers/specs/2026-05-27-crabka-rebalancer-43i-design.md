# Crabka rebalancer 43i — State migration to internal Crabka topic

**Date:** 2026-05-27
**Status:** Slice design. Follows slice 43h (scrape-target discovery via
Metadata). Part of the rebalancer roadmap
(`docs/superpowers/specs/2026-05-17-crabka-rebalancer-roadmap-design.md`).

## Why this exists

Today the rebalancer's executor persists its in-flight state to
`{data_dir}/in_flight.json`. Pod death loses local disk; restart on a
different node loses state entirely. The roadmap flagged this as the
gating dependency for multi-replica HA: a `Lease`-based leader-election
deployment can't run safely while state lives on each pod's filesystem
(two replicas would corrupt each other's view).

Migrating to a compacted internal topic
(`__crabka_rebalancer_state`) follows the standard Kafka pattern
(`__consumer_offsets`, `__transaction_state`) and is what Cruise
Control does with its sample-store and metric-data topics. State
survives pod restarts; the next slice (HA via `Lease`) becomes
viable.

## Goal

Replace `{data_dir}/in_flight.json` writes/reads/deletes with
produces/consumes against a single-partition compacted topic on the
Crabka cluster the rebalancer is administering. On startup, the
rebalancer loads its state from the topic in a background task;
state-dependent endpoints return a Kafka-idiomatic "loading" status
until the load completes (mirrors broker coordinator-load semantics
and Cruise Control's `LoadMonitor`).

## Non-goals

- **Multi-replica HA / leader election.** Separate slice (43j),
  blocked on this one.
- **Migration from existing JSON file to topic.** Greenfield, no
  users. The file path is removed; the JSON serde stays because the
  topic wire format reuses it.
- **Sharing the state topic across multiple rebalancer clusters.**
  One rebalancer cluster targets one Crabka cluster and gets one
  state topic. Discriminating by `--state-topic-name` is the escape
  hatch if a user really wants two rebalancer deployments against the
  same broker cluster.
- **Storing proposal history or anomaly data on the topic.** Out of
  scope; those have separate persistence (the `data_dir` still holds
  the anomaly ring buffer).

## Architecture

### New module: `crates/rebalancer/src/state_topic/`

```
state_topic/
  mod.rs       — `StateTopic` handle: write / delete / loaded / is_loaded
  loader.rs    — `StateTopicLoader`: one-shot async consume-from-beginning
  producer.rs  — wraps `crabka-client-producer` for the two write paths
```

**`StateTopic`** (`mod.rs`):

```rust
pub struct StateTopic {
    producer: StateProducer,
    loaded: Arc<ArcSwap<Option<InFlightFile>>>,
    is_loaded: Arc<AtomicBool>,
}

impl StateTopic {
    pub async fn write(&self, f: &InFlightFile) -> Result<(), StateTopicError> { … }
    pub async fn delete(&self) -> Result<(), StateTopicError> { … }
    pub fn loaded(&self) -> Option<InFlightFile> { … }   // clone the current value
    pub fn is_loaded(&self) -> bool { … }
}
```

`loaded` is updated by the loader as it consumes; once `is_loaded =
true`, further updates come only from the executor's own writes
(the loader has reached the end of the log).

**`StateTopicLoader`** (`loader.rs`): a single async task that:
1. Opens a consumer on the topic, partition 0, from offset 0.
2. For each record (newest record per key wins after compaction; we
   only use one key so the last record is the truth):
   - Tombstone (null value) → store `None` in `loaded`.
   - Valid JSON → deserialize `InFlightFile`, store `Some` in
     `loaded`.
   - Malformed → `WARN`, skip.
3. When the consumer reaches end-of-log (or stalls for
   `--state-load-timeout-secs`), set `is_loaded = true`.
4. Exit the task.

**`StateProducer`** (`producer.rs`): thin wrapper over the existing
`crabka-client-producer::Producer` configured to send to a single
partition with `acks=all`.

### Topic config

Created via AdminClient `CreateTopics` on startup if missing.

| Setting | Value | Why |
|---|---|---|
| `cleanup.policy` | `compact` | Latest record per key wins; no time-based eviction. |
| `min.cleanable.dirty.ratio` | `0.01` | Aggressive compaction; topic stays tiny. |
| `segment.ms` | `60000` | Force segment roll every minute so the active segment can become compactable quickly. |
| `partitions` | `1` | Single writer, ordered writes. |
| `replication.factor` | `--state-topic-replication` (default `3`) | Configurable; capped at broker count at create time. |

### Executor integration

`crates/rebalancer/src/executor/mod.rs`:

- The three existing call sites that use `InFlightFile::write/load/delete`
  on `self.state.config.data_dir` switch to
  `self.state_topic.write/loaded/delete`.
- `ExecutorState::config.data_dir` is no longer consulted by the
  executor (the anomaly store still uses `data_dir` for its ring
  buffer; that's unchanged).
- The `InFlightFile` struct stays exactly as-is — the topic wire
  format is the same JSON the file used.

### API gating

API endpoints check `state_topic.is_loaded()` per the Kafka
coordinator-load semantics:

| Endpoint | Behavior when not loaded |
|---|---|
| `POST /api/v1/proposals/{id}/execute` | `503` with body `{"status":"loading","message":"state topic not yet loaded"}`. Matches Cruise Control's behavior on `LoadMonitor` cold start. |
| `GET  /api/v1/proposals/{id}` | Returns the in-memory proposal; if the proposal has a persisted-state dependency that isn't loaded yet, status field includes `loading_pending: true`. |
| `GET  /api/v1/state` | Unaffected — the ingester's cluster snapshot is independent of persisted executor state. |
| `POST /api/v1/proposals` | Unaffected — compute-only, no persistence. |
| `GET  /healthz` | Always `200` (process alive). |
| `GET  /readyz` | `503` until `is_loaded == true`. K8s readiness probe gates Service routing. |

### Startup flow

```
binary main()
  │
  ├── parse args, build admin client, producer, ingester (existing)
  ├── AdminClient::create_topic("__crabka_rebalancer_state", configs) (idempotent)
  ├── StateTopic::new(producer)
  ├── spawn StateTopicLoader { state_topic.clone() }.run()
  ├── spawn ingester.run()
  ├── spawn scraper.run()
  ├── spawn axum API server (reads state_topic.is_loaded() in handlers)
  └── wait for shutdown
```

The API + executor start in parallel with the loader. Endpoints
self-gate via `is_loaded()`.

### Configuration

New CLI flags / env vars (mirror existing convention):

| Flag | Env | Default | Purpose |
|---|---|---|---|
| `--state-topic-name` | `CRABKA_REBALANCER_STATE_TOPIC` | `__crabka_rebalancer_state` | Name override (rarely needed). |
| `--state-topic-replication` | `CRABKA_REBALANCER_STATE_TOPIC_REPLICATION` | `3` | Replication factor at create time. |
| `--state-load-timeout-secs` | `CRABKA_REBALANCER_STATE_LOAD_TIMEOUT_SECS` | `60` | Soft deadline: WARN if loading takes longer; `/readyz` stays `503` indefinitely until success. |

`--data-dir` stays — anomaly store still uses it. Doc comment
clarifies it's anomaly-only post-43i.

## Error handling

| Failure | Surface |
|---|---|
| AdminClient cannot create topic (auth, broker unreachable) | Fatal at startup; process exits with non-zero; operator restarts. |
| Loader cannot connect to broker | Retries with backoff; `/readyz` stays `503` until the consumer connects and completes a tail-read. |
| Loader receives a record with malformed JSON | `WARN` + skip. If the latest non-tombstone record is malformed, `loaded` stays at the prior valid value or `None`. |
| Loader exceeds `--state-load-timeout-secs` | `WARN`; loader keeps retrying; `is_loaded` stays `false`. |
| Producer write fails (executor in mid-phase) | Returned as `PhaseError`; executor surfaces the same way the JSON-file path did. The retry semantics for the operator-facing CRD don't change. |
| Tombstone delete fails | Same as above; the executor surfaces the failure and the topic retains the prior in-flight record (resume-on-restart still works). |
| Two records arrive out-of-order on the consumer (shouldn't happen with single producer + single partition) | Last one wins. |

## Testing

- **Unit (`state_topic/mod.rs`, `loader.rs`, `producer.rs`)**:
  fake producer/consumer; verify:
  - write → loaded round-trip.
  - delete → `loaded == None`.
  - malformed record is skipped.
  - load-then-write transitions `is_loaded` correctly.
- **Integration (`crates/rebalancer/tests/state_topic.rs`, new)**:
  testcontainers-backed Crabka broker. End-to-end:
  - write a phase, restart the `StateTopic` handle, observe the
    same phase after the loader catches up.
  - delete via tombstone, restart, observe `loaded == None`.
  - topic-auto-create with the right configs.
- **Executor unit tests** (in
  `crates/rebalancer/src/executor/state.rs` and
  `executor/mod.rs`): replace tempdir + JSON file fixtures with an
  in-memory `StateTopic` test double. The state-machine assertions
  are unchanged; only the persistence mechanism swaps.

## Implementation order (informal)

1. New module skeleton: `StateTopic`, `StateTopicLoader`,
   `StateProducer` (no functionality yet — wires up the types).
2. Topic-create on startup via AdminClient + new CLI flags.
3. `StateTopic::write/delete/loaded/is_loaded` + producer wiring.
4. Loader implementation + integration test for round-trip.
5. Swap executor call sites from `InFlightFile::write/load/delete`
   to `StateTopic::write/loaded/delete`. Update executor tests to
   use the test double.
6. API gating: `/readyz` + `execute` endpoint check `is_loaded()`.

## Risk register

- **Consumer reaches end-of-log detection.** A naive consumer can't
  know it's at the latest record without polling. Use a fixed quiet
  period — 5 consecutive 100 ms poll cycles with no new records
  (~500 ms total). Cheap, deterministic, no extra protocol surface
  needed. The state topic is one key so this almost never matters
  beyond the first poll.
- **Topic existence race on first cluster boot.** AdminClient
  create is idempotent (`if-not-exists` semantics) but if two
  rebalancer instances start simultaneously (e.g., during a rolling
  restart), both try to create. K8s rolling restarts serialize
  pod startup so this is fine in practice; the slice doesn't
  introduce HA so there's only one rebalancer at a time anyway.
- **Producer `acks=all` blocks on under-replicated topic.** During
  bootstrap, the state topic may not yet have its replicas placed.
  The replication factor is capped at broker count at create
  time, so this only fails when the broker count drops below 3
  after the topic was created. Operators who care can re-create
  the topic with lower replication.
- **State load is non-trivially long on large compacted topics.**
  Our state topic has one key, so compacted size is bounded to ~1
  record. Load is effectively constant-time. No issue at any
  cluster scale.
