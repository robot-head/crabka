# Slice 64a follow-up — KIP-848 persistence write path + JVM-client integration depth

**Status:** design
**Date:** 2026-05-28
**Roadmap:** follow-up to slice 64a (KIP-848 next-gen consumer group protocol foundations, merged in PR #260). Closes the two gaps slice 64a deliberately left open.

## Goal

Make `kafka-clients 4.0` consumers with `group.protocol=consumer` drive Crabka's KIP-848 path end-to-end, and durably persist next-gen group state to `__consumer_offsets`. Concretely: land all four `jvm_kip848_*` JVM-acceptance tests in `crates/broker/tests/jvm_consumer_group_next_gen.rs` and the two bootstrap-replay tests in `crates/broker/tests/consumer_group_next_gen_persistence.rs` as **passing**, not `#[ignore]`d.

## Non-goals

- Rack-aware `UniformAssignor` (defer to 64b).
- Custom server-side assignor plugin point (64c).
- Group migration policy classic → next-gen (64d).
- Share groups KIP-932.
- Admin `kafka-consumer-groups --delete` for next-gen groups beyond what the foundations cover (group delete from inside the actor remains follow-up).
- Dynamic kill-switch flips at runtime — `group.coordinator.rebalance.protocols` is start-time only, as in foundations.

## Architecture

### Two coordinated changes

**A. Feature-level advertisement (KIP-584).** `ApiVersionsResponse` carries a `group.version=1` entry in both `supported_features` and `finalized_features`, gated on the broker's
`next_gen_consumer_group.next_gen_enabled()`. kafka-clients 4.0 consults
`finalized_features` to decide whether `group.protocol=consumer` engages KIP-848 or falls
back to classic. Without `group.version >= 1`, the client falls back to classic, then
collides with our group-type lock — which is exactly what made the JVM tests fail in
PR #260.

**B. Actor → `__consumer_offsets` write path.** Every `GroupState` mutation in
`group_actor.rs::handle_heartbeat` (and the session-tick path) pairs its in-memory
change with a write of the affected v3/v5/v6/v7/v8 records to `__consumer_offsets`
partition 0. Records produced by a single mutation are bundled into one `RecordBatch`
and sent through the partition's `writer_tx` mpsc — the same pattern
`handlers/offset_commit.rs::append_batch` uses for classic offset commits. Writes
happen *before* the heartbeat reply, matching the no-torn-transitions guarantee.

### File layout

**Create:**
- `crates/broker/src/coordinator/next_gen/offsets_log.rs` — `OffsetsLog` trait,
  `ProductionOffsetsLog` (wraps `Arc<Partition>`), `fake::InMemoryOffsetsLog`.

**Modify:**
- `crates/broker/src/coordinator/next_gen/group_actor.rs` — every mutation in
  `handle_heartbeat`, `apply_seed`, and the session-tick branch of `actor_loop`
  emits records and awaits `OffsetsLog::append` before replying. New constructor
  parameter `offsets_log: Arc<dyn OffsetsLog>`.
- `crates/broker/src/coordinator/next_gen/mod.rs` — `NextGenCoordinator`
  constructs `ProductionOffsetsLog` from broker's partitions map and threads
  `Arc<dyn OffsetsLog>` through `get_or_create` / `GroupActorHandle::spawn`.
  Adds a persistent `seeds_cache: DashMap<String, GroupSeed>` that mirrors the
  actor's last-good state for fast respawn on crash.
- `crates/broker/src/handlers/api_versions.rs` — populate `supported_features`
  and `finalized_features` with `group.version=1` when next-gen is enabled.
- `.github/workflows/ci.yml` — restore `--test jvm_consumer_group_next_gen` to
  the `broker-jvm-acceptance` cargo-llvm-cov invocation.
- `crates/broker/tests/consumer_group_next_gen_persistence.rs` — remove
  `#[ignore]` from both tests.
- `crates/broker/tests/jvm_consumer_group_next_gen.rs` — remove `#[ignore]`
  from all four tests.
- `STATUS.md` — append slice entry; drop the two follow-up bullets from
  slice 64a's out-of-scope list.

### OffsetsLog trait surface

```rust
#[async_trait]
pub trait OffsetsLog: Send + Sync + std::fmt::Debug {
    async fn append(&self, batch: RecordBatch) -> Result<(), BrokerError>;
}

#[derive(Debug)]
pub struct ProductionOffsetsLog {
    partition: Arc<Partition>,
}

impl ProductionOffsetsLog {
    pub fn from_partitions(
        partitions: &Arc<DashMap<(String, i32), Arc<Partition>>>,
    ) -> Option<Self> {
        partitions
            .get(&("__consumer_offsets".to_string(), 0))
            .map(|e| Self { partition: e.value().clone() })
    }
}

#[async_trait]
impl OffsetsLog for ProductionOffsetsLog {
    async fn append(&self, batch: RecordBatch) -> Result<(), BrokerError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.partition
            .writer_tx
            .send(WriterMessage::Produce(ProduceJob { batch, ack: ack_tx }))
            .await
            .map_err(|_| BrokerError::Internal("offsets partition writer dropped"))?;
        match ack_rx.await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(BrokerError::Internal("offsets writer ack channel closed")),
        }
    }
}

pub mod fake {
    use super::*;
    use tokio::sync::Mutex;

    #[derive(Debug, Default)]
    pub struct InMemoryOffsetsLog {
        pub appended: Mutex<Vec<RecordBatch>>,
    }
    #[async_trait]
    impl OffsetsLog for InMemoryOffsetsLog {
        async fn append(&self, batch: RecordBatch) -> Result<(), BrokerError> {
            self.appended.lock().await.push(batch);
            Ok(())
        }
    }
}
```

### Per-mutation record catalog

| Mutation site in `handle_heartbeat` / actor tick | Records emitted (single batch) |
|--------------------------------------------------|-------------------------------|
| Member leave (`member_epoch == -1`) | k5/k7/k8 tombstones for member; k3 with bumped epoch (skip k3 if group becomes empty *and* the spec says no-op — but we always bump for clarity). |
| First-join (`member_epoch == 0`, empty `member_id`) | k5 (member metadata) + k3 (epoch bump) + k6 (target metadata, new assignment epoch) + k7 (per-member target) + k8 (per-member current). |
| Subscription change (steady-state heartbeat, new `subscribed_topic_names`) | k5 (updated member metadata) + k3 (epoch bump) + reconciliation outputs (k6 + k7 + k8 for affected members). |
| Reconciliation triggered by metadata change | k6 + k7 (per member whose target changed) + k8 (per member whose current changed). |
| Acknowledgement (steady-state, `owned_partitions` finally match target) | k8 for just that member (state transition Stable; possibly epoch advance). |
| Session-timeout eviction (tick path) | Per evicted member: k5/k7/k8 tombstones; then k3 (epoch bump); then reconciliation outputs for survivors. |

Invariants:
- k3 written exactly when `state.group_epoch` increments.
- k6 written only when `state.target.epoch` changes.
- k7 written per member whose target assignment differs from the previous target
  (members with unchanged targets emit no k7).
- k8 written per member on every observable transition.

### Actor failure mode (Approach 1)

When `OffsetsLog::append` returns `Err`:
1. `handle_heartbeat` propagates the error up `actor_loop`.
2. `actor_loop` logs at WARN with `group_id` and exits.
3. The `JoinHandle` finishes; the actor's `tx` `mpsc::Sender` closes.
4. Next request that calls `NextGenCoordinator::get_or_create(group_id)` checks
   `handle.tx.is_closed()`; if so, removes the entry and spawns a fresh actor.
5. Fresh actor reads its seed from `NextGenCoordinator::seeds_cache`. The cache
   mirrors the *last successfully-written* state (each successful actor write
   also updates the cache via a coordinator-owned snapshot fn).
6. Caller of the failed write sees `COORDINATOR_LOAD_IN_PROGRESS`; client retries
   the heartbeat; new actor handles it cleanly.

This trades extra task churn (one new tokio task per failure) for zero rollback
code in the hot path. Re-uses the existing seed-replay machinery.

### `seeds_cache` on NextGenCoordinator

New field:

```rust
pub seeds_cache: Arc<DashMap<String, GroupSeed>>,
```

Updated by the actor immediately after every successful `OffsetsLog::append`:
the actor sends a `GroupActorMessage::CacheSnapshot { seed }` self-message (or
the coordinator exposes an `update_cache(group_id, seed)` method that the actor
calls directly via an `Arc<NextGenCoordinator>` it now holds). The latter is
simpler — added as the recommended path.

The existing `seeds` field (drained by `finalize_bootstrap`) is unchanged in
purpose; `seeds_cache` is a separate, persistent map.

### Feature-level advertisement details

In `handlers/api_versions.rs::handle`:

```rust
let next_gen_on = broker.config.next_gen_consumer_group.next_gen_enabled();
let supported_features = if next_gen_on {
    vec![SupportedFeatureKey {
        name: "group.version".into(),
        min_version: 1,
        max_version: 1,
        ..Default::default()
    }]
} else {
    vec![]
};
let finalized_features = if next_gen_on {
    vec![FinalizedFeatureKey {
        name: "group.version".into(),
        min_version_level: 1,
        max_version_level: 1,
        ..Default::default()
    }]
} else {
    vec![]
};
let finalized_features_epoch: i64 = if next_gen_on { 0 } else { -1 };
let resp = ApiVersionsResponse {
    error_code: codes::NONE,
    api_keys: supported_apis(),
    throttle_time_ms: 0,
    supported_features,
    finalized_features,
    finalized_features_epoch,
    ..Default::default()
};
```

The implementer verifies the exact codegen field names on
`SupportedFeatureKey` and `FinalizedFeatureKey` before writing
(`min_version`/`max_version` vs `min_version_level`/`max_version_level`).

## Error handling & edge cases

| Failure | Handling |
|---------|----------|
| `OffsetsLog::append` returns `Err` | Actor exits; respawn from `seeds_cache` on next heartbeat. Client sees one `COORDINATOR_LOAD_IN_PROGRESS`; retry succeeds. |
| `__consumer_offsets` partition not yet present | `ProductionOffsetsLog::from_partitions` returns `None`; coordinator falls back to in-memory only. `/readyz` already gates external traffic until coordinator load completes. |
| Actor panic | Same recovery path — closed `tx`, respawn from cache. |
| Cache miss on respawn | Bootstrap replay populates cache before first actor spawn, so this shouldn't happen. If it does, actor handles next heartbeat as a fresh join — recoverable but visible as a transient. |
| Classic client connects with `group.version=1` advertised | Classic clients don't consult features; ignored. No regression. |

## Testing

### Unit (new + extended)

- `next_gen/offsets_log.rs`: 3 tests.
  - `InMemoryOffsetsLog::append` records batches in order.
  - `ProductionOffsetsLog::from_partitions` returns `None` for missing partition.
  - `ProductionOffsetsLog::from_partitions` returns `Some` for present partition (mock partitions map).
- `next_gen/group_actor.rs`: 6 new tests (in addition to existing coverage).
  - First-join emits one batch with k5 + k3 + k6 + k7 + k8.
  - Steady-state unchanged heartbeat emits zero batches.
  - Member leave emits batch with k5/k7/k8 tombstones + k3.
  - Session-timeout eviction emits batch with tombstones + k3.
  - `apply_seed` from cache works without log read.
  - Actor exits cleanly on `OffsetsLog::append` Err (using a fake that errs once).

### Integration

- `crates/broker/tests/consumer_group_next_gen_persistence.rs` — remove
  `#[ignore]`; both tests must pass.

### JVM acceptance

- `crates/broker/tests/jvm_consumer_group_next_gen.rs` — remove `#[ignore]`
  from all 4 tests. All must pass against `mirror.gcr.io/apache/kafka:4.0.0`.

### CI

- Restore `--test jvm_consumer_group_next_gen` to the `broker-jvm-acceptance`
  job's `cargo llvm-cov` invocation in `.github/workflows/ci.yml`.

## Acceptance gates

1. `cargo test --workspace` green.
2. `cargo test --workspace -- --include-ignored` adds the 6 previously-ignored
   tests; all green.
3. JVM acceptance binary passes against `mirror.gcr.io/apache/kafka:4.0.0` with
   `group.protocol=consumer`.
4. `cargo clippy --workspace --all-targets -- -D warnings` clean.
5. STATUS.md updated; the two follow-up bullets dropped from slice 64a's
   out-of-scope list.
