# KIP-320 — Detect & handle log truncation (leader epoch in the Fetch path)

Date: 2026-06-02
Status: Approved (brainstorming) — pending spec review

## Context

KIP-320 makes log truncation detectable in-band: a fetcher (replica follower or
consumer) tells the leader the leader epoch of its last fetched record, and the
leader, on divergence, returns the epoch/offset the fetcher must truncate to —
instead of silently serving records that the fetcher then appends on top of a
divergent suffix.

Crabka is **~40% there**. Exploration confirmed what already exists:

- **Protocol structs carry every field.** `FetchRequest.FetchPartition` has
  `current_leader_epoch` (v9+) and `last_fetched_epoch` (v12+);
  `FetchResponse.PartitionData` has `diverging_epoch: EpochEndOffset{epoch,end_offset}`,
  `current_leader: LeaderIdAndEpoch`, and `snapshot_id` (all v12+, tagged)
  (`crates/protocol/generated/FetchRequest.owned.rs`,
  `crates/protocol/generated/FetchResponse.owned.rs`).
- **Leader-epoch checkpoint cache** (KIP-101/279, ✅):
  `LeaderEpochCheckpoint` with `end_offset_for_epoch(epoch, leo) -> i64`
  (`crates/log/src/leader_epoch_checkpoint.rs:118`).
- **OffsetForLeaderEpoch leader handler** fully implemented, advertised v2–v4
  (`crates/broker/src/handlers/offset_for_leader_epoch.rs`).
- **KIP-101 epoch fencing** in the Fetch handler: `current_leader_epoch`
  mismatch → `FENCED_LEADER_EPOCH` / `UNKNOWN_LEADER_EPOCH`
  (`crates/broker/src/handlers/fetch.rs:272`).
- **Reactive follower recovery**: on a fence error the replicator issues an
  `OffsetForLeaderEpoch` RPC and truncates
  (`crates/broker/src/replicator.rs:238`, `:384`, `:438`).

What is missing:

- The leader **never computes or returns `diverging_epoch`** and **ignores
  `last_fetched_epoch`**.
- The follower **never sends `last_fetched_epoch`** and **never reads
  `diverging_epoch`**.
- `current_leader` is **never populated** on any response/error.
- The native Rust consumer has **zero leader-epoch awareness**: it leaves both
  epoch fields default, never tracks the epoch of consumed records, never issues
  `OffsetForLeaderEpoch`, and its poll loop **swallows per-partition error
  codes** (it skips any partition with no `records`, masking
  `OFFSET_OUT_OF_RANGE` and fence errors).

### Why this is correctness, not an optimization

At **Fetch v12+** — the version JVM brokers and clients negotiate —
`diverging_epoch` is the *primary* truncation mechanism; the
`OffsetForLeaderEpoch` RPC path is only a fallback for older protocol versions.
Crabka advertises Fetch v4–v18 (`crates/broker/src/api_catalog.rs`). So in a
**mixed cluster**:

- a JVM follower fetching from a Crabka **leader** never receives a
  `diverging_epoch`, and
- a Crabka follower fetching from a JVM **leader** never sends
  `last_fetched_epoch`, so the JVM leader cannot signal divergence,

and in either direction a divergent log suffix is appended on top of, silently —
exactly the corruption KIP-320 exists to prevent. Completing KIP-320 is a
mixed-cluster correctness fix.

## Goal & scope

Complete KIP-320 end to end: the in-band `diverging_epoch` mechanism on both the
leader and the Crabka follower, and a **Java-faithful, proactive** truncation
detector in the native consumer (position-validation state machine +
`OffsetForLeaderEpoch`), validated against a real JVM mixed cluster.

**In scope:**

1. **Leader-epoch cache** — `epoch_and_offset_for(epoch, leo) -> (i32, i64)`
   returning the `(found_epoch, end_offset)` pair.
2. **Leader Fetch path** — divergence detection + `diverging_epoch` population;
   `current_leader` on fence / not-leader errors. v12+ only.
3. **Follower replicator** — send `last_fetched_epoch`; truncate in-band on
   `diverging_epoch`; keep the reactive fence path for leadership changes.
4. **Native consumer (Approach B)** — `FetchPosition` model with epoch + a
   `Fetchable`/`AwaitValidation` lifecycle; metadata-driven epoch tracking;
   proactive `OffsetForLeaderEpoch` validate-positions; error-first poll loop;
   `auto.offset.reset` handling incl. a new `None` policy that raises
   `LogTruncation`; `committed_leader_epoch` round-trip through commit/fetch.
5. **Tests** — deterministic Rust suite + a **full mixed JVM+Crabka** scenario
   that induces real divergence.

**Out of scope:**

- `snapshot_id` population in `FetchResponse` — that is KIP-405 (tiered storage),
  tracked separately. It stays default.
- Raising any advertised API version cap (Fetch is already v4–v18;
  OffsetForLeaderEpoch v2–v4). We only *populate* fields, gated to v12+.
- Producer-side / transactional epoch concerns.

## Components

### 1. Leader-epoch cache — `crates/log/src/leader_epoch_checkpoint.rs`

`diverging_epoch` needs the `(epoch, end_offset)` pair, but
`end_offset_for_epoch` returns only the offset. Add a method with Kafka
`LeaderEpochFileCache.endOffsetFor` semantics:

```rust
/// Returns (found_epoch, end_offset):
///   - requested == latest cached epoch  -> (requested, log_end_offset)
///   - requested known, older            -> (largest cached epoch <= requested,
///                                            start_offset of the next-larger epoch)
///   - requested above all cached entries -> (UNDEFINED_EPOCH = -1, log_end_offset)
///   - requested below the cache / empty   -> (UNDEFINED_EPOCH = -1, UNDEFINED_OFFSET = -1)
pub fn epoch_and_offset_for(&self, requested_epoch: i32, log_end_offset: i64) -> (i32, i64)
```

`end_offset_for_epoch` is reimplemented as `self.epoch_and_offset_for(..).1` so
the existing OffsetForLeaderEpoch handler is unchanged. Edge cases are pinned
against Kafka's implementation (verified empirically against cp-kafka if any
case is ambiguous, per CLAUDE.md). `UNDEFINED_EPOCH = -1`,
`UNDEFINED_OFFSET = -1` constants live alongside.

### 2. Leader Fetch path — `crates/broker/src/handlers/fetch.rs`

After the existing KIP-101 `current_leader_epoch` fence and **before** serving
records, for each partition whose request carries `last_fetched_epoch >= 0`
(only meaningful at v12+):

```
(found_epoch, end_offset) = epoch_cache.epoch_and_offset_for(last_fetched_epoch, leo)
if found_epoch < last_fetched_epoch || end_offset < fetch_offset {
    out.records = empty
    out.diverging_epoch = EpochEndOffset { epoch: found_epoch, end_offset }
    // skip the normal read for this partition
}
```

This is Kafka's `ReplicaManager`/`DelayedFetch` divergence rule. Additionally,
populate `out.current_leader = LeaderIdAndEpoch { leader_id, leader_epoch }`
whenever returning `FENCED_LEADER_EPOCH` or `NOT_LEADER_OR_FOLLOWER`, so peers
re-target without a full Metadata round-trip.

All three fields (`diverging_epoch`, `current_leader`, and honoring
`last_fetched_epoch`) are gated to **Fetch v12+**; sub-v12 responses are
byte-identical to today.

### 3. Follower replicator — `crates/broker/src/replicator.rs`

- **Negotiate Fetch v12+** for replication fetches (required for the fields to
  be honored).
- **Send `last_fetched_epoch`**: stamp it from the follower's own leader-epoch
  cache (`latest_epoch()` at the current LEO) in `build_fetch_request`.
- **Handle `diverging_epoch`** in `handle_response`: when present, truncate the
  local log **and** the epoch checkpoint to `end_offset`, reset the fetch offset
  to `end_offset`, and continue — entirely in-band, no extra RPC.
- **Keep the reactive path**: the existing
  `FENCED/UNKNOWN_LEADER_EPOCH → OffsetForLeaderEpoch → truncate` flow remains,
  now serving only the *leadership-change / stale-metadata* case (a distinct
  signal from log divergence). The two coexist exactly as in Kafka.

### 4. Native consumer (Approach B) — `crates/client-consumer/*`

#### 4a. Position model (`consumer.rs`)

Replace `next_offsets: HashMap<(String,i32), i64>` with positions:

```rust
struct FetchPosition {
    offset: i64,
    offset_epoch: i32,         // leader epoch of the last consumed record -> last_fetched_epoch
    current_leader_id: i32,    // from latest metadata
    current_leader_epoch: i32, // from latest metadata
    state: PositionState,
}
enum PositionState { Fetchable, AwaitValidation }
```

`ConsumerRecord` gains `leader_epoch: i32`, sourced from each batch header's
`partition_leader_epoch`.

#### 4b. Metadata → epoch tracking (`coordinator.rs`)

The metadata refresh currently retains only `topic_ids` and drops
`leader_epoch`. Extend it to capture per-partition `(leader_id, leader_epoch)`.
When a partition's metadata leader epoch **increases** beyond what its position
holds, flip the position to `AwaitValidation`
(Java `maybeValidatePositionForCurrentLeader`).

#### 4c. Proactive validate-positions pre-pass (new `validate.rs`)

Before each fetch, every `AwaitValidation` partition with a known
`offset_epoch >= 0` issues an `OffsetForLeaderEpoch` request to its leader
(batched per-leader; partial per-partition errors tolerated). No client code
sends this today — add the client RPC (helper in `client-core`). On the
`(end_offset, leader_epoch)` response:

- `end_offset < position.offset` **or** `leader_epoch < offset_epoch`
  → **truncation**: safe offset = `end_offset` → apply reset policy (§4e).
- otherwise → position becomes `Fetchable`; `offset_epoch ← response.leader_epoch`.
- `FENCED`/`UNKNOWN_LEADER_EPOCH`/`NOT_LEADER` → refresh metadata, retry.

`AwaitValidation` partitions are excluded from the fetch until cleared. This is
the proactive guarantee that defines Approach B: truncation is caught *before*
consuming bad offsets after a leadership change.

#### 4d. Error-first poll loop (`poll.rs`)

Today poll iterates partitions and silently skips any with no `records`, masking
errors. Restructure to inspect `error_code` and `diverging_epoch` **before**
decoding:

- `diverging_epoch` present → in-band truncation at `end_offset` → reset policy.
- `OFFSET_OUT_OF_RANGE` → `auto.offset.reset` via ListOffsets (currently missing).
- `FENCED`/`UNKNOWN_LEADER_EPOCH` → mark `AwaitValidation` + metadata refresh; retry.
- `NOT_LEADER_OR_FOLLOWER` → use the `current_leader` hint if present, else refresh.
- success → decode; capture each batch `partition_leader_epoch` into
  `offset_epoch`; advance offset.

The fetch builder sets `current_leader_epoch` and `last_fetched_epoch` from the
position, and negotiates **Fetch v12+**.

#### 4e. Offset-reset policy + truncation surfacing (`builder.rs`, `error.rs`)

Add `AutoOffsetReset::None` (greenfield — no compat shim). With a reset policy,
truncation auto-resets to earliest/latest. With `None`, truncation surfaces as
`ConsumerError::LogTruncation { topic, partition, fetch_offset, safe_offset }` —
the Java `LogTruncationException` analogue.

#### 4f. Commit/fetch epoch round-trip (`offset_wire.rs`)

- OffsetCommit sends `committed_leader_epoch = position.offset_epoch` (was
  hardcoded `-1` at `offset_wire.rs:115`).
- OffsetFetch reads `committed_leader_epoch` back and seeds `offset_epoch` on
  assignment, so a restarted consumer validates its committed position against
  the leader — **truncation detection survives restarts**.

#### 4g. Concurrency discipline

The coordinator task and `poll` share the position map under a mutex; the
coordinator holds it while reconciling assignment→positions during rebalance.
Validation `OffsetForLeaderEpoch` RPCs are issued **outside** the lock; their
results are applied **under** it with a re-check that the partition is still
assigned and its leader epoch still current, so a stale validation result cannot
clobber a concurrent rebalance.

## Data flow — end-to-end divergence example

Partition leader cache: epoch 0 → `[0,3)`, epoch 1 → `[3,5)` (LEO 5). A follower
diverged at epoch 0 with extra records up to LEO 5 (its epoch cache: epoch 0 →
`[0,5)`):

1. Follower fetches `fetch_offset=5, last_fetched_epoch=0` (Fetch v12).
2. Leader: `epoch_and_offset_for(0, 5) = (0, 3)` (epoch 0 ended at offset 3).
   `found_epoch(0) == last_fetched_epoch(0)` but `end_offset(3) < fetch_offset(5)`
   → divergence. Returns empty records, `diverging_epoch = {epoch:0, end_offset:3}`.
3. Follower truncates log + epoch checkpoint to 3, resets fetch offset to 3.
4. Follower re-fetches at `fetch_offset=3, last_fetched_epoch=0`;
   `epoch_and_offset_for(0,5)=(0,3)`, `end_offset(3) == fetch_offset(3)` → no
   divergence; leader serves `[3,5)` at epoch 1. Logs converge.

The consumer path is identical except detection also happens proactively in §4c
when the leader epoch bumps, before the bad fetch is ever issued.

## Error handling

| Condition | Leader returns | Fetcher action |
|-----------|----------------|----------------|
| `current_leader_epoch` < leader's | `FENCED_LEADER_EPOCH` + `current_leader` | refresh leader; (consumer) `AwaitValidation` |
| `current_leader_epoch` > leader's | `UNKNOWN_LEADER_EPOCH` | refresh metadata; retry |
| log divergence (`last_fetched_epoch`) | `diverging_epoch{epoch,end_offset}` | truncate to `end_offset`, reset, re-fetch |
| fetch offset below log start | `OFFSET_OUT_OF_RANGE` | `auto.offset.reset` (or `LogTruncation` if `None`) |
| not the leader | `NOT_LEADER_OR_FOLLOWER` + `current_leader` | re-target leader |

## Testing strategy

Deterministic core first (divergence constructed directly, not raced), then JVM:

1. **Epoch-cache unit tests** — `epoch_and_offset_for` across every edge:
   requested == latest, older-known, above-all, below-all, empty cache.
2. **Leader handler tests** — seed a log with a known epoch history; drive the
   Fetch handler with `(fetch_offset, last_fetched_epoch)` pairs; assert
   `diverging_epoch` vs. normal serve, and `current_leader` on fence errors.
3. **Follower truncation integration (Crabka↔Crabka)** — extend
   `crates/broker/tests/leader_epoch.rs`: write a divergent suffix to a
   follower's log via test hooks, start replication, assert it truncates the log
   **and** epoch checkpoint to the diverging offset and re-fetches in-band (no
   OffsetForLeaderEpoch RPC observed).
4. **Consumer integration (Crabka broker + native consumer)**, extending
   `crates/client-consumer/tests/integration.rs`:
   (a) proactive — bump a partition's leader epoch, induce divergence, poll →
   assert an `OffsetForLeaderEpoch` is issued and the position resets per policy;
   (b) in-band `diverging_epoch` reset; (c) `OFFSET_OUT_OF_RANGE` reset;
   (d) `auto.offset.reset=None` → `LogTruncation`; (e) **restart survival** —
   commit-with-epoch → restart → OffsetFetch seeds epoch → validation runs.
5. **JVM interop — full mixed-cluster scenario** (via the `broker-jvm-acceptance`
   harness):
   - **Wire-conformance:** a Java client/AdminClient against a Crabka broker
     confirms `diverging_epoch` and `OffsetForLeaderEpoch` responses are
     byte-exact and decode at v12+.
   - **Induced divergence:** a mixed JVM+Crabka cluster forces a real divergent
     suffix (unclean leadership change) and asserts a **JVM follower truncates
     from a Crabka leader**, and a **JVM consumer recovers** against a Crabka
     leader. Where tractable, also assert a **Crabka follower truncates from a
     JVM leader**.

## Risks

- **Inducing divergence deterministically** — the gate leans on direct log /
  epoch-checkpoint manipulation hooks rather than racing real elections; the
  unclean-election path is exercised only in the JVM scenario test, which is
  inherently the flakier tier.
- **Coordinator/validation race** — mitigated by the §4g lock discipline.
- **Fetch-version negotiation** — replicator and consumer must send v12+; low
  ripple since the advertised cap is unchanged, but any existing test that
  asserts these response fields stay default at v12+ must migrate.
- **New OffsetForLeaderEpoch client RPC** — must batch per-leader and tolerate
  partial per-partition errors.
- **`current_leader` correctness** — must carry the actual current leader id +
  epoch, not the requesting node.

## Documentation

Flip KIP-320 from ⚠️ to ✅ in `README.md` (the protocol matrix and the
replication-features table) and note completion in `STATUS.md`.

## File touch list

**Broker / log:** `crates/log/src/leader_epoch_checkpoint.rs`,
`crates/broker/src/handlers/fetch.rs`, `crates/broker/src/replicator.rs`,
`crates/broker/tests/leader_epoch.rs`.

**Consumer:** `crates/client-consumer/src/consumer.rs`,
`crates/client-consumer/src/poll.rs`,
`crates/client-consumer/src/coordinator.rs`,
`crates/client-consumer/src/offset_wire.rs`,
`crates/client-consumer/src/builder.rs`,
`crates/client-consumer/src/error.rs`,
`crates/client-consumer/src/validate.rs` (new),
`crates/client-core/src/fetch.rs` (OffsetForLeaderEpoch client helper),
`crates/client-consumer/tests/integration.rs`.

**JVM:** the `broker-jvm-acceptance` harness (new KIP-320 scenario).

**Docs:** `README.md`, `STATUS.md`.
