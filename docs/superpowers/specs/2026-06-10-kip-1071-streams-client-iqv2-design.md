# IQv2 (KIP-796 / 960 / 968) — Interactive Queries v2 for client-streams

**Date:** 2026-06-10
**Crate:** `crabka-client-streams`
**Status:** Design — approved decisions recorded; pending user spec review.

## 1. Goal

Add the JVM IQv2 query-object surface to the streams client:

```rust
let res = streams.query(
    StateQueryRequest::in_store("counts").with_query(KeyQuery::with_key("a"))
);
```

`KafkaStreams::query()` returns a `StateQueryResult<R>` aggregating one
`QueryResult<R>` per local partition, each carrying a `Position` and either a
typed result or a `FailureReason`. This is the IQv2 dispatch envelope
(KIP-796) plus the concrete query types, replacing nothing: the existing v1
`ReadOnly*Store` views (`key_value_store`/`window_store`/`session_store`)
stay and keep working unchanged.

### Query types

| Query | Result `R` | Slice | KIP |
|---|---|---|---|
| `KeyQuery<K,V>` | `Option<V>` | 3a | 796 |
| `RangeQuery<K,V>` | `Vec<(K,V)>` | 3a | 796/985 |
| `WindowKeyQuery<K,V>` | `Vec<(i64,V)>` | 3a | 806 |
| `WindowRangeQuery<K,V>` | `Vec<((K,i64),V)>` | 3a | 806 |
| `VersionedKeyQuery<K,V>` | `Option<VersionedRecord<V>>` | 3b | 960 |
| `MultiVersionedKeyQuery<K,V>` | `Vec<VersionedRecord<V>>` | 3b | 968 |

> **Window result divergence (pre-existing):** window stores persist only
> `windowStart`, not window size, so window-query results carry `windowStart`
> (`i64`) rather than the JVM's full `Windowed<K>` (`start`+`end`) — matching
> the existing v1 `ReadOnlyWindowStore::fetch`, which is already start-only.

## 2. Slice split

The dispatch envelope is shared, so it is designed once here and built in 3a;
3b adds only the two versioned query types on top.

- **Slice 3a (this design's first deliverable):** the IQv2 envelope
  (`StateQueryRequest` / `Query` / `QueryResult` / `StateQueryResult` /
  `Position` / `PositionBound` / `FailureReason`), `KafkaStreams::query()`,
  the per-partition reply refactor of the IQ channel, the
  `iq2_execute` store hook, and the four non-versioned query types
  (`KeyQuery`, `RangeQuery`, `WindowKeyQuery`, `WindowRangeQuery`) — including
  the new window key-range store op that `WindowRangeQuery` requires. Goldens
  for all four.
- **Slice 3b (follow-on, separate plan):** `VersionedKeyQuery` (KIP-960) and
  `MultiVersionedKeyQuery` (KIP-968), the versioned-range store op, and their
  goldens. Built entirely on the 3a envelope.

Each slice gets its own implementation plan. This document is the shared
design; `writing-plans` produces the 3a plan immediately after spec approval.

## 3. Dispatch model: implied serdes, store-side execution (A′)

JVM IQv2 reads K/V serdes from store config, so `query()` is serde-free at the
call site. Rust has no reflection, but **the store already owns its serdes** —
`VersionedBytesStore<K,V>`, `KeyValueBytesStore<K,V>`, `WindowBytesStore<K,V>`
are each constructed with `key_serde`/`value_serde` and remain generic over
`K,V` even though the supervisor only ever holds them as `&dyn IqQueryable`.
So the store does the (de)serialization and `query()` stays serde-free.

Mechanism:

1. The user's key reaches the store as `Box<dyn Any + Send>` (the raw `K`
   value); time bounds and ordering/bound flags travel as plain scalars. No
   bytes, no serde at the call site.
2. `IqQueryable` gains one dyn-safe hook:

   ```rust
   async fn iq2_execute(&self, query: &Iq2Query)
       -> Result<Box<dyn Any + Send>, FailureReason>;
   ```

   The concrete `…BytesStore<K,V>` impl downcasts the key to `&K`, serializes
   with *its own* key serde, runs the op against its byte storage, deserializes
   values with *its own* value serde, and returns the typed `R` boxed
   (e.g. `Box::new(Some(value))` for `KeyQuery`).
3. `query::<Q>()` downcasts each partition's `Box<dyn Any>` back to
   `Q::Result`. `R` is fixed by the `Query` trait's associated `Result` type,
   so the call site is fully type-inferred and serde-free.

The supervisor never learns concrete store types — everything stays behind
`dyn IqQueryable`, preserving the existing byte-level abstraction.

**Trade-off (accepted):** the type the store boxes and the `R` that `query()`
downcasts to form a runtime contract. A mismatch (caller infers the wrong `V`)
surfaces as `QueryResult::Failure { StoreException }` via a failed downcast,
not a compile error — consistent with the erasure-downcast model the
versioned-tables work already relies on. The contract is centralized: each
`Iq2Query` variant maps to exactly one boxed `R` type, asserted in one place
and covered by goldens.

### `Iq2Query` (internal)

```rust
pub(crate) enum Iq2Query {
    Key      { key: Box<dyn Any + Send> },
    Range    { lo: Option<Box<dyn Any + Send>>, hi: Option<Box<dyn Any + Send>>, descending: bool },
    WindowKey{ key: Box<dyn Any + Send>, from_ts: i64, to_ts: i64 },
    WindowRange { lo: Option<Box<dyn Any + Send>>, hi: Option<Box<dyn Any + Send>>, from_ts: i64, to_ts: i64 },
    // 3b:
    VersionedKey      { key: Box<dyn Any + Send>, as_of: Option<i64> },
    MultiVersionedKey { key: Box<dyn Any + Send>, from_ts: Option<i64>, to_ts: Option<i64>, descending: bool },
}
```

Public query builders lower to `Iq2Query` inside `query()`.

## 4. Channel & per-partition refactor

Today `serve_iq` gathers `&dyn IqQueryable` from every matching task and
`answer_iq` **merges** results across partitions, returning a single
`IqPayload`. IQv2 needs results kept **per partition**.

- The IQ channel message becomes an enum: `IqMessage::V1(IqRequest)` (existing
  byte path, unchanged behavior) and `IqMessage::V2(Iq2Request)`.
- `Iq2Request { store, query: Iq2Query, partitions: PartitionSel, bound: PositionBound, reply }`.
- `serve_iq` for V2: filter tasks by `partitions` (all, or an explicit set),
  call `iq2_execute` on each matching store, and tag each result with the
  task's `partition` and a `Position` snapshot. Reply:
  `Vec<(i32, Position, Result<Box<dyn Any + Send>, FailureReason>)>`.
- The v1 path is left exactly as-is; `answer_iq`'s merge logic is untouched.
  (No refactor of v1 views — V2 is a parallel path on the same channel.)

## 5. Position / PositionBound / FailureReason

- **`Position`** = `BTreeMap<String, BTreeMap<i32, i64>>` (topic → partition →
  offset). Built from the task's existing live `positions: HashMap<(String,i32),i64>`
  map, which is already advanced on every consumed record
  (`task.rs:312`) and reset on rebalance recovery (`seek_to_start`). Net-new
  work: a `StreamTask::position() -> Position` accessor. No new tracking.
- **`PositionBound`** = `Unbounded` (default) | `At(Position)`. JVM-exact: if a
  partition's snapshot does not meet the bound, that partition's `QueryResult`
  is `Failure { NotUpToBound }`. **Fail fast — never block.**
- **`FailureReason`**: `UnknownQueryType`, `NotUpToBound`, `NotPresent` (store
  exists in topology but not on this partition's task), `NotActive` (partition
  is standby/restoring when an active-only query is required), `DoesNotExist`
  (store name absent from topology), `StoreException` (downcast / internal).
  Mapped from today's `IqError` variants plus the downcast contract.

`StateQueryResult<R>` exposes `partition_results() -> &BTreeMap<i32, QueryResult<R>>`
and `only_partition_result()` (the JVM `getOnlyPartitionResult` convenience,
returning `None`/panic-free when not exactly one). Global-store results are
out of scope (see §9); `global_result()` returns `None`.

## 6. Public API surface

```rust
pub trait Query { type Result; }   // sealed in-crate

pub struct StateQueryRequest<Q: Query> { /* store, query, partitions, bound, exec_info */ }
impl<Q: Query> StateQueryRequest<Q> {
    pub fn in_store(name: impl Into<String>) -> StateQueryRequestBuilder; // .with_query(q)
    pub fn with_partitions(self, set: BTreeSet<i32>) -> Self;
    pub fn with_all_partitions(self) -> Self;            // default
    pub fn with_position_bound(self, b: PositionBound) -> Self;
}

pub enum QueryResult<R> {
    Success { result: R, position: Position },
    Failure { reason: FailureReason, message: String },
}
impl<R> QueryResult<R> {
    pub fn is_success(&self) -> bool;
    pub fn result(&self) -> Option<&R>;
    pub fn position(&self) -> Option<&Position>;
    pub fn failure_reason(&self) -> Option<FailureReason>;
}

pub struct StateQueryResult<R> { /* partition_results, global_result */ }

// builders — serde-free, K/V inferred
KeyQuery::<K,V>::with_key(k)
RangeQuery::<K,V>::with_range(lo, hi)
RangeQuery::<K,V>::with_lower_bound(lo) | ::with_upper_bound(hi) | ::with_no_bounds()
    .with_ascending_keys() /* default */ | .with_descending_keys()
WindowKeyQuery::<K,V>::with_key(k).from_time(t).to_time(t)
WindowRangeQuery::<K,V>::with_key_range(lo, hi).from_time(t).to_time(t)   // or ::with_all_keys()
VersionedKeyQuery::<K,V>::with_key(k).as_of(t)                            // 3b; as_of optional → latest
MultiVersionedKeyQuery::<K,V>::with_key(k).from_time(t).to_time(t)        // 3b
    .with_ascending_timestamps() /* default */ | .with_descending_timestamps()

impl KafkaStreams {
    pub fn query<Q: Query>(&self, req: StateQueryRequest<Q>) -> StateQueryResult<Q::Result>;
}
```

`query()` checks `state == Running` (else every partition result is
`Failure`), lowers the query to `Iq2Query`, round-trips the channel, and
downcasts each partition's `Box<dyn Any>` into `Q::Result`.

## 7. Store-layer additions

- **Window key-range op (3a, new):** `WindowBytesStore` gains an
  `iq_window_range(lo, hi, from_ts, to_ts) -> Vec<((Bytes /*key*/, i64 /*start*/), Bytes)>`
  byte method backing `WindowRangeQuery`. `iq_window_fetch` /
  `iq_window_fetch_single` already exist for `WindowKeyQuery`.
- **Versioned range op (3b, new):** walk the key's
  `BTreeMap<i64, Option<Bytes>>` chain, returning versions whose validity
  interval `[valid_from, valid_to)` overlaps `[from_time, to_time]`, ordered
  asc/desc. `iq_versioned_get` / `iq_versioned_get_as_of` already exist for
  `VersionedKeyQuery`.
- **`iq2_execute` impls:** on `KeyValueBytesStore`, `WindowBytesStore`
  (3a) and `VersionedBytesStore` (3b). Session stores have no JVM IQv2 query
  type and are excluded.

## 8. Module layout

```
src/runtime/iqv2/mod.rs       // re-exports (public surface)
src/runtime/iqv2/query.rs     // Query trait, public builders, Iq2Query, lowering
src/runtime/iqv2/request.rs   // StateQueryRequest, PartitionSel, PositionBound, Position
src/runtime/iqv2/result.rs    // QueryResult, StateQueryResult, FailureReason
```

Modified:
- `store/iq.rs` — `iq2_execute` hook on `IqQueryable`; window key-range default method.
- `store/window.rs` — window key-range impl + `iq2_execute`.
- `store/kv.rs` — `iq2_execute`.
- `store/versioned.rs` (3b) — versioned-range + `iq2_execute`.
- `runtime/iq.rs` — `IqMessage` enum, `Iq2Request`, per-partition reply type.
- `runtime/thread.rs` — `serve_iq` V2 arm: partition filter + position tagging.
- `runtime/task.rs` — `position()` accessor.
- `runtime/app.rs` — `KafkaStreams::query()`.
- `test_driver.rs` — `TopologyTestDriver::query()` helper (single-partition, partition 0).
- `lib.rs` / `runtime/mod.rs` — module wiring + re-exports.

## 9. Out of scope (YAGNI)

- **Session IQv2 queries** — no JVM query type exists.
- **Global-store queries** — `global_result()` is always `None`.
- **Multi-instance remote routing** — queries hit only local tasks, exactly
  like v1. (`StateQueryResult` is genuinely multi-partition because one
  instance owns multiple tasks, but cross-instance fan-out is not built.)
- **`enableExecutionInfo` detail** — accepted on the request and reported as an
  empty list; no per-store timing capture.
- **`Position` vectoring into changelog topics** — `Position` reflects source
  topic-partition offsets only (what the task's `positions` map holds).

## 10. Testing

`TopologyTestDriver` uses one graph at partition 0 and the same `iq2_execute`
path as the real supervisor, so behavioral goldens run JVM-free.

New `tests/iqv2_golden.rs` + `testdata/iqv2/*.json` captured from **Docker
Streams 4.1** (ground truth per project convention). Cases:

- **3a:** `KeyQuery` hit/miss; `RangeQuery` bounds (full / lower-only /
  upper-only / none) × ascending/descending; `WindowKeyQuery` time range;
  `WindowRangeQuery` key-range × time-range; `PositionBound` met vs
  `NotUpToBound`; `DoesNotExist` and `NotPresent` failures;
  `only_partition_result` on single-partition.
- **3b:** `VersionedKeyQuery` latest vs `as_of`; `MultiVersionedKeyQuery`
  asc/desc, overlap semantics at range edges, retention horizon.

Add `tests/iqv2_golden.rs` to the crate's llvm-cov `--test` list in
`.github/workflows/ci.yml` (per the per-crate-integration coverage convention).

## 11. Verification gate (per slice)

```
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p crabka-client-streams
cargo build --workspace
```

(Clippy cache can mask workspace lints — `touch` suspect-but-unchanged files
and check the real `$?`. New `tests/*.rs` must be in the crate's llvm-cov
`--test` list or coverage reports 0% for the patch.)
