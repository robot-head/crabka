# KIP-1071 Streams Client — Versioned KTables, Slice 2 (KIP-914 join half + KIP-923 join grace)

**Date:** 2026-06-10
**Status:** Design approved, pending spec review
**Scope:** A self-contained client-side slice making **joins** temporally correct
over versioned tables in `crates/client-streams`. Adds (1) as-of stream–table
lookups, (2) a stream–table join **grace period** (`Joined.withGracePeriod`,
KIP-923), and (3) out-of-order suppression for table–table joins. No broker or
wire-protocol changes.

## 1. Context

Versioned KTables **slice 1** (KIP-889 + the *table* half of KIP-914) merged in
[#481](https://github.com/anthropics/crabka/pull/481): the
`VersionedKeyValueStore` (`store/versioned.rs`), `Materialized::as_versioned`,
the `VersionedKTableSourceProcessor` (table-update rule: an older-timestamp
record no longer clobbers the latest), byte-exact versioned changelog, and the
store-side IQ surface. What slice 1 deliberately deferred (its §"Non-goals"):

- **Stream–table / table–table join as-of lookups** (KIP-914 *join* half) — this slice.
- **Public IQ query types** (`VersionedKeyValueQuery`, KIP-960/968) — slice 3.

This slice closes the join half. The remaining roadmap after this:

| Slice | Scope | KIP | Status |
|-------|-------|-----|--------|
| 1 | `VersionedKeyValueStore` + versioned `builder.table` + changelog/restore + IQ store surface | 889 + table half of 914 | **merged (#481)** |
| **2 (this spec)** | Stream–table as-of join; stream–table join grace; table–table out-of-order suppression | 914 (join half) + 923 | **this slice** |
| 3 | IQv2 `VersionedKeyValueQuery` / `MultiVersionedKeyQuery` | 960 / 968 | future |

## 2. Semantics (ground truth)

These were verified against the KIPs and will be **pinned byte-for-byte / behaviorally**
against an empirical Kafka-Streams 4.1.0 Docker capture (the project MO). Two
KIPs are in play — slice 2 conflates them because they land on the same join
operators:

### 2.1 Stream–table join, as-of (KIP-914)

> "the processor performs a `get(key, timestamp)` instead, where `timestamp` is
> the stream-side record's timestamp." — KIP-914

- When the joined table is **versioned**, the stream–table join looks up the
  table value valid *as of the stream record's timestamp* via
  `get_as_of(key, streamRec.ts)`, not the latest value.
- **Null handling** (KIP-914): "If the stream timestamp exceeds the store's
  history retention window … no join result will be produced for inner joins,
  whereas for left joins a join result with null table value will be produced."
  I.e. a null as-of result is treated exactly like a normal table miss.
- The output record keeps the **stream record's timestamp**.
- A **non-versioned** table is unchanged: latest `get(key)`.

### 2.2 Stream–table join grace (KIP-923)

`Joined.withGracePeriod(Duration)` adds a buffer on the **stream** side:

- **Unset** → "the join will execute as before" — i.e. join as-it-comes
  (as-of lookup if the table is versioned, latest otherwise).
- **Zero** → "the join will execute as a normal join where each record tries to
  join to the point of time in the versioned table as it comes in."
- **Non-zero** → "the record will enter a stream buffer and will dequeue when the
  record timestamp is less than or equal to stream time minus the grace period."
  Buffered records are processed in **ascending timestamp order**; the as-of
  lookup happens **at dequeue time**. A record that is *already late on arrival*
  (its ts is already `≤ streamTime − grace`) is processed immediately ("late
  records outside the grace period are executed as they come in").
- **Constraint** (KIP-923): "The grace period must be less than the joining
  table's history retention." Grace is only meaningful with a versioned table;
  we assert a versioned table at build time when grace is set.

### 2.3 Table–table join, out-of-order suppression (KIP-914)

> "the table-table join processors will only ever call `get(key)` and not
> `get(key, timestamp)`." … "if the new record is not out-of-order, emit the
> latest join result. Else, emit nothing." — KIP-914

- Table–table joins do **NOT** use as-of lookups. Each side still reads the
  other side's **latest** value via `get(key)`.
- The only versioned behavior: when a join input is **versioned**, an
  **out-of-order** change (the incoming record's ts is older than the versioned
  store's current latest `validFrom`) emits **nothing**.
- Slice 1's `VersionedKTableSourceProcessor` forwards *every* change, including
  out-of-order ones ("Out-of-order records still emit their local change; the
  store keeps the latest pointer"). Therefore the suppression gate lives in the
  **join processor**, not the source.
- Mixed versioning (KIP-914): when one side is versioned and the other is not,
  out-of-order records from the *versioned* side don't trigger results, but
  out-of-order records from the *unversioned* side do (the unversioned side has
  no notion of out-of-order).

## 3. Architecture

The crate models DSL features as a **triplet** (DSL surface + processor + store).
Slice 2 reuses existing stores where it can and adds exactly one new store (the
join grace buffer). Five components, designed for isolation:

### C1 — Propagate versioned-ness to the `KTable` handle

`Materialized` already carries `versioned: Option<VersionedConfig>` with
`history_retention_ms` (slice 1). The join operators need this at *build* time to
route. Add to `KTable`:

```rust
/// `Some(history_retention_ms)` when this table is materialized into a
/// versioned store (KIP-889). Drives as-of join lookups + grace validation.
/// Mirrors `window_grace_ms`.
pub(crate) versioned_retention_ms: Option<i64>,
```

- `table_explicit` sets it from `materialized.versioned.map(|v| v.history_retention_ms)`.
- It is propagated through store-preserving identity ops the same way
  `window_grace_ms` / `suppress_store_factory` already are. Key-changing or
  re-materializing ops reset it to `None` (the resulting table is not the
  versioned source store).
- **What does it do / how is it used / what does it depend on:** a read-only tag
  on the handle; read by `join_table_impl` (C2/C3) and the table–table join
  builder (C4); depends only on `Materialized`.

### C2 — Stream–table as-of join processor

New `KStreamKTableJoinAsOfProcessor<K, V, VT, VO, F>` next to the existing
`KStreamKTableJoinProcessor` in `dsl/processors/join.rs`. Identical shape, except
the lookup:

```rust
let vt = match ctx.get_versioned_store::<K, VT>(&self.table_store) {
    Some(s) => s.get_as_of(&key, r.timestamp).await.map(|rec| rec.value),
    None => None,
};
if vt.is_some() || self.emit_on_miss {
    ctx.forward(Record::new(Some(key), (self.joiner)(&r.value, vt.as_ref()), r.timestamp));
}
```

`join_table_impl` routes on `table.versioned_retention_ms`:
- `Some(_)` → `KStreamKTableJoinAsOfProcessor`.
- `None` → existing `KStreamKTableJoinProcessor` (unchanged).

**Topology/wire are unchanged either way** — the join node name
(`KSTREAM-JOIN-…`), store connection, subtopology union, and copartition group
are identical. The only byte-level difference vs a non-versioned join is the
*store's own* versioned changelog config, already emitted by slice 1. This is the
fidelity anchor: no new node, no new store, no counter shift.

### C3 — Stream–table join grace (KIP-923)

**Config.** A minimal `Joined` config (mirrors how `StreamJoined` exists for
stream–stream joins):

```rust
pub struct Joined {
    pub(crate) grace_ms: Option<i64>,
    pub(crate) name: Option<String>,   // optional buffer-store base name
}
impl Joined {
    pub fn with_grace_period(grace_ms: i64) -> Self { … }
    pub fn as_named(self, name: impl Into<String>) -> Self { … }
}
```

**DSL surface.** New methods that leave the existing two untouched:

```rust
pub fn join_table_with<…>(&self, table: &KTable<…>, joiner: F, joined: Joined) -> KStream<…>
pub fn left_join_table_with<…>(&self, table: &KTable<…>, joiner: F, joined: Joined) -> KStream<…>
```

(The existing `join_table` / `left_join_table` are the no-grace forms; they
delegate to `join_table_impl` which now also takes `Option<grace_ms>`.)

**Build-time validation** (assert, since Crabka is greenfield — no soft
fallbacks): when `grace_ms` is set,
1. `table.versioned_retention_ms.is_some()` — grace requires a versioned table.
2. `grace_ms < history_retention_ms` — KIP-923 constraint.

**Processor.** `KStreamKTableJoinGraceProcessor` mirrors the **suppress**
processor's stream-time model (`dsl/processors/suppress.rs`):

- Track `observed_stream_time = max(observed_stream_time, r.timestamp)` on each record.
- Buffer the incoming record into the join buffer store (C3-store) keyed by
  `(ts, seqnum)`.
- Compute `threshold = observed_stream_time − grace_ms`; **drain** every buffered
  record with `bufTs ≤ threshold` in ascending `(ts, seq)` order. For each
  drained record, perform the as-of lookup `get_as_of(key, bufTs)` and forward
  the join result (inner: skip on miss; left: `None` on miss) at `bufTs`.
- A record whose `ts ≤ threshold` at arrival is therefore drained in the same
  pass it was buffered → "executed as it comes in", matching KIP-923's late-record
  rule without a special path.

**C3-store — join grace buffer.** A new dedicated time-ordered buffer store
(reusing the *pattern* of `store/suppress_store.rs`, not its type, because its
name + changelog config must byte-match the JVM join buffer independently):
`JoinGraceBufferStore` holding serialized `(key, value, ts)` entries ordered by
`(ts, seq)`. Store name + changelog config (`cleanup.policy`, `retention.ms`) are
**golden-pinned** from the capture — expect a debug cycle on the JVM buffer store
name (the same class of trap as the window-store-name burn in prior slices).

### C4 — Table–table join out-of-order gate

Thread a `versioned: bool` into `KTableKTableJoin{This,Other}Processor`
(`dsl/processors/ktable_join.rs`). When `versioned`:

- On an incoming `Change`, determine whether the record is *in-order* — i.e. its
  timestamp equals the versioned store's current latest `validFrom` for the key
  (the record we just wrote is the latest). If it is out-of-order (older), **emit
  nothing**.
- The other side's value is still read via **latest** `get(key)` (never as-of).

The table–table join builder (`KTable::join_impl`) sets `versioned` per side from
each input table's `versioned_retention_ms`. Mixed versioning falls out
naturally: the unversioned side passes `versioned = false` and never suppresses.

The exact in-order detection mechanism (compare record ts to store latest
`validFrom`, vs a flag forwarded by the source) is **golden-pinned**; the design
intent is "out-of-order ⇒ no emit."

### C5 — JVM goldens

Extend `crates/client-streams/tests/jvm-capture` (alongside
`VersionedTableBehavior.java`) with capture programs + `run.sh` targets:

1. **`StreamTableAsOfBehavior`** — versioned table, stream record between two
   table versions; assert it joins the *as-of* value, and that a stream ts beyond
   `historyRetention` is a miss/`null`.
2. **`StreamTableGraceBehavior`** — out-of-order stream records under a grace
   period; assert buffered/ordered emission + the late-record immediate path.
3. **`TableTableVersionedBehavior`** — out-of-order update on a versioned side;
   assert no join result is emitted, in-order update is.

Capture **topology** (node/store names, changelog configs) + **behavioral**
(output records) + **changelog** (for the new grace buffer store). Rust replay
tests match byte-for-byte (topology/changelog) and record-for-record
(behavioral). The full golden suite is the gate — erasure type-mismatch is a
runtime downcast, not a compile error.

## 4. Data flow

```
stream rec (k, v, ts) ─┐
                       ├─ no grace ─→ KStreamKTableJoinAsOfProcessor
                       │                └─ get_as_of(k, ts) ─→ join ─→ forward@ts
                       └─ grace>0 ──→ KStreamKTableJoinGraceProcessor
                                        ├─ buffer (k,v,ts) by (ts,seq)
                                        ├─ streamTime = max(streamTime, ts)
                                        └─ drain bufTs ≤ streamTime−grace, asc:
                                             get_as_of(k, bufTs) ─→ join ─→ forward@bufTs

table-table: Change<VA>/Change<VB> ─→ JoinThis/Other (versioned: gate out-of-order)
                                        └─ get(k) latest other side ─→ result rule ─→ forward
```

## 5. Testing strategy

- **TDD per processor** against in-memory stores: as-of processor (hit between
  versions / beyond retention / left vs inner), grace processor (in-order drain,
  out-of-order buffering + ordered drain, late-on-arrival immediate, grace<
  retention assert), table–table gate (in-order emit / out-of-order suppress /
  mixed versioning).
- **Golden replay** as the suite gate (C5).
- Build-time assertion tests (grace without versioned table panics; grace ≥
  retention panics).

## 6. Non-goals (deferred)

- **IQv2 versioned query types** (`VersionedKeyValueQuery`, KIP-960/968) — slice 3.
- **Versioned-store join with a grace buffer changelog *restore* fidelity** beyond
  what the golden battery covers (the buffer is a standard changelogged store;
  restore reuses the existing clean-slate replay path).
- **FK-join / global-table as-of** — KIP-914 scopes only stream–table and
  table–table; FK and global joins are out of scope.

## 7. JVM-fidelity gotchas anticipated

- The grace **buffer store name** + changelog config (the C3-store trap).
- Whether table–table out-of-order detection compares against store latest or a
  source-forwarded flag — pin against `TableTableVersionedBehavior`.
- Output **timestamps**: as-of join forwards at the stream/buffered record ts;
  confirm against behavioral capture (no silent `−1` like the slice-1 fetch bug).
- Node-name counter must **not** shift vs a non-versioned join (C2 adds no node).

## 8. Batching (parallel execution, non-overlapping file sets)

- **Batch 1:** C1 (`dsl/ktable.rs` field + `dsl/builder.rs` propagation) ‖
  C5 capture harness (`tests/jvm-capture/src/main/java/**`, `run.sh`).
- **Batch 2:** C2 (`dsl/processors/join.rs` + `dsl/kstream.rs` route) ‖
  C4 (`dsl/processors/ktable_join.rs` + `dsl/ktable.rs::join_impl`).
- **Batch 3:** C3 (`dsl/config.rs` `Joined`, `dsl/processors/join.rs` grace
  processor or new `join_grace.rs`, `store/join_grace_buffer.rs`,
  `dsl/kstream.rs` `_with` methods).
- **Batch 4:** golden replay tests + reconciliation.
