# KIP-1071 Streams Client — Versioned KTables, Slice 1 (KIP-889 / 914)

**Date:** 2026-06-09
**Status:** Design approved, pending spec review
**Scope:** A self-contained client-side slice adding a `VersionedKeyValueStore`
and versioned-table materialization to `crates/client-streams`. No broker or
wire-protocol changes.

## 1. Context

The KIP-1071 streams **client** runtime (`crates/client-streams`, crate
`crabka-client-streams`) is feature-rich: the original 7-sub-project program
plus FK-join, global table, suppress, punctuation, EOS, standby/warmup,
schema-serde, and — most recently — sliding windows (KIP-450) have all merged.
The remaining DSL-parity gaps are **versioned KTables (KIP-889 / 914)**, cogroup
(KIP-150), and emit-final (KIP-825). This spec covers the first slice of
versioned tables.

"Versioned KTables" in Apache Kafka is a cluster of KIPs, not one:

- **KIP-889 — Versioned State Stores.** The `VersionedKeyValueStore<K,V>` store
  type: `put(k, v, ts)` / `delete(k, ts)` / `get(k)` (latest) / `get(k, asOf)` →
  `VersionedRecord{value, validFrom, validTo}`, governed by `historyRetention`
  and `segmentInterval`. Out-of-order puts are recorded as versions; the latest
  is not clobbered by a stale-timestamp record.
- **KIP-914 — DSL Processor Semantics for Versioned Stores.** The DSL behavior
  versioned stores unlock: `builder.table(...)` stops letting an older-timestamp
  record overwrite the latest, and **stream–table joins become temporally
  correct** (look up the table value valid *as of* the stream record's
  timestamp).
- **KIP-960 / 968 — IQv2 versioned queries.** Point-in-time `VersionedKeyQuery`
  and `MultiVersionedKeyQuery` over a time range.

(For the record: the KIP-1071 memory note bundled this as "KIP-889/962". KIP-962
is "relax non-null key requirement", unrelated to versioning; the IQ piece is
KIP-960/968.)

The crate models stateful DSL features as a **triplet** — a DSL surface, a
processor, and a store + codec — exactly as window/session stores do. This slice
adds versioned tables as a new triplet member.

## 2. The 3-slice roadmap

The whole versioned-tables program decomposes into three independently-shippable
slices, each its own spec → plan → PR:

| Slice | Scope | KIP | Observable payoff |
|------|-------|-----|-------------------|
| **1 (this spec)** | `VersionedKeyValueStore` + `builder.table` materializing versioned + the KIP-914 table-update rule + changelog/restore + IQ store surface | 889 + table half of 914 | A table whose out-of-order records don't clobber the latest; as-of(ts) reads available |
| **2** | Stream–table & table–table joins consume **as-of-timestamp** lookups; join grace | 914 (join half) | Temporally-correct joins |
| **3** | IQv2-style `VersionedKeyQuery` / `MultiVersionedKeyQuery` over the IQ surface | 960 / 968 | Point-in-time & version-range IQ |

Slice 1 builds the **complete** store (out-of-order insertion + as-of reads +
retention expiry) so it is pinned by a golden exactly once; slices 2–3 are pure
*consumers* of that store and add no new store semantics.

## 3. Goal and non-goals

### Goal

A Rust app can write, with full JVM behavioral parity including out-of-order
records and tombstones:

```rust
let table = builder.table_explicit(
    "input",
    Consumed::with(StringSerde, StringSerde),
    Materialized::as_versioned("versioned-store", /* history_retention_ms */ 600_000),
); // KTable<String, String> backed by a VersionedKeyValueStore
```

and the topology serializes byte-for-byte identically to JVM Kafka Streams 4.1
(`optimization=all`), the changelog records match the JVM byte-for-byte, and the
table's emitted change-stream matches the JVM for in-order, out-of-order, and
tombstone input.

### Non-goals (deferred)

- **Stream–table / table–table join as-of lookups** (KIP-914 join half) — slice 2.
- **Public IQ query types** (`VersionedKeyQuery`, KIP-960/968) — slice 3. Slice 1
  lands the store-side IQ byte methods but does not wire a public query.
- **Aggregations into versioned stores** — the JVM forbids this; the surface is
  not reachable in slice 1's DSL, so no guard is needed yet.
- No backwards-compat shims (greenfield, per `CLAUDE.md`).

## 4. Architecture — the triplet + module layout

Follows the crate's established **triplet** shape (DSL surface + processor +
store + codec), as window/session stores do.

New:

- **`src/store/versioned_schema.rs`** — the `ValueAndTimestamp` value codec
  (`validFrom:8B-BE ‖ value`; `None` = tombstone). Reuses the window store's
  `wrap_value` / `unwrap_value` byte layout, so the changelog value mirrors the
  timestamped-KV changelog. The raw key stays the changelog **key** (JVM-exact).
- **`src/store/versioned.rs`** — `VersionedBytesStore<K,V>` (the version-chain
  store) + the `VersionedKeyValueStore<K,V>` trait + `StateStore` / `IqQueryable`
  impls.

Modified:

- **`src/store/registry.rs`** — add `get_versioned::<K,V>()` downcast (mirrors
  `get_window`).
- **`src/store/iq.rs`** — `StoreKind::Versioned` + two byte methods (§7).
- **`src/store/mod.rs`** — module decls.
- **`src/dsl/processors/table.rs`** — add `VersionedKTableSourceProcessor`.
- **`src/dsl/config.rs`** — `Materialized` gains a `versioned:
  Option<VersionedConfig>` field + `Materialized::as_versioned(...)`; a
  `Stores::persistent_versioned_kv(name, history_retention_ms)` supplier.
- **`src/dsl/builder.rs`** — `table_explicit` / `table` take the versioned
  lowering branch when `versioned` is set.
- **`src/topology/builder.rs`** — `add_versioned_store(...)` emitting the
  versioned store type + changelog config into the topology JSON.
- **`src/dsl/mod.rs`, `src/lib.rs`** — re-exports + a module-doc section.

**No changes to the shared `StateStore` changelog tuple `(Bytes, Option<Bytes>)`
or the produce/restore plumbing** — the version timestamp rides in the value
header, exactly as the window store already does.

## 5. Store API & semantics

```rust
pub struct VersionedRecord<V> {
    pub value: V,
    pub valid_from: i64,
    pub valid_to: Option<i64>, // None = still latest (∞)
}

#[async_trait]
pub trait VersionedKeyValueStore<K: Send + Sync, V: Send>: StateStore {
    /// Insert a version at `validFrom = timestamp`. `None` value = tombstone version.
    async fn put(&mut self, key: K, value: Option<V>, timestamp: i64);
    async fn delete(&mut self, key: &K, timestamp: i64) -> Option<VersionedRecord<V>>;
    async fn get(&self, key: &K) -> Option<VersionedRecord<V>>;            // latest
    async fn get_as_of(&self, key: &K, as_of: i64) -> Option<VersionedRecord<V>>;
}
```

Internal representation (non-observable, chosen for simplicity — approach A1):
`BTreeMap<keyBytes, BTreeMap<validFrom_i64, Option<valueBytes>>>`.

Semantics (KIP-889; pinned by the behavioral golden, §8):

- **`get(k)`** → the version with the greatest `valid_from`; its `valid_to =
  None` (∞). A tombstone latest → `None`.
- **`get_as_of(k, t)`** → version with greatest `valid_from ≤ t`; `valid_to` =
  the next version's `valid_from` (or `None`). If that version is a tombstone, or
  `t` predates the oldest retained version, → `None`.
- **Out-of-order `put`** inserts mid-chain; the latest pointer only advances when
  `ts ≥` the current max `valid_from`.
- **History expiry** — the store tracks observed stream-time (the max `ts` seen).
  A `put` whose `ts < observedStreamTime − history_retention` is **dropped** (and
  counted, matching JVM); versions whose `valid_to < observedStreamTime −
  history_retention` are evicted. `segment_interval` only affects JVM eviction
  granularity (non-observable); the API accepts it but the in-memory store evicts
  per-version — no functional difference.
- **Changelog** — every `put` / `delete` appends `(rawKey, ValueAndTimestamp(ts ‖
  value))` or a tombstone entry; restore unpacks the ts and rebuilds the chain
  via the same `put` path with logging off.

## 6. DSL surface & the KIP-914 table-update processor

**Supplier + materialized config**, mirroring the JVM API:

```rust
Stores::persistent_versioned_kv(name, history_retention_ms /*, segment_interval_ms */)
Materialized::as_versioned(name, history_retention_ms)
```

`Materialized<KS,VS>` gains `versioned: Option<VersionedConfig {
history_retention_ms, segment_interval_ms }>`. When set, `table_explicit` /
`table` take the versioned lowering branch:

- `add_versioned_store(...)` instead of `add_state_store(...)`.
- `VersionedKTableSourceProcessor` instead of `KTableSourceProcessor`.

**`VersionedKTableSourceProcessor`** parallels `KTableSourceProcessor` but is
timestamp-aware, porting JVM `KTableSource` versioned behavior (KIP-914), pinned
by the behavioral golden:

- Reads `r.timestamp` as the version timestamp; calls `store.put(key, r.value,
  ts)` (a `None` value is a tombstone version).
- **Grace / late drop**: a record older than `observedStreamTime −
  history_retention` is dropped (not stored, not forwarded) — KIP-914's
  "out-of-bounds" rule.
- **`Change` emission**: `old` = the value previously valid *at this record's
  timestamp* (`get_as_of(key, ts)` taken before the put), `new` = incoming value.
  An out-of-order record that lands strictly before the latest still emits its
  local change but does **not** move the latest pointer.

The exact branch structure is ported from apache/kafka 4.1 and pinned by the
behavioral golden (§8); the design commits to *behavioral equivalence with the
JVM*, not to a paraphrase of the branches here.

## 7. Changelog / restore / IQ

- **Changelog drain** — unchanged plumbing. `VersionedBytesStore::take_changelog`
  yields `(rawKey, Some(ts ‖ value))` or tombstone entries, buffered on each
  `put` / `delete` like every other store. The changelog topic config carries
  `min.compaction.lag.ms = history_retention + 86_400_000` and a `cleanup.policy`
  (exact config pinned by the structural golden, §8). The `+ 24h` matches the
  JVM's allowance for broker wall-clock during compaction.
- **Restore** — `apply_changelog(key, value)` unpacks the `ValueAndTimestamp`
  header → `put(key, value, ts)` with logging off, replaying the changelog in
  offset order to rebuild every version chain. Because `min.compaction.lag.ms`
  holds recent history un-compacted, the replay reconstructs the retained
  versions.
- **IQ surface** — extend `IqQueryable` with `StoreKind::Versioned` and two byte
  methods: `iq_versioned_get(key) -> Option<(validFrom, validTo, Bytes)>` and
  `iq_versioned_get_as_of(key, asOf) -> Option<(validFrom, validTo, Bytes)>`.
  Slice 1 ships the store-side surface; wiring it to a public `VersionedKeyQuery`
  is slice 3, but landing the byte methods now keeps the store self-complete.

## 8. Verification — three gates

1. **Structural golden** (existing harness). Add a `versioned_table` topology to
   `tests/jvm-capture` (`Capture.java` + `run.sh`), producing
   `testdata/golden/dsl/versioned_table.topology.json`. Assert byte-equality in
   `dsl_golden_frame.rs`. Pins the versioned store type, processor/source/store
   names, store-name-burn, and the changelog config (`min.compaction.lag.ms`,
   cleanup policy).

2. **Changelog-bytes golden** (the fidelity gate for the open question). Capture
   the actual changelog records the JVM versioned table writes for a fixed input
   battery; dump `(keyBytes, valueBytes, recordTs)` per record to
   `testdata/golden/dsl/behavioral/versioned_changelog.json`; assert Crabka's
   drained changelog matches byte-for-byte. **This decides ts-packed-value vs
   record-ts-field** (KIP-889 defers the choice; the window-store precedent says
   ts-packed). If the golden shows the timestamp is *only* in the record-ts
   field, the fallback is a localized optional-timestamp on the changelog produce
   path (the slice's main risk — §9).

3. **Behavioral golden** (out-of-order correctness). Reuse the sliding-window
   slice's `TopologyTestDriver`-based runner: feed a battery including
   out-of-order timestamps, tombstones, and as-of boundary reads through the JVM
   versioned table, dump emitted `(key, oldChange, newChange, recordTs)` in
   emission order to `testdata/golden/dsl/behavioral/versioned_table.json`, replay
   identical inputs through Crabka's `TopologyTestDriver`, and assert the output
   sequence matches exactly.

Plus unit tests on `VersionedBytesStore` (get / get_as_of / out-of-order insert /
tombstone / retention expiry) and on the processor (in-process `Dispatch` /
`ProcessorContext` harness, mirroring `table.rs` tests).

TDD: write the behavioral + changelog goldens first (red), then port the store +
processor until green. Full `cargo test -p crabka-client-streams` is the
erasure-safety gate (DSL type mismatches are runtime downcast failures, not
compile errors). `cargo fmt --check` and `cargo clippy --workspace --all-targets
-D warnings` before push.

## 9. Risks

- **Changelog byte format ambiguity.** KIP-889 defers whether the version
  timestamp lives in the value header or the Kafka record-ts field. Mitigation:
  the changelog-bytes golden (gate 2) decides empirically; the ts-packed default
  follows the window-store precedent, and the record-ts-field fallback is a
  localized produce-path change.
- **KIP-914 update-semantics fidelity.** The out-of-order `Change` emission /
  latest-pointer rules are intricate. Mitigation: the behavioral golden (gate 3)
  surfaces any divergence as a sequence mismatch on captured JVM output.
- **Behavioral-capture determinism.** The `TopologyTestDriver` dump must be
  deterministic (fixed input, no wall-clock punctuation) to be a stable golden —
  same constraint the sliding-window slice already solved and reuses.

## 10. Files touched

New:
- `crates/client-streams/src/store/versioned.rs`
- `crates/client-streams/src/store/versioned_schema.rs`
- `crates/client-streams/tests/testdata/golden/dsl/versioned_table.topology.json`
- `crates/client-streams/tests/testdata/golden/dsl/behavioral/versioned_table.json`
- `crates/client-streams/tests/testdata/golden/dsl/behavioral/versioned_changelog.json`

Modified:
- `src/store/registry.rs`, `src/store/iq.rs`, `src/store/mod.rs`
- `src/dsl/processors/table.rs`
- `src/dsl/config.rs`, `src/dsl/builder.rs`
- `src/topology/builder.rs`
- `src/dsl/mod.rs`, `src/lib.rs` (re-exports + module doc)
- `tests/jvm-capture/{src/main/java/crabka/capture/Capture.java, run.sh,
  build.gradle}` (structural fixture + behavioral/changelog runner)
- `tests/dsl_golden_frame.rs`, `tests/dsl_execution.rs` (assertions)
