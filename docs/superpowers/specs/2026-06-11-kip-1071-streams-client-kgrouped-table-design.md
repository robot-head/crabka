# KIP-1071 Streams Client — `KTable.groupBy` / `KGroupedTable` (table aggregation)

> **Status:** design approved 2026-06-11. Single self-contained client-side DSL
> slice. Ground truth = empirical Kafka-Streams 4.1.0 Docker capture, replayed
> byte-for-byte.

## 1. Goal

Add the **table re-grouping + aggregation** path to the `crabka-client-streams`
DSL: `KTable<K,V>.group_by(mapper) -> KGroupedTable<KR,VR>` with `count`,
`reduce(adder, subtractor)`, and `aggregate(init, adder, subtractor)` producing a
materialized `KTable<KR, T>`.

This is the last major missing aggregation surface. KStream grouping/aggregation
(`KGroupedStream`: count/reduce/aggregate, windowed/session/sliding, cogroup) is
fully built; the **table** equivalent is absent. Its defining semantic is the
**subtractor**: because a `KTable` propagates a `Change<old,new>` change-stream,
re-grouping must *remove* the old value's contribution from its (old) group and
*add* the new value's contribution to its (new) group. A stream aggregation has
no subtractor, so it cannot model this — re-running an updated row would
double-count.

## 2. Scope

In scope (one slice):

- `KTable::group_by` + `group_by_explicit` (with `Grouped` serdes).
- `KGroupedTable<KR,VR>` handle with `count[_explicit]`, `reduce[_explicit]`,
  `aggregate[_explicit]`.
- The `KTABLE-SELECT` repartition-map processor and the `KTABLE-AGGREGATE`
  subtract-then-add processor.
- A `Changed` value serde for the repartition topic (carries `Change<VR>`).
- Goldens for all three terminals, including the key-change split and a
  downstream tombstone-subtract case.

Out of scope (YAGNI):

- **Windowed table aggregation** — the JVM `KGroupedTable` has no `windowedBy`;
  not a gap.
- **Source-level null tombstones** — a DSL-wide limitation (`graph.pipe` /
  `Record.value` are non-`Option`, shared with the KV/versioned `KTableSource`).
  The source battery stays tombstone-free; the subtractor's delete path is
  covered via a *downstream* tombstone instead (§7).
- **Caching / record-cache suppression** of intermediate aggregation results.

## 3. Why a subtractor (the core semantic)

A `KTable` node forwards `Record<K, Change<V>>` where `Change { old, new }`
(`new == None` is a tombstone). `KGroupedTable` consumes that change-stream. For
each upstream change the aggregate must reach the same result the JVM would:

```
agg = store.get(kr).unwrap_or_else(init)
if change.old.is_some(): agg = subtractor(kr, change.old, agg)   // remove old
if change.new.is_some(): agg = adder(kr, change.new, agg)        // add new
store.put(kr, agg)
forward(kr, Change { old: prior_agg, new: agg })
```

**Subtract before add** — matches JVM `KTableAggregate`. When the grouping key is
unchanged this nets the delta in one group; when the grouping key *changes*, the
old and new contributions land in *different* groups (handled by the
repartition-map below).

## 4. Architecture — triplet + lowering

Follows the established `KGroupedStream` lowering (`dsl/kgrouped.rs`), with two
differences driven by the change-stream input.

### 4.1 `group_by` records no node

`KTable::group_by[_explicit]` records **no** graph node. It captures the mapper
`(&K,&V) -> (KR,VR)`, the `Grouped` key/value serdes (for the repartition topic),
and the upstream lineage — exactly as `KGroupedStream`'s `group_by_key` does.
Returns `KGroupedTable<KR,VR>`.

### 4.2 Terminal aggregation always repartitions

Unlike `KStream.groupByKey` (which can skip the repartition when the key is
unchanged), **`KTable.groupBy` always repartitions** — the JVM always inserts the
repartition-map + sink + source. The terminal op lowers, in JVM counter order:

```
[upstream KTable node: forwards Change<V>]
  → KTABLE-SELECT       (RepartitionMap: Change<V> in → keyed Change<VR> out)
  → SINK                (serialize Change<VR> via Changed serde, keyed KR)
  → <app>-<store>-repartition          (internal repartition topic)
  → SOURCE              (deserialize Change<VR>)
  → KTABLE-AGGREGATE    (subtract-then-add → Change<T> out, + KV store)
  → [result KTable<KR, T>]
```

The repartition topic name is `<app_id>-<store_name>-repartition` (same rule as
the stream path: `mint_store_name` → `format!("{store}{REPARTITION_SUFFIX}")`).

### 4.3 Wire visibility

Per the cogroup precedent, the wire topology carries only **topic names, store
names, copartition groups, and changelog config** — *not* processor-node names.
So the `KTABLE-SELECT-`/`KTABLE-AGGREGATE-` prefixes do not affect golden bytes
when an explicit `Materialized` store name is used; they exist only to consume
the JVM auto-name counter at the right positions (so a *second* aggregation's
store lands at the same index as the JVM fixture). The store changelog is a
standard compacted KV changelog (`cleanup.policy=compact`), emitted iff
`Materialized::with_logging(true)` (default).

## 5. Components

| Unit | File | Responsibility |
|---|---|---|
| `KTable::group_by[_explicit]` | `dsl/ktable.rs` | Build `KGroupedTable`; capture mapper + serdes + lineage |
| `KGroupedTable<KR,VR>` | `dsl/kgrouped_table.rs` (new) | `count/reduce/aggregate[_explicit]`; mint store name; lower SELECT → repartition → AGGREGATE |
| `KTableRepartitionMapProcessor` | `dsl/processors/table_aggregate.rs` (new) | `Change<V>` → keyed `Change<VR>` with key-change split |
| `KTableAggregateProcessor` | `dsl/processors/table_aggregate.rs` (new) | `Change<VR>` → `Change<T>`, subtract-then-add over the KV store |
| `Changed` serde | `processor/serde/` (new module) | (de)serialize `Change<VR>` for the repartition topic — **byte format captured empirically** |
| Node prefixes | `dsl/names.rs` | `KTABLE-SELECT-`, `KTABLE-AGGREGATE-` |
| Export | `dsl/mod.rs` | Re-export `KGroupedTable` |

### 5.1 `KTableRepartitionMapProcessor`

On `Change<V>` at key `k`, map each present side through the user mapper:

```
old_pair = change.old.map(|v| mapper(&k, &v))   // Option<(KR,VR)>
new_pair = change.new.map(|v| mapper(&k, &v))
match (old_pair, new_pair) {
  (Some((ko,vo)), Some((kn,vn))) if ko == kn =>
      forward(kn, Change { old: Some(vo), new: Some(vn) }),
  _ => {
      if let Some((ko,vo)) = old_pair { forward(ko, Change { old: Some(vo), new: None }); }  // subtract-only
      if let Some((kn,vn)) = new_pair { forward(kn, Change { old: None, new: Some(vn) }); }   // add-only
  }
}
```

A tombstone (`change.new == None`) yields only the subtract-only record. Requires
`KR: PartialEq` (to detect the same-key fast path) — JVM compares the mapped keys
likewise.

### 5.2 `KTableAggregateProcessor`

The §3 body, over the materialized KV store. Holds `init`, `adder`, `subtractor`
closures. Forwards `Change { old: Some(prior_agg_if_present), new: agg }`. `count`
supplies `||0i64`, `|_,_,a| a+1`, `|_,_,a| a-1`; `reduce` supplies an
`init`-less adder/subtractor over `V` (first value seeds: with no prior store
entry and an add-only change, `agg = new`).

### 5.3 `Changed` serde — empirical byte format

The repartition-topic value is `Change<VR>` where **both** sides may be non-null
(the same-key fast path) or exactly one (the split path). The JVM serializes this
with `ChangedSerializer`/`ChangedDeserializer`. **Do not assume the framing** —
the versioned-tables slice proved a recalled changelog format wrong. The format
is pinned by capturing the live repartition-topic bytes (kafka-tap / Docker
harness, §7) and matching them exactly. Gated by a Rust round-trip self-test
**and** a JVM-bytes golden.

## 6. Public API sketch

```rust
impl<K, V, KS, VS> KTable<K, V, KS, VS> {
    pub fn group_by<KR, VR, M>(&self, mapper: M) -> KGroupedTable<KR, VR>
    where M: Fn(&K, &V) -> (KR, VR) + Clone + Send + Sync + 'static, /* + DefaultSerde bounds */;

    pub fn group_by_explicit<KR, VR, GKS, GVS, M>(&self, mapper: M, grouped: Grouped<GKS, GVS>)
        -> KGroupedTable<KR, VR> /* … */;
}

impl<KR, VR> KGroupedTable<KR, VR> {
    pub fn count(self, store: impl Into<String>) -> KTable<KR, i64, /*…*/>;
    pub fn reduce<Add, Sub>(self, adder: Add, subtractor: Sub, store: impl Into<String>)
        -> KTable<KR, VR, /*…*/>;
    pub fn aggregate<T, I, Add, Sub>(self, init: I, adder: Add, subtractor: Sub, store: impl Into<String>)
        -> KTable<KR, T, /*…*/>;
    // …_explicit variants taking Materialized<KS,VS>, mirroring kgrouped.rs.
}
```

(`reduce`'s adder/subtractor are `Fn(&VR,&VR)->VR`; `aggregate`'s are
`Fn(&KR,&VR,T)->T`, matching the JVM `Aggregator`/`Reducer` shapes already used
by `KGroupedStream`.)

## 7. Verification — capture-first

1. **JVM fixture.** `KGroupedTableTopology.java` under
   `crates/client-streams/tests/jvm-capture/`, building
   `table.groupBy(mapper).{count,reduce,aggregate}(…, Materialized.as("store"))`
   with explicit store names. Run against `mirror.gcr.io/apache/kafka:4.1.0` (single-broker
   Streams capture works on Mac — the emit-final precedent).
2. **Topology golden.** Capture the wire topology; assert the repartition topic
   name, store name, changelog name/config, and copartition wiring match the
   Rust-built `BuiltTopology` byte-for-byte.
3. **`Changed` bytes golden.** Capture the repartition-topic record bytes; pin the
   `Changed` serde against them.
4. **Behavioral golden.** Capture output records for a battery covering:
   - simple per-group counts/sums (no key change);
   - a **grouping-key change** (old row's value subtracted from group A, new
     value added to group B) — the discriminating case;
   - a **downstream tombstone** subtract: an upstream `KTable.filter` that drops a
     row emits a `Change { new: None }` *inside* the topology (no source null
     needed), exercising the subtract-only path;
   - `reduce` first-value-seeds.
   Replay byte-for-byte via `TopologyTestDriver::process`.
5. **CI gates.** `cargo fmt --check`, `cargo clippy --workspace --all-targets -D
   warnings`, full `cargo test`. The client-streams-integration job uses a
   catch-all `--tests` selector, so a new test binary is auto-covered.

## 8. Risks

- **`Changed` byte framing** (the one real fidelity risk) — mitigated by
  capture-first (§5.3, §7.3), never by recall.
- **Counter alignment** — the `KTABLE-SELECT`/`KTABLE-AGGREGATE`/filter-index
  positions must consume auto-name counters exactly as JVM, or a second
  aggregation's store name drifts. The topology golden catches this; the stream
  path's `record_repartition` already documents the null-key-filter index burn to
  mirror.
- **Erasure downcast** — like all DSL lowering, the mapper/agg closures are
  erased through dyn-Any thunks; a type mismatch surfaces at runtime, so the full
  golden suite is the gate.

## 9. Files touched

New:
- `crates/client-streams/src/dsl/kgrouped_table.rs`
- `crates/client-streams/src/dsl/processors/table_aggregate.rs`
- `crates/client-streams/src/processor/serde/changed.rs` (or sibling of existing serdes)
- `crates/client-streams/tests/jvm-capture/.../KGroupedTableTopology.java`
- `crates/client-streams/tests/testdata/kgrouped_table/*.json`
- `crates/client-streams/tests/kgrouped_table_golden.rs`

Modified:
- `crates/client-streams/src/dsl/ktable.rs` (`group_by[_explicit]`)
- `crates/client-streams/src/dsl/mod.rs` (export)
- `crates/client-streams/src/dsl/names.rs` (`KTABLE-SELECT-`, `KTABLE-AGGREGATE-`)
