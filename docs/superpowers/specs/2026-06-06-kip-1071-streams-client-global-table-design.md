# KIP-1071 streams client — GlobalKTable + stream-globaltable join (with real global consumer)

**Status:** design approved (brainstorm)
**Builds on:** #4 DSL (stateless + aggregations + KTable/KStream joins + windowing + suppress). Branches from `main` (independent of the suppress work on PR #412; rebase onto the post-#412 main when that merges).
**Ground truth:** Apache Kafka Streams 4.1 (Docker JVM capture) for byte-exact wire topology; KIP-1071 `StreamsGroupHeartbeatRequest.Topology` for the wire shape.

## 1. Goal

Add `GlobalKTable` and the **stream–globaltable join** to the DSL, **including the real-runtime global consumer** that materializes the global store from all partitions of the source topic.

A `GlobalKTable` is a fully-replicated table: every application instance reads **all** partitions of the source topic and holds the complete table locally. It is a **join target**, not a transform source — no aggregations, no `to_stream`. A `KStream` joins it by an **arbitrary lookup key** derived from each stream record (not the stream's key), so there is **no repartition and no copartition**.

## 2. Scope

### In scope
1. **`GlobalKTable<K,V>`** handle + **`StreamsBuilder::global_table(topic, Consumed, Materialized)`**.
2. **`KStream::join` / `KStream::left_join`** against a `GlobalKTable` (inner + left; no outer — Kafka has none).
3. **`Topology::add_global_store`** (global source topic + global KV store, **no changelog**) + the wire representation, **pinned byte-exact by a Docker JVM capture**.
4. **`GlobalStateManager`** — one shared global store registry per instance.
5. **Global consumer** — bootstraps each global store from all partitions of its source topic (offset 0 → end-of-log) before tasks process, then keeps consuming for live updates.
6. **`ProcessorContext` global-store access** + the **`KStreamGlobalTableJoinProcessor`**.
7. Execution: TopologyTestDriver (global store populated by piping to the global topic) + a real-broker end-to-end test.

### Non-goals (deferred)
- Foreign-key KTable-KTable join (KIP-213), cogroup, `process`/`processValues`, record caching (KIP-328).
- Interactive queries over the global store (#6), EOS (#7).
- GlobalKTable as a source for further DSL ops (it is a join target only — matches Kafka).

## 3. DSL surface

```rust
// builder
pub fn global_table<K, V, KS, VS>(
    &self, topic: impl Into<String>, consumed: Consumed<KS, VS>, materialized: Materialized<KS, VS>,
) -> GlobalKTable<K, V>;

// KStream<K, V>
pub fn join<GK, VG, VR, KM, J>(&self, global: &GlobalKTable<GK, VG>, key_mapper: KM, joiner: J) -> KStream<K, VR>
where KM: Fn(&K, &V) -> GK + ..., J: Fn(&V, &VG) -> VR + ...;        // inner: emit on hit only

pub fn left_join<GK, VG, VR, KM, J>(&self, global: &GlobalKTable<GK, VG>, key_mapper: KM, joiner: J) -> KStream<K, VR>
where KM: Fn(&K, &V) -> GK + ..., J: Fn(&V, Option<&VG>) -> VR + ...; // left: emit always (None on miss)
```

`GlobalKTable<K, V>` is a thin handle (builder `Rc` + logical node id + global store name). It carries the store name so a join can connect its processor to the global store. The lookup key `GK` may differ from both the stream key `K` and the global key — it is whatever `key_mapper` returns and must equal the global table's key type.

Method naming: `KStream::join`/`left_join` already exist for stream-stream (over `KStream` + `JoinWindows`); stream-table uses `join_table`/`left_join_table`. The JVM overloads `join`; Rust cannot, so the global variants are **`join_global` / `left_join_global`** (mirroring the existing `*_table` convention).

## 4. Topology + wire (capture-first)

`Topology::add_global_store<K, V, KS, VS>(store_name, source_name, topic, processor_name, consumed)` registers:
- a **global source** for `topic` (consumed with the given serdes),
- a **global KV store** — a `KeyValueBytesStore<K, V>` flagged **global** (no changelog topic; the source topic is the source of truth),
- a trivial **global-update processor** that writes each consumed record into the store (`put(k, v)`), mirroring the JVM's auto-generated global processor.

**Wire shape — the central unknown.** The KIP-1071 `StreamsGroupHeartbeatRequest.Topology` is `{ epoch, subtopologies[] }`; each `Subtopology` is `{ id, source_topics, source_topic_regex, state_changelog_topics, repartition_sink_topics, repartition_source_topics, copartition_groups }`. There is **no dedicated global-stores field**, so the JVM must encode the global store through `subtopologies` (most likely a dedicated subtopology whose `source_topics` is the global topic, with empty `state_changelog_topics`). **This is resolved capture-first**: capture the JVM topology for `globalTable(...).join(...)` BEFORE building the Rust wire, then match it byte-for-byte. The `NodeRegistry`/grouping/wire layers gain a minimal "global source/store" concept sized to whatever the capture shows.

All 14 existing goldens must stay **byte-identical**.

## 5. Runtime — Approach A (Kafka-faithful)

- **`GlobalStateManager`** — owns the instantiated global stores in a shared registry (`Arc`), one per `KafkaStreamsApp`. Distinct from the per-task `StoreRegistry`.
- **Global consumer** (a `GlobalStreamThread`-equivalent) — for each global store, reads **all partitions** of its source topic from offset 0 to end-of-log (bootstrap), deserializing and `put`-ing each record into the store; then keeps consuming to apply live updates. Bootstrap completes **before** the per-task processing path runs (Kafka blocks task start until the global store is ready).
- **`ProcessorContext`** gains read access to the shared global registry (in addition to the per-task one); the join processor fetches `get_global_kv_store::<GK, VG>(name)`.
- **`KStreamGlobalTableJoinProcessor<K, V, GK, VG, VR>`** — per `Record<K, V>`: `gk = key_mapper(&k, &v)`; look up the global store; **inner** → emit `joiner(&v, &vg)` on hit (keep stream key `K`, stream timestamp), skip on miss; **left** → emit `joiner(&v, opt)` always. Output `Record<K, VR>`. No `Change` — the join is stream-driven and the global side is a plain value lookup.

The global store reuses `KeyValueBytesStore<K, V>` but is registered through a global path that (a) emits no changelog topic in the wire and (b) is bootstrapped from the source topic rather than restored from a changelog.

## 6. Data flow

```
global_table("g", Consumed, Materialized)
   → global source "g"  +  global store "g-store" (no changelog)  +  global-update processor
runtime: GlobalStateManager + global consumer bootstrap "g-store" from ALL partitions of "g", then keep updated
stream.join_global(globalTable, key_mapper, joiner)
   → KStreamGlobalTableJoinProcessor connected to "g-store"
per stream record (k, v): gk = key_mapper(k, v) → store.get(gk) → joiner → emit (k, vr)
```

## 7. Slice decomposition (execution phases)

One feature, one spec, a phased plan, finished as one PR (split into stacked PRs only if the diff grows unwieldy):

- **G-i — DSL + topology + golden + TestDriver.** `GlobalKTable`, `global_table`, `add_global_store`, the global KV store + global-update processor, the wire golden (capture-first), the inner+left join processors, and TopologyTestDriver execution (global store populated in-process by piping records to the global topic). No real consumer yet — the TestDriver exercises the join against a populated store.
- **G-ii — real global consumer.** `GlobalStateManager` (shared), the global consumer (bootstrap-all-partitions + live updates), `ProcessorContext` global-store wiring in the real `KafkaStreamsApp`/runtime path, and a real-broker end-to-end integration test.

## 8. Testing

- **Golden:** a `global_table_join` topology fixture, **byte-exact** vs Docker JVM Kafka Streams 4.1 (`globalTable(...).join(...)`). All 14 prior goldens stay byte-identical.
- **TopologyTestDriver execution:** inner join hit (emit `joiner`) + miss (skip); left join hit (`joiner(v, Some)`) + miss (`joiner(v, None)`); key-mapper that maps to a key **different** from the stream key; a mid-stream global-topic update reflected in a later lookup.
- **GlobalStateManager bootstrap:** reading all partitions of the global topic populates the global store; a join processor sees the bootstrapped data.
- **Broker e2e:** produce to the global topic + the stream topic → assert the join output (real `KafkaStreamsApp` with the global consumer).

## 9. Error handling

- Inner join lookup miss → no output; left join miss → `joiner(v, None)`.
- Real runtime: task processing blocks until the global store finishes bootstrapping (Kafka semantics — no joins against a half-built table).
- `global_table` requires both `Consumed` and `Materialized` serdes (the store needs serdes; the source needs deserializers).

## 10. Open question resolved capture-first

How a global store/source appears in the KIP-1071 `StreamsGroupHeartbeat` topology (own subtopology vs. another encoding). G-i's first task captures the JVM topology and derives the wire shape before any Rust wiring is written.
