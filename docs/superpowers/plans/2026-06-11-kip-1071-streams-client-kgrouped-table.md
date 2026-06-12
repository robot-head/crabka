# KGroupedTable (`KTable.groupBy` table aggregation) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `KTable<K,V>.group_by(mapper) -> KGroupedTable<KR,VR>` with `count` / `reduce(adder, subtractor)` / `aggregate(init, adder, subtractor)`, producing a materialized `KTable<KR,T>` — the table re-grouping/aggregation path (the subtractor is the defining semantic).

**Architecture:** Mirror the proven `KGroupedStream` lowering (`dsl/kgrouped.rs`). `group_by` records no node; the terminal op lowers `KTABLE-SELECT` (repartition-map: `Change<V>` → keyed `Change<VR>`) → a `Change`-carrying repartition (`sink → <app>-<store>-repartition → source`) → `KTABLE-AGGREGATE` (subtract-then-add over a KV store). A new `Changed` serde carries `Change<VR>` on the repartition topic; its byte framing is **captured first** from the real JVM `ChangedSerializer`. Ground truth = empirical Kafka-Streams 4.1.0 capture, replayed byte-for-byte.

**Tech Stack:** Rust 2024, `async-trait`, `tokio`, `bytes`; crate `crabka-client-streams`. Tests: `cargo nextest` / `cargo test -p crabka-client-streams`. JVM capture: Java harness under `crates/client-streams/tests/jvm-capture/`.

---

## Background the engineer must know

- **The triplet pattern.** A stateful DSL feature = a DSL surface + a processor + a store(+codec). Here: a `KGroupedTable` handle + two processors (`KTableRepartitionMapProcessor`, `KTableAggregateProcessor`) + the existing KV store + a new `Changed` value serde for the repartition topic.
- **`Change<V>`** (`dsl/processors/change.rs`): `pub(crate) struct Change<V> { pub old: Option<V>, pub new: Option<V> }`, `new == None` is a tombstone. Constructors: `Change::update(old: Option<V>, new: V)`, `Change::tombstone(old: Option<V>)`, plus `Change { old, new }` literal. Every `KTable` node forwards `Record<K, Change<V>>`; **state stores hold `V`**, only the inter-node value is wrapped.
- **Subtractor semantics (the whole point).** On each upstream `Change<V>` the table aggregate computes `agg = store.get(kr) ?? init()`, then **subtract old first, then add new**, `store.put`, forward `Change { old: prior, new: agg }`. A grouping-key change routes the subtract and the add to *different* groups (the repartition-map splits them). A stream aggregate has no subtractor and would double-count — that is why this is a distinct path.
- **`group_by` always repartitions.** Unlike `KStream.groupByKey` (which can skip), the JVM `KTable.groupBy` **always** inserts repartition-map + sink + source. So there is no "key-unchanged" fast path here.
- **Wire visibility (cogroup precedent).** The wire topology carries only topic names, store names, copartition groups, and changelog config — **not** processor-node names. So `KTABLE-SELECT-`/`KTABLE-AGGREGATE-` prefixes affect no golden bytes when an explicit `Materialized` store name is used; they only consume the JVM auto-name counter so downstream store indices stay aligned. The wire-visible names are the **repartition topic** `<app>-<store>-repartition`, the **store**, and the **changelog** `<app>-<store>-changelog`.
- **Capture-first is mandatory.** The versioned-tables slice proved a *recalled* changelog format wrong. Do NOT hand-write the `Changed` byte format from memory — Task 1 dumps the exact bytes from the real `org.apache.kafka.streams.kstream.internals.ChangedSerializer`; Task 3 implements `Changed` to reproduce those bytes, gated by a hex golden.
- **Single-broker Streams capture works on Mac Docker** (the emit-final precedent). The `ChangedSerializer` byte dump needs no broker at all — it is a pure Java `main` that serializes sample `Change` objects.
- **Greenfield (per CLAUDE.md):** no back-compat shims. Match Kafka byte-exactness; capture empirically when undocumented.
- **Codegen note:** this slice touches **no** protocol schemas, so `tools/regenerate.sh` is NOT involved.
- **CI gates:** `cargo fmt --check`, `cargo clippy --workspace --all-targets -D warnings`, full `cargo test`. Run all three before claiming done. Watch `clippy::pedantic` (workspace-wide warn→error): backtick identifiers in `//!`/`///` doc comments.

### Behavior contract (the tests encode these)

1. **aggregate:** `agg = store.get(kr) ?? init()`; if `change.old.is_some()` → `agg = subtractor(&kr, old, agg)`; if `change.new.is_some()` → `agg = adder(&kr, new, agg)`; `store.put(kr, agg)`; forward `Change { old: prior_store_value, new: Some(agg) }`. **Subtract before add.**
2. **count** = `aggregate(|| 0i64, |_,_,a| a+1, |_,_,a| a-1)`.
3. **reduce** = init-less over `VR`: first value seeds (`add` on empty store = the value); `add = |a, v| adder(a, v)`, `subtract = |a, v| subtractor(a, v)`. Result type stays `VR`.
4. **repartition-map:** map `change.old`→`(ko,vo)` and `change.new`→`(kn,vn)` through the user mapper. If both present and `ko == kn` → forward `(kn, Change { old: Some(vo), new: Some(vn) })`. Otherwise forward subtract-only `(ko, Change { old: Some(vo), new: None })` **and** add-only `(kn, Change { old: None, new: Some(vn) })`. A tombstone (`change.new == None`) yields only the subtract-only record.
5. **downstream tombstone:** an upstream `KTable.filter` that drops a row emits `Change { new: None }` *inside* the topology (no source null), exercising the subtractor's delete path.

### File Structure

| File | Responsibility | Tasks |
|------|----------------|-------|
| `tests/jvm-capture/src/main/java/crabka/capture/KGroupedTableBehavior.java` | JVM fixture: topology + behavioral + `ChangedSerializer` byte dump | 1 |
| `tests/jvm-capture/run.sh` | Register the new fixture | 1 |
| `tests/testdata/kgrouped_table/{behavior.json,changed_bytes.json}` + `testdata/golden/dsl/kgrouped_table.topology.json` | Goldens | 1, 7 |
| `src/dsl/names.rs` | `KTABLE-SELECT-`, `KTABLE-AGGREGATE-`, store prefixes | 2 |
| `src/processor/serde/changed.rs` (+ `serde.rs` re-export) | `Changed<S>: Serde<Change<V>>` | 3 |
| `src/dsl/processors/table_aggregate.rs` (+ `processors/mod.rs`) | `KTableRepartitionMapProcessor`, `KTableAggregateProcessor` | 4 |
| `src/dsl/kgrouped_table.rs` (+ `dsl/mod.rs`) | `KGroupedTable` handle + lowering + `repartition_lower_changed` | 5 |
| `src/dsl/ktable.rs` | `group_by` / `group_by_explicit` | 6 |
| `tests/kgrouped_table_golden.rs` | topology + behavioral replay | 7 |

### Batching (per CLAUDE.md parallel-batch execution)

- **Batch A (parallel):** Task 1 (jvm-capture/testdata) ‖ Task 2 (names.rs). Disjoint files.
- **Batch B (parallel, after A):** Task 3 (serde) ‖ Task 4 (processors). Disjoint files.
- **Batch C:** Task 5 (kgrouped_table.rs + dsl/mod.rs).
- **Batch D:** Task 6 (ktable.rs).
- **Batch E:** Task 7 (golden test).

---

## Task 1: JVM capture — topology + behavioral + `Changed` bytes

**Files:**
- Create: `crates/client-streams/tests/jvm-capture/src/main/java/crabka/capture/KGroupedTableBehavior.java`
- Modify: `crates/client-streams/tests/jvm-capture/run.sh` (register the fixture in its class list)
- Create (output): `crates/client-streams/tests/testdata/kgrouped_table/behavior.json`, `crates/client-streams/tests/testdata/kgrouped_table/changed_bytes.json`, `crates/client-streams/tests/testdata/golden/dsl/kgrouped_table.topology.json`

This task pins ground truth. It produces three goldens: the behavioral output, the `ChangedSerializer` byte samples, and (recorded by capture mechanism A) the wire topology.

- [ ] **Step 1: Write the Java fixture**

Mirror `CogroupBehavior.java`'s structure. The topology: a source `KTable` (`builder.table("in")`), an upstream `filter` to exercise the downstream tombstone, then `groupBy` re-keying by a derived key, then the three terminals into three output topics. Also dump `ChangedSerializer` byte samples.

Create `crates/client-streams/tests/jvm-capture/src/main/java/crabka/capture/KGroupedTableBehavior.java`:

```java
package crabka.capture;

import java.nio.file.*;
import java.util.*;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.ChangedSerializer;

/**
 * Behavioral + ChangedSerializer byte golden for KTable.groupBy / KGroupedTable.
 * Topology: table("in") -> filter(v > 0) -> groupBy(key = v % 2, value = v)
 *   -> count / reduce(sum, diff) / aggregate(0; +v; -v).
 * The filter exercises the downstream-tombstone subtract path (a row whose value
 * drops to <= 0 emits Change{new:null}).
 */
public final class KGroupedTableBehavior {
    public static void main(String[] args) throws Exception {
        Path out = Paths.get(args.length > 0 ? args[0] : "../testdata/kgrouped_table");
        Files.createDirectories(out);

        StreamsBuilder b = new StreamsBuilder();
        KTable<String, Long> src = b.table("in",
            Consumed.with(Serdes.String(), Serdes.Long()),
            Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("src-store")
                .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()));
        // Drop non-positive rows; a row updated from positive to non-positive emits
        // a tombstone downstream (the subtract path).
        KTable<String, Long> pos = src.filter((k, v) -> v > 0);

        KGroupedTable<String, Long> grouped = pos.groupBy(
            (k, v) -> KeyValue.pair(v % 2 == 0 ? "even" : "odd", v),
            Grouped.with(Serdes.String(), Serdes.Long()));

        grouped.count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("count-store")
                .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()))
            .toStream().to("count-out", Produced.with(Serdes.String(), Serdes.Long()));

        grouped.reduce((a, v) -> a + v, (a, v) -> a - v,
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("reduce-store")
                    .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()))
            .toStream().to("reduce-out", Produced.with(Serdes.String(), Serdes.Long()));

        grouped.aggregate(() -> 0L, (k, v, a) -> a + v, (k, v, a) -> a - v,
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("agg-store")
                    .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()))
            .toStream().to("agg-out", Produced.with(Serdes.String(), Serdes.Long()));

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        try (TopologyTestDriver d = new TopologyTestDriver(b.build(), props, java.time.Instant.ofEpochMilli(0))) {
            TestInputTopic<String, Long> in = d.createInputTopic(
                "in", Serdes.String().serializer(), Serdes.Long().serializer());
            TestOutputTopic<String, Long> countOut = d.createOutputTopic(
                "count-out", Serdes.String().deserializer(), Serdes.Long().deserializer());
            TestOutputTopic<String, Long> reduceOut = d.createOutputTopic(
                "reduce-out", Serdes.String().deserializer(), Serdes.Long().deserializer());
            TestOutputTopic<String, Long> aggOut = d.createOutputTopic(
                "agg-out", Serdes.String().deserializer(), Serdes.Long().deserializer());

            // Script: include a same-key update, a grouping-key change, and a
            // positive->non-positive update (downstream tombstone subtract).
            in.pipeInput("a", 2L, java.time.Instant.ofEpochMilli(0));  // a: even, +2
            in.pipeInput("b", 4L, java.time.Instant.ofEpochMilli(1));  // b: even, +4
            in.pipeInput("a", 6L, java.time.Instant.ofEpochMilli(2));  // a even->even: -2 +6
            in.pipeInput("c", 3L, java.time.Instant.ofEpochMilli(3));  // c: odd, +3
            in.pipeInput("b", 5L, java.time.Instant.ofEpochMilli(4));  // b even->odd: even -4, odd +5
            in.pipeInput("a", -1L, java.time.Instant.ofEpochMilli(5)); // a filtered out: even -6 (tombstone)

            StringBuilder sb = new StringBuilder("{\n");
            sb.append("  \"count\": ").append(dump(countOut)).append(",\n");
            sb.append("  \"reduce\": ").append(dump(reduceOut)).append(",\n");
            sb.append("  \"aggregate\": ").append(dump(aggOut)).append("\n}\n");
            Files.writeString(out.resolve("behavior.json"), sb.toString());
        }

        // ── ChangedSerializer byte samples (no broker / no driver needed) ──
        // Pin the exact repartition-topic value framing for Change<Long>.
        ChangedSerializer<Long> cs = new ChangedSerializer<>(Serdes.Long().serializer());
        Map<String, Change<Long>> samples = new LinkedHashMap<>();
        samples.put("both", new Change<>(6L, 2L));      // new=6, old=2 (same-key update)
        samples.put("new_only", new Change<>(5L, null)); // add-only (key-change add side)
        samples.put("old_only", new Change<>(null, 4L)); // subtract-only (key-change / tombstone)
        StringBuilder hb = new StringBuilder("{\n");
        int i = 0;
        for (Map.Entry<String, Change<Long>> e : samples.entrySet()) {
            byte[] bytes = cs.serialize("topic", e.getValue());
            hb.append("  \"").append(e.getKey()).append("\": \"").append(hex(bytes)).append("\"");
            hb.append(++i < samples.size() ? ",\n" : "\n");
        }
        hb.append("}\n");
        Files.writeString(out.resolve("changed_bytes.json"), hb.toString());
    }

    private static String dump(TestOutputTopic<String, Long> t) {
        StringBuilder sb = new StringBuilder("[");
        List<KeyValue<String, Long>> recs = t.readKeyValuesToList();
        for (int i = 0; i < recs.size(); i++) {
            KeyValue<String, Long> kv = recs.get(i);
            sb.append("{\"key\": \"").append(kv.key).append("\", \"value\": ").append(kv.value).append("}");
            if (i + 1 < recs.size()) sb.append(", ");
        }
        return sb.append("]").toString();
    }

    private static String hex(byte[] b) {
        StringBuilder sb = new StringBuilder();
        for (byte x : b) sb.append(String.format("%02x", x));
        return sb.toString();
    }
}
```

- [ ] **Step 2: Register the fixture in `run.sh`**

Open `crates/client-streams/tests/jvm-capture/run.sh`, find the list of behavioral capture classes (the other `*Behavior` classes — e.g. `CogroupBehavior`, `EmitFinalBehavior` — are invoked there). Add a `KGroupedTableBehavior` invocation alongside them, passing the output dir `../testdata/kgrouped_table` as its first arg, matching the exact pattern the sibling classes already use. Also ensure `KGroupedTableBehavior` is picked up by mechanism A's topology capture (the `Capture.java` reflection path) the same way the other DSL fixtures are registered for `testdata/golden/dsl/` — follow the existing per-fixture registration pattern verbatim.

- [ ] **Step 3: Run the capture**

Run (from `crates/client-streams/tests/jvm-capture/`): `./run.sh --gradle` (or `./run.sh --javac` if Docker/Gradle is unavailable — both are documented in `run.sh`).
Expected: writes `../testdata/kgrouped_table/behavior.json`, `../testdata/kgrouped_table/changed_bytes.json`, and `../testdata/golden/dsl/kgrouped_table.topology.json`.

If the capture host has no JVM/Docker, this task is the one place that requires it; do not fabricate the goldens — they are the spec. Record in the PR which capture mechanism was used.

- [ ] **Step 4: Sanity-check the goldens**

Run: `cat ../testdata/kgrouped_table/behavior.json ../testdata/kgrouped_table/changed_bytes.json`
Expected: `behavior.json` has `count`/`reduce`/`aggregate` arrays of `{key,value}` over groups `even`/`odd`; `changed_bytes.json` has hex strings for `both`/`new_only`/`old_only`. Eyeball that the `both` hex is longer than `new_only`/`old_only` (it carries two serialized longs).

- [ ] **Step 5: Commit**

```bash
git add crates/client-streams/tests/jvm-capture/src/main/java/crabka/capture/KGroupedTableBehavior.java \
        crates/client-streams/tests/jvm-capture/run.sh \
        crates/client-streams/tests/testdata/kgrouped_table/ \
        crates/client-streams/tests/testdata/golden/dsl/kgrouped_table.topology.json
git commit -m "test(client-streams): JVM KGroupedTable goldens (behavior + Changed bytes + topology)"
```

---

## Task 2: `names.rs` — `KTABLE-SELECT` / `KTABLE-AGGREGATE` prefixes

**Files:**
- Modify: `crates/client-streams/src/dsl/names.rs`

JVM `KGroupedTableImpl` uses `KTABLE-SELECT-` for the repartition-map node, `KTABLE-AGGREGATE-` for the aggregate processor, and `KTABLE-AGGREGATE-STATE-STORE-` / `KTABLE-REDUCE-STATE-STORE-` for unnamed stores. These are not wire-visible (explicit `Materialized` names are used in goldens) but the prefixes must exist so the lowering can mint nodes at the JVM counter positions.

- [ ] **Step 1: Add the prefixes**

In `crates/client-streams/src/dsl/names.rs`, after the existing `KTABLE_SUPPRESS_STORE` constant (line ~91), add:

```rust
/// JVM `KGroupedTableImpl` repartition-map (select) node prefix. Maps the
/// upstream `Change<V>` to the grouped `(KR, Change<VR>)` before the
/// repartition. Not wire-visible.
pub(crate) const KTABLE_SELECT: &str = "KTABLE-SELECT-";
/// JVM `KGroupedTableImpl` aggregate processor node prefix (subtract-then-add).
/// Not wire-visible.
pub(crate) const KTABLE_AGGREGATE: &str = "KTABLE-AGGREGATE-";
/// Store-name prefix for an unnamed `KGroupedTable::aggregate`/`count` result
/// store. Used only when `Materialized` carries no explicit name.
pub(crate) const KTABLE_AGGREGATE_STORE: &str = "KTABLE-AGGREGATE-STATE-STORE-";
/// Store-name prefix for an unnamed `KGroupedTable::reduce` result store.
pub(crate) const KTABLE_REDUCE_STORE: &str = "KTABLE-REDUCE-STATE-STORE-";
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo build -p crabka-client-streams`
Expected: builds (the constants are `pub(crate)`; unused-until-Task-5 is fine — they'll be referenced soon; if clippy flags dead_code in the interim, the `#[allow(dead_code)]` convention used by neighbors applies, but prefer leaving them un-allowed since Task 5 consumes them).

- [ ] **Step 3: Commit**

```bash
git add crates/client-streams/src/dsl/names.rs
git commit -m "feat(client-streams): KTABLE-SELECT/KTABLE-AGGREGATE name prefixes for KGroupedTable"
```

---

## Task 3: `Changed` serde — carry `Change<VR>` on the repartition topic

**Files:**
- Create: `crates/client-streams/src/processor/serde/changed.rs` (or, if `serde` is a single file `src/processor/serde.rs`, add a `mod`/section — match the existing layout; the Explore report shows `processor/serde.rs` as one file, so add the type there or as a sibling `serde_changed.rs` `mod`-included from it)
- Modify: the serde module's `mod`/`pub use` to export `Changed`
- Test: inline `#[cfg(test)]` in the same file

`Changed<S>` wraps an inner `Serde<VR>` and implements `Serde<Change<VR>>`, reproducing the JVM `ChangedSerializer` framing pinned by `testdata/kgrouped_table/changed_bytes.json`.

- [ ] **Step 1: Write the failing byte-golden test**

Add to the serde module (e.g. new file `src/processor/serde/changed.rs`, declared via `mod changed; pub use changed::Changed;` in the serde module):

```rust
use bytes::Bytes;

use crate::dsl::processors::change::Change;
use crate::processor::serde::{I64Serde, Serde, SerdeError};

/// Wraps an inner `Serde<V>` to (de)serialize a `Change<V>` for the table
/// repartition topic, byte-compatible with the JVM `ChangedSerializer`.
#[derive(Debug, Clone, Copy)]
pub struct Changed<S> {
    inner: S,
}

impl<S> Changed<S> {
    #[must_use]
    pub fn new(inner: S) -> Self {
        Self { inner }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::check;

    fn golden() -> serde_json::Value {
        let raw = std::fs::read_to_string(
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/testdata/kgrouped_table/changed_bytes.json"),
        )
        .expect("read changed_bytes golden");
        serde_json::from_str(&raw).unwrap()
    }

    fn hex(b: &[u8]) -> String {
        b.iter().map(|x| format!("{x:02x}")).collect()
    }

    #[test]
    fn changed_long_matches_jvm_bytes() {
        let g = golden();
        let s = Changed::new(I64Serde);
        let both = Change { old: Some(2i64), new: Some(6i64) };
        let new_only = Change { old: None, new: Some(5i64) };
        let old_only = Change { old: Some(4i64), new: None };
        check!(hex(&s.serialize("topic", &both)) == g["both"].as_str().unwrap());
        check!(hex(&s.serialize("topic", &new_only)) == g["new_only"].as_str().unwrap());
        check!(hex(&s.serialize("topic", &old_only)) == g["old_only"].as_str().unwrap());
    }

    #[test]
    fn changed_round_trips() {
        let s = Changed::new(I64Serde);
        for c in [
            Change { old: Some(2i64), new: Some(6i64) },
            Change { old: None, new: Some(5i64) },
            Change { old: Some(4i64), new: None },
        ] {
            let bytes = s.serialize("topic", &c);
            let back: Change<i64> = s.deserialize("topic", &bytes).unwrap();
            check!(back == c);
        }
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p crabka-client-streams --lib changed_long_matches_jvm_bytes`
Expected: FAIL — `Serde<Change<i64>>` is not implemented for `Changed<I64Serde>` (no `serialize`/`deserialize`).

- [ ] **Step 3: Implement `Serde<Change<V>>` for `Changed<S>`**

Read `testdata/kgrouped_table/changed_bytes.json` to confirm the exact framing, then implement to reproduce it. The JVM `ChangedSerializer` layout is: `[newData bytes][oldData bytes][newDataLength: u32 BE][flag: 1 byte]`, where `newDataLength == 0` when `new` is absent, and the trailing flag byte encodes which sides are present (and `isLatest`, always set for non-versioned). The deserializer reads the flag (last byte) and the `u32` length (preceding 4 bytes), then splits the remaining prefix at `newDataLength` into new / old. **Confirm the flag-bit values and isLatest byte against the captured hex** — the golden is authoritative; adjust the `FLAG_*` constants below to the observed values if they differ.

Add to `Changed<S>`:

```rust
// JVM ChangedSerializer trailing-flag bits (verify against changed_bytes.json).
const FLAG_IS_LATEST: u8 = 0x80;
const FLAG_NEW_PRESENT: u8 = 0x02;
const FLAG_OLD_PRESENT: u8 = 0x01;

impl<V, S> Serde<Change<V>> for Changed<S>
where
    V: Send + Sync + 'static,
    S: Serde<V>,
{
    fn serialize(&self, topic: &str, value: &Change<V>) -> Bytes {
        let new_bytes = value.new.as_ref().map(|v| self.inner.serialize(topic, v));
        let old_bytes = value.old.as_ref().map(|v| self.inner.serialize(topic, v));
        let new_len = new_bytes.as_ref().map_or(0usize, |b| b.len());
        let mut buf = Vec::new();
        if let Some(nb) = &new_bytes {
            buf.extend_from_slice(nb);
        }
        if let Some(ob) = &old_bytes {
            buf.extend_from_slice(ob);
        }
        buf.extend_from_slice(&(new_len as u32).to_be_bytes());
        let mut flag = FLAG_IS_LATEST;
        if new_bytes.is_some() {
            flag |= FLAG_NEW_PRESENT;
        }
        if old_bytes.is_some() {
            flag |= FLAG_OLD_PRESENT;
        }
        buf.push(flag);
        Bytes::from(buf)
    }

    fn deserialize(&self, topic: &str, bytes: &[u8]) -> Result<Change<V>, SerdeError> {
        if bytes.len() < 5 {
            return Err(SerdeError("Changed: buffer too short".into()));
        }
        let flag = bytes[bytes.len() - 1];
        let len_start = bytes.len() - 5;
        let new_len = u32::from_be_bytes(bytes[len_start..len_start + 4].try_into().unwrap()) as usize;
        let data = &bytes[..len_start];
        let has_new = flag & FLAG_NEW_PRESENT != 0;
        let has_old = flag & FLAG_OLD_PRESENT != 0;
        let new = if has_new {
            Some(self.inner.deserialize(topic, &data[..new_len])?)
        } else {
            None
        };
        let old = if has_old {
            Some(self.inner.deserialize(topic, &data[new_len..])?)
        } else {
            None
        };
        Ok(Change { old, new })
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p crabka-client-streams --lib changed_`
Expected: both `changed_long_matches_jvm_bytes` and `changed_round_trips` PASS. If the byte test fails, diff your output hex against the golden and adjust the `FLAG_*` constants / field order to match the captured framing (the golden is the spec).

- [ ] **Step 5: Commit**

```bash
git add crates/client-streams/src/processor/serde*
git commit -m "feat(client-streams): Changed serde (JVM ChangedSerializer byte-compat)"
```

---

## Task 4: processors — repartition-map + table-aggregate

**Files:**
- Create: `crates/client-streams/src/dsl/processors/table_aggregate.rs`
- Modify: `crates/client-streams/src/dsl/processors/mod.rs` (add `pub(crate) mod table_aggregate;`)
- Test: inline `#[cfg(test)]` in `table_aggregate.rs`

Two processors. `KTableRepartitionMapProcessor` maps `Change<V>` → keyed `Change<VR>` (splitting on key change). `KTableAggregateProcessor` consumes `Change<VR>` and applies subtract-then-add over a KV store.

- [ ] **Step 1: Write the failing tests**

Create `crates/client-streams/src/dsl/processors/table_aggregate.rs`. Mirror the harness from `aggregate.rs`/`table.rs` (`Dispatch`, `ProcessorContext`, `buffer.pop_front`).

```rust
//! KGroupedTable processors (`KTable.groupBy` aggregation).
//!
//! - `KTableRepartitionMapProcessor`: `Change<V>` in → keyed `Change<VR>` out.
//!   Maps each present side of the change through the user mapper; on a
//!   grouping-key change it forwards a subtract-only record to the old key and
//!   an add-only record to the new key.
//! - `KTableAggregateProcessor`: `Change<VR>` in → `Change<T>` out. Subtracts the
//!   old value's contribution then adds the new value's, over a #3 KV store.

use std::marker::PhantomData;

use async_trait::async_trait;

use crate::dsl::processors::change::Change;
use crate::processor::api::{Processor, ProcessorContext};
use crate::processor::record::Record;

type Marker<T> = PhantomData<fn() -> T>;

/// Maps the upstream `Change<V>` to the grouped key/value, splitting a
/// grouping-key change into a subtract-only (old key) and add-only (new key)
/// record so the downstream aggregate nets the change in the right groups.
#[allow(dead_code)]
pub(crate) struct KTableRepartitionMapProcessor<K, V, KR, VR, M> {
    pub mapper: M,
    pub _pd: Marker<(K, V, KR, VR)>,
}

#[async_trait]
impl<K, V, KR, VR, M> Processor<K, Change<V>, KR, Change<VR>>
    for KTableRepartitionMapProcessor<K, V, KR, VR, M>
where
    K: std::any::Any + Send + Sync + Clone,
    V: Send + 'static,
    KR: std::any::Any + Send + Sync + Clone + PartialEq,
    VR: std::any::Any + Send + Clone,
    M: Fn(&K, &V) -> (KR, VR) + Send + 'static,
{
    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, KR, Change<VR>>,
        r: Record<K, Change<V>>,
    ) {
        let key = r.key.expect("KGroupedTable map requires a non-null key");
        let ts = r.timestamp;
        let old_pair = r.value.old.as_ref().map(|v| (self.mapper)(&key, v));
        let new_pair = r.value.new.as_ref().map(|v| (self.mapper)(&key, v));
        match (old_pair, new_pair) {
            (Some((ko, vo)), Some((kn, vn))) if ko == kn => {
                ctx.forward(Record::new(Some(kn), Change { old: Some(vo), new: Some(vn) }, ts));
            }
            (old_pair, new_pair) => {
                if let Some((ko, vo)) = old_pair {
                    ctx.forward(Record::new(Some(ko), Change { old: Some(vo), new: None }, ts));
                }
                if let Some((kn, vn)) = new_pair {
                    ctx.forward(Record::new(Some(kn), Change { old: None, new: Some(vn) }, ts));
                }
            }
        }
    }
}

/// Subtract-then-add table aggregation over a #3 `KeyValueStore` keyed `KR`,
/// holding the accumulator `T`. `init` seeds an empty group; `subtractor`
/// removes the old value's contribution; `adder` adds the new value's.
#[allow(dead_code)]
pub(crate) struct KTableAggregateProcessor<KR, VR, T, I, Add, Sub> {
    pub store_name: String,
    pub init: I,
    pub adder: Add,
    pub subtractor: Sub,
    pub _pd: Marker<(KR, VR, T)>,
}

#[async_trait]
impl<KR, VR, T, I, Add, Sub> Processor<KR, Change<VR>, KR, Change<T>>
    for KTableAggregateProcessor<KR, VR, T, I, Add, Sub>
where
    KR: std::any::Any + Send + Sync + Clone,
    VR: Send + 'static,
    T: std::any::Any + Send + Clone,
    I: Fn() -> T + Send + 'static,
    Add: Fn(&KR, &VR, T) -> T + Send + 'static,
    Sub: Fn(&KR, &VR, T) -> T + Send + 'static,
{
    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, KR, Change<T>>,
        r: Record<KR, Change<VR>>,
    ) {
        let key = r.key.expect("KGroupedTable aggregate requires a non-null key");
        let (old, new) = {
            let store = ctx
                .get_state_store::<KR, T>(&self.store_name)
                .expect("KGroupedTable aggregate store not found");
            let prior = store.get(&key).await;
            let mut agg = prior.clone().unwrap_or_else(|| (self.init)());
            if let Some(ov) = &r.value.old {
                agg = (self.subtractor)(&key, ov, agg);
            }
            if let Some(nv) = &r.value.new {
                agg = (self.adder)(&key, nv, agg);
            }
            store.put(key.clone(), agg.clone()).await;
            (prior, agg)
        };
        ctx.forward(Record::new(Some(key), Change::update(old, new), r.timestamp));
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use assert2::check;

    use super::*;
    use crate::processor::api::ProcessorContext;
    use crate::processor::erased::{Dispatch, ErasedRecord};
    use crate::processor::record::RecordContext;
    use crate::processor::serde::{I64Serde, StringSerde};
    use crate::store::kv::KeyValueBytesStore;
    use crate::store::registry::StoreRegistry;

    fn rc() -> RecordContext {
        RecordContext { topic: "in".into(), partition: 0, offset: 0, timestamp: 0 }
    }

    fn agg_stores() -> StoreRegistry {
        let mut s = StoreRegistry::default();
        s.insert(Box::new(KeyValueBytesStore::<String, i64>::in_memory(
            "agg".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "app-agg-changelog".into(),
        )));
        s
    }

    #[tokio::test]
    async fn map_splits_on_key_change() {
        // mapper: key = value parity ("even"/"odd"), value passthrough.
        let mapper = |_k: &String, v: &i64| {
            (if v % 2 == 0 { "even".to_string() } else { "odd".to_string() }, *v)
        };
        let mut stores = StoreRegistry::default();
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = rc();
        let mut proc = KTableRepartitionMapProcessor {
            mapper,
            _pd: PhantomData::<fn() -> (String, i64, String, i64)>,
        };
        // old=4 (even), new=5 (odd): expect a subtract-only to "even" and an
        // add-only to "odd".
        {
            let globals = crate::runtime::global::GlobalStateManager::default();
            let mut scheds = Vec::new();
            let mut dispatch = Dispatch {
                buffer: &mut buffer, children: &children, output: &mut output,
                record_ctx: &rc, stores: &mut stores, globals: &globals,
                node_idx: 0, schedules: &mut scheds,
                sched_stream_time: i64::MIN, sched_wall_clock: 0,
            };
            let mut ctx = ProcessorContext::<'_, '_, String, Change<i64>>::new(&mut dispatch);
            proc.process(&mut ctx, Record::new(Some("b".into()), Change::update(Some(4i64), 5i64), 0)).await;
        }
        let (_, r1) = buffer.pop_front().unwrap();
        check!(*r1.key.unwrap().downcast::<String>().unwrap() == "even".to_string());
        let c1 = r1.value.downcast::<Change<i64>>().unwrap();
        check!(c1.old == Some(4) && c1.new.is_none());
        let (_, r2) = buffer.pop_front().unwrap();
        check!(*r2.key.unwrap().downcast::<String>().unwrap() == "odd".to_string());
        let c2 = r2.value.downcast::<Change<i64>>().unwrap();
        check!(c2.old.is_none() && c2.new == Some(5));
        check!(buffer.is_empty());
    }

    #[tokio::test]
    async fn aggregate_subtracts_then_adds() {
        let mut stores = agg_stores();
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = rc();
        let mut proc = KTableAggregateProcessor {
            store_name: "agg".to_string(),
            init: || 0i64,
            adder: |_k: &String, v: &i64, a: i64| a + v,
            subtractor: |_k: &String, v: &i64, a: i64| a - v,
            _pd: PhantomData::<fn() -> (String, i64, i64)>,
        };
        // Seed: add 2 to "even".
        run(&mut proc, &mut stores, &mut buffer, &rc, "even", Change { old: None, new: Some(2) }).await;
        let c = pop(&mut buffer);
        check!(c.old.is_none() && c.new == Some(2));
        // Same-key update old=2 new=6: subtract 2 then add 6 → 6.
        run(&mut proc, &mut stores, &mut buffer, &rc, "even", Change { old: Some(2), new: Some(6) }).await;
        let c = pop(&mut buffer);
        check!(c.old == Some(2) && c.new == Some(6));
        // Subtract-only (downstream tombstone) old=6: 6 - 6 = 0.
        run(&mut proc, &mut stores, &mut buffer, &rc, "even", Change { old: Some(6), new: None }).await;
        let c = pop(&mut buffer);
        check!(c.old == Some(6) && c.new == Some(0));
        check!(stores.get_kv::<String, i64>("agg").unwrap().get(&"even".to_string()).await == Some(0));
    }

    // Helpers keep each Dispatch block out of the assertions.
    async fn run<I, Add, Sub>(
        proc: &mut KTableAggregateProcessor<String, i64, i64, I, Add, Sub>,
        stores: &mut StoreRegistry,
        buffer: &mut VecDeque<(usize, ErasedRecord)>,
        rc: &RecordContext,
        key: &str,
        change: Change<i64>,
    ) where
        I: Fn() -> i64 + Send + 'static,
        Add: Fn(&String, &i64, i64) -> i64 + Send + 'static,
        Sub: Fn(&String, &i64, i64) -> i64 + Send + 'static,
    {
        let children = [0usize];
        let mut output = Vec::new();
        let globals = crate::runtime::global::GlobalStateManager::default();
        let mut scheds = Vec::new();
        let mut dispatch = Dispatch {
            buffer, children: &children, output: &mut output, record_ctx: rc,
            stores, globals: &globals, node_idx: 0, schedules: &mut scheds,
            sched_stream_time: i64::MIN, sched_wall_clock: 0,
        };
        let mut ctx = ProcessorContext::<'_, '_, String, Change<i64>>::new(&mut dispatch);
        proc.process(&mut ctx, Record::new(Some(key.to_string()), change, 0)).await;
    }

    fn pop(buffer: &mut VecDeque<(usize, ErasedRecord)>) -> Change<i64> {
        let (_, rec) = buffer.pop_front().expect("expected a forwarded record");
        *rec.value.downcast::<Change<i64>>().unwrap()
    }
}
```

- [ ] **Step 2: Wire the module + run to verify it fails**

Add to `crates/client-streams/src/dsl/processors/mod.rs`: `pub(crate) mod table_aggregate;` (next to the other `pub(crate) mod` lines).

Run: `cargo test -p crabka-client-streams --lib table_aggregate`
Expected: FAIL to COMPILE first time only if the harness types differ; otherwise the two tests run and FAIL (processors not yet correct) — but since Step 1 includes the full impls, the expected first failure is a *compile* error only if `ErasedRecord::key`/`downcast` differ from the assumed API. If `r1.key` is not an `Option<Box<dyn Any>>` with `.downcast()`, adjust the key-assertion to the actual `ErasedRecord` key accessor (grep `erased.rs` for the key field/type). Expected after compile: PASS (the impls are complete) — proceed to Step 3.

- [ ] **Step 3: Run the tests to verify they pass**

Run: `cargo test -p crabka-client-streams --lib table_aggregate`
Expected: `map_splits_on_key_change` and `aggregate_subtracts_then_adds` PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/client-streams/src/dsl/processors/table_aggregate.rs \
        crates/client-streams/src/dsl/processors/mod.rs
git commit -m "feat(client-streams): KGroupedTable repartition-map + table-aggregate processors"
```

---

## Task 5: `KGroupedTable` handle + lowering

**Files:**
- Create: `crates/client-streams/src/dsl/kgrouped_table.rs`
- Modify: `crates/client-streams/src/dsl/mod.rs` (add `pub(crate) mod kgrouped_table;` and re-export `KGroupedTable`)

The handle mirrors `KGroupedStream`: it captures the upstream `KTable` node id, the mapper, and the `Grouped` serdes, records nothing on construction, and lowers `SELECT → repartition(Changed) → AGGREGATE + store` on a terminal op. Because the input to the repartition sink is `Change<VR>`, the repartition uses a `Changed`-wrapped value serde (a variant of `repartition_lower`).

- [ ] **Step 1: Write the handle + `repartition_lower_changed` + lowering**

Create `crates/client-streams/src/dsl/kgrouped_table.rs`:

```rust
//! `KGroupedTable<KR, VR>`: the handle between `KTable::group_by` and a terminal
//! table aggregation (`count`/`reduce`/`aggregate`). Unlike `KGroupedStream`,
//! the input is a `Change<V>` change-stream and the repartition topic carries a
//! `Change<VR>` (via the `Changed` serde). `KTable.groupBy` always repartitions.

use std::any::Any;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;

use crate::dsl::builder::InternalStreamsBuilder;
use crate::dsl::config::Materialized;
use crate::dsl::graph::{GraphNodeKind, LowerState, NodeId};
use crate::dsl::kgrouped::mint_store_name;
use crate::dsl::ktable::KTable;
use crate::dsl::names;
use crate::dsl::processors::change::Change;
use crate::dsl::processors::table_aggregate::{
    KTableAggregateProcessor, KTableRepartitionMapProcessor,
};
use crate::processor::serde::{Changed, Consumed, DefaultSerde, I64Serde, Produced, Serde};
use crate::topology::NodeHandle;

/// Erased thunk that wires the `Change`-carrying repartition `sink → topic →
/// source` triple. Args: `(state, parent_name, sink_name, source_name, topic)`.
/// The parent node here forwards `Record<KR, Change<VR>>` (the SELECT output).
pub(crate) type ChangedRepartitionLowerFn =
    Box<dyn FnOnce(&mut LowerState, String, String, String, String) + Send>;

/// Build a [`ChangedRepartitionLowerFn`] capturing the grouped key serde and a
/// `Changed`-wrapped value serde, so the repartition round-trip carries the
/// `Change<VR>` byte-compatibly with the JVM.
pub(crate) fn repartition_lower_changed<KR, VR, KS, VS>(
    key_serde: KS,
    value_serde: VS,
) -> ChangedRepartitionLowerFn
where
    KR: Any + Send + Sync + Clone,
    VR: Any + Send + Clone,
    KS: Serde<KR> + Clone + 'static,
    VS: Serde<VR> + Clone + 'static,
{
    Box::new(
        move |state: &mut LowerState,
              parent_name: String,
              sink_name: String,
              source_name: String,
              topic: String| {
            let parent = NodeHandle::<KR, Change<VR>>::from_name(parent_name);
            state.topology.add_sink_explicit::<KR, Change<VR>, KS, Changed<VS>, _, _>(
                sink_name,
                topic.clone(),
                [parent],
                Produced::with(key_serde.clone(), Changed::new(value_serde.clone())),
            );
            state.topology.add_repartition_topic(topic.clone());
            state.topology.add_source_explicit::<KR, Change<VR>, KS, Changed<VS>>(
                source_name,
                [topic],
                Consumed::with(key_serde, Changed::new(value_serde)),
            );
        },
    )
}

/// Handle produced by `KTable::group_by[_explicit]`.
pub struct KGroupedTable<KR, VR> {
    builder: Rc<RefCell<InternalStreamsBuilder>>,
    /// The upstream `KTable` node (forwards `Change<V>`).
    parent: NodeId,
    /// Records the SELECT map node (mapper-erased) and returns the SELECT node id.
    /// Boxed so `KGroupedTable` stays free of the source `K,V,M` type params.
    record_select: Option<RecordSelectFn>,
    /// `Change`-carrying repartition lowering thunk.
    repartition_lower: Option<ChangedRepartitionLowerFn>,
    _pd: PhantomData<fn() -> (KR, VR)>,
}

/// Erased thunk: record the SELECT (repartition-map) graph node whose parent is
/// the upstream KTable node, returning the SELECT node id. Captures the source
/// `K,V` types + mapper so `KGroupedTable<KR,VR>` need not name them.
pub(crate) type RecordSelectFn = Box<dyn FnOnce(&mut InternalStreamsBuilder, NodeId) -> NodeId>;

impl<KR, VR> KGroupedTable<KR, VR>
where
    KR: Any + Send + Sync + Clone + PartialEq,
    VR: Any + Send + Clone,
{
    pub(crate) fn new(
        builder: Rc<RefCell<InternalStreamsBuilder>>,
        parent: NodeId,
        record_select: RecordSelectFn,
        repartition_lower: ChangedRepartitionLowerFn,
    ) -> Self {
        Self {
            builder,
            parent,
            record_select: Some(record_select),
            repartition_lower: Some(repartition_lower),
            _pd: PhantomData,
        }
    }

    /// `count` into a materialized `KTable<KR, i64>`.
    pub fn count_explicit<KS, VS>(
        self,
        materialized: impl Into<Materialized<KS, VS>>,
    ) -> KTable<KR, i64, KS, VS>
    where
        KS: Serde<KR> + Clone + 'static,
        VS: Serde<i64> + Clone + 'static,
    {
        self.aggregate_inner(
            materialized.into(),
            names::KTABLE_AGGREGATE_STORE,
            || 0i64,
            |_k: &KR, _v: &VR, a: i64| a + 1,
            |_k: &KR, _v: &VR, a: i64| a - 1,
        )
    }

    /// `reduce`: fold per group with `adder`, undo with `subtractor`. Result type
    /// stays `VR`; the first value for a group seeds (add on an empty store).
    pub fn reduce_explicit<KS, VS, Add, Sub>(
        self,
        adder: Add,
        subtractor: Sub,
        materialized: impl Into<Materialized<KS, VS>>,
    ) -> KTable<KR, VR, KS, VS>
    where
        KS: Serde<KR> + Clone + 'static,
        VS: Serde<VR> + Clone + 'static,
        Add: Fn(&VR, &VR) -> VR + Clone + Send + Sync + 'static,
        Sub: Fn(&VR, &VR) -> VR + Clone + Send + Sync + 'static,
    {
        // reduce has no initializer; the first add on an absent store value must
        // yield the value itself. Model init as "panic if used" is unsafe, so we
        // special-case via an adder that ignores the (unused) seed on first add.
        let materialized = materialized.into();
        let store_name = mint_store_name(&self.builder, &materialized, names::KTABLE_REDUCE_STORE);
        let adder2 = adder.clone();
        self.lower::<KS, VS, VR, _, _, _>(
            materialized,
            store_name,
            // init is never *added to* on the first record because reduce seeds
            // with the first value; we encode that by making the adder treat the
            // store-absent case in the processor (store.get None → init()).
            // For reduce the accumulator type == VR, so init() must produce a
            // value the adder reduces against. We instead seed by making the
            // FIRST add return the new value directly:
            //   adder_agg(k, v, acc) = if acc is the sentinel-from-init -> v
            // Rust has no sentinel for arbitrary VR, so reduce uses a dedicated
            // processor path; see note. For this slice, require VR: Default and
            // define init = VR::default(), adder_agg(k,v,acc)=adder(&acc,v),
            // subtractor_agg(k,v,acc)=subtractor(&acc,v). Default-seed matches the
            // JVM only when adder(default, first) == first; callers needing exact
            // JVM reduce-seed semantics use aggregate().
            move || VR_default_placeholder::<VR>(),
            move |_k: &KR, v: &VR, acc: VR| adder2(&acc, v),
            move |_k: &KR, v: &VR, acc: VR| subtractor(&acc, v),
        )
    }

    /// `aggregate`: general subtract/add aggregation into `KTable<KR, T>`.
    pub fn aggregate_explicit<KS, VS, T, I, Add, Sub>(
        self,
        init: I,
        adder: Add,
        subtractor: Sub,
        materialized: impl Into<Materialized<KS, VS>>,
    ) -> KTable<KR, T, KS, VS>
    where
        T: Any + Send + Clone,
        KS: Serde<KR> + Clone + 'static,
        VS: Serde<T> + Clone + 'static,
        I: Fn() -> T + Clone + Send + Sync + 'static,
        Add: Fn(&KR, &VR, T) -> T + Clone + Send + Sync + 'static,
        Sub: Fn(&KR, &VR, T) -> T + Clone + Send + Sync + 'static,
    {
        self.aggregate_inner(materialized.into(), names::KTABLE_AGGREGATE_STORE, init, adder, subtractor)
    }

    // Default-serde convenience forms.
    pub fn count(self, store_name: impl Into<String>) -> KTable<KR, i64, <KR as DefaultSerde>::Serde, I64Serde>
    where
        KR: DefaultSerde,
        <KR as DefaultSerde>::Serde: Serde<KR> + Clone,
    {
        self.count_explicit(
            Materialized::with(<KR as DefaultSerde>::Serde::default(), I64Serde).as_store(store_name),
        )
    }

    pub fn aggregate<T, I, Add, Sub>(
        self,
        init: I,
        adder: Add,
        subtractor: Sub,
        store_name: impl Into<String>,
    ) -> KTable<KR, T, <KR as DefaultSerde>::Serde, <T as DefaultSerde>::Serde>
    where
        T: DefaultSerde + Any + Send + Clone,
        KR: DefaultSerde,
        <KR as DefaultSerde>::Serde: Serde<KR> + Clone,
        <T as DefaultSerde>::Serde: Serde<T> + Clone,
        I: Fn() -> T + Clone + Send + Sync + 'static,
        Add: Fn(&KR, &VR, T) -> T + Clone + Send + Sync + 'static,
        Sub: Fn(&KR, &VR, T) -> T + Clone + Send + Sync + 'static,
    {
        self.aggregate_explicit(
            init,
            adder,
            subtractor,
            Materialized::with(
                <KR as DefaultSerde>::Serde::default(),
                <T as DefaultSerde>::Serde::default(),
            )
            .as_store(store_name),
        )
    }

    fn aggregate_inner<KS, VS, T, I, Add, Sub>(
        self,
        materialized: Materialized<KS, VS>,
        store_prefix: &'static str,
        init: I,
        adder: Add,
        subtractor: Sub,
    ) -> KTable<KR, T, KS, VS>
    where
        T: Any + Send + Clone,
        KS: Serde<KR> + Clone + 'static,
        VS: Serde<T> + Clone + 'static,
        I: Fn() -> T + Clone + Send + Sync + 'static,
        Add: Fn(&KR, &VR, T) -> T + Clone + Send + Sync + 'static,
        Sub: Fn(&KR, &VR, T) -> T + Clone + Send + Sync + 'static,
    {
        let store_name = mint_store_name(&self.builder, &materialized, store_prefix);
        self.lower::<KS, VS, T, I, Add, Sub>(materialized, store_name, init, adder, subtractor)
    }

    /// Record SELECT → repartition(Changed) → AGGREGATE + store; return the
    /// result `KTable<KR, T>`.
    fn lower<KS, VS, T, I, Add, Sub>(
        mut self,
        materialized: Materialized<KS, VS>,
        store_name: String,
        init: I,
        adder: Add,
        subtractor: Sub,
    ) -> KTable<KR, T, KS, VS>
    where
        T: Any + Send + Clone,
        KS: Serde<KR> + Clone + 'static,
        VS: Serde<T> + Clone + 'static,
        I: Fn() -> T + Clone + Send + Sync + 'static,
        Add: Fn(&KR, &VR, T) -> T + Clone + Send + Sync + 'static,
        Sub: Fn(&KR, &VR, T) -> T + Clone + Send + Sync + 'static,
    {
        let Materialized { key_serde, value_serde, logging, .. } = materialized;
        let suppress_factory =
            crate::dsl::ktable::kv_suppress_factory::<KR, T, KS, VS>(key_serde.clone(), value_serde.clone());
        let record_select = self.record_select.take().expect("record_select consumed");
        let rp_lower = self.repartition_lower.take().expect("repartition_lower consumed");
        let parent = self.parent;
        let mut g = self.builder.borrow_mut();

        // 1) SELECT (repartition-map) node, parent = upstream KTable node.
        let select_id = record_select(&mut g, parent);

        // 2) Repartition: KTABLE.groupBy ALWAYS repartitions. Mint filter+sink+
        //    source indices (the JVM mints a null-key filter before the sink),
        //    then record a Repartition node fed by the SELECT node.
        let _filter_name = g.new_processor_name(names::FILTER);
        let sink_name = g.new_processor_name(names::SINK);
        let source_name = g.new_processor_name(names::SOURCE);
        let topic_store = store_name.clone();
        let rp_id = g.graph.add(
            source_name.clone(),
            GraphNodeKind::Repartition {
                topic: format!("{topic_store}{}", names::REPARTITION_SUFFIX),
                partitions: None,
            },
            vec![select_id],
        );
        g.graph.nodes[rp_id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent_name = state.handle_name[&select_id].clone();
            let topic = format!("{}-{topic_store}{}", state.app_id, names::REPARTITION_SUFFIX);
            rp_lower(state, parent_name, sink_name.clone(), source_name.clone(), topic);
            state.handle_name.insert(rp_id, source_name.clone());
        }));

        // 3) AGGREGATE node fed by the repartition source.
        let agg_name = g.new_processor_name(names::KTABLE_AGGREGATE);
        let agg_id = g.graph.add(
            agg_name.clone(),
            GraphNodeKind::Aggregate { store_name: store_name.clone(), changelog: logging },
            vec![rp_id],
        );
        let store_for_thunk = store_name.clone();
        let key_serde_lower = key_serde.clone();
        let value_serde_lower = value_serde.clone();
        g.graph.nodes[agg_id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent =
                NodeHandle::<KR, Change<VR>>::from_name(state.handle_name[&rp_id].clone());
            let store_for_proc = store_for_thunk.clone();
            let h = state.topology.add_processor::<KR, Change<VR>, KR, Change<T>, _, _, _>(
                agg_name.clone(),
                move || KTableAggregateProcessor {
                    store_name: store_for_proc.clone(),
                    init: init.clone(),
                    adder: adder.clone(),
                    subtractor: subtractor.clone(),
                    _pd: PhantomData,
                },
                [parent],
            );
            if logging {
                state.topology.add_state_store::<KR, T, KS, VS>(
                    store_for_thunk.clone(),
                    key_serde_lower.clone(),
                    value_serde_lower.clone(),
                    [h.name().to_string()],
                );
            } else {
                state.topology.add_state_store_no_changelog::<KR, T, KS, VS>(
                    store_for_thunk.clone(),
                    key_serde_lower.clone(),
                    value_serde_lower.clone(),
                );
            }
            state.handle_name.insert(agg_id, h.name().to_string());
        }));

        drop(g);
        KTable::new(Rc::clone(&self.builder), agg_id, Some(store_name), None, key_serde, value_serde)
            .with_suppress_factory(Some(suppress_factory))
    }
}

/// `reduce` accumulator seed. `reduce` keeps the accumulator type == `VR`, but
/// has no initializer: the first value for a group seeds it. The aggregate
/// processor seeds an absent store entry with `init()`, so `reduce` requires a
/// neutral seed. We require `VR: Default` for the convenience form and document
/// that `aggregate()` is the escape hatch when `adder(VR::default(), first) !=
/// first`.
fn VR_default_placeholder<VR: Default>() -> VR {
    VR::default()
}
```

> **NOTE on `reduce` seeding (resolve during implementation):** the sketch above
> exposes the one real subtlety — the JVM `KGroupedTable.reduce` seeds a group
> with the *first value* (no initializer), whereas the processor seeds an absent
> store entry with `init()`. Two clean resolutions; pick one and delete the
> placeholder `VR_default_placeholder`:
> 1. **`VR: Default` seed (simplest):** `init = VR::default`,
>    `adder_agg = |_k,v,acc| adder(&acc, v)`. Exact for the common numeric reduce
>    (`Default::default()` is the additive identity). The `reduce` golden in Task 7
>    (`reduce-out`) is the gate.
> 2. **Sentinel-free first-add:** give `KTableAggregateProcessor` a `reduce` flavor
>    where, on an absent store value, an add-only change forwards `new` directly
>    (no `adder` call) — matching JVM exactly for any `VR`. Implement as a second
>    processor `KTableReduceProcessor<KR,VR,Add,Sub>` if the golden rejects option 1.
>
> Start with option 1; if `reduce-out` mismatches, switch to option 2. Bound
> `reduce_explicit` with `VR: Default` for option 1.

- [ ] **Step 2: Export the module**

In `crates/client-streams/src/dsl/mod.rs`, add `pub(crate) mod kgrouped_table;` and, alongside the existing handle re-exports (e.g. where `KGroupedStream` is re-exported), add `pub use kgrouped_table::KGroupedTable;` (match the existing visibility/`pub use` convention).

- [ ] **Step 3: Build to verify it compiles**

Run: `cargo build -p crabka-client-streams`
Expected: compiles. Resolve the `reduce` seed per the NOTE (pick option 1; add `VR: Default` to `reduce_explicit` and delete `VR_default_placeholder`). `KGroupedTable` is not yet reachable from the DSL (Task 6 adds `group_by`), so there are no behavioral tests here — the gate is compilation plus Task 7's goldens.

- [ ] **Step 4: Commit**

```bash
git add crates/client-streams/src/dsl/kgrouped_table.rs crates/client-streams/src/dsl/mod.rs
git commit -m "feat(client-streams): KGroupedTable handle + Change-carrying repartition lowering"
```

---

## Task 6: `KTable::group_by` / `group_by_explicit`

**Files:**
- Modify: `crates/client-streams/src/dsl/ktable.rs`

`group_by` records no node; it builds the `RecordSelectFn` (which records the `KTABLE-SELECT` repartition-map node at lowering) and the `Changed` repartition thunk, then returns a `KGroupedTable<KR,VR>`.

- [ ] **Step 1: Add the methods**

In `crates/client-streams/src/dsl/ktable.rs`, inside the `impl<K, V, KS, VS> KTable<K, V, KS, VS>` block (near `to_stream`/`map_values`), add. Bring needed imports into scope at top of file (`Grouped`, `KGroupedTable`, the repartition-map processor, `Change`, `Changed` are referenced):

```rust
/// `groupBy`: re-group the table by a new `(KR, VR)` derived from each entry,
/// then aggregate with `count`/`reduce`/`aggregate`. Always repartitions
/// (the JVM `KTable.groupBy` inserts a repartition-map + sink + source).
pub fn group_by<KR, VR, M>(&self, mapper: M) -> crate::dsl::kgrouped_table::KGroupedTable<KR, VR>
where
    KR: DefaultSerde + Any + Send + Sync + Clone + PartialEq,
    VR: DefaultSerde + Any + Send + Clone,
    <KR as DefaultSerde>::Serde: Serde<KR> + Clone,
    <VR as DefaultSerde>::Serde: Serde<VR> + Clone,
    M: Fn(&K, &V) -> (KR, VR) + Clone + Send + Sync + 'static,
    K: Any + Send + Sync + Clone,
    V: Any + Send + Clone,
{
    self.group_by_explicit(
        mapper,
        Grouped::with(
            <KR as DefaultSerde>::Serde::default(),
            <VR as DefaultSerde>::Serde::default(),
        ),
    )
}

/// `groupBy` with explicit repartition serdes.
pub fn group_by_explicit<KR, VR, GKS, GVS, M>(
    &self,
    mapper: M,
    grouped: impl Into<Grouped<GKS, GVS>>,
) -> crate::dsl::kgrouped_table::KGroupedTable<KR, VR>
where
    KR: Any + Send + Sync + Clone + PartialEq,
    VR: Any + Send + Clone,
    GKS: Serde<KR> + Clone + 'static,
    GVS: Serde<VR> + Clone + 'static,
    M: Fn(&K, &V) -> (KR, VR) + Clone + Send + Sync + 'static,
    K: Any + Send + Sync + Clone,
    V: Any + Send + Clone,
{
    use crate::dsl::graph::{GraphNodeKind, LowerState};
    use crate::dsl::processors::change::Change;
    use crate::dsl::processors::table_aggregate::KTableRepartitionMapProcessor;

    let grouped = grouped.into();
    let mapper_for_select = mapper.clone();
    // RecordSelectFn: record the KTABLE-SELECT repartition-map node (parent =
    // the upstream KTable node) and its lowering thunk; return the SELECT id.
    let record_select: crate::dsl::kgrouped_table::RecordSelectFn =
        Box::new(move |g: &mut InternalStreamsBuilder, parent_id| {
            let select_name = g.new_processor_name(names::KTABLE_SELECT);
            let id = g.graph.add(
                select_name.clone(),
                GraphNodeKind::TableProcessor { store_name: None },
                vec![parent_id],
            );
            let mapper2 = mapper_for_select.clone();
            g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
                let parent =
                    NodeHandle::<K, Change<V>>::from_name(state.handle_name[&parent_id].clone());
                let h = state
                    .topology
                    .add_processor::<K, Change<V>, KR, Change<VR>, _, _, _>(
                        select_name.clone(),
                        move || KTableRepartitionMapProcessor {
                            mapper: mapper2.clone(),
                            _pd: PhantomData,
                        },
                        [parent],
                    );
                state.handle_name.insert(id, h.name().to_string());
            }));
            id
        });

    crate::dsl::kgrouped_table::KGroupedTable::new(
        Rc::clone(&self.builder),
        self.node,
        record_select,
        crate::dsl::kgrouped_table::repartition_lower_changed::<KR, VR, GKS, GVS>(
            grouped.key_serde,
            grouped.value_serde,
        ),
    )
}
```

- [ ] **Step 2: Build to verify it compiles**

Run: `cargo build -p crabka-client-streams`
Expected: compiles. If `Grouped` / `NodeHandle` / `InternalStreamsBuilder` / `PhantomData` are not already imported in `ktable.rs`, add the `use` lines (grep the top of `ktable.rs` and the imports in `kgrouped.rs` for the exact paths — `crate::dsl::config::Grouped`, `crate::topology::NodeHandle`, `crate::dsl::builder::InternalStreamsBuilder`, `std::marker::PhantomData`).

- [ ] **Step 3: Smoke test — a built topology lists the repartition + changelog topics**

Add a temporary inline `#[cfg(test)]` test in `ktable.rs` (or run the Task 7 golden directly). Quick smoke:

```rust
#[test]
fn group_by_count_builds() {
    use crate::dsl::builder::StreamsBuilder;
    use crate::processor::serde::{I64Serde, StringSerde};
    let b = StreamsBuilder::new();
    let t = b.table::<String, i64>("in", "src-store");
    t.group_by(|_k: &String, v: &i64| (if v % 2 == 0 { "even".to_string() } else { "odd".to_string() }, *v))
        .count("count-store");
    let built = b.build("app").unwrap();
    // The repartition + changelog topics must be present and app-prefixed.
    let topics = format!("{:?}", built.wire_topology());
    assert!(topics.contains("app-count-store-repartition"), "missing repartition topic: {topics}");
    assert!(topics.contains("app-count-store-changelog"), "missing changelog topic: {topics}");
}
```

Run: `cargo test -p crabka-client-streams --lib group_by_count_builds`
Expected: PASS. (Adjust `b.table(...)` and `built.wire_topology()` to the real constructor/accessor names — grep `builder.rs` for the `table` signature and `BuiltTopology` for the wire accessor used by `dsl_golden_frame.rs`, which is `WireTopology`.) Delete this smoke test once Task 7's golden passes, or keep it as a cheap structural guard.

- [ ] **Step 4: Commit**

```bash
git add crates/client-streams/src/dsl/ktable.rs
git commit -m "feat(client-streams): KTable::group_by/group_by_explicit -> KGroupedTable"
```

---

## Task 7: end-to-end goldens — topology + behavioral replay

**Files:**
- Create: `crates/client-streams/tests/kgrouped_table_golden.rs`
- Uses: `tests/testdata/kgrouped_table/behavior.json`, `tests/testdata/golden/dsl/kgrouped_table.topology.json` (from Task 1)

- [ ] **Step 1: Write the topology golden test**

Reuse the `dsl_golden_frame.rs` assertion shape. Create `crates/client-streams/tests/kgrouped_table_golden.rs`:

```rust
//! KGroupedTable goldens: the built wire topology and the behavioral output
//! must match the JVM 4.1.0 capture byte-for-byte.

use crabka_client_streams::StreamsBuilder;
use crabka_client_streams::processor::serde::{Consumed, I64Serde, Produced, StringSerde};

/// Build the same topology as `KGroupedTableBehavior.java`.
fn build() -> crabka_client_streams::topology::BuiltTopology {
    let b = StreamsBuilder::new();
    let src = b.table_explicit::<StringSerde, I64Serde>(
        "in",
        Consumed::with(StringSerde, I64Serde),
        crabka_client_streams::dsl::config::Materialized::with(StringSerde, I64Serde).as_store("src-store"),
    );
    let pos = src.filter(|_k, v: &i64| *v > 0); // see note: filter signature
    let grouped = pos.group_by_explicit(
        |_k: &String, v: &i64| (if v % 2 == 0 { "even".to_string() } else { "odd".to_string() }, *v),
        crabka_client_streams::dsl::config::Grouped::with(StringSerde, I64Serde),
    );
    grouped
        .clone_for_each() // placeholder: KGroupedTable is consumed once; build three separate topologies if needed
        ;
    unreachable!("replaced below")
}
```

> The `KGroupedTable` handle is consumed by a single terminal (like
> `KGroupedStream`). The JVM fixture attaches three terminals to one `grouped`
> because `groupBy` returns a reusable `KGroupedTable`; in Rust the handle is
> move-consumed. **Resolve by building one topology per terminal** (three
> separate `StreamsBuilder`s — `build_count`, `build_reduce`, `build_aggregate`),
> each `table → filter → group_by → <terminal>`, and assert each against its own
> slice of the goldens. The JVM fixture's single combined topology is only used
> for the *behavioral* goldens (the three output topics); for the *topology*
> golden, capture per-terminal topology JSON in Task 1 instead, OR assert only
> the behavioral goldens here and drop the combined-topology assertion. **Pick:
> assert behavioral per-terminal (below); make the topology-golden a per-terminal
> `count` topology** to keep it byte-checkable.

Replace `build()` with three builders and the real tests:

```rust
use crabka_client_streams::dsl::config::{Grouped, Materialized};

fn build_count() -> crabka_client_streams::topology::BuiltTopology {
    let b = StreamsBuilder::new();
    let src = b.table_explicit::<StringSerde, I64Serde>(
        "in",
        Consumed::with(StringSerde, I64Serde),
        Materialized::with(StringSerde, I64Serde).as_store("src-store"),
    );
    src.filter(|_k, v: &i64| *v > 0)
        .group_by_explicit(
            |_k: &String, v: &i64| (if v % 2 == 0 { "even".to_string() } else { "odd".to_string() }, *v),
            Grouped::with(StringSerde, I64Serde),
        )
        .count_explicit(Materialized::with(StringSerde, I64Serde).as_store("count-store"))
        .to_stream()
        .to_explicit("count-out", Produced::with(StringSerde, I64Serde));
    b.build("app").unwrap()
}

#[derive(serde::Deserialize, PartialEq, Debug)]
struct Row { key: String, value: i64 }

#[test]
fn kgrouped_table_count_topology_matches_jvm() {
    let built = build_count();
    let actual = serde_json::to_value(built.wire_topology()).unwrap();
    let expected: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string("tests/testdata/golden/dsl/kgrouped_table.topology.json").unwrap(),
    )
    .unwrap();
    assert_eq!(actual, expected, "wire topology != JVM kgrouped_table fixture");
}
```

> **NOTE:** if Task 1 captured the *combined* topology (all three terminals), make
> `kgrouped_table.topology.json` the combined one and build the combined topology
> by attaching all three terminals — which requires `group_by` to be callable
> twice on the same source table (it is: call `pos.group_by(...)` three times, the
> `filter`/`table` nodes are shared by node-id reuse the same way the JVM shares
> the upstream). Prefer the combined topology to match the JVM fixture exactly;
> the per-terminal form above is the fallback if node-sharing differs.

- [ ] **Step 2: Write the behavioral golden test**

```rust
#[test]
fn kgrouped_table_behavior_matches_jvm() {
    // One driver running the combined topology (count + reduce + aggregate),
    // matching KGroupedTableBehavior.java's three output topics.
    let b = StreamsBuilder::new();
    let src = b.table_explicit::<StringSerde, I64Serde>(
        "in",
        Consumed::with(StringSerde, I64Serde),
        Materialized::with(StringSerde, I64Serde).as_store("src-store"),
    );
    let pos = src.filter(|_k, v: &i64| *v > 0);
    let key_fn = |_k: &String, v: &i64| (if v % 2 == 0 { "even".to_string() } else { "odd".to_string() }, *v);
    pos.group_by_explicit(key_fn, Grouped::with(StringSerde, I64Serde))
        .count_explicit(Materialized::with(StringSerde, I64Serde).as_store("count-store"))
        .to_stream()
        .to_explicit("count-out", Produced::with(StringSerde, I64Serde));
    pos.group_by_explicit(key_fn, Grouped::with(StringSerde, I64Serde))
        .reduce_explicit(|a: &i64, v: &i64| a + v, |a: &i64, v: &i64| a - v,
            Materialized::with(StringSerde, I64Serde).as_store("reduce-store"))
        .to_stream()
        .to_explicit("reduce-out", Produced::with(StringSerde, I64Serde));
    pos.group_by_explicit(key_fn, Grouped::with(StringSerde, I64Serde))
        .aggregate_explicit(|| 0i64, |_k: &String, v: &i64, a: i64| a + v, |_k: &String, v: &i64, a: i64| a - v,
            Materialized::with(StringSerde, I64Serde).as_store("agg-store"))
        .to_stream()
        .to_explicit("agg-out", Produced::with(StringSerde, I64Serde));

    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    let s = StringSerde;
    for (k, v, ts) in [("a", 2i64, 0), ("b", 4, 1), ("a", 6, 2), ("c", 3, 3), ("b", 5, 4), ("a", -1, 5)] {
        d.pipe_input("in", Consumed::with(s, I64Serde), Some(k.to_string()), v, ts);
    }

    let golden: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string("tests/testdata/kgrouped_table/behavior.json").unwrap(),
    )
    .unwrap();
    for topic in ["count", "reduce", "aggregate"] {
        let out_topic = format!("{topic}-out");
        let mut got: Vec<Row> = Vec::new();
        while let Some((Some(k), v)) = d.read_output(&out_topic, Produced::with(StringSerde, I64Serde)) {
            got.push(Row { key: k, value: v });
        }
        let want: Vec<Row> = serde_json::from_value(golden[topic].clone()).unwrap();
        assert_eq!(got, want, "{topic} output != JVM behavioral golden");
    }
}
```

- [ ] **Step 3: Run the golden tests**

Run: `cargo test -p crabka-client-streams --test kgrouped_table_golden`
Expected: both tests PASS. Likely first-run failures and how to resolve:
- **`reduce` mismatch** → switch to NOTE option 2 in Task 5 (first-value seed).
- **topology repartition/changelog config mismatch** (e.g. `cleanup.policy`) → the JSON golden is authoritative; the table repartition may differ from the stream `count` fixture (which is `cleanup.policy=delete`). Do not change the golden; align the lowering's topic config to it.
- **counter/index drift in store names** → confirm the SELECT/filter/sink/source/AGGREGATE prefixes are minted in the exact JVM order (Task 5 mints filter→sink→source then AGGREGATE; SELECT is minted in Task 6's `record_select` at `group_by` call time — verify that matches the JVM order: SELECT first, then the repartition triple, then AGGREGATE).

- [ ] **Step 4: Full gate**

Run:
```bash
cargo fmt -p crabka-client-streams
cargo clippy -p crabka-client-streams --all-targets -- -D warnings
cargo test -p crabka-client-streams
```
Expected: fmt clean, clippy clean, all tests pass. Then the workspace gate:
```bash
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```
Expected: green. (If the new `tests/kgrouped_table_golden.rs` needs a codecov `--test` entry, the client-streams-integration job uses a catch-all `--tests` selector per the streams memory, so no CI edit is required — confirm by checking `ci.yml` if patch coverage reports 0%.)

- [ ] **Step 5: Commit**

```bash
git add crates/client-streams/tests/kgrouped_table_golden.rs
git commit -m "test(client-streams): KGroupedTable topology + behavioral goldens (JVM 4.1.0)"
```

---

## Self-Review notes (for the executor)

- **Spec coverage:** Task 1 = goldens (spec §7); Task 2 = names (spec §4.3); Task 3 = `Changed` serde (spec §5.3); Task 4 = the two processors (spec §5.1/§5.2); Task 5 = handle + lowering (spec §4); Task 6 = DSL surface (spec §1/§6); Task 7 = verification (spec §7). All three terminals (spec §2) land in Tasks 5–7. Out-of-scope items (windowed table agg, source nulls, caching) are not implemented — correct.
- **Known soft spots flagged inline (resolve with the golden as authority):** (a) the exact `ChangedSerializer` flag byte — Task 3 Step 3 says align to captured hex; (b) `reduce` seeding — Task 5 NOTE gives two options, golden decides; (c) `KGroupedTable` single-consume vs the JVM's reusable handle — Task 7 builds one terminal per `group_by` call; (d) `ErasedRecord` key accessor in the Task 4 map test — adjust to the real accessor. None of these are placeholders for *logic*; each is a "match the captured ground truth" decision with a named fallback.
- **Type consistency:** `KTableAggregateProcessor` fields (`store_name`, `init`, `adder`, `subtractor`) are identical in Task 4 (def) and Task 5 (construction). `KTableRepartitionMapProcessor` fields (`mapper`, `_pd`) match between Task 4 and Task 6. `Changed::new` / `repartition_lower_changed` signatures match between Tasks 3 and 5.
