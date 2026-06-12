# Cogroup (KIP-150) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the full Kafka-Streams cogroup (KIP-150) DSL surface to `crates/client-streams` — non-windowed plus time-, session-, and sliding-windowed cogroups — with byte-exact JVM 4.1.0 topology parity and behavioral parity.

**Architecture:** Reuse the existing per-window aggregate processors (`KStreamAggregateProcessor`, `KStreamWindowAggregateProcessor`, `KStreamSessionAggregateProcessor`, `KStreamSlidingWindowAggregateProcessor`) against a **single shared store**, fanning into one new passthrough **merge** processor that the result `KTable` reads. A `CogroupedKStream<K, VOut>` accumulates type-erased per-input lowering specs; one shared `lower_cogroup` helper records the per-input repartition+aggregate nodes and the merge node, registering the shared store exactly once in the merge node's lowering thunk.

**Tech Stack:** Rust 2024 (crate `crabka-client-streams`), `async-trait`, the crate's DSL graph/lowering infra, JVM Kafka Streams 4.1.0 golden captures via Docker.

---

## Background the executor needs

**The DSL pipeline** (`crates/client-streams/src/dsl/`): DSL ops record a logical `GraphNode` DAG (`graph.rs`), auto-naming each node from a global counter at call time (`builder.rs::new_processor_name` → `"{PREFIX}{index:010}"`). The optimizer (`optimizer.rs`) rewrites the graph, then `lower.rs` walks nodes **in id order** (parents always precede children) running each node's `lower` thunk to build the Processor-API `Topology`. `to_wire()` serializes that to the wire topology the golden tests compare.

**Each aggregate node's thunk** does two things: `add_processor(...)` (attaches the concrete processor, returns a `NodeHandle` whose `.name()` is the lowered node name) and registers the state store (`add_state_store` / `add_window_store` / `add_session_store`) listing which processors use it. The key insight for cogroup: **N per-input aggregate processors all point at ONE store; only the merge node (which runs last, in id order) registers that store once**, listing all N aggregate processor names.

**Erasure rule** (`store/window_schema.rs` etc.): the wire topology carries only TOPIC and STORE names; processor `KOut`/`VOut` types are internal to each thunk. So per-input thunks can each use their own concrete input value type `Vn` while the graph stays parameterized only by `K`/`VOut`. Type mismatches in the dyn-Any lowering surface as **runtime downcast panics**, not compile errors — so the full integration suite is the real gate.

**JVM ground truth:** `crates/client-streams/tests/jvm-capture/` is a Java harness driving the exact private Kafka client code path; `run.sh` runs it in Docker against Kafka Streams 4.1.0 and writes wire-topology JSON to `tests/testdata/golden/dsl/<name>.topology.json` and behavioral output to `tests/testdata/<feature>/behavior*.json`. **The `COGROUP-*` node names and the shared-store counter position are NOT known from memory — Task 0.1 captures them empirically and the rest of the plan reads the captured JSON for exact strings.**

**CI:** the `client-streams-integration` job (`.github/workflows/ci.yml`) runs `cargo llvm-cov nextest -p crabka-client-streams --tests`, which auto-discovers every `tests/*.rs` binary. **New test files need no ci.yml change.**

---

## File structure

**New source files:**
- `crates/client-streams/src/dsl/processors/cogroup_merge.rs` — `KStreamPassThrough<K, V>` merge processor.
- `crates/client-streams/src/dsl/cogrouped.rs` — `CogroupedKStream<K, VOut>`, the erasure types (`CogroupInput`, `CogroupSpec`, `CogroupKind`), `KGroupedStream::cogroup`, non-windowed `aggregate`, and the shared `lower_cogroup` helper.
- `crates/client-streams/src/dsl/time_windowed_cogrouped.rs` — `TimeWindowedCogroupedStream<K, VOut>` + `CogroupedKStream::windowed_by`.
- `crates/client-streams/src/dsl/sliding_windowed_cogrouped.rs` — `SlidingWindowedCogroupedStream<K, VOut>` + `CogroupedKStream::windowed_by_sliding`.
- `crates/client-streams/src/dsl/session_windowed_cogrouped.rs` — `SessionWindowedCogroupedStream<K, VOut>` + `CogroupedKStream::windowed_by_session`.

**New test files (each self-contained, auto-discovered by CI):**
- `crates/client-streams/tests/cogroup_nonwindowed.rs`
- `crates/client-streams/tests/cogroup_time_windowed.rs`
- `crates/client-streams/tests/cogroup_sliding_windowed.rs`
- `crates/client-streams/tests/cogroup_session_windowed.rs`

**New Java harness file:**
- `crates/client-streams/tests/jvm-capture/src/main/java/crabka/capture/CogroupBehavior.java`

**New golden fixtures (generated, committed):**
- `tests/testdata/golden/dsl/cogroup{,_time,_session,_sliding}.topology.json`
- `tests/testdata/cogroup/behavior{,_time,_session,_sliding}.json`

**Modified source files:**
- `crates/client-streams/src/dsl/names.rs` — add cogroup name constants.
- `crates/client-streams/src/dsl/processors/mod.rs` — add `pub(crate) mod cogroup_merge;`.
- `crates/client-streams/src/dsl/mod.rs` — add 4 module decls + 4 re-exports.
- `crates/client-streams/src/dsl/kgrouped.rs` — add `KGroupedStream::cogroup` (delegates to `cogrouped.rs`).
- `crates/client-streams/src/lib.rs` — add 4 handle types to the public re-export block.
- `crates/client-streams/tests/jvm-capture/src/main/java/crabka/capture/Capture.java` — add 4 cogroup topology methods + 4 `write(...)` calls.
- `crates/client-streams/tests/jvm-capture/run.sh` — add a `--cogroup` behavioral mode.

**Batching:**
- **Batch 0 (Tasks 0.1–0.6, serial):** capture all fixtures, name constants, merge processor, cogroup core + non-windowed end-to-end. Each task depends on the prior.
- **Batch 1 (Tasks 1.1–1.3, sequential):** the three windowed handles. Logically independent but each edits the shared `dsl/mod.rs` + `lib.rs` (and reads the shared `lower_cogroup`), so run them in sequence to avoid merge conflicts on those wiring files.

---

## Batch 0 — Foundation + non-windowed cogroup

### Task 0.1: Capture JVM golden fixtures for all four cogroup variants

**Files:**
- Modify: `crates/client-streams/tests/jvm-capture/src/main/java/crabka/capture/Capture.java`
- Create: `crates/client-streams/tests/jvm-capture/src/main/java/crabka/capture/CogroupBehavior.java`
- Modify: `crates/client-streams/tests/jvm-capture/run.sh`
- Generated: `tests/testdata/golden/dsl/cogroup{,_time,_session,_sliding}.topology.json`, `tests/testdata/cogroup/behavior{,_time,_session,_sliding}.json`

- [ ] **Step 1: Add four topology-capture methods to `Capture.java`**

Find the existing static topology methods (e.g. `slidingWindowCount()`) and the `optimizedProps()` helper. Add these four methods (use two co-partitioned input topics `in1`/`in2`, a shared output type `Long`, and an explicit `Materialized` store name so the store name is deterministic):

```java
/** Non-windowed cogroup: in1 (len) + in2 (constant) → aggregate → toStream → to. */
static Topology cogroup() {
    StreamsBuilder b = new StreamsBuilder();
    KGroupedStream<String, String> g1 = b.<String, String>stream("in1").groupByKey();
    KGroupedStream<String, String> g2 = b.<String, String>stream("in2").groupByKey();
    KTable<String, Long> t = g1
        .<Long>cogroup((k, v, agg) -> agg + v.length())
        .cogroup(g2, (k, v, agg) -> agg + 1)
        .aggregate(() -> 0L,
            Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("cg-store")
                .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()));
    t.toStream().to("out", Produced.with(Serdes.String(), Serdes.Long()));
    return b.build(optimizedProps());
}

/** Time-windowed cogroup. */
static Topology cogroupTime() {
    StreamsBuilder b = new StreamsBuilder();
    KGroupedStream<String, String> g1 = b.<String, String>stream("in1").groupByKey();
    KGroupedStream<String, String> g2 = b.<String, String>stream("in2").groupByKey();
    KTable<Windowed<String>, Long> t = g1
        .<Long>cogroup((k, v, agg) -> agg + v.length())
        .cogroup(g2, (k, v, agg) -> agg + 1)
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMillis(100)))
        .aggregate(() -> 0L,
            Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("cg-store")
                .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()));
    t.toStream().to("out", Produced.with(
        WindowedSerdes.timeWindowedSerdeFrom(String.class, 100L), Serdes.Long()));
    return b.build(optimizedProps());
}

/** Sliding-windowed cogroup. */
static Topology cogroupSliding() {
    StreamsBuilder b = new StreamsBuilder();
    KGroupedStream<String, String> g1 = b.<String, String>stream("in1").groupByKey();
    KGroupedStream<String, String> g2 = b.<String, String>stream("in2").groupByKey();
    KTable<Windowed<String>, Long> t = g1
        .<Long>cogroup((k, v, agg) -> agg + v.length())
        .cogroup(g2, (k, v, agg) -> agg + 1)
        .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofMillis(100)))
        .aggregate(() -> 0L,
            Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("cg-store")
                .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()));
    t.toStream().to("out", Produced.with(
        WindowedSerdes.timeWindowedSerdeFrom(String.class, 100L), Serdes.Long()));
    return b.build(optimizedProps());
}

/** Session-windowed cogroup (note the session merger). */
static Topology cogroupSession() {
    StreamsBuilder b = new StreamsBuilder();
    KGroupedStream<String, String> g1 = b.<String, String>stream("in1").groupByKey();
    KGroupedStream<String, String> g2 = b.<String, String>stream("in2").groupByKey();
    KTable<Windowed<String>, Long> t = g1
        .<Long>cogroup((k, v, agg) -> agg + v.length())
        .cogroup(g2, (k, v, agg) -> agg + 1)
        .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMillis(100)))
        .aggregate(() -> 0L, (k, a, bb) -> a + bb,
            Materialized.<String, Long, SessionStore<Bytes, byte[]>>as("cg-store")
                .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()));
    t.toStream().to("out", Produced.with(
        WindowedSerdes.sessionWindowedSerdeFrom(String.class), Serdes.Long()));
    return b.build(optimizedProps());
}
```

Add the imports the existing file is missing (`org.apache.kafka.streams.kstream.KGroupedStream`, `KTable`, `Windowed`, `TimeWindows`, `SlidingWindows`, `SessionWindows`, `WindowStore`, `SessionStore`, `Bytes`, `WindowedSerdes`, `Duration`) — match how the existing sliding/session capture methods import them.

In `main(...)`, add after the existing `write(...)` calls:

```java
write(outDir, "cogroup", cogroup());
write(outDir, "cogroup_time", cogroupTime());
write(outDir, "cogroup_sliding", cogroupSliding());
write(outDir, "cogroup_session", cogroupSession());
```

Update the trailing count in the final `System.out.println("Capture complete. Wrote N fixtures ...")` to match the new total.

- [ ] **Step 2: Create `CogroupBehavior.java`**

Model it on `SlidingWindowBehavior.java`. It drives a `TopologyTestDriver` over each of the four cogroup topologies above with a fixed two-topic input script and writes the emission sequence `(key, [windowStart, windowEnd,] value)` to JSON. Use this exact non-windowed body and follow the same structure for the windowed variants (windowed rows additionally emit `windowStart`/`windowEnd` from `kv.key.window()`):

```java
package crabka.capture;

import java.nio.file.*;
import java.time.*;
import java.util.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

public final class CogroupBehavior {
    public static void main(String[] args) throws Exception {
        Path out = Paths.get(args.length > 0 ? args[0] : "out");
        Files.createDirectories(out);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "cogroup-behavior");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // ── Non-windowed cogroup ───────────────────────────────────────────
        {
            StreamsBuilder b = new StreamsBuilder();
            KGroupedStream<String, String> g1 = b.<String, String>stream("in1").groupByKey();
            KGroupedStream<String, String> g2 = b.<String, String>stream("in2").groupByKey();
            g1.<Long>cogroup((k, v, agg) -> agg + v.length())
              .cogroup(g2, (k, v, agg) -> agg + 1)
              .aggregate(() -> 0L,
                  Materialized.<String, Long, org.apache.kafka.streams.state.KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("cg-store")
                      .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()))
              .toStream().to("out", Produced.with(Serdes.String(), Serdes.Long()));

            try (TopologyTestDriver driver = new TopologyTestDriver(b.build(), props, Instant.ofEpochMilli(0))) {
                TestInputTopic<String, String> in1 = driver.createInputTopic(
                    "in1", Serdes.String().serializer(), Serdes.String().serializer());
                TestInputTopic<String, String> in2 = driver.createInputTopic(
                    "in2", Serdes.String().serializer(), Serdes.String().serializer());
                TestOutputTopic<String, Long> outT = driver.createOutputTopic(
                    "out", Serdes.String().deserializer(), Serdes.Long().deserializer());

                // interleaved: (topic, key, value, ts)
                in1.pipeInput("a", "xx", Instant.ofEpochMilli(0));   // +2
                in2.pipeInput("a", "z", Instant.ofEpochMilli(1));    // +1
                in1.pipeInput("a", "y", Instant.ofEpochMilli(2));    // +1
                in1.pipeInput("b", "qqqq", Instant.ofEpochMilli(3)); // +4
                in2.pipeInput("b", "z", Instant.ofEpochMilli(4));    // +1

                StringBuilder sb = new StringBuilder("[\n");
                List<KeyValue<String, Long>> recs = outT.readKeyValuesToList();
                for (int i = 0; i < recs.size(); i++) {
                    KeyValue<String, Long> kv = recs.get(i);
                    sb.append("  {\"key\": \"").append(kv.key).append("\", \"value\": ").append(kv.value).append("}");
                    sb.append(i + 1 < recs.size() ? ",\n" : "\n");
                }
                sb.append("]\n");
                Files.writeString(out.resolve("behavior.json"), sb.toString());
            }
        }
        // ── Time / Sliding / Session blocks: same pattern, windowed rows add
        //    "windowStart"/"windowEnd"; write behavior_time.json / behavior_sliding.json
        //    / behavior_session.json. (Session aggregate takes the extra merger
        //    (k, a, bb) -> a + bb.) ───────────────────────────────────────────
    }
}
```

Write out the time/sliding/session blocks in full following the non-windowed block and the `SlidingWindowBehavior.java` windowed-row formatting; use the same input script for all four (it exercises two keys, two topics, multiple records per key).

- [ ] **Step 3: Add a `--cogroup` mode to `run.sh`**

Find the existing `--sliding)` case. Add a sibling `--cogroup)` case that compiles and runs `CogroupBehavior` into `tests/testdata/cogroup` (mirror the `--sliding` javac/java lines exactly, swapping the class name and output dir):

```bash
--cogroup)
    docker run --rm \
      -v "$TESTS_DIR":/tests -w /tests/jvm-capture \
      "$JDK_IMAGE" bash -c '
        set -euo pipefail
        # (identical jar download block as --sliding)
        mkdir -p /tmp/build /tests/testdata/cogroup
        javac -cp "$CP" -d /tmp/build src/main/java/crabka/capture/CogroupBehavior.java
        java -cp "/tmp/build:$RT" crabka.capture.CogroupBehavior /tests/testdata/cogroup
      '
    ;;
```

- [ ] **Step 4: Run the topology capture (regenerates all golden topology JSON)**

Run:
```bash
cd crates/client-streams/tests/jvm-capture && ./run.sh
```
Expected: `Capture complete. Wrote N fixtures to ...` and four new files under `tests/testdata/golden/dsl/`: `cogroup.topology.json`, `cogroup_time.topology.json`, `cogroup_sliding.topology.json`, `cogroup_session.topology.json`.

- [ ] **Step 5: Run the behavioral capture**

Run:
```bash
cd crates/client-streams/tests/jvm-capture && ./run.sh --cogroup
```
Expected: four files under `tests/testdata/cogroup/`: `behavior.json`, `behavior_time.json`, `behavior_sliding.json`, `behavior_session.json`.

- [ ] **Step 6: Inspect the captured non-windowed topology to read the real node names**

Run:
```bash
cat crates/client-streams/tests/testdata/golden/dsl/cogroup.topology.json
```
Record, for use in Task 0.2, the exact processor names the JVM emitted for: the per-input cogroup aggregate processors, the merge node, and confirm the shared store name appears once in `state_changelog_topics`. (Kafka's constants are `COGROUP-AGGREGATE-` and `COGROUP-MERGE-`, but **use whatever the JSON shows**.)

- [ ] **Step 7: Commit the harness + fixtures**

```bash
git add crates/client-streams/tests/jvm-capture crates/client-streams/tests/testdata/golden/dsl/cogroup*.topology.json crates/client-streams/tests/testdata/cogroup
git commit -m "test(streams): capture JVM 4.1 cogroup golden topology + behavior fixtures (KIP-150)"
```

---

### Task 0.2: Add cogroup name constants

**Files:**
- Modify: `crates/client-streams/src/dsl/names.rs`

- [ ] **Step 1: Add the constants read from the captured JSON in Task 0.1 Step 6**

Append to `names.rs` (replace the prefix string literals with the exact values seen in `cogroup.topology.json` if they differ):

```rust
/// KIP-150 cogroup per-input aggregate processor prefix (one node per input
/// stream; all share the cogroup state store). Pinned by the `cogroup` golden.
pub(crate) const COGROUP_AGGREGATE: &str = "COGROUP-AGGREGATE-";
/// KIP-150 cogroup passthrough merge node prefix (fans the per-input aggregate
/// processors into the single result `KTable` source). Pinned by the golden.
pub(crate) const COGROUP_MERGE: &str = "COGROUP-MERGE-";
```

If the captured JSON shows the shared store auto-names from a distinct counter prefix when no `Materialized` name is given, also add that prefix constant (the plan's fixtures all use an explicit `"cg-store"` name, so a prefix constant is only needed if you add an unnamed-store test later).

- [ ] **Step 2: Verify it compiles**

Run: `cargo build -p crabka-client-streams`
Expected: builds (the new `const`s are `pub(crate)` and may trigger `dead_code` until used — that's resolved in Task 0.4; if `-D warnings` is on locally, add `#[allow(dead_code)]` above each, matching the file's existing style).

- [ ] **Step 3: Commit**

```bash
git add crates/client-streams/src/dsl/names.rs
git commit -m "feat(client-streams): cogroup node-name constants (KIP-150)"
```

---

### Task 0.3: The merge passthrough processor

**Files:**
- Create: `crates/client-streams/src/dsl/processors/cogroup_merge.rs`
- Modify: `crates/client-streams/src/dsl/processors/mod.rs`

- [ ] **Step 1: Write the failing unit test**

Create `crates/client-streams/src/dsl/processors/cogroup_merge.rs` with the processor and a unit test that asserts it forwards its input record unchanged. Model the test plumbing on `aggregate.rs`'s `#[cfg(test)]` block (the `Dispatch`/`ProcessorContext` setup):

```rust
//! `KStreamPassThrough<K, V>`: forwards every record unchanged. Used as the
//! KIP-150 cogroup merge node — it fans the per-input aggregate processors
//! (each forwarding `Change<VOut>`) into the single result `KTable` source.

use std::marker::PhantomData;

use async_trait::async_trait;

use crate::processor::api::{Processor, ProcessorContext};
use crate::processor::record::Record;

/// Variance-neutral marker.
type Marker<T> = PhantomData<fn() -> T>;

#[allow(dead_code)]
pub(crate) struct KStreamPassThrough<K, V> {
    pub _pd: Marker<(K, V)>,
}

#[async_trait]
impl<K, V> Processor<K, V, K, V> for KStreamPassThrough<K, V>
where
    K: std::any::Any + Send + Clone,
    V: std::any::Any + Send + Clone,
{
    async fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, K, V>, r: Record<K, V>) {
        ctx.forward(r);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use assert2::check;

    use super::*;
    use crate::processor::erased::{Dispatch, ErasedRecord};
    use crate::processor::record::RecordContext;

    #[tokio::test]
    async fn passthrough_forwards_record_unchanged() {
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let mut stores = crate::store::registry::StoreRegistry::default();
        let rc = RecordContext { topic: "in".into(), partition: 0, offset: 0, timestamp: 7 };
        let globals = crate::runtime::global::GlobalStateManager::default();
        let mut scheds = Vec::new();
        let mut dispatch = Dispatch {
            buffer: &mut buffer,
            children: &children,
            output: &mut output,
            record_ctx: &rc,
            stores: &mut stores,
            globals: &globals,
            node_idx: 0,
            schedules: &mut scheds,
            sched_stream_time: i64::MIN,
            sched_wall_clock: 0,
        };
        let mut ctx = ProcessorContext::<'_, '_, String, i64>::new(&mut dispatch);
        let mut proc = KStreamPassThrough::<String, i64> { _pd: PhantomData };
        proc.process(&mut ctx, Record::new(Some("a".into()), 42, 7)).await;

        let (_, rec) = buffer.pop_front().expect("forwarded record");
        let v = rec.value.downcast::<i64>().unwrap();
        check!(v == 42);
    }
}
```

Add to `crates/client-streams/src/dsl/processors/mod.rs` after `pub(crate) mod change;`:
```rust
pub(crate) mod cogroup_merge;
```

- [ ] **Step 2: Run the test to verify it passes**

Run: `cargo test -p crabka-client-streams --lib cogroup_merge`
Expected: PASS (`passthrough_forwards_record_unchanged`). If the `Dispatch` field set differs from what you copied, fix to match the current `aggregate.rs` test (it is the source of truth for that struct's shape).

- [ ] **Step 3: Commit**

```bash
git add crates/client-streams/src/dsl/processors/cogroup_merge.rs crates/client-streams/src/dsl/processors/mod.rs
git commit -m "feat(client-streams): cogroup merge passthrough processor (KIP-150)"
```

---

### Task 0.4: Cogroup core — `CogroupedKStream`, `KGroupedStream::cogroup`, non-windowed aggregate, shared `lower_cogroup`

**Files:**
- Create: `crates/client-streams/src/dsl/cogrouped.rs`
- Modify: `crates/client-streams/src/dsl/kgrouped.rs` (add `cogroup` method)
- Modify: `crates/client-streams/src/dsl/mod.rs` (module decl + re-export)
- Modify: `crates/client-streams/src/lib.rs` (public re-export)

- [ ] **Step 1: Write `cogrouped.rs` — erasure types, builder, non-windowed aggregate, shared helper**

Create `crates/client-streams/src/dsl/cogrouped.rs`:

```rust
//! KIP-150 cogroup: aggregate multiple co-partitioned input streams (each with
//! its own value type `Vn` but a shared key `K` and output type `VOut`) into one
//! `KTable`. Each input contributes an `Aggregator<K, Vn, VOut>`. The topology is
//! one aggregate processor per input — all writing to a single shared store —
//! fanning into one passthrough merge node the result `KTable` reads.
//!
//! `KGroupedStream::cogroup` / `CogroupedKStream::cogroup` capture each input's
//! lineage plus a **type-erased** `make_agg` thunk (closing over the concrete
//! `Vn` + aggregator). The terminal `aggregate` / `windowed_by*` supply the
//! shared `Initializer` (and, for sessions, the `Merger`) as a [`CogroupSpec`],
//! then [`lower_cogroup`] records the per-input repartition+aggregate nodes and
//! the merge node, registering the shared store exactly once in the merge thunk.

use std::any::Any;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Arc;

use crate::dsl::builder::InternalStreamsBuilder;
use crate::dsl::config::Materialized;
use crate::dsl::graph::{GraphNodeKind, LowerState, NodeId};
use crate::dsl::kgrouped::{KGroupedStream, RepartitionLowerFn, mint_store_name};
use crate::dsl::ktable::KTable;
use crate::dsl::names;
use crate::dsl::processors::aggregate::KStreamAggregateProcessor;
use crate::dsl::processors::change::Change;
use crate::dsl::processors::cogroup_merge::KStreamPassThrough;
use crate::processor::serde::{DefaultSerde, Serde};
use crate::topology::NodeHandle;

/// Which window flavor the terminal aggregation uses. Carries the window spec;
/// the shared init + (session) merger live alongside in [`CogroupSpec`].
#[derive(Clone)]
pub(crate) enum CogroupKind {
    NonWindowed,
    Time(crate::dsl::windows::TimeWindows),
    Sliding(crate::dsl::windows::SlidingWindows),
    Session(crate::dsl::windows::SessionWindows),
}

/// The terminal aggregation spec, built once at `aggregate()` time and cloned
/// per input. `init`/`merger` are `Arc`-erased so a per-input `make_agg` thunk
/// (which doesn't know `VOut`'s concrete closure type) can hold them.
pub(crate) struct CogroupSpec<K, VOut> {
    pub kind: CogroupKind,
    pub init: Arc<dyn Fn() -> VOut + Send + Sync>,
    pub merger: Option<Arc<dyn Fn(&K, VOut, VOut) -> VOut + Send + Sync>>,
}

impl<K, VOut> Clone for CogroupSpec<K, VOut> {
    fn clone(&self) -> Self {
        Self { kind: self.kind.clone(), init: self.init.clone(), merger: self.merger.clone() }
    }
}

/// Given a [`CogroupSpec`], an input returns a node-lowering thunk that adds its
/// per-window aggregate processor wired to `parent_name`, named `proc_name`,
/// pointing at `store_name`, and returns the lowered processor's handle name.
type AggNodeThunk = Box<dyn FnOnce(&mut LowerState, String, String, String) -> String + Send>;
type MakeAggFn<K, VOut> = Box<dyn FnOnce(CogroupSpec<K, VOut>) -> AggNodeThunk + Send>;

/// One cogrouped input: its grouped lineage plus the erased per-window aggregate
/// builder.
pub(crate) struct CogroupInput<K, VOut> {
    pub parent: NodeId,
    pub key_changing_upstream: bool,
    pub repartition_lower: Option<RepartitionLowerFn>,
    pub make_agg: MakeAggFn<K, VOut>,
}

/// Handle accumulating cogrouped inputs; terminal `aggregate`/`windowed_by*`
/// consume it. Fields are `pub(crate)` so the windowed-handle modules can move
/// the inputs into their own handles.
pub struct CogroupedKStream<K, VOut> {
    pub(crate) builder: Rc<RefCell<InternalStreamsBuilder>>,
    pub(crate) inputs: Vec<CogroupInput<K, VOut>>,
    pub(crate) _pd: PhantomData<fn() -> (K, VOut)>,
}

/// Build the erased `make_agg` for one input, closing over concrete `Vn` + the
/// aggregator. The returned thunk matches on the window kind to attach the right
/// per-window processor. Shared by `KGroupedStream::cogroup` and the chained
/// `CogroupedKStream::cogroup`.
pub(crate) fn make_agg_for_input<K, Vn, VOut, A>(agg: A) -> MakeAggFn<K, VOut>
where
    K: Any + Send + Sync + Clone,
    Vn: Any + Send + Sync + Clone,
    VOut: Any + Send + Sync + Clone,
    A: Fn(&K, &Vn, VOut) -> VOut + Send + Sync + 'static,
{
    let agg = Arc::new(agg);
    Box::new(move |spec: CogroupSpec<K, VOut>| -> AggNodeThunk {
        Box::new(move |state: &mut LowerState, parent_name: String, proc_name: String, store_name: String| -> String {
            let parent = NodeHandle::<K, Vn>::from_name(parent_name);
            let init = spec.init.clone();
            match spec.kind.clone() {
                CogroupKind::NonWindowed => {
                    let agg = agg.clone();
                    let store = store_name.clone();
                    let h = state.topology.add_processor::<K, Vn, K, Change<VOut>, _, _, _>(
                        proc_name,
                        move || KStreamAggregateProcessor {
                            store_name: store.clone(),
                            init: { let i = init.clone(); move || i() },
                            agg: { let a = agg.clone(); move |k: &K, v: &Vn, acc: VOut| a(k, v, acc) },
                            _pd: PhantomData,
                        },
                        [parent],
                    );
                    h.name().to_string()
                }
                CogroupKind::Time(w) => {
                    use crate::dsl::processors::window_aggregate::KStreamWindowAggregateProcessor;
                    use crate::dsl::windows::Windowed;
                    let agg = agg.clone();
                    let store = store_name.clone();
                    let h = state.topology.add_processor::<K, Vn, Windowed<K>, Change<VOut>, _, _, _>(
                        proc_name,
                        move || KStreamWindowAggregateProcessor {
                            store_name: store.clone(),
                            windows: w,
                            init: { let i = init.clone(); move || i() },
                            agg: { let a = agg.clone(); move |k: &K, v: &Vn, acc: VOut| a(k, v, acc) },
                            _pd: PhantomData,
                        },
                        [parent],
                    );
                    h.name().to_string()
                }
                CogroupKind::Sliding(w) => {
                    use crate::dsl::processors::sliding_window_aggregate::KStreamSlidingWindowAggregateProcessor;
                    use crate::dsl::windows::Windowed;
                    let agg = agg.clone();
                    let store = store_name.clone();
                    let h = state.topology.add_processor::<K, Vn, Windowed<K>, Change<VOut>, _, _, _>(
                        proc_name,
                        move || KStreamSlidingWindowAggregateProcessor {
                            store_name: store.clone(),
                            windows: w,
                            init: { let i = init.clone(); move || i() },
                            agg: { let a = agg.clone(); move |k: &K, v: &Vn, acc: VOut| a(k, v, acc) },
                            stream_time: i64::MIN,
                            _pd: PhantomData,
                        },
                        [parent],
                    );
                    h.name().to_string()
                }
                CogroupKind::Session(w) => {
                    use crate::dsl::processors::session_aggregate::KStreamSessionAggregateProcessor;
                    use crate::dsl::windows::Windowed;
                    let agg = agg.clone();
                    let store = store_name.clone();
                    let merger = spec.merger.clone().expect("session cogroup requires a merger");
                    let h = state.topology.add_processor::<K, Vn, Windowed<K>, Change<VOut>, _, _, _>(
                        proc_name,
                        move || KStreamSessionAggregateProcessor {
                            store_name: store.clone(),
                            gap_ms: w.gap_ms,
                            init: { let i = init.clone(); move || i() },
                            agg: { let a = agg.clone(); move |k: &K, v: &Vn, acc: VOut| a(k, v, acc) },
                            merger: { let m = merger.clone(); move |k: &K, a: VOut, b: VOut| m(k, a, b) },
                            _pd: PhantomData,
                        },
                        [parent],
                    );
                    h.name().to_string()
                }
            }
        })
    })
}

impl<K, VOut> CogroupedKStream<K, VOut>
where
    K: Any + Send + Sync + Clone,
    VOut: Any + Send + Sync + Clone,
{
    /// Chain another co-partitioned input with its own aggregator.
    #[must_use]
    pub fn cogroup<Vn, A>(mut self, grouped: KGroupedStream<K, Vn>, agg: A) -> Self
    where
        Vn: Any + Send + Sync + Clone,
        A: Fn(&K, &Vn, VOut) -> VOut + Send + Sync + 'static,
    {
        let (parent, key_changing, rp_lower) = grouped.into_cogroup_parts();
        self.inputs.push(CogroupInput {
            parent,
            key_changing_upstream: key_changing,
            repartition_lower: rp_lower,
            make_agg: make_agg_for_input::<K, Vn, VOut, A>(agg),
        });
        self
    }

    /// Non-windowed terminal aggregation → `KTable<K, VOut>`.
    pub fn aggregate_explicit<KS, VS, I>(
        self,
        init: I,
        materialized: impl Into<Materialized<KS, VS>>,
    ) -> KTable<K, VOut, KS, VS>
    where
        KS: Serde<K> + Clone + 'static,
        VS: Serde<VOut> + Clone + 'static,
        I: Fn() -> VOut + Send + Sync + 'static,
    {
        let materialized = materialized.into();
        let store_name = mint_store_name(&self.builder, &materialized, names::AGGREGATE_STORE);
        let Materialized { key_serde, value_serde, logging, .. } = materialized;
        let spec = CogroupSpec::<K, VOut> {
            kind: CogroupKind::NonWindowed,
            init: Arc::new(init),
            merger: None,
        };
        let ks = key_serde.clone();
        let vs = value_serde.clone();
        let store_for_reg = store_name.clone();
        // Store registrar: a non-windowed KV store, honoring Materialized logging.
        let registrar: StoreRegistrarFn = Box::new(move |state, procs| {
            if logging {
                state.topology.add_state_store::<K, VOut, KS, VS>(store_for_reg.clone(), ks.clone(), vs.clone(), procs);
            } else {
                state.topology.add_state_store_no_changelog::<K, VOut, KS, VS>(store_for_reg.clone(), ks.clone(), vs.clone());
            }
        });
        let suppress = crate::dsl::ktable::kv_suppress_factory::<K, VOut, KS, VS>(key_serde.clone(), value_serde.clone());
        let merge_id = lower_cogroup::<K, VOut, K>(&self.builder, self.inputs, store_name.clone(), spec, logging, registrar);
        KTable::new(Rc::clone(&self.builder), merge_id, Some(store_name), None, key_serde, value_serde)
            .with_suppress_factory(Some(suppress))
    }

    /// Non-windowed terminal aggregation with default serdes.
    pub fn aggregate<I>(
        self,
        init: I,
        store_name: impl Into<String>,
    ) -> KTable<K, VOut, <K as DefaultSerde>::Serde, <VOut as DefaultSerde>::Serde>
    where
        K: DefaultSerde,
        VOut: DefaultSerde,
        <K as DefaultSerde>::Serde: Serde<K> + Clone,
        <VOut as DefaultSerde>::Serde: Serde<VOut> + Clone,
        I: Fn() -> VOut + Send + Sync + 'static,
    {
        self.aggregate_explicit(
            init,
            Materialized::with(<K as DefaultSerde>::Serde::default(), <VOut as DefaultSerde>::Serde::default())
                .as_store(store_name),
        )
    }
}

/// Registers the shared cogroup store with the given per-input processor names.
/// Boxed so each terminal supplies its window-specific store type + serdes.
pub(crate) type StoreRegistrarFn = Box<dyn FnOnce(&mut LowerState, Vec<String>) + Send>;

/// Record, in id order: each input's optional repartition + its aggregate node
/// (shared store), then the merge node. The merge thunk attaches the passthrough
/// processor (parents = all aggregate handles) and runs `registrar` once to
/// register the shared store. Returns the merge node id (the result `KTable`'s
/// source). Generic over the merge output key `KOut` (`K` non-windowed,
/// `Windowed<K>` windowed).
#[allow(clippy::too_many_lines)]
pub(crate) fn lower_cogroup<K, VOut, KOut>(
    builder: &Rc<RefCell<InternalStreamsBuilder>>,
    inputs: Vec<CogroupInput<K, VOut>>,
    store_name: String,
    spec: CogroupSpec<K, VOut>,
    logging: bool,
    registrar: StoreRegistrarFn,
) -> NodeId
where
    K: Any + Send + Sync + Clone,
    VOut: Any + Send + Sync + Clone,
    KOut: Any + Send + Clone,
{
    let mut g = builder.borrow_mut();
    let mut agg_ids: Vec<NodeId> = Vec::with_capacity(inputs.len());
    for input in inputs {
        let CogroupInput { parent, key_changing_upstream, repartition_lower, make_agg } = input;
        let agg_parent = KGroupedStream::<K, ()>::record_repartition(
            &mut g, &store_name, parent, key_changing_upstream, repartition_lower,
        );
        let proc_name = g.new_processor_name(names::COGROUP_AGGREGATE);
        let agg_id = g.graph.add(
            proc_name.clone(),
            GraphNodeKind::Aggregate { store_name: store_name.clone(), changelog: false },
            vec![agg_parent],
        );
        let thunk = make_agg(spec.clone());
        let store_for = store_name.clone();
        let pn = proc_name.clone();
        g.graph.nodes[agg_id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent_name = state.handle_name[&agg_parent].clone();
            let handle = thunk(state, parent_name, pn, store_for);
            state.handle_name.insert(agg_id, handle);
        }));
        agg_ids.push(agg_id);
    }

    let merge_name = g.new_processor_name(names::COGROUP_MERGE);
    let merge_id = g.graph.add(
        merge_name.clone(),
        GraphNodeKind::Aggregate { store_name: store_name.clone(), changelog: logging },
        agg_ids.clone(),
    );
    g.graph.nodes[merge_id].lower = Some(Box::new(move |state: &mut LowerState| {
        let parents: Vec<NodeHandle<KOut, Change<VOut>>> = agg_ids
            .iter()
            .map(|id| NodeHandle::<KOut, Change<VOut>>::from_name(state.handle_name[id].clone()))
            .collect();
        let h = state.topology.add_processor::<KOut, Change<VOut>, KOut, Change<VOut>, _, _, _>(
            merge_name.clone(),
            || KStreamPassThrough::<KOut, Change<VOut>> { _pd: PhantomData },
            parents,
        );
        let proc_names: Vec<String> = agg_ids.iter().map(|id| state.handle_name[id].clone()).collect();
        registrar(state, proc_names);
        state.handle_name.insert(merge_id, h.name().to_string());
    }));
    drop(g);
    merge_id
}
```

> Note: if `add_state_store` / `add_state_store_no_changelog` / `kv_suppress_factory` / `into_cogroup_parts` signatures differ from those shown, match the real ones — `lower_aggregate` in `kgrouped.rs` is the source of truth for the non-windowed store calls and suppress factory, and Task 0.4 Step 2 adds `into_cogroup_parts`.

- [ ] **Step 2: Add `KGroupedStream::cogroup` + `into_cogroup_parts` to `kgrouped.rs`**

In `crates/client-streams/src/dsl/kgrouped.rs`, inside `impl<K, V> KGroupedStream<K, V>`, add (after `windowed_by_sliding`):

```rust
/// `cogroup`: begin a KIP-150 cogroup with this stream as the first input and
/// `agg` its aggregator. Returns a [`CogroupedKStream<K, VOut>`] to chain more
/// inputs and terminate with `aggregate` / `windowed_by*`.
///
/// [`CogroupedKStream<K, VOut>`]: crate::dsl::cogrouped::CogroupedKStream
#[must_use]
pub fn cogroup<VOut, A>(self, agg: A) -> crate::dsl::cogrouped::CogroupedKStream<K, VOut>
where
    VOut: Any + Send + Sync + Clone,
    A: Fn(&K, &V, VOut) -> VOut + Send + Sync + 'static,
{
    let builder = Rc::clone(&self.builder);
    let (parent, key_changing, rp_lower) = self.into_cogroup_parts();
    crate::dsl::cogrouped::CogroupedKStream {
        builder,
        inputs: vec![crate::dsl::cogrouped::CogroupInput {
            parent,
            key_changing_upstream: key_changing,
            repartition_lower: rp_lower,
            make_agg: crate::dsl::cogrouped::make_agg_for_input::<K, V, VOut, A>(agg),
        }],
        _pd: PhantomData,
    }
}

/// Decompose into the lineage parts a cogroup input needs (consumes the handle).
pub(crate) fn into_cogroup_parts(mut self) -> (NodeId, bool, Option<RepartitionLowerFn>) {
    (self.parent, self.key_changing_upstream, self.repartition_lower.take())
}
```

(`Any` is already imported in `kgrouped.rs`; `RepartitionLowerFn` and `NodeId` are in scope there.)

- [ ] **Step 3: Wire the module + re-exports**

In `crates/client-streams/src/dsl/mod.rs`, after `pub mod kgrouped;` add:
```rust
pub mod cogrouped;
```
and after `pub use kgrouped::KGroupedStream;` add:
```rust
pub use cogrouped::CogroupedKStream;
```

In `crates/client-streams/src/lib.rs`, add `CogroupedKStream` to the `pub use dsl::{ ... }` block (alphabetically near `BranchedStream`).

- [ ] **Step 4: Verify it compiles**

Run: `cargo build -p crabka-client-streams`
Expected: builds clean. Common fixups: the `add_processor` turbofish arity, the `Change<VOut>` import path, and ensuring `make_agg_for_input` / `CogroupInput` / `CogroupSpec` are `pub(crate)`.

- [ ] **Step 5: Commit**

```bash
git add crates/client-streams/src/dsl/cogrouped.rs crates/client-streams/src/dsl/kgrouped.rs crates/client-streams/src/dsl/mod.rs crates/client-streams/src/lib.rs
git commit -m "feat(client-streams): non-windowed cogroup core + shared lower_cogroup (KIP-150)"
```

---

### Task 0.5: Non-windowed golden topology test

**Files:**
- Create: `crates/client-streams/tests/cogroup_nonwindowed.rs`

- [ ] **Step 1: Write the failing golden topology test**

Create `crates/client-streams/tests/cogroup_nonwindowed.rs`. Build the exact same topology as `Capture.java::cogroup()` and assert byte-equality with the committed fixture (inline the 8-line loader from `dsl_golden_frame.rs::assert_matches_fixture`):

```rust
//! KIP-150 non-windowed cogroup — JVM 4.1 wire-topology + behavioral goldens.
use crabka_client_streams::dsl::StreamsBuilder;
use crabka_client_streams::{I64Serde, Materialized, Produced, StringSerde};

fn assert_matches_fixture(wire: &crabka_client_streams::topology::WireTopology, fixture: &str) {
    let path = format!("tests/testdata/golden/dsl/{fixture}.topology.json");
    let expected: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"))).unwrap();
    let actual = serde_json::to_value(wire).unwrap();
    assert_eq!(actual, expected, "wire topology != JVM fixture {fixture}");
}

#[test]
fn cogroup_matches_jvm() {
    let b = StreamsBuilder::new();
    let g1 = b.stream::<String, String>(["in1"]).group_by_key();
    let g2 = b.stream::<String, String>(["in2"]).group_by_key();
    let t = g1
        .cogroup::<i64, _>(|_k, v: &String, acc| acc + v.len() as i64)
        .cogroup(g2, |_k, _v: &String, acc| acc + 1)
        .aggregate_explicit(
            || 0i64,
            Materialized::with(StringSerde, I64Serde).as_store("cg-store"),
        );
    t.to_stream().to_explicit("out", Produced::with(StringSerde, I64Serde));
    let wire = b.build_optimized("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "cogroup");
}
```

- [ ] **Step 2: Run it — expect FAIL (topology mismatch)**

Run: `cargo test -p crabka-client-streams --test cogroup_nonwindowed cogroup_matches_jvm`
Expected: FAIL with a JSON diff. Read the diff carefully — it tells you exactly where the Rust topology diverges from the JVM (node names, store/changelog names, subtopology membership, copartition group). Iterate on `lower_cogroup` / `names.rs` until it matches. Likely fix points: the merge-node id vs counter ordering, whether the shared store's changelog appears once, and copartition grouping of `in1`/`in2`.

- [ ] **Step 3: Add the behavioral golden test to the same file**

Append:

```rust
#[test]
fn cogroup_matches_jvm_behavior() {
    use crabka_client_streams::Consumed;
    #[derive(serde::Deserialize, PartialEq, Debug)]
    struct Row { key: String, value: i64 }

    let b = StreamsBuilder::new();
    let g1 = b.stream::<String, String>(["in1"]).group_by_key();
    let g2 = b.stream::<String, String>(["in2"]).group_by_key();
    g1.cogroup::<i64, _>(|_k, v: &String, acc| acc + v.len() as i64)
        .cogroup(g2, |_k, _v: &String, acc| acc + 1)
        .aggregate_explicit(|| 0i64, Materialized::with(StringSerde, I64Serde).as_store("cg-store"))
        .to_stream()
        .to_explicit("out", Produced::with(StringSerde, I64Serde));
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();

    // Same interleaved script as CogroupBehavior.java.
    let s = StringSerde;
    d.pipe_input("in1", Consumed::with(s, s), Some("a".into()), "xx".into(), 0);
    d.pipe_input("in2", Consumed::with(s, s), Some("a".into()), "z".into(), 1);
    d.pipe_input("in1", Consumed::with(s, s), Some("a".into()), "y".into(), 2);
    d.pipe_input("in1", Consumed::with(s, s), Some("b".into()), "qqqq".into(), 3);
    d.pipe_input("in2", Consumed::with(s, s), Some("b".into()), "z".into(), 4);

    let mut got: Vec<Row> = Vec::new();
    while let Some((Some(k), v)) = d.read_output("out", Produced::with(StringSerde, I64Serde)) {
        got.push(Row { key: k, value: v });
    }
    let golden: Vec<Row> =
        serde_json::from_str(&std::fs::read_to_string("tests/testdata/cogroup/behavior.json").unwrap()).unwrap();
    assert_eq!(got, golden, "cogroup output sequence != JVM behavioral golden");
}
```

> Confirm `pipe_input` / `read_output` signatures against `dsl_execution.rs::sliding_window_count_matches_jvm_behavior` (the source of truth) and adjust the `Consumed`/`Produced` calls to match. `StringSerde` is `Copy`, so reusing `s` is fine.

- [ ] **Step 4: Run both tests — expect PASS**

Run: `cargo test -p crabka-client-streams --test cogroup_nonwindowed`
Expected: both `cogroup_matches_jvm` and `cogroup_matches_jvm_behavior` PASS. If behavior diverges, the per-input `agg` fold or the merge ordering is wrong — compare your emitted sequence to `behavior.json` row by row.

- [ ] **Step 5: Commit**

```bash
git add crates/client-streams/tests/cogroup_nonwindowed.rs
git commit -m "test(client-streams): non-windowed cogroup golden + behavioral parity (KIP-150)"
```

---

### Task 0.6: Batch 0 verification gate

- [ ] **Step 1: Full crate test + lint**

Run:
```bash
cargo test -p crabka-client-streams
cargo clippy -p crabka-client-streams --all-targets -- -D warnings
cargo fmt -p crabka-client-streams -- --check
```
Expected: all green. (Per project memory, CI gates on `cargo fmt --check` and `clippy --all-targets -D warnings` — run them now, not just at the end.) If `share_consume` / unrelated load tests flake under full parallelism, re-run them isolated before treating as a regression.

- [ ] **Step 2: Commit any fmt/clippy fixes**

```bash
git add -A && git commit -m "chore(client-streams): fmt + clippy after non-windowed cogroup"
```

---

## Batch 1 — Windowed cogroup handles (run sequentially)

Each windowed handle: (a) a new handle file declaring the struct, a `windowed_by*` constructor as an `impl CogroupedKStream` block, and the terminal `aggregate*` methods that call the shared `lower_cogroup` with the right `CogroupKind` + store registrar; (b) module decl + re-export (shared `dsl/mod.rs` + `lib.rs` — hence sequential); (c) golden + behavioral tests in a dedicated test file. The `Capture.java`/`CogroupBehavior.java` fixtures were already generated in Task 0.1.

### Task 1.1: Time-windowed cogroup

**Files:**
- Create: `crates/client-streams/src/dsl/time_windowed_cogrouped.rs`
- Create: `crates/client-streams/tests/cogroup_time_windowed.rs`
- Modify: `crates/client-streams/src/dsl/mod.rs`, `crates/client-streams/src/lib.rs`

- [ ] **Step 1: Write the handle**

Create `crates/client-streams/src/dsl/time_windowed_cogrouped.rs`:

```rust
//! `TimeWindowedCogroupedStream<K, VOut>`: time-windowed KIP-150 cogroup. Built
//! by `CogroupedKStream::windowed_by(TimeWindows)`. Terminal `aggregate*`
//! produces `KTable<Windowed<K>, VOut>` over a shared window store.
use std::any::Any;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Arc;

use crate::dsl::builder::InternalStreamsBuilder;
use crate::dsl::cogrouped::{
    CogroupInput, CogroupKind, CogroupSpec, CogroupedKStream, StoreRegistrarFn, lower_cogroup,
};
use crate::dsl::config::Materialized;
use crate::dsl::kgrouped::mint_store_name;
use crate::dsl::ktable::KTable;
use crate::dsl::names;
use crate::dsl::windows::{TimeWindowedSerde, TimeWindows, Windowed};
use crate::processor::serde::Serde;

impl<K, VOut> CogroupedKStream<K, VOut>
where
    K: Any + Send + Sync + Clone,
    VOut: Any + Send + Sync + Clone,
{
    /// `windowedBy(TimeWindows)` → time-windowed cogroup.
    #[must_use]
    pub fn windowed_by(self, windows: TimeWindows) -> TimeWindowedCogroupedStream<K, VOut> {
        TimeWindowedCogroupedStream { builder: self.builder, inputs: self.inputs, windows, _pd: PhantomData }
    }
}

pub struct TimeWindowedCogroupedStream<K, VOut> {
    builder: Rc<RefCell<InternalStreamsBuilder>>,
    inputs: Vec<CogroupInput<K, VOut>>,
    windows: TimeWindows,
    _pd: PhantomData<fn() -> (K, VOut)>,
}

impl<K, VOut> TimeWindowedCogroupedStream<K, VOut>
where
    K: Any + Send + Sync + Clone,
    VOut: Any + Send + Sync + Clone,
{
    pub fn aggregate_explicit<KS, VS, I>(
        self,
        init: I,
        materialized: impl Into<Materialized<KS, VS>>,
    ) -> KTable<Windowed<K>, VOut, TimeWindowedSerde<KS>, VS>
    where
        KS: Serde<K> + Clone + 'static,
        VS: Serde<VOut> + Clone + 'static,
        I: Fn() -> VOut + Send + Sync + 'static,
    {
        let materialized = materialized.into();
        let store_name = mint_store_name(&self.builder, &materialized, names::AGGREGATE_STORE);
        let Materialized { key_serde, value_serde, logging, .. } = materialized;
        let spec = CogroupSpec::<K, VOut> { kind: CogroupKind::Time(self.windows), init: Arc::new(init), merger: None };
        let ks = key_serde.clone();
        let vs = value_serde.clone();
        let store_for_reg = store_name.clone();
        let size = self.windows.size_ms;
        let grace = self.windows.grace_ms;
        let registrar: StoreRegistrarFn = Box::new(move |state, procs| {
            state.topology.add_window_store::<K, VOut, KS, VS>(store_for_reg.clone(), ks.clone(), vs.clone(), size, grace, procs);
        });
        let merge_id = lower_cogroup::<K, VOut, Windowed<K>>(&self.builder, self.inputs, store_name.clone(), spec, logging, registrar);
        KTable::new(
            Rc::clone(&self.builder),
            merge_id,
            Some(store_name),
            None,
            TimeWindowedSerde::new(key_serde, self.windows.size_ms),
            value_serde,
        )
        .with_window_grace(Some(self.windows.grace_ms))
    }
}
```

(If `TimeWindows` field names are `size_ms`/`grace_ms`, the above is correct — confirm against `windows.rs`. Omit the default-serde convenience `aggregate` here unless a test needs it; KIP-150 cogroup always passes explicit aggregators, so `aggregate_explicit` is the primary surface. Add a default-serde `aggregate` mirroring Task 0.4 only if the behavioral test uses default serdes.)

- [ ] **Step 2: Wire module + export**

`dsl/mod.rs`: add `pub mod time_windowed_cogrouped;` and `pub use time_windowed_cogrouped::TimeWindowedCogroupedStream;`. `lib.rs`: add `TimeWindowedCogroupedStream` to the `pub use dsl::{...}` block.

- [ ] **Step 3: Write the golden + behavioral tests**

Create `crates/client-streams/tests/cogroup_time_windowed.rs` mirroring `cogroup_nonwindowed.rs` but: build with `.windowed_by(TimeWindows::of_size(100))` (match the exact constructor used in `Capture.java::cogroupTime()` — `ofSizeWithNoGrace(100)`), output via `Produced::with(TimeWindowedSerde::new(StringSerde, 100), I64Serde)`, assert against fixture `"cogroup_time"`, and read the behavioral golden `tests/testdata/cogroup/behavior_time.json` into a `Row { key, window_start, window_end, value }` (same `#[serde(rename)]` as `dsl_execution.rs::sliding_window_count_matches_jvm_behavior`). Inline the `assert_matches_fixture` loader.

- [ ] **Step 4: Build, run, iterate to green**

Run:
```bash
cargo test -p crabka-client-streams --test cogroup_time_windowed
```
Expected: both tests PASS (iterate on any topology diff exactly as in Task 0.5 Step 2).

- [ ] **Step 5: fmt + clippy + commit**

```bash
cargo fmt -p crabka-client-streams
cargo clippy -p crabka-client-streams --all-targets -- -D warnings
git add crates/client-streams/src/dsl/time_windowed_cogrouped.rs crates/client-streams/src/dsl/mod.rs crates/client-streams/src/lib.rs crates/client-streams/tests/cogroup_time_windowed.rs
git commit -m "feat(client-streams): time-windowed cogroup (KIP-150)"
```

---

### Task 1.2: Sliding-windowed cogroup

**Files:**
- Create: `crates/client-streams/src/dsl/sliding_windowed_cogrouped.rs`
- Create: `crates/client-streams/tests/cogroup_sliding_windowed.rs`
- Modify: `crates/client-streams/src/dsl/mod.rs`, `crates/client-streams/src/lib.rs`

- [ ] **Step 1: Write the handle**

Create `crates/client-streams/src/dsl/sliding_windowed_cogrouped.rs` identical in shape to Task 1.1's file, with these differences:
- Constructor: `pub fn windowed_by_sliding(self, windows: SlidingWindows) -> SlidingWindowedCogroupedStream<K, VOut>`.
- `CogroupKind::Sliding(self.windows)`.
- Store registrar uses the **sliding retention** (`window size = time_difference_ms * 2`), exactly as `sliding_windowed_kgrouped.rs::lower_aggregate`:
```rust
let size = self.windows.time_difference_ms * 2;
let grace = self.windows.grace_ms;
let registrar: StoreRegistrarFn = Box::new(move |state, procs| {
    state.topology.add_window_store::<K, VOut, KS, VS>(store_for_reg.clone(), ks.clone(), vs.clone(), size, grace, procs);
});
```
- Result KTable: `TimeWindowedSerde::new(key_serde, self.windows.time_difference_ms)`, `.with_window_grace(Some(self.windows.grace_ms))`.
- Imports: `use crate::dsl::windows::{SlidingWindows, TimeWindowedSerde, Windowed};`.

- [ ] **Step 2: Wire module + export**

`dsl/mod.rs`: `pub mod sliding_windowed_cogrouped;` + `pub use sliding_windowed_cogrouped::SlidingWindowedCogroupedStream;`. `lib.rs`: add `SlidingWindowedCogroupedStream`.

- [ ] **Step 3: Write the golden + behavioral tests**

Create `crates/client-streams/tests/cogroup_sliding_windowed.rs` mirroring Task 1.1's test file but: `.windowed_by_sliding(SlidingWindows::of_time_difference_with_no_grace(100))` (match `Capture.java::cogroupSliding()`), output `TimeWindowedSerde::new(StringSerde, 100)`, fixture `"cogroup_sliding"`, behavioral golden `tests/testdata/cogroup/behavior_sliding.json`.

- [ ] **Step 4: Build, run, iterate to green**

Run: `cargo test -p crabka-client-streams --test cogroup_sliding_windowed`
Expected: both tests PASS.

- [ ] **Step 5: fmt + clippy + commit**

```bash
cargo fmt -p crabka-client-streams
cargo clippy -p crabka-client-streams --all-targets -- -D warnings
git add crates/client-streams/src/dsl/sliding_windowed_cogrouped.rs crates/client-streams/src/dsl/mod.rs crates/client-streams/src/lib.rs crates/client-streams/tests/cogroup_sliding_windowed.rs
git commit -m "feat(client-streams): sliding-windowed cogroup (KIP-150)"
```

---

### Task 1.3: Session-windowed cogroup (with merger)

**Files:**
- Create: `crates/client-streams/src/dsl/session_windowed_cogrouped.rs`
- Create: `crates/client-streams/tests/cogroup_session_windowed.rs`
- Modify: `crates/client-streams/src/dsl/mod.rs`, `crates/client-streams/src/lib.rs`

- [ ] **Step 1: Write the handle (note the extra `merger` arg on aggregate)**

Create `crates/client-streams/src/dsl/session_windowed_cogrouped.rs` like Task 1.1 but:
- Constructor: `pub fn windowed_by_session(self, windows: SessionWindows) -> SessionWindowedCogroupedStream<K, VOut>`.
- `aggregate_explicit` takes the **merger** and threads it into the spec:
```rust
pub fn aggregate_explicit<KS, VS, I, M>(
    self,
    init: I,
    merger: M,
    materialized: impl Into<Materialized<KS, VS>>,
) -> KTable<Windowed<K>, VOut, SessionWindowedSerde<KS>, VS>
where
    KS: Serde<K> + Clone + 'static,
    VS: Serde<VOut> + Clone + 'static,
    I: Fn() -> VOut + Send + Sync + 'static,
    M: Fn(&K, VOut, VOut) -> VOut + Send + Sync + 'static,
{
    let materialized = materialized.into();
    let store_name = mint_store_name(&self.builder, &materialized, names::AGGREGATE_STORE);
    let Materialized { key_serde, value_serde, logging, .. } = materialized;
    let spec = CogroupSpec::<K, VOut> {
        kind: CogroupKind::Session(self.windows),
        init: Arc::new(init),
        merger: Some(Arc::new(merger)),
    };
    let ks = key_serde.clone();
    let vs = value_serde.clone();
    let store_for_reg = store_name.clone();
    let gap = self.windows.gap_ms;
    let grace = self.windows.grace_ms;
    let registrar: StoreRegistrarFn = Box::new(move |state, procs| {
        state.topology.add_session_store::<K, VOut, KS, VS>(store_for_reg.clone(), ks.clone(), vs.clone(), gap, grace, procs);
    });
    let merge_id = lower_cogroup::<K, VOut, Windowed<K>>(&self.builder, self.inputs, store_name.clone(), spec, logging, registrar);
    KTable::new(
        Rc::clone(&self.builder),
        merge_id,
        Some(store_name),
        None,
        SessionWindowedSerde::new(key_serde),
        value_serde,
    )
    .with_window_grace(Some(self.windows.grace_ms))
}
```
- Imports: `use crate::dsl::windows::{SessionWindowedSerde, SessionWindows, Windowed};` (confirm `SessionWindowedSerde::new` arity against `session_windowed_kgrouped.rs`).

- [ ] **Step 2: Wire module + export**

`dsl/mod.rs`: `pub mod session_windowed_cogrouped;` + `pub use session_windowed_cogrouped::SessionWindowedCogroupedStream;`. `lib.rs`: add `SessionWindowedCogroupedStream`.

- [ ] **Step 3: Write the golden + behavioral tests**

Create `crates/client-streams/tests/cogroup_session_windowed.rs` mirroring Task 1.1's test file but: `.windowed_by_session(SessionWindows::with_inactivity_gap(100))` (match `Capture.java::cogroupSession()` — `ofInactivityGapWithNoGrace(100)`), pass the merger `|_k, a, b| a + b`, output via `SessionWindowedSerde::new(StringSerde)`, fixture `"cogroup_session"`, behavioral golden `tests/testdata/cogroup/behavior_session.json`. Session output rows carry `windowStart`/`windowEnd` like the other windowed variants.

- [ ] **Step 4: Build, run, iterate to green**

Run: `cargo test -p crabka-client-streams --test cogroup_session_windowed`
Expected: both tests PASS.

- [ ] **Step 5: fmt + clippy + commit**

```bash
cargo fmt -p crabka-client-streams
cargo clippy -p crabka-client-streams --all-targets -- -D warnings
git add crates/client-streams/src/dsl/session_windowed_cogrouped.rs crates/client-streams/src/dsl/mod.rs crates/client-streams/src/lib.rs crates/client-streams/tests/cogroup_session_windowed.rs
git commit -m "feat(client-streams): session-windowed cogroup with merger (KIP-150)"
```

---

## Final verification gate

- [ ] **Step 1: Whole-crate green**

Run:
```bash
cargo test -p crabka-client-streams
cargo clippy -p crabka-client-streams --all-targets -- -D warnings
cargo fmt --check
```
Expected: all four cogroup test binaries pass plus the existing suite; clippy/fmt clean.

- [ ] **Step 2: Confirm all four golden topologies + four behavioral goldens are committed and asserted**

Run:
```bash
ls crates/client-streams/tests/testdata/golden/dsl/cogroup*.topology.json
ls crates/client-streams/tests/testdata/cogroup/
```
Expected: 4 topology fixtures + 4 behavior fixtures, each referenced by a passing test.

- [ ] **Step 3: Update the project memory**

Update `project-kip1071-streams` memory: mark cogroup (KIP-150) DONE; next remaining gaps = versioned KTables (KIP-889/962), emit-final (KIP-825).

---

## Self-review notes

- **Spec coverage:** all four handles (non-windowed §Task 0.4, time §1.1, sliding §1.2, session §1.3), shared-store-via-merge-node (`lower_cogroup`), erasure mechanism (`make_agg_for_input` + `CogroupSpec`), capture-first name pinning (Task 0.1→0.2), golden + behavioral tests per variant, fmt/clippy gates — all present.
- **Names not guessed:** `COGROUP_AGGREGATE`/`COGROUP_MERGE` strings are filled from the Task 0.1 capture (Step 6 reads the JSON), not assumed.
- **Signature caveats:** every place that depends on an existing-but-unread signature (`add_state_store`, `kv_suppress_factory`, `pipe_input`/`read_output`, `TimeWindows`/`SessionWindows` field + constructor names, `SessionWindowedSerde::new` arity) names its source-of-truth file to copy from. These are the only soft spots and are flagged inline, not left as silent placeholders.
- **Arc-not-Fn:** the per-input processors wrap the `Arc`-erased init/agg/merger in fresh closures (`{ let i = init.clone(); move || i() }`) because `Arc<dyn Fn>` does not itself implement `Fn` — do not pass the `Arc` directly as the processor's `I`/`A`/`M`.
