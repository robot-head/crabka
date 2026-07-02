# KIP-1071 Streams Client — Cogroup (KIP-150)

**Date:** 2026-06-09
**Status:** Design approved, pending spec review
**Scope:** A client-side DSL slice adding cogroup aggregations across all four
window surfaces (non-windowed + time/session/sliding windowed) to
`crates/client-streams`. No broker or wire-protocol changes.

## 1. Context

The KIP-1071 streams **client** runtime (`crates/client-streams`, crate
`crabka-client-streams`) is feature-rich: the original 7-sub-project program
plus FK-join, global table, suppress, punctuation, EOS, standby/warmup,
schema-serde, and (most recently) sliding windows (KIP-450) have all merged.
The remaining Kafka-Streams DSL-parity gaps are **cogroup (KIP-150)**, versioned
KTables (KIP-889/962), and emit-final (KIP-825). This slice implements cogroup.

Cogroup aggregates **multiple co-partitioned input streams** — each with its own
value type but a shared key `K` and shared output type `VOut` — into a single
`KTable`. Each input contributes its own `Aggregator<K, Vn, VOut>` that folds
that input's records into the shared accumulator:

```java
KGroupedStream<K, V1> g1 = ...;
KGroupedStream<K, V2> g2 = ...;
KTable<K, VOut> t = g1.cogroup(agg1).cogroup(g2, agg2).aggregate(initializer);
```

In the JVM the topology is **one aggregate processor per input, all writing to a
single shared state store, fanning into one passthrough merge node** that the
result `KTable` reads. This maps cleanly onto the crate's existing per-window
aggregate processors: reuse them against a shared store, add one merge node.

## 2. Goal and non-goals

### Goal

A Rust app can write, with full JVM behavioral and byte-topology parity:

```rust
// non-windowed
let t = g1.cogroup(agg1).cogroup(g2, agg2).aggregate(init, "store"); // KTable<K, VOut>

// time-windowed
let t = g1.cogroup(agg1).cogroup(g2, agg2)
    .windowed_by(TimeWindows::of_size(100))
    .aggregate(init, "store"); // KTable<Windowed<K>, VOut>

// session-windowed (note the extra Merger)
let t = g1.cogroup(agg1).cogroup(g2, agg2)
    .windowed_by_session(SessionWindows::with_inactivity_gap(100))
    .aggregate(init, merger, "store"); // KTable<Windowed<K>, VOut>

// sliding-windowed
let t = g1.cogroup(agg1).cogroup(g2, agg2)
    .windowed_by_sliding(SlidingWindows::of_time_difference_and_grace(100, 50))
    .aggregate(init, "store"); // KTable<Windowed<K>, VOut>
```

The built topology serializes byte-for-byte identically to JVM Kafka Streams 4.1
(`optimization=all`), and aggregation output matches the JVM for single-input
and multi-input cogroups, in-order and out-of-order.

### Non-goals (deferred)

- **Emit-final / `EmitStrategy.onWindowClose`** (KIP-825) — emit-on-update only,
  consistent with the rest of the crate's windowed aggregations.
- **Versioned KTables** (KIP-889/962) — separate slice.
- No backwards-compat shims (greenfield, per `CLAUDE.md`).

## 3. Design

### 3.1 Approach

Reuse the existing per-window aggregate processors against a **shared store**,
plus one new thin **passthrough merge processor**. Rejected alternatives: a
unified multi-input processor (diverges from the JVM's N-processors-share-a-store
wire shape); refactoring the existing single-input aggregate into "cogroup of
one" (large regression risk against golden-pinned code, no slice benefit).

### 3.2 DSL handles (4)

Four new handles mirror the existing windowed-kgrouped triplet style:

- `CogroupedKStream<K, VOut>` — built by `KGroupedStream::cogroup(aggregator)`,
  chained via `.cogroup(other_grouped, aggregator2)`; terminal `.aggregate(init,
  store_name)` / `.aggregate_explicit(init, Materialized)` → `KTable<K, VOut>`.
- `TimeWindowedCogroupedKStream<K, VOut>` — via
  `CogroupedKStream::windowed_by(TimeWindows)`; `.aggregate(...)` →
  `KTable<Windowed<K>, VOut>`.
- `SessionWindowedCogroupedKStream<K, VOut>` — via
  `::windowed_by_session(SessionWindows)`; `.aggregate(init, merger, ...)` →
  `KTable<Windowed<K>, VOut>`. Session cogroup **uniquely** requires a
  `Merger<K, VOut>` (= `Fn(&K, VOut, VOut) -> VOut`) to combine accumulators when
  sessions merge, matching the JVM signature.
- `SlidingWindowedCogroupedKStream<K, VOut>` — via
  `::windowed_by_sliding(SlidingWindows)`; `.aggregate(...)` →
  `KTable<Windowed<K>, VOut>`.

Method names use the existing distinct-name convention (`windowed_by`,
`windowed_by_session`, `windowed_by_sliding`) since Rust cannot overload by the
window-spec argument type.

### 3.3 Type-erasure of inputs (the novel mechanism)

`KGroupedStream<K, Vn>::cogroup` moves the grouped lineage — parent `NodeId`,
`key_changing_upstream` flag, and the typed `RepartitionLowerFn` — plus a
**type-erased aggregate-lowering thunk** into the `CogroupedKStream<K, VOut>`'s
input `Vec`. The thunk captures the concrete `Vn` and the `Aggregator<K, Vn,
VOut>`; `VOut` is known at `cogroup` time (it is the aggregator's output type).

```rust
pub struct CogroupedKStream<K, VOut> {
    builder: Rc<RefCell<InternalStreamsBuilder>>,
    inputs: Vec<CogroupInput<K, VOut>>,
    _pd: PhantomData<fn() -> (K, VOut)>,
}

struct CogroupInput<K, VOut> {
    parent: NodeId,
    key_changing_upstream: bool,
    repartition_lower: Option<RepartitionLowerFn>,
    // Adds THIS input's aggregate processor wired to its (repartitioned) parent,
    // pointed at the shared store; returns the processor handle name. Does NOT
    // register the store. `init` is threaded in at aggregate() time.
    aggregate_lower: CogroupAggLowerFn<K, VOut>,
}
```

The shared `init: Initializer<VOut>` is supplied only at `aggregate()` time, so
each thunk accepts it as `Arc<dyn Fn() -> VOut + Send + Sync>` (constructible
because `VOut` is in scope at the `CogroupedKStream` level). `VOut: 'static`.

The per-input processor is the **existing** aggregate processor for the chosen
window type (`KStreamAggregateProcessor` / window / session / sliding), wired
with `V = Vn`, `VA = VOut`, the captured aggregator, and the shared store name.

### 3.4 Shared-store wiring and node ordering

`aggregate()` (per window type):

1. Mint the shared store name **once** — from `Materialized` if named, else a
   fresh counter at the JVM position (prefix TBD, pinned by capture).
2. For each input in order: record its optional `Repartition` node (each input
   independently key-changing → each may mint its own null-key filter + sink +
   source indices), then its aggregate node referencing the shared store.
3. Record a single `Merge` node whose lowering **registers the shared state
   store exactly once** and wires every per-input processor handle as a parent.
   The merge processor (`KStreamPassThrough`) forwards `Change<VOut>` unchanged.
4. The result `KTable` reads from the merge node.

### 3.5 New name constants (`dsl/names.rs`)

Provisional, pinned empirically by the non-windowed golden capture in Batch 0:

- `COGROUP_AGGREGATE` — per-input aggregate processor prefix.
- `COGROUP_MERGE` — passthrough merge node prefix.
- shared-store prefix (cogroup aggregate state store).

Exact strings, the store-name counter position, and whether the merge node
burns a counter index are determined by the captured JVM 4.1.0 topology, not
assumed from memory.

### 3.6 Store byte layout (unchanged)

Windowed cogroups reuse the existing shared stores byte-for-byte: window store
KEY `key‖windowStart:8B-BE‖seqnum:4B-BE`, VALUE `recordTs:8B-BE‖agg`
(`ValueAndTimestamp` ts-prefix trap), `TimeWindowedSerde` output key
`key‖windowStart:8B-BE`; session store and schema likewise. Emit semantics stay
emit-on-update.

## 4. Implementation batches

Per `CLAUDE.md`, tasks with disjoint file sets within a batch run in parallel.

### Batch 0 — foundation (internally serial; golden-pins names)

- `dsl/names.rs`: add cogroup constants.
- `dsl/processors/cogroup_merge.rs`: `KStreamPassThrough` merge processor.
- `dsl/cogrouped.rs`: `KGroupedStream::cogroup` + `CogroupedKStream<K, VOut>` +
  non-windowed `aggregate`/`aggregate_explicit`; shared-store + merge lowering
  helper (reused by Batch 1).
- Wire into `dsl/mod.rs`, `dsl/processors/mod.rs`, `lib.rs`.
- **Capture the non-windowed cogroup topology + behavior first** to pin names
  and counter ordering before the windowed handles depend on them.

### Batch 1 — windowed handles (parallel; disjoint files)

- `dsl/time_windowed_cogrouped.rs` — reuse `window_aggregate` processor.
- `dsl/session_windowed_cogrouped.rs` — reuse `session_aggregate` processor;
  adds the `Merger` arg.
- `dsl/sliding_windowed_cogrouped.rs` — reuse `sliding_window_aggregate`
  processor.

Each consumes the Batch 0 shared-store + merge helper and adds its
`windowed_by*` constructor on `CogroupedKStream`.

## 5. Testing (the gate)

- **JVM ground truth:** new `tests/jvm-capture/src/main/java/crabka/capture/
  CogroupBehavior.java`, cross-validated byte-for-byte vs a live
  mirror.gcr.io/apache/kafka:4.1.0 broker, emitting behavioral goldens + topology JSON for all
  four variants, each with single-input and 2-input cogroups (and at least one
  key-changing input to exercise the repartition path).
- **Wire-topology goldens:** `tests/testdata/golden/dsl/cogroup*.topology.json`;
  register each in `dsl_golden_frame.rs`.
- **Behavioral + DSL-variant tests:** `tests/dsl_cogroup.rs` mirroring the
  sliding-window coverage commit — reduce/aggregate/count parity, out-of-order,
  multi-input fold ordering. Add the new test target to the crate's llvm-cov
  `--test` list in `ci.yml` (else it reports 0% patch — see codecov memory).
- **Erasure caveat:** type mismatches in the dyn-Any lowering are **runtime**
  downcast panics, not compile errors, so the full suite is the gate.

## 6. Risks / fidelity gotchas

- Exact `COGROUP-*` node names, the shared-store name + its counter position,
  and merge-node index — only the golden capture pins these; do not assume.
- Each key-changing input mints its own repartition triple (filter/sink/source)
  → counter indices shift; multi-input fixtures must exercise this.
- Shared store registered exactly once (first input creates, rest reference) —
  a double-register or missed-register diverges the wire `state_changelog_topics`.
- Session cogroup's `Merger` must thread through to the reused session processor.
- `streams.version` feature + `config.streams_group.enable` gating and the
  ListGroups/Describe/v10-offset-key requirements from prior slices are
  unaffected but must keep passing in the broker-backed integration tests.
